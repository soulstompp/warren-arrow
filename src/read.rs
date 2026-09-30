// Copyright (C) 2026 Kenneth Allen Flegal
// SPDX-License-Identifier: Apache-2.0

//! Reading a written table back: what a reader keeps for a condition, from the files' metadata
//! alone; the rows that condition keeps, held against what was kept; the rows a reader keeps,
//! read; how tightly each row group holds a phase; the runs of a phase; and each
//! page of a column.

use std::collections::HashSet;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Int64Array, RecordBatch};
use arrow::compute::cast;
use arrow::datatypes::{
    DataType, Field, Int16Type, Int32Type, Int64Type, TimestampMicrosecondType,
};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowSelection,
    RowSelector,
};
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
use parquet::file::page_index::column_index::ColumnIndexMetaData;
use parquet::file::statistics::Statistics;

use crate::Error;
use crate::layout::{Part, Partitions, Table, phase_piece_name};
use crate::value;
use crate::write::ROWS_FILE;

/// One condition a question puts on a table. A question's conditions all hold.
#[derive(Debug, Clone)]
pub enum Term {
    /// A part of an instant column is one of these values.
    PartIn { part: Part, values: Vec<i64> },
    /// An instant column lies in `[from, to)`, in microseconds since 1970 in UTC.
    Within { column: String, from: i64, to: i64 },
    /// Two integer columns together are one of these pairs.
    PairsIn {
        columns: (String, String),
        pairs: HashSet<(i64, i64)>,
    },
    /// One column is one of these values.
    In { column: String, values: Values },
}

impl Term {
    /// The source columns the term reads on a row.
    pub fn columns(&self) -> Vec<&str> {
        match self {
            Term::PartIn { part, .. } => vec![part.column()],
            Term::Within { column, .. } | Term::In { column, .. } => vec![column],
            Term::PairsIn { columns, .. } => vec![&columns.0, &columns.1],
        }
    }
}

/// The values one column is held to: whole numbers, or texts compared by their bytes. Kept in
/// order, each once.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Values {
    Ints(Vec<i64>),
    Texts(Vec<Vec<u8>>),
}

impl Values {
    pub fn ints(mut v: Vec<i64>) -> Values {
        v.sort_unstable();
        v.dedup();
        Values::Ints(v)
    }

    pub fn texts(mut v: Vec<Vec<u8>>) -> Values {
        v.sort_unstable();
        v.dedup();
        Values::Texts(v)
    }

    pub fn len(&self) -> usize {
        match self {
            Values::Ints(v) => v.len(),
            Values::Texts(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether any of the values lies within the bounds; `None` where the bounds are of the other
    /// kind, so that they exclude nothing.
    fn any_within(&self, b: &Bound) -> Option<bool> {
        match (self, b) {
            (Values::Ints(v), Bound::Int(lo, hi)) => {
                let i = v.partition_point(|x| x < lo);
                Some(i < v.len() && v[i] <= *hi)
            }
            (Values::Texts(v), Bound::Bytes(lo, hi)) => {
                let i = v.partition_point(|x| x.as_slice() < lo.as_slice());
                Some(i < v.len() && v[i].as_slice() <= hi.as_slice())
            }
            _ => None,
        }
    }

    fn holds(&self, batch: &RecordBatch, column: &str, row: usize) -> bool {
        match self {
            Values::Ints(v) => {
                int_at(batch, column, row).is_some_and(|x| v.binary_search(&x).is_ok())
            }
            Values::Texts(v) => text_at(batch, column, row)
                .is_some_and(|x| v.binary_search_by(|p| p.as_slice().cmp(x)).is_ok()),
        }
    }
}

/// A piece of a written table: its directory's name and its rows file.
#[derive(Debug, Clone)]
pub struct PieceFile {
    pub name: String,
    pub path: PathBuf,
    /// The phase's value, for a table cut by a phase.
    pub value: Option<i64>,
}

/// The pieces of a table written into `dir`, in the order a reader of its phase would take them.
pub fn pieces(table: &Table, dir: &Path) -> Result<Vec<PieceFile>, Error> {
    let phase = table.partitions.as_ref().and_then(Partitions::phase);
    let mut out = Vec::new();
    for e in std::fs::read_dir(dir)? {
        let e = e?;
        let path = e.path().join(ROWS_FILE);
        if !path.exists() {
            continue;
        }
        let name = e.file_name().to_string_lossy().into_owned();
        let value = phase.and_then(|p| {
            let (head, v) = name.split_once('=')?;
            if format!("{head}=0") != phase_piece_name(p, 0) {
                return None;
            }
            v.parse().ok()
        });
        out.push(PieceFile { name, path, value });
    }
    out.sort_by(|a, b| (a.value, &a.name).cmp(&(b.value, &b.name)));
    Ok(out)
}

/// A file's Parquet metadata, read from its footer.
pub fn metadata(path: &Path) -> Result<ParquetMetaData, Error> {
    let file = File::open(path)?;
    Ok(ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Optional)
        .parse_and_finish(&file)?)
}

/// A piece opened once: its file, its metadata with the page index where the file has one, and the
/// bytes of its footer, which a reader reads before it can keep or skip anything in the piece.
#[derive(Debug, Clone)]
pub struct OpenPiece {
    pub piece: PieceFile,
    pub meta: ArrowReaderMetadata,
    pub footer_bytes: u64,
}

/// Opens every piece of a table written into `dir`, in the order `pieces` gives.
pub fn open(table: &Table, dir: &Path) -> Result<Vec<OpenPiece>, Error> {
    pieces(table, dir)?
        .into_iter()
        .map(|piece| {
            let file = File::open(&piece.path)?;
            let meta = ArrowReaderMetadata::load(
                &file,
                ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional),
            )?;
            let footer_bytes = footer_len(&piece.path)?;
            Ok(OpenPiece {
                piece,
                meta,
                footer_bytes,
            })
        })
        .collect()
}

/// The footer's length, from the eight bytes that end every Parquet file, counting those eight.
fn footer_len(path: &Path) -> Result<u64, Error> {
    let mut f = File::open(path)?;
    f.seek(SeekFrom::End(-8))?;
    let mut tail = [0u8; 8];
    f.read_exact(&mut tail)?;
    let len = u32::from_le_bytes(tail[0..4].try_into().expect("four bytes"));
    Ok(u64::from(len) + 8)
}

/// A column chunk's or a page's least and greatest value, as its statistics state them: whole
/// numbers, or bytes. A text's bounds may be cut short by the writer, and still hold every value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Bound {
    Int(i64, i64),
    Bytes(Vec<u8>, Vec<u8>),
}

fn bounds(stats: Option<&Statistics>) -> Option<Bound> {
    match stats? {
        Statistics::Int32(s) => Some(Bound::Int(
            i64::from(*s.min_opt()?),
            i64::from(*s.max_opt()?),
        )),
        Statistics::Int64(s) => Some(Bound::Int(*s.min_opt()?, *s.max_opt()?)),
        Statistics::ByteArray(s) => Some(Bound::Bytes(
            s.min_opt()?.data().to_vec(),
            s.max_opt()?.data().to_vec(),
        )),
        _ => None,
    }
}

/// The least and greatest value of an integer column chunk, as its statistics state them.
fn int_bounds(stats: Option<&Statistics>) -> Option<(i64, i64)> {
    match bounds(stats)? {
        Bound::Int(lo, hi) => Some((lo, hi)),
        Bound::Bytes(..) => None,
    }
}

/// Each page's least and greatest value in a column index, `None` for a page of nulls.
fn page_bounds(index: &ColumnIndexMetaData) -> Option<Vec<Option<Bound>>> {
    let n = index.num_pages() as usize;
    Some(match index {
        ColumnIndexMetaData::INT32(ix) => (0..n)
            .map(|i| {
                Some(Bound::Int(
                    i64::from(*ix.min_value(i)?),
                    i64::from(*ix.max_value(i)?),
                ))
            })
            .collect(),
        ColumnIndexMetaData::INT64(ix) => (0..n)
            .map(|i| Some(Bound::Int(*ix.min_value(i)?, *ix.max_value(i)?)))
            .collect(),
        ColumnIndexMetaData::BYTE_ARRAY(ix) => (0..n)
            .map(|i| {
                Some(Bound::Bytes(
                    ix.min_value(i)?.to_vec(),
                    ix.max_value(i)?.to_vec(),
                ))
            })
            .collect(),
        _ => return None,
    })
}

/// Where a reader can see a term: the column of the file whose bounds it reads, or the piece's
/// name. A term it cannot see never excludes anything.
enum Seen {
    Column(String),
    Pair(String, String),
    PieceName,
    Not,
}

fn seen(table: &Table, term: &Term) -> Seen {
    match term {
        Term::PartIn { part, .. } => {
            if table.partitions.as_ref().and_then(Partitions::phase) == Some(part) {
                return Seen::PieceName;
            }
            match table
                .stored()
                .into_iter()
                .find(|s| s.part().ok().as_ref() == Some(part))
            {
                Some(s) => Seen::Column(s.store.clone()),
                None => Seen::Not,
            }
        }
        Term::Within { column, .. } | Term::In { column, .. } => Seen::Column(column.clone()),
        Term::PairsIn { columns, .. } => Seen::Pair(columns.0.clone(), columns.1.clone()),
    }
}

/// Whether a term excludes everything between these bounds: `a` for its column, and `b` for the
/// second column of a pair. Bounds of a kind the term does not read exclude nothing.
fn excludes(term: &Term, a: &Bound, b: Option<&Bound>) -> bool {
    let within = |v: i64, lo: i64, hi: i64| lo <= v && v <= hi;
    match (term, a) {
        (Term::PartIn { values, .. }, Bound::Int(lo, hi)) => {
            !values.iter().any(|v| within(*v, *lo, *hi))
        }
        (Term::Within { from, to, .. }, Bound::Int(lo, hi)) => *hi < *from || *lo >= *to,
        (Term::PairsIn { pairs, .. }, Bound::Int(alo, ahi)) => {
            let b = match b {
                Some(Bound::Int(lo, hi)) => Some((*lo, *hi)),
                Some(Bound::Bytes(..)) => return false,
                None => None,
            };
            !pairs
                .iter()
                .any(|(x, y)| within(*x, *alo, *ahi) && b.is_none_or(|(lo, hi)| within(*y, lo, hi)))
        }
        (Term::In { values, .. }, a) => values.any_within(a) == Some(false),
        _ => false,
    }
}

/// Whether a reader keeps one row group, and, where it read the page index, the rows of the pages
/// it keeps.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decision {
    pub kept: bool,
    /// The kept pages' rows, `[start, end)` within the row group; `None` where no page index was
    /// read, so the whole row group is read.
    pub pages: Option<Vec<(u64, u64)>>,
}

/// What a reader keeps of one table for a question, read from the files' metadata alone.
#[derive(Debug, Clone, Default)]
pub struct Kept {
    pub pieces: u64,
    pub pieces_kept: u64,
    pub row_groups: u64,
    pub row_groups_kept: u64,
    pub rows: u64,
    pub rows_in_kept: u64,
    pub bytes: u64,
    pub bytes_kept: u64,
    /// The footers of the pieces kept: what is read to keep or skip anything inside them.
    pub footer_bytes: u64,
    /// The page column's column and offset indexes in the kept row groups.
    pub page_index_bytes: u64,
    /// The column whose page index was read, where a term is seen on a column.
    pub page_column: Option<String>,
    pub pages: u64,
    pub pages_kept: u64,
    pub rows_in_kept_pages: u64,
    /// For each piece, and each of its row groups, whether it is kept, and its kept pages.
    pub decisions: Vec<(String, Vec<Decision>)>,
}

/// The pieces, row groups and pages a reader keeps for `terms`. A piece is kept unless its name
/// excludes it; a row group, unless a term's column statistics exclude it; a page, unless the page
/// index of the first term seen on a column excludes it.
pub fn kept(table: &Table, dir: &Path, terms: &[Term]) -> Result<Kept, Error> {
    Ok(keep(table, &open(table, dir)?, terms))
}

/// `kept`, on pieces already opened.
pub fn keep(table: &Table, open: &[OpenPiece], terms: &[Term]) -> Kept {
    let mut k = Kept::default();
    let page_term = terms
        .iter()
        .find(|t| matches!(seen(table, t), Seen::Column(_) | Seen::Pair(..)));
    k.page_column = page_term.map(|t| match seen(table, t) {
        Seen::Column(c) | Seen::Pair(c, _) => c,
        _ => unreachable!(),
    });
    for p in open {
        k.pieces += 1;
        let piece_out = terms.iter().any(|t| {
            matches!(seen(table, t), Seen::PieceName)
                && p.piece
                    .value
                    .is_some_and(|v| excludes(t, &Bound::Int(v, v), None))
        });
        if !piece_out {
            k.pieces_kept += 1;
            k.footer_bytes += p.footer_bytes;
        }
        let meta = p.meta.metadata();
        let schema = meta.file_metadata().schema_descr();
        let col = |name: &str| (0..schema.num_columns()).find(|i| schema.column(*i).name() == name);
        let mut decisions = Vec::new();
        for (g, rg) in meta.row_groups().iter().enumerate() {
            k.row_groups += 1;
            k.rows += rg.num_rows() as u64;
            k.bytes += rg.compressed_size() as u64;
            let group_bounds =
                |name: &str| col(name).and_then(|i| bounds(rg.column(i).statistics()));
            let out = piece_out
                || terms.iter().any(|t| match seen(table, t) {
                    Seen::Column(c) => group_bounds(&c).is_some_and(|a| excludes(t, &a, None)),
                    Seen::Pair(a, b) => match (group_bounds(&a), group_bounds(&b)) {
                        (Some(a), Some(b)) => excludes(t, &a, Some(&b)),
                        _ => false,
                    },
                    Seen::PieceName | Seen::Not => false,
                });
            if out {
                decisions.push(Decision {
                    kept: false,
                    pages: None,
                });
                continue;
            }
            k.row_groups_kept += 1;
            k.rows_in_kept += rg.num_rows() as u64;
            k.bytes_kept += rg.compressed_size() as u64;
            let mut kept_pages = None;
            if let (Some(t), Some(name)) = (page_term, &k.page_column)
                && let (Some(i), Some(ci), Some(oi)) =
                    (col(name), meta.column_index(), meta.offset_index())
                && let Some(pages) = page_bounds(&ci[g][i])
            {
                let chunk = rg.column(i);
                k.page_index_bytes += chunk.column_index_length().unwrap_or(0) as u64
                    + chunk.offset_index_length().unwrap_or(0) as u64;
                let locations = oi[g][i].page_locations();
                let mut ranges = Vec::new();
                for (pi, b) in pages.iter().enumerate() {
                    k.pages += 1;
                    let keep = b.as_ref().is_none_or(|b| !excludes(t, b, None));
                    if keep {
                        k.pages_kept += 1;
                        let start = locations[pi].first_row_index as u64;
                        let end = locations
                            .get(pi + 1)
                            .map_or(rg.num_rows(), |l| l.first_row_index)
                            as u64;
                        k.rows_in_kept_pages += end - start;
                        ranges.push((start, end));
                    }
                }
                kept_pages = Some(ranges);
            }
            decisions.push(Decision {
                kept: true,
                pages: kept_pages,
            });
        }
        k.decisions.push((p.piece.name.clone(), decisions));
    }
    k
}

/// What reading the kept rows read: the row groups entered and the rows decoded.
#[derive(Debug, Clone, Copy, Default)]
pub struct ReadCount {
    pub row_groups: u64,
    pub rows: u64,
}

/// Reads what a reader keeps, piece by piece in order: the kept row groups, and within them only
/// the kept pages where a page index was read. Each batch read, holding `columns`, goes to `f`.
pub fn read_kept(
    open: &[OpenPiece],
    k: &Kept,
    columns: &[&str],
    mut f: impl FnMut(&RecordBatch) -> Result<(), Error>,
) -> Result<ReadCount, Error> {
    let mut count = ReadCount::default();
    for (p, (name, decisions)) in open.iter().zip(&k.decisions) {
        assert_eq!(&p.piece.name, name, "the pieces are listed in one order");
        let meta = p.meta.metadata();
        let mut groups = Vec::new();
        let mut selectors = Vec::new();
        let mut paged = false;
        for (g, d) in decisions.iter().enumerate() {
            if !d.kept {
                continue;
            }
            let n = meta.row_group(g).num_rows() as u64;
            match &d.pages {
                None => {
                    groups.push(g);
                    selectors.push(RowSelector::select(n as usize));
                    count.rows += n;
                }
                Some(ranges) if ranges.is_empty() => {}
                Some(ranges) => {
                    paged = true;
                    groups.push(g);
                    let mut at = 0;
                    for (start, end) in ranges {
                        if *start > at {
                            selectors.push(RowSelector::skip((start - at) as usize));
                        }
                        selectors.push(RowSelector::select((end - start) as usize));
                        count.rows += end - start;
                        at = *end;
                    }
                    if n > at {
                        selectors.push(RowSelector::skip((n - at) as usize));
                    }
                }
            }
        }
        if groups.is_empty() {
            continue;
        }
        count.row_groups += groups.len() as u64;
        let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(
            File::open(&p.piece.path)?,
            p.meta.clone(),
        );
        let schema = builder.parquet_schema();
        let indices: Vec<usize> = (0..schema.num_columns())
            .filter(|i| columns.contains(&schema.column(*i).name()))
            .collect();
        let mask = ProjectionMask::leaves(schema, indices);
        let mut builder = builder.with_row_groups(groups).with_projection(mask);
        if paged {
            builder = builder.with_row_selection(RowSelection::from(selectors));
        }
        for batch in builder.build()? {
            f(&batch?)?;
        }
    }
    Ok(count)
}

/// The rows a question keeps, counted in the row groups a reader keeps and in those it skips, and,
/// in the kept row groups, outside the pages it keeps.
#[derive(Debug, Clone, Copy, Default)]
pub struct Held {
    pub in_kept: u64,
    pub in_skipped: u64,
    pub in_skipped_pages: u64,
}

/// Reads every row group and counts the rows `terms` keep, beside the decisions `kept` made.
pub fn held(table: &Table, dir: &Path, terms: &[Term], k: &Kept) -> Result<Held, Error> {
    let mut h = Held::default();
    let needed: HashSet<&str> = terms.iter().flat_map(Term::columns).collect();
    for (p, (name, decisions)) in open(table, dir)?.iter().zip(&k.decisions) {
        assert_eq!(&p.piece.name, name, "the pieces are listed in one order");
        for (g, d) in decisions.iter().enumerate() {
            let (n, outside) = count_rows(p, g, &needed, terms, d.pages.as_deref())?;
            if d.kept {
                h.in_kept += n;
                h.in_skipped_pages += outside;
            } else {
                h.in_skipped += n;
            }
        }
    }
    Ok(h)
}

/// The named columns of the given row groups of one file, read batch by batch.
pub fn reader(
    path: &Path,
    row_groups: Vec<usize>,
    columns: &HashSet<&str>,
) -> Result<parquet::arrow::arrow_reader::ParquetRecordBatchReader, Error> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
    let schema = builder.parquet_schema();
    let indices: Vec<usize> = (0..schema.num_columns())
        .filter(|i| columns.contains(schema.column(*i).name()))
        .collect();
    let mask = ProjectionMask::leaves(schema, indices);
    Ok(builder
        .with_row_groups(row_groups)
        .with_projection(mask)
        .build()?)
}

/// The rows of one row group the terms keep, and how many of them lie outside `pages`.
fn count_rows(
    p: &OpenPiece,
    row_group: usize,
    columns: &HashSet<&str>,
    terms: &[Term],
    pages: Option<&[(u64, u64)]>,
) -> Result<(u64, u64), Error> {
    let (mut n, mut outside, mut at) = (0, 0, 0u64);
    let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(
        File::open(&p.piece.path)?,
        p.meta.clone(),
    );
    let schema = builder.parquet_schema();
    let indices: Vec<usize> = (0..schema.num_columns())
        .filter(|i| columns.contains(schema.column(*i).name()))
        .collect();
    let mask = ProjectionMask::leaves(schema, indices);
    for batch in builder
        .with_row_groups(vec![row_group])
        .with_projection(mask)
        .build()?
    {
        let batch = batch?;
        for row in 0..batch.num_rows() {
            if terms.iter().all(|t| holds(t, &batch, row)) {
                n += 1;
                let offset = at + row as u64;
                if pages.is_some_and(|p| !p.iter().any(|(s, e)| *s <= offset && offset < *e)) {
                    outside += 1;
                }
            }
        }
        at += batch.num_rows() as u64;
    }
    Ok((n, outside))
}

/// A whole-number column's value on a row, `None` for NULL or a column of another type.
pub fn int_at(batch: &RecordBatch, column: &str, row: usize) -> Option<i64> {
    let a = batch.column_by_name(column)?;
    if a.is_null(row) {
        return None;
    }
    Some(match a.data_type() {
        DataType::Int16 => i64::from(a.as_primitive::<Int16Type>().value(row)),
        DataType::Int32 => i64::from(a.as_primitive::<Int32Type>().value(row)),
        DataType::Int64 => a.as_primitive::<Int64Type>().value(row),
        DataType::Timestamp(_, _) => a.as_primitive::<TimestampMicrosecondType>().value(row),
        _ => return None,
    })
}

/// A text column's value on a row, `None` for NULL or a column of another type.
pub fn str_at<'a>(batch: &'a RecordBatch, column: &str, row: usize) -> Option<&'a str> {
    let a = batch.column_by_name(column)?;
    if a.is_null(row) {
        return None;
    }
    Some(match a.data_type() {
        DataType::Utf8 => a.as_string::<i32>().value(row),
        DataType::LargeUtf8 => a.as_string::<i64>().value(row),
        DataType::Utf8View => a.as_string_view().value(row),
        _ => return None,
    })
}

fn text_at<'a>(batch: &'a RecordBatch, column: &str, row: usize) -> Option<&'a [u8]> {
    str_at(batch, column, row).map(str::as_bytes)
}

/// Whether a term holds on one row.
pub fn holds(term: &Term, batch: &RecordBatch, row: usize) -> bool {
    match term {
        Term::PartIn { part, values } => int_at(batch, part.column(), row)
            .is_some_and(|t| values.contains(&value::part_of_day(part, value::day_of(t)))),
        Term::Within { column, from, to } => {
            int_at(batch, column, row).is_some_and(|t| *from <= t && t < *to)
        }
        Term::PairsIn { columns, pairs } => {
            match (
                int_at(batch, &columns.0, row),
                int_at(batch, &columns.1, row),
            ) {
                (Some(a), Some(b)) => pairs.contains(&(a, b)),
                _ => false,
            }
        }
        Term::In { column, values } => values.holds(batch, column, row),
    }
}

/// One page of a column in a written table, beside its row group and its piece. The bounds are
/// whole numbers; a column of another type has none.
#[derive(Debug, Clone)]
pub struct PageRow {
    pub piece: String,
    pub footer_bytes: u64,
    pub row_group: u64,
    pub group_rows: u64,
    pub group_bytes: u64,
    pub group_min: Option<i64>,
    pub group_max: Option<i64>,
    /// `None` where the file has no page index.
    pub page: Option<u64>,
    pub first_row: Option<u64>,
    pub page_rows: Option<u64>,
    pub page_bytes: Option<u64>,
    pub page_min: Option<i64>,
    pub page_max: Option<i64>,
}

/// Every page of `column`, from the files' metadata alone.
pub fn page_rows(table: &Table, dir: &Path, column: &str) -> Result<Vec<PageRow>, Error> {
    let mut out = Vec::new();
    for p in open(table, dir)? {
        let meta = p.meta.metadata();
        let schema = meta.file_metadata().schema_descr();
        let i = (0..schema.num_columns())
            .find(|i| schema.column(*i).name() == column)
            .ok_or_else(|| Error::Refused(format!("{} holds no column {column}", p.piece.name)))?;
        for (g, rg) in meta.row_groups().iter().enumerate() {
            let group = int_bounds(rg.column(i).statistics());
            let base = PageRow {
                piece: p.piece.name.clone(),
                footer_bytes: p.footer_bytes,
                row_group: g as u64,
                group_rows: rg.num_rows() as u64,
                group_bytes: rg.compressed_size() as u64,
                group_min: group.map(|b| b.0),
                group_max: group.map(|b| b.1),
                page: None,
                first_row: None,
                page_rows: None,
                page_bytes: None,
                page_min: None,
                page_max: None,
            };
            let (Some(ci), Some(oi)) = (meta.column_index(), meta.offset_index()) else {
                out.push(base);
                continue;
            };
            let locations = oi[g][i].page_locations();
            let pages = page_bounds(&ci[g][i]).unwrap_or_else(|| vec![None; locations.len()]);
            for (pi, loc) in locations.iter().enumerate() {
                let end = locations
                    .get(pi + 1)
                    .map_or(rg.num_rows(), |l| l.first_row_index);
                let b = match pages.get(pi) {
                    Some(Some(Bound::Int(lo, hi))) => Some((*lo, *hi)),
                    _ => None,
                };
                out.push(PageRow {
                    page: Some(pi as u64),
                    first_row: Some(loc.first_row_index as u64),
                    page_rows: Some((end - loc.first_row_index) as u64),
                    page_bytes: Some(loc.compressed_page_size as u64),
                    page_min: b.map(|b| b.0),
                    page_max: b.map(|b| b.1),
                    ..base.clone()
                });
            }
        }
    }
    Ok(out)
}

/// How one row group holds a phase.
#[derive(Debug, Clone)]
pub struct GroupBox {
    pub piece: String,
    pub row_group: u64,
    pub rows: u64,
    /// The phase's bounds as a reader sees them: from a stored column's statistics or the piece's
    /// name. `None` where the reader cannot see the phase.
    pub seen: Option<(i64, i64)>,
    /// The instant column's bounds, from its statistics.
    pub instant: Option<(i64, i64)>,
    /// How many of the phase's values the row group's rows hold, read from the rows.
    pub values_held: u64,
}

/// For each row group, the phase's box a reader sees, and the phase's values its rows hold.
pub fn boxes(table: &Table, dir: &Path, part: &Part) -> Result<Vec<GroupBox>, Error> {
    let term = Term::PartIn {
        part: part.clone(),
        values: Vec::new(),
    };
    let mut out = Vec::new();
    let columns: HashSet<&str> = [part.column()].into_iter().collect();
    for piece in pieces(table, dir)? {
        let meta = metadata(&piece.path)?;
        let schema = meta.file_metadata().schema_descr();
        let col = |name: &str| (0..schema.num_columns()).find(|i| schema.column(*i).name() == name);
        for (g, rg) in meta.row_groups().iter().enumerate() {
            let bounds = |name: &str| col(name).and_then(|i| int_bounds(rg.column(i).statistics()));
            let seen_box = match seen(table, &term) {
                Seen::Column(c) => bounds(&c),
                Seen::PieceName => piece.value.map(|v| (v, v)),
                _ => None,
            };
            let mut values = HashSet::new();
            for batch in reader(&piece.path, vec![g], &columns)? {
                let batch = batch?;
                for row in 0..batch.num_rows() {
                    if let Some(t) = int_at(&batch, part.column(), row) {
                        values.insert(value::part_of_day(part, value::day_of(t)));
                    }
                }
            }
            out.push(GroupBox {
                piece: piece.name.clone(),
                row_group: g as u64,
                rows: rg.num_rows() as u64,
                seen: seen_box,
                instant: bounds(part.column()),
                values_held: values.len() as u64,
            });
        }
    }
    Ok(out)
}

/// A phase read as runs, per piece.
#[derive(Debug, Clone)]
pub struct Runs {
    pub piece: String,
    /// How the phase was read: `stored`, from the stored column; `name`, from the piece's name;
    /// `computed`, from the instant column's values.
    pub source: &'static str,
    /// The pages of the stored column, and those holding one value, from the page index alone.
    pub pages: u64,
    pub pages_one_value: u64,
    /// The runs of the phase, read into Arrow as run ends.
    pub runs: u64,
    /// The phase's distinct values in the piece: its blocks.
    pub blocks: u64,
    pub rows: u64,
}

/// For each piece, the phase's runs against its blocks.
pub fn runs(table: &Table, dir: &Path, part: &Part) -> Result<Vec<Runs>, Error> {
    let term = Term::PartIn {
        part: part.clone(),
        values: Vec::new(),
    };
    let mut out = Vec::new();
    for piece in pieces(table, dir)? {
        let meta = metadata(&piece.path)?;
        let rows: u64 = meta.row_groups().iter().map(|r| r.num_rows() as u64).sum();
        match seen(table, &term) {
            Seen::PieceName => out.push(Runs {
                piece: piece.name.clone(),
                source: "name",
                pages: 0,
                pages_one_value: 0,
                runs: 1,
                blocks: 1,
                rows,
            }),
            Seen::Column(c) => {
                let schema = meta.file_metadata().schema_descr();
                let i = (0..schema.num_columns())
                    .find(|i| schema.column(*i).name() == c)
                    .expect("the stored column is in the file");
                let (mut pages, mut one) = (0, 0);
                if let Some(ci) = meta.column_index() {
                    for g in ci {
                        for b in page_bounds(&g[i]).into_iter().flatten().flatten() {
                            pages += 1;
                            if matches!(&b, Bound::Int(lo, hi) if lo == hi)
                                || matches!(&b, Bound::Bytes(lo, hi) if lo == hi)
                            {
                                one += 1;
                            }
                        }
                    }
                }
                let columns: HashSet<&str> = [c.as_str()].into_iter().collect();
                let values = read_ints(&piece.path, &columns, &c)?;
                let (r, blocks) = run_count(&values)?;
                out.push(Runs {
                    piece: piece.name.clone(),
                    source: "stored",
                    pages,
                    pages_one_value: one,
                    runs: r,
                    blocks,
                    rows,
                });
            }
            _ => {
                let columns: HashSet<&str> = [part.column()].into_iter().collect();
                let values: Vec<i64> = read_ints(&piece.path, &columns, part.column())?
                    .into_iter()
                    .map(|t| value::part_of_day(part, value::day_of(t)))
                    .collect();
                let (r, blocks) = run_count(&values)?;
                out.push(Runs {
                    piece: piece.name.clone(),
                    source: "computed",
                    pages: 0,
                    pages_one_value: 0,
                    runs: r,
                    blocks,
                    rows,
                });
            }
        }
    }
    Ok(out)
}

fn read_ints(path: &Path, columns: &HashSet<&str>, name: &str) -> Result<Vec<i64>, Error> {
    let mut out = Vec::new();
    for batch in reader(
        path,
        (0..metadata(path)?.num_row_groups()).collect(),
        columns,
    )? {
        let batch = batch?;
        for row in 0..batch.num_rows() {
            out.extend(int_at(&batch, name, row));
        }
    }
    Ok(out)
}

/// The number of runs the values make once cast to Arrow's run-end form, and their distinct values.
fn run_count(values: &[i64]) -> Result<(u64, u64), Error> {
    let array: ArrayRef = Arc::new(Int64Array::from(values.to_vec()));
    let ree = cast(
        &array,
        &DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, false)),
            Arc::new(Field::new("values", DataType::Int64, true)),
        ),
    )?;
    let runs = ree.as_run::<Int32Type>().run_ends().values().len() as u64;
    let blocks = values.iter().collect::<HashSet<_>>().len() as u64;
    Ok((runs, blocks))
}
