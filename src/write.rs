//! Writing one table: its rows in the layout's types, in its key's order, cut into pieces, each
//! piece a directory holding one Parquet file, and the certificate read back from the files.

use std::collections::HashSet;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, Int32Array, Int64Array, RecordBatch, StringArray, UInt32Array,
    UInt64Array,
};
use arrow::compute::{SortOptions, concat_batches, take};
use arrow::datatypes::{DataType, Date32Type, Field, Schema, SchemaRef, TimestampMicrosecondType};
use arrow::row::{RowConverter, Rows, SortField};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use parquet::file::metadata::SortingColumn;
use parquet::file::properties::{EnabledStatistics, WriterProperties};

use crate::Error;
use crate::certificate::Certificate;
use crate::layout::{Cell, Layout, Level, Part, Table, Type};
use crate::value::{self, Reader, Value};

/// The file each piece's directory holds.
pub const ROWS_FILE: &str = "rows.parquet";
/// Each piece's bounds, rows and row groups, beside the pieces.
pub const PIECES_FILE: &str = "pieces.parquet";
/// The certificate of the rows read and of the rows written, beside the pieces.
pub const CERTIFICATE_FILE: &str = "certificate.parquet";

/// What the layout file does not say.
#[derive(Debug, Clone, Default)]
pub struct Options {
    /// The most rows in a row group. `None` leaves the parquet crate's default.
    pub max_row_group_rows: Option<usize>,
}

/// One piece as written.
#[derive(Debug, Clone)]
pub struct Piece {
    pub name: String,
    /// The least value of the range column the piece holds; `None` for the first piece.
    pub from: Option<String>,
    /// The least value of the next piece; `None` for the last.
    pub to: Option<String>,
    pub rows: u64,
    pub row_groups: u64,
}

/// One table as written.
#[derive(Debug, Clone)]
pub struct Written {
    pub table: String,
    pub dir: PathBuf,
    pub pieces: Vec<Piece>,
    pub source: Certificate,
    pub written: Certificate,
}

/// A Parquet file's rows, read whole.
pub fn read_source(path: &Path) -> Result<RecordBatch, Error> {
    let file = File::open(path)
        .map_err(|e| Error::Refused(format!("{}: {e}", path.display())))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let schema = builder.schema().clone();
    let reader = builder.build()?;
    let batches = reader.collect::<Result<Vec<_>, _>>()?;
    Ok(concat_batches(&schema, &batches)?)
}

/// Writes the layout's table `name` from `source` into `into/<name>/`, or refuses with the reason.
pub fn write_table(
    layout: &Layout,
    name: &str,
    source: &RecordBatch,
    into: &Path,
    opts: &Options,
) -> Result<Written, Error> {
    let table = layout
        .table(name)
        .ok_or_else(|| Error::Refused(format!("{name} is not a table of the layout")))?;
    let dir = into.join(&table.name);
    if dir.exists() {
        return Err(Error::Refused(format!(
            "{} already exists; nothing is written over",
            dir.display()
        )));
    }

    let rows = source.num_rows();
    let batch = typed(table, source)?;
    #[cfg(feature = "geo")]
    if table.columns.iter().any(|c| c.ty == Type::Geography) {
        crate::geo::install()?;
    }
    let source_certificate = Certificate::of(table, source)?;
    unique_primary_key(table, &batch)?;
    let order = key_order(table, &batch)?;
    let (pieces, bounds) = cut(table, &batch, &order)?;

    let partial = into.join(format!(".{}.partial", table.name));
    if partial.exists() {
        fs::remove_dir_all(&partial)?;
    }
    fs::create_dir_all(&partial)?;

    let props = properties(table, &batch.schema(), opts);
    let width = pieces.len().to_string().len();
    let range = table.partitions.as_ref().map(|p| {
        batch
            .column_by_name(&p.range)
            .expect("checked with the layout")
            .clone()
    });
    let mut written_pieces = Vec::with_capacity(pieces.len());
    for (i, rows_of_piece) in pieces.iter().enumerate() {
        let piece_name = format!("p{:0width$}", i + 1);
        let piece_dir = partial.join(&piece_name);
        fs::create_dir(&piece_dir)?;
        let indices = UInt32Array::from(rows_of_piece.clone());
        let columns = batch
            .columns()
            .iter()
            .map(|c| take(c.as_ref(), &indices, None))
            .collect::<Result<Vec<_>, _>>()?;
        let piece_batch = RecordBatch::try_new(batch.schema(), columns)?;
        let file = File::create(piece_dir.join(ROWS_FILE))?;
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props.clone()))?;
        writer.write(&piece_batch)?;
        let meta = writer.close()?;
        written_pieces.push(Piece {
            name: piece_name,
            from: None,
            to: None,
            rows: rows_of_piece.len() as u64,
            row_groups: meta.num_row_groups() as u64,
        });
    }
    if let Some(range) = &range {
        let col = table
            .column(&table.partitions.as_ref().expect("a range").range)
            .expect("checked with the layout");
        let reader = Reader::new(&table.name, col, range.as_ref())?;
        for i in 0..written_pieces.len() {
            if i > 0 {
                written_pieces[i].from = Some(show(reader.get(bounds[i - 1] as usize)?));
            }
            if i + 1 < written_pieces.len() {
                written_pieces[i].to = Some(show(reader.get(bounds[i] as usize)?));
            }
        }
    }
    write_pieces_file(&partial, table, range.as_ref(), &bounds, &written_pieces)?;

    let mut written_certificate = Certificate::default();
    for p in &written_pieces {
        let back = read_source(&partial.join(&p.name).join(ROWS_FILE))?;
        written_certificate = written_certificate.plus(Certificate::of(table, &back)?);
    }
    write_certificate_file(&partial, source_certificate, written_certificate)?;
    if source_certificate != written_certificate || written_certificate.rows != rows as u64 {
        return Err(Error::Refused(format!(
            "{}: the rows read back from {} are not the rows read from the source ({source_certificate:?} against {written_certificate:?}); the files are left there and {} is not made",
            table.name,
            partial.display(),
            dir.display()
        )));
    }
    fs::rename(&partial, &dir)?;
    Ok(Written {
        table: table.name.clone(),
        dir,
        pieces: written_pieces,
        source: source_certificate,
        written: written_certificate,
    })
}

/// The source's columns in the layout's types and order. A source column the layout does not name
/// is refused: dropping it would drop a fact.
fn typed(table: &Table, source: &RecordBatch) -> Result<RecordBatch, Error> {
    let schema = source.schema();
    let extra: Vec<&str> = schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .filter(|n| table.column(n).is_none())
        .collect();
    if !extra.is_empty() {
        return Err(Error::Refused(format!(
            "{}: the source holds {}, which the layout does not name; a column is written only where the layout names it",
            table.name,
            extra.join(", ")
        )));
    }
    let mut fields = Vec::with_capacity(table.columns.len());
    let mut columns = Vec::with_capacity(table.columns.len());
    for col in &table.columns {
        let array = source.column_by_name(&col.name).ok_or_else(|| {
            Error::Refused(format!("{}: the source has no column {}", table.name, col.name))
        })?;
        let reader = Reader::new(&table.name, col, array.as_ref())?;
        columns.push(value::convert(&table.name, col, &reader, source.num_rows())?);
        fields.push(value::field(col));
    }
    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?)
}

fn converter(arrays: &[ArrayRef]) -> Result<Rows, Error> {
    let options = SortOptions { descending: false, nulls_first: false };
    let fields = arrays
        .iter()
        .map(|a| SortField::new_with_options(a.data_type().clone(), options))
        .collect();
    let conv = RowConverter::new(fields)?;
    Ok(conv.convert_columns(arrays)?)
}

fn unique_primary_key(table: &Table, batch: &RecordBatch) -> Result<(), Error> {
    if table.primary_key.is_empty() {
        return Ok(());
    }
    let arrays: Vec<ArrayRef> = table
        .primary_key
        .iter()
        .map(|c| batch.column_by_name(c).expect("typed by the layout").clone())
        .collect();
    let rows = converter(&arrays)?;
    let mut seen = HashSet::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        if !seen.insert(rows.row(i)) {
            return Err(Error::Refused(format!(
                "{}: row {i} repeats a primary key ({}) an earlier row holds",
                table.name,
                table.primary_key.join(", ")
            )));
        }
    }
    Ok(())
}

/// The arrays the rows are ordered by: the key's levels, then the primary key, then every other
/// column in the layout's order, so that no two different rows tie.
fn order_arrays(table: &Table, batch: &RecordBatch) -> Result<Vec<ArrayRef>, Error> {
    let mut arrays = Vec::new();
    let mut used: HashSet<&str> = HashSet::new();
    if let Some(k) = table.key() {
        for l in &k.levels {
            match l {
                Level::Column(c) => {
                    arrays.push(batch.column_by_name(c).expect("typed by the layout").clone());
                    used.insert(c);
                }
                Level::Part(p) => arrays.push(part(table, batch, p)),
                Level::Cell(c) => arrays.push(cell(table, batch, c)?),
            }
        }
    }
    for c in table.primary_key.iter().chain(table.columns.iter().map(|c| &c.name)) {
        if used.insert(c) {
            arrays.push(batch.column_by_name(c).expect("typed by the layout").clone());
        }
    }
    Ok(arrays)
}

/// The rows' order: each row's index, in key order.
fn key_order(table: &Table, batch: &RecordBatch) -> Result<Vec<u32>, Error> {
    let rows = converter(&order_arrays(table, batch)?)?;
    let mut order: Vec<u32> = (0..batch.num_rows() as u32).collect();
    order.sort_by(|a, b| rows.row(*a as usize).cmp(&rows.row(*b as usize)));
    Ok(order)
}

/// A month or a year, computed from an instant or a date for each row.
fn part(table: &Table, batch: &RecordBatch, p: &Part) -> ArrayRef {
    let (c, month) = match p {
        Part::Month(c) => (c, true),
        Part::Year(c) => (c, false),
    };
    let a = batch.column_by_name(c).expect("typed by the layout");
    let day = |i: usize| -> Option<i64> {
        if a.is_null(i) {
            return None;
        }
        Some(match table.column(c).expect("checked with the layout").ty {
            Type::Instant => value::day_of(a.as_primitive::<TimestampMicrosecondType>().value(i)),
            _ => i64::from(a.as_primitive::<Date32Type>().value(i)),
        })
    };
    if month {
        Arc::new(
            (0..a.len())
                .map(|i| day(i).map(|d| value::year_month(d).1 as i32))
                .collect::<Int32Array>(),
        )
    } else {
        Arc::new(
            (0..a.len())
                .map(|i| day(i).map(|d| value::year_month(d).0))
                .collect::<Int64Array>(),
        )
    }
}

/// The rows of each piece, in key order, and the row holding each piece's least value after the
/// first. A piece starts at every `every`th row in the range
/// column's order, at a value greater than the one the piece before it starts at, so a value is
/// never split across two pieces and no piece is empty. With no partitions, one piece.
fn cut(
    table: &Table,
    batch: &RecordBatch,
    order: &[u32],
) -> Result<(Vec<Vec<u32>>, Vec<u32>), Error> {
    let n = batch.num_rows();
    if n == 0 {
        return Err(Error::Refused(format!("{}: the source holds no rows", table.name)));
    }
    let Some(p) = &table.partitions else {
        return Ok((vec![order.to_vec()], Vec::new()));
    };
    let range = batch.column_by_name(&p.range).expect("typed by the layout").clone();
    let rows = converter(&[range])?;
    let mut by_range: Vec<usize> = (0..n).collect();
    by_range.sort_by(|a, b| rows.row(*a).cmp(&rows.row(*b)));
    let every = usize::try_from(p.every).unwrap_or(usize::MAX);
    let mut bounds: Vec<usize> = Vec::new();
    let mut at = every;
    while at < n {
        let candidate = by_range[at];
        let previous = bounds.last().copied().unwrap_or(by_range[0]);
        if rows.row(candidate) > rows.row(previous) {
            bounds.push(candidate);
        }
        at = at.saturating_add(every);
    }
    let mut pieces = vec![Vec::new(); bounds.len() + 1];
    for &i in order {
        let v = rows.row(i as usize);
        let piece = bounds.partition_point(|&b| rows.row(b) <= v);
        pieces[piece].push(i);
    }
    Ok((pieces, bounds.into_iter().map(|b| b as u32).collect()))
}

fn properties(table: &Table, schema: &SchemaRef, opts: &Options) -> WriterProperties {
    let index = |c: &str| schema.index_of(c).expect("typed by the layout") as i32;
    let mut sorting = Vec::new();
    match table.key() {
        Some(k) => {
            for l in &k.levels {
                match l {
                    Level::Column(c) => sorting.push(index(c)),
                    Level::Part(_) | Level::Cell(_) => break,
                }
            }
        }
        None => sorting.extend(table.primary_key.iter().map(|c| index(c))),
    }
    let sorting = sorting
        .into_iter()
        .map(|column_idx| SortingColumn { column_idx, descending: false, nulls_first: false })
        .collect::<Vec<_>>();
    let mut b = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_sorting_columns(if sorting.is_empty() { None } else { Some(sorting) });
    if let Some(n) = opts.max_row_group_rows {
        b = b.set_max_row_group_row_count(Some(n));
    }
    b.build()
}

fn write_pieces_file(
    dir: &Path,
    table: &Table,
    range: Option<&ArrayRef>,
    bounds: &[u32],
    pieces: &[Piece],
) -> Result<(), Error> {
    let mut fields = vec![Field::new("piece", DataType::Utf8, false)];
    let mut columns: Vec<ArrayRef> =
        vec![Arc::new(StringArray::from_iter_values(pieces.iter().map(|p| p.name.as_str())))];
    if let Some(range) = range {
        let from: UInt32Array = (0..pieces.len())
            .map(|i| (i > 0).then(|| bounds[i - 1]))
            .collect();
        let to: UInt32Array = (0..pieces.len())
            .map(|i| (i + 1 < pieces.len()).then(|| bounds[i]))
            .collect();
        let name = &table.partitions.as_ref().expect("a range").range;
        fields.push(Field::new(format!("from_{name}"), range.data_type().clone(), true));
        columns.push(take(range.as_ref(), &from, None)?);
        fields.push(Field::new(format!("to_{name}"), range.data_type().clone(), true));
        columns.push(take(range.as_ref(), &to, None)?);
    }
    fields.push(Field::new("rows", DataType::Int64, false));
    columns.push(Arc::new(Int64Array::from_iter_values(pieces.iter().map(|p| p.rows as i64))));
    fields.push(Field::new("row_groups", DataType::Int64, false));
    columns.push(Arc::new(Int64Array::from_iter_values(
        pieces.iter().map(|p| p.row_groups as i64),
    )));
    write_small(&dir.join(PIECES_FILE), fields, columns)
}

fn write_certificate_file(dir: &Path, source: Certificate, written: Certificate) -> Result<(), Error> {
    let fields = vec![
        Field::new("side", DataType::Utf8, false),
        Field::new("rows", DataType::UInt64, false),
        Field::new("sum_a", DataType::UInt64, false),
        Field::new("sum_b", DataType::UInt64, false),
    ];
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec!["source", "written"])),
        Arc::new(UInt64Array::from(vec![source.rows, written.rows])),
        Arc::new(UInt64Array::from(vec![source.sum_a, written.sum_a])),
        Arc::new(UInt64Array::from(vec![source.sum_b, written.sum_b])),
    ];
    write_small(&dir.join(CERTIFICATE_FILE), fields, columns)
}

fn write_small(path: &Path, fields: Vec<Field>, columns: Vec<ArrayRef>) -> Result<(), Error> {
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), columns)?;
    let mut w = ArrowWriter::try_new(File::create(path)?, schema, None)?;
    w.write(&batch)?;
    w.close()?;
    Ok(())
}

/// A value as a person reads it.
fn show(v: Value<'_>) -> String {
    match v {
        Value::Null => "null".into(),
        Value::Int(i) => i.to_string(),
        Value::Text(s) => format!("{s:?}"),
        Value::Instant(t) => {
            let day = value::day_of(t);
            let (y, m) = value::year_month(day);
            let micros_of_day = t - day * 86_400_000_000;
            let first = days_before_month(y, m);
            let d = day - first + 1;
            let s = micros_of_day / 1_000_000;
            format!(
                "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{:06}Z",
                s / 3600,
                s / 60 % 60,
                s % 60,
                micros_of_day % 1_000_000
            )
        }
        Value::Date(d) => {
            let (y, m) = value::year_month(i64::from(d));
            let day = i64::from(d) - days_before_month(y, m) + 1;
            format!("{y:04}-{m:02}-{day:02}")
        }
        Value::Bool(b) => b.to_string(),
        Value::Wkb(b) => match value::point(b) {
            Ok((lon, lat)) => format!("POINT ({lon} {lat})"),
            Err(_) => format!("{} bytes of WKB", b.len()),
        },
    }
}

/// The day number of the first of a month.
fn days_before_month(y: i64, m: u32) -> i64 {
    let (y, m) = if m <= 2 { (y - 1, i64::from(m) + 9) } else { (y, i64::from(m) - 3) };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let doy = (153 * m + 2) / 5;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The geo cell of each row's point.
#[cfg(feature = "geo")]
fn cell(table: &Table, batch: &RecordBatch, c: &Cell) -> Result<ArrayRef, Error> {
    let grid = crate::geo::Grid::new(c.band, c.sector)
        .map_err(|e| Error::Refused(format!("{}: {e}", table.name)))?;
    let points = batch.column_by_name(&c.cell).expect("typed by the layout");
    let wkb = points.as_binary::<i32>();
    let mut out = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        if points.is_null(i) {
            out.push(None);
            continue;
        }
        let at = |e: String| Error::Refused(format!("{} at row {i}: {e}", table.name));
        let (lon, lat) = value::point(wkb.value(i)).map_err(at)?;
        out.push(Some(grid.cell(lon, lat).map_err(at)?));
    }
    Ok(Arc::new(Int64Array::from(out)))
}

#[cfg(not(feature = "geo"))]
fn cell(table: &Table, _: &RecordBatch, _: &Cell) -> Result<ArrayRef, Error> {
    Err(Error::Refused(format!(
        "{}: a cell level is read only with the geo feature, and this was built without the geo feature",
        table.name
    )))
}
