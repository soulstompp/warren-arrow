// Copyright (C) 2026 Kenneth Allen Flegal
// SPDX-License-Identifier: Apache-2.0

//! A burrow: one table of a layout held in memory, its rows in the key's order, added to as rows
//! arrive and read by the key. Nothing is written to disk. Its rows go to Parquet through
//! [`write_table`](crate::write_table) whenever files are wanted, and carry the same certificate
//! there.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Int16Array, Int32Array, Int64Array,
    RecordBatch, StringArray, TimestampMicrosecondArray, UInt32Array, new_null_array,
};
use arrow::compute::{filter_record_batch, take};
use arrow::datatypes::DataType;
use arrow::row::{OwnedRow, Row, RowConverter, Rows};

use crate::Error;
use crate::certificate::Certificate;
#[cfg(feature = "geo")]
use crate::layout::Type;
use crate::layout::{Layout, Level, Table, part_column};
use crate::read::{Term, holds};
use crate::value::{UTC, Value};
use crate::write::{level_array, order_arrays, sort_fields, typed, with_stored};

/// One table of a layout held in memory.
///
/// Its rows are kept in the order [`write_table`](crate::write_table) writes them: the key's
/// levels, then the primary key, then every other column. Rows are added only after those it
/// holds, so nothing held ever moves, and a read of rows already taken survives every later add.
///
/// ```
/// use std::sync::Arc;
/// use arrow::array::{Int64Array, RecordBatch};
/// use arrow::datatypes::{DataType, Field, Schema};
/// use warren_arrow::Layout;
/// use warren_arrow::burrow::Burrow;
/// use warren_arrow::value::Value;
///
/// let layout = Arc::new(Layout::parse(r#"{
///   "format": 2, "name": "talk",
///   "tables": [{
///     "name": "turns",
///     "columns": [
///       {"name": "conversation", "type": "bigint", "null": false},
///       {"name": "turn", "type": "bigint", "null": false}
///     ],
///     "primary_key": ["conversation", "turn"],
///     "keys": [{"name": "turns_key", "levels": ["conversation", "turn"]}]
///   }]
/// }"#)?);
/// let rows = |c: Vec<i64>, t: Vec<i64>| {
///     let schema = Arc::new(Schema::new(vec![
///         Field::new("conversation", DataType::Int64, false),
///         Field::new("turn", DataType::Int64, false),
///     ]));
///     RecordBatch::try_new(schema, vec![
///         Arc::new(Int64Array::from(c)),
///         Arc::new(Int64Array::from(t)),
///     ])
/// };
///
/// let mut turns = Burrow::begin(&layout, "turns", &rows(vec![1, 1, 2], vec![1, 2, 1])?)?;
/// turns.add(&rows(vec![2, 3], vec![2, 1])?)?;
/// assert!(turns.add(&rows(vec![1], vec![3])?).is_err());
///
/// let second: usize = turns.read(&[Value::Int(2)], &[])?.iter().map(|b| b.num_rows()).sum();
/// assert_eq!(second, 2);
/// assert_eq!(turns.len(), 5);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct Burrow {
    layout: Arc<Layout>,
    at: usize,
    /// Compares whole rows, in the order they are held.
    order: RowConverter,
    /// Compares the key's levels alone; none for a table with no key.
    key: Option<RowConverter>,
    /// The type each key level is compared as.
    key_types: Vec<DataType>,
    /// Compares the primary key alone; none for a table with no primary key.
    primary: Option<RowConverter>,
    chunks: Vec<Chunk>,
    greatest: Option<OwnedRow>,
    primary_keys: HashSet<OwnedRow>,
    certificate: Certificate,
    rows: usize,
}

/// The rows of one add, in order, beside their key's levels as compared.
struct Chunk {
    rows: RecordBatch,
    key: Option<Rows>,
}

impl Burrow {
    /// A burrow holding a layout table's rows, which may be none. The rows are typed to the
    /// layout, put in the key's order and certified, as [`write_table`](crate::write_table)
    /// takes a source.
    pub fn begin(layout: &Arc<Layout>, name: &str, rows: &RecordBatch) -> Result<Burrow, Error> {
        let at = layout
            .tables
            .iter()
            .position(|t| t.name == name)
            .ok_or_else(|| Error::Refused(format!("{name} is not a table of the layout")))?;
        let table = &layout.tables[at];
        #[cfg(feature = "geo")]
        if table.columns.iter().any(|c| c.ty == Type::Geography) {
            crate::geo::install()?;
        }
        let batch = with_stored(table, typed(table, &rows.slice(0, 0))?)?;
        let order = RowConverter::new(sort_fields(&order_arrays(table, &batch)?))?;
        let levels = key_arrays(table, &batch)?;
        let key_types = levels.iter().map(|a| a.data_type().clone()).collect();
        let key = match table.key() {
            Some(_) => Some(RowConverter::new(sort_fields(&levels))?),
            None => None,
        };
        let primary = if table.primary_key.is_empty() {
            None
        } else {
            Some(RowConverter::new(sort_fields(&primary_arrays(
                table, &batch,
            )))?)
        };
        let mut burrow = Burrow {
            layout: Arc::clone(layout),
            at,
            order,
            key,
            key_types,
            primary,
            chunks: Vec::new(),
            greatest: None,
            primary_keys: HashSet::new(),
            certificate: Certificate::default(),
            rows: 0,
        };
        burrow.add(rows)?;
        Ok(burrow)
    }

    /// Adds rows after those held. The rows are put in the key's order among themselves, and are
    /// refused unless the least of them comes after the greatest row held, and no primary key
    /// repeats one held or another added. A refused add leaves the burrow as it was.
    pub fn add(&mut self, rows: &RecordBatch) -> Result<(), Error> {
        let table = &self.layout.tables[self.at];
        let given = Certificate::of(table, rows)?;
        let batch = with_stored(table, typed(table, rows)?)?;
        let n = batch.num_rows();
        if n == 0 {
            return Ok(());
        }

        let order = self.order.convert_columns(&order_arrays(table, &batch)?)?;
        let mut sorted: Vec<u32> = (0..n as u32).collect();
        sorted.sort_by(|a, b| order.row(*a as usize).cmp(&order.row(*b as usize)));
        let least = order.row(sorted[0] as usize);
        if let Some(greatest) = &self.greatest
            && least <= greatest.row()
        {
            return Err(Error::Refused(format!(
                "{}: the rows added do not all come after the rows held in the key's order; a burrow adds rows only after those it holds",
                table.name
            )));
        }

        let mut added_keys = HashSet::new();
        if let Some(primary) = &self.primary {
            let keys = primary.convert_columns(&primary_arrays(table, &batch))?;
            for i in 0..n {
                let k = keys.row(i);
                if self.primary_keys.contains(&k.owned()) || !added_keys.insert(k.owned()) {
                    return Err(Error::Refused(format!(
                        "{}: row {i} repeats a primary key ({}) held or added before it",
                        table.name,
                        table.primary_key.join(", ")
                    )));
                }
            }
        }

        let indices = UInt32Array::from(sorted.clone());
        let columns = batch
            .columns()
            .iter()
            .map(|c| take(c.as_ref(), &indices, None))
            .collect::<Result<Vec<_>, _>>()?;
        let ordered = RecordBatch::try_new(batch.schema(), columns)?;
        let key = match &self.key {
            Some(k) => Some(k.convert_columns(&key_arrays(table, &ordered)?)?),
            None => None,
        };
        // A level the key stores orders the rows and is not held: the burrow holds the layout's
        // columns alone, so the rows it holds go to `write_table` or another burrow as they are.
        let held = ordered.project(&(0..table.columns.len()).collect::<Vec<_>>())?;
        let certificate = Certificate::of(table, &held)?;
        if certificate != given {
            return Err(Error::Refused(format!(
                "{}: the rows held are not the rows given ({given:?} against {certificate:?})",
                table.name
            )));
        }

        self.greatest = Some(order.row(sorted[n - 1] as usize).owned());
        self.primary_keys.extend(added_keys);
        self.certificate = self.certificate.plus(certificate);
        self.rows += n;
        self.chunks.push(Chunk { rows: held, key });
        Ok(())
    }

    /// The rows whose leading key levels hold `prefix`'s values, one value a level, kept where
    /// every term holds. An empty prefix reads every row held, and no terms keep every row of the
    /// prefix. Rows come back in the key's order, as slices of the rows held where no term is
    /// given.
    ///
    /// A level that orders rows by a part of an instant, or by a point's cell, takes the number the
    /// order uses: the month, the year, the weekday or the cell.
    pub fn read(&self, prefix: &[Value<'_>], terms: &[Term]) -> Result<Vec<RecordBatch>, Error> {
        let table = self.table();
        for t in terms {
            for c in t.columns() {
                if table.column(c).is_none() {
                    return Err(Error::Refused(format!(
                        "{}: a term reads {c}, which the table does not hold",
                        table.name
                    )));
                }
            }
        }
        let runs = self.runs(prefix)?;
        let mut out = Vec::with_capacity(runs.len());
        for run in runs {
            let run = if terms.is_empty() {
                run
            } else {
                let keep: BooleanArray = (0..run.num_rows())
                    .map(|r| Some(terms.iter().all(|t| holds(t, &run, r))))
                    .collect();
                filter_record_batch(&run, &keep)?
            };
            if run.num_rows() > 0 {
                out.push(run);
            }
        }
        Ok(out)
    }

    /// The layout table the burrow holds.
    pub fn table(&self) -> &Table {
        &self.layout.tables[self.at]
    }

    /// How many rows the burrow holds.
    pub fn len(&self) -> usize {
        self.rows
    }

    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// The certificate of the rows held: the same as [`write_table`](crate::write_table) gives the
    /// same rows.
    pub fn certificate(&self) -> Certificate {
        self.certificate
    }

    /// Each add's rows holding the prefix, in order.
    fn runs(&self, prefix: &[Value<'_>]) -> Result<Vec<RecordBatch>, Error> {
        if prefix.is_empty() {
            return Ok(self.chunks.iter().map(|c| c.rows.clone()).collect());
        }
        let table = self.table();
        let levels = table.key().map_or(&[][..], |k| &k.levels[..]);
        if prefix.len() > levels.len() {
            return Err(Error::Refused(format!(
                "{}: the prefix gives {} values and the key has {} levels",
                table.name,
                prefix.len(),
                levels.len()
            )));
        }
        let mut arrays = Vec::with_capacity(prefix.len());
        for (i, (v, l)) in prefix.iter().zip(levels).enumerate() {
            arrays.push(one(table, l, v, &self.key_types[i])?);
        }
        let wanted = RowConverter::new(sort_fields(&arrays))?.convert_columns(&arrays)?;
        let wanted = wanted.row(0);
        let wanted = wanted.as_ref();

        let first = self
            .chunks
            .partition_point(|c| last_key(c).is_some_and(|r| leads(r, wanted).is_lt()));
        let mut out = Vec::new();
        for c in &self.chunks[first..] {
            let rows = c
                .key
                .as_ref()
                .expect("a table with a key keeps each add's key");
            let n = rows.num_rows();
            if leads(rows.row(0), wanted).is_gt() {
                break;
            }
            let from = partition(n, |i| leads(rows.row(i), wanted).is_lt());
            let to = partition(n, |i| !leads(rows.row(i), wanted).is_gt());
            if to > from {
                out.push(c.rows.slice(from, to - from));
            }
        }
        Ok(out)
    }
}

/// How a row's leading key levels compare with a prefix's values: the bytes of each level's value
/// come one after another, so the prefix's bytes are the start of a row holding it.
fn leads(row: Row<'_>, prefix: &[u8]) -> std::cmp::Ordering {
    let row = row.as_ref();
    let k = row.len().min(prefix.len());
    match row[..k].cmp(&prefix[..k]) {
        std::cmp::Ordering::Equal if row.len() >= prefix.len() => std::cmp::Ordering::Equal,
        std::cmp::Ordering::Equal => std::cmp::Ordering::Less,
        o => o,
    }
}

fn last_key(c: &Chunk) -> Option<Row<'_>> {
    let rows = c.key.as_ref()?;
    Some(rows.row(rows.num_rows() - 1))
}

/// The first of `0..n` where `before` stops holding.
fn partition(n: usize, before: impl Fn(usize) -> bool) -> usize {
    let (mut lo, mut hi) = (0, n);
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if before(mid) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

/// The key's levels, each as the rows are compared by it.
fn key_arrays(table: &Table, batch: &RecordBatch) -> Result<Vec<ArrayRef>, Error> {
    table
        .key()
        .map_or(&[][..], |k| &k.levels[..])
        .iter()
        .map(|l| level_array(table, batch, l))
        .collect()
}

fn primary_arrays(table: &Table, batch: &RecordBatch) -> Vec<ArrayRef> {
    table
        .primary_key
        .iter()
        .map(|c| {
            batch
                .column_by_name(c)
                .expect("typed by the layout")
                .clone()
        })
        .collect()
}

fn level_name(l: &Level) -> String {
    match l {
        Level::Column(c) => c.clone(),
        Level::Part(p) => part_column(p),
        Level::Cell(c) => format!("the cell of {}", c.cell),
        Level::Stored(s) => s.store.clone(),
    }
}

/// One prefix value as a level's array of one row, or why it is not of the level's type.
fn one(table: &Table, l: &Level, v: &Value<'_>, dt: &DataType) -> Result<ArrayRef, Error> {
    let refuse = |why: &str| {
        Error::Refused(format!(
            "{}: the prefix's value for {} is {v:?}, {why}",
            table.name,
            level_name(l)
        ))
    };
    let fits = "which does not fit the level's type";
    Ok(match (v, dt) {
        (Value::Null, dt) => new_null_array(dt, 1),
        (Value::Int(i), DataType::Int16) => Arc::new(Int16Array::from(vec![
            i16::try_from(*i).map_err(|_| refuse(fits))?,
        ])),
        (Value::Int(i), DataType::Int32) => Arc::new(Int32Array::from(vec![
            i32::try_from(*i).map_err(|_| refuse(fits))?,
        ])),
        (Value::Int(i), DataType::Int64) => Arc::new(Int64Array::from(vec![
            i64::try_from(*i).map_err(|_| refuse(fits))?,
        ])),
        (Value::Text(s), DataType::Utf8) => Arc::new(StringArray::from(vec![*s])),
        (Value::Instant(t), DataType::Timestamp(..)) => {
            Arc::new(TimestampMicrosecondArray::from(vec![*t]).with_timezone(UTC))
        }
        (Value::Date(d), DataType::Date32) => Arc::new(Date32Array::from(vec![*d])),
        (Value::Bool(b), DataType::Boolean) => Arc::new(BooleanArray::from(vec![*b])),
        (Value::Wkb(b), DataType::Binary) => Arc::new(BinaryArray::from(vec![*b])),
        _ => return Err(refuse(fits)),
    })
}
