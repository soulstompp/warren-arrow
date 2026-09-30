//! The command line's pieces a binary built on this crate shares: a written table's arguments, a
//! question's conditions and how they are read, and the layout and part a command names.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use arrow::array::AsArray;
use arrow::compute::cast;
use arrow::datatypes::{DataType, Int64Type};

use crate::layout::{Part, Table};
use crate::read::{self, Term};
use crate::write::days_before_month;
use crate::{Error, Layout, read_source};

/// A question's conditions on one table. All of them hold.
#[derive(clap::Args)]
pub struct Terms {
    /// The month of an instant column is one of these, as COLUMN=V,V…
    #[arg(long = "month-in", value_name = "COLUMN=V,…")]
    pub month_in: Vec<String>,
    /// The weekday (1 for Monday to 7 for Sunday) of an instant column is one of these.
    #[arg(long = "weekday-in", value_name = "COLUMN=V,…")]
    pub weekday_in: Vec<String>,
    /// An instant column lies from the first day up to the second, in UTC, as COLUMN=FROM..TO with
    /// days written YYYY-MM-DD.
    #[arg(long, value_name = "COLUMN=FROM..TO")]
    pub within: Vec<String>,
    /// Two integer columns together are one of the pairs a Parquet file holds, as A,B=FILE.
    #[arg(long = "pairs-in", value_name = "A,B=FILE")]
    pub pairs_in: Vec<String>,
    /// A column is one of the values a Parquet file's column of the same name holds, whole numbers
    /// or texts, as COLUMN=FILE.
    #[arg(long = "in", value_name = "COLUMN=FILE")]
    pub one_of: Vec<String>,
}

/// A written table: the layout, the table's name, and the directory it was written into.
#[derive(clap::Args)]
pub struct Written {
    #[arg(long, value_name = "FILE")]
    pub layout: PathBuf,
    #[arg(long, value_name = "NAME")]
    pub table: String,
    /// The table's own directory, `<into>/<table>`.
    #[arg(long, value_name = "DIR")]
    pub dir: PathBuf,
}

/// The layout file at `path`, read and checked.
pub fn read_layout(path: &Path) -> Result<Layout, Error> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| Error::Refused(format!("{}: {e}", path.display())))?;
    Layout::parse(&text).map_err(|e| Error::Refused(format!("the layout: {e}")))
}

/// The table `name` of the layout.
pub fn table<'a>(layout: &'a Layout, name: &str) -> Result<&'a Table, Error> {
    layout
        .table(name)
        .ok_or_else(|| Error::Refused(format!("{name} is not a table of the layout")))
}

/// A part written as month:COLUMN, weekday:COLUMN or year:COLUMN.
pub fn parse_part(s: &str) -> Result<Part, Error> {
    match s.split_once(':') {
        Some(("month", c)) => Ok(Part::Month(c.into())),
        Some(("weekday", c)) => Ok(Part::Weekday(c.into())),
        Some(("year", c)) => Ok(Part::Year(c.into())),
        _ => Err(Error::Refused(format!(
            "{s}: a part is month:COLUMN, weekday:COLUMN or year:COLUMN"
        ))),
    }
}

fn split_eq(s: &str) -> Result<(&str, &str), Error> {
    s.split_once('=')
        .ok_or_else(|| Error::Refused(format!("{s}: expected COLUMN=VALUE")))
}

fn ints(s: &str) -> Result<Vec<i64>, Error> {
    s.split(',')
        .map(|v| {
            v.trim()
                .parse()
                .map_err(|_| Error::Refused(format!("{v}: not a whole number")))
        })
        .collect()
}

/// A day written YYYY-MM-DD, as microseconds since 1970 at its start in UTC.
fn day_micros(s: &str) -> Result<i64, Error> {
    let bad = || Error::Refused(format!("{s}: a day is written YYYY-MM-DD"));
    let mut it = s.split('-');
    let (Some(y), Some(m), Some(d), None) = (it.next(), it.next(), it.next(), it.next()) else {
        return Err(bad());
    };
    let (y, m, d): (i64, u32, i64) = (
        y.parse().map_err(|_| bad())?,
        m.parse().map_err(|_| bad())?,
        d.parse().map_err(|_| bad())?,
    );
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return Err(bad());
    }
    Ok((days_before_month(y, m) + d - 1) * 86_400_000_000)
}

/// The conditions a command was given, read.
pub fn parse_terms(t: &Terms) -> Result<Vec<Term>, Error> {
    let mut out = Vec::new();
    for s in &t.month_in {
        let (c, v) = split_eq(s)?;
        out.push(Term::PartIn {
            part: Part::Month(c.into()),
            values: ints(v)?,
        });
    }
    for s in &t.weekday_in {
        let (c, v) = split_eq(s)?;
        out.push(Term::PartIn {
            part: Part::Weekday(c.into()),
            values: ints(v)?,
        });
    }
    for s in &t.within {
        let (c, v) = split_eq(s)?;
        let (from, to) = v
            .split_once("..")
            .ok_or_else(|| Error::Refused(format!("{s}: expected COLUMN=FROM..TO")))?;
        out.push(Term::Within {
            column: c.into(),
            from: day_micros(from)?,
            to: day_micros(to)?,
        });
    }
    for s in &t.pairs_in {
        let (cols, file) = split_eq(s)?;
        let (a, b) = cols
            .split_once(',')
            .ok_or_else(|| Error::Refused(format!("{s}: expected A,B=FILE")))?;
        let batch = read_source(file.as_ref())?;
        let col = |name: &str| {
            batch
                .column_by_name(name)
                .cloned()
                .ok_or_else(|| Error::Refused(format!("{file} holds no column {name}")))
        };
        let ca = cast(&col(a)?, &DataType::Int64)?;
        let cb = cast(&col(b)?, &DataType::Int64)?;
        let (ca, cb) = (
            ca.as_primitive::<Int64Type>(),
            cb.as_primitive::<Int64Type>(),
        );
        let pairs: HashSet<(i64, i64)> = (0..batch.num_rows())
            .map(|i| (ca.value(i), cb.value(i)))
            .collect();
        out.push(Term::PairsIn {
            columns: (a.into(), b.into()),
            pairs,
        });
    }
    for s in &t.one_of {
        let (c, file) = split_eq(s)?;
        let batch = read_source(file.as_ref())?;
        let col = batch
            .column_by_name(c)
            .ok_or_else(|| Error::Refused(format!("{file} holds no column {c}")))?;
        let values = match col.data_type() {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32 => {
                let v = cast(col, &DataType::Int64)?;
                let v = v.as_primitive::<Int64Type>();
                read::Values::ints(v.iter().flatten().collect())
            }
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                let v = cast(col, &DataType::Utf8)?;
                let v = v.as_string::<i32>();
                read::Values::texts(v.iter().flatten().map(|x| x.as_bytes().to_vec()).collect())
            }
            other => {
                return Err(Error::Refused(format!(
                    "{file}: {c} holds {other}, and a column is held to whole numbers or texts"
                )));
            }
        };
        out.push(Term::In {
            column: c.into(),
            values,
        });
    }
    Ok(out)
}
