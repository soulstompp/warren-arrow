//! The layout file, format 2: each table's columns, its primary key and its key. What Parquet has
//! no place for is refused when the file is read, with the reason.

use serde::Deserialize;
use serde_json::Value;
use std::collections::HashSet;

use crate::Error;

/// The layout file's format this crate reads.
pub const FORMAT: u32 = 2;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Layout {
    pub format: u32,
    /// The layout's name.
    pub name: String,
    pub tables: Vec<Table>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    /// Unique on every row. With no key, the rows are written in its order.
    #[serde(default)]
    pub primary_key: Vec<String>,
    /// At most one: the order the rows are written in.
    #[serde(default)]
    pub keys: Vec<Key>,
    /// Refused: on 3NF a decided value is read through the join.
    #[serde(default)]
    pub decides: Vec<Value>,
    /// The table cut into pieces by ranges of one column.
    pub partitions: Option<Partitions>,
    /// Refused: a stored fold is a SELECT, and there is no engine here to run it.
    pub fold: Option<Value>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: Type,
    /// A `varchar`'s or a `char`'s length in characters.
    pub length: Option<u32>,
    /// Whether the column may hold NULL; it may unless the layout says `"null": false`.
    #[serde(rename = "null", default = "may_be_null")]
    pub nullable: bool,
}

fn may_be_null() -> bool {
    true
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Type {
    Smallint,
    Integer,
    Bigint,
    Varchar,
    /// Text of at most its length.
    Char,
    Text,
    /// A point in time, held in UTC to the microsecond.
    Instant,
    Date,
    Boolean,
}

impl Type {
    pub fn name(self) -> &'static str {
        match self {
            Type::Smallint => "smallint",
            Type::Integer => "integer",
            Type::Bigint => "bigint",
            Type::Varchar => "varchar",
            Type::Char => "char",
            Type::Text => "text",
            Type::Instant => "instant",
            Type::Date => "date",
            Type::Boolean => "boolean",
        }
    }
}

/// A level of a key: a column, or the month or year of one.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize)]
#[serde(untagged)]
pub enum Level {
    Column(String),
    Part(Part),
}

/// The month (1 to 12) or the year of an instant in UTC, or of a date. It orders the rows and is
/// never written as a column.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Part {
    Month(String),
    Year(String),
}

impl Level {
    /// The column the level is read from.
    pub fn column(&self) -> &str {
        match self {
            Level::Column(c) | Level::Part(Part::Month(c)) | Level::Part(Part::Year(c)) => c,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Key {
    pub name: String,
    pub levels: Vec<Level>,
    /// Every column of a Parquet file is read beside its key, so these are checked to be columns
    /// and nothing more is done with them.
    #[serde(default)]
    pub covers: Vec<String>,
}

/// A piece starting at every `every`th row in the order of `range`: each piece holds the rows from
/// its bound up to the next piece's, and a value is never split across two pieces.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Partitions {
    pub range: String,
    pub every: u64,
}

fn refuse<T>(msg: impl Into<String>) -> Result<T, Error> {
    Err(Error::Refused(msg.into()))
}

impl Layout {
    /// Reads a layout file's text and checks it.
    pub fn parse(text: &str) -> Result<Layout, Error> {
        let value: Value = serde_json::from_str(text)
            .map_err(|e| Error::Refused(format!("not a layout file: {e}")))?;
        match value.get("format").map(Value::as_u64) {
            Some(Some(n)) if n == u64::from(FORMAT) => {}
            Some(Some(n)) => {
                return refuse(format!("the layout file is format {n}; this reads format {FORMAT}"));
            }
            Some(None) => return refuse("not a layout file: its format is not a whole number"),
            None => return refuse("not a layout file: it names no format"),
        }
        if value.get("hints").is_some() {
            return refuse("format 2 has no hints");
        }
        for t in value.get("tables").and_then(Value::as_array).into_iter().flatten() {
            let name = t.get("name").and_then(Value::as_str).unwrap_or("a table");
            for part in ["pieces", "postgres", "mysql"] {
                if t.get(part).is_some() {
                    return refuse(format!("{name}: format 2 has no {part}"));
                }
            }
        }
        let layout: Layout = serde_json::from_value(value)
            .map_err(|e| Error::Refused(format!("not a layout file: {e}")))?;
        layout.checked()
    }

    fn checked(self) -> Result<Layout, Error> {
        name_ok("the layout", &self.name)?;
        let mut names = HashSet::new();
        for t in &self.tables {
            name_ok("a table", &t.name)?;
            if !names.insert(t.name.as_str()) {
                return refuse(format!("table {} is named twice", t.name));
            }
            t.check()?;
        }
        Ok(self)
    }

    pub fn table(&self, name: &str) -> Option<&Table> {
        self.tables.iter().find(|t| t.name == name)
    }
}

impl Table {
    pub fn column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// The table's one key, if it has one.
    pub fn key(&self) -> Option<&Key> {
        self.keys.first()
    }

    fn check(&self) -> Result<(), Error> {
        let at = |what: String| Error::Refused(format!("{}: {what}", self.name));
        if self.columns.is_empty() {
            return Err(at("it has no columns".into()));
        }
        let mut seen = HashSet::new();
        for c in &self.columns {
            name_ok(&format!("a column of {}", self.name), &c.name)?;
            if !seen.insert(c.name.as_str()) {
                return Err(at(format!("{} is named twice", c.name)));
            }
            match (c.ty, c.length) {
                (Type::Varchar | Type::Char, Some(n)) if n > 0 => {}
                (Type::Varchar | Type::Char, _) => {
                    return Err(at(format!("{} has no length", c.name)));
                }
                (_, Some(_)) => {
                    return Err(at(format!(
                        "{} has a length but is neither a varchar nor a char",
                        c.name
                    )));
                }
                _ => {}
            }
        }

        if !self.decides.is_empty() {
            return Err(at(
                "it declares decisions; on 3NF a decided value is read through the join, never written onto the row"
                    .into(),
            ));
        }
        if self.fold.is_some() {
            return Err(at(
                "it is a stored fold; a fold is a SELECT, and there is no engine here to run it, so write the fold's rows as a table of their own"
                    .into(),
            ));
        }
        if self.keys.len() > 1 {
            return Err(at(format!(
                "it has {} keys; the rows of a Parquet file are written in one order, and another order is another copy of the rows",
                self.keys.len()
            )));
        }

        let mut pk = HashSet::new();
        for c in &self.primary_key {
            let col = self
                .column(c)
                .ok_or_else(|| at(format!("the primary key's {c} is not a column")))?;
            if !pk.insert(c) {
                return Err(at(format!("the primary key names {c} twice")));
            }
            if col.nullable {
                return Err(at(format!(
                    "the primary key's {c} may hold null; the layout must say \"null\": false"
                )));
            }
        }

        if let Some(k) = self.key() {
            name_ok(&format!("a key of {}", self.name), &k.name)?;
            if k.levels.is_empty() {
                return Err(at(format!("key {} has no level", k.name)));
            }
            let mut parts = HashSet::new();
            for l in &k.levels {
                let col = self
                    .column(l.column())
                    .ok_or_else(|| at(format!("key {}: {} is not a column", k.name, l.column())))?;
                if matches!(l, Level::Part(_)) && !matches!(col.ty, Type::Instant | Type::Date) {
                    return Err(at(format!(
                        "key {}: {} is neither an instant nor a date",
                        k.name, col.name
                    )));
                }
                if !parts.insert(l.clone()) {
                    return Err(at(format!("key {}: a level is named twice", k.name)));
                }
            }
            for c in &k.covers {
                if self.column(c).is_none() {
                    return Err(at(format!("key {} covers {c}, which is not a column", k.name)));
                }
            }
        }

        if let Some(p) = &self.partitions {
            let col = self.column(&p.range).ok_or_else(|| {
                at(format!("the partitions range over {}, which is not a column", p.range))
            })?;
            if col.nullable {
                return Err(at(format!(
                    "the partitions range over {}, which may hold null, and a null has no place among the ranges; the layout must say \"null\": false",
                    p.range
                )));
            }
            if p.every == 0 {
                return Err(at("a piece every 0 rows".into()));
            }
        }
        Ok(())
    }
}

/// A name is lower-case letters, digits and underscores, starts with a letter or an underscore,
/// and has at most 63 characters.
fn name_ok(what: &str, name: &str) -> Result<(), Error> {
    let b = name.as_bytes();
    let ok = !b.is_empty()
        && b.len() <= 63
        && (b[0].is_ascii_lowercase() || b[0] == b'_')
        && b.iter().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'_');
    if ok {
        Ok(())
    } else {
        refuse(format!(
            "{what} is named {name:?}: a name is lower-case letters, digits and underscores, starts with a letter or an underscore, and has at most 63 characters"
        ))
    }
}
