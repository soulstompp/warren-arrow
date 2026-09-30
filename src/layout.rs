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
    /// A point on the globe, longitude then latitude in degrees, held as WKB. Read only with the
    /// geo feature.
    Geography,
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
            Type::Geography => "geography",
        }
    }
}

/// A level of a key: a column, the month, year or weekday of one, stored or not, or the geo cell of
/// a point.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum Level {
    Column(String),
    Part(Part),
    Cell(Cell),
    Stored(Stored),
}

/// A month, a year or a weekday of an instant, ordering the rows and written as a smallint column
/// named `store`, in place of [`part_column`]'s name. A trial spelling: format 2 does not have it.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Stored {
    pub month: Option<String>,
    pub year: Option<String>,
    pub weekday: Option<String>,
    /// The name of the column the part is written as.
    pub store: String,
}

impl Stored {
    /// A part written under [`part_column`]'s name.
    pub fn of(p: &Part) -> Stored {
        let c = Some(p.column().to_string());
        let (month, year, weekday) = match p {
            Part::Month(_) => (c, None, None),
            Part::Year(_) => (None, c, None),
            Part::Weekday(_) => (None, None, c),
        };
        Stored {
            month,
            year,
            weekday,
            store: part_column(p),
        }
    }

    /// The part it stores, or why it names none or more than one.
    pub fn part(&self) -> Result<Part, String> {
        match (&self.month, &self.year, &self.weekday) {
            (Some(c), None, None) => Ok(Part::Month(c.clone())),
            (None, Some(c), None) => Ok(Part::Year(c.clone())),
            (None, None, Some(c)) => Ok(Part::Weekday(c.clone())),
            _ => Err(format!(
                "stored level {} names no part, or more than one; it names one of month, year and weekday",
                self.store
            )),
        }
    }
}

/// The geo cell of a row's point, on a grid of `band`-degree latitude bands and `sector`-degree
/// longitude sectors. It orders the rows and is never written as a column. Read only with the geo
/// feature.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cell {
    /// The geography column the point is read from.
    pub cell: String,
    pub band: f64,
    pub sector: f64,
}

/// The month (1 to 12), the year or the weekday of an instant in UTC, or of a date. As a level it
/// orders the rows and is written as a smallint column of its own, named by [`part_column`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Part {
    Month(String),
    Year(String),
    /// The day of the week, 1 for Monday to 7 for Sunday.
    Weekday(String),
}

impl Part {
    /// The column the part is read from.
    pub fn column(&self) -> &str {
        match self {
            Part::Month(c) | Part::Year(c) | Part::Weekday(c) => c,
        }
    }

    /// The part's name: month, year or weekday.
    pub fn name(&self) -> &'static str {
        match self {
            Part::Month(_) => "month",
            Part::Year(_) => "year",
            Part::Weekday(_) => "weekday",
        }
    }
}

/// The column a key's part is written as: `<column>_month`, `<column>_year` or
/// `<column>_weekday`.
pub fn part_column(p: &Part) -> String {
    format!("{}_{}", p.column(), p.name())
}

impl Level {
    /// The columns the level is read from.
    pub fn columns(&self) -> Vec<&str> {
        match self {
            Level::Column(c) => vec![c],
            Level::Part(p) => vec![p.column()],
            Level::Cell(c) => vec![&c.cell],
            Level::Stored(s) => [&s.month, &s.year, &s.weekday]
                .into_iter()
                .flatten()
                .map(String::as_str)
                .collect(),
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

/// How a table is cut into pieces.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum Partitions {
    Range(Range),
    Phase(Phase),
}

/// A piece starting at every `every`th row in the order of `range`: each piece holds the rows from
/// its bound up to the next piece's, and a value is never split across two pieces.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Range {
    pub range: String,
    pub every: u64,
}

/// A piece for each value of a month, a year or a weekday, its directory named
/// `<column>_<part>=<value>`. A trial spelling: format 2 does not have it.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Phase {
    pub phase: Part,
}

impl Partitions {
    /// The range this table is cut by, if it is cut by one.
    pub fn range(&self) -> Option<&Range> {
        match self {
            Partitions::Range(r) => Some(r),
            Partitions::Phase(_) => None,
        }
    }

    /// The part this table is cut by, if it is cut by one.
    pub fn phase(&self) -> Option<&Part> {
        match self {
            Partitions::Range(_) => None,
            Partitions::Phase(p) => Some(&p.phase),
        }
    }
}

/// A piece directory's name, for the value `v` of a phase.
pub fn phase_piece_name(p: &Part, v: i64) -> String {
    format!("{}_{}={v}", p.column(), p.name())
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
                return refuse(format!(
                    "the layout file is format {n}; this reads format {FORMAT}"
                ));
            }
            Some(None) => return refuse("not a layout file: its format is not a whole number"),
            None => return refuse("not a layout file: it names no format"),
        }
        if value.get("hints").is_some() {
            return refuse("format 2 has no hints");
        }
        for t in value
            .get("tables")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
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

    /// The key's months, years and weekdays, in the key's order: the columns written beside the
    /// layout's own, each under its `store` name or [`part_column`]'s.
    pub fn stored(&self) -> Vec<Stored> {
        self.key()
            .map(|k| {
                k.levels
                    .iter()
                    .filter_map(|l| match l {
                        Level::Stored(s) => Some(s.clone()),
                        Level::Part(p) => Some(Stored::of(p)),
                        _ => None,
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Whether the key's first level is a month, a year or a weekday.
    pub fn led_by_part(&self) -> bool {
        matches!(
            self.key().and_then(|k| k.levels.first()),
            Some(Level::Part(_) | Level::Stored(_))
        )
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
            #[cfg(not(feature = "geo"))]
            if c.ty == Type::Geography {
                return Err(at(format!(
                    "{} is a geography, which is read only with the geo feature, and this was built without the geo feature",
                    c.name
                )));
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
            if col.ty == Type::Geography {
                return Err(at(format!(
                    "the primary key's {c} is a geography, and two spellings of one point are not one value"
                )));
            }
        }

        if let Some(k) = self.key() {
            name_ok(&format!("a key of {}", self.name), &k.name)?;
            if k.levels.is_empty() {
                return Err(at(format!("key {} has no level", k.name)));
            }
            let mut levels: Vec<&Level> = Vec::new();
            let mut stores: HashSet<String> = HashSet::new();
            for l in &k.levels {
                let written = match l {
                    Level::Stored(s) => {
                        s.part().map_err(|e| at(format!("key {}: {e}", k.name)))?;
                        Some(s.store.clone())
                    }
                    Level::Part(p) => Some(part_column(p)),
                    _ => None,
                };
                if let Some(store) = written {
                    name_ok(&format!("a stored level of {}", self.name), &store)?;
                    if self.column(&store).is_some() || !stores.insert(store.clone()) {
                        return Err(at(format!(
                            "key {}: the level written as {store} names a column the table already has",
                            k.name
                        )));
                    }
                }
                let mut cols = Vec::new();
                for c in l.columns() {
                    cols.push(
                        self.column(c)
                            .ok_or_else(|| at(format!("key {}: {c} is not a column", k.name)))?,
                    );
                }
                match l {
                    Level::Column(_) if cols[0].ty == Type::Geography => {
                        return Err(at(format!(
                            "key {}: {} is a geography, which orders rows only through its cell",
                            k.name, cols[0].name
                        )));
                    }
                    Level::Column(_) => {}
                    Level::Part(_) | Level::Stored(_) => {
                        if !matches!(cols[0].ty, Type::Instant | Type::Date) {
                            return Err(at(format!(
                                "key {}: {} is neither an instant nor a date",
                                k.name, cols[0].name
                            )));
                        }
                    }
                    Level::Cell(cell) => self.cell_ok(&k.name, cell, &cols)?,
                }
                if levels.contains(&l) {
                    return Err(at(format!("key {}: a level is named twice", k.name)));
                }
                levels.push(l);
            }
            for c in &k.covers {
                if self.column(c).is_none() {
                    return Err(at(format!(
                        "key {} covers {c}, which is not a column",
                        k.name
                    )));
                }
            }
        }

        if let Some(p) = self.partitions.as_ref().and_then(Partitions::phase) {
            let col = self.column(p.column()).ok_or_else(|| {
                at(format!(
                    "the pieces are cut by the {} of {}, which is not a column",
                    p.name(),
                    p.column()
                ))
            })?;
            if !matches!(col.ty, Type::Instant | Type::Date) {
                return Err(at(format!(
                    "the pieces are cut by the {} of {}, which is neither an instant nor a date",
                    p.name(),
                    col.name
                )));
            }
            if col.nullable {
                return Err(at(format!(
                    "the pieces are cut by the {} of {}, which may hold null; the layout must say \"null\": false",
                    p.name(),
                    col.name
                )));
            }
        }
        if let Some(p) = self.partitions.as_ref().and_then(Partitions::range) {
            let col = self.column(&p.range).ok_or_else(|| {
                at(format!(
                    "the partitions range over {}, which is not a column",
                    p.range
                ))
            })?;
            if col.ty == Type::Geography {
                return Err(at(format!(
                    "the partitions range over {}, a geography, which has no order of its own",
                    p.range
                )));
            }
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
        && b.iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'_');
    if ok {
        Ok(())
    } else {
        refuse(format!(
            "{what} is named {name:?}: a name is lower-case letters, digits and underscores, starts with a letter or an underscore, and has at most 63 characters"
        ))
    }
}

impl Table {
    /// Whether a cell level can be read: a geography column, on a grid that divides the globe evenly.
    #[cfg(feature = "geo")]
    fn cell_ok(&self, key: &str, cell: &Cell, cols: &[&Column]) -> Result<(), Error> {
        let at = |what: String| Error::Refused(format!("{}: key {key}: {what}", self.name));
        if cols[0].ty != Type::Geography {
            return Err(at(format!(
                "a cell is read from a geography column, and {} is a {}",
                cols[0].name,
                cols[0].ty.name()
            )));
        }
        crate::geo::Grid::new(cell.band, cell.sector).map_err(at)?;
        Ok(())
    }

    #[cfg(not(feature = "geo"))]
    fn cell_ok(&self, key: &str, _: &Cell, _: &[&Column]) -> Result<(), Error> {
        Err(Error::Refused(format!(
            "{}: key {key}: a cell level is read only with the geo feature, and this was built without the geo feature",
            self.name
        )))
    }
}
