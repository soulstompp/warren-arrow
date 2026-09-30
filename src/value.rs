// Copyright (C) 2026 Kenneth Allen Flegal
// SPDX-License-Identifier: Apache-2.0

//! A column read by value, whatever Arrow type the source holds it in, and written in the type the
//! layout gives it. A value that does not fit the layout's type exactly is refused, never rounded.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BinaryBuilder, BooleanBuilder, Date32Builder, Int16Builder,
    Int32Builder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{
    DataType, Date32Type, Date64Type, Field, Int8Type, Int16Type, Int32Type, Int64Type, TimeUnit,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};

use crate::Error;
use crate::layout::{Column, Type};

/// The zone every instant is written in.
pub const UTC: &str = "UTC";

const MICROS_PER_DAY: i64 = 86_400_000_000;
const MILLIS_PER_DAY: i64 = 86_400_000;

/// One value of a row, as the layout's types see it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Value<'a> {
    Null,
    Int(i128),
    Text(&'a str),
    /// Microseconds since 1970-01-01T00:00:00Z.
    Instant(i64),
    /// Days since 1970-01-01.
    Date(i32),
    Bool(bool),
    /// A point as WKB, byte for byte.
    Wkb(&'a [u8]),
}

#[derive(Debug, Clone, Copy)]
enum Kind {
    I8,
    I16,
    I32,
    I64,
    U8,
    U16,
    U32,
    U64,
    Utf8,
    LargeUtf8,
    Utf8View,
    Timestamp(TimeUnit),
    Date32,
    Date64,
    Bool,
    Binary,
    LargeBinary,
    BinaryView,
}

/// A source column, read one value at a time as the layout column it is written to.
pub struct Reader<'a> {
    name: &'a str,
    array: &'a dyn Array,
    kind: Kind,
}

impl<'a> Reader<'a> {
    /// Checks that the source's Arrow type can hold the layout column's values.
    pub fn new(table: &str, col: &'a Column, array: &'a dyn Array) -> Result<Reader<'a>, Error> {
        let refuse = |why: String| Error::Refused(format!("{table}.{}: {why}", col.name));
        let dt = array.data_type();
        let kind = match (col.ty, dt) {
            (Type::Smallint | Type::Integer | Type::Bigint, DataType::Int8) => Kind::I8,
            (Type::Smallint | Type::Integer | Type::Bigint, DataType::Int16) => Kind::I16,
            (Type::Smallint | Type::Integer | Type::Bigint, DataType::Int32) => Kind::I32,
            (Type::Smallint | Type::Integer | Type::Bigint, DataType::Int64) => Kind::I64,
            (Type::Smallint | Type::Integer | Type::Bigint, DataType::UInt8) => Kind::U8,
            (Type::Smallint | Type::Integer | Type::Bigint, DataType::UInt16) => Kind::U16,
            (Type::Smallint | Type::Integer | Type::Bigint, DataType::UInt32) => Kind::U32,
            (Type::Smallint | Type::Integer | Type::Bigint, DataType::UInt64) => Kind::U64,
            (Type::Varchar | Type::Char | Type::Text, DataType::Utf8) => Kind::Utf8,
            (Type::Varchar | Type::Char | Type::Text, DataType::LargeUtf8) => Kind::LargeUtf8,
            (Type::Varchar | Type::Char | Type::Text, DataType::Utf8View) => Kind::Utf8View,
            (Type::Instant, DataType::Timestamp(unit, Some(tz))) => {
                if !matches!(tz.as_ref(), "UTC" | "+00:00" | "Etc/UTC" | "Z") {
                    return Err(refuse(format!(
                        "the source holds instants in the zone {tz}, and an instant is held in UTC"
                    )));
                }
                Kind::Timestamp(*unit)
            }
            (Type::Instant, DataType::Timestamp(_, None)) => {
                return Err(refuse(
                    "the source's timestamps name no zone, so nothing says they are UTC".into(),
                ));
            }
            (Type::Date, DataType::Date32) => Kind::Date32,
            (Type::Date, DataType::Date64) => Kind::Date64,
            (Type::Boolean, DataType::Boolean) => Kind::Bool,
            (Type::Geography, DataType::Binary) => Kind::Binary,
            (Type::Geography, DataType::LargeBinary) => Kind::LargeBinary,
            (Type::Geography, DataType::BinaryView) => Kind::BinaryView,
            (ty, dt) => {
                return Err(refuse(format!(
                    "the source holds {dt}, which is not a {}{}",
                    ty.name(),
                    no_type_for(dt)
                )));
            }
        };
        Ok(Reader {
            name: &col.name,
            array,
            kind,
        })
    }

    /// The value at `row`.
    pub fn get(&self, row: usize) -> Result<Value<'a>, Error> {
        if self.array.is_null(row) {
            return Ok(Value::Null);
        }
        let a = self.array;
        Ok(match self.kind {
            Kind::I8 => Value::Int(a.as_primitive::<Int8Type>().value(row).into()),
            Kind::I16 => Value::Int(a.as_primitive::<Int16Type>().value(row).into()),
            Kind::I32 => Value::Int(a.as_primitive::<Int32Type>().value(row).into()),
            Kind::I64 => Value::Int(a.as_primitive::<Int64Type>().value(row).into()),
            Kind::U8 => Value::Int(a.as_primitive::<UInt8Type>().value(row).into()),
            Kind::U16 => Value::Int(a.as_primitive::<UInt16Type>().value(row).into()),
            Kind::U32 => Value::Int(a.as_primitive::<UInt32Type>().value(row).into()),
            Kind::U64 => Value::Int(a.as_primitive::<UInt64Type>().value(row).into()),
            Kind::Utf8 => Value::Text(a.as_string::<i32>().value(row)),
            Kind::LargeUtf8 => Value::Text(a.as_string::<i64>().value(row)),
            Kind::Utf8View => Value::Text(a.as_string_view().value(row)),
            Kind::Timestamp(unit) => Value::Instant(self.micros(unit, row)?),
            Kind::Date32 => Value::Date(a.as_primitive::<Date32Type>().value(row)),
            Kind::Date64 => {
                let ms = a.as_primitive::<Date64Type>().value(row);
                if ms % MILLIS_PER_DAY != 0 {
                    return Err(self.inexact(format!("{ms} ms is not a whole day")));
                }
                Value::Date(
                    i32::try_from(ms / MILLIS_PER_DAY)
                        .map_err(|_| self.inexact(format!("{ms} ms is out of a date's range")))?,
                )
            }
            Kind::Bool => Value::Bool(a.as_boolean().value(row)),
            Kind::Binary => Value::Wkb(a.as_binary::<i32>().value(row)),
            Kind::LargeBinary => Value::Wkb(a.as_binary::<i64>().value(row)),
            Kind::BinaryView => Value::Wkb(a.as_binary_view().value(row)),
        })
    }

    fn micros(&self, unit: TimeUnit, row: usize) -> Result<i64, Error> {
        let a = self.array;
        let over = |v: i64| self.inexact(format!("{v} is out of an instant's range"));
        match unit {
            TimeUnit::Second => {
                let v = a.as_primitive::<TimestampSecondType>().value(row);
                v.checked_mul(1_000_000).ok_or_else(|| over(v))
            }
            TimeUnit::Millisecond => {
                let v = a.as_primitive::<TimestampMillisecondType>().value(row);
                v.checked_mul(1_000).ok_or_else(|| over(v))
            }
            TimeUnit::Microsecond => Ok(a.as_primitive::<TimestampMicrosecondType>().value(row)),
            TimeUnit::Nanosecond => {
                let v = a.as_primitive::<TimestampNanosecondType>().value(row);
                if v % 1_000 != 0 {
                    return Err(self.inexact(format!(
                        "{v} ns carries nanoseconds, and an instant holds microseconds"
                    )));
                }
                Ok(v / 1_000)
            }
        }
    }

    fn inexact(&self, why: String) -> Error {
        Error::Refused(format!("{}: {why}", self.name))
    }
}

/// Why a source type has no layout type at all, where that is the reason.
fn no_type_for(dt: &DataType) -> &'static str {
    match dt {
        DataType::Float16 | DataType::Float32 | DataType::Float64 | DataType::Decimal128(..) => {
            ", and format 2 has no type for a fraction"
        }
        DataType::List(_) | DataType::LargeList(_) | DataType::Struct(_) | DataType::Map(..) => {
            ", and format 2 has no type for a value made of values"
        }
        _ => "",
    }
}

/// The Arrow field a layout column is written as.
pub fn field(col: &Column) -> Field {
    let dt = match col.ty {
        Type::Smallint => DataType::Int16,
        Type::Integer => DataType::Int32,
        Type::Bigint => DataType::Int64,
        Type::Varchar | Type::Char | Type::Text => DataType::Utf8,
        Type::Instant => DataType::Timestamp(TimeUnit::Microsecond, Some(UTC.into())),
        Type::Date => DataType::Date32,
        Type::Boolean => DataType::Boolean,
        Type::Geography => DataType::Binary,
    };
    let field = Field::new(&col.name, dt, col.nullable);
    #[cfg(feature = "geo")]
    if col.ty == Type::Geography {
        return crate::geo::geography_field(field);
    }
    field
}

/// The source column written in the layout column's type, or the first value that does not fit.
pub fn convert(
    table: &str,
    col: &Column,
    source: &Reader<'_>,
    rows: usize,
) -> Result<ArrayRef, Error> {
    let refuse = |row: usize, why: String| {
        Error::Refused(format!("{table}.{} at row {row}: {why}", col.name))
    };
    let not_null = |row: usize| {
        refuse(
            row,
            "a null, and the layout says the column holds none".to_string(),
        )
    };
    macro_rules! ints {
        ($builder:ty, $t:ty) => {{
            let mut b = <$builder>::with_capacity(rows);
            for row in 0..rows {
                match source.get(row)? {
                    Value::Null if col.nullable => b.append_null(),
                    Value::Null => return Err(not_null(row)),
                    Value::Int(v) => b.append_value(<$t>::try_from(v).map_err(|_| {
                        refuse(row, format!("{v} does not fit a {}", col.ty.name()))
                    })?),
                    _ => unreachable!("checked by the reader"),
                }
            }
            Arc::new(b.finish()) as ArrayRef
        }};
    }
    Ok(match col.ty {
        Type::Smallint => ints!(Int16Builder, i16),
        Type::Integer => ints!(Int32Builder, i32),
        Type::Bigint => ints!(Int64Builder, i64),
        Type::Varchar | Type::Char | Type::Text => {
            let mut b = StringBuilder::with_capacity(rows, 0);
            for row in 0..rows {
                match source.get(row)? {
                    Value::Null if col.nullable => b.append_null(),
                    Value::Null => return Err(not_null(row)),
                    Value::Text(s) => {
                        if let Some(n) = col.length {
                            let chars = s.chars().count();
                            if chars > n as usize {
                                return Err(refuse(
                                    row,
                                    format!(
                                        "{chars} characters, and a {}({n}) holds at most {n}",
                                        col.ty.name()
                                    ),
                                ));
                            }
                        }
                        b.append_value(s);
                    }
                    _ => unreachable!("checked by the reader"),
                }
            }
            Arc::new(b.finish())
        }
        Type::Instant => {
            let mut b = TimestampMicrosecondBuilder::with_capacity(rows).with_timezone(UTC);
            for row in 0..rows {
                match source.get(row)? {
                    Value::Null if col.nullable => b.append_null(),
                    Value::Null => return Err(not_null(row)),
                    Value::Instant(v) => b.append_value(v),
                    _ => unreachable!("checked by the reader"),
                }
            }
            Arc::new(b.finish())
        }
        Type::Date => {
            let mut b = Date32Builder::with_capacity(rows);
            for row in 0..rows {
                match source.get(row)? {
                    Value::Null if col.nullable => b.append_null(),
                    Value::Null => return Err(not_null(row)),
                    Value::Date(v) => b.append_value(v),
                    _ => unreachable!("checked by the reader"),
                }
            }
            Arc::new(b.finish())
        }
        Type::Boolean => {
            let mut b = BooleanBuilder::with_capacity(rows);
            for row in 0..rows {
                match source.get(row)? {
                    Value::Null if col.nullable => b.append_null(),
                    Value::Null => return Err(not_null(row)),
                    Value::Bool(v) => b.append_value(v),
                    _ => unreachable!("checked by the reader"),
                }
            }
            Arc::new(b.finish())
        }
        Type::Geography => {
            let mut b = BinaryBuilder::with_capacity(rows, rows * 21);
            for row in 0..rows {
                match source.get(row)? {
                    Value::Null if col.nullable => b.append_null(),
                    Value::Null => return Err(not_null(row)),
                    Value::Wkb(v) => {
                        point(v).map_err(|why| refuse(row, why))?;
                        b.append_value(v);
                    }
                    _ => unreachable!("checked by the reader"),
                }
            }
            Arc::new(b.finish())
        }
    })
}

/// The year and month (1 to 12) of a day counted from 1970-01-01, in the proleptic Gregorian
/// calendar.
pub fn year_month(days: i64) -> (i64, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month)
}

/// The day an instant falls on, in UTC.
pub fn day_of(micros: i64) -> i64 {
    micros.div_euclid(MICROS_PER_DAY)
}

/// The month (1 to 12), the year, or the weekday (1 for Monday to 7 for Sunday) of a day counted
/// from 1970-01-01, which was a Thursday.
pub fn part_of_day(part: &crate::layout::Part, day: i64) -> i64 {
    use crate::layout::Part;
    match part {
        Part::Month(_) => i64::from(year_month(day).1),
        Part::Year(_) => year_month(day).0,
        Part::Weekday(_) => (day + 3).rem_euclid(7) + 1,
    }
}

/// The longitude and latitude of a WKB point, in degrees, or why the bytes are not a place: a
/// two-dimensional point of finite coordinates, longitude within -180 to 180 and latitude within
/// -90 to 90.
pub fn point(wkb: &[u8]) -> Result<(f64, f64), String> {
    if wkb.len() != 21 {
        return Err(format!(
            "{} bytes, and a two-dimensional WKB point is 21",
            wkb.len()
        ));
    }
    let little = match wkb[0] {
        0 => false,
        1 => true,
        b => return Err(format!("a WKB byte order of {b}, which is neither 0 nor 1")),
    };
    let word = |b: &[u8]| {
        let a: [u8; 4] = b.try_into().expect("four bytes");
        if little {
            u32::from_le_bytes(a)
        } else {
            u32::from_be_bytes(a)
        }
    };
    let float = |b: &[u8]| {
        let a: [u8; 8] = b.try_into().expect("eight bytes");
        if little {
            f64::from_le_bytes(a)
        } else {
            f64::from_be_bytes(a)
        }
    };
    let kind = word(&wkb[1..5]);
    if kind != 1 {
        return Err(format!(
            "WKB of type {kind}, and a place is a point (type 1)"
        ));
    }
    let (lon, lat) = (float(&wkb[5..13]), float(&wkb[13..21]));
    if !(lon.is_finite() && lat.is_finite()) {
        return Err(format!(
            "({lon}, {lat}) is not a place: its coordinates must be finite"
        ));
    }
    if !(-180.0..=180.0).contains(&lon) || !(-90.0..=90.0).contains(&lat) {
        return Err(format!(
            "({lon}, {lat}) is not a place: longitude runs from -180 to 180 and latitude from -90 to 90"
        ));
    }
    Ok((lon, lat))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn days_fall_in_their_calendar_month() {
        assert_eq!(year_month(0), (1970, 1));
        assert_eq!(year_month(-1), (1969, 12));
        assert_eq!(year_month(31), (1970, 2));
        // 2000-02-29 and 2000-03-01
        assert_eq!(year_month(11_016), (2000, 2));
        assert_eq!(year_month(11_017), (2000, 3));
        // 2015-12-31 and 2016-01-01
        assert_eq!(year_month(16_800), (2015, 12));
        assert_eq!(year_month(16_801), (2016, 1));
        assert_eq!(day_of(-1), -1);
        assert_eq!(day_of(MICROS_PER_DAY), 1);
    }
}
