// Copyright (C) 2026 Kenneth Allen Flegal
// SPDX-License-Identifier: Apache-2.0

//! A month or a weekday as a level: written as a column of its own with a row group per value, as a
//! piece per value, or stored under a name of its own; and what a reader keeps for a condition on it.

use std::collections::HashSet;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{AsArray, Int32Array, Int64Array, RecordBatch, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Int16Type, Schema, TimeUnit};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::statistics::Statistics;
use warren_arrow::layout::Part;
use warren_arrow::read::{self, Term};
use warren_arrow::value::{day_of, part_of_day};
use warren_arrow::write::{ROWS_FILE, days_before_month};
use warren_arrow::{Layout, Options, read_source, write_table};

const MICROS_PER_DAY: i64 = 86_400_000_000;

fn layout(key: &str, partitions: &str) -> Layout {
    Layout::parse(&format!(
        r#"{{"format": 2, "name": "shop", "tables": [{{
            "name": "buy",
            "columns": [
                {{"name": "buy_id", "type": "bigint", "null": false}},
                {{"name": "builder_id", "type": "integer", "null": false}},
                {{"name": "row_no", "type": "integer", "null": false}},
                {{"name": "placed_at", "type": "instant", "null": false}}
            ],
            "primary_key": ["buy_id"]{key}{partitions}
        }}]}}"#
    ))
    .expect("the test layout reads")
}

const STORED: &str = r#", "keys": [{"name": "k", "levels": [{"month": "placed_at", "store": "placed_month"}, "placed_at"]}]"#;
const MONTH_FIRST: &str =
    r#", "keys": [{"name": "k", "levels": [{"month": "placed_at"}, "placed_at"]}]"#;
const ROW_FIRST: &str =
    r#", "keys": [{"name": "k", "levels": ["builder_id", "row_no", "placed_at"]}]"#;
const BY_MONTH: &str = r#", "partitions": {"phase": {"month": "placed_at"}}"#;

fn day(y: i64, m: u32, d: i64) -> i64 {
    days_before_month(y, m) + d - 1
}

/// Purchases over several years, on every month and weekday, in no particular order.
fn source(n: i64) -> RecordBatch {
    let ids: Vec<i64> = (0..n).map(|i| (i * 37) % n + 1).collect();
    let start = day(2009, 11, 3);
    let instants: Vec<i64> = ids
        .iter()
        .map(|i| (start + (i * 13) % 1500) * MICROS_PER_DAY + (i % 24) * 3_600_000_000)
        .collect();
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("buy_id", DataType::Int64, false),
            Field::new("builder_id", DataType::Int32, false),
            Field::new("row_no", DataType::Int32, false),
            Field::new(
                "placed_at",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
        ])),
        vec![
            Arc::new(Int64Array::from(ids.clone())),
            Arc::new(Int32Array::from_iter_values(
                ids.iter().map(|i| (*i % 7) as i32),
            )),
            Arc::new(Int32Array::from_iter_values(
                ids.iter().map(|i| (*i % 5) as i32),
            )),
            Arc::new(TimestampMicrosecondArray::from(instants).with_timezone("UTC")),
        ],
    )
    .expect("a batch")
}

fn month_of(t: i64) -> i64 {
    part_of_day(&Part::Month(String::new()), day_of(t))
}

fn row_groups(path: &Path) -> Vec<RecordBatch> {
    let reader = SerializedFileReader::new(File::open(path).expect("rows")).expect("parquet");
    let n = reader.metadata().num_row_groups();
    (0..n)
        .map(|g| {
            let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                File::open(path).expect("rows"),
            )
            .expect("parquet")
            .with_row_groups(vec![g]);
            let schema = builder.schema().clone();
            let batches: Vec<_> = builder
                .build()
                .expect("reader")
                .collect::<Result<_, _>>()
                .expect("batches");
            arrow::compute::concat_batches(&schema, &batches).expect("concat")
        })
        .collect()
}

fn instants(b: &RecordBatch) -> Vec<i64> {
    b.column_by_name("placed_at")
        .expect("placed_at")
        .as_primitive::<arrow::datatypes::TimestampMicrosecondType>()
        .values()
        .to_vec()
}

#[test]
fn a_weekday_counts_from_monday_as_1() {
    let wd = |y, m, d| part_of_day(&Part::Weekday(String::new()), day(y, m, d));
    assert_eq!(wd(1970, 1, 1), 4, "a Thursday");
    assert_eq!(wd(2026, 9, 30), 3, "a Wednesday");
    assert_eq!(wd(2024, 12, 29), 7, "a Sunday");
    assert_eq!(wd(2024, 12, 30), 1, "a Monday");
    assert_eq!(wd(1969, 12, 31), 3, "the day before 1970 is a Wednesday");
}

#[test]
fn a_stored_month_is_each_rows_month_and_leads_the_order() {
    let dir = tempfile::tempdir().expect("a directory");
    let l = layout(STORED, "");
    let src = source(500);
    let w = write_table(&l, "buy", &src, dir.path(), &Options::default()).expect("written");
    assert_eq!(w.source, w.written);
    let back = read_source(&w.dir.join(&w.pieces[0].name).join(ROWS_FILE)).expect("back");
    let stored = back
        .column_by_name("placed_month")
        .expect("the stored month is a column")
        .as_primitive::<Int16Type>()
        .clone();
    let months: Vec<i64> = instants(&back).into_iter().map(month_of).collect();
    for (i, m) in months.iter().enumerate() {
        assert_eq!(i64::from(stored.value(i)), *m, "row {i}");
    }
    assert!(
        months.windows(2).all(|p| p[0] <= p[1]),
        "rows in month order"
    );
    assert!(
        months.iter().collect::<HashSet<_>>().len() == 12,
        "every month is there"
    );
}

#[test]
fn a_month_level_is_written_as_a_column_and_cuts_a_row_group_per_month() {
    let dir = tempfile::tempdir().expect("a directory");
    let l = layout(MONTH_FIRST, "");
    let src = source(500);
    let w = write_table(&l, "buy", &src, dir.path(), &Options::default()).expect("written");
    assert_eq!(w.source, w.written);
    let path = w.dir.join(&w.pieces[0].name).join(ROWS_FILE);
    let back = read_source(&path).expect("back");
    let written = back
        .column_by_name("placed_at_month")
        .expect("the month is written as placed_at_month")
        .as_primitive::<Int16Type>()
        .clone();
    for (i, m) in instants(&back).into_iter().map(month_of).enumerate() {
        assert_eq!(i64::from(written.value(i)), m, "row {i}");
    }
    let groups = row_groups(&path);
    assert_eq!(groups.len(), 12, "a row group per month");
    for (g, b) in groups.iter().enumerate() {
        let months: HashSet<i64> = instants(b).into_iter().map(month_of).collect();
        assert_eq!(months.len(), 1, "row group {g} holds {months:?}");
    }
    let reader = SerializedFileReader::new(File::open(&path).expect("rows")).expect("parquet");
    let schema = reader.metadata().file_metadata().schema_descr();
    let i = (0..schema.num_columns())
        .find(|i| schema.column(*i).name() == "placed_at_month")
        .expect("the written month");
    for rg in reader.metadata().row_groups() {
        match rg.column(i).statistics() {
            Some(Statistics::Int32(s)) => assert_eq!(s.min_opt(), s.max_opt(), "one month"),
            other => panic!("the written month has no statistics: {other:?}"),
        }
    }
}

#[test]
fn a_month_level_whose_column_name_the_table_already_has_is_refused() {
    let e = Layout::parse(
        r#"{"format": 2, "name": "shop", "tables": [{
            "name": "buy",
            "columns": [
                {"name": "buy_id", "type": "bigint", "null": false},
                {"name": "placed_at", "type": "instant", "null": false},
                {"name": "placed_at_month", "type": "smallint", "null": false}
            ],
            "primary_key": ["buy_id"],
            "keys": [{"name": "k", "levels": [{"month": "placed_at"}, "placed_at"]}]
        }]}"#,
    )
    .expect_err("refused");
    assert!(e.to_string().contains("placed_at_month"), "{e}");
}

#[test]
fn a_piece_per_month_holds_exactly_that_month_and_is_named_for_it() {
    let dir = tempfile::tempdir().expect("a directory");
    let l = layout(MONTH_FIRST, BY_MONTH);
    let w = write_table(&l, "buy", &source(500), dir.path(), &Options::default()).expect("written");
    assert_eq!(w.pieces.len(), 12);
    let mut total = 0;
    for p in &w.pieces {
        let v = p.value.expect("a phase piece has a value");
        assert_eq!(p.name, format!("placed_at_month={v}"));
        let back = read_source(&w.dir.join(&p.name).join(ROWS_FILE)).expect("back");
        let months: HashSet<i64> = instants(&back).into_iter().map(month_of).collect();
        assert_eq!(months, HashSet::from([v]), "{}", p.name);
        total += back.num_rows();
    }
    assert_eq!(total, 500, "the pieces cover the table");
}

#[test]
fn a_row_group_per_block_holds_one_value_of_the_first_level() {
    let dir = tempfile::tempdir().expect("a directory");
    let l = layout(STORED, "");
    let opts = Options {
        row_group_per_block: true,
        ..Options::default()
    };
    let w = write_table(&l, "buy", &source(500), dir.path(), &opts).expect("written");
    let groups = row_groups(&w.dir.join(&w.pieces[0].name).join(ROWS_FILE));
    assert_eq!(groups.len(), 12);
    for (g, b) in groups.iter().enumerate() {
        let months: HashSet<i64> = instants(b).into_iter().map(month_of).collect();
        assert_eq!(months.len(), 1, "row group {g} holds {months:?}");
    }
    let reader = SerializedFileReader::new(
        File::open(w.dir.join(&w.pieces[0].name).join(ROWS_FILE)).expect("rows"),
    )
    .expect("parquet");
    let schema = reader.metadata().file_metadata().schema_descr();
    let i = (0..schema.num_columns())
        .find(|i| schema.column(*i).name() == "placed_month")
        .expect("the stored column");
    for rg in reader.metadata().row_groups() {
        match rg.column(i).statistics() {
            Some(Statistics::Int32(s)) => assert_eq!(s.min_opt(), s.max_opt(), "one month"),
            other => panic!("the stored month has no statistics: {other:?}"),
        }
    }
}

fn december() -> Vec<Term> {
    vec![Term::PartIn {
        part: Part::Month("placed_at".into()),
        values: vec![12],
    }]
}

fn decembers(src: &RecordBatch) -> u64 {
    instants(src)
        .into_iter()
        .filter(|t| month_of(*t) == 12)
        .count() as u64
}

#[test]
fn what_a_reader_keeps_holds_every_row_the_question_keeps_and_what_it_skips_holds_none() {
    let src = source(700);
    let n = decembers(&src);
    assert!(n > 0, "the source holds Decembers");
    let cases = [
        ("stored", layout(STORED, ""), true),
        ("stored, a row group per block", layout(STORED, ""), true),
        ("a piece per month", layout(MONTH_FIRST, BY_MONTH), true),
        ("a month level, as written", layout(MONTH_FIRST, ""), true),
        ("row order", layout(ROW_FIRST, ""), false),
    ];
    for (i, (what, l, skips)) in cases.iter().enumerate() {
        let dir = tempfile::tempdir().expect("a directory");
        let opts = Options {
            max_row_group_rows: Some(50),
            row_group_per_block: i == 1,
        };
        let w = write_table(l, "buy", &src, dir.path(), &opts).expect("written");
        let t = l.table("buy").expect("buy");
        let k = read::kept(t, &w.dir, &december()).expect("kept");
        let h = read::held(t, &w.dir, &december(), &k).expect("held");
        assert_eq!(
            h.in_kept, n,
            "{what}: the kept row groups hold every December"
        );
        assert_eq!(h.in_skipped, 0, "{what}: the skipped row groups hold none");
        assert_eq!(
            k.row_groups_kept < k.row_groups,
            *skips,
            "{what}: {} of {} row groups kept",
            k.row_groups_kept,
            k.row_groups
        );
    }
}

#[test]
fn a_stored_level_that_names_no_part_or_a_column_already_there_is_refused() {
    for (key, why) in [
        (
            r#", "keys": [{"name": "k", "levels": [{"store": "m"}]}]"#,
            "names no part",
        ),
        (
            r#", "keys": [{"name": "k", "levels": [{"month": "placed_at", "store": "row_no"}]}]"#,
            "already has",
        ),
    ] {
        let text = format!(
            r#"{{"format": 2, "name": "shop", "tables": [{{"name": "buy", "columns": [{{"name": "row_no", "type": "integer"}}, {{"name": "placed_at", "type": "instant"}}]{key}}}]}}"#
        );
        match Layout::parse(&text) {
            Err(warren_arrow::Error::Refused(r)) => assert!(r.contains(why), "{r}"),
            other => panic!("not refused: {other:?}"),
        }
    }
}

#[test]
fn pieces_by_a_phase_are_refused_on_a_column_that_may_hold_null() {
    let text = r#"{"format": 2, "name": "shop", "tables": [{"name": "buy", "columns": [{"name": "placed_at", "type": "instant"}], "partitions": {"phase": {"month": "placed_at"}}}]}"#;
    match Layout::parse(text) {
        Err(warren_arrow::Error::Refused(r)) => assert!(r.contains("may hold null"), "{r}"),
        other => panic!("not refused: {other:?}"),
    }
}

#[test]
fn the_stored_columns_are_not_in_the_certificate() {
    let dir = tempfile::tempdir().expect("a directory");
    let src = source(300);
    let plain = write_table(
        &layout(MONTH_FIRST, ""),
        "buy",
        &src,
        &dir.path().join("a"),
        &Options::default(),
    )
    .expect("written");
    let stored = write_table(
        &layout(STORED, ""),
        "buy",
        &src,
        &dir.path().join("b"),
        &Options::default(),
    )
    .expect("written");
    assert_eq!(
        plain.written, stored.written,
        "the same rows, whatever is stored beside them"
    );
}
