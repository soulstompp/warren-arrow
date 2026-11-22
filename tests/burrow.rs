// Copyright (C) 2026 Kenneth Allen Flegal
// SPDX-License-Identifier: Apache-2.0

//! What a burrow must hold: the rows written to Parquet for the same source, the same rows however
//! they were added, nothing added out of order or twice, and exactly the rows a read asks for.

use std::sync::Arc;

use arrow::array::{
    AsArray, Int32Array, Int64Array, LargeStringArray, RecordBatch, TimestampNanosecondArray,
    UInt32Array,
};
use arrow::compute::{concat_batches, take};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit, TimestampMicrosecondType};
use warren_arrow::burrow::Burrow;
use warren_arrow::layout::{Part, Table};
use warren_arrow::read::{Term, Values, holds};
use warren_arrow::value::{self, Reader, Value};
use warren_arrow::{Certificate, Error, Layout, Options, read_source, write_table};

const LAYOUT: &str = r#"{
  "format": 2,
  "name": "shop",
  "tables": [{
    "name": "buy",
    "columns": [
      {"name": "buy_id", "type": "bigint", "null": false},
      {"name": "set_num", "type": "varchar", "length": 16, "null": false},
      {"name": "root_id", "type": "integer", "null": false},
      {"name": "placed_at", "type": "instant", "null": false},
      {"name": "qty", "type": "integer"}
    ],
    "primary_key": ["buy_id"],
    "keys": [{"name": "buy_key", "levels": ["root_id", "set_num", {"month": "placed_at"}], "covers": ["qty"]}],
    "partitions": {"range": "root_id", "every": 7}
  }]
}"#;

const MICROS_PER_DAY: i64 = 86_400_000_000;

fn layout() -> Arc<Layout> {
    Arc::new(Layout::parse(LAYOUT).expect("the test layout reads"))
}

/// Rows in no particular order, with repeated roots and sets, months across two years, and some
/// quantities missing. The source's types differ from the layout's where they can be read exactly.
fn source(n: u32) -> RecordBatch {
    let ids: Vec<u32> = (0..n).map(|i| (i * 37) % n + 1).collect();
    let roots: Vec<i64> = ids
        .iter()
        .map(|i| [3, 1, 1, 4, 1, 5, 9, 2, 6][(*i % 9) as usize])
        .collect();
    let sets: Vec<String> = ids
        .iter()
        .map(|i| format!("{}-{}", 10 + i % 4, i % 3))
        .collect();
    let placed: Vec<i64> = ids
        .iter()
        .map(|i| {
            (16_000 + i64::from(*i % 23) * 29) * MICROS_PER_DAY * 1_000 + i64::from(*i) * 1_000
        })
        .collect();
    let qty: Vec<Option<i32>> = ids
        .iter()
        .map(|i| (i % 5 != 0).then_some(*i as i32 % 7))
        .collect();
    let schema = Schema::new(vec![
        Field::new("buy_id", DataType::UInt32, false),
        Field::new("set_num", DataType::LargeUtf8, false),
        Field::new("root_id", DataType::Int64, false),
        Field::new(
            "placed_at",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("qty", DataType::Int32, true),
    ]);
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(UInt32Array::from(ids)),
            Arc::new(LargeStringArray::from(
                sets.iter().map(String::as_str).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(roots)),
            Arc::new(TimestampNanosecondArray::from(placed).with_timezone("UTC")),
            Arc::new(Int32Array::from(qty)),
        ],
    )
    .expect("a batch")
}

fn rows(batch: &RecordBatch, indices: &[u32]) -> RecordBatch {
    let idx = UInt32Array::from(indices.to_vec());
    let columns = batch
        .columns()
        .iter()
        .map(|c| take(c.as_ref(), &idx, None).expect("take"))
        .collect();
    RecordBatch::try_new(batch.schema(), columns).expect("a batch")
}

/// Rows `from..to` of a batch, last first.
fn reversed(batch: &RecordBatch, from: u32, to: u32) -> RecordBatch {
    rows(batch, &(from..to).rev().collect::<Vec<_>>())
}

fn all(batches: &[RecordBatch]) -> RecordBatch {
    let schema = batches
        .first()
        .map(|b| b.schema())
        .expect("at least one batch");
    concat_batches(&schema, batches).expect("the batches concatenate")
}

/// Each row's values as the layout reads them, in the batches' order.
fn values(table: &Table, batches: &[RecordBatch]) -> Vec<Vec<String>> {
    let mut out = Vec::new();
    for b in batches {
        let readers: Vec<Reader<'_>> = table
            .columns
            .iter()
            .map(|c| {
                Reader::new(
                    &table.name,
                    c,
                    b.column_by_name(&c.name).expect("the column").as_ref(),
                )
                .expect("the column reads")
            })
            .collect();
        for r in 0..b.num_rows() {
            out.push(
                readers
                    .iter()
                    .map(|x| format!("{:?}", x.get(r).expect("a value")))
                    .collect(),
            );
        }
    }
    out
}

fn whole(b: &Burrow) -> Vec<RecordBatch> {
    b.read(&[], &[]).expect("every row reads")
}

/// Every row held, in order, as one batch.
fn held(b: &Burrow) -> RecordBatch {
    all(&whole(b))
}

fn refused<T>(r: Result<T, Error>) -> String {
    match r {
        Err(Error::Refused(why)) => why,
        Err(e) => panic!("failed rather than refused: {e}"),
        Ok(_) => panic!("not refused"),
    }
}

#[test]
fn a_burrow_holds_the_rows_write_table_writes_in_the_same_order() {
    let l = layout();
    let s = source(60);
    let b = Burrow::begin(&l, "buy", &s).expect("the burrow begins");

    let dir = tempfile::tempdir().expect("a directory");
    let w = write_table(&l, "buy", &s, dir.path(), &Options::default()).expect("buy is written");
    let back: Vec<RecordBatch> = w
        .pieces
        .iter()
        .map(|p| read_source(&w.dir.join(&p.name).join("rows.parquet")).expect("a piece reads"))
        .collect();

    let table = b.table();
    assert_eq!(values(table, &whole(&b)), values(table, &back));
    assert_eq!(b.certificate(), w.written);
    assert_eq!(b.len(), 60);

    // The rows a burrow holds are a source as they are: the layout's columns, written again whole.
    let again = tempfile::tempdir().expect("a directory");
    let w2 = write_table(&l, "buy", &held(&b), again.path(), &Options::default())
        .expect("a burrow's rows are written");
    assert_eq!(w2.written, w.written);
    let names: Vec<&str> = b.table().columns.iter().map(|c| c.name.as_str()).collect();
    let held_names: Vec<String> = held(&b)
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(held_names, names);

    // Seeded: one value changed in a copy of the rows held, and both comparisons see it.
    let held = all(&whole(&b));
    let mut changed: Vec<i64> = held
        .column_by_name("buy_id")
        .unwrap()
        .as_primitive::<arrow::datatypes::Int64Type>()
        .values()
        .to_vec();
    changed[17] += 1_000;
    let seeded = RecordBatch::try_new(
        held.schema(),
        held.columns()
            .iter()
            .enumerate()
            .map(|(i, c)| {
                if held.schema().field(i).name() == "buy_id" {
                    Arc::new(Int64Array::from(changed.clone())) as _
                } else {
                    c.clone()
                }
            })
            .collect(),
    )
    .unwrap();
    assert_ne!(
        values(table, std::slice::from_ref(&seeded)),
        values(table, &back)
    );
    assert_ne!(Certificate::of(table, &seeded).unwrap(), w.written);
}

#[test]
fn rows_added_in_order_are_the_rows_begun_with_at_once() {
    let l = layout();
    let at_once = Burrow::begin(&l, "buy", &source(60)).expect("the burrow begins");
    let held = held(&at_once);

    // Each add's rows arrive last first; the burrow orders them among themselves.
    let mut added = Burrow::begin(&l, "buy", &held.slice(0, 0)).expect("an empty burrow");
    assert!(added.is_empty());
    for (from, to) in [(0, 13), (13, 41), (41, 60)] {
        added.add(&reversed(&held, from, to)).expect("in order");
    }
    let table = added.table();
    assert_eq!(
        values(table, &whole(&added)),
        values(table, &whole(&at_once))
    );
    assert_eq!(added.certificate(), at_once.certificate());

    // Seeded: one add's rows dropped.
    let mut dropped = Burrow::begin(&l, "buy", &reversed(&held, 0, 13)).unwrap();
    dropped.add(&reversed(&held, 41, 60)).unwrap();
    assert_ne!(dropped.certificate(), at_once.certificate());
    assert_ne!(
        values(table, &whole(&dropped)),
        values(table, &whole(&at_once))
    );
}

#[test]
fn rows_that_do_not_come_after_those_held_are_refused_and_nothing_changes() {
    let l = layout();
    let held = held(&Burrow::begin(&l, "buy", &source(60)).unwrap());
    let mut b = Burrow::begin(&l, "buy", &held.slice(30, 30)).unwrap();
    let before = (b.certificate(), values(b.table(), &whole(&b)));

    let why = refused(b.add(&held.slice(0, 30)));
    assert!(why.contains("after"), "{why}");
    // One row overlapping the greatest held is refused too, and so is the greatest row itself.
    refused(b.add(&held.slice(59, 1)));
    refused(b.add(&held.slice(29, 2)));
    assert_eq!((b.certificate(), values(b.table(), &whole(&b))), before);

    // Seeded: the rows taken as given, without the check, are out of the key's order.
    let taken = [held.slice(30, 30), held.slice(0, 30)];
    assert_ne!(values(b.table(), &taken), values(b.table(), &[held]));
}

#[test]
fn a_primary_key_repeated_by_a_later_add_is_refused() {
    let l = layout();
    let held = held(&Burrow::begin(&l, "buy", &source(60)).unwrap());
    let mut b = Burrow::begin(&l, "buy", &held).unwrap();

    // The last row again, with a root past every root held: after them all in the key's order,
    // with a buy_id held.
    let last = held.slice(59, 1);
    let later = RecordBatch::try_new(
        last.schema(),
        last.columns()
            .iter()
            .enumerate()
            .map(|(i, c)| {
                if last.schema().field(i).name() == "root_id" {
                    Arc::new(Int32Array::from(vec![100])) as _
                } else {
                    c.clone()
                }
            })
            .collect(),
    )
    .unwrap();
    let before = b.certificate();
    let why = refused(b.add(&later));
    assert!(why.contains("primary key"), "{why}");
    assert_eq!(b.certificate(), before);

    // Two rows of one add sharing a primary key are refused as well.
    refused(b.add(&rows(&later, &[0, 0])));

    // Seeded: the same table with no primary key takes the row, so the order alone lets it in.
    let open = Arc::new(Layout::parse(&LAYOUT.replace(r#"["buy_id"]"#, "[]")).unwrap());
    let mut loose = Burrow::begin(&open, "buy", &held).unwrap();
    loose.add(&later).expect("no primary key to repeat");
    assert_eq!(loose.len(), 61);
}

/// One key level's value on each row: the root, the set, and the month of placed_at.
#[derive(Debug, Clone, PartialEq)]
enum Level {
    Int(i128),
    Text(String),
}

impl Level {
    fn value(&self) -> Value<'_> {
        match self {
            Level::Int(i) => Value::Int(*i),
            Level::Text(s) => Value::Text(s),
        }
    }
}

fn levels(batch: &RecordBatch) -> Vec<[Level; 3]> {
    let roots = batch
        .column_by_name("root_id")
        .unwrap()
        .as_primitive::<arrow::datatypes::Int32Type>();
    let sets = batch.column_by_name("set_num").unwrap().as_string::<i32>();
    let placed = batch
        .column_by_name("placed_at")
        .unwrap()
        .as_primitive::<TimestampMicrosecondType>();
    (0..batch.num_rows())
        .map(|r| {
            [
                Level::Int(i128::from(roots.value(r))),
                Level::Text(sets.value(r).to_string()),
                Level::Int(i128::from(
                    value::year_month(value::day_of(placed.value(r))).1,
                )),
            ]
        })
        .collect()
}

#[test]
fn a_prefix_reads_exactly_the_rows_holding_it() {
    let l = layout();
    let held = held(&Burrow::begin(&l, "buy", &source(60)).unwrap());
    // Held as several adds, so a prefix's rows can run across them.
    let mut b = Burrow::begin(&l, "buy", &held.slice(0, 11)).unwrap();
    for (from, to) in [(11, 12), (12, 33), (33, 60)] {
        b.add(&held.slice(from, to - from)).unwrap();
    }
    let every = levels(&held);

    let mut prefixes: Vec<Vec<Level>> = Vec::new();
    for n in 1..=3 {
        for row in &every {
            let p = row[..n].to_vec();
            if !prefixes.contains(&p) {
                prefixes.push(p);
            }
        }
    }
    // Prefixes no row holds: below, between and above the roots held, and a set no root has.
    prefixes.push(vec![Level::Int(0)]);
    prefixes.push(vec![Level::Int(7)]);
    prefixes.push(vec![Level::Int(100)]);
    prefixes.push(vec![Level::Int(1), Level::Text("zz".into())]);

    let table = b.table();
    let mut runs_across_adds = 0;
    for p in &prefixes {
        let asked: Vec<Value<'_>> = p.iter().map(Level::value).collect();
        let read = b.read(&asked, &[]).unwrap();
        if read.len() > 1 {
            runs_across_adds += 1;
        }
        let want: Vec<u32> = (0..held.num_rows() as u32)
            .filter(|r| every[*r as usize][..p.len()] == p[..])
            .collect();
        let got = if read.is_empty() {
            Vec::new()
        } else {
            values(table, &read)
        };
        assert_eq!(got, values(table, &[rows(&held, &want)]), "prefix {p:?}");

        // Seeded: the run's end moved one row on, wherever a row follows it.
        if let Some(&end) = want.last()
            && (end as usize) + 1 < held.num_rows()
        {
            let mut longer = want.clone();
            longer.push(end + 1);
            assert_ne!(got, values(table, &[rows(&held, &longer)]), "prefix {p:?}");
        }
    }
    assert!(
        runs_across_adds > 0,
        "some prefix's rows run across two adds"
    );
    assert_eq!(values(table, &whole(&b)), values(table, &[held]));
}

#[test]
fn terms_keep_exactly_the_rows_they_hold() {
    let l = layout();
    let held = held(&Burrow::begin(&l, "buy", &source(60)).unwrap());
    let mut b = Burrow::begin(&l, "buy", &held.slice(0, 25)).unwrap();
    b.add(&held.slice(25, 35)).unwrap();
    let table = b.table();
    let every = levels(&held);

    let quantities = Term::In {
        column: "qty".into(),
        values: Values::ints(vec![1, 2, 3]),
    };
    let winter = Term::PartIn {
        part: Part::Month("placed_at".into()),
        values: vec![12, 1, 2],
    };
    let after = Term::Within {
        column: "placed_at".into(),
        from: 16_200 * MICROS_PER_DAY,
        to: i64::MAX,
    };
    let terms = [quantities, winter, after];
    let mut prefixes: Vec<Vec<Level>> = vec![vec![]];
    for n in 1..=3 {
        for row in &every {
            let p = row[..n].to_vec();
            if !prefixes.contains(&p) {
                prefixes.push(p);
            }
        }
    }
    let mut decided = [0; 4];
    for prefix in prefixes {
        let asked: Vec<Value<'_>> = prefix.iter().map(Level::value).collect();
        let keep = |ts: &[Term]| -> Vec<u32> {
            (0..held.num_rows() as u32)
                .filter(|r| every[*r as usize][..prefix.len()] == prefix[..])
                .filter(|r| ts.iter().all(|t| holds(t, &held, *r as usize)))
                .collect()
        };
        let want = keep(&terms);
        let read = b.read(&asked, &terms).unwrap();
        let got = values(table, &read);
        assert_eq!(
            got,
            values(table, &[rows(&held, &want)]),
            "prefix {prefix:?}"
        );

        // Seeded: a term dropped from the filter, wherever one keeps other rows on this prefix.
        let looser = (0..terms.len())
            .map(|i| {
                let fewer: Vec<Term> = terms
                    .iter()
                    .enumerate()
                    .filter(|(j, _)| *j != i)
                    .map(|(_, t)| t.clone())
                    .collect();
                keep(&fewer)
            })
            .find(|k| *k != want);
        if let Some(looser) = looser {
            decided[prefix.len()] += 1;
            assert_ne!(
                got,
                values(table, &[rows(&held, &looser)]),
                "prefix {prefix:?}"
            );
        }
    }
    assert!(
        decided[..3].iter().all(|d| *d > 0),
        "the terms decide rows under prefixes of every length: {decided:?}"
    );
}

#[test]
fn a_read_the_table_cannot_answer_is_refused() {
    let l = layout();
    let b = Burrow::begin(&l, "buy", &source(20)).unwrap();
    let why = refused(b.read(
        &[
            Value::Int(1),
            Value::Text("11-1"),
            Value::Int(1),
            Value::Int(1),
        ],
        &[],
    ));
    assert!(why.contains("3 levels"), "{why}");
    let why = refused(b.read(&[Value::Text("1")], &[]));
    assert!(why.contains("root_id"), "{why}");
    let why = refused(b.read(&[Value::Int(1 << 40)], &[]));
    assert!(why.contains("root_id"), "{why}");
    let why = refused(b.read(
        &[],
        &[Term::In {
            column: "colour".into(),
            values: Values::ints(vec![1]),
        }],
    ));
    assert!(why.contains("colour"), "{why}");
    refused(Burrow::begin(&l, "sell", &source(1)));
}

#[test]
fn a_burrow_is_shared_between_threads() {
    fn shared<T: Send + Sync>() {}
    shared::<Burrow>();
}
