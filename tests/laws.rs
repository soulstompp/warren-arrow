//! What a written table must hold: the rows it was given, each piece's range, each row group's
//! bounds, the key's order, and the same bytes whatever order the rows arrived in.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, Float64Array, Int32Array, Int64Array, LargeStringArray, RecordBatch,
    StringArray, TimestampMicrosecondArray, TimestampNanosecondArray, UInt32Array,
};
use arrow::compute::take;
use arrow::datatypes::{DataType, Field, Int32Type, Int64Type, Schema, TimestampMicrosecondType};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::statistics::Statistics;
use warren_arrow::layout::Table;
use warren_arrow::write::{PIECES_FILE, ROWS_FILE};
use warren_arrow::{Certificate, Error, Layout, Options, write_table};

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

fn layout() -> Layout {
    Layout::parse(LAYOUT).expect("the test layout reads")
}

fn buy(layout: &Layout) -> &Table {
    layout.table("buy").expect("buy")
}

/// Rows in no particular order, with repeated roots and sets, months across two years, and some
/// quantities missing. The source's types differ from the layout's where they can be read exactly.
fn source(n: u32) -> RecordBatch {
    let ids: Vec<u32> = (0..n).map(|i| (i * 37) % n + 1).collect();
    let roots: Vec<i64> = ids.iter().map(|i| [3, 1, 1, 4, 1, 5, 9, 2, 6][(*i % 9) as usize]).collect();
    let sets: Vec<String> = ids.iter().map(|i| format!("{}-{}", 10 + i % 4, i % 3)).collect();
    let placed: Vec<i64> = ids
        .iter()
        .map(|i| (16_000 + i64::from(*i % 23) * 29) * MICROS_PER_DAY * 1_000 + i64::from(*i) * 1_000)
        .collect();
    let qty: Vec<Option<i32>> = ids.iter().map(|i| (i % 5 != 0).then_some(*i as i32 % 7)).collect();
    let schema = Schema::new(vec![
        Field::new("buy_id", DataType::UInt32, false),
        Field::new("set_num", DataType::LargeUtf8, false),
        Field::new("root_id", DataType::Int64, false),
        Field::new(
            "placed_at",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("qty", DataType::Int32, true),
    ]);
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(UInt32Array::from(ids)),
            Arc::new(LargeStringArray::from(sets.iter().map(String::as_str).collect::<Vec<_>>())),
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

fn written(batch: &RecordBatch, opts: &Options) -> (tempfile::TempDir, warren_arrow::Written) {
    written_with(&layout(), batch, opts)
}

fn written_with(
    layout: &Layout,
    batch: &RecordBatch,
    opts: &Options,
) -> (tempfile::TempDir, warren_arrow::Written) {
    let dir = tempfile::tempdir().expect("a directory");
    let w = write_table(layout, "buy", batch, dir.path(), opts).expect("buy is written");
    (dir, w)
}

/// The test layout with a piece every `every` rows, so a piece holds several roots.
fn layout_every(every: u32) -> Layout {
    Layout::parse(&LAYOUT.replace(r#""every": 7"#, &format!(r#""every": {every}"#))).expect("reads")
}

fn read(path: &Path) -> RecordBatch {
    warren_arrow::read_source(path).expect("the file reads")
}

fn piece_files(w: &warren_arrow::Written) -> Vec<PathBuf> {
    w.pieces.iter().map(|p| w.dir.join(&p.name).join(ROWS_FILE)).collect()
}

fn ints(batch: &RecordBatch, name: &str) -> Vec<i64> {
    let a = batch.column_by_name(name).expect("the column");
    match a.data_type() {
        DataType::Int32 => a.as_primitive::<Int32Type>().values().iter().map(|v| i64::from(*v)).collect(),
        DataType::Int64 => a.as_primitive::<Int64Type>().values().to_vec(),
        dt => panic!("{name} is {dt}"),
    }
}

fn texts(batch: &RecordBatch, name: &str) -> Vec<String> {
    let a = batch.column_by_name(name).expect("the column");
    (0..a.len()).map(|i| a.as_string::<i32>().value(i).to_string()).collect()
}

fn months(batch: &RecordBatch, name: &str) -> Vec<u32> {
    let a = batch.column_by_name(name).expect("the column");
    a.as_primitive::<TimestampMicrosecondType>()
        .values()
        .iter()
        .map(|t| warren_arrow::value::year_month(warren_arrow::value::day_of(*t)).1)
        .collect()
}

#[test]
fn the_rows_written_are_the_rows_read() {
    let l = layout();
    let s = source(60);
    let (_dir, w) = written(&s, &Options::default());
    let mut back = Certificate::default();
    for f in piece_files(&w) {
        back = back.plus(Certificate::of(buy(&l), &read(&f)).unwrap());
    }
    assert_eq!(back, Certificate::of(buy(&l), &s).unwrap());
    assert_eq!(back.rows, 60);
    assert_eq!(w.source, w.written);
}

#[test]
fn the_certificate_moves_when_a_row_is_dropped_added_or_changed() {
    let l = layout();
    let s = source(20);
    let all: Vec<u32> = (0..20).collect();
    let whole = Certificate::of(buy(&l), &s).unwrap();

    let reversed: Vec<u32> = all.iter().rev().copied().collect();
    assert_eq!(Certificate::of(buy(&l), &rows(&s, &reversed)).unwrap(), whole);

    let dropped: Vec<u32> = all[1..].to_vec();
    assert_ne!(Certificate::of(buy(&l), &rows(&s, &dropped)).unwrap(), whole);

    let mut added = all.clone();
    added.push(3);
    assert_ne!(Certificate::of(buy(&l), &rows(&s, &added)).unwrap(), whole);

    // one row in place of another, so the count stays
    let mut replaced = all.clone();
    replaced[0] = 1;
    assert_ne!(Certificate::of(buy(&l), &rows(&s, &replaced)).unwrap(), whole);

    // two copies of one row against two copies of another, the rest alike
    let two_of_a = [0u32, 0, 5];
    let two_of_b = [1u32, 1, 5];
    assert_ne!(
        Certificate::of(buy(&l), &rows(&s, &two_of_a)).unwrap(),
        Certificate::of(buy(&l), &rows(&s, &two_of_b)).unwrap()
    );
}

/// The bounds a row group's statistics give one column, as whole numbers or as bytes.
enum Bounds {
    Int(i64, i64),
    Bytes(Vec<u8>, Vec<u8>),
}

fn bounds(file: &Path, group: usize, column: usize) -> Bounds {
    let r = SerializedFileReader::new(File::open(file).unwrap()).unwrap();
    let md = r.metadata();
    let stats = md.row_group(group).column(column).statistics().expect("statistics are written");
    match stats {
        Statistics::Int32(s) => Bounds::Int(
            i64::from(*s.min_opt().expect("a least value")),
            i64::from(*s.max_opt().expect("a greatest value")),
        ),
        Statistics::Int64(s) => Bounds::Int(*s.min_opt().unwrap(), *s.max_opt().unwrap()),
        Statistics::ByteArray(s) => Bounds::Bytes(
            s.min_opt().unwrap().data().to_vec(),
            s.max_opt().unwrap().data().to_vec(),
        ),
        other => panic!("statistics of an unexpected kind: {other:?}"),
    }
}

fn row_groups(file: &Path) -> usize {
    SerializedFileReader::new(File::open(file).unwrap()).unwrap().metadata().num_row_groups()
}

fn group_rows(file: &Path, group: usize) -> RecordBatch {
    let b = ParquetRecordBatchReaderBuilder::try_new(File::open(file).unwrap())
        .unwrap()
        .with_row_groups(vec![group]);
    let schema = b.schema().clone();
    let batches: Vec<_> = b.build().unwrap().collect::<Result<_, _>>().unwrap();
    arrow::compute::concat_batches(&schema, &batches).unwrap()
}

#[test]
fn every_row_groups_bounds_hold_its_rows_key_values() {
    let s = source(90);
    let (_dir, w) = written_with(&layout_every(40), &s, &Options { max_row_group_rows: Some(4) });
    let mut groups = 0;
    for f in piece_files(&w) {
        for g in 0..row_groups(&f) {
            groups += 1;
            let rows = group_rows(&f, g);
            let Bounds::Int(lo, hi) = bounds(&f, g, 2) else { panic!("root_id is whole numbers") };
            for v in ints(&rows, "root_id") {
                assert!(lo <= v && v <= hi, "root_id {v} outside [{lo}, {hi}] in {} group {g}", f.display());
            }
            let Bounds::Bytes(lo, hi) = bounds(&f, g, 1) else { panic!("set_num is text") };
            for v in texts(&rows, "set_num") {
                assert!(lo.as_slice() <= v.as_bytes() && v.as_bytes() <= hi.as_slice());
            }
        }
    }
    assert!(groups > w.pieces.len(), "some piece must hold more than one row group");
}

#[test]
fn row_groups_follow_one_another_along_the_leading_level() {
    let s = source(90);
    let (_dir, w) = written_with(&layout_every(40), &s, &Options { max_row_group_rows: Some(4) });
    let (mut pairs, mut rising) = (0, 0);
    for f in piece_files(&w) {
        let n = row_groups(&f);
        for g in 1..n {
            let (Bounds::Int(_, before), Bounds::Int(after, _)) = (bounds(&f, g - 1, 2), bounds(&f, g, 2)) else {
                panic!("root_id is whole numbers")
            };
            assert!(before <= after, "{} group {g} starts at {after}, below {before}", f.display());
            pairs += 1;
            rising += usize::from(before < after);
        }
    }
    assert!(pairs > 0, "some piece must hold two row groups");
    assert!(rising > 0, "the leading level must change between some two row groups");
}

#[test]
fn each_piece_holds_its_own_range_and_the_pieces_cover_the_table() {
    let s = source(90);
    let (_dir, w) = written(&s, &Options::default());
    assert!(w.pieces.len() > 2, "the law needs pieces to hold apart");
    let listed = read(&w.dir.join(PIECES_FILE));
    let from = listed.column_by_name("from_root_id").unwrap().as_primitive::<Int32Type>().clone();
    let to = listed.column_by_name("to_root_id").unwrap().as_primitive::<Int32Type>().clone();
    let mut total = 0;
    let mut seen_in: std::collections::HashMap<i64, usize> = Default::default();
    for (i, f) in piece_files(&w).iter().enumerate() {
        let roots = ints(&read(f), "root_id");
        assert!(!roots.is_empty(), "piece {i} is empty");
        total += roots.len();
        for v in roots {
            if from.is_valid(i) {
                assert!(v >= i64::from(from.value(i)), "piece {i} holds {v}, below its bound");
            }
            if to.is_valid(i) {
                assert!(v < i64::from(to.value(i)), "piece {i} holds {v}, at or past the next piece's bound");
            }
            let piece = *seen_in.entry(v).or_insert(i);
            assert_eq!(piece, i, "root_id {v} is in piece {piece} and in piece {i}");
        }
    }
    assert_eq!(total, 90);
    assert!(from.is_null(0) && to.is_null(w.pieces.len() - 1));
}

#[test]
fn rows_come_in_the_keys_order_within_each_piece() {
    let s = source(90);
    let in_source: Vec<i64> = s.column(2).as_primitive::<Int64Type>().values().to_vec();
    assert!(in_source.windows(2).any(|p| p[0] > p[1]), "the source must not already be in order");
    let (_dir, w) = written(&s, &Options { max_row_group_rows: Some(4) });
    for f in piece_files(&w) {
        let b = read(&f);
        let names: Vec<String> = b.schema().fields().iter().map(|f| f.name().clone()).collect();
        assert_eq!(names, ["buy_id", "set_num", "root_id", "placed_at", "qty"], "a month is never written");
        let key: Vec<(i64, String, u32, i64)> = ints(&b, "root_id")
            .into_iter()
            .zip(texts(&b, "set_num"))
            .zip(months(&b, "placed_at"))
            .zip(ints(&b, "buy_id"))
            .map(|(((r, s), m), id)| (r, s, m, id))
            .collect();
        assert!(key.windows(2).all(|p| p[0] <= p[1]), "{} is out of the key's order", f.display());
    }
}

fn all_bytes(dir: &Path) -> Vec<(PathBuf, Vec<u8>)> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                stack.push(p);
            } else {
                out.push((p.strip_prefix(dir).unwrap().to_path_buf(), std::fs::read(&p).unwrap()));
            }
        }
    }
    out.sort();
    out
}

#[test]
fn the_files_are_the_same_bytes_whatever_order_the_rows_came_in() {
    let s = source(90);
    let shuffled: Vec<u32> = (0..90u32).map(|i| (i * 53 + 11) % 90).collect();
    let t = rows(&s, &shuffled);
    assert_ne!(
        s.column(0).as_primitive::<arrow::datatypes::UInt32Type>().values().to_vec(),
        t.column(0).as_primitive::<arrow::datatypes::UInt32Type>().values().to_vec()
    );
    let (a, wa) = written(&s, &Options { max_row_group_rows: Some(4) });
    let (b, wb) = written(&t, &Options { max_row_group_rows: Some(4) });
    let (fa, fb) = (all_bytes(a.path()), all_bytes(b.path()));
    assert!(fa.len() > 3);
    assert_eq!(fa.len(), fb.len());
    for ((pa, ba), (pb, bb)) in fa.iter().zip(&fb) {
        assert_eq!(pa, pb);
        assert!(ba == bb, "{} differs", pa.display());
    }
    assert_eq!(wa.written, wb.written);
}

/// A key of set and quantity, where many rows share a set and a quantity but differ in id.
#[test]
fn rows_the_key_leaves_tied_are_ordered_by_the_rest_of_the_row() {
    let text = LAYOUT.replace(
        r#""levels": ["root_id", "set_num", {"month": "placed_at"}], "covers": ["qty"]"#,
        r#""levels": ["qty"]"#,
    );
    let l = Layout::parse(&text).unwrap();
    let s = source(40);
    let shuffled: Vec<u32> = (0..40u32).rev().collect();
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    write_table(&l, "buy", &s, a.path(), &Options::default()).unwrap();
    write_table(&l, "buy", &rows(&s, &shuffled), b.path(), &Options::default()).unwrap();
    assert_eq!(all_bytes(a.path()), all_bytes(b.path()));
}

fn refused_layout(text: &str) -> String {
    match Layout::parse(text) {
        Err(Error::Refused(why)) => why,
        Err(e) => panic!("not a refusal: {e}"),
        Ok(_) => panic!("read, and it should have been refused"),
    }
}

#[test]
fn a_layout_asking_for_what_parquet_cannot_hold_is_refused() {
    let with = |from: &str, to: &str| {
        let t = LAYOUT.replacen(from, to, 1);
        assert_ne!(t, LAYOUT, "the replacement must apply: {from}");
        t
    };
    let pk = r#""primary_key": ["buy_id"],"#;
    let cases = [
        (with(pk, r#""primary_key": ["buy_id"], "decides": [{"by": "set_num", "columns": ["root_id"], "from": "sets"}],"#), "read through the join"),
        (with(pk, r#""primary_key": ["buy_id"], "fold": "SELECT 1","#), "no engine here"),
        (with(r#""keys": ["#, r#""keys": [{"name": "k2", "levels": ["qty"]}, "#), "another copy of the rows"),
        (with(pk, r#""primary_key": ["buy_id"], "pieces": {"list": "root_id"},"#), "format 2 has no pieces"),
        (with(r#""format": 2,"#, r#""format": 2, "hints": {},"#), "format 2 has no hints"),
        (with(r#""format": 2,"#, r#""format": 3,"#), "this reads format 2"),
        (with(r#"{"month": "placed_at"}"#, r#""nothing""#), "nothing is not a column"),
        (with(r#"{"month": "placed_at"}"#, r#"{"month": "qty"}"#), "neither an instant nor a date"),
        (with(r#"{"name": "buy_id", "type": "bigint", "null": false}"#, r#"{"name": "buy_id", "type": "bigint"}"#), "primary key's buy_id may hold null"),
        (with(r#"{"name": "root_id", "type": "integer", "null": false}"#, r#"{"name": "root_id", "type": "integer"}"#), "a null has no place among the ranges"),
        (with(r#""every": 7"#, r#""every": 0"#), "every 0 rows"),
        (with(pk, r#""primary_key": ["buy_id"], "shards": 4,"#), "unknown field"),
        (with(r#""name": "buy""#, r#""name": "Buy""#), "a name is lower-case"),
        (with(r#""covers": ["qty"]"#, r#""covers": ["price"]"#), "covers price, which is not a column"),
    ];
    for (text, why) in cases {
        let got = refused_layout(&text);
        assert!(got.contains(why), "expected {why:?}, got {got:?}");
    }
}

fn refused_write(batch: &RecordBatch) -> String {
    let dir = tempfile::tempdir().unwrap();
    match write_table(&layout(), "buy", batch, dir.path(), &Options::default()) {
        Err(Error::Refused(why)) => why,
        Err(e) => panic!("not a refusal: {e}"),
        Ok(_) => panic!("written, and it should have been refused"),
    }
}

fn replace_column(batch: &RecordBatch, name: &str, field: Field, array: ArrayRef) -> RecordBatch {
    let i = batch.schema().index_of(name).unwrap();
    let mut fields: Vec<Field> = batch.schema().fields().iter().map(|f| f.as_ref().clone()).collect();
    let mut columns = batch.columns().to_vec();
    fields[i] = field;
    columns[i] = array;
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

#[test]
fn a_source_the_layout_cannot_hold_exactly_is_refused() {
    let s = source(10);
    let n = s.num_rows();

    let missing = s.project(&[0, 1, 2, 3]).unwrap();
    assert!(refused_write(&missing).contains("has no column qty"));

    let mut fields: Vec<Field> = s.schema().fields().iter().map(|f| f.as_ref().clone()).collect();
    fields.push(Field::new("price", DataType::Int32, true));
    let mut columns = s.columns().to_vec();
    columns.push(Arc::new(Int32Array::from(vec![1; n])));
    let extra = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    assert!(refused_write(&extra).contains("holds price, which the layout does not name"));

    let fraction = replace_column(&s, "qty", Field::new("qty", DataType::Float64, true), Arc::new(Float64Array::from(vec![1.5; n])));
    assert!(refused_write(&fraction).contains("no type for a fraction"));

    let zoneless = replace_column(
        &s,
        "placed_at",
        Field::new("placed_at", DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None), false),
        Arc::new(TimestampMicrosecondArray::from(vec![0; n])),
    );
    assert!(refused_write(&zoneless).contains("name no zone"));

    let nanos = replace_column(
        &s,
        "placed_at",
        Field::new("placed_at", DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, Some("UTC".into())), false),
        Arc::new(TimestampNanosecondArray::from(vec![1; n]).with_timezone("UTC")),
    );
    assert!(refused_write(&nanos).contains("carries nanoseconds"));

    let wide = replace_column(&s, "root_id", Field::new("root_id", DataType::Int64, false), Arc::new(Int64Array::from(vec![1i64 << 40; n])));
    assert!(refused_write(&wide).contains("does not fit a integer"));

    let nulls = replace_column(
        &s,
        "set_num",
        Field::new("set_num", DataType::Utf8, true),
        Arc::new(StringArray::from(vec![None::<&str>; n])),
    );
    assert!(refused_write(&nulls).contains("holds none"));

    let long = replace_column(&s, "set_num", Field::new("set_num", DataType::Utf8, false), Arc::new(StringArray::from(vec!["x".repeat(17); n])));
    assert!(refused_write(&long).contains("holds at most 16"));

    let texts = replace_column(&s, "root_id", Field::new("root_id", DataType::Utf8, false), Arc::new(StringArray::from(vec!["1"; n])));
    assert!(refused_write(&texts).contains("which is not a integer"));

    let twice = rows(&s, &[0, 1, 2, 2]);
    assert!(refused_write(&twice).contains("repeats a primary key"));

    let dir = tempfile::tempdir().unwrap();
    write_table(&layout(), "buy", &s, dir.path(), &Options::default()).unwrap();
    match write_table(&layout(), "buy", &s, dir.path(), &Options::default()) {
        Err(Error::Refused(why)) => assert!(why.contains("nothing is written over")),
        other => panic!("written over: {other:?}"),
    }
}

#[cfg(not(feature = "geo"))]
#[test]
fn a_point_or_its_cell_is_refused_when_built_without_the_geo_feature() {
    let places = r#"{"format": 2, "name": "places", "tables": [{"name": "place",
      "columns": [{"name": "place_id", "type": "bigint", "null": false},
                  {"name": "place", "type": "geography", "null": false}],
      "keys": [{"name": "k", "levels": [{"cell": "place", "band": 10, "sector": 10}]}]}]}"#;
    assert!(refused_layout(places).contains("built without the geo feature"));
    let cell_only = places.replace(r#""type": "geography""#, r#""type": "bigint""#);
    assert!(refused_layout(&cell_only).contains("built without the geo feature"));
}
