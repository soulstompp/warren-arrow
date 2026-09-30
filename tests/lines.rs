// Copyright (C) 2026 Kenneth Allen Flegal
// SPDX-License-Identifier: Apache-2.0

//! What a reader keeps for a set of values, whole numbers or texts, on a small lego world's lines:
//! a row group per inventory kept for exactly the inventories asked, texts kept by their bytes, and
//! reading what is kept reading only the kept row groups.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, AsArray, Int32Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Int64Type, Schema};

use warren_arrow::read::{self, Term, Values};
use warren_arrow::write::days_before_month;
use warren_arrow::{Layout, Options, write_table};

const LINES_BY_INVENTORY: &str =
    r#", "keys": [{"name": "k", "levels": ["inventory_id", "part_num", "color_id"]}]"#;

fn layout(lines_key: &str) -> Layout {
    let text = format!(
        r#"{{"format": 2, "name": "lego", "tables": [
        {{"name": "lego_purchases", "columns": [
            {{"name": "purchase_id", "type": "bigint", "null": false}},
            {{"name": "builder_id", "type": "integer", "null": false}},
            {{"name": "row_no", "type": "integer", "null": false}},
            {{"name": "ordered_at", "type": "instant", "null": false}}],
          "primary_key": ["purchase_id"],
          "keys": [{{"name": "k", "levels": [{{"month": "ordered_at"}}, "ordered_at"]}}],
          "partitions": {{"phase": {{"month": "ordered_at"}}}}}},
        {{"name": "lego_collection", "columns": [
            {{"name": "builder_id", "type": "integer", "null": false}},
            {{"name": "row_no", "type": "integer", "null": false}},
            {{"name": "set_num", "type": "varchar", "length": 255, "null": false}}],
          "primary_key": ["builder_id", "row_no"]}},
        {{"name": "lego_builders", "columns": [
            {{"name": "builder_id", "type": "integer", "null": false}},
            {{"name": "home_zone", "type": "varchar", "length": 64, "null": false}}],
          "primary_key": ["builder_id"]}},
        {{"name": "lego_sets", "columns": [
            {{"name": "set_num", "type": "varchar", "length": 255, "null": false}},
            {{"name": "theme_id", "type": "integer"}}],
          "primary_key": ["set_num"],
          "keys": [{{"name": "k", "levels": ["theme_id", "set_num"]}}]}},
        {{"name": "lego_themes", "columns": [
            {{"name": "id", "type": "integer", "null": false}},
            {{"name": "name", "type": "varchar", "length": 255, "null": false}},
            {{"name": "parent_id", "type": "integer"}}],
          "primary_key": ["id"]}},
        {{"name": "lego_inventories", "columns": [
            {{"name": "id", "type": "integer", "null": false}},
            {{"name": "version", "type": "integer", "null": false}},
            {{"name": "set_num", "type": "varchar", "length": 255, "null": false}}],
          "primary_key": ["id"],
          "keys": [{{"name": "k", "levels": ["set_num", "version", "id"]}}]}},
        {{"name": "lego_inventory_sets", "columns": [
            {{"name": "inventory_id", "type": "integer", "null": false}},
            {{"name": "set_num", "type": "varchar", "length": 255, "null": false}},
            {{"name": "quantity", "type": "integer", "null": false}}]}},
        {{"name": "lego_inventory_parts", "columns": [
            {{"name": "inventory_id", "type": "integer", "null": false}},
            {{"name": "part_num", "type": "varchar", "length": 255, "null": false}},
            {{"name": "color_id", "type": "integer", "null": false}},
            {{"name": "quantity", "type": "integer", "null": false}}]{lines_key}}},
        {{"name": "lego_parts", "columns": [
            {{"name": "part_num", "type": "varchar", "length": 255, "null": false}},
            {{"name": "part_cat_id", "type": "integer", "null": false}}],
          "primary_key": ["part_num"]}},
        {{"name": "lego_part_categories", "columns": [
            {{"name": "id", "type": "integer", "null": false}},
            {{"name": "name", "type": "varchar", "length": 255, "null": false}}],
          "primary_key": ["id"]}},
        {{"name": "lego_colors", "columns": [
            {{"name": "id", "type": "integer", "null": false}},
            {{"name": "name", "type": "varchar", "length": 255, "null": false}}],
          "primary_key": ["id"]}}
    ]}}"#
    );
    Layout::parse(&text).expect("the test layout reads")
}

fn int(v: &[Option<i32>]) -> ArrayRef {
    Arc::new(Int32Array::from(v.to_vec()))
}

fn ints(v: &[i32]) -> ArrayRef {
    Arc::new(Int32Array::from(v.to_vec()))
}

fn texts(v: &[&str]) -> ArrayRef {
    Arc::new(StringArray::from(v.to_vec()))
}

fn batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
    let schema = Schema::new(
        columns
            .iter()
            .map(|(n, a)| Field::new(*n, a.data_type().clone(), true))
            .collect::<Vec<_>>(),
    );
    RecordBatch::try_new(Arc::new(schema), columns.into_iter().map(|c| c.1).collect())
        .expect("a batch")
}

/// An instant in microseconds since 1970, in UTC.
fn at(y: i64, m: u32, d: i64, h: i64) -> i64 {
    ((days_before_month(y, m) + d - 1) * 24 + h) * 3_600_000_000
}

/// Two roots named Town, a theme under no root, a set holding two of another set, a version-2
/// inventory, a part and a colour the catalogue does not hold, two colours named Red, a purchase
/// whose builder is not a builder and one whose collection row is not in the collection.
fn world() -> Vec<(&'static str, RecordBatch)> {
    vec![
        (
            "lego_themes",
            batch(vec![
                ("id", ints(&[1, 2, 3, 4, 5])),
                (
                    "name",
                    texts(&["Town", "Police", "Space", "Orphan", "Town"]),
                ),
                ("parent_id", int(&[None, Some(1), None, Some(99), None])),
            ]),
        ),
        (
            "lego_sets",
            batch(vec![
                (
                    "set_num",
                    texts(&["a-1", "b-1", "c-1", "d-1", "n-1", "z-1"]),
                ),
                (
                    "theme_id",
                    int(&[Some(2), Some(3), Some(4), Some(5), Some(3), None]),
                ),
            ]),
        ),
        (
            "lego_inventories",
            batch(vec![
                ("id", ints(&[10, 11, 20, 30, 40, 50])),
                ("version", ints(&[1, 2, 1, 1, 1, 1])),
                (
                    "set_num",
                    texts(&["a-1", "a-1", "b-1", "c-1", "d-1", "n-1"]),
                ),
            ]),
        ),
        (
            "lego_inventory_sets",
            batch(vec![
                ("inventory_id", ints(&[10])),
                ("set_num", texts(&["n-1"])),
                ("quantity", ints(&[2])),
            ]),
        ),
        (
            "lego_inventory_parts",
            batch(vec![
                (
                    "inventory_id",
                    ints(&[10, 10, 11, 20, 30, 40, 40, 50, 50, 10]),
                ),
                (
                    "part_num",
                    texts(&["p1", "p2", "p1", "p1", "p1", "p2", "p1", "p3", "px", "p1"]),
                ),
                ("color_id", ints(&[1, 2, 1, 2, 1, 1, 3, 1, 1, 9])),
                ("quantity", ints(&[3, 1, 99, 4, 5, 2, 1, 1, 7, 1])),
            ]),
        ),
        (
            "lego_parts",
            batch(vec![
                ("part_num", texts(&["p1", "p2", "p3"])),
                ("part_cat_id", ints(&[100, 100, 200])),
            ]),
        ),
        (
            "lego_part_categories",
            batch(vec![
                ("id", ints(&[100, 200])),
                ("name", texts(&["Bricks", "Minifig"])),
            ]),
        ),
        (
            "lego_colors",
            batch(vec![
                ("id", ints(&[1, 2, 3])),
                ("name", texts(&["Red", "Blue", "Red"])),
            ]),
        ),
        (
            "lego_builders",
            batch(vec![
                ("builder_id", ints(&[1, 2])),
                ("home_zone", texts(&["EU", "US"])),
            ]),
        ),
        (
            "lego_collection",
            batch(vec![
                ("builder_id", ints(&[1, 1, 2, 2, 2, 3])),
                ("row_no", ints(&[1, 2, 1, 2, 3, 1])),
                (
                    "set_num",
                    texts(&["a-1", "b-1", "a-1", "c-1", "d-1", "a-1"]),
                ),
            ]),
        ),
        (
            "lego_purchases",
            batch(vec![
                (
                    "purchase_id",
                    Arc::new(Int64Array::from(vec![
                        101, 102, 103, 104, 105, 106, 107, 108, 109,
                    ])),
                ),
                ("builder_id", ints(&[1, 1, 1, 2, 2, 2, 3, 9, 1])),
                ("row_no", ints(&[1, 1, 2, 1, 2, 3, 1, 9, 2])),
                (
                    "ordered_at",
                    Arc::new(
                        TimestampMicrosecondArray::from(vec![
                            at(2015, 12, 5, 10),
                            at(2016, 1, 10, 9),
                            at(2012, 12, 24, 18),
                            at(2015, 12, 31, 23),
                            at(2015, 6, 1, 12),
                            at(2019, 12, 1, 0),
                            at(2015, 12, 2, 8),
                            at(2015, 12, 3, 8),
                            at(2009, 12, 15, 8),
                        ])
                        .with_timezone("UTC"),
                    ),
                ),
            ]),
        ),
    ]
}

fn write_world(l: &Layout, into: &Path, opts: &Options) {
    for (name, b) in world() {
        write_table(l, name, &b, into, opts).expect("written");
    }
}

#[test]
fn a_row_group_per_inventory_is_kept_for_exactly_the_inventories_asked_for() {
    let dir = tempfile::tempdir().expect("a directory");
    let l = layout(LINES_BY_INVENTORY);
    let opts = Options {
        row_group_per_block: true,
        ..Options::default()
    };
    write_world(&l, dir.path(), &opts);
    let t = l.table("lego_inventory_parts").expect("the lines");
    let d = dir.path().join("lego_inventory_parts");
    let asked = [Term::In {
        column: "inventory_id".into(),
        values: Values::ints(vec![50, 10, 50]),
    }];
    let k = read::kept(t, &d, &asked).expect("kept");
    assert_eq!(
        k.row_groups, 6,
        "a row group for each of the six inventories"
    );
    assert_eq!(k.row_groups_kept, 2);
    assert_eq!(k.rows_in_kept, 5, "inventory 10's three lines and 50's two");
    let h = read::held(t, &d, &asked, &k).expect("held");
    assert_eq!((h.in_kept, h.in_skipped, h.in_skipped_pages), (5, 0, 0));
}

#[test]
fn texts_are_kept_by_their_bytes() {
    let dir = tempfile::tempdir().expect("a directory");
    let l = layout(LINES_BY_INVENTORY);
    let opts = Options {
        row_group_per_block: true,
        ..Options::default()
    };
    write_world(&l, dir.path(), &opts);
    let t = l.table("lego_inventories").expect("the inventories");
    let d = dir.path().join("lego_inventories");
    for (asked, groups, rows) in [
        (vec!["b-1"], 1, 1),
        (vec!["a-1", "zz"], 1, 2),
        (vec!["b"], 0, 0),
    ] {
        let terms = [Term::In {
            column: "set_num".into(),
            values: Values::texts(asked.iter().map(|s| s.as_bytes().to_vec()).collect()),
        }];
        let k = read::kept(t, &d, &terms).expect("kept");
        assert_eq!(k.row_groups, 5, "a row group for each set");
        assert_eq!(k.row_groups_kept, groups, "{asked:?}");
        let h = read::held(t, &d, &terms, &k).expect("held");
        assert_eq!((h.in_kept, h.in_skipped), (rows, 0), "{asked:?}");
    }
}

#[test]
fn reading_what_is_kept_reads_only_the_kept_row_groups_and_every_row_they_hold_for_the_question() {
    let dir = tempfile::tempdir().expect("a directory");
    let l = layout(LINES_BY_INVENTORY);
    let opts = Options {
        row_group_per_block: true,
        ..Options::default()
    };
    write_world(&l, dir.path(), &opts);
    let t = l.table("lego_inventory_parts").expect("the lines");
    let open = read::open(t, &dir.path().join("lego_inventory_parts")).expect("opened");
    let terms = [Term::In {
        column: "inventory_id".into(),
        values: Values::ints(vec![20, 40]),
    }];
    let k = read::keep(t, &open, &terms);
    let mut seen = Vec::new();
    let n = read::read_kept(&open, &k, &["inventory_id", "quantity"], |b| {
        let ids = b.column_by_name("inventory_id").expect("ids");
        let ids = arrow::compute::cast(ids, &DataType::Int64).expect("cast");
        seen.extend(ids.as_primitive::<Int64Type>().values().iter().copied());
        Ok(())
    })
    .expect("read");
    seen.sort_unstable();
    assert_eq!(seen, vec![20, 40, 40]);
    assert_eq!((n.row_groups, n.rows), (2, 3));
}
