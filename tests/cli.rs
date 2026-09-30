// Copyright (C) 2026 Kenneth Allen Flegal
// SPDX-License-Identifier: Apache-2.0

//! The binary itself: `write` writes a table and certifies it, refuses a source the layout cannot
//! hold and a table directory that already exists; `kept` counts from the metadata and fails its
//! `--check` when the count is not the rows the question keeps.

use std::fs::File;
use std::path::Path;
use std::process::{Command, Output};
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use parquet::arrow::ArrowWriter;

const LAYOUT: &str = r#"{"format": 2, "name": "shop", "tables": [{
    "name": "buy",
    "columns": [
        {"name": "buy_id", "type": "bigint", "null": false},
        {"name": "placed_at", "type": "instant", "null": false}
    ],
    "primary_key": ["buy_id"],
    "keys": [{"name": "k", "levels": [{"month": "placed_at"}, "placed_at"]}]
}]}"#;

const MICROS_PER_DAY: i64 = 86_400_000_000;

fn bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_warren-arrow"))
}

fn text(o: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&o.stdout),
        String::from_utf8_lossy(&o.stderr)
    )
}

/// 365 purchases, one a day through 2025, and the Decembers among them.
fn source(dir: &Path, extra_column: bool) -> (std::path::PathBuf, u64) {
    let n = 365i64;
    let start = 20_089; // 2025-01-01, in days since 1970
    let mut fields = vec![
        Field::new("buy_id", DataType::Int64, false),
        Field::new(
            "placed_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
    ];
    let mut columns: Vec<Arc<dyn arrow::array::Array>> = vec![
        Arc::new(Int64Array::from_iter_values(1..=n)),
        Arc::new(
            TimestampMicrosecondArray::from_iter_values(
                (0..n).map(|d| (start + d) * MICROS_PER_DAY),
            )
            .with_timezone("UTC"),
        ),
    ];
    if extra_column {
        fields.push(Field::new("note", DataType::Int64, true));
        columns.push(Arc::new(Int64Array::from_iter_values(0..n)));
    }
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("a batch");
    let path = dir.join(if extra_column {
        "extra.parquet"
    } else {
        "buy.parquet"
    });
    let mut w = ArrowWriter::try_new(File::create(&path).expect("file"), batch.schema(), None)
        .expect("writer");
    w.write(&batch).expect("write");
    w.close().expect("close");
    (path, 31)
}

#[test]
fn write_certifies_a_table_and_refuses_what_it_cannot_hold_or_would_write_over() {
    let dir = tempfile::tempdir().expect("a directory");
    let layout = dir.path().join("shop.json");
    std::fs::write(&layout, LAYOUT).expect("layout");
    let (src, _) = source(dir.path(), false);
    let into = dir.path().join("out");

    let o = bin()
        .args(["write", "--layout"])
        .arg(&layout)
        .arg("--table")
        .arg(format!("buy={}", src.display()))
        .arg("--into")
        .arg(&into)
        .output()
        .expect("runs");
    assert!(o.status.success(), "{}", text(&o));
    assert!(
        text(&o)
            .contains("buy: 365 rows in 1 pieces and 12 row groups, the same rows as the source"),
        "{}",
        text(&o)
    );
    for f in ["pieces.parquet", "certificate.parquet", "p1/rows.parquet"] {
        assert!(into.join("buy").join(f).exists(), "{f}");
    }

    let again = bin()
        .args(["write", "--layout"])
        .arg(&layout)
        .arg("--table")
        .arg(format!("buy={}", src.display()))
        .arg("--into")
        .arg(&into)
        .output()
        .expect("runs");
    assert!(!again.status.success(), "{}", text(&again));
    assert!(
        text(&again).contains("nothing is written over"),
        "{}",
        text(&again)
    );

    let (extra, _) = source(dir.path(), true);
    let other = dir.path().join("other");
    let refused = bin()
        .args(["write", "--layout"])
        .arg(&layout)
        .arg("--table")
        .arg(format!("buy={}", extra.display()))
        .arg("--into")
        .arg(&other)
        .output()
        .expect("runs");
    assert!(!refused.status.success(), "{}", text(&refused));
    assert!(
        text(&refused).contains("buy: refused:"),
        "{}",
        text(&refused)
    );
    assert!(
        !other.join("buy").exists(),
        "a refused table leaves no directory"
    );
}

#[test]
fn kept_counts_from_the_metadata_and_its_check_fails_on_a_wrong_count() {
    let dir = tempfile::tempdir().expect("a directory");
    let layout = dir.path().join("shop.json");
    std::fs::write(&layout, LAYOUT).expect("layout");
    let (src, decembers) = source(dir.path(), false);
    let into = dir.path().join("out");
    let w = bin()
        .args(["write", "--layout"])
        .arg(&layout)
        .arg("--table")
        .arg(format!("buy={}", src.display()))
        .arg("--into")
        .arg(&into)
        .output()
        .expect("runs");
    assert!(w.status.success(), "{}", text(&w));

    let kept = |check: u64| {
        bin()
            .args(["kept", "--layout"])
            .arg(&layout)
            .args(["--table", "buy", "--dir"])
            .arg(into.join("buy"))
            .args(["--month-in", "placed_at=12", "--check"])
            .arg(check.to_string())
            .output()
            .expect("runs")
    };
    let right = kept(decembers);
    assert!(right.status.success(), "{}", text(&right));
    let v: serde_json::Value = serde_json::from_slice(&right.stdout).expect("json");
    assert_eq!(v["row_groups"], 12);
    assert_eq!(v["row_groups_kept"], 1);
    assert_eq!(v["rows_in_kept"], decembers);
    assert_eq!(v["held_in_kept"], decembers);

    let wrong = kept(decembers + 1);
    assert!(!wrong.status.success(), "{}", text(&wrong));
}
