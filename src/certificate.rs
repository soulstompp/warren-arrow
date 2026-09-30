//! The certificate: how many rows a table holds, and two sums of a hash of each row, counted with
//! multiplicity. Two tables holding the same rows, in any order, have the same certificate; a row
//! dropped, added or changed moves it.

use arrow::array::RecordBatch;
use sha2::{Digest, Sha256};

use crate::Error;
use crate::layout::Table;
use crate::value::{Reader, Value};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Certificate {
    pub rows: u64,
    pub sum_a: u64,
    pub sum_b: u64,
}

impl Certificate {
    /// The certificate of a batch's rows, reading the layout table's columns by name.
    pub fn of(table: &Table, batch: &RecordBatch) -> Result<Certificate, Error> {
        let mut readers = Vec::with_capacity(table.columns.len());
        for col in &table.columns {
            let array = batch.column_by_name(&col.name).ok_or_else(|| {
                Error::Refused(format!("{}: the rows hold no column {}", table.name, col.name))
            })?;
            readers.push(Reader::new(&table.name, col, array.as_ref())?);
        }
        let mut c = Certificate::default();
        let mut buf = Vec::new();
        for row in 0..batch.num_rows() {
            buf.clear();
            for r in &readers {
                encode(r.get(row)?, &mut buf);
            }
            c.add_row(&buf);
        }
        Ok(c)
    }

    fn add_row(&mut self, encoded: &[u8]) {
        let h = Sha256::digest(encoded);
        let a = u64::from_be_bytes(h[0..8].try_into().expect("eight bytes"));
        let b = u64::from_be_bytes(h[8..16].try_into().expect("eight bytes"));
        self.rows += 1;
        self.sum_a = self.sum_a.wrapping_add(a);
        self.sum_b = self.sum_b.wrapping_add(b);
    }

    /// The certificate of two tables' rows taken together.
    pub fn plus(self, other: Certificate) -> Certificate {
        Certificate {
            rows: self.rows + other.rows,
            sum_a: self.sum_a.wrapping_add(other.sum_a),
            sum_b: self.sum_b.wrapping_add(other.sum_b),
        }
    }
}

/// Each value tagged by its kind, so that no two different rows encode alike.
fn encode(v: Value<'_>, buf: &mut Vec<u8>) {
    match v {
        Value::Null => buf.push(0),
        Value::Int(i) => {
            buf.push(1);
            buf.extend_from_slice(&i.to_be_bytes());
        }
        Value::Text(s) => {
            buf.push(2);
            buf.extend_from_slice(&(s.len() as u64).to_be_bytes());
            buf.extend_from_slice(s.as_bytes());
        }
        Value::Instant(t) => {
            buf.push(3);
            buf.extend_from_slice(&t.to_be_bytes());
        }
        Value::Date(d) => {
            buf.push(4);
            buf.extend_from_slice(&d.to_be_bytes());
        }
        Value::Bool(b) => buf.extend_from_slice(&[5, u8::from(b)]),
    }
}
