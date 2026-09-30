//! A warren in Parquet. Each table of a layout file is written as pieces of rows in its key's
//! order, and a certificate beside them shows that the rows written are the rows read.

pub mod certificate;
pub mod layout;
pub mod value;
pub mod write;

use std::fmt;

pub use certificate::Certificate;
pub use layout::Layout;
pub use write::{Options, Piece, Written, read_source, write_table};

/// Why a table was not written.
#[derive(Debug)]
pub enum Error {
    /// The layout or the source asks for something this writer will not do; the text says what.
    Refused(String),
    Io(std::io::Error),
    Parquet(parquet::errors::ParquetError),
    Arrow(arrow::error::ArrowError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Refused(why) => f.write_str(why),
            Error::Io(e) => write!(f, "{e}"),
            Error::Parquet(e) => write!(f, "{e}"),
            Error::Arrow(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

impl From<parquet::errors::ParquetError> for Error {
    fn from(e: parquet::errors::ParquetError) -> Self {
        Error::Parquet(e)
    }
}

impl From<arrow::error::ArrowError> for Error {
    fn from(e: arrow::error::ArrowError) -> Self {
        Error::Arrow(e)
    }
}
