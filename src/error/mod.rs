use std::sync::LockResult;

use sqlite3_ext::ffi::SQLITE_MISMATCH;

#[derive(Debug)]
pub enum TableError {
    ColumnTypeMismatch {
        expected: &'static str,
        found: &'static str,
    },
    SqlError(sqlite3_ext::Error),
    // Other error types as needed
}

pub enum LookupError {
    LockError(&'static str),
}

impl From<sqlite3_ext::Error> for TableError {
    fn from(e: sqlite3_ext::Error) -> Self {
        TableError::SqlError(e)
    }
}
impl Into<sqlite3_ext::Error> for TableError {
    fn into(self) -> sqlite3_ext::Error {
        match self {
            Self::ColumnTypeMismatch { expected, found } => sqlite3_ext::Error::Sqlite(
                SQLITE_MISMATCH,
                Some(format!("Expected: {}, Found: {}", expected, found)),
            ),
            Self::SqlError(err) => err,
        }
    }
}
