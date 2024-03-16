use std::fmt::{self, Display, Formatter};

use sqlite3_ext::ffi::SQLITE_MISMATCH;

#[derive(Debug)]
pub enum TableError {
    ColumnTypeMismatch {
        expected: &'static str,
        found: &'static str,
    },
    ColumnDeclaration(String),
    ParseValueType(String),
    SqlError(sqlite3_ext::Error),
    ParseInterval(String),
    PartitionColumn(String),
    WhereClause(String),
}

impl Display for TableError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            TableError::ColumnTypeMismatch { expected, found } => {
                write!(f, "Expected column type {}, but found {}", expected, found)
            }
            TableError::ColumnDeclaration(msg) => write!(f, "Column declaration error: {}", msg),
            TableError::ParseValueType(msg) => write!(f, "Parse value type error: {}", msg),
            TableError::SqlError(err) => write!(f, "SQL error: {}", err),
            TableError::ParseInterval(msg) => write!(f, "Parse interval error: {}", msg),
            TableError::PartitionColumn(msg) => write!(f, "Partition column error: {}", msg),
            TableError::WhereClause(msg) => write!(f, "Where clause error: {}", msg),
        }
    }
}
impl From<TableError> for String {
    fn from(value: TableError) -> Self {
        value.to_string()
    }
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
            Self::ColumnDeclaration(err) => sqlite3_ext::Error::Module(err),
            Self::ParseValueType(err) => sqlite3_ext::Error::Module(err),
            Self::ParseInterval(err) => sqlite3_ext::Error::Module(err),
            Self::PartitionColumn(err) => sqlite3_ext::Error::Module(err),
            Self::WhereClause(err) => sqlite3_ext::Error::Module(err),
        }
    }
}
