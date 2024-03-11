use std::fmt::Display;

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
}

impl Display for TableError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}", self.to_string()))
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
        }
    }
}
