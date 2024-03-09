use sqlite3_ext::query::Column as sqlite3_column;
use sqlite3_ext::{FromValue, Value};
/// Represents the result of a query against a single partition.
///
/// Contains information about the partition, including its value, name, and the rows retrieved
/// from the partition as a result of a query.
///
/// # Attributes
///
/// * `partition_value` - The index or identifier for the partition within the lookup table.
/// * `partition_name` - The name of the partition.
/// * `rows` - A vector of `ResultRow` instances representing the rows retrieved from the partition.
#[derive(Debug)]
pub struct Partition {
    pub partition_value: i64, //index in lookup_table.partitions.
    pub partition_name: String,
    pub rows: Vec<Row>,
    current_row_index: usize,
}

impl Partition {
    /// Creates a new `PartitionResult` instance.
    ///
    /// Returns `None` if the rows vector is empty, indicating that no data was retrieved
    /// for the partition.
    ///
    /// # Parameters
    ///
    /// * `partition_value` - The index or identifier for the partition.
    /// * `partition_name` - The name of the partition.
    /// * `rows` - A vector of `ResultRow` instances to be associated with this partition.
    ///
    /// # Returns
    ///
    /// An `Option<PartitionResult>`, which is `None` if `rows` is empty.
    pub fn new(partition_value: i64, partition_name: &String, rows: Vec<Row>) -> Option<Self> {
        if rows.is_empty() {
            return None;
        }

        Some(Self {
            partition_value,
            partition_name: partition_name.to_owned(),
            rows,
            current_row_index: 0,
        })
    }
    pub fn get_mut_current_row(&mut self) -> Option<&mut Row> {
        self.rows.get_mut(self.current_row_index)
    }
    pub fn get_current_row(&self) -> Option<&Row> {
        self.rows.get(self.current_row_index)
    }
    pub fn advance_to_next_row(&mut self) -> Option<&mut Row> {
        self.current_row_index += 1;
        self.get_mut_current_row()
    }
}

/// Represents a single row retrieved from a partition.
///
/// This struct encapsulates the data for a row, including a unique identifier (`rowid`)
/// and the columns of data contained within the row.
///
/// # Attributes
///
/// * `rowid` - The unique identifier for the row within its partition.
/// * `columns` - A vector of `ResultColumn` instances representing the data within the row.
#[derive(Debug)]
pub struct Row {
    pub rowid: Value,
    pub columns: Vec<Column>,
}

impl FromIterator<Column> for Option<Row> {
    fn from_iter<T: IntoIterator<Item = Column>>(iter: T) -> Self {
        let mut iter = iter.into_iter();
        let first_column = match iter.next() {
            Some(column) => column,
            None => return None,
        };

        Some(Row {
            rowid: first_column.value,
            columns: iter.collect(),
        })
    }
}
/// Represents a single column within a `ResultRow`.
///
/// Encapsulates the name and value of the column, providing structured access to row data.
///
/// # Attributes
///
/// * `_name` - The name of the column.
/// * `value` - The value stored in the column, encapsulated as a `Value`.
#[derive(Debug, Clone)]
pub struct Column {
    pub _name: String,
    pub value: Value,
}
/// Constructs a new `ResultColumn` from a SQLite column.
///
/// # Parameters
///
/// * `column` - A reference to the SQLite column from which to construct the `ResultColumn`.
///
/// # Returns
///
/// A `Result` which is:
/// - `Ok(Self)` on success, containing the newly created `ResultColumn`.
/// - `Err(e)` on failure, where `e` is an error that occurred during column creation.
impl Column {
    pub fn new(column: &sqlite3_column) -> sqlite3_ext::Result<Self> {
        let name = column.name()?.to_owned();
        let value = column.to_owned()?;
        Ok(Self { _name: name, value })
    }
}
