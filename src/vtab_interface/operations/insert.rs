use crate::{shadow_tables::interface::VirtualTable, vtab_interface::*};

/// Inserts a new row into the virtual table, distributing it into the appropriate partition
/// based on the partition column value.
///
/// This function first validates and maps the provided column data against the virtual table's
/// schema, identifying the partition column and its value. It then calculates the partition
/// into which the new row should be inserted, based on the partition column value and the
/// table's partitioning interval. Finally, it delegates the actual insertion to the
/// `VirtualTable`'s `insert` method, passing the partition value and the validated column data.
///
/// Parameters:
/// - `interface`: A reference to the `VirtualTable` instance representing the virtual table
///   into which the row is to be inserted.
/// - `info`: A mutable reference to the `ChangeInfo` struct containing information about the
///   change (insertion) being performed, including arguments that represent the column data
///   for the new row.
///
/// Returns:
/// - On success, returns the row ID of the newly inserted row as `Ok(i64)`.
/// - On failure, returns an error, such as when the partition column is not found or
///   if there's an issue with data validation or insertion into the database.
///
/// This function is critical for ensuring that data is correctly inserted into the appropriate
/// partition of a partitioned virtual table, adhering to the table's partitioning scheme.
pub fn insert(interface: &VirtualTable, info: &mut ChangeInfo) -> sqlite3_ext::Result<i64> {
    // SQLite passes one value per vtab column, including trailing companion-hidden
    // columns (NULL on writes); strip them so only real columns reach the partitions.
    let columns = &info.args()[1..1 + interface.real_column_count()];
    let partition_column = columns
        .get(interface.partition_column_index())
        .ok_or_else(|| {
            sqlite3_ext::Error::Sqlite(
                SQLITE_NOTFOUND,
                Some("Partition column not found".to_string()),
            )
        })?;
    let partition_value = interface.partition_value_for(partition_column)?;
    interface.buffer_insert(partition_value, columns)
}
