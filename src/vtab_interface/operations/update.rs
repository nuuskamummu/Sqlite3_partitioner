use sqlite3_ext::ValueRef;

use crate::shadow_tables::interface::VirtualTable;

/// Constructs an SQL UPDATE statement and identifies the changed values for a specific partition.
///
/// This function iterates over the provided arguments, which represent the new values for the row,
/// and constructs an UPDATE statement by determining which columns have changed. It skips columns
/// where the value has not changed (using the `nochange()` method to check) and prepares a list of
/// changed values to be used in the query execution.
///
/// Parameters:
/// - `partition_name`: The name of the partition (table) where the update will occur.
/// - `partition`: A reference to the `VirtualTable` representing the partition.
/// - `args`: A mutable slice of mutable references to `ValueRef`, representing the new values for the row.
///
/// Returns:
/// - A tuple containing the constructed SQL UPDATE statement as a `String` and a vector of mutable
///   references to the `ValueRef` instances that have changed.
///
/// Note:
/// The first element of `args` is assumed to be the new ROWID value, which is not directly used
/// in constructing the UPDATE clause but may be used for specifying the row to update. The function
/// assumes at least one value is present in `args`.
///
/// This approach ensures that only the necessary columns are updated, optimizing performance and
/// maintaining data integrity within the virtual table's partitioned structure.
pub fn update<'vtab>(
    partition_name: &str,
    partition: &VirtualTable,
    args: &'vtab mut [&'vtab mut ValueRef],
) -> (String, Vec<&'vtab mut &'vtab mut ValueRef>) {
    let columns = partition.columns();
    let mut return_values = Vec::new();

    let (mut _new_rowid, cols) = args.split_first_mut().unwrap();
    let update_clause = cols
        .iter_mut()
        .enumerate()
        .filter_map(|(index, value)| {
            if value.nochange() {
                None
            } else {
                return_values.push(value);

                let column_name = columns.0.get(index).unwrap().get_name();
                Some(format!("{} = ?", column_name))
            }
        })
        .collect::<Vec<String>>()
        .join(", ");

    let sql = format!(
        "UPDATE {} SET {} WHERE ROWID = ?",
        partition_name, update_clause
    );
    (sql, return_values)
}
