/// Prepares a SQL DELETE statement for a specified partition and number of columns.
///
/// This function constructs a DELETE statement to remove rows from a given partition
/// based on a list of ROWIDs. It dynamically generates the required placeholders for
/// the ROWID values based on the specified number of columns.
///
/// Parameters:
/// - `partition_name`: The name of the partition (table) from which rows are to be deleted.
/// - `num_columns`: The number of columns in the partition, determining the number of placeholders
///   in the DELETE statement's WHERE clause.
///
/// Returns:
/// - A string containing the SQL DELETE statement ready for execution with the appropriate
///   number of placeholders for binding ROWID values.
pub fn prepare_delete_statement(partition_name: &str, num_columns: usize) -> String {
    let placeholders = std::iter::repeat("?")
        .take(num_columns)
        .collect::<Vec<&str>>()
        .join(",");
    format!(
        "DELETE FROM {} WHERE ROWID IN ({})",
        partition_name, placeholders
    )
}

/// Constructs a simple SQL DELETE statement for removing rows from a specified partition
/// based on their ROWID.
///
/// This function generates a DELETE statement with a single placeholder for a ROWID value.
/// It's designed for cases where the deletion criteria are straightforward and target
/// a singular row or a batch of rows specified through a single parameter.
///
/// Parameters:
/// - `partition_name`: The name of the partition (table) from which rows are to be deleted.
///
/// Returns:
/// - A string containing the SQL DELETE statement with a single placeholder for the ROWID value.
pub fn delete(partition_name: &String) -> String {
    let sql = format!("DELETE FROM {} WHERE ROWID IN (?)", partition_name);
    sql
}
