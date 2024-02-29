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
pub fn delete(partition_name: &String) -> String {
    let sql = format!("DELETE FROM {} WHERE ROWID IN (?)", partition_name);
    sql
}
