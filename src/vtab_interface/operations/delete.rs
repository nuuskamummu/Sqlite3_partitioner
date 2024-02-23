use sqlite3_ext::ValueRef;

use crate::Partition;
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
pub fn update<'vtab>(
    partition_name: &str,
    partition: &Partition<i64>,
    args: &'vtab mut [&'vtab mut ValueRef],
) -> (String, Vec<&'vtab mut &'vtab mut ValueRef>) {
    let columns = &partition.columns;
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

                let column_name = columns.get(index).unwrap().get_name();
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
