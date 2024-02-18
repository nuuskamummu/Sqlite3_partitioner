use sqlite3_ext::{ffi::SQLITE_NOTFOUND, vtab::ChangeInfo, Connection, FromValue, Value, ValueRef};

use crate::{
    utils::{calculate_bucket, resolve_partition_name, validate_and_map_columns},
    Partition, PartitionAccessor, Root,
};
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
pub fn delete<'vtab>(
    partition: &'vtab Partition<i64>,
    connection: &'vtab Connection,
    info: &mut ChangeInfo,
) -> sqlite3_ext::Result<(String, Vec<Value>)> {
    println!("delete {:#?}", info);
    let columns = validate_and_map_columns(&info.args(), &partition.columns)?;

    let partition_column = columns
        .iter()
        .find(|&(col_name, _)| col_name == &partition.get_root().partition_column)
        .ok_or_else(|| {
            sqlite3_ext::Error::Sqlite(
                SQLITE_NOTFOUND,
                Some("Partition column not found".to_string()),
            )
        })?;
    let bucket = calculate_bucket(&partition_column.1, partition.get_root().get_interval())?;
    let partition_name: String = resolve_partition_name(&partition, connection, bucket)?;
    let sql = format!("DELETE FROM {} WHERE ROWID IN ({})", partition_name, "?");
    // let sql = prepare_delete_statement(&partition_name, columns.len());
    // let variadic_values = prepare_variadic_values(info.rowid());
    let value: &ValueRef = info.rowid();
    let mut values: Vec<Value> = Vec::new();
    values.push(value.to_owned().unwrap());
    Ok((sql, values))
}
