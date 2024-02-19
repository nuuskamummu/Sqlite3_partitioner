use sqlite3_ext::{ffi::SQLITE_NOTFOUND, vtab::ChangeInfo, Connection, FromValue, Value, ValueRef};

use crate::{
    utils::{calculate_bucket, resolve_partition_name, validate_and_map_columns},
    Lookup, Partition, PartitionAccessor, Root,
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
    info: &mut ChangeInfo,
) -> sqlite3_ext::Result<(String, Vec<Value>)> {
    println!("delete {:#?}", info);
    let (_partition_value, partition_name) = partition
        .get_lookup()
        .access_current_entry(|(partition_value, partition_name)| {
            (*partition_value, partition_name.clone())
        })
        .unwrap();
    let sql = format!("DELETE FROM {} WHERE ROWID IN ({})", partition_name, "?");
    let value: &ValueRef = info.rowid();
    let mut values: Vec<Value> = Vec::new();
    values.push(value.to_owned().unwrap());
    Ok((sql, values))
}
