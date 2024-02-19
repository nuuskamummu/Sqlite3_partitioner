use sqlite3_ext::{vtab::ChangeInfo, FromValue, Value, ValueRef};

use crate::{Lookup, Partition, PartitionAccessor};
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
