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
pub fn delete<'vtab>(partition_name: String, value: i64) -> sqlite3_ext::Result<String> {
    // let (_partition_value, partition_name) = partition
    //     .get_lookup()
    //     .access_current_entry(|(partition_value, partition_name)| {
    //         (*partition_value, partition_name.clone())
    //     })
    //     .unwrap();
    // let placeholders = std::iter::repeat("?")
    //     .take(values_count) // Take as many "?" as there are elements in the vec.
    //     .collect::<Vec<&str>>()
    //     .join(", ");
    let sql = format!("DELETE FROM {} WHERE ROWID IN ({})", partition_name, value);

    Ok(sql)
}
pub fn update<'vtab>(
    partition: &'vtab Partition<i64>,
    info: &mut ChangeInfo,
) -> sqlite3_ext::Result<(String, Vec<Value>)> {
    println!("{:#?}", info);
    let (_partition_value, partition_name) = partition
        .get_lookup()
        .access_current_entry(|(partition_value, partition_name)| {
            (*partition_value, partition_name.clone())
        })
        .unwrap();
    let mut values: Vec<Value> = info.args()[1..]
        .to_owned()
        .iter()
        .map(|&arg| arg.to_owned().unwrap())
        .collect();
    let columns = &partition.columns;
    let update_clause = values
        .iter()
        .enumerate()
        .map(|(index, _value)| {
            let column_name = columns.get(index).unwrap().get_name();
            format!("{} = ?", column_name)
        })
        .collect::<Vec<String>>()
        .join(", ");

    let sql = format!(
        "UPDATE {} SET {} WHERE {} = ?",
        partition_name, update_clause, "rowid"
    );

    values.push(info.args()[0].to_owned()?);
    Ok((sql, values))
}
