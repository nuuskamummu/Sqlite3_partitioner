use crate::{
    shadow_tables::operations::Table, utils::validate_and_map_columns, vtab_interface::*, Partition,
};
use sqlite3_ext::{Connection, Value};

pub fn insert<'vtab>(
    partition: &'vtab Partition<i64>,
    connection: &'vtab Connection,
    info: &mut ChangeInfo,
) -> sqlite3_ext::Result<(String, Vec<Value>)> {
    let columns =
        validate_and_map_columns(&info.args()[1..], partition.get_template().columns().into())?;
    let partition_column = columns
        .iter()
        .find(|&(col_name, _)| col_name == &partition.get_root().partition_column)
        .ok_or_else(|| {
            sqlite3_ext::Error::Sqlite(
                SQLITE_NOTFOUND,
                Some("Partition column not found".to_string()),
            )
        })?;

    let bucket = parse_partition_value(&partition_column.1, partition.get_root().get_interval())?;
    let partition_name: String = resolve_partition_name(partition, connection, bucket)?;
    let sql = prepare_insert_statement(&partition_name, columns.len());
    let variadic_values = prepare_variadic_values(&columns);
    Ok((sql, variadic_values))
}
