use crate::{
    shadow_tables::interface::VirtualTable, utils::validation::validate_and_map_columns,
    vtab_interface::*,
};

pub fn insert(interface: &VirtualTable, info: &mut ChangeInfo) -> sqlite3_ext::Result<i64> {
    let (columns, partition_column) = validate_and_map_columns(
        &info.args()[1..],
        interface.columns().into(),
        interface.partition_column_name(),
    )?;
    let partition_column = match partition_column {
        Some(value) => value,
        None => {
            return Err(sqlite3_ext::Error::Sqlite(
                SQLITE_NOTFOUND,
                Some("Partition column not found".to_string()),
            ))
        }
    };
    let partition_value = parse_partition_value(partition_column, interface.partition_interval())?;
    interface.insert(partition_value, columns)
}
