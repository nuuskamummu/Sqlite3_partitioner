use crate::error::TableError;
use crate::shadow_tables::interface::VirtualTable;
use crate::utils::parse_interval;
use crate::ColumnDeclaration;
use crate::ColumnDeclarations;
use crate::PartitionColumn;
use sqlite3_ext::Connection;
use sqlite3_ext::Value;
use sqlite3_ext::ValueType;
extern crate sqlite3_ext;

pub fn connect_to_virtual_table<'a, 'b>(
    db: &'a Connection,
    table_name: &'b str,
) -> sqlite3_ext::Result<VirtualTable<'a>> {
    VirtualTable::connect(db, table_name)
}
pub fn create_virtual_table<'a, 'b>(
    db: &'a Connection,
    args: &'b [&'b str],
) -> Result<VirtualTable<'a>, TableError> {
    let _module = args[0];
    let _database_name = args[1];
    let table_name = args[2];
    let interval_col = args[3];
    let column_args = &args[4..];

    let columns: ColumnDeclarations = ColumnDeclarations::from_iter(column_args);

    let interval = parse_interval(interval_col)?;
    let partition_column: ColumnDeclaration =
        match PartitionColumn::from_iter(columns.clone()).column_def() {
            Some(col) => Ok(col),
            None => Err(sqlite3_ext::Error::Module(
                "Could not find column with identifier partition_column.".into(),
            )),
        }?
        .clone();

    match partition_column.data_type() {
        ValueType::Integer => Ok(()),
        _ => Err(sqlite3_ext::Error::Module(
            "Incorrect data type for partition column. Expected Interval.".to_string(),
        )),
    }?;

    Ok(VirtualTable::create(
        db,
        table_name,
        columns,
        partition_column.get_name().to_string(),
        interval,
    )?)
}
