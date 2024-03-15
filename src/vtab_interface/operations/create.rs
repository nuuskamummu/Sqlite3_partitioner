use crate::error::TableError;
use crate::shadow_tables::interface::VirtualTable;
use crate::shadow_tables::PartitionValue;
use crate::utils::parse_interval;
use crate::ColumnDeclaration;
use crate::ColumnDeclarations;
use crate::PartitionColumn;
use sqlite3_ext::Connection;
extern crate sqlite3_ext;

pub fn connect_to_virtual_table<'a>(
    db: &'a Connection,
    table_name: &str,
) -> sqlite3_ext::Result<VirtualTable<'a>> {
    VirtualTable::connect(db, table_name)
}
pub fn create_virtual_table<'a>(
    db: &'a Connection,
    args: &[&str],
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

    PartitionValue::try_from(partition_column.data_type())?;

    Ok(VirtualTable::create(
        db,
        table_name,
        columns,
        partition_column.get_name().to_string(),
        interval,
    )?)
}
