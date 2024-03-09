use crate::error::TableError;
use crate::utils::parse_interval;
use crate::ColumnDeclaration;
use crate::ColumnDeclarations;
use crate::LookupTable;
use crate::Partition;
use crate::PartitionAccessor;
use crate::PartitionColumn;
use crate::RootTable;
use crate::TemplateTable;
use sqlite3_ext::Connection;
use sqlite3_ext::Value;
use sqlite3_ext::ValueType;
extern crate sqlite3_ext;
pub fn prepare_insert_statement(partition_name: &str, num_columns: usize) -> String {
    let placeholders = std::iter::repeat("?")
        .take(num_columns)
        .collect::<Vec<&str>>()
        .join(",");
    format!("INSERT INTO {} values({})", partition_name, placeholders)
}
pub fn prepare_variadic_values(columns: &[(String, Value)]) -> Vec<Value> {
    let c = columns.iter().map(|(_, value)| value.clone()).collect();
    c
}

pub fn create_partition<'a>(
    db: &Connection,
    args: &[&str],
    insert: bool,
) -> Result<Partition<i64>, TableError> {
    let _module = args[0];
    let _database_name = args[1];
    let table_name = args[2];
    let interval_col = args[3];
    let column_args = &args[4..];
    // let arguments = args.to_owned();
    // let s_args = [&arguments[0..3], &arguments[4..]].concat();
    // let create_table_args = parse_create_table_args(&s_args)?;
    let columns: Result<Vec<ColumnDeclaration>, TableError> = column_args
        .iter()
        .map(|&column_arg| ColumnDeclaration::try_from(column_arg))
        .collect();
    let columns = match columns {
        Ok(columns) => columns,
        Err(err) => return Err(err),
    };
    // let columns = &create_table_args.columns;
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

    let root_table: RootTable;
    let template_table: TemplateTable;
    let lookup_table: LookupTable<_>;
    if insert {
        root_table = RootTable::create(
            db,
            table_name,
            partition_column.get_name().to_string(),
            interval,
        )?;

        lookup_table = LookupTable::create(db, table_name)?;
        template_table = TemplateTable::create(
            db,
            table_name.to_string(),
            ColumnDeclarations(columns.clone()),
        )?;
    } else {
        root_table = RootTable::connect(db, table_name)?;
        template_table = TemplateTable::connect(db, table_name.to_string())?;
        lookup_table = LookupTable::connect(db, table_name)?;
    }
    Ok(Partition::new(
        table_name,
        columns.clone(),
        root_table,
        lookup_table,
        template_table,
    ))
}
