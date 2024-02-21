use crate::utils::parse_create_table_args;
use crate::utils::parse_interval;
use crate::Lookup;
use crate::LookupTable;
use crate::Partition;
use crate::PartitionAccessor;
use crate::Root;
use crate::RootTable;
use crate::Template;
use crate::TemplateTable;
use sqlite3_ext::Connection;
use sqlite3_ext::Value;
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
// pub fn testing<'a>(db: &Connection, args:&[&str]) {
//
// }
pub fn create_partition<'a>(
    db: &Connection,
    args: &[&str],
    insert: bool,
) -> sqlite3_ext::Result<Partition<i64>> {
    let interval_col = args[3];
    let arguments = args.to_owned();
    let s_args = [&arguments[0..3], &arguments[4..]].concat();
    let create_table_args = match parse_create_table_args(&s_args) {
        Ok(table_args) => Ok(table_args),
        Err(err) => Err(sqlite3_ext::Error::Sqlite(1, Some(err.to_string()))),
    }?;
    let columns = &create_table_args.columns;
    let interval = match parse_interval(interval_col) {
        Ok(interval) => Ok(interval),
        Err(err) => Err(sqlite3_ext::Error::Sqlite(1, Some(err.to_string()))),
    }?;
    let partition_column = &columns[0];

    let root_table: RootTable = Root::create(
        &create_table_args.table_name,
        partition_column.get_name().to_string(),
        interval,
    );

    let template_table: TemplateTable =
        Template::create(&create_table_args.table_name, columns.clone());
    let lookup_table: LookupTable<_> = Lookup::create(&create_table_args.table_name, Vec::new())?;
    if insert {
        root_table.create_table(db)?;
        lookup_table.create_table(db)?;
        template_table.create_table(db)?;
    } else {
        lookup_table.sync(db)?;
    }
    Ok(Partition::new(
        &create_table_args.table_name,
        columns.clone(),
        root_table,
        lookup_table,
        template_table,
    ))
}
