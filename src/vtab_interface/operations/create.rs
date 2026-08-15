use crate::error::TableError;
use crate::shadow_tables::interface::VirtualTable;
use crate::shadow_tables::PartitionValue;
use crate::utils::parse_interval;
use crate::ColumnDeclaration;
use crate::ColumnDeclarations;
use crate::PartitionColumn;
use sqlite3_ext::Connection;
extern crate sqlite3_ext;

#[derive(Debug)]
struct TableOptions {
    interval: i64,
    lifetime: Option<i64>,
}

#[derive(Debug)]
struct CreateTableSpec {
    options: TableOptions,
    columns: ColumnDeclarations,
    partition_column: ColumnDeclaration,
}

fn parse_lifetime_option(arg: &str) -> Result<Option<i64>, TableError> {
    let tokens: Vec<&str> = arg.split_whitespace().collect();
    if tokens.first().map(|token| token.to_lowercase()) != Some("lifetime".to_string()) {
        return Ok(None);
    }
    if tokens.len() != 3 {
        return Err(TableError::ColumnDeclaration(format!(
            "Invalid lifetime option: {}. Expected format 'lifetime <integer> <unit>'",
            arg
        )));
    }
    Ok(Some(parse_interval(&format!(
        "{} {}",
        tokens[1], tokens[2]
    ))?))
}

fn parse_create_table_spec(args: &[&str]) -> Result<CreateTableSpec, TableError> {
    let interval = parse_interval(args[3])?;
    let mut columns = Vec::new();
    let mut lifetime = None;

    for arg in &args[4..] {
        if let Some(parsed_lifetime) = parse_lifetime_option(arg)? {
            if lifetime.is_some() {
                return Err(TableError::ColumnDeclaration(
                    "Only one lifetime option can be specified.".to_string(),
                ));
            }
            lifetime = Some(parsed_lifetime);
            continue;
        }

        columns.push(ColumnDeclaration::try_from(*arg)?);
    }

    let columns = ColumnDeclarations(columns);
    let partition_column = match PartitionColumn::from_iter(columns.clone()).column_def() {
        Some(col) => Ok(col.clone()),
        None => Err(sqlite3_ext::Error::Module(
            "Could not find column with identifier partition_column.".into(),
        )),
    }?;
    PartitionValue::try_from(partition_column.data_type())?;

    Ok(CreateTableSpec {
        options: TableOptions { interval, lifetime },
        columns,
        partition_column,
    })
}

/// Connects to an existing virtual table by name.
///
/// This function attempts to establish a connection to a virtual table within the database,
/// enabling subsequent operations such as querying or manipulation of the virtual table.
///
/// Parameters:
/// - `db`: A reference to the active database connection.
/// - `table_name`: The name of the virtual table to connect to.
///
/// Returns:
/// - On success, a `VirtualTable` instance representing the connected virtual table.
/// - On failure, an error indicating the connection issue.
pub fn connect_to_virtual_table<'a>(
    db: &'a Connection,
    table_name: &str,
) -> sqlite3_ext::Result<VirtualTable<'a>> {
    VirtualTable::connect(db, table_name)
}

/// Creates a new virtual table within the database, based on the provided arguments.
///
/// This function processes the arguments to define the structure and behavior of the virtual table,
/// including its name, interval for partitioning, and column definitions. It also ensures that a
/// partition column is specified and matches the expected data type.
///
/// Parameters:
/// - `db`: A reference to the active database connection.
/// - `args`: A slice of string slices representing the arguments required for creating the virtual table.
///   Expected order: [module, database_name, table_name, interval_col, column_args...].
///
/// Returns:
/// - On success, a `VirtualTable` instance representing the newly created virtual table.
/// - On failure, a `TableError` indicating issues such as parsing errors or missing partition column.
pub fn create_virtual_table<'a>(
    db: &'a Connection,
    args: &[&str],
) -> Result<VirtualTable<'a>, TableError> {
    let _module = args[0];
    let _database_name = args[1];
    let table_name = args[2];
    let spec = parse_create_table_spec(args)?;

    Ok(VirtualTable::create(
        db,
        table_name,
        spec.columns,
        spec.partition_column.get_name().to_string(),
        spec.options.interval,
        spec.options.lifetime,
    )?)
}

#[cfg(test)]
mod tests {
    use super::{parse_create_table_spec, parse_lifetime_option};

    #[test]
    fn test_parse_lifetime_option() {
        assert_eq!(
            parse_lifetime_option("lifetime 1 day").unwrap(),
            Some(86_400)
        );
        assert_eq!(parse_lifetime_option("col1 text").unwrap(), None);
    }

    #[test]
    fn test_parse_create_table_spec_separates_options_from_columns() {
        let spec = parse_create_table_spec(&[
            "partitioner",
            "main",
            "test",
            "1 hour",
            "col1 timestamp partition_column",
            "col2 text",
            "lifetime 1 day",
        ])
        .unwrap();

        assert_eq!(spec.options.interval, 3_600);
        assert_eq!(spec.options.lifetime, Some(86_400));
        assert_eq!(spec.columns.0.len(), 2);
        assert_eq!(spec.partition_column.get_name(), "col1");
    }

    #[test]
    fn test_parse_create_table_spec_rejects_duplicate_lifetime() {
        let err = parse_create_table_spec(&[
            "partitioner",
            "main",
            "test",
            "1 hour",
            "col1 timestamp partition_column",
            "lifetime 1 day",
            "lifetime 2 day",
        ])
        .unwrap_err();

        assert!(err
            .to_string()
            .contains("Only one lifetime option can be specified."));
    }
}
