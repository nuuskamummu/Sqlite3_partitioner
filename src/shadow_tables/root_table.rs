use std::ops::IndexMut;

use sqlite3_ext::params;
use sqlite3_ext::Connection;
use sqlite3_ext::FromValue;
use sqlite3_ext::Result as ExtResult;
use sqlite3_ext::ValueType;

use super::operations::Connect;
use super::operations::Create;
use super::operations::Drop;
use super::operations::SchemaDeclaration;
use super::PartitionValue;

use super::operations::Table;
use super::PartitionType;

#[derive(Debug, Clone)]
pub struct RootTable {
    partition_column: String,
    interval: i64,
    schema: SchemaDeclaration,
}

impl Table for RootTable {
    fn schema(&self) -> &SchemaDeclaration {
        &self.schema
    }
    const POSTFIX: &'static str = "root";
}
impl Create for RootTable {}
impl Drop for RootTable {}
impl Connect for RootTable {}

impl PartitionType for RootTable {
    const PARTITION_NAME_COLUMN: &'static str = "partition_column";
    const PARTITION_VALUE_COLUMN: &'static str = "partition_value";
    const PARTITION_VALUE_COLUMN_TYPE: &'static PartitionValue = &PartitionValue::Interval;
    const PARTITION_NAME_COLUMN_TYPE: &'static ValueType = &ValueType::Text;
}

impl RootTable {
    pub fn partition_column(&self) -> &str {
        &self.partition_column
    }
    /// Creates a new `RootTable` instance with specified name, partition column, and interval.
    pub fn create(
        db: &Connection,
        base_name: &str,
        partition_column: String,
        interval: i64,
    ) -> ExtResult<Self> {
        let table_name = Self::format_name(base_name);
        let columns = <Self as PartitionType>::columns();
        let schema = <Self as Create>::schema(db, table_name.to_string(), columns)?;
        let table = Self {
            partition_column,
            interval,
            schema,
        };
        table.insert(db)?;

        Ok(table)
    }
    pub fn connect(db: &Connection, base_name: &str) -> ExtResult<Self> {
        let table_name = &Self::format_name(base_name);
        let schema = <Self as Connect>::schema(db, &table_name.to_string())?;
        let columns: String = schema
            .columns()
            .0
            .iter()
            .map(|column| column.get_name().to_string())
            .collect::<Vec<String>>()
            .join(", ");
        println!("columns: {:#?}", columns);
        let query = format!("SELECT {columns} FROM {table_name}");
        let mut partition_column: String = String::default();
        let mut interval: i64 = 0i64;
        db.query_row(&query, (), |row| {
            let column_count = row.len();
            for index in 0..column_count {
                let column = row.index_mut(index);
                println!("{:#?}", column);
                let name = column.name()?;
                println!("column name {:#?}", name);
                if name.eq(<Self as PartitionType>::partition_name_column().get_name()) {
                    partition_column = column.get_str()?.to_owned();
                } else if name.eq(<Self as PartitionType>::partition_value_column().get_name()) {
                    interval = column.get_i64();
                }
            }
            Ok(())
        })?;
        println!("partition_column_name {:#?}", partition_column);
        Ok(Self {
            schema,
            partition_column,
            interval,
        })
    }

    fn insert(&self, db: &Connection) -> ExtResult<bool> {
        let partition_name_column = Self::partition_name_column().get_name().to_owned();
        let partition_value_column = Self::partition_value_column().get_name().to_owned();
        let sql = format!(
            "INSERT INTO {} ({partition_name_column}, {partition_value_column}) VALUES (?, ?);",
            self.name()
        );

        db.insert(&sql, params![self.partition_column, self.get_interval()])?;
        Ok(true)
    }

    /// Retrieves the interval for table partitioning
    pub fn get_interval(&self) -> i64 {
        self.interval
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use rusqlite::Connection as RusqConn;
    use sqlite3_ext::Connection;
    #[test]
    fn test_db_create_and_drop() {
        let rusq_conn = match RusqConn::open_in_memory() {
            Ok(conn) => conn,
            Err(err) => panic!("{}", err.to_string()),
        };
        let root_table = RootTable::create(
            Connection::from_rusqlite(&rusq_conn),
            "test",
            "col".to_string(),
            3600,
        );

        assert_eq!(root_table.as_ref().unwrap().schema().name(), "test_root");
        assert_eq!(&root_table.is_ok(), &true);
        let root_table = root_table.unwrap();

        let result = root_table.drop_table(Connection::from_rusqlite(&rusq_conn));

        assert!(result.is_ok());

        // println!("{:#?}", r);
    }
    #[test]
    fn test_db_create_and_connect() {
        let rusq_conn = match RusqConn::open_in_memory() {
            Ok(conn) => conn,
            Err(err) => panic!("{}", err.to_string()),
        };
        let connection = Connection::from_rusqlite(&rusq_conn);
        let root_table = RootTable::create(connection, "test", "col".to_string(), 3600).unwrap();
        root_table.insert(connection).unwrap();

        let connected_table = RootTable::connect(connection, "test");
        assert!(connected_table.is_ok());

        // println!("{:#?}", r);
    }
    // Additional tests for `create_table`, `connect`, and `insert` could be added here
}
