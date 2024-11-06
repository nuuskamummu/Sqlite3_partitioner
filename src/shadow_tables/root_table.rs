use std::ops::IndexMut;

use sqlite3_ext::params;
use sqlite3_ext::Connection;
use sqlite3_ext::FromValue;
use sqlite3_ext::Result as ExtResult;
use sqlite3_ext::ValueType;

use crate::ColumnDeclaration;
use crate::ColumnDeclarations;
use crate::PartitionColumn;

use super::operations::Connect;
use super::operations::Create;
use super::operations::Drop;
use super::operations::SchemaDeclaration;
use super::PartitionValue;

use super::operations::Table;
use super::PartitionType;

/// Represents the root table in a database partitioning scheme, which manages partition metadata.
///
/// This table tracks the partition column and the corresponding interval for dynamic partitioning
/// of data based on specified criteria, I.E the time interval specified at creation. It is a central component in
/// implementing an efficient and scalable partitioning strategy.
#[derive(Debug, Clone)]
pub struct RootTable {
    /// The name of the column used for partitioning the data.
    partition_column: String,
    /// The interval at which new partitions are created.
    interval: i64,
    /// The Lifetime of each partition expressed as seconds
    lifetime: Option<i64>,
    /// The schema declaration for the root table, detailing its structure.
    schema: SchemaDeclaration,
}

impl Table for RootTable {
    /// Returns the schema declaration of the root table.
    fn schema(&self) -> &SchemaDeclaration {
        &self.schema
    }
    /// Specifies the postfix for the root table's name to distinguish it from other table types.
    const POSTFIX: &'static str = "root";
}
impl Create for RootTable {}
impl Drop for RootTable {}
impl Connect for RootTable {}

impl PartitionType for RootTable {
    /// The column name storing partition identifier, E.G the column which is used for partitioning the table will be stored in this column.
    const PARTITION_NAME_COLUMN: &'static str = "partition_column";
    /// The column name storing partition values, the specified interval will be stored here as a
    /// integer value in seconds. E.G 3600 if the interval was set to 1 hour.
    const PARTITION_VALUE_COLUMN: &'static str = "partition_value";

    /// The data type of the partition value column, indicating the nature of partitioning (e.g., time intervals).
    const PARTITION_VALUE_COLUMN_TYPE: PartitionValue = PartitionValue::Interval;
    /// The data type of the partition name column, typically text for naming partitions.
    const PARTITION_NAME_COLUMN_TYPE: ValueType = ValueType::Text;
    const COLUMNS: &'static [crate::ColumnDeclaration] = &[
        Self::PARTITION_IDENTIFIER,
        Self::PARTITION_TYPE,
        ColumnDeclaration::new(
            std::borrow::Cow::Borrowed(Self::PARTITION_LIFETIME_COLUMN),
            Self::PARTITION_LIFETIME_COLUMN_TYPE,
        ),
    ];
}

impl RootTable {
    const PARTITION_LIFETIME_COLUMN: &'static str = "lifetime";
    const PARTITION_LIFETIME_COLUMN_TYPE: ValueType = ValueType::Integer;
    /// Accesses the partition column name.
    pub fn partition_column(&self) -> &str {
        &self.partition_column
    }
    /// Constructs and initializes a new `RootTable` instance with the provided specifications,
    /// including the creation of the table schema in the database.
    ///
    /// Parameters:
    /// - `db`: Database connection for executing the creation.
    /// - `base_name`: Base name for the table, used to derive the full table name.
    /// - `partition_column`: Name of the column to be used for partitioning.
    /// - `interval`: Interval value for creating new partitions.
    ///
    /// Returns a newly created `RootTable` instance.
    pub fn create(
        db: &Connection,
        base_name: &str,
        partition_column: String,
        interval: i64,
        lifetime: Option<i64>,
    ) -> ExtResult<Self> {
        let table_name = Self::format_name(base_name);
        let columns = <Self as PartitionType>::columns();
        let schema = <Self as Create>::schema(db, table_name, columns)?;
        let table = Self {
            partition_column,
            interval,
            lifetime,
            schema,
        };
        table.insert(db)?;

        Ok(table)
    }

    /// Connects to an existing `RootTable` based on the base name, retrieving its schema
    /// and configuration from the database.
    ///
    /// Parameters:
    /// - `db`: Database connection for querying the table.
    /// - `base_name`: Base name of the table to connect to.
    ///
    /// Returns the connected `RootTable` instance.
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
        let query = format!("SELECT {columns} FROM {table_name}");
        let mut partition_column: String = String::default();
        let mut interval: i64 = 0i64;
        let mut lifetime: Option<i64> = None;
        db.query_row(&query, (), |row| {
            let column_count = row.len();
            for index in 0..column_count {
                let column = row.index_mut(index);
                let name = column.name()?;
                if name.eq(<Self as PartitionType>::COLUMNS[0].get_name()) {
                    partition_column = column.get_str()?.to_owned();
                } else if name.eq(<Self as PartitionType>::COLUMNS[1].get_name()) {
                    interval = column.get_i64();
                } else if name.eq(<Self as PartitionType>::COLUMNS[2].get_name()) {
                    lifetime = Some(column.get_i64());
                }
            }
            Ok(())
        })?;
        Ok(Self {
            schema,
            partition_column,
            interval,
            lifetime,
        })
    }

    /// Inserts partition metadata into the root table, recording a new partition's details.
    /// This should only be executed once, at creation. Only one row should be present in the root
    /// table
    /// Parameters:
    /// - `db`: Database connection for the insert operation.
    ///
    /// Returns a boolean indicating success of the insertion.
    fn insert(&self, db: &Connection) -> ExtResult<bool> {
        let partition_name_column = Self::COLUMNS[0].get_name().to_owned();
        let partition_value_column = Self::COLUMNS[1].get_name().to_owned();
        let partition_lifetime_column = Self::COLUMNS[2].get_name().to_owned();

        let sql = format!(
            "INSERT INTO {} ({partition_name_column}, {partition_value_column}, {partition_lifetime_column}) VALUES (?, ?, ?);",
            self.name()
        );
        println!("lifetime {:#?}", self.lifetime);
        db.insert(
            &sql,
            params![self.partition_column, self.get_interval(), self.lifetime], //TODO: Fix proper expiration
                                                                                //handling
        )?;
        Ok(true)
    }

    /// Retrieves the interval at which new partitions are created for the table.
    ///
    /// Returns the interval value as an `i64`.
    pub fn get_interval(&self) -> i64 {
        self.interval
    }
    pub fn get_lifetime(&self) -> Option<i64> {
        self.lifetime
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
            None,
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
        let root_table =
            RootTable::create(connection, "test", "col".to_string(), 3600, None).unwrap();
        root_table.insert(connection).unwrap();

        let connected_table = RootTable::connect(connection, "test");
        assert!(connected_table.is_ok());

        // println!("{:#?}", r);
    }
    #[test]
    fn test_db_create_and_connect_with_lifetime() {
        let rusq_conn = match RusqConn::open_in_memory() {
            Ok(conn) => conn,
            Err(err) => panic!("{}", err.to_string()),
        };
        let connection = Connection::from_rusqlite(&rusq_conn);
        let root_table =
            RootTable::create(connection, "test", "col".to_string(), 3600, Some(3600)).unwrap();
        root_table.insert(connection).unwrap();

        let connected_table = RootTable::connect(connection, "test");
        assert!(connected_table.is_ok());

        // println!("{:#?}", r);
    }
    // Additional tests for `create_table`, `connect`, and `insert` could be added here
}
