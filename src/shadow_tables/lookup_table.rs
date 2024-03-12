use sqlite3_ext::query::{Statement, ToParam};
use sqlite3_ext::{Connection, Value, ValueRef, ValueType};
use sqlite3_ext::{FallibleIteratorMut, FromValue, Result as ExtResult};
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::RwLock;

use crate::utils::parse_to_unix_epoch;
use crate::ColumnDeclaration;

use super::operations::{Connect, Create, Drop, SchemaDeclaration, Table};
use super::{PartitionType, PartitionValue};

/// A constant representing the postfix appended to the names of lookup tables.

/// Defines behavior for managing lookup tables, including creation, connection, data insertion,
/// and retrieval based on partitioning logic.
///
/// This trait encapsulates methods required for creating lookup tables, generating SQL queries for
/// creation and insertion, connecting to existing tables, managing and accessing partition information.
pub trait Lookup<T> {
    /// Retrieves information about a specific partition based on a bucket value, including whether
    /// a new partition needs to be created.
    ///
    /// # Parameters
    /// - `db`: A reference to the database connection.
    /// - `bucket`: The bucket value to lookup.
    ///
    /// # Returns
    /// - `Result<(String, bool)>`: The name of the partition table and a boolean indicating if it needs to be created.
    fn get_partition(&self, db: &Connection, bucket: T) -> ExtResult<(String, bool)>;

    /// Synchronizes the internal partitions map with the current state of the database.
    ///
    /// # Parameters
    /// - `db`: A reference to the database connection.
    ///
    /// # Returns
    /// - `Result<()>`: Indicates successful synchronization or provides an error.
    fn sync(&self, db: &Connection) -> ExtResult<()>;

    /// Performs a custom synchronization of the partitions map based on a specified condition.
    ///
    /// # Parameters
    /// - `db`: A reference to the database connection.
    /// - `where_clause`: A SQL WHERE clause to filter the synchronization process.
    ///
    /// # Returns
    /// - `Result<()>`: Indicates successful synchronization or provides an error.
    fn custom_sync(&self, db: &Connection, where_clause: String) -> ExtResult<()>;

    /// Retrieves a list of partitions within a specified range of partition values.
    ///
    /// # Parameters
    /// - `db`: A reference to the database connection.
    /// - `include`: The lower bound of the range (inclusive or exclusive).
    /// - `exclude`: The upper bound of the range (inclusive or exclusive).
    ///
    /// # Returns
    /// - `Result<Vec<(i64, String)>>`: A list of partition values and their corresponding table names within the specified range.
    fn get_partitions_by_range(
        &self,
        db: &Connection,
        from: Bound<T>,
        to: Bound<T>,
    ) -> ExtResult<Vec<(T, String)>>;
    // fn drop_table_query(&self) -> String;
    fn parse_partition_value(value: &Value, interval: T) -> sqlite3_ext::Result<T>;
}

impl PartitionType for LookupTable<i64> {
    const PARTITION_NAME_COLUMN: &'static str = "partition_table";
    const PARTITION_VALUE_COLUMN: &'static str = "partition_value";
    const PARTITION_VALUE_COLUMN_TYPE: &'static PartitionValue = &PartitionValue::Interval;
    const PARTITION_NAME_COLUMN_TYPE: &'static ValueType = &ValueType::Text;
}
// type LookUpSchema = Schema;
impl Table for LookupTable<i64> {
    const POSTFIX: &'static str = "lookup";
    fn schema(&self) -> &SchemaDeclaration {
        &self.schema
    }
}
impl Create for LookupTable<i64> {
    fn table_query(schema: &SchemaDeclaration) -> Result<String, String> {
        Ok(format!(
            "CREATE TABLE {} ({} UNIQUE, {} UNIQUE);",
            schema.name(),
            <Self as PartitionType>::partition_name_column(),
            <Self as PartitionType>::partition_value_column()
        ))
    }
}
impl Drop for LookupTable<i64> {}
impl Connect for LookupTable<i64> {}
#[derive(Debug)]
pub struct LookupTable<T> {
    pub(super) schema: SchemaDeclaration,
    // base_name: String,
    pub partitions: RwLock<BTreeMap<T, String>>,
}
impl LookupTable<i64> {
    fn parse_partition_value(value: &ValueRef, interval: i64) -> sqlite3_ext::Result<i64> {
        parse_to_unix_epoch(value).map(|epoch| epoch - epoch % interval)
    }

    pub fn partition_table_column(&self) -> ColumnDeclaration {
        <Self as PartitionType>::partition_name_column()
    }
    pub fn partition_value_column(&self) -> ColumnDeclaration {
        <Self as PartitionType>::partition_value_column()
    }

    /// Creates a new `LookupTable` instance with a specified base name and initial partitions.
    ///
    /// This method initializes the lookup table's partitions map with the given partitions and sets the base name.
    ///
    /// # Parameters
    /// - `name`: The base name of the lookup table.
    /// - `partitions`: A vector of `PartitionTable` representing the initial partitions.
    ///
    /// # Returns
    /// - `Result<Self>`: An instance of `LookupTable`.
    pub fn create(db: &Connection, base_name: &str) -> ExtResult<Self> {
        // let partition_tree: RwLock<BTreeMap<i64, String>> = RwLock::new(
        //     partitions
        //         .into_iter()
        //         .map(|partition| (partition.value, partition.name))
        //         .collect(),
        // );
        let table_name = Self::format_name(base_name);
        let columns = <Self as PartitionType>::columns();
        let schema = <Self as Create>::schema(db, table_name.to_string(), columns)?;
        Ok(LookupTable {
            partitions: RwLock::default(),
            schema,
        })
    }

    /// Executes the SQL query to create the lookup table in the specified database connection.
    ///
    /// # Parameters
    /// - `db`: A reference to the database connection.
    ///
    /// # Returns
    /// - `Result<bool>`: `Ok(true)` if the table is successfully created.

    /// Generates the SQL query to create the lookup table in the database.
    ///
    /// This query includes creating a table with columns for the partition table name and partition value.
    ///
    /// # Returns
    /// - `String`: The SQL query string.

    /// Generates the SQL query for inserting a new partition into the lookup table.
    ///
    /// # Returns
    /// - `String`: The SQL insert query string.
    fn insert_query(&self) -> String {
        let partition_table_name = self.partition_table_column().get_name().to_owned();
        let partition_value_name = self.partition_value_column().get_name().to_owned();
        let sql = format!(
            "INSERT INTO {} ({partition_table_name}, {partition_value_name}) VALUES (?, ?)",
            self.name()
        );
        sql
    }
    /// Retrieves a partition from the lookup table based on the provided bucket value.
    ///
    /// If the partition does not exist, it attempts to insert a new partition. This method also
    /// ensures the lookup table is synchronized before fetching the partition.
    ///
    /// # Parameters
    /// - `db`: A reference to the database connection.
    /// - `bucket`: The bucket value for which to retrieve the partition.
    ///
    /// # Returns
    /// - `Result<(String, bool)>`: The name of the partition table and a boolean indicating
    ///   whether the table needs to be created.
    pub fn get_partition(&self, partition_value: &i64) -> sqlite3_ext::Result<Option<String>> {
        let borrowed_partitions = self.partitions.read().map_err(|err| {
            sqlite3_ext::Error::Sqlite(1, Some(format!("Error reading partitions: {}", err)))
        })?;

        Ok(borrowed_partitions
            .get(partition_value)
            .map(|name| name.to_owned()))
    }

    /// Synchronizes the in-memory partitions map with the current state of the lookup table in the database.
    ///
    /// This method ensures that the partitions map reflects the actual partitions present in the database.
    ///
    /// # Parameters
    /// - `db`: A reference to the database connection.
    ///
    /// # Returns
    /// - `Result<()>`: Indicates success or failure of the synchronization process.
    pub fn sync(&self, db: &Connection) -> ExtResult<()> {
        // Acquire a write lock on partitions upfront, simplifying error handling.
        let mut borrowed_partitions = self.partitions.write().map_err(|err| {
            sqlite3_ext::Error::Sqlite(
                1,
                Some(format!("Error acquiring write lock on partitions: {}", err)),
            )
        })?;

        // Collect existing partition values to use in the query.
        let partition_values: Vec<i64> = borrowed_partitions.keys().copied().collect();
        let placeholders = std::iter::repeat("?")
            .take(partition_values.len())
            .collect::<Vec<_>>()
            .join(",");
        let sql = if !placeholders.is_empty() {
            format!(
                "SELECT {}, {} FROM {} WHERE {} NOT IN ({});",
                self.partition_value_column().get_name(),
                self.partition_table_column().get_name(),
                self.name(),
                self.partition_value_column().get_name(),
                placeholders
            )
        } else {
            format!(
                "SELECT {}, {} FROM {};",
                self.partition_value_column().get_name(),
                self.partition_table_column().get_name(),
                self.name(),
            )
        };
        // Prepare SQL query using placeholders for the collected partition values.

        let mut statement = db.prepare(&sql).map_err(|err| {
            sqlite3_ext::Error::Sqlite(1, Some(format!("Error preparing SQL statement: {}", err)))
        })?;

        // Execute the query with the collected partition values as parameters.
        let results = statement.query(partition_values).map_err(|err| {
            sqlite3_ext::Error::Sqlite(1, Some(format!("Error executing SQL query: {}", err)))
        })?;

        while let Ok(Some(row)) = results.next() {
            let partition_value = row[0].get_i64();
            let partition_table_name = row[1].get_str()?;
            borrowed_partitions.insert(partition_value, partition_table_name.to_string());
        }

        drop(borrowed_partitions);

        Ok(())
    }

    /// Retrieves a list of partitions within a specified range of partition values.
    ///
    /// This method filters partitions based on the provided bounds and returns their names along with their values.
    ///
    /// # Parameters
    /// - `db`: A reference to the database connection.
    /// - `include`: The lower bound of the range (inclusive or exclusive).
    /// - `exclude`: The upper bound of the range (inclusive or exclusive).
    ///
    /// # Returns
    /// - `Result<Vec<(i64, String)>>`: A vector of tuples containing partition values and their corresponding table names.
    pub fn get_partitions_by_range(
        &self,
        db: &Connection,
        from: &Bound<i64>,
        to: &Bound<i64>,
    ) -> ExtResult<Vec<(i64, String)>> {
        self.sync(db)?;
        let borrowed_partitions = self.partitions.read().map_err(|err| {
            sqlite3_ext::Error::Sqlite(
                1,
                Some(format!(
                    "Error acquiring read permissions to partitions: {}",
                    err
                )),
            )
        })?;
        let range = borrowed_partitions.range((*from, *to));
        let pair = range
            .map(|(key, value)| (*key, value.to_string()))
            .collect::<Vec<(i64, String)>>();
        Ok(pair)
    }

    /// Returns the full name of the lookup table by combining the base name with a predefined postfix.
    ///
    /// # Returns
    /// - `String`: The full name of the lookup table.
    // fn get_lookup_table_name(&self) -> String {
    //     format!("{}_{}", , Self::POSTFIX)
    // }

    /// Connects to an existing lookup table in the database, initializing the `LookupTable` instance
    /// based on the retrieved partitions.
    ///
    /// # Parameters
    /// - `db`: A reference to the database connection.
    /// - `name`: The base name of the lookup table to connect to.
    ///
    /// # Returns
    /// - `Result<Self>`: An instance of `LookupTable`.
    pub fn connect(db: &Connection, base_name: &str) -> ExtResult<Self> {
        let table_name = &Self::format_name(base_name);
        let schema = <Self as Connect>::schema(db, table_name)?;
        let table = Self {
            partitions: RwLock::new(std::collections::BTreeMap::new()),
            schema,
        };
        table.sync(db)?;
        Ok(table)
    }

    /// Inserts a new partition into the lookup table and updates the internal partitions map.
    ///
    /// # Parameters
    /// - `db`: A reference to the database connection.
    /// - `partition_value`: The value of the new partition to insert.
    ///
    /// # Returns
    /// - `Result<String>`: The name of the newly inserted partition table.
    pub(crate) fn insert<'a>(
        &'a self,
        db: &Connection,
        partition_name: &'a str,
        partition_value: i64,
    ) -> ExtResult<&str> {
        Connection::prepare(db, &self.insert_query())?.execute(|stmt: &mut Statement| {
            partition_name.bind_param(stmt, 1)?;
            partition_value.bind_param(stmt, 2)?;

            Ok(())
        })?;

        let mut borrowed_partitions = self.partitions.write().map_err(|err| {
            sqlite3_ext::Error::Sqlite(
                1,
                Some(format!(
                    "Error acquiring write permissions to partitions: {}",
                    err
                )),
            )
        })?;

        borrowed_partitions.insert(partition_value, partition_name.to_string());

        Ok(partition_name)
    }
}

#[cfg(test)]
mod tests {
    use crate::shadow_tables::interface::VirtualTable;
    use crate::ColumnDeclarations;

    use super::*;

    use rusqlite::Connection as RusqConn;
    use rusqlite::Result as SqlResult;
    use sqlite3_ext::Connection;

    // Function to set up an in-memory database connection
    fn setup_db(rusq_conn: &RusqConn) -> &Connection {
        let conn = Connection::from_rusqlite(rusq_conn);
        conn
    }
    fn init_rusq_conn() -> RusqConn {
        RusqConn::open_in_memory().unwrap()
    }

    fn setup_lookup_table<'a>(db: &'a Connection) -> VirtualTable<'a> {
        let declarations =
            ColumnDeclarations::from_iter(&["col1 timestamp partition_column", "col2 text"]);

        let virutal_table =
            VirtualTable::create(db, "test", declarations, "col1".to_string(), 3600).unwrap();
        virutal_table
    }
    // #[test]
    // fn test_create_table_query() {
    //     let lookup_table = setup_lookup_table();
    //     let query = lookup_table.lookup.();
    //     assert_eq!(
    //         query,
    //         "CREATE TABLE test_lookup (partition_table varchar UNIQUE, partition_value integer UNIQUE);"
    //     );
    // }
    #[test]
    fn test_insert() -> SqlResult<()> {
        let rusq_conn = init_rusq_conn();
        let db = setup_db(&rusq_conn);
        let virtual_table = setup_lookup_table(db);
        let lookup = virtual_table.lookup();
        let partition_value = 1i64;
        let partition_name = lookup.get_partition(&partition_value)?;
        assert!(partition_name.is_none());
        let partition_name = "test_1";
        let partition = lookup.insert(virtual_table.connection, partition_name, partition_value)?;
        assert_eq!(partition, partition_name);

        Ok(())
    }
    #[test]
    fn test_sync() -> sqlite3_ext::Result<()> {
        let rusq_conn = init_rusq_conn();
        let db = setup_db(&rusq_conn);
        let virtual_table = setup_lookup_table(db);
        let lookup_table = virtual_table.lookup();
        // Pre-insert a partition to simulate existing database state
        let partition_values = [1710003600, 1710000000, 1710007200];
        for partition_value in partition_values {
            lookup_table.insert(
                virtual_table.connection,
                &format!("test_{}", partition_value),
                partition_value,
            )?;
            let partition_name = lookup_table.get_partition(&partition_value)?;
            assert!(partition_name.is_some());
        }

        // Clear the in-memory map to simulate out-of-sync state
        {
            let mut partitions = lookup_table.partitions.write().unwrap();
            partitions.clear();
        }
        for partition_value in partition_values {
            let partition_name = lookup_table.get_partition(&partition_value)?;
            assert!(partition_name.is_none());
        }

        // Run sync to update in-memory map
        lookup_table.sync(virtual_table.connection)?;

        let partition = lookup_table.get_partition(&1710003600)?.unwrap();
        assert_eq!(partition, "test_1710003600".to_string());

        Ok(())
    }
}
