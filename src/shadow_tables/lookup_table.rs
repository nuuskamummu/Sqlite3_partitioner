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

/// This trait defines the necessary methods for creating the lookup table, generating SQL queries for
/// creation and insertion, connecting to existing tables, and managing and accessing partition information
pub trait Lookup<T> {
    /// Retrieves information about a specific partition based on a given value, including whether
    /// a new partition needs to be created.
    ///
    /// Parameters:
    /// - `db`: A reference to the database connection.
    /// - `value`: The value to lookup.
    ///
    /// Returns:
    /// - A result containing the name of the partition table and a boolean indicating if it needs to be created.

    fn get_partition(&self, db: &Connection, value: T) -> ExtResult<(String, bool)>;

    /// Synchronizes the internal partitions map with the current state of the database.
    ///
    /// Parameters:
    /// - `db`: A reference to the database connection.
    ///
    /// Returns:
    /// - A result indicating successful synchronization or an error.
    fn sync(&self, db: &Connection) -> ExtResult<()>;

    /// Performs a custom synchronization of the partitions map based on a specified condition.
    ///
    /// Parameters:
    /// - `db`: A reference to the database connection.
    /// - `where_clause`: A SQL WHERE clause to filter the synchronization process.
    ///
    /// Returns:
    /// - A result indicating successful synchronization or an error.
    fn custom_sync(&self, db: &Connection, where_clause: String) -> ExtResult<()>;

    /// Retrieves a list of partitions within a specified range of partition values.
    ///
    /// Parameters:
    /// - `db`: A reference to the database connection.
    /// - `from`: The lower bound of the range (inclusive or exclusive).
    /// - `to`: The upper bound of the range (inclusive or exclusive).
    ///
    /// Returns:
    /// - A result containing a list of partition values and their corresponding table names within the specified range.
    fn get_partitions_by_range(
        &self,
        db: &Connection,
        from: Bound<T>,
        to: Bound<T>,
    ) -> ExtResult<Vec<(T, String)>>;

    /// Parses a partition value from a given `Value` instance and an interval.
    ///
    /// Parameters:
    /// - `value`: A reference to the value to be parsed.
    /// - `interval`: The interval to apply during parsing.
    ///
    /// Returns:
    /// - A result containing the parsed partition value.
    fn parse_partition_value(value: &Value, interval: T) -> sqlite3_ext::Result<T>;
}

impl PartitionType for LookupTable<i64> {
    const PARTITION_NAME_COLUMN: &'static str = "partition_table";
    const PARTITION_VALUE_COLUMN: &'static str = "partition_value";
    const PARTITION_VALUE_COLUMN_TYPE: PartitionValue = PartitionValue::Interval;
    const PARTITION_NAME_COLUMN_TYPE: ValueType = ValueType::Text;
    const COLUMNS: &'static [crate::ColumnDeclaration] = &[
        Self::PARTITION_IDENTIFIER,
        Self::PARTITION_TYPE,
        ColumnDeclaration::new(
            std::borrow::Cow::Borrowed(Self::PARTITION_EXPIRATION_COLUMN),
            Self::PARTITION_EXPIRATION_COLUMN_TYPE,
        ),
    ];
}
impl Table for LookupTable<i64> {
    const POSTFIX: &'static str = "lookup";
    fn schema(&self) -> &SchemaDeclaration {
        &self.schema
    }
}
impl Create for LookupTable<i64> {
    fn table_query(schema: &SchemaDeclaration) -> Result<String, String> {
        Ok(format!(
            "CREATE TABLE {} ({} UNIQUE, {} UNIQUE, {});",
            schema.name(),
            <Self as PartitionType>::COLUMNS[0],
            <Self as PartitionType>::COLUMNS[1],
            <Self as PartitionType>::COLUMNS[2]
        ))
    }
}
impl Drop for LookupTable<i64> {}
impl Connect for LookupTable<i64> {}
#[derive(Debug)]
pub struct LookupTable<T> {
    pub(super) schema: SchemaDeclaration,
    pub partitions: RwLock<BTreeMap<T, String>>,
}
impl LookupTable<i64> {
    const PARTITION_EXPIRATION_COLUMN: &'static str = "expires_at";
    const PARTITION_EXPIRATION_COLUMN_TYPE: ValueType = ValueType::Integer;
    fn parse_partition_value(value: &ValueRef, interval: i64) -> sqlite3_ext::Result<i64> {
        parse_to_unix_epoch(value).map(|epoch| epoch - epoch % interval)
    }

    pub fn partition_table_column(&self) -> &'static ColumnDeclaration {
        &<Self as PartitionType>::COLUMNS[0]
    }
    pub fn partition_value_column(&self) -> &'static ColumnDeclaration {
        &<Self as PartitionType>::COLUMNS[1]
    }
    pub fn expiration_column(&self) -> &'static ColumnDeclaration {
        &<Self as PartitionType>::COLUMNS[2]
    }

    /// Creates a new instance of `LookupTable` with a specified base name. This involves initializing
    /// the lookup table's partitions map and setting up the table schema according to the specified
    /// parameters.
    ///
    /// The method constructs the table name from the provided base name and prepares the schema for
    /// the lookup table. It does not populate the partitions map with any partitions; this is expected
    /// to be done through synchronization.
    ///
    /// # Parameters
    /// - `db`: A reference to the database connection. Used to prepare the schema for the lookup table.
    /// - `base_name`: The base name for the lookup table. This name is used to derive the full table
    ///   name and should be unique within the database to avoid conflicts.
    ///
    /// # Returns
    /// - `Result<Self>`: On successful creation, returns an instance of `LookupTable`. On failure,
    ///   returns an error encapsulating the issue encountered during the table creation process.
    ///
    /// # Errors
    /// This method may return an error if there are issues creating the schema for the lookup table
    /// in the database. Errors could arise from invalid table names, issues with the database connection,
    /// or problems executing the SQL commands to set up the schema. All errors are returned as
    /// `ExtResult<Self>`, providing details about the failure.
    pub fn create(db: &Connection, base_name: &str) -> ExtResult<Self> {
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
        let expiration_column_name = self.expiration_column().get_name().to_owned();

        let sql = format!(
            "INSERT INTO {} ({partition_table_name}, {partition_value_name}, {expiration_column_name}) VALUES (?, ?, ?)",
            self.name()
        );
        sql
    }
    /// Retrieves a partition from the lookup table based on the provided value.
    ///
    /// This method searches for an existing partition matching the given value. If the partition does not exist, it indicates the need for creating a new partition. The lookup table is synchronized with the database before fetching the partition to ensure the in-memory representation is up-to-date.
    ///
    /// # Parameters
    /// - `db`: A reference to the database connection.
    /// - `partition_value`: The value for which to retrieve the partition.
    ///
    /// # Returns
    /// - `Result<Option<String>>`: On success, returns an optional containing the name of the partition table if found. Returns `None` if no matching partition is found. On failure, returns an error.
    ///
    /// # Errors
    /// This method may return an error if there's a problem reading the partitions from the database or if there are issues with database connectivity.
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
    /// This method updates the partitions map to reflect the actual partitions present in the database. It is particularly useful to ensure that the in-memory representation of partitions is consistent with the database, especially after modifications such as adding or dropping partitions.
    ///
    /// # Parameters
    /// - `db`: A reference to the database connection. This connection is used to query the current state of the lookup table.
    ///
    /// # Returns
    /// - `Result<()>`: Indicates success or failure of the synchronization process. Returns `Ok(())` on successful synchronization. On failure, returns an error detailing the issue encountered.
    ///
    /// # Errors
    /// Errors may occur due to issues acquiring write locks on the partitions map, preparing the SQL statement, executing the SQL query, or reading the query results. These errors are wrapped and returned as `sqlite3_ext::Result` for handling.
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
    /// This method filters the partitions by the specified range, defined by `from` and `to` bounds, and returns their names along with their corresponding values. It ensures that the lookup table's partition map is synchronized with the database state before fetching the partition information.
    ///
    /// # Parameters
    /// - `db`: A reference to the database connection. Used for syncing the lookup table and querying partition data.
    /// - `from`: The lower bound of the partition value range. This can be inclusive or exclusive, represented as a `Bound<i64>`.
    /// - `to`: The upper bound of the partition value range, similar to `from`, represented as a `Bound<i64>`.
    ///
    /// # Returns
    /// - `Result<Vec<(i64, String)>>`: On success, returns a vector of tuples where each tuple contains a partition value and the corresponding partition table name within the specified range. On failure, returns an error.
    ///
    /// # Errors
    /// This method may return an error if issues occur during the synchronization process, acquiring read permissions for the partitions map, or if the specified range is invalid. Errors are returned as `sqlite3_ext::Result`.
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

    /// Connects to an existing lookup table in the database, initializing the `LookupTable` instance
    /// based on the retrieved schema and partitions.
    ///
    /// This method is designed to establish a connection with an already existing lookup table
    /// identified by the base name. It retrieves the table's schema and synchronizes the internal
    /// partitions map with the current state of the table in the database. This ensures that the
    /// `LookupTable` instance reflects the actual structure and partitioning information of the table
    /// at the time of connection.
    ///
    /// # Parameters
    /// - `db`: A reference to the database connection. Used for accessing the lookup table and
    ///   performing the synchronization.
    /// - `base_name`: The base name of the lookup table to connect to. This is used to identify
    ///   the table within the database.
    ///
    /// # Returns
    /// - `Result<Self>`: On successful connection, returns an instance of `LookupTable` configured
    ///   with the schema and partitions of the existing lookup table. On failure, returns an error
    ///   detailing the issue encountered during the connection process.
    ///
    /// # Errors
    /// This method may return an error if there are issues retrieving the schema for the lookup table,
    /// synchronizing the partitions map, or if the lookup table specified by the base name does not
    /// exist in the database. Additionally, errors could arise from problems with the database
    /// connection itself. All errors are returned as `ExtResult<Self>`, providing detailed information
    /// about the failure.
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
    /// This method adds a new partition with the specified name and value into the lookup table.
    /// It ensures the new partition is properly recorded in the database and updates the in-memory
    /// partitions map to reflect this addition. This method is crucial for maintaining the integrity
    /// and accuracy of the partitioning system.
    ///
    /// # Parameters
    /// - `db`: A reference to the database connection. Used to execute the insert operation in the lookup table.
    /// - `partition_name`: The name of the new partition to insert. This name should be unique within the lookup table.
    /// - `partition_value`: The value associated with the new partition. This value is used to determine the partition's position and relationship with other partitions.
    ///
    /// # Returns
    /// - `Result<&str>`: On successful insertion, returns the name of the newly inserted partition table. On failure, returns an error detailing the issue encountered during the insertion process.
    ///
    /// # Errors
    /// This method may return an error if there are issues executing the insert query, such as database connectivity problems, violations of unique constraints, or if the provided partition name or value is invalid. Errors are wrapped and returned as `ExtResult<&str>` for handling.
    pub(crate) fn insert<'a>(
        &'a self,
        db: &Connection,
        partition_name: &'a str,
        partition_value: i64,
        expires_at: Option<i64>,
    ) -> ExtResult<&str> {
        Connection::prepare(db, &self.insert_query())?.execute(|stmt: &mut Statement| {
            partition_name.bind_param(stmt, 1)?;
            partition_value.bind_param(stmt, 2)?;
            expires_at.bind_param(stmt, 3)?;

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

        let virtual_table =
            VirtualTable::create(db, "test", declarations, "col1".to_string(), 3600, None).unwrap();
        virtual_table
    }
    #[test]
    fn test_create_table_query() {
        let rusq_conn = init_rusq_conn();
        let db = setup_db(&rusq_conn);
        let virtual_table = setup_lookup_table(db);
        let lookup = virtual_table.lookup();
        let query = LookupTable::table_query(lookup.schema()).unwrap();
        assert_eq!(
            query,
            "CREATE TABLE test_lookup (partition_table TEXT UNIQUE, partition_value INTEGER UNIQUE, expires_at INTEGER);"
        );
    }
    #[test]
    fn test_connect_to_lookup() {
        let rusq_conn = init_rusq_conn();
        let db = setup_db(&rusq_conn);
        let table = LookupTable::connect(db, "test");
        assert!(table.is_err());
        setup_lookup_table(db);
        let table = LookupTable::connect(db, "test");
        assert!(table.is_ok())
    }
    #[test]
    fn test_insert() -> SqlResult<()> {
        let rusq_conn = init_rusq_conn();
        let db = setup_db(&rusq_conn);
        let virtual_table = setup_lookup_table(db);
        let lookup = virtual_table.lookup();
        let lifetime = virtual_table.lifetime();
        let partition_value = 1i64;
        let partition_name = lookup.get_partition(&partition_value)?;
        assert!(partition_name.is_none());
        let partition_name = "test_1";
        let partition = lookup.insert(
            virtual_table.connection,
            partition_name,
            partition_value,
            lifetime,
        )?;
        assert_eq!(partition, partition_name);

        Ok(())
    }
    #[test]
    fn test_sync() -> sqlite3_ext::Result<()> {
        let rusq_conn = init_rusq_conn();
        let db = setup_db(&rusq_conn);
        let virtual_table = setup_lookup_table(db);
        let lookup_table = virtual_table.lookup();
        let lifetime = virtual_table.lifetime();
        // Pre-insert a partition to simulate existing database state
        let partition_values = [1710003600, 1710000000, 1710007200];
        for partition_value in partition_values {
            lookup_table.insert(
                virtual_table.connection,
                &format!("test_{}", partition_value),
                partition_value,
                lifetime,
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
    #[test]
    fn test_get_by_range() -> sqlite3_ext::Result<()> {
        let rusq_conn = init_rusq_conn();
        let db = setup_db(&rusq_conn);
        let virtual_table = setup_lookup_table(db);
        let lookup_table = virtual_table.lookup();
        let lifetime = virtual_table.lifetime();
        // Pre-insert a partition to simulate existing database state
        let partition_values = [1710003600, 1710000000, 1710007200];
        for partition_value in partition_values {
            lookup_table.insert(
                virtual_table.connection,
                &format!("test_{}", partition_value),
                partition_value,
                lifetime,
            )?;
            let partition_name = lookup_table.get_partition(&partition_value)?;
            assert!(partition_name.is_some());
        }
        let partitions = lookup_table.get_partitions_by_range(
            db,
            &Bound::Included(1710000000),
            &Bound::Excluded(1710007200),
        )?;
        assert_eq!(partitions[0].1, "test_1710000000");
        assert_eq!(partitions[1].1, "test_1710003600");
        assert!(partitions.len() == 2);
        Ok(())
    }
}
