use sqlite3_ext::query::{Statement, ToParam};
use sqlite3_ext::{Connection, FallibleIterator};
use sqlite3_ext::{FallibleIteratorMut, FromValue, Result};
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::RwLock;

/// A constant representing the postfix appended to the names of lookup tables.

static LOOKUP_TABLE_POSTFIX: &str = "lookup";

/// Defines behavior for managing lookup tables, including creation, connection, data insertion,
/// and retrieval based on partitioning logic.
///
/// This trait encapsulates methods required for creating lookup tables, generating SQL queries for
/// creation and insertion, connecting to existing tables, managing and accessing partition information.
pub trait Lookup<T> {
    /// Generates a SQL query string to create a new lookup table.
    ///
    /// # Returns
    /// - `String`: A SQL query string for creating a new lookup table.
    fn create_table_query(&self) -> String;

    /// Generates a SQL query string for inserting data into the lookup table.
    ///
    /// # Returns
    /// - `String`: A SQL query string for inserting partition information into the lookup table.
    fn insert_query(&self) -> String;

    /// Creates the lookup table in the specified database connection.
    ///
    /// # Parameters
    /// - `db`: A reference to the database connection.
    ///
    /// # Returns
    /// - `Result<bool>`: Returns `Ok(true)` if the table creation was successful.
    fn create_table(&self, db: &Connection) -> Result<bool>;

    /// Retrieves the full name of the lookup table, including the base name and postfix.
    ///
    /// # Returns
    /// - `String`: The full name of the lookup table.
    fn get_lookup_table_name(&self) -> String;

    /// Connects to an existing lookup table in the database and returns an instance of the implementing type.
    ///
    /// # Parameters
    /// - `db`: A reference to the database connection.
    /// - `name`: The base name of the lookup table to connect to.
    ///
    /// # Returns
    /// - `Result<Self>`: An instance of the implementing type representing the connected lookup table.
    fn connect(db: &Connection, name: &str) -> Result<Self>
    where
        Self: Sized;

    /// Inserts a new partition into the lookup table and updates the internal partition management structure.
    ///
    /// # Parameters
    /// - `db`: A reference to the database connection.
    /// - `partition_value`: The partition value to insert.
    ///
    /// # Returns
    /// - `Result<String>`: The name of the created or updated partition table.
    fn insert(&self, db: &Connection, partition_value: T) -> Result<String>;

    /// Creates a new instance of the implementing type representing a lookup table with specified partitions.
    ///
    /// # Parameters
    /// - `name`: The base name of the lookup table.
    /// - `partitions`: A vector of `PartitionTable` representing initial partitions.
    ///
    /// # Returns
    /// - `Result<Self>`: An instance of the implementing type.
    fn create(name: &str, partitions: Vec<PartitionTable>) -> Result<Self>
    where
        Self: Sized;

    /// Retrieves information about a specific partition based on a bucket value, including whether
    /// a new partition needs to be created.
    ///
    /// # Parameters
    /// - `db`: A reference to the database connection.
    /// - `bucket`: The bucket value to lookup.
    ///
    /// # Returns
    /// - `Result<(String, bool)>`: The name of the partition table and a boolean indicating if it needs to be created.
    fn get_partition(&self, db: &Connection, bucket: T) -> Result<(String, bool)>;

    /// Synchronizes the internal partitions map with the current state of the database.
    ///
    /// # Parameters
    /// - `db`: A reference to the database connection.
    ///
    /// # Returns
    /// - `Result<()>`: Indicates successful synchronization or provides an error.
    fn sync(&self, db: &Connection) -> Result<()>;

    /// Performs a custom synchronization of the partitions map based on a specified condition.
    ///
    /// # Parameters
    /// - `db`: A reference to the database connection.
    /// - `where_clause`: A SQL WHERE clause to filter the synchronization process.
    ///
    /// # Returns
    /// - `Result<()>`: Indicates successful synchronization or provides an error.
    fn custom_sync(&self, db: &Connection, where_clause: String) -> Result<()>;

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
    ) -> Result<Vec<(T, String)>>;
    fn drop_table_query(&self) -> String;
}
#[derive(Debug, Clone)]
/// Represents a partition table with a specific name and value.
pub struct PartitionTable {
    name: String,
    value: i64,
}
/// Represents a lookup table with a base name and a map of partitions.

#[derive(Debug)]
pub struct LookupTable<T> {
    base_name: String,
    pub partitions: RwLock<BTreeMap<T, String>>,
}
impl Lookup<i64> for LookupTable<i64> {
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
    fn create(name: &str, partitions: Vec<PartitionTable>) -> Result<Self> {
        let partition_tree: RwLock<BTreeMap<i64, String>> = RwLock::new(
            partitions
                .into_iter()
                .map(|partition| (partition.value, partition.name))
                .collect(),
        );

        Ok(LookupTable {
            partitions: partition_tree,
            base_name: name.to_string(),
        })
    }

    /// Executes the SQL query to create the lookup table in the specified database connection.
    ///
    /// # Parameters
    /// - `db`: A reference to the database connection.
    ///
    /// # Returns
    /// - `Result<bool>`: `Ok(true)` if the table is successfully created.
    fn create_table(&self, db: &Connection) -> Result<bool> {
        let sql = self.create_table_query();

        Connection::execute(db, &sql, ())?;
        Ok(true)
    }

    /// Generates the SQL query to create the lookup table in the database.
    ///
    /// This query includes creating a table with columns for the partition table name and partition value.
    ///
    /// # Returns
    /// - `String`: The SQL query string.
    fn create_table_query(&self) -> String {
        format!(
            "CREATE TABLE {} (partition_table varchar UNIQUE, partition_value integer UNIQUE);",
            self.get_lookup_table_name()
        )
    }
    fn drop_table_query(&self) -> String {
        format!("DROP TABLE {}", self.get_lookup_table_name())
    }

    /// Generates the SQL query for inserting a new partition into the lookup table.
    ///
    /// # Returns
    /// - `String`: The SQL insert query string.
    fn insert_query(&self) -> String {
        format!(
            "INSERT INTO {} (partition_table, partition_value) VALUES (?, ?)",
            self.get_lookup_table_name()
        )
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
    fn get_partition(&self, db: &Connection, bucket: i64) -> Result<(String, bool)> {
        self.sync(db)?;
        let borrowed_partitions = self.partitions.read().map_err(|err| {
            sqlite3_ext::Error::Sqlite(1, Some(format!("Error reading partitions: {}", err)))
        })?;

        if let Some(v) = borrowed_partitions.get(&bucket) {
            return Ok((v.to_owned(), false));
        }
        drop(borrowed_partitions);
        Ok((self.insert(db, bucket)?, true))
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
    fn sync(&self, db: &Connection) -> Result<()> {
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

        // Prepare SQL query using placeholders for the collected partition values.
        let sql = format!(
            "SELECT partition_value, partition_table FROM {} WHERE partition_value NOT IN ({});",
            self.get_lookup_table_name(),
            placeholders
        ); // Prepare and execute the query, handling errors uniformly.
        let mut statement = db.prepare(&sql).map_err(|err| {
            sqlite3_ext::Error::Sqlite(1, Some(format!("Error preparing SQL statement: {}", err)))
        })?;

        // Execute the query with the collected partition values as parameters.
        let results = statement.query(partition_values).map_err(|err| {
            sqlite3_ext::Error::Sqlite(1, Some(format!("Error executing SQL query: {}", err)))
        })?;

        // Iterate over query results to update partitions map.
        while let Ok(Some(row)) = results.next() {
            let partition_value = row[0].get_i64();
            let partition_table_name = row[1].get_str()?;
            borrowed_partitions.insert(partition_value, partition_table_name.to_string());
        }
        drop(borrowed_partitions);
        Ok(())
    }

    /// Performs a custom synchronization of the partitions map based on a specified SQL `WHERE` clause.
    ///
    /// This method allows for selective synchronization of partitions that meet certain conditions.
    ///
    /// # Parameters
    /// - `db`: A reference to the database connection.
    /// - `where_clause`: The SQL `WHERE` clause specifying the conditions for synchronization.
    ///
    /// # Returns
    /// - `Result<()>`: Indicates success or failure of the custom synchronization process.
    fn custom_sync(&self, db: &Connection, where_clause: String) -> Result<()> {
        let mut borrowed_partitions = self.partitions.write().map_err(|err| {
            sqlite3_ext::Error::Sqlite(
                1,
                Some(format!(
                    "Error acquiring write permissions to partitions: {}",
                    err
                )),
            )
        })?;
        borrowed_partitions.clear();
        let local_partition_values = borrowed_partitions.keys().cloned().collect::<Vec<i64>>();

        let variadric_params = std::iter::repeat("?")
            .take(local_partition_values.len()) // Directly use the length of the keys vector
            .collect::<Vec<&str>>()
            .join(",");

        let sql = format!(
            "SELECT * FROM {} WHERE partition_value NOT IN ({}) AND  {};",
            self.get_lookup_table_name(),
            variadric_params,
            where_clause
        );

        let mut statement = db.prepare(&sql)?;

        let results = statement.query(local_partition_values)?;
        while let Ok(Some(row)) = results.next() {
            let partition_value = row[1].get_i64();

            let partition_table_name = row[0].get_str().to_owned()?;
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
    fn get_partitions_by_range(
        &self,
        db: &Connection,
        from: Bound<i64>,
        to: Bound<i64>,
    ) -> Result<Vec<(i64, String)>> {
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
        let range = borrowed_partitions.range((from, to));
        let pair = range
            .map(|(key, value)| (*key, value.clone()))
            .collect::<Vec<(i64, String)>>();
        Ok(pair)
    }

    /// Returns the full name of the lookup table by combining the base name with a predefined postfix.
    ///
    /// # Returns
    /// - `String`: The full name of the lookup table.
    fn get_lookup_table_name(&self) -> String {
        format!("{}_{}", self.base_name, LOOKUP_TABLE_POSTFIX)
    }

    /// Connects to an existing lookup table in the database, initializing the `LookupTable` instance
    /// based on the retrieved partitions.
    ///
    /// # Parameters
    /// - `db`: A reference to the database connection.
    /// - `name`: The base name of the lookup table to connect to.
    ///
    /// # Returns
    /// - `Result<Self>`: An instance of `LookupTable`.
    fn connect(db: &Connection, name: &str) -> Result<Self> {
        let sql = format!("SELECT * FROM {}_{};", name, LOOKUP_TABLE_POSTFIX);
        let results = db
            .prepare(&sql)?
            .query(())?
            .map(|row| {
                Ok(PartitionTable {
                    name: row[0].get_str()?.to_owned(),
                    value: row[1].get_i64(),
                })
            })
            .collect()
            .to_owned()?;

        println!("{}", sql);
        Lookup::create(name, results)
    }

    /// Inserts a new partition into the lookup table and updates the internal partitions map.
    ///
    /// # Parameters
    /// - `db`: A reference to the database connection.
    /// - `partition_value`: The value of the new partition to insert.
    ///
    /// # Returns
    /// - `Result<String>`: The name of the newly inserted partition table.
    fn insert(&self, db: &Connection, partition_value: i64) -> Result<String> {
        let partition_table_name = format!("{}_{}", self.base_name, partition_value);

        Connection::prepare(db, &self.insert_query())?.execute(|stmt: &mut Statement| {
            partition_table_name.bind_param(stmt, 1)?;
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

        borrowed_partitions.insert(partition_value, partition_table_name.clone());

        Ok(partition_table_name)
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use rusqlite::Connection as RusqConn;
//     use rusqlite::Result as SqlResult;
//     use sqlite3_ext::Connection;
//     use std::sync::{Arc, RwLock};
//
//     // Function to set up an in-memory database connection
//     fn setup_db<'a>(rusq_conn: &'a RusqConn) -> &'a Connection {
//         let conn = Connection::from_rusqlite(rusq_conn);
//         conn
//     }
//     fn init_rusq_conn() -> RusqConn {
//         RusqConn::open_in_memory().unwrap()
//     }
//
//     // Function to create a LookupTable instance for testing
//     fn setup_lookup_table() -> LookupTable {
//         LookupTable {
//             base_name: "test".to_string(),
//             partitions: RwLock::new(std::collections::BTreeMap::new()),
//         }
//     }
//     #[test]
//     fn test_create_table_query() {
//         let lookup_table = setup_lookup_table();
//         let query = lookup_table.create_table_query();
//         assert_eq!(
//             query,
//             "CREATE TABLE test_lookup (partition_table varchar, partition_value integer);"
//         );
//     }
//     #[test]
//     fn test_insert() -> SqlResult<()> {
//         let rusq_conn = init_rusq_conn();
//         let db = setup_db(&rusq_conn);
//         let lookup_table = setup_lookup_table();
//         lookup_table.create_table(db).unwrap();
//         let partition_value = 1i64;
//         let expected_table_name = format!("{}_{}", lookup_table.base_name, partition_value);
//
//         lookup_table.insert(&db, partition_value)?;
//
//         let partition = lookup_table.get_partition(db, partition_value).unwrap();
//         assert_eq!(partition.1, false);
//         assert_eq!(partition.0, expected_table_name);
//
//         Ok(())
//     }
//     #[test]
//     fn test_sync() -> sqlite3_ext::Result<()> {
//         let rusq_conn = init_rusq_conn();
//         let db = setup_db(&rusq_conn);
//         let lookup_table = setup_lookup_table();
//         lookup_table.create_table(db).unwrap();
//         // Pre-insert a partition to simulate existing database state
//         let partition_value = 1i64;
//         lookup_table.insert(&db, partition_value)?;
//
//         // Clear the in-memory map to simulate out-of-sync state
//         {
//             let mut partitions = lookup_table.partitions.write().unwrap();
//             partitions.clear();
//         }
//
//         // Run sync to update in-memory map
//         lookup_table.sync(&db)?;
//
//         let partition = lookup_table.get_partition(db, partition_value)?;
//         assert_eq!(partition.0, "test_1".to_string());
//
//         Ok(())
//     }
// }
