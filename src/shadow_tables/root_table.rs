use sqlite3_ext::query::Statement;
use sqlite3_ext::query::ToParam;
use sqlite3_ext::Connection;
use sqlite3_ext::FromValue;
use sqlite3_ext::Result;

/// A constant representing the postfix appended to the names of root tables.
static ROOT_TABLE_POSTFIX: &str = "root";

/// Defines behavior for managing root tables, including creation, connection, and data manipulation.
///
/// This trait encapsulates methods required to create root tables, generate creation queries, connect to
/// existing tables, insert data, and manage table-specific properties like partition intervals.
pub trait Root {
    /// Generates a SQL query string to create a new root table.
    ///
    /// # Returns
    /// - `String`: A SQL query string for creating a new root table.
    fn create_table_query(&self) -> String;

    /// Creates the root table in the specified database connection.
    ///
    /// # Parameters
    /// - `db`: A reference to the database connection.
    ///
    /// # Returns
    /// - `Result<bool>`: Returns `Ok(true)` if the table creation was successful.
    fn create_table(&self, db: &Connection) -> Result<bool>;

    /// Connects to an existing root table in the database and returns an instance of the implementing type.
    ///
    /// # Parameters
    /// - `db`: A reference to the database connection.
    /// - `name`: The base name of the root table to connect to.
    ///
    /// # Returns
    /// - `Result<Self>`: An instance of the implementing type representing the connected root table.
    fn connect(db: &Connection, name: &str) -> Result<Self>
    where
        Self: Sized;

    /// Inserts data into the root table. This should only be called when first creating the table.
    /// Only one row should be present in this table.
    ///
    /// # Parameters
    /// - `db`: A reference to the database connection.
    ///
    /// # Returns
    /// - `Result<bool>`: Returns `Ok(true)` if the data insertion was successful.
    fn insert(&self, db: &Connection) -> Result<bool>;

    /// Creates a new instance of the implementing type representing a root table with specified properties.
    ///
    /// # Parameters
    /// - `name`: The base name of the root table.
    /// - `partition_column`: The name of the column used for partitioning the table.
    /// - `interval`: The interval value used for partitioning.
    ///
    /// # Returns
    /// - `Self`: An instance of the implementing type.
    fn create(name: &str, partition_column: String, interval: i64) -> Self
    where
        Self: Sized;

    /// Retrieves the interval value used for partitioning the table.
    ///
    /// # Returns
    /// - `i64`: The interval value.
    fn get_interval(&self) -> i64;
    fn drop_table_query(&self) -> String;
}

/// Represents a root table with a name, partition column, and interval for partitioning.
#[derive(Debug)]
pub struct RootTable {
    name: String,
    pub partition_column: String,
    interval: i64,
}

impl Root for RootTable {
    /// Creates a new `RootTable` instance with specified name, partition column, and interval.
    fn create(name: &str, partition_column: String, interval: i64) -> Self {
        RootTable {
            interval,
            name: format!("{}_{}", name, ROOT_TABLE_POSTFIX),
            partition_column,
        }
    }

    /// Generates a SQL query for creating the root table.
    fn create_table_query(&self) -> String {
        format!(
            "CREATE TABLE {}(partition_column varchar, interval int);",
            self.name
        )
    }
    fn drop_table_query(&self) -> String {
        format!("DROP TABLE {}", self.name)
    }

    /// Creates the root table in the database.
    fn create_table(&self, db: &Connection) -> Result<bool> {
        let sql = self.create_table_query();
        Connection::execute(db, &sql, ())?;
        self.insert(db)?;
        Ok(true)
    }

    /// Connects to an existing root table based on its name.
    fn connect(db: &Connection, name: &str) -> Result<Self> {
        let table_name = format!("{}_{}", name, ROOT_TABLE_POSTFIX);
        let sql = format!("SELECT * FROM {}", table_name);
        let mut partition_column: String = Default::default();
        let mut partition_interval: i64 = -1;
        Connection::query_row(db, &sql, (), |row| {
            partition_column = row[0].get_str().to_owned()?.to_string();
            partition_interval = row[1].get_i64();
            Ok(())
        })?;

        Ok(RootTable {
            name: table_name,
            partition_column,
            interval: partition_interval,
        })
    }

    /// Inserts initial data into the root table.

    fn insert(&self, db: &Connection) -> Result<bool> {
        let sql = format!(
            "INSERT INTO {} (partition_column, interval) VALUES (?, ?);",
            self.name
        );
        Connection::execute(db, &sql, |stmt: &mut Statement| {
            self.partition_column.bind_param(stmt, 1)?;
            self.interval.bind_param(stmt, 2)?;
            Ok(())
        })?;

        Ok(true)
    }

    /// Retrieves the interval for table partitioning
    fn get_interval(&self) -> i64 {
        self.interval
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use rusqlite::Connection as RusqConn;
    use sqlite3_ext::Connection;
    fn mock_root(name: &str) -> RootTable {
        Root::create(name, "col".to_string(), 3600)
    }

    #[test]
    fn test_db_create() {
        let name = "test";
        let rusq_conn = match RusqConn::open_in_memory() {
            Ok(conn) => conn,
            Err(err) => panic!("{}", err.to_string()),
        };
        let root_table = mock_root(name);
        let result = match root_table.create_table(Connection::from_rusqlite(&rusq_conn)) {
            Ok(r) => r,
            Err(err) => panic!("{}", err.to_string()),
        };
        assert!(result);
    }
    // Additional tests for `create_table`, `connect`, and `insert` could be added here
}
