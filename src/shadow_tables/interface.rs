use sqlite3_ext::query::ToParam;
use sqlite3_ext::Connection;
use sqlite3_ext::ValueRef;

use crate::expiration::LifetimeColumn;
use crate::ColumnDeclaration;
use crate::ColumnDeclarations;
use crate::LookupTable;
use crate::RootTable;
use crate::TemplateTable;

use super::operations::Drop;
use super::operations::Table;

/// Represents a virtual table with partitioning capabilities in SQLite.
///
/// Encapsulates the operations required for managing and interacting with a virtual table,
/// including connecting to existing tables, creating new tables with specific partitioning
/// settings, and performing data manipulation operations like insertions and deletions.
#[derive(Debug)]
pub struct VirtualTable<'vtab> {
    /// Reference to the SQLite database connection.
    pub connection: &'vtab Connection,
    /// Base name of the virtual table.
    base_name: String,
    /// Associated template table for creating new partitions.
    template_table: TemplateTable,
    /// Root table containing metadata about partitions.
    root_table: RootTable,
    /// Lookup table managing the mapping between partition values and partition names.
    lookup_table: LookupTable<i64>,
}

impl<'vtab> VirtualTable<'vtab> {
    /// Connects to an existing virtual table within the database.
    ///
    /// This function initializes a `VirtualTable` instance by connecting to the existing components
    /// of a virtual table, including the root, template, and lookup tables, based on the provided name.
    /// It enables subsequent operations on the virtual table through the returned `VirtualTable` instance.
    ///
    /// # Parameters
    /// - `db`: A reference to the active database connection.
    /// - `name`: The name of the virtual table to connect to.
    ///
    /// # Returns
    /// Returns a `VirtualTable` instance if the connection is successful, encapsulating the virtual
    /// table's operational context. On failure, returns an error indicating the issue encountered
    /// during the connection process.
    pub fn connect(
        db: &'vtab Connection,
        name: &str,
    ) -> Result<VirtualTable<'vtab>, sqlite3_ext::Error> {
        let table = VirtualTable {
            connection: db,
            base_name: name.to_string(),
            root_table: RootTable::connect(db, name)?,
            template_table: TemplateTable::connect(db, name)?,
            lookup_table: LookupTable::connect(db, name)?,
        };
        Ok(table)
    }

    /// Creates a new instance of a virtual table with specified configurations.
    ///
    /// Initializes and configures a new virtual table in the database, setting up associated structures
    /// like the lookup table for partition mapping, the root table for metadata, and a template table
    /// for defining the structure of partitions. This method facilitates setting up a partitioned virtual
    /// table environment with custom column definitions and partitioning strategy.
    ///
    /// # Parameters
    /// - `db`: A reference to the active database connection.
    /// - `name`: The base name for the virtual table and its associated structures.
    /// - `column_declarations`: Specifications of columns for the virtual table.
    /// - `partition_column`: The name of the column used to determine partitioning.
    /// - `interval`: The interval used for partitioning data.
    ///
    /// # Returns
    /// On success, returns an instance of `VirtualTable`. If any part of the setup fails, an error is returned.
    pub fn create(
        db: &'vtab Connection,
        name: &str,
        column_declarations: ColumnDeclarations,
        partition_column: String,
        interval: i64,
        lifetime_column: Option<i64>,
    ) -> sqlite3_ext::Result<Self> {
        Ok(VirtualTable {
            connection: db,
            base_name: name.to_string(),
            lookup_table: LookupTable::create(db, name)?,
            root_table: RootTable::create(db, name, partition_column, interval, lifetime_column)?,
            template_table: TemplateTable::create(db, name, column_declarations)?,
        })
    }
    /// Destroys the virtual table and all its associated data structures.
    ///
    /// This method deletes all partitions managed by the virtual table, as well as the lookup, root,
    /// and template tables. It ensures a clean removal of all database artifacts related to the virtual table.
    ///
    /// # Returns
    /// On successful execution, returns `Ok(())`. If an error occurs during the deletion of any component,
    /// an error is returned detailing the issue.
    pub fn destroy(&self) -> sqlite3_ext::Result<()> {
        for partition in self.lookup_table.get_partitions_by_range(
            self.connection,
            &std::ops::Bound::Unbounded,
            &std::ops::Bound::Unbounded,
        )? {
            self.connection
                .execute(&format!("DROP TABLE {}", partition.1), ())?;
        }
        self.lookup_table.drop_table(self.connection)?;
        self.root_table.drop_table(self.connection)?;
        self.template_table.drop_table(self.connection)?;
        Ok(())
    }
    /// Retrieves the name of an existing partition or creates a new partition for the given value.
    ///
    /// This method looks up the partition associated with the provided `partition_value`. If a
    /// partition does not exist, it creates a new partition by copying the template table structure,
    /// updates the lookup table with this new partition's information, and returns the new partition's name.
    ///
    /// # Parameters
    /// * `partition_value` - The value determining which partition to retrieve or create.
    ///
    /// # Returns
    /// The name of the existing or newly created partition as a result. In case of errors during
    /// lookup, creation, or insertion into the lookup table, an appropriate error is returned.
    pub fn get_partition(&self, partition_value: &i64) -> sqlite3_ext::Result<String> {
        self.lookup_table
            .get_partition(partition_value)
            .and_then(|name| match name {
                None => {
                    let new_partition_name = self.copy(&partition_value.to_string())?;
                    let lifetime = self.root_table.get_lifetime();
                    let expires_at = match lifetime {
                        Some(lifetime) => Some(lifetime + *partition_value),
                        None => None,
                    };
                    self.lookup_table.insert(
                        self.connection,
                        &new_partition_name,
                        *partition_value,
                        expires_at,
                    )?;
                    Ok(new_partition_name)
                }
                Some(name) => Ok(name.to_owned()),
            })
    }

    /// Copies the template table structure to create a new partition table with a specified suffix.
    ///
    /// # Parameters
    /// * `suffix` - The suffix to append to the base name for the new partition table.
    ///
    /// # Returns
    /// The name of the newly created partition table.
    fn copy(&self, suffix: &str) -> sqlite3_ext::Result<String> {
        let new_table_name = self.format_new_table_name(suffix);
        self.template_table.copy(&new_table_name, self.connection)?;
        Ok(new_table_name)
    }

    /// Generates a new table name by appending a suffix to the virtual table's base name.
    ///
    /// # Parameters
    /// * `suffix` - The suffix to be appended.
    ///
    /// # Returns
    /// The formatted new table name.
    fn format_new_table_name(&self, suffix: &str) -> String {
        format!("{}_{}", self.base_name, suffix)
    }

    /// Retrieves the SQL query to create a table based on the template table's schema.
    ///
    /// # Returns
    /// The SQL CREATE TABLE query string.
    pub fn create_table_query(&self) -> String {
        let mut interface_schema = self.template_table.schema().clone();
        // let mut hidden_column =
        //     ColumnDeclaration::new("_partition".to_string(), sqlite3_ext::ValueType::Text);
        // hidden_column.set_hidden();
        interface_schema.name = self.base_name.clone();
        // interface_schema.columns.0.push(hidden_column);
        interface_schema.table_query()
    }

    /// Accesses the column declarations of the template table.
    ///
    /// # Returns
    /// A reference to the `ColumnDeclarations` of the template table.
    pub fn columns(&self) -> &ColumnDeclarations {
        self.template_table.columns()
    }

    /// Retrieves the name of the partition column from the root table.
    ///
    /// # Returns
    /// The name of the partition column.
    pub fn partition_column_name(&self) -> &str {
        self.root_table.partition_column()
    }

    /// Retrieves the partition interval set in the root table.
    ///
    /// # Returns
    /// The partition interval in seconds.
    pub fn partition_interval(&self) -> i64 {
        self.root_table.get_interval()
    }

    pub fn lifetime(&self) -> Option<i64> {
        self.root_table.get_lifetime()
    }

    /// Provides a reference to the lookup table associated with the virtual table.
    ///
    /// # Returns
    /// A reference to the `LookupTable`.
    pub fn lookup(&self) -> &LookupTable<i64> {
        &self.lookup_table
    }

    /// Inserts a new row into the appropriate partition based on the specified partition value.
    ///
    /// # Parameters
    /// * `partition_value` - The value determining which partition the new row belongs to.
    /// * `columns` - An array of references to `ValueRef`, representing the values to be inserted.
    ///
    /// # Returns
    /// The ROWID of the inserted row.
    pub fn insert(&self, partition_value: i64, columns: &[&ValueRef]) -> sqlite3_ext::Result<i64> {
        let partition = self.get_partition(&partition_value)?;
        let placeholders = std::iter::repeat("?")
            .take(columns.len())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!("INSERT INTO {} VALUES({})", partition, placeholders);
        let mut stmt = self.connection.prepare(&sql)?;
        for (index, column) in columns.iter().enumerate() {
            column.bind_param(&mut stmt, (index + 1) as i32)?
        }
        stmt.insert(())
    }
}

#[cfg(test)]
mod tests {

    use std::ops::{Deref, Index, IndexMut};

    use crate::{utils::parse_interval, PartitionColumn};

    use super::*;
    use rusqlite::Connection as RusqConn;
    use sqlite3_ext::Connection;
    fn mock_template() -> (String, ColumnDeclarations, PartitionColumn, i64) {
        let columns = ColumnDeclarations::from_iter(&[
            "first_column timestamp partition_column",
            "second_column int",
            "third_column varchar",
        ]);
        let partition_column = PartitionColumn::from_iter(columns.clone());
        let interval = parse_interval("1 hour").unwrap();
        ("test".to_string(), columns, partition_column, interval)
    }

    fn create_virtual_table<'test>(conn: &'test Connection) -> VirtualTable<'test> {
        let (name, columns, partition_column, interval) = mock_template();
        let partition_column_name = partition_column.column_def().as_ref().unwrap().get_name();
        let table = VirtualTable::create(
            conn,
            &name,
            columns,
            partition_column_name.to_string(),
            interval,
            None,
        );
        assert!(table.is_ok());
        let table = table.unwrap();
        table
    }
    #[test]
    fn test_create_virtual_table() {
        let conn = match RusqConn::open_in_memory() {
            Ok(conn) => conn,
            Err(err) => panic!("{}", err.to_string()),
        };
        let conn = Connection::from_rusqlite(&conn);

        let virtual_table = create_virtual_table(&conn);
        let lookup_schema = virtual_table.lookup().schema();
        let root_schema = virtual_table.root_table.schema();
        let template_schema = virtual_table.template_table.schema();
        assert_eq!(
            virtual_table.create_table_query().to_lowercase(),
            "create table test (first_column text, second_column integer, third_column text)"
        )
    }
}
