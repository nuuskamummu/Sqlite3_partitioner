use std::ops::IndexMut;

use super::operations::{Connect, Create, Drop, SchemaDeclaration, Table};
use crate::{shadow_tables::operations::Copy, ColumnDeclarations};
use sqlite3_ext::{Connection, FallibleIterator, FallibleIteratorMut, FromValue, Result};
use sqlparser::{dialect::SQLiteDialect, parser::Parser};

/// Represents a template table in a database schema, used as a blueprint for creating
/// new tables with a similar structure, often in the context of data partitioning or replication.
///
/// The `TemplateTable` is designed to facilitate operations like copying itself to create
/// new tables with identical schemas but different data, enabling efficient data management
/// and scalability within the database.
#[derive(Debug)]
pub struct TemplateTable {
    /// The schema declaration of the template table, describing the structure of the partitions.
    pub(super) schema: SchemaDeclaration,
}
impl Table for TemplateTable {
    /// Specifies a postfix for distinguishing the template table.
    const POSTFIX: &'static str = "template";

    /// Accesses the schema declaration of the template table.
    fn schema(&self) -> &SchemaDeclaration {
        &self.schema
    }
}
impl Copy for TemplateTable {}
impl Create for TemplateTable {}
impl Drop for TemplateTable {}
impl Connect for TemplateTable {}
impl TemplateTable {
    /// Creates a new template table in the database based on the provided name and column declarations.
    ///
    /// This method initializes the table's schema and persists it in the database, effectively creating
    /// the template table for future use in data partitioning or table replication.
    ///
    /// Parameters:
    /// - `db`: Database connection for executing the creation.
    /// - `name`: Base name for the template table, used to derive the full table name.
    /// - `column_declarations`: Column declarations specifying the structure of the table.
    ///
    /// Returns a newly created `TemplateTable` instance.
    pub fn create(
        db: &Connection,
        name: &str,
        column_declarations: ColumnDeclarations,
    ) -> Result<Self> {
        let table_name = Self::format_name(name);
        let schema = <Self as Create>::schema(db, table_name, column_declarations)?;

        Ok(Self { schema })
    }

    /// Connects to an existing template table in the database, retrieving its schema and configuration.
    ///
    /// Parameters:
    /// - `db`: Database connection for querying the table.
    /// - `name`: Base name of the template table to connect to.
    ///
    /// Returns the connected `TemplateTable` instance.
    pub fn connect(db: &Connection, name: &str) -> Result<Self> {
        let table_name = Self::format_name(name);
        let schema = <Self as Connect>::schema(db, &table_name)?;
        Ok(Self { schema })
    }

    /// Generates an SQL query for copying the template table's structure to a new table.
    ///
    /// Parameters:
    /// - `new_table_name`: The name of the new table to create from the template.
    ///
    /// Returns the SQL CREATE TABLE query string.
    fn copy_query(&self, new_table_name: &str) -> String {
        format!(
            "CREATE TABLE IF NOT EXISTS {} AS SELECT * FROM {}",
            new_table_name,
            self.name()
        )
    }

    /// Copies the template table to create a new partition with the same structure but a different name.
    ///
    /// This operation facilitates data partitioning or replication by replicating the schema of the
    /// template table.
    ///
    /// Parameters:
    /// - `new_table_name`: The name of the new table to be created.
    /// - `db`: Database connection for executing the copy operation.
    ///
    /// Returns the name of the newly created table.
    pub fn copy<'a>(
        &self,
        new_table_name: &'a str,
        db: &Connection,
    ) -> sqlite3_ext::Result<&'a str> {
        let sql = self.copy_query(new_table_name);
        Connection::execute(db, &sql, ())?;
        Ok(new_table_name)
    }

    /// Generates and executes SQL queries for copying all indices from the template table to a new table.
    ///
    /// This method handles the duplication of index structures to maintain the same indexing on the new table,
    /// ensuring that performance and data access patterns are consistent.
    ///
    /// Parameters:
    /// - `db`: Database connection for querying existing indices and executing the copy operations.
    /// - `new_table`: The name of the new table to which indices will be copied.
    ///
    /// Returns a vector of SQL queries used to copy the indices.

    pub fn copy_indices_query(&self, db: &Connection, new_table: &str) -> Result<Vec<String>> {
        let dialect = SQLiteDialect {};
        let parser = Parser::new(&dialect);
        let schema_sql = format!(
            "SELECT sql FROM sqlite_schema WHERE tbl_name = '{}' AND type = 'index'",
            self.name()
        );
        let mut create_index_rows = db.query(&schema_sql, ())?;
        let queries = create_index_rows
            .map(|row| Ok(row.index_mut(0).get_str()?.to_owned()))
            .collect::<Vec<_>>()?
            .join(";");
        let statements = parser
            .try_with_sql(&queries)
            .map_err(|err| sqlite3_ext::Error::Module(err.to_string()))?
            .parse_statements()
            .map_err(|err| sqlite3_ext::Error::Module(err.to_string()))?;

        let index_queries = statements
            .iter()
            .map(|statement| <Self as Copy>::adjust_index_creation_statement(statement, new_table))
            .collect::<Vec<String>>();

        index_queries
            .iter()
            .try_for_each::<_, sqlite3_ext::Result<()>>(|query| {
                db.execute(query, ()).map(|_| ())
            })?;
        Ok(index_queries)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use rusqlite::Connection as RusqConn;
    use sqlite3_ext::Connection;
    fn mock_template() -> (String, ColumnDeclarations) {
        let columns = ColumnDeclarations::from_iter(&[
            "first_column int",
            "second_column int",
            "third_column varchar",
        ]);
        ("test".to_string(), columns)
    }

    #[test]
    fn test_db_create() {
        let conn = match RusqConn::open_in_memory() {
            Ok(conn) => conn,
            Err(err) => panic!("{}", err.to_string()),
        };
        let conn = Connection::from_rusqlite(&conn);

        let (name, columns) = mock_template();
        let table = TemplateTable::create(conn, &name, columns);

        assert!(table.is_ok());
    }
    #[test]
    fn test_create_index() {
        let conn = match RusqConn::open_in_memory() {
            Ok(conn) => conn,
            Err(err) => panic!("{}", err.to_string()),
        };
        let conn = Connection::from_rusqlite(&conn);

        let (name, columns) = mock_template();
        let table = TemplateTable::create(conn, &name, columns).unwrap();

        conn.execute(
            "CREATE INDEX template_test_testindex on test_template(first_column)",
            (),
        )
        .unwrap();
        conn.execute(
            "CREATE INDEX template_test_testindex2 on test_template(third_column)",
            (),
        )
        .unwrap();

        assert_eq!(table.copy("test_100", conn).unwrap(), "test_100");
        let indexes = table.copy_indices_query(conn, "test_100").unwrap();

        assert_eq!(
            indexes[0],
            "CREATE INDEX template_test_testindex_test_100 ON test_100(first_column)"
        );
        assert_eq!(
            indexes[1],
            "CREATE INDEX template_test_testindex2_test_100 ON test_100(third_column)"
        );
    }
}
