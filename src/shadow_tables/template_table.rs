use std::ops::IndexMut;

use super::operations::{Connect, Create, Drop, SchemaDeclaration, Table};
use crate::{shadow_tables::operations::Copy, ColumnDeclarations};
use sqlite3_ext::{Connection, FallibleIterator, FallibleIteratorMut, FromValue, Result};
use sqlparser::{dialect::SQLiteDialect, parser::Parser};
#[derive(Debug)]
pub struct TemplateTable {
    pub(super) schema: SchemaDeclaration,
}
impl Table for TemplateTable {
    const POSTFIX: &'static str = "template";
    fn schema(&self) -> &SchemaDeclaration {
        &self.schema
    }
}
impl Copy for TemplateTable {}
impl Create for TemplateTable {}
impl Drop for TemplateTable {}
impl Connect for TemplateTable {}
impl TemplateTable {
    pub fn create(
        db: &Connection,
        name: &str,
        column_declarations: ColumnDeclarations,
    ) -> Result<Self> {
        let table_name = Self::format_name(name);
        let schema = <Self as Create>::schema(db, table_name, column_declarations)?;

        Ok(Self { schema })
    }
    pub fn connect(db: &Connection, name: &str) -> Result<Self> {
        let table_name = Self::format_name(name);
        let schema = <Self as Connect>::schema(db, &table_name)?;
        Ok(Self { schema })
    }
    fn copy_query(&self, new_table_name: &str) -> String {
        format!(
            "CREATE TABLE IF NOT EXISTS {} AS SELECT * FROM {}",
            new_table_name,
            self.name()
        )
    }
    pub fn copy<'a>(
        &self,
        new_table_name: &'a str,
        db: &Connection,
    ) -> sqlite3_ext::Result<&'a str> {
        let sql = self.copy_query(new_table_name);
        Connection::execute(db, &sql, ())?;
        Ok(new_table_name)
    }
    // This function abstracts the logic for adjusting the index creation SQL.

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
