use sqlite3_ext::{Connection, Result};

use crate::ColumnDeclarations;

use super::operations::{Copy, Create, Drop, Schema, SchemaDeclaration, Table};
/// Represents a template table with a name and a list of column declarations.
#[derive(Debug)]
pub struct TemplateTable {
    schema: SchemaDeclaration,
}
impl<'vtab> Table for TemplateTable {
    const POSTFIX: &'static str = "template";
    fn schema(&self) -> &SchemaDeclaration {
        &self.schema
    }
}
impl Create for TemplateTable {}
impl Copy for TemplateTable {}
impl Drop for TemplateTable {}
impl Schema for TemplateTable {}
impl TemplateTable {
    pub fn create(
        db: &Connection,
        name: String,
        column_declarations: ColumnDeclarations,
    ) -> Result<Self> {
        let table_name = Self::format_name(&name);
        let schema = <Self as Schema>::create(db, table_name.to_string(), column_declarations)?;
        Ok(Self { schema })
    }
    pub fn connect(db: &Connection, name: String) -> Result<Self> {
        let schema = <Self as Schema>::connect(db, &name)?;
        Ok(Self { schema })
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use rusqlite::Connection as RusqConn;
    use sqlite3_ext::Connection;
    fn mock_template() -> (String, ColumnDeclarations) {
        let columns = ColumnDeclarations::from_iter([
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
        let table = TemplateTable::create(conn, name, columns);

        assert!(table.is_ok());
    }
    #[test]
    fn test_db_copy() {
        let conn = match RusqConn::open_in_memory() {
            Ok(conn) => conn,
            Err(err) => panic!("{}", err.to_string()),
        };
        let conn = Connection::from_rusqlite(&conn);
        let (name, columns) = mock_template();
        let table = TemplateTable::create(conn, name, columns).unwrap();

        let copy_result = table.copy("10000", conn).unwrap();

        assert_eq!(copy_result, "test_10000");
    }
}
