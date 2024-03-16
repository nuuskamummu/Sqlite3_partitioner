use std::ops::IndexMut;

use sqlite3_ext::ffi::SQLITE_ERROR;
use sqlite3_ext::ffi::SQLITE_FORMAT;
use sqlite3_ext::Connection;
use sqlite3_ext::Error as ExtError;
use sqlite3_ext::FromValue;
use sqlite3_ext::Result as ExtResult;
use sqlparser::ast::Ident;
use sqlparser::ast::ObjectName;
use sqlparser::ast::Statement as ParsedStatement;
use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::Parser;

use crate::error::TableError;
use crate::ColumnDeclaration;
use crate::ColumnDeclarations;

/// Provides a contract for table-related operations, including access to schema,
/// name, and column declarations. It serves as the foundation for defining
/// tables in the database, focusing on schema representation and naming conventions.
pub trait Table {
    /// Specifies a static postfix string to be appended to base table names,
    /// facilitating unique naming across different table types or partitions.
    const POSTFIX: &'static str;

    /// Retrieves the schema declaration associated with the table, including its name
    /// and column definitions, outlining the table's structure in the database.
    fn schema(&self) -> &SchemaDeclaration;

    /// Default implementation to get the table name from its schema.
    /// It utilizes the schema's name as the table's identifier in the database.
    fn name(&self) -> &str {
        self.schema().name()
    }

    /// Default implementation to access the column declarations from the table's schema,
    /// providing a structured representation of the table's fields and types.
    fn columns(&self) -> &ColumnDeclarations {
        self.schema().columns()
    }

    /// Computes the full name of the table by incorporating the base name with a predefined postfix,
    /// ensuring consistency in naming patterns for similar table types.
    fn format_name(base_name: &str) -> String {
        format!("{base_name}_{}", Self::POSTFIX)
    }
}

/// Extends the `Table` trait with functionality to adjust index creation statements
/// for copying or replicating tables. This includes copying any index to reflect.
pub trait Copy: Table {
    /// Generates an adjusted SQL statement for creating an index on a new table,
    /// modifying the original index statement to target the new table.
    fn adjust_index_creation_statement(statement: &ParsedStatement, new_table: &str) -> String {
        match statement.to_owned() {
            ParsedStatement::CreateIndex {
                name,
                table_name: _,
                using,
                columns,
                unique,
                concurrently,
                if_not_exists,
                include,
                nulls_distinct,
                predicate,
            } => ParsedStatement::CreateIndex {
                name: Some(ObjectName(vec![Ident::new(format!(
                    "{}_{}",
                    name.unwrap(),
                    new_table
                ))])),
                table_name: ObjectName(vec![Ident::new(new_table)]),
                using,
                columns,
                unique,
                concurrently,
                if_not_exists,
                include,
                nulls_distinct,
                predicate,
            }
            .to_string(),
            _ => unreachable!(),
        }
    }
}

/// Defines behaviors for creating a new table in the database, including schema
/// generation and persistence. This trait is crucial for initializing tables with
/// the correct structure and ensuring they are properly registered in the database.
pub trait Create: Table {
    /// Generates and persists a table's schema based on provided name and column
    /// declarations, effectively creating the table in the database.
    fn schema(
        db: &Connection,
        name: String,
        column_declarations: ColumnDeclarations,
    ) -> ExtResult<SchemaDeclaration>
    where
        Self: Sized,
    {
        let schema = SchemaDeclaration::new(name, column_declarations);
        Self::persist(&schema, db)?;
        Ok(schema)
    }

    /// Persists the table's schema in the database by executing the necessary SQL
    /// statements, facilitating table creation and registration.
    fn persist(schema: &SchemaDeclaration, db: &Connection) -> ExtResult<()> {
        let sql = &match <Self as Create>::table_query(schema) {
            Ok(sql) => Ok(sql),
            Err(err) => Err(sqlite3_ext::Error::Module(err.to_string())),
        }?;

        db.execute(sql, ())?;
        Ok(())
    }

    /// Constructs the SQL query string for creating the table, based on its schema,
    /// preparing the command for execution in the database environment.
    fn table_query(schema: &SchemaDeclaration) -> Result<String, String> {
        Ok(schema.table_query())
    }
}

/// Enables establishing a connection to an existing table in the database,
/// retrieving and interpreting the table's schema. This trait is essential for
/// interacting with and manipulating existing tables.
pub trait Connect: Table {
    /// Retrieves the schema for an existing table from the database, parsing and
    /// constructing a `SchemaDeclaration` to represent the table's structure accurately.
    fn schema(db: &Connection, name: &str) -> ExtResult<SchemaDeclaration>
    where
        Self: Sized,
    {
        let dialect = SQLiteDialect {};
        let parser = Parser::new(&dialect);
        let schema_sql = format!(
            "SELECT sql FROM sqlite_schema WHERE NAME = '{}'",
            name // Self::Table::format_name(name)
        );
        let mut schema = db.query_row(&schema_sql, (), |result| {
            let sql = &sqlite3_ext::query::QueryResult::index_mut(result, 0).get_str()?;
            parser
                .try_with_sql(sql)
                .map_err(|e| ExtError::Sqlite(SQLITE_FORMAT, Some(e.to_string())))
        })?;
        let (name, columns) = match schema.parse_statement() {
            Ok(ParsedStatement::CreateTable { name, columns, .. }) => (name, columns),
            _ => {
                return Err(ExtError::Sqlite(
                    SQLITE_ERROR,
                    Some("Unexpected statement type".into()),
                ))
            }
        };
        let column_declarations: Result<Vec<ColumnDeclaration>, TableError> =
            columns.iter().try_fold(Vec::default(), |mut acc, column| {
                let column_name = column.name.to_string();
                let data_type = column.data_type.to_string();
                let column_declaration =
                    ColumnDeclaration::try_from(format!("{column_name} {data_type}").as_str())?;

                acc.push(column_declaration);
                Ok(acc)
            });
        let column_declarations = match column_declarations {
            Ok(value) => Ok(value),
            Err(err) => Err(sqlite3_ext::Error::Module(err.to_string())),
        }?;
        let name = name.to_string();
        Ok(SchemaDeclaration::new(
            name,
            ColumnDeclarations(column_declarations),
        ))
    }
}

/// Facilitates the removal of tables from the database. This trait defines the
/// necessary operations to safely drop a table, ensuring that the associated
/// data and schema are correctly removed from the database system.
pub trait Drop: Table {
    /// Executes the SQL command to drop the table from the database, effectively
    /// removing its schema and data. This operation is irreversible and should
    /// be performed with caution.
    fn drop_table(&self, db: &Connection) -> ExtResult<()> {
        let sql = self.drop_table_query();
        db.execute(&sql, ())?;

        Ok(())
    }
    /// Constructs the SQL query string for dropping the table, specifying the
    /// command for execution in the database environment.
    fn drop_table_query(&self) -> String {
        format!("DROP TABLE {}", &self.name())
    }
}

/// Represents the schema declaration of a database table, including its name
/// and the definitions of its columns. This struct is used for creating,
/// querying, and manipulating table schemas within the database.
#[derive(Debug, Clone)]
pub struct SchemaDeclaration {
    /// The name of the database table.
    pub name: String,
    /// The column declarations of the table, detailing each column's name and data type.
    pub columns: ColumnDeclarations,
}
impl SchemaDeclaration {
    /// Retrieves the name of the table.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Accesses the column declarations of the table.
    ///
    /// Returns a reference to the `ColumnDeclarations` representing the schema of the
    /// table's columns, including names and data types.
    pub fn columns(&self) -> &ColumnDeclarations {
        &self.columns
    }

    /// Constructs a new `SchemaDeclaration` instance.
    ///
    /// This method initializes a `SchemaDeclaration` with the provided table name and
    /// column declarations, effectively representing the schema of a new or existing
    /// database table.
    ///
    /// Parameters:
    /// - `name`: The name of the table.
    /// - `columns`: The column declarations of the table.
    ///
    /// Returns a new instance of `SchemaDeclaration`.
    pub fn new(name: String, columns: ColumnDeclarations) -> Self {
        Self { name, columns }
    }

    /// Generates the SQL CREATE TABLE query for the table.
    ///
    /// This method constructs a SQL string that can be executed to create the database
    /// table represented by this schema declaration. The query includes the table name
    /// and a formatted list of column declarations.
    ///
    /// Returns the SQL CREATE TABLE query as a `String`.
    pub fn table_query(&self) -> String {
        let table_name = self.name();
        let columns: String = self.columns().to_string();
        format!("CREATE TABLE {table_name} ({columns})")
    }
}
