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

pub trait Table {
    // type Schema: SchemaDeclratation;
    const POSTFIX: &'static str;
    fn schema(&self) -> &SchemaDeclaration;
    fn name(&self) -> &str {
        self.schema().name()
    }
    fn columns(&self) -> &ColumnDeclarations {
        self.schema().columns()
    }
    fn format_name(base_name: &str) -> String {
        format!("{base_name}_{}", Self::POSTFIX)
    }
}

pub trait Copy: Table {
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
pub trait Create: Table {
    fn schema<'table>(
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
    fn persist(schema: &SchemaDeclaration, db: &Connection) -> ExtResult<()> {
        let sql = &match <Self as Create>::table_query(schema) {
            Ok(sql) => Ok(sql),
            Err(err) => Err(sqlite3_ext::Error::Module(err.to_string())),
        }?;

        db.execute(sql, ())?;
        Ok(())
    }
    fn table_query(schema: &SchemaDeclaration) -> Result<String, String> {
        Ok(schema.table_query())
    }
}
pub trait Connect: Table {
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

pub trait Drop: Table {
    fn drop_table(&self, db: &Connection) -> ExtResult<()> {
        let sql = self.drop_table_query();
        db.execute(&sql, ())?;

        Ok(())
    }
    fn drop_table_query(&self) -> String {
        format!("DROP TABLE {}", &self.name())
    }
}

#[derive(Debug, Clone)]
pub struct SchemaDeclaration {
    pub name: String,
    pub columns: ColumnDeclarations,
}
impl SchemaDeclaration {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn columns(&self) -> &ColumnDeclarations {
        &self.columns
    }
    pub fn new(name: String, columns: ColumnDeclarations) -> Self {
        Self { name, columns }
    }
    pub fn table_query(&self) -> String {
        let table_name = self.name();
        let columns: String = self.columns().to_string();
        format!("CREATE TABLE {table_name} ({columns})")
    }
}
