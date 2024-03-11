use sqlite3_ext::query::ToParam;
use sqlite3_ext::Connection;
use sqlite3_ext::ValueRef;

use crate::ColumnDeclaration;
use crate::ColumnDeclarations;
use crate::LookupTable;
use crate::RootTable;
use crate::TemplateTable;

use super::operations::Copy;
use super::operations::Drop;
use super::operations::Table;

#[derive(Debug)]
pub struct VirtualTable<'vtab> {
    connection: &'vtab Connection,
    base_name: String,
    template_table: TemplateTable,
    root_table: RootTable,
    lookup_table: LookupTable<i64>,
}
/// Defines behavior for managing template tables, including creation, copying, and schema definition.
///
/// This trait encapsulates methods required to create template tables, generate creation queries,
/// copy existing templates, and retrieve table schema information.
impl<'vtab> VirtualTable<'vtab> {
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

    pub fn create(
        db: &'vtab Connection,
        name: &str,
        column_declarations: ColumnDeclarations,
        partition_column: String,
        interval: i64,
    ) -> sqlite3_ext::Result<Self> {
        Ok(VirtualTable {
            connection: db,
            base_name: name.to_string(),
            lookup_table: LookupTable::create(db, name)?,
            root_table: RootTable::create(db, name, partition_column, interval)?,
            template_table: TemplateTable::create(db, name, column_declarations)?,
        })
    }
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
    pub fn get_partition(&self, partition_value: &i64) -> sqlite3_ext::Result<String> {
        self.lookup_table
            .get_partition(self.connection, partition_value)
            .and_then(|(name, should_create)| {
                if should_create {
                    self.template_table
                        .copy(&partition_value.to_string(), self.connection)
                } else {
                    Ok(name)
                }
            })
    }
    /// Create table query from the template table.
    pub fn create_table_query(&self) -> String {
        self.template_table.schema().table_query().clone()
    }
    pub fn columns(&self) -> &ColumnDeclarations {
        self.template_table.columns()
    }
    pub fn partition_column_name(&self) -> &str {
        self.root_table.partition_column()
    }
    pub fn partition_interval(&self) -> i64 {
        self.root_table.get_interval()
    }
    pub fn lookup(&self) -> &LookupTable<i64> {
        &self.lookup_table
    }

    pub fn insert(&self, partition_name: &str, columns: &[&ValueRef]) -> sqlite3_ext::Result<i64> {
        let placeholders = std::iter::repeat("?")
            .take(columns.len())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!("INSERT INTO {} VALUES({})", partition_name, placeholders);
        let mut stmt = self.connection.prepare(&sql)?;
        for (index, &column) in columns.iter().enumerate() {
            column.bind_param(&mut stmt, (index + 1) as i32)?
        }
        stmt.insert(())
    }
}
