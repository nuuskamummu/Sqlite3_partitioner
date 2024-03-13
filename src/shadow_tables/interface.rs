use sqlite3_ext::query::ToParam;
use sqlite3_ext::Connection;
use sqlite3_ext::ValueRef;

use crate::ColumnDeclarations;
use crate::LookupTable;
use crate::RootTable;
use crate::TemplateTable;

use super::operations::Drop;
use super::operations::Table;

#[derive(Debug)]
pub struct VirtualTable<'vtab> {
    pub connection: &'vtab Connection,
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
    /// If partition does not already exists, this method will copy the template table, and
    /// updating lookup table before returning the name for the (newly created) partition
    pub fn get_partition(&self, partition_value: &i64) -> sqlite3_ext::Result<String> {
        self.lookup_table
            .get_partition(partition_value)
            .and_then(|name| match name {
                None => {
                    let new_partition_name = self.copy(&partition_value.to_string())?;
                    self.lookup_table.insert(
                        self.connection,
                        &new_partition_name,
                        *partition_value,
                    )?;
                    Ok(new_partition_name)
                }
                Some(name) => Ok(name.to_owned()),
            })
    }
    fn copy(&self, suffix: &str) -> sqlite3_ext::Result<String> {
        let new_table_name = self.format_new_table_name(suffix);
        self.template_table.copy(&new_table_name, self.connection)?;
        Ok(new_table_name)
    }
    fn format_new_table_name(&self, suffix: &str) -> String {
        format!("{}_{}", self.base_name, suffix)
    }

    // fn prepare_copy_template<'a>(
    //     &'a self,
    //     new_table_name: &'a str,
    //     db: &'a Connection,
    // ) -> impl Fn() -> sqlite3_ext::Result<&'a str> + 'a {
    //     let sql = self.copy_query(new_table_name);
    //     move || {
    //         let result = db.execute(&sql, ());
    //         match result {
    //             Ok(_) => Ok(new_table_name),
    //             Err(err) => Err(err),
    //         }
    //     }
    // }
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

    pub fn insert(&self, partition_value: i64, columns: &[&ValueRef]) -> sqlite3_ext::Result<i64> {
        let partition = self.get_partition(&partition_value)?;
        let placeholders = std::iter::repeat("?")
            .take(columns.len())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!("INSERT INTO {} VALUES({})", partition, placeholders);
        let mut stmt = self.connection.prepare(&sql)?;
        for (index, &column) in columns.iter().enumerate() {
            column.bind_param(&mut stmt, (index + 1) as i32)?
        }
        stmt.insert(())
    }
}

// #[test]
// fn test_db_copy() {
//     let conn = match Connection::open_in_memory() {
//         Ok(conn) => conn,
//         Err(err) => panic!("{}", err.to_string()),
//     };
//     let conn = Connection::from_rusqlite(&conn);
//     let (name, columns) = mock_template();
//     let table = TemplateTable::create(conn, &name, columns).unwrap();
//
//     let copy_result = table.copy("10000", conn).unwrap();
//
//     assert_eq!(copy_result, "test_10000");
// }
