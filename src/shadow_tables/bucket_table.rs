// pub trait Bucket {
//     fn create_table_query(&self) -> String;
//     fn create(name: &str, columns: Vec<ColumnDeclaration>) -> Self
//     where
//         Self: Sized;
//     fn copy_template_query(&self, suffix: &str) -> String;
//     fn create_table(&self, db: &Connection) -> Result<bool>;
//     fn copy_template(&self, suffix: &str, db: &Connection) -> Result<String>;
//     fn get_base_name(&self) -> Option<&str>;
//     fn get_column_declarations(&self) -> String;
// }

use crate::ColumnDeclaration;

#[derive(Debug)]
pub struct Bucket {
    name: String,
    partition_value: i64,
    pub columns: Vec<ColumnDeclaration>,
}
impl Bucket {
    pub fn new(name: String, columns: Vec<ColumnDeclaration>, partition_value: i64) -> Self {
        Self {
            name,
            columns,
            partition_value,
        }
    }
    pub fn get_name(&self) -> &str {
        &self.name
    }
    pub fn get_partition_value(&self) -> i64 {
        self.partition_value
    }
}
