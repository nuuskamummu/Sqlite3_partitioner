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

use std::collections::HashMap;

use crate::ResultRow;

pub struct Bucket {
    rows: ResultRow,
}

type Buckets = HashMap<String, Bucket>;
