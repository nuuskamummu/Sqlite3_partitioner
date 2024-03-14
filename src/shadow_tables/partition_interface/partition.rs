use sqlite3_ext::query::{QueryResult, Statement};
use sqlite3_ext::FallibleIteratorMut;
#[derive(Debug)]
pub struct Partition {
    pub statement: Statement,
    partition_name: String,
}

impl Partition {
    pub fn next_row(&mut self) -> sqlite3_ext::Result<Option<&mut QueryResult>> {
        self.statement.next()
    }
    pub fn get_current_row_mut(&mut self) -> Option<&mut QueryResult> {
        self.statement.current_result_mut()
    }
    pub fn get_current_row(&self) -> Option<&QueryResult> {
        self.statement.current_result()
    }
    pub fn get_name(&self) -> &str {
        &self.partition_name
    }
}
impl From<(String, Statement)> for Partition {
    fn from(value: (String, Statement)) -> Self {
        Self {
            statement: value.1,
            partition_name: value.0,
        }
    }
}
