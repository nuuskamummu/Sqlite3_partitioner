use sqlite3_ext::query::{QueryResult, Statement, ToParam};
use sqlite3_ext::{Connection, FallibleIteratorMut};

use crate::constraints::Conditions;
use crate::ConstraintOpDef;
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

type PartitionName<'query> = &'query str;
type PartitionConditions<'query> = Option<&'query Conditions<'query>>;
type PartitionArgs<'vtab, 'query> = (
    &'vtab Connection,
    PartitionName<'query>,
    PartitionConditions<'query>,
);
impl<'vtab, 'query> TryFrom<PartitionArgs<'vtab, 'query>> for Partition {
    type Error = sqlite3_ext::Error;
    fn try_from(value: PartitionArgs) -> Result<Self, Self::Error> {
        let (db, partition_name, conditions) = value;
        let where_clause = if let Some(conditions) = conditions {
            let condition_str = conditions
                .as_slice()
                .iter()
                .map(|condition| {
                    format!(
                        "{} {} {}",
                        condition.column,
                        ConstraintOpDef::from(*condition.operator),
                        "?"
                    )
                })
                .collect::<Vec<String>>()
                .join(" AND ");

            format!("WHERE {}", condition_str)
        } else {
            String::new()
        };

        let sql = format!(
            "SELECT rowid as row_id, * FROM {} {}",
            partition_name, where_clause
        );
        let mut stmt = db.prepare(&sql)?;
        conditions.map(|conditions| {
            conditions
                .as_slice()
                .iter()
                .enumerate()
                .try_for_each(|(index, condition)| {
                    condition.value.bind_param(&mut stmt, (index + 1) as i32)
                })
        });

        Ok(Partition::from((partition_name.to_string(), stmt)))
    }
}
