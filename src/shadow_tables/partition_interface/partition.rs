use sqlite3_ext::query::{QueryResult, Statement, ToParam};
use sqlite3_ext::{Connection, FallibleIteratorMut};

use crate::constraints::Conditions;
use crate::ConstraintOpDef;

/// Represents a database partition, encapsulating the SQL statement for querying
/// the partition and the partition's name. It provides functionality for iterating over
/// rows within the partition and accessing row data.
#[derive(Debug)]
pub struct Partition {
    /// The SQL statement used for querying rows within the partition.
    pub statement: Statement,
    /// The name of the partition, which corresponds to a specific segment of the data.
    partition_name: String,
}

impl Partition {
    /// Advances to the next row in the partition query results.
    ///
    /// Returns an option containing a mutable reference to the `QueryResult` of the next row,
    /// or `None` if there are no more rows.
    pub fn next_row(&mut self) -> sqlite3_ext::Result<Option<&mut QueryResult>> {
        self.statement.next()
    }

    /// Retrieves a mutable reference to the current row's `QueryResult`.
    ///
    /// Returns `None` if there is no current row or if the iterator has not been advanced.
    pub fn get_current_row_mut(&mut self) -> Option<&mut QueryResult> {
        self.statement.current_result_mut()
    }

    /// Retrieves an immutable reference to the current row's `QueryResult`.
    ///
    /// Returns `None` if there is no current row or if the iterator has not been advanced.
    pub fn get_current_row(&self) -> Option<&QueryResult> {
        self.statement.current_result()
    }

    /// Retrieves the name of the partition.
    ///
    /// Returns a &str representing the partition's name.
    pub fn get_name(&self) -> &str {
        &self.partition_name
    }
}
impl From<(String, Statement)> for Partition {
    /// Constructs a `Partition` instance from a tuple containing the partition's name
    /// and the SQL statement for querying the partition.
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

    /// Attempts to construct a `Partition` instance from a tuple containing a database connection,
    /// the partition's name, and optional conditions for filtering the partition's data.
    ///
    /// The conditions are converted into a WHERE clause for the SQL query. This method prepares
    /// the SQL statement and binds any condition values as parameters.
    ///
    /// Returns a `Partition` instance on success, or an error if the SQL statement preparation
    /// or parameter binding fails.
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
