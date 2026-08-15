use sqlite3_ext::query::{QueryResult, Statement, ToParam};
use sqlite3_ext::{Connection, FallibleIteratorMut};

use crate::constraints::{Conditions, SortDirection};
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

/// Inputs for building a partition scan query: which partition, which WHERE conditions,
/// and optionally an ORDER BY on the partition column. Ordering is only requested when
/// `best_index` has told SQLite the scan order satisfies the query's ORDER BY; correctness
/// relies on partitions being disjoint, sorted ranges of the partition column.
pub struct PartitionQuery<'vtab, 'query> {
    pub db: &'vtab Connection,
    pub partition_name: &'query str,
    pub conditions: Option<&'query Conditions<'query>>,
    pub order: Option<(&'query str, SortDirection)>,
}

impl<'vtab, 'query> TryFrom<PartitionQuery<'vtab, 'query>> for Partition {
    type Error = sqlite3_ext::Error;

    /// Attempts to construct a `Partition` instance from a `PartitionQuery`.
    ///
    /// The conditions are converted into a WHERE clause for the SQL query, and an optional
    /// ORDER BY on the partition column is appended. This method prepares the SQL statement
    /// and binds any condition values as parameters.
    ///
    /// Returns a `Partition` instance on success, or an error if the SQL statement preparation
    /// or parameter binding fails.
    fn try_from(query: PartitionQuery) -> Result<Self, Self::Error> {
        let PartitionQuery {
            db,
            partition_name,
            conditions,
            order,
        } = query;
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

        let order_clause = order
            .map(|(column, direction)| format!("ORDER BY {} {}", column, direction))
            .unwrap_or_default();

        let sql = format!(
            "SELECT rowid as row_id, * FROM {} {} {}",
            partition_name, where_clause, order_clause
        );
        let mut stmt = db.prepare(&sql)?;
        if let Some(conditions) = conditions {
            conditions
                .as_slice()
                .iter()
                .enumerate()
                .try_for_each(|(index, condition)| {
                    condition.value.bind_param(&mut stmt, (index + 1) as i32)
                })?;
        }

        Ok(Partition::from((partition_name.to_string(), stmt)))
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Index;

    use rusqlite::Connection as RusqConn;
    use sqlite3_ext::Connection;

    use crate::constraints::{Condition, Conditions};

    use super::Partition;

    #[test]
    fn test_partition_try_from_propagates_bind_errors() -> sqlite3_ext::Result<()> {
        let rusq_conn = RusqConn::open_in_memory().unwrap();
        let db = Connection::from_rusqlite(&rusq_conn);
        db.execute("CREATE TABLE test_partition (col1 TEXT)", ())?;
        db.execute("INSERT INTO test_partition VALUES ('2024-01-01 12:15')", ())?;

        db.query_row("SELECT col1 FROM test_partition LIMIT 1", (), |row| {
            let value_ref = row.index(0).as_ref();
            let op = sqlite3_ext::vtab::ConstraintOp::Function(0);
            let condition = Condition {
                column: "col1",
                operator: &op,
                value: value_ref,
            };
            let conditions = Conditions::from_iter([condition]);

            let result = Partition::try_from(super::PartitionQuery {
                db,
                partition_name: "test_partition",
                conditions: Some(&conditions),
                order: None,
            });
            assert!(result.is_err());
            Ok(())
        })?;

        Ok(())
    }
}
