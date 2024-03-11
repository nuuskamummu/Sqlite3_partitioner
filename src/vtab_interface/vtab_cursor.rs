use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::ops::{Bound, Index};
use std::usize;

use super::{PartitionMetaTable, WhereClause, WhereClauses};
use crate::shadow_tables::{Partition, PartitionColumn, PartitionRow};
use crate::utils::{aggregate_conditions_to_ranges, Condition};
use crate::ConstraintOpDef;
use sqlite3_ext::query::ToParam;
use sqlite3_ext::vtab::ColumnContext;
use sqlite3_ext::{vtab::VTabCursor, ValueRef};
use sqlite3_ext::{FallibleIteratorMut, Result as ExtResult};

#[derive(Debug)]
pub struct RangePartitionCursor<'vtab> {
    pub internal_rowid_counter: i64,
    pub meta_table: &'vtab PartitionMetaTable<'vtab>,
    pub partitions: Vec<Partition>,
    pub current_partition_index: usize, // current_partition: Option<&'vtab PartitionResult<'vtab>>,
}
impl<'vtab> RangePartitionCursor<'vtab> {
    /// Constructs a new `RangePartitionCursor` for interacting with partitioned data.
    ///
    /// # Parameters
    ///
    /// * `meta_table` - A reference to the associated `PartitionMetaTable`.
    ///
    /// # Returns
    ///
    /// A new instance of `RangePartitionCursor`.
    pub fn new(meta_table: &'vtab PartitionMetaTable) -> Self {
        println!("new curosr");
        Self {
            partitions: Vec::new(),
            meta_table,
            internal_rowid_counter: i64::default(),
            current_partition_index: usize::default(),
        }
    }

    pub fn get_mut_current_partition(&mut self) -> Option<&mut Partition> {
        self.partitions.get_mut(self.current_partition_index)
    }
    fn get_current_partition(&self) -> Option<&Partition> {
        self.partitions.get(self.current_partition_index)
    }
    fn get_current_row(&self) -> Option<&PartitionRow> {
        self.get_current_partition()
            .and_then(|partition| partition.get_current_row())
    }
    /// Advances the cursor to the next partition.
    ///
    /// # Returns
    ///
    /// An `Option<&mut PartitionResult>` which is:
    /// - `Some(&mut PartitionResult)` if the next partition exists within the current result set.
    /// - `None` if there are no more partitions in the current result set.
    fn advance_to_next_partition(&mut self) -> Option<&mut Partition> {
        self.partitions
            .get_mut(self.current_partition_index + 1)
            .map(|partition| {
                self.current_partition_index += 1;
                partition
            })
    }
    /// Advances the cursor to the next row within the current partition.
    ///
    /// # Returns
    ///
    /// An `Option<&mut ResultRow>` which is:
    /// - `Some(&mut ResultRow)` if the next row exists within the current partition.
    /// - `None` if there are no more rows in the current partition.
    fn advance_to_next_row(&mut self) -> Option<&mut PartitionRow> {
        self.get_mut_current_partition()
            .and_then(|partition| partition.advance_to_next_row())
    }

    /// Retrieves a list of partition identifiers and names that fall within the specified bounds.
    ///
    /// This function queries the partition lookup to find partitions whose values are within
    /// the specified lower and upper bounds. It's used to narrow down the partitions that
    /// need to be queried based on the conditions provided.
    ///
    /// # Parameters
    ///
    /// * `lower_bound` - The lower bound of the partition value range to query.
    /// * `upper_bound` - The upper bound of the partition value range to query.
    ///
    /// # Returns
    ///
    /// An `ExtResult<Vec<(i64, String)>>` which is:
    /// - `Ok(vec)` on success, containing a vector of tuples where each tuple contains a partition's value and name.
    /// - `Err(e)` on failure, indicating an error occurred while fetching the partition information
    fn get_partitions_to_query(
        &self,
        lower_bound: &Bound<i64>,
        upper_bound: &Bound<i64>,
    ) -> ExtResult<Vec<(i64, String)>> {
        self.meta_table.interface.lookup().get_partitions_by_range(
            self.meta_table.connection,
            lower_bound,
            upper_bound,
        )
    }
    /// Executes a SQL query against a specified partition and collects the results.
    ///
    /// This function prepares and executes a SQL query for the given partition, collecting
    /// each row's data into `ResultRow` objects. It constructs `ResultRow` instances by
    /// aggregating `ResultColumn` data for each row returned by the query.
    ///
    /// # Parameters
    ///
    /// * `partition_name` - The name of the partition to query.
    /// * `partition_where_str` - The WHERE clause string to apply to the query, filtering the results.
    /// * `args` - A mutable slice of `ValueRef`, representing bound parameters for the query.
    ///
    /// # Returns
    ///
    /// An `ExtResult<Vec<ResultRow>>` which is:
    /// - `Ok(vec)` on success, containing a vector of `ResultRow` objects representing the query results.
    /// - `Err(e)` on failure, indicating an error occurred during query execution or result processing.
    fn execute_partition_query(
        &self,
        partition_name: &str,
        partition_conditions: &[Condition],
    ) -> ExtResult<Vec<PartitionRow>> {
        let mut where_clause = partition_conditions
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
        if !where_clause.is_empty() {
            where_clause = format!("WHERE {}", where_clause);
        }
        let sql = format!(
            "SELECT rowid as row_id, * FROM {} {}",
            partition_name, where_clause
        );
        let mut stmt = self.meta_table.connection.prepare(&sql)?;

        for (index, condition) in partition_conditions.iter().enumerate() {
            condition.value.bind_param(&mut stmt, (index + 1) as i32)?;
        }

        let result_rows = stmt.query(())?;

        let mut rows = Vec::new();
        while let Ok(Some(row)) = result_rows.next() {
            println!("rows {:#?}", row);
            let columns = (0..row.len())
                .filter_map(|index| {
                    let column = row.index(index);
                    PartitionColumn::new(column).ok()
                })
                .collect::<Vec<_>>();

            if let Some(result_row) = columns.into_iter().collect::<Option<PartitionRow>>() {
                rows.push(result_row);
            }
        }
        Ok(rows)
    }

    /// Queries partitions based on specified WHERE clause conditions and populates the cursor state.
    ///
    /// This method orchestrates the process of querying partitions within specified value ranges,
    /// executing partition-specific queries, and collecting the results. It updates the cursor's
    /// internal state to include the results from all queried partitions, readying it for row iteration.
    ///
    /// The function aggregates conditions into value ranges, identifies relevant partitions,
    /// executes queries against those partitions, and finally aggregates the results into the
    /// cursor's state.
    ///
    /// # Parameters
    ///
    /// * `partition_where_str` - A string representing the WHERE clause for partition queries.
    /// * `lookup_conditions` - A vector of `Condition` objects representing the conditions to apply to the lookup.
    /// * `args` - A mutable slice of `ValueRef`, representing bound parameters for the query.
    ///
    /// # Returns
    ///
    /// A `Result<(), Error>` indicating the success or failure of the operation. On success, the cursor's
    /// internal state is updated with the query results. On failure, an error is returned detailing the issue.
    fn query_partitions<'b>(
        &mut self,
        partition_conditions: &'b [Condition],
        lookup_conditions: &'b [Condition],
    ) -> ExtResult<()> {
        let ranges = aggregate_conditions_to_ranges(
            lookup_conditions,
            self.meta_table.interface.partition_interval(),
        );

        let (lower_bound, upper_bound) = ranges
            .get("partition_value")
            .unwrap_or(&(Bound::Unbounded, Bound::Unbounded));
        for (partition_value, partition_name) in self
            .borrow_mut()
            .get_partitions_to_query(lower_bound, upper_bound)?
        {
            let rows = &self.execute_partition_query(&partition_name, partition_conditions)?;
            if let Some(partition) = Partition::new(partition_value, partition_name, rows.clone()) {
                self.partitions.push(partition);
            }
        }

        self.current_partition_index = 0;
        Ok(())
    }
}

impl<'vtab> VTabCursor<'vtab> for RangePartitionCursor<'vtab> {
    /// Filters rows in the current cursor based on the provided WHERE clause conditions.
    ///
    /// This method prepares the cursor for row iteration by querying partitions based on
    /// specified conditions and setting up internal state for row traversal.
    ///
    /// # Parameters
    ///
    /// * `_idx_num` - An integer representing the index number used for optimization. Currently unused.
    /// * `idx_str` - An optional string representing serialized WHERE clause conditions.
    /// * `args` - A mutable slice of `ValueRef`, representing bound parameters for the query.
    ///
    /// # Returns
    ///
    /// A `Result<(), Error>` indicating the success or failure of the filter operation.
    fn filter(
        &mut self,
        _idx_num: i32,
        idx_str: Option<&str>,
        args: &mut [&mut ValueRef],
    ) -> ExtResult<()> {
        println!("filter");
        let where_clauses_serialized = idx_str.unwrap_or("");
        let where_clauses: WhereClauses =
            ron::from_str(where_clauses_serialized).unwrap_or(WhereClauses(HashMap::default()));

        let lookup_conditions =
            parse_conditions(where_clauses.get("lookup_table"), args).unwrap_or_default();
        let partition_conditions =
            parse_conditions(where_clauses.get("partition_table"), args).unwrap_or_default();

        // let (partition_conditions, lookup_conditions) = match parse_where_clause(idx_str, args) {
        //     Ok(value) => value,
        //     Err(err) => return Err(sqlite3_ext::Error::Module(err.to_string())),
        // };
        self.query_partitions(&partition_conditions, &lookup_conditions)?;

        Ok(())
    }
    /// Advances the cursor to the next row within the current or next partition.
    ///
    /// Attempts to move to the next row within the current partition. If no further rows are
    /// available, it tries to move to the first row of the next partition.
    ///
    /// # Returns
    ///
    /// A `Result<(), Error>` indicating the success or failure of advancing the cursor.
    fn next(&mut self) -> ExtResult<()> {
        // Attempt to advance to the next row within the current partition.
        // If there's no next row (None is returned), attempt to move to the next partition.
        if self.advance_to_next_row().is_none() {
            // Attempt to move to the next partition and then to its first row.
            // If successful, it means we've advanced to a new partition, so we increment the counter.
            if let Some(_row) = self
                .advance_to_next_partition()
                .and_then(|partition| partition.get_current_row())
            // .get_mut_current_row()
            {
                self.internal_rowid_counter += 1;
            }
        } else {
            // Successfully moved to the next row within the same partition, increment the counter.
            self.internal_rowid_counter += 1;
        }
        Ok(())
    }

    /// Checks if the cursor has reached the end of available rows.
    ///
    /// # Returns
    ///
    /// `true` if there are no more rows to iterate over, otherwise `false`.
    fn eof(&self) -> bool {
        self.get_current_row().is_none()
    }
    /// Retrieves the value of the column at the specified index in the current row.
    ///
    /// # Parameters
    ///
    /// * `idx` - The zero-based index of the column whose value is to be retrieved (rowid column excluded).
    /// * `c` - A context object used to set the result of the column retrieval.
    ///
    /// # Returns
    ///
    /// A `Result<(), Error>` indicating the success or failure of the column retrieval operation.
    fn column(&self, idx: usize, c: &ColumnContext) -> ExtResult<()> {
        if let Some(current_row) = self.get_current_row() {
            c.set_result(current_row.columns[idx].value.to_owned())?
        };

        Ok(())
    }

    /// Returns the row ID of the current row.
    ///
    /// # Returns
    ///
    /// A `Result<i64, Error>` containing the row ID of the current row, or an error
    /// if the row ID cannot be retrieved.
    fn rowid(&self) -> ExtResult<i64> {
        let rowid_column = self.get_current_row().map(|row| row.rowid.clone());

        let partition_name = match &self.get_current_partition() {
            Some(partition) => &partition.partition_name,
            None => {
                return Err(sqlite3_ext::Error::Sqlite(
                    1,
                    Some("Could not access current partition".to_owned()),
                ))
            }
        };
        if let Some(column) = rowid_column {
            let mut rowid_mapper = self.meta_table.rowid_mapper.write().map_err(|e| {
                sqlite3_ext::Error::Sqlite(1, Some(format!("Lock acquisition failed: {}", e)))
            })?;

            rowid_mapper.insert(
                self.internal_rowid_counter,
                (column.clone(), partition_name.to_string()),
            );
        }

        Ok(self.internal_rowid_counter)
    }
}

fn parse_conditions<'a>(
    constraints: Option<&'a Vec<WhereClause>>,
    args: &'a [&'a mut ValueRef],
) -> Result<Vec<Condition<'a>>, String> {
    constraints.map_or(Ok(Vec::default()), |constraints| {
        constraints
            .iter()
            .map(|constraint| {
                let arg = args.get(constraint.constraint_index as usize);
                match arg {
                    Some(value) => Ok(Condition {
                        operator: constraint.get_operator(),
                        column: constraint.get_name(),
                        value,
                    }),
                    None => Err(format!(
                        "Argument not found for constraint index {}",
                        constraint.constraint_index
                    )),
                }
            })
            .collect()
    })
}
#[cfg(test)]
mod tests {
    use sqlite3_ext::Value;

    use super::*;

    #[test]
    fn test_partition_result_new_with_empty_rows() {
        let partition_result = Partition::new(1, "test_partition".to_string(), vec![]);
        assert!(partition_result.is_none());
    }

    #[test]
    fn test_partition_result_new_with_non_empty_rows() {
        let rows = vec![PartitionRow {
            rowid: Value::Integer(1),
            columns: vec![PartitionColumn {
                _name: "column1".to_string(),
                value: Value::Integer(42),
            }],
        }];
        let partition_result = Partition::new(1, "test_partition".to_string(), rows);
        assert!(partition_result.is_some());
    }
}
