use std::ops::{Bound, Index};
use std::usize;

use super::{PartitionMetaTable, WhereClauses};
use crate::shadow_tables::{Partition, PartitionColumn, PartitionRow};
use crate::utils::{aggregate_conditions_to_ranges, Condition};
use crate::{Lookup, PartitionAccessor};
use sqlite3_ext::vtab::ColumnContext;
use sqlite3_ext::{vtab::VTabCursor, ValueRef};
use sqlite3_ext::{FallibleIteratorMut, FromValue, Result as ExtResult};

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

    /// Parses serialized WHERE clause conditions for partition and lookup table queries.
    ///
    /// # Parameters
    ///
    /// * `idx_str` - An optional serialized string representing WHERE clause conditions.
    /// * `args` - A mutable slice of `ValueRef`, representing bound query parameters.
    ///
    /// # Returns
    ///
    /// A tuple containing:
    /// - A `String` representing the WHERE clause for partition queries.
    /// - A `Vec<Condition>` representing conditions for the lookup table.
    fn parse_where_clauses(
        &self,
        idx_str: Option<&str>,
        args: &mut [&mut ValueRef],
    ) -> (String, Vec<Condition>) {
        let where_clauses_serialized = idx_str.unwrap_or("");

        let where_clauses: Option<WhereClauses> = ron::from_str(where_clauses_serialized).ok();
        let (lookup_where, partition_where) = match &where_clauses {
            Some(clauses) => (clauses.get("lookup_table"), clauses.get("partition_table")),
            None => (None, None),
        };
        let lookup_conditions = lookup_where.map_or(Vec::default(), |constraints| {
            constraints
                .iter()
                .filter_map(|constraint| {
                    args[constraint.constraint_index as usize]
                        .to_owned()
                        .ok()
                        .map(|value| Condition {
                            column: constraint.get_name(),
                            operator: constraint.operator,
                            value,
                        })
                })
                .collect()
        });

        let partition_where_str = partition_where.map_or(String::default(), |vec| {
            format!(
                "WHERE {}",
                vec.iter()
                    .map(|clause| clause.to_string())
                    .collect::<Vec<String>>()
                    .join(" AND ")
            )
        });

        (partition_where_str, lookup_conditions)
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
        lower_bound: Bound<i64>,
        upper_bound: Bound<i64>,
    ) -> ExtResult<Vec<(i64, String)>> {
        self.meta_table
            .partition_interface
            .get_lookup()
            .get_partitions_by_range(self.meta_table.connection, lower_bound, upper_bound)
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
        partition_where_str: &str,
        args: &mut [&mut ValueRef],
    ) -> ExtResult<Vec<PartitionRow>> {
        let sql = format!(
            "SELECT rowid as row_id, * FROM {} {}",
            partition_name, partition_where_str
        );
        let mut stmt = self.meta_table.connection.prepare(&sql)?;
        let result_rows = stmt.query(args.as_mut())?;

        let mut rows = Vec::new();
        while let Ok(Some(row)) = result_rows.next() {
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
    fn query_partitions(
        &mut self,
        partition_where_str: &str,
        lookup_conditions: Vec<Condition>,
        args: &mut [&mut ValueRef],
    ) -> ExtResult<()> {
        let ranges = aggregate_conditions_to_ranges(
            lookup_conditions,
            self.meta_table
                .partition_interface
                .get_root()
                .get_interval(),
        );

        let (lower_bound, upper_bound) = ranges
            .get("partition_value")
            .unwrap_or(&(Bound::Unbounded, Bound::Unbounded));
        let partitions_to_query = self.get_partitions_to_query(*lower_bound, *upper_bound)?;
        for (partition_value, partition_name) in partitions_to_query {
            let rows = self.execute_partition_query(&partition_name, partition_where_str, args)?;
            if let Some(partition) = Partition::new(partition_value, &partition_name, rows) {
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
        let (partition_where_str, lookup_conditions) = self.parse_where_clauses(idx_str, args);
        self.query_partitions(&partition_where_str, lookup_conditions, args)?;

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

#[cfg(test)]
mod tests {
    use sqlite3_ext::Value;

    use super::*;

    #[test]
    fn test_partition_result_new_with_empty_rows() {
        let partition_result = Partition::new(1, &"test_partition".to_string(), vec![]);
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
        let partition_result = Partition::new(1, &"test_partition".to_string(), rows);
        assert!(partition_result.is_some());
    }
}
