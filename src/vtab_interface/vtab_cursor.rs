use std::ops::{Bound, Index};
use std::usize;

use super::{PartitionMetaTable, WhereClauses};
use crate::utils::{aggregate_conditions_to_ranges, Condition};
use crate::{Lookup, PartitionAccessor, Root};
use sqlite3_ext::query::Column;
use sqlite3_ext::vtab::ColumnContext;
use sqlite3_ext::{vtab::VTabCursor, Value, ValueRef};
use sqlite3_ext::{FallibleIteratorMut, FromValue, Result as ExtResult};
use std::iter::FromIterator;

#[derive(Debug)]
pub struct RangePartitionCursor<'vtab> {
    internal_rowid_counter: i64,
    meta_table: &'vtab PartitionMetaTable<'vtab>,
    partitions: Vec<PartitionResult>,
    current_partition_index: usize, // current_partition: Option<&'vtab PartitionResult<'vtab>>,
}
/// Represents the result of a query against a single partition.
///
/// Contains information about the partition, including its value, name, and the rows retrieved
/// from the partition as a result of a query.
///
/// # Attributes
///
/// * `partition_value` - The index or identifier for the partition within the lookup table.
/// * `partition_name` - The name of the partition.
/// * `rows` - A vector of `ResultRow` instances representing the rows retrieved from the partition.
#[derive(Debug)]
pub struct PartitionResult {
    pub partition_value: i64, //index in lookup_table.partitions.
    pub partition_name: String,
    pub rows: Vec<ResultRow>,
    current_row_index: usize,
}
impl PartitionResult {
      /// Creates a new `PartitionResult` instance.
    ///
    /// Returns `None` if the rows vector is empty, indicating that no data was retrieved
    /// for the partition.
    ///
    /// # Parameters
    ///
    /// * `partition_value` - The index or identifier for the partition.
    /// * `partition_name` - The name of the partition.
    /// * `rows` - A vector of `ResultRow` instances to be associated with this partition.
    ///
    /// # Returns
    ///
    /// An `Option<PartitionResult>`, which is `None` if `rows` is empty.
    fn new(partition_value: i64, partition_name: &String, rows: Vec<ResultRow>) -> Option<Self> {
        if rows.is_empty() {
            return None;
        }

        Some(Self {
            partition_value,
            partition_name: partition_name.to_owned(),
            rows,
            current_row_index: 0,
        })
    }
    fn get_mut_current_row(&mut self) -> Option<&mut ResultRow> {
        self.rows.get_mut(self.current_row_index)
    }
    fn get_current_row(&self) -> Option<&ResultRow> {
        self.rows.get(self.current_row_index)
    }
    fn advance_to_next_row(&mut self) -> Option<&mut ResultRow> {
        self.current_row_index += 1;
        self.get_mut_current_row()
    }
}

/// Represents a single row retrieved from a partition.
///
/// This struct encapsulates the data for a row, including a unique identifier (`rowid`)
/// and the columns of data contained within the row.
///
/// # Attributes
///
/// * `rowid` - The unique identifier for the row within its partition.
/// * `columns` - A vector of `ResultColumn` instances representing the data within the row.
#[derive(Debug)]
pub struct ResultRow {
    rowid: Value,
    columns: Vec<ResultColumn>,
}

impl FromIterator<ResultColumn> for Option<ResultRow> {
    fn from_iter<T: IntoIterator<Item = ResultColumn>>(iter: T) -> Self {
        let mut iter = iter.into_iter();
        let first_column = match iter.next() {
            Some(column) => column,
            None => return None,
        };

        Some(ResultRow {
            rowid: first_column.value,
            columns: iter.collect(),
        })
    }
}
/// Represents a single column within a `ResultRow`.
///
/// Encapsulates the name and value of the column, providing structured access to row data.
///
/// # Attributes
///
/// * `_name` - The name of the column.
/// * `value` - The value stored in the column, encapsulated as a `Value`.
#[derive(Debug, Clone)]
pub struct ResultColumn {
    _name: String,
    value: Value,
}
/// Constructs a new `ResultColumn` from a SQLite column.
///
/// # Parameters
///
/// * `column` - A reference to the SQLite column from which to construct the `ResultColumn`.
///
/// # Returns
///
/// A `Result` which is:
/// - `Ok(Self)` on success, containing the newly created `ResultColumn`.
/// - `Err(e)` on failure, where `e` is an error that occurred during column creation.
impl ResultColumn {
    fn new(column: &Column) -> ExtResult<Self> {
        let name = column.name()?.to_owned();
        let value = column.to_owned()?;
        Ok(Self { _name: name, value })
    }
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

    fn get_mut_current_partition(&mut self) -> Option<&mut PartitionResult> {
        self.partitions.get_mut(self.current_partition_index)
    }
    fn get_current_partition(&self) -> Option<&PartitionResult> {
        self.partitions.get(self.current_partition_index)
    }
    fn get_current_row(&self) -> Option<&ResultRow> {
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
    fn advance_to_next_partition(&mut self) -> Option<&mut PartitionResult> {
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
    fn advance_to_next_row(&mut self) -> Option<&mut ResultRow> {
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
    /// Queries partitions based on specified WHERE clause conditions and updates the cursor state.
    ///
    /// # Parameters
    ///
    /// * `partition_where_str` - A `String` representing the WHERE clause for partition queries.
    /// * `lookup_conditions` - Conditions for the lookup table.
    /// * `args` - Query parameters.
    ///
    /// # Returns
    ///
    /// A `Result` which is:
    /// - `Ok(())` on successful query execution and state update.
    /// - `Err(e)` on failure, where `e` is an error that occurred during query execution.
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

        let partitions: ExtResult<Vec<PartitionResult>> = self
            .meta_table
            .partition_interface
            .get_lookup()
            .get_partitions_by_range(self.meta_table.connection, *lower_bound, *upper_bound)?
            .iter()
            .try_fold(Vec::new(), |mut acc, (partition_value, partition_name)| {
                let sql = format!(
                    "SELECT rowid as row_id, * FROM {} {}",
                    partition_name, partition_where_str
                );
                let mut stmt = self.meta_table.connection.prepare(&sql)?;
                let result_rows = stmt.query(args.as_mut())?;

                let mut row_columns = Vec::new();
                while let Ok(Some(row)) = result_rows.next() {
                    let columns = (0..row.len())
                        .filter_map(|index| {
                            let column = row.index(index);
                            ResultColumn::new(column).ok()
                        });
                
                    if let Some(result_row) = columns.collect::<Option<ResultRow>>() {
                        row_columns.push(result_row);
                    }
                }

                if let Some(partition) =
                    PartitionResult::new(*partition_value, partition_name, row_columns)
                {
                    acc.push(partition);
                }
                Ok(acc)
            });
        self.partitions = partitions?;
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
