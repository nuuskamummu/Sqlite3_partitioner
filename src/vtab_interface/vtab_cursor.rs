use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::ops::{Bound, Deref, Index};
use std::usize;

use super::{PartitionMetaTable, WhereClauses};
use crate::shadow_tables::Partition;
use crate::utils::{aggregate_conditions_to_ranges, Conditions};
use sqlite3_ext::query::QueryResult;
use sqlite3_ext::vtab::ColumnContext;
use sqlite3_ext::{vtab::VTabCursor, ValueRef};
use sqlite3_ext::{FromValue, Result as ExtResult};

#[derive(Debug)]
pub struct RangePartitionCursor<'vtab> {
    pub internal_rowid_counter: i64,
    pub meta_table: &'vtab PartitionMetaTable<'vtab>,
    pub prepared_partitions: std::vec::IntoIter<Partition>,
    pub current_partition: Option<Partition>,
    pub eof: bool,
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
            meta_table,
            internal_rowid_counter: i64::default(),
            current_partition_index: usize::default(),
            prepared_partitions: std::vec::IntoIter::default(),
            current_partition: None,
            eof: false,
        }
    }

    pub fn get_mut_current_partition(&mut self) -> Option<&mut Partition> {
        self.current_partition.borrow_mut().as_mut()
    }
    fn get_current_partition(&self) -> Option<&Partition> {
        self.current_partition.as_ref()
    }
    fn get_current_row(&self) -> Option<&QueryResult> {
        let current_partition = self.get_current_partition();
        let row = match current_partition {
            Some(partition) => partition.get_current_row(),
            None => None,
        };
        row
    }
    /// Advances the cursor to the next partition.
    ///
    /// # Returns
    ///
    /// An `Option<&mut PartitionResult>` which is:
    /// - `Some(&mut PartitionResult)` if the next partition exists within the current result set.
    /// - `None` if there are no more partitions in the current result set.
    fn advance_to_next_partition(&mut self) -> Option<&Partition> {
        self.current_partition = self.prepared_partitions.borrow_mut().next();
        self.get_current_partition()
    }
    /// Advances the cursor to the next row within the current partition.
    ///
    /// # Returns
    ///
    /// An `Option<&mut ResultRow>` which is:
    /// - `Some(&mut ResultRow)` if the next row exists within the current partition.
    /// - `None` if there are no more rows in the current partition.
    fn advance_to_next_row(&mut self) -> ExtResult<Option<&mut QueryResult>> {
        let current_partition = self.get_mut_current_partition();
        match current_partition {
            Some(v) => v.next_row(),
            None => Ok(None),
        }
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

    fn initialize_partitions<'b>(
        &mut self,
        partition_conditions: Option<&'b Conditions<'b>>,
        lookup_conditions: Option<&'b Conditions<'b>>,
    ) -> ExtResult<std::vec::IntoIter<Partition>> {
        let ranges = lookup_conditions
            .zip(Some(self.meta_table.interface.partition_interval()))
            .map(|(conditions, interval)| {
                aggregate_conditions_to_ranges(conditions.as_slice(), interval)
            })
            .unwrap_or_default();

        let (lower_bound, upper_bound) = ranges
            .get("partition_value")
            .unwrap_or(&(Bound::Unbounded, Bound::Unbounded));

        let prepared_partitions: ExtResult<Vec<Partition>> = self
            .borrow_mut()
            .get_partitions_to_query(lower_bound, upper_bound)?
            .iter()
            .try_fold(
                Vec::new(),
                |mut accumulator, (_partition_value, partition_name)| {
                    let partition: Partition = Partition::try_from((
                        self.meta_table.connection,
                        partition_name.as_str(),
                        partition_conditions,
                    ))?;
                    accumulator.push(partition);
                    Ok(accumulator)
                },
            );
        let prepared_partitions = prepared_partitions?;

        let mut partition_iter = prepared_partitions.into_iter();
        self.current_partition = partition_iter.next();
        self.current_partition
            .as_mut()
            .and_then(|partition| partition.next_row().transpose());

        Ok(partition_iter)
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
        let where_clauses_serialized = idx_str.unwrap_or("");
        let where_clauses: WhereClauses =
            ron::from_str(where_clauses_serialized).unwrap_or(WhereClauses(HashMap::default()));
        let lookup_conditions: Option<Conditions> = where_clauses
            .get("lookup_table")
            .map(|where_clauses| Conditions::try_from((where_clauses, args.deref())))
            .transpose()
            .map_err(|err| sqlite3_ext::Error::Module(err.to_string()))?;

        let partition_conditions: Option<Conditions> = where_clauses
            .get("partition_table")
            .map(|where_clauses| Conditions::try_from((where_clauses, args.deref())))
            .transpose()
            .map_err(|err| sqlite3_ext::Error::Module(err.to_string()))?;

        self.prepared_partitions =
            self.initialize_partitions(partition_conditions.as_ref(), lookup_conditions.as_ref())?;

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
        // Attempt to advance to the next row within the current partition.
        // If there's no next row (None is returned), attempt to move to the next partition.
        let did_advance = match self.advance_to_next_row()? {
            Some(_) => true,
            None => match self.advance_to_next_partition() {
                Some(_) => self.advance_to_next_row()?.is_some(),
                None => false,
            },
        };
        if did_advance {
            self.internal_rowid_counter += 1;
        } else {
            self.eof = true;
        }
        Ok(())
    }

    /// Checks if the cursor has reached the end of available rows.
    ///
    /// # Returns
    ///
    /// `true` if there are no more rows to iterate over, otherwise `false`.
    fn eof(&self) -> bool {
        self.eof
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
            c.set_result(current_row.index(idx + 1).as_ref())?
        };

        Ok(())
    }

    /// Returns the row ID of the current row.
    ///
    /// # Returns
    ///
    /// A `Ok<i64>` containing the row ID of the current row, or an Err
    /// if the row ID cannot be retrieved.
    fn rowid(&self) -> ExtResult<i64> {
        let rowid_column = self.get_current_row().map(|row| row.index(0));
        let partition_name = match self.get_current_partition() {
            Some(partition) => partition.get_name(),
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
                (column.get_i64(), partition_name.to_string()),
            );
        }

        Ok(self.internal_rowid_counter)
    }
}
