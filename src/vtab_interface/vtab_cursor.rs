use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::ops::{Bound, Deref, Index};
use std::usize;

use super::PartitionMetaTable;
use crate::constraints::{Conditions, ScanPlan, SortDirection, WhereClauses};
use crate::shadow_tables::{Partition, PartitionQuery};
use crate::utils::aggregate_conditions_to_ranges;
use sqlite3_ext::query::QueryResult;
use sqlite3_ext::vtab::ColumnContext;
use sqlite3_ext::{vtab::VTabCursor, ValueRef};
use sqlite3_ext::{FromValue, Result as ExtResult};

/// Represents a cursor for iterating over partitioned data in a virtual table.
///
/// The cursor maintains internal state to track the current partition and row, allowing
/// for seamless iteration and data retrieval across multiple partitions based on query conditions.
#[derive(Debug)]
pub struct RangePartitionCursor<'vtab> {
    /// Tracks the internal ROWID counter for the cursor's current position.
    pub internal_rowid_counter: i64,
    /// Reference to the metadata table associated with the partitioned data.
    pub meta_table: &'vtab PartitionMetaTable<'vtab>,
    /// Iterator over partitions prepared for querying based on the current query conditions.
    pub prepared_partitions: std::vec::IntoIter<Partition>,
    /// The current partition under iteration by the cursor.
    pub current_partition: Option<Partition>,
    /// Indicates whether the cursor has reached the end of available data.
    pub eof: bool,
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
            prepared_partitions: std::vec::IntoIter::default(),
            current_partition: None,
            eof: false,
        }
    }

    /// Retrieves a mutable reference to the current partition, if any.
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
    /// An `Option<&mut Partition>` which is:
    /// - `Some(&mut Partition)` if the next partition exists within the current result set.
    /// - `None` if there are no more partitions in the current result set.
    fn advance_to_next_partition(&mut self) -> Option<&Partition> {
        self.current_partition = self.prepared_partitions.borrow_mut().next();
        self.get_current_partition()
    }
    /// Advances the cursor to the next row within the current partition.
    ///
    /// # Returns
    ///
    /// An `Option<&mut QueryResult>` which is:
    /// - `Some(&mut QueryResult)` if the next row exists within the current partition.
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

    /// Initializes cursor with partitions matching specified conditions.
    ///
    /// # Parameters
    /// * `partition_conditions` - Optional conditions specific to the partition table.
    /// * `lookup_conditions` - Optional conditions for looking up partitions.
    ///
    /// # Returns
    /// An iterator over partitions that match the given conditions.
    fn initialize_partitions<'b>(
        &mut self,
        partition_conditions: Option<&'b Conditions<'b>>,
        lookup_conditions: Option<&'b Conditions<'b>>,
        partition_order: Option<SortDirection>,
    ) -> ExtResult<std::vec::IntoIter<Partition>> {
        let ranges = lookup_conditions
            .zip(Some(self.meta_table.interface.partition_interval()))
            .map(|(conditions, interval)| {
                aggregate_conditions_to_ranges(conditions.as_slice(), interval)
            })
            .transpose()?
            .unwrap_or_default();

        let (lower_bound, upper_bound) = ranges
            .get("partition_value")
            .unwrap_or(&(Bound::Unbounded, Bound::Unbounded));

        let mut partitions_in_range = self.get_partitions_to_query(lower_bound, upper_bound)?;
        if partition_order == Some(SortDirection::Desc) {
            partitions_in_range.reverse();
        }
        let order_column = partition_order
            .map(|direction| (self.meta_table.interface.partition_column_name(), direction));
        let prepared_partitions: ExtResult<Vec<Partition>> = partitions_in_range.iter().try_fold(
            Vec::new(),
            |mut accumulator, (_partition_value, partition_name)| {
                let partition: Partition = Partition::try_from(PartitionQuery {
                    db: self.meta_table.connection,
                    partition_name: partition_name.as_str(),
                    conditions: partition_conditions,
                    order: order_column,
                })?;
                accumulator.push(partition);
                Ok(accumulator)
            },
        );
        let prepared_partitions = prepared_partitions?;

        let mut partition_iter = prepared_partitions.into_iter();
        self.current_partition = partition_iter.next();
        // Skip partitions whose filtered query yields no rows; leaving an exhausted
        // partition as current would surface a phantom row to SQLite.
        while let Some(partition) = self.current_partition.as_mut() {
            if partition.next_row()?.is_some() {
                break;
            }
            self.current_partition = partition_iter.next();
        }
        if self.current_partition.is_none() {
            self.eof = true;
        }

        Ok(partition_iter)
    }
}

impl<'vtab> VTabCursor for RangePartitionCursor<'vtab> {
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
        self.meta_table.interface.flush_all()?;
        self.eof = false;
        let scan_plan_serialized = idx_str.unwrap_or("");
        let scan_plan: ScanPlan = if scan_plan_serialized.is_empty() {
            ScanPlan::new(WhereClauses(HashMap::default()), None)
        } else {
            ron::from_str(scan_plan_serialized)
                .map_err(|err| sqlite3_ext::Error::Module(err.to_string()))?
        };
        let where_clauses = scan_plan.where_clauses;
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

        self.prepared_partitions = self.initialize_partitions(
            partition_conditions.as_ref(),
            lookup_conditions.as_ref(),
            scan_plan.partition_order,
        )?;

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
        // Advance row-by-row, skipping partitions whose filtered query yields no rows.
        loop {
            if self.advance_to_next_row()?.is_some() {
                self.internal_rowid_counter += 1;
                return Ok(());
            }
            if self.advance_to_next_partition().is_none() {
                self.eof = true;
                return Ok(());
            }
        }
    }

    /// Checks if the cursor has reached the end of available rows.
    ///
    /// # Returns
    ///
    /// `true` if there are no more rows to iterate over, otherwise `false`.
    fn eof(&mut self) -> bool {
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
    fn column(&mut self, idx: usize, c: &ColumnContext) -> ExtResult<()> {
        if let Some(current_row) = self.get_current_row() {
            c.set_result(current_row.index(idx + 1).as_ref())?
        };

        Ok(())
    }

    /// Returns the row ID of the current row.
    ///
    /// # Returns
    /// The row ID or an error if it cannot be retrieved.
    fn rowid(&mut self) -> ExtResult<i64> {
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
            // The internal rowid counter is sequential, so the mapper is a plain Vec
            // indexed by counter — no hashing. Overwrite on revisit (e.g. re-filtered
            // cursor), append otherwise.
            let idx = self.internal_rowid_counter as usize;
            let entry = (column.get_i64(), partition_name.to_string());
            match rowid_mapper.get_mut(idx) {
                Some(slot) => *slot = entry,
                None => rowid_mapper.push(entry),
            }
        }

        Ok(self.internal_rowid_counter)
    }
}
