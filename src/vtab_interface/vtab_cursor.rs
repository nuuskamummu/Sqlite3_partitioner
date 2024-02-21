use std::ops::{Bound, Index};
use std::usize;

use super::{PartitionMetaTable, WhereClauses};
use crate::shadow_tables::Bucket;
use crate::utils::{aggregate_conditions_to_ranges, Condition};
use crate::vtab_interface::bucket_cursor::BucketCursor;
use crate::{Lookup, PartitionAccessor};
use sqlite3_ext::ffi::SQLITE_ERROR;
use sqlite3_ext::query::Column;
use sqlite3_ext::vtab::{ColumnContext, UpdateVTab, VTab};
use sqlite3_ext::{vtab::VTabCursor, Value, ValueRef};
use sqlite3_ext::{FallibleIteratorMut, FromValue, Result as ExtResult};

// #[derive(Debug)]
pub struct RangePartitionCursor<'cursor> {
    result_iterator_counter: i64,
    meta_table: &'cursor PartitionMetaTable<'cursor>,
    buckets: Vec<ResultBucket>,
}

#[derive(Debug, Clone)]
pub struct ResultBucket {
    pub partition_name: String,
    partition_index: i64, //index in lookup_table.partitions.
    pub rows: Vec<ResultRow>,
}
impl ResultBucket {
    pub fn new(partition_name: String, partition_index: i64, rows: Vec<ResultRow>) -> Self {
        Self {
            partition_name,
            partition_index,
            rows,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ResultRow {
    columns: Vec<ResultColumn>,
}
impl ResultRow {
    pub fn rowid_column(&self) -> sqlite3_ext::Result<&ResultColumn> {
        println!("{:#?}", self.columns);
        self.columns
            .iter()
            .find(|col| col.name.to_lowercase() == "rowid")
            .ok_or_else(|| {
                sqlite3_ext::Error::Sqlite(SQLITE_ERROR, Some("Rowid column not found".into()))
            })
    }
    pub fn get_columns(&self) -> &Vec<ResultColumn> {
        &self.columns
    }
}

impl FromIterator<ResultColumn> for ResultRow {
    fn from_iter<T: IntoIterator<Item = ResultColumn>>(iter: T) -> Self {
        let columns: Vec<ResultColumn> = iter.into_iter().collect();
        Self { columns }
    }
}

#[derive(Debug, Clone)]
pub struct ResultColumn {
    name: String,
    value: Value,
}
impl ResultColumn {
    pub fn new(column: &Column) -> ExtResult<Self> {
        let name = column.name()?.to_owned();
        let value = column.to_owned()?;
        Ok(Self { name, value })
    }
    pub fn get_value(&self) -> &Value {
        &self.value
    }
}
impl<'cursor> RangePartitionCursor<'cursor> {
    pub fn new(meta_table: &'cursor PartitionMetaTable<'cursor>) -> Self {
        Self {
            result_iterator_counter: i64::default(),
            buckets: Vec::new(),
            meta_table,
        }
    }
    fn get_current_partition(&self) -> ExtResult<(i64, String)> {
        self.meta_table
            .partition_interface
            .get_lookup()
            .access_current_entry(|(key, value)| (*key, value.clone()))
            .ok_or_else(|| {
                sqlite3_ext::Error::Sqlite(
                    SQLITE_ERROR,
                    Some("Could not access current partition".into()),
                )
            })
    }
    fn get_current_bucket(&self) -> ExtResult<(usize, &ResultBucket)> {
        let current_partition = self.get_current_partition()?;
        self.buckets
            .iter()
            .enumerate()
            .find(|(_index, row)| row.partition_index == current_partition.0)
            .ok_or_else(|| {
                sqlite3_ext::Error::Sqlite(
                    SQLITE_ERROR,
                    Some("Could not find current bucket".into()),
                )
            })
    }
    fn get_current_row(&self) -> ExtResult<&ResultRow> {
        let (_index, current_bucket) = self.get_current_bucket()?;
        current_bucket
            .rows
            .get(self.result_iterator_counter as usize)
            .ok_or_else(|| {
                sqlite3_ext::Error::Sqlite(
                    SQLITE_ERROR,
                    Some("Could not access current row".into()),
                )
            })
    }
    fn advance_to_next_bucket(&mut self) -> ExtResult<bool> {
        let current_partition = self.get_current_partition()?;
        let current_bucket_index = self
            .buckets
            .iter()
            .position(|bucket| bucket.partition_index == current_partition.0)
            .ok_or_else(|| {
                sqlite3_ext::Error::Sqlite(SQLITE_ERROR, Some("Current bucket not found".into()))
            })?;

        if let Some(next_bucket) = self.buckets.get(current_bucket_index + 1) {
            self.meta_table
                .partition_interface
                .get_lookup()
                .update_current_entry(next_bucket.partition_index);
            self.result_iterator_counter = 0; // Reset for new bucket
            Ok(true) // Successfully moved to the next bucket
        } else {
            Ok(false) // EOF, no more buckets
        }
    }
    fn advance_to_next_row(&mut self) -> ExtResult<bool> {
        let (_index, current_bucket) = match self.get_current_bucket() {
            Ok(v) => v,
            Err(_err) => return Ok(false), // EOF or error accessing current bucket
        };

        if let Some(_row) = current_bucket
            .rows
            .get((self.result_iterator_counter as usize) + 1)
        {
            self.result_iterator_counter += 1;
            Ok(true) // Successfully advanced to the next row
        } else {
            self.advance_to_next_bucket()
        }
    }
    fn parse_where_clauses(
        &self,
        idx_str: Option<&str>,
        args: &mut [&mut ValueRef],
    ) -> (String, Vec<Condition>) {
        let where_clauses_serialized = idx_str.unwrap_or("");
        let where_clauses: &WhereClauses = &ron::from_str(where_clauses_serialized).unwrap();

        let lookup_where = where_clauses.get("lookup_table");
        let partition_where = where_clauses.get("partition_table");

        let lookup_conditions = lookup_where.map_or(Vec::new(), |constraints| {
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
    fn get_partition_targets(
        &self,

        lookup_conditions: Vec<Condition>,
        args: &mut [&mut ValueRef],
    ) -> ExtResult<Vec<(i64, String)>> {
        let ranges = aggregate_conditions_to_ranges(lookup_conditions);
        let (lower_bound, upper_bound) = ranges
            .get("partition_value")
            .unwrap_or(&(Bound::Unbounded, Bound::Unbounded));

        self.meta_table
            .partition_interface
            .get_lookup()
            .get_partitions_by_range(self.meta_table.connection, *lower_bound, *upper_bound)
    }
    fn setup_cursor_state(&mut self) {
        if let Some(first_query) = self.buckets.first() {
            self.result_iterator_counter = 0;
            self.meta_table
                .partition_interface
                .get_lookup()
                .update_current_entry(first_query.partition_index);
        } else {
            self.result_iterator_counter = -1;
        }
    }
}

impl<'cursor> VTabCursor<'cursor> for RangePartitionCursor<'cursor> {
    fn filter(
        &mut self,
        _idx_num: i32,
        idx_str: Option<&str>,
        args: &mut [&mut ValueRef],
    ) -> ExtResult<()> {
        let (partition_where_str, lookup_conditions) = self.parse_where_clauses(idx_str, args);
        let partition_targets = self.get_partition_targets(lookup_conditions, args)?;

        // Clear existing cursors and prepare for new query
        // self.partition_buckets.clear();
        // let mut bucket_modules = self.meta_table.bucket_modules.write().unwrap();
        // bucket_modules.clear();

        // let slice_of_str: &[&str] = &a;
        let a = partition_targets
            .iter()
            .map(|(partition_value, partition_name)| {
                BucketCursor::new(
                    Bucket::new(
                        partition_name.to_string(),
                        self.meta_table
                            .partition_interface
                            .get_template()
                            .columns
                            .clone(),
                        *partition_value,
                    ),
                    // self.meta_table.aux,
                )
            });

        for mut b in a {
            let c = b.filter(&partition_where_str, args, self.meta_table.connection)?;
            match c {
                Some(result) => {
                    self.buckets.push(result.clone());
                    self.meta_table
                        .aux
                        .write()
                        .unwrap()
                        .insert(b.bucket_module.get_partition_value(), result.clone());
                }
                None => (),
            }
        }

        self.result_iterator_counter = 0;
        self.setup_cursor_state();
        Ok(())
    }

    fn next(&mut self) -> ExtResult<()> {
        match self.advance_to_next_row() {
            Ok(true) => Ok(()), // Successfully advanced to the next row/bucket
            Ok(false) => {
                // EOF reached, no more rows or buckets to advance to
                // println!("EOF reached.");
                self.result_iterator_counter = -1; // Optionally mark as EOF
                Ok(())
            }
            Err(e) => {
                // println!("Error advancing to next row/bucket: {:?}", e);
                Err(e)
            }
        }?;
        Ok(())
    }

    fn eof(&self) -> bool {
        let eof = self.result_iterator_counter == -1;
        eof
    }
    fn column(&self, idx: usize, c: &ColumnContext) -> ExtResult<()> {
        let current_row = self.get_current_row()?;
        let column = current_row.columns.get(idx).ok_or_else(|| {
            sqlite3_ext::Error::Sqlite(SQLITE_ERROR, Some("Could not access column".into()))
        })?;
        c.set_result(column.value.clone())?;

        Ok(())
    }

    fn rowid(&self) -> ExtResult<i64> {
        println!("ROWID {:?}", self.result_iterator_counter);
        let current_row = self.get_current_row()?;
        let rowid_column = current_row
            .columns
            .iter()
            .find(|col| col.name == "rowid")
            .ok_or_else(|| {
                sqlite3_ext::Error::Sqlite(SQLITE_ERROR, Some("Rowid column not found".into()))
            })?;

        match rowid_column.value {
            Value::Integer(id) => Ok(id),
            _ => Err(sqlite3_ext::Error::Sqlite(
                SQLITE_ERROR,
                Some("Rowid is not an integer".into()),
            )),
        }
    }
}
