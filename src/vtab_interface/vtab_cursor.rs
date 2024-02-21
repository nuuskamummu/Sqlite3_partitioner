use std::ops::{Bound, Index};
use std::usize;

use super::{PartitionMetaTable, WhereClauses};
use crate::utils::{aggregate_conditions_to_ranges, Condition};
use crate::{Lookup, PartitionAccessor};
use sqlite3_ext::ffi::SQLITE_ERROR;
use sqlite3_ext::query::Column;
use sqlite3_ext::vtab::ColumnContext;
use sqlite3_ext::{vtab::VTabCursor, Value, ValueRef};
use sqlite3_ext::{FallibleIteratorMut, FromValue, Result as ExtResult};

#[derive(Debug)]
pub struct RangePartitionCursor<'cursor> {
    result_iterator_counter: i64,
    internal_rowid_counter: i64,
    meta_table: &'cursor PartitionMetaTable<'cursor>,
    buckets: Vec<ResultBucket>,
}

#[derive(Debug)]
pub struct ResultBucket {
    pub partition_index: i64, //index in lookup_table.partitions.
    pub partition_name: String,
    pub rows: Vec<ResultRow>,
}
impl ResultBucket {
    fn new(partition_index: i64, rows: Vec<ResultRow>, partition_name: String) -> Self {
        Self {
            partition_index,
            rows,
            partition_name,
        }
    }
}

#[derive(Debug)]
pub struct ResultRow {
    columns: Vec<ResultColumn>,
}
impl FromIterator<ResultColumn> for ResultRow {
    fn from_iter<T: IntoIterator<Item = ResultColumn>>(iter: T) -> Self {
        let columns: Vec<ResultColumn> = iter.into_iter().collect();
        Self { columns }
    }
}

#[derive(Debug)]
pub struct ResultColumn {
    name: String,
    value: Value,
}
impl ResultColumn {
    fn new(column: &Column) -> ExtResult<Self> {
        let name = column.name()?.to_owned();
        let value = column.to_owned()?;
        Ok(Self { name, value })
    }
}
impl<'cursor> RangePartitionCursor<'cursor> {
    pub fn new(meta_table: &'cursor PartitionMetaTable) -> Self {
        Self {
            result_iterator_counter: i64::default(),
            buckets: Vec::new(),
            meta_table,
            internal_rowid_counter: i64::default(),
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
    fn query_partitions(
        &self,
        partition_where_str: &str,
        lookup_conditions: Vec<Condition>,
        args: &mut [&mut ValueRef],
    ) -> ExtResult<Vec<ResultBucket>> {
        let ranges = aggregate_conditions_to_ranges(lookup_conditions);
        let (lower_bound, upper_bound) = ranges
            .get("partition_value")
            .unwrap_or(&(Bound::Unbounded, Bound::Unbounded));

        self.meta_table
            .partition_interface
            .get_lookup()
            .get_partitions_by_range(self.meta_table.connection, *lower_bound, *upper_bound)?
            .iter()
            .try_fold(Vec::new(), |mut acc, (partition_value, partition_name)| {
                let sql = format!(
                    "SELECT *, rowid FROM {} {}",
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
                        })
                        .collect::<Vec<_>>();

                    if !columns.is_empty() {
                        row_columns.push(ResultRow::from_iter(columns));
                    }
                }

                if !row_columns.is_empty() {
                    acc.push(ResultBucket::new(
                        *partition_value,
                        row_columns,
                        partition_name.clone(),
                    ));
                }
                Ok(acc)
            })
    }
    fn setup_cursor_state(&mut self, queries: Vec<ResultBucket>) {
        if let Some(first_query) = queries.first() {
            self.result_iterator_counter = 0;
            self.meta_table
                .partition_interface
                .get_lookup()
                .update_current_entry(first_query.partition_index);
            self.buckets = queries;
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
        let queries = self.query_partitions(&partition_where_str, lookup_conditions, args)?;
        self.setup_cursor_state(queries);
        Ok(())
    }

    fn next(&mut self) -> ExtResult<()> {
        match self.advance_to_next_row() {
            Ok(true) => {
                let (current_partition, (current_bucket_index, current_bucket), current_row) = (
                    self.get_current_partition().unwrap(),
                    self.get_current_bucket().unwrap(),
                    self.get_current_row().unwrap(),
                );
                let rowid_column = current_row
                    .columns
                    .iter()
                    .find(|col| col.name == "rowid")
                    .ok_or_else(|| {
                        sqlite3_ext::Error::Sqlite(
                            SQLITE_ERROR,
                            Some("Rowid column not found".into()),
                        )
                    })?;

                let rowid = match rowid_column.value {
                    Value::Integer(id) => Ok(id),
                    _ => Err(sqlite3_ext::Error::Sqlite(
                        SQLITE_ERROR,
                        Some("Rowid is not an integer".into()),
                    )),
                }?;
                if current_bucket.rows.len() > 0 {
                    self.meta_table
                        .rowid_mapper
                        .write()
                        .unwrap()
                        .push((rowid, current_bucket.partition_name.clone()));
                    self.internal_rowid_counter += 1;
                }

                Ok(())
            } // Successfully advanced to the next row/bucket
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
        }
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
        println!("self counter: {:#?}", self.internal_rowid_counter);
        Ok(self.internal_rowid_counter)
        // let current_internal_row_id = self.meta_table.rowid_mapper.write().unwrap();
        // let current_row = self.get_current_row()?;
        // let rowid_column = current_row
        //     .columns
        //     .iter()
        //     .find(|col| col.name == "rowid")
        //     .ok_or_else(|| {
        //         sqlite3_ext::Error::Sqlite(SQLITE_ERROR, Some("Rowid column not found".into()))
        //     })?;
        //
        // match rowid_column.value {
        //     Value::Integer(id) => Ok(id),
        //     _ => Err(sqlite3_ext::Error::Sqlite(
        //         SQLITE_ERROR,
        //         Some("Rowid is not an integer".into()),
        //     )),
        // }
    }
}
