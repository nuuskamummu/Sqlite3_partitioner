use std::ops::{Add, Bound, Index};
use std::sync::RwLock;
use std::usize;

use super::{PartitionMetaTable, WhereClauses};
use crate::utils::{aggregate_conditions_to_ranges, Condition};
use crate::{Lookup, PartitionAccessor};
use sqlite3_ext::ffi::SQLITE_ERROR;
use sqlite3_ext::query::{Column, Statement};
use sqlite3_ext::vtab::ColumnContext;
use sqlite3_ext::{vtab::VTabCursor, Value, ValueRef};
use sqlite3_ext::{FallibleIteratorMut, FromValue, Result as ExtResult};

#[derive(Debug)]
pub struct RangePartitionCursor<'cursor> {
    rowid: i64,
    result_iterator_counter: i64,
    // rows: Vec<Vec<(String, Value)>>,
    meta_table: &'cursor PartitionMetaTable<'cursor>,
    buckets: Vec<ResultBucket>,
}

#[derive(Debug)]
pub struct ResultBucket {
    partition_index: i64, //index in lookup_table.partitions.
    rows: Vec<ResultRow>,
}
impl ResultBucket {
    fn new(partition_index: i64, rows: Vec<ResultRow>) -> Self {
        Self {
            partition_index,
            rows,
        }
    }
}

#[derive(Debug)]
pub struct ResultRow {
    table_name: String,
    columns: Vec<ResultColumn>,
}
impl FromIterator<ResultColumn> for ResultRow {
    fn from_iter<T: IntoIterator<Item = ResultColumn>>(iter: T) -> Self {
        let columns: Vec<ResultColumn> = iter.into_iter().collect();
        let table_name = columns[0].table_name.to_owned();
        Self {
            table_name: table_name.to_string(),
            columns,
        }
    }
}

impl ResultRow {
    fn new(table: String, columns: Vec<ResultColumn>) -> Self {
        Self {
            table_name: table,
            columns,
        }
    }
}

#[derive(Debug)]
pub struct ResultColumn {
    name: String,
    value: Value,
    table_name: String,
}
impl ResultColumn {
    fn new(
        column: &Column,
        table_name: String, /*Column::table_name will cause segfault!*/
    ) -> ExtResult<Self> {
        let name = column.name()?.to_owned();
        let value = column.to_owned()?;
        Ok(Self {
            name,
            value,
            table_name,
        })
    }
}
impl<'cursor> RangePartitionCursor<'cursor> {
    pub fn new(meta_table: &'cursor PartitionMetaTable) -> Self {
        Self {
            result_iterator_counter: i64::default(),
            rowid: i64::default(),
            buckets: Vec::new(),
            meta_table,
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
        self.result_iterator_counter = 0;
        let where_clauses_serialized = idx_str.unwrap_or("");
        let where_clauses: &WhereClauses = &ron::from_str(where_clauses_serialized).unwrap();
        let lookup_where = where_clauses.get("lookup_table");
        let partition_where = where_clauses.get("partition_table");

        let lookup_conditions = match lookup_where {
            Some(constraints) => constraints
                .iter()
                .map(|constraint| {
                    let value = match args[constraint.constraint_index as usize].to_owned() {
                        Ok(value) => value,
                        Err(_err) => return None,
                    };

                    Some(Condition {
                        column: constraint.get_name(),
                        operator: constraint.operator,
                        value,
                    })
                })
                .flatten()
                .collect::<Vec<Condition>>(),
            None => Vec::new(),
        };

        let ranges = aggregate_conditions_to_ranges(lookup_conditions);
        let (lower_bound, upper_bound) = match ranges.get("partition_value") {
            Some(bounds) => &bounds,
            None => &(
                Bound::Unbounded as Bound<i64>,
                Bound::Unbounded as Bound<i64>,
            ),
        };
        let mut partition_where_str: String = String::default();
        if let Some(vec) = partition_where {
            partition_where_str = format!(
                "WHERE {}",
                vec.iter()
                    .map(|clause| clause.to_string())
                    .collect::<Vec<String>>()
                    .join(" AND ")
            );
        };
        let partitions = self
            .meta_table
            .partition_interface
            .get_lookup()
            .get_partitions_by_range(self.meta_table.connection, *lower_bound, *upper_bound)?;

        // self.rows.clear();
        let queries: Vec<ResultBucket> = partitions
            .iter()
            .map(|(partition_value, partition_name)| {
                let sql = format!(
                    "SELECT *, rowid FROM {} {}",
                    partition_name, partition_where_str
                );
                let mut stmt: Statement = self.meta_table.connection.prepare(&sql).unwrap();
                let result_rows = stmt.query(args.as_mut()).unwrap();
                let mut row_columns: Vec<ResultRow> = Vec::new();
                while let Ok(Some(row)) = result_rows.next() {
                    let column_count = row.len();
                    let columns = (0..column_count)
                        .map(|index| row.index(index))
                        .filter_map(|column| {
                            ResultColumn::new(column, partition_name.to_string()).ok()
                        })
                        .collect::<ResultRow>();
                    row_columns.push(columns);
                }
                let r = ResultBucket::new(*partition_value, row_columns);
                r
            })
            // .flatten()
            .collect();
        self.meta_table
            .partition_interface
            .get_lookup()
            .update_current_entry(queries[0].partition_index);
        self.buckets = queries;

        Ok(())
    }

    fn next(&mut self) -> ExtResult<()> {
        println!("NEXT");
        //nomenclature: partition = reference to a partition table. (i64, String) =
        //(partition_value, partition_name)
        //
        //bucket = rows fetched from a partition
        let current_partition = match self
            .meta_table
            .partition_interface
            .get_lookup()
            .access_current_entry(|(key, value)| (*key, value.clone()))
        {
            Some((key, value)) => (key, value),
            None => return Ok(()),
        };
        println!("current partition: {:#?}", current_partition);
        let (current_bucket_index, current_bucket) = match self
            .buckets
            .iter()
            .enumerate()
            .find(|(_index, row)| row.partition_index == current_partition.0)
        {
            Some((index, bucket)) => (index, bucket),
            None => return Ok(()),
        };
        let current_bucket_rows = &current_bucket.rows;
        match current_bucket_rows
            .get((self.result_iterator_counter as usize + 1)..)
            .into_iter()
            .peekable()
            .peek()
        {
            Some(_next_row) => {
                self.result_iterator_counter = self.result_iterator_counter + 1;
            }
            None => match self
                .buckets
                .get(current_bucket_index + 1)
                .into_iter()
                .peekable()
                .peek()
            {
                Some(bucket) => {
                    self.meta_table
                        .partition_interface
                        .get_lookup()
                        .update_current_entry(bucket.partition_index);
                    self.result_iterator_counter = 0;
                }
                None => self.result_iterator_counter = -1,
            },
        }

        Ok(())
    }

    fn eof(&self) -> bool {
        let eof = self.result_iterator_counter == -1;
        eof
    }

    fn column(&self, idx: usize, c: &ColumnContext) -> ExtResult<()> {
        let current_partition = match self
            .meta_table
            .partition_interface
            .get_lookup()
            .access_current_entry(|(key, value)| (key.clone(), value.clone()))
        {
            Some((key, value)) => (key, value),
            None => return Ok(()),
        };

        let current_bucket = match self
            .buckets
            .iter()
            .find(|row| row.partition_index == current_partition.0)
        {
            Some(bucket) => bucket,
            None => return Ok(()),
        };
        let current_row = match current_bucket
            .rows
            .get(self.result_iterator_counter as usize)
        {
            Some(row) => row,
            None => return Ok(()),
        };
        let column = match current_row.columns.get(idx) {
            Some(column) => column,
            None => return Ok(()),
        };
        c.set_result(column.value.clone())?;

        Ok(())
    }

    fn rowid(&self) -> ExtResult<i64> {
        let def_err = Err(sqlite3_ext::Error::Sqlite(
            SQLITE_ERROR,
            Some(format!("couldnt get rowid")),
        ));
        let current_partition = match self
            .meta_table
            .partition_interface
            .get_lookup()
            .access_current_entry(|(key, value)| (key.clone(), value.clone()))
        {
            Some((key, value)) => (key, value),
            None => return def_err,
        };

        let current_bucket = match self
            .buckets
            .iter()
            .find(|row| row.partition_index == current_partition.0)
        {
            Some(bucket) => bucket,
            None => return def_err,
        };
        let current_row = match current_bucket
            .rows
            .get(self.result_iterator_counter as usize)
        {
            Some(row) => row,
            None => return def_err,
        };
        let rowid_value = match current_row.columns.iter().find(|col| col.name == "rowid") {
            Some(column) => Ok(column.value.to_owned()),
            None => return def_err,
        };
        let rowid = match rowid_value {
            Ok(value) => match value {
                Value::Integer(id) => id,
                _ => return def_err,
            },
            Err(err) => return err,
        };
        Ok(rowid)
    }
}
