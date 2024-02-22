use std::ops::{Bound, Index};
use std::usize;

use super::{PartitionMetaTable, WhereClauses};
use crate::utils::{aggregate_conditions_to_ranges, Condition};
use crate::{Lookup, PartitionAccessor, Root};
use rusqlite::ffi::SQLITE_ERROR;
use sqlite3_ext::query::Column;
use sqlite3_ext::vtab::ColumnContext;
use sqlite3_ext::{vtab::VTabCursor, Value, ValueRef};
use sqlite3_ext::{FallibleIteratorMut, FromValue, Result as ExtResult};

#[derive(Debug)]
pub struct RangePartitionCursor<'vtab> {
    internal_rowid_counter: i64,
    meta_table: &'vtab PartitionMetaTable<'vtab>,
    buckets: Vec<ResultBucket>,
    current_bucket_index: usize, // current_bucket: Option<&'vtab ResultBucket<'vtab>>,
}

#[derive(Debug)]
pub struct ResultBucket {
    pub partition_value: i64, //index in lookup_table.partitions.
    pub partition_name: String,
    pub rows: Vec<ResultRow>,
    current_row_index: usize,
}
impl ResultBucket {
    fn new(partition_value: i64, partition_name: String, rows: Vec<ResultRow>) -> Self {
        if rows.is_empty() {
            panic!()
        }

        Self {
            partition_value,
            partition_name,
            rows,
            current_row_index: usize::default(),
        }
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

#[derive(Debug)]
pub struct ResultRow {
    rowid: Value,
    columns: Vec<ResultColumn>,
}

impl FromIterator<ResultColumn> for ResultRow {
    fn from_iter<T: IntoIterator<Item = ResultColumn>>(iter: T) -> Self {
        let columns: Vec<ResultColumn> = iter.into_iter().collect();
        let (rowid, c) = {
            let b = columns.split_at(1);
            (b.0[0].value.clone(), b.1)
        };
        Self {
            rowid,
            columns: c.to_vec(),
        }
    }
}

#[derive(Debug, Clone)]
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
impl<'vtab> RangePartitionCursor<'vtab> {
    pub fn new(meta_table: &'vtab PartitionMetaTable) -> Self {
        Self {
            buckets: Vec::new(),
            meta_table,
            internal_rowid_counter: i64::default(),
            current_bucket_index: usize::default(), // current_bucket: None,
        }
    }

    fn get_mut_current_bucket(&mut self) -> Option<&mut ResultBucket> {
        self.buckets.get_mut(self.current_bucket_index)
    }
    fn get_current_bucket(&self) -> Option<&ResultBucket> {
        self.buckets.get(self.current_bucket_index)
    }
    fn get_current_row(&self) -> Option<&ResultRow> {
        self.get_current_bucket()
            .and_then(|bucket| bucket.get_current_row())
    }
    fn advance_to_next_bucket(&mut self) -> Option<&mut ResultBucket> {
        self.buckets
            .get_mut(self.current_bucket_index + 1)
            .map(|bucket| {
                self.current_bucket_index += 1;
                bucket
            })
    }
    fn advance_to_next_row(&mut self) -> Option<&mut ResultRow> {
        self.get_mut_current_bucket()
            .and_then(|bucket| bucket.advance_to_next_row())
    }
    fn parse_where_clauses(
        &self,
        idx_str: Option<&str>,
        args: &mut [&mut ValueRef],
    ) -> (String, Vec<Condition>) {
        let where_clauses_serialized = idx_str.unwrap_or("");
        let where_clauses: WhereClauses = ron::from_str(where_clauses_serialized).unwrap();

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

        let buckets: ExtResult<Vec<ResultBucket>> = self
            .meta_table
            .partition_interface
            .get_lookup()
            .get_partitions_by_range(self.meta_table.connection, *lower_bound, *upper_bound)?
            .iter()
            .try_fold(Vec::new(), |mut acc, (partition_value, partition_name)| {
                let sql = format!(
                    "SELECT rowid, * FROM {} {}",
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
                        partition_name.clone(),
                        row_columns,
                    ));
                }
                Ok(acc)
            });
        self.buckets = buckets?;
        Ok(())
    }
}

impl<'vtab> VTabCursor<'vtab> for RangePartitionCursor<'vtab> {
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

    fn next(&mut self) -> ExtResult<()> {
        let mut next_row = self.advance_to_next_row();
        if next_row.is_none() {
            next_row = self
                .advance_to_next_bucket()
                .and_then(|bucket| bucket.get_mut_current_row());
        };

        if next_row.is_some() {
            self.internal_rowid_counter += 1;
        }
        Ok(())
    }

    fn eof(&self) -> bool {
        self.get_current_row().is_none()
    }
    fn column(&self, idx: usize, c: &ColumnContext) -> ExtResult<()> {
        let column_value = self
            .get_current_row()
            .map(|row| &row.columns)
            .and_then(|columns| columns.get(idx))
            .map(|column| column.value.clone());
        if let Some(value) = column_value {
            println!("value : {:#?}", value);
            c.set_result(value)?;
        }
        //
        Ok(())
    }

    fn rowid(&self) -> ExtResult<i64> {
        let rowid_column = self.get_current_row().map(|row| row.rowid.clone());

        let partition_name = &self.get_current_bucket().unwrap().partition_name;
        if let Some(column) = rowid_column {
            self.meta_table
                .rowid_mapper
                .write()
                .unwrap()
                .push((column.clone(), partition_name.to_string()));
        }

        Ok(self.internal_rowid_counter)
    }
}
