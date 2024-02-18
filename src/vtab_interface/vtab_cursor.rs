use std::ops::{Add, Bound, Index};

use super::{PartitionMetaTable, WhereClauses};
use crate::utils::{aggregate_conditions_to_ranges, Condition};
use crate::{ColumnDeclaration, Lookup, PartitionAccessor};
use sqlite3_ext::query::{Column, QueryResult, Statement};
use sqlite3_ext::{query::ToParam, vtab::ColumnContext};
use sqlite3_ext::{vtab::VTabCursor, Value, ValueRef};
use sqlite3_ext::{FallibleIterator, FallibleIteratorMut, FromValue, Result as ExtResult};

#[derive(Debug)]
pub struct RangePartitionCursor<'cursor> {
    rowid: i64,
    // partition_tables: Vec<(i64, String)>,
    // rows: Vec<Vec<(String, Value)>>,
    meta_table: &'cursor PartitionMetaTable<'cursor>,
    rows: Vec<Vec<(String, String, Value)>>,
}
#[derive(Debug)]
pub struct PartitionRow {
    table_name: String,
    column_declarations: Vec<Vec<(String, String, Value)>>,
}
impl<'cursor> RangePartitionCursor<'cursor> {
    pub fn new(meta_table: &'cursor PartitionMetaTable) -> Self {
        Self {
            rowid: i64::default(),
            // partition_tables: Vec::new(),
            rows: Vec::new(),
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

        let partitions = self
            .meta_table
            .partition_interface
            .get_lookup()
            .get_partitions_by_range(self.meta_table.connection, *lower_bound, *upper_bound)?;

        let mut partition_where_str: String = String::default();
        if let Some(vec) = partition_where {
            partition_where_str = format!(
                "WHERE {}",
                vec.iter()
                    .map(|clause| clause.to_string())
                    .collect::<Vec<String>>()
                    .join(" AND ")
            );
        }
        // self.rows.clear();
        let queries: Vec<String> = partitions
            .iter()
            .map(|(_partition_value, partition_name)| {
                format!("SELECT * FROM {} {}", partition_name, partition_where_str)
                // let mut stmt: ExtResult<Statement> = self.meta_table.connection.prepare(&sql);
                // for (index, arg) in args.iter_mut().enumerate() {
                //     arg.bind_param(&mut stmt?, (index + 1) as i32);
                // }
                // println!("SQL: {}", sql);

                // let mut rows: Vec<Vec<&Column>> = Vec::new();
                //Ok(())
                // Ok(&stmt?)

                // .into_iter().filter_map(|row| {
                //                 let column_count = row.column_count();
                //                 let mut columns: Vec<&Column> = Vec::new();
                //                 for i in 0..column_count {
                //                     let column = row[i];
                //                     columns.push(column);
                //                 }
                //                 self.rows.push(columns);
                //             });
                //
                // self.meta_table.rows = rows;
                // for row in rows {
                //     self.rows.push(row);
                // }
            })
            .collect::<Vec<String>>();

        // let kk = queries.iter().map(|query| {
        //     let a = args
        //         .iter()
        //         .map(|&arg| arg.clone())
        //         .collect::<Vec<&ValueRef>>();
        //     let mut stmt = self.meta_table.connection.prepare(&query)?;
        //     stmt.query(a)?
        //         .map(|row| {
        //             let column_count = row.len();
        //             let mut partition_row = Vec::new();
        //             for index in 0..column_count {
        //                 let column = row.index(index);
        //                 let column_name = column.name()?.to_string();
        //
        //                 println!("COLUMN:  {:#?}", row[index]);
        //                 let source_table = column.table_name()?.unwrap().to_string();
        //
        //                 println!("COLUMN:  {:#?}", row[index]);
        //                 let column_value = column.to_owned()?;
        //
        //                 println!("COLUMN:  {:#?}", row[index]);
        //                 println!("{:#?}", column);
        //                 partition_row.push((source_table, column_name, column_value));
        //             }
        //             Ok(partition_row)
        //         })
        //         .collect::<Vec<Vec<(String, String, Value)>>>()
        // });
        let partition_count = queries.len();
        let sql = queries.join(" UNION ALL ");
        let mut stmt: Statement = self.meta_table.connection.prepare(&sql)?;
        for partition_index in 0..partition_count {
            for (arg_index, arg) in args.iter_mut().enumerate() {
                arg.bind_param(&mut stmt, (partition_index + arg_index + 1) as i32)?;
            }
        }
        let result = stmt.query(())?;
        self.rows = result
            .map(|row| {
                let column_count = row.len();
                let mut partition_row = Vec::new();
                for index in 0..column_count {
                    let column = row.index(index);
                    let column_name = column.name()?.to_string();

                    println!("COLUMN:  {:#?}", row[index]);
                    let source_table = column.table_name()?.unwrap().to_string();

                    println!("COLUMN:  {:#?}", row[index]);
                    let column_value = column.to_owned()?;

                    println!("COLUMN:  {:#?}", row[index]);
                    println!("{:#?}", column);
                    partition_row.push((source_table, column_name, column_value));
                }
                Ok(partition_row)
            })
            .collect::<Vec<Vec<(String, String, Value)>>>()?
            .clone();
        // self.rows = c;
        println!("{:#?}", stmt.query(())?);
        // let args_count = args.len();
        // // let args_copy: Vec<String> = args
        // //       .iter()
        // //       .take(union_query.len())
        // //       .map(|&args| args.to_owned()) // Use `?` to propagate the error
        // //       .collect()?; // Collecting Results into Vec<String>, errors are handled by `?`
        // //
        // // Ok(args_copy)
        // let args_copy: Vec<ExtResult<Value>> = args
        //     .iter()
        //     .take(union_query.len())
        //     .map::<ExtResult<Value>, _>(|&args| args.to_owned())
        //     .collect::<Vec<ExtResult<Value>>>();
        // for q in union_query {}
        // let stmt = self
        //     .meta_table
        //     .connection
        //     .prepare(&union_query.join(" UNION "))?;
        // stmt.query(args_copy);
        //
        self.rowid = 0;
        Ok(())
    }

    fn next(&mut self) -> ExtResult<()> {
        self.rowid += 1;
        Ok(())
    }

    fn eof(&self) -> bool {
        self.rowid().unwrap() >= self.rows.len() as i64
    }

    fn column(&self, idx: usize, c: &ColumnContext) -> ExtResult<()> {
        let row = self.rows.get(self.rowid()? as usize).unwrap();
        let column = match row.get(idx) {
            Some(col) => Ok(col),
            None => Err(sqlite3_ext::Error::Sqlite(
                1,
                Some(format!("Error parsing column ").to_string()),
            )),
        }?;
        c.set_result(column.2.clone())?;

        Ok(())
    }

    fn rowid(&self) -> ExtResult<i64> {
        Ok(self.rowid)
    }
}
