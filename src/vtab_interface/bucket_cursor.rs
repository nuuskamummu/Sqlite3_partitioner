use std::collections::BTreeMap;
use std::ops::Index;
use std::sync::RwLock;
use std::usize;

use super::vtab_cursor::{ResultBucket, ResultColumn};

use crate::shadow_tables::Bucket;
use crate::ResultRow;

use sqlite3_ext::{Connection, FallibleIteratorMut, Result as ExtResult, SQLITE_NOTFOUND};
use sqlite3_ext::{Value, ValueRef};

#[derive(Debug)]
pub struct BucketCursor<'cursor> {
    // pub rows: &'cursor RwLock<BTreeMap<i64, &'cursor (String, Vec<ResultRow>)>>,
    pub current_row: Option<&'cursor ResultRow>,

    pub bucket_module: Bucket, // Other fields as needed
}
impl<'cursor> BucketCursor<'cursor> {
    pub fn new(
        bucket_module: Bucket,
        // result_rows: &'cursor RwLock<BTreeMap<i64, &'cursor (String, Vec<ResultRow>)>>,
    ) -> Self {
        Self {
            bucket_module,
            // rows: result_rows,
            current_row: None,
        }
    }
    pub fn filter(
        &mut self,
        where_clause: &str,
        args: &mut [&mut ValueRef],
        connection: &Connection,
    ) -> ExtResult<Option<ResultBucket>> {
        let mut stmt = connection.prepare(
            format!(
                "SELECT *, rowid FROM {} {}",
                self.bucket_module.get_name(),
                where_clause
            )
            .trim(),
        )?;
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
        if row_columns.len() > 0 {
            let rb = ResultBucket::new(
                self.bucket_module.get_name().to_string(),
                self.bucket_module.get_partition_value(),
                row_columns,
            );
            return Ok(Some(rb));
        }

        Ok(None)
    }
    // fn next(&mut self) -> ExtResult<()> {
    //     println!("NEXT???");
    //
    //     self.current_row = match self.current_row {
    //         Some(current_row) => {
    //             let current_index = self
    //                 .rows
    //                 .read()
    //                 .unwrap()
    //                 .get(&self.bucket_module.get_partition_value())
    //                 .unwrap()
    //                 .1
    //                 .iter()
    //                 .position(|row| std::ptr::eq(row, current_row))
    //                 .unwrap()
    //                 .clone();
    //             self.rows
    //                 .read()
    //                 .unwrap()
    //                 .get(&self.bucket_module.get_partition_value())
    //                 .unwrap()
    //                 .1
    //                 .get(current_index + 1)
    //         }
    //         None => self
    //             .rows
    //             .read()
    //             .unwrap()
    //             .get(&self.bucket_module.get_partition_value())
    //             .unwrap()
    //             .1
    //             .get(0),
    //     };
    //
    //     Ok(())
    // }
    fn eof(&self) -> bool {
        self.current_row.is_none()
    }
    fn rowid(&self) -> ExtResult<i64> {
        let id_column_value = match self.current_row {
            Some(row) => Ok(row.rowid_column()?.get_value()),
            None => Err(SQLITE_NOTFOUND),
        }?;
        let id = match id_column_value {
            Value::Integer(id) => Ok(id),
            _ => Err(SQLITE_NOTFOUND),
        }?;
        Ok(*id)
    }
    fn column(&self, idx: usize) -> ExtResult<Value> {
        let current_row = match self.current_row {
            Some(row) => Ok(row),
            None => Err(SQLITE_NOTFOUND),
        }?;
        let column = current_row
            .get_columns()
            .get(idx)
            .ok_or_else(|| SQLITE_NOTFOUND)?;
        Ok(column.get_value().to_owned())
    }
}
