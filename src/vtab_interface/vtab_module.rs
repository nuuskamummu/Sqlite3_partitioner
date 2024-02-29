use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::sync::RwLock;

use crate::operations::delete::update;
use crate::vtab_interface::vtab_cursor::*;
use crate::{
    operations::{delete::delete, insert::insert},
    vtab_interface::WhereClause,
};
use crate::{Lookup, Partition, PartitionAccessor, Root, Template};
use sqlite3_ext::query::ToParam;
use sqlite3_ext::{sqlite3_ext_vtab, vtab::VTab};
use sqlite3_ext::{
    vtab::{ChangeInfo, ChangeType, CreateVTab, UpdateVTab, VTabConnection},
    Connection, Result as ExtResult,
};
use sqlite3_ext::{FromValue, Value};

use super::{construct_where_clause, create_partition};

#[derive(Debug)]
#[sqlite3_ext_vtab(StandardModule, UpdateVTab)]
pub struct PartitionMetaTable<'vtab> {
    pub partition_interface: Partition<i64>,
    pub connection: &'vtab Connection,
    pub rowid_mapper: &'vtab RwLock<HashMap<i64, (Value, String)>>,
}
impl<'vtab> CreateVTab<'vtab> for PartitionMetaTable<'vtab> {
    fn create(
        db: &'vtab VTabConnection,
        rowid_mapper: &'vtab Self::Aux,
        args: &[&str],
    ) -> ExtResult<(String, Self)>
    where
        Self: Sized,
    {
        let p = create_partition(db, args, true)?;
        let sql = p.get_template().create_table_query();
        Ok((
            sql.to_owned(),
            PartitionMetaTable {
                partition_interface: p,
                connection: db,
                rowid_mapper,
            },
        ))
    }
    fn destroy(&mut self) -> ExtResult<()> {
        for partition in self
            .partition_interface
            .get_lookup()
            .get_partitions_by_range(
                self.connection,
                std::ops::Bound::Unbounded,
                std::ops::Bound::Unbounded,
            )?
        {
            self.connection
                .execute(&format!("DROP TABLE {}", partition.1), ())?;
        }

        self.connection
            .execute(&self.partition_interface.get_root().drop_table_query(), ())?;
        self.connection.execute(
            &self.partition_interface.get_lookup().drop_table_query(),
            (),
        )?;
        self.connection.execute(
            &self.partition_interface.get_template().drop_table_query(),
            (),
        )?;

        Ok(())
    }
}
impl<'vtab> UpdateVTab<'vtab> for PartitionMetaTable<'vtab> {
    fn update(&'vtab self, info: &mut ChangeInfo) -> ExtResult<i64> {
        let (sql, params) = match info.change_type() {
            ChangeType::Insert => insert(&self.partition_interface, self.connection, info)?,
            ChangeType::Update => {
                let rowid_mapper = self.rowid_mapper.read().map_err(|e| {
                    sqlite3_ext::Error::Sqlite(1, Some(format!("Lock acquisition failed: {}", e)))
                })?;
                let id = info.rowid_mut().get_i64();
                if let Some((db_rowid, partition_name)) = rowid_mapper.get(&id) {
                    let (sql, mut values) =
                        update(partition_name, &self.partition_interface, info.args_mut());
                    let mut stmt = self.connection.prepare(&sql)?;
                    values.iter_mut().enumerate().for_each(|(index, value)| {
                        value
                            .bind_param(stmt.borrow_mut(), (index + 1) as i32)
                            .unwrap();
                    });

                    db_rowid
                        .clone()
                        .bind_param(stmt.borrow_mut(), (values.len() + 1) as i32)?;
                    stmt.execute(())?;
                }

                return Ok(id);
            }
            ChangeType::Delete => {
                let rowid_mapper = self.rowid_mapper.write().map_err(|e| {
                    sqlite3_ext::Error::Sqlite(1, Some(format!("Lock acquisition failed: {}", e)))
                })?;
                let id = info.rowid().get_i64();
                if let Some((db_rowid, partition_name)) = rowid_mapper.get(&id) {
                    let sql = delete(partition_name);
                    let mut stmt = self.connection.prepare(&sql)?;
                    db_rowid.clone().bind_param(stmt.borrow_mut(), 1)?;
                    stmt.execute(())?;
                }

                return Ok(id);
            }
        };

        self.connection.execute(&sql, params)
    }
}
impl<'vtab> VTab<'vtab> for PartitionMetaTable<'vtab> {
    type Aux = RwLock<HashMap<i64, (Value, String)>>; //internal rowid. rowid from table, table name
    type Cursor = RangePartitionCursor<'vtab>;

    fn connect(
        db: &'vtab VTabConnection,
        rowid_mapper: &'vtab Self::Aux,

        args: &[&str],
    ) -> ExtResult<(String, Self)>
    where
        Self: Sized,
    {
        let p = create_partition(db, args, false)?;
        let connection = db;

        Ok((
            p.get_template().create_table_query().to_owned(),
            PartitionMetaTable {
                partition_interface: p,
                connection,
                rowid_mapper, // rows: None,
            },
        ))
    }
    fn open(&'vtab self) -> ExtResult<Self::Cursor> {
        Ok(RangePartitionCursor::new(self))
    }

    fn best_index(&self, index_info: &mut sqlite3_ext::vtab::IndexInfo) -> ExtResult<()> {
        let mut argv_index = 0;
        for mut constraint in index_info.constraints() {
            if constraint.usable() {
                constraint.set_argv_index(Some(argv_index));
                argv_index += 1;
            }
        }
        index_info.set_estimated_cost(1.0); // Set a default cost, could be refined.
        let mut where_clauses = construct_where_clause(index_info, &self.partition_interface)?;
        let partition_column = where_clauses.get_key_value("partition_table");
        let lookup_where_clause = match partition_column {
            Some((_name, constraints)) => constraints
                .iter()
                .map(|constraint| {
                    let wherec = WhereClause {
                        column_name: "partition_value".to_string(),
                        operator: constraint.operator,
                        constraint_index: constraint.constraint_index,
                    };
                    Some(wherec)
                })
                .collect::<Option<Vec<WhereClause>>>(),
            None => None,
        };

        lookup_where_clause
            .and_then(|clause| where_clauses.insert("lookup_table".to_string(), clause));
        index_info.set_index_str(Some(&ron::to_string(&where_clauses).unwrap()))?;

        Ok(())
    }
    fn disconnect(&mut self) -> ExtResult<()> {
        let mut rowid_mapper = self.rowid_mapper.write().map_err(|e| {
            sqlite3_ext::Error::Sqlite(1, Some(format!("Lock acquisition failed: {}", e)))
        })?;
        rowid_mapper.clear();
        Ok(())
    }
}
