use crate::vtab_interface::vtab_cursor::*;
use crate::{
    operations::{delete::delete, insert::insert},
    vtab_interface::WhereClause,
};
use crate::{Partition, PartitionAccessor, Root, Template};
use sqlite3_ext::{sqlite3_ext_vtab, vtab::VTab};
use sqlite3_ext::{
    vtab::{ChangeInfo, ChangeType, CreateVTab, UpdateVTab, VTabConnection},
    Connection, Result as ExtResult,
};

use super::{construct_where_clause, create_partition};

#[derive(Debug)]
#[sqlite3_ext_vtab(StandardModule, UpdateVTab)]
pub struct PartitionMetaTable<'vtab> {
    pub partition_interface: Partition<i64>,
    // rows: Option<&'vtab mut Vec<Vec<(String, Value)>>>,
    pub connection: &'vtab Connection,
}
impl<'vtab> CreateVTab<'vtab> for PartitionMetaTable<'vtab> {
    fn create(
        db: &'vtab VTabConnection,
        _aux: &'vtab Self::Aux,
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
            },
        ))
    }
    fn destroy(&mut self) -> ExtResult<()> {
        Ok(())
    }
}
impl<'vtab> UpdateVTab<'vtab> for PartitionMetaTable<'vtab> {
    fn update(&'vtab self, info: &mut ChangeInfo) -> ExtResult<i64> {
        let (sql, params) = match info.change_type() {
            ChangeType::Insert => insert(&self.partition_interface, &self.connection, info)?,
            ChangeType::Update => unimplemented!(),
            ChangeType::Delete => delete(&self.partition_interface, info)?,
        };

        Ok(self.connection.execute(&sql, params)?)
    }
}
impl<'vtab> VTab<'vtab> for PartitionMetaTable<'vtab> {
    type Aux = ();
    type Cursor = RangePartitionCursor<'vtab>;

    fn connect(
        db: &'vtab VTabConnection,
        _aux: &'vtab Self::Aux,

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
                // rows: None,
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
        Ok(())
    }
}
