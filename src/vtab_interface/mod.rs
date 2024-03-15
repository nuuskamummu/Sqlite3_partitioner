pub mod operations;
mod vtab_cursor;
mod vtab_module;

use crate::constraints::{WhereClause, WhereClauses};
use crate::{shadow_tables::interface::VirtualTable, vtab_interface::vtab_module::*};
use operations::create::*;
use sqlite3_ext::{
    ffi::SQLITE_NOTFOUND,
    sqlite3_ext_main,
    vtab::{ChangeInfo, IndexInfoConstraint},
    Connection, Result as ExtResult,
};

use std::{collections::HashMap, sync::RwLock};

use crate::utils::parse_partition_value;

#[sqlite3_ext_main]
fn init(db: &Connection) -> ExtResult<()> {
    db.create_module(
        "Partitioner",
        PartitionMetaTable::module(),
        RwLock::default(),
    )?;
    Ok(())
}

//Could this work? Are the iterators always of the same length?
// impl<'a> TryFrom<(&'a Vec<WhereClause>, &'a [&'a mut ValueRef])> for Conditions<'a> {
//     type Error = TableError;
//     fn try_from(
//         value: (&'a Vec<WhereClause>, &'a [&'a mut ValueRef]),
//     ) -> Result<Self, Self::Error> {
//         let (constraints, args) = value;
//         constraints
//             .iter()
//             .zip(args.iter())
//             .map(|(where_clause, arg)| Ok(Condition::from((where_clause, arg))))
//             .collect()
//     }
// }
fn construct_where_clause(
    index_info: &sqlite3_ext::vtab::IndexInfo,
    virtual_table: &VirtualTable,
) -> ExtResult<WhereClauses> {
    let mut column_name_map: HashMap<String, Vec<(IndexInfoConstraint, i32)>> = HashMap::new();
    for (index, constraint) in index_info
        .constraints()
        .enumerate()
        .filter(|(_index, c)| c.usable())
    {
        let column_name = virtual_table.columns().0[constraint.column() as usize]
            .get_name()
            .to_owned();
        column_name_map
            .entry(column_name)
            .or_default()
            .push((constraint, index as i32));
    }

    let where_clauses = column_name_map
        .iter()
        .map(|(column_name, constraints)| {
            let clauses = constraints
                .iter()
                .map(|(constraint, index)| {
                    WhereClause::new(column_name.to_owned(), constraint.op(), *index)
                })
                .collect::<Vec<WhereClause>>();
            (
                virtual_table
                    .lookup()
                    .partition_table_column()
                    .get_name()
                    .to_owned(),
                clauses,
            )
        })
        .collect();
    Ok(where_clauses)
}
