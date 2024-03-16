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

/// Initializes the database with the Partitioner module.
///
/// This function sets up the virtual table module "Partitioner" in the SQLite database
/// to manage partitioned tables. It leverages a global lock for thread safety.
///
/// Parameters:
/// - `db`: Reference to the active database connection.
///
/// Returns:
/// - `ExtResult<()>`: Ok if successful, or an error on failure.
#[sqlite3_ext_main]
fn init(db: &Connection) -> ExtResult<()> {
    db.create_module(
        "Partitioner",
        PartitionMetaTable::module(),
        RwLock::default(),
    )?;
    Ok(())
}

/// Constructs `WhereClauses` from the provided index information and virtual table.
///
/// This function parses the index information to generate SQL WHERE clauses that are
/// applicable for querying the virtual table, based on its column constraints and indexes.
///
/// Parameters:
/// - `index_info`: Index information provided by the SQLite VTAB method bestIndex.
/// - `virtual_table`: Reference to the `VirtualTable`.
///
/// Returns:
/// - A result containing `WhereClauses` if successful, or an error on failure.
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
