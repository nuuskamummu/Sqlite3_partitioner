pub mod operations;
mod vtab_cursor;
mod vtab_module;

use crate::{vtab_interface::vtab_module::*, Partition};
use operations::create::*;
use serde::{Deserialize, Serialize};
use sqlite3_ext::{
    ffi::SQLITE_NOTFOUND,
    sqlite3_ext_main,
    vtab::{ChangeInfo, ConstraintOp, IndexInfoConstraint},
    Connection, Result as ExtResult,
};
pub use vtab_cursor::ResultRow;

use std::{
    collections::HashMap,
    fmt::Display,
    ops::{Deref, DerefMut},
};

use crate::{
    utils::{calculate_bucket, resolve_partition_name, validate_and_map_columns},
    ConstraintOpDef, PartitionAccessor,
};

#[sqlite3_ext_main]
fn init(db: &Connection) -> ExtResult<()> {
    db.create_module("Partitioner", PartitionMetaTable::module(), ())?;
    Ok(())
}

#[derive(Serialize, Deserialize, Debug)]
struct WhereClause {
    column_name: String,
    #[serde(with = "ConstraintOpDef")]
    operator: ConstraintOp,
    // #[serde(with = "ValueDef")]
    // right_hand_value: Option<Value>,
    constraint_index: i32,
}
impl WhereClause {
    fn get_name(&self) -> String {
        self.column_name.clone()
    }
    fn _get_operator(&self) -> ConstraintOp {
        self.operator
    }
    fn _get_constraint_index(&self) -> i32 {
        self.constraint_index
    }
}
#[derive(Serialize, Deserialize, Debug)]
struct WhereClauses(HashMap<String, Vec<WhereClause>>);
impl Deref for WhereClauses {
    type Target = HashMap<String, Vec<WhereClause>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for WhereClauses {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
impl FromIterator<(String, Vec<WhereClause>)> for WhereClauses {
    fn from_iter<T: IntoIterator<Item = (String, Vec<WhereClause>)>>(iter: T) -> Self {
        let mut data: HashMap<String, Vec<WhereClause>> = HashMap::new();

        for (key, clauses) in iter {
            for clause in clauses {
                data.entry(key.to_string())
                    .or_insert_with(Vec::new)
                    .push(clause);
            }
        }

        WhereClauses(data)
    }
}
impl Display for WhereClause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {} {}",
            self.column_name,
            ConstraintOpDef::from(self.operator),
            "?"
        )
    }
}

fn construct_where_clause<T>(
    index_info: &sqlite3_ext::vtab::IndexInfo,
    partition: &Partition<T>,
) -> ExtResult<WhereClauses> {
    let mut column_name_map: HashMap<String, Vec<(IndexInfoConstraint, i32)>> = HashMap::new();
    for (index, constraint) in index_info
        .constraints()
        .enumerate()
        .filter(|(_index, c)| c.usable())
    {
        let column_name = partition.columns[constraint.column() as usize]
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
                .map(|(constraint, index)| WhereClause {
                    column_name: column_name.into(),
                    operator: constraint.op(),
                    constraint_index: *index,
                })
                .collect::<Vec<WhereClause>>();
            ("partition_table".to_string(), clauses)
        })
        .collect();
    Ok(where_clauses)
}
