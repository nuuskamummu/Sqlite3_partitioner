pub mod operations;
mod vtab_cursor;
mod vtab_module;

use crate::{
    error::TableError,
    shadow_tables::interface::VirtualTable,
    utils::{Condition, Conditions},
    vtab_interface::vtab_module::*,
};
use operations::create::*;
use serde::{Deserialize, Serialize};
use sqlite3_ext::{
    ffi::SQLITE_NOTFOUND,
    sqlite3_ext_main,
    vtab::{ChangeInfo, ConstraintOp, IndexInfoConstraint},
    Connection, Result as ExtResult, ValueRef,
};

use std::{
    collections::HashMap,
    fmt::Display,
    ops::{Deref, DerefMut},
    sync::RwLock,
};

use crate::{utils::parse_partition_value, ConstraintOpDef};

#[sqlite3_ext_main]
fn init(db: &Connection) -> ExtResult<()> {
    db.create_module(
        "Partitioner",
        PartitionMetaTable::module(),
        RwLock::default(),
    )?;
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
    fn get_name(&self) -> &str {
        &self.column_name
    }
    fn get_operator(&self) -> &ConstraintOp {
        &self.operator
    }
    fn _get_constraint_index(&self) -> i32 {
        self.constraint_index
    }
}

impl<'a> From<(&'a WhereClause, &'a &'a mut ValueRef)> for Condition<'a> {
    fn from(value: (&'a WhereClause, &'a &'a mut ValueRef)) -> Self {
        let (constraint, arg) = value;
        Self {
            column: constraint.get_name(),
            operator: constraint.get_operator(),
            value: arg,
        }
    }
}
impl<'a> TryFrom<(&'a Vec<WhereClause>, &'a [&'a mut ValueRef])> for Conditions<'a> {
    type Error = TableError;
    fn try_from(
        value: (&'a Vec<WhereClause>, &'a [&'a mut ValueRef]),
    ) -> Result<Self, Self::Error> {
        let (constraints, args) = value;
        constraints
            .iter()
            .map(|constraint| {
                args.get(constraint.constraint_index as usize).map_or_else(
                    || {
                        Err(TableError::WhereClause(
                            "Argument not found for constraint index {}".to_owned(),
                        ))
                    },
                    |value| Ok(Condition::from((constraint, value))),
                )
            })
            .collect()
    }
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
            data.entry(key).or_default().extend(clauses);
        }

        WhereClauses(data)
    }
}
impl Display for WhereClause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {} ?",
            self.column_name,
            ConstraintOpDef::from(self.operator),
        )
    }
}

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
                .map(|(constraint, index)| WhereClause {
                    column_name: column_name.to_owned(),
                    operator: constraint.op(),
                    constraint_index: *index,
                    // right_hand_value: constraint
                    //     .rhs()
                    //     .map_or_else(|err| None, |value| Some(value.to_owned()?)),
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
