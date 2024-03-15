use std::{
    collections::HashMap,
    fmt::Display,
    ops::{Deref, DerefMut},
};

use serde::{Deserialize, Serialize};
use sqlite3_ext::vtab::ConstraintOp;

use crate::ConstraintOpDef;

#[derive(Serialize, Deserialize, Debug)]
pub struct WhereClause {
    column_name: String,
    #[serde(with = "ConstraintOpDef")]
    operator: ConstraintOp,
    // #[serde(with = "ValueDef")]
    // right_hand_value: Option<Value>,
    constraint_index: i32,
}
impl WhereClause {
    pub(crate) fn get_name(&self) -> &str {
        &self.column_name
    }
    pub(crate) fn get_operator(&self) -> &ConstraintOp {
        &self.operator
    }
    pub(crate) fn get_constraint_index(&self) -> i32 {
        self.constraint_index
    }
    pub(crate) fn new(column_name: String, operator: ConstraintOp, constraint_index: i32) -> Self {
        Self {
            column_name,
            constraint_index,
            operator,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct WhereClauses(pub HashMap<String, Vec<WhereClause>>);
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
