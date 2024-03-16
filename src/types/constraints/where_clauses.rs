use crate::ConstraintOpDef;
use serde::{Deserialize, Serialize};
use sqlite3_ext::vtab::ConstraintOp;
use std::{
    collections::HashMap,
    fmt::Display,
    ops::{Deref, DerefMut},
};

/// Represents a single condition within a SQL WHERE clause, including the column name,
/// comparison operator, and the index of the constraint within the query. This structure
/// is used for building complex query conditions dynamically.
#[derive(Serialize, Deserialize, Debug)]
pub struct WhereClause {
    /// The name of the column to which the condition applies.
    column_name: String,
    #[serde(with = "ConstraintOpDef")]
    /// The comparison operator used in the condition.
    operator: ConstraintOp,
    // #[serde(with = "ValueDef")]
    // right_hand_value: Option<Value>,
    /// The index of the constraint in the query, used for parameter binding. Set in the best_index
    /// function
    constraint_index: i32,
}
impl WhereClause {
    /// Retrieves the column name associated with the where clause.
    pub(crate) fn get_name(&self) -> &str {
        &self.column_name
    }

    /// Retrieves the comparison operator used in the where clause.
    pub(crate) fn get_operator(&self) -> &ConstraintOp {
        &self.operator
    }

    /// Retrieves the index of the constraint within the query.
    pub(crate) fn get_constraint_index(&self) -> i32 {
        self.constraint_index
    }

    /// Constructs a new `WhereClause` with the specified column name, operator, and constraint index.
    pub(crate) fn new(column_name: String, operator: ConstraintOp, constraint_index: i32) -> Self {
        Self {
            column_name,
            constraint_index,
            operator,
        }
    }
}
/// A collection of `WhereClause` instances, organized by their associated column name.
/// This structure allows for the aggregation and management of multiple conditions applied
/// to various columns within a query.
#[derive(Serialize, Deserialize, Debug)]
pub struct WhereClauses(pub HashMap<String, Vec<WhereClause>>);
impl Deref for WhereClauses {
    /// Provides immutable access to the underlying `HashMap` of where clauses.
    type Target = HashMap<String, Vec<WhereClause>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for WhereClauses {
    /// Provides mutable access to the underlying `HashMap` of where clauses.
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
impl FromIterator<(String, Vec<WhereClause>)> for WhereClauses {
    /// Creates a `WhereClauses` instance from an iterator of tuples, where each tuple contains
    /// a column name and a vector of `WhereClause` instances associated with that column.
    fn from_iter<T: IntoIterator<Item = (String, Vec<WhereClause>)>>(iter: T) -> Self {
        let mut data: HashMap<String, Vec<WhereClause>> = HashMap::new();

        for (key, clauses) in iter {
            data.entry(key).or_default().extend(clauses);
        }

        WhereClauses(data)
    }
}
impl Display for WhereClause {
    /// Formats a `WhereClause` for display, showing the column name, operator, and a placeholder
    /// for the value, which is represented by a "?" in prepared SQL statements.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {} ?",
            self.column_name,
            ConstraintOpDef::from(self.operator),
        )
    }
}
