use sqlite3_ext::ValueRef;

use crate::error::TableError;

pub use self::{
    conditions::{Condition, Conditions},
    where_clauses::{WhereClause, WhereClauses},
};

mod conditions;
mod where_clauses;

/// Converts a reference to a `WhereClause` and a mutable reference to a `ValueRef`
/// into a `Condition`. This allows for the creation of a query condition directly
/// from a high-level constraint specification and its corresponding value.
///
/// Parameters:
/// - `value`: A tuple containing a reference to the `WhereClause` and a mutable reference
///   to the `ValueRef` that holds the value to be used in the condition.
///
/// Returns:
/// - A `Condition` instance representing the specified where clause and value.
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

impl<'a> TryFrom<(&'a WhereClause, &'a [&'a mut ValueRef])> for Condition<'a> {
    type Error = TableError;
    fn try_from(value: (&'a WhereClause, &'a [&'a mut ValueRef])) -> Result<Self, Self::Error> {
        let (where_clause, args) = value;
        args.get(where_clause.get_constraint_index() as usize)
            .map_or_else(
                || {
                    Err(TableError::WhereClause(
                        "Argument not found for constraint index {}".to_owned(),
                    ))
                },
                |value| Ok(Condition::from((where_clause, value))),
            )
    }
}

/// Attempts to convert a tuple containing a vector of `WhereClause` instances and a slice of
/// mutable `ValueRef` references into a `Conditions` collection. This transformation is critical
/// for assembling multiple conditions into a coherent query based on dynamic inputs.
///
/// Parameters:
/// - `value`: A tuple comprising a reference to a vector of `WhereClause` instances and a slice
///   of mutable references to `ValueRef`, each corresponding to a value in the where clauses.
///
/// Returns:
/// - On success, a `Conditions` collection encapsulating the constructed conditions.
/// - On failure, a `TableError` indicating issues encountered during conversion, such as missing
///   arguments for specified constraint indices.
///
/// This conversion is essential for creating complex, dynamic queries by mapping high-level
/// constraint specifications to actual query conditions with associated values.
impl<'a> TryFrom<(&'a Vec<WhereClause>, &'a [&'a mut ValueRef])> for Conditions<'a> {
    type Error = TableError;
    fn try_from(
        value: (&'a Vec<WhereClause>, &'a [&'a mut ValueRef]),
    ) -> Result<Self, Self::Error> {
        let (where_clauses, args) = value;
        where_clauses
            .iter()
            .map(|where_clause| Condition::try_from((where_clause, args)))
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
