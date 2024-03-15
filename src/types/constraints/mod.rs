use sqlite3_ext::ValueRef;

use crate::error::TableError;

pub use self::{
    conditions::{Condition, Conditions},
    where_clauses::{WhereClause, WhereClauses},
};

mod conditions;
mod where_clauses;

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
        let (where_clauses, args) = value;
        where_clauses
            .iter()
            .map(|where_clause| {
                args.get(where_clause.get_constraint_index() as usize)
                    .map_or_else(
                        || {
                            Err(TableError::WhereClause(
                                "Argument not found for constraint index {}".to_owned(),
                            ))
                        },
                        |value| Ok(Condition::from((where_clause, value))),
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
