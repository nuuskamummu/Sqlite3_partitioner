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

/// Sort direction for a scan ordered by the partition column.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

impl Display for SortDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SortDirection::Asc => write!(f, "ASC"),
            SortDirection::Desc => write!(f, "DESC"),
        }
    }
}

/// A scan driven by a companion instead of the generic per-partition WHERE
/// path. The companion receives the driver constraint value (e.g. the MATCH
/// vector) plus its hidden-column scan parameters, and yields rows itself.
#[derive(Serialize, Deserialize, Debug)]
pub struct CompanionScanPlan {
    /// Name of the companion driving the scan (`<base>_<name>` shadows).
    pub companion: String,
    /// The claimed driver constraint (column, op, argv index).
    pub driver: WhereClause,
    /// Hidden column name -> its constraint (argv index), in declaration order.
    pub params: Vec<(String, WhereClause)>,
    /// Hidden column whose ascending order the scan already produces, set when
    /// a single-term ORDER BY on it was consumed.
    pub order_by_hidden: Option<String>,
}

/// The serialized scan plan passed from `best_index` to `xFilter` via `index_str`:
/// the WHERE clauses to enforce plus, when SQLite's ORDER BY can be satisfied by the
/// natural partition ordering, the direction in which to scan partitions.
#[derive(Serialize, Deserialize, Debug)]
pub struct ScanPlan {
    pub where_clauses: WhereClauses,
    pub partition_order: Option<SortDirection>,
    /// Set when a companion claimed a constraint and drives the scan.
    pub companion_scan: Option<CompanionScanPlan>,
}

impl ScanPlan {
    pub fn new(
        where_clauses: WhereClauses,
        partition_order: Option<SortDirection>,
        companion_scan: Option<CompanionScanPlan>,
    ) -> Self {
        Self {
            where_clauses,
            partition_order,
            companion_scan,
        }
    }
}
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constraints::CompanionScanPlan;
    use std::collections::HashMap;

    #[test]
    fn test_scan_plan_with_companion_scan_ron_roundtrip() {
        let mut clauses = HashMap::new();
        clauses.insert(
            "partition_table".to_string(),
            vec![WhereClause::new(
                "col1".to_string(),
                ConstraintOp::GE,
                0,
            )],
        );
        let plan = ScanPlan::new(
            WhereClauses(clauses),
            Some(SortDirection::Asc),
            Some(CompanionScanPlan {
                companion: "vec".to_string(),
                driver: WhereClause::new("emb".to_string(), ConstraintOp::Match, 1),
                params: vec![(
                    "k".to_string(),
                    WhereClause::new("k".to_string(), ConstraintOp::Eq, 2),
                )],
                order_by_hidden: Some("distance".to_string()),
            }),
        );
        let serialized = ron::to_string(&plan).unwrap();
        let deserialized: ScanPlan = ron::from_str(&serialized).unwrap();
        assert_eq!(format!("{:?}", plan), format!("{:?}", deserialized));
    }

    #[test]
    fn test_scan_plan_without_companion_scan_ron_roundtrip() {
        let plan = ScanPlan::new(WhereClauses(HashMap::new()), None, None);
        let serialized = ron::to_string(&plan).unwrap();
        let deserialized: ScanPlan = ron::from_str(&serialized).unwrap();
        assert!(deserialized.companion_scan.is_none());
        assert_eq!(format!("{:?}", plan), format!("{:?}", deserialized));
    }
}
