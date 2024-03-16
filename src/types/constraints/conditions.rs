use sqlite3_ext::{vtab::ConstraintOp, ValueRef};

/// Represents an individual condition in a SQL "WHERE" clause, encapsulating a column name,
/// a comparison operator, and a value for comparison.
///
/// Fields:
/// - `column`: The column name to which the condition applies.
/// - `operator`: The comparison operator used in the condition, such as "=", ">", or "<=".
/// - `value`: A reference to the value used in the comparison, supporting various data types.
#[derive(Debug, PartialEq)]
pub struct Condition<'a> {
    pub column: &'a str,
    pub operator: &'a ConstraintOp,
    pub value: &'a ValueRef,
}
/// A collection of `Condition` instances, providing a way to aggregate multiple conditions
/// for use in SQL WHERE clauses.
pub struct Conditions<'a> {
    /// A vector of `Condition` instances, representing multiple conditions to be applied in a query.
    inner: Vec<Condition<'a>>,
}
impl<'a> Conditions<'a> {
    /// Provides access to the conditions as a slice, allowing for iteration and inspection
    /// of the conditions without modifying the underlying collection.
    ///
    /// Returns a slice of `Condition` references.
    pub fn as_slice(&self) -> &[Condition<'a>] {
        &self.inner
    }
}
impl<'a> FromIterator<Condition<'a>> for Conditions<'a> {
    /// Constructs a `Conditions` instance from an iterator of `Condition` items. This allows for
    /// the creation of a `Conditions` collection from a sequence of individual condition
    /// instances.
    ///
    /// Parameters:
    /// - `iter`: An iterator over `Condition` instances to be included in the collection.
    ///
    /// Returns a new `Conditions` instance containing the provided conditions.
    fn from_iter<T: IntoIterator<Item = Condition<'a>>>(iter: T) -> Self {
        let conditions = iter.into_iter().collect();
        Self { inner: conditions }
    }
}
