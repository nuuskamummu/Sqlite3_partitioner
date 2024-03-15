use sqlite3_ext::{vtab::ConstraintOp, ValueRef};

/// Represents a single condition in a SQL "WHERE" clause with a column name, an operator, and a value.
///
/// - `column`: The name of the column the condition applies to.
/// - `operator`: The comparison operator used in the condition (e.g., "=", ">", "<=").
/// - `value`: The numeric value used for comparison in the condition.
#[derive(Debug, PartialEq)]
pub struct Condition<'a> {
    pub column: &'a str,
    pub operator: &'a ConstraintOp,
    pub value: &'a ValueRef,
}
pub struct Conditions<'a> {
    inner: Vec<Condition<'a>>,
}
impl<'a> Conditions<'a> {
    pub fn as_slice(&self) -> &[Condition<'a>] {
        &self.inner
    }
}
impl<'a> FromIterator<Condition<'a>> for Conditions<'a> {
    fn from_iter<T: IntoIterator<Item = Condition<'a>>>(iter: T) -> Self {
        let conditions = iter.into_iter().collect();
        Self { inner: conditions }
    }
}
