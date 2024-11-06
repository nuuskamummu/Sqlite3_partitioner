use super::ColumnDeclaration;

/// Represents the declaration of a partition column within a table schema, optionally
/// encapsulating a `ColumnDeclaration` to define the partitioning behavior.
pub type LifetimeColumn = ColumnDeclaration;
// pub struct LifetimeColumn(pub i64);
// impl FromIterator<ColumnDeclaration> for LifetimeColumn {
//     /// Creates a `PartitionColumn` from an iterator of `ColumnDeclaration` items, selecting
//     /// the first column marked as a partition column, if any.
//     fn from_iter<T: IntoIterator<Item = ColumnDeclaration>>(iter: T) -> Self {
//         let column = iter
//             .into_iter()
//             .find(|col_def| col_def.is_lifetime_column());
//         Self(column)
//     }
// }
// impl From<ColumnDeclaration> for Option<LifetimeColumn> {
//     fn from(value: ColumnDeclaration) -> Self {
//         if value.is_lifetime_column() {
//             value.
//             return Some(LifetimeColumn(1));
//         } else {
//             return None;
//         }
//     }
// }

// impl ExpirationColumn {
//     /// Returns a reference to the optional `ColumnDeclaration` representing the partition column.
//     pub fn column_def(&self) -> &Option<ColumnDeclaration> {
//         &self.0
//     }
//
//     /// Creates a new `PartitionColumn` with the specified `ColumnDeclaration`.
//     fn new(column_declaration: ColumnDeclaration) -> Self {
//         Self(Some(column_declaration))
//     }
// }
// impl From<ColumnDeclaration> for PartitionColumn {
//     /// Converts a `ColumnDeclaration` into a `PartitionColumn`.
//     fn from(value: ColumnDeclaration) -> Self {
//         Self::new(value)
//     }
// }
// impl<'a> From<&'a ColumnDeclaration> for PartitionColumn {
//     /// Converts a reference to a `ColumnDeclaration` into a `PartitionColumn`.
//     fn from(value: &'a ColumnDeclaration) -> Self {
//         PartitionColumn::new(value.clone())
//     }
// }
