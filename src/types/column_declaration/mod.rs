pub mod column;
pub mod columns;
pub mod expiration;
pub mod partition;
pub use column::ColumnDeclaration;
pub use columns::ColumnDeclarations;
pub use partition::PartitionColumn;

impl<'a> From<&'a ColumnDeclarations> for &'a [ColumnDeclaration] {
    /// Converts a reference to `ColumnDeclarations` into a slice of `ColumnDeclaration`.
    fn from(value: &'a ColumnDeclarations) -> Self {
        &value.0
    }
}
