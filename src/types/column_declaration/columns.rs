use std::fmt::Display;
use std::vec;

use super::ColumnDeclaration;

/// A collection of `ColumnDeclaration` instances, representing the schema of a table.
#[derive(Clone, Debug)]
pub struct ColumnDeclarations(pub Vec<ColumnDeclaration>);
/// Constructs `ColumnDeclarations` from an iterator over string slices, attempting
/// to parse each slice into a `ColumnDeclaration`.
impl<'a> FromIterator<&'a &'a str> for ColumnDeclarations {
    fn from_iter<T: IntoIterator<Item = &'a &'a str>>(iter: T) -> Self {
        let columns: Vec<ColumnDeclaration> = iter
            .into_iter()
            .filter_map(
                |&column_arg| match ColumnDeclaration::try_from(column_arg) {
                    Ok(column) => Some(column),
                    Err(_) => None,
                },
            )
            .collect();
        Self(columns)
    }
}

impl From<ColumnDeclarations> for String {
    /// Converts `ColumnDeclarations` into a comma-separated string of column definitions.
    fn from(value: ColumnDeclarations) -> Self {
        value
            .0
            .into_iter()
            .map::<String, _>(|col| col.to_string())
            .collect::<Vec<String>>()
            .join(", ")
    }
}
impl Display for ColumnDeclarations {
    /// Formats the `ColumnDeclarations` for display as a comma-separated list of column definitions.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s: String = self
            .0
            .iter()
            .map(|column_declaration| column_declaration.to_string())
            .collect::<Vec<String>>()
            .join(", ");
        f.write_str(&s)
    }
}

impl IntoIterator for ColumnDeclarations {
    /// Provides an iterator over the collection's `ColumnDeclaration` items.
    type Item = ColumnDeclaration;
    type IntoIter = vec::IntoIter<Self::Item>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}
