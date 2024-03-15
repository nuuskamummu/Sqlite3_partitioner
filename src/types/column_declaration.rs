use std::{
    fmt::{self, Display},
    vec,
};

use sqlite3_ext::ValueType;

use crate::{error::TableError, parse_value_type, utils::value_type_to_string};

pub struct PartitionColumn(pub Option<ColumnDeclaration>);
impl FromIterator<ColumnDeclaration> for PartitionColumn {
    fn from_iter<T: IntoIterator<Item = ColumnDeclaration>>(iter: T) -> Self {
        let column = iter
            .into_iter()
            .find(|col_def| col_def.is_partition_column());
        Self(column)
    }
}
impl PartitionColumn {
    pub fn column_def(&self) -> &Option<ColumnDeclaration> {
        &self.0
    }
    fn new(column_declaration: ColumnDeclaration) -> Self {
        Self(Some(column_declaration))
    }
}
impl From<ColumnDeclaration> for PartitionColumn {
    fn from(value: ColumnDeclaration) -> Self {
        Self::new(value)
    }
}
impl<'a> From<&'a ColumnDeclaration> for PartitionColumn {
    fn from(value: &'a ColumnDeclaration) -> Self {
        PartitionColumn::new(value.clone())
    }
}

#[derive(Clone, Debug)]
pub struct ColumnDeclaration {
    name: String,
    data_type: ValueType,
    is_partition_column: bool,
}

impl ColumnDeclaration {
    pub const fn new(name: String, data_type: ValueType) -> Self
    where
        Self: Sized,
    {
        Self {
            name,
            data_type,
            is_partition_column: false,
        }
    }

    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn get_type(&self) -> &str {
        value_type_to_string(self.data_type())
    }
    pub fn data_type(&self) -> &ValueType {
        &self.data_type
    }
    pub fn is_partition_column(&self) -> bool {
        self.is_partition_column
    }
}

impl<'a> TryFrom<&'a str> for ColumnDeclaration {
    type Error = TableError;
    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        let tokens: Vec<&str> = value.split_whitespace().collect();
        let mut is_partition_column = false;

        if tokens.len() != 2 {
            if tokens.len() == 3 && tokens[2] == "partition_column" {
                is_partition_column = true;
            } else {
                return Err(TableError::ColumnDeclaration(format!(
                    "Invalid source string: {}. Expected format 'name type'",
                    value
                )));
            }
        }

        Ok(Self {
            name: tokens[0].trim().to_string(),
            data_type: parse_value_type(&tokens[1].trim().to_uppercase())?,
            is_partition_column,
        })
    }
}

// impl<'a> TryFrom<&'a [&'a str]> for ColumnDeclaration {
//     type Error = TableError;
//     fn try_from(value: &'a [&'a str]) -> Result<Self, Self::Error> {
//         let columns: String = value
//             .iter()
//             .map(|&col_arg| col_arg.into())
//             .collect::<Vec<String>>()
//             .join(" ");
//         ColumnDeclaration::try_from(&columns)
//     }
// }

impl Display for ColumnDeclaration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{} {}", self.get_name(), self.get_type()))
    }
}

#[derive(Clone, Debug)]
pub struct ColumnDeclarations(pub Vec<ColumnDeclaration>);
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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s: String = self
            .0
            .iter()
            .map(|column_declaration| column_declaration.to_string())
            .collect::<Vec<String>>()
            .join(" ,");
        f.write_str(&s)
    }
}

impl IntoIterator for ColumnDeclarations {
    type Item = ColumnDeclaration;
    type IntoIter = vec::IntoIter<Self::Item>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> From<&'a ColumnDeclarations> for &'a [ColumnDeclaration] {
    fn from(value: &'a ColumnDeclarations) -> Self {
        &value.0
    }
}
