use std::{
    borrow::Cow,
    fmt::{self, Display},
};

use sqlite3_ext::ValueType;

use crate::{error::TableError, parse_value_type, utils::value_type_to_string};

/// Describes a single column within a table schema, including its name, data type,
/// and whether it serves as a partition column.
#[derive(Clone, Debug)]
pub struct ColumnDeclaration {
    name: Cow<'static, str>,
    data_type: ValueType,
    is_partition_column: bool,
    is_hidden: bool,
}

impl ColumnDeclaration {
    /// Constructs a new `ColumnDeclaration`.
    pub const fn new(name: Cow<'static, str>, data_type: ValueType) -> Self
    where
        Self: Sized,
    {
        Self {
            name,
            data_type,
            is_partition_column: false,
            is_hidden: false,
        }
    }

    /// Returns the column's name.
    pub fn get_name(&self) -> &str {
        &self.name
    }

    /// Returns the column's data type as a string.
    pub fn get_type(&self) -> &str {
        value_type_to_string(self.data_type())
    }

    /// Returns the column's `ValueType`.
    pub fn data_type(&self) -> &ValueType {
        &self.data_type
    }

    /// Indicates whether the column is marked as a partition column.
    pub fn is_partition_column(&self) -> bool {
        self.is_partition_column
    }

    /// Indicates that this column will be hidden.
    /// https://www.sqlite.org/vtab.html#hiddencol
    pub fn set_hidden(&mut self) {
        self.is_hidden = true;
    }
}

impl<'a> TryFrom<&'a str> for ColumnDeclaration {
    type Error = TableError;

    /// Attempts to create a `ColumnDeclaration` from a string slice, parsing the
    /// column name, data type, and partition column flag.
    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        let tokens: Vec<&str> = value.split_whitespace().collect();
        let mut is_partition_column = false;
        if tokens.len() != 2 {
            if tokens.len() == 3 {
                if tokens[2].to_lowercase().eq("partition_column") {
                    is_partition_column = true;
                } else {
                    return Err(TableError::ColumnDeclaration(format!(
                        "Invalid source string: {}. Expected optional third token 'partition_column'",
                        value
                    )));
                }
            } else {
                return Err(TableError::ColumnDeclaration(format!(
                    "Invalid source string: {}. Expected format 'name type'",
                    value
                )));
            }
        }
        let value_type: ValueType = parse_value_type(&tokens[1].trim().to_uppercase())?;
        Ok(Self {
            name: Cow::Owned(tokens[0].trim().to_string()),
            data_type: value_type,
            is_partition_column,
            is_hidden: false,
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
    /// Formats the `ColumnDeclaration` for display, including its name and data type.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let hidden = match self.is_hidden {
            true => " hidden",
            false => "",
        };
        f.write_fmt(format_args!(
            "{} {}{}",
            self.get_name(),
            self.get_type(),
            hidden
        ))
    }
}
