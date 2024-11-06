use std::{
    borrow::Cow,
    fmt::{self, Display},
    vec,
};

use sqlite3_ext::ValueType;

use crate::{
    error::TableError,
    parse_value_type,
    utils::{parse_interval, value_type_to_string},
};

use super::ColumnDeclarations;

/// Describes a single column within a table schema, including its name, data type,
/// and whether it serves as a partition column.
#[derive(Clone, Debug)]
pub struct ColumnDeclaration {
    name: Cow<'static, str>,
    data_type: ValueType,
    is_partition_column: bool,
    is_hidden: bool,
    is_lifetime_column: bool,
    default_value: Option<i64>, //TODO:should it really be here? If yes, make it accept any valid datatype
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
            is_lifetime_column: false,
            default_value: None,
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

    /// Indicates whether the column is marked as a partition column.
    pub fn is_lifetime_column(&self) -> bool {
        self.is_lifetime_column
    }
    /// Indicates whether the column is marked as a partition column.
    pub fn default_value(&self) -> Option<i64> {
        self.default_value
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
        let mut is_lifetime_column = false;
        let mut value_type: Option<ValueType> = None;
        let mut default_value: Option<i64> = None;
        if tokens.len() != 2 {
            if tokens.len() == 3 {
                if tokens[2].to_lowercase().eq("partition_column") {
                    is_partition_column = true;
                } else if tokens[0].to_lowercase().eq("lifetime") {
                    println!("{:#?}", "found lifetime");
                    is_lifetime_column = true;
                    value_type = Some(ValueType::Integer);
                    default_value = Some(parse_interval(&format!("{} {}", tokens[1], tokens[2]))?);
                }
            } else {
                return Err(TableError::ColumnDeclaration(format!(
                    "Invalid source string: {}. Expected format 'name type'",
                    value
                )));
            }
        }
        let value_type: ValueType = match value_type {
            Some(v) => v,
            None => parse_value_type(&tokens[1].trim().to_uppercase())?,
        };
        Ok(Self {
            name: Cow::Owned(tokens[0].trim().to_string()),
            data_type: value_type,
            is_partition_column,
            is_hidden: false,
            is_lifetime_column,
            default_value,
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
