pub mod interface;
pub mod lookup_table;
pub mod operations;
mod partition_interface;
pub mod root_table;
pub mod template_table;
pub use lookup_table::*;
pub use partition_interface::partition::Partition;

pub use root_table::*;
use sqlite3_ext::ValueType;
pub use template_table::*;

use crate::{error::TableError, ColumnDeclaration, ColumnDeclarations};

pub enum PartitionValue {
    Interval,
}

impl From<PartitionValue> for ValueType {
    fn from(value: PartitionValue) -> ValueType {
        match value {
            PartitionValue::Interval => ValueType::Integer,
        }
    }
}
impl<'a> From<&'a PartitionValue> for &'a ValueType {
    fn from(value: &'a PartitionValue) -> &'a ValueType {
        match value {
            PartitionValue::Interval => &ValueType::Integer,
        }
    }
}

impl<'a> TryFrom<&'a ValueType> for PartitionValue {
    type Error = TableError;
    fn try_from(value: &'a ValueType) -> Result<Self, Self::Error> {
        match value {
            ValueType::Text => Ok(PartitionValue::Interval),
            _ => Err(TableError::PartitionColumn(format!(
                "Supported types for partition column: {:#?}",
                "timestamp"
            ))),
        }
    }
}

pub trait PartitionType {
    const PARTITION_VALUE_COLUMN_TYPE: &'static PartitionValue;
    const PARTITION_VALUE_COLUMN: &'static str;
    const PARTITION_NAME_COLUMN: &'static str;
    const PARTITION_NAME_COLUMN_TYPE: &'static ValueType;
    fn columns() -> ColumnDeclarations {
        ColumnDeclarations(vec![
            Self::partition_name_column(),
            Self::partition_value_column(),
        ])
    }
    fn partition_name_column() -> ColumnDeclaration {
        ColumnDeclaration::new(
            Self::PARTITION_NAME_COLUMN.to_string(),
            *Self::PARTITION_NAME_COLUMN_TYPE,
        )
    }

    fn partition_value_column() -> ColumnDeclaration {
        let value_type: &ValueType = Self::PARTITION_VALUE_COLUMN_TYPE.into();
        ColumnDeclaration::new(Self::PARTITION_VALUE_COLUMN.to_string(), *value_type)
    }
}
