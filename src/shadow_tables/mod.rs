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

impl PartitionValue {
    const fn to_valuetype(partitionvalue: Self) -> ValueType {
        match partitionvalue {
            Self::Interval => ValueType::Integer,
        }
    }
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
// type IntervalPartition = ValueType::Integer;
pub trait PartitionType {
    const PARTITION_VALUE_COLUMN_TYPE: PartitionValue;
    const PARTITION_VALUE_COLUMN: &'static str;
    const PARTITION_NAME_COLUMN: &'static str;
    const PARTITION_NAME_COLUMN_TYPE: ValueType;
    const PARTITION_IDENTIFIER: ColumnDeclaration = ColumnDeclaration::new(
        std::borrow::Cow::Borrowed(Self::PARTITION_NAME_COLUMN),
        Self::PARTITION_NAME_COLUMN_TYPE,
    );
    const PARTITION_TYPE: ColumnDeclaration = ColumnDeclaration::new(
        std::borrow::Cow::Borrowed(Self::PARTITION_VALUE_COLUMN),
        PartitionValue::to_valuetype(Self::PARTITION_VALUE_COLUMN_TYPE),
    );
    const COLUMNS: &'static [ColumnDeclaration] =
        &[Self::PARTITION_IDENTIFIER, Self::PARTITION_TYPE];
    fn columns() -> ColumnDeclarations {
        ColumnDeclarations(Self::COLUMNS.to_vec())
    }
}
