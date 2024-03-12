use sqlite3_ext::{FromValue, ValueRef};

use crate::{error::TableError, ColumnDeclaration};

use super::{parse_to_unix_epoch, parsing::value_type_to_string};

/// Validates and maps columns from an insert or update operation against the column definitions
/// in a table partition, ensuring type compatibility.
///
/// This function is typically called during INSERT or UPDATE commands to assert that the incoming
/// column-value pairs match the data types declared during the table's creation. It compares each
/// value's type with the corresponding column's declared type in the partition.
///
/// # Parameters
/// - `info`: A slice of references to `ValueRef`, representing the values being inserted or updated.
/// - `partition`: A reference to the `RangePartition` struct for the table partition, which includes
///   the column definitions.
///
/// # Returns
/// - `sqlite3_ext::Result<Vec<(String, Value)>>`: On success, returns a vector of tuples, each containing
///   a column name and its corresponding value that passed the type validation. On failure, returns an
///   error indicating a type mismatch between the provided value and the column definition.
pub fn validate_and_map_columns<'a>(
    info: &'a [&'a ValueRef],
    column_declarations: &'a [ColumnDeclaration],
    partition_column_name: &'a str,
) -> sqlite3_ext::Result<(&'a [&'a ValueRef], Option<&'a ValueRef>)> {
    let mut partition_column: Option<&ValueRef> = None;
    info.iter().enumerate().try_for_each(|(i, &v)| {
        let reference_column = &column_declarations[i]; //info is always in the same order as the table was declared in.
        if reference_column.get_name() == partition_column_name {
            partition_column = Some(v);
        }
        if &v.value_type() == reference_column.data_type()
            || (reference_column.get_type().to_uppercase() == "TIMESTAMP"
                && parse_to_unix_epoch(v).is_ok())
        {
            Ok(())
        } else {
            let e: sqlite3_ext::Error = TableError::ColumnTypeMismatch {
                expected: value_type_to_string(reference_column.data_type()),
                found: value_type_to_string(&v.value_type()),
            }
            .into();
            Err(e)
        }
    })?;
    Ok((info, partition_column))
}
