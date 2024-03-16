use sqlite3_ext::{FromValue, ValueRef};

use crate::{error::TableError, ColumnDeclaration};

use super::{parse_to_unix_epoch, parsing::value_type_to_string};

/// Validates and maps the columns from a slice of `ValueRef` against their declarations,
/// specifically identifying the partition column within the input.
///
/// This function ensures that each `ValueRef` in the input corresponds to the expected
/// data type as declared in `ColumnDeclaration`. The partition column is also identified
/// and returned separately for further processing. The function primarily checks the
/// data type consistency for the partition column, leveraging the assumption that
/// SQLite's default behavior does not enforce strict data type matching for all columns.
///
/// Parameters:
/// - `info`: A slice of references to `ValueRef`, representing the column values to be validated.
/// - `column_declarations`: A slice of `ColumnDeclaration` instances, detailing the expected
///   structure and data types of the columns.
/// - `partition_column_name`: The name of the partition column to be identified within the `info`.
///
/// Returns:
/// - On success, a tuple containing the original `info` slice and an `Option` holding a reference
///   to the `ValueRef` for the identified partition column. If the partition column is not found,
///   the second element of the tuple will be `None`.
/// - On failure, returns an `Error` if any of the column values do not match their expected
///   data types as per their declarations, with specific mention of the expected and found types.
///
/// Note: This function is critical for operations that require accurate mapping and validation
/// of input data against a predefined schema, particularly when partitioning logic is involved.
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
        let at_partition_column = partition_column.is_some_and(|column| column == v);
        if !at_partition_column
            || &v.value_type() == reference_column.data_type()
            || parse_to_unix_epoch(v).is_ok()
        {
            Ok(()) // only confirm data type for partition column. Default sqlite behaviour is to
                   // not enforce data type
        } else {
            Err(sqlite3_ext::Error::Module(
                TableError::ColumnTypeMismatch {
                    expected: value_type_to_string(reference_column.data_type()),
                    found: value_type_to_string(&v.value_type()),
                }
                .to_string(),
            ))
        }
    })?;
    Ok((info, partition_column))
}
