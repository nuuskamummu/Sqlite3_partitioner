use sqlite3_ext::{FromValue, ValueRef};

use crate::{error::TableError, ColumnDeclaration};

use super::{parse_to_unix_epoch, parsing::value_type_to_string};

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
