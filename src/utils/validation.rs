use sqlite3_ext::{Connection, FromValue, Value, ValueRef};

use crate::{
    error::TableError, types::PartitionAccessor, ColumnDeclaration, Lookup, Partition, Template,
};

use super::parsing::value_type_to_string;

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
pub fn validate_and_map_columns<'vtab>(
    info: &[&ValueRef],
    column_declarations: &'vtab Vec<ColumnDeclaration>,
) -> sqlite3_ext::Result<Vec<(String, Value)>> {
    info.iter()
        .enumerate()
        .map(|(i, &v)| {
            let reference_column = &column_declarations[i];
            if &v.value_type() == reference_column.get_value_type() {
                Ok((reference_column.get_name().to_string(), v.to_owned()?))
            } else {
                Err(TableError::ColumnTypeMismatch {
                    expected: value_type_to_string(reference_column.get_value_type()),
                    found: value_type_to_string(&v.value_type()),
                }
                .into())
            }
        })
        .collect()
}

/// Resolves the name of a partition by first attempting to sync with the Lookup to see if the
/// partition exists, potentially created by another connection. If the partition does not exist,
/// it will be created by copying the template.
///
/// This function ensures that the partition name is correctly identified or created before
/// proceeding with operations that depend on the partition's existence.
///
/// # Parameters
/// - `partition`: A reference to the `RangePartition` struct representing the partition to resolve.
/// - `connection`: A reference to the `Connection` object, used for database interaction.
/// - `bucket`: An `i64` value representing the bucket number or identifier for the partition.
///
/// # Returns
/// - `sqlite3_ext::Result<String>`: On successful resolution or creation, returns the name of the
///   partition as a `String`. On failure, returns an error related to the partition lookup or creation process.
pub fn resolve_partition_name<'vtab>(
    partition: &'vtab Partition<i64>,
    connection: &Connection,
    bucket: i64,
) -> sqlite3_ext::Result<String> {
    partition
        .get_lookup()
        .get_partition(connection, bucket)
        .and_then(|(name, should_create)| {
            if should_create {
                partition
                    .get_template()
                    .copy_template(&bucket.to_string(), connection)
            } else {
                println!("resolving name {}", name);
                Ok(name)
            }
        })
}
