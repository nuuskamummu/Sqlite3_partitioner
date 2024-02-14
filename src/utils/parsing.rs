use std::{collections::HashMap, i64};

use chrono::NaiveDateTime;
use regex::Regex;
use sqlite3_ext::{ffi::SQLITE_FORMAT, Value, ValueType};

use crate::types::{ColumnDeclaration, CreateTableArgs};

pub fn calculate_bucket(value: &Value, interval: i64) -> sqlite3_ext::Result<i64> {
    parse_to_unix_epoch(value).map(|epoch| epoch - epoch % interval)
}
/// Converts a [`sqlite3_ext::ValueType`] to a [&`str`]
pub fn value_type_to_string(value_type: &ValueType) -> &'static str {
    match value_type {
        ValueType::Integer => "INTEGER",
        ValueType::Blob => "BLOB",
        ValueType::Text => "STRING",
        ValueType::Null => "NULL",
        ValueType::Float => "FLOAT",
    }
}
/// Converts a [str] to a [`sqlite3_ext::ValueType`]
pub fn parse_value_type(sqlite_type: &str) -> Result<ValueType, String> {
    match sqlite_type {
        "INT" | "INTEGER" | "TIMESTAMP" => Ok(ValueType::Integer),
        "TEXT" | "VARCHAR" => Ok(ValueType::Text),
        "FLOAT" => Ok(ValueType::Float),
        "BLOB" | "JSON" => Ok(ValueType::Blob),
        // Handle other SQLite types as needed
        _ => Err(format!("Cannot parse input type :'{}'", sqlite_type)),
    }
}
static DATETIME_FORMAT: &str = "%Y-%m-%d %H:%M:%S";
/// Accepts a [&str] with the format [`DATETIME_FORMAT`]
pub fn parse_datetime_to_epoch(datetime_str: &str) -> sqlite3_ext::Result<i64> {
    // This is a simplified example; you'll need to adjust the format according to your input
    match NaiveDateTime::parse_from_str(datetime_str, DATETIME_FORMAT) {
        Ok(result) => Ok(result.timestamp()),
        Err(err) => Err(sqlite3_ext::Error::Sqlite(
            SQLITE_FORMAT,
            Some(format!(
                "Could not parse string: {} to UNIX epoch. {}",
                datetime_str, err
            )),
        )),
    }
}

/// Parses an incoming [sqlite3_ext::ValueType] to UNIX epoch.
// TODO improve!
pub fn parse_to_unix_epoch(value: &Value) -> sqlite3_ext::Result<i64> {
    match value {
        Value::Integer(epoch) => Ok(*epoch),
        Value::Float(float_epoch) => Ok(float_epoch.round() as i64), // Assuming rounding is the desired behavior
        Value::Text(datetime_str) => parse_datetime_to_epoch(datetime_str),
        Value::Blob(_) | Value::Null => Err(sqlite3_ext::Error::Sqlite(
            SQLITE_FORMAT,
            Some(format!("Could not parse value to UNIX epoch")),
        )),
    }
}
/// Accepts a str with the format ["Numeric part Unit part"] where unit part is either [Hour] or
/// [Minute]. The result is a i64 representation of the interval in seconds.
// TODO better documentation, handle more cases.
pub fn parse_interval(interval_str: &str) -> Result<i64, String> {
    // Initialize the Regex pattern
    let re =
        Regex::new(r"(\d+)\s+(\w+)").map_err(|_| "Failed to compile regex pattern.".to_string())?;

    // Attempt to find matches in the input string
    let captures = re
        .captures(interval_str)
        .ok_or("Interval format is not valid.")?;

    // Extract the numeric part and unit part from the captures
    let numeric_part = captures
        .get(1)
        .ok_or("Missing numeric value in interval.")?
        .as_str();
    let unit_part = captures.get(2).ok_or("Missing unit in interval.")?.as_str();

    // Parse the numeric part as a u32
    let numeric_value = numeric_part
        .parse::<i64>()
        .map_err(|_| format!("Failed to parse '{}' as a number.", numeric_part))?;

    // Define a map for interval units to their sizes in seconds
    let mut interval_unit_to_size = HashMap::new();
    interval_unit_to_size.insert("hour", 60 * 60);
    interval_unit_to_size.insert("day", 24 * 60 * 60);

    // Calculate and return the total interval size based on the unit
    let size_in_seconds = interval_unit_to_size
        .get(unit_part)
        .ok_or_else(|| format!("Unsupported interval unit: '{}'.", unit_part))?;

    Ok(numeric_value * size_in_seconds)
}

/// Parses arguments for creating a table into a structured format, encapsulated in the `CreateTableArgs` structure.
///
/// # Parameters
/// - `args`: An array of string slices where the first three elements represent the module, database name, and table name, respectively,
/// and the subsequent elements represent column declarations.
///
/// # Returns
/// - `Result<CreateTableArgs, String>`: On success, returns a `CreateTableArgs` structure containing the table name and a vector of column declarations.
/// On failure, returns a `String` describing the error.
///
/// # Example Usage
/// ```
/// // Example of args: ["module_name", "database_name", "table_name", "column1 TYPE", "column2 TYPE"]
/// let args = vec!["module", "db_name", "table_name", "id INT", "name VARCHAR"];
/// let create_table_args = parse_create_table_args(&args);
/// ```
pub fn parse_create_table_args(args: &[&str]) -> Result<CreateTableArgs, String> {
    let _module = args[0];
    let _database_name = args[1];
    let table_name = args[2];
    let column_args = args[3..].to_vec();
    let columns: Result<Vec<ColumnDeclaration>, String> = column_args
        .iter()
        .map(|&column_arg| ColumnDeclaration::new(column_arg))
        .collect();
    match columns {
        Ok(cols) => Ok(CreateTableArgs {
            table_name: table_name.to_string(),
            columns: cols,
        }),
        Err(e) => Err(e),
    }
}
/// Extracts pairs of column names and their associated operators from a given input string.
///
/// The input string should contain pairs separated by ", " with each pair consisting of a column name followed by a whitespace and then an operator.
///
/// # Parameters
/// - `input`: A string slice containing the column-operator pairs, separated by ", ".
///
/// # Returns
/// - `Vec<(&str, &str)>`: A vector of tuples, each containing a column name and an operator extracted from the input string.
///
/// # Example Usage
/// ```
/// let input = "column1 =, column2 >";
/// let pairs = extract_column_operator_pairs(input);
/// // pairs would contain [("column1", "="), ("column2", ">")]
/// ```
pub fn extract_column_operator_pairs(input: &str) -> Vec<(&str, &str)> {
    // Split the input string into pairs of "COLUMN OPERATOR"
    let pairs = input.split(", ");

    // Map each pair into a tuple of (column, operator)
    let mut result = Vec::new();
    for pair in pairs {
        let parts: Vec<&str> = pair.split_whitespace().collect();
        if parts.len() == 2 {
            result.push((parts[0], parts[1]));
        } else {
            // Handle error or unexpected format
            println!("Unexpected format for pair: {}", pair);
        }
    }

    result
}
use std::ops::Bound;

#[derive(Debug, PartialEq)]

/// Represents a single condition in a SQL "WHERE" clause with a column name, an operator, and a value.
///
/// - `column`: The name of the column the condition applies to.
/// - `operator`: The comparison operator used in the condition (e.g., "=", ">", "<=").
/// - `value`: The numeric value used for comparison in the condition.
pub struct Condition {
    column: String,
    operator: String,
    value: i64,
}

/// Parses a `where_clause` string containing SQL-like conditions joined by "AND" into a vector of `Condition` structures.
///
/// # Parameters
///
/// - `where_clause`: A string representing the "WHERE" clause of a SQL query. Conditions within the string must be separated by "AND".
///
/// # Returns
///
/// - A vector of `Condition` structures representing each condition found in the `where_clause`.
///
/// # Example Usage
///
/// ```
/// let where_clause = "column1 >= 10 AND column2 < 20";
/// let conditions = parse_conditions(where_clause);
/// ```
pub fn parse_conditions(where_clause: &str) -> Vec<Condition> {
    where_clause
        .split(" AND ")
        .filter_map(|cond| {
            let parts: Vec<&str> = cond.split_whitespace().collect();
            if parts.len() == 3 {
                let column = parts[0].to_string();
                let operator = parts[1].to_string();
                parts[2].parse::<i64>().ok().map(|value| Condition {
                    column,
                    operator,
                    value,
                })
            } else {
                None
            }
        })
        .collect()
}

/// Aggregates a list of `Condition` structures into a hashmap where each key is a column name,
/// and the value is a tuple representing the lower and upper bounds of that column.
/// This aggregation takes into account the operators in each condition to adjust the bounds accordingly.
///
/// # Parameters
///
/// - `conditions`: A vector of `Condition` structures to be aggregated into column range bounds.
///
/// # Returns
///
/// - A hashmap where each key is a column name, and the value is a tuple of `Bound<i64>`
/// representing the inclusive or exclusive lower and upper bounds for the column based on the conditions provided.
///
/// # Example Usage
///
/// ```
/// let conditions = vec![
///     Condition {
///         column: "age".to_string(),
///         operator: ">=".to_string(),
///         value: 18,
///     },
///     Condition {
///         column: "age".to_string(),
///         operator: "<=".to_string(),
///         value: 65,
///     },
/// ];
/// let ranges = aggregate_conditions_to_ranges(conditions);
/// ```
pub fn aggregate_conditions_to_ranges(
    conditions: Vec<Condition>,
) -> HashMap<String, (Bound<i64>, Bound<i64>)> {
    let mut ranges: HashMap<String, (Bound<i64>, Bound<i64>)> = HashMap::new();

    for condition in conditions {
        let column_range = ranges
            .entry(condition.column.clone())
            .or_insert((Bound::Unbounded, Bound::Unbounded));

        match condition.operator.as_str() {
            ">=" | ">" => {
                // ">=" does not need adjustment for the interval
                let new_bound = Bound::Included(condition.value);
                column_range.0 = update_bound(column_range.0, new_bound, true);
            }
            "<" => {
                // "<" directly translates to an excluded upper bound without interval adjustment
                let new_bound = Bound::Excluded(condition.value);
                column_range.1 = update_bound(column_range.1, new_bound, false);
            }
            "<=" => {
                // Adjust "<=" to include the upper range bound considering the interval
                let new_bound = Bound::Included(condition.value);
                column_range.1 = update_bound(column_range.1, new_bound, false);
            }
            "=" => {
                // "=" conditions set both bounds to include the value, considering the interval for upper bound
                column_range.0 = Bound::Included(condition.value);
                column_range.1 = Bound::Included(condition.value);
            }
            _ => {}
        }
    }

    ranges
}
use Bound::{Excluded, Included, Unbounded};

/// Updates the bound based on the new value, considering whether it's a lower or upper bound. The
/// most restrictive bound is selected.
///
/// Parameters:
/// - `current`: The current bound.
/// - `new`: The new bound to compare against the current.
/// - `is_lower`: A boolean indicating if we're updating a lower bound. True for lower, false for upper.
///
/// Returns:
/// - The updated bound after comparing the current and new bounds.
fn update_bound(current: Bound<i64>, new: Bound<i64>, is_lower: bool) -> Bound<i64> {
    match (current, new) {
        // If the current bound is unbounded, any new bound is more restrictive and should be taken.
        (Unbounded, _) => new,

        // If the new bound is unbounded, it doesn't impose any new restrictions. Keep the current.
        (_, Unbounded) => current,

        // When both bounds are of the same type (both included or excluded),
        // decide based on the comparison and whether we're updating a lower or upper bound.
        (Included(a), Included(b)) | (Excluded(a), Excluded(b)) => {
            update_same_type_bounds(a, b, is_lower)
        }

        // When the bounds are of different types, handle inclusivity/exclusivity carefully,
        // especially important for adjacent or equal values.
        (Included(a), Excluded(b)) | (Excluded(a), Included(b)) => {
            update_different_type_bounds(a, b, is_lower)
        }
    }
}

/// Compares bounds of the same type (both included or excluded) to decide which to keep.
///
/// Parameters:
/// - `a`: The value of the current bound.
/// - `b`: The value of the new bound.
/// - `is_lower`: Indicates if updating a lower bound.
///
/// Returns:
/// - The chosen bound based on the comparison and bound type.
fn update_same_type_bounds(a: i64, b: i64, is_lower: bool) -> Bound<i64> {
    if is_lower {
        if a < b {
            Included(b)
        } else {
            Included(a)
        }
    } else {
        if a > b {
            Included(b)
        } else {
            Included(a)
        }
    }
}

/// Compares bounds of different types (one included, one excluded) to decide which to keep.
///
/// Parameters:
/// - `a`: The value of the current bound.
/// - `b`: The value of the new bound.
/// - `is_lower`: Indicates if updating a lower bound.
///
/// Returns:
/// - The chosen bound based on the comparison, considering inclusivity/exclusivity.
fn update_different_type_bounds(a: i64, b: i64, is_lower: bool) -> Bound<i64> {
    if is_lower {
        if a <= b {
            Excluded(b)
        } else {
            Excluded(a)
        }
    } else {
        if a >= b {
            Excluded(b)
        } else {
            Excluded(a)
        }
    }
}
