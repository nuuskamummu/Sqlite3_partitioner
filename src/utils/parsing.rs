use std::{collections::HashMap, i64};

use chrono::NaiveDateTime;
use regex::Regex;
use sqlite3_ext::{ffi::SQLITE_FORMAT, vtab::ConstraintOp, Value, ValueType};

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
use std::ops::Bound::{self, *};

/// Represents a single condition in a SQL "WHERE" clause with a column name, an operator, and a value.
///
/// - `column`: The name of the column the condition applies to.
/// - `operator`: The comparison operator used in the condition (e.g., "=", ">", "<=").
/// - `value`: The numeric value used for comparison in the condition.
#[derive(Debug, PartialEq)]
pub struct Condition {
    pub column: String,
    pub operator: ConstraintOp,
    pub value: Value,
}

//
/// Aggregates conditions into ranges for each column.
///
/// Parameters:
/// - `conditions`: A vector of conditions to be aggregated into ranges.
///
/// Returns:
/// - A HashMap where each key is a column name and its value is a tuple representing the range as lower and upper bounds.
pub fn aggregate_conditions_to_ranges(
    conditions: Vec<Condition>,
) -> HashMap<String, (Bound<i64>, Bound<i64>)> {
    let mut ranges: HashMap<String, (Bound<i64>, Bound<i64>)> = HashMap::new();

    for condition in conditions {
        if let Value::Integer(value) = condition.value {
            ranges
                .entry(condition.column.clone())
                .and_modify(|e| {
                    update_bound(e, &condition.operator, value);
                })
                .or_insert_with(|| initial_bound(&condition.operator, value));
        }
        // Handling of non-integer values omitted for brevity; could log, error, or skip.
    }

    ranges
}
/// Updates the range bounds based on the condition operator and value.
///
/// Parameters:
/// - `range`: The current range bounds to update.
/// - `operator`: The condition operator.
/// - `value`: The integer value for the condition.
fn update_bound(range: &mut (Bound<i64>, Bound<i64>), operator: &ConstraintOp, value: i64) {
    match operator {
        ConstraintOp::GT => range.0 = max_bound(range.0, Excluded(value)),
        ConstraintOp::LT => range.1 = min_bound(range.1, Excluded(value)),
        ConstraintOp::LE => range.1 = min_bound(range.1, Included(value)),
        ConstraintOp::Eq => {
            range.0 = Included(value);
            range.1 = Included(value);
        }
        _ => {} // Other operators could be handled here as needed
    }
}
/// Determines the initial bounds based on the first condition for a column.
///
/// Parameters:
/// - `operator`: The condition operator.
/// - `value`: The integer value for the condition.
///
/// Returns:
/// - A tuple representing the initial range as lower and upper bounds.
fn initial_bound(operator: &ConstraintOp, value: i64) -> (Bound<i64>, Bound<i64>) {
    match operator {
        ConstraintOp::GT => (Excluded(value), Unbounded),
        ConstraintOp::LT => (Unbounded, Excluded(value)),
        ConstraintOp::LE => (Unbounded, Included(value)),
        ConstraintOp::Eq => (Included(value), Included(value)),
        // Default case to handle other operators, assuming full range
        _ => (Unbounded, Unbounded),
    }
}

/// Finds the maximum of two lower bounds.
///
/// Parameters:
/// - `a`: The first lower bound.
/// - `b`: The second lower bound.
///
/// Returns:
/// - The more restrictive of the two bounds.
fn max_bound(a: Bound<i64>, b: Bound<i64>) -> Bound<i64> {
    match (a, b) {
        (Unbounded, _) => b,
        (_, Unbounded) => a,
        (Included(a_val), Included(b_val)) => Included(std::cmp::max(a_val, b_val)),
        (Excluded(a_val), Excluded(b_val)) => Excluded(std::cmp::max(a_val, b_val)),
        (Excluded(a_val), Included(b_val)) | (Included(a_val), Excluded(b_val)) => {
            if a_val >= b_val {
                Excluded(a_val)
            } else {
                Included(b_val)
            }
        }
    }
}

/// Finds the minimum of two upper bounds.
///
/// Parameters:
/// - `a`: The first upper bound.
/// - `b`: The second upper bound.
///
/// Returns:
/// - The more restrictive of the two bounds.
fn min_bound(a: Bound<i64>, b: Bound<i64>) -> Bound<i64> {
    match (a, b) {
        (Unbounded, _) => b,
        (_, Unbounded) => a,
        (Included(a_val), Included(b_val)) => Included(std::cmp::min(a_val, b_val)),
        (Excluded(a_val), Excluded(b_val)) => Excluded(std::cmp::min(a_val, b_val)),
        (Excluded(a_val), Included(b_val)) | (Included(a_val), Excluded(b_val)) => {
            if a_val <= b_val {
                Excluded(a_val)
            } else {
                Included(b_val)
            }
        }
    }
}
