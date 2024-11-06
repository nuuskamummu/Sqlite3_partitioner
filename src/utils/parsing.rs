use std::{
    cmp::{max, min},
    collections::HashMap,
    i64,
};

use chrono::{NaiveDate, NaiveDateTime};
use regex::Regex;
use sqlite3_ext::{ffi::SQLITE_FORMAT, vtab::ConstraintOp, FromValue, Value, ValueRef, ValueType};

use crate::{constraints::Condition, error::TableError};

/// Parses a `ValueRef` and adjusts it to the nearest lower interval boundary based on the provided interval.
///
/// Parameters:
/// - `value`: The value to be parsed and adjusted.
/// - `interval`: The interval by which to adjust the value.
///
/// Returns:
/// - A result containing the adjusted UNIX epoch time or an error if the value cannot be parsed
pub fn parse_partition_value(value: &ValueRef, interval: i64) -> sqlite3_ext::Result<i64> {
    parse_to_unix_epoch(value).map(|epoch| epoch - epoch % interval)
}
/// Converts a [`ValueType`] enum to a string representation.
///
/// Parameters:
/// - `value_type`: The value type to convert.
///
/// Returns:
/// - A string slice representing the data type.
pub fn value_type_to_string(value_type: &ValueType) -> &'static str {
    match value_type {
        ValueType::Integer => "INTEGER",
        ValueType::Blob => "BLOB",
        ValueType::Text => "TEXT",
        ValueType::Null => "NULL",
        ValueType::Float => "FLOAT",
    }
}
/// Converts a string representation of a SQLite data type to its [`ValueType`] enum.
///
/// Parameters:
/// - `sqlite_type`: The string representation of the SQLite data type.
///
/// Returns:
/// - A result containing the corresponding `ValueType` enum or a `TableError` if the type cannot be parsed.
pub fn parse_value_type(sqlite_type: &str) -> Result<ValueType, TableError> {
    match &sqlite_type.to_uppercase()[..] {
        "INT" | "INTEGER" => Ok(ValueType::Integer),
        "TEXT" | "VARCHAR" | "TIMESTAMP" => Ok(ValueType::Text),
        "FLOAT" => Ok(ValueType::Float),
        "BLOB" | "JSON" => Ok(ValueType::Blob),
        "NULL" => Ok(ValueType::Null),
        _ => Err(TableError::ParseValueType(format!(
            "Cannot parse input type :'{}'",
            sqlite_type
        ))),
    }
}

static DATETIME_FORMATS: &[&str] = &[
    "%Y-%m-%d %H:%M:%S",   // Standard ISO 8601 datetime
    "%Y-%m-%d %H:%M",      // ISO 8601 without seconds
    "%Y-%m-%d",            // ISO 8601 date only
    "%d-%m-%Y %H:%M:%S",   // European date with time
    "%d-%m-%Y %H:%M",      // European date without seconds
    "%d-%m-%Y",            // European date
    "%m/%d/%Y %H:%M:%S",   // US date with time
    "%m/%d/%Y %H:%M",      // US date without seconds
    "%m/%d/%Y",            // US date
    "%Y%m%d%H%M%S",        // Compact datetime without separators
    "%Y%m%d",              // Compact date without separators
    "%Y-%m-%dT%H:%M:%SZ",  // ISO 8601 datetime with Zulu (UTC) time zone
    "%Y-%m-%dT%H:%M:%S%z", // ISO 8601 datetime with numeric time zone
    "%I:%M:%S %p",         // 12-hour clock time with AM/PM
    "%I:%M %p",            // 12-hour clock time without seconds
    "%B %d, %Y %H:%M:%S",  // Full month name, day, year, time
    "%b %d, %Y %H:%M:%S",  // Abbreviated month name, day, year, time
                           // Add more formats as needed
];

/// Attempts to parse a datetime string into a UNIX epoch time.
///
/// Parameters:
/// - `value`: The `Value` instance containing a datetime string.
///
/// Returns:
/// - A result containing the parsed UNIX epoch time or an error if parsing fails.
pub fn parse_datetime_from_value(value: Value) -> sqlite3_ext::Result<i64> {
    match value {
        Value::Text(value) => parse_datetime_to_epoch(value.trim()),
        _ => Err(sqlite3_ext::Error::Sqlite(
            SQLITE_FORMAT,
            Some(format!(
                "Could not parse value: {:#?} to UNIX epoch.",
                value
            )),
        )),
    }
}

/// Parses a datetime string to a UNIX epoch time, trying multiple known formats.
///
/// Parameters:
/// - `datetime_str`: The datetime string to parse.
///
/// Returns:
/// - A result containing the UNIX epoch time or an error if all parsing attempts fail.
fn parse_datetime_to_epoch(datetime_str: &str) -> sqlite3_ext::Result<i64> {
    for &format in DATETIME_FORMATS.iter() {
        let trimmed_format = format.trim();
        // Attempt to parse as NaiveDateTime first
        if let Ok(datetime) = NaiveDateTime::parse_from_str(datetime_str, trimmed_format) {
            return Ok(datetime.and_utc().timestamp());
        }
        // Attempt to parse as NaiveDate if NaiveDateTime parsing fails
        if let Ok(date) = NaiveDate::parse_from_str(datetime_str, trimmed_format) {
            // Assuming start of the day for date-only entries
            let datetime = date.and_hms_opt(0, 0, 0).unwrap();
            return Ok(datetime.and_utc().timestamp());
        }
    }

    // If all parsing attempts fail, return an error
    Err(sqlite3_ext::Error::Sqlite(
        SQLITE_FORMAT,
        Some(format!(
            "Could not parse string: '{}' to UNIX epoch.",
            datetime_str
        )),
    ))
}

/// Converts a given `ValueRef` to a UNIX epoch timestamp (seconds since the UNIX epoch).
///
/// This function supports several `ValueType`s, converting them appropriately to ensure
/// consistent handling of datetime values across different data representations. The conversion
/// logic includes:
/// - `Integer`: Directly returned as the UNIX epoch timestamp.
/// - `Float`: Cast to `i64`, assuming rounding is acceptable for the use case.
/// - `Text`: Attempted parsing as a datetime string to UNIX epoch. Supports multiple datetime formats.
/// - `Blob` and `Null`: These types are considered incompatible with UNIX epoch timestamps, resulting in an error.
///
/// Parameters:
/// - `value`: A reference to the `ValueRef` representing the data to be converted.
///
/// Returns:
/// - On success, an `Ok(i64)` containing the UNIX epoch timestamp.
/// - On failure, particularly for `Blob` and `Null` types or if text parsing fails, returns
///   an `Error` indicating the inability to parse the value as a UNIX epoch timestamp.
///
/// Note: The handling of `Float` values involves casting to `i64`, which may not be suitable
/// for all use cases. Consider the desired behavior for your application when using this function.
pub fn parse_to_unix_epoch(value: &ValueRef) -> sqlite3_ext::Result<i64> {
    match value.value_type() {
        ValueType::Integer => Ok(value.get_i64()),
        ValueType::Float => Ok(value.get_f64() as i64), // Assuming rounding is the desired behavior
        ValueType::Text => parse_datetime_to_epoch(value.try_get_str()?),
        ValueType::Blob | ValueType::Null => Err(sqlite3_ext::Error::Sqlite(
            SQLITE_FORMAT,
            Some("Could not parse value to UNIX epoch".to_string()),
        )),
    }
}
/// Parses a textual representation of a datetime interval to its duration in seconds.
///
/// Parameters:
/// - `interval_str`: The interval string to parse, e.g., "1 hour".
///
/// Returns:
/// - A result containing the interval in seconds or a `TableError` if parsing fails.
pub fn parse_interval(interval_str: &str) -> Result<i64, TableError> {
    // Initialize the Regex pattern
    let re = Regex::new(r"(\d+)\s+(\w+)")
        .map_err(|_| TableError::ParseInterval("Failed to compile regex pattern.".to_string()))?;

    println!("lifetime str {:#?}", interval_str);
    // Attempt to find matches in the input string
    let captures = re.captures(interval_str).ok_or(TableError::ParseInterval(
        "Interval format is not valid.".to_string(),
    ))?;

    // Extract the numeric part and unit part from the captures
    let numeric_part = captures
        .get(1)
        .ok_or(TableError::ParseInterval(
            "Missing numeric value in interval.".to_string(),
        ))?
        .as_str();
    let unit_part = captures
        .get(2)
        .ok_or(TableError::ParseInterval(
            "Missing unit in interval.".to_string(),
        ))?
        .as_str();

    // Parse the numeric part as a u32
    let numeric_value = numeric_part.parse::<i64>().map_err(|_| {
        TableError::ParseInterval(format!("Failed to parse '{}' as a number.", numeric_part))
    })?;

    // Define a map for interval units to their sizes in seconds
    let mut interval_unit_to_size = HashMap::new();
    interval_unit_to_size.insert("hour", 60 * 60);
    interval_unit_to_size.insert("day", 24 * 60 * 60);

    // Calculate and return the total interval size based on the unit
    let size_in_seconds = interval_unit_to_size.get(unit_part).ok_or_else(|| {
        TableError::ParseInterval(format!("Unsupported interval unit: '{}'.", unit_part))
    })?;
    println!("returns {:#?}", numeric_value * size_in_seconds);
    Ok(numeric_value * size_in_seconds)
}

use std::ops::Bound::{self, *};

/// Aggregates a list of conditions into column-wise ranges, represented as lower and upper bounds.
///
/// Parameters:
/// - `conditions`: A slice of conditions to aggregate.
/// - `interval`: The interval by which the conditions should be adjusted.
///
/// Returns:
/// - A `HashMap` where each key is a column name and its value is a tuple representing the column's value range.
pub fn aggregate_conditions_to_ranges<'a>(
    conditions: &'a [Condition<'a>],
    interval: i64,
) -> HashMap<&'a str, (Bound<i64>, Bound<i64>)> {
    let mut ranges: HashMap<&'a str, (Bound<i64>, Bound<i64>)> = HashMap::new();
    for condition in conditions {
        let partition_start = parse_partition_value(condition.value, interval).unwrap(); //TODO handle
                                                                                         //error

        ranges
            .entry(condition.column)
            .and_modify(|e| {
                update_bound(e, condition.operator, partition_start, interval);
            })
            .or_insert_with(|| initial_bound(condition.operator, partition_start, interval));
    }

    ranges
}

/// Updates the range boundaries based on the provided operator and value.
///
/// This function adjusts the lower or upper bounds of a range tuple to reflect the
/// constraints imposed by a SQL condition. It uses `less_restrictive_bound` or
/// `more_restrictive_bound` functions to ensure the updated range accurately
/// represents the condition's intent.
///
/// Parameters:
/// - `range`: A mutable reference to a tuple representing the current range (lower and upper bounds).
/// - `operator`: The SQL comparison operator from the condition.
/// - `value`: The comparison value from the condition.
/// - `interval`: The interval for adjusting the range, used with certain operators to define the range more accurately.
///
/// No return value, but modifies the input range in place.
fn update_bound(
    range: &mut (Bound<i64>, Bound<i64>),
    operator: &ConstraintOp,
    value: i64,
    interval: i64,
) {
    match operator {
        ConstraintOp::GT | ConstraintOp::GE => {
            let lower_bound = Excluded(value);
            range.0 = less_restrictive_bound(range.0, lower_bound);
        }
        ConstraintOp::LT => {
            let upper_bound = Excluded(value + interval);
            range.1 = more_restrictive_bound(range.1, upper_bound);
        }
        ConstraintOp::LE => {
            let upper_bound = Included(value + interval);
            range.1 = more_restrictive_bound(range.1, upper_bound);
        }
        ConstraintOp::Eq => {
            let bound = Included(value);
            range.0 = more_restrictive_bound(range.0, bound);
            range.1 = more_restrictive_bound(range.1, bound);
        }
        _ => {}
    }
}

/// Calculates the initial range boundaries based on a given operator, value, and interval.
///
/// This function determines the starting lower and upper bounds for a range, based on the
/// specified operator and value. It is particularly useful for initializing the bounds
/// before refining them with further conditions.
///
/// Parameters:
/// - `operator`: The SQL comparison operator from the condition, dictating how the initial bounds are set.
/// - `value`: The comparison value for the condition, used to establish the initial bounds.
/// - `interval`: The interval for adjusting the range with certain operators, aiding in defining the initial range.
///
/// Returns:
/// - A tuple representing the initial range (lower and upper bounds) based on the operator and value.
fn initial_bound(operator: &ConstraintOp, value: i64, interval: i64) -> (Bound<i64>, Bound<i64>) {
    match operator {
        ConstraintOp::GT | ConstraintOp::GE => (Excluded(value), Unbounded),
        ConstraintOp::LT => (Unbounded, Excluded(value + interval)),
        ConstraintOp::LE => (Unbounded, Included(value + interval)),
        ConstraintOp::Eq => (Included(value), Included(value)),
        _ => (Unbounded, Unbounded), // Default case
    }
}
/// Chooses the less restrictive (broader) of two bounds.
///
/// Parameters:
/// - `a`: The first bound to compare.
/// - `b`: The second bound to compare.
///
/// Returns:
/// - The less restrictive bound.
fn less_restrictive_bound(a: Bound<i64>, b: Bound<i64>) -> Bound<i64> {
    match (a, b) {
        (Unbounded, _) | (_, Unbounded) => Unbounded,
        (Included(a_val), Included(b_val)) => Included(min(a_val, b_val)),
        (Excluded(a_val), Excluded(b_val)) => Excluded(min(a_val, b_val)),
        (Excluded(a_val), Included(b_val)) | (Included(a_val), Excluded(b_val)) => {
            if a_val <= b_val {
                Included(min(a_val, b_val))
            } else {
                Excluded(min(a_val, b_val))
            }
        }
    }
}

/// Chooses the more restrictive (narrower) of two bounds.
///
/// Parameters:
/// - `a`: The first bound to compare.
/// - `b`: The second bound to compare.
///
/// Returns:
/// - The more restrictive bound.
fn more_restrictive_bound(a: Bound<i64>, b: Bound<i64>) -> Bound<i64> {
    match (a, b) {
        (Unbounded, _) => b,
        (_, Unbounded) => a,
        (Included(a_val), Included(b_val)) => Included(max(a_val, b_val)),
        (Excluded(a_val), Excluded(b_val)) => Excluded(max(a_val, b_val)),
        (Excluded(a_val), Included(b_val)) | (Included(a_val), Excluded(b_val)) => {
            if a_val >= b_val {
                Excluded(a_val)
            } else {
                Included(b_val)
            }
        }
    }
}
