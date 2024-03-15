use std::{
    cmp::{max, min},
    collections::HashMap,
    i64,
};

use chrono::{NaiveDate, NaiveDateTime};
use regex::Regex;
use sqlite3_ext::{ffi::SQLITE_FORMAT, vtab::ConstraintOp, FromValue, Value, ValueRef, ValueType};

use crate::{constraints::Condition, error::TableError};

pub fn parse_partition_value(value: &ValueRef, interval: i64) -> sqlite3_ext::Result<i64> {
    parse_to_unix_epoch(value).map(|epoch| epoch - epoch % interval)
}
/// Converts a [`sqlite3_ext::ValueType`] to a [&`str`]
pub fn value_type_to_string(value_type: &ValueType) -> &'static str {
    match value_type {
        ValueType::Integer => "INTEGER",
        ValueType::Blob => "BLOB",
        ValueType::Text => "TEXT",
        ValueType::Null => "NULL",
        ValueType::Float => "FLOAT",
    }
}
/// Converts a [str] to a [`sqlite3_ext::ValueType`]
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

/// Parses an incoming [sqlite3_ext::ValueType] to UNIX epoch.
// TODO improve!
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
/// Accepts a str with the format ["Numeric part Unit part"] where unit part is either [Hour] or
/// [Minute]. The result is a i64 representation of the interval in seconds.
// TODO better documentation, handle more cases.
pub fn parse_interval(interval_str: &str) -> Result<i64, TableError> {
    // Initialize the Regex pattern
    let re = Regex::new(r"(\d+)\s+(\w+)")
        .map_err(|_| TableError::ParseInterval("Failed to compile regex pattern.".to_string()))?;

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

    Ok(numeric_value * size_in_seconds)
}

use std::ops::Bound::{self, *};

//
/// Aggregates conditions into ranges for each column.
///
/// Parameters:
/// - `conditions`: A vector of conditions to be aggregated into ranges.
///
/// Returns:
/// - A HashMap where each key is a column name and its value is a tuple representing the range as lower and upper bounds.
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
fn initial_bound(operator: &ConstraintOp, value: i64, interval: i64) -> (Bound<i64>, Bound<i64>) {
    match operator {
        ConstraintOp::GT | ConstraintOp::GE => (Excluded(value), Unbounded),
        ConstraintOp::LT => (Unbounded, Excluded(value + interval)),
        ConstraintOp::LE => (Unbounded, Included(value + interval)),
        ConstraintOp::Eq => (Included(value), Included(value)),
        _ => (Unbounded, Unbounded), // Default case
    }
}
// Choose the less restrictive (i.e., broader) lower bound
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

// Keep the existing logic for more restrictive bounds as it correctly narrows down the range
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
