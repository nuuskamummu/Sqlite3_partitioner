use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::rc::Rc;

#[derive(Clone, Debug)]
pub struct Column {
    name: String,
    data_type: String,
}

impl Column {
    // Constructor returning a Result
    pub fn new(source: &str) -> Result<Column, &'static str> {
        let tokens: Vec<&str> = source.split_whitespace().collect();

        if tokens.len() != 2 {
            return Err("Invalid source string: Expected format 'name type'");
        }

        Ok(Column {
            name: tokens[0].to_string(),
            data_type: tokens[1].to_string(),
        })
    }

    // Getters
    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn get_type(&self) -> &str {
        &self.data_type
    }
}

// Implementing PartialEq, Eq, and Hash for Column for comparison and usage in a HashSet
impl PartialEq for Column {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.data_type == other.data_type
    }
}

impl Eq for Column {}

impl Hash for Column {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.data_type.hash(state);
    }
}

pub type Columns = HashSet<Rc<Column>>;

pub fn collect(columns: &[Rc<Column>]) -> Columns {
    columns.iter().cloned().collect()
}

pub fn deep_compare_unordered_sets(first_set: &Columns, second_set: &Columns) -> bool {
    if first_set.len() != second_set.len() {
        return false;
    }

    first_set.iter().all(|column| second_set.contains(column))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_creation() {
        let col = Column::new("id int").expect("Failed to create Column");
        assert_eq!(col.get_name(), "id");
        assert_eq!(col.get_type(), "int");
    }

    #[test]
    #[should_panic(expected = "Invalid source string: Expected format 'name type'")]
    fn test_invalid_column_creation() {
        Column::new("invalid").expect("Invalid source string: Expected format 'name type'");
    }

    #[test]
    fn test_column_equality() {
        let col1 = Column::new("id int").expect("Failed to create Column");
        let col2 = Column::new("id int").expect("Failed to create Column");
        assert_eq!(col1, col2);
    }

    #[test]
    fn test_column_inequality() {
        let col1 = Column::new("id int").expect("Failed to create Column");
        let col2 = Column::new("name varchar").expect("Failed to create Column");
        assert_ne!(col1, col2);
    }

    // Additional tests can be added for collect and deep_compare_unordered_sets functions
}
