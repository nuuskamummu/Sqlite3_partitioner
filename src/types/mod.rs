use std::fmt::{self, Display};

use crate::shadow_tables::*;
pub use crate::utils::parse_value_type;
use chrono::Offset;
use sqlite3_ext::{vtab::ConstraintOp, ValueType};
#[derive(Clone)]
pub struct ColumnDeclaration {
    name: String,
    data_type: ValueType,
    data_type_str: String,
}

impl ColumnDeclaration {
    // Constructor returning a Result
    pub fn new(source: &str) -> Result<Self, String> {
        let tokens: Vec<&str> = source.split_whitespace().collect();

        println!("{}", source);
        if tokens.len() != 2 {
            return Err(format!(
                "Invalid source string: {}. Expected format 'name type'",
                source
            )
            .to_string());
        }

        Ok(Self {
            name: tokens[0].trim().to_string(),
            data_type: parse_value_type(&tokens[1].trim().to_uppercase())?,
            data_type_str: tokens[1].trim().to_string(),
        })
    }

    // Getters
    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn get_type(&self) -> &str {
        &self.data_type_str
    }
    pub fn get_value_type(&self) -> &ValueType {
        &self.data_type
    }
}
pub struct CreateTableArgs {
    pub table_name: String,
    pub columns: Vec<ColumnDeclaration>,
}
pub struct PartitionArgs {
    pub name: String,
    pub columns: Vec<String>,
}
pub struct RangePartition {
    pub name: String,
    pub columns: Vec<ColumnDeclaration>,
    pub interval: i64,
    pub root: RootTable,
    pub lookup: LookupTable,
    pub template: TemplateTable,
}

pub enum PartitionFactory {
    Range(PartitionArgs),
}
pub enum Partition {
    Range(RangePartition),
}
pub trait PartitionAccessor {
    fn get_template(&self) -> &TemplateTable;
    fn get_root(&self) -> &RootTable;
    fn get_lookup(&self) -> &LookupTable;
}

impl<'a> PartitionAccessor for RangePartition {
    fn get_root(&self) -> &RootTable {
        &self.root
    }
    fn get_lookup(&self) -> &LookupTable {
        &self.lookup
    }
    fn get_template(&self) -> &TemplateTable {
        &self.template
    }
}

pub struct ConstraintOperators(pub ConstraintOp);
pub struct RangeOperators(pub ConstraintOp);
impl Display for ConstraintOperators {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            ConstraintOp::Eq => write!(f, "="),
            ConstraintOp::GT => write!(f, ">"),
            ConstraintOp::LE => write!(f, "<="),
            ConstraintOp::LT => write!(f, "<"),
            ConstraintOp::GE => write!(f, ">="),
            ConstraintOp::Match => write!(f, "MATCH"),
            ConstraintOp::Like => write!(f, "LIKE"),
            ConstraintOp::Glob => write!(f, "GLOB"),
            ConstraintOp::Regexp => write!(f, "REGEXP"),
            ConstraintOp::NE => write!(f, "!="),
            ConstraintOp::IsNot => write!(f, "IS NOT"),
            ConstraintOp::IsNotNull => write!(f, "IS NOT NULL"),
            ConstraintOp::IsNull => write!(f, "IS NULL"),
            ConstraintOp::Is => write!(f, "IS"),
            ConstraintOp::Limit => write!(f, "LIMIT"),
            ConstraintOp::Offset => write!(f, "OFFSET"),
            ConstraintOp::Function(arg) => write!(f, "FUNCTION({})", arg),
        }
    }
}
impl Display for RangeOperators {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            ConstraintOp::Eq => write!(f, "Included"),
            ConstraintOp::GT => write!(f, "Excluded"),
            ConstraintOp::LE => write!(f, "Included"),
            ConstraintOp::LT => write!(f, "Excluded"),
            ConstraintOp::GE => write!(f, "Included"),

            _ => Err(fmt::Error),
        }
    }
}
