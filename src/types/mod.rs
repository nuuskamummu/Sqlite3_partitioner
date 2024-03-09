use std::fmt::{self, Display};
use std::vec;

use crate::error::TableError;
use crate::shadow_tables::*;
pub use crate::utils::parse_value_type;
use crate::utils::value_type_to_string;
use serde::de::{self, EnumAccess, SeqAccess, VariantAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sqlite3_ext::Blob;
use sqlite3_ext::{vtab::ConstraintOp, Value, ValueType};

pub struct PartitionColumn(pub Option<ColumnDeclaration>);
impl FromIterator<ColumnDeclaration> for PartitionColumn {
    fn from_iter<T: IntoIterator<Item = ColumnDeclaration>>(iter: T) -> Self {
        let column = iter
            .into_iter()
            .find(|col_def| col_def.is_partition_column());
        Self(column)
    }
}
impl PartitionColumn {
    pub fn column_def(&self) -> &Option<ColumnDeclaration> {
        &self.0
    }
    pub fn new(column_declaration: ColumnDeclaration) -> Self {
        Self(Some(column_declaration))
    }
}
impl From<ColumnDeclaration> for PartitionColumn {
    fn from(value: ColumnDeclaration) -> Self {
        Self::new(value)
    }
}
impl<'a> From<&'a ColumnDeclaration> for PartitionColumn {
    fn from(value: &'a ColumnDeclaration) -> Self {
        PartitionColumn::new(value.clone())
    }
}

#[derive(Clone, Debug)]
pub struct ColumnDeclaration {
    name: String,
    data_type: ValueType,
    is_partition_column: bool,
}

impl ColumnDeclaration {
    pub const fn new(name: String, data_type: ValueType) -> Self
    where
        Self: Sized,
    {
        Self {
            name,
            data_type,
            is_partition_column: false,
        }
    }

    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn get_type(&self) -> &str {
        value_type_to_string(self.data_type())
    }
    pub fn data_type(&self) -> &ValueType {
        &self.data_type
    }
    pub fn is_partition_column(&self) -> bool {
        self.is_partition_column
    }
}

impl<'a> TryFrom<&'a str> for ColumnDeclaration {
    type Error = TableError;
    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        let tokens: Vec<&str> = value.split_whitespace().collect();
        let mut is_partition_column = false;

        if tokens.len() != 2 {
            if tokens.len() == 3 && tokens[2] == "partition_column" {
                is_partition_column = true;
            } else {
                return Err(TableError::ColumnDeclaration(format!(
                    "Invalid source string: {}. Expected format 'name type'",
                    value
                )));
            }
        }

        Ok(Self {
            name: tokens[0].trim().to_string(),
            data_type: parse_value_type(&tokens[1].trim().to_uppercase())?,
            is_partition_column,
        })
    }
}

// impl<'a> TryFrom<&'a [&'a str]> for ColumnDeclaration {
//     type Error = TableError;
//     fn try_from(value: &'a [&'a str]) -> Result<Self, Self::Error> {
//         let columns: String = value
//             .iter()
//             .map(|&col_arg| col_arg.into())
//             .collect::<Vec<String>>()
//             .join(" ");
//         ColumnDeclaration::try_from(&columns)
//     }
// }

impl Display for ColumnDeclaration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{} {}", self.get_name(), self.get_type()))
    }
}

#[derive(Clone, Debug)]
pub struct ColumnDeclarations(pub Vec<ColumnDeclaration>);
impl<'a> FromIterator<&'a str> for ColumnDeclarations {
    fn from_iter<T: IntoIterator<Item = &'a str>>(iter: T) -> Self {
        let columns: Vec<ColumnDeclaration> = iter
            .into_iter()
            .filter_map(|column_arg| match ColumnDeclaration::try_from(column_arg) {
                Ok(column) => Some(column),
                Err(_) => None,
            })
            .collect();
        Self(columns)
    }
}
impl Into<String> for ColumnDeclarations {
    fn into(self) -> String {
        self.0
            .into_iter()
            .map::<String, _>(|col| col.to_string())
            .collect::<Vec<String>>()
            .join(" ,")
    }
}

impl Display for ColumnDeclarations {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s: String = self
            .0
            .iter()
            .map(|column_declaration| column_declaration.to_string())
            .collect::<Vec<String>>()
            .join(" ,");
        f.write_str(&s)
    }
}

impl IntoIterator for ColumnDeclarations {
    type Item = ColumnDeclaration;
    type IntoIter = vec::IntoIter<Self::Item>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}
impl<'a> Into<&'a [ColumnDeclaration]> for &'a ColumnDeclarations {
    fn into(self) -> &'a [ColumnDeclaration] {
        &self.0
    }
}
pub struct CreateTableArgs {
    pub table_name: String,
    pub columns: Vec<ColumnDeclaration>,
    pub partition_column: ColumnDeclaration,
}
impl CreateTableArgs {}
pub struct PartitionArgs {
    pub name: String,
    pub columns: Vec<String>,
}

#[derive(Debug)]
pub struct Partition<T> {
    pub name: String,
    pub columns: Vec<ColumnDeclaration>,
    // pub interval: i64,
    pub root: RootTable,
    pub lookup: LookupTable<T>,
    pub template: TemplateTable,
}

pub enum PartitionDef {
    RangePartition(Partition<i64>),
}
pub trait PartitionAccessor<T> {
    fn get_template(&self) -> &TemplateTable;
    fn get_root(&self) -> &RootTable;
    fn get_lookup(&self) -> &LookupTable<T>;
    fn new(
        name: &str,
        columns: Vec<ColumnDeclaration>,
        root: RootTable,
        lookup: LookupTable<T>,
        template: TemplateTable,
    ) -> Self;
}

impl<T> PartitionAccessor<T> for Partition<T> {
    fn get_root(&self) -> &RootTable {
        &self.root
    }
    fn get_lookup(&self) -> &LookupTable<T> {
        &self.lookup
    }
    fn get_template(&self) -> &TemplateTable {
        &self.template
    }
    fn new(
        name: &str,
        columns: Vec<ColumnDeclaration>,
        root: RootTable,
        lookup: LookupTable<T>,
        template: TemplateTable,
    ) -> Self {
        Self {
            name: name.to_string(),
            columns,
            root,
            lookup,
            template,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Clone, Copy)]
#[serde(remote = "ConstraintOp")]
pub enum ConstraintOpDef {
    Eq,
    GT,
    LE,
    LT,
    GE,
    Match,
    Like,
    Glob,
    Regexp,
    NE,
    IsNot,
    IsNotNull,
    IsNull,
    Is,
    Limit,
    Offset,
    Function(u8),
}

#[derive(Debug, PartialEq, Clone)]
pub struct BlobWrapper(Vec<u8>);

impl From<&Blob> for BlobWrapper {
    fn from(blob: &Blob) -> Self {
        BlobWrapper(blob.as_slice().to_vec())
    }
}

impl Into<Blob> for BlobWrapper {
    fn into(self) -> Blob {
        Blob::from(self.0.as_slice())
    }
}
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(remote = "Value")]
pub enum ValueDef {
    Integer(i64),
    Float(f64),
    Text(String),
    #[serde(skip)]
    Blob(Blob),
    Null,
}
impl<'de> Deserialize<'de> for BlobWrapper {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct BlobVisitor;

        impl<'de> Visitor<'de> for BlobVisitor {
            type Value = BlobWrapper;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a byte array")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<BlobWrapper, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut bytes = Vec::new();
                while let Some(byte) = seq.next_element()? {
                    bytes.push(byte);
                }
                // Assume Blob::from(&[u8]) is available for converting Vec<u8> to Blob
                Ok(BlobWrapper(bytes))
            }
        }

        deserializer.deserialize_byte_buf(BlobVisitor)
    }
}
impl Serialize for BlobWrapper {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Convert Blob to Vec<u8> and serialize it
        serializer.serialize_bytes(self.0.as_slice())
    }
}
impl Serialize for ValueDef {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            ValueDef::Integer(i) => {
                serializer.serialize_newtype_variant("ValueDef", 0, "Integer", i)
            }
            ValueDef::Float(f) => serializer.serialize_newtype_variant("ValueDef", 1, "Float", f),
            ValueDef::Text(t) => serializer.serialize_newtype_variant("ValueDef", 2, "Text", t),
            ValueDef::Blob(b) => {
                // Convert Blob to SerializableBlob for serialization
                let serializable_blob = BlobWrapper::from(b);
                // Serialize SerializableBlob instead of Blob directly
                serializer.serialize_newtype_variant("ValueDef", 3, "Blob", &serializable_blob)
            }
            ValueDef::Null => serializer.serialize_unit_variant("ValueDef", 4, "Null"),
        }
    }
}

impl<'de> Deserialize<'de> for ValueDef {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ValueDefVisitor;

        impl<'de> Visitor<'de> for ValueDefVisitor {
            type Value = ValueDef;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("an enum representing different types of SQL values")
            }

            fn visit_enum<A>(self, access: A) -> Result<Self::Value, A::Error>
            where
                A: EnumAccess<'de>,
            {
                let (key, variant) = access.variant()?;
                match key {
                    "Integer" => Ok(ValueDef::Integer(variant.newtype_variant()?)),
                    "Float" => Ok(ValueDef::Float(variant.newtype_variant()?)),
                    "Text" => Ok(ValueDef::Text(variant.newtype_variant()?)),
                    "Blob" => {
                        let blob_wrapper: BlobWrapper = variant.newtype_variant()?;
                        Ok(ValueDef::Blob(blob_wrapper.into()))
                    }
                    "Null" => Ok(ValueDef::Null),
                    _ => Err(de::Error::unknown_variant(key, VARIANTS)),
                }
            }
        }

        const VARIANTS: &[&str] = &["Integer", "Float", "Text", "Blob", "Null"];
        deserializer.deserialize_enum("ValueDef", VARIANTS, ValueDefVisitor)
    }
}
impl From<Value> for ValueDef {
    fn from(value: Value) -> Self {
        match value {
            Value::Integer(i) => ValueDef::Integer(i),
            Value::Float(f) => ValueDef::Float(f),
            Value::Text(t) => ValueDef::Text(t),
            Value::Blob(b) => ValueDef::Blob(b),
            Value::Null => ValueDef::Null,
        }
    }
}
impl Into<Value> for ValueDef {
    fn into(self) -> Value {
        match self {
            ValueDef::Integer(i) => Value::Integer(i),
            ValueDef::Float(f) => Value::Float(f),
            ValueDef::Text(t) => Value::Text(t),
            ValueDef::Blob(b) => Value::Blob(b),
            ValueDef::Null => Value::Null,
        }
    }
}

pub fn option_value_to_option_serializable_value(option_value: Option<Value>) -> Option<ValueDef> {
    option_value.map(|value| value.into())
}

// Convert Option<SerializableValue> to Option<Value>
pub fn option_serializable_value_to_option_value(
    option_serializable_value: Option<ValueDef>,
) -> Option<Value> {
    option_serializable_value.map(|serializable_value| serializable_value.into())
}

impl Display for ConstraintOpDef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConstraintOpDef::Eq => write!(f, "="),
            ConstraintOpDef::GT => write!(f, ">"),
            ConstraintOpDef::LE => write!(f, "<="),
            ConstraintOpDef::LT => write!(f, "<"),
            ConstraintOpDef::GE => write!(f, ">="),
            ConstraintOpDef::Match => write!(f, "MATCH"),
            ConstraintOpDef::Like => write!(f, "LIKE"),
            ConstraintOpDef::Glob => write!(f, "GLOB"),
            ConstraintOpDef::Regexp => write!(f, "REGEXP"),
            ConstraintOpDef::NE => write!(f, "!="),
            ConstraintOpDef::IsNot => write!(f, "IS NOT"),
            ConstraintOpDef::IsNotNull => write!(f, "IS NOT NULL"),
            ConstraintOpDef::IsNull => write!(f, "IS NULL"),
            ConstraintOpDef::Is => write!(f, "IS"),
            ConstraintOpDef::Limit => write!(f, "LIMIT"),
            ConstraintOpDef::Offset => write!(f, "OFFSET"),
            ConstraintOpDef::Function(arg) => write!(f, "FUNCTION({})", arg),
        }
    }
}
impl From<ConstraintOpDef> for ConstraintOp {
    fn from(def: ConstraintOpDef) -> Self {
        match def {
            ConstraintOpDef::Eq => ConstraintOp::Eq,
            ConstraintOpDef::GT => ConstraintOp::GT,
            ConstraintOpDef::LE => ConstraintOp::LE,
            ConstraintOpDef::LT => ConstraintOp::LT,
            ConstraintOpDef::GE => ConstraintOp::GE,
            ConstraintOpDef::Match => ConstraintOp::Match,
            ConstraintOpDef::Like => ConstraintOp::Like,
            ConstraintOpDef::Glob => ConstraintOp::Glob,
            ConstraintOpDef::Regexp => ConstraintOp::Regexp,
            ConstraintOpDef::NE => ConstraintOp::NE,
            ConstraintOpDef::IsNot => ConstraintOp::IsNot,
            ConstraintOpDef::IsNotNull => ConstraintOp::IsNotNull,
            ConstraintOpDef::IsNull => ConstraintOp::IsNull,
            ConstraintOpDef::Is => ConstraintOp::Is,
            ConstraintOpDef::Limit => ConstraintOp::Limit,
            ConstraintOpDef::Offset => ConstraintOp::Offset,
            ConstraintOpDef::Function(value) => ConstraintOp::Function(value),
        }
    }
}
impl From<ConstraintOp> for ConstraintOpDef {
    fn from(op: ConstraintOp) -> Self {
        match op {
            ConstraintOp::Eq => ConstraintOpDef::Eq,
            ConstraintOp::GT => ConstraintOpDef::GT,
            ConstraintOp::LE => ConstraintOpDef::LE,
            ConstraintOp::LT => ConstraintOpDef::LT,
            ConstraintOp::GE => ConstraintOpDef::GE,
            ConstraintOp::Match => ConstraintOpDef::Match,
            ConstraintOp::Like => ConstraintOpDef::Like,
            ConstraintOp::Glob => ConstraintOpDef::Glob,
            ConstraintOp::Regexp => ConstraintOpDef::Regexp,
            ConstraintOp::NE => ConstraintOpDef::NE,
            ConstraintOp::IsNot => ConstraintOpDef::IsNot,
            ConstraintOp::IsNotNull => ConstraintOpDef::IsNotNull,
            ConstraintOp::IsNull => ConstraintOpDef::IsNull,
            ConstraintOp::Is => ConstraintOpDef::Is,
            ConstraintOp::Limit => ConstraintOpDef::Limit,
            ConstraintOp::Offset => ConstraintOpDef::Offset,
            ConstraintOp::Function(value) => ConstraintOpDef::Function(value),
        }
    }
}
