use std::fmt::{self, Display};

pub use crate::utils::parse_value_type;
use serde::de::{self, EnumAccess, SeqAccess, VariantAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sqlite3_ext::Blob;
use sqlite3_ext::{vtab::ConstraintOp, Value};

pub use self::column_declaration::{ColumnDeclaration, ColumnDeclarations, PartitionColumn};

mod column_declaration;
pub mod constraints;

pub struct CreateTableArgs {
    pub table_name: String,
    pub columns: Vec<ColumnDeclaration>,
    pub partition_column: ColumnDeclaration,
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

impl From<BlobWrapper> for Blob {
    fn from(value: BlobWrapper) -> Self {
        Blob::from(value.0.as_slice())
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
impl From<ValueDef> for Value {
    fn from(value: ValueDef) -> Self {
        match value {
            ValueDef::Integer(i) => Value::Integer(i),
            ValueDef::Float(f) => Value::Float(f),
            ValueDef::Text(t) => Value::Text(t),
            ValueDef::Blob(b) => Value::Blob(b),
            ValueDef::Null => Value::Null,
        }
    }
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
