use std::fmt::Display;

use serde::{Serialize, Deserialize};


#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize)]
pub enum Type {
    Bool,
    Integer,
    Text,
}

impl Display for Type {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Type::Bool => write!(f, "bool"),
            Type::Integer => write!(f, "int"),
            Type::Text => write!(f, "text"),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum Value {
    Bool(bool),
    Int(i64),
    String(String),
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Bool(val) => write!(f, "{}", val),
            Value::Int(val) => write!(f, "{}", val),
            Value::String(val) => write!(f, "{}", val),
        }
    }
}

impl Value {
    pub fn type_(&self) -> Type {
        match self {
            Value::Bool(_) => Type::Bool,
            Value::Int(_) => Type::Integer,
            Value::String(_) => Type::Text,
        }
    }
}
