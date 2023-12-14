use std::{error::Error, fmt::Display, str::FromStr};

use serde::{Deserialize, Serialize};

mod parser;

pub type Var = String;

#[derive(Debug, PartialEq, Eq)]
pub enum Query {
    Select(Select),
    Insert(Insert),
    Create(Create),
    Drop(Drop),
}

impl FromStr for Query {
    type Err = Box<dyn Error>;

    fn from_str(query: &str) -> Result<Self, Self::Err> {
        parser::parse(query).map_err(|e| e.into())
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Select {
    pub selector: Selector,
    pub table: Var,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Selector {
    All,
    Fields(Vec<Var>),
}

#[derive(Debug, PartialEq, Eq)]
pub struct Insert {
    pub table: Var,
    pub values: Vec<Value>,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, PartialEq, Eq)]
pub struct Create {
    pub table: Var,
    pub fields: Vec<Field>,
}

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

impl Type {
    pub fn can_hold(self, other: Type) -> bool {
        // will be more complicated after varchar(n) and NULL implementation
        self == other
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Field {
    pub name: Var,
    pub type_: Type,
    pub primary_key: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Drop {
    pub table: Var,
}
