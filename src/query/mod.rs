use std::{error::Error, str::FromStr};

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

#[derive(Debug, PartialEq, Eq)]
pub enum Value {
    Bool(bool),
    Int(i64),
    String(String),
}

#[derive(Debug, PartialEq, Eq)]
pub struct Create {
    pub table: Var,
    pub fields: Vec<Field>,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Type {
    Bool,
    Integer,
    Varchar(usize),
}

#[derive(Debug, PartialEq, Eq)]
pub struct Field {
    pub name: Var,
    pub type_: Type,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Drop {
    pub table: Var,
}
