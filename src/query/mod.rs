use std::{error::Error, str::FromStr};

mod parser;

pub type Var = String;

#[derive(Debug, PartialEq, Eq)]
pub enum Query {
    Select(Select),
    Insert(Insert),
    Create(Create),
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
    table: Var,
    values: Vec<Value>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Value {
    Bool(bool),
    Int(i64),
    String(String),
}

#[derive(Debug, PartialEq, Eq)]
pub struct Create {
    table: Var,
    fields: Vec<Field>,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Type {
    Bool,
    Integer,
    Varchar(usize),
}

#[derive(Debug, PartialEq, Eq)]
pub struct Field {
    name: Var,
    type_: Type,
}
