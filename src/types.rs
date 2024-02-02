use std::fmt::Display;

use serde::{Deserialize, Serialize};
use sqlparser::ast::{self, ColumnDef};

use crate::schema::{Schema, Type};

pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type Result<T> = std::result::Result<T, BoxError>;
pub type Database = rocksdb::TransactionDB<rocksdb::MultiThreaded>;

#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub struct Row(Vec<Value>);

impl Row {
    pub fn deserialize(bytes: &[u8], _schema: &Schema) -> Result<Self> {
        let row: Vec<Value> = bincode::deserialize(bytes)?;
        Ok(Row(row))
    }

    pub fn serialize(&self, bytes: &mut Vec<u8>) -> Result<()> {
        bincode::serialize_into(bytes, &self.0)?;
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn values(&self) -> impl Iterator<Item = &Value> {
        self.0.iter()
    }

    pub fn get(&self, i: usize) -> &Value {
        &self.0[i]
    }
}

impl From<Vec<Value>> for Row {
    fn from(value: Vec<Value>) -> Self {
        Row(value)
    }
}

pub struct RowSet {
    pub schema: Schema,
    pub rows: Vec<Row>,
}

impl Display for RowSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use comfy_table::Table;

        let mut table = Table::new();
        let header: Vec<_> = self
            .schema
            .columns()
            .map(|column| column.name.clone())
            .collect();
        table.set_header(header);
        for row in &self.rows {
            let row: Vec<_> = row.values().map(|value| value.to_string()).collect();
            table.add_row(row);
        }

        write!(f, "{}", table)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    String(String),
}

impl Value {
    pub fn try_from(value: ast::Value) -> Result<Value> {
        let value = match value {
            ast::Value::Null => Value::Null,
            ast::Value::Boolean(val) => Value::Bool(val),
            ast::Value::Number(number, false) => Value::Int(number.parse::<i64>()?),
            ast::Value::SingleQuotedString(string) => Value::String(string),
            _ => return Err("Unsupported value type".into()),
        };

        Ok(value)
    }

    pub fn type_(&self) -> Type {
        match self {
            Value::Null => Type::Null,
            Value::Bool(_) => Type::Bool,
            Value::Int(_) => Type::Integer,
            Value::String(_) => Type::Text,
        }
    }

    pub fn add(&self, right: Value) -> Result<Value> {
        let left = self.to_int().ok_or("Invalid ADD")?;
        let right = right.to_int().ok_or("Invalid ADD")?;
        let result = left.checked_add(right).ok_or("Integer overflow on ADD")?;
        Ok(Value::Int(result))
    }

    pub fn sub(&self, right: Value) -> Result<Value> {
        let left = self.to_int().ok_or("Invalid SUB")?;
        let right = right.to_int().ok_or("Invalid SUB")?;
        let result = left.checked_sub(right).ok_or("Integer overflow on SUB")?;
        Ok(Value::Int(result))
    }

    pub fn mul(&self, right: Value) -> Result<Value> {
        let left = self.to_int().ok_or("Invalid MUL")?;
        let right = right.to_int().ok_or("Invalid MUL")?;
        let result = left.checked_mul(right).ok_or("Integer overflow on MUL")?;
        Ok(Value::Int(result))
    }

    pub fn div(&self, right: Value) -> Result<Value> {
        let left = self.to_int().ok_or("Invalid DIV")?;
        let right = right.to_int().ok_or("Invalid DIV")?;
        let result = left.checked_div(right).ok_or("Integer overflow on DIV")?;
        Ok(Value::Int(result))
    }

    pub fn and(&self, right: Value) -> Result<Value> {
        let left = self.to_bool().ok_or("Invalid AND")?;
        let right = right.to_bool().ok_or("Invalid AND")?;
        Ok(Value::Bool(left && right))
    }

    pub fn or(&self, right: Value) -> Result<Value> {
        let left = self.to_bool().ok_or("Invalid OR")?;
        let right = right.to_bool().ok_or("Invalid OR")?;
        Ok(Value::Bool(left || right))
    }

    pub fn to_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(val) => Some(*val),
            Value::Int(val) => Some(*val != 0),
            _ => None,
        }
    }

    pub fn to_int(&self) -> Option<i64> {
        match self {
            Value::Bool(val) => Some(*val as i64),
            Value::Int(val) => Some(*val),
            _ => None,
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "null"),
            Value::Bool(val) => write!(f, "{}", val),
            Value::Int(val) => write!(f, "{}", val),
            Value::String(val) => write!(f, "{}", val),
        }
    }
}

pub fn type_of(column: &ColumnDef) -> Result<Type> {
    match column.data_type {
        ast::DataType::Bool | ast::DataType::Boolean => Ok(Type::Bool),
        ast::DataType::Int(None) | ast::DataType::Integer(None) => Ok(Type::Integer),
        ast::DataType::Text => Ok(Type::Text),
        _ => Err("Unsupported column type".into()),
    }
}
