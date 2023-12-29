use std::fmt::Display;

use serde::{Deserialize, Serialize};
use sqlparser::ast::{self, ColumnDef};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync + 'static>>;
pub type Database = rocksdb::TransactionDB<rocksdb::MultiThreaded>;

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

    pub fn values(&self) -> impl Iterator<Item = &Value> {
        self.0.iter()
    }

    pub fn len(&self) -> usize {
        self.0.len()
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
            .columns
            .iter()
            .map(|field| field.name.clone())
            .collect();
        table.set_header(header);
        for row in &self.rows {
            let row: Vec<_> = row.values().map(|value| value.to_string()).collect();
            table.add_row(row);
        }

        write!(f, "{}", table)
    }
}

// TODO: decouple type system level schema from table schema
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    pub columns: Vec<ColumnDef>,
}

impl Schema {
    pub fn check(&self, row: &Row) -> Result<usize> {
        if row.len() != self.columns.len() {
            return Err(format!(
                "number of fields does not match: expected {} but got {}",
                self.columns.len(),
                row.len()
            )
            .into());
        }

        let mut primary_key = None;
        for (i, (column, value)) in self.columns.iter().zip(row.values()).enumerate() {
            let value_type = value.type_();
            let column_type = type_of(column)?;
            if value_type != column_type {
                return Err(format!(
                    "{} field type does not match: expected {} but got {}",
                    column.name, column_type, value_type
                )
                .into());
            }

            if is_primary_key(column) && primary_key.replace(i).is_some() {
                return Err("Duplicate primary key".into());
            }
        }

        let pk = primary_key.ok_or("No primary key in schema")?;
        Ok(pk)
    }
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Value {
    Bool(bool),
    Int(i64),
    String(String),
}

impl Value {
    pub fn try_from(value: ast::Value) -> Result<Value> {
        let value = match value {
            ast::Value::Boolean(val) => Value::Bool(val),
            ast::Value::Number(number, false) => Value::Int(number.parse::<i64>()?),
            ast::Value::SingleQuotedString(string) => Value::String(string),
            _ => return Err("Unsupported value type".into()),
        };

        Ok(value)
    }

    pub fn type_(&self) -> Type {
        match self {
            Value::Bool(_) => Type::Bool,
            Value::Int(_) => Type::Integer,
            Value::String(_) => Type::Text,
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Value::Bool(left), Value::Bool(right)) => Some(left.cmp(right)),
            (Value::Int(left), Value::Int(right)) => Some(left.cmp(right)),
            (Value::String(left), Value::String(right)) => Some(left.cmp(right)),
            _ => None,
        }
    }
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

fn type_of(column: &ColumnDef) -> Result<Type> {
    match column.data_type {
        ast::DataType::Bool | ast::DataType::Boolean => Ok(Type::Bool),
        ast::DataType::Int(None) => Ok(Type::Integer),
        ast::DataType::Text => Ok(Type::Text),
        _ => Err("Unsupported column type".into()),
    }
}

fn is_primary_key(column: &ColumnDef) -> bool {
    use sqlparser::ast::ColumnOption;

    column
        .options
        .iter()
        .any(|option| matches!(option.option, ColumnOption::Unique { is_primary: true }))
}
