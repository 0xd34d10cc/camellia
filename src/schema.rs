use std::fmt::Display;

use serde::{Deserialize, Serialize};
use sqlparser::ast;

use crate::types::{type_of, BoxError, Result, Row};

// TODO: decouple type-level schema from table schema
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    // Some(index) => index into columns
    // None => hidden primary key
    pub primary_key: Option<usize>,
    pub columns: Vec<Column>,
}

impl Schema {
    pub fn empty() -> Self {
        Schema {
            primary_key: None,
            columns: Vec::new(),
        }
    }

    pub fn new(desc: Vec<ast::ColumnDef>) -> Result<Self> {
        let mut columns = Vec::with_capacity(desc.len());
        let mut primary_key = None;
        for (i, column) in desc.into_iter().enumerate() {
            if is_primary_key(&column) && primary_key.replace(i).is_some() {
                return Err("At most one column of table must be marked as primary key".into());
            }

            columns.push(Column::try_from(column)?);
        }

        Ok(Schema {
            primary_key,
            columns,
        })
    }

    pub fn columns(&self) -> impl Iterator<Item = &Column> {
        self.columns.iter()
    }

    pub fn check_compatible(&self, other: &Schema) -> Result<()> {
        if self.columns.len() != other.columns.len() {
            return Err(format!(
                "Number of columns does not match: expected {} but got {}",
                self.columns.len(),
                other.columns.len()
            )
            .into());
        }

        for (this, other) in self.columns.iter().zip(other.columns()) {
            if this.type_ != other.type_ {
                return Err(format!(
                    "Column {} type mismatch: expected {} but got {}",
                    this.name, this.type_, other.type_
                ).into());
            }
        }

        Ok(())
    }

    pub fn check(&self, row: &Row) -> Result<()> {
        if row.len() != self.columns.len() {
            return Err(format!(
                "number of fields does not match: expected {} but got {}",
                self.columns.len(),
                row.len()
            )
            .into());
        }

        for (column, value) in self.columns.iter().zip(row.values()) {
            let value_type = value.type_();
            if value_type != column.type_ && value_type != Type::Null {
                return Err(format!(
                    "{} field type does not match: expected {} but got {}",
                    column.name, column.type_, value_type
                )
                .into());
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub type_: Type,
}

impl TryFrom<ast::ColumnDef> for Column {
    type Error = BoxError;

    fn try_from(column: ast::ColumnDef) -> Result<Self> {
        let type_ = type_of(&column)?;
        let name = column.name.value;
        Ok(Column { name, type_ })
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize)]
pub enum Type {
    Null,
    Bool,
    Integer,
    Text,
}

impl Type {
    pub fn convertable_to(&self, type_: Type) -> bool {
        if *self == type_ {
            return true;
        }

        match self {
            Type::Null => type_ == Type::Null,
            Type::Bool => type_ == Type::Integer,
            Type::Integer => type_ == Type::Bool,
            Type::Text => false,
        }
    }
}

impl Display for Type {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Type::Null => "null",
            Type::Bool => "bool",
            Type::Integer => "int",
            Type::Text => "text",
        };

        f.write_str(s)
    }
}

fn is_primary_key(column: &ast::ColumnDef) -> bool {
    column.options.iter().any(|option| {
        matches!(
            option.option,
            ast::ColumnOption::Unique { is_primary: true }
        )
    })
}
