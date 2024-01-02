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

    pub fn num_columns(&self) -> usize {
        self.columns.len()
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
            if value_type != column.type_ {
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

fn is_primary_key(column: &ast::ColumnDef) -> bool {
    column.options.iter().any(|option| {
        matches!(
            option.option,
            ast::ColumnOption::Unique { is_primary: true }
        )
    })
}
