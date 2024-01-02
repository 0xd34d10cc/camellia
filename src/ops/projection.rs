use sqlparser::ast::{Expr, Ident, SelectItem, WildcardAdditionalOptions};

use crate::schema::Schema;
use crate::types::{Result, Row};

use super::{Operation, Output};

pub struct Projection<'txn> {
    inner: Box<dyn Operation + 'txn>,

    schema: Schema,
    indexes: Vec<usize>,
}

impl<'txn> Projection<'txn> {
    pub fn new(projection: &[SelectItem], inner: Box<dyn Operation + 'txn>) -> Result<Self> {
        let schema = inner.schema();
        let mut indexes = Vec::new();
        for item in projection {
            match item {
                SelectItem::Wildcard(WildcardAdditionalOptions {
                    opt_except: None,
                    opt_exclude: None,
                    opt_rename: None,
                    opt_replace: None,
                }) => indexes.extend(0..schema.num_columns()),
                SelectItem::UnnamedExpr(Expr::Identifier(Ident {
                    value,
                    quote_style: None,
                })) => {
                    let index = schema
                        .columns()
                        .position(|field| field.name == *value)
                        .ok_or_else(|| format!("no such column: {}", value))?;
                    indexes.push(index);
                }
                _ => return Err("Unsupported projection type".into()),
            }
        }

        let mut columns = Vec::with_capacity(indexes.len());
        for &index in &indexes {
            columns.push(schema.columns[index].clone());
        }

        let schema = Schema {
            primary_key: None,
            columns,
        };

        Ok(Self {
            schema,
            indexes,
            inner,
        })
    }

    fn project(&self, row: &mut Row) {
        // TODO: avoid allocation?
        let mut projected = Vec::with_capacity(self.indexes.len());
        for &index in &self.indexes {
            projected.push(row.get(index).clone());
        }
        *row = Row::from(projected)
    }
}

impl<'txn> Operation for Projection<'txn> {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn poll(&mut self) -> Result<Output> {
        let output = match self.inner.poll()? {
            Output::Finished => Output::Finished,
            Output::Batch(mut rows) => {
                for row in rows.iter_mut() {
                    self.project(row);
                }

                Output::Batch(rows)
            }
        };

        Ok(output)
    }
}
