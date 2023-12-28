use sqlparser::ast::{Expr, Ident, SelectItem, WildcardAdditionalOptions};

use crate::types::{Result, Row, Schema};

use super::{Operation, Output};

// TODO: get rid of lifetimes
pub struct Projection<'a> {
    inner: Box<dyn Operation + 'a>,

    schema: Schema,
    indexes: Vec<usize>,
}

impl<'a> Projection<'a> {
    pub fn new(projection: &[SelectItem], inner: Box<dyn Operation + 'a>) -> Result<Self> {
        let schema = inner.schema();
        let mut indexes = Vec::new();
        for item in projection {
            match item {
                SelectItem::Wildcard(WildcardAdditionalOptions {
                    opt_except: None,
                    opt_exclude: None,
                    opt_rename: None,
                    opt_replace: None,
                }) => indexes.extend(0..schema.columns.len()),
                SelectItem::UnnamedExpr(Expr::Identifier(Ident {
                    value,
                    quote_style: None,
                })) => {
                    let index = schema
                        .columns
                        .iter()
                        .position(|field| &field.name.value == value)
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

        let schema = Schema { columns };

        Ok(Self {
            schema,
            indexes,
            inner,
        })
    }

    fn apply_one(&self, row: &mut Row) {
        // TODO: avoid allocation?
        let mut projected = Vec::with_capacity(self.indexes.len());
        for &index in &self.indexes {
            projected.push(row.get(index).clone());
        }
        *row = Row::from(projected)
    }
}

impl<'a> Operation for Projection<'a> {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn poll(&mut self) -> Result<Output> {
        let output = match self.inner.poll()? {
            Output::Finished => Output::Finished,
            Output::Batch(mut rows) => {
                for row in rows.iter_mut() {
                    self.apply_one(row);
                }

                Output::Batch(rows)
            }
        };

        Ok(output)
    }
}
