use sqlparser::ast;

use crate::expression::Expression;
use crate::schema::{Column, Schema};
use crate::types::{Result, Row};

use super::{Operation, Output};

pub struct Eval<'txn> {
    inner: Box<dyn Operation + 'txn>,

    schema: Schema,
    expressions: Vec<Expression>,
}

impl<'txn> Eval<'txn> {
    pub fn new(exprs: Vec<ast::SelectItem>, inner: Box<dyn Operation + 'txn>) -> Result<Self> {
        let schema = inner.schema();
        let mut columns = Vec::with_capacity(exprs.len());
        let mut expressions = Vec::with_capacity(exprs.len());
        for item in exprs {
            match item {
                ast::SelectItem::Wildcard(ast::WildcardAdditionalOptions {
                    opt_except: None,
                    opt_exclude: None,
                    opt_rename: None,
                    opt_replace: None,
                }) => {
                    for (i, column) in schema.columns().enumerate() {
                        columns.push(column.clone());
                        expressions.push(Expression::Field(i));
                    }
                }
                ast::SelectItem::UnnamedExpr(expr) => {
                    let e = Expression::parse(expr, schema)?;
                    columns.push(Column {
                        name: "?column?".into(),
                        type_: e.result_type(schema)?,
                    });
                    expressions.push(e);
                }
                ast::SelectItem::ExprWithAlias { expr, alias } => {
                    let e = Expression::parse(expr, schema)?;
                    columns.push(Column {
                        name: alias.to_string(),
                        type_: e.result_type(schema)?,
                    });
                    expressions.push(e);
                }
                _ => return Err("Unsupported projection type".into()),
            }
        }

        let schema = Schema {
            primary_key: None,
            columns,
        };

        Ok(Self {
            schema,
            expressions,
            inner,
        })
    }

    fn eval_on(&self, row: &mut Row) {
        // TODO: avoid allocation when possible?
        let mut mapped = Vec::with_capacity(self.expressions.len());
        for e in &self.expressions {
            // TODO: handle errors
            mapped.push(e.eval(row).unwrap());
        }
        *row = Row::from(mapped)
    }
}

impl<'txn> Operation for Eval<'txn> {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn poll(&mut self) -> Result<Output> {
        let output = match self.inner.poll()? {
            Output::Finished => Output::Finished,
            Output::Batch(mut rows) => {
                for row in rows.iter_mut() {
                    self.eval_on(row);
                }

                Output::Batch(rows)
            }
        };

        Ok(output)
    }
}
