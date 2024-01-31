use std::borrow::Cow;

use crate::expression::Expression;
use crate::schema::Schema;
use crate::types::{Result, Row};

use super::{Operation, Output};

pub struct Eval<'txn> {
    inner: Box<dyn Operation + 'txn>,

    schema: Schema,
    expressions: Vec<Expression>,
}

impl<'txn> Eval<'txn> {
    pub fn new(
        expressions: Vec<Expression>,
        schema: Schema,
        inner: Box<dyn Operation + 'txn>,
    ) -> Result<Self> {
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

    #[minitrace::trace]
    fn poll(&mut self) -> Result<Output> {
        let output = match self.inner.poll()? {
            Output::Finished => Output::Finished,
            Output::Batch(mut rows) => {
                for row in rows.iter_mut() {
                    self.eval_on(row);
                }

                minitrace::Event::add_to_local_parent("batch", || {
                    [(Cow::Borrowed("size"), Cow::Owned(format!("{}", rows.len())))]
                });
                Output::Batch(rows)
            }
        };

        Ok(output)
    }
}
