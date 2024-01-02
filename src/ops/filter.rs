use sqlparser::ast::Expr;

use super::{Operation, Output};
use crate::expression::Expression;
use crate::schema::Schema;
use crate::types::Result;

pub struct Filter<'txn> {
    inner: Box<dyn Operation + 'txn>,

    filter: Expression,
}

impl<'txn> Filter<'txn> {
    pub fn new(selection: Expr, inner: Box<dyn Operation + 'txn>) -> Result<Self> {
        let schema = inner.schema();
        let filter = Expression::parse(selection, schema)?;
        Ok(Filter { inner, filter })
    }
}

impl<'txn> Operation for Filter<'txn> {
    fn schema(&self) -> &Schema {
        self.inner.schema()
    }

    fn poll(&mut self) -> Result<Output> {
        match self.inner.poll()? {
            Output::Batch(mut batch) => {
                // TODO: handle errors
                batch.retain(|row| self.filter.eval(row).unwrap().to_bool().unwrap());
                Ok(Output::Batch(batch))
            }
            Output::Finished => Ok(Output::Finished),
        }
    }
}
