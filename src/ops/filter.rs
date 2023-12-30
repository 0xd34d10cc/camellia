use sqlparser::ast::Expr;

use super::{Operation, Output};
use crate::{
    expression::Expression,
    types::{Result, Schema},
};

pub struct Filter<'a> {
    inner: Box<dyn Operation + 'a>,

    filter: Expression,
}

impl<'a> Filter<'a> {
    pub fn new(selection: Expr, inner: Box<dyn Operation + 'a>) -> Result<Self> {
        let schema = inner.schema();
        let filter = Expression::parse(selection, schema)?;
        Ok(Filter { inner, filter })
    }
}

impl<'a> Operation for Filter<'a> {
    fn schema(&self) -> &Schema {
        self.inner.schema()
    }

    fn poll(&mut self) -> Result<Output> {
        match self.inner.poll()? {
            Output::Batch(mut batch) => {
                // TODO: handle errors
                batch.retain(|row| self.filter.eval(row).unwrap().as_bool().unwrap());
                Ok(Output::Batch(batch))
            }
            Output::Finished => Ok(Output::Finished),
        }
    }
}
