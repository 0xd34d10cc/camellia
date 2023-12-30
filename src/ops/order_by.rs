use sqlparser::ast::OrderByExpr;

use super::{Operation, Output};
use crate::expression::Expression;
use crate::temp_storage::TempStorage;
use crate::types::{Result, Row, Schema};

pub struct OrderBy<'a> {
    inner: Box<dyn Operation + 'a>,

    by: Expression,
    storage: Option<TempStorage>,
    iter: Option<Box<dyn Iterator<Item = Row>>>,
}

impl<'a> OrderBy<'a> {
    pub fn new(by: OrderByExpr, inner: Box<dyn Operation + 'a>) -> Result<Self> {
        if let Some(false) = by.asc {
            return Err("DESC is not implemented".into());
        }

        if by.nulls_first.is_some() {
            return Err("NULLS FIRST is not implemented".into());
        }

        let schema = inner.schema();
        let by = Expression::parse(by.expr, schema)?;
        Ok(OrderBy {
            inner,
            by,
            storage: Some(TempStorage::new()?),
            iter: None,
        })
    }
}

impl<'a> Operation for OrderBy<'a> {
    fn schema(&self) -> &Schema {
        self.inner.schema()
    }

    fn poll(&mut self) -> Result<Output> {
        if let Some(mut storage) = self.storage.take() {
            // TODO: give control flow back every N rows?
            loop {
                match self.inner.poll()? {
                    Output::Batch(batch) => {
                        storage.append(batch);
                    }
                    Output::Finished => {
                        // TODO: handle error
                        storage.sort_by(|row| self.by.eval(row).unwrap());
                        self.iter = Some(storage.into_iter());
                        break;
                    }
                }
            }
        }

        const BATCH_SIZE: usize = 1024;
        let mut batch = Vec::with_capacity(BATCH_SIZE);
        let iter = self.iter.as_mut().unwrap();
        loop {
            match iter.next() {
                Some(row) => {
                    batch.push(row);
                    if batch.len() >= BATCH_SIZE {
                        return Ok(Output::Batch(batch));
                    }
                }
                None => {
                    if batch.is_empty() {
                        return Ok(Output::Finished);
                    } else {
                        return Ok(Output::Batch(batch));
                    }
                }
            }
        }
    }
}
