use rocksdb::{DBIteratorWithThreadMode, Transaction};

use crate::types::{Database, Result, Row, Schema};

use super::{Operation, Output};

// TODO: get rid of lifetimes
pub struct FullScan<'a> {
    schema: Schema,
    iter: DBIteratorWithThreadMode<'a, Transaction<'a, Database>>,
}

impl<'a> FullScan<'a> {
    pub fn new(
        schema: Schema,
        iter: DBIteratorWithThreadMode<'a, Transaction<'a, Database>>,
    ) -> Result<Self> {
        Ok(FullScan { schema, iter })
    }
}

impl<'a> Operation for FullScan<'a> {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn poll(&mut self) -> Result<Output> {
        const BATCH_SIZE: usize = 1024;

        let mut batch = Vec::with_capacity(BATCH_SIZE);
        loop {
            match self.iter.next() {
                Some(Ok((_key, value))) => {
                    let row: Row = Row::deserialize(&value, &self.schema)?;
                    batch.push(row);
                    if batch.len() >= BATCH_SIZE {
                        return Ok(Output::Batch(batch));
                    }
                }
                Some(Err(e)) => return Err(e.into()),
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
