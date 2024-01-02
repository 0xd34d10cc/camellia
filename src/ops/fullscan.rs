use rocksdb::{DBIteratorWithThreadMode, Transaction};

use crate::schema::Schema;
use crate::types::{Database, Result, Row};

use super::{Operation, Output};

// TODO: get rid of lifetimes?
pub struct FullScan<'txn> {
    schema: Schema,
    iter: DBIteratorWithThreadMode<'txn, Transaction<'txn, Database>>,
}

impl<'txn> FullScan<'txn> {
    pub fn new(
        schema: Schema,
        iter: DBIteratorWithThreadMode<'txn, Transaction<'txn, Database>>,
    ) -> Result<Self> {
        Ok(FullScan { schema, iter })
    }
}

impl<'txn> Operation for FullScan<'txn> {
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
