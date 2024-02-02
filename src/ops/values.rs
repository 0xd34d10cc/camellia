use super::{Operation, Output};
use crate::schema::Schema;
use crate::types::{Result, Row};

// Row stream that emits specified rows
pub struct Values {
    schema: Schema,
    values: std::vec::IntoIter<Row>,
}

impl Values {
    pub fn new(values: Vec<Row>, schema: Schema) -> Result<Self> {
        Ok(Values {
            schema,
            values: values.into_iter(),
        })
    }
}

impl Operation for Values {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn poll(&mut self) -> Result<Output> {
        const BATCH_SIZE: usize = 1024;

        let cap = std::cmp::min(self.values.len(), BATCH_SIZE);
        let mut batch = Vec::with_capacity(cap);
        for row in self.values.by_ref() {
            batch.push(row);

            if batch.len() >= BATCH_SIZE {
                return Ok(Output::Batch(batch));
            }
        }

        if batch.is_empty() {
            Ok(Output::Finished)
        } else {
            Ok(Output::Batch(batch))
        }
    }
}
