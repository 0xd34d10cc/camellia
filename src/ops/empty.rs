use std::borrow::Cow;

use crate::schema::Schema;
use crate::types::{Result, Row};

use super::{Operation, Output};

// Source which produces single empty row
pub struct Empty {
    schema: Schema,
    empty: bool,
}

impl Empty {
    pub fn new() -> Self {
        Empty {
            schema: Schema {
                primary_key: None,
                columns: Vec::new(),
            },
            empty: false,
        }
    }
}

impl Operation for Empty {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    #[minitrace::trace]
    fn poll(&mut self) -> Result<Output> {
        match std::mem::replace(&mut self.empty, true) {
            true => Ok(Output::Finished),
            false => {
                let empty_row = Row::from(Vec::new());
                minitrace::Event::add_to_local_parent("batch", || {
                    [(Cow::Borrowed("size"), Cow::Borrowed("1"))]
                });
                Ok(Output::Batch(vec![empty_row]))
            }
        }
    }
}
