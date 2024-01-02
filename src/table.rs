use std::sync::atomic::{AtomicU64, Ordering};

use crate::schema::Schema;
use crate::types::{Row, Value};

#[derive(Debug)]
pub struct Table {
    schema: Schema,

    hidden_pk: AtomicU64,
}

impl Table {
    pub fn new(schema: Schema, hidden_pk: u64) -> Self {
        Table {
            schema,
            hidden_pk: AtomicU64::new(hidden_pk),
        }
    }

    pub fn get_key(&self, row: &Row) -> Vec<u8> {
        let mut key = Vec::new();
        match self.schema.primary_key {
            None => {
                let bytes = self.hidden_pk.fetch_add(1, Ordering::Relaxed).to_be_bytes();
                key.extend_from_slice(&bytes);
            }
            Some(index) => match row.get(index) {
                Value::Bool(val) => key.push(*val as u8),
                Value::Int(val) => key.extend_from_slice(&val.to_be_bytes()),
                Value::String(val) => key.extend_from_slice(val.as_bytes()),
            },
        };
        key
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}
