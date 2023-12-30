use crate::types::{Result, Row, Value};

pub struct TempStorage {
    // TODO: use disk backed storage to avoid OOM
    memory: Vec<Row>,
}

impl TempStorage {
    pub fn new() -> Result<Self> {
        Ok(TempStorage { memory: Vec::new() })
    }

    pub fn append(&mut self, mut rows: Vec<Row>) {
        self.memory.append(&mut rows);
    }

    pub fn sort_by<F>(&mut self, f: F)
    where
        F: FnMut(&Row) -> Value,
    {
        self.memory.sort_unstable_by_key(f);
    }

    pub fn into_iter(self) -> Box<dyn Iterator<Item = Row>> {
        Box::new(self.memory.into_iter())
    }
}
