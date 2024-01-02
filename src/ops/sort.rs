use std::collections::BinaryHeap;

use sqlparser::ast::OrderByExpr;

use super::{Operation, Output};
use crate::expression::Expression;
use crate::schema::Schema;
use crate::types::{Result, Row, Value};

enum State {
    Read,
    Merge,
    Emit,
}

// TODO: consider sorting on (key, row_id) instead of (key, row)
pub struct Sort<'txn> {
    inner: Box<dyn Operation + 'txn>,

    by: Expression,
    // TODO: use disk-backed storage for runs
    runs: Vec<Vec<Row>>,
    // TODO: use disk-backed storage for result
    results: std::vec::IntoIter<Row>,

    state: State,
}

impl<'txn> Sort<'txn> {
    pub fn new(by: OrderByExpr, inner: Box<dyn Operation + 'txn>) -> Result<Self> {
        if let Some(false) = by.asc {
            return Err("DESC is not implemented".into());
        }

        if by.nulls_first.is_some() {
            return Err("NULLS FIRST is not implemented".into());
        }

        let schema = inner.schema();
        let by = Expression::parse(by.expr, schema)?;
        Ok(Self {
            inner,
            by,
            runs: Vec::new(),
            results: Vec::new().into_iter(),

            state: State::Read,
        })
    }

    fn read(&mut self) -> Result<()> {
        loop {
            match self.inner.poll()? {
                Output::Batch(mut batch) => {
                    // TODO: handle errors
                    // TODO: batch can be small, use chunks of N
                    batch.sort_unstable_by_key(|row| self.by.eval(row).unwrap());
                    self.runs.push(batch);
                }
                Output::Finished => {
                    return Ok(());
                }
            }
        }
    }

    // NOTE: consumes internals of |runs|
    fn nway_merge(&self, runs: &mut [Vec<Row>]) -> Vec<Row> {
        struct Item {
            key: Value,
            row: Row,

            iter: std::vec::IntoIter<Row>,
        }

        impl PartialEq for Item {
            fn eq(&self, other: &Self) -> bool {
                self.key == other.key
            }
        }

        impl Eq for Item {}

        // we want min heap for sort
        #[allow(clippy::non_canonical_partial_ord_impl)]
        impl PartialOrd for Item {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.key.cmp(&other.key).reverse())
            }
        }

        impl Ord for Item {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                self.key.cmp(&other.key).reverse()
            }
        }

        let mut heap = BinaryHeap::new();
        for run in runs.iter_mut() {
            let mut iter = std::mem::take(run).into_iter();
            if let Some(row) = iter.next() {
                // TODO: handle error
                let key = self.by.eval(&row).unwrap();
                heap.push(Item { key, row, iter })
            }
        }

        let total_len = runs.iter().map(|run| run.len()).sum::<usize>();
        let mut sorted = Vec::with_capacity(total_len);
        while let Some(item) = heap.pop() {
            sorted.push(item.row);

            let mut iter = item.iter;
            if let Some(row) = iter.next() {
                // TODO: handle error
                let key = self.by.eval(&row).unwrap();
                heap.push(Item { key, row, iter });
            }
        }

        sorted
    }

    fn merge(&mut self) -> Result<()> {
        if self.runs.len() <= 1 {
            return Ok(());
        }

        let mut runs = std::mem::take(&mut self.runs);
        loop {
            const N: usize = 16;
            for chunk in runs.chunks_mut(N) {
                let merged = self.nway_merge(chunk);
                self.runs.push(merged);
            }

            runs.clear();
            if self.runs.len() <= 1 {
                break Ok(());
            }
            std::mem::swap(&mut self.runs, &mut runs);
        }
    }

    fn poll_batch(&mut self) -> Result<Output> {
        const BATCH_SIZE: usize = 1024;
        let mut chunk = Vec::with_capacity(BATCH_SIZE);
        loop {
            match self.results.next() {
                Some(row) => {
                    chunk.push(row);
                    if chunk.len() >= BATCH_SIZE {
                        return Ok(Output::Batch(chunk));
                    }
                }
                None => {
                    if chunk.is_empty() {
                        return Ok(Output::Finished);
                    } else {
                        return Ok(Output::Batch(chunk));
                    }
                }
            }
        }
    }
}

impl<'txn> Operation for Sort<'txn> {
    fn schema(&self) -> &Schema {
        self.inner.schema()
    }

    fn poll(&mut self) -> Result<Output> {
        loop {
            match self.state {
                State::Read => {
                    // TODO: give control flow back every N reads?
                    self.read()?;
                    self.state = State::Merge;
                }
                State::Merge => {
                    // TODO: give control flow back every N merges?
                    self.merge()?;
                    let runs = std::mem::take(&mut self.runs);
                    debug_assert!(runs.len() <= 1);
                    if let Some(all) = runs.into_iter().next() {
                        self.results = all.into_iter();
                    }
                    self.state = State::Emit;
                }
                State::Emit => {
                    return self.poll_batch();
                }
            }
        }
    }
}
