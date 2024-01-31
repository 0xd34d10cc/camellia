use std::borrow::Cow;
use std::collections::BinaryHeap;

use sqlparser::ast;

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

    // TODO: add specialization for single expression
    by: Vec<Expression>,

    // TODO: use disk-backed storage for runs
    runs: Vec<Vec<Row>>,
    // TODO: use disk-backed storage for result
    results: std::vec::IntoIter<Row>,

    state: State,
}

impl<'txn> Sort<'txn> {
    pub fn new(
        order_by: Vec<ast::OrderByExpr>,
        select: &[Expression],
        inner: Box<dyn Operation + 'txn>,
    ) -> Result<Self> {
        let schema = inner.schema();
        let mut expressions = Vec::with_capacity(order_by.len());
        for expr in order_by {
            if let Some(false) = expr.asc {
                return Err("DESC is not implemented".into());
            }

            if expr.nulls_first.is_some() {
                return Err("NULLS FIRST is not implemented".into());
            }

            let expr = Expression::parse(expr.expr, schema)?;
            let expr = match expr {
                // ORDER BY allows to specify column by number instead of name
                Expression::Const(Value::Int(n)) => {
                    if n <= 0 {
                        return Err(format!("Invalid column number: {}", n).into());
                    }
                    let index = (n - 1) as usize;
                    let e = select.get(index).ok_or_else(|| {
                        format!(
                            "ORDER BY term out of range - should be between 1 and {}",
                            select.len()
                        )
                    })?;
                    e.clone()
                }
                e => e,
            };
            expressions.push(expr);
        }

        Ok(Self {
            inner,

            by: expressions,
            runs: Vec::new(),
            results: Vec::new().into_iter(),

            state: State::Read,
        })
    }

    fn key_of(&self, row: &Row) -> Result<Row> {
        let mut key = Vec::with_capacity(self.by.len());
        for e in &self.by {
            let val = e.eval(row)?;
            key.push(val);
        }
        Ok(Row::from(key))
    }

    #[minitrace::trace]
    fn read(&mut self) -> Result<()> {
        loop {
            match self.inner.poll()? {
                Output::Batch(mut batch) => {
                    // TODO: handle errors
                    // TODO: batch can be small, use chunks of N
                    batch.sort_by_cached_key(|row| self.key_of(row).unwrap());
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
            key: Row,
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
                let key = self.key_of(&row).unwrap();
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
                let key = self.key_of(&row).unwrap();
                heap.push(Item { key, row, iter });
            }
        }

        sorted
    }

    #[minitrace::trace]
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

    #[minitrace::trace]
    fn poll_batch(&mut self) -> Result<Output> {
        const BATCH_SIZE: usize = 1024;
        let mut chunk = Vec::with_capacity(BATCH_SIZE);
        loop {
            match self.results.next() {
                Some(row) => {
                    chunk.push(row);
                    if chunk.len() >= BATCH_SIZE {
                        minitrace::Event::add_to_local_parent("batch", || {
                            [(Cow::Borrowed("size"), Cow::Owned(format!("{}", chunk.len())))]
                        });
                        return Ok(Output::Batch(chunk));
                    }
                }
                None => {
                    if chunk.is_empty() {
                        return Ok(Output::Finished);
                    } else {
                        minitrace::Event::add_to_local_parent("batch", || {
                            [(Cow::Borrowed("size"), Cow::Owned(format!("{}", chunk.len())))]
                        });
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

    #[minitrace::trace]
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
