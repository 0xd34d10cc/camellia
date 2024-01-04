use crate::schema::Schema;
use crate::types::{Result, Row};

mod filter;
mod fullscan;
mod eval;
mod sort;

pub use filter::Filter;
pub use fullscan::FullScan;
pub use eval::Eval;
pub use sort::Sort;

pub enum Output {
    Batch(Vec<Row>),
    Finished,
}

pub trait Operation {
    // Get schema of resulting rows
    fn schema(&self) -> &Schema;

    // Get next batch of rows
    fn poll(&mut self) -> Result<Output>;
}
