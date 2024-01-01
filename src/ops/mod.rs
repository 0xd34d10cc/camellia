use crate::types::{Result, Row, Schema};

mod fullscan;
mod projection;
mod filter;
mod sort;

pub use fullscan::FullScan;
pub use projection::Projection;
pub use filter::Filter;
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
