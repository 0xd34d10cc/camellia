use crate::types::{Result, Row, Schema};

mod fullscan;
mod projection;
mod filter;
mod order_by;

pub use fullscan::FullScan;
pub use projection::Projection;
pub use filter::Filter;
pub use order_by::OrderBy;

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
