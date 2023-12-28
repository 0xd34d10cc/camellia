use crate::types::{Result, Row, Schema};

mod fullscan;
mod projection;

pub use fullscan::FullScan;
pub use projection::Projection;

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
