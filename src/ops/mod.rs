use crate::schema::Schema;
use crate::types::{Result, Row};

mod empty;
mod eval;
mod filter;
mod fullscan;
mod sort;
mod values;

pub use empty::Empty;
pub use eval::Eval;
pub use filter::Filter;
pub use fullscan::FullScan;
pub use sort::Sort;
pub use values::Values;

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
