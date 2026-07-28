//! Redb cluster schema, explicit open adapters, and supervised access.

mod accessor;
mod meta;
mod open_boundary;
mod opener;
pub mod tables;

pub use accessor::RedbAccessor;
pub use open_boundary::{open_in_memory, open_persistent};
pub use opener::RedbOpenOpts;
