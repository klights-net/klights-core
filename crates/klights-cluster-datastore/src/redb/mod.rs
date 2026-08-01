//! Redb cluster schema, explicit open adapters, and supervised access.

mod accessor;
pub mod embedded;
pub mod key_codec;
pub mod live_committed_apply;
mod meta;
pub mod mutation_helpers;
mod open_boundary;
mod opener;
pub mod ordinary_mutations;
pub mod read_core;
mod read_store;
pub mod recovery;
mod replay_floor;
pub mod tables;

pub use accessor::RedbAccessor;
pub use open_boundary::{open_in_memory, open_persistent};
pub use opener::RedbOpenOpts;
pub use ordinary_mutations::{RedbOrdinaryNamespaceStore, RedbOrdinaryResourceStore};
pub use read_store::RedbReadStore;
