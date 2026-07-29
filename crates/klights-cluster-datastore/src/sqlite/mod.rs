//! SQLite cluster schema and explicit open adapters.

pub mod filters;
mod fingerprint;
mod open;
mod ownership;
mod position_membership;
pub mod read_helpers;
pub mod read_queries;
pub mod read_store;
mod replay_floor;
mod resource_read;
mod schema;
pub mod scope;
pub mod selector_index;
mod snapshot;

pub use fingerprint::{META_INSERT, META_SELECT};
pub use open::{
    check_db_health, init_schema, open_in_memory, open_read_only_with_opts, open_with_opts,
};
pub use read_store::SqliteReadStore;
#[cfg(any(test, feature = "test-support"))]
pub use resource_read::ListResourcesSnapshotPause;
pub use schema::init_schema_in_conn;
pub use snapshot::ExactSnapshotRead;

#[cfg(test)]
pub use open::open_in_memory_with_default_supervisor;
