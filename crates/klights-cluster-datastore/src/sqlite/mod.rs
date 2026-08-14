//! SQLite cluster schema and explicit open adapters.

mod apply_ledger;
pub mod embedded;
pub mod filters;
mod fingerprint;
pub mod live_apply;
pub mod mutation_diagnostics;
pub mod mutation_helpers;
pub mod mutation_queries;
mod open;
pub mod ordinary;
pub mod owner_ref_index;
mod ownership;
mod position_membership;
pub mod read_helpers;
pub mod read_queries;
pub mod read_store;
pub mod recovery;
mod replay_floor;
mod resource_read;
pub mod resource_shape;
mod schema;
pub mod scope;
pub mod selector_index;
mod snapshot;
pub mod transaction_primitives;

pub use apply_ledger::SqliteApplyLedgerRead;
pub use fingerprint::{META_INSERT, META_SELECT};
pub use open::{
    check_db_health, init_schema, open_in_memory, open_read_only_with_opts, open_with_opts,
};
pub use read_store::SqliteReadStore;
#[cfg(any(test, feature = "test-support"))]
pub use resource_read::ListResourcesSnapshotPause;
pub use schema::init_schema_in_conn;
pub use snapshot::ExactSnapshotRead;
pub use snapshot::{
    arm_historical_window_pause_for_test, historical_window_counts_for_test,
    physical_bound_counters_for_test, reset_physical_bound_counters_for_test,
    resume_historical_window_pause_for_test, wait_for_historical_window_pause_for_test,
};

#[cfg(test)]
pub use open::open_in_memory_with_default_supervisor;
