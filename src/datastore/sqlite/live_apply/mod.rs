//! Corrected Phase 10C.2 SQLite live committed-apply packet.
//!
//! This module owns the complete transaction coordinator and every mutation
//! variant it dispatches. Root datastore facades and post-commit composition
//! remain outside this packet.

mod context;
mod coordinator;
mod state;
pub(super) mod status;

pub(crate) use context::TransactionContext;
#[cfg(test)]
pub(crate) use coordinator::apply_commit_in_tx_for_raft;
pub(crate) use coordinator::{
    ApplyConflictCode, apply_commit_in_tx_for_raft_with_context,
    apply_commit_in_tx_returning_rv_and_mutation_with_context, apply_commit_in_tx_with_context,
    apply_conflict_error, apply_snapshot_restore_operation_in_tx, other_error,
};
#[cfg(test)]
pub(crate) use state::watch_history::watch_events_min_scope_rows_for_scope_count;
pub(crate) use state::watch_history::{gc_watch_events_in_tx, watch_events_min_scope_rows_in_conn};

// Temporary root-local dependency aliases. Each names a lower packet that
// moves before 10C.2; no Raft, replication, leader API, or broad datastore
// owner is reachable through this module.
use super::{
    create_staged_post_commit, mutation_helpers, owner_ref_index, queries, resource_shape,
    transaction_primitives, use_namespaced_table,
};
