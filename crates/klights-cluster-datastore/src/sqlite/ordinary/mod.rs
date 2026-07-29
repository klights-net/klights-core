//! Corrected Phase 10C.1 ordinary SQLite mutation transaction primitives.
//!
//! Root compatibility methods prepare API values, execute these primitives
//! through the app-owned DB executor, and perform post-commit observation.

mod namespace;
mod resource_create;
mod resource_delete;
mod resource_patch;
mod resource_update;

pub use namespace::{
    NamespaceDeleteResult, create_namespace_in_conn, delete_namespace_contents_in_conn,
    delete_namespace_in_conn, update_namespace_in_conn,
};
pub use resource_create::{CreateResourceInput, create_resource_in_conn};
pub use resource_delete::{DeleteResourceAttempt, DeleteResourceInput, delete_resource_in_conn};
pub use resource_patch::{PatchResourceInput, patch_resource_in_conn};
pub use resource_update::{
    MarkResourceForDeletionInput, UpdateResourceInput, mark_resource_for_deletion_in_conn,
    update_resource_in_conn,
};

// These are destination-approved lower packets: schema/query constants,
// selector and owner-reference indexes, resource-shape policy, and transaction
// allocators. No root datastore, Raft, replication, or leader surface is
// reachable through the ordinary mutation packet.
use super::mutation_queries as queries;
use super::scope::use_namespaced_table;
use super::{
    mutation_diagnostics, mutation_helpers, owner_ref_index, resource_shape, selector_index,
    transaction_primitives,
};
