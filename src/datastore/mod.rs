//! Datastore — runtime contract (`DatastoreBackend`) plus backend
//! implementations. The trait surface is in `backend.rs`, shared types in
//! `types.rs`, and each backend lives in its own sibling folder. Today
//! there is one backend: `sqlite/`. Future backends slot in alongside
//! with the same internal shape.

pub mod backend;
pub mod backend_kind;
pub(crate) mod cluster_store_adapter;
pub mod command;
pub mod diagnostics;
pub mod domain;
pub mod errors;
pub mod node_local;
pub mod pod_serviceaccount;
pub(crate) mod position_membership;
pub mod raft;
pub mod redb;
pub(crate) mod replay_retention;
pub(crate) mod selector;
pub(crate) mod sequenced;
pub mod snapshot;
pub(crate) mod snapshot_export;
pub mod sqlite;
pub mod stale_apply_policy;
pub mod status_merge_policy;
pub mod types;

pub use backend::*;
pub(crate) use klights_watch::{WatchSignalReceiver, WatchTopic};
pub use types::*;

#[cfg(test)]
pub use sqlite::test_support;

#[cfg(test)]
pub use sqlite::create_pending_watch_event;
