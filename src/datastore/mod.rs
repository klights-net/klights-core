//! Datastore — runtime contract (`DatastoreBackend`) plus backend
//! implementations. The trait surface is in `backend.rs`, shared types in
//! `types.rs`, and each backend lives in its own sibling folder. Today
//! there is one backend: `sqlite/`. Future backends slot in alongside
//! with the same internal shape.

pub mod backend;
pub mod backend_kind;
pub(crate) mod cluster_store_adapter;
pub mod diagnostics;
pub mod domain;
pub mod node_local;
pub mod pod_serviceaccount;
pub mod raft;
pub mod redb;
pub(crate) mod selector;
pub mod snapshot;
pub(crate) mod snapshot_export;
pub mod sqlite;
pub mod types;

pub use backend::*;
pub use klights_cluster_core::{
    PatchKind, PositionedWatchEvent, Resource, ResourceBatchOperation, ResourceBatchPutMode,
    ResourcePatchRequest, ResourcePreconditions, WatchReplayPosition,
};
#[cfg(test)]
pub(crate) use klights_watch::WatchTopic;
pub use types::*;

#[cfg(test)]
pub use sqlite::test_support;

#[cfg(test)]
pub use sqlite::create_pending_watch_event;
