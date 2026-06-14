//! Datastore — runtime contract (`DatastoreBackend`) plus backend
//! implementations. The trait surface is in `backend.rs`, shared types in
//! `types.rs`, and each backend lives in its own sibling folder. Today
//! there is one backend: `sqlite/`. Future backends slot in alongside
//! with the same internal shape.

pub mod backend;
pub mod backend_kind;
// DSB-HA-01 ships the types and codecs; DSB-HA-02 (ReplicatedDatastore)
// will be the first production consumer.  Suppress dead-code warnings until
// then.
pub mod command;
pub mod diagnostics;
pub mod domain;
pub mod errors;
pub mod node_local;
pub mod pod_serviceaccount;
pub mod raft;
pub mod redb;
pub mod replicated;
pub mod selector;
pub mod snapshot;
pub mod sqlite;
pub mod types;

pub use backend::*;
pub use types::*;

#[cfg(test)]
pub use sqlite::test_support;

#[cfg(test)]
pub use sqlite::create_pending_watch_event;
