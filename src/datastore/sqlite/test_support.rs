//! Test-only constructors for `Datastore`.
//!
//! Tests across the crate use `Datastore::new_in_memory().await.unwrap()`
//! (~580 sites). Routing them through this module gives a single seam to
//! update if the constructor signature changes (e.g. when the dual-DB P5
//! work lands). No behavior change today.
//!
//! The helpers also collapse two recurring follow-up patterns:
//! - `(Datastore, DatastoreHandle)` pair construction (side-effect tests,
//!   networking integration tests, `cni_plugin` test-state)
//! - `Context::new(Arc::new(db.clone()), "test-node".into())` for the
//!   controller-wrapper tests in `src/controllers/*_controller.rs`.

#![cfg(test)]

use super::{Datastore, DatastoreBackend, DatastoreHandle};
use std::sync::Arc;

/// Construct an in-memory `Datastore` for tests.  Panics on init failure
/// (an in-memory SQLite open + schema apply is not a recoverable test
/// condition; the fixture is broken if this fails).
pub async fn in_memory() -> Datastore {
    Datastore::new_in_memory()
        .await
        .expect("test-support in-memory Datastore init")
}

/// Construct an in-memory `Datastore` and a matching `DatastoreHandle`
/// (`Arc<dyn DatastoreBackend>`) cloned from it. Used by side-effect
/// tests, networking integration tests, and other code that needs both
/// the concrete `Datastore` (for direct method access) and the trait
/// handle (for code that takes `&dyn DatastoreBackend`).
pub async fn in_memory_with_handle() -> (Datastore, DatastoreHandle) {
    let db = in_memory().await;
    let handle: DatastoreHandle = Arc::new(db.clone()) as Arc<dyn DatastoreBackend>;
    (db, handle)
}

/// Idempotently ensure a namespace row exists, mirroring a live cluster where
/// the target namespace always pre-exists before objects are created in it.
/// Used by test harnesses that drive the API create path, which now enforces
/// the upstream `NamespaceLifecycle` "namespace must exist" admission rule.
pub async fn ensure_namespace(db: &dyn DatastoreBackend, name: &str) {
    db.seed_namespace_for_test(name).await;
}

/// Construct a controller `Context` over an in-memory `Datastore`.
/// `node_name` defaults to `"test-node"`.
pub fn test_context(db: &Datastore) -> crate::controller::Context {
    crate::controller::Context::new(
        Arc::new(db.clone()) as DatastoreHandle,
        "test-node".to_string(),
    )
}
