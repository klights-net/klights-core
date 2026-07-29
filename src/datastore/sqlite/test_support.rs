//! Test-only constructors for `Datastore`.
//!
//! Tests across the crate use `Datastore::new_in_memory().await.unwrap()`
//! (~580 sites). Routing them through this module gives a single seam to
//! update if the constructor signature changes (e.g. when the dual-DB P5
//! work lands). No behavior change today.
//!
//! The helpers also collapse the recurring `(Datastore, DatastoreHandle)` pair
//! construction used by root-owned integration tests.

#![cfg(test)]

use super::{Datastore, DatastoreBackend, DatastoreHandle};
use klights_cluster_core::{LogApplyCommit, LogApplyMutation};
use std::sync::Arc;

/// Build the RV-zero live-apply template consumed by passive-store tests.
///
/// Public resource versions are allocated by committed apply, so legacy
/// fixture RVs are deliberately erased before validation.
pub(crate) fn test_live_commit(
    candidate_resource_version: i64,
    mut mutations: Vec<LogApplyMutation>,
) -> LogApplyCommit {
    fn clear_nested_resource_version(data: &mut serde_json::Value) {
        if let Some(metadata) = data
            .get_mut("metadata")
            .and_then(serde_json::Value::as_object_mut)
        {
            metadata.remove("resourceVersion");
        }
    }

    for mutation in &mut mutations {
        match mutation {
            LogApplyMutation::PutResource(row) => {
                row.resource_version = 0;
                clear_nested_resource_version(&mut row.data);
            }
            LogApplyMutation::PatchResourceLatest(row) => {
                row.resource_version = 0;
                clear_nested_resource_version(&mut row.patch);
            }
            LogApplyMutation::PutNamespace(row) => {
                row.resource_version = 0;
                clear_nested_resource_version(&mut row.data);
            }
            LogApplyMutation::PutWatchEvent(row) => {
                row.resource_version = 0;
                clear_nested_resource_version(&mut row.data);
                if let Some(object) = row.data.get_mut("object") {
                    clear_nested_resource_version(object);
                }
            }
            LogApplyMutation::PutPodCleanupIntent(row) => row.resource_version = 0,
            LogApplyMutation::PutAppliedOutbox(row) => row.applied_rv = None,
            LogApplyMutation::AdvanceResourceVersion { resource_version } => {
                *resource_version = 0;
            }
            _ => {}
        }
    }
    let _ = candidate_resource_version;
    LogApplyCommit::try_new(mutations).expect("test live commit must be an RV-zero template")
}

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

/// Construct the focused controller test context used by the thin controller
/// runner fixtures. This helper is test-only and does not add a production
/// datastore-to-controller compatibility seam.
pub(crate) fn test_context(db: &Datastore) -> crate::controllers::Context {
    let db_handle = Arc::new(db.clone()) as DatastoreHandle;
    crate::controllers::Context::new(db_handle.clone(), "test-node".to_string())
        .with_non_pod_finalization(Arc::new(
            crate::gc_delete_adapter::GcNonPodFinalizationAdapter::new(db_handle),
        ))
}
