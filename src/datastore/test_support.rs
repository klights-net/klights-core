//! Test-only constructors for `Datastore`.
//!
//! Tests across the crate use `Datastore::new_in_memory().await.unwrap()`
//! (~580 sites). Routing them through this module gives a single seam to
//! update if the constructor signature changes (e.g. when the dual-DB P5
//! work lands). No behavior change today.
//!
//! The helpers also collapse the recurring `(Datastore, DatastoreHandle)` pair
//! construction used by root-owned integration tests.

#![cfg(any(test, feature = "integration-test-harness"))]

use super::sqlite::Datastore;
use super::{DatastoreBackend, DatastoreHandle};
#[cfg(test)]
use klights_cluster_core::{LogApplyCommit, LogApplyMutation};
use std::sync::Arc;

/// Build the RV-zero live-apply template consumed by passive-store tests.
///
/// Public resource versions are allocated by committed apply, so legacy
/// fixture RVs are deliberately erased before validation.
#[cfg(test)]
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

/// Build test-only passive read ports directly from the SQLite destination
/// adapter before the concrete store is erased behind `DatastoreHandle`.
pub(crate) fn sqlite_passive_read_ports(
    db: &Datastore,
) -> crate::datastore::selector::PassiveReadPorts {
    let focused_reads = db.focused_read_store();
    crate::datastore::selector::PassiveReadPorts::new(
        focused_reads.clone(),
        focused_reads.clone(),
        focused_reads,
    )
}

/// Fail-closed datastore-free focused reads for tests that construct a local
/// API client but declare its positioned-watch capability unused.
#[cfg(test)]
pub(crate) fn unused_fail_closed_passive_read_ports() -> crate::datastore::selector::PassiveReadPorts
{
    let reads = Arc::new(UnusedFailClosedPassiveRead);
    crate::datastore::selector::PassiveReadPorts::new(reads.clone(), reads.clone(), reads)
}

#[cfg(test)]
const UNUSED_READ_DIAGNOSTIC: &str = "positioned watch is declared unused by this test fixture";

#[cfg(test)]
struct UnusedFailClosedPassiveRead;

#[cfg(test)]
impl klights_cluster_store::ClusterResourceRead for UnusedFailClosedPassiveRead {
    fn get_resource(
        &self,
        _request: klights_cluster_store::ResourceGetRequest,
    ) -> klights_cluster_store::ResourceReadFuture<'_, Option<klights_cluster_core::Resource>> {
        Box::pin(async {
            Err(klights_cluster_store::ResourceReadError::UnsupportedMode {
                message: UNUSED_READ_DIAGNOSTIC.to_string(),
            })
        })
    }

    fn list_resources(
        &self,
        _request: klights_cluster_store::ResourceListRequest,
    ) -> klights_cluster_store::ResourceReadFuture<'_, klights_cluster_store::ResourceListRead>
    {
        Box::pin(async {
            Err(klights_cluster_store::ResourceReadError::UnsupportedMode {
                message: UNUSED_READ_DIAGNOSTIC.to_string(),
            })
        })
    }
}

#[cfg(test)]
impl klights_cluster_store::DurableWatchHistoryRead for UnusedFailClosedPassiveRead {
    fn replay_watch_history(
        &self,
        _request: klights_cluster_store::WatchHistoryRequest,
    ) -> klights_cluster_store::WatchHistoryFuture<'_, klights_cluster_store::WatchHistoryRead>
    {
        Box::pin(async {
            Err(klights_cluster_store::WatchHistoryError::UnsupportedMode {
                message: UNUSED_READ_DIAGNOSTIC.to_string(),
            })
        })
    }

    fn list_replay_floors(
        &self,
    ) -> klights_cluster_store::WatchHistoryFuture<'_, Vec<klights_cluster_store::DurableReplayFloor>>
    {
        Box::pin(async {
            Err(klights_cluster_store::WatchHistoryError::UnsupportedMode {
                message: UNUSED_READ_DIAGNOSTIC.to_string(),
            })
        })
    }
}

#[cfg(test)]
impl klights_cluster_store::DurableAllocatorRead for UnusedFailClosedPassiveRead {
    fn read_allocator_state(
        &self,
    ) -> klights_cluster_store::AllocatorStateFuture<'_, klights_cluster_store::DurableAllocatorState>
    {
        Box::pin(async {
            Err(
                klights_cluster_store::AllocatorStateError::UnsupportedMode {
                    message: UNUSED_READ_DIAGNOSTIC.to_string(),
                },
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{UNUSED_READ_DIAGNOSTIC, unused_fail_closed_passive_read_ports};
    use klights_cluster_core::WatchReplayPosition;
    use klights_cluster_store::{
        AllocatorStateError, DurableWatchTarget, ResourceCollectionScope, ResourceGetRequest,
        ResourceListQuery, ResourceListRequest, ResourceReadError, WatchHistoryError,
        WatchHistoryRequest,
    };

    #[tokio::test]
    async fn unused_fail_closed_passive_read_ports_reject_every_operation() {
        let ports = unused_fail_closed_passive_read_ports();

        let get_error = ports
            .resource_reads()
            .get_resource(ResourceGetRequest::new(
                "v1",
                "ConfigMap",
                Some("default".to_string()),
                "unused",
            ))
            .await
            .unwrap_err();
        assert!(matches!(
            get_error,
            ResourceReadError::UnsupportedMode { message }
                if message == UNUSED_READ_DIAGNOSTIC
        ));

        let list_error = ports
            .resource_reads()
            .list_resources(ResourceListRequest::new(
                "v1",
                "ConfigMap",
                ResourceCollectionScope::Namespace("default".to_string()),
                ResourceListQuery::all(),
            ))
            .await
            .unwrap_err();
        assert!(matches!(
            list_error,
            ResourceReadError::UnsupportedMode { message }
                if message == UNUSED_READ_DIAGNOSTIC
        ));

        let history_error = ports
            .history_reads()
            .replay_watch_history(
                WatchHistoryRequest::new(
                    vec![DurableWatchTarget::namespaced_in_namespace(
                        "v1",
                        "ConfigMap",
                        "default",
                    )],
                    WatchReplayPosition::default(),
                    1,
                )
                .unwrap(),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            history_error,
            WatchHistoryError::UnsupportedMode { message }
                if message == UNUSED_READ_DIAGNOSTIC
        ));

        let floors_error = ports
            .history_reads()
            .list_replay_floors()
            .await
            .unwrap_err();
        assert!(matches!(
            floors_error,
            WatchHistoryError::UnsupportedMode { message }
                if message == UNUSED_READ_DIAGNOSTIC
        ));

        let allocator_error = ports
            .allocator_reads()
            .read_allocator_state()
            .await
            .unwrap_err();
        assert!(matches!(
            allocator_error,
            AllocatorStateError::UnsupportedMode { message }
                if message == UNUSED_READ_DIAGNOSTIC
        ));
    }
}

/// Idempotently ensure a namespace row exists, mirroring a live cluster where
/// the target namespace always pre-exists before objects are created in it.
/// Used by test harnesses that drive the API create path, which now enforces
/// the upstream `NamespaceLifecycle` "namespace must exist" admission rule.
#[cfg(test)]
pub async fn ensure_namespace(db: &dyn DatastoreBackend, name: &str) {
    db.seed_namespace_for_test(name).await;
}
