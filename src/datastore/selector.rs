//! Passive cluster backend adapter dispatch.
//!
//! Returns the selected passive `DatastoreHandle` so no caller distinguishes
//! which backend was chosen. Replication sequencing is composed separately by
//! the root after Raft has been constructed from this passive handle. The
//! composition root owns environment, backend, mode, and path selection and
//! passes only the resulting typed request into this module.

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;

use crate::datastore::backend::DatastoreHandle;
use crate::datastore::sqlite;
use klights_supervisor::TaskSupervisor;

/// Fully selected passive cluster-store adapter request.
///
/// Root composition constructs this value after parsing and validating ambient
/// configuration. Variant-specific fields prevent persistence from receiving
/// unused root configuration or discovering a backend, mode, key, or path.
#[derive(Debug, Clone, Copy)]
pub(crate) enum PassiveStoreOpenRequest<'a> {
    SqliteInMemory,
    SqlitePersistent {
        cluster_db_path: &'a Path,
        db_key_file: Option<&'a Path>,
    },
    RedbInMemory,
    RedbPersistent {
        cluster_db_path: &'a Path,
    },
}

/// Immutable focused read capabilities selected with the passive backend.
///
/// Root composition owns this bundle. It is not a datastore facade and never
/// exposes the legacy backend.
#[derive(Clone)]
pub(crate) struct PassiveReadPorts {
    resource_reads: Arc<dyn klights_cluster_store::ClusterResourceRead>,
    history_reads: Arc<dyn klights_cluster_store::DurableWatchHistoryRead>,
    allocator_reads: Arc<dyn klights_cluster_store::DurableAllocatorRead>,
}

impl PassiveReadPorts {
    pub(crate) fn new(
        resource_reads: Arc<dyn klights_cluster_store::ClusterResourceRead>,
        history_reads: Arc<dyn klights_cluster_store::DurableWatchHistoryRead>,
        allocator_reads: Arc<dyn klights_cluster_store::DurableAllocatorRead>,
    ) -> Self {
        Self {
            resource_reads,
            history_reads,
            allocator_reads,
        }
    }

    pub(crate) fn resource_reads(&self) -> Arc<dyn klights_cluster_store::ClusterResourceRead> {
        self.resource_reads.clone()
    }

    pub(crate) fn history_reads(&self) -> Arc<dyn klights_cluster_store::DurableWatchHistoryRead> {
        self.history_reads.clone()
    }

    pub(crate) fn allocator_reads(&self) -> Arc<dyn klights_cluster_store::DurableAllocatorRead> {
        self.allocator_reads.clone()
    }
}

#[cfg(any(test, feature = "integration-test-harness"))]
const UNUSED_READ_DIAGNOSTIC: &str = "positioned watch is declared unused by this test fixture";

#[cfg(any(test, feature = "integration-test-harness"))]
struct UnusedFailClosedPassiveRead;

#[cfg(any(test, feature = "integration-test-harness"))]
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

#[cfg(any(test, feature = "integration-test-harness"))]
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

#[cfg(any(test, feature = "integration-test-harness"))]
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

/// Fail-closed datastore-free focused reads for tests that construct a local
/// API client but declare its positioned-watch capability unused.
#[cfg(any(test, feature = "integration-test-harness"))]
pub(crate) fn unused_fail_closed_passive_read_ports() -> PassiveReadPorts {
    let reads = Arc::new(UnusedFailClosedPassiveRead);
    PassiveReadPorts::new(reads.clone(), reads.clone(), reads)
}

/// Build test-only passive read ports directly from the SQLite destination
/// adapter before the concrete store is erased behind `DatastoreHandle`.
#[cfg(any(test, feature = "integration-test-harness"))]
pub(crate) fn sqlite_passive_read_ports(db: &sqlite::Datastore) -> PassiveReadPorts {
    let focused_reads = db.focused_read_store();
    PassiveReadPorts::new(focused_reads.clone(), focused_reads.clone(), focused_reads)
}

/// Construct the root SQLite test adapter before kubelet fixtures receive its
/// erased datastore handle. This keeps concrete cluster storage out of the
/// kubelet-owned test surface.
#[cfg(test)]
pub(crate) async fn sqlite_in_memory_store_for_test() -> (sqlite::Datastore, super::DatastoreHandle)
{
    sqlite::Datastore::new_in_memory_with_handle().await
}

/// Root-only result of selecting one passive persistence backend.
///
/// The legacy backend remains available while callers that already consume
/// focused read ports receive the concrete implementation directly.
pub(crate) struct OpenedPassiveStore {
    pub(crate) backend: DatastoreHandle,
    pub(crate) read_ports: PassiveReadPorts,
    pub(crate) committed_apply: Arc<dyn klights_cluster_store::PrivilegedCommittedRaftApply>,
    pub(crate) sqlite_recovery:
        Option<Arc<klights_cluster_datastore::sqlite::recovery::SqliteRecoveryStore>>,
}

/// Open the already-selected passive cluster datastore adapter.
///
/// Every variant returns the concrete backend behind a `DatastoreHandle`; this
/// dispatch never reads ambient configuration or installs replication behavior.
pub(crate) async fn open_with_sink(
    request: PassiveStoreOpenRequest<'_>,
    supervisor: Arc<TaskSupervisor>,
    #[cfg(any(test, feature = "integration-test-harness"))] commit_sink: Arc<
        dyn crate::datastore::CommitObservationSink,
    >,
    outbox_codec: Arc<dyn klights_cluster_store::OutboxResponseCodec>,
) -> Result<OpenedPassiveStore> {
    match request {
        PassiveStoreOpenRequest::SqliteInMemory => {
            tracing::info!(backend = "sqlite", mode = "in-memory", "opening datastore");
            let executor = klights_cluster_datastore::sqlite::open_in_memory(
                supervisor,
                "sqlite:selector-in-memory",
            )
            .await?;
            let ds = sqlite::Datastore::new_in_memory_with_watch_and_executor_with_sink(
                executor,
                #[cfg(any(test, feature = "integration-test-harness"))]
                commit_sink,
                outbox_codec,
                Arc::new(klights_supervisor::SystemWallClock),
            )
            .await?;
            let focused_reads = ds.focused_read_store();
            let committed_apply = ds.focused_committed_apply();
            let sqlite_recovery = ds.focused_recovery_store();
            Ok(OpenedPassiveStore {
                backend: Arc::new(ds),
                committed_apply,
                sqlite_recovery: Some(sqlite_recovery),
                read_ports: PassiveReadPorts::new(
                    focused_reads.clone(),
                    focused_reads.clone(),
                    focused_reads,
                ),
            })
        }
        PassiveStoreOpenRequest::SqlitePersistent {
            cluster_db_path,
            db_key_file,
        } => {
            tracing::info!(backend = "sqlite", mode = "persistent", "opening datastore");
            let ds = sqlite::Datastore::new_persistent_paths_with_sink(
                cluster_db_path,
                supervisor,
                db_key_file,
                #[cfg(any(test, feature = "integration-test-harness"))]
                commit_sink,
                outbox_codec,
                Arc::new(klights_supervisor::SystemWallClock),
            )
            .await?;
            let focused_reads = ds.focused_read_store();
            let committed_apply = ds.focused_committed_apply();
            let sqlite_recovery = ds.focused_recovery_store();
            Ok(OpenedPassiveStore {
                backend: Arc::new(ds),
                committed_apply,
                sqlite_recovery: Some(sqlite_recovery),
                read_ports: PassiveReadPorts::new(
                    focused_reads.clone(),
                    focused_reads.clone(),
                    focused_reads,
                ),
            })
        }
        PassiveStoreOpenRequest::RedbInMemory => {
            tracing::info!(backend = "redb", mode = "in-memory", "opening datastore");
            let ds = crate::datastore::redb::RedbDatastore::new_in_memory_with_supervisor_and_sink(
                supervisor,
                #[cfg(any(test, feature = "integration-test-harness"))]
                commit_sink,
                Arc::new(klights_supervisor::SystemWallClock),
            )
            .await?;
            let focused_reads = ds.focused_read_store();
            let committed_apply = ds.focused_committed_apply();
            Ok(OpenedPassiveStore {
                backend: Arc::new(ds),
                committed_apply,
                sqlite_recovery: None,
                read_ports: PassiveReadPorts::new(
                    focused_reads.clone(),
                    focused_reads.clone(),
                    focused_reads,
                ),
            })
        }
        PassiveStoreOpenRequest::RedbPersistent { cluster_db_path } => {
            tracing::info!(backend = "redb", mode = "persistent", "opening datastore");
            let ds = crate::datastore::redb::RedbDatastore::new_persistent_with_sink(
                cluster_db_path,
                supervisor,
                #[cfg(any(test, feature = "integration-test-harness"))]
                commit_sink,
                Arc::new(klights_supervisor::SystemWallClock),
            )
            .await?;
            let focused_reads = ds.focused_read_store();
            let committed_apply = ds.focused_committed_apply();
            Ok(OpenedPassiveStore {
                backend: Arc::new(ds),
                committed_apply,
                sqlite_recovery: None,
                read_ports: PassiveReadPorts::new(
                    focused_reads.clone(),
                    focused_reads.clone(),
                    focused_reads,
                ),
            })
        }
    }
}

#[cfg(test)]
pub(crate) async fn open(
    request: PassiveStoreOpenRequest<'_>,
    supervisor: Arc<TaskSupervisor>,
) -> Result<DatastoreHandle> {
    open_with_sink(
        request,
        supervisor,
        crate::bootstrap::watch_commit_wiring::new_sink(),
        crate::bootstrap::composition_adapters::outbox_response_codec_adapter::new_codec(),
    )
    .await
    .map(|opened| opened.backend)
}

#[cfg(test)]
mod tests {
    use super::{
        OpenedPassiveStore, PassiveStoreOpenRequest, UNUSED_READ_DIAGNOSTIC, open, open_with_sink,
        unused_fail_closed_passive_read_ports,
    };
    use crate::datastore::{
        PositionedWatchReplayRead, ResourceListQuery as LegacyResourceListQuery, WatchTarget,
    };
    use klights_cluster_store::{
        AllocatorStateError, DurableWatchTarget, ResourceCollectionScope, ResourceGetRequest,
        ResourceListQuery, ResourceListRead, ResourceListRequest, ResourceReadError,
        WatchHistoryError, WatchHistoryRead, WatchHistoryRequest,
    };
    use klights_supervisor::{TaskCategoryConfig, TaskSupervisor};
    use serde_json::json;
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    fn supervisor() -> Arc<TaskSupervisor> {
        Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()))
    }

    fn assert_all_focused_read_ports<T>()
    where
        T: klights_cluster_store::ClusterResourceRead
            + klights_cluster_store::ClusterOwnershipRead
            + klights_cluster_store::NamespaceContentRead
            + klights_cluster_store::DurableWatchHistoryRead
            + klights_cluster_store::DurableWatchRangeRead
            + klights_cluster_store::DurableRawWatchHistoryRead
            + klights_cluster_store::DurableAllocatorRead
            + klights_cluster_store::ClusterTopologyRead,
    {
    }

    #[test]
    fn concrete_read_stores_own_every_focused_read_port() {
        assert_all_focused_read_ports::<klights_cluster_datastore::sqlite::SqliteReadStore>();
        assert_all_focused_read_ports::<klights_cluster_datastore::redb::RedbReadStore>();
    }

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
                    klights_cluster_core::WatchReplayPosition::default(),
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

    async fn open_selected(request: PassiveStoreOpenRequest<'_>) -> OpenedPassiveStore {
        open_with_sink(
            request,
            supervisor(),
            crate::bootstrap::watch_commit_wiring::new_sink(),
            crate::bootstrap::composition_adapters::outbox_response_codec_adapter::new_codec(),
        )
        .await
        .expect("open selected passive store")
    }

    #[tokio::test]
    async fn typed_in_memory_requests_open_both_passive_adapters() {
        for request in [
            PassiveStoreOpenRequest::SqliteInMemory,
            PassiveStoreOpenRequest::RedbInMemory,
        ] {
            let store = open(request, supervisor()).await.expect("open adapter");
            assert_eq!(store.get_current_resource_version().await.unwrap(), 0);
            store.close();
        }
    }

    #[tokio::test]
    async fn typed_persistent_requests_preserve_adapter_paths() {
        let dir = tempfile::tempdir().unwrap();
        let sqlite_cluster = dir.path().join("sqlite/cluster.db");
        let sqlite_node = dir.path().join("sqlite/node.db");
        let sqlite = open(
            PassiveStoreOpenRequest::SqlitePersistent {
                cluster_db_path: &sqlite_cluster,
                db_key_file: None,
            },
            supervisor(),
        )
        .await
        .expect("open persistent sqlite");
        assert_eq!(sqlite.get_current_resource_version().await.unwrap(), 0);
        sqlite.close();
        assert!(sqlite_cluster.is_file());
        assert!(
            !sqlite_node.exists(),
            "passive cluster-store open must not create node.db"
        );

        let redb_path = dir.path().join("redb/cluster.redb");
        let redb = open(
            PassiveStoreOpenRequest::RedbPersistent {
                cluster_db_path: &redb_path,
            },
            supervisor(),
        )
        .await
        .expect("open persistent redb");
        assert_eq!(redb.get_current_resource_version().await.unwrap(), 0);
        redb.close();
        assert!(redb_path.is_file());
    }

    #[tokio::test]
    async fn focused_reads_match_legacy_list_watch_cursor_and_errors_for_both_backends() {
        for request in [
            PassiveStoreOpenRequest::SqliteInMemory,
            PassiveStoreOpenRequest::RedbInMemory,
        ] {
            let opened = open_selected(request).await;
            let backend = opened.backend;
            let reads = opened.read_ports;
            let start = reads
                .allocator_reads()
                .read_allocator_state()
                .await
                .expect("initial allocator")
                .position();

            for name in ["alpha", "beta"] {
                backend
                    .create_resource(
                        "v1",
                        "ConfigMap",
                        Some("phase9-repair"),
                        name,
                        json!({
                            "apiVersion": "v1",
                            "kind": "ConfigMap",
                            "metadata": {
                                "name": name,
                                "namespace": "phase9-repair",
                            },
                            "data": {"value": name},
                        }),
                    )
                    .await
                    .expect("seed list/watch parity resource");
            }

            let legacy_list = backend
                .list_resources(
                    "v1",
                    "ConfigMap",
                    Some("phase9-repair"),
                    LegacyResourceListQuery::all(),
                )
                .await
                .expect("legacy list");
            let focused_list = reads
                .resource_reads()
                .list_resources(ResourceListRequest::new(
                    "v1",
                    "ConfigMap",
                    ResourceCollectionScope::Namespace("phase9-repair".to_string()),
                    ResourceListQuery::all(),
                ))
                .await
                .expect("focused list");
            let ResourceListRead::Current(focused_page) = focused_list else {
                panic!("current focused LIST must return a current page");
            };
            assert_eq!(
                legacy_list
                    .items
                    .iter()
                    .map(|resource| (
                        resource.name.as_str(),
                        resource.resource_version,
                        resource.data.as_ref(),
                    ))
                    .collect::<Vec<_>>(),
                focused_page
                    .items()
                    .iter()
                    .map(|resource| (
                        resource.name.as_str(),
                        resource.resource_version,
                        resource.data.as_ref(),
                    ))
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                legacy_list.resource_version,
                focused_page.snapshot().resource_version()
            );
            assert_eq!(
                legacy_list.watch_replay_position,
                Some(focused_page.snapshot().position())
            );
            assert_eq!(legacy_list.continue_token, None);
            assert_eq!(
                legacy_list.remaining_item_count,
                focused_page.remaining_item_count()
            );

            let limit = NonZeroUsize::new(8).unwrap();
            let legacy_watch = backend
                .list_watch_events_after_position_checked_bounded(
                    &[WatchTarget::namespaced_in_namespace(
                        "v1",
                        "ConfigMap",
                        "phase9-repair",
                    )],
                    start,
                    limit,
                )
                .await
                .expect("legacy positioned history");
            let focused_watch = reads
                .history_reads()
                .replay_watch_history(
                    WatchHistoryRequest::new(
                        vec![DurableWatchTarget::namespaced_in_namespace(
                            "v1",
                            "ConfigMap",
                            "phase9-repair",
                        )],
                        start,
                        limit.get(),
                    )
                    .unwrap(),
                )
                .await
                .expect("focused positioned history");
            let PositionedWatchReplayRead::Events(legacy_watch) = legacy_watch else {
                panic!("legacy retained cursor unexpectedly expired");
            };
            let WatchHistoryRead::Events(focused_watch) = focused_watch else {
                panic!("focused retained cursor unexpectedly expired");
            };
            assert_eq!(legacy_watch.next_position, focused_watch.next_position());
            assert_eq!(
                legacy_watch
                    .events
                    .iter()
                    .map(|event| (
                        event.position,
                        event.event.event_type.as_ref(),
                        event.event.resource.name.as_str(),
                        event.event.resource.data.as_ref(),
                    ))
                    .collect::<Vec<_>>(),
                focused_watch
                    .events()
                    .iter()
                    .map(|event| (
                        event.position,
                        event.event.event_type(),
                        event.event.resource().name.as_str(),
                        event.event.resource().data.as_ref(),
                    ))
                    .collect::<Vec<_>>()
            );

            let legacy_allocator = backend
                .read_durable_allocator_observation()
                .await
                .expect("legacy allocator");
            let focused_allocator = reads
                .allocator_reads()
                .read_allocator_state()
                .await
                .expect("focused allocator");
            assert_eq!(legacy_allocator.position, focused_allocator.position());
            assert_eq!(legacy_allocator.position, focused_watch.next_position());

            let legacy_unbounded = backend
                .list_resources(
                    "v1",
                    "ConfigMap",
                    Some("phase9-repair"),
                    LegacyResourceListQuery::new(None, None, Some(-1), None),
                )
                .await
                .expect("legacy negative list limit remains unbounded");
            assert_eq!(legacy_unbounded.items.len(), 2);
            let focused_error = ResourceListQuery::try_new(
                None,
                None,
                Some(-1),
                None,
                klights_cluster_store::ResourceVersionMatch::Any,
            )
            .expect_err("focused invalid list limit must fail");
            assert_eq!(focused_error.status().code, 400);
            assert!(!focused_error.status().retryable);
            backend.close();
        }
    }
}
