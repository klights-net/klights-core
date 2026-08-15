//! Passive cluster backend adapter dispatch.
//!
//! Returns the selected passive store as a fixed set of focused capabilities.
//! Replication sequencing is composed separately by the root after selection.
//! The composition root owns environment, backend, mode, and path selection
//! and passes only the resulting typed request into this module.

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;

use klights_supervisor::TaskSupervisor;

/// Fully selected passive cluster-store adapter request.
///
/// Root composition constructs this value after parsing and validating ambient
/// configuration. Variant-specific fields prevent persistence from receiving
/// unused root configuration or discovering a backend, mode, key, or path.
#[derive(Debug, Clone, Copy)]
pub(crate) enum PassiveStoreOpenRequest<'a> {
    SqliteInMemory,
    SqlitePersistent { cluster_db_path: &'a Path },
    RedbInMemory,
    RedbPersistent { cluster_db_path: &'a Path },
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
    resource_scopes: Arc<dyn klights_cluster_store::ClusterResourceScopeRead>,
}

impl PassiveReadPorts {
    pub(crate) fn new(
        resource_reads: Arc<dyn klights_cluster_store::ClusterResourceRead>,
        history_reads: Arc<dyn klights_cluster_store::DurableWatchHistoryRead>,
        allocator_reads: Arc<dyn klights_cluster_store::DurableAllocatorRead>,
        resource_scopes: Arc<dyn klights_cluster_store::ClusterResourceScopeRead>,
    ) -> Self {
        Self {
            resource_reads,
            history_reads,
            allocator_reads,
            resource_scopes,
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

    pub(crate) fn resource_scopes(
        &self,
    ) -> Arc<dyn klights_cluster_store::ClusterResourceScopeRead> {
        self.resource_scopes.clone()
    }
}

/// Build test-only passive read ports directly from the SQLite destination
/// adapter.
#[cfg(test)]
pub(crate) fn sqlite_passive_read_ports(
    db: &klights_cluster_datastore::sqlite::embedded::Datastore,
) -> PassiveReadPorts {
    let focused_reads = db.focused_read_store();
    PassiveReadPorts::new(
        focused_reads.clone(),
        focused_reads.clone(),
        focused_reads.clone(),
        focused_reads,
    )
}

/// Test-only root composition bridge for an already-created SQLite fixture.
/// It exposes the same fixed focused bundle as production selection, never
/// the legacy backend handle.
#[cfg(test)]
pub(crate) fn sqlite_opened_passive_store(
    db: &klights_cluster_datastore::sqlite::embedded::Datastore,
) -> OpenedPassiveStore {
    OpenedPassiveStore::from_sqlite(db.clone())
}

/// Root-only result of selecting one passive persistence backend.
///
pub(crate) struct OpenedPassiveStore {
    pub(crate) read_ports: PassiveReadPorts,
    pub(crate) ownership_reads: Arc<dyn klights_cluster_store::ClusterOwnershipRead>,
    pub(crate) namespace_content_reads: Arc<dyn klights_cluster_store::NamespaceContentRead>,
    pub(crate) topology_reads: Arc<dyn klights_cluster_store::ClusterTopologyRead>,
    pub(crate) resource_mutations: Arc<dyn klights_cluster_store::ClusterResourceMutation>,
    pub(crate) watch_maintenance: Arc<dyn klights_cluster_store::ClusterWatchMaintenance>,
    pub(crate) topology_mutations: Arc<dyn klights_cluster_store::ClusterTopologyMutation>,
    pub(crate) pod_cleanup: Arc<dyn klights_cluster_store::ClusterPodCleanupStore>,
    pub(crate) applied_outbox: Arc<dyn klights_cluster_store::AppliedOutboxLedger>,
    pub(crate) metadata_mutations: Arc<dyn klights_cluster_store::ClusterMetadataMutation>,
    pub(crate) committed_apply: Arc<dyn klights_cluster_store::PrivilegedCommittedRaftApply>,
    pub(crate) snapshot_capture: Arc<dyn klights_cluster_store::AuthoritativeSnapshotCapture>,
    pub(crate) snapshot_persistence:
        Arc<dyn klights_cluster_store::AuthoritativeSnapshotPersistence>,
    pub(crate) metadata_reads: Arc<dyn klights_cluster_store::ClusterMetadataRead>,
    pub(crate) lifecycle: Arc<dyn klights_cluster_store::BackendLifecycleStore>,
}

impl OpenedPassiveStore {
    fn from_sqlite(ds: klights_cluster_datastore::sqlite::embedded::Datastore) -> Self {
        let ds = Arc::new(ds);
        let reads = ds.focused_read_store();
        let recovery = ds.focused_recovery_store();
        Self {
            read_ports: PassiveReadPorts::new(
                reads.clone(),
                reads.clone(),
                reads.clone(),
                reads.clone(),
            ),
            ownership_reads: reads.clone(),
            namespace_content_reads: reads.clone(),
            topology_reads: reads.clone(),
            resource_mutations: ds.clone(),
            watch_maintenance: ds.clone(),
            topology_mutations: ds.clone(),
            pod_cleanup: ds.clone(),
            applied_outbox: ds.clone(),
            metadata_mutations: ds.clone(),
            committed_apply: ds.focused_committed_apply(),
            snapshot_capture: recovery.clone(),
            snapshot_persistence: recovery.clone(),
            metadata_reads: recovery,
            lifecycle: ds,
        }
    }

    fn from_redb(ds: klights_cluster_datastore::redb::embedded::RedbDatastore) -> Self {
        let ds = Arc::new(ds);
        let reads = ds.focused_read_store();
        let recovery = ds.focused_recovery_store();
        Self {
            read_ports: PassiveReadPorts::new(
                reads.clone(),
                reads.clone(),
                reads.clone(),
                reads.clone(),
            ),
            ownership_reads: reads.clone(),
            namespace_content_reads: reads.clone(),
            topology_reads: reads.clone(),
            resource_mutations: ds.clone(),
            watch_maintenance: ds.clone(),
            topology_mutations: ds.clone(),
            pod_cleanup: ds.clone(),
            applied_outbox: ds.clone(),
            metadata_mutations: ds.clone(),
            committed_apply: ds.focused_committed_apply(),
            snapshot_capture: recovery.clone(),
            snapshot_persistence: recovery.clone(),
            metadata_reads: recovery,
            lifecycle: ds,
        }
    }
}

/// Open the already-selected passive cluster datastore adapter.
///
/// Every variant returns the same fixed focused composition bundle. This
/// dispatch never reads ambient configuration or installs replication behavior.
pub(crate) async fn open_with_sink(
    request: PassiveStoreOpenRequest<'_>,
    supervisor: Arc<TaskSupervisor>,
    #[cfg(test)] commit_sink: Arc<dyn klights_cluster_store::CommitObservationSink>,
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
            #[cfg(test)]
            let ds = klights_cluster_datastore::sqlite::embedded::Datastore::new_in_memory_with_watch_and_executor_with_sink(
                executor,
                commit_sink,
                outbox_codec,
                Arc::new(klights_supervisor::SystemWallClock),
            )
            .await?;
            #[cfg(not(test))]
            let ds = klights_cluster_datastore::sqlite::embedded::Datastore::new_in_memory_with_watch_and_executor(
                executor, outbox_codec, Arc::new(klights_supervisor::SystemWallClock),
            ).await?;
            Ok(OpenedPassiveStore::from_sqlite(ds))
        }
        PassiveStoreOpenRequest::SqlitePersistent { cluster_db_path } => {
            tracing::info!(backend = "sqlite", mode = "persistent", "opening datastore");
            #[cfg(test)]
            let ds = klights_cluster_datastore::sqlite::embedded::Datastore::new_persistent_paths_with_sink(
                cluster_db_path,
                supervisor,
                commit_sink,
                outbox_codec,
                Arc::new(klights_supervisor::SystemWallClock),
            )
            .await?;
            #[cfg(not(test))]
            let ds = klights_cluster_datastore::sqlite::embedded::Datastore::new_persistent_paths(
                cluster_db_path,
                supervisor,
                outbox_codec,
                Arc::new(klights_supervisor::SystemWallClock),
            )
            .await?;
            Ok(OpenedPassiveStore::from_sqlite(ds))
        }
        PassiveStoreOpenRequest::RedbInMemory => {
            tracing::info!(backend = "redb", mode = "in-memory", "opening datastore");
            #[cfg(test)]
            let ds = klights_cluster_datastore::redb::embedded::RedbDatastore::new_in_memory_with_supervisor_and_sink(
                supervisor,
                commit_sink,
                Arc::new(klights_supervisor::SystemWallClock),
            )
            .await?;
            #[cfg(not(test))]
            let ds = klights_cluster_datastore::redb::embedded::RedbDatastore::new_in_memory_with_supervisor(
                supervisor, Arc::new(klights_supervisor::SystemWallClock),
            ).await?;
            Ok(OpenedPassiveStore::from_redb(ds))
        }
        PassiveStoreOpenRequest::RedbPersistent { cluster_db_path } => {
            tracing::info!(backend = "redb", mode = "persistent", "opening datastore");
            #[cfg(test)]
            let ds =
                klights_cluster_datastore::redb::embedded::RedbDatastore::new_persistent_with_sink(
                    cluster_db_path,
                    supervisor,
                    commit_sink,
                    Arc::new(klights_supervisor::SystemWallClock),
                )
                .await?;
            #[cfg(not(test))]
            let ds = klights_cluster_datastore::redb::embedded::RedbDatastore::new_persistent(
                cluster_db_path,
                supervisor,
                Arc::new(klights_supervisor::SystemWallClock),
            )
            .await?;
            Ok(OpenedPassiveStore::from_redb(ds))
        }
    }
}

/// Root-composed canonical SQLite fixture for bootstrap tests that require
/// committed watch wakeups. The datastore remains the canonical embedded
/// implementation; only the root-owned test sink is composed here.
#[cfg(test)]
pub(crate) async fn canonical_sqlite_fixture()
-> anyhow::Result<klights_cluster_datastore::sqlite::embedded::Datastore> {
    let supervisor = Arc::new(TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let executor = klights_cluster_datastore::sqlite::open_in_memory(
        supervisor,
        "sqlite:root-canonical-fixture",
    )
    .await?;
    klights_cluster_datastore::sqlite::embedded::Datastore::new_in_memory_with_watch_and_executor_with_sink(
        executor,
        crate::bootstrap::watch_commit_wiring::new_sink(),
        crate::bootstrap::composition_adapters::outbox_response_codec_adapter::new_codec(),
        Arc::new(klights_supervisor::SystemWallClock),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::{
        OpenedPassiveStore, PassiveStoreOpenRequest, open_with_sink, sqlite_opened_passive_store,
    };
    use klights_cluster_store::{
        DurableWatchTarget, ResourceCollectionScope, ResourceListQuery, ResourceListRead,
        ResourceListRequest, WatchHistoryRead, WatchHistoryRequest,
    };
    use klights_supervisor::{TaskCategoryConfig, TaskSupervisor};
    use serde_json::json;
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    fn supervisor() -> Arc<TaskSupervisor> {
        Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()))
    }

    async fn canonical_sqlite_fixture() -> klights_cluster_datastore::sqlite::embedded::Datastore {
        let supervisor = supervisor();
        let executor = klights_cluster_datastore::sqlite::open_in_memory(
            supervisor,
            "sqlite:p10-3a-canonical-fixture",
        )
        .await
        .expect("open canonical SQLite fixture executor");
        klights_cluster_datastore::sqlite::embedded::Datastore::new_in_memory_with_watch_and_executor_with_sink(
            executor,
            crate::bootstrap::watch_commit_wiring::new_sink(),
            crate::bootstrap::composition_adapters::outbox_response_codec_adapter::new_codec(),
            Arc::new(klights_supervisor::SystemWallClock),
        )
        .await
        .expect("open canonical SQLite fixture")
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

    #[test]
    fn selector_never_returns_the_legacy_broad_backend_handle() {
        let source = include_str!("selector.rs");
        let forbidden = concat!("Datastore", "Handle");
        assert!(
            !source.contains(forbidden),
            "backend selection must return only its fixed focused composition bundle"
        );
    }

    #[test]
    fn root_sqlite_wrapper_has_no_remaining_owner() {
        let wrapper =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/datastore/sqlite/mod.rs");
        assert!(
            !wrapper.exists(),
            "P10.3a must delete the root SQLite wrapper after every consumer uses canonical ports"
        );
    }

    #[tokio::test]
    async fn sqlite_test_composition_accepts_the_canonical_embedded_store() {
        let sqlite = canonical_sqlite_fixture().await;
        let opened = sqlite_opened_passive_store(&sqlite);
        assert_eq!(
            opened
                .read_ports
                .allocator_reads()
                .read_allocator_state()
                .await
                .expect("canonical SQLite allocator state")
                .position()
                .resource_version,
            0
        );
        opened.lifecycle.close();
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
            let store = open_selected(request).await;
            assert_eq!(
                store
                    .read_ports
                    .allocator_reads()
                    .read_allocator_state()
                    .await
                    .unwrap()
                    .position()
                    .resource_version,
                0
            );
            store.lifecycle.close();
        }
    }

    #[tokio::test]
    async fn typed_persistent_requests_preserve_adapter_paths() {
        let dir = tempfile::tempdir().unwrap();
        let sqlite_cluster = dir.path().join("sqlite/cluster.db");
        let sqlite_node = dir.path().join("sqlite/node.db");
        let sqlite = open_selected(PassiveStoreOpenRequest::SqlitePersistent {
            cluster_db_path: &sqlite_cluster,
        })
        .await;
        assert_eq!(
            sqlite
                .read_ports
                .allocator_reads()
                .read_allocator_state()
                .await
                .unwrap()
                .position()
                .resource_version,
            0
        );
        sqlite.lifecycle.close();
        assert!(sqlite_cluster.is_file());
        assert!(
            !sqlite_node.exists(),
            "passive cluster-store open must not create node.db"
        );

        let redb_path = dir.path().join("redb/cluster.redb");
        let redb = open_selected(PassiveStoreOpenRequest::RedbPersistent {
            cluster_db_path: &redb_path,
        })
        .await;
        assert_eq!(
            redb.read_ports
                .allocator_reads()
                .read_allocator_state()
                .await
                .unwrap()
                .position()
                .resource_version,
            0
        );
        redb.lifecycle.close();
        assert!(redb_path.is_file());
    }

    #[tokio::test]
    async fn selected_focused_ports_preserve_list_watch_cursors_for_both_backends() {
        for request in [
            PassiveStoreOpenRequest::SqliteInMemory,
            PassiveStoreOpenRequest::RedbInMemory,
        ] {
            let opened = open_selected(request).await;
            let reads = opened.read_ports;
            let mutations = opened.resource_mutations;
            let lifecycle = opened.lifecycle;
            let start = reads
                .allocator_reads()
                .read_allocator_state()
                .await
                .expect("initial allocator")
                .position();

            for name in ["alpha", "beta"] {
                mutations
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
                focused_page
                    .items()
                    .iter()
                    .map(|resource| (resource.name.as_str(), resource.resource_version))
                    .collect::<Vec<_>>(),
                vec![("alpha", 1), ("beta", 2)]
            );
            assert_eq!(focused_page.snapshot().position().resource_version, 2);
            assert_eq!(focused_page.remaining_item_count(), None);

            let limit = NonZeroUsize::new(8).unwrap();
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
            let WatchHistoryRead::Events(focused_watch) = focused_watch else {
                panic!("focused retained cursor unexpectedly expired");
            };
            assert_eq!(
                focused_watch
                    .events()
                    .iter()
                    .map(|event| (
                        event.event.event_type(),
                        event.event.resource().name.as_str()
                    ))
                    .collect::<Vec<_>>(),
                vec![("ADDED", "alpha"), ("ADDED", "beta")]
            );

            let focused_allocator = reads
                .allocator_reads()
                .read_allocator_state()
                .await
                .expect("focused allocator");
            assert_eq!(focused_allocator.position(), focused_watch.next_position());
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
            lifecycle.close();
        }
    }
}
