use std::sync::Arc;
use std::time::Duration;

use klights_cluster_core::{
    ClusterMetadata, LogApplyAppliedOutboxRow, LogApplyCommit, LogApplyMutation,
    LogApplyNamespaceRow, LogApplyNodeDataplaneRow, LogApplyNodeSubnetRow,
    LogApplyPodCleanupIntentRow, LogApplyResourceKey, LogApplyResourceRow, LogApplyWatchEventRow,
    OutboxStreamWatermark, SnapshotRestoreOperation, StorageResponse, WatchReplayPosition,
};
use klights_cluster_datastore::redb::embedded::watch::RedbWatchStore;
use klights_cluster_datastore::redb::{
    RedbAccessor, RedbOpenOpts, RedbOrdinaryNamespaceStore, RedbOrdinaryResourceStore,
    RedbReadStore,
    live_committed_apply::{RedbLiveCommittedApplyStore, outbox_watermark_key},
    recovery::RedbRecoveryStore,
    tables,
};
use klights_cluster_datastore::sqlite::{
    self, SqliteReadStore,
    recovery::{SnapshotMembership, SnapshotMetadata, SqliteRecoveryStore},
};
use klights_cluster_store::{
    AppliedOutboxLookup, AuthoritativeSnapshot, AuthoritativeSnapshotCapture,
    AuthoritativeSnapshotPersistence, COMMAND_CODEC_V3_ACTIVATION_VALUE, ClusterMetadataRead,
    ClusterMetadataStoreError, ClusterOwnershipRead, ClusterResourceRead, ClusterResourceScopeRead,
    ClusterTopologyRead, CommittedApplyError, CommittedRaftApplyRequest, DatastoreSnapshotter,
    DurableAllocatorRead, DurableApplyLedgerRead, DurableRawWatchHistoryRead, DurableReplayFloor,
    DurableWatchHistoryRead, DurableWatchRangeRead, DurableWatchTarget, NamespaceContentRead,
    OutboxResponseCodec, PrivilegedCommittedRaftApply, ResourceCollectionKey,
    ResourceCollectionScope, ResourceContinuation, ResourceGetRequest, ResourceListQuery,
    ResourceListRead, ResourceListRequest, ResourceListSnapshot, ResourceVersionMatch,
    SnapshotCapturePageKind, SnapshotCaptureRequest, SnapshotExclusiveFence,
    SnapshotMembership as CanonicalMembership, SnapshotPageLimit, SnapshotPersistenceError,
    WatchHistoryRead, WatchHistoryRequest,
};
use klights_supervisor::{SystemWallClock, TaskCategoryConfig, TaskSupervisor};
use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata};

#[derive(Clone)]
struct JsonCodec;

impl OutboxResponseCodec for JsonCodec {
    fn encode(&self, response: &StorageResponse) -> Result<Vec<u8>, String> {
        serde_json::to_vec(response).map_err(|error| error.to_string())
    }

    fn decode(&self, bytes: &[u8]) -> Result<StorageResponse, String> {
        serde_json::from_slice(bytes).map_err(|error| error.to_string())
    }
}

fn supervisor() -> Arc<TaskSupervisor> {
    Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()))
}

async fn redb_components() -> (
    Arc<RedbAccessor>,
    RedbReadStore,
    RedbLiveCommittedApplyStore,
    RedbRecoveryStore,
) {
    let supervisor = supervisor();
    let database = klights_cluster_datastore::redb::open_in_memory(supervisor.as_ref())
        .await
        .unwrap();
    let accessor = Arc::new(RedbAccessor::new(Arc::new(database), supervisor));
    (
        accessor.clone(),
        RedbReadStore::new(accessor.clone()),
        RedbLiveCommittedApplyStore::new(accessor.clone()),
        RedbRecoveryStore::new(accessor, Arc::new(tokio::sync::Semaphore::new(2))),
    )
}

fn resource_row(name: &str, resource_version: i64, tier: &str) -> LogApplyResourceRow {
    LogApplyResourceRow {
        api_version: "v1".to_string(),
        kind: "ConfigMap".to_string(),
        namespace: Some("tenant-a".to_string()),
        name: name.to_string(),
        uid: format!("{name}-uid"),
        resource_version,
        data: serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "namespace": "tenant-a",
                "name": name,
                "uid": format!("{name}-uid"),
                "resourceVersion": resource_version.to_string(),
                "labels": {"tier": tier},
            }
        }),
        require_absent: false,
        require_existing: false,
        precondition_uid: None,
        precondition_resource_version: None,
        status_only: false,
    }
}

fn namespace_row(name: &str, resource_version: i64) -> LogApplyNamespaceRow {
    LogApplyNamespaceRow {
        name: name.to_string(),
        uid: format!("{name}-uid"),
        resource_version,
        data: serde_json::json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {
                "name": name,
                "uid": format!("{name}-uid"),
                "resourceVersion": resource_version.to_string(),
            }
        }),
    }
}

fn watch_event(row: &LogApplyResourceRow, event_id: i64) -> LogApplyWatchEventRow {
    LogApplyWatchEventRow {
        event_id: Some(event_id),
        api_version: row.api_version.clone(),
        kind: row.kind.clone(),
        namespace: row.namespace.clone(),
        name: row.name.clone(),
        resource_version: row.resource_version,
        event_type: "ADDED".to_string(),
        data: row.data.clone(),
    }
}

#[derive(Debug, Eq, PartialEq)]
struct ReadObservation {
    label_page_names: Vec<String>,
    label_cursor_position: WatchReplayPosition,
    historical_names: Vec<String>,
    namespace_identity: (String, String, String),
    watch_events: Vec<(i64, String)>,
    allocator_position: WatchReplayPosition,
}

async fn observe_read_contract<S>(store: &S) -> ReadObservation
where
    S: ClusterResourceRead + DurableWatchHistoryRead + DurableAllocatorRead,
{
    let first = store
        .list_resources(ResourceListRequest::new(
            "v1",
            "ConfigMap",
            ResourceCollectionScope::Namespace("tenant-a".to_string()),
            ResourceListQuery::try_new(
                Some("tier=frontend".to_string()),
                None,
                Some(1),
                None,
                ResourceVersionMatch::Any,
            )
            .unwrap(),
        ))
        .await
        .unwrap();
    let cursor = first.continuation().cloned().unwrap();
    let second = store
        .list_resources(ResourceListRequest::new(
            "v1",
            "ConfigMap",
            ResourceCollectionScope::Namespace("tenant-a".to_string()),
            ResourceListQuery::try_new(
                Some("tier=frontend".to_string()),
                None,
                Some(1),
                Some(cursor.clone()),
                ResourceVersionMatch::Exact(cursor.snapshot().resource_version()),
            )
            .unwrap(),
        ))
        .await
        .unwrap();
    let historical = store
        .list_resources(ResourceListRequest::new(
            "v1",
            "ConfigMap",
            ResourceCollectionScope::Namespace("tenant-a".to_string()),
            ResourceListQuery::try_new(None, None, None, None, ResourceVersionMatch::Exact(2))
                .unwrap(),
        ))
        .await
        .unwrap();
    assert!(matches!(historical, ResourceListRead::Historical(_)));
    let namespace = store
        .get_resource(ResourceGetRequest::new("v1", "Namespace", None, "tenant-a"))
        .await
        .unwrap()
        .unwrap();
    let history = match store
        .replay_watch_history(
            WatchHistoryRequest::new(
                vec![DurableWatchTarget::namespaced_in_namespace(
                    "v1",
                    "ConfigMap",
                    "tenant-a",
                )],
                WatchReplayPosition::default(),
                16,
            )
            .unwrap(),
        )
        .await
        .unwrap()
    {
        WatchHistoryRead::Events(page) => page,
        WatchHistoryRead::Expired => panic!("fresh certification history expired"),
    };

    ReadObservation {
        label_page_names: first
            .items()
            .iter()
            .chain(second.items())
            .map(|resource| resource.name.clone())
            .collect(),
        label_cursor_position: cursor.snapshot().position(),
        historical_names: historical
            .items()
            .iter()
            .map(|resource| resource.name.clone())
            .collect(),
        namespace_identity: (namespace.api_version, namespace.kind, namespace.name),
        watch_events: history
            .events()
            .iter()
            .map(|event| (event.position.event_id, event.event.resource().name.clone()))
            .collect(),
        allocator_position: store.read_allocator_state().await.unwrap().position(),
    }
}

async fn assert_expired_cursor_recovery_executes<S>(store: &S)
where
    S: ClusterResourceRead,
{
    let expired_cursor = ResourceContinuation::new(
        ResourceCollectionKey::new(Some("tenant-a"), "alpha"),
        ResourceListSnapshot::try_new(WatchReplayPosition {
            resource_version: 999,
            event_id: 999,
            resource_version_filter_through_event_id: 0,
        })
        .unwrap(),
    );
    let expired = store
        .list_resources(ResourceListRequest::new(
            "v1",
            "ConfigMap",
            ResourceCollectionScope::Namespace("tenant-a".to_string()),
            ResourceListQuery::try_new(
                None,
                None,
                Some(1),
                Some(expired_cursor),
                // A pinned continuation owns the replay position; Any is the
                // legal continuation mode and must still expire/recover at
                // that typed position on both backends.
                ResourceVersionMatch::Any,
            )
            .unwrap(),
        ))
        .await
        .unwrap();
    let ResourceListRead::Expired {
        oldest_available,
        replacement: Some(recovery),
        ..
    } = expired
    else {
        panic!("an unavailable pinned cursor must yield a typed recovery cursor: {expired:?}");
    };
    assert!(
        oldest_available > 0,
        "expiry diagnostics must carry an actual retained/history head, never zero"
    );
    assert_eq!(recovery.after().name(), "alpha");
    let recovery_after = recovery.after().name().to_string();
    let recovered = store
        .list_resources(ResourceListRequest::new(
            "v1",
            "ConfigMap",
            ResourceCollectionScope::Namespace("tenant-a".to_string()),
            ResourceListQuery::try_new_with_recovery(
                None,
                None,
                Some(1),
                None,
                Some(recovery),
                ResourceVersionMatch::Any,
            )
            .unwrap(),
        ))
        .await
        .unwrap();
    assert!(
        recovered.items()[0].name.as_str() > recovery_after.as_str(),
        "recovery must resume strictly after the expired opaque boundary"
    );
}

async fn seed_sqlite_reads() -> SqliteReadStore {
    let executor = sqlite::open_in_memory(supervisor(), "phase10f:sqlite-parity")
        .await
        .unwrap();
    let read_executor = executor.read_lane_clone();
    let recovery = SqliteRecoveryStore::new(
        executor,
        read_executor.clone(),
        None,
        Arc::new(tokio::sync::RwLock::new(())),
        Arc::new(JsonCodec),
    );
    let rows = [
        resource_row("alpha", 1, "frontend"),
        resource_row("beta", 2, "backend"),
        resource_row("gamma", 3, "frontend"),
    ];
    let mut operations = rows
        .iter()
        .enumerate()
        .map(|(index, row)| {
            SnapshotRestoreOperation::new(
                row.resource_version,
                None,
                vec![
                    LogApplyMutation::PutResource(row.clone()),
                    LogApplyMutation::PutWatchEvent(watch_event(
                        row,
                        i64::try_from(index + 1).unwrap(),
                    )),
                ],
            )
        })
        .collect::<Vec<_>>();
    let namespace = namespace_row("tenant-a", 4);
    operations.push(SnapshotRestoreOperation::new(
        4,
        None,
        vec![
            LogApplyMutation::PutNamespace(namespace.clone()),
            LogApplyMutation::PutWatchEvent(LogApplyWatchEventRow {
                event_id: Some(4),
                api_version: "v1".to_string(),
                kind: "Namespace".to_string(),
                namespace: None,
                name: namespace.name,
                resource_version: 4,
                event_type: "ADDED".to_string(),
                data: namespace.data,
            }),
        ],
    ));
    recovery
        .restore_snapshot_parts(
            operations,
            4,
            Some(4),
            Some(Vec::new()),
            Some(SnapshotMetadata {
                cluster_id: "phase10f-parity".to_string(),
                leader_epoch: 1,
                membership: SnapshotMembership::AuthoritativeAbsent,
                command_codec_activation_version: Some(3),
            }),
        )
        .await
        .unwrap();
    SqliteReadStore::new(read_executor)
}

async fn seed_redb_reads() -> RedbReadStore {
    let (accessor, reads, _, _) = redb_components().await;
    let resources = RedbOrdinaryResourceStore::new(accessor.clone(), Arc::new(SystemWallClock));
    for (name, tier) in [
        ("alpha", "frontend"),
        ("beta", "backend"),
        ("gamma", "frontend"),
    ] {
        resources
            .create_resource(
                "v1",
                "ConfigMap",
                Some("tenant-a"),
                name,
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {
                        "namespace": "tenant-a",
                        "name": name,
                        "uid": format!("{name}-uid"),
                        "labels": {"tier": tier},
                    }
                }),
            )
            .await
            .unwrap();
    }
    RedbOrdinaryNamespaceStore::new(accessor)
        .create_namespace(
            "tenant-a",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {"name": "tenant-a", "uid": "tenant-a-uid"},
            }),
        )
        .await
        .unwrap();
    reads
}

async fn seed_sqlite_historical_window() -> (SqliteReadStore, klights_supervisor::DbExecutor) {
    let executor = sqlite::open_in_memory(supervisor(), "historical-window-sqlite")
        .await
        .unwrap();
    let read_executor = executor.read_lane_clone();
    let recovery = SqliteRecoveryStore::new(
        executor,
        read_executor.clone(),
        None,
        Arc::new(tokio::sync::RwLock::new(())),
        Arc::new(JsonCodec),
    );
    let operations = (0..70)
        .map(|ordinal| {
            let name = format!("window-{ordinal:03}");
            let row = resource_row(
                &name,
                ordinal + 1,
                if ordinal == 69 { "selected" } else { "other" },
            );
            SnapshotRestoreOperation::new(
                row.resource_version,
                None,
                vec![
                    LogApplyMutation::PutResource(row.clone()),
                    LogApplyMutation::PutWatchEvent(watch_event(&row, ordinal + 1)),
                ],
            )
        })
        .collect::<Vec<_>>();
    recovery
        .restore_snapshot_parts(
            operations,
            70,
            Some(70),
            Some(Vec::new()),
            Some(SnapshotMetadata {
                cluster_id: "historical-window-sqlite".to_string(),
                leader_epoch: 1,
                membership: SnapshotMembership::AuthoritativeAbsent,
                command_codec_activation_version: Some(3),
            }),
        )
        .await
        .unwrap();
    (SqliteReadStore::new(read_executor.clone()), read_executor)
}

fn positioned_row(
    namespace: Option<&str>,
    name: &str,
    resource_version: i64,
    revision: &str,
    case: &str,
) -> LogApplyResourceRow {
    LogApplyResourceRow {
        api_version: "v1".to_string(),
        kind: "ConfigMap".to_string(),
        namespace: namespace.map(str::to_string),
        name: name.to_string(),
        uid: format!("{}-{name}-uid", namespace.unwrap_or("cluster")),
        resource_version,
        data: serde_json::json!({
            "apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {
                "namespace": namespace,
                "name": name,
                "uid": format!("{}-{name}-uid", namespace.unwrap_or("cluster")),
                "resourceVersion": resource_version.to_string(),
                "labels": {"case": case},
            },
            "data": {"revision": revision},
        }),
        require_absent: false,
        require_existing: false,
        precondition_uid: None,
        precondition_resource_version: None,
        status_only: false,
    }
}

fn positioned_watch_event(
    row: &LogApplyResourceRow,
    event_id: i64,
    resource_version: i64,
    event_type: &str,
) -> LogApplyWatchEventRow {
    LogApplyWatchEventRow {
        event_id: Some(event_id),
        api_version: row.api_version.clone(),
        kind: row.kind.clone(),
        namespace: row.namespace.clone(),
        name: row.name.clone(),
        resource_version,
        event_type: event_type.to_string(),
        data: row.data.clone(),
    }
}

fn delete_positioned_row(row: &LogApplyResourceRow) -> LogApplyResourceKey {
    LogApplyResourceKey {
        api_version: row.api_version.clone(),
        kind: row.kind.clone(),
        namespace: row.namespace.clone(),
        name: row.name.clone(),
        uid: row.uid.clone(),
        precondition_resource_version: None,
    }
}

async fn seed_positioned_sqlite_scope_matrix() -> SqliteReadStore {
    let executor = sqlite::open_in_memory(supervisor(), "positioned-scope-matrix-sqlite")
        .await
        .unwrap();
    let reads = executor.read_lane_clone();
    let recovery = SqliteRecoveryStore::new(
        executor,
        reads.clone(),
        None,
        Arc::new(tokio::sync::RwLock::new(())),
        Arc::new(JsonCodec),
    );
    let operations = positioned_scope_operations();
    recovery
        .restore_snapshot_parts(
            operations,
            12,
            Some(12),
            Some(Vec::new()),
            Some(SnapshotMetadata {
                cluster_id: "positioned-scope-matrix".to_string(),
                leader_epoch: 1,
                membership: SnapshotMembership::AuthoritativeAbsent,
                command_codec_activation_version: Some(3),
            }),
        )
        .await
        .unwrap();
    SqliteReadStore::new(reads)
}

fn positioned_scope_operations() -> Vec<SnapshotRestoreOperation> {
    let namespaced_deleted = positioned_row(Some("tenant-a"), "deleted", 1, "old", "deleted");
    let namespaced_changed_old = positioned_row(Some("tenant-a"), "changed", 2, "old", "changed");
    let namespaced_same = positioned_row(Some("tenant-a"), "same", 3, "old", "equal");
    let namespaced_same_other = positioned_row(Some("tenant-b"), "same", 4, "old", "equal");
    let namespaced_created_after =
        positioned_row(Some("tenant-a"), "born-later", 5, "new", "created");
    let namespaced_changed_new = positioned_row(Some("tenant-a"), "changed", 7, "new", "changed");
    let cluster_deleted = positioned_row(None, "deleted", 8, "old", "deleted");
    let cluster_changed_old = positioned_row(None, "changed", 9, "old", "changed");
    let cluster_created_after = positioned_row(None, "born-later", 10, "new", "created");
    let cluster_changed_new = positioned_row(None, "changed", 12, "new", "changed");
    vec![
        SnapshotRestoreOperation::new(
            1,
            None,
            vec![
                LogApplyMutation::PutResource(namespaced_deleted.clone()),
                LogApplyMutation::PutWatchEvent(positioned_watch_event(
                    &namespaced_deleted,
                    1,
                    1,
                    "ADDED",
                )),
            ],
        ),
        SnapshotRestoreOperation::new(
            2,
            None,
            vec![
                LogApplyMutation::PutResource(namespaced_changed_old.clone()),
                LogApplyMutation::PutWatchEvent(positioned_watch_event(
                    &namespaced_changed_old,
                    2,
                    2,
                    "ADDED",
                )),
            ],
        ),
        SnapshotRestoreOperation::new(
            3,
            None,
            vec![
                LogApplyMutation::PutResource(namespaced_same.clone()),
                LogApplyMutation::PutWatchEvent(positioned_watch_event(
                    &namespaced_same,
                    3,
                    3,
                    "ADDED",
                )),
            ],
        ),
        SnapshotRestoreOperation::new(
            4,
            None,
            vec![
                LogApplyMutation::PutResource(namespaced_same_other.clone()),
                LogApplyMutation::PutWatchEvent(positioned_watch_event(
                    &namespaced_same_other,
                    4,
                    4,
                    "ADDED",
                )),
            ],
        ),
        SnapshotRestoreOperation::new(
            5,
            None,
            vec![
                LogApplyMutation::PutResource(namespaced_created_after.clone()),
                LogApplyMutation::PutWatchEvent(positioned_watch_event(
                    &namespaced_created_after,
                    5,
                    5,
                    "ADDED",
                )),
            ],
        ),
        SnapshotRestoreOperation::new(
            6,
            None,
            vec![
                LogApplyMutation::DeleteResource(delete_positioned_row(&namespaced_deleted)),
                LogApplyMutation::PutWatchEvent(positioned_watch_event(
                    &namespaced_deleted,
                    6,
                    6,
                    "DELETED",
                )),
            ],
        ),
        SnapshotRestoreOperation::new(
            7,
            None,
            vec![
                LogApplyMutation::PutResource(namespaced_changed_new.clone()),
                LogApplyMutation::PutWatchEvent(positioned_watch_event(
                    &namespaced_changed_new,
                    7,
                    7,
                    "MODIFIED",
                )),
            ],
        ),
        SnapshotRestoreOperation::new(
            8,
            None,
            vec![
                LogApplyMutation::PutResource(cluster_deleted.clone()),
                LogApplyMutation::PutWatchEvent(positioned_watch_event(
                    &cluster_deleted,
                    8,
                    8,
                    "ADDED",
                )),
            ],
        ),
        SnapshotRestoreOperation::new(
            9,
            None,
            vec![
                LogApplyMutation::PutResource(cluster_changed_old.clone()),
                LogApplyMutation::PutWatchEvent(positioned_watch_event(
                    &cluster_changed_old,
                    9,
                    9,
                    "ADDED",
                )),
            ],
        ),
        SnapshotRestoreOperation::new(
            10,
            None,
            vec![
                LogApplyMutation::PutResource(cluster_created_after.clone()),
                LogApplyMutation::PutWatchEvent(positioned_watch_event(
                    &cluster_created_after,
                    10,
                    10,
                    "ADDED",
                )),
            ],
        ),
        SnapshotRestoreOperation::new(
            11,
            None,
            vec![
                LogApplyMutation::DeleteResource(delete_positioned_row(&cluster_deleted)),
                LogApplyMutation::PutWatchEvent(positioned_watch_event(
                    &cluster_deleted,
                    11,
                    11,
                    "DELETED",
                )),
            ],
        ),
        SnapshotRestoreOperation::new(
            12,
            None,
            vec![
                LogApplyMutation::PutResource(cluster_changed_new.clone()),
                LogApplyMutation::PutWatchEvent(positioned_watch_event(
                    &cluster_changed_new,
                    12,
                    12,
                    "MODIFIED",
                )),
            ],
        ),
    ]
}

async fn seed_positioned_redb_scope_matrix() -> RedbReadStore {
    let (accessor, reads, _, _) = redb_components().await;
    let resources = RedbOrdinaryResourceStore::new(accessor, Arc::new(SystemWallClock));
    let namespaced_deleted =
        create_positioned_redb(&resources, Some("tenant-a"), "deleted", "old", "deleted").await;
    let namespaced_changed =
        create_positioned_redb(&resources, Some("tenant-a"), "changed", "old", "changed").await;
    create_positioned_redb(&resources, Some("tenant-a"), "same", "old", "equal").await;
    create_positioned_redb(&resources, Some("tenant-b"), "same", "old", "equal").await;
    create_positioned_redb(&resources, Some("tenant-a"), "born-later", "new", "created").await;
    resources
        .delete_resource("v1", "ConfigMap", Some("tenant-a"), "deleted")
        .await
        .unwrap();
    resources
        .update_resource(
            "v1",
            "ConfigMap",
            Some("tenant-a"),
            "changed",
            positioned_row(Some("tenant-a"), "changed", 0, "new", "changed").data,
            namespaced_changed,
        )
        .await
        .unwrap();
    let cluster_deleted =
        create_positioned_redb(&resources, None, "deleted", "old", "deleted").await;
    let cluster_changed =
        create_positioned_redb(&resources, None, "changed", "old", "changed").await;
    create_positioned_redb(&resources, None, "born-later", "new", "created").await;
    resources
        .delete_resource("v1", "ConfigMap", None, "deleted")
        .await
        .unwrap();
    resources
        .update_resource(
            "v1",
            "ConfigMap",
            None,
            "changed",
            positioned_row(None, "changed", 0, "new", "changed").data,
            cluster_changed,
        )
        .await
        .unwrap();
    let _ = (namespaced_deleted, cluster_deleted);
    reads
}

async fn create_positioned_redb(
    resources: &RedbOrdinaryResourceStore,
    namespace: Option<&str>,
    name: &str,
    revision: &str,
    case: &str,
) -> i64 {
    resources
        .create_resource(
            "v1",
            "ConfigMap",
            namespace,
            name,
            positioned_row(namespace, name, 0, revision, case).data,
        )
        .await
        .unwrap()
        .0
        .resource_version
}

async fn assert_positioned_page_scope_matrix<S>(store: &S)
where
    S: ClusterResourceRead,
{
    for (scope, position, expected_names) in [
        (
            ResourceCollectionScope::Namespace("tenant-a".to_string()),
            4,
            vec!["changed", "deleted", "same"],
        ),
        (
            ResourceCollectionScope::AllNamespaces,
            4,
            vec!["changed", "deleted", "same", "same"],
        ),
        (
            ResourceCollectionScope::Cluster,
            9,
            vec!["changed", "deleted"],
        ),
    ] {
        let page = store
            .list_resources(ResourceListRequest::new(
                "v1",
                "ConfigMap",
                scope.clone(),
                ResourceListQuery::try_new(
                    None,
                    None,
                    Some(10),
                    None,
                    ResourceVersionMatch::Exact(position),
                )
                .unwrap(),
            ))
            .await
            .unwrap();
        assert!(matches!(page, ResourceListRead::Historical(_)));
        assert_eq!(
            page.items()
                .iter()
                .map(|item| item.name.as_str())
                .collect::<Vec<_>>(),
            expected_names
        );
        assert!(
            !page.items().iter().any(|item| item.name == "born-later"),
            "create-after-P must be excluded"
        );
        assert_eq!(
            page.items()
                .iter()
                .find(|item| item.name == "changed")
                .unwrap()
                .data
                .pointer("/data/revision")
                .and_then(serde_json::Value::as_str),
            Some("old"),
            "modified-after-P must reconstruct its predecessor"
        );
        assert!(
            page.items().iter().any(|item| item.name == "deleted"),
            "delete-after-P must resurrect the predecessor"
        );
    }
    let first = store
        .list_resources(ResourceListRequest::new(
            "v1",
            "ConfigMap",
            ResourceCollectionScope::AllNamespaces,
            ResourceListQuery::try_new(
                Some("case=equal".to_string()),
                None,
                Some(1),
                None,
                ResourceVersionMatch::Exact(4),
            )
            .unwrap(),
        ))
        .await
        .unwrap();
    let cursor = first
        .continuation()
        .cloned()
        .expect("equal names need a second page");
    let second = store
        .list_resources(ResourceListRequest::new(
            "v1",
            "ConfigMap",
            ResourceCollectionScope::AllNamespaces,
            ResourceListQuery::try_new(
                Some("case=equal".to_string()),
                None,
                Some(1),
                Some(cursor.clone()),
                ResourceVersionMatch::Any,
            )
            .unwrap(),
        ))
        .await
        .unwrap();
    assert_eq!(first.items()[0].namespace.as_deref(), Some("tenant-a"));
    assert_eq!(first.items()[0].name, "same");
    assert_eq!(second.snapshot(), Some(cursor.snapshot()));
    assert_eq!(second.items()[0].namespace.as_deref(), Some("tenant-b"));
    assert_eq!(second.items()[0].name, "same");
}

async fn seed_physical_bound_sqlite() -> SqliteReadStore {
    let executor = sqlite::open_in_memory(supervisor(), "physical-bound-sqlite")
        .await
        .unwrap();
    let reads = executor.read_lane_clone();
    let recovery = SqliteRecoveryStore::new(
        executor,
        reads.clone(),
        None,
        Arc::new(tokio::sync::RwLock::new(())),
        Arc::new(JsonCodec),
    );
    let operations = (1_i64..=256)
        .map(|resource_version| {
            let name = format!("physical-bound-{resource_version:03}");
            let row = positioned_row(
                Some("tenant-a"),
                &name,
                resource_version,
                "current",
                "physical-bound",
            );
            SnapshotRestoreOperation::new(
                resource_version,
                None,
                vec![
                    LogApplyMutation::PutResource(row.clone()),
                    LogApplyMutation::PutWatchEvent(positioned_watch_event(
                        &row,
                        resource_version,
                        resource_version,
                        "ADDED",
                    )),
                ],
            )
        })
        .collect();
    recovery
        .restore_snapshot_parts(
            operations,
            256,
            Some(256),
            Some(Vec::new()),
            Some(SnapshotMetadata {
                cluster_id: "physical-bound".to_string(),
                leader_epoch: 1,
                membership: SnapshotMembership::AuthoritativeAbsent,
                command_codec_activation_version: Some(3),
            }),
        )
        .await
        .unwrap();
    SqliteReadStore::new(reads)
}

async fn seed_physical_bound_redb() -> RedbReadStore {
    let (accessor, reads, _, _) = redb_components().await;
    let resources = RedbOrdinaryResourceStore::new(accessor, Arc::new(SystemWallClock));
    for ordinal in 1..=256 {
        let name = format!("physical-bound-{ordinal:03}");
        create_positioned_redb(
            &resources,
            Some("tenant-a"),
            &name,
            "current",
            "physical-bound",
        )
        .await;
    }
    reads
}

async fn assert_limit_two_progress<S>(store: &S, position: WatchReplayPosition)
where
    S: ClusterResourceRead,
{
    let request = |continuation, version| {
        ResourceListRequest::new(
            "v1",
            "ConfigMap",
            ResourceCollectionScope::Namespace("tenant-a".to_string()),
            ResourceListQuery::try_new(None, None, Some(2), continuation, version).unwrap(),
        )
    };
    let current = store
        .list_resources(request(None, ResourceVersionMatch::Any))
        .await
        .unwrap();
    assert_eq!(
        current
            .items()
            .iter()
            .map(|item| item.name.as_str())
            .collect::<Vec<_>>(),
        ["physical-bound-001", "physical-bound-002"]
    );
    let current_cursor = current.continuation().cloned().unwrap();
    let current_next = store
        .list_resources(request(Some(current_cursor), ResourceVersionMatch::Any))
        .await
        .unwrap();
    assert_eq!(
        current_next
            .items()
            .iter()
            .map(|item| item.name.as_str())
            .collect::<Vec<_>>(),
        ["physical-bound-003", "physical-bound-004"],
        "current keyset must advance strictly after its typed key"
    );
    let historical = store
        .list_resources(request(None, ResourceVersionMatch::AtPosition(position)))
        .await
        .unwrap();
    assert_eq!(
        historical
            .items()
            .iter()
            .map(|item| item.name.as_str())
            .collect::<Vec<_>>(),
        ["physical-bound-001", "physical-bound-002"]
    );
    let historical_cursor = historical.continuation().cloned().unwrap();
    let historical_next = store
        .list_resources(request(Some(historical_cursor), ResourceVersionMatch::Any))
        .await
        .unwrap();
    assert_eq!(
        historical_next
            .items()
            .iter()
            .map(|item| item.name.as_str())
            .collect::<Vec<_>>(),
        ["physical-bound-003", "physical-bound-004"],
        "positioned keyset must advance strictly after its typed key"
    );
}

#[tokio::test]
async fn positive_limit_pages_decode_only_bounded_physical_windows() {
    let sqlite = seed_physical_bound_sqlite().await;
    let sqlite_position = sqlite.read_allocator_state().await.unwrap().position();
    sqlite::reset_physical_bound_counters_for_test();
    assert_limit_two_progress(&sqlite, sqlite_position).await;
    let sqlite_bounds = sqlite::physical_bound_counters_for_test();
    assert!(
        sqlite_bounds.resource_decodes <= 6,
        "two current pages decode at most limit+1 each"
    );
    assert!(
        sqlite_bounds.event_decodes <= 9,
        "each of the three positioned turns decodes at most limit+1 events: {sqlite_bounds:?}"
    );
    assert!(sqlite_bounds.candidate_batch_max <= 64);
    assert!(sqlite_bounds.history_batch_max <= 64);

    let redb = seed_physical_bound_redb().await;
    let redb_position = redb.read_allocator_state().await.unwrap().position();
    klights_cluster_datastore::redb::read_core::reset_physical_bound_counters_for_test();
    assert_limit_two_progress(&redb, redb_position).await;
    let redb_bounds =
        klights_cluster_datastore::redb::read_core::physical_bound_counters_for_test();
    assert!(
        redb_bounds.resource_decodes <= 6,
        "two current pages decode at most limit+1 each"
    );
    assert!(
        redb_bounds.event_decodes <= 9,
        "each of the three positioned turns decodes at most limit+1 events: {redb_bounds:?}"
    );
    assert!(redb_bounds.candidate_batch_max <= 64);
    assert!(redb_bounds.history_batch_max <= 64);
}

#[tokio::test]
async fn sqlite_exact_list_filters_nonmonotonic_rv_events_through_one_apply_head() {
    let executor = sqlite::open_in_memory(supervisor(), "exact-filter-through-sqlite")
        .await
        .unwrap();
    let reads = executor.read_lane_clone();
    let recovery = SqliteRecoveryStore::new(
        executor,
        reads.clone(),
        None,
        Arc::new(tokio::sync::RwLock::new(())),
        Arc::new(JsonCodec),
    );
    let alpha_v3 = positioned_row(Some("tenant-a"), "alpha", 3, "v3", "exact");
    let alpha_v5 = positioned_row(Some("tenant-a"), "alpha", 5, "v5", "exact");
    let beta_v3 = positioned_row(Some("tenant-a"), "beta", 3, "v3", "exact");
    recovery
        .restore_snapshot_parts(
            vec![
                SnapshotRestoreOperation::new(
                    3,
                    None,
                    vec![
                        LogApplyMutation::PutResource(alpha_v3.clone()),
                        LogApplyMutation::PutWatchEvent(positioned_watch_event(
                            &alpha_v3, 1, 3, "ADDED",
                        )),
                    ],
                ),
                SnapshotRestoreOperation::new(
                    5,
                    None,
                    vec![
                        LogApplyMutation::PutResource(alpha_v5.clone()),
                        LogApplyMutation::PutWatchEvent(positioned_watch_event(
                            &alpha_v5, 2, 5, "MODIFIED",
                        )),
                    ],
                ),
                SnapshotRestoreOperation::new(
                    3,
                    None,
                    vec![
                        LogApplyMutation::PutResource(beta_v3.clone()),
                        LogApplyMutation::PutWatchEvent(positioned_watch_event(
                            &beta_v3, 3, 3, "ADDED",
                        )),
                    ],
                ),
            ],
            5,
            Some(3),
            Some(Vec::new()),
            Some(SnapshotMetadata {
                cluster_id: "exact-filter-through".to_string(),
                leader_epoch: 1,
                membership: SnapshotMembership::AuthoritativeAbsent,
                command_codec_activation_version: Some(3),
            }),
        )
        .await
        .unwrap();

    let reads = SqliteReadStore::new(reads);
    let page = ClusterResourceRead::list_resources(
        &reads,
        ResourceListRequest::new(
            "v1",
            "ConfigMap",
            ResourceCollectionScope::Namespace("tenant-a".to_string()),
            ResourceListQuery::try_new(None, None, Some(2), None, ResourceVersionMatch::Exact(3))
                .unwrap(),
        ),
    )
    .await
    .unwrap();
    assert_eq!(page.snapshot().unwrap().resource_version(), 3);
    assert_eq!(page.snapshot().unwrap().position().event_id, 0);
    assert_eq!(
        page.snapshot()
            .unwrap()
            .position()
            .resource_version_filter_through_event_id,
        3
    );
    assert_eq!(
        page.items()
            .iter()
            .map(|item| (
                item.name.as_str(),
                item.data
                    .pointer("/data/revision")
                    .and_then(serde_json::Value::as_str)
            ))
            .collect::<Vec<_>>(),
        [("alpha", Some("v3")), ("beta", Some("v3"))]
    );
}

#[test]
fn redb_complete_cluster_store_port_matrix_is_explicit() {
    fn read_ports<T>()
    where
        T: ClusterResourceRead
            + ClusterResourceScopeRead
            + ClusterOwnershipRead
            + NamespaceContentRead
            + DurableWatchHistoryRead
            + DurableWatchRangeRead
            + DurableRawWatchHistoryRead
            + DurableAllocatorRead
            + ClusterTopologyRead,
    {
    }
    fn apply_ports<T: PrivilegedCommittedRaftApply + DurableApplyLedgerRead>() {}
    fn recovery_ports<
        T: AuthoritativeSnapshotCapture + AuthoritativeSnapshotPersistence + ClusterMetadataRead,
    >() {
    }

    read_ports::<RedbReadStore>();
    apply_ports::<RedbLiveCommittedApplyStore>();
    recovery_ports::<RedbRecoveryStore>();
}

#[tokio::test]
async fn redb_resource_pages_history_and_allocators_match_sqlite_contract() {
    let sqlite = observe_read_contract(&seed_sqlite_reads().await).await;
    let redb = observe_read_contract(&seed_redb_reads().await).await;
    assert_eq!(redb, sqlite);
    assert_eq!(redb.label_page_names, ["alpha", "gamma"]);
    assert_eq!(redb.historical_names, ["alpha", "beta"]);
    assert_eq!(
        redb.namespace_identity,
        (
            "v1".to_string(),
            "Namespace".to_string(),
            "tenant-a".to_string()
        )
    );
    assert_eq!(
        redb.watch_events,
        [
            (1, "alpha".to_string()),
            (2, "beta".to_string()),
            (3, "gamma".to_string())
        ]
    );
    assert_eq!(redb.allocator_position.resource_version, 4);
    assert_eq!(redb.allocator_position.event_id, 4);
}

#[tokio::test]
async fn positioned_pages_restore_membership_and_predecessors_for_every_scope_on_both_backends() {
    assert_positioned_page_scope_matrix(&seed_positioned_sqlite_scope_matrix().await).await;
    assert_positioned_page_scope_matrix(&seed_positioned_redb_scope_matrix().await).await;
}

#[tokio::test]
async fn redb_executes_typed_list_recovery_after_expiry() {
    assert_expired_cursor_recovery_executes(&seed_redb_reads().await).await;
}

/// The Redb positioned-page reader must never fall back to scanning the
/// global event log.  Applied history is materialized in identity/key order
/// so a page can reverse only its bounded candidate window.
#[tokio::test]
async fn redb_open_creates_the_ordered_resource_history_index() {
    let (accessor, _, _, _) = redb_components().await;
    accessor
        .call("redb-certify:history-index", |database| {
            let read = database.begin_read()?;
            let _ = read.open_table(tables::RESOURCE_HISTORY_BY_IDENTITY)?;
            Ok(())
        })
        .await
        .expect("open must provision the derived ordered history index");
}

#[tokio::test]
async fn redb_current_all_namespace_page_uses_a_composite_keyset_cursor() {
    let (accessor, reads, _, _) = redb_components().await;
    let resources = RedbOrdinaryResourceStore::new(accessor, Arc::new(SystemWallClock));
    for namespace in ["tenant-a", "tenant-b"] {
        resources
            .create_resource(
                "v1",
                "ConfigMap",
                Some(namespace),
                "same",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {"namespace": namespace, "name": "same"},
                }),
            )
            .await
            .unwrap();
    }
    let first = reads
        .list_resources(ResourceListRequest::new(
            "v1",
            "ConfigMap",
            ResourceCollectionScope::AllNamespaces,
            ResourceListQuery::try_new(None, None, Some(1), None, ResourceVersionMatch::Any)
                .unwrap(),
        ))
        .await
        .unwrap();
    let cursor = first.continuation().cloned().expect("first current page");
    assert_eq!(cursor.after().namespace(), Some("tenant-a"));
    let second = reads
        .list_resources(ResourceListRequest::new(
            "v1",
            "ConfigMap",
            ResourceCollectionScope::AllNamespaces,
            ResourceListQuery::try_new(
                None,
                None,
                Some(1),
                Some(cursor),
                ResourceVersionMatch::Any,
            )
            .unwrap(),
        ))
        .await
        .unwrap();
    assert_eq!(second.items()[0].namespace.as_deref(), Some("tenant-b"));
}

#[tokio::test]
async fn redb_current_lexical_cursor_cannot_skip_a_shorter_later_name() {
    let (accessor, reads, _, _) = redb_components().await;
    let resources = RedbOrdinaryResourceStore::new(accessor, Arc::new(SystemWallClock));
    for name in ["z", "aa"] {
        resources.create_resource("v1", "ConfigMap", Some("tenant-a"), name,
            serde_json::json!({"apiVersion":"v1","kind":"ConfigMap","metadata":{"namespace":"tenant-a","name":name}})
        ).await.unwrap();
    }
    let first = reads
        .list_resources(ResourceListRequest::new(
            "v1",
            "ConfigMap",
            ResourceCollectionScope::Namespace("tenant-a".into()),
            ResourceListQuery::try_new(None, None, Some(1), None, ResourceVersionMatch::Any)
                .unwrap(),
        ))
        .await
        .unwrap();
    assert_eq!(first.items()[0].name, "aa");
    let second = reads
        .list_resources(ResourceListRequest::new(
            "v1",
            "ConfigMap",
            ResourceCollectionScope::Namespace("tenant-a".into()),
            ResourceListQuery::try_new(
                None,
                None,
                Some(1),
                first.continuation().cloned(),
                ResourceVersionMatch::Any,
            )
            .unwrap(),
        ))
        .await
        .unwrap();
    assert_eq!(second.items()[0].name, "z");
}

#[tokio::test]
async fn redb_positioned_page_keeps_stable_current_identity_after_its_old_event_is_compacted() {
    let (accessor, reads, _, _) = redb_components().await;
    RedbOrdinaryResourceStore::new(accessor.clone(), Arc::new(SystemWallClock))
        .create_resource("v1", "ConfigMap", Some("tenant-a"), "stable",
            serde_json::json!({"apiVersion":"v1","kind":"ConfigMap","metadata":{"namespace":"tenant-a","name":"stable"}})
        ).await.unwrap();
    let position = reads.read_allocator_state().await.unwrap().position();
    accessor
        .call("redb-certify:compact-stable-history-only", |database| {
            let write = database.begin_write()?;
            let ids = write
                .open_table(tables::WATCH_EVENTS)?
                .iter()?
                .map(|entry| entry.map(|(id, _)| id.value()))
                .collect::<Result<Vec<_>, _>>()?;
            let mut events = write.open_table(tables::WATCH_EVENTS)?;
            for id in ids {
                events.remove(id)?;
            }
            drop(events);
            let keys = write
                .open_table(tables::RESOURCE_HISTORY_BY_IDENTITY)?
                .iter()?
                .map(|entry| entry.map(|(key, _)| key.value().to_vec()))
                .collect::<Result<Vec<_>, _>>()?;
            let mut history = write.open_table(tables::RESOURCE_HISTORY_BY_IDENTITY)?;
            for key in keys {
                history.remove(key.as_slice())?;
            }
            drop(history);
            write.commit()?;
            Ok(())
        })
        .await
        .unwrap();
    let page = reads
        .list_resources(ResourceListRequest::new(
            "v1",
            "ConfigMap",
            ResourceCollectionScope::Namespace("tenant-a".into()),
            ResourceListQuery::try_new(
                None,
                None,
                Some(2),
                None,
                ResourceVersionMatch::AtPosition(position),
            )
            .unwrap(),
        ))
        .await
        .unwrap();
    assert_eq!(
        page.items()
            .iter()
            .map(|item| item.name.as_str())
            .collect::<Vec<_>>(),
        ["stable"]
    );
}

#[tokio::test]
async fn sqlite_positioned_page_keeps_stable_current_identity_after_its_old_event_is_compacted() {
    let (reads, executor) = seed_sqlite_historical_window().await;
    executor
        .call_raw("sqlite-certify:compact-stable-history-only", |connection| {
            connection.execute("DELETE FROM watch_events", [])?;
            Ok(())
        })
        .await
        .unwrap();
    let page = ClusterResourceRead::list_resources(
        &reads,
        ResourceListRequest::new(
            "v1",
            "ConfigMap",
            ResourceCollectionScope::Namespace("tenant-a".into()),
            ResourceListQuery::try_new(
                None,
                None,
                Some(2),
                None,
                ResourceVersionMatch::AtPosition(WatchReplayPosition {
                    resource_version: 70,
                    event_id: 70,
                    resource_version_filter_through_event_id: 0,
                }),
            )
            .unwrap(),
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        page.items()
            .iter()
            .map(|item| item.name.as_str())
            .collect::<Vec<_>>(),
        ["window-000", "window-001"]
    );
}

#[tokio::test]
async fn redb_apply_keeps_the_derived_history_index_in_lockstep() {
    let (accessor, _, _, _) = redb_components().await;
    RedbOrdinaryResourceStore::new(accessor.clone(), Arc::new(SystemWallClock))
        .create_resource(
            "v1",
            "ConfigMap",
            Some("tenant-a"),
            "indexed",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"namespace": "tenant-a", "name": "indexed"},
            }),
        )
        .await
        .unwrap();
    accessor
        .call("redb-certify:history-index-apply", |database| {
            let read = database.begin_read()?;
            let history = read.open_table(tables::RESOURCE_HISTORY_BY_IDENTITY)?;
            assert_eq!(history.len()?, 1, "one durable event has one index row");
            Ok(())
        })
        .await
        .expect("normal apply must atomically maintain derived history");
}

#[tokio::test]
async fn redb_normal_apply_gc_expires_historical_page_and_prunes_derived_history() {
    let (accessor, reads, _, _) = redb_components().await;
    let resources = RedbOrdinaryResourceStore::new(accessor.clone(), Arc::new(SystemWallClock));
    for ordinal in 0..6 {
        let name = format!("history-{ordinal}");
        resources
            .create_resource(
                "v1",
                "ConfigMap",
                Some("tenant-a"),
                &name,
                serde_json::json!({
                    "apiVersion": "v1", "kind": "ConfigMap",
                    "metadata": {"namespace": "tenant-a", "name": name},
                }),
            )
            .await
            .unwrap();
        if ordinal == 2 {
            let position = reads.read_allocator_state().await.unwrap().position();
            let page = reads
                .list_resources(ResourceListRequest::new(
                    "v1",
                    "ConfigMap",
                    ResourceCollectionScope::Namespace("tenant-a".into()),
                    ResourceListQuery::try_new(
                        None,
                        None,
                        Some(2),
                        None,
                        ResourceVersionMatch::AtPosition(position),
                    )
                    .unwrap(),
                ))
                .await
                .unwrap();
            assert!(matches!(page, ResourceListRead::Historical(_)));
            assert_eq!(
                page.items().len(),
                2,
                "historical page must honor its bound"
            );
        }
    }
    let historical_position = WatchReplayPosition {
        resource_version: 3,
        event_id: 3,
        resource_version_filter_through_event_id: 0,
    };
    assert!(
        RedbWatchStore::new(accessor.clone())
            .gc_watch(1, 100)
            .await
            .unwrap()
            >= 5
    );
    let expired = reads
        .list_resources(ResourceListRequest::new(
            "v1",
            "ConfigMap",
            ResourceCollectionScope::Namespace("tenant-a".into()),
            ResourceListQuery::try_new(
                None,
                None,
                Some(2),
                None,
                ResourceVersionMatch::AtPosition(historical_position),
            )
            .unwrap(),
        ))
        .await
        .unwrap();
    assert!(
        matches!(expired, ResourceListRead::Expired { .. }),
        "compacted positioned page must expire"
    );
    accessor
        .call(
            "redb-certify:derived-history-has-no-dangling-events",
            |database| {
                let read = database.begin_read()?;
                let events = read.open_table(tables::WATCH_EVENTS)?;
                let history = read.open_table(tables::RESOURCE_HISTORY_BY_IDENTITY)?;
                for entry in history.iter()? {
                    let (_, event_id) = entry?;
                    assert!(
                        events.get(event_id.value())?.is_some(),
                        "derived history must not reference GC-removed WATCH_EVENTS"
                    );
                }
                Ok(())
            },
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn redb_restore_and_reopen_preserve_the_current_derived_history_index() {
    let (source_accessor, source_reads, _, source_recovery) = redb_components().await;
    let source_resources =
        RedbOrdinaryResourceStore::new(source_accessor.clone(), Arc::new(SystemWallClock));
    for name in ["alpha", "beta", "gamma"] {
        source_resources
            .create_resource(
                "v1",
                "ConfigMap",
                Some("tenant-a"),
                name,
                serde_json::json!({
                    "apiVersion": "v1", "kind": "ConfigMap",
                    "metadata": {"namespace": "tenant-a", "name": name},
                }),
            )
            .await
            .unwrap();
    }
    let envelope = source_recovery
        .snapshot(SnapshotExclusiveFence::new(
            source_accessor.acquire_snapshot_exclusive().await,
        ))
        .await
        .unwrap();
    drop(source_reads);
    drop(source_recovery);
    drop(source_accessor);

    let temp = tempfile::tempdir().unwrap();
    let options = RedbOpenOpts {
        path: temp.path().join("restored.redb"),
        cache_size: 4 * 1024 * 1024,
    };
    let historical_request = || {
        ResourceListRequest::new(
            "v1",
            "ConfigMap",
            ResourceCollectionScope::Namespace("tenant-a".to_string()),
            ResourceListQuery::try_new(None, None, Some(2), None, ResourceVersionMatch::Exact(2))
                .unwrap(),
        )
    };

    let (expected_page, expected_marker, expected_index) = {
        let restore_supervisor = supervisor();
        let database = klights_cluster_datastore::redb::open_persistent(
            restore_supervisor.as_ref(),
            options.clone(),
        )
        .await
        .unwrap();
        let accessor = Arc::new(RedbAccessor::new(Arc::new(database), restore_supervisor));
        let recovery =
            RedbRecoveryStore::new(accessor.clone(), Arc::new(tokio::sync::Semaphore::new(2)));
        recovery
            .restore(
                &envelope,
                SnapshotExclusiveFence::new(accessor.acquire_snapshot_exclusive().await),
            )
            .await
            .unwrap();
        let reads = RedbReadStore::new(accessor.clone());
        let page = reads.list_resources(historical_request()).await.unwrap();
        assert!(matches!(page, ResourceListRead::Historical(_)));
        assert_eq!(
            page.items()
                .iter()
                .map(|item| item.name.as_str())
                .collect::<Vec<_>>(),
            ["alpha", "beta"],
        );
        let (marker, index) = derived_history_state(&accessor).await;
        assert_eq!(marker.as_deref(), Some(b"3".as_slice()));
        assert_eq!(
            index.len(),
            3,
            "restore must rebuild one identity row per event"
        );
        (page, marker, index)
    };

    klights_cluster_datastore::redb::mutation_helpers::
        reset_resource_history_index_rebuild_count_for_test();
    let reopen_supervisor = supervisor();
    let reopened_database =
        klights_cluster_datastore::redb::open_persistent(reopen_supervisor.as_ref(), options)
            .await
            .unwrap();
    let reopened_accessor = Arc::new(RedbAccessor::new(
        Arc::new(reopened_database),
        reopen_supervisor,
    ));
    let reopened_reads = RedbReadStore::new(reopened_accessor.clone());
    let reopened_page = reopened_reads
        .list_resources(historical_request())
        .await
        .unwrap();
    assert_eq!(
        reopened_page.snapshot(),
        expected_page.snapshot(),
        "reopen must retain the pinned historical position",
    );
    assert_eq!(
        reopened_page
            .items()
            .iter()
            .map(|item| item.name.as_str())
            .collect::<Vec<_>>(),
        expected_page
            .items()
            .iter()
            .map(|item| item.name.as_str())
            .collect::<Vec<_>>(),
        "reopen must retain the bounded historical page",
    );
    assert_eq!(reopened_page.continuation(), expected_page.continuation());
    assert_eq!(
        derived_history_state(&reopened_accessor).await,
        (expected_marker, expected_index)
    );
    assert_eq!(
        klights_cluster_datastore::redb::mutation_helpers::
            resource_history_index_rebuild_count_for_test(),
        0,
        "matching high-water marker must avoid an unnecessary reopen rebuild",
    );
}

async fn derived_history_state(
    accessor: &Arc<RedbAccessor>,
) -> (Option<Vec<u8>>, Vec<(Vec<u8>, u64)>) {
    accessor
        .call("redb-certify:derived-history-state", |database| {
            let read = database.begin_read()?;
            let marker = read
                .open_table(tables::META)?
                .get("resource_history_index_v2_high_water")?
                .map(|value| value.value().to_vec());
            let events = read.open_table(tables::WATCH_EVENTS)?;
            let history = read.open_table(tables::RESOURCE_HISTORY_BY_IDENTITY)?;
            let index = history
                .iter()?
                .map(|entry| {
                    let (key, event_id) = entry?;
                    assert!(events.get(event_id.value())?.is_some());
                    Ok((key.value().to_vec(), event_id.value()))
                })
                .collect::<Result<Vec<_>, redb::Error>>()?;
            Ok((marker, index))
        })
        .await
        .unwrap()
}

/// A selector match beyond the first derived-index window proves that the
/// historical LIST keeps its scan cursor request-local, advances it strictly,
/// and exposes only the final emitted Kubernetes key as continuation.
#[tokio::test]
async fn redb_historical_selector_underfill_crosses_bounded_identity_windows() {
    let (accessor, reads, _, _) = redb_components().await;
    let resources = RedbOrdinaryResourceStore::new(accessor, Arc::new(SystemWallClock));
    for ordinal in 0..70 {
        let name = format!("window-{ordinal:03}");
        resources
            .create_resource(
                "v1",
                "ConfigMap",
                Some("tenant-a"),
                &name,
                serde_json::json!({
                    "apiVersion": "v1", "kind": "ConfigMap",
                    "metadata": {
                        "namespace": "tenant-a", "name": name,
                        "labels": {"selected": if ordinal == 69 { "yes" } else { "no" }},
                    },
                }),
            )
            .await
            .unwrap();
    }
    let current = ClusterResourceRead::list_resources(
        &reads,
        ResourceListRequest::new(
            "v1",
            "ConfigMap",
            ResourceCollectionScope::Namespace("tenant-a".to_string()),
            ResourceListQuery::try_new(
                Some("selected=yes".to_string()),
                None,
                Some(2),
                None,
                ResourceVersionMatch::Any,
            )
            .unwrap(),
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        current
            .items()
            .iter()
            .map(|item| item.name.as_str())
            .collect::<Vec<_>>(),
        ["window-069"],
        "current selector LIST must cross the first bounded identity window"
    );
    let page = ClusterResourceRead::list_resources(
        &reads,
        ResourceListRequest::new(
            "v1",
            "ConfigMap",
            ResourceCollectionScope::Namespace("tenant-a".to_string()),
            ResourceListQuery::try_new(
                Some("selected=yes".to_string()),
                None,
                Some(2),
                None,
                ResourceVersionMatch::Exact(70),
            )
            .unwrap(),
        ),
    )
    .await
    .unwrap();
    assert!(matches!(page, ResourceListRead::Historical(_)));
    assert_eq!(
        page.items()
            .iter()
            .map(|item| item.name.as_str())
            .collect::<Vec<_>>(),
        ["window-069"]
    );
    assert!(page.continuation().is_none());
}

#[tokio::test]
async fn redb_historical_limit_two_continuation_keeps_one_fixed_position() {
    let (accessor, reads, _, _) = redb_components().await;
    let resources = RedbOrdinaryResourceStore::new(accessor, Arc::new(SystemWallClock));
    for ordinal in 0..4 {
        let name = format!("page-{ordinal}");
        resources
            .create_resource(
                "v1", "ConfigMap", Some("tenant-a"), &name,
                serde_json::json!({"apiVersion":"v1","kind":"ConfigMap","metadata":{"namespace":"tenant-a","name":name}}),
            ).await.unwrap();
    }
    let first = reads
        .list_resources(ResourceListRequest::new(
            "v1",
            "ConfigMap",
            ResourceCollectionScope::Namespace("tenant-a".to_string()),
            ResourceListQuery::try_new(None, None, Some(2), None, ResourceVersionMatch::Exact(4))
                .unwrap(),
        ))
        .await
        .unwrap();
    let cursor = first
        .continuation()
        .cloned()
        .expect("limit two has a public cursor");
    assert_eq!(
        first
            .items()
            .iter()
            .map(|item| item.name.as_str())
            .collect::<Vec<_>>(),
        ["page-0", "page-1"]
    );
    let second = reads
        .list_resources(ResourceListRequest::new(
            "v1",
            "ConfigMap",
            ResourceCollectionScope::Namespace("tenant-a".to_string()),
            ResourceListQuery::try_new(
                None,
                None,
                Some(2),
                Some(cursor.clone()),
                ResourceVersionMatch::Any,
            )
            .unwrap(),
        ))
        .await
        .unwrap();
    assert_eq!(second.snapshot(), Some(cursor.snapshot()));
    assert_eq!(
        second
            .items()
            .iter()
            .map(|item| item.name.as_str())
            .collect::<Vec<_>>(),
        ["page-2", "page-3"]
    );
}

#[tokio::test]
async fn redb_compaction_between_bounded_windows_expires_without_snapshot_drift() {
    let (accessor, reads, _, _) = redb_components().await;
    let resources = RedbOrdinaryResourceStore::new(accessor.clone(), Arc::new(SystemWallClock));
    for ordinal in 0..70 {
        let name = format!("compact-{ordinal:03}");
        resources.create_resource("v1", "ConfigMap", Some("tenant-a"), &name,
            serde_json::json!({"apiVersion":"v1","kind":"ConfigMap","metadata":{"namespace":"tenant-a","name":name,"labels":{"selected":if ordinal == 69 {"yes"} else {"no"}}}})
        ).await.unwrap();
    }
    klights_cluster_datastore::redb::read_core::arm_historical_window_pause_for_test(
        WatchReplayPosition {
            resource_version: 1,
            event_id: 0,
            resource_version_filter_through_event_id: 70,
        },
    );
    let pending_reads = reads.clone();
    let pending = tokio::spawn(async move {
        pending_reads
            .list_resources(ResourceListRequest::new(
                "v1",
                "ConfigMap",
                ResourceCollectionScope::Namespace("tenant-a".to_string()),
                ResourceListQuery::try_new(
                    Some("selected=yes".to_string()),
                    None,
                    Some(2),
                    None,
                    ResourceVersionMatch::Exact(1),
                )
                .unwrap(),
            ))
            .await
            .unwrap()
    });
    klights_cluster_datastore::redb::read_core::wait_for_historical_window_pause_for_test().await;
    accessor
        .call("test:compact-between-historical-windows", |database| {
            let write = database.begin_write()?;
            let ids = {
                let events = write.open_table(tables::WATCH_EVENTS)?;
                events
                    .iter()?
                    .map(|entry| entry.map(|(id, _)| id.value()))
                    .collect::<Result<Vec<_>, _>>()?
            };
            {
                let mut events = write.open_table(tables::WATCH_EVENTS)?;
                for id in ids {
                    events.remove(id)?;
                }
            }
            let mut floors = write.open_table(tables::WATCH_REPLAY_FLOORS)?;
            floors.insert(&b"v1\0ConfigMap\0tenant-a"[..], 70)?;
            drop(floors);
            let mut positioned = write.open_table(tables::WATCH_REPLAY_POSITION_FLOORS)?;
            let mut encoded = Vec::new();
            encoded.extend_from_slice(&70_u64.to_be_bytes());
            encoded.extend_from_slice(&70_u64.to_be_bytes());
            positioned.insert(&b"v1\0ConfigMap\0tenant-a"[..], encoded.as_slice())?;
            drop(positioned);
            write.commit()?;
            Ok(())
        })
        .await
        .unwrap();
    klights_cluster_datastore::redb::read_core::resume_historical_window_pause_for_test();
    let result = pending.await.unwrap();
    assert!(matches!(
        result,
        ResourceListRead::Expired { requested: 1, .. }
    ));
    assert_eq!(
        klights_cluster_datastore::redb::read_core::historical_window_counts_for_test(),
        (1, 0),
        "the next bounded turn must fail at its floor check before decoding history"
    );
}

#[tokio::test]
async fn sqlite_historical_selector_underfill_crosses_bounded_identity_windows() {
    let (reads, _) = seed_sqlite_historical_window().await;
    let current = ClusterResourceRead::list_resources(
        &reads,
        ResourceListRequest::new(
            "v1",
            "ConfigMap",
            ResourceCollectionScope::Namespace("tenant-a".to_string()),
            ResourceListQuery::try_new(
                Some("tier=selected".to_string()),
                None,
                Some(2),
                None,
                ResourceVersionMatch::Any,
            )
            .unwrap(),
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        current
            .items()
            .iter()
            .map(|item| item.name.as_str())
            .collect::<Vec<_>>(),
        ["window-069"],
        "current selector LIST must cross the first bounded identity window"
    );
    let page = ClusterResourceRead::list_resources(
        &reads,
        ResourceListRequest::new(
            "v1",
            "ConfigMap",
            ResourceCollectionScope::Namespace("tenant-a".to_string()),
            ResourceListQuery::try_new(
                Some("tier=selected".to_string()),
                None,
                Some(2),
                None,
                ResourceVersionMatch::Exact(70),
            )
            .unwrap(),
        ),
    )
    .await
    .unwrap();
    assert!(matches!(page, ResourceListRead::Historical(_)));
    assert_eq!(
        page.items()
            .iter()
            .map(|item| item.name.as_str())
            .collect::<Vec<_>>(),
        ["window-069"]
    );
    assert!(page.continuation().is_none());
}

#[tokio::test]
async fn sqlite_compaction_between_bounded_windows_expires_without_snapshot_drift() {
    let (reads, executor) = seed_sqlite_historical_window().await;
    let head = reads.read_allocator_state().await.unwrap().position();
    let position = WatchReplayPosition {
        resource_version: 1,
        event_id: 0,
        resource_version_filter_through_event_id: head.event_id,
    };
    sqlite::arm_historical_window_pause_for_test(position);
    let task_reads = reads.clone();
    let pending = tokio::spawn(async move {
        ClusterResourceRead::list_resources(
            &task_reads,
            ResourceListRequest::new(
                "v1",
                "ConfigMap",
                ResourceCollectionScope::Namespace("tenant-a".to_string()),
                ResourceListQuery::try_new(
                    Some("tier=selected".to_string()),
                    None,
                    Some(2),
                    None,
                    ResourceVersionMatch::Exact(1),
                )
                .unwrap(),
            ),
        )
        .await
        .unwrap()
    });
    sqlite::wait_for_historical_window_pause_for_test().await;
    executor.call_raw("test:compact-between-historical-windows", |connection| {
        connection.execute("DELETE FROM watch_events WHERE api_version = 'v1' AND kind = 'ConfigMap' AND namespace = 'tenant-a'", [])?;
        connection.execute(
            "INSERT OR REPLACE INTO watch_replay_floors (api_version, kind, namespace_key, floor_rv, floor_event_id, floor_position_exact) VALUES ('v1', 'ConfigMap', 'tenant-a', 70, 70, 1)",
            [],
        )?;
        Ok(())
    }).await.unwrap();
    sqlite::resume_historical_window_pause_for_test();
    let result = pending.await.unwrap();
    assert!(matches!(
        result,
        ResourceListRead::Expired { requested: 1, .. }
    ));
    let (candidate_windows, history_windows) = sqlite::historical_window_counts_for_test();
    assert!(candidate_windows >= 1);
    assert!(history_windows >= 1);
}

#[tokio::test]
async fn sqlite_executes_typed_list_recovery_after_expiry() {
    // Keep this alongside the backend-parity fixture: recovery must execute
    // against the real SQLite focused read port, not merely be asserted from a
    // synthetic typed error.
    assert_expired_cursor_recovery_executes(&seed_sqlite_reads().await).await;
}

#[derive(Debug, Eq, PartialEq)]
struct RawRedbState {
    resource_rows: Vec<(Vec<u8>, u64, Vec<u8>)>,
    namespace_rows: Vec<(String, Vec<u8>)>,
    applied_rows: Vec<(String, Vec<u8>)>,
    watermark_rows: Vec<(Vec<u8>, i64)>,
    meta_rows: Vec<(String, Vec<u8>)>,
}

async fn raw_redb_state(accessor: &RedbAccessor) -> RawRedbState {
    accessor
        .call("phase10f:raw-state", |database| {
            let read = database.begin_read()?;
            let resource_rows = read
                .open_table(tables::RES_NS)?
                .iter()?
                .map(|entry| {
                    let (key, value) = entry?;
                    Ok((
                        key.value().to_vec(),
                        value.value().0,
                        value.value().1.to_vec(),
                    ))
                })
                .collect::<Result<_, redb::StorageError>>()?;
            let namespace_rows = read
                .open_table(tables::NAMESPACES)?
                .iter()?
                .map(|entry| {
                    let (key, value) = entry?;
                    Ok((key.value().to_string(), value.value().to_vec()))
                })
                .collect::<Result<_, redb::StorageError>>()?;
            let applied_rows = read
                .open_table(tables::APPLIED_OUTBOX)?
                .iter()?
                .map(|entry| {
                    let (key, value) = entry?;
                    Ok((key.value().to_string(), value.value().to_vec()))
                })
                .collect::<Result<_, redb::StorageError>>()?;
            let watermark_rows = read
                .open_table(tables::OUTBOX_STREAM_WATERMARKS)?
                .iter()?
                .map(|entry| {
                    let (key, value) = entry?;
                    Ok((key.value().to_vec(), value.value()))
                })
                .collect::<Result<_, redb::StorageError>>()?;
            let meta_rows = read
                .open_table(tables::META)?
                .iter()?
                .map(|entry| {
                    let (key, value) = entry?;
                    Ok((key.value().to_string(), value.value().to_vec()))
                })
                .collect::<Result<_, redb::StorageError>>()?;
            Ok(RawRedbState {
                resource_rows,
                namespace_rows,
                applied_rows,
                watermark_rows,
                meta_rows,
            })
        })
        .await
        .unwrap()
}

fn committed_apply_v1() -> LogApplyCommit {
    // This remains the frozen CommittedApplyV1 / codec-v3 contract.
    LogApplyCommit::try_new_with_watermark(
        vec![
            LogApplyMutation::PutNamespace(namespace_row("must-not-apply", 0)),
            LogApplyMutation::PutAppliedOutbox(LogApplyAppliedOutboxRow {
                idempotency_key: "must-not-apply".to_string(),
                subject_key: "Namespace/must-not-apply".to_string(),
                operation: "Create".to_string(),
                first_seen_ms: 1,
                applied_rv: None,
                result_proto: vec![3],
                status_stamp: Some(1),
            }),
        ],
        Some(OutboxStreamWatermark {
            client_id: "worker-a".to_string(),
            stream_id: 1,
            stream_seq: 1,
        }),
    )
    .unwrap()
}

#[tokio::test]
async fn redb_committed_apply_and_restore_fail_closed_without_mutation() {
    let (accessor, reads, apply, recovery) = redb_components().await;
    RedbOrdinaryResourceStore::new(accessor.clone(), Arc::new(SystemWallClock))
        .create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "stable",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "namespace": "default",
                    "name": "stable",
                    "uid": "stable-uid",
                }
            }),
        )
        .await
        .unwrap();
    let before = raw_redb_state(accessor.as_ref()).await;
    let before_position = reads.read_allocator_state().await.unwrap().position();

    assert!(matches!(
        apply
            .apply_committed_raft(CommittedRaftApplyRequest::new(committed_apply_v1()))
            .await,
        Err(CommittedApplyError::UnsupportedMode { .. })
    ));
    let replacement = AuthoritativeSnapshot::try_new(
        vec![SnapshotRestoreOperation::new(
            1,
            None,
            vec![LogApplyMutation::PutNamespace(namespace_row(
                "replacement",
                1,
            ))],
        )],
        Some(before_position),
        Some(Vec::new()),
        ClusterMetadata {
            cluster_id: "replacement-cluster".to_string(),
            leader_epoch: 1,
            current_rv: before_position.resource_version,
        },
        CanonicalMembership::AuthoritativeAbsent,
    )
    .unwrap();
    assert!(matches!(
        recovery.restore_authoritative_snapshot(replacement).await,
        Err(SnapshotPersistenceError::UnsupportedMode { .. })
    ));

    assert_eq!(raw_redb_state(accessor.as_ref()).await, before);
    assert_eq!(
        reads.read_allocator_state().await.unwrap().position(),
        before_position
    );
    assert!(
        reads
            .get_resource(ResourceGetRequest::new(
                "v1",
                "ConfigMap",
                Some("default".to_string()),
                "stable",
            ))
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn redb_ledger_metadata_and_capture_preserve_durable_state() {
    let (accessor, _reads, apply, recovery) = redb_components().await;
    let row = LogApplyAppliedOutboxRow {
        idempotency_key: "capture-ledger".to_string(),
        subject_key: "v1/ConfigMap/default/stable".to_string(),
        operation: "Create".to_string(),
        first_seen_ms: 1,
        applied_rv: Some(7),
        result_proto: vec![7, 8, 9],
        status_stamp: Some(11),
    };
    let watermark = OutboxStreamWatermark {
        client_id: "worker-capture".to_string(),
        stream_id: 3,
        stream_seq: 4,
    };
    for (key, value) in [
        (klights_cluster_store::CLUSTER_ID_META_KEY, "redb-certified"),
        (klights_cluster_store::LEADER_EPOCH_META_KEY, "2"),
        (
            klights_cluster_store::COMMAND_CODEC_ACTIVATION_VERSION_META_KEY,
            COMMAND_CODEC_V3_ACTIVATION_VALUE,
        ),
    ] {
        apply.set_klights_meta(key, value).await.unwrap();
    }
    let row_bytes = serde_json::to_vec(&row).unwrap();
    let watermark_key = outbox_watermark_key(&watermark.client_id, watermark.stream_id).unwrap();
    let pod_cleanup = LogApplyPodCleanupIntentRow {
        node_name: "cp-1".to_string(),
        namespace: "default".to_string(),
        pod_name: "cleanup".to_string(),
        pod_uid: "cleanup-uid".to_string(),
        reason: "NodeLost".to_string(),
        resource_version: 7,
        created_at_ms: 10,
        pod_data: serde_json::json!({"metadata": {"name": "cleanup", "uid": "cleanup-uid"}}),
    };
    let pod_cleanup_bytes = serde_json::to_vec(&pod_cleanup).unwrap();
    accessor
        .call("phase10f:seed-durable-families", move |database| {
            let write = database.begin_write()?;
            {
                let mut meta = write.open_table(tables::META)?;
                meta.insert("rv", b"7".as_slice())?;
                meta.insert("watch_event_id", b"9".as_slice())?;
            }
            write
                .open_table(tables::APPLIED_OUTBOX)?
                .insert("capture-ledger", row_bytes.as_slice())?;
            write
                .open_table(tables::OUTBOX_STREAM_WATERMARKS)?
                .insert(watermark_key.as_slice(), 4)?;
            let subnet = serde_json::to_vec(&serde_json::json!({
                "subnet": "10.42.1.0/24",
                "subnet_base_int": u32::from(std::net::Ipv4Addr::new(10, 42, 1, 0)),
                "gateway_ip": "10.42.1.1",
                "node_ip": "10.0.0.1",
                "mode": "root",
            }))?;
            write
                .open_table(tables::NODE_SUBNETS)?
                .insert("cp-1", subnet.as_slice())?;
            let dataplane = serde_json::to_vec(&serde_json::json!({
                "mode": "root",
                "encryption": "enabled",
                "public_key": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
                "endpoint": "10.0.0.1",
                "port": 51820,
            }))?;
            write
                .open_table(tables::NODE_DATAPLANE)?
                .insert("cp-1", dataplane.as_slice())?;
            write
                .open_table(tables::POD_CLEANUP_INTENTS)?
                .insert(b"cleanup-key".as_slice(), pod_cleanup_bytes.as_slice())?;
            let floor_key = b"*\0*\0*";
            let mut floor = Vec::with_capacity(16);
            floor.extend_from_slice(&1_u64.to_be_bytes());
            floor.extend_from_slice(&1_u64.to_be_bytes());
            write
                .open_table(tables::WATCH_REPLAY_POSITION_FLOORS)?
                .insert(floor_key.as_slice(), floor.as_slice())?;
            write.commit()?;
            Ok(())
        })
        .await
        .unwrap();

    assert_eq!(
        apply.current_apply_position().await.unwrap(),
        WatchReplayPosition {
            resource_version: 7,
            event_id: 9,
            resource_version_filter_through_event_id: 0,
        }
    );
    assert_eq!(
        apply
            .get_applied_outbox(AppliedOutboxLookup::new("capture-ledger"))
            .await
            .unwrap(),
        Some(row.clone())
    );
    assert_eq!(
        apply.list_outbox_watermarks().await.unwrap(),
        vec![watermark.clone()]
    );

    let metadata = ClusterMetadataRead::read_cluster_metadata(&recovery)
        .await
        .unwrap();
    assert_eq!(metadata.metadata().cluster_id, "redb-certified");
    assert_eq!(metadata.metadata().leader_epoch, 2);
    assert_eq!(metadata.metadata().current_rv, 7);
    assert_eq!(
        metadata.membership(),
        &CanonicalMembership::AuthoritativeAbsent
    );

    let request = SnapshotCaptureRequest::try_new(
        SnapshotPageLimit::try_new(1).unwrap(),
        Duration::from_secs(30),
    )
    .unwrap();
    let mut session = recovery.begin_capture(request).await.unwrap();
    assert_eq!(session.header().command_codec_activation_version(), Some(3));
    assert_eq!(session.header().position().resource_version, 7);
    assert_eq!(session.header().position().event_id, 9);
    let mut kinds = Vec::new();
    let mut mutations = Vec::new();
    let mut captured_ledger = Vec::new();
    let mut captured_watermarks = Vec::new();
    let mut floors = Vec::<DurableReplayFloor>::new();
    while let Some(page) = session.next_page().await.unwrap() {
        assert_eq!(page.len(), 1, "page limit must remain exact");
        kinds.push(page.kind());
        if let Some(rows) = page.operations() {
            mutations.extend(
                rows.iter()
                    .flat_map(|operation| operation.mutations().iter().cloned()),
            );
        }
        if let Some(rows) = page.applied_outbox() {
            captured_ledger.extend_from_slice(rows);
        }
        if let Some(rows) = page.outbox_watermarks() {
            captured_watermarks.extend_from_slice(rows);
        }
        if let Some(rows) = page.replay_floors() {
            floors.extend_from_slice(rows);
        }
    }
    for kind in [
        SnapshotCapturePageKind::Commits,
        SnapshotCapturePageKind::AppliedOutbox,
        SnapshotCapturePageKind::OutboxWatermarks,
        SnapshotCapturePageKind::ReplayFloors,
    ] {
        assert!(kinds.contains(&kind), "missing capture family {kind:?}");
    }
    for present in [
        mutations.iter().any(|mutation| {
            matches!(
                mutation,
                LogApplyMutation::PutNodeSubnet(LogApplyNodeSubnetRow { .. })
            )
        }),
        mutations.iter().any(|mutation| {
            matches!(
                mutation,
                LogApplyMutation::PutNodeDataplane(LogApplyNodeDataplaneRow { .. })
            )
        }),
        mutations.iter().any(|mutation| {
            matches!(
                mutation,
                LogApplyMutation::PutPodCleanupIntent(LogApplyPodCleanupIntentRow { .. })
            )
        }),
    ] {
        assert!(present);
    }
    assert_eq!(captured_ledger, [row]);
    assert_eq!(captured_watermarks, [watermark]);
    assert_eq!(floors, [DurableReplayFloor::all(1, 1, true).unwrap()]);
}

#[tokio::test]
async fn redb_malformed_durable_rows_fail_with_typed_contract_errors() {
    let (accessor, _reads, apply, recovery) = redb_components().await;
    assert!(matches!(
        ClusterMetadataRead::read_cluster_metadata(&recovery).await,
        Err(ClusterMetadataStoreError::Incomplete { .. })
    ));

    apply
        .set_klights_meta(klights_cluster_store::CLUSTER_ID_META_KEY, "typed-errors")
        .await
        .unwrap();
    apply
        .set_klights_meta(klights_cluster_store::LEADER_EPOCH_META_KEY, "not-a-number")
        .await
        .unwrap();
    assert!(matches!(
        ClusterMetadataRead::read_cluster_metadata(&recovery).await,
        Err(ClusterMetadataStoreError::CorruptData { .. })
    ));

    accessor
        .call("phase10f:seed-hostile-ledger-rows", |database| {
            let write = database.begin_write()?;
            write
                .open_table(tables::APPLIED_OUTBOX)?
                .insert("corrupt-ledger", b"{not-json".as_slice())?;
            write
                .open_table(tables::OUTBOX_STREAM_WATERMARKS)?
                .insert(b"malformed-key".as_slice(), 1)?;
            write.commit()?;
            Ok(())
        })
        .await
        .unwrap();
    assert!(matches!(
        apply
            .get_applied_outbox(AppliedOutboxLookup::new("corrupt-ledger"))
            .await,
        Err(CommittedApplyError::CorruptData { .. })
    ));
    assert!(matches!(
        DurableApplyLedgerRead::list_outbox_watermarks(&apply).await,
        Err(CommittedApplyError::CorruptData { .. })
    ));
}
