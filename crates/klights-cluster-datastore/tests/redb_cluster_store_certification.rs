use std::sync::Arc;
use std::time::Duration;

use klights_cluster_core::{
    ClusterMetadata, LogApplyAppliedOutboxRow, LogApplyCommit, LogApplyMutation,
    LogApplyNamespaceRow, LogApplyNodeDataplaneRow, LogApplyNodeSubnetRow,
    LogApplyPodCleanupIntentRow, LogApplyResourceRow, LogApplyWatchEventRow, OutboxStreamWatermark,
    SnapshotRestoreOperation, StorageResponse, WatchReplayPosition,
};
use klights_cluster_datastore::redb::{
    RedbAccessor, RedbOrdinaryNamespaceStore, RedbOrdinaryResourceStore, RedbReadStore,
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
    ClusterTopologyRead, CommittedApplyError, CommittedRaftApplyRequest, DurableAllocatorRead,
    DurableApplyLedgerRead, DurableRawWatchHistoryRead, DurableReplayFloor,
    DurableWatchHistoryRead, DurableWatchRangeRead, DurableWatchTarget, NamespaceContentRead,
    OutboxResponseCodec, PrivilegedCommittedRaftApply, ResourceCollectionScope, ResourceGetRequest,
    ResourceListQuery, ResourceListRead, ResourceListRequest, ResourceVersionMatch,
    SnapshotCapturePageKind, SnapshotCaptureRequest, SnapshotMembership as CanonicalMembership,
    SnapshotPageLimit, SnapshotPersistenceError, WatchHistoryRead, WatchHistoryRequest,
};
use klights_supervisor::{SystemWallClock, TaskCategoryConfig, TaskSupervisor};
use redb::{ReadableDatabase, ReadableTable};

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
