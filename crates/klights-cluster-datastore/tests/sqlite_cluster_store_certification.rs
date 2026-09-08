use std::sync::Arc;
use std::time::Duration;

use klights_cluster_core::{
    ClusterMembership, ClusterMetadata, LogApplyAppliedOutboxRow, LogApplyCommit, LogApplyMutation,
    LogApplyNamespaceRow, LogApplyNodeDataplaneRow, LogApplyNodeSubnetRow,
    LogApplyPodCleanupIntentRow, LogApplyResourceKey, LogApplyResourceRow, LogApplyWatchEventRow,
    NoPublicChangeReason, OutboxStreamWatermark, SnapshotRestoreOperation, StorageResponse,
    WatchReplayPosition,
};
use klights_cluster_datastore::sqlite::{
    self, SqliteApplyLedgerRead, SqliteReadStore,
    live_apply::SqliteLiveCommittedApplyStore,
    recovery::{SnapshotMembership, SnapshotMetadata, SqliteRecoveryStore, SqliteSnapshotFactory},
};
use klights_cluster_store::{
    AppliedOutboxLookup, AuthoritativeSnapshot, AuthoritativeSnapshotCapture,
    AuthoritativeSnapshotPersistence, COMMAND_CODEC_V3_ACTIVATION_VALUE, ClusterMetadataRead,
    ClusterResourceRead, ClusterTopologyRead, CommittedRaftApplyRequest, DurableAllocatorRead,
    DurableApplyLedgerRead, DurableReplayFloor, DurableWatchHistoryRead, DurableWatchTarget,
    NodeTopologyRequest, OutboxResponseCodec, PrivilegedCommittedRaftApply,
    ResourceCollectionScope, ResourceGetRequest, ResourceListQuery, ResourceListRead,
    ResourceListRequest, ResourceReadError, ResourceVersionMatch, SnapshotCaptureHeader,
    SnapshotCapturePage, SnapshotCapturePageKind, SnapshotCaptureRequest,
    SnapshotMembership as CanonicalMembership, SnapshotPageLimit, WatchHistoryRead,
    WatchHistoryRequest,
};
use klights_supervisor::sqlite_open::OpenOpts;
use klights_supervisor::{TaskCategoryConfig, TaskSupervisor};

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

fn namespace(name: &str, resource_version: i64) -> LogApplyNamespaceRow {
    LogApplyNamespaceRow {
        name: name.to_string(),
        uid: format!("uid-{name}"),
        resource_version,
        data: serde_json::json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {
                "name": name,
                "uid": format!("uid-{name}"),
            }
        }),
    }
}

fn committed_apply_v1(codec: &dyn OutboxResponseCodec) -> LogApplyCommit {
    LogApplyCommit::try_new_with_watermark(
        vec![
            LogApplyMutation::PutNamespace(namespace("certified", 0)),
            LogApplyMutation::PutAppliedOutbox(LogApplyAppliedOutboxRow {
                idempotency_key: "certified-apply".to_string(),
                subject_key: "Namespace/certified".to_string(),
                operation: "Create".to_string(),
                first_seen_ms: 10,
                applied_rv: None,
                result_proto: codec
                    .encode(&StorageResponse::Ack {
                        resource_version: 0,
                    })
                    .unwrap(),
                status_stamp: Some(17),
            }),
        ],
        Some(OutboxStreamWatermark {
            client_id: "worker-a".to_string(),
            stream_id: 1,
            stream_seq: 1,
        }),
    )
    .expect("CommittedApplyV1 template")
}

fn resource_row(
    kind: &str,
    namespace: Option<&str>,
    name: &str,
    uid: &str,
    resource_version: i64,
    labels: serde_json::Value,
) -> LogApplyResourceRow {
    LogApplyResourceRow {
        api_version: "v1".to_string(),
        kind: kind.to_string(),
        namespace: namespace.map(str::to_string),
        name: name.to_string(),
        uid: uid.to_string(),
        resource_version,
        data: serde_json::json!({
            "apiVersion": "v1",
            "kind": kind,
            "metadata": {
                "namespace": namespace,
                "name": name,
                "uid": uid,
                "resourceVersion": resource_version.to_string(),
                "labels": labels,
            }
        }),
        require_absent: false,
        require_existing: false,
        precondition_uid: None,
        precondition_resource_version: None,
        status_only: false,
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

fn status_commit(
    codec: &dyn OutboxResponseCodec,
    idempotency_key: &str,
    status_message: &str,
    status_stamp: i64,
    stream_seq: i64,
) -> LogApplyCommit {
    LogApplyCommit::try_new_with_watermark(
        vec![
            LogApplyMutation::PutResource(LogApplyResourceRow {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: Some("default".to_string()),
                name: "certified-status".to_string(),
                uid: "certified-status-uid".to_string(),
                resource_version: 0,
                data: serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "namespace": "default",
                        "name": "certified-status",
                        "uid": "certified-status-uid",
                    },
                    "status": {"phase": "Running", "message": status_message},
                }),
                require_absent: false,
                require_existing: true,
                precondition_uid: Some("certified-status-uid".to_string()),
                precondition_resource_version: None,
                status_only: true,
            }),
            LogApplyMutation::PutAppliedOutbox(LogApplyAppliedOutboxRow {
                idempotency_key: idempotency_key.to_string(),
                subject_key: "v1/Pod/default/certified-status/certified-status-uid".to_string(),
                operation: "PodStatus".to_string(),
                first_seen_ms: status_stamp,
                applied_rv: None,
                result_proto: codec
                    .encode(&StorageResponse::Ack {
                        resource_version: 0,
                    })
                    .unwrap(),
                status_stamp: Some(status_stamp),
            }),
        ],
        Some(OutboxStreamWatermark {
            client_id: "certified-worker".to_string(),
            stream_id: 7,
            stream_seq,
        }),
    )
    .unwrap()
}

fn snapshot_from_capture(
    header: &SnapshotCaptureHeader,
    pages: &[SnapshotCapturePage],
) -> AuthoritativeSnapshot {
    let current_rv = header.metadata().current_rv;
    let mut operations = Vec::new();
    let mut floors = Vec::new();
    for page in pages {
        if let Some(rows) = page.operations() {
            operations.extend_from_slice(rows);
        } else if let Some(rows) = page.applied_outbox() {
            operations.extend(rows.iter().cloned().map(|row| {
                SnapshotRestoreOperation::new(
                    current_rv,
                    None,
                    vec![LogApplyMutation::PutAppliedOutbox(row)],
                )
            }));
        } else if let Some(rows) = page.outbox_watermarks() {
            operations.extend(rows.iter().cloned().map(|watermark| {
                SnapshotRestoreOperation::new(current_rv, Some(watermark), Vec::new())
            }));
        } else if let Some(rows) = page.replay_floors() {
            floors.extend_from_slice(rows);
        }
    }
    AuthoritativeSnapshot::try_new_restore_envelope(
        operations,
        current_rv,
        Some(header.position()),
        Some(floors),
        Some(header.metadata().clone()),
        header.membership().clone(),
        header.command_codec_activation_version(),
    )
    .unwrap()
}

#[tokio::test]
async fn canonical_snapshot_restore_preserves_exact_v3_activation() {
    let executor = sqlite::open_in_memory(supervisor(), "phase12e:canonical-v3")
        .await
        .unwrap();
    let recovery = SqliteRecoveryStore::new(
        executor.clone(),
        executor.read_lane_clone(),
        None,
        Arc::new(tokio::sync::RwLock::new(())),
        Arc::new(JsonCodec),
    );
    let snapshot = AuthoritativeSnapshot::try_new_restore_envelope(
        Vec::new(),
        0,
        Some(WatchReplayPosition {
            resource_version: 0,
            event_id: 0,
            resource_version_filter_through_event_id: 0,
        }),
        Some(Vec::new()),
        Some(ClusterMetadata {
            cluster_id: "phase12e-v3".to_string(),
            leader_epoch: 1,
            current_rv: 0,
        }),
        CanonicalMembership::AuthoritativeAbsent,
        Some(3),
    )
    .unwrap();

    recovery
        .restore_authoritative_snapshot(snapshot)
        .await
        .unwrap();

    let activation = executor
        .call_raw("phase12e_read_activation", |connection| {
            connection
                .query_row(
                    "SELECT value FROM _klights_meta
                     WHERE key = 'command_codec_activation_version'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .map_err(klights_supervisor::DbError::from)
        })
        .await
        .unwrap();
    assert_eq!(activation, COMMAND_CODEC_V3_ACTIVATION_VALUE);
}

#[tokio::test]
async fn canonical_snapshot_restore_preserves_legacy_metadata_omission_and_current_rv() {
    let executor = sqlite::open_in_memory(supervisor(), "phase12e:canonical-legacy")
        .await
        .unwrap();
    let recovery = SqliteRecoveryStore::new(
        executor.clone(),
        executor.read_lane_clone(),
        None,
        Arc::new(tokio::sync::RwLock::new(())),
        Arc::new(JsonCodec),
    );
    let membership = ClusterMembership {
        cluster_id: "phase12e-legacy".to_string(),
        voters: vec!["cp-1".to_string()],
        term: 4,
        leader_hint: Some("cp-1".to_string()),
    };
    recovery
        .restore_snapshot_parts(
            Vec::new(),
            1,
            Some(0),
            Some(Vec::new()),
            Some(SnapshotMetadata {
                cluster_id: membership.cluster_id.clone(),
                leader_epoch: 3,
                membership: SnapshotMembership::Present(membership.clone()),
                command_codec_activation_version: Some(3),
            }),
        )
        .await
        .unwrap();

    let legacy = AuthoritativeSnapshot::try_new_restore_envelope(
        Vec::new(),
        7,
        None,
        None,
        None,
        CanonicalMembership::LegacyOmitted,
        None,
    )
    .unwrap();
    recovery
        .restore_authoritative_snapshot(legacy)
        .await
        .unwrap();

    let observed = recovery.read_cluster_metadata().await.unwrap();
    assert_eq!(observed.metadata().cluster_id, membership.cluster_id);
    assert_eq!(observed.metadata().leader_epoch, 3);
    assert_eq!(
        observed.membership(),
        &CanonicalMembership::Present(membership)
    );
    let current_rv = executor
        .call_raw("phase12e_read_current_rv", |connection| {
            connection
                .query_row(
                    "SELECT value FROM metadata WHERE key = 'resource_version'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .map_err(klights_supervisor::DbError::from)
        })
        .await
        .unwrap();
    assert_eq!(current_rv, "7");
}

#[tokio::test]
async fn sqlite_ports_share_committed_position_ledger_and_atomic_rollback() {
    let executor = sqlite::open_in_memory(supervisor(), "phase10e:apply")
        .await
        .unwrap();
    let read_executor = executor.read_lane_clone();
    let codec: Arc<dyn OutboxResponseCodec> = Arc::new(JsonCodec);
    let reads = SqliteReadStore::new(read_executor.clone());
    let apply = SqliteLiveCommittedApplyStore::new(executor.clone(), codec.clone());
    let ledger = SqliteApplyLedgerRead::new(read_executor);

    executor
        .call_raw("failure_injection", |connection| {
            connection.execute_batch(
                "CREATE TRIGGER failure_injection
                 BEFORE INSERT ON applied_outbox
                 BEGIN
                   SELECT RAISE(ABORT, 'failure_injection');
                 END;",
            )?;
            Ok(())
        })
        .await
        .unwrap();
    assert!(
        apply
            .apply_committed_raft(klights_cluster_store::CommittedRaftApplyRequest::new(
                committed_apply_v1(codec.as_ref()),
            ))
            .await
            .is_err()
    );
    assert_eq!(
        ledger.current_apply_position().await.unwrap(),
        WatchReplayPosition::from_resource_version(0),
        "the namespace and ledger transaction must rollback together"
    );
    assert!(
        ClusterResourceRead::get_resource(
            &reads,
            ResourceGetRequest::new("v1", "Namespace", None, "certified",),
        )
        .await
        .unwrap()
        .is_none()
    );

    executor
        .call_raw("drop_failure_injection", |connection| {
            connection.execute_batch("DROP TRIGGER failure_injection")?;
            Ok(())
        })
        .await
        .unwrap();
    let receipt = apply
        .apply_committed_raft(klights_cluster_store::CommittedRaftApplyRequest::new(
            committed_apply_v1(codec.as_ref()),
        ))
        .await
        .unwrap();
    assert_eq!(receipt.applied_resource_version(), Some(1));

    let position = ledger.current_apply_position().await.unwrap();
    assert_eq!(position.resource_version, 1);
    assert!(position.event_id > 0);
    let applied = ledger
        .get_applied_outbox(AppliedOutboxLookup::new("certified-apply"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(applied.applied_rv, Some(1));
    assert_eq!(applied.status_stamp, Some(17));
    assert_eq!(
        ledger.list_outbox_watermarks().await.unwrap(),
        vec![OutboxStreamWatermark {
            client_id: "worker-a".to_string(),
            stream_id: 1,
            stream_seq: 1,
        }]
    );
}

#[tokio::test]
async fn sqlite_resource_pages_history_and_allocators_share_one_exact_snapshot() {
    let executor = sqlite::open_in_memory(supervisor(), "phase10e:read-conformance")
        .await
        .unwrap();
    let read_executor = executor.read_lane_clone();
    let reads = SqliteReadStore::new(read_executor.clone());
    let recovery = SqliteRecoveryStore::new(
        executor,
        read_executor,
        None,
        Arc::new(tokio::sync::RwLock::new(())),
        Arc::new(JsonCodec),
    );
    let rows = [
        resource_row(
            "ConfigMap",
            Some("tenant-a"),
            "alpha",
            "alpha-uid",
            1,
            serde_json::json!({"tier": "frontend"}),
        ),
        resource_row(
            "ConfigMap",
            Some("tenant-a"),
            "beta",
            "beta-uid",
            2,
            serde_json::json!({"tier": "backend"}),
        ),
        resource_row(
            "ConfigMap",
            Some("tenant-a"),
            "gamma",
            "gamma-uid",
            3,
            serde_json::json!({"tier": "frontend"}),
        ),
        resource_row(
            "Secret",
            Some("tenant-a"),
            "same-name",
            "same-name-a-uid",
            4,
            serde_json::json!({}),
        ),
        resource_row(
            "Secret",
            Some("tenant-b"),
            "same-name",
            "same-name-b-uid",
            5,
            serde_json::json!({}),
        ),
    ];
    recovery
        .restore_snapshot_parts(
            rows.iter()
                .enumerate()
                .map(|(index, row)| {
                    let mut mutations = vec![
                        LogApplyMutation::PutResource(row.clone()),
                        LogApplyMutation::PutWatchEvent(watch_event(
                            row,
                            i64::try_from(index + 1).unwrap(),
                        )),
                    ];
                    if index == 4 {
                        let legacy_namespace = namespace("legacy", 5);
                        mutations.extend([
                            LogApplyMutation::PutNamespace(legacy_namespace.clone()),
                            LogApplyMutation::PutWatchEvent(LogApplyWatchEventRow {
                                event_id: Some(6),
                                api_version: "v1".to_string(),
                                kind: "Namespace".to_string(),
                                namespace: None,
                                name: "legacy".to_string(),
                                resource_version: 5,
                                event_type: "MODIFIED".to_string(),
                                data: legacy_namespace.data,
                            }),
                        ]);
                    }
                    SnapshotRestoreOperation::new(row.resource_version, None, mutations)
                })
                .collect(),
            5,
            Some(7),
            Some(Vec::new()),
            Some(SnapshotMetadata {
                cluster_id: "read-cluster".to_string(),
                leader_epoch: 1,
                membership: SnapshotMembership::AuthoritativeAbsent,
                command_codec_activation_version: Some(3),
            }),
        )
        .await
        .unwrap();

    struct SelectorCase {
        name: &'static str,
        label: Option<&'static str>,
        field: Option<&'static str>,
        expected: &'static [&'static str],
    }
    for case in [
        SelectorCase {
            name: "label",
            label: Some("tier=frontend"),
            field: None,
            expected: &["alpha", "gamma"],
        },
        SelectorCase {
            name: "field",
            label: None,
            field: Some("metadata.name=beta"),
            expected: &["beta"],
        },
        SelectorCase {
            name: "combined",
            label: Some("tier=frontend"),
            field: Some("metadata.name!=alpha"),
            expected: &["gamma"],
        },
    ] {
        let page = ClusterResourceRead::list_resources(
            &reads,
            ResourceListRequest::new(
                "v1",
                "ConfigMap",
                ResourceCollectionScope::Namespace("tenant-a".to_string()),
                ResourceListQuery::try_new(
                    case.label.map(str::to_string),
                    case.field.map(str::to_string),
                    None,
                    None,
                    ResourceVersionMatch::Any,
                )
                .unwrap(),
            ),
        )
        .await
        .unwrap();
        assert_eq!(
            page.items()
                .iter()
                .map(|resource| resource.name.as_str())
                .collect::<Vec<_>>(),
            case.expected,
            "{}",
            case.name
        );
    }

    let first = ClusterResourceRead::list_resources(
        &reads,
        ResourceListRequest::new(
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
        ),
    )
    .await
    .unwrap();
    assert_eq!(first.items()[0].name, "alpha");
    let first_position = first.snapshot().unwrap().position();
    let second = ClusterResourceRead::list_resources(
        &reads,
        ResourceListRequest::new(
            "v1",
            "ConfigMap",
            ResourceCollectionScope::Namespace("tenant-a".to_string()),
            ResourceListQuery::try_new(
                Some("tier=frontend".to_string()),
                None,
                Some(1),
                first.continuation().cloned(),
                ResourceVersionMatch::Any,
            )
            .unwrap(),
        ),
    )
    .await
    .unwrap();
    assert_eq!(second.items()[0].name, "gamma");
    assert_eq!(second.snapshot().unwrap().position(), first_position);

    let unfiltered = ClusterResourceRead::list_resources(
        &reads,
        ResourceListRequest::new(
            "v1",
            "ConfigMap",
            ResourceCollectionScope::Namespace("tenant-a".to_string()),
            ResourceListQuery::try_new(None, None, Some(1), None, ResourceVersionMatch::Any)
                .unwrap(),
        ),
    )
    .await
    .unwrap();
    assert_eq!(unfiltered.remaining_item_count(), Some(2));

    let historical = ClusterResourceRead::list_resources(
        &reads,
        ResourceListRequest::new(
            "v1",
            "ConfigMap",
            ResourceCollectionScope::Namespace("tenant-a".to_string()),
            ResourceListQuery::try_new(None, None, None, None, ResourceVersionMatch::Exact(2))
                .unwrap(),
        ),
    )
    .await
    .unwrap();
    assert!(matches!(historical, ResourceListRead::Historical(_)));
    assert_eq!(
        historical
            .items()
            .iter()
            .map(|resource| resource.name.as_str())
            .collect::<Vec<_>>(),
        vec!["alpha", "beta"]
    );
    assert!(matches!(
        ClusterResourceRead::list_resources(
            &reads,
            ResourceListRequest::new(
                "v1",
                "ConfigMap",
                ResourceCollectionScope::Namespace("tenant-a".to_string()),
                ResourceListQuery::try_new(
                    None,
                    None,
                    None,
                    None,
                    ResourceVersionMatch::NotOlderThan(6),
                )
                .unwrap(),
            ),
        )
        .await,
        Err(ResourceReadError::Conflict { .. })
    ));
    let all_namespace_first = ClusterResourceRead::list_resources(
        &reads,
        ResourceListRequest::new(
            "v1",
            "Secret",
            ResourceCollectionScope::AllNamespaces,
            ResourceListQuery::try_new(None, None, Some(1), None, ResourceVersionMatch::Any)
                .unwrap(),
        ),
    )
    .await
    .unwrap();
    let all_namespace_cursor = all_namespace_first.continuation().cloned().unwrap();
    assert_eq!(all_namespace_cursor.after().namespace(), Some("tenant-a"));
    let all_namespace_second = ClusterResourceRead::list_resources(
        &reads,
        ResourceListRequest::new(
            "v1",
            "Secret",
            ResourceCollectionScope::AllNamespaces,
            ResourceListQuery::try_new(
                None,
                None,
                Some(1),
                Some(all_namespace_cursor.clone()),
                ResourceVersionMatch::Exact(all_namespace_cursor.snapshot().resource_version()),
            )
            .unwrap(),
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        all_namespace_second.items()[0].namespace.as_deref(),
        Some("tenant-b")
    );
    assert_eq!(
        all_namespace_second.snapshot(),
        Some(all_namespace_cursor.snapshot())
    );

    let hostile_limit = ClusterResourceRead::list_resources(
        &reads,
        ResourceListRequest::new(
            "v1",
            "ConfigMap",
            ResourceCollectionScope::Namespace("tenant-a".to_string()),
            ResourceListQuery::try_new(None, None, Some(i64::MAX), None, ResourceVersionMatch::Any)
                .unwrap(),
        ),
    )
    .await
    .unwrap();
    assert_eq!(hostile_limit.items().len(), 3);
    assert!(hostile_limit.continuation().is_none());

    let history = match reads
        .replay_watch_history(
            WatchHistoryRequest::new(
                vec![DurableWatchTarget::namespaced_in_namespace(
                    "v1",
                    "ConfigMap",
                    "tenant-a",
                )],
                WatchReplayPosition::default(),
                8,
            )
            .unwrap(),
        )
        .await
        .unwrap()
    {
        WatchHistoryRead::Events(page) => page,
        WatchHistoryRead::Expired => panic!("fresh restored history must be replayable"),
    };
    assert_eq!(
        history
            .events()
            .iter()
            .map(|event| (
                event.position.event_id,
                event.event.resource().name.as_str()
            ))
            .collect::<Vec<_>>(),
        vec![(1, "alpha"), (2, "beta"), (3, "gamma")]
    );
    assert_eq!(history.next_position().event_id, 3);
    let empty_suffix = match reads
        .replay_watch_history(
            WatchHistoryRequest::new(
                vec![DurableWatchTarget::namespaced_in_namespace(
                    "v1",
                    "ConfigMap",
                    "tenant-a",
                )],
                history.next_position(),
                8,
            )
            .unwrap(),
        )
        .await
        .unwrap()
    {
        WatchHistoryRead::Events(page) => page,
        WatchHistoryRead::Expired => panic!("exact restored suffix must be replayable"),
    };
    assert!(empty_suffix.events().is_empty());
    assert_eq!(empty_suffix.next_position().event_id, 7);

    let allocator = DurableAllocatorRead::read_allocator_state(&reads)
        .await
        .unwrap();
    assert_eq!(
        allocator.position(),
        WatchReplayPosition {
            resource_version: 5,
            event_id: 7,
            resource_version_filter_through_event_id: 0,
        }
    );
    assert_eq!(allocator.next_resource_version(), 6);
    assert_eq!(allocator.next_event_id(), 8);

    recovery
        .restore_snapshot_parts(
            rows.iter()
                .enumerate()
                .map(|(index, row)| {
                    let mut mutations = vec![
                        LogApplyMutation::PutResource(row.clone()),
                        LogApplyMutation::PutWatchEvent(watch_event(
                            row,
                            i64::try_from(index + 1).unwrap(),
                        )),
                    ];
                    if index == 4 {
                        let legacy_namespace = namespace("legacy", 5);
                        mutations.extend([
                            LogApplyMutation::PutNamespace(legacy_namespace.clone()),
                            LogApplyMutation::PutWatchEvent(LogApplyWatchEventRow {
                                event_id: Some(6),
                                api_version: "v1".to_string(),
                                kind: "Namespace".to_string(),
                                namespace: None,
                                name: "legacy".to_string(),
                                resource_version: 5,
                                event_type: "MODIFIED".to_string(),
                                data: legacy_namespace.data,
                            }),
                        ]);
                    }
                    SnapshotRestoreOperation::new(row.resource_version, None, mutations)
                })
                .collect(),
            5,
            Some(7),
            Some(vec![
                klights_cluster_datastore::sqlite::recovery::SnapshotReplayFloor {
                    api_version: "*".to_string(),
                    kind: "*".to_string(),
                    namespace_key: "*".to_string(),
                    floor_resource_version: 1,
                    floor_event_id: 1,
                    position_is_exact: true,
                },
            ]),
            Some(SnapshotMetadata {
                cluster_id: "read-cluster".to_string(),
                leader_epoch: 1,
                membership: SnapshotMembership::AuthoritativeAbsent,
                command_codec_activation_version: Some(3),
            }),
        )
        .await
        .unwrap();
    assert!(matches!(
        ClusterResourceRead::list_resources(
            &reads,
            ResourceListRequest::new(
                "v1",
                "Namespace",
                ResourceCollectionScope::Cluster,
                ResourceListQuery::try_new(None, None, None, None, ResourceVersionMatch::Exact(0),)
                    .unwrap(),
            ),
        )
        .await
        .unwrap(),
        ResourceListRead::Expired { .. }
    ));
}

#[tokio::test]
async fn sqlite_stale_and_equal_status_stamps_persist_ledgers_without_public_change() {
    let executor = sqlite::open_in_memory(supervisor(), "phase10e:status-ledger")
        .await
        .unwrap();
    let read_executor = executor.read_lane_clone();
    let codec: Arc<dyn OutboxResponseCodec> = Arc::new(JsonCodec);
    let reads = SqliteReadStore::new(read_executor.clone());
    let apply = SqliteLiveCommittedApplyStore::new(executor.clone(), codec.clone());
    let ledger = SqliteApplyLedgerRead::new(read_executor.clone());
    let recovery = SqliteRecoveryStore::new(
        executor,
        read_executor,
        None,
        Arc::new(tokio::sync::RwLock::new(())),
        codec.clone(),
    );
    let mut pod = resource_row(
        "Pod",
        Some("default"),
        "certified-status",
        "certified-status-uid",
        1,
        serde_json::json!({}),
    );
    pod.data["spec"] = serde_json::json!({"nodeName": "node-a"});
    pod.data["status"] = serde_json::json!({"phase": "Pending", "message": "origin"});
    recovery
        .restore_snapshot_parts(
            vec![SnapshotRestoreOperation::new(
                1,
                None,
                vec![LogApplyMutation::PutResource(pod)],
            )],
            1,
            Some(0),
            Some(Vec::new()),
            Some(SnapshotMetadata {
                cluster_id: "status-cluster".to_string(),
                leader_epoch: 1,
                membership: SnapshotMembership::AuthoritativeAbsent,
                command_codec_activation_version: Some(3),
            }),
        )
        .await
        .unwrap();

    let fresh = apply
        .apply_committed_raft(CommittedRaftApplyRequest::new(status_commit(
            codec.as_ref(),
            "fresh-status",
            "fresh",
            200,
            1,
        )))
        .await
        .unwrap();
    let fresh_rv = fresh.applied_resource_version().unwrap();
    let public_position = ledger.current_apply_position().await.unwrap();

    for (name, stamp, sequence, reason) in [
        (
            "stale-status",
            100,
            2,
            NoPublicChangeReason::StaleStatusStamp,
        ),
        (
            "equal-status",
            200,
            3,
            NoPublicChangeReason::EqualStatusStamp,
        ),
    ] {
        let receipt = apply
            .apply_committed_raft(CommittedRaftApplyRequest::new(status_commit(
                codec.as_ref(),
                name,
                name,
                stamp,
                sequence,
            )))
            .await
            .unwrap();
        assert!(matches!(
            receipt.outcome(),
            klights_cluster_core::CommittedApplyOutcome::NoPublicChange {
                reason: actual,
                ..
            } if *actual == reason
        ));
        assert_eq!(receipt.applied_resource_version(), Some(fresh_rv));
        assert_eq!(
            ledger.current_apply_position().await.unwrap(),
            public_position,
            "{name} must not allocate a public RV or watch event"
        );
        let row = ledger
            .get_applied_outbox(AppliedOutboxLookup::new(name))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.applied_rv, Some(fresh_rv));
        assert_eq!(row.status_stamp, Some(stamp));
    }
    assert_eq!(
        ledger.list_outbox_watermarks().await.unwrap()[0].stream_seq,
        3
    );
    let pod = ClusterResourceRead::get_resource(
        &reads,
        ResourceGetRequest::new("v1", "Pod", Some("default".to_string()), "certified-status"),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        pod.data.pointer("/status/message").and_then(|v| v.as_str()),
        Some("fresh")
    );
    assert_eq!(pod.resource_version, fresh_rv);
}

#[tokio::test]
async fn sqlite_noop_put_preserves_rv_while_committing_outbox_ledger_and_watermark() {
    let executor = sqlite::open_in_memory(supervisor(), "noop-put-ledger")
        .await
        .unwrap();
    let read_executor = executor.read_lane_clone();
    let codec: Arc<dyn OutboxResponseCodec> = Arc::new(JsonCodec);
    let reads = SqliteReadStore::new(read_executor.clone());
    let apply = SqliteLiveCommittedApplyStore::new(executor.clone(), codec.clone());
    let ledger = SqliteApplyLedgerRead::new(read_executor.clone());
    let recovery = SqliteRecoveryStore::new(
        executor,
        read_executor,
        None,
        Arc::new(tokio::sync::RwLock::new(())),
        codec.clone(),
    );
    let mut config_map = resource_row(
        "ConfigMap",
        Some("default"),
        "certified-noop-put",
        "certified-noop-put-uid",
        1,
        serde_json::json!({"example.test/value": "unchanged"}),
    );
    config_map.data["data"] = serde_json::json!({"value": "before"});
    recovery
        .restore_snapshot_parts(
            vec![SnapshotRestoreOperation::new(
                1,
                None,
                vec![LogApplyMutation::PutResource(config_map.clone())],
            )],
            1,
            Some(0),
            Some(Vec::new()),
            Some(SnapshotMetadata {
                cluster_id: "noop-put-cluster".to_string(),
                leader_epoch: 1,
                membership: SnapshotMembership::AuthoritativeAbsent,
                command_codec_activation_version: Some(3),
            }),
        )
        .await
        .unwrap();
    let before_position = ledger.current_apply_position().await.unwrap();

    let mut noop_put = config_map;
    noop_put.resource_version = 0;
    noop_put.data["metadata"]
        .as_object_mut()
        .unwrap()
        .remove("resourceVersion");
    noop_put.require_existing = true;
    noop_put.precondition_uid = Some("certified-noop-put-uid".to_string());
    noop_put.precondition_resource_version = Some(1);
    let watermark = OutboxStreamWatermark {
        client_id: "worker-noop".to_string(),
        stream_id: 17,
        stream_seq: 1,
    };
    let commit = LogApplyCommit::try_new_with_watermark(
        vec![
            LogApplyMutation::PutResource(noop_put),
            LogApplyMutation::PutAppliedOutbox(LogApplyAppliedOutboxRow {
                idempotency_key: "certified-noop-put".to_string(),
                subject_key: "v1/ConfigMap/default/certified-noop-put".to_string(),
                operation: "PatchResource".to_string(),
                first_seen_ms: 11,
                applied_rv: None,
                result_proto: codec
                    .encode(&StorageResponse::Ack {
                        resource_version: 0,
                    })
                    .unwrap(),
                status_stamp: None,
            }),
        ],
        Some(watermark.clone()),
    )
    .unwrap();

    let receipt = apply
        .apply_committed_raft(CommittedRaftApplyRequest::new(commit))
        .await
        .unwrap();

    assert!(matches!(
        receipt.outcome(),
        klights_cluster_core::CommittedApplyOutcome::NoPublicChange {
            resource_version: 1,
            reason: NoPublicChangeReason::LedgerOnly,
        }
    ));
    assert_eq!(
        ledger.current_apply_position().await.unwrap(),
        before_position
    );
    let applied = ledger
        .get_applied_outbox(AppliedOutboxLookup::new("certified-noop-put"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(applied.applied_rv, Some(1));
    assert_eq!(
        ledger.list_outbox_watermarks().await.unwrap(),
        vec![watermark.clone()]
    );
    let stored = ClusterResourceRead::get_resource(
        &reads,
        ResourceGetRequest::new(
            "v1",
            "ConfigMap",
            Some("default".to_string()),
            "certified-noop-put",
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(stored.resource_version, 1);
    assert_eq!(
        stored.data.pointer("/data/value"),
        Some(&serde_json::json!("before"))
    );
    assert_eq!(
        stored.data.pointer("/metadata/resourceVersion"),
        Some(&serde_json::json!("1"))
    );

    let mut stale_put = resource_row(
        "ConfigMap",
        Some("default"),
        "certified-noop-put",
        "certified-noop-put-uid",
        0,
        serde_json::json!({"example.test/value": "unchanged"}),
    );
    stale_put.data["metadata"]
        .as_object_mut()
        .unwrap()
        .remove("resourceVersion");
    stale_put.data["data"] = serde_json::json!({"value": "before"});
    stale_put.require_existing = true;
    stale_put.precondition_uid = Some("certified-noop-put-uid".to_string());
    stale_put.precondition_resource_version = Some(99);
    let gap_commit = LogApplyCommit::try_new_with_watermark(
        vec![
            LogApplyMutation::PutResource(stale_put),
            LogApplyMutation::PutAppliedOutbox(LogApplyAppliedOutboxRow {
                idempotency_key: "certified-noop-put-gap".to_string(),
                subject_key: "v1/ConfigMap/default/certified-noop-put".to_string(),
                operation: "PatchResource".to_string(),
                first_seen_ms: 12,
                applied_rv: None,
                result_proto: codec
                    .encode(&StorageResponse::Ack {
                        resource_version: 0,
                    })
                    .unwrap(),
                status_stamp: None,
            }),
        ],
        Some(OutboxStreamWatermark {
            stream_seq: 3,
            ..watermark.clone()
        }),
    )
    .unwrap();

    let gap_error = apply
        .apply_committed_raft(CommittedRaftApplyRequest::new(gap_commit))
        .await
        .expect_err("a watermark gap must take precedence over stale resource CAS");
    assert!(
        gap_error.to_string().contains("outbox stream gap"),
        "unexpected gap error: {gap_error}"
    );
    assert_eq!(
        ledger.current_apply_position().await.unwrap(),
        before_position
    );
    assert!(
        ledger
            .get_applied_outbox(AppliedOutboxLookup::new("certified-noop-put-gap"))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        ledger.list_outbox_watermarks().await.unwrap(),
        vec![watermark.clone()]
    );
}

#[tokio::test]
async fn sqlite_watermark_only_snapshot_restore_preserves_exact_public_rv() {
    let executor = sqlite::open_in_memory(supervisor(), "watermark-only-restore")
        .await
        .unwrap();
    let read_executor = executor.read_lane_clone();
    let ledger = SqliteApplyLedgerRead::new(read_executor.clone());
    let recovery = SqliteRecoveryStore::new(
        executor,
        read_executor,
        None,
        Arc::new(tokio::sync::RwLock::new(())),
        Arc::new(JsonCodec),
    );
    let watermark = OutboxStreamWatermark {
        client_id: "restored-worker".to_string(),
        stream_id: 29,
        stream_seq: 41,
    };

    recovery
        .restore_snapshot_parts(
            vec![SnapshotRestoreOperation::new(
                7,
                Some(watermark.clone()),
                Vec::new(),
            )],
            7,
            Some(0),
            Some(Vec::new()),
            Some(SnapshotMetadata {
                cluster_id: "watermark-restore-cluster".to_string(),
                leader_epoch: 1,
                membership: SnapshotMembership::AuthoritativeAbsent,
                command_codec_activation_version: Some(3),
            }),
        )
        .await
        .unwrap();

    assert_eq!(
        ledger.current_apply_position().await.unwrap(),
        WatchReplayPosition {
            resource_version: 7,
            event_id: 0,
            resource_version_filter_through_event_id: 0,
        }
    );
    assert_eq!(
        ledger.list_outbox_watermarks().await.unwrap(),
        vec![watermark]
    );
}

#[tokio::test]
async fn sqlite_recovery_preserves_metadata_codec_and_restore_rollback() {
    let executor = sqlite::open_in_memory(supervisor(), "phase10e:recovery")
        .await
        .unwrap();
    let read_executor = executor.read_lane_clone();
    let recovery = SqliteRecoveryStore::new(
        executor.clone(),
        read_executor.clone(),
        None,
        Arc::new(tokio::sync::RwLock::new(())),
        Arc::new(JsonCodec),
    );
    let metadata = SnapshotMetadata {
        cluster_id: "cluster-a".to_string(),
        leader_epoch: 4,
        membership: SnapshotMembership::AuthoritativeAbsent,
        command_codec_activation_version: Some(COMMAND_CODEC_V3_ACTIVATION_VALUE.parse().unwrap()),
    };
    recovery
        .restore_snapshot_parts(
            vec![SnapshotRestoreOperation::new(
                1,
                None,
                vec![LogApplyMutation::PutNamespace(namespace("stable", 1))],
            )],
            1,
            Some(0),
            Some(vec![
                klights_cluster_datastore::sqlite::recovery::SnapshotReplayFloor {
                    api_version: "*".to_string(),
                    kind: "*".to_string(),
                    namespace_key: "*".to_string(),
                    floor_resource_version: 1,
                    floor_event_id: 0,
                    position_is_exact: true,
                },
            ]),
            Some(metadata),
        )
        .await
        .unwrap();

    let observed = recovery.read_cluster_metadata().await.unwrap();
    assert_eq!(observed.metadata().cluster_id, "cluster-a");
    assert_eq!(observed.metadata().leader_epoch, 4);
    assert!(matches!(
        observed.membership(),
        CanonicalMembership::AuthoritativeAbsent
    ));

    let failed = recovery
        .restore_snapshot_parts(
            vec![
                SnapshotRestoreOperation::new(
                    1,
                    None,
                    vec![LogApplyMutation::PutNamespace(namespace("replacement", 1))],
                ),
                SnapshotRestoreOperation::new(
                    2,
                    None,
                    vec![LogApplyMutation::PutNamespace(namespace("invalid", 2))],
                ),
            ],
            1,
            Some(0),
            None,
            None,
        )
        .await;
    assert!(failed.is_err(), "failure_injection must abort restore");
    let stable: i64 = read_executor
        .call_raw("verify_restore_rollback", |connection| {
            connection
                .query_row(
                    "SELECT COUNT(*) FROM namespaces WHERE name = 'stable'",
                    [],
                    |row| row.get(0),
                )
                .map_err(klights_supervisor::DbError::from)
        })
        .await
        .unwrap();
    assert_eq!(stable, 1, "restore rollback must retain prior state");

    let snapshot = AuthoritativeSnapshot::try_new(
        vec![SnapshotRestoreOperation::new(
            1,
            None,
            vec![LogApplyMutation::PutNamespace(namespace("canonical", 1))],
        )],
        Some(WatchReplayPosition {
            resource_version: 1,
            event_id: 0,
            resource_version_filter_through_event_id: 0,
        }),
        Some(vec![
            klights_cluster_store::DurableReplayFloor::all(1, 0, true).unwrap(),
        ]),
        ClusterMetadata {
            cluster_id: "cluster-b".to_string(),
            leader_epoch: 5,
            current_rv: 1,
        },
        CanonicalMembership::AuthoritativeAbsent,
    )
    .unwrap();
    recovery
        .restore_authoritative_snapshot(snapshot)
        .await
        .unwrap();
    assert_eq!(
        recovery
            .read_cluster_metadata()
            .await
            .unwrap()
            .metadata()
            .cluster_id,
        "cluster-b"
    );
}

#[tokio::test]
async fn sqlite_metadata_rejects_malformed_rows_and_absent_membership_clears_stale_state() {
    let executor = sqlite::open_in_memory(supervisor(), "phase10e:metadata-conformance")
        .await
        .unwrap();
    let recovery = SqliteRecoveryStore::new(
        executor.clone(),
        executor.read_lane_clone(),
        None,
        Arc::new(tokio::sync::RwLock::new(())),
        Arc::new(JsonCodec),
    );
    let valid = || SnapshotMetadata {
        cluster_id: "metadata-cluster".to_string(),
        leader_epoch: 2,
        membership: SnapshotMembership::AuthoritativeAbsent,
        command_codec_activation_version: Some(3),
    };

    struct Corruption {
        name: &'static str,
        rows: &'static [(&'static str, &'static str)],
        expected: &'static str,
    }
    let cases = [
        Corruption {
            name: "malformed epoch",
            rows: &[(klights_cluster_store::LEADER_EPOCH_META_KEY, "not-a-number")],
            expected: "leader_epoch",
        },
        Corruption {
            name: "partial membership",
            rows: &[(klights_cluster_store::RAFT_VOTERS_META_KEY, "[\"cp-1\"]")],
            expected: "incomplete",
        },
        Corruption {
            name: "invalid voter set",
            rows: &[
                (klights_cluster_store::RAFT_VOTERS_META_KEY, "[]"),
                (klights_cluster_store::RAFT_TERM_META_KEY, "4"),
                (klights_cluster_store::RAFT_LEADER_HINT_META_KEY, ""),
            ],
            expected: "voter set",
        },
    ];
    for case in cases {
        recovery
            .restore_snapshot_parts(Vec::new(), 0, Some(0), Some(Vec::new()), Some(valid()))
            .await
            .unwrap();
        executor
            .call_raw("inject_malformed_cluster_metadata", move |connection| {
                for (key, value) in case.rows {
                    connection.execute(
                        "INSERT OR REPLACE INTO _klights_meta (key, value) VALUES (?1, ?2)",
                        rusqlite::params![key, value],
                    )?;
                }
                Ok(())
            })
            .await
            .unwrap();
        let error = recovery.read_cluster_metadata().await.unwrap_err();
        assert!(
            error.to_string().contains(case.expected),
            "{}: {error}",
            case.name
        );
    }

    recovery
        .restore_snapshot_parts(Vec::new(), 0, Some(0), Some(Vec::new()), Some(valid()))
        .await
        .unwrap();
    let invalid_codec = SnapshotMetadata {
        cluster_id: "must-rollback".to_string(),
        leader_epoch: 9,
        membership: SnapshotMembership::AuthoritativeAbsent,
        command_codec_activation_version: Some(2),
    };
    assert!(
        recovery
            .restore_snapshot_parts(
                Vec::new(),
                0,
                Some(0),
                Some(Vec::new()),
                Some(invalid_codec),
            )
            .await
            .is_err()
    );
    assert_eq!(
        recovery
            .read_cluster_metadata()
            .await
            .unwrap()
            .metadata()
            .cluster_id,
        "metadata-cluster",
        "invalid metadata must rollback the authoritative replacement"
    );

    executor
        .call_raw("seed_stale_membership", |connection| {
            for (key, value) in [
                (klights_cluster_store::RAFT_VOTERS_META_KEY, "[\"cp-1\"]"),
                (klights_cluster_store::RAFT_TERM_META_KEY, "4"),
                (klights_cluster_store::RAFT_LEADER_HINT_META_KEY, "cp-1"),
            ] {
                connection.execute(
                    "INSERT OR REPLACE INTO _klights_meta (key, value) VALUES (?1, ?2)",
                    rusqlite::params![key, value],
                )?;
            }
            Ok(())
        })
        .await
        .unwrap();
    recovery
        .restore_authoritative_snapshot(
            AuthoritativeSnapshot::try_new(
                Vec::new(),
                Some(WatchReplayPosition::default()),
                Some(Vec::new()),
                ClusterMetadata {
                    cluster_id: "metadata-cluster".to_string(),
                    leader_epoch: 2,
                    current_rv: 0,
                },
                CanonicalMembership::AuthoritativeAbsent,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let stale_rows: i64 = executor
        .call_raw("verify_absent_membership", |connection| {
            connection
                .query_row(
                    "SELECT COUNT(*) FROM _klights_meta \
                     WHERE key IN ('voters', 'term', 'leader_hint')",
                    [],
                    |row| row.get(0),
                )
                .map_err(klights_supervisor::DbError::from)
        })
        .await
        .unwrap();
    assert_eq!(stale_rows, 0);
    assert!(matches!(
        recovery.read_cluster_metadata().await.unwrap().membership(),
        CanonicalMembership::AuthoritativeAbsent
    ));
}

#[tokio::test]
async fn sqlite_capture_certifies_bounded_durable_families() {
    let source_root = tempfile::tempdir().unwrap();
    let mut source_opts = OpenOpts::disk(source_root.path().join("cluster.db"));
    source_opts.allow_existing_perms = true;
    let source_supervisor = supervisor();
    let source_executor = sqlite::open_with_opts(
        source_opts.clone(),
        source_supervisor.clone(),
        "phase10e:capture-source-write",
    )
    .await
    .unwrap();
    let source_read_executor = sqlite::open_read_only_with_opts(
        source_opts.clone(),
        source_supervisor.clone(),
        "phase10e:capture-source-read",
    )
    .await
    .unwrap();
    let source_reads = SqliteReadStore::new(source_read_executor.clone());
    let source_recovery = SqliteRecoveryStore::new(
        source_executor,
        source_read_executor,
        Some(SqliteSnapshotFactory::new(source_opts, source_supervisor)),
        Arc::new(tokio::sync::RwLock::new(())),
        Arc::new(JsonCodec),
    );
    let namespaced = resource_row(
        "ConfigMap",
        Some("captured"),
        "captured-config",
        "captured-config-uid",
        7,
        serde_json::json!({"tier": "certified"}),
    );
    let deleted_before_capture = resource_row(
        "ConfigMap",
        Some("captured"),
        "deleted-during-capture",
        "deleted-during-capture-uid",
        6,
        serde_json::json!({}),
    );
    let deleted_tombstone = resource_row(
        "ConfigMap",
        Some("captured"),
        "deleted-during-capture",
        "deleted-during-capture-uid",
        7,
        serde_json::json!({}),
    );
    let cluster_scoped = resource_row("Node", None, "cp-1", "cp-1-uid", 7, serde_json::json!({}));
    let watermark = OutboxStreamWatermark {
        client_id: "worker-capture".to_string(),
        stream_id: 3,
        stream_seq: 1,
    };
    let applied = LogApplyAppliedOutboxRow {
        idempotency_key: "capture-ledger".to_string(),
        subject_key: "v1/ConfigMap/captured/captured-config".to_string(),
        operation: "Create".to_string(),
        first_seen_ms: 1,
        applied_rv: Some(7),
        result_proto: vec![7, 8, 9],
        status_stamp: Some(11),
    };
    let floors = vec![
        DurableReplayFloor::all(1, 1, true).unwrap(),
        DurableReplayFloor::namespaced("v1", "ConfigMap", "captured", 7, 4, true).unwrap(),
    ];
    let membership = ClusterMembership {
        cluster_id: "capture-cluster".to_string(),
        voters: vec!["cp-1".to_string()],
        term: 3,
        leader_hint: Some("cp-1".to_string()),
    };
    source_recovery
        .restore_snapshot_parts(
            vec![
                SnapshotRestoreOperation::new(
                    6,
                    None,
                    vec![
                        LogApplyMutation::PutResource(deleted_before_capture.clone()),
                        LogApplyMutation::PutWatchEvent(watch_event(&deleted_before_capture, 3)),
                    ],
                ),
                SnapshotRestoreOperation::new(
                    7,
                    Some(watermark.clone()),
                    vec![
                        LogApplyMutation::PutNamespace(namespace("captured", 7)),
                        LogApplyMutation::PutResource(namespaced.clone()),
                        LogApplyMutation::PutResource(cluster_scoped),
                        LogApplyMutation::PutWatchEvent(watch_event(&namespaced, 4)),
                        LogApplyMutation::PutWatchEvent(LogApplyWatchEventRow {
                            event_id: Some(5),
                            api_version: deleted_tombstone.api_version.clone(),
                            kind: deleted_tombstone.kind.clone(),
                            namespace: deleted_tombstone.namespace.clone(),
                            name: deleted_tombstone.name.clone(),
                            resource_version: deleted_tombstone.resource_version,
                            event_type: "DELETED".to_string(),
                            data: deleted_tombstone.data.clone(),
                        }),
                        LogApplyMutation::DeleteResource(LogApplyResourceKey {
                            api_version: deleted_tombstone.api_version.clone(),
                            kind: deleted_tombstone.kind.clone(),
                            namespace: deleted_tombstone.namespace.clone(),
                            name: deleted_tombstone.name.clone(),
                            uid: deleted_tombstone.uid.clone(),
                            precondition_resource_version: Some(
                                deleted_before_capture.resource_version,
                            ),
                        }),
                        LogApplyMutation::PutNodeSubnet(LogApplyNodeSubnetRow {
                            node_name: "cp-1".to_string(),
                            subnet: "10.42.1.0/24".to_string(),
                            subnet_base_int: u32::from(std::net::Ipv4Addr::new(10, 42, 1, 0)),
                            gateway_ip: "10.42.1.1".to_string(),
                            node_ip: "10.0.0.1".to_string(),
                            mode: "root".to_string(),
                            hostport_range: None,
                        }),
                        LogApplyMutation::PutNodeDataplane(LogApplyNodeDataplaneRow {
                            node_name: "cp-1".to_string(),
                            mode: "root".to_string(),
                            encryption: "enabled".to_string(),
                            public_key: Some(
                                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string(),
                            ),
                            endpoint: "10.0.0.1".to_string(),
                            port: Some(51820),
                        }),
                        LogApplyMutation::PutPodCleanupIntent(LogApplyPodCleanupIntentRow {
                            node_name: "cp-1".to_string(),
                            namespace: "captured".to_string(),
                            pod_name: "cleanup-pod".to_string(),
                            pod_uid: "cleanup-uid".to_string(),
                            reason: "NodeLost".to_string(),
                            resource_version: 7,
                            created_at_ms: 456,
                            pod_data: serde_json::json!({
                                "metadata": {"name": "cleanup-pod", "uid": "cleanup-uid"},
                            }),
                        }),
                        LogApplyMutation::PutAppliedOutbox(applied.clone()),
                    ],
                ),
            ],
            7,
            Some(9),
            Some(
                floors
                    .iter()
                    .cloned()
                    .map(|floor| {
                        let (target, floor_resource_version, floor_event_id, position_is_exact) =
                            floor.into_parts();
                        let (api_version, kind, namespace_key) = match target {
                            klights_cluster_store::DurableReplayTarget::All => {
                                ("*".to_string(), "*".to_string(), "*".to_string())
                            }
                            klights_cluster_store::DurableReplayTarget::Cluster {
                                api_version,
                                kind,
                            } => (api_version, kind, "#cluster".to_string()),
                            klights_cluster_store::DurableReplayTarget::Namespaced {
                                api_version,
                                kind,
                                namespace,
                            } => (api_version, kind, namespace),
                        };
                        klights_cluster_datastore::sqlite::recovery::SnapshotReplayFloor {
                            api_version,
                            kind,
                            namespace_key,
                            floor_resource_version,
                            floor_event_id,
                            position_is_exact,
                        }
                    })
                    .collect(),
            ),
            Some(SnapshotMetadata {
                cluster_id: "capture-cluster".to_string(),
                leader_epoch: 2,
                membership: SnapshotMembership::Present(membership.clone()),
                command_codec_activation_version: Some(3),
            }),
        )
        .await
        .unwrap();

    assert!(
        ClusterResourceRead::get_resource(
            &source_reads,
            ResourceGetRequest::new(
                "v1",
                "ConfigMap",
                Some("captured".to_string()),
                "deleted-during-capture",
            ),
        )
        .await
        .unwrap()
        .is_none(),
        "qualified source delete must remove the live row before capture"
    );

    let request = SnapshotCaptureRequest::try_new(
        SnapshotPageLimit::try_new(1).unwrap(),
        Duration::from_secs(30),
    )
    .unwrap();
    let mut session = source_recovery.begin_capture(request).await.unwrap();
    let header = session.header().clone();
    assert_eq!(header.command_codec_activation_version(), Some(3));
    let mut pages = Vec::new();
    while let Some(page) = session.next_page().await.unwrap() {
        assert!(page.len() <= 1, "physical capture page exceeded its bound");
        pages.push(page);
    }
    let kinds = pages
        .iter()
        .map(SnapshotCapturePage::kind)
        .collect::<Vec<_>>();
    for required in [
        SnapshotCapturePageKind::Commits,
        SnapshotCapturePageKind::AppliedOutbox,
        SnapshotCapturePageKind::OutboxWatermarks,
        SnapshotCapturePageKind::ReplayFloors,
    ] {
        assert!(
            kinds.contains(&required),
            "missing capture family {required:?}"
        );
    }
    let captured_operations = pages
        .iter()
        .filter_map(SnapshotCapturePage::operations)
        .flat_map(|operations| operations.iter())
        .collect::<Vec<_>>();
    // Retires `snapshot_replays_resource_deletes_since_rv`: canonical capture
    // represents a delete as live-row absence plus its exact watch tombstone,
    // not as the legacy facade's synthetic `DeleteResource` operation.
    assert!(
        !captured_operations
            .iter()
            .flat_map(|operation| operation.mutations())
            .any(|mutation| {
                matches!(
                    mutation,
                    LogApplyMutation::PutResource(row)
                        if row.name == "deleted-during-capture"
                )
            }),
        "capture must not revive a deleted live row"
    );
    assert!(
        captured_operations
            .iter()
            .flat_map(|operation| operation.mutations())
            .any(|mutation| {
                matches!(
                    mutation,
                    LogApplyMutation::PutWatchEvent(event)
                        if event.event_type == "DELETED"
                            && event.name == "deleted-during-capture"
                            && event.event_id == Some(5)
                            && event.resource_version == 7
                )
            }),
        "capture must retain the exact deleted watch-history event"
    );

    let destination_root = tempfile::tempdir().unwrap();
    let mut destination_opts = OpenOpts::disk(destination_root.path().join("cluster.db"));
    destination_opts.allow_existing_perms = true;
    let destination_supervisor = supervisor();
    let destination_executor = sqlite::open_with_opts(
        destination_opts.clone(),
        destination_supervisor.clone(),
        "phase10e:capture-destination-write",
    )
    .await
    .unwrap();
    let destination_read_executor = sqlite::open_read_only_with_opts(
        destination_opts.clone(),
        destination_supervisor.clone(),
        "phase10e:capture-destination-read",
    )
    .await
    .unwrap();
    let destination_reads = SqliteReadStore::new(destination_read_executor.clone());
    let destination_ledger = SqliteApplyLedgerRead::new(destination_read_executor.clone());
    let destination_recovery = SqliteRecoveryStore::new(
        destination_executor,
        destination_read_executor,
        Some(SqliteSnapshotFactory::new(
            destination_opts,
            destination_supervisor,
        )),
        Arc::new(tokio::sync::RwLock::new(())),
        Arc::new(JsonCodec),
    );
    destination_recovery
        .restore_snapshot_parts(
            vec![SnapshotRestoreOperation::new(
                1,
                None,
                vec![
                    LogApplyMutation::PutResource(resource_row(
                        "ConfigMap",
                        Some("captured"),
                        "divergent",
                        "divergent-uid",
                        1,
                        serde_json::json!({}),
                    )),
                    LogApplyMutation::PutResource(resource_row(
                        "ConfigMap",
                        Some("captured"),
                        "deleted-during-capture",
                        "stale-replacement-uid",
                        1,
                        serde_json::json!({}),
                    )),
                ],
            )],
            1,
            Some(0),
            Some(Vec::new()),
            Some(SnapshotMetadata {
                cluster_id: "divergent-cluster".to_string(),
                leader_epoch: 1,
                membership: SnapshotMembership::AuthoritativeAbsent,
                command_codec_activation_version: Some(3),
            }),
        )
        .await
        .unwrap();
    assert!(
        ClusterResourceRead::get_resource(
            &destination_reads,
            ResourceGetRequest::new(
                "v1",
                "ConfigMap",
                Some("captured".to_string()),
                "deleted-during-capture",
            ),
        )
        .await
        .unwrap()
        .is_some(),
        "destination fixture must begin with a divergent same-name UID"
    );
    destination_recovery
        .restore_authoritative_snapshot(snapshot_from_capture(&header, &pages))
        .await
        .unwrap();

    for (name, present) in [("captured-config", true), ("divergent", false)] {
        assert_eq!(
            ClusterResourceRead::get_resource(
                &destination_reads,
                ResourceGetRequest::new("v1", "ConfigMap", Some("captured".to_string()), name,),
            )
            .await
            .unwrap()
            .is_some(),
            present,
            "{name}"
        );
    }
    assert!(
        ClusterResourceRead::get_resource(
            &destination_reads,
            ResourceGetRequest::new(
                "v1",
                "ConfigMap",
                Some("captured".to_string()),
                "deleted-during-capture",
            ),
        )
        .await
        .unwrap()
        .is_none(),
        "authoritative replacement must retain deleted-resource absence and remove a stale same-name UID"
    );
    assert!(
        ClusterTopologyRead::get_node_subnet(
            &destination_reads,
            NodeTopologyRequest::try_new("cp-1").unwrap(),
        )
        .await
        .unwrap()
        .is_some()
    );
    assert!(
        ClusterTopologyRead::get_node_dataplane(
            &destination_reads,
            NodeTopologyRequest::try_new("cp-1").unwrap(),
        )
        .await
        .unwrap()
        .is_some()
    );
    assert_eq!(
        destination_ledger
            .get_applied_outbox(AppliedOutboxLookup::new("capture-ledger"))
            .await
            .unwrap(),
        Some(applied)
    );
    assert_eq!(
        destination_ledger.list_outbox_watermarks().await.unwrap(),
        vec![watermark]
    );
    assert_eq!(
        DurableWatchHistoryRead::list_replay_floors(&destination_reads)
            .await
            .unwrap(),
        floors
    );
    assert_eq!(
        DurableAllocatorRead::read_allocator_state(&destination_reads)
            .await
            .unwrap()
            .position(),
        header.position()
    );
    let destination_metadata = destination_recovery.read_cluster_metadata().await.unwrap();
    assert_eq!(destination_metadata.metadata(), header.metadata());
    assert_eq!(
        destination_metadata.membership(),
        &CanonicalMembership::Present(membership)
    );

    let mut recapture = destination_recovery.begin_capture(request).await.unwrap();
    assert_eq!(recapture.header().position(), header.position());
    let mut durable_mutations = Vec::new();
    while let Some(page) = recapture.next_page().await.unwrap() {
        assert!(page.len() <= 1, "recapture page exceeded its bound");
        if let Some(operations) = page.operations() {
            durable_mutations.extend(
                operations
                    .iter()
                    .flat_map(|operation| operation.mutations().iter().cloned()),
            );
        }
    }
    for (family, present) in [
        (
            "namespace",
            durable_mutations
                .iter()
                .any(|mutation| matches!(mutation, LogApplyMutation::PutNamespace(_))),
        ),
        (
            "namespaced and cluster resources",
            durable_mutations
                .iter()
                .filter(|mutation| matches!(mutation, LogApplyMutation::PutResource(_)))
                .count()
                == 2,
        ),
        (
            "watch history",
            durable_mutations
                .iter()
                .any(|mutation| matches!(mutation, LogApplyMutation::PutWatchEvent(_))),
        ),
        (
            "deleted-resource watch tombstone",
            durable_mutations.iter().any(|mutation| {
                matches!(
                    mutation,
                    LogApplyMutation::PutWatchEvent(event)
                        if event.event_type == "DELETED"
                            && event.name == "deleted-during-capture"
                            && event.event_id == Some(5)
                            && event.resource_version == 7
                )
            }),
        ),
        (
            "node subnet",
            durable_mutations
                .iter()
                .any(|mutation| matches!(mutation, LogApplyMutation::PutNodeSubnet(_))),
        ),
        (
            "node dataplane",
            durable_mutations
                .iter()
                .any(|mutation| matches!(mutation, LogApplyMutation::PutNodeDataplane(_))),
        ),
        (
            "pod cleanup",
            durable_mutations
                .iter()
                .any(|mutation| matches!(mutation, LogApplyMutation::PutPodCleanupIntent(_))),
        ),
    ] {
        assert!(present, "recapture omitted {family}");
    }
}
