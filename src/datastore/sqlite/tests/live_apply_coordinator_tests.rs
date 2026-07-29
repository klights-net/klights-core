use super::*;
use klights_cluster_core::{
    LogApplyAppliedOutboxRow, LogApplyCommit, LogApplyMutation, LogApplyNodeDataplaneRow,
    LogApplyNodeSubnetRow, LogApplyResourceKey, LogApplyResourcePatch, LogApplyResourceRow,
    LogApplyWatchEventRow, OutboxStreamWatermark, SnapshotRestoreOperation,
};
use klights_cluster_datastore::sqlite::live_apply::{
    ApplyConflictCode, apply_conflict_error, is_terminal_apply_conflict, other_error,
};
use std::net::Ipv4Addr;

fn committed_apply_v1(commit: LogApplyCommit) -> LogApplyCommit {
    commit
}

fn v1_resource(name: &str, uid: &str) -> LogApplyMutation {
    LogApplyMutation::PutResource(LogApplyResourceRow {
        api_version: "v1".to_string(),
        kind: "ConfigMap".to_string(),
        namespace: Some("default".to_string()),
        name: name.to_string(),
        uid: uid.to_string(),
        resource_version: 0,
        data: serde_json::json!({"metadata": {"name": name, "namespace": "default", "uid": uid}}),
        require_absent: true,
        require_existing: false,
        precondition_uid: None,
        precondition_resource_version: None,
        status_only: false,
    })
}

async fn enable_committed_apply_v1(_db: &Datastore) {
    // Fixed contract: committed-apply V1 is unconditional.
}

fn snapshot_operation(
    resource_version: i64,
    outbox_watermark: Option<OutboxStreamWatermark>,
    mut mutations: Vec<LogApplyMutation>,
) -> SnapshotRestoreOperation {
    for mutation in &mut mutations {
        match mutation {
            LogApplyMutation::PutResource(row) => row.resource_version = resource_version,
            LogApplyMutation::PatchResourceLatest(row) => {
                row.resource_version = resource_version;
            }
            LogApplyMutation::PutNamespace(row) => row.resource_version = resource_version,
            LogApplyMutation::PutWatchEvent(row) => row.resource_version = resource_version,
            LogApplyMutation::PutPodCleanupIntent(row) => {
                row.resource_version = resource_version;
            }
            LogApplyMutation::PutAppliedOutbox(row) => {
                row.applied_rv = Some(resource_version);
            }
            LogApplyMutation::AdvanceResourceVersion {
                resource_version: row_resource_version,
            } => *row_resource_version = resource_version,
            _ => {}
        }
    }
    SnapshotRestoreOperation::new(resource_version, outbox_watermark, mutations)
}

fn snapshot_watch_event(row: LogApplyWatchEventRow) -> SnapshotRestoreOperation {
    SnapshotRestoreOperation::new(
        row.resource_version,
        None,
        vec![LogApplyMutation::PutWatchEvent(row)],
    )
}

fn pod_status_subject_key(name: &str, uid: &str) -> String {
    format!("v1/Pod/default/{name}/{uid}")
}

fn pod_status_commit_with_stamp(
    idempotency_key: &str,
    status_message: &str,
    status_stamp: i64,
    name: &str,
    uid: &str,
) -> LogApplyCommit {
    crate::datastore::test_support::test_live_commit(
        0,
        vec![
            LogApplyMutation::PutResource(LogApplyResourceRow {
                api_version: "v1".into(),
                kind: "Pod".into(),
                namespace: Some("default".into()),
                name: name.to_string(),
                uid: uid.to_string(),
                resource_version: 0,
                data: serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": name, "namespace": "default", "uid": uid},
                    "status": {"phase": "Running", "message": status_message},
                }),
                require_absent: false,
                require_existing: true,
                precondition_uid: Some(uid.to_string()),
                precondition_resource_version: None,
                status_only: true,
            }),
            LogApplyMutation::PutAppliedOutbox(LogApplyAppliedOutboxRow {
                idempotency_key: idempotency_key.to_string(),
                subject_key: pod_status_subject_key(name, uid),
                operation: "PodStatus".to_string(),
                first_seen_ms: status_stamp,
                applied_rv: None,
                result_proto: crate::datastore::sqlite::outbox_codec::encode(
                    &klights_cluster_core::command::StorageResponse::Ack {
                        resource_version: 0,
                    },
                )
                .unwrap_or_default(),
                status_stamp: Some(status_stamp),
            }),
        ],
    )
}

fn watermarked_pod_status_commit_with_stamp(
    idempotency_key: &str,
    status_message: &str,
    status_stamp: i64,
    stream_seq: i64,
    name: &str,
    uid: &str,
) -> LogApplyCommit {
    let commit = committed_apply_v1(pod_status_commit_with_stamp(
        idempotency_key,
        status_message,
        status_stamp,
        name,
        uid,
    ));
    let (_, _, mutations) = commit.into_parts();
    LogApplyCommit::try_new_with_watermark(
        mutations,
        Some(OutboxStreamWatermark {
            client_id: "worker-status-client".to_string(),
            stream_id: 7,
            stream_seq,
        }),
    )
    .expect("status commit must remain an RV-zero live template")
}

struct PodStatusApplySnapshot {
    current_rv: i64,
    pod_rv: i64,
    status_message: String,
    watch_count: usize,
    outbox_rows: Vec<LogApplyAppliedOutboxRow>,
    watermarks: Vec<OutboxStreamWatermark>,
}

async fn pod_status_apply_snapshot(
    db: &Datastore,
    name: &str,
    keys: &[&str],
) -> PodStatusApplySnapshot {
    let pod = db
        .get_resource("v1", "Pod", Some("default"), name)
        .await
        .unwrap()
        .expect("pod exists");
    let status_message = pod
        .data
        .pointer("/status/message")
        .and_then(|value| value.as_str())
        .expect("status message")
        .to_string();
    let watch_count = db
        .list_resources_modified_since("v1", "Pod", Some("default"), 0)
        .await
        .unwrap()
        .len();
    let mut outbox_rows = Vec::new();
    for key in keys {
        if let Some(row) = db.get_applied_outbox(key).await.unwrap() {
            outbox_rows.push(row);
        }
    }
    PodStatusApplySnapshot {
        current_rv: db.get_current_resource_version().await.unwrap(),
        pod_rv: pod.resource_version,
        status_message,
        watch_count,
        outbox_rows,
        watermarks: db.list_outbox_stream_watermarks().await.unwrap(),
    }
}

fn applied_outbox_ack_rv(row: &LogApplyAppliedOutboxRow) -> i64 {
    match crate::datastore::sqlite::outbox_codec::decode(&row.result_proto)
        .expect("decode applied-outbox response")
    {
        klights_cluster_core::command::StorageResponse::Ack { resource_version } => {
            resource_version
        }
        other => panic!("expected Ack response, got {other:?}"),
    }
}

#[tokio::test]
async fn committed_apply_v1_allocates_one_rv_after_current_counter() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.advance_resource_version_after(10).await.unwrap();
    enable_committed_apply_v1(&db).await;

    let result = db
        .apply_raft_log_apply_commit(committed_apply_v1(
            crate::datastore::test_support::test_live_commit(
                0,
                vec![v1_resource("v1-one", "v1-one-uid")],
            ),
        ))
        .await
        .unwrap();
    let rv = result.applied_rv.unwrap();
    assert!(rv > 10);
    let resource = db
        .get_resource("v1", "ConfigMap", Some("default"), "v1-one")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resource.resource_version, rv);
    assert_eq!(
        resource
            .data
            .pointer("/metadata/resourceVersion")
            .and_then(|value| value.as_str()),
        Some(rv.to_string().as_str())
    );
}

#[tokio::test]
async fn snapshot_replace_restores_history_then_allocates_a_newer_live_rv() {
    let db = Datastore::new_in_memory().await.unwrap();
    let mut resource = v1_resource("snapshot-v1", "snapshot-uid");
    if let LogApplyMutation::PutResource(row) = &mut resource {
        row.resource_version = 10;
    }
    let snapshot_commit = snapshot_operation(10, None, vec![resource]);
    db.replace_replicated_resource_state(
        vec![snapshot_commit],
        10,
        None,
        None,
        Some(ReplicatedSnapshotMetadata {
            cluster_id: String::new(),
            leader_epoch: 0,
            membership: crate::datastore::ReplicatedMembershipState::LegacyOmitted,
            command_codec_activation_version: None,
        }),
    )
    .await
    .unwrap();
    assert_eq!(
        db.get_resource("v1", "ConfigMap", Some("default"), "snapshot-v1")
            .await
            .unwrap()
            .unwrap()
            .resource_version,
        10
    );
    let applied = db
        .apply_raft_log_apply_commit(committed_apply_v1(
            crate::datastore::test_support::test_live_commit(
                0,
                vec![v1_resource("after-snapshot", "after-snapshot-uid")],
            ),
        ))
        .await
        .unwrap();
    assert!(applied.applied_rv.unwrap() > 10);
}

#[tokio::test]
async fn snapshot_restore_preserves_exact_historical_resource_bytes() {
    let db = Datastore::new_in_memory().await.unwrap();
    let historical_data = serde_json::json!({
        "metadata": {
            "name": "exact-snapshot",
            "namespace": "default",
            "uid": "exact-snapshot-uid",
            "resourceVersion": "7"
        },
        "data": {"lexical-shape": ["must", "remain", "exact"]}
    });
    let expected_bytes = serde_json::to_vec(&historical_data).unwrap();

    db.replace_replicated_resource_state(
        vec![SnapshotRestoreOperation::new(
            7,
            None,
            vec![LogApplyMutation::PutResource(LogApplyResourceRow {
                api_version: "v1".into(),
                kind: "ConfigMap".into(),
                namespace: Some("default".into()),
                name: "exact-snapshot".into(),
                uid: "exact-snapshot-uid".into(),
                resource_version: 7,
                data: historical_data,
                require_absent: false,
                require_existing: false,
                precondition_uid: None,
                precondition_resource_version: None,
                status_only: false,
            })],
        )],
        7,
        None,
        None,
        None,
    )
    .await
    .unwrap();

    let stored_bytes = db
        .db_call("test_exact_snapshot_resource_bytes", |conn| {
            Ok(conn.query_row(
                "SELECT data FROM namespaced_resources
                     WHERE api_version = 'v1' AND kind = 'ConfigMap'
                       AND namespace = 'default' AND name = 'exact-snapshot'",
                [],
                |row| row.get::<_, Vec<u8>>(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(
        stored_bytes, expected_bytes,
        "authoritative snapshot restore must not run live hydration"
    );
}

fn watermark_commit(
    resource_version: i64,
    client_id: &str,
    stream_id: i64,
    stream_seq: i64,
) -> LogApplyCommit {
    let _ = resource_version;
    LogApplyCommit::try_new_with_watermark(
        Vec::new(),
        Some(OutboxStreamWatermark {
            client_id: client_id.to_string(),
            stream_id,
            stream_seq,
        }),
    )
    .expect("watermark commit must be an RV-zero live template")
}

fn snapshot_watermark_operation(
    resource_version: i64,
    client_id: &str,
    stream_id: i64,
    stream_seq: i64,
) -> SnapshotRestoreOperation {
    snapshot_operation(
        resource_version,
        Some(OutboxStreamWatermark {
            client_id: client_id.to_string(),
            stream_id,
            stream_seq,
        }),
        Vec::new(),
    )
}

#[tokio::test]
async fn snapshot_replace_prunes_destination_only_and_higher_outbox_watermarks() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.apply_raft_log_apply_commit(watermark_commit(1, "destination-only", 1, 9))
        .await
        .unwrap();
    db.apply_raft_log_apply_commit(watermark_commit(1, "shared", 2, 99))
        .await
        .unwrap();

    db.replace_replicated_resource_state(
        vec![snapshot_watermark_operation(1, "shared", 2, 4)],
        1,
        None,
        None,
        None,
    )
    .await
    .unwrap();

    assert_eq!(
        db.list_outbox_stream_watermarks().await.unwrap(),
        vec![OutboxStreamWatermark {
            client_id: "shared".to_string(),
            stream_id: 2,
            stream_seq: 4,
        }]
    );
}

#[tokio::test]
async fn failed_snapshot_replace_rolls_back_outbox_watermark_pruning() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.apply_raft_log_apply_commit(watermark_commit(1, "preserved", 7, 8))
        .await
        .unwrap();

    db.replace_replicated_resource_state(
        vec![snapshot_watermark_operation(2, "snapshot", 8, 1)],
        1,
        None,
        None,
        None,
    )
    .await
    .expect_err("snapshot entry ahead of current_rv must roll back replacement");

    assert_eq!(
        db.list_outbox_stream_watermarks().await.unwrap(),
        vec![OutboxStreamWatermark {
            client_id: "preserved".to_string(),
            stream_id: 7,
            stream_seq: 8,
        }]
    );
}

#[tokio::test]
async fn snapshot_replace_accepts_authoritative_counter_rollback() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.advance_resource_version_after(20).await.unwrap();
    db.replace_replicated_resource_state(Vec::new(), 10, None, None, None)
        .await
        .expect("authoritative snapshot must replace the local counter");
    assert_eq!(db.get_current_resource_version().await.unwrap(), 10);
}

#[tokio::test]
async fn legacy_snapshot_floor_allows_fresh_positioned_handoff() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.replace_replicated_resource_state(Vec::new(), 10, Some(5), None, None)
        .await
        .unwrap();

    let target = [crate::datastore::WatchTarget::namespaced_in_namespace(
        "v1",
        "ConfigMap",
        "default",
    )];
    let fresh =
        crate::datastore::WatchReplayPosition::from_resource_version_through_event_id(10, 5);
    let replay = db
        .list_watch_events_after_position_checked_bounded(
            &target,
            fresh,
            std::num::NonZeroUsize::new(3).unwrap(),
        )
        .await
        .unwrap();
    assert!(
        matches!(
            replay,
            crate::datastore::PositionedWatchReplayRead::Events(_)
        ),
        "fresh LIST-to-WATCH cursor at the snapshot high-water must not expire"
    );

    let stale =
        crate::datastore::WatchReplayPosition::from_resource_version_through_event_id(10, 4);
    assert!(matches!(
        db.list_watch_events_after_position_checked_bounded(
            &target,
            stale,
            std::num::NonZeroUsize::new(3).unwrap(),
        )
        .await
        .unwrap(),
        crate::datastore::PositionedWatchReplayRead::Expired
    ));
}

#[tokio::test]
async fn stable_snapshot_floor_missing_exact_flag_keeps_positioned_handoff() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.replace_replicated_resource_state(
        Vec::new(),
        10,
        Some(5),
        Some(vec![crate::datastore::WatchReplayFloor {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace_key: "default".to_string(),
            floor_resource_version: 10,
            floor_event_id: 5,
            position_is_exact: false,
        }]),
        None,
    )
    .await
    .unwrap();

    let target = [crate::datastore::WatchTarget::namespaced_in_namespace(
        "v1",
        "ConfigMap",
        "default",
    )];
    let fresh =
        crate::datastore::WatchReplayPosition::from_resource_version_through_event_id(10, 5);
    assert!(matches!(
        db.list_watch_events_after_position_checked_bounded(
            &target,
            fresh,
            std::num::NonZeroUsize::new(3).unwrap(),
        )
        .await
        .unwrap(),
        crate::datastore::PositionedWatchReplayRead::Events(_)
    ));

    let stale =
        crate::datastore::WatchReplayPosition::from_resource_version_through_event_id(10, 4);
    assert!(matches!(
        db.list_watch_events_after_position_checked_bounded(
            &target,
            stale,
            std::num::NonZeroUsize::new(3).unwrap(),
        )
        .await
        .unwrap(),
        crate::datastore::PositionedWatchReplayRead::Expired
    ));
}

#[tokio::test]
async fn zero_high_water_legacy_snapshot_does_not_block_future_positioned_handoff() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.replace_replicated_resource_state(Vec::new(), 10, None, None, None)
        .await
        .unwrap();
    let created = db
        .create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "after-legacy",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"namespace": "default", "name": "after-legacy"}
            }),
        )
        .await
        .unwrap();
    let position = db.current_watch_replay_position().await.unwrap();
    assert!(position.event_id > 0);

    let target = [crate::datastore::WatchTarget::namespaced_in_namespace(
        "v1",
        "ConfigMap",
        "default",
    )];
    assert!(matches!(
        db.list_watch_events_after_position_checked_bounded(
            &target,
            position,
            std::num::NonZeroUsize::new(3).unwrap(),
        )
        .await
        .unwrap(),
        crate::datastore::PositionedWatchReplayRead::Events(_)
    ));
    assert!(matches!(
        db.list_watch_events_since_checked_bounded(
            &target,
            created.resource_version - 2,
            std::num::NonZeroUsize::new(3).unwrap(),
        )
        .await
        .unwrap(),
        crate::datastore::WatchReplayRead::Expired
    ));
}

#[tokio::test]
async fn committed_apply_v1_multi_mutation_shares_one_rv() {
    let db = Datastore::new_in_memory().await.unwrap();
    enable_committed_apply_v1(&db).await;
    let result = db
        .apply_raft_log_apply_commit(committed_apply_v1(
            crate::datastore::test_support::test_live_commit(
                0,
                vec![
                    v1_resource("v1-left", "v1-left-uid"),
                    v1_resource("v1-right", "v1-right-uid"),
                ],
            ),
        ))
        .await
        .unwrap();
    let rv = result.applied_rv.unwrap();
    for name in ["v1-left", "v1-right"] {
        assert_eq!(
            db.get_resource("v1", "ConfigMap", Some("default"), name)
                .await
                .unwrap()
                .unwrap()
                .resource_version,
            rv
        );
    }
}

#[tokio::test]
async fn raft_apply_stale_ordinary_put_returns_terminal_conflict_without_side_effects() {
    let db = Datastore::new_in_memory().await.unwrap();
    enable_committed_apply_v1(&db).await;
    let created = db
            .create_resource(
                "v1",
                "ConfigMap",
                Some("default"),
                "stale-put",
                serde_json::json!({"metadata":{"name":"stale-put","namespace":"default","uid":"stale-put-uid"},"data":{"v":"old"}}),
            )
            .await
            .unwrap();
    let current = db
            .update_resource(
                "v1",
                "ConfigMap",
                Some("default"),
                "stale-put",
                serde_json::json!({"metadata":{"name":"stale-put","namespace":"default","uid":"stale-put-uid"},"data":{"v":"current"}}),
                created.resource_version,
            )
            .await
            .unwrap();
    let rv_before = db.get_current_resource_version().await.unwrap();
    let watch_before = db
        .list_resources_modified_since("v1", "ConfigMap", Some("default"), 0)
        .await
        .unwrap()
        .len();
    let result = db
            .apply_raft_log_apply_commit(committed_apply_v1(crate::datastore::test_support::test_live_commit(
                0,
                vec![LogApplyMutation::PutResource(LogApplyResourceRow {
                    api_version: "v1".into(), kind: "ConfigMap".into(), namespace: Some("default".into()), name: "stale-put".into(), uid: current.uid.clone(), resource_version: 0,
                    data: serde_json::json!({"metadata":{"name":"stale-put","namespace":"default","uid":"stale-put-uid"},"data":{"v":"stale"}}),
                    require_absent: false, require_existing: true, precondition_uid: Some(current.uid), precondition_resource_version: Some(created.resource_version), status_only: false,
                })],
            )))
            .await
            .unwrap();
    assert_eq!(result.applied_rv, None);
    assert!(result.error_message.is_some());
    assert_eq!(db.get_current_resource_version().await.unwrap(), rv_before);
    assert_eq!(
        db.get_resource("v1", "ConfigMap", Some("default"), "stale-put")
            .await
            .unwrap()
            .unwrap()
            .data
            .pointer("/data/v")
            .and_then(|value| value.as_str()),
        Some("current")
    );
    assert_eq!(
        db.list_resources_modified_since("v1", "ConfigMap", Some("default"), 0)
            .await
            .unwrap()
            .len(),
        watch_before
    );
}

#[tokio::test]
async fn raft_apply_stale_ordinary_patch_returns_terminal_conflict_without_side_effects() {
    let db = Datastore::new_in_memory().await.unwrap();
    enable_committed_apply_v1(&db).await;
    let created = db
            .create_resource("v1", "ConfigMap", Some("default"), "stale-patch", serde_json::json!({"metadata":{"name":"stale-patch","namespace":"default","uid":"stale-patch-uid"},"data":{"v":"old"}}))
            .await.unwrap();
    db.update_resource("v1", "ConfigMap", Some("default"), "stale-patch", serde_json::json!({"metadata":{"name":"stale-patch","namespace":"default","uid":"stale-patch-uid"},"data":{"v":"current"}}), created.resource_version).await.unwrap();
    let rv_before = db.get_current_resource_version().await.unwrap();
    let watch_before = db
        .list_resources_modified_since("v1", "ConfigMap", Some("default"), 0)
        .await
        .unwrap()
        .len();
    let result = db
        .apply_raft_log_apply_commit(committed_apply_v1(
            crate::datastore::test_support::test_live_commit(
                0,
                vec![LogApplyMutation::PatchResourceLatest(
                    LogApplyResourcePatch {
                        api_version: "v1".into(),
                        kind: "ConfigMap".into(),
                        namespace: Some("default".into()),
                        name: "stale-patch".into(),
                        resource_version: 0,
                        patch_kind: crate::datastore::PatchKind::Merge,
                        patch: serde_json::json!({"data":{"v":"stale"}}),
                        require_existing: true,
                        precondition_uid: Some("stale-patch-uid".into()),
                        precondition_resource_version: Some(created.resource_version),
                        terminating_pod_unready_timestamp: None,
                    },
                )],
            ),
        ))
        .await
        .unwrap();
    assert_eq!(result.applied_rv, None);
    assert!(result.error_message.is_some());
    assert_eq!(db.get_current_resource_version().await.unwrap(), rv_before);
    assert_eq!(
        db.get_resource("v1", "ConfigMap", Some("default"), "stale-patch")
            .await
            .unwrap()
            .unwrap()
            .data
            .pointer("/data/v")
            .and_then(|value| value.as_str()),
        Some("current")
    );
    assert_eq!(
        db.list_resources_modified_since("v1", "ConfigMap", Some("default"), 0)
            .await
            .unwrap()
            .len(),
        watch_before
    );
}

#[tokio::test]
async fn raft_apply_stale_ordinary_delete_returns_terminal_conflict_without_side_effects() {
    let db = Datastore::new_in_memory().await.unwrap();
    enable_committed_apply_v1(&db).await;
    let created = db.create_resource("v1", "ConfigMap", Some("default"), "stale-delete", serde_json::json!({"metadata":{"name":"stale-delete","namespace":"default","uid":"stale-delete-uid"}})).await.unwrap();
    db.update_resource("v1", "ConfigMap", Some("default"), "stale-delete", serde_json::json!({"metadata":{"name":"stale-delete","namespace":"default","uid":"stale-delete-uid"},"data":{"v":"current"}}), created.resource_version).await.unwrap();
    let rv_before = db.get_current_resource_version().await.unwrap();
    let watch_before = db
        .list_resources_modified_since("v1", "ConfigMap", Some("default"), 0)
        .await
        .unwrap()
        .len();
    let result = db
        .apply_raft_log_apply_commit(committed_apply_v1(
            crate::datastore::test_support::test_live_commit(
                0,
                vec![LogApplyMutation::DeleteResource(LogApplyResourceKey {
                    api_version: "v1".into(),
                    kind: "ConfigMap".into(),
                    namespace: Some("default".into()),
                    name: "stale-delete".into(),
                    uid: "stale-delete-uid".into(),
                    precondition_resource_version: Some(created.resource_version),
                })],
            ),
        ))
        .await
        .unwrap();
    assert_eq!(result.applied_rv, None);
    assert!(result.error_message.is_some());
    assert_eq!(db.get_current_resource_version().await.unwrap(), rv_before);
    assert!(
        db.get_resource("v1", "ConfigMap", Some("default"), "stale-delete")
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        db.list_resources_modified_since("v1", "ConfigMap", Some("default"), 0)
            .await
            .unwrap()
            .len(),
        watch_before
    );
}

#[tokio::test]
async fn raft_apply_stale_status_without_stamp_returns_terminal_conflict_without_side_effects() {
    let db = Datastore::new_in_memory().await.unwrap();
    enable_committed_apply_v1(&db).await;
    let created = db.create_resource("v1", "Pod", Some("default"), "stale-status", serde_json::json!({"metadata":{"name":"stale-status","namespace":"default","uid":"stale-status-uid"},"spec":{"nodeName":"node-a"},"status":{"phase":"Pending"}})).await.unwrap();
    db.update_resource("v1", "Pod", Some("default"), "stale-status", serde_json::json!({"metadata":{"name":"stale-status","namespace":"default","uid":"stale-status-uid","labels":{"scheduler":"owned"}},"spec":{"nodeName":"node-a"},"status":{"phase":"Running"}}), created.resource_version).await.unwrap();
    let rv_before = db.get_current_resource_version().await.unwrap();
    let watch_before = db
        .list_resources_modified_since("v1", "Pod", Some("default"), 0)
        .await
        .unwrap()
        .len();
    let result = db.apply_raft_log_apply_commit(committed_apply_v1(crate::datastore::test_support::test_live_commit(0, vec![LogApplyMutation::PutResource(LogApplyResourceRow {
            api_version:"v1".into(), kind:"Pod".into(), namespace:Some("default".into()), name:"stale-status".into(), uid:"stale-status-uid".into(), resource_version:0,
            data:serde_json::json!({"metadata":{"name":"stale-status","namespace":"default","uid":"stale-status-uid"},"spec":{"nodeName":"node-a"},"status":{"phase":"Failed"}}), require_absent:false, require_existing:true, precondition_uid:Some("stale-status-uid".into()), precondition_resource_version:Some(created.resource_version), status_only:true,
        })]))).await.unwrap();
    assert_eq!(result.applied_rv, None);
    assert!(result.error_message.is_some());
    assert_eq!(db.get_current_resource_version().await.unwrap(), rv_before);
    assert_eq!(
        db.get_resource("v1", "Pod", Some("default"), "stale-status")
            .await
            .unwrap()
            .unwrap()
            .data
            .pointer("/status/phase")
            .and_then(|v| v.as_str()),
        Some("Running")
    );
    assert_eq!(
        db.list_resources_modified_since("v1", "Pod", Some("default"), 0)
            .await
            .unwrap()
            .len(),
        watch_before
    );
}

#[tokio::test]
async fn committed_apply_v1_stale_pod_status_stamp_replay_updates_only_outbox() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_root = dir.path().join("db");
    let supervisor = std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let db = Datastore::new_persistent(&db_root, supervisor.clone(), None)
        .await
        .unwrap();
    enable_committed_apply_v1(&db).await;

    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "stamped-status",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "stamped-status",
                "namespace": "default",
                "uid": "stamped-status-uid"
            },
            "status": {"phase": "Pending", "message": "origin"}
        }),
    )
    .await
    .unwrap();

    let keys = ["fresh", "stale", "same", "newer"];
    let before = pod_status_apply_snapshot(&db, "stamped-status", &keys).await;
    assert_eq!(before.status_message, "origin");
    assert!(
        before.outbox_rows.is_empty(),
        "test starts without applied outbox rows"
    );
    assert!(
        before.watermarks.is_empty(),
        "test starts without stream watermarks"
    );

    let fresh = db
        .apply_raft_log_apply_commit(watermarked_pod_status_commit_with_stamp(
            "fresh",
            "fresh",
            200,
            1,
            "stamped-status",
            "stamped-status-uid",
        ))
        .await
        .unwrap();
    assert_eq!(fresh.error_message, None);
    assert!(fresh.public_resource_changed);
    let fresh_rv = fresh.applied_rv.expect("fresh status allocates an RV");
    assert!(
        fresh_rv > before.current_rv,
        "fresh stamped status must allocate one committed RV"
    );

    let after_fresh = pod_status_apply_snapshot(&db, "stamped-status", &keys).await;
    assert_eq!(after_fresh.current_rv, fresh_rv);
    assert_eq!(after_fresh.pod_rv, fresh_rv);
    assert_eq!(after_fresh.status_message, "fresh");
    assert_eq!(after_fresh.watch_count, before.watch_count + 1);
    assert_eq!(after_fresh.outbox_rows.len(), 1);
    assert_eq!(after_fresh.outbox_rows[0].idempotency_key, "fresh");
    assert_eq!(after_fresh.outbox_rows[0].applied_rv, Some(fresh_rv));
    assert_eq!(after_fresh.outbox_rows[0].status_stamp, Some(200));
    assert_eq!(applied_outbox_ack_rv(&after_fresh.outbox_rows[0]), fresh_rv);
    assert_eq!(
        after_fresh.watermarks,
        vec![OutboxStreamWatermark {
            client_id: "worker-status-client".to_string(),
            stream_id: 7,
            stream_seq: 1,
        }]
    );

    let stale = db
        .apply_raft_log_apply_commit(watermarked_pod_status_commit_with_stamp(
            "stale",
            "stale",
            100,
            2,
            "stamped-status",
            "stamped-status-uid",
        ))
        .await
        .unwrap();
    assert_eq!(stale.error_message, None);
    assert!(
        !stale.public_resource_changed,
        "stale status ledger-only commits must not request downstream effects"
    );
    assert_eq!(
        stale.applied_rv,
        Some(fresh_rv),
        "stale stamp reports the current committed RV without allocating"
    );

    let after_stale = pod_status_apply_snapshot(&db, "stamped-status", &keys).await;
    assert_eq!(after_stale.current_rv, fresh_rv);
    assert_eq!(after_stale.pod_rv, fresh_rv);
    assert_eq!(after_stale.status_message, "fresh");
    assert_eq!(after_stale.watch_count, after_fresh.watch_count);
    assert_eq!(after_stale.outbox_rows.len(), 2);
    let stale_row = after_stale
        .outbox_rows
        .iter()
        .find(|row| row.idempotency_key == "stale")
        .expect("stale terminal ledger row");
    assert_eq!(stale_row.applied_rv, Some(fresh_rv));
    assert_eq!(stale_row.status_stamp, Some(100));
    assert_eq!(applied_outbox_ack_rv(stale_row), fresh_rv);
    assert_eq!(
        after_stale.watermarks,
        vec![OutboxStreamWatermark {
            client_id: "worker-status-client".to_string(),
            stream_id: 7,
            stream_seq: 2,
        }]
    );

    let same_stamp = db
        .apply_raft_log_apply_commit(watermarked_pod_status_commit_with_stamp(
            "same",
            "same",
            200,
            3,
            "stamped-status",
            "stamped-status-uid",
        ))
        .await
        .unwrap();
    assert_eq!(same_stamp.error_message, None);
    assert!(
        !same_stamp.public_resource_changed,
        "equal status ledger-only commits must not request downstream effects"
    );
    assert_eq!(
        same_stamp.applied_rv,
        Some(fresh_rv),
        "equal stamp reports the current committed RV without allocating"
    );

    let after_same = pod_status_apply_snapshot(&db, "stamped-status", &keys).await;
    assert_eq!(after_same.current_rv, fresh_rv);
    assert_eq!(after_same.pod_rv, fresh_rv);
    assert_eq!(after_same.status_message, "fresh");
    assert_eq!(after_same.watch_count, after_stale.watch_count);
    assert_eq!(after_same.outbox_rows.len(), 3);
    let same_row = after_same
        .outbox_rows
        .iter()
        .find(|row| row.idempotency_key == "same")
        .expect("equal terminal ledger row");
    assert_eq!(same_row.applied_rv, Some(fresh_rv));
    assert_eq!(same_row.status_stamp, Some(200));
    assert_eq!(applied_outbox_ack_rv(same_row), fresh_rv);
    assert_eq!(
        after_same.watermarks,
        vec![OutboxStreamWatermark {
            client_id: "worker-status-client".to_string(),
            stream_id: 7,
            stream_seq: 3,
        }]
    );

    let newer = db
        .apply_raft_log_apply_commit(watermarked_pod_status_commit_with_stamp(
            "newer",
            "newer",
            300,
            4,
            "stamped-status",
            "stamped-status-uid",
        ))
        .await
        .unwrap();
    assert_eq!(newer.error_message, None);
    assert!(newer.public_resource_changed);
    let newer_rv = newer.applied_rv.expect("newer status allocates an RV");
    assert!(
        newer_rv > fresh_rv,
        "newer stamp after stale/equal terminal rows must still apply"
    );

    let after_newer = pod_status_apply_snapshot(&db, "stamped-status", &keys).await;
    assert_eq!(after_newer.current_rv, newer_rv);
    assert_eq!(after_newer.pod_rv, newer_rv);
    assert_eq!(after_newer.status_message, "newer");
    assert_eq!(after_newer.watch_count, after_same.watch_count + 1);
    assert_eq!(after_newer.outbox_rows.len(), 4);
    let newer_row = after_newer
        .outbox_rows
        .iter()
        .find(|row| row.idempotency_key == "newer")
        .expect("newer ledger row");
    assert_eq!(newer_row.applied_rv, Some(newer_rv));
    assert_eq!(newer_row.status_stamp, Some(300));
    assert_eq!(applied_outbox_ack_rv(newer_row), newer_rv);
    assert_eq!(
        after_newer.watermarks,
        vec![OutboxStreamWatermark {
            client_id: "worker-status-client".to_string(),
            stream_id: 7,
            stream_seq: 4,
        }]
    );

    drop(db);
    let reopened = Datastore::new_persistent(&db_root, supervisor, None)
        .await
        .unwrap();
    let after_reopen = pod_status_apply_snapshot(&reopened, "stamped-status", &keys).await;
    assert_eq!(after_reopen.current_rv, after_newer.current_rv);
    assert_eq!(after_reopen.pod_rv, after_newer.pod_rv);
    assert_eq!(after_reopen.status_message, after_newer.status_message);
    assert_eq!(after_reopen.watch_count, after_newer.watch_count);
    assert_eq!(after_reopen.watermarks, after_newer.watermarks);
    assert_eq!(
        after_reopen.outbox_rows.len(),
        after_newer.outbox_rows.len()
    );
    for row in &after_reopen.outbox_rows {
        let expected_rv = if row.idempotency_key == "newer" {
            newer_rv
        } else {
            fresh_rv
        };
        assert_eq!(row.applied_rv, Some(expected_rv));
        assert_eq!(applied_outbox_ack_rv(row), expected_rv);
    }
}

#[tokio::test]
async fn committed_apply_v1_conflict_and_duplicate_do_not_allocate_rv() {
    let db = Datastore::new_in_memory().await.unwrap();
    enable_committed_apply_v1(&db).await;
    db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "v1-existing",
            serde_json::json!({"metadata": {"name": "v1-existing", "namespace": "default", "uid": "existing-uid"}}),
        )
        .await
        .unwrap();
    let before_conflict = db.get_current_resource_version().await.unwrap();
    let conflict = db
        .apply_raft_log_apply_commit(committed_apply_v1(
            crate::datastore::test_support::test_live_commit(
                0,
                vec![v1_resource("v1-existing", "new-uid")],
            ),
        ))
        .await
        .unwrap();
    assert!(conflict.error_message.is_some());
    assert!(!conflict.public_resource_changed);
    assert_eq!(
        db.get_current_resource_version().await.unwrap(),
        before_conflict
    );

    let outbox = klights_cluster_core::LogApplyAppliedOutboxRow {
        idempotency_key: "v1-duplicate".to_string(),
        subject_key: "v1/Pod/default/test".to_string(),
        operation: "PodStatus".to_string(),
        first_seen_ms: 1,
        applied_rv: None,
        result_proto: crate::datastore::sqlite::outbox_codec::encode(
            &klights_cluster_core::command::StorageResponse::Ack {
                resource_version: 0,
            },
        )
        .unwrap(),
        status_stamp: None,
    };
    let commit = committed_apply_v1(crate::datastore::test_support::test_live_commit(
        0,
        vec![LogApplyMutation::PutAppliedOutbox(outbox)],
    ));
    db.apply_raft_log_apply_commit(commit.clone())
        .await
        .unwrap();
    let before_duplicate = db.get_current_resource_version().await.unwrap();
    let duplicate = db.apply_raft_log_apply_commit(commit).await.unwrap();
    assert_eq!(duplicate.applied_rv, Some(0));
    assert!(
        !duplicate.public_resource_changed,
        "duplicate committed ledger entries must not request downstream effects"
    );
    assert_eq!(
        db.get_current_resource_version().await.unwrap(),
        before_duplicate
    );
}

#[tokio::test]
async fn outbox_terminal_decision_commits_error_ledger_and_watermark_without_public_rv() {
    let db = Datastore::new_in_memory().await.unwrap();
    enable_committed_apply_v1(&db).await;
    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "terminal-existing",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "namespace": "default",
                "name": "terminal-existing",
                "uid": "existing-uid"
            }
        }),
    )
    .await
    .unwrap();
    let rv_before = db.get_current_resource_version().await.unwrap();
    let watch_position_before = db.current_watch_replay_position().await.unwrap();
    let ledger = |key: &str| {
        LogApplyMutation::PutAppliedOutbox(klights_cluster_core::LogApplyAppliedOutboxRow {
            idempotency_key: key.to_string(),
            subject_key: "v1/ConfigMap/default/terminal-existing".to_string(),
            operation: "PodStatus".to_string(),
            first_seen_ms: 1,
            applied_rv: None,
            result_proto: crate::datastore::sqlite::outbox_codec::encode(
                &klights_cluster_core::command::StorageResponse::Ack {
                    resource_version: 0,
                },
            )
            .unwrap(),
            status_stamp: None,
        })
    };

    let terminal = LogApplyCommit::try_new_with_watermark(
        vec![
            v1_resource("terminal-existing", "different-uid"),
            ledger("terminal-1"),
        ],
        Some(OutboxStreamWatermark {
            client_id: "terminal-client".to_string(),
            stream_id: 91,
            stream_seq: 1,
        }),
    )
    .unwrap();
    let rejected = db
        .apply_raft_log_apply_commit(committed_apply_v1(terminal))
        .await
        .unwrap();
    assert!(rejected.error_message.is_some());
    assert_eq!(db.get_current_resource_version().await.unwrap(), rv_before);
    assert_eq!(
        db.current_watch_replay_position().await.unwrap(),
        watch_position_before,
        "a terminal decision must not append public watch history"
    );
    assert_eq!(
        db.get_resource("v1", "ConfigMap", Some("default"), "terminal-existing")
            .await
            .unwrap()
            .expect("conflicting resource remains")
            .uid,
        "existing-uid",
        "a terminal decision must not mutate its conflicting resource"
    );
    assert_eq!(
        db.list_outbox_stream_watermarks().await.unwrap(),
        vec![OutboxStreamWatermark {
            client_id: "terminal-client".to_string(),
            stream_id: 91,
            stream_seq: 1,
        }],
        "a committed terminal decision must consume its assigned sequence"
    );
    let terminal_row = db
        .get_applied_outbox("terminal-1")
        .await
        .unwrap()
        .expect("terminal decision ledger row");
    assert!(matches!(
        crate::datastore::sqlite::outbox_codec::decode(&terminal_row.result_proto),
        Ok(klights_cluster_core::command::StorageResponse::Error { .. })
    ));

    let successor = LogApplyCommit::try_new_with_watermark(
        vec![
            v1_resource("after-terminal", "after-terminal-uid"),
            ledger("terminal-2"),
        ],
        Some(OutboxStreamWatermark {
            client_id: "terminal-client".to_string(),
            stream_id: 91,
            stream_seq: 2,
        }),
    )
    .unwrap();
    let applied = db
        .apply_raft_log_apply_commit(committed_apply_v1(successor))
        .await
        .expect("next ordered sequence applies after terminal decision");
    assert!(applied.error_message.is_none());
    assert_eq!(
        db.list_outbox_stream_watermarks().await.unwrap()[0].stream_seq,
        2
    );
}

fn subnet_commit(
    resource_version: i64,
    node_name: &str,
    subnet: Ipv4Addr,
) -> SnapshotRestoreOperation {
    snapshot_operation(
        resource_version,
        None,
        vec![LogApplyMutation::PutNodeSubnet(LogApplyNodeSubnetRow {
            node_name: node_name.to_string(),
            subnet: format!("{subnet}/24"),
            subnet_base_int: u32::from(subnet),
            gateway_ip: subnet.to_string(),
            node_ip: "192.0.2.1".to_string(),
            mode: "root".to_string(),
            hostport_range: None,
        })],
    )
}

#[test]
fn terminal_apply_conflict_classification_uses_typed_codes_not_message_text() {
    for code in [
        ApplyConflictCode::NotFound,
        ApplyConflictCode::AlreadyExists,
        ApplyConflictCode::UidPrecondition,
        ApplyConflictCode::ResourceVersionPrecondition,
    ] {
        let err = apply_conflict_error(code, "typed conflict without status text");
        assert!(
            is_terminal_apply_conflict(&err),
            "typed conflict {code:?} must classify as terminal"
        );
    }

    let transient = other_error("transient text mentioning 409 Conflict and 404 Not Found");
    assert!(
        !is_terminal_apply_conflict(&transient),
        "untyped internal errors must not classify as terminal by message text"
    );
}

fn dataplane_commit(
    resource_version: i64,
    node_name: &str,
    endpoint: &str,
    port: u16,
) -> SnapshotRestoreOperation {
    snapshot_operation(
        resource_version,
        None,
        vec![LogApplyMutation::PutNodeDataplane(
            LogApplyNodeDataplaneRow {
                node_name: node_name.to_string(),
                mode: "root".to_string(),
                encryption: "enabled".to_string(),
                public_key: Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string()),
                endpoint: endpoint.to_string(),
                port: Some(port),
            },
        )],
    )
}

fn node_commit(resource_version: i64, name: &str, uid: &str) -> SnapshotRestoreOperation {
    snapshot_operation(
        resource_version,
        None,
        vec![LogApplyMutation::PutResource(LogApplyResourceRow {
            api_version: "v1".to_string(),
            kind: "Node".to_string(),
            namespace: None,
            name: name.to_string(),
            uid: uid.to_string(),
            resource_version,
            data: serde_json::json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {
                    "name": name,
                    "uid": uid,
                    "resourceVersion": resource_version.to_string()
                }
            }),
            require_absent: false,
            require_existing: false,
            precondition_uid: None,
            precondition_resource_version: None,
            status_only: false,
        })],
    )
}

#[tokio::test]
async fn replace_replicated_resource_state_applies_and_prunes_peer_state() {
    let db = crate::datastore::test_support::in_memory().await;
    db.allocate_node_subnet("stale", "10.43.0.0/16", "192.0.2.200")
        .await
        .unwrap();
    db.update_node_dataplane(
        klights_cluster_store::DataplanePeerMetadata::try_new(
            "stale".to_string(),
            klights_cluster_store::DataplaneMode::Root,
            klights_cluster_store::DataplaneEncryption::Enabled,
            Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string()),
            Some("192.0.2.200".to_string()),
            Some(51_820),
        )
        .unwrap(),
    )
    .await
    .unwrap();

    db.replace_replicated_resource_state(
        vec![
            subnet_commit(1, "leader", Ipv4Addr::new(10, 42, 5, 0)),
            dataplane_commit(2, "leader", "192.0.2.1", 51_820),
        ],
        2,
        None,
        None,
        None,
    )
    .await
    .unwrap();

    assert!(
        db.get_node_subnet("leader").await.unwrap().is_some(),
        "snapshot restore must apply replicated node_subnets rows"
    );
    assert!(
        db.get_node_dataplane("leader").await.unwrap().is_some(),
        "snapshot restore must apply replicated node_dataplane rows"
    );
    assert!(
        db.get_node_subnet("stale").await.unwrap().is_none(),
        "snapshot restore must remove local peer rows absent from the leader snapshot"
    );
    assert!(
        db.get_node_dataplane("stale").await.unwrap().is_none(),
        "snapshot restore must remove stale dataplane metadata absent from the leader snapshot"
    );
}

#[tokio::test]
async fn replace_replicated_resource_state_applies_peer_rows_at_snapshot_rv() {
    let db = crate::datastore::test_support::in_memory().await;

    db.replace_replicated_resource_state(
        vec![
            node_commit(10, "worker", "node-uid"),
            subnet_commit(10, "worker", Ipv4Addr::new(10, 43, 0, 0)),
            dataplane_commit(10, "worker", "192.0.2.10", 7679),
        ],
        10,
        None,
        None,
        None,
    )
    .await
    .unwrap();

    assert_eq!(db.get_current_resource_version().await.unwrap(), 10);
    assert!(db.get_node_subnet("worker").await.unwrap().is_some());
    assert!(db.get_node_dataplane("worker").await.unwrap().is_some());
}

#[tokio::test]
async fn replace_replicated_resource_state_clears_stale_owner_ref_index_rows() {
    let db = crate::datastore::test_support::in_memory().await;
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "stale",
            "namespace": "default",
            "ownerReferences": [{
                "apiVersion": "apps/v1",
                "kind": "ReplicaSet",
                "name": "stale-rs",
                "uid": "stale-owner"
            }]
        },
        "spec": { "containers": [{ "name": "c", "image": "nginx" }] }
    });

    db.create_resource("v1", "Pod", Some("default"), "stale", pod)
        .await
        .unwrap();
    let before: i64 = db
        .db_call("test_count_owner_refs", |conn| {
            conn.query_row("SELECT COUNT(*) FROM resource_owner_refs", [], |row| {
                row.get(0)
            })
            .map_err(tokio_rusqlite::Error::from)
        })
        .await
        .unwrap();
    assert_eq!(before, 1);

    db.replace_replicated_resource_state(Vec::new(), 0, None, None, None)
        .await
        .unwrap();

    let after: i64 = db
        .db_call("test_count_owner_refs", |conn| {
            conn.query_row("SELECT COUNT(*) FROM resource_owner_refs", [], |row| {
                row.get(0)
            })
            .map_err(tokio_rusqlite::Error::from)
        })
        .await
        .unwrap();
    assert_eq!(
        after, 0,
        "snapshot replacement must clear owner-ref index rows for resources absent from the leader snapshot"
    );
}

#[tokio::test]
async fn replace_replicated_resource_state_restores_created_rv_from_watch_history() {
    let db = crate::datastore::test_support::in_memory().await;

    db.replace_replicated_resource_state(
        vec![
            snapshot_operation(
                5,
                None,
                vec![LogApplyMutation::PutResource(LogApplyResourceRow {
                    api_version: "v1".to_string(),
                    kind: "ConfigMap".to_string(),
                    namespace: Some("default".to_string()),
                    name: "from-snapshot".to_string(),
                    uid: "cm-uid".to_string(),
                    resource_version: 5,
                    data: serde_json::json!({
                        "apiVersion": "v1",
                        "kind": "ConfigMap",
                        "metadata": {
                            "name": "from-snapshot",
                            "namespace": "default",
                            "uid": "cm-uid",
                            "resourceVersion": "5"
                        },
                        "data": {"state": "current"}
                    }),
                    require_absent: false,
                    require_existing: false,
                    precondition_uid: None,
                    precondition_resource_version: None,
                    status_only: false,
                })],
            ),
            snapshot_watch_event(LogApplyWatchEventRow {
                event_id: None,
                api_version: "v1".to_string(),
                kind: "ConfigMap".to_string(),
                namespace: Some("default".to_string()),
                name: "from-snapshot".to_string(),
                resource_version: 2,
                event_type: "ADDED".to_string(),
                data: serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {
                        "name": "from-snapshot",
                        "namespace": "default",
                        "uid": "cm-uid",
                        "resourceVersion": "2"
                    },
                    "data": {"state": "created"}
                }),
            }),
            snapshot_watch_event(LogApplyWatchEventRow {
                event_id: None,
                api_version: "v1".to_string(),
                kind: "ConfigMap".to_string(),
                namespace: Some("default".to_string()),
                name: "from-snapshot".to_string(),
                resource_version: 5,
                event_type: "MODIFIED".to_string(),
                data: serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {
                        "name": "from-snapshot",
                        "namespace": "default",
                        "uid": "cm-uid",
                        "resourceVersion": "5"
                    },
                    "data": {"state": "current"}
                }),
            }),
        ],
        5,
        None,
        None,
        None,
    )
    .await
    .unwrap();

    let created_rv: i64 = db
        .db_call("test_created_rv_after_snapshot", |conn| {
            conn.query_row(
                "SELECT created_rv FROM namespaced_resources \
                     WHERE api_version = 'v1' AND kind = 'ConfigMap' \
                       AND namespace = 'default' AND name = 'from-snapshot'",
                [],
                |row| row.get(0),
            )
            .map_err(tokio_rusqlite::Error::from)
        })
        .await
        .unwrap();
    assert_eq!(
        created_rv, 2,
        "snapshot restore must preserve the leader's resource creation RV"
    );
}

#[tokio::test]
async fn snapshot_replacement_sets_allocator_to_leader_high_water_exactly() {
    let db = crate::datastore::test_support::in_memory().await;
    for index in 0..8 {
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            &format!("divergent-{index}"),
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "name": format!("divergent-{index}"),
                    "namespace": "default"
                }
            }),
        )
        .await
        .unwrap();
    }
    assert!(db.current_watch_replay_position().await.unwrap().event_id > 2);

    db.replace_replicated_resource_state(
        vec![snapshot_watch_event(LogApplyWatchEventRow {
            event_id: Some(2),
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace: Some("default".to_string()),
            name: "leader-row".to_string(),
            resource_version: 2,
            event_type: "ADDED".to_string(),
            data: serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "name": "leader-row",
                    "namespace": "default",
                    "resourceVersion": "2"
                }
            }),
        })],
        2,
        Some(2),
        None,
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        db.current_watch_replay_position().await.unwrap().event_id,
        2
    );

    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "after-restore",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "after-restore", "namespace": "default"}
        }),
    )
    .await
    .unwrap();
    assert_eq!(
        db.current_watch_replay_position().await.unwrap().event_id,
        3,
        "the first follower event must use the leader high-water plus one"
    );
}

#[tokio::test]
async fn snapshot_replacement_rejects_high_water_below_restored_event_id() {
    let db = crate::datastore::test_support::in_memory().await;
    let err = db
        .replace_replicated_resource_state(
            vec![snapshot_watch_event(LogApplyWatchEventRow {
                event_id: Some(4),
                api_version: "v1".to_string(),
                kind: "ConfigMap".to_string(),
                namespace: Some("default".to_string()),
                name: "leader-row".to_string(),
                resource_version: 2,
                event_type: "ADDED".to_string(),
                data: serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {
                        "name": "leader-row",
                        "namespace": "default",
                        "resourceVersion": "2"
                    }
                }),
            })],
            2,
            Some(3),
            None,
            None,
        )
        .await
        .expect_err("an allocator below a restored row must be rejected");
    assert!(err.to_string().contains("below restored event ID 4"));
}

#[tokio::test]
async fn snapshot_event_pages_follow_event_id_across_lower_resource_versions() {
    let db = crate::datastore::test_support::in_memory().await;
    let row = |event_id, resource_version, name: &str| {
        snapshot_watch_event(LogApplyWatchEventRow {
            event_id: Some(event_id),
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace: Some("default".to_string()),
            name: name.to_string(),
            resource_version,
            event_type: "ADDED".to_string(),
            data: serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "name": name,
                    "namespace": "default",
                    "resourceVersion": resource_version.to_string()
                }
            }),
        })
    };
    db.replace_replicated_resource_state(
        vec![row(1, 100, "higher-rv"), row(2, 50, "later-lower-rv")],
        100,
        Some(2),
        Some(Vec::new()),
        None,
    )
    .await
    .unwrap();

    let rows = db
        .list_all_watch_events_after_id_bounded(0, 2, std::num::NonZeroUsize::new(10).unwrap())
        .await
        .unwrap();
    assert_eq!(
        rows.iter()
            .map(|(id, event)| (*id, event.resource.resource_version))
            .collect::<Vec<_>>(),
        vec![(1, 100), (2, 50)]
    );
}

#[tokio::test]
async fn legacy_snapshot_without_floors_forces_relist_for_unknown_scopes() {
    let db = crate::datastore::test_support::in_memory().await;
    db.replace_replicated_resource_state(Vec::new(), 10, Some(5), None, None)
        .await
        .unwrap();
    let target = [crate::datastore::WatchTarget::namespaced_in_namespace(
        "example.test/v1",
        "GoneResource",
        "gone-ns",
    )];

    assert!(matches!(
        db.list_watch_events_after_position_checked_bounded(
            &target,
            crate::datastore::WatchReplayPosition {
                resource_version: 9,
                event_id: 4,
                resource_version_filter_through_event_id: 0,
            },
            std::num::NonZeroUsize::new(10).unwrap(),
        )
        .await
        .unwrap(),
        crate::datastore::PositionedWatchReplayRead::Expired
    ));
    assert!(matches!(
        db.list_watch_events_since_checked_bounded(
            &target,
            9,
            std::num::NonZeroUsize::new(10).unwrap(),
        )
        .await
        .unwrap(),
        crate::datastore::WatchReplayRead::Expired
    ));
    assert!(matches!(
        db.snapshot_resources_at_position(
            &target,
            None,
            None,
            crate::datastore::WatchReplayPosition {
                resource_version: 9,
                event_id: 4,
                resource_version_filter_through_event_id: 0,
            },
        )
        .await
        .unwrap(),
        crate::datastore::SnapshotAtRv::Expired
    ));
}

#[tokio::test]
async fn stale_uid_delete_does_not_remove_same_name_replacement() {
    // T1.1: follower applies a stale DeleteResource commit whose
    // LogApplyResourceKey.uid points at an older (already-replaced)
    // Pod identity. The same-name replacement Pod with a different
    // UID must remain in cluster.db; the stale delete is a no-op.
    let db = crate::datastore::test_support::in_memory().await;

    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "p1",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "p1",
                "namespace": "default",
                "uid": "pod-uid-A"
            }
        }),
    )
    .await
    .unwrap();

    db.apply_log_apply_commit(crate::datastore::test_support::test_live_commit(
        5,
        vec![LogApplyMutation::DeleteResource(LogApplyResourceKey {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "p1".to_string(),
            uid: "pod-uid-A".to_string(),
            precondition_resource_version: None,
        })],
    ))
    .await
    .unwrap();

    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "p1",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "p1",
                "namespace": "default",
                "uid": "pod-uid-B"
            }
        }),
    )
    .await
    .unwrap();

    // Replay the stale UID-A delete commit. With UID-qualified
    // deletes this must be a no-op; without the guard it would hit
    // by (api_version, kind, namespace, name) and remove UID-B's row.
    db.apply_log_apply_commit(crate::datastore::test_support::test_live_commit(
        7,
        vec![LogApplyMutation::DeleteResource(LogApplyResourceKey {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "p1".to_string(),
            uid: "pod-uid-A".to_string(),
            precondition_resource_version: None,
        })],
    ))
    .await
    .unwrap();

    let surviving = db
        .get_resource("v1", "Pod", Some("default"), "p1")
        .await
        .unwrap()
        .expect("replacement Pod with UID-B must survive stale UID-A delete");
    assert_eq!(
        surviving.uid, "pod-uid-B",
        "stale UID-A delete must not free the same-name slot for UID-B"
    );
}

/// A committed PUT with require_existing=true on an absent row returns a
/// terminal "404 Not Found" result. Fixed committed apply enforces both
/// structural presence and UID/resourceVersion preconditions.
#[tokio::test]
async fn raft_apply_missing_required_resource_returns_terminal_result() {
    let db = crate::datastore::test_support::in_memory().await;
    db.create_resource(
        "apps/v1",
        "Deployment",
        Some("default"),
        "stale-status",
        serde_json::json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": "stale-status",
                "namespace": "default",
                "uid": "deploy-uid"
            },
            "spec": {"selector": {"matchLabels": {"app": "stale-status"}}},
            "status": {"replicas": 1}
        }),
    )
    .await
    .unwrap();
    db.delete_resource_with_preconditions(
        "apps/v1",
        "Deployment",
        Some("default"),
        "stale-status",
        crate::datastore::ResourcePreconditions::uid("deploy-uid"),
    )
    .await
    .unwrap();

    let result = db
        .apply_raft_log_apply_commit(crate::datastore::test_support::test_live_commit(
            10,
            vec![LogApplyMutation::PutResource(LogApplyResourceRow {
                api_version: "apps/v1".to_string(),
                kind: "Deployment".to_string(),
                namespace: Some("default".to_string()),
                name: "stale-status".to_string(),
                uid: "deploy-uid".to_string(),
                resource_version: 10,
                data: serde_json::json!({
                    "apiVersion": "apps/v1",
                    "kind": "Deployment",
                    "metadata": {
                        "name": "stale-status",
                        "namespace": "default",
                        "uid": "deploy-uid",
                        "resourceVersion": "10"
                    },
                    "spec": {"selector": {"matchLabels": {"app": "stale-status"}}},
                    "status": {"replicas": 2}
                }),
                require_absent: false,
                require_existing: true,
                precondition_uid: Some("deploy-uid".to_string()),
                precondition_resource_version: None,
                status_only: false,
            })],
        ))
        .await
        .expect("raft apply must not fail committed log entry on stale missing resource");

    assert_eq!(result.applied_rv, None);
    assert!(
        result
            .error_message
            .as_deref()
            .is_some_and(|message| message.contains("404 Not Found")),
        "missing resource must be returned as a terminal command result, got {result:?}"
    );
    assert!(
        db.get_resource("apps/v1", "Deployment", Some("default"), "stale-status")
            .await
            .unwrap()
            .is_none(),
        "stale apply must not recreate the deleted resource"
    );
}

#[tokio::test]
async fn apply_log_apply_commit_broadcasts_explicit_watch_event() {
    let db = crate::datastore::test_support::in_memory().await;
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "bound-pod",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "bound-pod",
                "namespace": "default",
                "uid": "pod-uid"
            },
            "spec": {"containers": [{"name": "c", "image": "pause"}]}
        }),
    )
    .await
    .unwrap();
    let mut watch_rx = db.subscribe_watch(klights_watch::WatchTopic::new("v1", "Pod"));

    let leader_watch_row = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "bound-pod",
            "namespace": "default",
            "uid": "pod-uid",
            "resourceVersion": "7"
        },
        "spec": {
            "nodeName": "mn-controlplane3",
            "containers": [{"name": "c", "image": "pause"}]
        }
    });

    db.apply_log_apply_commit(crate::datastore::test_support::test_live_commit(
        7,
        vec![LogApplyMutation::PutWatchEvent(LogApplyWatchEventRow {
            event_id: None,
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "bound-pod".to_string(),
            resource_version: 7,
            event_type: "MODIFIED".to_string(),
            data: leader_watch_row,
        })],
    ))
    .await
    .unwrap();
    let applied_rv = db.get_current_resource_version().await.unwrap();

    let event = watch_rx
        .try_recv()
        .expect("explicit watch-history apply must wake local watchers");
    assert_eq!(event.event_type, crate::watch::EventType::Modified);
    assert_eq!(event.resource_version(), Some(applied_rv));
    assert_eq!(
        event
            .object
            .pointer("/spec/nodeName")
            .and_then(|v| v.as_str()),
        Some("mn-controlplane3")
    );
}

#[tokio::test]
async fn apply_log_apply_commit_replays_explicit_watch_event_without_synthesizing() {
    let db = crate::datastore::test_support::in_memory().await;
    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "exact-watch",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "exact-watch",
                "namespace": "default",
                "uid": "cm-uid"
            },
            "data": {"state": "initial"}
        }),
    )
    .await
    .unwrap();

    let resource_row = serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "exact-watch",
            "namespace": "default",
            "uid": "cm-uid",
            "resourceVersion": "7"
        },
        "data": {"state": "current-row"}
    });
    let leader_watch_row = serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "exact-watch",
            "namespace": "default",
            "uid": "cm-uid",
            "resourceVersion": "7"
        },
        "data": {"state": "leader-watch-history"}
    });

    db.apply_log_apply_commit(crate::datastore::test_support::test_live_commit(
        7,
        vec![
            LogApplyMutation::PutResource(LogApplyResourceRow {
                api_version: "v1".to_string(),
                kind: "ConfigMap".to_string(),
                namespace: Some("default".to_string()),
                name: "exact-watch".to_string(),
                uid: "cm-uid".to_string(),
                resource_version: 7,
                data: resource_row,
                require_absent: false,
                require_existing: false,
                precondition_uid: None,
                precondition_resource_version: None,
                status_only: false,
            }),
            LogApplyMutation::PutWatchEvent(LogApplyWatchEventRow {
                event_id: None,
                api_version: "v1".to_string(),
                kind: "ConfigMap".to_string(),
                namespace: Some("default".to_string()),
                name: "exact-watch".to_string(),
                resource_version: 7,
                event_type: "MODIFIED".to_string(),
                data: leader_watch_row.clone(),
            }),
        ],
    ))
    .await
    .unwrap();
    let applied_rv = db.get_current_resource_version().await.unwrap();
    let mut expected_watch_row = leader_watch_row;
    expected_watch_row["metadata"]["resourceVersion"] =
        serde_json::Value::String(applied_rv.to_string());

    let watch_data: String = db
        .db_call("test_exact_watch_history_after_log_apply", move |conn| {
            conn.query_row(
                "SELECT CAST(data AS TEXT) FROM watch_events WHERE resource_version = ?1",
                [applied_rv],
                |row| row.get(0),
            )
            .map_err(tokio_rusqlite::Error::from)
        })
        .await
        .unwrap();
    assert_eq!(watch_data, expected_watch_row.to_string());
}

#[tokio::test]
async fn apply_log_apply_commit_replays_explicit_watch_payload_without_synthesizing() {
    let db = crate::datastore::test_support::in_memory().await;
    let explicit_payload = serde_json::json!({
        "type": "ADDED",
        "object": {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "explicit-watch-payload",
                "namespace": "default",
                "uid": "explicit-watch-payload-uid"
            },
            "data": {
                "state": "raw"
            }
        }
    });
    let expected_watch_payload = explicit_payload.clone();

    db.apply_log_apply_commit(crate::datastore::test_support::test_live_commit(
        11,
        vec![LogApplyMutation::PutWatchEvent(LogApplyWatchEventRow {
            event_id: None,
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace: Some("default".to_string()),
            name: "explicit-watch-payload".to_string(),
            resource_version: 11,
            event_type: "ADDED".to_string(),
            data: explicit_payload,
        })],
    ))
    .await
    .unwrap();
    let applied_rv = db.get_current_resource_version().await.unwrap();
    let mut expected_watch_payload = expected_watch_payload;
    expected_watch_payload["object"]["metadata"]["resourceVersion"] =
        serde_json::Value::String(applied_rv.to_string());

    let watch_data: Vec<u8> = db
        .db_call("test_explicit_watch_event_no_synthesizing", move |conn| {
            Ok(conn.query_row(
                "SELECT data FROM watch_events WHERE resource_version = ?1",
                rusqlite::params![applied_rv],
                |row| row.get(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(
        watch_data,
        serde_json::to_vec(&expected_watch_payload).unwrap()
    );
}

#[tokio::test]
async fn committed_apply_v1_watch_event_payload_hydrates_only_resource_shape_events() {
    let db = Datastore::new_in_memory().await.unwrap();
    enable_committed_apply_v1(&db).await;

    let resource_shape_payload = serde_json::json!({
        "metadata": {
            "name": "v1-watch-resource-shape",
            "namespace": "default",
            "uid": "v1-watch-resource-shape-uid"
        },
        "data": {"state": "seed"},
    });

    let result = db
        .apply_raft_log_apply_commit(committed_apply_v1(
            crate::datastore::test_support::test_live_commit(
                0,
                vec![LogApplyMutation::PutWatchEvent(LogApplyWatchEventRow {
                    event_id: None,
                    api_version: "v1".to_string(),
                    kind: "ConfigMap".to_string(),
                    namespace: Some("default".to_string()),
                    name: "v1-watch-resource-shape".to_string(),
                    resource_version: 0,
                    event_type: "ADDED".to_string(),
                    data: resource_shape_payload,
                })],
            ),
        ))
        .await
        .unwrap();
    let applied_rv = result
        .applied_rv
        .expect("applied watch event should allocate RV");

    let watch_data: Vec<u8> = db
        .db_call(
            "test_committed_watch_event_resource_shape_hydrates",
            move |conn| {
                Ok(conn.query_row(
                    "SELECT data FROM watch_events WHERE resource_version = ?1",
                    rusqlite::params![applied_rv],
                    |row| row.get(0),
                )?)
            },
        )
        .await
        .unwrap();

    let hydrated = serde_json::to_vec(&serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "v1-watch-resource-shape",
            "namespace": "default",
            "uid": "v1-watch-resource-shape-uid",
            "resourceVersion": applied_rv.to_string()
        },
        "data": {"state": "seed"},
    }))
    .unwrap();
    assert_eq!(watch_data, hydrated);
}

#[tokio::test]
async fn live_watch_envelope_hydrates_nested_object_with_committed_rv() {
    let db = Datastore::new_in_memory().await.unwrap();
    let commit = LogApplyCommit::try_new(vec![LogApplyMutation::PutWatchEvent(
        LogApplyWatchEventRow {
            event_id: None,
            api_version: "v1".into(),
            kind: "ConfigMap".into(),
            namespace: Some("default".into()),
            name: "nested-live-watch".into(),
            resource_version: 0,
            event_type: "MODIFIED".into(),
            data: serde_json::json!({
                "type": "MODIFIED",
                "object": {
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {
                        "name": "nested-live-watch",
                        "namespace": "default",
                        "uid": "nested-live-watch-uid"
                    },
                    "data": {"state": "committed"}
                }
            }),
        },
    )])
    .unwrap();
    let applied_rv = db
        .apply_raft_log_apply_commit(commit)
        .await
        .unwrap()
        .applied_rv
        .unwrap();

    let stored: Vec<u8> = db
        .db_call("test_live_watch_nested_committed_rv", move |conn| {
            Ok(conn.query_row(
                "SELECT data FROM watch_events WHERE resource_version = ?1",
                [applied_rv],
                |row| row.get(0),
            )?)
        })
        .await
        .unwrap();
    let stored: serde_json::Value = serde_json::from_slice(&stored).unwrap();
    assert_eq!(
        stored.pointer("/object/metadata/resourceVersion"),
        Some(&serde_json::Value::String(applied_rv.to_string())),
        "live envelope object must receive the committed public RV exactly once"
    );
}

// ── Task 1: Committed Raft Apply Authoritative Over Stale Follower State ─────────────────

/// Committed delete must remove a row even when the follower's local rv differs from the
/// precondition the leader encoded (the follower missed an intermediate update).
#[tokio::test]
async fn committed_delete_applies_when_raw_resource_version_precondition_matches() {
    let db = crate::datastore::test_support::in_memory().await;
    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "stale-cm",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "stale-cm", "namespace": "default", "uid": "cm-stale-del"}
        }),
    )
    .await
    .unwrap();
    // Strict committed apply validates the raw precondition before normalization.
    let result = db
        .apply_raft_log_apply_commit(crate::datastore::test_support::test_live_commit(
            50,
            vec![LogApplyMutation::DeleteResource(LogApplyResourceKey {
                api_version: "v1".to_string(),
                kind: "ConfigMap".to_string(),
                namespace: Some("default".to_string()),
                name: "stale-cm".to_string(),
                uid: "cm-stale-del".to_string(),
                precondition_resource_version: Some(1),
            })],
        ))
        .await
        .expect("raft delete must not error on stale rv");
    assert!(
        result.error_message.is_none(),
        "committed delete must succeed without error_message: {result:?}"
    );
    let row = db
        .get_resource("v1", "ConfigMap", Some("default"), "stale-cm")
        .await
        .unwrap();
    assert!(
        row.is_none(),
        "committed delete must remove stale row; row still present: {row:?}"
    );
}

#[tokio::test]
async fn committed_namespace_delete_errors_on_corrupt_stored_json_instead_of_emitting_null_event() {
    let db = crate::datastore::test_support::in_memory().await;
    db.create_namespace(
        "corrupt-delete-ns",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {
                "name": "corrupt-delete-ns",
                "uid": "corrupt-delete-ns-uid"
            }
        }),
    )
    .await
    .expect("seed Namespace");

    db.db_call("test_corrupt_delete_namespace_data", |conn| {
        conn.execute(
            "UPDATE namespaces SET data = ?1 WHERE name = 'corrupt-delete-ns'",
            [b"{not-json".as_slice()],
        )?;
        Ok(())
    })
    .await
    .expect("corrupt stored JSON");

    let err = db
        .apply_raft_log_apply_commit(crate::datastore::test_support::test_live_commit(
            60,
            vec![LogApplyMutation::DeleteNamespace {
                name: "corrupt-delete-ns".to_string(),
            }],
        ))
        .await
        .expect_err("corrupt stored JSON must fail committed namespace delete apply");
    assert!(
        err.to_string().contains("expected ident")
            || err.to_string().contains("key must be a string"),
        "error must come from JSON decoding, got: {err}"
    );
}

/// Committed put applies when its raw UID/RV preconditions match.
#[tokio::test]
async fn committed_put_applies_with_matching_raw_preconditions() {
    let db = crate::datastore::test_support::in_memory().await;
    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "put-target",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "put-target", "namespace": "default", "uid": "cm-put-uid"},
            "data": {"k": "old"}
        }),
    )
    .await
    .unwrap();
    let result = db
        .apply_raft_log_apply_commit(crate::datastore::test_support::test_live_commit(
            60,
            vec![LogApplyMutation::PutResource(LogApplyResourceRow {
                api_version: "v1".to_string(),
                kind: "ConfigMap".to_string(),
                namespace: Some("default".to_string()),
                name: "put-target".to_string(),
                uid: "cm-put-uid".to_string(),
                resource_version: 60,
                data: serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {
                        "name": "put-target",
                        "namespace": "default",
                        "uid": "cm-put-uid",
                        "resourceVersion": "60"
                    },
                    "data": {"k": "committed"}
                }),
                require_absent: false,
                require_existing: false,
                precondition_uid: Some("cm-put-uid".to_string()),
                precondition_resource_version: Some(1),
                status_only: false,
            })],
        ))
        .await
        .expect("raft put must not error on stale rv");
    assert!(
        result.error_message.is_none(),
        "committed PUT must succeed: {result:?}"
    );
    let row = db
        .get_resource("v1", "ConfigMap", Some("default"), "put-target")
        .await
        .unwrap()
        .expect("committed PUT must materialise the row");
    assert_eq!(
        row.data.pointer("/data/k").and_then(|v| v.as_str()),
        Some("committed"),
        "row data must reflect the committed value"
    );
    assert_eq!(
        Some(row.resource_version),
        result.applied_rv,
        "committed apply owns the public RV"
    );
}

#[tokio::test]
async fn stale_same_uid_committed_put_does_not_revert_newer_client_owned_state() {
    let db = crate::datastore::test_support::in_memory().await;
    let created = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "web",
            serde_json::json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {
                    "name": "web",
                    "namespace": "default",
                    "uid": "deploy-stale-put-uid",
                    "generation": 2
                },
                "spec": {"replicas": 10},
                "status": {"replicas": 13, "availableReplicas": 8}
            }),
        )
        .await
        .unwrap();

    let mut scaled = (*created.data).clone();
    scaled["metadata"]["generation"] = serde_json::json!(3);
    scaled["spec"]["replicas"] = serde_json::json!(30);
    db.update_resource(
        "apps/v1",
        "Deployment",
        Some("default"),
        "web",
        scaled,
        created.resource_version,
    )
    .await
    .unwrap();

    let result = db
        .apply_raft_log_apply_commit(crate::datastore::test_support::test_live_commit(
            60,
            vec![LogApplyMutation::PutResource(LogApplyResourceRow {
                api_version: "apps/v1".to_string(),
                kind: "Deployment".to_string(),
                namespace: Some("default".to_string()),
                name: "web".to_string(),
                uid: "deploy-stale-put-uid".to_string(),
                resource_version: 60,
                data: serde_json::json!({
                    "apiVersion": "apps/v1",
                    "kind": "Deployment",
                    "metadata": {
                        "name": "web",
                        "namespace": "default",
                        "uid": "deploy-stale-put-uid",
                        "resourceVersion": "60",
                        "generation": 2
                    },
                    "spec": {"replicas": 10},
                    "status": {"replicas": 13, "availableReplicas": 8}
                }),
                require_absent: false,
                require_existing: true,
                precondition_uid: Some("deploy-stale-put-uid".to_string()),
                precondition_resource_version: Some(created.resource_version),
                status_only: false,
            })],
        ))
        .await
        .expect("stale committed PUT should apply without surfacing a state-machine error");
    assert!(
        result.error_message.is_some(),
        "stale committed PUT must fail strict RV validation: {result:?}"
    );

    let row = db
        .get_resource("apps/v1", "Deployment", Some("default"), "web")
        .await
        .unwrap()
        .expect("deployment remains after stale committed put");
    assert_eq!(
        row.data.pointer("/spec/replicas"),
        Some(&serde_json::json!(30)),
        "same-UID stale committed PUT must not roll back a newer Deployment scale update"
    );
    assert_eq!(
        row.data.pointer("/metadata/generation"),
        Some(&serde_json::json!(3)),
        "same-UID stale committed PUT must preserve the newer client-owned generation"
    );
    assert_eq!(result.applied_rv, None);
}

#[tokio::test]
async fn stale_same_uid_generationless_committed_put_preserves_newer_configmap_state() {
    let db = crate::datastore::test_support::in_memory().await;
    let seed = db
        .apply_raft_log_apply_commit(crate::datastore::test_support::test_live_commit(
            10,
            vec![LogApplyMutation::PutResource(LogApplyResourceRow {
                api_version: "v1".to_string(),
                kind: "ConfigMap".to_string(),
                namespace: Some("default".to_string()),
                name: "generationless-cm".to_string(),
                uid: "generationless-cm-uid".to_string(),
                resource_version: 10,
                data: serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {
                        "name": "generationless-cm",
                        "namespace": "default",
                        "uid": "generationless-cm-uid",
                        "resourceVersion": "10"
                    },
                    "data": {"winner": "initial"}
                }),
                require_absent: false,
                require_existing: false,
                precondition_uid: None,
                precondition_resource_version: None,
                status_only: false,
            })],
        ))
        .await
        .expect("seed generation-less ConfigMap from raft");

    db.apply_raft_log_apply_commit(crate::datastore::test_support::test_live_commit(
        20,
        vec![LogApplyMutation::PutResource(LogApplyResourceRow {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace: Some("default".to_string()),
            name: "generationless-cm".to_string(),
            uid: "generationless-cm-uid".to_string(),
            resource_version: 20,
            data: serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "name": "generationless-cm",
                    "namespace": "default",
                    "uid": "generationless-cm-uid",
                    "resourceVersion": "20"
                },
                "data": {"winner": "newer"}
            }),
            require_absent: false,
            require_existing: true,
            precondition_uid: Some("generationless-cm-uid".to_string()),
            precondition_resource_version: Some(10),
            status_only: false,
        })],
    ))
    .await
    .expect("newer generation-less ConfigMap update applies");
    assert!(seed.error_message.is_none());

    let result = db
        .apply_raft_log_apply_commit(crate::datastore::test_support::test_live_commit(
            30,
            vec![LogApplyMutation::PutResource(LogApplyResourceRow {
                api_version: "v1".to_string(),
                kind: "ConfigMap".to_string(),
                namespace: Some("default".to_string()),
                name: "generationless-cm".to_string(),
                uid: "generationless-cm-uid".to_string(),
                resource_version: 30,
                data: serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {
                        "name": "generationless-cm",
                        "namespace": "default",
                        "uid": "generationless-cm-uid",
                        "resourceVersion": "30"
                    },
                    "data": {"winner": "stale"}
                }),
                require_absent: false,
                require_existing: true,
                precondition_uid: Some("generationless-cm-uid".to_string()),
                precondition_resource_version: Some(10),
                status_only: false,
            })],
        ))
        .await
        .expect("stale generation-less committed PUT should not fail raft apply");
    assert!(result.error_message.is_some());

    let row = db
        .get_resource("v1", "ConfigMap", Some("default"), "generationless-cm")
        .await
        .unwrap()
        .expect("ConfigMap remains after stale committed put");
    assert_eq!(
        row.data
            .pointer("/data/winner")
            .and_then(|value| value.as_str()),
        Some("initial"),
        "rejected writes must preserve the current same-UID state"
    );
    assert!(
        row.data.pointer("/metadata/generation").is_none(),
        "test fixture must remain generation-less"
    );
    assert_eq!(row.resource_version, 1);
}

#[tokio::test]
async fn newer_generation_committed_put_applies_after_status_only_rv_advance() {
    let db = crate::datastore::test_support::in_memory().await;
    let created = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "web",
            serde_json::json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {
                    "name": "web",
                    "namespace": "default",
                    "uid": "deploy-newer-generation-put-uid",
                    "generation": 2
                },
                "spec": {"replicas": 10},
                "status": {"replicas": 13, "availableReplicas": 8}
            }),
        )
        .await
        .unwrap();

    db.update_status_only_with_preconditions(
        "apps/v1",
        "Deployment",
        Some("default"),
        "web",
        serde_json::json!({
            "replicas": 13,
            "availableReplicas": 8,
            "observedGeneration": 2
        }),
        crate::datastore::ResourcePreconditions::uid(created.uid.clone()),
    )
    .await
    .expect("status update advances RV before stale-precondition scale PUT apply");

    let result = db
        .apply_raft_log_apply_commit(crate::datastore::test_support::test_live_commit(
            60,
            vec![LogApplyMutation::PutResource(LogApplyResourceRow {
                api_version: "apps/v1".to_string(),
                kind: "Deployment".to_string(),
                namespace: Some("default".to_string()),
                name: "web".to_string(),
                uid: "deploy-newer-generation-put-uid".to_string(),
                resource_version: 60,
                data: serde_json::json!({
                    "apiVersion": "apps/v1",
                    "kind": "Deployment",
                    "metadata": {
                        "name": "web",
                        "namespace": "default",
                        "uid": "deploy-newer-generation-put-uid",
                        "resourceVersion": "60",
                        "generation": 3
                    },
                    "spec": {"replicas": 30},
                    "status": {
                        "replicas": 13,
                        "availableReplicas": 8,
                        "observedGeneration": 2
                    }
                }),
                require_absent: false,
                require_existing: true,
                precondition_uid: Some("deploy-newer-generation-put-uid".to_string()),
                precondition_resource_version: Some(created.resource_version + 1),
                status_only: false,
            })],
        ))
        .await
        .expect("newer-generation committed PUT should apply after status RV advance");
    assert!(
        result.error_message.is_none(),
        "newer-generation committed PUT must not fail raft apply: {result:?}"
    );

    let row = db
        .get_resource("apps/v1", "Deployment", Some("default"), "web")
        .await
        .unwrap()
        .expect("deployment remains after newer-generation committed put");
    assert_eq!(
        row.data.pointer("/spec/replicas"),
        Some(&serde_json::json!(30)),
        "newer-generation stale-precondition committed PUT must apply the scale update"
    );
    assert_eq!(
        row.data.pointer("/metadata/generation"),
        Some(&serde_json::json!(3)),
        "newer-generation stale-precondition committed PUT must publish the new generation"
    );
}

#[tokio::test]
async fn stale_same_uid_pod_put_after_status_rv_advance_preserves_live_status() {
    let db = crate::datastore::test_support::in_memory().await;
    let created = db
        .create_resource(
            "v1",
            "Pod",
            Some("sonobuoy"),
            "sonobuoy",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "sonobuoy",
                    "namespace": "sonobuoy",
                    "uid": "sonobuoy-uid",
                    "generation": 1
                },
                "spec": {
                    "nodeName": "mn-controlplane2",
                    "containers": [{
                        "name": "kube-sonobuoy",
                        "image": "sonobuoy/sonobuoy:v0.57.5"
                    }]
                },
                "status": {
                    "phase": "Pending",
                    "containerStatuses": [{
                        "name": "kube-sonobuoy",
                        "containerID": "containerd://ctr-sonobuoy",
                        "ready": false,
                        "started": false,
                        "restartCount": 0,
                        "state": {"waiting": {"reason": "ContainerCreating"}}
                    }]
                }
            }),
        )
        .await
        .unwrap();

    let running = db
        .update_status_only_with_preconditions(
            "v1",
            "Pod",
            Some("sonobuoy"),
            "sonobuoy",
            serde_json::json!({
                "phase": "Running",
                "conditions": [
                    {"type": "PodScheduled", "status": "True"},
                    {"type": "Initialized", "status": "True"},
                    {"type": "ContainersReady", "status": "True"},
                    {"type": "Ready", "status": "True"}
                ],
                "containerStatuses": [{
                    "name": "kube-sonobuoy",
                    "containerID": "containerd://ctr-sonobuoy",
                    "ready": true,
                    "started": true,
                    "restartCount": 0,
                    "state": {"running": {"startedAt": "2026-07-05T09:32:10Z"}}
                }]
            }),
            crate::datastore::ResourcePreconditions::uid(created.uid.clone()),
        )
        .await
        .expect("kubelet status update should advance rv before stale put apply");
    assert!(
        running.resource_version > created.resource_version,
        "status update must create the stale-precondition overlap"
    );

    let result = db
        .apply_raft_log_apply_commit(crate::datastore::test_support::test_live_commit(
            60,
            vec![LogApplyMutation::PutResource(LogApplyResourceRow {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: Some("sonobuoy".to_string()),
                name: "sonobuoy".to_string(),
                uid: "sonobuoy-uid".to_string(),
                resource_version: 60,
                data: serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "name": "sonobuoy",
                        "namespace": "sonobuoy",
                        "uid": "sonobuoy-uid",
                        "generation": 1,
                        "resourceVersion": "60",
                        "annotations": {
                            "sonobuoy.hept.io/status": "{\"status\":\"running\"}"
                        }
                    },
                    "spec": {
                        "nodeName": "mn-controlplane2",
                        "containers": [{
                            "name": "kube-sonobuoy",
                            "image": "sonobuoy/sonobuoy:v0.57.5"
                        }]
                    },
                    "status": {
                        "phase": "Pending",
                        "containerStatuses": [{
                            "name": "kube-sonobuoy",
                            "containerID": "containerd://ctr-sonobuoy",
                            "ready": false,
                            "started": false,
                            "restartCount": 0,
                            "state": {"waiting": {"reason": "ContainerCreating"}}
                        }]
                    }
                }),
                require_absent: false,
                require_existing: true,
                precondition_uid: Some("sonobuoy-uid".to_string()),
                precondition_resource_version: Some(created.resource_version),
                status_only: false,
            })],
        ))
        .await
        .expect("stale committed Pod PUT should apply by rebasing status");
    assert!(
        result.error_message.is_some(),
        "stale committed Pod PUT must fail strict RV validation: {result:?}"
    );

    let row = db
        .get_resource("v1", "Pod", Some("sonobuoy"), "sonobuoy")
        .await
        .unwrap()
        .expect("pod remains after stale committed put");
    assert_eq!(
        row.data
            .pointer("/metadata/annotations/sonobuoy.hept.io~1status"),
        None,
        "rejected stale Pod PUT must not apply metadata"
    );
    assert_eq!(
        row.data.pointer("/status/phase"),
        Some(&serde_json::json!("Running")),
        "stale same-UID Pod PUT must preserve the newer kubelet-owned phase"
    );
    assert!(
        row.data
            .pointer("/status/containerStatuses/0/state/running")
            .is_some(),
        "stale same-UID Pod PUT must not regress a running container to ContainerCreating"
    );
    assert_eq!(
        row.data.pointer("/status/containerStatuses/0/ready"),
        Some(&serde_json::json!(true)),
        "stale same-UID Pod PUT must preserve container readiness"
    );
}

#[tokio::test]
async fn committed_pod_put_preserves_existing_deletion_metadata_for_same_uid() {
    let db = crate::datastore::test_support::in_memory().await;
    db.create_resource(
        "v1",
        "Pod",
        Some("gc-2688"),
        "simpletest-01798",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "simpletest-01798",
                "namespace": "gc-2688",
                "uid": "pod-uid"
            },
            "spec": {
                "containers": [{"name": "pause", "image": "registry.k8s.io/pause:3.10"}]
            },
            "status": {
                "phase": "Pending",
                "conditions": [
                    {"type": "Ready", "status": "False"},
                    {"type": "ContainersReady", "status": "False"}
                ]
            }
        }),
    )
    .await
    .unwrap();
    let existing = db
        .get_resource("v1", "Pod", Some("gc-2688"), "simpletest-01798")
        .await
        .unwrap()
        .expect("pod exists before delete mark");
    let mut deleting = (*existing.data).clone();
    deleting["metadata"]["deletionTimestamp"] = serde_json::json!("2026-06-25T02:25:51Z");
    deleting["metadata"]["deletionGracePeriodSeconds"] = serde_json::json!(30);
    db.update_resource(
        "v1",
        "Pod",
        Some("gc-2688"),
        "simpletest-01798",
        deleting,
        existing.resource_version,
    )
    .await
    .unwrap();

    let result = db
        .apply_raft_log_apply_commit(crate::datastore::test_support::test_live_commit(
            60,
            vec![LogApplyMutation::PutResource(LogApplyResourceRow {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: Some("gc-2688".to_string()),
                name: "simpletest-01798".to_string(),
                uid: "pod-uid".to_string(),
                resource_version: 60,
                data: serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "name": "simpletest-01798",
                        "namespace": "gc-2688",
                        "uid": "pod-uid",
                        "resourceVersion": "60"
                    },
                    "spec": {
                        "nodeName": "mn-worker",
                        "containers": [{"name": "pause", "image": "registry.k8s.io/pause:3.10"}]
                    },
                    "status": {
                        "phase": "Running",
                        "conditions": [
                            {"type": "PodScheduled", "status": "True"},
                            {"type": "Ready", "status": "True"},
                            {"type": "ContainersReady", "status": "True"}
                        ],
                        "containerStatuses": [{"name": "pause", "ready": true}]
                    }
                }),
                require_absent: false,
                require_existing: false,
                precondition_uid: Some("pod-uid".to_string()),
                precondition_resource_version: Some(existing.resource_version + 1),
                status_only: false,
            })],
        ))
        .await
        .expect("raft put applies");
    assert!(
        result.error_message.is_none(),
        "committed Pod PUT must succeed: {result:?}"
    );

    let row = db
        .get_resource("v1", "Pod", Some("gc-2688"), "simpletest-01798")
        .await
        .unwrap()
        .expect("pod remains after committed put");
    assert_eq!(
        row.data
            .pointer("/metadata/deletionTimestamp")
            .and_then(|v| v.as_str()),
        Some("2026-06-25T02:25:51Z"),
        "same-UID Pod PUT must not erase a live deletionTimestamp"
    );
    assert_eq!(
        row.data
            .pointer("/metadata/deletionGracePeriodSeconds")
            .and_then(|v| v.as_i64()),
        Some(30),
        "same-UID Pod PUT must not erase deletion grace"
    );
    let ready = row
        .data
        .pointer("/status/conditions")
        .and_then(|v| v.as_array())
        .and_then(|conditions| {
            conditions.iter().find(|condition| {
                condition.pointer("/type").and_then(|v| v.as_str()) == Some("Ready")
            })
        })
        .expect("Ready condition present");
    assert_eq!(
        ready.pointer("/status").and_then(|v| v.as_str()),
        Some("False"),
        "terminating Pod must stay unready after stale committed PUT"
    );
    assert_eq!(
        row.data
            .pointer("/status/containerStatuses/0/ready")
            .and_then(|v| v.as_bool()),
        Some(false),
        "terminating Pod container readiness must stay false"
    );
}

#[tokio::test]
async fn committed_pod_put_clears_finalizers_when_stale_rv_put_drain_was_committed() {
    let db = crate::datastore::test_support::in_memory().await;
    db.create_resource(
        "v1",
        "Pod",
        Some("nsdeletetest"),
        "test-pod",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "test-pod",
                "namespace": "nsdeletetest",
                "uid": "pod-uid",
                "finalizers": ["e2e.example.com/finalizer"]
            },
            "spec": {
                "nodeName": "mn-replica",
                "containers": [{"name": "nginx", "image": "registry.k8s.io/pause:3.10.1"}]
            },
            "status": {
                "phase": "Running",
                "conditions": [
                    {"type": "PodScheduled", "status": "True"},
                    {"type": "Ready", "status": "True"},
                    {"type": "ContainersReady", "status": "True"}
                ],
                "containerStatuses": [{"name": "nginx", "ready": true}]
            }
        }),
    )
    .await
    .unwrap();
    let observed_before_delete = db
        .get_resource("v1", "Pod", Some("nsdeletetest"), "test-pod")
        .await
        .unwrap()
        .expect("pod exists before namespace deletion mark");
    let mut deleting = (*observed_before_delete.data).clone();
    deleting["metadata"]["deletionTimestamp"] = serde_json::json!("2026-07-02T08:15:35Z");
    deleting["metadata"]["deletionGracePeriodSeconds"] = serde_json::json!(0);
    db.update_resource(
        "v1",
        "Pod",
        Some("nsdeletetest"),
        "test-pod",
        deleting,
        observed_before_delete.resource_version,
    )
    .await
    .unwrap();

    let result = db
            .apply_raft_log_apply_commit(crate::datastore::test_support::test_live_commit(
                72,
                vec![LogApplyMutation::PutResource(LogApplyResourceRow {
                    api_version: "v1".to_string(),
                    kind: "Pod".to_string(),
                    namespace: Some("nsdeletetest".to_string()),
                    name: "test-pod".to_string(),
                    uid: "pod-uid".to_string(),
                    resource_version: 72,
                    data: serde_json::json!({
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "metadata": {
                            "name": "test-pod",
                            "namespace": "nsdeletetest",
                            "uid": "pod-uid",
                            "resourceVersion": "72",
                            "deletionTimestamp": "2026-07-02T08:15:35Z",
                            "deletionGracePeriodSeconds": 0
                        },
                        "spec": {
                            "nodeName": "mn-replica",
                            "containers": [{"name": "nginx", "image": "registry.k8s.io/pause:3.10.1"}]
                        },
                        "status": {
                            "phase": "Running",
                            "conditions": [
                                {"type": "PodScheduled", "status": "True"},
                                {"type": "Ready", "status": "False", "reason": "PodTerminating"},
                                {"type": "ContainersReady", "status": "False", "reason": "PodTerminating"}
                            ],
                            "containerStatuses": [{"name": "nginx", "ready": false}]
                        }
                    }),
                    require_absent: false,
                    require_existing: false,
                    precondition_uid: Some("pod-uid".to_string()),
                    precondition_resource_version: Some(
                        observed_before_delete.resource_version + 1,
                    ),
                    status_only: false,
                })],
            ))
            .await
            .expect("raft put applies");
    assert!(
        result.error_message.is_none(),
        "committed Pod PUT must succeed: {result:?}"
    );

    let row = db
        .get_resource("v1", "Pod", Some("nsdeletetest"), "test-pod")
        .await
        .unwrap()
        .expect("pod remains until actor-owned finalization removes it");
    assert!(
        row.data
            .pointer("/metadata/finalizers")
            .and_then(|value| value.as_array())
            .is_none_or(|finalizers| finalizers.is_empty()),
        "committed stale Pod PUT that drains finalizers must not merge old finalizers back: {:?}",
        row.data.pointer("/metadata/finalizers")
    );
    assert_eq!(
        row.data
            .pointer("/metadata/deletionTimestamp")
            .and_then(|v| v.as_str()),
        Some("2026-07-02T08:15:35Z"),
        "same-UID Pod PUT must still preserve deletionTimestamp"
    );
    assert_eq!(
        row.data
            .pointer("/metadata/deletionGracePeriodSeconds")
            .and_then(|v| v.as_i64()),
        Some(0),
        "same-UID Pod PUT must still preserve deletion grace"
    );
}

#[tokio::test]
async fn committed_put_preserves_existing_deletion_metadata_for_non_pod_same_uid() {
    let db = crate::datastore::test_support::in_memory().await;
    db.create_resource(
        "apps/v1",
        "Deployment",
        Some("default"),
        "terminating-deploy",
        serde_json::json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": "terminating-deploy",
                "namespace": "default",
                "uid": "deploy-uid",
                "generation": 1
            },
            "spec": {"replicas": 1},
            "status": {"availableReplicas": 1}
        }),
    )
    .await
    .unwrap();
    let existing = db
        .get_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "terminating-deploy",
        )
        .await
        .unwrap()
        .expect("deployment exists before delete mark");
    let mut deleting = (*existing.data).clone();
    deleting["metadata"]["deletionTimestamp"] = serde_json::json!("2026-06-25T02:35:00Z");
    deleting["metadata"]["deletionGracePeriodSeconds"] = serde_json::json!(30);
    deleting["metadata"]["finalizers"] = serde_json::json!(["example.com/protect"]);
    db.update_resource(
        "apps/v1",
        "Deployment",
        Some("default"),
        "terminating-deploy",
        deleting,
        existing.resource_version,
    )
    .await
    .unwrap();

    let result = db
        .apply_raft_log_apply_commit(crate::datastore::test_support::test_live_commit(
            61,
            vec![LogApplyMutation::PutResource(LogApplyResourceRow {
                api_version: "apps/v1".to_string(),
                kind: "Deployment".to_string(),
                namespace: Some("default".to_string()),
                name: "terminating-deploy".to_string(),
                uid: "deploy-uid".to_string(),
                resource_version: 61,
                data: serde_json::json!({
                    "apiVersion": "apps/v1",
                    "kind": "Deployment",
                    "metadata": {
                        "name": "terminating-deploy",
                        "namespace": "default",
                        "uid": "deploy-uid",
                        "resourceVersion": "61",
                        "generation": 2
                    },
                    "spec": {"replicas": 2},
                    "status": {"availableReplicas": 1}
                }),
                require_absent: false,
                require_existing: false,
                precondition_uid: Some("deploy-uid".to_string()),
                precondition_resource_version: Some(existing.resource_version),
                status_only: false,
            })],
        ))
        .await
        .expect("raft put applies");
    assert!(
        result.error_message.is_some(),
        "stale committed Deployment PUT must fail strict RV validation: {result:?}"
    );

    let row = db
        .get_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "terminating-deploy",
        )
        .await
        .unwrap()
        .expect("deployment remains after committed put");
    assert_eq!(
        row.data
            .pointer("/metadata/deletionTimestamp")
            .and_then(|v| v.as_str()),
        Some("2026-06-25T02:35:00Z"),
        "same-UID non-Pod PUT must not erase a live deletionTimestamp"
    );
    assert_eq!(
        row.data
            .pointer("/metadata/deletionGracePeriodSeconds")
            .and_then(|v| v.as_i64()),
        Some(30),
        "same-UID non-Pod PUT must not erase deletion grace"
    );
    assert_eq!(
        row.data
            .pointer("/metadata/finalizers/0")
            .and_then(|v| v.as_str()),
        Some("example.com/protect"),
        "stale same-UID non-Pod PUT must not erase live finalizers"
    );
    assert_eq!(
        row.data.pointer("/spec/replicas"),
        Some(&serde_json::json!(1))
    );
}

/// Committed patch must apply to the current local state regardless of precondition mismatch,
/// reconciling the follower toward the committed result before last_applied advances.
#[tokio::test]
async fn committed_patch_conflict_reconciles_to_committed_value_before_advancing() {
    let db = crate::datastore::test_support::in_memory().await;
    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "patch-target",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "patch-target", "namespace": "default", "uid": "cm-patch"},
            "data": {"existing": "yes"}
        }),
    )
    .await
    .unwrap();
    // Committed patch has precondition_rv=888 (leader rv). Follower has rv=1. The patch
    // must still be applied to the current local state.
    let result = db
        .apply_raft_log_apply_commit(crate::datastore::test_support::test_live_commit(
            70,
            vec![LogApplyMutation::PatchResourceLatest(
                LogApplyResourcePatch {
                    api_version: "v1".to_string(),
                    kind: "ConfigMap".to_string(),
                    namespace: Some("default".to_string()),
                    name: "patch-target".to_string(),
                    resource_version: 70,
                    patch_kind: klights_cluster_core::PatchKind::Merge,
                    patch: serde_json::json!({"data": {"added": "by-patch"}}),
                    precondition_uid: Some("cm-patch".to_string()),
                    precondition_resource_version: Some(1),
                    require_existing: true,
                    terminating_pod_unready_timestamp: None,
                },
            )],
        ))
        .await
        .expect("raft patch must not error on stale rv");
    assert!(
        result.error_message.is_none(),
        "committed PATCH must succeed: {result:?}"
    );
    let row = db
        .get_resource("v1", "ConfigMap", Some("default"), "patch-target")
        .await
        .unwrap()
        .expect("committed PATCH must preserve the row");
    assert_eq!(
        row.data.pointer("/data/added").and_then(|v| v.as_str()),
        Some("by-patch"),
        "patch field must be present after committed apply"
    );
}

/// A committed patch conflict must NOT advance last_applied while leaving the local state
/// divergent — the follower must reconcile before the index is recorded as applied.
///
/// This test exercises the `apply_commit_in_tx_for_raft` path and verifies that the returned
/// result does not carry `error_message` (the old "conflict swallowed, last_applied advanced"
/// signal), which is the prior buggy behavior.
#[tokio::test]
async fn committed_patch_conflict_does_not_advance_applied_index_with_divergence() {
    let db = crate::datastore::test_support::in_memory().await;
    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "diverge-cm",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "diverge-cm", "namespace": "default", "uid": "cm-div"},
            "data": {"state": "stale"}
        }),
    )
    .await
    .unwrap();
    // Committed patch with precondition_rv mismatch. With the fix the apply succeeds
    // (no error_message). Without the fix the conflict is swallowed with error_message set,
    // which is the "advanced index with divergence" bug.
    let result = db
        .apply_raft_log_apply_commit(crate::datastore::test_support::test_live_commit(
            75,
            vec![LogApplyMutation::PatchResourceLatest(
                LogApplyResourcePatch {
                    api_version: "v1".to_string(),
                    kind: "ConfigMap".to_string(),
                    namespace: Some("default".to_string()),
                    name: "diverge-cm".to_string(),
                    resource_version: 75,
                    patch_kind: klights_cluster_core::PatchKind::Merge,
                    patch: serde_json::json!({"data": {"state": "reconciled"}}),
                    precondition_uid: Some("cm-div".to_string()),
                    precondition_resource_version: Some(1),
                    require_existing: true,
                    terminating_pod_unready_timestamp: None,
                },
            )],
        ))
        .await
        .expect("raft patch must succeed even with stale precondition");
    assert!(
        result.error_message.is_none(),
        "committed patch must reconcile state rather than swallow conflict: got error_message={:?}",
        result.error_message
    );
    assert!(
        result.applied_rv.is_some(),
        "applied_rv must be set after successful reconcile"
    );
}

/// Re-applying an already-committed entry (local state already equals the committed state)
/// must advance last_applied silently without emitting a conflict.
#[tokio::test]
async fn idempotent_reapply_of_already_committed_state_advances_silently() {
    let db = crate::datastore::test_support::in_memory().await;
    // Apply a committed PUT once to establish local state.
    let first = db
        .apply_raft_log_apply_commit(crate::datastore::test_support::test_live_commit(
            80,
            vec![LogApplyMutation::PutResource(LogApplyResourceRow {
                api_version: "v1".to_string(),
                kind: "ConfigMap".to_string(),
                namespace: Some("default".to_string()),
                name: "idempotent-cm".to_string(),
                uid: "cm-idem".to_string(),
                resource_version: 80,
                data: serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {
                        "name": "idempotent-cm",
                        "namespace": "default",
                        "uid": "cm-idem",
                        "resourceVersion": "80"
                    },
                    "data": {"v": "1"}
                }),
                require_absent: false,
                require_existing: false,
                precondition_uid: None,
                precondition_resource_version: None,
                status_only: false,
            })],
        ))
        .await
        .unwrap();
    assert!(
        first.error_message.is_none(),
        "first apply must succeed: {first:?}"
    );

    // Re-apply the identical commit (simulating restart or redundant delivery).
    let second = db
        .apply_raft_log_apply_commit(crate::datastore::test_support::test_live_commit(
            80,
            vec![LogApplyMutation::PutResource(LogApplyResourceRow {
                api_version: "v1".to_string(),
                kind: "ConfigMap".to_string(),
                namespace: Some("default".to_string()),
                name: "idempotent-cm".to_string(),
                uid: "cm-idem".to_string(),
                resource_version: 80,
                data: serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {
                        "name": "idempotent-cm",
                        "namespace": "default",
                        "uid": "cm-idem",
                        "resourceVersion": "80"
                    },
                    "data": {"v": "1"}
                }),
                require_absent: false,
                require_existing: false,
                precondition_uid: None,
                precondition_resource_version: None,
                status_only: false,
            })],
        ))
        .await
        .unwrap();
    assert!(
        second.error_message.is_none(),
        "idempotent re-apply must not set error_message: {second:?}"
    );
}

#[tokio::test]
async fn stale_raft_status_only_apply_preserves_newer_job_custom_condition_timestamp() {
    let db = crate::datastore::test_support::in_memory().await;
    let created = db
        .create_resource(
            "batch/v1",
            "Job",
            Some("default"),
            "condition-job",
            serde_json::json!({
                "apiVersion": "batch/v1",
                "kind": "Job",
                "metadata": {
                    "name": "condition-job",
                    "namespace": "default",
                    "uid": "job-condition-uid"
                },
                "spec": {
                    "parallelism": 2,
                    "completions": 4,
                    "template": {
                        "spec": {
                            "restartPolicy": "Never",
                            "containers": [{"name": "main", "image": "busybox"}]
                        }
                    }
                },
                "status": {
                    "active": 2,
                    "ready": 2,
                    "succeeded": 0,
                    "failed": 0,
                    "conditions": [{
                        "type": "CustomConditionType",
                        "status": "True",
                        "lastTransitionTime": "2026-06-30T18:14:03Z"
                    }],
                    "startTime": "2026-06-30T18:14:00Z",
                    "terminating": 0
                }
            }),
        )
        .await
        .unwrap();

    db.update_status_only_with_preconditions(
        "batch/v1",
        "Job",
        Some("default"),
        "condition-job",
        serde_json::json!({
            "active": 2,
            "ready": 2,
            "succeeded": 0,
            "failed": 0,
            "conditions": [{
                "type": "CustomConditionType",
                "status": "True",
                "lastTransitionTime": "2026-06-30T18:14:07Z"
            }],
            "startTime": "2026-06-30T18:14:00Z",
            "terminating": 0
        }),
        crate::datastore::ResourcePreconditions::from_resource(&created),
    )
    .await
    .unwrap();

    db.apply_raft_log_apply_commit(crate::datastore::test_support::test_live_commit(
        30,
        vec![LogApplyMutation::PutResource(LogApplyResourceRow {
            api_version: "batch/v1".to_string(),
            kind: "Job".to_string(),
            namespace: Some("default".to_string()),
            name: "condition-job".to_string(),
            uid: "job-condition-uid".to_string(),
            resource_version: 30,
            data: serde_json::json!({
                "apiVersion": "batch/v1",
                "kind": "Job",
                "metadata": {
                    "name": "condition-job",
                    "namespace": "default",
                    "uid": "job-condition-uid",
                    "resourceVersion": "30"
                },
                "spec": {
                    "parallelism": 2,
                    "completions": 4,
                    "template": {
                        "spec": {
                            "restartPolicy": "Never",
                            "containers": [{"name": "main", "image": "busybox"}]
                        }
                    }
                },
                "status": {
                    "active": 2,
                    "ready": 2,
                    "succeeded": 0,
                    "failed": 0,
                    "conditions": [{
                        "type": "CustomConditionType",
                        "status": "True",
                        "lastTransitionTime": "2026-06-30T18:14:03Z"
                    }],
                    "startTime": "2026-06-30T18:14:00Z",
                    "terminating": 0
                }
            }),
            require_absent: false,
            require_existing: true,
            precondition_uid: Some("job-condition-uid".to_string()),
            precondition_resource_version: Some(created.resource_version),
            status_only: true,
        })],
    ))
    .await
    .unwrap();

    let stored = db
        .get_resource("batch/v1", "Job", Some("default"), "condition-job")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        stored
            .data
            .pointer("/status/conditions/0/lastTransitionTime")
            .and_then(|value| value.as_str()),
        Some("2026-06-30T18:14:07Z"),
        "stale raft status-only apply must not roll back a newer user-updated Job condition"
    );
}

/// A follower that holds a stale row (the leader already deleted it via a committed log entry)
/// must converge to the leader fingerprint (row absent) without requiring a snapshot install.
#[tokio::test]
async fn follower_converges_to_leader_fingerprint_without_snapshot_after_stale_delete() {
    // Simulate a follower that holds a row the leader committed as deleted.
    let follower = crate::datastore::test_support::in_memory().await;
    follower
        .create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "conv-cm",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "name": "conv-cm",
                    "namespace": "default",
                    "uid": "cm-conv"
                }
            }),
        )
        .await
        .unwrap();

    // Leader backend has the resource deleted (empty). Committed delete arrives via log.
    let result = follower
        .apply_raft_log_apply_commit(crate::datastore::test_support::test_live_commit(
            100,
            vec![LogApplyMutation::DeleteResource(LogApplyResourceKey {
                api_version: "v1".to_string(),
                kind: "ConfigMap".to_string(),
                namespace: Some("default".to_string()),
                name: "conv-cm".to_string(),
                uid: "cm-conv".to_string(),
                precondition_resource_version: None, // no precondition — pure committed delete
            })],
        ))
        .await
        .expect("committed log delete must not fail");
    assert!(
        result.error_message.is_none(),
        "no error_message after authoritative delete"
    );

    // Follower now matches leader fingerprint: row absent.
    let row = follower
        .get_resource("v1", "ConfigMap", Some("default"), "conv-cm")
        .await
        .unwrap();
    assert!(
        row.is_none(),
        "follower must converge to leader fingerprint without snapshot; row still present"
    );
}

/// Applying the same committed entry encoded as JSON and as protobuf must produce identical
/// cluster.db rows (api_version, kind, namespace, name, uid, rv, data).
#[tokio::test]
async fn committed_apply_json_and_protobuf_paths_produce_identical_rows() {
    let commit = crate::datastore::test_support::test_live_commit(
        110,
        vec![LogApplyMutation::PutResource(LogApplyResourceRow {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace: Some("default".to_string()),
            name: "encoding-cm".to_string(),
            uid: "cm-enc".to_string(),
            resource_version: 110,
            data: serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "name": "encoding-cm",
                    "namespace": "default",
                    "uid": "cm-enc",
                    "resourceVersion": "110"
                },
                "data": {"encoded": "true"}
            }),
            require_absent: false,
            require_existing: false,
            precondition_uid: None,
            precondition_resource_version: None,
            status_only: false,
        })],
    );

    // JSON path
    let json_bytes = crate::replication::log_apply_wire::encode_commit_json(&commit).unwrap();
    let commit_from_json =
        crate::replication::log_apply_wire::decode_commit_json(&json_bytes).unwrap();
    let db_json = crate::datastore::test_support::in_memory().await;
    db_json
        .apply_raft_log_apply_commit(commit_from_json)
        .await
        .unwrap();
    let row_json = db_json
        .get_resource("v1", "ConfigMap", Some("default"), "encoding-cm")
        .await
        .unwrap()
        .expect("json path must materialise row");

    // Protobuf path
    let proto_bytes = crate::replication::log_apply_wire::encode_commit_protobuf(&commit).unwrap();
    let commit_from_proto =
        crate::replication::log_apply_wire::decode_commit_protobuf(&proto_bytes).unwrap();
    let db_proto = crate::datastore::test_support::in_memory().await;
    db_proto
        .apply_raft_log_apply_commit(commit_from_proto)
        .await
        .unwrap();
    let row_proto = db_proto
        .get_resource("v1", "ConfigMap", Some("default"), "encoding-cm")
        .await
        .unwrap()
        .expect("proto path must materialise row");

    assert_eq!(
        row_json.uid, row_proto.uid,
        "JSON and protobuf paths must produce identical uid"
    );
    assert_eq!(
        row_json.resource_version, row_proto.resource_version,
        "JSON and protobuf paths must produce identical rv"
    );
    assert_eq!(
        row_json.data, row_proto.data,
        "JSON and protobuf paths must produce identical data"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_caller_after_commit_still_publishes_and_retry_recovers_receipt() {
    let db = crate::datastore::test_support::in_memory().await;
    enable_committed_apply_v1(&db).await;
    let mut watch = db.subscribe_watch_signals(klights_watch::WatchTopic::new("v1", "ConfigMap"));
    let key = "cancel-after-commit";
    let commit = committed_apply_v1(crate::datastore::test_support::test_live_commit(
        0,
        vec![
            v1_resource("cancelled-apply", "cancelled-uid"),
            LogApplyMutation::PutAppliedOutbox(LogApplyAppliedOutboxRow {
                idempotency_key: key.to_string(),
                subject_key: "v1/ConfigMap/default/cancelled-apply/cancelled-uid".to_string(),
                operation: "Create".to_string(),
                first_seen_ms: 1,
                applied_rv: None,
                result_proto: crate::datastore::sqlite::outbox_codec::encode(
                    &klights_cluster_core::command::StorageResponse::Ack {
                        resource_version: 0,
                    },
                )
                .unwrap(),
                status_stamp: None,
            }),
        ],
    ));
    let pause = db.install_post_commit_publish_pause();
    let task_db = db.clone();
    let task_commit = commit.clone();
    let task = tokio::spawn(async move { task_db.apply_raft_log_apply_commit(task_commit).await });
    pause.reached.notified().await;
    task.abort();
    pause.resume();
    pause.published.notified().await;

    let stored = db
        .get_resource("v1", "ConfigMap", Some("default"), "cancelled-apply")
        .await
        .unwrap()
        .expect("commit survived caller cancellation");
    let committed_position = db.current_watch_replay_position().await.unwrap();
    assert!(
        watch
            .recv()
            .await
            .unwrap()
            .advances
            .iter()
            .any(|advance| advance.high_rv == stored.resource_version)
    );

    let receipt = db.apply_raft_log_apply_commit(commit).await.unwrap();
    assert_eq!(receipt.applied_rv, Some(stored.resource_version));
    assert!(receipt.error_message.is_none());
    assert_eq!(
        db.current_watch_replay_position().await.unwrap(),
        committed_position
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_snapshot_restore_after_commit_still_publishes_and_is_retryable() {
    let db = crate::datastore::test_support::in_memory().await;
    let mut watch = db.subscribe_watch_signals(klights_watch::WatchTopic::new("v1", "ConfigMap"));
    let restored = serde_json::json!({
        "metadata": {
            "name": "restored-after-cancel",
            "namespace": "default",
            "uid": "restore-uid",
            "resourceVersion": "5"
        }
    });
    let commit = snapshot_operation(
        5,
        None,
        vec![
            LogApplyMutation::PutResource(LogApplyResourceRow {
                api_version: "v1".into(),
                kind: "ConfigMap".into(),
                namespace: Some("default".into()),
                name: "restored-after-cancel".into(),
                uid: "restore-uid".into(),
                resource_version: 5,
                data: restored.clone(),
                require_absent: true,
                require_existing: false,
                precondition_uid: None,
                precondition_resource_version: None,
                status_only: false,
            }),
            LogApplyMutation::PutWatchEvent(LogApplyWatchEventRow {
                event_id: Some(1),
                api_version: "v1".into(),
                kind: "ConfigMap".into(),
                namespace: Some("default".into()),
                name: "restored-after-cancel".into(),
                resource_version: 5,
                event_type: "ADDED".into(),
                data: restored,
            }),
        ],
    );
    let pause = db.install_post_commit_publish_pause();
    let task_db = db.clone();
    let task_commit = commit.clone();
    let task = tokio::spawn(async move {
        task_db
            .replace_replicated_resource_state(vec![task_commit], 5, None, None, None)
            .await
    });
    pause.reached.notified().await;
    task.abort();
    pause.resume();
    pause.published.notified().await;
    assert!(
        watch
            .recv()
            .await
            .unwrap()
            .advances
            .iter()
            .any(|advance| advance.high_rv == 5)
    );
    assert!(
        db.get_resource("v1", "ConfigMap", Some("default"), "restored-after-cancel")
            .await
            .unwrap()
            .is_some()
    );
    db.replace_replicated_resource_state(vec![commit], 5, None, None, None)
        .await
        .unwrap();
    assert_eq!(db.get_current_resource_version().await.unwrap(), 5);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn post_commit_pause_is_scoped_to_its_datastore_instance() {
    let paused_db = crate::datastore::test_support::in_memory().await;
    let independent_db = crate::datastore::test_support::in_memory().await;
    let pause = paused_db.install_post_commit_publish_pause();

    let independent_commit =
        snapshot_operation(1, None, vec![v1_resource("independent", "independent-uid")]);
    let independent_task = tokio::spawn(async move {
        independent_db
            .replace_replicated_resource_state(vec![independent_commit], 1, None, None, None)
            .await
    });
    tokio::pin!(independent_task);
    tokio::select! {
        result = &mut independent_task => {
            result.unwrap().unwrap();
        }
        () = pause.reached.notified() => {
            pause.resume();
            independent_task.await.unwrap().unwrap();
            panic!("another datastore instance intercepted the post-commit pause");
        }
    }

    let paused_commit = snapshot_operation(1, None, vec![v1_resource("paused", "paused-uid")]);
    let task_db = paused_db.clone();
    let paused_task = tokio::spawn(async move {
        task_db
            .replace_replicated_resource_state(vec![paused_commit], 1, None, None, None)
            .await
    });
    pause.reached.notified().await;
    paused_task.abort();
    pause.resume();
    pause.published.notified().await;
    assert!(
        paused_db
            .get_resource("v1", "ConfigMap", Some("default"), "paused")
            .await
            .unwrap()
            .is_some()
    );
}
