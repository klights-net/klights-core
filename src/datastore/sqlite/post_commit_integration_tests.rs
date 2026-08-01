//! Root SQLite commit-to-active-watch cancellation integration.

use klights_cluster_core::{
    LogApplyAppliedOutboxRow, LogApplyCommit, LogApplyMutation, LogApplyResourceRow,
    LogApplyWatchEventRow, OutboxStreamWatermark, SnapshotRestoreOperation,
};

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
        data: serde_json::json!({
            "metadata": {"name": name, "namespace": "default", "uid": uid}
        }),
        require_absent: true,
        require_existing: false,
        precondition_uid: None,
        precondition_resource_version: None,
        status_only: false,
    })
}

fn snapshot_operation(
    resource_version: i64,
    outbox_watermark: Option<OutboxStreamWatermark>,
    mut mutations: Vec<LogApplyMutation>,
) -> SnapshotRestoreOperation {
    for mutation in &mut mutations {
        match mutation {
            LogApplyMutation::PutResource(row) => row.resource_version = resource_version,
            LogApplyMutation::PatchResourceLatest(row) => row.resource_version = resource_version,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_caller_after_commit_still_publishes_and_retry_recovers_receipt() {
    let _ = super::Datastore::apply_outbox_command_in_tx;
    let db = crate::datastore::test_support::in_memory().await;
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
                    &klights_cluster_core::StorageResponse::Ack {
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
