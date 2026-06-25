pub mod flow_control;
pub mod grpc_network;
pub mod log_storage;
pub mod membership_client;
pub mod network;
pub mod node;
pub mod rtt_estimator;
pub mod snapshot;
pub mod state_machine;
pub mod state_machine_impl;
pub mod types;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;

    use crate::datastore::raft::state_machine::N1Raft;
    use crate::kubelet::outbox::payload::OutboxOperation;

    fn pod_status_payload(ns: &str, name: &str, uid: &str) -> Bytes {
        crate::kubelet::outbox::payload::OutboxPayload::from_command(
            crate::datastore::command::StorageCommand::UpdateStatus {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: Some(ns.to_string()),
                name: name.to_string(),
                status: serde_json::json!({"phase": "Running"}),
                expected_rv: None,
                preconditions: crate::datastore::ResourcePreconditions {
                    uid: Some(uid.to_string()),
                    resource_version: None,
                },
                observed_status_stamp: None,
            },
        )
        .encode_protobuf()
        .expect("encode payload")
        .into()
    }

    #[tokio::test]
    async fn n1_cluster_self_commits_writes() {
        let db = Arc::new(crate::datastore::test_support::in_memory().await);
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "raft-pod",
            serde_json::json!({
                "metadata": {"name": "raft-pod", "namespace": "default", "uid": "uid-raft"}
            }),
        )
        .await
        .unwrap();
        let raft = N1Raft::new(db.clone());

        let applied = raft
            .propose_outbox(
                "raft-key-1",
                OutboxOperation::PodStatus,
                pod_status_payload("default", "raft-pod", "uid-raft"),
                "node-a",
            )
            .await
            .expect("raft apply");

        assert!(matches!(
            applied.result,
            crate::kubelet::outbox::OutboxApplyResult::Applied { .. }
        ));
        assert!(raft.last_commit_index().await > 0);
    }

    #[tokio::test]
    async fn n1_apply_returns_correct_rv() {
        let db = Arc::new(crate::datastore::test_support::in_memory().await);
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "rv-pod",
            serde_json::json!({
                "metadata": {"name": "rv-pod", "namespace": "default", "uid": "uid-rv"}
            }),
        )
        .await
        .unwrap();
        let raft = N1Raft::new(db);

        let applied = raft
            .propose_outbox(
                "raft-key-rv",
                OutboxOperation::PodStatus,
                pod_status_payload("default", "rv-pod", "uid-rv"),
                "node-a",
            )
            .await
            .expect("raft apply");

        assert_eq!(
            applied.applied_resource_version(),
            Some(raft.last_commit_index().await)
        );
    }

    #[tokio::test]
    async fn watch_event_rv_equals_raft_commit_index() {
        let db = Arc::new(crate::datastore::test_support::in_memory().await);
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "watch-pod",
            serde_json::json!({
                "metadata": {"name": "watch-pod", "namespace": "default", "uid": "uid-watch"}
            }),
        )
        .await
        .unwrap();
        let mut watch_rx = db.subscribe_watch(crate::watch::WatchTopic::new("v1", "Pod"));
        let raft = N1Raft::new(db);

        let applied = raft
            .propose_outbox(
                "raft-key-watch",
                OutboxOperation::PodStatus,
                pod_status_payload("default", "watch-pod", "uid-watch"),
                "node-a",
            )
            .await
            .expect("raft apply");
        let commit_index = raft.last_commit_index().await;

        let event = watch_rx.recv().await.expect("watch event");
        assert_eq!(applied.applied_resource_version(), Some(commit_index));
        assert_eq!(event.resource_version(), Some(commit_index));
    }
}
