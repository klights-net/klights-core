use klights_replication::ReplicationService;
use std::sync::Arc;

use anyhow::Result;
use klights_cluster_core::ReplicationEntry;
use klights_leader_api::{JoinRequest, JoinResponse, NetworkDataplane};
use klights_node_api::{
    FollowerCompletionContext, FollowerControlMessage, NodeExecFrame, NodeExecRequest,
    NodeExecSyncRequest, NodeLogEvent, NodeLogRequest, NodeMetricsError, NodeMetricsRequest,
    NodeMetricsResult, NodeOperationKind, RoutedNodeExecFrame, RoutedNodeLogEvent,
    RoutedNodeMetricsResponse,
};

#[cfg(test)]
mod tests {
    use super::*;
    use klights_cluster_core::command::{
        COMMAND_CODEC_VERSION, CommandId, CommandMeta, StorageCommand,
    };
    use klights_node_api::ExecStreamChannel;
    use klights_supervisor::{TaskCategoryConfig, TaskSupervisor};
    use serde_json::json;

    const EXPECTED_NODE_EXEC_STREAM_FRAME_CAPACITY: usize = 128;

    fn test_network_dataplane(
        node_name: String,
        mode: klights_networking::wireguard::DataplaneMode,
        encryption: klights_networking::wireguard::DataplaneEncryption,
        public_key: Option<String>,
        endpoint: Option<String>,
        port: Option<u16>,
    ) -> Result<NetworkDataplane, klights_leader_api::NetworkTopologyError> {
        NetworkDataplane::try_new(
            node_name,
            match mode {
                klights_networking::wireguard::DataplaneMode::Root => {
                    klights_leader_api::NetworkNodeMode::Root
                }
                klights_networking::wireguard::DataplaneMode::Rootless => {
                    klights_leader_api::NetworkNodeMode::Rootless
                }
            },
            match encryption {
                klights_networking::wireguard::DataplaneEncryption::Enabled => {
                    klights_leader_api::DataplaneEncryption::WireGuard
                }
                klights_networking::wireguard::DataplaneEncryption::Disabled => {
                    klights_leader_api::DataplaneEncryption::Direct
                }
            },
            public_key.as_deref(),
            endpoint
                .as_deref()
                .unwrap_or_default()
                .parse()
                .map_err(|error| {
                    klights_leader_api::NetworkTopologyError::corrupt_response(format!(
                        "invalid test endpoint: {error}"
                    ))
                })?,
            port,
        )
    }

    async fn test_service_with_db() -> (ReplicationService, Arc<crate::datastore::sqlite::Datastore>)
    {
        let db = Arc::new(crate::datastore::test_support::in_memory().await);
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));

        // Initialize cluster metadata (required for join validation)
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();

        let service = crate::grpc_test_support::replication_service_with_metadata(
            db.clone(),
            db.focused_recovery_store(),
            supervisor,
        );
        (service, db)
    }

    async fn test_service() -> ReplicationService {
        test_service_with_db().await.0
    }

    async fn create_scoped_token_for_test(
        db: &dyn crate::datastore::backend::DatastoreBackend,
        token: &str,
        scope: crate::bootstrap::bootstrap_token::BootstrapTokenScope,
    ) {
        crate::bootstrap::bootstrap_token::create_scoped_bootstrap_token_secret_for_test(
            db, scope, token,
        )
        .await
        .unwrap();
    }

    fn sample_node_log_request(
        node_name: &str,
        follow: bool,
        tail_lines: Option<usize>,
    ) -> NodeLogRequest {
        NodeLogRequest::new(
            klights_node_api::NodeLogTarget::try_new(
                node_name,
                "sonobuoy",
                "sonobuoy-e2e-job",
                "pod-uid",
                "e2e",
            )
            .unwrap(),
            klights_node_api::NodeLogOptions::new(
                follow.then(|| "true".to_string()),
                tail_lines,
                None,
                None,
                None,
                None,
                None,
            ),
        )
    }

    fn sample_node_exec_request(node_name: &str) -> NodeExecRequest {
        let target = klights_node_api::NodeExecTarget::try_new(
            node_name,
            "default",
            "test-pod",
            "containerd://abc",
        )
        .unwrap();
        NodeExecRequest::exec(
            target,
            vec!["sh".to_string()],
            klights_node_api::ExecStreamOptions::new(false, true, false, false),
        )
    }

    fn sample_node_exec_sync_request(node_name: &str) -> NodeExecSyncRequest {
        NodeExecSyncRequest::try_new(
            klights_node_api::NodeExecTarget::try_new(
                node_name,
                "default",
                "test-pod",
                "containerd://abc",
            )
            .unwrap(),
            vec!["true".to_string()],
            300,
        )
        .unwrap()
    }

    fn sample_entry(rv: i64) -> ReplicationEntry {
        ReplicationEntry {
            command: StorageCommand::CreateResource {
                api_version: "v1".into(),
                kind: "ConfigMap".into(),
                namespace: Some("default".into()),
                name: "test".into(),
                data: json!({"metadata": {"name": "test"}}),
            },
            meta: CommandMeta {
                command_id: CommandId(format!("replication-service-sample-{rv}")),
                codec_version: COMMAND_CODEC_VERSION,
                resource_version: rv,
                uid: None,
                timestamp_ms: 0,
                authoring_node: "test".into(),
            },
        }
    }

    #[tokio::test]
    async fn service_starts_idle_without_error() {
        let service = test_service().await;
        assert_eq!(service.current_position(), 0);
    }

    #[tokio::test]
    async fn notify_entry_updates_position() {
        let service = test_service().await;
        service.notify_entry(sample_entry(42));
        assert_eq!(service.current_position(), 42);
    }

    #[tokio::test]
    async fn subscribe_receives_entries() {
        let service = test_service().await;
        let mut rx = service.subscribe_entries();

        service.notify_entry(sample_entry(1));

        // Watch channel should have the latest value
        assert!(rx.changed().await.is_ok());
        let entry = rx.borrow().clone();
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().meta.resource_version, 1);
    }

    #[tokio::test]
    async fn stream_subscription_receives_every_entry() {
        let service = test_service().await;
        let mut rx = service.subscribe_stream_entries();

        service.notify_entry(sample_entry(1));
        service.notify_entry(sample_entry(2));

        let first = rx.recv().await.unwrap();
        let second = rx.recv().await.unwrap();
        assert_eq!(first.meta.resource_version, 1);
        assert_eq!(second.meta.resource_version, 2);
    }

    #[tokio::test]
    async fn stream_subscription_receives_out_of_order_older_entry() {
        let service = test_service().await;
        let mut rx = service.subscribe_stream_entries();

        service.notify_entry(sample_entry(2));
        service.notify_entry(sample_entry(1));

        let first = rx.recv().await.unwrap();
        let second = rx.recv().await.unwrap();
        assert_eq!(first.meta.resource_version, 2);
        assert_eq!(second.meta.resource_version, 1);
        assert_eq!(service.current_position(), 2);
    }

    #[tokio::test]
    async fn fanout_stream_follower_receives_live_entries_without_using_broadcast_directly() {
        let service = Arc::new(test_service().await);
        let mut follower = service
            .register_stream_follower("replica-1".to_string(), 1)
            .await
            .unwrap();

        service.notify_entry(sample_entry(1));
        service.notify_entry(sample_entry(2));

        let first = follower.recv().await.unwrap();
        let second = follower.recv().await.unwrap();
        assert_eq!(first.meta.resource_version, 1);
        assert_eq!(second.meta.resource_version, 2);
    }

    #[tokio::test]
    async fn fanout_stream_replaces_existing_node_sender_on_rejoin() {
        let service = Arc::new(test_service().await);
        let metadata_a = test_network_dataplane(
            "replica-1".to_string(),
            klights_networking::wireguard::DataplaneMode::Root,
            klights_networking::wireguard::DataplaneEncryption::Enabled,
            Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string()),
            Some("127.0.0.1".to_string()),
            Some(51_820),
        )
        .unwrap();
        let metadata_b = test_network_dataplane(
            "replica-1".to_string(),
            klights_networking::wireguard::DataplaneMode::Root,
            klights_networking::wireguard::DataplaneEncryption::Enabled,
            Some("AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=".to_string()),
            Some("127.0.0.1".to_string()),
            Some(51_821),
        )
        .unwrap();

        let (_control_a, session_a) = service.register_follower(metadata_a).await;
        let mut old_stream = service
            .register_stream_follower("replica-1".to_string(), session_a)
            .await
            .unwrap();
        let (_control_b, session_b) = service.register_follower(metadata_b.clone()).await;
        let mut new_stream = service
            .register_stream_follower("replica-1".to_string(), session_b)
            .await
            .unwrap();

        assert!(matches!(
            old_stream.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected)
        ));

        service.notify_entry(sample_entry(3));
        assert_eq!(new_stream.recv().await.unwrap().meta.resource_version, 3);

        let metrics = service.follower_metrics().await;
        let expected_key = metadata_b.public_key().map(str::to_owned);
        assert_eq!(
            metrics.followers[0].public_key.as_deref(),
            expected_key.as_deref()
        );
    }

    #[tokio::test]
    async fn fanout_delivers_to_500_followers_without_head_of_line_blocking() {
        let service = Arc::new(test_service().await);
        let mut followers = Vec::new();
        for idx in 0..500 {
            followers.push(
                service
                    .register_stream_follower(format!("replica-{idx}"), idx as u64)
                    .await
                    .unwrap(),
            );
        }

        service.notify_entry(sample_entry(500));

        for follower in &mut followers {
            let entry = tokio::time::timeout(std::time::Duration::from_secs(1), follower.recv())
                .await
                .expect("fanout receiver timed out")
                .expect("fanout sender should stay connected");
            assert_eq!(entry.meta.resource_version, 500);
        }
    }

    #[tokio::test]
    async fn pod_log_follow_stream_routes_chunks_until_terminal_frame() {
        let service = Arc::new(test_service().await);
        let metadata = test_network_dataplane(
            "worker-1".to_string(),
            klights_networking::wireguard::DataplaneMode::Root,
            klights_networking::wireguard::DataplaneEncryption::Disabled,
            None,
            Some("127.0.0.1".to_string()),
            None,
        )
        .unwrap();
        let (mut follower_rx, follower_session) = service.register_follower(metadata).await;

        let session = service
            .open_node_logs_with_command_id(
                CommandId("log-stream-1".to_string()),
                sample_node_log_request("worker-1", true, Some(200)),
            )
            .await
            .unwrap();

        let Some(FollowerControlMessage::PodLog(request)) = follower_rx.recv().await else {
            panic!("expected pod log follow request");
        };
        assert_eq!(request.request_id, "log-stream-1");
        assert!(request.follow);
        assert_eq!(request.request.options().tail_lines(), Some(200));

        service
            .complete_node_log_event(
                FollowerCompletionContext::new(
                    "worker-1",
                    follower_session,
                    NodeOperationKind::Log,
                ),
                RoutedNodeLogEvent {
                    request_id: "log-stream-1".to_string(),
                    event: NodeLogEvent::data(b"first\n".to_vec()),
                },
            )
            .await
            .unwrap();
        service
            .complete_node_log_event(
                FollowerCompletionContext::new(
                    "worker-1",
                    follower_session,
                    NodeOperationKind::Log,
                ),
                RoutedNodeLogEvent {
                    request_id: "log-stream-1".to_string(),
                    event: NodeLogEvent::data(b"second\n".to_vec()),
                },
            )
            .await
            .unwrap();
        service
            .complete_node_log_event(
                FollowerCompletionContext::new(
                    "worker-1",
                    follower_session,
                    NodeOperationKind::Log,
                ),
                RoutedNodeLogEvent {
                    request_id: "log-stream-1".to_string(),
                    event: NodeLogEvent::terminal(),
                },
            )
            .await
            .unwrap();

        assert_eq!(
            session.recv_frame().await.unwrap().unwrap().content(),
            b"first\n"
        );
        assert_eq!(
            session.recv_frame().await.unwrap().unwrap().content(),
            b"second\n"
        );
        let terminal = session.recv_frame().await.unwrap().unwrap();
        assert!(terminal.is_terminal());
    }

    #[tokio::test]
    async fn handle_join_accepts_valid_token() {
        let (service, db) = test_service_with_db().await;
        let token = crate::bootstrap::bootstrap_token::ensure_default_bootstrap_token(
            db.as_ref(),
            std::time::Duration::from_secs(3600),
        )
        .await
        .unwrap();

        let req = JoinRequest {
            token,
            node_name: "worker-1".into(),
            role: klights_leader_api::JoinRole::Worker,
        };

        let resp = service.handle_join(req).await;
        match resp {
            JoinResponse::Accepted { cluster_id, .. } => {
                assert!(!cluster_id.is_empty());
            }
            JoinResponse::Rejected { reason } => {
                panic!("expected accepted, got rejected: {reason}");
            }
        }
    }

    #[tokio::test]
    async fn handle_authenticated_join_does_not_send_service_account_signer_to_worker() {
        let db = Arc::new(crate::datastore::test_support::in_memory().await);
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();
        let namespace_dir = tempfile::tempdir().unwrap();
        let namespace = namespace_dir.path().to_string_lossy().to_string();
        let signing_key_path = crate::paths::service_account_signing_key_path(&namespace);
        let signing_key = crate::auth::generate_ca_full().unwrap().3;
        crate::signing_key_state_adapter::persist(&signing_key_path, &signing_key, &supervisor)
            .await
            .unwrap();
        let service = crate::grpc_test_support::replication_service_with_metadata(
            db.clone(),
            db.focused_recovery_store(),
            supervisor,
        );

        let worker_resp = service
            .handle_authenticated_join(JoinRequest {
                token: "token".into(),
                node_name: "worker-1".into(),
                role: klights_leader_api::JoinRole::Worker,
            })
            .await;
        assert!(
            matches!(worker_resp, JoinResponse::Accepted { .. }),
            "expected accepted worker join"
        );
        let json = serde_json::to_string(&worker_resp).unwrap();
        assert!(!json.contains("service_account_signing_key_pem"));
    }

    #[tokio::test]
    async fn handle_join_rejects_controlplane_token_for_worker_join() {
        let (service, db) = test_service_with_db().await;
        create_scoped_token_for_test(
            db.as_ref(),
            "abcdef.0123456789abcdef",
            crate::bootstrap::bootstrap_token::BootstrapTokenScope::Controlplane,
        )
        .await;

        let req = JoinRequest {
            token: "abcdef.0123456789abcdef".into(),
            node_name: "worker-1".into(),
            role: klights_leader_api::JoinRole::Worker,
        };

        let resp = service.handle_join(req).await;
        match resp {
            JoinResponse::Rejected { reason } => {
                assert!(reason.contains("worker bootstrap token"), "{reason}");
            }
            JoinResponse::Accepted { .. } => {
                panic!("worker join must reject a controlplane bootstrap token");
            }
        }
    }

    #[tokio::test]
    async fn handle_join_rejects_invalid_token() {
        let service = test_service().await;

        let req = JoinRequest {
            token: "wrong-token".into(),
            node_name: "worker-1".into(),
            role: klights_leader_api::JoinRole::Worker,
        };

        let resp = service.handle_join(req).await;
        match resp {
            JoinResponse::Rejected { reason } => {
                assert!(reason.contains("bootstrap token"));
            }
            JoinResponse::Accepted { .. } => {
                panic!("expected rejected for bad token");
            }
        }
    }

    #[tokio::test]
    async fn handle_join_rejects_expired_bootstrap_token() {
        let (service, db) = test_service_with_db().await;
        crate::bootstrap::bootstrap_token::create_scoped_bootstrap_token_secret_with_ttl_for_test(
            db.as_ref(),
            crate::bootstrap::bootstrap_token::BootstrapTokenScope::Worker,
            "abcdef.0123456789abcdef",
            std::time::Duration::from_secs(0),
        )
        .await
        .unwrap();

        let req = JoinRequest {
            token: "abcdef.0123456789abcdef".into(),
            node_name: "worker-1".into(),
            role: klights_leader_api::JoinRole::Worker,
        };

        let resp = service.handle_join(req).await;
        match resp {
            JoinResponse::Rejected { reason } => {
                assert!(reason.contains("expired"));
            }
            JoinResponse::Accepted { .. } => {
                panic!("expected rejected for expired bootstrap token");
            }
        }
    }

    #[tokio::test]
    async fn handle_metadata_returns_values() {
        let service = test_service().await;
        let resp = service.handle_metadata().await;
        assert!(!resp.cluster_id.is_empty());
        assert_eq!(resp.leader_epoch, 0);
        assert_eq!(resp.current_log_index, 0);
    }

    struct FailingMetadataRead;

    impl klights_cluster_store::ClusterMetadataRead for FailingMetadataRead {
        fn read_cluster_metadata(
            &self,
        ) -> klights_cluster_store::ClusterMetadataFuture<
            '_,
            klights_cluster_store::PersistedClusterMetadata,
        > {
            Box::pin(async {
                Err(
                    klights_cluster_store::ClusterMetadataStoreError::persistence_failed(
                        "injected metadata failure",
                    ),
                )
            })
        }
    }

    #[tokio::test]
    async fn metadata_read_failure_preserves_rpc_and_join_error_semantics() {
        let db: Arc<dyn crate::datastore::DatastoreBackend> =
            Arc::new(crate::datastore::test_support::in_memory().await);
        let service = crate::grpc_test_support::replication_service_with_metadata(
            db,
            Arc::new(FailingMetadataRead),
            Arc::new(TaskSupervisor::new(TaskCategoryConfig::default())),
        );

        let metadata = service.handle_metadata().await;
        assert!(metadata.cluster_id.is_empty());
        assert_eq!(metadata.leader_epoch, 0);
        assert_eq!(metadata.current_rv, 0);
        assert_eq!(metadata.current_log_index, 0);

        let join = service
            .handle_authenticated_join(JoinRequest {
                token: String::new(),
                node_name: "worker-1".into(),
                role: klights_leader_api::JoinRole::Worker,
            })
            .await;
        assert_eq!(
            join,
            JoinResponse::Rejected {
                reason: "leader metadata error".into(),
            }
        );
    }

    #[tokio::test]
    async fn service_no_replica_connection_required() {
        // The service starts and is fully functional without any replica.
        let service = test_service().await;
        // Just verify we can create and use it
        assert_eq!(service.current_position(), 0);
        service.notify_entry(sample_entry(5));
        assert_eq!(service.current_position(), 5);
    }

    #[tokio::test]
    async fn follower_metrics_track_ack_lag_and_disconnect() {
        let service = test_service().await;
        service.notify_entry(sample_entry(10));
        let metadata = test_network_dataplane(
            "replica-1".to_string(),
            klights_networking::wireguard::DataplaneMode::Root,
            klights_networking::wireguard::DataplaneEncryption::Disabled,
            None,
            Some("127.0.0.1".to_string()),
            None,
        )
        .unwrap();

        let (_control_rx, session_id) = service.register_follower(metadata).await;
        service.update_follower_ack("replica-1", 7).await;

        let metrics = service.follower_metrics().await;
        assert_eq!(metrics.follower_count, 1);
        assert_eq!(metrics.max_lag, 3);
        assert_eq!(metrics.followers[0].node_name, "replica-1");

        service.unregister_follower("replica-1", session_id).await;
        assert_eq!(service.follower_metrics().await.follower_count, 0);
    }

    /// Old-session unregister must never remove a reconnected follower.
    #[tokio::test]
    async fn reconnect_race_old_session_unregister_must_not_remove_new_follower() {
        let service = test_service().await;
        let metadata_a = test_network_dataplane(
            "replica-1".to_string(),
            klights_networking::wireguard::DataplaneMode::Root,
            klights_networking::wireguard::DataplaneEncryption::Enabled,
            Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string()),
            Some("127.0.0.1".to_string()),
            Some(51_820),
        )
        .unwrap();
        let metadata_b = test_network_dataplane(
            "replica-1".to_string(),
            klights_networking::wireguard::DataplaneMode::Root,
            klights_networking::wireguard::DataplaneEncryption::Enabled,
            Some("AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=".to_string()),
            Some("127.0.0.1".to_string()),
            Some(51_821),
        )
        .unwrap();

        let (_control_rx_a, session_a) = service.register_follower(metadata_a).await;

        // Reconnect — this must invalidate session_a's control channel and
        // assign a new session.
        let (_control_rx_b, session_b) = service.register_follower(metadata_b.clone()).await;
        assert_ne!(
            session_a, session_b,
            "reconnect must produce a new session id"
        );

        // The old stream observes control_rx_a closed, breaks out of its loop,
        // and calls unregister_follower with the stale session_a.
        service.unregister_follower("replica-1", session_a).await;

        // The new follower (session_b) must still be registered.
        let metrics = service.follower_metrics().await;
        assert_eq!(
            metrics.follower_count, 1,
            "new follower must survive old-session unregister"
        );
        let expected_key = metadata_b.public_key().map(str::to_owned);
        assert_eq!(
            metrics.followers[0].public_key.as_deref(),
            expected_key.as_deref(),
            "surviving follower must be the reconnected session"
        );

        // A legitimate unregister with the current session must still work.
        service.unregister_follower("replica-1", session_b).await;
        assert_eq!(service.follower_metrics().await.follower_count, 0);
    }

    /// When the replication node-exec session is dropped without cancellation, the
    /// pending entry must be removed by the Drop impl.
    #[tokio::test]
    async fn node_exec_stream_session_drop_clears_pending_entry() {
        let service = Arc::new(test_service().await);
        let metadata = test_network_dataplane(
            "worker-1".to_string(),
            klights_networking::wireguard::DataplaneMode::Root,
            klights_networking::wireguard::DataplaneEncryption::Disabled,
            None,
            Some("127.0.0.1".to_string()),
            None,
        )
        .unwrap();
        let (mut control_rx, _session_id) = service.register_follower(metadata).await;

        let session = service
            .open_node_exec_with_command_id(
                CommandId("drop-test-1".to_string()),
                sample_node_exec_request("worker-1"),
            )
            .await
            .unwrap();

        assert!(matches!(
            control_rx.recv().await,
            Some(FollowerControlMessage::NodeExec(_))
        ));

        // Drop the session without calling close().
        drop(session);

        let replacement = service
            .open_node_exec_with_command_id(
                CommandId("drop-test-1".to_string()),
                sample_node_exec_request("worker-1"),
            )
            .await
            .expect("drop must release the correlation ID");
        drop(replacement);
    }

    /// Same drop-safety for the replication node-log stream.
    #[tokio::test]
    async fn pod_log_stream_session_drop_clears_pending_entry() {
        let service = Arc::new(test_service().await);
        let metadata = test_network_dataplane(
            "worker-1".to_string(),
            klights_networking::wireguard::DataplaneMode::Root,
            klights_networking::wireguard::DataplaneEncryption::Disabled,
            None,
            Some("127.0.0.1".to_string()),
            None,
        )
        .unwrap();
        let (mut control_rx, _session_id) = service.register_follower(metadata).await;

        let session = service
            .open_node_logs_with_command_id(
                CommandId("log-drop-1".to_string()),
                sample_node_log_request("worker-1", true, None),
            )
            .await
            .unwrap();

        assert!(matches!(
            control_rx.recv().await,
            Some(FollowerControlMessage::PodLog(_))
        ));

        drop(session);

        let replacement = service
            .open_node_logs_with_command_id(
                CommandId("log-drop-1".to_string()),
                sample_node_log_request("worker-1", true, None),
            )
            .await
            .expect("drop must release the correlation ID");
        drop(replacement);
    }

    /// When a follower disconnects, unregister_follower must sweep the pending
    /// maps and complete every in-flight request/stream targeted at that node.
    /// Without this, callers block until timeout.
    #[tokio::test]
    async fn unregister_follower_completes_pending_requests() {
        let service = Arc::new(test_service().await);

        // Register a follower.
        let metadata = test_network_dataplane(
            "test-node".to_string(),
            klights_networking::wireguard::DataplaneMode::Root,
            klights_networking::wireguard::DataplaneEncryption::Enabled,
            Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string()),
            Some("127.0.0.1".to_string()),
            Some(51_820),
        )
        .unwrap();
        let (mut control_rx, session_id) = service.register_follower(metadata).await;
        let other_metadata = test_network_dataplane(
            "other-node".to_string(),
            klights_networking::wireguard::DataplaneMode::Root,
            klights_networking::wireguard::DataplaneEncryption::Disabled,
            None,
            Some("127.0.0.2".to_string()),
            None,
        )
        .unwrap();
        let (mut other_control_rx, other_session_id) =
            service.register_follower(other_metadata).await;

        let exec_service = service.clone();
        let exec = tokio::spawn(async move {
            exec_service
                .execute_node_sync_with_command_id(
                    CommandId("exec-req-1".to_string()),
                    sample_node_exec_sync_request("test-node"),
                )
                .await
        });
        let log_service = service.clone();
        let log = tokio::spawn(async move {
            log_service
                .read_node_logs_with_command_id(
                    CommandId("log-req-1".to_string()),
                    sample_node_log_request("test-node", false, None),
                )
                .await
        });
        let metrics_service = service.clone();
        let metrics = tokio::spawn(async move {
            metrics_service
                .collect_node_metrics_with_command_id(
                    CommandId("metrics-req-1".to_string()),
                    NodeMetricsRequest::new(
                        klights_node_api::NodeMetricsTarget::try_new("test-node").unwrap(),
                        Vec::new(),
                    ),
                )
                .await
        });
        let other_service = service.clone();
        let other = tokio::spawn(async move {
            other_service
                .execute_node_sync_with_command_id(
                    CommandId("exec-req-2".to_string()),
                    sample_node_exec_sync_request("other-node"),
                )
                .await
        });

        for _ in 0..3 {
            control_rx.recv().await.expect("test-node request routed");
        }
        other_control_rx
            .recv()
            .await
            .expect("other-node request routed");

        // Unregister the follower for test-node.
        service.unregister_follower("test-node", session_id).await;

        let exec_err = exec.await.unwrap().unwrap_err().to_string();
        assert!(
            exec_err.contains("test-node"),
            "exec error must mention the disconnected node: {exec_err}"
        );

        let log_result = log.await.unwrap();
        assert!(
            log_result.unwrap_err().to_string().contains("test-node"),
            "pod log error must mention the disconnected node"
        );
        let metrics_result = metrics.await.unwrap();
        assert!(
            metrics_result.is_err(),
            "pending node metrics must fail on follower disconnect"
        );
        assert!(
            metrics_result
                .unwrap_err()
                .to_string()
                .contains("test-node"),
            "node metrics error must mention the disconnected node"
        );

        tokio::task::yield_now().await;
        assert!(
            !other.is_finished(),
            "other-node request must survive unregister of a different follower"
        );
        service
            .unregister_follower("other-node", other_session_id)
            .await;
        assert!(other.await.unwrap().is_err());
    }

    #[tokio::test]
    async fn request_node_metrics_sends_control_message_and_completes_response() {
        let service = Arc::new(test_service().await);
        let metadata = test_network_dataplane(
            "worker-1".to_string(),
            klights_networking::wireguard::DataplaneMode::Root,
            klights_networking::wireguard::DataplaneEncryption::Disabled,
            None,
            Some("127.0.0.1".to_string()),
            None,
        )
        .unwrap();
        let (mut control_rx, session_id) = service.register_follower(metadata).await;

        let service_for_request = service.clone();
        let request_task = tokio::spawn(async move {
            service_for_request
                .collect_node_metrics_with_command_id(
                    CommandId("metrics-1".to_string()),
                    NodeMetricsRequest::new(
                        klights_node_api::NodeMetricsTarget::try_new("worker-1").unwrap(),
                        Vec::new(),
                    ),
                )
                .await
                .unwrap()
        });

        let Some(FollowerControlMessage::NodeMetrics(request)) = control_rx.recv().await else {
            panic!("expected node metrics request");
        };
        assert_eq!(request.request_id, "metrics-1");
        assert_eq!(request.request.target().node_name(), "worker-1");

        service
            .complete_node_metrics(
                FollowerCompletionContext::new("worker-1", session_id, NodeOperationKind::Metrics),
                RoutedNodeMetricsResponse {
                    request_id: request.request_id,
                    node_name: "worker-1".to_string(),
                    result: Ok(NodeMetricsResult::new(
                        request.request.target().clone(),
                        None,
                        Vec::new(),
                    )),
                },
            )
            .await
            .unwrap();

        let response = request_task.await.unwrap();
        assert_eq!(response.target().node_name(), "worker-1");
        assert!(response.node().is_none());
    }

    #[tokio::test]
    async fn duplicate_node_metrics_correlation_does_not_replace_original_waiter() {
        let service = Arc::new(test_service().await);
        let metadata = test_network_dataplane(
            "worker-1".to_string(),
            klights_networking::wireguard::DataplaneMode::Root,
            klights_networking::wireguard::DataplaneEncryption::Disabled,
            None,
            Some("127.0.0.1".to_string()),
            None,
        )
        .unwrap();
        let (mut control_rx, session_id) = service.register_follower(metadata).await;
        let original_service = service.clone();
        let original = tokio::spawn(async move {
            original_service
                .collect_node_metrics_with_command_id(
                    CommandId("metrics-duplicate".to_string()),
                    NodeMetricsRequest::new(
                        klights_node_api::NodeMetricsTarget::try_new("worker-1").unwrap(),
                        Vec::new(),
                    ),
                )
                .await
        });
        let routed = control_rx.recv().await.expect("original request routed");
        assert!(matches!(routed, FollowerControlMessage::NodeMetrics(_)));

        let error = service
            .collect_node_metrics_with_command_id(
                CommandId("metrics-duplicate".to_string()),
                NodeMetricsRequest::new(
                    klights_node_api::NodeMetricsTarget::try_new("worker-1").unwrap(),
                    Vec::new(),
                ),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, NodeMetricsError::DuplicateRequest { .. }));

        let expected = NodeMetricsResult::new(
            klights_node_api::NodeMetricsTarget::try_new("worker-1").unwrap(),
            None,
            Vec::new(),
        );
        service
            .complete_node_metrics(
                FollowerCompletionContext::new("worker-1", session_id, NodeOperationKind::Metrics),
                RoutedNodeMetricsResponse {
                    request_id: "metrics-duplicate".to_string(),
                    node_name: "worker-1".to_string(),
                    result: Ok(expected.clone()),
                },
            )
            .await
            .unwrap();
        assert_eq!(original.await.unwrap().unwrap(), expected);
    }

    #[tokio::test]
    async fn authenticated_completion_mismatch_never_consumes_metrics_waiter() {
        let service = Arc::new(test_service().await);
        let worker = |name: &str| {
            test_network_dataplane(
                name.to_string(),
                klights_networking::wireguard::DataplaneMode::Root,
                klights_networking::wireguard::DataplaneEncryption::Disabled,
                None,
                Some("127.0.0.1".to_string()),
                None,
            )
            .unwrap()
        };
        let (mut worker_one_rx, worker_one_session) =
            service.register_follower(worker("worker-1")).await;
        let (_worker_two_rx, worker_two_session) =
            service.register_follower(worker("worker-2")).await;

        let request_service = service.clone();
        let request = tokio::spawn(async move {
            request_service
                .collect_node_metrics_with_command_id(
                    CommandId("shared-metrics-id".to_string()),
                    NodeMetricsRequest::new(
                        klights_node_api::NodeMetricsTarget::try_new("worker-1").unwrap(),
                        Vec::new(),
                    ),
                )
                .await
        });
        let Some(FollowerControlMessage::NodeMetrics(routed)) = worker_one_rx.recv().await else {
            panic!("expected metrics request");
        };
        let response = || RoutedNodeMetricsResponse {
            request_id: routed.request_id.clone(),
            node_name: "worker-1".to_string(),
            result: Ok(NodeMetricsResult::new(
                klights_node_api::NodeMetricsTarget::try_new("worker-1").unwrap(),
                None,
                Vec::new(),
            )),
        };

        for context in [
            FollowerCompletionContext::new(
                "worker-2",
                worker_two_session,
                NodeOperationKind::Metrics,
            ),
            FollowerCompletionContext::new(
                "worker-1",
                worker_one_session,
                NodeOperationKind::ExecSync,
            ),
        ] {
            assert!(
                service
                    .complete_node_metrics(context, response())
                    .await
                    .is_err()
            );
        }

        let mismatched_payload = RoutedNodeMetricsResponse {
            node_name: "worker-2".to_string(),
            ..response()
        };
        assert!(
            service
                .complete_node_metrics(
                    FollowerCompletionContext::new(
                        "worker-1",
                        worker_one_session,
                        NodeOperationKind::Metrics,
                    ),
                    mismatched_payload,
                )
                .await
                .is_err()
        );
        service
            .complete_node_metrics(
                FollowerCompletionContext::new(
                    "worker-1",
                    worker_one_session,
                    NodeOperationKind::Metrics,
                ),
                response(),
            )
            .await
            .unwrap();
        assert_eq!(
            request.await.unwrap().unwrap().target().node_name(),
            "worker-1"
        );
    }

    #[tokio::test]
    async fn stale_exec_stream_completion_cannot_remove_reused_request_id() {
        let service = Arc::new(test_service().await);
        let metadata = test_network_dataplane(
            "worker-1".to_string(),
            klights_networking::wireguard::DataplaneMode::Root,
            klights_networking::wireguard::DataplaneEncryption::Disabled,
            None,
            Some("127.0.0.1".to_string()),
            None,
        )
        .unwrap();
        let (mut control_rx, follower_session) = service.register_follower(metadata).await;
        let old_session = service
            .open_node_exec_with_command_id(
                CommandId("reused-exec-id".to_string()),
                sample_node_exec_request("worker-1"),
            )
            .await
            .unwrap();
        assert!(matches!(
            control_rx.recv().await,
            Some(FollowerControlMessage::NodeExec(_))
        ));
        let context = FollowerCompletionContext::new(
            "worker-1",
            follower_session,
            NodeOperationKind::ExecStream,
        );
        for _ in 0..EXPECTED_NODE_EXEC_STREAM_FRAME_CAPACITY {
            service
                .complete_node_exec_stream_frame(
                    context,
                    RoutedNodeExecFrame {
                        request_id: "reused-exec-id".to_string(),
                        frame: NodeExecFrame::new(ExecStreamChannel::Stdout, vec![1], false),
                    },
                )
                .await
                .unwrap();
        }
        let blocked_service = service.clone();
        let blocked = tokio::spawn(async move {
            blocked_service
                .complete_node_exec_stream_frame(
                    context,
                    RoutedNodeExecFrame {
                        request_id: "reused-exec-id".to_string(),
                        frame: NodeExecFrame::new(ExecStreamChannel::Error, Vec::new(), true),
                    },
                )
                .await
        });
        tokio::task::yield_now().await;
        drop(old_session);
        let replacement = service
            .open_node_exec_with_command_id(
                CommandId("reused-exec-id".to_string()),
                sample_node_exec_request("worker-1"),
            )
            .await
            .unwrap();
        assert!(matches!(
            control_rx.recv().await,
            Some(FollowerControlMessage::NodeExec(_))
        ));
        assert!(blocked.await.unwrap().is_err());

        service
            .complete_node_exec_stream_frame(
                context,
                RoutedNodeExecFrame {
                    request_id: "reused-exec-id".to_string(),
                    frame: NodeExecFrame::new(ExecStreamChannel::Stdout, b"new".to_vec(), false),
                },
            )
            .await
            .unwrap();
        assert_eq!(
            replacement.recv_frame().await.unwrap().unwrap().data(),
            b"new"
        );
    }

    #[tokio::test]
    async fn unregister_follower_closes_pending_node_exec_stream_immediately() {
        let service = Arc::new(test_service().await);
        let metadata = test_network_dataplane(
            "worker-1".to_string(),
            klights_networking::wireguard::DataplaneMode::Root,
            klights_networking::wireguard::DataplaneEncryption::Disabled,
            None,
            Some("127.0.0.1".to_string()),
            None,
        )
        .unwrap();
        let (mut control_rx, session_id) = service.register_follower(metadata).await;

        let session = service
            .open_node_exec_with_command_id(
                CommandId("exec-stream-disconnect-1".to_string()),
                sample_node_exec_request("worker-1"),
            )
            .await
            .unwrap();

        let routed = control_rx
            .recv()
            .await
            .expect("control request must be routed");
        assert!(matches!(
            routed,
            FollowerControlMessage::NodeExec(request)
                if request.request_id == "exec-stream-disconnect-1"
        ));

        service.unregister_follower("worker-1", session_id).await;

        let closed =
            tokio::time::timeout(std::time::Duration::from_millis(100), session.recv_frame())
                .await
                .expect("stream recv must resolve immediately after follower disconnect")
                .unwrap();
        assert!(
            closed.is_none(),
            "disconnect must close the exec stream receiver"
        );
    }

    #[tokio::test]
    async fn unregister_follower_closes_pending_pod_log_stream_immediately() {
        let service = Arc::new(test_service().await);
        let metadata = test_network_dataplane(
            "worker-1".to_string(),
            klights_networking::wireguard::DataplaneMode::Root,
            klights_networking::wireguard::DataplaneEncryption::Disabled,
            None,
            Some("127.0.0.1".to_string()),
            None,
        )
        .unwrap();
        let (mut control_rx, session_id) = service.register_follower(metadata).await;

        let session = service
            .open_node_logs_with_command_id(
                CommandId("pod-log-stream-disconnect-1".to_string()),
                sample_node_log_request("worker-1", true, None),
            )
            .await
            .unwrap();

        let routed = control_rx
            .recv()
            .await
            .expect("control request must be routed");
        assert!(matches!(
            routed,
            FollowerControlMessage::PodLog(request)
                if request.request_id == "pod-log-stream-disconnect-1"
        ));

        service.unregister_follower("worker-1", session_id).await;

        let closed =
            tokio::time::timeout(std::time::Duration::from_millis(100), session.recv_frame())
                .await
                .expect("stream recv must resolve immediately after follower disconnect")
                .unwrap();
        assert!(
            closed.is_none(),
            "disconnect must close the pod log stream receiver"
        );
    }
}
