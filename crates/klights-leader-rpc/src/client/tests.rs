//! gRPC client tests.

mod cases {

    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    use super::super::{ConnectDispatchContext, dispatch_leader_message};

    use klights_internal_protobuf::{self, follower_message, leader_message};
    use klights_leader_api::JoinRole;

    use klights_leader_rpc::client::{
        GrpcClientConfig, JoinDataplaneMetadata, NodeControlRuntimes, NodeExecCapability,
        NodeLogCapability, NodeMetricsCapability, ReplicationGrpcClient,
    };
    use klights_node_api::{
        NodeExecRuntime, NodeLogRuntime, NodeMetricsContainerSample, NodeMetricsNodeSample,
        NodeMetricsPodSample, NodeMetricsResult,
    };
    use klights_supervisor::{TaskCategoryConfig, TaskSupervisor};
    use tokio_util::sync::CancellationToken;

    use klights_leader_rpc::tls_policy::ResolvedLeaderTlsVerification;

    fn unavailable_runtimes() -> NodeControlRuntimes {
        NodeControlRuntimes::new(
            NodeExecCapability::Unavailable,
            NodeLogCapability::Unavailable,
            NodeMetricsCapability::Unavailable,
        )
    }

    #[test]
    fn plaintext_endpoint_is_rejected_by_every_connector_build() {
        assert!(
            super::super::normalized_endpoint("http://127.0.0.1:7679").is_err(),
            "the production endpoint normalizer must remain HTTPS-only"
        );
        assert!(
            super::super::connector_endpoint(" http://127.0.0.1:7679 ").is_err(),
            "tests and production must execute the same HTTPS-only connector path"
        );
        assert_eq!(
            super::super::connector_endpoint("127.0.0.1:7679").unwrap(),
            "https://127.0.0.1:7679",
            "bare endpoints must keep the production HTTPS default"
        );
    }

    #[test]
    fn legacy_entry_wire_item_projects_to_progress_heartbeat() {
        let entry = klights_cluster_core::ReplicationEntry {
            command: klights_cluster_core::StorageCommand::CreateNamespace {
                name: "legacy".to_string(),
                data: serde_json::json!({"metadata": {"name": "legacy"}}),
            },
            meta: klights_cluster_core::CommandMeta {
                command_id: klights_cluster_core::CommandId("legacy-entry".to_string()),
                codec_version: klights_cluster_core::COMMAND_CODEC_VERSION,
                resource_version: 37,
                uid: None,
                timestamp_ms: 0,
                authoring_node: "leader-a".to_string(),
            },
        };
        let item = klights_internal_protobuf::StreamItem {
            item: Some(klights_internal_protobuf::stream_item::Item::Entry(
                klights_leader_rpc::conversions::entry_to_proto(&entry).unwrap(),
            )),
        };

        assert_eq!(
            super::super::stream_item_from_proto(item).unwrap(),
            klights_cluster_core::StreamItem::Heartbeat { current_rv: 37 }
        );
    }

    #[test]
    fn resource_command_already_exists_survives_grpc_decode() {
        let error = super::super::resource_command_rpc_error(super::super::UnaryRpcError::Status(
            tonic::Status::already_exists("duplicate RuntimeClass"),
        ));
        assert!(matches!(
            error,
            klights_leader_api::ResourceCommandError::AlreadyExists { .. }
        ));
    }

    #[test]
    fn watch_request_wire_preserves_absent_and_explicit_zero_resource_version() {
        for expected in [None, Some(0)] {
            let request = klights_internal_protobuf::WatchResourcesRequest {
                api_version: "v1".to_string(),
                kind: "ConfigMap".to_string(),
                namespace: None,
                field_selector: None,
                start_resource_version: expected,
                label_selector: None,
                start_watch_replay_position: None,
            };
            let bytes = prost::Message::encode_to_vec(&request);
            let decoded =
                <klights_internal_protobuf::WatchResourcesRequest as prost::Message>::decode(
                    bytes.as_slice(),
                )
                .expect("decode WatchResourcesRequest");
            assert_eq!(
                decoded.start_resource_version, expected,
                "fresh-watch absence and explicit Kubernetes RV=0 are distinct wire intents"
            );
        }
    }

    #[test]
    fn projected_token_client_error_mapping_preserves_binding_and_authority_classes() {
        use klights_leader_api::ProjectedServiceAccountTokenError as Error;

        for (status, expected) in [
            (
                tonic::Status::failed_precondition("not raft leader"),
                Error::NotLeader,
            ),
            (
                tonic::Status::permission_denied("wrong caller node"),
                Error::Unauthorized,
            ),
            (
                tonic::Status::aborted("Pod UID changed"),
                Error::binding_mismatch("Pod UID changed"),
            ),
        ] {
            assert_eq!(
                super::super::projected_token_error_from_unary(
                    super::super::UnaryRpcError::Status(status)
                ),
                expected
            );
        }
    }

    fn dataplane() -> JoinDataplaneMetadata {
        JoinDataplaneMetadata {
            public_key: None,
            endpoint: "127.0.0.1".to_string(),
            port: None,
            mode: klights_leader_api::NetworkNodeMode::Root,
            encryption: klights_leader_api::DataplaneEncryption::Direct,
        }
    }

    fn cancellation_test_context(
        supervisor: Arc<TaskSupervisor>,
        exec: Option<Arc<dyn NodeExecRuntime>>,
        logs: Option<Arc<dyn NodeLogRuntime>>,
    ) -> ConnectDispatchContext {
        ConnectDispatchContext {
            supervisor,
            runtimes: NodeControlRuntimes::new(
                exec.map_or(
                    NodeExecCapability::Unavailable,
                    NodeExecCapability::Available,
                ),
                logs.map_or(NodeLogCapability::Unavailable, NodeLogCapability::Available),
                NodeMetricsCapability::Unavailable,
            ),
            node_exec_inputs: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
            node_stream_cancellations: Arc::new(tokio::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
            observed_leader_endpoint: None,
        }
    }

    #[tokio::test]
    async fn connect_disconnect_cancels_and_clears_all_private_stream_routes() {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let context = cancellation_test_context(supervisor.clone(), None, None);
        let exec_cancel = Arc::new(CancellationToken::new());
        let log_cancel = Arc::new(CancellationToken::new());
        context.node_stream_cancellations.lock().await.extend([
            (
                (super::super::ActiveRuntimeKind::Exec, "exec-1".to_string()),
                exec_cancel.clone(),
            ),
            (
                (super::super::ActiveRuntimeKind::Log, "log-1".to_string()),
                log_cancel.clone(),
            ),
        ]);
        let (input_tx, _input_rx) = tokio::sync::mpsc::channel(1);
        context.node_exec_inputs.lock().await.insert(
            "exec-1".to_string(),
            super::super::NodeExecInputRoute {
                sender: input_tx,
                cancellation: exec_cancel.clone(),
            },
        );

        super::super::cancel_all_node_streams(&context).await;

        assert!(exec_cancel.is_cancelled());
        assert!(log_cancel.is_cancelled());
        assert!(context.node_stream_cancellations.lock().await.is_empty());
        assert!(context.node_exec_inputs.lock().await.is_empty());
        let _ = supervisor.shutdown(Duration::from_secs(1)).await;
    }

    #[test]
    fn node_metrics_worker_wire_conversions_preserve_fields_and_errors() {
        let routed = super::super::node_metrics_request_from_proto(
            klights_internal_protobuf::NodeMetricsRequest {
                request_id: "metrics-worker-3".to_string(),
                node_name: "worker-3".to_string(),
                pod_uids: vec!["uid-a".to_string(), "uid-b".to_string()],
            },
        )
        .unwrap();
        assert_eq!(routed.request_id, "metrics-worker-3");
        assert_eq!(routed.request.target().node_name(), "worker-3");
        assert_eq!(routed.request.pod_uids(), ["uid-a", "uid-b"]);

        let response = super::super::node_metrics_response_to_proto(
            klights_node_api::RoutedNodeMetricsResponse {
                request_id: routed.request_id,
                node_name: "worker-3".to_string(),
                result: Ok(NodeMetricsResult::new(
                    routed.request.target().clone(),
                    Some(NodeMetricsNodeSample::new(37, 41)),
                    vec![NodeMetricsPodSample::new(
                        "kube-system",
                        "pod-a",
                        "uid-a",
                        vec![NodeMetricsContainerSample::new("main", 43, 47)],
                    )],
                )),
            },
        );
        assert_eq!(response.request_id, "metrics-worker-3");
        assert_eq!(response.node_name, "worker-3");
        assert_eq!(response.node.unwrap().cpu_nanos, 37);
        assert_eq!(response.pods[0].namespace, "kube-system");
        assert_eq!(response.pods[0].name, "pod-a");
        assert_eq!(response.pods[0].uid, "uid-a");
        assert_eq!(response.pods[0].containers[0].name, "main");
        assert_eq!(response.pods[0].containers[0].cpu_nanos, 43);
        assert_eq!(response.pods[0].containers[0].memory_bytes, 47);
        assert!(response.error.is_none());

        let error = super::super::node_metrics_response_to_proto(
            klights_node_api::RoutedNodeMetricsResponse {
                request_id: "metrics-worker-4".to_string(),
                node_name: "worker-4".to_string(),
                result: Err(klights_node_api::NodeMetricsError::unavailable(
                    "CRI unavailable",
                )),
            },
        );
        assert_eq!(error.request_id, "metrics-worker-4");
        assert_eq!(error.node_name, "worker-4");
        assert!(error.node.is_none());
        assert!(error.pods.is_empty());
        assert_eq!(error.error.as_deref(), Some("CRI unavailable"));
    }

    #[test]
    fn node_lease_renewal_rpc_preserves_focused_error_kinds() {
        use klights_leader_api::NodeLeaseRenewalError;

        let cases = [
            (
                super::super::UnaryRpcError::Status(tonic::Status::invalid_argument("bad lease")),
                NodeLeaseRenewalError::InvalidRequest {
                    field: "lease",
                    message: "bad lease".to_string(),
                },
            ),
            (
                super::super::UnaryRpcError::Status(tonic::Status::permission_denied("wrong node")),
                NodeLeaseRenewalError::Unauthorized {
                    message: "wrong node".to_string(),
                },
            ),
            (
                super::super::UnaryRpcError::Retryable(
                    "status: FailedPrecondition, message: not raft leader".to_string(),
                ),
                NodeLeaseRenewalError::NotLeader,
            ),
            (
                super::super::UnaryRpcError::Status(tonic::Status::unavailable("leader down")),
                NodeLeaseRenewalError::Unavailable {
                    message: "leader down".to_string(),
                },
            ),
            (
                super::super::UnaryRpcError::Retryable("connect failed".to_string()),
                NodeLeaseRenewalError::Retryable {
                    message: "connect failed".to_string(),
                },
            ),
            (
                super::super::UnaryRpcError::Status(tonic::Status::deadline_exceeded("late")),
                NodeLeaseRenewalError::Timeout,
            ),
            (
                super::super::UnaryRpcError::Status(tonic::Status::cancelled("shutdown")),
                NodeLeaseRenewalError::Cancelled,
            ),
        ];

        for (error, expected) in cases {
            assert_eq!(
                super::super::node_lease_renewal_error_from_unary(error),
                expected
            );
        }
    }

    #[test]
    fn outbox_transport_contract_unknown_response_error_fails_closed() {
        for error_type in [Some("FutureServerError"), None] {
            let error = super::super::outbox_error_from_response(
                error_type,
                "unrecognized server decision".to_string(),
            );
            assert!(matches!(
                &error,
                klights_leader_api::OutboxDeliveryError::CorruptResponse { .. }
            ));
            assert!(error.is_retryable());
        }
    }

    #[test]
    fn outbox_transport_terminal_decisions_require_typed_consumed_responses() {
        let response_error = super::super::decode_apply_outbox_response(
            klights_internal_protobuf::ApplyOutboxResponse {
                already_applied: true,
                applied_rv: 41,
                error: Some("conflict".to_string()),
                error_type: Some("ConflictTerminal".to_string()),
            },
        )
        .expect_err("success evidence and a terminal decision are contradictory");
        assert!(matches!(
            &response_error,
            klights_leader_api::OutboxDeliveryError::CorruptResponse { .. }
        ));
        assert!(response_error.is_retryable());

        for status in [
            tonic::Status::not_found("Pod is absent"),
            tonic::Status::failed_precondition("uid mismatch"),
            tonic::Status::already_exists("resource conflict"),
            tonic::Status::invalid_argument("malformed command"),
        ] {
            let status_error = super::super::outbox_error_from_status(status);
            assert!(
                status_error.is_retryable(),
                "a gRPC status carries no durable sequence-consumption proof: {status_error}"
            );
            assert!(!status_error.is_terminal());
        }
    }

    #[test]
    fn outbox_response_codec_preserves_absent_already_applied_resource_version() {
        let decoded = super::super::decode_apply_outbox_response(
            klights_internal_protobuf::ApplyOutboxResponse {
                already_applied: true,
                applied_rv: 0,
                error: None,
                error_type: None,
            },
        )
        .expect("zero is the stable wire encoding for an absent replay RV");
        assert_eq!(
            decoded,
            klights_leader_api::OutboxDeliveryResult::AlreadyApplied { applied_rv: None }
        );
    }

    #[test]
    fn outbox_response_codec_rejects_zero_resource_version_for_new_apply() {
        let error = super::super::decode_apply_outbox_response(
            klights_internal_protobuf::ApplyOutboxResponse {
                already_applied: false,
                applied_rv: 0,
                error: None,
                error_type: None,
            },
        )
        .expect_err("a new committed apply must carry a positive public resourceVersion");
        assert!(matches!(
            &error,
            klights_leader_api::OutboxDeliveryError::CorruptResponse { .. }
        ));
        assert!(error.is_retryable());
    }

    #[test]
    fn outbox_response_codec_preserves_positive_resource_versions() {
        for (already_applied, expected) in [
            (
                false,
                klights_leader_api::OutboxDeliveryResult::Applied { applied_rv: 19 },
            ),
            (
                true,
                klights_leader_api::OutboxDeliveryResult::AlreadyApplied {
                    applied_rv: Some(19),
                },
            ),
        ] {
            let decoded = super::super::decode_apply_outbox_response(
                klights_internal_protobuf::ApplyOutboxResponse {
                    already_applied,
                    applied_rv: 19,
                    error: None,
                    error_type: None,
                },
            )
            .expect("positive wire resourceVersion");
            assert_eq!(decoded, expected);
        }
    }

    fn resource_object() -> klights_internal_protobuf::ResourceObject {
        klights_internal_protobuf::ResourceObject {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "web".to_string(),
            uid: "uid-web".to_string(),
            resource_version: 42,
            data_json: serde_json::to_vec(&serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "web",
                    "uid": "uid-web",
                    "resourceVersion": "42"
                }
            }))
            .unwrap(),
        }
    }

    #[test]
    fn get_response_rejects_presence_contradictions() {
        for response in [
            klights_internal_protobuf::GetResourceResponse {
                found: true,
                resource: None,
            },
            klights_internal_protobuf::GetResourceResponse {
                found: false,
                resource: Some(resource_object()),
            },
        ] {
            assert!(matches!(
                super::super::resource_from_get_response(response),
                Err(klights_leader_api::ResourceQueryError::CorruptResponse { .. })
            ));
        }
    }

    #[test]
    fn resource_response_rejects_each_wire_body_identity_mismatch() {
        let mut cases = Vec::new();
        let mut api_version = resource_object();
        api_version.api_version = "apps/v1".to_string();
        cases.push(api_version);
        let mut kind = resource_object();
        kind.kind = "Node".to_string();
        cases.push(kind);
        let mut namespace = resource_object();
        namespace.namespace = Some("other".to_string());
        cases.push(namespace);
        let mut name = resource_object();
        name.name = "other".to_string();
        cases.push(name);
        let mut uid = resource_object();
        uid.uid = "other-uid".to_string();
        cases.push(uid);
        let mut resource_version = resource_object();
        resource_version.resource_version = 43;
        cases.push(resource_version);

        for resource in cases {
            assert!(matches!(
                super::super::resource_from_proto(resource),
                Err(klights_leader_api::ResourceQueryError::CorruptResponse { .. })
            ));
        }
    }

    #[test]
    fn list_response_rejects_negative_or_contradictory_metadata() {
        let base = || klights_internal_protobuf::ListResourcesResponse {
            items: vec![resource_object()],
            total: 1,
            continue_token: None,
            resource_version: 42,
            remaining_item_count: None,
            watch_replay_position: Some(klights_internal_protobuf::WatchReplayPosition {
                resource_version: 42,
                event_id: 9,
                resource_version_filter_through_event_id: 9,
            }),
            frozen_custom_resource_definition: None,
            candidate_continuations: Vec::new(),
        };
        let mut negative = base();
        negative.watch_replay_position.as_mut().unwrap().event_id = -1;
        let mut wrong_total = base();
        wrong_total.total = 2;
        let mut wrong_rv = base();
        wrong_rv
            .watch_replay_position
            .as_mut()
            .unwrap()
            .resource_version = 41;
        for response in [negative, wrong_total, wrong_rv] {
            assert!(matches!(
                super::super::validate_list_response_metadata(&response),
                Err(klights_leader_api::ResourceQueryError::CorruptResponse { .. })
            ));
        }
    }

    #[test]
    fn list_response_preserves_per_candidate_continuation_presence() {
        let candidates = vec![
            klights_internal_protobuf::CandidateContinuation {
                continue_token: Some("opaque/after-first".to_string()),
            },
            klights_internal_protobuf::CandidateContinuation {
                continue_token: None,
            },
        ];
        assert_eq!(
            super::super::candidate_continue_tokens_from_wire(candidates),
            vec![Some("opaque/after-first".to_string()), None],
            "RPC must preserve each candidate boundary, including absence"
        );
    }

    #[test]
    fn watch_wire_decode_preserves_event_id_and_rejects_unknown_type() {
        let wire = |event_type: &str| klights_internal_protobuf::WatchEvent {
            event_type: event_type.to_string(),
            resource: Some(klights_internal_protobuf::ResourceObject {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: Some("default".to_string()),
                name: "web".to_string(),
                uid: "uid-web".to_string(),
                resource_version: 42,
                data_json: serde_json::to_vec(&serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "namespace": "default",
                        "name": "web",
                        "uid": "uid-web",
                        "resourceVersion": "42"
                    }
                }))
                .expect("encode test Pod"),
            }),
            resume_position: Some(klights_internal_protobuf::WatchReplayPosition {
                resource_version: 42,
                event_id: 92,
                resource_version_filter_through_event_id: 0,
            }),
        };

        let event = super::super::resource_event_from_proto(wire("MODIFIED"))
            .expect("decode positioned event");
        assert_eq!(event.resume_position().unwrap().event_id, 92);
        assert!(matches!(
            super::super::resource_event_from_proto(wire("RENAMED")),
            Err(klights_leader_api::LeaderWatchError::UnknownEventType { .. })
        ));
    }

    fn default_transport_policy() -> klights_leader_rpc::transport_policy::SharedGrpcTransportPolicy
    {
        klights_leader_rpc::transport_policy::GrpcTransportPolicy::shared_default()
    }

    fn grpc_config_for_tls(ca_cert_path: Option<PathBuf>, skip_ca: bool) -> GrpcClientConfig {
        GrpcClientConfig {
            leader_endpoint: "https://leader:7679".to_string(),
            token: "abcdef.0123456789abcdef".to_string(),
            node_name: "worker-1".to_string(),
            role: JoinRole::Worker,
            dataplane: dataplane(),
            ca_cert_path,
            skip_ca,
            client_cert_pem: None,
            client_key_pem: None,
        }
    }

    async fn resolve_grpc_tls_verification(
        config: &GrpcClientConfig,
    ) -> ResolvedLeaderTlsVerification {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let resolved = config
            .leader_tls_verification(supervisor.as_ref())
            .await
            .unwrap();
        let _ = supervisor.shutdown(Duration::from_secs(1)).await;
        resolved
    }

    #[tokio::test]
    async fn tls_verification_policy_prefers_configured_ca_over_skip_ca() {
        let dir = tempfile::tempdir().unwrap();
        let ca_path = dir.path().join("leader-ca.crt");
        let ca_pem = b"configured-public-ca";
        std::fs::write(&ca_path, ca_pem).unwrap();
        let config = grpc_config_for_tls(Some(ca_path.clone()), true);

        assert_eq!(
            resolve_grpc_tls_verification(&config).await,
            ResolvedLeaderTlsVerification::CaPem(ca_pem.to_vec())
        );
    }

    #[tokio::test]
    async fn tls_verification_policy_uses_configured_ca_without_skip_ca() {
        let dir = tempfile::tempdir().unwrap();
        let ca_path = dir.path().join("leader-ca.crt");
        let ca_pem = b"configured-public-ca";
        std::fs::write(&ca_path, ca_pem).unwrap();
        let config = grpc_config_for_tls(Some(ca_path.clone()), false);

        assert_eq!(
            resolve_grpc_tls_verification(&config).await,
            ResolvedLeaderTlsVerification::CaPem(ca_pem.to_vec())
        );
    }

    #[tokio::test]
    async fn tls_verification_policy_uses_system_roots_without_ca_or_skip_ca() {
        let config = grpc_config_for_tls(None, false);

        assert_eq!(
            resolve_grpc_tls_verification(&config).await,
            ResolvedLeaderTlsVerification::SystemRoots
        );
    }

    #[test]
    fn worker_constructor_preserves_skip_ca_flag() {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let client = ReplicationGrpcClient::worker(
            "https://leader:7679".to_string(),
            "worker-1".to_string(),
            "abcdef.0123456789abcdef".to_string(),
            dataplane(),
            None,
            true,
            supervisor,
            default_transport_policy(),
        );

        assert!(client.config.skip_ca);
    }

    #[test]
    fn steady_state_join_payload_omits_bootstrap_token() {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let client = ReplicationGrpcClient::new(
            GrpcClientConfig {
                leader_endpoint: "https://leader:7679".to_string(),
                token: "abcdef.0123456789abcdef".to_string(),
                node_name: "worker-1".to_string(),
                role: JoinRole::Worker,
                dataplane: dataplane(),
                ca_cert_path: None,
                skip_ca: false,
                client_cert_pem: None,
                client_key_pem: None,
            },
            supervisor,
            klights_leader_rpc::transport_policy::GrpcTransportPolicy::shared_default(),
        );

        assert_eq!(client.join_request().token, "");
        assert_eq!(
            client.join_request().command_codec_version,
            klights_cluster_core::COMMAND_CODEC_VERSION,
            "every worker stream handshake must advertise exact codec v3"
        );
    }

    #[tokio::test]
    async fn observe_leader_endpoint_request_sends_observed_endpoint_response() {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let context = ConnectDispatchContext {
            supervisor,
            runtimes: unavailable_runtimes(),
            node_exec_inputs: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
            node_stream_cancellations: Arc::new(tokio::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
            observed_leader_endpoint: Some("10.99.0.10".to_string()),
        };
        let (outbound, mut outbound_rx) = tokio::sync::mpsc::channel(1);
        let (stream_tx, _stream_rx) = tokio::sync::mpsc::channel(1);

        dispatch_leader_message(
            klights_internal_protobuf::LeaderMessage {
                payload: Some(leader_message::Payload::ObserveLeaderEndpointRequest(
                    klights_internal_protobuf::ObserveLeaderEndpointRequest {},
                )),
            },
            &outbound,
            &stream_tx,
            &context,
        )
        .await
        .expect("observe request should be handled");

        let response = outbound_rx
            .recv()
            .await
            .expect("client should send observed endpoint response");
        match response.payload {
            Some(follower_message::Payload::ObservedLeaderEndpoint(observed)) => {
                assert_eq!(observed.endpoint, "10.99.0.10");
            }
            other => panic!("unexpected follower response: {other:?}"),
        }
    }

    #[test]
    fn csr_rpc_allows_bootstrap_token_metadata_before_node_cert_exists() {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let client = ReplicationGrpcClient::new(
            GrpcClientConfig {
                leader_endpoint: "https://leader:7679".to_string(),
                token: "abcdef.0123456789abcdef".to_string(),
                node_name: "worker-1".to_string(),
                role: JoinRole::Worker,
                dataplane: dataplane(),
                ca_cert_path: None,
                skip_ca: false,
                client_cert_pem: None,
                client_key_pem: None,
            },
            supervisor,
            klights_leader_rpc::transport_policy::GrpcTransportPolicy::shared_default(),
        );

        // bug-grpc A2: the CSR token is now precomputed (so the `unary_call`
        // closure can attach it per attempt). With a bootstrap token and no
        // node-cert mTLS, the value must be present.
        let value = client
            .bootstrap_csr_token_value()
            .expect("token must parse as gRPC metadata");
        assert!(
            value.is_some(),
            "a configured bootstrap token (no node cert) must produce a CSR token value"
        );
    }

    #[test]
    fn controlplane_join_proto_preserves_joiner_owned_registration_snapshot() {
        let registration = klights_leader_api::RemoteNodeRegistrationSnapshot {
            node_mode: klights_leader_api::RemoteNodeMode::Rootless,
            host: klights_leader_api::RemoteNodeHostFacts {
                cpu_count: 23,
                memory_ki: 45_678_901,
                architecture: "joiner-arch".to_string(),
                operating_system: "linux".to_string(),
                os_image: "Joiner OS".to_string(),
                kernel_version: "7.7-joiner".to_string(),
                container_runtime_version: "containerd://8.0".to_string(),
                kubelet_version: "v1.34.0-joiner".to_string(),
                git_commit: "joinercommit".to_string(),
            },
        };

        let proto = klights_leader_rpc::client::node_registration_to_proto(&registration);
        assert_eq!(proto.cpu_count, 23);
        assert_eq!(proto.memory_ki, 45_678_901);
        assert_eq!(proto.architecture, "joiner-arch");
        assert_eq!(proto.operating_system, "linux");
        assert_eq!(proto.os_image, "Joiner OS");
        assert_eq!(proto.kernel_version, "7.7-joiner");
        assert_eq!(proto.container_runtime_version, "containerd://8.0");
        assert_eq!(proto.kubelet_version, "v1.34.0-joiner");
        assert_eq!(proto.git_commit, "joinercommit");
        assert_eq!(proto.node_mode, "rootless");
    }

    #[test]
    fn cleanup_intent_wire_decode_rejects_noncanonical_pod_snapshot_with_typed_error() {
        let error = klights_leader_rpc::client::pod_cleanup_intent_from_proto(
            klights_internal_protobuf::PodCleanupIntentObject {
                node_name: "worker-1".to_string(),
                namespace: "default".to_string(),
                pod_name: "web".to_string(),
                pod_uid: "pod-uid".to_string(),
                reason: "NodeLost".to_string(),
                resource_version: 22,
                created_at_ms: 1_700_000_000_000,
                pod_data_json: br#"{"apiVersion":"v1","kind":"Pod","metadata":{"namespace":"default","name":"web","uid":"other-uid","resourceVersion":"17"},"spec":{"nodeName":"worker-1"}}"#.to_vec(),
            },
        )
        .expect_err("mismatched Pod UID must fail closed");
        assert!(matches!(
            error,
            klights_leader_api::PodCleanupIntentError::CorruptIntent { .. }
        ));
    }

    /// T3: InstallSnapshot must use its own channel lane, NOT the Raft lane, so
    /// a stalled multi-chunk snapshot transfer cannot head-of-line-block
    /// heartbeats/AppendEntries multiplexed over the same connection under loss.
    /// Driving one install_snapshot RPC must materialize the InstallSnapshot
    /// lane pool and leave the Raft lane untouched.

    // `client_times_out_hung_forward_response_and_clears_stream` removed
    // in T6 — it exercised the deleted ForwardCommand round-trip path.

    // --- Worker auto-rejoin: self-heal of wedged Read/Status lanes ---
    //
    // After a leader *process* restart the worker's warm channel pools
    // wedge. Only the Stream lane self-heals (`clear_stream`); the Read
    // (watch/informers) and Status (lease/outbox) lanes must also evict on
    // a transport-level error so the existing reconnect/heartbeat/dispatch
    // loops rebuild a fresh channel and the node rejoins without a restart.
    // Mirrors the replication-owned Raft transport self-heal.

    #[test]
    fn is_transport_status_classifies_connection_failures_only() {
        use tonic::Status;
        let cases: [(Status, bool); 8] = [
            (Status::unavailable("error trying to connect"), true),
            (Status::unknown("h2 protocol error: connection reset"), true),
            (Status::cancelled("stream reset by peer"), true),
            (Status::failed_precondition("not raft leader"), false),
            (Status::not_found("missing"), false),
            (Status::already_exists("dup"), false),
            (Status::aborted("conflict"), false),
            (Status::invalid_argument("bad request"), false),
        ];
        for (status, expected) in cases {
            assert_eq!(
                super::super::is_transport_status(&status),
                expected,
                "unexpected classification for code {:?}",
                status.code()
            );
        }
    }

    // ── Task 7: raft lane health and per-peer loss observability ─────────────

    /// Helper: build a test client against a server that wedges the given
    /// gRPC method path for 30 s, with a short raft_unary_deadline.

    #[test]
    fn raft_rpc_deadline_is_below_election_timeout_floor() {
        // T7 election-floor invariant: the raft unary RPC deadline must be
        // strictly below the election timeout minimum so a wedged peer cannot
        // prevent the cluster from electing a new leader.
        //
        // The replication owner keeps the matching compile-time assertion
        // beside its OpenRaft configuration.
        // This runtime test records the same invariant for the transport layer.
        let policy = klights_leader_rpc::transport_policy::GrpcTransportPolicy::shared_default();
        let raft_deadline_ms = policy.raft_unary_deadline.as_millis() as u64;
        // RAFT_ELECTION_TIMEOUT_MIN_MS = 9000 (must stay in sync with node.rs)
        let election_floor_ms: u64 = 9000;
        assert!(
            raft_deadline_ms < election_floor_ms,
            "raft_unary_deadline ({raft_deadline_ms} ms) must be below election timeout floor \
             ({election_floor_ms} ms) so a wedged peer cannot block leader election"
        );
    }

    #[tokio::test]
    async fn try_set_tcp_congestion_bbr_is_infallible_on_a_real_socket() {
        // The BBR tuning helper must never propagate an error regardless of
        // whether the host kernel exposes BBR: on a BBR-less kernel it logs at
        // debug and returns; on a BBR kernel it sets the algorithm. Either way
        // the caller is unaffected.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        // Mirrors the connector order: nodelay first, then the BBR helper.
        assert!(stream.set_nodelay(true).is_ok());
        super::super::try_set_tcp_congestion_bbr(&stream);
        // Reaching here means the helper neither panicked nor tore down the
        // socket; the connector invokes it immediately after set_nodelay.
        assert!(stream.nodelay().unwrap());
    }

    #[tokio::test]
    async fn observed_peer_connector_sets_nodelay_then_bbr_without_failing() {
        use tonic::transport::Uri;
        use tower::Service as _;

        // A bound, listening socket completes the TCP handshake from the
        // kernel listen queue without the test ever calling accept(), so
        // connect + the connector's stream setup (set_nodelay -> BBR helper)
        // complete end-to-end.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let observed = Arc::new(std::sync::Mutex::new(None::<String>));
        let mut connector = super::super::ObservedPeerTcpConnector::new(observed.clone());
        let uri: Uri = format!("http://{addr}").parse().unwrap();
        connector
            .call(uri)
            .await
            .expect("connector stream setup (set_nodelay + bbr tuning) must succeed");
        // observed_peer_ip is recorded *after* set_nodelay and the BBR helper,
        // so a populated peer IP proves the full stream-setup path ran without
        // taking the connection down.
        assert_eq!(
            *observed.lock().unwrap(),
            Some("127.0.0.1".to_string()),
            "connector must record the peer IP after stream setup"
        );
    }

    #[test]
    fn node_subnet_wire_decode_rejects_redundancy_and_shape_mismatches() {
        let valid = klights_internal_protobuf::NodeSubnetObject {
            node_name: "node-a".to_string(),
            subnet: "10.42.1.0/24".to_string(),
            subnet_base_int: u32::from(std::net::Ipv4Addr::new(10, 42, 1, 0)),
            gateway_ip: "10.42.1.0".to_string(),
            node_ip: "192.0.2.10".to_string(),
            mode: "root".to_string(),
            hostport_range: None,
        };
        assert!(klights_leader_rpc::client::node_subnet_from_proto(valid.clone()).is_ok());

        for invalid in [
            klights_internal_protobuf::NodeSubnetObject {
                subnet: "10.42.1.0/25".to_string(),
                ..valid.clone()
            },
            klights_internal_protobuf::NodeSubnetObject {
                subnet_base_int: u32::from(std::net::Ipv4Addr::new(10, 42, 9, 0)),
                ..valid.clone()
            },
            klights_internal_protobuf::NodeSubnetObject {
                gateway_ip: "10.42.1.1".to_string(),
                ..valid.clone()
            },
            klights_internal_protobuf::NodeSubnetObject {
                mode: "unknown".to_string(),
                ..valid.clone()
            },
            klights_internal_protobuf::NodeSubnetObject {
                mode: "rootless".to_string(),
                ..valid
            },
        ] {
            assert!(matches!(
                klights_leader_rpc::client::node_subnet_from_proto(invalid),
                Err(klights_leader_api::NetworkTopologyError::CorruptResponse { .. })
            ));
        }
    }

    #[test]
    fn dataplane_wire_decode_rejects_unknown_and_overlay_shaped_direct_routes() {
        let direct = klights_internal_protobuf::DataplaneMetadataObject {
            node_name: "node-a".to_string(),
            mode: "root".to_string(),
            encryption: "disabled".to_string(),
            public_key: None,
            endpoint: "192.0.2.10".to_string(),
            port: None,
        };
        assert!(klights_leader_rpc::client::dataplane_metadata_from_proto(direct.clone()).is_ok());

        for invalid in [
            klights_internal_protobuf::DataplaneMetadataObject {
                mode: "unknown".to_string(),
                ..direct.clone()
            },
            klights_internal_protobuf::DataplaneMetadataObject {
                encryption: "unknown".to_string(),
                ..direct.clone()
            },
            klights_internal_protobuf::DataplaneMetadataObject {
                public_key: Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string()),
                ..direct.clone()
            },
            klights_internal_protobuf::DataplaneMetadataObject {
                port: Some(7_679),
                ..direct
            },
        ] {
            assert!(matches!(
                klights_leader_rpc::client::dataplane_metadata_from_proto(invalid),
                Err(klights_leader_api::NetworkTopologyError::CorruptResponse { .. })
            ));
        }
    }
}

#[test]
fn remote_api_client_exposes_resource_command_capability() {
    fn assert_capability<T: klights_leader_api::LeaderResourceCommand>() {}
    assert_capability::<crate::client::RemoteApiClient>();
}
