use std::sync::Arc;

use std::sync::atomic::{AtomicUsize, Ordering};

use crate::server::{require_worker_command_codec_v3, validate_join_metadata};

use klights_cluster_core::WatchReplayPosition;

use klights_internal_protobuf::{JoinRequest, JoinRole};

#[test]
fn resource_command_already_exists_uses_distinct_grpc_status() {
    let status =
        super::resource_command_status(klights_leader_api::ResourceCommandError::AlreadyExists {
            message: "duplicate RuntimeClass".to_string(),
        });
    assert_eq!(status.code(), tonic::Code::AlreadyExists);
}

#[test]
fn positioned_replay_expiry_keeps_the_typed_grpc_relist_marker() {
    let status =
        super::leader_watch_error_to_status(klights_leader_api::LeaderWatchError::ReplayExpired {
            accepted_resource_version: 73,
        });
    assert!(klights_leader_rpc::is_watch_replay_expired_status(&status));
    assert_eq!(
        crate::watch_replay_expired_resource_version(&status),
        Some(73)
    );
}

#[test]
fn node_metrics_wire_conversions_preserve_correlation_and_sample_fields() {
    let request = crate::protocol::RoutedNodeMetricsRequest {
        request_id: "metrics-request-7".to_string(),
        request: klights_node_api::NodeMetricsRequest::new(
            klights_node_api::NodeMetricsTarget::try_new("worker-7").unwrap(),
            vec!["pod-a".to_string(), "pod-b".to_string()],
        ),
    };
    let request = super::node_metrics_request_to_proto(request);
    assert_eq!(request.request_id, "metrics-request-7");
    assert_eq!(request.node_name, "worker-7");
    assert_eq!(request.pod_uids, ["pod-a", "pod-b"]);

    let response =
        super::node_metrics_response_from_proto(klights_internal_protobuf::NodeMetricsResponse {
            request_id: "metrics-request-7".to_string(),
            node_name: "worker-7".to_string(),
            node: Some(klights_internal_protobuf::NodeMetricsNodeSample {
                cpu_nanos: 17,
                memory_bytes: 23,
            }),
            pods: vec![klights_internal_protobuf::NodeMetricsPodSample {
                namespace: "default".to_string(),
                name: "pod-a".to_string(),
                uid: "pod-a-uid".to_string(),
                containers: vec![klights_internal_protobuf::NodeMetricsContainerSample {
                    name: "main".to_string(),
                    cpu_nanos: 29,
                    memory_bytes: 31,
                }],
            }],
            error: None,
        });
    assert_eq!(response.request_id, "metrics-request-7");
    assert_eq!(response.node_name, "worker-7");
    let result = response.result.unwrap();
    assert_eq!(result.target().node_name(), "worker-7");
    assert_eq!(result.node().unwrap().cpu_nanos(), 17);
    assert_eq!(result.node().unwrap().memory_bytes(), 23);
    assert_eq!(result.pods()[0].namespace(), "default");
    assert_eq!(result.pods()[0].name(), "pod-a");
    assert_eq!(result.pods()[0].uid(), "pod-a-uid");
    assert_eq!(result.pods()[0].containers()[0].name(), "main");
    assert_eq!(result.pods()[0].containers()[0].cpu_nanos(), 29);
    assert_eq!(result.pods()[0].containers()[0].memory_bytes(), 31);

    let error =
        super::node_metrics_response_from_proto(klights_internal_protobuf::NodeMetricsResponse {
            request_id: "metrics-request-8".to_string(),
            node_name: "worker-8".to_string(),
            node: None,
            pods: Vec::new(),
            error: Some("runtime stats unavailable".to_string()),
        });
    assert_eq!(error.request_id, "metrics-request-8");
    assert_eq!(error.node_name, "worker-8");
    assert!(matches!(
        error.result,
        Err(klights_node_api::NodeMetricsError::Unavailable { ref message })
            if message == "runtime stats unavailable"
    ));

    let mixed =
        super::node_metrics_response_from_proto(klights_internal_protobuf::NodeMetricsResponse {
            request_id: "metrics-request-9".to_string(),
            node_name: "worker-9".to_string(),
            node: Some(klights_internal_protobuf::NodeMetricsNodeSample {
                cpu_nanos: 99,
                memory_bytes: 101,
            }),
            pods: Vec::new(),
            error: Some("runtime failed".to_string()),
        });
    assert!(matches!(
        mixed.result,
        Err(klights_node_api::NodeMetricsError::Unavailable { ref message })
            if message == "runtime failed"
    ));
}

#[test]
fn resource_proto_body_uses_authoritative_identity_and_resource_version() {
    let resource = klights_cluster_core::Resource {
        id: 7,
        api_version: "v1".to_string(),
        kind: "ConfigMap".to_string(),
        namespace: Some("default".to_string()),
        name: "canonical".to_string(),
        uid: "uid-canonical".to_string(),
        resource_version: 42,
        data: Arc::new(serde_json::json!({
            "apiVersion": "stale/v1",
            "kind": "Stale",
            "metadata": {
                "namespace": "stale",
                "name": "stale",
                "uid": "uid-stale"
            },
            "status": {"phase": "Ready", "observedGeneration": 9}
        })),
    };

    let wire = super::resource_to_proto(&resource);
    let body: serde_json::Value = serde_json::from_slice(&wire.data_json).expect("resource JSON");
    assert_eq!(body["apiVersion"], "v1");
    assert_eq!(body["kind"], "ConfigMap");
    assert_eq!(body["metadata"]["namespace"], "default");
    assert_eq!(body["metadata"]["name"], "canonical");
    assert_eq!(body["metadata"]["uid"], "uid-canonical");
    assert_eq!(body["metadata"]["resourceVersion"], "42");
    assert_eq!(body["status"]["phase"], "Ready");
    assert_eq!(body["status"]["observedGeneration"], 9);
}

#[test]
fn node_lease_renew_time_skew_allows_100_seconds_but_rejects_101() {
    let now = chrono::DateTime::parse_from_rfc3339("2026-07-07T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let past_100 =
        klights_cluster_core::k8s_time::format_time(now - chrono::Duration::seconds(100));
    let future_100 =
        klights_cluster_core::k8s_time::format_time(now + chrono::Duration::seconds(100));
    let past_101 =
        klights_cluster_core::k8s_time::format_time(now - chrono::Duration::seconds(101));
    let future_101 =
        klights_cluster_core::k8s_time::format_time(now + chrono::Duration::seconds(101));

    super::validate_node_lease_renew_time_skew(&past_100, now)
        .expect("100s past skew is accepted at boundary");
    super::validate_node_lease_renew_time_skew(&future_100, now)
        .expect("100s future skew is accepted at boundary");
    assert_eq!(
        super::validate_node_lease_renew_time_skew(&past_101, now)
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
    assert_eq!(
        super::validate_node_lease_renew_time_skew(&future_101, now)
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
}

fn valid_join() -> JoinRequest {
    JoinRequest {
        token: "token".to_string(),
        node_name: "worker-1".to_string(),
        role: JoinRole::Worker as i32,
        dataplane_public_key: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string(),
        dataplane_endpoint: "192.0.2.10".to_string(),
        dataplane_port: 51_820,
        dataplane_mode: "root".to_string(),
        dataplane_encryption: "enabled".to_string(),
        command_codec_version: klights_cluster_core::COMMAND_CODEC_VERSION,
    }
}

/// bug-grpc A1/B2: serve the replication gRPC service built with an
/// explicit [`GrpcTransportPolicy`] so a test can shrink `max_message_bytes`
/// (decode-limit test) or `watch_heartbeat_interval` (per-stream heartbeat
/// test). `injected_node_cert` injects a node client cert so handlers that
/// require steady-state auth (e.g. `watch_resources`) accept the request;
/// `None` leaves auth to fail (the decode-size check fires first regardless).

/// bug-grpc A1: the server now applies `GrpcTransportPolicy::max_message_bytes`
/// to the tonic service decode limit (previously unset → unbounded). A
/// request larger than the configured limit must be rejected at decode,
/// before the handler runs.

/// bug-grpc B2: a quiet *matching* internal watch stream must still emit a
/// per-stream BOOKMARK heartbeat even while the global broadcast carries a
/// continuous stream of *non-matching* events. The old code reset the
/// heartbeat deadline on every loop iteration, so unrelated traffic starved
/// the bookmark and the worker idle-reconnected every window.

/// Worker pod watches are field-selected by `spec.nodeName`. A signal for a
/// higher-RV non-matching Pod must replay the durable Pod history from the
/// worker stream's accepted RV, so a lower-RV matching Pod already present
/// in `watch_events` is delivered instead of being skipped behind the
/// non-matching high-water mark.

// --- watch_resources leadership-termination tests (issue #4) -----------

fn test_node_registration_proto(
    git_commit: &str,
) -> klights_internal_protobuf::NodeRegistrationSnapshot {
    klights_internal_protobuf::NodeRegistrationSnapshot {
        cpu_count: 6,
        memory_ki: 12 * 1024 * 1024,
        architecture: "test-arch".to_string(),
        operating_system: "linux".to_string(),
        os_image: "Test Linux".to_string(),
        kernel_version: "6.1-test".to_string(),
        container_runtime_version: "containerd://1.7.0".to_string(),
        kubelet_version: "v1.34.0-test".to_string(),
        git_commit: git_commit.to_string(),
        node_mode: "root".to_string(),
    }
}

#[test]
fn controlplane_node_registration_rejects_empty_or_invalid_host_facts() {
    let mut cases = Vec::new();
    let mut zero_cpu = test_node_registration_proto("joiner");
    zero_cpu.cpu_count = 0;
    cases.push(("zero-cpu", zero_cpu));
    let mut zero_memory = test_node_registration_proto("joiner");
    zero_memory.memory_ki = 0;
    cases.push(("zero-memory", zero_memory));
    let mut empty_kernel = test_node_registration_proto("joiner");
    empty_kernel.kernel_version.clear();
    cases.push(("empty-kernel", empty_kernel));
    let mut invalid_mode = test_node_registration_proto("joiner");
    invalid_mode.node_mode = "leader-root".to_string();
    cases.push(("invalid-mode", invalid_mode));

    for (name, registration) in cases {
        assert!(
            klights_leader_rpc::server::validate_controlplane_node_registration(registration)
                .is_err(),
            "{name} must be rejected"
        );
    }
}

/// Test double whose callers are never existing members — exercises the
/// "worker / first-time caller without a controlplane token is rejected"
/// path on JoinAsControlplane.

/// A control-plane node client certificate: `system:nodes` plus the
/// `system:controlplanes` group that the controlplane-token-gated bootstrap
/// stamps. This is what authorizes raft consensus RPCs.

// ── CRIT-2: NodeRestriction on node-scoped RPCs ──

#[test]
fn production_auth_mount_graph_is_compiler_reachable_in_tests() {
    let _mount = super::mount_service_full_production;
}

struct StatefulPeerAuthenticator {
    calls: AtomicUsize,
    reject_first: bool,
}

#[async_trait::async_trait]

impl super::ReplicationPeerAuthenticator for StatefulPeerAuthenticator {
    async fn authenticate(
        &self,
        _certificate: &klights_types::TlsClientCertificate,
    ) -> Result<super::ReplicationPeerIdentity, super::ReplicationPeerAuthenticationError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if self.reject_first || call > 0 {
            return Err(super::ReplicationPeerAuthenticationError::Rejected {
                message: "stateful rejection".to_string(),
            });
        }
        Ok(super::ReplicationPeerIdentity {
            username: "system:node:worker-7".to_string(),
            groups: vec!["system:nodes".to_string()],
        })
    }
}

#[tokio::test]
async fn node_authority_reuses_exactly_one_authenticated_identity() {
    let authenticator = StatefulPeerAuthenticator {
        calls: AtomicUsize::new(0),
        reject_first: false,
    };
    let certificate = klights_types::TlsClientCertificate(vec![1, 2, 3]);
    let identity = super::authenticate_peer_identity(&authenticator, &certificate)
        .await
        .unwrap();
    assert!(matches!(
        super::node_authority_from_identity(&identity),
        super::CallerAuthority::Node(node) if node == "worker-7"
    ));
    assert_eq!(authenticator.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn node_authority_fails_closed_when_the_single_authentication_rejects() {
    let authenticator = StatefulPeerAuthenticator {
        calls: AtomicUsize::new(0),
        reject_first: true,
    };
    let certificate = klights_types::TlsClientCertificate(vec![1, 2, 3]);
    let status = super::authenticate_peer_identity(&authenticator, &certificate)
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::Unauthenticated);
    assert_eq!(authenticator.calls.load(Ordering::SeqCst), 1);
}

struct RejectingPeerAuthenticator(super::ReplicationPeerAuthenticationError);

#[async_trait::async_trait]

impl super::ReplicationPeerAuthenticator for RejectingPeerAuthenticator {
    async fn authenticate(
        &self,
        _certificate: &klights_types::TlsClientCertificate,
    ) -> Result<super::ReplicationPeerIdentity, super::ReplicationPeerAuthenticationError> {
        Err(self.0.clone())
    }
}

#[tokio::test]
async fn replication_peer_authentication_errors_preserve_tonic_categories() {
    let cases = [
        (
            super::ReplicationPeerAuthenticationError::Rejected {
                message: "rejected".to_string(),
            },
            tonic::Code::Unauthenticated,
        ),
        (
            super::ReplicationPeerAuthenticationError::DependencyFailure {
                message: "dependency".to_string(),
            },
            tonic::Code::Unavailable,
        ),
        (
            super::ReplicationPeerAuthenticationError::InternalFailure {
                message: "internal".to_string(),
            },
            tonic::Code::Internal,
        ),
    ];
    let certificate = klights_types::TlsClientCertificate(vec![1, 2, 3]);
    for (error, expected) in cases {
        let authenticator = RejectingPeerAuthenticator(error);
        assert_eq!(
            super::authenticate_peer_identity(&authenticator, &certificate)
                .await
                .unwrap_err()
                .code(),
            expected
        );
    }
}

struct RejectingCredentialIssuer(super::ControlplaneCredentialError);

#[async_trait::async_trait]

impl super::ControlplaneCredentialIssuer for RejectingCredentialIssuer {
    async fn sign_server_csr(
        &self,
        _ca_cert_pem: &str,
        _ca_key_pem: &str,
        _csr_pem: Vec<u8>,
    ) -> Result<String, super::ControlplaneCredentialError> {
        unreachable!("encryption mapping test must not sign")
    }

    async fn encrypt_key_material(
        &self,
        _join_token: &str,
        _plaintext: &[u8],
    ) -> Result<(Vec<u8>, Vec<u8>), super::ControlplaneCredentialError> {
        Err(self.0.clone())
    }
}

#[tokio::test]
async fn controlplane_credential_errors_preserve_tonic_categories() {
    let cases = [
        (
            super::ControlplaneCredentialError::Rejected {
                message: "rejected".to_string(),
            },
            tonic::Code::InvalidArgument,
        ),
        (
            super::ControlplaneCredentialError::DependencyFailure {
                message: "dependency".to_string(),
            },
            tonic::Code::Unavailable,
        ),
        (
            super::ControlplaneCredentialError::InternalFailure {
                message: "internal".to_string(),
            },
            tonic::Code::Internal,
        ),
    ];
    for (error, expected) in cases {
        let issuer = RejectingCredentialIssuer(error);
        assert_eq!(
            super::encrypt_controlplane_key_material(
                &issuer,
                "test operation",
                "join-token",
                b"secret",
            )
            .await
            .unwrap_err()
            .code(),
            expected
        );
    }
}

#[test]
fn caller_node_authority_non_node_identity_is_unrestricted() {
    let identity = super::ReplicationPeerIdentity {
        username: "admin".to_string(),
        groups: vec!["system:masters".to_string()],
    };
    assert!(matches!(
        super::node_authority_from_identity(&identity),
        super::CallerAuthority::Unrestricted
    ));
}

#[test]
fn caller_node_authority_extracts_node_name() {
    let identity = super::ReplicationPeerIdentity {
        username: "system:node:worker-7".to_string(),
        groups: vec!["system:nodes".to_string()],
    };
    match super::node_authority_from_identity(&identity) {
        super::CallerAuthority::Node(name) => assert_eq!(name, "worker-7"),
        super::CallerAuthority::Unrestricted => panic!("node cert must be node-bound"),
    }
}

#[test]
fn caller_node_authority_controlplane_node_is_unrestricted() {
    let identity = super::ReplicationPeerIdentity {
        username: "system:node:cp1".to_string(),
        groups: vec![
            "system:nodes".to_string(),
            "system:controlplanes".to_string(),
        ],
    };
    assert!(matches!(
        super::node_authority_from_identity(&identity),
        super::CallerAuthority::Unrestricted
    ));
}

#[test]
fn enforce_node_authority_matrix() {
    assert!(super::enforce_node_authority(&super::CallerAuthority::Unrestricted, "any").is_ok());
    assert!(
        super::enforce_node_authority(&super::CallerAuthority::Node("w1".to_string()), "w1")
            .is_ok()
    );
    for operation in [
        "renew_node_lease",
        "allocate_node_subnet",
        "observe_peer_endpoint",
        "list_pod_cleanup_intents_for_node",
        "delete_pod_cleanup_intent",
    ] {
        let err =
            super::enforce_node_authority(&super::CallerAuthority::Node("w1".to_string()), "w2")
                .expect_err("node may not act for another node");
        assert_eq!(
            err.code(),
            tonic::Code::PermissionDenied,
            "{operation} must reject a mismatched node identity"
        );
    }
}

// ── CRIT-1: raft RPC authentication ──

#[test]
fn projected_token_server_error_mapping_preserves_binding_and_authority_classes() {
    use klights_leader_api::ProjectedServiceAccountTokenError as Error;

    for (error, expected_code, expected_message) in [
        (
            Error::NotLeader,
            tonic::Code::FailedPrecondition,
            "not raft leader",
        ),
        (
            Error::Unauthorized,
            tonic::Code::PermissionDenied,
            "projected token issuance requires the node identity bound to the Pod",
        ),
        (
            Error::binding_mismatch("Pod UID changed"),
            tonic::Code::Aborted,
            "Pod UID changed",
        ),
    ] {
        let status = super::projected_token_error_to_status(error);
        assert_eq!(status.code(), expected_code);
        assert_eq!(status.message(), expected_message);
    }
}

#[test]
fn validate_join_metadata_accepts_enabled_root_and_rootless() {
    let root = validate_join_metadata(&valid_join()).unwrap();
    assert_eq!(root.node_name(), "worker-1");

    let mut rootless = valid_join();
    rootless.dataplane_mode = "rootless".to_string();
    assert!(validate_join_metadata(&rootless).is_ok());
}

#[test]
fn worker_handshake_rejects_pre_v3_before_join_admission() {
    let current = valid_join();
    require_worker_command_codec_v3(&current).expect("v3 worker is admitted");

    let mut old_worker = current;
    old_worker.command_codec_version = klights_cluster_core::COMMAND_CODEC_VERSION - 1;
    let status = require_worker_command_codec_v3(&old_worker)
        .expect_err("a pre-v3 worker must fail the stream handshake");
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
}

#[test]
fn validate_join_metadata_rejects_missing_enabled_wireguard_fields() {
    let mut missing_key = valid_join();
    missing_key.dataplane_public_key.clear();
    assert!(
        validate_join_metadata(&missing_key)
            .unwrap_err()
            .to_string()
            .contains("public key")
    );

    let mut missing_endpoint = valid_join();
    missing_endpoint.dataplane_endpoint.clear();
    assert!(
        validate_join_metadata(&missing_endpoint)
            .unwrap_err()
            .to_string()
            .contains("endpoint")
    );

    let mut missing_port = valid_join();
    missing_port.dataplane_port = 0;
    assert!(
        validate_join_metadata(&missing_port)
            .unwrap_err()
            .to_string()
            .contains("port")
    );
}

#[test]
fn validate_join_metadata_defaults_empty_encryption_to_enabled() {
    let mut join = valid_join();
    join.dataplane_encryption.clear();
    let metadata = validate_join_metadata(&join).unwrap();
    assert_eq!(
        metadata.encryption(),
        klights_leader_api::DataplaneEncryption::WireGuard
    );
}

#[test]
fn validate_join_metadata_accepts_explicit_disabled_without_public_key() {
    let mut join = valid_join();
    join.dataplane_encryption = "disabled".to_string();
    join.dataplane_public_key.clear();
    join.dataplane_port = 0;
    let metadata = validate_join_metadata(&join).unwrap();
    assert_eq!(
        metadata.encryption(),
        klights_leader_api::DataplaneEncryption::Direct
    );
    assert!(metadata.public_key().is_none());
}

#[test]
fn watch_heartbeat_proto_is_a_bookmark_carrying_the_cursor_rv() {
    // bug-grpc: the idle heartbeat must be a BOOKMARK that carries the
    // stream cursor RV so the worker treats it as liveness + a resume
    // point, and it must round-trip through the normal event proto shape
    // (the client decode requires a `resource`).
    let resume_position = WatchReplayPosition {
        resource_version: 4242,
        event_id: 77,
        resource_version_filter_through_event_id: 0,
    };
    let event = super::watch_heartbeat_proto("v1", "Pod", 4242, resume_position);
    assert_eq!(event.event_type, "BOOKMARK");
    assert_eq!(
        event
            .resume_position
            .as_ref()
            .map(|position| position.event_id),
        Some(77)
    );
    let resource = event.resource.expect("heartbeat must carry a resource");
    assert_eq!(resource.resource_version, 4242);
    let data: serde_json::Value =
        serde_json::from_slice(&resource.data_json).expect("heartbeat data_json must decode");
    assert_eq!(
        data.pointer("/metadata/resourceVersion")
            .and_then(|v| v.as_str()),
        Some("4242"),
        "bookmark metadata must carry the cursor RV as the resume point"
    );
    assert_eq!(data.get("kind").and_then(|v| v.as_str()), Some("Pod"));
}
// ─────────────────────────────────────────────────────────────────
// memory-improvement.md §10 P1 — streaming snapshot serve path.
// ─────────────────────────────────────────────────────────────────
