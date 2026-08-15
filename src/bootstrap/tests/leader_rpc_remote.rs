use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt as _;
use serde_json::json;

use crate::bootstrap::composition_tests::leader_rpc::support::SqliteTestStore;
use klights_cluster_core::Resource;
use klights_cluster_core::ResourcePreconditions;
use klights_cluster_core::command::StorageCommand;
use klights_leader_api::JoinRole;
use klights_leader_api::OutboxDeliveryError as OutboxApplyError;
use klights_leader_api::{
    CacheReadinessError, CacheReadinessRequest, LeaderCacheReadiness, LeaderNetworkTopologyQuery,
    LeaderNodeSubnetAllocation, LeaderResourceQuery, LeaderWatch, NodeDataplaneQuery,
    NodeSubnetAllocationError, NodeSubnetAllocationRequest, NodeSubnetQuery, PeerSubnetsQuery,
    ResourceEvent, ResourceGetRequest, ResourceListRequest, ResourceListScope,
    ResourceQueryConsistency, WatchEventType, WatchRequest, pod_get_request,
};
use klights_leader_api::{LeaderOutboxDelivery, OutboxDeliveryRequest};
use klights_leader_rpc::client::RemoteApiClient;
use klights_leader_rpc::client::{GrpcClientConfig, JoinDataplaneMetadata, ReplicationGrpcClient};
use klights_supervisor::{TaskCategoryConfig, TaskSupervisor};
use klights_types::ResourceKey;
use klights_watch::RemoteInformerCache;

struct TestRemote {
    client: RemoteApiClient,
    cache: Arc<
        crate::bootstrap::composition_adapters::remote_informer_cache_adapter::WatchCacheAdapter,
    >,
}

impl std::ops::Deref for TestRemote {
    type Target = RemoteApiClient;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

fn remote_for_tests(node_name: &str) -> TestRemote {
    let cache = Arc::new(
        crate::bootstrap::composition_adapters::remote_informer_cache_adapter::WatchCacheAdapter::new(),
    );
    TestRemote {
        client: RemoteApiClient::without_transport(node_name, cache.clone()),
        cache,
    }
}

impl TestRemote {
    async fn cache_insert_pod(&self, pod: Resource) {
        self.cache.insert(pod).await;
    }

    async fn cache_prime_scope(&self, scope: CacheReadinessRequest) {
        let request = ResourceListRequest::try_new(
            scope.api_version().to_string(),
            scope.kind().to_string(),
            scope
                .namespace()
                .map(|namespace| ResourceListScope::Namespace(namespace.to_owned()))
                .unwrap_or(ResourceListScope::AllNamespaces),
            scope.label_selector().map(str::to_owned),
            scope.field_selector().map(str::to_owned),
            None,
            None,
            ResourceQueryConsistency::Cached,
        )
        .expect("cache readiness scope is valid");
        self.cache
            .replace_scope(
                &request,
                Vec::new(),
                klights_cluster_core::WatchReplayPosition::from_resource_version(0),
            )
            .await
            .expect("test cache scope baseline");
        self.cache
            .mark_ready(scope)
            .await
            .expect("test cache scope readiness");
    }

    async fn cache_clear_scope_for_test(&self, scope: &CacheReadinessRequest) {
        self.cache.clear_ready(scope).await;
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

/// Self-signed `system:node:<name>` certificate (DER) for simulating the
/// mTLS node identity in the in-process test harness.
fn test_node_cert_der(node_name: &str) -> Vec<u8> {
    use rcgen::{CertificateParams, DnType, KeyPair};
    let mut params = CertificateParams::default();
    params.distinguished_name = rcgen::DistinguishedName::new();
    params
        .distinguished_name
        .push(DnType::CommonName, format!("system:node:{node_name}"));
    params
        .distinguished_name
        .push(DnType::OrganizationName, "system:nodes".to_string());
    let key_pair = KeyPair::generate().unwrap();
    params.self_signed(&key_pair).unwrap().der().to_vec()
}

async fn remote_client_and_leader_db() -> (
    RemoteApiClient,
    SqliteTestStore,
    tokio::task::JoinHandle<()>,
) {
    remote_client_and_leader_db_with_node_names("worker-1".to_string(), "worker-1".to_string())
        .await
}

async fn remote_client_and_leader_db_with_node_names(
    remote_node_name: String,
    grpc_node_name: String,
) -> (
    RemoteApiClient,
    SqliteTestStore,
    tokio::task::JoinHandle<()>,
) {
    let concrete_db =
        crate::bootstrap::composition::cluster_store::selector::canonical_sqlite_fixture()
            .await
            .unwrap();
    let passive_reads =
            crate::bootstrap::composition_tests::leader_rpc::support::IntegrationLeaderRpcComposition::passive_reads_for(
                &concrete_db,
            );
    let db: SqliteTestStore = Arc::new(concrete_db.clone());
    crate::bootstrap::cluster_meta::ensure_cluster_metadata_sqlite(db.as_ref())
        .await
        .unwrap();
    let token = crate::bootstrap::bootstrap_token::ensure_worker_bootstrap_token(db.as_ref())
        .await
        .unwrap();
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let composition =
            crate::bootstrap::composition_tests::leader_rpc::support::IntegrationLeaderRpcComposition::new(
                db.clone(),
                Arc::new(concrete_db.clone()),
                concrete_db
                    .clone()
                    .focused_committed_apply(),
                concrete_db.clone().focused_read_store(),
            );
    let service = Arc::new(composition.replication_service(supervisor.clone()));
    let app = composition.mount_service_full(
        axum::Router::new(),
        service,
        Some(passive_reads),
        None,
        None,
        None,
        None,
        "",
        None,
        None,
        None,
        None,
        None,
        klights_leader_rpc::transport_policy::GrpcTransportPolicy::shared_default(),
    );
    // Simulate the mTLS identity edge: the shared test server provides the
    // production-required TLS 1.3 transport, while this middleware injects
    // the gRPC transport's node cert so node-scoped RPCs (NodeRestriction)
    // see the same authenticated identity.
    let grpc_node_cert = test_node_cert_der(&grpc_node_name);
    let app = app.layer(axum::middleware::from_fn(
        move |mut request: axum::extract::Request, next: axum::middleware::Next| {
            let grpc_node_cert = grpc_node_cert.clone();
            async move {
                request
                    .extensions_mut()
                    .insert(klights_types::TlsClientCertificate(grpc_node_cert));
                next.run(request).await
            }
        },
    ));
    let (endpoint, handle) =
            crate::bootstrap::composition_tests::leader_rpc::support::IntegrationLeaderRpcComposition::serve_tls_test_app(
                app,
            )
            .await;
    let grpc = Arc::new(
        ReplicationGrpcClient::connect(
            GrpcClientConfig {
                leader_endpoint: endpoint,
                token,
                node_name: grpc_node_name,
                role: JoinRole::Worker,
                dataplane: dataplane(),
                ca_cert_path: None,
                skip_ca: true,
                client_cert_pem: None,
                client_key_pem: None,
            },
            supervisor.clone(),
            klights_leader_rpc::transport_policy::GrpcTransportPolicy::shared_default(),
            klights_leader_rpc::client::NodeControlRuntimes::new(
                klights_leader_rpc::client::NodeExecCapability::Unavailable,
                klights_leader_rpc::client::NodeLogCapability::Unavailable,
                klights_leader_rpc::client::NodeMetricsCapability::Unavailable,
            ),
        )
        .await
        .unwrap(),
    );
    (
            RemoteApiClient::from_grpc(
                grpc,
                supervisor,
                remote_node_name,
                Arc::new(
                    crate::bootstrap::composition_adapters::remote_informer_cache_adapter::WatchCacheAdapter::new(),
                ),
            ),
            db,
            handle,
        )
}

fn make_pod(ns: &str, name: &str, uid: &str, node_name: &str, phase: &str) -> Resource {
    let data = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": ns,
            "name": name,
            "uid": uid
        },
        "spec": {
            "nodeName": node_name,
            "containers": [{"name": "app", "image": "nginx"}]
        },
        "status": {
            "phase": phase
        }
    });
    klights_cluster_core::Resource {
        id: 0,
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some(ns.to_string()),
        name: name.to_string(),
        uid: uid.to_string(),
        resource_version: 1,
        data: std::sync::Arc::new(data),
    }
}

fn pod_status_payload(uid: &str) -> Bytes {
    let command = StorageCommand::UpdateStatus {
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "web".to_string(),
        status: json!({"phase": "Running"}),
        expected_rv: None,
        preconditions: ResourcePreconditions {
            uid: Some(uid.to_string()),
            resource_version: None,
        },
        observed_status_stamp: None,
    };
    Bytes::from(
        klights_leader_rpc::storage_wire_codec::encode_outbox_payload_protobuf(
            &klights_cluster_core::OutboxPayload::new(command),
        )
        .expect("encode outbox payload"),
    )
}

#[tokio::test]
async fn grpc_cache_read_primes_unready_scope_before_reporting_miss() {
    let (client, db, handle) = remote_client_and_leader_db().await;
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "web",
        (*make_pod("default", "web", "uid-1", "worker-1", "Pending").data).clone(),
    )
    .await
    .unwrap();

    let pod = client
        .get_resource(
            pod_get_request("default", "web", ResourceQueryConsistency::Cached)
                .expect("valid Pod request"),
        )
        .await
        .expect("remote cache-prime get pod")
        .expect("unready cache scope should be synchronously primed before reporting absence");
    assert_eq!(pod.uid, "uid-1");

    db.update_status_only_with_preconditions(
        "v1",
        "Pod",
        Some("default"),
        "web",
        json!({"phase": "Running"}),
        ResourcePreconditions {
            uid: Some("uid-1".to_string()),
            resource_version: None,
        },
    )
    .await
    .unwrap();
    let cached = client
        .get_resource(
            pod_get_request("default", "web", ResourceQueryConsistency::Cached)
                .expect("valid Pod request"),
        )
        .await
        .expect("remote cached pod")
        .expect("pod should remain cached");
    assert_eq!(
        cached
            .data
            .pointer("/status/phase")
            .and_then(|value| value.as_str()),
        Some("Pending"),
        "cache hit should not perform an unnecessary strong read"
    );
    handle.abort();
}

#[tokio::test]
async fn missing_grpc_apply_outbox_is_retryable_not_acknowledged() {
    let client = remote_for_tests("worker-1");

    let err = client
        .deliver_outbox(
            OutboxDeliveryRequest::try_new(
                "missing-grpc-watermarked-status",
                klights_leader_api::OutboxDeliveryOperation::PodStatus,
                Arc::<[u8]>::from(pod_status_payload("uid-1").to_vec()),
                "worker-client",
                7,
                1,
            )
            .expect("valid delivery request"),
        )
        .await
        .expect_err("missing gRPC must not acknowledge a sequenced outbox row");

    assert!(
        matches!(&err, OutboxApplyError::Retryable(message) if message.contains("missing gRPC transport")),
        "missing gRPC should be a retryable dispatcher error, got {err:?}"
    );
}

#[tokio::test]
async fn grpc_apply_outbox_node_identity_mismatch_is_terminal() {
    let (client, _db, handle) =
        remote_client_and_leader_db_with_node_names("worker-1".to_string(), "worker-2".to_string())
            .await;

    let err = client
        .deliver_outbox(
            OutboxDeliveryRequest::try_new(
                "identity-mismatch",
                klights_leader_api::OutboxDeliveryOperation::PodStatus,
                Arc::<[u8]>::from(pod_status_payload("uid-1").to_vec()),
                "worker-client",
                7,
                1,
            )
            .expect("valid delivery request"),
        )
        .await
        .expect_err("identity mismatch must not remain in durable retry");

    assert!(
        matches!(&err, OutboxApplyError::ConflictTerminal(message) if message.contains("RemoteApiClient node identity")),
        "identity mismatch must be terminal, got {err:?}"
    );
    handle.abort();
}

#[tokio::test]
async fn grpc_apply_outbox_uid_mismatch_propagates() {
    let (client, db, handle) = remote_client_and_leader_db().await;
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "web",
        (*make_pod("default", "web", "uid-1", "worker-1", "Pending").data).clone(),
    )
    .await
    .unwrap();

    let err = client
        .deliver_outbox(
            OutboxDeliveryRequest::try_new(
                "uid-mismatch",
                klights_leader_api::OutboxDeliveryOperation::PodStatus,
                Arc::<[u8]>::from(pod_status_payload("uid-2").to_vec()),
                "client",
                1,
                1,
            )
            .expect("valid delivery request"),
        )
        .await
        .expect_err("unwatermarked leader uid mismatch must propagate");
    assert!(matches!(err, OutboxApplyError::UidMismatch { .. }));
    handle.abort();
}

#[tokio::test]
async fn grpc_focused_pod_watch_streams_leader_events() {
    let (client, db, handle) = remote_client_and_leader_db().await;
    let mut stream = client
        .watch_resources(
            WatchRequest::try_new(
                "v1",
                "Pod",
                None,
                None,
                Some("spec.nodeName=worker-1".to_string()),
                None,
                None,
            )
            .expect("valid Pod watch"),
        )
        .await
        .expect("open remote pod watch");
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "watched",
        (*make_pod("default", "watched", "uid-watch", "worker-1", "Pending").data).clone(),
    )
    .await
    .unwrap();

    let event = stream
        .next()
        .await
        .expect("watch should yield")
        .expect("watch event should decode");
    assert_eq!(event.resource().name, "watched");
    assert_eq!(event.resource().uid, "uid-watch");
    handle.abort();
}

#[tokio::test]
async fn grpc_network_metadata_uses_typed_unary_rpcs() {
    let (client, db, handle) = remote_client_and_leader_db().await;

    let subnet = client
        .allocate_node_subnet(
            NodeSubnetAllocationRequest::try_new("worker-1", "10.42.0.0/16", "192.0.2.20")
                .expect("valid request"),
        )
        .await
        .expect("allocate worker subnet through typed gRPC")
        .into_subnet();
    assert_eq!(subnet.node_name(), "worker-1");
    assert_eq!(subnet.subnet(), "10.42.0.0/24");

    let fetched = client
        .get_node_subnet(NodeSubnetQuery::try_new("worker-1").expect("valid query"))
        .await
        .expect("get worker subnet through typed gRPC")
        .into_option()
        .expect("worker subnet should exist");
    assert_eq!(fetched, subnet);

    let peer_error = client
        .allocate_node_subnet(
            NodeSubnetAllocationRequest::try_new("worker-2", "10.42.0.0/16", "192.0.2.21")
                .expect("valid request"),
        )
        .await
        .expect_err("worker certificate must not allocate a peer subnet");
    assert!(matches!(
        peer_error,
        NodeSubnetAllocationError::Unauthorized { .. }
    ));
    let peers = client
        .list_peer_subnets(PeerSubnetsQuery::try_new("worker-1").expect("valid query"))
        .await
        .expect("list peer subnets through typed gRPC")
        .into_vec();
    assert!(peers.is_empty());

    let stored_metadata = db
        .get_node_dataplane("worker-1")
        .await
        .expect("dataplane metadata lookup")
        .expect("join should have stored worker dataplane metadata");
    let fetched_metadata = client
        .get_node_dataplane(NodeDataplaneQuery::try_new("worker-1").expect("valid query"))
        .await
        .expect("get worker dataplane metadata through typed gRPC")
        .into_option();
    assert_eq!(
        fetched_metadata,
        Some(
            crate::bootstrap::leader_conversions::topology::focused_dataplane(stored_metadata)
                .expect("valid focused metadata"),
        )
    );

    handle.abort();
}

#[tokio::test]
async fn grpc_watch_replays_events_after_start_resource_version() {
    let (client, db, handle) = remote_client_and_leader_db().await;
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "old",
        (*make_pod("default", "old", "uid-old", "worker-1", "Pending").data).clone(),
    )
    .await
    .unwrap();
    let start_rv = db.get_current_resource_version().await.unwrap();
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "missed",
        (*make_pod("default", "missed", "uid-missed", "worker-1", "Pending").data).clone(),
    )
    .await
    .unwrap();

    let mut stream = client
        .watch_resources(
            WatchRequest::try_new(
                "v1",
                "Pod",
                None,
                None,
                Some("spec.nodeName=worker-1".to_string()),
                Some(start_rv),
                None,
            )
            .expect("valid continuation watch"),
        )
        .await
        .expect("open continuation watch");
    let event = tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
        .await
        .expect("continuation watch should replay missed event")
        .expect("stream should yield")
        .expect("watch event should decode");
    assert!(
        event
            .resume_position()
            .is_some_and(|position| position.event_id > 0),
        "gRPC watch events must carry an apply-order resume position"
    );
    let pod_name = event
        .resource()
        .data
        .pointer("/metadata/name")
        .and_then(|value| value.as_str());
    assert_eq!(pod_name, Some("missed"));
    handle.abort();
}

#[tokio::test]
async fn grpc_list_position_round_trips_into_lossless_watch_resume() {
    let (client, db, handle) = remote_client_and_leader_db().await;
    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "before-list",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"namespace": "default", "name": "before-list"}
        }),
    )
    .await
    .unwrap();
    let list_req = ResourceListRequest::try_new(
        "v1",
        "ConfigMap",
        ResourceListScope::Namespace("default".to_string()),
        None,
        None,
        None,
        None,
        ResourceQueryConsistency::LeaderFresh,
    )
    .expect("valid ConfigMap list request");
    let list = client
        .list_resources(list_req)
        .await
        .expect("list through gRPC");
    let list_position = list
        .watch_replay_position()
        .expect("gRPC LIST must preserve its atomic replay position");
    assert!(list_position.event_id > 0);

    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "after-list",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"namespace": "default", "name": "after-list"}
        }),
    )
    .await
    .unwrap();
    let mut stream = client
        .watch_resources(
            WatchRequest::try_new(
                "v1",
                "ConfigMap",
                Some("default".to_string()),
                None,
                None,
                Some(list.resource_version()),
                Some(list_position),
            )
            .expect("valid positioned ConfigMap watch"),
        )
        .await
        .expect("resume watch from atomic list position");
    let event = tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
        .await
        .expect("post-list event must replay")
        .expect("stream should yield")
        .expect("event should decode");
    assert_eq!(event.resource().data["metadata"]["name"], "after-list");
    assert!(
        event
            .resume_position()
            .is_some_and(|position| position.event_id > list_position.event_id)
    );
    handle.abort();
}

#[tokio::test]
async fn grpc_paginated_list_preserves_namespace_scope_and_pinned_mode_on_page_two() {
    let (client, db, handle) = remote_client_and_leader_db().await;
    for name in ["cm-a", "cm-b", "cm-c"] {
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            name,
            json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"namespace": "default", "name": name}
            }),
        )
        .await
        .unwrap();
    }
    let first = client
        .list_resources(
            ResourceListRequest::try_new(
                "v1",
                "ConfigMap",
                ResourceListScope::Namespace("default".to_string()),
                None,
                None,
                Some(1),
                None,
                ResourceQueryConsistency::LeaderFresh,
            )
            .unwrap(),
        )
        .await
        .expect("first direct RPC page");
    assert_eq!(
        first.remaining_item_count(),
        Some(2),
        "unfiltered RPC page one must preserve the exact datastore remaining count"
    );
    let continuation = first
        .continue_token()
        .expect("first page must carry typed private continuation")
        .to_string();
    let second = client
        .list_resources(
            ResourceListRequest::try_new_with_continuation_mode(
                "v1",
                "ConfigMap",
                ResourceListScope::Namespace("default".to_string()),
                None,
                None,
                Some(1),
                Some(continuation),
                klights_leader_api::ResourceListContinuationMode::Pinned,
                ResourceQueryConsistency::LeaderFresh,
            )
            .unwrap(),
        )
        .await
        .expect("pinned page two must retain direct RPC scope/mode");
    assert_eq!(
        second
            .items()
            .iter()
            .map(|item| item.name.as_str())
            .collect::<Vec<_>>(),
        ["cm-b"]
    );
    assert_eq!(second.resource_version(), first.resource_version());
    assert_eq!(
        second.remaining_item_count(),
        Some(1),
        "unfiltered pinned RPC page two must preserve the exact datastore remaining count"
    );
    assert!(second.continue_token().is_some());
    handle.abort();
}

#[tokio::test]
async fn grpc_custom_resource_list_accepts_retained_served_versions_across_pages() {
    let (client, db, handle) = remote_client_and_leader_db().await;
    db.create_resource(
        "apiextensions.k8s.io/v1",
        "CustomResourceDefinition",
        None,
        "widgets.example.com",
        json!({
            "apiVersion": "apiextensions.k8s.io/v1",
            "kind": "CustomResourceDefinition",
            "metadata": {"name": "widgets.example.com", "uid": "widgets-crd"},
            "spec": {
                "group": "example.com", "scope": "Namespaced",
                "names": {"kind": "Widget", "plural": "widgets", "singular": "widget"},
                "versions": [
                    {"name": "v1", "served": true, "storage": true},
                    {"name": "v2", "served": true, "storage": false}
                ]
            }
        }),
    )
    .await
    .unwrap();
    for (api_version, name) in [
        ("example.com/v1", "a-stored-v1"),
        ("example.com/v2", "z-retained-v2"),
    ] {
        db.create_resource(
            api_version,
            "Widget",
            Some("default"),
            name,
            json!({
                "apiVersion": api_version, "kind": "Widget",
                "metadata": {"namespace": "default", "name": name}
            }),
        )
        .await
        .unwrap();
    }
    let first = client
        .list_resources(
            ResourceListRequest::try_new(
                "example.com/v2",
                "Widget",
                ResourceListScope::Namespace("default".to_string()),
                None,
                None,
                Some(1),
                None,
                ResourceQueryConsistency::LeaderFresh,
            )
            .unwrap()
            .with_custom_resource_identity(
                klights_leader_api::CustomResourceListIdentity::try_new(
                    "example.com",
                    "widgets",
                    "v2",
                )
                .unwrap(),
            )
            .unwrap(),
        )
        .await
        .expect("remote v2 CRD page one must accept the storage-version item");
    let continuation = first
        .continue_token()
        .expect("page one continuation")
        .to_string();
    let second = client
        .list_resources(
            ResourceListRequest::try_new_with_continuation_mode(
                "example.com/v2",
                "Widget",
                ResourceListScope::Namespace("default".to_string()),
                None,
                None,
                Some(1),
                Some(continuation),
                klights_leader_api::ResourceListContinuationMode::Pinned,
                ResourceQueryConsistency::LeaderFresh,
            )
            .unwrap()
            .with_custom_resource_identity(
                klights_leader_api::CustomResourceListIdentity::try_new(
                    "example.com",
                    "widgets",
                    "v2",
                )
                .unwrap(),
            )
            .unwrap(),
        )
        .await
        .expect("remote v2 CRD page two must accept a retained served-version item");
    let served_versions = [
        first.items()[0].api_version.as_str(),
        second.items()[0].api_version.as_str(),
    ];
    assert!(served_versions.contains(&"example.com/v1"));
    assert!(served_versions.contains(&"example.com/v2"));
    handle.abort();
}

#[tokio::test]
async fn paginated_leader_fresh_list_never_marks_a_partial_scope_cache_complete() {
    let (client, db, handle) = remote_client_and_leader_db().await;
    for name in ["cm-a", "cm-b", "cm-c"] {
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            name,
            json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"namespace": "default", "name": name}
            }),
        )
        .await
        .unwrap();
    }
    let partial = client
        .list_resources(
            ResourceListRequest::try_new(
                "v1",
                "ConfigMap",
                ResourceListScope::Namespace("default".to_string()),
                None,
                None,
                Some(1),
                None,
                ResourceQueryConsistency::LeaderFresh,
            )
            .unwrap(),
        )
        .await
        .expect("leader-fresh first page");
    assert_eq!(partial.items().len(), 1);
    let cached_full_scope = client
        .list_resources(
            ResourceListRequest::try_new(
                "v1",
                "ConfigMap",
                ResourceListScope::Namespace("default".to_string()),
                None,
                None,
                None,
                None,
                ResourceQueryConsistency::Cached,
            )
            .unwrap(),
        )
        .await
        .expect("cached request must synchronously prime the complete scope, not reuse page one");
    assert_eq!(
        cached_full_scope
            .items()
            .iter()
            .map(|item| item.name.as_str())
            .collect::<Vec<_>>(),
        ["cm-a", "cm-b", "cm-c"]
    );
    handle.abort();
}

#[tokio::test]
async fn cache_never_answers_historical_or_custom_resource_lists() {
    let remote = remote_for_tests("worker-cache-contract");
    let scope = CacheReadinessRequest::try_new(
        "v1".to_string(),
        "ConfigMap".to_string(),
        Some("default".to_string()),
        None,
        None,
    )
    .unwrap();
    remote.cache_prime_scope(scope).await;

    // The cache is deliberately ready but has no leader transport. Both calls
    // must therefore fail as direct reads instead of silently returning the
    // ready live cache for an Exact or root-composed custom-resource request.
    let exact = ResourceListRequest::try_new(
        "v1",
        "ConfigMap",
        ResourceListScope::Namespace("default".to_string()),
        None,
        None,
        None,
        None,
        ResourceQueryConsistency::Cached,
    )
    .unwrap()
    .with_resource_version_match(klights_leader_api::ResourceListResourceVersionMatch::Exact(
        1,
    ))
    .unwrap();
    assert!(remote.list_resources(exact).await.is_err());

    let custom = ResourceListRequest::try_new(
        "example.com/v1",
        "Widget",
        ResourceListScope::Namespace("default".to_string()),
        None,
        Some("spec.rank=7".to_string()),
        None,
        None,
        ResourceQueryConsistency::Cached,
    )
    .unwrap()
    .with_custom_resource_identity(
        klights_leader_api::CustomResourceListIdentity::try_new("example.com", "widgets", "v1")
            .unwrap(),
    )
    .unwrap();
    assert!(remote.list_resources(custom).await.is_err());
}

#[tokio::test]
async fn watch_continuation_after_disconnect() {
    // Tests that the informer cache can be rebuilt after a watch disconnect.
    // Simulates: cache primed, disconnect clears scope, re-list repopulates.
    let client = remote_for_tests("worker-1");

    let pod_scope =
        CacheReadinessRequest::try_new("v1", "Pod", None, None, None).expect("valid cache scope");

    // Prime the scope and insert data
    client.cache_prime_scope(pod_scope.clone()).await;
    client
        .cache_insert_pod(make_pod("default", "web", "uid-1", "worker-1", "Running"))
        .await;

    // Verify cache is ready
    assert!(client.wait_cache_ready(pod_scope.clone()).await.is_ok());

    // Simulate 410 Gone: clear scope and re-prime
    // In production, RemoteApiClient would re-list and re-prime;
    // here we test that the rebuilt cache works correctly.
    client.cache_clear_scope_for_test(&pod_scope).await;
    assert!(client.wait_cache_ready(pod_scope.clone()).await.is_err());

    // Re-prime and re-insert
    client.cache_prime_scope(pod_scope.clone()).await;
    client
        .cache_insert_pod(make_pod("default", "web", "uid-2", "worker-1", "Running"))
        .await;
    assert!(client.wait_cache_ready(pod_scope).await.is_ok());
    let pod = client
        .get_resource(
            pod_get_request("default", "web", ResourceQueryConsistency::Cached)
                .expect("valid Pod request"),
        )
        .await
        .unwrap();
    assert!(pod.is_some());
    assert_eq!(pod.unwrap().uid, "uid-2");
}

#[tokio::test]
async fn unary_fallback_on_cache_miss() {
    // Tests that when the cache misses, the client signals the result
    // correctly (None when not found). In production this would trigger
    // a unary gRPC GetResource; here the cache simply returns None.
    let client = remote_for_tests("worker-1");

    // No pod in cache → cache miss → returns None
    let result = client
        .get_resource(
            pod_get_request("default", "nonexistent", ResourceQueryConsistency::Cached)
                .expect("valid Pod request"),
        )
        .await;
    assert!(result.is_ok());
    assert!(result.unwrap().is_none(), "cache miss should return None");

    // Insert pod → cache hit
    client
        .cache_insert_pod(make_pod("default", "web", "uid-1", "worker-1", "Running"))
        .await;
    let result = client
        .get_resource(
            pod_get_request("default", "web", ResourceQueryConsistency::Cached)
                .expect("valid Pod request"),
        )
        .await;
    assert!(result.is_ok());
    assert!(result.unwrap().is_some(), "cache hit should return pod");
}

#[tokio::test]
async fn cache_based_get_resource_returns_primed_value() {
    let client = remote_for_tests("worker-1");
    let scope =
        CacheReadinessRequest::try_new("v1", "Pod", Some("default".to_string()), None, None)
            .expect("valid cache scope");
    let pod = make_pod("default", "web", "uid-1", "worker-1", "Running");
    client.cache_prime_scope(scope).await;
    client.cache_insert_pod(pod.clone()).await;

    let fetched = client
        .get_resource(
            ResourceGetRequest::try_new(
                ResourceKey {
                    api_version: "v1".to_string(),
                    kind: "Pod".to_string(),
                    namespace: Some("default".to_string()),
                    name: "web".to_string(),
                },
                ResourceQueryConsistency::Cached,
            )
            .expect("valid Pod request"),
        )
        .await
        .expect("get_resource");

    assert_eq!(
        fetched.as_ref().map(|resource| resource.uid.as_str()),
        Some("uid-1")
    );
    assert_eq!(
        fetched.as_ref().map(|resource| resource.resource_version),
        Some(pod.resource_version)
    );
}

#[tokio::test]
async fn leader_fresh_get_never_falls_back_to_a_primed_cache_without_transport() {
    let client = remote_for_tests("worker-1");
    let scope =
        CacheReadinessRequest::try_new("v1", "Pod", Some("default".to_string()), None, None)
            .expect("valid cache scope");
    client.cache_prime_scope(scope).await;
    client
        .cache_insert_pod(make_pod(
            "default",
            "web",
            "stale-uid",
            "worker-1",
            "Running",
        ))
        .await;

    let error = client
        .get_resource(
            pod_get_request("default", "web", ResourceQueryConsistency::LeaderFresh)
                .expect("valid leader-fresh request"),
        )
        .await
        .expect_err("leader-fresh must fail closed without a leader transport");
    assert!(matches!(
        error,
        klights_leader_api::ResourceQueryError::Retryable { .. }
    ));
    let error = client
        .list_resources(
            ResourceListRequest::try_new(
                "v1",
                "Pod",
                ResourceListScope::Namespace("default".to_string()),
                None,
                None,
                None,
                None,
                ResourceQueryConsistency::LeaderFresh,
            )
            .unwrap(),
        )
        .await
        .expect_err("leader-fresh LIST must fail closed without a leader transport");
    assert!(matches!(
        error,
        klights_leader_api::ResourceQueryError::Retryable { .. }
    ));
}

#[tokio::test]
async fn cache_readiness_keeps_selector_scopes_distinct() {
    let client = remote_for_tests("worker-1");
    let selected = CacheReadinessRequest::try_new(
        "v1",
        "Pod",
        None,
        None,
        Some("spec.nodeName=worker-1".to_string()),
    )
    .expect("valid selected Pod scope");
    let unfiltered = CacheReadinessRequest::try_new("v1", "Pod", None, None, None)
        .expect("valid unfiltered Pod scope");

    client.cache_prime_scope(selected.clone()).await;
    assert!(client.wait_cache_ready(selected).await.is_ok());
    assert!(matches!(
        client.wait_cache_ready(unfiltered).await,
        Err(CacheReadinessError::Unavailable { .. })
    ));
}

#[tokio::test]
async fn apply_outbox_without_grpc_is_retryable() {
    let client = remote_for_tests("worker-1");

    let err = client
        .deliver_outbox(
            OutboxDeliveryRequest::try_new(
                "key-1",
                klights_leader_api::OutboxDeliveryOperation::PodStatus,
                Arc::<[u8]>::from(&b"test"[..]),
                "client",
                1,
                1,
            )
            .expect("valid delivery request"),
        )
        .await
        .expect_err("missing gRPC must not acknowledge an outbox row");
    assert!(
        matches!(&err, OutboxApplyError::Retryable(message) if message.contains("missing gRPC transport")),
        "missing gRPC should be retryable, got {err:?}"
    );
}

/// bug-grpc B2/B3: cursor-advance-only-after-safe-apply. `run_watch_driver`
/// advances its resume `next_resource_version` only after applying each
/// canonical event. This locks the direct watch-cache behavior: BOOKMARK is
/// a no-op while a resource event updates the cache before cursor advance.
#[tokio::test]
async fn informer_apply_event_gates_cursor_advance() {
    let cache = klights_watch::WatchCache::new();

    // BOOKMARK: apply is a no-op success, so its RV is a safe resume point
    // the driver may advance to.
    let bookmark = ResourceEvent::try_new(
        WatchEventType::Bookmark,
        klights_cluster_core::Resource::from_data_lossy(Arc::new(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"resourceVersion": "42"}
        }))),
        None,
    )
    .expect("valid bookmark");
    assert!(
        cache.apply_event(&bookmark).await.is_none(),
        "a BOOKMARK must apply as a no-op so its RV is a valid resume point"
    );

    // A well-formed event applies successfully (cursor may advance).
    let pod = make_pod("default", "web", "uid-1", "worker-1", "Running");
    let good = ResourceEvent::try_new(WatchEventType::Added, pod, None).expect("valid Pod event");
    assert!(
        cache.apply_event(&good).await.is_some(),
        "a well-formed event must apply so its RV becomes the resume point"
    );
}
