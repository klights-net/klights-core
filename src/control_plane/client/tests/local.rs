//! T6 step 1: `LocalApiClient` inner write gate.
//!
//! Every mutation method must consult `is_leader_rx` and refuse with
//! `WriteRejection::FollowerWrite` (or the OutboxApplyError equivalent)
//! when this node is not the elected raft leader. Reads stay allowed.
//! Promotion is a watch flip — the same instance starts accepting
//! writes the moment the receiver observes `true`.

use super::*;
use crate::bootstrap::composition_tests::support::OutboxPayload;
use crate::datastore::ReplicatedCreateOptions;
use crate::datastore::ResourcePreconditions;
use crate::datastore::{DatastoreBackend, ResourceListQuery};
use futures::StreamExt as _;
use klights_cluster_core::command::StorageCommand;
use klights_kubelet::node_outbox::payload::OutboxOperation;
use klights_leader_api::OutboxDeliveryError as OutboxApplyError;
use klights_leader_api::{
    LeaderResourceCommand, ResourceCommandError, ResourceCommandRequest, ResourceCommandResult,
    ResourceQueryError, WatchEventType,
};
use klights_types::ResourceKey;

fn pod_status_payload() -> bytes::Bytes {
    let command = StorageCommand::UpdateStatus {
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "web".to_string(),
        status: serde_json::json!({"phase": "Running"}),
        expected_rv: None,
        preconditions: ResourcePreconditions {
            uid: Some("uid-1".to_string()),
            resource_version: None,
        },
        observed_status_stamp: None,
    };
    bytes::Bytes::from(
        OutboxPayload::from_command(command)
            .encode_protobuf()
            .expect("encode pod status payload"),
    )
}

async fn make_pod(db: &crate::datastore::sqlite::Datastore) {
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "web",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"namespace": "default", "name": "web", "uid": "uid-1"},
            "spec": {"containers": [{"name": "app", "image": "nginx"}]}
        }),
    )
    .await
    .expect("create pod");
}

#[tokio::test]
async fn local_protobuf_pod_status_reconciles_json_endpoint_tables() {
    let sqlite = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let db: DatastoreHandle = Arc::new(sqlite.clone());
    let service = db
        .create_resource(
            "v1",
            "Service",
            Some("default"),
            "web",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {"namespace": "default", "name": "web", "uid": "service-uid"},
                "spec": {"selector": {"app": "web"}, "ports": [{"port": 80, "targetPort": 8080}]}
            }),
        )
        .await
        .unwrap();
    db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "web",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"namespace": "default", "name": "web", "uid": "uid-1", "labels": {"app": "web"}},
                "spec": {"nodeName": "worker-1", "containers": [{"name": "app", "image": "nginx", "ports": [{"containerPort": 8080}]}]},
                "status": {"phase": "Pending"}
            }),
        )
        .await
        .unwrap();
    let client = LocalApiClient::new(
        db.clone(),
        "worker-1".to_string(),
        crate::control_plane::client::local::always_leader_watch(),
    );
    let dispatcher =
        crate::bootstrap::controller_adapters::controller_runtime_adapter::dispatcher_for_test(
            &sqlite,
            Arc::new(klights_controllers::service::ServiceIpam::new(
                "10.43.128.0/17",
            )),
        );
    client.set_controller_dispatcher(dispatcher.clone());
    let command = StorageCommand::UpdateStatus {
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "web".to_string(),
        status: serde_json::json!({
            "phase": "Running",
            "podIP": "10.42.0.8",
            "podIPs": [{"ip": "10.42.0.8"}],
            "conditions": [{"type": "Ready", "status": "True"}]
        }),
        expected_rv: None,
        preconditions: ResourcePreconditions::uid("uid-1"),
        observed_status_stamp: None,
    };
    let payload = bytes::Bytes::from(
        OutboxPayload::from_command(command)
            .encode_protobuf()
            .expect("encode local Pod status protobuf"),
    );
    client
        .deliver_test_outbox(
            "local-pod-ready",
            OutboxOperation::PodStatus,
            payload,
            "worker-1",
            1,
            1,
        )
        .await
        .expect("apply local Pod status");

    let keys = klights_reconcile_api::ControllerDispatcherPort::pending_reconcile_keys(
        dispatcher.as_ref(),
    )
    .await;
    assert_eq!(
        keys.iter()
            .filter(|key| key.kind() == "Service" && key.name() == "web")
            .count(),
        1
    );
    let pod_store = crate::bootstrap::pod_repository_composition::new_pod_store(db.clone());
    klights_controllers::endpoints::reconcile_service_endpoints_batch(
        db.as_ref(),
        &pod_store,
        klights_controllers::endpoints::ServiceEndpointBatchReconcileRequest {
            service_name: "web",
            service_uid: &service.uid,
            namespace: "default",
            selector: service.data.pointer("/spec/selector"),
            service_ports: service.data.pointer("/spec/ports"),
            publish_not_ready: false,
        },
    )
    .await
    .unwrap();
    let endpoints = db
        .get_resource("v1", "Endpoints", Some("default"), "web")
        .await
        .unwrap()
        .expect("JSON Endpoints row");
    let slice = db
        .get_resource(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some("default"),
            "web-klights",
        )
        .await
        .unwrap()
        .expect("JSON EndpointSlice row");
    assert_eq!(
        endpoints.data.pointer("/subsets/0/addresses/0/ip"),
        Some(&serde_json::json!("10.42.0.8"))
    );
    assert_eq!(
        slice.data.pointer("/endpoints/0/conditions/ready"),
        Some(&serde_json::json!(true))
    );
}

/// Mutation gate: every `LeaderApiClient` mutation refuses when
/// `is_leader_rx=false`. Asserts the gate fires before any datastore
/// work happens.
#[tokio::test]
async fn local_api_client_refuses_apply_outbox_when_not_leader() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    make_pod(&db).await;
    let (_tx, rx) = watch::channel(false);
    let client = LocalApiClient::new(Arc::new(db), "node-a".to_string(), rx);

    let err = client
        .deliver_test_outbox(
            "idem-1",
            OutboxOperation::PodStatus,
            pod_status_payload(),
            "client",
            1,
            1,
        )
        .await
        .expect_err("non-leader apply_outbox must be rejected");
    assert_eq!(err, OutboxApplyError::NotLeader);
    assert!(err.is_retryable());
}

#[tokio::test]
async fn outbox_terminal_decision_local_invalid_and_malformed_rows_consume_in_order() {
    let db: DatastoreHandle = Arc::new(
        crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap(),
    );
    db.create_resource(
        "v1",
        "Node",
        None,
        "node-a",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "node-a", "uid": "node-uid-a"},
            "status": {"conditions": []}
        }),
    )
    .await
    .expect("create local Node");
    let client = LocalApiClient::new(
        db.clone(),
        "node-a".to_string(),
        crate::control_plane::client::local::always_leader_watch(),
    );
    let command = StorageCommand::UpdateStatus {
        api_version: "v1".to_string(),
        kind: "Node".to_string(),
        namespace: None,
        name: "node-a".to_string(),
        status: serde_json::json!({"conditions": []}),
        expected_rv: Some(7),
        preconditions: ResourcePreconditions {
            uid: Some("node-uid-a".to_string()),
            resource_version: Some(7),
        },
        observed_status_stamp: None,
    };
    let payload = bytes::Bytes::from(
        OutboxPayload::from_command(command)
            .encode_protobuf()
            .expect("encode invalid worker Node status"),
    );

    let error = client
        .deliver_test_outbox(
            "invalid-node-status-rv",
            OutboxOperation::NodeStatus,
            payload,
            "client",
            1,
            1,
        )
        .await
        .expect_err("local focused delivery must enforce NodeSelfStatusRequest validation");
    assert!(matches!(
        error,
        klights_leader_api::OutboxDeliveryError::InvalidRequest {
            field: "status.resource_version",
            ..
        }
    ));
    assert_eq!(
        db.list_outbox_stream_watermarks().await.unwrap()[0].stream_seq,
        1,
        "local authorization rejection must durably consume sequence one"
    );

    let valid_status = || {
        bytes::Bytes::from(
            OutboxPayload::from_command(StorageCommand::UpdateStatus {
                api_version: "v1".to_string(),
                kind: "Node".to_string(),
                namespace: None,
                name: "node-a".to_string(),
                status: serde_json::json!({"conditions": []}),
                expected_rv: None,
                preconditions: ResourcePreconditions::uid("node-uid-a"),
                observed_status_stamp: None,
            })
            .encode_protobuf()
            .expect("encode valid local Node status"),
        )
    };
    client
        .deliver_test_outbox(
            "valid-node-status-after-invalid",
            OutboxOperation::NodeStatus,
            valid_status(),
            "client",
            1,
            2,
        )
        .await
        .expect("sequence two applies after terminal authorization decision");

    let malformed = client
        .deliver_test_outbox(
            "malformed-node-status",
            OutboxOperation::NodeStatus,
            bytes::Bytes::from_static(&[0xff, 0x00, 0x81]),
            "client",
            1,
            3,
        )
        .await
        .expect_err("malformed delivery stays fail-closed");
    assert!(malformed.is_terminal());
    assert_eq!(
        db.list_outbox_stream_watermarks().await.unwrap()[0].stream_seq,
        3,
        "malformed sequence must receive a durable terminal decision"
    );
    client
        .deliver_test_outbox(
            "valid-node-status-after-malformed",
            OutboxOperation::NodeStatus,
            valid_status(),
            "client",
            1,
            4,
        )
        .await
        .expect("sequence four applies after malformed terminal decision");
}

#[tokio::test]
async fn exact_codec_rejection_precedes_decode_ledger_and_watermark() {
    let db: DatastoreHandle = Arc::new(
        crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap(),
    );
    let client = LocalApiClient::new(
        db.clone(),
        "node-a".to_string(),
        crate::control_plane::client::local::always_leader_watch(),
    );
    let initial_rv = db
        .get_current_resource_version()
        .await
        .expect("read initial RV");

    for advertised in [
        klights_cluster_core::COMMAND_CODEC_VERSION - 1,
        klights_cluster_core::COMMAND_CODEC_VERSION + 1,
    ] {
        let idempotency_key = format!("incompatible-codec-{advertised}");
        let error = klights_leader_api::LeaderOutboxDelivery::deliver_outbox(
            &client,
            klights_leader_api::OutboxDeliveryRequest::try_new_versioned(
                advertised,
                idempotency_key.clone(),
                klights_leader_api::OutboxDeliveryOperation::PodMetadata,
                Arc::<[u8]>::from([0xff, 0x00, 0x81]),
                "peer-a",
                71,
                1,
            )
            .expect("transport preserves the advertised codec"),
        )
        .await
        .expect_err("only exact codec v3 is accepted");
        assert_eq!(
            error,
            klights_leader_api::OutboxDeliveryError::codec_incompatible(
                advertised,
                klights_cluster_core::COMMAND_CODEC_VERSION,
            )
        );
        assert!(error.is_retryable());
        assert!(
            db.get_applied_outbox(&idempotency_key)
                .await
                .expect("read incompatible ledger")
                .is_none(),
            "exact-version rejection must precede ledger insertion"
        );
        assert!(
            db.list_outbox_stream_watermarks()
                .await
                .expect("read incompatible watermarks")
                .is_empty(),
            "exact-version rejection must precede watermark advancement"
        );
        assert_eq!(
            db.get_current_resource_version()
                .await
                .expect("read RV after rejection"),
            initial_rv,
            "rejected opaque bytes must not mutate cluster state"
        );
    }
}

#[tokio::test]
async fn local_resource_command_is_leader_gated_before_datastore_mutation() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let (_tx, rx) = watch::channel(false);
    let client = LocalApiClient::new(Arc::new(db), "node-a".to_string(), rx);
    let request = ResourceCommandRequest::try_new(StorageCommand::CreateResource {
        api_version: "v1".to_string(),
        kind: "ConfigMap".to_string(),
        namespace: Some("default".to_string()),
        name: "settings".to_string(),
        data: serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"namespace": "default", "name": "settings"}
        }),
    })
    .expect("valid command");

    let error = LeaderResourceCommand::submit_resource_command(&client, request)
        .await
        .expect_err("a follower must reject resource commands");
    assert_eq!(error, ResourceCommandError::NotLeader);
    assert!(
        client
            .db
            .get_resource("v1", "ConfigMap", Some("default"), "settings")
            .await
            .expect("read after rejection")
            .is_none()
    );
}

#[tokio::test]
async fn local_resource_command_returns_the_created_resource() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let (_tx, rx) = watch::channel(true);
    let client = LocalApiClient::new(Arc::new(db), "node-a".to_string(), rx);
    let request = ResourceCommandRequest::try_new(StorageCommand::CreateResource {
        api_version: "v1".to_string(),
        kind: "ConfigMap".to_string(),
        namespace: Some("default".to_string()),
        name: "settings".to_string(),
        data: serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"namespace": "default", "name": "settings"}
        }),
    })
    .expect("valid command");

    let result = LeaderResourceCommand::submit_resource_command(&client, request)
        .await
        .expect("leader command");
    assert!(
        matches!(result, ResourceCommandResult::Resource(resource) if resource.name == "settings")
    );
}

#[tokio::test]
async fn local_resource_command_preserves_duplicate_create_as_already_exists() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let (_tx, rx) = watch::channel(true);
    let client = LocalApiClient::new(Arc::new(db), "node-a".to_string(), rx);
    let command = StorageCommand::CreateResource {
        api_version: "v1".to_string(),
        kind: "ConfigMap".to_string(),
        namespace: Some("default".to_string()),
        name: "settings".to_string(),
        data: serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"namespace": "default", "name": "settings"}
        }),
    };
    LeaderResourceCommand::submit_resource_command(
        &client,
        ResourceCommandRequest::try_new(command.clone()).expect("valid command"),
    )
    .await
    .expect("first create");
    let error = LeaderResourceCommand::submit_resource_command(
        &client,
        ResourceCommandRequest::try_new(command).expect("valid command"),
    )
    .await
    .expect_err("duplicate create must be rejected");
    assert!(matches!(error, ResourceCommandError::AlreadyExists { .. }));
}

/// `allocate_node_subnet` writes cluster state and must be gated.
#[tokio::test]
async fn local_api_client_refuses_allocate_node_subnet_when_not_leader() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let (_tx, rx) = watch::channel(false);
    let client = LocalApiClient::new(Arc::new(db), "node-a".to_string(), rx);

    let request = klights_leader_api::NodeSubnetAllocationRequest::try_new(
        "node-a",
        "10.50.0.0/16",
        "10.99.0.10",
    )
    .expect("valid allocation request");
    let err =
        klights_leader_api::LeaderNodeSubnetAllocation::allocate_node_subnet(&client, request)
            .await
            .expect_err("non-leader subnet allocation must be rejected");
    assert!(
        matches!(
            err,
            klights_leader_api::NodeSubnetAllocationError::NotLeader
        ),
        "expected typed NotLeader, got: {err}"
    );
}

#[tokio::test]
async fn local_api_client_maps_subnet_exhaustion_to_typed_error() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let (_tx, rx) = watch::channel(true);
    let client = LocalApiClient::new(Arc::new(db), "node-a".to_string(), rx);

    for node_name in ["node-a", "node-b"] {
        let request = klights_leader_api::NodeSubnetAllocationRequest::try_new(
            node_name,
            "10.50.0.0/24",
            "10.99.0.10",
        )
        .expect("valid allocation request");
        let result =
            klights_leader_api::LeaderNodeSubnetAllocation::allocate_node_subnet(&client, request)
                .await;
        if node_name == "node-a" {
            result.expect("the only /24 must be allocated");
        } else {
            assert!(
                matches!(
                    result,
                    Err(klights_leader_api::NodeSubnetAllocationError::Exhausted { .. })
                ),
                "the second allocation must report typed exhaustion, got {result:?}"
            );
        }
    }
}

#[tokio::test]
async fn local_api_client_refuses_network_topology_query_when_not_leader() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let (_tx, rx) = watch::channel(false);
    let client = LocalApiClient::new(Arc::new(db), "node-a".to_string(), rx);
    let request =
        klights_leader_api::NodeSubnetQuery::try_new("node-a").expect("valid topology query");

    let err = klights_leader_api::LeaderNetworkTopologyQuery::get_node_subnet(&client, request)
        .await
        .expect_err("non-leader topology query must fail closed");
    assert!(matches!(
        err,
        klights_leader_api::NetworkTopologyError::NotLeader
    ));
}

/// Cached reads may use follower-applied state, but LeaderFresh must not.
#[tokio::test]
async fn local_api_client_allows_reads_when_not_leader() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    make_pod(&db).await;
    let (_tx, rx) = watch::channel(false);
    let client = LocalApiClient::new(Arc::new(db), "node-a".to_string(), rx);

    let key = ResourceKey {
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "web".to_string(),
    };
    assert!(
        client
            .get_resource(
                ResourceGetRequest::try_new(key.clone(), ResourceQueryConsistency::Cached)
                    .expect("valid Pod request"),
            )
            .await
            .expect("read allowed")
            .is_some(),
        "non-leader get_resource must succeed"
    );
    assert!(
        client
            .get_resource(
                pod_get_request("default", "web", ResourceQueryConsistency::Cached)
                    .expect("valid Pod request"),
            )
            .await
            .expect("read allowed")
            .is_some(),
        "non-leader get_pod must succeed"
    );
    let listed = client
        .list_resources(
            ResourceListRequest::try_new(
                "v1",
                "Pod",
                Some("default".to_string()),
                None,
                None,
                None,
                None,
                ResourceQueryConsistency::Cached,
            )
            .expect("valid Pod list request"),
        )
        .await
        .expect("list allowed");
    assert_eq!(
        listed.items().len(),
        1,
        "non-leader list_resources must succeed"
    );
    assert!(matches!(
        client
            .get_resource(
                ResourceGetRequest::try_new(key, ResourceQueryConsistency::LeaderFresh)
                    .expect("valid fresh Pod request"),
            )
            .await,
        Err(ResourceQueryError::Retryable { .. })
    ));
    assert!(matches!(
        client
            .list_resources(
                ResourceListRequest::try_new(
                    "v1",
                    "Pod",
                    Some("default".to_string()),
                    None,
                    None,
                    None,
                    None,
                    ResourceQueryConsistency::LeaderFresh,
                )
                .expect("valid fresh Pod list request"),
            )
            .await,
        Err(ResourceQueryError::Retryable { .. })
    ));
}

#[tokio::test]
async fn local_selector_watch_synthesizes_deleted_when_pod_leaves_node() {
    let concrete_db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let passive_reads = crate::datastore::selector::sqlite_passive_read_ports(&concrete_db);
    let db: DatastoreHandle = Arc::new(concrete_db);
    let pod = db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "moving",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"namespace": "default", "name": "moving", "uid": "uid-moving"},
                "spec": {"nodeName": "node-a", "containers": [{"name": "app", "image": "pause"}]}
            }),
        )
        .await
        .unwrap();
    let (_tx, rx) = watch::channel(true);
    let client =
        LocalApiClient::new_with_passive_reads(db.clone(), passive_reads, "node-a".into(), rx);
    let mut stream = client
        .watch_resources(
            WatchRequest::try_new(
                "v1",
                "Pod",
                None,
                None,
                Some("spec.nodeName=node-a".to_string()),
                None,
                None,
            )
            .expect("valid Pod watch"),
        )
        .await
        .unwrap();

    let mut moved = (*pod.data).clone();
    moved["spec"]["nodeName"] = serde_json::Value::String("node-b".to_string());
    db.update_resource(
        "v1",
        "Pod",
        Some("default"),
        "moving",
        moved,
        pod.resource_version,
    )
    .await
    .unwrap();

    let event = tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
        .await
        .expect("leave transition should arrive")
        .expect("stream should remain open")
        .expect("event should decode");
    assert_eq!(event.event_type(), WatchEventType::Deleted);
    assert_eq!(event.resource().data["metadata"]["name"], "moving");
}

async fn register_watch_scope_crd(
    db: &DatastoreHandle,
    group: &str,
    kind: &str,
    plural: &str,
    namespaced: bool,
) {
    db.create_resource(
        "apiextensions.k8s.io/v1",
        "CustomResourceDefinition",
        None,
        &format!("{plural}.{group}"),
        serde_json::json!({
            "apiVersion": "apiextensions.k8s.io/v1",
            "kind": "CustomResourceDefinition",
            "metadata": {"name": format!("{plural}.{group}")},
            "spec": {
                "group": group,
                "scope": if namespaced { "Namespaced" } else { "Cluster" },
                "names": {"kind": kind, "plural": plural, "singular": plural},
                "versions": [{"name": "v1", "served": true, "storage": true}]
            }
        }),
    )
    .await
    .expect("register CRD scope metadata");
}

#[tokio::test]
async fn local_positioned_watch_resolves_namespaced_crd_for_all_namespaces_delivery() {
    let concrete_db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let passive_reads = crate::datastore::selector::sqlite_passive_read_ports(&concrete_db);
    let db: DatastoreHandle = Arc::new(concrete_db);
    register_watch_scope_crd(&db, "example.com", "Widget", "widgets", true).await;
    let (_tx, rx) = watch::channel(true);
    let client =
        LocalApiClient::new_with_passive_reads(db.clone(), passive_reads, "node-a".into(), rx);
    let mut stream = client
        .watch_resources(
            WatchRequest::try_new("example.com/v1", "Widget", None, None, None, None, None)
                .expect("valid namespaced CRD watch"),
        )
        .await
        .expect("namespaced CRD watch opens");

    db.create_resource(
        "example.com/v1",
        "Widget",
        Some("default"),
        "namespaced",
        serde_json::json!({
            "apiVersion": "example.com/v1",
            "kind": "Widget",
            "metadata": {"namespace": "default", "name": "namespaced"}
        }),
    )
    .await
    .expect("create namespaced CR");

    let event = tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
        .await
        .expect("all-namespaces CRD watch must receive namespaced events")
        .expect("watch remains open")
        .expect("event is valid");
    assert_eq!(event.resource().namespace.as_deref(), Some("default"));
}

#[tokio::test]
async fn local_positioned_watch_resolves_cluster_scoped_crd_delivery() {
    let concrete_db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let passive_reads = crate::datastore::selector::sqlite_passive_read_ports(&concrete_db);
    let db: DatastoreHandle = Arc::new(concrete_db);
    register_watch_scope_crd(
        &db,
        "cluster.example.com",
        "ClusterWidget",
        "clusterwidgets",
        false,
    )
    .await;
    let (_tx, rx) = watch::channel(true);
    let client =
        LocalApiClient::new_with_passive_reads(db.clone(), passive_reads, "node-a".into(), rx);
    let mut stream = client
        .watch_resources(
            WatchRequest::try_new(
                "cluster.example.com/v1",
                "ClusterWidget",
                None,
                None,
                None,
                None,
                None,
            )
            .expect("valid cluster CRD watch"),
        )
        .await
        .expect("cluster CRD watch opens");

    db.create_resource(
        "cluster.example.com/v1",
        "ClusterWidget",
        None,
        "clustered",
        serde_json::json!({
            "apiVersion": "cluster.example.com/v1",
            "kind": "ClusterWidget",
            "metadata": {"name": "clustered"}
        }),
    )
    .await
    .expect("create cluster CR");

    let event = tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
        .await
        .expect("cluster CRD watch must receive cluster events")
        .expect("watch remains open")
        .expect("event is valid");
    assert_eq!(event.resource().namespace, None);
}

#[tokio::test]
async fn exact_position_selector_watch_replays_late_lower_rv_leave_as_deleted() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let selected = serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "namespace": "default",
            "name": "selected",
            "uid": "uid-selected",
            "labels": {"track": "yes"}
        }
    });
    db.apply_replicated_create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "selected",
        selected.clone(),
        ReplicatedCreateOptions {
            resource_version: 40,
            meta_uid: Some("uid-selected".into()),
        },
    )
    .await
    .unwrap();
    db.apply_replicated_create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "rv-high-water",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "namespace": "default",
                "name": "rv-high-water",
                "uid": "uid-high-water"
            }
        }),
        ReplicatedCreateOptions {
            resource_version: 50,
            meta_uid: Some("uid-high-water".into()),
        },
    )
    .await
    .unwrap();

    let list = db
        .list_resources(
            "v1",
            "ConfigMap",
            Some("default"),
            ResourceListQuery::new(Some("track=yes"), None, None, None),
        )
        .await
        .unwrap();
    assert_eq!(list.items.len(), 1);
    assert_eq!(list.resource_version, 50);
    let list_position = list
        .watch_replay_position
        .expect("LIST must carry its exact durable position");

    let mut nonmatching = selected;
    nonmatching["metadata"]["labels"]["track"] = serde_json::json!("no");
    db.apply_replicated_create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "selected",
        nonmatching,
        ReplicatedCreateOptions {
            resource_version: 45,
            meta_uid: Some("uid-selected".into()),
        },
    )
    .await
    .unwrap();

    let passive_reads = crate::datastore::selector::sqlite_passive_read_ports(&db);
    let db: DatastoreHandle = Arc::new(db);
    let (_tx, rx) = watch::channel(true);
    let client = LocalApiClient::new_with_passive_reads(db, passive_reads, "node-a".into(), rx);
    let mut stream = client
        .watch_resources(
            WatchRequest::try_new(
                "v1",
                "ConfigMap",
                Some("default".into()),
                Some("track=yes".into()),
                None,
                Some(50),
                Some(list_position),
            )
            .expect("valid positioned selector watch"),
        )
        .await
        .unwrap();

    let event = tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
        .await
        .expect("retained lower-RV leave must replay")
        .expect("watch remains open")
        .expect("event decodes");
    assert_eq!(event.event_type(), WatchEventType::Deleted);
    assert_eq!(event.resource().data["metadata"]["labels"]["track"], "yes");
    assert!(
        event
            .resume_position()
            .is_some_and(|position| position.event_id > list_position.event_id),
        "resume cursor must advance through the lower-RV mutation"
    );
}

#[tokio::test]
async fn local_omitted_rv_watch_starts_after_existing_objects() {
    let concrete_db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let passive_reads = crate::datastore::selector::sqlite_passive_read_ports(&concrete_db);
    let db: DatastoreHandle = Arc::new(concrete_db);
    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "existing",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"namespace": "default", "name": "existing"}
        }),
    )
    .await
    .unwrap();
    let (_tx, rx) = watch::channel(true);
    let client =
        LocalApiClient::new_with_passive_reads(db.clone(), passive_reads, "node-a".into(), rx);
    let mut stream = client
        .watch_resources(
            WatchRequest::try_new(
                "v1",
                "ConfigMap",
                Some("default".to_string()),
                None,
                None,
                None,
                None,
            )
            .expect("valid ConfigMap watch"),
        )
        .await
        .unwrap();
    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "fresh",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"namespace": "default", "name": "fresh"}
        }),
    )
    .await
    .unwrap();

    let event = tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
        .await
        .expect("post-establishment event should arrive")
        .expect("stream should remain open")
        .expect("event should decode");
    assert_eq!(event.resource().data["metadata"]["name"], "fresh");
}

/// Promotion is a watch flip. The same client instance must start
/// accepting writes the moment is_leader_rx observes `true`. No
/// re-construction or rewiring.
#[tokio::test]
async fn local_api_client_flips_to_accepting_writes_on_promotion() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    make_pod(&db).await;
    let (tx, rx) = watch::channel(false);
    let client = LocalApiClient::new(Arc::new(db.clone()), "node-a".to_string(), rx);
    client.set_controller_dispatcher(
        crate::bootstrap::controller_adapters::controller_runtime_adapter::dispatcher_for_test(
            &db,
            Arc::new(klights_controllers::service::ServiceIpam::new(
                "10.43.128.0/17",
            )),
        ),
    );

    // Pre-promotion: write refused.
    let pre = client
        .deliver_test_outbox(
            "idem-2",
            OutboxOperation::PodStatus,
            pod_status_payload(),
            "client",
            1,
            1,
        )
        .await;
    assert!(pre.is_err(), "pre-promotion write must be refused");

    // Promotion: flip the watch.
    tx.send(true).expect("send promotion signal");

    // Post-promotion: same client instance, write succeeds.
    let post = client
        .deliver_test_outbox(
            "idem-3",
            OutboxOperation::PodStatus,
            pod_status_payload(),
            "client",
            1,
            1,
        )
        .await;
    assert!(
        post.is_ok(),
        "post-promotion write must succeed on the same instance, got: {post:?}"
    );
}

/// Demotion is the symmetric flip. A live leader that loses
/// leadership (term lost, voluntary step-down) must stop accepting
/// writes on the next call.
#[tokio::test]
async fn local_api_client_revokes_writes_on_demotion() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    make_pod(&db).await;
    let (tx, rx) = watch::channel(true);
    let client = LocalApiClient::new(Arc::new(db.clone()), "node-a".to_string(), rx);
    client.set_controller_dispatcher(
        crate::bootstrap::controller_adapters::controller_runtime_adapter::dispatcher_for_test(
            &db,
            Arc::new(klights_controllers::service::ServiceIpam::new(
                "10.43.128.0/17",
            )),
        ),
    );

    // Pre-demotion: write succeeds.
    let pre = client
        .deliver_test_outbox(
            "idem-4",
            OutboxOperation::PodStatus,
            pod_status_payload(),
            "client",
            1,
            1,
        )
        .await;
    assert!(pre.is_ok(), "pre-demotion write must succeed");

    // Demotion: flip the watch to false.
    tx.send(false).expect("send demotion signal");

    // Post-demotion: same client instance, write refused.
    let post = client
        .deliver_test_outbox(
            "idem-5",
            OutboxOperation::PodStatus,
            pod_status_payload(),
            "client",
            1,
            1,
        )
        .await
        .expect_err("post-demotion write must be refused");
    assert_eq!(post, OutboxApplyError::NotLeader);
    assert!(post.is_retryable());
}

/// The focused delivery port uses the same leader gate as every local
/// mutation and must surface a retryable result after demotion.
#[tokio::test]
async fn outbox_apply_client_respects_leader_gate() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    make_pod(&db).await;
    let (_tx, rx) = watch::channel(false);
    let client = LocalApiClient::new(Arc::new(db), "node-a".to_string(), rx);
    let trait_obj: &dyn klights_leader_api::LeaderOutboxDelivery = &client;

    let err = trait_obj
        .deliver_outbox(
            klights_leader_api::OutboxDeliveryRequest::try_new(
                "idem-6",
                klights_leader_api::OutboxDeliveryOperation::PodStatus,
                Arc::<[u8]>::from(pod_status_payload().to_vec()),
                "client",
                1,
                1,
            )
            .expect("valid delivery request"),
        )
        .await
        .expect_err("non-leader outbox apply must be refused");
    assert_eq!(err, OutboxApplyError::NotLeader);
    assert!(
        err.is_retryable(),
        "outbox dispatcher must re-queue typed NotLeader"
    );
}

/// Compile-time pin: the `is_leader_rx` field is a required
/// `watch::Receiver<bool>` and the constructor signature demands it.
/// If a future refactor moves the field behind an `Option<>` or
/// adds a default-true fallback, this test breaks at compile time
/// (it asserts the exact constructor arity and parameter type).
#[test]
fn local_api_client_constructor_requires_is_leader_rx() {
    // Force the compiler to verify the constructor signature. This
    // closure can only be constructed if `LocalApiClient::new` has
    // exactly the (DatastoreHandle, String, watch::Receiver<bool>)
    // shape — any change to the watch arg breaks the binding.
    let _check: fn(DatastoreHandle, String, watch::Receiver<bool>) -> LocalApiClient =
        LocalApiClient::new;
    let _check_with_tracker: fn(
        DatastoreHandle,
        String,
        Arc<klights_controllers::node_lease::NodeLeaseTracker>,
        watch::Receiver<bool>,
    ) -> LocalApiClient = LocalApiClient::new_with_node_lease_tracker;
}

/// `always_leader_watch()` returns a receiver permanently held at
/// `true`. Required for tests and for boot paths where leadership
/// has already been established (e.g. cp1 after bootstrap_single_voter
/// runs synchronously, before any real watch wiring exists).
#[test]
fn always_leader_watch_observes_true_forever() {
    let rx = always_leader_watch();
    assert!(*rx.borrow(), "always_leader_watch must start true");
    // The internal sender is leaked — drop the rx clone we have and
    // recreate; both copies must still observe true.
    drop(rx);
    let rx2 = always_leader_watch();
    assert!(*rx2.borrow(), "always_leader_watch must stay true");
}

#[tokio::test]
async fn local_projected_token_capability_remains_self_node_scoped() {
    let db: DatastoreHandle = Arc::new(
        crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap(),
    );
    let client = LocalApiClient::new(db, "leader-cp1".to_string(), always_leader_watch());
    let request = ProjectedServiceAccountTokenRequest::try_new(
        "default",
        "default",
        vec!["api".to_string()],
        3_600,
        "client",
        "pod-uid",
        "mn-worker",
        None,
    )
    .unwrap();

    let error =
        LeaderProjectedServiceAccountToken::issue_projected_service_account_token(&client, request)
            .await
            .expect_err("local kubelet capability must not mint for another node");
    assert_eq!(error, ProjectedServiceAccountTokenError::Unauthorized);
}

#[test]
fn projected_token_leadership_fence_rejects_demotion_and_aba() {
    for transitions in [&[false][..], &[false, true][..]] {
        let (tx, rx) = watch::channel(true);
        let fence = LeadershipGenerationFence::sample(rx).expect("initial leader");
        for state in transitions {
            tx.send(*state).unwrap();
        }
        assert_eq!(
            fence.ensure_unchanged(),
            Err(ProjectedServiceAccountTokenError::NotLeader),
            "every leadership generation change must invalidate the operation: {transitions:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn projected_token_signing_fence_blocks_demotion_until_signing_finishes() {
    let (authority, publisher) =
        klights_replication::authority::WatchLeaderAuthority::channel(true, None);
    let fence = LeadershipGenerationFence::sample(authority.clone())
        .expect("initial leader")
        .with_signing_fence(Some(authority.signing_fence()));
    let (signing_entered_tx, signing_entered_rx) = std::sync::mpsc::channel();
    let (release_signing_tx, release_signing_rx) = std::sync::mpsc::channel();
    let signing = std::thread::spawn(move || {
        fence.sign_if_unchanged(|| {
            signing_entered_tx.send(()).unwrap();
            release_signing_rx.recv().unwrap();
            "signed"
        })
    });
    signing_entered_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("signing operation did not enter its fenced section");

    let mut transition = tokio::spawn(async move {
        publisher.publish(false, None).await;
        publisher.publish(true, None).await;
    });
    tokio::select! {
        result = &mut transition => panic!("leadership transition completed before signing released: {result:?}"),
        _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {}
    }
    assert_eq!(
        klights_leader_api::LeaderAuthority::route(authority.as_ref()),
        klights_leader_api::AuthorityRoute::Unavailable,
        "new authority calls must fail closed while the transition waits for signing"
    );

    release_signing_tx.send(()).unwrap();
    assert_eq!(signing.join().unwrap(), Ok("signed"));
    transition.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn projected_token_crypto_worker_revalidates_after_acquiring_signing_fence() {
    let (authority, publisher) =
        klights_replication::authority::WatchLeaderAuthority::channel(true, None);
    let permit = match klights_leader_api::LeaderAuthority::route(authority.as_ref()) {
        klights_leader_api::AuthorityRoute::Local(permit) => permit,
        route => panic!("expected local authority, got {route:?}"),
    };
    let signing_fence = authority.signing_fence();
    let crypto = crate::bootstrap::file_blocking::test_file_process_executor().crypto_executor();
    let (signing_entered_tx, signing_entered_rx) = std::sync::mpsc::channel();
    let (release_signing_tx, release_signing_rx) = std::sync::mpsc::channel();
    let signing = tokio::spawn({
        let authority = authority.clone();
        async move {
            crypto
                .run_blocking("test-projected-token-signing-fence", move || {
                    let _authority_read = signing_fence.blocking_read();
                    klights_leader_api::LeaderAuthority::validate(authority.as_ref(), &permit)
                        .map_err(|_| ProjectedServiceAccountTokenError::NotLeader)?;
                    signing_entered_tx.send(()).unwrap();
                    release_signing_rx.recv().unwrap();
                    Ok::<_, ProjectedServiceAccountTokenError>("signed")
                })
                .await
                .expect("supervised signing worker")
        }
    });
    signing_entered_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("signing worker did not acquire and validate its authority fence");

    let mut transition = tokio::spawn(async move {
        publisher.publish(false, None).await;
    });
    tokio::select! {
        result = &mut transition => panic!("demotion completed before signing released: {result:?}"),
        _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {}
    }
    assert_eq!(
        klights_leader_api::LeaderAuthority::route(authority.as_ref()),
        klights_leader_api::AuthorityRoute::Unavailable,
        "new authority calls must fail closed while signing holds the read fence"
    );

    release_signing_tx.send(()).unwrap();
    assert_eq!(signing.await.unwrap().unwrap(), "signed");
    transition.await.unwrap();
}

#[test]
fn projected_token_leadership_fence_accepts_only_stable_generation() {
    let (_tx, rx) = watch::channel(true);
    let fence = LeadershipGenerationFence::sample(rx).expect("initial leader");
    assert_eq!(fence.ensure_unchanged(), Ok(()));

    let (_tx, rx) = watch::channel(false);
    assert!(matches!(
        LeadershipGenerationFence::sample(rx),
        Err(ProjectedServiceAccountTokenError::NotLeader)
    ));
}

#[tokio::test]
async fn projected_token_full_issuance_rejects_demotion_and_aba_before_signing() {
    for transitions in [&[false][..], &[false, true][..]] {
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let reader = {
            let entered = entered.clone();
            let release = release.clone();
            Arc::new(move || {
                let entered = entered.clone();
                let release = release.clone();
                let future: std::pin::Pin<
                    Box<dyn std::future::Future<Output = ()> + Send + 'static>,
                > = Box::pin(async move {
                    entered.notify_one();
                    release.notified().await;
                });
                future
            })
        };
        let db: DatastoreHandle = Arc::new(
            crate::datastore::sqlite::Datastore::new_in_memory()
                .await
                .unwrap(),
        );
        let (leadership_tx, leadership_rx) = watch::channel(true);
        let data_root = tempfile::tempdir().unwrap();
        let namespace = data_root.path().to_str().unwrap().to_string();
        let signing_key_path = data_root.path().join("etc/service-account-signing.key");
        klights_supervisor::runtime_fs::create_dir_all(signing_key_path.parent().unwrap()).unwrap();
        std::fs::write(&signing_key_path, "unused-test-signing-key").unwrap();
        let sign_probe = install_projected_token_issue_test_probe(namespace.clone(), reader);
        let client = Arc::new(
            LocalApiClient::new_with_node_lease_tracker_namespace_signing_key_and_file_process(
                LocalApiPersistencePorts::new(
                    db.clone(),
                    crate::datastore::selector::unused_fail_closed_passive_read_ports(),
                    test_watch_signals(&db),
                ),
                "node-a".to_string(),
                namespace,
                signing_key_path,
                Arc::new(klights_controllers::node_lease::NodeLeaseTracker::new_at(
                    chrono::Utc::now(),
                )),
                leadership_rx,
                crate::bootstrap::file_blocking::test_file_process_executor(),
            ),
        );
        let request = ProjectedServiceAccountTokenRequest::try_new(
            "default",
            "default",
            vec!["api".to_string()],
            3_600,
            "pod-a",
            "pod-uid-a",
            "node-a",
            Some("node-uid-a".to_string()),
        )
        .unwrap();
        let issue = {
            let client = client.clone();
            tokio::spawn(async move {
                client
                    .issue_projected_token_after_transport_auth(request)
                    .await
            })
        };
        tokio::time::timeout(std::time::Duration::from_secs(2), entered.notified())
            .await
            .expect("signing-key reader did not enter its async boundary");
        for state in transitions {
            leadership_tx.send(*state).unwrap();
        }
        release.notify_one();

        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(2), issue)
                .await
                .expect("issuance did not finish after releasing signing-key reader")
                .unwrap(),
            Err(ProjectedServiceAccountTokenError::NotLeader),
            "full issuance must reject leadership transition {transitions:?}"
        );
        assert_eq!(
            sign_probe.sign_attempts(),
            0,
            "synchronous signing must not be invoked after {transitions:?}"
        );
    }
}

async fn seed_projected_token_adapter_resources(db: &dyn DatastoreBackend) {
    for name in ["default", "other"] {
        db.create_resource(
            "v1",
            "ServiceAccount",
            Some("default"),
            name,
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "ServiceAccount",
                "metadata": {"namespace": "default", "name": name, "uid": format!("sa-{name}")}
            }),
        )
        .await
        .unwrap();
    }
    db.create_resource(
        "v1",
        "Node",
        None,
        "node-a",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "node-a", "uid": "node-uid-a"}
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "pod-a",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"namespace": "default", "name": "pod-a", "uid": "pod-uid-a"},
            "spec": {"serviceAccountName": "default", "nodeName": "node-a"}
        }),
    )
    .await
    .unwrap();
}

async fn seeded_authenticated_projected_token_adapter() -> (
    tempfile::TempDir,
    AuthenticatedProjectedTokenIssuer,
    ProjectedServiceAccountTokenRequest,
) {
    let db: DatastoreHandle = Arc::new(
        crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap(),
    );
    seed_projected_token_adapter_resources(db.as_ref()).await;
    let data_root = tempfile::tempdir().unwrap();
    let namespace = data_root.path().to_str().unwrap().to_string();
    let signing_key_path = data_root.path().join("etc/service-account-signing.key");
    klights_supervisor::runtime_fs::create_dir_all(signing_key_path.parent().unwrap()).unwrap();
    let signing_key =
        klights_auth::test_support::generate_ca_full_at(time::OffsetDateTime::now_utc())
            .unwrap()
            .3;
    std::fs::write(&signing_key_path, &signing_key).unwrap();
    let local = Arc::new(
        LocalApiClient::new_with_node_lease_tracker_namespace_signing_key_and_file_process(
            LocalApiPersistencePorts::new(
                db.clone(),
                crate::datastore::selector::unused_fail_closed_passive_read_ports(),
                test_watch_signals(&db),
            ),
            "leader-cp1".to_string(),
            namespace,
            signing_key_path,
            Arc::new(klights_controllers::node_lease::NodeLeaseTracker::new_at(
                chrono::Utc::now(),
            )),
            always_leader_watch(),
            crate::bootstrap::file_blocking::test_file_process_executor(),
        ),
    );
    let request = ProjectedServiceAccountTokenRequest::try_new(
        "default",
        "default",
        vec!["api".to_string()],
        3_600,
        "pod-a",
        "pod-uid-a",
        "node-a",
        Some("node-uid-a".to_string()),
    )
    .unwrap();
    (
        data_root,
        AuthenticatedProjectedTokenIssuer::new(local),
        request,
    )
}

#[tokio::test]
async fn authenticated_projected_token_adapter_signs_from_seeded_leader_state() {
    let (_data_root, adapter, request) = seeded_authenticated_projected_token_adapter().await;
    let token = adapter
        .issue_authenticated_projected_service_account_token(request)
        .await
        .expect("privileged post-auth adapter must sign authoritative bound claims");
    assert_eq!(token.token().split('.').count(), 3);
}

#[tokio::test]
async fn authenticated_projected_token_adapter_preserves_binding_mismatches() {
    let cases = [
        (
            "service account",
            "other",
            "pod-uid-a",
            "node-a",
            "node-uid-a",
        ),
        ("Pod UID", "default", "wrong-pod", "node-a", "node-uid-a"),
        ("node name", "default", "pod-uid-a", "node-b", "node-uid-a"),
        ("node UID", "default", "pod-uid-a", "node-a", "wrong-node"),
    ];
    for (label, service_account, pod_uid, node_name, node_uid) in cases {
        let (_data_root, adapter, _) = seeded_authenticated_projected_token_adapter().await;
        let request = ProjectedServiceAccountTokenRequest::try_new(
            "default",
            service_account,
            vec!["api".to_string()],
            3_600,
            "pod-a",
            pod_uid,
            node_name,
            Some(node_uid.to_string()),
        )
        .unwrap();
        assert!(
            matches!(
                adapter
                    .issue_authenticated_projected_service_account_token(request)
                    .await,
                Err(ProjectedServiceAccountTokenError::BindingMismatch { .. })
            ),
            "{label} mismatch must remain a binding mismatch"
        );
    }
}

/// The test-only focused services preserve the production leader gate:
/// invoke delivery with watch=false, assert typed refusal, and confirm
/// cluster.db has no trace of a proposal.
#[tokio::test]
async fn delegated_outbox_service_refuses_before_proposal_on_non_leader() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    make_pod(&db).await;
    let pre_rv = db
        .get_resource("v1", "Pod", Some("default"), "web")
        .await
        .expect("read pod")
        .expect("pod exists")
        .resource_version;
    let (_tx, rx) = watch::channel(false);
    let client = LocalApiClient::new(Arc::new(db.clone()), "node-a".to_string(), rx);

    let err = client
        .deliver_test_outbox(
            "n1raft-audit",
            OutboxOperation::PodStatus,
            pod_status_payload(),
            "client",
            1,
            1,
        )
        .await
        .expect_err("non-leader delivery must refuse before proposal");
    assert_eq!(err, OutboxApplyError::NotLeader);
    assert!(err.is_retryable());

    // Confirm proposal never executed: resourceVersion and status remain
    // unchanged from the pre-call state.
    let post = db
        .get_resource("v1", "Pod", Some("default"), "web")
        .await
        .expect("re-read pod")
        .expect("pod still exists");
    assert_eq!(
        post.resource_version, pre_rv,
        "non-leader proposal must not advance cluster.db resourceVersion"
    );
    assert!(
        post.data.pointer("/status/phase").is_none(),
        "non-leader proposal must not write Pod status"
    );
}
