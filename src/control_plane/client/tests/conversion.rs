use std::sync::Arc;

use bytes::Bytes;

use crate::control_plane::client::local::LocalApiClient;
use klights_leader_api::{
    LeaderNetworkTopologyQuery, LeaderNodeSubnetAllocation, LeaderPodCleanupIntents,
    LeaderResourceQuery, NodeDataplaneQuery, NodeSubnetAllocationRequest, NodeSubnetQuery,
    PeerSubnetsQuery, PodCleanupIntentAckRequest, ResourceQueryConsistency, pod_get_request,
};

#[test]
fn concrete_leader_clients_implement_focused_pod_effect_ports() {
    fn assert_ports<T>()
    where
        T: klights_leader_api::LeaderProjectedServiceAccountToken
            + klights_leader_api::LeaderPodCleanupIntents,
    {
    }

    assert_ports::<crate::control_plane::client::local::LocalApiClient>();
    assert_ports::<klights_leader_rpc::client::RemoteApiClient>();
    assert_ports::<crate::control_plane::client::leader_proxy::LeaderProxyApiClient>();
    assert_ports::<crate::control_plane::client::leader_proxy::StubRemoteForwarder>();
}

#[test]
fn node_effect_ports_have_the_frozen_authority_split() {
    fn assert_lease<T: klights_leader_api::LeaderNodeLeaseRenewal>() {}
    fn assert_local_lifecycle<T: klights_leader_api::LeaderNodeLifecycleStatus>() {}

    assert_lease::<crate::control_plane::client::local::LocalApiClient>();
    assert_lease::<klights_leader_rpc::client::RemoteApiClient>();
    assert_lease::<crate::control_plane::client::leader_proxy::LeaderProxyApiClient>();
    assert_lease::<crate::control_plane::client::leader_proxy::StubRemoteForwarder>();
    assert_local_lifecycle::<crate::control_plane::client::local::LocalApiClient>();
}

#[tokio::test]
async fn node_effect_ports_gate_follower_lease_before_tracker_mutation() {
    let db: crate::datastore::DatastoreHandle = Arc::new(
        crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap(),
    );
    let tracker = Arc::new(klights_controllers::node_lease::NodeLeaseTracker::new_at(
        chrono::Utc::now(),
    ));
    let (_leader_tx, follower_rx) = tokio::sync::watch::channel(false);
    let local = crate::control_plane::client::local::LocalApiClient::new_with_node_lease_tracker(
        db,
        "cp-1".to_string(),
        tracker.clone(),
        follower_rx,
    );
    let request = klights_leader_api::NodeLeaseRenewalRequest::try_new(
        "cp-1",
        klights_cluster_core::k8s_time::format_time(chrono::Utc::now()),
        30,
    )
    .expect("valid renewal");

    let error = klights_leader_api::LeaderNodeLeaseRenewal::renew_node_lease(&local, request)
        .await
        .expect_err("follower must reject local lease renewal");
    assert_eq!(error, klights_leader_api::NodeLeaseRenewalError::NotLeader);
    assert!(
        tracker.observed("cp-1").await.is_none(),
        "leadership must be checked before the in-memory tracker is mutated"
    );
}

#[tokio::test]
async fn node_effect_ports_remote_rejects_cross_node_before_transport() {
    let remote = klights_leader_rpc::client::RemoteApiClient::without_transport(
        "worker-1",
        Arc::new(
            crate::bootstrap::composition_adapters::remote_informer_cache_adapter::WatchCacheAdapter::new(),
        ),
    );
    let request = klights_leader_api::NodeLeaseRenewalRequest::try_new(
        "worker-2",
        klights_cluster_core::k8s_time::format_time(chrono::Utc::now()),
        30,
    )
    .expect("valid renewal shape");
    assert!(matches!(
        klights_leader_api::LeaderNodeLeaseRenewal::renew_node_lease(&remote, request).await,
        Err(klights_leader_api::NodeLeaseRenewalError::Unauthorized { .. })
    ));
}

#[tokio::test]
async fn node_effect_lease_renewal_has_no_cluster_rv_watch_or_lease_row() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let tracker = Arc::new(klights_controllers::node_lease::NodeLeaseTracker::new_at(
        chrono::Utc::now(),
    ));
    let client = crate::control_plane::client::local::LocalApiClient::new_with_node_lease_tracker(
        Arc::new(db.clone()),
        "cp-1".to_string(),
        tracker.clone(),
        crate::control_plane::client::local::always_leader_watch(),
    );
    let before_rv = db.get_current_resource_version().await.expect("read RV");
    let request = klights_leader_api::NodeLeaseRenewalRequest::try_new(
        "cp-1",
        klights_cluster_core::k8s_time::format_time(chrono::Utc::now()),
        30,
    )
    .expect("valid renewal");
    klights_leader_api::LeaderNodeLeaseRenewal::renew_node_lease(&client, request)
        .await
        .expect("renew in memory");

    assert!(tracker.observed("cp-1").await.is_some());
    assert_eq!(
        db.get_current_resource_version().await.expect("read RV"),
        before_rv
    );
    assert!(
        db.get_resource(
            "coordination.k8s.io/v1",
            "Lease",
            Some("kube-node-lease"),
            "cp-1",
        )
        .await
        .expect("read Lease")
        .is_none()
    );
}

#[tokio::test]
async fn node_effect_lifecycle_status_preserves_spec_metadata_and_conflicts_stale_rv() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let created = db
        .create_resource(
            "v1",
            "Node",
            None,
            "worker-a",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {
                    "name": "worker-a",
                    "uid": "node-uid-a",
                    "labels": {"owned-by": "control-plane"}
                },
                "spec": {"unschedulable": true},
                "status": {"conditions": []}
            }),
        )
        .await
        .expect("create Node");
    let client = crate::control_plane::client::local::LocalApiClient::new(
        Arc::new(db.clone()),
        "cp-1".to_string(),
        crate::control_plane::client::local::always_leader_watch(),
    );
    let command = StorageCommand::UpdateStatus {
        api_version: "v1".to_string(),
        kind: "Node".to_string(),
        namespace: None,
        name: "worker-a".to_string(),
        status: serde_json::json!({
            "conditions": [{"type": "Ready", "status": "Unknown"}]
        }),
        expected_rv: Some(created.resource_version),
        preconditions: ResourcePreconditions::from_resource(&created),
        observed_status_stamp: None,
    };
    let request = klights_leader_api::NodeLifecycleStatusRequest::try_new(command.clone())
        .expect("valid lifecycle CAS");
    let result = klights_leader_api::LeaderNodeLifecycleStatus::submit_node_lifecycle_status(
        &client, request,
    )
    .await
    .expect("apply lifecycle status");
    assert!(matches!(
        result,
        klights_leader_api::NodeLifecycleStatusResult::Updated { resource_version }
            if resource_version > created.resource_version
    ));

    let stored = db
        .get_resource("v1", "Node", None, "worker-a")
        .await
        .expect("read Node")
        .expect("Node exists");
    assert_eq!(
        stored.data.pointer("/spec/unschedulable"),
        Some(&serde_json::json!(true))
    );
    assert_eq!(
        stored.data.pointer("/metadata/labels/owned-by"),
        Some(&serde_json::json!("control-plane"))
    );
    assert_eq!(
        stored.data.pointer("/status/conditions/0/status"),
        Some(&serde_json::json!("Unknown"))
    );

    let stale = klights_leader_api::NodeLifecycleStatusRequest::try_new(command)
        .expect("same old CAS remains structurally valid");
    assert!(matches!(
            klights_leader_api::LeaderNodeLifecycleStatus::submit_node_lifecycle_status(
                &client, stale,
            )
            .await,
            Err(klights_leader_api::NodeLifecycleStatusError::Conflict { .. })
        ));
}
use crate::bootstrap::composition_tests::support::OutboxPayload;
use crate::datastore::ResourcePreconditions;
use klights_cluster_core::command::StorageCommand;
use klights_cluster_store::{DataplaneEncryption, DataplaneMode, DataplanePeerMetadata};
use klights_kubelet::node_outbox::payload::OutboxOperation;
use klights_leader_api::{
    OutboxDeliveryError as OutboxApplyError, OutboxDeliveryResult as OutboxApplyResult,
};

#[test]
fn positioned_watch_adapters_implement_focused_ports() {
    fn assert_focused<
        T: klights_leader_api::LeaderWatch + klights_leader_api::LeaderCacheReadiness,
    >() {
    }

    assert_focused::<crate::control_plane::client::local::LocalApiClient>();
    assert_focused::<klights_leader_rpc::client::RemoteApiClient>();
    assert_focused::<crate::control_plane::client::leader_proxy::LeaderProxyApiClient>();
    assert_focused::<crate::control_plane::client::leader_proxy::StubRemoteForwarder>();
}

#[tokio::test]
async fn stub_watch_and_readiness_are_typed_unavailable() {
    let stub =
        crate::control_plane::client::leader_proxy::StubRemoteForwarder::new("cp-stub".to_string());
    let watch =
        klights_leader_api::WatchRequest::try_new("v1", "Pod", None, None, None, Some(41), None)
            .expect("valid watch");
    assert!(matches!(
        klights_leader_api::LeaderWatch::watch_resources(&stub, watch).await,
        Err(klights_leader_api::LeaderWatchError::Unavailable { .. })
    ));

    let readiness = klights_leader_api::CacheReadinessRequest::try_new(
        "v1",
        "Pod",
        None,
        None,
        Some("spec.nodeName=worker-a".to_string()),
    )
    .expect("valid readiness scope");
    assert!(matches!(
        klights_leader_api::LeaderCacheReadiness::wait_cache_ready(&stub, readiness).await,
        Err(klights_leader_api::CacheReadinessError::Unavailable { .. })
    ));
}

fn pod_status_payload(uid: &str) -> Bytes {
    let command = StorageCommand::UpdateStatus {
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "web".to_string(),
        status: serde_json::json!({"phase": "Running"}),
        expected_rv: None,
        preconditions: ResourcePreconditions {
            uid: Some(uid.to_string()),
            resource_version: None,
        },
        observed_status_stamp: None,
    };
    Bytes::from(
        OutboxPayload::from_command(command)
            .encode_protobuf()
            .expect("encode payload"),
    )
}

fn pod_delete_payload(name: &str, uid: &str, observed_resource_version: i64) -> Bytes {
    pod_delete_payload_for("default", name, uid, observed_resource_version)
}

fn pod_delete_payload_for(
    namespace: &str,
    name: &str,
    uid: &str,
    observed_resource_version: i64,
) -> Bytes {
    let command = StorageCommand::FinalizeBoundPod {
        namespace: namespace.to_string(),
        name: name.to_string(),
        pod_uid: uid.to_string(),
        node_name: "worker-a".to_string(),
        observed_resource_version,
    };
    Bytes::from(
        OutboxPayload::from_command(command)
            .encode_protobuf()
            .expect("encode payload"),
    )
}

#[tokio::test]
async fn local_client_reads_pods_through_focused_resource_query() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
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
            "metadata": {
                "namespace": "default",
                "name": "web",
                "uid": "uid-1"
            },
            "spec": {"containers": [{"name": "app", "image": "nginx"}]}
        }),
    )
    .await
    .expect("create pod");
    let client = LocalApiClient::new(
        Arc::new(db),
        "node-a".to_string(),
        crate::control_plane::client::local::always_leader_watch(),
    );

    assert!(
        client
            .get_resource(
                pod_get_request("default", "web", ResourceQueryConsistency::Cached)
                    .expect("valid Pod request"),
            )
            .await
            .expect("get pod")
            .is_some()
    );
}

#[tokio::test]
async fn cleanup_intent_ack_is_idempotent_and_never_touches_same_name_pod_row() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let replacement = db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "web",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "web",
                    "uid": "replacement-uid"
                },
                "spec": {
                    "nodeName": "node-a",
                    "containers": [{"name": "app", "image": "nginx"}]
                }
            }),
        )
        .await
        .expect("create same-name replacement Pod");
    let client = LocalApiClient::new(
        Arc::new(db.clone()),
        "node-a".to_string(),
        crate::control_plane::client::local::always_leader_watch(),
    );
    let ack = PodCleanupIntentAckRequest::try_new(
        "node-a",
        "default",
        "web",
        "old-uid",
        crate::datastore::POD_CLEANUP_REASON_NODE_LOST,
    )
    .unwrap();

    for _ in 0..2 {
        client
            .acknowledge_pod_cleanup_intent(ack.clone())
            .await
            .expect("missing cleanup intent acknowledgement is idempotent");
    }

    let stored = db
        .get_resource("v1", "Pod", Some("default"), "web")
        .await
        .unwrap()
        .expect("same-name replacement Pod must remain");
    assert_eq!(stored.uid, "replacement-uid");
    assert_eq!(stored.resource_version, replacement.resource_version);
}

#[tokio::test]
async fn local_client_apply_outbox_is_idempotent_and_uid_bound() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
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
            "metadata": {
                "namespace": "default",
                "name": "web",
                "uid": "uid-1"
            },
            "spec": {"containers": [{"name": "app", "image": "nginx"}]},
            "status": {"phase": "Pending"}
        }),
    )
    .await
    .expect("create pod");
    let client = LocalApiClient::new(
        Arc::new(db.clone()),
        "node-a".to_string(),
        crate::control_plane::client::local::always_leader_watch(),
    );
    client.set_controller_dispatcher(
        crate::bootstrap::controller_adapters::controller_runtime_adapter::dispatcher_for_test(
            &db,
            Arc::new(klights_controllers::service::ServiceIpam::new(
                "10.43.128.0/17",
            )),
        ),
    );

    let first = client
        .deliver_test_outbox(
            "stable-key",
            OutboxOperation::PodStatus,
            pod_status_payload("uid-1"),
            "client",
            1,
            1,
        )
        .await
        .expect("first apply");
    let duplicate = client
        .deliver_test_outbox(
            "stable-key",
            OutboxOperation::PodStatus,
            pod_status_payload("uid-1"),
            "client",
            1,
            1,
        )
        .await
        .expect("duplicate apply");
    assert!(matches!(first, OutboxApplyResult::Applied { .. }));
    assert!(matches!(
        duplicate,
        OutboxApplyResult::AlreadyApplied { .. }
    ));
    let stored = db
        .get_resource("v1", "Pod", Some("default"), "web")
        .await
        .expect("get pod")
        .expect("pod exists");
    assert_eq!(
        stored
            .data
            .pointer("/status/phase")
            .and_then(|v| v.as_str()),
        Some("Running")
    );
    let rv_before_mismatch = stored.resource_version;
    let watch_before_mismatch = db
        .current_watch_replay_position()
        .await
        .expect("watch position before assigned UID mismatch");

    let err = client
        .deliver_test_outbox(
            "uid-mismatch-key",
            OutboxOperation::PodStatus,
            pod_status_payload("uid-2"),
            "client",
            1,
            2,
        )
        .await
        .expect_err("assigned uid mismatch");
    assert!(matches!(err, OutboxApplyError::UidMismatch { .. }));
    assert_eq!(
        db.get_current_resource_version().await.expect("read RV"),
        rv_before_mismatch,
        "terminal UID mismatch must not allocate a public resourceVersion"
    );
    assert_eq!(
        db.current_watch_replay_position()
            .await
            .expect("watch position after assigned UID mismatch"),
        watch_before_mismatch,
        "terminal UID mismatch must not append watch history"
    );
    let ledger = db
        .get_applied_outbox("uid-mismatch-key")
        .await
        .expect("read terminal ledger")
        .expect("terminal ledger row");
    assert!(matches!(
        klights_leader_rpc::storage_wire_codec::decode_response_protobuf(&ledger.result_proto),
        Ok(klights_cluster_core::command::StorageResponse::Error { message })
            if message.contains("delivery UID mismatch")
    ));
    assert_eq!(
        db.list_outbox_stream_watermarks()
            .await
            .expect("read terminal watermark")[0]
            .stream_seq,
        2,
        "terminal UID mismatch must consume its exact assigned sequence"
    );
}

#[tokio::test]
async fn local_client_apply_outbox_returns_committed_resource_version() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
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
            "metadata": {
                "namespace": "default",
                "name": "web",
                "uid": "uid-1"
            },
            "spec": {"containers": [{"name": "app", "image": "nginx"}]},
            "status": {"phase": "Pending"}
        }),
    )
    .await
    .expect("create pod");
    let client = LocalApiClient::new(
        Arc::new(db.clone()),
        "node-a".to_string(),
        crate::control_plane::client::local::always_leader_watch(),
    );
    client.set_controller_dispatcher(
        crate::bootstrap::controller_adapters::controller_runtime_adapter::dispatcher_for_test(
            &db,
            Arc::new(klights_controllers::service::ServiceIpam::new(
                "10.43.128.0/17",
            )),
        ),
    );

    let applied = client
        .deliver_test_outbox(
            "raft-client-key",
            OutboxOperation::PodStatus,
            pod_status_payload("uid-1"),
            "client",
            1,
            1,
        )
        .await
        .expect("apply outbox through local client");

    let OutboxApplyResult::Applied { applied_rv } = applied else {
        panic!("first local apply must commit a new write");
    };
    assert!(applied_rv > 0);
}

#[tokio::test]
async fn local_client_pod_delete_outbox_reconciles_terminating_namespace() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    db.create_namespace(
        "worker-finalize-ns",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {
                "name": "worker-finalize-ns",
                "uid": "worker-finalize-ns-uid"
            },
            "spec": {"finalizers": ["kubernetes"]},
            "status": {"phase": "Active"}
        }),
    )
    .await
    .expect("create namespace");
    let namespace = db
        .get_namespace("worker-finalize-ns")
        .await
        .expect("read namespace")
        .expect("namespace exists");
    let mut terminating = std::sync::Arc::unwrap_or_clone(namespace.data);
    k8s_native_service::set_namespace_terminating_status_at(
        &mut terminating,
        false,
        klights_supervisor::SystemWallClock::now_utc(),
    );
    db.update_namespace(
        "worker-finalize-ns",
        terminating,
        namespace.resource_version,
    )
    .await
    .expect("mark namespace terminating");
    db.create_resource(
        "v1",
        "ConfigMap",
        Some("worker-finalize-ns"),
        "leftover-cm",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "namespace": "worker-finalize-ns",
                "name": "leftover-cm"
            },
            "data": {"k": "v"}
        }),
    )
    .await
    .expect("create non-pod content");
    let observed_pod = db
        .create_resource(
            "v1",
            "Pod",
            Some("worker-finalize-ns"),
            "worker-pod",
            serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "worker-finalize-ns",
                "name": "worker-pod",
                "uid": "worker-pod-uid",
                "deletionTimestamp": "2026-05-20T00:00:00Z",
                "deletionGracePeriodSeconds": 0
            },
            "spec": {
                "nodeName": "worker-a",
                "containers": [{"name": "app", "image": "nginx"}]
            },
            "status": {"phase": "Running"}
            }),
        )
        .await
        .expect("create terminating pod");

    let client = LocalApiClient::new(
        Arc::new(db.clone()),
        "worker-a".to_string(),
        crate::control_plane::client::local::always_leader_watch(),
    );
    client.set_namespace_termination(
            crate::bootstrap::composition_adapters::api_state_adapter::RootNamespaceTerminationReconciler::new(
                crate::bootstrap::composition_adapters::api_state_adapter::RootNamespaceTerminationStore::new(Arc::new(db.clone())),
                klights_controllers::side_effects::SideEffectMetrics::new(),
            ),
        );
    client.set_non_pod_finalization(Arc::new(
        crate::bootstrap::controller_adapters::gc_delete_adapter::GcNonPodFinalizationAdapter::new(
            Arc::new(db.clone()),
        ),
    ));
    let dispatcher =
        crate::bootstrap::controller_adapters::controller_runtime_adapter::dispatcher_for_test(
            &db,
            Arc::new(klights_controllers::service::ServiceIpam::new(
                "10.43.128.0/17",
            )),
        );
    client.set_controller_dispatcher(dispatcher);
    let applied = client
        .deliver_test_outbox(
            "worker-pod-actor-finalize-delete",
            OutboxOperation::PodMetadata,
            pod_delete_payload_for(
                "worker-finalize-ns",
                "worker-pod",
                "worker-pod-uid",
                observed_pod.resource_version,
            ),
            "client",
            1,
            1,
        )
        .await
        .expect("apply worker pod delete outbox");
    assert!(matches!(applied, OutboxApplyResult::Applied { .. }));

    assert!(
        db.get_resource("v1", "Pod", Some("worker-finalize-ns"), "worker-pod")
            .await
            .expect("get pod")
            .is_none(),
        "leader apply must remove the actor-finalized Pod row"
    );
    assert!(
        db.get_namespace("worker-finalize-ns")
            .await
            .expect("get namespace")
            .is_none(),
        "leader must reconcile namespace termination immediately after applying worker Pod delete"
    );
}

#[tokio::test]
async fn local_client_pod_delete_outbox_finalizes_ready_foreground_owner() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    db.create_resource(
        "v1",
        "ReplicationController",
        Some("default"),
        "foreground-owner",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "ReplicationController",
            "metadata": {
                "name": "foreground-owner",
                "namespace": "default",
                "uid": "foreground-owner-uid",
                "deletionTimestamp": "2026-05-17T00:00:00Z",
                "finalizers": ["foregroundDeletion"]
            },
            "spec": {"replicas": 1, "selector": {"app": "foreground-owner"}}
        }),
    )
    .await
    .expect("create foreground owner");
    let observed_child = db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "foreground-child",
            serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "foreground-child",
                "namespace": "default",
                "uid": "foreground-child-uid",
                "deletionTimestamp": "2026-05-17T00:00:00Z",
                "deletionGracePeriodSeconds": 0,
                "ownerReferences": [{
                    "apiVersion": "v1",
                    "kind": "ReplicationController",
                    "name": "foreground-owner",
                    "uid": "foreground-owner-uid",
                    "controller": true,
                    "blockOwnerDeletion": true
                }]
            },
            "spec": {"nodeName": "worker-a", "containers": [{"name": "app", "image": "nginx"}]},
            "status": {"phase": "Running"}
            }),
        )
        .await
        .expect("create foreground child");

    let client = LocalApiClient::new(
        Arc::new(db.clone()),
        "worker-a".to_string(),
        crate::control_plane::client::local::always_leader_watch(),
    );
    client.set_non_pod_finalization(Arc::new(
        crate::bootstrap::controller_adapters::gc_delete_adapter::GcNonPodFinalizationAdapter::new(
            Arc::new(db.clone()),
        ),
    ));
    let dispatcher =
        crate::bootstrap::controller_adapters::controller_runtime_adapter::dispatcher_for_test(
            &db,
            Arc::new(klights_controllers::service::ServiceIpam::new(
                "10.43.128.0/17",
            )),
        );
    client.set_controller_dispatcher(dispatcher);

    let applied = client
        .deliver_test_outbox(
            "foreground-child-actor-finalize-delete",
            OutboxOperation::PodMetadata,
            pod_delete_payload(
                "foreground-child",
                "foreground-child-uid",
                observed_child.resource_version,
            ),
            "client",
            1,
            1,
        )
        .await
        .expect("apply pod delete outbox");
    assert!(matches!(applied, OutboxApplyResult::Applied { .. }));

    assert!(
        db.get_resource("v1", "Pod", Some("default"), "foreground-child")
            .await
            .expect("get child")
            .is_none(),
        "leader apply must remove the finalized Pod row"
    );
    assert!(
        db.get_resource(
            "v1",
            "ReplicationController",
            Some("default"),
            "foreground-owner"
        )
        .await
        .expect("get foreground owner")
        .is_none(),
        "leader apply of the final dependent Pod delete must remove a ready foreground owner"
    );
}

#[tokio::test]
async fn local_client_serves_network_metadata_without_calling_forwarder() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let client = LocalApiClient::new(
        Arc::new(db.clone()),
        "node-a".to_string(),
        crate::control_plane::client::local::always_leader_watch(),
    );

    let subnet = client
        .allocate_node_subnet(
            NodeSubnetAllocationRequest::try_new("node-a", "10.42.0.0/16", "192.0.2.10")
                .expect("valid allocation request"),
        )
        .await
        .expect("allocate local subnet through leader API")
        .into_subnet();
    assert_eq!(subnet.node_name(), "node-a");
    assert_eq!(subnet.subnet(), "10.42.0.0/24");

    let stored = client
        .get_node_subnet(NodeSubnetQuery::try_new("node-a").expect("valid query"))
        .await
        .expect("get local subnet through leader API")
        .into_option()
        .expect("allocated subnet should exist");
    assert_eq!(stored, subnet);

    let peer = client
        .allocate_node_subnet(
            NodeSubnetAllocationRequest::try_new("node-b", "10.42.0.0/16", "192.0.2.11")
                .expect("valid allocation request"),
        )
        .await
        .expect("allocate peer subnet")
        .into_subnet();
    let peers = client
        .list_peer_subnets(PeerSubnetsQuery::try_new("node-a").expect("valid query"))
        .await
        .expect("list peer subnets through leader API")
        .into_vec();
    assert_eq!(peers, vec![peer]);

    let metadata = DataplanePeerMetadata::try_new(
        "node-b".to_string(),
        DataplaneMode::Root,
        DataplaneEncryption::Disabled,
        None,
        Some("192.0.2.11".to_string()),
        None,
    )
    .expect("valid dataplane metadata");
    db.update_node_dataplane(metadata.clone())
        .await
        .expect("store dataplane metadata");
    assert_eq!(
        client
            .get_node_dataplane(NodeDataplaneQuery::try_new("node-b").expect("valid query"),)
            .await
            .expect("get dataplane metadata through leader API")
            .into_option(),
        Some(
            crate::control_plane::client::focused_dataplane(metadata)
                .expect("valid focused metadata"),
        )
    );
}
