//! Base-owned active-watch integration over passive SQLite commit facts.

use klights_cluster_core::{
    LogApplyCommit, LogApplyMutation, LogApplyWatchEventRow, ResourceBatchOperation,
    ResourceBatchPutMode, ResourcePreconditions,
};
use serde_json::json;

#[tokio::test]
async fn committed_apply_json_and_protobuf_paths_produce_identical_rows() {
    let commit = LogApplyCommit::try_new(vec![LogApplyMutation::PutResource(
        klights_cluster_core::LogApplyResourceRow {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace: Some("default".to_string()),
            name: "encoding-cm".to_string(),
            uid: "cm-enc".to_string(),
            resource_version: 0,
            data: serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "name": "encoding-cm",
                    "namespace": "default",
                    "uid": "cm-enc"
                },
                "data": {"encoded": "true"}
            }),
            require_absent: false,
            require_existing: false,
            precondition_uid: None,
            precondition_resource_version: None,
            status_only: false,
        },
    )])
    .expect("codec fixture is a valid RV-zero live commit");

    let json_bytes = klights_replication::log_apply_wire::encode_commit_json(&commit).unwrap();
    let json_commit = klights_replication::log_apply_wire::decode_commit_json(&json_bytes).unwrap();
    let json_state =
        crate::bootstrap::composition_tests::native_api::support::build_test_app_state().await;
    let json_store = json_state.resource_store();
    json_store
        .apply_log_apply_commit(json_commit)
        .await
        .unwrap();

    let protobuf_bytes =
        klights_replication::log_apply_wire::encode_commit_protobuf(&commit).unwrap();
    let protobuf_commit =
        klights_replication::log_apply_wire::decode_commit_protobuf(&protobuf_bytes).unwrap();
    let protobuf_state =
        crate::bootstrap::composition_tests::native_api::support::build_test_app_state().await;
    let protobuf_store = protobuf_state.resource_store();
    protobuf_store
        .apply_log_apply_commit(protobuf_commit)
        .await
        .unwrap();

    let json_row = json_store
        .get_resource("v1", "ConfigMap", Some("default"), "encoding-cm")
        .await
        .unwrap()
        .expect("JSON apply must materialize the row");
    let protobuf_row = protobuf_store
        .get_resource("v1", "ConfigMap", Some("default"), "encoding-cm")
        .await
        .unwrap()
        .expect("protobuf apply must materialize the row");

    assert_eq!(json_row.uid, protobuf_row.uid);
    assert_eq!(json_row.resource_version, protobuf_row.resource_version);
    assert_eq!(json_row.data, protobuf_row.data);
}

#[tokio::test]
async fn sqlite_create_reaches_the_root_active_watch_adapter_after_commit() {
    let state =
        crate::bootstrap::composition_tests::native_api::support::build_test_app_state().await;
    let db = state.resource_store();
    let mut events = state.subscribe_watch("v1", "ConfigMap");
    let created = db
        .create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "active-watch",
            json!({"metadata": {"name": "active-watch", "namespace": "default"}}),
        )
        .await
        .unwrap();

    let event = events
        .try_recv()
        .expect("root adapter must publish the commit");
    assert_eq!(event.resource_version(), Some(created.resource_version));
    assert_eq!(event.object["metadata"]["name"], "active-watch");
}

#[tokio::test]
async fn sqlite_batch_reaches_root_watch_topics_with_one_commit_position() {
    let state =
        crate::bootstrap::composition_tests::native_api::support::build_test_app_state().await;
    let db = state.resource_store();
    let mut endpoints = state.subscribe_watch("v1", "Endpoints");
    let mut slices = state.subscribe_watch("discovery.k8s.io/v1", "EndpointSlice");

    db.apply_resource_batch(vec![
        ResourceBatchOperation::Put {
            api_version: "v1".to_string(),
            kind: "Endpoints".to_string(),
            namespace: Some("default".to_string()),
            name: "watch-ep".to_string(),
            data: json!({"metadata":{"name":"watch-ep","namespace":"default"}}),
            mode: ResourceBatchPutMode::Create,
            preconditions: ResourcePreconditions::default(),
        },
        ResourceBatchOperation::Put {
            api_version: "discovery.k8s.io/v1".to_string(),
            kind: "EndpointSlice".to_string(),
            namespace: Some("default".to_string()),
            name: "watch-eps".to_string(),
            data: json!({"metadata":{"name":"watch-eps","namespace":"default"}}),
            mode: ResourceBatchPutMode::Create,
            preconditions: ResourcePreconditions::default(),
        },
    ])
    .await
    .unwrap();

    assert_eq!(
        endpoints.try_recv().unwrap().resource_version(),
        slices.try_recv().unwrap().resource_version(),
    );
}

#[tokio::test]
async fn apply_log_apply_commit_broadcasts_explicit_watch_event() {
    let state =
        crate::bootstrap::composition_tests::native_api::support::build_test_app_state().await;
    let db = state.resource_store();
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "bound-pod",
        json!({
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
    let mut watch_rx = state.subscribe_watch("v1", "Pod");

    let leader_watch_row = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "bound-pod",
            "namespace": "default",
            "uid": "pod-uid"
        },
        "spec": {
            "nodeName": "mn-controlplane3",
            "containers": [{"name": "c", "image": "pause"}]
        }
    });

    db.apply_log_apply_commit(
        LogApplyCommit::try_new(vec![LogApplyMutation::PutWatchEvent(
            LogApplyWatchEventRow {
                event_id: None,
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: Some("default".to_string()),
                name: "bound-pod".to_string(),
                resource_version: 0,
                event_type: "MODIFIED".to_string(),
                data: leader_watch_row,
            },
        )])
        .expect("explicit watch event is a valid RV-zero live commit"),
    )
    .await
    .unwrap();
    let applied_rv = db.get_current_resource_version().await.unwrap();

    let event = watch_rx
        .try_recv()
        .expect("explicit watch-history apply must wake local watchers");
    assert_eq!(event.event_type, klights_watch::EventType::Modified);
    assert_eq!(event.resource_version(), Some(applied_rv));
    assert_eq!(
        event
            .object
            .pointer("/spec/nodeName")
            .and_then(serde_json::Value::as_str),
        Some("mn-controlplane3")
    );
}
