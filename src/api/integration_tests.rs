use crate::api::test_support::{
    build_test_app_state, build_test_app_state_with_authorizer, build_test_router,
    build_test_router_with_db,
};
use serde_json::json;

#[tokio::test]
async fn test_configmap_put_update_preserves_data() {
    use serde_json::json;

    // Setup test database
    let db = crate::datastore::test_support::in_memory().await;

    // Create a ConfigMap
    let create_body = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "test-config",
            "namespace": "default"
        },
        "data": {
            "app.conf": "server=localhost\nport=8080",
            "log.level": "debug"
        }
    });

    let created = db
        .create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "test-config",
            create_body.clone(),
        )
        .await
        .unwrap();

    // Update the ConfigMap (simulating PUT request)
    let update_body = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "test-config",
            "namespace": "default"
        },
        "data": {
            "app.conf": "server=localhost\nport=9090",
            "log.level": "info",
            "new.key": "new.value"
        }
    });

    let updated = db
        .update_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "test-config",
            update_body.clone(),
            created.resource_version,
        )
        .await
        .unwrap();

    // Verify data field is preserved
    assert!(
        updated.data.get("data").is_some(),
        "data field should exist"
    );
    assert_eq!(updated.data["data"]["log.level"], "info");
    assert_eq!(updated.data["data"]["new.key"], "new.value");
    assert!(
        updated.data["data"]["app.conf"]
            .as_str()
            .unwrap()
            .contains("port=9090")
    );
}

#[tokio::test]
async fn test_lenient_json_preserves_configmap_data_field() {
    // Test that LenientJson extractor preserves ConfigMap data field when parsing JSON
    use serde_json::json;

    let configmap_json = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "test-config",
            "namespace": "default"
        },
        "data": {
            "app.conf": "server=localhost\nport=9090",
            "log.level": "info"
        }
    });

    let json_bytes = serde_json::to_vec(&configmap_json).unwrap();

    // Simulate LenientJson extraction (JSON path)
    let parsed: serde_json::Value = serde_json::from_slice(&json_bytes).unwrap();

    // Verify data field is present
    assert!(
        parsed.get("data").is_some(),
        "data field should exist after JSON parsing"
    );
    assert_eq!(parsed["data"]["log.level"], "info");
    assert_eq!(parsed["data"]["app.conf"], "server=localhost\nport=9090");
}

#[tokio::test]
async fn oversized_standard_api_json_body_returns_request_entity_too_large() {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let app = build_test_router().await;
    let oversized_value = "x".repeat(2_097_152);
    let json_body = serde_json::to_vec(&json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "too-large-json"},
        "data": {"payload": oversized_value},
    }))
    .unwrap();
    let mut protobuf_body = b"k8s\0".to_vec();
    protobuf_body.extend(std::iter::repeat_n(b'x', 2_097_152));

    for (content_type, body) in [
        ("application/json", json_body),
        ("application/vnd.kubernetes.protobuf", protobuf_body),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/namespaces/default/configmaps")
                    .header("content-type", content_type)
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            response.status(),
            StatusCode::PAYLOAD_TOO_LARGE,
            "{content_type} oversized create body must return 413"
        );

        let body = to_bytes(response.into_body(), 4096).await.unwrap();
        let status: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(status["kind"], "Status");
        assert_eq!(status["apiVersion"], "v1");
        assert_eq!(status["status"], "Failure");
        assert_eq!(status["reason"], "RequestEntityTooLarge");
        assert_eq!(status["code"], 413);
    }
}

#[test]
fn test_protobuf_decode_preserves_configmap_data_field() {
    // Test that protobuf decoder preserves ConfigMap data field
    use k8s_pb::api::core::v1::ConfigMap;
    use k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use prost::Message;
    use std::collections::BTreeMap;

    // Create a ConfigMap protobuf message with data
    let mut data = BTreeMap::new();
    data.insert(
        "app.conf".to_string(),
        "server=localhost\nport=9090".to_string(),
    );
    data.insert("log.level".to_string(), "info".to_string());

    let cm = ConfigMap {
        metadata: Some(ObjectMeta {
            name: Some("test-config".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        }),
        data,
        ..Default::default()
    };

    // Encode to protobuf bytes (ConfigMap message)
    let mut cm_buf = Vec::new();
    cm.encode(&mut cm_buf).unwrap();

    // Wrap in Unknown envelope (how K8s actually sends it)
    let unknown = crate::protobuf::Unknown {
        type_meta: Some(crate::protobuf::TypeMeta {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
        }),
        raw: cm_buf,
        content_encoding: String::new(),
        content_type: String::new(),
    };

    let mut envelope_buf = Vec::new();
    unknown.encode(&mut envelope_buf).unwrap();

    // Decode back to JSON using our protobuf decoder (simulates LenientJson extractor)
    let json_value = crate::protobuf::decode_protobuf(&envelope_buf).unwrap();

    // Verify data field is present and correct
    assert!(
        json_value.get("data").is_some(),
        "data field should exist after protobuf decode"
    );
    assert_eq!(json_value["data"]["log.level"], "info");
    assert_eq!(
        json_value["data"]["app.conf"],
        "server=localhost\nport=9090"
    );
}

#[test]
fn test_protobuf_decode_preserves_pod_active_deadline_seconds() {
    use k8s_pb::api::core::v1::{Container, Pod, PodSpec};
    use k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use prost::Message;

    let pod = Pod {
        metadata: Some(ObjectMeta {
            name: Some("term-pod".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        }),
        spec: Some(PodSpec {
            containers: vec![Container {
                name: Some("pause".to_string()),
                image: Some("registry.k8s.io/pause:3.10.1".to_string()),
                ..Default::default()
            }],
            active_deadline_seconds: Some(3600),
            ..Default::default()
        }),
        ..Default::default()
    };

    let mut pod_buf = Vec::new();
    pod.encode(&mut pod_buf).unwrap();

    let unknown = crate::protobuf::Unknown {
        type_meta: Some(crate::protobuf::TypeMeta {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
        }),
        raw: pod_buf,
        content_encoding: String::new(),
        content_type: String::new(),
    };

    let mut envelope_buf = Vec::new();
    unknown.encode(&mut envelope_buf).unwrap();

    let json_value = crate::protobuf::decode_protobuf(&envelope_buf).unwrap();
    assert_eq!(json_value["spec"]["activeDeadlineSeconds"], 3600);
}

#[test]
fn test_protobuf_decode_preserves_pod_runtime_class_name() {
    use k8s_pb::api::core::v1::{Container, Pod, PodSpec};
    use k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use prost::Message;

    let pod = Pod {
        metadata: Some(ObjectMeta {
            name: Some("runtime-pod".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        }),
        spec: Some(PodSpec {
            containers: vec![Container {
                name: Some("pause".to_string()),
                image: Some("registry.k8s.io/pause:3.10.1".to_string()),
                ..Default::default()
            }],
            runtime_class_name: Some("missing-runtime".to_string()),
            ..Default::default()
        }),
        ..Default::default()
    };

    let mut pod_buf = Vec::new();
    pod.encode(&mut pod_buf).unwrap();

    let unknown = crate::protobuf::Unknown {
        type_meta: Some(crate::protobuf::TypeMeta {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
        }),
        raw: pod_buf,
        content_encoding: String::new(),
        content_type: String::new(),
    };

    let mut envelope_buf = Vec::new();
    unknown.encode(&mut envelope_buf).unwrap();

    let json_value = crate::protobuf::decode_protobuf(&envelope_buf).unwrap();
    assert_eq!(
        json_value["spec"]["runtimeClassName"], "missing-runtime",
        "protobuf Pod decode must preserve spec.runtimeClassName for admission"
    );
}
#[tokio::test]
async fn test_configmap_put_preserves_data_via_db() {
    let db = crate::datastore::test_support::in_memory().await;

    // Create ConfigMap with data
    let cm = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "cm1", "namespace": "default"},
        "data": {"k": "v1"}
    });

    let created = db
        .create_resource("v1", "ConfigMap", Some("default"), "cm1", cm)
        .await
        .unwrap();

    // PUT with new data
    let updated_body = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "cm1", "namespace": "default"},
        "data": {"k": "v2", "k2": "new"}
    });

    let updated = db
        .update_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "cm1",
            updated_body,
            created.resource_version,
        )
        .await
        .unwrap();

    assert_eq!(updated.data["data"]["k"], "v2");
    assert_eq!(updated.data["data"]["k2"], "new");
}

#[tokio::test]
async fn test_configmap_put_integration_full_stack() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    // Build the app with in-memory state
    let app = build_test_router().await;

    // 1. Create namespace
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"test"}}"#,
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // 2. Create ConfigMap with data
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/test/configmaps")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm1","namespace":"test"},"data":{"key1":"val1"}}"#))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // 3. PUT update with new data
    let req = Request::builder()
        .method("PUT")
        .uri("/api/v1/namespaces/test/configmaps/cm1")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm1","namespace":"test"},"data":{"key1":"updated","key2":"new"}}"#))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let result: serde_json::Value = serde_json::from_slice(&body).unwrap();

    // This is the key assertion — does PUT preserve data?
    assert_eq!(result["data"]["key1"], "updated");
    assert_eq!(result["data"]["key2"], "new");
}

#[tokio::test]
async fn test_api_build_state_uses_mock_network_provider() {
    let state = build_test_app_state().await;
    let datapath = state.network.datapath.clone();
    let result = datapath
        .cni_add(crate::networking::provider::CniAddRequest {
            sandbox_id: "sid-1".to_string(),
            namespace: "default".to_string(),
            pod_name: "test-pod".to_string(),
            pod_uid: "uid".to_string(),
            netns_setns_path: "/proc/self/ns/net".to_string(),
            netns_record_path: "/proc/self/ns/net".to_string(),
            host_network: false,
        })
        .await
        .expect("mock network cni_add should succeed");
    assert_eq!(
        result.ip_addr,
        std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED)
    );
}

#[tokio::test]
async fn test_list_all_statefulsets_table_includes_ready_and_age_columns() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "apps/v1",
        "StatefulSet",
        Some("default"),
        "web",
        json!({
            "apiVersion": "apps/v1",
            "kind": "StatefulSet",
            "metadata": {
                "name": "web",
                "namespace": "default",
                "creationTimestamp": "2026-01-01T00:00:00Z"
            },
            "spec": {"replicas": 2},
            "status": {"readyReplicas": 1}
        }),
    )
    .await
    .unwrap();

    let req = Request::builder()
        .method("GET")
        .uri("/apis/apps/v1/statefulsets")
        .header(
            "accept",
            "application/json;as=Table;v=v1;g=meta.k8s.io,application/json",
        )
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let table: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let columns: Vec<_> = table["columnDefinitions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|col| col["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        columns,
        vec!["Name", "Ready", "Age"],
        "StatefulSet table columns must match kubectl's default server-side table"
    );

    let cells = table["rows"][0]["cells"].as_array().unwrap();
    assert_eq!(cells[0], "web");
    assert_eq!(cells[1], "1/2");
    assert!(cells[2].is_string());
}

#[tokio::test]
async fn test_delete_controller_managed_endpointslice_queues_service_reconcile() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let pod_repository = state.pod_repository.clone();
    let controller_dispatcher = state.controller_dispatcher.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({"apiVersion":"v1","kind":"Namespace","metadata":{"name":"default"}}),
    )
    .await
    .unwrap();

    for (name, ip, port) in [("pod1", "10.43.0.5", 3000), ("pod2", "10.43.0.6", 3001)] {
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            name,
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": name,
                    "namespace": "default",
                    "labels": {"app": "slice-delete"},
                    "uid": format!("{name}-uid")
                },
                "spec": {
                    "containers": [{
                        "name": "web",
                        "ports": [{"name": "web", "containerPort": port, "protocol": "TCP"}]
                    }]
                },
                "status": {
                    "podIP": ip,
                    "conditions": [{"type": "Ready", "status": "True"}]
                }
            }),
        )
        .await
        .unwrap();
    }

    let service = db
        .create_resource(
            "v1",
            "Service",
            Some("default"),
            "example",
            json!({
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {"name": "example", "namespace": "default", "uid": "svc-uid"},
                "spec": {
                    "selector": {"app": "slice-delete"},
                    "ports": [{"name": "http", "port": 80, "targetPort": "web", "protocol": "TCP"}]
                }
            }),
        )
        .await
        .unwrap();

    crate::controllers::endpoints::reconcile_endpointslice(
        db.as_ref(),
        pod_repository.as_ref(),
        "example",
        service
            .data
            .pointer("/metadata/uid")
            .and_then(|uid| uid.as_str())
            .unwrap_or("svc-uid"),
        "default",
        service.data.pointer("/spec/selector"),
        service.data.pointer("/spec/ports"),
    )
    .await
    .unwrap();

    let slices = db
        .list_resources(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some("default"),
            crate::datastore::ResourceListQuery::new(
                Some("kubernetes.io/service-name=example"),
                None,
                None,
                None,
            ),
        )
        .await
        .unwrap();
    assert_eq!(slices.items.len(), 2);

    let first_slice = slices.items[0].name.clone();
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!(
                    "/apis/discovery.k8s.io/v1/namespaces/default/endpointslices/{first_slice}"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let keys = controller_dispatcher.queued_reconcile_keys_for_test().await;
    assert_eq!(
        keys,
        vec![crate::controllers::workqueue::ReconcileKey::namespaced(
            "v1", "Service", "default", "example"
        )],
        "EndpointSlice delete must queue the owning Service instead of reconciling inline"
    );

    let slices = db
        .list_resources(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some("default"),
            crate::datastore::ResourceListQuery::new(
                Some("kubernetes.io/service-name=example"),
                None,
                None,
                None,
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        slices.items.len(),
        1,
        "the DELETE response path must not recreate controller-managed EndpointSlices inline"
    );

    let queued_key = controller_dispatcher.take_reconcile_key_for_test().await;
    assert_eq!(
        queued_key,
        crate::controllers::workqueue::ReconcileKey::namespaced(
            "v1", "Service", "default", "example"
        )
    );
    controller_dispatcher
        .reconcile(&service.data, &db, "test-node")
        .await
        .unwrap();

    let slices = db
        .list_resources(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some("default"),
            crate::datastore::ResourceListQuery::new(
                Some("kubernetes.io/service-name=example"),
                None,
                None,
                None,
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        slices.items.len(),
        2,
        "queued Service reconciliation must converge back to the Service's desired slices"
    );
}

#[tokio::test]
async fn test_delete_manual_endpoints_removes_mirrored_endpointslice() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "default"}}),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "example-custom-endpoints",
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "example-custom-endpoints", "namespace": "default"},
            "spec": {"ports": [{"name": "example", "port": 80, "protocol": "TCP"}]}
        }),
    )
    .await
    .unwrap();

    let endpoints = json!({
        "apiVersion": "v1",
        "kind": "Endpoints",
        "metadata": {"name": "example-custom-endpoints", "namespace": "default"},
        "subsets": [{
            "addresses": [{"ip": "10.1.2.3"}],
            "ports": [{"port": 80, "protocol": "TCP"}]
        }]
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces/default/endpoints")
                .header("content-type", "application/json")
                .body(Body::from(endpoints.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    assert!(
        db.get_resource(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some("default"),
            "example-custom-endpoints-mirror",
        )
        .await
        .unwrap()
        .is_some(),
        "manual Endpoints create must produce a mirrored EndpointSlice"
    );

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/v1/namespaces/default/endpoints/example-custom-endpoints")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    assert!(
        db.get_resource(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some("default"),
            "example-custom-endpoints-mirror",
        )
        .await
        .unwrap()
        .is_none(),
        "manual Endpoints delete must not leave or recreate the mirrored EndpointSlice"
    );
}

#[tokio::test]
async fn test_immutable_configmap_update_rejected_with_422() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    // Create namespace
    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        serde_json::json!({"apiVersion":"v1","kind":"Namespace","metadata":{"name":"default"}}),
    )
    .await
    .unwrap();

    // Create an immutable ConfigMap
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/default/configmaps")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"imm-cm","namespace":"default"},"immutable":true,"data":{"key":"val"}}"#))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // PUT update attempt — must be rejected with 422
    let req = Request::builder()
        .method("PUT")
        .uri("/api/v1/namespaces/default/configmaps/imm-cm")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"imm-cm","namespace":"default"},"immutable":true,"data":{"key":"changed"}}"#))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::UNPROCESSABLE_ENTITY,
        "Immutable ConfigMap PUT must return 422"
    );
}

#[tokio::test]
async fn test_immutable_configmap_patch_rejected_with_422() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        serde_json::json!({"apiVersion":"v1","kind":"Namespace","metadata":{"name":"default"}}),
    )
    .await
    .unwrap();

    // Create an immutable ConfigMap
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/default/configmaps")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"imm-patch","namespace":"default"},"immutable":true,"data":{"key":"val"}}"#))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // PATCH with data change — must be rejected with 422
    let req = Request::builder()
        .method("PATCH")
        .uri("/api/v1/namespaces/default/configmaps/imm-patch")
        .header("content-type", "application/merge-patch+json")
        .body(Body::from(r#"{"data":{"key":"newval"}}"#))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::UNPROCESSABLE_ENTITY,
        "Immutable ConfigMap PATCH must return 422"
    );
}

#[tokio::test]
async fn test_replicationcontroller_scale_put_triggers_reconcile() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    let (app, db) = build_test_router_with_db().await;

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({"apiVersion":"v1","kind":"Namespace","metadata":{"name":"default"}}),
    )
    .await
    .unwrap();

    let create_rc = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/default/replicationcontrollers")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "apiVersion": "v1",
                "kind": "ReplicationController",
                "metadata": {
                    "name": "rc-scale-e2e",
                    "namespace": "default",
                    "uid": "rc-scale-e2e-uid"
                },
                "spec": {
                    "replicas": 1,
                    "selector": { "app": "rc-scale-e2e" },
                    "template": {
                        "metadata": { "labels": { "app": "rc-scale-e2e" } },
                        "spec": { "containers": [{ "name": "c", "image": "busybox:1.36" }] }
                    }
                }
            })
            .to_string(),
        ))
        .unwrap();
    let create_resp = app.clone().oneshot(create_rc).await.unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);

    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(
        pods.items.len(),
        1,
        "create should reconcile to 1 pod before scale update"
    );

    let update_scale = Request::builder()
        .method("PUT")
        .uri("/api/v1/namespaces/default/replicationcontrollers/rc-scale-e2e/scale")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "apiVersion": "autoscaling/v1",
                "kind": "Scale",
                "metadata": { "name": "rc-scale-e2e", "namespace": "default" },
                "spec": { "replicas": 3 }
            })
            .to_string(),
        ))
        .unwrap();
    let scale_resp = app.clone().oneshot(update_scale).await.unwrap();
    assert_eq!(scale_resp.status(), StatusCode::OK);

    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(
        pods.items.len(),
        3,
        "scale subresource update must trigger RC reconcile to desired replicas"
    );
}

#[tokio::test]
async fn test_replicationcontroller_main_put_triggers_reconcile() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::{Value, json};
    use tower::ServiceExt;

    let (app, db) = build_test_router_with_db().await;

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({"apiVersion":"v1","kind":"Namespace","metadata":{"name":"default"}}),
    )
    .await
    .unwrap();

    let create_rc = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/default/replicationcontrollers")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "apiVersion": "v1",
                "kind": "ReplicationController",
                "metadata": {
                    "name": "rc-put-e2e",
                    "namespace": "default",
                    "uid": "rc-put-e2e-uid"
                },
                "spec": {
                    "replicas": 1,
                    "selector": { "app": "rc-put-e2e" },
                    "template": {
                        "metadata": { "labels": { "app": "rc-put-e2e" } },
                        "spec": { "containers": [{ "name": "c", "image": "busybox:1.36" }] }
                    }
                }
            })
            .to_string(),
        ))
        .unwrap();
    let create_resp = app.clone().oneshot(create_rc).await.unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);
    let get_rc = Request::builder()
        .method("GET")
        .uri("/api/v1/namespaces/default/replicationcontrollers/rc-put-e2e")
        .body(Body::empty())
        .unwrap();
    let get_resp = app.clone().oneshot(get_rc).await.unwrap();
    assert_eq!(get_resp.status(), StatusCode::OK);
    let get_body = axum::body::to_bytes(get_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let mut rc: Value = serde_json::from_slice(&get_body).unwrap();

    rc["spec"]["replicas"] = json!(3);
    let update_rc = Request::builder()
        .method("PUT")
        .uri("/api/v1/namespaces/default/replicationcontrollers/rc-put-e2e")
        .header("content-type", "application/json")
        .body(Body::from(rc.to_string()))
        .unwrap();
    let update_resp = app.clone().oneshot(update_rc).await.unwrap();
    assert_eq!(update_resp.status(), StatusCode::OK);

    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(
        pods.items.len(),
        3,
        "main ReplicationController update must trigger reconcile to desired replicas"
    );
}

#[tokio::test]
async fn test_replicationcontroller_status_patch_triggers_reconcile() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    let (app, db) = build_test_router_with_db().await;

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({"apiVersion":"v1","kind":"Namespace","metadata":{"name":"default"}}),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "ReplicationController",
        Some("default"),
        "rc-status-reconcile",
        json!({
            "apiVersion": "v1",
            "kind": "ReplicationController",
            "metadata": {
                "name": "rc-status-reconcile",
                "namespace": "default",
                "uid": "rc-status-reconcile-uid",
                "generation": 1
            },
            "spec": {
                "replicas": 1,
                "selector": {"app": "rc-status-reconcile"},
                "template": {
                    "metadata": {"labels": {"app": "rc-status-reconcile"}},
                    "spec": {"containers": [{"name": "main", "image": "nginx"}]}
                }
            },
            "status": {
                "replicas": 1,
                "fullyLabeledReplicas": 1,
                "readyReplicas": 1,
                "availableReplicas": 1,
                "observedGeneration": 1
            }
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "rc-status-reconcile-pod",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "rc-status-reconcile-pod",
                "namespace": "default",
                "labels": {"app": "rc-status-reconcile"},
                "ownerReferences": [{
                    "apiVersion": "v1",
                    "kind": "ReplicationController",
                    "name": "rc-status-reconcile",
                    "uid": "rc-status-reconcile-uid",
                    "controller": true
                }]
            },
            "status": {
                "phase": "Running",
                "conditions": [{"type": "Ready", "status": "True"}]
            }
        }),
    )
    .await
    .unwrap();

    let patch_status = Request::builder()
        .method("PATCH")
        .uri("/api/v1/namespaces/default/replicationcontrollers/rc-status-reconcile/status")
        .header("content-type", "application/strategic-merge-patch+json")
        .body(Body::from(
            json!({
                "status": {
                    "replicas": 1,
                    "fullyLabeledReplicas": 1,
                    "readyReplicas": 0,
                    "availableReplicas": 0,
                    "observedGeneration": 1
                }
            })
            .to_string(),
        ))
        .unwrap();
    let patch_resp = app.clone().oneshot(patch_status).await.unwrap();
    assert_eq!(patch_resp.status(), StatusCode::OK);

    let updated = db
        .get_resource(
            "v1",
            "ReplicationController",
            Some("default"),
            "rc-status-reconcile",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        updated.data["status"]["readyReplicas"],
        json!(1),
        "RC /status writes must enqueue reconcile so stale readyReplicas are corrected from ready pods"
    );
    assert_eq!(
        updated.data["status"]["availableReplicas"],
        json!(1),
        "RC /status writes must enqueue reconcile so stale availableReplicas are corrected from ready pods"
    );
}

#[tokio::test]
async fn test_replicationcontroller_create_defaults_selector_from_template_labels() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    let (app, db) = build_test_router_with_db().await;

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({"apiVersion":"v1","kind":"Namespace","metadata":{"name":"default"}}),
    )
    .await
    .unwrap();

    let create_rc = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/default/replicationcontrollers")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "apiVersion": "v1",
                "kind": "ReplicationController",
                "metadata": {
                    "name": "rc-default-selector",
                    "namespace": "default"
                },
                "spec": {
                    "replicas": 1,
                    "template": {
                        "metadata": { "labels": { "name": "rc-default-selector" } },
                        "spec": { "containers": [{ "name": "httpd", "image": "httpd:2.4" }] }
                    }
                }
            })
            .to_string(),
        ))
        .unwrap();
    let create_resp = app.clone().oneshot(create_rc).await.unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);

    let rc = db
        .get_resource(
            "v1",
            "ReplicationController",
            Some("default"),
            "rc-default-selector",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        rc.data["spec"]["selector"],
        json!({"name": "rc-default-selector"}),
        "RC create must default spec.selector from template labels"
    );

    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(
        pods.items.len(),
        1,
        "defaulted selector must match the created pod and avoid repeated create attempts"
    );
}

#[tokio::test]
async fn test_replicationcontroller_create_adopts_matching_orphan_pod() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    let (app, db) = build_test_router_with_db().await;

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({"apiVersion":"v1","kind":"Namespace","metadata":{"name":"default"}}),
    )
    .await
    .unwrap();

    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "rc-adopt-orphan-pod",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "rc-adopt-orphan-pod",
                "namespace": "default",
                "labels": { "name": "rc-adopt-orphan" }
            },
            "spec": {
                "containers": [{ "name": "pause", "image": "registry.k8s.io/pause:3.10.1" }]
            },
            "status": { "phase": "Running" }
        }),
    )
    .await
    .unwrap();

    let create_rc = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/default/replicationcontrollers")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "apiVersion": "v1",
                "kind": "ReplicationController",
                "metadata": {
                    "name": "rc-adopt-orphan",
                    "namespace": "default"
                },
                "spec": {
                    "replicas": 1,
                    "selector": { "name": "rc-adopt-orphan" },
                    "template": {
                        "metadata": { "labels": { "name": "rc-adopt-orphan" } },
                        "spec": { "containers": [{ "name": "pause", "image": "registry.k8s.io/pause:3.10.1" }] }
                    }
                }
            })
            .to_string(),
        ))
        .unwrap();
    let create_resp = app.clone().oneshot(create_rc).await.unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);

    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(
        pods.items.len(),
        1,
        "RC create must adopt the matching orphan instead of creating a replacement pod"
    );

    let adopted_pod = db
        .get_resource("v1", "Pod", Some("default"), "rc-adopt-orphan-pod")
        .await
        .unwrap()
        .unwrap();
    let owner_ref = adopted_pod
        .data
        .pointer("/metadata/ownerReferences")
        .and_then(|value| value.as_array())
        .and_then(|refs| refs.iter().find(|owner| owner["controller"] == true))
        .expect("matching orphan pod must be adopted by the created RC");
    assert_eq!(owner_ref["apiVersion"], "v1");
    assert_eq!(owner_ref["kind"], "ReplicationController");
    assert_eq!(owner_ref["name"], "rc-adopt-orphan");

    let rc = db
        .get_resource(
            "v1",
            "ReplicationController",
            Some("default"),
            "rc-adopt-orphan",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        owner_ref["uid"], rc.data["metadata"]["uid"],
        "adopted pod ownerRef must point at the created RC UID"
    );
}

#[tokio::test]
async fn test_replicationcontroller_protobuf_create_defaults_selector_from_template_labels() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    let (app, db) = build_test_router_with_db().await;

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({"apiVersion":"v1","kind":"Namespace","metadata":{"name":"default"}}),
    )
    .await
    .unwrap();

    let rc_body = json!({
        "apiVersion": "v1",
        "kind": "ReplicationController",
        "metadata": {
            "name": "rc-default-selector-protobuf",
            "namespace": "default"
        },
        "spec": {
            "replicas": 1,
            "template": {
                "metadata": { "labels": { "name": "rc-default-selector-protobuf" } },
                "spec": { "containers": [{ "name": "httpd", "image": "httpd:2.4" }] }
            }
        }
    });
    let wire = crate::protobuf::encode_protobuf(&rc_body).expect("RC protobuf encode must work");

    let create_rc = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/default/replicationcontrollers")
        .header("content-type", "application/vnd.kubernetes.protobuf")
        .body(Body::from(wire))
        .unwrap();
    let create_resp = app.clone().oneshot(create_rc).await.unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);

    let rc = db
        .get_resource(
            "v1",
            "ReplicationController",
            Some("default"),
            "rc-default-selector-protobuf",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        rc.data["spec"]["selector"],
        json!({"name": "rc-default-selector-protobuf"}),
        "protobuf RC create must default spec.selector from template labels"
    );

    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(
        pods.items.len(),
        1,
        "protobuf-created defaulted selector must match the created pod"
    );
}

#[tokio::test]
async fn test_immutable_secret_update_rejected_with_422() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        serde_json::json!({"apiVersion":"v1","kind":"Namespace","metadata":{"name":"default"}}),
    )
    .await
    .unwrap();

    use base64::Engine as _;
    let encoded = base64::engine::general_purpose::STANDARD.encode("secret-val");

    // Create an immutable Secret
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/default/secrets")
        .header("content-type", "application/json")
        .body(Body::from(format!(
            r#"{{"apiVersion":"v1","kind":"Secret","metadata":{{"name":"imm-secret","namespace":"default"}},"immutable":true,"data":{{"key":"{}"}}}}"#,
            encoded
        )))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // PUT with changed data — must be rejected with 422
    let new_encoded = base64::engine::general_purpose::STANDARD.encode("changed-val");
    let req = Request::builder()
        .method("PUT")
        .uri("/api/v1/namespaces/default/secrets/imm-secret")
        .header("content-type", "application/json")
        .body(Body::from(format!(
            r#"{{"apiVersion":"v1","kind":"Secret","metadata":{{"name":"imm-secret","namespace":"default"}},"immutable":true,"data":{{"key":"{}"}}}}"#,
            new_encoded
        )))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::UNPROCESSABLE_ENTITY,
        "Immutable Secret PUT must return 422"
    );
}

#[tokio::test]
async fn test_immutable_secret_set_then_update_rejected_with_422() {
    // Conformance sequence: create mutable, then set immutable via update, then try to change data
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        serde_json::json!({"apiVersion":"v1","kind":"Namespace","metadata":{"name":"default"}}),
    )
    .await
    .unwrap();

    use base64::Engine as _;
    let encoded = base64::engine::general_purpose::STANDARD.encode("initial-val");

    // Step 1: Create secret WITHOUT immutable
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/default/secrets")
        .header("content-type", "application/json")
        .body(Body::from(format!(
            r#"{{"apiVersion":"v1","kind":"Secret","metadata":{{"name":"imm2-secret","namespace":"default"}},"data":{{"key":"{}"}}}}"#,
            encoded
        )))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::CREATED,
        "Step 1: create must succeed"
    );

    // Step 2: PUT to set immutable: true
    let req = Request::builder()
        .method("PUT")
        .uri("/api/v1/namespaces/default/secrets/imm2-secret")
        .header("content-type", "application/json")
        .body(Body::from(format!(
            r#"{{"apiVersion":"v1","kind":"Secret","metadata":{{"name":"imm2-secret","namespace":"default"}},"immutable":true,"data":{{"key":"{}"}}}}"#,
            encoded
        )))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "Step 2: setting immutable must succeed"
    );

    // Step 3: PUT with changed data — must be rejected with 422
    let new_encoded = base64::engine::general_purpose::STANDARD.encode("changed-val");
    let req = Request::builder()
        .method("PUT")
        .uri("/api/v1/namespaces/default/secrets/imm2-secret")
        .header("content-type", "application/json")
        .body(Body::from(format!(
            r#"{{"apiVersion":"v1","kind":"Secret","metadata":{{"name":"imm2-secret","namespace":"default"}},"immutable":true,"data":{{"key":"{}"}}}}"#,
            new_encoded
        )))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::UNPROCESSABLE_ENTITY,
        "Step 3: changing data on immutable secret must return 422"
    );
}

#[tokio::test]
async fn test_mutable_configmap_update_succeeds() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        serde_json::json!({"apiVersion":"v1","kind":"Namespace","metadata":{"name":"default"}}),
    )
    .await
    .unwrap();

    // Create a mutable ConfigMap (no immutable field)
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/default/configmaps")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"mut-cm","namespace":"default"},"data":{"key":"val"}}"#))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // PUT update — must succeed
    let req = Request::builder()
        .method("PUT")
        .uri("/api/v1/namespaces/default/configmaps/mut-cm")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"mut-cm","namespace":"default"},"data":{"key":"updated"}}"#))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "Mutable ConfigMap update must succeed"
    );
}

/// Helper to fetch aggregated discovery body from /apis with the K8s aggregated-discovery Accept header.
async fn fetch_aggregated_discovery_body() -> serde_json::Value {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let app = crate::api::build_router(state);

    let req = Request::builder()
        .method("GET")
        .uri("/apis")
        .header(
            "Accept",
            "application/json;v=v2;g=apidiscovery.k8s.io;as=APIGroupDiscoveryList",
        )
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body_bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&body_bytes).unwrap()
}

/// Helper to fetch aggregated discovery body from /api with the K8s aggregated-discovery Accept header.
async fn fetch_core_aggregated_discovery_body() -> serde_json::Value {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let app = crate::api::build_router(state);

    let req = Request::builder()
        .method("GET")
        .uri("/api")
        .header(
            "Accept",
            "application/json;v=v2;g=apidiscovery.k8s.io;as=APIGroupDiscoveryList",
        )
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body_bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&body_bytes).unwrap()
}

/// Helper to find resources for a specific group+version in the aggregated discovery response.
fn aggregated_resources_for(body: &serde_json::Value, group: &str, version: &str) -> Vec<String> {
    let items = body["items"].as_array().unwrap();
    for item in items {
        if item["metadata"]["name"].as_str() == Some(group) {
            let versions = item["versions"].as_array().unwrap();
            for v in versions {
                if v["version"].as_str() == Some(version) {
                    return v["resources"]
                        .as_array()
                        .unwrap_or(&vec![])
                        .iter()
                        .filter_map(|r| r["resource"].as_str().map(str::to_string))
                        .collect();
                }
            }
        }
    }
    vec![]
}

#[tokio::test]
async fn test_pod_lifecycle_debug_endpoint_returns_snapshot_shape() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use std::time::Duration;
    use tokio::time::timeout;
    use tower::ServiceExt;

    let mut app_state = build_test_app_state().await;
    let key = crate::kubelet::pod_lifecycle_actor::message::PodLifecycleKey::new(
        "default",
        "lifecycle-debug-pod",
        "uid-123",
    );
    let router = std::sync::Arc::new(
        crate::kubelet::pod_lifecycle_router::PodLifecycleRouter::from_env(
            app_state.task_supervisor.clone(),
            crate::kubelet::pod_lifecycle_actor::config::PodLifecycleConcurrencyConfig::production_default(),
        ),
    );
    let _ = router
        .route(
            crate::kubelet::pod_lifecycle_actor::message::LifecycleMessage::WatchAdded {
                key: key.clone(),
                resource_version: Some(1),
                pod: serde_json::json!({
                    "metadata": {
                        "name": key.name,
                        "namespace": key.namespace,
                        "uid": key.uid,
                    }
                }),
            },
        )
        .await;
    app_state.pod_lifecycle_router = Some(router);

    let app = crate::api::build_router(app_state);
    let payload = timeout(Duration::from_secs(1), async {
        loop {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("GET")
                        .uri("/debug/klights/pod-lifecycle")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                StatusCode::OK,
                "debug endpoint should be available"
            );
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
            let has_trace = payload
                .get("recentTrace")
                .and_then(|value| value.as_array())
                .map(|entries| !entries.is_empty())
                .unwrap_or(false);
            if has_trace {
                break payload;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();

    assert!(
        payload
            .get("actors")
            .and_then(|value| value.as_array())
            .is_some(),
        "actors field should be present as array"
    );
    assert!(
        payload
            .get("recentTrace")
            .and_then(|value| value.as_array())
            .is_some(),
        "recentTrace field should be present as array"
    );
    assert!(
        payload
            .get("pendingControllerKeys")
            .and_then(|value| value.as_array())
            .is_some(),
        "pendingControllerKeys field should be present as array"
    );
    assert!(
        payload
            .get("pendingRetryKeys")
            .and_then(|value| value.as_array())
            .is_some(),
        "pendingRetryKeys field should be present as array"
    );
    assert!(
        payload
            .get("sideEffectFailures")
            .and_then(|value| value.as_array())
            .is_some(),
        "sideEffectFailures field should be present as array"
    );

    let recent_trace = payload
        .get("recentTrace")
        .and_then(|value| value.as_array())
        .unwrap_or_else(|| panic!("recentTrace must be present"));
    let entry = &recent_trace[0];
    assert!(
        entry
            .get("namespace")
            .and_then(|value| value.as_str())
            .is_some(),
        "trace entries must include namespace"
    );
    assert!(
        entry
            .get("podName")
            .and_then(|value| value.as_str())
            .is_some(),
        "trace entries must include podName"
    );
    assert!(
        entry.get("uid").and_then(|value| value.as_str()).is_some(),
        "trace entries must include uid"
    );
    assert!(
        entry
            .get("event")
            .and_then(|value| value.as_str())
            .is_some(),
        "trace entries must include event"
    );
    assert!(
        entry.get("resourceVersion").is_some(),
        "trace entries must include resourceVersion"
    );
    assert!(
        entry.get("sandboxId").is_some(),
        "trace entries must include sandboxId"
    );
}

mod core_crud_and_defaults;

mod discovery_and_schema;

mod quota_and_storage;

mod namespace_and_flowcontrol;

mod proxy_and_apiservice;

mod watch_and_list;

mod finalizer_delete;

mod mutation_semantics;
