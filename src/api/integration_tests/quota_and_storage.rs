use super::*;

#[tokio::test]
async fn test_put_node_status_preserves_extended_resource_capacity() {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let node_name = state.config.node_name.clone();
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "Node",
        None,
        &node_name,
        json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": node_name},
            "spec": {},
            "status": {
                "conditions": [{"type": "Ready", "status": "True"}],
                "capacity": {
                    "cpu": "8",
                    "memory": "8Gi",
                    "pods": "110",
                    "example.com/fakecpu": "1000"
                },
                "allocatable": {
                    "cpu": "8",
                    "memory": "8Gi",
                    "pods": "110",
                    "example.com/fakecpu": "1000"
                }
            }
        }),
    )
    .await
    .unwrap();

    let put = json!({
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {"name": node_name},
        "status": {
            "conditions": [{"type": "Ready", "status": "True"}],
            "capacity": {"cpu": "8", "memory": "8Gi", "pods": "110"},
            "allocatable": {"cpu": "8", "memory": "8Gi", "pods": "110"}
        }
    });
    let req = Request::builder()
        .method("PUT")
        .uri(format!("/api/v1/nodes/{node_name}/status"))
        .header("content-type", "application/json")
        .body(Body::from(put.to_string()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(resp.into_body(), usize::MAX).await.unwrap()).unwrap();

    assert_eq!(
        body.pointer("/status/capacity/example.com~1fakecpu"),
        Some(&json!("1000")),
        "Node status PUT must not erase extended resource capacity reported through status patch"
    );
    assert_eq!(
        body.pointer("/status/allocatable/example.com~1fakecpu"),
        Some(&json!("1000")),
        "Node status PUT must not erase extended resource allocatable reported through status patch"
    );
}

#[tokio::test]
async fn test_patch_node_status_preserves_extended_resource_capacity() {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let node_name = state.config.node_name.clone();
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "Node",
        None,
        &node_name,
        json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": node_name},
            "spec": {},
            "status": {
                "conditions": [{"type": "Ready", "status": "True"}],
                "capacity": {"cpu": "8", "memory": "8Gi", "pods": "110"},
                "allocatable": {"cpu": "8", "memory": "8Gi", "pods": "110"}
            }
        }),
    )
    .await
    .unwrap();

    let patch = json!({
        "status": {
            "capacity": {"example.com/fakecpu": "1000"},
            "allocatable": {"example.com/fakecpu": "1000"}
        }
    });
    let req = Request::builder()
        .method("PATCH")
        .uri(format!("/api/v1/nodes/{node_name}/status"))
        .header("content-type", "application/strategic-merge-patch+json")
        .body(Body::from(patch.to_string()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(resp.into_body(), usize::MAX).await.unwrap()).unwrap();

    assert_eq!(
        body.pointer("/status/capacity/example.com~1fakecpu"),
        Some(&json!("1000")),
        "Node status PATCH must preserve extended resource capacity"
    );
    assert_eq!(
        body.pointer("/status/allocatable/example.com~1fakecpu"),
        Some(&json!("1000")),
        "Node status PATCH must preserve extended resource allocatable"
    );
}

#[tokio::test]
async fn test_json_patch_node_status_preserves_extended_resource_capacity() {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let node_name = state.config.node_name.clone();
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "Node",
        None,
        &node_name,
        json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": node_name},
            "spec": {},
            "status": {
                "conditions": [{"type": "Ready", "status": "True"}],
                "capacity": {"cpu": "8", "memory": "8Gi", "pods": "110"},
                "allocatable": {"cpu": "8", "memory": "8Gi", "pods": "110"}
            }
        }),
    )
    .await
    .unwrap();

    let patch = json!([
        {"op": "add", "path": "/status/capacity/example.com~1fakecpu", "value": "1000"},
        {"op": "add", "path": "/status/allocatable/example.com~1fakecpu", "value": "1000"}
    ]);
    let req = Request::builder()
        .method("PATCH")
        .uri(format!("/api/v1/nodes/{node_name}/status"))
        .header("content-type", "application/json-patch+json")
        .body(Body::from(patch.to_string()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(resp.into_body(), usize::MAX).await.unwrap()).unwrap();

    assert_eq!(
        body.pointer("/status/capacity/example.com~1fakecpu"),
        Some(&json!("1000")),
        "Node status JSON Patch must unescape extended resource capacity paths"
    );
    assert_eq!(
        body.pointer("/status/allocatable/example.com~1fakecpu"),
        Some(&json!("1000")),
        "Node status JSON Patch must unescape extended resource allocatable paths"
    );
}

#[tokio::test]
async fn test_resourcequota_pod_create_denies_when_cpu_or_memory_would_exceed_hard_limit() {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let app = build_test_router().await;

    let ns_req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"rq-enforce"}}"#,
        ))
        .unwrap();
    let ns_resp = app.clone().oneshot(ns_req).await.unwrap();
    assert_eq!(ns_resp.status(), StatusCode::CREATED);

    let rq_req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/rq-enforce/resourcequotas")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{
                "apiVersion":"v1",
                "kind":"ResourceQuota",
                "metadata":{"name":"rq-cpu-mem","namespace":"rq-enforce"},
                "spec":{"hard":{"pods":"5","cpu":"500m","memory":"256Mi"}}
            }"#,
        ))
        .unwrap();
    let rq_resp = app.clone().oneshot(rq_req).await.unwrap();
    assert_eq!(rq_resp.status(), StatusCode::CREATED);

    let pod_payload = r#"{
        "apiVersion":"v1",
        "kind":"Pod",
        "metadata":{"name":"pod-a","namespace":"rq-enforce"},
        "spec":{
            "containers":[{
                "name":"c",
                "image":"busybox",
                "resources":{"requests":{"cpu":"500m","memory":"252Mi"}}
            }]
        }
    }"#;

    let first_pod_req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/rq-enforce/pods")
        .header("content-type", "application/json")
        .body(Body::from(pod_payload))
        .unwrap();
    let first_pod_resp = app.clone().oneshot(first_pod_req).await.unwrap();
    assert_eq!(first_pod_resp.status(), StatusCode::CREATED);

    let second_pod_req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/rq-enforce/pods")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{
                "apiVersion":"v1",
                "kind":"Pod",
                "metadata":{"name":"pod-b","namespace":"rq-enforce"},
                "spec":{
                    "containers":[{
                        "name":"c",
                        "image":"busybox",
                        "resources":{"requests":{"cpu":"500m","memory":"252Mi"}}
                    }]
                }
            }"#,
        ))
        .unwrap();
    let second_pod_resp = app.clone().oneshot(second_pod_req).await.unwrap();
    assert_eq!(
        second_pod_resp.status(),
        StatusCode::FORBIDDEN,
        "second pod should be denied when quota requests exceed hard cpu/memory limit"
    );
    let second_body = axum::body::to_bytes(second_pod_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let second_text = String::from_utf8_lossy(&second_body);
    assert!(
        second_text.contains("exceeded quota"),
        "quota rejection should include exceeded quota message, got: {}",
        second_text
    );
}

#[tokio::test]
async fn test_resourcequota_pod_update_denies_when_requests_would_exceed_hard_limit() {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let app = build_test_router().await;

    let ns_req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"rq-update"}}"#,
        ))
        .unwrap();
    let ns_resp = app.clone().oneshot(ns_req).await.unwrap();
    assert_eq!(ns_resp.status(), StatusCode::CREATED);

    let rq_req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/rq-update/resourcequotas")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{
                "apiVersion":"v1",
                "kind":"ResourceQuota",
                "metadata":{"name":"rq-cpu-mem","namespace":"rq-update"},
                "spec":{"hard":{"pods":"5","cpu":"500m","memory":"256Mi"}}
            }"#,
        ))
        .unwrap();
    let rq_resp = app.clone().oneshot(rq_req).await.unwrap();
    assert_eq!(rq_resp.status(), StatusCode::CREATED);

    let create_req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/rq-update/pods")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{
                "apiVersion":"v1",
                "kind":"Pod",
                "metadata":{"name":"test-pod","namespace":"rq-update"},
                "spec":{
                    "containers":[{
                        "name":"c",
                        "image":"busybox",
                        "resources":{"requests":{"cpu":"100m","memory":"128Mi"}}
                    }]
                }
            }"#,
        ))
        .unwrap();
    let create_resp = app.clone().oneshot(create_req).await.unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);

    let update_req = Request::builder()
        .method("PUT")
        .uri("/api/v1/namespaces/rq-update/pods/test-pod")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{
                "apiVersion":"v1",
                "kind":"Pod",
                "metadata":{"name":"test-pod","namespace":"rq-update"},
                "spec":{
                    "containers":[{
                        "name":"c",
                        "image":"busybox",
                        "resources":{"requests":{"cpu":"600m","memory":"300Mi"}}
                    }]
                }
            }"#,
        ))
        .unwrap();
    let update_resp = app.clone().oneshot(update_req).await.unwrap();
    assert_eq!(
        update_resp.status(),
        StatusCode::FORBIDDEN,
        "pod update must be denied when requested resources exceed quota hard limits"
    );
    let update_body = axum::body::to_bytes(update_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let update_text = String::from_utf8_lossy(&update_body);
    assert!(
        update_text.contains("exceeded quota")
            || update_text.contains("may not change container resource requirements"),
        "pod update should be rejected by quota/immutability validation, got: {}",
        update_text
    );
}

#[tokio::test]
async fn test_pod_update_rejects_changes_to_container_resource_requirements() {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let app = build_test_router().await;

    let ns_req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"pod-immutability"}}"#,
        ))
        .unwrap();
    let ns_resp = app.clone().oneshot(ns_req).await.unwrap();
    assert_eq!(ns_resp.status(), StatusCode::CREATED);

    let create_req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/pod-immutability/pods")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{
                "apiVersion":"v1",
                "kind":"Pod",
                "metadata":{"name":"test-pod","namespace":"pod-immutability"},
                "spec":{
                    "containers":[{
                        "name":"c",
                        "image":"busybox",
                        "resources":{"requests":{"cpu":"500m","memory":"252Mi"}}
                    }]
                }
            }"#,
        ))
        .unwrap();
    let create_resp = app.clone().oneshot(create_req).await.unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);

    let update_req = Request::builder()
        .method("PUT")
        .uri("/api/v1/namespaces/pod-immutability/pods/test-pod")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{
                "apiVersion":"v1",
                "kind":"Pod",
                "metadata":{"name":"test-pod","namespace":"pod-immutability"},
                "spec":{
                    "containers":[{
                        "name":"c",
                        "image":"busybox",
                        "resources":{"requests":{"cpu":"100m","memory":"100Mi"}}
                    }]
                }
            }"#,
        ))
        .unwrap();
    let update_resp = app.clone().oneshot(update_req).await.unwrap();
    assert_eq!(
        update_resp.status(),
        StatusCode::FORBIDDEN,
        "pod update must reject resource requirement mutations"
    );
}

#[tokio::test]
async fn test_volumeattachment_status_get_put_patch() {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let app = build_test_router().await;

    // Create VolumeAttachment (cluster-scoped)
    let va_body = r#"{"apiVersion":"storage.k8s.io/v1","kind":"VolumeAttachment","metadata":{"name":"test-va"},"spec":{"attacher":"driver.csi.k8s.io","nodeName":"node1","source":{"persistentVolumeName":"pv1"}}}"#;
    let req = Request::builder()
        .method("POST")
        .uri("/apis/storage.k8s.io/v1/volumeattachments")
        .header("content-type", "application/json")
        .body(Body::from(va_body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::CREATED,
        "VolumeAttachment create must return 201"
    );

    // GET /status
    let req = Request::builder()
        .method("GET")
        .uri("/apis/storage.k8s.io/v1/volumeattachments/test-va/status")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "GET VolumeAttachment /status must return 200 not 404"
    );

    // PUT /status
    let put_body = r#"{"apiVersion":"storage.k8s.io/v1","kind":"VolumeAttachment","metadata":{"name":"test-va"},"spec":{"attacher":"driver.csi.k8s.io","nodeName":"node1","source":{"persistentVolumeName":"pv1"}},"status":{"attached":true}}"#;
    let req = Request::builder()
        .method("PUT")
        .uri("/apis/storage.k8s.io/v1/volumeattachments/test-va/status")
        .header("content-type", "application/json")
        .body(Body::from(put_body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "PUT VolumeAttachment /status must return 200"
    );
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let updated: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        updated["status"]["attached"], true,
        "PUT /status must update status.attached"
    );

    // PATCH /status
    let patch_body = r#"{"status":{"attached":false}}"#;
    let req = Request::builder()
        .method("PATCH")
        .uri("/apis/storage.k8s.io/v1/volumeattachments/test-va/status")
        .header("content-type", "application/merge-patch+json")
        .body(Body::from(patch_body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "PATCH VolumeAttachment /status must return 200"
    );
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let patched: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        patched["status"]["attached"], false,
        "PATCH /status must update status.attached"
    );
}

#[tokio::test]
async fn test_crd_status_get_put_patch() {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let app = build_test_router().await;

    // Create CRD (cluster-scoped)
    let crd_body = r#"{"apiVersion":"apiextensions.k8s.io/v1","kind":"CustomResourceDefinition","metadata":{"name":"foos.example.com"},"spec":{"group":"example.com","names":{"kind":"Foo","plural":"foos","singular":"foo"},"scope":"Namespaced","versions":[{"name":"v1","served":true,"storage":true,"schema":{"openAPIV3Schema":{"type":"object"}}}]}}"#;
    let req = Request::builder()
        .method("POST")
        .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions")
        .header("content-type", "application/json")
        .body(Body::from(crd_body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::CREATED,
        "CRD create must return 201"
    );

    // GET /status
    let req = Request::builder()
        .method("GET")
        .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions/foos.example.com/status")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "GET CRD /status must return 200 not 404"
    );

    // PUT /status — update conditions
    let put_body = r#"{"apiVersion":"apiextensions.k8s.io/v1","kind":"CustomResourceDefinition","metadata":{"name":"foos.example.com"},"spec":{"group":"example.com","names":{"kind":"Foo","plural":"foos"},"scope":"Namespaced","versions":[{"name":"v1","served":true,"storage":true}]},"status":{"conditions":[{"type":"Established","status":"True","reason":"InitialNamesAccepted"}],"acceptedNames":{"kind":"Foo","plural":"foos"}}}"#;
    let req = Request::builder()
        .method("PUT")
        .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions/foos.example.com/status")
        .header("content-type", "application/json")
        .body(Body::from(put_body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "PUT CRD /status must return 200"
    );
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let updated: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        updated["status"]["conditions"][0]["type"], "Established",
        "PUT /status must update conditions"
    );

    // PATCH /status
    let patch_body = r#"{"status":{"conditions":[{"type":"NamesAccepted","status":"True","reason":"NoConflicts"}]}}"#;
    let req = Request::builder()
        .method("PATCH")
        .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions/foos.example.com/status")
        .header("content-type", "application/merge-patch+json")
        .body(Body::from(patch_body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "PATCH CRD /status must return 200"
    );
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let patched: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        patched["status"]["conditions"][0]["type"], "NamesAccepted",
        "PATCH /status must update conditions"
    );
}

#[tokio::test]
async fn test_crd_create_response_rv_tracks_established_status_update() {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let (app, db) = build_test_router_with_db().await;

    let crd_body = r#"{"apiVersion":"apiextensions.k8s.io/v1","kind":"CustomResourceDefinition","metadata":{"name":"bars.example.com"},"spec":{"group":"example.com","names":{"kind":"Bar","plural":"bars","singular":"bar"},"scope":"Namespaced","versions":[{"name":"v1","served":true,"storage":true,"schema":{"openAPIV3Schema":{"type":"object"}}}]}}"#;
    let req = Request::builder()
        .method("POST")
        .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions")
        .header("content-type", "application/json")
        .body(Body::from(crd_body))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let created: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let response_rv: i64 = created
        .pointer("/metadata/resourceVersion")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0);
    assert!(
        response_rv > 0,
        "create response must include resourceVersion"
    );

    let stored = db
        .get_resource(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            "bars.example.com",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        response_rv, stored.resource_version,
        "CRD create response rv must reflect post-create status update rv so watch catch-up from this rv does not replay a stale MODIFIED event before DELETE"
    );
}

#[tokio::test]
async fn test_csistoragecapacity_crud() {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let app = build_test_router().await;

    // Create namespace first
    let ns_body = r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"default"}}"#;
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces")
        .header("content-type", "application/json")
        .body(Body::from(ns_body))
        .unwrap();
    let _ = app.clone().oneshot(req).await.unwrap();

    // Create CSIStorageCapacity (namespaced in storage.k8s.io/v1)
    let csc_body = r#"{"apiVersion":"storage.k8s.io/v1","kind":"CSIStorageCapacity","metadata":{"name":"test-csc","namespace":"default"},"storageClassName":"my-storage-class","capacity":"10Gi"}"#;
    let req = Request::builder()
        .method("POST")
        .uri("/apis/storage.k8s.io/v1/namespaces/default/csistoragecapacities")
        .header("content-type", "application/json")
        .body(Body::from(csc_body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::CREATED,
        "CSIStorageCapacity create must return 201"
    );

    // GET
    let req = Request::builder()
        .method("GET")
        .uri("/apis/storage.k8s.io/v1/namespaces/default/csistoragecapacities/test-csc")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "CSIStorageCapacity get must return 200"
    );

    // LIST (namespaced)
    let req = Request::builder()
        .method("GET")
        .uri("/apis/storage.k8s.io/v1/namespaces/default/csistoragecapacities")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "CSIStorageCapacity list must return 200"
    );

    // LIST (all namespaces)
    let req = Request::builder()
        .method("GET")
        .uri("/apis/storage.k8s.io/v1/csistoragecapacities")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "CSIStorageCapacity cluster-wide list must return 200"
    );

    // DELETE
    let req = Request::builder()
        .method("DELETE")
        .uri("/apis/storage.k8s.io/v1/namespaces/default/csistoragecapacities/test-csc")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "CSIStorageCapacity delete must return 200"
    );
}

#[tokio::test]
async fn test_csistoragecapacity_list_protobuf_decodes_as_native_list() {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use prost::Message;
    use tower::ServiceExt;

    let app = build_test_router().await;

    let ns_body = r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"csc-protobuf"}}"#;
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces")
        .header("content-type", "application/json")
        .body(Body::from(ns_body))
        .unwrap();
    let _ = app.clone().oneshot(req).await.unwrap();

    let csc_body = r#"{"apiVersion":"storage.k8s.io/v1","kind":"CSIStorageCapacity","metadata":{"name":"test-csc","namespace":"csc-protobuf","labels":{"test":"csc-protobuf"}},"storageClassName":"my-storage-class","capacity":"10Gi"}"#;
    let req = Request::builder()
        .method("POST")
        .uri("/apis/storage.k8s.io/v1/namespaces/csc-protobuf/csistoragecapacities")
        .header("content-type", "application/json")
        .body(Body::from(csc_body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let req = Request::builder()
        .method("GET")
        .uri("/apis/storage.k8s.io/v1/namespaces/csc-protobuf/csistoragecapacities?labelSelector=test%3Dcsc-protobuf")
        .header("accept", "application/vnd.kubernetes.protobuf")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    assert_eq!(&body[0..4], b"k8s\0");
    let unknown = crate::protobuf::Unknown::decode(&body[4..]).unwrap();
    assert!(
        unknown.content_type.is_empty(),
        "CSIStorageCapacityList must use native protobuf, not JSON-in-Unknown"
    );
    let decoded =
        k8s_pb::api::storage::v1::CSIStorageCapacityList::decode(unknown.raw.as_slice()).unwrap();
    assert_eq!(decoded.items.len(), 1);
    assert_eq!(
        decoded.items[0].storage_class_name.as_deref(),
        Some("my-storage-class")
    );
}

#[tokio::test]
async fn test_csidriver_crud() {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let app = build_test_router().await;

    let body = r#"{"apiVersion":"storage.k8s.io/v1","kind":"CSIDriver","metadata":{"name":"test.csi.klights"},"spec":{"attachRequired":false,"podInfoOnMount":false}}"#;
    let req = Request::builder()
        .method("POST")
        .uri("/apis/storage.k8s.io/v1/csidrivers")
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let req = Request::builder()
        .method("GET")
        .uri("/apis/storage.k8s.io/v1/csidrivers/test.csi.klights")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let req = Request::builder()
        .method("GET")
        .uri("/apis/storage.k8s.io/v1/csidrivers")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let req = Request::builder()
        .method("DELETE")
        .uri("/apis/storage.k8s.io/v1/csidrivers/test.csi.klights")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_pvc_status_subresource_get() {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let app = build_test_router().await;

    let ns_body = r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"pvc-status-test"}}"#;
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces")
        .header("content-type", "application/json")
        .body(Body::from(ns_body))
        .unwrap();
    let _ = app.clone().oneshot(req).await.unwrap();

    let pvc_body = r#"{"apiVersion":"v1","kind":"PersistentVolumeClaim","metadata":{"name":"pvc1","namespace":"pvc-status-test"},"spec":{"accessModes":["ReadWriteOnce"],"resources":{"requests":{"storage":"1Gi"}}}}"#;
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/pvc-status-test/persistentvolumeclaims")
        .header("content-type", "application/json")
        .body(Body::from(pvc_body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/namespaces/pvc-status-test/persistentvolumeclaims/pvc1/status")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        body["status"]["phase"], "Pending",
        "PVC create should default status.phase=Pending"
    );
}

#[tokio::test]
async fn test_patch_pvc_status_merge_patch_returns_conditions() {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let app = build_test_router().await;

    let ns_body =
        r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"pvc-status-patch"}}"#;
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces")
        .header("content-type", "application/json")
        .body(Body::from(ns_body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let pvc_body = r#"{
        "apiVersion":"v1",
        "kind":"PersistentVolumeClaim",
        "metadata":{"name":"pvc1","namespace":"pvc-status-patch"},
        "spec":{
            "accessModes":["ReadWriteOnce"],
            "resources":{"requests":{"storage":"1Gi"}}
        }
    }"#;
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/pvc-status-patch/persistentvolumeclaims")
        .header("content-type", "application/json")
        .body(Body::from(pvc_body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let patch = r#"{"status":{"conditions":[{"type":"StatusPatched","status":"True","reason":"E2E patchedStatus","message":"Set from e2e test"}]}}"#;
    let req = Request::builder()
        .method("PATCH")
        .uri("/api/v1/namespaces/pvc-status-patch/persistentvolumeclaims/pvc1/status")
        .header("content-type", "application/merge-patch+json")
        .body(Body::from(patch))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(resp.into_body(), usize::MAX).await.unwrap()).unwrap();
    let condition = body
        .pointer("/status/conditions/0")
        .expect("patched PVC status condition must be returned");
    assert_eq!(condition["type"], "StatusPatched");
    assert_eq!(condition["status"], "True");
    assert_eq!(condition["reason"], "E2E patchedStatus");
    assert_eq!(condition["message"], "Set from e2e test");
}

#[tokio::test]
async fn test_patch_pvc_status_condition_survives_stale_controller_status_commit() {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    let ns_body = r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"pvc-status-race"}}"#;
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces")
        .header("content-type", "application/json")
        .body(Body::from(ns_body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let pvc_body = r#"{
        "apiVersion":"v1",
        "kind":"PersistentVolumeClaim",
        "metadata":{"name":"pvc1","namespace":"pvc-status-race","uid":"pvc-status-race-uid"},
        "spec":{
            "accessModes":["ReadWriteOnce"],
            "resources":{"requests":{"storage":"1Gi"}}
        }
    }"#;
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/pvc-status-race/persistentvolumeclaims")
        .header("content-type", "application/json")
        .body(Body::from(pvc_body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let created = db
        .get_resource(
            "v1",
            "PersistentVolumeClaim",
            Some("pvc-status-race"),
            "pvc1",
        )
        .await
        .unwrap()
        .expect("PVC should exist after API create");

    let patch = r#"{"status":{"conditions":[{"type":"StatusPatched","status":"True","reason":"E2E patchedStatus","message":"Set from e2e test"}]}}"#;
    let req = Request::builder()
        .method("PATCH")
        .uri("/api/v1/namespaces/pvc-status-race/persistentvolumeclaims/pvc1/status")
        .header("content-type", "application/merge-patch+json")
        .body(Body::from(patch))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let patched = db
        .get_resource(
            "v1",
            "PersistentVolumeClaim",
            Some("pvc-status-race"),
            "pvc1",
        )
        .await
        .unwrap()
        .expect("PVC should exist after status patch");

    let committed_status = crate::log_apply::LogApplyCommit::new(
        patched.resource_version + 1,
        vec![crate::log_apply::LogApplyMutation::PutResource(
            crate::log_apply::LogApplyResourceRow {
                api_version: "v1".to_string(),
                kind: "PersistentVolumeClaim".to_string(),
                namespace: Some("pvc-status-race".to_string()),
                name: "pvc1".to_string(),
                uid: created.uid.clone(),
                resource_version: patched.resource_version + 1,
                data: json!({
                    "apiVersion": "v1",
                    "kind": "PersistentVolumeClaim",
                    "metadata": {
                        "name": "pvc1",
                        "namespace": "pvc-status-race",
                        "uid": created.uid.clone(),
                        "resourceVersion": (patched.resource_version + 1).to_string()
                    },
                    "spec": {
                        "accessModes": ["ReadWriteOnce"],
                        "resources": {"requests": {"storage": "1Gi"}}
                    },
                    "status": {
                        "phase": "Bound",
                        "accessModes": ["ReadWriteOnce"],
                        "capacity": {"storage": "1Gi"},
                        "volumeName": "pv-pvc-status-race"
                    }
                }),
                require_absent: false,
                require_existing: true,
                precondition_uid: Some(created.uid.clone()),
                precondition_resource_version: Some(created.resource_version),
                status_only: true,
            },
        )],
    );
    let result = db
        .apply_raft_log_apply_commit(committed_status)
        .await
        .expect("stale controller status commit should apply through raft");
    assert_eq!(result.applied_rv, Some(patched.resource_version + 1));

    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/namespaces/pvc-status-race/persistentvolumeclaims/pvc1/status")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(resp.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(body.pointer("/status/phase"), Some(&json!("Bound")));
    assert_eq!(
        body.pointer("/status/conditions/0/type"),
        Some(&json!("StatusPatched")),
        "PVC /status patch condition must survive stale controller status commit"
    );
}

#[tokio::test]
async fn test_pv_status_defaults_phase_available_on_create() {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let app = build_test_router().await;

    let pv_body = r#"{
        "apiVersion":"v1",
        "kind":"PersistentVolume",
        "metadata":{"name":"pv-status-default-available"},
        "spec":{
            "capacity":{"storage":"1Gi"},
            "accessModes":["ReadWriteOnce"],
            "persistentVolumeReclaimPolicy":"Retain",
            "hostPath":{"path":"/tmp/pv-status-default-available"}
        },
        "status":{}
    }"#;

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/persistentvolumes")
        .header("content-type", "application/json")
        .body(Body::from(pv_body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/persistentvolumes/pv-status-default-available/status")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        body["status"]["phase"], "Available",
        "PV create should default status.phase=Available when no claimRef"
    );
}

#[tokio::test]
async fn test_runtimeclass_crud() {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let app = build_test_router().await;

    // Create RuntimeClass (cluster-scoped in node.k8s.io/v1)
    let rc_body = r#"{"apiVersion":"node.k8s.io/v1","kind":"RuntimeClass","metadata":{"name":"gvisor"},"handler":"runsc"}"#;
    let req = Request::builder()
        .method("POST")
        .uri("/apis/node.k8s.io/v1/runtimeclasses")
        .header("content-type", "application/json")
        .body(Body::from(rc_body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::CREATED,
        "RuntimeClass create must return 201"
    );

    // GET
    let req = Request::builder()
        .method("GET")
        .uri("/apis/node.k8s.io/v1/runtimeclasses/gvisor")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "RuntimeClass get must return 200"
    );

    // LIST
    let req = Request::builder()
        .method("GET")
        .uri("/apis/node.k8s.io/v1/runtimeclasses")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "RuntimeClass list must return 200"
    );

    // DELETE
    let req = Request::builder()
        .method("DELETE")
        .uri("/apis/node.k8s.io/v1/runtimeclasses/gvisor")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "RuntimeClass delete must return 200"
    );
}

#[tokio::test]
async fn test_runtimeclass_admission_rejects_pod_with_missing_runtimeclass() {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let app = build_test_router().await;

    // Create namespace
    let ns_body = r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"default"}}"#;
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces")
        .header("content-type", "application/json")
        .body(Body::from(ns_body))
        .unwrap();
    let _ = app.clone().oneshot(req).await.unwrap();

    // Create a RuntimeClass
    let rc_body = r#"{"apiVersion":"node.k8s.io/v1","kind":"RuntimeClass","metadata":{"name":"gvisor"},"handler":"runsc"}"#;
    let req = Request::builder()
        .method("POST")
        .uri("/apis/node.k8s.io/v1/runtimeclasses")
        .header("content-type", "application/json")
        .body(Body::from(rc_body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Delete the RuntimeClass
    let req = Request::builder()
        .method("DELETE")
        .uri("/apis/node.k8s.io/v1/runtimeclasses/gvisor")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Create a Pod referencing the deleted RuntimeClass — must be rejected
    let pod_body = r#"{"apiVersion":"v1","kind":"Pod","metadata":{"name":"test-pod","namespace":"default"},"spec":{"runtimeClassName":"gvisor","containers":[{"name":"c","image":"busybox"}]}}"#;
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/default/pods")
        .header("content-type", "application/json")
        .body(Body::from(pod_body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "Pod with missing RuntimeClass must be rejected with 403"
    );
}

#[tokio::test]
async fn test_runtimeclass_admission_rejects_protobuf_pod_with_missing_runtimeclass() {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let app = build_test_router().await;

    let ns_body =
        r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"runtime-protobuf"}}"#;
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces")
        .header("content-type", "application/json")
        .body(Body::from(ns_body))
        .unwrap();
    let _ = app.clone().oneshot(req).await.unwrap();

    let pod_json = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "runtime-pod", "namespace": "runtime-protobuf"},
        "spec": {
            "runtimeClassName": "missing-runtime",
            "containers": [{"name": "c", "image": "busybox"}]
        }
    });
    let pod_pb = crate::protobuf::encode_protobuf(&pod_json).unwrap();
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/runtime-protobuf/pods")
        .header("content-type", "application/vnd.kubernetes.protobuf")
        .body(Body::from(pod_pb))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "protobuf Pod with missing RuntimeClass must be rejected with 403"
    );
}

#[tokio::test]
async fn test_protobuf_pod_create_respects_non_matching_node_selector() {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let node_name = state.config.node_name.clone();
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "Node",
        None,
        &node_name,
        json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {
                "name": node_name,
                "labels": {
                    "kubernetes.io/hostname": node_name
                }
            },
            "spec": {},
            "status": {
                "conditions": [{"type": "Ready", "status": "True"}],
                "allocatable": {"cpu": "8", "memory": "8Gi", "pods": "110"}
            }
        }),
    )
    .await
    .unwrap();

    let ns_body =
        r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"protobuf-selector"}}"#;
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces")
        .header("content-type", "application/json")
        .body(Body::from(ns_body))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(req).await.unwrap().status(),
        StatusCode::CREATED
    );

    let pod_json = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "restricted-pod", "namespace": "protobuf-selector"},
        "spec": {
            "nodeSelector": {"label": "nonempty"},
            "containers": [{
                "name": "restricted-pod",
                "image": "registry.k8s.io/pause:3.10.1"
            }]
        }
    });
    let pod_pb = crate::protobuf::encode_protobuf(&pod_json).unwrap();
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/protobuf-selector/pods")
        .header("content-type", "application/vnd.kubernetes.protobuf")
        .body(Body::from(pod_pb))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let created: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(
        created.pointer("/spec/nodeSelector/label"),
        Some(&json!("nonempty")),
        "protobuf create must retain nodeSelector"
    );
    assert!(
        created.pointer("/spec/nodeName").is_none(),
        "pod with non-matching nodeSelector must not be assigned"
    );
    let scheduled = created
        .pointer("/status/conditions")
        .and_then(|v| v.as_array())
        .and_then(|conditions| {
            conditions
                .iter()
                .find(|condition| condition["type"] == "PodScheduled")
        })
        .expect("PodScheduled condition present");
    assert_eq!(scheduled["status"], json!("False"));
    assert_eq!(scheduled["reason"], json!("SchedulingPending"));
}

#[tokio::test]
async fn test_runtimeclass_admission_rejects_dry_run_pod_with_missing_runtimeclass() {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let app = build_test_router().await;

    let ns_body = r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"runtime-dryrun"}}"#;
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces")
        .header("content-type", "application/json")
        .body(Body::from(ns_body))
        .unwrap();
    let _ = app.clone().oneshot(req).await.unwrap();

    let pod_json = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "runtime-pod", "namespace": "runtime-dryrun"},
        "spec": {
            "runtimeClassName": "missing-runtime",
            "containers": [{"name": "c", "image": "busybox"}]
        }
    });
    let pod_pb = crate::protobuf::encode_protobuf(&pod_json).unwrap();
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/runtime-dryrun/pods?dryRun=All")
        .header("content-type", "application/vnd.kubernetes.protobuf")
        .body(Body::from(pod_pb))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "dry-run Pod create must still run RuntimeClass admission"
    );
}

#[tokio::test]
async fn test_runtimeclass_admission_applies_overhead_to_pod_spec() {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let app = build_test_router().await;

    let ns_body =
        r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"runtime-overhead"}}"#;
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces")
        .header("content-type", "application/json")
        .body(Body::from(ns_body))
        .unwrap();
    let _ = app.clone().oneshot(req).await.unwrap();

    let rc_body = r#"{
        "apiVersion":"node.k8s.io/v1",
        "kind":"RuntimeClass",
        "metadata":{"name":"with-overhead"},
        "handler":"runc",
        "overhead":{"podFixed":{"cpu":"10m","memory":"16Mi"}}
    }"#;
    let req = Request::builder()
        .method("POST")
        .uri("/apis/node.k8s.io/v1/runtimeclasses")
        .header("content-type", "application/json")
        .body(Body::from(rc_body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let pod_body = r#"{
        "apiVersion":"v1",
        "kind":"Pod",
        "metadata":{"name":"runtime-overhead-pod","namespace":"runtime-overhead"},
        "spec":{
            "runtimeClassName":"with-overhead",
            "containers":[{"name":"c","image":"busybox"}]
        }
    }"#;
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/runtime-overhead/pods")
        .header("content-type", "application/json")
        .body(Body::from(pod_body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let created: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        created
            .pointer("/spec/overhead/cpu")
            .and_then(|v| v.as_str()),
        Some("10m"),
        "pod.spec.overhead.cpu must be copied from RuntimeClass.overhead.podFixed"
    );
    assert_eq!(
        created
            .pointer("/spec/overhead/memory")
            .and_then(|v| v.as_str()),
        Some("16Mi"),
        "pod.spec.overhead.memory must be copied from RuntimeClass.overhead.podFixed"
    );
}

#[tokio::test]
async fn test_events_v1_create_normalizes_event_time_microtime() {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let app = build_test_router().await;
    let ns = format!("event-time-test-{}", uuid::Uuid::new_v4().simple());

    let ns_body = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {"name": ns}
    });
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&ns_body).unwrap()))
        .unwrap();
    let _ = app.clone().oneshot(req).await.unwrap();

    let event = json!({
        "apiVersion": "events.k8s.io/v1",
        "kind": "Event",
        "metadata": {"name": "event-time", "namespace": ns},
        "eventTime": "2017-09-19T13:49:16+00:00",
        "reportingController": "e2e.test",
        "reportingInstance": "e2e.test",
        "action": "Testing",
        "reason": "Started",
        "regarding": {"apiVersion": "v1", "kind": "Pod", "name": "pod-1", "namespace": ns},
        "note": "test event",
        "type": "Normal"
    });
    let req = Request::builder()
        .method("POST")
        .uri(format!("/apis/events.k8s.io/v1/namespaces/{}/events", ns))
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&event).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let created: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        created["eventTime"], "2017-09-19T13:49:16.000000Z",
        "events.k8s.io/v1 eventTime must be canonical metav1.MicroTime"
    );
}

#[tokio::test]
async fn test_events_v1_create_is_listed_via_core_events_source_selector() {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let app = build_test_router().await;
    let ns = format!("events-core-bridge-{}", uuid::Uuid::new_v4().simple());

    let ns_body = json!({
        "apiVersion":"v1",
        "kind":"Namespace",
        "metadata":{"name": ns}
    });
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&ns_body).unwrap()))
        .unwrap();
    let _ = app.clone().oneshot(req).await.unwrap();

    let event = json!({
        "apiVersion": "events.k8s.io/v1",
        "kind": "Event",
        "metadata": {"name": "bridge-event", "namespace": ns},
        "eventTime": "2026-04-26T00:00:00Z",
        "reportingController": "test-controller",
        "reportingInstance": "node-1",
        "action": "Testing",
        "reason": "Started",
        "regarding": {"apiVersion": "v1", "kind": "Pod", "name": "pod-1", "namespace": ns},
        "note": "bridge event",
        "type": "Normal"
    });
    let req = Request::builder()
        .method("POST")
        .uri(format!("/apis/events.k8s.io/v1/namespaces/{}/events", ns))
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&event).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let req = Request::builder()
        .method("GET")
        .uri(format!(
            "/api/v1/namespaces/{}/events?fieldSelector=source%3Dtest-controller",
            ns
        ))
        .header("accept", "application/vnd.kubernetes.protobuf")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let listed = crate::protobuf::decode_protobuf(&bytes[4..]).unwrap();
    let items = listed["items"].as_array().expect("items must be array");
    assert_eq!(
        items.len(),
        1,
        "core events list must include mirrored event"
    );
    assert_eq!(items[0]["metadata"]["name"], "bridge-event");
    let component = items[0]
        .pointer("/source/component")
        .and_then(|v| v.as_str())
        .or_else(|| items[0].get("reportingController").and_then(|v| v.as_str()));
    assert_eq!(component, Some("test-controller"));
}

#[tokio::test]
async fn test_events_v1_protobuf_create_is_listed_via_core_events_source_selector() {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let app = build_test_router().await;
    let ns = format!("events-core-proto-{}", uuid::Uuid::new_v4().simple());

    let ns_body = json!({
        "apiVersion":"v1",
        "kind":"Namespace",
        "metadata":{"name": ns}
    });
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&ns_body).unwrap()))
        .unwrap();
    let _ = app.clone().oneshot(req).await.unwrap();

    let event = k8s_pb::api::events::v1::Event {
        metadata: Some(k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta {
            name: Some("event-test".to_string()),
            labels: std::collections::BTreeMap::from([(
                "testevent-constant".to_string(),
                "true".to_string(),
            )]),
            ..Default::default()
        }),
        event_time: Some(k8s_pb::apimachinery::pkg::apis::meta::v1::MicroTime {
            seconds: Some(1_505_828_956),
            nanos: Some(0),
        }),
        reporting_controller: Some("test-controller".to_string()),
        reporting_instance: Some("test-node".to_string()),
        action: Some("Do".to_string()),
        reason: Some("Test".to_string()),
        regarding: Some(k8s_pb::api::core::v1::ObjectReference {
            namespace: Some(ns.clone()),
            ..Default::default()
        }),
        note: Some("This is event-test".to_string()),
        r#type: Some("Normal".to_string()),
        ..Default::default()
    };
    let mut raw = Vec::new();
    prost::Message::encode(&event, &mut raw).unwrap();
    let envelope = crate::protobuf::Unknown {
        type_meta: Some(crate::protobuf::TypeMeta {
            api_version: String::new(),
            kind: "Event".to_string(),
        }),
        raw,
        content_encoding: String::new(),
        content_type: "application/vnd.kubernetes.protobuf".to_string(),
    };
    let mut encoded_event = b"k8s\0".to_vec();
    prost::Message::encode(&envelope, &mut encoded_event).unwrap();
    let req = Request::builder()
        .method("POST")
        .uri(format!("/apis/events.k8s.io/v1/namespaces/{}/events", ns))
        .header("content-type", "application/vnd.kubernetes.protobuf")
        .body(Body::from(encoded_event))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let req = Request::builder()
        .method("GET")
        .uri(format!(
            "/api/v1/namespaces/{}/events?fieldSelector=source%3Dtest-controller",
            ns
        ))
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let listed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let items = listed["items"].as_array().expect("items must be array");
    assert_eq!(
        items.len(),
        1,
        "protobuf-created event must match core source selector; list was {listed}"
    );
    assert_eq!(items[0]["metadata"]["name"], "event-test");
    assert_eq!(
        items[0]
            .pointer("/source/component")
            .and_then(|v| v.as_str()),
        Some("test-controller")
    );
}

#[tokio::test]
async fn test_core_event_update_protobuf_preserves_series() {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let app = build_test_router().await;

    let ns_body =
        r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"event-series-test"}}"#;
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces")
        .header("content-type", "application/json")
        .body(Body::from(ns_body))
        .unwrap();
    let _ = app.clone().oneshot(req).await.unwrap();

    let event = json!({
        "apiVersion": "v1",
        "kind": "Event",
        "metadata": {
            "name": "event-test",
            "namespace": "event-series-test",
            "labels": {"testevent-constant": "true"}
        },
        "message": "This is a test event",
        "reason": "Test",
        "type": "Normal",
        "count": 1,
        "involvedObject": {"namespace": "event-series-test"}
    });
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/event-series-test/events")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&event).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let mut updated = event.clone();
    updated["series"] = json!({
        "count": 100,
        "lastObservedTime": "2017-09-19T13:49:16Z"
    });
    let updated_pb = crate::protobuf::encode_protobuf(&updated).unwrap();
    let req = Request::builder()
        .method("PUT")
        .uri("/api/v1/namespaces/event-series-test/events/event-test")
        .header("content-type", "application/vnd.kubernetes.protobuf")
        .body(Body::from(updated_pb))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/namespaces/event-series-test/events/event-test")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let fetched: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(fetched["series"]["count"], 100);
    assert_eq!(
        fetched["series"]["lastObservedTime"],
        "2017-09-19T13:49:16.000000Z"
    );
}

#[tokio::test]
async fn test_ingress_status_update_with_protobuf_preserves_load_balancer() {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use prost::Message;
    use tower::ServiceExt;

    let app = build_test_router().await;

    let ns_body =
        r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"ingress-status-test"}}"#;
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces")
        .header("content-type", "application/json")
        .body(Body::from(ns_body))
        .unwrap();
    let _ = app.clone().oneshot(req).await.unwrap();

    let ingress = json!({
        "apiVersion": "networking.k8s.io/v1",
        "kind": "Ingress",
        "metadata": {"name": "ing", "namespace": "ingress-status-test"},
        "spec": {"ingressClassName": "example"},
        "status": {"loadBalancer": {}}
    });
    let req = Request::builder()
        .method("POST")
        .uri("/apis/networking.k8s.io/v1/namespaces/ingress-status-test/ingresses")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&ingress).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let status_update = json!({
        "apiVersion": "networking.k8s.io/v1",
        "kind": "Ingress",
        "metadata": {"name": "ing", "namespace": "ingress-status-test"},
        "status": {
            "loadBalancer": {
                "ingress": [{"ip": "192.0.2.2"}]
            }
        }
    });
    let status_update_pb = crate::protobuf::encode_protobuf(&status_update).unwrap();
    let req = Request::builder()
        .method("PUT")
        .uri("/apis/networking.k8s.io/v1/namespaces/ingress-status-test/ingresses/ing/status")
        .header("content-type", "application/vnd.kubernetes.protobuf")
        .body(Body::from(status_update_pb))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let updated: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        updated["status"]["loadBalancer"]["ingress"][0]["ip"],
        "192.0.2.2"
    );

    let req = Request::builder()
        .method("GET")
        .uri("/apis/networking.k8s.io/v1/namespaces/ingress-status-test/ingresses/ing")
        .header("accept", "application/vnd.kubernetes.protobuf")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let unknown = crate::protobuf::Unknown::decode(&body[4..]).unwrap();
    let decoded = k8s_pb::api::networking::v1::Ingress::decode(unknown.raw.as_slice()).unwrap();
    let status = decoded.status.expect("Ingress protobuf status must be set");
    let lb = status
        .load_balancer
        .expect("Ingress protobuf loadBalancer must be set");
    assert_eq!(lb.ingress[0].ip.as_deref(), Some("192.0.2.2"));
}

#[tokio::test]
async fn test_pod_log_websocket_upgrade_returns_switching_protocols() {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    state
        .db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "log-target",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "log-target",
                    "namespace": "default",
                    "uid": "pod-log-ws-uid"
                },
                "spec": {"containers": [{"name": "main", "image": "busybox"}]},
                "status": {"podIP": "10.0.0.10"}
            }),
        )
        .await
        .unwrap();
    let app = crate::api::build_router(state);

    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/namespaces/default/pods/log-target/log?container=main")
        .header("connection", "Upgrade")
        .header("upgrade", "websocket")
        .header("sec-websocket-version", "13")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-protocol", "binary.k8s.io")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::SWITCHING_PROTOCOLS);
}

#[tokio::test]
async fn test_pod_attach_route_exists_and_requires_streaming_upgrade() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    state
        .db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "attach-target",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": "attach-target", "namespace": "default"},
                "spec": {"containers": [{"name": "c", "image": "busybox"}]},
                "status": {
                    "phase": "Running",
                    "containerStatuses": [{
                        "name": "c",
                        "containerID": "containerd://attach-container"
                    }]
                }
            }),
        )
        .await
        .unwrap();
    let app = crate::api::build_router(state);

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/default/pods/attach-target/attach")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();

    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "pods/attach route must reject non-upgrade requests after streaming support is wired"
    );
}

#[tokio::test]
async fn test_pod_attach_validating_webhook_denies_before_stream_upgrade() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tower::ServiceExt;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 32768];
        let n = stream.read(&mut buf).await.unwrap();
        let req = String::from_utf8_lossy(&buf[..n]);
        let body_start = req.find("\r\n\r\n").map(|i| i + 4).unwrap_or(req.len());
        let review_req: serde_json::Value = serde_json::from_str(&req[body_start..]).unwrap();
        let uid = review_req["request"]["uid"]
            .as_str()
            .unwrap_or_default()
            .to_string();

        let deny = review_req["request"]["name"] == "attach-target"
            && review_req["request"]["resource"]["resource"] == "pods"
            && review_req["request"]["subResource"] == "attach"
            && review_req["request"]["object"]["stdin"] == true
            && review_req["request"]["object"]["container"] == "container1";

        let response_body = if deny {
            json!({
                "apiVersion": "admission.k8s.io/v1",
                "kind": "AdmissionReview",
                "response": {
                    "uid": uid,
                    "allowed": false,
                    "status": {"message": "attaching to pod 'attach-target' is not allowed"}
                }
            })
        } else {
            json!({
                "apiVersion": "admission.k8s.io/v1",
                "kind": "AdmissionReview",
                "response": {
                    "uid": uid,
                    "allowed": true
                }
            })
        };
        let payload = serde_json::to_string(&response_body).unwrap();
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            payload.len(),
            payload
        );
        stream.write_all(response.as_bytes()).await.unwrap();
    });

    let state = build_test_app_state().await;
    state
        .db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "attach-target",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": "attach-target", "namespace": "default"},
                "spec": {"containers": [{"name": "container1", "image": "busybox"}]},
                "status": {"phase": "Running"}
            }),
        )
        .await
        .unwrap();
    let app = crate::api::build_router(state);

    let ns_create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"default","labels":{"webhook-match":"yes"}}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ns_create.status(), StatusCode::CREATED);

    let create_vwc = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "apiVersion": "admissionregistration.k8s.io/v1",
                        "kind": "ValidatingWebhookConfiguration",
                        "metadata": {"name": "attach-deny"},
                        "webhooks": [{
                            "name": "deny-attaching-pod.k8s.io",
                            "rules": [{
                                "operations": ["CONNECT"],
                                "apiGroups": [""],
                                "apiVersions": ["v1"],
                                "resources": ["pods/attach"]
                            }],
                            "clientConfig": {"url": format!("http://127.0.0.1:{}/pods/attach", port)},
                            "sideEffects": "None",
                            "admissionReviewVersions": ["v1"],
                            "namespaceSelector": {"matchLabels": {"webhook-match": "yes"}}
                        }]
                    }))
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_vwc.status(), StatusCode::CREATED);

    let attach_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces/default/pods/attach-target/attach?stdin=true&container=container1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let status = attach_resp.status();
    let body = axum::body::to_bytes(attach_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "validating webhook deny must be returned for pods/attach before stream upgrade handling"
    );
    let message = value["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("attaching to pod 'attach-target' is not allowed"),
        "expected attach webhook denial message, got: {message}"
    );
}

#[tokio::test]
async fn test_pod_binding_validating_webhook_denies_before_bind() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tower::ServiceExt;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 32768];
        let n = stream.read(&mut buf).await.unwrap();
        let req = String::from_utf8_lossy(&buf[..n]);
        let body_start = req.find("\r\n\r\n").map(|i| i + 4).unwrap_or(req.len());
        let review_req: serde_json::Value = serde_json::from_str(&req[body_start..]).unwrap();
        let uid = review_req["request"]["uid"]
            .as_str()
            .unwrap_or_default()
            .to_string();

        let deny = review_req["request"]["name"] == "bind-denied"
            && review_req["request"]["resource"]["resource"] == "pods"
            && review_req["request"]["subResource"] == "binding"
            && review_req["request"]["object"]["target"]["name"] == "worker-a";

        let response_body = if deny {
            json!({
                "apiVersion": "admission.k8s.io/v1",
                "kind": "AdmissionReview",
                "response": {
                    "uid": uid,
                    "allowed": false,
                    "status": {"message": "binding pod 'bind-denied' is not allowed"}
                }
            })
        } else {
            json!({
                "apiVersion": "admission.k8s.io/v1",
                "kind": "AdmissionReview",
                "response": {
                    "uid": uid,
                    "allowed": true
                }
            })
        };
        let payload = serde_json::to_string(&response_body).unwrap();
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            payload.len(),
            payload
        );
        stream.write_all(response.as_bytes()).await.unwrap();
    });

    let state = build_test_app_state().await;
    state
        .db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "bind-denied",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": "bind-denied", "namespace": "default", "uid": "bind-denied-uid"},
                "spec": {"containers": [{"name": "c", "image": "busybox"}]},
                "status": {"phase": "Pending", "conditions": []}
            }),
        )
        .await
        .unwrap();
    let app = crate::api::build_router(state.clone());

    let ns_create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"default","labels":{"webhook-match":"yes"}}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ns_create.status(), StatusCode::CREATED);

    let create_vwc = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "apiVersion": "admissionregistration.k8s.io/v1",
                        "kind": "ValidatingWebhookConfiguration",
                        "metadata": {"name": "binding-deny"},
                        "webhooks": [{
                            "name": "deny-binding-pod.k8s.io",
                            "rules": [{
                                "operations": ["CREATE"],
                                "apiGroups": [""],
                                "apiVersions": ["v1"],
                                "resources": ["pods/binding"]
                            }],
                            "clientConfig": {"url": format!("http://127.0.0.1:{}/pods/binding", port)},
                            "sideEffects": "None",
                            "admissionReviewVersions": ["v1"],
                            "namespaceSelector": {"matchLabels": {"webhook-match": "yes"}}
                        }]
                    }))
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_vwc.status(), StatusCode::CREATED);

    let binding = json!({
        "apiVersion": "v1",
        "kind": "Binding",
        "metadata": {"name": "bind-denied", "namespace": "default"},
        "target": {"apiVersion": "v1", "kind": "Node", "name": "worker-a"}
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces/default/pods/bind-denied/binding")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&binding).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    let status = resp.status();
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "validating webhook deny must be returned for pods/binding before binding"
    );
    let message = value["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("binding pod 'bind-denied' is not allowed"),
        "expected binding webhook denial message, got: {message}"
    );
    let pod = state
        .db
        .get_resource("v1", "Pod", Some("default"), "bind-denied")
        .await
        .unwrap()
        .expect("denied pod must remain present");
    assert!(pod.data.pointer("/spec/nodeName").is_none());
}

#[tokio::test]
async fn test_pod_binding_subresource_binds_unassigned_pod() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    state
        .db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "bind-target",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": "bind-target", "namespace": "default", "uid": "bind-target-uid"},
                "spec": {"containers": [{"name": "c", "image": "busybox"}]},
                "status": {"phase": "Pending", "conditions": []}
            }),
        )
        .await
        .unwrap();
    let app = crate::api::build_router(state.clone());

    let binding = json!({
        "apiVersion": "v1",
        "kind": "Binding",
        "metadata": {"name": "bind-target", "namespace": "default"},
        "target": {"apiVersion": "v1", "kind": "Node", "name": "worker-a"}
    });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces/default/pods/bind-target/binding")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&binding).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["kind"], "Status");
    assert_eq!(value["status"], "Success");

    let pod = state
        .db
        .get_resource("v1", "Pod", Some("default"), "bind-target")
        .await
        .unwrap()
        .expect("bound pod must remain present");
    assert_eq!(pod.data["spec"]["nodeName"], "worker-a");
    let scheduled = pod
        .data
        .pointer("/status/conditions")
        .and_then(|v| v.as_array())
        .and_then(|conditions| {
            conditions.iter().find(|condition| {
                condition.get("type").and_then(|v| v.as_str()) == Some("PodScheduled")
            })
        })
        .expect("PodScheduled condition must be present");
    assert_eq!(scheduled["status"], "True");
}

#[tokio::test]
async fn test_mutating_webhook_pod_create_applies_init_container_defaults_after_mutation() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use base64::Engine;
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tower::ServiceExt;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 32768];
        let n = stream.read(&mut buf).await.unwrap();
        let req = String::from_utf8_lossy(&buf[..n]);
        let body_start = req.find("\r\n\r\n").map(|i| i + 4).unwrap_or(req.len());
        let review_req: serde_json::Value = serde_json::from_str(&req[body_start..]).unwrap();
        let uid = review_req["request"]["uid"]
            .as_str()
            .unwrap_or_default()
            .to_string();

        let patch = json!([
            {
                "op": "add",
                "path": "/spec/initContainers",
                "value": [{
                    "name": "injected-init",
                    "image": "busybox",
                    "command": ["sh", "-c", "echo hi"]
                }]
            }
        ]);
        let patch_b64 =
            base64::engine::general_purpose::STANDARD.encode(serde_json::to_vec(&patch).unwrap());
        let response_body = json!({
            "apiVersion": "admission.k8s.io/v1",
            "kind": "AdmissionReview",
            "response": {
                "uid": uid,
                "allowed": true,
                "patchType": "JSONPatch",
                "patch": patch_b64
            }
        });
        let payload = serde_json::to_string(&response_body).unwrap();
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            payload.len(),
            payload
        );
        stream.write_all(response.as_bytes()).await.unwrap();
    });

    let app = build_test_router().await;

    let ns_create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"default","labels":{"webhook-match":"yes"}}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ns_create.status(), StatusCode::CREATED);

    let create_mwc = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "apiVersion": "admissionregistration.k8s.io/v1",
                        "kind": "MutatingWebhookConfiguration",
                        "metadata": {"name": "pod-defaults-mutation"},
                        "webhooks": [{
                            "name": "mutate-pod-defaults.k8s.io",
                            "rules": [{
                                "operations": ["CREATE"],
                                "apiGroups": [""],
                                "apiVersions": ["v1"],
                                "resources": ["pods"]
                            }],
                            "clientConfig": {"url": format!("http://127.0.0.1:{}/pods", port)},
                            "sideEffects": "None",
                            "admissionReviewVersions": ["v1"],
                            "namespaceSelector": {"matchLabels": {"webhook-match": "yes"}}
                        }]
                    }))
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_mwc.status(), StatusCode::CREATED);

    let pod_create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces/default/pods")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{
                        "apiVersion":"v1",
                        "kind":"Pod",
                        "metadata":{"name":"pod-defaults-after-mutation"},
                        "spec":{"containers":[{"name":"app","image":"busybox"}]}
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(pod_create.status(), StatusCode::CREATED);

    let pod: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(pod_create.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        pod["spec"]["initContainers"][0]["terminationMessagePolicy"], "File",
        "mutated init containers must still receive API default terminationMessagePolicy=File",
    );
}

#[tokio::test]
async fn test_mutating_webhook_pod_create_applies_init_container_defaults_after_mutation_protobuf()
{
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use base64::Engine;
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tower::ServiceExt;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 32768];
        let n = stream.read(&mut buf).await.unwrap();
        let req = String::from_utf8_lossy(&buf[..n]);
        let body_start = req.find("\r\n\r\n").map(|i| i + 4).unwrap_or(req.len());
        let review_req: serde_json::Value = serde_json::from_str(&req[body_start..]).unwrap();
        let uid = review_req["request"]["uid"]
            .as_str()
            .unwrap_or_default()
            .to_string();

        let patch = json!([
            {
                "op": "add",
                "path": "/spec/initContainers",
                "value": [{
                    "name": "injected-init",
                    "image": "busybox",
                    "command": ["sh", "-c", "echo hi"]
                }]
            }
        ]);
        let patch_b64 =
            base64::engine::general_purpose::STANDARD.encode(serde_json::to_vec(&patch).unwrap());
        let response_body = json!({
            "apiVersion": "admission.k8s.io/v1",
            "kind": "AdmissionReview",
            "response": {
                "uid": uid,
                "allowed": true,
                "patchType": "JSONPatch",
                "patch": patch_b64
            }
        });
        let payload = serde_json::to_string(&response_body).unwrap();
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            payload.len(),
            payload
        );
        stream.write_all(response.as_bytes()).await.unwrap();
    });

    let app = build_test_router().await;

    let ns_create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"default","labels":{"webhook-match":"yes"}}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ns_create.status(), StatusCode::CREATED);

    let create_mwc = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "apiVersion": "admissionregistration.k8s.io/v1",
                        "kind": "MutatingWebhookConfiguration",
                        "metadata": {"name": "pod-defaults-mutation-protobuf"},
                        "webhooks": [{
                            "name": "mutate-pod-defaults.k8s.io",
                            "rules": [{
                                "operations": ["CREATE"],
                                "apiGroups": [""],
                                "apiVersions": ["v1"],
                                "resources": ["pods"]
                            }],
                            "clientConfig": {"url": format!("http://127.0.0.1:{}/pods", port)},
                            "sideEffects": "None",
                            "admissionReviewVersions": ["v1"],
                            "namespaceSelector": {"matchLabels": {"webhook-match": "yes"}}
                        }]
                    }))
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_mwc.status(), StatusCode::CREATED);

    let pod_json = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "pod-defaults-after-mutation-protobuf"},
        "spec": {"containers": [{"name": "app", "image": "busybox"}]}
    });
    let pod_pb = crate::protobuf::encode_protobuf(&pod_json).unwrap();

    let pod_create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces/default/pods")
                .header("content-type", "application/vnd.kubernetes.protobuf")
                .header("accept", "application/vnd.kubernetes.protobuf")
                .body(Body::from(pod_pb))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(pod_create.status(), StatusCode::CREATED);

    let content_type = pod_create
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_ascii_lowercase();
    let response_bytes = axum::body::to_bytes(pod_create.into_body(), usize::MAX)
        .await
        .unwrap();
    let pod: serde_json::Value = if content_type.contains("application/vnd.kubernetes.protobuf") {
        crate::protobuf::decode_protobuf(&response_bytes).unwrap()
    } else {
        serde_json::from_slice(&response_bytes).unwrap()
    };
    assert_eq!(
        pod["spec"]["initContainers"][0]["terminationMessagePolicy"], "File",
        "protobuf pod create response must include default terminationMessagePolicy=File on mutated init containers",
    );
}

#[tokio::test]
async fn test_validate_against_schema_rejects_extra_properties() {
    let schema = serde_json::json!({
        "type": "object",
        "properties": {
            "spec": {
                "type": "object",
                "properties": {
                    "replicas": {"type": "integer"},
                    "image": {"type": "string"}
                }
            }
        }
    });

    let body = serde_json::json!({
        "apiVersion": "stable.example.com/v1",
        "kind": "Widget",
        "metadata": {"name": "test"},
        "spec": {
            "replicas": 3,
            "image": "nginx",
            "unknownField": "should-fail"
        }
    });

    let result = crate::api::validate_against_schema(&body, &schema, "");
    assert!(
        result.is_err(),
        "Should reject extra field 'unknownField' in spec"
    );
    let err_msg = format!("{:?}", result.unwrap_err());
    assert!(
        err_msg.contains("unknownField"),
        "Error should mention the unknown field"
    );
}

#[tokio::test]
async fn test_validate_against_schema_allows_valid_properties() {
    let schema = serde_json::json!({
        "type": "object",
        "properties": {
            "spec": {
                "type": "object",
                "properties": {
                    "replicas": {"type": "integer"},
                    "image": {"type": "string"}
                }
            }
        }
    });

    let body = serde_json::json!({
        "apiVersion": "stable.example.com/v1",
        "kind": "Widget",
        "metadata": {"name": "test"},
        "spec": {
            "replicas": 3,
            "image": "nginx"
        }
    });

    let result = crate::api::validate_against_schema(&body, &schema, "");
    assert!(result.is_ok(), "Should accept body with only known fields");
}

#[tokio::test]
async fn test_validate_against_schema_rejects_invalid_enum_value() {
    let schema = serde_json::json!({
        "type": "object",
        "properties": {
            "spec": {
                "type": "object",
                "properties": {
                    "mode": {
                        "type": "string",
                        "enum": ["strict", "permissive"]
                    }
                }
            }
        }
    });

    let body = serde_json::json!({
        "apiVersion": "stable.example.com/v1",
        "kind": "Widget",
        "metadata": {"name": "test"},
        "spec": {"mode": "invalid"}
    });

    let result = crate::api::validate_against_schema(&body, &schema, "");
    let err = result.expect_err("Should reject values outside enum for schema-defined fields");
    match err {
        crate::api::AppError::UnprocessableEntity(msg) => {
            assert!(
                msg.contains("Unsupported value: \"invalid\""),
                "Enum rejection must expose kubectl-parity Unsupported value format, got: {}",
                msg
            );
            assert!(
                msg.contains("spec.mode"),
                "Enum rejection should include the full field path, got: {}",
                msg
            );
        }
        other => panic!("expected UnprocessableEntity, got: {:?}", other),
    }
}

#[tokio::test]
async fn test_validate_against_schema_rejects_missing_required_nested_field() {
    let schema = serde_json::json!({
        "type": "object",
        "properties": {
            "spec": {
                "type": "object",
                "properties": {
                    "bars": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "required": ["name"],
                            "properties": {
                                "name": {"type": "string"},
                                "age": {"type": "string"}
                            }
                        }
                    }
                }
            }
        }
    });

    let body = serde_json::json!({
        "apiVersion": "stable.example.com/v1",
        "kind": "Widget",
        "metadata": {"name": "test"},
        "spec": {
            "bars": [{"age": "10"}]
        }
    });

    let err = crate::api::validate_against_schema(&body, &schema, "")
        .expect_err("Should reject missing required nested fields");
    match err {
        crate::api::AppError::UnprocessableEntity(msg) => {
            assert!(
                msg.contains("spec.bars[0].name: Required value"),
                "Missing required field should report Kubernetes-style path/message, got: {}",
                msg
            );
        }
        other => panic!("expected UnprocessableEntity, got: {:?}", other),
    }
}

#[tokio::test]
async fn test_validate_against_schema_allows_standard_top_level_fields() {
    let schema = serde_json::json!({
        "type": "object",
        "properties": {
            "spec": {
                "type": "object",
                "properties": {
                    "replicas": {"type": "integer"}
                }
            }
        }
    });

    let body = serde_json::json!({
        "apiVersion": "stable.example.com/v1",
        "kind": "Widget",
        "metadata": {"name": "test"},
        "status": {"ready": true},
        "spec": {"replicas": 1}
    });

    let result = crate::api::validate_against_schema(&body, &schema, "");
    assert!(
        result.is_ok(),
        "Standard K8s top-level fields should always be allowed"
    );
}

#[tokio::test]
async fn test_check_cr_field_validation_strict_with_crd_schema() {
    let db = crate::datastore::test_support::in_memory().await;

    // Create a CRD with validation schema
    let crd = serde_json::json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "widgets.stable.example.com"},
        "spec": {
            "group": "stable.example.com",
            "scope": "Namespaced",
            "names": {
                "kind": "Widget",
                "plural": "widgets",
                "singular": "widget"
            },
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {
                    "openAPIV3Schema": {
                        "type": "object",
                        "properties": {
                            "spec": {
                                "type": "object",
                                "properties": {
                                    "color": {"type": "string"},
                                    "size": {"type": "integer"}
                                }
                            }
                        }
                    }
                }
            }]
        }
    });

    db.create_resource(
        "apiextensions.k8s.io/v1",
        "CustomResourceDefinition",
        None,
        "widgets.stable.example.com",
        crd,
    )
    .await
    .unwrap();

    // CR with extra field should be rejected
    let invalid_body = serde_json::json!({
        "apiVersion": "stable.example.com/v1",
        "kind": "Widget",
        "metadata": {"name": "test-widget"},
        "spec": {
            "color": "blue",
            "extraProperty": "should-fail"
        }
    });

    let result = crate::api::check_cr_field_validation_strict(
        &db,
        "stable.example.com",
        "v1",
        "Widget",
        &invalid_body,
    )
    .await;
    assert!(result.is_err(), "Should reject CR with extra properties");

    // CR with only valid fields should be accepted
    let valid_body = serde_json::json!({
        "apiVersion": "stable.example.com/v1",
        "kind": "Widget",
        "metadata": {"name": "test-widget"},
        "spec": {
            "color": "blue",
            "size": 5
        }
    });

    let result = crate::api::check_cr_field_validation_strict(
        &db,
        "stable.example.com",
        "v1",
        "Widget",
        &valid_body,
    )
    .await;
    assert!(result.is_ok(), "Should accept CR with only known fields");
}

// ========================
// Pod status PATCH tests
// ========================

#[tokio::test]
async fn test_patch_pod_status_merge_patch_updates_status_only() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    // Create namespace and pod
    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        serde_json::json!({"apiVersion":"v1","kind":"Namespace","metadata":{"name":"default"}}),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "test-pod",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "test-pod", "namespace": "default"},
            "spec": {"containers": [{"name": "c1", "image": "nginx"}]},
            "status": {"phase": "Pending"}
        }),
    )
    .await
    .unwrap();

    // PATCH pods/status with merge patch
    let patch = r#"{"status":{"phase":"Running","message":"patch applied"}}"#;
    let req = Request::builder()
        .method("PATCH")
        .uri("/api/v1/namespaces/default/pods/test-pod/status")
        .header("content-type", "application/merge-patch+json")
        .body(Body::from(patch))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "PATCH pods/status must return 200"
    );

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let updated: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(updated["status"]["phase"], "Running");
    assert_eq!(updated["status"]["message"], "patch applied");
    // Spec must be untouched
    assert_eq!(updated["spec"]["containers"][0]["name"], "c1");
}

#[tokio::test]
async fn test_patch_pod_status_not_found_returns_404() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let app = crate::api::build_router(state);

    let patch = r#"{"status":{"phase":"Running"}}"#;
    let req = Request::builder()
        .method("PATCH")
        .uri("/api/v1/namespaces/default/pods/nonexistent/status")
        .header("content-type", "application/merge-patch+json")
        .body(Body::from(patch))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "PATCH on nonexistent pod must return 404"
    );
}

#[tokio::test]
async fn test_patch_pod_status_with_stale_resource_version_returns_409() {
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
    let created = db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "status-occ-patch",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": "status-occ-patch", "namespace": "default", "uid": "uid-status-occ-patch"},
                "spec": {"containers": [{"name": "app", "image": "nginx"}]},
                "status": {"phase": "Pending"}
            }),
        )
        .await
        .unwrap();
    let stale_rv = created.resource_version;
    db.update_resource(
        "v1",
        "Pod",
        Some("default"),
        "status-occ-patch",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "status-occ-patch",
                "namespace": "default",
                "uid": "uid-status-occ-patch",
                "resourceVersion": stale_rv.to_string()
            },
            "spec": {"containers": [{"name": "app", "image": "nginx:1.25"}]},
            "status": {"phase": "Pending"}
        }),
        stale_rv,
    )
    .await
    .unwrap();

    let patch = serde_json::json!({
        "metadata": {"resourceVersion": stale_rv.to_string()},
        "status": {"phase": "Running"}
    });
    let req = Request::builder()
        .method("PATCH")
        .uri("/api/v1/namespaces/default/pods/status-occ-patch/status")
        .header("content-type", "application/merge-patch+json")
        .body(Body::from(patch.to_string()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::CONFLICT,
        "PATCH pods/status must reject stale metadata.resourceVersion"
    );
}

#[tokio::test]
async fn test_put_pod_status_with_stale_resource_version_returns_409() {
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
    let created = db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "status-occ-put",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": "status-occ-put", "namespace": "default", "uid": "uid-status-occ-put"},
                "spec": {"containers": [{"name": "app", "image": "nginx"}]},
                "status": {"phase": "Pending"}
            }),
        )
        .await
        .unwrap();
    let stale_rv = created.resource_version;
    db.update_resource(
        "v1",
        "Pod",
        Some("default"),
        "status-occ-put",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "status-occ-put",
                "namespace": "default",
                "uid": "uid-status-occ-put",
                "resourceVersion": stale_rv.to_string()
            },
            "spec": {"containers": [{"name": "app", "image": "nginx:1.25"}]},
            "status": {"phase": "Pending"}
        }),
        stale_rv,
    )
    .await
    .unwrap();

    let body = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "status-occ-put",
            "namespace": "default",
            "resourceVersion": stale_rv.to_string()
        },
        "status": {"phase": "Running"}
    });
    let req = Request::builder()
        .method("PUT")
        .uri("/api/v1/namespaces/default/pods/status-occ-put/status")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::CONFLICT,
        "PUT pods/status must reject stale metadata.resourceVersion"
    );
}

// ========================
// API chunking list tests
// ========================

#[tokio::test]
async fn test_list_configmaps_omits_continue_when_no_more_pages() {
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

    for i in 0..3u32 {
        let name = format!("cm-{:04}", i);
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            &name.clone(),
            serde_json::json!({"metadata":{"name":name,"namespace":"default"}}),
        )
        .await
        .unwrap();
    }

    // Request all 3 with limit=3 (exact fit) — no continue
    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/namespaces/default/configmaps?limit=3")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let list: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(list["items"].as_array().unwrap().len(), 3);
    // "continue" must NOT be present (not null, absent)
    assert!(
        list["metadata"].get("continue").is_none(),
        "metadata.continue must be absent when no more pages, got: {:?}",
        list["metadata"]
    );
    assert!(
        list["metadata"].get("remainingItemCount").is_none(),
        "metadata.remainingItemCount must be absent when no more pages"
    );
}

#[tokio::test]
async fn test_list_configmaps_returns_continue_and_remaining_when_more_pages_exist() {
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

    // Create 5 configmaps with alphabetical names
    for i in 0..5u32 {
        let name = format!("cm-{:04}", i);
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            &name.clone(),
            serde_json::json!({"metadata":{"name":name,"namespace":"default"}}),
        )
        .await
        .unwrap();
    }

    // Request page of 3 — should have continue + remainingItemCount
    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/namespaces/default/configmaps?limit=3")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let page1: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let items = page1["items"].as_array().unwrap();
    assert_eq!(items.len(), 3, "First page must have 3 items");

    // Verify alphabetical order
    assert_eq!(items[0]["metadata"]["name"], "cm-0000");
    assert_eq!(items[1]["metadata"]["name"], "cm-0001");
    assert_eq!(items[2]["metadata"]["name"], "cm-0002");

    // continue must be present (not null)
    let continue_token = page1["metadata"]["continue"]
        .as_str()
        .expect("metadata.continue must be a string when more pages exist");
    assert!(
        !continue_token.is_empty(),
        "continue token must not be empty"
    );

    // remainingItemCount must be present and >= 1
    let remaining = page1["metadata"]["remainingItemCount"]
        .as_i64()
        .expect("metadata.remainingItemCount must be present when more pages exist");
    assert!(remaining >= 1, "remainingItemCount must be >= 1");

    // Fetch second page using continue token
    let req2 = Request::builder()
        .method("GET")
        .uri(format!(
            "/api/v1/namespaces/default/configmaps?limit=3&continue={}",
            urlencoding::encode(continue_token)
        ))
        .body(Body::empty())
        .unwrap();
    let resp2 = app.oneshot(req2).await.unwrap();
    assert_eq!(resp2.status(), StatusCode::OK);

    let body2 = axum::body::to_bytes(resp2.into_body(), usize::MAX)
        .await
        .unwrap();
    let page2: serde_json::Value = serde_json::from_slice(&body2).unwrap();
    let items2 = page2["items"].as_array().unwrap();
    assert_eq!(items2.len(), 2, "Second page must have remaining 2 items");
    assert_eq!(items2[0]["metadata"]["name"], "cm-0003");
    assert_eq!(items2[1]["metadata"]["name"], "cm-0004");

    // No more continue on last page
    assert!(
        page2["metadata"].get("continue").is_none(),
        "Last page must not have continue token"
    );
}

#[tokio::test]
async fn test_immutable_configmap_metadata_update_succeeds() {
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
        .body(Body::from(r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"imm-meta","namespace":"default"},"immutable":true,"data":{"key":"val"}}"#))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // PUT with metadata change only (add label), data unchanged — must succeed
    let req = Request::builder()
        .method("PUT")
        .uri("/api/v1/namespaces/default/configmaps/imm-meta")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"imm-meta","namespace":"default","labels":{"env":"test"}},"immutable":true,"data":{"key":"val"}}"#))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "Immutable ConfigMap metadata-only update must succeed"
    );
}

#[tokio::test]
async fn test_immutable_configmap_flip_immutable_to_false_rejected() {
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
        .body(Body::from(r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"imm-flip","namespace":"default"},"immutable":true,"data":{"key":"val"}}"#))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // PUT attempting to flip immutable to false — must be rejected
    let req = Request::builder()
        .method("PUT")
        .uri("/api/v1/namespaces/default/configmaps/imm-flip")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"imm-flip","namespace":"default"},"immutable":false,"data":{"key":"val"}}"#))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::UNPROCESSABLE_ENTITY,
        "Flipping immutable to false must be rejected"
    );
}

#[test]
fn test_validate_metadata_fields_rejects_unknown() {
    let meta = serde_json::json!({
        "name": "test",
        "namespace": "default",
        "unknownField": "bad"
    });
    let meta_map = meta.as_object().unwrap();
    let result = crate::api::validate_metadata_fields(meta_map);
    assert!(result.is_err());
    match result.unwrap_err() {
        crate::api::AppError::UnprocessableEntity(msg) => {
            assert!(
                msg.contains("metadata.unknownField"),
                "Error must mention the unknown field: {}",
                msg
            );
        }
        other => panic!("Expected UnprocessableEntity, got {:?}", other),
    }
}

#[test]
fn test_validate_metadata_fields_accepts_known() {
    let meta = serde_json::json!({
        "name": "test",
        "namespace": "default",
        "labels": {"app": "test"},
        "annotations": {"note": "hi"},
        "finalizers": ["cleanup"],
        "ownerReferences": []
    });
    let meta_map = meta.as_object().unwrap();
    let result = crate::api::validate_metadata_fields(meta_map);
    assert!(result.is_ok());
}

#[test]
fn test_apply_schema_defaults_fills_missing_fields() {
    let schema = serde_json::json!({
        "type": "object",
        "properties": {
            "spec": {
                "type": "object",
                "properties": {
                    "replicas": {"type": "integer", "default": 1},
                    "paused": {"type": "boolean", "default": false},
                    "name": {"type": "string"}
                }
            }
        }
    });
    let mut body = serde_json::json!({
        "apiVersion": "example.com/v1",
        "kind": "Foo",
        "metadata": {"name": "test"},
        "spec": {"name": "bar"}
    });
    crate::api::apply_schema_defaults_pub(&mut body, &schema);
    assert_eq!(
        body["spec"]["replicas"], 1,
        "Missing field should get default"
    );
    assert_eq!(
        body["spec"]["paused"], false,
        "Missing bool field should get default"
    );
    assert_eq!(
        body["spec"]["name"], "bar",
        "Existing field should not be overwritten"
    );
}

#[test]
fn test_apply_schema_defaults_skips_existing_fields() {
    let schema = serde_json::json!({
        "type": "object",
        "properties": {
            "spec": {
                "type": "object",
                "properties": {
                    "replicas": {"type": "integer", "default": 1}
                }
            }
        }
    });
    let mut body = serde_json::json!({
        "spec": {"replicas": 5}
    });
    crate::api::apply_schema_defaults_pub(&mut body, &schema);
    assert_eq!(
        body["spec"]["replicas"], 5,
        "Existing field must not be overwritten by default"
    );
}

#[test]
fn test_apply_schema_defaults_no_schema_properties_is_noop() {
    let schema = serde_json::json!({"type": "object"});
    let mut body = serde_json::json!({"spec": {"foo": "bar"}});
    let original = body.clone();
    crate::api::apply_schema_defaults_pub(&mut body, &schema);
    assert_eq!(body, original, "No properties in schema = no changes");
}

#[tokio::test]
async fn test_csr_status_subresource_get_put() {
    use serde_json::json;

    let db = crate::datastore::test_support::in_memory().await;

    let csr = json!({
        "apiVersion": "certificates.k8s.io/v1",
        "kind": "CertificateSigningRequest",
        "metadata": {"name": "test-csr"},
        "spec": {
            "request": "LS0t...",
            "signerName": "kubernetes.io/kube-apiserver-client",
            "usages": ["client auth"]
        }
    });

    let created = db
        .create_resource(
            "certificates.k8s.io/v1",
            "CertificateSigningRequest",
            None,
            "test-csr",
            csr,
        )
        .await
        .unwrap();

    // GET status returns the full CSR
    let got = db
        .get_resource(
            "certificates.k8s.io/v1",
            "CertificateSigningRequest",
            None,
            "test-csr",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        got.data["spec"]["signerName"],
        "kubernetes.io/kube-apiserver-client"
    );

    // PUT status updates only the status field
    let mut updated_csr: serde_json::Value = (*got.data).clone();
    updated_csr["status"] = json!({
        "conditions": [{
            "type": "Approved",
            "status": "True",
            "reason": "AutoApproved",
            "message": "Auto approved by test"
        }]
    });

    let updated = db
        .update_resource(
            "certificates.k8s.io/v1",
            "CertificateSigningRequest",
            None,
            "test-csr",
            updated_csr,
            created.resource_version,
        )
        .await
        .unwrap();

    assert_eq!(updated.data["status"]["conditions"][0]["type"], "Approved");
    assert_eq!(
        updated.data["spec"]["signerName"],
        "kubernetes.io/kube-apiserver-client"
    );
}

#[tokio::test]
async fn test_csr_approval_subresource_get_put() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = build_test_router().await;

    let csr = json!({
        "apiVersion": "certificates.k8s.io/v1",
        "kind": "CertificateSigningRequest",
        "metadata": {"name": "approval-csr"},
        "spec": {
            "request": "LS0t...",
            "signerName": "kubernetes.io/kube-apiserver-client",
            "usages": ["client auth"]
        }
    });

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/certificates.k8s.io/v1/certificatesigningrequests")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&csr).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    let get_approval = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(
                    "/apis/certificates.k8s.io/v1/certificatesigningrequests/approval-csr/approval",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        get_approval.status(),
        StatusCode::OK,
        "GET /approval must be supported for CSR API compatibility"
    );
    let get_body = axum::body::to_bytes(get_approval.into_body(), usize::MAX)
        .await
        .unwrap();
    let mut approval_doc: serde_json::Value = serde_json::from_slice(&get_body).unwrap();
    assert_eq!(approval_doc["metadata"]["name"], "approval-csr");
    assert_eq!(
        approval_doc["spec"]["signerName"],
        "kubernetes.io/kube-apiserver-client"
    );

    approval_doc["status"] = json!({
        "conditions": [{
            "type": "Approved",
            "status": "True",
            "reason": "AutoApproved",
            "message": "approved in test"
        }]
    });
    let put_approval = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(
                    "/apis/certificates.k8s.io/v1/certificatesigningrequests/approval-csr/approval",
                )
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&approval_doc).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put_approval.status(), StatusCode::OK);

    let patch_approval = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(
                    "/apis/certificates.k8s.io/v1/certificatesigningrequests/approval-csr/approval",
                )
                .header("content-type", "application/merge-patch+json")
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "metadata": {
                            "annotations": {
                                "patchedapproval": "true"
                            }
                        },
                        "status": {
                            "conditions": [{
                                "type": "Approved",
                                "status": "True",
                                "reason": "PatchedApproval",
                                "message": "patched in test"
                            }]
                        }
                    }))
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        patch_approval.status(),
        StatusCode::OK,
        "PATCH /approval must be supported for CSR API compatibility"
    );

    let get_resource = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/certificates.k8s.io/v1/certificatesigningrequests/approval-csr")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get_resource.status(), StatusCode::OK);
    let body = axum::body::to_bytes(get_resource.into_body(), usize::MAX)
        .await
        .unwrap();
    let updated: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(updated["status"]["conditions"][0]["type"], "Approved");
    assert_eq!(
        updated["status"]["conditions"][0]["reason"],
        "PatchedApproval"
    );
    assert_eq!(
        updated["metadata"]["annotations"]["patchedapproval"], "true",
        "PATCH /approval must merge metadata annotations like Kubernetes"
    );
}

#[tokio::test]
async fn test_csr_discovery_includes_approval_get_update_patch() {
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    let app = build_test_router().await;
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/certificates.k8s.io/v1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let resources: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let approval = resources["resources"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["name"] == "certificatesigningrequests/approval")
        .unwrap();
    let verbs = approval["verbs"].as_array().unwrap();
    let verb_strings: Vec<&str> = verbs.iter().filter_map(|v| v.as_str()).collect();
    assert!(
        verb_strings.contains(&"get"),
        "csr approval discovery verbs must include get, got: {:?}",
        verb_strings
    );
    assert!(
        verb_strings.contains(&"update"),
        "csr approval discovery verbs must include update, got: {:?}",
        verb_strings
    );
    assert!(
        verb_strings.contains(&"patch"),
        "csr approval discovery verbs must include patch, got: {:?}",
        verb_strings
    );
}

#[tokio::test]
async fn test_ephemeral_containers_update_adds_containers_to_pod() {
    use serde_json::json;

    let db = crate::datastore::test_support::in_memory().await;

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "test-pod", "namespace": "default"},
        "spec": {
            "containers": [{"name": "main", "image": "nginx"}]
        },
        "status": {"phase": "Running"}
    });

    let created = db
        .create_resource("v1", "Pod", Some("default"), "test-pod", pod)
        .await
        .unwrap();

    // Simulate PUT ephemeralcontainers with a pod body that has spec.ephemeralContainers
    let mut updated_pod: serde_json::Value = (*created.data).clone();
    updated_pod["spec"]["ephemeralContainers"] = json!([
        {"name": "debugger", "image": "busybox", "command": ["sh"]}
    ]);

    let updated = db
        .update_resource(
            "v1",
            "Pod",
            Some("default"),
            "test-pod",
            updated_pod,
            created.resource_version,
        )
        .await
        .unwrap();

    let ephemeral = updated.data.pointer("/spec/ephemeralContainers").unwrap();
    assert_eq!(ephemeral.as_array().unwrap().len(), 1);
    assert_eq!(ephemeral[0]["name"], "debugger");
    assert_eq!(ephemeral[0]["image"], "busybox");
}

#[tokio::test]
async fn test_ephemeral_containers_subresource_does_not_fabricate_runtime_status() {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let app = build_test_router().await;

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"ephemeral-test"}}"#,
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/ephemeral-test/pods")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{
              "apiVersion":"v1",
              "kind":"Pod",
              "metadata":{"name":"ephemeral-target","namespace":"ephemeral-test"},
              "spec":{"containers":[{"name":"main","image":"busybox","command":["sleep","10000"]}]}
            }"#,
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let req = Request::builder()
        .method("PUT")
        .uri("/api/v1/namespaces/ephemeral-test/pods/ephemeral-target/ephemeralcontainers")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{
              "apiVersion":"v1",
              "kind":"Pod",
              "metadata":{"name":"ephemeral-target","namespace":"ephemeral-test"},
              "spec":{"ephemeralContainers":[{"name":"debugger","image":"busybox","command":["sh"]}]}
            }"#,
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

    let synthetic_status = body.pointer("/status/ephemeralContainerStatuses/0/containerID");
    assert!(
        synthetic_status.is_none(),
        "pods/ephemeralcontainers handler must not synthesize runtime container IDs"
    );
    assert_eq!(
        body.pointer("/metadata/generation")
            .and_then(|v| v.as_i64()),
        Some(2),
        "updating pod ephemeralcontainers must bump metadata.generation"
    );
}

#[tokio::test]
async fn test_ephemeral_containers_subresource_put_appends_second_container() {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let app = build_test_router().await;

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"ephemeral-append-test"}}"#,
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/ephemeral-append-test/pods")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{
              "apiVersion":"v1",
              "kind":"Pod",
              "metadata":{"name":"ephemeral-target","namespace":"ephemeral-append-test"},
              "spec":{"containers":[{"name":"main","image":"busybox","command":["sleep","10000"]}]}
            }"#,
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let req = Request::builder()
        .method("PUT")
        .uri("/api/v1/namespaces/ephemeral-append-test/pods/ephemeral-target/ephemeralcontainers")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{
              "apiVersion":"v1",
              "kind":"Pod",
              "metadata":{"name":"ephemeral-target","namespace":"ephemeral-append-test"},
              "spec":{"ephemeralContainers":[{"name":"debugger-1","image":"busybox","command":["sh"]}]}
            }"#,
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Second update uses the top-level ephemeralContainers shape used by some clients.
    let req = Request::builder()
        .method("PUT")
        .uri("/api/v1/namespaces/ephemeral-append-test/pods/ephemeral-target/ephemeralcontainers")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{
              "apiVersion":"v1",
              "kind":"Pod",
              "metadata":{"name":"ephemeral-target","namespace":"ephemeral-append-test"},
              "ephemeralContainers":[{"name":"debugger-2","image":"busybox","command":["sh"]}]
            }"#,
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

    let ephemeral = body
        .pointer("/spec/ephemeralContainers")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        ephemeral.len(),
        2,
        "second subresource update must append, not replace, existing ephemeral containers"
    );
    assert_eq!(ephemeral[0]["name"], "debugger-1");
    assert_eq!(ephemeral[1]["name"], "debugger-2");
}

#[tokio::test]
async fn test_ephemeral_containers_patch_visible_on_pod_get_for_followup_put() {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let app = build_test_router().await;

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"ephemeral-followup-test"}}"#,
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/ephemeral-followup-test/pods")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{
              "apiVersion":"v1",
              "kind":"Pod",
              "metadata":{"name":"ephemeral-target","namespace":"ephemeral-followup-test"},
              "spec":{"containers":[{"name":"main","image":"busybox","command":["sleep","10000"]}]}
            }"#,
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let req = Request::builder()
        .method("PATCH")
        .uri("/api/v1/namespaces/ephemeral-followup-test/pods/ephemeral-target/ephemeralcontainers")
        .header("content-type", "application/strategic-merge-patch+json")
        .body(Body::from(
            r#"{
              "spec":{"ephemeralContainers":[{"name":"debugger-1","image":"busybox","command":["sh"]}]}
            }"#,
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/namespaces/ephemeral-followup-test/pods/ephemeral-target")
        .header("accept", "application/vnd.kubernetes.protobuf")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let content_type = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        content_type.contains("application/vnd.kubernetes.protobuf"),
        "normal pod GET should exercise protobuf response path, got {content_type}"
    );
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let mut pod = crate::protobuf::decode_protobuf(&bytes).unwrap();

    let ephemeral = pod
        .pointer("/spec/ephemeralContainers")
        .and_then(|v| v.as_array())
        .expect("normal pod GET must include patched spec.ephemeralContainers");
    assert_eq!(ephemeral.len(), 1);
    assert_eq!(ephemeral[0]["name"], "debugger-1");

    pod["spec"]["ephemeralContainers"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!({"name":"debugger-2","image":"busybox","command":["sh"]}));

    let req = Request::builder()
        .method("PUT")
        .uri("/api/v1/namespaces/ephemeral-followup-test/pods/ephemeral-target/ephemeralcontainers")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&pod).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

    let ephemeral = body
        .pointer("/spec/ephemeralContainers")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        ephemeral.len(),
        2,
        "PUT body built from normal pod GET must append the second ephemeral container"
    );
    assert_eq!(ephemeral[0]["name"], "debugger-1");
    assert_eq!(ephemeral[1]["name"], "debugger-2");
}

/// Covers the resourcequota-stub chainsaw test inline. The cluster-wide and
/// namespaced ResourceQuota list endpoints must return a `ResourceQuotaList`
/// (with `items: []`) rather than 404 when no quotas exist. Several K8s tools
/// (kubectl, sonobuoy) call these even when quotas are unconfigured.
#[tokio::test]
async fn test_resourcequota_list_empty_returns_valid_list_kind() {
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = build_test_router().await;

    let ns_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"rq-empty-list"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ns_resp.status(), StatusCode::CREATED);

    for uri in [
        "/api/v1/resourcequotas",
        "/api/v1/namespaces/rq-empty-list/resourcequotas",
    ] {
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(uri)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "GET {uri} must succeed");
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(resp.into_body(), usize::MAX).await.unwrap()).unwrap();
        assert_eq!(
            body["kind"], "ResourceQuotaList",
            "kind must be set on {uri}"
        );
        assert_eq!(
            body["items"].as_array().map(|a| a.len()),
            Some(0),
            "items must be an empty array when no quotas configured (uri={uri})"
        );
    }
}

/// Covers the pod-events chainsaw test inline. `kubectl describe pod`
/// translates to a list events request with `fieldSelector=involvedObject.name=<pod>`.
/// Verify that the field selector filters server-side so siblings in the same
/// Deployment do not bleed into a pod's described events.
#[tokio::test]
async fn test_event_list_field_selector_involved_object_name_filters_per_pod() {
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = build_test_router().await;

    let ns = "events-by-involved-object";
    let ns_body = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {"name": ns}
    });
    let ns_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&ns_body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ns_resp.status(), StatusCode::CREATED);

    for (event_name, pod_name) in [
        ("evt-pod-a-1", "pod-a"),
        ("evt-pod-a-2", "pod-a"),
        ("evt-pod-b-1", "pod-b"),
    ] {
        let event = json!({
            "apiVersion": "v1",
            "kind": "Event",
            "metadata": {"name": event_name, "namespace": ns},
            "message": "test",
            "reason": "Started",
            "type": "Normal",
            "count": 1,
            "involvedObject": {"kind": "Pod", "name": pod_name, "namespace": ns}
        });
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/v1/namespaces/{ns}/events"))
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&event).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::CREATED,
            "creating event {event_name} must succeed"
        );
    }

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "/api/v1/namespaces/{ns}/events?fieldSelector=involvedObject.name%3Dpod-a"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let listed: serde_json::Value =
        serde_json::from_slice(&to_bytes(resp.into_body(), usize::MAX).await.unwrap()).unwrap();
    let items = listed["items"].as_array().expect("items must be array");

    assert_eq!(
        items.len(),
        2,
        "pod-a must have exactly 2 events (got {})",
        items.len()
    );
    for item in items {
        assert_eq!(
            item["involvedObject"]["name"], "pod-a",
            "all returned events must reference pod-a; got {item}"
        );
    }
    let names: Vec<&str> = items
        .iter()
        .map(|n| n["metadata"]["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"evt-pod-a-1"));
    assert!(names.contains(&"evt-pod-a-2"));
    assert!(
        !names.contains(&"evt-pod-b-1"),
        "pod-b event must be filtered out"
    );
}

/// Covers the pod-invalid-subpath chainsaw test inline at the HTTP layer.
/// `validate_volume_subpaths` is exercised by unit tests in
/// `kubelet/volumes/tests_refresh_subpath.rs`, but the API handler path that
/// translates a validation failure into a 422 response is not otherwise covered
/// in cargo. K8s rejects pods with absolute subPath or `..` components.
#[tokio::test]
async fn test_pod_create_rejects_invalid_subpath_with_422() {
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = build_test_router().await;

    let ns_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"subpath-rej"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ns_resp.status(), StatusCode::CREATED);

    for (label, sub_path) in [("absolute", "/etc/passwd"), ("dotdot", "../secrets")] {
        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": format!("bad-{label}-subpath"), "namespace": "subpath-rej"},
            "spec": {
                "volumes": [{"name": "data", "emptyDir": {}}],
                "containers": [{
                    "name": "app",
                    "image": "registry.example.invalid/klights/test-image:1",
                    "volumeMounts": [{
                        "name": "data",
                        "mountPath": "/data",
                        "subPath": sub_path
                    }]
                }]
            }
        });
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/namespaces/subpath-rej/pods")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&pod).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::UNPROCESSABLE_ENTITY,
            "{label} subPath must be rejected with 422"
        );
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(resp.into_body(), usize::MAX).await.unwrap()).unwrap();
        assert_eq!(body["kind"], "Status");
        assert_eq!(body["status"], "Failure");
        let message = body["message"].as_str().unwrap_or_default();
        assert!(
            message.contains("subPath"),
            "422 Status message must mention subPath; got: {message}"
        );
    }

    let good_pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "good-subpath", "namespace": "subpath-rej"},
        "spec": {
            "volumes": [{"name": "data", "emptyDir": {}}],
            "containers": [{
                "name": "app",
                "image": "registry.example.invalid/klights/test-image:1",
                "volumeMounts": [{
                    "name": "data",
                    "mountPath": "/data",
                    "subPath": "config/app.conf"
                }]
            }]
        }
    });
    let good_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces/subpath-rej/pods")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&good_pod).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        good_resp.status(),
        StatusCode::CREATED,
        "valid relative subPath must succeed"
    );
}

/// Phase 1 deletion-gap closer: chainsaw configmap-secret asserted that
/// `kubectl delete configmap` then `kubectl get` returns "not found". The
/// generic delete handler is exercised by 10+ other DELETE tests, but no
/// cargo test covered the ConfigMap-specific DELETE → GET 404 round-trip.
#[tokio::test]
async fn test_configmap_delete_returns_ok_then_get_returns_404() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = build_test_router().await;

    let ns_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"cm-delete-test"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ns_resp.status(), StatusCode::CREATED);

    let create_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces/cm-delete-test/configmaps")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"to-delete"},"data":{"k":"v"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);

    let delete_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/v1/namespaces/cm-delete-test/configmaps/to-delete")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let s = delete_resp.status();
    assert!(
        s == StatusCode::OK || s == StatusCode::ACCEPTED,
        "DELETE configmap must return 200 or 202 (got {s})"
    );

    let get_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/namespaces/cm-delete-test/configmaps/to-delete")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        get_resp.status(),
        StatusCode::NOT_FOUND,
        "GET deleted configmap must return 404"
    );
}

/// Phase 1 deletion-gap closer: same as above but for Secret. The chainsaw
/// configmap-secret test asserted Secret DELETE round-trip; cargo had Secret
/// CRUD/PATCH coverage but no DELETE → GET 404 specifically.
#[tokio::test]
async fn test_secret_delete_returns_ok_then_get_returns_404() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = build_test_router().await;

    let ns_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"sec-delete-test"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ns_resp.status(), StatusCode::CREATED);

    let create_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces/sec-delete-test/secrets")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"apiVersion":"v1","kind":"Secret","metadata":{"name":"to-delete"},"type":"Opaque","stringData":{"u":"admin"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);

    let delete_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/v1/namespaces/sec-delete-test/secrets/to-delete")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let s = delete_resp.status();
    assert!(
        s == StatusCode::OK || s == StatusCode::ACCEPTED,
        "DELETE secret must return 200 or 202 (got {s})"
    );

    let get_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/namespaces/sec-delete-test/secrets/to-delete")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        get_resp.status(),
        StatusCode::NOT_FOUND,
        "GET deleted secret must return 404"
    );
}

/// HTTP-level safety net for the sa-volume-automount chainsaw deletion. The
/// injection function is unit-tested in db/tests/ipam_and_network_tests.rs but
/// nothing previously asserted that POST /api/v1/.../pods triggers the
/// injection on the create code path. This closes the gap.
#[tokio::test]
async fn test_pod_create_via_http_auto_injects_kube_api_access_projected_volume() {
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = build_test_router().await;

    let ns_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"sa-inject-test"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ns_resp.status(), StatusCode::CREATED);

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "sa-injected", "namespace": "sa-inject-test"},
        "spec": {
            "containers": [{"name": "app", "image": "registry.example.invalid/klights/test-image:1"}]
        }
    });
    let create_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces/sa-inject-test/pods")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&pod).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);

    let get_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/namespaces/sa-inject-test/pods/sa-injected")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get_resp.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(get_resp.into_body(), usize::MAX).await.unwrap()).unwrap();

    let volumes = body["spec"]["volumes"]
        .as_array()
        .expect("pod must have volumes after SA injection");
    let sa_volume = volumes
        .iter()
        .find(|v| {
            v["name"]
                .as_str()
                .map(|n| n.starts_with("kube-api-access-"))
                .unwrap_or(false)
        })
        .expect("kube-api-access-* projected volume must be auto-injected");
    let sources = sa_volume["projected"]["sources"]
        .as_array()
        .expect("projected sources must be an array");
    assert_eq!(
        sources.len(),
        3,
        "projected volume must have 3 sources (serviceAccountToken, configMap, downwardAPI), got {}",
        sources.len()
    );
    let kinds: Vec<&str> = sources
        .iter()
        .filter_map(|s| {
            if s.get("serviceAccountToken").is_some() {
                Some("serviceAccountToken")
            } else if s.get("configMap").is_some() {
                Some("configMap")
            } else if s.get("downwardAPI").is_some() {
                Some("downwardAPI")
            } else {
                None
            }
        })
        .collect();
    assert!(
        kinds.contains(&"serviceAccountToken"),
        "missing serviceAccountToken source: {kinds:?}"
    );
    assert!(
        kinds.contains(&"configMap"),
        "missing configMap source: {kinds:?}"
    );
    assert!(
        kinds.contains(&"downwardAPI"),
        "missing downwardAPI source: {kinds:?}"
    );

    let mounts = body["spec"]["containers"][0]["volumeMounts"]
        .as_array()
        .expect("container must have volumeMounts");
    let sa_mount = mounts
        .iter()
        .find(|m| m["mountPath"].as_str() == Some("/var/run/secrets/kubernetes.io/serviceaccount"))
        .expect(
            "container must have SA volumeMount at /var/run/secrets/kubernetes.io/serviceaccount",
        );
    assert!(
        sa_mount["name"]
            .as_str()
            .map(|n| n.starts_with("kube-api-access-"))
            .unwrap_or(false),
        "SA volumeMount must reference the kube-api-access-* volume"
    );
}

/// HTTP-level safety net continued: automountServiceAccountToken=false must
/// suppress the injection so pods can opt out. Pairs with the test above.
#[tokio::test]
async fn test_pod_create_with_automount_false_skips_kube_api_access_injection() {
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = build_test_router().await;

    let ns_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"sa-skip-test"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ns_resp.status(), StatusCode::CREATED);

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "sa-skipped", "namespace": "sa-skip-test"},
        "spec": {
            "automountServiceAccountToken": false,
            "containers": [{"name": "app", "image": "registry.example.invalid/klights/test-image:1"}]
        }
    });
    let create_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces/sa-skip-test/pods")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&pod).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);

    let get_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/namespaces/sa-skip-test/pods/sa-skipped")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get_resp.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(get_resp.into_body(), usize::MAX).await.unwrap()).unwrap();

    let volume_names: Vec<&str> = body["spec"]["volumes"]
        .as_array()
        .map(|arr| arr.iter().filter_map(|v| v["name"].as_str()).collect())
        .unwrap_or_default();
    assert!(
        !volume_names
            .iter()
            .any(|n| n.starts_with("kube-api-access-")),
        "automountServiceAccountToken=false must skip kube-api-access-* injection; got volumes: {volume_names:?}"
    );

    let mount_paths: Vec<&str> = body["spec"]["containers"][0]["volumeMounts"]
        .as_array()
        .map(|arr| arr.iter().filter_map(|m| m["mountPath"].as_str()).collect())
        .unwrap_or_default();
    assert!(
        !mount_paths.contains(&"/var/run/secrets/kubernetes.io/serviceaccount"),
        "automountServiceAccountToken=false must skip SA volumeMount; got mounts: {mount_paths:?}"
    );
}
