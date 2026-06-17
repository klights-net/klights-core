use super::*;

#[tokio::test]
async fn test_cronjob_status_subresource_patch() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = build_test_router().await;

    // Create a CronJob
    let cronjob = json!({
        "apiVersion": "batch/v1",
        "kind": "CronJob",
        "metadata": {"name": "test-cronjob", "namespace": "default"},
        "spec": {
            "schedule": "*/1 * * * *",
            "jobTemplate": {
                "spec": {
                    "template": {
                        "spec": {
                            "containers": [{"name": "main", "image": "nginx"}],
                            "restartPolicy": "OnFailure"
                        }
                    }
                }
            }
        }
    });

    let req = Request::builder()
        .method("POST")
        .uri("/apis/batch/v1/namespaces/default/cronjobs")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&cronjob).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // PATCH status subresource
    let patch = json!({
        "status": {
            "lastScheduleTime": "2024-01-01T00:00:00Z",
            "active": [{"kind": "Job", "name": "test-job-123", "namespace": "default", "uid": "uid-123"}]
        }
    });

    let req = Request::builder()
        .method("PATCH")
        .uri("/apis/batch/v1/namespaces/default/cronjobs/test-cronjob/status")
        .header("content-type", "application/merge-patch+json")
        .body(Body::from(serde_json::to_vec(&patch).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "CronJob PATCH /status must return 200 not 404"
    );

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let updated: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        updated["status"]["lastScheduleTime"],
        "2024-01-01T00:00:00Z"
    );
    assert_eq!(updated["status"]["active"][0]["name"], "test-job-123");
}

/// Regression for P0-E2E-20260424-04: CronJob status PATCH with metadata annotations
/// K8s conformance test patches /status with {"metadata": {"annotations": {"patchedstatus": "true"}}}
/// and expects the annotation to appear on the main CronJob.
#[tokio::test]
async fn test_cronjob_status_patch_metadata_annotation_preserved() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = build_test_router().await;

    let cronjob = json!({
        "apiVersion": "batch/v1",
        "kind": "CronJob",
        "metadata": {"name": "annot-cj", "namespace": "default", "annotations": {"updated": "true"}},
        "spec": {
            "schedule": "*/1 * * * *",
            "jobTemplate": {"spec": {"template": {"spec": {
                "containers": [{"name": "main", "image": "nginx"}],
                "restartPolicy": "OnFailure"
            }}}}
        }
    });
    let req = Request::builder()
        .method("POST")
        .uri("/apis/batch/v1/namespaces/default/cronjobs")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&cronjob).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // PATCH /status with metadata.annotations — conformance test scenario
    let patch = json!({
        "metadata": {"annotations": {"patchedstatus": "true"}},
        "status": {"lastScheduleTime": "2024-01-01T00:00:00Z"}
    });
    let req = Request::builder()
        .method("PATCH")
        .uri("/apis/batch/v1/namespaces/default/cronjobs/annot-cj/status")
        .header("content-type", "application/merge-patch+json")
        .body(Body::from(serde_json::to_vec(&patch).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let updated: serde_json::Value = serde_json::from_slice(&body).unwrap();
    // The annotation from the status PATCH must be in the main resource
    assert_eq!(
        updated["metadata"]["annotations"]["patchedstatus"], "true",
        "status PATCH must propagate metadata.annotations to main resource"
    );
    // Previously existing annotations must also be preserved
    assert_eq!(
        updated["metadata"]["annotations"]["updated"], "true",
        "status PATCH must preserve existing annotations"
    );
    // Status must be updated
    assert_eq!(
        updated["status"]["lastScheduleTime"],
        "2024-01-01T00:00:00Z"
    );
}

/// Verifies that each item in a list response includes metadata.resourceVersion.
/// K8s spec requires all objects returned by the API (including in list items)
/// to have resourceVersion set. Server-side apply and informers rely on this.
#[tokio::test]
async fn test_list_items_include_resource_version() {
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

    // Create two ConfigMaps
    for i in 0..2u32 {
        let name = format!("rv-test-{}", i);
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            &name.clone(),
            serde_json::json!({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":name,"namespace":"default"}}),
        )
        .await
        .unwrap();
    }

    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/namespaces/default/configmaps")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let list: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let items = list["items"].as_array().expect("items must be an array");
    assert_eq!(items.len(), 2, "should have 2 items");

    for item in items {
        let rv = item["metadata"]["resourceVersion"].as_str();
        assert!(
            rv.is_some() && !rv.unwrap().is_empty(),
            "each list item must have metadata.resourceVersion set, got item: {:?}",
            item["metadata"]["name"]
        );
        // resourceVersion must be a numeric string (K8s convention)
        let rv_str = rv.unwrap();
        assert!(
            rv_str.parse::<i64>().is_ok(),
            "metadata.resourceVersion must be a numeric string, got '{}'",
            rv_str
        );
    }
}

/// Verifies list items have resourceVersion when paginating (not just full-list).
#[tokio::test]
async fn test_list_items_include_resource_version_when_paginating() {
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

    for i in 0..4u32 {
        let name = format!("pg-rv-{:04}", i);
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            &name.clone(),
            serde_json::json!({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":name,"namespace":"default"}}),
        )
        .await
        .unwrap();
    }

    // Fetch first page (limit=2)
    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/namespaces/default/configmaps?limit=2")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let page1: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let items = page1["items"].as_array().expect("items must be array");
    assert_eq!(items.len(), 2);

    for item in items {
        let rv = item["metadata"]["resourceVersion"].as_str();
        assert!(
            rv.is_some() && !rv.unwrap().is_empty(),
            "paginated list item must have metadata.resourceVersion"
        );
    }

    // Fetch second page
    let token = page1["metadata"]["continue"]
        .as_str()
        .expect("continue token must be present");
    let req2 = Request::builder()
        .method("GET")
        .uri(format!(
            "/api/v1/namespaces/default/configmaps?limit=2&continue={}",
            urlencoding::encode(token)
        ))
        .body(Body::empty())
        .unwrap();
    let resp2 = app.oneshot(req2).await.unwrap();
    assert_eq!(resp2.status(), StatusCode::OK);

    let body2 = axum::body::to_bytes(resp2.into_body(), usize::MAX)
        .await
        .unwrap();
    let page2: serde_json::Value = serde_json::from_slice(&body2).unwrap();
    let items2 = page2["items"].as_array().expect("items must be array");
    assert_eq!(items2.len(), 2);

    for item in items2 {
        let rv = item["metadata"]["resourceVersion"].as_str();
        assert!(
            rv.is_some() && !rv.unwrap().is_empty(),
            "second page list item must have metadata.resourceVersion"
        );
    }
}

/// Verifies that listing with a limit parameter returns exactly that many items.
#[tokio::test]
async fn test_list_with_limit_returns_correct_count() {
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

    // Create 5 ConfigMaps
    for i in 0..5u32 {
        let name = format!("chunk-cm-{:04}", i);
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            &name.clone(),
            serde_json::json!({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":name,"namespace":"default"}}),
        )
        .await
        .unwrap();
    }

    // limit=2 must return exactly 2 items, not more
    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/namespaces/default/configmaps?limit=2")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let list: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let items = list["items"].as_array().expect("items must be array");
    assert_eq!(
        items.len(),
        2,
        "limit=2 must return exactly 2 items, got {}",
        items.len()
    );

    // continue token must be present (more items remain)
    assert!(
        list["metadata"]["continue"].as_str().is_some(),
        "continue token must be set when items remain beyond the limit"
    );

    // remainingItemCount must be >= 1
    let remaining = list["metadata"]["remainingItemCount"].as_i64();
    assert!(
        remaining.is_some() && remaining.unwrap() >= 1,
        "remainingItemCount must be present and >= 1 when more pages exist"
    );

    // All returned items must have resourceVersion
    for item in items {
        assert!(
            item["metadata"]["resourceVersion"].as_str().is_some(),
            "list item must have metadata.resourceVersion"
        );
    }
}

/// Verifies that listing with a continue token resumes from the correct position.
#[tokio::test]
async fn test_list_with_continue_returns_next_chunk() {
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

    for i in 0..5u32 {
        let name = format!("next-cm-{:04}", i);
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            &name.clone(),
            serde_json::json!({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":name,"namespace":"default"}}),
        )
        .await
        .unwrap();
    }

    // Fetch first chunk
    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/namespaces/default/configmaps?limit=2")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let page1: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let items1 = page1["items"].as_array().unwrap();
    assert_eq!(items1.len(), 2);
    assert_eq!(items1[0]["metadata"]["name"], "next-cm-0000");
    assert_eq!(items1[1]["metadata"]["name"], "next-cm-0001");

    let token = page1["metadata"]["continue"]
        .as_str()
        .expect("continue token must be present after first chunk");

    // Fetch second chunk using the continue token — must not repeat items
    let req2 = Request::builder()
        .method("GET")
        .uri(format!(
            "/api/v1/namespaces/default/configmaps?limit=2&continue={}",
            urlencoding::encode(token)
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

    assert_eq!(items2.len(), 2, "second chunk must have 2 items");
    // Items must follow on alphabetically — no repeats, no gaps
    assert_eq!(items2[0]["metadata"]["name"], "next-cm-0002");
    assert_eq!(items2[1]["metadata"]["name"], "next-cm-0003");

    // All items must have resourceVersion
    for item in items2 {
        assert!(
            item["metadata"]["resourceVersion"].as_str().is_some(),
            "chunked item must have metadata.resourceVersion"
        );
    }
}

/// Verifies that iterating through all chunks with limit=2 returns all 5 items exactly once.
#[tokio::test]
async fn test_list_chunking_complete_iteration() {
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

    // Create 5 ConfigMaps (odd count to force uneven final page)
    for i in 0..5u32 {
        let name = format!("iter-cm-{:04}", i);
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            &name.clone(),
            serde_json::json!({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":name,"namespace":"default"}}),
        )
        .await
        .unwrap();
    }

    // Paginate through all items with chunk-size=2, collecting all names
    let mut all_names: Vec<String> = Vec::new();
    let mut page_count = 0usize;
    let mut continue_token: Option<String> = None;

    loop {
        let uri = match &continue_token {
            None => "/api/v1/namespaces/default/configmaps?limit=2".to_string(),
            Some(tok) => format!(
                "/api/v1/namespaces/default/configmaps?limit=2&continue={}",
                urlencoding::encode(tok)
            ),
        };
        let req = Request::builder()
            .method("GET")
            .uri(&uri)
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let page: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let items = page["items"].as_array().expect("items must be array");

        for item in items {
            let name = item["metadata"]["name"].as_str().unwrap().to_string();
            all_names.push(name);
            // Each item must have resourceVersion
            assert!(
                item["metadata"]["resourceVersion"].as_str().is_some(),
                "item must have metadata.resourceVersion"
            );
        }

        page_count += 1;
        continue_token = page["metadata"]["continue"].as_str().map(String::from);
        if continue_token.is_none() {
            break;
        }
    }

    // Must have seen all 5 items, no duplicates, in order
    assert_eq!(
        all_names,
        vec![
            "iter-cm-0000",
            "iter-cm-0001",
            "iter-cm-0002",
            "iter-cm-0003",
            "iter-cm-0004"
        ],
        "all 5 items must appear exactly once across all chunks"
    );
    // 5 items with limit=2 → 3 pages (2+2+1)
    assert_eq!(
        page_count, 3,
        "should take 3 pages to iterate 5 items with limit=2"
    );
}

// ========================================
// P0-S13-7: Namespace status GET tests
// ========================================

/// Regression test for Sonobuoy S13-API-1 (`namespace.go:314`).
///
/// `GET /api/v1/namespaces/{name}/status` returned 404 even after a namespace
/// was successfully created, because `get_namespace_status` delegated to the
/// generic `get_cluster_status_subresource` path which calls `db.get_resource`.
/// `get_resource` uses `is_namespaced("Namespace") == true` (the catch-all
/// branch) and queries `namespaced_resources`, but namespaces are stored in
/// the dedicated `namespaces` table.  The lookup always misses → 404.
///
/// Fix: `get_namespace_status` must delegate to `db.get_namespace` (the same
/// path used by `GET /api/v1/namespaces/{name}`).
#[tokio::test]
async fn test_get_namespace_status_returns_200_after_create() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let app = crate::api::build_router(state);

    // Create namespace via the API (stores in the `namespaces` table)
    let create_req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"namespaces-2752"}}"#,
        ))
        .unwrap();
    let create_resp = app.clone().oneshot(create_req).await.unwrap();
    assert_eq!(
        create_resp.status(),
        StatusCode::CREATED,
        "namespace creation must succeed"
    );

    // GET /api/v1/namespaces/{name}/status must return 200 and a valid body
    let status_req = Request::builder()
        .method("GET")
        .uri("/api/v1/namespaces/namespaces-2752/status")
        .header("accept", "application/json")
        .body(Body::empty())
        .unwrap();
    let status_resp = app.clone().oneshot(status_req).await.unwrap();
    assert_eq!(
        status_resp.status(),
        StatusCode::OK,
        "GET /api/v1/namespaces/{{name}}/status must return 200, not 404"
    );

    let body_bytes = axum::body::to_bytes(status_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(body["kind"], "Namespace", "kind must be Namespace");
    assert_eq!(
        body["metadata"]["name"], "namespaces-2752",
        "name must match"
    );
}

/// Regression test for P0-S13-7 write side: PUT /api/v1/namespaces/{name}/status
/// must update the namespace status (not 404).  Same root cause as the GET
/// handler — the previous implementation delegated to
/// `update_cluster_status_subresource` which reads via `db.get_resource`
/// (wrong table for namespaces).
#[tokio::test]
async fn test_update_namespace_status_returns_200_after_create() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let app = crate::api::build_router(state);

    // Create namespace via the API (stores in the `namespaces` table) with a label
    // so we can verify non-status fields are preserved across the PUT.
    let create_req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"ns-put-status","labels":{"team":"klights"}}}"#,
        ))
        .unwrap();
    let create_resp = app.clone().oneshot(create_req).await.unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);

    // PUT /status with a new phase
    let put_req = Request::builder()
        .method("PUT")
        .uri("/api/v1/namespaces/ns-put-status/status")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"ns-put-status"},"status":{"phase":"Terminating"}}"#,
        ))
        .unwrap();
    let put_resp = app.clone().oneshot(put_req).await.unwrap();
    assert_eq!(
        put_resp.status(),
        StatusCode::OK,
        "PUT /api/v1/namespaces/{{name}}/status must return 200, not 404"
    );

    // GET /status to confirm phase was persisted
    let get_req = Request::builder()
        .method("GET")
        .uri("/api/v1/namespaces/ns-put-status/status")
        .body(Body::empty())
        .unwrap();
    let get_resp = app.clone().oneshot(get_req).await.unwrap();
    assert_eq!(get_resp.status(), StatusCode::OK);
    let body_bytes = axum::body::to_bytes(get_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(
        body["status"]["phase"], "Terminating",
        "phase must reflect the PUT body"
    );

    // GET full namespace to confirm non-status fields (labels) survived PUT /status
    let get_full_req = Request::builder()
        .method("GET")
        .uri("/api/v1/namespaces/ns-put-status")
        .body(Body::empty())
        .unwrap();
    let get_full_resp = app.clone().oneshot(get_full_req).await.unwrap();
    assert_eq!(get_full_resp.status(), StatusCode::OK);
    let full_bytes = axum::body::to_bytes(get_full_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let full: serde_json::Value = serde_json::from_slice(&full_bytes).unwrap();
    assert_eq!(
        full["metadata"]["labels"]["team"], "klights",
        "non-status field (metadata.labels.team) must survive PUT /status"
    );
}

/// Regression test for P0-S13-7 write side: PATCH /api/v1/namespaces/{name}/status
/// must merge the status into the namespace (not 404).
#[tokio::test]
async fn test_patch_namespace_status_returns_200_after_create() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let app = crate::api::build_router(state);

    // Create namespace via the API with a label so we can verify non-status
    // fields are preserved across PATCH /status.
    let create_req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"ns-patch-status","labels":{"team":"klights"}}}"#,
        ))
        .unwrap();
    let create_resp = app.clone().oneshot(create_req).await.unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);

    // PATCH /status with merge patch
    let patch_req = Request::builder()
        .method("PATCH")
        .uri("/api/v1/namespaces/ns-patch-status/status")
        .header("content-type", "application/merge-patch+json")
        .body(Body::from(r#"{"status":{"phase":"Terminating"}}"#))
        .unwrap();
    let patch_resp = app.clone().oneshot(patch_req).await.unwrap();
    assert_eq!(
        patch_resp.status(),
        StatusCode::OK,
        "PATCH /api/v1/namespaces/{{name}}/status must return 200, not 404"
    );

    // GET /status to confirm phase was merged
    let get_req = Request::builder()
        .method("GET")
        .uri("/api/v1/namespaces/ns-patch-status/status")
        .body(Body::empty())
        .unwrap();
    let get_resp = app.clone().oneshot(get_req).await.unwrap();
    assert_eq!(get_resp.status(), StatusCode::OK);
    let body_bytes = axum::body::to_bytes(get_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(
        body["status"]["phase"], "Terminating",
        "phase must reflect the PATCH body"
    );

    // GET full namespace to confirm non-status fields (labels) survived PATCH /status
    let get_full_req = Request::builder()
        .method("GET")
        .uri("/api/v1/namespaces/ns-patch-status")
        .body(Body::empty())
        .unwrap();
    let get_full_resp = app.clone().oneshot(get_full_req).await.unwrap();
    assert_eq!(get_full_resp.status(), StatusCode::OK);
    let full_bytes = axum::body::to_bytes(get_full_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let full: serde_json::Value = serde_json::from_slice(&full_bytes).unwrap();
    assert_eq!(
        full["metadata"]["labels"]["team"], "klights",
        "non-status field (metadata.labels.team) must survive PATCH /status"
    );
}

/// Regression test for namespace status defaulting:
/// creating a Namespace with an explicit but empty `status` object must still
/// default `status.phase` to `Active` (K8s behavior).
#[tokio::test]
async fn test_create_namespace_with_empty_status_defaults_phase_active() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let app = crate::api::build_router(state);

    let create_req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"ns-empty-status"},"status":{}}"#,
        ))
        .unwrap();
    let create_resp = app.clone().oneshot(create_req).await.unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);

    let create_body = axum::body::to_bytes(create_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let created: serde_json::Value = serde_json::from_slice(&create_body).unwrap();
    assert_eq!(
        created["status"]["phase"], "Active",
        "create response must default status.phase=Active when status is empty"
    );

    let get_status_req = Request::builder()
        .method("GET")
        .uri("/api/v1/namespaces/ns-empty-status/status")
        .body(Body::empty())
        .unwrap();
    let get_status_resp = app.clone().oneshot(get_status_req).await.unwrap();
    assert_eq!(get_status_resp.status(), StatusCode::OK);
    let status_body = axum::body::to_bytes(get_status_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let status: serde_json::Value = serde_json::from_slice(&status_body).unwrap();
    assert_eq!(
        status["status"]["phase"], "Active",
        "persisted namespace must keep status.phase=Active"
    );
}

/// Duplicate Namespace creates must return a Kubernetes AlreadyExists Status.
/// Client-go's namespace test framework retries only when apierrors.IsAlreadyExists
/// recognizes the response; a generic/internal error makes Sonobuoy fail in BeforeEach.
#[tokio::test]
async fn duplicate_namespace_create_returns_already_exists_status() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let app = crate::api::build_router(state);
    let body = r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"ns-duplicate"}}"#;

    let first_req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces")
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap();
    let first_resp = app.clone().oneshot(first_req).await.unwrap();
    assert_eq!(first_resp.status(), StatusCode::CREATED);

    let second_req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces")
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap();
    let second_resp = app.oneshot(second_req).await.unwrap();
    assert_eq!(second_resp.status(), StatusCode::CONFLICT);

    let bytes = axum::body::to_bytes(second_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let status: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(status["kind"], "Status");
    assert_eq!(status["apiVersion"], "v1");
    assert_eq!(status["status"], "Failure");
    assert_eq!(status["reason"], "AlreadyExists");
    assert_eq!(status["code"], 409);
    assert_eq!(
        status["message"],
        r#"namespaces "ns-duplicate" already exists"#
    );
}

/// Regression test for P0-S17-32: deleting a namespace must first place it
/// into Terminating (with deletionTimestamp) rather than removing it
/// immediately.
#[tokio::test]
async fn test_delete_namespace_enters_terminating_before_final_removal() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let pod_repository = state.pod_repository.clone();
    let metrics = state.metrics.clone();
    let app = crate::api::build_router(state);

    let create_ns_req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"ns-delete-order"}}"#,
        ))
        .unwrap();
    let create_ns_resp = app.clone().oneshot(create_ns_req).await.unwrap();
    assert_eq!(create_ns_resp.status(), StatusCode::CREATED);

    let pod = db
        .create_resource(
            "v1",
            "Pod",
            Some("ns-delete-order"),
            "pod-a",
            json!({
                "apiVersion":"v1",
                "kind":"Pod",
                "metadata":{"name":"pod-a","namespace":"ns-delete-order"}
            }),
        )
        .await
        .unwrap();

    let delete_req = Request::builder()
        .method("DELETE")
        .uri("/api/v1/namespaces/ns-delete-order")
        .body(Body::empty())
        .unwrap();
    let delete_resp = app.clone().oneshot(delete_req).await.unwrap();
    assert_eq!(
        delete_resp.status(),
        StatusCode::ACCEPTED,
        "namespace delete should be asynchronous when finalizers exist"
    );
    let delete_body = axum::body::to_bytes(delete_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let delete_status: serde_json::Value =
        serde_json::from_slice(&delete_body).expect("DELETE namespace must return JSON Status");
    assert_eq!(delete_status["kind"], "Status");
    assert_eq!(delete_status["status"], "Success");
    assert_eq!(delete_status["code"], 202);

    let terminating_pod = db
        .get_resource("v1", "Pod", Some("ns-delete-order"), "pod-a")
        .await
        .unwrap()
        .expect("Pod must remain until actor cleanup");
    assert!(
        terminating_pod
            .data
            .pointer("/metadata/deletionTimestamp")
            .and_then(|v| v.as_str())
            .is_some(),
        "namespace DELETE must mark Pods terminating before actor cleanup"
    );

    assert!(
        pod_repository
            .finalize_pod_deletion_after_actor_cleanup("ns-delete-order", "pod-a", &pod.uid)
            .await
            .unwrap(),
        "actor finalization should remove terminating Pod"
    );
    crate::api::reconcile_namespace_termination(db.as_ref(), "ns-delete-order", metrics.as_ref())
        .await
        .unwrap();

    let mut namespace_deleted = false;
    for _ in 0..30 {
        let get_ns_req = Request::builder()
            .method("GET")
            .uri("/api/v1/namespaces/ns-delete-order")
            .body(Body::empty())
            .unwrap();
        let get_ns_resp = app.clone().oneshot(get_ns_req).await.unwrap();
        if get_ns_resp.status() == StatusCode::NOT_FOUND {
            namespace_deleted = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(
        namespace_deleted,
        "namespace should complete deletion when there are no blocking pod finalizers"
    );
}

#[tokio::test]
async fn test_namespace_termination_preserves_pod_until_actor_finalizes() {
    use serde_json::json;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let metrics = state.metrics.clone();

    db.create_namespace(
        "ns-actor-owned-delete",
        json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {"name": "ns-actor-owned-delete", "uid": "ns-actor-owned-delete-uid"},
            "spec": {"finalizers": ["kubernetes"]},
            "status": {"phase": "Active"}
        }),
    )
    .await
    .unwrap();
    let pod = db
        .create_resource(
            "v1",
            "Pod",
            Some("ns-actor-owned-delete"),
            "pod-a",
            json!({
                "apiVersion":"v1",
                "kind":"Pod",
                "metadata":{"name":"pod-a","namespace":"ns-actor-owned-delete","uid":"pod-a-uid"}
            }),
        )
        .await
        .unwrap();

    let namespace = db
        .get_namespace("ns-actor-owned-delete")
        .await
        .unwrap()
        .expect("namespace exists");
    let mut terminating = std::sync::Arc::unwrap_or_clone(namespace.data);
    crate::api::set_namespace_terminating_status(&mut terminating, false);
    db.update_namespace(
        "ns-actor-owned-delete",
        terminating,
        namespace.resource_version,
    )
    .await
    .unwrap();

    crate::api::reconcile_namespace_termination(
        db.as_ref(),
        "ns-actor-owned-delete",
        metrics.as_ref(),
    )
    .await
    .unwrap();

    let terminating_pod = db
        .get_resource("v1", "Pod", Some("ns-actor-owned-delete"), "pod-a")
        .await
        .unwrap()
        .expect("namespace termination must not remove the Pod row before actor cleanup");
    assert!(
        terminating_pod
            .data
            .pointer("/metadata/deletionTimestamp")
            .and_then(|v| v.as_str())
            .is_some(),
        "namespace termination must mark the Pod terminating so the actor owns cleanup"
    );
    assert_eq!(
        terminating_pod
            .data
            .pointer("/metadata/deletionGracePeriodSeconds")
            .and_then(|v| v.as_i64()),
        Some(0)
    );
    assert!(
        db.get_namespace("ns-actor-owned-delete")
            .await
            .unwrap()
            .is_some(),
        "namespace must remain until actor-owned Pod finalization removes the Pod row"
    );

    assert!(
        state
            .pod_repository
            .finalize_pod_deletion_after_actor_cleanup("ns-actor-owned-delete", "pod-a", &pod.uid,)
            .await
            .unwrap(),
        "actor finalization should remove the terminating Pod by UID"
    );
    crate::api::reconcile_namespace_termination(
        db.as_ref(),
        "ns-actor-owned-delete",
        metrics.as_ref(),
    )
    .await
    .unwrap();
    assert!(
        db.get_namespace("ns-actor-owned-delete")
            .await
            .unwrap()
            .is_none(),
        "namespace can be deleted after actor finalization removes the Pod row"
    );
}

#[tokio::test]
async fn test_namespace_delete_pod_finalizer_blocks_non_pod_deletion_until_cleared() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let pod_repository = state.pod_repository.clone();
    let metrics = state.metrics.clone();
    let app = crate::api::build_router(state);

    let create_ns_req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"ns-order-pod-finalizer"}}"#,
        ))
        .unwrap();
    let create_ns_resp = app.clone().oneshot(create_ns_req).await.unwrap();
    assert_eq!(create_ns_resp.status(), StatusCode::CREATED);

    let pod = db
        .create_resource(
            "v1",
            "Pod",
            Some("ns-order-pod-finalizer"),
            "pod-blocker",
            json!({
                "apiVersion":"v1",
                "kind":"Pod",
                "metadata":{
                    "name":"pod-blocker",
                    "namespace":"ns-order-pod-finalizer",
                    "finalizers":["example.com/hold"]
                },
                "spec":{"containers":[{"name":"hold","image":"busybox"}]}
            }),
        )
        .await
        .unwrap();

    db.create_resource(
        "v1",
        "ConfigMap",
        Some("ns-order-pod-finalizer"),
        "cm-after-pod",
        json!({
            "apiVersion":"v1",
            "kind":"ConfigMap",
            "metadata":{"name":"cm-after-pod","namespace":"ns-order-pod-finalizer"},
            "data":{"k":"v"}
        }),
    )
    .await
    .unwrap();

    let delete_ns_req = Request::builder()
        .method("DELETE")
        .uri("/api/v1/namespaces/ns-order-pod-finalizer")
        .body(Body::empty())
        .unwrap();
    let delete_ns_resp = app.clone().oneshot(delete_ns_req).await.unwrap();
    assert_eq!(delete_ns_resp.status(), StatusCode::ACCEPTED);

    let get_ns_req = Request::builder()
        .method("GET")
        .uri("/api/v1/namespaces/ns-order-pod-finalizer")
        .body(Body::empty())
        .unwrap();
    let get_ns_resp = app.clone().oneshot(get_ns_req).await.unwrap();
    assert_eq!(get_ns_resp.status(), StatusCode::OK);
    let ns_body = axum::body::to_bytes(get_ns_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let ns_obj: serde_json::Value = serde_json::from_slice(&ns_body).unwrap();
    assert_eq!(ns_obj["status"]["phase"], "Terminating");
    assert!(
        ns_obj["status"]["conditions"]
            .as_array()
            .is_some_and(|conds| conds
                .iter()
                .any(|c| c["type"] == "NamespaceDeletionContentFailure")),
        "terminating namespace should report content failure while pod finalizer blocks deletion"
    );

    let get_cm_req = Request::builder()
        .method("GET")
        .uri("/api/v1/namespaces/ns-order-pod-finalizer/configmaps/cm-after-pod")
        .body(Body::empty())
        .unwrap();
    let get_cm_resp = app.clone().oneshot(get_cm_req).await.unwrap();
    assert_eq!(
        get_cm_resp.status(),
        StatusCode::OK,
        "non-pod resources must remain while pod finalizer blocks namespace teardown"
    );

    let create_while_terminating_req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/ns-order-pod-finalizer/configmaps")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm-new","namespace":"ns-order-pod-finalizer"}}"#,
        ))
        .unwrap();
    let create_while_terminating_resp = app
        .clone()
        .oneshot(create_while_terminating_req)
        .await
        .unwrap();
    assert_eq!(
        create_while_terminating_resp.status(),
        StatusCode::FORBIDDEN,
        "namespace termination must reject creating new namespaced resources"
    );

    let get_pod_req = Request::builder()
        .method("GET")
        .uri("/api/v1/namespaces/ns-order-pod-finalizer/pods/pod-blocker")
        .body(Body::empty())
        .unwrap();
    let get_pod_resp = app.clone().oneshot(get_pod_req).await.unwrap();
    assert_eq!(get_pod_resp.status(), StatusCode::OK);
    let pod_body = axum::body::to_bytes(get_pod_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let pod_obj: serde_json::Value = serde_json::from_slice(&pod_body).unwrap();
    assert!(
        pod_obj["metadata"]["deletionTimestamp"].as_str().is_some(),
        "pod should have deletionTimestamp set while namespace deletion is blocked"
    );

    let patch_pod_req = Request::builder()
        .method("PATCH")
        .uri("/api/v1/namespaces/ns-order-pod-finalizer/pods/pod-blocker")
        .header("content-type", "application/merge-patch+json")
        .body(Body::from(r#"{"metadata":{"finalizers":[]}}"#))
        .unwrap();
    let patch_pod_resp = app.clone().oneshot(patch_pod_req).await.unwrap();
    assert_eq!(patch_pod_resp.status(), StatusCode::OK);

    assert!(
        pod_repository
            .finalize_pod_deletion_after_actor_cleanup(
                "ns-order-pod-finalizer",
                "pod-blocker",
                &pod.uid,
            )
            .await
            .unwrap(),
        "actor finalization should remove Pod after finalizers are cleared"
    );
    crate::api::reconcile_namespace_termination(
        db.as_ref(),
        "ns-order-pod-finalizer",
        metrics.as_ref(),
    )
    .await
    .unwrap();

    let mut namespace_gone = false;
    for _ in 0..30 {
        let req = Request::builder()
            .method("GET")
            .uri("/api/v1/namespaces/ns-order-pod-finalizer")
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        if resp.status() == StatusCode::NOT_FOUND {
            namespace_gone = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(
        namespace_gone,
        "namespace should be deleted after pod finalizer is cleared"
    );
}

#[tokio::test]
async fn test_stale_namespace_termination_uid_does_not_delete_recreated_namespace_content() {
    use serde_json::json;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let metrics = state.metrics.clone();

    db.create_namespace(
        "ns-recreated",
        json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {
                "name": "ns-recreated",
                "uid": "new-namespace-uid"
            },
            "spec": {"finalizers": ["kubernetes"]},
            "status": {"phase": "Active"}
        }),
    )
    .await
    .unwrap();

    db.create_resource(
        "v1",
        "Pod",
        Some("ns-recreated"),
        "new-pod",
        json!({
            "apiVersion":"v1",
            "kind":"Pod",
            "metadata":{"name":"new-pod","namespace":"ns-recreated"}
        }),
    )
    .await
    .unwrap();

    crate::api::reconcile_namespace_termination_for_uid_with_outcome(
        db.as_ref(),
        "ns-recreated",
        "old-namespace-uid",
        metrics.as_ref(),
    )
    .await
    .unwrap();

    let pod = db
        .get_resource("v1", "Pod", Some("ns-recreated"), "new-pod")
        .await
        .unwrap();
    assert!(
        pod.is_some(),
        "stale termination work for an old namespace UID must not delete content in a recreated namespace"
    );
}

/// P0-S13-4 follow-up: bulk DELETE on a namespaced collection (DELETE
/// /api/v1/namespaces/{ns}/pods) must trigger ResourceQuota reconciliation
/// immediately, not wait for the 30s periodic reconciler. This exercises the
/// `$delete_collection_fn` macro path end-to-end via the HTTP router.
#[tokio::test]
async fn test_delete_collection_pods_reconciles_resource_quota_used_immediately() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    // 1. Create namespace
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"rq-bulk"}}"#,
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // 2. Create a ResourceQuota with hard.pods=10
    let rq_body = json!({
        "apiVersion": "v1",
        "kind": "ResourceQuota",
        "metadata": {"name": "rq-pods", "namespace": "rq-bulk"},
        "spec": {"hard": {"pods": "10"}}
    });
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/rq-bulk/resourcequotas")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&rq_body).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // 3. Create 5 pods
    for i in 0..5u8 {
        let pod_body = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": format!("pod-{i}"), "namespace": "rq-bulk"},
            "spec": {"containers": [{"name": "c", "image": "busybox"}]}
        });
        let req = Request::builder()
            .method("POST")
            .uri("/api/v1/namespaces/rq-bulk/pods")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&pod_body).unwrap()))
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED, "pod-{i} create");
    }

    // After per-pod create reconciliation, status.used.pods should be 5.
    let rq = db
        .get_resource("v1", "ResourceQuota", Some("rq-bulk"), "rq-pods")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        rq.data
            .pointer("/status/used/pods")
            .and_then(|v| v.as_str()),
        Some("5"),
        "status.used.pods must be 5 after creating 5 pods"
    );

    // 4. DELETE the collection — exercises $delete_collection_fn
    let req = Request::builder()
        .method("DELETE")
        .uri("/api/v1/namespaces/rq-bulk/pods")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // 5. Immediately after the bulk DELETE returns, Pods are terminating
    // (deletionTimestamp set) but still occupy datastore rows. The
    // ResourceQuota reconciler excludes terminating pods from counting,
    // matching upstream K8s behavior.
    let rq = db
        .get_resource("v1", "ResourceQuota", Some("rq-bulk"), "rq-pods")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        rq.data
            .pointer("/status/used/pods")
            .and_then(|v| v.as_str()),
        Some("0"),
        "status.used.pods must be 0 after bulk DELETE marks all pods terminating"
    );
    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("rq-bulk"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(pods.items.len(), 5);
    assert!(
        pods.items
            .iter()
            .all(|pod| pod.data.pointer("/metadata/deletionTimestamp").is_some()),
        "bulk DELETE must mark every matching Pod terminating"
    );
}

#[tokio::test]
async fn test_delete_configmap_with_finalizer_marks_terminating_without_hard_delete() {
    use crate::datastore::WatchTarget;
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    let ns = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {"name": "finalizer-ns"}
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&ns).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let cm = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "held",
            "namespace": "finalizer-ns",
            "finalizers": ["example.com/hold"]
        },
        "data": {"k": "v"}
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces/finalizer-ns/configmaps")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&cm).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let created_rv = db.get_current_resource_version().await.unwrap();

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/v1/namespaces/finalizer-ns/configmaps/held?gracePeriodSeconds=7")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(resp.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert!(
        body.pointer("/metadata/deletionTimestamp")
            .and_then(|v| v.as_str())
            .is_some(),
        "delete response must mark ConfigMap terminating: {body:?}"
    );
    assert_eq!(
        body.pointer("/metadata/deletionGracePeriodSeconds"),
        Some(&json!(7)),
        "ConfigMap DELETE must honor query gracePeriodSeconds"
    );
    assert_eq!(
        body.pointer("/metadata/finalizers/0")
            .and_then(|v| v.as_str()),
        Some("example.com/hold")
    );

    let live = db
        .get_resource("v1", "ConfigMap", Some("finalizer-ns"), "held")
        .await
        .unwrap()
        .expect("finalizer-held ConfigMap must remain in datastore");
    assert!(
        live.data
            .pointer("/metadata/deletionTimestamp")
            .and_then(|v| v.as_str())
            .is_some(),
        "datastore ConfigMap must be terminating, not deleted: {:?}",
        live.data
    );
    assert_eq!(
        live.data
            .pointer("/metadata/finalizers/0")
            .and_then(|v| v.as_str()),
        Some("example.com/hold")
    );

    let events = db
        .list_watch_events_since(
            &[WatchTarget::namespaced_in_namespace(
                "v1",
                "ConfigMap",
                "finalizer-ns",
            )],
            created_rv,
        )
        .await
        .unwrap();
    assert!(
        events
            .iter()
            .any(|event| event.resource.name == "held" && event.event_type.as_ref() == "MODIFIED"),
        "marking the held ConfigMap terminating must emit a MODIFIED event: {events:?}"
    );
    assert!(
        events
            .iter()
            .all(|event| event.resource.name != "held" || event.event_type.as_ref() != "DELETED"),
        "first delete of a finalizer-held ConfigMap must not emit DELETED: {events:?}"
    );
}

#[tokio::test]
async fn test_delete_collection_configmap_with_finalizer_does_not_cascade_child() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_namespace(
        "finalizer-collection-ns",
        json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {"name": "finalizer-collection-ns"}
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "ConfigMap",
        Some("finalizer-collection-ns"),
        "held-owner",
        json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "held-owner",
                "namespace": "finalizer-collection-ns",
                "uid": "held-owner-uid",
                "finalizers": ["example.com/hold"]
            },
            "data": {"owner": "true"}
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Secret",
        Some("finalizer-collection-ns"),
        "owned-child",
        json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "name": "owned-child",
                "namespace": "finalizer-collection-ns",
                "ownerReferences": [{
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "name": "held-owner",
                    "uid": "held-owner-uid"
                }]
            },
            "type": "Opaque",
            "data": {}
        }),
    )
    .await
    .unwrap();

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/v1/namespaces/finalizer-collection-ns/configmaps")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let parent = db
        .get_resource(
            "v1",
            "ConfigMap",
            Some("finalizer-collection-ns"),
            "held-owner",
        )
        .await
        .unwrap()
        .expect("delete-collection must not hard-delete finalizer-held parent");
    assert!(
        parent
            .data
            .pointer("/metadata/deletionTimestamp")
            .and_then(|v| v.as_str())
            .is_some(),
        "finalizer-held parent must be marked terminating: {:?}",
        parent.data
    );
    assert_eq!(
        parent
            .data
            .pointer("/metadata/finalizers/0")
            .and_then(|v| v.as_str()),
        Some("example.com/hold")
    );

    let child = db
        .get_resource(
            "v1",
            "Secret",
            Some("finalizer-collection-ns"),
            "owned-child",
        )
        .await
        .unwrap()
        .expect("child must remain because held parent was not hard-deleted");
    assert!(
        child.data.pointer("/metadata/deletionTimestamp").is_none(),
        "delete-collection must not cascade from a parent that was only marked terminating: {:?}",
        child.data
    );
}

#[tokio::test]
async fn test_delete_collection_dry_run_does_not_delete_configmaps_or_pods() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_namespace(
        "dryrun-collection-ns",
        json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {"name": "dryrun-collection-ns"}
        }),
    )
    .await
    .unwrap();

    for name in ["cm-a", "cm-b"] {
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("dryrun-collection-ns"),
            name,
            json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "name": name,
                    "namespace": "dryrun-collection-ns"
                },
                "data": {"key": name}
            }),
        )
        .await
        .unwrap();
    }

    for name in ["pod-a", "pod-b"] {
        db.create_resource(
            "v1",
            "Pod",
            Some("dryrun-collection-ns"),
            name,
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": name,
                    "namespace": "dryrun-collection-ns",
                    "labels": {"delete": "dryrun"}
                },
                "spec": {
                    "containers": [{"name": "app", "image": "registry.k8s.io/pause:3.10"}]
                },
                "status": {"phase": "Running"}
            }),
        )
        .await
        .unwrap();
    }

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/v1/namespaces/dryrun-collection-ns/configmaps?dryRun=All")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/v1/namespaces/dryrun-collection-ns/pods?labelSelector=delete%3Ddryrun&dryRun=All")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    for name in ["cm-a", "cm-b"] {
        let config_map = db
            .get_resource("v1", "ConfigMap", Some("dryrun-collection-ns"), name)
            .await
            .unwrap()
            .expect("dry-run deletecollection must not remove ConfigMaps");
        assert!(
            config_map
                .data
                .pointer("/metadata/deletionTimestamp")
                .is_none(),
            "dry-run deletecollection must not mark ConfigMap {name} terminating: {:?}",
            config_map.data
        );
    }

    for name in ["pod-a", "pod-b"] {
        let pod = db
            .get_resource("v1", "Pod", Some("dryrun-collection-ns"), name)
            .await
            .unwrap()
            .expect("dry-run deletecollection must not remove Pods");
        assert!(
            pod.data.pointer("/metadata/deletionTimestamp").is_none(),
            "dry-run deletecollection must not mark Pod {name} terminating: {:?}",
            pod.data
        );
    }
}

#[tokio::test]
async fn test_delete_collection_stale_same_name_replacement_does_not_delete_or_cascade() {
    let state = build_test_app_state().await;
    let db = state.db.clone();

    db.create_namespace(
        "stale-delete-collection-ns",
        json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {"name": "stale-delete-collection-ns"}
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "ConfigMap",
        Some("stale-delete-collection-ns"),
        "same-name",
        json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "same-name",
                "namespace": "stale-delete-collection-ns",
                "uid": "old-owner-uid"
            },
            "data": {"generation": "old"}
        }),
    )
    .await
    .unwrap();
    let stale = db
        .get_resource(
            "v1",
            "ConfigMap",
            Some("stale-delete-collection-ns"),
            "same-name",
        )
        .await
        .unwrap()
        .expect("old owner must exist");

    db.delete_resource_with_preconditions(
        "v1",
        "ConfigMap",
        Some("stale-delete-collection-ns"),
        "same-name",
        crate::datastore::ResourcePreconditions::uid("old-owner-uid"),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "ConfigMap",
        Some("stale-delete-collection-ns"),
        "same-name",
        json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "same-name",
                "namespace": "stale-delete-collection-ns",
                "uid": "new-owner-uid"
            },
            "data": {"generation": "new"}
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Secret",
        Some("stale-delete-collection-ns"),
        "old-owned-child",
        json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "name": "old-owned-child",
                "namespace": "stale-delete-collection-ns",
                "ownerReferences": [{
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "name": "same-name",
                    "uid": "old-owner-uid"
                }]
            },
            "type": "Opaque",
            "data": {}
        }),
    )
    .await
    .unwrap();

    let deleted = crate::api::generated_handlers::inners::delete_collection_listed_resource_inner(
        std::sync::Arc::new(state.clone()),
        "v1",
        "ConfigMap",
        Some("stale-delete-collection-ns"),
        stale,
    )
    .await
    .expect("stale UID conflict must be treated as collection progress");
    assert!(
        !deleted,
        "stale list item must not be reported as hard-deleted"
    );

    let replacement = db
        .get_resource(
            "v1",
            "ConfigMap",
            Some("stale-delete-collection-ns"),
            "same-name",
        )
        .await
        .unwrap()
        .expect("same-name replacement must remain");
    assert_eq!(replacement.uid, "new-owner-uid");
    let child = db
        .get_resource(
            "v1",
            "Secret",
            Some("stale-delete-collection-ns"),
            "old-owned-child",
        )
        .await
        .unwrap()
        .expect("old child must not be cascaded from stale owner UID");
    assert!(
        child.data.pointer("/metadata/deletionTimestamp").is_none(),
        "stale owner UID must not cascade old child: {:?}",
        child.data
    );
}

#[tokio::test]
async fn test_configmap_finalizer_drain_hard_deletes_non_pod_row() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_namespace(
        "finalizer-drain-ns",
        json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {"name": "finalizer-drain-ns"}
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "ConfigMap",
        Some("finalizer-drain-ns"),
        "held",
        json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "held",
                "namespace": "finalizer-drain-ns",
                "finalizers": ["example.com/hold"]
            },
            "data": {"k": "v"}
        }),
    )
    .await
    .unwrap();

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/v1/namespaces/finalizer-drain-ns/configmaps/held")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    assert!(
        db.get_resource("v1", "ConfigMap", Some("finalizer-drain-ns"), "held")
            .await
            .unwrap()
            .is_some(),
        "finalizer-held ConfigMap must remain after initial delete"
    );

    let update = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "held",
            "namespace": "finalizer-drain-ns",
            "finalizers": []
        },
        "data": {"k": "v"}
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/api/v1/namespaces/finalizer-drain-ns/configmaps/held")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&update).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(
        db.get_resource("v1", "ConfigMap", Some("finalizer-drain-ns"), "held")
            .await
            .unwrap()
            .is_none(),
        "clearing finalizers on a terminating non-Pod must complete hard delete"
    );
}

#[tokio::test]
async fn test_orphan_delete_configmap_removes_child_ownerrefs_before_owner_delete() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_namespace(
        "orphan-delete-ns",
        json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "orphan-delete-ns"}}),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "ConfigMap",
        Some("orphan-delete-ns"),
        "owner",
        json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "owner", "namespace": "orphan-delete-ns", "uid": "orphan-owner-uid"}
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Secret",
        Some("orphan-delete-ns"),
        "child",
        json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "name": "child",
                "namespace": "orphan-delete-ns",
                "ownerReferences": [{
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "name": "owner",
                    "uid": "orphan-owner-uid"
                }]
            },
            "type": "Opaque",
            "data": {}
        }),
    )
    .await
    .unwrap();

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/v1/namespaces/orphan-delete-ns/configmaps/owner")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"propagationPolicy":"Orphan"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(
        db.get_resource("v1", "ConfigMap", Some("orphan-delete-ns"), "owner")
            .await
            .unwrap()
            .is_none(),
        "orphan delete must hard-delete owner"
    );
    let child = db
        .get_resource("v1", "Secret", Some("orphan-delete-ns"), "child")
        .await
        .unwrap()
        .expect("orphan delete must keep child");
    let owner_refs = child.data.pointer("/metadata/ownerReferences");
    assert!(
        owner_refs
            .and_then(|v| v.as_array())
            .is_none_or(|refs| refs.is_empty()),
        "orphan delete must remove child ownerReferences before owner deletion: {:?}",
        child.data
    );
}

#[tokio::test]
async fn test_orphan_delete_deployment_removes_replicaset_ownerrefs() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_namespace(
        "orphan-deploy-ns",
        json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {"name": "orphan-deploy-ns"}
        }),
    )
    .await
    .unwrap();

    let deployment = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": "demo", "namespace": "orphan-deploy-ns"},
        "spec": {
            "replicas": 2,
            "selector": {"matchLabels": {"app": "demo"}},
            "template": {
                "metadata": {"labels": {"app": "demo"}},
                "spec": {"containers": [{"name": "nginx", "image": "registry.k8s.io/e2e-test-images/nginx:1.14-4"}]}
            }
        }
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apps/v1/namespaces/orphan-deploy-ns/deployments")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&deployment).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let created: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let deployment_uid = created
        .pointer("/metadata/uid")
        .and_then(|value| value.as_str())
        .expect("deployment uid");

    let replicasets = db
        .list_resources(
            "apps/v1",
            "ReplicaSet",
            Some("orphan-deploy-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap()
        .items;
    assert_eq!(
        replicasets.len(),
        1,
        "Deployment reconcile must create one ReplicaSet"
    );
    assert_eq!(
        replicasets[0]
            .data
            .pointer("/metadata/ownerReferences/0/uid")
            .and_then(|value| value.as_str()),
        Some(deployment_uid),
        "test precondition: ReplicaSet must start owned by the Deployment"
    );

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/apis/apps/v1/namespaces/orphan-deploy-ns/deployments/demo")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"propagationPolicy":"Orphan"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let orphaned = db
        .list_resources(
            "apps/v1",
            "ReplicaSet",
            Some("orphan-deploy-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap()
        .items;
    assert_eq!(
        orphaned.len(),
        1,
        "Orphan delete must keep the Deployment's ReplicaSet"
    );
    let owner_refs = orphaned[0].data.pointer("/metadata/ownerReferences");
    assert!(
        owner_refs
            .and_then(|value| value.as_array())
            .is_none_or(|refs| refs.is_empty()),
        "Orphan-deleted Deployment must remove ownerReferences from the ReplicaSet: {:?}",
        orphaned[0].data
    );
}

#[tokio::test]
async fn test_finalizer_held_orphan_delete_orphans_child_and_drain_does_not_cascade() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_namespace(
        "orphan-finalizer-ns",
        json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "orphan-finalizer-ns"}}),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "ConfigMap",
        Some("orphan-finalizer-ns"),
        "owner",
        json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "owner",
                "namespace": "orphan-finalizer-ns",
                "uid": "orphan-finalizer-owner-uid",
                "finalizers": ["example.com/hold"]
            }
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Secret",
        Some("orphan-finalizer-ns"),
        "child",
        json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "name": "child",
                "namespace": "orphan-finalizer-ns",
                "ownerReferences": [{
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "name": "owner",
                    "uid": "orphan-finalizer-owner-uid"
                }]
            },
            "type": "Opaque",
            "data": {}
        }),
    )
    .await
    .unwrap();

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/v1/namespaces/orphan-finalizer-ns/configmaps/owner")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"propagationPolicy":"Orphan"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);

    let owner = db
        .get_resource("v1", "ConfigMap", Some("orphan-finalizer-ns"), "owner")
        .await
        .unwrap()
        .expect("finalizer-held orphan owner must remain terminating");
    assert!(
        owner
            .data
            .pointer("/metadata/deletionTimestamp")
            .and_then(|v| v.as_str())
            .is_some(),
        "orphan finalizer delete must mark parent terminating: {:?}",
        owner.data
    );
    let child = db
        .get_resource("v1", "Secret", Some("orphan-finalizer-ns"), "child")
        .await
        .unwrap()
        .expect("orphan finalizer delete must keep child");
    assert!(
        child
            .data
            .pointer("/metadata/ownerReferences")
            .and_then(|v| v.as_array())
            .is_none_or(|refs| refs.is_empty()),
        "orphan finalizer delete must remove child ownerReferences immediately: {:?}",
        child.data
    );

    let update = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "owner", "namespace": "orphan-finalizer-ns", "finalizers": []}
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/api/v1/namespaces/orphan-finalizer-ns/configmaps/owner")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&update).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(
        db.get_resource("v1", "ConfigMap", Some("orphan-finalizer-ns"), "owner")
            .await
            .unwrap()
            .is_none(),
        "finalizer drain must remove orphan-deleted owner"
    );
    assert!(
        db.get_resource("v1", "Secret", Some("orphan-finalizer-ns"), "child")
            .await
            .unwrap()
            .is_some(),
        "finalizer drain must not cascade previously orphaned child"
    );
}

#[tokio::test]
async fn test_single_delete_explicit_uid_race_returns_conflict_not_notfound() {
    let state = build_test_app_state().await;
    let db = state.db.clone();

    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "cm-explicit-uid-race",
        json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "cm-explicit-uid-race", "namespace": "default", "uid": "old-uid"}
        }),
    )
    .await
    .unwrap();
    let stale = db
        .get_resource("v1", "ConfigMap", Some("default"), "cm-explicit-uid-race")
        .await
        .unwrap()
        .expect("old resource must exist");
    db.delete_resource_with_preconditions(
        "v1",
        "ConfigMap",
        Some("default"),
        "cm-explicit-uid-race",
        crate::datastore::ResourcePreconditions::uid("old-uid"),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "cm-explicit-uid-race",
        json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "cm-explicit-uid-race", "namespace": "default", "uid": "new-uid"}
        }),
    )
    .await
    .unwrap();

    let err =
        crate::api::generated_handlers::inners::complete_non_foreground_delete_with_live_recheck(
            db.as_ref(),
            crate::api::generated_handlers::inners::GeneratedDeleteCompletionRequest {
                target: crate::api::finalizer_delete::ResourceDeleteTarget {
                    api_version: "v1",
                    kind: "ConfigMap",
                    namespace: Some("default"),
                    name: "cm-explicit-uid-race",
                },
                initial_resource: stale,
                delete_preconditions: crate::datastore::ResourcePreconditions::uid("old-uid"),
                orphan_children_before_completion: false,
                uid_mismatch_is_conflict: true,
            },
        )
        .await
        .expect_err("explicit UID mismatch must return conflict");
    assert!(
        matches!(err, crate::api::AppError::Conflict(_)),
        "expected explicit UID race to return Conflict, got {err:?}"
    );
}

/// `/apis` aggregated discovery must only contain named API groups.
/// The core/legacy v1 group belongs under `/api`, not `/apis`.
#[tokio::test]
async fn test_aggregated_discovery_excludes_core_group() {
    let body = fetch_aggregated_discovery_body().await;
    let items = body["items"]
        .as_array()
        .expect("aggregated discovery items must be an array");
    assert!(
        items
            .iter()
            .all(|item| item["metadata"]["name"].as_str().unwrap_or_default() != ""),
        "aggregated /apis must not include the core group; got items: {:?}",
        items
    );
}

#[tokio::test]
async fn test_api_aggregated_discovery_includes_core_v1_namespaces() {
    let body = fetch_core_aggregated_discovery_body().await;
    let resources = aggregated_resources_for(&body, "", "v1");
    assert!(
        resources.contains(&"namespaces".to_string()),
        "aggregated /api must include core group ('') v1 with namespaces. Got: {:?}",
        resources
    );
}

/// Regression for aggregated_discovery.go:282
/// validatingwebhookconfigurations must appear in admissionregistration.k8s.io/v1
/// in the aggregated discovery response.
#[tokio::test]
async fn test_aggregated_discovery_admissionregistration_includes_vwc() {
    let body = fetch_aggregated_discovery_body().await;
    let resources = aggregated_resources_for(&body, "admissionregistration.k8s.io", "v1");
    assert!(
        resources.contains(&"validatingwebhookconfigurations".to_string()),
        "admissionregistration.k8s.io/v1 aggregated discovery must include \
        validatingwebhookconfigurations. Got: {:?}",
        resources
    );
}

#[tokio::test]
async fn test_admissionregistration_v1_discovery_includes_webhook_status_subresources() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let app = crate::api::build_router(state);

    let req = Request::builder()
        .method("GET")
        .uri("/apis/admissionregistration.k8s.io/v1")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body_bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    let resources = body["resources"]
        .as_array()
        .expect("resources must be an array")
        .iter()
        .filter_map(|r| r["name"].as_str())
        .collect::<Vec<_>>();

    assert!(
        resources.contains(&"mutatingwebhookconfigurations/status"),
        "admissionregistration/v1 discovery must include mutatingwebhookconfigurations/status. Got: {:?}",
        resources
    );
    assert!(
        resources.contains(&"validatingwebhookconfigurations/status"),
        "admissionregistration/v1 discovery must include validatingwebhookconfigurations/status. Got: {:?}",
        resources
    );
}

/// Regression for apimachinery/flowcontrol.go conformance:
/// flowcontrol.apiserver.k8s.io/v1 discovery must publish status subresources.
#[tokio::test]
async fn test_flowcontrol_v1_standard_discovery_includes_status_subresources() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let app = crate::api::build_router(state);

    let req = Request::builder()
        .method("GET")
        .uri("/apis/flowcontrol.apiserver.k8s.io/v1")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body_bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

    let resources = body["resources"]
        .as_array()
        .expect("resources must be an array");
    let names: Vec<&str> = resources
        .iter()
        .filter_map(|r| r["name"].as_str())
        .collect();

    assert!(
        names.contains(&"flowschemas/status"),
        "flowcontrol v1 discovery must include flowschemas/status, got: {:?}",
        names
    );
    assert!(
        names.contains(&"prioritylevelconfigurations/status"),
        "flowcontrol v1 discovery must include prioritylevelconfigurations/status, got: {:?}",
        names
    );
}

/// Regression for apimachinery/flowcontrol.go conformance:
/// aggregated discovery must also expose flowcontrol status subresources.
#[tokio::test]
async fn test_aggregated_discovery_flowcontrol_includes_status_subresources() {
    let body = fetch_aggregated_discovery_body().await;
    let resources = aggregated_resources_for(&body, "flowcontrol.apiserver.k8s.io", "v1");

    assert!(
        resources.contains(&"flowschemas/status".to_string()),
        "aggregated discovery flowcontrol v1 must include flowschemas/status, got: {:?}",
        resources
    );
    assert!(
        resources.contains(&"prioritylevelconfigurations/status".to_string()),
        "aggregated discovery flowcontrol v1 must include prioritylevelconfigurations/status, got: {:?}",
        resources
    );
}

#[tokio::test]
async fn test_flowschema_api_patch_response_includes_incremented_resource_version() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let app = crate::api::build_router(state);

    let create_body = json!({
        "apiVersion": "flowcontrol.apiserver.k8s.io/v1",
        "kind": "FlowSchema",
        "metadata": {
            "name": "e2e-fs-rv",
            "labels": {"example-e2e-fs-label": "rv"}
        },
        "spec": {
            "matchingPrecedence": 10000,
            "priorityLevelConfiguration": {"name": "global-default"},
            "distinguisherMethod": {"type": "ByUser"},
            "rules": [{
                "subjects": [{
                    "kind": "User",
                    "user": {"name": "example-e2e-user"}
                }],
                "nonResourceRules": [{
                    "verbs": ["*"],
                    "nonResourceURLs": ["*"]
                }]
            }]
        }
    });

    let create_req = Request::builder()
        .method("POST")
        .uri("/apis/flowcontrol.apiserver.k8s.io/v1/flowschemas")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&create_body).unwrap()))
        .unwrap();
    let create_resp = app.clone().oneshot(create_req).await.unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);
    let create_json: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(create_resp.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    let create_rv = create_json["metadata"]["resourceVersion"]
        .as_str()
        .expect("FlowSchema create must return metadata.resourceVersion")
        .parse::<i64>()
        .expect("create resourceVersion must be numeric");

    let patch_body = json!({
        "metadata": {"annotations": {"patched": "true"}},
        "spec": {"matchingPrecedence": 9999}
    });
    let patch_req = Request::builder()
        .method("PATCH")
        .uri("/apis/flowcontrol.apiserver.k8s.io/v1/flowschemas/e2e-fs-rv")
        .header("content-type", "application/merge-patch+json")
        .body(Body::from(serde_json::to_vec(&patch_body).unwrap()))
        .unwrap();
    let patch_resp = app.clone().oneshot(patch_req).await.unwrap();
    assert_eq!(patch_resp.status(), StatusCode::OK);
    let patch_json: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(patch_resp.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();

    assert_eq!(patch_json["metadata"]["annotations"]["patched"], "true");
    assert_eq!(patch_json["spec"]["matchingPrecedence"], 9999);
    let patch_rv = patch_json["metadata"]["resourceVersion"]
        .as_str()
        .expect("FlowSchema patch response must include metadata.resourceVersion")
        .parse::<i64>()
        .expect("patch resourceVersion must be numeric");
    assert!(
        patch_rv > create_rv,
        "FlowSchema patch resourceVersion must increase: create={}, patch={}",
        create_rv,
        patch_rv
    );
}

#[tokio::test]
async fn test_prioritylevelconfiguration_api_patch_response_includes_incremented_resource_version()
{
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let app = crate::api::build_router(state);

    let create_body = json!({
        "apiVersion": "flowcontrol.apiserver.k8s.io/v1",
        "kind": "PriorityLevelConfiguration",
        "metadata": {
            "name": "e2e-pl-rv",
            "labels": {"example-e2e-pl-label": "rv"}
        },
        "spec": {
            "type": "Limited",
            "limited": {
                "nominalConcurrencyShares": 2,
                "limitResponse": {"type": "Reject"}
            }
        }
    });

    let create_req = Request::builder()
        .method("POST")
        .uri("/apis/flowcontrol.apiserver.k8s.io/v1/prioritylevelconfigurations")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&create_body).unwrap()))
        .unwrap();
    let create_resp = app.clone().oneshot(create_req).await.unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);
    let create_json: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(create_resp.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    let create_rv = create_json["metadata"]["resourceVersion"]
        .as_str()
        .expect("PriorityLevelConfiguration create must return metadata.resourceVersion")
        .parse::<i64>()
        .expect("create resourceVersion must be numeric");

    let patch_body = json!({
        "metadata": {"annotations": {"patched": "true"}},
        "spec": {"limited": {"nominalConcurrencyShares": 4}}
    });
    let patch_req = Request::builder()
        .method("PATCH")
        .uri("/apis/flowcontrol.apiserver.k8s.io/v1/prioritylevelconfigurations/e2e-pl-rv")
        .header("content-type", "application/merge-patch+json")
        .body(Body::from(serde_json::to_vec(&patch_body).unwrap()))
        .unwrap();
    let patch_resp = app.clone().oneshot(patch_req).await.unwrap();
    assert_eq!(patch_resp.status(), StatusCode::OK);
    let patch_json: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(patch_resp.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();

    assert_eq!(patch_json["metadata"]["annotations"]["patched"], "true");
    assert_eq!(patch_json["spec"]["limited"]["nominalConcurrencyShares"], 4);
    let patch_rv = patch_json["metadata"]["resourceVersion"]
        .as_str()
        .expect("PriorityLevelConfiguration patch response must include metadata.resourceVersion")
        .parse::<i64>()
        .expect("patch resourceVersion must be numeric");
    assert!(
        patch_rv > create_rv,
        "PriorityLevelConfiguration patch resourceVersion must increase: create={}, patch={}",
        create_rv,
        patch_rv
    );
}

/// Regression for apimachinery/flowcontrol.go conformance:
/// watch=true without sendInitialEvents must not replay stale ADDED events for
/// pre-existing FlowSchemas.
#[tokio::test]
async fn test_flowschema_watch_default_does_not_replay_added_for_existing_object() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use futures::StreamExt;
    use serde_json::json;
    use std::time::Duration;
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let app = crate::api::build_router(state);

    let create_body = json!({
        "apiVersion": "flowcontrol.apiserver.k8s.io/v1",
        "kind": "FlowSchema",
        "metadata": {"name": "e2e-fs-watch-default"},
        "spec": {
            "matchingPrecedence": 10000,
            "priorityLevelConfiguration": {"name": "global-default"},
            "distinguisherMethod": {"type": "ByUser"},
            "rules": [{
                "subjects": [{
                    "kind": "User",
                    "user": {"name": "example-e2e-user"}
                }],
                "nonResourceRules": [{
                    "verbs": ["*"],
                    "nonResourceURLs": ["*"]
                }]
            }]
        }
    });
    let create_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/flowcontrol.apiserver.k8s.io/v1/flowschemas")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&create_body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);

    let watch_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/flowcontrol.apiserver.k8s.io/v1/flowschemas?watch=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(watch_resp.status(), StatusCode::OK);
    let mut stream = watch_resp.into_body().into_data_stream();

    // Default watch=true must not replay existing resources.
    // Before fix this returned an immediate ADDED event for e2e-fs-watch-default.
    let result = tokio::time::timeout(Duration::from_millis(300), stream.next()).await;
    assert!(
        result.is_err(),
        "watch=true default must not replay stale ADDED events for existing FlowSchema objects"
    );
}

/// Regression for apimachinery/flowcontrol.go conformance:
/// watch=true without sendInitialEvents must not replay stale ADDED events for
/// pre-existing PriorityLevelConfigurations.
#[tokio::test]
async fn test_prioritylevel_watch_default_does_not_replay_added_for_existing_object() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use futures::StreamExt;
    use serde_json::json;
    use std::time::Duration;
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let app = crate::api::build_router(state);

    let create_body = json!({
        "apiVersion": "flowcontrol.apiserver.k8s.io/v1",
        "kind": "PriorityLevelConfiguration",
        "metadata": {"name": "e2e-pl-watch-default"},
        "spec": {
            "type": "Limited",
            "limited": {
                "nominalConcurrencyShares": 2,
                "limitResponse": {"type": "Reject"}
            }
        }
    });
    let create_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/flowcontrol.apiserver.k8s.io/v1/prioritylevelconfigurations")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&create_body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);

    let watch_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/flowcontrol.apiserver.k8s.io/v1/prioritylevelconfigurations?watch=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(watch_resp.status(), StatusCode::OK);
    let mut stream = watch_resp.into_body().into_data_stream();

    // Default watch=true must not replay existing resources.
    // Before fix this returned an immediate ADDED event for e2e-pl-watch-default.
    let result = tokio::time::timeout(Duration::from_millis(300), stream.next()).await;
    assert!(
        result.is_err(),
        "watch=true default must not replay stale ADDED events for existing PriorityLevelConfiguration objects"
    );
}

/// Regression for garbage_collector.go:436
/// Orphan-deleting an RC must NOT delete the pods it created.
/// After orphan delete: all pods must still exist in the API (any phase).
#[tokio::test]
async fn test_gc_orphan_rc_delete_pods_survive() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let app = crate::api::build_router(state);

    // Create RC with 3 replicas (small for speed, same logic as 50)
    let rc_body = json!({
        "apiVersion": "v1",
        "kind": "ReplicationController",
        "metadata": {"name": "gc-orphan-test", "namespace": "default"},
        "spec": {
            "replicas": 3,
            "selector": {"app": "gc-orphan"},
            "template": {
                "metadata": {"labels": {"app": "gc-orphan"}},
                "spec": {"containers": [{"name": "app", "image": "registry.example.invalid/klights/test-image:1"}]}
            }
        }
    });
    let create = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/default/replicationcontrollers")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&rc_body).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(create).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Verify 3 pods were created synchronously by the reconcile
    let list = Request::builder()
        .method("GET")
        .uri("/api/v1/namespaces/default/pods")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(list).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        body["items"].as_array().map(|v| v.len()).unwrap_or(0),
        3,
        "RC reconcile must create 3 pods synchronously. Got: {:?}",
        body["items"].as_array().map(|v| v
            .iter()
            .map(|p| p["metadata"]["name"].as_str())
            .collect::<Vec<_>>())
    );

    // Delete RC with Orphan propagation policy
    let del = Request::builder()
        .method("DELETE")
        .uri("/api/v1/namespaces/default/replicationcontrollers/gc-orphan-test")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"propagationPolicy":"Orphan"}"#))
        .unwrap();
    let resp = app.clone().oneshot(del).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Immediately after orphan delete: pods must still exist
    let list2 = Request::builder()
        .method("GET")
        .uri("/api/v1/namespaces/default/pods")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(list2).await.unwrap();
    let body2: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        body2["items"].as_array().map(|v| v.len()).unwrap_or(0),
        3,
        "After orphan delete, 3 pods must still exist. Got: {:?}",
        body2["items"]
            .as_array()
            .map(|v| v.iter().map(|p| &p["metadata"]["name"]).collect::<Vec<_>>())
    );

    // Verify pods have no ownerReferences (they're orphaned)
    for pod in body2["items"].as_array().unwrap() {
        let owner_refs = pod["metadata"]["ownerReferences"].as_array();
        assert!(
            owner_refs.is_none() || owner_refs.unwrap().is_empty(),
            "Orphaned pod must have no ownerReferences. Got: {:?}",
            owner_refs
        );
    }
}

#[tokio::test]
async fn test_gc_foreground_rc_delete_retains_parent_until_pods_are_deleted() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "ReplicationController",
        Some("default"),
        "gc-foreground-test",
        json!({
            "apiVersion": "v1",
            "kind": "ReplicationController",
            "metadata": {
                "name": "gc-foreground-test",
                "namespace": "default",
                "uid": "rc-foreground-uid"
            },
            "spec": {
                "replicas": 1,
                "selector": {"app": "gc-foreground"},
                "template": {
                    "metadata": {"labels": {"app": "gc-foreground"}},
                    "spec": {"containers": [{"name": "app", "image": "registry.k8s.io/pause:3.10"}]}
                }
            }
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "gc-foreground-child",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "gc-foreground-child",
                "namespace": "default",
                "uid": "pod-foreground-uid",
                "labels": {"app": "gc-foreground"},
                "ownerReferences": [{
                    "apiVersion": "v1",
                    "kind": "ReplicationController",
                    "name": "gc-foreground-test",
                    "uid": "rc-foreground-uid",
                    "controller": true,
                    "blockOwnerDeletion": true
                }]
            },
            "spec": {"containers": [{"name": "app", "image": "registry.k8s.io/pause:3.10"}]},
            "status": {"phase": "Running"}
        }),
    )
    .await
    .unwrap();

    let del = Request::builder()
        .method("DELETE")
        .uri("/api/v1/namespaces/default/replicationcontrollers/gc-foreground-test")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"propagationPolicy":"Foreground","preconditions":{"uid":"rc-foreground-uid"}}"#,
        ))
        .unwrap();
    let resp = app.clone().oneshot(del).await.unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);

    let parent = db
        .get_resource(
            "v1",
            "ReplicationController",
            Some("default"),
            "gc-foreground-test",
        )
        .await
        .unwrap()
        .expect("foreground delete must retain parent while owned Pods remain");
    assert!(
        parent
            .data
            .pointer("/metadata/deletionTimestamp")
            .and_then(|v| v.as_str())
            .is_some(),
        "foreground-deleting RC must have deletionTimestamp: {:?}",
        parent.data
    );
    assert!(
        parent
            .data
            .pointer("/metadata/finalizers")
            .and_then(|v| v.as_array())
            .is_some_and(|finalizers| finalizers
                .iter()
                .any(|f| f.as_str() == Some("foregroundDeletion"))),
        "foreground-deleting RC must carry foregroundDeletion finalizer: {:?}",
        parent.data
    );

    let child = db
        .get_resource("v1", "Pod", Some("default"), "gc-foreground-child")
        .await
        .unwrap()
        .expect("foreground GC must not hard-delete Pod rows");
    assert!(
        child
            .data
            .pointer("/metadata/deletionTimestamp")
            .and_then(|v| v.as_str())
            .is_some(),
        "foreground GC must mark owned Pod terminating through the Pod delete path: {:?}",
        child.data
    );
}

#[tokio::test]
async fn test_gc_foreground_delete_ignores_non_blocking_owner_ref_child() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "gc-nonblocking-owner",
        json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "gc-nonblocking-owner",
                "namespace": "default",
                "uid": "cm-nonblocking-owner-uid"
            },
            "data": {"role": "owner"}
        }),
    )
    .await
    .unwrap();

    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "gc-nonblocking-child",
        json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "gc-nonblocking-child",
                "namespace": "default",
                "uid": "cm-nonblocking-child-uid",
                "finalizers": ["example.com/hold"],
                "ownerReferences": [{
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "name": "gc-nonblocking-owner",
                    "uid": "cm-nonblocking-owner-uid",
                    "blockOwnerDeletion": false
                }]
            },
            "data": {"role": "child"}
        }),
    )
    .await
    .unwrap();

    let del = Request::builder()
        .method("DELETE")
        .uri("/api/v1/namespaces/default/configmaps/gc-nonblocking-owner")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"propagationPolicy":"Foreground","preconditions":{"uid":"cm-nonblocking-owner-uid"}}"#,
        ))
        .unwrap();
    let resp = app.clone().oneshot(del).await.unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);

    let parent = db
        .get_resource("v1", "ConfigMap", Some("default"), "gc-nonblocking-owner")
        .await
        .unwrap();
    assert!(
        parent.is_none(),
        "non-blocking ownerReferences must not retain a foreground-deleting owner: {:?}",
        parent.map(|resource| resource.data)
    );

    let child = db
        .get_resource("v1", "ConfigMap", Some("default"), "gc-nonblocking-child")
        .await
        .unwrap()
        .expect("non-blocking dependent should remain after foreground owner finalization");
    assert!(
        child
            .data
            .pointer("/metadata/deletionTimestamp")
            .and_then(|v| v.as_str())
            .is_none(),
        "non-blocking dependent must not be deleted by foreground GC: {:?}",
        child.data
    );
}

#[tokio::test]
async fn test_foreground_delete_mark_retries_internal_rv_conflict_without_user_rv_precondition() {
    let state = build_test_app_state().await;
    let db = state.db.clone();

    db.create_resource(
        "v1",
        "ReplicationController",
        Some("default"),
        "gc-foreground-race",
        json!({
            "apiVersion": "v1",
            "kind": "ReplicationController",
            "metadata": {
                "name": "gc-foreground-race",
                "namespace": "default",
                "uid": "rc-foreground-race-uid",
                "finalizers": ["example.com/keep"]
            },
            "spec": {
                "replicas": 1,
                "selector": {"app": "gc-foreground-race"},
                "template": {
                    "metadata": {"labels": {"app": "gc-foreground-race"}},
                    "spec": {"containers": [{"name": "app", "image": "registry.k8s.io/pause:3.10"}]}
                }
            },
            "status": {"replicas": 0, "readyReplicas": 0}
        }),
    )
    .await
    .unwrap();

    let stale = db
        .get_resource(
            "v1",
            "ReplicationController",
            Some("default"),
            "gc-foreground-race",
        )
        .await
        .unwrap()
        .expect("created rc must exist");

    let bumped = db
        .update_status_only_with_preconditions(
            "v1",
            "ReplicationController",
            Some("default"),
            "gc-foreground-race",
            json!({"replicas": 1, "readyReplicas": 1}),
            crate::datastore::ResourcePreconditions::from_resource(&stale),
        )
        .await
        .unwrap();
    assert!(bumped.resource_version > stale.resource_version);

    let updated = crate::api::generated_handlers::inners::mark_foreground_deletion_with_retry(
        db.as_ref(),
        "v1",
        "ReplicationController",
        Some("default"),
        "gc-foreground-race",
        stale,
        crate::datastore::ResourcePreconditions::uid("rc-foreground-race-uid"),
    )
    .await
    .expect("foreground delete marking must retry internal rv conflicts");

    assert!(
        updated
            .data
            .pointer("/metadata/deletionTimestamp")
            .and_then(|v| v.as_str())
            .is_some(),
        "foreground delete must set deletionTimestamp: {:?}",
        updated.data
    );
    let finalizers = updated
        .data
        .pointer("/metadata/finalizers")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(
        finalizers
            .iter()
            .any(|f| f.as_str() == Some("example.com/keep")),
        "foreground delete must preserve existing finalizers: {:?}",
        finalizers
    );
    assert!(
        finalizers
            .iter()
            .any(|f| f.as_str() == Some("foregroundDeletion")),
        "foreground delete must add foregroundDeletion finalizer: {:?}",
        finalizers
    );
    assert_eq!(
        updated
            .data
            .pointer("/status/replicas")
            .and_then(|v| v.as_i64()),
        Some(1),
        "foreground delete retry must preserve concurrent status updates"
    );
}

#[tokio::test]
async fn test_finalizer_delete_mark_retries_internal_rv_conflict_preserving_concurrent_update() {
    let state = build_test_app_state().await;
    let db = state.db.clone();

    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "cm-finalizer-race",
        json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "cm-finalizer-race",
                "namespace": "default",
                "uid": "cm-finalizer-race-uid",
                "finalizers": ["example.com/hold"]
            },
            "data": {"key": "old"}
        }),
    )
    .await
    .unwrap();

    let stale = db
        .get_resource("v1", "ConfigMap", Some("default"), "cm-finalizer-race")
        .await
        .unwrap()
        .expect("created ConfigMap must exist");

    let bumped = db
        .update_resource_with_preconditions(
            "v1",
            "ConfigMap",
            Some("default"),
            "cm-finalizer-race",
            json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "name": "cm-finalizer-race",
                    "namespace": "default",
                    "uid": "cm-finalizer-race-uid",
                    "finalizers": ["example.com/hold"]
                },
                "data": {"key": "new"}
            }),
            crate::datastore::ResourcePreconditions::from_resource(&stale),
        )
        .await
        .unwrap();
    assert!(bumped.resource_version > stale.resource_version);

    let outcome =
        crate::api::generated_handlers::inners::complete_non_foreground_delete_with_live_recheck(
            db.as_ref(),
            crate::api::generated_handlers::inners::GeneratedDeleteCompletionRequest {
                target: crate::api::finalizer_delete::ResourceDeleteTarget {
                    api_version: "v1",
                    kind: "ConfigMap",
                    namespace: Some("default"),
                    name: "cm-finalizer-race",
                },
                initial_resource: stale,
                delete_preconditions: crate::datastore::ResourcePreconditions::uid(
                    "cm-finalizer-race-uid",
                ),
                orphan_children_before_completion: false,
                uid_mismatch_is_conflict: false,
            },
        )
        .await
        .expect("finalizer delete marking must retry internal rv conflicts");
    let updated = match outcome {
        crate::api::generated_handlers::inners::DeleteCompletion::MarkedTerminating(resource) => {
            resource
        }
        other => panic!("expected MarkedTerminating, got {other:?}"),
    };

    assert!(
        updated
            .data
            .pointer("/metadata/deletionTimestamp")
            .and_then(|v| v.as_str())
            .is_some(),
        "finalizer delete must set deletionTimestamp: {:?}",
        updated.data
    );
    assert_eq!(
        updated
            .data
            .pointer("/metadata/finalizers/0")
            .and_then(|v| v.as_str()),
        Some("example.com/hold")
    );
    assert_eq!(
        updated.data.pointer("/data/key").and_then(|v| v.as_str()),
        Some("new"),
        "finalizer delete retry must preserve concurrent data updates"
    );
}

#[tokio::test]
async fn test_delete_collection_finalizer_mark_retries_internal_rv_conflict_preserving_concurrent_update()
 {
    let state = build_test_app_state().await;
    let db = state.db.clone();

    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "cm-collection-finalizer-race",
        json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "cm-collection-finalizer-race",
                "namespace": "default",
                "uid": "cm-collection-finalizer-race-uid",
                "finalizers": ["example.com/hold"]
            },
            "data": {"key": "old"}
        }),
    )
    .await
    .unwrap();

    let stale = db
        .get_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "cm-collection-finalizer-race",
        )
        .await
        .unwrap()
        .expect("created ConfigMap must exist");

    db.update_resource_with_preconditions(
        "v1",
        "ConfigMap",
        Some("default"),
        "cm-collection-finalizer-race",
        json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "cm-collection-finalizer-race",
                "namespace": "default",
                "uid": "cm-collection-finalizer-race-uid",
                "finalizers": ["example.com/hold"]
            },
            "data": {"key": "new"}
        }),
        crate::datastore::ResourcePreconditions::from_resource(&stale),
    )
    .await
    .unwrap();

    let outcome =
        crate::api::generated_handlers::inners::complete_non_foreground_delete_with_live_recheck(
            db.as_ref(),
            crate::api::generated_handlers::inners::GeneratedDeleteCompletionRequest {
                target: crate::api::finalizer_delete::ResourceDeleteTarget {
                    api_version: "v1",
                    kind: "ConfigMap",
                    namespace: Some("default"),
                    name: "cm-collection-finalizer-race",
                },
                initial_resource: stale,
                delete_preconditions: crate::datastore::ResourcePreconditions::uid(
                    "cm-collection-finalizer-race-uid",
                ),
                orphan_children_before_completion: false,
                uid_mismatch_is_conflict: false,
            },
        )
        .await
        .expect("collection finalizer mark should handle internal rv conflicts");
    let updated = match outcome {
        crate::api::generated_handlers::inners::DeleteCompletion::MarkedTerminating(resource) => {
            resource
        }
        other => panic!("expected MarkedTerminating, got {other:?}"),
    };

    assert!(
        updated
            .data
            .pointer("/metadata/deletionTimestamp")
            .and_then(|v| v.as_str())
            .is_some(),
        "finalizer delete must set deletionTimestamp: {:?}",
        updated.data
    );
    assert_eq!(
        updated
            .data
            .pointer("/metadata/finalizers/0")
            .and_then(|v| v.as_str()),
        Some("example.com/hold")
    );
    assert_eq!(
        updated.data.pointer("/data/key").and_then(|v| v.as_str()),
        Some("new"),
        "collection finalizer mark retry must preserve concurrent data updates"
    );
}

#[tokio::test]
async fn test_single_delete_live_recheck_marks_when_same_uid_gains_finalizer() {
    let state = build_test_app_state().await;
    let db = state.db.clone();

    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "cm-single-live-finalizer",
        json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "cm-single-live-finalizer",
                "namespace": "default",
                "uid": "cm-single-live-finalizer-uid"
            },
            "data": {"key": "old"}
        }),
    )
    .await
    .unwrap();

    let stale = db
        .get_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "cm-single-live-finalizer",
        )
        .await
        .unwrap()
        .expect("created ConfigMap must exist");

    db.update_resource_with_preconditions(
        "v1",
        "ConfigMap",
        Some("default"),
        "cm-single-live-finalizer",
        json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "cm-single-live-finalizer",
                "namespace": "default",
                "uid": "cm-single-live-finalizer-uid",
                "finalizers": ["example.com/hold"]
            },
            "data": {"key": "new"}
        }),
        crate::datastore::ResourcePreconditions::from_resource(&stale),
    )
    .await
    .unwrap();

    let outcome =
        crate::api::generated_handlers::inners::complete_non_foreground_delete_with_live_recheck(
            db.as_ref(),
            crate::api::generated_handlers::inners::GeneratedDeleteCompletionRequest {
                target: crate::api::finalizer_delete::ResourceDeleteTarget {
                    api_version: "v1",
                    kind: "ConfigMap",
                    namespace: Some("default"),
                    name: "cm-single-live-finalizer",
                },
                initial_resource: stale,
                delete_preconditions: crate::datastore::ResourcePreconditions::uid(
                    "cm-single-live-finalizer-uid",
                ),
                orphan_children_before_completion: false,
                uid_mismatch_is_conflict: false,
            },
        )
        .await
        .expect("live recheck should mark same-UID finalizer add as terminating");

    let updated = match outcome {
        crate::api::generated_handlers::inners::DeleteCompletion::MarkedTerminating(resource) => {
            resource
        }
        other => panic!("expected MarkedTerminating, got {other:?}"),
    };
    assert!(
        updated
            .data
            .pointer("/metadata/deletionTimestamp")
            .and_then(|v| v.as_str())
            .is_some(),
        "live finalizer add must be marked terminating: {:?}",
        updated.data
    );
    assert_eq!(
        updated
            .data
            .pointer("/metadata/finalizers/0")
            .and_then(|v| v.as_str()),
        Some("example.com/hold")
    );
    assert_eq!(
        updated.data.pointer("/data/key").and_then(|v| v.as_str()),
        Some("new"),
        "live recheck must preserve concurrent same-UID update"
    );
}

#[tokio::test]
async fn test_delete_collection_live_recheck_marks_when_same_uid_gains_finalizer() {
    let state = build_test_app_state().await;
    let db = state.db.clone();

    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "cm-collection-live-finalizer",
        json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "cm-collection-live-finalizer",
                "namespace": "default",
                "uid": "cm-collection-live-finalizer-uid"
            },
            "data": {"key": "old"}
        }),
    )
    .await
    .unwrap();

    let stale = db
        .get_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "cm-collection-live-finalizer",
        )
        .await
        .unwrap()
        .expect("created ConfigMap must exist");

    db.update_resource_with_preconditions(
        "v1",
        "ConfigMap",
        Some("default"),
        "cm-collection-live-finalizer",
        json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "cm-collection-live-finalizer",
                "namespace": "default",
                "uid": "cm-collection-live-finalizer-uid",
                "finalizers": ["example.com/hold"]
            },
            "data": {"key": "new"}
        }),
        crate::datastore::ResourcePreconditions::from_resource(&stale),
    )
    .await
    .unwrap();

    let deleted = crate::api::generated_handlers::inners::delete_collection_listed_resource_inner(
        std::sync::Arc::new(state.clone()),
        "v1",
        "ConfigMap",
        Some("default"),
        stale,
    )
    .await
    .expect("collection live recheck should handle same-UID finalizer add");
    assert!(
        !deleted,
        "collection item marked terminating must not be reported hard-deleted"
    );

    let live = db
        .get_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "cm-collection-live-finalizer",
        )
        .await
        .unwrap()
        .expect("finalizer-held ConfigMap must remain");
    assert!(
        live.data
            .pointer("/metadata/deletionTimestamp")
            .and_then(|v| v.as_str())
            .is_some(),
        "collection live finalizer add must be marked terminating: {:?}",
        live.data
    );
    assert_eq!(
        live.data.pointer("/data/key").and_then(|v| v.as_str()),
        Some("new"),
        "collection live recheck must preserve concurrent same-UID update"
    );
}

#[tokio::test]
async fn test_node_proxy_route_accepts_name_with_port_suffix() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "Node",
        None,
        "dp",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "dp"},
            "spec": {},
            "status": {}
        }),
    )
    .await
    .unwrap();

    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "mypod",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "mypod", "namespace": "default"},
            "spec": {
                "nodeName": "dp",
                "containers": [{"name": "c1", "image": "nginx"}]
            },
            "status": {}
        }),
    )
    .await
    .unwrap();

    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/nodes/dp:10250/proxy/pods")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body_bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(body["kind"], "PodList");
    let items = body["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["metadata"]["name"], "mypod");
}

#[tokio::test]
async fn test_pod_and_service_proxy_routes_return_backend_response() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tower::ServiceExt;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let _ = stream.read(&mut buf).await.unwrap();
            let response = "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello";
            stream.write_all(response.as_bytes()).await.unwrap();
        }
    });

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "web-pod",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "web-pod", "namespace": "default"},
            "spec": {
                "containers": [{
                    "name": "web",
                    "image": "nginx",
                    "ports": [{"containerPort": port}]
                }]
            },
            "status": {"podIP": "127.0.0.1"}
        }),
    )
    .await
    .unwrap();

    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "web-svc",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "web-svc", "namespace": "default"},
            "spec": {
                "ports": [{"port": port, "targetPort": port}]
            }
        }),
    )
    .await
    .unwrap();

    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "web-svc",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {"name": "web-svc", "namespace": "default"},
            "subsets": [{
                "addresses": [{"ip": "127.0.0.1"}],
                "ports": [{"port": port}]
            }]
        }),
    )
    .await
    .unwrap();

    let pod_req = Request::builder()
        .method("GET")
        .uri("/api/v1/namespaces/default/pods/web-pod/proxy/")
        .body(Body::empty())
        .unwrap();
    let pod_resp = app.clone().oneshot(pod_req).await.unwrap();
    assert_eq!(pod_resp.status(), StatusCode::OK);
    let pod_body = axum::body::to_bytes(pod_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(&pod_body[..], b"hello");

    let svc_req = Request::builder()
        .method("GET")
        .uri("/api/v1/namespaces/default/services/web-svc/proxy/")
        .body(Body::empty())
        .unwrap();
    let svc_resp = app.oneshot(svc_req).await.unwrap();
    assert_eq!(svc_resp.status(), StatusCode::OK);
    let svc_body = axum::body::to_bytes(svc_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(&svc_body[..], b"hello");
}

#[tokio::test]
async fn test_pod_and_service_proxy_filters_sensitive_forwarded_headers() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;
    use tokio::time::{Duration, timeout};
    use tower::ServiceExt;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (requests_tx, mut requests_rx) = mpsc::channel::<String>(2);

    tokio::spawn(async move {
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let n = stream.read(&mut buf).await.unwrap();
            let request = String::from_utf8_lossy(&buf[..n]).into_owned();
            requests_tx.send(request).await.unwrap();
            let response = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
            stream.write_all(response.as_bytes()).await.unwrap();
        }
    });

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let bootstrap_token = crate::bootstrap::bootstrap_token::generate_bootstrap_token();
    crate::bootstrap::bootstrap_token::create_scoped_bootstrap_token_secret_for_test(
        db.as_ref(),
        crate::bootstrap::bootstrap_token::BootstrapTokenScope::Worker,
        &bootstrap_token,
    )
    .await
    .unwrap();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "web-pod",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "web-pod", "namespace": "default"},
            "spec": {
                "containers": [{
                    "name": "web",
                    "image": "nginx",
                    "ports": [{"containerPort": port}]
                }]
            },
            "status": {"podIP": "127.0.0.1"}
        }),
    )
    .await
    .unwrap();

    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "web-svc",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "web-svc", "namespace": "default"},
            "spec": {"ports": [{"port": port, "targetPort": port}]}
        }),
    )
    .await
    .unwrap();

    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "web-svc",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {"name": "web-svc", "namespace": "default"},
            "subsets": [{
                "addresses": [{"ip": "127.0.0.1"}],
                "ports": [{"port": port}]
            }]
        }),
    )
    .await
    .unwrap();

    for uri in [
        "/api/v1/namespaces/default/pods/web-pod/proxy/",
        "/api/v1/namespaces/default/services/web-svc/proxy/",
    ] {
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(uri)
                    .header("authorization", format!("Bearer {bootstrap_token}"))
                    .header("proxy-authorization", "Basic d29ya2xvYWQ6c2VjcmV0")
                    .header("impersonate-user", "mallory")
                    .header("impersonate-group", "system:authenticated")
                    .header("x-remote-user", "spoofed-user")
                    .header("x-remote-group", "spoofed-group")
                    .header("x-remote-extra-project", "spoofed-extra")
                    .header("x-trace-id", "trace-123")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "{uri} should proxy");
    }

    for proxy_kind in ["Pod", "Service"] {
        let request = timeout(Duration::from_secs(2), requests_rx.recv())
            .await
            .expect("backend should receive proxied request")
            .expect("backend should capture request");
        let lower = request.to_ascii_lowercase();
        for forbidden in [
            "\r\nauthorization:",
            "\r\nproxy-authorization:",
            "\r\nimpersonate-user:",
            "\r\nimpersonate-group:",
            "\r\nx-remote-user:",
            "\r\nx-remote-group:",
            "\r\nx-remote-extra-project:",
        ] {
            assert!(
                !lower.contains(forbidden),
                "{proxy_kind} proxy must not forward sensitive header {forbidden:?}. got:\n{request}"
            );
        }
        assert!(
            lower.contains("\r\nx-trace-id: trace-123\r\n"),
            "{proxy_kind} proxy should preserve non-sensitive end-to-end headers. got:\n{request}"
        );
    }
}

#[tokio::test]
async fn test_pod_and_service_proxy_root_get_redirects_to_trailing_slash() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode, header};
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "web-pod",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "web-pod", "namespace": "default"},
            "spec": {
                "containers": [{
                    "name": "web",
                    "image": "nginx",
                    "ports": [{"containerPort": 80}]
                }]
            },
            "status": {"podIP": "127.0.0.1"}
        }),
    )
    .await
    .unwrap();

    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "web-svc",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "web-svc", "namespace": "default"},
            "spec": {
                "ports": [{"port": 80, "targetPort": 80}]
            }
        }),
    )
    .await
    .unwrap();

    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "web-svc",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {"name": "web-svc", "namespace": "default"},
            "subsets": [{
                "addresses": [{"ip": "127.0.0.1"}],
                "ports": [{"port": 80}]
            }]
        }),
    )
    .await
    .unwrap();

    let pod_req = Request::builder()
        .method("GET")
        .uri("/api/v1/namespaces/default/pods/web-pod/proxy?method=GET")
        .body(Body::empty())
        .unwrap();
    let pod_resp = app.clone().oneshot(pod_req).await.unwrap();
    assert_eq!(pod_resp.status(), StatusCode::MOVED_PERMANENTLY);
    assert_eq!(
        pod_resp.headers().get(header::LOCATION).unwrap(),
        "/api/v1/namespaces/default/pods/web-pod/proxy/?method=GET"
    );

    let svc_req = Request::builder()
        .method("GET")
        .uri("/api/v1/namespaces/default/services/web-svc/proxy?method=GET")
        .body(Body::empty())
        .unwrap();
    let svc_resp = app.oneshot(svc_req).await.unwrap();
    assert_eq!(svc_resp.status(), StatusCode::MOVED_PERMANENTLY);
    assert_eq!(
        svc_resp.headers().get(header::LOCATION).unwrap(),
        "/api/v1/namespaces/default/services/web-svc/proxy/?method=GET"
    );
}

#[tokio::test]
async fn test_service_proxy_named_port_uses_matching_endpoint_port() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tower::ServiceExt;

    let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port_a = listener_a.local_addr().unwrap().port();
    let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port_b = listener_b.local_addr().unwrap().port();

    tokio::spawn(async move {
        let (mut stream, _) = listener_a.accept().await.unwrap();
        let mut buf = vec![0u8; 4096];
        let _ = stream.read(&mut buf).await.unwrap();
        let response = "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nfoo";
        stream.write_all(response.as_bytes()).await.unwrap();
    });
    tokio::spawn(async move {
        let (mut stream, _) = listener_b.accept().await.unwrap();
        let mut buf = vec![0u8; 4096];
        let _ = stream.read(&mut buf).await.unwrap();
        let response = "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nbar";
        stream.write_all(response.as_bytes()).await.unwrap();
    });

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "web-svc",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "web-svc", "namespace": "default"},
            "spec": {
                "ports": [
                    {"name": "portname1", "port": 80, "targetPort": 160},
                    {"name": "portname2", "port": 81, "targetPort": 162}
                ]
            }
        }),
    )
    .await
    .unwrap();

    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "web-svc",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {"name": "web-svc", "namespace": "default"},
            "subsets": [{
                "addresses": [{"ip": "127.0.0.1"}],
                "ports": [
                    {"name": "portname1", "port": port_a},
                    {"name": "portname2", "port": port_b}
                ]
            }]
        }),
    )
    .await
    .unwrap();

    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/namespaces/default/services/web-svc:portname1/proxy/")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(&body[..], b"foo");
}

#[tokio::test]
async fn test_service_proxy_https_get_retries_transient_tls_eof() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio_rustls::TlsAcceptor;
    use tower::ServiceExt;

    let _ = rustls::crypto::ring::default_provider().install_default();

    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_der = rustls::pki_types::CertificateDer::from(cert.cert.der().to_vec());
    let key_der = rustls::pki_types::PrivateKeyDer::Pkcs8(
        rustls::pki_types::PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der()),
    );
    let server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key_der)
        .unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(server_config));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        drop(stream);

        let (stream, _) = listener.accept().await.unwrap();
        let mut stream = acceptor.accept(stream).await.unwrap();
        let mut buf = vec![0u8; 4096];
        let _ = stream.read(&mut buf).await.unwrap();
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
            .await
            .unwrap();
    });

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "web-svc",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "web-svc", "namespace": "default"},
            "spec": {
                "ports": [{"name": "tlsportname2", "port": 443, "targetPort": port}]
            }
        }),
    )
    .await
    .unwrap();

    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "web-svc",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {"name": "web-svc", "namespace": "default"},
            "subsets": [{
                "addresses": [{"ip": "127.0.0.1"}],
                "ports": [{"name": "tlsportname2", "port": port}]
            }]
        }),
    )
    .await
    .unwrap();

    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/namespaces/default/services/https:web-svc:tlsportname2/proxy/")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(&body[..], b"ok");
}

/// Regression for aggregator.go:374
/// POST /apis/apiregistration.k8s.io/v1/apiservices must work (was 404).
#[tokio::test]
async fn test_apiservice_crud_create_and_list() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let app = crate::api::build_router(state);

    let body = json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": {"name": "v1alpha1.wardle.example.com"},
        "spec": {
            "group": "wardle.example.com",
            "version": "v1alpha1",
            "groupPriorityMinimum": 1,
            "versionPriority": 1,
            "insecureSkipTLSVerify": true,
            "service": {"name": "wardle-service", "namespace": "default", "port": 443}
        }
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiregistration.k8s.io/v1/apiservices")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::CREATED,
        "POST apiservices must return 201"
    );

    let resp2 = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/apiregistration.k8s.io/v1/apiservices")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let list: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(resp2.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        list["items"].as_array().map(|v| v.len()).unwrap_or(0),
        1,
        "apiservices list must return 1 item"
    );
}

#[tokio::test]
async fn test_apiservice_status_subresource_returns_available_condition() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let app = crate::api::build_router(state);

    let service = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "wardle-service", "namespace": "default"},
        "spec": {"ports": [{"port": 443}]}
    });
    let create_service = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces/default/services")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&service).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_service.status(), StatusCode::CREATED);

    let endpoints = json!({
        "apiVersion": "v1",
        "kind": "Endpoints",
        "metadata": {"name": "wardle-service", "namespace": "default"},
        "subsets": [{"addresses": [{"ip": "127.0.0.1"}], "ports": [{"port": 443}]}]
    });
    let create_endpoints = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces/default/endpoints")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&endpoints).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_endpoints.status(), StatusCode::CREATED);

    let body = json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": {"name": "v1alpha1.wardle.example.com"},
        "spec": {
            "group": "wardle.example.com",
            "version": "v1alpha1",
            "groupPriorityMinimum": 1000,
            "versionPriority": 10,
            "insecureSkipTLSVerify": true,
            "service": {"name": "wardle-service", "namespace": "default", "port": 443}
        }
    });

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiregistration.k8s.io/v1/apiservices")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    let status = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/apiregistration.k8s.io/v1/apiservices/v1alpha1.wardle.example.com/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        status.status(),
        StatusCode::OK,
        "APIService status subresource must be readable"
    );
    let json: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(status.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        json["status"]["conditions"][0]["message"], "all checks passed",
        "APIService status.conditions must include the aggregator-ready message"
    );
}

#[tokio::test]
async fn test_apiservice_status_subresource_put_updates_status_only() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let app = crate::api::build_router(state);

    let create_body = json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": {"name": "v1alpha1.wardle.example.com"},
        "spec": {
            "group": "wardle.example.com",
            "version": "v1alpha1",
            "groupPriorityMinimum": 1000,
            "versionPriority": 10,
            "insecureSkipTLSVerify": true,
            "service": {"name": "wardle-service", "namespace": "default", "port": 443}
        }
    });

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiregistration.k8s.io/v1/apiservices")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&create_body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    let put_body = json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": {"name": "v1alpha1.wardle.example.com"},
        "status": {
            "conditions": [{
                "type": "Available",
                "status": "False",
                "reason": "ServiceNotFound",
                "message": "wardle service not found"
            }]
        }
    });

    let update = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/apis/apiregistration.k8s.io/v1/apiservices/v1alpha1.wardle.example.com/status")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&put_body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        update.status(),
        StatusCode::OK,
        "APIService PUT /status must return 200"
    );
    let updated: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(update.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        updated["status"]["conditions"][0]["reason"], "ServiceNotFound",
        "PUT /status must persist status.conditions"
    );

    let get = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/apiregistration.k8s.io/v1/apiservices/v1alpha1.wardle.example.com")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::OK);
    let got: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(get.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        got["spec"]["group"], "wardle.example.com",
        "PUT /status must not overwrite spec"
    );
    assert_eq!(
        got["status"]["conditions"][0]["reason"], "ServiceNotFound",
        "status update must be visible on main resource GET"
    );
}

#[tokio::test]
async fn test_apiservice_delete_collection_with_label_selector() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let app = crate::api::build_router(state);

    for (name, keep) in [
        ("v1alpha1.wardle.example.com", false),
        ("v1beta1.wardle.example.com", true),
    ] {
        let body = json!({
            "apiVersion": "apiregistration.k8s.io/v1",
            "kind": "APIService",
            "metadata": {
                "name": name,
                "labels": {
                    "cleanup": if keep { "no" } else { "yes" }
                }
            },
            "spec": {
                "group": "wardle.example.com",
                "version": if keep { "v1beta1" } else { "v1alpha1" },
                "groupPriorityMinimum": 1000,
                "versionPriority": 10,
                "insecureSkipTLSVerify": true,
                "service": {"name": "wardle-service", "namespace": "default", "port": 443}
            }
        });
        let create = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/apis/apiregistration.k8s.io/v1/apiservices")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(create.status(), StatusCode::CREATED);
    }

    let del = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/apis/apiregistration.k8s.io/v1/apiservices?labelSelector=cleanup%3Dyes")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        del.status(),
        StatusCode::OK,
        "APIService delete collection with labelSelector must return 200"
    );

    let list = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/apiregistration.k8s.io/v1/apiservices")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list.status(), StatusCode::OK);
    let body: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(list.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    let names: Vec<String> = body["items"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|v| {
            v.pointer("/metadata/name")
                .and_then(|x| x.as_str())
                .map(ToString::to_string)
        })
        .collect();
    assert!(
        !names.contains(&"v1alpha1.wardle.example.com".to_string()),
        "deleteCollection must delete matched APIService"
    );
    assert!(
        names.contains(&"v1beta1.wardle.example.com".to_string()),
        "deleteCollection must keep non-matching APIService"
    );
}

#[tokio::test]
async fn test_apiservice_group_discovery_is_exposed_via_apis_group() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let app = crate::api::build_router(state);

    let body = json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": {"name": "v1alpha1.wardle.example.com"},
        "spec": {
            "group": "wardle.example.com",
            "version": "v1alpha1",
            "groupPriorityMinimum": 1000,
            "versionPriority": 10,
            "insecureSkipTLSVerify": true,
            "service": {"name": "wardle-service", "namespace": "default", "port": 18081}
        }
    });

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiregistration.k8s.io/v1/apiservices")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    let group = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/wardle.example.com")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        group.status(),
        StatusCode::OK,
        "GET /apis/<group> must expose APIService-backed groups"
    );

    let group_body: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(group.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(group_body["name"], "wardle.example.com");
    assert_eq!(group_body["preferredVersion"]["version"], "v1alpha1");
}

#[tokio::test]
async fn test_apiservice_paths_proxy_to_registered_service_backend() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    use serde_json::json;
    use std::sync::{Arc, Mutex};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio_rustls::TlsAcceptor;
    use tower::ServiceExt;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let cert =
        rcgen::generate_simple_self_signed(vec!["wardle-service.default.svc".to_string()]).unwrap();
    let cert_der = CertificateDer::from(cert.cert.der().to_vec());
    let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der()));
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key_der)
        .unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(server_config));

    let observed_hosts = Arc::new(Mutex::new(Vec::<String>::new()));
    let observed_hosts_task = observed_hosts.clone();
    let expected_host = format!("wardle-service.default.svc:{port}");

    tokio::spawn(async move {
        for _ in 0..2 {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = acceptor.accept(stream).await.unwrap();
            let mut buf = vec![0u8; 4096];
            let n = stream.read(&mut buf).await.unwrap();
            let req = String::from_utf8_lossy(&buf[..n]);
            let host_header = req
                .lines()
                .find(|line| line.to_ascii_lowercase().starts_with("host:"))
                .map(str::to_string)
                .unwrap_or_else(|| "missing".to_string());
            observed_hosts_task.lock().unwrap().push(host_header);
            let is_discovery = req.starts_with("GET /apis/wardle.example.com/v1alpha1 ");
            let body = if is_discovery {
                r#"{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"wardle.example.com/v1alpha1","resources":[{"name":"flunders","singularName":"flunder","namespaced":true,"kind":"Flunder","verbs":["get","list","watch","create","delete","patch","update"]}]}"#
            } else {
                r#"{"kind":"FlunderList","apiVersion":"wardle.example.com/v1alpha1","items":[]}"#
            };
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        }
    });

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "spec": {"ports": [{"port": port}]}
        }),
    )
    .await
    .unwrap();

    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "subsets": [{"addresses": [{"ip": "127.0.0.1"}], "ports": [{"port": port}]}]
        }),
    )
    .await
    .unwrap();

    let apiservice = json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": {"name": "v1alpha1.wardle.example.com"},
        "spec": {
            "group": "wardle.example.com",
            "version": "v1alpha1",
            "groupPriorityMinimum": 1000,
            "versionPriority": 10,
            "insecureSkipTLSVerify": true,
            "service": {"name": "wardle-service", "namespace": "default", "port": port}
        }
    });

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiregistration.k8s.io/v1/apiservices")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&apiservice).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    let discovery = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/wardle.example.com/v1alpha1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        discovery.status(),
        StatusCode::OK,
        "APIService-backed group/version discovery must proxy to backend service"
    );

    let list = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/wardle.example.com/v1alpha1/flunders")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        list.status(),
        StatusCode::OK,
        "APIService-backed resource list path must proxy to backend service"
    );

    let hosts = observed_hosts.lock().unwrap().clone();
    assert!(
        hosts.iter().all(|h| h.contains(&expected_host)),
        "proxies must use service DNS host {expected_host}; got hosts: {hosts:?}"
    );
}

#[tokio::test]
async fn test_apiservice_service_reference_uses_https_on_non_443_port() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;
    use tokio::time::{Duration, timeout};
    use tower::ServiceExt;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (first_byte_tx, first_byte_rx) = oneshot::channel::<Option<u8>>();

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 512];
        let n = stream.read(&mut buf).await.unwrap_or(0);
        let _ = first_byte_tx.send((n > 0).then_some(buf[0]));

        // If client accidentally used plaintext HTTP, return a success payload so
        // the test can detect the wrong behavior via status code.
        if n >= 4 && &buf[..4] == b"GET " {
            let body = r#"{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"wardle.example.com/v1alpha1","resources":[]}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(response.as_bytes()).await;
        }
    });

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "spec": {"ports": [{"port": port}]}
        }),
    )
    .await
    .unwrap();

    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "subsets": [{"addresses": [{"ip": "127.0.0.1"}], "ports": [{"port": port}]}]
        }),
    )
    .await
    .unwrap();

    let apiservice = json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": {"name": "v1alpha1.wardle.example.com"},
        "spec": {
            "group": "wardle.example.com",
            "version": "v1alpha1",
            "groupPriorityMinimum": 1000,
            "versionPriority": 10,
            "service": {"name": "wardle-service", "namespace": "default", "port": port}
        }
    });

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiregistration.k8s.io/v1/apiservices")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&apiservice).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    let discovery = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/wardle.example.com/v1alpha1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        discovery.status(),
        StatusCode::BAD_GATEWAY,
        "APIService service references must be proxied over TLS regardless of service port"
    );

    let first_byte = timeout(Duration::from_secs(2), first_byte_rx)
        .await
        .expect("proxy client should connect to backend")
        .expect("capture task should publish first byte")
        .expect("backend should receive at least one byte");
    assert_eq!(
        first_byte, 0x16,
        "expected TLS ClientHello (0x16), got first byte {first_byte:#x}"
    );
}

#[tokio::test]
async fn test_apiservice_proxy_uses_endpoint_port_when_service_port_differs() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    use serde_json::json;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;
    use tokio::time::{Duration, timeout};
    use tokio_rustls::TlsAcceptor;
    use tower::ServiceExt;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint_port = listener.local_addr().unwrap().port();
    let service_port: u16 = 7443;
    let (host_tx, host_rx) = oneshot::channel::<String>();
    let cert =
        rcgen::generate_simple_self_signed(vec!["wardle-service.default.svc".to_string()]).unwrap();
    let cert_der = CertificateDer::from(cert.cert.der().to_vec());
    let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der()));
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key_der)
        .unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(server_config));

    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut stream = acceptor.accept(stream).await.unwrap();
        let mut buf = vec![0u8; 4096];
        let n = stream.read(&mut buf).await.unwrap();
        let req = String::from_utf8_lossy(&buf[..n]);
        let host_header = req
            .lines()
            .find(|line| line.to_ascii_lowercase().starts_with("host:"))
            .map(str::to_string)
            .unwrap_or_else(|| "missing".to_string());
        let _ = host_tx.send(host_header);

        let body =
            r#"{"kind":"FlunderList","apiVersion":"wardle.example.com/v1alpha1","items":[]}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        stream.write_all(response.as_bytes()).await.unwrap();
    });

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "spec": {
                "ports": [{
                    "port": service_port,
                    "targetPort": 443
                }]
            }
        }),
    )
    .await
    .unwrap();

    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "subsets": [{"addresses": [{"ip": "127.0.0.1"}], "ports": [{"port": endpoint_port}]}]
        }),
    )
    .await
    .unwrap();

    let apiservice = json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": {"name": "v1alpha1.wardle.example.com"},
        "spec": {
            "group": "wardle.example.com",
            "version": "v1alpha1",
            "groupPriorityMinimum": 1000,
            "versionPriority": 10,
            "insecureSkipTLSVerify": true,
            "service": {"name": "wardle-service", "namespace": "default", "port": service_port}
        }
    });

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiregistration.k8s.io/v1/apiservices")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&apiservice).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    let list = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/wardle.example.com/v1alpha1/namespaces/default/flunders")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        list.status(),
        StatusCode::OK,
        "APIService proxy must connect to endpoint target port, not service port"
    );

    let host = timeout(Duration::from_secs(2), host_rx)
        .await
        .expect("backend should receive a proxied request")
        .expect("capture task should publish host header");
    assert!(
        host.to_ascii_lowercase()
            .contains(&format!("host: wardle-service.default.svc:{service_port}")),
        "Host header should preserve service DNS and service port. got: {host}"
    );
}

#[tokio::test]
async fn test_apiservice_proxy_falls_back_to_not_ready_endpoint_addresses() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    use serde_json::json;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio_rustls::TlsAcceptor;
    use tower::ServiceExt;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let cert =
        rcgen::generate_simple_self_signed(vec!["wardle-service.default.svc".to_string()]).unwrap();
    let cert_der = CertificateDer::from(cert.cert.der().to_vec());
    let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der()));
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key_der)
        .unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(server_config));

    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut stream = acceptor.accept(stream).await.unwrap();
        let mut buf = vec![0u8; 4096];
        let _ = stream.read(&mut buf).await.unwrap();

        let body =
            r#"{"kind":"FlunderList","apiVersion":"wardle.example.com/v1alpha1","items":[]}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        stream.write_all(response.as_bytes()).await.unwrap();
    });

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "spec": {"ports": [{"port": port}]}
        }),
    )
    .await
    .unwrap();

    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "subsets": [{
                "notReadyAddresses": [{"ip": "127.0.0.1"}],
                "ports": [{"port": port}]
            }]
        }),
    )
    .await
    .unwrap();

    let apiservice = json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": {"name": "v1alpha1.wardle.example.com"},
        "spec": {
            "group": "wardle.example.com",
            "version": "v1alpha1",
            "groupPriorityMinimum": 1000,
            "versionPriority": 10,
            "insecureSkipTLSVerify": true,
            "service": {"name": "wardle-service", "namespace": "default", "port": port}
        }
    });

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiregistration.k8s.io/v1/apiservices")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&apiservice).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    let proxied = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/wardle.example.com/v1alpha1/namespaces/default/flunders")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        proxied.status(),
        StatusCode::OK,
        "APIService proxy should use notReadyAddresses when ready addresses are temporarily absent"
    );
}

#[tokio::test]
async fn test_apiservice_proxy_sets_requestheader_identity_headers() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    use serde_json::json;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;
    use tokio::time::{Duration, timeout};
    use tokio_rustls::TlsAcceptor;
    use tower::ServiceExt;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (req_tx, req_rx) = oneshot::channel::<String>();
    let cert =
        rcgen::generate_simple_self_signed(vec!["wardle-service.default.svc".to_string()]).unwrap();
    let cert_der = CertificateDer::from(cert.cert.der().to_vec());
    let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der()));
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key_der)
        .unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(server_config));

    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut stream = acceptor.accept(stream).await.unwrap();
        let mut buf = vec![0u8; 4096];
        let n = stream.read(&mut buf).await.unwrap();
        let req = String::from_utf8_lossy(&buf[..n]).to_string();
        let _ = req_tx.send(req);

        let body =
            r#"{"kind":"FlunderList","apiVersion":"wardle.example.com/v1alpha1","items":[]}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        stream.write_all(response.as_bytes()).await.unwrap();
    });

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "spec": {"ports": [{"port": port}]}
        }),
    )
    .await
    .unwrap();

    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "wardle-service",
        json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "subsets": [{"addresses": [{"ip": "127.0.0.1"}], "ports": [{"port": port}]}]
        }),
    )
    .await
    .unwrap();

    let apiservice = json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": {"name": "v1alpha1.wardle.example.com"},
        "spec": {
            "group": "wardle.example.com",
            "version": "v1alpha1",
            "groupPriorityMinimum": 1000,
            "versionPriority": 10,
            "insecureSkipTLSVerify": true,
            "service": {"name": "wardle-service", "namespace": "default", "port": port}
        }
    });

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiregistration.k8s.io/v1/apiservices")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&apiservice).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    let list = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/wardle.example.com/v1alpha1/namespaces/default/flunders")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list.status(), StatusCode::OK);

    let req = timeout(Duration::from_secs(2), req_rx)
        .await
        .expect("backend should receive proxied request")
        .expect("capture task should publish request");
    let req_lower = req.to_ascii_lowercase();
    // The proxy now forwards the real effective identity. Without authentication,
    // the identity is system:anonymous.
    assert!(
        req_lower.contains("\r\nx-remote-user: system:anonymous\r\n"),
        "proxy must forward requestheader user identity. got:\n{req}"
    );
    assert!(
        !req_lower.contains("x-remote-user: system:admin"),
        "proxy must not hard-code system:admin. got:\n{req}"
    );
    assert!(
        req_lower.contains("\r\nx-remote-group: system:unauthenticated\r\n"),
        "proxy must forward requestheader groups. got:\n{req}"
    );
}

/// Covers the namespace-crud chainsaw test inline: create with labels, list with
/// labelSelector, GET single, PATCH labels via merge-patch, DELETE.
#[tokio::test]
async fn test_namespace_root_crud_lifecycle_with_labels_and_label_selector() {
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = build_test_router().await;

    let create_body = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name": "ns-crud-lifecycle",
            "labels": {"test": "klights", "env": "testing"}
        }
    });
    let create_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&create_body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);
    let created: serde_json::Value =
        serde_json::from_slice(&to_bytes(create_resp.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    assert_eq!(created["metadata"]["labels"]["test"], "klights");
    assert_eq!(created["status"]["phase"], "Active");

    let get_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/namespaces/ns-crud-lifecycle")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get_resp.status(), StatusCode::OK);
    let fetched: serde_json::Value =
        serde_json::from_slice(&to_bytes(get_resp.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(fetched["metadata"]["name"], "ns-crud-lifecycle");
    assert_eq!(fetched["metadata"]["labels"]["env"], "testing");

    let list_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/namespaces?labelSelector=test%3Dklights")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list_resp.status(), StatusCode::OK);
    let listed: serde_json::Value =
        serde_json::from_slice(&to_bytes(list_resp.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    let names: Vec<&str> = listed["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n["metadata"]["name"].as_str().unwrap())
        .collect();
    assert!(
        names.contains(&"ns-crud-lifecycle"),
        "label selector list missed our namespace; got {names:?}"
    );

    let patch_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/api/v1/namespaces/ns-crud-lifecycle")
                .header("content-type", "application/merge-patch+json")
                .body(Body::from(
                    r#"{"metadata":{"labels":{"env":"production","updated":"true"}}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(patch_resp.status(), StatusCode::OK);
    let patched: serde_json::Value =
        serde_json::from_slice(&to_bytes(patch_resp.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    assert_eq!(patched["metadata"]["labels"]["env"], "production");
    assert_eq!(patched["metadata"]["labels"]["updated"], "true");
    assert_eq!(
        patched["metadata"]["labels"]["test"], "klights",
        "merge-patch must preserve unrelated existing labels"
    );

    let delete_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/v1/namespaces/ns-crud-lifecycle")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let deleted_status = delete_resp.status();
    assert!(
        deleted_status == StatusCode::OK || deleted_status == StatusCode::ACCEPTED,
        "DELETE namespace must return 200 or 202 (got {deleted_status})"
    );
}

/// Regression: namespace DELETE and GC cleanup must NOT block an HTTP request
/// on picked-up Pod row removal (HR #11). Once a Pod has `spec.nodeName` set,
/// only the pod lifecycle actor may remove its datastore row; namespace/GC
/// paths must mark/queue UID-bound actor cleanup and return promptly. The
/// namespace final deletion is then re-driven event-style from the
/// actor-owned Pod row removal — no synchronous wait, no production polling.
#[tokio::test]
async fn namespace_delete_returns_while_picked_up_pods_wait_for_actor_finalization() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let pod_repository = state.pod_repository.clone();
    let app = crate::api::build_router(state);

    // Create the namespace via the HTTP path so all namespace-finalizer wiring
    // (default finalizers, status Active) matches production.
    let create_ns_req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"gc-cleanup"}}"#,
        ))
        .unwrap();
    let create_ns_resp = app.clone().oneshot(create_ns_req).await.unwrap();
    assert_eq!(create_ns_resp.status(), StatusCode::CREATED);

    // Create 12 PICKED-UP pods (spec.nodeName set) — actor-owned delete path
    // applies. The lossy run that motivated this task hung here.
    let mut pod_uids: Vec<(String, String)> = Vec::with_capacity(12);
    for i in 0..12 {
        let name = format!("simpletest-rc-{i}");
        let uid = format!("uid-{i}");
        let pod = db
            .create_resource(
                "v1",
                "Pod",
                Some("gc-cleanup"),
                &name,
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "name": name,
                        "namespace": "gc-cleanup",
                        "uid": uid,
                    },
                    "spec": {
                        "nodeName": "worker-a",
                        "containers": [{"name": "app", "image": "busybox"}]
                    },
                    "status": {"phase": "Running"}
                }),
            )
            .await
            .unwrap();
        pod_uids.push((pod.name.clone(), pod.uid.clone()));
    }

    // The DELETE must return within 2s — it must NOT block on actor-owned Pod
    // finalization. OK (sync delete, no finalizers/content) or ACCEPTED (async)
    // are both acceptable per the K8s contract.
    let delete = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        app.clone().oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/v1/namespaces/gc-cleanup")
                .body(Body::empty())
                .unwrap(),
        ),
    )
    .await
    .expect("namespace DELETE must not block on actor-owned Pod finalization")
    .unwrap();
    assert!(
        matches!(delete.status(), StatusCode::OK | StatusCode::ACCEPTED),
        "unexpected delete status: {}",
        delete.status()
    );

    // The namespace must remain Terminating — picked-up Pods are not yet
    // finalized, so the actor-owned delete invariant forbids removing the
    // namespace content synchronously.
    let ns = db
        .get_namespace("gc-cleanup")
        .await
        .unwrap()
        .expect("namespace should remain terminating until pods finalize");
    assert_eq!(
        ns.data.pointer("/status/phase"),
        Some(&json!("Terminating")),
        "namespace must be in Terminating phase while picked-up Pods await actor finalization"
    );

    // No picked-up Pod row may have been hard-deleted by the namespace/GC path
    // (HR #11): every Pod must still be present, now marked terminating.
    for (name, _uid) in &pod_uids {
        let pod = db
            .get_resource("v1", "Pod", Some("gc-cleanup"), name)
            .await
            .unwrap()
            .unwrap_or_else(|| {
                panic!(
                    "picked-up Pod {name} must not be hard-deleted by namespace/GC cleanup (HR #11)"
                )
            });
        assert!(
            pod.data
                .pointer("/metadata/deletionTimestamp")
                .and_then(|v| v.as_str())
                .is_some(),
            "picked-up Pod {name} must be marked terminating, not hard-deleted (HR #11)"
        );
    }

    // Drive each picked-up Pod through the actor-owned finalization seam. This
    // is the ONLY production path allowed to remove a picked-up Pod row.
    for (name, uid) in &pod_uids {
        assert!(
            pod_repository
                .finalize_pod_deletion_after_actor_cleanup("gc-cleanup", name, uid)
                .await
                .unwrap(),
            "actor-owned finalization should remove picked-up Pod {name} by UID"
        );
    }

    // Namespace finalization must be re-drivable event-style from the
    // actor-owned Pod row removal — no production polling.
    crate::controllers::namespace::reconcile_namespace_for_test(db.as_ref(), "gc-cleanup")
        .await
        .unwrap();
    assert!(
        db.get_namespace("gc-cleanup").await.unwrap().is_none(),
        "namespace must finalize once actor-owned Pod rows are removed"
    );
}
