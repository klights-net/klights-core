use super::*;

#[tokio::test]
async fn test_cluster_endpoints_protobuf_list_resource_version_primes_watch() {
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use futures::StreamExt;
    use serde_json::json;
    use std::time::Duration;
    use tower::ServiceExt;

    let app = build_test_router().await;
    let namespace = "endpoint-list-watch-rv";

    let ns_body = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {"name": namespace}
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

    let list_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/endpoints?labelSelector=test-endpoint-static%3Dtrue")
                .header(
                    "accept",
                    "application/vnd.kubernetes.protobuf,application/json",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list_resp.status(), StatusCode::OK);
    assert!(
        list_resp
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.contains("application/vnd.kubernetes.protobuf")),
        "kubectl-compatible list requests should be served as Kubernetes protobuf"
    );
    let list_body = to_bytes(list_resp.into_body(), usize::MAX).await.unwrap();
    let list_json = crate::protobuf::decode_protobuf(&list_body).unwrap();
    let start_rv = list_json
        .pointer("/metadata/resourceVersion")
        .and_then(|value| value.as_str())
        .expect(
            "cluster-wide EndpointsList protobuf response must include metadata.resourceVersion",
        );
    assert!(
        start_rv.parse::<i64>().is_ok_and(|rv| rv > 0),
        "cluster-wide EndpointsList protobuf resourceVersion must be a non-empty numeric string, got {start_rv:?}"
    );

    let endpoints = json!({
        "apiVersion": "v1",
        "kind": "Endpoints",
        "metadata": {
            "name": "testservice",
            "namespace": namespace,
            "labels": {"test-endpoint-static": "true"}
        },
        "subsets": [{
            "addresses": [{"ip": "10.180.0.1"}],
            "ports": [{"port": 80, "protocol": "TCP"}]
        }]
    });
    let create_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/namespaces/{namespace}/endpoints"))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&endpoints).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);

    let watch_uri = format!(
        "/api/v1/namespaces/{namespace}/endpoints?watch=true&resourceVersion={start_rv}&labelSelector=test-endpoint-static%3Dtrue"
    );
    let watch_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(watch_uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(watch_resp.status(), StatusCode::OK);

    let mut stream = watch_resp.into_body().into_data_stream();
    let chunk = tokio::time::timeout(Duration::from_secs(1), stream.next())
        .await
        .expect("watch should replay the Endpoint created after the list RV")
        .expect("watch stream should yield a chunk")
        .expect("watch chunk should be ok");
    let event: serde_json::Value = serde_json::from_slice(&chunk).unwrap();
    assert_eq!(event["type"], "ADDED");
    assert_eq!(
        event
            .pointer("/object/metadata/name")
            .and_then(|value| value.as_str()),
        Some("testservice")
    );
}

#[tokio::test]
async fn test_cluster_watch_timeout_seconds_closes_stream() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use futures::StreamExt;
    use std::time::Duration;
    use tower::ServiceExt;

    let app = build_test_router().await;

    let watch_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/services?watch=true&timeoutSeconds=1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(watch_resp.status(), StatusCode::OK);

    let mut stream = watch_resp.into_body().into_data_stream();
    let item = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("watch stream must close after timeoutSeconds");

    assert!(
        item.is_none(),
        "watch stream should end after timeoutSeconds, got {item:?}"
    );
}

#[tokio::test]
async fn test_cluster_pod_watch_send_initial_events_emits_initial_events_end_bookmark() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use futures::StreamExt;
    use serde_json::json;
    use std::time::Duration;
    use tower::ServiceExt;

    let app = build_test_router().await;
    let namespace = "cluster-pod-watch-initial-bookmark";

    let ns_body = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {"name": namespace}
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

    let pod_body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "cluster-pod-watch-initial", "namespace": namespace},
        "spec": {
            "containers": [{
                "name": "pause",
                "image": "registry.k8s.io/pause:3.10.1"
            }]
        }
    });
    let pod_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/namespaces/{namespace}/pods"))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&pod_body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(pod_resp.status(), StatusCode::CREATED);

    let watch_resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/pods?watch=true&sendInitialEvents=true&resourceVersionMatch=NotOlderThan&allowWatchBookmarks=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(watch_resp.status(), StatusCode::OK);

    let mut stream = watch_resp.into_body().into_data_stream();
    let mut saw_pod = false;
    let mut saw_initial_events_end = false;

    for _ in 0..8 {
        let chunk = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("cluster pod watch must emit initial-events-end bookmark")
            .expect("watch stream ended before initial-events-end bookmark")
            .expect("watch stream chunk error");
        let text = String::from_utf8(chunk.to_vec()).unwrap();
        for line in text.lines().filter(|line| !line.trim().is_empty()) {
            let event: serde_json::Value = serde_json::from_str(line).unwrap();
            if event["type"] == "ADDED"
                && event["object"]["metadata"]["namespace"] == namespace
                && event["object"]["metadata"]["name"] == "cluster-pod-watch-initial"
            {
                saw_pod = true;
            }
            if event["type"] == "BOOKMARK"
                && event["object"]["metadata"]["annotations"]["k8s.io/initial-events-end"] == "true"
            {
                saw_initial_events_end = true;
                break;
            }
        }
        if saw_initial_events_end {
            break;
        }
    }

    assert!(
        saw_pod,
        "cluster pod watch must include existing pods as initial ADDED events"
    );
    assert!(
        saw_initial_events_end,
        "cluster pod watch with sendInitialEvents=true must emit initial-events-end bookmark"
    );
}

#[tokio::test]
async fn test_pod_watchlist_send_initial_events_requires_not_older_than() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let app = crate::api::build_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/pods?watch=true&sendInitialEvents=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

/// A `sendInitialEvents=true` (WatchList) watch over an EMPTY collection
/// must still report the collection's snapshot resourceVersion in its
/// `initial-events-end` bookmark — not `0`/`""`. An informer that opens
/// WatchList with `resourceVersion=""` (the `[sig-scheduling] LimitRange`
/// conformance flow) would otherwise resume from an invalid revision. The
/// stream must also keep delivering live creates after the bookmark.
#[tokio::test]
async fn test_send_initial_events_empty_collection_bookmark_reports_snapshot_rv() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use futures::StreamExt;
    use serde_json::json;
    use std::time::Duration;
    use tower::ServiceExt;

    let app = build_test_router().await;
    let namespace = "send-initial-empty";

    let ns_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces")
                .header("content-type", "application/json")
                .body(Body::from(format!(
                    r#"{{"apiVersion":"v1","kind":"Namespace","metadata":{{"name":"{namespace}"}}}}"#
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ns_resp.status(), StatusCode::CREATED);

    // Open a WatchList-style watch with resourceVersion unset (== "") over an
    // empty configmap collection.
    let watch_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "/api/v1/namespaces/{namespace}/configmaps?watch=true&sendInitialEvents=true&resourceVersionMatch=NotOlderThan&allowWatchBookmarks=true"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(watch_resp.status(), StatusCode::OK);
    let mut stream = watch_resp.into_body().into_data_stream();

    // First event must be the initial-events-end bookmark with a non-zero rv.
    let chunk = tokio::time::timeout(Duration::from_secs(1), stream.next())
        .await
        .expect("must emit initial-events-end bookmark")
        .expect("stream ended before bookmark")
        .expect("chunk error");
    let line = chunk
        .split(|b| *b == b'\n')
        .find(|l| !l.is_empty())
        .expect("at least one line");
    let event: serde_json::Value = serde_json::from_slice(line).unwrap();
    assert_eq!(event["type"], "BOOKMARK");
    assert_eq!(
        event["object"]["metadata"]["annotations"]["k8s.io/initial-events-end"],
        "true"
    );
    let bookmark_rv = event
        .pointer("/object/metadata/resourceVersion")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<i64>().ok())
        .expect("bookmark must carry a numeric resourceVersion");
    assert!(
        bookmark_rv > 0,
        "initial-events-end bookmark over an empty collection must report the snapshot resourceVersion, got {bookmark_rv}"
    );

    // A live create after the bookmark must still be delivered as ADDED.
    let cm = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "later"}
    });
    let create_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/namespaces/{namespace}/configmaps"))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&cm).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);

    let mut saw_added = false;
    'outer: for _ in 0..16 {
        let chunk = match tokio::time::timeout(Duration::from_millis(500), stream.next()).await {
            Ok(Some(Ok(bytes))) => bytes,
            _ => break,
        };
        for l in chunk.split(|b| *b == b'\n') {
            if l.is_empty() {
                continue;
            }
            let ev: serde_json::Value = match serde_json::from_slice(l) {
                Ok(v) => v,
                Err(_) => continue,
            };
            if ev["type"] == "ADDED"
                && ev.pointer("/object/metadata/name").and_then(|v| v.as_str()) == Some("later")
            {
                saw_added = true;
                break 'outer;
            }
        }
    }
    assert!(
        saw_added,
        "live create after initial-events-end must be delivered as ADDED"
    );
}

#[tokio::test]
async fn test_namespace_watch_send_initial_events_emits_initial_events_end_bookmark() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use futures::StreamExt;
    use serde_json::json;
    use std::time::Duration;
    use tower::ServiceExt;

    let app = build_test_router().await;
    let namespace = "namespace-watch-initial-bookmark";

    let ns_body = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {"name": namespace}
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

    let watch_resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/namespaces?watch=true&sendInitialEvents=true&resourceVersionMatch=NotOlderThan&allowWatchBookmarks=true&fieldSelector=metadata.name%3Dnamespace-watch-initial-bookmark")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(watch_resp.status(), StatusCode::OK);

    let mut stream = watch_resp.into_body().into_data_stream();
    let mut saw_namespace = false;
    let mut saw_initial_events_end = false;

    for _ in 0..8 {
        let chunk = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("namespace watch must emit initial-events-end bookmark")
            .expect("watch stream ended before initial-events-end bookmark")
            .expect("watch stream chunk error");
        let text = String::from_utf8(chunk.to_vec()).unwrap();
        for line in text.lines().filter(|line| !line.trim().is_empty()) {
            let event: serde_json::Value = serde_json::from_str(line).unwrap();
            if event["type"] == "ADDED" && event["object"]["metadata"]["name"] == namespace {
                saw_namespace = true;
            }
            if event["type"] == "BOOKMARK"
                && event["object"]["metadata"]["annotations"]["k8s.io/initial-events-end"] == "true"
            {
                saw_initial_events_end = true;
                break;
            }
        }
        if saw_initial_events_end {
            break;
        }
    }

    assert!(
        saw_namespace,
        "namespace watch must include the selected namespace as an initial ADDED event"
    );
    assert!(
        saw_initial_events_end,
        "namespace watch with sendInitialEvents=true must emit initial-events-end bookmark"
    );
}

#[tokio::test]
async fn test_cluster_service_watch_send_initial_events_emits_initial_events_end_bookmark() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use futures::StreamExt;
    use serde_json::json;
    use std::time::Duration;
    use tower::ServiceExt;

    let app = build_test_router().await;
    let namespace = "cluster-service-watch-initial-bookmark";

    let ns_body = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {"name": namespace}
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

    let service_body = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "cluster-service-watch-initial", "namespace": namespace},
        "spec": {"ports": [{"port": 80, "targetPort": 80}]}
    });
    let service_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/namespaces/{namespace}/services"))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&service_body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(service_resp.status(), StatusCode::CREATED);

    let watch_resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/services?watch=true&sendInitialEvents=true&resourceVersionMatch=NotOlderThan&allowWatchBookmarks=true&fieldSelector=metadata.name%3Dcluster-service-watch-initial")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(watch_resp.status(), StatusCode::OK);

    let mut stream = watch_resp.into_body().into_data_stream();
    let mut saw_service = false;
    let mut saw_initial_events_end = false;

    for _ in 0..8 {
        let chunk = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("cluster service watch must emit initial-events-end bookmark")
            .expect("watch stream ended before initial-events-end bookmark")
            .expect("watch stream chunk error");
        let text = String::from_utf8(chunk.to_vec()).unwrap();
        for line in text.lines().filter(|line| !line.trim().is_empty()) {
            let event: serde_json::Value = serde_json::from_str(line).unwrap();
            if event["type"] == "ADDED"
                && event["object"]["metadata"]["namespace"] == namespace
                && event["object"]["metadata"]["name"] == "cluster-service-watch-initial"
            {
                saw_service = true;
            }
            if event["type"] == "BOOKMARK"
                && event["object"]["metadata"]["annotations"]["k8s.io/initial-events-end"] == "true"
            {
                saw_initial_events_end = true;
                break;
            }
        }
        if saw_initial_events_end {
            break;
        }
    }

    assert!(
        saw_service,
        "cluster service watch must include the selected service as an initial ADDED event"
    );
    assert!(
        saw_initial_events_end,
        "cluster service watch with sendInitialEvents=true must emit initial-events-end bookmark"
    );
}

#[tokio::test]
async fn test_namespace_watch_from_resource_version_observes_created_namespace() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use futures::StreamExt;
    use serde_json::json;
    use std::time::Duration;
    use tower::ServiceExt;

    let app = build_test_router().await;

    let seed_ns = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {"name": "watch-seed-namespace"}
    });
    let seed_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&seed_ns).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(seed_resp.status(), StatusCode::CREATED);

    let list_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/namespaces")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list_resp.status(), StatusCode::OK);
    let list_body = axum::body::to_bytes(list_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let list: serde_json::Value = serde_json::from_slice(&list_body).unwrap();
    let start_rv = list
        .pointer("/metadata/resourceVersion")
        .and_then(|v| v.as_str())
        .expect("namespace list must include resourceVersion");

    let watch_uri = format!("/api/v1/namespaces?watch=true&resourceVersion={start_rv}");
    let watch_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(watch_uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(watch_resp.status(), StatusCode::OK);

    let ns_name = "watch-created-namespace";
    let ns_body = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {"name": ns_name}
    });
    let create_resp = app
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
    assert_eq!(create_resp.status(), StatusCode::CREATED);

    let mut stream = watch_resp.into_body().into_data_stream();
    let chunk = tokio::time::timeout(Duration::from_secs(1), stream.next())
        .await
        .expect("namespace watch should observe a namespace created after the watch starts")
        .expect("watch stream should yield a chunk")
        .expect("watch chunk should be ok");
    let event: serde_json::Value = serde_json::from_slice(&chunk).unwrap();

    assert_eq!(event["type"], "ADDED");
    assert_eq!(
        event
            .pointer("/object/metadata/name")
            .and_then(|v| v.as_str()),
        Some(ns_name)
    );
}

#[tokio::test]
async fn test_serviceaccount_label_selector_watch_replays_existing_match_as_added() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use futures::StreamExt;
    use serde_json::json;
    use std::time::Duration;
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
                    r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"svcaccounts"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ns_resp.status(), StatusCode::CREATED);

    let sa_body = json!({
        "apiVersion": "v1",
        "kind": "ServiceAccount",
        "metadata": {
            "name": "testserviceaccount",
            "labels": {"test-serviceaccount-static": "true"}
        }
    });
    let create_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces/svcaccounts/serviceaccounts")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&sa_body).unwrap()))
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
                .uri("/api/v1/namespaces/svcaccounts/serviceaccounts?watch=true&labelSelector=test-serviceaccount-static%3Dtrue")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(watch_resp.status(), StatusCode::OK);

    let mut stream = watch_resp.into_body().into_data_stream();
    let chunk = tokio::time::timeout(Duration::from_secs(1), stream.next())
        .await
        .expect("label-selector watch should replay the existing matching ServiceAccount")
        .expect("watch stream should yield a chunk")
        .expect("watch chunk should be ok");
    let event: serde_json::Value = serde_json::from_slice(&chunk).unwrap();

    assert_eq!(event["type"], "ADDED");
    assert_eq!(
        event
            .pointer("/object/metadata/name")
            .and_then(|v| v.as_str()),
        Some("testserviceaccount")
    );
}

/// Reproduces the `[sig-auth] ServiceAccounts should run through the
/// lifecycle of a ServiceAccount [Conformance]` flake: open an rv-less
/// label-selector watch on an existing SA, read its baseline ADDED, then
/// strategic-merge patch the SA and expect a MODIFIED (not a rewritten
/// ADDED, not a synthetic DELETED, not a dropped event).
#[tokio::test]
async fn test_serviceaccount_label_selector_watch_delivers_modified_after_patch() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use futures::StreamExt;
    use serde_json::json;
    use std::time::Duration;
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
                    r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"svcaccounts-lc"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ns_resp.status(), StatusCode::CREATED);

    let sa_body = json!({
        "apiVersion": "v1",
        "kind": "ServiceAccount",
        "metadata": {
            "name": "testserviceaccount",
            "labels": {"test-serviceaccount-static": "true"}
        }
    });
    let create_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces/svcaccounts-lc/serviceaccounts")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&sa_body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);

    // rv-less label-selector watch, exactly as the conformance client opens it.
    let watch_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/namespaces/svcaccounts-lc/serviceaccounts?watch=true&labelSelector=test-serviceaccount-static%3Dtrue")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(watch_resp.status(), StatusCode::OK);
    let mut stream = watch_resp.into_body().into_data_stream();

    // Baseline ADDED for the existing SA.
    let chunk = tokio::time::timeout(Duration::from_secs(1), stream.next())
        .await
        .expect("baseline ADDED must arrive")
        .expect("stream yields a chunk")
        .expect("chunk ok");
    let event: serde_json::Value = serde_json::from_slice(&chunk).unwrap();
    assert_eq!(event["type"], "ADDED");
    assert_eq!(
        event
            .pointer("/object/metadata/name")
            .and_then(|v| v.as_str()),
        Some("testserviceaccount")
    );

    // Strategic-merge patch: {"automountServiceAccountToken":false} — exactly
    // what the conformance client sends (json.Marshal of a SA with only that
    // field also emits {"metadata":{"creationTimestamp":null}}).
    let patch_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/api/v1/namespaces/svcaccounts-lc/serviceaccounts/testserviceaccount")
                .header("content-type", "application/strategic-merge-patch+json")
                .body(Body::from(
                    r#"{"metadata":{"creationTimestamp":null},"automountServiceAccountToken":false}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(patch_resp.status(), StatusCode::OK, "patch must succeed");

    // The next event for this SA must be a MODIFIED.
    let mut seen_types: Vec<String> = Vec::new();
    let mut found_modified = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while tokio::time::Instant::now() < deadline {
        let Ok(Some(Ok(chunk))) =
            tokio::time::timeout(Duration::from_millis(500), stream.next()).await
        else {
            break;
        };
        for line in chunk.split(|b| *b == b'\n') {
            if line.is_empty() {
                continue;
            }
            let Ok(event) = serde_json::from_slice::<serde_json::Value>(line) else {
                continue;
            };
            let ty = event["type"].as_str().unwrap_or("").to_string();
            let name = event
                .pointer("/object/metadata/name")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if name != "testserviceaccount" {
                continue;
            }
            seen_types.push(ty.clone());
            if ty == "MODIFIED" {
                found_modified = true;
                break;
            }
        }
        if found_modified {
            break;
        }
    }
    assert!(
        found_modified,
        "expected MODIFIED after strategic-merge patch; saw {seen_types:?}"
    );
}

/// Multinode reproduction of the ServiceAccount-lifecycle MODIFIED flake.
/// The watch is served by a node that receives the create and the patch via
/// the *replicated* apply path (`apply_log_apply_commit`) — i.e. a raft
/// follower/replica applying committed entries — rather than the local API
/// create/update handlers. This is the path that runs on the node serving
/// the watch in the 6-node harness.
#[tokio::test]
async fn test_serviceaccount_watch_modified_via_replicated_apply() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use futures::StreamExt;
    use serde_json::json;
    use std::sync::Arc;
    use std::time::Duration;
    use tower::ServiceExt;

    let (app, db) = build_test_router_with_db().await;

    // Namespace exists (replicated in too, but API create is fine for the ns).
    let ns_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"svcaccounts-repl"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ns_resp.status(), StatusCode::CREATED);

    let base_rv = db.get_current_resource_version().await.unwrap();
    let create_rv = base_rv + 10;

    // Follower applies the committed CREATE for the SA (with labels).
    let sa_resource = crate::datastore::Resource {
        id: 0,
        api_version: "v1".into(),
        kind: "ServiceAccount".into(),
        namespace: Some("svcaccounts-repl".into()),
        name: "testserviceaccount".into(),
        uid: "sa-uid-1".into(),
        resource_version: create_rv,
        data: Arc::new(json!({
            "apiVersion": "v1",
            "kind": "ServiceAccount",
            "metadata": {
                "name": "testserviceaccount",
                "namespace": "svcaccounts-repl",
                "uid": "sa-uid-1",
                "labels": {"test-serviceaccount-static": "true"}
            }
        })),
    };
    db.apply_log_apply_commit(crate::log_apply::LogApplyCommit::put_resource(&sa_resource))
        .await
        .expect("replicated create apply");

    // rv-less label-selector watch, exactly as the conformance client opens it.
    let watch_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/namespaces/svcaccounts-repl/serviceaccounts?watch=true&labelSelector=test-serviceaccount-static%3Dtrue")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(watch_resp.status(), StatusCode::OK);
    let mut stream = watch_resp.into_body().into_data_stream();

    let chunk = tokio::time::timeout(Duration::from_secs(1), stream.next())
        .await
        .expect("baseline ADDED must arrive")
        .expect("stream yields a chunk")
        .expect("chunk ok");
    let event: serde_json::Value = serde_json::from_slice(&chunk).unwrap();
    assert_eq!(event["type"], "ADDED");

    // Follower applies the committed PATCH result (full updated object, as the
    // leader read-modify-writes a strategic-merge patch and replicates it).
    let patch_rv = create_rv + 10;
    let patched = crate::datastore::Resource {
        resource_version: patch_rv,
        data: Arc::new(json!({
            "apiVersion": "v1",
            "kind": "ServiceAccount",
            "metadata": {
                "name": "testserviceaccount",
                "namespace": "svcaccounts-repl",
                "uid": "sa-uid-1",
                "labels": {"test-serviceaccount-static": "true"}
            },
            "automountServiceAccountToken": false
        })),
        ..sa_resource.clone()
    };
    db.apply_log_apply_commit(crate::log_apply::LogApplyCommit::put_resource(&patched))
        .await
        .expect("replicated patch apply");

    let mut seen_types: Vec<String> = Vec::new();
    let mut found_modified = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while tokio::time::Instant::now() < deadline {
        let Ok(Some(Ok(chunk))) =
            tokio::time::timeout(Duration::from_millis(500), stream.next()).await
        else {
            break;
        };
        for line in chunk.split(|b| *b == b'\n') {
            if line.is_empty() {
                continue;
            }
            let Ok(event) = serde_json::from_slice::<serde_json::Value>(line) else {
                continue;
            };
            let name = event
                .pointer("/object/metadata/name")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if name != "testserviceaccount" {
                continue;
            }
            let ty = event["type"].as_str().unwrap_or("").to_string();
            seen_types.push(ty.clone());
            if ty == "MODIFIED" {
                found_modified = true;
                break;
            }
        }
        if found_modified {
            break;
        }
    }
    assert!(
        found_modified,
        "expected MODIFIED after replicated patch apply; saw {seen_types:?}"
    );
}

/// Same as above but the patch is applied as a `PatchResourceLatest`
/// merge-patch mutation (the per-node merge path) rather than a full
/// put_resource. Exercises the strategic/merge-patch broadcast event shape.
#[tokio::test]
async fn test_serviceaccount_watch_modified_via_replicated_patch_latest() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use futures::StreamExt;
    use serde_json::json;
    use std::sync::Arc;
    use std::time::Duration;
    use tower::ServiceExt;

    let (app, db) = build_test_router_with_db().await;

    let ns_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"svcaccounts-patch"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ns_resp.status(), StatusCode::CREATED);

    let base_rv = db.get_current_resource_version().await.unwrap();
    let create_rv = base_rv + 10;
    let sa_resource = crate::datastore::Resource {
        id: 0,
        api_version: "v1".into(),
        kind: "ServiceAccount".into(),
        namespace: Some("svcaccounts-patch".into()),
        name: "testserviceaccount".into(),
        uid: "sa-uid-2".into(),
        resource_version: create_rv,
        data: Arc::new(json!({
            "apiVersion": "v1",
            "kind": "ServiceAccount",
            "metadata": {
                "name": "testserviceaccount",
                "namespace": "svcaccounts-patch",
                "uid": "sa-uid-2",
                "labels": {"test-serviceaccount-static": "true"}
            }
        })),
    };
    db.apply_log_apply_commit(crate::log_apply::LogApplyCommit::put_resource(&sa_resource))
        .await
        .expect("replicated create apply");

    let watch_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/namespaces/svcaccounts-patch/serviceaccounts?watch=true&labelSelector=test-serviceaccount-static%3Dtrue")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(watch_resp.status(), StatusCode::OK);
    let mut stream = watch_resp.into_body().into_data_stream();
    let chunk = tokio::time::timeout(Duration::from_secs(1), stream.next())
        .await
        .expect("baseline ADDED must arrive")
        .expect("stream yields a chunk")
        .expect("chunk ok");
    let event: serde_json::Value = serde_json::from_slice(&chunk).unwrap();
    assert_eq!(event["type"], "ADDED");

    // Apply the patch as a PatchResourceLatest merge mutation, exactly the
    // conformance strategic-merge body.
    let patch_rv = create_rv + 10;
    let patch_mutation = crate::log_apply::LogApplyMutation::PatchResourceLatest(
        crate::log_apply::LogApplyResourcePatch {
            api_version: "v1".into(),
            kind: "ServiceAccount".into(),
            namespace: Some("svcaccounts-patch".into()),
            name: "testserviceaccount".into(),
            resource_version: patch_rv,
            patch_kind: crate::datastore::PatchKind::Merge,
            patch: json!({"metadata":{"creationTimestamp":null},"automountServiceAccountToken":false}),
            require_existing: true,
            precondition_uid: None,
            precondition_resource_version: None,
            terminating_pod_unready_timestamp: None,
        },
    );
    db.apply_log_apply_commit(crate::log_apply::LogApplyCommit::new(
        patch_rv,
        vec![patch_mutation],
    ))
    .await
    .expect("replicated patch-latest apply");

    let mut seen_types: Vec<String> = Vec::new();
    let mut found_modified = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while tokio::time::Instant::now() < deadline {
        let Ok(Some(Ok(chunk))) =
            tokio::time::timeout(Duration::from_millis(500), stream.next()).await
        else {
            break;
        };
        for line in chunk.split(|b| *b == b'\n') {
            if line.is_empty() {
                continue;
            }
            let Ok(event) = serde_json::from_slice::<serde_json::Value>(line) else {
                continue;
            };
            if event
                .pointer("/object/metadata/name")
                .and_then(|v| v.as_str())
                != Some("testserviceaccount")
            {
                continue;
            }
            let ty = event["type"].as_str().unwrap_or("").to_string();
            seen_types.push(ty.clone());
            if ty == "MODIFIED" {
                found_modified = true;
                break;
            }
        }
        if found_modified {
            break;
        }
    }
    assert!(
        found_modified,
        "expected MODIFIED after replicated patch-latest apply; saw {seen_types:?}"
    );
}

#[tokio::test]
async fn test_cluster_scoped_selector_watch_from_list_rv_delivers_modified_without_pre_poll() {
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use futures::StreamExt;
    use prost::Message;
    use serde_json::json;
    use std::time::Duration;
    use tower::ServiceExt;

    fn go_style_vapb_protobuf_body(name: &str, label_key: &str, label_value: &str) -> Vec<u8> {
        let binding = k8s_pb::api::admissionregistration::v1::ValidatingAdmissionPolicyBinding {
            metadata: Some(k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta {
                name: Some(name.to_string()),
                generate_name: Some(String::new()),
                namespace: Some(String::new()),
                labels: vec![(label_key.to_string(), label_value.to_string())]
                    .into_iter()
                    .collect(),
                ..Default::default()
            }),
            spec: Some(
                k8s_pb::api::admissionregistration::v1::ValidatingAdmissionPolicyBindingSpec {
                    policy_name: Some("missing-policy.example.com".to_string()),
                    validation_actions: vec!["Deny".to_string()],
                    ..Default::default()
                },
            ),
        };
        let mut raw = Vec::new();
        binding.encode(&mut raw).unwrap();
        let envelope = crate::protobuf::Unknown {
            type_meta: Some(crate::protobuf::TypeMeta {
                api_version: "admissionregistration.k8s.io/v1".to_string(),
                kind: "ValidatingAdmissionPolicyBinding".to_string(),
            }),
            raw,
            content_encoding: String::new(),
            content_type: String::new(),
        };
        let mut body = Vec::new();
        body.extend_from_slice(b"k8s\0");
        envelope.encode(&mut body).unwrap();
        body
    }

    let app = build_test_router().await;
    let label_key = "example-e2e-vapb-label";
    let label_value = "lazy-watch";
    let target_name = "e2e-example-vapb-lazy-target";

    for name in [
        "e2e-example-vapb-lazy-a",
        "e2e-example-vapb-lazy-b",
        target_name,
    ] {
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicybindings")
                    .header("content-type", "application/vnd.kubernetes.protobuf")
                    .header(
                        "accept",
                        "application/vnd.kubernetes.protobuf,application/json",
                    )
                    .body(Body::from(go_style_vapb_protobuf_body(
                        name,
                        label_key,
                        label_value,
                    )))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            status,
            StatusCode::CREATED,
            "create {name} failed: {}",
            String::from_utf8_lossy(&body)
        );
    }

    let list_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicybindings?labelSelector={label_key}%3D{label_value}"
                ))
                .header(
                    "accept",
                    "application/vnd.kubernetes.protobuf,application/json",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list_resp.status(), StatusCode::OK);
    let list_body = to_bytes(list_resp.into_body(), usize::MAX).await.unwrap();
    let list = crate::protobuf::decode_protobuf(&list_body).unwrap();
    let list_rv = list
        .pointer("/metadata/resourceVersion")
        .and_then(|value| value.as_str())
        .expect("list response must include a resourceVersion");

    let watch_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicybindings?labelSelector={label_key}%3D{label_value}&resourceVersion={list_rv}&watch=true"
                ))
                .header(
                    "accept",
                    "application/vnd.kubernetes.protobuf,application/json",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(watch_resp.status(), StatusCode::OK);

    let patch_body = json!({
        "metadata": {"annotations": {"patched": "true"}},
        "spec": {"validationActions": ["Warn"]}
    });
    let patch_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!(
                    "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicybindings/{target_name}"
                ))
                .header("content-type", "application/merge-patch+json")
                .body(Body::from(serde_json::to_vec(&patch_body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(patch_resp.status(), StatusCode::OK);

    let get_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicybindings/{target_name}"
                ))
                .header(
                    "accept",
                    "application/vnd.kubernetes.protobuf,application/json",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get_resp.status(), StatusCode::OK);
    let get_body = to_bytes(get_resp.into_body(), usize::MAX).await.unwrap();
    let mut updated = crate::protobuf::decode_protobuf(&get_body).unwrap();
    updated["metadata"]["annotations"]["updated"] = json!("true");
    updated["spec"]["validationActions"] = json!(["Deny"]);

    let update_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!(
                    "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicybindings/{target_name}"
                ))
                .header("content-type", "application/vnd.kubernetes.protobuf")
                .header(
                    "accept",
                    "application/vnd.kubernetes.protobuf,application/json",
                )
                .body(Body::from(
                    crate::protobuf::encode_protobuf(&updated).unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(update_resp.status(), StatusCode::OK);

    let mut stream = watch_resp.into_body().into_data_stream();
    let mut seen = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while tokio::time::Instant::now() < deadline {
        let Ok(Some(Ok(chunk))) =
            tokio::time::timeout(Duration::from_millis(500), stream.next()).await
        else {
            break;
        };
        for line in chunk.split(|byte| *byte == b'\n') {
            if line.is_empty() {
                continue;
            }
            let event: serde_json::Value = serde_json::from_slice(line).unwrap();
            if event
                .pointer("/object/metadata/name")
                .and_then(|value| value.as_str())
                != Some(target_name)
            {
                continue;
            }
            let ty = event["type"].as_str().unwrap_or("").to_string();
            let patched = event
                .pointer("/object/metadata/annotations/patched")
                .and_then(|value| value.as_str())
                .unwrap_or("")
                .to_string();
            seen.push((ty, patched));
        }
        if seen
            .iter()
            .any(|(ty, patched)| ty == "MODIFIED" && patched == "true")
        {
            break;
        }
    }
    assert!(
        seen.iter()
            .any(|(ty, patched)| ty == "MODIFIED" && patched == "true"),
        "expected patched MODIFIED event after lazy watch read; saw {seen:?}"
    );
}

/// Run-2 reproduction: under the cluster-wide `v1/ServiceAccount` event
/// flood the watch's broadcast receiver overflows (capacity 1024) and lags;
/// the cursor must replay and still deliver the patch's MODIFIED. The watch
/// is suspended (not polled) while >1024 same-topic events are committed,
/// then the patch lands, then we resume reading.
#[tokio::test]
async fn test_serviceaccount_watch_modified_survives_broadcast_lag() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use futures::StreamExt;
    use serde_json::json;
    use std::sync::Arc;
    use std::time::Duration;
    use tower::ServiceExt;

    let (app, db) = build_test_router_with_db().await;

    let ns_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"svcaccounts-lag"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ns_resp.status(), StatusCode::CREATED);

    let mut rv = db.get_current_resource_version().await.unwrap() + 10;
    let create_rv = rv;
    let sa_resource = crate::datastore::Resource {
        id: 0,
        api_version: "v1".into(),
        kind: "ServiceAccount".into(),
        namespace: Some("svcaccounts-lag".into()),
        name: "testserviceaccount".into(),
        uid: "sa-uid-3".into(),
        resource_version: create_rv,
        data: Arc::new(json!({
            "apiVersion": "v1",
            "kind": "ServiceAccount",
            "metadata": {
                "name": "testserviceaccount",
                "namespace": "svcaccounts-lag",
                "uid": "sa-uid-3",
                "labels": {"test-serviceaccount-static": "true"}
            }
        })),
    };
    db.apply_log_apply_commit(crate::log_apply::LogApplyCommit::put_resource(&sa_resource))
        .await
        .expect("replicated create apply");

    let watch_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/namespaces/svcaccounts-lag/serviceaccounts?watch=true&labelSelector=test-serviceaccount-static%3Dtrue")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(watch_resp.status(), StatusCode::OK);
    let mut stream = watch_resp.into_body().into_data_stream();

    // Baseline ADDED.
    let chunk = tokio::time::timeout(Duration::from_secs(1), stream.next())
        .await
        .expect("baseline ADDED")
        .expect("chunk")
        .expect("ok");
    let event: serde_json::Value = serde_json::from_slice(&chunk).unwrap();
    assert_eq!(event["type"], "ADDED");

    // Flood the v1/ServiceAccount topic with >1024 events in OTHER namespaces
    // while the watch is not being polled, overflowing its receiver buffer.
    for i in 0..1200 {
        rv += 1;
        let other = crate::datastore::Resource {
            id: 0,
            api_version: "v1".into(),
            kind: "ServiceAccount".into(),
            namespace: Some("flood-ns".into()),
            name: format!("flood-sa-{i}"),
            uid: format!("flood-uid-{i}"),
            resource_version: rv,
            data: Arc::new(json!({
                "apiVersion": "v1",
                "kind": "ServiceAccount",
                "metadata": {
                    "name": format!("flood-sa-{i}"),
                    "namespace": "flood-ns",
                    "uid": format!("flood-uid-{i}"),
                    "labels": {"other": "x"}
                }
            })),
        };
        db.apply_log_apply_commit(crate::log_apply::LogApplyCommit::put_resource(&other))
            .await
            .expect("flood apply");
    }

    // Now apply the patch for our SA (MODIFIED), still on the same topic.
    rv += 1;
    let patch_rv = rv;
    let patched = crate::datastore::Resource {
        resource_version: patch_rv,
        data: Arc::new(json!({
            "apiVersion": "v1",
            "kind": "ServiceAccount",
            "metadata": {
                "name": "testserviceaccount",
                "namespace": "svcaccounts-lag",
                "uid": "sa-uid-3",
                "labels": {"test-serviceaccount-static": "true"}
            },
            "automountServiceAccountToken": false
        })),
        ..sa_resource.clone()
    };
    db.apply_log_apply_commit(crate::log_apply::LogApplyCommit::put_resource(&patched))
        .await
        .expect("replicated patch apply");

    // Resume reading: the cursor lagged on the flood and must replay to find
    // our namespace's MODIFIED.
    let mut seen_types: Vec<String> = Vec::new();
    let mut found_modified = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        let Ok(Some(Ok(chunk))) =
            tokio::time::timeout(Duration::from_millis(500), stream.next()).await
        else {
            break;
        };
        for line in chunk.split(|b| *b == b'\n') {
            if line.is_empty() {
                continue;
            }
            let Ok(event) = serde_json::from_slice::<serde_json::Value>(line) else {
                continue;
            };
            if event
                .pointer("/object/metadata/name")
                .and_then(|v| v.as_str())
                != Some("testserviceaccount")
            {
                continue;
            }
            let ty = event["type"].as_str().unwrap_or("").to_string();
            seen_types.push(ty.clone());
            if ty == "MODIFIED" {
                found_modified = true;
                break;
            }
        }
        if found_modified {
            break;
        }
    }
    assert!(
        found_modified,
        "expected MODIFIED for our SA after broadcast lag + replay; saw {seen_types:?}"
    );
}

/// Establishment race: the watch is opened BEFORE the SA is visible (empty
/// baseline), so the key enters `seen_resources` only via the live ADDED.
/// The subsequent patch must still be delivered as MODIFIED, not rewritten.
#[tokio::test]
async fn test_serviceaccount_watch_modified_when_baseline_empty() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use futures::StreamExt;
    use serde_json::json;
    use std::sync::Arc;
    use std::time::Duration;
    use tower::ServiceExt;

    let (app, db) = build_test_router_with_db().await;
    let ns_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"svcaccounts-race"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ns_resp.status(), StatusCode::CREATED);

    // Watch opened first — baseline is empty for this selector.
    let watch_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/namespaces/svcaccounts-race/serviceaccounts?watch=true&labelSelector=test-serviceaccount-static%3Dtrue")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(watch_resp.status(), StatusCode::OK);
    let mut stream = watch_resp.into_body().into_data_stream();

    // Advance the (lazy) stream generator past the empty baseline list before
    // any writes, so the baseline is genuinely empty and the key can only
    // enter `seen_resources` via the live ADDED — the real establishment race.
    let _ = tokio::time::timeout(Duration::from_millis(200), stream.next()).await;

    let mut rv = db.get_current_resource_version().await.unwrap() + 10;
    let create_rv = rv;
    let sa_resource = crate::datastore::Resource {
        id: 0,
        api_version: "v1".into(),
        kind: "ServiceAccount".into(),
        namespace: Some("svcaccounts-race".into()),
        name: "testserviceaccount".into(),
        uid: "sa-uid-4".into(),
        resource_version: create_rv,
        data: Arc::new(json!({
            "apiVersion": "v1",
            "kind": "ServiceAccount",
            "metadata": {
                "name": "testserviceaccount",
                "namespace": "svcaccounts-race",
                "uid": "sa-uid-4",
                "labels": {"test-serviceaccount-static": "true"}
            }
        })),
    };
    db.apply_log_apply_commit(crate::log_apply::LogApplyCommit::put_resource(&sa_resource))
        .await
        .expect("create apply");

    rv += 1;
    let patched = crate::datastore::Resource {
        resource_version: rv,
        data: Arc::new(json!({
            "apiVersion": "v1",
            "kind": "ServiceAccount",
            "metadata": {
                "name": "testserviceaccount",
                "namespace": "svcaccounts-race",
                "uid": "sa-uid-4",
                "labels": {"test-serviceaccount-static": "true"}
            },
            "automountServiceAccountToken": false
        })),
        ..sa_resource.clone()
    };
    db.apply_log_apply_commit(crate::log_apply::LogApplyCommit::put_resource(&patched))
        .await
        .expect("patch apply");

    let mut seen_types: Vec<String> = Vec::new();
    let mut found_added = false;
    let mut found_modified = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while tokio::time::Instant::now() < deadline {
        let Ok(Some(Ok(chunk))) =
            tokio::time::timeout(Duration::from_millis(500), stream.next()).await
        else {
            break;
        };
        for line in chunk.split(|b| *b == b'\n') {
            if line.is_empty() {
                continue;
            }
            let Ok(event) = serde_json::from_slice::<serde_json::Value>(line) else {
                continue;
            };
            if event
                .pointer("/object/metadata/name")
                .and_then(|v| v.as_str())
                != Some("testserviceaccount")
            {
                continue;
            }
            let ty = event["type"].as_str().unwrap_or("").to_string();
            seen_types.push(ty.clone());
            match ty.as_str() {
                "ADDED" => found_added = true,
                "MODIFIED" => found_modified = true,
                _ => {}
            }
        }
        if found_modified {
            break;
        }
    }
    assert!(found_added, "expected ADDED; saw {seen_types:?}");
    assert!(
        found_modified,
        "expected MODIFIED after patch (empty-baseline race); saw {seen_types:?}"
    );
}

#[tokio::test]
async fn test_label_selector_watch_catchup_filters_persisted_events() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use futures::StreamExt;
    use serde_json::json;
    use std::time::Duration;
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
                    r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"watch-catchup"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ns_resp.status(), StatusCode::CREATED);

    let list_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/namespaces/watch-catchup/configmaps")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list_resp.status(), StatusCode::OK);
    let list_body = axum::body::to_bytes(list_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let list: serde_json::Value = serde_json::from_slice(&list_body).unwrap();
    let start_rv = list
        .pointer("/metadata/resourceVersion")
        .and_then(|v| v.as_str())
        .expect("list must include resourceVersion");

    for cm in [
        json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "backend",
                "labels": {"app": "guestbook", "tier": "backend"}
            }
        }),
        json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "frontend",
                "labels": {"app": "guestbook", "tier": "frontend"}
            }
        }),
    ] {
        let create_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/namespaces/watch-catchup/configmaps")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&cm).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(create_resp.status(), StatusCode::CREATED);
    }

    let watch_uri = format!(
        "/api/v1/namespaces/watch-catchup/configmaps?watch=true&labelSelector=app%3Dguestbook%2Ctier%3Dfrontend&resourceVersion={start_rv}"
    );
    let watch_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(watch_uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(watch_resp.status(), StatusCode::OK);

    let mut stream = watch_resp.into_body().into_data_stream();
    let chunk = tokio::time::timeout(Duration::from_secs(1), stream.next())
        .await
        .expect("watch should replay the selector-matching catch-up event")
        .expect("watch stream should yield a chunk")
        .expect("watch chunk should be ok");
    let event: serde_json::Value = serde_json::from_slice(&chunk).unwrap();

    assert_eq!(event["type"], "ADDED");
    assert_eq!(
        event
            .pointer("/object/metadata/name")
            .and_then(|v| v.as_str()),
        Some("frontend")
    );
}

/// Reproduces the `[sig-scheduling] LimitRange should create a LimitRange
/// with defaults and ensure pod has those defaults applied` conformance
/// failure ("Timeout while waiting for LimitRange creation").
///
/// The conformance test uses an informer (`NewIndexerInformerWatcher`):
///   1. LIST limitranges with `labelSelector=time=<value>` → empty, capture
///      the collection resourceVersion.
///   2. WATCH from that resourceVersion with the same label selector.
///   3. CREATE a LimitRange carrying `metadata.labels.time=<value>`.
///   4. Expect the watch to deliver a live ADDED for it.
///
/// This exercises *live* selector-watch delivery: the create happens after
/// the watch is already streaming, so it must arrive over the broadcast
/// channel (not the catch-up replay). The label value mirrors the
/// conformance test's `nanos + uuid` shape (digits + hyphens).
#[tokio::test]
async fn test_limitrange_label_selector_watch_observes_live_create() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use futures::StreamExt;
    use serde_json::json;
    use std::time::Duration;
    use tower::ServiceExt;

    let app = build_test_router().await;
    let namespace = "limitrange-watch-live";
    let value = "532930117a1b2c3d-5cc8-4f1f-9abc-deadbeef0001";

    let ns_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces")
                .header("content-type", "application/json")
                .body(Body::from(format!(
                    r#"{{"apiVersion":"v1","kind":"Namespace","metadata":{{"name":"{namespace}"}}}}"#
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ns_resp.status(), StatusCode::CREATED);

    // Step 1: LIST with the label selector → empty, capture resourceVersion.
    let list_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "/api/v1/namespaces/{namespace}/limitranges?labelSelector=time%3D{value}"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list_resp.status(), StatusCode::OK);
    let list_body = axum::body::to_bytes(list_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let list: serde_json::Value = serde_json::from_slice(&list_body).unwrap();
    assert!(
        list["items"].as_array().is_some_and(|i| i.is_empty()),
        "limitrange list must be empty before create, got {list}"
    );
    let start_rv = list
        .pointer("/metadata/resourceVersion")
        .and_then(|v| v.as_str())
        .expect("list must include resourceVersion")
        .to_string();

    // Step 2: open the WATCH from the list resourceVersion with the selector.
    let watch_uri = format!(
        "/api/v1/namespaces/{namespace}/limitranges?watch=true&labelSelector=time%3D{value}&resourceVersion={start_rv}"
    );
    let watch_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(watch_uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(watch_resp.status(), StatusCode::OK);
    let mut stream = watch_resp.into_body().into_data_stream();

    // Step 3: CREATE the LimitRange with the matching label (after the watch
    // is already streaming — so this is a live broadcast, not catch-up).
    let limit_range = json!({
        "apiVersion": "v1",
        "kind": "LimitRange",
        "metadata": {
            "name": "limit-range",
            "labels": {"time": value}
        },
        "spec": {
            "limits": [{
                "type": "Container",
                "max": {"cpu": "500m"},
                "min": {"cpu": "50m"},
                "default": {"cpu": "500m"},
                "defaultRequest": {"cpu": "100m"}
            }]
        }
    });
    let create_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/namespaces/{namespace}/limitranges"))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&limit_range).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);

    // Step 4: the watch must deliver a live ADDED for the LimitRange.
    let mut observed_added = false;
    'outer: for _ in 0..16 {
        let chunk = match tokio::time::timeout(Duration::from_millis(500), stream.next()).await {
            Ok(Some(Ok(bytes))) => bytes,
            _ => break,
        };
        for line in chunk.split(|b| *b == b'\n') {
            if line.is_empty() {
                continue;
            }
            let event: serde_json::Value = match serde_json::from_slice(line) {
                Ok(v) => v,
                Err(_) => continue,
            };
            if event["type"] == "ADDED"
                && event
                    .pointer("/object/metadata/name")
                    .and_then(|v| v.as_str())
                    == Some("limit-range")
            {
                observed_added = true;
                break 'outer;
            }
        }
    }
    assert!(
        observed_added,
        "watch with label selector must observe the live LimitRange creation as ADDED"
    );
}

#[tokio::test]
async fn test_pod_watch_catchup_honors_metadata_name_field_selector() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use futures::StreamExt;
    use std::time::Duration;
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
                    r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"pod-watch-field-selector"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ns_resp.status(), StatusCode::CREATED);

    let list_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/namespaces/pod-watch-field-selector/pods")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list_resp.status(), StatusCode::OK);
    let list_body = axum::body::to_bytes(list_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let list: serde_json::Value = serde_json::from_slice(&list_body).unwrap();
    let start_rv = list
        .pointer("/metadata/resourceVersion")
        .and_then(|v| v.as_str())
        .expect("pod list must include resourceVersion");

    for name in ["ignored-pod", "target-pod"] {
        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": name},
            "spec": {
                "containers": [{
                    "name": "pause",
                    "image": "registry.k8s.io/pause:3.10.1"
                }]
            }
        });
        let create_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/namespaces/pod-watch-field-selector/pods")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&pod).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(create_resp.status(), StatusCode::CREATED);
    }

    let watch_uri = format!(
        "/api/v1/namespaces/pod-watch-field-selector/pods?watch=true&resourceVersion={start_rv}&fieldSelector=metadata.name%3Dtarget-pod"
    );
    let watch_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(watch_uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(watch_resp.status(), StatusCode::OK);

    let mut stream = watch_resp.into_body().into_data_stream();
    let chunk = tokio::time::timeout(Duration::from_secs(1), stream.next())
        .await
        .expect("field-selected pod watch should replay the matching pod")
        .expect("watch stream should yield a chunk")
        .expect("watch chunk should be ok");
    let event: serde_json::Value = serde_json::from_slice(&chunk).unwrap();

    assert_eq!(event["type"], "ADDED");
    assert_eq!(
        event
            .pointer("/object/metadata/name")
            .and_then(|v| v.as_str()),
        Some("target-pod"),
        "field-selected watch must not emit non-matching pod events: {event:#?}"
    );
}

#[tokio::test]
async fn test_builtin_list_rejects_unsupported_field_selector() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = build_test_router().await;

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/namespaces/default/configmaps?fieldSelector=data.foo%3Dbar")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let text = String::from_utf8_lossy(&body);
    assert!(
        text.contains("field label not supported: data.foo"),
        "unexpected unsupported field-selector response: {text}"
    );

    let namespace_resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/namespaces?fieldSelector=metadata.namespace%3Ddefault")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(namespace_resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_builtin_watch_rejects_unsupported_field_selector_before_streaming() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = build_test_router().await;

    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/configmaps?watch=true&fieldSelector=data.foo%3Dbar")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_builtin_field_selector_accepts_supported_fields() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = build_test_router().await;

    for uri in [
        "/api/v1/pods?fieldSelector=spec.nodeName%3Dnode-a",
        "/api/v1/nodes?fieldSelector=spec.unschedulable%3Dfalse",
        "/api/v1/namespaces?fieldSelector=metadata.name%3Ddefault",
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
        assert_eq!(resp.status(), StatusCode::OK, "unexpected status for {uri}");
    }
}

#[tokio::test]
async fn test_builtin_selectable_fields_cover_pod_namespace_and_secret_fields() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    let (app, db) = build_test_router_with_db().await;
    for (name, phase) in [
        ("selectable-fields", "Active"),
        ("selectable-fields-terminating", "Terminating"),
    ] {
        db.create_resource(
            "v1",
            "Namespace",
            None,
            name,
            json!({
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {"name": name},
                "status": {"phase": phase}
            }),
        )
        .await
        .unwrap();
    }

    for (name, service_account, pod_ip) in [
        ("pod-builder", "builder", "10.42.0.7"),
        ("pod-runner", "runner", "10.42.0.8"),
    ] {
        db.create_resource(
            "v1",
            "Pod",
            Some("selectable-fields"),
            name,
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": name, "namespace": "selectable-fields"},
                "spec": {
                    "serviceAccountName": service_account,
                    "containers": [{"name": "app", "image": "busybox"}]
                },
                "status": {"podIP": pod_ip}
            }),
        )
        .await
        .unwrap();
    }

    for (name, secret_type) in [
        ("opaque-secret", "Opaque"),
        ("tls-secret", "kubernetes.io/tls"),
    ] {
        db.create_resource(
            "v1",
            "Secret",
            Some("selectable-fields"),
            name,
            json!({
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {"name": name, "namespace": "selectable-fields"},
                "type": secret_type
            }),
        )
        .await
        .unwrap();
    }

    let cases = [
        (
            "/api/v1/namespaces/selectable-fields/pods?fieldSelector=spec.serviceAccountName%3Dbuilder",
            vec!["pod-builder"],
        ),
        (
            "/api/v1/namespaces/selectable-fields/pods?fieldSelector=status.podIP%3D10.42.0.8",
            vec!["pod-runner"],
        ),
        (
            "/api/v1/namespaces?fieldSelector=status.phase%3DTerminating",
            vec!["selectable-fields-terminating"],
        ),
        (
            "/api/v1/namespaces/selectable-fields/secrets?fieldSelector=type%3Dkubernetes.io%2Ftls",
            vec!["tls-secret"],
        ),
    ];

    for (uri, expected_names) in cases {
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
        assert_eq!(resp.status(), StatusCode::OK, "unexpected status for {uri}");
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let list: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            list_item_names(&list),
            expected_names,
            "unexpected field-selector result for {uri}"
        );
    }
}

/// Reproduces sonobuoy "should observe an object deletion if it stops
/// meeting the requirements of the selector" against the real watch
/// pipeline. The conformance test opens a label-selector watch, creates a
/// matching ConfigMap, then changes its label to a non-matching value and
/// expects a synthetic DELETED event. The unit test at
/// `apply_selector_transition_event` already exercises the transition
/// helper in isolation — this test guards the end-to-end path
/// (broadcast → matches_filter_parsed → transition).
///
/// `build_test_app_state` now shares the datastore's topic-aware watch bus
/// with production state, so live mutations reach the watch stream and this
/// end-to-end selector-transition path is covered.
#[tokio::test]
async fn test_label_selector_watch_emits_synthetic_deleted_when_label_stops_matching() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use futures::StreamExt;
    use serde_json::json;
    use std::time::Duration;
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
                    r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"watch-label-stops"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ns_resp.status(), StatusCode::CREATED);

    let watch_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/namespaces/watch-label-stops/configmaps?watch=true&labelSelector=watch-this-configmap%3Dlabel-changed-and-restored")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(watch_resp.status(), StatusCode::OK);
    let mut stream = watch_resp.into_body().into_data_stream();

    async fn drain_until<S>(stream: &mut S, seen: &mut Vec<String>, target: &str) -> bool
    where
        S: futures::Stream<Item = Result<axum::body::Bytes, axum::Error>> + Unpin,
    {
        for _ in 0..16 {
            let chunk = match tokio::time::timeout(Duration::from_millis(500), stream.next()).await
            {
                Ok(Some(Ok(bytes))) => bytes,
                _ => return false,
            };
            for line in chunk.split(|b| *b == b'\n') {
                if line.is_empty() {
                    continue;
                }
                let event: serde_json::Value = match serde_json::from_slice(line) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let t = event["type"].as_str().unwrap_or("").to_string();
                let hit = t == target;
                seen.push(t);
                if hit {
                    return true;
                }
            }
        }
        false
    }

    let mut seen_types = Vec::<String>::new();

    // Create matching configmap → ADDED.
    let cm = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "e2e-watch-test-label-changed",
            "labels": {"watch-this-configmap": "label-changed-and-restored"}
        }
    });
    let create_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces/watch-label-stops/configmaps")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&cm).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);
    assert!(
        drain_until(&mut stream, &mut seen_types, "ADDED").await,
        "expected ADDED for matching create; got {seen_types:?}"
    );

    // Change the label value to one that doesn't match — via a merge-patch
    // (what `kubectl label` / the conformance client uses for label changes).
    // The selector-aware watch must convert this incoming MODIFIED into a
    // synthetic DELETED because the object has left the selector view.
    let patch = serde_json::json!({
        "metadata": {"labels": {"watch-this-configmap": "wrong-value"}}
    });
    let patch_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/api/v1/namespaces/watch-label-stops/configmaps/e2e-watch-test-label-changed")
                .header("content-type", "application/merge-patch+json")
                .body(Body::from(serde_json::to_vec(&patch).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(patch_resp.status(), StatusCode::OK);

    assert!(
        drain_until(&mut stream, &mut seen_types, "DELETED").await,
        "expected synthetic DELETED when label stops matching selector; got {seen_types:?}"
    );
}

#[tokio::test]
async fn test_crd_delete_removes_cluster_custom_resources() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    let app = build_test_router().await;

    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "noxus.mygroup.example.com"},
        "spec": {
            "group": "mygroup.example.com",
            "scope": "Cluster",
            "names": {"plural": "noxus", "singular": "noxu", "kind": "WishIHadChosenNoxu"},
            "versions": [{
                "name": "v1beta1",
                "served": true,
                "storage": true,
                "schema": {"openAPIV3Schema": {"type": "object", "x-kubernetes-preserve-unknown-fields": true}}
            }]
        }
    });

    let create_crd = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&crd).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_crd.status(), StatusCode::CREATED);

    let create_cr = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/mygroup.example.com/v1beta1/noxus")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "apiVersion": "mygroup.example.com/v1beta1",
                        "kind": "WishIHadChosenNoxu",
                        "metadata": {"name": "name1"},
                        "content": {"key": "old"}
                    }))
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_cr.status(), StatusCode::CREATED);

    let delete_crd = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions/noxus.mygroup.example.com")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(delete_crd.status(), StatusCode::OK);

    let recreate_crd = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&crd).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(recreate_crd.status(), StatusCode::CREATED);

    let recreate_cr = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/mygroup.example.com/v1beta1/noxus")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "apiVersion": "mygroup.example.com/v1beta1",
                        "kind": "WishIHadChosenNoxu",
                        "metadata": {"name": "name1"},
                        "content": {"key": "new"}
                    }))
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        recreate_cr.status(),
        StatusCode::CREATED,
        "Deleting a CRD must remove its custom resources so recreating the CRD can recreate same-name objects"
    );
}

#[tokio::test]
async fn test_crd_delete_removes_namespaced_custom_resources() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    let app = build_test_router().await;

    let create_ns = |name: &str| {
        Request::builder()
            .method("POST")
            .uri("/api/v1/namespaces")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&json!({
                    "apiVersion": "v1",
                    "kind": "Namespace",
                    "metadata": {"name": name}
                }))
                .unwrap(),
            ))
            .unwrap()
    };
    let ns_resp = app.clone().oneshot(create_ns("default")).await.unwrap();
    assert_eq!(ns_resp.status(), StatusCode::CREATED);

    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "widgets.example.com"},
        "spec": {
            "group": "example.com",
            "scope": "Namespaced",
            "names": {"plural": "widgets", "singular": "widget", "kind": "Widget"},
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {"openAPIV3Schema": {"type": "object", "x-kubernetes-preserve-unknown-fields": true}}
            }]
        }
    });

    let create_crd = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&crd).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_crd.status(), StatusCode::CREATED);

    let create_cr = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/example.com/v1/namespaces/default/widgets")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "apiVersion": "example.com/v1",
                        "kind": "Widget",
                        "metadata": {"name": "name1", "namespace": "default"},
                        "spec": {"value": "old"}
                    }))
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_cr.status(), StatusCode::CREATED);

    let delete_crd = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions/widgets.example.com")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(delete_crd.status(), StatusCode::OK);

    let recreate_crd = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&crd).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(recreate_crd.status(), StatusCode::CREATED);

    let recreate_cr = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/example.com/v1/namespaces/default/widgets")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "apiVersion": "example.com/v1",
                        "kind": "Widget",
                        "metadata": {"name": "name1", "namespace": "default"},
                        "spec": {"value": "new"}
                    }))
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        recreate_cr.status(),
        StatusCode::CREATED,
        "Deleting a namespaced CRD must remove all namespaced custom resources for that CRD"
    );
}

#[tokio::test]
async fn test_namespaced_crd_watch_field_selector_accepts_declared_selectable_fields() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let registry = state.crd_registry.clone();
    let app = crate::api::build_router(state);

    let crd_name = "e2e-test-crd-selectable-fields-unit-crds.stable.example.com";
    let plural = "e2e-test-crd-selectable-fields-unit-crds";
    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": crd_name},
        "spec": {
            "group": "stable.example.com",
            "scope": "Namespaced",
            "names": {
                "kind": "SelectableFieldCRD",
                "plural": plural,
                "singular": "selectablefieldcrd"
            },
            "versions": [
                {
                    "name": "v1",
                    "served": true,
                    "storage": true,
                    "selectableFields": [{"jsonPath": ".hostPort"}],
                    "schema": {
                        "openAPIV3Schema": {
                            "type": "object",
                            "properties": {"hostPort": {"type": "string"}}
                        }
                    }
                },
                {
                    "name": "v2",
                    "served": true,
                    "storage": false,
                    "selectableFields": [{"jsonPath": ".host"}, {"jsonPath": ".port"}],
                    "schema": {
                        "openAPIV3Schema": {
                            "type": "object",
                            "properties": {
                                "host": {"type": "string"},
                                "port": {"type": "string"}
                            }
                        }
                    }
                }
            ],
            "conversion": {
                "strategy": "Webhook",
                "webhook": {
                    "clientConfig": {
                        "service": {
                            "namespace": "default",
                            "name": "dummy-converter",
                            "path": "/crdconvert",
                            "port": 9443
                        }
                    },
                    "conversionReviewVersions": ["v1", "v1beta1"]
                }
            }
        }
    });

    let create_crd = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&crd).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_crd.status(), StatusCode::CREATED);

    let v2 = registry
        .get("stable.example.com", "v2", plural)
        .await
        .expect("v2 CRD must be registered");
    assert!(
        v2.selectable_fields.contains(&"host".to_string()),
        "v2 selectable fields must include host, got {:?}",
        v2.selectable_fields
    );
    assert!(
        v2.selectable_fields.contains(&"port".to_string()),
        "v2 selectable fields must include port, got {:?}",
        v2.selectable_fields
    );

    let watch_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "/apis/stable.example.com/v2/namespaces/default/{plural}?watch=true&fieldSelector=host%3Dhost1"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        watch_resp.status(),
        StatusCode::OK,
        "watch with field selector host=host1 should be accepted for v2 selectableFields"
    );
}

#[tokio::test]
async fn test_list_pagination_metadata_for_selector_free_and_label_selector_requests() {
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use serde_json::json;
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
                    serde_json::to_vec(&json!({
                        "apiVersion": "v1",
                        "kind": "Namespace",
                        "metadata": {"name": "default"}
                    }))
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(ns_resp.status() == StatusCode::CREATED || ns_resp.status() == StatusCode::CONFLICT);

    for (name, app_label) in [
        ("a-web-1", "web"),
        ("a-web-2", "web"),
        ("a-web-3", "web"),
        ("b-api-1", "api"),
        ("c-web-4", "web"),
    ] {
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/namespaces/default/configmaps")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "apiVersion": "v1",
                            "kind": "ConfigMap",
                            "metadata": {"name": name, "labels": {"app": app_label}},
                            "data": {"k": "v"}
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
    }

    let selector_free_page1 = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/namespaces/default/configmaps?limit=2")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(selector_free_page1.status(), StatusCode::OK);
    let selector_free_page1_body = to_bytes(selector_free_page1.into_body(), usize::MAX)
        .await
        .unwrap();
    let selector_free_page1_json: serde_json::Value =
        serde_json::from_slice(&selector_free_page1_body).unwrap();

    assert!(
        selector_free_page1_json["metadata"]["resourceVersion"]
            .as_str()
            .is_some()
    );
    assert_eq!(
        selector_free_page1_json["items"][0]["metadata"]["name"]
            .as_str()
            .unwrap(),
        "a-web-1"
    );
    assert_eq!(
        selector_free_page1_json["items"][1]["metadata"]["name"]
            .as_str()
            .unwrap(),
        "a-web-2"
    );
    assert!(
        selector_free_page1_json["metadata"]["continue"]
            .as_str()
            .is_some()
    );
    assert!(
        selector_free_page1_json["metadata"]["remainingItemCount"]
            .as_i64()
            .unwrap()
            >= 1
    );

    let selector_page1 = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/namespaces/default/configmaps?labelSelector=app%3Dweb&limit=2")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(selector_page1.status(), StatusCode::OK);
    let selector_page1_body = to_bytes(selector_page1.into_body(), usize::MAX)
        .await
        .unwrap();
    let selector_page1_json: serde_json::Value =
        serde_json::from_slice(&selector_page1_body).unwrap();

    assert!(
        selector_page1_json["metadata"]["resourceVersion"]
            .as_str()
            .is_some()
    );
    assert_eq!(
        selector_page1_json["items"][0]["metadata"]["name"]
            .as_str()
            .unwrap(),
        "a-web-1"
    );
    assert_eq!(
        selector_page1_json["items"][1]["metadata"]["name"]
            .as_str()
            .unwrap(),
        "a-web-2"
    );
    assert_eq!(
        selector_page1_json["metadata"]["remainingItemCount"].as_i64(),
        None,
        "selector queries omit exact remainingItemCount"
    );
    let token = selector_page1_json["metadata"]["continue"]
        .as_str()
        .expect("selector paginated response must include continue token");
    assert!(!token.is_empty());

    let selector_page2 = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "/api/v1/namespaces/default/configmaps?labelSelector=app%3Dweb&limit=2&continue={}",
                    urlencoding::encode(token)
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(selector_page2.status(), StatusCode::OK);
    let selector_page2_body = to_bytes(selector_page2.into_body(), usize::MAX)
        .await
        .unwrap();
    let selector_page2_json: serde_json::Value =
        serde_json::from_slice(&selector_page2_body).unwrap();

    assert_eq!(
        selector_page2_json["items"][0]["metadata"]["name"]
            .as_str()
            .unwrap(),
        "a-web-3"
    );
    assert_eq!(
        selector_page2_json["items"][1]["metadata"]["name"]
            .as_str()
            .unwrap(),
        "c-web-4"
    );
    assert!(selector_page2_json["metadata"].get("continue").is_none());
    assert!(
        selector_page2_json["metadata"]
            .get("remainingItemCount")
            .is_none()
    );
}

#[tokio::test]
async fn test_cluster_custom_resource_watch_from_create_rv_sees_deleted_before_modified() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use futures::StreamExt;
    use serde_json::json;
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let app = crate::api::build_router(state);

    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "clusterwatchprimed.example.com"},
        "spec": {
            "group": "example.com",
            "scope": "Cluster",
            "names": {"plural": "clusterwatchprimed", "singular": "clusterwatchprimed", "kind": "ClusterWatchPrimed"},
            "versions": [{"name": "v1", "served": true, "storage": true, "schema": {"openAPIV3Schema": {"type": "object"}}}]
        }
    });
    let create_crd = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&crd).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_crd.status(), StatusCode::CREATED);

    let create_cr = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/example.com/v1/clusterwatchprimed")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"apiVersion":"example.com/v1","kind":"ClusterWatchPrimed","metadata":{"name":"setup-instance"},"spec":{"x":"y"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_cr.status(), StatusCode::CREATED);
    let create_value: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(create_cr.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    let create_rv = create_value["metadata"]["resourceVersion"]
        .as_str()
        .expect("create response must include metadata.resourceVersion")
        .to_string();

    let delete_cr = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/apis/example.com/v1/clusterwatchprimed/setup-instance")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(delete_cr.status(), StatusCode::OK);

    let watch_uri = format!(
        "/apis/example.com/v1/clusterwatchprimed?watch=true&resourceVersion={}&fieldSelector=metadata.name%3Dsetup-instance",
        create_rv
    );
    let watch_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(&watch_uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(watch_resp.status(), StatusCode::OK);
    let mut stream = watch_resp.into_body().into_data_stream();
    let chunk = tokio::time::timeout(std::time::Duration::from_secs(3), stream.next())
        .await
        .expect("watch stream timed out")
        .expect("watch stream ended unexpectedly")
        .expect("watch stream chunk error");
    let chunk_text = String::from_utf8(chunk.to_vec()).unwrap();
    let first_line = chunk_text
        .lines()
        .find(|line| !line.trim().is_empty())
        .expect("watch response chunk should contain at least one JSON event line");
    let event: serde_json::Value = serde_json::from_str(first_line).unwrap();
    assert_eq!(
        event["type"], "DELETED",
        "watch from create resourceVersion must observe DELETED first for immediate delete, got: {}",
        event
    );
}

/// P0-E2E-20260424b-07: DELETE /apis/storage.k8s.io/v1/csinodes (collection)
/// must return 200 Status (not 405 Method Not Allowed).
#[tokio::test]
async fn test_delete_collection_csinodes_removes_all() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let (app, db) = build_test_router_with_db().await;

    // Create two CSINodes
    for name in ["node-a", "node-b"] {
        let node = serde_json::json!({
            "apiVersion": "storage.k8s.io/v1",
            "kind": "CSINode",
            "metadata": {"name": name},
            "spec": {"drivers": []}
        });
        db.create_resource("storage.k8s.io/v1", "CSINode", None, name, node)
            .await
            .unwrap();
    }

    // DELETE the collection
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/apis/storage.k8s.io/v1/csinodes")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "DELETE /csinodes collection must return 200"
    );

    // Verify both nodes are gone
    let list_resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/storage.k8s.io/v1/csinodes")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list_resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(list_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let list: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        list["items"].as_array().map(|a| a.len()).unwrap_or(0),
        0,
        "all CSINodes must be deleted after delete_collection"
    );
}

/// `kubectl get` server-side Table printing must emit the per-kind columns for
/// BOTH dispatch paths: namespaced resources go through the macros inline list
/// handler, cluster-scoped resources go through the shared `list_inner`. A
/// regression here (the `list_inner` NAME-only fallback) showed only NAME for
/// cluster-scoped kinds, so this exercises one of each end to end.
#[tokio::test]
async fn test_get_table_columns_for_cluster_and_namespaced_resources() {
    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    // Cluster-scoped: ClusterRole -> list_inner path. Default columns.
    db.create_resource(
        "rbac.authorization.k8s.io/v1",
        "ClusterRole",
        None,
        "view-test",
        json!({
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "ClusterRole",
            "metadata": {"name": "view-test", "creationTimestamp": "2025-01-02T03:04:05Z"},
            "rules": []
        }),
    )
    .await
    .unwrap();

    // Namespaced: Service -> macros inline list handler. Custom columns.
    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "svc-test",
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "svc-test", "namespace": "default", "creationTimestamp": "2025-01-02T03:04:05Z"},
            "spec": {"type": "ClusterIP", "clusterIP": "10.0.0.5", "ports": [{"port": 80, "protocol": "TCP"}]}
        }),
    )
    .await
    .unwrap();

    let table_accept = "application/json;as=Table;v=v1;g=meta.k8s.io";

    async fn get_table(app: &axum::Router, uri: &str, accept: &str) -> serde_json::Value {
        use axum::body::Body;
        use axum::http::Request;
        use tower::ServiceExt;
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(uri)
                    .header("accept", accept)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK, "GET {uri}");
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    fn col_names(table: &serde_json::Value) -> Vec<String> {
        table["columnDefinitions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["name"].as_str().unwrap().to_string())
            .collect()
    }
    fn row_for<'a>(table: &'a serde_json::Value, name: &str) -> &'a serde_json::Value {
        table["rows"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["cells"][0].as_str() == Some(name))
            .unwrap_or_else(|| panic!("row {name} not found in {table}"))
    }

    // Cluster-scoped path.
    let cr = get_table(
        &app,
        "/apis/rbac.authorization.k8s.io/v1/clusterroles",
        table_accept,
    )
    .await;
    assert_eq!(cr["kind"], "Table");
    assert_eq!(col_names(&cr), vec!["Name", "Created At"]);
    let cr_row = row_for(&cr, "view-test");
    assert_eq!(cr_row["cells"][1].as_str(), Some("2025-01-02T03:04:05Z"));

    // Namespaced path.
    let svc = get_table(&app, "/api/v1/namespaces/default/services", table_accept).await;
    assert_eq!(svc["kind"], "Table");
    assert_eq!(
        col_names(&svc),
        vec![
            "Name",
            "Type",
            "Cluster-IP",
            "External-IP",
            "Port(s)",
            "Age",
            "Selector"
        ]
    );
    let svc_row = row_for(&svc, "svc-test");
    assert_eq!(svc_row["cells"][1].as_str(), Some("ClusterIP"));
    assert_eq!(svc_row["cells"][2].as_str(), Some("10.0.0.5"));
    assert_eq!(svc_row["cells"][4].as_str(), Some("80/TCP"));
}

/// P0-E2E-20260424b-07: DELETE /apis/rbac.authorization.k8s.io/v1/clusterrolebindings
/// must return 200 (sonobuoy delete calls this).
#[tokio::test]
async fn test_delete_collection_clusterrolebindings_returns_200() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = build_test_router().await;
    let resp = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/apis/rbac.authorization.k8s.io/v1/clusterrolebindings")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "DELETE /clusterrolebindings collection must return 200"
    );
}

#[tokio::test]
async fn test_cluster_delete_collection_persistentvolume_with_finalizer_marks_terminating() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "PersistentVolume",
        None,
        "pv-held",
        json!({
            "apiVersion": "v1",
            "kind": "PersistentVolume",
            "metadata": {
                "name": "pv-held",
                "finalizers": ["example.com/hold"]
            },
            "spec": {
                "capacity": {"storage": "1Gi"},
                "accessModes": ["ReadWriteOnce"],
                "hostPath": {"path": "/tmp/pv-held"}
            }
        }),
    )
    .await
    .unwrap();

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/v1/persistentvolumes")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let live = db
        .get_resource("v1", "PersistentVolume", None, "pv-held")
        .await
        .unwrap()
        .expect("finalizer-held PersistentVolume must not be hard-deleted");
    assert!(
        live.data
            .pointer("/metadata/deletionTimestamp")
            .and_then(|v| v.as_str())
            .is_some(),
        "cluster deleteCollection must mark finalizer-held PV terminating: {:?}",
        live.data
    );
    assert_eq!(
        live.data
            .pointer("/metadata/finalizers/0")
            .and_then(|v| v.as_str()),
        Some("example.com/hold")
    );
}

// ServiceCIDR tests

#[tokio::test]
async fn test_servicecidr_list_returns_empty() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = build_test_router().await;
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/networking.k8s.io/v1/servicecidrs")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let list: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(list["apiVersion"], "networking.k8s.io/v1");
    assert_eq!(list["kind"], "ServiceCIDRList");
    assert_eq!(list["items"].as_array().map(|a| a.len()).unwrap_or(0), 0);
}

#[tokio::test]
async fn test_servicecidr_create_get_delete() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = build_test_router().await;

    let servicecidr = json!({
        "apiVersion": "networking.k8s.io/v1",
        "kind": "ServiceCIDR",
        "metadata": {"name": "test-service-cidr"},
        "spec": {
            "cidrs": ["192.168.0.0/16"]
        }
    });

    // Create
    let req = Request::builder()
        .method("POST")
        .uri("/apis/networking.k8s.io/v1/servicecidrs")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&servicecidr).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Get
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/networking.k8s.io/v1/servicecidrs/test-service-cidr")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let get: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(get["metadata"]["name"], "test-service-cidr");
    assert_eq!(get["spec"]["cidrs"][0], "192.168.0.0/16");

    // Delete
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/apis/networking.k8s.io/v1/servicecidrs/test-service-cidr")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Verify deletion
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/networking.k8s.io/v1/servicecidrs/test-service-cidr")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_servicecidr_update() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = build_test_router().await;

    let servicecidr = json!({
        "apiVersion": "networking.k8s.io/v1",
        "kind": "ServiceCIDR",
        "metadata": {"name": "update-service-cidr"},
        "spec": {"cidrs": ["10.0.0.0/8"]}
    });

    // Create
    let req = Request::builder()
        .method("POST")
        .uri("/apis/networking.k8s.io/v1/servicecidrs")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&servicecidr).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Get with resourceVersion
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/networking.k8s.io/v1/servicecidrs/update-service-cidr")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let original: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let rv = original["metadata"]["resourceVersion"].as_str().unwrap();

    // Update
    let mut updated = original.clone();
    updated["spec"]["cidrs"] = json!(["172.16.0.0/12"]);
    let req = Request::builder()
        .method("PUT")
        .uri("/apis/networking.k8s.io/v1/servicecidrs/update-service-cidr")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&updated).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Verify update
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/networking.k8s.io/v1/servicecidrs/update-service-cidr")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let get: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(get["spec"]["cidrs"][0], "172.16.0.0/12");
    assert_ne!(get["metadata"]["resourceVersion"].as_str().unwrap(), rv);
}

// IPAddress tests

#[tokio::test]
async fn test_ipaddress_list_returns_empty() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = build_test_router().await;
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/networking.k8s.io/v1/ipaddresses")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let list: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(list["apiVersion"], "networking.k8s.io/v1");
    assert_eq!(list["kind"], "IPAddressList");
    assert_eq!(list["items"].as_array().map(|a| a.len()).unwrap_or(0), 0);
}

#[tokio::test]
async fn test_ipaddress_create_get_delete() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = build_test_router().await;

    let ipaddress = json!({
        "apiVersion": "networking.k8s.io/v1",
        "kind": "IPAddress",
        "metadata": {"name": "test-ip-address"},
        "spec": {
            "parentRef": {
                "group": "cluster.x-k8s.io",
                "kind": "Machine",
                "name": "test-machine"
            }
        }
    });

    // Create
    let req = Request::builder()
        .method("POST")
        .uri("/apis/networking.k8s.io/v1/ipaddresses")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&ipaddress).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Get
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/networking.k8s.io/v1/ipaddresses/test-ip-address")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let get: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(get["metadata"]["name"], "test-ip-address");
    assert_eq!(get["spec"]["parentRef"]["kind"], "Machine");

    // Delete
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/apis/networking.k8s.io/v1/ipaddresses/test-ip-address")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Verify deletion
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/networking.k8s.io/v1/ipaddresses/test-ip-address")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_ipaddress_delete_collection_deletes_all_items() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = build_test_router().await;

    for name in ["ip-a", "ip-b"] {
        let body = json!({
            "apiVersion": "networking.k8s.io/v1",
            "kind": "IPAddress",
            "metadata": {"name": name},
            "spec": {
                "parentRef": {
                    "group": "",
                    "kind": "Service",
                    "name": "svc"
                }
            }
        });
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/apis/networking.k8s.io/v1/ipaddresses")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
    }

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/apis/networking.k8s.io/v1/ipaddresses")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/networking.k8s.io/v1/ipaddresses")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let list: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(list["items"].as_array().map(|a| a.len()).unwrap_or(0), 0);
}

#[tokio::test]
async fn test_ipaddress_with_spec_creates_successfully() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = build_test_router().await;

    let ipaddress = json!({
        "apiVersion": "networking.k8s.io/v1",
        "kind": "IPAddress",
        "metadata": {"name": "service-ip"},
        "spec": {
            "parentRef": {
                "group": "",
                "kind": "Service",
                "name": "test-service"
            }
        }
    });

    let req = Request::builder()
        .method("POST")
        .uri("/apis/networking.k8s.io/v1/ipaddresses")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&ipaddress).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Verify the parentRef was preserved
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/networking.k8s.io/v1/ipaddresses/service-ip")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let get: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(get["spec"]["parentRef"]["kind"], "Service");
    assert_eq!(get["spec"]["parentRef"]["name"], "test-service");
}

#[tokio::test]
async fn test_pod_create_applies_limitrange_default_requests_and_limits() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = build_test_router().await;
    let namespace = "limitrange-defaults";

    let ns = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {"name": namespace}
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

    let limit_range = json!({
        "apiVersion": "v1",
        "kind": "LimitRange",
        "metadata": {"name": "defaults"},
        "spec": {
            "limits": [{
                "type": "Container",
                "default": {
                    "cpu": "500m",
                    "memory": "500Mi",
                    "ephemeral-storage": "500Gi"
                },
                "defaultRequest": {
                    "cpu": "100m",
                    "memory": "200Mi",
                    "ephemeral-storage": "200Gi"
                }
            }]
        }
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/namespaces/{namespace}/limitranges"))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&limit_range).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "pod-no-resources"},
        "spec": {
            "containers": [{
                "name": "pause",
                "image": "registry.k8s.io/pause:3.10.1"
            }]
        }
    });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/namespaces/{namespace}/pods"))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&pod).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let created: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let resources = &created["spec"]["containers"][0]["resources"];
    assert_eq!(resources["requests"]["cpu"], "100m");
    assert_eq!(resources["requests"]["memory"], "200Mi");
    assert_eq!(resources["requests"]["ephemeral-storage"], "200Gi");
    assert_eq!(resources["limits"]["cpu"], "500m");
    assert_eq!(resources["limits"]["memory"], "500Mi");
    assert_eq!(resources["limits"]["ephemeral-storage"], "500Gi");
}

#[tokio::test]
async fn test_pod_create_limitrange_explicit_limit_defaults_request_to_limit() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = build_test_router().await;
    let namespace = "limitrange-partial";

    let ns = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {"name": namespace}
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

    let limit_range = json!({
        "apiVersion": "v1",
        "kind": "LimitRange",
        "metadata": {"name": "defaults"},
        "spec": {
            "limits": [{
                "type": "Container",
                "default": {
                    "cpu": "500m",
                    "memory": "500Mi",
                    "ephemeral-storage": "500Gi"
                },
                "defaultRequest": {
                    "cpu": "100m",
                    "memory": "150Mi",
                    "ephemeral-storage": "150Gi"
                }
            }]
        }
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/namespaces/{namespace}/limitranges"))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&limit_range).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "pod-partial-resources"},
        "spec": {
            "containers": [{
                "name": "pause",
                "image": "registry.k8s.io/pause:3.10.1",
                "resources": {
                    "limits": {
                        "cpu": "300m"
                    }
                }
            }]
        }
    });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/namespaces/{namespace}/pods"))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&pod).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let created: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let resources = &created["spec"]["containers"][0]["resources"];
    assert_eq!(
        resources["requests"]["cpu"], "300m",
        "cpu request must default to explicitly set cpu limit"
    );
    assert_eq!(resources["requests"]["memory"], "150Mi");
    assert_eq!(resources["requests"]["ephemeral-storage"], "150Gi");
    assert_eq!(resources["limits"]["cpu"], "300m");
}

#[tokio::test]
async fn test_pod_create_limitrange_rejects_resources_below_minimum() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = build_test_router().await;
    let namespace = "limitrange-min-reject";

    let ns = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {"name": namespace}
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

    let limit_range = json!({
        "apiVersion": "v1",
        "kind": "LimitRange",
        "metadata": {"name": "limits"},
        "spec": {
            "limits": [{
                "type": "Container",
                "min": {
                    "cpu": "200m",
                    "memory": "100Mi",
                    "ephemeral-storage": "100Gi"
                }
            }]
        }
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/namespaces/{namespace}/limitranges"))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&limit_range).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "below-min"},
        "spec": {
            "containers": [{
                "name": "pause",
                "image": "registry.k8s.io/pause:3.10.1",
                "resources": {
                    "requests": {
                        "cpu": "100m",
                        "memory": "100Mi",
                        "ephemeral-storage": "100Gi"
                    },
                    "limits": {
                        "cpu": "300m",
                        "memory": "100Mi",
                        "ephemeral-storage": "100Gi"
                    }
                }
            }]
        }
    });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/namespaces/{namespace}/pods"))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&pod).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn test_pod_create_limitrange_rejects_pod_aggregate_over_maximum() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = build_test_router().await;
    let namespace = "limitrange-pod-max";

    let ns = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {"name": namespace}
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

    let limit_range = json!({
        "apiVersion": "v1",
        "kind": "LimitRange",
        "metadata": {"name": "pod-aggregate"},
        "spec": {
            "limits": [{
                "type": "Pod",
                "max": {"cpu": "500m"}
            }]
        }
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/namespaces/{namespace}/limitranges"))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&limit_range).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "too-large"},
        "spec": {
            "containers": [
                {
                    "name": "a",
                    "image": "registry.k8s.io/pause:3.10.1",
                    "resources": {"requests": {"cpu": "300m"}}
                },
                {
                    "name": "b",
                    "image": "registry.k8s.io/pause:3.10.1",
                    "resources": {"requests": {"cpu": "300m"}}
                }
            ]
        }
    });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/namespaces/{namespace}/pods"))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&pod).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "LimitRange type=Pod max.cpu must reject aggregate Pod requests over the maximum"
    );
}

#[tokio::test]
async fn test_pvc_create_limitrange_rejects_storage_over_maximum() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = build_test_router().await;
    let namespace = "limitrange-pvc-max";

    let ns = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {"name": namespace}
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

    let limit_range = json!({
        "apiVersion": "v1",
        "kind": "LimitRange",
        "metadata": {"name": "pvc-storage"},
        "spec": {
            "limits": [{
                "type": "PersistentVolumeClaim",
                "max": {"storage": "1Gi"}
            }]
        }
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/namespaces/{namespace}/limitranges"))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&limit_range).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let pvc = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": "too-large"},
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": {"requests": {"storage": "2Gi"}}
        }
    });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/api/v1/namespaces/{namespace}/persistentvolumeclaims"
                ))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&pvc).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "LimitRange type=PersistentVolumeClaim max.storage must reject oversized PVC requests"
    );
}

#[tokio::test]
async fn test_pod_create_limitrange_rejects_max_limit_request_ratio() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = build_test_router().await;
    let namespace = "limitrange-ratio-reject";

    let ns = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {"name": namespace}
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

    let limit_range = json!({
        "apiVersion": "v1",
        "kind": "LimitRange",
        "metadata": {"name": "limits"},
        "spec": {
            "limits": [{
                "type": "Container",
                "maxLimitRequestRatio": {
                    "cpu": "2"
                }
            }]
        }
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/namespaces/{namespace}/limitranges"))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&limit_range).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "ratio-over"},
        "spec": {
            "containers": [{
                "name": "pause",
                "image": "registry.k8s.io/pause:3.10.1",
                "resources": {
                    "requests": {"cpu": "100m"},
                    "limits": {"cpu": "300m"}
                }
            }]
        }
    });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/namespaces/{namespace}/pods"))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&pod).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

/// P0-CORR-02: Test that CRD status update with malformed status.conditions
/// (non-array values) does not panic and normalizes to empty array.
///
/// This tests the fix for a panic where `status["conditions"].as_array_mut().unwrap()`
/// would panic if a user sent a request with status.conditions as a string, number,
/// or object instead of an array.
#[tokio::test]
async fn test_crd_status_conditions_normalizes_non_array_values() {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use serde_json::json;
    use tower::ServiceExt;

    let app = build_test_router().await;

    // Test 1: status.conditions as string
    {
        let crd_body = json!({
            "apiVersion": "apiextensions.k8s.io/v1",
            "kind": "CustomResourceDefinition",
            "metadata": {"name": "test-cond-string.example.com"},
            "spec": {
                "group": "example.com",
                "names": {"kind": "TestCondString", "plural": "testcondstrings"},
                "scope": "Namespaced",
                "versions": [{"name": "v1", "served": true, "storage": true, "schema": {"openAPIV3Schema": {"type": "object"}}}]
            },
            "status": {
                "conditions": "not-an-array"
            }
        });

        let req = Request::builder()
            .method("POST")
            .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&crd_body).unwrap()))
            .unwrap();

        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::CREATED,
            "CRD with string conditions must succeed"
        );
    }

    // Test 2: status.conditions as number
    {
        let crd_body = json!({
            "apiVersion": "apiextensions.k8s.io/v1",
            "kind": "CustomResourceDefinition",
            "metadata": {"name": "test-cond-number.example.com"},
            "spec": {
                "group": "example.com",
                "names": {"kind": "TestCondNumber", "plural": "testcondnumbers"},
                "scope": "Namespaced",
                "versions": [{"name": "v1", "served": true, "storage": true, "schema": {"openAPIV3Schema": {"type": "object"}}}]
            },
            "status": {
                "conditions": 42
            }
        });

        let req = Request::builder()
            .method("POST")
            .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&crd_body).unwrap()))
            .unwrap();

        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::CREATED,
            "CRD with number conditions must succeed"
        );
    }

    // Test 3: status.conditions as object
    {
        let crd_body = json!({
            "apiVersion": "apiextensions.k8s.io/v1",
            "kind": "CustomResourceDefinition",
            "metadata": {"name": "test-cond-object.example.com"},
            "spec": {
                "group": "example.com",
                "names": {"kind": "TestCondObject", "plural": "testcondobjects"},
                "scope": "Namespaced",
                "versions": [{"name": "v1", "served": true, "storage": true, "schema": {"openAPIV3Schema": {"type": "object"}}}]
            },
            "status": {
                "conditions": {"not": "an-array"}
            }
        });

        let req = Request::builder()
            .method("POST")
            .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&crd_body).unwrap()))
            .unwrap();

        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::CREATED,
            "CRD with object conditions must succeed"
        );
    }

    // Test 4: status.conditions as null
    {
        let crd_body = json!({
            "apiVersion": "apiextensions.k8s.io/v1",
            "kind": "CustomResourceDefinition",
            "metadata": {"name": "test-cond-null.example.com"},
            "spec": {
                "group": "example.com",
                "names": {"kind": "TestCondNull", "plural": "testcondnulls"},
                "scope": "Namespaced",
                "versions": [{"name": "v1", "served": true, "storage": true, "schema": {"openAPIV3Schema": {"type": "object"}}}]
            },
            "status": {
                "conditions": null
            }
        });

        let req = Request::builder()
            .method("POST")
            .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&crd_body).unwrap()))
            .unwrap();

        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::CREATED,
            "CRD with null conditions must succeed"
        );

        // Verify conditions is now an array with Established condition
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let created: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(
            created["status"]["conditions"].is_array(),
            "status.conditions must be an array"
        );
        assert_eq!(created["status"]["conditions"][0]["type"], "Established");
    }
}

/// Closes the historical-migration gap left by `test_watch_no_duplicate_headers.sh`
/// + `test_watch_table.sh` (deleted in commit 4b8c098, replaced by
/// `chainsaw/watch-events` which only checks resource name appears in stream).
///
/// What the deleted shell tests asserted, that nothing else exercised at the
/// HTTP layer:
///   1. `kubectl get pods -w` (Accept: as=Table) shows column headers exactly
///      ONCE on the initial response.
///   2. Subsequent watch events do NOT carry `columnDefinitions` (otherwise
///      kubectl repeats the header on every event — the bug the shell test
///      caught).
///   3. Watch events carry table cells with READY/STATUS so kubectl renders
///      pod state.
///
/// `mod_tests` covers `watch_event_to_table` in isolation
/// (`test_watch_event_to_table_pod_modified_includes_ready_status_restarts`,
/// `test_watch_bookmark_table_omits_column_definitions_for_periodic_bookmarks`,
/// `test_watch_bookmark_table_initial_events_end_has_column_definitions`),
/// but no test wires the conversion through the HTTP watch handler.
#[tokio::test]
async fn test_watch_pods_as_table_emits_columns_once_then_table_rows_per_event() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use futures::StreamExt;
    use serde_json::json;
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
                    r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"watch-table-test"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ns_resp.status(), StatusCode::CREATED);

    // Create a pod first so the watch starts with something. Capture its rv so
    // we can resume the watch from that point and observe a deterministic
    // post-create event sequence.
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "table-watch-pod", "namespace": "watch-table-test"},
        "spec": {"containers": [{"name": "c", "image": "registry.example.invalid/klights/test-image:1"}]}
    });
    let create_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces/watch-table-test/pods")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&pod).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);
    let create_value: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(create_resp.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    let initial_rv = create_value["metadata"]["resourceVersion"]
        .as_str()
        .expect("create response must carry resourceVersion")
        .to_string();

    // Open a watch with as=Table from before the pod was created (rv=0) AND
    // sendInitialEvents=true so kubectl gets columnDefinitions on the first
    // event and an initial-events-end BOOKMARK afterwards.
    let watch_uri = "/api/v1/namespaces/watch-table-test/pods\
        ?watch=true&sendInitialEvents=true&resourceVersionMatch=NotOlderThan&allowWatchBookmarks=true&resourceVersion=0";
    let watch_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(watch_uri)
                .header(
                    "Accept",
                    "application/json;as=Table;v=v1;g=meta.k8s.io,application/json",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(watch_resp.status(), StatusCode::OK);

    // Drain at least the initial events: ADDED for the existing pod and the
    // initial-events-end BOOKMARK. Use a short timeout so we don't hang if the
    // server fails to flush.
    let mut stream = watch_resp.into_body().into_data_stream();
    let mut events: Vec<serde_json::Value> = Vec::new();
    let drain_deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    while events.len() < 2 && std::time::Instant::now() < drain_deadline {
        let remaining = drain_deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        let chunk = match tokio::time::timeout(remaining, stream.next()).await {
            Ok(Some(Ok(c))) => c,
            _ => break,
        };
        let text = String::from_utf8(chunk.to_vec()).unwrap();
        for line in text.lines().filter(|l| !l.trim().is_empty()) {
            let event: serde_json::Value = serde_json::from_str(line).unwrap_or_else(|err| {
                panic!("watch event line must be valid JSON; err={err}, line={line}");
            });
            events.push(event);
        }
    }

    assert!(
        !events.is_empty(),
        "watch with as=Table must emit at least one event"
    );

    // First event must carry columnDefinitions. This is the equivalent of the
    // header line kubectl renders. Without this, the deleted shell test's
    // HEADER_COUNT=0 assertion would have failed.
    // Klights's table-watch contract (matching upstream kube-apiserver):
    //   - Initial ADDED events carry table `rows` but NO `columnDefinitions`.
    //   - The single `initial-events-end` BOOKMARK carries `columnDefinitions`
    //     (this is what kubectl uses to print the header line ONCE).
    //   - All other (periodic) BOOKMARKs and post-bookmark MODIFIED events
    //     omit `columnDefinitions` so kubectl never re-prints headers.
    //
    // The deleted shell test (`test_watch_no_duplicate_headers.sh`) asserted
    // headers appear exactly once in `kubectl get pods -w` output; this test
    // is the structural HTTP-level equivalent.

    // Locate the initial-events-end BOOKMARK and the initial ADDED event for
    // our pod. Both must be present.
    let mut initial_end_bookmark: Option<&serde_json::Value> = None;
    let mut added_pod_event: Option<&serde_json::Value> = None;
    for evt in &events {
        let event_type = evt.get("type").and_then(|t| t.as_str()).unwrap_or("");
        let obj = evt.get("object").unwrap_or(&serde_json::Value::Null);
        if event_type == "BOOKMARK" {
            let is_end = obj
                .pointer("/metadata/annotations/k8s.io~1initial-events-end")
                .is_some();
            if is_end && initial_end_bookmark.is_none() {
                initial_end_bookmark = Some(evt);
            }
        } else if event_type == "ADDED" && added_pod_event.is_none() {
            added_pod_event = Some(evt);
        }
    }
    let initial_end_bookmark = initial_end_bookmark.unwrap_or_else(|| {
        panic!(
            "expected an initial-events-end BOOKMARK so kubectl can print the header line; got events: {events:#?}"
        )
    });
    let added_pod_event = added_pod_event.unwrap_or_else(|| {
        panic!("expected at least one ADDED event for the existing pod; got events: {events:#?}")
    });

    // BOOKMARK must carry columnDefinitions including READY+STATUS.
    let bookmark_columns = initial_end_bookmark["object"]["columnDefinitions"]
        .as_array()
        .expect("initial-events-end BOOKMARK must include columnDefinitions");
    let column_names: Vec<&str> = bookmark_columns
        .iter()
        .filter_map(|c| c["name"].as_str())
        .collect();
    assert!(
        column_names.iter().any(|n| n.eq_ignore_ascii_case("READY")),
        "Pod table columnDefinitions must include READY (kubectl shows pod readiness); got {column_names:?}"
    );
    assert!(
        column_names
            .iter()
            .any(|n| n.eq_ignore_ascii_case("STATUS")),
        "Pod table columnDefinitions must include STATUS; got {column_names:?}"
    );

    // No non-BOOKMARK event may carry columnDefinitions (would cause kubectl
    // to re-print headers). The original `test_watch_no_duplicate_headers.sh`
    // bug.
    for evt in &events {
        let event_type = evt.get("type").and_then(|t| t.as_str()).unwrap_or("");
        if event_type == "BOOKMARK" {
            continue;
        }
        let has_columns = evt["object"]
            .get("columnDefinitions")
            .and_then(|c| c.as_array())
            .map(|a| !a.is_empty())
            .unwrap_or(false);
        assert!(
            !has_columns,
            "non-BOOKMARK event must not carry columnDefinitions (kubectl would print duplicate headers); offending event: {evt:#?}"
        );
    }

    // The ADDED event for our existing pod must have a row with cells matching
    // the BOOKMARK column count, so kubectl can render the row underneath
    // the printed header. This is the equivalent of `test_watch_table.sh`'s
    // "Watch events have READY/STATUS columns" assertion.
    let rows = added_pod_event["object"]["rows"]
        .as_array()
        .expect("ADDED event for pod must include table rows");
    assert!(
        !rows.is_empty(),
        "ADDED event for the existing pod must include at least one row"
    );
    let first_row = &rows[0];
    let cells = first_row["cells"]
        .as_array()
        .expect("row must include cells array");
    assert_eq!(
        cells.len(),
        column_names.len(),
        "row cell count ({}) must match columnDefinitions count ({}); cells={cells:?}",
        cells.len(),
        column_names.len()
    );
    let _ = initial_rv;
}

/// Cluster-wide label-selector watch baseline must include same-name objects
/// from different namespaces. The selector transition logic is verified by
/// unit tests for  which use the correct
///  tuple key.
#[tokio::test]
async fn test_cluster_wide_selector_watch_baseline_includes_same_name_different_namespace() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use futures::StreamExt;
    use serde_json::json;
    use std::time::Duration;
    use tower::ServiceExt;

    let app = build_test_router().await;

    // Create namespaces a and b
    for ns in ["a", "b"] {
        let ns_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/namespaces")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "apiVersion": "v1",
                            "kind": "Namespace",
                            "metadata": {"name": ns}
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ns_resp.status(), StatusCode::CREATED);
    }

    // Create a/shared and b/shared both matching label app=web
    for ns in ["a", "b"] {
        let cm = json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "shared",
                "namespace": ns,
                "labels": {"app": "web"}
            }
        });
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/v1/namespaces/{ns}/configmaps"))
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&cm).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
    }

    // Start cluster-wide watch with labelSelector
    let watch_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/configmaps?watch=true&labelSelector=app%3Dweb")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(watch_resp.status(), StatusCode::OK);
    let mut stream = watch_resp.into_body().into_data_stream();

    // Both a/shared and b/shared must appear as separate ADDED events
    let mut seen = Vec::new();
    for _ in 0..2 {
        let chunk = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("should receive initial ADDED event")
            .expect("stream should yield chunk")
            .expect("chunk should be ok");
        let event: serde_json::Value = serde_json::from_slice(&chunk).unwrap();
        assert_eq!(
            event["type"], "ADDED",
            "initial events must be ADDED: {event:#?}"
        );
        let ns = event
            .pointer("/object/metadata/namespace")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        let name = event
            .pointer("/object/metadata/name")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        seen.push((ns, name));
    }
    assert!(
        seen.contains(&(Some("a".to_string()), Some("shared".to_string())))
            && seen.contains(&(Some("b".to_string()), Some("shared".to_string()))),
        "both a/shared and b/shared must appear as separate ADDED events: {seen:?}"
    );
}

/// Regression for the phantom-pod artifact (k8s-fix.md): a complete list must
/// report the global snapshot resourceVersion, not max(surviving item RV). When
/// it under-reports, a follow-up `?watch=true&resourceVersion=<list rv>` replays
/// the durable create→delete history of already-removed objects — which is what
/// makes `kubectl get -A -w` print rows for pods that are gone. ConfigMaps
/// exercise the identical generic list/watch path without pod-lifecycle timing.
#[tokio::test]
async fn test_list_rv_is_global_snapshot_so_watch_does_not_replay_deleted_objects() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use futures::StreamExt;
    use serde_json::json;
    use std::time::Duration;
    use tower::ServiceExt;

    let (app, db) = build_test_router_with_db().await;

    let ns_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"phantom-ns"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ns_resp.status(), StatusCode::CREATED);

    // A surviving object (the "coredns" analog) created first → lowest RV.
    let survivor = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "cm-survivor", "namespace": "phantom-ns"}
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces/phantom-ns/configmaps")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&survivor).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // A doomed object created later, then deleted → both ADDED and DELETED land
    // in the durable watch history at RVs above the survivor's RV.
    let doomed = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "cm-doomed", "namespace": "phantom-ns"}
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces/phantom-ns/configmaps")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&doomed).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let del_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/v1/namespaces/phantom-ns/configmaps/cm-doomed")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(del_resp.status(), StatusCode::OK);

    // Plain (complete) list of the namespace.
    let list_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/namespaces/phantom-ns/configmaps")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list_resp.status(), StatusCode::OK);
    let list_body = axum::body::to_bytes(list_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let list: serde_json::Value = serde_json::from_slice(&list_body).unwrap();
    let names: Vec<&str> = list["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|i| i.pointer("/metadata/name").and_then(|v| v.as_str()))
        .collect();
    assert_eq!(
        names,
        vec!["cm-survivor"],
        "only the survivor must remain in the list"
    );
    let list_rv: i64 = list
        .pointer("/metadata/resourceVersion")
        .and_then(|v| v.as_str())
        .expect("list must carry a resourceVersion")
        .parse()
        .expect("list resourceVersion must be an integer");

    // The list RV must be the store's global snapshot RV, not max(survivor RV).
    let current_rv = db.get_current_resource_version().await.unwrap();
    assert_eq!(
        list_rv, current_rv,
        "list resourceVersion ({list_rv}) must equal the global snapshot rv ({current_rv}), \
         not the surviving item's lower rv"
    );

    // Watch from the list RV. Because it anchors to "now", the server must NOT
    // replay cm-doomed's create/delete. We prove the negative robustly: create a
    // fresh sentinel and assert the FIRST streamed event is the sentinel ADDED,
    // never a replayed cm-doomed event.
    let watch_uri =
        format!("/api/v1/namespaces/phantom-ns/configmaps?watch=true&resourceVersion={list_rv}");
    let watch_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(watch_uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(watch_resp.status(), StatusCode::OK);

    let sentinel = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "cm-sentinel", "namespace": "phantom-ns"}
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces/phantom-ns/configmaps")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&sentinel).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let mut stream = watch_resp.into_body().into_data_stream();
    let chunk = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("watch from list rv should yield the sentinel event")
        .expect("watch stream should yield a chunk")
        .expect("watch chunk should be ok");
    let event: serde_json::Value = serde_json::from_slice(&chunk).unwrap();
    assert_eq!(
        event
            .pointer("/object/metadata/name")
            .and_then(|v| v.as_str()),
        Some("cm-sentinel"),
        "first watch event must be the sentinel, not a replayed phantom: {event:#?}"
    );
    assert_eq!(event["type"], "ADDED");
}

/// bug-grpc B1: a plain (selector-less) RV-less watch must not drop an event
/// committed in the establishment window.
///
/// The handler subscribes to the broadcast during request handling (eagerly,
/// before the lazy stream body is polled), while the stream's own
/// resourceVersion read happens on the first poll. Committing the event
/// *between* the watch response returning and the first stream read places it
/// squarely in that window. With the old `floor = current_rv` (read after
/// subscribe) the buffered event's `rv <= floor` and it was silently filtered —
/// the APF-PLC / RC-lifecycle flake. With the floor captured before subscribe
/// the event is delivered (live or via the resume catch-up).
#[tokio::test]
async fn watch_delivers_event_committed_immediately_after_establishment() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use futures::StreamExt;
    use serde_json::json;
    use std::time::Duration;
    use tower::ServiceExt;

    let app = build_test_router().await;
    let ns = "b1-establish";
    let ns_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"apiVersion":"v1","kind":"Namespace","metadata":{"name":ns}})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ns_resp.status(), StatusCode::CREATED);

    // Seed a ConfigMap so the collection has a non-zero baseline RV.
    let seed = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/namespaces/{ns}/configmaps"))
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "apiVersion": "v1",
                        "kind": "ConfigMap",
                        "metadata": {"name": "cm-seed", "namespace": ns},
                        "data": {"k": "v"}
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(seed.status(), StatusCode::CREATED);

    // Open a plain, selector-less, RV-less watch. The handler subscribes during
    // this await; the stream body is not polled yet.
    let watch_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/api/v1/namespaces/{ns}/configmaps?watch=true"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(watch_resp.status(), StatusCode::OK);

    // Establishment window: commit a new ConfigMap BEFORE reading the stream.
    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/namespaces/{ns}/configmaps"))
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "apiVersion": "v1",
                        "kind": "ConfigMap",
                        "metadata": {"name": "cm-establish", "namespace": ns},
                        "data": {"k": "v"}
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    // Now read the stream and assert the establishment-window event arrives.
    let mut stream = watch_resp.into_body().into_data_stream();
    let mut delivered = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while tokio::time::Instant::now() < deadline {
        let Ok(Some(Ok(chunk))) =
            tokio::time::timeout(Duration::from_millis(500), stream.next()).await
        else {
            break;
        };
        for line in chunk.split(|b| *b == b'\n').filter(|l| !l.is_empty()) {
            if let Ok(event) = serde_json::from_slice::<serde_json::Value>(line)
                && event
                    .pointer("/object/metadata/name")
                    .and_then(|v| v.as_str())
                    == Some("cm-establish")
            {
                delivered = true;
                break;
            }
        }
        if delivered {
            break;
        }
    }
    assert!(
        delivered,
        "a ConfigMap committed in the establishment window must be delivered to a plain RV-less watch"
    );
}

#[tokio::test]
async fn selectorless_watch_resource_version_zero_replays_current_state() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use futures::StreamExt;
    use serde_json::json;
    use std::time::Duration;
    use tower::ServiceExt;

    let app = build_test_router().await;
    let ns = "rv-zero-current-state";

    let ns_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"apiVersion":"v1","kind":"Namespace","metadata":{"name":ns}})
                        .to_string(),
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
                .uri(format!("/api/v1/namespaces/{ns}/configmaps"))
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "apiVersion": "v1",
                        "kind": "ConfigMap",
                        "metadata": {"name": "cm-existing", "namespace": ns},
                        "data": {"k": "v"}
                    })
                    .to_string(),
                ))
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
                .uri(format!(
                    "/api/v1/namespaces/{ns}/configmaps?watch=true&resourceVersion=0"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(watch_resp.status(), StatusCode::OK);

    let mut stream = watch_resp.into_body().into_data_stream();
    let chunk = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("resourceVersion=0 watch must replay current state")
        .expect("stream must remain open")
        .expect("watch chunk must be readable");

    let mut saw_existing = false;
    for line in chunk.split(|b| *b == b'\n').filter(|line| !line.is_empty()) {
        let event: serde_json::Value = serde_json::from_slice(line).unwrap();
        if event["type"] == "ADDED"
            && event
                .pointer("/object/metadata/name")
                .and_then(|v| v.as_str())
                == Some("cm-existing")
        {
            saw_existing = true;
        }
    }
    assert!(
        saw_existing,
        "selector-less resourceVersion=0 watch must emit existing objects as ADDED"
    );
}

/// Regression: a watch BOOKMARK must report the rv this scoped watch has
/// actually caught up to — NOT the global collection resourceVersion. Under
/// parallel load the global rv races ahead of a namespaced/selector watch's
/// delivered frontier; advertising it in a bookmark lets a client-go
/// RetryWatcher (AllowWatchBookmarks) advance its resume point past an
/// undelivered event and skip it on reconnect. This is the flaky
/// `[sig-apps] Deployment should run the lifecycle` failure (the watcher
/// only ever observes MODIFIED, never the ADDED). Here we inflate the global
/// rv with unrelated writes after capturing the watch's resume rv, then assert
/// the immediate bookmark reports the resume rv, not the inflated global one.
#[tokio::test]
async fn test_bookmark_reports_caught_up_rv_not_global_collection_rv() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use futures::StreamExt;
    use serde_json::json;
    use std::time::Duration;
    use tower::ServiceExt;

    let app = build_test_router().await;
    let namespace = "bookmark-no-overreport";

    let ns_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces")
                .header("content-type", "application/json")
                .body(Body::from(format!(
                    r#"{{"apiVersion":"v1","kind":"Namespace","metadata":{{"name":"{namespace}"}}}}"#
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ns_resp.status(), StatusCode::CREATED);

    // Capture the deployment collection rv (== global rv) BEFORE inflating it.
    let list_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/apis/apps/v1/namespaces/{namespace}/deployments"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let list_bytes = axum::body::to_bytes(list_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let list_json: serde_json::Value = serde_json::from_slice(&list_bytes).unwrap();
    let resume_rv: i64 = list_json["metadata"]["resourceVersion"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();

    // Inflate the GLOBAL resourceVersion with unrelated writes (ConfigMaps) so
    // the global rv races well ahead of the (empty) deployment watch scope.
    for i in 0..5 {
        let cm = json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": format!("inflate-{i}"), "namespace": namespace},
            "data": {"k": "v"}
        });
        let r = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/v1/namespaces/{namespace}/configmaps"))
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&cm).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::CREATED);
    }

    // Confirm the global rv really advanced past the resume point.
    let list2 = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/api/v1/namespaces/{namespace}/configmaps"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let list2_bytes = axum::body::to_bytes(list2.into_body(), usize::MAX)
        .await
        .unwrap();
    let list2_json: serde_json::Value = serde_json::from_slice(&list2_bytes).unwrap();
    let global_rv: i64 = list2_json["metadata"]["resourceVersion"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert!(
        global_rv > resume_rv,
        "test setup: global rv ({global_rv}) must advance past the resume rv ({resume_rv})"
    );

    // Watch the (empty) deployment scope from the resume rv with bookmarks on.
    // The bookmark timer fires an immediate tick, so the first event is a
    // BOOKMARK carrying the rv this watch has caught up to.
    let watch_resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "/apis/apps/v1/namespaces/{namespace}/deployments?watch=true&resourceVersion={resume_rv}&allowWatchBookmarks=true"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(watch_resp.status(), StatusCode::OK);

    let mut stream = watch_resp.into_body().into_data_stream();
    let mut bookmark_rv: Option<i64> = None;
    'outer: for _ in 0..8 {
        let chunk = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("watch must emit a bookmark")
            .expect("watch stream ended before a bookmark")
            .expect("watch stream chunk error");
        let text = String::from_utf8(chunk.to_vec()).unwrap();
        for line in text.lines().filter(|l| !l.trim().is_empty()) {
            let event: serde_json::Value = serde_json::from_str(line).unwrap();
            if event["type"] == "BOOKMARK" {
                bookmark_rv = event["object"]["metadata"]["resourceVersion"]
                    .as_str()
                    .and_then(|s| s.parse().ok());
                break 'outer;
            }
        }
    }

    let bookmark_rv = bookmark_rv.expect("watch must deliver a BOOKMARK event");
    assert!(
        bookmark_rv <= resume_rv,
        "bookmark rv ({bookmark_rv}) must not exceed the watch's caught-up rv ({resume_rv}); \
         leaking the global rv ({global_rv}) lets a resuming client skip undelivered events"
    );
}

/// Characterization coverage for scoped-watch resume, NOT a regression for the
/// `initial_list_rv` catch-up advancement. It locks in the end-to-end behavior a
/// client-go reflector relies on: after a label-scoped watch resumes from a
/// BOOKMARK produced under unrelated (out-of-scope) write churn, subsequent
/// in-scope create + modify events are still delivered in order.
///
/// This is deliberately not a regression for the `delivered_scoped_catchup_rv`
/// change in `build_label_selector_watch_stream`: that change tightens the
/// catch-up floor to the last *emitted* scoped RV instead of the last
/// *encountered* (incl. out-of-scope) RV, but `list_resources_modified_since`
/// returns events `ORDER BY resource_version ASC`, so under RV monotonicity no
/// in-scope event can ever land in `(emitted_scoped_max, encountered_max]`. The
/// inflated floor therefore never drops an extra event -- the change is a
/// defensive invariant cleanup with no observable delivery difference, which is
/// why this test passes with or without it.
#[tokio::test]
async fn test_scoped_watch_reconnect_from_bookmark_delivers_subsequent_in_scope_events() {
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use futures::StreamExt;
    use serde_json::json;
    use std::time::Duration;
    use tower::ServiceExt;

    let app = build_test_router().await;
    let namespace = "scoped-watch-bookmark-resume";
    let selector = "app%3Dguestbook%2Ctier%3Dfrontend";
    let deployment_name = "bookmark-frontend-deployment";
    let first_pod = "bookmark-first-pod";
    let second_pod = "bookmark-second-pod";

    let ns_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces")
                .header("content-type", "application/json")
                .body(Body::from(format!(
                    r#"{{"apiVersion":"v1","kind":"Namespace","metadata":{{"name":"{namespace}"}}}}"#
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ns_resp.status(), StatusCode::CREATED);

    let deployment_body = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": deployment_name,
            "namespace": namespace,
            "labels": {"app": "guestbook", "tier": "frontend"}
        },
        "spec": {
            "replicas": 1,
            "selector": {
                "matchLabels": {"app": "guestbook", "tier": "frontend"}
            },
            "template": {
                "metadata": {
                    "labels": {"app": "guestbook", "tier": "frontend"}
                },
                "spec": {
                    "containers": [{
                        "name": "c",
                        "image": "registry.k8s.io/pause:3.10.1"
                    }]
                }
            }
        }
    });
    let deployment_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/apis/apps/v1/namespaces/{namespace}/deployments"))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&deployment_body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(deployment_resp.status(), StatusCode::CREATED);

    let list_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "/api/v1/namespaces/{namespace}/pods?labelSelector={selector}"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list_resp.status(), StatusCode::OK);
    let list_body = to_bytes(list_resp.into_body(), usize::MAX).await.unwrap();
    let list_rv: i64 = serde_json::from_slice::<serde_json::Value>(&list_body)
        .unwrap()
        .pointer("/metadata/resourceVersion")
        .and_then(|value| value.as_str())
        .and_then(|value| value.parse().ok())
        .expect("list must return a resourceVersion");

    let watch_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "/api/v1/namespaces/{namespace}/pods?watch=true&resourceVersion={list_rv}&labelSelector={selector}&allowWatchBookmarks=true"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(watch_resp.status(), StatusCode::OK);

    let watch_stream = watch_resp.into_body().into_data_stream();
    let first_pod_body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": first_pod,
            "namespace": namespace,
            "labels": {"app": "guestbook", "tier": "frontend"}
        },
        "spec": {
            "containers": [{
                "name": "c",
                "image": "registry.k8s.io/pause:3.10.1"
            }]
        }
    });
    let create_first = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/namespaces/{namespace}/pods"))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&first_pod_body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_first.status(), StatusCode::CREATED);

    let patch_first = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/api/v1/namespaces/{namespace}/pods/{first_pod}"))
                .header("content-type", "application/merge-patch+json")
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "metadata": {
                            "annotations": {"watch-reconnect": "phase-one"}
                        }
                    }))
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(patch_first.status(), StatusCode::OK);

    for i in 0..20 {
        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": format!("bookmark-noise-{i}"),
                "namespace": namespace,
                "labels": {"app": "noise", "tier": "backend"}
            },
            "spec": {
                "containers": [{"name": "c", "image": "registry.k8s.io/pause:3.10.1"}]
            }
        });
        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/v1/namespaces/{namespace}/pods"))
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&pod).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
    }

    let mut first_events: Vec<String> = Vec::new();
    let mut resume_rv: Option<i64> = None;
    let mut first_events_max_rv: i64 = 0;
    let mut watch_stream = watch_stream;
    let namespace_match = namespace;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while first_events.len() < 2 || resume_rv.is_none() {
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        let chunk = tokio::time::timeout(Duration::from_millis(500), watch_stream.next())
            .await
            .expect("watch stream should continue during reconnect test")
            .expect("watch stream should yield data")
            .expect("watch stream chunk error")
            .to_vec();
        let text = String::from_utf8(chunk).unwrap();
        for line in text.lines().filter(|line| !line.trim().is_empty()) {
            let event = match serde_json::from_str::<serde_json::Value>(line) {
                Ok(event) => event,
                Err(_) => continue,
            };
            let event_type = event
                .get("type")
                .and_then(|t| t.as_str())
                .unwrap_or("")
                .to_string();
            let name = event
                .pointer("/object/metadata/name")
                .and_then(|value| value.as_str())
                .unwrap_or("");
            let event_namespace = event
                .pointer("/object/metadata/namespace")
                .and_then(|value| value.as_str())
                .unwrap_or("");

            match event_type.as_str() {
                "ADDED" | "MODIFIED" => {
                    if name == first_pod && event_namespace == namespace_match {
                        first_events.push(event_type);
                        let rv = event
                            .pointer("/object/metadata/resourceVersion")
                            .and_then(|value| value.as_str())
                            .and_then(|value| value.parse::<i64>().ok())
                            .unwrap_or(0);
                        first_events_max_rv = first_events_max_rv.max(rv);
                    }
                }
                "BOOKMARK" => {
                    if let Some(rv) = event
                        .pointer("/object/metadata/resourceVersion")
                        .and_then(|value| value.as_str())
                        .and_then(|value| value.parse::<i64>().ok())
                    {
                        resume_rv = Some(resume_rv.unwrap_or(0).max(rv));
                    }
                }
                _ => {}
            }
        }
    }
    drop(watch_stream);

    let resume_rv = resume_rv.expect("watch must emit a bookmark while reconnect test is active");
    assert_eq!(
        first_events,
        ["ADDED", "MODIFIED"],
        "first scoped watch must deliver create then modify for the in-scope pod"
    );
    assert!(
        first_events_max_rv > 0,
        "scoped watch must capture in-scope event resourceVersions"
    );

    let reconnect_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "/api/v1/namespaces/{namespace}/pods?watch=true&resourceVersion={resume_rv}&labelSelector={selector}&allowWatchBookmarks=true"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(reconnect_resp.status(), StatusCode::OK);

    let mut reconnect_stream = reconnect_resp.into_body().into_data_stream();
    let second_pod_body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": second_pod,
            "namespace": namespace,
            "labels": {"app": "guestbook", "tier": "frontend"}
        },
        "spec": {
            "containers": [{
                "name": "c",
                "image": "registry.k8s.io/pause:3.10.1"
            }]
        }
    });
    let create_second = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/namespaces/{namespace}/pods"))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&second_pod_body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_second.status(), StatusCode::CREATED);

    let patch_second = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/api/v1/namespaces/{namespace}/pods/{second_pod}"))
                .header("content-type", "application/merge-patch+json")
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "metadata": {
                            "annotations": {"watch-reconnect": "phase-two"}
                        }
                    }))
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(patch_second.status(), StatusCode::OK);

    let mut second_events: Vec<String> = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while second_events.len() < 2 {
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        let chunk = tokio::time::timeout(Duration::from_millis(500), reconnect_stream.next())
            .await
            .expect("reconnected watch should remain active")
            .expect("reconnected watch should emit frames")
            .expect("reconnected watch chunk error")
            .to_vec();
        let text = String::from_utf8(chunk).unwrap();
        for line in text.lines().filter(|line| !line.trim().is_empty()) {
            let event = match serde_json::from_str::<serde_json::Value>(line) {
                Ok(event) => event,
                Err(_) => continue,
            };
            let event_type = event.get("type").and_then(|t| t.as_str()).unwrap_or("");
            let name = event
                .pointer("/object/metadata/name")
                .and_then(|value| value.as_str())
                .unwrap_or("");
            let event_namespace = event
                .pointer("/object/metadata/namespace")
                .and_then(|value| value.as_str())
                .unwrap_or("");
            if (event_type == "ADDED" || event_type == "MODIFIED")
                && name == second_pod
                && event_namespace == namespace_match
            {
                second_events.push(event_type.to_string());
            }
        }
    }

    assert_eq!(
        second_events,
        ["ADDED", "MODIFIED"],
        "reconnected scoped watch must deliver in-order create and modify events"
    );
}

/// Regression for the `[sig-cli] Kubectl client Guestbook application ... should
/// create and stop a working application` readiness timeout under multinode
/// netns latency + packet loss (canary run `/tmp/2r.log` run 25, build
/// `e9bf241`). Reproduces the scoped-watch delivery stall: a positive-RV label
/// selector watch over frontend pods must deliver a Running+Ready MODIFIED for
/// EVERY in-scope pod even while heavy out-of-scope write churn advances the
/// cursor high-water RV far past the scoped delivery frontier.
///
/// Closes the gap left by `test_label_selector_watch_catchup_filters_persisted_events`,
/// `test_bookmark_reports_caught_up_rv_not_global_collection_rv`, and
/// `test_scoped_watch_reconnect_from_bookmark_delivers_subsequent_in_scope_events`:
/// none covers the failed Guestbook shape of positive-RV selector watch +
/// multi-pod readiness status transitions + concurrent out-of-scope churn +
/// bookmark/reconnect. Writes are driven concurrently to recreate the
/// out-of-order broadcast delivery + floor advancement the lossy multinode
/// harness induces (the `klights::watch_diag` "scoped bookmark held at delivered
/// scoped rv" / "cursor dropped an undelivered event (floor advanced past it)"
/// signature). If a BOOKMARK lands before all in-scope statuses are delivered,
/// resuming from it must deliver the remainder or return 410 Expired -- never a
/// quiet bookmark-only stall.
#[tokio::test]
async fn test_positive_rv_selector_pod_watch_delivers_all_ready_statuses_under_churn() {
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use futures::StreamExt;
    use serde_json::json;
    use std::time::Duration;
    use tower::ServiceExt;

    fn mk(method: &str, uri: String, body: Option<Vec<u8>>) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .body(body.map(Body::from).unwrap_or_else(Body::empty))
            .unwrap()
    }

    let (app, db) = build_test_router_with_db().await;
    let namespace = "gb-scoped-watch-churn".to_string();
    let noise_namespace = "gb-scoped-watch-noise".to_string();
    let selector = "app%3Dguestbook%2Ctier%3Dfrontend";
    let nodes = ["mn-controlplane1", "mn-replica", "mn-worker"];
    // (pod name, node) -- distinct spec.nodeName like the failed run.
    let frontend_pods: [(&str, &str); 3] = [
        ("fe-controlplane1", "mn-controlplane1"),
        ("fe-replica", "mn-replica"),
        ("fe-worker", "mn-worker"),
    ];

    // Register the three nodes so spec.nodeName is honored on pod create.
    for node in nodes {
        db.create_resource(
            "v1",
            "Node",
            None,
            node,
            json!({"apiVersion":"v1","kind":"Node","metadata":{"name":node},"spec":{},"status":{}}),
        )
        .await
        .unwrap();
    }
    for ns in [&namespace, &noise_namespace] {
        let resp = app
            .clone()
            .oneshot(mk(
                "POST",
                "/api/v1/namespaces".to_string(),
                Some(
                    format!(
                        r#"{{"apiVersion":"v1","kind":"Namespace","metadata":{{"name":"{ns}"}}}}"#
                    )
                    .into_bytes(),
                ),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
    }
    // Three frontend pods (Pending) with distinct nodeNames, present before the list.
    for (pod, node) in frontend_pods {
        let body = json!({
            "apiVersion":"v1","kind":"Pod",
            "metadata":{"name":pod,"namespace":namespace,"labels":{"app":"guestbook","tier":"frontend"}},
            "spec":{"nodeName":node,"containers":[{"name":"c","image":"registry.k8s.io/pause:3.10.1"}]}
        });
        let resp = app
            .clone()
            .oneshot(mk(
                "POST",
                format!("/api/v1/namespaces/{namespace}/pods"),
                Some(serde_json::to_vec(&body).unwrap()),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
    }

    // Initial selector LIST with resourceVersion=0; capture the collection RV.
    let list_resp = app
        .clone()
        .oneshot(mk(
            "GET",
            format!("/api/v1/namespaces/{namespace}/pods?labelSelector={selector}"),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(list_resp.status(), StatusCode::OK);
    let list_rv: i64 = serde_json::from_slice::<serde_json::Value>(
        &to_bytes(list_resp.into_body(), usize::MAX).await.unwrap(),
    )
    .unwrap()
    .pointer("/metadata/resourceVersion")
    .and_then(|v| v.as_str())
    .and_then(|s| s.parse().ok())
    .expect("list must return a resourceVersion");

    let ready_patch = json!({"status":{"phase":"Running","podIP":"10.42.0.1","conditions":[
        {"type":"Ready","status":"True","reason":"PodReady"},
        {"type":"ContainersReady","status":"True","reason":"PodReady"}
    ]}});

    // CATCH-UP PATH: commit one frontend Ready status AFTER the list but BEFORE
    // the watch opens, so it is served from the watch catch-up window (an
    // in-scope event committed with rv > list_rv before the watch streams).
    let resp = app
        .clone()
        .oneshot(mk(
            "PATCH",
            format!(
                "/api/v1/namespaces/{namespace}/pods/{}/status",
                frontend_pods[0].0
            ),
            Some(serde_json::to_vec(&ready_patch).unwrap()),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // CATCH-UP WINDOW CHURN: commit out-of-scope pods AFTER the in-scope
    // Ready status but BEFORE the watch opens, so they land in the watch's
    // `list_resources_modified_since` catch-up. The catch-up advances
    // `initial_list_rv` (the cursor floor) from these out-of-scope events
    // before selector filtering; the in-scope Ready status (lower RV, ASC
    // order) must still be delivered, not dropped by the inflated floor.
    // Same-namespace noise drives the floor; other-namespace noise exercises
    // the cluster-wide broadcast vs namespaced replay scope.
    for i in 0..10 {
        let pod = json!({"apiVersion":"v1","kind":"Pod",
            "metadata":{"name":format!("noise-catchup-same-{i}"),"namespace":namespace.clone(),"labels":{"app":"noise","tier":"backend"}},
            "spec":{"containers":[{"name":"c","image":"registry.k8s.io/pause:3.10.1"}]}});
        let r = app
            .clone()
            .oneshot(mk(
                "POST",
                format!("/api/v1/namespaces/{namespace}/pods"),
                Some(serde_json::to_vec(&pod).unwrap()),
            ))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::CREATED);
    }
    for i in 0..6 {
        let pod = json!({"apiVersion":"v1","kind":"Pod",
            "metadata":{"name":format!("noise-catchup-other-{i}"),"namespace":noise_namespace.clone(),"labels":{"app":"noise","tier":"backend"}},
            "spec":{"containers":[{"name":"c","image":"registry.k8s.io/pause:3.10.1"}]}});
        let r = app
            .clone()
            .oneshot(mk(
                "POST",
                format!("/api/v1/namespaces/{noise_namespace}/pods"),
                Some(serde_json::to_vec(&pod).unwrap()),
            ))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::CREATED);
    }

    // Open the selector WATCH from list_rv with bookmarks enabled.
    let watch_resp = app
        .clone()
        .oneshot(mk(
            "GET",
            format!(
                "/api/v1/namespaces/{namespace}/pods?watch=true&resourceVersion={list_rv}&labelSelector={selector}&allowWatchBookmarks=true"
            ),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(watch_resp.status(), StatusCode::OK);
    let mut watch_stream = watch_resp.into_body().into_data_stream();

    // LIVE PATH + churn: drive the other two frontend Ready statuses and 24+
    // out-of-scope pod writes concurrently so broadcasts interleave out of RV
    // order and the cursor high-water advances past the scoped frontier.
    let mut write_handles = Vec::new();
    for (pod, _) in frontend_pods.iter().skip(1) {
        let app = app.clone();
        let namespace = namespace.clone();
        let patch = ready_patch.clone();
        let pod = (*pod).to_string();
        write_handles.push(tokio::spawn(async move {
            let _ = app
                .clone()
                .oneshot(mk(
                    "PATCH",
                    format!("/api/v1/namespaces/{namespace}/pods/{pod}/status"),
                    Some(serde_json::to_vec(&patch).unwrap()),
                ))
                .await;
        }));
    }
    for i in 0..12 {
        let app = app.clone();
        let namespace = namespace.clone();
        write_handles.push(tokio::spawn(async move {
            let pod = json!({"apiVersion":"v1","kind":"Pod",
                "metadata":{"name":format!("noise-same-{i}"),"namespace":namespace.clone(),"labels":{"app":"noise","tier":"backend"}},
                "spec":{"containers":[{"name":"c","image":"registry.k8s.io/pause:3.10.1"}]}});
            let _ = app
                .clone()
                .oneshot(mk(
                    "POST",
                    format!("/api/v1/namespaces/{namespace}/pods"),
                    Some(serde_json::to_vec(&pod).unwrap()),
                ))
                .await;
        }));
    }
    for i in 0..12 {
        let app = app.clone();
        let noise_namespace = noise_namespace.clone();
        write_handles.push(tokio::spawn(async move {
            let pod = json!({"apiVersion":"v1","kind":"Pod",
                "metadata":{"name":format!("noise-other-{i}"),"namespace":noise_namespace.clone(),"labels":{"app":"noise","tier":"backend"}},
                "spec":{"containers":[{"name":"c","image":"registry.k8s.io/pause:3.10.1"}]}});
            let _ = app
                .clone()
                .oneshot(mk(
                    "POST",
                    format!("/api/v1/namespaces/{noise_namespace}/pods"),
                    Some(serde_json::to_vec(&pod).unwrap()),
                ))
                .await;
        }));
    }

    // Collect, per frontend pod, whether a Running+Ready MODIFIED was observed,
    // and the highest bookmark RV seen (resume point if a reconnect is needed).
    let mut ready_pods: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut bookmark_rv: Option<i64> = None;
    let mut saw_expired = false;
    let frontend_names: std::collections::HashSet<&str> =
        frontend_pods.iter().map(|(n, _)| *n).collect();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while ready_pods.len() < 3 && tokio::time::Instant::now() < deadline {
        let chunk = match tokio::time::timeout(Duration::from_secs(2), watch_stream.next()).await {
            Ok(Some(Ok(bytes))) => bytes,
            Ok(Some(Err(_))) | Ok(None) => break,
            Err(_) => continue,
        };
        for line in String::from_utf8(chunk.to_vec())
            .unwrap_or_default()
            .lines()
            .filter(|l| !l.trim().is_empty())
        {
            let event = match serde_json::from_str::<serde_json::Value>(line) {
                Ok(e) => e,
                Err(_) => continue,
            };
            match event.get("type").and_then(|t| t.as_str()).unwrap_or("") {
                "ERROR" => {
                    if event
                        .pointer("/object/code")
                        .and_then(|c| c.as_i64())
                        .is_some_and(|c| c == 410)
                    {
                        saw_expired = true;
                    }
                }
                "BOOKMARK" => {
                    if let Some(rv) = event
                        .pointer("/object/metadata/resourceVersion")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<i64>().ok())
                    {
                        bookmark_rv = Some(bookmark_rv.unwrap_or(0).max(rv));
                    }
                }
                "MODIFIED" => {
                    let name = event
                        .pointer("/object/metadata/name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    if frontend_names.contains(name)
                        && event
                            .pointer("/object/status/phase")
                            .and_then(|v| v.as_str())
                            .is_some_and(|p| p == "Running")
                        && event
                            .pointer("/object/status/conditions")
                            .and_then(|v| v.as_array())
                            .is_some_and(|conds| {
                                conds.iter().any(|c| {
                                    c.pointer("/type").and_then(|t| t.as_str()) == Some("Ready")
                                        && c.pointer("/status").and_then(|s| s.as_str())
                                            == Some("True")
                                })
                            })
                    {
                        ready_pods.insert(name.to_string());
                    }
                }
                _ => {}
            }
        }
    }
    drop(watch_stream);

    // If all three arrived on the first watch, the scoped delivery held.
    if ready_pods.len() < 3 && !saw_expired {
        // The stream stalled with undelivered in-scope status. A correct server
        // must let a client resume from the last scoped BOOKMARK and either
        // receive the missing events or a 410 Expired -- never a quiet stall.
        let resume_rv = match bookmark_rv {
            Some(rv) => rv,
            None => {
                for h in write_handles {
                    let _ = h.await;
                }
                panic!(
                    "scoped watch stalled with {}/3 frontend Ready statuses and no BOOKMARK to resume from",
                    ready_pods.len()
                );
            }
        };
        let reconnect_resp = app
            .clone()
            .oneshot(mk(
                "GET",
                format!(
                    "/api/v1/namespaces/{namespace}/pods?watch=true&resourceVersion={resume_rv}&labelSelector={selector}&allowWatchBookmarks=true"
                ),
                None,
            ))
            .await
            .unwrap();
        assert_eq!(reconnect_resp.status(), StatusCode::OK);
        let mut reconnect_stream = reconnect_resp.into_body().into_data_stream();
        let reconnect_deadline = tokio::time::Instant::now() + Duration::from_secs(8);
        while ready_pods.len() < 3
            && !saw_expired
            && tokio::time::Instant::now() < reconnect_deadline
        {
            let chunk =
                match tokio::time::timeout(Duration::from_secs(2), reconnect_stream.next()).await {
                    Ok(Some(Ok(bytes))) => bytes,
                    Ok(Some(Err(_))) | Ok(None) => break,
                    Err(_) => continue,
                };
            for line in String::from_utf8(chunk.to_vec())
                .unwrap_or_default()
                .lines()
                .filter(|l| !l.trim().is_empty())
            {
                let event = match serde_json::from_str::<serde_json::Value>(line) {
                    Ok(e) => e,
                    Err(_) => continue,
                };
                match event.get("type").and_then(|t| t.as_str()).unwrap_or("") {
                    "ERROR" => {
                        if event
                            .pointer("/object/code")
                            .and_then(|c| c.as_i64())
                            .is_some_and(|c| c == 410)
                        {
                            saw_expired = true;
                        }
                    }
                    "BOOKMARK" => {}
                    "MODIFIED" => {
                        let name = event
                            .pointer("/object/metadata/name")
                            .and_then(|v| v.as_str())
                            .unwrap_or("");
                        if frontend_names.contains(name)
                            && event
                                .pointer("/object/status/phase")
                                .and_then(|v| v.as_str())
                                .is_some_and(|p| p == "Running")
                            && event
                                .pointer("/object/status/conditions")
                                .and_then(|v| v.as_array())
                                .is_some_and(|conds| {
                                    conds.iter().any(|c| {
                                        c.pointer("/type").and_then(|t| t.as_str()) == Some("Ready")
                                            && c.pointer("/status").and_then(|s| s.as_str())
                                                == Some("True")
                                    })
                                })
                        {
                            ready_pods.insert(name.to_string());
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    for h in write_handles {
        let _ = h.await;
    }

    assert!(
        ready_pods.len() == 3 || saw_expired,
        "scoped selector watch must deliver Running+Ready MODIFIED for all 3 frontend pods \
         (got {}/3: {:?}) or return 410 Expired on resume -- never a quiet bookmark-only stall",
        ready_pods.len(),
        ready_pods
    );
}

/// Regression: a resourceVersion-less (`resourceVersion=""`) label-selector
/// watch must deliver the ADDED of a matching object created AFTER the watch
/// establishes, even while unrelated writes inflate the global resourceVersion.
/// Previously the live floor was anchored to the baseline list's *global*
/// collection rv (read after subscribing); under load that raced ahead of the
/// new object's rv and dropped its ADDED (`rv <= floor`) — the flaky
/// `[sig-api-machinery] Watchers should be able to restart watching from the
/// last resource version observed` failure. Existing matches must still arrive
/// as ADDED exactly once (no duplicate from the lowered floor).
#[tokio::test]
async fn test_rv_less_selector_watch_delivers_live_added_under_rv_inflation() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use futures::StreamExt;
    use serde_json::json;
    use std::time::Duration;
    use tower::ServiceExt;

    let app = build_test_router().await;
    let namespace = "rvless-live-added";
    let app2 = app.clone();
    let mk = |method: &str, uri: String, body: Option<Vec<u8>>| {
        let mut b = Request::builder().method(method).uri(uri);
        if body.is_some() {
            b = b.header("content-type", "application/json");
        }
        b.body(body.map(Body::from).unwrap_or(Body::empty()))
            .unwrap()
    };

    let ns_resp = app
        .clone()
        .oneshot(mk(
            "POST",
            "/api/v1/namespaces".into(),
            Some(format!(r#"{{"apiVersion":"v1","kind":"Namespace","metadata":{{"name":"{namespace}"}}}}"#).into_bytes()),
        ))
        .await
        .unwrap();
    assert_eq!(ns_resp.status(), StatusCode::CREATED);

    // An existing match present before the watch opens.
    let pre = json!({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"pre","namespace":namespace,"labels":{"team":"x"}}});
    assert_eq!(
        app.clone()
            .oneshot(mk(
                "POST",
                format!("/api/v1/namespaces/{namespace}/configmaps"),
                Some(serde_json::to_vec(&pre).unwrap())
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::CREATED
    );

    // Open an rv-less label-selector watch.
    let watch_resp = app
        .oneshot(mk(
            "GET",
            format!("/api/v1/namespaces/{namespace}/configmaps?watch=true&labelSelector=team%3Dx"),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(watch_resp.status(), StatusCode::OK);
    let mut stream = watch_resp.into_body().into_data_stream();

    // Drain the baseline ADDED for "pre".
    let mut saw_pre = false;
    for _ in 0..6 {
        let chunk = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("baseline")
            .expect("stream")
            .expect("chunk");
        for line in String::from_utf8(chunk.to_vec())
            .unwrap()
            .lines()
            .filter(|l| !l.trim().is_empty())
        {
            let ev: serde_json::Value = serde_json::from_str(line).unwrap();
            if ev["type"] == "ADDED" && ev["object"]["metadata"]["name"] == "pre" {
                saw_pre = true;
            }
        }
        if saw_pre {
            break;
        }
    }
    assert!(
        saw_pre,
        "existing match must be delivered as ADDED from baseline"
    );

    // Inflate the global rv with unrelated (non-matching) writes.
    for i in 0..6 {
        let cm = json!({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":format!("noise-{i}"),"namespace":namespace},"data":{"k":"v"}});
        assert_eq!(
            app2.clone()
                .oneshot(mk(
                    "POST",
                    format!("/api/v1/namespaces/{namespace}/configmaps"),
                    Some(serde_json::to_vec(&cm).unwrap())
                ))
                .await
                .unwrap()
                .status(),
            StatusCode::CREATED
        );
    }

    // Create a matching object AFTER the watch established and after inflation.
    let live = json!({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"live","namespace":namespace,"labels":{"team":"x"}}});
    assert_eq!(
        app2.clone()
            .oneshot(mk(
                "POST",
                format!("/api/v1/namespaces/{namespace}/configmaps"),
                Some(serde_json::to_vec(&live).unwrap())
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::CREATED
    );

    // The live match's ADDED must be delivered (not dropped by an inflated floor).
    let mut saw_live = false;
    let mut pre_added_count = 0;
    for _ in 0..10 {
        let chunk = match tokio::time::timeout(Duration::from_secs(3), stream.next()).await {
            Ok(Some(Ok(c))) => c,
            _ => break,
        };
        for line in String::from_utf8(chunk.to_vec())
            .unwrap()
            .lines()
            .filter(|l| !l.trim().is_empty())
        {
            let ev: serde_json::Value = serde_json::from_str(line).unwrap();
            if ev["type"] == "ADDED" && ev["object"]["metadata"]["name"] == "live" {
                saw_live = true;
            }
            if ev["type"] == "ADDED" && ev["object"]["metadata"]["name"] == "pre" {
                pre_added_count += 1;
            }
        }
        if saw_live {
            break;
        }
    }
    assert!(
        saw_live,
        "live match created after establishment must be delivered as ADDED despite rv inflation"
    );
    assert_eq!(
        pre_added_count, 0,
        "existing match must not be re-delivered as ADDED by the lowered floor"
    );
}

#[tokio::test]
async fn test_rv_less_selector_watch_delivers_live_added_below_establishment_floor() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use futures::StreamExt;
    use serde_json::json;
    use std::sync::Arc;
    use std::time::Duration;
    use tower::ServiceExt;

    let (app, db) = build_test_router_with_db().await;
    let namespace = "rvless-low-added";
    let mk = |method: &str, uri: String, body: Option<Vec<u8>>| {
        let mut request = Request::builder().method(method).uri(uri);
        if body.is_some() {
            request = request.header("content-type", "application/json");
        }
        request
            .body(body.map(Body::from).unwrap_or_else(Body::empty))
            .unwrap()
    };

    let ns_resp = app
        .clone()
        .oneshot(mk(
            "POST",
            "/api/v1/namespaces".to_string(),
            Some(
                serde_json::to_vec(&json!({
                    "apiVersion": "v1",
                    "kind": "Namespace",
                    "metadata": {"name": namespace}
                }))
                .unwrap(),
            ),
        ))
        .await
        .unwrap();
    assert_eq!(ns_resp.status(), StatusCode::CREATED);

    let floor = db
        .advance_resource_version_after(
            db.get_current_resource_version()
                .await
                .unwrap()
                .saturating_add(20),
        )
        .await
        .unwrap();

    let watch_resp = app
        .oneshot(mk(
            "GET",
            format!(
                "/api/v1/namespaces/{namespace}/configmaps?watch=true&labelSelector=watch-this-configmap%20in%20%28multiple-watchers-A%29"
            ),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(watch_resp.status(), StatusCode::OK);
    let mut stream = watch_resp.into_body().into_data_stream();

    // Poll once so the stream completes its empty baseline list and parks on
    // the live cursor before the lower-RV matching object is created.
    let _ = tokio::time::timeout(Duration::from_millis(200), stream.next()).await;

    let create_rv = floor - 1;
    db.apply_log_apply_commit(crate::log_apply::LogApplyCommit::put_resource(
        &crate::datastore::Resource {
            id: 0,
            api_version: "v1".into(),
            kind: "ConfigMap".into(),
            namespace: Some(namespace.to_string()),
            name: "e2e-watch-test-configmap-a".into(),
            uid: "cm-low-added".into(),
            resource_version: create_rv,
            data: Arc::new(json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "name": "e2e-watch-test-configmap-a",
                    "namespace": namespace,
                    "uid": "cm-low-added",
                    "labels": {"watch-this-configmap": "multiple-watchers-A"}
                }
            })),
        },
    ))
    .await
    .expect("replicated create below establishment floor");

    let mut saw_added = false;
    for _ in 0..6 {
        let chunk = match tokio::time::timeout(Duration::from_millis(500), stream.next()).await {
            Ok(Some(Ok(chunk))) => chunk,
            _ => break,
        };
        for line in chunk.split(|byte| *byte == b'\n') {
            if line.is_empty() {
                continue;
            }
            let event: serde_json::Value = serde_json::from_slice(line).unwrap();
            if event["type"] == "ADDED"
                && event
                    .pointer("/object/metadata/name")
                    .and_then(|value| value.as_str())
                    == Some("e2e-watch-test-configmap-a")
            {
                saw_added = true;
                break;
            }
        }
        if saw_added {
            break;
        }
    }
    assert!(
        saw_added,
        "rv-less selector watch must deliver live ADDED rv={create_rv} below establishment floor rv={floor}"
    );
}

/// Regression: a resourceVersion-less (`resourceVersion=""`) field-selector
/// watch on a *custom resource* must deliver the ADDED of a matching object
/// whose resourceVersion is at or below the watch's establishment floor —
/// i.e. an object committed before the watch read the global rv. This is the
/// flaky `[sig-api-machinery] CustomResourceFieldSelectors MUST list and watch
/// custom resources matching the field selector [Conformance]` failure: under
/// parallel load (and the harness's added replication latency) an unrelated
/// write inflates the global rv past a matching CR's rv, so a CR watch that
/// anchored its floor to that global rv (`requested_rv = floor`) dropped the
/// CR's ADDED — caught by neither catch-up (`rv <= floor`) nor live (the
/// post-commit broadcast arrives below the floor). The label-selector watch
/// path solved this with a baseline ADDED list; the custom-resource watch
/// path must do the same.
#[tokio::test]
async fn test_rv_less_field_selector_cr_watch_delivers_match_below_floor() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use futures::StreamExt;
    use serde_json::json;
    use std::time::Duration;
    use tower::ServiceExt;

    let app = build_test_router().await;
    let namespace = "crd-fieldsel-watch";
    let mk = |method: &str, uri: String, body: Option<Vec<u8>>| {
        let mut b = Request::builder().method(method).uri(uri);
        if body.is_some() {
            b = b.header("content-type", "application/json");
        }
        b.body(body.map(Body::from).unwrap_or(Body::empty()))
            .unwrap()
    };

    // Namespace for the custom resources.
    let ns_resp = app
        .clone()
        .oneshot(mk(
            "POST",
            "/api/v1/namespaces".into(),
            Some(
                format!(
                    r#"{{"apiVersion":"v1","kind":"Namespace","metadata":{{"name":"{namespace}"}}}}"#
                )
                .into_bytes(),
            ),
        ))
        .await
        .unwrap();
    assert_eq!(ns_resp.status(), StatusCode::CREATED);

    // Register a Namespaced CRD.
    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "selws.selwatch.example.com"},
        "spec": {
            "group": "selwatch.example.com",
            "scope": "Namespaced",
            "names": {"plural": "selws", "singular": "selw", "kind": "Selw"},
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {"openAPIV3Schema": {"type": "object", "x-kubernetes-preserve-unknown-fields": true}}
            }]
        }
    });
    assert_eq!(
        app.clone()
            .oneshot(mk(
                "POST",
                "/apis/apiextensions.k8s.io/v1/customresourcedefinitions".into(),
                Some(serde_json::to_vec(&crd).unwrap()),
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::CREATED
    );

    // A matching CR present BEFORE the watch opens.
    let pre = json!({
        "apiVersion": "selwatch.example.com/v1",
        "kind": "Selw",
        "metadata": {"name": "pre", "namespace": namespace}
    });
    assert_eq!(
        app.clone()
            .oneshot(mk(
                "POST",
                format!("/apis/selwatch.example.com/v1/namespaces/{namespace}/selws"),
                Some(serde_json::to_vec(&pre).unwrap()),
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::CREATED
    );

    // Inflate the global resourceVersion with unrelated writes so the CR
    // watch's establishment floor races ahead of "pre"'s rv.
    for i in 0..6 {
        let cm = json!({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":format!("noise-{i}"),"namespace":namespace},"data":{"k":"v"}});
        assert_eq!(
            app.clone()
                .oneshot(mk(
                    "POST",
                    format!("/api/v1/namespaces/{namespace}/configmaps"),
                    Some(serde_json::to_vec(&cm).unwrap()),
                ))
                .await
                .unwrap()
                .status(),
            StatusCode::CREATED
        );
    }

    // Open an rv-less field-selector watch on the custom resource. The field
    // selector `metadata.namespace=<ns>` is always supported and makes this a
    // selector watch; every CR in the namespace matches it.
    let watch_resp = app
        .clone()
        .oneshot(mk(
            "GET",
            format!(
                "/apis/selwatch.example.com/v1/namespaces/{namespace}/selws?watch=true&fieldSelector=metadata.namespace%3D{namespace}"
            ),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(watch_resp.status(), StatusCode::OK);
    let mut stream = watch_resp.into_body().into_data_stream();

    // "pre" (rv <= establishment floor) must be delivered as ADDED from the
    // baseline list. On the buggy path it is dropped entirely.
    let mut saw_pre = false;
    for _ in 0..8 {
        let chunk = match tokio::time::timeout(Duration::from_secs(3), stream.next()).await {
            Ok(Some(Ok(c))) => c,
            _ => break,
        };
        for line in String::from_utf8(chunk.to_vec())
            .unwrap()
            .lines()
            .filter(|l| !l.trim().is_empty())
        {
            let ev: serde_json::Value = serde_json::from_str(line).unwrap();
            if ev["type"] == "ADDED" && ev["object"]["metadata"]["name"] == "pre" {
                saw_pre = true;
            }
        }
        if saw_pre {
            break;
        }
    }
    assert!(
        saw_pre,
        "matching CR created before the watch (rv <= floor) must be delivered as ADDED"
    );

    // A matching CR created AFTER establishment must also arrive (live path),
    // exactly once, with no duplicate ADDED for "pre".
    let live = json!({
        "apiVersion": "selwatch.example.com/v1",
        "kind": "Selw",
        "metadata": {"name": "live", "namespace": namespace}
    });
    assert_eq!(
        app.clone()
            .oneshot(mk(
                "POST",
                format!("/apis/selwatch.example.com/v1/namespaces/{namespace}/selws"),
                Some(serde_json::to_vec(&live).unwrap()),
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::CREATED
    );

    let mut saw_live = false;
    let mut pre_added_count = if saw_pre { 1 } else { 0 };
    for _ in 0..10 {
        let chunk = match tokio::time::timeout(Duration::from_secs(3), stream.next()).await {
            Ok(Some(Ok(c))) => c,
            _ => break,
        };
        for line in String::from_utf8(chunk.to_vec())
            .unwrap()
            .lines()
            .filter(|l| !l.trim().is_empty())
        {
            let ev: serde_json::Value = serde_json::from_str(line).unwrap();
            if ev["type"] == "ADDED" && ev["object"]["metadata"]["name"] == "live" {
                saw_live = true;
            }
            if ev["type"] == "ADDED" && ev["object"]["metadata"]["name"] == "pre" {
                pre_added_count += 1;
            }
        }
        if saw_live {
            break;
        }
    }
    assert!(
        saw_live,
        "matching CR created after establishment must be delivered as ADDED (live)"
    );
    assert_eq!(
        pre_added_count, 1,
        "the pre-existing match must be delivered as ADDED exactly once"
    );
}

#[tokio::test]
async fn test_selector_watch_from_list_rv_delivers_baseline_delete_below_floor() {
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use futures::StreamExt;
    use serde_json::json;
    use std::sync::Arc;
    use std::time::Duration;
    use tower::ServiceExt;

    let (app, db) = build_test_router_with_db().await;
    let namespace = "selector-low-rv-delete";
    let mk = |method: &str, uri: String, body: Option<Vec<u8>>| {
        let mut request = Request::builder().method(method).uri(uri);
        if body.is_some() {
            request = request.header("content-type", "application/json");
        }
        request
            .body(body.map(Body::from).unwrap_or_else(Body::empty))
            .unwrap()
    };

    let ns_resp = app
        .clone()
        .oneshot(mk(
            "POST",
            "/api/v1/namespaces".to_string(),
            Some(
                serde_json::to_vec(&json!({
                    "apiVersion": "v1",
                    "kind": "Namespace",
                    "metadata": {"name": namespace}
                }))
                .unwrap(),
            ),
        ))
        .await
        .unwrap();
    assert_eq!(ns_resp.status(), StatusCode::CREATED);

    let base_rv = db.get_current_resource_version().await.unwrap();
    let create_rv = base_rv + 1;
    let cm = crate::datastore::Resource {
        id: 0,
        api_version: "v1".into(),
        kind: "ConfigMap".into(),
        namespace: Some(namespace.into()),
        name: "watched".into(),
        uid: "cm-low-rv-delete".into(),
        resource_version: create_rv,
        data: Arc::new(json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "watched",
                "namespace": namespace,
                "uid": "cm-low-rv-delete",
                "labels": {"race": "below-floor"}
            },
            "data": {"k": "v"}
        })),
    };
    db.apply_log_apply_commit(crate::log_apply::LogApplyCommit::put_resource(&cm))
        .await
        .expect("replicated create apply");

    let inflated_rv = db
        .advance_resource_version_after(create_rv + 20)
        .await
        .expect("inflate global rv");
    assert!(inflated_rv > create_rv);

    let list_resp = app
        .clone()
        .oneshot(mk(
            "GET",
            format!(
                "/api/v1/namespaces/{namespace}/configmaps?labelSelector=race%3Dbelow-floor&limit=500&resourceVersion=0"
            ),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(list_resp.status(), StatusCode::OK);
    let list_body = to_bytes(list_resp.into_body(), usize::MAX).await.unwrap();
    let list_json: serde_json::Value = serde_json::from_slice(&list_body).unwrap();
    assert_eq!(
        list_json
            .pointer("/items/0/metadata/name")
            .and_then(|value| value.as_str()),
        Some("watched")
    );
    let list_rv = list_json
        .pointer("/metadata/resourceVersion")
        .and_then(|value| value.as_str())
        .and_then(|value| value.parse::<i64>().ok())
        .expect("list resourceVersion");
    assert!(
        list_rv > create_rv,
        "test setup requires a collection rv above the baseline object rv"
    );

    let watch_resp = app
        .clone()
        .oneshot(mk(
            "GET",
            format!(
                "/api/v1/namespaces/{namespace}/configmaps?watch=true&resourceVersion={list_rv}&labelSelector=race%3Dbelow-floor"
            ),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(watch_resp.status(), StatusCode::OK);
    let mut stream = watch_resp.into_body().into_data_stream();
    let reader = tokio::spawn(async move {
        let mut seen = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while tokio::time::Instant::now() < deadline {
            let Ok(Some(Ok(chunk))) =
                tokio::time::timeout(Duration::from_millis(500), stream.next()).await
            else {
                continue;
            };
            for line in chunk.split(|byte| *byte == b'\n') {
                if line.is_empty() {
                    continue;
                }
                let event: serde_json::Value = serde_json::from_slice(line).unwrap();
                if event
                    .pointer("/object/metadata/name")
                    .and_then(|value| value.as_str())
                    != Some("watched")
                {
                    continue;
                }
                let ty = event["type"].as_str().unwrap_or("").to_string();
                seen.push(ty.clone());
                if ty == "DELETED" {
                    return seen;
                }
            }
        }
        seen
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    let delete_rv = list_rv - 1;
    assert!(
        delete_rv > create_rv,
        "test setup requires delete rv above object rv but below list rv"
    );
    db.apply_log_apply_commit(crate::log_apply::LogApplyCommit::new(
        delete_rv,
        vec![crate::log_apply::LogApplyMutation::DeleteResource(
            crate::log_apply::LogApplyResourceKey {
                api_version: "v1".to_string(),
                kind: "ConfigMap".to_string(),
                namespace: Some(namespace.to_string()),
                name: "watched".to_string(),
                uid: "cm-low-rv-delete".to_string(),
                precondition_resource_version: None,
            },
        )],
    ))
    .await
    .expect("replicated delete below list rv");

    let seen = reader.await.expect("watch reader task");
    assert!(
        seen.iter().any(|ty| ty == "DELETED"),
        "selector watch from list rv={list_rv} must deliver delete rv={delete_rv} for baseline object; saw {seen:?}"
    );
}

/// Plain LIST must validate `resourceVersionMatch`: an unsupported value is a
/// 400, and a valid `Exact` match pins the reported list `resourceVersion`.
#[tokio::test]
async fn test_list_resource_version_match_validation_and_exact() {
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = build_test_router().await;

    // Unsupported resourceVersionMatch ⇒ 400 BadRequest.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/namespaces/default/configmaps?resourceVersion=1&resourceVersionMatch=Bogus")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    // resourceVersionMatch=Exact with rv=0 ⇒ 400.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/namespaces/default/configmaps?resourceVersion=0&resourceVersionMatch=Exact")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    // Valid Exact match pins the reported resourceVersion.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/namespaces/default/configmaps?resourceVersion=123&resourceVersionMatch=Exact")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let list: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(list["metadata"]["resourceVersion"], "123");
}

/// T1.2: `resourceVersionMatch=Exact` must serve a true historical snapshot
/// (the object's value at that rv, not the live value), negotiate protobuf like
/// any other list, and answer 410 Expired once the rv falls out of the retained
/// watch-event window.
#[tokio::test]
async fn test_list_exact_serves_snapshot_protobuf_and_410_when_too_old() {
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let (app, db) = build_test_router_with_db().await;

    let created = db
        .create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "cm",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "cm", "namespace": "default"},
                "data": {"k": "old"}
            }),
        )
        .await
        .unwrap();
    let rv_old = created.resource_version;
    db.update_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "cm",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "cm", "namespace": "default"},
            "data": {"k": "new"}
        }),
        rv_old,
    )
    .await
    .unwrap();

    let exact_uri = format!(
        "/api/v1/namespaces/default/configmaps?resourceVersion={rv_old}&resourceVersionMatch=Exact"
    );

    // Exact at the old rv returns the pre-update snapshot value.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(&exact_uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let list: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(list["metadata"]["resourceVersion"], rv_old.to_string());
    let items = list["items"].as_array().unwrap();
    let cm = items
        .iter()
        .find(|i| i["metadata"]["name"] == "cm")
        .expect("cm present in snapshot");
    assert_eq!(
        cm["data"]["k"], "old",
        "Exact must serve the historical value, not the current one: {list}"
    );

    // Protobuf negotiation on the Exact path.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(&exact_uri)
                .header("accept", "application/vnd.kubernetes.protobuf")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let ct = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    assert!(
        ct.contains("protobuf"),
        "content-type should be protobuf: {ct}"
    );
    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    assert_eq!(&body[..4], b"k8s\x00", "protobuf frame magic prefix");

    // Once the rv ages out of the retained window, Exact ⇒ 410 Expired.
    db.gc_watch_events(1, 1000).await.unwrap();
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(&exact_uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::GONE);
    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let status: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(status["reason"], "Expired");
    assert_eq!(status["code"], 410);
}

/// Regression for Sonobuoy chunking:
/// - a still-fresh continue token must not fail just because unrelated
///   watch_events churn compacted the historical snapshot window;
/// - once the server hands out an inconsistent continuation, later page tokens
///   must stay inconsistent instead of re-entering the historical snapshot path.
#[tokio::test]
async fn test_paginated_continue_falls_back_to_inconsistent_after_snapshot_compaction() {
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use base64::Engine as _;
    use tower::ServiceExt;

    fn decode_continue_token(token: &str) -> crate::api::query::ContinueTokenData {
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(token)
            .expect("continue token must be base64url");
        serde_json::from_slice(&bytes).expect("continue token must be JSON")
    }

    let (app, db) = build_test_router_with_db().await;

    let mut template_rvs = std::collections::HashMap::new();
    for i in 0..45 {
        let name = format!("template-{i:04}");
        let created = db
            .create_resource(
                "v1",
                "PodTemplate",
                Some("default"),
                &name,
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "PodTemplate",
                    "metadata": {"name": name, "namespace": "default"},
                    "template": {
                        "spec": {
                            "containers": [{"name": "main", "image": "registry.k8s.io/pause:3.10"}]
                        }
                    }
                }),
            )
            .await
            .unwrap();
        template_rvs.insert(name, created.resource_version);
    }

    let first_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/namespaces/default/podtemplates?limit=20")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(first_resp.status(), StatusCode::OK);
    let first_body = to_bytes(first_resp.into_body(), usize::MAX).await.unwrap();
    let first_list: serde_json::Value = serde_json::from_slice(&first_body).unwrap();
    let first_rv = first_list["metadata"]["resourceVersion"]
        .as_str()
        .expect("first page must carry resourceVersion")
        .to_string();
    let first_token = first_list["metadata"]["continue"]
        .as_str()
        .expect("first page must return a continue token")
        .to_string();
    let first_token_data = decode_continue_token(&first_token);
    assert!(first_token_data.ts.is_some());

    // Same-scope PodTemplate churn can compact the historical snapshot while
    // a client still holds a fresh continue token. The token must still make
    // progress by downgrading to an inconsistent continuation rather than
    // returning an early 410.
    let changed_name = "template-0020";
    db.update_resource(
        "v1",
        "PodTemplate",
        Some("default"),
        changed_name,
        serde_json::json!({
            "apiVersion": "v1",
        "kind": "PodTemplate",
        "metadata": {"name": changed_name, "namespace": "default"},
        "template": {
            "metadata": {"labels": {"changed": "true"}},
            "spec": {
                "containers": [{"name": "main", "image": "registry.k8s.io/pause:3.10"}]
            }
        }
        }),
        *template_rvs
            .get(changed_name)
            .expect("changed template rv must be recorded"),
    )
    .await
    .unwrap();
    db.gc_watch_events(1, 1000).await.unwrap();

    let compacted_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/api/v1/namespaces/default/podtemplates?limit=20&continue={first_token}"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        compacted_resp.status(),
        StatusCode::OK,
        "fresh continue token must not fail after unrelated watch history compaction"
    );
    let compacted_body = to_bytes(compacted_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let compacted_list: serde_json::Value = serde_json::from_slice(&compacted_body).unwrap();
    assert_eq!(
        compacted_list["metadata"]["resourceVersion"], first_rv,
        "fresh continue fallback must keep the original consistent list RV"
    );
    assert_eq!(compacted_list["items"].as_array().unwrap().len(), 20);
    let fallback_token = compacted_list["metadata"]["continue"]
        .as_str()
        .expect("fallback page should still have another page");
    let fallback_token_data = decode_continue_token(fallback_token);
    assert!(fallback_token_data.ts.is_none());
    assert!(
        fallback_token_data.session,
        "continuations after a compacted snapshot fallback must remain inconsistent"
    );
    assert_eq!(fallback_token_data.rv.to_string(), first_rv);

    // TTL-expired tokens still return the Kubernetes 410 response with a
    // recovery token. The page served from that recovery token must also keep
    // subsequent tokens inconsistent.
    let expired_token = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(
        serde_json::to_vec(&crate::api::query::ContinueTokenData {
            n: first_token_data.n,
            rv: first_token_data.rv,
            ts: Some(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64
                    - crate::api::query::CONTINUE_TOKEN_TTL_SECS
                    - 1,
            ),
            session: false,
        })
        .unwrap(),
    );

    let expired_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/api/v1/namespaces/default/podtemplates?limit=20&continue={expired_token}"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(expired_resp.status(), StatusCode::GONE);
    let expired_body = to_bytes(expired_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let expired_status: serde_json::Value = serde_json::from_slice(&expired_body).unwrap();
    let recovery_token = expired_status["metadata"]["continue"]
        .as_str()
        .expect("410 Expired must include a recovery continue token");

    let recovery_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/api/v1/namespaces/default/podtemplates?limit=20&continue={recovery_token}"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(recovery_resp.status(), StatusCode::OK);
    let recovery_body = to_bytes(recovery_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let recovery_list: serde_json::Value = serde_json::from_slice(&recovery_body).unwrap();
    let recovery_rv = recovery_list["metadata"]["resourceVersion"]
        .as_str()
        .expect("recovery page must carry resourceVersion")
        .to_string();
    assert_ne!(
        recovery_rv, first_rv,
        "410 recovery must start a fresh inconsistent list RV"
    );
    let recovery_next = recovery_list["metadata"]["continue"]
        .as_str()
        .expect("recovery page should still have another page");
    let recovery_next_data = decode_continue_token(recovery_next);
    assert!(recovery_next_data.ts.is_none());
    assert!(
        recovery_next_data.session,
        "continuations after a 410 recovery token must remain inconsistent"
    );
    assert_eq!(recovery_next_data.rv.to_string(), recovery_rv);
}

fn list_item_names(list: &serde_json::Value) -> Vec<String> {
    list["items"]
        .as_array()
        .expect("items array")
        .iter()
        .map(|i| {
            i["metadata"]["name"]
                .as_str()
                .expect("item name")
                .to_string()
        })
        .collect()
}

/// Pods are the most-paginated kind. A paginated Pod LIST must serve pages 2+
/// from the snapshot pinned on page 1, not from current state — otherwise an
/// object created mid-pagination leaks into a later page and the reported
/// resourceVersion drifts. Regression for the chunked-list snapshot gap.
#[tokio::test]
async fn test_pod_pagination_serves_consistent_snapshot_across_pages() {
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let (app, db) = build_test_router_with_db().await;

    let pod_body = |name: &str| {
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": name, "namespace": "default"},
            "spec": {"containers": [{"name": "main", "image": "registry.k8s.io/pause:3.10"}]}
        })
    };
    for i in 0..4 {
        let name = format!("pod-{i:02}");
        db.create_resource("v1", "Pod", Some("default"), &name, pod_body(&name))
            .await
            .unwrap();
    }

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/namespaces/default/pods?limit=2")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let page1: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(list_item_names(&page1), vec!["pod-00", "pod-01"]);
    let rv1 = page1["metadata"]["resourceVersion"]
        .as_str()
        .expect("page 1 resourceVersion")
        .to_string();
    let token = page1["metadata"]["continue"]
        .as_str()
        .expect("page 1 continue token")
        .to_string();

    // Mutate mid-pagination: create a Pod that sorts into the page-2 window.
    db.create_resource("v1", "Pod", Some("default"), "pod-01a", pod_body("pod-01a"))
        .await
        .unwrap();

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/api/v1/namespaces/default/pods?limit=2&continue={token}"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let page2: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        list_item_names(&page2),
        vec!["pod-02", "pod-03"],
        "page 2 must serve the pinned snapshot, not leak pod-01a created mid-pagination"
    );
    assert_eq!(
        page2["metadata"]["resourceVersion"].as_str().unwrap(),
        rv1,
        "paginated Pod pages must keep the snapshot resourceVersion"
    );
}

/// Namespaces persist in a dedicated table but must still serve consistent
/// paginated snapshots. Regression: the namespace list handler previously
/// re-encoded the session token but served page bodies from current state.
#[tokio::test]
async fn test_namespace_pagination_serves_consistent_snapshot_across_pages() {
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let (app, db) = build_test_router_with_db().await;

    let ns_body = |name: &str| {
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {"name": name, "labels": {"snaptest": "yes"}}
        })
    };
    for suffix in ["a", "b", "c", "d"] {
        let name = format!("ns-{suffix}");
        db.create_namespace(&name, ns_body(&name)).await.unwrap();
    }

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/namespaces?labelSelector=snaptest%3Dyes&limit=2")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let page1: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(list_item_names(&page1), vec!["ns-a", "ns-b"]);
    let rv1 = page1["metadata"]["resourceVersion"]
        .as_str()
        .expect("page 1 resourceVersion")
        .to_string();
    let token = page1["metadata"]["continue"]
        .as_str()
        .expect("page 1 continue token")
        .to_string();

    // Mutate mid-pagination: create a labeled namespace sorting into page 2.
    db.create_namespace("ns-bx", ns_body("ns-bx"))
        .await
        .unwrap();

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/api/v1/namespaces?labelSelector=snaptest%3Dyes&limit=2&continue={token}"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let page2: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        list_item_names(&page2),
        vec!["ns-c", "ns-d"],
        "page 2 must serve the pinned namespace snapshot, not leak ns-bx"
    );
    assert_eq!(
        page2["metadata"]["resourceVersion"].as_str().unwrap(),
        rv1,
        "paginated namespace pages must keep the snapshot resourceVersion"
    );
}

/// Cluster-wide (all-namespaces) collection endpoints are generated by the
/// `cluster_wide_list_handler!` macro (e.g. `GET /api/v1/configmaps`). They must
/// pin a consistent historical snapshot across pages exactly like the namespaced
/// handler. Regression: the macro re-encoded the session continue token but
/// served page bodies from current state, so `kubectl get <kind> -A` and
/// ArgoCD cluster-wide lists leaked objects created mid-pagination.
#[tokio::test]
async fn test_cluster_wide_pagination_serves_consistent_snapshot_across_pages() {
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let (app, db) = build_test_router_with_db().await;

    let cm_body = |name: &str| {
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": name, "namespace": "default"},
            "data": {"k": "v"}
        })
    };
    for i in 0..4 {
        let name = format!("cm-{i:02}");
        db.create_resource("v1", "ConfigMap", Some("default"), &name, cm_body(&name))
            .await
            .unwrap();
    }

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/configmaps?limit=2")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let page1: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(list_item_names(&page1), vec!["cm-00", "cm-01"]);
    let rv1 = page1["metadata"]["resourceVersion"]
        .as_str()
        .expect("page 1 resourceVersion")
        .to_string();
    let token = page1["metadata"]["continue"]
        .as_str()
        .expect("page 1 continue token")
        .to_string();

    // Mutate mid-pagination: create a ConfigMap sorting into the page-2 window.
    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "cm-01a",
        cm_body("cm-01a"),
    )
    .await
    .unwrap();

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/configmaps?limit=2&continue={token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let page2: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        list_item_names(&page2),
        vec!["cm-02", "cm-03"],
        "page 2 must serve the pinned cluster-wide snapshot, not leak cm-01a created mid-pagination"
    );
    assert_eq!(
        page2["metadata"]["resourceVersion"].as_str().unwrap(),
        rv1,
        "paginated cluster-wide pages must keep the snapshot resourceVersion"
    );
}

/// Non-conversion custom resources live in the generic resource table and must
/// pin a real historical snapshot across pages, exactly like the core kinds.
#[tokio::test]
async fn test_crd_pagination_serves_consistent_snapshot_across_pages() {
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use hyper::header::CONTENT_TYPE;
    use tower::ServiceExt;

    let (app, db) = build_test_router_with_db().await;

    let crd_yaml = r#"
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: widgets.example.com
spec:
  group: example.com
  scope: Namespaced
  names:
    kind: Widget
    plural: widgets
    singular: widget
  versions:
  - name: v1
    served: true
    storage: true
    schema:
      openAPIV3Schema:
        type: object
        properties:
          spec:
            type: object
            properties:
              size:
                type: string
"#;
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions/widgets.example.com")
                .header(CONTENT_TYPE, "application/apply-patch+yaml")
                .body(Body::from(crd_yaml))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "CRD apply failed: {}",
        resp.status()
    );

    let widget_body = |name: &str| {
        serde_json::json!({
            "apiVersion": "example.com/v1",
            "kind": "Widget",
            "metadata": {"name": name, "namespace": "default"},
            "spec": {"size": "m"}
        })
    };
    for i in 0..4 {
        let name = format!("w-{i:02}");
        db.create_resource(
            "example.com/v1",
            "Widget",
            Some("default"),
            &name,
            widget_body(&name),
        )
        .await
        .unwrap();
    }

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/apis/example.com/v1/namespaces/default/widgets?limit=2")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let page1: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(list_item_names(&page1), vec!["w-00", "w-01"]);
    let rv1 = page1["metadata"]["resourceVersion"]
        .as_str()
        .expect("page 1 resourceVersion")
        .to_string();
    let token = page1["metadata"]["continue"]
        .as_str()
        .expect("page 1 continue token")
        .to_string();

    // Mutate mid-pagination: create a CR sorting into the page-2 window.
    db.create_resource(
        "example.com/v1",
        "Widget",
        Some("default"),
        "w-01a",
        widget_body("w-01a"),
    )
    .await
    .unwrap();

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/apis/example.com/v1/namespaces/default/widgets?limit=2&continue={token}"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let page2: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        list_item_names(&page2),
        vec!["w-02", "w-03"],
        "page 2 must serve the pinned CRD snapshot, not leak w-01a"
    );
    assert_eq!(
        page2["metadata"]["resourceVersion"].as_str().unwrap(),
        rv1,
        "paginated CRD pages must keep the snapshot resourceVersion"
    );
}

/// Register a single-version Namespaced CRD `selws.selwatch.example.com`
/// (kind `Selw`) used by the custom-resource selector-watch regressions.
async fn register_selw_crd(app: &axum::Router) {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;
    let crd = serde_json::json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "selws.selwatch.example.com"},
        "spec": {
            "group": "selwatch.example.com",
            "scope": "Namespaced",
            "names": {"plural": "selws", "singular": "selw", "kind": "Selw"},
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {"openAPIV3Schema": {"type": "object", "x-kubernetes-preserve-unknown-fields": true}}
            }]
        }
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&crd).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
}

/// Regression (B1): a CRD WatchList (`sendInitialEvents=true`) must terminate
/// the initial ADDED stream with an `initial-events-end` BOOKMARK carrying the
/// snapshot resourceVersion, so the client knows where to resume. The built-in
/// watch builder emits this; the CR builder previously omitted it entirely,
/// leaving WatchList clients against CRDs unable to learn a resume point. Also
/// covers the empty-collection case (finding #2): the bookmark RV must be the
/// real snapshot RV, not the stale requested RV (0).
#[tokio::test]
async fn test_crd_watchlist_emits_initial_events_end_bookmark() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use futures::StreamExt;
    use serde_json::json;
    use std::sync::Arc;
    use std::time::Duration;
    use tower::ServiceExt;

    let (app, db) = build_test_router_with_db().await;
    let namespace = "crd-watchlist-bookmark";
    let mk = |method: &str, uri: String, body: Option<Vec<u8>>| {
        let mut request = Request::builder().method(method).uri(uri);
        if body.is_some() {
            request = request.header("content-type", "application/json");
        }
        request
            .body(body.map(Body::from).unwrap_or_else(Body::empty))
            .unwrap()
    };

    let ns_resp = app
        .clone()
        .oneshot(mk(
            "POST",
            "/api/v1/namespaces".to_string(),
            Some(
                serde_json::to_vec(&json!({
                    "apiVersion": "v1",
                    "kind": "Namespace",
                    "metadata": {"name": namespace}
                }))
                .unwrap(),
            ),
        ))
        .await
        .unwrap();
    assert_eq!(ns_resp.status(), StatusCode::CREATED);
    register_selw_crd(&app).await;

    // One existing CR object so the initial list is non-empty.
    let create_rv = db
        .get_current_resource_version()
        .await
        .unwrap()
        .saturating_add(1);
    db.apply_log_apply_commit(crate::log_apply::LogApplyCommit::put_resource(
        &crate::datastore::Resource {
            id: 0,
            api_version: "selwatch.example.com/v1".into(),
            kind: "Selw".into(),
            namespace: Some(namespace.to_string()),
            name: "obj-a".into(),
            uid: "selw-bookmark-a".into(),
            resource_version: create_rv,
            data: Arc::new(json!({
                "apiVersion": "selwatch.example.com/v1",
                "kind": "Selw",
                "metadata": {"name": "obj-a", "namespace": namespace, "uid": "selw-bookmark-a"}
            })),
        },
    ))
    .await
    .expect("seed CR object");

    let watch_resp = app
        .clone()
        .oneshot(mk(
            "GET",
            format!(
                "/apis/selwatch.example.com/v1/namespaces/{namespace}/selws?watch=true&sendInitialEvents=true&resourceVersionMatch=NotOlderThan&allowWatchBookmarks=true"
            ),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(watch_resp.status(), StatusCode::OK);
    let mut stream = watch_resp.into_body().into_data_stream();

    let mut saw_added = false;
    let mut bookmark_rv: Option<i64> = None;
    for _ in 0..8 {
        let chunk = match tokio::time::timeout(Duration::from_millis(500), stream.next()).await {
            Ok(Some(Ok(chunk))) => chunk,
            _ => break,
        };
        for line in chunk.split(|byte| *byte == b'\n') {
            if line.is_empty() {
                continue;
            }
            let event: serde_json::Value = serde_json::from_slice(line).unwrap();
            if event["type"] == "ADDED"
                && event
                    .pointer("/object/metadata/name")
                    .and_then(|v| v.as_str())
                    == Some("obj-a")
            {
                saw_added = true;
            }
            if event["type"] == "BOOKMARK"
                && event
                    .pointer("/object/metadata/annotations/k8s.io~1initial-events-end")
                    .and_then(|v| v.as_str())
                    == Some("true")
            {
                bookmark_rv = event
                    .pointer("/object/metadata/resourceVersion")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<i64>().ok());
            }
        }
        if saw_added && bookmark_rv.is_some() {
            break;
        }
    }

    assert!(saw_added, "WatchList must deliver the existing CR as ADDED");
    let rv = bookmark_rv.expect("CRD WatchList must emit an initial-events-end BOOKMARK");
    assert!(
        rv >= create_rv,
        "initial-events-end bookmark RV {rv} must report the snapshot RV (>= {create_rv}), not the stale requested RV"
    );
}

/// Regression (CR watch builder bug 1): an rv-less label-selector custom-resource
/// watch must keep its live-delivery floor at 0 so a genuinely live ADDED whose
/// replicated commit broadcasts below the establishment floor still reaches the
/// client. The divergent CR builder previously pinned the floor to
/// `rv_less_floor`, dropping it — the fix the built-in path already had.
#[tokio::test]
async fn test_rv_less_selector_cr_watch_delivers_live_added_below_establishment_floor() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use futures::StreamExt;
    use serde_json::json;
    use std::sync::Arc;
    use std::time::Duration;
    use tower::ServiceExt;

    let (app, db) = build_test_router_with_db().await;
    let namespace = "crd-rvless-low-added";
    let mk = |method: &str, uri: String, body: Option<Vec<u8>>| {
        let mut request = Request::builder().method(method).uri(uri);
        if body.is_some() {
            request = request.header("content-type", "application/json");
        }
        request
            .body(body.map(Body::from).unwrap_or_else(Body::empty))
            .unwrap()
    };

    let ns_resp = app
        .clone()
        .oneshot(mk(
            "POST",
            "/api/v1/namespaces".to_string(),
            Some(
                serde_json::to_vec(&json!({
                    "apiVersion": "v1",
                    "kind": "Namespace",
                    "metadata": {"name": namespace}
                }))
                .unwrap(),
            ),
        ))
        .await
        .unwrap();
    assert_eq!(ns_resp.status(), StatusCode::CREATED);
    register_selw_crd(&app).await;

    let floor = db
        .advance_resource_version_after(
            db.get_current_resource_version()
                .await
                .unwrap()
                .saturating_add(20),
        )
        .await
        .unwrap();

    let watch_resp = app
        .clone()
        .oneshot(mk(
            "GET",
            format!(
                "/apis/selwatch.example.com/v1/namespaces/{namespace}/selws?watch=true&labelSelector=race%3Dbelow-floor"
            ),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(watch_resp.status(), StatusCode::OK);
    let mut stream = watch_resp.into_body().into_data_stream();
    // Park on the live cursor after the empty baseline list.
    let _ = tokio::time::timeout(Duration::from_millis(200), stream.next()).await;

    let create_rv = floor - 1;
    db.apply_log_apply_commit(crate::log_apply::LogApplyCommit::put_resource(
        &crate::datastore::Resource {
            id: 0,
            api_version: "selwatch.example.com/v1".into(),
            kind: "Selw".into(),
            namespace: Some(namespace.to_string()),
            name: "live-low".into(),
            uid: "selw-low-added".into(),
            resource_version: create_rv,
            data: Arc::new(json!({
                "apiVersion": "selwatch.example.com/v1",
                "kind": "Selw",
                "metadata": {
                    "name": "live-low",
                    "namespace": namespace,
                    "uid": "selw-low-added",
                    "labels": {"race": "below-floor"}
                }
            })),
        },
    ))
    .await
    .expect("replicated CR create below establishment floor");

    let mut saw_added = false;
    for _ in 0..6 {
        let chunk = match tokio::time::timeout(Duration::from_millis(500), stream.next()).await {
            Ok(Some(Ok(chunk))) => chunk,
            _ => break,
        };
        for line in chunk.split(|byte| *byte == b'\n') {
            if line.is_empty() {
                continue;
            }
            let event: serde_json::Value = serde_json::from_slice(line).unwrap();
            if event["type"] == "ADDED"
                && event
                    .pointer("/object/metadata/name")
                    .and_then(|v| v.as_str())
                    == Some("live-low")
            {
                saw_added = true;
                break;
            }
        }
        if saw_added {
            break;
        }
    }
    assert!(
        saw_added,
        "rv-less selector CR watch must deliver live ADDED rv={create_rv} below establishment floor rv={floor}"
    );
}

/// Regression (CR watch builder bug 2): a `resourceVersion>0` label-selector
/// custom-resource watch must register its baseline members and grant each a
/// per-key low-rv exception, so a below-floor DELETED tombstone for a baseline
/// member still reaches the client. The divergent CR builder had no allowlist,
/// so the tombstone was silently swallowed and the client kept a phantom member.
#[tokio::test]
async fn test_rv_selector_cr_watch_delivers_baseline_delete_below_floor() {
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use futures::StreamExt;
    use serde_json::json;
    use std::sync::Arc;
    use std::time::Duration;
    use tower::ServiceExt;

    let (app, db) = build_test_router_with_db().await;
    let namespace = "crd-selector-low-rv-delete";
    let mk = |method: &str, uri: String, body: Option<Vec<u8>>| {
        let mut request = Request::builder().method(method).uri(uri);
        if body.is_some() {
            request = request.header("content-type", "application/json");
        }
        request
            .body(body.map(Body::from).unwrap_or_else(Body::empty))
            .unwrap()
    };

    let ns_resp = app
        .clone()
        .oneshot(mk(
            "POST",
            "/api/v1/namespaces".to_string(),
            Some(
                serde_json::to_vec(&json!({
                    "apiVersion": "v1",
                    "kind": "Namespace",
                    "metadata": {"name": namespace}
                }))
                .unwrap(),
            ),
        ))
        .await
        .unwrap();
    assert_eq!(ns_resp.status(), StatusCode::CREATED);
    register_selw_crd(&app).await;

    let create_rv = db.get_current_resource_version().await.unwrap() + 1;
    let cr = crate::datastore::Resource {
        id: 0,
        api_version: "selwatch.example.com/v1".into(),
        kind: "Selw".into(),
        namespace: Some(namespace.into()),
        name: "watched".into(),
        uid: "selw-low-rv-delete".into(),
        resource_version: create_rv,
        data: Arc::new(json!({
            "apiVersion": "selwatch.example.com/v1",
            "kind": "Selw",
            "metadata": {
                "name": "watched",
                "namespace": namespace,
                "uid": "selw-low-rv-delete",
                "labels": {"race": "below-floor"}
            }
        })),
    };
    db.apply_log_apply_commit(crate::log_apply::LogApplyCommit::put_resource(&cr))
        .await
        .expect("replicated CR create apply");

    let inflated_rv = db
        .advance_resource_version_after(create_rv + 20)
        .await
        .expect("inflate global rv");
    assert!(inflated_rv > create_rv);

    let list_resp = app
        .clone()
        .oneshot(mk(
            "GET",
            format!(
                "/apis/selwatch.example.com/v1/namespaces/{namespace}/selws?labelSelector=race%3Dbelow-floor&limit=500&resourceVersion=0"
            ),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(list_resp.status(), StatusCode::OK);
    let list_body = to_bytes(list_resp.into_body(), usize::MAX).await.unwrap();
    let list_json: serde_json::Value = serde_json::from_slice(&list_body).unwrap();
    assert_eq!(
        list_json
            .pointer("/items/0/metadata/name")
            .and_then(|v| v.as_str()),
        Some("watched")
    );
    let list_rv = list_json
        .pointer("/metadata/resourceVersion")
        .and_then(|v| v.as_str())
        .and_then(|v| v.parse::<i64>().ok())
        .expect("list resourceVersion");
    assert!(list_rv > create_rv);

    let watch_resp = app
        .clone()
        .oneshot(mk(
            "GET",
            format!(
                "/apis/selwatch.example.com/v1/namespaces/{namespace}/selws?watch=true&resourceVersion={list_rv}&labelSelector=race%3Dbelow-floor"
            ),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(watch_resp.status(), StatusCode::OK);
    let mut stream = watch_resp.into_body().into_data_stream();
    let reader = tokio::spawn(async move {
        let mut seen = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while tokio::time::Instant::now() < deadline {
            let Ok(Some(Ok(chunk))) =
                tokio::time::timeout(Duration::from_millis(500), stream.next()).await
            else {
                continue;
            };
            for line in chunk.split(|byte| *byte == b'\n') {
                if line.is_empty() {
                    continue;
                }
                let event: serde_json::Value = serde_json::from_slice(line).unwrap();
                if event
                    .pointer("/object/metadata/name")
                    .and_then(|v| v.as_str())
                    != Some("watched")
                {
                    continue;
                }
                let ty = event["type"].as_str().unwrap_or("").to_string();
                seen.push(ty.clone());
                if ty == "DELETED" {
                    return seen;
                }
            }
        }
        seen
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    let delete_rv = list_rv - 1;
    assert!(delete_rv > create_rv);
    db.apply_log_apply_commit(crate::log_apply::LogApplyCommit::new(
        delete_rv,
        vec![crate::log_apply::LogApplyMutation::DeleteResource(
            crate::log_apply::LogApplyResourceKey {
                api_version: "selwatch.example.com/v1".to_string(),
                kind: "Selw".to_string(),
                namespace: Some(namespace.to_string()),
                name: "watched".to_string(),
                uid: "selw-low-rv-delete".to_string(),
                precondition_resource_version: None,
            },
        )],
    ))
    .await
    .expect("replicated CR delete below list rv");

    let seen = reader.await.expect("watch reader task");
    assert!(
        seen.iter().any(|ty| ty == "DELETED"),
        "rv>0 selector CR watch from list rv={list_rv} must deliver delete rv={delete_rv} for baseline member; saw {seen:?}"
    );
}

/// Regression: a metadata-only annotation strategic-merge patch on an
/// RC-owned pod must not make the pod disappear from label-selected lists.
/// kubectl conformance patches controller pods with metadata annotations
/// and expects the pod to remain selector-visible with its labels and
/// RC ownerReferences intact. This pins the API PATCH path
/// (strategic-merge of object metadata) plus the label-selector list path.
#[tokio::test]
async fn pod_metadata_patch_preserves_rc_selector_visibility() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let (app, db) = build_test_router_with_db().await;

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "kubectl-rc",
        json!({"apiVersion":"v1","kind":"Namespace","metadata":{"name":"kubectl-rc"}}),
    )
    .await
    .unwrap();

    db.create_resource(
        "v1",
        "Pod",
        Some("kubectl-rc"),
        "agnhost-primary",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "agnhost-primary",
                "namespace": "kubectl-rc",
                "uid": "agnhost-primary-uid",
                "labels": {"name": "agnhost-primary"},
                "ownerReferences": [{
                    "apiVersion": "v1",
                    "kind": "ReplicationController",
                    "name": "agnhost-primary",
                    "uid": "agnhost-rc-uid",
                    "controller": true,
                    "blockOwnerDeletion": true
                }]
            },
            "spec": {
                "nodeName": "worker-a",
                "containers": [{"name": "agnhost", "image": "registry.k8s.io/e2e-test-images/agnhost:2.56"}]
            },
            "status": {"phase": "Running"}
        }),
    )
    .await
    .unwrap();

    let patch_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/api/v1/namespaces/kubectl-rc/pods/agnhost-primary")
                .header("content-type", "application/strategic-merge-patch+json")
                .body(Body::from(
                    r#"{"metadata":{"annotations":{"patched":"true"}}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        patch_resp.status(),
        StatusCode::OK,
        "metadata patch must succeed"
    );

    let list_resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/namespaces/kubectl-rc/pods?labelSelector=name%3Dagnhost-primary")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list_resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(list_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let list: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let items = list
        .pointer("/items")
        .and_then(|v| v.as_array())
        .expect("items array");
    assert_eq!(
        items.len(),
        1,
        "patched pod must remain selector-visible after metadata annotation patch"
    );
    assert_eq!(
        items[0].pointer("/metadata/labels/name"),
        Some(&serde_json::json!("agnhost-primary")),
        "metadata annotation patch must preserve selector label"
    );
    assert_eq!(
        items[0].pointer("/metadata/ownerReferences/0/uid"),
        Some(&serde_json::json!("agnhost-rc-uid")),
        "metadata annotation patch must preserve RC ownerRef"
    );
}
