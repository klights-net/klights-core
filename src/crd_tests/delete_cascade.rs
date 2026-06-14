use super::*;

#[tokio::test]
async fn test_crd_delete_with_empty_name_returns_error() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let db = crate::datastore::test_support::in_memory().await;
    let registry = CrdRegistry::new();

    // Create a CRD first
    let crd = make_crd_value(
        "cert-manager.io",
        "Certificate",
        "certificates",
        "Namespaced",
    );
    register_crd_from_value(&registry, &crd).await.unwrap();
    db.create_resource(
        "apiextensions.k8s.io/v1",
        "CustomResourceDefinition",
        None,
        "certificates.cert-manager.io",
        crd,
    )
    .await
    .unwrap();

    // Create a custom resource
    let cert = serde_json::json!({
        "apiVersion": "cert-manager.io/v1",
        "kind": "Certificate",
        "metadata": {
            "name": "test-cert",
            "namespace": "default"
        },
        "spec": {
            "secretName": "test-secret"
        }
    });
    db.create_resource(
        "cert-manager.io/v1",
        "Certificate",
        Some("default"),
        "test-cert",
        cert,
    )
    .await
    .unwrap();

    // Build router from shared fixture
    let app = build_test_router(db, registry).await;

    // Test DELETE with whitespace-only name (URL encoded as %20)
    // The API handler should validate that name is not empty/whitespace
    // This reproduces the Sonobuoy error: "resource name may not be empty"

    let request = Request::builder()
        .method("DELETE")
        .uri("/apis/cert-manager.io/v1/namespaces/default/certificates/%20")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    // After fix: should return 400 Bad Request
    // Before fix: returns 404 or other error
    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "DELETE with whitespace-only name should return 400 Bad Request"
    );
}

#[tokio::test]
async fn test_crd_get_with_empty_name_returns_error() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let db = crate::datastore::test_support::in_memory().await;
    let registry = CrdRegistry::new();

    // Create a CRD
    let crd = make_crd_value(
        "cert-manager.io",
        "Certificate",
        "certificates",
        "Namespaced",
    );
    register_crd_from_value(&registry, &crd).await.unwrap();

    let app = build_test_router(db, registry).await;

    // Test GET with whitespace-only name
    let request = Request::builder()
        .method("GET")
        .uri("/apis/cert-manager.io/v1/namespaces/default/certificates/%20")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "GET with whitespace-only name should return 400 Bad Request"
    );
}

#[tokio::test]
async fn test_crd_update_with_empty_name_returns_error() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let db = crate::datastore::test_support::in_memory().await;
    let registry = CrdRegistry::new();

    // Create a CRD
    let crd = make_crd_value(
        "cert-manager.io",
        "Certificate",
        "certificates",
        "Namespaced",
    );
    register_crd_from_value(&registry, &crd).await.unwrap();

    let app = build_test_router(db, registry).await;

    // Test PUT with whitespace-only name
    let update_body = serde_json::json!({
        "apiVersion": "cert-manager.io/v1",
        "kind": "Certificate",
        "metadata": {
            "name": " ",
            "namespace": "default"
        },
        "spec": {
            "secretName": "updated-secret"
        }
    });

    let request = Request::builder()
        .method("PUT")
        .uri("/apis/cert-manager.io/v1/namespaces/default/certificates/%20")
        .header("Content-Type", "application/json")
        .body(Body::from(serde_json::to_vec(&update_body).unwrap()))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "PUT with whitespace-only name should return 400 Bad Request"
    );
}

#[tokio::test]
async fn test_crd_patch_with_empty_name_returns_error() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let db = crate::datastore::test_support::in_memory().await;
    let registry = CrdRegistry::new();

    // Create a CRD
    let crd = make_crd_value(
        "cert-manager.io",
        "Certificate",
        "certificates",
        "Namespaced",
    );
    register_crd_from_value(&registry, &crd).await.unwrap();

    let app = build_test_router(db, registry).await;

    // Test PATCH with whitespace-only name
    let patch_body = serde_json::json!({
        "spec": {
            "secretName": "patched-secret"
        }
    });

    let request = Request::builder()
        .method("PATCH")
        .uri("/apis/cert-manager.io/v1/namespaces/default/certificates/%20")
        .header("Content-Type", "application/merge-patch+json")
        .body(Body::from(serde_json::to_vec(&patch_body).unwrap()))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "PATCH with whitespace-only name should return 400 Bad Request"
    );
}

/// P0-7 regression test: watch at rv=N opened after CRD creation must receive an ADDED
/// event (not MODIFIED) within 5 seconds.
///
/// Failure scenario that caused Sonobuoy S9-API-8 timeouts:
///   1. Client LISTs CRDs → gets resourceVersion=N
///   2. Client creates CRD (INSERT at rv=N+1, status UPDATE at rv=N+2)
///   3. Client opens watch?resourceVersion=N
///   4. Watch catch-up queries DB for resources with rv>N → finds CRD at rv=N+2
///   5. BUG: catch-up emitted MODIFIED instead of ADDED (CRD didn't exist at rv=N)
///   6. K8s client waited for ADDED but got MODIFIED → 5s timeout
#[tokio::test]
async fn test_crd_watch_catchup_emits_added_for_newly_created_crd() {
    // Only needs the DB catch-up query path — no watch hook required.
    let db = crate::datastore::test_support::in_memory().await;

    // Step 1: capture the "list rv" before any CRD exists
    let list_rv = db.get_current_resource_version().await.unwrap();

    // Step 2: create a CRD (simulates POST /apis/apiextensions.k8s.io/v1/customresourcedefinitions)
    let crd = make_crd_value("example.com", "Widget", "widgets", "Namespaced");
    db.create_resource(
        "apiextensions.k8s.io/v1",
        "CustomResourceDefinition",
        None,
        "widgets.example.com",
        crd.clone(),
    )
    .await
    .unwrap();

    // Also update the CRD status (mimics create_crd_with_registration)
    let crd_with_status = crate::api::add_crd_established_condition(crd.clone());
    let created = db
        .get_resource(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            "widgets.example.com",
        )
        .await
        .unwrap()
        .unwrap();
    db.update_resource(
        "apiextensions.k8s.io/v1",
        "CustomResourceDefinition",
        None,
        "widgets.example.com",
        crd_with_status,
        created.resource_version,
    )
    .await
    .unwrap();

    // Step 3: simulate watch?resourceVersion=list_rv via DB catch-up
    // This replicates the catch-up path in cluster_resource_handlers!
    let missed = db
        .list_cluster_resources_modified_since(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            list_rv,
        )
        .await
        .unwrap();

    // Step 4 & 5: verify that the catch-up emits ADDED (not MODIFIED) for the new CRD
    assert!(
        !missed.is_empty(),
        "catch-up must find the CRD created after rv={}",
        list_rv
    );

    // The CRD was created after list_rv, so it must be reported as ADDED
    let crd_resource = missed
        .iter()
        .find(|r| r.resource.name == "widgets.example.com")
        .expect("catch-up must return the newly created CRD");

    assert_eq!(
        crd_resource.event_type, "ADDED",
        "Catch-up must emit ADDED for a CRD created after the watch rv={}. \
        This is P0-7: Sonobuoy waits for ADDED but gets MODIFIED → 5s timeout.",
        list_rv
    );
}

/// P0-7 regression test: the watch channel broadcasts an ADDED event for CRDs, and a
/// subscriber receives it synchronously.
///
/// This tests the broadcast channel plumbing that `api.rs` relies on: when the watch
/// handler emits a `WatchEvent::added(...)` for a CRD (e.g. after the catch-up query
/// identifies a newly-created resource), downstream subscribers (open watch connections)
/// must receive it.
#[tokio::test]
async fn test_crd_watch_broadcast_delivers_added_event_to_subscriber() {
    let watch_bus = crate::watch::WatchBus::new(16);
    let mut rx = watch_bus.subscribe(crate::watch::WatchTopic::new(
        "apiextensions.k8s.io/v1",
        "CustomResourceDefinition",
    ));

    // Simulate the api.rs watch handler broadcasting ADDED for a newly created CRD.
    // (In production this fires from the catch-up path or the DB update hook.)
    let crd_object = serde_json::json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {
            "name": "foos.test.io",
            "resourceVersion": "42"
        }
    });
    watch_bus.publish(WatchEvent::added(crd_object));

    // Subscriber must receive the ADDED event immediately (no async I/O involved)
    match tokio::time::timeout(tokio::time::Duration::from_millis(100), rx.recv()).await {
        Ok(Ok(WatchEvent {
            event_type, object, ..
        })) => {
            assert_eq!(event_type, EventType::Added);
            assert_eq!(
                object.get("kind").and_then(|k| k.as_str()),
                Some("CustomResourceDefinition")
            );
            assert_eq!(
                object.pointer("/metadata/name").and_then(|n| n.as_str()),
                Some("foos.test.io")
            );
        }
        Ok(Err(e)) => panic!("broadcast recv error: {e}"),
        Err(_) => panic!(
            "Watch subscriber did not receive ADDED event within timeout \
            (P0-7: channel plumbing broken)"
        ),
    }
}

/// P0-7 regression test for the Lagged-recovery branch.
///
/// Ensures lagged recovery replays persisted event history types in order, so resources
/// created after the watch opened are still delivered as ADDED.
///
/// Scenario:
///   - Watch opens at requested_rv = list_rv (resource A already exists at rv=R_A).
///   - Subscriber lags, broadcast events drop.
///   - Resource B is created (rv=R_B) AFTER the watch opened.
///   - Lagged-recovery iterates [A, B] from persisted watch_events and must emit ADDED for B.
#[tokio::test]
async fn test_lagged_recovery_replays_persisted_event_types_for_fresh_creation() {
    let db = crate::datastore::test_support::in_memory().await;

    // Step 1: capture the watch's starting resourceVersion (requested_rv = threshold-at-entry).
    let requested_rv = db.get_current_resource_version().await.unwrap();

    // Step 2: pre-create resource A so that the recovery loop has something to process
    // BEFORE reaching B so recovery iterates multiple events.
    let pre_existing = make_crd_value("pre.example.com", "Pre", "pres", "Cluster");
    let resource_a = db
        .create_resource(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            "pres.pre.example.com",
            pre_existing,
        )
        .await
        .unwrap();
    let r_a = resource_a.resource_version;
    assert!(
        r_a > requested_rv,
        "resource A must be created at rv > requested_rv ({} > {})",
        r_a,
        requested_rv
    );

    // Step 3: create resource B (fresh creation, AFTER the watch opened).
    let fresh = make_crd_value("fresh.example.com", "Fresh", "freshes", "Cluster");
    let resource_b = db
        .create_resource(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            "freshes.fresh.example.com",
            fresh,
        )
        .await
        .unwrap();
    let r_b = resource_b.resource_version;
    assert!(
        r_b > r_a,
        "resource B's rv must exceed A's ({} > {})",
        r_b,
        r_a
    );

    // Step 4: pull the recovered resources via the same DB query the Lagged branch uses.
    let recovered = db
        .list_cluster_resources_modified_since(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            requested_rv,
        )
        .await
        .unwrap();
    assert_eq!(recovered.len(), 2, "must recover both A and B");

    // Confirm event history preserved the creation event.
    let recovered_b = recovered
        .iter()
        .find(|c| c.resource.name == "freshes.fresh.example.com")
        .expect("B must be in recovered set");
    assert_eq!(
        recovered_b.event_type, "ADDED",
        "freshly created resource must be replayed as ADDED"
    );

    // Step 5: simulate the Lagged-recovery loop: events are replayed directly from persisted
    // event history and must retain their original types.
    let mut last_rv = requested_rv;
    let mut events = Vec::with_capacity(2);
    for catchup in recovered {
        let resource = catchup.resource;
        last_rv = last_rv.max(resource.resource_version);
        events.push((resource.name, catchup.event_type));
    }

    // Step 6: BOTH A and B must be ADDED — they were created after the watch opened.
    let event_b = events
        .iter()
        .find(|(n, _)| n == "freshes.fresh.example.com")
        .expect("B must appear in events");
    assert_eq!(
        event_b.1, "ADDED",
        "Lagged-recovery must emit ADDED for resource created after watch opened \
        (requested_rv={}, created_rv={}, last_rv-after-A={}).",
        requested_rv, r_b, last_rv
    );
    let event_a = events
        .iter()
        .find(|(n, _)| n == "pres.pre.example.com")
        .expect("A must appear in events");
    assert_eq!(
        event_a.1, "ADDED",
        "A also created after watch opened → ADDED"
    );
}
