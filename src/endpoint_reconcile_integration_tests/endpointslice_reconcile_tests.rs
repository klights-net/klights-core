use super::*;

#[tokio::test]
async fn test_mirror_endpoints_sets_owner_reference() {
    // P0-E2E-20260423-09: mirror EndpointSlice must carry an ownerReference
    // so GC deletes it when the Endpoints is deleted.
    let db = crate::datastore::test_support::in_memory().await;
    let ns = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "default"}});
    db.create_resource("v1", "Namespace", None, "default", ns)
        .await
        .unwrap();

    let endpoints = json!({
        "apiVersion": "v1",
        "kind": "Endpoints",
        "metadata": {
            "name": "my-svc",
            "namespace": "default",
            "uid": "test-uid-123"
        },
        "subsets": [{"addresses": [{"ip": "10.1.2.3"}], "ports": [{"port": 8080, "protocol": "TCP"}]}]
    });
    let created = db
        .create_resource("v1", "Endpoints", Some("default"), "my-svc", endpoints)
        .await
        .unwrap();
    let endpoints = crate::controller_test_support::inject_resource_version(
        created.data,
        created.resource_version,
    );

    mirror_endpoints_to_endpointslice(&controller_store(&db), &endpoints)
        .await
        .unwrap();

    let mirror = db
        .get_resource(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some("default"),
            "my-svc-mirror",
        )
        .await
        .unwrap()
        .expect("mirror should exist");

    let owner_refs = mirror
        .data
        .pointer("/metadata/ownerReferences")
        .unwrap()
        .as_array()
        .unwrap();
    assert!(!owner_refs.is_empty(), "mirror must have ownerReferences");
    assert_eq!(owner_refs[0]["kind"], "Endpoints");
    assert_eq!(owner_refs[0]["name"], "my-svc");
    assert_eq!(owner_refs[0]["uid"], "test-uid-123");
}

#[tokio::test]
async fn test_mirror_endpoints_stale_snapshot_after_delete_does_not_recreate_slice() {
    let db = crate::datastore::test_support::in_memory().await;
    let ns = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "default"}});
    db.create_resource("v1", "Namespace", None, "default", ns)
        .await
        .unwrap();

    let endpoints = json!({
        "apiVersion": "v1",
        "kind": "Endpoints",
        "metadata": {
            "name": "gone-svc",
            "namespace": "default",
            "uid": "gone-endpoints-uid"
        },
        "subsets": [{"addresses": [{"ip": "10.1.2.3"}], "ports": [{"port": 8080, "protocol": "TCP"}]}]
    });
    let created = db
        .create_resource("v1", "Endpoints", Some("default"), "gone-svc", endpoints)
        .await
        .unwrap();
    let stale_snapshot = crate::controller_test_support::inject_resource_version(
        created.data,
        created.resource_version,
    );

    db.delete_resource("v1", "Endpoints", Some("default"), "gone-svc")
        .await
        .unwrap();

    mirror_endpoints_to_endpointslice(&controller_store(&db), &stale_snapshot)
        .await
        .unwrap();

    let mirror = db
        .get_resource(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some("default"),
            "gone-svc-mirror",
        )
        .await
        .unwrap();
    assert!(
        mirror.is_none(),
        "stale deleted Endpoints mirror reconcile must not recreate EndpointSlice"
    );
}

#[tokio::test]
async fn test_reconcile_endpoints_named_target_port_resolves_to_container_port() {
    let db = crate::datastore::test_support::in_memory().await;

    let ns = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "test"}});
    db.create_resource("v1", "Namespace", None, "test", ns)
        .await
        .unwrap();

    // Pod with named container port "http" -> 8080
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "nginx-1",
            "namespace": "test",
            "labels": {"app": "nginx"}
        },
        "spec": {
            "containers": [{
                "name": "nginx",
                "ports": [{"name": "http", "containerPort": 8080, "protocol": "TCP"}]
            }]
        },
        "status": {
            "podIP": "10.43.0.2",
            "conditions": [{"type": "Ready", "status": "True"}]
        }
    });
    db.create_resource("v1", "Pod", Some("test"), "nginx-1", pod)
        .await
        .unwrap();

    // Service port 80 -> targetPort "http" (named port)
    let selector = json!({"app": "nginx"});
    let ports = json!([{"port": 80, "targetPort": "http", "protocol": "TCP"}]);

    reconcile_endpoints(
        &controller_store(&db),
        crate::controller_test_support::pod_repository_for_test(&db).as_ref(),
        "nginx-service",
        "test",
        Some(&selector),
        Some(&ports),
        false,
    )
    .await
    .unwrap();

    let ep = db
        .get_resource("v1", "Endpoints", Some("test"), "nginx-service")
        .await
        .unwrap()
        .unwrap();

    let ep_ports = ep.data["subsets"][0]["ports"].as_array().unwrap();
    assert_eq!(ep_ports.len(), 1);
    assert_eq!(
        ep_ports[0]["port"], 8080,
        "Named targetPort 'http' should resolve to container port 8080, not service port 80 or 0"
    );
}

#[tokio::test]
async fn test_reconcile_endpoints_numeric_string_target_port_and_skip_unresolved_named_port() {
    let db = crate::datastore::test_support::in_memory().await;

    let ns = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "test"}});
    db.create_resource("v1", "Namespace", None, "test", ns)
        .await
        .unwrap();

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "pod-1",
            "namespace": "test",
            "labels": {"app": "demo"}
        },
        "spec": {
            "containers": [{
                "name": "demo",
                "ports": [{"name": "portname1", "containerPort": 100, "protocol": "TCP"}]
            }]
        },
        "status": {
            "podIP": "10.43.0.10",
            "conditions": [{"type": "Ready", "status": "True"}]
        }
    });
    db.create_resource("v1", "Pod", Some("test"), "pod-1", pod)
        .await
        .unwrap();

    let selector = json!({"app": "demo"});
    let ports = json!([
        {"name": "portname1", "port": 80, "targetPort": "100", "protocol": "TCP"},
        {"name": "portname2", "port": 81, "targetPort": "portname2", "protocol": "TCP"}
    ]);

    reconcile_endpoints(
        &controller_store(&db),
        crate::controller_test_support::pod_repository_for_test(&db).as_ref(),
        "multi-endpoint-test",
        "test",
        Some(&selector),
        Some(&ports),
        false,
    )
    .await
    .unwrap();

    let ep = db
        .get_resource("v1", "Endpoints", Some("test"), "multi-endpoint-test")
        .await
        .unwrap()
        .unwrap();

    let ep_ports = ep.data["subsets"][0]["ports"].as_array().unwrap();
    assert_eq!(
        ep_ports.len(),
        1,
        "unresolved named targetPort must be skipped"
    );
    assert_eq!(
        ep_ports[0]["port"], 100,
        "numeric-string targetPort must be interpreted as integer 100"
    );
}

#[tokio::test]
async fn test_reconcile_endpoints_preserves_service_port_name() {
    let db = crate::datastore::test_support::in_memory().await;

    let ns = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "test"}});
    db.create_resource("v1", "Namespace", None, "test", ns)
        .await
        .unwrap();

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "pod-1",
            "namespace": "test",
            "labels": {"app": "demo"}
        },
        "spec": {
            "containers": [{
                "name": "demo",
                "ports": [{"name": "dest1", "containerPort": 160, "protocol": "TCP"}]
            }]
        },
        "status": {
            "podIP": "10.43.0.10",
            "conditions": [{"type": "Ready", "status": "True"}]
        }
    });
    db.create_resource("v1", "Pod", Some("test"), "pod-1", pod)
        .await
        .unwrap();

    let selector = json!({"app": "demo"});
    let ports = json!([{
        "name": "portname1",
        "port": 80,
        "targetPort": "dest1",
        "protocol": "TCP"
    }]);

    reconcile_endpoints(
        &controller_store(&db),
        crate::controller_test_support::pod_repository_for_test(&db).as_ref(),
        "svc",
        "test",
        Some(&selector),
        Some(&ports),
        false,
    )
    .await
    .unwrap();

    let ep = db
        .get_resource("v1", "Endpoints", Some("test"), "svc")
        .await
        .unwrap()
        .unwrap();

    let ep_ports = ep.data["subsets"][0]["ports"].as_array().unwrap();
    assert_eq!(ep_ports.len(), 1);
    assert_eq!(ep_ports[0]["port"], 160);
    assert_eq!(
        ep_ports[0]["name"], "portname1",
        "Endpoints port name must preserve Service port name for named proxy routing"
    );
}

#[tokio::test]
async fn test_endpointslice_deleted_when_service_deleted_via_cascade() {
    let db = crate::datastore::test_support::in_memory().await;

    // Create namespace
    let ns = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "test"}});
    db.create_resource("v1", "Namespace", None, "test", ns)
        .await
        .unwrap();

    // Create a Service with ownerReferences
    let service = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": "test-service",
            "namespace": "test",
            "uid": "test-service-uid-123"
        },
        "spec": {
            "selector": {"app": "nginx"},
            "ports": [{"port": 80, "targetPort": 8080}]
        }
    });
    db.create_resource("v1", "Service", Some("test"), "test-service", service)
        .await
        .unwrap();

    // Create a Pod that matches the service selector
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "nginx-pod",
            "namespace": "test",
            "labels": {"app": "nginx"}
        },
        "status": {
            "podIP": "10.43.0.3",
            "conditions": [{"type": "Ready", "status": "True"}]
        }
    });
    db.create_resource("v1", "Pod", Some("test"), "nginx-pod", pod)
        .await
        .unwrap();

    // Reconcile to create EndpointSlice
    let selector = json!({"app": "nginx"});
    let ports = json!([{"port": 80, "targetPort": 8080}]);
    reconcile_endpointslice(
        &controller_store(&db),
        crate::controller_test_support::pod_repository_for_test(&db).as_ref(),
        "test-service",
        "test-service-uid-123",
        "test",
        Some(&selector),
        Some(&ports),
    )
    .await
    .unwrap();

    // Verify EndpointSlice exists
    let endpointslice = db
        .get_resource(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some("test"),
            "test-service-klights",
        )
        .await
        .unwrap();
    assert!(
        endpointslice.is_some(),
        "EndpointSlice should exist after reconciliation"
    );

    // Now simulate cascade delete (what happens when Service is deleted)
    let coordination = klights_controllers::ControllerCoordination::new();
    klights_controllers::gc::cascade_delete_with_uid(
        &controller_store(&db),
        "test-service-uid-123",
        "v1",
        "test-service",
        "Service",
        Some("test".to_string()),
        &crate::gc_ownership_integration_tests::NoOpGcPodDeleteSink,
        &crate::bootstrap::controller_adapters::gc_delete_adapter::GcNonPodFinalizationAdapter::new(
            std::sync::Arc::new(db.clone()),
        ),
        &coordination,
    )
    .await
    .unwrap();

    // Verify EndpointSlice was cascade deleted
    let endpointslice_after = db
        .get_resource(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some("test"),
            "test-service-klights",
        )
        .await
        .unwrap();
    assert!(
        endpointslice_after.is_none(),
        "EndpointSlice should be cascade deleted when Service is deleted"
    );
}
