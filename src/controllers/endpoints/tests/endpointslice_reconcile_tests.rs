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
    let endpoints = crate::api::inject_resource_version(created.data, created.resource_version);

    mirror_endpoints_to_endpointslice(&db, &endpoints)
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
    let stale_snapshot =
        crate::api::inject_resource_version(created.data, created.resource_version);

    db.delete_resource("v1", "Endpoints", Some("default"), "gone-svc")
        .await
        .unwrap();

    mirror_endpoints_to_endpointslice(&db, &stale_snapshot)
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
async fn test_reconcile_endpointslice_named_target_port_resolves_to_container_port() {
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
    let ports = json!([{"port": 80, "targetPort": "http", "protocol": "TCP", "name": "http"}]);

    reconcile_endpointslice(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        "nginx-service",
        "test-service-uid",
        "test",
        Some(&selector),
        Some(&ports),
    )
    .await
    .unwrap();

    let slice = db
        .get_resource(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some("test"),
            "nginx-service-klights",
        )
        .await
        .unwrap()
        .unwrap();

    let slice_ports = slice.data["ports"].as_array().unwrap();
    assert_eq!(slice_ports.len(), 1);
    assert_eq!(
        slice_ports[0]["port"], 8080,
        "Named targetPort 'http' should resolve to container port 8080, not service port 80 or 0"
    );
}

#[tokio::test]
async fn test_reconcile_endpointslice_splits_named_target_ports_per_resolved_port() {
    let db = crate::datastore::test_support::in_memory().await;

    let ns = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "test"}});
    db.create_resource("v1", "Namespace", None, "test", ns)
        .await
        .unwrap();

    for (name, ip, port) in [("pod1", "10.43.0.5", 3000), ("pod2", "10.43.0.6", 3001)] {
        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": name,
                "namespace": "test",
                "labels": {"shared": "on"},
                "uid": format!("{name}-uid")
            },
            "spec": {
                "containers": [{
                    "name": "nginx",
                    "ports": [{"name": "example-name", "containerPort": port, "protocol": "TCP"}]
                }]
            },
            "status": {
                "podIP": ip,
                "conditions": [{"type": "Ready", "status": "True"}]
            }
        });
        db.create_resource("v1", "Pod", Some("test"), name, pod)
            .await
            .unwrap();
    }

    let selector = json!({"shared": "on"});
    let ports = json!([{
        "name": "http",
        "port": 80,
        "targetPort": "example-name",
        "protocol": "TCP"
    }]);

    reconcile_endpointslice(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        "example-named-port",
        "svc-uid",
        "test",
        Some(&selector),
        Some(&ports),
    )
    .await
    .unwrap();

    let slices = db
        .list_resources(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some("test"),
            crate::datastore::ResourceListQuery::new(
                Some("kubernetes.io/service-name=example-named-port"),
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
        "named targetPorts resolving to different pod ports require separate EndpointSlices"
    );
    let mut seen_ports: Vec<i64> = slices
        .items
        .iter()
        .map(|slice| {
            assert_eq!(slice.data["endpoints"].as_array().unwrap().len(), 1);
            slice.data["ports"][0]["port"].as_i64().unwrap()
        })
        .collect();
    seen_ports.sort_unstable();
    assert_eq!(seen_ports, vec![3000, 3001]);
}

#[tokio::test]
async fn test_reconcile_endpointslice_create_conflict_recovers_to_desired_state() {
    let db = crate::datastore::test_support::in_memory().await;

    let ns = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "test"}});
    db.create_resource("v1", "Namespace", None, "test", ns)
        .await
        .unwrap();

    for (name, ip, port) in [("pod1", "10.43.0.5", 3000), ("pod2", "10.43.0.6", 3001)] {
        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": name,
                "namespace": "test",
                "labels": {"shared": "on"},
                "uid": format!("{name}-uid")
            },
            "spec": {
                "containers": [{
                    "name": "nginx",
                    "ports": [{"name": "example-name", "containerPort": port, "protocol": "TCP"}]
                }]
            },
            "status": {
                "podIP": ip,
                "conditions": [{"type": "Ready", "status": "True"}]
            }
        });
        db.create_resource("v1", "Pod", Some("test"), name, pod)
            .await
            .unwrap();
    }

    let selector = json!({"shared": "on"});
    let ports = json!([{
        "name": "http",
        "port": 80,
        "targetPort": "example-name",
        "protocol": "TCP"
    }]);
    let service_name = "example-named-port";
    let desired_slice_name = format!("{service_name}-klights-1");

    reconcile_endpointslice(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        service_name,
        "svc-uid",
        "test",
        Some(&selector),
        Some(&ports),
    )
    .await
    .unwrap();

    // Force a concurrent create race by replacing the canonical slice with a stale
    // object that is intentionally not discoverable through label selectors.
    db.delete_resource(
        "discovery.k8s.io/v1",
        "EndpointSlice",
        Some("test"),
        &desired_slice_name,
    )
    .await
    .unwrap();
    let stale_slice = json!({
        "apiVersion": "discovery.k8s.io/v1",
        "kind": "EndpointSlice",
        "metadata": {
            "name": desired_slice_name,
            "namespace": "test",
            "labels": {"stale": "true"},
        },
        "addressType": "IPv4",
        "endpoints": [{
            "addresses": ["10.43.0.250"],
            "conditions": {
                "ready": false,
                "serving": false,
                "terminating": false
            }
        }],
        "ports": [{"name":"stale","port": 65535, "protocol":"TCP"}]
    });
    db.create_resource(
        "discovery.k8s.io/v1",
        "EndpointSlice",
        Some("test"),
        &desired_slice_name,
        stale_slice,
    )
    .await
    .unwrap();

    reconcile_endpointslice(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        service_name,
        "svc-uid",
        "test",
        Some(&selector),
        Some(&ports),
    )
    .await
    .unwrap();

    let refreshed = db
        .get_resource(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some("test"),
            &desired_slice_name,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        refreshed.data["metadata"]["labels"]["endpointslice.kubernetes.io/managed-by"],
        "endpointslice-controller.k8s.io",
        "race-recovered EndpointSlice must keep controller managed-by label"
    );
    assert_eq!(
        refreshed.data["metadata"]["labels"]["kubernetes.io/service-name"], service_name,
        "race-recovered EndpointSlice must remain tied to the service"
    );
    assert_ne!(
        refreshed.data["ports"][0]["port"], 65535,
        "stale conflicting EndpointSlice should be converged to desired ports"
    );
}

#[tokio::test]
async fn test_reconcile_endpointslice_numeric_string_target_port_and_skip_unresolved_named_port() {
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

    reconcile_endpointslice(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        "multi-endpoint-test",
        "svc-uid-3",
        "test",
        Some(&selector),
        Some(&ports),
    )
    .await
    .unwrap();

    let slice = db
        .get_resource(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some("test"),
            "multi-endpoint-test-klights",
        )
        .await
        .unwrap()
        .unwrap();

    let slice_ports = slice.data["ports"].as_array().unwrap();
    assert_eq!(
        slice_ports.len(),
        1,
        "unresolved named targetPort must be skipped"
    );
    assert_eq!(slice_ports[0]["name"], "portname1");
    assert_eq!(
        slice_ports[0]["port"], 100,
        "numeric-string targetPort must be interpreted as integer 100"
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
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
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
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
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
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
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
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
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
    crate::controllers::gc::cascade_delete_with_uid(
        &db,
        "test-service-uid-123",
        "v1",
        "test-service",
        "Service",
        Some("test".to_string()),
        &crate::controllers::gc::NoOpGcPodDeleteSink,
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

#[tokio::test]
async fn test_endpointslice_ports_match_service_targetport() {
    let db = crate::datastore::test_support::in_memory().await;

    let ns = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "test"}});
    db.create_resource("v1", "Namespace", None, "test", ns)
        .await
        .unwrap();

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "web-1", "namespace": "test", "labels": {"app": "web"}},
        "spec": {"containers": [{"name": "web", "ports": [{"containerPort": 8080}]}]},
        "status": {"podIP": "10.43.0.5", "conditions": [{"type": "Ready", "status": "True"}]}
    });
    db.create_resource("v1", "Pod", Some("test"), "web-1", pod)
        .await
        .unwrap();

    // Service port 80 maps to targetPort 8080
    let selector = json!({"app": "web"});
    let ports = json!([{"port": 80, "targetPort": 8080, "protocol": "TCP"}]);

    reconcile_endpointslice(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        "web-svc",
        "svc-uid-1",
        "test",
        Some(&selector),
        Some(&ports),
    )
    .await
    .unwrap();

    let slice = db
        .get_resource(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some("test"),
            "web-svc-klights",
        )
        .await
        .unwrap()
        .unwrap();

    let slice_ports = slice.data["ports"].as_array().unwrap();
    assert_eq!(slice_ports.len(), 1);
    assert_eq!(
        slice_ports[0]["port"], 8080,
        "EndpointSlice port must equal Service targetPort (8080)"
    );
}

#[tokio::test]
async fn test_endpointslice_ports_use_service_port_when_no_targetport() {
    let db = crate::datastore::test_support::in_memory().await;

    let ns = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "test"}});
    db.create_resource("v1", "Namespace", None, "test", ns)
        .await
        .unwrap();

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "web-1", "namespace": "test", "labels": {"app": "web"}},
        "status": {"podIP": "10.43.0.5", "conditions": [{"type": "Ready", "status": "True"}]}
    });
    db.create_resource("v1", "Pod", Some("test"), "web-1", pod)
        .await
        .unwrap();

    // Service port 9000, no targetPort
    let selector = json!({"app": "web"});
    let ports = json!([{"port": 9000, "protocol": "TCP"}]);

    reconcile_endpointslice(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        "web-svc",
        "svc-uid-2",
        "test",
        Some(&selector),
        Some(&ports),
    )
    .await
    .unwrap();

    let slice = db
        .get_resource(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some("test"),
            "web-svc-klights",
        )
        .await
        .unwrap()
        .unwrap();

    let slice_ports = slice.data["ports"].as_array().unwrap();
    assert_eq!(slice_ports.len(), 1);
    assert_eq!(
        slice_ports[0]["port"], 9000,
        "EndpointSlice port must equal Service port (9000) when no targetPort is set"
    );
}

#[tokio::test]
async fn test_endpointslice_ports_not_zero() {
    let db = crate::datastore::test_support::in_memory().await;

    let ns = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "test"}});
    db.create_resource("v1", "Namespace", None, "test", ns)
        .await
        .unwrap();

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "web-1", "namespace": "test", "labels": {"app": "web"}},
        "spec": {"containers": [{"name": "web", "ports": [{"name": "http", "containerPort": 8080}]}]},
        "status": {"podIP": "10.43.0.5", "conditions": [{"type": "Ready", "status": "True"}]}
    });
    db.create_resource("v1", "Pod", Some("test"), "web-1", pod)
        .await
        .unwrap();

    // Test both integer targetPort and named targetPort — neither should produce 0
    for (test_case, ports_val) in [
        (
            "integer targetPort",
            json!([{"port": 80, "targetPort": 8080, "protocol": "TCP"}]),
        ),
        (
            "named targetPort",
            json!([{"port": 80, "targetPort": "http", "protocol": "TCP"}]),
        ),
        ("no targetPort", json!([{"port": 7070, "protocol": "TCP"}])),
    ] {
        // Use unique service names per case to avoid conflicts
        let svc_name = format!("web-svc-{}", test_case.replace(' ', "-"));
        let slice_name = format!("{}-klights", svc_name);
        reconcile_endpointslice(
            &db,
            crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
            &svc_name,
            &format!("uid-{}", test_case),
            "test",
            Some(&json!({"app": "web"})),
            Some(&ports_val),
        )
        .await
        .unwrap();

        let slice = db
            .get_resource(
                "discovery.k8s.io/v1",
                "EndpointSlice",
                Some("test"),
                &slice_name.clone(),
            )
            .await
            .unwrap()
            .unwrap();

        let slice_ports = slice.data["ports"].as_array().unwrap();
        for port_obj in slice_ports {
            let port_num = port_obj["port"].as_u64().unwrap();
            assert_ne!(
                port_num, 0,
                "EndpointSlice port must not be 0 (case: {test_case})"
            );
        }
    }
}
