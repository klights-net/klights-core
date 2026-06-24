use super::*;

#[test]
fn test_endpoints_desired_state_match_ignores_metadata_resource_version() {
    let current = json!({
        "apiVersion": "v1",
        "kind": "Endpoints",
        "metadata": {"name": "svc", "namespace": "default", "resourceVersion": "10"},
        "subsets": [{"addresses": [{"ip": "10.43.0.10"}], "ports": [{"port": 80}]}]
    });
    let desired = json!({
        "apiVersion": "v1",
        "kind": "Endpoints",
        "metadata": {"name": "svc", "namespace": "default"},
        "subsets": [{"addresses": [{"ip": "10.43.0.10"}], "ports": [{"port": 80}]}]
    });

    assert!(
        endpoints_desired_state_matches(&current, &desired),
        "conflict retry should treat an already-updated Endpoints object as converged"
    );
}

#[test]
fn test_endpointslice_desired_state_match_detects_endpoint_changes() {
    let current = json!({
        "apiVersion": "discovery.k8s.io/v1",
        "kind": "EndpointSlice",
        "metadata": {"name": "svc-klights", "namespace": "default", "resourceVersion": "10"},
        "endpoints": [{"addresses": ["10.43.0.10"], "conditions": {"ready": true}}],
        "ports": [{"port": 80, "protocol": "TCP"}]
    });
    let desired = json!({
        "apiVersion": "discovery.k8s.io/v1",
        "kind": "EndpointSlice",
        "metadata": {"name": "svc-klights", "namespace": "default"},
        "endpoints": [{"addresses": ["10.43.0.11"], "conditions": {"ready": true}}],
        "ports": [{"port": 80, "protocol": "TCP"}]
    });

    assert!(
        !endpointslice_desired_state_matches(&current, &desired),
        "conflict retry must continue when the refreshed EndpointSlice still has stale endpoints"
    );
}

#[tokio::test]
async fn service_endpoint_batch_reconcile_creates_slice_and_endpoints_with_same_rv() {
    let db = crate::datastore::test_support::in_memory().await;
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "batch-pod",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "batch-pod",
                "namespace": "default",
                "uid": "batch-pod-uid",
                "labels": {"app": "batch"}
            },
            "spec": {
                "containers": [{
                    "name": "app",
                    "image": "nginx",
                    "ports": [{"containerPort": 8080}]
                }]
            },
            "status": {
                "phase": "Running",
                "podIP": "10.43.0.30",
                "conditions": [{"type": "Ready", "status": "True"}]
            }
        }),
    )
    .await
    .unwrap();

    reconcile_service_endpoints_batch(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        ServiceEndpointBatchReconcileRequest {
            service_name: "batch-svc",
            service_uid: "batch-svc-uid",
            namespace: "default",
            selector: Some(&json!({"app": "batch"})),
            service_ports: Some(
                &json!([{"name": "http", "port": 80, "targetPort": 8080, "protocol": "TCP"}]),
            ),
            publish_not_ready: false,
        },
    )
    .await
    .unwrap();

    let endpoints = db
        .get_resource("v1", "Endpoints", Some("default"), "batch-svc")
        .await
        .unwrap()
        .expect("Endpoints should exist");
    let slices = db
        .list_resources(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some("default"),
            crate::datastore::ResourceListQuery::new(
                Some("kubernetes.io/service-name=batch-svc"),
                None,
                None,
                None,
            ),
        )
        .await
        .unwrap();
    let slice = slices.items.first().expect("EndpointSlice should exist");

    assert_eq!(slice.resource_version, endpoints.resource_version);
}

#[tokio::test]
async fn service_endpoint_batch_reconcile_is_noop_when_desired_state_matches() {
    let db = crate::datastore::test_support::in_memory().await;
    let pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);
    reconcile_service_endpoints_batch(
        &db,
        pod_repo.as_ref(),
        ServiceEndpointBatchReconcileRequest {
            service_name: "noop-svc",
            service_uid: "noop-svc-uid",
            namespace: "default",
            selector: Some(&json!({"app": "none"})),
            service_ports: Some(&json!([{"port": 80, "targetPort": 80, "protocol": "TCP"}])),
            publish_not_ready: false,
        },
    )
    .await
    .unwrap();
    let rv_after_first = db.get_current_resource_version().await.unwrap();

    reconcile_service_endpoints_batch(
        &db,
        pod_repo.as_ref(),
        ServiceEndpointBatchReconcileRequest {
            service_name: "noop-svc",
            service_uid: "noop-svc-uid",
            namespace: "default",
            selector: Some(&json!({"app": "none"})),
            service_ports: Some(&json!([{"port": 80, "targetPort": 80, "protocol": "TCP"}])),
            publish_not_ready: false,
        },
    )
    .await
    .unwrap();
    let rv_after_second = db.get_current_resource_version().await.unwrap();

    assert_eq!(rv_after_second, rv_after_first);
}

#[tokio::test]
async fn test_reconcile_endpoints_skips_terminating_namespace() {
    let db = crate::datastore::test_support::in_memory().await;

    db.create_namespace(
        "terminating",
        json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {
                "name": "terminating",
                "deletionTimestamp": crate::utils::k8s_timestamp()
            },
            "spec": {"finalizers": ["kubernetes"]},
            "status": {"phase": "Terminating"}
        }),
    )
    .await
    .unwrap();

    db.create_resource(
        "v1",
        "Pod",
        Some("terminating"),
        "pod-a",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "pod-a",
                "namespace": "terminating",
                "labels": {"app": "latency"}
            },
            "status": {
                "podIP": "10.42.0.10",
                "conditions": [{"type": "Ready", "status": "True"}]
            }
        }),
    )
    .await
    .unwrap();

    let selector = json!({"app": "latency"});
    let ports = json!([{"port": 80, "targetPort": 80}]);
    let pod_reader = crate::controllers::test_utils::pod_repository_for_test(&db);

    reconcile_endpoints(
        &db,
        pod_reader.as_ref(),
        "latency-svc",
        "terminating",
        Some(&selector),
        Some(&ports),
        false,
    )
    .await
    .unwrap();
    reconcile_endpointslice(
        &db,
        pod_reader.as_ref(),
        "latency-svc",
        "svc-uid",
        "terminating",
        Some(&selector),
        Some(&ports),
    )
    .await
    .unwrap();

    assert!(
        db.get_resource("v1", "Endpoints", Some("terminating"), "latency-svc")
            .await
            .unwrap()
            .is_none(),
        "endpoint reconciliation must not create Endpoints in a terminating namespace"
    );
    assert!(
        db.get_resource(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some("terminating"),
            "latency-svc-klights",
        )
        .await
        .unwrap()
        .is_none(),
        "endpoint reconciliation must not create EndpointSlices in a terminating namespace"
    );
}

#[tokio::test]
async fn test_reconcile_endpointslice_includes_hostname_for_statefulset_pods() {
    let db = crate::datastore::test_support::in_memory().await;

    let ns = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "default"}});
    db.create_resource("v1", "Namespace", None, "default", ns)
        .await
        .unwrap();

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "web-0",
            "namespace": "default",
            "labels": {"app": "web"}
        },
        "spec": {
            "hostname": "web-0",
            "subdomain": "web-headless"
        },
        "status": {
            "podIP": "10.43.0.5",
            "conditions": [{"type": "Ready", "status": "True"}]
        }
    });
    db.create_resource("v1", "Pod", Some("default"), "web-0", pod)
        .await
        .unwrap();

    let selector = json!({"app": "web"});
    let ports = json!([{"port": 80, "protocol": "TCP"}]);

    reconcile_endpointslice(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        "web-headless",
        "test-service-uid",
        "default",
        Some(&selector),
        Some(&ports),
    )
    .await
    .unwrap();

    let slice = db
        .get_resource(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some("default"),
            "web-headless-klights",
        )
        .await
        .unwrap()
        .unwrap();

    let endpoints = slice.data["endpoints"].as_array().unwrap();
    assert_eq!(endpoints.len(), 1);
    assert_eq!(
        endpoints[0]["hostname"], "web-0",
        "EndpointSlice should include hostname for StatefulSet pods"
    );
}

// P2: Endpoint reconciliation deduplication tests
#[tokio::test]
async fn test_endpoint_reconciliation_dedup_skips_when_unchanged() {
    let db = crate::datastore::test_support::in_memory().await;

    // Create namespace
    let ns = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "test"}});
    db.create_resource("v1", "Namespace", None, "test", ns)
        .await
        .unwrap();

    // Create pod
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "nginx-1",
            "namespace": "test",
            "labels": {"app": "nginx"}
        },
        "status": {"podIP": "10.42.0.5", "conditions": [{"type": "Ready", "status": "True"}]}
    });
    db.create_resource("v1", "Pod", Some("test"), "nginx-1", pod)
        .await
        .unwrap();

    let selector = json!({"app": "nginx"});
    let ports = json!([{"port": 80}]);

    // First reconcile creates endpoints
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

    let first_rv = db
        .get_resource("v1", "Endpoints", Some("test"), "nginx-service")
        .await
        .unwrap()
        .unwrap()
        .resource_version;

    // Second reconcile with same data should NOT update (no change)
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

    let second_rv = db
        .get_resource("v1", "Endpoints", Some("test"), "nginx-service")
        .await
        .unwrap()
        .unwrap()
        .resource_version;

    // Resource version should NOT increment if nothing changed
    assert_eq!(
        first_rv, second_rv,
        "Reconciling unchanged endpoints should not increment resource version"
    );
}

#[tokio::test]
async fn test_endpoint_reconciliation_dedup_updates_when_changed() {
    let db = crate::datastore::test_support::in_memory().await;

    // Create namespace
    let ns = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "test"}});
    db.create_resource("v1", "Namespace", None, "test", ns)
        .await
        .unwrap();

    // Create initial pod
    let pod1 = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "nginx-1",
            "namespace": "test",
            "labels": {"app": "nginx"}
        },
        "status": {"podIP": "10.42.0.5", "conditions": [{"type": "Ready", "status": "True"}]}
    });
    db.create_resource("v1", "Pod", Some("test"), "nginx-1", pod1)
        .await
        .unwrap();

    let selector = json!({"app": "nginx"});
    let ports = json!([{"port": 80}]);

    // First reconcile
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

    let first_rv = db
        .get_resource("v1", "Endpoints", Some("test"), "nginx-service")
        .await
        .unwrap()
        .unwrap()
        .resource_version;

    // Add second pod (actual change)
    let pod2 = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "nginx-2",
            "namespace": "test",
            "labels": {"app": "nginx"}
        },
        "status": {"podIP": "10.42.0.6", "conditions": [{"type": "Ready", "status": "True"}]}
    });
    db.create_resource("v1", "Pod", Some("test"), "nginx-2", pod2)
        .await
        .unwrap();

    // Second reconcile with changed data SHOULD update
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

    let second_rv = db
        .get_resource("v1", "Endpoints", Some("test"), "nginx-service")
        .await
        .unwrap()
        .unwrap()
        .resource_version;

    // Resource version SHOULD increment when endpoints actually changed
    assert!(
        second_rv > first_rv,
        "Resource version should increment when endpoints change"
    );
}

#[tokio::test]
async fn test_reconcile_endpoints_ready_pods_in_addresses() {
    let db = crate::datastore::test_support::in_memory().await;

    // Create namespace
    let ns = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "test"}});
    db.create_resource("v1", "Namespace", None, "test", ns)
        .await
        .unwrap();

    // Create a Ready pod
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "ready-pod",
            "namespace": "test",
            "labels": {"app": "nginx"}
        },
        "status": {
            "podIP": "10.42.0.5",
            "conditions": [{"type": "Ready", "status": "True"}]
        }
    });
    db.create_resource("v1", "Pod", Some("test"), "ready-pod", pod)
        .await
        .unwrap();

    let selector = json!({"app": "nginx"});
    let ports = json!([{"port": 80, "protocol": "TCP"}]);

    reconcile_endpoints(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        "nginx-svc",
        "test",
        Some(&selector),
        Some(&ports),
        false,
    )
    .await
    .unwrap();

    let ep = db
        .get_resource("v1", "Endpoints", Some("test"), "nginx-svc")
        .await
        .unwrap()
        .unwrap();

    let subsets = ep.data["subsets"].as_array().unwrap();
    assert_eq!(subsets.len(), 1, "Should have 1 subset");

    let addresses = subsets[0]["addresses"].as_array().unwrap();
    assert_eq!(addresses.len(), 1, "Ready pod should be in addresses");
    assert_eq!(addresses[0]["ip"], "10.42.0.5");

    // notReadyAddresses should not exist or be empty
    let not_ready = subsets[0]
        .get("notReadyAddresses")
        .and_then(|v| v.as_array());
    assert!(
        not_ready.is_none() || not_ready.unwrap().is_empty(),
        "No not-ready addresses expected"
    );
}

#[tokio::test]
async fn test_reconcile_endpoints_and_slices_exclude_terminating_pods() {
    let db = crate::datastore::test_support::in_memory().await;

    let ns = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "test"}});
    db.create_resource("v1", "Namespace", None, "test", ns)
        .await
        .unwrap();

    let live_pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "live-pod",
            "namespace": "test",
            "uid": "live-pod-uid",
            "labels": {"app": "nginx"}
        },
        "status": {
            "podIP": "10.42.0.5",
            "conditions": [{"type": "Ready", "status": "True"}]
        }
    });
    db.create_resource("v1", "Pod", Some("test"), "live-pod", live_pod)
        .await
        .unwrap();

    let terminating_pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "terminating-pod",
            "namespace": "test",
            "uid": "terminating-pod-uid",
            "labels": {"app": "nginx"},
            "deletionTimestamp": "2026-05-05T00:00:00Z"
        },
        "status": {
            "podIP": "10.42.0.6",
            "conditions": [{"type": "Ready", "status": "True"}]
        }
    });
    db.create_resource(
        "v1",
        "Pod",
        Some("test"),
        "terminating-pod",
        terminating_pod,
    )
    .await
    .unwrap();

    let selector = json!({"app": "nginx"});
    let ports = json!([{"port": 80, "protocol": "TCP"}]);
    let pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);

    reconcile_endpoints(
        &db,
        pod_repo.as_ref(),
        "nginx-svc",
        "test",
        Some(&selector),
        Some(&ports),
        false,
    )
    .await
    .unwrap();
    reconcile_endpointslice(
        &db,
        pod_repo.as_ref(),
        "nginx-svc",
        "nginx-svc-uid",
        "test",
        Some(&selector),
        Some(&ports),
    )
    .await
    .unwrap();

    let ep = db
        .get_resource("v1", "Endpoints", Some("test"), "nginx-svc")
        .await
        .unwrap()
        .unwrap();
    let addresses = ep.data["subsets"][0]["addresses"].as_array().unwrap();
    assert_eq!(addresses.len(), 1);
    assert_eq!(addresses[0]["targetRef"]["name"], "live-pod");

    let slice = db
        .get_resource(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some("test"),
            "nginx-svc-klights",
        )
        .await
        .unwrap()
        .unwrap();
    let endpoints = slice.data["endpoints"].as_array().unwrap();
    assert_eq!(endpoints.len(), 1);
    assert_eq!(endpoints[0]["targetRef"]["name"], "live-pod");
}

#[tokio::test]
async fn test_reconcile_endpoints_not_ready_pods_in_not_ready_addresses() {
    let db = crate::datastore::test_support::in_memory().await;

    // Create namespace
    let ns = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "test"}});
    db.create_resource("v1", "Namespace", None, "test", ns)
        .await
        .unwrap();

    // Create a Ready pod
    let ready_pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "ready-pod",
            "namespace": "test",
            "labels": {"app": "nginx"}
        },
        "status": {
            "podIP": "10.42.0.5",
            "conditions": [{"type": "Ready", "status": "True"}]
        }
    });
    db.create_resource("v1", "Pod", Some("test"), "ready-pod", ready_pod)
        .await
        .unwrap();

    // Create a not-Ready pod
    let not_ready_pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "not-ready-pod",
            "namespace": "test",
            "labels": {"app": "nginx"}
        },
        "status": {
            "podIP": "10.42.0.6",
            "conditions": [{"type": "Ready", "status": "False"}]
        }
    });
    db.create_resource("v1", "Pod", Some("test"), "not-ready-pod", not_ready_pod)
        .await
        .unwrap();

    // Create a pod with no conditions (not ready)
    let no_conditions_pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "no-cond-pod",
            "namespace": "test",
            "labels": {"app": "nginx"}
        },
        "status": {
            "podIP": "10.42.0.7"
        }
    });
    db.create_resource("v1", "Pod", Some("test"), "no-cond-pod", no_conditions_pod)
        .await
        .unwrap();

    let selector = json!({"app": "nginx"});
    let ports = json!([{"port": 80, "protocol": "TCP"}]);

    reconcile_endpoints(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        "nginx-svc",
        "test",
        Some(&selector),
        Some(&ports),
        false,
    )
    .await
    .unwrap();

    let ep = db
        .get_resource("v1", "Endpoints", Some("test"), "nginx-svc")
        .await
        .unwrap()
        .unwrap();

    let subsets = ep.data["subsets"].as_array().unwrap();
    assert_eq!(subsets.len(), 1, "Should have 1 subset");

    // Ready pod in addresses
    let addresses = subsets[0]["addresses"].as_array().unwrap();
    assert_eq!(addresses.len(), 1, "Only ready pod in addresses");
    assert_eq!(addresses[0]["ip"], "10.42.0.5");

    // Not-ready pods in notReadyAddresses
    let not_ready = subsets[0]["notReadyAddresses"].as_array().unwrap();
    assert_eq!(
        not_ready.len(),
        2,
        "Two not-ready pods in notReadyAddresses"
    );
    let not_ready_ips: Vec<&str> = not_ready
        .iter()
        .map(|a| a["ip"].as_str().unwrap())
        .collect();
    assert!(not_ready_ips.contains(&"10.42.0.6"));
    assert!(not_ready_ips.contains(&"10.42.0.7"));
}

#[tokio::test]
async fn test_reconcile_endpoints_publish_not_ready_all_in_addresses() {
    let db = crate::datastore::test_support::in_memory().await;

    // Create namespace
    let ns = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "test"}});
    db.create_resource("v1", "Namespace", None, "test", ns)
        .await
        .unwrap();

    // Create a Ready pod
    let ready_pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "ready-pod",
            "namespace": "test",
            "labels": {"app": "nginx"}
        },
        "status": {
            "podIP": "10.42.0.5",
            "conditions": [{"type": "Ready", "status": "True"}]
        }
    });
    db.create_resource("v1", "Pod", Some("test"), "ready-pod", ready_pod)
        .await
        .unwrap();

    // Create a not-Ready pod
    let not_ready_pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "not-ready-pod",
            "namespace": "test",
            "labels": {"app": "nginx"}
        },
        "status": {
            "podIP": "10.42.0.6",
            "conditions": [{"type": "Ready", "status": "False"}]
        }
    });
    db.create_resource("v1", "Pod", Some("test"), "not-ready-pod", not_ready_pod)
        .await
        .unwrap();

    let selector = json!({"app": "nginx"});
    let ports = json!([{"port": 80, "protocol": "TCP"}]);

    // publish_not_ready = true: all pods go into addresses
    reconcile_endpoints(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        "nginx-svc",
        "test",
        Some(&selector),
        Some(&ports),
        true,
    )
    .await
    .unwrap();

    let ep = db
        .get_resource("v1", "Endpoints", Some("test"), "nginx-svc")
        .await
        .unwrap()
        .unwrap();

    let subsets = ep.data["subsets"].as_array().unwrap();
    assert_eq!(subsets.len(), 1, "Should have 1 subset");

    // All pods in addresses regardless of readiness
    let addresses = subsets[0]["addresses"].as_array().unwrap();
    assert_eq!(addresses.len(), 2, "Both pods should be in addresses");
    let ips: Vec<&str> = addresses
        .iter()
        .map(|a| a["ip"].as_str().unwrap())
        .collect();
    assert!(ips.contains(&"10.42.0.5"));
    assert!(ips.contains(&"10.42.0.6"));

    // notReadyAddresses should not exist or be empty
    let not_ready = subsets[0]
        .get("notReadyAddresses")
        .and_then(|v| v.as_array());
    assert!(
        not_ready.is_none() || not_ready.unwrap().is_empty(),
        "No not-ready addresses when publishNotReadyAddresses=true"
    );
}

#[tokio::test]
async fn test_reconcile_endpoints_includes_hostname_for_statefulset_pods() {
    let db = crate::datastore::test_support::in_memory().await;

    let ns = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "default"}});
    db.create_resource("v1", "Namespace", None, "default", ns)
        .await
        .unwrap();

    // StatefulSet pod with hostname and subdomain matching the service name
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "web-0",
            "namespace": "default",
            "labels": {"app": "web"}
        },
        "spec": {
            "hostname": "web-0",
            "subdomain": "web-headless"
        },
        "status": {
            "podIP": "10.43.0.5",
            "conditions": [{"type": "Ready", "status": "True"}]
        }
    });
    db.create_resource("v1", "Pod", Some("default"), "web-0", pod)
        .await
        .unwrap();

    let selector = json!({"app": "web"});
    let ports = json!([{"port": 80, "protocol": "TCP"}]);

    // Service name matches subdomain
    reconcile_endpoints(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        "web-headless",
        "default",
        Some(&selector),
        Some(&ports),
        false,
    )
    .await
    .unwrap();

    let ep = db
        .get_resource("v1", "Endpoints", Some("default"), "web-headless")
        .await
        .unwrap()
        .unwrap();

    let subsets = ep.data["subsets"].as_array().unwrap();
    assert_eq!(subsets.len(), 1);

    let addresses = subsets[0]["addresses"].as_array().unwrap();
    assert_eq!(addresses.len(), 1);
    assert_eq!(addresses[0]["ip"], "10.43.0.5");
    assert_eq!(
        addresses[0]["hostname"], "web-0",
        "StatefulSet pod should have hostname in Endpoints address"
    );
}

#[tokio::test]
async fn test_reconcile_endpoints_no_hostname_when_subdomain_differs() {
    let db = crate::datastore::test_support::in_memory().await;

    let ns = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "default"}});
    db.create_resource("v1", "Namespace", None, "default", ns)
        .await
        .unwrap();

    // Pod with subdomain that does NOT match the service name
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "web-0",
            "namespace": "default",
            "labels": {"app": "web"}
        },
        "spec": {
            "hostname": "web-0",
            "subdomain": "other-service"
        },
        "status": {
            "podIP": "10.43.0.5",
            "conditions": [{"type": "Ready", "status": "True"}]
        }
    });
    db.create_resource("v1", "Pod", Some("default"), "web-0", pod)
        .await
        .unwrap();

    let selector = json!({"app": "web"});
    let ports = json!([{"port": 80, "protocol": "TCP"}]);

    reconcile_endpoints(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        "web-headless",
        "default",
        Some(&selector),
        Some(&ports),
        false,
    )
    .await
    .unwrap();

    let ep = db
        .get_resource("v1", "Endpoints", Some("default"), "web-headless")
        .await
        .unwrap()
        .unwrap();

    let addresses = ep.data["subsets"][0]["addresses"].as_array().unwrap();
    // hostname should NOT be present when subdomain doesn't match service name
    assert!(
        addresses[0].get("hostname").is_none(),
        "No hostname when subdomain doesn't match service"
    );
}

#[tokio::test]
async fn test_endpointslice_managed_by_label() {
    let db = crate::datastore::test_support::in_memory().await;

    // Create namespace
    let ns = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "test"}});
    db.create_resource("v1", "Namespace", None, "test", ns)
        .await
        .unwrap();

    // Create service
    let svc = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "test-svc", "namespace": "test"},
        "spec": {
            "selector": {"app": "test"},
            "ports": [{"port": 80, "targetPort": 8080, "protocol": "TCP"}]
        }
    });
    db.create_resource("v1", "Service", Some("test"), "test-svc", svc.clone())
        .await
        .unwrap();

    // Create pod
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "test-pod", "namespace": "test", "labels": {"app": "test"}},
        "status": {
            "phase": "Running",
            "podIP": "10.43.0.5",
            "conditions": [{"type": "Ready", "status": "True"}]
        }
    });
    db.create_resource("v1", "Pod", Some("test"), "test-pod", pod)
        .await
        .unwrap();

    // Reconcile to create EndpointSlice
    let selector = svc.pointer("/spec/selector");
    let ports = svc.pointer("/spec/ports");
    let service_uid = svc
        .pointer("/metadata/uid")
        .and_then(|u| u.as_str())
        .unwrap_or("");
    reconcile_endpointslice(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        "test-svc",
        service_uid,
        "test",
        selector,
        ports,
    )
    .await
    .unwrap();

    // Get the created EndpointSlice
    let slices = db
        .list_resources(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some("test"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();

    assert_eq!(slices.items.len(), 1, "Should have one EndpointSlice");
    let slice = &slices.items[0];

    // Verify managed-by label matches K8s convention
    assert_eq!(
        slice.data["metadata"]["labels"]["endpointslice.kubernetes.io/managed-by"],
        "endpointslice-controller.k8s.io",
        "EndpointSlice managed-by label must match K8s convention"
    );
}

#[tokio::test]
async fn test_reconcile_endpoints_splits_named_target_ports_per_resolved_port() {
    let db = crate::datastore::test_support::in_memory().await;

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "test",
        json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "test"}}),
    )
    .await
    .unwrap();

    for (name, ip, port) in [("pod1", "10.43.0.5", 3000), ("pod2", "10.43.0.6", 3001)] {
        db.create_resource(
            "v1",
            "Pod",
            Some("test"),
            name,
            json!({
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
            }),
        )
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

    reconcile_endpoints(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        "example-named-port",
        "test",
        Some(&selector),
        Some(&ports),
        true,
    )
    .await
    .unwrap();

    let endpoints = db
        .get_resource("v1", "Endpoints", Some("test"), "example-named-port")
        .await
        .unwrap()
        .unwrap();
    let subsets = endpoints.data["subsets"].as_array().unwrap();
    assert_eq!(
        subsets.len(),
        2,
        "named targetPorts resolving to different pod ports require separate Endpoints subsets"
    );
    let mut seen_ports: Vec<i64> = subsets
        .iter()
        .map(|subset| {
            assert_eq!(subset["addresses"].as_array().unwrap().len(), 1);
            subset["ports"][0]["port"].as_i64().unwrap()
        })
        .collect();
    seen_ports.sort_unstable();
    assert_eq!(seen_ports, vec![3000, 3001]);
}

#[tokio::test]
async fn test_mirror_endpoints_to_endpointslice_creates_matching_endpointslice() {
    let db = crate::datastore::test_support::in_memory().await;

    // Create namespace
    let ns = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "test"}});
    db.create_resource("v1", "Namespace", None, "test", ns)
        .await
        .unwrap();

    // Create manual Endpoints (not managed by Service controller)
    let endpoints = json!({
        "apiVersion": "v1",
        "kind": "Endpoints",
        "metadata": {
            "name": "my-service",
            "namespace": "test"
        },
        "subsets": [{
            "addresses": [
                {"ip": "10.42.0.5", "targetRef": {"kind": "Pod", "name": "pod-1"}},
                {"ip": "10.42.0.6", "targetRef": {"kind": "Pod", "name": "pod-2"}}
            ],
            "notReadyAddresses": [
                {"ip": "10.42.0.7", "targetRef": {"kind": "Pod", "name": "pod-3"}}
            ],
            "ports": [
                {"port": 80, "protocol": "TCP", "name": "http"},
                {"port": 443, "protocol": "TCP", "name": "https"}
            ]
        }]
    });

    let created = db
        .create_resource("v1", "Endpoints", Some("test"), "my-service", endpoints)
        .await
        .unwrap();
    let endpoints = crate::api::inject_resource_version(created.data, created.resource_version);

    // Call mirroring function
    mirror_endpoints_to_endpointslice(&db, &endpoints)
        .await
        .unwrap();

    // Verify EndpointSlice was created
    let endpointslice = db
        .get_resource(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some("test"),
            "my-service-mirror",
        )
        .await
        .unwrap();

    assert!(endpointslice.is_some());
    let ep_slice = endpointslice.unwrap();

    // Verify labels
    assert_eq!(
        ep_slice
            .data
            .pointer("/metadata/labels/kubernetes.io~1service-name"),
        Some(&json!("my-service"))
    );
    assert_eq!(
        ep_slice
            .data
            .pointer("/metadata/labels/endpointslice.kubernetes.io~1managed-by"),
        Some(&json!("endpointslicemirroring-controller.k8s.io"))
    );

    // Verify endpoints (2 ready + 1 not ready)
    let endpoints_list = ep_slice.data.get("endpoints").unwrap().as_array().unwrap();
    assert_eq!(endpoints_list.len(), 3);

    // Verify ready endpoints
    let ready_endpoints: Vec<_> = endpoints_list
        .iter()
        .filter(|e| e.pointer("/conditions/ready") == Some(&json!(true)))
        .collect();
    assert_eq!(ready_endpoints.len(), 2);

    // Verify not-ready endpoints
    let not_ready_endpoints: Vec<_> = endpoints_list
        .iter()
        .filter(|e| e.pointer("/conditions/ready") == Some(&json!(false)))
        .collect();
    assert_eq!(not_ready_endpoints.len(), 1);

    // Verify ports
    let ports = ep_slice.data.get("ports").unwrap().as_array().unwrap();
    assert_eq!(ports.len(), 2);
    assert_eq!(ports[0].get("port"), Some(&json!(80)));
    assert_eq!(ports[0].get("name"), Some(&json!("http")));
    assert_eq!(ports[1].get("port"), Some(&json!(443)));
    assert_eq!(ports[1].get("name"), Some(&json!("https")));
}

#[tokio::test]
async fn test_mirror_endpoints_to_endpointslice_updates_existing_mirror() {
    let db = crate::datastore::test_support::in_memory().await;

    let ns = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "test"}});
    db.create_resource("v1", "Namespace", None, "test", ns)
        .await
        .unwrap();

    let endpoints_v1 = json!({
        "apiVersion": "v1",
        "kind": "Endpoints",
        "metadata": {
            "name": "my-service",
            "namespace": "test"
        },
        "subsets": [{
            "addresses": [{"ip": "10.1.2.3"}],
            "ports": [{"port": 80, "protocol": "TCP"}]
        }]
    });
    let created = db
        .create_resource("v1", "Endpoints", Some("test"), "my-service", endpoints_v1)
        .await
        .unwrap();
    let endpoints_v1 = crate::api::inject_resource_version(created.data, created.resource_version);
    mirror_endpoints_to_endpointslice(&db, &endpoints_v1)
        .await
        .unwrap();

    let endpoints_v2 = json!({
        "apiVersion": "v1",
        "kind": "Endpoints",
        "metadata": {
            "name": "my-service",
            "namespace": "test"
        },
        "subsets": [{
            "addresses": [{"ip": "10.2.3.4"}],
            "ports": [{"port": 80, "protocol": "TCP"}]
        }]
    });
    let updated = db
        .update_resource(
            "v1",
            "Endpoints",
            Some("test"),
            "my-service",
            endpoints_v2,
            created.resource_version,
        )
        .await
        .unwrap();
    let endpoints_v2 = crate::api::inject_resource_version(updated.data, updated.resource_version);
    mirror_endpoints_to_endpointslice(&db, &endpoints_v2)
        .await
        .unwrap();

    let mirror = db
        .get_resource(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some("test"),
            "my-service-mirror",
        )
        .await
        .unwrap()
        .expect("mirrored EndpointSlice should exist");

    assert_eq!(
        mirror.data.pointer("/endpoints/0/addresses/0"),
        Some(&json!("10.2.3.4")),
        "mirror update must reflect latest Endpoints address"
    );
}

#[tokio::test]
async fn test_mirror_endpoints_skips_if_skip_mirror_label_set() {
    let db = crate::datastore::test_support::in_memory().await;

    // Create namespace
    let ns = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "test"}});
    db.create_resource("v1", "Namespace", None, "test", ns)
        .await
        .unwrap();

    // Create Endpoints with skip-mirror label
    let endpoints = json!({
        "apiVersion": "v1",
        "kind": "Endpoints",
        "metadata": {
            "name": "my-service",
            "namespace": "test",
            "labels": {
                "endpointslice.kubernetes.io/skip-mirror": "true"
            }
        },
        "subsets": [{
            "addresses": [{"ip": "10.42.0.5"}],
            "ports": [{"port": 80, "protocol": "TCP"}]
        }]
    });

    // Call mirroring function
    mirror_endpoints_to_endpointslice(&db, &endpoints)
        .await
        .unwrap();

    // Verify EndpointSlice was NOT created
    let endpointslice = db
        .get_resource(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some("test"),
            "my-service-mirror",
        )
        .await
        .unwrap();

    assert!(endpointslice.is_none());
}
