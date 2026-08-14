use serde_json::json;

#[tokio::test]
async fn test_reconcile_endpointslice_includes_hostname_for_statefulset_pods() {
    let state = crate::bootstrap::native_api_composition::support::build_test_app_state().await;
    let db = &state;

    let ns = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "default"}});
    db.seed_endpoint_namespace("default", ns).await.unwrap();

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
    db.seed_endpoint_pod("default", "web-0", pod).await.unwrap();

    let selector = json!({"app": "web"});
    let ports = json!([{"port": 80, "protocol": "TCP"}]);

    state
        .reconcile_endpointslice(
            "web-headless",
            "test-service-uid",
            "default",
            Some(&selector),
            Some(&ports),
        )
        .await
        .unwrap();

    let slice = db
        .observe_endpoint_slice("default", "web-headless-klights")
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

#[tokio::test]
async fn test_endpointslice_managed_by_label() {
    let state = crate::bootstrap::native_api_composition::support::build_test_app_state().await;
    let db = &state;

    // Create namespace
    let ns = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "test"}});
    db.seed_endpoint_namespace("test", ns).await.unwrap();

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
    db.seed_endpoint_service("test", "test-svc", svc.clone())
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
    db.seed_endpoint_pod("test", "test-pod", pod).await.unwrap();

    // Reconcile to create EndpointSlice
    let selector = svc.pointer("/spec/selector");
    let ports = svc.pointer("/spec/ports");
    let service_uid = svc
        .pointer("/metadata/uid")
        .and_then(|u| u.as_str())
        .unwrap_or("");
    state
        .reconcile_endpointslice("test-svc", service_uid, "test", selector, ports)
        .await
        .unwrap();

    // Get the created EndpointSlice
    let slices = state.observe_endpoint_slices("test", None).await.unwrap();

    assert_eq!(slices.len(), 1, "Should have one EndpointSlice");
    let slice = &slices[0];

    // Verify managed-by label matches K8s convention
    assert_eq!(
        slice.data["metadata"]["labels"]["endpointslice.kubernetes.io/managed-by"],
        "endpointslice-controller.k8s.io",
        "EndpointSlice managed-by label must match K8s convention"
    );
}

#[tokio::test]
async fn test_reconcile_endpointslice_named_target_port_resolves_to_container_port() {
    let state = crate::bootstrap::native_api_composition::support::build_test_app_state().await;
    let db = &state;

    let ns = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "test"}});
    db.seed_endpoint_namespace("test", ns).await.unwrap();

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
    db.seed_endpoint_pod("test", "nginx-1", pod).await.unwrap();

    // Service port 80 -> targetPort "http" (named port)
    let selector = json!({"app": "nginx"});
    let ports = json!([{"port": 80, "targetPort": "http", "protocol": "TCP", "name": "http"}]);

    state
        .reconcile_endpointslice(
            "nginx-service",
            "test-service-uid",
            "test",
            Some(&selector),
            Some(&ports),
        )
        .await
        .unwrap();

    let slice = db
        .observe_endpoint_slice("test", "nginx-service-klights")
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
    let state = crate::bootstrap::native_api_composition::support::build_test_app_state().await;
    let db = &state;

    let ns = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "test"}});
    db.seed_endpoint_namespace("test", ns).await.unwrap();

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
        db.seed_endpoint_pod("test", name, pod).await.unwrap();
    }

    let selector = json!({"shared": "on"});
    let ports = json!([{
        "name": "http",
        "port": 80,
        "targetPort": "example-name",
        "protocol": "TCP"
    }]);

    state
        .reconcile_endpointslice(
            "example-named-port",
            "svc-uid",
            "test",
            Some(&selector),
            Some(&ports),
        )
        .await
        .unwrap();

    let slices = state
        .observe_endpoint_slices(
            "test",
            Some("kubernetes.io/service-name=example-named-port"),
        )
        .await
        .unwrap();

    assert_eq!(
        slices.len(),
        2,
        "named targetPorts resolving to different pod ports require separate EndpointSlices"
    );
    let mut seen_ports: Vec<i64> = slices
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
    let state = crate::bootstrap::native_api_composition::support::build_test_app_state().await;
    let db = &state;

    let ns = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "test"}});
    db.seed_endpoint_namespace("test", ns).await.unwrap();

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
        db.seed_endpoint_pod("test", name, pod).await.unwrap();
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

    state
        .reconcile_endpointslice(
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
    db.remove_endpoint_slice("test", &desired_slice_name)
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
    db.seed_endpoint_slice("test", &desired_slice_name, stale_slice)
        .await
        .unwrap();

    state
        .reconcile_endpointslice(
            service_name,
            "svc-uid",
            "test",
            Some(&selector),
            Some(&ports),
        )
        .await
        .unwrap();

    let refreshed = db
        .observe_endpoint_slice("test", &desired_slice_name)
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
    let state = crate::bootstrap::native_api_composition::support::build_test_app_state().await;
    let db = &state;

    let ns = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "test"}});
    db.seed_endpoint_namespace("test", ns).await.unwrap();

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
    db.seed_endpoint_pod("test", "pod-1", pod).await.unwrap();

    let selector = json!({"app": "demo"});
    let ports = json!([
        {"name": "portname1", "port": 80, "targetPort": "100", "protocol": "TCP"},
        {"name": "portname2", "port": 81, "targetPort": "portname2", "protocol": "TCP"}
    ]);

    state
        .reconcile_endpointslice(
            "multi-endpoint-test",
            "svc-uid-3",
            "test",
            Some(&selector),
            Some(&ports),
        )
        .await
        .unwrap();

    let slice = db
        .observe_endpoint_slice("test", "multi-endpoint-test-klights")
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
async fn test_endpointslice_ports_match_service_targetport() {
    let state = crate::bootstrap::native_api_composition::support::build_test_app_state().await;
    let db = &state;

    let ns = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "test"}});
    db.seed_endpoint_namespace("test", ns).await.unwrap();

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "web-1", "namespace": "test", "labels": {"app": "web"}},
        "spec": {"containers": [{"name": "web", "ports": [{"containerPort": 8080}]}]},
        "status": {"podIP": "10.43.0.5", "conditions": [{"type": "Ready", "status": "True"}]}
    });
    db.seed_endpoint_pod("test", "web-1", pod).await.unwrap();

    // Service port 80 maps to targetPort 8080
    let selector = json!({"app": "web"});
    let ports = json!([{"port": 80, "targetPort": 8080, "protocol": "TCP"}]);

    state
        .reconcile_endpointslice(
            "web-svc",
            "svc-uid-1",
            "test",
            Some(&selector),
            Some(&ports),
        )
        .await
        .unwrap();

    let slice = db
        .observe_endpoint_slice("test", "web-svc-klights")
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
    let state = crate::bootstrap::native_api_composition::support::build_test_app_state().await;
    let db = &state;

    let ns = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "test"}});
    db.seed_endpoint_namespace("test", ns).await.unwrap();

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "web-1", "namespace": "test", "labels": {"app": "web"}},
        "status": {"podIP": "10.43.0.5", "conditions": [{"type": "Ready", "status": "True"}]}
    });
    db.seed_endpoint_pod("test", "web-1", pod).await.unwrap();

    // Service port 9000, no targetPort
    let selector = json!({"app": "web"});
    let ports = json!([{"port": 9000, "protocol": "TCP"}]);

    state
        .reconcile_endpointslice(
            "web-svc",
            "svc-uid-2",
            "test",
            Some(&selector),
            Some(&ports),
        )
        .await
        .unwrap();

    let slice = db
        .observe_endpoint_slice("test", "web-svc-klights")
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
    let state = crate::bootstrap::native_api_composition::support::build_test_app_state().await;
    let db = &state;

    let ns = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "test"}});
    db.seed_endpoint_namespace("test", ns).await.unwrap();

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "web-1", "namespace": "test", "labels": {"app": "web"}},
        "spec": {"containers": [{"name": "web", "ports": [{"name": "http", "containerPort": 8080}]}]},
        "status": {"podIP": "10.43.0.5", "conditions": [{"type": "Ready", "status": "True"}]}
    });
    db.seed_endpoint_pod("test", "web-1", pod).await.unwrap();

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
        state
            .reconcile_endpointslice(
                &svc_name,
                &format!("uid-{}", test_case),
                "test",
                Some(&json!({"app": "web"})),
                Some(&ports_val),
            )
            .await
            .unwrap();

        let slice = db
            .observe_endpoint_slice("test", &slice_name.clone())
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
#[tokio::test]
async fn test_reconcile_endpointslice_sets_empty_name_for_unnamed_service_port() {
    let state = crate::bootstrap::native_api_composition::support::build_test_app_state().await;
    let db = &state;

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "agnhost-primary",
            "namespace": "test",
            "labels": {"app": "agnhost", "role": "primary"}
        },
        "status": {
            "podIP": "10.42.0.10",
            "conditions": [{"type": "Ready", "status": "True"}]
        }
    });
    db.seed_endpoint_pod("test", "agnhost-primary", pod)
        .await
        .unwrap();

    let selector = json!({"app": "agnhost", "role": "primary"});
    let ports = json!([{"port": 6379, "targetPort": 6379, "protocol": "TCP"}]);
    state
        .reconcile_endpointslice(
            "agnhost-primary",
            "svc-uid",
            "test",
            Some(&selector),
            Some(&ports),
        )
        .await
        .unwrap();

    let endpointslice = db
        .observe_endpoint_slice("test", "agnhost-primary-klights")
        .await
        .unwrap()
        .expect("EndpointSlice must be created");

    assert_eq!(
        endpointslice.data["ports"][0]["name"], "",
        "unnamed Service ports must become EndpointSlice port name=\"\" so kubectl describe does not see a nil name"
    );
}

#[tokio::test]
async fn test_reconcile_endpointslice_creates_slice_for_matching_pods() {
    let state = crate::bootstrap::native_api_composition::support::build_test_app_state().await;
    let db = &state;

    // Create namespace
    let ns = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "test"}});
    db.seed_endpoint_namespace("test", ns).await.unwrap();

    // Create pods with matching labels
    let pod1 = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "nginx-1",
            "namespace": "test",
            "uid": "pod-uid-nginx-1",
            "labels": {"app": "nginx"}
        },
        "status": {
            "podIP": "10.43.0.2",
            "conditions": [{"type": "Ready", "status": "True"}]
        }
    });

    db.seed_endpoint_pod("test", "nginx-1", pod1).await.unwrap();

    let selector = json!({"app": "nginx"});
    let ports = json!([{"port": 80, "targetPort": 8080, "name": "http"}]);

    state
        .reconcile_endpointslice(
            "nginx-service",
            "test-service-uid",
            "test",
            Some(&selector),
            Some(&ports),
        )
        .await
        .unwrap();

    let slice = db
        .observe_endpoint_slice("test", "nginx-service-klights")
        .await
        .unwrap()
        .unwrap();

    // Verify EndpointSlice structure
    assert_eq!(slice.data["addressType"], "IPv4");
    assert_eq!(
        slice.data["metadata"]["labels"]["kubernetes.io/service-name"],
        "nginx-service"
    );
    assert_eq!(
        slice.data["metadata"]["labels"]["endpointslice.kubernetes.io/managed-by"],
        "endpointslice-controller.k8s.io"
    );

    // Verify ownerReferences for cascade delete
    let owner_refs = slice.data["metadata"]["ownerReferences"]
        .as_array()
        .unwrap();
    assert_eq!(owner_refs.len(), 1, "Should have one ownerReference");
    assert_eq!(owner_refs[0]["apiVersion"], "v1");
    assert_eq!(owner_refs[0]["kind"], "Service");
    assert_eq!(owner_refs[0]["name"], "nginx-service");
    assert_eq!(owner_refs[0]["uid"], "test-service-uid");
    assert_eq!(owner_refs[0]["controller"], false);
    assert_eq!(owner_refs[0]["blockOwnerDeletion"], true);

    // Verify endpoints
    let endpoints = slice.data["endpoints"].as_array().unwrap();
    assert_eq!(endpoints.len(), 1, "Should have one endpoint");
    assert_eq!(endpoints[0]["addresses"][0], "10.43.0.2");
    assert_eq!(endpoints[0]["conditions"]["ready"], true);
    assert_eq!(endpoints[0]["conditions"]["serving"], true);
    assert_eq!(endpoints[0]["targetRef"]["name"], "nginx-1");
    assert_eq!(endpoints[0]["targetRef"]["uid"], "pod-uid-nginx-1");

    // Verify ports
    let slice_ports = slice.data["ports"].as_array().unwrap();
    assert_eq!(slice_ports.len(), 1);
    assert_eq!(slice_ports[0]["port"], 8080);
    assert_eq!(slice_ports[0]["protocol"], "TCP");
    assert_eq!(slice_ports[0]["name"], "http");
}

#[tokio::test]
async fn test_reconcile_endpointslice_empty_matchlabels_does_not_create_slice() {
    let state = crate::bootstrap::native_api_composition::support::build_test_app_state().await;
    let db = &state;

    let selector = json!({"matchLabels": {}});
    let ports = json!([{"port": 80, "name": "http"}]);
    state
        .reconcile_endpointslice(
            "selectorless-service",
            "selectorless-service-uid",
            "test",
            Some(&selector),
            Some(&ports),
        )
        .await
        .unwrap();

    let slice = db
        .observe_endpoint_slice("test", "selectorless-service-klights")
        .await
        .unwrap();

    assert!(
        slice.is_none(),
        "Controller should NOT create EndpointSlice for service with empty matchLabels"
    );
}

#[tokio::test]
async fn test_reconcile_endpointslice_marks_not_ready_pods() {
    let state = crate::bootstrap::native_api_composition::support::build_test_app_state().await;
    let db = &state;

    // Create namespace
    let ns = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "test"}});
    db.seed_endpoint_namespace("test", ns).await.unwrap();

    // Create pod with Ready=False
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "nginx-1",
            "namespace": "test",
            "labels": {"app": "nginx"}
        },
        "status": {
            "podIP": "10.43.0.2",
            "conditions": [{"type": "Ready", "status": "False"}]
        }
    });

    db.seed_endpoint_pod("test", "nginx-1", pod).await.unwrap();

    let selector = json!({"app": "nginx"});
    let ports = json!([{"port": 80}]);

    state
        .reconcile_endpointslice(
            "nginx-service",
            "test-service-uid",
            "test",
            Some(&selector),
            Some(&ports),
        )
        .await
        .unwrap();

    let slice = db
        .observe_endpoint_slice("test", "nginx-service-klights")
        .await
        .unwrap()
        .unwrap();

    let endpoints = slice.data["endpoints"].as_array().unwrap();
    assert_eq!(endpoints.len(), 1);
    assert_eq!(
        endpoints[0]["conditions"]["ready"], false,
        "Not-ready pod should have ready=false"
    );
    assert_eq!(
        endpoints[0]["conditions"]["serving"], false,
        "Not-ready pod should have serving=false"
    );
}
