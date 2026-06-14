use super::*;
#[test]
fn test_resolve_target_port_zero_falls_back_to_service_port() {
    let service_port = json!({"port": 100, "targetPort": 0, "protocol": "TCP"});
    let result = resolve_target_port(&service_port, &[]);
    assert_eq!(
        result,
        Some(100),
        "targetPort=0 must fall back to service port=100"
    );
}

#[test]
fn test_resolve_target_port_absent_uses_service_port() {
    let service_port = json!({"port": 200, "protocol": "TCP"});
    let result = resolve_target_port(&service_port, &[]);
    assert_eq!(
        result,
        Some(200),
        "absent targetPort must use service port=200"
    );
}

#[test]
fn test_resolve_target_port_nonzero_integer_used_directly() {
    let service_port = json!({"port": 80, "targetPort": 8080, "protocol": "TCP"});
    let result = resolve_target_port(&service_port, &[]);
    assert_eq!(result, Some(8080), "non-zero targetPort must be used as-is");
}

#[test]
fn test_resolve_target_port_intorstring_object_string_type_uses_str_val() {
    let service_port = json!({
        "port": 80,
        "targetPort": {
            "type": 1,
            "intVal": 0,
            "strVal": "100"
        },
        "protocol": "TCP"
    });
    let result = resolve_target_port(&service_port, &[]);
    assert_eq!(result, Some(100));
}

#[test]
fn test_resolve_target_port_intorstring_object_named_port_resolves_container_port() {
    let pod = json!({
        "spec": {
            "containers": [{
                "ports": [{"name": "portname1", "containerPort": 100, "protocol": "TCP"}]
            }]
        }
    });
    let service_port = json!({
        "port": 80,
        "targetPort": {
            "type": 1,
            "intVal": 0,
            "strVal": "portname1"
        },
        "protocol": "TCP"
    });
    let result = resolve_target_port(&service_port, &[&pod]);
    assert_eq!(result, Some(100));
}

#[tokio::test]
async fn test_reconcile_endpointslice_sets_empty_name_for_unnamed_service_port() {
    let db = crate::datastore::test_support::in_memory().await;

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
    db.create_resource("v1", "Pod", Some("test"), "agnhost-primary", pod)
        .await
        .unwrap();

    let selector = json!({"app": "agnhost", "role": "primary"});
    let ports = json!([{"port": 6379, "targetPort": 6379, "protocol": "TCP"}]);
    reconcile_endpointslice(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        "agnhost-primary",
        "svc-uid",
        "test",
        Some(&selector),
        Some(&ports),
    )
    .await
    .unwrap();

    let endpointslice = db
        .get_resource(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some("test"),
            "agnhost-primary-klights",
        )
        .await
        .unwrap()
        .expect("EndpointSlice must be created");

    assert_eq!(
        endpointslice.data["ports"][0]["name"], "",
        "unnamed Service ports must become EndpointSlice port name=\"\" so kubectl describe does not see a nil name"
    );
}

#[tokio::test]
async fn test_mirror_endpoints_to_endpointslice_sets_empty_name_for_unnamed_port() {
    let db = crate::datastore::test_support::in_memory().await;
    let ns = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "test"}});
    db.create_resource("v1", "Namespace", None, "test", ns)
        .await
        .unwrap();
    let endpoints = json!({
        "apiVersion": "v1",
        "kind": "Endpoints",
        "metadata": {
            "name": "manual-service",
            "namespace": "test",
            "uid": "ep-uid"
        },
        "subsets": [{
            "addresses": [{"ip": "10.42.0.20"}],
            "ports": [{"port": 8080, "protocol": "TCP"}]
        }]
    });
    let created = db
        .create_resource("v1", "Endpoints", Some("test"), "manual-service", endpoints)
        .await
        .unwrap();
    let endpoints = crate::api::inject_resource_version(created.data, created.resource_version);

    mirror_endpoints_to_endpointslice(&db, &endpoints)
        .await
        .unwrap();

    let endpointslice = db
        .get_resource(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some("test"),
            "manual-service-mirror",
        )
        .await
        .unwrap()
        .expect("mirrored EndpointSlice must be created");

    assert_eq!(
        endpointslice.data["ports"][0]["name"], "",
        "mirrored unnamed Endpoints ports must become EndpointSlice port name=\"\""
    );
}

#[tokio::test]
async fn test_reconcile_endpoints_creates_endpoints_for_matching_pods() {
    let db = crate::datastore::test_support::in_memory().await;

    // Create namespace
    let ns = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "test"}});
    db.create_resource("v1", "Namespace", None, "test", ns)
        .await
        .unwrap();

    // Create 2 pods with matching labels and podIP
    let pod1 = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "nginx-1",
            "namespace": "test",
            "uid": "pod-uid-nginx-1",
            "labels": {"app": "nginx"}
        },
        "status": {"podIP": "10.42.0.5", "conditions": [{"type": "Ready", "status": "True"}]}
    });
    db.create_resource("v1", "Pod", Some("test"), "nginx-1", pod1)
        .await
        .unwrap();

    let pod2 = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "nginx-2",
            "namespace": "test",
            "uid": "pod-uid-nginx-2",
            "labels": {"app": "nginx"}
        },
        "status": {"podIP": "10.42.0.6", "conditions": [{"type": "Ready", "status": "True"}]}
    });
    db.create_resource("v1", "Pod", Some("test"), "nginx-2", pod2)
        .await
        .unwrap();

    // Create Service selector and ports
    let selector = json!({"app": "nginx"});
    let ports = json!([{"port": 80, "targetPort": 8080, "protocol": "TCP"}]);

    // Reconcile endpoints
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

    // Verify Endpoints created with correct subsets
    let endpoints = db
        .get_resource("v1", "Endpoints", Some("test"), "nginx-service")
        .await
        .unwrap();
    assert!(endpoints.is_some(), "Endpoints should be created");

    let ep_data = endpoints.unwrap().data;
    assert_eq!(ep_data["metadata"]["name"], "nginx-service");
    assert_eq!(ep_data["metadata"]["namespace"], "test");

    let subsets = ep_data["subsets"].as_array().unwrap();
    assert_eq!(subsets.len(), 1, "Should have 1 subset");

    let addresses = subsets[0]["addresses"].as_array().unwrap();
    assert_eq!(addresses.len(), 2, "Should have 2 addresses");

    // Check addresses contain both pod IPs
    let ips: Vec<&str> = addresses
        .iter()
        .map(|a| a["ip"].as_str().unwrap())
        .collect();
    assert!(ips.contains(&"10.42.0.5"));
    assert!(ips.contains(&"10.42.0.6"));

    // Check targetRefs
    let target_names: Vec<&str> = addresses
        .iter()
        .map(|a| a["targetRef"]["name"].as_str().unwrap())
        .collect();
    assert!(target_names.contains(&"nginx-1"));
    assert!(target_names.contains(&"nginx-2"));
    let target_uids: Vec<&str> = addresses
        .iter()
        .map(|a| a["targetRef"]["uid"].as_str().unwrap_or(""))
        .collect();
    assert!(target_uids.contains(&"pod-uid-nginx-1"));
    assert!(target_uids.contains(&"pod-uid-nginx-2"));

    // Check ports (should use targetPort)
    let ports = subsets[0]["ports"].as_array().unwrap();
    assert_eq!(ports.len(), 1);
    assert_eq!(ports[0]["port"], 8080);
    assert_eq!(ports[0]["protocol"], "TCP");
}

#[tokio::test]
async fn test_reconcile_endpoints_empty_when_no_matching_pods() {
    let db = crate::datastore::test_support::in_memory().await;

    // Create namespace
    let ns = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "test"}});
    db.create_resource("v1", "Namespace", None, "test", ns)
        .await
        .unwrap();

    // Create pod with NON-matching labels
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "redis-1",
            "namespace": "test",
            "labels": {"app": "redis"}
        },
        "status": {"podIP": "10.42.0.5"}
    });
    db.create_resource("v1", "Pod", Some("test"), "redis-1", pod)
        .await
        .unwrap();

    // Service selector looking for nginx (won't match)
    let selector = json!({"app": "nginx"});
    let ports = json!([{"port": 80}]);

    // Reconcile endpoints
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

    // Verify Endpoints created but with empty subsets
    let endpoints = db
        .get_resource("v1", "Endpoints", Some("test"), "nginx-service")
        .await
        .unwrap();
    assert!(endpoints.is_some(), "Endpoints should be created");

    let ep_data = endpoints.unwrap().data;
    let subsets = ep_data["subsets"].as_array().unwrap();
    assert_eq!(
        subsets.len(),
        0,
        "Subsets should be empty when no pods match"
    );
}

#[tokio::test]
async fn test_reconcile_endpoints_updates_existing_endpoints() {
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

    // Verify initial state
    let endpoints = db
        .get_resource("v1", "Endpoints", Some("test"), "nginx-service")
        .await
        .unwrap()
        .unwrap();
    let subsets = endpoints.data["subsets"].as_array().unwrap();
    assert_eq!(subsets[0]["addresses"].as_array().unwrap().len(), 1);

    // Add second pod
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

    // Second reconcile updates endpoints
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

    // Verify updated state has 2 addresses
    let updated_endpoints = db
        .get_resource("v1", "Endpoints", Some("test"), "nginx-service")
        .await
        .unwrap()
        .unwrap();
    let updated_subsets = updated_endpoints.data["subsets"].as_array().unwrap();
    assert_eq!(
        updated_subsets[0]["addresses"].as_array().unwrap().len(),
        2,
        "Should have 2 addresses after update"
    );

    let ips: Vec<&str> = updated_subsets[0]["addresses"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["ip"].as_str().unwrap())
        .collect();
    assert!(ips.contains(&"10.42.0.5"));
    assert!(ips.contains(&"10.42.0.6"));
}

#[tokio::test]
async fn test_reconcile_endpoints_uses_target_port() {
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

    // Service port 80 maps to container targetPort 8080
    let ports = json!([{
        "port": 80,
        "targetPort": 8080,
        "protocol": "TCP"
    }]);

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

    let endpoints = db
        .get_resource("v1", "Endpoints", Some("test"), "nginx-service")
        .await
        .unwrap()
        .unwrap();
    let ep_ports = endpoints.data["subsets"][0]["ports"].as_array().unwrap();

    // Endpoints port should use targetPort (8080), not service port (80)
    assert_eq!(
        ep_ports[0]["port"], 8080,
        "Endpoints port should match targetPort"
    );
    assert_eq!(ep_ports[0]["protocol"], "TCP");
}

#[tokio::test]
async fn test_reconcile_endpoints_excludes_pods_with_zero_ip() {
    let db = crate::datastore::test_support::in_memory().await;

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "pending-pod",
            "namespace": "test",
            "labels": {"app": "nginx"}
        },
        "status": {"podIP": "0.0.0.0"}
    });
    db.create_resource("v1", "Pod", Some("test"), "pending-pod", pod)
        .await
        .unwrap();

    let selector = json!({"app": "nginx"});
    let ports = json!([{"port": 80}]);

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
    assert_eq!(
        subsets.len(),
        0,
        "Pod with 0.0.0.0 IP should not appear in subsets"
    );
}

#[tokio::test]
async fn test_reconcile_endpoints_excludes_pods_with_empty_ip() {
    let db = crate::datastore::test_support::in_memory().await;

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "pending-pod",
            "namespace": "test",
            "labels": {"app": "nginx"}
        },
        "status": {"podIP": ""}
    });
    db.create_resource("v1", "Pod", Some("test"), "pending-pod", pod)
        .await
        .unwrap();

    let selector = json!({"app": "nginx"});
    let ports = json!([{"port": 80}]);

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
    assert_eq!(
        subsets.len(),
        0,
        "Pod with empty IP should not appear in subsets"
    );
}

#[tokio::test]
async fn test_reconcile_endpoints_no_selector_creates_empty_subsets() {
    let db = crate::datastore::test_support::in_memory().await;

    // Create a pod (should not be matched when no selector)
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "nginx-1",
            "namespace": "test",
            "labels": {"app": "nginx"}
        },
        "status": {"podIP": "10.42.0.5"}
    });
    db.create_resource("v1", "Pod", Some("test"), "nginx-1", pod)
        .await
        .unwrap();

    let ports = json!([{"port": 80}]);

    // No selector — headless-style service, no automatic endpoint population
    // K8s behavior: controller does NOT create Endpoints when service has no selector
    reconcile_endpoints(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        "headless-svc",
        "test",
        None,
        Some(&ports),
        false,
    )
    .await
    .unwrap();

    // Verify NO Endpoints were created (user must create manually for selectorless services)
    let ep = db
        .get_resource("v1", "Endpoints", Some("test"), "headless-svc")
        .await
        .unwrap();

    assert!(
        ep.is_none(),
        "Controller should NOT create Endpoints for service without selector"
    );
}

#[tokio::test]
async fn test_reconcile_endpoints_empty_selector_does_not_create_endpoints() {
    let db = crate::datastore::test_support::in_memory().await;
    let selector = json!({});
    let ports = json!([{"port": 80}]);

    reconcile_endpoints(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        "selectorless-svc",
        "test",
        Some(&selector),
        Some(&ports),
        false,
    )
    .await
    .unwrap();

    let ep = db
        .get_resource("v1", "Endpoints", Some("test"), "selectorless-svc")
        .await
        .unwrap();

    assert!(
        ep.is_none(),
        "Controller should NOT create Endpoints for service with empty selector map"
    );
}

#[tokio::test]
async fn test_reconcile_endpoints_falls_back_to_port_when_no_target_port() {
    let db = crate::datastore::test_support::in_memory().await;

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
    // No targetPort specified — should fall back to port
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

    let ep_ports = ep.data["subsets"][0]["ports"].as_array().unwrap();
    assert_eq!(
        ep_ports[0]["port"], 80,
        "Should fall back to service port when targetPort is absent"
    );
}

#[tokio::test]
async fn test_reconcile_endpointslice_creates_slice_for_matching_pods() {
    let db = crate::datastore::test_support::in_memory().await;

    // Create namespace
    let ns = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "test"}});
    db.create_resource("v1", "Namespace", None, "test", ns)
        .await
        .unwrap();

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

    db.create_resource("v1", "Pod", Some("test"), "nginx-1", pod1)
        .await
        .unwrap();

    let selector = json!({"app": "nginx"});
    let ports = json!([{"port": 80, "targetPort": 8080, "name": "http"}]);

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
    let db = crate::datastore::test_support::in_memory().await;

    let selector = json!({"matchLabels": {}});
    let ports = json!([{"port": 80, "name": "http"}]);
    reconcile_endpointslice(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        "selectorless-service",
        "selectorless-service-uid",
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
            "selectorless-service-klights",
        )
        .await
        .unwrap();

    assert!(
        slice.is_none(),
        "Controller should NOT create EndpointSlice for service with empty matchLabels"
    );
}

#[tokio::test]
async fn test_reconcile_endpointslice_marks_not_ready_pods() {
    let db = crate::datastore::test_support::in_memory().await;

    // Create namespace
    let ns = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "test"}});
    db.create_resource("v1", "Namespace", None, "test", ns)
        .await
        .unwrap();

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

    db.create_resource("v1", "Pod", Some("test"), "nginx-1", pod)
        .await
        .unwrap();

    let selector = json!({"app": "nginx"});
    let ports = json!([{"port": 80}]);

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
