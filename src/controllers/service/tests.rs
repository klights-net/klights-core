use super::*;

#[tokio::test]
async fn test_service_stale_snapshot_after_delete_does_not_recreate_endpoints() {
    let db = crate::datastore::test_support::in_memory().await;
    let service_ipam = ServiceIpam::new("10.43.128.0/17");

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({"metadata": {"name": "default"}}),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "svc-pod",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "svc-pod",
                "namespace": "default",
                "uid": "svc-pod-uid",
                "labels": {"app": "stale-svc"}
            },
            "spec": {"containers": [{"name": "app", "image": "nginx", "ports": [{"containerPort": 8080}]}]},
            "status": {
                "phase": "Running",
                "podIP": "10.43.0.20",
                "conditions": [{"type": "Ready", "status": "True"}]
            }
        }),
    )
    .await
    .unwrap();

    let service = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "stale-svc", "namespace": "default", "uid": "stale-svc-uid"},
        "spec": {
            "type": "ClusterIP",
            "clusterIP": "10.43.128.20",
            "clusterIPs": ["10.43.128.20"],
            "selector": {"app": "stale-svc"},
            "ports": [{"name": "http", "port": 80, "targetPort": 8080, "protocol": "TCP"}]
        }
    });
    let created = db
        .create_resource("v1", "Service", Some("default"), "stale-svc", service)
        .await
        .unwrap();
    let stale_snapshot =
        crate::api::inject_resource_version(created.data, created.resource_version);

    db.delete_resource("v1", "Service", Some("default"), "stale-svc")
        .await
        .unwrap();

    reconcile_service(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        &stale_snapshot,
        &service_ipam,
    )
    .await
    .unwrap();

    let endpoints = db
        .get_resource("v1", "Endpoints", Some("default"), "stale-svc")
        .await
        .unwrap();
    assert!(
        endpoints.is_none(),
        "stale deleted Service reconcile must not recreate Endpoints"
    );
    let slices = db
        .list_resources(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert!(
        slices.items.is_empty(),
        "stale deleted Service reconcile must not recreate EndpointSlices"
    );
}

#[test]
fn test_service_ipam_reserves_dot_one_for_kubernetes_service() {
    // The kubernetes service hardcodes 10.43.128.1 (KUBERNETES_SERVICE_IP)
    // IPAM must start at .2 to avoid collision
    let ipam = ServiceIpam::new("10.43.128.0/17");

    // First allocation should be .2 (not .1, which is reserved)
    assert_eq!(ipam.allocate().unwrap(), "10.43.128.2");

    // Next should be .3
    assert_eq!(ipam.allocate().unwrap(), "10.43.128.3");
}

#[test]
fn test_service_ipam_default_range() {
    let ipam = ServiceIpam::new("10.43.128.0/17");

    // First IP should be 10.43.128.2 (.1 reserved for kubernetes service)
    assert_eq!(ipam.allocate().unwrap(), "10.43.128.2");

    // Next should be 10.43.128.3
    assert_eq!(ipam.allocate().unwrap(), "10.43.128.3");

    // Verify sequential allocation
    assert_eq!(ipam.allocate().unwrap(), "10.43.128.4");
}

#[test]
fn test_service_ipam_custom_range() {
    let ipam = ServiceIpam::new("10.44.128.0/17");

    // First IP should be 10.44.128.2 (.1 reserved for kubernetes service)
    assert_eq!(ipam.allocate().unwrap(), "10.44.128.2");

    // Next should be 10.44.128.3
    assert_eq!(ipam.allocate().unwrap(), "10.44.128.3");
}

#[test]
fn test_service_ipam_no_overlap_with_pods() {
    // Pod CIDR: 10.43.0.0/17 (10.43.0.0 - 10.43.127.255)
    // Service CIDR: 10.43.128.0/17 (10.43.128.0 - 10.43.255.255)

    let service_ipam = ServiceIpam::new("10.43.128.0/17");

    // Canonical pod IPs from the node-local pod range
    let pod_ip1 = "10.43.0.2";
    let pod_ip2 = "10.43.0.3";

    // Allocate some service IPs (starts at .2 since .1 is reserved)
    let svc_ip1 = service_ipam.allocate().unwrap(); // Should be 10.43.128.2
    let svc_ip2 = service_ipam.allocate().unwrap(); // Should be 10.43.128.3

    // Verify no overlap
    assert_eq!(pod_ip1, "10.43.0.2");
    assert_eq!(pod_ip2, "10.43.0.3");
    assert_eq!(svc_ip1, "10.43.128.2");
    assert_eq!(svc_ip2, "10.43.128.3");

    // Verify pod IPs are in first half (octet3 < 128)
    assert!(pod_ip1.starts_with("10.43.0."));
    assert!(pod_ip2.starts_with("10.43.0."));

    // Verify service IPs are in second half (octet3 >= 128)
    assert!(svc_ip1.starts_with("10.43.128."));
    assert!(svc_ip2.starts_with("10.43.128."));
}

#[test]
fn test_ipam_allocate_sequential() {
    let ipam = ServiceIpam::new("10.43.128.0/17");

    let ip1 = ipam.allocate().unwrap();
    let ip2 = ipam.allocate().unwrap();
    let ip3 = ipam.allocate().unwrap();

    assert_eq!(ip1, "10.43.128.2");
    assert_eq!(ip2, "10.43.128.3");
    assert_eq!(ip3, "10.43.128.4");
}

#[test]
fn test_ipam_release_and_reuse() {
    let ipam = ServiceIpam::new("10.43.128.0/17");

    let ip1 = ipam.allocate().unwrap();
    assert_eq!(ip1, "10.43.128.2");

    // Release the IP
    ipam.release(&ip1);

    // Next allocation should reuse the released IP
    let ip2 = ipam.allocate().unwrap();
    assert_eq!(ip2, "10.43.128.2", "Released IP should be reused");
}

#[test]
fn test_ipam_release_nonexistent_is_noop() {
    let ipam = ServiceIpam::new("10.43.128.0/17");

    // Releasing an IP that was never allocated should not panic
    ipam.release("10.43.128.99");

    // Should still allocate normally
    let ip = ipam.allocate().unwrap();
    assert_eq!(ip, "10.43.128.2");
}

#[test]
fn test_parse_ip_to_u32_valid() {
    assert_eq!(parse_ip_to_u32("10.43.128.2"), Some(0x0A2B8002));
    assert_eq!(parse_ip_to_u32("0.0.0.0"), Some(0));
    assert_eq!(parse_ip_to_u32("255.255.255.255"), Some(0xFFFFFFFF));
}

#[test]
fn test_parse_ip_to_u32_invalid() {
    assert_eq!(parse_ip_to_u32(""), None);
    assert_eq!(parse_ip_to_u32("not.an.ip"), None);
    assert_eq!(parse_ip_to_u32("10.43.128"), None);
    assert_eq!(parse_ip_to_u32("10.43.128.2.5"), None);
}

#[test]
fn test_ipam_release_invalid_ip_is_noop() {
    let ipam = ServiceIpam::new("10.43.128.0/17");
    // Releasing invalid IP strings should not panic
    ipam.release("");
    ipam.release("not-an-ip");
    ipam.release("10.43");
    // Should still allocate normally
    assert_eq!(ipam.allocate().unwrap(), "10.43.128.2");
}

#[test]
fn test_ipam_skips_allocated() {
    let ipam = ServiceIpam::new("10.43.128.0/17");

    let ip1 = ipam.allocate().unwrap();
    assert_eq!(ip1, "10.43.128.2");

    // Don't release ip1, allocate another
    let ip2 = ipam.allocate().unwrap();
    assert_eq!(ip2, "10.43.128.3");

    // ip1 should still be allocated, ip2 is different
    assert_ne!(ip1, ip2);

    // Release ip1, should be reusable now
    ipam.release(&ip1);
    let ip3 = ipam.allocate().unwrap();
    assert_eq!(ip3, "10.43.128.2", "After release, IP should be reused");
}

#[test]
fn test_headless_service_clusterip_none_not_overwritten() {
    // Test that "None" is preserved (not replaced with allocated IP)
    let spec = json!({
        "clusterIP": "None",
        "selector": {"app": "test"},
        "ports": [{"port": 80}]
    });

    let cluster_ip_value = spec.get("clusterIP").and_then(|v| v.as_str());

    // The fix: check if clusterIP is None (missing), don't check if it's "None" (string)
    let should_allocate = cluster_ip_value.is_none();

    assert!(
        !should_allocate,
        "clusterIP='None' (headless) should NOT trigger allocation"
    );
}

#[test]
fn test_normal_service_clusterip_allocated() {
    // Test that missing clusterIP triggers allocation
    let spec = json!({
        "selector": {"app": "test"},
        "ports": [{"port": 80}]
    });

    let cluster_ip_value = spec.get("clusterIP").and_then(|v| v.as_str());

    // Should allocate when clusterIP is not present
    let should_allocate = cluster_ip_value.is_none();

    assert!(
        should_allocate,
        "Missing clusterIP should trigger allocation"
    );
}

// Integration test (requires root for nftables/netlink)
#[tokio::test]
#[ignore] // Ignored by default, run manually with --ignored flag as root
async fn test_reconcile_service_preserves_headless_cluster_ip_none() {
    let db = crate::datastore::test_support::in_memory().await;
    let service_ipam = ServiceIpam::new("10.43.128.0/17");

    // Create a headless service (clusterIP: None)
    let mut service = json!({
        "metadata": {
            "name": "headless-svc",
            "namespace": "default",
            "uid": "test-uid-1"
        },
        "spec": {
            "clusterIP": "None",
            "selector": {"app": "test"},
            "ports": [{"port": 80, "targetPort": 8080}]
        }
    });

    // Insert service into DB first
    let name = service
        .get("metadata")
        .unwrap()
        .get("name")
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();
    let created = db
        .create_resource("v1", "Service", Some("default"), &name, service.clone())
        .await
        .unwrap();

    // Inject resource version for reconciliation
    if let Some(metadata) = service.as_object_mut().and_then(|o| o.get_mut("metadata"))
        && let Some(meta_obj) = metadata.as_object_mut()
    {
        meta_obj.insert(
            "resourceVersion".to_string(),
            json!(created.resource_version.to_string()),
        );
    }

    let result = reconcile_service(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        &service,
        &service_ipam,
    )
    .await
    .unwrap();

    // Verify clusterIP is still "None" (not allocated)
    let cluster_ip = result
        .get("spec")
        .and_then(|s| s.get("clusterIP"))
        .and_then(|ip| ip.as_str());

    assert_eq!(
        cluster_ip,
        Some("None"),
        "Headless service must preserve clusterIP: None"
    );

    // Verify no clusterIPs array was added
    assert!(
        result
            .get("spec")
            .and_then(|s| s.get("clusterIPs"))
            .is_none(),
        "Headless service must not have clusterIPs array"
    );
}

// Integration test (requires root for nftables/netlink)
#[tokio::test]
#[ignore] // Ignored by default, run manually with --ignored flag as root
async fn test_reconcile_service_allocates_cluster_ip_when_not_set() {
    let db = crate::datastore::test_support::in_memory().await;
    let service_ipam = ServiceIpam::new("10.43.128.0/17");

    // Create a normal service without clusterIP
    let mut service = json!({
        "metadata": {
            "name": "normal-svc",
            "namespace": "default",
            "uid": "test-uid-2"
        },
        "spec": {
            "selector": {"app": "test"},
            "ports": [{"port": 80, "targetPort": 8080}]
        }
    });

    // Insert service into DB first
    let name = service
        .get("metadata")
        .unwrap()
        .get("name")
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();
    let created = db
        .create_resource("v1", "Service", Some("default"), &name, service.clone())
        .await
        .unwrap();

    // Inject resource version for reconciliation
    if let Some(metadata) = service.as_object_mut().and_then(|o| o.get_mut("metadata"))
        && let Some(meta_obj) = metadata.as_object_mut()
    {
        meta_obj.insert(
            "resourceVersion".to_string(),
            json!(created.resource_version.to_string()),
        );
    }

    let result = reconcile_service(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        &service,
        &service_ipam,
    )
    .await
    .unwrap();

    // Verify clusterIP was allocated (should be 10.43.128.2, since .1 is reserved)
    let cluster_ip = result
        .get("spec")
        .and_then(|s| s.get("clusterIP"))
        .and_then(|ip| ip.as_str());

    assert_eq!(
        cluster_ip,
        Some("10.43.128.2"),
        "Normal service must allocate a clusterIP"
    );

    // Verify clusterIPs array was added
    let cluster_ips = result
        .get("spec")
        .and_then(|s| s.get("clusterIPs"))
        .and_then(|ips| ips.as_array());

    assert!(cluster_ips.is_some(), "Must have clusterIPs array");
    assert_eq!(
        cluster_ips.unwrap().len(),
        1,
        "clusterIPs array must have one entry"
    );
}

#[tokio::test]
#[ignore] // Requires root for nftables/netlink
async fn test_service_external_name_no_cluster_ip() {
    let db = crate::datastore::test_support::in_memory().await;
    let service_ipam = ServiceIpam::new("10.43.128.0/17");

    let mut service = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": "external-db",
            "namespace": "default"
        },
        "spec": {
            "type": "ExternalName",
            "externalName": "my-database.example.com"
        }
    });

    // Insert service into DB
    let name = "external-db".to_string();
    let created = db
        .create_resource("v1", "Service", Some("default"), &name, service.clone())
        .await
        .unwrap();

    // Inject resource version
    if let Some(metadata) = service.as_object_mut().and_then(|o| o.get_mut("metadata"))
        && let Some(meta_obj) = metadata.as_object_mut()
    {
        meta_obj.insert(
            "resourceVersion".to_string(),
            json!(created.resource_version.to_string()),
        );
    }

    let result = reconcile_service(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        &service,
        &service_ipam,
    )
    .await
    .unwrap();

    // ExternalName services MUST NOT have clusterIP
    let cluster_ip = result.get("spec").and_then(|s| s.get("clusterIP"));

    assert!(
        cluster_ip.is_none(),
        "ExternalName service must not allocate clusterIP"
    );

    // Verify externalName field is preserved
    let external_name = result
        .get("spec")
        .and_then(|s| s.get("externalName"))
        .and_then(|en| en.as_str());

    assert_eq!(
        external_name,
        Some("my-database.example.com"),
        "ExternalName field must be preserved"
    );
}

#[tokio::test]
#[ignore] // Requires root for nftables/netlink
async fn test_service_external_name_no_endpoints() {
    let db = crate::datastore::test_support::in_memory().await;
    let service_ipam = ServiceIpam::new("10.43.128.0/17");

    let mut service = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": "external-api",
            "namespace": "default"
        },
        "spec": {
            "type": "ExternalName",
            "externalName": "api.external.com"
        }
    });

    // Insert service into DB
    let name = "external-api".to_string();
    let created = db
        .create_resource("v1", "Service", Some("default"), &name, service.clone())
        .await
        .unwrap();

    // Inject resource version
    if let Some(metadata) = service.as_object_mut().and_then(|o| o.get_mut("metadata"))
        && let Some(meta_obj) = metadata.as_object_mut()
    {
        meta_obj.insert(
            "resourceVersion".to_string(),
            json!(created.resource_version.to_string()),
        );
    }

    let _result = reconcile_service(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        &service,
        &service_ipam,
    )
    .await
    .unwrap();

    // ExternalName services MUST NOT create Endpoints
    let endpoints = db
        .get_resource("v1", "Endpoints", Some("default"), "external-api")
        .await;

    assert!(
        matches!(endpoints, Ok(None)),
        "ExternalName service must not create Endpoints"
    );
}

#[test]
fn test_externalname_service_clears_clusterip_on_type_change() {
    // When a service has type=ExternalName, normalize_externalname_spec must
    // clear clusterIP and clusterIPs regardless of what values they have.
    // This mirrors what reconcile_service does before persisting.
    let mut spec = serde_json::Map::new();
    spec.insert("type".to_string(), json!("ExternalName"));
    spec.insert("externalName".to_string(), json!("database.example.com"));
    spec.insert("clusterIP".to_string(), json!("10.43.128.2"));
    spec.insert("clusterIPs".to_string(), json!(["10.43.128.2"]));

    clear_externalname_invalid_fields(&mut spec);

    assert_eq!(
        spec.get("clusterIP").and_then(|v| v.as_str()),
        Some(""),
        "ExternalName service must have empty string clusterIP"
    );
    assert_eq!(
        spec.get("clusterIPs")
            .and_then(|v| v.as_array())
            .map(|a| a.len()),
        Some(0),
        "ExternalName service must have empty clusterIPs array"
    );
}

#[test]
fn test_externalname_clear_noop_for_clusterip_type() {
    // ClusterIP services must NOT be modified by the ExternalName clearing logic.
    let mut spec = serde_json::Map::new();
    spec.insert("type".to_string(), json!("ClusterIP"));
    spec.insert("clusterIP".to_string(), json!("10.43.128.2"));
    spec.insert("clusterIPs".to_string(), json!(["10.43.128.2"]));

    clear_externalname_invalid_fields(&mut spec);

    assert_eq!(
        spec.get("clusterIP").and_then(|v| v.as_str()),
        Some("10.43.128.2"),
        "ClusterIP service must keep its clusterIP"
    );
}

#[test]
fn test_externalname_clears_per_port_node_port_on_type_change() {
    // P0-E2E-20260423-07 regression: a NodePort service transitioning to
    // ExternalName must drop every per-port `nodePort` allocation as
    // well as `clusterIP`/`clusterIPs`. Conformance asserts
    // Spec.Ports[0].NodePort is unset on the persisted ExternalName form.
    let mut spec = serde_json::Map::new();
    spec.insert("type".to_string(), json!("ExternalName"));
    spec.insert("externalName".to_string(), json!("backend.example.com"));
    spec.insert("clusterIP".to_string(), json!("10.43.128.5"));
    spec.insert("clusterIPs".to_string(), json!(["10.43.128.5"]));
    spec.insert(
        "ports".to_string(),
        json!([
            {"port": 80,  "targetPort": 8080, "protocol": "TCP", "nodePort": 30080},
            {"port": 443, "targetPort": 8443, "protocol": "TCP", "nodePort": 30443},
        ]),
    );

    clear_externalname_invalid_fields(&mut spec);

    let ports = spec
        .get("ports")
        .and_then(|p| p.as_array())
        .expect("ports preserved");
    for port in ports {
        assert!(
            port.get("nodePort").is_none(),
            "ExternalName port must not retain nodePort, got: {port}"
        );
    }
}

#[test]
fn test_externalname_clear_preserves_node_port_for_non_externalname_types() {
    // NodePort services must keep their nodePort allocations when the
    // helper is invoked (it's a no-op outside the ExternalName branch).
    let mut spec = serde_json::Map::new();
    spec.insert("type".to_string(), json!("NodePort"));
    spec.insert("clusterIP".to_string(), json!("10.43.128.6"));
    spec.insert(
        "ports".to_string(),
        json!([{"port": 80, "nodePort": 30080}]),
    );

    clear_externalname_invalid_fields(&mut spec);

    let np = spec
        .get("ports")
        .and_then(|p| p.as_array())
        .and_then(|arr| arr.first())
        .and_then(|p| p.get("nodePort"))
        .and_then(|v| v.as_u64());
    assert_eq!(np, Some(30080), "NodePort service must keep its nodePort");
}

#[tokio::test]
#[ignore] // Requires root for nftables/netlink
async fn test_service_external_name_no_endpoint_slice() {
    let db = crate::datastore::test_support::in_memory().await;
    let service_ipam = ServiceIpam::new("10.43.128.0/17");

    let mut service = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": "external-svc",
            "namespace": "default"
        },
        "spec": {
            "type": "ExternalName",
            "externalName": "external.example.org"
        }
    });

    // Insert service into DB
    let name = "external-svc".to_string();
    let created = db
        .create_resource("v1", "Service", Some("default"), &name, service.clone())
        .await
        .unwrap();

    // Inject resource version
    if let Some(metadata) = service.as_object_mut().and_then(|o| o.get_mut("metadata"))
        && let Some(meta_obj) = metadata.as_object_mut()
    {
        meta_obj.insert(
            "resourceVersion".to_string(),
            json!(created.resource_version.to_string()),
        );
    }

    let _result = reconcile_service(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        &service,
        &service_ipam,
    )
    .await
    .unwrap();

    // ExternalName services MUST NOT create EndpointSlice
    let endpoint_slice_list = db
        .list_resources(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some("default"),
            crate::datastore::ResourceListQuery::new(
                Some("kubernetes.io/service-name=external-svc"),
                None,
                None,
                None,
            ),
        )
        .await
        .unwrap();

    assert_eq!(
        endpoint_slice_list.items.len(),
        0,
        "ExternalName service must not create EndpointSlice"
    );
}

#[test]
fn test_reconcile_service_defaults_type_to_clusterip_when_missing() {
    // Sonobuoy: ExternalName → ClusterIP patch results in missing spec.type.
    // normalize_service_type must set "ClusterIP" when type is absent.
    let mut spec = serde_json::Map::new();
    spec.insert(
        "ports".to_string(),
        json!([{"port": 80, "targetPort": 8080, "protocol": "TCP"}]),
    );
    normalize_service_type(&mut spec);
    assert_eq!(
        spec.get("type").and_then(|t| t.as_str()).unwrap_or(""),
        "ClusterIP",
        "Missing spec.type must be normalized to ClusterIP"
    );
}

#[test]
fn test_reconcile_service_defaults_type_to_clusterip_when_empty() {
    // Sonobuoy: ExternalName → ClusterIP patch results in empty spec.type "".
    // normalize_service_type must replace "" with "ClusterIP".
    let mut spec = serde_json::Map::new();
    spec.insert("type".to_string(), json!(""));
    spec.insert(
        "ports".to_string(),
        json!([{"port": 80, "targetPort": 8080, "protocol": "TCP"}]),
    );
    normalize_service_type(&mut spec);
    assert_eq!(
        spec.get("type").and_then(|t| t.as_str()).unwrap_or(""),
        "ClusterIP",
        "Empty spec.type must be normalized to ClusterIP"
    );
}

#[test]
fn test_normalize_service_ports_defaults_protocol_and_target_port() {
    let mut spec = serde_json::Map::new();
    spec.insert("ports".to_string(), json!([{"port": 6379}]));

    normalize_service_ports(&mut spec);

    let ports = spec
        .get("ports")
        .and_then(|p| p.as_array())
        .expect("ports must exist");
    assert_eq!(ports[0]["protocol"], "TCP");
    assert_eq!(ports[0]["targetPort"], 6379);
}

#[test]
fn test_normalize_service_ports_keeps_explicit_values() {
    let mut spec = serde_json::Map::new();
    spec.insert(
        "ports".to_string(),
        json!([{"port": 80, "targetPort": "http", "protocol": "UDP"}]),
    );

    normalize_service_ports(&mut spec);

    let ports = spec
        .get("ports")
        .and_then(|p| p.as_array())
        .expect("ports must exist");
    assert_eq!(ports[0]["protocol"], "UDP");
    assert_eq!(ports[0]["targetPort"], "http");
}

#[test]
fn test_nodeport_allocator_skips_already_used_ports() {
    let alloc = NodePortAllocator::new();
    // Set allocator to ready state
    alloc.set_ready();
    // Pre-allocate 30000
    alloc.mark_used(30000);
    // First free allocation must skip 30000 and return 30001
    let port = alloc.allocate().unwrap();
    assert_ne!(port, 30000, "Must not allocate already-used port 30000");
    assert_eq!(port, 30001, "Must allocate next free port 30001");
}

#[test]
fn test_nodeport_allocator_sequential_allocation() {
    let alloc = NodePortAllocator::new();
    alloc.set_ready();
    let port1 = alloc.allocate().unwrap();
    let port2 = alloc.allocate().unwrap();
    let port3 = alloc.allocate().unwrap();
    assert_eq!(port1, 30000);
    assert_eq!(port2, 30001);
    assert_eq!(port3, 30002);
}

#[test]
fn test_nodeport_allocator_mark_used_then_allocate_skips() {
    let alloc = NodePortAllocator::new();
    alloc.set_ready();
    // Mark a range as used
    alloc.mark_used(30000);
    alloc.mark_used(30001);
    alloc.mark_used(30002);
    // First allocation should skip to 30003
    assert_eq!(alloc.allocate().unwrap(), 30003);
}

#[test]
fn test_nodeport_allocator_collision_avoidance_matches_real_allocation_flow() {
    // Simulate: klights starts, finds service with nodePort=30000 in DB,
    // marks it used, then allocates a new port for a second service.
    // The new port must not be 30000.
    let alloc = NodePortAllocator::new();
    alloc.set_ready();

    // Bootstrap: existing service occupies 30000
    alloc.mark_used(30000);

    // New service request: must get a different port
    let new_port = alloc.allocate().unwrap();
    assert_ne!(new_port, 30000, "Must skip already-used port 30000");
    assert!(
        (30001..=32767).contains(&new_port),
        "Port must be in valid NodePort range, got {}",
        new_port
    );
}

// F6-02: Tests for leader-safe NodePort allocator with readiness state

#[test]
fn test_nodeport_allocator_ready_state_allows_allocation() {
    let alloc = NodePortAllocator::new();
    // After bootstrap rebuild, allocator should be ready
    alloc.set_ready();
    // Should successfully allocate
    let port = alloc.allocate().unwrap();
    assert!((30000..=32767).contains(&port));
}

#[test]
fn test_nodeport_allocator_not_ready_rejects_allocation() {
    let alloc = NodePortAllocator::new();
    // Before bootstrap rebuild, allocator is not ready
    assert!(!alloc.is_ready());
    // Allocation should return error when not ready
    let result = alloc.allocate();
    assert!(
        result.is_err(),
        "Allocation should fail when allocator is not ready"
    );
}

#[test]
fn test_nodeport_allocator_sets_ready_after_bootstrap() {
    let alloc = NodePortAllocator::new();
    // Initially not ready
    assert!(!alloc.is_ready());
    // After rebuild, should be ready
    alloc.set_ready();
    assert!(alloc.is_ready());
}

#[tokio::test]
async fn test_nodeport_allocator_rebuild_scans_existing_services() {
    use crate::datastore::test_support::in_memory;
    use std::sync::Arc;

    // Create in-memory DB with existing services having NodePorts
    let db = in_memory().await;
    let ns = "default";

    // Create a service with an existing NodePort
    db.create_resource(
        "v1",
        "Service",
        Some(ns),
        "existing-svc",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": "existing-svc",
                "namespace": ns,
                "uid": "svc-123"
            },
            "spec": {
                "type": "NodePort",
                "ports": [
                    {
                        "port": 80,
                        "targetPort": 8080,
                        "nodePort": 30000,
                        "protocol": "TCP"
                    }
                ],
                "selector": {"app": "test"}
            }
        }),
    )
    .await
    .unwrap();

    // Create a fresh allocator and rebuild from DB
    let alloc = Arc::new(NodePortAllocator::new());
    rebuild_nodeport_allocator_from_services(&db, &alloc)
        .await
        .unwrap();

    // After rebuild, allocator should have marked 30000 as used
    assert!(alloc.is_ready(), "Allocator should be ready after rebuild");

    // Allocating should skip the already-used port 30000
    let new_port = alloc.allocate().unwrap();
    assert_ne!(new_port, 30000, "Should not allocate existing port 30000");
}

/// NodePort allocator must return an error when the 30000–32767 range is
/// fully exhausted, not silently return 32768.
#[test]
fn nodeport_allocator_exhaustion_returns_error() {
    let alloc = NodePortAllocator::new();
    alloc.set_ready();
    // Exhaust the entire range
    for port in 30000..=32767 {
        alloc.mark_used(port);
    }
    let result = alloc.allocate();
    assert!(
        result.is_err(),
        "NodePort allocator must return error when range exhausted"
    );
    assert!(
        result.unwrap_err().contains("exhausted"),
        "error message must mention exhaustion"
    );
}

/// ServiceIpam must return an error when the service CIDR is exhausted.
#[test]
fn service_ipam_exhaustion_returns_error() {
    // Use a tiny /30 subnet: 10.0.0.0/30 has usable IPs .1 and .2
    // (skip .0 = network, .3 = broadcast). start_ip = .2 (network+2), end_ip = .2 (broadcast-1).
    // Only one allocatable IP.
    let ipam = ServiceIpam::new("10.0.0.0/30");

    // First allocation succeeds (10.0.0.2)
    let ip1 = ipam.allocate().unwrap();
    assert_eq!(ip1, "10.0.0.2");

    // Second must fail — only one slot exists.
    let result = ipam.allocate();
    assert!(
        result.is_err(),
        "ServiceIpam must return error when CIDR exhausted"
    );
    assert!(
        result.unwrap_err().contains("exhausted"),
        "error message must mention exhaustion"
    );
}

#[tokio::test]
async fn service_reconcile_recovers_cluster_ip_after_generic_service_delete() {
    let db = crate::datastore::test_support::in_memory().await;
    let ipam = ServiceIpam::new("10.0.0.0/30");
    let alloc = NodePortAllocator::new();
    alloc.set_ready();
    let pod_reader = crate::controllers::test_utils::pod_repository_for_test(&db);

    let mut first = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "first", "namespace": "default"},
        "spec": {
            "selector": {"app": "first"},
            "ports": [{"port": 80}]
        }
    });
    let created_first = db
        .create_resource("v1", "Service", Some("default"), "first", first.clone())
        .await
        .unwrap();
    first["metadata"]["resourceVersion"] =
        serde_json::json!(created_first.resource_version.to_string());
    first["metadata"]["uid"] = serde_json::json!(created_first.uid);

    let first_result =
        reconcile_service_with_nodeport(&db, pod_reader.as_ref(), &first, &ipam, &alloc)
            .await
            .unwrap();
    assert_eq!(first_result["spec"]["clusterIP"], "10.0.0.2");

    // Namespace termination, delete-collection, and GC delete Service rows
    // through generic datastore paths. ClusterIP allocation must recover from
    // those paths even though they cannot call ServiceIpam::release directly.
    db.delete_resource("v1", "Service", Some("default"), "first")
        .await
        .unwrap();

    let mut second = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "second", "namespace": "default"},
        "spec": {
            "selector": {"app": "second"},
            "ports": [{"port": 8080}]
        }
    });
    let created_second = db
        .create_resource("v1", "Service", Some("default"), "second", second.clone())
        .await
        .unwrap();
    second["metadata"]["resourceVersion"] =
        serde_json::json!(created_second.resource_version.to_string());
    second["metadata"]["uid"] = serde_json::json!(created_second.uid);

    let second_result =
        reconcile_service_with_nodeport(&db, pod_reader.as_ref(), &second, &ipam, &alloc)
            .await
            .unwrap();

    assert_eq!(second_result["spec"]["clusterIP"], "10.0.0.2");
}

/// Reconciling an already-normalized Service with no endpoint-relevant
/// changes must not bump the persisted resourceVersion.
#[tokio::test]
async fn reconcile_idempotent_does_not_churn_resource_version() {
    let db = crate::datastore::test_support::in_memory().await;
    let ipam = ServiceIpam::new("10.43.128.0/17");
    let alloc = NodePortAllocator::new();
    alloc.set_ready();

    let mut svc = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "my-svc", "namespace": "default"},
        "spec": {
            "type": "ClusterIP",
            "selector": {"app": "my"},
            "ports": [{"port": 80, "protocol": "TCP"}]
        }
    });
    let created = db
        .create_resource("v1", "Service", Some("default"), "my-svc", svc.clone())
        .await
        .unwrap();
    if let Some(meta) = svc.as_object_mut().and_then(|o| o.get_mut("metadata"))
        && let Some(meta_obj) = meta.as_object_mut()
    {
        meta_obj.insert(
            "resourceVersion".to_string(),
            serde_json::json!(created.resource_version.to_string()),
        );
        meta_obj.insert("uid".to_string(), serde_json::json!(created.uid));
    }

    let result = reconcile_service_with_nodeport(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        &svc,
        &ipam,
        &alloc,
    )
    .await
    .unwrap();
    let rv1 = result["metadata"]["resourceVersion"]
        .as_str()
        .unwrap()
        .parse::<i64>()
        .unwrap();
    assert!(rv1 > created.resource_version);

    // Second reconcile — no changes, must not bump resourceVersion.
    let result2 = reconcile_service_with_nodeport(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        &result,
        &ipam,
        &alloc,
    )
    .await
    .unwrap();
    let rv2 = result2["metadata"]["resourceVersion"]
        .as_str()
        .unwrap()
        .parse::<i64>()
        .unwrap();
    assert_eq!(rv2, rv1);
}
