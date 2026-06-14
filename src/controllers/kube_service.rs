use crate::datastore::DatastoreBackend;
use crate::datastore::types::Resource;
use anyhow::Result;
use serde_json::{Value, json};

/// Derive the kubernetes service ClusterIP from the service CIDR.
/// Returns the first usable IP (network + 1), e.g. "10.43.128.0/17" -> "10.43.128.1".
pub fn derive_kubernetes_service_ip(service_cidr: &str) -> String {
    crate::utils::derive_first_ip(service_cidr)
}

/// Bootstrap the default ServiceCIDR object expected by conformance tests.
/// Idempotent — skips creation if the resource already exists.
pub async fn bootstrap_default_service_cidr(
    db: &dyn DatastoreBackend,
    service_cidr: &str,
) -> Result<()> {
    let exists = db
        .get_resource("networking.k8s.io/v1", "ServiceCIDR", None, "kubernetes")
        .await?
        .is_some();
    if exists {
        return Ok(());
    }

    let service_cidr_obj = json!({
        "apiVersion": "networking.k8s.io/v1",
        "kind": "ServiceCIDR",
        "metadata": {
            "name": "kubernetes"
        },
        "spec": {
            "cidrs": [service_cidr]
        }
    });

    db.create_resource(
        "networking.k8s.io/v1",
        "ServiceCIDR",
        None,
        "kubernetes",
        service_cidr_obj,
    )
    .await?;
    tracing::info!("Created default ServiceCIDR kubernetes ({})", service_cidr);

    Ok(())
}

/// Bootstrap kubernetes Service and Endpoints on startup.
/// Creates the "kubernetes" service with ClusterIP derived from service_cidr,
/// and Endpoints pointing to the API listener host IP for in-pod API access.
/// Idempotent — skips creation if service already exists.
pub async fn bootstrap_kubernetes_service(
    db: &dyn DatastoreBackend,
    service_cidr: &str,
    tls_port: u16,
    datapath: &dyn crate::networking::Datapath,
) -> Result<()> {
    let kubernetes_service_ip = derive_kubernetes_service_ip(service_cidr);

    let service = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": "kubernetes",
            "namespace": "default",
            "labels": {
                "component": "apiserver",
                "provider": "klights"
            }
        },
        "spec": {
            "clusterIP": kubernetes_service_ip,
            "clusterIPs": [kubernetes_service_ip],
            "ports": [
                {"name": "https", "port": 443, "protocol": "TCP", "targetPort": tls_port}
            ]
        }
    });

    create_or_reconcile_bootstrap_resource(
        db,
        "v1",
        "Service",
        Some("default"),
        "kubernetes",
        service,
        &["spec"],
    )
    .await?;
    tracing::info!(
        "Reconciled kubernetes Service (ClusterIP: {})",
        kubernetes_service_ip
    );

    // Create Endpoints pointing at the node-local pod gateway. Remote pods can
    // reach that address through the pod dataplane even when the leader's
    // underlay host IP is private to a different physical network.
    let endpoint_ip = match datapath.pod_gateway_ip().await {
        Ok(ip) => ip.to_string(),
        Err(err) => {
            tracing::warn!(
                error = %err,
                "failed to resolve pod gateway IP for kubernetes Endpoints; falling back to host IP"
            );
            datapath
                .host_ip()
                .await
                .map(|ip| ip.to_string())
                .unwrap_or_else(|_| "127.0.0.1".to_string())
        }
    };

    let endpoints = json!({
        "apiVersion": "v1",
        "kind": "Endpoints",
        "metadata": {
            "name": "kubernetes",
            "namespace": "default"
        },
        "subsets": [{
            "addresses": [{"ip": endpoint_ip}],
            "ports": [{"name": "https", "port": tls_port, "protocol": "TCP"}]
        }]
    });

    create_or_reconcile_bootstrap_resource(
        db,
        "v1",
        "Endpoints",
        Some("default"),
        "kubernetes",
        endpoints,
        &["subsets"],
    )
    .await?;
    tracing::info!(
        "Reconciled kubernetes Endpoints ({}:{})",
        endpoint_ip,
        tls_port
    );

    // P0-E2E-20260424b-08: conformance test asserts the kubernetes Service has
    // an EndpointSlice. Services without selectors are skipped by the normal
    // EndpointSlice reconciler, so we bootstrap it here alongside Endpoints.
    let endpointslice = json!({
        "apiVersion": "discovery.k8s.io/v1",
        "kind": "EndpointSlice",
        "metadata": {
            "name": "kubernetes",
            "namespace": "default",
            "labels": {
                "kubernetes.io/service-name": "kubernetes",
                "endpointslice.kubernetes.io/managed-by": "endpointslice-controller.k8s.io"
            }
        },
        "addressType": "IPv4",
        "endpoints": [{
            "addresses": [&endpoint_ip],
            "conditions": {"ready": true, "serving": true, "terminating": false}
        }],
        "ports": [{"name": "https", "port": tls_port as i64, "protocol": "TCP"}]
    });

    create_or_reconcile_bootstrap_resource(
        db,
        "discovery.k8s.io/v1",
        "EndpointSlice",
        Some("default"),
        "kubernetes",
        endpointslice,
        &["addressType", "endpoints", "ports"],
    )
    .await?;
    tracing::info!(
        "Reconciled kubernetes EndpointSlice ({}:{})",
        endpoint_ip,
        tls_port
    );

    Ok(())
}

async fn create_or_reconcile_bootstrap_resource(
    db: &dyn DatastoreBackend,
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
    desired: Value,
    top_level_fields: &[&str],
) -> Result<Resource> {
    let Some(existing) = db.get_resource(api_version, kind, namespace, name).await? else {
        return db
            .create_resource(api_version, kind, namespace, name, desired)
            .await;
    };

    let mut updated = (*existing.data).clone();
    for field in top_level_fields {
        if let Some(value) = desired.get(*field) {
            updated[*field] = value.clone();
        } else if let Some(obj) = updated.as_object_mut() {
            obj.remove(*field);
        }
    }
    if let Some(labels) = desired.pointer("/metadata/labels").cloned()
        && let Some(metadata) = updated.get_mut("metadata").and_then(|v| v.as_object_mut())
    {
        metadata.insert("labels".to_string(), labels);
    }

    if updated == *existing.data {
        return Ok(existing);
    }

    db.update_resource(
        api_version,
        kind,
        namespace,
        name,
        updated,
        existing.resource_version,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    struct EnvGuard {
        name: &'static str,
        previous: Option<std::ffi::OsString>,
    }

    impl EnvGuard {
        fn set(name: &'static str, value: &str) -> Self {
            let previous = std::env::var_os(name);
            // TODO: Audit that the environment access only happens in single-threaded code.
            unsafe { std::env::set_var(name, value) };
            Self { name, previous }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            match &self.previous {
                // TODO: Audit that the environment access only happens in single-threaded code.
                Some(value) => unsafe { std::env::set_var(self.name, value) },
                // TODO: Audit that the environment access only happens in single-threaded code.
                None => unsafe { std::env::remove_var(self.name) },
            }
        }
    }

    #[test]
    fn test_derive_kubernetes_service_ip_default() {
        assert_eq!(
            derive_kubernetes_service_ip("10.43.128.0/17"),
            "10.43.128.1"
        );
    }

    #[test]
    fn test_derive_kubernetes_service_ip_custom() {
        assert_eq!(
            derive_kubernetes_service_ip("10.50.128.0/17"),
            "10.50.128.1"
        );
        assert_eq!(
            derive_kubernetes_service_ip("10.44.128.0/17"),
            "10.44.128.1"
        );
        assert_eq!(
            derive_kubernetes_service_ip("192.168.0.0/24"),
            "192.168.0.1"
        );
    }

    #[tokio::test]
    async fn test_bootstrap_kubernetes_service_creates_service_and_endpoints() {
        let db = crate::datastore::test_support::in_memory().await;
        crate::controllers::namespace::init_default_namespaces(&db)
            .await
            .unwrap();

        bootstrap_kubernetes_service(
            &db,
            "10.50.128.0/17",
            7444,
            &crate::networking::test_support::MockNetworkProvider::new(),
        )
        .await
        .unwrap();

        // Verify Service uses derived ClusterIP from service CIDR
        let svc = db
            .get_resource("v1", "Service", Some("default"), "kubernetes")
            .await
            .unwrap();
        assert!(svc.is_some(), "kubernetes Service should exist");
        let svc_data = svc.unwrap().data;
        assert_eq!(svc_data["spec"]["clusterIP"], "10.50.128.1");
        assert_eq!(svc_data["spec"]["ports"][0]["port"], 443);
        assert_eq!(svc_data["spec"]["ports"][0]["targetPort"], 7444);

        // Verify Endpoints exist with host IP and correct port
        let ep = db
            .get_resource("v1", "Endpoints", Some("default"), "kubernetes")
            .await
            .unwrap();
        assert!(ep.is_some(), "kubernetes Endpoints should exist");
        let ep_data = ep.unwrap().data;
        let subsets = ep_data["subsets"].as_array().unwrap();
        // Host IP varies by machine — just verify it's a non-empty IP
        let ip = subsets[0]["addresses"][0]["ip"].as_str().unwrap();
        assert!(!ip.is_empty(), "Endpoint IP should not be empty");
        assert_ne!(ip, "0.0.0.0", "Endpoint IP should not be 0.0.0.0");
        assert_eq!(subsets[0]["ports"][0]["port"], 7444);

        // P0-E2E-20260424b-08: verify EndpointSlice also exists
        let eps = db
            .get_resource(
                "discovery.k8s.io/v1",
                "EndpointSlice",
                Some("default"),
                "kubernetes",
            )
            .await
            .unwrap();
        assert!(eps.is_some(), "kubernetes EndpointSlice should exist");
        let eps_data = eps.unwrap().data;
        assert_eq!(
            eps_data["metadata"]["labels"]["kubernetes.io/service-name"],
            "kubernetes"
        );
        assert_eq!(eps_data["addressType"], "IPv4");
        assert_eq!(eps_data["ports"][0]["port"], 7444);
        let eps_ip = eps_data["endpoints"][0]["addresses"][0]
            .as_str()
            .unwrap_or("");
        assert!(
            !eps_ip.is_empty(),
            "EndpointSlice address should not be empty"
        );
    }

    #[allow(clippy::await_holding_lock)] // ENV_LOCK serializes env-var-mutating tests.
    #[tokio::test]
    async fn test_bootstrap_kubernetes_service_uses_pod_gateway_not_underlay_host_ip() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _endpoint = EnvGuard::set("KLIGHTS_EXTERNAL_ENDPOINT", "198.51.100.74");
        let db = crate::datastore::test_support::in_memory().await;
        crate::controllers::namespace::init_default_namespaces(&db)
            .await
            .unwrap();
        let network = crate::networking::test_support::MockNetworkProvider::new();
        network.set_host_ip(std::net::Ipv4Addr::new(10, 206, 0, 10));
        network.set_pod_gateway_ip(std::net::Ipv4Addr::new(10, 43, 0, 1));

        bootstrap_kubernetes_service(&db, "10.43.128.0/17", 7679, &network)
            .await
            .unwrap();

        let endpoints = db
            .get_resource("v1", "Endpoints", Some("default"), "kubernetes")
            .await
            .unwrap()
            .expect("kubernetes Endpoints should exist")
            .data;
        assert_eq!(endpoints["subsets"][0]["addresses"][0]["ip"], "10.43.0.1");

        let endpoint_slice = db
            .get_resource(
                "discovery.k8s.io/v1",
                "EndpointSlice",
                Some("default"),
                "kubernetes",
            )
            .await
            .unwrap()
            .expect("kubernetes EndpointSlice should exist")
            .data;
        assert_eq!(endpoint_slice["endpoints"][0]["addresses"][0], "10.43.0.1");
    }

    #[tokio::test]
    async fn test_bootstrap_kubernetes_service_idempotent() {
        let db = crate::datastore::test_support::in_memory().await;
        crate::controllers::namespace::init_default_namespaces(&db)
            .await
            .unwrap();

        bootstrap_kubernetes_service(
            &db,
            "10.43.128.0/17",
            7443,
            &crate::networking::test_support::MockNetworkProvider::new(),
        )
        .await
        .unwrap();
        let result = bootstrap_kubernetes_service(
            &db,
            "10.43.128.0/17",
            7443,
            &crate::networking::test_support::MockNetworkProvider::new(),
        )
        .await;
        assert!(result.is_ok(), "Second bootstrap call should not error");

        let svcs = db
            .list_resources(
                "v1",
                "Service",
                Some("default"),
                crate::datastore::ResourceListQuery::all(),
            )
            .await
            .unwrap();
        let kubernetes_svcs: Vec<_> = svcs
            .items
            .iter()
            .filter(|r| r.name == "kubernetes")
            .collect();
        assert_eq!(
            kubernetes_svcs.len(),
            1,
            "Should have exactly 1 kubernetes Service"
        );
    }

    #[tokio::test]
    async fn test_bootstrap_kubernetes_service_reconciles_existing_tls_port() {
        let db = crate::datastore::test_support::in_memory().await;
        crate::controllers::namespace::init_default_namespaces(&db)
            .await
            .unwrap();
        let network = crate::networking::test_support::MockNetworkProvider::new();

        bootstrap_kubernetes_service(&db, "10.43.128.0/17", 7443, &network)
            .await
            .unwrap();
        bootstrap_kubernetes_service(&db, "10.43.128.0/17", 7679, &network)
            .await
            .unwrap();

        let service = db
            .get_resource("v1", "Service", Some("default"), "kubernetes")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            service.data["spec"]["ports"][0]["targetPort"].as_i64(),
            Some(7679)
        );

        let endpoints = db
            .get_resource("v1", "Endpoints", Some("default"), "kubernetes")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            endpoints.data["subsets"][0]["ports"][0]["port"].as_i64(),
            Some(7679)
        );

        let endpoint_slice = db
            .get_resource(
                "discovery.k8s.io/v1",
                "EndpointSlice",
                Some("default"),
                "kubernetes",
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(endpoint_slice.data["ports"][0]["port"].as_i64(), Some(7679));
    }

    #[tokio::test]
    async fn test_bootstrap_default_service_cidr_creates_resource() {
        let db = crate::datastore::test_support::in_memory().await;

        bootstrap_default_service_cidr(&db, "10.43.128.0/17")
            .await
            .unwrap();

        let sc = db
            .get_resource("networking.k8s.io/v1", "ServiceCIDR", None, "kubernetes")
            .await
            .unwrap();
        assert!(sc.is_some(), "default ServiceCIDR should exist");
        let sc_data = sc.unwrap().data;
        assert_eq!(sc_data["spec"]["cidrs"][0], "10.43.128.0/17");
    }

    #[tokio::test]
    async fn test_bootstrap_default_service_cidr_idempotent() {
        let db = crate::datastore::test_support::in_memory().await;

        bootstrap_default_service_cidr(&db, "10.43.128.0/17")
            .await
            .unwrap();
        bootstrap_default_service_cidr(&db, "10.43.128.0/17")
            .await
            .unwrap();

        let list = db
            .list_resources(
                "networking.k8s.io/v1",
                "ServiceCIDR",
                None,
                crate::datastore::ResourceListQuery::all(),
            )
            .await
            .unwrap();
        let defaults: Vec<_> = list
            .items
            .iter()
            .filter(|r| r.name == "kubernetes")
            .collect();
        assert_eq!(
            defaults.len(),
            1,
            "must only create one default ServiceCIDR"
        );
    }
}
