use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::Resource;
use klights_reconcile_api::ControllerStoreResult;
use serde_json::{Value, json};

#[async_trait]
pub trait KubernetesBootstrapStore: Send + Sync {
    async fn get_bootstrap_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> ControllerStoreResult<Option<Resource>>;

    async fn create_bootstrap_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        value: Value,
    ) -> ControllerStoreResult<Resource>;

    async fn update_bootstrap_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        value: Value,
        expected_resource_version: i64,
    ) -> ControllerStoreResult<Resource>;
}

/// Derive the kubernetes service ClusterIP from the service CIDR.
/// Returns the first usable IP (network + 1), e.g. "10.43.128.0/17" -> "10.43.128.1".
pub fn derive_kubernetes_service_ip(service_cidr: &str) -> String {
    klights_types::first_usable_ipv4(service_cidr)
}

/// Bootstrap the default ServiceCIDR object expected by conformance tests.
/// Idempotent — skips creation if the resource already exists.
pub async fn bootstrap_default_service_cidr<S: KubernetesBootstrapStore + ?Sized>(
    store: &S,
    service_cidr: &str,
) -> Result<()> {
    let exists = store
        .get_bootstrap_resource("networking.k8s.io/v1", "ServiceCIDR", None, "kubernetes")
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

    store
        .create_bootstrap_resource(
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
pub async fn bootstrap_kubernetes_service<S: KubernetesBootstrapStore + ?Sized>(
    store: &S,
    service_cidr: &str,
    tls_port: u16,
    datapath: &dyn klights_network_api::Datapath,
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
        store,
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
        store,
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
        store,
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

async fn create_or_reconcile_bootstrap_resource<S: KubernetesBootstrapStore + ?Sized>(
    store: &S,
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
    desired: Value,
    top_level_fields: &[&str],
) -> Result<Resource> {
    let Some(existing) = store
        .get_bootstrap_resource(api_version, kind, namespace, name)
        .await?
    else {
        return store
            .create_bootstrap_resource(api_version, kind, namespace, name, desired)
            .await
            .map_err(Into::into);
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

    store
        .update_bootstrap_resource(
            api_version,
            kind,
            namespace,
            name,
            updated,
            existing.resource_version,
        )
        .await
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use klights_network_api::{CniAddRequest, DatapathFuture, PodNetwork, SandboxId};
    use klights_reconcile_api::ControllerStoreError;
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct MemoryBootstrapStore {
        resources: Mutex<Vec<Resource>>,
    }

    impl MemoryBootstrapStore {
        fn resource(
            &self,
            api_version: &str,
            kind: &str,
            namespace: Option<&str>,
            name: &str,
        ) -> Option<Resource> {
            self.resources
                .lock()
                .unwrap()
                .iter()
                .find(|resource| {
                    resource.api_version == api_version
                        && resource.kind == kind
                        && resource.namespace.as_deref() == namespace
                        && resource.name == name
                })
                .cloned()
        }

        fn count(&self, api_version: &str, kind: &str, name: &str) -> usize {
            self.resources
                .lock()
                .unwrap()
                .iter()
                .filter(|resource| {
                    resource.api_version == api_version
                        && resource.kind == kind
                        && resource.name == name
                })
                .count()
        }
    }

    fn resource_with_version(mut value: Value, resource_version: i64) -> Resource {
        let metadata = value
            .get_mut("metadata")
            .and_then(Value::as_object_mut)
            .expect("bootstrap test resource metadata");
        metadata
            .entry("uid".to_string())
            .or_insert_with(|| json!(format!("bootstrap-{resource_version}")));
        metadata.insert(
            "resourceVersion".to_string(),
            json!(resource_version.to_string()),
        );
        Resource::try_from_data(Arc::new(value)).expect("bootstrap test resource identity")
    }

    #[async_trait]
    impl KubernetesBootstrapStore for MemoryBootstrapStore {
        async fn get_bootstrap_resource(
            &self,
            api_version: &str,
            kind: &str,
            namespace: Option<&str>,
            name: &str,
        ) -> ControllerStoreResult<Option<Resource>> {
            Ok(self.resource(api_version, kind, namespace, name))
        }

        async fn create_bootstrap_resource(
            &self,
            api_version: &str,
            kind: &str,
            namespace: Option<&str>,
            name: &str,
            value: Value,
        ) -> ControllerStoreResult<Resource> {
            if self.resource(api_version, kind, namespace, name).is_some() {
                return Err(ControllerStoreError::conflict(
                    "duplicate bootstrap resource",
                ));
            }
            let resource_version = self.resources.lock().unwrap().len() as i64 + 1;
            let resource = resource_with_version(value, resource_version);
            self.resources.lock().unwrap().push(resource.clone());
            Ok(resource)
        }

        async fn update_bootstrap_resource(
            &self,
            api_version: &str,
            kind: &str,
            namespace: Option<&str>,
            name: &str,
            value: Value,
            expected_resource_version: i64,
        ) -> ControllerStoreResult<Resource> {
            let mut resources = self.resources.lock().unwrap();
            let Some(current) = resources.iter_mut().find(|resource| {
                resource.api_version == api_version
                    && resource.kind == kind
                    && resource.namespace.as_deref() == namespace
                    && resource.name == name
            }) else {
                return Err(ControllerStoreError::not_found(
                    "bootstrap resource missing",
                ));
            };
            if current.resource_version != expected_resource_version {
                return Err(ControllerStoreError::conflict("stale bootstrap resource"));
            }
            let updated = resource_with_version(value, expected_resource_version + 1);
            *current = updated.clone();
            Ok(updated)
        }
    }

    struct FixedDatapath {
        host_ip: IpAddr,
        pod_gateway_ip: IpAddr,
    }

    impl Default for FixedDatapath {
        fn default() -> Self {
            Self {
                host_ip: Ipv4Addr::new(192, 0, 2, 10).into(),
                pod_gateway_ip: Ipv4Addr::new(10, 43, 0, 1).into(),
            }
        }
    }

    impl klights_network_api::Datapath for FixedDatapath {
        fn cni_add(&self, _request: CniAddRequest) -> DatapathFuture<'_, PodNetwork> {
            Box::pin(async { panic!("kubernetes Service bootstrap must not attach pods") })
        }

        fn cni_del<'a>(&'a self, _sandbox_id: &'a SandboxId) -> DatapathFuture<'a, ()> {
            Box::pin(async { Ok(()) })
        }

        fn host_ip(&self) -> DatapathFuture<'_, IpAddr> {
            let host_ip = self.host_ip;
            Box::pin(async move { Ok(host_ip) })
        }

        fn pod_gateway_ip(&self) -> DatapathFuture<'_, IpAddr> {
            let pod_gateway_ip = self.pod_gateway_ip;
            Box::pin(async move { Ok(pod_gateway_ip) })
        }

        fn shutdown(&self) -> DatapathFuture<'_, ()> {
            Box::pin(async { Ok(()) })
        }
    }

    async fn bootstrap_service(store: &MemoryBootstrapStore, tls_port: u16) {
        bootstrap_kubernetes_service(store, "10.43.128.0/17", tls_port, &FixedDatapath::default())
            .await
            .unwrap();
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
        for (cidr, expected) in [
            ("10.50.128.0/17", "10.50.128.1"),
            ("10.44.128.0/17", "10.44.128.1"),
            ("192.168.0.0/24", "192.168.0.1"),
        ] {
            assert_eq!(derive_kubernetes_service_ip(cidr), expected);
        }
    }

    #[tokio::test]
    async fn test_bootstrap_kubernetes_service_creates_service_and_endpoints() {
        let store = MemoryBootstrapStore::default();
        let datapath = FixedDatapath::default();
        bootstrap_kubernetes_service(&store, "10.50.128.0/17", 7444, &datapath)
            .await
            .unwrap();

        let service = store
            .resource("v1", "Service", Some("default"), "kubernetes")
            .unwrap();
        assert_eq!(service.data["spec"]["clusterIP"], "10.50.128.1");
        assert_eq!(service.data["spec"]["ports"][0]["port"], 443);
        assert_eq!(service.data["spec"]["ports"][0]["targetPort"], 7444);

        let endpoints = store
            .resource("v1", "Endpoints", Some("default"), "kubernetes")
            .unwrap();
        assert_eq!(
            endpoints.data["subsets"][0]["addresses"][0]["ip"],
            "10.43.0.1"
        );
        assert_eq!(endpoints.data["subsets"][0]["ports"][0]["port"], 7444);

        let slice = store
            .resource(
                "discovery.k8s.io/v1",
                "EndpointSlice",
                Some("default"),
                "kubernetes",
            )
            .unwrap();
        assert_eq!(
            slice.data["metadata"]["labels"]["kubernetes.io/service-name"],
            "kubernetes"
        );
        assert_eq!(slice.data["addressType"], "IPv4");
        assert_eq!(slice.data["ports"][0]["port"], 7444);
        assert_eq!(slice.data["endpoints"][0]["addresses"][0], "10.43.0.1");
    }

    #[tokio::test]
    async fn test_bootstrap_kubernetes_service_uses_pod_gateway_not_underlay_host_ip() {
        let store = MemoryBootstrapStore::default();
        let datapath = FixedDatapath {
            host_ip: Ipv4Addr::new(10, 206, 0, 10).into(),
            pod_gateway_ip: Ipv4Addr::new(10, 43, 0, 1).into(),
        };
        bootstrap_kubernetes_service(&store, "10.43.128.0/17", 7679, &datapath)
            .await
            .unwrap();

        let endpoints = store
            .resource("v1", "Endpoints", Some("default"), "kubernetes")
            .unwrap();
        assert_eq!(
            endpoints.data["subsets"][0]["addresses"][0]["ip"],
            "10.43.0.1"
        );
        let slice = store
            .resource(
                "discovery.k8s.io/v1",
                "EndpointSlice",
                Some("default"),
                "kubernetes",
            )
            .unwrap();
        assert_eq!(slice.data["endpoints"][0]["addresses"][0], "10.43.0.1");
    }

    #[tokio::test]
    async fn test_bootstrap_kubernetes_service_idempotent() {
        let store = MemoryBootstrapStore::default();
        bootstrap_service(&store, 7443).await;
        bootstrap_service(&store, 7443).await;
        assert_eq!(store.count("v1", "Service", "kubernetes"), 1);
    }

    #[tokio::test]
    async fn test_bootstrap_kubernetes_service_reconciles_existing_tls_port() {
        let store = MemoryBootstrapStore::default();
        bootstrap_service(&store, 7443).await;
        bootstrap_service(&store, 7679).await;

        let service = store
            .resource("v1", "Service", Some("default"), "kubernetes")
            .unwrap();
        assert_eq!(service.data["spec"]["ports"][0]["targetPort"], 7679);
        let endpoints = store
            .resource("v1", "Endpoints", Some("default"), "kubernetes")
            .unwrap();
        assert_eq!(endpoints.data["subsets"][0]["ports"][0]["port"], 7679);
        let slice = store
            .resource(
                "discovery.k8s.io/v1",
                "EndpointSlice",
                Some("default"),
                "kubernetes",
            )
            .unwrap();
        assert_eq!(slice.data["ports"][0]["port"], 7679);
    }

    #[tokio::test]
    async fn test_bootstrap_default_service_cidr_creates_resource() {
        let store = MemoryBootstrapStore::default();
        bootstrap_default_service_cidr(&store, "10.43.128.0/17")
            .await
            .unwrap();
        let service_cidr = store
            .resource("networking.k8s.io/v1", "ServiceCIDR", None, "kubernetes")
            .unwrap();
        assert_eq!(service_cidr.data["spec"]["cidrs"][0], "10.43.128.0/17");
    }

    #[tokio::test]
    async fn test_bootstrap_default_service_cidr_idempotent() {
        let store = MemoryBootstrapStore::default();
        bootstrap_default_service_cidr(&store, "10.43.128.0/17")
            .await
            .unwrap();
        bootstrap_default_service_cidr(&store, "10.43.128.0/17")
            .await
            .unwrap();
        assert_eq!(
            store.count("networking.k8s.io/v1", "ServiceCIDR", "kubernetes"),
            1
        );
    }
}
