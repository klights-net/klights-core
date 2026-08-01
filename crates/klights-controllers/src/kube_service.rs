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
