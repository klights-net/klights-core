use crate::admission::http_client::webhook_http_client_for;
use crate::admission::request_context::AdmissionRequestContext;
use crate::datastore::{DatastoreBackend, ResourceListQuery};
use crate::networking::service_routing::{Protocol, ServiceSpec};
use anyhow::{Context, Result};
use serde_json::Value;
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ResolvedWebhookTarget {
    pub(super) base_url: String,
    pub(super) dns_override: Option<(String, SocketAddr)>,
}

/// Resolve webhook target from clientConfig (either url field or service reference).
/// For service references, keep service DNS hostname in URL and pin DNS resolution
/// to the Service ClusterIP so TLS/SNI stays spec-compatible and Service targetPort
/// translation remains in the dataplane.
pub(super) async fn resolve_webhook_target(
    db: &dyn DatastoreBackend,
    client_config: &Value,
) -> Result<ResolvedWebhookTarget> {
    if let Some(url) = client_config.get("url").and_then(|u| u.as_str()) {
        return Ok(ResolvedWebhookTarget {
            base_url: url.to_string(),
            dns_override: None,
        });
    }

    if let Some(service_ref) = client_config.get("service") {
        let name = service_ref
            .get("name")
            .and_then(|n| n.as_str())
            .ok_or_else(|| anyhow::anyhow!("Service reference missing name"))?;
        let namespace = service_ref
            .get("namespace")
            .and_then(|ns| ns.as_str())
            .ok_or_else(|| anyhow::anyhow!("Service reference missing namespace"))?;

        let requested_port = service_ref
            .get("port")
            .and_then(|p| p.as_u64())
            .map(|p| u16::try_from(p).context("Service reference port out of range"))
            .transpose()?
            .unwrap_or(443);
        let service_spec = resolve_webhook_service_spec(db, namespace, name).await?;
        let selected_service_port = service_spec
            .ports
            .iter()
            .find(|port| {
                port.protocol == Protocol::Tcp
                    && port.service_port == requested_port
                    && !port.endpoints.is_empty()
            })
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Service {}/{} has no ready TCP endpoint for port {}",
                    namespace,
                    name,
                    requested_port
                )
            })?;
        let endpoint_ip = selected_service_port
            .endpoints
            .first()
            .copied()
            .ok_or_else(|| {
                anyhow::anyhow!("Service {}/{} has no ready endpoints", namespace, name)
            })?;
        let endpoint_port = selected_service_port.target_port;
        let path = service_ref
            .get("path")
            .and_then(|p| p.as_str())
            .unwrap_or("");
        let host = format!("{}.{}.svc", name, namespace);

        return Ok(ResolvedWebhookTarget {
            base_url: format!("https://{}:{}{}", host, endpoint_port, path),
            dns_override: Some((
                host,
                SocketAddr::new(IpAddr::V4(endpoint_ip), endpoint_port),
            )),
        });
    }

    anyhow::bail!("clientConfig must have either url or service field")
}

async fn resolve_webhook_service_spec(
    db: &dyn DatastoreBackend,
    namespace: &str,
    name: &str,
) -> Result<ServiceSpec> {
    let service = db
        .get_resource("v1", "Service", Some(namespace), name)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Service not found: {}/{}", namespace, name))?;

    let label_selector = format!("kubernetes.io/service-name={name}");
    let endpoint_slices = db
        .list_resources(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some(namespace),
            ResourceListQuery::new(Some(&label_selector), None, None, None),
        )
        .await?;
    let slice_refs: Vec<&Value> = endpoint_slices
        .items
        .iter()
        .map(|slice| slice.data.as_ref())
        .collect();
    if !slice_refs.is_empty()
        && let Some(spec) = ServiceSpec::from_service_and_endpointslices(&service.data, &slice_refs)
    {
        return Ok(spec);
    }

    let endpoints = db
        .get_resource("v1", "Endpoints", Some(namespace), name)
        .await?;
    if let Some(endpoints) = endpoints
        && let Some(spec) =
            ServiceSpec::from_service_and_endpoints(service.data.as_ref(), Some(&endpoints.data))
    {
        return Ok(spec);
    }

    anyhow::bail!("Service {}/{} has no ready endpoints", namespace, name)
}

pub(super) async fn call_webhook(
    db: &dyn DatastoreBackend,
    webhook: &Value,
    resource: &Value,
    context: &AdmissionRequestContext,
    timeout_seconds: u64,
) -> Result<Value> {
    let client_config = webhook
        .get("clientConfig")
        .ok_or_else(|| anyhow::anyhow!("Webhook missing clientConfig"))?;

    let target = resolve_webhook_target(db, client_config).await?;
    let url = add_timeout_query(&target.base_url, timeout_seconds)?;

    let admission_review = super::build_admission_review(context, resource);

    let dns_override = target
        .dns_override
        .as_ref()
        .map(|(host, addr)| (host.as_str(), *addr));
    let client = webhook_http_client_for(client_config, dns_override)?;
    let resp = client
        .post(&url)
        .timeout(Duration::from_secs(timeout_seconds))
        .json(&admission_review)
        .send()
        .await
        .map_err(|err| {
            let error_text = err.to_string();
            anyhow::anyhow!(
                "{}",
                format_webhook_call_error(&url, &error_text, err.is_timeout())
            )
        })?;

    if !resp.status().is_success() {
        anyhow::bail!("Webhook returned status {}", resp.status());
    }

    let response: Value = resp
        .json()
        .await
        .context("Failed to parse webhook response")?;

    Ok(response)
}

pub(super) fn add_timeout_query(base_url: &str, timeout_seconds: u64) -> Result<String> {
    let mut parsed = reqwest::Url::parse(base_url)
        .with_context(|| format!("Invalid webhook URL: {}", base_url))?;
    parsed
        .query_pairs_mut()
        .append_pair("timeout", &format!("{}s", timeout_seconds));
    Ok(parsed.to_string())
}

fn is_timeout_error_text(error_text: &str) -> bool {
    let normalized = error_text.to_ascii_lowercase();
    normalized.contains("deadline exceeded")
        || normalized.contains("timed out")
        || normalized.contains("timeout")
}

pub(super) fn format_webhook_call_error(url: &str, error_text: &str, is_timeout: bool) -> String {
    if is_timeout || is_timeout_error_text(error_text) {
        return format!(
            "Failed to call webhook at {}: context deadline exceeded: {}",
            url, error_text
        );
    }
    format!("Failed to call webhook at {}: {}", url, error_text)
}
