use std::collections::{HashMap, VecDeque};
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context as _, Result, anyhow};
use klights_cluster_core::Resource;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::datastore::{DatastoreHandle, ResourceListQuery};
use k8s_native_service::admission::{
    AdmissionDependencyError, AdmissionEngine, AdmissionQuery, AdmissionResource,
    AdmissionWebhookClient, AdmissionWebhookRequest, WebhookTarget, WebhookTargetResolver,
};
use klights_networking::service_routing::{Protocol, ServiceSpec};

const CA_BUNDLE_CLIENT_CACHE_CAPACITY: usize = 32;
type CaFingerprint = [u8; 32];

pub(crate) struct RootAdmissionQuery {
    db: DatastoreHandle,
}

impl RootAdmissionQuery {
    pub(crate) fn new(db: DatastoreHandle) -> Arc<Self> {
        Arc::new(Self { db })
    }
}

fn admission_resource(resource: Resource) -> AdmissionResource {
    AdmissionResource {
        name: resource.name,
        data: resource.data,
    }
}

fn dependency_error(error: impl std::fmt::Display) -> AdmissionDependencyError {
    AdmissionDependencyError::new(error.to_string())
}

#[async_trait::async_trait]
impl AdmissionQuery for RootAdmissionQuery {
    async fn get_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> std::result::Result<Option<AdmissionResource>, AdmissionDependencyError> {
        crate::datastore::DatastoreBackend::get_resource(
            self.db.as_ref(),
            api_version,
            kind,
            namespace,
            name,
        )
        .await
        .map(|resource| resource.map(admission_resource))
        .map_err(dependency_error)
    }

    async fn list_resources(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        label_selector: Option<&str>,
    ) -> std::result::Result<Vec<AdmissionResource>, AdmissionDependencyError> {
        crate::datastore::DatastoreBackend::list_resources(
            self.db.as_ref(),
            api_version,
            kind,
            namespace,
            ResourceListQuery::new(label_selector, None, None, None),
        )
        .await
        .map(|page| page.items.into_iter().map(admission_resource).collect())
        .map_err(dependency_error)
    }
}

pub(crate) struct RootWebhookTargetResolver {
    query: Arc<dyn AdmissionQuery>,
}

impl RootWebhookTargetResolver {
    pub(crate) fn new(query: Arc<dyn AdmissionQuery>) -> Arc<Self> {
        Arc::new(Self { query })
    }

    async fn service_spec(
        &self,
        namespace: &str,
        name: &str,
    ) -> std::result::Result<ServiceSpec, AdmissionDependencyError> {
        let service = self
            .query
            .get_resource("v1", "Service", Some(namespace), name)
            .await?
            .ok_or_else(|| {
                AdmissionDependencyError::new(format!("Service not found: {namespace}/{name}"))
            })?;

        let label_selector = format!("kubernetes.io/service-name={name}");
        let endpoint_slices = self
            .query
            .list_resources(
                "discovery.k8s.io/v1",
                "EndpointSlice",
                Some(namespace),
                Some(&label_selector),
            )
            .await?;
        let slice_refs: Vec<&Value> = endpoint_slices
            .iter()
            .map(|slice| slice.data.as_ref())
            .collect();
        if !slice_refs.is_empty()
            && let Some(spec) =
                ServiceSpec::from_service_and_endpointslices(&service.data, &slice_refs)
        {
            return Ok(spec);
        }

        let endpoints = self
            .query
            .get_resource("v1", "Endpoints", Some(namespace), name)
            .await?;
        if let Some(endpoints) = endpoints
            && let Some(spec) = ServiceSpec::from_service_and_endpoints(
                service.data.as_ref(),
                Some(&endpoints.data),
            )
        {
            return Ok(spec);
        }

        Err(AdmissionDependencyError::new(format!(
            "Service {namespace}/{name} has no ready endpoints"
        )))
    }
}

#[async_trait::async_trait]
impl WebhookTargetResolver for RootWebhookTargetResolver {
    async fn resolve(
        &self,
        client_config: &Value,
    ) -> std::result::Result<WebhookTarget, AdmissionDependencyError> {
        if let Some(url) = client_config.get("url").and_then(Value::as_str) {
            return Ok(WebhookTarget {
                base_url: url.to_string(),
                dns_override: None,
            });
        }

        let service_ref = client_config.get("service").ok_or_else(|| {
            AdmissionDependencyError::new("clientConfig must have either url or service field")
        })?;
        let name = service_ref
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| AdmissionDependencyError::new("Service reference missing name"))?;
        let namespace = service_ref
            .get("namespace")
            .and_then(Value::as_str)
            .ok_or_else(|| AdmissionDependencyError::new("Service reference missing namespace"))?;
        let requested_port = service_ref
            .get("port")
            .and_then(Value::as_u64)
            .map(|port| {
                u16::try_from(port).map_err(|_| {
                    AdmissionDependencyError::new("Service reference port out of range")
                })
            })
            .transpose()?
            .unwrap_or(443);
        let service_spec = self.service_spec(namespace, name).await?;
        let selected_port = service_spec
            .ports
            .iter()
            .find(|port| {
                port.protocol == Protocol::Tcp
                    && port.service_port == requested_port
                    && !port.endpoints.is_empty()
            })
            .ok_or_else(|| {
                AdmissionDependencyError::new(format!(
                    "Service {namespace}/{name} has no ready TCP endpoint for port {requested_port}"
                ))
            })?;
        let path = service_ref
            .get("path")
            .and_then(Value::as_str)
            .unwrap_or("");
        let host = format!("{name}.{namespace}.svc");
        // Preserve the Kubernetes Service boundary. The host-side nft output
        // path owns ClusterIP load balancing and targetPort translation,
        // including routing to ready endpoints on another node. Pinning the
        // client directly to the first Pod IP bypasses those semantics and
        // makes admission availability depend on one endpoint and route.
        let service_port = selected_port.service_port;
        Ok(WebhookTarget {
            base_url: format!("https://{host}:{service_port}{path}"),
            dns_override: Some((
                host,
                SocketAddr::new(IpAddr::V4(service_spec.cluster_ip), service_port),
            )),
        })
    }
}

pub(crate) struct RootAdmissionWebhookClient {
    default_client: std::result::Result<reqwest::Client, String>,
    ca_bundle_clients: Mutex<CaBundleClientCache>,
}

impl RootAdmissionWebhookClient {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            default_client: build_default_webhook_http_client().map_err(|error| error.to_string()),
            ca_bundle_clients: Mutex::new(CaBundleClientCache::new(
                CA_BUNDLE_CLIENT_CACHE_CAPACITY,
            )),
        })
    }

    fn client_for(
        &self,
        client_config: &Value,
        dns_override: Option<(&str, SocketAddr)>,
    ) -> Result<reqwest::Client> {
        if dns_override.is_some() {
            return build_webhook_http_client(client_config, dns_override);
        }
        if client_config
            .get("caBundle")
            .and_then(Value::as_str)
            .is_some_and(|bundle| !bundle.is_empty())
        {
            return lock_ca_bundle_cache(&self.ca_bundle_clients)?.client_for(client_config);
        }
        self.default_client
            .as_ref()
            .cloned()
            .map_err(|error| anyhow!("Failed to build webhook HTTP client: {error}"))
    }
}

#[async_trait::async_trait]
impl AdmissionWebhookClient for RootAdmissionWebhookClient {
    async fn call(
        &self,
        request: AdmissionWebhookRequest,
    ) -> std::result::Result<Value, AdmissionDependencyError> {
        let url = add_timeout_query(&request.target.base_url, request.timeout_seconds)
            .map_err(dependency_error)?;
        let dns_override = request
            .target
            .dns_override
            .as_ref()
            .map(|(host, address)| (host.as_str(), *address));
        let client = self
            .client_for(&request.client_config, dns_override)
            .map_err(dependency_error)?;
        let response = client
            .post(&url)
            .timeout(Duration::from_secs(request.timeout_seconds))
            .json(&request.admission_review)
            .send()
            .await
            .map_err(|error| {
                AdmissionDependencyError::new(
                    k8s_native_service::admission::format_webhook_call_error(
                        &url,
                        &error.to_string(),
                        error.is_timeout(),
                    ),
                )
            })?;
        if !response.status().is_success() {
            return Err(AdmissionDependencyError::new(format!(
                "Webhook returned status {}",
                response.status()
            )));
        }
        response
            .json()
            .await
            .map_err(|error| dependency_error(format!("Failed to parse webhook response: {error}")))
    }
}

pub(crate) fn add_timeout_query(base_url: &str, timeout_seconds: u64) -> Result<String> {
    let mut parsed = reqwest::Url::parse(base_url)
        .with_context(|| format!("Invalid webhook URL: {base_url}"))?;
    parsed
        .query_pairs_mut()
        .append_pair("timeout", &format!("{timeout_seconds}s"));
    Ok(parsed.to_string())
}

fn build_default_webhook_http_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(10))
        .build()
        .context("Failed to build webhook HTTP client")
}

pub(crate) struct CaBundleClientCache {
    capacity: usize,
    clients: HashMap<CaFingerprint, Arc<reqwest::Client>>,
    recency: VecDeque<CaFingerprint>,
}

impl CaBundleClientCache {
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            clients: HashMap::new(),
            recency: VecDeque::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(capacity: usize) -> Self {
        Self::new(capacity)
    }

    pub(crate) fn client_for(&mut self, client_config: &Value) -> Result<reqwest::Client> {
        let fingerprint = ca_bundle_fingerprint(client_config)?;
        if let Some(client) = self.clients.get(&fingerprint).cloned() {
            self.touch(fingerprint);
            return Ok(client.as_ref().clone());
        }
        let client = Arc::new(build_webhook_http_client(client_config, None)?);
        self.insert(fingerprint, Arc::clone(&client));
        Ok(client.as_ref().clone())
    }

    fn insert(&mut self, fingerprint: CaFingerprint, client: Arc<reqwest::Client>) {
        if self.clients.insert(fingerprint, client).is_some() {
            self.touch(fingerprint);
            return;
        }
        self.recency.push_back(fingerprint);
        while self.clients.len() > self.capacity {
            let Some(oldest) = self.recency.pop_front() else {
                break;
            };
            if oldest != fingerprint && self.clients.remove(&oldest).is_some() {
                break;
            }
        }
    }

    fn touch(&mut self, fingerprint: CaFingerprint) {
        self.recency.retain(|existing| *existing != fingerprint);
        self.recency.push_back(fingerprint);
    }

    #[cfg(test)]
    pub(crate) fn len_for_test(&self) -> usize {
        self.clients.len()
    }

    #[cfg(test)]
    pub(crate) fn contains_for_test(&self, fingerprint: &CaFingerprint) -> bool {
        self.clients.contains_key(fingerprint)
    }
}

pub(crate) fn ca_bundle_fingerprint(client_config: &Value) -> Result<CaFingerprint> {
    let ca_bundle = client_config
        .get("caBundle")
        .and_then(Value::as_str)
        .filter(|bundle| !bundle.is_empty())
        .ok_or_else(|| anyhow!("clientConfig.caBundle is required for CA bundle client cache"))?;
    use base64::Engine as _;
    let ca_bytes = base64::engine::general_purpose::STANDARD
        .decode(ca_bundle)
        .context("Invalid base64 in clientConfig.caBundle")?;
    let digest = Sha256::digest(ca_bytes);
    let mut fingerprint = [0_u8; 32];
    fingerprint.copy_from_slice(digest.as_ref());
    Ok(fingerprint)
}

fn lock_ca_bundle_cache(
    cache: &Mutex<CaBundleClientCache>,
) -> Result<std::sync::MutexGuard<'_, CaBundleClientCache>> {
    cache
        .lock()
        .map_err(|_| anyhow!("caBundle client cache poisoned"))
}

#[cfg(test)]
pub(crate) fn lock_ca_bundle_cache_for_test(
    cache: &Mutex<CaBundleClientCache>,
) -> Result<std::sync::MutexGuard<'_, CaBundleClientCache>> {
    lock_ca_bundle_cache(cache)
}

pub(crate) fn build_webhook_http_client(
    client_config: &Value,
    dns_override: Option<(&str, SocketAddr)>,
) -> Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(10));
    if let Some(ca_bundle) = client_config.get("caBundle").and_then(Value::as_str)
        && !ca_bundle.is_empty()
    {
        use base64::Engine as _;
        let ca_der = base64::engine::general_purpose::STANDARD
            .decode(ca_bundle)
            .context("Invalid base64 in clientConfig.caBundle")?;
        let certificate = reqwest::Certificate::from_der(&ca_der)
            .or_else(|_| reqwest::Certificate::from_pem(&ca_der))
            .context("Invalid certificate in clientConfig.caBundle")?;
        builder = builder.add_root_certificate(certificate);
    }
    if let Some((host, address)) = dns_override {
        builder = builder.resolve(host, address);
    }
    builder
        .build()
        .context("Failed to build webhook HTTP client")
}

pub(crate) struct ResourceAdmissionAdapter {
    identity: Arc<dyn k8s_native_service::ApiIdentityGenerator>,
    query: Arc<dyn AdmissionQuery>,
    target_resolver: Arc<dyn WebhookTargetResolver>,
    webhook_client: Arc<dyn AdmissionWebhookClient>,
}

impl ResourceAdmissionAdapter {
    pub(crate) fn new(
        identity: Arc<dyn k8s_native_service::ApiIdentityGenerator>,
        db: DatastoreHandle,
    ) -> Arc<Self> {
        let query: Arc<dyn AdmissionQuery> = RootAdmissionQuery::new(db);
        let target_resolver: Arc<dyn WebhookTargetResolver> =
            RootWebhookTargetResolver::new(Arc::clone(&query));
        let webhook_client: Arc<dyn AdmissionWebhookClient> = RootAdmissionWebhookClient::new();
        Arc::new(Self {
            identity,
            query,
            target_resolver,
            webhook_client,
        })
    }
}

impl ResourceAdmissionAdapter {
    fn execute_admission<'a>(
        &'a self,
        mut context: k8s_native_service::admission::AdmissionRequestContext,
    ) -> k8s_native_service::generic_command::GenericCommandFuture<'a, Value> {
        Box::pin(async move {
            let engine = AdmissionEngine::new(
                self.identity.as_ref(),
                self.query.as_ref(),
                self.target_resolver.as_ref(),
                self.webhook_client.as_ref(),
            );
            let admitted = engine
                .run_with_context(&context, true)
                .await
                .map_err(crate::api::map_mutating_admission_error)?;
            context.object = admitted.clone();
            engine
                .run_with_context(&context, false)
                .await
                .map_err(crate::api::map_validating_admission_error)?;
            Ok(admitted)
        })
    }
}

impl k8s_native_service::generic_command::ResourceAdmissionPort for ResourceAdmissionAdapter {
    fn admit(
        &self,
        request: k8s_native_service::generic_command::ResourceAdmissionRequest,
    ) -> k8s_native_service::generic_command::GenericCommandFuture<'_, Value> {
        let mut context =
            crate::api::build_admission_context(crate::api::AdmissionContextRequest {
                api_version: &request.api_version,
                kind: &request.kind,
                operation: &request.operation,
                namespace: request.namespace,
                name: request.name,
                object: request.object,
                old_object: request.old_object,
                dry_run: request.dry_run,
                subresource: request.subresource.as_deref(),
                options: request.options,
            });
        if let Some(resource) = request.resource {
            context.resource = resource;
        }
        self.execute_admission(context)
    }
}
