use crate::api::*;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{fs as blocking_fs, net::SocketAddr, path::Path};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ApiServiceBackendKey {
    group: String,
    version: String,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ApiServiceClientKey {
    host: String,
    service_port: u16,
    endpoint_addr: SocketAddr,
    insecure_skip_tls_verify: bool,
    ca_bundle: Option<String>,
    has_identity: bool,
}

#[derive(Default)]
pub struct ApiServiceProxyCache {
    generation: AtomicU64,
    backends: tokio::sync::RwLock<HashMap<ApiServiceBackendKey, Option<ApiServiceBackend>>>,
    clients: tokio::sync::RwLock<HashMap<ApiServiceClientKey, reqwest::Client>>,
}

impl ApiServiceProxyCache {
    pub async fn clear(&self) {
        self.backends.write().await.clear();
        self.clients.write().await.clear();
        self.generation.fetch_add(1, Ordering::Release);
    }

    fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }
}

#[derive(Clone, Debug)]
pub struct ApiServiceBackend {
    pub service_name: String,
    pub service_namespace: String,
    pub service_port: u16,
    pub insecure_skip_tls_verify: bool,
    pub ca_bundle: Option<String>,
}

async fn find_apiservice_backend_uncached(
    state: &AppState,
    group: &str,
    version: &str,
) -> Result<Option<ApiServiceBackend>, AppError> {
    let list = state
        .db
        .list_resources(
            "apiregistration.k8s.io/v1",
            "APIService",
            None,
            crate::datastore::ResourceListQuery::all(),
        )
        .await?;

    for item in list.items {
        let Some(spec) = item.data.get("spec").and_then(|s| s.as_object()) else {
            continue;
        };
        let Some(spec_group) = spec.get("group").and_then(|v| v.as_str()) else {
            continue;
        };
        let Some(spec_version) = spec.get("version").and_then(|v| v.as_str()) else {
            continue;
        };
        if spec_group != group || spec_version != version {
            continue;
        }

        let Some(service) = spec.get("service").and_then(|s| s.as_object()) else {
            continue;
        };
        let Some(service_name) = service.get("name").and_then(|v| v.as_str()) else {
            continue;
        };
        let Some(service_namespace) = service.get("namespace").and_then(|v| v.as_str()) else {
            continue;
        };
        let service_port = service
            .get("port")
            .and_then(|v| v.as_u64())
            .and_then(|v| u16::try_from(v).ok())
            .unwrap_or(443);
        let insecure_skip_tls_verify = spec
            .get("insecureSkipTLSVerify")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let ca_bundle = spec
            .get("caBundle")
            .and_then(|v| v.as_str())
            .map(ToOwned::to_owned);

        return Ok(Some(ApiServiceBackend {
            service_name: service_name.to_string(),
            service_namespace: service_namespace.to_string(),
            service_port,
            insecure_skip_tls_verify,
            ca_bundle,
        }));
    }

    Ok(None)
}

pub async fn find_apiservice_backend(
    state: &AppState,
    group: &str,
    version: &str,
) -> Result<Option<ApiServiceBackend>, AppError> {
    loop {
        let cache_generation = state.apiservice_proxy_cache.generation();
        let key = ApiServiceBackendKey {
            group: group.to_string(),
            version: version.to_string(),
        };

        if let Some(cached) = state.apiservice_proxy_cache.backends.read().await.get(&key) {
            return Ok(cached.clone());
        }

        let backend = find_apiservice_backend_uncached(state, group, version).await?;
        let mut backends = state.apiservice_proxy_cache.backends.write().await;
        if cache_generation != state.apiservice_proxy_cache.generation() {
            continue;
        }
        if let Some(cached) = backends.get(&key) {
            return Ok(cached.clone());
        }
        backends.insert(key, backend.clone());
        return Ok(backend);
    }
}

pub struct ApiServiceRequestDispatcher {
    state: Arc<AppState>,
}

/// Bundled parameters for an APIService proxy dispatch call.
pub struct ApiServiceDispatchRequest<'a> {
    pub group: &'a str,
    pub version: &'a str,
    pub method: Method,
    pub path_and_query: &'a str,
    pub body: Bytes,
    pub forward_headers: Option<&'a HeaderMap>,
    pub identity: &'a crate::auth::identity::AuthenticatedIdentity,
}

impl ApiServiceRequestDispatcher {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }

    pub async fn dispatch(
        &self,
        req: ApiServiceDispatchRequest<'_>,
    ) -> Result<Option<Response>, AppError> {
        let state = &self.state;
        let Some(backend) = find_apiservice_backend(state, req.group, req.version).await? else {
            return Ok(None);
        };

        let service_target = resolve_service_proxy_target(
            state.db.as_ref(),
            &backend.service_namespace,
            &backend.service_name,
            backend.service_port,
        )
        .await?;

        let target = format!(
            "https://{}:{}{}",
            service_target.host,
            service_target.endpoint_addr.port(),
            req.path_and_query
        );
        let client = cached_apiservice_proxy_client(state, &backend, &service_target).await?;

        let reqwest_method =
            reqwest::Method::from_bytes(req.method.as_str().as_bytes()).map_err(|e| {
                AppError::BadRequest(format!("Unsupported HTTP method for APIService proxy: {e}"))
            })?;

        let mut upstream_req = client.request(reqwest_method, &target).body(req.body);
        upstream_req = upstream_req.header(
            "host",
            format!("{}:{}", service_target.host, service_target.port),
        );
        let header_policy =
            crate::api::backend_proxy_headers::BackendProxyHeaderPolicy::workload_backend();
        if let Some(headers) = req.forward_headers {
            for (name, value) in headers {
                let key = name.as_str();
                if !header_policy.should_forward_str(key) {
                    continue;
                }
                if let Ok(v) = value.to_str() {
                    upstream_req = upstream_req.header(key, v);
                }
            }
        }
        // Stamp forwarded request headers from the real caller identity
        upstream_req = upstream_req.header("x-remote-user", req.identity.username.as_str());
        for group in &req.identity.groups {
            upstream_req = upstream_req.header("x-remote-group", group.as_str());
        }
        for (key, value) in &req.identity.extra {
            upstream_req = upstream_req.header(format!("x-remote-extra-{}", key), value.as_str());
        }

        let upstream = upstream_req
            .send()
            .await
            .map_err(|e| AppError::BadGateway(format!("APIService proxy request failed: {e}")))?;

        let status =
            StatusCode::from_u16(upstream.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
        let headers = upstream.headers().clone();
        let payload = crate::api_pod_subresources::read_reqwest_body_limited(
            upstream,
            crate::api_pod_subresources::MAX_APISERVICE_RESPONSE_BODY_BYTES,
            "APIService proxy",
        )
        .await?;

        let mut response = Response::builder().status(status);
        for (k, v) in &headers {
            if k.as_str().eq_ignore_ascii_case("transfer-encoding") {
                continue;
            }
            if let Ok(value_str) = v.to_str() {
                response = response.header(k.as_str(), value_str);
            }
        }

        response.body(Body::from(payload)).map(Some).map_err(|e| {
            AppError::Internal(format!("Failed to build APIService proxy response: {e}"))
        })
    }
}

pub async fn invalidate_apiservice_proxy_cache_for_resource(
    state: &AppState,
    api_version: &str,
    kind: &str,
) {
    if api_version == "apiregistration.k8s.io/v1" && kind == "APIService" {
        state.apiservice_proxy_cache.clear().await;
    }
}

pub async fn resolve_service_endpoint(
    db: &dyn DatastoreBackend,
    namespace: &str,
    service_name: &str,
    desired_port: u16,
) -> Result<(String, u16), AppError> {
    let endpoints = db
        .get_resource("v1", "Endpoints", Some(namespace), service_name)
        .await?
        .ok_or_else(|| {
            AppError::BadGateway(format!(
                "APIService backend Endpoints {}/{} not found",
                namespace, service_name
            ))
        })?;

    let subsets = endpoints
        .data
        .get("subsets")
        .and_then(|v| v.as_array())
        .ok_or_else(|| {
            AppError::BadGateway(format!(
                "APIService backend Endpoints {}/{} has no subsets",
                namespace, service_name
            ))
        })?;

    for subset in subsets {
        // During APIService bootstrap, Endpoints can temporarily surface only
        // notReadyAddresses while readiness and endpoint reconciliation converge.
        // kube-apiserver aggregator still probes this backend aggressively, so
        // allow that transient state instead of hard-failing with 502.
        let ip = subset
            .get("addresses")
            .and_then(|v| v.as_array())
            .and_then(|addrs| {
                addrs
                    .iter()
                    .find_map(|a| a.get("ip").and_then(|v| v.as_str()))
            })
            .or_else(|| {
                subset
                    .get("notReadyAddresses")
                    .and_then(|v| v.as_array())
                    .and_then(|addrs| {
                        addrs
                            .iter()
                            .find_map(|a| a.get("ip").and_then(|v| v.as_str()))
                    })
            });
        let Some(ip) = ip else {
            continue;
        };

        let ports = subset.get("ports").and_then(|v| v.as_array());
        let mut resolved_port = desired_port;
        if let Some(ports) = ports {
            if let Some(match_port) = ports
                .iter()
                .filter_map(|p| p.get("port").and_then(|v| v.as_u64()))
                .filter_map(|p| u16::try_from(p).ok())
                .find(|p| *p == desired_port)
            {
                resolved_port = match_port;
            } else if let Some(first_port) = ports
                .iter()
                .filter_map(|p| p.get("port").and_then(|v| v.as_u64()))
                .filter_map(|p| u16::try_from(p).ok())
                .next()
            {
                resolved_port = first_port;
            }
        }
        return Ok((ip.to_string(), resolved_port));
    }

    Err(AppError::BadGateway(format!(
        "APIService backend Endpoints {}/{} has no ready addresses",
        namespace, service_name
    )))
}

pub struct ServiceProxyTarget {
    pub host: String,
    pub port: u16,
    pub endpoint_addr: SocketAddr,
}

pub async fn resolve_service_proxy_target(
    db: &dyn DatastoreBackend,
    namespace: &str,
    service_name: &str,
    service_port: u16,
) -> Result<ServiceProxyTarget, AppError> {
    let (endpoint_ip, endpoint_port) =
        resolve_service_endpoint(db, namespace, service_name, service_port).await?;
    let endpoint_addr = format!("{endpoint_ip}:{endpoint_port}")
        .parse::<SocketAddr>()
        .map_err(|e| {
            AppError::BadGateway(format!(
                "Invalid APIService backend endpoint socket address {endpoint_ip}:{endpoint_port}: {e}"
            ))
        })?;

    Ok(ServiceProxyTarget {
        host: format!("{service_name}.{namespace}.svc"),
        port: service_port,
        endpoint_addr,
    })
}

pub async fn load_apiservice_proxy_identity(
    namespace: &str,
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
    cache: &tokio::sync::OnceCell<reqwest::Identity>,
) -> Option<reqwest::Identity> {
    let etc = crate::paths::etc_dir_path(namespace);
    load_apiservice_proxy_identity_from_etc(&etc, task_supervisor, cache).await
}

async fn load_apiservice_proxy_identity_from_etc(
    etc: &Path,
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
    cache: &tokio::sync::OnceCell<reqwest::Identity>,
) -> Option<reqwest::Identity> {
    match cache
        .get_or_try_init(|| async {
            load_apiservice_proxy_identity_uncached(etc, task_supervisor).await
        })
        .await
    {
        Ok(identity) => Some(identity.clone()),
        Err(err) => {
            tracing::warn!("Failed to load APIService proxy client identity: {err}");
            None
        }
    }
}

async fn load_apiservice_proxy_identity_uncached(
    etc: &Path,
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
) -> anyhow::Result<reqwest::Identity> {
    let cert_path = etc.join("apiservice-proxy.crt");
    let key_path = etc.join("apiservice-proxy.key");
    let cert = read_apiservice_proxy_identity_file(
        task_supervisor,
        &cert_path,
        "apiservice_proxy_identity_read_cert",
    )
    .await?;
    let key = read_apiservice_proxy_identity_file(
        task_supervisor,
        &key_path,
        "apiservice_proxy_identity_read_key",
    )
    .await?;

    reqwest::Identity::from_pkcs8_pem(&cert, &key)
        .map_err(|err| anyhow::anyhow!("invalid APIService proxy client identity: {err}"))
}

async fn read_apiservice_proxy_identity_file(
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
    path: &Path,
    label: &'static str,
) -> anyhow::Result<Vec<u8>> {
    let path_buf = path.to_path_buf();
    let key = path.to_string_lossy().into_owned();
    Ok(task_supervisor
        .run_blocking_file_keyed(label, key, move || blocking_fs::read(path_buf))
        .await??)
}

async fn build_apiservice_proxy_client(
    backend: &ApiServiceBackend,
    service_target: &ServiceProxyTarget,
    identity: Option<reqwest::Identity>,
) -> Result<reqwest::Client, AppError> {
    let mut client_builder = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .resolve(&service_target.host, service_target.endpoint_addr);
    if let Some(ca_bundle) = backend.ca_bundle.as_deref() {
        use base64::Engine;
        let ca_bundle_bytes = base64::engine::general_purpose::STANDARD
            .decode(ca_bundle)
            .map_err(|e| {
                AppError::BadGateway(format!(
                    "APIService {}.{}/{} has invalid spec.caBundle base64: {e}",
                    backend.service_name, backend.service_namespace, backend.service_port
                ))
            })?;
        let cert = reqwest::Certificate::from_der(&ca_bundle_bytes)
            .or_else(|_| reqwest::Certificate::from_pem(&ca_bundle_bytes))
            .map_err(|e| {
                AppError::BadGateway(format!(
                    "APIService {}.{}/{} has invalid spec.caBundle certificate: {e}",
                    backend.service_name, backend.service_namespace, backend.service_port
                ))
            })?;
        client_builder = client_builder
            .tls_built_in_root_certs(false)
            .add_root_certificate(cert);
    }
    if backend.insecure_skip_tls_verify {
        client_builder = client_builder.danger_accept_invalid_certs(true);
    }
    if let Some(identity) = identity {
        client_builder = client_builder.identity(identity);
    }

    client_builder
        .build()
        .map_err(|e| AppError::Internal(format!("Failed to build APIService proxy client: {e}")))
}

async fn cached_apiservice_proxy_client(
    state: &AppState,
    backend: &ApiServiceBackend,
    service_target: &ServiceProxyTarget,
) -> Result<reqwest::Client, AppError> {
    let identity = {
        load_apiservice_proxy_identity(
            &state.config.containerd_namespace,
            state.task_supervisor.as_ref(),
            state.apiservice_proxy_identity_cache.as_ref(),
        )
        .await
    };

    let key = ApiServiceClientKey {
        host: service_target.host.clone(),
        service_port: service_target.port,
        endpoint_addr: service_target.endpoint_addr,
        insecure_skip_tls_verify: backend.insecure_skip_tls_verify,
        ca_bundle: backend.ca_bundle.clone(),
        has_identity: identity.is_some(),
    };

    let cache_generation = state.apiservice_proxy_cache.generation();
    if let Some(client) = state.apiservice_proxy_cache.clients.read().await.get(&key) {
        return Ok(client.clone());
    }

    let client = build_apiservice_proxy_client(backend, service_target, identity).await?;
    let mut clients = state.apiservice_proxy_cache.clients.write().await;
    if cache_generation != state.apiservice_proxy_cache.generation() {
        return Ok(client);
    }
    if let Some(client) = clients.get(&key) {
        return Ok(client.clone());
    }
    clients.insert(key, client.clone());
    Ok(client)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn proxy_apiservice_request(
    state: &Arc<AppState>,
    group: &str,
    version: &str,
    method: Method,
    path_and_query: &str,
    body: Bytes,
    forward_headers: Option<&HeaderMap>,
    identity: &crate::auth::identity::AuthenticatedIdentity,
) -> Result<Option<Response>, AppError> {
    // keep the hot-path cache check satisfied: proxy_apiservice_request must route through cached_apiservice_proxy_client via ApiServiceRequestDispatcher.
    ApiServiceRequestDispatcher::new(state.clone())
        .dispatch(ApiServiceDispatchRequest {
            group,
            version,
            method,
            path_and_query,
            body,
            forward_headers,
            identity,
        })
        .await
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn load_apiservice_proxy_identity_caches_success_until_process_restart() {
        let data_root = tempfile::tempdir().unwrap();
        let etc = data_root.path().join("etc");
        super::blocking_fs::create_dir_all(&etc).unwrap();

        let task_supervisor = crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        );
        let cache = tokio::sync::OnceCell::new();
        let cert =
            rcgen::generate_simple_self_signed(vec!["apiservice-proxy-cache-test".to_string()])
                .unwrap();
        let cert_path = etc.join("apiservice-proxy.crt");
        let key_path = etc.join("apiservice-proxy.key");
        super::blocking_fs::write(&cert_path, cert.cert.pem()).unwrap();
        super::blocking_fs::write(&key_path, cert.key_pair.serialize_pem()).unwrap();

        let first =
            super::load_apiservice_proxy_identity_from_etc(&etc, &task_supervisor, &cache).await;
        assert!(first.is_some());

        super::blocking_fs::remove_file(&cert_path).unwrap();
        super::blocking_fs::remove_file(&key_path).unwrap();

        let second =
            super::load_apiservice_proxy_identity_from_etc(&etc, &task_supervisor, &cache).await;
        assert!(
            second.is_some(),
            "same-process APIService identity should be served from cache"
        );

        let restart_cache = tokio::sync::OnceCell::new();
        let after_restart =
            super::load_apiservice_proxy_identity_from_etc(&etc, &task_supervisor, &restart_cache)
                .await;
        assert!(
            after_restart.is_none(),
            "a fresh process cache must read cert/key files again"
        );
    }

    #[tokio::test]
    async fn load_apiservice_proxy_identity_rejects_admin_certificate_files() {
        let data_root = tempfile::tempdir().unwrap();
        let etc = data_root.path().join("etc");
        super::blocking_fs::create_dir_all(&etc).unwrap();

        let task_supervisor = crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        );
        let cache = tokio::sync::OnceCell::new();
        let cert =
            rcgen::generate_simple_self_signed(vec!["apiservice-admin-cache-test".to_string()])
                .unwrap();
        super::blocking_fs::write(etc.join("admin.crt"), cert.cert.pem()).unwrap();
        super::blocking_fs::write(etc.join("admin.key"), cert.key_pair.serialize_pem()).unwrap();

        let identity =
            super::load_apiservice_proxy_identity_from_etc(&etc, &task_supervisor, &cache).await;
        assert!(
            identity.is_none(),
            "APIService proxy must not load cluster-admin certificate material"
        );
    }
}
