use super::*;

/// Maximum size for proxied request bodies (pod/service/APIService proxy).
/// Requests exceeding this return 413 RequestEntityTooLarge.
pub const MAX_PROXY_REQUEST_BODY_BYTES: usize = 16 * 1024 * 1024; // 16 MiB

/// Maximum size for proxied response bodies that must be buffered.
/// Responses exceeding this return 502 BadGateway.
pub const MAX_PROXY_RESPONSE_BODY_BYTES: usize = 32 * 1024 * 1024; // 32 MiB

/// Maximum size for APIService aggregated backend response bodies.
pub const MAX_APISERVICE_RESPONSE_BODY_BYTES: usize = 32 * 1024 * 1024; // 32 MiB

const POD_PROXY_UPSTREAM_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
const SERVICE_PROXY_UPSTREAM_REQUEST_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(10);

/// Round-robin cursor for service-proxy endpoint selection. Kubernetes picks
/// a *random* ready endpoint per service-proxy request; a rotating cursor
/// gives the same even spread without an RNG and—paired with the failover
/// loop in `service_proxy_inner`—stops a single slow/unreachable endpoint
/// (e.g. a cross-node pod the dataplane is briefly black-holing) from
/// capturing every request and failing the whole call.
static SERVICE_PROXY_ENDPOINT_CURSOR: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Rebuild an owned proxy request from buffered parts so it can be replayed
/// against another endpoint on failover. The client URI/headers are
/// preserved verbatim; only the target endpoint (passed separately as a URL)
/// changes between attempts.
fn rebuild_proxy_request(
    method: axum::http::Method,
    uri: axum::http::Uri,
    headers: axum::http::HeaderMap,
    body: bytes::Bytes,
) -> Request {
    let mut builder = axum::http::Request::builder().method(method).uri(uri);
    if let Some(dst) = builder.headers_mut() {
        *dst = headers;
    }
    builder
        .body(axum::body::Body::from(body))
        .expect("rebuild proxy request from validated parts")
}

/// Read a reqwest response body into a `Bytes` buffer, capping at `limit`.
/// Returns `BadGateway` as soon as the cumulative length exceeds the limit,
/// without consuming the rest of the upstream body. This mirrors the bounded
/// `axum::body::to_bytes` path used for HTTP proxying.
pub async fn read_reqwest_body_limited(
    response: reqwest::Response,
    limit: usize,
    context: &str,
) -> Result<bytes::Bytes, AppError> {
    use bytes::BytesMut;
    use futures::StreamExt;

    let mut stream = response.bytes_stream();
    let mut buf = BytesMut::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk
            .map_err(|e| AppError::BadGateway(format!("{context}: read chunk failed: {e}")))?;
        if buf.len() + chunk.len() > limit {
            return Err(AppError::BadGateway(format!(
                "{context}: response body exceeds limit of {limit} bytes"
            )));
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(buf.freeze())
}

#[derive(Debug, Deserialize)]
pub struct ProxyQuery {
    pub port: Option<u16>,
}

pub fn maybe_redirect_proxy_root(req: &Request, proxy_path: &str) -> Option<Response> {
    if !proxy_path.is_empty() {
        return None;
    }
    if req.method() != axum::http::Method::GET && req.method() != axum::http::Method::HEAD {
        return None;
    }

    let raw_path = req.uri().path();
    if !raw_path.ends_with("/proxy") {
        return None;
    }

    let canonical_path = if raw_path.starts_with("/namespaces/") || raw_path.starts_with("/nodes/")
    {
        format!("/api/v1{raw_path}")
    } else {
        raw_path.to_string()
    };

    let mut location = format!("{canonical_path}/");
    if let Some(query) = req.uri().query() {
        location.push('?');
        location.push_str(query);
    }

    Response::builder()
        .status(StatusCode::MOVED_PERMANENTLY)
        .header(header::LOCATION, location)
        .body(axum::body::Body::empty())
        .ok()
}

#[derive(Debug, Clone, Copy)]
pub struct ProxyNamePort<'a> {
    pub scheme: Option<&'a str>,
    pub name: &'a str,
    pub port_num: Option<u16>,
    pub port_name: Option<&'a str>,
}

pub fn parse_proxy_name_port(name_param: &str) -> ProxyNamePort<'_> {
    let (scheme, rest) = if let Some(rest) = name_param.strip_prefix("http:") {
        (Some("http"), rest)
    } else if let Some(rest) = name_param.strip_prefix("https:") {
        (Some("https"), rest)
    } else {
        (None, name_param)
    };

    if let Some(idx) = rest.rfind(':') {
        let base = &rest[..idx];
        let suffix = &rest[idx + 1..];
        if !base.is_empty() && !suffix.is_empty() {
            if let Ok(port) = suffix.parse::<u16>() {
                return ProxyNamePort {
                    scheme,
                    name: base,
                    port_num: Some(port),
                    port_name: None,
                };
            }
            return ProxyNamePort {
                scheme,
                name: base,
                port_num: None,
                port_name: Some(suffix),
            };
        }
    }
    ProxyNamePort {
        scheme,
        name: rest,
        port_num: None,
        port_name: None,
    }
}

pub fn should_allow_pod_proxy_default_port_fallback(
    port_override: Option<u16>,
    parsed: ProxyNamePort<'_>,
    resolved_port: u16,
) -> bool {
    // K8s pod proxy should not silently retarget explicitly-selected or
    // discovered non-default pod ports (e.g. :9376) to 8080. Only allow the
    // legacy fallback when the request resolved to the plain default HTTP port.
    port_override.is_none()
        && parsed.port_num.is_none()
        && parsed.port_name.is_none()
        && resolved_port == 80
}

fn should_set_proxy_content_length(method: &axum::http::Method, body_len: usize) -> bool {
    body_len > 0
        || *method == axum::http::Method::POST
        || *method == axum::http::Method::PUT
        || *method == axum::http::Method::PATCH
}

fn should_retry_service_proxy_transient_failure(method: &axum::http::Method) -> bool {
    *method == axum::http::Method::GET
        || *method == axum::http::Method::HEAD
        || *method == axum::http::Method::OPTIONS
}

/// Resolve the `(max_attempts, retry_delay)` for proxying an upstream request.
///
/// A transient upstream failure (connection refused / not-ready) is only safe
/// to retry for idempotent methods; retrying a slow-but-successful mutating
/// request would duplicate it on the target. Shared by the pod-proxy and
/// service-proxy readiness-retry paths so both gate retries identically.
fn proxy_retry_policy(method: &axum::http::Method) -> (usize, std::time::Duration) {
    if should_retry_service_proxy_transient_failure(method) {
        (8, std::time::Duration::from_millis(250))
    } else {
        (1, std::time::Duration::ZERO)
    }
}

/// GET/POST/PUT/DELETE/PATCH /api/v1/namespaces/{ns}/pods/{name}/proxy
pub async fn pod_proxy(
    State(state): State<Arc<AppState>>,
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<ProxyQuery>,
    req: Request,
) -> Result<Response, AppError> {
    pod_proxy_inner(state, &namespace, &name, "", query.port, req).await
}

/// GET/POST/PUT/DELETE/PATCH /api/v1/namespaces/{ns}/pods/{name}/proxy/{*path}
pub async fn pod_proxy_with_path(
    State(state): State<Arc<AppState>>,
    Path((namespace, name, proxy_path)): Path<(String, String, String)>,
    Query(query): Query<ProxyQuery>,
    req: Request,
) -> Result<Response, AppError> {
    pod_proxy_inner(state, &namespace, &name, &proxy_path, query.port, req).await
}

pub async fn pod_proxy_inner(
    state: Arc<AppState>,
    namespace: &str,
    name_param: &str,
    proxy_path: &str,
    port_override: Option<u16>,
    req: Request,
) -> Result<Response, AppError> {
    if let Some(resp) = maybe_redirect_proxy_root(&req, proxy_path) {
        return Ok(resp);
    }

    // K8s proxy URL may include a port suffix: /pods/{name}:{port|portName}/proxy
    let parsed = parse_proxy_name_port(name_param);
    let scheme = parsed.scheme.unwrap_or("http");
    let name = parsed.name;
    let effective_port_override = port_override.or(parsed.port_num);

    // Look up the pod
    let pod = crate::kubelet::pod_repository::PodReader::get_pod(
        state.pod_repository.as_ref(),
        namespace,
        name,
    )
    .await?
    .ok_or_else(|| AppError::NotFound(format!("Pod {}/{} not found", namespace, name_param)))?;

    // Get pod IP
    let pod_ip = pod
        .data
        .get("status")
        .and_then(|s| s.get("podIP"))
        .and_then(|ip| ip.as_str())
        .ok_or_else(|| {
            AppError::BadRequest(format!(
                "Pod {}/{} has no IP address (not running?)",
                namespace, name
            ))
        })?;

    // Determine target port
    let port = if let Some(p) = effective_port_override {
        p
    } else if let Some(named) = parsed.port_name {
        pod.data
            .get("spec")
            .and_then(|s| s.get("containers"))
            .and_then(|c| c.as_array())
            .and_then(|containers| {
                containers.iter().find_map(|c| {
                    c.get("ports").and_then(|p| p.as_array()).and_then(|ports| {
                        ports.iter().find_map(|port| {
                            if port.get("name").and_then(|n| n.as_str()) == Some(named) {
                                port.get("containerPort")
                                    .and_then(|cp| cp.as_u64())
                                    .map(|p| p as u16)
                            } else {
                                None
                            }
                        })
                    })
                })
            })
            .ok_or_else(|| {
                AppError::BadRequest(format!(
                    "Pod {}/{} has no container port named {}",
                    namespace, name, named
                ))
            })?
    } else {
        // Default: use first containerPort from spec, or 80.
        let discovered_port = pod
            .data
            .get("spec")
            .and_then(|s| s.get("containers"))
            .and_then(|c| c.as_array())
            .and_then(|arr| arr.first())
            .and_then(|c| c.get("ports"))
            .and_then(|p| p.as_array())
            .and_then(|arr| arr.first())
            .and_then(|p| p.get("containerPort"))
            .and_then(|cp| cp.as_u64())
            .map(|p| p as u16);
        discovered_port.unwrap_or(80)
    };
    let allow_default_port_fallback =
        should_allow_pod_proxy_default_port_fallback(port_override, parsed, port);

    // Build target URL
    let target_url = if proxy_path.is_empty() {
        format!("{}://{}:{}/", scheme, pod_ip, port)
    } else {
        format!("{}://{}:{}/{}", scheme, pod_ip, port, proxy_path)
    };

    tracing::debug!(
        "pods/proxy: {}/{} -> {} (method={})",
        namespace,
        name,
        target_url,
        req.method()
    );

    pod_proxy_request_with_readiness_retries(
        req,
        &target_url,
        allow_default_port_fallback && port != 8080,
        state.task_supervisor.clone(),
    )
    .await
}

async fn pod_proxy_request_with_readiness_retries(
    req: Request,
    target_url: &str,
    allow_fallback_8080: bool,
    task_supervisor: std::sync::Arc<crate::task_supervisor::TaskSupervisor>,
) -> Result<Response, AppError> {
    // Gate readiness retries to idempotent methods, like the service-proxy path.
    // The route accepts POST/PUT/PATCH/DELETE; retrying a slow-but-successful
    // mutating request would duplicate it. The within-attempt 8080 port fallback
    // is unaffected (it only fires on a connection-level failure).
    let (max_attempts, retry_delay) = proxy_retry_policy(req.method());
    proxy_request_with_fallback_port_and_retries(
        req,
        target_url,
        allow_fallback_8080,
        8080,
        max_attempts,
        retry_delay,
        task_supervisor,
    )
    .await
}

async fn service_proxy_request_with_readiness_retries(
    req: Request,
    target_url: &str,
    task_supervisor: std::sync::Arc<crate::task_supervisor::TaskSupervisor>,
) -> Result<Response, AppError> {
    let (max_attempts, retry_delay) = proxy_retry_policy(req.method());

    proxy_request_with_fallback_port_and_retries_with_options(
        req,
        target_url,
        ProxyRequestOptions {
            allow_fallback: false,
            fallback_port: 8080,
            max_attempts,
            retry_delay,
            upstream_request_timeout: SERVICE_PROXY_UPSTREAM_REQUEST_TIMEOUT,
        },
        task_supervisor,
    )
    .await
}

/// Forward an HTTP request to the target URL and return the response.
#[cfg(test)]
pub async fn proxy_request(
    req: Request,
    target_url: &str,
    task_supervisor: std::sync::Arc<crate::task_supervisor::TaskSupervisor>,
) -> Result<Response, AppError> {
    proxy_request_with_fallback(req, target_url, false, task_supervisor).await
}

#[cfg(test)]
pub async fn proxy_request_with_fallback(
    req: Request,
    target_url: &str,
    allow_fallback_8080: bool,
    task_supervisor: std::sync::Arc<crate::task_supervisor::TaskSupervisor>,
) -> Result<Response, AppError> {
    proxy_request_with_fallback_port(req, target_url, allow_fallback_8080, 8080, task_supervisor)
        .await
}

#[cfg(test)]
pub async fn proxy_request_with_fallback_port(
    req: Request,
    target_url: &str,
    allow_fallback: bool,
    fallback_port: u16,
    task_supervisor: std::sync::Arc<crate::task_supervisor::TaskSupervisor>,
) -> Result<Response, AppError> {
    proxy_request_with_fallback_port_and_retries(
        req,
        target_url,
        allow_fallback,
        fallback_port,
        1,
        std::time::Duration::ZERO,
        task_supervisor,
    )
    .await
}

pub async fn proxy_request_with_fallback_port_and_retries(
    req: Request,
    target_url: &str,
    allow_fallback: bool,
    fallback_port: u16,
    max_attempts: usize,
    retry_delay: std::time::Duration,
    task_supervisor: std::sync::Arc<crate::task_supervisor::TaskSupervisor>,
) -> Result<Response, AppError> {
    proxy_request_with_fallback_port_and_retries_with_options(
        req,
        target_url,
        ProxyRequestOptions {
            allow_fallback,
            fallback_port,
            max_attempts,
            retry_delay,
            upstream_request_timeout: POD_PROXY_UPSTREAM_REQUEST_TIMEOUT,
        },
        task_supervisor,
    )
    .await
}

#[derive(Debug, Clone, Copy)]
struct ProxyRequestOptions {
    allow_fallback: bool,
    fallback_port: u16,
    max_attempts: usize,
    retry_delay: std::time::Duration,
    upstream_request_timeout: std::time::Duration,
}

async fn proxy_request_with_fallback_port_and_retries_with_options(
    req: Request,
    target_url: &str,
    options: ProxyRequestOptions,
    task_supervisor: std::sync::Arc<crate::task_supervisor::TaskSupervisor>,
) -> Result<Response, AppError> {
    let uri: hyper::Uri = target_url
        .parse()
        .map_err(|e| AppError::Internal(format!("Invalid target URL: {}", e)))?;

    let host = uri.host().unwrap_or("127.0.0.1");
    let port = uri.port_u16().unwrap_or(80);
    let scheme = uri.scheme_str().unwrap_or("http");
    let method = req.method().clone();
    let req_headers = req.headers().clone();
    let request_path = req.uri().path().to_string();
    let req_query = req.uri().query().map(str::to_string);
    let body_bytes = axum::body::to_bytes(req.into_body(), MAX_PROXY_REQUEST_BODY_BYTES)
        .await
        .map_err(|e| {
            let msg = e.to_string();
            if msg.contains("length limit") || msg.contains("too large") {
                AppError::PayloadTooLarge(format!(
                    "Request body exceeds {} bytes",
                    MAX_PROXY_REQUEST_BODY_BYTES
                ))
            } else {
                AppError::BadGateway(format!("Failed to read request body: {}", e))
            }
        })?;

    let mut path_and_query = uri
        .path_and_query()
        .map(|pq| pq.as_str().to_string())
        .unwrap_or_else(|| "/".to_string());
    if let Some(query) = req_query.as_deref() {
        let has_query = path_and_query.contains('?');
        if !query.is_empty() && !has_query {
            path_and_query.push('?');
            path_and_query.push_str(query);
        }
    }

    let attempts = options.max_attempts.max(1);
    let mut last_bad_gateway = None;
    for attempt in 0..attempts {
        match send_proxy_request_with_fallback_attempt(
            ProxyUpstreamRequest {
                scheme,
                host,
                port,
                path_and_query: &path_and_query,
                request_path: &request_path,
                method: &method,
                req_headers: &req_headers,
                body_bytes: body_bytes.clone(),
                upstream_request_timeout: options.upstream_request_timeout,
            },
            options.allow_fallback,
            options.fallback_port,
            task_supervisor.clone(),
        )
        .await
        {
            Ok(resp) => return Ok(resp),
            Err(ProxyUpstreamError::Readiness(msg)) if attempt + 1 < attempts => {
                tracing::debug!(
                    "proxy: retrying {}:{} after readiness attempt {}/{}: {}",
                    host,
                    port,
                    attempt + 1,
                    attempts,
                    msg
                );
                last_bad_gateway = Some(AppError::BadGateway(msg));
                task_supervisor
                    .sleep("pod_proxy_readiness_retry", options.retry_delay)
                    .await
                    .map_err(|err| {
                        AppError::Internal(format!("Proxy retry timer failed: {err}"))
                    })?;
            }
            Err(err) => return Err(err.into_app_error()),
        }
    }

    Err(last_bad_gateway.unwrap_or_else(|| {
        AppError::BadGateway(format!("Failed to proxy request to {host}:{port}"))
    }))
}

#[derive(Debug)]
enum ProxyUpstreamError {
    Readiness(String),
    Terminal(AppError),
}

impl ProxyUpstreamError {
    fn into_app_error(self) -> AppError {
        match self {
            ProxyUpstreamError::Readiness(msg) => AppError::BadGateway(msg),
            ProxyUpstreamError::Terminal(err) => err,
        }
    }
}

async fn send_proxy_request_with_fallback_attempt(
    req: ProxyUpstreamRequest<'_>,
    allow_fallback: bool,
    fallback_port: u16,
    task_supervisor: std::sync::Arc<crate::task_supervisor::TaskSupervisor>,
) -> Result<Response, ProxyUpstreamError> {
    let ProxyUpstreamRequest {
        scheme,
        host,
        port,
        path_and_query,
        request_path,
        method,
        req_headers,
        body_bytes,
        upstream_request_timeout,
    } = req;

    match send_proxy_request(
        ProxyUpstreamRequest {
            scheme,
            host,
            port,
            path_and_query,
            request_path,
            method,
            req_headers,
            body_bytes: body_bytes.clone(),
            upstream_request_timeout,
        },
        task_supervisor.clone(),
    )
    .await
    {
        Ok(resp)
            if allow_fallback
                && port != fallback_port
                && resp.status() == StatusCode::BAD_GATEWAY =>
        {
            tracing::debug!(
                "proxy: retrying {}:{} via {} after upstream {} response",
                host,
                port,
                fallback_port,
                StatusCode::BAD_GATEWAY
            );
            send_proxy_request(
                ProxyUpstreamRequest {
                    scheme,
                    host,
                    port: fallback_port,
                    path_and_query,
                    request_path,
                    method,
                    req_headers,
                    body_bytes,
                    upstream_request_timeout,
                },
                task_supervisor.clone(),
            )
            .await
        }
        Ok(resp) => Ok(resp),
        Err(ProxyUpstreamError::Readiness(msg)) if allow_fallback && port != fallback_port => {
            tracing::debug!(
                "proxy: retrying {}:{} via {} after primary failure: {}",
                host,
                port,
                fallback_port,
                msg
            );
            send_proxy_request(
                ProxyUpstreamRequest {
                    scheme,
                    host,
                    port: fallback_port,
                    path_and_query,
                    request_path,
                    method,
                    req_headers,
                    body_bytes,
                    upstream_request_timeout,
                },
                task_supervisor.clone(),
            )
            .await
        }
        Err(e) => Err(e),
    }
}

struct ProxyUpstreamRequest<'a> {
    scheme: &'a str,
    host: &'a str,
    port: u16,
    path_and_query: &'a str,
    request_path: &'a str,
    method: &'a axum::http::Method,
    req_headers: &'a axum::http::HeaderMap,
    body_bytes: Bytes,
    upstream_request_timeout: std::time::Duration,
}

async fn send_proxy_request(
    req: ProxyUpstreamRequest<'_>,
    task_supervisor: std::sync::Arc<crate::task_supervisor::TaskSupervisor>,
) -> Result<Response, ProxyUpstreamError> {
    let ProxyUpstreamRequest {
        scheme,
        host,
        port,
        path_and_query,
        request_path,
        method,
        req_headers,
        body_bytes,
        upstream_request_timeout,
    } = req;
    if scheme.eq_ignore_ascii_case("https") {
        return match send_proxy_request_https(
            host,
            port,
            path_and_query,
            request_path,
            method,
            req_headers,
            body_bytes,
        )
        .await
        {
            Ok(resp) => Ok(resp),
            Err(AppError::BadGateway(msg)) => Err(ProxyUpstreamError::Readiness(msg)),
            Err(err) => Err(ProxyUpstreamError::Terminal(err)),
        };
    }

    // Use short bounded timeouts so proxy calls fail fast and retry at the client/test layer
    // instead of hanging until the parent context expires.
    let connect_timeout = std::time::Duration::from_secs(5);
    let connect_result = task_supervisor
        .timeout(
            "pod_proxy_tcp_connect_timeout",
            connect_timeout,
            tokio::net::TcpStream::connect(format!("{}:{}", host, port)),
        )
        .await
        .map_err(|e| ProxyUpstreamError::Readiness(format!("TCP connect cancelled: {e}")))?;
    let stream = connect_result
        .map_err(|_| {
            ProxyUpstreamError::Readiness(format!(
                "Timed out connecting to pod at {}:{} after {:?}",
                host, port, connect_timeout
            ))
        })?
        .map_err(|e| {
            ProxyUpstreamError::Readiness(format!(
                "Failed to connect to pod at {}:{}: {}",
                host, port, e
            ))
        })?;

    let io = hyper_util::rt::TokioIo::new(stream);

    let (mut sender, conn) = hyper::client::conn::http1::handshake(io)
        .await
        .map_err(|e| ProxyUpstreamError::Readiness(format!("HTTP handshake failed: {}", e)))?;

    // Spawn connection driver
    if let Err(err) = task_supervisor
        .spawn_async(
            crate::task_supervisor::TaskCategory::Others,
            "pod_proxy_connection_driver",
            async move {
                if let Err(e) = conn.await {
                    tracing::error!("proxy connection error: {}", e);
                }
            },
        )
        .await
    {
        tracing::warn!("Failed to spawn proxy connection driver: {}", err);
    }

    // Build the forwarded request
    let mut builder = hyper::Request::builder().method(method).uri(path_and_query);

    // Forward end-to-end headers. Body framing and hop-by-hop headers are
    // recomputed for the buffered outbound request.
    let header_policy =
        crate::api::backend_proxy_headers::BackendProxyHeaderPolicy::workload_backend();
    for (key, value) in req_headers {
        if header_policy.should_forward(key) {
            builder = builder.header(key, value);
        }
    }
    if should_set_proxy_content_length(method, body_bytes.len()) {
        builder = builder.header(header::CONTENT_LENGTH, body_bytes.len().to_string());
    }
    builder = builder.header(header::HOST, format!("{}:{}", host, port));

    let upstream_req = builder
        .body(axum::body::Body::from(body_bytes))
        .map_err(|e| {
            ProxyUpstreamError::Terminal(AppError::Internal(format!(
                "Failed to build proxy request: {}",
                e
            )))
        })?;

    // Bound upstream header response wait.
    let request_timeout = upstream_request_timeout;
    let upstream_result = task_supervisor
        .timeout(
            "pod_proxy_upstream_request_timeout",
            request_timeout,
            sender.send_request(upstream_req),
        )
        .await
        .map_err(|e| {
            ProxyUpstreamError::Terminal(AppError::BadGateway(format!(
                "Proxy request cancelled: {e}"
            )))
        })?;
    let upstream_resp = upstream_result
        .map_err(|_| {
            ProxyUpstreamError::Readiness(format!(
                "Proxy request to {}:{} timed out after {:?}",
                host, port, request_timeout
            ))
        })?
        .map_err(|e| ProxyUpstreamError::Readiness(format!("Proxy request failed: {}", e)))?;

    // Fully read the upstream response body with timeout to avoid returning a body stream
    // that can hang the caller until outer context cancellation.
    let (mut parts, body) = upstream_resp.into_parts();
    let body_read_timeout = std::time::Duration::from_secs(10);
    let body_result = task_supervisor
        .timeout(
            "pod_proxy_response_body_timeout",
            body_read_timeout,
            axum::body::to_bytes(axum::body::Body::new(body), MAX_PROXY_RESPONSE_BODY_BYTES),
        )
        .await
        .map_err(|e| {
            ProxyUpstreamError::Terminal(AppError::BadGateway(format!(
                "Proxy response body read cancelled: {e}"
            )))
        })?;
    let body_bytes = body_result
        .map_err(|_| {
            ProxyUpstreamError::Terminal(AppError::BadGateway(format!(
                "Proxy response body from {}:{} timed out after {:?}",
                host, port, body_read_timeout
            )))
        })?
        .map_err(|e| {
            ProxyUpstreamError::Terminal(AppError::BadGateway(format!(
                "Failed to read proxied response body: {}",
                e
            )))
        })?;

    let body_bytes = rewrite_proxy_response_body(&mut parts.headers, request_path, body_bytes);
    Ok(Response::from_parts(
        parts,
        axum::body::Body::from(body_bytes),
    ))
}

pub async fn send_proxy_request_https(
    host: &str,
    port: u16,
    path_and_query: &str,
    request_path: &str,
    method: &axum::http::Method,
    req_headers: &axum::http::HeaderMap,
    body_bytes: Bytes,
) -> Result<Response, AppError> {
    let method = reqwest::Method::from_bytes(method.as_str().as_bytes())
        .map_err(|e| AppError::BadRequest(format!("Unsupported proxy method: {e}")))?;
    let url = format!("https://{}:{}{}", host, port, path_and_query);

    let client = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(5))
        .timeout(std::time::Duration::from_secs(20))
        .danger_accept_invalid_certs(true)
        .no_proxy()
        .build()
        .map_err(|e| AppError::Internal(format!("Failed to build HTTPS proxy client: {e}")))?;

    let mut request_builder = client.request(method, &url);
    let header_policy =
        crate::api::backend_proxy_headers::BackendProxyHeaderPolicy::workload_backend();
    for (key, value) in req_headers {
        if header_policy.should_forward(key)
            && let Ok(value_str) = value.to_str()
        {
            request_builder = request_builder.header(key.as_str(), value_str);
        }
    }

    let upstream_resp = request_builder
        .body(body_bytes)
        .send()
        .await
        .map_err(|e| AppError::BadGateway(format!("HTTPS proxy request failed: {e}")))?;
    let status = StatusCode::from_u16(upstream_resp.status().as_u16())
        .map_err(|e| AppError::Internal(format!("Invalid proxied status code: {e}")))?;
    let upstream_headers = upstream_resp.headers().clone();
    let body =
        read_reqwest_body_limited(upstream_resp, MAX_PROXY_RESPONSE_BODY_BYTES, "HTTPS proxy")
            .await?;

    let mut headers = axum::http::HeaderMap::new();
    for (key, value) in &upstream_headers {
        if let Ok(name) = header::HeaderName::from_bytes(key.as_str().as_bytes())
            && let Ok(val) = header::HeaderValue::from_bytes(value.as_bytes())
        {
            headers.append(name, val);
        }
    }
    let body = rewrite_proxy_response_body(&mut headers, request_path, body);
    let mut response_builder = Response::builder().status(status);
    for (key, value) in &headers {
        response_builder = response_builder.header(key, value);
    }
    response_builder
        .body(axum::body::Body::from(body))
        .map_err(|e| AppError::Internal(format!("Failed to build HTTPS proxy response: {e}")))
}

pub fn rewrite_proxy_response_body(
    headers: &mut axum::http::HeaderMap,
    request_path: &str,
    body: Bytes,
) -> Bytes {
    let Some(proxy_prefix_raw) = request_path
        .find("/proxy/")
        .map(|idx| &request_path[..idx + "/proxy/".len()])
    else {
        return body;
    };
    let proxy_prefix = if proxy_prefix_raw.starts_with("/namespaces/")
        || proxy_prefix_raw.starts_with("/nodes/")
    {
        format!("/api/v1{proxy_prefix_raw}")
    } else {
        proxy_prefix_raw.to_string()
    };

    let Some(content_type) = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.to_ascii_lowercase())
    else {
        return body;
    };
    if !content_type.starts_with("text/html") && !content_type.starts_with("application/xhtml+xml")
    {
        return body;
    }

    let Ok(text) = std::str::from_utf8(&body) else {
        return body;
    };

    let rewritten = text.replace("href=\"/", &format!("href=\"{proxy_prefix}"));
    if rewritten == text {
        return body;
    }

    let rewritten = Bytes::from(rewritten);
    match header::HeaderValue::from_str(&rewritten.len().to_string()) {
        Ok(value) => {
            headers.insert(header::CONTENT_LENGTH, value);
        }
        _ => {
            headers.remove(header::CONTENT_LENGTH);
        }
    }
    rewritten
}

/// GET/POST/PUT/DELETE/PATCH /api/v1/namespaces/{ns}/services/{name}/proxy
pub async fn service_proxy(
    State(state): State<Arc<AppState>>,
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<ProxyQuery>,
    req: Request,
) -> Result<Response, AppError> {
    service_proxy_inner(state, &namespace, &name, "", query.port, req).await
}

/// GET/POST/PUT/DELETE/PATCH /api/v1/namespaces/{ns}/services/{name}/proxy/{*path}
pub async fn service_proxy_with_path(
    State(state): State<Arc<AppState>>,
    Path((namespace, name, proxy_path)): Path<(String, String, String)>,
    Query(query): Query<ProxyQuery>,
    req: Request,
) -> Result<Response, AppError> {
    service_proxy_inner(state, &namespace, &name, &proxy_path, query.port, req).await
}

pub async fn service_proxy_inner(
    state: Arc<AppState>,
    namespace: &str,
    name_param: &str,
    proxy_path: &str,
    port_override: Option<u16>,
    req: Request,
) -> Result<Response, AppError> {
    if let Some(resp) = maybe_redirect_proxy_root(&req, proxy_path) {
        return Ok(resp);
    }

    // K8s service proxy URL may include a port: /services/{name}:{port}/proxy
    let parsed = parse_proxy_name_port(name_param);
    let scheme = parsed.scheme.unwrap_or("http");
    let name = parsed.name;
    let effective_port_override = port_override.or(parsed.port_num);

    // Look up the service
    let service = state
        .db
        .get_resource("v1", "Service", Some(namespace), name)
        .await?
        .ok_or_else(|| {
            AppError::NotFound(format!("Service {}/{} not found", namespace, name_param))
        })?;

    // Get service spec
    let spec = service
        .data
        .get("spec")
        .ok_or_else(|| AppError::Internal("Service has no spec".to_string()))?;

    // Select service port by explicit numeric override, explicit named override,
    // or first declared service port.
    let service_ports = spec
        .get("ports")
        .and_then(|p| p.as_array())
        .ok_or_else(|| {
            AppError::BadRequest(format!("Service {}/{} has no ports", namespace, name))
        })?;
    let selected_service_port = if let Some(port_num) = effective_port_override {
        service_ports.iter().find(|p| {
            p.get("port")
                .and_then(|port| port.as_u64())
                .map(|p| p as u16 == port_num)
                .unwrap_or(false)
        })
    } else if let Some(port_name) = parsed.port_name {
        service_ports
            .iter()
            .find(|p| p.get("name").and_then(|n| n.as_str()) == Some(port_name))
    } else {
        service_ports.first()
    }
    .ok_or_else(|| {
        AppError::BadRequest(format!(
            "Service {}/{} does not expose requested port",
            namespace, name_param
        ))
    })?;

    // Get Endpoints for this service
    let endpoints = state
        .db
        .get_resource("v1", "Endpoints", Some(namespace), name)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("Endpoints {}/{} not found", namespace, name)))?;

    // Resolve the requested service port to its name/number once; each
    // endpoint's concrete target port is resolved from its own subset below.
    let selected_service_port_name = selected_service_port.get("name").and_then(|n| n.as_str());
    let selected_service_port_number = selected_service_port
        .get("port")
        .and_then(|port| port.as_u64())
        .map(|p| p as u16)
        .unwrap_or(80);

    // Collect ALL ready endpoint addresses across every subset, each paired
    // with the target port resolved from its own subset (ports are already
    // numeric after endpoints reconciliation, including named targetPort
    // lookups). K8s selects a random ready endpoint per request and must not
    // get stuck on a single unreachable one, so we rotate the starting
    // endpoint and fail over across the rest within this request.
    let empty_subsets: Vec<serde_json::Value> = Vec::new();
    let subsets = endpoints
        .data
        .get("subsets")
        .and_then(|s| s.as_array())
        .unwrap_or(&empty_subsets);
    let mut candidates: Vec<(String, u16)> = Vec::new();
    for subset in subsets {
        let target_port = subset
            .get("ports")
            .and_then(|ports| ports.as_array())
            .and_then(|ports| {
                if let Some(port_name) = selected_service_port_name {
                    ports
                        .iter()
                        .find(|p| p.get("name").and_then(|n| n.as_str()) == Some(port_name))
                } else {
                    ports.first()
                }
            })
            .and_then(|p| p.get("port"))
            .and_then(|port| port.as_u64())
            .map(|p| p as u16)
            .unwrap_or(selected_service_port_number);
        if let Some(addresses) = subset.get("addresses").and_then(|a| a.as_array()) {
            for addr in addresses {
                if let Some(ip) = addr.get("ip").and_then(|ip| ip.as_str()) {
                    candidates.push((ip.to_string(), target_port));
                }
            }
        }
    }
    if candidates.is_empty() {
        return Err(AppError::BadGateway(format!(
            "No ready endpoints for service {}/{}",
            namespace, name
        )));
    }

    // Rotate the starting endpoint across requests for even distribution.
    let start = SERVICE_PROXY_ENDPOINT_CURSOR.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let candidate_count = candidates.len();

    // Buffer the client request once so it can be replayed against each
    // candidate endpoint on failover.
    let method = req.method().clone();
    let req_uri = req.uri().clone();
    let req_headers = req.headers().clone();
    let body_bytes = axum::body::to_bytes(req.into_body(), MAX_PROXY_REQUEST_BODY_BYTES)
        .await
        .map_err(|e| {
            let msg = e.to_string();
            if msg.contains("length limit") || msg.contains("too large") {
                AppError::PayloadTooLarge(format!(
                    "Request body exceeds {} bytes",
                    MAX_PROXY_REQUEST_BODY_BYTES
                ))
            } else {
                AppError::BadGateway(format!("Failed to read request body: {}", e))
            }
        })?;

    let mut last_bad_gateway: Option<AppError> = None;
    for offset in 0..candidate_count {
        let (endpoint_ip, target_port) =
            &candidates[(start.wrapping_add(offset)) % candidate_count];
        let target_url = if proxy_path.is_empty() {
            format!("{}://{}:{}/", scheme, endpoint_ip, target_port)
        } else {
            format!(
                "{}://{}:{}/{}",
                scheme, endpoint_ip, target_port, proxy_path
            )
        };

        tracing::debug!(
            "services/proxy: {}/{} -> {} (method={}, endpoint {}/{})",
            namespace,
            name,
            target_url,
            method,
            offset + 1,
            candidate_count
        );

        let replay = rebuild_proxy_request(
            method.clone(),
            req_uri.clone(),
            req_headers.clone(),
            body_bytes.clone(),
        );
        match service_proxy_request_with_readiness_retries(
            replay,
            &target_url,
            state.task_supervisor.clone(),
        )
        .await
        {
            Ok(resp) => return Ok(resp),
            // A bad gateway (timeout / connect failure / readiness) against
            // one endpoint should fail over to the remaining endpoints rather
            // than failing the whole request on a single unreachable pod.
            Err(AppError::BadGateway(msg)) => {
                tracing::debug!(
                    "services/proxy: endpoint {} failed ({}); trying next of {}",
                    endpoint_ip,
                    msg,
                    candidate_count
                );
                last_bad_gateway = Some(AppError::BadGateway(msg));
            }
            Err(other) => return Err(other),
        }
    }

    Err(last_bad_gateway.unwrap_or_else(|| {
        AppError::BadGateway(format!(
            "Failed to proxy request to service {}/{}",
            namespace, name
        ))
    }))
}

#[cfg(test)]
mod retry_policy_tests {
    use super::proxy_retry_policy;
    use axum::http::Method;

    #[test]
    fn idempotent_methods_retry_transient_failures() {
        for method in [Method::GET, Method::HEAD, Method::OPTIONS] {
            let (attempts, delay) = proxy_retry_policy(&method);
            assert_eq!(attempts, 8, "{method} should retry transient failures");
            assert_eq!(delay, std::time::Duration::from_millis(250));
        }
    }

    #[test]
    fn mutating_methods_are_not_retried() {
        // A slow-but-successful POST/PUT/PATCH/DELETE must not be duplicated.
        for method in [Method::POST, Method::PUT, Method::PATCH, Method::DELETE] {
            let (attempts, delay) = proxy_retry_policy(&method);
            assert_eq!(attempts, 1, "{method} must not retry (non-idempotent)");
            assert_eq!(delay, std::time::Duration::ZERO);
        }
    }
}
