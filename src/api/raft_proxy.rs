//! Raft follower-to-leader API proxy.
//!
//! In the klights HA model, all controlplanes bind TCP 7679 but only
//! the raft leader serves K8s API requests directly. Follower
//! controlplanes transparently reverse-proxy K8s API requests to the
//! current leader. gRPC (raft transport) and `/healthz` always go
//! through locally.

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::HeaderValue;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use std::sync::Arc;
use std::{fs as blocking_fs, path::Path};

/// Shared state for the leader-proxy middleware.
#[derive(Clone)]
pub struct RaftLeaderProxy {
    /// Watch receiver: `true` when this node is the raft leader.
    is_leader: tokio::sync::watch::Receiver<bool>,
    /// Watch receiver: the current leader's `https://<ip>:<port>` address.
    leader_addr: tokio::sync::watch::Receiver<Option<String>>,
    /// Cluster/front-proxy CA certificate PEM for verifying leader serving
    /// certificates. When set, the proxy verifies the leader's TLS certificate
    /// against this CA instead of accepting any certificate.
    ca_cert_pem: Option<String>,
    /// Internal server/proxy client identity used to authenticate the follower
    /// proxy hop to the leader. The original caller identity is delegated
    /// through sanitized requestheader fields.
    proxy_client_identity: Option<reqwest::Identity>,
}

impl RaftLeaderProxy {
    pub fn new(
        is_leader: tokio::sync::watch::Receiver<bool>,
        leader_addr: tokio::sync::watch::Receiver<Option<String>>,
        ca_cert_pem: Option<String>,
    ) -> Self {
        Self {
            is_leader,
            leader_addr,
            ca_cert_pem,
            proxy_client_identity: None,
        }
    }

    pub fn with_proxy_client_identity(mut self, identity: Option<reqwest::Identity>) -> Self {
        self.proxy_client_identity = identity;
        self
    }

    /// Build a reqwest client for proxying to leader.
    ///
    /// The follower presents its internal API server certificate so the leader
    /// can trust the requestheader identity stamped from the original caller.
    pub(crate) fn http_client(&self) -> reqwest::Client {
        let mut builder = reqwest::Client::builder()
            // The leader is reached directly at its in-cluster address; an
            // ambient HTTP(S)_PROXY env var must never reroute control-plane
            // traffic (mirrors the CRD conversion webhook client).
            .no_proxy()
            // Bound connection establishment so an unreachable leader still
            // fails fast into retryable 503, but do NOT bound the total
            // request: streamed responses (pod log `follow`, chunked GETs)
            // must be relayed for as long as the client stays connected.
            .connect_timeout(std::time::Duration::from_secs(10));

        // Verify the leader's server certificate against the cluster CA.
        if let Some(ref ca_pem) = self.ca_cert_pem {
            let cert = reqwest::Certificate::from_pem(ca_pem.as_bytes())
                .expect("cluster CA cert for raft leader proxy");
            builder = builder
                .tls_built_in_root_certs(false)
                .add_root_certificate(cert);
        }
        if let Some(identity) = &self.proxy_client_identity {
            builder = builder.identity(identity.clone());
        }

        builder.build().unwrap_or_default()
    }

    /// Whether this node is currently the raft leader.
    pub fn is_leader(&self) -> bool {
        *self.is_leader.borrow()
    }

    /// The current leader's API address (e.g. `https://10.99.0.10:7679`).
    pub fn leader_addr(&self) -> Option<String> {
        self.leader_addr.borrow().clone()
    }
}

pub(crate) async fn load_proxy_client_identity(
    containerd_namespace: &str,
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
) -> Option<reqwest::Identity> {
    match load_proxy_client_identity_from_namespace(containerd_namespace, task_supervisor).await {
        Ok(identity) => Some(identity),
        Err(err) => {
            tracing::warn!("Failed to load raft leader proxy client identity: {err}");
            None
        }
    }
}

async fn load_proxy_client_identity_from_namespace(
    containerd_namespace: &str,
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
) -> anyhow::Result<reqwest::Identity> {
    let cert_path = crate::paths::api_proxy_cert_path(containerd_namespace);
    let key_path = crate::paths::api_proxy_key_path(containerd_namespace);
    let cert = read_proxy_client_identity_file(
        task_supervisor,
        &cert_path,
        "raft_proxy_identity_read_cert",
    )
    .await?;
    let key =
        read_proxy_client_identity_file(task_supervisor, &key_path, "raft_proxy_identity_read_key")
            .await?;

    reqwest::Identity::from_pkcs8_pem(&cert, &key)
        .map_err(|err| anyhow::anyhow!("invalid raft leader proxy client identity: {err}"))
}

async fn read_proxy_client_identity_file(
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

/// Axum middleware: gate K8s API requests on raft leadership.
///
/// - gRPC requests (raft transport) → always pass through
/// - `/healthz` → always pass through
/// - On leader → pass through to normal handlers
/// - On follower → reverse-proxy to the leader
pub async fn leader_proxy_middleware(
    State(state): State<Arc<crate::api::AppState>>,
    request: Request,
    next: Next,
) -> Response {
    // Check if this is a gRPC request (raft transport) or healthz
    let is_grpc = request
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.starts_with("application/grpc"));
    let path = request.uri().path();
    let is_health = path == "/healthz";

    if is_grpc || is_health {
        return next.run(request).await;
    }

    // Retrieve the proxy state from AppState
    let Some(proxy) = state.is_raft_leader_rx.clone() else {
        // No raft proxy configured — single-node or worker; pass through
        return next.run(request).await;
    };

    if proxy.is_leader() {
        return next.run(request).await;
    }

    // Follower: proxy to leader or fail closed if no safe leader path exists.
    follower_handle(request, &proxy).await
}

/// Follower request handler: proxy to leader when available. If no current
/// leader endpoint is known or the leader cannot be reached, fail closed with a
/// retryable Kubernetes 503 rather than serving stale local cluster.db state.
async fn follower_handle(request: Request, proxy: &RaftLeaderProxy) -> Response {
    if let Some(leader_addr) = proxy.leader_addr() {
        let (parts, body) = request.into_parts();
        let body_bytes = match axum::body::to_bytes(body, 10 * 1024 * 1024).await {
            Ok(bytes) => bytes,
            Err(e) => {
                tracing::warn!(error = %e, "failed to read request body");
                return service_unavailable("failed to read request body");
            }
        };

        return proxy_raw(&parts, &body_bytes, &leader_addr, &proxy.http_client()).await;
    }

    service_unavailable("no raft leader elected; retry when a leader is available")
}

/// Send a pre-buffered request to the leader and return the response.
/// Returns 503 on connection failure; followers must fail closed rather than
/// retrying the request against local cluster.db state.
async fn proxy_raw(
    parts: &axum::http::request::Parts,
    body_bytes: &[u8],
    leader_addr: &str,
    client: &reqwest::Client,
) -> Response {
    let target_url = format!(
        "{}{}",
        leader_addr.trim_end_matches('/'),
        parts.uri.path_and_query().map_or("/", |pq| pq.as_str())
    );

    let method = reqwest::Method::from_bytes(parts.method.as_str().as_bytes())
        .unwrap_or(reqwest::Method::GET);

    // No total request timeout: a follow/watch-style streamed response must be
    // relayed for its full (open-ended) lifetime. Connection establishment is
    // bounded by the client's connect_timeout instead.
    let mut req_builder = client
        .request(method, &target_url)
        .body(body_bytes.to_vec());
    let delegated_identity = parts
        .extensions
        .get::<crate::auth::AuthenticatedIdentity>()
        .cloned();
    // The original caller's actual TLS client certificate, if they authenticated
    // with one. The follower's TLS stack already verified it against the cluster
    // CA. Forwarding it lets the leader *re-authenticate it natively* and thus
    // preserve the caller's real identity (including `system:masters`) without
    // the follower having to assert that group on its own say-so. This is the
    // end user's cert — never the follower's own proxy credential, which stays
    // the mTLS transport identity only.
    let forwarded_client_cert = parts
        .extensions
        .get::<crate::auth::TlsClientCertificate>()
        .cloned();

    for (name, value) in &parts.headers {
        if name == "host" || name == "connection" {
            continue;
        }
        // Defense in depth: never forward client-supplied identity-impersonation
        // headers to the leader. The leader's authenticate middleware also strips
        // these, but stripping here too means a reordering or a new
        // pre-auth-trusting endpoint cannot turn the follower proxy into an
        // identity-spoofing vector.
        let lname = name.as_str().to_ascii_lowercase();
        if lname == "x-remote-user"
            || lname == "x-remote-uid"
            || lname == crate::auth::FORWARDED_CLIENT_CERT_HEADER
            || lname.starts_with("x-remote-group")
            || lname.starts_with("x-remote-extra-")
        {
            continue;
        }
        if let (Ok(req_name), Ok(req_value)) = (
            reqwest::header::HeaderName::from_bytes(name.as_str().as_bytes()),
            reqwest::header::HeaderValue::from_bytes(value.as_bytes()),
        ) {
            req_builder = req_builder.header(req_name, req_value);
        }
    }
    if let Some(identity) = delegated_identity {
        req_builder = stamp_delegated_identity_headers(req_builder, &identity);
    }
    if let Some(cert) = forwarded_client_cert {
        req_builder = stamp_forwarded_client_cert(req_builder, &cert);
    }

    match req_builder.send().await {
        Ok(resp) => {
            let status = axum::http::StatusCode::from_u16(resp.status().as_u16())
                .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
            let resp_headers: Vec<_> = resp
                .headers()
                .iter()
                .map(|(n, v)| (n.clone(), v.clone()))
                .collect();

            // Stream the body through rather than buffering it to completion.
            // Buffering blocked long-lived responses (pod log `follow`, chunked
            // GETs) until the whole body arrived — which for a follow stream is
            // never — so the client saw nothing. Relaying the byte stream lets
            // each frame reach the client as the leader emits it.
            let resp_body = Body::from_stream(resp.bytes_stream());

            let mut response = axum::http::Response::new(resp_body);
            *response.status_mut() = status;

            // Forward response headers
            for (name, value) in &resp_headers {
                if name == "connection" || name == "transfer-encoding" {
                    continue;
                }
                if let (Ok(ax_name), Ok(ax_value)) = (
                    axum::http::HeaderName::from_bytes(name.as_str().as_bytes()),
                    axum::http::HeaderValue::from_bytes(value.as_bytes()),
                ) {
                    response.headers_mut().insert(ax_name, ax_value);
                }
            }

            response
        }
        Err(e) => {
            tracing::warn!(
                target = %target_url,
                error = %e,
                "failed to proxy request to leader"
            );
            service_unavailable(&format!("leader unreachable: {e}"))
        }
    }
}

fn stamp_delegated_identity_headers(
    mut req_builder: reqwest::RequestBuilder,
    identity: &crate::auth::AuthenticatedIdentity,
) -> reqwest::RequestBuilder {
    req_builder = req_builder.header("x-remote-user", identity.username.as_str());
    for group in &identity.groups {
        req_builder = req_builder.header("x-remote-group", group.as_str());
    }
    if let Some(uid) = identity.uid.as_deref() {
        req_builder = req_builder.header("x-remote-uid", uid);
    }
    for (key, value) in &identity.extra {
        req_builder = req_builder.header(format!("x-remote-extra-{key}"), value.as_str());
    }
    req_builder
}

/// Stamp the original caller's actual client certificate (base64 DER) so the
/// leader can cryptographically re-authenticate it against the cluster CA. This
/// is what carries a kubectl admin's real `system:masters` access across the
/// follower→leader hop in a verifiable, unforgeable way.
fn stamp_forwarded_client_cert(
    req_builder: reqwest::RequestBuilder,
    cert: &crate::auth::TlsClientCertificate,
) -> reqwest::RequestBuilder {
    use base64::Engine;
    let encoded = base64::engine::general_purpose::STANDARD.encode(&cert.0);
    req_builder.header(crate::auth::FORWARDED_CLIENT_CERT_HEADER, encoded)
}

fn service_unavailable(msg: &str) -> Response {
    let mut resp = crate::api::AppError::ServiceUnavailable(msg.to_string()).into_response();
    resp.headers_mut()
        .insert("connection", HeaderValue::from_static("close"));
    resp.headers_mut()
        .insert("retry-after", HeaderValue::from_static("1"));
    resp
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stamp_forwarded_client_cert_encodes_der_into_header() {
        use base64::Engine;
        let der = vec![0x30u8, 0x82, 0xDE, 0xAD, 0xBE, 0xEF];
        let client = reqwest::Client::new();
        let builder = client.get("https://leader.invalid/api/v1/nodes");
        let builder =
            stamp_forwarded_client_cert(builder, &crate::auth::TlsClientCertificate(der.clone()));
        let req = builder.build().expect("request builds");
        let header = req
            .headers()
            .get(crate::auth::FORWARDED_CLIENT_CERT_HEADER)
            .expect("forwarded client cert header present")
            .to_str()
            .unwrap();
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(header)
            .unwrap();
        assert_eq!(decoded, der, "leader must receive the verbatim client cert");
    }

    #[test]
    fn raft_leader_proxy_reflects_initial_state() {
        let (_, is_leader_rx) = tokio::sync::watch::channel(true);
        let (_, leader_addr_rx) =
            tokio::sync::watch::channel(Some("https://10.99.0.10:7679".to_string()));
        let proxy = RaftLeaderProxy::new(is_leader_rx, leader_addr_rx, None);
        assert!(proxy.is_leader());
        assert_eq!(
            proxy.leader_addr(),
            Some("https://10.99.0.10:7679".to_string())
        );
    }

    #[test]
    fn raft_leader_proxy_follower_state() {
        let (_, is_leader_rx) = tokio::sync::watch::channel(false);
        let (_, leader_addr_rx) =
            tokio::sync::watch::channel(Some("https://10.99.0.10:7679".to_string()));
        let proxy = RaftLeaderProxy::new(is_leader_rx, leader_addr_rx, None);
        assert!(!proxy.is_leader());
        assert_eq!(
            proxy.leader_addr(),
            Some("https://10.99.0.10:7679".to_string())
        );
    }

    #[test]
    fn raft_leader_proxy_no_leader() {
        let (_, is_leader_rx) = tokio::sync::watch::channel(false);
        let (_, leader_addr_rx) = tokio::sync::watch::channel(None::<String>);
        let proxy = RaftLeaderProxy::new(is_leader_rx, leader_addr_rx, None);
        assert!(!proxy.is_leader());
        assert_eq!(proxy.leader_addr(), None);
    }

    #[tokio::test]
    async fn service_unavailable_is_kubernetes_status_response() {
        let response = service_unavailable("no raft leader elected");
        assert_eq!(
            response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );

        let content_type = response
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("");
        assert!(
            content_type.starts_with("application/json"),
            "503 must be a Kubernetes Status JSON response, got content-type {content_type:?}"
        );

        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes)
            .expect("503 response body must be valid Kubernetes Status JSON");
        assert_eq!(body["apiVersion"], "v1");
        assert_eq!(body["kind"], "Status");
        assert_eq!(body["status"], "Failure");
        assert_eq!(body["reason"], "ServiceUnavailable");
        assert_eq!(body["code"], 503);
    }

    /// A long-lived streaming GET (e.g. `kubectl logs -f`, which is a chunked
    /// `GET .../log?follow=true`) must be relayed to the client frame-by-frame.
    /// The previous implementation buffered the whole response body before
    /// returning, so a never-ending follow stream delivered nothing to the
    /// client until the proxy timed out. This test pins the streaming
    /// behaviour: the proxy must surface the leader's first chunk promptly even
    /// while the leader holds the connection open.
    #[tokio::test]
    async fn proxy_streams_response_body_without_buffering() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Mock leader: send headers + one chunk, then hold the connection open
        // (no Content-Length, no EOF) to emulate an active follow stream.
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let _ = stream.read(&mut buf).await.unwrap_or(0);
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\nfirst-line\n")
                .await
                .unwrap();
            stream.flush().await.unwrap();
            // Hold the connection open so the body never reaches EOF.
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        });

        let parts = axum::http::Request::builder()
            .method("GET")
            .uri("/api/v1/namespaces/kube-system/pods/p/log?follow=true")
            .body(())
            .unwrap()
            .into_parts()
            .0;

        let leader_addr = format!("http://{addr}");
        // Ignore any ambient HTTP_PROXY a sibling test may have set in this
        // process; this test exercises the direct streaming relay only.
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let response = proxy_raw(&parts, &[], &leader_addr, &client).await;
        assert_eq!(response.status(), axum::http::StatusCode::OK);

        // The first chunk must arrive without waiting for the (never-arriving)
        // end of the body. A buffering proxy would block here until timeout.
        use futures::StreamExt;
        let mut body = response.into_body().into_data_stream();
        let first = tokio::time::timeout(std::time::Duration::from_secs(5), body.next())
            .await
            .expect("first chunk should stream promptly, not block on full-body buffering")
            .expect("stream should yield a frame")
            .expect("frame should not be an error");
        assert_eq!(&first[..], b"first-line\n");

        server.abort();
    }

    #[tokio::test]
    async fn proxy_http_client_presents_configured_api_proxy_identity() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let _ = rustls::crypto::ring::default_provider().install_default();

        let (ca_cert, ca_key, ca_pem, _) = crate::auth::generate_ca_full().unwrap();
        let (server_cert_pem, server_key_pem) =
            crate::auth::generate_server_cert(&ca_cert, &ca_key).unwrap();
        let (proxy_cert_pem, proxy_key_pem) =
            crate::auth::generate_api_proxy_cert(&ca_cert, &ca_key, "mn-controlplane2").unwrap();
        let identity =
            reqwest::Identity::from_pkcs8_pem(proxy_cert_pem.as_bytes(), proxy_key_pem.as_bytes())
                .expect("api proxy cert/key should build reqwest client identity");

        let server_certs: Vec<rustls::pki_types::CertificateDer> =
            rustls_pemfile::certs(&mut server_cert_pem.as_bytes())
                .collect::<Result<_, _>>()
                .unwrap();
        let server_key = rustls_pemfile::private_key(&mut server_key_pem.as_bytes())
            .unwrap()
            .unwrap();
        let ca_certs: Vec<rustls::pki_types::CertificateDer> =
            rustls_pemfile::certs(&mut ca_pem.as_bytes())
                .collect::<Result<_, _>>()
                .unwrap();
        let mut roots = rustls::RootCertStore::empty();
        let (accepted, _) = roots.add_parsable_certificates(ca_certs);
        assert!(accepted > 0);
        let verifier = rustls::server::WebPkiClientVerifier::builder(std::sync::Arc::new(roots))
            .build()
            .unwrap();
        let server_config = std::sync::Arc::new(
            rustls::ServerConfig::builder()
                .with_client_cert_verifier(verifier)
                .with_single_cert(server_certs, server_key)
                .unwrap(),
        );
        let acceptor = tokio_rustls::TlsAcceptor::from(server_config);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (peer_tx, peer_rx) = tokio::sync::oneshot::channel::<String>();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut tls = acceptor.accept(stream).await.unwrap();
            let peer = tls
                .get_ref()
                .1
                .peer_certificates()
                .and_then(|certs| certs.first())
                .map(|cert| crate::auth::user_from_cert(cert.as_ref()).unwrap().username)
                .unwrap_or_default();
            let _ = peer_tx.send(peer);
            let mut buf = [0u8; 1024];
            let _ = tls.read(&mut buf).await;
            tls.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                .await
                .unwrap();
        });

        let (_, is_leader_rx) = tokio::sync::watch::channel(false);
        let (_, leader_addr_rx) = tokio::sync::watch::channel(Some(format!("https://{addr}")));
        let proxy = RaftLeaderProxy::new(is_leader_rx, leader_addr_rx, Some(ca_pem))
            .with_proxy_client_identity(Some(identity));
        let response = proxy
            .http_client()
            .get(format!("https://{addr}/api"))
            .send()
            .await
            .expect("proxy client should present an accepted client certificate");

        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let peer = peer_rx.await.expect("server should report peer identity");
        assert_eq!(peer, "system:klights:api-proxy:mn-controlplane2");
        server.abort();
    }

    #[tokio::test]
    async fn proxy_http_client_trusts_generated_server_certificate() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let _ = rustls::crypto::ring::default_provider().install_default();

        let (ca_cert, ca_key, ca_pem, _) = crate::auth::generate_ca_full().unwrap();
        let (server_cert_pem, server_key_pem) =
            crate::auth::generate_server_cert(&ca_cert, &ca_key).unwrap();

        let server_certs: Vec<rustls::pki_types::CertificateDer> =
            rustls_pemfile::certs(&mut server_cert_pem.as_bytes())
                .collect::<Result<_, _>>()
                .unwrap();
        let server_key = rustls_pemfile::private_key(&mut server_key_pem.as_bytes())
            .unwrap()
            .unwrap();
        let server_config = std::sync::Arc::new(
            rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
                .with_no_client_auth()
                .with_single_cert(server_certs, server_key)
                .unwrap(),
        );
        let acceptor = tokio_rustls::TlsAcceptor::from(server_config);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut tls = acceptor.accept(stream).await.unwrap();
            let mut buf = [0u8; 1024];
            let _ = tls.read(&mut buf).await;
            tls.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                .await
                .unwrap();
        });

        let (_, is_leader_rx) = tokio::sync::watch::channel(false);
        let (_, leader_addr_rx) = tokio::sync::watch::channel(Some(format!("https://{addr}")));
        let proxy = RaftLeaderProxy::new(is_leader_rx, leader_addr_rx, Some(ca_pem));
        let response = proxy
            .http_client()
            .get(format!("https://{addr}/api"))
            .send()
            .await
            .expect("proxy client should trust generated server certificate");

        assert_eq!(response.status(), reqwest::StatusCode::OK);
        server.abort();
    }
}
