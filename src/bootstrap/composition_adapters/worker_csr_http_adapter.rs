//! Private HTTP/TLS adapter for the auth-owned worker CSR bootstrap port.

use std::{path::PathBuf, sync::Arc};

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose};
use klights_auth::{
    CredentialOperationError,
    worker_credential::{WorkerCsrBootstrapClient, validate_insecure_bootstrap_authentication},
};
use klights_leader_rpc::tls_policy::{LeaderTlsVerificationPolicy, ResolvedLeaderTlsVerification};
use klights_supervisor::TaskSupervisor;
use serde_json::json;

const CSR_WATCH_TIMEOUT_SECONDS: u64 = 5;
const CSR_EMPTY_WATCH_RETRIES: usize = 12;

pub(crate) struct HttpCsrBootstrapClient {
    client: reqwest::Client,
    base_url: String,
    token: String,
}

impl HttpCsrBootstrapClient {
    pub(crate) async fn new(
        leader_endpoint: String,
        token: String,
        ca_cert_path: Option<PathBuf>,
        skip_ca: bool,
        supervisor: Arc<TaskSupervisor>,
    ) -> Result<Self, CredentialOperationError> {
        let mut builder = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy();
        match LeaderTlsVerificationPolicy::new(ca_cert_path, skip_ca)
            .resolve(supervisor.as_ref())
            .await
            .map_err(|error| {
                CredentialOperationError::dependency_failure(format!(
                    "failed to resolve leader TLS verification: {error}"
                ))
            })? {
            ResolvedLeaderTlsVerification::CaPem(ca_pem) => {
                let certificate = reqwest::Certificate::from_pem(&ca_pem).map_err(|error| {
                    CredentialOperationError::rejected(format!(
                        "failed to parse leader CA certificate: {error}"
                    ))
                })?;
                builder = builder
                    .tls_built_in_root_certs(false)
                    .add_root_certificate(certificate);
            }
            ResolvedLeaderTlsVerification::SkipCa => {
                validate_insecure_bootstrap_authentication(true, &token)?;
                tracing::warn!(
                    leader_endpoint = %leader_endpoint,
                    security = "insecure-bootstrap",
                    "SECURITY: leader TLS CA verification is DISABLED for worker CSR bootstrap (skip-ca). The initial join is exposed to man-in-the-middle. Provide the leader CA certificate (or a CA hash pin) for a secure join; skip-ca should be used only on a trusted network."
                );
                builder = builder.danger_accept_invalid_certs(true);
            }
            ResolvedLeaderTlsVerification::SystemRoots => {}
        }

        let client = builder.build().map_err(|error| {
            CredentialOperationError::dependency_failure(format!(
                "failed to build CSR bootstrap HTTP client: {error}"
            ))
        })?;
        Ok(Self {
            client,
            base_url: normalize_api_endpoint(&leader_endpoint),
            token,
        })
    }

    fn csr_collection_url(&self) -> String {
        format!(
            "{}/apis/certificates.k8s.io/v1/certificatesigningrequests",
            self.base_url
        )
    }

    fn csr_named_url(&self, csr_name: &str) -> String {
        format!("{}/{}", self.csr_collection_url(), csr_name)
    }

    fn csr_watch_url(&self, csr_name: &str, resource_version: Option<&str>) -> String {
        let mut url = format!(
            "{}?watch=true&timeoutSeconds={CSR_WATCH_TIMEOUT_SECONDS}&fieldSelector=metadata.name%3D{csr_name}",
            self.csr_collection_url()
        );
        if let Some(resource_version) = resource_version.filter(|value| !value.is_empty()) {
            url.push_str("&resourceVersion=");
            url.push_str(resource_version);
        }
        url
    }

    async fn get_csr(&self, csr_name: &str) -> Result<serde_json::Value, CredentialOperationError> {
        self.client
            .get(self.csr_named_url(csr_name))
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|error| dependency(format!("failed to get CSR {csr_name}: {error}")))?
            .error_for_status()
            .map_err(|error| {
                dependency(format!("CSR {csr_name} get request was rejected: {error}"))
            })?
            .json::<serde_json::Value>()
            .await
            .map_err(|error| dependency(format!("failed to decode CSR {csr_name}: {error}")))
    }
}

#[async_trait]
impl WorkerCsrBootstrapClient for HttpCsrBootstrapClient {
    async fn submit_kubelet_client_csr(
        &self,
        csr: &klights_auth::kubelet_client_cert::KubeletClientCsr,
    ) -> Result<String, CredentialOperationError> {
        let body = json!({
            "apiVersion": "certificates.k8s.io/v1",
            "kind": "CertificateSigningRequest",
            "metadata": {"generateName": "klights-node-client-"},
            "spec": {
                "request": general_purpose::STANDARD.encode(&csr.csr_pem),
                "signerName": klights_auth::csr_policy::KUBELET_CLIENT_SIGNER_NAME,
                "usages": [klights_auth::csr_policy::KUBELET_CLIENT_AUTH_USAGE],
            }
        });
        let response = self
            .client
            .post(self.csr_collection_url())
            .bearer_auth(&self.token)
            .json(&body)
            .send()
            .await
            .map_err(|error| dependency(format!("failed to create CSR: {error}")))?
            .error_for_status()
            .map_err(|error| dependency(format!("CSR create request was rejected: {error}")))?
            .json::<serde_json::Value>()
            .await
            .map_err(|error| {
                dependency(format!("failed to decode CSR create response: {error}"))
            })?;

        response
            .pointer("/metadata/name")
            .and_then(|name| name.as_str())
            .filter(|name| !name.is_empty())
            .map(ToOwned::to_owned)
            .ok_or_else(|| {
                CredentialOperationError::rejected(
                    "CSR create response did not include metadata.name",
                )
            })
    }

    async fn wait_for_certificate(
        &self,
        csr_name: &str,
    ) -> Result<String, CredentialOperationError> {
        let mut empty_watch_closes = 0usize;
        loop {
            let current = self.get_csr(csr_name).await?;
            if let Some(certificate) = issued_certificate_pem(&current)? {
                return Ok(certificate);
            }
            let resource_version = current
                .pointer("/metadata/resourceVersion")
                .and_then(|value| value.as_str());
            let mut response = self
                .client
                .get(self.csr_watch_url(csr_name, resource_version))
                .bearer_auth(&self.token)
                .send()
                .await
                .map_err(|error| dependency(format!("failed to watch CSR {csr_name}: {error}")))?
                .error_for_status()
                .map_err(|error| {
                    dependency(format!(
                        "CSR {csr_name} watch request was rejected: {error}"
                    ))
                })?;
            let mut pending = Vec::new();
            let mut saw_relevant_event = false;
            while let Some(chunk) = response.chunk().await.map_err(|error| {
                dependency(format!(
                    "failed reading CSR {csr_name} watch stream: {error}"
                ))
            })? {
                pending.extend_from_slice(&chunk);
                while let Some(newline) = pending.iter().position(|byte| *byte == b'\n') {
                    let line: Vec<u8> = pending.drain(..=newline).collect();
                    let line = line.strip_suffix(b"\n").unwrap_or(&line);
                    let line = line.strip_suffix(b"\r").unwrap_or(line);
                    if line.is_empty() {
                        continue;
                    }
                    let event: serde_json::Value =
                        serde_json::from_slice(line).map_err(|error| {
                            CredentialOperationError::rejected(format!(
                                "failed to decode CSR watch event: {error}"
                            ))
                        })?;
                    let object = event.get("object").unwrap_or(&event);
                    if !csr_object_matches_name(object, csr_name) {
                        continue;
                    }
                    saw_relevant_event = true;
                    if let Some(certificate) = issued_certificate_pem(object)? {
                        return Ok(certificate);
                    }
                }
            }
            if saw_relevant_event {
                empty_watch_closes = 0;
            } else {
                empty_watch_closes += 1;
                if empty_watch_closes >= CSR_EMPTY_WATCH_RETRIES {
                    return Err(CredentialOperationError::dependency_failure(format!(
                        "CSR {csr_name} watch ended before certificate was issued"
                    )));
                }
            }
        }
    }
}

fn dependency(message: String) -> CredentialOperationError {
    CredentialOperationError::dependency_failure(message)
}

fn csr_object_matches_name(csr: &serde_json::Value, csr_name: &str) -> bool {
    csr.pointer("/metadata/name").and_then(|name| name.as_str()) == Some(csr_name)
}

fn issued_certificate_pem(
    csr: &serde_json::Value,
) -> Result<Option<String>, CredentialOperationError> {
    if let Some(encoded) = csr
        .pointer("/status/certificate")
        .and_then(|certificate| certificate.as_str())
        .filter(|certificate| !certificate.is_empty())
    {
        let pem = general_purpose::STANDARD.decode(encoded).map_err(|error| {
            CredentialOperationError::rejected(format!(
                "failed to decode issued CSR certificate: {error}"
            ))
        })?;
        return String::from_utf8(pem).map(Some).map_err(|error| {
            CredentialOperationError::rejected(format!(
                "issued CSR certificate was not valid UTF-8: {error}"
            ))
        });
    }
    if let Some(reason) = terminal_csr_condition(csr) {
        return Err(CredentialOperationError::rejected(format!(
            "CSR was not issued: {reason}"
        )));
    }
    Ok(None)
}

fn terminal_csr_condition(csr: &serde_json::Value) -> Option<String> {
    csr.pointer("/status/conditions")?
        .as_array()?
        .iter()
        .find_map(|condition| {
            let status = condition.get("status").and_then(|status| status.as_str());
            let kind = condition.get("type").and_then(|kind| kind.as_str());
            match (kind, status) {
                (Some("Denied" | "Failed"), Some("True")) => {
                    let reason = condition
                        .get("reason")
                        .and_then(|reason| reason.as_str())
                        .unwrap_or("unknown");
                    let message = condition
                        .get("message")
                        .and_then(|message| message.as_str())
                        .unwrap_or("");
                    Some(if message.is_empty() {
                        reason.to_string()
                    } else {
                        format!("{reason}: {message}")
                    })
                }
                _ => None,
            }
        })
}

fn normalize_api_endpoint(endpoint: &str) -> String {
    let trimmed = endpoint.trim().trim_end_matches('/');
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        trimmed.to_string()
    } else {
        format!("https://{trimmed}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn generate_tls_identity() -> (String, String, String) {
        let (ca_certificate, ca_key, ca_pem, _) =
            klights_auth::test_support::generate_ca_full_at(time::OffsetDateTime::now_utc())
                .expect("test CA");
        let server_key = rcgen::KeyPair::generate().expect("server key");
        let mut parameters = rcgen::CertificateParams::new(vec!["127.0.0.1".to_string()])
            .expect("server parameters");
        parameters.distinguished_name = rcgen::DistinguishedName::new();
        parameters
            .distinguished_name
            .push(rcgen::DnType::CommonName, "klights-server");
        let server_certificate = parameters
            .signed_by(&server_key, &ca_certificate, &ca_key)
            .expect("server certificate");
        (ca_pem, server_certificate.pem(), server_key.serialize_pem())
    }

    async fn start_tls_submit_server(
        certificate_pem: String,
        key_pem: String,
    ) -> (String, tokio::task::JoinHandle<()>) {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        let _ = rustls::crypto::ring::default_provider().install_default();
        let certificates = rustls_pemfile::certs(&mut certificate_pem.as_bytes())
            .collect::<Result<Vec<_>, _>>()
            .expect("server certificate chain");
        let key = rustls_pemfile::private_key(&mut key_pem.as_bytes())
            .expect("server key parse")
            .expect("server key");
        let config = Arc::new(
            rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
                .with_no_client_auth()
                .with_single_cert(certificates, key)
                .expect("server TLS config"),
        );
        let acceptor = tokio_rustls::TlsAcceptor::from(config);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test listener");
        let endpoint = format!(
            "https://{}",
            listener.local_addr().expect("listener address")
        );
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("client connection");
            let Ok(mut tls) = acceptor.accept(stream).await else {
                return;
            };
            let mut request = [0_u8; 8192];
            let _ = tls.read(&mut request).await;
            let body = br#"{"metadata":{"name":"worker-csr-test"}}"#;
            let response = format!(
                "HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                String::from_utf8_lossy(body)
            );
            tls.write_all(response.as_bytes()).await.expect("response");
        });
        (endpoint, server)
    }

    async fn submit_to_explicit_ca_server() {
        let directory = tempfile::tempdir().expect("temporary TLS root");
        let (ca_pem, server_certificate, server_key) = generate_tls_identity();
        let ca_path = directory.path().join("leader-ca.crt");
        std::fs::write(&ca_path, ca_pem).expect("write leader CA");
        let (endpoint, server) = start_tls_submit_server(server_certificate, server_key).await;
        let supervisor = Arc::new(TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let client = HttpCsrBootstrapClient::new(
            endpoint,
            "abcdef.0123456789abcdef".to_string(),
            Some(ca_path),
            false,
            supervisor.clone(),
        )
        .await
        .expect("explicit-CA client");
        let csr = klights_auth::kubelet_client_cert::generate_kubelet_client_csr("worker-a")
            .expect("test kubelet CSR");
        let csr_name = client
            .submit_kubelet_client_csr(&csr)
            .await
            .expect("submit CSR");
        assert_eq!(csr_name, "worker-csr-test");
        server.await.expect("TLS fixture");
        let _ = supervisor.shutdown(std::time::Duration::from_secs(1)).await;
    }

    fn assert_isolated_test_passed(
        output: std::process::Output,
        test_name: &str,
        failure_message: &str,
    ) {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        let expected = format!("test {test_name} ... ok");
        assert!(
            output.status.success() && stdout.contains(&expected),
            "{failure_message}\nstdout:\n{stdout}\nstderr:\n{stderr}"
        );
    }

    #[tokio::test]
    async fn known_ca_path_cannot_be_downgraded_by_skip_ca() {
        let directory = tempfile::tempdir().expect("temporary TLS root");
        let missing_ca = directory.path().join("missing-leader-ca.crt");
        let supervisor = Arc::new(TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let result = HttpCsrBootstrapClient::new(
            "https://leader:7679".to_string(),
            "abcdef.0123456789abcdef".to_string(),
            Some(missing_ca),
            true,
            supervisor.clone(),
        )
        .await;
        assert!(result.is_err(), "known CA path must fail closed");
        let error = result.err().expect("known CA failure");
        assert!(
            error
                .message()
                .contains("failed to read leader CA certificate")
        );
        let _ = supervisor.shutdown(std::time::Duration::from_secs(1)).await;
    }

    #[test]
    fn endpoint_normalization_preserves_explicit_scheme() {
        assert_eq!(
            normalize_api_endpoint("leader:7679/"),
            "https://leader:7679"
        );
        assert_eq!(
            normalize_api_endpoint("http://leader:7679/"),
            "http://leader:7679"
        );
    }

    #[test]
    fn csr_transport_parsing_rejects_terminal_conditions_and_malformed_certificates() {
        let denied = serde_json::json!({
            "metadata": {"name": "worker-csr"},
            "status": {"conditions": [{
                "type": "Denied",
                "status": "True",
                "reason": "PolicyDenied",
                "message": "worker scope required"
            }]}
        });
        assert!(csr_object_matches_name(&denied, "worker-csr"));
        assert!(!csr_object_matches_name(&denied, "other-csr"));
        let error = issued_certificate_pem(&denied).expect_err("terminal denial");
        assert!(
            error
                .message()
                .contains("PolicyDenied: worker scope required")
        );

        let malformed = serde_json::json!({"status": {"certificate": "%%%"}});
        assert!(issued_certificate_pem(&malformed).is_err());
    }

    #[tokio::test]
    async fn explicit_ca_ignores_same_subject_system_root() {
        const CHILD_MARKER: &str = "KLIGHTS_WORKER_CSR_CA_ISOLATION_CHILD";
        const TEST_NAME: &str = "bootstrap::composition_adapters::worker_csr_http_adapter::tests::explicit_ca_ignores_same_subject_system_root";
        if std::env::var_os(CHILD_MARKER).is_some() {
            submit_to_explicit_ca_server().await;
            return;
        }

        let directory = tempfile::tempdir().expect("temporary system-root fixture");
        let (_, _, stale_ca_pem, _) =
            klights_auth::test_support::generate_ca_full_at(time::OffsetDateTime::now_utc())
                .expect("stale CA");
        let stale_ca_path = directory.path().join("stale-system-ca.crt");
        let empty_ca_dir = directory.path().join("empty-ca-dir");
        std::fs::write(&stale_ca_path, stale_ca_pem).expect("write stale CA");
        std::fs::create_dir(&empty_ca_dir).expect("empty CA directory");
        let output = std::process::Command::new(std::env::current_exe().expect("test executable"))
            .args(["--exact", TEST_NAME, "--nocapture"])
            .env(CHILD_MARKER, "1")
            .env("SSL_CERT_FILE", &stale_ca_path)
            .env("SSL_CERT_DIR", &empty_ca_dir)
            .env("NO_PROXY", "127.0.0.1,localhost")
            .env("no_proxy", "127.0.0.1,localhost")
            .output()
            .expect("isolated test process");
        assert_isolated_test_passed(
            output,
            TEST_NAME,
            "explicit leader CA was shadowed by a same-subject system root",
        );
    }

    #[tokio::test]
    async fn client_connects_directly_despite_proxy_environment() {
        const CHILD_MARKER: &str = "KLIGHTS_WORKER_CSR_NO_PROXY_CHILD";
        const TEST_NAME: &str = "bootstrap::composition_adapters::worker_csr_http_adapter::tests::client_connects_directly_despite_proxy_environment";
        if std::env::var_os(CHILD_MARKER).is_some() {
            submit_to_explicit_ca_server().await;
            return;
        }
        let output = std::process::Command::new(std::env::current_exe().expect("test executable"))
            .args(["--exact", TEST_NAME, "--nocapture"])
            .env(CHILD_MARKER, "1")
            .env("HTTPS_PROXY", "http://127.0.0.1:9")
            .env("https_proxy", "http://127.0.0.1:9")
            .env("ALL_PROXY", "http://127.0.0.1:9")
            .env("all_proxy", "http://127.0.0.1:9")
            .env("NO_PROXY", "")
            .env("no_proxy", "")
            .output()
            .expect("isolated test process");
        assert_isolated_test_passed(
            output,
            TEST_NAME,
            "internal leader bootstrap must not use proxy environment variables",
        );
    }

    #[tokio::test]
    async fn wait_ignores_unrelated_events_and_rechecks_after_watch_close() {
        use axum::{
            body::Body,
            extract::{Path, State},
            http::Response,
            routing::get,
        };
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[derive(Clone)]
        struct WaitState {
            named_gets: Arc<AtomicUsize>,
            target_certificate: String,
            unrelated_certificate: String,
        }
        fn csr(name: &str, resource_version: &str, certificate: Option<&str>) -> serde_json::Value {
            let mut object = json!({
                "metadata": {"name": name, "resourceVersion": resource_version}
            });
            if let Some(certificate) = certificate {
                object["status"] = json!({"certificate": certificate});
            }
            object
        }
        async fn named(
            Path(name): Path<String>,
            State(state): State<WaitState>,
        ) -> axum::Json<serde_json::Value> {
            let call = state.named_gets.fetch_add(1, Ordering::SeqCst);
            let certificate = (call > 0).then_some(state.target_certificate.as_str());
            axum::Json(csr(&name, if call == 0 { "10" } else { "12" }, certificate))
        }
        async fn watch(State(state): State<WaitState>) -> Response<Body> {
            let event = json!({
                "type": "MODIFIED",
                "object": csr("other-csr", "11", Some(&state.unrelated_certificate))
            });
            let mut body = serde_json::to_vec(&event).expect("watch event");
            body.push(b'\n');
            Response::builder()
                .body(Body::from(body))
                .expect("watch response")
        }

        let target = "-----BEGIN CERTIFICATE-----\nTARGET\n-----END CERTIFICATE-----\n";
        let unrelated = "-----BEGIN CERTIFICATE-----\nOTHER\n-----END CERTIFICATE-----\n";
        let state = WaitState {
            named_gets: Arc::new(AtomicUsize::new(0)),
            target_certificate: general_purpose::STANDARD.encode(target.as_bytes()),
            unrelated_certificate: general_purpose::STANDARD.encode(unrelated.as_bytes()),
        };
        let named_gets = state.named_gets.clone();
        let app = axum::Router::new()
            .route(
                "/apis/certificates.k8s.io/v1/certificatesigningrequests/{name}",
                get(named),
            )
            .route(
                "/apis/certificates.k8s.io/v1/certificatesigningrequests",
                get(watch),
            )
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("watch listener");
        let base_url = format!("http://{}", listener.local_addr().expect("watch address"));
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        let client = HttpCsrBootstrapClient {
            client: reqwest::Client::builder()
                .no_proxy()
                .build()
                .expect("HTTP client"),
            base_url,
            token: "bootstrap-token".to_string(),
        };
        let certificate = client
            .wait_for_certificate("target-csr")
            .await
            .expect("issued target certificate");
        assert_eq!(certificate, target);
        assert!(named_gets.load(Ordering::SeqCst) >= 2);
        server.abort();
    }
}
