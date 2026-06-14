//! Webhook token authentication.
//!
//! Validates bearer tokens by calling an external webhook that returns a
//! Kubernetes-compatible `TokenReview` status.
//!
//! # OO design
//!
//! - `WebhookTokenReviewer` trait — sends TokenReview, returns status. Mockable.
//! - `HttpWebhookTokenReviewer` — production implementation using reqwest.
//! - `WebhookAuth` — cached authenticator wrapping the reviewer. Held in AppState.

use crate::api::AppError;
use crate::auth::identity::AuthenticatedIdentity;
use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

// ─── Configuration ─────────────────────────────────────────────────────────

/// Webhook configuration.
#[derive(Clone, Debug)]
pub struct WebhookAuthConfig {
    pub url: String,
    pub ca_bundle: Option<String>,
    pub client_cert: Option<String>,
    pub client_key: Option<String>,
    pub audiences: Vec<String>,
    pub cache_authorized_ttl_secs: u64,
    pub cache_unauthorized_ttl_secs: u64,
}

// ─── Domain types ──────────────────────────────────────────────────────────

/// Status from a TokenReview response.
#[derive(Clone, Debug)]
pub struct TokenReviewStatus {
    pub authenticated: bool,
    pub user: Option<TokenReviewUser>,
    /// Error message from the webhook (empty string = no error).
    pub error: Option<String>,
    /// Audiences accepted by the authenticator.
    pub audiences: Vec<String>,
}

/// User information from a TokenReview response.
#[derive(Clone, Debug)]
pub struct TokenReviewUser {
    pub username: String,
    pub uid: Option<String>,
    pub groups: Vec<String>,
    /// Extra key-value pairs (K8s `Extra` map unfolded to pairs).
    pub extra: Vec<(String, String)>,
}

// ─── Trait: WebhookTokenReviewer ───────────────────────────────────────────

/// Trait for calling an external webhook to validate a token.
/// Production uses HTTP; tests inject a mock.
#[async_trait::async_trait]
pub trait WebhookTokenReviewer: Send + Sync {
    async fn review_token(
        &self,
        token: &str,
        audiences: &[String],
    ) -> Result<Option<TokenReviewStatus>, String>;
}

// ─── Cached authenticator ──────────────────────────────────────────────────

const DEFAULT_WEBHOOK_AUTH_CACHE_CAPACITY: usize = 4096;
const DEFAULT_WEBHOOK_AUTH_AUDIENCE: &str = "https://kubernetes.default.svc.cluster.local";

/// Cached entry in the token review cache.
#[derive(Clone)]
struct CachedEntry {
    result: Result<Option<TokenReviewStatus>, String>,
    inserted_at: Instant,
}

#[derive(Default)]
struct WebhookAuthCache {
    entries: HashMap<[u8; 32], CachedEntry>,
    order: VecDeque<[u8; 32]>,
}

/// Webhook token authenticator with TTL cache.
pub struct WebhookAuth {
    reviewer: Arc<dyn WebhookTokenReviewer>,
    cache: Mutex<WebhookAuthCache>,
    cache_capacity: usize,
    authorized_ttl: Duration,
    unauthorized_ttl: Duration,
    audiences: Vec<String>,
}

impl WebhookAuth {
    pub fn new(
        reviewer: Arc<dyn WebhookTokenReviewer>,
        authorized_ttl: Duration,
        unauthorized_ttl: Duration,
        audiences: Vec<String>,
    ) -> Self {
        Self::new_with_cache_capacity(
            reviewer,
            authorized_ttl,
            unauthorized_ttl,
            audiences,
            DEFAULT_WEBHOOK_AUTH_CACHE_CAPACITY,
        )
    }

    pub fn new_with_cache_capacity(
        reviewer: Arc<dyn WebhookTokenReviewer>,
        authorized_ttl: Duration,
        unauthorized_ttl: Duration,
        audiences: Vec<String>,
        cache_capacity: usize,
    ) -> Self {
        Self {
            reviewer,
            cache: Mutex::new(WebhookAuthCache::default()),
            cache_capacity,
            authorized_ttl,
            unauthorized_ttl,
            audiences,
        }
    }

    /// Authenticate a bearer token. Returns `None` if the webhook returned
    /// authenticated=true but no user info (can't construct identity).
    /// Returns `Some(Err)` if not authenticated or webhook call failed.
    pub async fn authenticate(
        &self,
        token: &str,
    ) -> Option<Result<AuthenticatedIdentity, AppError>> {
        let cache_key = sha256_token(token);
        let now = Instant::now();

        // Check cache with lazy eviction
        {
            let mut cache = self.cache.lock().await;
            if let Some(entry) = cache.entries.get(&cache_key) {
                let ttl = if matches!(&entry.result, Ok(Some(s)) if s.authenticated) {
                    self.authorized_ttl
                } else {
                    self.unauthorized_ttl
                };
                if now.duration_since(entry.inserted_at) < ttl {
                    let result = entry.result.clone();
                    return self.convert_result(result);
                }
                remove_cache_entry(&mut cache, &cache_key);
            }
        }

        // Cache miss — call webhook
        let result = self.reviewer.review_token(token, &self.audiences).await;

        // If the result has audiences, validate intersection
        let result = if let Ok(Some(ref status)) = result {
            if status.authenticated && !audiences_compatible(&status.audiences, &self.audiences) {
                Err("webhook audiences don't intersect with server audiences".to_string())
            } else {
                Ok(Some(status.clone()))
            }
        } else {
            result
        };

        // Insert into cache
        {
            let mut cache = self.cache.lock().await;
            insert_cache_entry(
                &mut cache,
                cache_key,
                CachedEntry {
                    result: result.clone(),
                    inserted_at: now,
                },
                self.cache_capacity,
            );
        }

        self.convert_result(result)
    }

    fn convert_result(
        &self,
        result: Result<Option<TokenReviewStatus>, String>,
    ) -> Option<Result<AuthenticatedIdentity, AppError>> {
        match result {
            Ok(Some(status)) => {
                // K8s: error takes precedence over authenticated status
                // See staging/src/k8s.io/apiserver/plugin/pkg/authenticator/token/webhook/webhook.go
                if let Some(err) = status.error.filter(|e| !e.is_empty()) {
                    return Some(Err(AppError::Unauthorized(format!(
                        "webhook token review error: {err}"
                    ))));
                }
                if status.authenticated {
                    let user = status.user?;
                    let identity = AuthenticatedIdentity::webhook(
                        user.username,
                        user.groups,
                        user.uid,
                        user.extra,
                    );
                    return Some(Ok(identity));
                }
                Some(Err(AppError::Unauthorized(
                    "webhook token review: not authenticated".to_string(),
                )))
            }
            Ok(None) => Some(Err(AppError::Unauthorized(
                "webhook token review: no status returned".to_string(),
            ))),
            Err(err) => Some(Err(AppError::Unauthorized(format!(
                "webhook token review failed: {err}"
            )))),
        }
    }
}

fn remove_cache_entry(cache: &mut WebhookAuthCache, key: &[u8; 32]) {
    cache.entries.remove(key);
    cache.order.retain(|candidate| candidate != key);
}

fn insert_cache_entry(
    cache: &mut WebhookAuthCache,
    key: [u8; 32],
    entry: CachedEntry,
    capacity: usize,
) {
    if capacity == 0 {
        return;
    }

    if cache.entries.insert(key, entry).is_none() {
        cache.order.push_back(key);
    }

    while cache.entries.len() > capacity {
        let Some(oldest) = cache.order.pop_front() else {
            break;
        };
        cache.entries.remove(&oldest);
    }
}

fn sha256_token(token: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    let result = hasher.finalize();
    let mut key = [0u8; 32];
    key.copy_from_slice(&result);
    key
}

fn audiences_intersect(response_audiences: &[String], server_audiences: &[String]) -> bool {
    response_audiences
        .iter()
        .any(|a| server_audiences.contains(a))
}

fn audiences_compatible(response_audiences: &[String], server_audiences: &[String]) -> bool {
    if response_audiences.is_empty() {
        return server_audiences
            .iter()
            .any(|audience| audience == DEFAULT_WEBHOOK_AUTH_AUDIENCE);
    }
    audiences_intersect(response_audiences, server_audiences)
}

// ─── Builder ───────────────────────────────────────────────────────────────

/// Build a WebhookAuth from config. Returns None if webhook auth is not configured.
pub fn build_webhook_auth(config: Option<WebhookAuthConfig>) -> Result<Option<Arc<WebhookAuth>>> {
    let Some(config) = config else {
        return Ok(None);
    };
    if config.url.is_empty() {
        return Ok(None);
    }
    let url = reqwest::Url::parse(&config.url)
        .map_err(|err| anyhow!("invalid webhook auth URL {}: {err}", config.url))?;
    if url.scheme() != "https" {
        return Err(anyhow!("webhook auth URL must use https"));
    }

    let reviewer: Arc<dyn WebhookTokenReviewer> =
        Arc::new(HttpWebhookTokenReviewer::new(config.clone())?);

    let audiences: Vec<String> = if config.audiences.is_empty() {
        vec![DEFAULT_WEBHOOK_AUTH_AUDIENCE.to_string()]
    } else {
        config.audiences
    };

    Ok(Some(Arc::new(WebhookAuth::new(
        reviewer,
        Duration::from_secs(config.cache_authorized_ttl_secs.max(1)),
        Duration::from_secs(config.cache_unauthorized_ttl_secs.max(1)),
        audiences,
    ))))
}

pub async fn build_webhook_auth_from_config(
    config: &crate::KlightsConfig,
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
) -> Result<Option<Arc<WebhookAuth>>> {
    let Some(url) = config.webhook_auth_url.as_ref() else {
        return Ok(None);
    };

    let ca_bundle = read_optional_pem_file(
        task_supervisor,
        "webhook_auth_read_ca_bundle",
        "webhook auth CA bundle",
        config.webhook_auth_ca_bundle.as_ref(),
    )
    .await?;
    let client_cert = read_optional_pem_file(
        task_supervisor,
        "webhook_auth_read_client_cert",
        "webhook auth client certificate",
        config.webhook_auth_client_cert.as_ref(),
    )
    .await?;
    let client_key = read_optional_pem_file(
        task_supervisor,
        "webhook_auth_read_client_key",
        "webhook auth client key",
        config.webhook_auth_client_key.as_ref(),
    )
    .await?;

    build_webhook_auth(Some(WebhookAuthConfig {
        url: url.clone(),
        ca_bundle,
        client_cert,
        client_key,
        audiences: config
            .webhook_auth_audiences
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect(),
        cache_authorized_ttl_secs: config.webhook_auth_cache_authorized_ttl_secs,
        cache_unauthorized_ttl_secs: config.webhook_auth_cache_unauthorized_ttl_secs,
    }))
}

async fn read_optional_pem_file(
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
    label: &'static str,
    description: &'static str,
    path: Option<&String>,
) -> Result<Option<String>> {
    let Some(path) = path else {
        return Ok(None);
    };
    let path_buf = std::path::PathBuf::from(path);
    let key = path_buf.to_string_lossy().into_owned();
    let pem = task_supervisor
        .run_blocking_file_keyed(label, key, move || crate::utils::read_utf8_file(path_buf))
        .await
        .with_context(|| format!("failed to join {description} read"))?
        .with_context(|| format!("failed to read {description} {path}"))?;
    Ok(Some(pem))
}

/// Try webhook token auth. Returns `None` if not configured.
pub async fn try_webhook_auth(
    authenticator: &Option<Arc<WebhookAuth>>,
    token: &str,
) -> Option<Result<AuthenticatedIdentity, AppError>> {
    authenticator.as_ref()?.authenticate(token).await
}

// ─── Wire format (K8s authentication.k8s.io/v1) ────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct TokenReviewRequest {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub spec: TokenReviewSpec,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct TokenReviewSpec {
    pub token: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub audiences: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TokenReviewResponse {
    #[serde(rename = "apiVersion")]
    pub _api_version: Option<String>,
    #[serde(rename = "kind")]
    pub _kind: Option<String>,
    pub status: Option<TokenReviewStatusRaw>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TokenReviewStatusRaw {
    #[serde(default)]
    pub authenticated: bool,
    pub user: Option<TokenReviewUserRaw>,
    #[serde(default)]
    pub audiences: Option<Vec<String>>,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TokenReviewUserRaw {
    pub username: String,
    pub uid: Option<String>,
    #[serde(default)]
    pub groups: Vec<String>,
    #[serde(default)]
    pub extra: Option<HashMap<String, Vec<String>>>,
}

// ─── Production implementation ─────────────────────────────────────────────

/// HTTP-based webhook token reviewer.
pub struct HttpWebhookTokenReviewer {
    url: String,
    client: reqwest::Client,
}

impl HttpWebhookTokenReviewer {
    pub fn new(config: WebhookAuthConfig) -> Result<Self> {
        let mut builder = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .https_only(true)
            .no_proxy();

        let has_mtls = config.client_cert.is_some() && config.client_key.is_some();
        if config.client_cert.is_some() != config.client_key.is_some() {
            return Err(anyhow!(
                "webhook auth client certificate and key must both be configured for mTLS"
            ));
        }

        if has_mtls || config.ca_bundle.is_some() {
            // Build a native_tls::TlsConnector that includes both CA roots
            // and client identity (mTLS) as a single connector.
            let mut tls_builder = native_tls::TlsConnector::builder();

            if let Some(pem) = &config.ca_bundle {
                let certs = parse_pem_certs(pem);
                if certs.is_empty() {
                    return Err(anyhow!("webhook auth CA certificate bundle is empty"));
                }
                for cert in certs {
                    let cert = native_tls::Certificate::from_pem(cert.as_bytes())
                        .map_err(|e| anyhow!("failed to parse webhook auth CA certificate: {e}"))?;
                    tls_builder.add_root_certificate(cert);
                }
            }

            // Attach client certificate identity when available (mTLS).
            if let (Some(cert_pem), Some(key_pem)) = (&config.client_cert, &config.client_key) {
                let identity = build_native_tls_identity(cert_pem, key_pem)
                    .map_err(|e| anyhow!("failed to load webhook auth client identity: {e}"))?;
                tls_builder.identity(identity);
            }

            let tls = tls_builder
                .build()
                .map_err(|e| anyhow!("failed to build webhook auth TLS connector: {e}"))?;
            builder = builder.use_preconfigured_tls(tls);
        }

        let client = builder
            .build()
            .map_err(|e| anyhow!("failed to build webhook auth HTTP client: {e}"))?;
        Ok(Self {
            url: config.url,
            client,
        })
    }
}

#[async_trait::async_trait]
impl WebhookTokenReviewer for HttpWebhookTokenReviewer {
    async fn review_token(
        &self,
        token: &str,
        audiences: &[String],
    ) -> Result<Option<TokenReviewStatus>, String> {
        let request = TokenReviewRequest {
            api_version: "authentication.k8s.io/v1".to_string(),
            kind: "TokenReview".to_string(),
            spec: TokenReviewSpec {
                token: token.to_string(),
                audiences: audiences.to_vec(),
            },
        };

        let resp = self
            .client
            .post(&self.url)
            .timeout(std::time::Duration::from_secs(10))
            .json(&request)
            .send()
            .await
            .map_err(|e| format!("webhook request failed: {e}"))?;

        if !resp.status().is_success() {
            return Err(format!("webhook returned status {}", resp.status()));
        }

        let review: TokenReviewResponse = resp
            .json()
            .await
            .map_err(|e| format!("webhook response parse failed: {e}"))?;

        let Some(status) = review.status else {
            return Ok(None);
        };

        let user = status.user.map(|u| {
            let mut extra = Vec::new();
            if let Some(extra_map) = u.extra {
                for (key, values) in extra_map {
                    for val in values {
                        extra.push((key.clone(), val));
                    }
                }
            }
            TokenReviewUser {
                username: u.username,
                uid: u.uid,
                groups: u.groups,
                extra,
            }
        });

        Ok(Some(TokenReviewStatus {
            authenticated: status.authenticated,
            user,
            error: status.error.filter(|e| !e.is_empty()),
            audiences: status.audiences.unwrap_or_default(),
        }))
    }
}

// ─── mTLS helpers ──────────────────────────────────────────────────────────

/// Build a `native_tls::Identity` from PEM-encoded certificate and private key.
fn build_native_tls_identity(
    cert_pem: &str,
    key_pem: &str,
) -> Result<native_tls::Identity, String> {
    let cert_der = pem_to_der(cert_pem)?;
    let key_der = pem_to_der(key_pem)?;
    native_tls::Identity::from_pkcs8(&cert_der, &key_der)
        .map_err(|e| format!("failed to load client identity: {e}"))
}

/// Strip PEM headers and base64-decode to DER.
fn pem_to_der(pem: &str) -> Result<Vec<u8>, String> {
    use base64::Engine;
    let b64: String = pem.lines().filter(|l| !l.starts_with("-----")).collect();
    base64::engine::general_purpose::STANDARD
        .decode(b64.as_bytes())
        .map_err(|e| format!("failed to decode PEM: {e}"))
}

/// Split a PEM bundle into individual certificates.
fn parse_pem_certs(pem: &str) -> Vec<String> {
    let mut certs = Vec::new();
    let mut current = String::new();
    let mut in_cert = false;
    for line in pem.lines() {
        if line.starts_with("-----BEGIN CERTIFICATE-----") {
            in_cert = true;
            current.clear();
            current.push_str(line);
            current.push('\n');
        } else if line.starts_with("-----END CERTIFICATE-----") {
            current.push_str(line);
            current.push('\n');
            certs.push(current.clone());
            in_cert = false;
        } else if in_cert {
            current.push_str(line);
            current.push('\n');
        }
    }
    certs
}

// ─── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ─── Wire format tests ──────────────────────────────────────────────

    #[test]
    fn test_token_review_request_serialization_matches_k8s() {
        let request = TokenReviewRequest {
            api_version: "authentication.k8s.io/v1".to_string(),
            kind: "TokenReview".to_string(),
            spec: TokenReviewSpec {
                token: "my-bearer-token".to_string(),
                audiences: vec!["https://kubernetes.default.svc.cluster.local".to_string()],
            },
        };

        let json = serde_json::to_value(&request).expect("serialize should succeed");
        assert_eq!(json["apiVersion"], "authentication.k8s.io/v1");
        assert_eq!(json["kind"], "TokenReview");
        assert_eq!(json["spec"]["token"], "my-bearer-token");
        assert!(json["spec"]["audiences"].is_array());
    }

    #[test]
    fn test_token_review_request_audiences_omitted_when_empty() {
        let request = TokenReviewRequest {
            api_version: "authentication.k8s.io/v1".to_string(),
            kind: "TokenReview".to_string(),
            spec: TokenReviewSpec {
                token: "token".to_string(),
                audiences: vec![],
            },
        };

        let json = serde_json::to_value(&request).expect("serialize should succeed");
        // skip_serializing_if should omit empty audiences
        assert!(
            json["spec"].get("audiences").is_none()
                || json["spec"]["audiences"]
                    .as_array()
                    .map(|a| a.is_empty())
                    .unwrap_or(false),
            "empty audiences should be omitted or empty array"
        );
    }

    #[test]
    fn test_token_review_response_full_deserialization() {
        let json = serde_json::json!({
            "apiVersion": "authentication.k8s.io/v1",
            "kind": "TokenReview",
            "status": {
                "authenticated": true,
                "user": {
                    "username": "jane",
                    "uid": "abc-123",
                    "groups": ["system:masters", "developers"],
                    "extra": {
                        "org": ["engineering"],
                        "region": ["us-east-1", "us-west-2"]
                    }
                },
                "audiences": ["https://kubernetes.default.svc.cluster.local"],
                "error": ""
            }
        });

        let response: TokenReviewResponse =
            serde_json::from_value(json).expect("deserialize should succeed");
        let status = response.status.expect("should have status");
        assert!(status.authenticated);
        let user_raw = status.user.expect("should have user");
        assert_eq!(user_raw.username, "jane");
        assert_eq!(user_raw.uid, Some("abc-123".to_string()));
        assert_eq!(user_raw.groups, vec!["system:masters", "developers"]);
        let extra_map = user_raw.extra.expect("should have extra");
        assert_eq!(
            extra_map.get("org").unwrap(),
            &vec!["engineering".to_string()]
        );
        assert_eq!(
            status.audiences,
            Some(vec![
                "https://kubernetes.default.svc.cluster.local".to_string()
            ])
        );
        assert_eq!(status.error, Some("".to_string()));
    }

    #[test]
    fn test_token_review_response_not_authenticated() {
        let json = serde_json::json!({
            "apiVersion": "authentication.k8s.io/v1",
            "kind": "TokenReview",
            "status": {
                "authenticated": false,
                "user": null,
                "audiences": null,
                "error": null
            }
        });

        let response: TokenReviewResponse =
            serde_json::from_value(json).expect("deserialize should succeed");
        let status = response.status.expect("should have status");
        assert!(!status.authenticated);
        assert!(status.user.is_none());
    }

    #[test]
    fn test_token_review_response_with_error() {
        let json = serde_json::json!({
            "apiVersion": "authentication.k8s.io/v1",
            "kind": "TokenReview",
            "status": {
                "authenticated": false,
                "user": null,
                "audiences": null,
                "error": "token expired, please re-login"
            }
        });

        let response: TokenReviewResponse =
            serde_json::from_value(json).expect("deserialize should succeed");
        let status = response.status.expect("should have status");
        assert!(!status.authenticated);
        assert_eq!(
            status.error,
            Some("token expired, please re-login".to_string())
        );
    }

    #[test]
    fn test_token_review_response_v1beta1_compat() {
        let json = serde_json::json!({
            "apiVersion": "authentication.k8s.io/v1beta1",
            "kind": "TokenReview",
            "status": {
                "authenticated": true,
                "user": {
                    "username": "jane",
                    "uid": "uid-1",
                    "groups": ["developers"],
                    "extra": {}
                },
                "audiences": null,
                "error": null
            }
        });

        let response: TokenReviewResponse =
            serde_json::from_value(json).expect("should parse v1beta1 response");
        assert!(response.status.unwrap().authenticated);
    }

    #[test]
    fn test_token_review_status_minimal_fields() {
        let json = serde_json::json!({
            "apiVersion": "authentication.k8s.io/v1",
            "kind": "TokenReview",
            "status": {
                "authenticated": false
            }
        });

        let response: TokenReviewResponse =
            serde_json::from_value(json).expect("deserialize should succeed");
        let status = response.status.expect("should have status");
        assert!(!status.authenticated);
        assert!(status.user.is_none());
        assert!(status.error.is_none());
        assert!(status.audiences.unwrap_or_default().is_empty());
    }

    #[test]
    fn test_token_review_extra_single_value() {
        let json = serde_json::json!({
            "apiVersion": "authentication.k8s.io/v1",
            "kind": "TokenReview",
            "status": {
                "authenticated": true,
                "user": {
                    "username": "alice",
                    "uid": "uid-a",
                    "groups": [],
                    "extra": { "department": ["ops"] }
                }
            }
        });
        let resp: TokenReviewResponse =
            serde_json::from_value(json).expect("deserialize should succeed");
        let user_raw = resp.status.unwrap().user.unwrap();
        let extra = user_raw.extra.expect("should have extra");
        assert_eq!(extra.get("department").unwrap(), &vec!["ops".to_string()]);
    }

    #[test]
    fn test_token_review_extra_multi_value() {
        let json = serde_json::json!({
            "apiVersion": "authentication.k8s.io/v1",
            "kind": "TokenReview",
            "status": {
                "authenticated": true,
                "user": {
                    "username": "bob",
                    "uid": "uid-b",
                    "groups": [],
                    "extra": { "projects": ["alpha", "beta", "gamma"] }
                }
            }
        });
        let resp: TokenReviewResponse =
            serde_json::from_value(json).expect("deserialize should succeed");
        let user_raw = resp.status.unwrap().user.unwrap();
        let extra = user_raw.extra.expect("should have extra");
        assert_eq!(
            extra.get("projects").unwrap(),
            &vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()]
        );
    }
}
