//! OIDC token authentication.
//!
//! Validates JWT bearer tokens against an external OIDC provider (Keycloak,
//! Dex, Azure AD, etc.) using JWKS-based signature verification.
//!
//! # OO design
//!
//! - `OidcDiscovery` trait — fetches provider metadata and JWKS keys. Mockable.
//! - `OidcValidator` trait — validates a JWT and returns OIDC claims. Mockable.
//! - `HttpOidcDiscovery` — production implementation using reqwest.
//! - `JwtOidcValidator` — production implementation using jsonwebtoken.
//!
//! The middleware only depends on `OidcValidator`, so tests inject a mock.

use jsonwebtoken::Algorithm;
use klights_auth::AuthenticationError;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use time::OffsetDateTime;

/// OIDC provider metadata from `/.well-known/openid-configuration`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OidcProviderMetadata {
    pub issuer: String,
    pub jwks_uri: String,
    #[serde(default)]
    pub token_endpoint: Option<String>,
    #[serde(default)]
    pub authorization_endpoint: Option<String>,
}

/// JWKS key set from the OIDC provider's `jwks_uri`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JwkSet {
    pub keys: Vec<serde_json::Value>,
}

/// OIDC configuration for klights.
#[derive(Clone, Debug)]
pub struct OidcConfig {
    pub issuer_url: String,
    pub client_id: String,
    pub username_claim: String,
    pub username_prefix: Option<String>,
    pub groups_claim: String,
    pub groups_prefix: String,
    pub ca_bundle: Option<String>,
    pub signing_algs: Vec<Algorithm>,
}

/// Extracted OIDC claims after successful token validation.
#[derive(Clone, Debug)]
pub struct OidcClaims {
    pub username: String,
    pub groups: Vec<String>,
    pub uid: Option<String>,
    /// Token audiences actually validated by this authenticator.
    pub audiences: Vec<String>,
}

/// Trait for fetching OIDC provider metadata and JWKS keys.
/// Production uses HTTP; tests inject a mock.
#[async_trait::async_trait]
pub trait OidcDiscovery: Send + Sync {
    async fn fetch_discovery(
        &self,
        issuer_url: &str,
    ) -> Result<OidcProviderMetadata, AuthenticationError>;
    async fn fetch_jwks(&self, jwks_uri: &str) -> Result<JwkSet, AuthenticationError>;
}

/// Trait for validating an OIDC JWT and extracting claims.
/// Production uses JWKS + jsonwebtoken; tests inject a mock.
#[async_trait::async_trait]
pub trait OidcValidator: Send + Sync {
    async fn validate_token(
        &self,
        token: &str,
        now: OffsetDateTime,
    ) -> Result<OidcClaims, AuthenticationError>;
}

/// Validate and default focused OIDC policy configuration.
///
/// Root composition owns concrete discovery-client and validator selection
/// after this transport-neutral configuration step succeeds.
pub fn prepare_oidc_config(config: Option<OidcConfig>) -> Option<OidcConfig> {
    defaulted_config(config?)
}

pub fn default_signing_algs() -> Vec<Algorithm> {
    vec![Algorithm::RS256]
}

fn is_https_url(url: &str) -> bool {
    reqwest::Url::parse(url)
        .map(|parsed| parsed.scheme() == "https")
        .unwrap_or(false)
}

fn validate_jwks_uri(jwks_uri: &str) -> Result<(), String> {
    if is_https_url(jwks_uri) {
        Ok(())
    } else {
        Err("OIDC JWKS URI must use https".to_string())
    }
}

fn username_prefix(config: &OidcConfig) -> &str {
    match config.username_prefix.as_deref() {
        Some(prefix) => prefix,
        None if config.username_claim == "email" => "",
        None => &config.issuer_url,
    }
}

fn apply_username_prefix(config: &OidcConfig, username: &str) -> String {
    let prefix = username_prefix(config);
    if prefix.is_empty() {
        username.to_string()
    } else if config.username_prefix.is_some() {
        format!("{prefix}{username}")
    } else {
        format!("{prefix}#{username}")
    }
}

fn numeric_claim(claims: &serde_json::Value, claim: &str) -> Result<Option<i64>, String> {
    match claims.get(claim) {
        Some(value) => value
            .as_i64()
            .or_else(|| value.as_u64().and_then(|v| i64::try_from(v).ok()))
            .map(Some)
            .ok_or_else(|| format!("OIDC token claim '{claim}' must be a numeric timestamp")),
        None => Ok(None),
    }
}

fn validate_time_claims(claims: &serde_json::Value, now: OffsetDateTime) -> Result<(), String> {
    let now = now.unix_timestamp();
    let exp = numeric_claim(claims, "exp")?
        .ok_or_else(|| "OIDC token missing required exp claim".to_string())?;
    if exp <= now {
        return Err("OIDC token has expired".to_string());
    }
    if let Some(nbf) = numeric_claim(claims, "nbf")?
        && nbf > now
    {
        return Err("OIDC token nbf is in the future".to_string());
    }
    Ok(())
}

fn validate_alg(config: &OidcConfig, alg: Algorithm) -> Result<(), String> {
    if config.signing_algs.contains(&alg) {
        Ok(())
    } else {
        Err(format!("OIDC token algorithm {alg:?} is not accepted"))
    }
}

fn validate_configured_issuer(config: &OidcConfig) -> Result<(), String> {
    if config.issuer_url.is_empty() || config.client_id.is_empty() {
        return Err("OIDC issuer URL and client ID are required".to_string());
    }
    if !is_https_url(&config.issuer_url) {
        return Err("OIDC issuer URL must use https".to_string());
    }
    if config.signing_algs.is_empty() {
        return Err("OIDC signing algorithm allowlist cannot be empty".to_string());
    }
    Ok(())
}

fn extract_groups(config: &OidcConfig, claims: &serde_json::Value) -> Vec<String> {
    match claims.get(&config.groups_claim) {
        Some(value) if value.is_array() => value
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|v| v.as_str().map(|s| format!("{}{}", config.groups_prefix, s)))
            .collect(),
        Some(value) => value
            .as_str()
            .map(|s| vec![format!("{}{}", config.groups_prefix, s)])
            .unwrap_or_default(),
        None => Vec::new(),
    }
}

fn oidc_http_client(ca_bundle: Option<&str>) -> Result<reqwest::Client, String> {
    let mut builder = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .https_only(true);
    if let Some(pem) = ca_bundle {
        let cert = reqwest::Certificate::from_pem(pem.as_bytes())
            .map_err(|e| format!("invalid OIDC CA bundle: {e}"))?;
        builder = builder.add_root_certificate(cert);
    }
    builder
        .build()
        .map_err(|e| format!("failed to build OIDC HTTP client: {e}"))
}

fn usable_jwks_keys(jwks: &JwkSet) -> Result<Vec<(String, jsonwebtoken::DecodingKey)>, String> {
    let mut keys = Vec::new();
    for key_value in &jwks.keys {
        let kid = key_value
            .get("kid")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if let Ok(rsa_n) = key_value.get("n").and_then(|v| v.as_str()).ok_or("")
            && let Ok(rsa_e) = key_value.get("e").and_then(|v| v.as_str()).ok_or("")
            && let Ok(decoding_key) = jsonwebtoken::DecodingKey::from_rsa_components(rsa_n, rsa_e)
        {
            keys.push((kid.clone(), decoding_key));
            continue;
        }
        if let Some(x5c) = key_value.get("x5c").and_then(|v| v.as_array())
            && let Some(cert_b64) = x5c.first().and_then(|v| v.as_str())
        {
            use base64::Engine;
            if let Ok(der) = base64::engine::general_purpose::STANDARD.decode(cert_b64)
                && let Ok(pem) = x509_pubkey_to_pem(&der)
            {
                if let Ok(dk) = jsonwebtoken::DecodingKey::from_rsa_pem(pem.as_bytes()) {
                    keys.push((kid.clone(), dk));
                    continue;
                }
                if let Ok(dk) = jsonwebtoken::DecodingKey::from_ec_pem(pem.as_bytes()) {
                    keys.push((kid.clone(), dk));
                    continue;
                }
            }
        }
    }
    if keys.is_empty() {
        return Err("no usable signing keys found in OIDC JWKS".to_string());
    }
    Ok(keys)
}

fn defaulted_config(mut config: OidcConfig) -> Option<OidcConfig> {
    if config.signing_algs.is_empty() {
        config.signing_algs = default_signing_algs();
    }
    if validate_configured_issuer(&config).is_err() {
        return None;
    }
    Some(config)
}

/// Validate an OIDC bearer token against the configured authenticator.
/// Returns `None` if no OIDC authenticator is configured (caller should try
/// the next auth method). Returns `Some(Err)` if OIDC was tried and failed.
pub async fn try_oidc_auth(
    authenticator: Option<&dyn OidcValidator>,
    token: &str,
    clock: &dyn klights_auth::clock::Clock,
) -> Option<Result<klights_auth::AuthenticatedIdentity, AuthenticationError>> {
    try_oidc_auth_with_audiences(authenticator, token, clock)
        .await
        .map(|result| result.map(|(identity, _audiences)| identity))
}

async fn try_oidc_auth_with_audiences(
    authenticator: Option<&dyn OidcValidator>,
    token: &str,
    clock: &dyn klights_auth::clock::Clock,
) -> Option<Result<(klights_auth::AuthenticatedIdentity, Vec<String>), AuthenticationError>> {
    let authenticator = authenticator?;
    let now = clock.now();
    match authenticator.validate_token(token, now).await {
        Ok(claims) => {
            let identity = klights_auth::AuthenticatedIdentity::oidc(
                claims.username,
                claims.groups,
                claims.uid,
            );
            Some(Ok((identity, claims.audiences)))
        }
        Err(error) => Some(Err(match error {
            AuthenticationError::Unauthenticated { message } => {
                AuthenticationError::unauthenticated(format!("invalid OIDC token: {message}"))
            }
            AuthenticationError::DependencyFailure { message } => {
                AuthenticationError::dependency_failure(format!(
                    "OIDC dependency failed: {message}"
                ))
            }
            AuthenticationError::InternalFailure { message } => {
                AuthenticationError::internal_failure(format!(
                    "OIDC verification failed internally: {message}"
                ))
            }
        })),
    }
}

pub async fn try_oidc_auth_for_review(
    authenticator: Option<&dyn OidcValidator>,
    token: &str,
    clock: &dyn klights_auth::clock::Clock,
    requested_audiences: &[String],
) -> Option<Result<(klights_auth::AuthenticatedIdentity, Vec<String>), AuthenticationError>> {
    const DEFAULT_API_AUDIENCE: &str = "https://kubernetes.default.svc.cluster.local";
    let result = try_oidc_auth_with_audiences(authenticator, token, clock).await?;
    Some(result.and_then(|(identity, _client_id_audiences)| {
        // OIDC's JWT audience validates the configured client ID. Kubernetes
        // then wraps the authenticator as audience-agnostic for API audiences.
        // This API server exposes its default audience independently of that
        // OIDC client ID.
        if requested_audiences.is_empty() {
            return Ok((identity, Vec::new()));
        }
        let audiences = requested_audiences
            .iter()
            .filter(|audience| audience.as_str() == DEFAULT_API_AUDIENCE)
            .cloned()
            .collect::<Vec<_>>();
        if audiences.is_empty() {
            Err(AuthenticationError::unauthenticated(
                "OIDC authenticator audiences do not intersect TokenReview audiences",
            ))
        } else {
            Ok((identity, audiences))
        }
    }))
}

// ─── Production implementations ────────────────────────────────────────────

/// HTTP-based OIDC discovery using reqwest.
pub struct HttpOidcDiscovery {
    client: reqwest::Client,
}

impl HttpOidcDiscovery {
    pub fn new(ca_bundle: Option<String>) -> Result<Self, String> {
        Ok(Self {
            client: oidc_http_client(ca_bundle.as_deref())?,
        })
    }
}

#[async_trait::async_trait]
impl OidcDiscovery for HttpOidcDiscovery {
    async fn fetch_discovery(
        &self,
        issuer_url: &str,
    ) -> Result<OidcProviderMetadata, AuthenticationError> {
        if !is_https_url(issuer_url) {
            return Err(AuthenticationError::internal_failure(
                "OIDC issuer URL must use https",
            ));
        }
        let url = format!(
            "{}/.well-known/openid-configuration",
            issuer_url.trim_end_matches('/')
        );
        let resp = self
            .client
            .get(&url)
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
            .map_err(|e| {
                AuthenticationError::dependency_failure(format!(
                    "OIDC discovery request failed: {e}"
                ))
            })?;
        if !resp.status().is_success() {
            return Err(AuthenticationError::dependency_failure(format!(
                "OIDC discovery returned status {}",
                resp.status()
            )));
        }
        resp.json::<OidcProviderMetadata>().await.map_err(|e| {
            AuthenticationError::dependency_failure(format!("OIDC discovery parse failed: {e}"))
        })
    }

    async fn fetch_jwks(&self, jwks_uri: &str) -> Result<JwkSet, AuthenticationError> {
        validate_jwks_uri(jwks_uri).map_err(AuthenticationError::internal_failure)?;
        let resp = self
            .client
            .get(jwks_uri)
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
            .map_err(|e| {
                AuthenticationError::dependency_failure(format!("OIDC JWKS request failed: {e}"))
            })?;
        if !resp.status().is_success() {
            return Err(AuthenticationError::dependency_failure(format!(
                "OIDC JWKS returned status {}",
                resp.status()
            )));
        }
        resp.json::<JwkSet>().await.map_err(|e| {
            AuthenticationError::dependency_failure(format!("OIDC JWKS parse failed: {e}"))
        })
    }
}

/// Extract the SubjectPublicKeyInfo PEM from a DER-encoded X.509 certificate.
fn x509_pubkey_to_pem(cert_der: &[u8]) -> Result<String, String> {
    use base64::Engine;
    use x509_parser::prelude::*;
    let (_, cert) =
        X509Certificate::from_der(cert_der).map_err(|e| format!("x5c cert parse failed: {e}"))?;
    let spki = cert.public_key();
    let raw = spki.raw;
    // Re-wrap as PEM
    let b64 = base64::engine::general_purpose::STANDARD.encode(raw);
    Ok(format!(
        "-----BEGIN PUBLIC KEY-----\n{b64}\n-----END PUBLIC KEY-----\n"
    ))
}

/// JWT-based OIDC token validator.
type ResolvedOidcKeys = Arc<Vec<(String, jsonwebtoken::DecodingKey)>>;

pub struct JwtOidcValidator {
    config: OidcConfig,
    discovery: Box<dyn OidcDiscovery>,
    keys: tokio::sync::RwLock<Option<ResolvedOidcKeys>>,
    supervisor: Arc<klights_supervisor::TaskSupervisor>,
}

impl JwtOidcValidator {
    pub fn new(
        config: OidcConfig,
        discovery: Box<dyn OidcDiscovery>,
        supervisor: Arc<klights_supervisor::TaskSupervisor>,
    ) -> Self {
        let config = defaulted_config(config).unwrap_or(OidcConfig {
            issuer_url: String::new(),
            client_id: String::new(),
            username_claim: "sub".to_string(),
            username_prefix: None,
            groups_claim: "groups".to_string(),
            groups_prefix: String::new(),
            ca_bundle: None,
            signing_algs: default_signing_algs(),
        });
        Self {
            config,
            discovery,
            keys: tokio::sync::RwLock::new(None),
            supervisor,
        }
    }

    /// Resolve the signing keys from the OIDC provider.
    /// Returns a vec of (kid, decoding_key) pairs.
    async fn resolve_keys(&self) -> Result<ResolvedOidcKeys, AuthenticationError> {
        validate_configured_issuer(&self.config).map_err(AuthenticationError::internal_failure)?;
        if let Some(keys) = self.keys.read().await.as_ref().cloned() {
            return Ok(keys);
        }
        let mut guard = self.keys.write().await;
        if let Some(keys) = guard.as_ref().cloned() {
            return Ok(keys);
        }
        let metadata = self
            .discovery
            .fetch_discovery(&self.config.issuer_url)
            .await?;
        if metadata.issuer != self.config.issuer_url {
            return Err(AuthenticationError::dependency_failure(format!(
                "OIDC discovery issuer '{}' does not match configured issuer '{}'",
                metadata.issuer, self.config.issuer_url
            )));
        }
        validate_jwks_uri(&metadata.jwks_uri).map_err(AuthenticationError::dependency_failure)?;
        let jwks = self.discovery.fetch_jwks(&metadata.jwks_uri).await?;
        let keys = Arc::new(
            self.supervisor
                .run_blocking(
                    klights_supervisor::TaskCategory::Others,
                    "resolve-oidc-signing-keys",
                    move || usable_jwks_keys(&jwks),
                )
                .await
                .map_err(|error| {
                    AuthenticationError::internal_failure(format!(
                        "supervised OIDC key parsing failed: {error}"
                    ))
                })?
                .map_err(|error| {
                    AuthenticationError::dependency_failure(format!(
                        "OIDC signing keys are invalid: {error}"
                    ))
                })?,
        );
        *guard = Some(keys.clone());
        Ok(keys)
    }
}

#[async_trait::async_trait]
impl OidcValidator for JwtOidcValidator {
    async fn validate_token(
        &self,
        token: &str,
        now: OffsetDateTime,
    ) -> Result<OidcClaims, AuthenticationError> {
        let keys = self.resolve_keys().await?;
        let token = token.to_string();
        let config = self.config.clone();
        self.supervisor
            .run_blocking(
                klights_supervisor::TaskCategory::Others,
                "verify-oidc-token",
                move || validate_token_with_keys(&config, keys.as_ref(), &token, now),
            )
            .await
            .map_err(|error| {
                AuthenticationError::internal_failure(format!(
                    "supervised OIDC verification failed: {error}"
                ))
            })?
            .map_err(|error| {
                AuthenticationError::unauthenticated(format!("invalid OIDC token: {error}"))
            })
    }
}

fn validate_token_with_keys(
    config: &OidcConfig,
    keys: &[(String, jsonwebtoken::DecodingKey)],
    token: &str,
    now: OffsetDateTime,
) -> Result<OidcClaims, String> {
    // Extract the kid from the JWT header to select the right key
    let header = jsonwebtoken::decode_header(token)
        .map_err(|e| format!("OIDC token header parse failed: {e}"))?;
    validate_alg(config, header.alg)?;
    let target_kid = header.kid.as_deref().unwrap_or("");

    // Try matching key first, then all keys as fallback
    let mut attempts = Vec::new();
    for (kid, decoding_key) in keys {
        if kid == target_kid || target_kid.is_empty() {
            attempts.push(decoding_key);
        }
    }
    // Fallback: try all keys if no kid match
    if attempts.is_empty() {
        for (_, decoding_key) in keys {
            attempts.push(decoding_key);
        }
    }

    let mut last_err = String::new();
    for decoding_key in attempts {
        let mut validation = jsonwebtoken::Validation::new(config.signing_algs[0]);
        validation.algorithms = config.signing_algs.clone();
        validation.set_issuer(&[&config.issuer_url]);
        validation.set_audience(&[&config.client_id]);
        validation.validate_exp = false;
        validation.validate_nbf = false;
        validation.set_required_spec_claims(&["exp", "iss", "aud"]);
        match jsonwebtoken::decode::<serde_json::Value>(token, decoding_key, &validation) {
            Ok(token_data) => {
                let claims = &token_data.claims;
                validate_time_claims(claims, now)?;

                // Extract username
                let raw_username = claims
                    .get(&config.username_claim)
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if raw_username.is_empty() {
                    return Err(format!(
                        "OIDC token missing username claim '{}'",
                        config.username_claim
                    ));
                }
                let username = apply_username_prefix(config, raw_username);

                // Extract groups
                let groups = extract_groups(config, claims);

                // Extract uid (optional — use `sub` as fallback)
                let uid = claims
                    .get("sub")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());

                return Ok(OidcClaims {
                    username,
                    groups,
                    uid,
                    audiences: vec![config.client_id.clone()],
                });
            }
            Err(e) => {
                last_err = format!("{e}");
            }
        }
    }

    Err(format!(
        "OIDC token validation failed against all keys: {last_err}"
    ))
}
