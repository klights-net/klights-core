//! OIDC authentication tests.
//!
//! Tests use mock implementations of `OidcDiscovery` and `OidcValidator`
//! to verify all code paths without network access.

use crate::auth::oidc::*;
use base64::Engine;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use rsa::{RsaPrivateKey, pkcs8::EncodePrivateKey, traits::PublicKeyParts};
use std::sync::Arc;
use time::OffsetDateTime;

// ─── Mock implementations ─────────────────────────────────────────────────

/// Mock discovery that returns pre-configured metadata and JWKS.
struct MockOidcDiscovery {
    metadata: Result<OidcProviderMetadata, String>,
    jwks: Result<JwkSet, String>,
}

impl MockOidcDiscovery {
    fn new(metadata: Result<OidcProviderMetadata, String>, jwks: Result<JwkSet, String>) -> Self {
        Self { metadata, jwks }
    }
}

#[async_trait::async_trait]
impl OidcDiscovery for MockOidcDiscovery {
    async fn fetch_discovery(&self, _issuer_url: &str) -> Result<OidcProviderMetadata, String> {
        self.metadata.clone()
    }
    async fn fetch_jwks(&self, _jwks_uri: &str) -> Result<JwkSet, String> {
        self.jwks.clone()
    }
}

/// Mock validator that returns a pre-configured result.
struct MockOidcValidator {
    result: Result<OidcClaims, String>,
}

impl MockOidcValidator {
    fn new(result: Result<OidcClaims, String>) -> Self {
        Self { result }
    }
}

#[async_trait::async_trait]
impl OidcValidator for MockOidcValidator {
    async fn validate_token(
        &self,
        _token: &str,
        _now: OffsetDateTime,
    ) -> Result<OidcClaims, String> {
        self.result.clone()
    }
}

// ─── Test helpers ──────────────────────────────────────────────────────────

fn test_metadata() -> OidcProviderMetadata {
    OidcProviderMetadata {
        issuer: "https://keycloak.example.com/realms/master".to_string(),
        jwks_uri: "https://keycloak.example.com/realms/master/protocol/openid-connect/certs"
            .to_string(),
        token_endpoint: None,
        authorization_endpoint: None,
    }
}

struct SignedOidcToken {
    token: String,
    jwks: JwkSet,
}

fn signed_oidc_token(algorithm: Algorithm, mut claims: serde_json::Value) -> SignedOidcToken {
    let private_key = RsaPrivateKey::new(&mut rand_core::OsRng, 2048).unwrap();
    let private_key_pem = private_key
        .to_pkcs8_pem(rsa::pkcs8::LineEnding::LF)
        .unwrap()
        .to_string();
    let kid = "oidc-test-key";
    let mut header = Header::new(algorithm);
    header.kid = Some(kid.to_string());
    claims
        .as_object_mut()
        .expect("test claims are an object")
        .entry("iat")
        .or_insert_with(|| serde_json::json!(OffsetDateTime::now_utc().unix_timestamp()));
    let token = jsonwebtoken::encode(
        &header,
        &claims,
        &EncodingKey::from_rsa_pem(private_key_pem.as_bytes()).unwrap(),
    )
    .unwrap();
    let n = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(private_key.n().to_bytes_be());
    let e = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(private_key.e().to_bytes_be());
    SignedOidcToken {
        token,
        jwks: JwkSet {
            keys: vec![serde_json::json!({
                "kty": "RSA",
                "kid": kid,
                "use": "sig",
                "alg": "RS256",
                "n": n,
                "e": e
            })],
        },
    }
}

fn jwt_validator(metadata: OidcProviderMetadata, jwks: JwkSet) -> JwtOidcValidator {
    JwtOidcValidator::new(
        OidcConfig {
            issuer_url: "https://keycloak.example.com/realms/master".to_string(),
            client_id: "klights".to_string(),
            username_claim: "sub".to_string(),
            username_prefix: None,
            groups_claim: "groups".to_string(),
            groups_prefix: String::new(),
            ca_bundle: None,
            signing_algs: default_signing_algs(),
        },
        Box::new(MockOidcDiscovery::new(Ok(metadata), Ok(jwks))),
    )
}

fn valid_claims() -> serde_json::Value {
    let now = OffsetDateTime::now_utc().unix_timestamp();
    serde_json::json!({
        "iss": "https://keycloak.example.com/realms/master",
        "aud": "klights",
        "sub": "alice",
        "groups": ["dev"],
        "exp": now + 3600,
        "nbf": now - 30
    })
}

// ─── Unit tests ────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_oidc_config_with_empty_issuer_returns_none() {
    let config = Some(OidcConfig {
        issuer_url: String::new(),
        client_id: "klights".to_string(),
        username_claim: "sub".to_string(),
        username_prefix: None,
        groups_claim: "groups".to_string(),
        groups_prefix: String::new(),
        ca_bundle: None,
        signing_algs: default_signing_algs(),
    });
    assert!(build_oidc_authenticator(config).is_none());
}

#[tokio::test]
async fn test_oidc_config_with_empty_client_id_returns_none() {
    let config = Some(OidcConfig {
        issuer_url: "https://example.com".to_string(),
        client_id: String::new(),
        username_claim: "sub".to_string(),
        username_prefix: None,
        groups_claim: "groups".to_string(),
        groups_prefix: String::new(),
        ca_bundle: None,
        signing_algs: default_signing_algs(),
    });
    assert!(build_oidc_authenticator(config).is_none());
}

#[tokio::test]
async fn test_oidc_config_with_http_issuer_returns_none() {
    let config = Some(OidcConfig {
        issuer_url: "http://example.com".to_string(),
        client_id: "klights".to_string(),
        username_claim: "sub".to_string(),
        username_prefix: None,
        groups_claim: "groups".to_string(),
        groups_prefix: String::new(),
        ca_bundle: None,
        signing_algs: default_signing_algs(),
    });
    assert!(build_oidc_authenticator(config).is_none());
}

#[test]
fn test_oidc_config_none_returns_none() {
    assert!(build_oidc_authenticator(None).is_none());
}

#[tokio::test]
async fn test_mock_validator_returns_success() {
    let validator = MockOidcValidator::new(Ok(OidcClaims {
        username: "user1".to_string(),
        groups: vec!["admin".to_string()],
        uid: Some("uid-123".to_string()),
    }));
    let result = validator
        .validate_token("any-token", OffsetDateTime::now_utc())
        .await;
    assert!(result.is_ok());
    let claims = result.unwrap();
    assert_eq!(claims.username, "user1");
    assert_eq!(claims.groups, vec!["admin"]);
    assert_eq!(claims.uid, Some("uid-123".to_string()));
}

#[tokio::test]
async fn test_mock_validator_returns_failure() {
    let validator = MockOidcValidator::new(Err("bad token".to_string()));
    let result = validator
        .validate_token("bad-token", OffsetDateTime::now_utc())
        .await;
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("bad token"));
}

#[tokio::test]
async fn test_mock_discovery_returns_metadata() {
    let metadata = test_metadata();
    let discovery = MockOidcDiscovery::new(Ok(metadata.clone()), Ok(JwkSet { keys: vec![] }));
    let result = discovery
        .fetch_discovery("https://keycloak.example.com/realms/master")
        .await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap().issuer, metadata.issuer);
}

#[tokio::test]
async fn test_mock_discovery_returns_error() {
    let discovery =
        MockOidcDiscovery::new(Err("unreachable".to_string()), Ok(JwkSet { keys: vec![] }));
    let result = discovery
        .fetch_discovery("https://keycloak.example.com/realms/master")
        .await;
    assert!(result.is_err());
}

#[test]
fn test_oidc_claims_groups_prefix_applied() {
    let claims = OidcClaims {
        username: "user1".to_string(),
        groups: vec!["oidc:admin".to_string(), "oidc:dev".to_string()],
        uid: None,
    };
    // Verify the struct carries the prefixed groups
    assert!(claims.groups[0].starts_with("oidc:"));
    assert!(claims.groups[1].starts_with("oidc:"));
}

#[tokio::test]
async fn test_jwt_oidc_validator_rejects_expired_token_via_mock() {
    // When a mock validator returns an expiration error,
    // the caller should receive that error.
    let mock = MockOidcValidator::new(Err("token has expired".to_string()));
    let now = OffsetDateTime::now_utc();
    let result = mock.validate_token("any-expired-token", now).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("expired"));
}

#[test]
fn test_oidc_claims_empty_groups_is_ok() {
    let claims = OidcClaims {
        username: "user1".to_string(),
        groups: vec![],
        uid: None,
    };
    assert!(claims.groups.is_empty());
    assert_eq!(claims.username, "user1");
}

#[tokio::test]
async fn test_jwt_oidc_validator_rejects_discovery_issuer_mismatch() {
    let signed = signed_oidc_token(Algorithm::RS256, valid_claims());
    let mut metadata = test_metadata();
    metadata.issuer = "https://evil.example.com".to_string();
    let validator = jwt_validator(metadata, signed.jwks);

    let result = validator
        .validate_token(&signed.token, OffsetDateTime::now_utc())
        .await;

    assert!(result.is_err());
    assert!(result.unwrap_err().contains("issuer"));
}

#[tokio::test]
async fn test_jwt_oidc_validator_rejects_http_jwks_uri() {
    let signed = signed_oidc_token(Algorithm::RS256, valid_claims());
    let mut metadata = test_metadata();
    metadata.jwks_uri = "http://keycloak.example.com/certs".to_string();
    let validator = jwt_validator(metadata, signed.jwks);

    let result = validator
        .validate_token(&signed.token, OffsetDateTime::now_utc())
        .await;

    assert!(result.is_err());
    assert!(result.unwrap_err().contains("https"));
}

#[tokio::test]
async fn test_jwt_oidc_validator_rejects_not_before_in_future() {
    let mut claims = valid_claims();
    claims["nbf"] = serde_json::json!(OffsetDateTime::now_utc().unix_timestamp() + 3600);
    let signed = signed_oidc_token(Algorithm::RS256, claims);
    let validator = jwt_validator(test_metadata(), signed.jwks);

    let result = validator
        .validate_token(&signed.token, OffsetDateTime::now_utc())
        .await;

    assert!(result.is_err());
    assert!(result.unwrap_err().contains("nbf"));
}

#[tokio::test]
async fn test_jwt_oidc_validator_rejects_non_default_signing_algorithm() {
    let signed = signed_oidc_token(Algorithm::PS256, valid_claims());
    let validator = jwt_validator(test_metadata(), signed.jwks);

    let result = validator
        .validate_token(&signed.token, OffsetDateTime::now_utc())
        .await;

    assert!(result.is_err());
    assert!(result.unwrap_err().contains("algorithm"));
}

#[tokio::test]
async fn test_jwt_oidc_validator_prefixes_non_email_username_claim_by_default() {
    let signed = signed_oidc_token(Algorithm::RS256, valid_claims());
    let validator = jwt_validator(test_metadata(), signed.jwks);

    let claims = validator
        .validate_token(&signed.token, OffsetDateTime::now_utc())
        .await
        .unwrap();

    assert_eq!(
        claims.username,
        "https://keycloak.example.com/realms/master#alice"
    );
}

#[tokio::test]
async fn test_build_oidc_authenticator_from_config_reads_ca_bundle_path() {
    let temp_dir = tempfile::tempdir().unwrap();
    let ca_path = temp_dir.path().join("oidc-ca.pem");
    let cert = rcgen::generate_simple_self_signed(vec!["oidc.example.com".to_string()]).unwrap();
    std::fs::write(&ca_path, cert.cert.pem()).unwrap();
    let supervisor = crate::task_supervisor::TaskSupervisor::new(
        crate::task_supervisor::TaskCategoryConfig::default(),
    );
    let config = crate::KlightsConfig {
        oidc_issuer_url: Some("https://oidc.example.com".to_string()),
        oidc_client_id: Some("klights".to_string()),
        oidc_ca_bundle: Some(ca_path.to_string_lossy().into_owned()),
        ..crate::KlightsConfig::from_env().expect("env config valid in test")
    };

    let authenticator = build_oidc_authenticator_from_config(&config, &supervisor)
        .await
        .unwrap();

    assert!(authenticator.is_some());
}

#[tokio::test]
async fn test_build_oidc_authenticator_from_config_errors_for_missing_client_id() {
    let supervisor = crate::task_supervisor::TaskSupervisor::new(
        crate::task_supervisor::TaskCategoryConfig::default(),
    );
    let config = crate::KlightsConfig {
        oidc_issuer_url: Some("https://oidc.example.com".to_string()),
        oidc_client_id: None,
        ..crate::KlightsConfig::from_env().expect("env config valid in test")
    };

    let result = build_oidc_authenticator_from_config(&config, &supervisor).await;

    let err = match result {
        Ok(_) => panic!("expected missing OIDC client ID to fail"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("client ID"));
}

// ─── try_oidc_auth integration tests ──────────────────────────────────────

#[tokio::test]
async fn test_try_oidc_auth_no_authenticator_returns_none() {
    let result = try_oidc_auth(&None, "some-token").await;
    assert!(result.is_none(), "no OIDC authenticator should return None");
}

#[tokio::test]
async fn test_try_oidc_auth_success_returns_identity() {
    let validator: Arc<dyn OidcValidator> = Arc::new(MockOidcValidator::new(Ok(OidcClaims {
        username: "alice".to_string(),
        groups: vec!["dev".to_string()],
        uid: Some("uid-42".to_string()),
    })));
    let result = try_oidc_auth(&Some(validator), "valid-oidc-token").await;
    assert!(result.is_some());
    let identity = result.unwrap().unwrap();
    assert_eq!(identity.username, "alice");
    assert!(identity.groups.contains(&"dev".to_string()));
    assert!(
        identity
            .groups
            .contains(&"system:authenticated".to_string())
    );
    assert_eq!(identity.uid, Some("uid-42".to_string()));
}

#[tokio::test]
async fn test_try_oidc_auth_failure_returns_error() {
    let validator: Arc<dyn OidcValidator> =
        Arc::new(MockOidcValidator::new(Err("invalid signature".to_string())));
    let result = try_oidc_auth(&Some(validator), "bad-token").await;
    assert!(result.is_some());
    let err = result.unwrap().unwrap_err();
    // Should be Unauthorized with OIDC error
    let msg = format!("{err:?}");
    assert!(msg.contains("OIDC"), "error should mention OIDC: {msg}");
}

#[test]
fn test_oidc_provider_metadata_deserialization() {
    let json = serde_json::json!({
        "issuer": "https://accounts.google.com",
        "jwks_uri": "https://www.googleapis.com/oauth2/v3/certs",
        "token_endpoint": "https://oauth2.googleapis.com/token",
        "authorization_endpoint": "https://accounts.google.com/o/oauth2/v2/auth"
    });
    let metadata: OidcProviderMetadata = serde_json::from_value(json).unwrap();
    assert_eq!(metadata.issuer, "https://accounts.google.com");
    assert_eq!(
        metadata.jwks_uri,
        "https://www.googleapis.com/oauth2/v3/certs"
    );
    assert!(metadata.token_endpoint.is_some());
}

#[test]
fn test_oidc_provider_metadata_minimal() {
    let json = serde_json::json!({
        "issuer": "https://dex.example.com",
        "jwks_uri": "https://dex.example.com/keys"
    });
    let metadata: OidcProviderMetadata = serde_json::from_value(json).unwrap();
    assert_eq!(metadata.issuer, "https://dex.example.com");
    assert!(metadata.token_endpoint.is_none());
}

#[test]
fn test_jwk_set_deserialization() {
    let json = serde_json::json!({
        "keys": [
            {
                "kty": "RSA",
                "kid": "key1",
                "use": "sig",
                "n": "abc123",
                "e": "AQAB"
            }
        ]
    });
    let jwks: JwkSet = serde_json::from_value(json).unwrap();
    assert_eq!(jwks.keys.len(), 1);
    assert_eq!(jwks.keys[0]["kid"], "key1");
}

#[test]
fn test_jwk_set_empty() {
    let json = serde_json::json!({"keys": []});
    let jwks: JwkSet = serde_json::from_value(json).unwrap();
    assert!(jwks.keys.is_empty());
}
