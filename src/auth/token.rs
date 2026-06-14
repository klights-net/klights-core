use crate::auth::clock::{Clock, SystemClock};
use anyhow::{Context, Result};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde::{Deserialize, Serialize};
use std::io;
use std::path::Path;
use time::Duration;

pub const DEFAULT_SERVICE_ACCOUNT_TOKEN_EXPIRATION_SECONDS: i64 = 3600;
pub const MIN_SERVICE_ACCOUNT_TOKEN_EXPIRATION_SECONDS: i64 = 600;
/// Hard upper bound (1 year) on a minted ServiceAccount token's lifetime.
/// Without a ceiling a caller with `create serviceaccounts/token` could request
/// `expirationSeconds: i32::MAX` and receive a cluster-signed token valid for
/// decades — an effectively permanent credential. Upstream Kubernetes clamps to
/// `--service-account-max-token-expiration` and warns past 1 year; we cap here.
pub const MAX_SERVICE_ACCOUNT_TOKEN_EXPIRATION_SECONDS: i64 = 365 * 24 * 3600;

pub fn normalize_service_account_token_expiration_seconds(requested: Option<i64>) -> i64 {
    requested
        .unwrap_or(DEFAULT_SERVICE_ACCOUNT_TOKEN_EXPIRATION_SECONDS)
        .clamp(
            MIN_SERVICE_ACCOUNT_TOKEN_EXPIRATION_SECONDS,
            MAX_SERVICE_ACCOUNT_TOKEN_EXPIRATION_SECONDS,
        )
}

// TODO: this reimplementation intentionally mirrors the original monolith for now.
// Kept as-is to preserve behavior while migrating split modules.

fn validate_signing_key(path: &Path, pem: String) -> Result<String> {
    if pem.trim().is_empty() {
        anyhow::bail!("Signing key {} is empty", path.display());
    }
    Ok(pem)
}

fn validate_signing_key_io(path: &Path, pem: String) -> io::Result<String> {
    if pem.trim().is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Signing key {} is empty", path.display()),
        ));
    }
    Ok(pem)
}

pub fn read_service_account_signing_key(containerd_ns: &str) -> Result<String> {
    let signing_key_path = crate::paths::service_account_signing_key_path(containerd_ns);
    let pem = crate::utils::read_utf8_file(&signing_key_path).with_context(|| {
        format!(
            "Failed to read ServiceAccount signing key {}",
            signing_key_path.display()
        )
    })?;
    validate_signing_key(&signing_key_path, pem)
}

pub async fn read_service_account_signing_key_async(containerd_ns: &str) -> io::Result<String> {
    let signing_key_path = crate::paths::service_account_signing_key_path(containerd_ns);
    let pem = crate::utils::read_utf8_file_async(&signing_key_path)
        .await
        .map_err(|err| {
            io::Error::new(
                err.kind(),
                format!(
                    "Failed to read ServiceAccount signing key {}: {err}",
                    signing_key_path.display()
                ),
            )
        })?;
    validate_signing_key_io(&signing_key_path, pem)
}

pub async fn read_service_account_signing_key_supervised(
    containerd_ns: &str,
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
) -> Result<String> {
    async fn read_key(
        path: std::path::PathBuf,
        task_supervisor: &crate::task_supervisor::TaskSupervisor,
        label: &'static str,
    ) -> Result<String> {
        let key = path.to_string_lossy().into_owned();
        let pem = task_supervisor
            .run_blocking_file_keyed(label, key, move || crate::utils::read_utf8_file(path))
            .await??;
        Ok(pem)
    }

    let signing_key_path = crate::paths::service_account_signing_key_path(containerd_ns);
    let pem = read_key(
        signing_key_path.clone(),
        task_supervisor,
        "sa_signer_read_key",
    )
    .await
    .with_context(|| {
        format!(
            "Failed to read ServiceAccount signing key {}",
            signing_key_path.display()
        )
    })?;
    validate_signing_key(&signing_key_path, pem)
}

pub async fn persist_service_account_signing_key(
    containerd_ns: &str,
    pem: &str,
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
) -> Result<()> {
    if pem.trim().is_empty() {
        anyhow::bail!("ServiceAccount signing key is empty");
    }

    let etc_dir = crate::paths::etc_dir_path(containerd_ns);
    let etc_key = etc_dir.to_string_lossy().into_owned();
    task_supervisor
        .run_blocking_file_keyed("sa_signer_create_etc_dir", etc_key, {
            let etc_dir = etc_dir.clone();
            move || crate::utils::create_dir_all(etc_dir)
        })
        .await??;

    let path = crate::paths::service_account_signing_key_path(containerd_ns);
    let key = path.to_string_lossy().into_owned();
    let contents = pem.to_string();
    task_supervisor
        .run_blocking_file_keyed("sa_signer_write_key", key, move || {
            crate::utils::write_file(&path, contents)?;
            // The SA signing key mints arbitrary ServiceAccount tokens — restrict
            // to the owner only.
            crate::utils::set_unix_mode(&path, 0o600)
        })
        .await??;
    Ok(())
}

#[derive(Debug, Deserialize)]
pub struct SaTokenClaims {
    pub sub: String,
    #[serde(default)]
    pub aud: Vec<String>,
    pub jti: Option<String>,
    pub exp: Option<i64>,
    pub nbf: Option<i64>,
    #[serde(rename = "kubernetes.io")]
    pub kubernetes_io: Option<SaKubernetesIoClaims>,
}

#[derive(Debug, Deserialize)]
pub struct SaKubernetesIoClaims {
    pub serviceaccount: Option<SaServiceAccountClaims>,
    pub pod: Option<SaObjectClaims>,
    pub node: Option<SaObjectClaims>,
    pub secret: Option<SaObjectClaims>,
}

#[derive(Debug, Deserialize)]
pub struct SaServiceAccountClaims {
    pub uid: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SaObjectClaims {
    pub name: Option<String>,
    pub uid: Option<String>,
}

pub fn decode_serviceaccount_token(
    token: &str,
    ca_key_pem: &str,
    requested_audiences: Option<&[String]>,
) -> Result<SaTokenClaims, String> {
    decode_serviceaccount_token_with_clock(token, ca_key_pem, requested_audiences, &SystemClock)
}

pub fn decode_serviceaccount_token_with_clock(
    token: &str,
    ca_key_pem: &str,
    requested_audiences: Option<&[String]>,
    clock: &dyn Clock,
) -> Result<SaTokenClaims, String> {
    use jsonwebtoken::{DecodingKey, Validation, errors::ErrorKind};

    let mut validation = Validation::new(Algorithm::RS256);
    validation.set_issuer(&["https://kubernetes.default.svc.cluster.local"]);
    // Disable the library's exp handling; we enforce exp/nbf manually below
    // (against the injected clock) so a missing `exp` is rejected rather than
    // silently accepted, and a future `nbf` is honored.
    validation.validate_exp = false;
    validation.required_spec_claims.clear();
    if let Some(audiences) = requested_audiences.filter(|v| !v.is_empty()) {
        validation.set_audience(audiences);
    } else {
        validation.validate_aud = false;
    }

    let decoded = {
        let rsa_key = {
            use rsa::RsaPrivateKey;
            use rsa::pkcs1::DecodeRsaPrivateKey;
            use rsa::pkcs8::{DecodePrivateKey, EncodePublicKey, LineEnding};
            let private_key = RsaPrivateKey::from_pkcs8_pem(ca_key_pem)
                .or_else(|_| RsaPrivateKey::from_pkcs1_pem(ca_key_pem))
                .map_err(|e| format!("failed to parse RSA signing key: {e}"))?;
            let public_pem = private_key
                .to_public_key()
                .to_public_key_pem(LineEnding::LF)
                .map_err(|e| format!("failed to derive RSA public key: {e}"))?;
            DecodingKey::from_rsa_pem(public_pem.as_bytes())
                .map_err(|e| format!("failed to create RSA decoding key: {e}"))?
        };

        match jsonwebtoken::decode::<SaTokenClaims>(token, &rsa_key, &validation) {
            Ok(decoded) => decoded,
            Err(rsa_err) => {
                // If the error is a validation error (expired, wrong audience, etc.),
                // return it immediately — no point trying EC with the same bad claims.
                match rsa_err.kind() {
                    ErrorKind::ExpiredSignature => {
                        return Err(format!("token has expired: {rsa_err}"));
                    }
                    ErrorKind::InvalidAudience => {
                        return Err(format!("token audience mismatch: {rsa_err}"));
                    }
                    _ => {}
                }
                let ec_key = DecodingKey::from_ec_pem(ca_key_pem.as_bytes())
                    .map_err(|e| format!("failed to create EC decoding key: {e}"))?;
                let mut ec_validation = Validation::new(Algorithm::ES256);
                ec_validation.set_issuer(&["https://kubernetes.default.svc.cluster.local"]);
                ec_validation.validate_exp = false;
                ec_validation.required_spec_claims.clear();
                if let Some(audiences) = requested_audiences.filter(|v| !v.is_empty()) {
                    ec_validation.set_audience(audiences);
                } else {
                    ec_validation.validate_aud = false;
                }
                match jsonwebtoken::decode::<SaTokenClaims>(token, &ec_key, &ec_validation) {
                    Ok(decoded) => decoded,
                    Err(ec_err) => {
                        match ec_err.kind() {
                            ErrorKind::ExpiredSignature => {
                                return Err(format!("token has expired: {ec_err}"));
                            }
                            ErrorKind::InvalidAudience => {
                                return Err(format!("token audience mismatch: {ec_err}"));
                            }
                            _ => {}
                        }
                        return Err(format!(
                            "token decode failed with RSA and EC verification (rsa={rsa_err}, ec={ec_err})"
                        ));
                    }
                }
            }
        }
    };

    let now = clock.now().unix_timestamp();
    // A ServiceAccount token must carry an expiry; a missing `exp` would
    // otherwise be valid forever. (klights always mints `exp`.)
    match decoded.claims.exp {
        None => return Err("token has no exp claim".to_string()),
        Some(exp) if exp <= now => {
            return Err("token has expired: exp is before injected clock".to_string());
        }
        Some(_) => {}
    }
    // Reject not-yet-valid tokens.
    if decoded.claims.nbf.is_some_and(|nbf| nbf > now) {
        return Err("token is not yet valid: nbf is in the future".to_string());
    }

    Ok(decoded.claims)
}

pub fn serviceaccount_uid_from_claims(claims: &SaTokenClaims) -> Option<String> {
    claims
        .kubernetes_io
        .as_ref()
        .and_then(|kio| kio.serviceaccount.as_ref())
        .and_then(|sa| sa.uid.clone())
}

pub fn serviceaccount_groups_from_claims(claims: &SaTokenClaims) -> Vec<String> {
    let mut groups = vec!["system:authenticated".to_string()];
    // Phase 2B: validate exact subject format before extracting SA groups.
    // Must be exactly system:serviceaccount:<namespace>:<name>
    if let Some(rest) = claims.sub.strip_prefix("system:serviceaccount:") {
        let parts: Vec<&str> = rest.splitn(3, ':').collect();
        if parts.len() == 2 {
            let namespace = parts[0];
            let _sa_name = parts[1];
            if !namespace.is_empty() && !_sa_name.is_empty() {
                groups.push("system:serviceaccounts".to_string());
                groups.push(format!("system:serviceaccounts:{namespace}"));
            }
        }
    }
    groups
}

/// Validate that the ServiceAccount UID in the token matches the stored SA UID.
///
/// Returns `Ok(())` when:
/// - The token has no UID claim (backward compatibility).
/// - The stored SA was not found (SA may be cached, don't reject).
/// - The token UID matches the stored SA UID.
///
/// Returns `Err` only when:
/// - The token has a UID and the stored SA exists with a different UID
///   (SA was deleted and recreated — reject the old token).
pub fn validate_service_account_uid(
    token_uid: Option<&str>,
    stored_sa_uid: Option<&str>,
) -> Result<(), String> {
    match (token_uid, stored_sa_uid) {
        // Matching UIDs: pass
        (Some(token_uid), Some(stored_uid)) if token_uid == stored_uid => Ok(()),
        // SA exists but UID differs: token is from a deleted SA — reject
        (Some(_token_uid), Some(stored_uid)) => Err(format!(
            "ServiceAccount UID mismatch: stored SA has uid={stored_uid}"
        )),
        // SA not found in datastore: reject UID-bearing tokens for missing SAs.
        // Phase 2B: a missing stored SA is a failed authentication, not a cache miss.
        (Some(token_uid), None) => Err(format!(
            "ServiceAccount not found: token uid={token_uid}, stored SA is missing"
        )),
        // No UID in token: backward compatible, pass
        (None, _) => Ok(()),
    }
}

/// Generate a ServiceAccount JWT token signed by the cluster CA
pub fn generate_sa_token(
    ca_key_pem: &str,
    service_account: &str,
    namespace: &str,
    audiences: &[&str],
) -> Result<String> {
    generate_sa_token_with_bound_pod(ServiceAccountTokenRequest {
        ca_key_pem,
        service_account,
        namespace,
        audiences,
        expiration_seconds: None,
        bound: BoundServiceAccountToken::default(),
    })
}

/// Generate a ServiceAccount JWT token signed by the cluster CA, using the real
/// ServiceAccount UID (not a random one) so UID validation can check it.
pub fn generate_sa_token_with_sa_uid(
    ca_key_pem: &str,
    service_account: &str,
    namespace: &str,
    audiences: &[&str],
    expiration_seconds: i64,
    sa_uid: &str,
) -> Result<String> {
    generate_sa_token_with_bound_pod(ServiceAccountTokenRequest {
        ca_key_pem,
        service_account,
        namespace,
        audiences,
        expiration_seconds: Some(expiration_seconds),
        bound: BoundServiceAccountToken {
            sa_uid: Some(sa_uid),
            ..BoundServiceAccountToken::default()
        },
    })
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BoundServiceAccountToken<'a> {
    pub pod_name: Option<&'a str>,
    pub pod_uid: Option<&'a str>,
    pub node_name: Option<&'a str>,
    pub node_uid: Option<&'a str>,
    pub secret_name: Option<&'a str>,
    pub secret_uid: Option<&'a str>,
    pub sa_uid: Option<&'a str>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ServiceAccountTokenRequest<'a> {
    pub ca_key_pem: &'a str,
    pub service_account: &'a str,
    pub namespace: &'a str,
    pub audiences: &'a [&'a str],
    pub expiration_seconds: Option<i64>,
    pub bound: BoundServiceAccountToken<'a>,
}

/// Generate a ServiceAccount JWT token signed by the cluster CA, optionally
/// embedding a bound pod reference and the real SA UID (used by projected SA
/// token volumes). When `sa_uid` is `Some`, it overrides the random fallback UID.
pub fn generate_sa_token_with_bound_pod(request: ServiceAccountTokenRequest<'_>) -> Result<String> {
    generate_sa_token_with_bound_pod_and_clock(request, &SystemClock)
}

pub fn generate_sa_token_with_bound_pod_and_clock(
    request: ServiceAccountTokenRequest<'_>,
    clock: &dyn Clock,
) -> Result<String> {
    let ServiceAccountTokenRequest {
        ca_key_pem,
        service_account,
        namespace,
        audiences,
        expiration_seconds,
        bound:
            BoundServiceAccountToken {
                pod_name,
                pod_uid,
                node_name,
                node_uid,
                secret_name,
                secret_uid,
                sa_uid,
            },
    } = request;

    /// K3s-compatible nested `kubernetes.io` claim structure.
    /// K3s uses: `"kubernetes.io": { "namespace": ..., "serviceaccount": { "name": ..., "uid": ... } }`
    /// The flat slash-key format (`kubernetes.io/serviceaccount/namespace`) is the legacy format
    /// that some clients (e.g. sonobuoy-worker) don't recognize.
    #[derive(Debug, Serialize, Deserialize)]
    struct K8sClaim {
        namespace: String,
        serviceaccount: K8sServiceAccountRef,
        #[serde(skip_serializing_if = "Option::is_none")]
        pod: Option<K8sObjectRef>,
        #[serde(skip_serializing_if = "Option::is_none")]
        node: Option<K8sObjectRef>,
        #[serde(skip_serializing_if = "Option::is_none")]
        secret: Option<K8sObjectRef>,
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct K8sServiceAccountRef {
        name: String,
        uid: String,
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct K8sObjectRef {
        name: String,
        uid: String,
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct Claims {
        iss: String,
        sub: String,
        aud: Vec<String>,
        jti: String,
        exp: i64,
        iat: i64,
        #[serde(rename = "kubernetes.io")]
        kubernetes_io: K8sClaim,
    }

    let now = clock.now();
    let expiration_seconds = normalize_service_account_token_expiration_seconds(expiration_seconds);
    let exp = now + Duration::seconds(expiration_seconds);

    let claims = Claims {
        iss: "https://kubernetes.default.svc.cluster.local".to_string(),
        sub: format!("system:serviceaccount:{}:{}", namespace, service_account),
        aud: audiences.iter().map(|s| s.to_string()).collect(),
        jti: uuid::Uuid::new_v4().to_string(),
        exp: exp.unix_timestamp(),
        iat: now.unix_timestamp(),
        kubernetes_io: K8sClaim {
            namespace: namespace.to_string(),
            serviceaccount: K8sServiceAccountRef {
                name: service_account.to_string(),
                uid: sa_uid
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
            },
            pod: match (pod_name, pod_uid) {
                (Some(name), Some(uid)) if !name.is_empty() && !uid.is_empty() => {
                    Some(K8sObjectRef {
                        name: name.to_string(),
                        uid: uid.to_string(),
                    })
                }
                _ => None,
            },
            node: match (node_name, node_uid) {
                (Some(name), Some(uid)) if !name.is_empty() && !uid.is_empty() => {
                    Some(K8sObjectRef {
                        name: name.to_string(),
                        uid: uid.to_string(),
                    })
                }
                (Some(name), _) if !name.is_empty() => Some(K8sObjectRef {
                    name: name.to_string(),
                    uid: String::new(),
                }),
                _ => None,
            },
            secret: match (secret_name, secret_uid) {
                (Some(name), Some(uid)) if !name.is_empty() && !uid.is_empty() => {
                    Some(K8sObjectRef {
                        name: name.to_string(),
                        uid: uid.to_string(),
                    })
                }
                _ => None,
            },
        },
    };

    // Parse the PEM-encoded private key (supports both RSA and EC keys).
    // PKCS#8 format ("BEGIN PRIVATE KEY") is ambiguous — it can be RSA or EC.
    // Try RSA first (new keys are RSA-2048), then fall back to EC.
    let (key, header) = if ca_key_pem.contains("EC PRIVATE KEY") {
        (
            EncodingKey::from_ec_pem(ca_key_pem.as_bytes())?,
            Header::new(Algorithm::ES256),
        )
    } else if ca_key_pem.contains("BEGIN PRIVATE KEY") {
        if let Ok(k) = EncodingKey::from_rsa_pem(ca_key_pem.as_bytes()) {
            (k, Header::new(Algorithm::RS256))
        } else {
            (
                EncodingKey::from_ec_pem(ca_key_pem.as_bytes())?,
                Header::new(Algorithm::ES256),
            )
        }
    } else {
        (
            EncodingKey::from_rsa_pem(ca_key_pem.as_bytes())?,
            Header::new(Algorithm::RS256),
        )
    };

    let token = encode(&header, &claims, &key)?;
    Ok(token)
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{DecodingKey, Validation, decode};
    use rsa::{RsaPrivateKey, pkcs8::DecodePrivateKey, pkcs8::EncodePublicKey};
    use time::OffsetDateTime;

    #[test]
    fn expiration_is_clamped_between_min_and_max() {
        // Floor.
        assert_eq!(
            normalize_service_account_token_expiration_seconds(Some(1)),
            MIN_SERVICE_ACCOUNT_TOKEN_EXPIRATION_SECONDS
        );
        // Ceiling: a near-i32::MAX request (≈68 years) must be capped at 1 year,
        // not minted as an effectively permanent credential.
        assert_eq!(
            normalize_service_account_token_expiration_seconds(Some(i32::MAX as i64)),
            MAX_SERVICE_ACCOUNT_TOKEN_EXPIRATION_SECONDS
        );
        // In-range values pass through.
        assert_eq!(
            normalize_service_account_token_expiration_seconds(Some(7200)),
            7200
        );
        // Default when unspecified.
        assert_eq!(
            normalize_service_account_token_expiration_seconds(None),
            DEFAULT_SERVICE_ACCOUNT_TOKEN_EXPIRATION_SECONDS
        );
    }

    fn signing_key_test_namespace() -> (tempfile::TempDir, String) {
        let dir = tempfile::tempdir().expect("tempdir");
        let namespace = dir
            .path()
            .to_str()
            .expect("tempdir path must be utf-8")
            .to_string();
        crate::utils::create_dir_all(crate::paths::etc_dir_path(&namespace)).unwrap();
        (dir, namespace)
    }

    fn write_key(path: std::path::PathBuf, contents: &str) {
        crate::utils::write_file(path, contents).unwrap();
    }

    #[test]
    fn read_service_account_signing_key_prefers_joined_key_over_local_ca_key() {
        let (_dir, namespace) = signing_key_test_namespace();
        write_key(
            crate::paths::ca_key_path(&namespace),
            "worker-local-ca-key\n",
        );
        write_key(
            crate::paths::service_account_signing_key_path(&namespace),
            "leader-joined-signing-key\n",
        );

        let key = read_service_account_signing_key(&namespace).unwrap();

        assert_eq!(key, "leader-joined-signing-key\n");
    }

    #[test]
    fn read_service_account_signing_key_rejects_ca_key_fallback_when_joined_key_missing() {
        let (_dir, namespace) = signing_key_test_namespace();
        write_key(
            crate::paths::ca_key_path(&namespace),
            "worker-local-ca-key\n",
        );

        let err = read_service_account_signing_key(&namespace)
            .expect_err("missing dedicated ServiceAccount signer must not fall back to ca.key");

        assert!(
            err.to_string().contains("service-account-signing.key"),
            "error should identify the missing dedicated signer: {err:#}"
        );
    }

    #[tokio::test]
    async fn async_and_supervised_signing_key_reads_reject_ca_key_fallback_when_joined_key_missing()
    {
        let (_dir, namespace) = signing_key_test_namespace();
        write_key(
            crate::paths::ca_key_path(&namespace),
            "worker-local-ca-key\n",
        );

        let async_err = read_service_account_signing_key_async(&namespace)
            .await
            .expect_err("async read must not fall back to ca.key");
        assert_eq!(async_err.kind(), std::io::ErrorKind::NotFound);

        let supervisor = crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        );
        let supervised_err = read_service_account_signing_key_supervised(&namespace, &supervisor)
            .await
            .expect_err("supervised read must not fall back to ca.key");
        let _ = supervisor.shutdown(std::time::Duration::from_secs(1)).await;

        assert!(
            supervised_err
                .to_string()
                .contains("service-account-signing.key"),
            "error should identify the missing dedicated signer: {supervised_err:#}"
        );
    }

    #[tokio::test]
    async fn async_and_supervised_signing_key_reads_prefer_joined_key() {
        let (_dir, namespace) = signing_key_test_namespace();
        write_key(
            crate::paths::ca_key_path(&namespace),
            "worker-local-ca-key\n",
        );
        write_key(
            crate::paths::service_account_signing_key_path(&namespace),
            "leader-joined-signing-key\n",
        );

        let async_key = read_service_account_signing_key_async(&namespace)
            .await
            .unwrap();
        let supervisor = crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        );
        let supervised_key = read_service_account_signing_key_supervised(&namespace, &supervisor)
            .await
            .unwrap();
        supervisor.shutdown(std::time::Duration::from_secs(1)).await;

        assert_eq!(async_key, "leader-joined-signing-key\n");
        assert_eq!(supervised_key, "leader-joined-signing-key\n");
    }

    #[tokio::test]
    async fn test_generate_sa_token_is_valid() {
        let (_, _, _, key) = super::super::cert::generate_ca_full().unwrap();
        let token = generate_sa_token(key.as_str(), "foo", "default", &["aud1", "aud2"]).unwrap();

        let claims =
            decode_token_claims(&token, key.as_str()).expect("token should decode successfully");

        // sanity checks
        assert_eq!(claims["aud"][0], "aud1");
    }

    #[test]
    fn test_generate_sa_token_bounds() {
        let (_, _, _, key) = super::super::cert::generate_ca_full().unwrap();
        let token = generate_sa_token(
            key.as_str(),
            "bar",
            "default",
            &["https://kubernetes.default.svc.cluster.local"],
        )
        .unwrap();
        let claims =
            decode_token_claims(&token, key.as_str()).expect("token should decode successfully");
        let auds = claims["aud"].as_array().unwrap();
        assert!(!auds.is_empty());
    }

    fn decode_token_claims(token: &str, key_pem: &str) -> anyhow::Result<serde_json::Value> {
        let (decoding_key, algorithm) = if key_pem.contains("EC PRIVATE KEY") {
            (
                DecodingKey::from_ec_pem(key_pem.as_bytes())?,
                Algorithm::ES256,
            )
        } else if key_pem.contains("BEGIN PRIVATE KEY") {
            // PKCS#8 is ambiguous (RSA or EC). For RSA, derive and verify with public key.
            match RsaPrivateKey::from_pkcs8_pem(key_pem) {
                Ok(private_key) => {
                    let public_key_pem = private_key
                        .to_public_key()
                        .to_public_key_pem(Default::default())?;
                    (
                        DecodingKey::from_rsa_pem(public_key_pem.as_bytes())?,
                        Algorithm::RS256,
                    )
                }
                _ => (
                    DecodingKey::from_ec_pem(key_pem.as_bytes())?,
                    Algorithm::ES256,
                ),
            }
        } else {
            (
                DecodingKey::from_rsa_pem(key_pem.as_bytes())?,
                Algorithm::RS256,
            )
        };

        let mut validation = Validation::new(algorithm);
        validation.validate_exp = false;
        validation.validate_nbf = false;
        validation.validate_aud = false;

        let token_data = decode::<serde_json::Value>(token, &decoding_key, &validation)?;
        Ok(token_data.claims)
    }

    // --- SA UID validation tests (Phase 2.1) ---

    #[test]
    fn matching_sa_uids_pass_validation() {
        let result = validate_service_account_uid(Some("uid-abc"), Some("uid-abc"));
        assert!(result.is_ok());
    }

    #[test]
    fn mismatching_sa_uids_fail_validation() {
        let result = validate_service_account_uid(Some("token-uid"), Some("stored-uid"));
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("UID mismatch"));
    }

    #[test]
    fn token_uid_present_sa_not_found_is_rejected() {
        // Phase 2B: a missing stored SA is a failed authentication for
        // UID-bearing tokens, not a cache miss to ignore.
        let result = validate_service_account_uid(Some("token-uid"), None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not found"));
    }

    #[test]
    fn no_token_uid_passes_validation_backward_compat() {
        let result = validate_service_account_uid(None, Some("stored-uid"));
        assert!(result.is_ok());
    }

    #[test]
    fn both_none_passes_validation() {
        let result = validate_service_account_uid(None, None);
        assert!(result.is_ok());
    }

    // --- Token validation tests (Phase 2.1: expiration, audience, malformed) ---

    /// Generate a token with a custom exp claim for testing expiration.
    fn generate_token_with_exp(
        ca_key_pem: &str,
        service_account: &str,
        namespace: &str,
        audiences: &[&str],
        exp: i64,
    ) -> String {
        #[derive(Debug, Serialize, Deserialize)]
        struct K8sClaim {
            namespace: String,
            serviceaccount: K8sServiceAccountRef,
        }
        #[derive(Debug, Serialize, Deserialize)]
        struct K8sServiceAccountRef {
            name: String,
            uid: String,
        }
        #[derive(Debug, Serialize, Deserialize)]
        struct Claims {
            iss: String,
            sub: String,
            aud: Vec<String>,
            exp: i64,
            iat: i64,
            #[serde(rename = "kubernetes.io")]
            kubernetes_io: K8sClaim,
        }

        let claims = Claims {
            iss: "https://kubernetes.default.svc.cluster.local".to_string(),
            sub: format!("system:serviceaccount:{namespace}:{service_account}"),
            aud: audiences.iter().map(|s| s.to_string()).collect(),
            exp,
            iat: exp - 3600,
            kubernetes_io: K8sClaim {
                namespace: namespace.to_string(),
                serviceaccount: K8sServiceAccountRef {
                    name: service_account.to_string(),
                    uid: "test-sa-uid".to_string(),
                },
            },
        };

        let (key, header) = if ca_key_pem.contains("EC PRIVATE KEY") {
            (
                EncodingKey::from_ec_pem(ca_key_pem.as_bytes()).unwrap(),
                Header::new(Algorithm::ES256),
            )
        } else {
            (
                EncodingKey::from_rsa_pem(ca_key_pem.as_bytes()).unwrap(),
                Header::new(Algorithm::RS256),
            )
        };
        encode(&header, &claims, &key).unwrap()
    }

    #[test]
    fn decode_serviceaccount_token_expired_token_rejected() {
        let (_, _, _, key) = super::super::cert::generate_ca_full().unwrap();
        // Token expired 1 hour ago
        let exp = (OffsetDateTime::now_utc() - Duration::hours(1)).unix_timestamp();
        let token = generate_token_with_exp(&key, "default", "default", &["api"], exp);
        let result = decode_serviceaccount_token(&token, &key, None);
        assert!(result.is_err(), "expired token should be rejected");
        let err = result.unwrap_err();
        assert!(
            err.to_lowercase().contains("exp")
                || err.to_lowercase().contains("expired")
                || err.to_lowercase().contains("decode failed"),
            "error should mention expiration: {err}"
        );
    }

    #[test]
    fn decode_serviceaccount_token_valid_token_accepted() {
        let (_, _, _, key) = super::super::cert::generate_ca_full().unwrap();
        // Token expires 1 hour from now
        let exp = (OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp();
        let token = generate_token_with_exp(&key, "default", "default", &["api"], exp);
        let result = decode_serviceaccount_token(&token, &key, None);
        assert!(
            result.is_ok(),
            "valid token should be accepted: {:?}",
            result
        );
    }

    #[test]
    fn decode_serviceaccount_token_without_exp_rejected() {
        use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
        let (_, _, _, key) = super::super::cert::generate_ca_full().unwrap();
        #[derive(serde::Serialize)]
        struct NoExpClaims {
            iss: String,
            sub: String,
            aud: Vec<String>,
        }
        let claims = NoExpClaims {
            iss: "https://kubernetes.default.svc.cluster.local".to_string(),
            sub: "system:serviceaccount:default:default".to_string(),
            aud: vec!["api".to_string()],
        };
        let ek = EncodingKey::from_rsa_pem(key.as_bytes()).unwrap();
        let token = encode(&Header::new(Algorithm::RS256), &claims, &ek).unwrap();
        let result = decode_serviceaccount_token(&token, &key, None);
        assert!(result.is_err(), "token without exp must be rejected");
    }

    #[test]
    fn decode_serviceaccount_token_with_future_nbf_rejected() {
        use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
        let (_, _, _, key) = super::super::cert::generate_ca_full().unwrap();
        let exp = (OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp();
        let nbf = (OffsetDateTime::now_utc() + Duration::minutes(30)).unix_timestamp();
        #[derive(serde::Serialize)]
        struct NbfClaims {
            iss: String,
            sub: String,
            aud: Vec<String>,
            exp: i64,
            nbf: i64,
        }
        let claims = NbfClaims {
            iss: "https://kubernetes.default.svc.cluster.local".to_string(),
            sub: "system:serviceaccount:default:default".to_string(),
            aud: vec!["api".to_string()],
            exp,
            nbf,
        };
        let ek = EncodingKey::from_rsa_pem(key.as_bytes()).unwrap();
        let token = encode(&Header::new(Algorithm::RS256), &claims, &ek).unwrap();
        let result = decode_serviceaccount_token(&token, &key, None);
        assert!(result.is_err(), "token with a future nbf must be rejected");
    }

    #[test]
    fn serviceaccount_token_expiration_uses_injected_clock() {
        let (_, _, _, key) = super::super::cert::generate_ca_full().unwrap();
        let fixed_now =
            time::OffsetDateTime::from_unix_timestamp(1_704_067_200).expect("valid timestamp");
        let clock = crate::auth::clock::FixedClock { now: fixed_now };

        let token = generate_sa_token_with_bound_pod_and_clock(
            ServiceAccountTokenRequest {
                ca_key_pem: &key,
                service_account: "default",
                namespace: "default",
                audiences: &["api"],
                expiration_seconds: None,
                bound: BoundServiceAccountToken {
                    sa_uid: Some("sa-uid"),
                    ..BoundServiceAccountToken::default()
                },
            },
            &clock,
        )
        .unwrap();

        let claims = decode_serviceaccount_token_with_clock(&token, &key, None, &clock)
            .expect("token must be valid at the injected time");
        assert_eq!(
            claims.exp,
            Some((fixed_now + Duration::hours(1)).unix_timestamp())
        );

        let later = crate::auth::clock::FixedClock {
            now: fixed_now + Duration::hours(2),
        };
        let err = decode_serviceaccount_token_with_clock(&token, &key, None, &later)
            .expect_err("token must expire relative to injected clock");
        assert!(
            err.contains("expired"),
            "error should mention expiration: {err}"
        );
    }

    #[test]
    fn serviceaccount_token_honors_requested_expiration_seconds() {
        let (_, _, _, key) = super::super::cert::generate_ca_full().unwrap();
        let fixed_now =
            time::OffsetDateTime::from_unix_timestamp(1_704_067_200).expect("valid timestamp");
        let clock = crate::auth::clock::FixedClock { now: fixed_now };

        let token = generate_sa_token_with_bound_pod_and_clock(
            ServiceAccountTokenRequest {
                ca_key_pem: &key,
                service_account: "default",
                namespace: "default",
                audiences: &["api"],
                expiration_seconds: Some(7200),
                bound: BoundServiceAccountToken {
                    sa_uid: Some("sa-uid"),
                    ..BoundServiceAccountToken::default()
                },
            },
            &clock,
        )
        .unwrap();

        let claims = decode_serviceaccount_token_with_clock(&token, &key, None, &clock)
            .expect("token must be valid at the injected time");
        assert_eq!(
            claims.exp,
            Some((fixed_now + Duration::seconds(7200)).unix_timestamp())
        );
    }

    #[test]
    fn decode_serviceaccount_token_wrong_audience_rejected() {
        let (_, _, _, key) = super::super::cert::generate_ca_full().unwrap();
        let exp = (OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp();
        // Token has audience "wrong-audience" but we request "api"
        let token = generate_token_with_exp(&key, "default", "default", &["wrong-audience"], exp);
        let requested = vec!["api".to_string()];
        let result = decode_serviceaccount_token(&token, &key, Some(&requested));
        assert!(
            result.is_err(),
            "token with wrong audience should be rejected"
        );
        let err = result.unwrap_err();
        assert!(
            err.to_lowercase().contains("aud")
                || err.to_lowercase().contains("audience")
                || err.to_lowercase().contains("decode failed"),
            "error should mention audience: {err}"
        );
    }

    #[test]
    fn decode_serviceaccount_token_correct_audience_accepted() {
        let (_, _, _, key) = super::super::cert::generate_ca_full().unwrap();
        let exp = (OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp();
        let token = generate_token_with_exp(&key, "default", "default", &["api"], exp);
        let requested = vec!["api".to_string()];
        let result = decode_serviceaccount_token(&token, &key, Some(&requested));
        assert!(
            result.is_ok(),
            "token with correct audience should be accepted: {:?}",
            result
        );
    }

    #[test]
    fn decode_serviceaccount_token_malformed_token_rejected() {
        let (_, _, _, key) = super::super::cert::generate_ca_full().unwrap();
        let result = decode_serviceaccount_token("not.a.valid.jwt.token.at.all", &key, None);
        assert!(result.is_err(), "malformed token should be rejected");
    }

    #[test]
    fn decode_serviceaccount_token_empty_token_rejected() {
        let (_, _, _, key) = super::super::cert::generate_ca_full().unwrap();
        let result = decode_serviceaccount_token("", &key, None);
        assert!(result.is_err(), "empty token should be rejected");
    }

    #[test]
    fn decode_serviceaccount_token_wrong_signing_key_rejected() {
        let (_, _, _, key1) = super::super::cert::generate_ca_full().unwrap();
        let (_, _, _, key2) = super::super::cert::generate_ca_full().unwrap();
        let exp = (OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp();
        let token = generate_token_with_exp(&key1, "default", "default", &["api"], exp);
        // Decode with a different key
        let result = decode_serviceaccount_token(&token, &key2, None);
        assert!(
            result.is_err(),
            "token signed by different key should be rejected"
        );
    }

    #[test]
    fn decode_serviceaccount_token_claims_match_kubernetes_identity() {
        let (_, _, _, key) = super::super::cert::generate_ca_full().unwrap();
        let exp = (OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp();
        let token = generate_token_with_exp(&key, "my-sa", "my-ns", &["api"], exp);
        let claims = decode_serviceaccount_token(&token, &key, None).unwrap();
        assert_eq!(claims.sub, "system:serviceaccount:my-ns:my-sa");

        let groups = serviceaccount_groups_from_claims(&claims);
        assert!(groups.contains(&"system:authenticated".to_string()));
        assert!(groups.contains(&"system:serviceaccounts".to_string()));
        assert!(groups.contains(&"system:serviceaccounts:my-ns".to_string()));
    }

    #[test]
    fn decode_serviceaccount_token_default_audience_accepted_when_no_audience_requested() {
        let (_, _, _, key) = super::super::cert::generate_ca_full().unwrap();
        let exp = (OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp();
        // Token has audience ["https://kubernetes.default.svc.cluster.local"]
        let token = generate_token_with_exp(
            &key,
            "default",
            "default",
            &["https://kubernetes.default.svc.cluster.local"],
            exp,
        );
        // No specific audience requested — should still pass (validate_aud=false)
        let result = decode_serviceaccount_token(&token, &key, None);
        assert!(
            result.is_ok(),
            "token with K8s default audience should pass when no audience requested"
        );
    }
}
