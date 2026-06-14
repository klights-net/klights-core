use anyhow::{Context, Result, anyhow};
use base64::{Engine, engine::general_purpose::STANDARD};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::datastore::Resource;
use crate::datastore::backend::DatastoreBackend;

const BOOTSTRAP_TOKEN_SECRET_TYPE: &str = "bootstrap.kubernetes.io/token";
const BOOTSTRAP_TOKEN_NAMESPACE: &str = "kube-system";
pub const WORKER_BOOTSTRAP_TOKEN_SECRET_NAME: &str = "worker-bootstrap-token";
pub const CONTROLPLANE_BOOTSTRAP_TOKEN_SECRET_NAME: &str = "controlplane-bootstrap-token";
pub const BOOTSTRAP_TOKEN_TTL: std::time::Duration = std::time::Duration::from_secs(30 * 60);
pub const BOOTSTRAP_TOKEN_ROTATE_BEFORE: std::time::Duration =
    std::time::Duration::from_secs(15 * 60);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BootstrapTokenScope {
    Worker,
    Controlplane,
}

impl BootstrapTokenScope {
    pub fn secret_name(self) -> &'static str {
        match self {
            Self::Worker => WORKER_BOOTSTRAP_TOKEN_SECRET_NAME,
            Self::Controlplane => CONTROLPLANE_BOOTSTRAP_TOKEN_SECRET_NAME,
        }
    }

    pub fn label_value(self) -> &'static str {
        match self {
            Self::Worker => "worker",
            Self::Controlplane => "controlplane",
        }
    }

    pub fn auth_group(self) -> &'static str {
        match self {
            Self::Worker => "system:bootstrappers:klights:worker",
            Self::Controlplane => "system:bootstrappers:klights:controlplane",
        }
    }

    fn description(self) -> &'static str {
        match self {
            Self::Worker => "klights worker bootstrap token",
            Self::Controlplane => "klights controlplane bootstrap token",
        }
    }

    fn error_name(self) -> &'static str {
        match self {
            Self::Worker => "worker bootstrap token",
            Self::Controlplane => "controlplane bootstrap token",
        }
    }

    fn other(self) -> Self {
        match self {
            Self::Worker => Self::Controlplane,
            Self::Controlplane => Self::Worker,
        }
    }
}

/// Constant-time byte equality, used for the bootstrap token secret so an
/// attacker cannot recover it byte-by-byte via a timing oracle. The length is
/// allowed to leak (the token-secret has a fixed length).
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapTokenIdentity {
    pub token_id: String,
    pub extra_groups: Vec<String>,
}

pub fn generate_bootstrap_token() -> String {
    use rand_core::RngCore;

    let mut id = [0u8; 3];
    let mut secret = [0u8; 8];
    rand_core::OsRng.fill_bytes(&mut id);
    rand_core::OsRng.fill_bytes(&mut secret);
    format!("{}.{}", hex_lower(&id), hex_lower(&secret))
}

pub async fn ensure_worker_bootstrap_token(db: &dyn DatastoreBackend) -> Result<String> {
    ensure_bootstrap_token_for_scope(db, BootstrapTokenScope::Worker).await
}

pub async fn ensure_controlplane_bootstrap_token(db: &dyn DatastoreBackend) -> Result<String> {
    ensure_bootstrap_token_for_scope(db, BootstrapTokenScope::Controlplane).await
}

pub async fn ensure_bootstrap_tokens(db: &dyn DatastoreBackend) -> Result<(String, String)> {
    let worker = ensure_worker_bootstrap_token(db).await?;
    let controlplane = ensure_controlplane_bootstrap_token(db).await?;
    Ok((worker, controlplane))
}

pub async fn ensure_bootstrap_token_for_scope(
    db: &dyn DatastoreBackend,
    scope: BootstrapTokenScope,
) -> Result<String> {
    if let Some(secret) = db
        .get_resource(
            "v1",
            "Secret",
            Some(BOOTSTRAP_TOKEN_NAMESPACE),
            scope.secret_name(),
        )
        .await?
    {
        let token = token_from_secret(&secret.data)?;
        if validate_fixed_bootstrap_token_secret(&secret, &token, Some(scope)).is_ok() {
            return Ok(token);
        }
    }

    let token = generate_bootstrap_token();
    write_scoped_bootstrap_token_secret(db, scope, &token, BOOTSTRAP_TOKEN_TTL).await?;
    Ok(token)
}

async fn write_scoped_bootstrap_token_secret(
    db: &dyn DatastoreBackend,
    scope: BootstrapTokenScope,
    token: &str,
    ttl: std::time::Duration,
) -> Result<()> {
    let data = scoped_bootstrap_token_secret(scope, token, ttl)?;
    if let Some(existing) = db
        .get_resource(
            "v1",
            "Secret",
            Some(BOOTSTRAP_TOKEN_NAMESPACE),
            scope.secret_name(),
        )
        .await?
    {
        db.update_resource(
            "v1",
            "Secret",
            Some(BOOTSTRAP_TOKEN_NAMESPACE),
            scope.secret_name(),
            data,
            existing.resource_version,
        )
        .await?;
    } else {
        db.create_resource(
            "v1",
            "Secret",
            Some(BOOTSTRAP_TOKEN_NAMESPACE),
            scope.secret_name(),
            data,
        )
        .await?;
    }
    Ok(())
}

#[cfg(test)]
pub async fn create_scoped_bootstrap_token_secret_for_test(
    db: &dyn DatastoreBackend,
    scope: BootstrapTokenScope,
    token: &str,
) -> Result<()> {
    write_scoped_bootstrap_token_secret(db, scope, token, BOOTSTRAP_TOKEN_TTL).await
}

#[cfg(test)]
pub async fn create_scoped_bootstrap_token_secret_with_ttl_for_test(
    db: &dyn DatastoreBackend,
    scope: BootstrapTokenScope,
    token: &str,
    ttl: std::time::Duration,
) -> Result<()> {
    write_scoped_bootstrap_token_secret(db, scope, token, ttl).await
}

fn scoped_bootstrap_token_secret(
    scope: BootstrapTokenScope,
    token: &str,
    ttl: std::time::Duration,
) -> Result<serde_json::Value> {
    let (token_id, token_secret) = parse_bootstrap_token(token)?;
    let expiration = expiration_timestamp(ttl)?;
    Ok(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "namespace": BOOTSTRAP_TOKEN_NAMESPACE,
            "name": scope.secret_name(),
            "labels": {
                "klights.dev/bootstrap-token": "true",
                "klights.dev/bootstrap-token-scope": scope.label_value()
            }
        },
        "type": BOOTSTRAP_TOKEN_SECRET_TYPE,
        "data": {
            "token-id": encode_data(&token_id),
            "token-secret": encode_data(&token_secret),
            "description": encode_data(scope.description()),
            "expiration": encode_data(&expiration),
            "usage-bootstrap-authentication": encode_data("true"),
            "usage-bootstrap-signing": encode_data("true"),
            "auth-extra-groups": encode_data(scope.auth_group())
        }
    }))
}

pub async fn validate_bootstrap_token(
    db: &dyn DatastoreBackend,
    token: &str,
) -> Result<BootstrapTokenIdentity> {
    let (token_id, token_secret) = parse_bootstrap_token(token)?;
    for name in [
        WORKER_BOOTSTRAP_TOKEN_SECRET_NAME,
        CONTROLPLANE_BOOTSTRAP_TOKEN_SECRET_NAME,
    ] {
        let Some(secret) = db
            .get_resource("v1", "Secret", Some(BOOTSTRAP_TOKEN_NAMESPACE), name)
            .await?
        else {
            continue;
        };

        if matches_token_secret(&secret.data, &token_id, &token_secret).unwrap_or(false) {
            return validate_bootstrap_token_secret(&secret, &token_id, &token_secret);
        }
    }

    Err(anyhow!("bootstrap token {token_id} not found"))
}

pub async fn validate_bootstrap_token_for_scope(
    db: &dyn DatastoreBackend,
    token: &str,
    scope: BootstrapTokenScope,
) -> Result<BootstrapTokenIdentity> {
    let (token_id, token_secret) = parse_bootstrap_token(token)?;
    let secret = db
        .get_resource(
            "v1",
            "Secret",
            Some(BOOTSTRAP_TOKEN_NAMESPACE),
            scope.secret_name(),
        )
        .await?;

    if let Some(secret) = &secret
        && matches_token_secret(&secret.data, &token_id, &token_secret).unwrap_or(false)
    {
        validate_fixed_bootstrap_token_secret(secret, token, Some(scope))?;
        return validate_bootstrap_token_secret(secret, &token_id, &token_secret);
    }

    let other_scope = scope.other();
    if let Some(other_secret) = db
        .get_resource(
            "v1",
            "Secret",
            Some(BOOTSTRAP_TOKEN_NAMESPACE),
            other_scope.secret_name(),
        )
        .await?
        && matches_token_secret(&other_secret.data, &token_id, &token_secret).unwrap_or(false)
    {
        validate_fixed_bootstrap_token_secret(&other_secret, token, Some(other_scope))?;
        return Err(anyhow!("token is not a {}", scope.error_name()));
    }

    match secret {
        Some(secret) => validate_bootstrap_token_secret(&secret, &token_id, &token_secret),
        None => Err(anyhow!("{} not found", scope.error_name())),
    }
}

fn validate_fixed_bootstrap_token_secret(
    secret: &Resource,
    token: &str,
    expected_scope: Option<BootstrapTokenScope>,
) -> Result<()> {
    let (token_id, token_secret) = parse_bootstrap_token(token)?;
    validate_bootstrap_token_secret(secret, &token_id, &token_secret)?;
    if let Some(scope) = expected_scope {
        let groups = optional_decoded_data_field(&secret.data, "auth-extra-groups")?;
        let has_scope = groups
            .as_deref()
            .unwrap_or("")
            .split(',')
            .map(str::trim)
            .any(|group| group == scope.auth_group());
        if !has_scope {
            return Err(anyhow!("token is not a {}", scope.error_name()));
        }
        if secret.name != scope.secret_name() {
            return Err(anyhow!("token is not stored as {}", scope.secret_name()));
        }
    }
    Ok(())
}

fn validate_bootstrap_token_secret(
    secret: &Resource,
    token_id: &str,
    token_secret: &str,
) -> Result<BootstrapTokenIdentity> {
    if secret.data.get("type").and_then(|value| value.as_str()) != Some(BOOTSTRAP_TOKEN_SECRET_TYPE)
    {
        return Err(anyhow!("bootstrap token {token_id} has wrong Secret type"));
    }

    let stored_id = decode_data_field(&secret.data, "token-id")?;
    let stored_secret = decode_data_field(&secret.data, "token-secret")?;
    // token-id is public (it is the Secret's name, used for the lookup above);
    // the token-secret is compared in constant time to avoid a timing oracle.
    let id_ok = stored_id == token_id;
    let secret_ok = constant_time_eq(stored_secret.as_bytes(), token_secret.as_bytes());
    if !(id_ok && secret_ok) {
        return Err(anyhow!("invalid bootstrap token"));
    }

    let usage = decode_data_field(&secret.data, "usage-bootstrap-authentication")?;
    if usage != "true" {
        return Err(anyhow!(
            "bootstrap token {token_id} does not allow usage-bootstrap-authentication"
        ));
    }

    if let Some(expiration) = optional_decoded_data_field(&secret.data, "expiration")? {
        let expires_at = OffsetDateTime::parse(&expiration, &Rfc3339)
            .with_context(|| format!("invalid bootstrap token expiration {expiration:?}"))?;
        if expires_at <= OffsetDateTime::now_utc() {
            return Err(anyhow!("bootstrap token {token_id} expired"));
        }
    }

    let mut extra_groups = Vec::new();
    if let Some(raw) = optional_decoded_data_field(&secret.data, "auth-extra-groups")? {
        for group in raw.split(',').map(str::trim).filter(|g| !g.is_empty()) {
            // Phase 2B: reject invalid extra groups instead of silently filtering.
            // Only system:bootstrappers:* groups are allowed.
            if !group.starts_with("system:bootstrappers:") {
                return Err(anyhow!(
                    "bootstrap token {token_id}: invalid auth-extra-group {group:?} (must start with system:bootstrappers:)"
                ));
            }
            extra_groups.push(group.to_string());
        }
    }

    Ok(BootstrapTokenIdentity {
        token_id: token_id.to_string(),
        extra_groups,
    })
}

#[cfg(test)]
pub async fn ensure_default_bootstrap_token(
    db: &dyn DatastoreBackend,
    _ttl: std::time::Duration,
) -> Result<String> {
    ensure_worker_bootstrap_token(db).await
}

pub async fn rotate_bootstrap_token_secret_for_get(
    db: &dyn DatastoreBackend,
    resource: &Resource,
) -> Result<Resource> {
    let Some(scope) = fixed_secret_scope(&resource.namespace, &resource.name) else {
        return Ok(resource.clone());
    };
    let Some(expiration) = optional_decoded_data_field(&resource.data, "expiration")? else {
        let token = generate_bootstrap_token();
        write_scoped_bootstrap_token_secret(db, scope, &token, BOOTSTRAP_TOKEN_TTL).await?;
        return read_fixed_secret(db, scope).await;
    };
    let expires_at = OffsetDateTime::parse(&expiration, &Rfc3339)
        .with_context(|| format!("invalid bootstrap token expiration {expiration:?}"))?;
    let now = OffsetDateTime::now_utc();
    let rotate_before =
        time::Duration::try_from(BOOTSTRAP_TOKEN_ROTATE_BEFORE).context("rotation threshold")?;
    if expires_at - now >= rotate_before {
        return Ok(resource.clone());
    }

    let token = generate_bootstrap_token();
    write_scoped_bootstrap_token_secret(db, scope, &token, BOOTSTRAP_TOKEN_TTL).await?;
    read_fixed_secret(db, scope).await
}

fn fixed_secret_scope(namespace: &Option<String>, name: &str) -> Option<BootstrapTokenScope> {
    if namespace.as_deref() != Some(BOOTSTRAP_TOKEN_NAMESPACE) {
        return None;
    }
    match name {
        WORKER_BOOTSTRAP_TOKEN_SECRET_NAME => Some(BootstrapTokenScope::Worker),
        CONTROLPLANE_BOOTSTRAP_TOKEN_SECRET_NAME => Some(BootstrapTokenScope::Controlplane),
        _ => None,
    }
}

async fn read_fixed_secret(
    db: &dyn DatastoreBackend,
    scope: BootstrapTokenScope,
) -> Result<Resource> {
    db.get_resource(
        "v1",
        "Secret",
        Some(BOOTSTRAP_TOKEN_NAMESPACE),
        scope.secret_name(),
    )
    .await?
    .ok_or_else(|| anyhow!("{} not found after rotation", scope.secret_name()))
}

fn token_from_secret(data: &serde_json::Value) -> Result<String> {
    let token_id = decode_data_field(data, "token-id")?;
    let token_secret = decode_data_field(data, "token-secret")?;
    Ok(format!("{token_id}.{token_secret}"))
}

fn matches_token_secret(
    data: &serde_json::Value,
    token_id: &str,
    token_secret: &str,
) -> Result<bool> {
    let stored_id = decode_data_field(data, "token-id")?;
    let stored_secret = decode_data_field(data, "token-secret")?;
    Ok(
        stored_id == token_id
            && constant_time_eq(stored_secret.as_bytes(), token_secret.as_bytes()),
    )
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

fn parse_bootstrap_token(token: &str) -> Result<(String, String)> {
    let (token_id, token_secret) = token
        .split_once('.')
        .ok_or_else(|| anyhow!("bootstrap token must have <id>.<secret> format"))?;
    if token_id.len() != 6 || token_secret.len() != 16 {
        return Err(anyhow!(
            "bootstrap token must have 6-character id and 16-character secret"
        ));
    }
    if token_secret.contains('.') {
        return Err(anyhow!("bootstrap token must contain exactly one dot"));
    }
    if !token_id
        .chars()
        .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit())
        || !token_secret
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit())
    {
        return Err(anyhow!(
            "bootstrap token id and secret must be lowercase alphanumeric"
        ));
    }
    Ok((token_id.to_string(), token_secret.to_string()))
}

fn expiration_timestamp(ttl: std::time::Duration) -> Result<String> {
    let expires_at = if ttl.is_zero() {
        OffsetDateTime::now_utc() - time::Duration::seconds(1)
    } else {
        OffsetDateTime::now_utc()
            + time::Duration::try_from(ttl).context("bootstrap token ttl out of range")?
    };
    expires_at
        .format(&Rfc3339)
        .context("format bootstrap token expiration")
}

fn encode_data(value: &str) -> String {
    STANDARD.encode(value.as_bytes())
}

fn decode_data_field(data: &serde_json::Value, key: &str) -> Result<String> {
    optional_decoded_data_field(data, key)?.ok_or_else(|| anyhow!("bootstrap token missing {key}"))
}

fn optional_decoded_data_field(data: &serde_json::Value, key: &str) -> Result<Option<String>> {
    let Some(encoded) = data
        .pointer(&format!("/data/{key}"))
        .and_then(|value| value.as_str())
    else {
        return Ok(None);
    };
    let bytes = STANDARD
        .decode(encoded)
        .with_context(|| format!("bootstrap token field {key} is not valid base64"))?;
    String::from_utf8(bytes)
        .with_context(|| format!("bootstrap token field {key} is not utf-8"))
        .map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn constant_time_eq_matches_semantics() {
        assert!(constant_time_eq(b"abcdef01", b"abcdef01"));
        assert!(!constant_time_eq(b"abcdef01", b"abcdef02"));
        assert!(!constant_time_eq(b"abc", b"abcd"));
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn generate_bootstrap_token_uses_kubernetes_format() {
        let token = generate_bootstrap_token();
        let (id, secret) = token.split_once('.').expect("token contains dot");
        assert_eq!(id.len(), 6);
        assert_eq!(secret.len(), 16);
        assert!(
            id.chars()
                .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit()),
            "token id must be lowercase alphanumeric"
        );
        assert!(
            secret
                .chars()
                .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit()),
            "token secret must be lowercase alphanumeric"
        );
    }

    #[tokio::test]
    async fn fixed_worker_and_controlplane_token_secret_names_validate() {
        let db = crate::datastore::test_support::in_memory().await;
        create_scoped_bootstrap_token_secret_for_test(
            &db,
            BootstrapTokenScope::Worker,
            "abcdef.0123456789abcdef",
        )
        .await
        .unwrap();
        create_scoped_bootstrap_token_secret_for_test(
            &db,
            BootstrapTokenScope::Controlplane,
            "123456.fedcba9876543210",
        )
        .await
        .unwrap();

        let worker = validate_bootstrap_token(&db, "abcdef.0123456789abcdef")
            .await
            .expect("fixed worker token Secret should validate");
        assert_eq!(
            worker.extra_groups,
            vec!["system:bootstrappers:klights:worker"]
        );
        let controlplane = validate_bootstrap_token(&db, "123456.fedcba9876543210")
            .await
            .expect("fixed controlplane token Secret should validate");
        assert_eq!(
            controlplane.extra_groups,
            vec!["system:bootstrappers:klights:controlplane"]
        );

        assert!(
            db.get_resource(
                "v1",
                "Secret",
                Some("kube-system"),
                "bootstrap-token-abcdef",
            )
            .await
            .unwrap()
            .is_none(),
            "production bootstrap tokens must not use bootstrap-token-<id> names"
        );
    }

    #[tokio::test]
    async fn scoped_bootstrap_token_validation_rejects_wrong_join_scope() {
        let db = crate::datastore::test_support::in_memory().await;
        create_scoped_bootstrap_token_secret_for_test(
            &db,
            BootstrapTokenScope::Controlplane,
            "abcdef.0123456789abcdef",
        )
        .await
        .unwrap();
        let err = validate_bootstrap_token_for_scope(
            &db,
            "abcdef.0123456789abcdef",
            BootstrapTokenScope::Worker,
        )
        .await
        .expect_err("controlplane token must not validate for worker joins");
        assert!(err.to_string().contains("worker bootstrap token"));

        let db = crate::datastore::test_support::in_memory().await;
        create_scoped_bootstrap_token_secret_for_test(
            &db,
            BootstrapTokenScope::Worker,
            "123456.fedcba9876543210",
        )
        .await
        .unwrap();
        let err = validate_bootstrap_token_for_scope(
            &db,
            "123456.fedcba9876543210",
            BootstrapTokenScope::Controlplane,
        )
        .await
        .expect_err("worker token must not validate for controlplane joins");
        assert!(err.to_string().contains("controlplane bootstrap token"));
    }

    #[tokio::test]
    async fn get_rotates_fixed_token_when_less_than_fifteen_minutes_remain() {
        let db = crate::datastore::test_support::in_memory().await;
        create_scoped_bootstrap_token_secret_with_ttl_for_test(
            &db,
            BootstrapTokenScope::Worker,
            "abcdef.0123456789abcdef",
            std::time::Duration::from_secs(14 * 60),
        )
        .await
        .unwrap();
        let before = db
            .get_resource(
                "v1",
                "Secret",
                Some("kube-system"),
                WORKER_BOOTSTRAP_TOKEN_SECRET_NAME,
            )
            .await
            .unwrap()
            .unwrap();

        let after = rotate_bootstrap_token_secret_for_get(&db, &before)
            .await
            .unwrap();
        let old_token = token_from_secret(&before.data).unwrap();
        let new_token = token_from_secret(&after.data).unwrap();

        assert_ne!(old_token, new_token, "GET renewal must rotate token bytes");
        assert!(
            validate_bootstrap_token_for_scope(&db, &new_token, BootstrapTokenScope::Worker)
                .await
                .is_ok()
        );
        let expiration = optional_decoded_data_field(&after.data, "expiration")
            .unwrap()
            .unwrap();
        let expires_at = OffsetDateTime::parse(&expiration, &Rfc3339).unwrap();
        let remaining = expires_at - OffsetDateTime::now_utc();
        assert!(
            remaining >= time::Duration::minutes(29),
            "rotated token should receive a fresh 30 minute ttl, remaining={remaining:?}"
        );
    }

    #[tokio::test]
    async fn get_keeps_fixed_token_when_more_than_fifteen_minutes_remain() {
        let db = crate::datastore::test_support::in_memory().await;
        create_scoped_bootstrap_token_secret_with_ttl_for_test(
            &db,
            BootstrapTokenScope::Controlplane,
            "123456.fedcba9876543210",
            std::time::Duration::from_secs(16 * 60),
        )
        .await
        .unwrap();
        let before = db
            .get_resource(
                "v1",
                "Secret",
                Some("kube-system"),
                CONTROLPLANE_BOOTSTRAP_TOKEN_SECRET_NAME,
            )
            .await
            .unwrap()
            .unwrap();

        let after = rotate_bootstrap_token_secret_for_get(&db, &before)
            .await
            .unwrap();

        assert_eq!(
            token_from_secret(&before.data).unwrap(),
            token_from_secret(&after.data).unwrap()
        );
    }

    #[tokio::test]
    async fn validate_bootstrap_token_rejects_expired_secret() {
        let db = crate::datastore::test_support::in_memory().await;
        create_scoped_bootstrap_token_secret_with_ttl_for_test(
            &db,
            BootstrapTokenScope::Worker,
            "abcdef.0123456789abcdef",
            std::time::Duration::from_secs(0),
        )
        .await
        .unwrap();

        let err = validate_bootstrap_token(&db, "abcdef.0123456789abcdef")
            .await
            .expect_err("expired token must fail validation");
        assert!(err.to_string().contains("expired"));
    }

    #[tokio::test]
    async fn validate_bootstrap_token_rejects_secret_without_authentication_usage() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Secret",
            Some("kube-system"),
            WORKER_BOOTSTRAP_TOKEN_SECRET_NAME,
            json!({
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {
                    "namespace": "kube-system",
                    "name": WORKER_BOOTSTRAP_TOKEN_SECRET_NAME
                },
                "type": "bootstrap.kubernetes.io/token",
                "data": {
                    "token-id": "YWJjZGVm",
                    "token-secret": "MDEyMzQ1Njc4OWFiY2RlZg==",
                    "usage-bootstrap-authentication": "ZmFsc2U="
                }
            }),
        )
        .await
        .unwrap();

        let err = validate_bootstrap_token(&db, "abcdef.0123456789abcdef")
            .await
            .expect_err("token without auth usage must fail validation");
        assert!(err.to_string().contains("usage-bootstrap-authentication"));
    }

    #[tokio::test]
    async fn ensure_default_bootstrap_token_reuses_live_default_token() {
        let db = crate::datastore::test_support::in_memory().await;

        let first = ensure_default_bootstrap_token(&db, std::time::Duration::from_secs(3600))
            .await
            .unwrap();
        let second = ensure_default_bootstrap_token(&db, std::time::Duration::from_secs(3600))
            .await
            .unwrap();

        assert_eq!(first, second);
        validate_bootstrap_token(&db, &first).await.unwrap();
    }

    #[tokio::test]
    async fn validate_bootstrap_token_rejects_invalid_extra_groups() {
        // Phase 2B: bootstrap tokens must reject auth-extra-groups that don't
        // start with system:bootstrappers:.
        let db = crate::datastore::test_support::in_memory().await;
        let token_id = "abcdef";
        let secret_val = "0123456789abcdef";
        db.create_resource(
            "v1",
            "Secret",
            Some("kube-system"),
            WORKER_BOOTSTRAP_TOKEN_SECRET_NAME,
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Secret",
                "type": "bootstrap.kubernetes.io/token",
                "metadata": {
                    "name": WORKER_BOOTSTRAP_TOKEN_SECRET_NAME,
                    "namespace": "kube-system"
                },
                "data": {
                    "token-id": encode_data(token_id),
                    "token-secret": encode_data(secret_val),
                    "usage-bootstrap-authentication": encode_data("true"),
                    "auth-extra-groups": encode_data("system:nodes,system:bootstrappers:nodes")
                }
            }),
        )
        .await
        .unwrap();

        let token = format!("{token_id}.{secret_val}");
        let err = validate_bootstrap_token(&db, &token).await.unwrap_err();
        assert!(
            err.to_string().contains("system:nodes"),
            "error should mention the invalid group: {err}"
        );
    }
}
