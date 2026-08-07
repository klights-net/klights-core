//! Framework-neutral Kubernetes bootstrap-token policy.

use anyhow::{Context, Result, anyhow};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::Value;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

pub use klights_leader_api::{BootstrapTokenIdentity, BootstrapTokenScope};

pub const BOOTSTRAP_TOKEN_SECRET_TYPE: &str = "bootstrap.kubernetes.io/token";
pub const BOOTSTRAP_TOKEN_NAMESPACE: &str = "kube-system";
pub const WORKER_BOOTSTRAP_TOKEN_SECRET_NAME: &str = "worker-bootstrap-token";
pub const CONTROLPLANE_BOOTSTRAP_TOKEN_SECRET_NAME: &str = "controlplane-bootstrap-token";
pub const BOOTSTRAP_TOKEN_TTL: std::time::Duration = std::time::Duration::from_secs(30 * 60);
pub const BOOTSTRAP_TOKEN_ROTATE_BEFORE: std::time::Duration =
    std::time::Duration::from_secs(15 * 60);

pub trait BootstrapTokenScopePolicy {
    fn secret_name(self) -> &'static str;
    fn label_value(self) -> &'static str;
    fn auth_group(self) -> &'static str;
    fn description(self) -> &'static str;
    fn error_name(self) -> &'static str;
    fn other(self) -> Self;
}

impl BootstrapTokenScopePolicy for BootstrapTokenScope {
    fn secret_name(self) -> &'static str {
        match self {
            Self::Worker => WORKER_BOOTSTRAP_TOKEN_SECRET_NAME,
            Self::Controlplane => CONTROLPLANE_BOOTSTRAP_TOKEN_SECRET_NAME,
        }
    }

    fn label_value(self) -> &'static str {
        match self {
            Self::Worker => "worker",
            Self::Controlplane => "controlplane",
        }
    }

    fn auth_group(self) -> &'static str {
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParsedBootstrapToken {
    pub token_id: String,
    pub token_secret: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BootstrapTokenReadAction {
    Keep,
    RewriteLegacy,
    Rotate,
}

#[derive(Clone, Copy, Debug)]
pub struct BootstrapTokenSecret<'a> {
    pub namespace: Option<&'a str>,
    pub name: &'a str,
    pub data: &'a Value,
}

impl<'a> BootstrapTokenSecret<'a> {
    pub fn from_value(value: &'a Value) -> Result<Self> {
        let name = value
            .pointer("/metadata/name")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("bootstrap token Secret missing metadata.name"))?;
        let namespace = value.pointer("/metadata/namespace").and_then(Value::as_str);
        Ok(Self {
            namespace,
            name,
            data: value,
        })
    }

    pub fn read_action_at(self, now: OffsetDateTime) -> Result<BootstrapTokenReadAction> {
        let Some(expiration) = optional_secret_field(self.data, "expiration")? else {
            return Ok(BootstrapTokenReadAction::Rotate);
        };
        let expires_at = OffsetDateTime::parse(&expiration, &Rfc3339)
            .with_context(|| format!("invalid bootstrap token expiration {expiration:?}"))?;
        let rotate_before = time::Duration::try_from(BOOTSTRAP_TOKEN_ROTATE_BEFORE)
            .context("rotation threshold")?;
        if expires_at - now < rotate_before {
            return Ok(BootstrapTokenReadAction::Rotate);
        }
        if has_single_token_data_field(self.data) {
            Ok(BootstrapTokenReadAction::Keep)
        } else {
            Ok(BootstrapTokenReadAction::RewriteLegacy)
        }
    }
}

/// Compare fixed-length token secrets without data-dependent early exit.
pub fn constant_time_token_secret_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut difference = 0u8;
    for (left, right) in left.iter().zip(right.iter()) {
        difference |= left ^ right;
    }
    difference == 0
}

pub fn generate_bootstrap_token(id_entropy: [u8; 3], secret_entropy: [u8; 8]) -> String {
    format!("{}.{}", hex_lower(&id_entropy), hex_lower(&secret_entropy))
}

pub fn parse_bootstrap_token(token: &str) -> Result<ParsedBootstrapToken> {
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
        .all(|character| character.is_ascii_lowercase() || character.is_ascii_digit())
        || !token_secret
            .chars()
            .all(|character| character.is_ascii_lowercase() || character.is_ascii_digit())
    {
        return Err(anyhow!(
            "bootstrap token id and secret must be lowercase alphanumeric"
        ));
    }
    Ok(ParsedBootstrapToken {
        token_id: token_id.to_string(),
        token_secret: token_secret.to_string(),
    })
}

pub fn build_scoped_bootstrap_token_secret_at(
    scope: BootstrapTokenScope,
    token: &str,
    ttl: std::time::Duration,
    now: OffsetDateTime,
) -> Result<Value> {
    let _ = parse_bootstrap_token(token)?;
    let expiration = expiration_timestamp_at(ttl, now)?;
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
            "token": encode_data(token),
            "description": encode_data(scope.description()),
            "expiration": encode_data(&expiration),
            "usage-bootstrap-authentication": encode_data("true"),
            "usage-bootstrap-signing": encode_data("true"),
            "auth-extra-groups": encode_data(scope.auth_group())
        }
    }))
}

pub fn fixed_secret_scope(namespace: Option<&str>, name: &str) -> Option<BootstrapTokenScope> {
    if namespace != Some(BOOTSTRAP_TOKEN_NAMESPACE) {
        return None;
    }
    match name {
        WORKER_BOOTSTRAP_TOKEN_SECRET_NAME => Some(BootstrapTokenScope::Worker),
        CONTROLPLANE_BOOTSTRAP_TOKEN_SECRET_NAME => Some(BootstrapTokenScope::Controlplane),
        _ => None,
    }
}

pub fn bootstrap_token_matches(data: &Value, token: &str) -> Result<bool> {
    let supplied = parse_bootstrap_token(token)?;
    let stored = token_parts_from_secret_data(data)?;
    Ok(stored.token_id == supplied.token_id
        && constant_time_token_secret_eq(
            stored.token_secret.as_bytes(),
            supplied.token_secret.as_bytes(),
        ))
}

pub fn validate_bootstrap_token_secret_at(
    secret: BootstrapTokenSecret<'_>,
    token: &str,
    expected_scope: Option<BootstrapTokenScope>,
    now: OffsetDateTime,
) -> Result<BootstrapTokenIdentity> {
    let supplied = parse_bootstrap_token(token)?;
    if secret.data.get("type").and_then(Value::as_str) != Some(BOOTSTRAP_TOKEN_SECRET_TYPE) {
        return Err(anyhow!(
            "bootstrap token {} has wrong Secret type",
            supplied.token_id
        ));
    }

    let stored = token_parts_from_secret_data(secret.data)?;
    let id_matches = stored.token_id == supplied.token_id;
    let secret_matches = constant_time_token_secret_eq(
        stored.token_secret.as_bytes(),
        supplied.token_secret.as_bytes(),
    );
    if !(id_matches && secret_matches) {
        return Err(anyhow!("invalid bootstrap token"));
    }

    let usage = secret_field(secret.data, "usage-bootstrap-authentication")?;
    if usage != "true" {
        return Err(anyhow!(
            "bootstrap token {} does not allow usage-bootstrap-authentication",
            supplied.token_id
        ));
    }
    if let Some(expiration) = optional_secret_field(secret.data, "expiration")? {
        let expires_at = OffsetDateTime::parse(&expiration, &Rfc3339)
            .with_context(|| format!("invalid bootstrap token expiration {expiration:?}"))?;
        if expires_at <= now {
            return Err(anyhow!("bootstrap token {} expired", supplied.token_id));
        }
    }

    let mut extra_groups = Vec::new();
    if let Some(groups) = optional_secret_field(secret.data, "auth-extra-groups")? {
        for group in groups
            .split(',')
            .map(str::trim)
            .filter(|group| !group.is_empty())
        {
            if !group.starts_with("system:bootstrappers:") {
                return Err(anyhow!(
                    "bootstrap token {}: invalid auth-extra-group {group:?} (must start with system:bootstrappers:)",
                    supplied.token_id
                ));
            }
            extra_groups.push(group.to_string());
        }
    }

    if let Some(scope) = expected_scope {
        if secret.namespace != Some(BOOTSTRAP_TOKEN_NAMESPACE) || secret.name != scope.secret_name()
        {
            return Err(anyhow!("token is not stored as {}", scope.secret_name()));
        }
        if !extra_groups.iter().any(|group| group == scope.auth_group()) {
            return Err(anyhow!("token is not a {}", scope.error_name()));
        }
    }

    BootstrapTokenIdentity::try_new(supplied.token_id, extra_groups).map_err(anyhow::Error::from)
}

pub fn token_from_secret(data: &Value) -> Result<String> {
    if let Some(token) = optional_secret_field(data, "token")? {
        let _ = parse_bootstrap_token(&token)?;
        return Ok(token);
    }
    let parts = legacy_token_parts_from_secret_data(data)?;
    Ok(format!("{}.{}", parts.token_id, parts.token_secret))
}

pub fn has_single_token_data_field(data: &Value) -> bool {
    secret_field_exists(data, "token")
        && !secret_field_exists(data, "token-id")
        && !secret_field_exists(data, "token-secret")
}

pub fn migrate_legacy_token_fields(data: &mut Value, token: &str) -> Result<()> {
    let _ = parse_bootstrap_token(token)?;
    let field_name = if data.get("stringData").is_some() && data.get("data").is_none() {
        "stringData"
    } else {
        "data"
    };
    let fields = data
        .get_mut(field_name)
        .and_then(Value::as_object_mut)
        .ok_or_else(|| anyhow!("bootstrap token Secret missing {field_name} object"))?;
    fields.insert(
        "token".to_string(),
        Value::String(if field_name == "data" {
            encode_data(token)
        } else {
            token.to_string()
        }),
    );
    fields.remove("token-id");
    fields.remove("token-secret");
    if let Some(other_fields) = data
        .get_mut(if field_name == "data" {
            "stringData"
        } else {
            "data"
        })
        .and_then(Value::as_object_mut)
    {
        other_fields.remove("token-id");
        other_fields.remove("token-secret");
    }
    Ok(())
}

fn token_parts_from_secret_data(data: &Value) -> Result<ParsedBootstrapToken> {
    if let Some(token) = optional_secret_field(data, "token")? {
        return parse_bootstrap_token(&token);
    }
    legacy_token_parts_from_secret_data(data)
}

fn legacy_token_parts_from_secret_data(data: &Value) -> Result<ParsedBootstrapToken> {
    Ok(ParsedBootstrapToken {
        token_id: secret_field(data, "token-id")?,
        token_secret: secret_field(data, "token-secret")?,
    })
}

fn secret_field(data: &Value, key: &str) -> Result<String> {
    optional_secret_field(data, key)?.ok_or_else(|| anyhow!("bootstrap token missing {key}"))
}

fn optional_secret_field(data: &Value, key: &str) -> Result<Option<String>> {
    if let Some(value) = data
        .pointer(&format!("/stringData/{key}"))
        .and_then(Value::as_str)
    {
        return Ok(Some(value.to_string()));
    }
    let Some(encoded) = data
        .pointer(&format!("/data/{key}"))
        .and_then(Value::as_str)
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

fn secret_field_exists(data: &Value, key: &str) -> bool {
    data.pointer(&format!("/stringData/{key}")).is_some()
        || data.pointer(&format!("/data/{key}")).is_some()
}

fn expiration_timestamp_at(ttl: std::time::Duration, now: OffsetDateTime) -> Result<String> {
    let expires_at = if ttl.is_zero() {
        now - time::Duration::seconds(1)
    } else {
        now + time::Duration::try_from(ttl).context("bootstrap token ttl out of range")?
    };
    expires_at
        .format(&Rfc3339)
        .context("format bootstrap token expiration")
}

fn encode_data(value: &str) -> String {
    STANDARD.encode(value.as_bytes())
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write;
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use time::Duration;

    const WORKER_TOKEN: &str = "abcdef.0123456789abcdef";
    const CONTROLPLANE_TOKEN: &str = "123456.fedcba9876543210";

    fn encoded(value: &str) -> String {
        STANDARD.encode(value)
    }

    fn now() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_800_000_000).unwrap()
    }

    #[test]
    fn token_parser_and_constant_time_comparison_are_fail_closed() {
        assert_eq!(
            generate_bootstrap_token(
                [0xab, 0xcd, 0xef],
                [0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef],
            ),
            WORKER_TOKEN
        );
        for (token, accepted) in [
            (WORKER_TOKEN, true),
            ("abcde.0123456789abcdef", false),
            ("abcdef.0123456789abcde", false),
            ("ABCDEF.0123456789abcdef", false),
            ("abcdef.0123456789abcdef.extra", false),
            ("abcdef0123456789abcdef", false),
        ] {
            assert_eq!(parse_bootstrap_token(token).is_ok(), accepted, "{token}");
        }
        for (left, right, equal) in [
            (
                b"0123456789abcdef".as_slice(),
                b"0123456789abcdef".as_slice(),
                true,
            ),
            (
                b"0123456789abcdef".as_slice(),
                b"0123456789abcdee".as_slice(),
                false,
            ),
            (b"short".as_slice(), b"different".as_slice(), false),
        ] {
            assert_eq!(constant_time_token_secret_eq(left, right), equal);
        }
    }

    #[test]
    fn scoped_policy_covers_worker_and_controlplane_names_groups_and_type() {
        for (scope, token, expected_name, expected_group) in [
            (
                BootstrapTokenScope::Worker,
                WORKER_TOKEN,
                "worker-bootstrap-token",
                "system:bootstrappers:klights:worker",
            ),
            (
                BootstrapTokenScope::Controlplane,
                CONTROLPLANE_TOKEN,
                "controlplane-bootstrap-token",
                "system:bootstrappers:klights:controlplane",
            ),
        ] {
            let value = build_scoped_bootstrap_token_secret_at(
                scope,
                token,
                std::time::Duration::from_secs(30 * 60),
                now(),
            )
            .unwrap();
            let secret = BootstrapTokenSecret::from_value(&value).unwrap();
            let identity =
                validate_bootstrap_token_secret_at(secret, token, Some(scope), now()).unwrap();
            assert_eq!(secret.name, expected_name);
            assert_eq!(value["type"], BOOTSTRAP_TOKEN_SECRET_TYPE);
            assert_eq!(identity.extra_groups(), &[expected_group.to_string()]);

            let wrong_scope = scope.other();
            assert!(
                validate_bootstrap_token_secret_at(secret, token, Some(wrong_scope), now())
                    .is_err()
            );
        }
    }

    #[test]
    fn secret_fields_accept_kubernetes_data_and_string_data_forms() {
        for fields in [
            json!({
                "data": {
                    "token": encoded(WORKER_TOKEN),
                    "usage-bootstrap-authentication": encoded("true"),
                    "auth-extra-groups": encoded(BootstrapTokenScope::Worker.auth_group()),
                }
            }),
            json!({
                "stringData": {
                    "token": WORKER_TOKEN,
                    "usage-bootstrap-authentication": "true",
                    "auth-extra-groups": BootstrapTokenScope::Worker.auth_group(),
                }
            }),
        ] {
            let mut value = json!({
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {"namespace": "kube-system", "name": "worker-bootstrap-token"},
                "type": BOOTSTRAP_TOKEN_SECRET_TYPE,
            });
            value
                .as_object_mut()
                .unwrap()
                .extend(fields.as_object().unwrap().clone());
            let secret = BootstrapTokenSecret::from_value(&value).unwrap();
            assert_eq!(token_from_secret(secret.data).unwrap(), WORKER_TOKEN);
            validate_bootstrap_token_secret_at(
                secret,
                WORKER_TOKEN,
                Some(BootstrapTokenScope::Worker),
                now(),
            )
            .unwrap();
        }
    }

    #[test]
    fn expiry_and_rotate_before_decisions_are_deterministic() {
        for (ttl_seconds, expected) in [
            (0, BootstrapTokenReadAction::Rotate),
            (14 * 60, BootstrapTokenReadAction::Rotate),
            (15 * 60, BootstrapTokenReadAction::Keep),
            (16 * 60, BootstrapTokenReadAction::Keep),
        ] {
            let value = build_scoped_bootstrap_token_secret_at(
                BootstrapTokenScope::Worker,
                WORKER_TOKEN,
                std::time::Duration::from_secs(ttl_seconds),
                now(),
            )
            .unwrap();
            let secret = BootstrapTokenSecret::from_value(&value).unwrap();
            assert_eq!(secret.read_action_at(now()).unwrap(), expected);
            assert_eq!(
                validate_bootstrap_token_secret_at(
                    secret,
                    WORKER_TOKEN,
                    Some(BootstrapTokenScope::Worker),
                    now(),
                )
                .is_ok(),
                ttl_seconds != 0,
            );
        }
        assert_eq!(
            BOOTSTRAP_TOKEN_ROTATE_BEFORE,
            std::time::Duration::from_secs(15 * 60)
        );
    }

    #[test]
    fn legacy_split_fields_validate_and_migrate_without_changing_token() {
        let mut value = json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"namespace": "kube-system", "name": "worker-bootstrap-token"},
            "type": BOOTSTRAP_TOKEN_SECRET_TYPE,
            "data": {
                "token-id": encoded("abcdef"),
                "token-secret": encoded("0123456789abcdef"),
                "usage-bootstrap-authentication": encoded("true"),
                "auth-extra-groups": encoded(BootstrapTokenScope::Worker.auth_group()),
                "expiration": encoded(&(now() + Duration::minutes(30)).format(&Rfc3339).unwrap()),
            }
        });
        let before = BootstrapTokenSecret::from_value(&value).unwrap();
        assert_eq!(token_from_secret(before.data).unwrap(), WORKER_TOKEN);
        assert_eq!(
            before.read_action_at(now()).unwrap(),
            BootstrapTokenReadAction::RewriteLegacy
        );

        migrate_legacy_token_fields(&mut value, WORKER_TOKEN).unwrap();
        let after = BootstrapTokenSecret::from_value(&value).unwrap();
        assert_eq!(token_from_secret(after.data).unwrap(), WORKER_TOKEN);
        assert_eq!(
            after.read_action_at(now()).unwrap(),
            BootstrapTokenReadAction::Keep
        );
        assert!(value.pointer("/data/token-id").is_none());
        assert!(value.pointer("/data/token-secret").is_none());
    }
}
