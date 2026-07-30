pub const DEFAULT_TLS_PORT: u16 = 7679;
pub const DEFAULT_API_SLOW_LOG_MS: u64 = 250;
pub const DEFAULT_NODE_NOT_READY_POD_EVICTION_GRACE_SECONDS: i64 = 0;
pub const DEFAULT_MAX_WATCH_EVENTS: i64 = 100_000;
pub const DEFAULT_GC_INTERVAL_SECONDS: u64 = 30;

use crate::datastore::backend_kind::BackendKind;

#[derive(Clone, Debug)]
pub struct KlightsConfig {
    pub bridge_name: String,
    pub pod_subnet: String,
    pub cluster_cidr: String,
    pub service_cidr: String,
    pub tls_port: u16,
    /// FQDN for the API server, included as DNS SAN in the TLS server cert.
    pub api_fqdn: Option<String>,
    pub log_file: Option<String>,
    pub containerd_namespace: String,
    pub containerd_socket: Option<String>,
    /// Immutable registry-proxy settings captured once at bootstrap.
    pub registry_proxy: crate::kubelet::registry_proxy::RegistryProxyConfig,
    pub node_name: String,
    /// Node-local IP override for the Kubernetes Node InternalIP and local
    /// endpoint fallback. When unset, startup discovers the host IP.
    pub node_ip: Option<String>,

    /// Whether unauthenticated requests are admitted as system:anonymous.
    pub anonymous_auth: bool,

    /// Dataplane encryption mode. Missing/empty env defaults to enabled.
    pub dataplane_encryption: klights_networking::wireguard::DataplaneEncryption,

    /// Public/ingress endpoint advertised to peers for API joins and encrypted
    /// dataplane reachability.
    pub external_endpoint: Option<String>,

    /// Opt-in for the future outbound-only worker dataplane path. The default
    /// cluster contract is that workers accept inbound WireGuard UDP.
    pub worker_dataplane_no_ingress: bool,

    /// WireGuard overlay device name used when dataplane encryption is enabled.
    pub wireguard_device: String,

    /// WireGuard UDP listen port used when dataplane encryption is enabled.
    pub wireguard_port: u16,

    /// Persistent authoritative cluster datastore path.
    pub cluster_db_path: std::path::PathBuf,

    /// Persistent node-local durability datastore path.
    pub node_db_path: std::path::PathBuf,

    /// Absolute process data root, resolved exactly once during bootstrap.
    pub data_root: std::path::PathBuf,

    /// Immutable API slow-request threshold captured at the root.
    pub api_slow_log_threshold: std::time::Duration,

    /// Nonnegative delay after a Node becomes Unknown before Pod eviction.
    pub node_not_ready_pod_eviction_grace: std::time::Duration,

    /// Positive global watch-history retention window.
    pub max_watch_events: i64,

    /// GC scheduler cadence captured once with the rest of root configuration.
    pub gc_interval: std::time::Duration,

    /// When true, use an in-memory datastore instead of persistent disk.
    pub in_memory: bool,

    /// Database encryption mode.
    pub db_encryption: DbEncryption,

    /// Path to the SQLCipher key file (only used when db_encryption=Sqlcipher).
    /// Defaults to the configured DB root key path when not set.
    pub db_key_file: Option<std::path::PathBuf>,

    /// Cluster datastore backend selection.
    pub datastore_backend: BackendKind,

    /// Node-local durability backend selection.
    pub node_local_backend: BackendKind,

    // ─── Authentication ───────────────────────────────────────────────────
    /// OIDC issuer URL. When set (along with client_id), enables OIDC token auth.
    pub oidc_issuer_url: Option<String>,
    /// OIDC client ID that tokens must be issued for.
    pub oidc_client_id: Option<String>,
    /// JWT claim to use as the username. Defaults to "sub".
    pub oidc_username_claim: String,
    /// JWT claim to use as groups. Defaults to "groups".
    pub oidc_groups_claim: String,
    /// Prefix prepended to all OIDC groups. Defaults to empty.
    pub oidc_groups_prefix: String,
    /// Path to CA bundle PEM for OIDC issuer TLS verification.
    pub oidc_ca_bundle: Option<String>,

    /// Webhook URL for token authentication. When set, enables webhook token auth.
    pub webhook_auth_url: Option<String>,
    /// Path to CA bundle PEM for webhook TLS verification.
    pub webhook_auth_ca_bundle: Option<String>,
    /// Path to client certificate PEM for webhook mTLS.
    pub webhook_auth_client_cert: Option<String>,
    /// Path to client key PEM for webhook mTLS.
    pub webhook_auth_client_key: Option<String>,
    /// Comma-separated audiences. Defaults to K8s default audience.
    pub webhook_auth_audiences: String,
    /// Cache TTL for authenticated TokenReview results, in seconds.
    pub webhook_auth_cache_authorized_ttl_secs: u64,
    /// Cache TTL for unauthenticated/error TokenReview results, in seconds.
    pub webhook_auth_cache_unauthorized_ttl_secs: u64,
}

/// Database encryption mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbEncryption {
    /// Plaintext (default).
    Disabled,
    /// SQLCipher whole-file encryption (requires `sqlcipher` cargo feature).
    Sqlcipher,
}

impl DbEncryption {
    fn from_env() -> Self {
        match std::env::var("KLIGHTS_DB_ENCRYPTION").as_deref() {
            Ok("sqlcipher") => DbEncryption::Sqlcipher,
            _ => DbEncryption::Disabled,
        }
    }
}

fn parse_pod_subnet(raw: &str) -> anyhow::Result<String> {
    klights_types::PodSubnet::parse(raw)
        .map(|subnet| subnet.to_string())
        .map_err(|error| anyhow::anyhow!("invalid KLIGHTS_POD_SUBNET '{}': {}", raw, error))
}

impl KlightsConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        Self::from_env_with_namespace_override(None)
    }

    pub fn from_env_with_namespace_override(
        namespace_override: Option<&str>,
    ) -> anyhow::Result<Self> {
        use crate::networking::BridgeName;
        use anyhow::anyhow;
        use klights_types::{ClusterCidr, NodeName};

        let containerd_namespace = namespace_override.map(str::to_string).unwrap_or_else(|| {
            std::env::var("KLIGHTS_CONTAINERD_NAMESPACE").unwrap_or_else(|_| "klights".to_string())
        });
        let bridge_raw =
            std::env::var("KLIGHTS_BRIDGE_NAME").unwrap_or_else(|_| containerd_namespace.clone());
        // Truncation policy lives in BridgeName::parse_truncating (last 15 chars
        // to preserve suffix uniqueness, e.g. klights-developer-1 vs -2).
        let bridge_name = BridgeName::parse_truncating(&bridge_raw)
            .map_err(|e| anyhow!("invalid KLIGHTS_BRIDGE_NAME '{}': {}", bridge_raw, e))?
            .into_string();
        if bridge_name != bridge_raw {
            tracing::warn!(
                "Bridge name '{}' exceeds 15 char IFNAMSIZ limit, truncated to '{}'",
                bridge_raw,
                bridge_name
            );
        }

        let pod_subnet_raw =
            std::env::var("KLIGHTS_POD_SUBNET").unwrap_or_else(|_| "10.43.0.0/17".to_string());
        // Validate via the typed parser; we keep the canonical String form
        // here so consumers that take &str interop without re-parsing.
        let pod_subnet = parse_pod_subnet(&pod_subnet_raw)?;

        let cluster_raw =
            std::env::var("KLIGHTS_CLUSTER_CIDR").unwrap_or_else(|_| pod_subnet.clone());
        let cluster_cidr = ClusterCidr::parse(&cluster_raw)
            .map_err(|e| anyhow!("invalid KLIGHTS_CLUSTER_CIDR '{}': {}", cluster_raw, e))?
            .to_string();

        let service_raw =
            std::env::var("KLIGHTS_SERVICE_CIDR").unwrap_or_else(|_| "10.43.128.0/17".to_string());
        let service_cidr = ClusterCidr::parse(&service_raw)
            .map_err(|e| anyhow!("invalid KLIGHTS_SERVICE_CIDR '{}': {}", service_raw, e))?
            .to_string();

        let (node_name_source, node_name_raw) = match std::env::var("KLIGHTS_NODE_NAME") {
            Ok(value) => ("KLIGHTS_NODE_NAME", value),
            Err(std::env::VarError::NotPresent) => (
                "hostname",
                hostname::get()
                    .ok()
                    .and_then(|h| h.into_string().ok())
                    .unwrap_or_else(|| "unknown".to_string()),
            ),
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err(anyhow!("KLIGHTS_NODE_NAME must be valid Unicode"));
            }
        };
        let node_name = NodeName::parse(&node_name_raw)
            .map_err(|e| anyhow!("invalid {} '{}': {}", node_name_source, node_name_raw, e))?
            .into_string();

        let datastore_backend = parse_datastore_backend_env()?;
        let node_local_backend = parse_node_local_backend_env()?;
        let data_root = crate::paths::data_root_path(&containerd_namespace);
        let cluster_db_path =
            crate::paths::cluster_db_path(&containerd_namespace, datastore_backend.as_str());
        let node_db_path =
            crate::paths::node_db_path(&containerd_namespace, node_local_backend.as_str());
        let in_memory = parse_bool_env("KLIGHTS_IN_MEMORY", false)?;
        let db_encryption = DbEncryption::from_env();
        let db_key_file = std::env::var("KLIGHTS_DB_KEY_FILE")
            .ok()
            .map(std::path::PathBuf::from);
        let containerd_socket = std::env::var("KLIGHTS_CONTAINERD_SOCKET").ok();
        let registry_proxy_endpoint = std::env::var("KLIGHTS_REGISTRY_PROXY_ENDPOINT").ok();
        let registry_proxy = crate::kubelet::registry_proxy::RegistryProxyConfig::from_inputs(
            parse_bool_env("KLIGHTS_REGISTRY_PROXY_ENABLED", false)?,
            registry_proxy_endpoint.as_deref(),
            containerd_socket.is_some(),
        )?;

        Ok(Self {
            bridge_name,
            pod_subnet,
            cluster_cidr,
            service_cidr,
            tls_port: parse_u16_env("KLIGHTS_TLS_PORT", DEFAULT_TLS_PORT)?,
            api_fqdn: parse_optional_trimmed_env("KLIGHTS_API_FQDN"),
            log_file: std::env::var("KLIGHTS_LOG_FILE").ok(),
            containerd_namespace,
            containerd_socket,
            registry_proxy,
            node_name,
            node_ip: parse_optional_ipv4_env("KLIGHTS_NODE_IP")?,
            anonymous_auth: parse_bool_env("KLIGHTS_ANONYMOUS_AUTH", true)?,
            dataplane_encryption: parse_dataplane_encryption_env()?,
            external_endpoint: parse_optional_trimmed_env("KLIGHTS_EXTERNAL_ENDPOINT"),
            worker_dataplane_no_ingress: parse_bool_env(
                "KLIGHTS_WORKER_DATAPLANE_NO_INGRESS",
                false,
            )?,
            wireguard_device: parse_wireguard_device_env(
                klights_networking::wireguard::DEFAULT_WIREGUARD_DEVICE,
            )?,
            wireguard_port: parse_u16_env(
                "KLIGHTS_WIREGUARD_PORT",
                klights_networking::wireguard::DEFAULT_WIREGUARD_PORT,
            )?,
            cluster_db_path,
            node_db_path,
            data_root,
            api_slow_log_threshold: std::time::Duration::from_millis(
                parse_positive_u64_env_or_default(
                    "KLIGHTS_API_SLOW_LOG_MS",
                    DEFAULT_API_SLOW_LOG_MS,
                ),
            ),
            node_not_ready_pod_eviction_grace: std::time::Duration::from_secs(
                parse_nonnegative_i64_env_or_default(
                    "KLIGHTS_NODE_NOT_READY_POD_EVICTION_GRACE_SECONDS",
                    DEFAULT_NODE_NOT_READY_POD_EVICTION_GRACE_SECONDS,
                ) as u64,
            ),
            max_watch_events: parse_positive_i64_env_or_default(
                "KLIGHTS_MAX_WATCH_EVENTS",
                DEFAULT_MAX_WATCH_EVENTS,
            ),
            gc_interval: std::time::Duration::from_secs(parse_u64_env_or_default(
                "KLIGHTS_GC_INTERVAL_SECS",
                DEFAULT_GC_INTERVAL_SECONDS,
            )),
            in_memory,
            db_encryption,
            db_key_file,
            datastore_backend,
            node_local_backend,

            // ─── Authentication ─────────────────────────────────────────
            oidc_issuer_url: parse_optional_trimmed_env("KLIGHTS_OIDC_ISSUER_URL"),
            oidc_client_id: parse_optional_trimmed_env("KLIGHTS_OIDC_CLIENT_ID"),
            oidc_username_claim: std::env::var("KLIGHTS_OIDC_USERNAME_CLAIM")
                .ok()
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "sub".to_string()),
            oidc_groups_claim: std::env::var("KLIGHTS_OIDC_GROUPS_CLAIM")
                .ok()
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "groups".to_string()),
            oidc_groups_prefix: std::env::var("KLIGHTS_OIDC_GROUPS_PREFIX")
                .ok()
                .unwrap_or_default(),
            oidc_ca_bundle: parse_optional_trimmed_env("KLIGHTS_OIDC_CA_BUNDLE"),
            webhook_auth_url: parse_optional_trimmed_env("KLIGHTS_WEBHOOK_AUTH_URL"),
            webhook_auth_ca_bundle: parse_optional_trimmed_env("KLIGHTS_WEBHOOK_AUTH_CA_BUNDLE"),
            webhook_auth_client_cert: parse_optional_trimmed_env(
                "KLIGHTS_WEBHOOK_AUTH_CLIENT_CERT",
            ),
            webhook_auth_client_key: parse_optional_trimmed_env("KLIGHTS_WEBHOOK_AUTH_CLIENT_KEY"),
            webhook_auth_audiences: std::env::var("KLIGHTS_WEBHOOK_AUTH_AUDIENCES")
                .unwrap_or_default(),
            webhook_auth_cache_authorized_ttl_secs: std::env::var(
                "KLIGHTS_WEBHOOK_AUTH_CACHE_AUTHORIZED_TTL_SECS",
            )
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(300),
            webhook_auth_cache_unauthorized_ttl_secs: std::env::var(
                "KLIGHTS_WEBHOOK_AUTH_CACHE_UNAUTHORIZED_TTL_SECS",
            )
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(30),
        })
    }

    pub fn log_file_path(&self) -> String {
        self.log_file
            .as_deref()
            .map(|value| {
                if value.trim().eq_ignore_ascii_case("true") {
                    self.data_root.join("logs").join("klights.log")
                } else {
                    std::path::PathBuf::from(value)
                }
            })
            .unwrap_or_else(|| {
                self.data_root
                    .join("logs")
                    .join(format!("{}.log", self.bridge_name))
            })
            .to_string_lossy()
            .into_owned()
    }

    /// Test-friendly default with in-memory DB.
    #[cfg(test)]
    pub fn test_default() -> Self {
        let ns = "klights-test";
        Self {
            bridge_name: ns.into(),
            pod_subnet: "10.43.0.0/17".into(),
            cluster_cidr: "10.43.0.0/17".into(),
            service_cidr: "10.43.128.0/17".into(),
            tls_port: DEFAULT_TLS_PORT,
            api_fqdn: None,
            log_file: None,
            containerd_namespace: ns.into(),
            containerd_socket: None,
            registry_proxy: crate::kubelet::registry_proxy::RegistryProxyConfig::from_inputs(
                false, None, false,
            )
            .expect("default registry proxy configuration must be valid"),
            node_name: "test-node".into(),
            node_ip: None,
            anonymous_auth: true,
            dataplane_encryption: klights_networking::wireguard::DataplaneEncryption::Enabled,
            external_endpoint: None,
            worker_dataplane_no_ingress: false,
            wireguard_device: klights_networking::wireguard::DEFAULT_WIREGUARD_DEVICE.into(),
            wireguard_port: klights_networking::wireguard::DEFAULT_WIREGUARD_PORT,
            cluster_db_path: crate::paths::test_data_root_path(ns)
                .join("db")
                .join("sqlite")
                .join("cluster.db"),
            node_db_path: crate::paths::test_data_root_path(ns)
                .join("db")
                .join("sqlite")
                .join("node.db"),
            data_root: crate::paths::test_data_root_path(ns),
            api_slow_log_threshold: std::time::Duration::from_millis(DEFAULT_API_SLOW_LOG_MS),
            node_not_ready_pod_eviction_grace: std::time::Duration::from_secs(
                DEFAULT_NODE_NOT_READY_POD_EVICTION_GRACE_SECONDS as u64,
            ),
            max_watch_events: DEFAULT_MAX_WATCH_EVENTS,
            gc_interval: std::time::Duration::from_secs(DEFAULT_GC_INTERVAL_SECONDS),
            in_memory: true,
            db_encryption: DbEncryption::Disabled,
            db_key_file: None,
            datastore_backend: BackendKind::Sqlite,
            node_local_backend: BackendKind::Sqlite,

            oidc_issuer_url: None,
            oidc_client_id: None,
            oidc_username_claim: "sub".to_string(),
            oidc_groups_claim: "groups".to_string(),
            oidc_groups_prefix: String::new(),
            oidc_ca_bundle: None,
            webhook_auth_url: None,
            webhook_auth_ca_bundle: None,
            webhook_auth_client_cert: None,
            webhook_auth_client_key: None,
            webhook_auth_audiences: String::new(),
            webhook_auth_cache_authorized_ttl_secs: 300,
            webhook_auth_cache_unauthorized_ttl_secs: 30,
        }
    }
}

fn parse_datastore_backend_env() -> anyhow::Result<BackendKind> {
    match std::env::var("KLIGHTS_DATASTORE_BACKEND") {
        Ok(value) => BackendKind::parse(&value),
        Err(std::env::VarError::NotPresent) => match std::env::var("KLIGHTS_BACKEND") {
            Ok(value) => BackendKind::parse(&value),
            Err(std::env::VarError::NotPresent) => Ok(BackendKind::Sqlite),
            Err(std::env::VarError::NotUnicode(_)) => {
                Err(anyhow::anyhow!("KLIGHTS_BACKEND must be valid Unicode"))
            }
        },
        Err(std::env::VarError::NotUnicode(_)) => Err(anyhow::anyhow!(
            "KLIGHTS_DATASTORE_BACKEND must be valid Unicode"
        )),
    }
}

fn parse_node_local_backend_env() -> anyhow::Result<BackendKind> {
    match std::env::var("KLIGHTS_NODE_LOCAL_BACKEND") {
        Ok(value) => BackendKind::parse(&value),
        Err(std::env::VarError::NotPresent) => Ok(BackendKind::Sqlite),
        Err(std::env::VarError::NotUnicode(_)) => Err(anyhow::anyhow!(
            "KLIGHTS_NODE_LOCAL_BACKEND must be valid Unicode"
        )),
    }
}

fn parse_bool_env(var: &str, default: bool) -> anyhow::Result<bool> {
    match std::env::var(var) {
        Ok(v) => match v.as_str() {
            "true" | "1" => Ok(true),
            "false" | "0" => Ok(false),
            other => Err(anyhow::anyhow!(
                "{} must be true/false/1/0, got '{}'",
                var,
                other
            )),
        },
        Err(_) => Ok(default),
    }
}

fn parse_u16_env(var: &str, default: u16) -> anyhow::Result<u16> {
    match std::env::var(var) {
        Ok(v) => v
            .parse::<u16>()
            .map_err(|e| anyhow::anyhow!("{} must be a u16: {}", var, e)),
        Err(_) => Ok(default),
    }
}

fn parse_u64_env_or_default(var: &str, default: u64) -> u64 {
    std::env::var(var)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

fn parse_positive_u64_env_or_default(var: &str, default: u64) -> u64 {
    std::env::var(var)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn parse_nonnegative_i64_env_or_default(var: &str, default: i64) -> i64 {
    std::env::var(var)
        .ok()
        .and_then(|value| value.trim().parse::<i64>().ok())
        .filter(|value| *value >= 0)
        .unwrap_or(default)
}

fn parse_positive_i64_env_or_default(var: &str, default: i64) -> i64 {
    std::env::var(var)
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn parse_optional_trimmed_env(var: &str) -> Option<String> {
    std::env::var(var)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn parse_optional_ipv4_env(var: &str) -> anyhow::Result<Option<String>> {
    let Some(value) = parse_optional_trimmed_env(var) else {
        return Ok(None);
    };
    value.parse::<std::net::Ipv4Addr>().map_err(|err| {
        anyhow::anyhow!("{} must be a valid IPv4 address '{}': {}", var, value, err)
    })?;
    Ok(Some(value))
}

fn parse_wireguard_device_env(default: &str) -> anyhow::Result<String> {
    let raw = std::env::var("KLIGHTS_WIREGUARD_DEVICE").unwrap_or_else(|_| default.to_string());
    klights_networking::wireguard::parse_wireguard_device_name(&raw)
}

fn parse_dataplane_encryption_env()
-> anyhow::Result<klights_networking::wireguard::DataplaneEncryption> {
    std::env::var("KLIGHTS_DATAPLANE_ENCRYPTION")
        .ok()
        .map(|value| klights_networking::wireguard::DataplaneEncryption::parse(Some(&value)))
        .transpose()
        .map(|mode| mode.unwrap_or_default())
}

#[cfg(test)]
pub fn resolve_local_pod_subnet(node_subnet: Option<String>, fallback_pod_subnet: &str) -> String {
    node_subnet.unwrap_or_else(|| fallback_pod_subnet.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datastore::backend_kind::BackendKind;
    use std::sync::Mutex;

    // Global lock to serialize env var tests (env vars are process-global)
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    // Helper to clear all KLIGHTS_ env vars before each test
    fn clear_klights_env() {
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::remove_var("KLIGHTS_BRIDGE_NAME") };
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::remove_var("KLIGHTS_POD_SUBNET") };
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::remove_var("KLIGHTS_CLUSTER_CIDR") };
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::remove_var("KLIGHTS_SERVICE_CIDR") };
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::remove_var("KLIGHTS_TLS_PORT") };
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::remove_var("KLIGHTS_LOG_FILE") };
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::remove_var("KLIGHTS_CONTAINERD_NAMESPACE") };
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::remove_var("KLIGHTS_CONTAINERD_SOCKET") };
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::remove_var("KLIGHTS_NODE_NAME") };
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::remove_var("KLIGHTS_NODE_IP") };
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::remove_var("KLIGHTS_ANONYMOUS_AUTH") };
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::remove_var("KLIGHTS_DATAPLANE_ENCRYPTION") };
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::remove_var("KLIGHTS_EXTERNAL_ENDPOINT") };
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::remove_var("KLIGHTS_WIREGUARD_DEVICE") };
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::remove_var("KLIGHTS_WIREGUARD_PORT") };
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::remove_var("KLIGHTS_WORKER_DATAPLANE_NO_INGRESS") };
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::remove_var("KLIGHTS_BACKEND") };
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::remove_var("KLIGHTS_DATASTORE_BACKEND") };
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::remove_var("KLIGHTS_NODE_LOCAL_BACKEND") };
        unsafe { std::env::remove_var("KLIGHTS_API_SLOW_LOG_MS") };
        unsafe { std::env::remove_var("KLIGHTS_NODE_NOT_READY_POD_EVICTION_GRACE_SECONDS") };
        unsafe { std::env::remove_var("KLIGHTS_MAX_WATCH_EVENTS") };
        unsafe { std::env::remove_var("KLIGHTS_GC_INTERVAL_SECS") };
    }

    #[test]
    fn test_config_default_values() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_klights_env();

        let config = KlightsConfig::from_env().expect("env config valid in test");

        assert_eq!(config.bridge_name, "klights"); // Defaults to containerd_namespace
        assert_eq!(config.pod_subnet, "10.43.0.0/17");
        assert_eq!(
            config.cluster_cidr, config.pod_subnet,
            "default cluster CIDR must contain the single-node pod subnet so nft masquerade does not classify pod-to-pod traffic as external"
        );
        assert_eq!(config.service_cidr, "10.43.128.0/17");
        assert_eq!(
            config.dataplane_encryption,
            klights_networking::wireguard::DataplaneEncryption::Enabled
        );
        assert_eq!(
            config.wireguard_device,
            klights_networking::wireguard::DEFAULT_WIREGUARD_DEVICE
        );
        assert_eq!(
            config.wireguard_port, 7_679,
            "WireGuard UDP default must be the shared multinode dataplane port"
        );
        assert_eq!(
            config.wireguard_port,
            klights_networking::wireguard::DEFAULT_WIREGUARD_PORT
        );
        assert!(
            config.external_endpoint.is_none(),
            "external endpoint should be operator-provided when external ingress matters"
        );
        assert!(
            !config.worker_dataplane_no_ingress,
            "workers require inbound WireGuard UDP by default"
        );
        assert_eq!(config.tls_port, 7679);
        assert_eq!(config.containerd_namespace, "klights");
        assert_eq!(config.node_ip, None);
        assert!(
            config.anonymous_auth,
            "anonymous-auth must default to kube-apiserver-compatible enabled"
        );
        assert_eq!(config.datastore_backend, BackendKind::Sqlite);
        assert_eq!(config.node_local_backend, BackendKind::Sqlite);
        assert_eq!(
            config.api_slow_log_threshold,
            std::time::Duration::from_millis(DEFAULT_API_SLOW_LOG_MS)
        );
        assert_eq!(
            config.node_not_ready_pod_eviction_grace,
            std::time::Duration::ZERO
        );
        assert_eq!(config.max_watch_events, DEFAULT_MAX_WATCH_EVENTS);
        let log_path = config.log_file_path();
        assert!(
            log_path.ends_with("/logs/klights.log"),
            "default log path must live under {{data_root}}/logs/, got: {log_path}"
        );
    }

    #[test]
    fn ambient_runtime_inputs_preserve_defaults_invalid_fallbacks_and_valid_overrides() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_klights_env();
        unsafe { std::env::set_var("KLIGHTS_API_SLOW_LOG_MS", "0") };
        unsafe { std::env::set_var("KLIGHTS_NODE_NOT_READY_POD_EVICTION_GRACE_SECONDS", "-1") };
        unsafe { std::env::set_var("KLIGHTS_MAX_WATCH_EVENTS", "invalid") };
        let fallback = KlightsConfig::from_env().expect("invalid optional inputs use defaults");
        assert_eq!(
            fallback.api_slow_log_threshold,
            std::time::Duration::from_millis(DEFAULT_API_SLOW_LOG_MS)
        );
        assert_eq!(
            fallback.node_not_ready_pod_eviction_grace,
            std::time::Duration::ZERO
        );
        assert_eq!(fallback.max_watch_events, DEFAULT_MAX_WATCH_EVENTS);

        unsafe { std::env::set_var("KLIGHTS_API_SLOW_LOG_MS", "700") };
        unsafe { std::env::set_var("KLIGHTS_NODE_NOT_READY_POD_EVICTION_GRACE_SECONDS", "45") };
        unsafe { std::env::set_var("KLIGHTS_MAX_WATCH_EVENTS", "12345") };
        let captured = KlightsConfig::from_env().expect("valid optional inputs");
        assert_eq!(
            captured.api_slow_log_threshold,
            std::time::Duration::from_millis(700)
        );
        assert_eq!(
            captured.node_not_ready_pod_eviction_grace,
            std::time::Duration::from_secs(45)
        );
        assert_eq!(captured.max_watch_events, 12_345);

        unsafe { std::env::set_var("KLIGHTS_API_SLOW_LOG_MS", "900") };
        assert_eq!(
            captured.api_slow_log_threshold,
            std::time::Duration::from_millis(700)
        );
        clear_klights_env();
    }

    #[test]
    fn datastore_backend_env_defaults_to_sqlite() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_klights_env();

        let config = KlightsConfig::from_env().expect("env config valid in test");

        assert_eq!(config.datastore_backend, BackendKind::Sqlite);
        assert!(config.cluster_db_path.ends_with("db/sqlite/cluster.db"));
    }

    #[test]
    fn node_local_backend_env_defaults_to_sqlite() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_klights_env();

        let config = KlightsConfig::from_env().expect("env config valid in test");

        assert_eq!(config.node_local_backend, BackendKind::Sqlite);
        assert!(config.node_db_path.ends_with("db/sqlite/node.db"));
    }

    #[test]
    fn datastore_and_node_local_backend_env_can_differ() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_klights_env();
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var("KLIGHTS_DATASTORE_BACKEND", "redb") };
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var("KLIGHTS_NODE_LOCAL_BACKEND", "sqlite") };

        let config = KlightsConfig::from_env().expect("env config valid in test");

        assert_eq!(config.datastore_backend, BackendKind::Redb);
        assert_eq!(config.node_local_backend, BackendKind::Sqlite);
        assert!(config.cluster_db_path.ends_with("db/redb/cluster.redb"));
        assert!(config.node_db_path.ends_with("db/sqlite/node.db"));
    }

    #[test]
    fn legacy_backend_env_does_not_select_node_local_backend() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_klights_env();
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var("KLIGHTS_BACKEND", "redb") };

        let config = KlightsConfig::from_env().expect("env config valid in test");

        assert_eq!(config.datastore_backend, BackendKind::Redb);
        assert_eq!(
            config.node_local_backend,
            BackendKind::Sqlite,
            "legacy KLIGHTS_BACKEND is a cluster-backend alias only"
        );
        assert!(config.cluster_db_path.ends_with("db/redb/cluster.redb"));
        assert!(config.node_db_path.ends_with("db/sqlite/node.db"));
    }

    #[test]
    fn test_config_custom_values() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_klights_env();
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var("KLIGHTS_BRIDGE_NAME", "klights-dev") };
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var("KLIGHTS_POD_SUBNET", "10.44.0.0/17") };
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var("KLIGHTS_SERVICE_CIDR", "10.44.128.0/17") };
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var("KLIGHTS_TLS_PORT", "8443") };
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var("KLIGHTS_LOG_FILE", "/tmp/custom.log") };
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var("KLIGHTS_CONTAINERD_NAMESPACE", "klights-dev") };

        let config = KlightsConfig::from_env().expect("env config valid in test");

        assert_eq!(config.bridge_name, "klights-dev");
        assert_eq!(config.pod_subnet, "10.44.0.0/17");
        assert_eq!(
            config.cluster_cidr, "10.44.0.0/17",
            "cluster CIDR must default to the resolved pod subnet when only KLIGHTS_POD_SUBNET is overridden"
        );
        assert_eq!(config.service_cidr, "10.44.128.0/17");
        assert_eq!(config.tls_port, 8443);
        assert_eq!(config.log_file_path(), "/tmp/custom.log");
        assert_eq!(config.containerd_namespace, "klights-dev");
    }

    #[test]
    fn log_file_true_uses_data_root_klights_log_case_insensitive() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_klights_env();
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var("KLIGHTS_CONTAINERD_NAMESPACE", "klights-dev") };
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var("KLIGHTS_LOG_FILE", "TrUe") };

        let config = KlightsConfig::from_env().expect("env config valid in test");
        let log_path = config.log_file_path();

        assert!(
            log_path.ends_with("/klights-dev/logs/klights.log"),
            "KLIGHTS_LOG_FILE=true should resolve to data_root/logs/klights.log, got: {log_path}"
        );
    }

    #[test]
    fn test_external_endpoint_and_worker_no_ingress_env() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_klights_env();
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var("KLIGHTS_EXTERNAL_ENDPOINT", " 192.0.2.55 ") };
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var("KLIGHTS_WORKER_DATAPLANE_NO_INGRESS", "true") };

        let config = KlightsConfig::from_env().expect("env config valid in test");

        assert_eq!(config.external_endpoint.as_deref(), Some("192.0.2.55"));
        assert!(config.worker_dataplane_no_ingress);
    }

    #[test]
    fn test_config_cluster_cidr_explicit_override() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_klights_env();
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var("KLIGHTS_POD_SUBNET", "10.44.0.0/17") };
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var("KLIGHTS_CLUSTER_CIDR", "10.200.0.0/16") };

        let config = KlightsConfig::from_env().expect("env config valid in test");

        assert_eq!(config.pod_subnet, "10.44.0.0/17");
        assert_eq!(config.cluster_cidr, "10.200.0.0/16");
    }

    #[test]
    fn test_config_bridge_name_defaults_to_namespace() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_klights_env();
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var("KLIGHTS_CONTAINERD_NAMESPACE", "my-namespace") };

        let config = KlightsConfig::from_env().expect("env config valid in test");

        assert_eq!(config.containerd_namespace, "my-namespace");
        assert_eq!(config.bridge_name, "my-namespace"); // Should default to namespace value
        let log_path = config.log_file_path();
        assert!(
            log_path.ends_with("/logs/my-namespace.log"),
            "default log path must live under {{data_root}}/logs/, got: {log_path}"
        );
    }

    #[test]
    fn test_config_containerd_socket_default() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_klights_env();

        let config = KlightsConfig::from_env().expect("env config valid in test");

        // Default is None (spawn own containerd)
        assert_eq!(config.containerd_socket, None);
    }

    #[test]
    fn anonymous_auth_env_can_disable_anonymous_requests() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_klights_env();
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var("KLIGHTS_ANONYMOUS_AUTH", "false") };

        let config = KlightsConfig::from_env().expect("env config valid in test");

        assert!(!config.anonymous_auth);
    }

    #[test]
    fn test_config_containerd_socket_override() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_klights_env();
        let sock_path = format!(
            "{}/containerd.sock",
            crate::paths::test_data_root_path("klights-test").display()
        );
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var("KLIGHTS_CONTAINERD_SOCKET", &sock_path) };

        let config = KlightsConfig::from_env().expect("env config valid in test");

        assert_eq!(
            config.containerd_socket.as_deref(),
            Some(sock_path.as_str())
        );
    }

    #[test]
    fn test_config_node_name_override_from_env() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_klights_env();
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var("KLIGHTS_NODE_NAME", "192.168.8.22") };

        let config = KlightsConfig::from_env().expect("env config valid in test");

        assert_eq!(config.node_name, "192.168.8.22");
    }

    #[test]
    fn test_config_node_ip_override_from_env() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_klights_env();
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var("KLIGHTS_NODE_IP", " 192.168.8.23 ") };

        let config = KlightsConfig::from_env().expect("env config valid in test");

        assert_eq!(config.node_ip.as_deref(), Some("192.168.8.23"));
    }

    #[test]
    fn test_from_env_rejects_invalid_node_ip() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_klights_env();
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var("KLIGHTS_NODE_IP", "not-an-ip") };

        let err = KlightsConfig::from_env().expect_err("must reject invalid node IP");

        assert!(
            format!("{:#}", err).contains("KLIGHTS_NODE_IP"),
            "error should name the bad var, got: {:#}",
            err
        );
    }

    #[test]
    fn test_bridge_name_truncated_to_15_chars() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_klights_env();
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var("KLIGHTS_BRIDGE_NAME", "klights-developer-1") }; // 19 chars

        let config = KlightsConfig::from_env().expect("env config valid in test");

        assert_eq!(config.bridge_name, "hts-developer-1"); // Last 15 chars
        assert_eq!(config.bridge_name.len(), 15);
    }

    #[test]
    fn test_bridge_name_under_15_unchanged() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_klights_env();
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var("KLIGHTS_BRIDGE_NAME", "klights") }; // 7 chars

        let config = KlightsConfig::from_env().expect("env config valid in test");

        assert_eq!(config.bridge_name, "klights");
        assert_eq!(config.bridge_name.len(), 7);
    }

    #[test]
    fn test_bridge_name_exactly_15_unchanged() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_klights_env();
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var("KLIGHTS_BRIDGE_NAME", "tes-developer-1") }; // Exactly 15 chars

        let config = KlightsConfig::from_env().expect("env config valid in test");

        assert_eq!(config.bridge_name, "tes-developer-1");
        assert_eq!(config.bridge_name.len(), 15);
    }

    #[test]
    fn test_bridge_name_truncation_preserves_uniqueness() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_klights_env();

        // Test developer-1
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var("KLIGHTS_BRIDGE_NAME", "klights-developer-1") };
        let config1 = KlightsConfig::from_env().expect("env config valid in test");

        // Test developer-2
        clear_klights_env();
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var("KLIGHTS_BRIDGE_NAME", "klights-developer-2") };
        let config2 = KlightsConfig::from_env().expect("env config valid in test");

        // Must be different (no collision)
        assert_eq!(config1.bridge_name, "hts-developer-1");
        assert_eq!(config2.bridge_name, "hts-developer-2");
        assert_ne!(config1.bridge_name, config2.bridge_name);
    }

    #[test]
    fn test_resolve_local_pod_subnet_prefers_node_subnet() {
        let resolved = resolve_local_pod_subnet(Some("10.244.7.0/24".to_string()), "10.244.0.0/16");
        assert_eq!(resolved, "10.244.7.0/24");
    }

    #[test]
    fn test_resolve_local_pod_subnet_falls_back_to_config_subnet() {
        let resolved = resolve_local_pod_subnet(None, "10.244.0.0/16");
        assert_eq!(resolved, "10.244.0.0/16");
    }

    #[test]
    fn test_rejects_invalid_pod_subnet() {
        let err = parse_pod_subnet("not-a-cidr").expect_err("must reject invalid CIDR");
        assert!(
            format!("{:#}", err).contains("KLIGHTS_POD_SUBNET"),
            "error should name the bad var, got: {:#}",
            err
        );
    }
}
