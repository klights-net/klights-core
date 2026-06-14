use anyhow::{Context, Result, anyhow};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::Duration;

const CA_BUNDLE_CLIENT_CACHE_CAPACITY: usize = 32;

type CaFingerprint = [u8; 32];

/// Shared reqwest::Client for admission webhook calls.
/// Avoids rebuilding the OpenSSL TLS context on every webhook invocation.
pub(super) fn webhook_http_client() -> Result<reqwest::Client> {
    static CLIENT: OnceLock<Result<reqwest::Client, String>> = OnceLock::new();
    CLIENT
        .get_or_init(|| build_default_webhook_http_client().map_err(|e| e.to_string()))
        .as_ref()
        .cloned()
        .map_err(|e| anyhow!("Failed to build webhook HTTP client: {}", e))
}

fn build_default_webhook_http_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(10))
        .build()
        .context("Failed to build webhook HTTP client")
}

pub(super) fn webhook_http_client_for(
    client_config: &Value,
    dns_override: Option<(&str, SocketAddr)>,
) -> Result<reqwest::Client> {
    if dns_override.is_some() {
        return build_webhook_http_client(client_config, dns_override);
    }

    if let Some(ca_bundle) = client_config.get("caBundle").and_then(|v| v.as_str())
        && !ca_bundle.is_empty()
    {
        static CA_BUNDLE_CLIENTS: OnceLock<Mutex<CaBundleClientCache>> = OnceLock::new();
        let cache = CA_BUNDLE_CLIENTS
            .get_or_init(|| Mutex::new(CaBundleClientCache::new(CA_BUNDLE_CLIENT_CACHE_CAPACITY)));
        return lock_ca_bundle_cache(cache)?.client_for(client_config);
    }
    webhook_http_client()
}

pub(super) struct CaBundleClientCache {
    capacity: usize,
    clients: HashMap<CaFingerprint, Arc<reqwest::Client>>,
    recency: VecDeque<CaFingerprint>,
}

impl CaBundleClientCache {
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            clients: HashMap::new(),
            recency: VecDeque::new(),
        }
    }

    #[cfg(test)]
    pub(super) fn new_for_test(capacity: usize) -> Self {
        Self::new(capacity)
    }

    pub(super) fn client_for(&mut self, client_config: &Value) -> Result<reqwest::Client> {
        let fingerprint = ca_bundle_fingerprint(client_config)?;
        if let Some(client) = self.clients.get(&fingerprint).cloned() {
            self.touch(fingerprint);
            return Ok(client.as_ref().clone());
        }

        let client = Arc::new(build_webhook_http_client(client_config, None)?);
        self.insert(fingerprint, Arc::clone(&client));
        Ok(client.as_ref().clone())
    }

    fn insert(&mut self, fingerprint: CaFingerprint, client: Arc<reqwest::Client>) {
        if self.clients.insert(fingerprint, client).is_some() {
            self.touch(fingerprint);
            return;
        }

        self.recency.push_back(fingerprint);
        while self.clients.len() > self.capacity {
            let Some(oldest) = self.recency.pop_front() else {
                break;
            };
            if oldest != fingerprint && self.clients.remove(&oldest).is_some() {
                break;
            }
        }
    }

    fn touch(&mut self, fingerprint: CaFingerprint) {
        self.recency.retain(|existing| *existing != fingerprint);
        self.recency.push_back(fingerprint);
    }

    #[cfg(test)]
    pub(super) fn len_for_test(&self) -> usize {
        self.clients.len()
    }

    #[cfg(test)]
    pub(super) fn contains_for_test(&self, fingerprint: &CaFingerprint) -> bool {
        self.clients.contains_key(fingerprint)
    }
}

pub(super) fn ca_bundle_fingerprint(client_config: &Value) -> Result<CaFingerprint> {
    let ca_bundle = client_config
        .get("caBundle")
        .and_then(|v| v.as_str())
        .filter(|bundle| !bundle.is_empty())
        .ok_or_else(|| anyhow!("clientConfig.caBundle is required for CA bundle client cache"))?;

    use base64::Engine;
    let ca_bytes = base64::engine::general_purpose::STANDARD
        .decode(ca_bundle)
        .context("Invalid base64 in clientConfig.caBundle")?;
    let digest = Sha256::digest(ca_bytes.as_slice());
    let mut fingerprint = [0u8; 32];
    fingerprint.copy_from_slice(digest.as_ref());
    Ok(fingerprint)
}

fn lock_ca_bundle_cache(
    cache: &Mutex<CaBundleClientCache>,
) -> Result<MutexGuard<'_, CaBundleClientCache>> {
    cache
        .lock()
        .map_err(|_| anyhow!("caBundle client cache poisoned"))
}

#[cfg(test)]
pub(super) fn lock_ca_bundle_cache_for_test(
    cache: &Mutex<CaBundleClientCache>,
) -> Result<MutexGuard<'_, CaBundleClientCache>> {
    lock_ca_bundle_cache(cache)
}

pub(super) fn build_webhook_http_client(
    client_config: &Value,
    dns_override: Option<(&str, SocketAddr)>,
) -> Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(10));

    if let Some(ca_bundle) = client_config.get("caBundle").and_then(|v| v.as_str())
        && !ca_bundle.is_empty()
    {
        use base64::Engine;
        let ca_der = base64::engine::general_purpose::STANDARD
            .decode(ca_bundle)
            .context("Invalid base64 in clientConfig.caBundle")?;
        let cert = reqwest::Certificate::from_der(&ca_der)
            .or_else(|_| reqwest::Certificate::from_pem(&ca_der))
            .context("Invalid certificate in clientConfig.caBundle")?;
        builder = builder.add_root_certificate(cert);
    }

    if let Some((host, addr)) = dns_override {
        builder = builder.resolve(host, addr);
    }

    builder
        .build()
        .context("Failed to build webhook HTTP client")
}
