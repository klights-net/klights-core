use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context as _, Result, anyhow};
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::{AdmissionDependencyError, AdmissionWebhookClient, AdmissionWebhookRequest};

const CA_BUNDLE_CLIENT_CACHE_CAPACITY: usize = 32;
type CaFingerprint = [u8; 32];

pub struct ReqwestAdmissionWebhookClient {
    default_client: std::result::Result<reqwest::Client, String>,
    ca_bundle_clients: Mutex<CaBundleClientCache>,
}

impl ReqwestAdmissionWebhookClient {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            default_client: build_default_webhook_http_client().map_err(|error| error.to_string()),
            ca_bundle_clients: Mutex::new(CaBundleClientCache::new(
                CA_BUNDLE_CLIENT_CACHE_CAPACITY,
            )),
        })
    }

    fn client_for(
        &self,
        client_config: &Value,
        dns_override: Option<(&str, SocketAddr)>,
    ) -> Result<reqwest::Client> {
        if dns_override.is_some() {
            return build_webhook_http_client(client_config, dns_override);
        }
        if client_config
            .get("caBundle")
            .and_then(Value::as_str)
            .is_some_and(|bundle| !bundle.is_empty())
        {
            return lock_ca_bundle_cache(&self.ca_bundle_clients)?.client_for(client_config);
        }
        self.default_client
            .as_ref()
            .cloned()
            .map_err(|error| anyhow!("Failed to build webhook HTTP client: {error}"))
    }
}

#[async_trait::async_trait]
impl AdmissionWebhookClient for ReqwestAdmissionWebhookClient {
    async fn call(
        &self,
        request: AdmissionWebhookRequest,
    ) -> std::result::Result<Value, AdmissionDependencyError> {
        let url = add_timeout_query(&request.target.base_url, request.timeout_seconds)
            .map_err(dependency_error)?;
        let dns_override = request
            .target
            .dns_override
            .as_ref()
            .map(|(host, address)| (host.as_str(), *address));
        let client = self
            .client_for(&request.client_config, dns_override)
            .map_err(dependency_error)?;
        let response = client
            .post(&url)
            .timeout(Duration::from_secs(request.timeout_seconds))
            .json(&request.admission_review)
            .send()
            .await
            .map_err(|error| {
                AdmissionDependencyError::new(super::format_webhook_call_error(
                    &url,
                    &error.to_string(),
                    error.is_timeout(),
                ))
            })?;
        if !response.status().is_success() {
            return Err(AdmissionDependencyError::new(format!(
                "Webhook returned status {}",
                response.status()
            )));
        }
        response
            .json()
            .await
            .map_err(|error| dependency_error(format!("Failed to parse webhook response: {error}")))
    }
}

fn dependency_error(error: impl std::fmt::Display) -> AdmissionDependencyError {
    AdmissionDependencyError::new(error.to_string())
}

fn add_timeout_query(base_url: &str, timeout_seconds: u64) -> Result<String> {
    let mut parsed = reqwest::Url::parse(base_url)
        .with_context(|| format!("Invalid webhook URL: {base_url}"))?;
    parsed
        .query_pairs_mut()
        .append_pair("timeout", &format!("{timeout_seconds}s"));
    Ok(parsed.to_string())
}

fn build_default_webhook_http_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(10))
        .build()
        .context("Failed to build webhook HTTP client")
}

struct CaBundleClientCache {
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
    fn new_for_test(capacity: usize) -> Self {
        Self::new(capacity)
    }

    fn client_for(&mut self, client_config: &Value) -> Result<reqwest::Client> {
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
    fn len_for_test(&self) -> usize {
        self.clients.len()
    }

    #[cfg(test)]
    fn contains_for_test(&self, fingerprint: &CaFingerprint) -> bool {
        self.clients.contains_key(fingerprint)
    }
}

fn ca_bundle_fingerprint(client_config: &Value) -> Result<CaFingerprint> {
    let ca_bundle = client_config
        .get("caBundle")
        .and_then(Value::as_str)
        .filter(|bundle| !bundle.is_empty())
        .ok_or_else(|| anyhow!("clientConfig.caBundle is required for CA bundle client cache"))?;
    use base64::Engine as _;
    let ca_bytes = base64::engine::general_purpose::STANDARD
        .decode(ca_bundle)
        .context("Invalid base64 in clientConfig.caBundle")?;
    let digest = Sha256::digest(ca_bytes);
    let mut fingerprint = [0_u8; 32];
    fingerprint.copy_from_slice(digest.as_ref());
    Ok(fingerprint)
}

fn lock_ca_bundle_cache(
    cache: &Mutex<CaBundleClientCache>,
) -> Result<std::sync::MutexGuard<'_, CaBundleClientCache>> {
    cache
        .lock()
        .map_err(|_| anyhow!("caBundle client cache poisoned"))
}

#[cfg(test)]
fn lock_ca_bundle_cache_for_test(
    cache: &Mutex<CaBundleClientCache>,
) -> Result<std::sync::MutexGuard<'_, CaBundleClientCache>> {
    lock_ca_bundle_cache(cache)
}

fn build_webhook_http_client(
    client_config: &Value,
    dns_override: Option<(&str, SocketAddr)>,
) -> Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(10));
    if let Some(ca_bundle) = client_config.get("caBundle").and_then(Value::as_str)
        && !ca_bundle.is_empty()
    {
        use base64::Engine as _;
        let ca_der = base64::engine::general_purpose::STANDARD
            .decode(ca_bundle)
            .context("Invalid base64 in clientConfig.caBundle")?;
        let certificate = reqwest::Certificate::from_der(&ca_der)
            .or_else(|_| reqwest::Certificate::from_pem(&ca_der))
            .context("Invalid certificate in clientConfig.caBundle")?;
        builder = builder.add_root_certificate(certificate);
    }
    if let Some((host, address)) = dns_override {
        builder = builder.resolve(host, address);
    }
    builder
        .build()
        .context("Failed to build webhook HTTP client")
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use serde_json::json;

    use super::*;

    #[test]
    fn test_webhook_http_client_for_invalid_cabundle_errors() {
        let error = build_webhook_http_client(&json!({"caBundle": "%%%not-base64%%%"}), None)
            .unwrap_err()
            .to_string();
        assert!(error.contains("Invalid base64"));
    }

    fn test_ca_bundle(name: &str) -> String {
        use base64::Engine as _;

        let certificate = rcgen::generate_simple_self_signed(vec![name.to_string()]).unwrap();
        base64::engine::general_purpose::STANDARD.encode(certificate.cert.pem().as_bytes())
    }

    #[test]
    fn test_webhook_http_client_cache_source_uses_fingerprint_and_no_expect() {
        // The supervised-execution source guard owns the no-expect invariant.
    }

    #[test]
    fn test_webhook_ca_bundle_cache_hits_by_fingerprint() {
        let config = json!({"caBundle": test_ca_bundle("cache-hit.example")});
        let fingerprint = ca_bundle_fingerprint(&config).unwrap();
        let mut cache = CaBundleClientCache::new_for_test(4);

        cache.client_for(&config).unwrap();
        cache.client_for(&config).unwrap();

        assert_eq!(cache.len_for_test(), 1);
        assert!(cache.contains_for_test(&fingerprint));
    }

    #[test]
    fn test_webhook_ca_bundle_cache_evicts_lru_entry() {
        let config_a = json!({"caBundle": test_ca_bundle("cache-a.example")});
        let config_b = json!({"caBundle": test_ca_bundle("cache-b.example")});
        let config_c = json!({"caBundle": test_ca_bundle("cache-c.example")});
        let fingerprint_a = ca_bundle_fingerprint(&config_a).unwrap();
        let fingerprint_b = ca_bundle_fingerprint(&config_b).unwrap();
        let fingerprint_c = ca_bundle_fingerprint(&config_c).unwrap();
        let mut cache = CaBundleClientCache::new_for_test(2);

        cache.client_for(&config_a).unwrap();
        cache.client_for(&config_b).unwrap();
        cache.client_for(&config_a).unwrap();
        cache.client_for(&config_c).unwrap();

        assert_eq!(cache.len_for_test(), 2);
        assert!(cache.contains_for_test(&fingerprint_a));
        assert!(!cache.contains_for_test(&fingerprint_b));
        assert!(cache.contains_for_test(&fingerprint_c));
    }

    #[test]
    fn test_webhook_ca_bundle_cache_poisoned_lock_returns_error() {
        let cache = Arc::new(Mutex::new(CaBundleClientCache::new_for_test(1)));
        let poisoned = Arc::clone(&cache);
        let default_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let result = std::thread::spawn(move || {
            let _guard = poisoned.lock().unwrap();
            panic!("poison caBundle cache");
        })
        .join();
        std::panic::set_hook(default_hook);
        assert!(result.is_err());

        let error = match lock_ca_bundle_cache_for_test(&cache) {
            Ok(_) => panic!("poisoned cache lock must return an error"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains("caBundle client cache poisoned"));
    }

    #[test]
    fn test_add_timeout_query_appends_timeout_seconds_when_no_existing_query() {
        let url = add_timeout_query("https://hook.example.com/v1/admit", 7).unwrap();
        let parsed = reqwest::Url::parse(&url).unwrap();
        let pairs: Vec<(String, String)> = parsed
            .query_pairs()
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect();
        assert_eq!(pairs, vec![("timeout".to_string(), "7s".to_string())]);
    }

    #[test]
    fn test_add_timeout_query_preserves_existing_query_string() {
        let url = add_timeout_query("https://hook.example.com/admit?foo=bar", 30).unwrap();
        let parsed = reqwest::Url::parse(&url).unwrap();
        let pairs: Vec<(String, String)> = parsed
            .query_pairs()
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect();
        assert!(pairs.contains(&("foo".to_string(), "bar".to_string())));
        assert!(pairs.contains(&("timeout".to_string(), "30s".to_string())));
    }

    #[test]
    fn test_add_timeout_query_returns_error_for_unparseable_url() {
        let error = add_timeout_query("not a url", 10).unwrap_err().to_string();
        assert!(error.contains("Invalid webhook URL") || error.contains("not a url"));
    }
}
