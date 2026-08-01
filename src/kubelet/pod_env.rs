use crate::kubelet::pod_field_ref::{resolve_field_ref, resolve_resource_field_ref_with_capacity};
use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;

use klights_leader_api::{
    LeaderResourceQuery, ResourceListRequest, ResourceQueryConsistency, config_map_get_request,
    secret_get_request,
};

#[async_trait]
pub trait EnvSourceReader: Send + Sync {
    async fn secret(
        &self,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<klights_cluster_core::Resource>>;

    async fn config_map(
        &self,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<klights_cluster_core::Resource>>;

    async fn services(
        &self,
        namespace: &str,
    ) -> anyhow::Result<Vec<klights_cluster_core::Resource>>;
}

pub struct LeaderApiEnvSourceReader {
    cluster_api: Arc<dyn LeaderResourceQuery>,
}

impl LeaderApiEnvSourceReader {
    pub fn new(cluster_api: Arc<dyn LeaderResourceQuery>) -> Self {
        Self { cluster_api }
    }
}

#[async_trait]
impl EnvSourceReader for LeaderApiEnvSourceReader {
    async fn secret(
        &self,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<klights_cluster_core::Resource>> {
        // Fresh leader read (not the worker cache): env injection happens at
        // container start, and a Secret created moments earlier may not yet have
        // propagated to a primed-but-lagging worker cache. A cached miss would
        // spuriously fail the container with a not-found; the fresh read confirms
        // against the leader. Mirrors the volume-source reader. See B4.
        self.cluster_api
            .get_resource(secret_get_request(
                namespace,
                name,
                ResourceQueryConsistency::LeaderFresh,
            )?)
            .await
            .map_err(anyhow::Error::new)
    }

    async fn config_map(
        &self,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<klights_cluster_core::Resource>> {
        // Fresh leader read (see `secret`): a just-created ConfigMap may not yet
        // be in a primed-but-lagging worker cache, and a cached miss would
        // spuriously fail the container.
        self.cluster_api
            .get_resource(config_map_get_request(
                namespace,
                name,
                ResourceQueryConsistency::LeaderFresh,
            )?)
            .await
            .map_err(anyhow::Error::new)
    }

    async fn services(
        &self,
        namespace: &str,
    ) -> anyhow::Result<Vec<klights_cluster_core::Resource>> {
        Ok(self
            .cluster_api
            .list_resources(ResourceListRequest::try_new(
                "v1",
                "Service",
                Some(namespace.to_string()),
                None,
                None,
                None,
                None,
                ResourceQueryConsistency::Cached,
            )?)
            .await?
            .into_items())
    }
}

#[cfg(test)]
struct DatastoreEnvSourceReader<'a> {
    db: &'a dyn crate::datastore::DatastoreBackend,
}

#[cfg(test)]
#[async_trait]
impl EnvSourceReader for DatastoreEnvSourceReader<'_> {
    async fn secret(
        &self,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<klights_cluster_core::Resource>> {
        self.db
            .get_resource("v1", "Secret", Some(namespace), name)
            .await
    }

    async fn config_map(
        &self,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<klights_cluster_core::Resource>> {
        self.db
            .get_resource("v1", "ConfigMap", Some(namespace), name)
            .await
    }

    async fn services(
        &self,
        namespace: &str,
    ) -> anyhow::Result<Vec<klights_cluster_core::Resource>> {
        Ok(self
            .db
            .list_resources(
                "v1",
                "Service",
                Some(namespace),
                crate::datastore::ResourceListQuery::all(),
            )
            .await?
            .items)
    }
}

/// Collect env vars that have a literal `value` field (not `valueFrom`).
/// Needed for subPathExpr expansion, which must see all env vars.
pub fn collect_literal_env_vars(
    container_spec: &Value,
) -> std::collections::HashMap<String, String> {
    let mut map = std::collections::HashMap::new();
    let Some(env_array) = container_spec.get("env").and_then(|e| e.as_array()) else {
        return map;
    };
    for entry in env_array {
        if let (Some(name), Some(val)) = (
            entry.get("name").and_then(|n| n.as_str()),
            entry.get("value").and_then(|v| v.as_str()),
        ) {
            map.insert(name.to_string(), val.to_string());
        }
    }
    map
}

/// Collect env vars resolvable from valueFrom.fieldRef/resourceFieldRef for subPathExpr expansion.
/// This uses pod/container context available at create/restart time.
pub fn collect_value_from_env_vars_for_subpath_with_capacity(
    container_spec: &Value,
    pod_data: &Value,
    node_capacity: klights_kubelet::node_capacity::NodeCapacity,
) -> std::collections::HashMap<String, String> {
    let mut map = std::collections::HashMap::new();
    let Some(env_array) = container_spec.get("env").and_then(|e| e.as_array()) else {
        return map;
    };

    for entry in env_array {
        let Some(name) = entry.get("name").and_then(|n| n.as_str()) else {
            continue;
        };
        let Some(value_from) = entry.get("valueFrom") else {
            continue;
        };

        if let Some(field_ref) = value_from.get("fieldRef") {
            if let Some(field_path) = field_ref.get("fieldPath").and_then(|f| f.as_str()) {
                map.insert(name.to_string(), resolve_field_ref(field_path, pod_data));
            }
            continue;
        }

        if let Some(resource_field_ref) = value_from.get("resourceFieldRef")
            && let Some(resource) = resource_field_ref.get("resource").and_then(|r| r.as_str())
        {
            map.insert(
                name.to_string(),
                resolve_resource_field_ref_with_capacity(resource, container_spec, node_capacity),
            );
        }
    }

    map
}

/// Resolve env vars that use `valueFrom.secretKeyRef` or `valueFrom.configMapKeyRef`.
/// Returns a map of env var name -> resolved value.
#[cfg(test)]
pub async fn resolve_env_value_from(
    container_spec: &Value,
    namespace: &str,
    db: &dyn crate::datastore::DatastoreBackend,
) -> std::collections::HashMap<String, String> {
    let source = DatastoreEnvSourceReader { db };
    resolve_env_value_from_source(container_spec, namespace, &source).await
}

/// Resolve env vars that use `valueFrom.secretKeyRef` or `valueFrom.configMapKeyRef`.
/// Returns a map of env var name -> resolved value.
pub async fn resolve_env_value_from_source(
    container_spec: &Value,
    namespace: &str,
    source: &dyn EnvSourceReader,
) -> std::collections::HashMap<String, String> {
    let mut resolved = std::collections::HashMap::new();

    let env_array = match container_spec.get("env").and_then(|e| e.as_array()) {
        Some(arr) => arr,
        None => return resolved,
    };

    for env in env_array {
        let name = match env.get("name").and_then(|n| n.as_str()) {
            Some(n) => n,
            None => continue,
        };

        let value_from = match env.get("valueFrom") {
            Some(vf) => vf,
            None => continue,
        };

        // Handle secretKeyRef
        if let Some(secret_ref) = value_from.get("secretKeyRef") {
            let secret_name = secret_ref
                .get("name")
                .and_then(|n| n.as_str())
                .unwrap_or("");
            let secret_key = secret_ref.get("key").and_then(|k| k.as_str()).unwrap_or("");
            let optional = secret_ref
                .get("optional")
                .and_then(|o| o.as_bool())
                .unwrap_or(false);

            match source.secret(namespace, secret_name).await {
                Ok(Some(resource)) => {
                    if let Some(encoded_value) = resource
                        .data
                        .pointer(&format!("/data/{}", secret_key))
                        .and_then(|v| v.as_str())
                    {
                        // Secret data values are base64-encoded
                        use base64::Engine;
                        if let Ok(decoded) =
                            base64::engine::general_purpose::STANDARD.decode(encoded_value)
                            && let Ok(value_str) = String::from_utf8(decoded)
                        {
                            resolved.insert(name.to_string(), value_str);
                        }
                    } else if !optional {
                        tracing::warn!(
                            "Secret {}/{} key {} not found for env var {}",
                            namespace,
                            secret_name,
                            secret_key,
                            name
                        );
                    }
                }
                Ok(None) => {
                    if !optional {
                        tracing::warn!(
                            "Secret {}/{} not found for env var {}",
                            namespace,
                            secret_name,
                            name
                        );
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to get Secret {}/{} for env var {}: {}",
                        namespace,
                        secret_name,
                        name,
                        e
                    );
                }
            }
        }

        // Handle configMapKeyRef
        if let Some(cm_ref) = value_from.get("configMapKeyRef") {
            let cm_name = cm_ref.get("name").and_then(|n| n.as_str()).unwrap_or("");
            let cm_key = cm_ref.get("key").and_then(|k| k.as_str()).unwrap_or("");
            let optional = cm_ref
                .get("optional")
                .and_then(|o| o.as_bool())
                .unwrap_or(false);

            match source.config_map(namespace, cm_name).await {
                Ok(Some(resource)) => {
                    if let Some(value) = resource
                        .data
                        .pointer(&format!("/data/{}", cm_key))
                        .and_then(|v| v.as_str())
                    {
                        resolved.insert(name.to_string(), value.to_string());
                    } else if !optional {
                        tracing::warn!(
                            "ConfigMap {}/{} key {} not found for env var {}",
                            namespace,
                            cm_name,
                            cm_key,
                            name
                        );
                    }
                }
                Ok(None) => {
                    if !optional {
                        tracing::warn!(
                            "ConfigMap {}/{} not found for env var {}",
                            namespace,
                            cm_name,
                            name
                        );
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to get ConfigMap {}/{} for env var {}: {}",
                        namespace,
                        cm_name,
                        name,
                        e
                    );
                }
            }
        }
    }

    resolved
}

/// Resolve envFrom entries (bulk injection of all Secret/ConfigMap keys as env vars)
/// K8s envFrom allows injecting ALL keys from a Secret or ConfigMap as environment variables.
/// With optional prefix, each key is prefixed (e.g., key "foo" with prefix "CFG_" becomes "CFG_foo").
/// envFrom vars are added BEFORE individual env vars (individual vars override envFrom).
#[cfg(test)]
pub async fn resolve_env_from(
    container_spec: &Value,
    namespace: &str,
    db: &dyn crate::datastore::DatastoreBackend,
) -> Vec<(String, String)> {
    let source = DatastoreEnvSourceReader { db };
    resolve_env_from_source(container_spec, namespace, &source).await
}

/// Resolve envFrom entries through the supplied source reader.
pub async fn resolve_env_from_source(
    container_spec: &Value,
    namespace: &str,
    source: &dyn EnvSourceReader,
) -> Vec<(String, String)> {
    let mut resolved = Vec::new();

    let env_from_array = match container_spec.get("envFrom").and_then(|e| e.as_array()) {
        Some(arr) => arr,
        None => return resolved,
    };

    for env_from_entry in env_from_array {
        let prefix = env_from_entry
            .get("prefix")
            .and_then(|p| p.as_str())
            .unwrap_or("");

        // Handle secretRef
        if let Some(secret_ref) = env_from_entry.get("secretRef") {
            let secret_name = secret_ref
                .get("name")
                .and_then(|n| n.as_str())
                .unwrap_or("");
            let optional = secret_ref
                .get("optional")
                .and_then(|o| o.as_bool())
                .unwrap_or(false);

            match source.secret(namespace, secret_name).await {
                Ok(Some(resource)) => {
                    if let Some(data_obj) = resource.data.get("data").and_then(|d| d.as_object()) {
                        for (key, encoded_value) in data_obj {
                            if let Some(encoded_str) = encoded_value.as_str() {
                                // Secret data values are base64-encoded
                                use base64::Engine;
                                if let Ok(decoded) =
                                    base64::engine::general_purpose::STANDARD.decode(encoded_str)
                                    && let Ok(value_str) = String::from_utf8(decoded)
                                {
                                    let env_name = format!("{}{}", prefix, key);
                                    resolved.push((env_name, value_str));
                                }
                            }
                        }
                    }
                }
                Ok(None) => {
                    if !optional {
                        tracing::warn!(
                            "Secret {}/{} not found for envFrom",
                            namespace,
                            secret_name
                        );
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to get Secret {}/{} for envFrom: {}",
                        namespace,
                        secret_name,
                        e
                    );
                }
            }
        }

        // Handle configMapRef
        if let Some(cm_ref) = env_from_entry.get("configMapRef") {
            let cm_name = cm_ref.get("name").and_then(|n| n.as_str()).unwrap_or("");
            let optional = cm_ref
                .get("optional")
                .and_then(|o| o.as_bool())
                .unwrap_or(false);

            match source.config_map(namespace, cm_name).await {
                Ok(Some(resource)) => {
                    if let Some(data_obj) = resource.data.get("data").and_then(|d| d.as_object()) {
                        for (key, value) in data_obj {
                            if let Some(value_str) = value.as_str() {
                                let env_name = format!("{}{}", prefix, key);
                                resolved.push((env_name, value_str.to_string()));
                            }
                        }
                    }
                }
                Ok(None) => {
                    if !optional {
                        tracing::warn!("ConfigMap {}/{} not found for envFrom", namespace, cm_name);
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to get ConfigMap {}/{} for envFrom: {}",
                        namespace,
                        cm_name,
                        e
                    );
                }
            }
        }
    }

    resolved
}

pub fn build_subpath_env_with_capacity(
    container_spec: &Value,
    pod_data: &Value,
    resolved_env_from: &[(String, String)],
    resolved_env: &std::collections::HashMap<String, String>,
    node_capacity: klights_kubelet::node_capacity::NodeCapacity,
) -> std::collections::HashMap<String, String> {
    let mut subpath_env: std::collections::HashMap<String, String> =
        resolved_env_from.iter().cloned().collect();
    for (key, value) in collect_literal_env_vars(container_spec) {
        subpath_env.insert(key, value);
    }
    for (key, value) in resolved_env {
        subpath_env.insert(key.clone(), value.clone());
    }
    for (key, value) in collect_value_from_env_vars_for_subpath_with_capacity(
        container_spec,
        pod_data,
        node_capacity,
    ) {
        subpath_env.insert(key, value);
    }
    subpath_env
}

#[cfg(test)]
pub fn collect_value_from_env_vars_for_subpath(
    container_spec: &Value,
    pod_data: &Value,
) -> std::collections::HashMap<String, String> {
    collect_value_from_env_vars_for_subpath_with_capacity(
        container_spec,
        pod_data,
        klights_kubelet::node_capacity::NodeCapacity::default(),
    )
}

#[cfg(test)]
pub fn build_subpath_env(
    container_spec: &Value,
    pod_data: &Value,
    resolved_env_from: &[(String, String)],
    resolved_env: &std::collections::HashMap<String, String>,
) -> std::collections::HashMap<String, String> {
    build_subpath_env_with_capacity(
        container_spec,
        pod_data,
        resolved_env_from,
        resolved_env,
        klights_kubelet::node_capacity::NodeCapacity::default(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collect_literal_env_vars_returns_literal_values() {
        let container = serde_json::json!({
            "env": [
                {"name": "PLAIN", "value": "/absolute/path"},
                {"name": "FROM_SECRET", "valueFrom": {"secretKeyRef": {"name": "s", "key": "k"}}},
                {"name": "EMPTY", "value": ""},
                {"name": "NO_VALUE"}
            ]
        });
        let vars = collect_literal_env_vars(&container);
        assert_eq!(
            vars.get("PLAIN").map(|s| s.as_str()),
            Some("/absolute/path")
        );
        assert_eq!(vars.get("EMPTY").map(|s| s.as_str()), Some(""));
        assert!(
            !vars.contains_key("FROM_SECRET"),
            "valueFrom entries must not be included"
        );
        assert!(
            !vars.contains_key("NO_VALUE"),
            "entries without value field must not be included"
        );
    }

    #[test]
    fn test_collect_value_from_env_vars_for_subpath_resolves_field_ref_annotation() {
        let container = serde_json::json!({
            "env": [{
                "name": "ANNOTATION",
                "valueFrom": {"fieldRef": {"fieldPath": "metadata.annotations['mysubpath']"}}
            }]
        });
        let pod = serde_json::json!({
            "metadata": {
                "annotations": {
                    "mysubpath": "mypath"
                }
            }
        });

        let vars = collect_value_from_env_vars_for_subpath(&container, &pod);
        assert_eq!(
            vars.get("ANNOTATION").map(|v| v.as_str()),
            Some("mypath"),
            "fieldRef annotation must resolve for subPathExpr expansion"
        );
    }

    #[tokio::test]
    async fn test_resolve_env_value_from_secret_key_ref() {
        use base64::Engine;
        let db = crate::datastore::test_support::in_memory().await;

        // Create namespace

        // Create a Secret with base64-encoded data
        let cert_data = base64::engine::general_purpose::STANDARD
            .encode("-----BEGIN CERTIFICATE-----\nMIIB...\n-----END CERTIFICATE-----");
        let secret = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": "my-tls", "namespace": "default"},
            "data": {
                "tls.crt": cert_data,
                "tls.key": base64::engine::general_purpose::STANDARD.encode("private-key-data")
            }
        });
        db.create_resource("v1", "Secret", Some("default"), "my-tls", secret)
            .await
            .unwrap();

        // Container spec with secretKeyRef
        let container_spec = serde_json::json!({
            "image": "worker:latest",
            "env": [
                {"name": "CLIENT_CERT", "valueFrom": {"secretKeyRef": {"name": "my-tls", "key": "tls.crt"}}},
                {"name": "CLIENT_KEY", "valueFrom": {"secretKeyRef": {"name": "my-tls", "key": "tls.key"}}},
                {"name": "PLAIN_VAR", "value": "hello"}
            ]
        });

        let resolved = resolve_env_value_from(&container_spec, "default", &db).await;

        // secretKeyRef vars should be resolved
        assert_eq!(
            resolved.get("CLIENT_CERT").unwrap(),
            "-----BEGIN CERTIFICATE-----\nMIIB...\n-----END CERTIFICATE-----"
        );
        assert_eq!(resolved.get("CLIENT_KEY").unwrap(), "private-key-data");
        // Plain value vars should NOT appear in resolved map
        assert!(!resolved.contains_key("PLAIN_VAR"));
    }

    /// Regression for P0-E2E-20260424-12b: Secret key with dash character must resolve correctly.
    /// Conformance test creates secret with key "data-1" and expects env var "data-1=value-1".
    #[tokio::test]
    async fn test_resolve_env_value_from_secret_key_dash() {
        use base64::Engine;
        let db = crate::datastore::test_support::in_memory().await;

        let ns = serde_json::json!({"apiVersion":"v1","kind":"Namespace","metadata":{"name":"secrets-test"}});
        db.create_namespace("secrets-test", ns).await.unwrap();

        // Create secret with dash key (conformance test scenario)
        let secret = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": "secret-test-abc", "namespace": "secrets-test"},
            "data": {
                "data-1": base64::engine::general_purpose::STANDARD.encode("value-1"),
                "data.2": base64::engine::general_purpose::STANDARD.encode("value-2"),
            }
        });
        db.create_resource(
            "v1",
            "Secret",
            Some("secrets-test"),
            "secret-test-abc",
            secret,
        )
        .await
        .unwrap();

        let container_spec = serde_json::json!({
            "image": "busybox",
            "env": [
                {"name": "data-1", "valueFrom": {"secretKeyRef": {"name": "secret-test-abc", "key": "data-1"}}},
                {"name": "DATA_2", "valueFrom": {"secretKeyRef": {"name": "secret-test-abc", "key": "data.2"}}},
            ]
        });

        let resolved = resolve_env_value_from(&container_spec, "secrets-test", &db).await;
        assert_eq!(
            resolved.get("data-1").map(|s| s.as_str()),
            Some("value-1"),
            "dash-key secret must resolve to 'value-1'"
        );
        assert_eq!(
            resolved.get("DATA_2").map(|s| s.as_str()),
            Some("value-2"),
            "dot-key secret must resolve to 'value-2'"
        );
    }

    #[tokio::test]
    async fn test_resolve_env_value_from_configmap_key_ref() {
        let db = crate::datastore::test_support::in_memory().await;

        let cm = serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "app-config", "namespace": "default"},
            "data": {"log_level": "debug", "port": "8080"}
        });
        db.create_resource("v1", "ConfigMap", Some("default"), "app-config", cm)
            .await
            .unwrap();

        let container_spec = serde_json::json!({
            "image": "app:latest",
            "env": [
                {"name": "LOG_LEVEL", "valueFrom": {"configMapKeyRef": {"name": "app-config", "key": "log_level"}}}
            ]
        });

        let resolved = resolve_env_value_from(&container_spec, "default", &db).await;
        assert_eq!(resolved.get("LOG_LEVEL").unwrap(), "debug");
    }

    #[tokio::test]
    async fn test_resolve_env_value_from_missing_secret_optional() {
        let db = crate::datastore::test_support::in_memory().await;

        let container_spec = serde_json::json!({
            "image": "app:latest",
            "env": [
                {"name": "OPT_VAR", "valueFrom": {"secretKeyRef": {"name": "nonexistent", "key": "x", "optional": true}}}
            ]
        });

        let resolved = resolve_env_value_from(&container_spec, "default", &db).await;
        // Optional missing secret should not produce a value
        assert!(!resolved.contains_key("OPT_VAR"));
    }

    #[tokio::test]
    async fn test_resolve_env_value_from_missing_key_in_existing_secret() {
        use base64::Engine;
        let db = crate::datastore::test_support::in_memory().await;

        let secret = serde_json::json!({
            "apiVersion": "v1", "kind": "Secret",
            "metadata": {"name": "my-secret", "namespace": "default"},
            "data": {"existing-key": base64::engine::general_purpose::STANDARD.encode("value")}
        });
        db.create_resource("v1", "Secret", Some("default"), "my-secret", secret)
            .await
            .unwrap();

        let spec = serde_json::json!({
            "image": "app",
            "env": [{"name": "MISSING", "valueFrom": {"secretKeyRef": {"name": "my-secret", "key": "nonexistent-key"}}}]
        });

        let resolved = resolve_env_value_from(&spec, "default", &db).await;
        assert!(
            !resolved.contains_key("MISSING"),
            "Missing key should not produce a value"
        );
    }

    #[tokio::test]
    async fn test_resolve_env_value_from_invalid_base64_in_secret() {
        let db = crate::datastore::test_support::in_memory().await;

        // Secret with invalid base64 in data
        let secret = serde_json::json!({
            "apiVersion": "v1", "kind": "Secret",
            "metadata": {"name": "bad-secret", "namespace": "default"},
            "data": {"key": "!!!not-valid-base64!!!"}
        });
        db.create_resource("v1", "Secret", Some("default"), "bad-secret", secret)
            .await
            .unwrap();

        let spec = serde_json::json!({
            "image": "app",
            "env": [{"name": "BAD", "valueFrom": {"secretKeyRef": {"name": "bad-secret", "key": "key"}}}]
        });

        let resolved = resolve_env_value_from(&spec, "default", &db).await;
        assert!(
            !resolved.contains_key("BAD"),
            "Invalid base64 should not produce a value"
        );
    }

    #[tokio::test]
    async fn test_resolve_env_value_from_no_env_array_returns_empty() {
        let db = crate::datastore::test_support::in_memory().await;

        let spec = serde_json::json!({"image": "app"});
        let resolved = resolve_env_value_from(&spec, "default", &db).await;
        assert!(resolved.is_empty(), "No env array should return empty map");
    }

    #[tokio::test]
    async fn test_resolve_env_value_from_configmap_missing_key_non_optional() {
        let db = crate::datastore::test_support::in_memory().await;

        let cm = serde_json::json!({
            "apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {"name": "my-cm", "namespace": "default"},
            "data": {"real-key": "real-value"}
        });
        db.create_resource("v1", "ConfigMap", Some("default"), "my-cm", cm)
            .await
            .unwrap();

        let spec = serde_json::json!({
            "image": "app",
            "env": [{"name": "MISS", "valueFrom": {"configMapKeyRef": {"name": "my-cm", "key": "no-such-key"}}}]
        });

        let resolved = resolve_env_value_from(&spec, "default", &db).await;
        assert!(
            !resolved.contains_key("MISS"),
            "Missing CM key should not produce a value"
        );
    }

    #[tokio::test]
    async fn test_resolve_env_value_from_skips_plain_value_vars() {
        let db = crate::datastore::test_support::in_memory().await;

        let spec = serde_json::json!({
            "image": "app",
            "env": [
                {"name": "PLAIN1", "value": "hello"},
                {"name": "PLAIN2", "value": "world"}
            ]
        });

        let resolved = resolve_env_value_from(&spec, "default", &db).await;
        assert!(
            resolved.is_empty(),
            "Plain value env vars should not appear in resolved map"
        );
    }

    #[tokio::test]
    async fn test_resolve_env_from_secret_ref() {
        use base64::Engine;
        let db = crate::datastore::test_support::in_memory().await;

        // Create Secret with base64-encoded data
        let secret_data = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": "my-secret", "namespace": "default"},
            "data": {
                "USERNAME": base64::engine::general_purpose::STANDARD.encode("admin"),
                "PASSWORD": base64::engine::general_purpose::STANDARD.encode("secret123")
            }
        });
        db.create_resource("v1", "Secret", Some("default"), "my-secret", secret_data)
            .await
            .unwrap();

        let container_spec = serde_json::json!({
            "envFrom": [{"secretRef": {"name": "my-secret"}}]
        });

        let result = resolve_env_from(&container_spec, "default", &db).await;

        assert_eq!(result.len(), 2);
        assert!(result.contains(&("USERNAME".to_string(), "admin".to_string())));
        assert!(result.contains(&("PASSWORD".to_string(), "secret123".to_string())));
    }

    #[tokio::test]
    async fn test_resolve_env_from_configmap_ref() {
        let db = crate::datastore::test_support::in_memory().await;

        // Create ConfigMap
        let cm_data = serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "my-config", "namespace": "default"},
            "data": {
                "LOG_LEVEL": "debug",
                "CACHE_SIZE": "100"
            }
        });
        db.create_resource("v1", "ConfigMap", Some("default"), "my-config", cm_data)
            .await
            .unwrap();

        let container_spec = serde_json::json!({
            "envFrom": [{"configMapRef": {"name": "my-config"}}]
        });

        let result = resolve_env_from(&container_spec, "default", &db).await;

        assert_eq!(result.len(), 2);
        assert!(result.contains(&("LOG_LEVEL".to_string(), "debug".to_string())));
        assert!(result.contains(&("CACHE_SIZE".to_string(), "100".to_string())));
    }

    #[tokio::test]
    async fn test_resolve_env_from_with_prefix() {
        let db = crate::datastore::test_support::in_memory().await;

        // Create ConfigMap
        let cm_data = serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "my-config", "namespace": "default"},
            "data": {"KEY": "value"}
        });
        db.create_resource("v1", "ConfigMap", Some("default"), "my-config", cm_data)
            .await
            .unwrap();

        let container_spec = serde_json::json!({
            "envFrom": [{"prefix": "APP_", "configMapRef": {"name": "my-config"}}]
        });

        let result = resolve_env_from(&container_spec, "default", &db).await;

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, "APP_KEY");
        assert_eq!(result[0].1, "value");
    }

    #[tokio::test]
    async fn test_resolve_env_from_optional_missing_skipped() {
        let db = crate::datastore::test_support::in_memory().await;

        let container_spec = serde_json::json!({
            "envFrom": [{"secretRef": {"name": "missing-secret", "optional": true}}]
        });

        let result = resolve_env_from(&container_spec, "default", &db).await;

        // Missing optional resource should be skipped, not error
        assert_eq!(result.len(), 0);
    }

    /// B4 regression: env-var Secret/ConfigMap reads must go through a FRESH
    /// leader read, not the worker cache. A Secret created moments before a
    /// container starts may not yet be in a primed-but-lagging worker cache; a
    /// cached miss would spuriously fail the container with not-found. This mocks
    /// a leader client whose cache (`get_resource`) errors if touched and whose
    /// fresh path (`get_resource_fresh`) returns the object, proving the env
    /// reader uses fresh.
    mod fresh_env_reads {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        use crate::kubelet::pod_env::{EnvSourceReader, LeaderApiEnvSourceReader};
        use klights_cluster_core::Resource;
        use klights_leader_api::{
            CacheReadinessError, CacheReadinessFuture, CacheReadinessRequest, LeaderCacheReadiness,
            LeaderResourceQuery, LeaderWatch, LeaderWatchError, LeaderWatchFuture,
            ResourceGetRequest, ResourceListRequest, ResourceListResult, ResourceQueryConsistency,
            ResourceQueryError, ResourceQueryFuture, WatchRequest,
        };

        struct FreshOnlyLeaderApiClient {
            resource: Resource,
            cache_calls: AtomicUsize,
            fresh_calls: AtomicUsize,
        }

        impl LeaderResourceQuery for FreshOnlyLeaderApiClient {
            fn get_resource(
                &self,
                request: ResourceGetRequest,
            ) -> ResourceQueryFuture<'_, Option<Resource>> {
                Box::pin(async move {
                    let consistency = request.consistency();
                    let key = request.into_key();
                    if consistency == ResourceQueryConsistency::Cached {
                        self.cache_calls.fetch_add(1, Ordering::SeqCst);
                        return Err(ResourceQueryError::query_failed(format!(
                            "env reads must not hit the worker cache for {key:?}"
                        )));
                    }
                    self.fresh_calls.fetch_add(1, Ordering::SeqCst);
                    Ok((key.kind == self.resource.kind
                        && key.namespace.as_deref() == self.resource.namespace.as_deref()
                        && key.name == self.resource.name)
                        .then(|| self.resource.clone()))
                })
            }

            fn list_resources(
                &self,
                request: ResourceListRequest,
            ) -> ResourceQueryFuture<'_, ResourceListResult> {
                Box::pin(async move {
                    Err(ResourceQueryError::query_failed(format!(
                        "unexpected list_resources {request:?}"
                    )))
                })
            }
        }

        impl LeaderWatch for FreshOnlyLeaderApiClient {
            fn watch_resources(&self, _req: WatchRequest) -> LeaderWatchFuture<'_> {
                Box::pin(async { Err(LeaderWatchError::unavailable("unexpected watch_resources")) })
            }
        }

        impl LeaderCacheReadiness for FreshOnlyLeaderApiClient {
            fn wait_cache_ready(&self, _scope: CacheReadinessRequest) -> CacheReadinessFuture<'_> {
                Box::pin(async {
                    Err(CacheReadinessError::unavailable(
                        "unexpected wait_cache_ready",
                    ))
                })
            }
        }

        crate::control_plane::client::impl_unavailable_leader_pod_effects!(
            FreshOnlyLeaderApiClient
        );

        fn secret_resource() -> Resource {
            Resource {
                id: 1,
                api_version: "v1".to_string(),
                kind: "Secret".to_string(),
                namespace: Some("default".to_string()),
                name: "fresh-secret".to_string(),
                uid: "sec-uid".to_string(),
                resource_version: 9,
                data: serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Secret",
                    "metadata": {"namespace": "default", "name": "fresh-secret"}
                })
                .into(),
            }
        }

        #[tokio::test]
        async fn env_secret_read_uses_fresh_leader_not_cache() {
            let client = Arc::new(FreshOnlyLeaderApiClient {
                resource: secret_resource(),
                cache_calls: AtomicUsize::new(0),
                fresh_calls: AtomicUsize::new(0),
            });
            let reader = LeaderApiEnvSourceReader::new(client.clone());

            let found = reader
                .secret("default", "fresh-secret")
                .await
                .expect("secret lookup must succeed");

            assert_eq!(
                found.as_ref().map(|r| r.uid.as_str()),
                Some("sec-uid"),
                "env Secret read must find the freshly-created Secret"
            );
            assert_eq!(client.fresh_calls.load(Ordering::SeqCst), 1);
            assert_eq!(
                client.cache_calls.load(Ordering::SeqCst),
                0,
                "env Secret read must not consult the worker cache"
            );
        }
    }
}
