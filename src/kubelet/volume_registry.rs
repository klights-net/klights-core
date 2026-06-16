use crate::kubelet::volumes;
use anyhow::{Context, Result};
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

pub struct VolumeContext<'a> {
    pub sources: &'a dyn crate::kubelet::volume_sources::VolumeSourceReader,
    pub namespace: &'a str,
    pub pod_name: &'a str,
    pub pod_dir_id: &'a str,
    pub pod: &'a Value,
    pub containerd_namespace: &'a str,
}

#[async_trait]
pub trait VolumeHandler: Send + Sync {
    fn volume_type(&self) -> &'static str;
    async fn setup(
        &self,
        volume: &Value,
        volume_name: &str,
        ctx: &VolumeContext<'_>,
    ) -> Result<Option<String>>;
}

pub struct VolumeRegistry {
    handlers: HashMap<&'static str, Arc<dyn VolumeHandler>>,
}

impl VolumeRegistry {
    pub fn with_defaults() -> Self {
        let mut registry = Self {
            handlers: HashMap::new(),
        };
        registry.register(Arc::new(EmptyDirHandler));
        registry.register(Arc::new(HostPathHandler));
        registry.register(Arc::new(ConfigMapHandler));
        registry.register(Arc::new(SecretHandler));
        registry.register(Arc::new(DownwardApiHandler));
        registry.register(Arc::new(ProjectedHandler));
        registry.register(Arc::new(PersistentVolumeClaimHandler));
        registry
    }

    pub fn has_handler(&self, volume_type: &str) -> bool {
        self.handlers.contains_key(volume_type)
    }

    pub fn supported_type<'a>(&self, volume: &'a Value) -> Option<&'a str> {
        volume
            .as_object()
            .and_then(|obj| obj.keys().find(|k| self.has_handler(k)))
            .map(String::as_str)
    }

    pub async fn resolve_path(
        &self,
        volume: &Value,
        volume_name: &str,
        ctx: &VolumeContext<'_>,
    ) -> Result<Option<String>> {
        let Some(volume_type) = self.supported_type(volume) else {
            return Ok(None);
        };
        let handler = self.handlers.get(volume_type).ok_or_else(|| {
            anyhow::anyhow!("Missing volume handler for registered type {}", volume_type)
        })?;
        handler.setup(volume, volume_name, ctx).await
    }

    fn register(&mut self, handler: Arc<dyn VolumeHandler>) {
        self.handlers.insert(handler.volume_type(), handler);
    }
}

async fn ensure_optional_placeholder_volume_path(
    volume_type_dir: &str,
    pod_dir_id: &str,
    volume_name: &str,
) -> Result<String> {
    let volume_path = format!(
        "{}/{}/volumes/{}/{}",
        crate::kubelet::volumes::volumes_root(),
        pod_dir_id,
        volume_type_dir,
        volume_name
    );
    let volume_key = volume_path.clone();
    let volume_path_for_blocking = volume_path.clone();
    volumes::run_blocking_fs_keyed(
        "ensure_optional_placeholder_volume_path",
        &volume_key,
        move || ensure_optional_placeholder_volume_path_blocking(&volume_path_for_blocking),
    )
    .await?;
    Ok(volume_path)
}

fn ensure_optional_placeholder_volume_path_blocking(volume_path: &str) -> Result<()> {
    std::fs::create_dir_all(volume_path).with_context(|| {
        format!(
            "Failed to create optional volume placeholder directory {}",
            volume_path
        )
    })
}

struct EmptyDirHandler;
struct HostPathHandler;
struct ConfigMapHandler;
struct SecretHandler;
struct DownwardApiHandler;
struct ProjectedHandler;
struct PersistentVolumeClaimHandler;

async fn lookup_node_uid(
    sources: &dyn crate::kubelet::volume_sources::VolumeSourceReader,
    node_name: &str,
) -> Option<String> {
    sources
        .node(node_name)
        .await
        .ok()
        .flatten()
        .and_then(|res| {
            res.data
                .pointer("/metadata/uid")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
        })
}

#[async_trait]
impl VolumeHandler for EmptyDirHandler {
    fn volume_type(&self) -> &'static str {
        "emptyDir"
    }

    async fn setup(
        &self,
        volume: &Value,
        volume_name: &str,
        ctx: &VolumeContext<'_>,
    ) -> Result<Option<String>> {
        let empty_dir = volume
            .get("emptyDir")
            .ok_or_else(|| anyhow::anyhow!("emptyDir volume missing emptyDir field"))?;
        let medium = empty_dir
            .get("medium")
            .and_then(|m| m.as_str())
            .map(str::to_string);
        let size_limit = empty_dir
            .get("sizeLimit")
            .and_then(|s| s.as_str())
            .map(str::to_string);
        let containerd_namespace = ctx.containerd_namespace.to_string();
        let pod_dir_id = ctx.pod_dir_id.to_string();
        let volume_name = volume_name.to_string();
        let key = volumes::empty_dir_volume_path_for_namespace(
            &containerd_namespace,
            &pod_dir_id,
            &volume_name,
        );
        let path = volumes::run_blocking_fs_keyed("create_empty_dir", &key, move || {
            volumes::create_empty_dir_for_namespace(
                &containerd_namespace,
                &pod_dir_id,
                &volume_name,
                medium.as_deref(),
                size_limit.as_deref(),
            )
        })
        .await?;
        Ok(Some(path))
    }
}

#[async_trait]
impl VolumeHandler for HostPathHandler {
    fn volume_type(&self) -> &'static str {
        "hostPath"
    }

    async fn setup(
        &self,
        volume: &Value,
        _volume_name: &str,
        _ctx: &VolumeContext<'_>,
    ) -> Result<Option<String>> {
        let host_path = volume
            .get("hostPath")
            .ok_or_else(|| anyhow::anyhow!("hostPath volume missing hostPath field"))?;
        let path = host_path
            .get("path")
            .and_then(|p| p.as_str())
            .ok_or_else(|| anyhow::anyhow!("hostPath missing path"))?
            .to_string();
        let host_type = host_path
            .get("type")
            .and_then(|t| t.as_str())
            .map(str::to_string);
        let resolved =
            crate::kubelet::file_blocking::run_blocking_file("resolve_host_path", move || {
                volumes::resolve_host_path(&path, host_type.as_deref())
            })
            .await?;
        Ok(Some(resolved))
    }
}

#[async_trait]
impl VolumeHandler for ConfigMapHandler {
    fn volume_type(&self) -> &'static str {
        "configMap"
    }

    async fn setup(
        &self,
        volume: &Value,
        volume_name: &str,
        ctx: &VolumeContext<'_>,
    ) -> Result<Option<String>> {
        let config_map = volume
            .get("configMap")
            .ok_or_else(|| anyhow::anyhow!("configMap volume missing configMap field"))?;
        let cm_name = config_map
            .get("name")
            .and_then(|n| n.as_str())
            .ok_or_else(|| anyhow::anyhow!("configMap missing name"))?;
        let optional = config_map
            .get("optional")
            .and_then(|o| o.as_bool())
            .unwrap_or(false);
        let default_mode = config_map
            .get("defaultMode")
            .and_then(|m| m.as_u64())
            .map(|m| m as u32);
        let items = config_map.get("items");

        match volumes::create_config_map_volume(
            ctx.sources,
            ctx.namespace,
            cm_name,
            ctx.pod_dir_id,
            volume_name,
            default_mode,
            items,
        )
        .await
        {
            Ok(path) => Ok(Some(path)),
            Err(_e) if optional => {
                tracing::info!(
                    "Optional configMap volume {} ({}) not found, skipping",
                    volume_name,
                    cm_name
                );
                ensure_optional_placeholder_volume_path("config-map", ctx.pod_dir_id, volume_name)
                    .await
                    .map(Some)
            }
            Err(e) => Err(e),
        }
    }
}

#[async_trait]
impl VolumeHandler for SecretHandler {
    fn volume_type(&self) -> &'static str {
        "secret"
    }

    async fn setup(
        &self,
        volume: &Value,
        volume_name: &str,
        ctx: &VolumeContext<'_>,
    ) -> Result<Option<String>> {
        let secret = volume
            .get("secret")
            .ok_or_else(|| anyhow::anyhow!("secret volume missing secret field"))?;
        let secret_name = secret
            .get("secretName")
            .and_then(|n| n.as_str())
            .ok_or_else(|| anyhow::anyhow!("secret missing secretName"))?;
        let optional = secret
            .get("optional")
            .and_then(|o| o.as_bool())
            .unwrap_or(false);
        let default_mode = secret
            .get("defaultMode")
            .and_then(|m| m.as_u64())
            .map(|m| m as u32);
        let items = secret.get("items");

        match volumes::create_secret_volume(
            ctx.sources,
            ctx.namespace,
            secret_name,
            ctx.pod_dir_id,
            volume_name,
            default_mode,
            items,
        )
        .await
        {
            Ok(path) => Ok(Some(path)),
            Err(_e) if optional => {
                tracing::info!(
                    "Optional secret volume {} ({}) not found, skipping",
                    volume_name,
                    secret_name
                );
                ensure_optional_placeholder_volume_path("secret", ctx.pod_dir_id, volume_name)
                    .await
                    .map(Some)
            }
            Err(e) => Err(e),
        }
    }
}

#[async_trait]
impl VolumeHandler for DownwardApiHandler {
    fn volume_type(&self) -> &'static str {
        "downwardAPI"
    }

    async fn setup(
        &self,
        volume: &Value,
        volume_name: &str,
        ctx: &VolumeContext<'_>,
    ) -> Result<Option<String>> {
        let downward_api = volume
            .get("downwardAPI")
            .ok_or_else(|| anyhow::anyhow!("downwardAPI volume missing downwardAPI field"))?;
        let default_mode = downward_api
            .get("defaultMode")
            .and_then(|m| m.as_u64())
            .map(|m| m as u32);
        let items = downward_api
            .get("items")
            .ok_or_else(|| anyhow::anyhow!("downwardAPI missing items"))?;

        let path = volumes::create_downward_api_volume_ns(volumes::DownwardApiVolumeNsRequest {
            sources: ctx.sources,
            namespace: ctx.namespace,
            pod_dir_id: ctx.pod_dir_id,
            pod_db_name: ctx.pod_name,
            volume_name,
            default_mode,
            items,
        })
        .await?;
        Ok(Some(path))
    }
}

#[async_trait]
impl VolumeHandler for ProjectedHandler {
    fn volume_type(&self) -> &'static str {
        "projected"
    }

    async fn setup(
        &self,
        volume: &Value,
        volume_name: &str,
        ctx: &VolumeContext<'_>,
    ) -> Result<Option<String>> {
        let projected = volume
            .get("projected")
            .ok_or_else(|| anyhow::anyhow!("projected volume missing projected field"))?;
        let default_mode = projected
            .get("defaultMode")
            .and_then(|m| m.as_u64())
            .map(|m| m as u32);
        let sources = projected
            .get("sources")
            .ok_or_else(|| anyhow::anyhow!("projected volume missing sources"))?;

        let needs_token = sources
            .as_array()
            .map(|arr| arr.iter().any(|s| s.get("serviceAccountToken").is_some()))
            .unwrap_or(false);

        let mut sources_for_write = sources.clone();
        if needs_token {
            let sa_name = ctx
                .pod
                .get("spec")
                .and_then(|s| s.get("serviceAccountName"))
                .and_then(|n| n.as_str())
                .filter(|s| !s.is_empty())
                .unwrap_or("default");
            let pod_uid = ctx
                .pod
                .pointer("/metadata/uid")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty());
            let node_name = ctx
                .pod
                .pointer("/spec/nodeName")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty());
            let node_uid = if let Some(node_name) = node_name {
                lookup_node_uid(ctx.sources, node_name).await
            } else {
                None
            };
            if let Some(arr) = sources_for_write.as_array_mut() {
                for source in arr {
                    let Some(sa_token) = source
                        .get_mut("serviceAccountToken")
                        .and_then(|v| v.as_object_mut())
                    else {
                        continue;
                    };

                    let audience = sa_token
                        .get("audience")
                        .and_then(|v| v.as_str())
                        .filter(|s| !s.is_empty())
                        .unwrap_or("https://kubernetes.default.svc.cluster.local");
                    let expiration_seconds =
                        crate::auth::normalize_service_account_token_expiration_seconds(
                            sa_token.get("expirationSeconds").and_then(|v| v.as_i64()),
                        );

                    let token = ctx
                        .sources
                        .projected_service_account_token(
                            crate::kubelet::volume_sources::ProjectedServiceAccountTokenRequest {
                                namespace: ctx.namespace.to_string(),
                                service_account_name: sa_name.to_string(),
                                audiences: vec![audience.to_string()],
                                expiration_seconds,
                                bound_pod_name: Some(ctx.pod_name.to_string()),
                                bound_pod_uid: pod_uid.map(str::to_string),
                                bound_node_name: node_name.map(str::to_string),
                                bound_node_uid: node_uid.clone(),
                            },
                        )
                        .await?
                        .token;
                    sa_token.insert("tokenValue".to_string(), Value::String(token));
                }
            }
        }

        let path = volumes::create_projected_volume_ns(volumes::ProjectedVolumeNsRequest {
            source_reader: ctx.sources,
            namespace: ctx.namespace,
            pod_dir_id: ctx.pod_dir_id,
            pod_db_name: ctx.pod_name,
            pod: ctx.pod,
            volume_name,
            default_mode,
            sources: &sources_for_write,
            token: None,
        })
        .await?;

        Ok(Some(path))
    }
}

#[async_trait]
impl VolumeHandler for PersistentVolumeClaimHandler {
    fn volume_type(&self) -> &'static str {
        "persistentVolumeClaim"
    }

    async fn setup(
        &self,
        volume: &Value,
        _volume_name: &str,
        ctx: &VolumeContext<'_>,
    ) -> Result<Option<String>> {
        let pvc = volume.get("persistentVolumeClaim").ok_or_else(|| {
            anyhow::anyhow!("persistentVolumeClaim volume missing persistentVolumeClaim field")
        })?;
        let pvc_name = pvc
            .get("claimName")
            .and_then(|n| n.as_str())
            .ok_or_else(|| anyhow::anyhow!("persistentVolumeClaim missing claimName"))?;

        let pvc_resource = ctx
            .sources
            .persistent_volume_claim(ctx.namespace, pvc_name)
            .await?
            .ok_or_else(|| anyhow::anyhow!("PVC {} not found", pvc_name))?;

        let pvc_phase = pvc_resource
            .data
            .get("status")
            .and_then(|s| s.get("phase"))
            .and_then(|p| p.as_str());
        if pvc_phase != Some("Bound") {
            return Err(anyhow::anyhow!(
                "PVC {} is not Bound (phase: {:?})",
                pvc_name,
                pvc_phase
            ));
        }

        let pv_name = pvc_resource
            .data
            .get("status")
            .and_then(|s| s.get("volumeName"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("PVC {} has no volumeName", pvc_name))?;

        let pv_resource = ctx
            .sources
            .persistent_volume(pv_name)
            .await?
            .ok_or_else(|| anyhow::anyhow!("PV {} not found", pv_name))?;

        let pv_host_path = pv_resource
            .data
            .get("spec")
            .and_then(|s| s.get("hostPath"))
            .and_then(|h| h.get("path"))
            .and_then(|p| p.as_str())
            .ok_or_else(|| anyhow::anyhow!("PV {} has no hostPath.path", pv_name))?;

        let pv_host_path = pv_host_path.to_string();
        let resolved = crate::kubelet::file_blocking::run_blocking_file(
            "resolve_persistent_volume_host_path",
            move || volumes::resolve_host_path(&pv_host_path, None),
        )
        .await?;
        Ok(Some(resolved))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use anyhow::Result;
    use async_trait::async_trait;
    use serde_json::{Value, json};
    use std::sync::{Arc, Mutex, OnceLock};

    fn env_lock() -> &'static tokio::sync::Mutex<()> {
        static ENV_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
        ENV_LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
    }

    fn test_resource(
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
    ) -> crate::datastore::Resource {
        crate::datastore::Resource {
            id: 1,
            api_version: api_version.to_string(),
            kind: kind.to_string(),
            namespace: namespace.map(str::to_string),
            name: name.to_string(),
            uid: data
                .pointer("/metadata/uid")
                .and_then(|v| v.as_str())
                .unwrap_or("uid")
                .to_string(),
            resource_version: 1,
            data: Arc::new(data),
        }
    }

    struct RecordingVolumeSourceReader {
        resources: Vec<crate::datastore::Resource>,
        token: String,
        token_requests:
            Mutex<Vec<crate::kubelet::volume_sources::ProjectedServiceAccountTokenRequest>>,
    }

    impl RecordingVolumeSourceReader {
        fn new(resources: Vec<crate::datastore::Resource>, token: impl Into<String>) -> Self {
            Self {
                resources,
                token: token.into(),
                token_requests: Mutex::new(Vec::new()),
            }
        }

        fn token_requests(
            &self,
        ) -> Vec<crate::kubelet::volume_sources::ProjectedServiceAccountTokenRequest> {
            self.token_requests.lock().unwrap().clone()
        }

        fn find(
            &self,
            api_version: &str,
            kind: &str,
            namespace: Option<&str>,
            name: &str,
        ) -> Option<crate::datastore::Resource> {
            self.resources
                .iter()
                .find(|resource| {
                    resource.api_version == api_version
                        && resource.kind == kind
                        && resource.namespace.as_deref() == namespace
                        && resource.name == name
                })
                .cloned()
        }
    }

    #[async_trait]
    impl crate::kubelet::volume_sources::VolumeSourceReader for RecordingVolumeSourceReader {
        async fn config_map(
            &self,
            namespace: &str,
            name: &str,
        ) -> Result<Option<crate::datastore::Resource>> {
            Ok(self.find("v1", "ConfigMap", Some(namespace), name))
        }

        async fn secret(
            &self,
            namespace: &str,
            name: &str,
        ) -> Result<Option<crate::datastore::Resource>> {
            Ok(self.find("v1", "Secret", Some(namespace), name))
        }

        async fn service_account(
            &self,
            namespace: &str,
            name: &str,
        ) -> Result<Option<crate::datastore::Resource>> {
            Ok(self.find("v1", "ServiceAccount", Some(namespace), name))
        }

        async fn pod(
            &self,
            namespace: &str,
            name: &str,
        ) -> Result<Option<crate::datastore::Resource>> {
            Ok(self.find("v1", "Pod", Some(namespace), name))
        }

        async fn node(&self, name: &str) -> Result<Option<crate::datastore::Resource>> {
            Ok(self.find("v1", "Node", None, name))
        }

        async fn persistent_volume_claim(
            &self,
            namespace: &str,
            name: &str,
        ) -> Result<Option<crate::datastore::Resource>> {
            Ok(self.find("v1", "PersistentVolumeClaim", Some(namespace), name))
        }

        async fn persistent_volume(
            &self,
            name: &str,
        ) -> Result<Option<crate::datastore::Resource>> {
            Ok(self.find("v1", "PersistentVolume", None, name))
        }

        async fn projected_service_account_token(
            &self,
            request: crate::kubelet::volume_sources::ProjectedServiceAccountTokenRequest,
        ) -> Result<crate::kubelet::volume_sources::ProjectedServiceAccountToken> {
            self.token_requests.lock().unwrap().push(request);
            Ok(
                crate::kubelet::volume_sources::ProjectedServiceAccountToken {
                    token: self.token.clone(),
                },
            )
        }
    }

    #[test]
    fn test_default_registry_has_core_handlers() {
        let registry = VolumeRegistry::with_defaults();
        assert!(registry.has_handler("emptyDir"));
        assert!(registry.has_handler("hostPath"));
        assert!(registry.has_handler("configMap"));
        assert!(registry.has_handler("secret"));
        assert!(registry.has_handler("downwardAPI"));
        assert!(registry.has_handler("projected"));
        assert!(registry.has_handler("persistentVolumeClaim"));
    }

    #[test]
    fn test_supported_type_detects_registered_volume_kind() {
        let registry = VolumeRegistry::with_defaults();
        let volume = json!({
            "name": "cfg",
            "configMap": {
                "name": "app-config"
            }
        });
        assert_eq!(registry.supported_type(&volume), Some("configMap"));
    }

    #[test]
    fn test_supported_type_returns_none_for_unknown_kind() {
        let registry = VolumeRegistry::with_defaults();
        let volume = json!({
            "name": "mystery",
            "cephfs": {}
        });
        assert_eq!(registry.supported_type(&volume), None);
    }

    #[tokio::test]
    async fn test_projected_service_account_token_requests_leader_issued_token() {
        let registry = VolumeRegistry::with_defaults();
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let runtime_ns = format!("volreg-no-local-signer-{}", suffix);
        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "sa-pod", "namespace": "default", "uid": "pod-uid-a"},
            "spec": {"serviceAccountName": "default", "nodeName": "node-a"}
        });
        let volume = json!({
            "name": "kube-api-access-x",
            "projected": {
                "sources": [
                    {"serviceAccountToken": {
                        "path": "token",
                        "audience": "oidc-discovery-test",
                        "expirationSeconds": 7200
                    }}
                ]
            }
        });
        let sources = RecordingVolumeSourceReader::new(
            vec![
                test_resource(
                    "v1",
                    "ServiceAccount",
                    Some("default"),
                    "default",
                    json!({
                        "apiVersion": "v1",
                        "kind": "ServiceAccount",
                        "metadata": {"name": "default", "namespace": "default", "uid": "default-sa-uid"}
                    }),
                ),
                test_resource(
                    "v1",
                    "Node",
                    None,
                    "node-a",
                    json!({
                        "apiVersion": "v1",
                        "kind": "Node",
                        "metadata": {"name": "node-a", "uid": "node-uid-a"}
                    }),
                ),
            ],
            "leader-issued-token",
        );

        let ctx = VolumeContext {
            sources: &sources,
            namespace: "default",
            pod_name: "sa-pod",
            pod_dir_id: "default_sa-pod",
            pod: &pod,
            containerd_namespace: &runtime_ns,
        };

        let resolved = registry
            .resolve_path(&volume, "kube-api-access-x", &ctx)
            .await
            .expect("projected SA token should come from the leader API path")
            .expect("projected volume path should be created");

        assert_eq!(
            crate::utils::read_utf8_file(format!("{}/token", resolved)).unwrap(),
            "leader-issued-token"
        );
        let requests = sources.token_requests();
        assert_eq!(requests.len(), 1);
        let request = &requests[0];
        assert_eq!(request.namespace, "default");
        assert_eq!(request.service_account_name, "default");
        assert_eq!(request.audiences, vec!["oidc-discovery-test".to_string()]);
        assert_eq!(request.expiration_seconds, 7200);
        assert_eq!(request.bound_pod_name.as_deref(), Some("sa-pod"));
        assert_eq!(request.bound_pod_uid.as_deref(), Some("pod-uid-a"));
        assert_eq!(request.bound_node_name.as_deref(), Some("node-a"));

        let _ = std::fs::remove_dir_all(crate::paths::data_root_path(&runtime_ns));
    }

    #[tokio::test]
    async fn test_projected_service_account_token_on_worker_does_not_require_local_ca_cert() {
        let registry = VolumeRegistry::with_defaults();
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let runtime_ns = format!("volreg-worker-sa-token-{}", suffix);

        let pod_name = "sa-pod";
        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": pod_name,
                "namespace": "default",
                "uid": "pod-uid-a"
            },
            "spec": {
                "serviceAccountName": "default",
                "containers": [{"name": "app", "image": "busybox"}],
                "volumes": [{
                    "name": "kube-api-access-x",
                    "projected": {
                        "sources": [
                            {"serviceAccountToken": {"path": "token"}},
                            {"configMap": {"name": "kube-root-ca.crt", "items": [{"key": "ca.crt", "path": "ca.crt"}]}},
                            {"downwardAPI": {"items": [{"path": "namespace", "fieldRef": {"fieldPath": "metadata.namespace"}}]}}
                        ]
                    }
                }]
            }
        });
        let sources = RecordingVolumeSourceReader::new(
            vec![
                test_resource(
                    "v1",
                    "ServiceAccount",
                    Some("default"),
                    "default",
                    json!({
                        "apiVersion": "v1",
                        "kind": "ServiceAccount",
                        "metadata": {"name": "default", "namespace": "default", "uid": "default-sa-uid"}
                    }),
                ),
                test_resource(
                    "v1",
                    "ConfigMap",
                    Some("default"),
                    "kube-root-ca.crt",
                    json!({
                        "apiVersion": "v1",
                        "kind": "ConfigMap",
                        "metadata": {"name": "kube-root-ca.crt", "namespace": "default"},
                        "data": {"ca.crt": "cluster-ca-from-configmap"}
                    }),
                ),
                test_resource("v1", "Pod", Some("default"), pod_name, pod.clone()),
            ],
            "worker-leader-token",
        );

        let volume = pod["spec"]["volumes"][0].clone();
        let ctx = VolumeContext {
            sources: &sources,
            namespace: "default",
            pod_name,
            pod_dir_id: "default_sa-pod",
            pod: &pod,
            containerd_namespace: &runtime_ns,
        };

        let resolved = registry
            .resolve_path(&volume, "kube-api-access-x", &ctx)
            .await
            .expect("joined workers should project SA volumes without a local ca.crt")
            .expect("projected volume path should be created");

        assert!(std::path::Path::new(&format!("{}/token", resolved)).exists());
        assert_eq!(
            crate::utils::read_utf8_file(format!("{}/token", resolved)).unwrap(),
            "worker-leader-token"
        );
        assert_eq!(
            crate::utils::read_utf8_file(format!("{}/ca.crt", resolved)).unwrap(),
            "cluster-ca-from-configmap"
        );
        assert_eq!(
            crate::utils::read_utf8_file(format!("{}/namespace", resolved)).unwrap(),
            "default"
        );
        assert_eq!(sources.token_requests().len(), 1);

        let _ = std::fs::remove_dir_all(crate::paths::data_root_path(&runtime_ns));
    }

    #[tokio::test]
    async fn test_projected_downward_api_uses_current_pod_snapshot() {
        let registry = VolumeRegistry::with_defaults();
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let runtime_ns = format!("volreg-projected-pod-snapshot-{}", suffix);

        let pod_name = "downwardapi-volume-test";
        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": pod_name,
                "namespace": "default",
                "uid": "pod-uid-a"
            },
            "spec": {
                "containers": [{"name": "app", "image": "busybox"}],
                "volumes": [{
                    "name": "podinfo",
                    "projected": {
                        "sources": [
                            {"downwardAPI": {"items": [
                                {"path": "podname", "fieldRef": {"fieldPath": "metadata.name"}}
                            ]}}
                        ]
                    }
                }]
            }
        });
        let stale_pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "",
                "namespace": "default",
                "uid": "stale-pod-uid"
            },
            "spec": {"containers": [{"name": "app", "image": "busybox"}]}
        });
        let sources = RecordingVolumeSourceReader::new(
            vec![test_resource(
                "v1",
                "Pod",
                Some("default"),
                pod_name,
                stale_pod,
            )],
            "unused-token",
        );

        let volume = pod["spec"]["volumes"][0].clone();
        let ctx = VolumeContext {
            sources: &sources,
            namespace: "default",
            pod_name,
            pod_dir_id: "default_downwardapi-volume-test",
            pod: &pod,
            containerd_namespace: &runtime_ns,
        };

        let resolved = registry
            .resolve_path(&volume, "podinfo", &ctx)
            .await
            .expect("projected downwardAPI should render from the current pod snapshot")
            .expect("projected volume path should be created");

        assert_eq!(
            crate::utils::read_utf8_file(format!("{}/podname", resolved)).unwrap(),
            pod_name
        );

        let _ = std::fs::remove_dir_all(crate::paths::data_root_path(&runtime_ns));
    }

    #[tokio::test]
    async fn test_optional_configmap_missing_creates_placeholder_path() {
        let _env_guard = env_lock().lock().await;
        let db = crate::datastore::test_support::in_memory().await;
        let registry = VolumeRegistry::with_defaults();

        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let runtime_ns = format!("volreg-test-cm-{}", suffix);
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var("KLIGHTS_CONTAINERD_NAMESPACE", &runtime_ns) };

        let pod_dir_id = "default_optional-cm";
        let volume_name = "cfg";
        let expected_path = format!(
            "{}/{}/volumes/config-map/{}",
            crate::kubelet::volumes::volumes_root(),
            pod_dir_id,
            volume_name
        );

        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "optional-cm", "namespace": "default"},
            "spec": {
                "containers": [{"name": "app", "image": "busybox"}],
                "volumes": [{
                    "name": volume_name,
                    "configMap": {"name": "missing-cm", "optional": true}
                }]
            }
        });
        let volume = json!({
            "name": volume_name,
            "configMap": {"name": "missing-cm", "optional": true}
        });
        let ctx = VolumeContext {
            sources: &db,
            namespace: "default",
            pod_name: "optional-cm",
            pod_dir_id,
            pod: &pod,
            containerd_namespace: &runtime_ns,
        };

        let resolved = registry
            .resolve_path(&volume, volume_name, &ctx)
            .await
            .expect("optional missing configmap should resolve to placeholder path");

        assert_eq!(resolved.as_deref(), Some(expected_path.as_str()));
        assert!(std::path::Path::new(&expected_path).exists());

        let _ = std::fs::remove_dir_all(crate::paths::data_root_path(&runtime_ns));
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::remove_var("KLIGHTS_CONTAINERD_NAMESPACE") };
    }

    #[tokio::test]
    async fn test_empty_dir_setup_uses_keyed_filesystem_task() {
        let db = crate::datastore::test_support::in_memory().await;
        let registry = VolumeRegistry::with_defaults();

        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let runtime_ns = format!("volreg-emptydir-{suffix}");
        let pod_dir_id = format!("default_emptydir-pod-{suffix}");

        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "emptydir-pod", "namespace": "default"},
            "spec": {
                "containers": [{"name": "app", "image": "busybox"}],
                "volumes": [{"name": "work", "emptyDir": {}}]
            }
        });
        let volume = json!({"name": "work", "emptyDir": {}});
        let ctx = VolumeContext {
            sources: &db,
            namespace: "default",
            pod_name: "emptydir-pod",
            pod_dir_id: &pod_dir_id,
            pod: &pod,
            containerd_namespace: &runtime_ns,
        };

        let key = volumes::empty_dir_volume_path_for_namespace(&runtime_ns, &pod_dir_id, "work");
        let before = volumes::blocking_fs_keyed_call_count_for("create_empty_dir", &key);
        let resolved = registry
            .resolve_path(&volume, "work", &ctx)
            .await
            .expect("emptyDir should resolve");
        let after = volumes::blocking_fs_keyed_call_count_for("create_empty_dir", &key);

        assert_eq!(
            after,
            before + 1,
            "emptyDir setup must serialize by volume path so concurrent restarts cannot stack tmpfs mounts"
        );
        let resolved = resolved.expect("emptyDir should return a host path");
        assert!(
            resolved.ends_with(&format!("{pod_dir_id}/volumes/empty-dir/work")),
            "unexpected emptyDir path: {resolved}"
        );
        let _ = std::fs::remove_dir_all(
            std::path::Path::new(&resolved)
                .ancestors()
                .nth(3)
                .unwrap_or_else(|| std::path::Path::new(&resolved)),
        );
    }

    #[tokio::test]
    async fn test_projected_service_account_token_includes_node_uid_when_node_exists() {
        let registry = VolumeRegistry::with_defaults();
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let runtime_ns = format!("volreg-test-nodeuid-{}", suffix);

        let node_name = "node-a";
        let node_uid = "node-uid-a";

        let pod_name = "sa-pod";
        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": pod_name,
                "namespace": "default",
                "uid": "pod-uid-a"
            },
            "spec": {
                "serviceAccountName": "default",
                "nodeName": node_name
            }
        });
        let sources = RecordingVolumeSourceReader::new(
            vec![test_resource(
                "v1",
                "Node",
                None,
                node_name,
                json!({
                    "apiVersion": "v1",
                    "kind": "Node",
                    "metadata": {"name": node_name, "uid": node_uid}
                }),
            )],
            "node-bound-token",
        );
        let volume = json!({
            "name": "kube-api-access-x",
            "projected": {
                "sources": [
                    {"serviceAccountToken": {"path": "token", "audience": "oidc-discovery-test"}}
                ]
            }
        });
        let ctx = VolumeContext {
            sources: &sources,
            namespace: "default",
            pod_name,
            pod_dir_id: "default_sa-pod",
            pod: &pod,
            containerd_namespace: &runtime_ns,
        };

        let resolved = registry
            .resolve_path(&volume, "kube-api-access-x", &ctx)
            .await
            .unwrap()
            .expect("projected volume path should be created");
        assert_eq!(
            crate::utils::read_utf8_file(format!("{}/token", resolved)).unwrap(),
            "node-bound-token"
        );

        let requests = sources.token_requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].audiences,
            vec!["oidc-discovery-test".to_string()]
        );
        assert_eq!(requests[0].bound_node_name.as_deref(), Some(node_name));
        assert_eq!(requests[0].bound_node_uid.as_deref(), Some(node_uid));

        let _ = std::fs::remove_dir_all(crate::paths::data_root_path(&runtime_ns));
    }

    #[tokio::test]
    async fn test_projected_service_account_token_includes_stored_serviceaccount_uid() {
        let registry = VolumeRegistry::with_defaults();
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let runtime_ns = format!("volreg-test-sauid-{}", suffix);

        let sa_uid = "sa-uid-a";
        let pod_name = "sa-pod";
        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": pod_name,
                "namespace": "default",
                "uid": "pod-uid-a"
            },
            "spec": {"serviceAccountName": "default"}
        });
        let sources = RecordingVolumeSourceReader::new(
            vec![test_resource(
                "v1",
                "ServiceAccount",
                Some("default"),
                "default",
                json!({
                    "apiVersion": "v1",
                    "kind": "ServiceAccount",
                    "metadata": {"name": "default", "namespace": "default", "uid": sa_uid}
                }),
            )],
            "service-account-bound-token",
        );
        let volume = json!({
            "name": "kube-api-access-x",
            "projected": {
                "sources": [
                    {"serviceAccountToken": {"path": "token", "audience": "oidc-discovery-test"}}
                ]
            }
        });
        let ctx = VolumeContext {
            sources: &sources,
            namespace: "default",
            pod_name,
            pod_dir_id: "default_sa-pod",
            pod: &pod,
            containerd_namespace: &runtime_ns,
        };

        let resolved = registry
            .resolve_path(&volume, "kube-api-access-x", &ctx)
            .await
            .unwrap()
            .expect("projected volume path should be created");
        assert_eq!(
            crate::utils::read_utf8_file(format!("{}/token", resolved)).unwrap(),
            "service-account-bound-token"
        );

        let requests = sources.token_requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].service_account_name, "default");
        assert_eq!(requests[0].bound_pod_name.as_deref(), Some(pod_name));
        assert_eq!(requests[0].bound_pod_uid.as_deref(), Some("pod-uid-a"));

        let _ = std::fs::remove_dir_all(crate::paths::data_root_path(&runtime_ns));
    }

    #[tokio::test]
    async fn test_projected_service_account_token_treats_empty_serviceaccount_as_default() {
        let registry = VolumeRegistry::with_defaults();
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let runtime_ns = format!("volreg-test-empty-sauid-{}", suffix);

        let sa_uid = "default-sa-uid";
        let pod_name = "empty-sa-pod";
        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": pod_name,
                "namespace": "default",
                "uid": "pod-uid-a"
            },
            "spec": {"serviceAccountName": ""}
        });
        let sources = RecordingVolumeSourceReader::new(
            vec![test_resource(
                "v1",
                "ServiceAccount",
                Some("default"),
                "default",
                json!({
                    "apiVersion": "v1",
                    "kind": "ServiceAccount",
                    "metadata": {"name": "default", "namespace": "default", "uid": sa_uid}
                }),
            )],
            "default-service-account-token",
        );
        let volume = json!({
            "name": "kube-api-access-x",
            "projected": {
                "sources": [
                    {"serviceAccountToken": {"path": "token", "audience": "oidc-discovery-test"}}
                ]
            }
        });
        let ctx = VolumeContext {
            sources: &sources,
            namespace: "default",
            pod_name,
            pod_dir_id: "default_empty-sa-pod",
            pod: &pod,
            containerd_namespace: &runtime_ns,
        };

        let resolved = registry
            .resolve_path(&volume, "kube-api-access-x", &ctx)
            .await
            .unwrap()
            .expect("projected volume path should be created");
        assert_eq!(
            crate::utils::read_utf8_file(format!("{}/token", resolved)).unwrap(),
            "default-service-account-token"
        );

        let requests = sources.token_requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].service_account_name, "default",
            "empty serviceAccountName must request a token for the Kubernetes default ServiceAccount"
        );

        let _ = std::fs::remove_dir_all(crate::paths::data_root_path(&runtime_ns));
    }

    #[tokio::test]
    async fn test_projected_service_account_token_does_not_read_local_signing_key() {
        let registry = VolumeRegistry::with_defaults();
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let runtime_ns = format!("volreg-test-sa-signer-{}", suffix);
        let etc_dir = crate::paths::etc_dir_path(&runtime_ns)
            .to_string_lossy()
            .into_owned();
        std::fs::create_dir_all(&etc_dir).unwrap();
        std::fs::write(
            format!("{}/service-account-signing.key", etc_dir),
            "this is not a valid signing key and must not be read",
        )
        .unwrap();

        let pod_name = "sa-pod";
        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": pod_name,
                "namespace": "default",
                "uid": "pod-uid-a"
            },
            "spec": {"serviceAccountName": "default"}
        });
        let sources = RecordingVolumeSourceReader::new(Vec::new(), "externally-issued-token");
        let volume = json!({
            "name": "kube-api-access-x",
            "projected": {
                "sources": [
                    {"serviceAccountToken": {"path": "token", "audience": "oidc-discovery-test"}}
                ]
            }
        });
        let ctx = VolumeContext {
            sources: &sources,
            namespace: "default",
            pod_name,
            pod_dir_id: "default_sa-pod",
            pod: &pod,
            containerd_namespace: &runtime_ns,
        };

        let resolved = registry
            .resolve_path(&volume, "kube-api-access-x", &ctx)
            .await
            .unwrap()
            .expect("projected volume path should be created");
        assert_eq!(
            crate::utils::read_utf8_file(format!("{}/token", resolved)).unwrap(),
            "externally-issued-token"
        );
        assert_eq!(sources.token_requests().len(), 1);

        let _ = std::fs::remove_dir_all(crate::paths::data_root_path(&runtime_ns));
    }
}
