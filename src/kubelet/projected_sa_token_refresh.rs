use std::sync::Arc;

use anyhow::Result;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::kubelet::pod_runtime::service::PodRuntimeKey;
use crate::kubelet::volume_sources::VolumeSourceReader;
use crate::task_supervisor::TaskSupervisor;

const DEFAULT_SERVICE_ACCOUNT_AUDIENCE: &str = "https://kubernetes.default.svc.cluster.local";
const MAX_PROJECTED_TOKEN_REFRESH_DELAY: std::time::Duration =
    std::time::Duration::from_secs(24 * 60 * 60);

#[derive(Clone, Debug, PartialEq, Eq)]
struct ProjectedServiceAccountTokenRef {
    volume_name: String,
    token_path: String,
    audience: String,
    expiration_seconds: i64,
    mode: u32,
}

#[derive(Clone)]
pub(crate) struct ProjectedSaTokenRefreshRequest {
    pub sources: Arc<dyn VolumeSourceReader>,
    pub volumes_root: String,
    pub key: PodRuntimeKey,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ProjectedSaTokenRefreshOutcome {
    Continue { next_delay: std::time::Duration },
    Stop,
}

fn projected_service_account_token_refs(pod: &Value) -> Vec<ProjectedServiceAccountTokenRef> {
    let mut refs = Vec::new();
    let Some(volumes) = pod.pointer("/spec/volumes").and_then(|v| v.as_array()) else {
        return refs;
    };

    for volume in volumes {
        let Some(volume_name) = volume.get("name").and_then(|v| v.as_str()) else {
            continue;
        };
        let Some(projected) = volume.get("projected") else {
            continue;
        };
        let default_mode = projected
            .get("defaultMode")
            .and_then(|m| m.as_u64())
            .map(|m| m as u32)
            .unwrap_or(0o644);
        let Some(sources) = projected.get("sources").and_then(|s| s.as_array()) else {
            continue;
        };

        for source in sources {
            let Some(sa_token) = source.get("serviceAccountToken") else {
                continue;
            };
            let token_path = sa_token
                .get("path")
                .and_then(|p| p.as_str())
                .unwrap_or("token");
            let audience = sa_token
                .get("audience")
                .and_then(|a| a.as_str())
                .filter(|a| !a.is_empty())
                .unwrap_or(DEFAULT_SERVICE_ACCOUNT_AUDIENCE);
            let expiration_seconds =
                crate::auth::normalize_service_account_token_expiration_seconds(
                    sa_token.get("expirationSeconds").and_then(|v| v.as_i64()),
                );
            refs.push(ProjectedServiceAccountTokenRef {
                volume_name: volume_name.to_string(),
                token_path: token_path.to_string(),
                audience: audience.to_string(),
                expiration_seconds,
                mode: default_mode,
            });
        }
    }

    refs
}

pub(crate) fn pod_has_projected_service_account_tokens(pod: &Value) -> bool {
    !projected_service_account_token_refs(pod).is_empty()
}

pub(crate) fn refresh_delay_for_expiration_seconds(expiration_seconds: i64) -> std::time::Duration {
    let expiration_seconds =
        crate::auth::normalize_service_account_token_expiration_seconds(Some(expiration_seconds));
    std::time::Duration::from_secs(((expiration_seconds * 80) / 100).max(1) as u64)
        .min(MAX_PROJECTED_TOKEN_REFRESH_DELAY)
}

fn next_refresh_delay_for_pod(pod: &Value) -> Option<std::time::Duration> {
    projected_service_account_token_refs(pod)
        .iter()
        .map(|token_ref| refresh_delay_for_expiration_seconds(token_ref.expiration_seconds))
        .min()
}

fn pod_service_account_name(pod: &Value) -> &str {
    pod.pointer("/spec/serviceAccountName")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or("default")
}

fn pod_node_name(pod: &Value) -> Option<&str> {
    pod.pointer("/spec/nodeName")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
}

fn pod_uid(pod: &Value) -> Option<&str> {
    pod.pointer("/metadata/uid")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
}

fn pod_is_terminal_or_deleting(pod: &Value) -> bool {
    pod.pointer("/metadata/deletionTimestamp")
        .and_then(|v| v.as_str())
        .is_some()
        || matches!(
            pod.pointer("/status/phase").and_then(|v| v.as_str()),
            Some("Succeeded" | "Failed")
        )
}

async fn lookup_node_uid(sources: &dyn VolumeSourceReader, node_name: &str) -> Option<String> {
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
                .map(str::to_string)
        })
}

async fn mint_projected_service_account_token(
    sources: &dyn VolumeSourceReader,
    pod: &Value,
    token_ref: &ProjectedServiceAccountTokenRef,
) -> Result<String> {
    let namespace = pod
        .pointer("/metadata/namespace")
        .and_then(|v| v.as_str())
        .unwrap_or("default");
    let pod_name = pod
        .pointer("/metadata/name")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let pod_uid = pod_uid(pod);
    let node_name = pod_node_name(pod);
    let node_uid = if let Some(node_name) = node_name {
        lookup_node_uid(sources, node_name).await
    } else {
        None
    };
    let service_account = pod_service_account_name(pod);
    sources
        .projected_service_account_token(
            crate::kubelet::volume_sources::ProjectedServiceAccountTokenRequest {
                namespace: namespace.to_string(),
                service_account_name: service_account.to_string(),
                audiences: vec![token_ref.audience.clone()],
                expiration_seconds: token_ref.expiration_seconds,
                bound_pod_name: Some(pod_name.to_string()),
                bound_pod_uid: pod_uid.map(str::to_string),
                bound_node_name: node_name.map(str::to_string),
                bound_node_uid: node_uid,
            },
        )
        .await
        .map(|token| token.token)
}

async fn write_projected_service_account_token_file(
    volume_path: String,
    token_ref: ProjectedServiceAccountTokenRef,
    token: String,
) -> Result<()> {
    if !std::path::Path::new(&volume_path).exists() {
        anyhow::bail!("projected volume path {} no longer exists", volume_path);
    }
    let key = volume_path.clone();
    crate::kubelet::volumes::run_blocking_fs_keyed(
        "refresh_projected_sa_token_file",
        &key,
        move || {
            crate::kubelet::volumes::shared::write_projection_file_blocking(
                &volume_path,
                &token_ref.token_path,
                token.as_bytes(),
                token_ref.mode,
            )
        },
    )
    .await
}

pub(crate) async fn refresh_projected_service_account_tokens_once(
    request: ProjectedSaTokenRefreshRequest,
) -> Result<ProjectedSaTokenRefreshOutcome> {
    let Some(pod_resource) = request
        .sources
        .pod(&request.key.namespace, &request.key.name)
        .await?
    else {
        return Ok(ProjectedSaTokenRefreshOutcome::Stop);
    };
    let pod = pod_resource.data.as_ref();
    if pod_uid(pod) != Some(request.key.uid.as_str()) || pod_is_terminal_or_deleting(pod) {
        return Ok(ProjectedSaTokenRefreshOutcome::Stop);
    }

    let refs = projected_service_account_token_refs(pod);
    if refs.is_empty() {
        return Ok(ProjectedSaTokenRefreshOutcome::Stop);
    }

    let pod_dir_id = format!("{}_{}", request.key.namespace, request.key.name);
    for token_ref in refs.iter().cloned() {
        let token =
            mint_projected_service_account_token(request.sources.as_ref(), pod, &token_ref).await?;
        let volume_path = format!(
            "{}/{}/volumes/projected/{}",
            request.volumes_root, pod_dir_id, token_ref.volume_name
        );
        write_projected_service_account_token_file(volume_path, token_ref, token).await?;
    }

    let next_delay = refs
        .iter()
        .map(|token_ref| refresh_delay_for_expiration_seconds(token_ref.expiration_seconds))
        .min()
        .unwrap_or_else(|| std::time::Duration::from_secs(480));
    Ok(ProjectedSaTokenRefreshOutcome::Continue { next_delay })
}

fn schedule_projected_service_account_token_refresh_after(
    request: ProjectedSaTokenRefreshRequest,
    supervisor: Arc<TaskSupervisor>,
    cancel: CancellationToken,
    delay: std::time::Duration,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>> {
    Box::pin(async move {
        let next_request = request.clone();
        let next_supervisor = supervisor.clone();
        let next_cancel = cancel.clone();
        supervisor
            .spawn_delay("projected_sa_token_refresh", delay, async move {
                if next_cancel.is_cancelled() {
                    return;
                }
                match refresh_projected_service_account_tokens_once(next_request.clone()).await {
                    Ok(ProjectedSaTokenRefreshOutcome::Continue { next_delay }) => {
                        if next_cancel.is_cancelled() {
                            return;
                        }
                        if let Err(err) = schedule_projected_service_account_token_refresh_after(
                            next_request,
                            next_supervisor,
                            next_cancel,
                            next_delay,
                        )
                        .await
                        {
                            tracing::warn!(
                                "Failed to schedule projected ServiceAccount token refresh: {err:#}"
                            );
                        }
                    }
                    Ok(ProjectedSaTokenRefreshOutcome::Stop) => {}
                    Err(err) => {
                        if next_cancel.is_cancelled() {
                            return;
                        }
                        tracing::warn!(
                            namespace = %next_request.key.namespace,
                            name = %next_request.key.name,
                            uid = %next_request.key.uid,
                            "Projected ServiceAccount token refresh failed: {err:#}"
                        );
                        if let Err(err) = schedule_projected_service_account_token_refresh_after(
                            next_request,
                            next_supervisor,
                            next_cancel,
                            std::time::Duration::from_secs(60),
                        )
                        .await
                        {
                            tracing::warn!(
                                "Failed to schedule projected ServiceAccount token refresh retry: {err:#}"
                            );
                        }
                    }
                }
            })
            .await?;
        Ok(())
    })
}

pub(crate) async fn schedule_projected_service_account_token_refresh(
    request: ProjectedSaTokenRefreshRequest,
    pod: &Value,
    supervisor: Arc<TaskSupervisor>,
    cancel: CancellationToken,
) -> Result<()> {
    let Some(delay) = next_refresh_delay_for_pod(pod) else {
        return Ok(());
    };
    schedule_projected_service_account_token_refresh_after(request, supervisor, cancel, delay).await
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use anyhow::Result;
    use async_trait::async_trait;
    use serde_json::{Value, json};

    use crate::datastore::Resource;
    use crate::kubelet::pod_runtime::service::PodRuntimeKey;
    use crate::kubelet::volume_sources::VolumeSourceReader;

    fn resource(
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
    ) -> Resource {
        Resource {
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

    struct RefreshSourceReader {
        pod: Resource,
        service_account: Resource,
        node: Resource,
        token_requests:
            Mutex<Vec<crate::kubelet::volume_sources::ProjectedServiceAccountTokenRequest>>,
    }

    #[async_trait]
    impl VolumeSourceReader for RefreshSourceReader {
        async fn config_map(&self, _namespace: &str, _name: &str) -> Result<Option<Resource>> {
            Ok(None)
        }

        async fn secret(&self, _namespace: &str, _name: &str) -> Result<Option<Resource>> {
            Ok(None)
        }

        async fn service_account(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
            Ok(
                (self.service_account.namespace.as_deref() == Some(namespace)
                    && self.service_account.name == name)
                    .then(|| self.service_account.clone()),
            )
        }

        async fn pod(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
            Ok(
                (self.pod.namespace.as_deref() == Some(namespace) && self.pod.name == name)
                    .then(|| self.pod.clone()),
            )
        }

        async fn node(&self, name: &str) -> Result<Option<Resource>> {
            Ok((self.node.name == name).then(|| self.node.clone()))
        }

        async fn persistent_volume_claim(
            &self,
            _namespace: &str,
            _name: &str,
        ) -> Result<Option<Resource>> {
            Ok(None)
        }

        async fn persistent_volume(&self, _name: &str) -> Result<Option<Resource>> {
            Ok(None)
        }

        async fn projected_service_account_token(
            &self,
            request: crate::kubelet::volume_sources::ProjectedServiceAccountTokenRequest,
        ) -> Result<crate::kubelet::volume_sources::ProjectedServiceAccountToken> {
            self.token_requests.lock().unwrap().push(request);
            Ok(
                crate::kubelet::volume_sources::ProjectedServiceAccountToken {
                    token: "refreshed-from-leader".to_string(),
                },
            )
        }
    }

    #[test]
    fn projected_sa_token_refresh_delay_uses_requested_expiration_seconds() {
        assert_eq!(
            super::refresh_delay_for_expiration_seconds(3607),
            std::time::Duration::from_secs(2885)
        );
        assert_eq!(
            super::refresh_delay_for_expiration_seconds(600),
            std::time::Duration::from_secs(480)
        );
        assert_eq!(
            super::refresh_delay_for_expiration_seconds(7 * 24 * 60 * 60),
            std::time::Duration::from_secs(24 * 60 * 60)
        );
    }

    #[tokio::test]
    async fn refresh_once_rewrites_projected_serviceaccount_token_file() {
        let temp = tempfile::tempdir().expect("tempdir");
        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "kube-system",
                "name": "coredns",
                "uid": "pod-uid"
            },
            "spec": {
                "serviceAccountName": "coredns",
                "nodeName": "node-a",
                "volumes": [{
                    "name": "kube-api-access-x",
                    "projected": {
                        "defaultMode": 420,
                        "sources": [{
                            "serviceAccountToken": {
                                "path": "token",
                                "audience": "https://kubernetes.default.svc.cluster.local",
                                "expirationSeconds": 7200
                            }
                        }]
                    }
                }]
            },
            "status": {"phase": "Running"}
        });
        let sources = Arc::new(RefreshSourceReader {
            pod: resource("v1", "Pod", Some("kube-system"), "coredns", pod),
            service_account: resource(
                "v1",
                "ServiceAccount",
                Some("kube-system"),
                "coredns",
                json!({
                    "metadata": {
                        "namespace": "kube-system",
                        "name": "coredns",
                        "uid": "sa-uid"
                    }
                }),
            ),
            node: resource(
                "v1",
                "Node",
                None,
                "node-a",
                json!({"metadata": {"name": "node-a", "uid": "node-uid"}}),
            ),
            token_requests: Mutex::new(Vec::new()),
        });

        let volumes_root = temp.path().join("pods");
        let token_dir = volumes_root
            .join("kube-system_coredns")
            .join("volumes/projected/kube-api-access-x");
        std::fs::create_dir_all(&token_dir).expect("create projected dir");
        std::fs::write(token_dir.join("token"), "expired").expect("write old token");

        let outcome = super::refresh_projected_service_account_tokens_once(
            super::ProjectedSaTokenRefreshRequest {
                sources: sources.clone(),
                volumes_root: volumes_root.to_string_lossy().into_owned(),
                key: PodRuntimeKey::new("kube-system", "coredns", "pod-uid"),
            },
        )
        .await
        .expect("refresh must succeed");

        assert_eq!(
            outcome,
            super::ProjectedSaTokenRefreshOutcome::Continue {
                next_delay: std::time::Duration::from_secs(5760)
            }
        );
        let token = std::fs::read_to_string(token_dir.join("token")).expect("read refreshed token");
        assert_eq!(token, "refreshed-from-leader");
        let requests = sources.token_requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].namespace, "kube-system");
        assert_eq!(requests[0].service_account_name, "coredns");
        assert_eq!(
            requests[0].audiences,
            vec!["https://kubernetes.default.svc.cluster.local".to_string()]
        );
        assert_eq!(requests[0].expiration_seconds, 7200);
        assert_eq!(requests[0].bound_pod_name.as_deref(), Some("coredns"));
        assert_eq!(requests[0].bound_pod_uid.as_deref(), Some("pod-uid"));
        assert_eq!(requests[0].bound_node_name.as_deref(), Some("node-a"));
        assert_eq!(requests[0].bound_node_uid.as_deref(), Some("node-uid"));
    }
}
