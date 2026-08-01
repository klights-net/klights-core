//! Multi-node runtime traits for Pod cluster-level operations.
//!
//! These traits are separated from `PodRuntimeService` because they represent
//! cluster-level concerns (node ownership, cross-node status forwarding,
//! delivery) that are orthogonal to the single-node CRI/CNI/volume
//! operations in the core runtime service.

use std::sync::Arc;

use crate::kubelet::pod_repository::{
    PodReader, PodRepository, PodStatusUpdate, PodStatusWriter, RuntimeReconcileStatus,
};

/// View of the local node's identity and Pod ownership.
pub trait NodeRuntimeView: Send + Sync {
    fn node_name(&self) -> &str;
    fn owns_pod_runtime(&self, pod: &serde_json::Value) -> bool;
}

/// Cross-node cluster operations for Pod runtime.
#[async_trait::async_trait]
pub trait ClusterRuntimeView: Send + Sync {
    /// Fetch the latest Pod state from the authoritative source (leader).
    /// This is the only name-keyed method — it is the pre-UID lookup used
    /// by leader-side finalization.
    async fn get_fresh_pod(
        &self,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<klights_cluster_core::Resource>>;

    /// Forward a Pod status update to the owning node.
    async fn forward_pod_status(
        &self,
        key: &crate::kubelet::pod_runtime::service::PodRuntimeKey,
        status: serde_json::Value,
    ) -> anyhow::Result<klights_cluster_core::Resource>;
}

// --- Production adapters ---

fn status_array(status: &serde_json::Value, field: &str) -> Vec<serde_json::Value> {
    status
        .get(field)
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default()
}

fn optional_status_array(
    status: &serde_json::Value,
    field: &str,
) -> Option<Vec<serde_json::Value>> {
    status.get(field).and_then(|v| v.as_array()).cloned()
}

fn optional_status_string(status: &serde_json::Value, field: &str) -> Option<String> {
    status
        .get(field)
        .and_then(|v| v.as_str())
        .map(str::to_string)
}

fn live_status_string(
    resource: Option<&klights_cluster_core::Resource>,
    field: &str,
) -> Option<String> {
    resource
        .and_then(|resource| resource.data.pointer("/status"))
        .and_then(|status| status.get(field))
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

async fn apply_forwarded_status(
    repository: &PodRepository,
    key: &crate::kubelet::pod_runtime::service::PodRuntimeKey,
    status: serde_json::Value,
) -> anyhow::Result<klights_cluster_core::Resource> {
    let phase = status
        .get("phase")
        .and_then(|v| v.as_str())
        .unwrap_or("Pending")
        .to_string();
    let container_statuses = status_array(&status, "containerStatuses");
    let init_container_statuses = optional_status_array(&status, "initContainerStatuses");

    if status.get("podIP").is_none()
        && status.get("hostIP").is_none()
        && init_container_statuses.is_none()
    {
        return repository
            .apply_runtime_reconcile_status_for_uid(
                &key.namespace,
                &key.name,
                &key.uid,
                RuntimeReconcileStatus {
                    phase,
                    container_statuses,
                },
                None,
            )
            .await
            .map_err(|e| anyhow::anyhow!("{:#}", e));
    }

    let live = repository
        .get_pod_for_uid(&key.namespace, &key.name, &key.uid)
        .await
        .map_err(|e| anyhow::anyhow!("{:#}", e))?;
    let status_update = PodStatusUpdate {
        phase,
        pod_ip: optional_status_string(&status, "podIP")
            .or_else(|| live_status_string(live.as_ref(), "podIP"))
            .unwrap_or_default(),
        host_ip: optional_status_string(&status, "hostIP")
            .or_else(|| live_status_string(live.as_ref(), "hostIP"))
            .unwrap_or_default(),
        container_statuses,
        init_container_statuses,
        qos_class: None,
    };
    repository
        .set_pod_status_for_uid(&key.namespace, &key.name, &key.uid, status_update, None)
        .await
        .map_err(|e| anyhow::anyhow!("{:#}", e))
}

/// Local node identity and Pod-ownership view.
pub struct LocalNodeRuntimeView {
    node_name: String,
}

impl LocalNodeRuntimeView {
    pub fn new(node_name: String) -> Self {
        Self { node_name }
    }
}

impl NodeRuntimeView for LocalNodeRuntimeView {
    fn node_name(&self) -> &str {
        &self.node_name
    }

    fn owns_pod_runtime(&self, pod: &serde_json::Value) -> bool {
        pod.pointer("/spec/nodeName")
            .and_then(|v| v.as_str())
            .is_some_and(|n| n == self.node_name)
    }
}

/// Cluster runtime view shared by every node role (leader, worker, replica).
///
/// The view is role-agnostic: it routes Pod cluster operations through whatever
/// `PodRepository` it is handed. The leader is wired with the cluster-datastore
/// repository (writes land locally); a worker is wired with the worker-safe
/// repository that forwards to the leader. Because the role difference lives in
/// the repository, the leader's kubelet uses this exact same path as a normal
/// worker — there is no leader-specific runtime view or status bypass.
pub struct RepositoryClusterRuntimeView {
    repository: Arc<PodRepository>,
}

impl RepositoryClusterRuntimeView {
    pub fn new(repository: Arc<PodRepository>) -> Self {
        Self { repository }
    }
}

#[async_trait::async_trait]
impl ClusterRuntimeView for RepositoryClusterRuntimeView {
    async fn get_fresh_pod(
        &self,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<klights_cluster_core::Resource>> {
        self.repository
            .get_pod(namespace, name)
            .await
            .map_err(|e| anyhow::anyhow!("{:#}", e))
    }

    async fn forward_pod_status(
        &self,
        key: &crate::kubelet::pod_runtime::service::PodRuntimeKey,
        status: serde_json::Value,
    ) -> anyhow::Result<klights_cluster_core::Resource> {
        apply_forwarded_status(self.repository.as_ref(), key, status).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;

    use super::*;
    use crate::kubelet::pod_repository::{PodObjectWriter, PodReader};
    use crate::kubelet::pod_runtime::service::PodRuntimeKey;

    async fn build_repo() -> PodRepository {
        let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let metrics = klights_controllers::side_effects::SideEffectMetrics::new();
        let side_effects = Arc::new(klights_controllers::side_effects::SideEffectRegistry::new());
        PodRepository::new(db, supervisor, side_effects, metrics)
    }

    #[tokio::test]
    async fn forwarded_full_status_preserves_completed_init_container_statuses() {
        let repo = build_repo().await;
        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"namespace": "default", "name": "init-forwarded"},
            "spec": {
                "restartPolicy": "Never",
                "initContainers": [
                    {"name": "init1", "image": "busybox"},
                    {"name": "init2", "image": "busybox"}
                ],
                "containers": [{"name": "run1", "image": "busybox"}]
            },
            "status": {
                "phase": "Pending",
                "conditions": [
                    {"type": "Initialized", "status": "False", "reason": "ContainersNotInitialized"}
                ]
            }
        });
        let created = repo
            .create_controller_pod("default", "init-forwarded", "worker-1", pod)
            .await
            .unwrap();
        let key = PodRuntimeKey::new("default", "init-forwarded", &created.uid);

        apply_forwarded_status(
            &repo,
            &key,
            json!({
                "phase": "Succeeded",
                "podIP": "10.50.0.17",
                "hostIP": "192.0.2.10",
                "initContainerStatuses": [
                    {
                        "name": "init1",
                        "ready": true,
                        "restartCount": 0,
                        "state": {"terminated": {"exitCode": 0, "reason": "Completed"}}
                    },
                    {
                        "name": "init2",
                        "ready": true,
                        "restartCount": 0,
                        "state": {"terminated": {"exitCode": 0, "reason": "Completed"}}
                    }
                ],
                "containerStatuses": [
                    {
                        "name": "run1",
                        "ready": false,
                        "restartCount": 0,
                        "state": {"terminated": {"exitCode": 0, "reason": "Completed"}}
                    }
                ]
            }),
        )
        .await
        .unwrap();

        let stored = repo
            .get_pod_for_uid("default", "init-forwarded", &created.uid)
            .await
            .unwrap()
            .unwrap();
        let init_statuses = stored
            .data
            .pointer("/status/initContainerStatuses")
            .and_then(|value| value.as_array())
            .expect("forwarded full status must keep initContainerStatuses");
        assert_eq!(init_statuses.len(), 2);
        let initialized = stored
            .data
            .pointer("/status/conditions")
            .and_then(|value| value.as_array())
            .and_then(|conditions| {
                conditions.iter().find(|condition| {
                    condition.pointer("/type").and_then(|value| value.as_str())
                        == Some("Initialized")
                })
            })
            .expect("Initialized condition must exist");
        assert_eq!(
            initialized
                .pointer("/status")
                .and_then(|value| value.as_str()),
            Some("True"),
            "completed forwarded init statuses must make Initialized=True"
        );
    }

    #[tokio::test]
    async fn forwarded_init_status_without_network_fields_preserves_init_statuses() {
        let repo = build_repo().await;
        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"namespace": "default", "name": "init-retry-forwarded"},
            "spec": {
                "restartPolicy": "Always",
                "initContainers": [
                    {"name": "init1", "image": "busybox"},
                    {"name": "init2", "image": "busybox"}
                ],
                "containers": [{"name": "run1", "image": "busybox"}]
            },
            "status": {
                "phase": "Pending",
                "podIP": "10.50.0.17",
                "podIPs": [{"ip": "10.50.0.17"}],
                "hostIP": "192.0.2.10",
                "hostIPs": [{"ip": "192.0.2.10"}],
                "conditions": [
                    {"type": "Initialized", "status": "False", "reason": "ContainersNotInitialized"}
                ],
                "containerStatuses": []
            }
        });
        let created = repo
            .create_controller_pod("default", "init-retry-forwarded", "worker-1", pod)
            .await
            .unwrap();
        let key = PodRuntimeKey::new("default", "init-retry-forwarded", &created.uid);
        apply_forwarded_status(
            &repo,
            &key,
            json!({
                "phase": "Pending",
                "podIP": "10.50.0.17",
                "hostIP": "192.0.2.10",
                "containerStatuses": []
            }),
        )
        .await
        .unwrap();

        apply_forwarded_status(
            &repo,
            &key,
            json!({
                "phase": "Pending",
                "initContainerStatuses": [
                    {
                        "name": "init1",
                        "ready": false,
                        "restartCount": 1,
                        "state": {"waiting": {"reason": "PodInitializing"}},
                        "lastState": {"terminated": {"exitCode": 1, "reason": "Error"}}
                    },
                    {
                        "name": "init2",
                        "ready": false,
                        "restartCount": 0,
                        "state": {"waiting": {"reason": "PodInitializing"}}
                    }
                ],
                "containerStatuses": [
                    {
                        "name": "run1",
                        "ready": false,
                        "restartCount": 0,
                        "state": {"waiting": {"reason": "PodInitializing"}}
                    }
                ]
            }),
        )
        .await
        .unwrap();

        let stored = repo
            .get_pod_for_uid("default", "init-retry-forwarded", &created.uid)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            stored
                .data
                .pointer("/status/podIP")
                .and_then(|value| value.as_str()),
            Some("10.50.0.17"),
            "forwarded retry status without network fields must not clear podIP"
        );
        assert_eq!(
            stored
                .data
                .pointer("/status/initContainerStatuses/0/restartCount")
                .and_then(|value| value.as_i64()),
            Some(1),
            "forwarded init retry status must reach the leader"
        );
    }

    #[tokio::test]
    async fn forwarded_network_status_without_init_statuses_preserves_existing_init_state() {
        let repo = build_repo().await;
        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"namespace": "default", "name": "split-init-forwarded"},
            "spec": {
                "restartPolicy": "Always",
                "initContainers": [
                    {"name": "init1", "image": "busybox"},
                    {"name": "init2", "image": "busybox"}
                ],
                "containers": [{"name": "run1", "image": "busybox"}]
            },
            "status": {
                "phase": "Pending",
                "conditions": [
                    {"type": "Initialized", "status": "False", "reason": "ContainersNotInitialized"}
                ],
                "containerStatuses": []
            }
        });
        let created = repo
            .create_controller_pod("default", "split-init-forwarded", "worker-1", pod)
            .await
            .unwrap();
        let key = PodRuntimeKey::new("default", "split-init-forwarded", &created.uid);

        apply_forwarded_status(
            &repo,
            &key,
            json!({
                "phase": "Pending",
                "initContainerStatuses": [
                    {
                        "name": "init1",
                        "ready": false,
                        "restartCount": 1,
                        "state": {"waiting": {"reason": "PodInitializing"}},
                        "lastState": {"terminated": {"exitCode": 1, "reason": "Error"}}
                    },
                    {
                        "name": "init2",
                        "ready": false,
                        "restartCount": 0,
                        "state": {"waiting": {"reason": "PodInitializing"}}
                    }
                ],
                "containerStatuses": [
                    {
                        "name": "run1",
                        "ready": false,
                        "restartCount": 0,
                        "state": {"waiting": {"reason": "PodInitializing"}}
                    }
                ]
            }),
        )
        .await
        .unwrap();

        apply_forwarded_status(
            &repo,
            &key,
            json!({
                "phase": "Pending",
                "podIP": "10.50.0.18",
                "hostIP": "192.0.2.11",
                "containerStatuses": [
                    {
                        "name": "run1",
                        "ready": false,
                        "restartCount": 0,
                        "state": {"waiting": {"reason": "PodInitializing"}}
                    }
                ]
            }),
        )
        .await
        .unwrap();

        let stored = repo
            .get_pod_for_uid("default", "split-init-forwarded", &created.uid)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            stored
                .data
                .pointer("/status/initContainerStatuses/0/restartCount")
                .and_then(|value| value.as_i64()),
            Some(1),
            "network-bearing forwarded status must not clear prior init retry state"
        );
        assert_eq!(
            stored
                .data
                .pointer("/status/podIP")
                .and_then(|value| value.as_str()),
            Some("10.50.0.18")
        );
        let initialized = stored
            .data
            .pointer("/status/conditions")
            .and_then(|value| value.as_array())
            .and_then(|conditions| {
                conditions.iter().find(|condition| {
                    condition.pointer("/type").and_then(|value| value.as_str())
                        == Some("Initialized")
                })
            })
            .expect("Initialized condition must exist");
        assert_eq!(
            initialized
                .pointer("/status")
                .and_then(|value| value.as_str()),
            Some("False"),
            "preserved retrying init status must keep Initialized=False"
        );
    }
}
