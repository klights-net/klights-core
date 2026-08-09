//! Multi-node runtime traits for Pod cluster-level operations.
//!
//! These traits are separated from `PodRuntimeService` because they represent
//! cluster-level concerns (node ownership, cross-node status forwarding,
//! delivery) that are orthogonal to the single-node CRI/CNI/volume
//! operations in the core runtime service.

use std::sync::Arc;

use klights_kubelet::pod_repository::PodStatusWriter;
use klights_kubelet::pod_repository::{PodStatusUpdate, RuntimeReconcileStatus};
use klights_pod_api::{PodGetRequest, PodQuery};

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
    pod_query: &dyn PodQuery,
    pod_status: &dyn PodStatusWriter,
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
        return pod_status
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

    let live = pod_query
        .get_pod(PodGetRequest::try_by_identity(
            klights_types::PodIdentity::new(&key.namespace, &key.name, &key.uid),
        )?)
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
    pod_status
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
/// The view is role-agnostic: it routes Pod cluster operations through
/// whatever focused query/status capabilities it is handed at construction.
/// The leader is wired with the cluster-datastore repository (writes land
/// locally); a worker is wired with the worker-safe repository that forwards
/// to the leader. Because the role difference lives in the repository, the
/// leader's kubelet uses this exact same path as a normal worker — there is
/// no leader-specific runtime view or status bypass.
///
/// The constructor accepts the two focused trait objects directly. Every
/// stored field and every method body dispatches exclusively through those
/// capabilities; there is no aggregate accessor or role-specific bypass.
pub struct RepositoryClusterRuntimeView {
    pod_query: Arc<dyn PodQuery>,
    pod_status: Arc<dyn PodStatusWriter>,
}

impl RepositoryClusterRuntimeView {
    pub fn new(
        pod_query: Arc<dyn klights_pod_api::PodQuery>,
        pod_status: Arc<dyn klights_kubelet::pod_repository::status::PodStatusWriter>,
    ) -> Self {
        Self {
            pod_query,
            pod_status,
        }
    }
}

#[async_trait::async_trait]
impl ClusterRuntimeView for RepositoryClusterRuntimeView {
    async fn get_fresh_pod(
        &self,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<klights_cluster_core::Resource>> {
        self.pod_query
            .get_pod(PodGetRequest::try_by_name(namespace, name)?)
            .await
            .map_err(|e| anyhow::anyhow!("{:#}", e))
    }

    async fn forward_pod_status(
        &self,
        key: &crate::kubelet::pod_runtime::service::PodRuntimeKey,
        status: serde_json::Value,
    ) -> anyhow::Result<klights_cluster_core::Resource> {
        apply_forwarded_status(
            self.pod_query.as_ref(),
            self.pod_status.as_ref(),
            key,
            status,
        )
        .await
    }
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::kubelet::pod_runtime::service::PodRuntimeKey;

    struct PodClusterTestPorts {
        pod_query: Arc<dyn klights_pod_api::PodQuery>,
        pod_status_writer: Arc<dyn klights_kubelet::pod_repository::status::PodStatusWriter>,
        test_api: Option<Arc<dyn klights_pod_api::PodApiMutation>>,
    }

    async fn build_repo() -> PodClusterTestPorts {
        let (_ds, db) = crate::datastore::sqlite::Datastore::new_in_memory_with_handle().await;
        let (
            pod_query,
            _pod_snapshot,
            _pod_update,
            pod_status_writer,
            _pod_workqueue,
            _pod_network_assignment,
            _pod_host_ip,
            _background,
            _deletion_finalizer,
            _dirty_counter,
            _mutation_reconcile,
            _gc_delete,
            _eviction_admission,
            _namespace_bootstrap,
            _namespace_termination_queue,
            _pod_api,
            _pod_subresource,
            _pod_scheduling,
            _watch_source,
            _bound_finalization,
            _deferred_runtime,
            test_api,
            _test_subresource,
        ) = crate::pod_repository_composition::build_pod_repository_parts(
            crate::pod_repository_composition::PodRepositoryBuildConfig {
                db: db.clone(),
                pod_workqueue_store: None,
                supervisor: Arc::new(klights_supervisor::TaskSupervisor::new(
                    klights_supervisor::TaskCategoryConfig::default(),
                )),
                side_effects: Arc::new(klights_controllers::side_effects::SideEffectRegistry::new()),
                metrics: klights_controllers::side_effects::SideEffectMetrics::new(),
                pod_network_cache: crate::pod_repository_composition::empty_test_pod_network_cache(),
                assignment_waiter: crate::pod_repository_composition::test_assignment_bus(),
                scheduling_mode: crate::pod_repository_composition::PodSchedulingMode::InlineSingleNode,
                outbox: None,
                cluster_api: None,
                remote_delivery_required: false,
                controller_identity: crate::bootstrap::controller_adapters::system_identity_adapter::deterministic_controller_identity(),
                scheduler_bind_gate: None,
            },
            None,
        );
        PodClusterTestPorts {
            pod_query,
            pod_status_writer,
            test_api,
        }
    }

    async fn test_create_pod(
        parts: &PodClusterTestPorts,
        namespace: &str,
        name: &str,
        _node_name: &str,
        body: serde_json::Value,
    ) -> anyhow::Result<klights_cluster_core::Resource> {
        let created = parts
            .test_api
            .as_ref()
            .expect("runtime fixture requires the root Pod API test port")
            .create_pod(klights_pod_api::PodApiCreateRequest {
                namespace: namespace.to_string(),
                body,
                dry_run: false,
            })
            .await?;
        created
            .resource
            .ok_or_else(|| anyhow::anyhow!("test Pod {namespace}/{name} create returned dry-run"))
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
        let created = test_create_pod(&repo, "default", "init-forwarded", "worker-1", pod)
            .await
            .unwrap();
        let key = PodRuntimeKey::new("default", "init-forwarded", &created.uid);

        apply_forwarded_status(
            repo.pod_query.as_ref(),
            repo.pod_status_writer.as_ref(),
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

        let stored =
            repo.pod_query
                .get_pod(
                    klights_pod_api::PodGetRequest::try_by_identity(
                        klights_types::PodIdentity::new("default", "init-forwarded", &created.uid),
                    )
                    .unwrap(),
                )
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
        let created = test_create_pod(&repo, "default", "init-retry-forwarded", "worker-1", pod)
            .await
            .unwrap();
        let key = PodRuntimeKey::new("default", "init-retry-forwarded", &created.uid);
        apply_forwarded_status(
            repo.pod_query.as_ref(),
            repo.pod_status_writer.as_ref(),
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
            repo.pod_query.as_ref(),
            repo.pod_status_writer.as_ref(),
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
            .pod_query
            .get_pod(
                klights_pod_api::PodGetRequest::try_by_identity(klights_types::PodIdentity::new(
                    "default",
                    "init-retry-forwarded",
                    &created.uid,
                ))
                .unwrap(),
            )
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
        let created = test_create_pod(&repo, "default", "split-init-forwarded", "worker-1", pod)
            .await
            .unwrap();
        let key = PodRuntimeKey::new("default", "split-init-forwarded", &created.uid);

        apply_forwarded_status(
            repo.pod_query.as_ref(),
            repo.pod_status_writer.as_ref(),
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
            repo.pod_query.as_ref(),
            repo.pod_status_writer.as_ref(),
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
            .pod_query
            .get_pod(
                klights_pod_api::PodGetRequest::try_by_identity(klights_types::PodIdentity::new(
                    "default",
                    "split-init-forwarded",
                    &created.uid,
                ))
                .unwrap(),
            )
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

#[cfg(test)]
/// Fake node implementing `NodeRuntimeView` for multi-node tests.
pub(crate) struct FakeNode {
    node_name: String,
}

#[cfg(test)]
impl FakeNode {
    pub(crate) fn new(node_name: &str) -> Self {
        Self {
            node_name: node_name.to_string(),
        }
    }
}

#[cfg(test)]
impl NodeRuntimeView for FakeNode {
    fn node_name(&self) -> &str {
        &self.node_name
    }

    fn owns_pod_runtime(&self, pod: &serde_json::Value) -> bool {
        pod.pointer("/spec/nodeName")
            .and_then(|v| v.as_str())
            .is_some_and(|n| n == self.node_name)
    }
}

// --- FakeCluster ---

/// Records forwarded status updates for multi-node tests.
#[cfg(test)]
type StatusForward = (
    crate::kubelet::pod_runtime::service::PodRuntimeKey,
    serde_json::Value,
);
/// Fake cluster implementing `ClusterRuntimeView` for multi-node tests.
#[cfg(test)]
pub(crate) struct FakeCluster {
    fresh_pods:
        std::sync::Mutex<std::collections::HashMap<(String, String), crate::datastore::Resource>>,
    status_forwards: std::sync::Mutex<Vec<StatusForward>>,
}

#[cfg(test)]
impl Default for FakeCluster {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
impl FakeCluster {
    pub(crate) fn new() -> Self {
        Self {
            fresh_pods: std::sync::Mutex::new(std::collections::HashMap::new()),
            status_forwards: std::sync::Mutex::new(Vec::new()),
        }
    }

    pub(crate) fn recorded_status_forwards(&self) -> Vec<StatusForward> {
        self.status_forwards.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
#[cfg(test)]
impl ClusterRuntimeView for FakeCluster {
    async fn get_fresh_pod(
        &self,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<crate::datastore::Resource>> {
        Ok(self
            .fresh_pods
            .lock()
            .unwrap()
            .get(&(namespace.to_string(), name.to_string()))
            .cloned())
    }

    async fn forward_pod_status(
        &self,
        key: &crate::kubelet::pod_runtime::service::PodRuntimeKey,
        status: serde_json::Value,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.status_forwards
            .lock()
            .unwrap()
            .push((key.clone(), status));
        Ok(crate::datastore::Resource {
            id: 0,
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some(key.namespace.clone()),
            name: key.name.clone(),
            uid: key.uid.clone(),
            data: std::sync::Arc::new(serde_json::json!({
                "metadata": {
                    "namespace": key.namespace,
                    "name": key.name,
                    "uid": key.uid,
                },
            })),
            resource_version: 1,
        })
    }
}
