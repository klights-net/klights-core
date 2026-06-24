use crate::kubelet::lifecycle::LifecycleCommand;
use crate::kubelet::pod_lifecycle_core::message::PodLifecycleKey;
use crate::kubelet::pod_lifecycle_router::LifecycleReplyHandle;
pub use crate::kubelet::pod_runtime::service_dependencies::RealPodRuntimeServiceDependencies;
pub use crate::kubelet::pod_runtime::slot_admission::PodSlotAdmissionRequest;
use tokio_util::sync::CancellationToken;

/// UID-bearing identity key for runtime operations.
/// Every mutating runtime call below the API admission layer must carry
/// one of these; name-only lookup is forbidden.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct PodRuntimeKey {
    pub namespace: String,
    pub name: String,
    pub uid: String,
}

impl PodRuntimeKey {
    pub fn new(namespace: &str, name: &str, uid: &str) -> Self {
        Self {
            namespace: namespace.to_string(),
            name: name.to_string(),
            uid: uid.to_string(),
        }
    }
}

impl From<&PodLifecycleKey> for PodRuntimeKey {
    fn from(key: &PodLifecycleKey) -> Self {
        Self {
            namespace: key.namespace.clone(),
            name: key.name.clone(),
            uid: key.uid.clone(),
        }
    }
}

fn append_service_envs(
    config: &mut k8s_cri::v1::ContainerConfig,
    service_envs: &[(String, String)],
) {
    for (key, value) in service_envs {
        config.envs.push(k8s_cri::v1::KeyValue {
            key: key.clone(),
            value: value.clone(),
        });
    }
}

/// Outcome of a pod start attempt through the runtime service.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PodStartResult {
    /// Pod started successfully. `sandbox_id` is the recorded CRI sandbox ID
    /// if the runtime recorded one; `None` means actor state should already
    /// have it (e.g. from a previous start_pod call).
    Started { sandbox_id: Option<String> },
    /// Pod start was cancelled before completion.
    Cancelled,
    /// Pod start failed with a retryable error (e.g. image pull, CRI
    /// unavailable). The actor may retry after a backoff.
    Failed(String),
    /// Pod start failed with a terminal error (e.g. InvalidPodSpec,
    /// InitContainerFailed with restartPolicy=Never). The actor must
    /// not retry and should transition the pod to Failed phase.
    Terminal(String),
}

/// Outcome of actor-owned pod deletion finalization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PodDeletionFinalizeResult {
    /// Pod row was deleted or was already gone.
    DeletedOrAlreadyGone,
    /// Finalizers are still pending; deletion was deferred.
    FinalizersPending,
}

/// Outcome of startup finalization. `Unconfirmed` keeps the actor's
/// startup-finalized bit false so the next Running+podIP watch echo can retry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PodFinalizeStartupResult {
    Confirmed { sandbox_id: String },
    Unconfirmed,
}

/// Typed error raised by runtime cleanup paths (e.g. [`PodRuntimeService::stop_pod`])
/// when the local node does not own a Pod's runtime.
///
/// This MUST NOT be classified as a retryable kubelet-lifecycle failure: the
/// local node has no CRI/CNI/volume state for a Pod it does not own, so retrying
/// `StopPod` locally can never succeed and would spin the lifecycle actor
/// forever. Row cleanup is owned by `PodStore::delete_unscheduled_with_uid`
/// (unscheduled Pods, HR#11 exception) or the owning node's lifecycle actor
/// (node-assigned Pods).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodOwnershipError {
    /// Node performing the (refused) cleanup.
    pub local_node: String,
    /// Node that owns the Pod, or `None` when `spec.nodeName` is absent (the
    /// Pod was never scheduled / picked up by any kubelet).
    pub target_node: Option<String>,
}

impl PodOwnershipError {
    /// Build the ownership error from the local node name and the Pod's
    /// `spec.nodeName` value (parsed from the raw Pod JSON).
    pub fn from_pod_node_name(local_node: impl Into<String>, pod: &serde_json::Value) -> Self {
        let target_node = pod
            .pointer("/spec/nodeName")
            .and_then(|value| value.as_str())
            .map(|s| s.to_string());
        Self {
            local_node: local_node.into(),
            target_node,
        }
    }
}

impl std::fmt::Display for PodOwnershipError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.target_node {
            Some(target) => write!(
                f,
                "pod runtime is owned by node {target}, not by local node {}",
                self.local_node
            ),
            None => write!(
                f,
                "pod has no assigned node; local node {} cannot own runtime cleanup",
                self.local_node
            ),
        }
    }
}

impl std::error::Error for PodOwnershipError {}

/// Hint carried from a CRI container event into deferred runtime reconcile.
///
/// Extracted into `reconcile_hint` to keep this hub under its size cap; the
/// type is re-exported here so the public path
/// `crate::kubelet::pod_runtime::service::RuntimeReconcileHint` stays stable.
pub use crate::kubelet::pod_runtime::reconcile_hint::RuntimeReconcileHint;

/// Backend-neutral lifecycle runtime trait.
/// Every lifecycle operation below `PodWorkExecutor` takes `PodRuntimeKey`
/// or another UID-bearing command object.
#[async_trait::async_trait]
pub trait PodRuntimeService: Send + Sync {
    async fn start_pod(
        &self,
        key: PodRuntimeKey,
        pod: Option<serde_json::Value>,
        cancel: CancellationToken,
    ) -> anyhow::Result<PodStartResult>;

    async fn stop_pod(
        &self,
        key: PodRuntimeKey,
        pod: Option<serde_json::Value>,
        // Sandbox ID to clean up. `None` means resolve via store → annotation → CRI.
        sandbox_id: Option<String>,
    ) -> anyhow::Result<()>;

    async fn finalize_startup(
        &self,
        key: PodRuntimeKey,
        pod: Option<serde_json::Value>,
        sandbox_id_hint: Option<String>,
    ) -> anyhow::Result<PodFinalizeStartupResult>;

    async fn finalize_deletion(
        &self,
        key: PodRuntimeKey,
    ) -> anyhow::Result<PodDeletionFinalizeResult>;

    async fn reconcile_runtime(
        &self,
        key: PodRuntimeKey,
        hint: RuntimeReconcileHint,
    ) -> anyhow::Result<()>;

    async fn reconcile_cri_leftovers(&self, key: PodRuntimeKey) -> anyhow::Result<()>;

    async fn reconcile_ephemeral(
        &self,
        key: PodRuntimeKey,
        pod: Option<serde_json::Value>,
    ) -> anyhow::Result<()>;

    async fn handle_lifecycle_command(&self, command: LifecycleCommand) -> anyhow::Result<()>;

    async fn check_slot_admission(
        &self,
        request: PodSlotAdmissionRequest,
        reply_to: LifecycleReplyHandle,
        cancel: CancellationToken,
    ) -> anyhow::Result<()>;

    async fn schedule_retry(
        &self,
        key: PodRuntimeKey,
        delay: std::time::Duration,
        reply_to: LifecycleReplyHandle,
    ) -> anyhow::Result<()>;

    async fn schedule_start_pod_retry(
        &self,
        key: PodRuntimeKey,
        delay: std::time::Duration,
        error_message: String,
        attempt: u32,
        reply_to: LifecycleReplyHandle,
    ) -> anyhow::Result<()>;
}

// --- Runtime configuration ---

/// Scalar configuration for `RealPodRuntimeService`.
/// Contains only static per-node values; does not include port references.
#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    pub node_name: String,
    pub service_cidr: String,
    pub containerd_namespace: String,
}

// --- RealPodRuntimeService ---

use std::sync::Arc;

use crate::kubelet::pod_cluster_runtime::{ClusterRuntimeView, NodeRuntimeView};
use crate::kubelet::pod_runtime::cri::{
    ContainerRuntimeControl, ContainerRuntimeState, CriRuntime, CriRuntimeContainerEventKind,
    CriRuntimeContainerEventStream,
};
use crate::kubelet::pod_runtime::deletion_finalizer::PodDeletionFinalizer;
use crate::kubelet::pod_runtime::events::PodEventSink;
use crate::kubelet::pod_runtime::filesystem::PodFilesystem;
use crate::kubelet::pod_runtime::hooks::{HookOutcome, PodHookRuntime};
use crate::kubelet::pod_runtime::hostports::HostPortRuntime;
use crate::kubelet::pod_runtime::init_container_status::{
    InitContainerStop, build_completed_init_container_status, build_failed_init_container_statuses,
    build_init_failure_terminated_state, build_pod_start_failure_app_statuses,
    build_retrying_init_container_statuses, init_container_completed,
    init_container_stop_from_status, record_completed_init_container_status,
};
use crate::kubelet::pod_runtime::network::PodNetworkRuntime;
use crate::kubelet::pod_runtime::probes::{ProbeRuntime, StartupFinalizationAction};
use crate::kubelet::pod_runtime::repository::{LivePodUidCheck, PodRuntimeRepository};
use crate::kubelet::pod_runtime::status_emitter::PodStatusEmitter;
use crate::kubelet::pod_runtime::status_helpers::{
    replace_container_status, restart_last_state_from_reconciled_status,
    restarted_running_container_status, runtime_status_container_id,
};
use crate::kubelet::pod_runtime::store::{PodRuntimeStore, PodSlotAdmission};
use crate::kubelet::pod_runtime::volumes::PodVolumeRuntime;
use crate::kubelet::pod_sandbox_config::build_sandbox_config_with_dns_policy;
use crate::kubelet::pod_startup_error::PodStartupErrorKind;
use crate::kubelet::pod_status_builders::{
    build_initial_pending_status, build_pod_initializing_app_statuses,
};
use crate::kubelet::pod_termination::{
    find_pod_container_spec, get_termination_message_path, termination_message_policy,
};
use crate::task_supervisor::TaskSupervisor;

const INIT_CONTAINER_WAIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);
const INIT_CONTAINER_FAST_EXIT_RECHECK_DELAY: std::time::Duration =
    std::time::Duration::from_millis(50);

struct ContainerConfigBuildRequest<'a> {
    key: &'a PodRuntimeKey,
    pod: &'a serde_json::Value,
    container: &'a serde_json::Value,
    container_name: &'a str,
    kubernetes_service_ip: &'a str,
    volume_paths: &'a std::collections::HashMap<String, String>,
    ignore_mount_errors: bool,
}

#[derive(Clone, Debug)]
struct ReconcileContainerInfo {
    container_id: String,
    state: ContainerRuntimeState,
    exit_code: i32,
    started_at: i64,
    finished_at: i64,
    created_at: i64,
    image: String,
    image_ref: String,
    termination_message: String,
}

fn pod_status_container_name_by_id(
    pod: &serde_json::Value,
) -> std::collections::HashMap<String, String> {
    let mut name_by_id = std::collections::HashMap::new();
    let Some(statuses) = pod
        .pointer("/status/containerStatuses")
        .and_then(|v| v.as_array())
    else {
        return name_by_id;
    };

    for status in statuses {
        let id = status
            .get("containerID")
            .and_then(|id| id.as_str())
            .map(|id| id.strip_prefix("containerd://").unwrap_or(id).to_string());
        let name = status
            .get("name")
            .and_then(|name| name.as_str())
            .map(str::to_string);
        if let (Some(id), Some(name)) = (id, name) {
            name_by_id.insert(id, name);
        }
    }
    name_by_id
}

fn pod_status_container_id_by_name(
    pod: &serde_json::Value,
    container_name: &str,
) -> Option<String> {
    pod.pointer("/status/containerStatuses")
        .and_then(|v| v.as_array())
        .and_then(|statuses| {
            statuses.iter().find(|status| {
                status.get("name").and_then(|name| name.as_str()) == Some(container_name)
            })
        })
        .and_then(|status| status.get("containerID"))
        .and_then(|id| id.as_str())
        .map(|id| id.strip_prefix("containerd://").unwrap_or(id).to_string())
        .filter(|id| !id.is_empty())
}

fn pod_status_ip(pod: &serde_json::Value) -> &str {
    pod.pointer("/status/podIP")
        .and_then(|v| v.as_str())
        .or_else(|| {
            pod.pointer("/status/podIPs")
                .and_then(|v| v.as_array())
                .and_then(|ips| ips.first())
                .and_then(|entry| entry.get("ip"))
                .and_then(|v| v.as_str())
        })
        .unwrap_or("")
}

fn pod_status_host_ip(pod: &serde_json::Value) -> Option<&str> {
    pod.pointer("/status/hostIP")
        .and_then(|v| v.as_str())
        .or_else(|| {
            pod.pointer("/status/hostIPs")
                .and_then(|v| v.as_array())
                .and_then(|ips| ips.first())
                .and_then(|entry| entry.get("ip"))
                .and_then(|v| v.as_str())
        })
        .filter(|ip| !ip.trim().is_empty())
}

fn managed_hosts_file_path(
    containerd_namespace: &str,
    key: &PodRuntimeKey,
    pod: &serde_json::Value,
) -> Option<String> {
    if crate::kubelet::pod_hosts::is_host_network(pod) {
        return None;
    }

    Some(
        crate::paths::containerd_hosts_dir_path(containerd_namespace, &key.namespace, &key.name)
            .join("hosts")
            .to_string_lossy()
            .into_owned(),
    )
}

fn append_managed_hosts_mount(mounts: &mut Vec<k8s_cri::v1::Mount>, hosts_file_path: Option<&str>) {
    let Some(host_path) = hosts_file_path else {
        return;
    };
    if crate::kubelet::pod_hosts::container_has_etc_hosts_mount(mounts) {
        return;
    }

    mounts.push(k8s_cri::v1::Mount {
        container_path: "/etc/hosts".to_string(),
        host_path: host_path.to_string(),
        readonly: false,
        selinux_relabel: false,
        propagation: 0,
        gid_mappings: vec![],
        uid_mappings: vec![],
        image: None,
        recursive_read_only: false,
    });
}
fn cri_timestamp_from_ns(ns: i64) -> String {
    if ns <= 0 {
        return crate::utils::k8s_timestamp();
    }
    let secs = ns / 1_000_000_000;
    let sub_ns = (ns % 1_000_000_000) as u32;
    chrono::DateTime::from_timestamp(secs, sub_ns)
        .map(|dt| dt.format("%Y-%m-%dT%H:%M:%S.%fZ").to_string())
        .unwrap_or_else(crate::utils::k8s_timestamp)
}

struct EphemeralContainerStatusInput<'a> {
    container_name: &'a str,
    container_id: Option<&'a str>,
    state: i32,
    started_at_ns: i64,
    finished_at_ns: i64,
    exit_code: i32,
    image: &'a str,
    image_ref: &'a str,
}

fn build_ephemeral_container_status(input: EphemeralContainerStatusInput<'_>) -> serde_json::Value {
    let EphemeralContainerStatusInput {
        container_name,
        container_id,
        state,
        started_at_ns,
        finished_at_ns,
        exit_code,
        image,
        image_ref,
    } = input;
    let state_obj = match state {
        state if state == k8s_cri::v1::ContainerState::ContainerRunning as i32 => {
            serde_json::json!({
                "running": {
                    "startedAt": cri_timestamp_from_ns(started_at_ns)
                }
            })
        }
        state if state == k8s_cri::v1::ContainerState::ContainerExited as i32 => {
            serde_json::json!({
                "terminated": {
                    "exitCode": exit_code,
                    "reason": if exit_code == 0 { "Completed" } else { "Error" },
                    "startedAt": cri_timestamp_from_ns(started_at_ns),
                    "finishedAt": cri_timestamp_from_ns(finished_at_ns),
                }
            })
        }
        _ => serde_json::json!({
            "waiting": {
                "reason": "ContainerCreating"
            }
        }),
    };

    let mut status = serde_json::json!({
        "name": container_name,
        "state": state_obj,
        "ready": state == k8s_cri::v1::ContainerState::ContainerRunning as i32,
        "started": state == k8s_cri::v1::ContainerState::ContainerRunning as i32
            || state == k8s_cri::v1::ContainerState::ContainerExited as i32,
        "restartCount": 0,
        "image": image,
        "imageID": image_ref,
    });
    if let Some(id) = container_id {
        status["containerID"] = serde_json::json!(format!("containerd://{}", id));
    }
    status
}

/// Production `PodRuntimeService` orchestrating CRI, CNI, volumes,
/// filesystem, probes, hostports, events, and actor-owned deletion.
pub struct RealPodRuntimeService {
    // `pub(super)` fields are consumed by the orphan-stop helper in the sibling
    // `orphan_stop` module (extracted to keep this hub under its size cap).
    pub(super) cri: Arc<dyn CriRuntime>,
    pub(super) container_control: Arc<dyn ContainerRuntimeControl>,
    pub(super) network: Arc<dyn PodNetworkRuntime>,
    pub(super) store: Arc<dyn PodRuntimeStore>,
    pub(super) slot_admission: Arc<dyn PodSlotAdmission>,
    pub(super) repository: Arc<dyn PodRuntimeRepository>,
    pub(super) filesystem: Arc<dyn PodFilesystem>,
    volumes: Arc<dyn PodVolumeRuntime>,
    probes: Arc<dyn ProbeRuntime>,
    hostports: Arc<dyn HostPortRuntime>,
    events: Arc<dyn PodEventSink>,
    hooks: Arc<dyn PodHookRuntime>,
    env_source: Arc<dyn crate::kubelet::pod_env::EnvSourceReader>,
    finalizer: Arc<dyn PodDeletionFinalizer>,
    supervisor: Arc<TaskSupervisor>,
    config: RuntimeConfig,
    node_view: Arc<dyn NodeRuntimeView>,
    cluster_view: Arc<dyn ClusterRuntimeView>,
    pub(super) status_emitter: PodStatusEmitter,
}

impl RealPodRuntimeService {
    fn exceeded_active_deadline_seconds(pod: &serde_json::Value) -> Option<i64> {
        let deadline_secs = pod
            .pointer("/spec/activeDeadlineSeconds")
            .and_then(|v| v.as_i64())?;

        let start_ts = pod
            .pointer("/status/startTime")
            .and_then(|v| v.as_str())
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.timestamp())
            .or_else(|| {
                pod.pointer("/metadata/creationTimestamp")
                    .and_then(|v| v.as_str())
                    .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                    .map(|dt| dt.timestamp())
            })?;

        let now = chrono::Utc::now().timestamp();
        if now - start_ts >= deadline_secs {
            Some(deadline_secs)
        } else {
            None
        }
    }

    async fn enforce_active_deadline_if_exceeded(
        &self,
        key: &PodRuntimeKey,
        pod: &serde_json::Value,
        resource_version: i64,
    ) -> anyhow::Result<bool> {
        let Some(deadline_secs) = Self::exceeded_active_deadline_seconds(pod) else {
            return Ok(false);
        };

        tracing::info!(
            namespace = key.namespace,
            name = key.name,
            uid = key.uid,
            deadline_secs,
            "pod exceeded activeDeadlineSeconds, terminating containers"
        );

        if self.node_view.owns_pod_runtime(pod)
            && let Some(sandbox_id) = self.resolve_sandbox_id_for_stop(key, pod).await
            && let Ok(containers) = self
                .container_control
                .list_containers(Some(&sandbox_id))
                .await
        {
            for (container_id, _) in containers {
                let _ = self.cri.stop_container(&container_id, 0).await;
            }
        }

        let message = format!(
            "Pod was active on the node longer than the specified deadline ({}s)",
            deadline_secs
        );
        if let Err(e) = self
            .repository
            .set_deadline_exceeded_for_uid(
                &key.namespace,
                &key.name,
                &key.uid,
                message,
                Some(resource_version),
            )
            .await
        {
            tracing::warn!(
                namespace = key.namespace,
                name = key.name,
                uid = key.uid,
                "Failed to mark pod as DeadlineExceeded: {e:#}"
            );
        }

        Ok(true)
    }

    /// Write Pod status through the cluster boundary `ClusterRuntimeView`.
    /// On the leader this resolves to a local repository write; on a worker it
    /// forwards to the leader — a single status path for every node role.
    async fn write_pod_status(
        &self,
        key: &PodRuntimeKey,
        status: serde_json::Value,
    ) -> anyhow::Result<()> {
        let emitted = self
            .status_emitter
            .emit_if_changed(key, status, |status| async move {
                self.cluster_view.forward_pod_status(key, status).await?;
                Ok::<(), anyhow::Error>(())
            })
            .await?;
        if !emitted {
            tracing::debug!(
                target: "klights::pod_status",
                namespace = %key.namespace,
                pod = %key.name,
                uid = %key.uid,
                "pod status emit suppressed because actor memory cache already has identical status"
            );
        }
        Ok(())
    }

    async fn build_container_config_with_env(
        &self,
        request: ContainerConfigBuildRequest<'_>,
    ) -> anyhow::Result<k8s_cri::v1::ContainerConfig> {
        let resolved_env_from = crate::kubelet::pod_env::resolve_env_from_source(
            request.container,
            &request.key.namespace,
            self.env_source.as_ref(),
        )
        .await;
        let resolved_env = crate::kubelet::pod_env::resolve_env_value_from_source(
            request.container,
            &request.key.namespace,
            self.env_source.as_ref(),
        )
        .await;
        let subpath_env = crate::kubelet::pod_env::build_subpath_env(
            request.container,
            request.pod,
            &resolved_env_from,
            &resolved_env,
        );
        let mut container_config = crate::kubelet::pod_container_config::build_container_config(
            request.container,
            request.pod,
            request.container_name,
            request.kubernetes_service_ip,
            &resolved_env_from,
            &resolved_env,
        );
        let service_envs = crate::kubelet::pod_service_envs::resolve_service_envs_from_source(
            &request.key.namespace,
            self.env_source.as_ref(),
        )
        .await;
        append_service_envs(&mut container_config, &service_envs);

        match crate::kubelet::pod_volume_manager::PodVolumeManager::build_mounts(
            request.container,
            request.volume_paths,
            &subpath_env,
        )
        .map_err(|e| anyhow::anyhow!("{}", e))
        {
            Ok((mounts, _subpath_dirs)) => {
                container_config.mounts = mounts;
            }
            Err(e) if request.ignore_mount_errors => {
                tracing::warn!(
                    namespace = request.key.namespace,
                    name = request.key.name,
                    uid = request.key.uid,
                    container = request.container_name,
                    "Failed to build container mounts: {:#}",
                    e
                );
            }
            Err(e) => {
                return Err(e);
            }
        }

        let hosts_file_path =
            managed_hosts_file_path(&self.config.containerd_namespace, request.key, request.pod);
        append_managed_hosts_mount(&mut container_config.mounts, hosts_file_path.as_deref());

        let termination_log_host = self
            .filesystem
            .ensure_termination_log_file(request.key, request.container_name)
            .await;
        container_config.mounts.push(k8s_cri::v1::Mount {
            container_path: get_termination_message_path(request.container).to_string(),
            host_path: termination_log_host,
            readonly: false,
            selinux_relabel: false,
            propagation: 0,
            gid_mappings: vec![],
            uid_mappings: vec![],
            image: None,
            recursive_read_only: false,
        });

        Ok(container_config)
    }

    async fn restart_container_in_sandbox(
        &self,
        key: &PodRuntimeKey,
        pod: &serde_json::Value,
        sandbox_id: &str,
        container_name: &str,
        old_container_id: &str,
        last_state: serde_json::Value,
    ) -> anyhow::Result<String> {
        let _ = self.cri.stop_container(old_container_id, 10).await;
        self.cri.remove_container(old_container_id).await?;

        let volume_paths = self.volumes.process_volumes(key, pod).await?;
        if pod
            .pointer("/spec/securityContext/fsGroup")
            .and_then(|value| value.as_u64())
            .is_some()
        {
            let _ = self.filesystem.apply_fs_group(key, pod).await;
        }

        let Some(container) = find_pod_container_spec(pod, container_name) else {
            anyhow::bail!(
                "container {} not found in pod {}/{} spec",
                container_name,
                key.namespace,
                key.name
            );
        };
        let dns_ip = crate::controllers::coredns::derive_dns_service_ip(&self.config.service_cidr);
        let kubernetes_service_ip = crate::controllers::kube_service::derive_kubernetes_service_ip(
            &self.config.service_cidr,
        );
        let container_config = self
            .build_container_config_with_env(ContainerConfigBuildRequest {
                key,
                pod,
                container,
                container_name,
                kubernetes_service_ip: &kubernetes_service_ip,
                volume_paths: &volume_paths,
                ignore_mount_errors: false,
            })
            .await?;
        let default_spec = serde_json::json!({});
        let pod_spec = pod.get("spec").unwrap_or(&default_spec);
        let sandbox_config = build_sandbox_config_with_dns_policy(
            &key.name,
            &key.namespace,
            pod_status_ip(pod),
            &key.uid,
            &self.config.containerd_namespace,
            &dns_ip,
            pod_spec,
        );

        let new_container_id = self
            .cri
            .create_container(container_config, sandbox_id, sandbox_config)
            .await?;
        self.cri.start_container(&new_container_id).await?;
        let _ = self
            .repository
            .note_container_restart_for_uid(
                &key.namespace,
                &key.name,
                &key.uid,
                container_name,
                last_state,
                None,
            )
            .await;
        Ok(new_container_id)
    }

    async fn restart_exited_containers_if_needed(
        &self,
        key: &PodRuntimeKey,
        pod: &serde_json::Value,
        sandbox_id: &str,
        container_statuses: &[serde_json::Value],
    ) -> anyhow::Result<Option<Vec<serde_json::Value>>> {
        if pod.pointer("/metadata/deletionTimestamp").is_some() {
            return Ok(None);
        }

        let restart_policy = crate::kubelet::pod_runtime::status_helpers::pod_restart_policy(pod);
        let mut restarted_statuses = container_statuses.to_vec();
        let mut restarted = false;
        for status in container_statuses {
            let Some(container_name) = status.get("name").and_then(|value| value.as_str()) else {
                continue;
            };
            let Some(exit_code) = status
                .pointer("/state/terminated/exitCode")
                .and_then(|value| value.as_i64())
                .and_then(|value| i32::try_from(value).ok())
            else {
                continue;
            };
            if !crate::kubelet::pod_runtime::status_helpers::should_restart_exited_container(
                restart_policy,
                exit_code,
            ) {
                continue;
            }
            let Some(old_container_id) = runtime_status_container_id(status) else {
                tracing::warn!(
                    namespace = key.namespace,
                    pod = key.name,
                    uid = key.uid,
                    container = container_name,
                    "exited container needs restart but runtime container id is missing"
                );
                continue;
            };
            let Some(last_state) = restart_last_state_from_reconciled_status(status) else {
                continue;
            };
            let last_state_for_status = last_state.clone();

            match self
                .restart_container_in_sandbox(
                    key,
                    pod,
                    sandbox_id,
                    container_name,
                    &old_container_id,
                    last_state,
                )
                .await
            {
                Ok(new_container_id) => {
                    restarted = true;
                    if let Some(replacement) = restarted_running_container_status(
                        pod,
                        container_name,
                        &new_container_id,
                        status,
                        &last_state_for_status,
                    ) {
                        replace_container_status(
                            &mut restarted_statuses,
                            container_name,
                            replacement,
                        );
                    }
                    tracing::info!(
                        namespace = key.namespace,
                        pod = key.name,
                        uid = key.uid,
                        container = container_name,
                        old_container_id = old_container_id,
                        new_container_id = new_container_id,
                        "restarted exited container during runtime reconcile"
                    );
                }
                Err(err) => {
                    tracing::warn!(
                        namespace = key.namespace,
                        pod = key.name,
                        uid = key.uid,
                        container = container_name,
                        old_container_id = old_container_id,
                        "failed to restart exited container during runtime reconcile: {:#}",
                        err
                    );
                }
            }
        }

        Ok(restarted.then_some(restarted_statuses))
    }

    async fn reconcile_container_statuses_from_pod_spec(
        &self,
        key: &PodRuntimeKey,
        pod: &serde_json::Value,
        observed: &[(String, ContainerRuntimeState)],
    ) -> (String, Vec<serde_json::Value>) {
        let spec_containers = pod
            .pointer("/spec/containers")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let existing_statuses = pod
            .pointer("/status/containerStatuses")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        let spec_names: std::collections::HashSet<String> = spec_containers
            .iter()
            .filter_map(|container| {
                container
                    .get("name")
                    .and_then(|v| v.as_str())
                    .map(ToString::to_string)
            })
            .collect();

        let mut infos_by_name: std::collections::HashMap<String, ReconcileContainerInfo> =
            std::collections::HashMap::new();
        for (idx, (container_id, observed_state)) in observed.iter().enumerate() {
            let status = match self.cri.container_status(container_id).await {
                Ok(response) => response.status,
                Err(e) => {
                    tracing::warn!(
                        container_id = container_id,
                        "failed to inspect container during runtime reconcile: {}",
                        e
                    );
                    None
                }
            };
            let fallback_spec = spec_containers.get(idx);
            // Prefer the CRI metadata name, then the existing status entry
            // whose containerID references this container (so a CRI event
            // container is never assigned to the wrong spec container when
            // CRI omits the metadata name), then the spec index.
            let cri_name = status
                .as_ref()
                .and_then(|status| status.metadata.as_ref())
                .map(|metadata| metadata.name.as_str())
                .filter(|name| !name.is_empty());
            let existing_status_name = existing_statuses.iter().find_map(|existing| {
                let matches_id = existing
                    .get("containerID")
                    .and_then(|id| id.as_str())
                    .map(|id| id.strip_prefix("containerd://").unwrap_or(id) == container_id)
                    .unwrap_or(false);
                if matches_id {
                    existing
                        .get("name")
                        .and_then(|name| name.as_str())
                        .filter(|name| !name.is_empty())
                } else {
                    None
                }
            });
            let spec_index_name = fallback_spec
                .and_then(|container| container.get("name").and_then(|name| name.as_str()));
            let container_name = cri_name
                .or(existing_status_name)
                .or(spec_index_name)
                .unwrap_or("");
            if container_name.is_empty() || !spec_names.contains(container_name) {
                continue;
            }

            let image = status
                .as_ref()
                .and_then(|status| status.image.as_ref())
                .map(|image| image.image.as_str())
                .filter(|image| !image.is_empty())
                .or_else(|| {
                    fallback_spec.and_then(|container| {
                        container.get("image").and_then(|image| image.as_str())
                    })
                })
                .unwrap_or("nginx:latest")
                .to_string();
            let image_ref = status
                .as_ref()
                .map(|status| {
                    if !status.image_ref.is_empty() {
                        status.image_ref.clone()
                    } else if !status.image_id.is_empty() {
                        status.image_id.clone()
                    } else {
                        image.clone()
                    }
                })
                .unwrap_or_else(|| image.clone());
            let state = *observed_state;
            let termination_message = match status.as_ref() {
                Some(status) if !status.message.is_empty() => status.message.clone(),
                _ if state == ContainerRuntimeState::Exited => {
                    self.read_termination_message_for_container(
                        key,
                        pod,
                        container_name,
                        status.as_ref().map(|status| status.exit_code).unwrap_or(0),
                    )
                    .await
                }
                _ => String::new(),
            };
            let info = ReconcileContainerInfo {
                container_id: container_id.clone(),
                state,
                exit_code: status.as_ref().map(|status| status.exit_code).unwrap_or(0),
                started_at: status.as_ref().map(|status| status.started_at).unwrap_or(0),
                finished_at: status
                    .as_ref()
                    .map(|status| status.finished_at)
                    .unwrap_or(0),
                created_at: status
                    .as_ref()
                    .map(|status| status.created_at)
                    .unwrap_or(idx as i64),
                image,
                image_ref,
                termination_message,
            };

            match infos_by_name.get(container_name) {
                Some(existing) if existing.created_at > info.created_at => {}
                _ => {
                    infos_by_name.insert(container_name.to_string(), info);
                }
            }
        }

        let container_statuses = spec_containers
            .iter()
            .filter_map(|container| {
                let container_name = container.get("name").and_then(|v| v.as_str())?;
                let image = container
                    .get("image")
                    .and_then(|v| v.as_str())
                    .unwrap_or("nginx:latest");
                let existing = existing_statuses
                    .iter()
                    .find(|status| status.get("name").and_then(|v| v.as_str()) == Some(container_name));
                let has_readiness_probe = container.get("readinessProbe").is_some();
                let info = infos_by_name.get(container_name);
                let running = info
                    .map(|info| info.state.is_running())
                    .unwrap_or(false);
                let ready = running
                    && if !has_readiness_probe {
                        true
                    } else {
                        existing
                            .and_then(|status| status.get("ready"))
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false)
                    };
                let started = info
                    .map(|info| info.state.has_started())
                    .unwrap_or(false);
                let state_obj = match info {
                    Some(info) if info.state == ContainerRuntimeState::Running => {
                        let started_at = if info.started_at > 0 {
                            cri_timestamp_from_ns(info.started_at)
                        } else {
                            existing
                                .and_then(|status| status.pointer("/state/running/startedAt"))
                                .and_then(|value| value.as_str())
                                .map(ToString::to_string)
                                .unwrap_or_else(crate::utils::k8s_timestamp)
                        };
                        serde_json::json!({ "running": { "startedAt": started_at } })
                    }
                    Some(info) if info.state == ContainerRuntimeState::Exited => {
                        let mut terminated = serde_json::json!({
                            "exitCode": info.exit_code,
                            "reason": if info.exit_code == 0 { "Completed" } else { "Error" },
                            "startedAt": cri_timestamp_from_ns(info.started_at),
                            "finishedAt": cri_timestamp_from_ns(info.finished_at),
                        });
                        if !info.termination_message.is_empty() {
                            terminated["message"] =
                                serde_json::json!(info.termination_message.clone());
                        }
                        serde_json::json!({ "terminated": terminated })
                    }
                    _ => serde_json::json!({ "waiting": { "reason": "ContainerCreating" } }),
                };
                let mut status = serde_json::json!({
                    "name": container_name,
                    "containerID": info
                        .map(|info| serde_json::json!(format!("containerd://{}", info.container_id)))
                        .unwrap_or(serde_json::Value::Null),
                    "ready": ready,
                    "started": started,
                    "restartCount": existing
                        .and_then(|status| status.get("restartCount"))
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0),
                    "state": state_obj,
                    "image": info.map(|info| info.image.as_str()).unwrap_or(image),
                    "imageID": info.map(|info| info.image_ref.as_str()).unwrap_or(image),
                });
                if let Some(last_state) = existing.and_then(|status| status.get("lastState"))
                    && let Some(obj) = status.as_object_mut() {
                        obj.insert("lastState".to_string(), last_state.clone());
                    }
                Some(status)
            })
            .collect();

        let phase = Self::compute_reconciled_phase(&spec_containers, &infos_by_name, pod);
        (phase, container_statuses)
    }

    async fn read_termination_message_for_container(
        &self,
        key: &PodRuntimeKey,
        pod: &serde_json::Value,
        container_name: &str,
        exit_code: i32,
    ) -> String {
        let container_spec = find_pod_container_spec(pod, container_name);
        let policy = termination_message_policy(container_spec);
        self.filesystem
            .read_termination_message(key, container_name, policy, exit_code)
            .await
    }

    /// Read the CRI status for a specific container id and map it to the
    /// runtime-reconcile state enum. Returns `None` when CRI cannot report
    /// the container (already removed, unknown id) so the caller can decide
    /// whether to fall back to ContainerCreating or skip the entry.
    async fn runtime_state_from_container_status(
        &self,
        container_id: &str,
    ) -> anyhow::Result<Option<ContainerRuntimeState>> {
        let state = match self.cri.container_status(container_id).await {
            Ok(response) => response
                .status
                .map(|status| ContainerRuntimeState::from_cri_state_i32(status.state)),
            Err(e) => {
                tracing::warn!(
                    container_id = container_id,
                    "failed to inspect hinted container during runtime reconcile: {}",
                    e
                );
                None
            }
        };
        Ok(state)
    }

    fn compute_reconciled_phase(
        spec_containers: &[serde_json::Value],
        infos_by_name: &std::collections::HashMap<String, ReconcileContainerInfo>,
        pod: &serde_json::Value,
    ) -> String {
        if spec_containers.is_empty() || infos_by_name.is_empty() {
            return "Pending".to_string();
        }

        let restart_policy = pod
            .pointer("/spec/restartPolicy")
            .and_then(|value| value.as_str())
            .filter(|value| !value.is_empty())
            .unwrap_or("Always");
        let mut any_running = false;
        let mut any_exited_nonzero = false;
        let mut all_exited_zero = true;

        for container in spec_containers {
            let Some(name) = container.get("name").and_then(|value| value.as_str()) else {
                all_exited_zero = false;
                continue;
            };
            let Some(info) = infos_by_name.get(name) else {
                all_exited_zero = false;
                continue;
            };
            match info.state {
                ContainerRuntimeState::Running => {
                    any_running = true;
                    all_exited_zero = false;
                }
                ContainerRuntimeState::Exited => {
                    if info.exit_code != 0 {
                        any_exited_nonzero = true;
                        all_exited_zero = false;
                    }
                }
                _ => {
                    all_exited_zero = false;
                }
            }
        }

        if any_running {
            return "Running".to_string();
        }
        if restart_policy == "Always" {
            return "Running".to_string();
        }
        if restart_policy == "OnFailure" && any_exited_nonzero {
            return "Running".to_string();
        }
        if all_exited_zero && matches!(restart_policy, "Never" | "OnFailure") {
            return "Succeeded".to_string();
        }
        if any_exited_nonzero && restart_policy == "Never" {
            return "Failed".to_string();
        }
        "Pending".to_string()
    }

    fn pod_with_network_status(
        mut pod: serde_json::Value,
        pod_ip: &str,
        host_ip: &str,
    ) -> serde_json::Value {
        if let Some(obj) = pod.as_object_mut() {
            let status = obj
                .entry("status".to_string())
                .or_insert_with(|| serde_json::json!({}));
            if let Some(status_obj) = status.as_object_mut() {
                status_obj.insert("podIP".to_string(), serde_json::json!(pod_ip));
                status_obj.insert("podIPs".to_string(), serde_json::json!([{ "ip": pod_ip }]));
                status_obj.insert("hostIP".to_string(), serde_json::json!(host_ip));
                status_obj.insert(
                    "hostIPs".to_string(),
                    serde_json::json!([{ "ip": host_ip }]),
                );
            }
        }
        pod
    }

    async fn create_and_record_sandbox(
        &self,
        key: &PodRuntimeKey,
        sandbox_config: k8s_cri::v1::PodSandboxConfig,
    ) -> anyhow::Result<String> {
        let sandbox_id = self
            .cri
            .run_pod_sandbox(sandbox_config)
            .await
            .map_err(|e| anyhow::anyhow!("sandbox creation failed: {:#}", e))?;

        if let Err(e) = self.store.record_sandbox(key, &sandbox_id).await {
            tracing::warn!(
                namespace = key.namespace,
                name = key.name,
                uid = key.uid,
                sandbox_id = sandbox_id,
                "Failed to record sandbox in store (will use annotation fallback): {}",
                e
            );
        }
        Ok(sandbox_id)
    }

    fn is_network_assignment_timeout(err: &anyhow::Error) -> bool {
        err.chain().any(|cause| {
            cause.downcast_ref::<PodStartupErrorKind>()
                == Some(&PodStartupErrorKind::NetworkAssignmentTimedOut)
        })
    }

    async fn rollback_partial_pod_start(
        &self,
        key: &PodRuntimeKey,
        sandbox_id: &str,
        reason: &str,
    ) {
        tracing::warn!(
            namespace = key.namespace,
            name = key.name,
            uid = key.uid,
            sandbox_id,
            "rolling back partial pod start: {reason}"
        );

        let containers = self
            .container_control
            .list_containers(Some(sandbox_id))
            .await
            .unwrap_or_else(|err| {
                tracing::warn!(
                    namespace = key.namespace,
                    name = key.name,
                    uid = key.uid,
                    sandbox_id,
                    "failed to list containers during partial rollback: {err:#}"
                );
                Vec::new()
            });
        let mut seen = std::collections::HashSet::new();
        for (container_id, _) in containers
            .into_iter()
            .filter(|(id, _)| seen.insert(id.clone()))
        {
            let _ = self.cri.stop_container(&container_id, 10).await;
            let _ = self.cri.remove_container(&container_id).await;
        }
        let _ = self.network.release_sandbox_network(key, sandbox_id).await;
        let _ = self.cri.stop_pod_sandbox(sandbox_id).await;
        let _ = self.cri.remove_pod_sandbox(sandbox_id).await;
        let _ = self.store.delete_sandbox(key).await;
        self.cleanup_pod_local_artifacts(key).await;
    }

    /// Shared local-artifact teardown for every pod stop path (normal delete,
    /// failed-create rollback, orphan/cold-sandbox finalize). Unmounts and
    /// removes the pod's volumes first, reclaims the cgroup tree, then removes
    /// the pod filesystem root, so the recursive root removal never runs over a
    /// still-live mount. Every step derives entirely from `key` and is
    /// idempotent, so this path needs no deleted-Pod snapshot, is safe to re-run
    /// after a timed-out finalize, and never leaks the cgroup when no sandbox
    /// could be resolved.
    pub(super) async fn cleanup_pod_local_artifacts(&self, key: &PodRuntimeKey) {
        let _ = self.volumes.cleanup_volumes(key).await;
        let _ = self.filesystem.cleanup_cgroup(key).await;
        let _ = self.filesystem.cleanup_pod_filesystem(key).await;
    }

    /// Construct the production runtime service with all required ports.
    /// Every field is wired at construction time; no late initialization.
    pub fn new(dependencies: RealPodRuntimeServiceDependencies) -> Self {
        let RealPodRuntimeServiceDependencies {
            cri,
            container_control,
            network,
            store,
            slot_admission,
            repository,
            filesystem,
            volumes,
            probes,
            hostports,
            events,
            hooks,
            env_source,
            finalizer,
            supervisor,
            config,
            node_view,
            cluster_view,
        } = dependencies;
        Self {
            cri,
            container_control,
            network,
            store,
            slot_admission,
            repository,
            filesystem,
            volumes,
            probes,
            hostports,
            events,
            hooks,
            env_source,
            finalizer,
            supervisor,
            config,
            node_view,
            cluster_view,
            status_emitter: PodStatusEmitter::default(),
        }
    }

    /// Resolve a sandbox ID for pod stop via the store → annotation → CRI
    /// ladder. Matches the legacy `resolve_sandbox_id_for_delete_with_timeout`.
    async fn resolve_sandbox_id_for_stop(
        &self,
        key: &PodRuntimeKey,
        pod: &serde_json::Value,
    ) -> Option<String> {
        // 1. Store row.
        if let Ok(Some(id)) = self.store.get_sandbox_id(key).await
            && !id.is_empty()
        {
            return Some(id);
        }

        // 2. klights.dev/sandbox-id annotation.
        let annotation_key = "klights.dev/sandbox-id";
        if let Some(id) = pod
            .pointer("/metadata/annotations")
            .and_then(|a| a.get(annotation_key))
            .and_then(|v| v.as_str())
            && !id.is_empty()
        {
            return Some(id.to_string());
        }

        // 3. CRI list_pod_sandboxes matched by pod UID.
        let pod_uid = pod
            .pointer("/metadata/uid")
            .and_then(|v| v.as_str())
            .unwrap_or(&key.uid);
        if let Ok(sandboxes) = self.cri.list_pod_sandboxes(Some(pod_uid)).await {
            for (sandbox_id, _state) in &sandboxes {
                if !sandbox_id.is_empty() {
                    return Some(sandbox_id.clone());
                }
            }
        }

        None
    }

    async fn wait_for_init_container_stop(
        &self,
        mut events: Box<dyn CriRuntimeContainerEventStream>,
        container_id: &str,
        container_name: &str,
    ) -> anyhow::Result<InitContainerStop> {
        if let Some(stopped) = self.observed_init_container_stop(container_id).await? {
            return Ok(stopped);
        }

        self.supervisor
            .sleep(
                "init_container_fast_exit_recheck",
                INIT_CONTAINER_FAST_EXIT_RECHECK_DELAY,
            )
            .await?;
        if let Some(stopped) = self.observed_init_container_stop(container_id).await? {
            return Ok(stopped);
        }

        loop {
            let Some(event) = events.next_event().await? else {
                anyhow::bail!(
                    "CRI event stream ended while waiting for init container {} — pod start will be retried",
                    container_name
                );
            };
            if event.container_id == container_id
                && event.kind == CriRuntimeContainerEventKind::Stopped
            {
                let status = self.cri.container_status(container_id).await?;
                if let Some(stopped) = init_container_stop_from_status(&status) {
                    return Ok(stopped);
                }
                anyhow::bail!(
                    "CRI reported stopped event for init container {} but container status was not exited",
                    container_name
                );
            }

            if let Some(stopped) = self.observed_init_container_stop(container_id).await? {
                return Ok(stopped);
            }
        }
    }

    async fn observed_init_container_stop(
        &self,
        container_id: &str,
    ) -> anyhow::Result<Option<InitContainerStop>> {
        let status = self.cri.container_status(container_id).await?;
        Ok(init_container_stop_from_status(&status))
    }
}

#[async_trait::async_trait]
impl PodRuntimeService for RealPodRuntimeService {
    async fn check_slot_admission(
        &self,
        request: PodSlotAdmissionRequest,
        reply_to: LifecycleReplyHandle,
        cancel: CancellationToken,
    ) -> anyhow::Result<()> {
        crate::kubelet::pod_runtime::slot_admission::check_slot_admission(
            self.slot_admission.as_ref(),
            &self.config.node_name,
            request,
            reply_to,
            cancel,
        )
        .await
    }

    async fn start_pod(
        &self,
        key: PodRuntimeKey,
        pod: Option<serde_json::Value>,
        cancel: CancellationToken,
    ) -> anyhow::Result<PodStartResult> {
        let (pod, from_snapshot) = match pod {
            Some(p) => (p, true),
            None => {
                let resource = self
                    .repository
                    .get_pod_for_uid(&key.namespace, &key.name, &key.uid)
                    .await
                    .map_err(|e| anyhow::anyhow!("failed to read pod: {:#}", e))?
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "pod {}/{} not found for uid {}",
                            key.namespace,
                            key.name,
                            key.uid
                        )
                    })?;
                ((*resource.data).clone(), false)
            }
        };

        // Verify the snapshot UID matches the key UID.
        let pod_uid = pod
            .pointer("/metadata/uid")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow::anyhow!("pod snapshot missing metadata.uid"))?;
        if pod_uid != key.uid {
            return Ok(PodStartResult::Failed(format!(
                "UID mismatch: key {} != pod {}",
                key.uid, pod_uid
            )));
        }
        if from_snapshot
            && let LivePodUidCheck::Different { live_uid } = self
                .repository
                .check_live_pod_uid(&key.namespace, &key.name, &key.uid)
                .await
                .map_err(|e| anyhow::anyhow!("failed to verify live pod identity: {:#}", e))?
        {
            return Ok(PodStartResult::Failed(format!(
                "UID mismatch: key {} != live pod {}",
                key.uid, live_uid
            )));
        }

        // Node ownership check: worker starts only pods assigned to this node.
        if !self.node_view.owns_pod_runtime(&pod) {
            return Ok(PodStartResult::Failed(format!(
                "pod {}/{} is not assigned to this node ({})",
                key.namespace,
                key.name,
                self.node_view.node_name()
            )));
        }

        if let Some(sandbox_id) =
            crate::kubelet::pod_runtime::recovery::already_realized_running_sandbox(
                self.store.as_ref(),
                self.container_control.as_ref(),
                &key,
                &pod,
            )
            .await
        {
            tracing::info!(
                namespace = key.namespace,
                name = key.name,
                uid = key.uid,
                sandbox_id = %sandbox_id,
                "pod startup recovery found already realized running sandbox; skipping duplicate start"
            );
            if let Err(e) = self.volumes.process_volumes(&key, &pod).await {
                let message = format!("Failed to reconcile volumes for running pod: {e:#}");
                tracing::warn!(
                    namespace = key.namespace,
                    name = key.name,
                    uid = key.uid,
                    sandbox_id = %sandbox_id,
                    "{message}"
                );
                return Ok(PodStartResult::Failed(message));
            }
            return Ok(PodStartResult::Started {
                sandbox_id: Some(sandbox_id),
            });
        }

        // HostPort admission check.
        if let Err(e) = self.hostports.check_host_port_admission(&key, &pod).await {
            let failure_message = format!("hostPort admission failed: {:#}", e);
            // Emit admission failure event.
            let _ = self
                .events
                .emit_pod_event(
                    &key,
                    "Warning",
                    "Failed",
                    &format!("Error: failed to admit pod: {:#}", e),
                    "klights-kubelet",
                    &self.config.node_name,
                )
                .await;
            let mut failed_status = serde_json::json!({
                "phase": "Failed",
                "containerStatuses": build_pod_start_failure_app_statuses(
                    &pod,
                    &failure_message,
                ),
                "initContainerStatuses": pod
                    .pointer("/status/initContainerStatuses")
                    .and_then(|value| value.as_array())
                    .cloned()
                    .unwrap_or_default(),
            });
            if let Some(status_obj) = failed_status.as_object_mut() {
                let pod_ip = pod_status_ip(&pod);
                if !pod_ip.trim().is_empty() {
                    status_obj.insert("podIP".to_string(), serde_json::json!(pod_ip));
                }
                if let Some(host_ip) = pod_status_host_ip(&pod) {
                    status_obj.insert("hostIP".to_string(), serde_json::json!(host_ip));
                }
            }
            self.write_pod_status(&key, failed_status).await?;
            return Ok(PodStartResult::Terminal(failure_message));
        }

        // Write initial Pending status through cluster boundary. For init
        // pods this must include status arrays; otherwise a retry from a
        // stale actor snapshot can transiently erase initContainerStatuses
        // and violate Kubernetes watch invariants.
        let pending_status = build_initial_pending_status(&pod);
        if let Err(e) = self.write_pod_status(&key, pending_status).await {
            tracing::warn!(
                namespace = key.namespace,
                name = key.name,
                uid = key.uid,
                "Failed to write initial Pending status: {}",
                e
            );
        }

        // Emit Scheduled event.
        let _ = self
            .events
            .emit_pod_event(
                &key,
                "Normal",
                "Scheduled",
                &format!(
                    "Successfully assigned {}/{} to {}",
                    key.namespace, key.name, self.config.node_name
                ),
                "klights-kubelet",
                &self.config.node_name,
            )
            .await;

        // Image pull phase.
        let containers = match pod
            .get("spec")
            .and_then(|s| s.get("containers"))
            .and_then(|c| c.as_array())
        {
            Some(c) => c,
            None => {
                return Ok(PodStartResult::Failed("Pod missing spec.containers".into()));
            }
        };

        for container in containers {
            let image = match container.get("image").and_then(|i| i.as_str()) {
                Some(i) => i,
                None => continue,
            };

            let normalized_image = crate::kubelet::pod_runtime::images::normalize_image_name(image);
            let pull_policy = crate::kubelet::pod_runtime::images::effective_pull_policy(
                container,
                &normalized_image,
            );

            if pull_policy == "Never" {
                continue;
            }
            if pull_policy == "IfNotPresent" {
                match self.cri.image_status(&normalized_image).await {
                    Ok(true) => continue,
                    Ok(false) => {}
                    Err(e) => {
                        tracing::warn!(
                            "image_status check failed for {} ({}); attempting pull",
                            normalized_image,
                            e
                        );
                    }
                }
            }

            // Emit Pulling event.
            let _ = self
                .events
                .emit_pod_event(
                    &key,
                    "Normal",
                    "Pulling",
                    &format!("Pulling image \"{}\"", normalized_image),
                    "klights-kubelet",
                    &self.config.node_name,
                )
                .await;

            match self.cri.pull_image(&normalized_image).await {
                Ok(image_ref) => {
                    tracing::info!(
                        "Pulled image {} for pod {}/{}",
                        normalized_image,
                        key.namespace,
                        key.name
                    );
                    // Emit Pulled event.
                    let _ = self
                        .events
                        .emit_pod_event(
                            &key,
                            "Normal",
                            "Pulled",
                            &format!(
                                "Successfully pulled image \"{}\" in {}",
                                normalized_image, image_ref
                            ),
                            "klights-kubelet",
                            &self.config.node_name,
                        )
                        .await;
                }
                Err(e) => {
                    // Emit Failed event.
                    let _ = self
                        .events
                        .emit_pod_event(
                            &key,
                            "Warning",
                            "Failed",
                            &format!("Failed to pull image \"{}\": {:#}", normalized_image, e),
                            "klights-kubelet",
                            &self.config.node_name,
                        )
                        .await;
                    return Ok(PodStartResult::Failed(format!(
                        "Failed to pull image \"{}\": {:#}",
                        normalized_image, e
                    )));
                }
            }
        }

        // --- Cancellation check: before sandbox ---
        if cancel.is_cancelled() {
            return Ok(PodStartResult::Cancelled);
        }

        // Sandbox creation and network assignment phase.
        let host_network = pod
            .pointer("/spec/hostNetwork")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let dns_ip = crate::controllers::coredns::derive_dns_service_ip(&self.config.service_cidr);
        let default_spec = serde_json::json!({});
        let pod_spec = pod.get("spec").unwrap_or(&default_spec);
        let sandbox_config = build_sandbox_config_with_dns_policy(
            &key.name,
            &key.namespace,
            "",
            &key.uid,
            &self.config.containerd_namespace,
            &dns_ip,
            pod_spec,
        );
        let container_sandbox_config = sandbox_config.clone();
        let kubernetes_service_ip = crate::controllers::kube_service::derive_kubernetes_service_ip(
            &self.config.service_cidr,
        );

        let sandbox_id = match self.store.get_sandbox_id(&key).await {
            Ok(Some(existing)) if !existing.trim().is_empty() => existing,
            Ok(_) => self.create_and_record_sandbox(&key, sandbox_config).await?,
            Err(e) => {
                tracing::warn!(
                    namespace = key.namespace,
                    name = key.name,
                    uid = key.uid,
                    "Failed to look up existing sandbox in store before pod start: {}",
                    e
                );
                self.create_and_record_sandbox(&key, sandbox_config).await?
            }
        };

        // Read CNI network assignment.
        let assignment = match self
            .network
            .read_assignment(&sandbox_id, &key, host_network)
            .await
        {
            Ok(assignment) => assignment,
            Err(e) => {
                if Self::is_network_assignment_timeout(&e) {
                    self.rollback_partial_pod_start(
                        &key,
                        &sandbox_id,
                        "network assignment timed out",
                    )
                    .await;
                }
                return Err(anyhow::anyhow!("network assignment failed: {:#}", e));
            }
        };
        let pod = Self::pod_with_network_status(
            pod,
            assignment.pod_ip.as_str(),
            assignment.host_ip.as_str(),
        );

        // --- Cancellation check: after sandbox + store + network, before hostports/containers ---
        if cancel.is_cancelled() {
            self.rollback_partial_pod_start(&key, &sandbox_id, "startup cancelled after sandbox")
                .await;
            return Ok(PodStartResult::Cancelled);
        }

        // HostPort rules.
        if let Err(e) = self.hostports.add_host_ports(&key, &pod).await {
            tracing::warn!(
                namespace = key.namespace,
                name = key.name,
                uid = key.uid,
                "Failed to add hostPort rules: {}",
                e
            );
        }

        // Filesystem: write /etc/hosts and create log directories.
        if let Err(e) = self.filesystem.write_hosts(&key, &pod).await {
            tracing::warn!(
                namespace = key.namespace,
                name = key.name,
                uid = key.uid,
                "Failed to write hosts: {}",
                e
            );
        }
        if let Err(e) = self.filesystem.create_log_directory(&key).await {
            tracing::warn!(
                namespace = key.namespace,
                name = key.name,
                uid = key.uid,
                "Failed to create log directory: {}",
                e
            );
        }

        // Volumes.
        let volume_paths = match self.volumes.process_volumes(&key, &pod).await {
            Ok(paths) => paths,
            Err(e) => {
                let message = format!("Failed to process volumes: {e:#}");
                tracing::warn!(
                    namespace = key.namespace,
                    name = key.name,
                    uid = key.uid,
                    "{message}"
                );
                let _ = self.hostports.remove_host_ports(&key, &pod).await;
                self.rollback_partial_pod_start(&key, &sandbox_id, "volume processing failed")
                    .await;
                let _ = self
                    .events
                    .emit_pod_event(
                        &key,
                        "Warning",
                        "Failed",
                        &message,
                        "klights-kubelet",
                        &self.config.node_name,
                    )
                    .await;
                return Ok(PodStartResult::Failed(message));
            }
        };

        // Apply fsGroup ownership to volume files.
        if let Some(_fs_group_gid) = pod
            .pointer("/spec/securityContext/fsGroup")
            .and_then(|v| v.as_u64())
        {
            let _ = self.filesystem.apply_fs_group(&key, &pod).await;
        }

        // --- Init Containers ---
        // Init containers run sequentially and must complete before main
        // containers. Each init container is pulled, created, started, and
        // waited on. Non-zero exit codes abort the pod start.
        let init_containers: Vec<serde_json::Value> = pod
            .pointer("/spec/initContainers")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let mut init_container_statuses = pod
            .pointer("/status/initContainerStatuses")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        for init_container in &init_containers {
            let container_name = init_container
                .get("name")
                .and_then(|n| n.as_str())
                .unwrap_or("init");
            if init_container_completed(&init_container_statuses, container_name) {
                continue;
            }

            let image = match init_container.get("image").and_then(|i| i.as_str()) {
                Some(i) => i,
                None => {
                    return Ok(PodStartResult::Failed(format!(
                        "init container {} missing image",
                        container_name
                    )));
                }
            };

            let normalized_image = crate::kubelet::pod_runtime::images::normalize_image_name(image);
            let pull_policy = crate::kubelet::pod_runtime::images::effective_pull_policy(
                init_container,
                &normalized_image,
            );

            let needs_pull = if pull_policy == "Never" {
                false
            } else if pull_policy == "IfNotPresent" {
                !matches!(self.cri.image_status(&normalized_image).await, Ok(true))
            } else {
                true
            };

            if needs_pull && let Err(e) = self.cri.pull_image(&normalized_image).await {
                let _ = self
                    .events
                    .emit_pod_event(
                        &key,
                        "Warning",
                        "Failed",
                        &format!(
                            "Error: failed to pull init container image \"{}\": {:#}",
                            normalized_image, e
                        ),
                        "klights-kubelet",
                        &self.config.node_name,
                    )
                    .await;
                return Ok(PodStartResult::Failed(format!(
                    "Failed to pull init container image \"{}\": {:#}",
                    normalized_image, e
                )));
            }

            let container_config = self
                .build_container_config_with_env(ContainerConfigBuildRequest {
                    key: &key,
                    pod: &pod,
                    container: init_container,
                    container_name,
                    kubernetes_service_ip: &kubernetes_service_ip,
                    volume_paths: &volume_paths,
                    ignore_mount_errors: true,
                })
                .await?;

            let container_id = self
                .cri
                .create_container(
                    container_config,
                    &sandbox_id,
                    container_sandbox_config.clone(),
                )
                .await
                .map_err(|e| {
                    anyhow::anyhow!(
                        "failed to create init container {}: {:#}",
                        container_name,
                        e
                    )
                })?;

            let started_at = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64;
            self.cri.start_container(&container_id).await.map_err(|e| {
                anyhow::anyhow!("failed to start init container {}: {:#}", container_name, e)
            })?;

            let event_stream = self.cri.subscribe_container_events().await.map_err(|e| {
                anyhow::anyhow!(
                    "CRI event stream unavailable for init container {}: {:#}",
                    container_name,
                    e
                )
            })?;

            let wait_result = self
                .supervisor
                .timeout(
                    "init_container_wait_for_stop",
                    INIT_CONTAINER_WAIT_TIMEOUT,
                    self.wait_for_init_container_stop(event_stream, &container_id, container_name),
                )
                .await?;
            let stopped = match wait_result {
                Ok(Ok(stopped)) => stopped,
                Ok(Err(e)) => return Err(e),
                Err(_) => {
                    anyhow::bail!("init container {} timed out after 300s", container_name);
                }
            };

            if cancel.is_cancelled() {
                return Ok(PodStartResult::Cancelled);
            }

            let exit_code = stopped.exit_code;
            if exit_code != 0 {
                let failure_message = format!(
                    "Init container {} failed with exit code {}",
                    container_name, exit_code
                );
                let _ = self
                    .events
                    .emit_pod_event(
                        &key,
                        "Warning",
                        "Failed",
                        &format!(
                            "Error: init container {} failed with exit code {}",
                            container_name, exit_code
                        ),
                        "klights-kubelet",
                        &self.config.node_name,
                    )
                    .await;

                let restart_policy =
                    crate::kubelet::pod_runtime::status_helpers::pod_restart_policy(&pod);
                let retry =
                    crate::kubelet::pod_runtime::status_helpers::should_restart_exited_container(
                        restart_policy,
                        exit_code,
                    );
                let terminated =
                    build_init_failure_terminated_state(exit_code, started_at, stopped.finished_at);
                let next_init_statuses = if retry {
                    build_retrying_init_container_statuses(
                        &pod,
                        container_name,
                        &init_container_statuses,
                        terminated,
                    )
                } else {
                    build_failed_init_container_statuses(
                        &pod,
                        container_name,
                        exit_code,
                        started_at,
                        stopped.finished_at,
                    )
                };
                self.write_pod_status(
                    &key,
                    serde_json::json!({
                        "phase": if retry { "Pending" } else { "Failed" },
                        "podIP": pod_status_ip(&pod),
                        "hostIP": assignment.host_ip.as_str(),
                        "containerStatuses": build_pod_initializing_app_statuses(&pod),
                        "initContainerStatuses": next_init_statuses,
                    }),
                )
                .await?;

                if retry {
                    if let Err(e) = self.cri.remove_container(&container_id).await {
                        tracing::warn!(
                            namespace = key.namespace,
                            pod = key.name,
                            uid = key.uid,
                            container = container_name,
                            container_id = container_id,
                            "failed to remove failed init container before retry: {:#}",
                            e
                        );
                    }
                    return Ok(PodStartResult::Failed(failure_message));
                }
                return Ok(PodStartResult::Terminal(failure_message));
            }

            record_completed_init_container_status(
                &mut init_container_statuses,
                container_name,
                build_completed_init_container_status(
                    container_name,
                    &normalized_image,
                    &container_id,
                    exit_code,
                    started_at,
                    stopped.finished_at,
                ),
            );
        }

        // --- Cancellation check: after init containers, before main ---
        if cancel.is_cancelled() {
            self.rollback_partial_pod_start(&key, &sandbox_id, "startup cancelled after init")
                .await;
            return Ok(PodStartResult::Cancelled);
        }

        // --- Main Containers ---
        let containers = pod
            .get("spec")
            .and_then(|s| s.get("containers"))
            .and_then(|c| c.as_array())
            .cloned()
            .unwrap_or_default();

        let mut container_statuses: Vec<serde_json::Value> = Vec::new();
        let mut started_containers: Vec<(serde_json::Value, String)> = Vec::new();
        for container in &containers {
            let container_name = container
                .get("name")
                .and_then(|n| n.as_str())
                .unwrap_or("container");

            let container_config = match self
                .build_container_config_with_env(ContainerConfigBuildRequest {
                    key: &key,
                    pod: &pod,
                    container,
                    container_name,
                    kubernetes_service_ip: &kubernetes_service_ip,
                    volume_paths: &volume_paths,
                    ignore_mount_errors: false,
                })
                .await
            {
                Ok(config) => config,
                Err(e) => {
                    let message = format!("invalid subPath in container {container_name}: {e:#}");
                    tracing::warn!(
                        container = container_name,
                        "Container rejected due to invalid mount config: {:#}",
                        e
                    );
                    let _ = self
                        .events
                        .emit_pod_event(
                            &key,
                            "Warning",
                            "Failed",
                            &format!(
                                "Error: failed to create container {}: {}",
                                container_name, message
                            ),
                            "klights-kubelet",
                            &self.config.node_name,
                        )
                        .await;
                    container_statuses.push(
                        crate::kubelet::pod_runtime::status_helpers::build_create_container_config_error_status(
                            container,
                            container_name,
                            &message,
                        ),
                    );
                    continue;
                }
            };

            if let Err(message) = crate::kubelet::pod_container_config::check_run_as_non_root(
                &pod,
                container,
                container_name,
            ) {
                tracing::warn!(
                    container = container_name,
                    "Container rejected: {}",
                    message
                );
                let _ = self
                    .events
                    .emit_pod_event(
                        &key,
                        "Warning",
                        "Failed",
                        &format!(
                            "Error: failed to create container {}: {}",
                            container_name, message
                        ),
                        "klights-kubelet",
                        &self.config.node_name,
                    )
                    .await;
                container_statuses.push(
                    crate::kubelet::pod_runtime::status_helpers::build_create_container_config_error_status(
                        container,
                        container_name,
                        &message,
                    ),
                );
                continue;
            }

            let container_id = match self
                .cri
                .create_container(
                    container_config,
                    &sandbox_id,
                    container_sandbox_config.clone(),
                )
                .await
            {
                Ok(container_id) => container_id,
                Err(e) => {
                    self.rollback_partial_pod_start(
                        &key,
                        &sandbox_id,
                        "app container create failed",
                    )
                    .await;
                    return Err(anyhow::anyhow!(
                        "failed to create container {}: {:#}",
                        container_name,
                        e
                    ));
                }
            };

            let _ = self
                .events
                .emit_pod_event(
                    &key,
                    "Normal",
                    "Created",
                    &format!("Created container {}", container_name),
                    "klights-kubelet",
                    &self.config.node_name,
                )
                .await;

            started_containers.push((container.clone(), container_id));
        }

        if cancel.is_cancelled() {
            self.rollback_partial_pod_start(&key, &sandbox_id, "startup cancelled after create")
                .await;
            return Ok(PodStartResult::Cancelled);
        }

        if started_containers.is_empty() && !container_statuses.is_empty() {
            self.write_pod_status(
                &key,
                serde_json::json!({
                    "phase": "Pending",
                    "podIP": assignment.pod_ip,
                    "hostIP": assignment.host_ip,
                    "containerStatuses": container_statuses,
                    "initContainerStatuses": init_container_statuses,
                }),
            )
            .await?;
            return Ok(PodStartResult::Terminal(format!(
                "All {} app container(s) failed with CreateContainerConfigError; pod cannot start",
                containers.len()
            )));
        }

        if !started_containers.is_empty() {
            let mut waiting_statuses = container_statuses;
            waiting_statuses.extend(started_containers.iter().map(|(container, container_id)| {
                let container_name = container
                    .get("name")
                    .and_then(|n| n.as_str())
                    .unwrap_or("container");
                let image = container
                    .get("image")
                    .and_then(|i| i.as_str())
                    .unwrap_or("nginx:latest");
                serde_json::json!({
                    "name": container_name,
                    "containerID": format!("containerd://{}", container_id),
                    "ready": false,
                    "started": false,
                    "restartCount": 0,
                    "state": { "waiting": { "reason": "ContainerCreating" } },
                    "image": image,
                    "imageID": "",
                })
            }));
            self.write_pod_status(
                &key,
                serde_json::json!({
                    "phase": "Pending",
                    "podIP": assignment.pod_ip,
                    "hostIP": assignment.host_ip,
                    "containerStatuses": waiting_statuses,
                    "initContainerStatuses": init_container_statuses,
                }),
            )
            .await?;
        }

        for (container, container_id) in &started_containers {
            let container_name = container
                .get("name")
                .and_then(|n| n.as_str())
                .unwrap_or("container");

            if let Err(e) = self.cri.start_container(container_id).await {
                let _ = self
                    .events
                    .emit_pod_event(
                        &key,
                        "Warning",
                        "Failed",
                        &format!(
                            "Error: failed to start container {}: {:#}",
                            container_name, e
                        ),
                        "klights-kubelet",
                        &self.config.node_name,
                    )
                    .await;
                self.rollback_partial_pod_start(&key, &sandbox_id, "app container start failed")
                    .await;
                return Err(anyhow::anyhow!(
                    "failed to start container {}: {:#}",
                    container_name,
                    e
                ));
            }

            let _ = self
                .events
                .emit_pod_event(
                    &key,
                    "Normal",
                    "Started",
                    &format!("Started container {}", container_name),
                    "klights-kubelet",
                    &self.config.node_name,
                )
                .await;

            // PostStart lifecycle hook.
            if let Some(post_start) = container.pointer("/lifecycle/postStart") {
                match self
                    .hooks
                    .execute_post_start(container_id, &assignment.pod_ip, post_start, container)
                    .await
                {
                    Ok(crate::kubelet::pod_runtime::hooks::HookOutcome::Succeeded) => {
                        tracing::debug!(container = container_name, "postStart hook succeeded");
                    }
                    Ok(crate::kubelet::pod_runtime::hooks::HookOutcome::Failed(msg)) => {
                        let _ = self
                            .events
                            .emit_pod_event(
                                &key,
                                "Warning",
                                "FailedPostStartHook",
                                &format!(
                                    "postStart hook failed for container {}: {}",
                                    container_name, msg
                                ),
                                "klights-kubelet",
                                &self.config.node_name,
                            )
                            .await;
                        let _ = self.cri.stop_container(container_id, 30).await;
                        return Ok(PodStartResult::Failed(format!(
                            "postStart hook failed for container {}: {}",
                            container_name, msg
                        )));
                    }
                    Err(e) => {
                        let _ = self
                            .events
                            .emit_pod_event(
                                &key,
                                "Warning",
                                "FailedPostStartHook",
                                &format!(
                                    "postStart hook failed for container {}: {:#}",
                                    container_name, e
                                ),
                                "klights-kubelet",
                                &self.config.node_name,
                            )
                            .await;
                        let _ = self.cri.stop_container(container_id, 30).await;
                        return Ok(PodStartResult::Failed(format!(
                            "postStart hook failed for container {}: {:#}",
                            container_name, e
                        )));
                    }
                }
            }
        }

        Ok(PodStartResult::Started {
            sandbox_id: Some(sandbox_id),
        })
    }

    async fn stop_pod(
        &self,
        key: PodRuntimeKey,
        pod: Option<serde_json::Value>,
        sandbox_id: Option<String>,
    ) -> anyhow::Result<()> {
        // Stop probes by UID.
        let _ = self.probes.stop_probes(&key).await;

        // Orphan cleanup may not have a deleted Pod snapshot. Delegate to the
        // focused helper, which resolves the sandbox(es) via hint → store →
        // CRI-by-UID and tears them down before clearing the slot (HR #11).
        if pod.is_none() {
            return self.stop_orphan_pod(&key, sandbox_id).await;
        }

        let pod = pod.unwrap();

        // Verify pod UID matches key UID.
        let pod_uid = pod
            .pointer("/metadata/uid")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        if pod_uid != key.uid {
            tracing::warn!(
                namespace = key.namespace,
                name = key.name,
                key_uid = key.uid,
                pod_uid = pod_uid,
                "UID mismatch in stop_pod"
            );
        }

        // Node ownership check: only clean up CRI/CNI/volumes for pods
        // owned by this node. Cross-node deletes must not release network
        // or clear sandbox rows on a node that doesn't own the pod.
        //
        // The refusal is returned as a typed `PodOwnershipError` so the
        // lifecycle executor can classify it as terminal/non-retryable
        // (HR#11). An unscheduled (`spec.nodeName` absent) or other-node
        // Pod can never be cleaned up locally; retrying would spin the
        // actor forever (P0 high-CPU StopPod loop).
        let owned_by_this_node = self.node_view.owns_pod_runtime(&pod);
        if !owned_by_this_node {
            let ownership = PodOwnershipError::from_pod_node_name(self.node_view.node_name(), &pod);
            let target_node = ownership.target_node.as_deref().unwrap_or("<unscheduled>");
            tracing::warn!(
                namespace = %key.namespace,
                name = %key.name,
                uid = %key.uid,
                local_node = %self.node_view.node_name(),
                target_node = %target_node,
                "refusing Pod cleanup on non-owner node"
            );
            return Err(anyhow::Error::new(ownership));
        }

        // --- Sandbox resolution ladder ---
        // Resolve sandbox_id via store → annotation → CRI list when not
        // provided by the caller (e.g., watch-driven deletes).
        let sandbox_id: Option<String> = if let Some(ref id) = sandbox_id {
            if !id.is_empty() {
                Some(id.clone())
            } else {
                self.resolve_sandbox_id_for_stop(&key, &pod).await
            }
        } else {
            self.resolve_sandbox_id_for_stop(&key, &pod).await
        };

        // --- PreStop lifecycle hooks ---
        // Execute before any container is stopped, giving each container
        // a chance to drain connections or flush state.
        let spec_containers = pod
            .get("spec")
            .and_then(|s| s.get("containers"))
            .and_then(|c| c.as_array())
            .cloned()
            .unwrap_or_default();
        let status_name_by_id = pod_status_container_name_by_id(&pod);

        let grace_period_seconds = pod
            .pointer("/spec/terminationGracePeriodSeconds")
            .and_then(|v| v.as_i64())
            .unwrap_or(30);

        let mut container_ids: Vec<String> = Vec::new();
        let mut prestop_container_ids: Vec<String> = Vec::new();
        if let Some(sandbox_id) = sandbox_id.as_deref() {
            let containers = self
                .container_control
                .list_containers(Some(sandbox_id))
                .await
                .unwrap_or_else(|e| {
                    tracing::warn!(
                        namespace = key.namespace,
                        name = key.name,
                        uid = key.uid,
                        sandbox_id = sandbox_id,
                        "failed to list containers for pod stop: {:#}",
                        e
                    );
                    Vec::new()
                });
            if containers.is_empty() {
                container_ids.extend(status_name_by_id.keys().cloned());
            } else {
                for (container_id, state) in containers {
                    if state.is_running() {
                        prestop_container_ids.push(container_id.clone());
                    }
                    container_ids.push(container_id);
                }
            }
        }

        let pod_ip = pod_status_ip(&pod);
        let mut seen_pre_stop_ids = std::collections::HashSet::new();
        for container_id in prestop_container_ids
            .iter()
            .filter(|id| seen_pre_stop_ids.insert((*id).clone()))
        {
            let mut container_name = status_name_by_id.get(container_id).cloned();
            if container_name.is_none() {
                container_name = self
                    .cri
                    .container_status(container_id)
                    .await
                    .ok()
                    .and_then(|response| response.status)
                    .and_then(|status| status.metadata.map(|metadata| metadata.name))
                    .filter(|name| !name.is_empty());
            }
            let Some(container_name) = container_name else {
                continue;
            };
            let Some(container) = spec_containers.iter().find(|container| {
                container.get("name").and_then(|n| n.as_str()) == Some(&container_name)
            }) else {
                continue;
            };
            let Some(hook) = container.pointer("/lifecycle/preStop") else {
                continue;
            };
            match self
                .hooks
                .execute_pre_stop(container_id, pod_ip, hook, container)
                .await
            {
                Ok(HookOutcome::Succeeded) | Ok(HookOutcome::Failed(_)) => {}
                Err(e) => {
                    tracing::warn!(container = container_name, "preStop hook error: {:#}", e);
                }
            }
        }

        if let Some(sandbox_id) = sandbox_id.as_deref() {
            let mut seen_container_ids = std::collections::HashSet::new();
            for container_id in container_ids
                .iter()
                .filter(|id| seen_container_ids.insert((*id).clone()))
            {
                let _ = self
                    .cri
                    .stop_container(container_id, grace_period_seconds)
                    .await;
                let _ = self.cri.remove_container(container_id).await;
            }

            // Stop and remove sandbox.
            let _ = self.cri.stop_pod_sandbox(sandbox_id).await;
            let _ = self.cri.remove_pod_sandbox(sandbox_id).await;

            // Release CNI network. (cgroup teardown is UID-keyed and runs
            // unconditionally in cleanup_pod_local_artifacts below.)
            let _ = self.network.release_sandbox_network(&key, sandbox_id).await;

            // Delete the sandbox row after CNI release. Real network cleanup
            // uses the UID-qualified row as the authorization witness.
            let _ = self.store.delete_sandbox(&key).await;
        } else {
            tracing::warn!(
                namespace = %key.namespace,
                name = %key.name,
                uid = %key.uid,
                "no sandbox id resolved for pod stop; skipping CRI and CNI teardown"
            );
        }

        // Remove hostPort rules.
        let _ = self.hostports.remove_host_ports(&key, &pod).await;

        self.cleanup_pod_local_artifacts(&key).await;

        // Clear pod slot by UID.
        let _ = self.slot_admission.clear_slot(&key).await;

        Ok(())
    }

    async fn finalize_startup(
        &self,
        key: PodRuntimeKey,
        pod: Option<serde_json::Value>,
        sandbox_id_hint: Option<String>,
    ) -> anyhow::Result<PodFinalizeStartupResult> {
        let live_resource = match self
            .repository
            .get_pod_for_uid(&key.namespace, &key.name, &key.uid)
            .await
        {
            Ok(resource) => resource,
            Err(e) if pod.is_some() => {
                tracing::warn!(
                    namespace = key.namespace,
                    name = key.name,
                    uid = key.uid,
                    "failed to read pod for startup finalization; using actor snapshot: {e:#}"
                );
                None
            }
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "failed to read pod for startup finalization: {:#}",
                    e
                ));
            }
        };
        let pod = match live_resource
            .as_ref()
            .map(|resource| resource.data.as_ref())
        {
            Some(live_pod) => live_pod,
            None => match pod.as_ref() {
                Some(snapshot) => snapshot,
                None => return Ok(PodFinalizeStartupResult::Unconfirmed), // pod gone
            },
        };

        let pod_uid = pod
            .pointer("/metadata/uid")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if pod_uid != key.uid {
            return Ok(PodFinalizeStartupResult::Unconfirmed); // UID mismatch
        }

        let phase = pod
            .pointer("/status/phase")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let has_published_pod_ip = pod
            .pointer("/status/podIP")
            .and_then(|v| v.as_str())
            .is_some_and(|ip| !ip.trim().is_empty())
            || pod
                .pointer("/status/podIPs/0/ip")
                .and_then(|v| v.as_str())
                .is_some_and(|ip| !ip.trim().is_empty());
        if phase != "Running" || !has_published_pod_ip {
            return Ok(PodFinalizeStartupResult::Unconfirmed);
        }

        let sandbox_id =
            crate::kubelet::pod_runtime::startup_finalization::resolve_startup_sandbox_id(
                self.store.as_ref(),
                &key,
                sandbox_id_hint.as_deref(),
                pod,
            )
            .await;
        let sandbox_id = match sandbox_id {
            Some(id) => id,
            None => {
                tracing::warn!(
                    namespace = key.namespace,
                    name = key.name,
                    uid = key.uid,
                    "no sandbox id found for confirmed Running pod"
                );
                return Ok(PodFinalizeStartupResult::Unconfirmed);
            }
        };

        match self
            .probes
            .record_started_sandbox(&key, &sandbox_id)
            .await?
        {
            StartupFinalizationAction::AlreadyFinalized => {
                return Ok(PodFinalizeStartupResult::Confirmed { sandbox_id });
            }
            StartupFinalizationAction::RunFinalizers => {}
        }

        if let Err(error) = self.probes.start_probes(&key, &sandbox_id, pod).await {
            tracing::warn!(
                namespace = %key.namespace,
                name = %key.name,
                uid = %key.uid,
                sandbox_id = %sandbox_id,
                "failed to start probes during startup finalization: {error:#}"
            );
            return Ok(PodFinalizeStartupResult::Unconfirmed);
        }
        self.probes
            .mark_started_sandbox_finalized(&key, &sandbox_id)
            .await?;
        Ok(PodFinalizeStartupResult::Confirmed { sandbox_id })
    }

    async fn finalize_deletion(
        &self,
        key: PodRuntimeKey,
    ) -> anyhow::Result<PodDeletionFinalizeResult> {
        let result = self.finalizer.finalize_after_actor_cleanup(&key).await?;
        if matches!(result, PodDeletionFinalizeResult::DeletedOrAlreadyGone) {
            self.status_emitter.forget(&key);
        }
        Ok(result)
    }

    async fn reconcile_runtime(
        &self,
        key: PodRuntimeKey,
        hint: RuntimeReconcileHint,
    ) -> anyhow::Result<()> {
        let resource = self
            .repository
            .get_pod_for_uid(&key.namespace, &key.name, &key.uid)
            .await
            .map_err(|e| anyhow::anyhow!("failed to read pod for runtime reconcile: {e:#}"))?;
        let Some(resource) = resource else {
            return Ok(());
        };

        if self
            .enforce_active_deadline_if_exceeded(&key, &resource.data, resource.resource_version)
            .await?
        {
            return Ok(());
        }

        // 1. Read sandbox id.
        let sandbox_id = match self.store.get_sandbox_id(&key).await? {
            Some(id) => id,
            None => return Ok(()),
        };

        // 2. List containers in the sandbox. Fast-exit / lossy scheduling can
        // race the reconcile so the listing is empty or stale by the time it
        // runs. When a CRI event carried a concrete container id, fall back to
        // reading its status directly so the pod does not stay API-visible as
        // Pending/ContainerCreating.
        let mut containers = self
            .container_control
            .list_containers(Some(&sandbox_id))
            .await
            .unwrap_or_default();
        // Augment with ALL observed container IDs from the hint, not just when
        // the listing is empty. Multi-container pods and partial listings miss
        // exited containers that have already been removed from the sandbox;
        // the hint carries every CRI event's container ID so we can fetch their
        // terminal state directly even when they're absent from the listing.
        for container_id in hint.container_ids() {
            if containers.iter().any(|(id, _)| id == container_id) {
                continue; // Already present in the listing — skip the direct fetch.
            }
            if let Some(state) = self
                .runtime_state_from_container_status(container_id)
                .await?
            {
                containers.push((container_id.to_string(), state));
            }
            // If the hinted ID has no runtime status, treat as observation miss
            // and skip without regressing the Pod's existing phase/container state.
        }

        // 3. Build phase and container statuses from CRI state plus the Pod spec.
        let (mut phase, mut container_statuses) = self
            .reconcile_container_statuses_from_pod_spec(&key, &resource.data, &containers)
            .await;
        if let Some(restarted_statuses) = self
            .restart_exited_containers_if_needed(
                &key,
                &resource.data,
                &sandbox_id,
                &container_statuses,
            )
            .await?
        {
            container_statuses = restarted_statuses;
            phase = "Running".to_string();
        }
        let status = serde_json::json!({
            "phase": phase,
            "containerStatuses": container_statuses,
        });
        // Route the computed status through the central Pod status merge
        // policy before emission so a stale reconcile cannot regress terminal
        // phase/container state (e.g. a CRI list racing the reconcile seeing
        // an empty sandbox after the pod already Succeeded).
        let mut status = status;
        crate::pod_status_merge::merge_pod_status_for_update(
            "v1",
            "Pod",
            &resource.data,
            &mut status,
            crate::pod_status_merge::PodStatusUpdateSource::KubeletRuntime,
        );
        let emit_key = key.clone();
        let emitted = self
            .status_emitter
            .emit_if_changed(&key, status, |status| async move {
                self.cluster_view
                    .forward_pod_status(&emit_key, status)
                    .await?;
                Ok::<(), anyhow::Error>(())
            })
            .await?;
        if !emitted {
            tracing::debug!(
                target: "klights::pod_status",
                namespace = %key.namespace,
                pod = %key.name,
                uid = %key.uid,
                "runtime reconcile status emit suppressed because actor memory cache already has identical status"
            );
        }

        Ok(())
    }

    async fn reconcile_cri_leftovers(&self, key: PodRuntimeKey) -> anyhow::Result<()> {
        // CRI leftover cleanup is node-local: only clean up leftovers for
        // pods owned by this node.
        let resource = self
            .repository
            .get_pod_for_uid(&key.namespace, &key.name, &key.uid)
            .await
            .map_err(|e| anyhow::anyhow!("failed to read pod for CRI leftover check: {:#}", e))?;
        let Some(resource) = resource else {
            return Ok(()); // Pod already gone, nothing to reconcile.
        };
        if !self.node_view.owns_pod_runtime(&resource.data) {
            return Ok(());
        }
        // Node-local CRI leftover cleanup goes here (future task).
        let _ = key;
        Ok(())
    }

    async fn reconcile_ephemeral(
        &self,
        key: PodRuntimeKey,
        pod: Option<serde_json::Value>,
    ) -> anyhow::Result<()> {
        let Some(pod) = pod else {
            return Ok(());
        };

        // Verify UID matches.
        let pod_uid = pod
            .pointer("/metadata/uid")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if pod_uid != key.uid {
            tracing::warn!(
                namespace = key.namespace,
                name = key.name,
                key_uid = key.uid,
                pod_uid = pod_uid,
                "UID mismatch in reconcile_ephemeral"
            );
            return Ok(());
        }

        // Node ownership check.
        if !self.node_view.owns_pod_runtime(&pod) {
            return Ok(());
        }

        // Ephemeral containers: check spec.ephemeralContainers and
        // reconcile against running containers in the sandbox.
        let ephemeral_containers = pod
            .pointer("/spec/ephemeralContainers")
            .and_then(|v| v.as_array())
            .map(|a| a.as_slice())
            .unwrap_or(&[]);

        if ephemeral_containers.is_empty() {
            return Ok(());
        }

        let sandbox_id = match self.store.get_sandbox_id(&key).await? {
            Some(id) => id,
            None => return Ok(()),
        };

        let runtime_containers = self
            .container_control
            .list_containers(Some(&sandbox_id))
            .await
            .unwrap_or_default();

        let mut runtime_by_name: std::collections::HashMap<String, (String, String, String, i64)> =
            std::collections::HashMap::new();
        for (container_id, _) in runtime_containers {
            let status = match self.cri.container_status(&container_id).await {
                Ok(response) => response.status,
                Err(e) => {
                    tracing::warn!(
                        namespace = key.namespace,
                        name = key.name,
                        uid = key.uid,
                        container_id = container_id,
                        "Failed to inspect runtime container for ephemeral reconcile: {}",
                        e
                    );
                    None
                }
            };
            let Some(status) = status else {
                continue;
            };
            let container_name = status
                .metadata
                .as_ref()
                .map(|metadata| metadata.name.clone())
                .unwrap_or_default();
            if container_name.is_empty() {
                continue;
            }
            let runtime_image = status
                .image
                .as_ref()
                .map(|image| image.image.clone())
                .unwrap_or_default();
            let image_ref = if status.image_ref.is_empty() {
                status.image_id.clone()
            } else {
                status.image_ref.clone()
            };
            match runtime_by_name.get(&container_name) {
                Some((_, _, _, existing_created_at))
                    if status.created_at <= *existing_created_at =>
                {
                    continue;
                }
                _ => {
                    runtime_by_name.insert(
                        container_name,
                        (status.id, runtime_image, image_ref, status.created_at),
                    );
                }
            }
        }

        let pod_ip = pod
            .pointer("/status/podIP")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let dns_ip = crate::controllers::coredns::derive_dns_service_ip(&self.config.service_cidr);
        let kubernetes_service_ip = crate::controllers::kube_service::derive_kubernetes_service_ip(
            &self.config.service_cidr,
        );
        let default_spec = serde_json::json!({});
        let pod_spec = pod.get("spec").unwrap_or(&default_spec);
        let sandbox_config = build_sandbox_config_with_dns_policy(
            &key.name,
            &key.namespace,
            pod_ip,
            &key.uid,
            &self.config.containerd_namespace,
            &dns_ip,
            pod_spec,
        );
        let mut volume_paths: Option<std::collections::HashMap<String, String>> = None;

        for ec in ephemeral_containers {
            let ec_name = ec.get("name").and_then(|n| n.as_str()).unwrap_or("");
            if ec_name.is_empty() || runtime_by_name.contains_key(ec_name) {
                continue;
            }

            if let Err(message) =
                crate::kubelet::pod_container_config::check_run_as_non_root(&pod, ec, ec_name)
            {
                tracing::warn!(
                    namespace = key.namespace,
                    name = key.name,
                    uid = key.uid,
                    container = ec_name,
                    "Ephemeral container rejected by runAsNonRoot: {}",
                    message
                );
                continue;
            }

            if volume_paths.is_none() {
                let paths = self
                    .volumes
                    .process_volumes(&key, &pod)
                    .await
                    .map_err(|e| {
                        anyhow::anyhow!(
                            "failed to process volumes for ephemeral container {}: {:#}",
                            ec_name,
                            e
                        )
                    })?;
                if pod
                    .pointer("/spec/securityContext/fsGroup")
                    .and_then(|v| v.as_u64())
                    .is_some()
                {
                    let _ = self.filesystem.apply_fs_group(&key, &pod).await;
                }
                volume_paths = Some(paths);
            }

            let image = ec.get("image").and_then(|i| i.as_str()).unwrap_or("");
            if !image.is_empty() {
                let normalized_image =
                    crate::kubelet::pod_runtime::images::normalize_image_name(image);
                let pull_policy = crate::kubelet::pod_runtime::images::effective_pull_policy(
                    ec,
                    &normalized_image,
                );
                let needs_pull = if pull_policy == "Never" {
                    false
                } else if pull_policy == "IfNotPresent" {
                    !matches!(self.cri.image_status(&normalized_image).await, Ok(true))
                } else {
                    true
                };
                if needs_pull {
                    self.cri.pull_image(&normalized_image).await.map_err(|e| {
                        anyhow::anyhow!(
                            "failed to pull ephemeral container image \"{}\": {:#}",
                            normalized_image,
                            e
                        )
                    })?;
                }
            }

            let empty_volume_paths = std::collections::HashMap::new();
            let paths = volume_paths.as_ref().unwrap_or(&empty_volume_paths);
            let container_config = self
                .build_container_config_with_env(ContainerConfigBuildRequest {
                    key: &key,
                    pod: &pod,
                    container: ec,
                    container_name: ec_name,
                    kubernetes_service_ip: &kubernetes_service_ip,
                    volume_paths: paths,
                    ignore_mount_errors: false,
                })
                .await
                .map_err(|e| {
                    anyhow::anyhow!("invalid ephemeral container {} config: {:#}", ec_name, e)
                })?;

            let container_id = self
                .cri
                .create_container(container_config, &sandbox_id, sandbox_config.clone())
                .await
                .map_err(|e| {
                    anyhow::anyhow!("failed to create ephemeral container {}: {:#}", ec_name, e)
                })?;
            self.cri.start_container(&container_id).await.map_err(|e| {
                anyhow::anyhow!("failed to start ephemeral container {}: {:#}", ec_name, e)
            })?;

            let _ = self
                .events
                .emit_pod_event(
                    &key,
                    "Normal",
                    "Started",
                    &format!("Started container {}", ec_name),
                    "klights-kubelet",
                    &self.config.node_name,
                )
                .await;

            runtime_by_name.insert(
                ec_name.to_string(),
                (
                    container_id,
                    image.to_string(),
                    String::new(),
                    chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default(),
                ),
            );
        }

        let existing_statuses = pod
            .pointer("/status/ephemeralContainerStatuses")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let existing_by_name: std::collections::HashMap<String, serde_json::Value> =
            existing_statuses
                .iter()
                .filter_map(|status| {
                    status
                        .get("name")
                        .and_then(|name| name.as_str())
                        .map(|name| (name.to_string(), status.clone()))
                })
                .collect();

        let mut new_statuses = Vec::with_capacity(ephemeral_containers.len());
        for ec in ephemeral_containers {
            let ec_name = ec.get("name").and_then(|n| n.as_str()).unwrap_or("");
            if ec_name.is_empty() {
                continue;
            }

            if let Some((container_id, runtime_image, runtime_image_ref, _)) =
                runtime_by_name.get(ec_name)
            {
                let status = match self.cri.container_status(container_id).await {
                    Ok(response) => response.status,
                    Err(e) => {
                        tracing::warn!(
                            namespace = key.namespace,
                            name = key.name,
                            uid = key.uid,
                            container = ec_name,
                            container_id = container_id,
                            "Failed to read ephemeral container status: {}",
                            e
                        );
                        None
                    }
                };
                let state = status
                    .as_ref()
                    .map(|status| status.state)
                    .unwrap_or(k8s_cri::v1::ContainerState::ContainerCreated as i32);
                let started_at = status.as_ref().map(|status| status.started_at).unwrap_or(0);
                let finished_at = status
                    .as_ref()
                    .map(|status| status.finished_at)
                    .unwrap_or(0);
                let exit_code = status.as_ref().map(|status| status.exit_code).unwrap_or(0);
                let status_image = status
                    .as_ref()
                    .and_then(|status| status.image.as_ref())
                    .map(|image| image.image.clone())
                    .unwrap_or_default();
                let status_image_ref = status
                    .as_ref()
                    .map(|status| {
                        if status.image_ref.is_empty() {
                            status.image_id.clone()
                        } else {
                            status.image_ref.clone()
                        }
                    })
                    .unwrap_or_default();
                let image = if !status_image.is_empty() {
                    status_image
                } else if !runtime_image.is_empty() {
                    runtime_image.clone()
                } else {
                    ec.get("image")
                        .and_then(|image| image.as_str())
                        .unwrap_or("")
                        .to_string()
                };
                let image_ref = if !status_image_ref.is_empty() {
                    status_image_ref
                } else {
                    runtime_image_ref.clone()
                };
                new_statuses.push(build_ephemeral_container_status(
                    EphemeralContainerStatusInput {
                        container_name: ec_name,
                        container_id: Some(container_id),
                        state,
                        started_at_ns: started_at,
                        finished_at_ns: finished_at,
                        exit_code,
                        image: &image,
                        image_ref: &image_ref,
                    },
                ));
                continue;
            }

            if let Some(existing) = existing_by_name.get(ec_name) {
                new_statuses.push(existing.clone());
                continue;
            }

            new_statuses.push(build_ephemeral_container_status(
                EphemeralContainerStatusInput {
                    container_name: ec_name,
                    container_id: None,
                    state: k8s_cri::v1::ContainerState::ContainerCreated as i32,
                    started_at_ns: 0,
                    finished_at_ns: 0,
                    exit_code: 0,
                    image: ec
                        .get("image")
                        .and_then(|image| image.as_str())
                        .unwrap_or(""),
                    image_ref: "",
                },
            ));
        }

        if existing_statuses == new_statuses {
            return Ok(());
        }

        let mut attempt = 0u8;
        while attempt < 5 {
            let Some(current) = self
                .repository
                .get_pod_for_uid(&key.namespace, &key.name, &key.uid)
                .await?
            else {
                return Ok(());
            };
            match self
                .repository
                .apply_ephemeral_container_statuses_for_uid(
                    &key.namespace,
                    &key.name,
                    &key.uid,
                    new_statuses.clone(),
                    Some(current.resource_version),
                )
                .await
            {
                Ok(_) => return Ok(()),
                Err(e) if crate::datastore::errors::is_conflict_error(&e) && attempt < 4 => {
                    attempt += 1;
                    let _ = self
                        .supervisor
                        .sleep(
                            "ephemeral_status_conflict_backoff",
                            std::time::Duration::from_millis(50 * attempt as u64),
                        )
                        .await;
                    continue;
                }
                Err(e) => {
                    return Err(anyhow::anyhow!(
                        "failed to persist ephemeral container status: {:#}",
                        e
                    ));
                }
            }
        }

        Ok(())
    }

    async fn handle_lifecycle_command(&self, command: LifecycleCommand) -> anyhow::Result<()> {
        use crate::kubelet::lifecycle::LifecycleCommand;

        match &command {
            LifecycleCommand::ReadinessChanged {
                pod_uid,
                namespace,
                pod_name,
                container_name,
                ready,
            } => {
                self.handle_readiness_changed(namespace, pod_name, pod_uid, container_name, *ready)
                    .await?;
            }
            LifecycleCommand::RestartRequested {
                pod_uid,
                namespace,
                pod_name,
                container_name,
                reason,
            } => {
                tracing::info!(
                    namespace = namespace,
                    pod = pod_name,
                    uid = pod_uid,
                    container = container_name,
                    reason = format!("{:?}", reason),
                    "restart requested"
                );
                let key = PodRuntimeKey::new(namespace, pod_name, pod_uid);
                let Some(pod_resource) = self
                    .repository
                    .get_pod_for_uid(namespace, pod_name, pod_uid)
                    .await?
                else {
                    return Ok(());
                };
                let pod = pod_resource.data.as_ref().clone();
                let Some(sandbox_id) = self.store.get_sandbox_id(&key).await? else {
                    tracing::warn!(
                        namespace = namespace,
                        pod = pod_name,
                        uid = pod_uid,
                        "restart requested but sandbox id is missing"
                    );
                    return Ok(());
                };

                let mut old_container_id = pod_status_container_id_by_name(&pod, container_name);
                if old_container_id.is_none() {
                    let containers = self
                        .container_control
                        .list_containers(Some(&sandbox_id))
                        .await
                        .unwrap_or_default();
                    for (candidate_id, _state) in containers {
                        let runtime_name = self
                            .cri
                            .container_status(&candidate_id)
                            .await
                            .ok()
                            .and_then(|response| response.status)
                            .and_then(|status| status.metadata.map(|metadata| metadata.name))
                            .filter(|name| !name.is_empty());
                        if runtime_name.as_deref() == Some(container_name.as_str()) {
                            old_container_id = Some(candidate_id);
                            break;
                        }
                    }
                }
                let Some(old_container_id) = old_container_id else {
                    tracing::warn!(
                        namespace = namespace,
                        pod = pod_name,
                        uid = pod_uid,
                        container = container_name,
                        "restart requested but runtime container id is missing"
                    );
                    return Ok(());
                };

                let _ = self.cri.stop_container(&old_container_id, 10).await;
                let stopped_status = self
                    .cri
                    .container_status(&old_container_id)
                    .await
                    .ok()
                    .and_then(|response| response.status);
                let _ = self
                    .repository
                    .note_container_restart_for_uid(
                        namespace,
                        pod_name,
                        pod_uid,
                        container_name,
                        crate::kubelet::pod_runtime::status_helpers::restart_last_state_from_runtime_status(
                            stopped_status.as_ref(),
                        ),
                        None,
                    )
                    .await;
                self.cri.remove_container(&old_container_id).await?;

                let volume_paths = self.volumes.process_volumes(&key, &pod).await?;
                if pod
                    .pointer("/spec/securityContext/fsGroup")
                    .and_then(|v| v.as_u64())
                    .is_some()
                {
                    let _ = self.filesystem.apply_fs_group(&key, &pod).await;
                }

                let Some(container) = find_pod_container_spec(&pod, container_name) else {
                    tracing::warn!(
                        namespace = namespace,
                        pod = pod_name,
                        uid = pod_uid,
                        container = container_name,
                        "restart requested but container spec is missing"
                    );
                    return Ok(());
                };
                let dns_ip =
                    crate::controllers::coredns::derive_dns_service_ip(&self.config.service_cidr);
                let kubernetes_service_ip =
                    crate::controllers::kube_service::derive_kubernetes_service_ip(
                        &self.config.service_cidr,
                    );
                let container_config = self
                    .build_container_config_with_env(ContainerConfigBuildRequest {
                        key: &key,
                        pod: &pod,
                        container,
                        container_name,
                        kubernetes_service_ip: &kubernetes_service_ip,
                        volume_paths: &volume_paths,
                        ignore_mount_errors: false,
                    })
                    .await?;
                let default_spec = serde_json::json!({});
                let pod_spec = pod.get("spec").unwrap_or(&default_spec);
                let sandbox_config = build_sandbox_config_with_dns_policy(
                    pod_name,
                    namespace,
                    pod_status_ip(&pod),
                    pod_uid,
                    &self.config.containerd_namespace,
                    &dns_ip,
                    pod_spec,
                );

                let new_id = self
                    .cri
                    .create_container(container_config, &sandbox_id, sandbox_config)
                    .await?;
                self.cri.start_container(&new_id).await?;
            }
            LifecycleCommand::StartupPassed {
                pod_uid,
                namespace,
                pod_name,
                container_name,
            } => {
                tracing::info!(
                    namespace = namespace,
                    pod = pod_name,
                    uid = pod_uid,
                    container = container_name,
                    "startup probe passed"
                );
                // Startup passed: the container is now ready for liveness probes.
                // The probe manager handles this transition internally.
            }
        }
        Ok(())
    }

    async fn schedule_retry(
        &self,
        key: PodRuntimeKey,
        delay: std::time::Duration,
        reply_to: LifecycleReplyHandle,
    ) -> anyhow::Result<()> {
        crate::kubelet::pod_runtime::retry::schedule_retry(&self.supervisor, key, delay, reply_to)
            .await
    }

    async fn schedule_start_pod_retry(
        &self,
        key: PodRuntimeKey,
        delay: std::time::Duration,
        error_message: String,
        attempt: u32,
        reply_to: LifecycleReplyHandle,
    ) -> anyhow::Result<()> {
        crate::kubelet::pod_runtime::retry::schedule_start_pod_retry(
            crate::kubelet::pod_runtime::retry::RetryRuntimeContext {
                repository: self.repository.as_ref(),
                events: self.events.as_ref(),
                supervisor: &self.supervisor,
                node_name: &self.config.node_name,
            },
            crate::kubelet::pod_runtime::retry::StartPodRetryRequest {
                key,
                delay,
                error_message,
                attempt,
            },
            reply_to,
        )
        .await
    }
}
