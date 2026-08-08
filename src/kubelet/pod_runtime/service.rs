pub use crate::kubelet::pod_runtime::service_dependencies::RealPodRuntimeServiceDependencies;
pub use crate::kubelet::pod_runtime::slot_admission::PodSlotAdmissionRequest;
pub use crate::kubelet::pod_runtime::types::{
    PodDeletionFinalizeResult, PodFinalizeStartupResult, PodOwnershipError, PodRuntimeKey,
    PodStartResult,
};
use klights_kubelet::lifecycle::LifecycleCommand;
use klights_kubelet::pod_lifecycle_router::LifecycleReplyHandle;
pub use klights_kubelet::runtime::{PodRuntimeService, RuntimeReconcileHint};
use tokio_util::sync::CancellationToken;

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

// --- Runtime configuration ---

/// Scalar configuration for `RealPodRuntimeService`.
/// Contains only static per-node values; does not include port references.
#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    pub node_name: String,
    pub service_cidr: String,
    pub containerd_namespace: String,
    pub sandbox_inputs: klights_kubelet::pod_sandbox_config::SandboxRuntimeInputs,
    pub node_capacity: klights_kubelet::node_capacity::NodeCapacity,
    pub paths: klights_kubelet::runtime_paths::KubeletRuntimePaths,
}

// --- RealPodRuntimeService ---

use std::sync::Arc;

use crate::kubelet::pod_cluster_runtime::{ClusterRuntimeView, NodeRuntimeView};
use crate::kubelet::pod_runtime::active_deadline::ActiveDeadlineEnforcer;
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
    EphemeralContainerStatusInput, build_ephemeral_container_status,
    pod_status_container_name_by_id, pod_status_host_ip, pod_status_ip, replace_container_status,
    restart_last_state_from_reconciled_status, restarted_running_container_status,
    runtime_status_container_id,
};
use crate::kubelet::pod_runtime::status_projection;
use crate::kubelet::pod_runtime::store::{PodRuntimeStore, PodSlotAdmission};
use crate::kubelet::pod_runtime::volumes::PodVolumeRuntime;
use crate::kubelet::pod_termination::{find_pod_container_spec, get_termination_message_path};
use klights_kubelet::pod_container_config::{
    build_container_config_with_capacity, check_run_as_non_root,
};
use klights_kubelet::pod_sandbox_config::build_sandbox_config_with_runtime_inputs;
use klights_kubelet::pod_startup_error::PodStartupErrorKind;
use klights_kubelet::pod_status_builders::{
    build_initial_pending_status, build_pod_initializing_app_statuses,
};
use klights_kubelet::runtime::cri::{
    ContainerRuntimeControl, CriRuntime, CriRuntimeContainerEventKind,
    CriRuntimeContainerEventStream,
};
use klights_supervisor::TaskSupervisor;

const INIT_CONTAINER_WAIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);
const INIT_CONTAINER_FAST_EXIT_RECHECK_DELAY: std::time::Duration =
    std::time::Duration::from_millis(50);
#[cfg(not(test))]
const POST_SANDBOX_HOSTPORT_SETUP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
#[cfg(test)]
const POST_SANDBOX_HOSTPORT_SETUP_TIMEOUT: std::time::Duration =
    std::time::Duration::from_millis(50);
#[cfg(not(test))]
const POST_SANDBOX_VOLUME_SETUP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);
#[cfg(test)]
const POST_SANDBOX_VOLUME_SETUP_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(50);

fn apply_runtime_event_hint(
    hint: &RuntimeReconcileHint,
    container_id: &str,
    state: klights_kubelet::runtime::cri::ContainerRuntimeState,
) -> klights_kubelet::runtime::cri::ContainerRuntimeState {
    if hint.event_kind(container_id) == Some(klights_kubelet::cri_events::KubeletEventKind::Started)
        && matches!(
            state,
            klights_kubelet::runtime::cri::ContainerRuntimeState::Created
                | klights_kubelet::runtime::cri::ContainerRuntimeState::Unknown
        )
    {
        klights_kubelet::runtime::cri::ContainerRuntimeState::Running
    } else {
        state
    }
}

pub(super) struct ContainerConfigBuildRequest<'a> {
    pub(super) key: &'a PodRuntimeKey,
    pub(super) pod: &'a serde_json::Value,
    pub(super) container: &'a serde_json::Value,
    pub(super) container_name: &'a str,
    pub(super) kubernetes_service_ip: &'a str,
    pub(super) volume_paths: &'a std::collections::HashMap<String, String>,
    pub(super) ignore_mount_errors: bool,
}

fn managed_hosts_file_path(
    paths: &klights_kubelet::runtime_paths::KubeletRuntimePaths,
    key: &PodRuntimeKey,
    pod: &serde_json::Value,
) -> Option<String> {
    if klights_kubelet::pod_hosts::is_host_network(pod) {
        return None;
    }

    Some(
        paths
            .containerd_hosts_dir(&key.namespace, &key.name)
            .join("hosts")
            .to_string_lossy()
            .into_owned(),
    )
}

fn append_managed_hosts_mount(mounts: &mut Vec<k8s_cri::v1::Mount>, hosts_file_path: Option<&str>) {
    let Some(host_path) = hosts_file_path else {
        return;
    };
    if klights_kubelet::pod_hosts::container_has_etc_hosts_mount(mounts) {
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

async fn stop_step_until_cancelled<T>(
    cancel: &CancellationToken,
    future: impl std::future::Future<Output = T>,
) -> Option<T> {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => None,
        result = future => Some(result),
    }
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
    pub(super) clock: Arc<dyn klights_kubelet::runtime_clock::RuntimeClock>,
    pub(super) slot_admission: Arc<dyn PodSlotAdmission>,
    pub(super) repository: Arc<dyn PodRuntimeRepository>,
    pub(super) filesystem: Arc<dyn PodFilesystem>,
    pub(super) volumes: Arc<dyn PodVolumeRuntime>,
    probes: Arc<dyn ProbeRuntime>,
    hostports: Arc<dyn HostPortRuntime>,
    events: Arc<dyn PodEventSink>,
    hooks: Arc<dyn PodHookRuntime>,
    env_source: Arc<dyn klights_kubelet::pod_env::EnvSourceReader>,
    finalizer: Arc<dyn PodDeletionFinalizer>,
    supervisor: Arc<TaskSupervisor>,
    pub(super) config: RuntimeConfig,
    node_view: Arc<dyn NodeRuntimeView>,
    cluster_view: Arc<dyn ClusterRuntimeView>,
    pub(super) status_emitter: PodStatusEmitter,
    active_deadline: ActiveDeadlineEnforcer,
}

impl RealPodRuntimeService {
    async fn enforce_active_deadline_if_exceeded(
        &self,
        key: &PodRuntimeKey,
        pod: &serde_json::Value,
        resource_version: i64,
    ) -> anyhow::Result<bool> {
        let Some(deadline_secs) =
            crate::kubelet::pod_runtime::active_deadline::exceeded_active_deadline_seconds_at(
                pod,
                self.clock.now_ms().div_euclid(1_000),
            )
        else {
            return Ok(false);
        };

        let sandbox_id = if self.node_view.owns_pod_runtime(pod) {
            self.resolve_sandbox_id_for_stop(key, pod).await?
        } else {
            None
        };
        self.active_deadline
            .enforce_exceeded(key, resource_version, deadline_secs, sandbox_id.as_deref())
            .await
    }

    /// Write Pod status through the cluster boundary `ClusterRuntimeView`.
    /// On the leader this resolves to a local repository write; on a worker it
    /// forwards to the leader — a single status path for every node role.
    pub(super) async fn write_pod_status(
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

    pub(super) async fn build_container_config_with_env(
        &self,
        request: ContainerConfigBuildRequest<'_>,
    ) -> anyhow::Result<k8s_cri::v1::ContainerConfig> {
        let resolved_env_from = klights_kubelet::pod_env::resolve_env_from_source(
            request.container,
            &request.key.namespace,
            self.env_source.as_ref(),
        )
        .await;
        let resolved_env = klights_kubelet::pod_env::resolve_env_value_from_source(
            request.container,
            &request.key.namespace,
            self.env_source.as_ref(),
        )
        .await;
        let subpath_env = klights_kubelet::pod_env::build_subpath_env_with_capacity(
            request.container,
            request.pod,
            &resolved_env_from,
            &resolved_env,
            self.config.node_capacity,
        );
        let mut container_config = build_container_config_with_capacity(
            request.container,
            request.pod,
            request.container_name,
            request.kubernetes_service_ip,
            &resolved_env_from,
            &resolved_env,
            self.config.node_capacity,
        );
        let service_envs = klights_kubelet::pod_service_envs::resolve_service_envs_from_source(
            &request.key.namespace,
            self.env_source.as_ref(),
        )
        .await;
        append_service_envs(&mut container_config, &service_envs);

        match klights_kubelet::pod_volume_manager::PodVolumeManager::build_mounts(
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

        let hosts_file_path = managed_hosts_file_path(&self.config.paths, request.key, request.pod);
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
        self.cri.stop_container(old_container_id, 10).await?;
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
        let dns_ip = klights_types::dns_service_ipv4(&self.config.service_cidr);
        let kubernetes_service_ip = klights_types::first_usable_ipv4(&self.config.service_cidr);
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
        let sandbox_config = build_sandbox_config_with_runtime_inputs(
            klights_kubelet::pod_sandbox_config::SandboxIdentity {
                pod_name: &key.name,
                namespace: &key.namespace,
                pod_uid: &key.uid,
                containerd_namespace: &self.config.containerd_namespace,
            },
            pod_status_ip(pod),
            &dns_ip,
            pod_spec,
            &self.config.sandbox_inputs,
            &self.config.paths,
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
                        self.clock.now_utc(),
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

        if let Err(record_error) = self.store.record_sandbox(key, &sandbox_id).await {
            let rollback_error = self.cri.remove_pod_sandbox(&sandbox_id).await.err();
            if let Some(rollback_error) = rollback_error {
                return Err(anyhow::anyhow!(
                    "failed to persist UID-qualified sandbox ownership: {record_error:#}; \
                     failed to roll back unowned sandbox {sandbox_id}: {rollback_error:#}"
                ));
            }
            return Err(anyhow::anyhow!(
                "failed to persist UID-qualified sandbox ownership: {record_error:#}"
            ));
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
    ) -> anyhow::Result<()> {
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
            .await?;
        let mut seen = std::collections::HashSet::new();
        for (container_id, _) in containers
            .into_iter()
            .filter(|(id, _)| seen.insert(id.clone()))
        {
            self.cri.stop_container(&container_id, 10).await?;
            self.cri.remove_container(&container_id).await?;
        }
        self.network
            .release_sandbox_network(key, sandbox_id)
            .await?;
        self.cri.stop_pod_sandbox(sandbox_id).await?;
        self.cri.remove_pod_sandbox(sandbox_id).await?;
        self.store.delete_sandbox(key).await?;
        self.cleanup_pod_local_artifacts(key).await?;
        Ok(())
    }

    /// Shared local-artifact teardown for every pod stop path (normal delete,
    /// failed-create rollback, orphan/cold-sandbox finalize). Unmounts and
    /// removes the pod's volumes first, reclaims the cgroup tree, then removes
    /// the pod filesystem root, so the recursive root removal never runs over a
    /// still-live mount. Every step derives entirely from `key` and is
    /// idempotent, so this path needs no deleted-Pod snapshot, is safe to re-run
    /// after a timed-out finalize, and never leaks the cgroup when no sandbox
    /// could be resolved.
    pub(super) async fn cleanup_pod_local_artifacts(
        &self,
        key: &PodRuntimeKey,
    ) -> anyhow::Result<()> {
        self.volumes.cleanup_volumes(key).await?;
        self.filesystem.cleanup_cgroup(key).await?;
        self.filesystem.cleanup_pod_filesystem(key).await?;
        Ok(())
    }

    /// Construct the production runtime service with all required ports.
    /// Every field is wired at construction time; no late initialization.
    pub fn new(dependencies: RealPodRuntimeServiceDependencies) -> Self {
        let RealPodRuntimeServiceDependencies {
            cri,
            container_control,
            network,
            store,
            clock,
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
        let active_deadline =
            ActiveDeadlineEnforcer::new(cri.clone(), container_control.clone(), repository.clone());
        Self {
            cri,
            container_control,
            network,
            store,
            clock,
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
            active_deadline,
        }
    }

    /// Resolve a sandbox ID for pod stop via the store → annotation → CRI
    /// ladder. Matches the legacy `resolve_sandbox_id_for_delete_with_timeout`.
    async fn resolve_sandbox_id_for_stop(
        &self,
        key: &PodRuntimeKey,
        pod: &serde_json::Value,
    ) -> anyhow::Result<Option<String>> {
        // 1. Store row.
        if let Some(id) = self.store.get_sandbox_id(key).await?
            && !id.is_empty()
        {
            return Ok(Some(id));
        }

        // 2. klights.dev/sandbox-id annotation.
        let annotation_key = "klights.dev/sandbox-id";
        if let Some(id) = pod
            .pointer("/metadata/annotations")
            .and_then(|a| a.get(annotation_key))
            .and_then(|v| v.as_str())
            && !id.is_empty()
        {
            return Ok(Some(id.to_string()));
        }

        // 3. CRI list_pod_sandboxes matched by pod UID.
        let pod_uid = pod
            .pointer("/metadata/uid")
            .and_then(|v| v.as_str())
            .unwrap_or(&key.uid);
        let sandboxes = self.cri.list_pod_sandboxes(Some(pod_uid)).await?;
        for (sandbox_id, _state) in &sandboxes {
            if !sandbox_id.is_empty() {
                return Ok(Some(sandbox_id.clone()));
            }
        }

        Ok(None)
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
                if let Some(stopped) =
                    init_container_stop_from_status(&status, self.clock.now_ms().div_euclid(1_000))
                {
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
        Ok(init_container_stop_from_status(
            &status,
            self.clock.now_ms().div_euclid(1_000),
        ))
    }
}

#[cfg(test)]
impl RealPodRuntimeService {
    pub(super) async fn stop_pod(
        &self,
        key: PodRuntimeKey,
        pod: Option<serde_json::Value>,
        sandbox_id: Option<String>,
    ) -> anyhow::Result<()> {
        let deletion_deadline = pod.as_ref().map(|pod| {
            let grace = pod
                .pointer("/spec/terminationGracePeriodSeconds")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or(30)
                .max(0);
            self.clock.now_utc() + chrono::Duration::seconds(grace)
        });
        let mode = if deletion_deadline.is_some() {
            klights_kubelet::runtime::PodStopMode::Graceful
        } else {
            klights_kubelet::runtime::PodStopMode::Forced
        };
        let result = <Self as PodRuntimeService>::stop_pod(
            self,
            klights_kubelet::runtime::PodStopRequest {
                key,
                pod,
                sandbox_id,
                deletion_deadline,
                mode,
                operation_id: 0,
                cancel: CancellationToken::new(),
            },
        )
        .await?;
        match result {
            klights_kubelet::runtime::PodStopResult::Completed => Ok(()),
            klights_kubelet::runtime::PodStopResult::Cancelled => {
                anyhow::bail!("test Pod stop unexpectedly cancelled")
            }
        }
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
        let admission_host_ports = super::hostports::pod_host_ports_from_resource(&key, &pod)?;
        if let Err(e) = self
            .hostports
            .check_host_port_admission(&admission_host_ports)
            .await
        {
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

        let dns_ip = klights_types::dns_service_ipv4(&self.config.service_cidr);
        let default_spec = serde_json::json!({});
        let pod_spec = pod.get("spec").unwrap_or(&default_spec);
        let sandbox_config = build_sandbox_config_with_runtime_inputs(
            klights_kubelet::pod_sandbox_config::SandboxIdentity {
                pod_name: &key.name,
                namespace: &key.namespace,
                pod_uid: &key.uid,
                containerd_namespace: &self.config.containerd_namespace,
            },
            "",
            &dns_ip,
            pod_spec,
            &self.config.sandbox_inputs,
            &self.config.paths,
        );
        let container_sandbox_config = sandbox_config.clone();
        let kubernetes_service_ip = klights_types::first_usable_ipv4(&self.config.service_cidr);

        let sandbox_id = match self.store.get_sandbox_id(&key).await {
            Ok(Some(existing)) if !existing.trim().is_empty() => existing,
            Ok(_) => self.create_and_record_sandbox(&key, sandbox_config).await?,
            Err(error) => {
                return Err(anyhow::anyhow!(
                    "failed to read UID-qualified sandbox ownership before pod start: {error:#}"
                ));
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
                    .await?;
                }
                return Err(anyhow::anyhow!("network assignment failed: {:#}", e));
            }
        };
        let pod = Self::pod_with_network_status(
            pod,
            assignment.pod_ip.as_str(),
            assignment.host_ip.as_str(),
        );
        let pod_host_ports = super::hostports::pod_host_ports_from_resource(&key, &pod)?;

        // --- Cancellation check: after sandbox + store + network, before hostports/containers ---
        if cancel.is_cancelled() {
            self.rollback_partial_pod_start(&key, &sandbox_id, "startup cancelled after sandbox")
                .await?;
            return Ok(PodStartResult::Cancelled);
        }

        // HostPort rules.
        let hostport_result = self
            .supervisor
            .timeout(
                "pod_start_add_hostports",
                POST_SANDBOX_HOSTPORT_SETUP_TIMEOUT,
                self.hostports.add_host_ports(&pod_host_ports),
            )
            .await?;
        match hostport_result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                let message = format!("Failed to add hostPort rules: {e:#}");
                tracing::warn!(
                    namespace = key.namespace,
                    name = key.name,
                    uid = key.uid,
                    "{message}"
                );
                self.rollback_partial_pod_start(&key, &sandbox_id, "hostPort setup failed")
                    .await?;
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
            Err(_) => {
                let message = format!(
                    "Timed out adding hostPort rules after {:?}",
                    POST_SANDBOX_HOSTPORT_SETUP_TIMEOUT
                );
                tracing::warn!(
                    namespace = key.namespace,
                    name = key.name,
                    uid = key.uid,
                    "{message}"
                );
                self.rollback_partial_pod_start(&key, &sandbox_id, "hostPort setup timed out")
                    .await?;
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
        let volume_result = self
            .supervisor
            .timeout(
                "pod_start_process_volumes",
                POST_SANDBOX_VOLUME_SETUP_TIMEOUT,
                self.volumes.process_volumes(&key, &pod),
            )
            .await?;
        let volume_paths = match volume_result {
            Ok(Ok(paths)) => paths,
            Ok(Err(e)) => {
                let message = format!("Failed to process volumes: {e:#}");
                tracing::warn!(
                    namespace = key.namespace,
                    name = key.name,
                    uid = key.uid,
                    "{message}"
                );
                let _ = self.hostports.remove_host_ports(&pod_host_ports).await;
                self.rollback_partial_pod_start(&key, &sandbox_id, "volume processing failed")
                    .await?;
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
            Err(_) => {
                let message = format!(
                    "Timed out processing volumes after {:?}",
                    POST_SANDBOX_VOLUME_SETUP_TIMEOUT
                );
                tracing::warn!(
                    namespace = key.namespace,
                    name = key.name,
                    uid = key.uid,
                    "{message}"
                );
                let _ = self.hostports.remove_host_ports(&pod_host_ports).await;
                self.rollback_partial_pod_start(&key, &sandbox_id, "volume processing timed out")
                    .await?;
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

            let started_at = self.clock.now_ms().div_euclid(1_000);
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
                let terminated = build_init_failure_terminated_state(
                    exit_code,
                    started_at,
                    stopped.finished_at,
                    self.clock.now_utc(),
                );
                let next_init_statuses = if retry {
                    build_retrying_init_container_statuses(
                        &pod,
                        container_name,
                        &init_container_statuses,
                        terminated,
                        self.clock.now_utc(),
                    )
                } else {
                    build_failed_init_container_statuses(
                        &pod,
                        container_name,
                        exit_code,
                        started_at,
                        stopped.finished_at,
                        self.clock.now_utc(),
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
                    self.clock.now_utc(),
                ),
            );
        }

        // --- Cancellation check: after init containers, before main ---
        if cancel.is_cancelled() {
            self.rollback_partial_pod_start(&key, &sandbox_id, "startup cancelled after init")
                .await?;
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

            if let Err(message) = check_run_as_non_root(&pod, container, container_name) {
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
                    .await?;
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
                    .await?;
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
                .await?;
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
                    .await?;
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
                        self.cri.stop_container(container_id, 30).await?;
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
                        self.cri.stop_container(container_id, 30).await?;
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
        request: klights_kubelet::runtime::PodStopRequest,
    ) -> anyhow::Result<klights_kubelet::runtime::PodStopResult> {
        let klights_kubelet::runtime::PodStopRequest {
            key,
            pod,
            sandbox_id,
            deletion_deadline,
            mode,
            operation_id,
            cancel,
        } = request;
        if cancel.is_cancelled() {
            return Ok(klights_kubelet::runtime::PodStopResult::Cancelled);
        }
        tracing::debug!(
            namespace = %key.namespace,
            pod = %key.name,
            uid = %key.uid,
            operation_id,
            ?mode,
            ?deletion_deadline,
            "starting correlated Pod stop"
        );
        // Stop probes by UID.
        if stop_step_until_cancelled(&cancel, self.probes.stop_probes(&key))
            .await
            .is_none()
        {
            return Ok(klights_kubelet::runtime::PodStopResult::Cancelled);
        }

        // Orphan cleanup may not have a deleted Pod snapshot. Delegate to the
        // focused helper, which resolves the sandbox(es) via hint → store →
        // CRI-by-UID and tears them down before clearing the slot (HR #11).
        if pod.is_none() {
            let Some(result) =
                stop_step_until_cancelled(&cancel, self.stop_orphan_pod(&key, sandbox_id)).await
            else {
                return Ok(klights_kubelet::runtime::PodStopResult::Cancelled);
            };
            result?;
            return Ok(klights_kubelet::runtime::PodStopResult::Completed);
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
                self.resolve_sandbox_id_for_stop(&key, &pod).await?
            }
        } else {
            self.resolve_sandbox_id_for_stop(&key, &pod).await?
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

        let mut container_ids: Vec<String> = Vec::new();
        let mut prestop_container_ids: Vec<String> = Vec::new();
        if let Some(sandbox_id) = sandbox_id.as_deref() {
            let Some(containers) = stop_step_until_cancelled(
                &cancel,
                self.container_control.list_containers(Some(sandbox_id)),
            )
            .await
            else {
                return Ok(klights_kubelet::runtime::PodStopResult::Cancelled);
            };
            let containers = containers?;
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
                let Some(status) =
                    stop_step_until_cancelled(&cancel, self.cri.container_status(container_id))
                        .await
                else {
                    return Ok(klights_kubelet::runtime::PodStopResult::Cancelled);
                };
                container_name = status?
                    .status
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
            let remaining = klights_kubelet::runtime::remaining_stop_grace(
                deletion_deadline,
                self.clock.now_utc(),
                mode,
            );
            if remaining.is_zero() {
                break;
            }
            let hook = self.supervisor.timeout(
                "pod_pre_stop_deadline",
                remaining,
                self.hooks
                    .execute_pre_stop(container_id, pod_ip, hook, container),
            );
            let Some(result) = stop_step_until_cancelled(&cancel, hook).await else {
                return Ok(klights_kubelet::runtime::PodStopResult::Cancelled);
            };
            match result {
                Ok(Ok(Ok(HookOutcome::Succeeded) | Ok(HookOutcome::Failed(_)))) => {}
                Ok(Ok(Err(error))) => {
                    tracing::warn!(container = container_name, "preStop hook error: {error:#}");
                }
                Ok(Err(_elapsed)) => {
                    tracing::warn!(
                        container = container_name,
                        "preStop hook reached Pod deletion deadline"
                    );
                }
                Err(error) => {
                    tracing::warn!(
                        container = container_name,
                        "preStop supervision failed: {error:#}"
                    );
                }
            }
        }

        if let Some(sandbox_id) = sandbox_id.as_deref() {
            let mut seen_container_ids = std::collections::HashSet::new();
            for container_id in container_ids
                .iter()
                .filter(|id| seen_container_ids.insert((*id).clone()))
            {
                let grace_period_seconds = klights_kubelet::runtime::remaining_stop_grace_seconds(
                    deletion_deadline,
                    self.clock.now_utc(),
                    mode,
                );
                let Some(result) = stop_step_until_cancelled(
                    &cancel,
                    self.cri.stop_container(container_id, grace_period_seconds),
                )
                .await
                else {
                    return Ok(klights_kubelet::runtime::PodStopResult::Cancelled);
                };
                result?;
                let Some(result) =
                    stop_step_until_cancelled(&cancel, self.cri.remove_container(container_id))
                        .await
                else {
                    return Ok(klights_kubelet::runtime::PodStopResult::Cancelled);
                };
                result?;
            }

            // Stop and remove sandbox.
            let Some(result) =
                stop_step_until_cancelled(&cancel, self.cri.stop_pod_sandbox(sandbox_id)).await
            else {
                return Ok(klights_kubelet::runtime::PodStopResult::Cancelled);
            };
            result?;
            let Some(result) =
                stop_step_until_cancelled(&cancel, self.cri.remove_pod_sandbox(sandbox_id)).await
            else {
                return Ok(klights_kubelet::runtime::PodStopResult::Cancelled);
            };
            result?;

            // Release CNI network. (cgroup teardown is UID-keyed and runs
            // unconditionally in cleanup_pod_local_artifacts below.)
            let Some(result) = stop_step_until_cancelled(
                &cancel,
                self.network.release_sandbox_network(&key, sandbox_id),
            )
            .await
            else {
                return Ok(klights_kubelet::runtime::PodStopResult::Cancelled);
            };
            result?;

            // Delete the sandbox row after CNI release. Real network cleanup
            // uses the UID-qualified row as the authorization witness.
            let Some(result) =
                stop_step_until_cancelled(&cancel, self.store.delete_sandbox(&key)).await
            else {
                return Ok(klights_kubelet::runtime::PodStopResult::Cancelled);
            };
            result?;
        } else {
            tracing::warn!(
                namespace = %key.namespace,
                name = %key.name,
                uid = %key.uid,
                "no sandbox id resolved for pod stop; skipping CRI and CNI teardown"
            );
        }

        // Remove hostPort rules.
        let pod_host_ports = super::hostports::pod_host_ports_from_resource(&key, &pod)?;
        let Some(result) =
            stop_step_until_cancelled(&cancel, self.hostports.remove_host_ports(&pod_host_ports))
                .await
        else {
            return Ok(klights_kubelet::runtime::PodStopResult::Cancelled);
        };
        result?;

        let Some(result) =
            stop_step_until_cancelled(&cancel, self.cleanup_pod_local_artifacts(&key)).await
        else {
            return Ok(klights_kubelet::runtime::PodStopResult::Cancelled);
        };
        result?;

        // Clear pod slot by UID.
        let Some(result) =
            stop_step_until_cancelled(&cancel, self.slot_admission.clear_slot(&key)).await
        else {
            return Ok(klights_kubelet::runtime::PodStopResult::Cancelled);
        };
        result?;

        Ok(klights_kubelet::runtime::PodStopResult::Completed)
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
        if matches!(
            result,
            PodDeletionFinalizeResult::DeletedOrAlreadyGone | PodDeletionFinalizeResult::Queued
        ) {
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
            .await?;
        // containerd can publish a Started event a few milliseconds before
        // ListContainers/ContainerStatus advances from Created. The event is
        // the newer CRI observation, so preserve that monotonic transition
        // while still sourcing container identity/image/timestamps below from
        // ContainerStatus. A later Exited snapshot always wins.
        for (container_id, state) in &mut containers {
            *state = apply_runtime_event_hint(&hint, container_id, *state);
        }
        // Augment with ALL observed container IDs from the hint, not just when
        // the listing is empty. Multi-container pods and partial listings miss
        // exited containers that have already been removed from the sandbox;
        // the hint carries every CRI event's container ID so we can fetch their
        // terminal state directly even when they're absent from the listing.
        for container_id in hint.container_ids() {
            if containers.iter().any(|(id, _)| id == container_id) {
                continue; // Already present in the listing — skip the direct fetch.
            }
            if let Some(mut state) = status_projection::runtime_state_from_container_status(
                self.cri.as_ref(),
                container_id,
            )
            .await?
            {
                state = apply_runtime_event_hint(&hint, container_id, state);
                containers.push((container_id.to_string(), state));
            }
            // If the hinted ID has no runtime status, treat as observation miss
            // and skip without regressing the Pod's existing phase/container state.
        }

        // 3. Build phase and container statuses from CRI state plus the Pod spec.
        let (mut phase, mut container_statuses) =
            status_projection::reconcile_container_statuses_from_pod_spec(
                self.cri.as_ref(),
                self.filesystem.as_ref(),
                &key,
                &resource.data,
                &containers,
                self.clock.now_utc(),
            )
            .await?;
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
        klights_types::merge_pod_status_for_update(
            "v1",
            "Pod",
            &resource.data,
            &mut status,
            klights_types::PodStatusOwner::KubeletRuntime,
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
            .await?;

        let mut runtime_by_name: std::collections::HashMap<String, (String, String, String, i64)> =
            std::collections::HashMap::new();
        for (container_id, _) in runtime_containers {
            let status = self.cri.container_status(&container_id).await?.status;
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
        let dns_ip = klights_types::dns_service_ipv4(&self.config.service_cidr);
        let kubernetes_service_ip = klights_types::first_usable_ipv4(&self.config.service_cidr);
        let default_spec = serde_json::json!({});
        let pod_spec = pod.get("spec").unwrap_or(&default_spec);
        let sandbox_config = build_sandbox_config_with_runtime_inputs(
            klights_kubelet::pod_sandbox_config::SandboxIdentity {
                pod_name: &key.name,
                namespace: &key.namespace,
                pod_uid: &key.uid,
                containerd_namespace: &self.config.containerd_namespace,
            },
            pod_ip,
            &dns_ip,
            pod_spec,
            &self.config.sandbox_inputs,
            &self.config.paths,
        );
        let mut volume_paths: Option<std::collections::HashMap<String, String>> = None;

        for ec in ephemeral_containers {
            let ec_name = ec.get("name").and_then(|n| n.as_str()).unwrap_or("");
            if ec_name.is_empty() || runtime_by_name.contains_key(ec_name) {
                continue;
            }

            if let Err(message) = check_run_as_non_root(&pod, ec, ec_name) {
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
                    self.clock.now_ms().saturating_mul(1_000_000),
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
                    self.clock.now_utc(),
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
                self.clock.now_utc(),
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
                Err(e)
                    if matches!(
                        e.downcast_ref::<klights_pod_api::PodRepositoryError>(),
                        Some(klights_pod_api::PodRepositoryError::Conflict { .. })
                    ) && attempt < 4 =>
                {
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
        crate::kubelet::pod_runtime::lifecycle_commands::handle_lifecycle_command(self, command)
            .await
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
