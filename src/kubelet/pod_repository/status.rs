//! `PodStatusService` — owns standard, runtime-reconcile, probe-readiness,
//! and deadline-exceeded status writes.
//!
//! Holds `Arc<PodStore>` only — does not hold a `DatastoreHandle`.
//! Implementations land in Tasks 4, 5, 7, 8.

use std::sync::Arc;

use anyhow::{Result, anyhow};
use serde_json::{Value, json};

use crate::control_plane::client::{LeaderApiClient, ResourceKey};
use crate::controllers::workqueue::ReconcileKey;
use crate::datastore::Resource;
use crate::kubelet::outbox::payload::OutboxOperation;
use crate::kubelet::outbox::{Outbox, OutboxCommand, OutboxSendPlanner, OutboxSubject};
use crate::kubelet::pod_manager::get_cached_host_ip;
use crate::kubelet::pod_status_logic::{
    compute_initialized_condition, get_condition_last_transition_time,
};
use crate::side_effects::ControllerDispatcherSlot;

use super::state_only_writer::StateOnlyWriter;
use super::store::PodStore;
use super::types::{PodStatusUpdate, RuntimeReconcileStatus};

pub(super) struct PodStatusService {
    store: Arc<PodStore>,
    status_only: Arc<dyn StateOnlyWriter>,
    controller_dispatcher: ControllerDispatcherSlot,
    outbox: Option<Arc<Outbox>>,
    cluster_api: Option<Arc<dyn LeaderApiClient>>,
}

pub(super) struct PodStatusWriteResult {
    pub(super) resource: Resource,
    pub(super) changed: bool,
    pub(super) endpoint_state_changed: bool,
}

impl PodStatusWriteResult {
    fn unchanged(resource: Resource) -> Self {
        Self {
            resource,
            changed: false,
            endpoint_state_changed: false,
        }
    }
}

impl PodStatusService {
    pub(super) fn new(
        store: Arc<PodStore>,
        status_only: Arc<dyn StateOnlyWriter>,
        controller_dispatcher: ControllerDispatcherSlot,
        outbox: Option<Arc<Outbox>>,
        cluster_api: Option<Arc<dyn LeaderApiClient>>,
    ) -> Self {
        Self {
            store,
            status_only,
            controller_dispatcher,
            outbox,
            cluster_api,
        }
    }

    async fn send_status_command(&self, command: OutboxCommand) -> Result<bool> {
        if self.outbox.is_some() {
            OutboxSendPlanner::new(self.outbox.as_deref())
                .route(command)
                .await?;
            return Ok(true);
        }

        if self.cluster_api.is_some() {
            return Err(anyhow!(
                "outbox is unavailable for node-local queueing; caller must retry after outbox initialization"
            ));
        }

        Ok(false)
    }

    async fn read_current_pod(
        &self,
        ns: &str,
        name: &str,
        expected_uid: Option<&str>,
    ) -> Result<Resource> {
        let mut pod_resource = if let Some(cluster_api) = &self.cluster_api {
            cluster_api
                .get_resource_fresh(ResourceKey {
                    api_version: "v1".to_string(),
                    kind: "Pod".to_string(),
                    namespace: Some(ns.to_string()),
                    name: name.to_string(),
                })
                .await?
        } else {
            self.store.get(ns, name).await?
        }
        .ok_or_else(|| anyhow!("Pod not found"))?;
        if self.cluster_api.is_some()
            && let Some(outbox) = &self.outbox
        {
            pod_resource = outbox.merge_pod_status_checkpoint(pod_resource).await?;
        }
        if let Some(uid) = expected_uid {
            super::ensure_pod_uid_matches(&pod_resource.data, uid, ns, name)?;
        }
        Ok(pod_resource)
    }

    async fn enqueue_status_outbox(
        &self,
        operation: OutboxOperation,
        pod_resource: &Resource,
        status: Value,
        expected_rv: Option<i64>,
    ) -> Result<Option<Resource>> {
        let namespace = pod_resource.namespace.as_deref().unwrap_or("default");
        let pod_uid = pod_resource.uid.as_str();
        let subject_key = format!(
            "v1/Pod/{}/{}/{}",
            namespace, pod_resource.name, pod_resource.uid
        );
        // The stamp is only consumed by the leader's outbox-apply lost-update
        // gate, so it is issued only when an outbox is present (worker / outbox
        // leader). It must come from the durable per-node allocator so it stays
        // monotonic across worker restarts. Direct (outbox-less) writes never
        // reach the gate, so they carry no stamp.
        let observed_status_stamp = match self.outbox.as_deref() {
            Some(outbox) => Some(outbox.next_status_stamp().await?),
            None => None,
        };
        let command = crate::datastore::command::StorageCommand::UpdateStatus {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some(namespace.to_string()),
            name: pod_resource.name.clone(),
            status: status.clone(),
            expected_rv,
            preconditions: crate::datastore::ResourcePreconditions {
                uid: Some(pod_resource.uid.clone()),
                resource_version: expected_rv,
            },
            observed_status_stamp,
        };
        let synthetic = synthetic_status_resource(pod_resource, &status);
        let sent = self
            .send_status_command(OutboxCommand {
                idempotency_key: format!("{}:{}", subject_key, uuid::Uuid::new_v4()),
                operation,
                subject: OutboxSubject {
                    key: subject_key,
                    namespace: Some(namespace.to_string()),
                    name: pod_resource.name.clone(),
                    uid: Some(pod_uid.to_string()),
                },
                pod_uid: pod_uid.to_string(),
                command,
                now_ms: now_ms(),
            })
            .await?;

        if sent {
            tracing::info!(
                target: "klights::pod_status::trace",
                writer = %operation,
                namespace,
                pod = %pod_resource.name,
                uid = %pod_resource.uid,
                expected_rv,
                stored_rv = pod_resource.resource_version,
                stored_phase = ?pod_resource.data.pointer("/status/phase").and_then(|v| v.as_str()),
                synthetic_rv = synthetic.resource_version,
                synthetic_phase = ?synthetic.data.pointer("/status/phase").and_then(|v| v.as_str()),
                stored_container_statuses = %container_statuses_for_log(&pod_resource.data),
                synthetic_container_statuses = %container_statuses_for_log(&synthetic.data),
                "pod status outbox: enqueued status write"
            );
            if let Some(outbox) = &self.outbox {
                outbox
                    .record_pod_status_checkpoint(pod_resource, status, now_ms())
                    .await?;
                tracing::info!(
                    target: "klights::pod_status::trace",
                    writer = %operation,
                    namespace,
                    pod = %pod_resource.name,
                    uid = %pod_resource.uid,
                    synthetic_rv = synthetic.resource_version,
                    synthetic_container_statuses = %container_statuses_for_log(&synthetic.data),
                "pod status outbox: recorded node-local status checkpoint"
                );
            }
            return Ok(Some(synthetic));
        }

        tracing::info!(
        target: "klights::pod_status::trace",
        writer = %operation,
        namespace,
        pod = %pod_resource.name,
        uid = %pod_resource.uid,
        expected_rv,
        stored_rv = pod_resource.resource_version,
        stored_phase = ?pod_resource.data.pointer("/status/phase").and_then(|v| v.as_str()),
        synthetic_rv = synthetic.resource_version,
        synthetic_phase = ?synthetic.data.pointer("/status/phase").and_then(|v| v.as_str()),
            stored_container_statuses = %container_statuses_for_log(&pod_resource.data),
            synthetic_container_statuses = %container_statuses_for_log(&synthetic.data),
        "pod status write will be applied locally"
        );
        Ok(None)
    }

    /// Standard post-sandbox / post-IP status write.
    ///
    /// Reads the current pod (for `lastTransitionTime` continuity, restart
    /// preservation, and qosClass), builds the full status object, and
    /// persists status-only via `PodStore::update_status` with CAS.
    ///
    /// `expected_rv` semantics:
    /// - `Some(rv)` → use that as the CAS value. Caller is asserting they
    ///   know the live RV (typical for read-modify-write paths).
    /// - `None` → fall back to the read's RV. This matches today's
    ///   `update_pod_status` which always reads-then-writes against the
    ///   read's RV.
    ///
    /// LEGACY no-UID variant. Kept for test scaffolding only — production
    /// must use `set_pod_status_for_uid`.
    pub(super) async fn set_pod_status(
        &self,
        ns: &str,
        name: &str,
        update: &PodStatusUpdate,
        expected_rv: Option<i64>,
    ) -> Result<PodStatusWriteResult> {
        self.set_pod_status_checked(ns, name, None, update, expected_rv)
            .await
    }

    /// UID-bound status write. Refuses to apply if the live pod's UID
    /// has changed since the caller's snapshot — prevents stale events
    /// for a deleted pod from folding into a same-name recreated pod.
    pub(super) async fn set_pod_status_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        update: PodStatusUpdate,
        expected_rv: Option<i64>,
    ) -> Result<PodStatusWriteResult> {
        self.set_pod_status_checked(ns, name, Some(pod_uid), &update, expected_rv)
            .await
    }

    async fn set_pod_status_checked(
        &self,
        ns: &str,
        name: &str,
        expected_uid: Option<&str>,
        update: &PodStatusUpdate,
        expected_rv: Option<i64>,
    ) -> Result<PodStatusWriteResult> {
        let max_attempts = if expected_rv.is_some() { 1 } else { 5 };
        for attempt in 0..max_attempts {
            let pod_resource = self.read_current_pod(ns, name, expected_uid).await?;
            let cas_rv = expected_rv.unwrap_or(pod_resource.resource_version);
            let pod = pod_resource.data.clone();

            let existing_conditions = pod
                .get("status")
                .and_then(|s| s.get("conditions"))
                .and_then(|c| c.as_array())
                .cloned()
                .unwrap_or_default();

            let init_container_statuses =
                update.init_container_statuses.clone().unwrap_or_else(|| {
                    pod.get("status")
                        .and_then(|s| s.get("initContainerStatuses"))
                        .and_then(|s| s.as_array())
                        .cloned()
                        .unwrap_or_default()
                });
            let (init_initialized, init_not_ready_message) =
                compute_initialized_condition(&pod, &init_container_statuses);

            let preserved = preserve_restart_fields(&pod);
            let mut container_statuses = update.container_statuses.clone();
            for status in &mut container_statuses {
                apply_preserved_restart_fields(status, &preserved);
            }

            let now = crate::utils::k8s_timestamp();
            let all_containers_ready = if container_statuses.is_empty() {
                update.phase == "Running"
            } else {
                container_statuses
                    .iter()
                    .all(|c| c.get("ready").and_then(|r| r.as_bool()).unwrap_or(false))
            };
            let bool_status = |b: bool| if b { "True" } else { "False" };

            let initialized_condition = if init_initialized {
                build_pod_condition(
                    &existing_conditions,
                    &now,
                    "Initialized",
                    "True",
                    None,
                    None,
                )
            } else {
                build_pod_condition(
                    &existing_conditions,
                    &now,
                    "Initialized",
                    "False",
                    Some("ContainersNotInitialized"),
                    Some(init_not_ready_message.as_deref().unwrap_or("")),
                )
            };
            let containers_ready_status = bool_status(all_containers_ready);
            let ready_status = bool_status(update.phase == "Running" && all_containers_ready);
            // The kubelet rebuilds its owned lifecycle conditions and routes the
            // result through the typed Pod status ownership policy, which
            // preserves by `type` every condition the kubelet runtime does not
            // own (e.g. the scheduler's DisruptionTarget). Ownership is decided
            // by PodStatusOwner, not by an ad hoc condition-type heuristic here.
            let conditions = crate::pod_status_merge::merge_owned_and_preserved_conditions(
                crate::pod_status_merge::PodStatusOwner::KubeletRuntime,
                vec![
                    build_pod_condition(
                        &existing_conditions,
                        &now,
                        "PodScheduled",
                        "True",
                        Some("PodScheduled"),
                        None,
                    ),
                    initialized_condition,
                    build_pod_condition(
                        &existing_conditions,
                        &now,
                        "ContainersReady",
                        containers_ready_status,
                        None,
                        None,
                    ),
                    build_pod_condition(
                        &existing_conditions,
                        &now,
                        "Ready",
                        ready_status,
                        None,
                        None,
                    ),
                ],
                &existing_conditions,
            );

            let pod_ip = update.pod_ip.clone();
            let host_ip = if update.host_ip.is_empty() {
                get_cached_host_ip().to_string()
            } else {
                update.host_ip.clone()
            };

            let mut status_obj = json!({
                "phase": update.phase,
                "podIP": pod_ip,
                "hostIP": host_ip,
                "hostIPs": [{ "ip": host_ip }],
                "conditions": conditions,
                "containerStatuses": container_statuses,
            });
            if !pod_ip.is_empty() {
                status_obj["podIPs"] = json!([{ "ip": pod_ip }]);
            }
            if !init_container_statuses.is_empty() {
                status_obj["initContainerStatuses"] = json!(init_container_statuses);
            }

            let preserved_qos = update.qos_class.clone().or_else(|| {
                pod.get("status")
                    .and_then(|s| s.get("qosClass"))
                    .and_then(|q| q.as_str())
                    .map(String::from)
            });
            if let Some(qos) = preserved_qos {
                status_obj["qosClass"] = json!(qos);
            }

            // Dedup gate intentionally removed (sonobuoy "Container Runtime …
            // should run with the expected status" reproducer). Every status
            // update is now forwarded to the writer; the per-field diff inside
            // `log_pod_status_write_result` still suppresses noisy watch
            // traffic when the object on disk does not actually change.
            if let Some(resource) = self
                .enqueue_status_outbox(
                    OutboxOperation::PodStatus,
                    &pod_resource,
                    status_obj.clone(),
                    expected_rv,
                )
                .await?
            {
                return Ok(PodStatusWriteResult {
                    resource,
                    changed: false,
                    endpoint_state_changed: false,
                });
            }

            match self
                .status_only
                .write_status(ns, name, status_obj, Some(cas_rv))
                .await
            {
                Ok(updated) => {
                    let changed = log_pod_status_write_result(
                        "set-pod-status",
                        ns,
                        name,
                        cas_rv,
                        &pod,
                        &updated,
                    );
                    if changed {
                        self.refresh_owner_status_after_pod_status_change(&pod, &updated.data)
                            .await;
                    }
                    let endpoint_state_changed = changed
                        && crate::side_effects::service_pod::pod_endpoint_state_changed(
                            &pod,
                            &updated.data,
                        );
                    return Ok(PodStatusWriteResult {
                        resource: updated,
                        changed,
                        endpoint_state_changed,
                    });
                }
                Err(e)
                    if expected_rv.is_none() && attempt + 1 < max_attempts && is_conflict(&e) =>
                {
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        unreachable!("status retry loop must return before exhausting attempts")
    }

    /// Runtime-reconcile path used by the event handler. Overwrites
    /// `phase` and `containerStatuses` while preserving every other
    /// field of the existing status (`podIP`, `podIPs`, `hostIP`,
    /// `hostIPs`, `conditions`, `qosClass`, `initContainerStatuses`).
    /// The runtime reconciler does not own those fields, so it must not
    /// erase them when it observes a CRI state change.
    pub(super) async fn apply_runtime_reconcile_status(
        &self,
        ns: &str,
        name: &str,
        update: RuntimeReconcileStatus,
        expected_rv: Option<i64>,
    ) -> Result<PodStatusWriteResult> {
        self.apply_runtime_reconcile_status_inner(ns, name, None, update, expected_rv)
            .await
    }

    pub(super) async fn apply_runtime_reconcile_status_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        update: RuntimeReconcileStatus,
        expected_rv: Option<i64>,
    ) -> Result<PodStatusWriteResult> {
        self.apply_runtime_reconcile_status_inner(ns, name, Some(pod_uid), update, expected_rv)
            .await
    }

    /// Surface a retryable StartPod failure (image pull, CNI readiness,
    /// transient CRI connectivity) as `containerStatuses[].state.waiting`
    /// without making controller-owned Pods terminal.
    ///
    /// On the first failure the waiting reason is `ErrImagePull` (for pull
    /// errors) or `CreateContainerError` / `PodInitializing`; on the second
    /// and subsequent failures the pull-error reason escalates to
    /// `ImagePullBackOff`. Phase stays `Pending` so Deployment / ReplicaSet
    /// controllers do not count the pod as permanently failed.
    ///
    /// UID-bound: stale-UID writes are rejected by `read_current_pod`.
    pub(super) async fn mark_start_pending_for_retry_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        error_message: &str,
    ) -> Result<PodStatusWriteResult> {
        use crate::kubelet::pod_status_builders::{
            build_creation_error_statuses, build_image_pull_error_statuses,
            build_retrying_init_container_statuses,
        };
        use crate::kubelet::pod_status_logic::{
            is_image_pull_error_msg, parse_init_container_failure,
        };

        let pod_resource = self.read_current_pod(ns, name, Some(pod_uid)).await?;
        let pod = pod_resource.data.as_ref();

        let existing_init_container_statuses = pod
            .pointer("/status/initContainerStatuses")
            .and_then(|s| s.as_array())
            .cloned()
            .unwrap_or_default();

        let (container_statuses, init_container_statuses) =
            if let Some((failed_name, exit_code)) = parse_init_container_failure(error_message) {
                (
                    build_creation_error_statuses(pod, error_message),
                    build_retrying_init_container_statuses(
                        pod,
                        failed_name,
                        exit_code,
                        &existing_init_container_statuses,
                    ),
                )
            } else if is_image_pull_error_msg(error_message) {
                (
                    build_image_pull_error_statuses(pod, error_message),
                    existing_init_container_statuses,
                )
            } else {
                let statuses = pod
                    .pointer("/status/containerStatuses")
                    .and_then(|s| s.as_array())
                    .filter(|statuses| !statuses.is_empty())
                    .cloned()
                    .unwrap_or_else(|| build_creation_error_statuses(pod, error_message));
                (statuses, existing_init_container_statuses)
            };

        let pod_ip = pod
            .pointer("/status/podIP")
            .and_then(|ip| ip.as_str())
            .unwrap_or("")
            .to_string();

        let update = PodStatusUpdate {
            phase: "Pending".to_string(),
            pod_ip,
            host_ip: String::new(),
            container_statuses,
            init_container_statuses: Some(init_container_statuses),
            qos_class: None,
        };

        self.set_pod_status_for_uid(ns, name, pod_uid, update, None)
            .await
    }

    async fn apply_runtime_reconcile_status_inner(
        &self,
        ns: &str,
        name: &str,
        expected_uid: Option<&str>,
        update: RuntimeReconcileStatus,
        expected_rv: Option<i64>,
    ) -> Result<PodStatusWriteResult> {
        let max_attempts = if expected_rv.is_some() { 1 } else { 5 };
        for attempt in 0..max_attempts {
            let pod_resource = self.read_current_pod(ns, name, expected_uid).await?;
            // UID precondition: refuse to apply runtime-reconcile status
            // (phase, containerStatuses) to a pod row whose UID has
            // changed since the caller's CRI event was generated. Without
            // this check a stale CRI event for a deleted pod would set
            // phase=Running on a freshly-created same-name pod whose own
            // CNI/sandbox state has not yet produced its podIP, leaving
            // it Running without an IP.
            let cas_rv = expected_rv.unwrap_or(pod_resource.resource_version);

            // K8s invariant: a Pod cannot be `Running` without a `podIP`.
            // Under multinode write churn the CRI "container running"
            // event can fire on the worker BEFORE the worker's earlier
            // (phase=Pending, podIP=X) write has replicated back from
            // the leader. The local row read here would then have
            // podIP=null. If we proceeded to write phase=Running with
            // the (preserved) null podIP, the leader would overwrite
            // its row's podIP with null and the pod stays Running
            // without an IP forever. Keep the phase as observed, but
            // still persist runtime-owned containerStatuses so fast
            // restart/terminal transitions do not lose restartCount.
            //
            // Only applied when the caller did not pin `expected_rv` —
            // production CRI events use `None`; tests pin a snapshot
            // RV and don't model the podIP-publish step, so they can
            // still drive the legacy seed-then-reconcile flow.
            //
            // IP publication state machine:
            // Pending/no IP --sandbox status--> Pending/podIP
            // Pending/podIP --CRI running--> Running/podIP
            // Pending/no IP --CRI running--> Pending/no IP (defer)
            let mut phase_override = None;
            if expected_rv.is_none() {
                let current_pod_ip = pod_resource
                    .data
                    .pointer("/status/podIP")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty());
                let current_pod_ips_first = pod_resource
                    .data
                    .pointer("/status/podIPs/0/ip")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty());
                let host_network = pod_resource
                    .data
                    .pointer("/spec/hostNetwork")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                let has_pod_ip = current_pod_ip.is_some() || current_pod_ips_first.is_some();
                if update.phase == "Running" && !has_pod_ip && !host_network {
                    let current_phase = pod_resource
                        .data
                        .pointer("/status/phase")
                        .and_then(|v| v.as_str())
                        .filter(|phase| !phase.is_empty())
                        .unwrap_or("Pending")
                        .to_string();
                    tracing::info!(
                        namespace = %ns,
                        pod = %name,
                        phase = %update.phase,
                        current_phase = %current_phase,
                        has_pod_ip,
                        host_network,
                        current_pod_ip = ?current_pod_ip,
                        "runtime reconcile: preserving current phase until podIP is published"
                    );
                    phase_override = Some(current_phase);
                }
            }

            let mut status = pod_resource
                .data
                .get("status")
                .cloned()
                .unwrap_or_else(|| json!({}));
            if !status.is_object() {
                status = json!({});
            }
            if let Some(obj) = status.as_object_mut() {
                let preserved = preserve_restart_fields(&pod_resource.data);
                let mut container_statuses = update.container_statuses.clone();
                if container_statuses.is_empty() {
                    if let Some(existing_config_error_statuses) = pod_resource
                        .data
                        .pointer("/status/containerStatuses")
                        .and_then(|statuses| statuses.as_array())
                        .filter(|statuses| has_create_container_config_error_status(statuses))
                    {
                        tracing::info!(
                            target: "klights::pod_status::trace",
                            ns,
                            name,
                            expected_rv,
                            stored_rv = pod_resource.resource_version,
                            stored_phase = ?pod_resource.data.pointer("/status/phase").and_then(|v| v.as_str()),
                            stored_container_statuses = %container_statuses_for_log(&pod_resource.data),
                            "runtime-reconcile: preserving live CreateContainerConfigError statuses for empty CRI observation"
                        );
                        container_statuses = existing_config_error_statuses.clone();
                    } else {
                        tracing::info!(
                            target: "klights::pod_status::trace",
                            ns,
                            name,
                            expected_rv,
                            stored_rv = pod_resource.resource_version,
                            stored_phase = ?pod_resource.data.pointer("/status/phase").and_then(|v| v.as_str()),
                            stored_container_statuses = %container_statuses_for_log(&pod_resource.data),
                            "runtime-reconcile: empty CRI observation has no live CreateContainerConfigError status to preserve"
                        );
                    }
                }
                for status in &mut container_statuses {
                    apply_preserved_restart_fields(status, &preserved);
                }

                let phase = phase_override.as_deref().unwrap_or(&update.phase);
                obj.insert("phase".to_string(), json!(phase));
                obj.insert("containerStatuses".to_string(), json!(container_statuses));

                // The CRI's container_statuses don't include the `ready`
                // or `started` fields (those are kubelet-side decisions).
                // Preserve them from the existing pod status so the
                // Ready condition computed below stays correct.
                preserve_ready_and_started(obj, &pod_resource.data);

                repair_scalar_and_list_ip_fields(obj, "podIP", "podIPs");
                repair_scalar_and_list_ip_fields(obj, "hostIP", "hostIPs");

                apply_runtime_readiness_conditions(obj);
            }

            // Dedup gate intentionally removed for the runtime-reconcile path
            // (sonobuoy "Container Runtime … should run with the expected
            // status" reproducer): the prior skip-on-equality could suppress a
            // terminated→running publish when a brief CRI lag let two
            // consecutive reconciles compute the same terminated entry. Every
            // computed status is now forwarded; per-field diff inside
            // `log_pod_status_write_result` still suppresses noisy watch
            // traffic when the object on disk does not actually change.
            tracing::info!(
                target: "klights::pod_status::trace",
                ns,
                name,
                writer = "runtime-reconcile",
                expected_rv,
                stored_rv = pod_resource.resource_version,
                stored_phase = ?pod_resource.data.pointer("/status/phase").and_then(|v| v.as_str()),
                next_phase = ?status.pointer("/phase").and_then(|v| v.as_str()),
                stored_container_statuses = %pod_resource.data.pointer("/status/containerStatuses").map(|v| v.to_string()).unwrap_or_else(|| "<none>".into()),
                next_container_statuses = %status.pointer("/containerStatuses").map(|v| v.to_string()).unwrap_or_else(|| "<none>".into()),
                "runtime-reconcile: forwarding status write"
            );

            if let Some(resource) = self
                .enqueue_status_outbox(
                    OutboxOperation::RuntimeReconcile,
                    &pod_resource,
                    status.clone(),
                    expected_rv,
                )
                .await?
            {
                return Ok(PodStatusWriteResult {
                    resource,
                    changed: false,
                    endpoint_state_changed: false,
                });
            }

            match self
                .status_only
                .write_status(ns, name, status, Some(cas_rv))
                .await
            {
                Ok(updated) => {
                    let changed = log_pod_status_write_result(
                        "runtime-reconcile",
                        ns,
                        name,
                        cas_rv,
                        &pod_resource.data,
                        &updated,
                    );
                    if changed {
                        self.refresh_owner_status_after_pod_status_change(
                            &pod_resource.data,
                            &updated.data,
                        )
                        .await;
                    }
                    let endpoint_state_changed = changed
                        && crate::side_effects::service_pod::pod_endpoint_state_changed(
                            &pod_resource.data,
                            &updated.data,
                        );
                    return Ok(PodStatusWriteResult {
                        resource: updated,
                        changed,
                        endpoint_state_changed,
                    });
                }
                Err(e)
                    if expected_rv.is_none() && attempt + 1 < max_attempts && is_conflict(&e) =>
                {
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        unreachable!("status retry loop must return before exhausting attempts")
    }

    /// Increment `containerStatuses[name=container].restartCount` and
    /// stamp `lastState.terminated` for the named container after the
    /// kubelet's lifecycle command path restarts it. No-op if the
    /// container is not in `containerStatuses` yet (the next runtime
    /// reconcile will fill it in). Preserves every other status field.
    pub(super) async fn note_container_restart(
        &self,
        ns: &str,
        name: &str,
        container_name: &str,
        terminated: Value,
        expected_rv: Option<i64>,
    ) -> Result<Option<Resource>> {
        self.note_container_restart_inner(ns, name, None, container_name, terminated, expected_rv)
            .await
    }

    pub(super) async fn note_container_restart_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        container_name: &str,
        terminated: Value,
        expected_rv: Option<i64>,
    ) -> Result<Option<Resource>> {
        self.note_container_restart_inner(
            ns,
            name,
            Some(pod_uid),
            container_name,
            terminated,
            expected_rv,
        )
        .await
    }

    async fn note_container_restart_inner(
        &self,
        ns: &str,
        name: &str,
        expected_uid: Option<&str>,
        container_name: &str,
        terminated: Value,
        expected_rv: Option<i64>,
    ) -> Result<Option<Resource>> {
        let max_attempts = if expected_rv.is_some() { 1 } else { 5 };
        for attempt in 0..max_attempts {
            let pod_resource = self.read_current_pod(ns, name, expected_uid).await?;
            let cas_rv = expected_rv.unwrap_or(pod_resource.resource_version);

            let mut status = pod_resource
                .data
                .get("status")
                .cloned()
                .unwrap_or_else(|| json!({}));
            if !status.is_object() {
                status = json!({});
            }
            let mut touched = false;
            if let Some(arr) = status
                .pointer_mut("/containerStatuses")
                .and_then(|s| s.as_array_mut())
            {
                for cs in arr.iter_mut() {
                    if cs.get("name").and_then(|n| n.as_str()) != Some(container_name) {
                        continue;
                    }
                    if let Some(obj) = cs.as_object_mut() {
                        let current = obj
                            .get("restartCount")
                            .and_then(|v| v.as_i64())
                            .unwrap_or(0);
                        obj.insert("restartCount".to_string(), json!(current + 1));
                        obj.insert("lastState".to_string(), terminated.clone());
                        touched = true;
                    }
                    break;
                }
            }

            if !touched {
                return Ok(None);
            }

            if let Some(resource) = self
                .enqueue_status_outbox(
                    OutboxOperation::ContainerStatusSnapshot,
                    &pod_resource,
                    status.clone(),
                    expected_rv,
                )
                .await?
            {
                return Ok(Some(resource));
            }

            match self
                .status_only
                .write_status(ns, name, status, Some(cas_rv))
                .await
            {
                Ok(updated) => {
                    log_pod_status_write_result(
                        "note-container-restart",
                        ns,
                        name,
                        cas_rv,
                        &pod_resource.data,
                        &updated,
                    );
                    return Ok(Some(updated));
                }
                Err(e)
                    if expected_rv.is_none() && attempt + 1 < max_attempts && is_conflict(&e) =>
                {
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        unreachable!("status retry loop must return before exhausting attempts")
    }

    /// Replace `status.ephemeralContainerStatuses` with the given slice
    /// while preserving every other status field. Used by the kubelet
    /// runtime reconciler when CRI reports a state change for one of the
    /// ephemeral containers `kubectl debug` injected via the
    /// `/ephemeralcontainers` subresource. Mirrors
    /// `apply_runtime_reconcile_status`'s "overlay one slice, keep the
    /// rest" contract.
    pub(super) async fn apply_ephemeral_container_statuses(
        &self,
        ns: &str,
        name: &str,
        statuses: Vec<Value>,
        expected_rv: Option<i64>,
    ) -> Result<PodStatusWriteResult> {
        self.apply_ephemeral_container_statuses_inner(ns, name, None, statuses, expected_rv)
            .await
    }

    pub(super) async fn apply_ephemeral_container_statuses_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        statuses: Vec<Value>,
        expected_rv: Option<i64>,
    ) -> Result<PodStatusWriteResult> {
        self.apply_ephemeral_container_statuses_inner(
            ns,
            name,
            Some(pod_uid),
            statuses,
            expected_rv,
        )
        .await
    }

    async fn apply_ephemeral_container_statuses_inner(
        &self,
        ns: &str,
        name: &str,
        expected_uid: Option<&str>,
        statuses: Vec<Value>,
        expected_rv: Option<i64>,
    ) -> Result<PodStatusWriteResult> {
        let pod_resource = self.read_current_pod(ns, name, expected_uid).await?;
        let cas_rv = expected_rv.unwrap_or(pod_resource.resource_version);

        let mut status = pod_resource
            .data
            .get("status")
            .cloned()
            .unwrap_or_else(|| json!({}));
        if !status.is_object() {
            status = json!({});
        }
        if let Some(obj) = status.as_object_mut() {
            obj.insert("ephemeralContainerStatuses".to_string(), json!(statuses));
        }

        // Dedup gate intentionally removed; see runtime-reconcile call site
        // for rationale.

        if let Some(resource) = self
            .enqueue_status_outbox(
                OutboxOperation::EphemeralContainerStatuses,
                &pod_resource,
                status.clone(),
                expected_rv,
            )
            .await?
        {
            return Ok(PodStatusWriteResult {
                resource,
                changed: false,
                endpoint_state_changed: false,
            });
        }

        let updated = self
            .status_only
            .write_status(ns, name, status, Some(cas_rv))
            .await?;
        let changed = log_pod_status_write_result(
            "ephemeral-container-status",
            ns,
            name,
            cas_rv,
            &pod_resource.data,
            &updated,
        );
        let endpoint_state_changed = changed
            && crate::side_effects::service_pod::pod_endpoint_state_changed(
                &pod_resource.data,
                &updated.data,
            );
        Ok(PodStatusWriteResult {
            resource: updated,
            changed,
            endpoint_state_changed,
        })
    }

    /// Probe-driven readiness flip. Mutates
    /// `containerStatuses[name=container].ready` to `ready`, then aligns
    /// the `Ready` and `ContainersReady` pod conditions
    /// (`ReadinessProbeSucceeded` / `ReadinessProbeFailed`).
    /// `lastTransitionTime` only changes on an actual status flip — a
    /// no-op call (matching status) preserves the existing timestamp so
    /// downstream watchers don't see spurious MODIFIED events.
    pub(super) async fn set_probe_readiness(
        &self,
        ns: &str,
        name: &str,
        container_name: &str,
        ready: bool,
        expected_rv: Option<i64>,
    ) -> Result<PodStatusWriteResult> {
        self.set_probe_readiness_inner(ns, name, None, container_name, ready, expected_rv)
            .await
    }

    pub(super) async fn set_probe_readiness_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        container_name: &str,
        ready: bool,
        expected_rv: Option<i64>,
    ) -> Result<PodStatusWriteResult> {
        self.set_probe_readiness_inner(ns, name, Some(pod_uid), container_name, ready, expected_rv)
            .await
    }

    async fn set_probe_readiness_inner(
        &self,
        ns: &str,
        name: &str,
        expected_uid: Option<&str>,
        container_name: &str,
        ready: bool,
        expected_rv: Option<i64>,
    ) -> Result<PodStatusWriteResult> {
        let max_attempts = if expected_rv.is_some() { 1 } else { 5 };
        for attempt in 0..max_attempts {
            let pod_resource = self.read_current_pod(ns, name, expected_uid).await?;
            if let Some(expected) = expected_rv
                && expected != pod_resource.resource_version
            {
                return Err(crate::datastore::errors::DatastoreError::conflict(format!(
                    "resourceVersion precondition failed: expected {} got {}",
                    expected, pod_resource.resource_version
                ))
                .into());
            }
            let cas_rv = pod_resource.resource_version;

            let mut status = pod_resource
                .data
                .get("status")
                .cloned()
                .unwrap_or_else(|| json!({}));
            if !status.is_object() {
                status = json!({});
            }
            if ready && !can_publish_probe_ready(&status, container_name) {
                crate::datastore::diagnostics::log_noop_resource_write(
                    crate::datastore::diagnostics::NoopResourceWrite {
                        operation: "probe-readiness",
                        api_version: "v1",
                        kind: "Pod",
                        namespace: Some(ns),
                        name,
                        uid: &pod_resource.uid,
                        resource_version: pod_resource.resource_version,
                        reason: "successful readiness probe ignored until container is running",
                    },
                );
                return Ok(PodStatusWriteResult::unchanged(pod_resource));
            }
            let existing_status = status.clone();

            if let Some(arr) = status
                .pointer_mut("/containerStatuses")
                .and_then(|s| s.as_array_mut())
            {
                for cs in arr.iter_mut() {
                    if cs.get("name").and_then(|n| n.as_str()) == Some(container_name) {
                        if let Some(obj) = cs.as_object_mut() {
                            obj.insert("ready".to_string(), json!(ready));
                        }
                        break;
                    }
                }
            }

            let cond_status = if ready { "True" } else { "False" };
            let cond_reason = if ready {
                "ReadinessProbeSucceeded"
            } else {
                "ReadinessProbeFailed"
            };
            let now = crate::utils::k8s_timestamp();

            let conditions_value = status
                .as_object_mut()
                .ok_or_else(|| anyhow!("status is not a JSON object"))?
                .entry("conditions".to_string())
                .or_insert_with(|| json!([]));
            let conditions = conditions_value
                .as_array_mut()
                .ok_or_else(|| anyhow!("status.conditions is not a JSON array"))?;

            for cond_type in ["Ready", "ContainersReady"] {
                let mut found = false;
                for c in conditions.iter_mut() {
                    if c.get("type").and_then(|t| t.as_str()) == Some(cond_type) {
                        let current_status =
                            c.get("status").and_then(|s| s.as_str()).map(String::from);
                        let flipped = current_status.as_deref() != Some(cond_status);
                        if let Some(obj) = c.as_object_mut() {
                            obj.insert("status".to_string(), json!(cond_status));
                            obj.insert("reason".to_string(), json!(cond_reason));
                            if flipped {
                                obj.insert("lastTransitionTime".to_string(), json!(now));
                            }
                        }
                        found = true;
                        break;
                    }
                }
                if !found {
                    conditions.push(json!({
                        "type": cond_type,
                        "status": cond_status,
                        "reason": cond_reason,
                        "lastTransitionTime": now,
                    }));
                }
            }

            if status == existing_status {
                tracing::debug!(
                    target: "klights::pod_status",
                    writer = "probe-readiness",
                    namespace = %ns,
                    pod = %name,
                    old_rv = cas_rv,
                    container = %container_name,
                    ready,
                    "pod status write skipped because probe readiness produced no object change"
                );
                crate::datastore::diagnostics::log_noop_resource_write(
                    crate::datastore::diagnostics::NoopResourceWrite {
                        operation: "probe-readiness",
                        api_version: "v1",
                        kind: "Pod",
                        namespace: Some(ns),
                        name,
                        uid: &pod_resource.uid,
                        resource_version: pod_resource.resource_version,
                        reason: "computed pod status unchanged",
                    },
                );
                return Ok(PodStatusWriteResult::unchanged(pod_resource));
            }

            if let Some(resource) = self
                .enqueue_status_outbox(
                    OutboxOperation::ProbeReadiness,
                    &pod_resource,
                    status.clone(),
                    expected_rv,
                )
                .await?
            {
                return Ok(PodStatusWriteResult {
                    resource,
                    changed: false,
                    endpoint_state_changed: false,
                });
            }

            match self
                .status_only
                .write_status(ns, name, status, Some(cas_rv))
                .await
            {
                Ok(updated) => {
                    let changed = log_pod_status_write_result(
                        "probe-readiness",
                        ns,
                        name,
                        cas_rv,
                        &pod_resource.data,
                        &updated,
                    );
                    if changed {
                        self.refresh_owner_status_after_pod_status_change(
                            &pod_resource.data,
                            &updated.data,
                        )
                        .await;
                    }
                    let endpoint_state_changed = changed
                        && crate::side_effects::service_pod::pod_endpoint_state_changed(
                            &pod_resource.data,
                            &updated.data,
                        );
                    return Ok(PodStatusWriteResult {
                        resource: updated,
                        changed,
                        endpoint_state_changed,
                    });
                }
                Err(e)
                    if expected_rv.is_none() && attempt + 1 < max_attempts && is_conflict(&e) =>
                {
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        unreachable!("probe-readiness retry loop must return before exhausting attempts")
    }

    /// After a pod status write, enqueue all workload owner controllers so
    /// their top-down reconcile can update owner status from fresh pod state.
    ///
    /// This replaces the old bottom-up `OwnerStatusService::refresh_for_pod`
    /// which directly wrote owner `.status` using simplified pod counting that
    /// diverged from the richer controller-computed status.
    ///
    /// Owner status is now exclusively top-down: only the owning controller
    /// writes `.status` for Deployment, ReplicaSet, StatefulSet, DaemonSet,
    /// Job, and ReplicationController. Pod status changes propagate upward
    /// only as reconcile signals.
    async fn refresh_owner_status_after_pod_status_change(
        &self,
        previous: &Value,
        updated: &Value,
    ) {
        // Always enqueue the direct controller owner for all supported workload
        // kinds. This ensures status freshness even for kinds that don't gate
        // creation on readiness (ReplicaSet, DaemonSet, ReplicationController).
        if let Err(err) = self.enqueue_owner_reconcile_for_pod_status(updated).await {
            tracing::debug!(
                target: "klights::pod_status",
                error = %err,
                "failed to enqueue owner reconcile after pod status write"
            );
        }
        if let Err(err) = crate::side_effects::service_pod::enqueue_services_after_pod_update(
            previous,
            updated,
            self.store.db().as_ref(),
            &self.controller_dispatcher,
        )
        .await
        {
            tracing::debug!(
                target: "klights::pod_status",
                error = %err,
                "failed to enqueue Service reconcile after pod endpoint state changed"
            );
        }

        if !pod_owner_reconcile_status_changed(previous, updated) {
            return;
        }

        // Deployment rollout depends on RS pod readiness — enqueue the parent
        // Deployment when the pod's RS owner has a Deployment parent.
        if let Err(err) = self.enqueue_deployment_rollout_for_pod(updated).await {
            tracing::debug!(
                target: "klights::pod_status",
                error = %err,
                "failed to enqueue Deployment rollout after pod readiness transition"
            );
        }
    }

    /// Enqueue the direct controller owner for any supported workload kind.
    /// This covers ReplicaSet, StatefulSet, DaemonSet, Job, and
    /// ReplicationController — ensuring the owning controller re-reads fresh
    /// pod state and writes accurate top-down status.
    async fn enqueue_owner_reconcile_for_pod_status(&self, pod: &Value) -> Result<()> {
        match pod
            .pointer("/metadata/ownerReferences")
            .and_then(|v| v.as_array())
        {
            Some(refs) if !refs.is_empty() => {}
            _ => return Ok(()),
        }

        let Some(dispatcher) = self.controller_dispatcher.get() else {
            return Ok(());
        };

        dispatcher.enqueue_controller_owner_for_pod(pod).await;

        Ok(())
    }

    async fn enqueue_deployment_rollout_for_pod(&self, pod: &Value) -> Result<()> {
        let Some(dispatcher) = self.controller_dispatcher.get() else {
            return Ok(());
        };
        let namespace = pod
            .pointer("/metadata/namespace")
            .and_then(|v| v.as_str())
            .unwrap_or("default");
        let Some(rs_name) = controller_owner_name(pod, "apps/v1", "ReplicaSet") else {
            return Ok(());
        };
        let Some(rs) = self
            .store
            .db()
            .get_resource("apps/v1", "ReplicaSet", Some(namespace), &rs_name)
            .await?
        else {
            return Ok(());
        };
        let Some(deployment_name) = controller_owner_name(&rs.data, "apps/v1", "Deployment") else {
            return Ok(());
        };

        // Deployment rolling updates have a bounded desired-state dependency on
        // child availability: once replacement pods become Ready, the owning
        // Deployment must reconcile once to scale old ReplicaSets down.
        dispatcher
            .enqueue_reconcile_key(ReconcileKey::namespaced(
                "apps/v1",
                "Deployment",
                namespace,
                &deployment_name,
            ))
            .await;
        Ok(())
    }

    /// Mark a pod Failed because its `activeDeadlineSeconds` elapsed.
    /// Sets `phase=Failed`, `reason=DeadlineExceeded`, the supplied
    /// message, and replaces conditions with
    /// `[Ready=False/PodFailed, ContainersReady=False/PodFailed]` (matching
    /// today's behavior at `src/kubelet/pod_manager.rs:685-718`). Every
    /// other status field — `podIP`, `podIPs`, `hostIP`, `hostIPs`,
    /// `containerStatuses`, `qosClass` — is preserved.
    pub(super) async fn set_deadline_exceeded(
        &self,
        ns: &str,
        name: &str,
        message: String,
        expected_rv: Option<i64>,
    ) -> Result<PodStatusWriteResult> {
        self.set_deadline_exceeded_inner(ns, name, None, message, expected_rv)
            .await
    }

    pub(super) async fn set_deadline_exceeded_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        message: String,
        expected_rv: Option<i64>,
    ) -> Result<PodStatusWriteResult> {
        self.set_deadline_exceeded_inner(ns, name, Some(pod_uid), message, expected_rv)
            .await
    }

    async fn set_deadline_exceeded_inner(
        &self,
        ns: &str,
        name: &str,
        expected_uid: Option<&str>,
        message: String,
        expected_rv: Option<i64>,
    ) -> Result<PodStatusWriteResult> {
        let pod_resource = self.read_current_pod(ns, name, expected_uid).await?;
        let cas_rv = expected_rv.unwrap_or(pod_resource.resource_version);

        let mut status = pod_resource
            .data
            .get("status")
            .cloned()
            .unwrap_or_else(|| json!({}));
        if !status.is_object() {
            status = json!({});
        }
        let now = crate::utils::k8s_timestamp();
        if let Some(obj) = status.as_object_mut() {
            obj.insert("phase".to_string(), json!("Failed"));
            obj.insert("reason".to_string(), json!("DeadlineExceeded"));
            obj.insert("message".to_string(), json!(message));
            obj.insert(
                "conditions".to_string(),
                json!([
                    {
                        "type": "Ready",
                        "status": "False",
                        "lastTransitionTime": now,
                        "reason": "PodFailed",
                    },
                    {
                        "type": "ContainersReady",
                        "status": "False",
                        "lastTransitionTime": now,
                        "reason": "PodFailed",
                    }
                ]),
            );
        }

        // Dedup gate intentionally removed; see runtime-reconcile call site
        // for rationale.

        if let Some(resource) = self
            .enqueue_status_outbox(
                OutboxOperation::DeadlineExceeded,
                &pod_resource,
                status.clone(),
                expected_rv,
            )
            .await?
        {
            return Ok(PodStatusWriteResult {
                resource,
                changed: false,
                endpoint_state_changed: false,
            });
        }

        let updated = self
            .status_only
            .write_status(ns, name, status, Some(cas_rv))
            .await?;
        let changed = log_pod_status_write_result(
            "deadline-exceeded",
            ns,
            name,
            cas_rv,
            &pod_resource.data,
            &updated,
        );
        if changed {
            self.refresh_owner_status_after_pod_status_change(&pod_resource.data, &updated.data)
                .await;
        }
        let endpoint_state_changed = changed
            && crate::side_effects::service_pod::pod_endpoint_state_changed(
                &pod_resource.data,
                &updated.data,
            );
        Ok(PodStatusWriteResult {
            resource: updated,
            changed,
            endpoint_state_changed,
        })
    }
}

fn repair_scalar_and_list_ip_fields(
    status: &mut serde_json::Map<String, Value>,
    scalar_key: &str,
    list_key: &str,
) {
    let scalar_ip = status
        .get(scalar_key)
        .and_then(|value| value.as_str())
        .unwrap_or("")
        .to_string();
    let list_first_ip = status
        .get(list_key)
        .and_then(|value| value.as_array())
        .and_then(|ips| ips.first())
        .and_then(|entry| entry.get("ip"))
        .and_then(|value| value.as_str())
        .unwrap_or("")
        .to_string();

    if !scalar_ip.is_empty() {
        match status
            .get_mut(list_key)
            .and_then(|value| value.as_array_mut())
        {
            Some(ips) if !ips.is_empty() => {
                ips[0] = json!({ "ip": scalar_ip });
            }
            Some(ips) => ips.push(json!({ "ip": scalar_ip })),
            None => {
                status.insert(list_key.to_string(), json!([{ "ip": scalar_ip }]));
            }
        }
    } else if !list_first_ip.is_empty() {
        status.insert(scalar_key.to_string(), json!(list_first_ip));
    }
}

fn synthetic_status_resource(pod_resource: &Resource, status: &Value) -> Resource {
    let mut synthetic = pod_resource.clone();
    let mut data = (*pod_resource.data).clone();
    if !data.is_object() {
        data = json!({});
    }
    if let Some(obj) = data.as_object_mut() {
        obj.insert("status".to_string(), status.clone());
    }
    synthetic.data = Arc::new(data);
    synthetic
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

fn pod_ready_for_rollout(pod: &Value) -> bool {
    crate::controllers::common::is_pod_ready_value(pod)
}

fn pod_terminal_phase(pod: &Value) -> Option<&str> {
    match pod.pointer("/status/phase").and_then(|v| v.as_str()) {
        Some("Failed") => Some("Failed"),
        Some("Succeeded") => Some("Succeeded"),
        _ => None,
    }
}

fn pod_owner_reconcile_status_changed(previous: &Value, updated: &Value) -> bool {
    pod_ready_for_rollout(previous) != pod_ready_for_rollout(updated)
        || pod_terminal_phase(previous) != pod_terminal_phase(updated)
}

fn controller_owner_name(resource: &Value, api_version: &str, kind: &str) -> Option<String> {
    resource
        .pointer("/metadata/ownerReferences")
        .and_then(|v| v.as_array())
        .and_then(|refs| {
            refs.iter().find_map(|owner| {
                let is_controller = owner.get("controller").and_then(|v| v.as_bool()) == Some(true);
                let api_matches = owner
                    .get("apiVersion")
                    .and_then(|v| v.as_str())
                    .is_some_and(|value| value == api_version);
                let kind_matches = owner
                    .get("kind")
                    .and_then(|v| v.as_str())
                    .is_some_and(|value| value == kind);
                if is_controller && api_matches && kind_matches {
                    owner
                        .get("name")
                        .and_then(|v| v.as_str())
                        .map(str::to_string)
                } else {
                    None
                }
            })
        })
}

fn build_pod_condition(
    existing_conditions: &[Value],
    now: &str,
    cond_type: &str,
    status: &str,
    reason: Option<&str>,
    message: Option<&str>,
) -> Value {
    let mut obj = serde_json::Map::new();
    obj.insert("type".to_string(), json!(cond_type));
    obj.insert("status".to_string(), json!(status));
    obj.insert(
        "lastTransitionTime".to_string(),
        json!(get_condition_last_transition_time(
            existing_conditions,
            cond_type,
            status,
            now,
        )),
    );
    if let Some(r) = reason {
        obj.insert("reason".to_string(), json!(r));
    }
    if let Some(m) = message {
        obj.insert("message".to_string(), json!(m));
    }
    Value::Object(obj)
}

fn log_pod_status_write_result(
    writer: &'static str,
    namespace: &str,
    name: &str,
    old_rv: i64,
    previous_pod: &Value,
    updated: &Resource,
) -> bool {
    let paths = pod_object_debug_changed_paths(previous_pod, &updated.data);
    let changed_fields = paths.join(",");
    if paths.is_empty() {
        tracing::debug!(
            target: "klights::pod_status",
            writer,
            namespace,
            pod = name,
            old_rv,
            new_rv = updated.resource_version,
            "pod status writer returned unchanged pod object"
        );
        false
    } else {
        tracing::debug!(
            target: "klights::pod_status",
            writer,
            namespace,
            pod = name,
            old_rv,
            new_rv = updated.resource_version,
            changed_count = paths.len(),
            changed_fields = %changed_fields,
            "pod status writer persisted pod object changes"
        );
        true
    }
}

#[cfg(test)]
fn pod_status_debug_changed_paths(pod: &Value, next_status: &Value) -> Vec<String> {
    let mut next_pod = pod.clone();
    if let Some(obj) = next_pod.as_object_mut() {
        obj.insert("status".to_string(), next_status.clone());
    }
    pod_object_debug_changed_paths(pod, &next_pod)
}

fn pod_object_debug_changed_paths(previous: &Value, updated: &Value) -> Vec<String> {
    let mut paths = Vec::new();
    collect_debug_changed_paths(previous, updated, "", &mut paths);
    paths
}

fn collect_debug_changed_paths(
    previous: &Value,
    updated: &Value,
    path: &str,
    out: &mut Vec<String>,
) {
    match (previous, updated) {
        (Value::Object(previous), Value::Object(updated)) => {
            let mut keys: Vec<&str> = previous
                .keys()
                .chain(updated.keys())
                .map(String::as_str)
                .collect();
            keys.sort_unstable();
            keys.dedup();
            for key in keys {
                if path.is_empty() && key == "metadata" {
                    collect_metadata_changed_paths(previous.get(key), updated.get(key), key, out);
                    continue;
                }
                let next_path = if path.is_empty() {
                    key.to_string()
                } else {
                    format!("{path}.{key}")
                };
                match (previous.get(key), updated.get(key)) {
                    (Some(left), Some(right)) => {
                        collect_debug_changed_paths(left, right, &next_path, out)
                    }
                    _ => out.push(next_path),
                }
            }
        }
        (Value::Array(previous), Value::Array(updated)) => {
            let max_len = previous.len().max(updated.len());
            for idx in 0..max_len {
                let next_path = format!("{path}[{idx}]");
                match (previous.get(idx), updated.get(idx)) {
                    (Some(left), Some(right)) => {
                        collect_debug_changed_paths(left, right, &next_path, out)
                    }
                    _ => out.push(next_path),
                }
            }
        }
        _ if previous != updated => out.push(path.to_string()),
        _ => {}
    }
}

fn collect_metadata_changed_paths(
    previous: Option<&Value>,
    updated: Option<&Value>,
    path: &str,
    out: &mut Vec<String>,
) {
    let (Some(Value::Object(previous)), Some(Value::Object(updated))) = (previous, updated) else {
        if previous != updated {
            out.push(path.to_string());
        }
        return;
    };
    let mut keys: Vec<&str> = previous
        .keys()
        .chain(updated.keys())
        .map(String::as_str)
        .filter(|key| *key != "resourceVersion")
        .collect();
    keys.sort_unstable();
    keys.dedup();
    for key in keys {
        let next_path = format!("{path}.{key}");
        match (previous.get(key), updated.get(key)) {
            (Some(left), Some(right)) => collect_debug_changed_paths(left, right, &next_path, out),
            _ => out.push(next_path),
        }
    }
}

fn can_publish_probe_ready(status: &Value, container_name: &str) -> bool {
    if status.get("phase").and_then(|v| v.as_str()) != Some("Running") {
        return false;
    }

    let Some(statuses) = status.get("containerStatuses").and_then(|s| s.as_array()) else {
        return true;
    };
    let Some(container_status) = statuses
        .iter()
        .find(|s| s.get("name").and_then(|n| n.as_str()) == Some(container_name))
    else {
        return true;
    };

    if container_status.get("started").and_then(|v| v.as_bool()) == Some(false) {
        return false;
    }
    if container_status.pointer("/state/waiting").is_some()
        || container_status.pointer("/state/terminated").is_some()
    {
        return false;
    }

    true
}

fn apply_terminal_readiness_conditions(status: &mut serde_json::Map<String, Value>) {
    let Some(phase) = status.get("phase").and_then(|v| v.as_str()) else {
        return;
    };
    let reason = match phase {
        "Succeeded" => "PodCompleted",
        "Failed" => "PodFailed",
        _ => return,
    };
    let now = crate::utils::k8s_timestamp();
    let conditions = status
        .entry("conditions".to_string())
        .or_insert_with(|| json!([]));
    if !conditions.is_array() {
        *conditions = json!([]);
    }
    let Some(conditions) = conditions.as_array_mut() else {
        return;
    };
    upsert_terminal_readiness_condition(conditions, "Ready", reason, &now);
    upsert_terminal_readiness_condition(conditions, "ContainersReady", reason, &now);
}

fn apply_runtime_readiness_conditions(status: &mut serde_json::Map<String, Value>) {
    let Some(phase) = status.get("phase").and_then(|v| v.as_str()) else {
        return;
    };
    if matches!(phase, "Succeeded" | "Failed") {
        apply_terminal_readiness_conditions(status);
        return;
    }

    let all_containers_ready = status
        .get("containerStatuses")
        .and_then(|s| s.as_array())
        .is_some_and(|statuses| {
            !statuses.is_empty()
                && statuses
                    .iter()
                    .all(|s| s.get("ready").and_then(|r| r.as_bool()).unwrap_or(false))
        });
    let containers_ready_status = if all_containers_ready {
        "True"
    } else {
        "False"
    };
    let ready_status = if phase == "Running" && all_containers_ready {
        "True"
    } else {
        "False"
    };
    let now = crate::utils::k8s_timestamp();
    let existing_conditions = status
        .get("conditions")
        .and_then(|c| c.as_array())
        .cloned()
        .unwrap_or_default();
    let conditions = status
        .entry("conditions".to_string())
        .or_insert_with(|| json!([]));
    if !conditions.is_array() {
        *conditions = json!([]);
    }
    let Some(conditions) = conditions.as_array_mut() else {
        return;
    };
    upsert_runtime_readiness_condition(
        conditions,
        &existing_conditions,
        "ContainersReady",
        containers_ready_status,
        &now,
    );
    upsert_runtime_readiness_condition(
        conditions,
        &existing_conditions,
        "Ready",
        ready_status,
        &now,
    );
}

fn upsert_runtime_readiness_condition(
    conditions: &mut Vec<Value>,
    existing_conditions: &[Value],
    cond_type: &str,
    cond_status: &str,
    now: &str,
) {
    let transition_time =
        get_condition_last_transition_time(existing_conditions, cond_type, cond_status, now);
    for condition in conditions.iter_mut() {
        if condition.get("type").and_then(|t| t.as_str()) == Some(cond_type) {
            if let Some(obj) = condition.as_object_mut() {
                // Only update if the status actually changed
                let prev_status = obj.get("status").and_then(|v| v.as_str()).unwrap_or("");
                if prev_status == cond_status {
                    return; // unchanged — skip to preserve exact JSON for no-op detection
                }
                obj.insert("status".to_string(), json!(cond_status));
                obj.insert("lastTransitionTime".to_string(), json!(transition_time));
                obj.remove("reason");
                obj.remove("message");
            }
            return;
        }
    }
    conditions.push(json!({
        "type": cond_type,
        "status": cond_status,
        "lastTransitionTime": transition_time,
    }));
}

fn upsert_terminal_readiness_condition(
    conditions: &mut Vec<Value>,
    cond_type: &str,
    reason: &str,
    now: &str,
) {
    for condition in conditions.iter_mut() {
        if condition.get("type").and_then(|v| v.as_str()) != Some(cond_type) {
            continue;
        }
        let flipped = condition.get("status").and_then(|v| v.as_str()) != Some("False");
        if let Some(obj) = condition.as_object_mut() {
            obj.insert("status".to_string(), json!("False"));
            obj.insert("reason".to_string(), json!(reason));
            if flipped || !obj.contains_key("lastTransitionTime") {
                obj.insert("lastTransitionTime".to_string(), json!(now));
            }
        }
        return;
    }
    conditions.push(json!({
        "type": cond_type,
        "status": "False",
        "reason": reason,
        "lastTransitionTime": now,
    }));
}

fn is_conflict(err: &anyhow::Error) -> bool {
    crate::datastore::errors::is_conflict_error(err)
}

fn preserve_restart_fields(
    pod: &Value,
) -> std::collections::HashMap<String, (Option<Value>, Option<Value>)> {
    let mut out = std::collections::HashMap::new();
    if let Some(existing_statuses) = pod
        .pointer("/status/containerStatuses")
        .and_then(|s| s.as_array())
    {
        for existing in existing_statuses {
            let Some(name) = existing.get("name").and_then(|n| n.as_str()) else {
                continue;
            };
            let restart_count = existing.get("restartCount").cloned();
            let last_state = existing.get("lastState").cloned();
            out.insert(name.to_string(), (restart_count, last_state));
        }
    }
    out
}

fn apply_preserved_restart_fields(
    status: &mut Value,
    preserved: &std::collections::HashMap<String, (Option<Value>, Option<Value>)>,
) {
    let Some(name) = status.get("name").and_then(|n| n.as_str()) else {
        return;
    };
    let Some((restart_count, last_state)) = preserved.get(name) else {
        return;
    };
    if let Some(obj) = status.as_object_mut() {
        if let Some(v) = restart_count {
            let preserved_count = v.as_i64().unwrap_or(0);
            let current_count = obj
                .get("restartCount")
                .and_then(|value| value.as_i64())
                .unwrap_or(0);
            if !obj.contains_key("restartCount") || preserved_count > current_count {
                obj.insert("restartCount".to_string(), v.clone());
            }
        }
        if obj.get("lastState").is_none()
            && let Some(v) = last_state
        {
            obj.insert("lastState".to_string(), v.clone());
        }
    }
}

fn has_create_container_config_error_status(statuses: &[Value]) -> bool {
    statuses.iter().any(|status| {
        status
            .pointer("/state/waiting/reason")
            .and_then(|reason| reason.as_str())
            == Some("CreateContainerConfigError")
    })
}

fn container_statuses_for_log(pod: &Value) -> String {
    pod.pointer("/status/containerStatuses")
        .map(Value::to_string)
        .unwrap_or_else(|| "<none>".to_string())
}

/// The CRI reports container statuses without the `ready` or `started`
/// fields — those are kubelet-side decisions.  Preserve them from the
/// existing pod status so the Ready condition stays correct after a
/// runtime reconcile.  Does NOT overwrite fields that are already true
/// (a container that becomes ready stays ready).
fn preserve_ready_and_started(
    new_status: &mut serde_json::Map<String, serde_json::Value>,
    pod_data: &serde_json::Value,
) {
    let Some(new_cs) = new_status
        .get_mut("containerStatuses")
        .and_then(|v| v.as_array_mut())
    else {
        return;
    };
    let Some(old_arr) = pod_data
        .pointer("/status/containerStatuses")
        .and_then(|v| v.as_array())
    else {
        return;
    };
    for new in new_cs.iter_mut() {
        let Some(name) = new.get("name").and_then(|n| n.as_str()) else {
            continue;
        };
        let Some(old) = old_arr
            .iter()
            .find(|o| o.get("name").and_then(|n| n.as_str()) == Some(name))
        else {
            continue;
        };
        if let Some(obj) = new.as_object_mut() {
            if !obj.contains_key("ready")
                && let Some(ready) = old.get("ready")
            {
                obj.insert("ready".to_string(), ready.clone());
            }
            if !obj.contains_key("started")
                && let Some(started) = old.get("started")
            {
                obj.insert("started".to_string(), started.clone());
            }
        }
    }
}

#[cfg(test)]
mod debug_diff_tests {
    use serde_json::json;

    #[test]
    fn pod_status_debug_changed_paths_returns_empty_for_noop() {
        let pod = json!({
            "status": {
                "phase": "Running",
                "containerStatuses": [
                    {"name": "app", "ready": true}
                ]
            }
        });
        let next_status = pod["status"].clone();

        assert!(super::pod_status_debug_changed_paths(&pod, &next_status).is_empty());
    }

    #[test]
    fn pod_status_debug_changed_paths_reports_nested_status_fields() {
        let pod = json!({
            "status": {
                "phase": "Pending",
                "containerStatuses": [
                    {"name": "app", "ready": false}
                ]
            }
        });
        let next_status = json!({
            "phase": "Running",
            "containerStatuses": [
                {"name": "app", "ready": true}
            ]
        });

        let paths = super::pod_status_debug_changed_paths(&pod, &next_status);

        assert_eq!(
            paths,
            vec![
                "status.containerStatuses[0].ready".to_string(),
                "status.phase".to_string(),
            ]
        );
    }
}
