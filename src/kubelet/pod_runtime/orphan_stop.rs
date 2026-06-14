//! Orphan pod stop teardown, extracted from the `service` hub.
//!
//! Under pod churn the per-UID lifecycle actor can have already exited by the
//! time the leader's delete propagates, so the delete is finalized via the
//! orphan path with no deleted-Pod snapshot. This helper still tears down the
//! UID-bound runtime state. Critically it resolves the sandbox via the caller
//! hint, then the node-local store row, then the authoritative runtime (CRI, by
//! UID). HR #11 requires runtime cleanup to be confirmed before the slot is
//! freed, so it must not skip straight to `clear_slot` and leak a running
//! sandbox — "clear slot only" is correct solely when CRI confirms none.

// Methods are invoked on `Arc<dyn Trait>` fields of RealPodRuntimeService;
// trait-object dispatch does not require the traits to be in scope.
use crate::kubelet::pod_runtime::service::{PodRuntimeKey, RealPodRuntimeService};

impl RealPodRuntimeService {
    /// Persist a readiness-probe result, deduped through the actor's in-memory
    /// status emitter so repeated identical `ReadinessChanged` signals — which
    /// the leader's full-status no-op guard would discard anyway — never
    /// re-cross the worker→leader boundary. A genuine flip re-emits and carries
    /// its downstream side effects; a failed write retries.
    pub(super) async fn handle_readiness_changed(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        container_name: &str,
        ready: bool,
    ) -> anyhow::Result<()> {
        tracing::debug!(
            namespace = namespace,
            pod = pod_name,
            uid = pod_uid,
            container = container_name,
            ready = ready,
            "readiness changed"
        );
        let emit_key = PodRuntimeKey::new(namespace, pod_name, pod_uid);
        let emitted = self
            .status_emitter
            .emit_readiness_if_changed(&emit_key, container_name, ready, |ready| async move {
                self.repository
                    .set_probe_readiness_for_uid(
                        namespace,
                        pod_name,
                        pod_uid,
                        container_name,
                        ready,
                        None,
                    )
                    .await?;
                Ok::<(), anyhow::Error>(())
            })
            .await?;
        if !emitted {
            tracing::debug!(
                target: "klights::pod_status",
                namespace = %namespace,
                pod = %pod_name,
                uid = %pod_uid,
                container = %container_name,
                ready,
                "readiness emit suppressed because actor memory cache already has identical readiness"
            );
        }
        Ok(())
    }

    pub(super) async fn stop_orphan_pod(
        &self,
        key: &PodRuntimeKey,
        sandbox_hint: Option<String>,
    ) -> anyhow::Result<()> {
        let sandbox_ids = self.resolve_orphan_sandbox_ids(key, sandbox_hint).await?;

        if sandbox_ids.is_empty() {
            tracing::warn!(
                namespace = %key.namespace,
                name = %key.name,
                uid = %key.uid,
                "no sandbox resolved for orphan pod stop (CRI reports none); clearing slot"
            );
        }

        for sandbox_id in &sandbox_ids {
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
                        "failed to list containers for orphan pod stop: {:#}",
                        e
                    );
                    Vec::new()
                });
            for (container_id, state) in containers {
                if state.is_running() {
                    let _ = self.cri.stop_container(&container_id, 0).await;
                }
                let _ = self.cri.remove_container(&container_id).await;
            }
            let _ = self.cri.stop_pod_sandbox(sandbox_id).await;
            let _ = self.cri.remove_pod_sandbox(sandbox_id).await;
            let _ = self.filesystem.cleanup_cgroup(key, sandbox_id).await;
            let _ = self.network.release_sandbox_network(key, sandbox_id).await;
        }
        if !sandbox_ids.is_empty() {
            let _ = self.store.delete_sandbox(key).await;
        }
        self.cleanup_pod_local_artifacts(key, None).await;
        let _ = self.slot_admission.clear_slot(key).await;
        Ok(())
    }

    /// Resolve the sandbox(es) to tear down for an orphan stop: caller hint,
    /// then node-local store row, then CRI listed by pod UID. The CRI fallback
    /// is the correctness fix — under churn the hint and store row can both be
    /// absent while a sandbox is still running.
    async fn resolve_orphan_sandbox_ids(
        &self,
        key: &PodRuntimeKey,
        sandbox_hint: Option<String>,
    ) -> anyhow::Result<Vec<String>> {
        if let Some(id) = sandbox_hint.filter(|id| !id.trim().is_empty()) {
            return Ok(vec![id]);
        }
        if let Some(id) = self
            .store
            .get_sandbox_id(key)
            .await?
            .filter(|id| !id.trim().is_empty())
        {
            return Ok(vec![id]);
        }
        match self.cri.list_pod_sandboxes(Some(key.uid.as_str())).await {
            Ok(found) => Ok(found
                .into_iter()
                .map(|(id, _state)| id)
                .filter(|id| !id.trim().is_empty())
                .collect()),
            Err(e) => {
                tracing::warn!(
                    namespace = %key.namespace,
                    name = %key.name,
                    uid = %key.uid,
                    "failed to list CRI sandboxes for orphan pod stop: {:#}",
                    e
                );
                Ok(Vec::new())
            }
        }
    }
}
