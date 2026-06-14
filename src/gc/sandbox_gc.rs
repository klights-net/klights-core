//! Orphan CRI-sandbox sweep (P0-LEAK-01).
//!
//! Three failure modes leave a sandbox + netns + veth alive forever in spite
//! of the 3-tier `resolve_sandbox_id_for_delete()` fallback:
//!  1. apiserver DELETE event reached us but klights crashed mid-delete
//!  2. SQLite row written at RunPodSandbox but no DELETE event ever arrives
//!  3. Pod was re-created with a fresh UID; the old sandbox's `metadata.uid`
//!     no longer matches anything live
//!
//! Each tick:
//!   * `cri.list_pod_sandboxes(None)` → for every sandbox, look up the live
//!     Pod by namespace+name and compare uid. No-pod or uid-mismatch ⇒ orphan.
//!   * Up to `MAX_PER_TICK` orphans are torn down (`stop_pod_sandbox`,
//!     `remove_pod_sandbox`, `db.delete_sandbox`, `db.delete_pod_network`)
//!     per tick — keeps the event loop snappy under sustained leak pressure.
//!   * Second pass: `pod_sandboxes` rows whose sandbox_id is not in the CRI
//!     list get dropped and stale `pod_networks` rows for missing sandbox IDs
//!     are reclaimed (prevents IPAM exhaustion after partial failures).

use crate::datastore::DatastoreHandle;
use crate::kubelet::cgroup_cleanup::cleanup_pod_cgroup;
use crate::kubelet::cri::CriClient;
use crate::kubelet::pod_repository::PodReader;
use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Mutex;

/// Maximum orphan sandboxes torn down per tick. Keeps the event loop snappy
/// even under sustained leak pressure (large backlog drains over several ticks).
pub const MAX_PER_TICK: usize = 64;

pub struct SandboxGc {
    db: DatastoreHandle,
    cri: Arc<Mutex<CriClient>>,
    pod_reader: Arc<dyn PodReader>,
    containerd_ns: String,
    /// Shared counter: incremented by PodStore on create/update/delete.
    /// Zero when the cluster has been quiescent — no sweep needed.
    dirty: Arc<AtomicUsize>,
}

impl SandboxGc {
    pub fn new(
        db: DatastoreHandle,
        cri: Arc<Mutex<CriClient>>,
        pod_reader: Arc<dyn PodReader>,
        containerd_ns: impl Into<String>,
        dirty: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            db,
            cri,
            pod_reader,
            containerd_ns: containerd_ns.into(),
            dirty,
        }
    }

    async fn list_live_sandbox_ids(&self) -> Result<HashSet<String>> {
        let mut cri = self.cri.lock().await;
        let sandboxes = cri.list_pod_sandboxes(None).await?;
        Ok(sandboxes.into_iter().map(|sb| sb.id).collect())
    }

    /// Run one sweep. Returns the number of orphan sandboxes removed.
    /// Public so the kubelet can call it once at startup after the initial
    /// reconcile, catching sandboxes orphaned during the previous lifetime.
    pub async fn sweep(&self) -> Result<usize> {
        let mut cri = self.cri.lock().await;
        let sandboxes = cri.list_pod_sandboxes(None).await?;

        let mut live_sandbox_ids: HashSet<String> = HashSet::with_capacity(sandboxes.len());
        let mut removed = 0usize;

        for sandbox in &sandboxes {
            live_sandbox_ids.insert(sandbox.id.clone());
            if removed >= MAX_PER_TICK {
                continue;
            }

            let Some(meta) = sandbox.metadata.as_ref() else {
                // Sandbox without metadata cannot be matched to any Pod — leave it
                // alone; an admin or an upstream tool may be managing it.
                continue;
            };
            if meta.namespace.is_empty() || meta.name.is_empty() {
                continue;
            }

            let live_pod = match self.pod_reader.get_pod(&meta.namespace, &meta.name).await {
                Ok(p) => p,
                Err(e) => {
                    tracing::debug!(
                        sandbox_id = %sandbox.id,
                        ns = %meta.namespace,
                        name = %meta.name,
                        error = %e,
                        "sandbox_gc: failed to look up Pod, skipping this tick"
                    );
                    continue;
                }
            };

            let orphan_reason = match live_pod {
                None => "no live Pod",
                Some(ref p) => {
                    let pod_uid = p
                        .data
                        .pointer("/metadata/uid")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    if !meta.uid.is_empty() && !pod_uid.is_empty() && pod_uid != meta.uid {
                        "Pod uid differs from sandbox uid"
                    } else {
                        ""
                    }
                }
            };

            if orphan_reason.is_empty() {
                continue;
            }

            tracing::warn!(
                orphan_sandbox_gc = true,
                sandbox_id = %sandbox.id,
                ns = %meta.namespace,
                name = %meta.name,
                sandbox_uid = %meta.uid,
                reason = %orphan_reason,
                "sandbox_gc: removing orphan sandbox"
            );

            if let Err(e) = cri.stop_pod_sandbox(&sandbox.id).await {
                tracing::warn!(
                    sandbox_id = %sandbox.id,
                    error = %e,
                    "sandbox_gc: stop_pod_sandbox failed; will retry next tick"
                );
                continue;
            }
            if let Err(e) = cri.remove_pod_sandbox(&sandbox.id).await {
                tracing::warn!(
                    sandbox_id = %sandbox.id,
                    error = %e,
                    "sandbox_gc: remove_pod_sandbox failed; will retry next tick"
                );
                continue;
            }
            if !cleanup_pod_cgroup_for_gc(
                &self.containerd_ns,
                &meta.uid,
                &sandbox.id,
                "runtime orphan sandbox",
            )
            .await
            {
                removed += 1;
                continue;
            }

            // Best-effort SQLite cleanup. Use UID+sandbox-id qualification so
            // GC for an old orphan cannot delete a replacement Pod's sandbox row.
            if let Err(e) = self
                .db
                .delete_sandbox_for_uid(&meta.namespace, &meta.name, &meta.uid, &sandbox.id)
                .await
            {
                tracing::debug!(
                    ns = %meta.namespace,
                    name = %meta.name,
                    error = %e,
                    "sandbox_gc: SQLite delete_sandbox_for_uid failed"
                );
            }
            if let Err(e) = self.db.delete_pod_network(&sandbox.id).await {
                tracing::debug!(
                    sandbox_id = %sandbox.id,
                    error = %e,
                    "sandbox_gc: SQLite delete_pod_network failed"
                );
            }
            removed += 1;
        }

        // Drop the CRI lock before walking SQLite — second pass only needs DB.
        drop(cri);

        // Second pass: drop SQLite pod_sandboxes rows whose sandbox_id has
        // disappeared from CRI. Records were never the leak themselves; this
        // just keeps the table from accumulating dead entries.
        match self.db.list_sandboxes().await {
            Ok(rows) => {
                for sb in rows {
                    if !live_sandbox_ids.contains(&sb.sandbox_id) {
                        if !cleanup_pod_cgroup_for_gc(
                            &self.containerd_ns,
                            &sb.pod_uid,
                            &sb.sandbox_id,
                            "stale sandbox row",
                        )
                        .await
                        {
                            continue;
                        }
                        if let Err(e) = self
                            .db
                            .delete_sandbox_for_uid(
                                &sb.namespace,
                                &sb.pod_name,
                                &sb.pod_uid,
                                &sb.sandbox_id,
                            )
                            .await
                        {
                            tracing::debug!(
                                ns = %sb.namespace,
                                pod = %sb.pod_name,
                                error = %e,
                                "sandbox_gc: failed to drop stale pod_sandboxes row"
                            );
                        }
                    }
                }
            }
            Err(e) => tracing::debug!(
                error = %e,
                "sandbox_gc: list_sandboxes failed; skipping table cleanup this tick"
            ),
        }
        let refresh_result = self.list_live_sandbox_ids().await;
        if let Err(ref e) = refresh_result {
            tracing::debug!(
                error = %e,
                "sandbox_gc: live sandbox refresh failed; using initial snapshot for pod_networks cleanup"
            );
        }
        let live_ids_for_network_cleanup =
            pod_network_cleanup_live_ids(&live_sandbox_ids, refresh_result);

        match self.db.list_pod_network_sandbox_ids().await {
            Ok(sandbox_ids) => {
                for sandbox_id in sandbox_ids {
                    if !live_ids_for_network_cleanup.contains(&sandbox_id)
                        && let Err(e) = self.db.delete_pod_network(&sandbox_id).await
                    {
                        tracing::debug!(
                            sandbox_id = %sandbox_id,
                            error = %e,
                            "sandbox_gc: failed to drop stale pod_networks row"
                        );
                    }
                }
            }
            Err(e) => tracing::debug!(
                error = %e,
                "sandbox_gc: list_pod_network_sandbox_ids failed; skipping pod_networks cleanup this tick"
            ),
        }

        if removed > 0 {
            tracing::info!(
                orphan_sandbox_gc = true,
                removed,
                "sandbox_gc: tick complete"
            );
        }
        Ok(removed)
    }
}

async fn cleanup_pod_cgroup_for_gc(
    containerd_ns: &str,
    pod_uid: &str,
    sandbox_id: &str,
    source: &str,
) -> bool {
    if pod_uid.trim().is_empty() {
        tracing::debug!(
            sandbox_id = %sandbox_id,
            source = %source,
            "sandbox_gc: pod cgroup cleanup skipped because pod UID is missing"
        );
        return true;
    }

    match cleanup_pod_cgroup(containerd_ns, pod_uid).await {
        Ok(0) => {
            tracing::debug!(
                sandbox_id = %sandbox_id,
                pod_uid = %pod_uid,
                source = %source,
                "sandbox_gc: no pod cgroup directories remained"
            );
            true
        }
        Ok(removed) => {
            tracing::info!(
                sandbox_id = %sandbox_id,
                pod_uid = %pod_uid,
                removed,
                source = %source,
                "sandbox_gc: removed pod cgroup directories"
            );
            true
        }
        Err(e) => {
            tracing::warn!(
                sandbox_id = %sandbox_id,
                pod_uid = %pod_uid,
                source = %source,
                error = %e,
                "sandbox_gc: pod cgroup cleanup failed; will retry while sandbox row remains"
            );
            false
        }
    }
}

fn pod_network_cleanup_live_ids(
    initial_live_ids: &HashSet<String>,
    refreshed_live_ids: Result<HashSet<String>>,
) -> HashSet<String> {
    match refreshed_live_ids {
        Ok(ids) => ids,
        Err(_) => initial_live_ids.clone(),
    }
}

#[async_trait]
impl super::GcTask for SandboxGc {
    fn name(&self) -> &'static str {
        "sandbox_gc"
    }
    async fn run(&self) -> Result<()> {
        // Event-driven: skip the CRI list if no pod lifecycle events have occurred
        // since the last successful sweep.
        let pending = self.dirty.swap(0, Ordering::Acquire);
        if pending == 0 {
            return Ok(());
        }
        let removed = self.sweep().await?;
        // If orphans were found, re-arm the flag so the next tick retries
        // until the cluster is fully clean.
        if removed > 0 {
            self.dirty.fetch_add(1, Ordering::Release);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::pod_network_cleanup_live_ids;
    use crate::gc::GcTask;
    use anyhow::{Result, anyhow};
    use async_trait::async_trait;
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn pod_network_cleanup_prefers_refreshed_live_ids() {
        let initial = HashSet::from(["sandbox-old".to_string()]);
        let refreshed = HashSet::from(["sandbox-old".to_string(), "sandbox-new".to_string()]);

        let selected = pod_network_cleanup_live_ids(&initial, Ok(refreshed.clone()));
        assert_eq!(selected, refreshed);
    }

    #[test]
    fn pod_network_cleanup_falls_back_to_initial_live_ids_when_refresh_fails() {
        let initial = HashSet::from(["sandbox-old".to_string()]);
        let selected = pod_network_cleanup_live_ids(&initial, Err(anyhow!("refresh failed")));
        assert_eq!(selected, initial);
    }

    // ---- Event-driven dirty flag ----

    struct CountingSweepGc {
        dirty: Arc<AtomicUsize>,
        sweep_count: AtomicUsize,
    }

    #[async_trait]
    impl GcTask for CountingSweepGc {
        fn name(&self) -> &'static str {
            "counting_sweep"
        }
        async fn run(&self) -> Result<()> {
            let pending = self.dirty.swap(0, Ordering::Acquire);
            if pending == 0 {
                return Ok(());
            }
            self.sweep_count.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    #[tokio::test]
    async fn event_driven_gc_skips_tick_when_clean() {
        let dirty = Arc::new(AtomicUsize::new(1));
        let gc = CountingSweepGc {
            dirty: dirty.clone(),
            sweep_count: AtomicUsize::new(0),
        };
        // First tick: dirty=1 → runs sweep
        gc.run().await.unwrap();
        assert_eq!(gc.sweep_count.load(Ordering::Relaxed), 1);

        // Second tick: dirty=0 → skipped
        gc.run().await.unwrap();
        assert_eq!(
            gc.sweep_count.load(Ordering::Relaxed),
            1,
            "should skip when clean"
        );
    }

    #[tokio::test]
    async fn event_driven_gc_runs_after_mark_dirty() {
        let dirty = Arc::new(AtomicUsize::new(1));
        let gc = CountingSweepGc {
            dirty: dirty.clone(),
            sweep_count: AtomicUsize::new(0),
        };
        // First tick: runs
        gc.run().await.unwrap();
        assert_eq!(gc.sweep_count.load(Ordering::Relaxed), 1);

        // Second tick: skipped (clean)
        gc.run().await.unwrap();
        assert_eq!(gc.sweep_count.load(Ordering::Relaxed), 1);

        // Mark dirty → next tick runs
        dirty.fetch_add(1, Ordering::Release);
        gc.run().await.unwrap();
        assert_eq!(gc.sweep_count.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn event_driven_gc_mark_dirty_during_idle_doesnt_cause_double_sweep() {
        let dirty = Arc::new(AtomicUsize::new(0));
        let gc = CountingSweepGc {
            dirty: dirty.clone(),
            sweep_count: AtomicUsize::new(0),
        };
        // Tick while clean: skipped
        gc.run().await.unwrap();
        assert_eq!(gc.sweep_count.load(Ordering::Relaxed), 0);

        // Mark dirty twice (two pod creates), then tick: one sweep
        dirty.fetch_add(1, Ordering::Release);
        dirty.fetch_add(1, Ordering::Release);
        gc.run().await.unwrap();
        assert_eq!(gc.sweep_count.load(Ordering::Relaxed), 1);
    }
}
