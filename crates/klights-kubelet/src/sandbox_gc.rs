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
//!     list get dropped, along with their matching `pod_networks` rows.

use crate::cgroup_cleanup::cleanup_pod_cgroup;
use crate::cri::CriClient;
use anyhow::Result;
use async_trait::async_trait;
use k8s_cri::v1::PodSandbox;
use klights_node_store::{CacheNetworkError, PodNetworkCache, SandboxKey};
use klights_pod_api::{PodGetRequest, PodQuery};
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Mutex;

/// Maximum orphan sandboxes torn down per tick. Keeps the event loop snappy
/// even under sustained leak pressure (large backlog drains over several ticks).
const MAX_PER_TICK: usize = 64;

#[async_trait]
trait SandboxRuntime: Send + Sync {
    async fn list_pod_sandboxes(&self) -> Result<Vec<PodSandbox>>;
    async fn stop_pod_sandbox(&self, sandbox_id: &str) -> Result<()>;
    async fn remove_pod_sandbox(&self, sandbox_id: &str) -> Result<()>;
}

#[async_trait]
impl SandboxRuntime for Mutex<CriClient> {
    async fn list_pod_sandboxes(&self) -> Result<Vec<PodSandbox>> {
        self.lock().await.list_pod_sandboxes(None).await
    }

    async fn stop_pod_sandbox(&self, sandbox_id: &str) -> Result<()> {
        self.lock().await.stop_pod_sandbox(sandbox_id).await
    }

    async fn remove_pod_sandbox(&self, sandbox_id: &str) -> Result<()> {
        self.lock().await.remove_pod_sandbox(sandbox_id).await
    }
}

pub struct SandboxGc {
    pod_network_cache: Arc<dyn PodNetworkCache>,
    pod_runtime_store: Arc<dyn klights_node_store::PodRuntimeStore>,
    runtime: Arc<dyn SandboxRuntime>,
    pod_query: Arc<dyn PodQuery>,
    containerd_ns: String,
    /// Shared counter: incremented by PodStore on create/update/delete.
    /// Zero when the cluster has been quiescent — no sweep needed.
    dirty: Arc<AtomicUsize>,
    file_process: klights_supervisor::FileProcessExecutor,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct SweepOutcome {
    removed: usize,
    retry_needed: bool,
}

impl SandboxGc {
    pub fn new(
        pod_network_cache: Arc<dyn PodNetworkCache>,
        pod_runtime_store: Arc<dyn klights_node_store::PodRuntimeStore>,
        cri: Arc<Mutex<CriClient>>,
        pod_query: Arc<dyn PodQuery>,
        containerd_ns: impl Into<String>,
        dirty: Arc<AtomicUsize>,
        file_process: klights_supervisor::FileProcessExecutor,
    ) -> Self {
        let runtime: Arc<dyn SandboxRuntime> = cri;
        Self::with_runtime(
            pod_network_cache,
            pod_runtime_store,
            runtime,
            pod_query,
            containerd_ns,
            dirty,
            file_process,
        )
    }

    fn with_runtime(
        pod_network_cache: Arc<dyn PodNetworkCache>,
        pod_runtime_store: Arc<dyn klights_node_store::PodRuntimeStore>,
        runtime: Arc<dyn SandboxRuntime>,
        pod_query: Arc<dyn PodQuery>,
        containerd_ns: impl Into<String>,
        dirty: Arc<AtomicUsize>,
        file_process: klights_supervisor::FileProcessExecutor,
    ) -> Self {
        Self {
            pod_network_cache,
            pod_runtime_store,
            runtime,
            pod_query,
            containerd_ns: containerd_ns.into(),
            dirty,
            file_process,
        }
    }

    async fn list_live_sandbox_ids(&self) -> Result<HashSet<String>> {
        let sandboxes = self.runtime.list_pod_sandboxes().await?;
        Ok(sandboxes.into_iter().map(|sb| sb.id).collect())
    }

    async fn delete_cached_network(&self, sandbox_id: &str) -> Result<(), CacheNetworkError> {
        PodNetworkCache::delete_network_for_sandbox(
            self.pod_network_cache.as_ref(),
            SandboxKey::try_new(sandbox_id)?,
        )
        .await
    }

    async fn list_cached_network_ids(&self) -> Result<Vec<String>, CacheNetworkError> {
        PodNetworkCache::list_network_assignments(self.pod_network_cache.as_ref())
            .await
            .map(|rows| {
                rows.into_iter()
                    .map(|row| row.request().sandbox_id().to_string())
                    .collect()
            })
    }

    /// Run one sweep and report whether any transiently incomplete work must
    /// remain armed for the next supervised cadence.
    async fn sweep(&self) -> Result<SweepOutcome> {
        let sandboxes = self.runtime.list_pod_sandboxes().await?;

        let mut live_sandbox_ids: HashSet<String> = HashSet::with_capacity(sandboxes.len());
        let mut stale_sandbox_row_ids: HashSet<String> = HashSet::new();
        let mut removed = 0usize;
        let mut retry_needed = false;

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

            let request = match PodGetRequest::try_by_name(&meta.namespace, &meta.name) {
                Ok(request) => request,
                Err(error) => {
                    retry_needed = true;
                    tracing::debug!(
                        sandbox_id = %sandbox.id,
                        ns = %meta.namespace,
                        name = %meta.name,
                        error = %error,
                        "sandbox_gc: invalid Pod identity, skipping this tick"
                    );
                    continue;
                }
            };
            let live_pod = match self.pod_query.get_pod(request).await {
                Ok(p) => p,
                Err(e) => {
                    retry_needed = true;
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

            if let Err(e) = self.runtime.stop_pod_sandbox(&sandbox.id).await {
                retry_needed = true;
                tracing::warn!(
                    sandbox_id = %sandbox.id,
                    error = %e,
                    "sandbox_gc: stop_pod_sandbox failed; will retry next tick"
                );
                continue;
            }
            if let Err(e) = self.runtime.remove_pod_sandbox(&sandbox.id).await {
                retry_needed = true;
                tracing::warn!(
                    sandbox_id = %sandbox.id,
                    error = %e,
                    "sandbox_gc: remove_pod_sandbox failed; will retry next tick"
                );
                continue;
            }
            if !cleanup_pod_cgroup_for_gc(
                &self.file_process,
                &self.containerd_ns,
                &meta.uid,
                &sandbox.id,
                "runtime orphan sandbox",
            )
            .await
            {
                retry_needed = true;
                removed += 1;
                continue;
            }

            // Best-effort SQLite cleanup. Use UID+sandbox-id qualification so
            // GC for an old orphan cannot delete a replacement Pod's sandbox row.
            if let Err(e) = self.delete_runtime_for_match(&meta.uid, &sandbox.id).await {
                retry_needed = true;
                tracing::debug!(
                    ns = %meta.namespace,
                    name = %meta.name,
                    error = %e,
                    "sandbox_gc: SQLite delete_sandbox_for_uid failed"
                );
            }
            if let Err(e) = self.delete_cached_network(&sandbox.id).await {
                retry_needed = true;
                tracing::debug!(
                    sandbox_id = %sandbox.id,
                    error = %e,
                    "sandbox_gc: SQLite delete_pod_network failed"
                );
            }
            removed += 1;
        }

        // Second pass: drop SQLite pod_sandboxes rows whose sandbox_id has
        // disappeared from CRI. Records were never the leak themselves; this
        // just keeps the table from accumulating dead entries.
        match self.pod_runtime_store.list_pod_runtime().await {
            Ok(rows) => {
                for sb in rows {
                    let Some(sandbox_id) = sb.sandbox_id().map(str::to_string) else {
                        continue;
                    };
                    if !live_sandbox_ids.contains(&sandbox_id) {
                        if !cleanup_pod_cgroup_for_gc(
                            &self.file_process,
                            &self.containerd_ns,
                            &sb.pod().uid,
                            &sandbox_id,
                            "stale sandbox row",
                        )
                        .await
                        {
                            retry_needed = true;
                            continue;
                        }
                        stale_sandbox_row_ids.insert(sandbox_id.clone());
                        if let Err(e) = self
                            .delete_runtime_for_match(&sb.pod().uid, &sandbox_id)
                            .await
                        {
                            retry_needed = true;
                            tracing::debug!(
                                ns = %sb.pod().namespace,
                                pod = %sb.pod().name,
                                error = %e,
                                "sandbox_gc: failed to drop stale pod_sandboxes row"
                            );
                        }
                    }
                }
            }
            Err(e) => {
                retry_needed = true;
                tracing::debug!(
                    error = %e,
                    "sandbox_gc: list_sandboxes failed; skipping table cleanup this tick"
                );
            }
        }
        let refresh_result = self.list_live_sandbox_ids().await;
        if let Err(ref e) = refresh_result {
            retry_needed = true;
            tracing::debug!(
                error = %e,
                "sandbox_gc: live sandbox refresh failed; using initial snapshot for pod_networks cleanup"
            );
        }
        let live_ids_for_network_cleanup =
            pod_network_cleanup_live_ids(&live_sandbox_ids, refresh_result);

        match self.list_cached_network_ids().await {
            Ok(sandbox_ids) => {
                for sandbox_id in pod_network_cleanup_candidates(
                    sandbox_ids,
                    &live_ids_for_network_cleanup,
                    &stale_sandbox_row_ids,
                ) {
                    if let Err(e) = self.delete_cached_network(&sandbox_id).await {
                        retry_needed = true;
                        tracing::debug!(
                            sandbox_id = %sandbox_id,
                            error = %e,
                            "sandbox_gc: failed to drop stale pod_networks row"
                        );
                    }
                }
            }
            Err(e) => {
                retry_needed = true;
                tracing::debug!(
                    error = %e,
                    "sandbox_gc: list_pod_network_sandbox_ids failed; skipping pod_networks cleanup this tick"
                );
            }
        }

        if removed > 0 {
            tracing::info!(
                orphan_sandbox_gc = true,
                removed,
                "sandbox_gc: tick complete"
            );
        }
        Ok(SweepOutcome {
            removed,
            retry_needed,
        })
    }

    async fn delete_runtime_for_match(&self, pod_uid: &str, sandbox_id: &str) -> Result<()> {
        let key = klights_node_store::RuntimePodUid::try_new(pod_uid)?;
        let Some(row) = self.pod_runtime_store.get_pod_runtime(key).await? else {
            return Ok(());
        };
        if row.sandbox_id() == Some(sandbox_id) {
            self.pod_runtime_store
                .delete_pod_runtime_for_uid(klights_node_store::RuntimePodUid::try_new(pod_uid)?)
                .await?;
        }
        Ok(())
    }
}

async fn cleanup_pod_cgroup_for_gc(
    file_process: &klights_supervisor::FileProcessExecutor,
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

    match cleanup_pod_cgroup(file_process, containerd_ns, pod_uid).await {
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

fn pod_network_cleanup_candidates(
    network_sandbox_ids: Vec<String>,
    live_sandbox_ids: &HashSet<String>,
    stale_sandbox_row_ids: &HashSet<String>,
) -> Vec<String> {
    network_sandbox_ids
        .into_iter()
        .filter(|sandbox_id| {
            !live_sandbox_ids.contains(sandbox_id) && stale_sandbox_row_ids.contains(sandbox_id)
        })
        .collect()
}

impl SandboxGc {
    pub async fn run_if_dirty(&self) -> Result<()> {
        // Event-driven: skip the CRI list if no pod lifecycle events have occurred
        // since the last successful sweep.
        let pending = self.dirty.swap(0, Ordering::Acquire);
        if pending == 0 {
            return Ok(());
        }
        match self.sweep().await {
            Ok(outcome) => {
                // A successful removal may expose the next bounded batch. Any
                // transiently incomplete item must also survive until the next
                // supervised cadence. No task or polling loop is created here.
                if outcome.removed > 0 || outcome.retry_needed {
                    self.dirty.fetch_add(1, Ordering::Release);
                }
                Ok(())
            }
            Err(error) => {
                // The initial CRI inventory failed before a complete sweep was
                // possible. Preserve one pending unit for the existing cadence.
                self.dirty.fetch_add(1, Ordering::Release);
                Err(error)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        SandboxGc, SandboxRuntime, pod_network_cleanup_candidates, pod_network_cleanup_live_ids,
    };
    use anyhow::{Result, anyhow};
    use k8s_cri::v1::{PodSandbox, PodSandboxMetadata};
    use klights_cluster_core::Resource;
    use klights_node_store::{
        CacheNetworkFuture, OwnedPodSandbox, PodNetworkAllocationRequest,
        PodNetworkAssignmentSnapshot, PodNetworkCache, PodNetworkEndpoint, PodRuntimeAdmission,
        PodRuntimeCgroup, PodRuntimeRecord, PodRuntimeStore, PodUidKey, RuntimeNamespace,
        RuntimePodUid, RuntimeWorkFuture, SandboxKey,
    };
    use klights_pod_api::{
        PodGetRequest, PodListRequest, PodListResult, PodOwnerListRequest, PodQuery,
        PodRepositoryError, PodRepositoryFuture,
    };
    use std::collections::{HashSet, VecDeque};
    use std::sync::Arc;
    use std::sync::Mutex as StdMutex;
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

    #[test]
    fn pod_network_cleanup_skips_inflight_rows_without_stale_sandbox_record() {
        let stale_sandbox_rows = HashSet::from(["sandbox-stale".to_string()]);
        let selected = pod_network_cleanup_candidates(
            vec![
                "sandbox-inflight".to_string(),
                "sandbox-stale".to_string(),
                "sandbox-live".to_string(),
            ],
            &HashSet::from(["sandbox-live".to_string()]),
            &stale_sandbox_rows,
        );

        assert_eq!(selected, vec!["sandbox-stale".to_string()]);
        assert!(
            !selected.contains(&"sandbox-inflight".to_string()),
            "network rows created by CNI before RunPodSandbox returns must not be deleted"
        );
    }

    fn sandbox(uid: &str) -> PodSandbox {
        PodSandbox {
            id: "sandbox-1".to_string(),
            metadata: Some(PodSandboxMetadata {
                name: "pod-1".to_string(),
                uid: uid.to_string(),
                namespace: "default".to_string(),
                attempt: 0,
            }),
            ..Default::default()
        }
    }

    fn take_failure(remaining: &AtomicUsize) -> bool {
        remaining
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |count| {
                count.checked_sub(1)
            })
            .is_ok()
    }

    struct ScriptedRuntime {
        sandboxes: StdMutex<Vec<PodSandbox>>,
        list_failures: AtomicUsize,
        stop_failures: AtomicUsize,
        remove_failures: AtomicUsize,
        list_calls: AtomicUsize,
        stop_calls: AtomicUsize,
        remove_calls: AtomicUsize,
    }

    impl ScriptedRuntime {
        fn new(sandboxes: Vec<PodSandbox>) -> Self {
            Self {
                sandboxes: StdMutex::new(sandboxes),
                list_failures: AtomicUsize::new(0),
                stop_failures: AtomicUsize::new(0),
                remove_failures: AtomicUsize::new(0),
                list_calls: AtomicUsize::new(0),
                stop_calls: AtomicUsize::new(0),
                remove_calls: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait::async_trait]
    impl SandboxRuntime for ScriptedRuntime {
        async fn list_pod_sandboxes(&self) -> Result<Vec<PodSandbox>> {
            self.list_calls.fetch_add(1, Ordering::SeqCst);
            if take_failure(&self.list_failures) {
                anyhow::bail!("transient sandbox list failure");
            }
            Ok(self.sandboxes.lock().unwrap().clone())
        }

        async fn stop_pod_sandbox(&self, _sandbox_id: &str) -> Result<()> {
            self.stop_calls.fetch_add(1, Ordering::SeqCst);
            if take_failure(&self.stop_failures) {
                anyhow::bail!("transient sandbox stop failure");
            }
            Ok(())
        }

        async fn remove_pod_sandbox(&self, sandbox_id: &str) -> Result<()> {
            self.remove_calls.fetch_add(1, Ordering::SeqCst);
            if take_failure(&self.remove_failures) {
                anyhow::bail!("transient sandbox remove failure");
            }
            self.sandboxes
                .lock()
                .unwrap()
                .retain(|sandbox| sandbox.id != sandbox_id);
            Ok(())
        }
    }

    enum QueryOutcome {
        Missing,
        Live(String),
        Fail,
    }

    struct ScriptedPodQuery {
        outcomes: StdMutex<VecDeque<QueryOutcome>>,
        calls: AtomicUsize,
    }

    impl ScriptedPodQuery {
        fn new(outcomes: impl IntoIterator<Item = QueryOutcome>) -> Self {
            Self {
                outcomes: StdMutex::new(outcomes.into_iter().collect()),
                calls: AtomicUsize::new(0),
            }
        }
    }

    impl PodQuery for ScriptedPodQuery {
        fn get_pod(&self, _request: PodGetRequest) -> PodRepositoryFuture<'_, Option<Resource>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let outcome = self
                .outcomes
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(QueryOutcome::Missing);
            Box::pin(async move {
                match outcome {
                    QueryOutcome::Missing => Ok(None),
                    QueryOutcome::Live(uid) => {
                        Resource::try_from_data(Arc::new(serde_json::json!({
                            "apiVersion": "v1",
                            "kind": "Pod",
                            "metadata": {
                                "namespace": "default",
                                "name": "pod-1",
                                "uid": uid,
                            },
                        })))
                        .map(Some)
                        .map_err(|error| PodRepositoryError::unavailable(error.to_string()))
                    }
                    QueryOutcome::Fail => Err(PodRepositoryError::unavailable(
                        "transient Pod query failure",
                    )),
                }
            })
        }

        fn list_pods(&self, _request: PodListRequest) -> PodRepositoryFuture<'_, PodListResult> {
            Box::pin(async { Err(PodRepositoryError::unavailable("unused list operation")) })
        }

        fn list_pods_by_owner_uid(
            &self,
            _request: PodOwnerListRequest,
        ) -> PodRepositoryFuture<'_, Vec<Resource>> {
            Box::pin(async { Err(PodRepositoryError::unavailable("unused owner query")) })
        }
    }

    struct EmptyNodeStores;

    impl PodNetworkCache for EmptyNodeStores {
        fn get_network_for_uid(
            &self,
            _pod_uid: PodUidKey,
        ) -> CacheNetworkFuture<'_, Option<PodNetworkEndpoint>> {
            panic!("unused network UID lookup")
        }

        fn get_network_for_pod(
            &self,
            _pod: klights_types::PodIdentity,
        ) -> CacheNetworkFuture<'_, Option<PodNetworkEndpoint>> {
            panic!("unused network Pod lookup")
        }

        fn get_network_for_sandbox(
            &self,
            _sandbox_id: SandboxKey,
        ) -> CacheNetworkFuture<'_, Option<PodNetworkEndpoint>> {
            panic!("unused network sandbox lookup")
        }

        fn get_network_for_assignment(
            &self,
            _sandbox_id: SandboxKey,
            _pod: klights_types::PodIdentity,
        ) -> CacheNetworkFuture<'_, Option<PodNetworkEndpoint>> {
            panic!("unused network assignment lookup")
        }

        fn delete_network_for_sandbox(
            &self,
            _sandbox_id: SandboxKey,
        ) -> CacheNetworkFuture<'_, ()> {
            Box::pin(async { Ok(()) })
        }

        fn delete_network_if_matches(
            &self,
            _request: PodNetworkAllocationRequest,
        ) -> CacheNetworkFuture<'_, bool> {
            panic!("unused conditional network delete")
        }

        fn list_network_assignments(
            &self,
        ) -> CacheNetworkFuture<'_, Vec<PodNetworkAssignmentSnapshot>> {
            Box::pin(async { Ok(Vec::new()) })
        }
    }

    impl PodRuntimeStore for EmptyNodeStores {
        fn admit_pod_runtime(&self, _admission: PodRuntimeAdmission) -> RuntimeWorkFuture<'_, ()> {
            panic!("unused runtime admission")
        }

        fn record_owned_sandbox(&self, _sandbox: OwnedPodSandbox) -> RuntimeWorkFuture<'_, ()> {
            panic!("unused sandbox recording")
        }

        fn record_cgroup(&self, _cgroup: PodRuntimeCgroup) -> RuntimeWorkFuture<'_, ()> {
            panic!("unused cgroup recording")
        }

        fn delete_pod_runtime_for_uid(&self, _pod_uid: RuntimePodUid) -> RuntimeWorkFuture<'_, ()> {
            panic!("unused runtime deletion")
        }

        fn get_pod_runtime(
            &self,
            _pod_uid: RuntimePodUid,
        ) -> RuntimeWorkFuture<'_, Option<PodRuntimeRecord>> {
            Box::pin(async { Ok(None) })
        }

        fn list_pod_runtime(&self) -> RuntimeWorkFuture<'_, Vec<PodRuntimeRecord>> {
            Box::pin(async { Ok(Vec::new()) })
        }

        fn list_pod_runtime_by_namespace(
            &self,
            _namespace: RuntimeNamespace,
        ) -> RuntimeWorkFuture<'_, Vec<PodRuntimeRecord>> {
            panic!("unused namespace runtime list")
        }
    }

    fn real_gc(
        runtime: Arc<ScriptedRuntime>,
        pod_query: Arc<ScriptedPodQuery>,
        dirty: Arc<AtomicUsize>,
    ) -> SandboxGc {
        let stores = Arc::new(EmptyNodeStores);
        let supervisor = klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        );
        SandboxGc::with_runtime(
            stores.clone(),
            stores,
            runtime,
            pod_query,
            "klights",
            dirty,
            klights_supervisor::FileProcessExecutor::from_supervisor(&supervisor),
        )
    }

    #[tokio::test]
    async fn event_driven_gc_skips_tick_when_clean() {
        let dirty = Arc::new(AtomicUsize::new(1));
        let runtime = Arc::new(ScriptedRuntime::new(Vec::new()));
        let gc = real_gc(runtime.clone(), Arc::new(ScriptedPodQuery::new([])), dirty);

        gc.run_if_dirty().await.unwrap();
        assert_eq!(runtime.list_calls.load(Ordering::SeqCst), 2);
        gc.run_if_dirty().await.unwrap();
        assert_eq!(runtime.list_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn event_driven_gc_runs_after_mark_dirty() {
        let dirty = Arc::new(AtomicUsize::new(1));
        let runtime = Arc::new(ScriptedRuntime::new(Vec::new()));
        let gc = real_gc(
            runtime.clone(),
            Arc::new(ScriptedPodQuery::new([])),
            dirty.clone(),
        );

        gc.run_if_dirty().await.unwrap();
        assert_eq!(runtime.list_calls.load(Ordering::SeqCst), 2);
        gc.run_if_dirty().await.unwrap();
        assert_eq!(runtime.list_calls.load(Ordering::SeqCst), 2);
        dirty.fetch_add(1, Ordering::Release);
        gc.run_if_dirty().await.unwrap();
        assert_eq!(runtime.list_calls.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn event_driven_gc_mark_dirty_during_idle_doesnt_cause_double_sweep() {
        let dirty = Arc::new(AtomicUsize::new(0));
        let runtime = Arc::new(ScriptedRuntime::new(Vec::new()));
        let gc = real_gc(
            runtime.clone(),
            Arc::new(ScriptedPodQuery::new([])),
            dirty.clone(),
        );

        gc.run_if_dirty().await.unwrap();
        assert_eq!(runtime.list_calls.load(Ordering::SeqCst), 0);
        dirty.fetch_add(1, Ordering::Release);
        dirty.fetch_add(1, Ordering::Release);
        gc.run_if_dirty().await.unwrap();
        assert_eq!(runtime.list_calls.load(Ordering::SeqCst), 2);
        assert_eq!(dirty.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn transient_list_failure_rearms_real_sandbox_gc_for_next_cadence() {
        let dirty = Arc::new(AtomicUsize::new(1));
        let runtime = Arc::new(ScriptedRuntime::new(Vec::new()));
        runtime.list_failures.store(1, Ordering::SeqCst);
        let gc = real_gc(
            runtime.clone(),
            Arc::new(ScriptedPodQuery::new([])),
            dirty.clone(),
        );

        assert!(gc.run_if_dirty().await.is_err());
        assert_eq!(dirty.load(Ordering::Acquire), 1);
        gc.run_if_dirty().await.unwrap();
        assert_eq!(runtime.list_calls.load(Ordering::SeqCst), 3);
        gc.run_if_dirty().await.unwrap();
        assert_eq!(runtime.list_calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn transient_pod_query_failure_rearms_real_sandbox_gc_for_next_cadence() {
        let dirty = Arc::new(AtomicUsize::new(1));
        let runtime = Arc::new(ScriptedRuntime::new(vec![sandbox("uid-1")]));
        let query = Arc::new(ScriptedPodQuery::new([
            QueryOutcome::Fail,
            QueryOutcome::Live("uid-1".to_string()),
        ]));
        let gc = real_gc(runtime.clone(), query.clone(), dirty.clone());

        gc.run_if_dirty().await.unwrap();
        assert_eq!(dirty.load(Ordering::Acquire), 1);
        gc.run_if_dirty().await.unwrap();
        assert_eq!(query.calls.load(Ordering::SeqCst), 2);
        assert_eq!(dirty.load(Ordering::Acquire), 0);
        let list_calls = runtime.list_calls.load(Ordering::SeqCst);
        gc.run_if_dirty().await.unwrap();
        assert_eq!(runtime.list_calls.load(Ordering::SeqCst), list_calls);
    }

    #[tokio::test]
    async fn transient_stop_failure_rearms_real_sandbox_gc_for_next_cadence() {
        let dirty = Arc::new(AtomicUsize::new(1));
        let runtime = Arc::new(ScriptedRuntime::new(vec![sandbox("")]));
        runtime.stop_failures.store(1, Ordering::SeqCst);
        let gc = real_gc(
            runtime.clone(),
            Arc::new(ScriptedPodQuery::new([])),
            dirty.clone(),
        );

        gc.run_if_dirty().await.unwrap();
        assert_eq!(dirty.load(Ordering::Acquire), 1);
        gc.run_if_dirty().await.unwrap();
        assert_eq!(runtime.stop_calls.load(Ordering::SeqCst), 2);
        assert_eq!(runtime.remove_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn transient_remove_failure_rearms_real_sandbox_gc_for_next_cadence() {
        let dirty = Arc::new(AtomicUsize::new(1));
        let runtime = Arc::new(ScriptedRuntime::new(vec![sandbox("")]));
        runtime.remove_failures.store(1, Ordering::SeqCst);
        let gc = real_gc(
            runtime.clone(),
            Arc::new(ScriptedPodQuery::new([])),
            dirty.clone(),
        );

        gc.run_if_dirty().await.unwrap();
        assert_eq!(dirty.load(Ordering::Acquire), 1);
        gc.run_if_dirty().await.unwrap();
        assert_eq!(runtime.stop_calls.load(Ordering::SeqCst), 2);
        assert_eq!(runtime.remove_calls.load(Ordering::SeqCst), 2);
    }
}
