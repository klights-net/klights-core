//! Policy-aware Pod repository facade. Concrete datastore access is owned by
//! the private root composition persistence adapter.
//!
//! `pod_network` and `sandbox` table access is intentionally NOT routed
//! through this hub — those are network-runtime / GC concerns owned by
//! `src/networking/cni.rs`, `crates/klights-kubelet/src/sandbox_gc.rs`, `src/shutdown.rs`,
//! `src/kubelet/pod_sandbox.rs`, and `src/datastore/sqlite/crud/sandbox_network.rs`.

use anyhow::Result;
use serde_json::Value;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(any(test, feature = "pod-repository-test-support"))]
use tokio::sync::broadcast;

use klights_cluster_core::{PatchKind, Resource, ResourcePreconditions};
use klights_pod_api::{
    PodRepositoryCreateRequest, PodRepositoryError, PodRepositoryGetRequest,
    PodRepositoryListRequest, PodRepositoryOwnerListRequest, PodRepositoryPatchRequest,
    PodRepositoryReadPersistence, PodRepositoryReplaceRequest, PodRepositoryStatusNoop,
    PodRepositoryStatusRequest, PodRepositoryWritePersistence,
};
#[cfg(any(test, feature = "pod-repository-test-support"))]
use klights_watch::WatchEvent;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ActorPodDeleteObservation {
    Ready {
        resource_version: i64,
        node_name: String,
    },
    IdentityChanged,
    FinalizersPending,
    Retry,
}

fn pod_is_terminating_or_node_lost(pod: &Value) -> bool {
    pod.pointer("/metadata/deletionTimestamp")
        .and_then(|value| value.as_str())
        .is_some_and(|value| !value.is_empty())
        || (pod.pointer("/status/phase").and_then(Value::as_str) == Some("Failed")
            && pod.pointer("/status/reason").and_then(Value::as_str) == Some("NodeLost"))
}

#[cfg(any(test, feature = "pod-repository-test-support"))]
pub(crate) trait PodRepositoryWatchPersistence: Send + Sync {
    fn pod_watch_receiver(&self) -> tokio::sync::broadcast::Receiver<WatchEvent>;
}

pub struct PodStore {
    reads: Arc<dyn PodRepositoryReadPersistence>,
    writes: Arc<dyn PodRepositoryWritePersistence>,
    #[cfg(any(test, feature = "pod-repository-test-support"))]
    watches: Option<Arc<dyn PodRepositoryWatchPersistence>>,
    wall_clock: Arc<dyn klights_kubelet::runtime_clock::RuntimeClock>,
    /// Incremented on every pod create/delete to signal sandbox GC that a sweep may be needed.
    pub(super) sandbox_gc_dirty: Arc<AtomicUsize>,
}

impl PodStore {
    pub(crate) fn from_persistence(
        reads: Arc<dyn PodRepositoryReadPersistence>,
        writes: Arc<dyn PodRepositoryWritePersistence>,
        wall_clock: Arc<dyn klights_kubelet::runtime_clock::RuntimeClock>,
        sandbox_gc_dirty: Arc<AtomicUsize>,
        #[cfg(any(test, feature = "pod-repository-test-support"))] watches: Option<
            Arc<dyn PodRepositoryWatchPersistence>,
        >,
    ) -> Self {
        Self {
            reads,
            writes,
            #[cfg(any(test, feature = "pod-repository-test-support"))]
            watches,
            wall_clock,
            sandbox_gc_dirty,
        }
    }

    fn mark_sandbox_dirty(&self) {
        self.sandbox_gc_dirty.fetch_add(1, Ordering::Release);
    }

    /// Borrow the underlying datastore handle. Reserved for the limited
    /// set of repository services that legitimately need a non-Pod DB
    /// surface (see `mod.rs` doc comment). Outside `pod_repository/`,
    /// callers must always go through the typed methods.
    pub(crate) async fn get(&self, ns: &str, name: &str) -> Result<Option<Resource>> {
        self.reads
            .get_persisted_pod(PodRepositoryGetRequest {
                namespace: ns.to_string(),
                name: name.to_string(),
            })
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn list(
        &self,
        ns: Option<&str>,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        limit: Option<i64>,
        continue_token: Option<&str>,
    ) -> Result<klights_pod_api::PodListResult> {
        let result = self
            .reads
            .list_persisted_pods(PodRepositoryListRequest {
                namespace: ns.map(str::to_string),
                label_selector: label_selector.map(str::to_string),
                field_selector: field_selector.map(str::to_string),
                limit,
                continue_token: continue_token.map(str::to_string),
            })
            .await?;
        let (items, resource_version, continue_token, remaining_item_count) = result.into_parts();
        klights_pod_api::PodListResult::try_new(
            items,
            resource_version,
            continue_token,
            remaining_item_count,
        )
        .map_err(Into::into)
    }

    pub(crate) async fn snapshot_list(
        &self,
        request: klights_pod_api::PodSnapshotListRequest,
    ) -> Result<klights_pod_api::PodSnapshotListOutcome> {
        self.reads
            .snapshot_persisted_pods(request)
            .await
            .map_err(Into::into)
    }

    pub(super) async fn list_by_owner(&self, ns: &str, owner_uid: &str) -> Result<Vec<Resource>> {
        self.reads
            .list_persisted_pods_by_owner(PodRepositoryOwnerListRequest {
                namespace: ns.to_string(),
                owner_uid: owner_uid.to_string(),
            })
            .await
            .map_err(Into::into)
    }

    #[cfg(feature = "pod-repository-test-support")]
    pub(crate) async fn integration_list_by_owner(
        &self,
        namespace: &str,
        owner_uid: &str,
    ) -> Result<Vec<Resource>> {
        self.list_by_owner(namespace, owner_uid).await
    }

    pub(crate) async fn create(&self, ns: &str, name: &str, body: Value) -> Result<Resource> {
        self.mark_sandbox_dirty();
        self.writes
            .create_persisted_pod(PodRepositoryCreateRequest {
                namespace: ns.to_string(),
                name: name.to_string(),
                body,
            })
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn update(
        &self,
        ns: &str,
        name: &str,
        mut body: Value,
        expected_rv: i64,
    ) -> Result<Resource> {
        let current = self
            .get(ns, name)
            .await?
            .ok_or_else(|| PodRepositoryError::not_found(ns, name))?;
        preserve_status_from_current(&current.data, &mut body);
        self.mark_sandbox_dirty();
        self.writes
            .replace_persisted_pod(PodRepositoryReplaceRequest {
                namespace: ns.to_string(),
                name: name.to_string(),
                body,
                preconditions: ResourcePreconditions {
                    uid: Some(current.uid),
                    resource_version: Some(expected_rv),
                },
            })
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn patch_metadata(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
        expected_rv: i64,
        patch: Value,
    ) -> Result<Resource> {
        self.writes
            .patch_persisted_pod(PodRepositoryPatchRequest {
                namespace: ns.to_string(),
                name: name.to_string(),
                patch_kind: PatchKind::Merge,
                patch,
                preconditions: ResourcePreconditions {
                    uid: Some(uid.to_string()),
                    resource_version: Some(expected_rv),
                },
            })
            .await?
            .ok_or_else(|| PodRepositoryError::not_found(ns, name).into())
    }

    pub(super) async fn mark_deleting_latest(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
        body: &Value,
    ) -> Result<Resource> {
        let metadata = body.get("metadata").and_then(|m| m.as_object());
        let deletion_timestamp = metadata
            .and_then(|m| m.get("deletionTimestamp"))
            .filter(|value| !value.is_null())
            .cloned()
            .unwrap_or_else(|| {
                Value::String(klights_cluster_core::k8s_time::format_legacy_timestamp(
                    self.wall_clock.now_utc(),
                ))
            });
        let deletion_grace_period_seconds = metadata
            .and_then(|m| m.get("deletionGracePeriodSeconds"))
            .cloned()
            .unwrap_or(Value::Null);
        let patch = serde_json::json!({
            "metadata": {
                "deletionTimestamp": deletion_timestamp,
                "deletionGracePeriodSeconds": deletion_grace_period_seconds
            }
        });
        self.mark_sandbox_dirty();
        self.writes
            .patch_persisted_pod(PodRepositoryPatchRequest {
                namespace: ns.to_string(),
                name: name.to_string(),
                patch_kind: PatchKind::Merge,
                patch,
                preconditions: ResourcePreconditions {
                    uid: Some(uid.to_string()),
                    resource_version: None,
                },
            })
            .await?
            .ok_or_else(|| PodRepositoryError::not_found(ns, name).into())
    }

    #[cfg(feature = "pod-repository-test-support")]
    pub(crate) async fn integration_mark_deleting_latest(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        body: &Value,
    ) -> Result<Resource> {
        self.mark_deleting_latest(namespace, name, uid, body).await
    }

    pub(super) async fn mark_deleting_at_resource_version(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
        body: Value,
        expected_rv: i64,
    ) -> Result<Resource> {
        self.mark_sandbox_dirty();
        self.writes
            .replace_persisted_pod(PodRepositoryReplaceRequest {
                namespace: ns.to_string(),
                name: name.to_string(),
                body,
                preconditions: ResourcePreconditions {
                    uid: Some(uid.to_string()),
                    resource_version: Some(expected_rv),
                },
            })
            .await
            .map_err(Into::into)
    }

    /// Internal scheduler path: bind `spec.nodeName` and update the
    /// PodScheduled condition in one datastore mutation. Normal Pod API update
    /// paths must use `update()`, which preserves status.
    pub(crate) async fn update_including_status_for_scheduler(
        &self,
        ns: &str,
        name: &str,
        body: Value,
        expected_rv: i64,
    ) -> Result<Resource> {
        let current = self
            .get(ns, name)
            .await?
            .ok_or_else(|| PodRepositoryError::not_found(ns, name))?;
        self.mark_sandbox_dirty();
        self.writes
            .replace_persisted_pod(PodRepositoryReplaceRequest {
                namespace: ns.to_string(),
                name: name.to_string(),
                body,
                preconditions: ResourcePreconditions {
                    uid: Some(current.uid),
                    resource_version: Some(expected_rv),
                },
            })
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn update_status_typed(
        &self,
        ns: &str,
        name: &str,
        status: Value,
        expected_rv: Option<i64>,
    ) -> std::result::Result<Resource, PodRepositoryError> {
        let current = self
            .reads
            .get_persisted_pod(PodRepositoryGetRequest {
                namespace: ns.to_string(),
                name: name.to_string(),
            })
            .await?
            .ok_or_else(|| PodRepositoryError::not_found(ns, name))?;
        if current.data.get("status") == Some(&status) {
            if let Some(expected) = expected_rv
                && expected != current.resource_version
            {
                return Err(PodRepositoryError::conflict(format!(
                    "resourceVersion precondition failed: expected {} got {}",
                    expected, current.resource_version
                )));
            }
            self.writes
                .log_persisted_pod_status_noop(PodRepositoryStatusNoop {
                    namespace: ns,
                    name,
                    resource: &current,
                });
            return Ok(current);
        }
        self.writes
            .write_persisted_pod_status(PodRepositoryStatusRequest {
                namespace: ns.to_string(),
                name: name.to_string(),
                status,
                preconditions: ResourcePreconditions {
                    uid: Some(current.uid),
                    resource_version: expected_rv,
                },
            })
            .await
    }

    #[cfg(feature = "pod-repository-test-support")]
    async fn update_status(
        &self,
        ns: &str,
        name: &str,
        status: Value,
        expected_rv: Option<i64>,
    ) -> Result<Resource> {
        self.update_status_typed(ns, name, status, expected_rv)
            .await
            .map_err(Into::into)
    }

    #[cfg(feature = "pod-repository-test-support")]
    pub(crate) async fn integration_update_status(
        &self,
        ns: &str,
        name: &str,
        status: Value,
        expected_rv: Option<i64>,
    ) -> Result<Resource> {
        self.update_status(ns, name, status, expected_rv).await
    }

    pub(crate) fn classify_bound_finalization(
        &self,
        current: Option<&Resource>,
        uid: &str,
    ) -> ActorPodDeleteObservation {
        classify_bound_finalization(current, uid)
    }

    #[cfg(any(test, feature = "pod-repository-test-support"))]
    pub(super) fn subscribe_watch(&self) -> broadcast::Receiver<WatchEvent> {
        self.watches
            .as_ref()
            .expect("watch subscription is unavailable for this persistence adapter")
            .pod_watch_receiver()
    }
}

pub(crate) fn classify_bound_finalization(
    current: Option<&Resource>,
    uid: &str,
) -> ActorPodDeleteObservation {
    let Some(current) = current else {
        return ActorPodDeleteObservation::IdentityChanged;
    };
    if current.uid != uid {
        return ActorPodDeleteObservation::IdentityChanged;
    }
    let Some(node_name) = current
        .data
        .pointer("/spec/nodeName")
        .and_then(Value::as_str)
        .filter(|node| !node.trim().is_empty())
    else {
        return ActorPodDeleteObservation::Retry;
    };
    if !pod_is_terminating_or_node_lost(&current.data) {
        return ActorPodDeleteObservation::Retry;
    }
    if current
        .data
        .pointer("/metadata/finalizers")
        .and_then(Value::as_array)
        .is_some_and(|finalizers| !finalizers.is_empty())
    {
        return ActorPodDeleteObservation::FinalizersPending;
    }
    ActorPodDeleteObservation::Ready {
        resource_version: current.resource_version,
        node_name: node_name.to_string(),
    }
}

impl klights_pod_api::PodQuery for PodStore {
    fn get_pod(
        &self,
        request: klights_pod_api::PodGetRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Option<Resource>> {
        Box::pin(async move {
            let pod = self
                .reads
                .get_persisted_pod(PodRepositoryGetRequest {
                    namespace: request.namespace().to_string(),
                    name: request.name().to_string(),
                })
                .await?;
            Ok(match request.uid() {
                Some(uid) => pod.filter(|pod| pod.uid == uid),
                None => pod,
            })
        })
    }

    fn list_pods(
        &self,
        request: klights_pod_api::PodListRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, klights_pod_api::PodListResult> {
        Box::pin(async move {
            self.reads
                .list_persisted_pods(PodRepositoryListRequest {
                    namespace: request.namespace().map(str::to_string),
                    label_selector: request.label_selector().map(str::to_string),
                    field_selector: request.field_selector().map(str::to_string),
                    limit: request.limit(),
                    continue_token: request.continue_token().map(str::to_string),
                })
                .await
        })
    }

    fn list_pods_by_owner_uid(
        &self,
        request: klights_pod_api::PodOwnerListRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Vec<Resource>> {
        self.reads
            .list_persisted_pods_by_owner(PodRepositoryOwnerListRequest {
                namespace: request.namespace().to_string(),
                owner_uid: request.owner_uid().to_string(),
            })
    }
}

pub(crate) fn preserve_status_from_current(current: &Value, next: &mut Value) {
    let Some(next_obj) = next.as_object_mut() else {
        return;
    };
    if let Some(status) = current.get("status") {
        next_obj.insert("status".to_string(), status.clone());
    } else {
        next_obj.remove("status");
    }
}
