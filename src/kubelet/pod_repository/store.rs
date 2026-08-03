//! `PodStore` — the only file in the crate allowed to call
//! [`crate::datastore::DatastoreBackend`] methods with `("v1","Pod",...)`
//! literals. All other `pod_repository` services depend on
//! `Arc<PodStore>` rather than `DatastoreHandle`, which keeps the
//! pod-shaped DB boundary localized to a single file (enforced by
//! tests/source_guard_tests.py).
//!
//! `pod_network` and `sandbox` table access is intentionally NOT routed
//! through this hub — those are network-runtime / GC concerns owned by
//! `src/networking/cni.rs`, `crates/klights-kubelet/src/sandbox_gc.rs`, `src/shutdown.rs`,
//! `src/kubelet/pod_sandbox.rs`, and `src/datastore/sqlite/crud/sandbox_network.rs`.

use anyhow::Result;
use serde_json::Value;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(test)]
use tokio::sync::broadcast;

use super::PodResourceList;
#[cfg(test)]
use crate::datastore::DatastoreHandle;
#[cfg(test)]
use crate::watch::WatchEvent;
use klights_cluster_core::{PatchKind, Resource, ResourcePreconditions};
use klights_kubelet::unscheduled_deletion::EligibleUnscheduledPodDeletion;
use klights_pod_api::PodRepositoryError;

/// Result of the root-private, actor-owned bound-Pod finalization primitive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BoundPodDeleteOutcome {
    Removed,
    IdentityChanged,
    FinalizersPending,
    Retry,
}

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

/// A hard-delete that reports the row already vanished concurrently, rather
/// than a precondition conflict.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PodDeleteCasOutcome {
    Removed,
    Conflict,
    Gone,
}

#[async_trait::async_trait]
pub(crate) trait PodPersistence: Send + Sync {
    async fn get(&self, namespace: &str, name: &str) -> Result<Option<Resource>>;
    async fn list(
        &self,
        namespace: Option<&str>,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        limit: Option<i64>,
        continue_token: Option<&str>,
    ) -> Result<PodResourceList>;
    async fn snapshot_list(
        &self,
        request: klights_pod_api::PodSnapshotListRequest,
    ) -> Result<klights_pod_api::PodSnapshotListOutcome>;
    async fn list_by_owner(&self, namespace: &str, owner_uid: &str) -> Result<Vec<Resource>>;
    async fn create(&self, namespace: &str, name: &str, body: Value) -> Result<Resource>;
    async fn update(
        &self,
        namespace: &str,
        name: &str,
        body: Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource>;
    async fn patch_latest(
        &self,
        namespace: &str,
        name: &str,
        patch_kind: PatchKind,
        patch: Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Option<Resource>>;
    async fn update_status(
        &self,
        namespace: &str,
        name: &str,
        status: Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource>;
    async fn delete(
        &self,
        namespace: &str,
        name: &str,
        preconditions: ResourcePreconditions,
    ) -> Result<PodDeleteCasOutcome>;
    fn log_status_noop(&self, namespace: &str, name: &str, resource: &Resource);
    #[cfg(test)]
    fn subscribe_watch(&self) -> tokio::sync::broadcast::Receiver<WatchEvent> {
        panic!("watch subscription is unavailable for this persistence adapter")
    }
    #[cfg(test)]
    fn legacy_db(&self) -> DatastoreHandle {
        panic!("legacy datastore access is unavailable for this persistence adapter")
    }
}

pub struct PodStore {
    persistence: Arc<dyn PodPersistence>,
    wall_clock: Arc<dyn klights_kubelet::runtime_clock::RuntimeClock>,
    /// Incremented on every pod create/delete to signal sandbox GC that a sweep may be needed.
    pub(super) sandbox_gc_dirty: Arc<AtomicUsize>,
}

impl PodStore {
    pub(crate) fn from_persistence(
        persistence: Arc<dyn PodPersistence>,
        wall_clock: Arc<dyn klights_kubelet::runtime_clock::RuntimeClock>,
    ) -> Self {
        Self {
            persistence,
            wall_clock,
            sandbox_gc_dirty: Arc::new(AtomicUsize::new(1)),
        }
    }

    #[cfg(test)]
    pub(crate) fn new(db: DatastoreHandle) -> Self {
        crate::pod_repository_composition::new_pod_store(db)
    }

    fn mark_sandbox_dirty(&self) {
        self.sandbox_gc_dirty.fetch_add(1, Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn db(&self) -> DatastoreHandle {
        self.persistence.legacy_db()
    }

    /// Borrow the underlying datastore handle. Reserved for the limited
    /// set of repository services that legitimately need a non-Pod DB
    /// surface (see `mod.rs` doc comment). Outside `pod_repository/`,
    /// callers must always go through the typed methods.
    pub(crate) async fn get(&self, ns: &str, name: &str) -> Result<Option<Resource>> {
        self.persistence.get(ns, name).await
    }

    pub(crate) async fn list(
        &self,
        ns: Option<&str>,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        limit: Option<i64>,
        continue_token: Option<&str>,
    ) -> Result<PodResourceList> {
        self.persistence
            .list(ns, label_selector, field_selector, limit, continue_token)
            .await
    }

    pub(crate) async fn snapshot_list(
        &self,
        request: klights_pod_api::PodSnapshotListRequest,
    ) -> Result<klights_pod_api::PodSnapshotListOutcome> {
        self.persistence.snapshot_list(request).await
    }

    pub(super) async fn list_by_owner(&self, ns: &str, owner_uid: &str) -> Result<Vec<Resource>> {
        self.persistence.list_by_owner(ns, owner_uid).await
    }

    pub(crate) async fn create(&self, ns: &str, name: &str, body: Value) -> Result<Resource> {
        self.mark_sandbox_dirty();
        self.persistence.create(ns, name, body).await
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
        self.persistence
            .update(
                ns,
                name,
                body,
                ResourcePreconditions {
                    uid: Some(current.uid),
                    resource_version: Some(expected_rv),
                },
            )
            .await
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
        self.persistence
            .patch_latest(
                ns,
                name,
                PatchKind::Merge,
                patch,
                ResourcePreconditions {
                    uid: Some(uid.to_string()),
                    resource_version: None,
                },
            )
            .await?
            .ok_or_else(|| PodRepositoryError::not_found(ns, name).into())
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
        self.persistence
            .update(
                ns,
                name,
                body,
                ResourcePreconditions {
                    uid: Some(uid.to_string()),
                    resource_version: Some(expected_rv),
                },
            )
            .await
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
        self.persistence
            .update(
                ns,
                name,
                body,
                ResourcePreconditions {
                    uid: Some(current.uid),
                    resource_version: Some(expected_rv),
                },
            )
            .await
    }

    pub(super) async fn update_status(
        &self,
        ns: &str,
        name: &str,
        status: Value,
        expected_rv: Option<i64>,
    ) -> Result<Resource> {
        let current = self
            .get(ns, name)
            .await?
            .ok_or_else(|| PodRepositoryError::not_found(ns, name))?;
        if current.data.get("status") == Some(&status) {
            if let Some(expected) = expected_rv
                && expected != current.resource_version
            {
                return Err(PodRepositoryError::conflict(format!(
                    "resourceVersion precondition failed: expected {} got {}",
                    expected, current.resource_version
                ))
                .into());
            }
            self.persistence.log_status_noop(ns, name, &current);
            return Ok(current);
        }
        self.persistence
            .update_status(
                ns,
                name,
                status,
                ResourcePreconditions {
                    uid: Some(current.uid),
                    resource_version: expected_rv,
                },
            )
            .await
    }

    /// Remove a bound Pod only after revalidating actor-finalization state at
    /// the datastore boundary.
    ///
    /// The observed resourceVersion CAS closes the race between this
    /// validation read and row removal. Any concurrent bind, finalizer, or
    /// lifecycle write forces the actor to retry from a fresh observation.
    pub(crate) async fn finalize_bound_with_uid(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
    ) -> Result<BoundPodDeleteOutcome> {
        let observed_resource_version = match self.observe_bound_finalization(ns, name, uid).await?
        {
            ActorPodDeleteObservation::Ready {
                resource_version, ..
            } => resource_version,
            ActorPodDeleteObservation::IdentityChanged => {
                return Ok(BoundPodDeleteOutcome::IdentityChanged);
            }
            ActorPodDeleteObservation::FinalizersPending => {
                return Ok(BoundPodDeleteOutcome::FinalizersPending);
            }
            ActorPodDeleteObservation::Retry => return Ok(BoundPodDeleteOutcome::Retry),
        };

        match self
            .persistence
            .delete(
                ns,
                name,
                ResourcePreconditions {
                    uid: Some(uid.to_string()),
                    resource_version: Some(observed_resource_version),
                },
            )
            .await?
        {
            PodDeleteCasOutcome::Removed => {
                self.mark_sandbox_dirty();
                Ok(BoundPodDeleteOutcome::Removed)
            }
            PodDeleteCasOutcome::Conflict => Ok(BoundPodDeleteOutcome::Retry),
            PodDeleteCasOutcome::Gone => Ok(BoundPodDeleteOutcome::IdentityChanged),
        }
    }

    pub(crate) async fn observe_bound_finalization(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
    ) -> Result<ActorPodDeleteObservation> {
        let current = self.get(ns, name).await?;
        Ok(self.classify_bound_finalization(current.as_ref(), uid))
    }

    pub(crate) fn classify_bound_finalization(
        &self,
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

    /// Execute the HR#11 exact UID/resourceVersion CAS authorized by the
    /// kubelet-owned unscheduled-deletion policy.
    ///
    /// The opaque token can be constructed only after a fresh observation
    /// proves the same UID/RV is terminating, finalizer-free, and unbound.
    /// Persistence still performs the exact CAS, so an intervening bind,
    /// resourceVersion change, or same-name replacement cannot be deleted.
    pub(super) async fn delete_unscheduled_with_uid(
        &self,
        eligible: EligibleUnscheduledPodDeletion,
    ) -> Result<PodDeleteCasOutcome> {
        let identity = eligible.identity();
        let outcome = self
            .persistence
            .delete(
                &identity.namespace,
                &identity.name,
                ResourcePreconditions {
                    uid: Some(identity.uid.clone()),
                    resource_version: Some(eligible.observed_resource_version()),
                },
            )
            .await?;
        if outcome == PodDeleteCasOutcome::Removed {
            self.mark_sandbox_dirty();
        }
        Ok(outcome)
    }

    #[cfg(test)]
    pub(super) fn subscribe_watch(&self) -> broadcast::Receiver<WatchEvent> {
        self.persistence.subscribe_watch()
    }
}

#[async_trait::async_trait]
impl super::PodReader for PodStore {
    async fn get_pod(&self, ns: &str, name: &str) -> Result<Option<Resource>> {
        self.get(ns, name).await
    }

    async fn get_pod_for_uid(&self, ns: &str, name: &str, uid: &str) -> Result<Option<Resource>> {
        Ok(self.get(ns, name).await?.filter(|pod| pod.uid == uid))
    }

    async fn list_pods(
        &self,
        ns: Option<&str>,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        limit: Option<i64>,
        continue_token: Option<&str>,
    ) -> Result<PodResourceList> {
        self.list(ns, label_selector, field_selector, limit, continue_token)
            .await
    }
    async fn list_pods_by_owner_uid(&self, ns: &str, owner_uid: &str) -> Result<Vec<Resource>> {
        self.list_by_owner(ns, owner_uid).await
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
