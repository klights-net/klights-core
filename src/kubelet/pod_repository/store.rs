//! `PodStore` — the only file in the crate allowed to call
//! [`crate::datastore::DatastoreBackend`] methods with `("v1","Pod",...)`
//! literals. All other `pod_repository` services depend on
//! `Arc<PodStore>` rather than `DatastoreHandle`, which keeps the
//! pod-shaped DB boundary localized to a single file (enforced by
//! tests/source_guard_tests.py).
//!
//! `pod_network` and `sandbox` table access is intentionally NOT routed
//! through this hub — those are network-runtime / GC concerns owned by
//! `src/networking/cni.rs`, `src/gc/sandbox_gc.rs`, `src/shutdown.rs`,
//! `src/kubelet/pod_sandbox.rs`, and `src/datastore/sqlite/crud/sandbox_network.rs`.

use anyhow::Result;
use serde_json::Value;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(test)]
use tokio::sync::broadcast;

#[cfg(test)]
use crate::datastore::DatastoreBackend;
use crate::datastore::errors::DatastoreError;
use crate::datastore::{
    DatastoreHandle, PatchKind, Resource, ResourceList, ResourcePatchRequest, ResourcePreconditions,
};
#[cfg(test)]
use crate::watch::WatchEvent;

const POD_API_VERSION: &str = "v1";
const POD_KIND: &str = "Pod";

/// Result of [`PodStore::delete_unscheduled_with_uid`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnscheduledPodDeleteOutcome {
    /// The Pod row was removed (or was already gone / superseded by a
    /// same-name replacement UID).
    Removed,
    /// The Pod has been picked up by a kubelet (`spec.nodeName` set), a bind
    /// raced the atomic delete, or the Pod is not actually terminating. Row
    /// removal must go through actor-owned finalization; the caller must not
    /// hard-delete.
    DeferToActor,
    /// The Pod still carries finalizers; removal must wait until they clear.
    FinalizersPending,
}

/// True once a Pod has been bound to a node (a kubelet owns its lifecycle).
fn pod_has_node_assignment(pod: &Value) -> bool {
    pod.pointer("/spec/nodeName")
        .and_then(|node| node.as_str())
        .is_some_and(|node| !node.trim().is_empty())
}

/// A hard-delete that reports the row already vanished concurrently, rather
/// than a precondition conflict.
fn delete_error_means_gone(err: &anyhow::Error) -> bool {
    if let Some(datastore_err) = err.downcast_ref::<DatastoreError>() {
        return matches!(datastore_err, DatastoreError::NotFound { .. });
    }
    format!("{err:#}")
        .to_ascii_lowercase()
        .contains("not found")
}

pub struct PodStore {
    db: DatastoreHandle,
    /// Incremented on every pod create/delete to signal sandbox GC that a sweep may be needed.
    pub(super) sandbox_gc_dirty: Arc<AtomicUsize>,
}

impl PodStore {
    pub fn new(db: DatastoreHandle) -> Self {
        Self {
            db,
            sandbox_gc_dirty: Arc::new(AtomicUsize::new(1)),
        }
    }

    pub fn new_with_dirty(db: DatastoreHandle, dirty: Arc<AtomicUsize>) -> Self {
        Self {
            db,
            sandbox_gc_dirty: dirty,
        }
    }

    fn mark_sandbox_dirty(&self) {
        self.sandbox_gc_dirty.fetch_add(1, Ordering::Release);
    }

    /// Borrow the underlying datastore handle. Reserved for the limited
    /// set of repository services that legitimately need a non-Pod DB
    /// surface (see `mod.rs` doc comment). Outside `pod_repository/`,
    /// callers must always go through the typed methods.
    pub fn db(&self) -> &DatastoreHandle {
        &self.db
    }

    pub async fn get(&self, ns: &str, name: &str) -> Result<Option<Resource>> {
        self.db
            .get_resource(POD_API_VERSION, POD_KIND, Some(ns), name)
            .await
    }

    pub(super) async fn list(
        &self,
        ns: Option<&str>,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        limit: Option<i64>,
        continue_token: Option<&str>,
    ) -> Result<ResourceList> {
        self.db
            .list_resources(
                POD_API_VERSION,
                POD_KIND,
                ns,
                crate::datastore::ResourceListQuery::new(
                    label_selector,
                    field_selector,
                    limit,
                    continue_token,
                ),
            )
            .await
    }

    pub(super) async fn list_by_owner(&self, ns: &str, owner_uid: &str) -> Result<Vec<Resource>> {
        self.db
            .list_resources_by_owner_uid(POD_API_VERSION, POD_KIND, Some(ns), owner_uid)
            .await
    }

    pub(super) async fn create(&self, ns: &str, name: &str, body: Value) -> Result<Resource> {
        self.mark_sandbox_dirty();
        self.db
            .create_resource(POD_API_VERSION, POD_KIND, Some(ns), name, body)
            .await
    }

    pub(super) async fn update(
        &self,
        ns: &str,
        name: &str,
        mut body: Value,
        expected_rv: i64,
    ) -> Result<Resource> {
        let current = self.get(ns, name).await?.ok_or_else(|| {
            DatastoreError::not_found(format!("Pod {ns}/{name} not found for update"))
        })?;
        preserve_status_from_current(&current.data, &mut body);
        self.mark_sandbox_dirty();
        self.db
            .update_resource_with_preconditions(
                POD_API_VERSION,
                POD_KIND,
                Some(ns),
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
            .unwrap_or_else(|| Value::String(crate::utils::k8s_timestamp()));
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
        self.db
            .patch_resource_latest_with_preconditions(
                POD_API_VERSION,
                POD_KIND,
                Some(ns),
                name,
                ResourcePatchRequest::new(
                    PatchKind::Merge,
                    patch,
                    ResourcePreconditions {
                        uid: Some(uid.to_string()),
                        resource_version: None,
                    },
                ),
            )
            .await?
            .ok_or_else(|| {
                DatastoreError::not_found(format!("Pod {ns}/{name} not found for delete mark"))
                    .into()
            })
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
        self.db
            .update_resource_with_preconditions(
                POD_API_VERSION,
                POD_KIND,
                Some(ns),
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
    pub(super) async fn update_including_status_for_scheduler(
        &self,
        ns: &str,
        name: &str,
        body: Value,
        expected_rv: i64,
    ) -> Result<Resource> {
        let current = self.get(ns, name).await?.ok_or_else(|| {
            DatastoreError::not_found(format!("Pod {ns}/{name} not found for scheduler update"))
        })?;
        self.mark_sandbox_dirty();
        self.db
            .update_resource_with_preconditions(
                POD_API_VERSION,
                POD_KIND,
                Some(ns),
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
        let current = self.get(ns, name).await?.ok_or_else(|| {
            DatastoreError::not_found(format!("Pod {ns}/{name} not found for status update"))
        })?;
        if current.data.get("status") == Some(&status) {
            if let Some(expected) = expected_rv
                && expected != current.resource_version
            {
                return Err(DatastoreError::conflict(format!(
                    "resourceVersion precondition failed: expected {} got {}",
                    expected, current.resource_version
                ))
                .into());
            }
            crate::datastore::diagnostics::log_noop_resource_write(
                crate::datastore::diagnostics::NoopResourceWrite {
                    operation: "pod_store_update_status",
                    api_version: POD_API_VERSION,
                    kind: POD_KIND,
                    namespace: Some(ns),
                    name,
                    uid: &current.uid,
                    resource_version: current.resource_version,
                    reason: "pod status unchanged",
                },
            );
            return Ok(current);
        }
        self.db
            .update_status_only_with_preconditions(
                POD_API_VERSION,
                POD_KIND,
                Some(ns),
                name,
                status,
                ResourcePreconditions {
                    uid: Some(current.uid),
                    resource_version: expected_rv,
                },
            )
            .await
    }

    pub async fn delete_with_uid(&self, ns: &str, name: &str, uid: &str) -> Result<()> {
        self.mark_sandbox_dirty();
        self.db
            .delete_resource_with_preconditions(
                POD_API_VERSION,
                POD_KIND,
                Some(ns),
                name,
                ResourcePreconditions {
                    uid: Some(uid.to_string()),
                    resource_version: None,
                },
            )
            .await
    }

    /// HR#11 exception: remove an *unscheduled* Pod row from the leader /
    /// API-server side.
    ///
    /// A Pod that was never bound to a node (`spec.nodeName` empty) has no
    /// kubelet lifecycle actor that will ever finalize it. Once such a Pod is
    /// marked for deletion, its datastore row — and therefore its namespace —
    /// would otherwise linger forever. This is the only non-actor path
    /// permitted to remove a Pod row, and it stays safe by atomically
    /// confirming no kubelet has picked the Pod up: the hard delete is gated on
    /// the exact `resourceVersion` observed while `spec.nodeName` was still
    /// empty. If a scheduler bind lands first, the `resourceVersion` changes,
    /// the compare-and-swap delete no-ops, and the caller must fall back to
    /// actor-owned finalization (which the bind's watch event already wakes).
    ///
    /// Once a kubelet has picked up a Pod (`spec.nodeName` set), only the Pod
    /// lifecycle actor may remove the row — this method refuses
    /// (`DeferToActor`).
    pub async fn delete_unscheduled_with_uid(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
    ) -> Result<UnscheduledPodDeleteOutcome> {
        let Some(current) = self.get(ns, name).await? else {
            // Already gone (actor or a prior sweep finalized it) — idempotent.
            return Ok(UnscheduledPodDeleteOutcome::Removed);
        };
        if current.uid != uid {
            // A same-name replacement Pod owns the slot now; our UID is gone.
            return Ok(UnscheduledPodDeleteOutcome::Removed);
        }
        // A kubelet has picked the Pod up — only the actor may remove the row.
        if pod_has_node_assignment(&current.data) {
            return Ok(UnscheduledPodDeleteOutcome::DeferToActor);
        }
        // Never hard-delete a live (non-terminating) Pod.
        if current
            .data
            .pointer("/metadata/deletionTimestamp")
            .and_then(|value| value.as_str())
            .filter(|value| !value.is_empty())
            .is_none()
        {
            return Ok(UnscheduledPodDeleteOutcome::DeferToActor);
        }
        // Respect finalizers exactly like the actor finalizer does.
        if current
            .data
            .pointer("/metadata/finalizers")
            .and_then(|value| value.as_array())
            .is_some_and(|finalizers| !finalizers.is_empty())
        {
            return Ok(UnscheduledPodDeleteOutcome::FinalizersPending);
        }

        self.mark_sandbox_dirty();
        match self
            .db
            .delete_resource_with_preconditions(
                POD_API_VERSION,
                POD_KIND,
                Some(ns),
                name,
                ResourcePreconditions {
                    uid: Some(uid.to_string()),
                    // Compare-and-swap on the observed RV: confirms no bind /
                    // kubelet pickup raced between the read above and here.
                    resource_version: Some(current.resource_version),
                },
            )
            .await
        {
            Ok(()) => Ok(UnscheduledPodDeleteOutcome::Removed),
            // CAS lost: the row changed (almost always a scheduler bind setting
            // spec.nodeName). Defer to actor-owned finalization.
            Err(err) if crate::datastore::errors::is_conflict_error(&err) => {
                Ok(UnscheduledPodDeleteOutcome::DeferToActor)
            }
            // Row vanished concurrently — treat as removed (idempotent).
            Err(err) if delete_error_means_gone(&err) => Ok(UnscheduledPodDeleteOutcome::Removed),
            Err(err) => Err(err),
        }
    }

    #[cfg(test)]
    pub(super) fn subscribe_watch(&self) -> broadcast::Receiver<WatchEvent> {
        DatastoreBackend::subscribe_watch(
            self.db.as_ref(),
            crate::watch::WatchTopic::new("v1", "Pod"),
        )
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
    ) -> Result<ResourceList> {
        self.list(ns, label_selector, field_selector, limit, continue_token)
            .await
    }
    async fn list_pods_by_owner_uid(&self, ns: &str, owner_uid: &str) -> Result<Vec<Resource>> {
        self.list_by_owner(ns, owner_uid).await
    }
}

pub(super) fn preserve_status_from_current(current: &Value, next: &mut Value) {
    let Some(next_obj) = next.as_object_mut() else {
        return;
    };
    if let Some(status) = current.get("status") {
        next_obj.insert("status".to_string(), status.clone());
    } else {
        next_obj.remove("status");
    }
}
