//! `PodSubresourceService` — API-driven `/status` replace, `/status` patch
//! (all four content types), and `/ephemeralcontainers` writes.
//!
//! Holds `Arc<PodStore>` only. `/status` writes route through
//! `StateOnlyWriter` so non-status fields are never persisted by this
//! subresource.

use std::sync::Arc;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use serde_json::{Value, json};

use crate::api::apply_patch;
use crate::datastore::Resource;
use crate::side_effects::ControllerDispatcherSlot;

use crate::kubelet::pod_repository::state_only_writer::StateOnlyWriter;
use crate::kubelet::pod_repository::store::PodStore;
use crate::kubelet::pod_repository::types::PodStatusPatchType;

pub(crate) struct PodSubresourceService {
    store: Arc<PodStore>,
    status_only: Arc<dyn StateOnlyWriter>,
    db: crate::datastore::DatastoreHandle,
    controller_dispatcher: ControllerDispatcherSlot,
}

impl PodSubresourceService {
    pub(crate) fn new(
        store: Arc<PodStore>,
        status_only: Arc<dyn StateOnlyWriter>,
        db: crate::datastore::DatastoreHandle,
        controller_dispatcher: ControllerDispatcherSlot,
    ) -> Self {
        Self {
            store,
            status_only,
            db,
            controller_dispatcher,
        }
    }

    async fn replace_status_from_api_checked(
        &self,
        ns: &str,
        name: &str,
        expected_uid: Option<&str>,
        status: Value,
        expected_rv: i64,
    ) -> Result<Resource> {
        let current = self
            .store
            .get(ns, name)
            .await?
            .ok_or_else(|| anyhow!("Pod not found"))?;
        if let Some(uid) = expected_uid {
            crate::kubelet::pod_repository::ensure_pod_uid_matches(&current.data, uid, ns, name)?;
        }
        if current.resource_version != expected_rv {
            return Err(anyhow!(
                "Resource not found or version conflict (409 Conflict)"
            ));
        }
        let mut status = status;
        crate::datastore::status_merge_policy::merge_status_for_apply(
            "v1",
            "Pod",
            current.data.as_ref(),
            &mut status,
            crate::datastore::status_merge_policy::StatusApplyFreshness::Fresh,
            crate::datastore::status_merge_policy::StatusApplyOrigin::ApiSubresource,
        );
        let previous = std::sync::Arc::unwrap_or_clone(current.data);
        let updated = self
            .status_only
            .write_status(ns, name, status, Some(expected_rv))
            .await?;
        if let Err(err) = crate::side_effects::service_pod::enqueue_services_after_pod_update(
            &previous,
            &updated.data,
            self.db.as_ref(),
            &self.controller_dispatcher,
        )
        .await
        {
            tracing::debug!(
                target: "klights::kubelet::pod_repository::subresource",
                error = %err,
                pod = %name,
                "failed to enqueue Service reconcile after API status replace"
            );
        }
        Ok(updated)
    }

    /// PATCH `/api/v1/.../pods/{name}/status` — apply the patch and
    /// persist only the resulting `status` subtree.
    pub(super) async fn patch_status_from_api(
        &self,
        ns: &str,
        name: &str,
        patch: Value,
        patch_type: PodStatusPatchType,
        expected_rv: i64,
    ) -> Result<Resource> {
        let current = self
            .store
            .get(ns, name)
            .await?
            .ok_or_else(|| anyhow!("Pod not found"))?;
        if current.resource_version != expected_rv {
            return Err(anyhow!(
                "Resource not found or version conflict (409 Conflict)"
            ));
        }
        let patched = apply_patch(
            &current.data,
            &patch,
            Some(patch_type_to_content_type(patch_type)),
        )
        .map_err(|e| anyhow!("apply_patch failed: {e:?}"))?;
        let mut next_status = patched.get("status").cloned().unwrap_or(Value::Null);
        crate::datastore::status_merge_policy::merge_status_for_apply(
            "v1",
            "Pod",
            current.data.as_ref(),
            &mut next_status,
            crate::datastore::status_merge_policy::StatusApplyFreshness::Fresh,
            crate::datastore::status_merge_policy::StatusApplyOrigin::ApiSubresource,
        );
        let previous = std::sync::Arc::unwrap_or_clone(current.data);
        let updated = self
            .status_only
            .write_status(ns, name, next_status, Some(expected_rv))
            .await?;
        if let Err(err) = crate::side_effects::service_pod::enqueue_services_after_pod_update(
            &previous,
            &updated.data,
            self.db.as_ref(),
            &self.controller_dispatcher,
        )
        .await
        {
            tracing::debug!(
                target: "klights::kubelet::pod_repository::subresource",
                error = %err,
                pod = %name,
                "failed to enqueue Service reconcile after API status patch"
            );
        }
        Ok(updated)
    }

    /// PATCH `/api/v1/.../pods/{name}/ephemeralcontainers` — replace the
    /// `spec.ephemeralContainers` array with the caller's list. Validation
    /// (immutability of existing entries) stays in the API handler; the
    /// repository only persists.
    ///
    /// When the new list grows beyond the existing one, `metadata.generation`
    /// is bumped — matches today's handler-side behaviour and the K8s
    /// "spec mutation increments generation" contract.
    pub(super) async fn update_ephemeral_containers(
        &self,
        ns: &str,
        name: &str,
        containers: Vec<Value>,
        expected_rv: i64,
    ) -> Result<Resource> {
        let current = self
            .store
            .get(ns, name)
            .await?
            .ok_or_else(|| anyhow!("Pod not found"))?;
        if current.resource_version != expected_rv {
            return Err(anyhow!(
                "Resource not found or version conflict (409 Conflict)"
            ));
        }
        let existing_count = current
            .data
            .pointer("/spec/ephemeralContainers")
            .and_then(|v| v.as_array())
            .map(|a| a.len())
            .unwrap_or(0);
        let new_count = containers.len();
        let mut body: Value = std::sync::Arc::unwrap_or_clone(current.data);
        let spec = body
            .as_object_mut()
            .ok_or_else(|| anyhow!("Pod body is not a JSON object"))?
            .entry("spec".to_string())
            .or_insert_with(|| json!({}));
        let spec_obj = spec
            .as_object_mut()
            .ok_or_else(|| anyhow!("Pod spec is not a JSON object"))?;
        spec_obj.insert("ephemeralContainers".to_string(), json!(containers));
        if new_count > existing_count {
            bump_metadata_generation(&mut body);
        }
        self.store.update(ns, name, body, expected_rv).await
    }
}

#[async_trait]
impl crate::kubelet::pod_repository::PodSubresourcePort for PodSubresourceService {
    async fn replace_status(
        &self,
        ns: &str,
        name: &str,
        pod_uid: Option<&str>,
        status: Value,
        expected_rv: i64,
    ) -> std::result::Result<Resource, klights_pod_api::PodRepositoryError> {
        self.replace_status_from_api_checked(ns, name, pod_uid, status, expected_rv)
            .await
            .map_err(|error| map_subresource_error(error, ns, name))
    }

    async fn patch_status(
        &self,
        ns: &str,
        name: &str,
        patch: Value,
        patch_type: PodStatusPatchType,
        expected_rv: i64,
    ) -> std::result::Result<Resource, klights_pod_api::PodRepositoryError> {
        self.patch_status_from_api(ns, name, patch, patch_type, expected_rv)
            .await
            .map_err(|error| map_subresource_error(error, ns, name))
    }

    async fn update_ephemeral_containers(
        &self,
        ns: &str,
        name: &str,
        containers: Vec<Value>,
        expected_rv: i64,
    ) -> std::result::Result<Resource, klights_pod_api::PodRepositoryError> {
        self.update_ephemeral_containers(ns, name, containers, expected_rv)
            .await
            .map_err(|error| map_subresource_error(error, ns, name))
    }
}

fn map_subresource_error(
    error: anyhow::Error,
    namespace: &str,
    name: &str,
) -> klights_pod_api::PodRepositoryError {
    if let Some(mismatch) = error.downcast_ref::<crate::kubelet::pod_repository::PodUidMismatch>() {
        return klights_pod_api::PodRepositoryError::uid_mismatch(
            &mismatch.expected,
            &mismatch.actual,
        );
    }
    if crate::datastore::errors::is_conflict_error(&error) {
        return klights_pod_api::PodRepositoryError::conflict(error.to_string());
    }
    if error.to_string().contains("Pod not found") {
        return klights_pod_api::PodRepositoryError::not_found(namespace, name);
    }
    klights_pod_api::PodRepositoryError::unavailable(error.to_string())
}

fn patch_type_to_content_type(p: PodStatusPatchType) -> &'static str {
    match p {
        PodStatusPatchType::JsonPatch => "application/json-patch+json",
        PodStatusPatchType::MergePatch => "application/merge-patch+json",
        PodStatusPatchType::StrategicMerge => "application/strategic-merge-patch+json",
        PodStatusPatchType::ApplyPatch => "application/apply-patch+yaml",
    }
}

/// Increment `metadata.generation` (or set it to 2 if missing) so spec
/// mutations through the ephemeral-containers subresource bump generation
/// the same way K8s does for spec PATCH/PUT writes.
fn bump_metadata_generation(obj: &mut Value) {
    if let Some(meta_obj) = obj.get_mut("metadata").and_then(|m| m.as_object_mut()) {
        let current_generation = meta_obj
            .get("generation")
            .and_then(|v| v.as_i64())
            .unwrap_or(1);
        meta_obj.insert("generation".to_string(), json!(current_generation + 1));
    }
}
