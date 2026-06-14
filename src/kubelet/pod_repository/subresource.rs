//! `PodSubresourceService` — API-driven `/status` replace, `/status` patch
//! (all four content types), and `/ephemeralcontainers` writes.
//!
//! Holds `Arc<PodStore>` only. `/status` writes route through
//! `StateOnlyWriter` so non-status fields are never persisted by this
//! subresource.

use std::sync::Arc;

use anyhow::{Result, anyhow};
use serde_json::{Value, json};

use crate::api::apply_patch;
use crate::datastore::Resource;
use crate::side_effects::ControllerDispatcherSlot;

use super::state_only_writer::StateOnlyWriter;
use super::store::PodStore;
use super::types::PodStatusPatchType;

pub(super) struct PodSubresourceService {
    store: Arc<PodStore>,
    status_only: Arc<dyn StateOnlyWriter>,
    controller_dispatcher: ControllerDispatcherSlot,
}

impl PodSubresourceService {
    pub(super) fn new(
        store: Arc<PodStore>,
        status_only: Arc<dyn StateOnlyWriter>,
        controller_dispatcher: ControllerDispatcherSlot,
    ) -> Self {
        Self {
            store,
            status_only,
            controller_dispatcher,
        }
    }

    /// PUT `/api/v1/.../pods/{name}/status` — replace the persisted
    /// status subtree while preserving all non-status fields.
    pub(super) async fn replace_status_from_api(
        &self,
        ns: &str,
        name: &str,
        status: Value,
        expected_rv: i64,
    ) -> Result<Resource> {
        self.replace_status_from_api_checked(ns, name, None, status, expected_rv)
            .await
    }

    pub(super) async fn replace_status_from_api_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        status: Value,
        expected_rv: i64,
    ) -> Result<Resource> {
        self.replace_status_from_api_checked(ns, name, Some(pod_uid), status, expected_rv)
            .await
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
            super::ensure_pod_uid_matches(&current.data, uid, ns, name)?;
        }
        if current.resource_version != expected_rv {
            return Err(anyhow!(
                "Resource not found or version conflict (409 Conflict)"
            ));
        }
        let previous = std::sync::Arc::unwrap_or_clone(current.data);
        let updated = self
            .status_only
            .write_status(ns, name, status, Some(expected_rv))
            .await?;
        if let Err(err) = crate::side_effects::service_pod::enqueue_services_after_pod_update(
            &previous,
            &updated.data,
            self.store.db().as_ref(),
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
        let next_status = patched.get("status").cloned().unwrap_or(Value::Null);
        let previous = std::sync::Arc::unwrap_or_clone(current.data);
        let updated = self
            .status_only
            .write_status(ns, name, next_status, Some(expected_rv))
            .await?;
        if let Err(err) = crate::side_effects::service_pod::enqueue_services_after_pod_update(
            &previous,
            &updated.data,
            self.store.db().as_ref(),
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
