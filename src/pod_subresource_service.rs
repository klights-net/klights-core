//! `PodSubresourceService` — API-driven `/status` replace, `/status` patch
//! (all four content types), and `/ephemeralcontainers` writes.
//!
//! Holds `Arc<PodStore>` only. `/status` writes route through
//! `StateOnlyWriter` so non-status fields are never persisted by this
//! subresource.

use std::sync::Arc;

use anyhow::{Result, anyhow};
#[cfg(test)]
use async_trait::async_trait;
use serde_json::{Value, json};

use crate::api::apply_patch;
use crate::datastore::Resource;

use crate::kubelet::pod_repository::state_only_writer::StateOnlyWriter;
use crate::kubelet::pod_repository::store::PodStore;
use crate::kubelet::pod_repository::types::PodStatusPatchType;

pub(crate) struct PodSubresourceService {
    store: Arc<PodStore>,
    status_only: Arc<dyn StateOnlyWriter>,
    mutation_reconcile: Arc<dyn klights_reconcile_api::PodMutationReconcileSink>,
}

impl PodSubresourceService {
    pub(crate) fn new(
        store: Arc<PodStore>,
        status_only: Arc<dyn StateOnlyWriter>,
        mutation_reconcile: Arc<dyn klights_reconcile_api::PodMutationReconcileSink>,
    ) -> Self {
        Self {
            store,
            status_only,
            mutation_reconcile,
        }
    }

    async fn reconcile_status_change(&self, previous: Value, updated: &Resource, operation: &str) {
        if let Err(err) = self
            .mutation_reconcile
            .reconcile_pod_mutation(
                klights_reconcile_api::PodMutationReconcileRequest::StatusChanged {
                    previous: Resource::from_data_lossy(Arc::new(previous)),
                    updated: updated.clone(),
                },
            )
            .await
        {
            tracing::debug!(
                target: "klights::kubelet::pod_repository::subresource",
                error = %err,
                pod = %updated.name,
                operation,
                "failed to reconcile controllers after API status mutation"
            );
        }
    }

    pub(crate) async fn replace_status_from_api_checked(
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
        klights_cluster_core::merge_status_for_apply(
            "v1",
            "Pod",
            current.data.as_ref(),
            &mut status,
            klights_cluster_core::StatusApplyFreshness::Fresh,
            klights_cluster_core::StatusApplyOrigin::ApiSubresource,
        );
        let previous = std::sync::Arc::unwrap_or_clone(current.data);
        let updated = self
            .status_only
            .write_status(ns, name, status, Some(expected_rv))
            .await?;
        self.reconcile_status_change(previous, &updated, "replace")
            .await;
        Ok(updated)
    }

    /// PATCH `/api/v1/.../pods/{name}/status` — apply the patch and
    /// persist only the resulting `status` subtree.
    pub(crate) async fn patch_status_from_api(
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
        klights_cluster_core::merge_status_for_apply(
            "v1",
            "Pod",
            current.data.as_ref(),
            &mut next_status,
            klights_cluster_core::StatusApplyFreshness::Fresh,
            klights_cluster_core::StatusApplyOrigin::ApiSubresource,
        );
        let previous = std::sync::Arc::unwrap_or_clone(current.data);
        let updated = self
            .status_only
            .write_status(ns, name, next_status, Some(expected_rv))
            .await?;
        self.reconcile_status_change(previous, &updated, "patch")
            .await;
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
    pub(crate) async fn update_ephemeral_containers(
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

impl klights_pod_api::PodSubresourceMutation for PodSubresourceService {
    fn replace_status(
        &self,
        request: klights_pod_api::PodStatusReplaceRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Resource> {
        Box::pin(async move {
            self.replace_status_from_api_checked(
                &request.namespace,
                &request.name,
                None,
                request.status,
                request.expected_resource_version,
            )
            .await
            .map_err(|error| map_subresource_error(error, &request.namespace, &request.name))
        })
    }

    fn patch_status(
        &self,
        request: klights_pod_api::PodStatusPatchRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Resource> {
        Box::pin(async move {
            let patch_type = match request.patch_kind {
                klights_pod_api::PodStatusPatchKind::JsonPatch => PodStatusPatchType::JsonPatch,
                klights_pod_api::PodStatusPatchKind::MergePatch => PodStatusPatchType::MergePatch,
                klights_pod_api::PodStatusPatchKind::StrategicMerge => {
                    PodStatusPatchType::StrategicMerge
                }
                klights_pod_api::PodStatusPatchKind::ApplyPatch => PodStatusPatchType::ApplyPatch,
            };
            self.patch_status_from_api(
                &request.namespace,
                &request.name,
                request.patch,
                patch_type,
                request.expected_resource_version,
            )
            .await
            .map_err(|error| map_subresource_error(error, &request.namespace, &request.name))
        })
    }

    fn update_ephemeral_containers(
        &self,
        request: klights_pod_api::PodEphemeralContainersRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Resource> {
        Box::pin(async move {
            self.update_ephemeral_containers(
                &request.namespace,
                &request.name,
                request.containers,
                request.expected_resource_version,
            )
            .await
            .map_err(|error| map_subresource_error(error, &request.namespace, &request.name))
        })
    }
}

#[cfg(test)]
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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use klights_reconcile_api::{
        PodMutationReconcileRequest, PodMutationReconcileSink, ReconcileSinkFuture,
    };
    use serde_json::json;

    use super::PodSubresourceService;
    use crate::kubelet::pod_repository::state_only_writer::StatusOnlyWriterService;
    use crate::kubelet::pod_repository::store::PodStore;
    use crate::kubelet::pod_repository::types::PodStatusPatchType;

    #[derive(Default)]
    struct RecordingMutationReconcile {
        requests: Mutex<Vec<PodMutationReconcileRequest>>,
    }

    impl PodMutationReconcileSink for RecordingMutationReconcile {
        fn reconcile_pod_mutation(
            &self,
            request: PodMutationReconcileRequest,
        ) -> ReconcileSinkFuture<'_> {
            Box::pin(async move {
                self.requests.lock().unwrap().push(request);
                Ok(())
            })
        }
    }

    #[tokio::test]
    async fn api_status_replace_and_patch_emit_focused_status_changed_reconcile() {
        for operation in ["replace", "patch"] {
            let (_datastore, db) = crate::datastore::test_support::in_memory_with_handle().await;
            let store = Arc::new(PodStore::new(db.clone()));
            let status_only = Arc::new(StatusOnlyWriterService::new(store.clone()));
            let reconcile = Arc::new(RecordingMutationReconcile::default());
            let service = PodSubresourceService::new(store.clone(), status_only, reconcile.clone());
            let created = store
                .create(
                    "default",
                    "node-agent",
                    json!({
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "metadata": {
                            "name": "node-agent",
                            "namespace": "default",
                            "uid": "pod-node-agent",
                            "ownerReferences": [{
                                "apiVersion": "apps/v1",
                                "kind": "DaemonSet",
                                "name": "node-agent",
                                "uid": "ds-node-agent",
                                "controller": true
                            }]
                        },
                        "spec": {
                            "nodeName": "node-a",
                            "containers": [{"name": "agent", "image": "busybox"}]
                        },
                        "status": {"phase": "Running"}
                    }),
                )
                .await
                .unwrap();

            match operation {
                "replace" => {
                    service
                        .replace_status_from_api_checked(
                            "default",
                            "node-agent",
                            None,
                            json!({"phase": "Failed"}),
                            created.resource_version,
                        )
                        .await
                        .unwrap();
                }
                "patch" => {
                    service
                        .patch_status_from_api(
                            "default",
                            "node-agent",
                            json!({"status": {"phase": "Failed"}}),
                            PodStatusPatchType::MergePatch,
                            created.resource_version,
                        )
                        .await
                        .unwrap();
                }
                _ => unreachable!(),
            }

            let requests = reconcile.requests.lock().unwrap();
            assert_eq!(
                requests.len(),
                1,
                "{operation} must route the status transition through the focused mutation reconcile sink"
            );
            let PodMutationReconcileRequest::StatusChanged { previous, updated } = &requests[0]
            else {
                panic!("{operation} emitted the wrong reconcile intent");
            };
            assert_eq!(previous.data["status"]["phase"], "Running");
            assert_eq!(updated.data["status"]["phase"], "Failed");
            assert_eq!(
                updated.data["metadata"]["ownerReferences"][0]["kind"],
                "DaemonSet"
            );
        }
    }
}
