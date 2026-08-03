//! `PodSubresourceService` — API-driven `/status` replace, `/status` patch
//! (all four content types), and `/ephemeralcontainers` writes.
//!
//! Query, full-object persistence, and status-only persistence arrive through
//! focused neutral Pod ports, so this API policy owner cannot reach a kubelet
//! repository or concrete datastore.

use std::sync::Arc;

use anyhow::{Result, anyhow};
use serde_json::{Value, json};

use super::helpers::apply_patch;
use klights_cluster_core::Resource;
use klights_pod_api::{
    PodGetRequest, PodPersistence, PodPersistenceReplaceRequest, PodQuery, PodRepositoryError,
    PodStatusPatchKind, PodStatusPersistence, PodStatusWriteRequest,
};

pub struct PodSubresourceService {
    pod_query: Arc<dyn PodQuery>,
    persistence: Arc<dyn PodPersistence>,
    status_persistence: Arc<dyn PodStatusPersistence>,
    mutation_reconcile: Arc<dyn klights_reconcile_api::PodMutationReconcileSink>,
}

impl PodSubresourceService {
    pub fn new(
        pod_query: Arc<dyn PodQuery>,
        persistence: Arc<dyn PodPersistence>,
        status_persistence: Arc<dyn PodStatusPersistence>,
        mutation_reconcile: Arc<dyn klights_reconcile_api::PodMutationReconcileSink>,
    ) -> Self {
        Self {
            pod_query,
            persistence,
            status_persistence,
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
            .pod_query
            .get_pod(PodGetRequest::try_by_name(
                ns.to_string(),
                name.to_string(),
            )?)
            .await?
            .ok_or_else(|| anyhow!("Pod not found"))?;
        if let Some(uid) = expected_uid
            && current.uid != uid
        {
            return Err(anyhow!(PodRepositoryError::uid_mismatch(uid, &current.uid)));
        }
        if current.resource_version != expected_rv {
            return Err(anyhow!(PodRepositoryError::conflict(
                "the Pod resourceVersion does not match the current object",
            )));
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
            .status_persistence
            .write_pod_status(PodStatusWriteRequest {
                namespace: ns.to_string(),
                name: name.to_string(),
                status,
                expected_resource_version: Some(expected_rv),
            })
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
        patch_type: PodStatusPatchKind,
        expected_rv: Option<i64>,
    ) -> Result<Resource> {
        for attempt in 0..super::POD_MUTATION_CONFLICT_MAX_ATTEMPTS {
            let current = self
                .pod_query
                .get_pod(PodGetRequest::try_by_name(
                    ns.to_string(),
                    name.to_string(),
                )?)
                .await?
                .ok_or_else(|| anyhow!("Pod not found"))?;
            if expected_rv.is_some_and(|expected| current.resource_version != expected) {
                return Err(anyhow!(PodRepositoryError::conflict(
                    "the Pod resourceVersion does not match the current object",
                )));
            }
            let observed_rv = current.resource_version;
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
            match self
                .status_persistence
                .write_pod_status(PodStatusWriteRequest {
                    namespace: ns.to_string(),
                    name: name.to_string(),
                    status: next_status,
                    expected_resource_version: Some(observed_rv),
                })
                .await
            {
                Ok(updated) => {
                    self.reconcile_status_change(previous, &updated, "patch")
                        .await;
                    return Ok(updated);
                }
                Err(PodRepositoryError::Conflict { .. })
                    if expected_rv.is_none()
                        && attempt + 1 < super::POD_MUTATION_CONFLICT_MAX_ATTEMPTS =>
                {
                    continue;
                }
                Err(error) => return Err(anyhow!(error)),
            }
        }
        Err(anyhow!(PodRepositoryError::conflict(
            "the Pod status patch conflicted too many times",
        )))
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
            .pod_query
            .get_pod(PodGetRequest::try_by_name(
                ns.to_string(),
                name.to_string(),
            )?)
            .await?
            .ok_or_else(|| anyhow!("Pod not found"))?;
        if current.resource_version != expected_rv {
            return Err(anyhow!(PodRepositoryError::conflict(
                "the Pod resourceVersion does not match the current object",
            )));
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
        self.persistence
            .replace_pod(PodPersistenceReplaceRequest {
                namespace: ns.to_string(),
                name: name.to_string(),
                body,
                expected_resource_version: expected_rv,
            })
            .await
            .map_err(Into::into)
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
                request.expected_uid.as_deref(),
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
                klights_pod_api::PodStatusPatchKind::JsonPatch => PodStatusPatchKind::JsonPatch,
                klights_pod_api::PodStatusPatchKind::MergePatch => PodStatusPatchKind::MergePatch,
                klights_pod_api::PodStatusPatchKind::StrategicMerge => {
                    PodStatusPatchKind::StrategicMerge
                }
                klights_pod_api::PodStatusPatchKind::ApplyPatch => PodStatusPatchKind::ApplyPatch,
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

fn map_subresource_error(
    error: anyhow::Error,
    namespace: &str,
    name: &str,
) -> klights_pod_api::PodRepositoryError {
    if let Some(error) = error.downcast_ref::<PodRepositoryError>() {
        return error.clone();
    }
    if error.to_string().contains("Pod not found") {
        return klights_pod_api::PodRepositoryError::not_found(namespace, name);
    }
    klights_pod_api::PodRepositoryError::unavailable(error.to_string())
}

fn patch_type_to_content_type(p: PodStatusPatchKind) -> &'static str {
    match p {
        PodStatusPatchKind::JsonPatch => "application/json-patch+json",
        PodStatusPatchKind::MergePatch => "application/merge-patch+json",
        PodStatusPatchKind::StrategicMerge => "application/strategic-merge-patch+json",
        PodStatusPatchKind::ApplyPatch => "application/apply-patch+yaml",
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
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use klights_reconcile_api::{
        PodMutationReconcileRequest, PodMutationReconcileSink, ReconcileSinkFuture,
    };
    use serde_json::json;

    use super::PodSubresourceService;
    use klights_pod_api::{
        PodGetRequest, PodListRequest, PodListResult, PodOwnerListRequest, PodPersistence,
        PodPersistenceCreateRequest, PodPersistenceReplaceRequest, PodQuery, PodRepositoryError,
        PodRepositoryFuture, PodStatusPatchKind, PodStatusPersistence, PodStatusWriteRequest,
        PodSubresourceMutation,
    };

    struct TestPodStore {
        current: Mutex<klights_cluster_core::Resource>,
        advance_status_before_next_write: AtomicBool,
        status_write_attempts: AtomicUsize,
    }

    impl TestPodStore {
        fn new(body: serde_json::Value) -> Self {
            Self {
                current: Mutex::new(
                    klights_cluster_core::Resource::try_from_data(Arc::new(body)).unwrap(),
                ),
                advance_status_before_next_write: AtomicBool::new(false),
                status_write_attempts: AtomicUsize::new(0),
            }
        }

        fn advance_status_before_next_write(&self) {
            self.advance_status_before_next_write
                .store(true, Ordering::SeqCst);
        }

        fn current(&self) -> klights_cluster_core::Resource {
            self.current.lock().unwrap().clone()
        }

        fn replace(
            &self,
            mut body: serde_json::Value,
            expected_resource_version: i64,
        ) -> Result<klights_cluster_core::Resource, PodRepositoryError> {
            let mut current = self.current.lock().unwrap();
            if current.resource_version != expected_resource_version {
                return Err(PodRepositoryError::conflict(
                    "the Pod resourceVersion does not match the current object",
                ));
            }
            body.pointer_mut("/metadata/resourceVersion")
                .expect("test Pod has metadata.resourceVersion")
                .clone_from(&json!((expected_resource_version + 1).to_string()));
            let updated = klights_cluster_core::Resource::try_from_data(Arc::new(body)).unwrap();
            *current = updated.clone();
            Ok(updated)
        }
    }

    impl PodQuery for TestPodStore {
        fn get_pod(
            &self,
            request: PodGetRequest,
        ) -> PodRepositoryFuture<'_, Option<klights_cluster_core::Resource>> {
            Box::pin(async move {
                let current = self.current();
                let matches = current.namespace.as_deref() == Some(request.namespace())
                    && current.name == request.name()
                    && request.uid().is_none_or(|uid| uid == current.uid);
                Ok(matches.then_some(current))
            })
        }

        fn list_pods(&self, _request: PodListRequest) -> PodRepositoryFuture<'_, PodListResult> {
            Box::pin(async move {
                let current = self.current();
                PodListResult::try_new(vec![current.clone()], current.resource_version, None, None)
            })
        }

        fn list_pods_by_owner_uid(
            &self,
            _request: PodOwnerListRequest,
        ) -> PodRepositoryFuture<'_, Vec<klights_cluster_core::Resource>> {
            Box::pin(async move { Ok(vec![self.current()]) })
        }
    }

    impl PodPersistence for TestPodStore {
        fn create_pod(
            &self,
            request: PodPersistenceCreateRequest,
        ) -> PodRepositoryFuture<'_, klights_cluster_core::Resource> {
            Box::pin(async move {
                let expected = self.current().resource_version;
                self.replace(request.body, expected)
            })
        }

        fn replace_pod(
            &self,
            request: PodPersistenceReplaceRequest,
        ) -> PodRepositoryFuture<'_, klights_cluster_core::Resource> {
            Box::pin(async move { self.replace(request.body, request.expected_resource_version) })
        }

        fn replace_pod_including_status(
            &self,
            request: PodPersistenceReplaceRequest,
        ) -> PodRepositoryFuture<'_, klights_cluster_core::Resource> {
            Box::pin(async move { self.replace(request.body, request.expected_resource_version) })
        }
    }

    impl PodStatusPersistence for TestPodStore {
        fn write_pod_status(
            &self,
            request: PodStatusWriteRequest,
        ) -> PodRepositoryFuture<'_, klights_cluster_core::Resource> {
            Box::pin(async move {
                self.status_write_attempts.fetch_add(1, Ordering::SeqCst);
                if self
                    .advance_status_before_next_write
                    .swap(false, Ordering::SeqCst)
                {
                    let current = self.current();
                    let mut raced = Arc::unwrap_or_clone(current.data);
                    raced["status"]["internalHeartbeat"] = json!("new");
                    self.replace(raced, current.resource_version)
                        .expect("coordinated internal status writer advances the Pod RV");
                }
                let current = self.current();
                let mut body = Arc::unwrap_or_clone(current.data);
                body.as_object_mut()
                    .expect("test Pod is an object")
                    .insert("status".to_string(), request.status);
                self.replace(
                    body,
                    request
                        .expected_resource_version
                        .unwrap_or(current.resource_version),
                )
            })
        }
    }

    /// Kubernetes PATCH without `metadata.resourceVersion` is an unconditional
    /// read-modify-write. An internal status writer may advance the Pod between
    /// this request's read and CAS; the API patch must re-read, re-apply, and
    /// succeed instead of exposing that internal race as a client conflict.
    #[tokio::test]
    async fn status_patch_without_client_rv_retries_coordinated_internal_status_race() {
        let store = Arc::new(TestPodStore::new(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "status-race",
                "namespace": "default",
                "uid": "pod-status-race",
                "resourceVersion": "7"
            },
            "spec": {"containers": [{"name": "app", "image": "busybox"}]},
            "status": {"phase": "Running"}
        })));
        let service = PodSubresourceService::new(
            store.clone(),
            store.clone(),
            store.clone(),
            Arc::new(RecordingMutationReconcile::default()),
        );
        store.advance_status_before_next_write();

        let updated = service
            .patch_status_from_api(
                "default",
                "status-race",
                json!({"status": {"phase": "Failed"}}),
                PodStatusPatchKind::MergePatch,
                None,
            )
            .await
            .expect("an internal status RV advance must be retried");

        assert_eq!(updated.data["status"]["phase"], "Failed");
        assert_eq!(updated.data["status"]["internalHeartbeat"], "new");
        assert_eq!(
            store.status_write_attempts.load(Ordering::SeqCst),
            2,
            "the coordinated race must cause exactly one retry"
        );
    }

    #[tokio::test]
    async fn status_patch_with_explicit_client_rv_does_not_retry_coordinated_conflict() {
        let store = Arc::new(TestPodStore::new(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "strict-status-race",
                "namespace": "default",
                "uid": "pod-strict-status-race",
                "resourceVersion": "7"
            },
            "spec": {"containers": [{"name": "app", "image": "busybox"}]},
            "status": {"phase": "Running"}
        })));
        let service = PodSubresourceService::new(
            store.clone(),
            store.clone(),
            store.clone(),
            Arc::new(RecordingMutationReconcile::default()),
        );
        store.advance_status_before_next_write();

        let error = PodSubresourceMutation::patch_status(
            &service,
            klights_pod_api::PodStatusPatchRequest {
                namespace: "default".to_string(),
                name: "strict-status-race".to_string(),
                patch: json!({"status": {"phase": "Failed"}}),
                patch_kind: PodStatusPatchKind::MergePatch,
                expected_resource_version: Some(7),
            },
        )
        .await
        .expect_err("an explicit stale client resourceVersion must remain a conflict");

        assert!(matches!(error, PodRepositoryError::Conflict { .. }));
        assert_eq!(
            store.status_write_attempts.load(Ordering::SeqCst),
            1,
            "an explicit client precondition must never be silently refreshed"
        );
        assert_eq!(store.current().data["status"]["phase"], "Running");
    }

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
            let store = Arc::new(TestPodStore::new(json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "node-agent",
                    "namespace": "default",
                    "uid": "pod-node-agent",
                    "resourceVersion": "7",
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
            })));
            let reconcile = Arc::new(RecordingMutationReconcile::default());
            let service = PodSubresourceService::new(
                store.clone(),
                store.clone(),
                store.clone(),
                reconcile.clone(),
            );
            let created = store.current();

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
                            PodStatusPatchKind::MergePatch,
                            Some(created.resource_version),
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

    #[tokio::test]
    async fn stale_ephemeral_container_write_preserves_typed_conflict() {
        let store = Arc::new(TestPodStore::new(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "debuggable",
                "namespace": "default",
                "uid": "pod-debuggable",
                "resourceVersion": "9"
            },
            "spec": {
                "containers": [{"name": "main", "image": "busybox"}]
            }
        })));
        let service = PodSubresourceService::new(
            store.clone(),
            store.clone(),
            store.clone(),
            Arc::new(RecordingMutationReconcile::default()),
        );
        let created = store.current();

        let error = PodSubresourceMutation::update_ephemeral_containers(
            &service,
            klights_pod_api::PodEphemeralContainersRequest {
                namespace: "default".to_string(),
                name: "debuggable".to_string(),
                containers: vec![json!({"name": "debug", "image": "busybox"})],
                expected_resource_version: created.resource_version - 1,
            },
        )
        .await
        .expect_err("stale ephemeral-container update must conflict");

        assert!(
            matches!(error, PodRepositoryError::Conflict { .. }),
            "neutral subresource boundary changed stale write into {error:?}"
        );
    }
}
