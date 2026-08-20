use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use klights_cluster_core::Resource;
use klights_pod_api::{
    PodGetRequest, PodListRequest, PodListResult, PodOwnerListRequest, PodQuery,
    PodRepositoryFuture, PodStatusPersistence, PodStatusWriteRequest,
};

use crate::context::HostIpState;
use crate::pod_repository::status::{
    PodStatusService, PodStatusServiceDependencies, PodStatusWriter,
};
use crate::pod_repository::{PodStatusUpdate, PublishedAddress, RuntimeReconcileStatus};
use crate::runtime::{PodDeletionFinalizeResult, PodRuntimeKey};

#[derive(Default)]
pub(super) struct InMemoryPodRepository {
    pods: Mutex<HashMap<(String, String), Resource>>,
    status_writes: std::sync::atomic::AtomicUsize,
}

impl InMemoryPodRepository {
    pub(super) fn insert(&self, mut body: serde_json::Value) -> anyhow::Result<Resource> {
        let namespace = body
            .pointer("/metadata/namespace")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("default")
            .to_string();
        let name = body
            .pointer("/metadata/name")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("test Pod metadata.name is required"))?
            .to_string();
        body["metadata"]["namespace"] = serde_json::json!(namespace);
        body["metadata"]["resourceVersion"] = serde_json::json!("1");
        let resource = Resource::try_from_data(Arc::new(body))?;
        self.pods
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert((namespace, name), resource.clone());
        Ok(resource)
    }

    fn get(&self, namespace: &str, name: &str, uid: Option<&str>) -> Option<Resource> {
        self.pods
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&(namespace.to_string(), name.to_string()))
            .filter(|resource| uid.is_none_or(|uid| resource.uid == uid))
            .cloned()
    }

    fn remove_uid(&self, key: &PodRuntimeKey) -> bool {
        let mut pods = self
            .pods
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let slot = (key.namespace.clone(), key.name.clone());
        if pods
            .get(&slot)
            .is_some_and(|resource| resource.uid == key.uid)
        {
            pods.remove(&slot);
            true
        } else {
            false
        }
    }

    pub(super) fn status_write_count(&self) -> usize {
        self.status_writes.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl PodQuery for InMemoryPodRepository {
    fn get_pod(&self, request: PodGetRequest) -> PodRepositoryFuture<'_, Option<Resource>> {
        Box::pin(async move { Ok(self.get(request.namespace(), request.name(), request.uid())) })
    }

    fn list_pods(&self, request: PodListRequest) -> PodRepositoryFuture<'_, PodListResult> {
        Box::pin(async move {
            let pods = self
                .pods
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .values()
                .filter(|resource| {
                    request
                        .namespace()
                        .is_none_or(|namespace| resource.namespace.as_deref() == Some(namespace))
                })
                .cloned()
                .collect();
            PodListResult::try_new(pods, 1, None, None)
        })
    }

    fn list_pods_by_owner_uid(
        &self,
        request: PodOwnerListRequest,
    ) -> PodRepositoryFuture<'_, Vec<Resource>> {
        Box::pin(async move {
            Ok(self
                .pods
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .values()
                .filter(|resource| resource.namespace.as_deref() == Some(request.namespace()))
                .filter(|resource| {
                    resource
                        .data
                        .pointer("/metadata/ownerReferences")
                        .and_then(serde_json::Value::as_array)
                        .is_some_and(|owners| {
                            owners.iter().any(|owner| {
                                owner.get("uid").and_then(serde_json::Value::as_str)
                                    == Some(request.owner_uid())
                            })
                        })
                })
                .cloned()
                .collect())
        })
    }
}

impl PodStatusPersistence for InMemoryPodRepository {
    fn write_pod_status(
        &self,
        request: PodStatusWriteRequest,
    ) -> PodRepositoryFuture<'_, Resource> {
        Box::pin(async move {
            let mut pods = self
                .pods
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let key = (request.namespace.clone(), request.name.clone());
            let current = pods
                .get(&key)
                .cloned()
                .ok_or_else(|| klights_pod_api::PodRepositoryError::unavailable("Pod not found"))?;
            if request
                .expected_resource_version
                .is_some_and(|expected| expected != current.resource_version)
            {
                return Err(klights_pod_api::PodRepositoryError::conflict(
                    "test Pod resourceVersion conflict",
                ));
            }
            let mut body = current.data.as_ref().clone();
            body["status"] = request.status;
            let next_rv = current.resource_version.saturating_add(1);
            body["metadata"]["resourceVersion"] = serde_json::json!(next_rv.to_string());
            let updated = Resource::try_from_data(Arc::new(body)).map_err(|error| {
                klights_pod_api::PodRepositoryError::unavailable(error.to_string())
            })?;
            pods.insert(key, updated.clone());
            self.status_writes
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(updated)
        })
    }
}

struct NoopMutationReconcile;

impl klights_reconcile_api::PodMutationReconcileSink for NoopMutationReconcile {
    fn reconcile_pod_mutation(
        &self,
        _request: klights_reconcile_api::PodMutationReconcileRequest,
    ) -> klights_reconcile_api::ReconcileSinkFuture<'_> {
        Box::pin(async { Ok(()) })
    }
}

pub(super) struct TestPodStatusWriter {
    status: PodStatusService,
}

impl TestPodStatusWriter {
    pub(super) fn new(repository: Arc<InMemoryPodRepository>) -> Self {
        Self {
            status: PodStatusService::new(PodStatusServiceDependencies {
                pod_query: repository.clone(),
                status_persistence: repository,
                mutation_reconcile: Arc::new(NoopMutationReconcile),
                outbox: None,
                remote_delivery_required: false,
                cluster_api: None,
                host_ip: HostIpState::default(),
                wall_clock: Arc::new(crate::runtime_clock::SystemRuntimeClock),
            }),
        }
    }
}

#[async_trait::async_trait]
impl PodStatusWriter for TestPodStatusWriter {
    async fn set_pod_status(
        &self,
        ns: &str,
        name: &str,
        update: PodStatusUpdate,
        rv: Option<i64>,
    ) -> anyhow::Result<Resource> {
        Ok(self
            .status
            .set_pod_status(ns, name, &update, rv)
            .await?
            .resource)
    }
    async fn set_pod_status_for_uid(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
        update: PodStatusUpdate,
        rv: Option<i64>,
    ) -> anyhow::Result<Resource> {
        Ok(self
            .status
            .set_pod_status_for_uid(ns, name, uid, update, rv)
            .await?
            .resource)
    }
    async fn apply_runtime_reconcile_status(
        &self,
        ns: &str,
        name: &str,
        update: RuntimeReconcileStatus,
        rv: Option<i64>,
    ) -> anyhow::Result<Resource> {
        Ok(self
            .status
            .apply_runtime_reconcile_status(ns, name, update, rv)
            .await?
            .resource)
    }
    async fn apply_runtime_reconcile_status_for_uid(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
        update: RuntimeReconcileStatus,
        rv: Option<i64>,
    ) -> anyhow::Result<Resource> {
        Ok(self
            .status
            .apply_runtime_reconcile_status_for_uid(ns, name, uid, update, rv)
            .await?
            .resource)
    }
    async fn mark_start_pending_for_retry_for_uid(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
        message: &str,
    ) -> anyhow::Result<Resource> {
        Ok(self
            .status
            .mark_start_pending_for_retry_for_uid(ns, name, uid, message)
            .await?
            .resource)
    }
    async fn set_probe_readiness(
        &self,
        ns: &str,
        name: &str,
        container: &str,
        ready: bool,
        rv: Option<i64>,
    ) -> anyhow::Result<crate::pod_repository::status::PodStatusWriteResult> {
        self.status
            .set_probe_readiness(ns, name, container, ready, rv)
            .await
    }
    async fn set_probe_readiness_for_uid(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
        container: &str,
        ready: bool,
        rv: Option<i64>,
    ) -> anyhow::Result<crate::pod_repository::status::PodStatusWriteResult> {
        self.status
            .set_probe_readiness_for_uid(ns, name, uid, container, ready, rv)
            .await
    }
    async fn set_deadline_exceeded(
        &self,
        ns: &str,
        name: &str,
        message: String,
        rv: Option<i64>,
    ) -> anyhow::Result<Resource> {
        Ok(self
            .status
            .set_deadline_exceeded(ns, name, message, rv)
            .await?
            .resource)
    }
    async fn set_deadline_exceeded_for_uid(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
        message: String,
        rv: Option<i64>,
    ) -> anyhow::Result<Resource> {
        Ok(self
            .status
            .set_deadline_exceeded_for_uid(ns, name, uid, message, rv)
            .await?
            .resource)
    }
    async fn apply_ephemeral_container_statuses(
        &self,
        ns: &str,
        name: &str,
        statuses: Vec<serde_json::Value>,
        rv: Option<i64>,
    ) -> anyhow::Result<Resource> {
        Ok(self
            .status
            .apply_ephemeral_container_statuses(ns, name, statuses, rv)
            .await?
            .resource)
    }
    async fn apply_ephemeral_container_statuses_for_uid(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
        statuses: Vec<serde_json::Value>,
        rv: Option<i64>,
    ) -> anyhow::Result<Resource> {
        Ok(self
            .status
            .apply_ephemeral_container_statuses_for_uid(ns, name, uid, statuses, rv)
            .await?
            .resource)
    }
    async fn note_container_restart(
        &self,
        ns: &str,
        name: &str,
        container: &str,
        terminated: serde_json::Value,
        rv: Option<i64>,
    ) -> anyhow::Result<Option<Resource>> {
        self.status
            .note_container_restart(ns, name, container, terminated, rv)
            .await
    }
    async fn note_container_restart_for_uid(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
        container: &str,
        terminated: serde_json::Value,
        rv: Option<i64>,
    ) -> anyhow::Result<Option<Resource>> {
        self.status
            .note_container_restart_for_uid(ns, name, uid, container, terminated, rv)
            .await
    }

    async fn read_pod_with_own_writes(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
    ) -> anyhow::Result<Option<Resource>> {
        self.status.read_pod_with_own_writes(ns, name, uid).await
    }
}

pub(super) struct TestDeletionFinalizer {
    repository: Arc<InMemoryPodRepository>,
}

impl TestDeletionFinalizer {
    pub(super) fn new(repository: Arc<InMemoryPodRepository>) -> Self {
        Self { repository }
    }
}

#[async_trait::async_trait]
impl crate::pod_deletion_finalizer::PodDeletionFinalizer for TestDeletionFinalizer {
    async fn finalize_after_actor_cleanup(
        &self,
        key: &PodRuntimeKey,
    ) -> anyhow::Result<PodDeletionFinalizeResult> {
        self.repository.remove_uid(key);
        Ok(PodDeletionFinalizeResult::DeletedOrAlreadyGone)
    }
}
