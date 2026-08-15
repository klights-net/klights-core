//! Focused-port controller registry and event-driven dispatcher.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context as _, Result};
use klights_reconcile_api::{
    ControllerReconcileSink, ReconcileKey, ReconcileSinkFuture, ServiceReconcileKey,
    ServiceReconcileSink,
};
use serde_json::Value;

use crate::workqueue::{Key, controller_kind_static};
use crate::{
    Context, Controller, ControllerRuntimeDependencies, DispatcherRuntime,
    apiservice_controller::APIServiceController, daemonset_controller::DaemonSetController,
    deployment_controller::DeploymentController, job_controller::JobController,
    pdb_controller::PDBController, pvc_controller::PVCController,
    replicaset_controller::ReplicaSetController,
    replication_controller_runner::ReplicationControllerController,
    service_controller::ServiceController, statefulset_controller::StatefulSetController,
};

pub struct ControllerDispatcher {
    controllers: HashMap<(&'static str, &'static str), Arc<dyn Controller>>,
    runtime: Arc<DispatcherRuntime>,
    dependencies: Option<ControllerRuntimeDependencies>,
    coordination: Arc<crate::ControllerCoordination>,
}

impl ControllerDispatcher {
    /// Default event-driven concurrency for the controller reconciliation
    /// workqueue. Bootstrap selects the process lifetime only.
    pub const DEFAULT_WORKQUEUE_WORKERS: usize = 8;

    #[allow(clippy::too_many_arguments)]
    pub fn new_complete(
        service_ipam: Arc<crate::service::ServiceIpam>,
        nodeport_alloc: Arc<crate::service::NodePortAllocator>,
        task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
        csr_issuer: Option<Arc<dyn crate::csr_signer::CsrIssuer>>,
        hpa_controller: Arc<crate::hpa::HpaController>,
        dependencies: ControllerRuntimeDependencies,
        identity: Arc<dyn crate::ControllerIdentityGenerator>,
    ) -> Self {
        let controllers = Self::controller_registry(
            service_ipam,
            nodeport_alloc,
            csr_issuer,
            hpa_controller,
            identity,
        );
        let coordination = dependencies.coordination.clone();
        Self {
            controllers,
            runtime: Arc::new(DispatcherRuntime::new(task_supervisor)),
            dependencies: Some(dependencies),
            coordination,
        }
    }

    fn controller_registry(
        service_ipam: Arc<crate::service::ServiceIpam>,
        nodeport_alloc: Arc<crate::service::NodePortAllocator>,
        csr_issuer: Option<Arc<dyn crate::csr_signer::CsrIssuer>>,
        hpa_controller: Arc<crate::hpa::HpaController>,
        identity: Arc<dyn crate::ControllerIdentityGenerator>,
    ) -> HashMap<(&'static str, &'static str), Arc<dyn Controller>> {
        let mut controllers: HashMap<(&'static str, &'static str), Arc<dyn Controller>> =
            HashMap::new();
        controllers.insert(
            ("apps/v1", "Deployment"),
            Arc::new(DeploymentController::new(identity.clone())),
        );
        controllers.insert(
            ("apps/v1", "ReplicaSet"),
            Arc::new(ReplicaSetController::new(identity.clone())),
        );
        controllers.insert(
            ("apps/v1", "StatefulSet"),
            Arc::new(StatefulSetController::new(identity.clone())),
        );
        controllers.insert(
            ("apps/v1", "DaemonSet"),
            Arc::new(DaemonSetController::new(identity.clone())),
        );
        controllers.insert(
            ("batch/v1", "Job"),
            Arc::new(JobController::new(identity.clone())),
        );
        controllers.insert(
            ("v1", "Service"),
            Arc::new(ServiceController {
                service_ipam,
                nodeport_alloc,
                identity: identity.clone(),
            }),
        );
        controllers.insert(("v1", "PersistentVolumeClaim"), Arc::new(PVCController));
        controllers.insert(
            ("v1", "ReplicationController"),
            Arc::new(ReplicationControllerController::new(identity)),
        );
        controllers.insert(
            ("policy/v1", "PodDisruptionBudget"),
            Arc::new(PDBController),
        );
        controllers.insert(
            ("autoscaling/v1", "HorizontalPodAutoscaler"),
            hpa_controller.clone(),
        );
        controllers.insert(
            ("autoscaling/v2", "HorizontalPodAutoscaler"),
            hpa_controller,
        );
        controllers.insert(
            ("apiregistration.k8s.io/v1", "APIService"),
            Arc::new(APIServiceController),
        );
        if let Some(issuer) = csr_issuer {
            controllers.insert(
                ("certificates.k8s.io/v1", "CertificateSigningRequest"),
                Arc::new(crate::csr_signer_controller::CsrSignerController::new(
                    issuer,
                )),
            );
        }
        controllers
    }

    pub fn gc_coordination(&self) -> &dyn klights_reconcile_api::GcForegroundDeleteCoordination {
        self.coordination.as_ref()
    }

    pub fn pod_delete_sink(&self) -> &dyn klights_reconcile_api::GcPodDeleteSink {
        self.dependencies().pod_delete_sink.as_ref()
    }

    async fn enqueue(&self, resource: &Value) {
        if let Some(key) = key_for_value(resource) {
            self.runtime.enqueue(key).await;
        }
    }

    async fn enqueue_reconcile_key(&self, key: ReconcileKey) {
        self.runtime.enqueue(key).await;
    }

    async fn enqueue_reconcile_batch(&self, keys: Vec<ReconcileKey>) {
        self.runtime.enqueue_batch(keys).await;
    }

    pub async fn run_worker_pool(
        self: Arc<Self>,
        worker_count: usize,
        cancel: tokio_util::sync::CancellationToken,
    ) {
        let runtime = self.runtime.clone();
        runtime
            .run_worker_pool(worker_count, cancel, move |key| {
                let dispatcher = self.clone();
                async move { dispatcher.dispatch_key(&key).await }
            })
            .await;
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn worker_running(&self) -> bool {
        self.runtime.worker_running()
    }

    /// Dispatches one already-ready key for the canonical test-only runtime
    /// fixture. It never waits for work, so a bounded fixture drain can only
    /// iterate when it actually owns a key.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) async fn dispatch_one_ready_for_test_support(
        &self,
    ) -> anyhow::Result<Option<ReconcileKey>> {
        if self.runtime.worker_running() {
            anyhow::bail!("controller runtime fixture refuses a concurrent worker");
        }
        let Some(key) = self.runtime.try_take_ready().await else {
            return Ok(None);
        };
        if !self.runtime.begin_key_dispatch(&key).await {
            anyhow::bail!("controller runtime fixture refused an in-flight key");
        }
        self.dispatch_key(&key).await;
        self.runtime.finish_key_dispatch(key.clone()).await;
        Ok(Some(key))
    }

    async fn dispatch_key(&self, key: &Key) {
        let resource = match self
            .dependencies()
            .resource_query
            .get_reconcile_resource(key.api_version(), key.kind(), key.namespace(), key.name())
            .await
        {
            Ok(Some(resource)) => resource,
            Ok(None) => {
                self.runtime.record_success(key).await;
                return;
            }
            Err(error) => {
                tracing::warn!(workqueue_key = %key, %error, "workqueue resource read failed");
                self.runtime.requeue_with_backoff(key.clone()).await;
                return;
            }
        };
        let value = crate::ports::inject_resource_version(
            Arc::unwrap_or_clone(resource.data),
            resource.resource_version,
        );
        match self.reconcile_unlocked(&value).await {
            Ok(()) => {
                self.runtime.record_success(key).await;
                if let Err(error) = self.schedule_finished_job_ttl_requeue_if_needed(key).await {
                    tracing::warn!(workqueue_key = %key, %error, "job TTL requeue failed");
                }
            }
            Err(error) => {
                tracing::warn!(workqueue_key = %key, %error, "controller reconcile failed");
                self.runtime.requeue_with_backoff(key.clone()).await;
            }
        }
    }

    async fn schedule_finished_job_ttl_requeue_if_needed(&self, key: &Key) -> Result<()> {
        if key.api_version() != "batch/v1" || key.kind() != "Job" {
            return Ok(());
        }
        let Some(resource) = self
            .dependencies()
            .resource_query
            .get_reconcile_resource(key.api_version(), key.kind(), key.namespace(), key.name())
            .await?
        else {
            return Ok(());
        };
        if resource
            .data
            .pointer("/metadata/deletionTimestamp")
            .is_some()
        {
            return Ok(());
        }
        let Some(delay) = crate::job::job_ttl_cleanup_delay_at(
            &resource.data,
            (self.dependencies().wall_time)(),
        )?
        else {
            return Ok(());
        };
        self.runtime.enqueue_after(key.clone(), delay).await;
        Ok(())
    }

    async fn reconcile_unlocked(&self, resource: &Value) -> Result<()> {
        let api_version = resource
            .get("apiVersion")
            .and_then(Value::as_str)
            .context("Missing apiVersion in resource")?;
        let kind = resource
            .get("kind")
            .and_then(Value::as_str)
            .context("Missing kind in resource")?;
        if let Some(namespace) = resource
            .pointer("/metadata/namespace")
            .and_then(Value::as_str)
            .filter(|namespace| !namespace.is_empty())
            && self
                .dependencies()
                .resource_query
                .namespace_is_terminating(namespace)
                .await?
        {
            return Ok(());
        }
        if let Some(controller) = self.controllers.get(&(api_version, kind)) {
            controller
                .reconcile(
                    resource.clone(),
                    Context::new(
                        self.dependencies().clone(),
                        (self.dependencies().wall_time)(),
                    ),
                )
                .await
                .with_context(|| format!("{} controller reconcile failed", controller.name()))?;
        }
        Ok(())
    }

    fn dependencies(&self) -> &ControllerRuntimeDependencies {
        self.dependencies
            .as_ref()
            .expect("queue-only ControllerDispatcher cannot execute reconciliation")
    }
}

impl klights_reconcile_api::ControllerDispatcherPort for ControllerDispatcher {
    fn enqueue<'a>(
        &'a self,
        resource: &'a Value,
    ) -> klights_reconcile_api::ControllerDispatchFuture<'a, ()> {
        Box::pin(async move { self.enqueue(resource).await })
    }

    fn enqueue_reconcile(
        &self,
        key: ReconcileKey,
    ) -> klights_reconcile_api::ControllerDispatchFuture<'_, ()> {
        Box::pin(async move { self.enqueue_reconcile_key(key).await })
    }

    fn pending_reconcile_keys(
        &self,
    ) -> klights_reconcile_api::ControllerDispatchFuture<'_, Vec<ReconcileKey>> {
        Box::pin(async move { self.runtime.pending_keys().await })
    }
}

fn key_for_value(resource: &Value) -> Option<Key> {
    let api_version = resource.get("apiVersion").and_then(Value::as_str)?;
    let kind = resource.get("kind").and_then(Value::as_str)?;
    let (api_version, kind) = controller_kind_static(api_version, kind)?;
    let name = resource.pointer("/metadata/name").and_then(Value::as_str)?;
    let namespace = resource
        .pointer("/metadata/namespace")
        .and_then(Value::as_str);
    Some(match namespace {
        Some(namespace) if !namespace.is_empty() => {
            Key::namespaced(api_version, kind, namespace, name)
        }
        _ => Key::cluster(api_version, kind, name),
    })
}

impl ControllerReconcileSink for ControllerDispatcher {
    fn enqueue_reconcile_batch(&self, keys: Vec<ReconcileKey>) -> ReconcileSinkFuture<'_> {
        Box::pin(async move {
            if keys
                .iter()
                .any(|key| key.api_version() == "v1" && key.kind() == "Service")
            {
                return Err(klights_reconcile_api::ReconcileSinkError::unsupported_key(
                    "Service reconcile keys must use ServiceReconcileSink",
                ));
            }
            ControllerDispatcher::enqueue_reconcile_batch(self, keys).await;
            Ok(())
        })
    }
}

impl ServiceReconcileSink for ControllerDispatcher {
    fn enqueue_service_reconcile_batch(
        &self,
        keys: Vec<ServiceReconcileKey>,
    ) -> ReconcileSinkFuture<'_> {
        Box::pin(async move {
            ControllerDispatcher::enqueue_reconcile_batch(
                self,
                keys.into_iter()
                    .map(ServiceReconcileKey::into_reconcile_key)
                    .collect(),
            )
            .await;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn key_projection_rejects_unregistered_and_incomplete_resources() {
        assert!(key_for_value(&json!({})).is_none());
        assert!(
            key_for_value(&json!({
                "apiVersion": "v1",
                "kind": "Endpoints",
                "metadata": {"name": "legacy", "namespace": "default"}
            }))
            .is_none()
        );
        assert_eq!(
            key_for_value(&json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {"name": "web", "namespace": "default"}
            }))
            .expect("registered controller key")
            .to_string(),
            "apps/v1/Deployment default/web"
        );
    }

    #[test]
    fn default_worker_pool_concurrency_is_controller_owned() {
        assert_eq!(ControllerDispatcher::DEFAULT_WORKQUEUE_WORKERS, 8);
    }
}
