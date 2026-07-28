use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::{PatchKind, Resource, ResourcePreconditions};
use serde_json::{Value, json};

use crate::controller_store_error_adapter::map_controller_store_error;
use crate::controllers::hpa::{
    HpaRuntime, ScaleTarget, ScaleTargetKind, reconcile_hpa_with_runtime,
};
use crate::controllers::{Context, Controller};
use crate::datastore::{DatastoreBackend, ResourcePatchRequest};
use crate::kubelet::pod_repository::{PodReader, PodRepository};
use klights_node_api::NodeMetrics;

#[cfg(test)]
pub struct HpaController;

#[cfg(test)]
impl HpaController {
    pub(crate) fn new(
        _db: crate::datastore::DatastoreHandle,
        _pod_repository: std::sync::Arc<PodRepository>,
        _non_pod_finalization: std::sync::Arc<dyn klights_reconcile_api::GcNonPodFinalizationPort>,
        _coordination: std::sync::Arc<crate::controllers::ControllerCoordination>,
        _node_name: std::sync::Arc<str>,
        _node_metrics: std::sync::Arc<dyn NodeMetrics>,
    ) -> Self {
        Self
    }
}

#[cfg(not(test))]
pub struct HpaController {
    db: crate::datastore::DatastoreHandle,
    pod_repository: std::sync::Arc<PodRepository>,
    non_pod_finalization: std::sync::Arc<dyn klights_reconcile_api::GcNonPodFinalizationPort>,
    coordination: std::sync::Arc<crate::controllers::ControllerCoordination>,
    node_name: std::sync::Arc<str>,
    node_metrics: std::sync::Arc<dyn NodeMetrics>,
}

#[cfg(not(test))]
impl HpaController {
    pub(crate) fn new(
        db: crate::datastore::DatastoreHandle,
        pod_repository: std::sync::Arc<PodRepository>,
        non_pod_finalization: std::sync::Arc<dyn klights_reconcile_api::GcNonPodFinalizationPort>,
        coordination: std::sync::Arc<crate::controllers::ControllerCoordination>,
        node_name: std::sync::Arc<str>,
        node_metrics: std::sync::Arc<dyn NodeMetrics>,
    ) -> Self {
        Self {
            db,
            pod_repository,
            non_pod_finalization,
            coordination,
            node_name,
            node_metrics,
        }
    }
}

#[async_trait]
impl Controller for HpaController {
    fn name(&self) -> &'static str {
        "horizontalpodautoscaler"
    }

    async fn reconcile(&self, resource: Value, ctx: Context) -> Result<()> {
        #[cfg(not(test))]
        {
            return reconcile_hpa_with_metrics(
                self.db.as_ref(),
                self.pod_repository.as_ref(),
                self.non_pod_finalization.as_ref(),
                self.coordination.as_ref(),
                &resource,
                &self.node_name,
                self.node_metrics.as_ref(),
                ctx.reconcile_time(),
            )
            .await;
        }
        #[cfg(test)]
        {
            let pod_repository = ctx.pod_repository().ok_or_else(|| {
                anyhow::anyhow!(
                    "horizontalpodautoscaler requires pod_repository in Context — wire it via \
                 ControllerDispatcher::set_pod_repository or Context::with_pod_repository"
                )
            })?;
            let fallback_metrics;
            let node_metrics = match ctx.node_metrics() {
                Some(provider) => provider.as_ref(),
                None => {
                    fallback_metrics = crate::node_metrics_adapter::UnavailableNodeMetrics;
                    &fallback_metrics as &dyn NodeMetrics
                }
            };
            let non_pod_finalization = ctx.non_pod_finalization().ok_or_else(|| {
                anyhow::anyhow!(
                    "horizontalpodautoscaler requires non-Pod GC finalization in Context"
                )
            })?;
            reconcile_hpa_with_metrics(
                ctx.db_handle().as_ref(),
                pod_repository.as_ref(),
                non_pod_finalization.as_ref(),
                ctx.coordination(),
                &resource,
                ctx.node_name(),
                node_metrics,
                ctx.reconcile_time(),
            )
            .await
        }
    }
}

#[cfg(test)]
mod dispatch_tests {
    use super::*;

    #[test]
    fn controller_name_is_stable() {
        assert_eq!(HpaController.name(), "horizontalpodautoscaler");
    }
}

struct HpaControllerAdapter<'a> {
    db: &'a dyn DatastoreBackend,
    pod_repository: &'a PodRepository,
    non_pod_finalization: &'a dyn klights_reconcile_api::GcNonPodFinalizationPort,
    coordination: &'a crate::controllers::ControllerCoordination,
}

#[async_trait]
impl HpaRuntime for HpaControllerAdapter<'_> {
    async fn get_hpa(
        &self,
        api_version: &str,
        namespace: &str,
        name: &str,
    ) -> klights_reconcile_api::ControllerStoreResult<Option<Resource>> {
        self.db
            .get_resource(
                api_version,
                "HorizontalPodAutoscaler",
                Some(namespace),
                name,
            )
            .await
            .map_err(map_controller_store_error)
    }

    async fn get_scale_target(
        &self,
        api_version: &str,
        kind: &str,
        namespace: &str,
        name: &str,
    ) -> klights_reconcile_api::ControllerStoreResult<Option<Resource>> {
        self.db
            .get_resource(api_version, kind, Some(namespace), name)
            .await
            .map_err(map_controller_store_error)
    }

    async fn list_pods(
        &self,
        namespace: &str,
    ) -> klights_reconcile_api::ControllerStoreResult<Vec<Resource>> {
        PodReader::list_pods(self.pod_repository, Some(namespace), None, None, None, None)
            .await
            .map(|listing| listing.items)
            .map_err(map_controller_store_error)
    }

    async fn patch_scale_target(
        &self,
        target: &ScaleTarget,
        replicas: i64,
    ) -> klights_reconcile_api::ControllerStoreResult<Resource> {
        let patched = self
            .db
            .patch_resource_latest_with_preconditions(
                target.api_version,
                target.kind,
                Some(&target.namespace),
                &target.name,
                ResourcePatchRequest::new(
                    PatchKind::Merge,
                    json!({"spec": {"replicas": replicas.max(0)}}),
                    ResourcePreconditions::uid(target.uid.clone()),
                ),
            )
            .await
            .map_err(map_controller_store_error)?;
        patched.ok_or_else(|| {
            klights_reconcile_api::ControllerStoreError::not_found(format!(
                "{} {} disappeared during HPA scale",
                target.kind, target.name
            ))
        })
    }

    async fn reconcile_scaled_target(
        &self,
        target: &ScaleTarget,
        resource: &Value,
        node_name: &str,
    ) -> klights_reconcile_api::ControllerStoreResult<()> {
        let pods = self.pod_repository;
        let now = chrono::Utc::now();
        match target.kind_tag {
            ScaleTargetKind::Deployment => crate::controllers::deployment::reconcile_deployment(
                self.db,
                pods,
                pods,
                pods,
                self.non_pod_finalization,
                resource,
                crate::controllers::ControllerReconcileContext::at(
                    self.coordination,
                    node_name,
                    now,
                ),
            )
            .await
            .map_err(map_controller_store_error),
            ScaleTargetKind::ReplicaSet => crate::controllers::replicaset::reconcile_replicaset(
                self.db,
                pods,
                pods,
                pods,
                self.non_pod_finalization,
                resource,
                crate::controllers::ControllerReconcileContext::at(
                    self.coordination,
                    node_name,
                    now,
                ),
            )
            .await
            .map_err(map_controller_store_error),
            ScaleTargetKind::StatefulSet => crate::controllers::statefulset::reconcile_statefulset(
                self.db,
                pods,
                pods,
                pods,
                self.non_pod_finalization,
                resource,
                crate::controllers::ControllerReconcileContext::at(
                    self.coordination,
                    node_name,
                    now,
                ),
            )
            .await
            .map_err(map_controller_store_error),
            ScaleTargetKind::ReplicationController => {
                crate::controllers::replicationcontroller::reconcile_replicationcontroller(
                    self.db,
                    pods,
                    pods,
                    pods,
                    self.non_pod_finalization,
                    resource,
                    crate::controllers::ControllerReconcileContext::at(
                        self.coordination,
                        node_name,
                        now,
                    ),
                )
                .await
                .map_err(map_controller_store_error)
            }
        }
    }

    async fn update_hpa_status(
        &self,
        current: &Resource,
        status: Value,
    ) -> klights_reconcile_api::ControllerStoreResult<()> {
        self.db
            .update_status_only_with_preconditions(
                &current.api_version,
                "HorizontalPodAutoscaler",
                current.namespace.as_deref(),
                &current.name,
                status,
                ResourcePreconditions::from_resource(current),
            )
            .await
            .map(|_| ())
            .map_err(map_controller_store_error)
    }
}

#[cfg(test)]
pub async fn reconcile_hpa(
    db: &dyn DatastoreBackend,
    pod_repository: &PodRepository,
    non_pod_finalization: &dyn klights_reconcile_api::GcNonPodFinalizationPort,
    hpa: &Value,
    node_name: &str,
) -> Result<()> {
    reconcile_hpa_with_metrics(
        db,
        pod_repository,
        non_pod_finalization,
        &crate::controllers::ControllerCoordination::new(),
        hpa,
        node_name,
        &crate::node_metrics_adapter::UnavailableNodeMetrics,
        chrono::Utc::now(),
    )
    .await
}

pub async fn reconcile_hpa_with_metrics(
    db: &dyn DatastoreBackend,
    pod_repository: &PodRepository,
    non_pod_finalization: &dyn klights_reconcile_api::GcNonPodFinalizationPort,
    coordination: &crate::controllers::ControllerCoordination,
    hpa: &Value,
    node_name: &str,
    node_metrics: &dyn NodeMetrics,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    reconcile_hpa_with_runtime(
        &HpaControllerAdapter {
            db,
            pod_repository,
            non_pod_finalization,
            coordination,
        },
        hpa,
        node_name,
        node_metrics,
        now,
    )
    .await
}
