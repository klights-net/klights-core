use anyhow::{Result, anyhow};
use async_trait::async_trait;
use klights_cluster_core::{PatchKind, Resource, ResourcePreconditions};
use serde_json::{Value, json};

use crate::controller::{Context, Controller};
use crate::controllers::hpa::{
    HpaRuntime, ScaleTarget, ScaleTargetKind, reconcile_hpa_with_runtime,
};
use crate::datastore::{DatastoreBackend, ResourcePatchRequest};
use crate::kubelet::pod_repository::{PodReader, PodRepository};
use crate::metrics::MetricsProvider;

pub struct HpaController;

#[async_trait]
impl Controller for HpaController {
    fn name(&self) -> &'static str {
        "horizontalpodautoscaler"
    }

    async fn reconcile(&self, resource: Value, ctx: Context) -> Result<()> {
        let pod_repository = ctx.pod_repository().ok_or_else(|| {
            anyhow::anyhow!(
                "horizontalpodautoscaler requires pod_repository in Context — wire it via \
                 ControllerDispatcher::set_pod_repository or Context::with_pod_repository"
            )
        })?;
        let fallback_metrics;
        let metrics_provider = match ctx.metrics_provider() {
            Some(provider) => provider.as_ref(),
            None => {
                fallback_metrics = crate::metrics::FallbackOnlyMetricsProvider;
                &fallback_metrics as &dyn MetricsProvider
            }
        };
        let non_pod_finalization = ctx.non_pod_finalization().ok_or_else(|| {
            anyhow::anyhow!("horizontalpodautoscaler requires non-Pod GC finalization in Context")
        })?;
        reconcile_hpa_with_metrics(
            ctx.db_handle().as_ref(),
            pod_repository.as_ref(),
            non_pod_finalization,
            &resource,
            ctx.node_name(),
            metrics_provider,
        )
        .await
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
}

#[async_trait]
impl HpaRuntime for HpaControllerAdapter<'_> {
    async fn get_hpa(
        &self,
        api_version: &str,
        namespace: &str,
        name: &str,
    ) -> Result<Option<Resource>> {
        self.db
            .get_resource(
                api_version,
                "HorizontalPodAutoscaler",
                Some(namespace),
                name,
            )
            .await
    }

    async fn get_scale_target(
        &self,
        api_version: &str,
        kind: &str,
        namespace: &str,
        name: &str,
    ) -> Result<Option<Resource>> {
        self.db
            .get_resource(api_version, kind, Some(namespace), name)
            .await
    }

    async fn list_pods(&self, namespace: &str) -> Result<Vec<Resource>> {
        PodReader::list_pods(self.pod_repository, Some(namespace), None, None, None, None)
            .await
            .map(|listing| listing.items)
    }

    async fn patch_scale_target(&self, target: &ScaleTarget, replicas: i64) -> Result<Resource> {
        self.db
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
            .await?
            .ok_or_else(|| {
                anyhow!(
                    "{} {} disappeared during HPA scale",
                    target.kind,
                    target.name
                )
            })
    }

    async fn reconcile_scaled_target(
        &self,
        target: &ScaleTarget,
        resource: &Value,
        node_name: &str,
    ) -> Result<()> {
        let pods = self.pod_repository;
        match target.kind_tag {
            ScaleTargetKind::Deployment => {
                crate::controllers::deployment::reconcile_deployment(
                    self.db,
                    pods,
                    pods,
                    pods,
                    self.non_pod_finalization,
                    resource,
                    node_name,
                )
                .await
            }
            ScaleTargetKind::ReplicaSet => {
                crate::controllers::replicaset::reconcile_replicaset(
                    self.db,
                    pods,
                    pods,
                    pods,
                    self.non_pod_finalization,
                    resource,
                    node_name,
                )
                .await
            }
            ScaleTargetKind::StatefulSet => {
                crate::controllers::statefulset::reconcile_statefulset(
                    self.db,
                    pods,
                    pods,
                    pods,
                    self.non_pod_finalization,
                    resource,
                    node_name,
                )
                .await
            }
            ScaleTargetKind::ReplicationController => {
                crate::controllers::replicationcontroller::reconcile_replicationcontroller(
                    self.db,
                    pods,
                    pods,
                    pods,
                    self.non_pod_finalization,
                    resource,
                    node_name,
                )
                .await
            }
        }
    }

    async fn update_hpa_status(&self, current: &Resource, status: Value) -> Result<()> {
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
    }

    fn is_conflict(&self, error: &anyhow::Error) -> bool {
        crate::datastore::errors::is_conflict_error(error)
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
        hpa,
        node_name,
        &crate::metrics::FallbackOnlyMetricsProvider,
    )
    .await
}

pub async fn reconcile_hpa_with_metrics(
    db: &dyn DatastoreBackend,
    pod_repository: &PodRepository,
    non_pod_finalization: &dyn klights_reconcile_api::GcNonPodFinalizationPort,
    hpa: &Value,
    node_name: &str,
    metrics_provider: &dyn MetricsProvider,
) -> Result<()> {
    reconcile_hpa_with_runtime(
        &HpaControllerAdapter {
            db,
            pod_repository,
            non_pod_finalization,
        },
        hpa,
        node_name,
        metrics_provider,
    )
    .await
}
