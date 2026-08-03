use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::{PatchKind, Resource, ResourcePreconditions};
use serde_json::{Value, json};

use crate::bootstrap::controller_adapters::controller_store_error_adapter::map_controller_store_error;
use crate::datastore::{DatastoreBackend, ResourcePatchRequest};
use crate::kubelet::pod_repository::{PodReader, PodRepository};
use klights_controllers::hpa::{
    HpaMetricUsage, HpaMetrics, HpaMetricsSnapshot, HpaRuntime, ScaleTarget, ScaleTargetKind,
    reconcile_hpa_with_runtime,
};
use klights_node_api::NodeMetrics;

#[cfg(test)]
#[path = "../../controller_policy_tests/hpa.rs"]
mod policy_tests;

struct NodeApiHpaMetrics<'a> {
    node_metrics: &'a dyn NodeMetrics,
}

#[async_trait]
impl HpaMetrics for NodeApiHpaMetrics<'_> {
    async fn snapshot(&self, pods: &[Resource]) -> HpaMetricsSnapshot {
        let nodes = pods
            .iter()
            .filter_map(|pod| {
                pod.data
                    .pointer("/spec/nodeName")
                    .and_then(Value::as_str)
                    .filter(|node| !node.is_empty())
                    .map(str::to_string)
            })
            .collect::<std::collections::BTreeSet<_>>();
        let results = futures::future::join_all(nodes.into_iter().map(|node_name| async move {
            let request = klights_node_api::NodeMetricsTarget::try_new(node_name)
                .map(|target| klights_node_api::NodeMetricsRequest::new(target, Vec::new()));
            match request {
                Ok(request) => self.node_metrics.collect_metrics(request).await,
                Err(error) => Err(error),
            }
        }))
        .await;
        let node_snapshot = klights_node_api::NodeMetricsSnapshot::from_results(
            results.into_iter().filter_map(Result::ok),
        );
        let mut snapshot = HpaMetricsSnapshot::default();
        for pod in pods {
            let namespace = pod
                .namespace
                .as_deref()
                .or_else(|| {
                    pod.data
                        .pointer("/metadata/namespace")
                        .and_then(Value::as_str)
                })
                .unwrap_or_default();
            let uid = if pod.uid.is_empty() {
                pod.data
                    .pointer("/metadata/uid")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
            } else {
                &pod.uid
            };
            for container_name in pod
                .data
                .pointer("/spec/containers")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|container| container.get("name").and_then(Value::as_str))
            {
                if let Some(usage) =
                    node_snapshot.container_usage(uid, namespace, &pod.name, container_name)
                {
                    snapshot.insert_container(
                        uid,
                        namespace,
                        &pod.name,
                        container_name,
                        HpaMetricUsage::new(usage.cpu_nanos(), usage.memory_bytes()),
                    );
                }
            }
        }
        snapshot
    }
}

struct RootHpaReconcileAdapter {
    db: crate::datastore::DatastoreHandle,
    pod_repository: std::sync::Arc<PodRepository>,
    non_pod_finalization: std::sync::Arc<dyn klights_reconcile_api::GcNonPodFinalizationPort>,
    coordination: std::sync::Arc<klights_controllers::ControllerCoordination>,
    node_name: std::sync::Arc<str>,
    node_metrics: std::sync::Arc<dyn NodeMetrics>,
    identity: std::sync::Arc<dyn klights_controllers::ControllerIdentityGenerator>,
}

pub(crate) fn controller(
    db: crate::datastore::DatastoreHandle,
    pod_repository: std::sync::Arc<PodRepository>,
    non_pod_finalization: std::sync::Arc<dyn klights_reconcile_api::GcNonPodFinalizationPort>,
    coordination: std::sync::Arc<klights_controllers::ControllerCoordination>,
    node_name: std::sync::Arc<str>,
    node_metrics: std::sync::Arc<dyn NodeMetrics>,
    identity: std::sync::Arc<dyn klights_controllers::ControllerIdentityGenerator>,
) -> std::sync::Arc<klights_controllers::hpa::HpaController> {
    std::sync::Arc::new(klights_controllers::hpa::HpaController::new(
        std::sync::Arc::new(RootHpaReconcileAdapter {
            db,
            pod_repository,
            non_pod_finalization,
            coordination,
            node_name,
            node_metrics,
            identity,
        }),
    ))
}

#[async_trait]
impl klights_controllers::hpa::HpaReconcilePort for RootHpaReconcileAdapter {
    async fn reconcile(
        &self,
        resource: &Value,
        reconcile_time: chrono::DateTime<chrono::Utc>,
    ) -> Result<()> {
        reconcile_hpa_with_metrics(
            self.db.as_ref(),
            self.pod_repository.as_ref(),
            self.non_pod_finalization.as_ref(),
            self.coordination.as_ref(),
            resource,
            &self.node_name,
            self.node_metrics.as_ref(),
            self.identity.as_ref(),
            reconcile_time,
        )
        .await
    }
}

struct HpaControllerAdapter<'a> {
    db: &'a dyn DatastoreBackend,
    pod_repository: &'a PodRepository,
    non_pod_finalization: &'a dyn klights_reconcile_api::GcNonPodFinalizationPort,
    coordination: &'a klights_controllers::ControllerCoordination,
    identity: &'a dyn klights_controllers::ControllerIdentityGenerator,
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
            ScaleTargetKind::Deployment => klights_controllers::deployment::reconcile_deployment(
                self.db,
                pods,
                pods,
                self.identity,
                pods,
                self.non_pod_finalization,
                resource,
                klights_controllers::ControllerReconcileContext::at(
                    self.coordination,
                    node_name,
                    now,
                ),
            )
            .await
            .map_err(map_controller_store_error),
            ScaleTargetKind::ReplicaSet => klights_controllers::replicaset::reconcile_replicaset(
                self.db,
                pods,
                pods,
                self.identity,
                pods,
                self.non_pod_finalization,
                resource,
                klights_controllers::ControllerReconcileContext::at(
                    self.coordination,
                    node_name,
                    now,
                ),
            )
            .await
            .map_err(map_controller_store_error),
            ScaleTargetKind::StatefulSet => {
                klights_controllers::statefulset::reconcile_statefulset(
                    self.db,
                    pods,
                    pods,
                    self.identity,
                    pods,
                    self.non_pod_finalization,
                    resource,
                    klights_controllers::ControllerReconcileContext::at(
                        self.coordination,
                        node_name,
                        now,
                    ),
                )
                .await
                .map_err(map_controller_store_error)
            }
            ScaleTargetKind::ReplicationController => {
                klights_controllers::replicationcontroller::reconcile_replicationcontroller(
                    self.db,
                    pods,
                    pods,
                    self.identity,
                    pods,
                    self.non_pod_finalization,
                    resource,
                    klights_controllers::ControllerReconcileContext::at(
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

#[allow(clippy::too_many_arguments)]
pub async fn reconcile_hpa_with_metrics(
    db: &dyn DatastoreBackend,
    pod_repository: &PodRepository,
    non_pod_finalization: &dyn klights_reconcile_api::GcNonPodFinalizationPort,
    coordination: &klights_controllers::ControllerCoordination,
    hpa: &Value,
    node_name: &str,
    node_metrics: &dyn NodeMetrics,
    identity: &dyn klights_controllers::ControllerIdentityGenerator,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    reconcile_hpa_with_runtime(
        &HpaControllerAdapter {
            db,
            pod_repository,
            non_pod_finalization,
            coordination,
            identity,
        },
        hpa,
        node_name,
        &NodeApiHpaMetrics { node_metrics },
        now,
    )
    .await
}
