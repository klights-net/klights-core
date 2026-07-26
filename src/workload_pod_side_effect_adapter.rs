use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::Resource;
use serde_json::Value;

use crate::datastore::{DatastoreBackend, DatastoreHandle, ResourceListQuery};
use crate::side_effects::workload_pod::{WorkloadPodStore, workload_reconcile_keys_for_pod};
use crate::side_effects::{ControllerDispatcherSlot, SideEffect};

/// Enqueues the explicit controller owner of a mutated Pod.
///
/// This is intentionally narrow: Pod status writers do not run side effects,
/// and this hook only follows controller ownerReferences already present on
/// the Pod. The owning controller remains responsible for release/adoption
/// during its normal reconcile.
struct WorkloadPodReconcileEffect {
    db: DatastoreHandle,
    controller_dispatcher: ControllerDispatcherSlot,
}

#[async_trait]
impl SideEffect for WorkloadPodReconcileEffect {
    fn name(&self) -> &'static str {
        "workload_pod_reconcile"
    }

    async fn apply(&self, resource: &Value) -> Result<()> {
        let namespace = resource
            .pointer("/metadata/namespace")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if namespace.is_empty() {
            return Ok(());
        }

        let Some(dispatcher) = self.controller_dispatcher.get() else {
            tracing::debug!(
                "WorkloadPodReconcileEffect skipped for {}: controller dispatcher not yet bound",
                namespace
            );
            return Ok(());
        };

        dispatcher
            .enqueue_reconcile_batch(
                workload_reconcile_keys_for_pod(resource, self.db.as_ref(), namespace).await?,
            )
            .await?;
        Ok(())
    }
}

#[async_trait]
impl<T> WorkloadPodStore for T
where
    T: DatastoreBackend + ?Sized,
{
    async fn get_replica_set(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        self.get_resource("apps/v1", "ReplicaSet", Some(namespace), name)
            .await
    }

    async fn list_replica_sets(&self, namespace: &str) -> Result<Vec<Resource>> {
        self.list_resources(
            "apps/v1",
            "ReplicaSet",
            Some(namespace),
            ResourceListQuery::all(),
        )
        .await
        .map(|listing| listing.items)
    }

    async fn list_replication_controllers(&self, namespace: &str) -> Result<Vec<Resource>> {
        self.list_resources(
            "v1",
            "ReplicationController",
            Some(namespace),
            ResourceListQuery::all(),
        )
        .await
        .map(|listing| listing.items)
    }
}

pub(crate) fn effect(
    db: DatastoreHandle,
    controller_dispatcher: ControllerDispatcherSlot,
) -> Arc<dyn SideEffect> {
    Arc::new(WorkloadPodReconcileEffect {
        db,
        controller_dispatcher,
    })
}
