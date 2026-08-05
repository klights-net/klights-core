use klights_cluster_core::{Resource, ResourcePreconditions};

use crate::datastore::DatastoreHandle;

pub(crate) struct GcOwnerLifecycleAdapter {
    db: DatastoreHandle,
    pod_delete_sink: std::sync::Arc<crate::kubelet::pod_repository::PodRepository>,
    non_pod_finalization: GcNonPodFinalizationAdapter,
    coordination: std::sync::Arc<dyn klights_reconcile_api::GcForegroundDeleteCoordination>,
}

impl GcOwnerLifecycleAdapter {
    pub(crate) fn new_with_coordination(
        db: DatastoreHandle,
        pod_delete_sink: std::sync::Arc<crate::kubelet::pod_repository::PodRepository>,
        coordination: std::sync::Arc<dyn klights_reconcile_api::GcForegroundDeleteCoordination>,
    ) -> Self {
        Self {
            non_pod_finalization: GcNonPodFinalizationAdapter::new(db.clone()),
            db,
            pod_delete_sink,
            coordination,
        }
    }

    fn sink_error(error: anyhow::Error) -> klights_reconcile_api::ReconcileSinkError {
        klights_reconcile_api::ReconcileSinkError::unavailable(error.to_string())
    }
}

impl klights_reconcile_api::GcOwnerLifecyclePort for GcOwnerLifecycleAdapter {
    fn reconcile_owner_references(
        &self,
        resource: Resource,
    ) -> klights_reconcile_api::ReconcileSinkFuture<'_> {
        Box::pin(async move {
            klights_controllers::gc::reconcile_owner_references(
                self.db.as_ref(),
                resource,
                self.pod_delete_sink.as_ref(),
                &self.non_pod_finalization,
                self.coordination.as_ref(),
            )
            .await
            .map(|_| ())
            .map_err(Self::sink_error)
        })
    }

    fn cascade_delete(
        &self,
        owner: klights_reconcile_api::GcOwnerIdentity,
    ) -> klights_reconcile_api::ReconcileSinkFuture<'_> {
        Box::pin(async move {
            klights_controllers::gc::cascade_delete_with_uid(
                self.db.as_ref(),
                &owner.uid,
                &owner.api_version,
                &owner.name,
                &owner.kind,
                owner.namespace,
                self.pod_delete_sink.as_ref(),
                &self.non_pod_finalization,
                self.coordination.as_ref(),
            )
            .await
            .map_err(Self::sink_error)
        })
    }

    fn sweep_dependents(
        &self,
        owner: klights_reconcile_api::GcOwnerIdentity,
    ) -> klights_reconcile_api::GcOwnerBoolFuture<'_> {
        Box::pin(async move {
            klights_controllers::gc::owner_cascade_sweep_once(
                self.db.as_ref(),
                &owner.uid,
                &owner.api_version,
                &owner.name,
                &owner.kind,
                owner.namespace,
                self.pod_delete_sink.as_ref(),
                &self.non_pod_finalization,
                self.coordination.as_ref(),
            )
            .await
            .map_err(Self::sink_error)
        })
    }

    fn finalize_foreground_owner(
        &self,
        owner: Resource,
    ) -> klights_reconcile_api::GcOwnerBoolFuture<'_> {
        Box::pin(async move {
            klights_controllers::gc::finalize_foreground_owner_if_ready(
                self.db.as_ref(),
                &owner,
                self.pod_delete_sink.as_ref(),
                &self.non_pod_finalization,
                self.coordination.as_ref(),
            )
            .await
            .map_err(Self::sink_error)
        })
    }
}

pub(crate) struct GcNonPodFinalizationAdapter {
    db: DatastoreHandle,
}

impl GcNonPodFinalizationAdapter {
    pub(crate) fn new(db: DatastoreHandle) -> Self {
        Self { db }
    }
}

impl klights_reconcile_api::GcNonPodFinalizationPort for GcNonPodFinalizationAdapter {
    fn finalize_non_pod(
        &self,
        request: klights_reconcile_api::GcNonPodFinalizationRequest,
    ) -> klights_reconcile_api::GcNonPodFinalizationFuture<'_> {
        Box::pin(async move {
            if request.resource.api_version == "v1" && request.resource.kind == "Pod" {
                return Err(klights_reconcile_api::ReconcileSinkError::unavailable(
                    "GC non-Pod finalization port rejects v1/Pod",
                ));
            }
            let lifecycle =
                crate::bootstrap::finalizer_lifecycle_adapter::BorrowedFinalizerLifecycleStore::new(
                    self.db.as_ref(),
                );
            let delete = k8s_native_service::generic_command::NonForegroundDeleteRequest {
                target: k8s_native_service::generic_command::ResourceDeleteTarget {
                    api_version: &request.resource.api_version,
                    kind: &request.resource.kind,
                    namespace: request.resource.namespace.as_deref(),
                    name: &request.resource.name,
                },
                initial_resource: request.resource.clone(),
                delete_preconditions: ResourcePreconditions::uid(request.resource.uid.clone()),
                orphan_children_before_completion: request.orphan_children,
                uid_mismatch_is_conflict: false,
                grace_seconds: 0,
                operation_now: klights_supervisor::SystemWallClock::now_utc(),
            };
            match k8s_native_service::generic_command::complete_non_foreground_delete_with_live_recheck(
                &lifecycle,
                delete,
            )
            .await
            {
                Ok(k8s_native_service::generic_command::DeleteCompletion::HardDeleted(_)) => Ok(
                    klights_reconcile_api::GcNonPodFinalizationOutcome::HardDeleted,
                ),
                Ok(k8s_native_service::generic_command::DeleteCompletion::MarkedTerminating(_)) => {
                    Ok(klights_reconcile_api::GcNonPodFinalizationOutcome::MarkedTerminating)
                }
                Ok(k8s_native_service::generic_command::DeleteCompletion::GoneOrUidChanged)
                | Err(k8s_native_service::AppError::NotFound(_)) => {
                    Ok(klights_reconcile_api::GcNonPodFinalizationOutcome::Gone)
                }
                Err(error) => Err(klights_reconcile_api::ReconcileSinkError::unavailable(
                    format!("{error:?}"),
                )),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use klights_reconcile_api::{GcNonPodFinalizationPort, GcNonPodFinalizationRequest};

    #[tokio::test]
    async fn non_pod_port_rejects_pod_without_touching_datastore() {
        let (db, db_handle) =
            crate::datastore::sqlite::Datastore::new_in_memory_with_handle().await;
        let pod = db
            .create_resource(
                "v1",
                "Pod",
                Some("default"),
                "guarded",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "namespace": "default",
                        "name": "guarded",
                        "uid": "guarded-uid"
                    },
                    "spec": {"nodeName": "node-a", "containers": []}
                }),
            )
            .await
            .expect("create Pod");
        let adapter = GcNonPodFinalizationAdapter::new(db_handle);

        let error = adapter
            .finalize_non_pod(GcNonPodFinalizationRequest {
                resource: pod,
                orphan_children: false,
            })
            .await
            .expect_err("non-Pod port must reject Pod");

        assert!(error.to_string().contains("rejects v1/Pod"));
        assert!(
            db.get_resource("v1", "Pod", Some("default"), "guarded")
                .await
                .expect("read Pod")
                .is_some(),
            "rejected request must not remove the actor-owned Pod row"
        );
    }
}
