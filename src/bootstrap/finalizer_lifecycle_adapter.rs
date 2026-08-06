use std::sync::Arc;

use klights_reconcile_api::{
    FinalizerEffectsRequest, FinalizerLifecycleError, FinalizerLifecycleFuture,
    FinalizerLifecyclePort, FinalizerOrphanRequest, FinalizerResourceTarget,
    FinalizerTombstoneDeleteRequest, FinalizerUpdateRequest, GcPodDeleteSink,
};

use crate::datastore::DatastoreBackend;
use crate::datastore::DatastoreHandle;
use klights_cluster_datastore::errors::DatastoreError;

pub(crate) struct DatastoreFinalizerLifecycleAdapter {
    db: DatastoreHandle,
    pod_delete_sink: Arc<dyn GcPodDeleteSink>,
    side_effects: Arc<klights_controllers::side_effects::SideEffectRegistry>,
    metrics: Arc<klights_controllers::side_effects::SideEffectMetrics>,
    non_pod_finalization:
        crate::bootstrap::controller_adapters::gc_delete_adapter::GcNonPodFinalizationAdapter,
    coordination: Arc<dyn klights_reconcile_api::GcForegroundDeleteCoordination>,
}

impl DatastoreFinalizerLifecycleAdapter {
    #[cfg(test)]
    pub(crate) fn new(
        db: DatastoreHandle,
        pod_delete_sink: Arc<dyn GcPodDeleteSink>,
        side_effects: Arc<klights_controllers::side_effects::SideEffectRegistry>,
        metrics: Arc<klights_controllers::side_effects::SideEffectMetrics>,
    ) -> Arc<Self> {
        Self::new_with_coordination(
            db,
            pod_delete_sink,
            side_effects,
            metrics,
            Arc::new(klights_controllers::ControllerCoordination::new()),
        )
    }

    pub(crate) fn new_with_coordination(
        db: DatastoreHandle,
        pod_delete_sink: Arc<dyn GcPodDeleteSink>,
        side_effects: Arc<klights_controllers::side_effects::SideEffectRegistry>,
        metrics: Arc<klights_controllers::side_effects::SideEffectMetrics>,
        coordination: Arc<dyn klights_reconcile_api::GcForegroundDeleteCoordination>,
    ) -> Arc<Self> {
        Arc::new(Self {
            non_pod_finalization: crate::bootstrap::controller_adapters::gc_delete_adapter::GcNonPodFinalizationAdapter::new(
                db.clone(),
            ),
            db,
            pod_delete_sink,
            side_effects,
            metrics,
            coordination,
        })
    }
}

fn lifecycle_error(error: anyhow::Error) -> FinalizerLifecycleError {
    if let Some(error) = error.downcast_ref::<DatastoreError>() {
        return match error {
            DatastoreError::NotFound { message } => {
                FinalizerLifecycleError::NotFound(message.clone())
            }
            DatastoreError::AlreadyExists { message } | DatastoreError::Conflict { message } => {
                FinalizerLifecycleError::Conflict(message.clone())
            }
        };
    }
    if klights_cluster_datastore::errors::is_conflict_error(&error) {
        FinalizerLifecycleError::Conflict(error.to_string())
    } else {
        FinalizerLifecycleError::Internal(error.to_string())
    }
}

fn target_parts(target: &FinalizerResourceTarget) -> (&str, &str, Option<&str>, &str) {
    (
        target.api_version(),
        target.kind(),
        target.namespace(),
        target.name(),
    )
}

impl FinalizerLifecyclePort for DatastoreFinalizerLifecycleAdapter {
    fn get_resource(
        &self,
        target: FinalizerResourceTarget,
    ) -> FinalizerLifecycleFuture<'_, Option<klights_cluster_core::Resource>> {
        Box::pin(async move {
            let (api_version, kind, namespace, name) = target_parts(&target);
            self.db
                .get_resource(api_version, kind, namespace, name)
                .await
                .map_err(lifecycle_error)
        })
    }

    fn update_resource(
        &self,
        request: FinalizerUpdateRequest,
    ) -> FinalizerLifecycleFuture<'_, klights_cluster_core::Resource> {
        Box::pin(async move {
            let (api_version, kind, namespace, name) = target_parts(&request.target);
            self.db
                .update_resource_with_preconditions(
                    api_version,
                    kind,
                    namespace,
                    name,
                    request.data,
                    request.preconditions,
                )
                .await
                .map_err(lifecycle_error)
        })
    }

    fn delete_with_tombstone(
        &self,
        request: FinalizerTombstoneDeleteRequest,
    ) -> FinalizerLifecycleFuture<'_, klights_cluster_core::Resource> {
        Box::pin(async move {
            let (api_version, kind, namespace, name) = target_parts(&request.target);
            self.db
                .delete_resource_without_watch_with_tombstone(
                    api_version,
                    kind,
                    namespace,
                    name,
                    request.preconditions,
                    request.grace_seconds,
                )
                .await
                .map_err(lifecycle_error)
        })
    }

    fn orphan_children(&self, request: FinalizerOrphanRequest) -> FinalizerLifecycleFuture<'_, ()> {
        Box::pin(async move {
            klights_controllers::gc::orphan_children(
                self.db.as_ref(),
                &request.owner_uid,
                request.target.api_version(),
                request.target.name(),
                request.target.kind(),
                request.target.namespace().map(str::to_string),
            )
            .await
            .map_err(|error| FinalizerLifecycleError::Internal(error.to_string()))
        })
    }

    fn run_finalized_effects(
        &self,
        request: FinalizerEffectsRequest,
    ) -> FinalizerLifecycleFuture<'_, ()> {
        Box::pin(async move {
            let resource = request.resource;
            if let Err(error) = klights_controllers::gc::cascade_delete_with_uid(
                self.db.as_ref(),
                &resource.uid,
                &resource.api_version,
                &resource.name,
                &resource.kind,
                resource.namespace.clone(),
                self.pod_delete_sink.as_ref(),
                &self.non_pod_finalization,
                self.coordination.as_ref(),
            )
            .await
            {
                self.metrics
                    .cascade_delete_failures_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::error!(
                    namespace = ?resource.namespace,
                    name = %resource.name,
                    error = %error,
                    "cascade delete after finalizer-drained hard delete failed"
                );
            }
            let _ = self.side_effects.run_hooks(&resource.data).await;
            Ok(())
        })
    }
}

pub(crate) struct BorrowedFinalizerLifecycleStore<'a> {
    db: &'a dyn DatastoreBackend,
}

impl<'a> BorrowedFinalizerLifecycleStore<'a> {
    pub(crate) fn new(db: &'a dyn DatastoreBackend) -> Self {
        Self { db }
    }
}

impl FinalizerLifecyclePort for BorrowedFinalizerLifecycleStore<'_> {
    fn get_resource(
        &self,
        target: FinalizerResourceTarget,
    ) -> FinalizerLifecycleFuture<'_, Option<klights_cluster_core::Resource>> {
        Box::pin(async move {
            let (api_version, kind, namespace, name) = target_parts(&target);
            self.db
                .get_resource(api_version, kind, namespace, name)
                .await
                .map_err(lifecycle_error)
        })
    }

    fn update_resource(
        &self,
        request: FinalizerUpdateRequest,
    ) -> FinalizerLifecycleFuture<'_, klights_cluster_core::Resource> {
        Box::pin(async move {
            let (api_version, kind, namespace, name) = target_parts(&request.target);
            self.db
                .update_resource_with_preconditions(
                    api_version,
                    kind,
                    namespace,
                    name,
                    request.data,
                    request.preconditions,
                )
                .await
                .map_err(lifecycle_error)
        })
    }

    fn delete_with_tombstone(
        &self,
        request: FinalizerTombstoneDeleteRequest,
    ) -> FinalizerLifecycleFuture<'_, klights_cluster_core::Resource> {
        Box::pin(async move {
            let (api_version, kind, namespace, name) = target_parts(&request.target);
            self.db
                .delete_resource_without_watch_with_tombstone(
                    api_version,
                    kind,
                    namespace,
                    name,
                    request.preconditions,
                    request.grace_seconds,
                )
                .await
                .map_err(lifecycle_error)
        })
    }

    fn orphan_children(&self, request: FinalizerOrphanRequest) -> FinalizerLifecycleFuture<'_, ()> {
        Box::pin(async move {
            klights_controllers::gc::orphan_children(
                self.db,
                &request.owner_uid,
                request.target.api_version(),
                request.target.name(),
                request.target.kind(),
                request.target.namespace().map(str::to_string),
            )
            .await
            .map_err(|error| FinalizerLifecycleError::Internal(error.to_string()))
        })
    }

    fn run_finalized_effects(
        &self,
        _request: FinalizerEffectsRequest,
    ) -> FinalizerLifecycleFuture<'_, ()> {
        Box::pin(async {
            Err(FinalizerLifecycleError::Internal(
                "borrowed finalizer store has no post-delete effects".to_string(),
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use klights_reconcile_api::{GcPodDeleteFuture, GcPodDeleteRequest};

    use super::*;

    #[derive(Default)]
    struct RecordingPodDeleteSink {
        requests: Mutex<Vec<klights_types::PodIdentity>>,
    }

    impl GcPodDeleteSink for RecordingPodDeleteSink {
        fn request_gc_pod_delete(&self, request: GcPodDeleteRequest) -> GcPodDeleteFuture<'_> {
            self.requests
                .lock()
                .expect("Pod delete request lock poisoned")
                .push(request.into_identity());
            Box::pin(async { Ok(()) })
        }
    }

    #[tokio::test]
    async fn finalized_effects_route_bound_pod_children_to_actor_sink() {
        let db = crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap();
        let db_handle: DatastoreHandle = Arc::new(db.clone());
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "child",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "child",
                    "namespace": "default",
                    "uid": "child-uid",
                    "ownerReferences": [{
                        "apiVersion": "apps/v1",
                        "kind": "Deployment",
                        "name": "owner",
                        "uid": "owner-uid"
                    }]
                },
                "spec": {
                    "nodeName": "worker-1",
                    "containers": [{"name": "app", "image": "example.invalid/app"}]
                }
            }),
        )
        .await
        .expect("create bound Pod child");

        let sink = Arc::new(RecordingPodDeleteSink::default());
        let adapter = DatastoreFinalizerLifecycleAdapter::new(
            db_handle,
            sink.clone(),
            Arc::new(klights_controllers::side_effects::SideEffectRegistry::new()),
            klights_controllers::side_effects::SideEffectMetrics::new(),
        );
        let owner = klights_cluster_core::Resource {
            id: 1,
            api_version: "apps/v1".to_string(),
            kind: "Deployment".to_string(),
            namespace: Some("default".to_string()),
            name: "owner".to_string(),
            uid: "owner-uid".to_string(),
            resource_version: 7,
            data: Arc::new(serde_json::json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {
                    "name": "owner",
                    "namespace": "default",
                    "uid": "owner-uid",
                    "resourceVersion": "7"
                }
            })),
        };

        adapter
            .run_finalized_effects(FinalizerEffectsRequest { resource: owner })
            .await
            .expect("finalized effects should complete");

        {
            let requests = sink
                .requests
                .lock()
                .expect("Pod delete request lock poisoned");
            assert_eq!(requests.len(), 1);
            assert_eq!(requests[0].uid, "child-uid");
        }
        assert!(
            db.get_resource("v1", "Pod", Some("default"), "child")
                .await
                .expect("read bound Pod child")
                .is_some(),
            "root finalizer effects must not hard-delete a bound Pod row"
        );
    }

    #[test]
    fn finalizer_target_rejects_pod_before_root_adapter_dispatch() {
        assert!(matches!(
            FinalizerResourceTarget::try_new("v1", "Pod", Some("default"), "child"),
            Err(FinalizerLifecycleError::PodForbidden(_))
        ));
    }
}
