use std::sync::Arc;

use klights_reconcile_api::{
    FinalizerEffectsRequest, FinalizerLifecycleError, FinalizerLifecycleFuture,
    FinalizerLifecyclePort, FinalizerOrphanRequest, FinalizerResourceTarget,
    FinalizerTombstoneDeleteRequest, FinalizerUpdateRequest, GcPodDeleteSink,
};

use klights_cluster_core::StorageCommand;
use klights_leader_api::{LeaderResourceCommand, ResourceCommandRequest, ResourceCommandResult};

use klights_cluster_datastore::errors::DatastoreError;
use klights_cluster_store::{ClusterOwnershipRead, ClusterResourceRead, ResourceGetRequest};

pub(crate) struct DatastoreFinalizerLifecycleAdapter {
    lifecycle: CommandFinalizerLifecycleStore,
    pod_delete_sink: Arc<dyn GcPodDeleteSink>,
    side_effects: Arc<klights_controllers::side_effects::SideEffectRegistry>,
    metrics: Arc<klights_controllers::side_effects::SideEffectMetrics>,
    non_pod_finalization:
        crate::bootstrap::controller_adapters::gc_delete_adapter::GcNonPodFinalizationAdapter,
    coordination: Arc<dyn klights_reconcile_api::GcForegroundDeleteCoordination>,
}

impl DatastoreFinalizerLifecycleAdapter {
    #[cfg(test)]
    pub(crate) fn new_for_test_with_coordination(
        resource_reads: Arc<dyn ClusterResourceRead>,
        ownership_reads: Arc<dyn ClusterOwnershipRead>,
        resource_commands: Arc<dyn LeaderResourceCommand>,
        pod_delete_sink: Arc<dyn GcPodDeleteSink>,
        side_effects: Arc<klights_controllers::side_effects::SideEffectRegistry>,
        metrics: Arc<klights_controllers::side_effects::SideEffectMetrics>,
        coordination: Arc<dyn klights_reconcile_api::GcForegroundDeleteCoordination>,
    ) -> Arc<Self> {
        Arc::new(Self {
            non_pod_finalization: crate::bootstrap::controller_adapters::gc_delete_adapter::GcNonPodFinalizationAdapter::new_with_commands(
                resource_reads.clone(),
                ownership_reads.clone(),
                resource_commands.clone(),
            ),
            lifecycle: CommandFinalizerLifecycleStore::new(
                resource_reads,
                ownership_reads,
                resource_commands,
            ),
            pod_delete_sink,
            side_effects,
            metrics,
            coordination,
        })
    }
    pub(crate) fn new_with_coordination(
        resource_reads: Arc<dyn ClusterResourceRead>,
        ownership_reads: Arc<dyn ClusterOwnershipRead>,
        resource_commands: Arc<dyn LeaderResourceCommand>,
        pod_delete_sink: Arc<dyn GcPodDeleteSink>,
        side_effects: Arc<klights_controllers::side_effects::SideEffectRegistry>,
        metrics: Arc<klights_controllers::side_effects::SideEffectMetrics>,
        coordination: Arc<dyn klights_reconcile_api::GcForegroundDeleteCoordination>,
    ) -> Arc<Self> {
        Arc::new(Self {
            non_pod_finalization: crate::bootstrap::controller_adapters::gc_delete_adapter::GcNonPodFinalizationAdapter::new_with_commands(
                resource_reads.clone(), ownership_reads.clone(),
                resource_commands.clone(),
            ),
            lifecycle: CommandFinalizerLifecycleStore::new(
                resource_reads, ownership_reads,
                resource_commands,
            ),
            pod_delete_sink,
            side_effects,
            metrics,
            coordination,
        })
    }
}

#[derive(Clone)]
pub(crate) struct CommandFinalizerLifecycleStore {
    resource_reads: Arc<dyn ClusterResourceRead>,
    commands: Arc<dyn LeaderResourceCommand>,
    gc: Arc<
        crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerLeaderPort,
    >,
}

impl CommandFinalizerLifecycleStore {
    pub(crate) fn new(
        resource_reads: Arc<dyn ClusterResourceRead>,
        ownership_reads: Arc<dyn ClusterOwnershipRead>,
        commands: Arc<dyn LeaderResourceCommand>,
    ) -> Self {
        Self {
            gc: Arc::new(crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerLeaderPort::new_with_commands(
                resource_reads.clone(), ownership_reads,
                commands.clone(),
            )),
            resource_reads,
            commands,
        }
    }

    async fn submit(
        &self,
        command: StorageCommand,
    ) -> Result<klights_cluster_core::Resource, FinalizerLifecycleError> {
        let request = ResourceCommandRequest::try_new(command).map_err(command_lifecycle_error)?;
        match self
            .commands
            .submit_resource_command(request)
            .await
            .map_err(command_lifecycle_error)?
        {
            ResourceCommandResult::Resource(resource) => Ok(resource),
            ResourceCommandResult::Ack { .. } => Err(FinalizerLifecycleError::Internal(
                "finalizer mutation returned no resource".to_string(),
            )),
        }
    }
}

fn command_lifecycle_error(
    error: klights_leader_api::ResourceCommandError,
) -> FinalizerLifecycleError {
    let message = error.to_string();
    match error {
        klights_leader_api::ResourceCommandError::NotFound { .. } => {
            FinalizerLifecycleError::NotFound(message)
        }
        klights_leader_api::ResourceCommandError::AlreadyExists { .. }
        | klights_leader_api::ResourceCommandError::Conflict { .. } => {
            FinalizerLifecycleError::Conflict(message)
        }
        _ => FinalizerLifecycleError::Internal(message),
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
        Box::pin(async move { self.lifecycle.get_resource(target).await })
    }

    fn update_resource(
        &self,
        request: FinalizerUpdateRequest,
    ) -> FinalizerLifecycleFuture<'_, klights_cluster_core::Resource> {
        Box::pin(async move { self.lifecycle.update_resource(request).await })
    }

    fn delete_with_tombstone(
        &self,
        request: FinalizerTombstoneDeleteRequest,
    ) -> FinalizerLifecycleFuture<'_, klights_cluster_core::Resource> {
        Box::pin(async move { self.lifecycle.delete_with_tombstone(request).await })
    }

    fn orphan_children(&self, request: FinalizerOrphanRequest) -> FinalizerLifecycleFuture<'_, ()> {
        Box::pin(async move { self.lifecycle.orphan_children(request).await })
    }

    fn run_finalized_effects(
        &self,
        request: FinalizerEffectsRequest,
    ) -> FinalizerLifecycleFuture<'_, ()> {
        Box::pin(async move {
            let resource = request.resource;
            if let Err(error) = klights_controllers::gc::cascade_delete_with_uid(
                self.lifecycle.gc.as_ref(),
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

impl FinalizerLifecyclePort for CommandFinalizerLifecycleStore {
    fn get_resource(
        &self,
        target: FinalizerResourceTarget,
    ) -> FinalizerLifecycleFuture<'_, Option<klights_cluster_core::Resource>> {
        Box::pin(async move {
            let (api_version, kind, namespace, name) = target_parts(&target);
            self.resource_reads
                .get_resource(ResourceGetRequest::new(
                    api_version,
                    kind,
                    namespace.map(ToOwned::to_owned),
                    name,
                ))
                .await
                .map_err(|error| lifecycle_error(error.into()))
        })
    }

    fn update_resource(
        &self,
        request: FinalizerUpdateRequest,
    ) -> FinalizerLifecycleFuture<'_, klights_cluster_core::Resource> {
        Box::pin(async move {
            let (api_version, kind, namespace, name) = target_parts(&request.target);
            let expected_rv = request.preconditions.resource_version.unwrap_or_default();
            self.submit(StorageCommand::UpdateResource {
                api_version: api_version.to_string(),
                kind: kind.to_string(),
                namespace: namespace.map(str::to_string),
                name: name.to_string(),
                data: request.data,
                expected_rv,
                preconditions: request.preconditions,
                preserve_status: false,
            })
            .await
        })
    }

    fn delete_with_tombstone(
        &self,
        request: FinalizerTombstoneDeleteRequest,
    ) -> FinalizerLifecycleFuture<'_, klights_cluster_core::Resource> {
        Box::pin(async move {
            let (api_version, kind, namespace, name) = target_parts(&request.target);
            self.submit(StorageCommand::DeleteResourceWithTombstone {
                api_version: api_version.to_string(),
                kind: kind.to_string(),
                namespace: namespace.map(str::to_string),
                name: name.to_string(),
                preconditions: request.preconditions,
                grace_seconds: request.grace_seconds,
            })
            .await
        })
    }

    fn orphan_children(&self, request: FinalizerOrphanRequest) -> FinalizerLifecycleFuture<'_, ()> {
        Box::pin(async move {
            klights_controllers::gc::orphan_children(
                self.gc.as_ref(),
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
                "command finalizer store has no post-delete effects".to_string(),
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
        let db = crate::bootstrap::cluster_store::selector::canonical_sqlite_fixture()
            .await
            .unwrap();
        let canonical = db.clone();
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
        let resource_reads = db.focused_read_store();
        let authority =
            crate::bootstrap::composition_adapters::authority_adapter::always_leader_authority();
        let query = crate::bootstrap::composition_adapters::resource_query_adapter::DatastoreResourceQueryAdapter::new_focused_for_test(
            resource_reads.clone(), authority.clone());
        let commands = Arc::new(
            klights_replication::leader_api::EmbeddedLeaderResourceCommand::new(
                Arc::new(
                    crate::bootstrap::outbox_apply_adapter::BackendProposalFixture::new(
                        Arc::new(canonical.clone()),
                        Arc::new(canonical.clone()),
                        canonical.focused_read_store(),
                    ),
                ),
                query,
                authority,
            ),
        );
        let adapter = DatastoreFinalizerLifecycleAdapter::new_for_test_with_coordination(
            resource_reads.clone(),
            resource_reads,
            commands,
            sink.clone(),
            Arc::new(klights_controllers::side_effects::SideEffectRegistry::new()),
            klights_controllers::side_effects::SideEffectMetrics::new(),
            Arc::new(klights_controllers::ControllerCoordination::new()),
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
