use std::sync::Arc;

use crate::runtime::{PodDeletionFinalizeResult, PodRuntimeKey};
use klights_leader_api::NodeOutbox;
use klights_pod_api::{
    BoundPodFinalization, BoundPodFinalizationOutcome, BoundPodFinalizationRequest, PodGetRequest,
    PodQuery,
};
use klights_reconcile_api::{
    GcPodDeleteRequest, GcPodDeleteSink, NamespaceTerminationRequest, NamespaceTerminationSink,
    PodGcReconcileSink, PodPdbReconcileSink,
};
use klights_types::PodIdentity;

fn pod_is_node_lost_terminal(pod: &serde_json::Value) -> bool {
    pod.pointer("/status/phase")
        .and_then(|value| value.as_str())
        == Some("Failed")
        && pod
            .pointer("/status/reason")
            .and_then(|value| value.as_str())
            == Some("NodeLost")
}

/// Actor-owned Pod API-object deletion finalizer port.
/// The production implementation is the only code path allowed to
/// hard-delete a `v1/Pod` datastore row after actor cleanup completes.
#[async_trait::async_trait]
pub trait PodDeletionFinalizer: Send + Sync {
    /// Finalize pod deletion after actor-side runtime cleanup completes.
    async fn finalize_after_actor_cleanup(
        &self,
        key: &PodRuntimeKey,
    ) -> anyhow::Result<PodDeletionFinalizeResult>;
}

/// Production actor-owned Pod deletion finalizer.
///
/// Moves the body of `PodRepository::finalize_pod_deletion_after_actor_cleanup`
/// behind the `PodDeletionFinalizer` trait so the actor-owned hard-delete
/// invariant can be source-guarded.
pub struct RealPodDeletionFinalizer {
    pod_query: Arc<dyn PodQuery>,
    gc_pod_delete_sink: Arc<dyn GcPodDeleteSink>,
    gc_reconcile: Arc<dyn PodGcReconcileSink>,
    pdb_reconcile: Arc<dyn PodPdbReconcileSink>,
    namespace_termination: Arc<dyn NamespaceTerminationSink>,
    cluster_api: Option<Arc<dyn klights_leader_api::LeaderResourceQuery>>,
    outbox: Option<Arc<dyn NodeOutbox>>,
    bound_pod_finalization: Arc<dyn BoundPodFinalization>,
    mutation_reconcile: Arc<dyn klights_reconcile_api::PodMutationReconcileSink>,
    metrics: Arc<dyn klights_reconcile_api::ReconcileFailureMetrics>,
    supervisor: Arc<klights_supervisor::TaskSupervisor>,
}

pub struct RealPodDeletionFinalizerDependencies {
    pub pod_query: Arc<dyn PodQuery>,
    pub gc_pod_delete_sink: Arc<dyn GcPodDeleteSink>,
    pub gc_reconcile: Arc<dyn PodGcReconcileSink>,
    pub pdb_reconcile: Arc<dyn PodPdbReconcileSink>,
    pub namespace_termination: Arc<dyn NamespaceTerminationSink>,
    pub cluster_api: Option<Arc<dyn klights_leader_api::LeaderResourceQuery>>,
    pub outbox: Option<Arc<dyn NodeOutbox>>,
    pub bound_pod_finalization: Arc<dyn BoundPodFinalization>,
    pub mutation_reconcile: Arc<dyn klights_reconcile_api::PodMutationReconcileSink>,
    pub metrics: Arc<dyn klights_reconcile_api::ReconcileFailureMetrics>,
    pub supervisor: Arc<klights_supervisor::TaskSupervisor>,
}

impl RealPodDeletionFinalizer {
    fn new_with_dependencies(dependencies: RealPodDeletionFinalizerDependencies) -> Self {
        let RealPodDeletionFinalizerDependencies {
            pod_query,
            gc_pod_delete_sink,
            gc_reconcile,
            pdb_reconcile,
            namespace_termination,
            cluster_api,
            outbox,
            bound_pod_finalization,
            mutation_reconcile,
            metrics,
            supervisor,
        } = dependencies;
        Self {
            pod_query,
            gc_pod_delete_sink,
            gc_reconcile,
            pdb_reconcile,
            namespace_termination,
            cluster_api,
            outbox,
            bound_pod_finalization,
            mutation_reconcile,
            metrics,
            supervisor,
        }
    }

    async fn spawn_post_write_maintenance(&self, namespace: &str) {
        let pdb_reconcile = self.pdb_reconcile.clone();
        let namespace_termination = self.namespace_termination.clone();
        let ns = namespace.to_string();
        drop(self.supervisor.spawn_async(
            klights_supervisor::TaskCategory::Background,
            format!("post_write_maintenance/{ns}"),
            async move {
                if let Err(err) = pdb_reconcile.reconcile_namespace_pdbs(ns.clone()).await {
                    tracing::warn!(
                        namespace = %ns,
                        error = ?err,
                        "post-write PDB reconcile failed"
                    );
                }
                if let Err(err) = namespace_termination
                    .reconcile_namespace_termination(NamespaceTerminationRequest {
                        namespace: ns.clone(),
                        expected_uid: None,
                    })
                    .await
                {
                    tracing::warn!(
                        namespace = %ns,
                        error = ?err,
                        "post-write namespace termination reconcile failed"
                    );
                }
            },
        ));
    }

    async fn delete_status_checkpoint_after_finalization(&self, uid: &str) {
        let Some(outbox) = &self.outbox else {
            return;
        };
        if let Err(err) = outbox.delete_pod_status_checkpoint(uid).await {
            tracing::warn!(
                pod_uid = %uid,
                error = %err,
                "actor-owned Pod finalization failed to delete node-local status checkpoint"
            );
        }
    }
}

pub fn compose_real_pod_deletion_finalizer(
    dependencies: RealPodDeletionFinalizerDependencies,
) -> Arc<dyn PodDeletionFinalizer> {
    Arc::new(RealPodDeletionFinalizer::new_with_dependencies(
        dependencies,
    ))
}

#[async_trait::async_trait]
impl PodDeletionFinalizer for RealPodDeletionFinalizer {
    async fn finalize_after_actor_cleanup(
        &self,
        key: &PodRuntimeKey,
    ) -> anyhow::Result<PodDeletionFinalizeResult> {
        let ns = &key.namespace;
        let name = &key.name;
        let uid = &key.uid;

        let live = if let Some(cluster_api) = &self.cluster_api {
            cluster_api
                .get_resource(klights_leader_api::pod_get_request(
                    ns,
                    name,
                    klights_leader_api::ResourceQueryConsistency::LeaderFresh,
                )?)
                .await?
        } else {
            self.pod_query
                .get_pod(PodGetRequest::try_by_name(ns, name)?)
                .await?
        };
        let Some(live) = live else {
            self.delete_status_checkpoint_after_finalization(uid).await;
            return Ok(PodDeletionFinalizeResult::DeletedOrAlreadyGone);
        };

        if live.uid != *uid {
            tracing::warn!(
                namespace = %ns,
                pod = %name,
                requested_uid = %uid,
                live_uid = %live.uid,
                "actor-owned Pod finalization ignored stale UID because a replacement Pod exists"
            );
            self.delete_status_checkpoint_after_finalization(uid).await;
            return Ok(PodDeletionFinalizeResult::DeletedOrAlreadyGone);
        }

        if live.data.pointer("/metadata/deletionTimestamp").is_none()
            && !pod_is_node_lost_terminal(live.data.as_ref())
        {
            tracing::warn!(
                namespace = %ns,
                pod = %name,
                uid = %uid,
                "actor-owned Pod finalization reissued UID-bound delete mark for non-terminating Pod"
            );
            match self
                .gc_pod_delete_sink
                .request_gc_pod_delete(GcPodDeleteRequest::new(PodIdentity::new(ns, name, uid)))
                .await
            {
                Ok(()) => return Ok(PodDeletionFinalizeResult::FinalizersPending),
                Err(err) if err.is_gone_or_identity_changed() => {
                    tracing::debug!(
                        namespace = %ns,
                        pod = %name,
                        uid = %uid,
                        error = %err,
                        "actor-owned Pod finalization delete-mark retry found Pod gone or UID changed"
                    );
                    return Ok(PodDeletionFinalizeResult::DeletedOrAlreadyGone);
                }
                Err(err) => return Err(err.into()),
            }
        }

        if live
            .data
            .pointer("/metadata/finalizers")
            .and_then(|finalizers| finalizers.as_array())
            .is_some_and(|finalizers| !finalizers.is_empty())
        {
            return Ok(PodDeletionFinalizeResult::FinalizersPending);
        }

        let finalization_request =
            BoundPodFinalizationRequest::try_new(PodIdentity::new(ns, name, uid))
                .map_err(anyhow::Error::new)?;
        let finalization_outcome = self
            .bound_pod_finalization
            .finalize_bound_pod(finalization_request)
            .await
            .map_err(anyhow::Error::new)?;
        if matches!(finalization_outcome, BoundPodFinalizationOutcome::Accepted) {
            return Ok(PodDeletionFinalizeResult::Queued);
        }
        if matches!(
            finalization_outcome,
            BoundPodFinalizationOutcome::FinalizersPending | BoundPodFinalizationOutcome::Retry
        ) {
            return Ok(PodDeletionFinalizeResult::FinalizersPending);
        }
        self.delete_status_checkpoint_after_finalization(uid).await;
        if !matches!(finalization_outcome, BoundPodFinalizationOutcome::Removed) {
            return Ok(PodDeletionFinalizeResult::DeletedOrAlreadyGone);
        }

        let deleted_data = live.data.clone();

        if let Err(err) = self
            .gc_reconcile
            .finalize_foreground_owners(live.clone(), self.gc_pod_delete_sink.as_ref())
            .await
        {
            self.metrics.record_cascade_delete_failure();
            tracing::error!(
                namespace = %ns,
                pod = %name,
                uid = %uid,
                error = %err,
                "actor-owned Pod finalization foreground-owner check failed"
            );
        }

        let deleted_resource =
            klights_cluster_core::Resource::from_data_lossy(deleted_data.clone());
        if let Err(err) = self
            .mutation_reconcile
            .reconcile_pod_mutation(
                klights_reconcile_api::PodMutationReconcileRequest::ServicesAfterDelete {
                    deleted: deleted_resource.clone(),
                },
            )
            .await
        {
            tracing::debug!(
                target: "klights::kubelet::pod_repository",
                error = %err,
                pod = %name,
                "failed to enqueue Service reconcile after actor-owned pod finalization"
            );
        }

        let _ = self
            .mutation_reconcile
            .reconcile_pod_mutation(
                klights_reconcile_api::PodMutationReconcileRequest::RunHooks {
                    pod: deleted_resource,
                    named_hook: None,
                    context: "pod_actor_finalize_delete",
                },
            )
            .await;
        self.spawn_post_write_maintenance(ns).await;
        Ok(PodDeletionFinalizeResult::DeletedOrAlreadyGone)
    }
}

#[cfg(test)]
mod policy_tests {
    use std::sync::{Arc, Mutex};

    use klights_cluster_core::Resource;
    use klights_pod_api::{
        BoundPodFinalizationFuture, PodListRequest, PodRepositoryError, PodRepositoryFuture,
    };
    use klights_reconcile_api::{
        GcPodDeleteError, GcPodDeleteFuture, PodMutationReconcileRequest, ReconcileSinkFuture,
    };

    use super::*;

    struct RecordingQuery {
        events: Arc<Mutex<Vec<&'static str>>>,
        live: Resource,
    }

    impl PodQuery for RecordingQuery {
        fn get_pod(&self, _request: PodGetRequest) -> PodRepositoryFuture<'_, Option<Resource>> {
            self.events.lock().unwrap().push("fresh_uid_check");
            let live = self.live.clone();
            Box::pin(async move { Ok(Some(live)) })
        }

        fn list_pods(
            &self,
            _request: PodListRequest,
        ) -> PodRepositoryFuture<'_, klights_pod_api::PodListResult> {
            Box::pin(async { Err(PodRepositoryError::unavailable("unused list query")) })
        }

        fn list_pods_by_owner_uid(
            &self,
            _request: klights_pod_api::PodOwnerListRequest,
        ) -> PodRepositoryFuture<'_, Vec<Resource>> {
            Box::pin(async { Err(PodRepositoryError::unavailable("unused owner query")) })
        }
    }

    struct RecordingBoundFinalization {
        events: Arc<Mutex<Vec<&'static str>>>,
    }

    impl BoundPodFinalization for RecordingBoundFinalization {
        fn finalize_bound_pod(
            &self,
            _request: BoundPodFinalizationRequest,
        ) -> BoundPodFinalizationFuture<'_> {
            self.events
                .lock()
                .unwrap()
                .push("opaque_bound_finalization");
            Box::pin(async { Ok(BoundPodFinalizationOutcome::Accepted) })
        }
    }

    struct NoopGcDelete;

    impl GcPodDeleteSink for NoopGcDelete {
        fn request_gc_pod_delete(&self, _request: GcPodDeleteRequest) -> GcPodDeleteFuture<'_> {
            Box::pin(async {
                Err(GcPodDeleteError::unavailable(
                    "unexpected non-terminating Pod",
                ))
            })
        }
    }

    struct NoopGcReconcile;

    impl PodGcReconcileSink for NoopGcReconcile {
        fn reconcile_owner_references<'a>(
            &'a self,
            _pod: Resource,
            _pod_delete_sink: &'a dyn GcPodDeleteSink,
        ) -> ReconcileSinkFuture<'a> {
            Box::pin(async { Ok(()) })
        }

        fn cascade_delete_dependents<'a>(
            &'a self,
            _owner: PodIdentity,
            _pod_delete_sink: &'a dyn GcPodDeleteSink,
        ) -> ReconcileSinkFuture<'a> {
            Box::pin(async { Ok(()) })
        }

        fn finalize_foreground_owners<'a>(
            &'a self,
            _deleted_dependent: Resource,
            _pod_delete_sink: &'a dyn GcPodDeleteSink,
        ) -> ReconcileSinkFuture<'a> {
            Box::pin(async { Ok(()) })
        }
    }

    struct NoopPdbReconcile;

    impl PodPdbReconcileSink for NoopPdbReconcile {
        fn reconcile_namespace_pdbs(&self, _namespace: String) -> ReconcileSinkFuture<'_> {
            Box::pin(async { Ok(()) })
        }
    }

    struct NoopNamespaceTermination;

    impl NamespaceTerminationSink for NoopNamespaceTermination {
        fn reconcile_namespace_termination(
            &self,
            _request: NamespaceTerminationRequest,
        ) -> klights_reconcile_api::NamespaceTerminationFuture<'_> {
            Box::pin(async { Ok(klights_reconcile_api::NamespaceTerminationOutcome::Finalized) })
        }
    }

    struct NoopMutationReconcile;

    impl klights_reconcile_api::PodMutationReconcileSink for NoopMutationReconcile {
        fn reconcile_pod_mutation(
            &self,
            _request: PodMutationReconcileRequest,
        ) -> ReconcileSinkFuture<'_> {
            Box::pin(async { Ok(()) })
        }
    }

    struct NoopMetrics;

    impl klights_reconcile_api::ReconcileFailureMetrics for NoopMetrics {
        fn record_cascade_delete_failure(&self) {}

        fn record_namespace_delete_failure(&self) {}
    }

    fn terminating_pod() -> Resource {
        Resource::from_data_lossy(Arc::new(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "same-name",
                "uid": "uid-old",
                "deletionTimestamp": "2026-07-31T00:00:00Z"
            },
            "spec": {
                "nodeName": "worker-a",
                "containers": [{"name": "app", "image": "example.invalid/app"}]
            }
        })))
    }

    #[tokio::test]
    async fn fresh_uid_termination_and_finalizer_check_precedes_opaque_finalization() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let finalizer = compose_real_pod_deletion_finalizer(RealPodDeletionFinalizerDependencies {
            pod_query: Arc::new(RecordingQuery {
                events: events.clone(),
                live: terminating_pod(),
            }),
            gc_pod_delete_sink: Arc::new(NoopGcDelete),
            gc_reconcile: Arc::new(NoopGcReconcile),
            pdb_reconcile: Arc::new(NoopPdbReconcile),
            namespace_termination: Arc::new(NoopNamespaceTermination),
            cluster_api: None,
            outbox: None,
            bound_pod_finalization: Arc::new(RecordingBoundFinalization {
                events: events.clone(),
            }),
            mutation_reconcile: Arc::new(NoopMutationReconcile),
            metrics: Arc::new(NoopMetrics),
            supervisor: Arc::new(klights_supervisor::TaskSupervisor::new(
                klights_supervisor::TaskCategoryConfig::default(),
            )),
        });

        let result = finalizer
            .finalize_after_actor_cleanup(&PodRuntimeKey::new("default", "same-name", "uid-old"))
            .await
            .expect("eligible bound Pod finalization");

        assert_eq!(result, PodDeletionFinalizeResult::Queued);
        assert_eq!(
            *events.lock().unwrap(),
            ["fresh_uid_check", "opaque_bound_finalization"],
            "fresh UID/termination/finalizer validation must precede the opaque deleting capability"
        );
    }
}
