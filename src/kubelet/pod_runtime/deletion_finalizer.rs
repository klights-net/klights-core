use std::sync::Arc;

use crate::kubelet::outbox::Outbox;
use crate::kubelet::pod_repository::store::PodStore;
use crate::kubelet::pod_runtime::service::{PodDeletionFinalizeResult, PodRuntimeKey};
use klights_pod_api::{
    BoundPodFinalization, BoundPodFinalizationOutcome, BoundPodFinalizationRequest,
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

#[cfg(test)]
struct TestNamespaceTerminationSink;

#[cfg(test)]
impl NamespaceTerminationSink for TestNamespaceTerminationSink {
    fn reconcile_namespace_termination(
        &self,
        _request: NamespaceTerminationRequest,
    ) -> klights_reconcile_api::NamespaceTerminationFuture<'_> {
        Box::pin(async { Ok(klights_reconcile_api::NamespaceTerminationOutcome::Finalized) })
    }
}

#[cfg(test)]
struct TestPodGcReconcileSink;

#[cfg(test)]
impl PodGcReconcileSink for TestPodGcReconcileSink {
    fn reconcile_owner_references<'a>(
        &'a self,
        _pod: klights_cluster_core::Resource,
        _pod_delete_sink: &'a dyn GcPodDeleteSink,
    ) -> klights_reconcile_api::ReconcileSinkFuture<'a> {
        Box::pin(async { Ok(()) })
    }

    fn cascade_delete_dependents<'a>(
        &'a self,
        _owner: PodIdentity,
        _pod_delete_sink: &'a dyn GcPodDeleteSink,
    ) -> klights_reconcile_api::ReconcileSinkFuture<'a> {
        Box::pin(async { Ok(()) })
    }

    fn finalize_foreground_owners<'a>(
        &'a self,
        _deleted_dependent: klights_cluster_core::Resource,
        _pod_delete_sink: &'a dyn GcPodDeleteSink,
    ) -> klights_reconcile_api::ReconcileSinkFuture<'a> {
        Box::pin(async { Ok(()) })
    }
}

#[cfg(test)]
struct TestPodPdbReconcileSink;

#[cfg(test)]
impl PodPdbReconcileSink for TestPodPdbReconcileSink {
    fn reconcile_namespace_pdbs(
        &self,
        _namespace: String,
    ) -> klights_reconcile_api::ReconcileSinkFuture<'_> {
        Box::pin(async { Ok(()) })
    }
}

/// Production actor-owned Pod deletion finalizer.
///
/// Moves the body of `PodRepository::finalize_pod_deletion_after_actor_cleanup`
/// behind the `PodDeletionFinalizer` trait so the actor-owned hard-delete
/// invariant can be source-guarded.
pub(crate) struct RealPodDeletionFinalizer {
    pub(crate) store: Arc<PodStore>,
    gc_pod_delete_sink: Arc<dyn GcPodDeleteSink>,
    actor_delete_mark: Arc<dyn GcPodDeleteSink>,
    gc_reconcile: Arc<dyn PodGcReconcileSink>,
    pdb_reconcile: Arc<dyn PodPdbReconcileSink>,
    namespace_termination: Arc<dyn NamespaceTerminationSink>,
    cluster_api: Option<Arc<dyn klights_leader_api::LeaderResourceQuery>>,
    outbox: Option<Arc<Outbox>>,
    bound_pod_finalization: Arc<dyn BoundPodFinalization>,
    mutation_reconcile: Arc<dyn klights_reconcile_api::PodMutationReconcileSink>,
    metrics: Arc<dyn klights_reconcile_api::ReconcileFailureMetrics>,
    supervisor: Arc<klights_supervisor::TaskSupervisor>,
}

pub(crate) struct RealPodDeletionFinalizerDependencies {
    pub(crate) store: Arc<PodStore>,
    pub(crate) gc_pod_delete_sink: Arc<dyn GcPodDeleteSink>,
    pub(crate) actor_delete_mark: Arc<dyn GcPodDeleteSink>,
    pub(crate) gc_reconcile: Arc<dyn PodGcReconcileSink>,
    pub(crate) pdb_reconcile: Arc<dyn PodPdbReconcileSink>,
    pub(crate) namespace_termination: Arc<dyn NamespaceTerminationSink>,
    pub(crate) cluster_api: Option<Arc<dyn klights_leader_api::LeaderResourceQuery>>,
    pub(crate) outbox: Option<Arc<Outbox>>,
    pub(crate) bound_pod_finalization: Arc<dyn BoundPodFinalization>,
    pub(crate) mutation_reconcile: Arc<dyn klights_reconcile_api::PodMutationReconcileSink>,
    pub(crate) metrics: Arc<dyn klights_reconcile_api::ReconcileFailureMetrics>,
    pub(crate) supervisor: Arc<klights_supervisor::TaskSupervisor>,
}

impl RealPodDeletionFinalizer {
    fn new_with_dependencies(dependencies: RealPodDeletionFinalizerDependencies) -> Self {
        let RealPodDeletionFinalizerDependencies {
            store,
            gc_pod_delete_sink,
            actor_delete_mark,
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
            store,
            gc_pod_delete_sink,
            actor_delete_mark,
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

    #[cfg(test)]
    pub(crate) fn new(
        store: Arc<PodStore>,
        gc_pod_delete_sink: Arc<dyn GcPodDeleteSink>,
        cluster_api: Option<Arc<dyn klights_leader_api::LeaderResourceQuery>>,
        outbox: Option<Arc<Outbox>>,
        side_effects: Arc<crate::side_effects::SideEffectRegistry>,
        metrics: Arc<crate::side_effects::SideEffectMetrics>,
        supervisor: Arc<klights_supervisor::TaskSupervisor>,
    ) -> Self {
        let bound_pod_adapter =
            crate::bound_pod_finalization_adapter::RootBoundPodFinalization::new(
                store.clone(),
                cluster_api.clone(),
                outbox.clone(),
            );
        let bound_pod_finalization = bound_pod_adapter.clone();
        let actor_delete_mark = if outbox.is_some() {
            bound_pod_adapter
        } else {
            gc_pod_delete_sink.clone()
        };
        let mutation_reconcile = Arc::new(crate::pod_reconcile_adapter::PodReconcileAdapter::new(
            store.db().clone(),
            side_effects.controller_dispatcher_slot(),
            metrics.clone(),
            side_effects,
            store.clone(),
        ));
        Self::new_with_dependencies(RealPodDeletionFinalizerDependencies {
            store,
            gc_pod_delete_sink,
            actor_delete_mark,
            gc_reconcile: Arc::new(TestPodGcReconcileSink),
            pdb_reconcile: Arc::new(TestPodPdbReconcileSink),
            namespace_termination: Arc::new(TestNamespaceTerminationSink),
            cluster_api,
            outbox,
            bound_pod_finalization,
            mutation_reconcile,
            metrics,
            supervisor,
        })
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

pub(crate) fn compose_real_pod_deletion_finalizer(
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
            self.store.get(ns, name).await?
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
                .actor_delete_mark
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
mod bound_finalization_tests {
    use super::*;
    use klights_pod_api::{
        BoundPodFinalization, BoundPodFinalizationOutcome, BoundPodFinalizationRequest,
    };
    use klights_types::PodIdentity;

    fn terminating_pod(name: &str, uid: &str) -> serde_json::Value {
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": name,
                "uid": uid,
                "deletionTimestamp": "2026-07-20T00:00:00Z",
                "deletionGracePeriodSeconds": 0
            },
            "spec": {
                "nodeName": "worker-a",
                "containers": [{"name": "app", "image": "nginx"}]
            }
        })
    }

    #[tokio::test]
    async fn root_bound_finalization_adapter_removes_only_the_requested_uid() {
        let (_datastore, db) = crate::datastore::test_support::in_memory_with_handle().await;
        let store = Arc::new(PodStore::new(db.clone()));
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "same-name",
            terminating_pod("same-name", "uid-current"),
        )
        .await
        .expect("create current Pod");
        let capability = crate::bound_pod_finalization_adapter::RootBoundPodFinalization::new(
            store.clone(),
            None,
            None,
        );

        let stale = capability
            .finalize_bound_pod(
                BoundPodFinalizationRequest::try_new(PodIdentity::new(
                    "default",
                    "same-name",
                    "uid-stale",
                ))
                .expect("stale UID request"),
            )
            .await
            .expect("stale UID is a terminal no-op");
        assert_eq!(stale, BoundPodFinalizationOutcome::IdentityChanged);
        assert_eq!(
            store
                .get("default", "same-name")
                .await
                .expect("read current Pod")
                .expect("current Pod remains")
                .uid,
            "uid-current"
        );

        let removed = capability
            .finalize_bound_pod(
                BoundPodFinalizationRequest::try_new(PodIdentity::new(
                    "default",
                    "same-name",
                    "uid-current",
                ))
                .expect("current UID request"),
            )
            .await
            .expect("current UID finalization");
        assert_eq!(removed, BoundPodFinalizationOutcome::Removed);
        assert!(
            store
                .get("default", "same-name")
                .await
                .expect("read after finalization")
                .is_none()
        );
    }

    #[tokio::test]
    async fn root_bound_finalization_adapter_revalidates_actor_delete_eligibility() {
        let cases = [
            (
                "unbound",
                {
                    let mut pod = terminating_pod("unbound", "uid-unbound");
                    pod["spec"]["nodeName"] = serde_json::json!("");
                    pod
                },
                BoundPodFinalizationOutcome::Retry,
            ),
            (
                "live",
                {
                    let mut pod = terminating_pod("live", "uid-live");
                    pod["metadata"]
                        .as_object_mut()
                        .expect("metadata object")
                        .remove("deletionTimestamp");
                    pod
                },
                BoundPodFinalizationOutcome::Retry,
            ),
            (
                "finalizer-held",
                {
                    let mut pod = terminating_pod("finalizer-held", "uid-finalizer-held");
                    pod["metadata"]["finalizers"] = serde_json::json!(["example.com/hold"]);
                    pod
                },
                BoundPodFinalizationOutcome::FinalizersPending,
            ),
        ];

        for (name, pod, expected) in cases {
            let (_datastore, db) = crate::datastore::test_support::in_memory_with_handle().await;
            let store = Arc::new(PodStore::new(db.clone()));
            db.create_resource("v1", "Pod", Some("default"), name, pod)
                .await
                .expect("create guarded Pod");
            let capability = crate::bound_pod_finalization_adapter::RootBoundPodFinalization::new(
                store.clone(),
                None,
                None,
            );

            let outcome = capability
                .finalize_bound_pod(
                    BoundPodFinalizationRequest::try_new(PodIdentity::new(
                        "default",
                        name,
                        &format!("uid-{name}"),
                    ))
                    .expect("valid bound finalization request"),
                )
                .await
                .expect("guarded disposition");
            assert_eq!(outcome, expected, "{name}");
            assert!(
                store
                    .get("default", name)
                    .await
                    .expect("read guarded Pod")
                    .is_some(),
                "{name} Pod must remain"
            );
        }
    }
}
