//! Root adapters for Pod mutation reconciliation.
//!
//! Kubelet Pod services depend only on the focused reconcile capabilities.
//! Concrete GC, datastore, and side-effect registry ownership stays here at
//! the composition root.

use klights_cluster_core::Resource;
use klights_reconcile_api::{
    GcPodDeleteSink, NamespaceBootstrapSink, NamespaceTerminationFuture,
    NamespaceTerminationOutcome, NamespaceTerminationRequest, NamespaceTerminationSink,
    PodEvictionAdmissionFuture, PodEvictionAdmissionRequest, PodEvictionAdmissionSink,
    PodGcReconcileSink, PodMutationReconcileRequest, PodMutationReconcileSink, PodPdbReconcileSink,
    PodServiceReconcileSink, PvcReconcileFuture, PvcReconcileOutcome, PvcReconcileSink,
    ReconcileSinkError, ReconcileSinkFuture,
};
use klights_types::PodIdentity;

use crate::datastore::DatastoreHandle;
use klights_controllers::side_effects::ControllerDispatcherSlot;

pub(crate) struct PodReconcileAdapter {
    db: DatastoreHandle,
    namespace_lifecycle: std::sync::Arc<dyn klights_reconcile_api::NamespaceLifecycleStore>,
    dispatcher: ControllerDispatcherSlot,
    metrics: std::sync::Arc<klights_controllers::side_effects::SideEffectMetrics>,
    side_effects: std::sync::Arc<klights_controllers::side_effects::SideEffectRegistry>,
    pod_reader: std::sync::Arc<dyn crate::kubelet::pod_repository::PodReader>,
    non_pod_finalization: crate::gc_delete_adapter::GcNonPodFinalizationAdapter,
    coordination: std::sync::Arc<dyn klights_reconcile_api::GcForegroundDeleteCoordination>,
}

impl PodReconcileAdapter {
    #[cfg(test)]
    pub(crate) fn new(
        db: DatastoreHandle,
        dispatcher: ControllerDispatcherSlot,
        metrics: std::sync::Arc<klights_controllers::side_effects::SideEffectMetrics>,
        side_effects: std::sync::Arc<klights_controllers::side_effects::SideEffectRegistry>,
        pod_reader: std::sync::Arc<dyn crate::kubelet::pod_repository::PodReader>,
    ) -> Self {
        Self::new_with_coordination(
            db,
            dispatcher,
            metrics,
            side_effects,
            pod_reader,
            std::sync::Arc::new(klights_controllers::ControllerCoordination::new()),
        )
    }

    pub(crate) fn new_with_coordination(
        db: DatastoreHandle,
        dispatcher: ControllerDispatcherSlot,
        metrics: std::sync::Arc<klights_controllers::side_effects::SideEffectMetrics>,
        side_effects: std::sync::Arc<klights_controllers::side_effects::SideEffectRegistry>,
        pod_reader: std::sync::Arc<dyn crate::kubelet::pod_repository::PodReader>,
        coordination: std::sync::Arc<dyn klights_reconcile_api::GcForegroundDeleteCoordination>,
    ) -> Self {
        Self {
            non_pod_finalization: crate::gc_delete_adapter::GcNonPodFinalizationAdapter::new(
                db.clone(),
            ),
            #[cfg(not(test))]
            namespace_lifecycle: crate::api_state_adapter::RootNamespaceTerminationStore::new(
                db.clone(),
            ),
            #[cfg(test)]
            namespace_lifecycle:
                crate::api_state_adapter_test_owner::RootNamespaceTerminationStore::new(db.clone()),
            db,
            dispatcher,
            metrics,
            side_effects,
            pod_reader,
            coordination,
        }
    }
}

impl PodMutationReconcileSink for PodReconcileAdapter {
    fn reconcile_pod_mutation(
        &self,
        request: PodMutationReconcileRequest,
    ) -> ReconcileSinkFuture<'_> {
        Box::pin(async move {
            match request {
                PodMutationReconcileRequest::RunHooks {
                    pod,
                    named_hook,
                    context,
                } => {
                    if let Some(hook_name) = named_hook {
                        crate::side_effect_registry_composition::run_named_hook_logged(
                            &self.side_effects,
                            &pod.data,
                            &self.metrics,
                            hook_name,
                            context,
                        )
                        .await;
                    } else {
                        crate::side_effect_registry_composition::run_hooks_logged(
                            &self.side_effects,
                            &pod.data,
                            &self.metrics,
                            context,
                        )
                        .await;
                    }
                }
                PodMutationReconcileRequest::ServicesAfterUpdate { previous, updated } => {
                    klights_controllers::side_effects::service_pod::enqueue_services_after_pod_update(
                        &previous.data,
                        &updated.data,
                        self.db.as_ref(),
                        &self.dispatcher,
                    )
                    .await
                    .map_err(|error| ReconcileSinkError::unavailable(error.to_string()))?;
                }
                PodMutationReconcileRequest::ServicesAfterDelete { deleted } => {
                    klights_controllers::side_effects::service_pod::enqueue_services_after_pod_delete(
                        &deleted.data,
                        self.db.as_ref(),
                        &self.dispatcher,
                    )
                    .await
                    .map_err(|error| ReconcileSinkError::unavailable(error.to_string()))?;
                }
                PodMutationReconcileRequest::StatusChanged { previous, updated } => {
                    klights_controllers::side_effects::service_pod::enqueue_services_after_pod_update(
                        &previous.data,
                        &updated.data,
                        self.db.as_ref(),
                        &self.dispatcher,
                    )
                    .await
                    .map_err(|error| ReconcileSinkError::unavailable(error.to_string()))?;
                    if let Some(dispatcher) = self.dispatcher.get() {
                        let namespace = updated
                            .data
                            .pointer("/metadata/namespace")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or("default");
                        dispatcher
                            .enqueue_reconcile_batch(
                                crate::side_effects::workload_pod::workload_owner_keys_for_pod(
                                    &updated.data,
                                    namespace,
                                ),
                            )
                            .await
                            .map_err(|error| ReconcileSinkError::unavailable(error.to_string()))?;
                        if pod_owner_reconcile_status_changed(&previous.data, &updated.data)
                            && let Some(replica_set) =
                                controller_owner_name(&updated.data, "apps/v1", "ReplicaSet")
                            && let Some(replica_set) = self
                                .db
                                .get_resource(
                                    "apps/v1",
                                    "ReplicaSet",
                                    Some(namespace),
                                    &replica_set,
                                )
                                .await
                                .map_err(|error| {
                                    ReconcileSinkError::unavailable(error.to_string())
                                })?
                            && let Some(deployment) =
                                controller_owner_name(&replica_set.data, "apps/v1", "Deployment")
                        {
                            dispatcher
                                .enqueue_reconcile_batch(vec![
                                    klights_reconcile_api::ReconcileKey::namespaced(
                                        "apps/v1",
                                        "Deployment",
                                        namespace,
                                        &deployment,
                                    ),
                                ])
                                .await
                                .map_err(|error| {
                                    ReconcileSinkError::unavailable(error.to_string())
                                })?;
                        }
                    }
                }
                PodMutationReconcileRequest::EnqueueJobOwner { pod } => {
                    let Some(namespace) = pod
                        .data
                        .pointer("/metadata/namespace")
                        .and_then(serde_json::Value::as_str)
                    else {
                        return Ok(());
                    };
                    let Some(job) = controller_owner_name(&pod.data, "batch/v1", "Job") else {
                        return Ok(());
                    };
                    if let Some(dispatcher) = self.dispatcher.get() {
                        dispatcher
                            .enqueue_reconcile_batch(vec![
                                klights_reconcile_api::ReconcileKey::namespaced(
                                    "batch/v1", "Job", namespace, &job,
                                ),
                            ])
                            .await
                            .map_err(|error| ReconcileSinkError::unavailable(error.to_string()))?;
                    }
                }
            }
            Ok(())
        })
    }
}

fn controller_owner_name(pod: &serde_json::Value, api_version: &str, kind: &str) -> Option<String> {
    pod.pointer("/metadata/ownerReferences")
        .and_then(serde_json::Value::as_array)?
        .iter()
        .find(|owner| {
            owner.get("apiVersion").and_then(serde_json::Value::as_str) == Some(api_version)
                && owner.get("kind").and_then(serde_json::Value::as_str) == Some(kind)
                && owner
                    .get("controller")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false)
        })
        .and_then(|owner| owner.get("name"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
}

fn pod_owner_reconcile_status_changed(
    previous: &serde_json::Value,
    updated: &serde_json::Value,
) -> bool {
    [
        "/status/phase",
        "/status/conditions",
        "/metadata/deletionTimestamp",
    ]
    .into_iter()
    .any(|pointer| previous.pointer(pointer) != updated.pointer(pointer))
}

impl NamespaceTerminationSink for PodReconcileAdapter {
    fn reconcile_namespace_termination(
        &self,
        request: NamespaceTerminationRequest,
    ) -> NamespaceTerminationFuture<'_> {
        Box::pin(async move {
            let outcome = match request.expected_uid {
                Some(uid) => crate::api::reconcile_namespace_termination_for_uid_with_outcome_at(
                    self.namespace_lifecycle.as_ref(),
                    &request.namespace,
                    &uid,
                    self.metrics.as_ref(),
                    klights_supervisor::SystemWallClock::now_utc(),
                )
                .await
                .map(|outcome| match outcome {
                    crate::api::NamespaceTerminationOutcome::Finalized => {
                        NamespaceTerminationOutcome::Finalized
                    }
                    crate::api::NamespaceTerminationOutcome::StillPending => {
                        NamespaceTerminationOutcome::StillPending
                    }
                }),
                None => crate::api::reconcile_namespace_termination_at(
                    self.namespace_lifecycle.as_ref(),
                    &request.namespace,
                    self.metrics.as_ref(),
                    klights_supervisor::SystemWallClock::now_utc(),
                )
                .await
                .map(|()| NamespaceTerminationOutcome::Finalized),
            };
            outcome.map_err(|error| ReconcileSinkError::unavailable(format!("{error:?}")))
        })
    }
}

impl NamespaceBootstrapSink for PodReconcileAdapter {
    fn create_default_service_account(&self, namespace: String) -> ReconcileSinkFuture<'_> {
        Box::pin(async move {
            crate::controllers::namespace::create_default_service_account_at(
                self.db.as_ref(),
                &namespace,
                chrono::Utc::now(),
            )
            .await
            .map_err(|error| ReconcileSinkError::unavailable(error.to_string()))
        })
    }

    fn create_root_ca_config_map(
        &self,
        namespace: String,
        ca_certificate: String,
    ) -> ReconcileSinkFuture<'_> {
        Box::pin(async move {
            crate::controllers::namespace::create_kube_root_ca_configmap_at(
                self.db.as_ref(),
                &namespace,
                &ca_certificate,
                chrono::Utc::now(),
            )
            .await
            .map_err(|error| ReconcileSinkError::unavailable(error.to_string()))
        })
    }
}

impl PodGcReconcileSink for PodReconcileAdapter {
    fn reconcile_owner_references<'a>(
        &'a self,
        pod: Resource,
        pod_delete_sink: &'a dyn GcPodDeleteSink,
    ) -> ReconcileSinkFuture<'a> {
        Box::pin(async move {
            crate::controllers::gc::reconcile_owner_references(
                self.db.as_ref(),
                pod,
                pod_delete_sink,
                &self.non_pod_finalization,
                self.coordination.as_ref(),
            )
            .await
            .map(|_| ())
            .map_err(|error| ReconcileSinkError::unavailable(error.to_string()))
        })
    }

    fn cascade_delete_dependents<'a>(
        &'a self,
        owner: PodIdentity,
        pod_delete_sink: &'a dyn GcPodDeleteSink,
    ) -> ReconcileSinkFuture<'a> {
        Box::pin(async move {
            crate::controllers::gc::cascade_delete_with_uid(
                self.db.as_ref(),
                &owner.uid,
                "v1",
                &owner.name,
                "Pod",
                Some(owner.namespace),
                pod_delete_sink,
                &self.non_pod_finalization,
                self.coordination.as_ref(),
            )
            .await
            .map_err(|error| ReconcileSinkError::unavailable(error.to_string()))
        })
    }

    fn finalize_foreground_owners<'a>(
        &'a self,
        deleted_dependent: Resource,
        pod_delete_sink: &'a dyn GcPodDeleteSink,
    ) -> ReconcileSinkFuture<'a> {
        Box::pin(async move {
            crate::controllers::gc::finalize_foreground_owners_after_dependent_delete(
                self.db.as_ref(),
                &deleted_dependent,
                pod_delete_sink,
                &self.non_pod_finalization,
                self.coordination.as_ref(),
            )
            .await
            .map_err(|error| ReconcileSinkError::unavailable(error.to_string()))
        })
    }
}

impl PodPdbReconcileSink for PodReconcileAdapter {
    fn reconcile_namespace_pdbs(&self, namespace: String) -> ReconcileSinkFuture<'_> {
        Box::pin(async move {
            let now = chrono::Utc::now();
            crate::controllers::pdb::reconcile_pdbs_for_namespace(
                self.db.as_ref(),
                self.pod_reader.as_ref(),
                &namespace,
                now,
            )
            .await;
            Ok(())
        })
    }
}

impl PodEvictionAdmissionSink for PodReconcileAdapter {
    fn admit_pod_eviction(
        &self,
        request: PodEvictionAdmissionRequest,
    ) -> PodEvictionAdmissionFuture<'_> {
        Box::pin(async move {
            let now = chrono::Utc::now();
            let namespace = request.pod.namespace.as_deref().ok_or_else(|| {
                ReconcileSinkError::unavailable("stored Pod is missing metadata.namespace")
            })?;
            crate::controllers::pdb::reconcile_pdbs_for_namespace_checked(
                self.db.as_ref(),
                self.pod_reader.as_ref(),
                namespace,
                now,
            )
            .await
            .map_err(|error| ReconcileSinkError::unavailable(error.to_string()))?;
            crate::controllers::pdb::admit_pod_eviction_at(
                self.db.as_ref(),
                &request.pod,
                request.dry_run,
                now,
            )
            .await
            .map_err(|error| ReconcileSinkError::unavailable(error.to_string()))
        })
    }
}

pub(crate) struct PersistentVolumeReconcileAdapter<'a> {
    db: &'a dyn crate::datastore::DatastoreBackend,
    file_process: &'a klights_supervisor::FileProcessExecutor,
    local_path_provisioner_root: &'a std::path::Path,
}

impl<'a> PersistentVolumeReconcileAdapter<'a> {
    pub(crate) fn new(
        db: &'a dyn crate::datastore::DatastoreBackend,
        file_process: &'a klights_supervisor::FileProcessExecutor,
        local_path_provisioner_root: &'a std::path::Path,
    ) -> Self {
        Self {
            db,
            file_process,
            local_path_provisioner_root,
        }
    }
}

impl PvcReconcileSink for PersistentVolumeReconcileAdapter<'_> {
    fn reconcile_pvc(&self, pvc: Resource) -> PvcReconcileFuture<'_> {
        Box::pin(async move {
            let updated = crate::controllers::pvc::reconcile_pvc(
                self.file_process,
                self.local_path_provisioner_root,
                self.db,
                &pvc.data,
            )
            .await
            .map_err(|error| ReconcileSinkError::unavailable(error.to_string()))?;
            Ok(PvcReconcileOutcome {
                phase: updated
                    .pointer("/status/phase")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string),
                volume_name: updated
                    .pointer("/status/volumeName")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string),
            })
        })
    }
}

impl PodServiceReconcileSink for PodReconcileAdapter {
    fn enqueue_after_pod_create(&self, pod: Resource) -> ReconcileSinkFuture<'_> {
        Box::pin(async move {
            klights_controllers::side_effects::service_pod::enqueue_services_after_pod_create(
                &pod.data,
                self.db.as_ref(),
                &self.dispatcher,
            )
            .await
            .map_err(|error| ReconcileSinkError::unavailable(error.to_string()))
        })
    }

    fn enqueue_after_pod_update(
        &self,
        previous: Resource,
        updated: Resource,
    ) -> ReconcileSinkFuture<'_> {
        Box::pin(async move {
            klights_controllers::side_effects::service_pod::enqueue_services_after_pod_update(
                &previous.data,
                &updated.data,
                self.db.as_ref(),
                &self.dispatcher,
            )
            .await
            .map_err(|error| ReconcileSinkError::unavailable(error.to_string()))
        })
    }
}
