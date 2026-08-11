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
    controller_store: std::sync::Arc<super::controller_runtime_adapter::RootControllerLeaderPort>,
    namespace_bootstrap: crate::bootstrap::composition_adapters::leader_bootstrap_store_adapter::LeaderBootstrapStore,
    namespace_lifecycle: std::sync::Arc<dyn klights_reconcile_api::NamespaceLifecycleStore>,
    dispatcher: ControllerDispatcherSlot,
    metrics: std::sync::Arc<klights_controllers::side_effects::SideEffectMetrics>,
    side_effects: std::sync::Arc<klights_controllers::side_effects::SideEffectRegistry>,
    pod_reader: std::sync::Arc<dyn klights_pod_api::PodQuery>,
    non_pod_finalization:
        crate::bootstrap::controller_adapters::gc_delete_adapter::GcNonPodFinalizationAdapter,
    coordination: std::sync::Arc<dyn klights_reconcile_api::GcForegroundDeleteCoordination>,
    identity: std::sync::Arc<dyn klights_controllers::ControllerIdentityGenerator>,
}

pub(crate) struct PodReconcileStorage {
    db: DatastoreHandle,
    resource_commands: std::sync::Arc<dyn klights_leader_api::LeaderResourceCommand>,
}

impl PodReconcileStorage {
    pub(crate) fn new(
        db: DatastoreHandle,
        resource_commands: std::sync::Arc<dyn klights_leader_api::LeaderResourceCommand>,
    ) -> Self {
        Self {
            db,
            resource_commands,
        }
    }
}

impl PodReconcileAdapter {
    #[cfg(test)]
    pub(crate) fn new(
        db: DatastoreHandle,
        dispatcher: ControllerDispatcherSlot,
        metrics: std::sync::Arc<klights_controllers::side_effects::SideEffectMetrics>,
        side_effects: std::sync::Arc<klights_controllers::side_effects::SideEffectRegistry>,
        pod_reader: std::sync::Arc<dyn klights_pod_api::PodQuery>,
        identity: std::sync::Arc<dyn klights_controllers::ControllerIdentityGenerator>,
    ) -> Self {
        let resource_commands =
            super::controller_runtime_adapter::RootControllerLeaderPort::resource_commands_for_test(
                db.clone(),
            );
        Self::new_with_coordination(
            PodReconcileStorage::new(db, resource_commands),
            dispatcher,
            metrics,
            side_effects,
            pod_reader,
            std::sync::Arc::new(klights_controllers::ControllerCoordination::new()),
            identity,
        )
    }

    pub(crate) fn new_with_coordination(
        storage: PodReconcileStorage,
        dispatcher: ControllerDispatcherSlot,
        metrics: std::sync::Arc<klights_controllers::side_effects::SideEffectMetrics>,
        side_effects: std::sync::Arc<klights_controllers::side_effects::SideEffectRegistry>,
        pod_reader: std::sync::Arc<dyn klights_pod_api::PodQuery>,
        coordination: std::sync::Arc<dyn klights_reconcile_api::GcForegroundDeleteCoordination>,
        identity: std::sync::Arc<dyn klights_controllers::ControllerIdentityGenerator>,
    ) -> Self {
        let PodReconcileStorage {
            db,
            resource_commands,
        } = storage;
        let controller_store = std::sync::Arc::new(
            super::controller_runtime_adapter::RootControllerLeaderPort::new_with_commands(
                db.clone(),
                resource_commands.clone(),
            ),
        );
        Self {
            non_pod_finalization: crate::bootstrap::controller_adapters::gc_delete_adapter::GcNonPodFinalizationAdapter::new_with_commands(
                db.clone(), resource_commands.clone(),
            ),
            namespace_lifecycle: crate::bootstrap::composition_adapters::api_state_adapter::RootNamespaceTerminationStore::new_with_commands(
                db.clone(),
                resource_commands.clone(),
            ),
            namespace_bootstrap: crate::bootstrap::composition_adapters::leader_bootstrap_store_adapter::LeaderBootstrapStore::new(
                db.clone(),
                resource_commands,
            ),
            controller_store,
            db,
            dispatcher,
            metrics,
            side_effects,
            pod_reader,
            coordination,
            identity,
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
                        klights_controllers::side_effects::run_named_hook_logged(
                            &self.side_effects,
                            &pod.data,
                            &self.metrics,
                            hook_name,
                            context,
                        )
                        .await;
                    } else {
                        klights_controllers::side_effects::run_hooks_logged(
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
                                klights_controllers::side_effects::workload_pod::workload_owner_keys_for_pod(
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
                Some(uid) => {
                    k8s_native_service::reconcile_namespace_termination_for_uid_with_outcome_at(
                        self.namespace_lifecycle.as_ref(),
                        &request.namespace,
                        &uid,
                        self.metrics.as_ref(),
                        klights_supervisor::SystemWallClock::now_utc(),
                    )
                    .await
                    .map(|outcome| match outcome {
                        k8s_native_service::NamespaceTerminationOutcome::Finalized => {
                            NamespaceTerminationOutcome::Finalized
                        }
                        k8s_native_service::NamespaceTerminationOutcome::StillPending => {
                            NamespaceTerminationOutcome::StillPending
                        }
                    })
                }
                None => k8s_native_service::reconcile_namespace_termination_at(
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
            klights_controllers::namespace::create_default_service_account_at(
                &self.namespace_bootstrap,
                &namespace,
                chrono::Utc::now(),
                self.identity.as_ref(),
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
            klights_controllers::namespace::create_kube_root_ca_configmap_at(
                &self.namespace_bootstrap,
                &namespace,
                &ca_certificate,
                chrono::Utc::now(),
                self.identity.as_ref(),
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
            klights_controllers::gc::reconcile_owner_references(
                self.controller_store.as_ref(),
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
            klights_controllers::gc::cascade_delete_with_uid(
                self.controller_store.as_ref(),
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
            klights_controllers::gc::finalize_foreground_owners_after_dependent_delete(
                self.controller_store.as_ref(),
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
            klights_controllers::pdb::reconcile_pdbs_for_namespace(
                self.controller_store.as_ref(),
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
            klights_controllers::pdb::reconcile_pdbs_for_namespace_checked(
                self.controller_store.as_ref(),
                self.pod_reader.as_ref(),
                namespace,
                now,
            )
            .await
            .map_err(|error| ReconcileSinkError::unavailable(error.to_string()))?;
            klights_controllers::pdb::admit_pod_eviction_at(
                self.controller_store.as_ref(),
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
    store: &'a dyn klights_controllers::pvc::PvcStore,
    file_process: &'a klights_supervisor::FileProcessExecutor,
    local_path_provisioner_root: &'a std::path::Path,
}

impl<'a> PersistentVolumeReconcileAdapter<'a> {
    pub(crate) fn new(
        store: &'a dyn klights_controllers::pvc::PvcStore,
        file_process: &'a klights_supervisor::FileProcessExecutor,
        local_path_provisioner_root: &'a std::path::Path,
    ) -> Self {
        Self {
            store,
            file_process,
            local_path_provisioner_root,
        }
    }
}

impl PvcReconcileSink for PersistentVolumeReconcileAdapter<'_> {
    fn reconcile_pvc(&self, pvc: Resource) -> PvcReconcileFuture<'_> {
        Box::pin(async move {
            let updated = klights_controllers::pvc::reconcile_pvc(
                self.file_process,
                self.local_path_provisioner_root,
                self.store,
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
