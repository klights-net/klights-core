//! Applied-Pod side-effect policy shared by every committed outbox path.
//!
//! Every successful Pod status update or actor-owned finalization must
//! enqueue workload-owner, Job, and Service reconcile keys through focused
//! leader-side reconciliation sinks. Without this the leader's controllers
//! never see the change — Endpoints/EndpointSlices stay empty, Deployment /
//! StatefulSet rollout never observes pod readiness, Job `.status.ready` stays
//! stale, and StatefulSet ordinal recreate stalls after a Pod is finalized off
//! the worker.
//!
//! Two callers wire into this:
//!
//!  * `LocalApiClient::apply_outbox` — leader-bundled-worker writes through
//!    the in-process outbox.
//!  * `replication::grpc::server::Replication::apply_outbox` — remote worker
//!    writes forwarded over gRPC.
//!
//! Both paths converge on the same leader-side apply result. Root supplies
//! concrete adapters for the focused stores below; controller policy never
//! receives or names the cluster datastore implementation.

use async_trait::async_trait;
use klights_cluster_core::{Resource, command::StorageCommand};
use klights_reconcile_api::{
    ControllerReconcileSink, ControllerStoreResult, GcForegroundDeleteCoordination,
    GcNonPodFinalizationPort, GcPodDeleteSink, NamespaceTerminationRequest,
    NamespaceTerminationSink, ReconcileKey, ServiceReconcileKey, ServiceReconcileSink,
};
use thiserror::Error;

use super::job::JobSideEffectStore;
use super::service_pod::ServicePodStore;
use super::workload_pod::WorkloadPodStore;
use crate::gc::GcResourceStore;

#[async_trait]
pub trait AppliedPodPdbStore: Send + Sync {
    async fn list_pod_disruption_budgets(
        &self,
        namespace: &str,
    ) -> ControllerStoreResult<Vec<Resource>>;
}

pub struct AppliedPodSideEffectStores<'a> {
    service_pods: &'a dyn ServicePodStore,
    workload_pods: &'a dyn WorkloadPodStore,
    jobs: &'a dyn JobSideEffectStore,
    pdbs: &'a dyn AppliedPodPdbStore,
    gc: &'a dyn GcResourceStore,
}

impl<'a> AppliedPodSideEffectStores<'a> {
    pub const fn new(
        service_pods: &'a dyn ServicePodStore,
        workload_pods: &'a dyn WorkloadPodStore,
        jobs: &'a dyn JobSideEffectStore,
        pdbs: &'a dyn AppliedPodPdbStore,
        gc: &'a dyn GcResourceStore,
    ) -> Self {
        Self {
            service_pods,
            workload_pods,
            jobs,
            pdbs,
            gc,
        }
    }
}

pub struct AppliedPodSideEffectSinks<'a> {
    controller: Option<&'a dyn ControllerReconcileSink>,
    service: Option<&'a dyn ServiceReconcileSink>,
    pod_delete: Option<&'a dyn GcPodDeleteSink>,
    non_pod_finalization: Option<&'a dyn GcNonPodFinalizationPort>,
    namespace_termination: Option<&'a dyn NamespaceTerminationSink>,
    gc_coordination: &'a dyn GcForegroundDeleteCoordination,
}

impl<'a> AppliedPodSideEffectSinks<'a> {
    pub const fn new(
        controller: Option<&'a dyn ControllerReconcileSink>,
        service: Option<&'a dyn ServiceReconcileSink>,
        pod_delete: Option<&'a dyn GcPodDeleteSink>,
        non_pod_finalization: Option<&'a dyn GcNonPodFinalizationPort>,
        namespace_termination: Option<&'a dyn NamespaceTerminationSink>,
        gc_coordination: &'a dyn GcForegroundDeleteCoordination,
    ) -> Self {
        Self {
            controller,
            service,
            pod_delete,
            non_pod_finalization,
            namespace_termination,
            gc_coordination,
        }
    }
}

#[derive(Debug, Error)]
#[error("{message}")]
pub struct AppliedPodSideEffectError {
    message: String,
}

impl AppliedPodSideEffectError {
    fn required_cascade(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

pub fn needs_committed_pod_side_effects(command: &StorageCommand) -> bool {
    matches!(
        command,
        StorageCommand::UpdateStatus {
            api_version,
            kind,
            ..
        } | StorageCommand::DeleteResource {
            api_version,
            kind,
            ..
        } if api_version == "v1" && kind == "Pod"
    ) || matches!(command, StorageCommand::FinalizeBoundPod { .. })
}

pub async fn handle_applied_pod_side_effects(
    stores: AppliedPodSideEffectStores<'_>,
    sinks: AppliedPodSideEffectSinks<'_>,
    command: &StorageCommand,
    resource: Option<&Resource>,
    pod_endpoint_effect: klights_cluster_core::PodEndpointEffect,
) -> Result<(), AppliedPodSideEffectError> {
    // The actor delete is already committed at this point. Its dependent
    // cascade is the one mandatory delivery in this group: absence or a
    // transient failure must keep the worker's durable outbox item pending so
    // an idempotent replay can use the persisted exact delete receipt.
    cascade_dependents_after_actor_pod_delete(
        sinks.pod_delete,
        sinks.non_pod_finalization,
        sinks.gc_coordination,
        command,
        resource,
        stores.gc,
    )
    .await?;
    enqueue_pod_status_side_effects_with_endpoint_change(
        sinks.controller,
        sinks.service,
        command,
        resource,
        pod_endpoint_effect,
        &stores,
    )
    .await;
    finalize_foreground_owners_after_pod_delete(
        sinks.pod_delete,
        sinks.non_pod_finalization,
        sinks.gc_coordination,
        command,
        resource,
        stores.gc,
    )
    .await;
    reconcile_namespace_after_pod_delete(command, resource, sinks.namespace_termination).await;
    Ok(())
}

async fn enqueue_pod_status_side_effects_with_endpoint_change(
    controller_sink: Option<&dyn ControllerReconcileSink>,
    service_sink: Option<&dyn ServiceReconcileSink>,
    command: &StorageCommand,
    resource: Option<&Resource>,
    pod_endpoint_effect: klights_cluster_core::PodEndpointEffect,
    stores: &AppliedPodSideEffectStores<'_>,
) {
    if controller_sink.is_none() && service_sink.is_none() {
        return;
    }
    let is_endpoint_relevant_patch = matches!(
        command,
        StorageCommand::PatchResource {
            api_version,
            kind,
            patch,
            ..
        } if api_version == "v1"
            && kind == "Pod"
            && patch
                .pointer("/metadata")
                .and_then(serde_json::Value::as_object)
                .is_some_and(|metadata| {
                    metadata.contains_key("labels")
                        || metadata.contains_key("deletionTimestamp")
                })
    );
    let is_endpoint_relevant_status = matches!(
        command,
        StorageCommand::UpdateStatus { api_version, kind, .. }
            if api_version == "v1"
                && kind == "Pod"
                && pod_endpoint_effect == klights_cluster_core::PodEndpointEffect::Changed
    );
    let is_pod_status_delete_or_endpoint_patch = is_endpoint_relevant_patch
        || matches!(
            command,
            StorageCommand::UpdateStatus { api_version, kind, .. }
                | StorageCommand::DeleteResource { api_version, kind, .. }
            if api_version == "v1" && kind == "Pod"
        )
        || matches!(command, StorageCommand::FinalizeBoundPod { .. });
    if !is_pod_status_delete_or_endpoint_patch {
        return;
    }
    let Some(resource) = resource else {
        return;
    };
    let namespace = resource
        .data
        .pointer("/metadata/namespace")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if namespace.is_empty() {
        return;
    }
    let service_keys = if service_sink.is_some()
        && (is_endpoint_relevant_patch
            || is_endpoint_relevant_status
            || matches!(
                command,
                StorageCommand::DeleteResource { .. } | StorageCommand::FinalizeBoundPod { .. }
            )) {
        match super::service_pod::service_reconcile_keys_for_pod(
            &resource.data,
            stores.service_pods,
            namespace,
        )
        .await
        {
            Ok(keys) => keys,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    namespace,
                    "failed to derive Service keys for pod status side effects"
                );
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };
    if is_endpoint_relevant_patch {
        enqueue_service_keys(service_sink, service_keys).await;
        return;
    }
    let Some(controller_sink) = controller_sink else {
        enqueue_service_keys(service_sink, service_keys).await;
        return;
    };
    let workload_keys = match super::workload_pod::workload_reconcile_keys_for_pod(
        &resource.data,
        stores.workload_pods,
        namespace,
    )
    .await
    {
        Ok(keys) => keys,
        Err(err) => {
            tracing::warn!(
                error = %err,
                namespace,
                "failed to derive workload owner keys for pod status side effects"
            );
            Vec::new()
        }
    };
    let job_keys = match super::job::job_reconcile_keys_for_pod(
        &resource.data,
        stores.jobs,
        namespace,
    )
    .await
    {
        Ok(keys) => keys,
        Err(err) => {
            tracing::warn!(
                error = %err,
                namespace,
                "failed to derive Job keys for pod status side effects"
            );
            Vec::new()
        }
    };
    let pdb_keys = pdb_reconcile_keys_for_namespace(stores.pdbs, namespace).await;
    let mut controller_keys = workload_keys;
    controller_keys.extend(job_keys);
    controller_keys.extend(pdb_keys);
    if let Err(err) = controller_sink
        .enqueue_reconcile_batch(controller_keys)
        .await
    {
        tracing::warn!(error = %err, namespace, "failed to enqueue controller reconcile batch");
    }
    enqueue_service_keys(service_sink, service_keys).await;
}

async fn enqueue_service_keys(
    service_sink: Option<&dyn ServiceReconcileSink>,
    keys: Vec<ServiceReconcileKey>,
) {
    let Some(service_sink) = service_sink else {
        return;
    };
    if let Err(err) = service_sink.enqueue_service_reconcile_batch(keys).await {
        tracing::warn!(error = %err, "failed to enqueue Service reconcile batch");
    }
}

async fn finalize_foreground_owners_after_pod_delete(
    gc_pod_delete_sink: Option<&dyn GcPodDeleteSink>,
    non_pod_finalization: Option<&dyn GcNonPodFinalizationPort>,
    coordination: &dyn GcForegroundDeleteCoordination,
    command: &StorageCommand,
    resource: Option<&Resource>,
    gc_store: &dyn GcResourceStore,
) {
    let is_pod_delete = matches!(
        command,
        StorageCommand::DeleteResource { api_version, kind, .. }
            if api_version == "v1" && kind == "Pod"
    ) || matches!(command, StorageCommand::FinalizeBoundPod { .. });
    if !is_pod_delete {
        return;
    }

    let Some(resource) = resource else {
        return;
    };
    let Some(gc_pod_delete_sink) = gc_pod_delete_sink else {
        return;
    };
    let Some(non_pod_finalization) = non_pod_finalization else {
        return;
    };
    let deleted_resource = resource.clone();
    if let Err(err) = crate::gc::finalize_foreground_owners_after_dependent_delete(
        gc_store,
        &deleted_resource,
        gc_pod_delete_sink,
        non_pod_finalization,
        coordination,
    )
    .await
    {
        tracing::error!(
            namespace = ?deleted_resource.namespace,
            pod = %deleted_resource.name,
            uid = %deleted_resource.uid,
            error = %err,
            "leader outbox Pod delete foreground-owner check failed"
        );
    }
}

async fn cascade_dependents_after_actor_pod_delete(
    gc_pod_delete_sink: Option<&dyn GcPodDeleteSink>,
    non_pod_finalization: Option<&dyn GcNonPodFinalizationPort>,
    coordination: &dyn GcForegroundDeleteCoordination,
    command: &StorageCommand,
    resource: Option<&Resource>,
    gc_store: &dyn GcResourceStore,
) -> Result<(), AppliedPodSideEffectError> {
    if !matches!(command, StorageCommand::FinalizeBoundPod { .. }) {
        return Ok(());
    }
    let resource = resource.ok_or_else(|| {
        AppliedPodSideEffectError::required_cascade(
            "committed bound Pod finalization is missing its durable delete receipt",
        )
    })?;
    let gc_pod_delete_sink = gc_pod_delete_sink.ok_or_else(|| {
        AppliedPodSideEffectError::required_cascade(
            "committed bound Pod finalization has no dependent-cascade sink",
        )
    })?;
    let non_pod_finalization = non_pod_finalization.ok_or_else(|| {
        AppliedPodSideEffectError::required_cascade(
            "committed bound Pod finalization has no non-Pod finalization port",
        )
    })?;
    let namespace = resource.namespace.clone().or_else(|| {
        resource
            .data
            .pointer("/metadata/namespace")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
    });
    let uid = resource
        .data
        .pointer("/metadata/uid")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    if uid.is_empty() {
        return Err(AppliedPodSideEffectError::required_cascade(
            "committed bound Pod finalization receipt has no metadata.uid",
        ));
    }
    crate::gc::cascade_delete_with_uid(
        gc_store,
        uid,
        &resource.api_version,
        &resource.name,
        &resource.kind,
        namespace,
        gc_pod_delete_sink,
        non_pod_finalization,
        coordination,
    )
    .await
    .map_err(|error| {
        AppliedPodSideEffectError::required_cascade(format!(
            "leader committed Pod delete dependent cascade failed for {}/{} uid {}: {error}",
            resource.namespace.as_deref().unwrap_or(""),
            resource.name,
            uid
        ))
    })
}

async fn reconcile_namespace_after_pod_delete(
    command: &StorageCommand,
    resource: Option<&Resource>,
    sink: Option<&dyn NamespaceTerminationSink>,
) {
    let Some(sink) = sink else {
        return;
    };
    let namespace = match command {
        StorageCommand::DeleteResource {
            api_version,
            kind,
            namespace,
            ..
        } if api_version == "v1" && kind == "Pod" => namespace.as_deref(),
        StorageCommand::FinalizeBoundPod { namespace, .. } => Some(namespace.as_str()),
        _ => return,
    }
    .or_else(|| {
        resource
            .and_then(|resource| resource.data.pointer("/metadata/namespace"))
            .and_then(|value| value.as_str())
    })
    .unwrap_or("default");
    if namespace.is_empty() {
        return;
    }

    if let Err(err) = sink
        .reconcile_namespace_termination(NamespaceTerminationRequest {
            namespace: namespace.to_string(),
            expected_uid: None,
        })
        .await
    {
        tracing::warn!(
            namespace,
            error = ?err,
            "leader outbox Pod delete namespace termination reconcile failed"
        );
    }
}

async fn pdb_reconcile_keys_for_namespace(
    store: &dyn AppliedPodPdbStore,
    namespace: &str,
) -> Vec<ReconcileKey> {
    let pdbs = match store.list_pod_disruption_budgets(namespace).await {
        Ok(pdbs) => pdbs,
        Err(err) => {
            tracing::warn!(
                error = %err,
                namespace,
                "failed to list PDBs for pod status side effects"
            );
            return Vec::new();
        }
    };

    pdbs.into_iter()
        .filter_map(|pdb| {
            pdb.data
                .pointer("/metadata/name")
                .and_then(|name| name.as_str())
                .map(|name| {
                    ReconcileKey::namespaced("policy/v1", "PodDisruptionBudget", namespace, name)
                })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use async_trait::async_trait;
    use klights_cluster_core::{PatchKind, ResourcePreconditions};
    use klights_reconcile_api::{ControllerStoreError, ReconcileSinkFuture};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    fn gc_coordination() -> &'static crate::ControllerCoordination {
        static COORDINATION: std::sync::LazyLock<crate::ControllerCoordination> =
            std::sync::LazyLock::new(crate::ControllerCoordination::new);
        &COORDINATION
    }

    use klights_cluster_core::command::StorageCommand;
    use klights_types::PodIdentity;
    use serde_json::json;

    macro_rules! test_resource {
        (
            api_version: $api_version:expr,
            kind: $kind:expr,
            namespace: $namespace:expr,
            name: $name:expr,
            resource_version: $resource_version:expr,
            data: $data:expr $(,)?
        ) => {{
            let data = Arc::new($data);
            Resource {
                id: 0,
                api_version: $api_version,
                kind: $kind,
                namespace: $namespace,
                name: $name,
                uid: Resource::uid_from_data(&data),
                resource_version: $resource_version,
                data,
            }
        }};
    }

    #[derive(Default)]
    struct FakeAppliedPodStore {
        resources: Mutex<Vec<Resource>>,
    }

    impl FakeAppliedPodStore {
        fn insert(
            &self,
            api_version: &str,
            kind: &str,
            namespace: Option<&str>,
            name: &str,
            data: serde_json::Value,
        ) {
            let data = Arc::new(data);
            self.resources.lock().unwrap().push(Resource {
                id: 0,
                api_version: api_version.to_string(),
                kind: kind.to_string(),
                namespace: namespace.map(str::to_string),
                name: name.to_string(),
                uid: Resource::uid_from_data(&data),
                resource_version: 1,
                data,
            });
        }

        fn list(&self, api_version: &str, kind: &str, namespace: &str) -> Vec<Resource> {
            self.resources
                .lock()
                .unwrap()
                .iter()
                .filter(|resource| {
                    resource.api_version == api_version
                        && resource.kind == kind
                        && resource.namespace.as_deref() == Some(namespace)
                })
                .cloned()
                .collect()
        }

        fn replace_resource(
            &self,
            api_version: &str,
            kind: &str,
            namespace: Option<&str>,
            name: &str,
            data: serde_json::Value,
        ) -> ControllerStoreResult<Resource> {
            let mut resources = self.resources.lock().unwrap();
            let resource = resources
                .iter_mut()
                .find(|resource| {
                    resource.api_version == api_version
                        && resource.kind == kind
                        && resource.namespace.as_deref() == namespace
                        && resource.name == name
                })
                .ok_or_else(|| ControllerStoreError::not_found("test resource not found"))?;
            resource.data = Arc::new(data);
            resource.uid = Resource::uid_from_data(&resource.data);
            resource.resource_version += 1;
            Ok(resource.clone())
        }
    }

    #[async_trait]
    impl super::super::service_pod::ServicePodStore for FakeAppliedPodStore {
        async fn load_service_endpoint_state(
            &self,
            namespace: &str,
        ) -> Result<super::super::service_pod::ServiceEndpointState> {
            Ok(super::super::service_pod::ServiceEndpointState {
                services: self.list("v1", "Service", namespace),
                endpoints: self.list("v1", "Endpoints", namespace),
                endpoint_slices: self.list("discovery.k8s.io/v1", "EndpointSlice", namespace),
            })
        }
    }

    #[async_trait]
    impl super::super::workload_pod::WorkloadPodStore for FakeAppliedPodStore {
        async fn get_replica_set(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
            Ok(self
                .list("apps/v1", "ReplicaSet", namespace)
                .into_iter()
                .find(|resource| resource.name == name))
        }

        async fn list_replica_sets(&self, namespace: &str) -> Result<Vec<Resource>> {
            Ok(self.list("apps/v1", "ReplicaSet", namespace))
        }

        async fn list_replication_controllers(&self, namespace: &str) -> Result<Vec<Resource>> {
            Ok(self.list("v1", "ReplicationController", namespace))
        }
    }

    #[async_trait]
    impl super::super::job::JobSideEffectStore for FakeAppliedPodStore {
        async fn list_jobs(&self, namespace: &str) -> Result<Vec<Resource>> {
            Ok(self.list("batch/v1", "Job", namespace))
        }
    }

    #[async_trait]
    impl AppliedPodPdbStore for FakeAppliedPodStore {
        async fn list_pod_disruption_budgets(
            &self,
            namespace: &str,
        ) -> ControllerStoreResult<Vec<Resource>> {
            Ok(self.list("policy/v1", "PodDisruptionBudget", namespace))
        }
    }

    #[async_trait]
    impl GcResourceStore for FakeAppliedPodStore {
        async fn list_custom_resource_definitions(&self) -> ControllerStoreResult<Vec<Resource>> {
            Ok(self.list("apiextensions.k8s.io/v1", "CustomResourceDefinition", ""))
        }

        async fn get_resource(
            &self,
            api_version: &str,
            kind: &str,
            namespace: Option<&str>,
            name: &str,
        ) -> ControllerStoreResult<Option<Resource>> {
            Ok(self
                .resources
                .lock()
                .unwrap()
                .iter()
                .find(|resource| {
                    resource.api_version == api_version
                        && resource.kind == kind
                        && resource.namespace.as_deref() == namespace
                        && resource.name == name
                })
                .cloned())
        }

        async fn update_resource_with_preconditions(
            &self,
            api_version: &str,
            kind: &str,
            namespace: Option<&str>,
            name: &str,
            data: serde_json::Value,
            _preconditions: ResourcePreconditions,
        ) -> ControllerStoreResult<Resource> {
            self.replace_resource(api_version, kind, namespace, name, data)
        }

        async fn update_main_resource_with_preconditions(
            &self,
            api_version: &str,
            kind: &str,
            namespace: Option<&str>,
            name: &str,
            data: serde_json::Value,
            _preconditions: ResourcePreconditions,
        ) -> ControllerStoreResult<Resource> {
            self.replace_resource(api_version, kind, namespace, name, data)
        }

        async fn find_owned_resources(
            &self,
            owner_uid: &str,
            namespace: Option<&str>,
        ) -> ControllerStoreResult<Vec<Resource>> {
            Ok(self
                .resources
                .lock()
                .unwrap()
                .iter()
                .filter(|resource| {
                    resource.namespace.as_deref() == namespace
                        && resource
                            .data
                            .pointer("/metadata/ownerReferences")
                            .and_then(serde_json::Value::as_array)
                            .is_some_and(|owners| {
                                owners.iter().any(|owner| {
                                    owner.get("uid").and_then(serde_json::Value::as_str)
                                        == Some(owner_uid)
                                })
                            })
                })
                .cloned()
                .collect())
        }

        async fn find_owned_by_name_kind_empty_uid(
            &self,
            owner_api_version: &str,
            owner_name: &str,
            owner_kind: &str,
            namespace: Option<&str>,
        ) -> ControllerStoreResult<Vec<Resource>> {
            Ok(self
                .resources
                .lock()
                .unwrap()
                .iter()
                .filter(|resource| {
                    resource.namespace.as_deref() == namespace
                        && resource
                            .data
                            .pointer("/metadata/ownerReferences")
                            .and_then(serde_json::Value::as_array)
                            .is_some_and(|owners| {
                                owners.iter().any(|owner| {
                                    owner.get("apiVersion").and_then(serde_json::Value::as_str)
                                        == Some(owner_api_version)
                                        && owner.get("kind").and_then(serde_json::Value::as_str)
                                            == Some(owner_kind)
                                        && owner.get("name").and_then(serde_json::Value::as_str)
                                            == Some(owner_name)
                                        && owner
                                            .get("uid")
                                            .and_then(serde_json::Value::as_str)
                                            .is_none_or(str::is_empty)
                                })
                            })
                })
                .cloned()
                .collect())
        }
    }

    #[derive(Default)]
    struct RecordingReconcileSink {
        keys: Mutex<Vec<ReconcileKey>>,
    }

    impl RecordingReconcileSink {
        fn enqueue(&self, key: ReconcileKey) {
            let mut keys = self.keys.lock().unwrap();
            if !keys.contains(&key) {
                keys.push(key);
            }
        }

        fn queued(&self) -> Vec<ReconcileKey> {
            self.keys.lock().unwrap().clone()
        }
    }

    impl ControllerReconcileSink for RecordingReconcileSink {
        fn enqueue_reconcile_batch(&self, keys: Vec<ReconcileKey>) -> ReconcileSinkFuture<'_> {
            Box::pin(async move {
                for key in keys {
                    self.enqueue(key);
                }
                Ok(())
            })
        }
    }

    impl ServiceReconcileSink for RecordingReconcileSink {
        fn enqueue_service_reconcile_batch(
            &self,
            keys: Vec<ServiceReconcileKey>,
        ) -> ReconcileSinkFuture<'_> {
            Box::pin(async move {
                for key in keys {
                    self.enqueue(key.into_reconcile_key());
                }
                Ok(())
            })
        }
    }

    #[derive(Default)]
    struct NoopNonPodFinalization;

    impl GcNonPodFinalizationPort for NoopNonPodFinalization {
        fn finalize_non_pod(
            &self,
            _request: klights_reconcile_api::GcNonPodFinalizationRequest,
        ) -> klights_reconcile_api::GcNonPodFinalizationFuture<'_> {
            Box::pin(async { Ok(klights_reconcile_api::GcNonPodFinalizationOutcome::Gone) })
        }
    }

    fn stores(store: &FakeAppliedPodStore) -> AppliedPodSideEffectStores<'_> {
        AppliedPodSideEffectStores::new(store, store, store, store, store)
    }

    fn sinks<'a>(
        reconcile: Option<&'a RecordingReconcileSink>,
        pod_delete: Option<&'a dyn GcPodDeleteSink>,
        non_pod: Option<&'a dyn GcNonPodFinalizationPort>,
    ) -> AppliedPodSideEffectSinks<'a> {
        AppliedPodSideEffectSinks::new(
            reconcile.map(|sink| sink as &dyn ControllerReconcileSink),
            reconcile.map(|sink| sink as &dyn ServiceReconcileSink),
            pod_delete,
            non_pod,
            None,
            gc_coordination(),
        )
    }

    #[derive(Default)]
    struct RecordingPodDeleteSink {
        requests: Mutex<Vec<PodIdentity>>,
    }

    impl GcPodDeleteSink for RecordingPodDeleteSink {
        fn request_gc_pod_delete(
            &self,
            request: klights_reconcile_api::GcPodDeleteRequest,
        ) -> klights_reconcile_api::GcPodDeleteFuture<'_> {
            Box::pin(async move {
                self.requests.lock().unwrap().push(request.into_identity());
                Ok(())
            })
        }
    }

    #[derive(Default)]
    struct FailOncePodDeleteSink {
        failed: AtomicBool,
        attempts: AtomicUsize,
        successful: Mutex<Vec<PodIdentity>>,
    }

    impl GcPodDeleteSink for FailOncePodDeleteSink {
        fn request_gc_pod_delete(
            &self,
            request: klights_reconcile_api::GcPodDeleteRequest,
        ) -> klights_reconcile_api::GcPodDeleteFuture<'_> {
            Box::pin(async move {
                self.attempts.fetch_add(1, Ordering::Relaxed);
                if !self.failed.swap(true, Ordering::Relaxed) {
                    return Err(klights_reconcile_api::GcPodDeleteError::unavailable(
                        "transient leader-side delete failure",
                    ));
                }
                self.successful
                    .lock()
                    .unwrap()
                    .push(request.into_identity());
                Ok(())
            })
        }
    }

    #[tokio::test]
    async fn outbox_pod_status_enqueues_pdb_reconcile_for_namespace() {
        let store = FakeAppliedPodStore::default();
        store.insert(
            "policy/v1",
            "PodDisruptionBudget",
            Some("default"),
            "pdb-ready",
            json!({
                "apiVersion": "policy/v1",
                "kind": "PodDisruptionBudget",
                "metadata": {"namespace": "default", "name": "pdb-ready"},
                "spec": {
                    "minAvailable": 1,
                    "selector": {"matchLabels": {"app": "x"}}
                }
            }),
        );

        let dispatcher = RecordingReconcileSink::default();
        let command = StorageCommand::UpdateStatus {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "pdb-pod".to_string(),
            status: json!({"phase": "Running"}),
            expected_rv: None,
            preconditions: ResourcePreconditions {
                uid: Some("pod-uid".to_string()),
                resource_version: None,
            },
            observed_status_stamp: None,
        };
        let resource = test_resource! {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "pdb-pod".to_string(),
            resource_version: 2,
            data: json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "pdb-pod",
                    "uid": "pod-uid",
                    "labels": {"app": "x"}
                },
                "spec": {"containers": [{"name": "c", "image": "pause"}]},
                "status": {"phase": "Running"}
            }),
        };

        enqueue_pod_status_side_effects_with_endpoint_change(
            Some(&dispatcher),
            Some(&dispatcher),
            &command,
            Some(&resource),
            klights_cluster_core::PodEndpointEffect::Unchanged,
            &stores(&store),
        )
        .await;

        let keys = dispatcher.queued();
        assert!(
            keys.iter().any(|key| {
                key.api_version() == "policy/v1"
                    && key.kind() == "PodDisruptionBudget"
                    && key.namespace() == Some("default")
                    && key.name() == "pdb-ready"
            }),
            "outbox Pod status applies must enqueue matching PDB reconciliation"
        );
    }

    #[tokio::test]
    async fn committed_actor_delete_receipt_cascades_pod_dependents_exactly_once() {
        let store = FakeAppliedPodStore::default();
        store.insert(
            "v1",
            "Pod",
            Some("default"),
            "child",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "child",
                    "uid": "child-uid",
                    "ownerReferences": [{
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "name": "owner",
                        "uid": "owner-uid"
                    }]
                },
                "spec": {"nodeName": "worker-b"}
            }),
        );
        let command = StorageCommand::FinalizeBoundPod {
            namespace: "default".to_string(),
            name: "owner".to_string(),
            pod_uid: "owner-uid".to_string(),
            node_name: "worker-a".to_string(),
            observed_resource_version: 7,
        };
        let receipt = test_resource! {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "owner".to_string(),
            resource_version: 8,
            data: json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "owner",
                    "uid": "owner-uid"
                },
                "spec": {"nodeName": "worker-a"}
            }),
        };
        let sink = RecordingPodDeleteSink::default();
        let non_pod = NoopNonPodFinalization;

        let missing_sink = handle_applied_pod_side_effects(
            stores(&store),
            sinks(None, None, Some(&non_pod)),
            &command,
            Some(&receipt),
            klights_cluster_core::PodEndpointEffect::NotApplicable,
        )
        .await
        .expect_err("a committed finalize command must have a cascade sink");
        assert!(
            missing_sink
                .to_string()
                .contains("no dependent-cascade sink")
        );

        handle_applied_pod_side_effects(
            stores(&store),
            sinks(None, Some(&sink), Some(&non_pod)),
            &command,
            Some(&receipt),
            klights_cluster_core::PodEndpointEffect::NotApplicable,
        )
        .await
        .unwrap();
        let missing_receipt = handle_applied_pod_side_effects(
            stores(&store),
            sinks(None, Some(&sink), Some(&non_pod)),
            &command,
            None,
            klights_cluster_core::PodEndpointEffect::NotApplicable,
        )
        .await
        .expect_err("a surfaced committed finalize command must retain its receipt");
        assert!(
            missing_receipt
                .to_string()
                .contains("durable delete receipt")
        );

        let requests = sink.requests.lock().unwrap();
        assert_eq!(
            requests.as_slice(),
            &[PodIdentity::new("default", "child", "child-uid")]
        );
    }

    #[tokio::test]
    async fn committed_actor_delete_cascade_retries_after_transient_sink_failure() {
        let store = FakeAppliedPodStore::default();
        store.insert(
            "v1",
            "Pod",
            Some("default"),
            "child",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "child",
                    "uid": "child-uid",
                    "ownerReferences": [{
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "name": "owner",
                        "uid": "owner-uid"
                    }]
                },
                "spec": {"nodeName": "worker-b"}
            }),
        );
        let command = StorageCommand::FinalizeBoundPod {
            namespace: "default".to_string(),
            name: "owner".to_string(),
            pod_uid: "owner-uid".to_string(),
            node_name: "worker-a".to_string(),
            observed_resource_version: 7,
        };
        let receipt = test_resource! {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "owner".to_string(),
            resource_version: 8,
            data: json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "owner",
                    "uid": "owner-uid"
                },
                "spec": {"nodeName": "worker-a"}
            }),
        };
        let sink = FailOncePodDeleteSink::default();
        let non_pod = NoopNonPodFinalization;

        let first = handle_applied_pod_side_effects(
            stores(&store),
            sinks(None, Some(&sink), Some(&non_pod)),
            &command,
            Some(&receipt),
            klights_cluster_core::PodEndpointEffect::NotApplicable,
        )
        .await
        .expect_err("transient cascade failure must keep the durable outbox pending");
        assert!(
            first
                .to_string()
                .contains("transient leader-side delete failure")
        );

        handle_applied_pod_side_effects(
            stores(&store),
            sinks(None, Some(&sink), Some(&non_pod)),
            &command,
            Some(&receipt),
            klights_cluster_core::PodEndpointEffect::NotApplicable,
        )
        .await
        .expect("replayed committed receipt should complete the cascade");

        assert_eq!(sink.attempts.load(Ordering::Relaxed), 2);
        assert_eq!(
            sink.successful.lock().unwrap().as_slice(),
            &[PodIdentity::new("default", "child", "child-uid")]
        );
    }

    #[tokio::test]
    async fn outbox_pod_status_enqueues_job_reconcile_for_owner_reference() {
        let store = FakeAppliedPodStore::default();
        let dispatcher = RecordingReconcileSink::default();
        let command = StorageCommand::UpdateStatus {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "job-pod".to_string(),
            status: json!({"phase": "Running"}),
            expected_rv: None,
            preconditions: ResourcePreconditions {
                uid: Some("pod-uid".to_string()),
                resource_version: None,
            },
            observed_status_stamp: None,
        };
        let resource = test_resource! {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "job-pod".to_string(),
            resource_version: 2,
            data: json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "job-pod",
                    "uid": "pod-uid",
                    "ownerReferences": [{
                        "apiVersion": "batch/v1",
                        "kind": "Job",
                        "name": "ready-job",
                        "uid": "job-uid",
                        "controller": true
                    }]
                },
                "spec": {"containers": [{"name": "c", "image": "pause"}]},
                "status": {
                    "phase": "Running",
                    "conditions": [{"type": "Ready", "status": "True"}]
                }
            }),
        };

        enqueue_pod_status_side_effects_with_endpoint_change(
            Some(&dispatcher),
            Some(&dispatcher),
            &command,
            Some(&resource),
            klights_cluster_core::PodEndpointEffect::Changed,
            &stores(&store),
        )
        .await;

        let keys = dispatcher.queued();
        assert!(
            keys.iter().any(|key| {
                key.api_version() == "batch/v1"
                    && key.kind() == "Job"
                    && key.namespace() == Some("default")
                    && key.name() == "ready-job"
            }),
            "outbox Pod status applies must enqueue owning Job reconciliation"
        );
    }

    #[tokio::test]
    async fn outbox_ready_pod_status_keeps_deployment_rollout_reconcile_queued() {
        let store = FakeAppliedPodStore::default();
        store.insert(
            "apps/v1",
            "Deployment",
            Some("default"),
            "web",
            json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {
                    "namespace": "default",
                    "name": "web",
                    "uid": "deploy-web-uid"
                },
                "spec": {
                    "replicas": 1,
                    "selector": {"matchLabels": {"app": "web"}},
                    "template": {
                        "metadata": {"labels": {"app": "web"}},
                        "spec": {"containers": [{"name": "web", "image": "agnhost"}]}
                    }
                }
            }),
        );
        store.insert(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            "web-5812782185",
            json!({
                "apiVersion": "apps/v1",
                "kind": "ReplicaSet",
                "metadata": {
                    "namespace": "default",
                    "name": "web-5812782185",
                    "uid": "rs-web-uid",
                    "ownerReferences": [{
                        "apiVersion": "apps/v1",
                        "kind": "Deployment",
                        "name": "web",
                        "uid": "deploy-web-uid",
                        "controller": true
                    }]
                },
                "spec": {"replicas": 1},
                "status": {"replicas": 1, "readyReplicas": 0, "availableReplicas": 0}
            }),
        );
        let dispatcher = RecordingReconcileSink::default();
        let deployment_key = ReconcileKey::namespaced("apps/v1", "Deployment", "default", "web");
        dispatcher.enqueue(deployment_key.clone());

        let command = StorageCommand::UpdateStatus {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "web-pod".to_string(),
            status: json!({"phase": "Running"}),
            expected_rv: None,
            preconditions: ResourcePreconditions {
                uid: Some("pod-web-uid".to_string()),
                resource_version: None,
            },
            observed_status_stamp: None,
        };
        let resource = test_resource! {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "web-pod".to_string(),
            resource_version: 2,
            data: json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "web-pod",
                    "uid": "pod-web-uid",
                    "labels": {"app": "web"},
                    "ownerReferences": [{
                        "apiVersion": "apps/v1",
                        "kind": "ReplicaSet",
                        "name": "web-5812782185",
                        "uid": "rs-web-uid",
                        "controller": true
                    }]
                },
                "spec": {"containers": [{"name": "web", "image": "agnhost"}]},
                "status": {
                    "phase": "Running",
                    "conditions": [{"type": "Ready", "status": "True"}],
                    "containerStatuses": [{"name": "web", "ready": true, "restartCount": 0}]
                }
            }),
        };

        enqueue_pod_status_side_effects_with_endpoint_change(
            Some(&dispatcher),
            Some(&dispatcher),
            &command,
            Some(&resource),
            klights_cluster_core::PodEndpointEffect::Changed,
            &stores(&store),
        )
        .await;

        let keys = dispatcher.queued();
        assert_eq!(
            keys.iter().filter(|key| *key == &deployment_key).count(),
            1,
            "worker-applied pod readiness must leave one fresh Deployment rollout key queued"
        );
    }

    #[tokio::test]
    async fn outbox_pod_status_and_actor_finalization_keep_service_reconcile_queued() {
        let store = FakeAppliedPodStore::default();
        store.insert(
            "v1",
            "Service",
            Some("default"),
            "web",
            json!({
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {
                    "namespace": "default",
                    "name": "web",
                    "uid": "svc-web-uid"
                },
                "spec": {
                    "selector": {"app": "web"},
                    "ports": [{"name": "http", "port": 80, "targetPort": 9376, "protocol": "TCP"}]
                }
            }),
        );
        let service_key = ReconcileKey::namespaced("v1", "Service", "default", "web");
        let resource = test_resource! {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "web-pod".to_string(),
            resource_version: 2,
            data: json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "web-pod",
                    "uid": "pod-web-uid",
                    "labels": {"app": "web"}
                },
                "spec": {
                    "containers": [{
                        "name": "web",
                        "image": "agnhost",
                        "ports": [{"name": "http", "containerPort": 9376}]
                    }]
                },
                "status": {
                    "phase": "Running",
                    "podIP": "10.50.1.2",
                    "conditions": [{"type": "Ready", "status": "True"}],
                    "containerStatuses": [{"name": "web", "ready": true, "restartCount": 0}]
                }
            }),
        };

        let cases = [
            (
                "status",
                StorageCommand::UpdateStatus {
                    api_version: "v1".to_string(),
                    kind: "Pod".to_string(),
                    namespace: Some("default".to_string()),
                    name: "web-pod".to_string(),
                    status: json!({"phase": "Running"}),
                    expected_rv: None,
                    preconditions: ResourcePreconditions {
                        uid: Some("pod-web-uid".to_string()),
                        resource_version: None,
                    },
                    observed_status_stamp: None,
                },
                klights_cluster_core::PodEndpointEffect::Changed,
            ),
            (
                "actor finalization",
                StorageCommand::FinalizeBoundPod {
                    namespace: "default".to_string(),
                    name: "web-pod".to_string(),
                    pod_uid: "pod-web-uid".to_string(),
                    node_name: "worker-a".to_string(),
                    observed_resource_version: 1,
                },
                klights_cluster_core::PodEndpointEffect::NotApplicable,
            ),
        ];

        for (case, command, endpoint_effect) in cases {
            let dispatcher = RecordingReconcileSink::default();
            enqueue_pod_status_side_effects_with_endpoint_change(
                Some(&dispatcher),
                Some(&dispatcher),
                &command,
                Some(&resource),
                endpoint_effect,
                &stores(&store),
            )
            .await;

            assert_eq!(
                dispatcher
                    .queued()
                    .iter()
                    .filter(|key| *key == &service_key)
                    .count(),
                1,
                "{case} must leave one fresh Service endpoint key queued"
            );
        }
    }

    #[tokio::test]
    async fn pod_label_patch_enqueues_matching_and_stale_targetref_services_only() {
        let store = FakeAppliedPodStore::default();
        for (name, selector) in [("matching", "new"), ("stale", "old")] {
            store.insert(
                "v1",
                "Service",
                Some("default"),
                name,
                json!({
                    "apiVersion": "v1",
                    "kind": "Service",
                    "metadata": {"namespace": "default", "name": name},
                    "spec": {"selector": {"app": selector}, "ports": [{"port": 80}]}
                }),
            );
        }
        store.insert(
            "v1",
            "Endpoints",
            Some("default"),
            "stale",
            json!({
                "apiVersion": "v1",
                "kind": "Endpoints",
                "metadata": {"namespace": "default", "name": "stale"},
                "subsets": [{"addresses": [{
                    "ip": "10.42.0.2",
                    "targetRef": {"kind": "Pod", "namespace": "default", "name": "web", "uid": "pod-uid"}
                }]}]
            }),
        );
        let dispatcher = RecordingReconcileSink::default();
        let command = StorageCommand::PatchResource {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "web".to_string(),
            patch_kind: PatchKind::Merge,
            patch: json!({"metadata": {"labels": {"app": "new"}}}),
            preconditions: ResourcePreconditions {
                uid: Some("pod-uid".to_string()),
                resource_version: None,
            },
            strict_resource_version: false,
        };
        let resource = test_resource! {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "web".to_string(),
            resource_version: 2,
            data: json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"namespace": "default", "name": "web", "uid": "pod-uid", "labels": {"app": "new"}},
                "status": {"podIP": "10.42.0.2"}
            }),
        };

        enqueue_pod_status_side_effects_with_endpoint_change(
            Some(&dispatcher),
            Some(&dispatcher),
            &command,
            Some(&resource),
            klights_cluster_core::PodEndpointEffect::NotApplicable,
            &stores(&store),
        )
        .await;
        let keys = dispatcher.queued();
        assert!(
            keys.iter()
                .any(|key| key.kind() == "Service" && key.name() == "matching")
        );
        assert!(
            keys.iter()
                .any(|key| key.kind() == "Service" && key.name() == "stale")
        );
        assert!(keys.iter().all(|key| key.kind() == "Service"));
    }

    #[tokio::test]
    async fn unrelated_pod_status_fields_do_not_enqueue_service_reconcile() {
        let store = FakeAppliedPodStore::default();
        store.insert(
            "v1",
            "Service",
            Some("default"),
            "web",
            json!({
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {"namespace": "default", "name": "web"},
                "spec": {"selector": {"app": "web"}}
            }),
        );
        let dispatcher = RecordingReconcileSink::default();
        let command = StorageCommand::UpdateStatus {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "web-pod".to_string(),
            status: json!({"containerStatuses": [{
                "name": "web",
                "ready": true,
                "restartCount": 1
            }]}),
            expected_rv: None,
            preconditions: ResourcePreconditions {
                uid: Some("pod-web-uid".to_string()),
                resource_version: None,
            },
            observed_status_stamp: None,
        };
        let resource = test_resource! {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "web-pod".to_string(),
            resource_version: 2,
            data: json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "web-pod",
                    "uid": "pod-web-uid",
                    "labels": {"app": "web"}
                },
                "status": {
                    "phase": "Running",
                    "podIP": "10.42.0.8",
                    "conditions": [{"type": "Ready", "status": "True"}]
                }
            }),
        };

        enqueue_pod_status_side_effects_with_endpoint_change(
            Some(&dispatcher),
            Some(&dispatcher),
            &command,
            Some(&resource),
            klights_cluster_core::PodEndpointEffect::Unchanged,
            &stores(&store),
        )
        .await;

        let repeated_full_status = StorageCommand::UpdateStatus {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "web-pod".to_string(),
            status: resource.data["status"].clone(),
            expected_rv: None,
            preconditions: ResourcePreconditions {
                uid: Some("pod-web-uid".to_string()),
                resource_version: None,
            },
            observed_status_stamp: None,
        };
        enqueue_pod_status_side_effects_with_endpoint_change(
            Some(&dispatcher),
            Some(&dispatcher),
            &repeated_full_status,
            Some(&resource),
            klights_cluster_core::PodEndpointEffect::Unchanged,
            &stores(&store),
        )
        .await;

        assert!(
            dispatcher
                .queued()
                .iter()
                .all(|key| key.kind() != "Service"),
            "unrelated or repeated unchanged status is not endpoint-relevant Service work"
        );
    }
}
