#[cfg(test)]
use crate::datastore::DatastoreHandle;
use klights_cluster_store::{
    ClusterOwnershipRead, ClusterResourceRead, OwnerNameKindRequest, OwnerUidRequest,
    ResourceCollectionScope, ResourceGetRequest, ResourceListQuery, ResourceListRead,
    ResourceListRequest,
};
use std::sync::Arc;

use async_trait::async_trait;
use klights_cluster_core::{
    PatchKind, Resource, ResourceBatchOperation, ResourcePreconditions, StorageCommand,
};
use klights_reconcile_api::{ControllerStoreError, ControllerStoreResult as Result};

use crate::bootstrap::controller_adapters::controller_store_error_adapter::map_controller_store_error;
use klights_controllers::{
    ControllerEffectPort, ControllerNetworkPort, ControllerReconcilePort, ControllerResourceQuery,
};

fn validate_controller_effect() -> Result<()> {
    klights_leader_api::validate_controller_lease_if_scoped().map_err(|error| {
        ControllerStoreError::unavailable(format!("controller authority rejected effect: {error}"))
    })
}

pub(crate) struct RootControllerLeaderPort {
    #[cfg(test)]
    store: Option<DatastoreHandle>,
    resource_reads: Option<Arc<dyn ClusterResourceRead>>,
    ownership_reads: Option<Arc<dyn ClusterOwnershipRead>>,
    commands: Arc<dyn klights_leader_api::LeaderResourceCommand>,
}

impl RootControllerLeaderPort {
    #[cfg(test)]
    pub(crate) fn new(store: DatastoreHandle) -> Self {
        let commands = Self::resource_commands_for_test(store.clone());
        Self::new_for_test_with_commands(store, commands)
    }

    #[cfg(test)]
    pub(crate) fn resource_commands_for_test(
        store: DatastoreHandle,
    ) -> Arc<dyn klights_leader_api::LeaderResourceCommand> {
        let authority =
            crate::bootstrap::composition_adapters::authority_adapter::always_leader_authority();
        let query = crate::bootstrap::composition_adapters::resource_query_adapter::DatastoreResourceQueryAdapter::new(store.clone(), authority.clone());
        Arc::new(
            klights_replication::leader_api::EmbeddedLeaderResourceCommand::new(
                Arc::new(
                    crate::bootstrap::outbox_apply_adapter::BackendProposalFixture::new(store),
                ),
                query,
                authority,
            ),
        )
    }

    #[cfg(test)]
    pub(crate) fn new_for_test_with_commands(
        store: DatastoreHandle,
        commands: Arc<dyn klights_leader_api::LeaderResourceCommand>,
    ) -> Self {
        Self {
            store: Some(store),
            resource_reads: None,
            ownership_reads: None,
            commands,
        }
    }
    pub(crate) fn new_with_commands(
        resource_reads: Arc<dyn ClusterResourceRead>,
        ownership_reads: Arc<dyn ClusterOwnershipRead>,
        commands: Arc<dyn klights_leader_api::LeaderResourceCommand>,
    ) -> Self {
        Self {
            #[cfg(test)]
            store: None,
            resource_reads: Some(resource_reads),
            ownership_reads: Some(ownership_reads),
            commands,
        }
    }

    async fn read_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<Option<Resource>> {
        #[cfg(test)]
        if let Some(store) = &self.store {
            return store
                .get_resource(api_version, kind, namespace, name)
                .await
                .map_err(map_controller_store_error);
        }
        self.resource_reads
            .as_ref()
            .expect("focused controller resource reads")
            .get_resource(ResourceGetRequest::new(
                api_version,
                kind,
                namespace.map(ToOwned::to_owned),
                name,
            ))
            .await
            .map_err(|error| map_controller_store_error(error.into()))
    }

    async fn list_resources(
        &self,
        api_version: &str,
        kind: &str,
        scope: ResourceCollectionScope,
        label_selector: Option<&str>,
    ) -> Result<Vec<Resource>> {
        #[cfg(test)]
        if let Some(store) = &self.store {
            let namespace = match &scope {
                ResourceCollectionScope::Namespace(namespace) => Some(namespace.as_str()),
                _ => None,
            };
            return store
                .list_resources(
                    api_version,
                    kind,
                    namespace,
                    klights_cluster_store::ResourceListOptions::new(
                        label_selector,
                        None,
                        None,
                        None,
                    ),
                )
                .await
                .map(|list| list.items)
                .map_err(map_controller_store_error);
        }
        match self
            .resource_reads
            .as_ref()
            .expect("focused controller resource reads")
            .list_resources(ResourceListRequest::new(
                api_version,
                kind,
                scope,
                ResourceListQuery::try_new_borrowed(
                    label_selector,
                    None,
                    None,
                    None,
                    klights_cluster_store::ResourceVersionMatch::Any,
                )
                .map_err(|error| map_controller_store_error(error.into()))?,
            ))
            .await
            .map_err(|error| map_controller_store_error(error.into()))?
        {
            ResourceListRead::Current(page) | ResourceListRead::Historical(page) => {
                Ok(page.into_items())
            }
            ResourceListRead::Expired {
                requested,
                oldest_available,
                ..
            } => Err(klights_reconcile_api::ControllerStoreError::unavailable(
                format!("LIST at resourceVersion {requested} expired before {oldest_available}"),
            )),
        }
    }

    async fn read_owned(&self, uid: &str, namespace: Option<&str>) -> Result<Vec<Resource>> {
        #[cfg(test)]
        if let Some(store) = &self.store {
            return klights_controllers::gc::GcResourceStore::find_owned_resources(
                store.as_ref(),
                uid,
                namespace,
            )
            .await;
        }
        self.ownership_reads
            .as_ref()
            .expect("focused controller ownership reads")
            .find_owned_resources(
                OwnerUidRequest::try_new(uid, namespace.map(ToOwned::to_owned))
                    .map_err(|error| map_controller_store_error(error.into()))?,
            )
            .await
            .map_err(|error| map_controller_store_error(error.into()))
    }

    async fn read_owned_empty_uid(
        &self,
        api_version: &str,
        name: &str,
        kind: &str,
        namespace: Option<&str>,
    ) -> Result<Vec<Resource>> {
        #[cfg(test)]
        if let Some(store) = &self.store {
            return klights_controllers::gc::GcResourceStore::find_owned_by_name_kind_empty_uid(
                store.as_ref(),
                api_version,
                name,
                kind,
                namespace,
            )
            .await;
        }
        self.ownership_reads
            .as_ref()
            .expect("focused controller ownership reads")
            .find_owned_by_name_kind_empty_uid(
                OwnerNameKindRequest::try_new(
                    api_version,
                    name,
                    kind,
                    namespace.map(ToOwned::to_owned),
                )
                .map_err(|error| map_controller_store_error(error.into()))?,
            )
            .await
            .map_err(|error| map_controller_store_error(error.into()))
    }

    async fn submit_resource(&self, command: StorageCommand) -> Result<Resource> {
        let request = klights_leader_api::ResourceCommandRequest::try_new(command)
            .map_err(map_resource_command_error)?;
        match self
            .commands
            .submit_resource_command(request)
            .await
            .map_err(map_resource_command_error)?
        {
            klights_leader_api::ResourceCommandResult::Resource(resource) => Ok(resource),
            klights_leader_api::ResourceCommandResult::Ack { .. } => Err(
                ControllerStoreError::internal("controller mutation returned no resource"),
            ),
        }
    }

    async fn submit_ack(&self, command: StorageCommand) -> Result<()> {
        let request = klights_leader_api::ResourceCommandRequest::try_new(command)
            .map_err(map_resource_command_error)?;
        self.commands
            .submit_resource_command(request)
            .await
            .map(|_| ())
            .map_err(map_resource_command_error)
    }
}

fn map_resource_command_error(
    error: klights_leader_api::ResourceCommandError,
) -> ControllerStoreError {
    let message = error.to_string();
    match error {
        klights_leader_api::ResourceCommandError::AlreadyExists { .. } => {
            ControllerStoreError::already_exists(message)
        }
        klights_leader_api::ResourceCommandError::Conflict { .. } => {
            ControllerStoreError::conflict(message)
        }
        klights_leader_api::ResourceCommandError::NotFound { .. } => {
            ControllerStoreError::not_found(message)
        }
        klights_leader_api::ResourceCommandError::NotLeader
        | klights_leader_api::ResourceCommandError::Retryable { .. }
        | klights_leader_api::ResourceCommandError::Timeout
        | klights_leader_api::ResourceCommandError::Cancelled => {
            ControllerStoreError::unavailable(message)
        }
        _ => ControllerStoreError::internal(message),
    }
}

#[async_trait]
impl ControllerResourceQuery for RootControllerLeaderPort {
    async fn get_reconcile_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> std::result::Result<Option<Resource>, klights_leader_api::ResourceQueryError> {
        self.read_resource(api_version, kind, namespace, name)
            .await
            .map_err(|error| {
                klights_leader_api::ResourceQueryError::retryable(format!(
                    "controller resource query failed: {error}"
                ))
            })
    }

    async fn namespace_is_terminating(
        &self,
        namespace: &str,
    ) -> std::result::Result<bool, klights_leader_api::ResourceQueryError> {
        Ok(self
            .read_resource("v1", "Namespace", None, namespace)
            .await
            .map_err(|error| {
                klights_leader_api::ResourceQueryError::retryable(format!(
                    "controller namespace query failed: {error}"
                ))
            })?
            .is_some_and(|resource| {
                resource
                    .data
                    .pointer("/metadata/deletionTimestamp")
                    .and_then(serde_json::Value::as_str)
                    .is_some()
            }))
    }
}

#[async_trait]
impl klights_controllers::gc::GcResourceStore for RootControllerLeaderPort {
    async fn list_custom_resource_definitions(&self) -> Result<Vec<Resource>> {
        self.list_resources(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            ResourceCollectionScope::Cluster,
            None,
        )
        .await
    }

    async fn get_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<Option<Resource>> {
        self.read_resource(api_version, kind, namespace, name).await
    }

    async fn update_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: serde_json::Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource> {
        validate_controller_effect()?;
        self.submit_resource(StorageCommand::UpdateResource {
            api_version: api_version.into(),
            kind: kind.into(),
            namespace: namespace.map(Into::into),
            name: name.into(),
            data,
            expected_rv: preconditions.resource_version.unwrap_or_default(),
            preconditions,
            preserve_status: false,
        })
        .await
    }

    async fn update_main_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: serde_json::Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource> {
        validate_controller_effect()?;
        self.submit_resource(StorageCommand::UpdateResource {
            api_version: api_version.into(),
            kind: kind.into(),
            namespace: namespace.map(Into::into),
            name: name.into(),
            data,
            expected_rv: preconditions.resource_version.unwrap_or_default(),
            preconditions,
            preserve_status: true,
        })
        .await
    }

    async fn find_owned_resources(
        &self,
        owner_uid: &str,
        namespace: Option<&str>,
    ) -> Result<Vec<Resource>> {
        self.read_owned(owner_uid, namespace).await
    }

    async fn find_owned_by_name_kind_empty_uid(
        &self,
        owner_api_version: &str,
        owner_name: &str,
        owner_kind: &str,
        namespace: Option<&str>,
    ) -> Result<Vec<Resource>> {
        self.read_owned_empty_uid(owner_api_version, owner_name, owner_kind, namespace)
            .await
    }
}

#[async_trait]
impl klights_controllers::replicaset::ReplicaSetStore for RootControllerLeaderPort {
    async fn get_replicaset(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        self.read_resource("apps/v1", "ReplicaSet", Some(namespace), name)
            .await
    }

    async fn update_replicaset_status(
        &self,
        resource: &Resource,
        status: serde_json::Value,
    ) -> Result<()> {
        validate_controller_effect()?;
        self.submit_ack(StorageCommand::UpdateStatus {
            api_version: "apps/v1".into(),
            kind: "ReplicaSet".into(),
            namespace: resource.namespace.clone(),
            name: resource.name.clone(),
            status,
            expected_rv: Some(resource.resource_version),
            preconditions: ResourcePreconditions::from_resource(resource),
            observed_status_stamp: None,
        })
        .await
    }
}

#[async_trait]
impl klights_controllers::deployment::DeploymentFinalizeStore for RootControllerLeaderPort {
    async fn get_deployment(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        self.read_resource("apps/v1", "Deployment", Some(namespace), name)
            .await
    }

    async fn patch_deployment_revision(
        &self,
        namespace: &str,
        name: &str,
        revision: String,
        expected_uid: String,
    ) -> Result<()> {
        validate_controller_effect()?;
        self.submit_ack(StorageCommand::PatchResource {
            api_version: "apps/v1".into(), kind: "Deployment".into(),
            namespace: Some(namespace.into()), name: name.into(), patch_kind: PatchKind::Merge,
            patch: serde_json::json!({"metadata":{"annotations":{"deployment.kubernetes.io/revision":revision}}}),
            preconditions: ResourcePreconditions::uid(expected_uid), strict_resource_version: false,
        }).await
    }

    async fn delete_replicaset(
        &self,
        namespace: &str,
        name: &str,
        expected_uid: String,
    ) -> Result<()> {
        validate_controller_effect()?;
        self.submit_ack(StorageCommand::DeleteResource {
            api_version: "apps/v1".into(),
            kind: "ReplicaSet".into(),
            namespace: Some(namespace.into()),
            name: name.into(),
            preconditions: ResourcePreconditions::uid(expected_uid),
        })
        .await
    }
}

#[async_trait]
impl klights_controllers::deployment::DeploymentStore for RootControllerLeaderPort {
    async fn list_replicasets(&self, namespace: &str) -> Result<Vec<Resource>> {
        self.list_resources(
            "apps/v1",
            "ReplicaSet",
            ResourceCollectionScope::Namespace(namespace.to_string()),
            None,
        )
        .await
    }

    async fn create_replicaset(
        &self,
        namespace: &str,
        name: &str,
        replicaset: serde_json::Value,
    ) -> Result<Resource> {
        validate_controller_effect()?;
        self.submit_resource(StorageCommand::CreateResource {
            api_version: "apps/v1".into(),
            kind: "ReplicaSet".into(),
            namespace: Some(namespace.into()),
            name: name.into(),
            data: replicaset,
        })
        .await
    }

    async fn patch_replicaset_scale(
        &self,
        namespace: &str,
        name: &str,
        patch: serde_json::Value,
        expected_uid: String,
    ) -> Result<Option<Resource>> {
        validate_controller_effect()?;
        self.submit_resource(StorageCommand::PatchResource {
            api_version: "apps/v1".into(),
            kind: "ReplicaSet".into(),
            namespace: Some(namespace.into()),
            name: name.into(),
            patch_kind: PatchKind::Merge,
            patch,
            preconditions: ResourcePreconditions::uid(expected_uid),
            strict_resource_version: false,
        })
        .await
        .map(Some)
    }

    async fn update_deployment_status(
        &self,
        resource: &Resource,
        status: serde_json::Value,
    ) -> Result<()> {
        validate_controller_effect()?;
        self.submit_ack(StorageCommand::UpdateStatus {
            api_version: "apps/v1".into(),
            kind: "Deployment".into(),
            namespace: resource.namespace.clone(),
            name: resource.name.clone(),
            status,
            expected_rv: Some(resource.resource_version),
            preconditions: ResourcePreconditions::from_resource(resource),
            observed_status_stamp: None,
        })
        .await
    }
}

#[async_trait]
impl klights_controllers::statefulset::StatefulSetStore for RootControllerLeaderPort {
    async fn get_statefulset(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        self.read_resource("apps/v1", "StatefulSet", Some(namespace), name)
            .await
    }

    async fn update_statefulset_status(
        &self,
        resource: &Resource,
        status: serde_json::Value,
    ) -> Result<()> {
        validate_controller_effect()?;
        self.submit_ack(StorageCommand::UpdateStatus {
            api_version: "apps/v1".into(),
            kind: "StatefulSet".into(),
            namespace: resource.namespace.clone(),
            name: resource.name.clone(),
            status,
            expected_rv: Some(resource.resource_version),
            preconditions: ResourcePreconditions::from_resource(resource),
            observed_status_stamp: None,
        })
        .await
    }
}

#[async_trait]
impl klights_controllers::daemonset::DaemonSetStore for RootControllerLeaderPort {
    async fn list_controller_revisions(&self, namespace: &str) -> Result<Vec<Resource>> {
        self.list_resources(
            "apps/v1",
            "ControllerRevision",
            ResourceCollectionScope::Namespace(namespace.to_string()),
            None,
        )
        .await
    }

    async fn create_controller_revision(
        &self,
        namespace: &str,
        name: &str,
        revision: serde_json::Value,
    ) -> Result<Resource> {
        validate_controller_effect()?;
        self.submit_resource(StorageCommand::CreateResource {
            api_version: "apps/v1".into(),
            kind: "ControllerRevision".into(),
            namespace: Some(namespace.into()),
            name: name.into(),
            data: revision,
        })
        .await
    }

    async fn list_nodes(&self) -> Result<Vec<Resource>> {
        self.list_resources("v1", "Node", ResourceCollectionScope::Cluster, None)
            .await
    }

    async fn update_daemonset_status(
        &self,
        resource: &Resource,
        status: serde_json::Value,
    ) -> Result<()> {
        validate_controller_effect()?;
        self.submit_ack(StorageCommand::UpdateStatus {
            api_version: "apps/v1".into(),
            kind: "DaemonSet".into(),
            namespace: resource.namespace.clone(),
            name: resource.name.clone(),
            status,
            expected_rv: Some(resource.resource_version),
            preconditions: ResourcePreconditions::from_resource(resource),
            observed_status_stamp: None,
        })
        .await
    }
}

#[async_trait]
impl klights_controllers::job::JobStore for RootControllerLeaderPort {
    async fn get_job(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        self.read_resource("batch/v1", "Job", Some(namespace), name)
            .await
    }

    async fn update_job_status(
        &self,
        resource: &Resource,
        status: serde_json::Value,
    ) -> Result<Resource> {
        validate_controller_effect()?;
        self.submit_resource(StorageCommand::UpdateStatus {
            api_version: "batch/v1".into(),
            kind: "Job".into(),
            namespace: resource.namespace.clone(),
            name: resource.name.clone(),
            status,
            expected_rv: Some(resource.resource_version),
            preconditions: ResourcePreconditions::from_resource(resource),
            observed_status_stamp: None,
        })
        .await
    }
}

#[async_trait]
impl klights_controllers::service::ServiceReconcileStore for RootControllerLeaderPort {
    async fn list_services(&self) -> Result<Vec<Resource>> {
        self.list_resources(
            "v1",
            "Service",
            ResourceCollectionScope::AllNamespaces,
            None,
        )
        .await
    }

    async fn get_service(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        self.read_resource("v1", "Service", Some(namespace), name)
            .await
    }

    async fn update_service(
        &self,
        namespace: &str,
        name: &str,
        data: serde_json::Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource> {
        validate_controller_effect()?;
        self.submit_resource(StorageCommand::UpdateResource {
            api_version: "v1".into(),
            kind: "Service".into(),
            namespace: Some(namespace.into()),
            name: name.into(),
            data,
            expected_rv: preconditions.resource_version.unwrap_or_default(),
            preconditions,
            preserve_status: false,
        })
        .await
    }
}

#[async_trait]
impl klights_controllers::endpoints::EndpointReconcileStore for RootControllerLeaderPort {
    async fn endpoint_namespace_is_terminating(&self, namespace: &str) -> Result<bool> {
        Ok(self
            .read_resource("v1", "Namespace", None, namespace)
            .await?
            .is_some_and(|resource| {
                resource
                    .data
                    .pointer("/metadata/deletionTimestamp")
                    .and_then(serde_json::Value::as_str)
                    .is_some()
            }))
    }

    async fn get_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<Option<Resource>> {
        self.read_resource(api_version, kind, namespace, name).await
    }

    async fn list_service_endpoint_slices(
        &self,
        namespace: &str,
        service_name: &str,
    ) -> Result<Vec<Resource>> {
        self.list_resources(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            ResourceCollectionScope::Namespace(namespace.to_string()),
            Some(&format!("kubernetes.io/service-name={service_name}")),
        )
        .await
    }

    async fn create_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: serde_json::Value,
    ) -> Result<Resource> {
        validate_controller_effect()?;
        self.submit_resource(StorageCommand::CreateResource {
            api_version: api_version.into(),
            kind: kind.into(),
            namespace: namespace.map(Into::into),
            name: name.into(),
            data,
        })
        .await
    }

    async fn update_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: serde_json::Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource> {
        validate_controller_effect()?;
        self.submit_resource(StorageCommand::UpdateResource {
            api_version: api_version.into(),
            kind: kind.into(),
            namespace: namespace.map(Into::into),
            name: name.into(),
            data,
            expected_rv: preconditions.resource_version.unwrap_or_default(),
            preconditions,
            preserve_status: false,
        })
        .await
    }

    async fn delete_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
    ) -> Result<()> {
        validate_controller_effect()?;
        self.submit_ack(StorageCommand::DeleteResource {
            api_version: api_version.into(),
            kind: kind.into(),
            namespace: namespace.map(Into::into),
            name: name.into(),
            preconditions,
        })
        .await
    }

    async fn apply_resource_batch(&self, operations: Vec<ResourceBatchOperation>) -> Result<()> {
        validate_controller_effect()?;
        self.submit_ack(StorageCommand::ApplyResourceBatch { operations })
            .await
    }
}

#[async_trait]
impl klights_controllers::common::ControllerStatusStore for RootControllerLeaderPort {
    async fn get_status_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<Option<Resource>> {
        self.read_resource(api_version, kind, namespace, name).await
    }

    async fn update_status(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        status: serde_json::Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource> {
        validate_controller_effect()?;
        self.submit_resource(StorageCommand::UpdateStatus {
            api_version: api_version.into(),
            kind: kind.into(),
            namespace: namespace.map(Into::into),
            name: name.into(),
            status,
            expected_rv: preconditions.resource_version,
            preconditions,
            observed_status_stamp: None,
        })
        .await
    }

    fn log_noop_status_write(
        &self,
        operation: &'static str,
        resource: &Resource,
        reason: &'static str,
    ) {
        tracing::debug!(operation, resource = %resource.name, reason, "controller status write was a no-op");
    }
}

#[async_trait]
impl klights_controllers::cronjob::CronJobStore for RootControllerLeaderPort {
    async fn get_cronjob(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        self.read_resource("batch/v1", "CronJob", Some(namespace), name)
            .await
    }

    async fn get_job(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        self.read_resource("batch/v1", "Job", Some(namespace), name)
            .await
    }

    async fn create_job(
        &self,
        namespace: &str,
        name: &str,
        value: serde_json::Value,
    ) -> Result<Resource> {
        validate_controller_effect()?;
        self.submit_resource(StorageCommand::CreateResource {
            api_version: "batch/v1".into(),
            kind: "Job".into(),
            namespace: Some(namespace.into()),
            name: name.into(),
            data: value,
        })
        .await
    }

    async fn list_jobs(&self, namespace: &str) -> Result<Vec<Resource>> {
        self.list_resources(
            "batch/v1",
            "Job",
            ResourceCollectionScope::Namespace(namespace.to_string()),
            None,
        )
        .await
    }

    async fn delete_job(
        &self,
        namespace: &str,
        name: &str,
        uid: String,
        resource_version: i64,
    ) -> Result<()> {
        validate_controller_effect()?;
        self.submit_ack(StorageCommand::DeleteResource {
            api_version: "batch/v1".into(),
            kind: "Job".into(),
            namespace: Some(namespace.into()),
            name: name.into(),
            preconditions: ResourcePreconditions::uid_and_resource_version(uid, resource_version),
        })
        .await
    }
}

#[async_trait]
impl klights_controllers::pvc::PvcStore for RootControllerLeaderPort {
    async fn get_pvc(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        self.read_resource("v1", "PersistentVolumeClaim", Some(namespace), name)
            .await
    }

    async fn list_persistent_volumes(&self) -> Result<Vec<Resource>> {
        self.list_resources(
            "v1",
            "PersistentVolume",
            ResourceCollectionScope::Cluster,
            None,
        )
        .await
    }

    async fn get_persistent_volume(&self, name: &str) -> Result<Option<Resource>> {
        self.read_resource("v1", "PersistentVolume", None, name)
            .await
    }

    async fn create_persistent_volume(
        &self,
        name: &str,
        value: serde_json::Value,
    ) -> Result<Resource> {
        validate_controller_effect()?;
        self.submit_resource(StorageCommand::CreateResource {
            api_version: "v1".into(),
            kind: "PersistentVolume".into(),
            namespace: None,
            name: name.into(),
            data: value,
        })
        .await
    }

    async fn update_persistent_volume(
        &self,
        name: &str,
        value: serde_json::Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource> {
        validate_controller_effect()?;
        self.submit_resource(StorageCommand::UpdateResource {
            api_version: "v1".into(),
            kind: "PersistentVolume".into(),
            namespace: None,
            name: name.into(),
            data: value,
            expected_rv: preconditions.resource_version.unwrap_or_default(),
            preconditions,
            preserve_status: false,
        })
        .await
    }
}

#[async_trait]
impl klights_controllers::pdb::PdbStore for RootControllerLeaderPort {
    async fn list_pdbs(&self, namespace: &str) -> Result<Vec<Resource>> {
        self.list_resources(
            "policy/v1",
            "PodDisruptionBudget",
            ResourceCollectionScope::Namespace(namespace.to_string()),
            None,
        )
        .await
    }
}

#[async_trait]
impl klights_controllers::replicationcontroller::ReplicationControllerStore
    for RootControllerLeaderPort
{
    async fn get_replication_controller(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<Resource>> {
        self.read_resource("v1", "ReplicationController", Some(namespace), name)
            .await
    }

    async fn list_resource_quotas(&self, namespace: &str) -> Result<Vec<Resource>> {
        self.list_resources(
            "v1",
            "ResourceQuota",
            ResourceCollectionScope::Namespace(namespace.to_string()),
            None,
        )
        .await
    }

    async fn update_replication_controller_status(
        &self,
        resource: &Resource,
        status: serde_json::Value,
    ) -> Result<()> {
        validate_controller_effect()?;
        self.submit_ack(StorageCommand::UpdateStatus {
            api_version: "v1".into(),
            kind: "ReplicationController".into(),
            namespace: resource.namespace.clone(),
            name: resource.name.clone(),
            status,
            expected_rv: Some(resource.resource_version),
            preconditions: ResourcePreconditions::from_resource(resource),
            observed_status_stamp: None,
        })
        .await
    }
}

#[async_trait]
impl klights_controllers::apiservice::ApiServiceStore for RootControllerLeaderPort {
    async fn get_apiservice(&self, name: &str) -> Result<Option<Resource>> {
        self.read_resource("apiregistration.k8s.io/v1", "APIService", None, name)
            .await
    }

    async fn service_exists(&self, namespace: &str, name: &str) -> Result<bool> {
        Ok(self
            .read_resource("v1", "Service", Some(namespace), name)
            .await?
            .is_some())
    }

    async fn list_endpoint_slices(
        &self,
        namespace: &str,
        service_name: &str,
    ) -> Result<Vec<Resource>> {
        self.list_resources(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            ResourceCollectionScope::Namespace(namespace.to_string()),
            Some(&format!("kubernetes.io/service-name={service_name}")),
        )
        .await
    }

    async fn get_endpoints(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        self.read_resource("v1", "Endpoints", Some(namespace), name)
            .await
    }

    async fn update_apiservice_status(
        &self,
        current: &Resource,
        status: serde_json::Value,
    ) -> Result<()> {
        validate_controller_effect()?;
        self.submit_ack(StorageCommand::UpdateStatus {
            api_version: "apiregistration.k8s.io/v1".into(),
            kind: "APIService".into(),
            namespace: None,
            name: current.name.clone(),
            status,
            expected_rv: Some(current.resource_version),
            preconditions: ResourcePreconditions::from_resource(current),
            observed_status_stamp: None,
        })
        .await
    }
}

#[async_trait]
impl klights_controllers::csr_signer::CsrStatusStore for RootControllerLeaderPort {
    async fn get_csr(&self, name: &str) -> Result<Option<Resource>> {
        self.read_resource(
            "certificates.k8s.io/v1",
            "CertificateSigningRequest",
            None,
            name,
        )
        .await
    }

    async fn update_csr_status(
        &self,
        name: &str,
        uid: &str,
        resource_version: i64,
        status: serde_json::Value,
    ) -> Result<()> {
        validate_controller_effect()?;
        self.submit_ack(StorageCommand::UpdateStatus {
            api_version: "certificates.k8s.io/v1".into(),
            kind: "CertificateSigningRequest".into(),
            namespace: None,
            name: name.into(),
            status,
            expected_rv: Some(resource_version),
            preconditions: ResourcePreconditions {
                uid: Some(uid.into()),
                resource_version: Some(resource_version),
            },
            observed_status_stamp: None,
        })
        .await
    }
}

pub(crate) struct RootControllerPodPort {
    query: Arc<dyn klights_pod_api::PodQuery>,
    update: Arc<dyn klights_pod_api::PodUpdate>,
    api: Arc<dyn klights_pod_api::PodApiMutation>,
    subresource: Arc<dyn klights_pod_api::PodSubresourceMutation>,
}

impl RootControllerPodPort {
    /// Focused-port constructor: the concrete root repository aggregate no
    /// longer exists; callers pass the query + update ports from
    /// the focused Pod composition ports.
    pub(crate) fn new(
        query: Arc<dyn klights_pod_api::PodQuery>,
        update: Arc<dyn klights_pod_api::PodUpdate>,
        api: Arc<dyn klights_pod_api::PodApiMutation>,
        subresource: Arc<dyn klights_pod_api::PodSubresourceMutation>,
    ) -> Self {
        Self {
            query,
            update,
            api,
            subresource,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(
        query: Arc<dyn klights_pod_api::PodQuery>,
        update: Arc<dyn klights_pod_api::PodUpdate>,
        api: Arc<dyn klights_pod_api::PodApiMutation>,
        subresource: Arc<dyn klights_pod_api::PodSubresourceMutation>,
    ) -> Self {
        Self::new(query, update, api, subresource)
    }

    pub(crate) async fn create_controller_pod(
        &self,
        namespace: &str,
        name: &str,
        pod: serde_json::Value,
    ) -> anyhow::Result<Resource> {
        let result = klights_pod_api::PodApiMutation::create_pod(
            self,
            klights_pod_api::PodApiCreateRequest {
                namespace: namespace.to_string(),
                body: pod,
                dry_run: false,
            },
        )
        .await
        .map_err(anyhow::Error::new)?;
        result.resource.ok_or_else(|| {
            anyhow::anyhow!("controller Pod {namespace}/{name} create returned dry-run")
        })
    }

    pub(crate) async fn replace_controller_owner_references(
        &self,
        namespace: &str,
        name: &str,
        expected_uid: Option<&str>,
        owner_references: Vec<serde_json::Value>,
    ) -> anyhow::Result<Resource> {
        let target = match expected_uid {
            Some(uid) => klights_pod_api::PodMutationTarget::try_by_identity(
                klights_types::PodIdentity::new(namespace, name, uid),
            ),
            None => klights_pod_api::PodMutationTarget::try_by_name(namespace, name),
        }
        .map_err(anyhow::Error::new)?;
        let owner_references = owner_references
            .into_iter()
            .map(|owner| {
                klights_pod_api::PodOwnerReference::try_new(
                    owner
                        .get("apiVersion")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default(),
                    owner
                        .get("kind")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default(),
                    owner
                        .get("name")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default(),
                    owner
                        .get("uid")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default(),
                    owner.get("controller").and_then(serde_json::Value::as_bool),
                    owner
                        .get("blockOwnerDeletion")
                        .and_then(serde_json::Value::as_bool),
                )
            })
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(anyhow::Error::new)?;
        klights_pod_api::PodUpdate::update_pod(
            self,
            klights_pod_api::PodUpdateRequest::replace_owner_references(target, owner_references),
        )
        .await
        .map_err(anyhow::Error::new)
    }

    pub(crate) async fn merge_controller_pod_labels(
        &self,
        namespace: &str,
        name: &str,
        expected_uid: Option<&str>,
        labels: Vec<(String, String)>,
    ) -> anyhow::Result<Resource> {
        let target = match expected_uid {
            Some(uid) => klights_pod_api::PodMutationTarget::try_by_identity(
                klights_types::PodIdentity::new(namespace, name, uid),
            ),
            None => klights_pod_api::PodMutationTarget::try_by_name(namespace, name),
        }
        .map_err(anyhow::Error::new)?;
        let labels = labels
            .into_iter()
            .map(|(key, value)| klights_pod_api::PodLabel::try_new(key, value))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(anyhow::Error::new)?;
        klights_pod_api::PodUpdate::update_pod(
            self,
            klights_pod_api::PodUpdateRequest::merge_labels(target, labels),
        )
        .await
        .map_err(anyhow::Error::new)
    }
}

impl klights_pod_api::PodUpdate for RootControllerPodPort {
    fn update_pod(
        &self,
        request: klights_pod_api::PodUpdateRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Resource> {
        Box::pin(async move {
            validate_controller_effect().map_err(|error| {
                klights_pod_api::PodRepositoryError::forbidden(error.to_string())
            })?;
            self.update.update_pod(request).await
        })
    }
}

impl klights_pod_api::PodApiMutation for RootControllerPodPort {
    fn create_pod(
        &self,
        request: klights_pod_api::PodApiCreateRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, klights_pod_api::PodApiCreateResult> {
        Box::pin(async move {
            validate_controller_effect().map_err(|error| {
                klights_pod_api::PodRepositoryError::forbidden(error.to_string())
            })?;
            self.api.create_pod(request).await
        })
    }

    fn update_pod(
        &self,
        request: klights_pod_api::PodApiUpdateRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, klights_pod_api::PodApiWriteOutcome> {
        self.api.update_pod(request)
    }

    fn patch_pod(
        &self,
        request: klights_pod_api::PodApiPatchRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, klights_pod_api::PodApiWriteOutcome> {
        self.api.patch_pod(request)
    }

    fn delete_pod(
        &self,
        request: klights_pod_api::PodApiDeleteRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, klights_pod_api::PodApiDeleteOutcome> {
        Box::pin(async move {
            validate_controller_effect().map_err(|error| {
                klights_pod_api::PodRepositoryError::forbidden(error.to_string())
            })?;
            self.api.delete_pod(request).await
        })
    }

    fn delete_collection_pods(
        &self,
        request: klights_pod_api::PodApiDeleteCollectionRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, ()> {
        self.api.delete_collection_pods(request)
    }

    fn bind_pod(
        &self,
        request: klights_pod_api::PodBindingRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, ()> {
        self.api.bind_pod(request)
    }
}

impl klights_pod_api::PodSubresourceMutation for RootControllerPodPort {
    fn replace_status(
        &self,
        request: klights_pod_api::PodStatusReplaceRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Resource> {
        self.subresource.replace_status(request)
    }

    fn patch_status(
        &self,
        request: klights_pod_api::PodStatusPatchRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Resource> {
        self.subresource.patch_status(request)
    }

    fn update_ephemeral_containers(
        &self,
        request: klights_pod_api::PodEphemeralContainersRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Resource> {
        self.subresource.update_ephemeral_containers(request)
    }
}

#[async_trait]
impl klights_controllers::node_lifecycle::NodeLifecyclePodStore for RootControllerPodPort {
    async fn list_pods_bound_to_node(&self, node_name: &str) -> Result<Vec<Resource>> {
        let field_selector = format!("spec.nodeName={node_name}");
        let request = klights_pod_api::PodListRequest::try_new(
            None,
            None,
            Some(field_selector),
            None,
            None,
        )
        .map_err(anyhow::Error::new)
        .map_err(crate::bootstrap::controller_adapters::controller_store_error_adapter::map_controller_store_error)?;
        Ok(self.query.list_pods(request)
            .await
            .map_err(anyhow::Error::new)
            .map_err(crate::bootstrap::controller_adapters::controller_store_error_adapter::map_controller_store_error)?
            .into_parts()
            .0)
    }

    async fn replace_pod_status_for_uid(
        &self,
        pod: &Resource,
        status: serde_json::Value,
    ) -> Result<Resource> {
        validate_controller_effect()?;
        self.subresource
            .replace_status(klights_pod_api::PodStatusReplaceRequest {
                namespace: pod.namespace.as_deref().unwrap_or("default").to_string(),
                name: pod.name.clone(),
                expected_uid: Some(pod.uid.clone()),
                status,
                expected_resource_version: pod.resource_version,
            })
            .await
            .map_err(anyhow::Error::new)
            .map_err(crate::bootstrap::controller_adapters::controller_store_error_adapter::map_controller_store_error)
    }
}

pub(crate) struct RootControllerReconcilePort {
    finalization: Arc<dyn klights_reconcile_api::GcNonPodFinalizationPort>,
}

impl RootControllerReconcilePort {
    pub(crate) fn new(
        finalization: Arc<dyn klights_reconcile_api::GcNonPodFinalizationPort>,
    ) -> Self {
        Self { finalization }
    }
}

impl ControllerReconcilePort for RootControllerReconcilePort {
    fn non_pod_finalization(&self) -> &dyn klights_reconcile_api::GcNonPodFinalizationPort {
        self.finalization.as_ref()
    }
}

pub(crate) struct RootControllerNetworkPort {
    services: Arc<dyn klights_network_api::ServiceRouter>,
}

impl RootControllerNetworkPort {
    pub(crate) fn new(services: Arc<dyn klights_network_api::ServiceRouter>) -> Self {
        Self { services }
    }
}

impl ControllerNetworkPort for RootControllerNetworkPort {
    fn service_router(&self) -> &dyn klights_network_api::ServiceRouter {
        self.services.as_ref()
    }
}

pub(crate) struct RootControllerEffectPort {
    file_process: klights_supervisor::FileProcessExecutor,
    local_path_provisioner_root: std::path::PathBuf,
}

impl RootControllerEffectPort {
    pub(crate) fn new(
        file_process: klights_supervisor::FileProcessExecutor,
        local_path_provisioner_root: std::path::PathBuf,
    ) -> Self {
        Self {
            file_process,
            local_path_provisioner_root,
        }
    }
}

impl ControllerEffectPort for RootControllerEffectPort {
    fn file_process(&self) -> &klights_supervisor::FileProcessExecutor {
        &self.file_process
    }

    fn local_path_provisioner_root(&self) -> &std::path::Path {
        &self.local_path_provisioner_root
    }
}

#[cfg(test)]
pub(crate) fn inject_resource_version(
    data: impl Into<Arc<serde_json::Value>>,
    resource_version: i64,
) -> serde_json::Value {
    let mut data = Arc::unwrap_or_clone(data.into());
    if let Some(metadata) = data
        .get_mut("metadata")
        .and_then(serde_json::Value::as_object_mut)
    {
        metadata.insert(
            "resourceVersion".to_string(),
            serde_json::Value::String(resource_version.to_string()),
        );
    }
    data
}

#[cfg(test)]
fn runtime_dependencies_for_test(
    db: &crate::datastore::sqlite::Datastore,
    node_name: &str,
) -> klights_controllers::ControllerRuntimeDependencies {
    let db_handle: crate::datastore::DatastoreHandle = Arc::new(db.clone());
    let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let resource_query = crate::bootstrap::composition_adapters::resource_query_adapter::DatastoreResourceQueryAdapter::new(
        db_handle.clone(),
        crate::bootstrap::composition_adapters::authority_adapter::always_leader_watch(),
    );
    let resource_commands = RootControllerLeaderPort::resource_commands_for_test(db_handle.clone());
    let (
        pod_query,
        _pod_snapshot,
        pod_update,
        _pod_status_writer,
        _pod_workqueue,
        _pod_network_assignment,
        _pod_host_ip,
        _background,
        _deletion_finalizer,
        _dirty_counter,
        _mutation_reconcile,
        gc_delete,
        _eviction_admission,
        _namespace_bootstrap,
        _namespace_termination_queue,
        _pod_api,
        _pod_subresource,
        _pod_scheduling,
        _watch_source,
        _bound_finalization,
        _deferred_runtime,
        test_api,
        test_subresource,
    ) = crate::bootstrap::pod_repository_composition::build_pod_repository_parts(
        crate::bootstrap::pod_repository_composition::PodRepositoryBuildConfig {
            resource_query,
            ownership_reads: db.focused_read_store(),
            resource_reads: db.focused_read_store(),
            namespace_content_reads: db.focused_read_store(),
            topology_reads: db.focused_read_store(),
            pod_workqueue_store: None,
            supervisor,
            side_effects: Arc::new(klights_controllers::side_effects::SideEffectRegistry::new()),
            metrics: klights_controllers::side_effects::SideEffectMetrics::new(),
            pod_network_cache: crate::bootstrap::pod_repository_composition::empty_test_pod_network_cache(),
            assignment_waiter: crate::bootstrap::pod_repository_composition::test_assignment_bus(),
            scheduling_mode: crate::bootstrap::pod_repository_composition::PodSchedulingMode::InlineSingleNode,
            outbox: None,
            cluster_api: None,
            resource_commands: Some(resource_commands),
            remote_delivery_required: false,
            controller_identity: crate::bootstrap::controller_adapters::system_identity_adapter::deterministic_controller_identity(),
            api_identity: Arc::new(k8s_native_service::test_support::admission::DeterministicApiIdentity::default()),
            scheduler_bind_gate: None,
            post_write_maintenance_notify: None,
            gc_coordination: Arc::new(klights_controllers::ControllerCoordination::new()),
        },
        None,
    );
    let leader = Arc::new(RootControllerLeaderPort::new(db_handle.clone()));
    let pods = Arc::new(RootControllerPodPort::new_for_test(
        pod_query.clone(),
        pod_update.clone(),
        test_api
            .clone()
            .expect("controller test runtime requires the root Pod API port"),
        test_subresource
            .clone()
            .expect("controller test runtime requires the root Pod subresource port"),
    ));
    let pod_mutations = Arc::new(klights_controllers::ControllerPodMutationAdapter::new(
        pods.clone(),
        pods.clone(),
    ));
    let non_pod_finalization: Arc<dyn klights_reconcile_api::GcNonPodFinalizationPort> = Arc::new(
        crate::bootstrap::controller_adapters::gc_delete_adapter::GcNonPodFinalizationAdapter::new(
            db_handle,
        ),
    );
    let services = Arc::new(klights_networking::test_support::MockServiceRouter::default());
    klights_controllers::ControllerRuntimeDependencies {
        wall_time: chrono::Utc::now,
        resource_query: leader.clone(),
        deployment_store: leader.clone(),
        replicaset_store: leader.clone(),
        statefulset_store: leader.clone(),
        daemonset_store: leader.clone(),
        job_store: leader.clone(),
        service_store: leader.clone(),
        pvc_store: leader.clone(),
        pdb_store: leader.clone(),
        replicationcontroller_store: leader.clone(),
        apiservice_store: leader.clone(),
        csr_status_store: leader,
        pod_query: pod_query.clone(),
        deployment_pod_mutation: pod_mutations.clone(),
        replicaset_pod_mutation: pod_mutations.clone(),
        statefulset_pod_mutation: pod_mutations.clone(),
        daemonset_pod_mutation: pod_mutations.clone(),
        job_pod_mutation: pod_mutations.clone(),
        replicationcontroller_pod_mutation: pod_mutations,
        pod_delete_sink: gc_delete.clone(),
        reconcile: Arc::new(RootControllerReconcilePort::new(non_pod_finalization)),
        network: Arc::new(RootControllerNetworkPort::new(services)),
        effects: Arc::new(RootControllerEffectPort::new(
            crate::bootstrap::file_blocking::test_file_process_executor(),
            crate::KlightsConfig::test_default()
                .data_root
                .join("local-path-provisioner"),
        )),
        coordination: Arc::new(klights_controllers::ControllerCoordination::new()),
        node_name: Arc::from(node_name),
    }
}

#[cfg(test)]
struct NoopHpaReconcilePort;

#[cfg(test)]
#[async_trait]
impl klights_controllers::hpa::HpaReconcilePort for NoopHpaReconcilePort {
    async fn reconcile(
        &self,
        _resource: &serde_json::Value,
        _reconcile_time: chrono::DateTime<chrono::Utc>,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
pub(crate) fn dispatcher_for_test(
    db: &crate::datastore::sqlite::Datastore,
    service_ipam: Arc<klights_controllers::service::ServiceIpam>,
) -> Arc<klights_controllers::ControllerDispatcher> {
    let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    Arc::new(klights_controllers::ControllerDispatcher::new_complete(
        service_ipam,
        Arc::new(klights_controllers::service::NodePortAllocator::new()),
        supervisor,
        None,
        Arc::new(klights_controllers::hpa::HpaController::new(Arc::new(
            NoopHpaReconcilePort,
        ))),
        runtime_dependencies_for_test(db, "test-node"),
        super::system_identity_adapter::deterministic_controller_identity(),
    ))
}
