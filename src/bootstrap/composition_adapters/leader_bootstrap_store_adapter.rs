//! Focused proposal-backed stores for cluster bootstrap reconciliation.

use std::sync::Arc;

use async_trait::async_trait;
use klights_auth::bootstrap_token::BootstrapTokenScopePolicy;
use klights_cluster_core::{Resource, ResourcePreconditions, StorageCommand};
use klights_cluster_store::ResourceListOptions;
use klights_controllers::kube_service::KubernetesBootstrapStore;
use klights_controllers::namespace::NamespaceBootstrapStore;
use klights_controllers::rbac_reconcile::RbacPolicyStore;
use klights_leader_api::{
    LeaderResourceCommand, ResourceCommandError, ResourceCommandRequest, ResourceCommandResult,
};
use klights_reconcile_api::{ControllerStoreError, ControllerStoreResult};

use crate::datastore::DatastoreHandle;

/// Bootstrap-only read/command composition. Reads use the committed local
/// state machine; every mutation crosses the leader command boundary and is
/// therefore committed by Raft before it is observed locally.
pub(crate) struct LeaderBootstrapStore {
    reads: DatastoreHandle,
    commands: Arc<dyn LeaderResourceCommand>,
}

impl LeaderBootstrapStore {
    pub(crate) fn new(reads: DatastoreHandle, commands: Arc<dyn LeaderResourceCommand>) -> Self {
        Self { reads, commands }
    }

    async fn submit(&self, command: StorageCommand) -> ControllerStoreResult<Resource> {
        let request = ResourceCommandRequest::try_new(command).map_err(map_command_error)?;
        match self
            .commands
            .submit_resource_command(request)
            .await
            .map_err(map_command_error)?
        {
            ResourceCommandResult::Resource(resource) => Ok(resource),
            ResourceCommandResult::Ack { .. } => Err(ControllerStoreError::internal(
                "bootstrap resource mutation returned an acknowledgement without a resource",
            )),
        }
    }

    async fn get(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> ControllerStoreResult<Option<Resource>> {
        self.reads
            .get_resource(api_version, kind, namespace, name)
            .await
            .map_err(|error| ControllerStoreError::unavailable(format!("{error:#}")))
    }

    async fn create(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        value: serde_json::Value,
    ) -> ControllerStoreResult<Resource> {
        self.submit(StorageCommand::CreateResource {
            api_version: api_version.to_string(),
            kind: kind.to_string(),
            namespace: namespace.map(str::to_string),
            name: name.to_string(),
            data: value,
        })
        .await
    }

    async fn update(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        value: serde_json::Value,
        expected_resource_version: i64,
    ) -> ControllerStoreResult<Resource> {
        self.submit(StorageCommand::UpdateResource {
            api_version: api_version.to_string(),
            kind: kind.to_string(),
            namespace: namespace.map(str::to_string),
            name: name.to_string(),
            data: value,
            expected_rv: expected_resource_version,
            preconditions: ResourcePreconditions::resource_version(expected_resource_version),
            preserve_status: false,
        })
        .await
    }
}

#[async_trait]
impl klights_kubelet::node_registration::NodeRegistrationStore for LeaderBootstrapStore {
    async fn get_node(&self, node_name: &str) -> anyhow::Result<Option<Resource>> {
        self.get("v1", "Node", None, node_name)
            .await
            .map_err(anyhow::Error::new)
    }

    async fn stamp_routing_metadata(
        &self,
        node_name: &str,
        node: &mut serde_json::Value,
    ) -> anyhow::Result<bool> {
        crate::bootstrap::composition_adapters::node_routing_metadata::stamp_from_store(
            self.reads.as_ref(),
            node_name,
            node,
        )
        .await
    }

    async fn update_node(
        &self,
        node_name: &str,
        node: serde_json::Value,
        preconditions: ResourcePreconditions,
    ) -> anyhow::Result<()> {
        let expected_rv = preconditions.resource_version.unwrap_or_default();
        self.submit(StorageCommand::UpdateResource {
            api_version: "v1".into(),
            kind: "Node".into(),
            namespace: None,
            name: node_name.to_string(),
            data: node,
            expected_rv,
            preconditions,
            preserve_status: false,
        })
        .await
        .map(|_| ())
        .map_err(anyhow::Error::new)
    }

    async fn create_node(&self, node_name: &str, node: serde_json::Value) -> anyhow::Result<()> {
        self.create("v1", "Node", None, node_name, node)
            .await
            .map(|_| ())
            .map_err(anyhow::Error::new)
    }
}

#[async_trait]
impl klights_kubelet::node_leader_labels::NodeLeaderLabelStore for LeaderBootstrapStore {
    async fn list_nodes(&self) -> anyhow::Result<Vec<Resource>> {
        self.reads
            .list_resources("v1", "Node", None, ResourceListOptions::all())
            .await
            .map(|list| list.items)
    }

    async fn update_node_with_preconditions(
        &self,
        name: &str,
        data: serde_json::Value,
        preconditions: ResourcePreconditions,
    ) -> anyhow::Result<Resource> {
        let expected_rv = preconditions.resource_version.unwrap_or_default();
        self.submit(StorageCommand::UpdateResource {
            api_version: "v1".into(),
            kind: "Node".into(),
            namespace: None,
            name: name.to_string(),
            data,
            expected_rv,
            preconditions,
            preserve_status: false,
        })
        .await
        .map_err(anyhow::Error::new)
    }
}

#[async_trait]
impl crate::bootstrap::bootstrap_token::BootstrapTokenStore for LeaderBootstrapStore {
    async fn get_bootstrap_token_secret(
        &self,
        scope: klights_auth::bootstrap_token::BootstrapTokenScope,
    ) -> anyhow::Result<Option<Resource>> {
        self.get(
            "v1",
            "Secret",
            Some(klights_auth::bootstrap_token::BOOTSTRAP_TOKEN_NAMESPACE),
            scope.secret_name(),
        )
        .await
        .map_err(anyhow::Error::new)
    }

    async fn create_bootstrap_token_secret(
        &self,
        scope: klights_auth::bootstrap_token::BootstrapTokenScope,
        data: serde_json::Value,
    ) -> anyhow::Result<Resource> {
        self.create(
            "v1",
            "Secret",
            Some(klights_auth::bootstrap_token::BOOTSTRAP_TOKEN_NAMESPACE),
            scope.secret_name(),
            data,
        )
        .await
        .map_err(anyhow::Error::new)
    }

    async fn update_bootstrap_token_secret(
        &self,
        resource: &Resource,
        data: serde_json::Value,
    ) -> anyhow::Result<Resource> {
        self.update(
            "v1",
            "Secret",
            Some(klights_auth::bootstrap_token::BOOTSTRAP_TOKEN_NAMESPACE),
            &resource.name,
            data,
            resource.resource_version,
        )
        .await
        .map_err(anyhow::Error::new)
    }
}

fn map_command_error(error: ResourceCommandError) -> ControllerStoreError {
    let message = error.to_string();
    match error {
        ResourceCommandError::AlreadyExists { .. } => ControllerStoreError::already_exists(message),
        ResourceCommandError::Conflict { .. } => ControllerStoreError::conflict(message),
        ResourceCommandError::NotFound { .. } => ControllerStoreError::not_found(message),
        ResourceCommandError::NotLeader
        | ResourceCommandError::Retryable { .. }
        | ResourceCommandError::Timeout
        | ResourceCommandError::Cancelled => ControllerStoreError::unavailable(message),
        _ => ControllerStoreError::internal(message),
    }
}

#[async_trait]
impl NamespaceBootstrapStore for LeaderBootstrapStore {
    async fn get_namespace(&self, name: &str) -> ControllerStoreResult<Option<Resource>> {
        self.get("v1", "Namespace", None, name).await
    }

    async fn create_namespace(
        &self,
        name: &str,
        value: serde_json::Value,
    ) -> ControllerStoreResult<Resource> {
        self.submit(StorageCommand::CreateNamespace {
            name: name.to_string(),
            data: value,
        })
        .await
    }

    async fn get_default_service_account(
        &self,
        namespace: &str,
    ) -> ControllerStoreResult<Option<Resource>> {
        self.get("v1", "ServiceAccount", Some(namespace), "default")
            .await
    }

    async fn create_default_service_account(
        &self,
        namespace: &str,
        value: serde_json::Value,
    ) -> ControllerStoreResult<Resource> {
        self.create("v1", "ServiceAccount", Some(namespace), "default", value)
            .await
    }

    async fn get_configmap(
        &self,
        namespace: &str,
        name: &str,
    ) -> ControllerStoreResult<Option<Resource>> {
        self.get("v1", "ConfigMap", Some(namespace), name).await
    }

    async fn create_configmap(
        &self,
        namespace: &str,
        name: &str,
        value: serde_json::Value,
    ) -> ControllerStoreResult<Resource> {
        self.create("v1", "ConfigMap", Some(namespace), name, value)
            .await
    }

    async fn update_configmap(
        &self,
        namespace: &str,
        name: &str,
        value: serde_json::Value,
        expected_resource_version: i64,
    ) -> ControllerStoreResult<Resource> {
        self.update(
            "v1",
            "ConfigMap",
            Some(namespace),
            name,
            value,
            expected_resource_version,
        )
        .await
    }
}

#[async_trait]
impl RbacPolicyStore for LeaderBootstrapStore {
    async fn get_rbac_object(
        &self,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> ControllerStoreResult<Option<Resource>> {
        self.get("rbac.authorization.k8s.io/v1", kind, namespace, name)
            .await
    }

    async fn create_rbac_object(
        &self,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        value: serde_json::Value,
    ) -> ControllerStoreResult<Resource> {
        self.create("rbac.authorization.k8s.io/v1", kind, namespace, name, value)
            .await
    }

    async fn update_rbac_object(
        &self,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        value: serde_json::Value,
        expected_resource_version: i64,
    ) -> ControllerStoreResult<Resource> {
        self.update(
            "rbac.authorization.k8s.io/v1",
            kind,
            namespace,
            name,
            value,
            expected_resource_version,
        )
        .await
    }

    async fn list_cluster_roles(&self) -> ControllerStoreResult<Vec<Resource>> {
        self.reads
            .list_resources(
                "rbac.authorization.k8s.io/v1",
                "ClusterRole",
                None,
                ResourceListOptions::all(),
            )
            .await
            .map(|listing| listing.items)
            .map_err(|error| ControllerStoreError::unavailable(format!("{error:#}")))
    }
}

#[async_trait]
impl KubernetesBootstrapStore for LeaderBootstrapStore {
    async fn get_bootstrap_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> ControllerStoreResult<Option<Resource>> {
        self.get(api_version, kind, namespace, name).await
    }

    async fn create_bootstrap_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        value: serde_json::Value,
    ) -> ControllerStoreResult<Resource> {
        self.create(api_version, kind, namespace, name, value).await
    }

    async fn update_bootstrap_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        value: serde_json::Value,
        expected_resource_version: i64,
    ) -> ControllerStoreResult<Resource> {
        self.update(
            api_version,
            kind,
            namespace,
            name,
            value,
            expected_resource_version,
        )
        .await
    }
}
