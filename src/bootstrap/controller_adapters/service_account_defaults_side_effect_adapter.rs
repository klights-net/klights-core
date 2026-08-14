use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use klights_cluster_store::{ClusterResourceRead, ClusterTopologyRead};
use klights_controllers::namespace;
use klights_controllers::side_effects::service_account_defaults::DefaultServiceAccountPort;

struct RootDefaultServiceAccountPort {
    store: crate::bootstrap::composition_adapters::leader_bootstrap_store_adapter::LeaderBootstrapStore,
    identity: Arc<dyn klights_controllers::ControllerIdentityGenerator>,
}

#[async_trait]
impl DefaultServiceAccountPort for RootDefaultServiceAccountPort {
    async fn ensure_default_service_account(&self, namespace: &str) -> Result<()> {
        namespace::reconcile_default_service_account_at(
            &self.store,
            namespace,
            chrono::Utc::now(),
            self.identity.as_ref(),
        )
        .await
    }
}

pub(crate) fn port(
    resource_reads: Arc<dyn ClusterResourceRead>,
    topology_reads: Arc<dyn ClusterTopologyRead>,
    commands: Arc<dyn klights_leader_api::LeaderResourceCommand>,
    identity: Arc<dyn klights_controllers::ControllerIdentityGenerator>,
) -> Arc<dyn DefaultServiceAccountPort> {
    Arc::new(RootDefaultServiceAccountPort {
        store: crate::bootstrap::composition_adapters::leader_bootstrap_store_adapter::LeaderBootstrapStore::new(
            resource_reads,
            topology_reads,
            commands,
        ),
        identity,
    })
}

#[cfg(test)]
struct DirectDefaultServiceAccountPort {
    store: DirectNamespaceBootstrapStore,
    identity: Arc<dyn klights_controllers::ControllerIdentityGenerator>,
}

#[cfg(test)]
struct DirectNamespaceBootstrapStore {
    db: crate::datastore::DatastoreHandle,
    commands: Arc<dyn klights_leader_api::LeaderResourceCommand>,
}

#[cfg(test)]
impl DirectNamespaceBootstrapStore {
    async fn get(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> klights_reconcile_api::ControllerStoreResult<Option<klights_cluster_core::Resource>> {
        self.db
            .get_resource(api_version, kind, namespace, name)
            .await
            .map_err(|error| {
                klights_reconcile_api::ControllerStoreError::unavailable(error.to_string())
            })
    }
    async fn create(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: serde_json::Value,
    ) -> klights_reconcile_api::ControllerStoreResult<klights_cluster_core::Resource> {
        let request = klights_leader_api::ResourceCommandRequest::try_new(
            klights_cluster_core::StorageCommand::CreateResource {
                api_version: api_version.to_string(),
                kind: kind.to_string(),
                namespace: namespace.map(str::to_string),
                name: name.to_string(),
                data,
            },
        )
        .map_err(|error| {
            klights_reconcile_api::ControllerStoreError::internal(error.to_string())
        })?;
        match self
            .commands
            .submit_resource_command(request)
            .await
            .map_err(|error| {
                klights_reconcile_api::ControllerStoreError::unavailable(error.to_string())
            })? {
            klights_leader_api::ResourceCommandResult::Resource(resource) => Ok(resource),
            klights_leader_api::ResourceCommandResult::Ack { .. } => {
                Err(klights_reconcile_api::ControllerStoreError::internal(
                    "bootstrap create returned acknowledgement",
                ))
            }
        }
    }
}

#[cfg(test)]
#[async_trait]
impl klights_controllers::namespace::NamespaceBootstrapStore for DirectNamespaceBootstrapStore {
    async fn get_namespace(
        &self,
        name: &str,
    ) -> klights_reconcile_api::ControllerStoreResult<Option<klights_cluster_core::Resource>> {
        self.get("v1", "Namespace", None, name).await
    }
    async fn create_namespace(
        &self,
        name: &str,
        value: serde_json::Value,
    ) -> klights_reconcile_api::ControllerStoreResult<klights_cluster_core::Resource> {
        self.create("v1", "Namespace", None, name, value).await
    }
    async fn get_default_service_account(
        &self,
        namespace: &str,
    ) -> klights_reconcile_api::ControllerStoreResult<Option<klights_cluster_core::Resource>> {
        self.get("v1", "ServiceAccount", Some(namespace), "default")
            .await
    }
    async fn create_default_service_account(
        &self,
        namespace: &str,
        value: serde_json::Value,
    ) -> klights_reconcile_api::ControllerStoreResult<klights_cluster_core::Resource> {
        self.create("v1", "ServiceAccount", Some(namespace), "default", value)
            .await
    }
    async fn get_configmap(
        &self,
        namespace: &str,
        name: &str,
    ) -> klights_reconcile_api::ControllerStoreResult<Option<klights_cluster_core::Resource>> {
        self.get("v1", "ConfigMap", Some(namespace), name).await
    }
    async fn create_configmap(
        &self,
        namespace: &str,
        name: &str,
        value: serde_json::Value,
    ) -> klights_reconcile_api::ControllerStoreResult<klights_cluster_core::Resource> {
        self.create("v1", "ConfigMap", Some(namespace), name, value)
            .await
    }
    async fn update_configmap(
        &self,
        namespace: &str,
        name: &str,
        value: serde_json::Value,
        _expected_resource_version: i64,
    ) -> klights_reconcile_api::ControllerStoreResult<klights_cluster_core::Resource> {
        self.create("v1", "ConfigMap", Some(namespace), name, value)
            .await
    }
}

#[cfg(test)]
#[async_trait]
impl DefaultServiceAccountPort for DirectDefaultServiceAccountPort {
    async fn ensure_default_service_account(&self, namespace: &str) -> Result<()> {
        namespace::reconcile_default_service_account_at(
            &self.store,
            namespace,
            chrono::Utc::now(),
            self.identity.as_ref(),
        )
        .await
    }
}

#[cfg(test)]
pub(crate) fn port_for_test(
    db: crate::datastore::DatastoreHandle,
    commands: Arc<dyn klights_leader_api::LeaderResourceCommand>,
    identity: Arc<dyn klights_controllers::ControllerIdentityGenerator>,
) -> Arc<dyn DefaultServiceAccountPort> {
    Arc::new(DirectDefaultServiceAccountPort {
        store: DirectNamespaceBootstrapStore { db, commands },
        identity,
    })
}
