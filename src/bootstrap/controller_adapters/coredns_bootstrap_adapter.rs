use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::Resource;
use serde_json::Value;

use crate::bootstrap::controller_adapters::controller_store_error_adapter::map_controller_store_error;
use crate::datastore::DatastoreBackend;
use klights_controllers::coredns::{
    CoreDnsBootstrapStore, CoreDnsResourceKind, bootstrap_coredns_with_store,
};

#[cfg(test)]
#[path = "../../controller_policy_tests/coredns.rs"]
mod policy_tests;

struct CoreDnsBootstrapAdapter<'a> {
    db: &'a dyn DatastoreBackend,
    pod_reader: Arc<dyn klights_controllers::DeploymentControllerPodReader>,
    pod_mutation: Arc<dyn klights_controllers::DeploymentControllerPodMutation>,
    pod_delete_sink: Arc<dyn klights_reconcile_api::GcPodDeleteSink>,
    non_pod_finalization: &'a dyn klights_reconcile_api::GcNonPodFinalizationPort,
    coordination: &'a klights_controllers::ControllerCoordination,
    identity: &'a dyn klights_controllers::ControllerIdentityGenerator,
}

#[async_trait]
impl CoreDnsBootstrapStore for CoreDnsBootstrapAdapter<'_> {
    async fn get_coredns_resource(
        &self,
        kind: CoreDnsResourceKind,
    ) -> klights_reconcile_api::ControllerStoreResult<Option<Resource>> {
        let (api_version, kind, namespace, name) = kind.coordinates();
        self.db
            .get_resource(api_version, kind, namespace, name)
            .await
            .map_err(map_controller_store_error)
    }

    async fn create_coredns_resource(
        &self,
        kind: CoreDnsResourceKind,
        value: Value,
    ) -> klights_reconcile_api::ControllerStoreResult<Resource> {
        let (api_version, kind, namespace, name) = kind.coordinates();
        self.db
            .create_resource(api_version, kind, namespace, name, value)
            .await
            .map_err(map_controller_store_error)
    }

    async fn update_coredns_resource(
        &self,
        kind: CoreDnsResourceKind,
        value: Value,
        expected_resource_version: i64,
    ) -> klights_reconcile_api::ControllerStoreResult<Resource> {
        let (api_version, kind, namespace, name) = kind.coordinates();
        self.db
            .update_resource(
                api_version,
                kind,
                namespace,
                name,
                value,
                expected_resource_version,
            )
            .await
            .map_err(map_controller_store_error)
    }

    async fn reconcile_coredns_deployment(
        &self,
        deployment: Resource,
        node_name: &str,
    ) -> klights_reconcile_api::ControllerStoreResult<()> {
        let now = chrono::Utc::now();
        let deployment = klights_controllers::resource_projection::with_resource_version(
            deployment.data,
            deployment.resource_version,
            now,
            self.identity,
        );
        klights_controllers::deployment::reconcile_deployment(
            self.db,
            self.pod_reader.as_ref(),
            self.pod_mutation.as_ref(),
            self.identity,
            self.pod_delete_sink.as_ref(),
            self.non_pod_finalization,
            &deployment,
            klights_controllers::ControllerReconcileContext::at(self.coordination, node_name, now),
        )
        .await
        .map_err(map_controller_store_error)
    }
}

pub(crate) struct CoreDnsBootstrapConfig<'a> {
    pub(crate) tls_port: u16,
    pub(crate) service_cidr: &'a str,
    pub(crate) containerd_namespace: &'a str,
    pub(crate) node_name: &'a str,
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn bootstrap_coredns(
    db: &dyn DatastoreBackend,
    pod_reader: Arc<dyn klights_controllers::DeploymentControllerPodReader>,
    pod_mutation: Arc<dyn klights_controllers::DeploymentControllerPodMutation>,
    pod_delete_sink: Arc<dyn klights_reconcile_api::GcPodDeleteSink>,
    non_pod_finalization: &dyn klights_reconcile_api::GcNonPodFinalizationPort,
    coordination: &klights_controllers::ControllerCoordination,
    identity: &dyn klights_controllers::ControllerIdentityGenerator,
    config: CoreDnsBootstrapConfig<'_>,
) -> Result<()> {
    bootstrap_coredns_with_store(
        &CoreDnsBootstrapAdapter {
            db,
            pod_reader,
            pod_mutation,
            pod_delete_sink,
            non_pod_finalization,
            coordination,
            identity,
        },
        config.tls_port,
        config.service_cidr,
        config.containerd_namespace,
        config.node_name,
    )
    .await
}
