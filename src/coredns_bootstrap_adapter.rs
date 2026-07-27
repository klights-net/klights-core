use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::Resource;
use serde_json::Value;

use crate::controllers::ControllerPodPort;
use crate::controllers::coredns::{
    CoreDnsBootstrapStore, CoreDnsResourceKind, bootstrap_coredns_with_store,
};
use crate::datastore::DatastoreBackend;

struct CoreDnsBootstrapAdapter<'a> {
    db: &'a dyn DatastoreBackend,
    pods: Arc<dyn ControllerPodPort>,
    non_pod_finalization: &'a dyn klights_reconcile_api::GcNonPodFinalizationPort,
    coordination: &'a crate::controllers::ControllerCoordination,
}

fn coordinates(
    kind: CoreDnsResourceKind,
) -> (
    &'static str,
    &'static str,
    Option<&'static str>,
    &'static str,
) {
    match kind {
        CoreDnsResourceKind::ServiceAccount => {
            ("v1", "ServiceAccount", Some("kube-system"), "coredns")
        }
        CoreDnsResourceKind::ClusterRole => (
            "rbac.authorization.k8s.io/v1",
            "ClusterRole",
            None,
            "system:coredns",
        ),
        CoreDnsResourceKind::ClusterRoleBinding => (
            "rbac.authorization.k8s.io/v1",
            "ClusterRoleBinding",
            None,
            "system:coredns",
        ),
        CoreDnsResourceKind::ConfigMap => ("v1", "ConfigMap", Some("kube-system"), "coredns"),
        CoreDnsResourceKind::Deployment => {
            ("apps/v1", "Deployment", Some("kube-system"), "coredns")
        }
        CoreDnsResourceKind::Service => ("v1", "Service", Some("kube-system"), "kube-dns"),
    }
}

#[async_trait]
impl CoreDnsBootstrapStore for CoreDnsBootstrapAdapter<'_> {
    async fn get_coredns_resource(&self, kind: CoreDnsResourceKind) -> Result<Option<Resource>> {
        let (api_version, kind, namespace, name) = coordinates(kind);
        self.db
            .get_resource(api_version, kind, namespace, name)
            .await
    }

    async fn create_coredns_resource(
        &self,
        kind: CoreDnsResourceKind,
        value: Value,
    ) -> Result<Resource> {
        let (api_version, kind, namespace, name) = coordinates(kind);
        self.db
            .create_resource(api_version, kind, namespace, name, value)
            .await
    }

    async fn update_coredns_resource(
        &self,
        kind: CoreDnsResourceKind,
        value: Value,
        expected_resource_version: i64,
    ) -> Result<Resource> {
        let (api_version, kind, namespace, name) = coordinates(kind);
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
    }

    async fn reconcile_coredns_deployment(
        &self,
        deployment: Resource,
        node_name: &str,
    ) -> Result<()> {
        let deployment = crate::controllers::resource_projection::with_resource_version(
            deployment.data,
            deployment.resource_version,
        );
        crate::controllers::deployment::reconcile_deployment(
            self.db,
            self.pods.deployment_reader(),
            self.pods.deployment_mutation(),
            self.pods.delete_sink(),
            self.non_pod_finalization,
            &deployment,
            crate::controllers::ControllerReconcileContext::new(self.coordination, node_name),
        )
        .await
    }
}

pub(crate) struct CoreDnsBootstrapConfig<'a> {
    pub(crate) tls_port: u16,
    pub(crate) service_cidr: &'a str,
    pub(crate) containerd_namespace: &'a str,
    pub(crate) node_name: &'a str,
}

pub(crate) async fn bootstrap_coredns(
    db: &dyn DatastoreBackend,
    pods: Arc<dyn ControllerPodPort>,
    non_pod_finalization: &dyn klights_reconcile_api::GcNonPodFinalizationPort,
    coordination: &crate::controllers::ControllerCoordination,
    config: CoreDnsBootstrapConfig<'_>,
) -> Result<()> {
    bootstrap_coredns_with_store(
        &CoreDnsBootstrapAdapter {
            db,
            pods,
            non_pod_finalization,
            coordination,
        },
        config.tls_port,
        config.service_cidr,
        config.containerd_namespace,
        config.node_name,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_every_coredns_resource_to_exact_kubernetes_identity() {
        let cases = [
            (
                CoreDnsResourceKind::ServiceAccount,
                ("v1", "ServiceAccount", Some("kube-system"), "coredns"),
            ),
            (
                CoreDnsResourceKind::ClusterRole,
                (
                    "rbac.authorization.k8s.io/v1",
                    "ClusterRole",
                    None,
                    "system:coredns",
                ),
            ),
            (
                CoreDnsResourceKind::ClusterRoleBinding,
                (
                    "rbac.authorization.k8s.io/v1",
                    "ClusterRoleBinding",
                    None,
                    "system:coredns",
                ),
            ),
            (
                CoreDnsResourceKind::ConfigMap,
                ("v1", "ConfigMap", Some("kube-system"), "coredns"),
            ),
            (
                CoreDnsResourceKind::Deployment,
                ("apps/v1", "Deployment", Some("kube-system"), "coredns"),
            ),
            (
                CoreDnsResourceKind::Service,
                ("v1", "Service", Some("kube-system"), "kube-dns"),
            ),
        ];
        for (kind, expected) in cases {
            assert_eq!(coordinates(kind), expected);
        }
    }
}
