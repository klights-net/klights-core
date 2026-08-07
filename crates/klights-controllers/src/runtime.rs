//! Engine-neutral controller trait, context, and wrapper helpers.

use std::fmt::Debug;

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;

use crate::ControllerRuntimeDependencies;

#[derive(Clone)]
pub(crate) struct Context {
    dependencies: ControllerRuntimeDependencies,
    reconcile_time: chrono::DateTime<chrono::Utc>,
}

impl Context {
    pub(crate) fn new(
        dependencies: ControllerRuntimeDependencies,
        reconcile_time: chrono::DateTime<chrono::Utc>,
    ) -> Self {
        Self {
            dependencies,
            reconcile_time,
        }
    }

    pub(crate) fn deployment_store(&self) -> &dyn crate::deployment::DeploymentStore {
        self.dependencies.deployment_store.as_ref()
    }

    pub(crate) fn replicaset_store(&self) -> &dyn crate::replicaset::ReplicaSetStore {
        self.dependencies.replicaset_store.as_ref()
    }

    pub(crate) fn statefulset_store(&self) -> &dyn crate::statefulset::StatefulSetStore {
        self.dependencies.statefulset_store.as_ref()
    }

    pub(crate) fn daemonset_store(&self) -> &dyn crate::daemonset::DaemonSetStore {
        self.dependencies.daemonset_store.as_ref()
    }

    pub(crate) fn job_store(&self) -> &dyn crate::job::JobStore {
        self.dependencies.job_store.as_ref()
    }

    pub(crate) fn service_store(&self) -> &dyn crate::service::ServiceControllerStore {
        self.dependencies.service_store.as_ref()
    }

    pub(crate) fn pvc_store(&self) -> &dyn crate::pvc::PvcStore {
        self.dependencies.pvc_store.as_ref()
    }

    pub(crate) fn pdb_store(&self) -> &dyn crate::pdb::PdbStore {
        self.dependencies.pdb_store.as_ref()
    }

    pub(crate) fn replicationcontroller_store(
        &self,
    ) -> &dyn crate::replicationcontroller::ReplicationControllerStore {
        self.dependencies.replicationcontroller_store.as_ref()
    }

    pub(crate) fn apiservice_store(&self) -> &dyn crate::apiservice::ApiServiceStore {
        self.dependencies.apiservice_store.as_ref()
    }

    pub(crate) fn csr_status_store(&self) -> &dyn crate::csr_signer::CsrStatusStore {
        self.dependencies.csr_status_store.as_ref()
    }

    pub(crate) fn pod_query(&self) -> &dyn klights_pod_api::PodQuery {
        self.dependencies.pod_query.as_ref()
    }

    pub(crate) fn deployment_mutation(&self) -> &dyn crate::DeploymentControllerPodMutation {
        self.dependencies.deployment_pod_mutation.as_ref()
    }

    pub(crate) fn replicaset_mutation(&self) -> &dyn crate::replicaset::ReplicaSetPodMutation {
        self.dependencies.replicaset_pod_mutation.as_ref()
    }

    pub(crate) fn statefulset_mutation(&self) -> &dyn crate::statefulset::StatefulSetPodMutation {
        self.dependencies.statefulset_pod_mutation.as_ref()
    }

    pub(crate) fn daemonset_mutation(&self) -> &dyn crate::daemonset::DaemonSetPodMutation {
        self.dependencies.daemonset_pod_mutation.as_ref()
    }

    pub(crate) fn job_mutation(&self) -> &dyn crate::job::JobPodMutation {
        self.dependencies.job_pod_mutation.as_ref()
    }

    pub(crate) fn replicationcontroller_mutation(
        &self,
    ) -> &dyn crate::replicationcontroller::ReplicationControllerPodMutation {
        self.dependencies
            .replicationcontroller_pod_mutation
            .as_ref()
    }

    pub(crate) fn pod_delete_sink(&self) -> &dyn klights_reconcile_api::GcPodDeleteSink {
        self.dependencies.pod_delete_sink.as_ref()
    }

    pub(crate) fn reconcile_port(&self) -> &dyn crate::ControllerReconcilePort {
        self.dependencies.reconcile.as_ref()
    }

    pub(crate) fn network(&self) -> &dyn crate::ControllerNetworkPort {
        self.dependencies.network.as_ref()
    }

    pub(crate) fn effects(&self) -> &dyn crate::ControllerEffectPort {
        self.dependencies.effects.as_ref()
    }

    pub(crate) fn coordination(&self) -> &crate::ControllerCoordination {
        self.dependencies.coordination.as_ref()
    }

    pub(crate) fn node_name(&self) -> &str {
        &self.dependencies.node_name
    }

    pub(crate) fn reconcile_time(&self) -> chrono::DateTime<chrono::Utc> {
        self.reconcile_time
    }
}

impl Debug for Context {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Context")
            .field("node_name", &self.dependencies.node_name)
            .field(
                "focused_dependencies",
                &[
                    "leader",
                    "pods",
                    "reconcile",
                    "network",
                    "effects",
                    "coordination",
                ],
            )
            .finish()
    }
}

#[async_trait]
pub(crate) trait Controller: Send + Sync {
    fn name(&self) -> &'static str;
    async fn reconcile(&self, resource: Value, context: Context) -> Result<()>;
}

macro_rules! controller_wrapper {
    (
        $struct_name:ident, $name:literal, $core_fn:path,
        with_node, with_pod_repository,
        store = $store:ident, reader = $reader:ident, mutation = $mutation:ident
    ) => {
        pub struct $struct_name {
            identity: ::std::sync::Arc<dyn $crate::ControllerIdentityGenerator>,
        }

        impl $struct_name {
            pub(crate) fn new(
                identity: ::std::sync::Arc<dyn $crate::ControllerIdentityGenerator>,
            ) -> Self {
                Self { identity }
            }
        }

        #[::async_trait::async_trait]
        impl $crate::Controller for $struct_name {
            fn name(&self) -> &'static str {
                $name
            }

            async fn reconcile(
                &self,
                resource: ::serde_json::Value,
                context: $crate::Context,
            ) -> ::anyhow::Result<()> {
                $core_fn(
                    context.$store(),
                    context.$reader(),
                    context.$mutation(),
                    self.identity.as_ref(),
                    context.pod_delete_sink(),
                    context.reconcile_port().non_pod_finalization(),
                    &resource,
                    $crate::ControllerReconcileContext::at(
                        context.coordination(),
                        context.node_name(),
                        context.reconcile_time(),
                    ),
                )
                .await
            }
        }
    };
    ($struct_name:ident, $name:literal, $core_fn:path, no_node, store = $store:ident) => {
        pub struct $struct_name;

        #[::async_trait::async_trait]
        impl $crate::Controller for $struct_name {
            fn name(&self) -> &'static str {
                $name
            }

            async fn reconcile(
                &self,
                resource: ::serde_json::Value,
                context: $crate::Context,
            ) -> ::anyhow::Result<()> {
                $core_fn(context.$store(), &resource).await
            }
        }
    };
    (
        $struct_name:ident, $name:literal, $core_fn:path,
        no_node, discard, with_file_process, store = $store:ident
    ) => {
        pub struct $struct_name;

        #[::async_trait::async_trait]
        impl $crate::Controller for $struct_name {
            fn name(&self) -> &'static str {
                $name
            }

            async fn reconcile(
                &self,
                resource: ::serde_json::Value,
                context: $crate::Context,
            ) -> ::anyhow::Result<()> {
                $core_fn(
                    context.effects().file_process(),
                    context.effects().local_path_provisioner_root(),
                    context.$store(),
                    &resource,
                )
                .await
                .map(|_| ())
            }
        }
    };
    (
        $struct_name:ident, $name:literal, $core_fn:path,
        no_node, with_pod_repository,
        store = $store:ident, mutation = $mutation:ident
    ) => {
        pub struct $struct_name {
            identity: ::std::sync::Arc<dyn $crate::ControllerIdentityGenerator>,
        }

        impl $struct_name {
            pub(crate) fn new(
                identity: ::std::sync::Arc<dyn $crate::ControllerIdentityGenerator>,
            ) -> Self {
                Self { identity }
            }
        }

        #[::async_trait::async_trait]
        impl $crate::Controller for $struct_name {
            fn name(&self) -> &'static str {
                $name
            }

            async fn reconcile(
                &self,
                resource: ::serde_json::Value,
                context: $crate::Context,
            ) -> ::anyhow::Result<()> {
                $core_fn(
                    context.$store(),
                    context.pod_query(),
                    context.$mutation(),
                    self.identity.as_ref(),
                    context.pod_delete_sink(),
                    context.reconcile_port().non_pod_finalization(),
                    context.coordination(),
                    &resource,
                    context.reconcile_time(),
                )
                .await
            }
        }
    };
    (
        $struct_name:ident, $name:literal, $core_fn:path,
        no_node, with_pod_reader, store = $store:ident, reader = $reader:ident
    ) => {
        pub struct $struct_name;

        #[::async_trait::async_trait]
        impl $crate::Controller for $struct_name {
            fn name(&self) -> &'static str {
                $name
            }

            async fn reconcile(
                &self,
                resource: ::serde_json::Value,
                context: $crate::Context,
            ) -> ::anyhow::Result<()> {
                $core_fn(context.$store(), context.$reader(), &resource).await
            }
        }
    };
    (
        $struct_name:ident, $name:literal, $core_fn:path,
        with_node, discard, with_pod_repository,
        store = $store:ident, mutation = $mutation:ident
    ) => {
        pub struct $struct_name;

        #[::async_trait::async_trait]
        impl $crate::Controller for $struct_name {
            fn name(&self) -> &'static str {
                $name
            }

            async fn reconcile(
                &self,
                resource: ::serde_json::Value,
                context: $crate::Context,
            ) -> ::anyhow::Result<()> {
                $core_fn(
                    context.$store(),
                    context.pod_query(),
                    context.$mutation(),
                    context.pod_delete_sink(),
                    context.reconcile_port().non_pod_finalization(),
                    &resource,
                    $crate::ControllerReconcileContext::at(
                        context.coordination(),
                        context.node_name(),
                        context.reconcile_time(),
                    ),
                )
                .await
                .map(|_| ())
            }
        }
    };
}
pub(crate) use controller_wrapper;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn controller_is_object_safe_and_context_is_clone() {
        fn assert_controller(_: Option<std::sync::Arc<dyn Controller>>) {}
        fn assert_clone<T: Clone>() {}
        assert_controller(None);
        assert_clone::<Context>();
    }
}
