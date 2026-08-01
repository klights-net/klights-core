//! Controller trait and related types for extensible controller implementations
//!
//! This module defines the `Controller` trait which provides a uniform interface
//! for all controllers in klights. Each controller (Deployment, StatefulSet, Service, etc.)
//! implements this trait to provide reconcile functionality.
//!
//! Dispatch goes through [`crate::controllers::ControllerDispatcher`],
//! which holds the workqueue and routes resources by `(apiVersion, kind)`.

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use std::fmt::Debug;
#[cfg(test)]
use std::sync::Arc;

use super::ControllerRuntimeDependencies;

/// Controller context passed to reconcile methods.
///
/// Wraps the datastore as a trait object handle so controller implementations
/// remain backend-agnostic and can work with alternative datastore implementations
/// in future phases.
#[derive(Clone)]
pub(crate) struct Context {
    dependencies: ControllerRuntimeDependencies,
    reconcile_time: chrono::DateTime<chrono::Utc>,
    #[cfg(test)]
    db_handle: crate::datastore::DatastoreHandle,
    #[cfg(test)]
    pod_repository: Option<Arc<crate::kubelet::pod_repository::PodRepository>>,
    #[cfg(test)]
    non_pod_finalization: Option<Arc<dyn klights_reconcile_api::GcNonPodFinalizationPort>>,
    #[cfg(test)]
    node_metrics: Option<Arc<dyn klights_node_api::NodeMetrics>>,
}

impl Context {
    #[cfg(not(test))]
    pub(crate) fn new(
        dependencies: ControllerRuntimeDependencies,
        reconcile_time: chrono::DateTime<chrono::Utc>,
    ) -> Self {
        Self {
            dependencies,
            reconcile_time,
        }
    }

    #[cfg(test)]
    pub(crate) fn new(db_handle: crate::datastore::DatastoreHandle, node_name: String) -> Self {
        Self::test_context(db_handle, node_name, None)
    }

    #[cfg(test)]
    pub(crate) fn with_services(
        db_handle: crate::datastore::DatastoreHandle,
        node_name: String,
        services: Arc<dyn klights_network_api::ServiceRouter>,
    ) -> Self {
        Self::test_context(db_handle, node_name, Some(services))
    }

    #[cfg(test)]
    fn test_context(
        db_handle: crate::datastore::DatastoreHandle,
        node_name: String,
        services: Option<Arc<dyn klights_network_api::ServiceRouter>>,
    ) -> Self {
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let side_effects = Arc::new(klights_controllers::side_effects::SideEffectRegistry::new());
        let metrics = klights_controllers::side_effects::SideEffectMetrics::new();
        let repository = Arc::new(crate::kubelet::pod_repository::PodRepository::new(
            db_handle.clone(),
            supervisor.clone(),
            side_effects,
            metrics,
        ));
        let services = services.unwrap_or_else(|| {
            Arc::new(crate::networking::test_support::MockServiceRouter::default())
        });
        let non_pod_finalization: Arc<dyn klights_reconcile_api::GcNonPodFinalizationPort> =
            Arc::new(crate::gc_delete_adapter::GcNonPodFinalizationAdapter::new(
                db_handle.clone(),
            ));
        let leader_ports = Arc::new(
            crate::controller_runtime_adapter::RootControllerLeaderPort::new(db_handle.clone()),
        );
        let pod_ports = Arc::new(
            crate::controller_runtime_adapter::RootControllerPodPort::new_for_test(
                repository.clone(),
            ),
        );
        Self {
            dependencies: ControllerRuntimeDependencies {
                wall_time: chrono::Utc::now,
                resource_query: leader_ports.clone(),
                deployment_store: leader_ports.clone(),
                replicaset_store: leader_ports.clone(),
                statefulset_store: leader_ports.clone(),
                daemonset_store: leader_ports.clone(),
                job_store: leader_ports.clone(),
                service_store: leader_ports.clone(),
                pvc_store: leader_ports.clone(),
                pdb_store: leader_ports.clone(),
                replicationcontroller_store: leader_ports.clone(),
                apiservice_store: leader_ports.clone(),
                csr_status_store: leader_ports,
                pod_query: repository.clone(),
                pdb_pod_reader: repository.clone(),
                deployment_pod_reader: repository.clone(),
                deployment_pod_mutation: pod_ports.clone(),
                replicaset_pod_mutation: pod_ports.clone(),
                statefulset_pod_mutation: pod_ports.clone(),
                daemonset_pod_mutation: pod_ports.clone(),
                job_pod_mutation: pod_ports.clone(),
                replicationcontroller_pod_mutation: pod_ports,
                pod_delete_sink: repository.clone(),
                reconcile: Arc::new(
                    crate::controller_runtime_adapter::RootControllerReconcilePort::new(
                        non_pod_finalization.clone(),
                    ),
                ),
                network: Arc::new(
                    crate::controller_runtime_adapter::RootControllerNetworkPort::new(services),
                ),
                effects: Arc::new(
                    crate::controller_runtime_adapter::RootControllerEffectPort::new(
                        crate::kubelet::file_blocking::test_file_process_executor(),
                        crate::KlightsConfig::test_default()
                            .data_root
                            .join("local-path-provisioner"),
                    ),
                ),
                coordination: Arc::new(klights_controllers::ControllerCoordination::new()),
                node_name: Arc::from(node_name),
            },
            reconcile_time: chrono::Utc::now(),
            db_handle,
            pod_repository: Some(repository),
            non_pod_finalization: Some(non_pod_finalization),
            node_metrics: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_with_file_process(
        db_handle: crate::datastore::DatastoreHandle,
        node_name: String,
        file_process: klights_supervisor::FileProcessExecutor,
    ) -> Self {
        Self::new(db_handle, node_name).with_file_process(file_process)
    }

    #[cfg(test)]
    pub(crate) fn with_services_and_file_process(
        db_handle: crate::datastore::DatastoreHandle,
        node_name: String,
        services: Arc<dyn klights_network_api::ServiceRouter>,
        file_process: klights_supervisor::FileProcessExecutor,
    ) -> Self {
        Self::with_services(db_handle, node_name, services).with_file_process(file_process)
    }

    #[cfg(test)]
    fn with_file_process(mut self, file_process: klights_supervisor::FileProcessExecutor) -> Self {
        let local_path_provisioner_root = self
            .dependencies
            .effects
            .local_path_provisioner_root()
            .to_path_buf();
        self.dependencies.effects = Arc::new(
            crate::controller_runtime_adapter::RootControllerEffectPort::new(
                file_process,
                local_path_provisioner_root,
            ),
        );
        self
    }

    #[cfg(test)]
    pub(crate) fn with_pod_repository(
        mut self,
        repository: Arc<crate::kubelet::pod_repository::PodRepository>,
    ) -> Self {
        let pod_ports = Arc::new(
            crate::controller_runtime_adapter::RootControllerPodPort::new_for_test(
                repository.clone(),
            ),
        );
        self.dependencies.pod_query = repository.clone();
        self.dependencies.pdb_pod_reader = repository.clone();
        self.dependencies.deployment_pod_reader = repository.clone();
        self.dependencies.deployment_pod_mutation = pod_ports.clone();
        self.dependencies.replicaset_pod_mutation = pod_ports.clone();
        self.dependencies.statefulset_pod_mutation = pod_ports.clone();
        self.dependencies.daemonset_pod_mutation = pod_ports.clone();
        self.dependencies.job_pod_mutation = pod_ports.clone();
        self.dependencies.replicationcontroller_pod_mutation = pod_ports;
        self.dependencies.pod_delete_sink = repository.clone();
        self.pod_repository = Some(repository);
        self
    }

    #[cfg(test)]
    pub(crate) fn with_non_pod_finalization(
        mut self,
        finalization: Arc<dyn klights_reconcile_api::GcNonPodFinalizationPort>,
    ) -> Self {
        self.dependencies.reconcile = Arc::new(
            crate::controller_runtime_adapter::RootControllerReconcilePort::new(
                finalization.clone(),
            ),
        );
        self.non_pod_finalization = Some(finalization);
        self
    }

    #[cfg(test)]
    pub(crate) fn with_node_metrics(
        mut self,
        node_metrics: Arc<dyn klights_node_api::NodeMetrics>,
    ) -> Self {
        self.node_metrics = Some(node_metrics);
        self
    }

    #[cfg(test)]
    pub(crate) fn db_handle(&self) -> &crate::datastore::DatastoreHandle {
        &self.db_handle
    }

    #[cfg(test)]
    pub(crate) fn pod_repository(
        &self,
    ) -> Option<&Arc<crate::kubelet::pod_repository::PodRepository>> {
        self.pod_repository.as_ref()
    }

    #[cfg(test)]
    pub(crate) fn non_pod_finalization(
        &self,
    ) -> Option<&Arc<dyn klights_reconcile_api::GcNonPodFinalizationPort>> {
        self.non_pod_finalization.as_ref()
    }

    #[cfg(test)]
    pub(crate) fn node_metrics(&self) -> Option<&Arc<dyn klights_node_api::NodeMetrics>> {
        self.node_metrics.as_ref()
    }

    pub(crate) fn deployment_store(&self) -> &dyn super::deployment::DeploymentStore {
        self.dependencies.deployment_store.as_ref()
    }

    pub(crate) fn replicaset_store(&self) -> &dyn super::replicaset::ReplicaSetStore {
        self.dependencies.replicaset_store.as_ref()
    }

    pub(crate) fn statefulset_store(&self) -> &dyn super::statefulset::StatefulSetStore {
        self.dependencies.statefulset_store.as_ref()
    }

    pub(crate) fn daemonset_store(&self) -> &dyn super::daemonset::DaemonSetStore {
        self.dependencies.daemonset_store.as_ref()
    }

    pub(crate) fn job_store(&self) -> &dyn super::job::JobStore {
        self.dependencies.job_store.as_ref()
    }

    pub(crate) fn service_store(&self) -> &dyn super::service::ServiceControllerStore {
        self.dependencies.service_store.as_ref()
    }

    pub(crate) fn pvc_store(&self) -> &dyn super::pvc::PvcStore {
        self.dependencies.pvc_store.as_ref()
    }

    pub(crate) fn pdb_store(&self) -> &dyn super::pdb::PdbStore {
        self.dependencies.pdb_store.as_ref()
    }

    pub(crate) fn replicationcontroller_store(
        &self,
    ) -> &dyn super::replicationcontroller::ReplicationControllerStore {
        self.dependencies.replicationcontroller_store.as_ref()
    }

    pub(crate) fn apiservice_store(&self) -> &dyn super::apiservice::ApiServiceStore {
        self.dependencies.apiservice_store.as_ref()
    }

    pub(crate) fn csr_status_store(&self) -> &dyn super::csr_signer::CsrStatusStore {
        self.dependencies.csr_status_store.as_ref()
    }

    pub(crate) fn pod_query(&self) -> &dyn klights_pod_api::PodQuery {
        self.dependencies.pod_query.as_ref()
    }

    pub(crate) fn pdb_reader(&self) -> &dyn super::pdb::PdbPodReader {
        self.dependencies.pdb_pod_reader.as_ref()
    }

    pub(crate) fn deployment_reader(&self) -> &dyn super::DeploymentControllerPodReader {
        self.dependencies.deployment_pod_reader.as_ref()
    }

    pub(crate) fn deployment_mutation(&self) -> &dyn super::DeploymentControllerPodMutation {
        self.dependencies.deployment_pod_mutation.as_ref()
    }

    pub(crate) fn replicaset_mutation(&self) -> &dyn super::replicaset::ReplicaSetPodMutation {
        self.dependencies.replicaset_pod_mutation.as_ref()
    }

    pub(crate) fn statefulset_mutation(&self) -> &dyn super::statefulset::StatefulSetPodMutation {
        self.dependencies.statefulset_pod_mutation.as_ref()
    }

    pub(crate) fn daemonset_mutation(&self) -> &dyn super::daemonset::DaemonSetPodMutation {
        self.dependencies.daemonset_pod_mutation.as_ref()
    }

    pub(crate) fn job_mutation(&self) -> &dyn super::job::JobPodMutation {
        self.dependencies.job_pod_mutation.as_ref()
    }

    pub(crate) fn replicationcontroller_mutation(
        &self,
    ) -> &dyn super::replicationcontroller::ReplicationControllerPodMutation {
        self.dependencies
            .replicationcontroller_pod_mutation
            .as_ref()
    }

    pub(crate) fn pod_delete_sink(&self) -> &dyn klights_reconcile_api::GcPodDeleteSink {
        self.dependencies.pod_delete_sink.as_ref()
    }

    pub(crate) fn reconcile_port(&self) -> &dyn super::ControllerReconcilePort {
        self.dependencies.reconcile.as_ref()
    }

    pub(crate) fn network(&self) -> &dyn super::ControllerNetworkPort {
        self.dependencies.network.as_ref()
    }

    pub(crate) fn effects(&self) -> &dyn super::ControllerEffectPort {
        self.dependencies.effects.as_ref()
    }

    pub(crate) fn coordination(&self) -> &klights_controllers::ControllerCoordination {
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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Context")
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

/// Controller trait for reconciling Kubernetes resources
///
/// All controllers implement this trait to provide a uniform interface
/// for reconciliation logic. The trait is async and designed to work
/// with tokio's runtime.
///
/// # Example
///
/// ```text
/// use klights::controllers::{Controller, Context};
/// use anyhow::Result;
/// use serde_json::Value;
/// use async_trait::async_trait;
///
/// struct MyController;
///
/// #[async_trait]
/// impl Controller for MyController {
///     fn name(&self) -> &'static str {
///         "mycontroller"
///     }
///
///     async fn reconcile(&self, resource: Value, ctx: Context) -> Result<()> {
///         // Reconcile logic here
///         Ok(())
///     }
/// }
/// ```
#[async_trait]
pub(crate) trait Controller: Send + Sync {
    /// Returns the name of this controller
    ///
    /// Used for logging and identification purposes.
    ///
    /// Note: Not yet called, but implemented in all 9 controllers.
    /// Reserved for future use in tracing spans, metrics, and debug output.
    #[cfg_attr(not(test), allow(dead_code))]
    fn name(&self) -> &'static str;

    /// Reconcile a resource to its desired state
    ///
    /// This method is called when a resource is created, updated, or patched.
    /// It should:
    /// 1. Read the current state of the resource and any dependent resources
    /// 2. Compute the desired state
    /// 3. Make changes to reach the desired state (create/update/delete dependent resources)
    /// 4. Update the resource's status if applicable
    ///
    /// # Arguments
    ///
    /// * `resource` - The resource to reconcile (as a JSON Value)
    /// * `ctx` - The controller context providing access to shared state
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if reconciliation succeeded, or an error if it failed.
    ///
    /// The HTTP mutation handler does not call this directly — it enqueues the
    /// resource on the [`ControllerDispatcher`](crate::controllers::ControllerDispatcher)
    /// workqueue and returns 2xx to the client immediately. The dispatcher's
    /// background worker pops the key, fetches the freshest resource state from
    /// the datastore, and invokes `reconcile`. On error the worker re-enqueues
    /// the key with exponential backoff (250ms → 30s, 7 attempts capped); after
    /// `MAX_RETRY_ATTEMPTS` the key is dropped and only the next mutation or
    /// watch event will trigger another attempt.
    async fn reconcile(&self, resource: Value, ctx: Context) -> Result<()>;
}

/// Generate a unit-struct `Controller` impl that delegates to a free reconcile
/// function in the matching `controllers::<kind>` module.
///
/// Most kind controllers in `src/controllers/*_controller.rs` are mechanical
/// thin shims — `pub struct XController; impl Controller { name -> "x";
/// reconcile -> x_core::reconcile_x(db, &resource[, node]) }`. This macro
/// collapses that boilerplate. `ServiceController` (carries fields) and
/// `EndpointsController` (extracts metadata fields before delegating) stay
/// explicit.
///
/// Four arms cover the call shape combinations actually in use:
///
/// | Arm | Reconcile body |
/// |---|---|
/// | `with_node` | `core(db, &resource, ctx.node_name()).await` |
/// | `no_node` | `core(db, &resource).await` |
/// | `with_node, discard` | `core(...).await.map(\|_\| ())` (core returns `Result<Value>`) |
/// | `no_node, discard` | same, no node arg |
/// | `with_node, with_pod_repository` | `core(db, pod_reader, pod_writer, &resource, ctx.node_name()).await` |
/// | `no_node, with_pod_repository` | `core(db, pod_reader, pod_writer, &resource).await` |
/// | `with_node, discard, with_pod_repository` | same as with_node+with_pod_repository, but maps Result<Value> → Result<()> |
///
/// Example:
/// ```ignore
/// controller_wrapper!(DeploymentController, "deployment",
///     deployment_core::reconcile_deployment, with_node);
/// controller_wrapper!(JobController, "job",
///     job_core::reconcile_job, with_node, discard);
/// controller_wrapper!(PDBController, "poddisruptionbudget",
///     pdb_core::reconcile_pdb, no_node);
/// ```
macro_rules! controller_wrapper {
    (
        $struct_name:ident, $name:literal, $core_fn:path,
        with_node, with_pod_repository,
        store = $store:ident, reader = $reader:ident, mutation = $mutation:ident
    ) => {
        pub struct $struct_name;

        #[::async_trait::async_trait]
        impl $crate::controllers::Controller for $struct_name {
            fn name(&self) -> &'static str {
                $name
            }
            async fn reconcile(
                &self,
                resource: ::serde_json::Value,
                ctx: $crate::controllers::Context,
            ) -> ::anyhow::Result<()> {
                $core_fn(
                    ctx.$store(),
                    ctx.$reader(),
                    ctx.$mutation(),
                    ctx.pod_delete_sink(),
                    ctx.reconcile_port().non_pod_finalization(),
                    &resource,
                    klights_controllers::ControllerReconcileContext::at(
                        ctx.coordination(),
                        ctx.node_name(),
                        ctx.reconcile_time(),
                    ),
                )
                .await
            }
        }
    };
    ($struct_name:ident, $name:literal, $core_fn:path, no_node, store = $store:ident) => {
        pub struct $struct_name;

        #[::async_trait::async_trait]
        impl $crate::controllers::Controller for $struct_name {
            fn name(&self) -> &'static str {
                $name
            }
            async fn reconcile(
                &self,
                resource: ::serde_json::Value,
                ctx: $crate::controllers::Context,
            ) -> ::anyhow::Result<()> {
                $core_fn(ctx.$store(), &resource).await
            }
        }
    };
    (
        $struct_name:ident, $name:literal, $core_fn:path,
        no_node, discard, with_file_process, store = $store:ident
    ) => {
        pub struct $struct_name;

        #[::async_trait::async_trait]
        impl $crate::controllers::Controller for $struct_name {
            fn name(&self) -> &'static str {
                $name
            }
            async fn reconcile(
                &self,
                resource: ::serde_json::Value,
                ctx: $crate::controllers::Context,
            ) -> ::anyhow::Result<()> {
                $core_fn(
                    ctx.effects().file_process(),
                    ctx.effects().local_path_provisioner_root(),
                    ctx.$store(),
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
        pub struct $struct_name;

        #[::async_trait::async_trait]
        impl $crate::controllers::Controller for $struct_name {
            fn name(&self) -> &'static str {
                $name
            }
            async fn reconcile(
                &self,
                resource: ::serde_json::Value,
                ctx: $crate::controllers::Context,
            ) -> ::anyhow::Result<()> {
                $core_fn(
                    ctx.$store(),
                    ctx.pod_query(),
                    ctx.$mutation(),
                    ctx.pod_delete_sink(),
                    ctx.reconcile_port().non_pod_finalization(),
                    ctx.coordination(),
                    &resource,
                    ctx.reconcile_time(),
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
        impl $crate::controllers::Controller for $struct_name {
            fn name(&self) -> &'static str {
                $name
            }
            async fn reconcile(
                &self,
                resource: ::serde_json::Value,
                ctx: $crate::controllers::Context,
            ) -> ::anyhow::Result<()> {
                $core_fn(ctx.$store(), ctx.$reader(), &resource).await
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
        impl $crate::controllers::Controller for $struct_name {
            fn name(&self) -> &'static str {
                $name
            }
            async fn reconcile(
                &self,
                resource: ::serde_json::Value,
                ctx: $crate::controllers::Context,
            ) -> ::anyhow::Result<()> {
                $core_fn(
                    ctx.$store(),
                    ctx.pod_query(),
                    ctx.$mutation(),
                    ctx.pod_delete_sink(),
                    ctx.reconcile_port().non_pod_finalization(),
                    &resource,
                    klights_controllers::ControllerReconcileContext::at(
                        ctx.coordination(),
                        ctx.node_name(),
                        ctx.reconcile_time(),
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
    use crate::datastore::DatastoreHandle;
    use crate::datastore::sqlite::Datastore;
    use std::sync::Arc;

    fn handle_for(db: Datastore) -> DatastoreHandle {
        Arc::new(db)
    }

    /// A simple test controller for verifying the Controller trait
    struct TestController {
        name: &'static str,
    }

    #[async_trait]
    impl Controller for TestController {
        fn name(&self) -> &'static str {
            self.name
        }

        async fn reconcile(&self, _resource: Value, _ctx: Context) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn test_controller_name_returns_correct_name() {
        let controller = TestController {
            name: "test-controller",
        };
        assert_eq!(controller.name(), "test-controller");
    }

    #[tokio::test]
    async fn test_controller_reconcile_returns_ok() {
        let controller = TestController { name: "test" };
        let resource = serde_json::json!({"apiVersion": "v1", "kind": "Pod"});

        let db = crate::datastore::test_support::in_memory().await;
        let ctx = Context::new(handle_for(db), "test-node".to_string());

        let result = controller.reconcile(resource, ctx).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn private_controller_example_compiles_with_focused_test_ports() {
        struct ExampleController;

        #[async_trait]
        impl Controller for ExampleController {
            fn name(&self) -> &'static str {
                "example"
            }

            async fn reconcile(&self, resource: Value, ctx: Context) -> Result<()> {
                assert_eq!(ctx.node_name(), "example-node");
                assert_eq!(
                    resource.pointer("/metadata/name").and_then(Value::as_str),
                    Some("example")
                );
                Ok(())
            }
        }

        let db = crate::datastore::test_support::in_memory().await;
        let ctx = Context::new(handle_for(db), "example-node".to_string());
        let controller = ExampleController;
        assert_eq!(controller.name(), "example");
        controller
            .reconcile(serde_json::json!({"metadata": {"name": "example"}}), ctx)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_context_new_creates_context_with_handle_and_node_name() {
        let db = crate::datastore::test_support::in_memory().await;
        let ctx = Context::new(handle_for(db), "test-node".to_string());

        assert_eq!(ctx.node_name(), "test-node");
    }

    #[tokio::test]
    async fn test_context_db_handle_returns_handle() {
        let db = crate::datastore::test_support::in_memory().await;
        let handle = handle_for(db);
        let ctx = Context::new(handle.clone(), "test-node".to_string());

        // Same Arc pointee — the handle inside the context is what we passed in.
        assert!(Arc::ptr_eq(&handle, ctx.db_handle()));
    }

    #[tokio::test]
    async fn test_context_debug_formatting() {
        let db = crate::datastore::test_support::in_memory().await;
        let ctx = Context::new(handle_for(db), "test-node".to_string());

        let debug_str = format!("{:?}", ctx);
        assert!(debug_str.contains("test-node"));
        assert!(debug_str.contains("Context"));
        assert!(debug_str.contains("focused_dependencies"));
        for dependency in ["leader", "pods", "reconcile", "network", "effects"] {
            assert!(debug_str.contains(dependency));
        }
        assert!(!debug_str.contains("db_handle"));
        assert!(!debug_str.contains("Datastore"));
    }

    #[tokio::test]
    async fn test_controller_reconcile_error_propagation() {
        struct FailingController;

        #[async_trait]
        impl Controller for FailingController {
            fn name(&self) -> &'static str {
                "failing"
            }
            async fn reconcile(&self, _resource: Value, _ctx: Context) -> Result<()> {
                anyhow::bail!("intentional failure")
            }
        }

        let db = crate::datastore::test_support::in_memory().await;
        let ctx = Context::new(handle_for(db), "node".to_string());
        let controller = FailingController;

        let result = controller.reconcile(serde_json::json!({}), ctx).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("intentional failure")
        );
    }

    #[test]
    fn test_context_clone() {
        // Context derives Clone — verify it works
        fn assert_clone<T: Clone>() {}
        assert_clone::<Context>();
    }
}
