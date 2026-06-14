//! Controller trait and related types for extensible controller implementations
//!
//! This module defines the `Controller` trait which provides a uniform interface
//! for all controllers in klights. Each controller (Deployment, StatefulSet, Service, etc.)
//! implements this trait to provide reconcile functionality.
//!
//! Dispatch goes through [`crate::controller_dispatcher::ControllerDispatcher`],
//! which holds the workqueue and routes resources by `(apiVersion, kind)`.

use crate::datastore::DatastoreHandle;
use crate::kubelet::pod_repository::PodRepository;
use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use std::fmt::Debug;
use std::sync::Arc;

/// Controller context passed to reconcile methods.
///
/// Wraps the datastore as a trait object handle so controller implementations
/// remain backend-agnostic and can work with alternative datastore implementations
/// in future phases.
#[derive(Clone)]
pub struct Context {
    /// Datastore handle (trait object) — the abstraction-level reference.
    pub db_handle: DatastoreHandle,
    /// Node name (for single-node clusters, always the current node).
    pub node_name: String,
    /// Service router for controllers that need to trigger nft sync
    /// (Service controller). Optional because the unit-test workqueue
    /// wiring constructs a Context without a live router.
    pub services: Option<Arc<dyn crate::networking::ServiceRouter>>,
    /// Single-instance pod persistence boundary. Workload controllers
    /// (Deployment, ReplicaSet) use this to read/write Pod objects
    /// instead of going through the raw datastore. Optional so test
    /// fixtures that exercise non-pod controllers can construct a
    /// Context without wiring a repository.
    pub pod_repository: Option<Arc<PodRepository>>,
}

impl Context {
    /// Create a new controller context from a [`DatastoreHandle`].
    pub fn new(db_handle: DatastoreHandle, node_name: String) -> Self {
        Self {
            db_handle,
            node_name,
            services: None,
            pod_repository: None,
        }
    }

    /// Construct a Context with a live service router attached. Used by
    /// the production controller dispatcher; tests use `Context::new`.
    pub fn with_services(
        db_handle: DatastoreHandle,
        node_name: String,
        services: Arc<dyn crate::networking::ServiceRouter>,
    ) -> Self {
        Self {
            db_handle,
            node_name,
            services: Some(services),
            pod_repository: None,
        }
    }

    /// Attach a `PodRepository` to this context. Returns `self` for builder-style
    /// chaining off `Context::new`/`Context::with_services`.
    pub fn with_pod_repository(mut self, pod_repository: Arc<PodRepository>) -> Self {
        self.pod_repository = Some(pod_repository);
        self
    }

    /// Returns the datastore handle (trait object).
    pub fn db_handle(&self) -> &DatastoreHandle {
        &self.db_handle
    }

    /// Get the node name
    pub fn node_name(&self) -> &str {
        &self.node_name
    }

    /// Borrow the service router if one is attached.
    pub fn services(&self) -> Option<&Arc<dyn crate::networking::ServiceRouter>> {
        self.services.as_ref()
    }

    /// Borrow the pod repository if one is attached.
    pub fn pod_repository(&self) -> Option<&Arc<PodRepository>> {
        self.pod_repository.as_ref()
    }
}

impl Debug for Context {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Context")
            .field("node_name", &self.node_name)
            .field("db_handle", &"<DatastoreHandle>")
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
/// ```rust
/// use klights::controller::{Controller, Context};
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
pub trait Controller: Send + Sync {
    /// Returns the name of this controller
    ///
    /// Used for logging and identification purposes.
    ///
    /// Note: Not yet called, but implemented in all 9 controllers.
    /// Reserved for future use in tracing spans, metrics, and debug output.
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
    /// resource on the [`ControllerDispatcher`](crate::controller_dispatcher::ControllerDispatcher)
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
    ($struct_name:ident, $name:literal, $core_fn:path, with_node) => {
        pub struct $struct_name;

        #[::async_trait::async_trait]
        impl $crate::controller::Controller for $struct_name {
            fn name(&self) -> &'static str {
                $name
            }
            async fn reconcile(
                &self,
                resource: ::serde_json::Value,
                ctx: $crate::controller::Context,
            ) -> ::anyhow::Result<()> {
                $core_fn(ctx.db_handle().as_ref(), &resource, ctx.node_name()).await
            }
        }
    };
    ($struct_name:ident, $name:literal, $core_fn:path, with_node, with_pod_repository) => {
        pub struct $struct_name;

        #[::async_trait::async_trait]
        impl $crate::controller::Controller for $struct_name {
            fn name(&self) -> &'static str {
                $name
            }
            async fn reconcile(
                &self,
                resource: ::serde_json::Value,
                ctx: $crate::controller::Context,
            ) -> ::anyhow::Result<()> {
                let pod_repository = ctx.pod_repository().ok_or_else(|| {
                    ::anyhow::anyhow!(
                        "{} requires pod_repository in Context — wire it via \
                         ControllerDispatcher::set_pod_repository or \
                         Context::with_pod_repository",
                        $name
                    )
                })?;
                let pod_repo_ref = pod_repository.as_ref();
                $core_fn(
                    ctx.db_handle().as_ref(),
                    pod_repo_ref,
                    pod_repo_ref,
                    pod_repo_ref,
                    &resource,
                    ctx.node_name(),
                )
                .await
            }
        }
    };
    ($struct_name:ident, $name:literal, $core_fn:path, no_node) => {
        pub struct $struct_name;

        #[::async_trait::async_trait]
        impl $crate::controller::Controller for $struct_name {
            fn name(&self) -> &'static str {
                $name
            }
            async fn reconcile(
                &self,
                resource: ::serde_json::Value,
                ctx: $crate::controller::Context,
            ) -> ::anyhow::Result<()> {
                $core_fn(ctx.db_handle().as_ref(), &resource).await
            }
        }
    };
    ($struct_name:ident, $name:literal, $core_fn:path, with_node, discard) => {
        pub struct $struct_name;

        #[::async_trait::async_trait]
        impl $crate::controller::Controller for $struct_name {
            fn name(&self) -> &'static str {
                $name
            }
            async fn reconcile(
                &self,
                resource: ::serde_json::Value,
                ctx: $crate::controller::Context,
            ) -> ::anyhow::Result<()> {
                $core_fn(ctx.db_handle().as_ref(), &resource, ctx.node_name())
                    .await
                    .map(|_| ())
            }
        }
    };
    ($struct_name:ident, $name:literal, $core_fn:path, no_node, discard) => {
        pub struct $struct_name;

        #[::async_trait::async_trait]
        impl $crate::controller::Controller for $struct_name {
            fn name(&self) -> &'static str {
                $name
            }
            async fn reconcile(
                &self,
                resource: ::serde_json::Value,
                ctx: $crate::controller::Context,
            ) -> ::anyhow::Result<()> {
                $core_fn(ctx.db_handle().as_ref(), &resource)
                    .await
                    .map(|_| ())
            }
        }
    };
    ($struct_name:ident, $name:literal, $core_fn:path, no_node, with_pod_repository) => {
        pub struct $struct_name;

        #[::async_trait::async_trait]
        impl $crate::controller::Controller for $struct_name {
            fn name(&self) -> &'static str {
                $name
            }
            async fn reconcile(
                &self,
                resource: ::serde_json::Value,
                ctx: $crate::controller::Context,
            ) -> ::anyhow::Result<()> {
                let pod_repository = ctx.pod_repository().ok_or_else(|| {
                    ::anyhow::anyhow!(
                        "{} requires pod_repository in Context — wire it via \
                         ControllerDispatcher::set_pod_repository or \
                         Context::with_pod_repository",
                        $name
                    )
                })?;
                let pod_repo_ref = pod_repository.as_ref();
                $core_fn(
                    ctx.db_handle().as_ref(),
                    pod_repo_ref,
                    pod_repo_ref,
                    pod_repo_ref,
                    &resource,
                )
                .await
            }
        }
    };
    ($struct_name:ident, $name:literal, $core_fn:path, no_node, with_pod_reader) => {
        pub struct $struct_name;

        #[::async_trait::async_trait]
        impl $crate::controller::Controller for $struct_name {
            fn name(&self) -> &'static str {
                $name
            }
            async fn reconcile(
                &self,
                resource: ::serde_json::Value,
                ctx: $crate::controller::Context,
            ) -> ::anyhow::Result<()> {
                let pod_repository = ctx.pod_repository().ok_or_else(|| {
                    ::anyhow::anyhow!(
                        "{} requires pod_repository in Context — wire it via \
                         ControllerDispatcher::set_pod_repository or \
                         Context::with_pod_repository",
                        $name
                    )
                })?;
                $core_fn(ctx.db_handle().as_ref(), pod_repository.as_ref(), &resource).await
            }
        }
    };
    ($struct_name:ident, $name:literal, $core_fn:path, with_node, discard, with_pod_repository) => {
        pub struct $struct_name;

        #[::async_trait::async_trait]
        impl $crate::controller::Controller for $struct_name {
            fn name(&self) -> &'static str {
                $name
            }
            async fn reconcile(
                &self,
                resource: ::serde_json::Value,
                ctx: $crate::controller::Context,
            ) -> ::anyhow::Result<()> {
                let pod_repository = ctx.pod_repository().ok_or_else(|| {
                    ::anyhow::anyhow!(
                        "{} requires pod_repository in Context — wire it via \
                         ControllerDispatcher::set_pod_repository or \
                         Context::with_pod_repository",
                        $name
                    )
                })?;
                let pod_repo_ref = pod_repository.as_ref();
                $core_fn(
                    ctx.db_handle().as_ref(),
                    pod_repo_ref,
                    pod_repo_ref,
                    pod_repo_ref,
                    &resource,
                    ctx.node_name(),
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
        assert!(debug_str.contains("db_handle"));
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
