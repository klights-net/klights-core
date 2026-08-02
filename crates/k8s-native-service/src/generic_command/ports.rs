//! Private transitional ports consumed by generic command orchestration.
//!
//! These interfaces expose only focused, already-existing Phase 9
//! capabilities. Root implements the family adapters; native-service never
//! reaches a concrete store, replication, RPC, controller, or kubelet owner.

use std::future::Future;
use std::pin::Pin;

use klights_cluster_core::{Resource, ResourcePreconditions};
use serde_json::Value;

use crate::{
    ApiState, AppError,
    discovery::{ApiServiceProxyCache, DiscoveryAggregation},
};

use super::{CreateUpdateQuery, DryRunMode};

pub type GenericCommandFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, AppError>> + Send + 'a>>;

pub struct ResourceAdmissionRequest {
    pub api_version: String,
    pub kind: String,
    /// Exact Kubernetes resource name used for webhook rule matching.
    /// `None` derives the ordinary plural from `kind`; subresource callers
    /// supply the discovery resource explicitly (for example, `pods` for
    /// a `Binding` object submitted to `pods/binding`).
    pub resource: Option<String>,
    pub operation: String,
    pub namespace: Option<String>,
    pub name: Option<String>,
    pub object: Value,
    pub old_object: Option<Value>,
    pub dry_run: bool,
    pub subresource: Option<String>,
    pub options: Option<Value>,
}

pub trait ResourceAdmissionPort: Send + Sync {
    fn admit(&self, request: ResourceAdmissionRequest) -> GenericCommandFuture<'_, Value>;
}

pub trait BuiltinAdmissionDefaultsPort: Send + Sync {
    fn ensure_namespace_active(&self, namespace: String) -> GenericCommandFuture<'_, ()>;
    fn validate_pod_volume_paths(&self, pod: &Value) -> Result<(), AppError>;
    fn prepare_pod_create(&self, namespace: String, pod: Value) -> GenericCommandFuture<'_, Value>;
    fn prepare_pvc_create(
        &self,
        namespace: String,
        claim: Value,
    ) -> GenericCommandFuture<'_, Value>;
}

pub trait GeneratedLifecyclePort: Send + Sync {
    fn rotate_bootstrap_token_secret(
        &self,
        resource: Resource,
    ) -> GenericCommandFuture<'_, Resource>;
    fn reconcile_cluster_role_aggregation(&self) -> GenericCommandFuture<'_, ()>;
    fn create_default_service_account(&self, namespace: String) -> GenericCommandFuture<'_, ()>;
    fn create_root_ca_config_map(&self, namespace: String) -> GenericCommandFuture<'_, ()>;
    fn reconcile_root_ca_data(&self, namespace: String) -> GenericCommandFuture<'_, ()>;
    fn reconcile_root_ca(&self, namespace: String) -> GenericCommandFuture<'_, ()>;
    fn delete_node_cleanup_intents(&self, node_name: String) -> GenericCommandFuture<'_, ()>;
    fn maybe_finalize_pod_after_finalizers_drained(
        &self,
        namespace: String,
        name: String,
        pod: Value,
    ) -> GenericCommandFuture<'_, ()>;
}

pub trait GeneratedResourceMutationPort: Send + Sync {
    fn update_main_resource(
        &self,
        api_version: String,
        kind: String,
        namespace: Option<String>,
        name: String,
        data: Value,
        preconditions: ResourcePreconditions,
    ) -> GenericCommandFuture<'_, Resource>;
}

pub trait GenericCommandStore: Send + Sync {
    fn identity(&self) -> &dyn crate::ApiIdentityGenerator;
    fn identity_owned(&self) -> std::sync::Arc<dyn crate::ApiIdentityGenerator>;
    fn resource_query(&self) -> &dyn klights_leader_api::LeaderResourceQuery;
    fn resource_command(&self) -> &dyn klights_leader_api::LeaderResourceCommand;
    fn finalizer_lifecycle(&self) -> &dyn klights_reconcile_api::FinalizerLifecyclePort;
    fn generated_mutations(&self) -> &dyn GeneratedResourceMutationPort;
    fn pod_mutation(&self) -> &dyn klights_pod_api::PodApiMutation;
    fn pod_subresource_mutation(&self) -> &dyn klights_pod_api::PodSubresourceMutation;
    fn pod_eviction_admission(
        &self,
    ) -> std::sync::Arc<dyn klights_reconcile_api::PodEvictionAdmissionSink>;
    fn pod_eviction_delete(&self) -> &dyn klights_pod_api::PodEvictionDelete;
}

pub trait GenericCommandAdmission: Send + Sync {
    fn admission(&self) -> &dyn ResourceAdmissionPort;
    fn quota_runtime(&self) -> &dyn klights_reconcile_api::ResourceQuotaAdmissionRuntime;
    fn builtin_admission_defaults(&self) -> &dyn BuiltinAdmissionDefaultsPort;
}

pub trait GenericCommandLifecycle: Send + Sync {
    fn mutation_effects(&self) -> &dyn klights_reconcile_api::ResourceMutationEffectsPort;
    fn generated_lifecycle(&self) -> &dyn GeneratedLifecyclePort;
    fn gc_owner_lifecycle(&self) -> &dyn klights_reconcile_api::GcOwnerLifecyclePort;
    fn gc_owner_lifecycle_owned(
        &self,
    ) -> std::sync::Arc<dyn klights_reconcile_api::GcOwnerLifecyclePort>;
}

pub trait GenericCommandAuthorization: Send + Sync {
    #[allow(clippy::too_many_arguments)]
    fn enforce_rbac_write_authorization<'a>(
        &'a self,
        resource_query: &'a dyn klights_leader_api::LeaderResourceQuery,
        identity: &'a klights_auth::AuthenticatedIdentity,
        api_version: &'a str,
        kind: &'a str,
        namespace: Option<&'a str>,
        object: &'a Value,
    ) -> GenericCommandFuture<'a, ()>;
}

pub struct PreparedCreate {
    pub resource_name: String,
    pub body: Value,
}

pub trait GenericCommandPolicy: Send + Sync {
    fn apply_patch(
        &self,
        current: &Value,
        patch: &Value,
        content_type: Option<&str>,
    ) -> Result<Value, AppError>;

    fn decode_patch<'a>(
        &'a self,
        headers: &'a axum::http::HeaderMap,
        body: &'a bytes::Bytes,
    ) -> GenericCommandFuture<'a, Value>;

    fn validate_patch_request(
        &self,
        api_version: &str,
        kind: &str,
        query: &CreateUpdateQuery,
        patch: &Value,
        content_type: Option<&str>,
    ) -> Result<(), AppError>;

    #[allow(clippy::too_many_arguments)]
    fn before_create<'a>(
        &'a self,
        authorization: &'a dyn GenericCommandAuthorization,
        resource_query: &'a dyn klights_leader_api::LeaderResourceQuery,
        identity: &'a klights_auth::AuthenticatedIdentity,
        api_version: &'a str,
        kind: &'a str,
        namespace: Option<&'a str>,
        query: &'a CreateUpdateQuery,
        body: Value,
    ) -> GenericCommandFuture<'a, Value>;

    fn prepare_create<'a>(
        &'a self,
        api_version: &'a str,
        kind: &'a str,
        namespace: Option<&'a str>,
        body: Value,
        operation_now: Option<chrono::DateTime<chrono::Utc>>,
        dry_run: DryRunMode,
    ) -> GenericCommandFuture<'a, PreparedCreate>;

    #[allow(clippy::too_many_arguments)]
    fn before_update<'a>(
        &'a self,
        authorization: &'a dyn GenericCommandAuthorization,
        resource_query: &'a dyn klights_leader_api::LeaderResourceQuery,
        identity: &'a klights_auth::AuthenticatedIdentity,
        api_version: &'a str,
        kind: &'a str,
        namespace: Option<&'a str>,
        name: &'a str,
        query: &'a CreateUpdateQuery,
        current: &'a Resource,
        body: Value,
        dry_run: DryRunMode,
    ) -> GenericCommandFuture<'a, Value>;

    #[allow(clippy::too_many_arguments)]
    fn after_update_admission<'a>(
        &'a self,
        api_version: &'a str,
        kind: &'a str,
        namespace: Option<&'a str>,
        current: &'a Resource,
        body: Value,
    ) -> GenericCommandFuture<'a, Value>;

    fn prepare_update_for_persistence<'a>(
        &'a self,
        kind: &'a str,
        body: Value,
    ) -> GenericCommandFuture<'a, Value>;

    #[allow(clippy::too_many_arguments)]
    fn prepare_apply_create<'a>(
        &'a self,
        authorization: &'a dyn GenericCommandAuthorization,
        resource_query: &'a dyn klights_leader_api::LeaderResourceQuery,
        identity: &'a klights_auth::AuthenticatedIdentity,
        api_version: &'a str,
        kind: &'a str,
        namespace: Option<&'a str>,
        name: &'a str,
        query: &'a CreateUpdateQuery,
        patch: Value,
        operation_now: chrono::DateTime<chrono::Utc>,
        dry_run: DryRunMode,
    ) -> GenericCommandFuture<'a, Value>;

    #[allow(clippy::too_many_arguments)]
    fn prepare_patch_update<'a>(
        &'a self,
        authorization: &'a dyn GenericCommandAuthorization,
        resource_query: &'a dyn klights_leader_api::LeaderResourceQuery,
        identity: &'a klights_auth::AuthenticatedIdentity,
        api_version: &'a str,
        kind: &'a str,
        namespace: Option<&'a str>,
        name: &'a str,
        query: &'a CreateUpdateQuery,
        current: &'a Resource,
        patch: &'a Value,
        content_type: Option<&'a str>,
        operation_now: chrono::DateTime<chrono::Utc>,
        dry_run: DryRunMode,
    ) -> GenericCommandFuture<'a, Value>;

    fn normalize_for_storage(&self, api_version: &str, kind: &str, body: &mut Value);
}

pub trait GenericCommandReconcile: Send + Sync {
    fn service_allocations(&self) -> &dyn klights_reconcile_api::ServiceWriteAllocator;
    fn controller_dispatcher(&self) -> &dyn klights_reconcile_api::ControllerDispatcherPort;
    fn failure_metrics(&self) -> &dyn klights_reconcile_api::ReconcileFailureMetrics;
    fn failure_metrics_owned(
        &self,
    ) -> std::sync::Arc<dyn klights_reconcile_api::ReconcileFailureMetrics>;
}

pub trait GenericCommandRuntime: Send + Sync {
    fn clock(&self) -> &dyn klights_auth::clock::Clock;
    fn task_supervisor(&self) -> &klights_supervisor::TaskSupervisor;
    fn task_supervisor_owned(&self) -> std::sync::Arc<klights_supervisor::TaskSupervisor>;
}

pub trait GenericCommandState: Send + Sync {
    fn command_authorization(&self) -> &dyn GenericCommandAuthorization;
    fn command_store(&self) -> &dyn GenericCommandStore;
    fn command_admission(&self) -> &dyn GenericCommandAdmission;
    fn command_lifecycle(&self) -> &dyn GenericCommandLifecycle;
    fn command_reconcile(&self) -> &dyn GenericCommandReconcile;
    fn command_runtime(&self) -> &dyn GenericCommandRuntime;
    fn command_policy(&self) -> &dyn GenericCommandPolicy;
    fn apiservice_proxy_cache(&self) -> &ApiServiceProxyCache;
}

impl<Auth, Resources, Discovery, Controllers, PodNode, Operational> GenericCommandState
    for ApiState<Auth, Resources, Discovery, Controllers, PodNode, Operational>
where
    Auth: GenericCommandAuthorization,
    Resources: GenericCommandStore
        + GenericCommandAdmission
        + GenericCommandLifecycle
        + GenericCommandPolicy,
    Discovery: DiscoveryAggregation,
    Controllers: GenericCommandReconcile,
    PodNode: Send + Sync,
    Operational: GenericCommandRuntime,
{
    fn command_authorization(&self) -> &dyn GenericCommandAuthorization {
        self.auth_policy()
    }

    fn command_store(&self) -> &dyn GenericCommandStore {
        self.resource_mutation()
    }

    fn command_admission(&self) -> &dyn GenericCommandAdmission {
        self.resource_mutation()
    }

    fn command_lifecycle(&self) -> &dyn GenericCommandLifecycle {
        self.resource_mutation()
    }

    fn command_reconcile(&self) -> &dyn GenericCommandReconcile {
        self.controller_reconcile()
    }

    fn command_runtime(&self) -> &dyn GenericCommandRuntime {
        self.operational()
    }

    fn command_policy(&self) -> &dyn GenericCommandPolicy {
        self.resource_mutation()
    }

    fn apiservice_proxy_cache(&self) -> &ApiServiceProxyCache {
        self.discovery().apiservice_proxy_cache()
    }
}

impl<S: GenericCommandState + ?Sized> GenericCommandState for std::sync::Arc<S> {
    fn command_authorization(&self) -> &dyn GenericCommandAuthorization {
        self.as_ref().command_authorization()
    }

    fn command_store(&self) -> &dyn GenericCommandStore {
        self.as_ref().command_store()
    }

    fn command_admission(&self) -> &dyn GenericCommandAdmission {
        self.as_ref().command_admission()
    }

    fn command_lifecycle(&self) -> &dyn GenericCommandLifecycle {
        self.as_ref().command_lifecycle()
    }

    fn command_reconcile(&self) -> &dyn GenericCommandReconcile {
        self.as_ref().command_reconcile()
    }

    fn command_runtime(&self) -> &dyn GenericCommandRuntime {
        self.as_ref().command_runtime()
    }

    fn command_policy(&self) -> &dyn GenericCommandPolicy {
        self.as_ref().command_policy()
    }

    fn apiservice_proxy_cache(&self) -> &ApiServiceProxyCache {
        self.as_ref().apiservice_proxy_cache()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_family_ports_are_object_safe() {
        fn assert_object_safe<T: ?Sized>() {}
        assert_object_safe::<dyn ResourceAdmissionPort>();
        assert_object_safe::<dyn BuiltinAdmissionDefaultsPort>();
        assert_object_safe::<dyn GeneratedLifecyclePort>();
        assert_object_safe::<dyn GeneratedResourceMutationPort>();
        assert_object_safe::<dyn GenericCommandStore>();
        assert_object_safe::<dyn GenericCommandAdmission>();
        assert_object_safe::<dyn GenericCommandLifecycle>();
        assert_object_safe::<dyn GenericCommandAuthorization>();
        assert_object_safe::<dyn GenericCommandReconcile>();
        assert_object_safe::<dyn GenericCommandRuntime>();
        assert_object_safe::<dyn GenericCommandPolicy>();
        assert_object_safe::<dyn GenericCommandState>();
    }
}
