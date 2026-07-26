use klights_leader_api::CrdRegistry;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct ApiResourceMutationServices {
    pub(crate) db: crate::api::state_ports::ApiResourceStore,
    pub(crate) resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
    pub(crate) resource_command: Arc<dyn klights_leader_api::LeaderResourceCommand>,
    pub(crate) finalizer_lifecycle: Arc<dyn klights_reconcile_api::FinalizerLifecyclePort>,
    pub(crate) mutation_effects: Arc<dyn klights_reconcile_api::ResourceMutationEffectsPort>,
    pub(crate) list_resource_versions: Arc<dyn crate::api::query::ListResourceVersionPort>,
    pub(crate) namespace_lists: Arc<dyn crate::api::query::NamespaceListPort>,
    pub(crate) quota_runtime: Arc<dyn klights_reconcile_api::ResourceQuotaAdmissionRuntime>,
    pub(crate) admission: Arc<dyn crate::api::admission_ports::ResourceAdmissionPort>,
    pub(crate) custom_resource_reads:
        Arc<dyn crate::api::custom_resource_ports::CustomResourceReadPort>,
    pub(crate) builtin_admission_defaults:
        Arc<dyn crate::api::generated_handler_ports::BuiltinAdmissionDefaultsPort>,
    pub(crate) generated_lifecycle:
        Arc<dyn crate::api::generated_handler_ports::GeneratedLifecyclePort>,
    pub(crate) generated_mutations:
        Arc<dyn crate::api::generated_handler_ports::GeneratedResourceMutationPort>,
    pub(crate) generated_watch: Arc<dyn crate::api::generated_handler_ports::GeneratedWatchPort>,
    pub(crate) gc_owner_lifecycle: Arc<dyn klights_reconcile_api::GcOwnerLifecyclePort>,
    pub(crate) pod_repository: crate::api::state_ports::ApiPodRepository,
}

#[derive(Clone)]
pub(crate) struct ApiAuthenticators {
    pub(crate) bootstrap_token: Arc<dyn crate::auth::middleware::BootstrapTokenAuthenticator>,
    pub(crate) oidc: Option<Arc<dyn crate::auth::oidc::OidcValidator>>,
    pub(crate) webhook: Option<Arc<dyn crate::auth::webhook_auth::WebhookAuthenticator>>,
}

impl ApiAuthenticators {
    pub(crate) fn new(
        bootstrap_token: Arc<dyn crate::auth::middleware::BootstrapTokenAuthenticator>,
        oidc: Option<Arc<dyn crate::auth::oidc::OidcValidator>>,
        webhook: Option<Arc<dyn crate::auth::webhook_auth::WebhookAuthenticator>>,
    ) -> Self {
        Self {
            bootstrap_token,
            oidc,
            webhook,
        }
    }
}

#[derive(Clone)]
pub struct ApiAuthPolicy {
    pub(crate) authorizer: Arc<dyn crate::auth::authorizer::Authorizer>,
    pub(crate) audit_sink: Arc<dyn crate::audit::AuditSink>,
    pub(crate) api_priority_fairness: Arc<crate::api_priority_fairness::ApiPriorityFairness>,
    pub(crate) rbac_policy_store: Arc<dyn crate::auth::rbac_policy_store::RbacPolicyStore>,
    pub(crate) bootstrap_token_authenticator:
        Arc<dyn crate::auth::middleware::BootstrapTokenAuthenticator>,
    pub(crate) oidc_authenticator: Option<Arc<dyn crate::auth::oidc::OidcValidator>>,
    pub(crate) webhook_authenticator:
        Option<Arc<dyn crate::auth::webhook_auth::WebhookAuthenticator>>,
    pub(crate) cluster_ca_pem: Option<Arc<String>>,
}

impl ApiAuthPolicy {
    pub(crate) fn new(
        authorizer: Arc<dyn crate::auth::authorizer::Authorizer>,
        audit_sink: Arc<dyn crate::audit::AuditSink>,
        api_priority_fairness: Arc<crate::api_priority_fairness::ApiPriorityFairness>,
        rbac_policy_store: Arc<dyn crate::auth::rbac_policy_store::RbacPolicyStore>,
        authenticators: ApiAuthenticators,
        cluster_ca_pem: Option<Arc<String>>,
    ) -> Self {
        let ApiAuthenticators {
            bootstrap_token,
            oidc,
            webhook,
        } = authenticators;
        Self {
            authorizer,
            audit_sink,
            api_priority_fairness,
            rbac_policy_store,
            bootstrap_token_authenticator: bootstrap_token,
            oidc_authenticator: oidc,
            webhook_authenticator: webhook,
            cluster_ca_pem,
        }
    }
}

#[derive(Clone)]
pub(crate) struct ApiDiscoveryAggregationServices {
    pub(crate) crd_registry: CrdRegistry,
    pub(crate) apiservice_proxy_identity_cache: Arc<tokio::sync::OnceCell<reqwest::Identity>>,
    pub(crate) apiservice_proxy_cache: Arc<crate::api::apiservice_proxy::ApiServiceProxyCache>,
}

impl ApiDiscoveryAggregationServices {
    pub(crate) fn new(
        crd_registry: CrdRegistry,
        apiservice_proxy_identity_cache: Arc<tokio::sync::OnceCell<reqwest::Identity>>,
        apiservice_proxy_cache: Arc<crate::api::apiservice_proxy::ApiServiceProxyCache>,
    ) -> Self {
        Self {
            crd_registry,
            apiservice_proxy_identity_cache,
            apiservice_proxy_cache,
        }
    }
}

#[derive(Clone)]
pub(crate) struct ApiControllerReconcileServices {
    pub(crate) service_allocations: Arc<dyn klights_reconcile_api::ServiceWriteAllocator>,
    #[cfg(test)]
    pub(crate) service_ipam: Arc<crate::controllers::service::ServiceIpam>,
    #[cfg(test)]
    pub(crate) nodeport_alloc: Arc<crate::controllers::service::NodePortAllocator>,
    #[cfg(not(test))]
    pub(crate) controller_dispatcher: Arc<dyn klights_reconcile_api::ControllerDispatcherPort>,
    #[cfg(test)]
    pub(crate) controller_dispatcher: Arc<crate::controller_dispatcher::ControllerDispatcher>,
    pub(crate) metrics: crate::api::state_ports::ApiFailureMetrics,
    pub(crate) node_lease_tracker: crate::api::state_ports::ApiNodeLeaseObservations,
}

impl ApiControllerReconcileServices {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        service_allocations: Arc<dyn klights_reconcile_api::ServiceWriteAllocator>,
        #[cfg(test)] service_ipam: Arc<crate::controllers::service::ServiceIpam>,
        #[cfg(test)] nodeport_alloc: Arc<crate::controllers::service::NodePortAllocator>,
        #[cfg(not(test))] controller_dispatcher: Arc<
            dyn klights_reconcile_api::ControllerDispatcherPort,
        >,
        #[cfg(test)] controller_dispatcher: Arc<crate::controller_dispatcher::ControllerDispatcher>,
        metrics: crate::api::state_ports::ApiFailureMetrics,
        node_lease_tracker: crate::api::state_ports::ApiNodeLeaseObservations,
    ) -> Self {
        Self {
            service_allocations,
            #[cfg(test)]
            service_ipam,
            #[cfg(test)]
            nodeport_alloc,
            controller_dispatcher,
            metrics,
            node_lease_tracker,
        }
    }
}

#[derive(Clone)]
pub(crate) struct ApiPodNodeSubresourceServices {
    pub(crate) services: Arc<dyn klights_reconcile_api::ServiceRoutingSync>,
    pub(crate) pod_log_follow_watch: crate::api_pod_subresources::logs::PodLogFollowWatchSource,
    pub(crate) local_node_exec: Option<Arc<dyn klights_node_api::NodeExec>>,
    pub(crate) metrics_provider: Arc<dyn crate::metrics::MetricsProvider>,
    pub(crate) node_port_forward: Arc<dyn klights_node_api::NodePortForward>,
    pub(crate) pod_lifecycle_router: crate::api::state_ports::ApiPodLifecycleRouter,
    pub(crate) pod_lifecycle_diagnostics:
        Option<Arc<dyn klights_pod_api::PodLifecycleDiagnosticsQuery>>,
    pub(crate) pod_start_retry_state: Option<Arc<dyn klights_pod_api::PodStartRetryDiagnostics>>,
}

impl ApiPodNodeSubresourceServices {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        services: Arc<dyn klights_reconcile_api::ServiceRoutingSync>,
        pod_log_follow_watch: crate::api_pod_subresources::logs::PodLogFollowWatchSource,
        local_node_exec: Option<Arc<dyn klights_node_api::NodeExec>>,
        metrics_provider: Arc<dyn crate::metrics::MetricsProvider>,
        node_port_forward: Arc<dyn klights_node_api::NodePortForward>,
        pod_lifecycle_router: crate::api::state_ports::ApiPodLifecycleRouter,
        pod_lifecycle_diagnostics: Option<Arc<dyn klights_pod_api::PodLifecycleDiagnosticsQuery>>,
        pod_start_retry_state: Option<Arc<dyn klights_pod_api::PodStartRetryDiagnostics>>,
    ) -> Self {
        Self {
            services,
            pod_log_follow_watch,
            local_node_exec,
            metrics_provider,
            node_port_forward,
            pod_lifecycle_router,
            pod_lifecycle_diagnostics,
            pod_start_retry_state,
        }
    }
}

#[derive(Clone)]
pub(crate) enum ApiNodeRole {
    Leader,
    Controlplane {
        leader_endpoints: Vec<String>,
        as_learner: bool,
    },
    Worker {
        leader_endpoints: Vec<String>,
    },
}

#[derive(Clone)]
pub(crate) struct ApiOperationalConfig {
    pub(crate) node_name: String,
    pub(crate) containerd_namespace: String,
    pub(crate) anonymous_auth: bool,
    pub(crate) cluster_cidr: String,
}

impl ApiOperationalConfig {
    pub(crate) fn new(
        node_name: String,
        containerd_namespace: String,
        anonymous_auth: bool,
        cluster_cidr: String,
    ) -> Self {
        Self {
            node_name,
            containerd_namespace,
            anonymous_auth,
            cluster_cidr,
        }
    }

    #[cfg(test)]
    pub(crate) fn from_test(config: crate::KlightsConfig) -> Arc<Self> {
        Arc::new(Self::new(
            config.node_name,
            config.containerd_namespace,
            config.anonymous_auth,
            config.cluster_cidr,
        ))
    }
}

#[derive(Clone)]
pub(crate) struct ApiRemoteNodeServices {
    pub(crate) exec: Arc<dyn klights_node_api::NodeExec>,
    pub(crate) logs: Arc<dyn klights_node_api::NodeLog>,
    pub(crate) diagnostics: Arc<dyn klights_leader_api::LeaderFollowerDiagnostics>,
}

impl ApiRemoteNodeServices {
    pub(crate) fn new(
        exec: Arc<dyn klights_node_api::NodeExec>,
        logs: Arc<dyn klights_node_api::NodeLog>,
        diagnostics: Arc<dyn klights_leader_api::LeaderFollowerDiagnostics>,
    ) -> Self {
        Self {
            exec,
            logs,
            diagnostics,
        }
    }

    #[cfg(test)]
    pub(crate) fn from_test(replication: Arc<crate::replication::ReplicationService>) -> Self {
        Self::new(replication.clone(), replication.clone(), replication)
    }
}

#[derive(Clone)]
pub(crate) struct ApiOperationalServices {
    pub(crate) role: ApiNodeRole,
    pub(crate) replication: Option<ApiRemoteNodeServices>,
    pub(crate) config: Arc<ApiOperationalConfig>,
    pub(crate) cluster_status: Arc<dyn klights_leader_api::LeaderClusterStatusMetadata>,
    pub(crate) task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
    pub(crate) file_process: klights_supervisor::FileProcessExecutor,
    pub(crate) is_raft_leader_rx: Option<Arc<crate::api::raft_proxy::RaftLeaderProxy>>,
}

impl ApiOperationalServices {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        role: ApiNodeRole,
        replication: Option<ApiRemoteNodeServices>,
        config: Arc<ApiOperationalConfig>,
        cluster_status: Arc<dyn klights_leader_api::LeaderClusterStatusMetadata>,
        task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
        file_process: klights_supervisor::FileProcessExecutor,
        is_raft_leader_rx: Option<Arc<crate::api::raft_proxy::RaftLeaderProxy>>,
    ) -> Self {
        Self {
            role,
            replication,
            config,
            cluster_status,
            task_supervisor,
            file_process,
            is_raft_leader_rx,
        }
    }
}

#[derive(Clone)]
pub struct AppState {
    auth_policy: ApiAuthPolicy,
    resource_mutation: ApiResourceMutationServices,
    discovery: ApiDiscoveryAggregationServices,
    controller_reconcile: ApiControllerReconcileServices,
    pod_node_subresources: ApiPodNodeSubresourceServices,
    operational: ApiOperationalServices,
}

impl AppState {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        auth_policy: ApiAuthPolicy,
        resource_mutation: ApiResourceMutationServices,
        discovery: ApiDiscoveryAggregationServices,
        controller_reconcile: ApiControllerReconcileServices,
        pod_node_subresources: ApiPodNodeSubresourceServices,
        operational: ApiOperationalServices,
    ) -> Self {
        Self {
            auth_policy,
            resource_mutation,
            discovery,
            controller_reconcile,
            pod_node_subresources,
            operational,
        }
    }

    pub(crate) fn with_local_node_exec(
        mut self,
        local_node_exec: Option<Arc<dyn klights_node_api::NodeExec>>,
    ) -> Self {
        self.pod_node_subresources.local_node_exec = local_node_exec;
        self
    }

    pub(crate) fn resource_mutation(&self) -> &ApiResourceMutationServices {
        &self.resource_mutation
    }

    pub(crate) fn discovery(&self) -> &ApiDiscoveryAggregationServices {
        &self.discovery
    }

    pub(crate) fn controller_reconcile(&self) -> &ApiControllerReconcileServices {
        &self.controller_reconcile
    }

    pub(crate) fn pod_node_subresources(&self) -> &ApiPodNodeSubresourceServices {
        &self.pod_node_subresources
    }

    pub(crate) fn operational(&self) -> &ApiOperationalServices {
        &self.operational
    }

    #[cfg(test)]
    pub(crate) fn resource_mutation_mut(&mut self) -> &mut ApiResourceMutationServices {
        &mut self.resource_mutation
    }

    #[cfg(test)]
    pub(crate) fn discovery_mut(&mut self) -> &mut ApiDiscoveryAggregationServices {
        &mut self.discovery
    }

    #[cfg(test)]
    pub(crate) fn controller_reconcile_mut(&mut self) -> &mut ApiControllerReconcileServices {
        &mut self.controller_reconcile
    }

    #[cfg(test)]
    pub(crate) fn pod_node_subresources_mut(&mut self) -> &mut ApiPodNodeSubresourceServices {
        &mut self.pod_node_subresources
    }

    #[cfg(test)]
    pub(crate) fn operational_mut(&mut self) -> &mut ApiOperationalServices {
        &mut self.operational
    }
}

impl std::ops::Deref for AppState {
    type Target = ApiAuthPolicy;

    fn deref(&self) -> &Self::Target {
        &self.auth_policy
    }
}

impl std::ops::DerefMut for AppState {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.auth_policy
    }
}
