use klights_leader_api::CrdRegistry;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct ApiResourceMutationServices {
    #[cfg(test)]
    pub(crate) db: crate::datastore::DatastoreHandle,
    pub(crate) watch_stream: Arc<dyn crate::api::watch_stream::WatchStreamSource>,
    pub(crate) namespace_termination: Arc<dyn klights_reconcile_api::NamespaceLifecycleStore>,
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
    #[cfg(not(test))]
    pub(crate) pod_repository: Arc<dyn crate::api::state_ports::ApiPodRepository>,
    #[cfg(test)]
    pub(crate) pod_repository: Arc<crate::kubelet::pod_repository::PodRepository>,
}

#[derive(Clone)]
pub(crate) struct ApiAuthenticators {
    pub(crate) bootstrap_token:
        Arc<dyn klights_auth::cluster_identity::BootstrapTokenAuthenticator>,
    pub(crate) oidc: Option<Arc<dyn klights_auth::oidc::OidcValidator>>,
    pub(crate) webhook: Option<Arc<dyn klights_auth::webhook_auth::WebhookAuthenticator>>,
}

impl ApiAuthenticators {
    pub(crate) fn new(
        bootstrap_token: Arc<dyn klights_auth::cluster_identity::BootstrapTokenAuthenticator>,
        oidc: Option<Arc<dyn klights_auth::oidc::OidcValidator>>,
        webhook: Option<Arc<dyn klights_auth::webhook_auth::WebhookAuthenticator>>,
    ) -> Self {
        Self {
            bootstrap_token,
            oidc,
            webhook,
        }
    }
}

#[derive(Clone)]
pub(crate) struct ApiAuthPolicy {
    pub(crate) authorizer: Arc<dyn klights_auth::authorizer::Authorizer>,
    pub(crate) audit_sink: Arc<dyn crate::audit::AuditSink>,
    pub(crate) api_priority_fairness: Arc<crate::api::priority_fairness::ApiPriorityFairness>,
    pub(crate) rbac_policy_store: Arc<dyn klights_auth::rbac_policy_store::RbacPolicyStore>,
    pub(crate) bootstrap_token_authenticator:
        Arc<dyn klights_auth::cluster_identity::BootstrapTokenAuthenticator>,
    pub(crate) oidc_authenticator: Option<Arc<dyn klights_auth::oidc::OidcValidator>>,
    pub(crate) webhook_authenticator:
        Option<Arc<dyn klights_auth::webhook_auth::WebhookAuthenticator>>,
    pub(crate) cluster_ca_pem: Option<Arc<String>>,
}

impl ApiAuthPolicy {
    pub(crate) fn new(
        authorizer: Arc<dyn klights_auth::authorizer::Authorizer>,
        audit_sink: Arc<dyn crate::audit::AuditSink>,
        api_priority_fairness: Arc<crate::api::priority_fairness::ApiPriorityFairness>,
        rbac_policy_store: Arc<dyn klights_auth::rbac_policy_store::RbacPolicyStore>,
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
    pub(crate) controller_dispatcher: Arc<crate::controllers::ControllerDispatcher>,
    #[cfg(not(test))]
    pub(crate) metrics: Arc<dyn crate::api::state_ports::ApiFailureMetrics>,
    #[cfg(test)]
    pub(crate) metrics: Arc<crate::side_effects::SideEffectMetrics>,
    #[cfg(not(test))]
    pub(crate) node_lease_tracker: Arc<dyn crate::api::state_ports::ApiNodeLeaseObservations>,
    #[cfg(test)]
    pub(crate) node_lease_tracker: Arc<crate::node_lease_tracker::NodeLeaseTracker>,
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
        #[cfg(test)] controller_dispatcher: Arc<crate::controllers::ControllerDispatcher>,
        #[cfg(not(test))] metrics: Arc<dyn crate::api::state_ports::ApiFailureMetrics>,
        #[cfg(test)] metrics: Arc<crate::side_effects::SideEffectMetrics>,
        #[cfg(not(test))] node_lease_tracker: Arc<
            dyn crate::api::state_ports::ApiNodeLeaseObservations,
        >,
        #[cfg(test)] node_lease_tracker: Arc<crate::node_lease_tracker::NodeLeaseTracker>,
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
    pub(crate) pod_log_follow_watch: crate::api::pod_subresources::logs::PodLogFollowWatchSource,
    pub(crate) local_node_exec: Option<Arc<dyn klights_node_api::NodeExec>>,
    pub(crate) node_metrics: Arc<dyn klights_node_api::NodeMetrics>,
    pub(crate) node_port_forward: Arc<dyn klights_node_api::NodePortForward>,
    #[cfg(test)]
    pub(crate) pod_lifecycle_router:
        Option<Arc<crate::kubelet::pod_lifecycle_router::PodLifecycleRouter>>,
    pub(crate) pod_lifecycle_diagnostics:
        Option<Arc<dyn klights_pod_api::PodLifecycleDiagnosticsQuery>>,
    pub(crate) pod_start_retry_state: Option<Arc<dyn klights_pod_api::PodStartRetryDiagnostics>>,
}

impl ApiPodNodeSubresourceServices {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        services: Arc<dyn klights_reconcile_api::ServiceRoutingSync>,
        pod_log_follow_watch: crate::api::pod_subresources::logs::PodLogFollowWatchSource,
        local_node_exec: Option<Arc<dyn klights_node_api::NodeExec>>,
        node_metrics: Arc<dyn klights_node_api::NodeMetrics>,
        node_port_forward: Arc<dyn klights_node_api::NodePortForward>,
        #[cfg(test)] pod_lifecycle_router: Option<
            Arc<crate::kubelet::pod_lifecycle_router::PodLifecycleRouter>,
        >,
        pod_lifecycle_diagnostics: Option<Arc<dyn klights_pod_api::PodLifecycleDiagnosticsQuery>>,
        pod_start_retry_state: Option<Arc<dyn klights_pod_api::PodStartRetryDiagnostics>>,
    ) -> Self {
        Self {
            services,
            pod_log_follow_watch,
            local_node_exec,
            node_metrics,
            node_port_forward,
            #[cfg(test)]
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
    pub(crate) anonymous_auth: bool,
    pub(crate) runtime: ApiRuntimeInputs,
    pub(crate) version_info: crate::api::version::VersionInfo,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ApiRuntimePaths {
    pub(crate) ca_cert: std::path::PathBuf,
    pub(crate) api_proxy_cert: std::path::PathBuf,
    pub(crate) api_proxy_key: std::path::PathBuf,
    pub(crate) apiservice_proxy_cert: std::path::PathBuf,
    pub(crate) apiservice_proxy_key: std::path::PathBuf,
    pub(crate) pod_logs_root: std::path::PathBuf,
}

impl ApiRuntimePaths {
    pub(crate) fn from_data_root(data_root: std::path::PathBuf) -> anyhow::Result<Self> {
        anyhow::ensure!(
            data_root.is_absolute(),
            "API runtime data root must be absolute: {}",
            data_root.display()
        );
        let etc = data_root.join("etc");
        Ok(Self {
            ca_cert: etc.join("ca.crt"),
            api_proxy_cert: etc.join("api-proxy.crt"),
            api_proxy_key: etc.join("api-proxy.key"),
            apiservice_proxy_cert: etc.join("apiservice-proxy.crt"),
            apiservice_proxy_key: etc.join("apiservice-proxy.key"),
            pod_logs_root: data_root.join("logs").join("pods"),
        })
    }

    pub(crate) fn pod_log_dir(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
    ) -> std::path::PathBuf {
        self.pod_logs_root
            .join(format!("{namespace}_{pod_name}_{pod_uid}"))
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ApiRuntimeInputs {
    pub(crate) paths: ApiRuntimePaths,
    pub(crate) slow_log_threshold: std::time::Duration,
}

impl ApiRuntimeInputs {
    pub(crate) fn new(
        paths: ApiRuntimePaths,
        slow_log_threshold: std::time::Duration,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !slow_log_threshold.is_zero(),
            "API slow-log threshold must be positive"
        );
        Ok(Self {
            paths,
            slow_log_threshold,
        })
    }
}

impl ApiOperationalConfig {
    pub(crate) fn new(
        node_name: String,
        anonymous_auth: bool,
        runtime: ApiRuntimeInputs,
        version_info: crate::api::version::VersionInfo,
    ) -> Self {
        Self {
            node_name,
            anonymous_auth,
            runtime,
            version_info,
        }
    }

    #[cfg(test)]
    pub(crate) fn from_test(config: crate::KlightsConfig) -> Arc<Self> {
        let paths = ApiRuntimePaths::from_data_root(config.data_root.clone())
            .expect("test API data root must be absolute");
        let runtime = ApiRuntimeInputs::new(paths, config.api_slow_log_threshold)
            .expect("test API slow-log threshold must be positive");
        Arc::new(Self::new(
            config.node_name,
            config.anonymous_auth,
            runtime,
            crate::api::version::VersionInfo::new(
                "1",
                "34",
                "v1.34.6+klights-test",
                "test-commit",
                "clean",
                "",
                "rustc test",
                "test-target",
            ),
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
    pub(crate) fn from_test(replication: Arc<klights_replication::ReplicationService>) -> Self {
        let runtime = crate::bootstrap::grpc_runtime_adapter::GrpcReplicationRuntimeAdapter::new(
            replication.clone(),
        );
        Self::new(runtime.clone(), runtime, replication)
    }
}

#[derive(Clone)]
pub(crate) struct ApiOperationalServices {
    pub(crate) role: ApiNodeRole,
    pub(crate) replication: Option<ApiRemoteNodeServices>,
    pub(crate) config: Arc<ApiOperationalConfig>,
    pub(crate) clock: Arc<dyn klights_auth::clock::Clock>,
    pub(crate) cluster_status: Arc<dyn klights_leader_api::LeaderClusterStatusMetadata>,
    pub(crate) task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
    pub(crate) file_process: klights_supervisor::FileProcessExecutor,
    pub(crate) signing_keys:
        Arc<dyn klights_auth::cluster_identity::ServiceAccountSigningKeyProvider>,
    pub(crate) authority: Option<Arc<dyn klights_leader_api::LeaderAuthority>>,
}

impl ApiOperationalServices {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        role: ApiNodeRole,
        replication: Option<ApiRemoteNodeServices>,
        config: Arc<ApiOperationalConfig>,
        clock: Arc<dyn klights_auth::clock::Clock>,
        cluster_status: Arc<dyn klights_leader_api::LeaderClusterStatusMetadata>,
        task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
        file_process: klights_supervisor::FileProcessExecutor,
        signing_keys: Arc<dyn klights_auth::cluster_identity::ServiceAccountSigningKeyProvider>,
        authority: Option<Arc<dyn klights_leader_api::LeaderAuthority>>,
    ) -> Self {
        Self {
            role,
            replication,
            config,
            clock,
            cluster_status,
            task_supervisor,
            file_process,
            signing_keys,
            authority,
        }
    }
}

#[cfg(not(test))]
pub(crate) enum RootApiRole {
    Leader,
    Controlplane {
        leader_endpoints: Vec<String>,
        as_learner: bool,
    },
    Worker {
        leader_endpoints: Vec<String>,
    },
}

#[cfg(not(test))]
pub(crate) type RootApiRemoteNodeServices = (
    Arc<dyn klights_node_api::NodeExec>,
    Arc<dyn klights_node_api::NodeLog>,
    Arc<dyn klights_leader_api::LeaderFollowerDiagnostics>,
);

/// Consumes root-owned capabilities and seals them inside the private HTTP
/// state. The composition root receives only the completed router back.
#[cfg(not(test))]
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_router_from_root(
    authorizer: Arc<dyn klights_auth::authorizer::Authorizer>,
    rbac_policy_store: Arc<dyn klights_auth::rbac_policy_store::RbacPolicyStore>,
    bootstrap_token: Arc<dyn klights_auth::cluster_identity::BootstrapTokenAuthenticator>,
    oidc: Option<Arc<dyn klights_auth::oidc::OidcValidator>>,
    webhook: Option<Arc<dyn klights_auth::webhook_auth::WebhookAuthenticator>>,
    cluster_ca_pem: Option<Arc<String>>,
    watch_stream: Arc<dyn crate::api::watch_stream::WatchStreamSource>,
    namespace_termination: Arc<dyn klights_reconcile_api::NamespaceLifecycleStore>,
    resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
    resource_command: Arc<dyn klights_leader_api::LeaderResourceCommand>,
    finalizer_lifecycle: Arc<dyn klights_reconcile_api::FinalizerLifecyclePort>,
    mutation_effects: Arc<dyn klights_reconcile_api::ResourceMutationEffectsPort>,
    list_resource_versions: Arc<dyn crate::api::query::ListResourceVersionPort>,
    namespace_lists: Arc<dyn crate::api::query::NamespaceListPort>,
    quota_runtime: Arc<dyn klights_reconcile_api::ResourceQuotaAdmissionRuntime>,
    admission: Arc<dyn crate::api::admission_ports::ResourceAdmissionPort>,
    custom_resource_reads: Arc<dyn crate::api::custom_resource_ports::CustomResourceReadPort>,
    builtin_admission_defaults: Arc<
        dyn crate::api::generated_handler_ports::BuiltinAdmissionDefaultsPort,
    >,
    generated_lifecycle: Arc<dyn crate::api::generated_handler_ports::GeneratedLifecyclePort>,
    generated_mutations: Arc<
        dyn crate::api::generated_handler_ports::GeneratedResourceMutationPort,
    >,
    generated_watch: Arc<dyn crate::api::generated_handler_ports::GeneratedWatchPort>,
    gc_owner_lifecycle: Arc<dyn klights_reconcile_api::GcOwnerLifecyclePort>,
    pod_repository: Arc<dyn crate::api::state_ports::ApiPodRepository>,
    crd_registry: CrdRegistry,
    service_allocations: Arc<dyn klights_reconcile_api::ServiceWriteAllocator>,
    controller_dispatcher: Arc<dyn klights_reconcile_api::ControllerDispatcherPort>,
    metrics: Arc<dyn crate::api::state_ports::ApiFailureMetrics>,
    node_lease_tracker: Arc<dyn crate::api::state_ports::ApiNodeLeaseObservations>,
    services: Arc<dyn klights_reconcile_api::ServiceRoutingSync>,
    pod_log_follow_watch: crate::api::pod_subresources::logs::PodLogFollowWatchSource,
    local_node_exec: Option<Arc<dyn klights_node_api::NodeExec>>,
    node_metrics: Arc<dyn klights_node_api::NodeMetrics>,
    node_port_forward: Arc<dyn klights_node_api::NodePortForward>,
    pod_lifecycle_diagnostics: Option<Arc<dyn klights_pod_api::PodLifecycleDiagnosticsQuery>>,
    pod_start_retry_state: Option<Arc<dyn klights_pod_api::PodStartRetryDiagnostics>>,
    role: RootApiRole,
    replication: Option<RootApiRemoteNodeServices>,
    node_name: String,
    anonymous_auth: bool,
    runtime_inputs: ApiRuntimeInputs,
    version_info: crate::api::version::VersionInfo,
    clock: Arc<dyn klights_auth::clock::Clock>,
    cluster_status: Arc<dyn klights_leader_api::LeaderClusterStatusMetadata>,
    task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
    signing_keys: Arc<dyn klights_auth::cluster_identity::ServiceAccountSigningKeyProvider>,
    authority: Option<Arc<dyn klights_leader_api::LeaderAuthority>>,
) -> (axum::Router, crate::api::routes::NativeApiOuterLayers) {
    let role = match role {
        RootApiRole::Leader => ApiNodeRole::Leader,
        RootApiRole::Controlplane {
            leader_endpoints,
            as_learner,
        } => ApiNodeRole::Controlplane {
            leader_endpoints,
            as_learner,
        },
        RootApiRole::Worker { leader_endpoints } => ApiNodeRole::Worker { leader_endpoints },
    };
    let replication = replication
        .map(|(exec, logs, diagnostics)| ApiRemoteNodeServices::new(exec, logs, diagnostics));
    let state = ApiState::new(
        ApiAuthPolicy::new(
            authorizer,
            crate::audit::default_audit_sink(),
            Arc::new(crate::api::priority_fairness::ApiPriorityFairness::new()),
            rbac_policy_store,
            ApiAuthenticators::new(bootstrap_token, oidc, webhook),
            cluster_ca_pem,
        ),
        ApiResourceMutationServices {
            watch_stream,
            namespace_termination,
            resource_query,
            resource_command,
            finalizer_lifecycle,
            mutation_effects,
            list_resource_versions,
            namespace_lists,
            quota_runtime,
            admission,
            custom_resource_reads,
            builtin_admission_defaults,
            generated_lifecycle,
            generated_mutations,
            generated_watch,
            gc_owner_lifecycle,
            pod_repository,
        },
        ApiDiscoveryAggregationServices::new(
            crd_registry,
            Arc::new(tokio::sync::OnceCell::new()),
            Arc::new(crate::api::apiservice_proxy::ApiServiceProxyCache::default()),
        ),
        ApiControllerReconcileServices::new(
            service_allocations,
            controller_dispatcher,
            metrics,
            node_lease_tracker,
        ),
        ApiPodNodeSubresourceServices::new(
            services,
            pod_log_follow_watch,
            local_node_exec,
            node_metrics,
            node_port_forward,
            pod_lifecycle_diagnostics,
            pod_start_retry_state,
        ),
        ApiOperationalServices::new(
            role,
            replication,
            Arc::new(ApiOperationalConfig::new(
                node_name,
                anonymous_auth,
                runtime_inputs,
                version_info,
            )),
            clock,
            cluster_status,
            task_supervisor.clone(),
            klights_supervisor::FileProcessExecutor::new(task_supervisor),
            signing_keys,
            authority,
        ),
    );
    crate::api::routes::build_router_parts(state)
}

#[derive(Clone)]
#[cfg(not(test))]
pub(super) struct ApiState {
    auth_policy: ApiAuthPolicy,
    resource_mutation: ApiResourceMutationServices,
    discovery: ApiDiscoveryAggregationServices,
    controller_reconcile: ApiControllerReconcileServices,
    pod_node_subresources: ApiPodNodeSubresourceServices,
    operational: ApiOperationalServices,
}

#[derive(Clone)]
#[cfg(test)]
pub(crate) struct ApiState {
    auth_policy: ApiAuthPolicy,
    resource_mutation: ApiResourceMutationServices,
    discovery: ApiDiscoveryAggregationServices,
    controller_reconcile: ApiControllerReconcileServices,
    pod_node_subresources: ApiPodNodeSubresourceServices,
    operational: ApiOperationalServices,
}

impl ApiState {
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

    #[cfg(test)]
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

    pub(crate) fn auth_policy(&self) -> &ApiAuthPolicy {
        &self.auth_policy
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

#[cfg(test)]
impl std::ops::Deref for ApiState {
    type Target = ApiAuthPolicy;

    fn deref(&self) -> &Self::Target {
        &self.auth_policy
    }
}

#[cfg(test)]
impl std::ops::DerefMut for ApiState {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.auth_policy
    }
}

#[cfg(test)]
mod runtime_input_tests {
    use super::*;

    #[test]
    fn api_runtime_paths_are_derived_from_one_absolute_root() {
        let paths = ApiRuntimePaths::from_data_root(std::path::PathBuf::from("/srv/klights"))
            .expect("absolute root");
        assert_eq!(
            paths.ca_cert,
            std::path::PathBuf::from("/srv/klights/etc/ca.crt")
        );
        assert_eq!(
            paths.api_proxy_cert,
            std::path::PathBuf::from("/srv/klights/etc/api-proxy.crt")
        );
        assert_eq!(
            paths.apiservice_proxy_key,
            std::path::PathBuf::from("/srv/klights/etc/apiservice-proxy.key")
        );
        assert_eq!(
            paths.pod_log_dir("default", "pod", "uid"),
            std::path::PathBuf::from("/srv/klights/logs/pods/default_pod_uid")
        );
    }

    #[test]
    fn api_runtime_paths_reject_relative_root_and_inputs_reject_zero_threshold() {
        assert!(
            ApiRuntimePaths::from_data_root(std::path::PathBuf::from("relative/root")).is_err()
        );
        let paths = ApiRuntimePaths::from_data_root(std::path::PathBuf::from("/srv/klights"))
            .expect("absolute root");
        assert!(ApiRuntimeInputs::new(paths, std::time::Duration::ZERO).is_err());
    }
}
