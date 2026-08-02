use klights_leader_api::CrdRegistry;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct ApiResourceMutationServices {
    pub(crate) identity: Arc<dyn crate::ApiIdentityGenerator>,
    pub(crate) watch_stream: Arc<dyn crate::current::watch_stream::WatchStreamSource>,
    pub(crate) namespace_termination: Arc<dyn klights_reconcile_api::NamespaceLifecycleStore>,
    pub(crate) resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
    pub(crate) resource_command: Arc<dyn klights_leader_api::LeaderResourceCommand>,
    pub(crate) finalizer_lifecycle: Arc<dyn klights_reconcile_api::FinalizerLifecyclePort>,
    pub(crate) mutation_effects: Arc<dyn klights_reconcile_api::ResourceMutationEffectsPort>,
    pub(crate) list_resource_versions: Arc<dyn crate::current::query::ListResourceVersionPort>,
    pub(crate) namespace_lists: Arc<dyn crate::current::query::NamespaceListPort>,
    pub(crate) quota_runtime: Arc<dyn klights_reconcile_api::ResourceQuotaAdmissionRuntime>,
    pub(crate) admission: Arc<dyn crate::generic_command::ResourceAdmissionPort>,
    pub(crate) custom_resource_reads:
        Arc<dyn crate::current::custom_resource_ports::CustomResourceReadPort>,
    pub(crate) builtin_admission_defaults:
        Arc<dyn crate::generic_command::BuiltinAdmissionDefaultsPort>,
    pub(crate) generated_lifecycle: Arc<dyn crate::generic_command::GeneratedLifecyclePort>,
    pub(crate) generated_mutations: Arc<dyn crate::generic_command::GeneratedResourceMutationPort>,
    pub(crate) generated_watch:
        Arc<dyn crate::current::generated_handler_ports::GeneratedWatchPort>,
    pub(crate) gc_owner_lifecycle: Arc<dyn klights_reconcile_api::GcOwnerLifecyclePort>,
    pub(crate) pod_repository: Arc<dyn crate::current::state_ports::ApiPodRepository>,
}

impl crate::generic_command::GenericCommandStore for ApiResourceMutationServices {
    fn identity(&self) -> &dyn crate::ApiIdentityGenerator {
        self.identity.as_ref()
    }

    fn identity_owned(&self) -> Arc<dyn crate::ApiIdentityGenerator> {
        self.identity.clone()
    }

    fn resource_query(&self) -> &dyn klights_leader_api::LeaderResourceQuery {
        self.resource_query.as_ref()
    }

    fn resource_command(&self) -> &dyn klights_leader_api::LeaderResourceCommand {
        self.resource_command.as_ref()
    }

    fn finalizer_lifecycle(&self) -> &dyn klights_reconcile_api::FinalizerLifecyclePort {
        self.finalizer_lifecycle.as_ref()
    }

    fn generated_mutations(&self) -> &dyn crate::generic_command::GeneratedResourceMutationPort {
        self.generated_mutations.as_ref()
    }

    fn pod_mutation(&self) -> &dyn klights_pod_api::PodApiMutation {
        self.pod_repository.as_ref()
    }

    fn pod_subresource_mutation(&self) -> &dyn klights_pod_api::PodSubresourceMutation {
        self.pod_repository.as_ref()
    }

    fn pod_eviction_admission(&self) -> Arc<dyn klights_reconcile_api::PodEvictionAdmissionSink> {
        self.pod_repository.eviction_admission_port()
    }

    fn pod_eviction_delete(&self) -> &dyn klights_pod_api::PodEvictionDelete {
        self.pod_repository.as_ref()
    }
}

impl crate::generic_command::GenericCommandAdmission for ApiResourceMutationServices {
    fn admission(&self) -> &dyn crate::generic_command::ResourceAdmissionPort {
        self.admission.as_ref()
    }

    fn quota_runtime(&self) -> &dyn klights_reconcile_api::ResourceQuotaAdmissionRuntime {
        self.quota_runtime.as_ref()
    }

    fn builtin_admission_defaults(
        &self,
    ) -> &dyn crate::generic_command::BuiltinAdmissionDefaultsPort {
        self.builtin_admission_defaults.as_ref()
    }
}

impl crate::generic_command::GenericCommandLifecycle for ApiResourceMutationServices {
    fn mutation_effects(&self) -> &dyn klights_reconcile_api::ResourceMutationEffectsPort {
        self.mutation_effects.as_ref()
    }

    fn generated_lifecycle(&self) -> &dyn crate::generic_command::GeneratedLifecyclePort {
        self.generated_lifecycle.as_ref()
    }

    fn gc_owner_lifecycle(&self) -> &dyn klights_reconcile_api::GcOwnerLifecyclePort {
        self.gc_owner_lifecycle.as_ref()
    }

    fn gc_owner_lifecycle_owned(&self) -> Arc<dyn klights_reconcile_api::GcOwnerLifecyclePort> {
        self.gc_owner_lifecycle.clone()
    }
}

impl crate::discovery::DiscoveryResourceQuery for ApiResourceMutationServices {
    fn resource_query(&self) -> &dyn klights_leader_api::LeaderResourceQuery {
        self.resource_query.as_ref()
    }
}

impl crate::generic_read::GenericReadSnapshotPort for ApiResourceMutationServices {
    fn snapshot_resources_at_rv(
        &self,
        request: crate::generic_read::GenericReadSnapshotRequest,
    ) -> crate::generic_read::GenericReadFuture<'_, crate::generic_read::GenericReadSnapshot> {
        Box::pin(async move {
            let snapshot = self
                .custom_resource_reads
                .snapshot_resources_at_rv(
                    crate::current::custom_resource_ports::CustomResourceSnapshotRequest {
                        api_version: request.api_version,
                        kind: request.kind,
                        namespace: request.namespace,
                        label_selector: request.label_selector,
                        field_selector: request.field_selector,
                        limit: request.limit,
                        continue_token: request.continue_token,
                        resource_version: request.resource_version,
                    },
                )
                .await?;
            Ok(match snapshot {
                crate::current::custom_resource_ports::CustomResourceListSnapshot::Current => {
                    crate::generic_read::GenericReadSnapshot::Current
                }
                crate::current::custom_resource_ports::CustomResourceListSnapshot::Expired => {
                    crate::generic_read::GenericReadSnapshot::Expired
                }
                crate::current::custom_resource_ports::CustomResourceListSnapshot::List(list) => {
                    crate::generic_read::GenericReadSnapshot::List(list)
                }
            })
        })
    }
}

impl crate::generic_read::GenericReadResourceInputs for ApiResourceMutationServices {
    fn resource_query(&self) -> &dyn klights_leader_api::LeaderResourceQuery {
        self.resource_query.as_ref()
    }

    fn snapshot_port(&self) -> &dyn crate::generic_read::GenericReadSnapshotPort {
        self
    }

    fn resource_versions(&self) -> &dyn crate::generic_read::ListResourceVersionPort {
        self.list_resource_versions.as_ref()
    }

    fn prepare_resource_for_read(
        &self,
        api_version: &'static str,
        kind: &'static str,
        resource: klights_cluster_core::Resource,
        is_get: bool,
    ) -> crate::generic_read::GenericReadFuture<'_, serde_json::Value> {
        Box::pin(async move {
            let resource = if is_get && api_version == "v1" && kind == "Secret" {
                self.generated_lifecycle
                    .rotate_bootstrap_token_secret(resource)
                    .await?
            } else {
                resource
            };
            let mut value = crate::current::inject_resource_version_with_identity(
                self.identity.as_ref(),
                resource.data,
                resource.resource_version,
            );
            crate::current::normalize_resource_for_read(api_version, kind, &mut value);
            Ok(value)
        })
    }

    fn build_watch(
        &self,
        request: crate::generic_read::GenericReadWatchRequest,
    ) -> crate::generic_read::GenericReadFuture<'_, axum::response::Response> {
        Box::pin(async move {
            let query = request.query;
            let send_initial_events = query.send_initial_events.as_deref() == Some("true");
            let explicit_resource_version_zero = query
                .resource_version
                .as_deref()
                .is_some_and(|resource_version| resource_version.trim() == "0");
            let requested_resource_version = query
                .resource_version
                .as_ref()
                .and_then(|resource_version| resource_version.parse::<i64>().ok())
                .unwrap_or(0);
            let send_bookmarks = query.allow_watch_bookmarks.as_deref() == Some("true");
            let table_format = crate::response::wants_table_format(&request.headers)?;
            let protobuf_supported =
                crate::current::watch_stream::protobuf_watch_supported_for_request(
                    request.api_version,
                    request.kind,
                    table_format,
                    query.label_selector.as_deref(),
                    query.field_selector.as_deref(),
                );
            let stream_format = crate::current::watch_stream::negotiate_watch_stream_format(
                &request.headers,
                protobuf_supported,
            )?;
            let body = self
                .generated_watch
                .build_watch_stream(
                    crate::current::generated_handler_ports::GeneratedWatchRequest {
                        api_version: request.api_version.to_string(),
                        kind: request.kind.to_string(),
                        namespace: request.namespace,
                        requested_resource_version,
                        send_initial_events,
                        send_bookmarks,
                        label_selector: query.label_selector,
                        field_selector: query.field_selector,
                        table_format,
                        stream_format,
                        timeout_seconds: query.timeout_seconds,
                        emit_initial_state_for_resource_version_zero:
                            explicit_resource_version_zero,
                        wall_clock: request.wall_clock,
                    },
                )
                .await;
            Ok(axum::response::Response::builder()
                .header("Content-Type", stream_format.content_type())
                .header("Transfer-Encoding", "chunked")
                .body(body)
                .expect("static watch response headers must be valid"))
        })
    }

    fn render_list(
        &self,
        response: crate::generic_read::GenericListResponse,
    ) -> Result<axum::response::Response, crate::AppError> {
        use axum::response::IntoResponse as _;

        let resource_version = response.response_rv.to_string();
        let operation_now = time::OffsetDateTime::from_unix_timestamp_nanos(
            response.operation_unix_timestamp_nanos,
        )
        .map_err(|error| {
            crate::AppError::Internal(format!(
                "operation time is outside the supported timestamp range: {error}"
            ))
        })?;
        if crate::response::wants_table_format(&response.headers)? {
            let table = match response.kind {
                "Pod" => crate::response::pod_list_to_table_at(
                    response.items,
                    resource_version,
                    operation_now,
                ),
                "Node" => crate::response::node_list_to_table_at(
                    response.items,
                    resource_version,
                    operation_now,
                ),
                "ReplicaSet" => crate::response::replicaset_list_to_table_at(
                    response.items,
                    resource_version,
                    operation_now,
                ),
                "Deployment" => crate::response::deployment_list_to_table_at(
                    response.items,
                    resource_version,
                    operation_now,
                ),
                "StatefulSet" => crate::response::statefulset_list_to_table_at(
                    response.items,
                    resource_version,
                    operation_now,
                ),
                _ => crate::response::generic_list_to_table_at(
                    response.kind,
                    response.items,
                    resource_version,
                    operation_now,
                ),
            };
            return Ok(axum::Json(table).into_response());
        }

        let mut metadata = serde_json::json!({"resourceVersion": resource_version});
        if let Some(token) = response.continue_token {
            metadata["continue"] = serde_json::Value::String(token);
        }
        if let Some(remaining) = response.remaining_item_count {
            metadata["remainingItemCount"] = serde_json::json!(remaining);
        }
        Ok(crate::current::K8sResponse::new(
            serde_json::json!({
                "apiVersion": response.api_version,
                "kind": response.list_kind,
                "metadata": metadata,
                "items": response.items,
            }),
            &response.headers,
        )
        .into_response())
    }

    fn render_get(
        &self,
        value: serde_json::Value,
        headers: axum::http::HeaderMap,
    ) -> axum::response::Response {
        use axum::response::IntoResponse as _;
        crate::current::K8sResponse::new(value, &headers).into_response()
    }
}

#[derive(Clone)]
pub(crate) struct ApiAuthenticators {
    pub(crate) bootstrap_token: Arc<dyn klights_leader_api::LeaderBootstrapTokenAuthentication>,
    pub(crate) oidc: Option<Arc<dyn klights_auth::oidc::OidcValidator>>,
    pub(crate) webhook: Option<Arc<dyn klights_auth::webhook_auth::WebhookAuthenticator>>,
}

impl ApiAuthenticators {
    pub(crate) fn new(
        bootstrap_token: Arc<dyn klights_leader_api::LeaderBootstrapTokenAuthentication>,
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
    pub(crate) api_priority_fairness: Arc<crate::priority_fairness::ApiPriorityFairness>,
    pub(crate) rbac_policy_store: Arc<dyn klights_auth::rbac_policy_store::RbacPolicyStore>,
    pub(crate) bootstrap_token_authenticator:
        Arc<dyn klights_leader_api::LeaderBootstrapTokenAuthentication>,
    pub(crate) oidc_authenticator: Option<Arc<dyn klights_auth::oidc::OidcValidator>>,
    pub(crate) webhook_authenticator:
        Option<Arc<dyn klights_auth::webhook_auth::WebhookAuthenticator>>,
    pub(crate) cluster_ca_pem: Option<Arc<String>>,
}

impl ApiAuthPolicy {
    pub(crate) fn new(
        authorizer: Arc<dyn klights_auth::authorizer::Authorizer>,
        audit_sink: Arc<dyn crate::audit::AuditSink>,
        api_priority_fairness: Arc<crate::priority_fairness::ApiPriorityFairness>,
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
    pub(crate) apiservice_proxy_cache: Arc<crate::current::apiservice_proxy::ApiServiceProxyCache>,
}

impl ApiDiscoveryAggregationServices {
    pub(crate) fn new(
        crd_registry: CrdRegistry,
        apiservice_proxy_identity_cache: Arc<tokio::sync::OnceCell<reqwest::Identity>>,
        apiservice_proxy_cache: Arc<crate::current::apiservice_proxy::ApiServiceProxyCache>,
    ) -> Self {
        Self {
            crd_registry,
            apiservice_proxy_identity_cache,
            apiservice_proxy_cache,
        }
    }
}

impl crate::discovery::DiscoveryAggregation for ApiDiscoveryAggregationServices {
    fn crd_registry(&self) -> &CrdRegistry {
        &self.crd_registry
    }

    fn apiservice_proxy_identity_cache(&self) -> &tokio::sync::OnceCell<reqwest::Identity> {
        self.apiservice_proxy_identity_cache.as_ref()
    }

    fn apiservice_proxy_cache(&self) -> &crate::discovery::ApiServiceProxyCache {
        self.apiservice_proxy_cache.as_ref()
    }
}

#[derive(Clone)]
pub(crate) struct ApiControllerReconcileServices {
    pub(crate) service_allocations: Arc<dyn klights_reconcile_api::ServiceWriteAllocator>,
    pub(crate) controller_dispatcher: Arc<dyn klights_reconcile_api::ControllerDispatcherPort>,
    pub(crate) metrics: Arc<dyn crate::current::state_ports::ApiFailureMetrics>,
    pub(crate) node_lease_tracker: Arc<dyn crate::current::state_ports::ApiNodeLeaseObservations>,
}

impl ApiControllerReconcileServices {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        service_allocations: Arc<dyn klights_reconcile_api::ServiceWriteAllocator>,
        controller_dispatcher: Arc<dyn klights_reconcile_api::ControllerDispatcherPort>,
        metrics: Arc<dyn crate::current::state_ports::ApiFailureMetrics>,
        node_lease_tracker: Arc<dyn crate::current::state_ports::ApiNodeLeaseObservations>,
    ) -> Self {
        Self {
            service_allocations,
            controller_dispatcher,
            metrics,
            node_lease_tracker,
        }
    }
}

impl crate::generic_read::GenericReadControllerInputs for ApiControllerReconcileServices {
    fn observed_node_renew_time(
        &self,
        node_name: &str,
    ) -> crate::generic_read::GenericReadFuture<'_, Option<String>> {
        let node_name = node_name.to_string();
        Box::pin(async move {
            Ok(self
                .node_lease_tracker
                .observed_renew_time(&node_name)
                .await)
        })
    }
}

impl crate::generic_command::GenericCommandReconcile for ApiControllerReconcileServices {
    fn service_allocations(&self) -> &dyn klights_reconcile_api::ServiceWriteAllocator {
        self.service_allocations.as_ref()
    }

    fn controller_dispatcher(&self) -> &dyn klights_reconcile_api::ControllerDispatcherPort {
        self.controller_dispatcher.as_ref()
    }

    fn failure_metrics(&self) -> &dyn klights_reconcile_api::ReconcileFailureMetrics {
        self.metrics.as_ref()
    }

    fn failure_metrics_owned(&self) -> Arc<dyn klights_reconcile_api::ReconcileFailureMetrics> {
        self.metrics.clone()
    }
}

#[derive(Clone)]
pub(crate) struct ApiPodNodeSubresourceServices {
    pub(crate) services: Arc<dyn klights_reconcile_api::ServiceRoutingSync>,
    pub(crate) pod_logs: Arc<crate::subresources::pod::logs::PodLogCapabilities>,
    pub(crate) node_metrics: Arc<dyn klights_node_api::NodeMetrics>,
    pub(crate) pod_lifecycle_diagnostics:
        Option<Arc<dyn klights_pod_api::PodLifecycleDiagnosticsQuery>>,
    pub(crate) pod_start_retry_state: Option<Arc<dyn klights_pod_api::PodStartRetryDiagnostics>>,
}

impl ApiPodNodeSubresourceServices {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        services: Arc<dyn klights_reconcile_api::ServiceRoutingSync>,
        pod_logs: Arc<crate::subresources::pod::logs::PodLogCapabilities>,
        node_metrics: Arc<dyn klights_node_api::NodeMetrics>,
        pod_lifecycle_diagnostics: Option<Arc<dyn klights_pod_api::PodLifecycleDiagnosticsQuery>>,
        pod_start_retry_state: Option<Arc<dyn klights_pod_api::PodStartRetryDiagnostics>>,
    ) -> Self {
        Self {
            services,
            pod_logs,
            node_metrics,
            pod_lifecycle_diagnostics,
            pod_start_retry_state,
        }
    }
}

#[derive(Clone)]
pub(crate) struct ApiOperationalConfig {
    pub(crate) anonymous_auth: bool,
    pub(crate) runtime: ApiRuntimeInputs,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApiRuntimePaths {
    pub(crate) ca_cert: std::path::PathBuf,
    pub(crate) api_proxy_cert: std::path::PathBuf,
    pub(crate) api_proxy_key: std::path::PathBuf,
    pub(crate) apiservice_proxy_cert: std::path::PathBuf,
    pub(crate) apiservice_proxy_key: std::path::PathBuf,
    pub(crate) pod_logs_root: std::path::PathBuf,
}

impl ApiRuntimePaths {
    pub fn from_data_root(data_root: std::path::PathBuf) -> anyhow::Result<Self> {
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

    pub fn ca_cert(&self) -> &std::path::Path {
        &self.ca_cert
    }

    pub fn api_proxy_cert(&self) -> &std::path::Path {
        &self.api_proxy_cert
    }

    pub fn api_proxy_key(&self) -> &std::path::Path {
        &self.api_proxy_key
    }

    #[cfg(test)]
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

#[derive(Clone)]
pub struct ApiRuntimeInputs {
    pub(crate) paths: ApiRuntimePaths,
    pub(crate) slow_log_threshold: std::time::Duration,
    #[cfg(feature = "test-support")]
    pub(crate) audit_sink: Option<Arc<dyn crate::audit::AuditSink>>,
    #[cfg(feature = "test-support")]
    pub(crate) priority_fairness: Option<Arc<crate::priority_fairness::ApiPriorityFairness>>,
}

impl ApiRuntimeInputs {
    pub fn new(
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
            #[cfg(feature = "test-support")]
            audit_sink: None,
            #[cfg(feature = "test-support")]
            priority_fairness: None,
        })
    }

    #[cfg(feature = "test-support")]
    pub fn with_audit_sink(mut self, audit_sink: Arc<dyn crate::audit::AuditSink>) -> Self {
        self.audit_sink = Some(audit_sink);
        self
    }

    #[cfg(feature = "test-support")]
    pub fn with_priority_fairness(
        mut self,
        priority_fairness: Arc<crate::priority_fairness::ApiPriorityFairness>,
    ) -> Self {
        self.priority_fairness = Some(priority_fairness);
        self
    }
}

pub type NativeApiRemoteNodeServices = (
    Arc<dyn klights_node_api::NodeExec>,
    Arc<dyn klights_node_api::NodeLog>,
);

/// Seal root-selected focused capabilities inside the disposable native service.
#[allow(clippy::too_many_arguments)]
pub fn build_current_router(
    identity: Arc<dyn crate::ApiIdentityGenerator>,
    authorizer: Arc<dyn klights_auth::authorizer::Authorizer>,
    rbac_policy_store: Arc<dyn klights_auth::rbac_policy_store::RbacPolicyStore>,
    bootstrap_token: Arc<dyn klights_leader_api::LeaderBootstrapTokenAuthentication>,
    oidc: Option<Arc<dyn klights_auth::oidc::OidcValidator>>,
    webhook: Option<Arc<dyn klights_auth::webhook_auth::WebhookAuthenticator>>,
    cluster_ca_pem: Option<Arc<String>>,
    watch_stream: Arc<dyn crate::watch::WatchStreamSource>,
    namespace_termination: Arc<dyn klights_reconcile_api::NamespaceLifecycleStore>,
    resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
    resource_command: Arc<dyn klights_leader_api::LeaderResourceCommand>,
    finalizer_lifecycle: Arc<dyn klights_reconcile_api::FinalizerLifecyclePort>,
    mutation_effects: Arc<dyn klights_reconcile_api::ResourceMutationEffectsPort>,
    list_resource_versions: Arc<dyn crate::generic_read::ListResourceVersionPort>,
    namespace_lists: Arc<dyn crate::generic_read::NamespaceListPort>,
    quota_runtime: Arc<dyn klights_reconcile_api::ResourceQuotaAdmissionRuntime>,
    admission: Arc<dyn crate::generic_command::ResourceAdmissionPort>,
    custom_resource_reads: Arc<dyn super::custom_resource_ports::CustomResourceReadPort>,
    builtin_admission_defaults: Arc<dyn crate::generic_command::BuiltinAdmissionDefaultsPort>,
    generated_lifecycle: Arc<dyn crate::generic_command::GeneratedLifecyclePort>,
    generated_mutations: Arc<dyn crate::generic_command::GeneratedResourceMutationPort>,
    generated_watch: Arc<dyn super::generated_handler_ports::GeneratedWatchPort>,
    gc_owner_lifecycle: Arc<dyn klights_reconcile_api::GcOwnerLifecyclePort>,
    pod_repository: Arc<dyn super::state_ports::ApiPodRepository>,
    crd_registry: CrdRegistry,
    service_allocations: Arc<dyn klights_reconcile_api::ServiceWriteAllocator>,
    controller_dispatcher: Arc<dyn klights_reconcile_api::ControllerDispatcherPort>,
    metrics: Arc<dyn super::state_ports::ApiFailureMetrics>,
    node_lease_tracker: Arc<dyn super::state_ports::ApiNodeLeaseObservations>,
    services: Arc<dyn klights_reconcile_api::ServiceRoutingSync>,
    pod_logs: Arc<crate::subresources::pod::logs::PodLogCapabilities>,
    local_node_exec: Option<Arc<dyn klights_node_api::NodeExec>>,
    node_metrics: Arc<dyn klights_node_api::NodeMetrics>,
    node_port_forward: Arc<dyn klights_node_api::NodePortForward>,
    pod_lifecycle_diagnostics: Option<Arc<dyn klights_pod_api::PodLifecycleDiagnosticsQuery>>,
    pod_start_retry_state: Option<Arc<dyn klights_pod_api::PodStartRetryDiagnostics>>,
    replication: Option<NativeApiRemoteNodeServices>,
    node_name: String,
    anonymous_auth: bool,
    runtime_inputs: ApiRuntimeInputs,
    clock: Arc<dyn klights_auth::clock::Clock>,
    task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
    signing_keys: Arc<dyn klights_leader_api::LeaderServiceAccountSigningKeyState>,
    authority: Option<Arc<dyn klights_leader_api::LeaderAuthority>>,
) -> (crate::CurrentRouter, super::routes::NativeApiOuterLayers) {
    #[cfg(feature = "test-support")]
    let audit_sink = runtime_inputs
        .audit_sink
        .clone()
        .unwrap_or_else(crate::audit::default_audit_sink);
    #[cfg(not(feature = "test-support"))]
    let audit_sink = crate::audit::default_audit_sink();
    #[cfg(feature = "test-support")]
    let priority_fairness = runtime_inputs
        .priority_fairness
        .clone()
        .unwrap_or_else(|| Arc::new(crate::priority_fairness::ApiPriorityFairness::new()));
    #[cfg(not(feature = "test-support"))]
    let priority_fairness = Arc::new(crate::priority_fairness::ApiPriorityFairness::new());
    let replication = replication.map(|(exec, logs)| ApiRemoteNodeServices::new(exec, logs));
    let streaming = crate::StreamingDependencies::new(
        pod_repository.clone(),
        local_node_exec,
        replication.as_ref().map(|services| services.exec.clone()),
        node_port_forward,
        Arc::<str>::from(node_name),
        task_supervisor.clone(),
    );
    let state = ApiState::new(
        ApiAuthPolicy::new(
            authorizer,
            audit_sink,
            priority_fairness,
            rbac_policy_store,
            ApiAuthenticators::new(bootstrap_token, oidc, webhook),
            cluster_ca_pem,
        ),
        ApiResourceMutationServices {
            identity,
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
            Arc::new(crate::discovery::ApiServiceProxyCache::default()),
        ),
        ApiControllerReconcileServices::new(
            service_allocations,
            controller_dispatcher,
            metrics,
            node_lease_tracker,
        ),
        ApiPodNodeSubresourceServices::new(
            services,
            pod_logs,
            node_metrics,
            pod_lifecycle_diagnostics,
            pod_start_retry_state,
        ),
        ApiOperationalServices::new(
            replication,
            Arc::new(ApiOperationalConfig::new(anonymous_auth, runtime_inputs)),
            clock,
            task_supervisor.clone(),
            klights_supervisor::FileProcessExecutor::new(task_supervisor),
            signing_keys,
            authority,
        ),
        streaming,
    );
    super::routes::build_router_parts(state)
}

impl ApiOperationalConfig {
    pub(crate) fn new(anonymous_auth: bool, runtime: ApiRuntimeInputs) -> Self {
        Self {
            anonymous_auth,
            runtime,
        }
    }
}

#[derive(Clone)]
pub(crate) struct ApiRemoteNodeServices {
    pub(crate) exec: Arc<dyn klights_node_api::NodeExec>,
    pub(crate) logs: Arc<dyn klights_node_api::NodeLog>,
}

impl ApiRemoteNodeServices {
    pub(crate) fn new(
        exec: Arc<dyn klights_node_api::NodeExec>,
        logs: Arc<dyn klights_node_api::NodeLog>,
    ) -> Self {
        Self { exec, logs }
    }
}

#[derive(Clone)]
pub(crate) struct ApiOperationalServices {
    pub(crate) replication: Option<ApiRemoteNodeServices>,
    pub(crate) config: Arc<ApiOperationalConfig>,
    pub(crate) clock: Arc<dyn klights_auth::clock::Clock>,
    pub(crate) task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
    pub(crate) file_process: klights_supervisor::FileProcessExecutor,
    pub(crate) signing_keys: Arc<dyn klights_leader_api::LeaderServiceAccountSigningKeyState>,
    pub(crate) authority: Option<Arc<dyn klights_leader_api::LeaderAuthority>>,
}

impl ApiOperationalServices {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        replication: Option<ApiRemoteNodeServices>,
        config: Arc<ApiOperationalConfig>,
        clock: Arc<dyn klights_auth::clock::Clock>,
        task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
        file_process: klights_supervisor::FileProcessExecutor,
        signing_keys: Arc<dyn klights_leader_api::LeaderServiceAccountSigningKeyState>,
        authority: Option<Arc<dyn klights_leader_api::LeaderAuthority>>,
    ) -> Self {
        Self {
            replication,
            config,
            clock,
            task_supervisor,
            file_process,
            signing_keys,
            authority,
        }
    }
}

impl crate::discovery::DiscoveryOperationalInputs for ApiOperationalServices {
    fn apiservice_proxy_cert(&self) -> &std::path::Path {
        &self.config.runtime.paths.apiservice_proxy_cert
    }

    fn apiservice_proxy_key(&self) -> &std::path::Path {
        &self.config.runtime.paths.apiservice_proxy_key
    }

    fn task_supervisor(&self) -> &klights_supervisor::TaskSupervisor {
        self.task_supervisor.as_ref()
    }
}

impl crate::generic_read::GenericReadOperationalInputs for ApiOperationalServices {
    fn operation_unix_timestamp_nanos(&self) -> i128 {
        self.clock.now().unix_timestamp_nanos()
    }

    fn wall_clock(&self) -> Arc<dyn klights_auth::clock::Clock> {
        self.clock.clone()
    }

    fn has_local_authority(&self) -> bool {
        self.authority.as_ref().is_some_and(|authority| {
            let klights_leader_api::AuthorityRoute::Local(permit) = authority.route() else {
                return false;
            };
            authority.validate(&permit).is_ok()
        })
    }
}

impl crate::generic_command::GenericCommandRuntime for ApiOperationalServices {
    fn clock(&self) -> &dyn klights_auth::clock::Clock {
        self.clock.as_ref()
    }

    fn task_supervisor(&self) -> &klights_supervisor::TaskSupervisor {
        self.task_supervisor.as_ref()
    }

    fn task_supervisor_owned(&self) -> Arc<klights_supervisor::TaskSupervisor> {
        self.task_supervisor.clone()
    }
}

pub type ApiState = crate::ApiState<
    ApiAuthPolicy,
    ApiResourceMutationServices,
    ApiDiscoveryAggregationServices,
    ApiControllerReconcileServices,
    ApiPodNodeSubresourceServices,
    ApiOperationalServices,
>;
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
