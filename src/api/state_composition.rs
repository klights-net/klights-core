use klights_leader_api::CrdRegistry;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct ApiResourceMutationServices {
    pub(crate) identity: Arc<dyn k8s_native_service::ApiIdentityGenerator>,
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
    pub(crate) admission: Arc<dyn k8s_native_service::generic_command::ResourceAdmissionPort>,
    pub(crate) custom_resource_reads:
        Arc<dyn crate::api::custom_resource_ports::CustomResourceReadPort>,
    pub(crate) builtin_admission_defaults:
        Arc<dyn k8s_native_service::generic_command::BuiltinAdmissionDefaultsPort>,
    pub(crate) generated_lifecycle:
        Arc<dyn k8s_native_service::generic_command::GeneratedLifecyclePort>,
    pub(crate) generated_mutations:
        Arc<dyn k8s_native_service::generic_command::GeneratedResourceMutationPort>,
    pub(crate) generated_watch: Arc<dyn crate::api::generated_handler_ports::GeneratedWatchPort>,
    pub(crate) gc_owner_lifecycle: Arc<dyn klights_reconcile_api::GcOwnerLifecyclePort>,
    #[cfg(not(test))]
    pub(crate) pod_repository: Arc<dyn crate::api::state_ports::ApiPodRepository>,
    #[cfg(test)]
    pub(crate) pod_repository: Arc<crate::kubelet::pod_repository::PodRepository>,
}

impl k8s_native_service::generic_command::GenericCommandStore for ApiResourceMutationServices {
    fn identity(&self) -> &dyn k8s_native_service::ApiIdentityGenerator {
        self.identity.as_ref()
    }

    fn identity_owned(&self) -> Arc<dyn k8s_native_service::ApiIdentityGenerator> {
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

    fn generated_mutations(
        &self,
    ) -> &dyn k8s_native_service::generic_command::GeneratedResourceMutationPort {
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

impl k8s_native_service::generic_command::GenericCommandAdmission for ApiResourceMutationServices {
    fn admission(&self) -> &dyn k8s_native_service::generic_command::ResourceAdmissionPort {
        self.admission.as_ref()
    }

    fn quota_runtime(&self) -> &dyn klights_reconcile_api::ResourceQuotaAdmissionRuntime {
        self.quota_runtime.as_ref()
    }

    fn builtin_admission_defaults(
        &self,
    ) -> &dyn k8s_native_service::generic_command::BuiltinAdmissionDefaultsPort {
        self.builtin_admission_defaults.as_ref()
    }
}

impl k8s_native_service::generic_command::GenericCommandLifecycle for ApiResourceMutationServices {
    fn mutation_effects(&self) -> &dyn klights_reconcile_api::ResourceMutationEffectsPort {
        self.mutation_effects.as_ref()
    }

    fn generated_lifecycle(
        &self,
    ) -> &dyn k8s_native_service::generic_command::GeneratedLifecyclePort {
        self.generated_lifecycle.as_ref()
    }

    fn gc_owner_lifecycle(&self) -> &dyn klights_reconcile_api::GcOwnerLifecyclePort {
        self.gc_owner_lifecycle.as_ref()
    }

    fn gc_owner_lifecycle_owned(&self) -> Arc<dyn klights_reconcile_api::GcOwnerLifecyclePort> {
        self.gc_owner_lifecycle.clone()
    }
}

impl k8s_native_service::discovery::DiscoveryResourceQuery for ApiResourceMutationServices {
    fn resource_query(&self) -> &dyn klights_leader_api::LeaderResourceQuery {
        self.resource_query.as_ref()
    }
}

impl k8s_native_service::generic_read::GenericReadSnapshotPort for ApiResourceMutationServices {
    fn snapshot_resources_at_rv(
        &self,
        request: k8s_native_service::generic_read::GenericReadSnapshotRequest,
    ) -> k8s_native_service::generic_read::GenericReadFuture<
        '_,
        k8s_native_service::generic_read::GenericReadSnapshot,
    > {
        Box::pin(async move {
            let snapshot = self
                .custom_resource_reads
                .snapshot_resources_at_rv(
                    crate::api::custom_resource_ports::CustomResourceSnapshotRequest {
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
                crate::api::custom_resource_ports::CustomResourceListSnapshot::Current => {
                    k8s_native_service::generic_read::GenericReadSnapshot::Current
                }
                crate::api::custom_resource_ports::CustomResourceListSnapshot::Expired => {
                    k8s_native_service::generic_read::GenericReadSnapshot::Expired
                }
                crate::api::custom_resource_ports::CustomResourceListSnapshot::List(list) => {
                    k8s_native_service::generic_read::GenericReadSnapshot::List(list)
                }
            })
        })
    }
}

impl k8s_native_service::generic_read::GenericReadResourceInputs for ApiResourceMutationServices {
    fn resource_query(&self) -> &dyn klights_leader_api::LeaderResourceQuery {
        self.resource_query.as_ref()
    }

    fn snapshot_port(&self) -> &dyn k8s_native_service::generic_read::GenericReadSnapshotPort {
        self
    }

    fn resource_versions(&self) -> &dyn k8s_native_service::generic_read::ListResourceVersionPort {
        self.list_resource_versions.as_ref()
    }

    fn prepare_resource_for_read(
        &self,
        api_version: &'static str,
        kind: &'static str,
        resource: klights_cluster_core::Resource,
        is_get: bool,
    ) -> k8s_native_service::generic_read::GenericReadFuture<'_, serde_json::Value> {
        Box::pin(async move {
            let resource = if is_get && api_version == "v1" && kind == "Secret" {
                self.generated_lifecycle
                    .rotate_bootstrap_token_secret(resource)
                    .await?
            } else {
                resource
            };
            let mut value = crate::api::inject_resource_version_with_identity(
                self.identity.as_ref(),
                resource.data,
                resource.resource_version,
            );
            crate::api::normalize_resource_for_read(api_version, kind, &mut value);
            Ok(value)
        })
    }

    fn build_watch(
        &self,
        request: k8s_native_service::generic_read::GenericReadWatchRequest,
    ) -> k8s_native_service::generic_read::GenericReadFuture<'_, axum::response::Response> {
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
            let table_format = k8s_native_service::response::wants_table_format(&request.headers)?;
            let protobuf_supported = crate::api::watch_stream::protobuf_watch_supported_for_request(
                request.api_version,
                request.kind,
                table_format,
                query.label_selector.as_deref(),
                query.field_selector.as_deref(),
            );
            let stream_format = crate::api::watch_stream::negotiate_watch_stream_format(
                &request.headers,
                protobuf_supported,
            )?;
            let body = self
                .generated_watch
                .build_watch_stream(crate::api::generated_handler_ports::GeneratedWatchRequest {
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
                    emit_initial_state_for_resource_version_zero: explicit_resource_version_zero,
                    wall_clock: request.wall_clock,
                })
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
        response: k8s_native_service::generic_read::GenericListResponse,
    ) -> Result<axum::response::Response, k8s_native_service::AppError> {
        use axum::response::IntoResponse as _;

        let resource_version = response.response_rv.to_string();
        let operation_now = time::OffsetDateTime::from_unix_timestamp_nanos(
            response.operation_unix_timestamp_nanos,
        )
        .map_err(|error| {
            k8s_native_service::AppError::Internal(format!(
                "operation time is outside the supported timestamp range: {error}"
            ))
        })?;
        if k8s_native_service::response::wants_table_format(&response.headers)? {
            let table = match response.kind {
                "Pod" => k8s_native_service::response::pod_list_to_table_at(
                    response.items,
                    resource_version,
                    operation_now,
                ),
                "Node" => k8s_native_service::response::node_list_to_table_at(
                    response.items,
                    resource_version,
                    operation_now,
                ),
                "ReplicaSet" => k8s_native_service::response::replicaset_list_to_table_at(
                    response.items,
                    resource_version,
                    operation_now,
                ),
                "Deployment" => k8s_native_service::response::deployment_list_to_table_at(
                    response.items,
                    resource_version,
                    operation_now,
                ),
                "StatefulSet" => k8s_native_service::response::statefulset_list_to_table_at(
                    response.items,
                    resource_version,
                    operation_now,
                ),
                _ => k8s_native_service::response::generic_list_to_table_at(
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
        Ok(crate::api::K8sResponse::new(
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
        crate::api::K8sResponse::new(value, &headers).into_response()
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
    pub(crate) audit_sink: Arc<dyn k8s_native_service::audit::AuditSink>,
    pub(crate) api_priority_fairness:
        Arc<k8s_native_service::priority_fairness::ApiPriorityFairness>,
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
        audit_sink: Arc<dyn k8s_native_service::audit::AuditSink>,
        api_priority_fairness: Arc<k8s_native_service::priority_fairness::ApiPriorityFairness>,
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

impl k8s_native_service::discovery::DiscoveryAggregation for ApiDiscoveryAggregationServices {
    fn crd_registry(&self) -> &CrdRegistry {
        &self.crd_registry
    }

    fn apiservice_proxy_identity_cache(&self) -> &tokio::sync::OnceCell<reqwest::Identity> {
        self.apiservice_proxy_identity_cache.as_ref()
    }

    fn apiservice_proxy_cache(&self) -> &k8s_native_service::discovery::ApiServiceProxyCache {
        self.apiservice_proxy_cache.as_ref()
    }
}

#[derive(Clone)]
pub(crate) struct ApiControllerReconcileServices {
    pub(crate) service_allocations: Arc<dyn klights_reconcile_api::ServiceWriteAllocator>,
    #[cfg(test)]
    pub(crate) service_ipam: Arc<klights_controllers::service::ServiceIpam>,
    #[cfg(test)]
    pub(crate) nodeport_alloc: Arc<klights_controllers::service::NodePortAllocator>,
    #[cfg(not(test))]
    pub(crate) controller_dispatcher: Arc<dyn klights_reconcile_api::ControllerDispatcherPort>,
    #[cfg(test)]
    pub(crate) controller_dispatcher: Arc<crate::controllers::ControllerDispatcher>,
    #[cfg(not(test))]
    pub(crate) metrics: Arc<dyn crate::api::state_ports::ApiFailureMetrics>,
    #[cfg(test)]
    pub(crate) metrics: Arc<klights_controllers::side_effects::SideEffectMetrics>,
    #[cfg(not(test))]
    pub(crate) node_lease_tracker: Arc<dyn crate::api::state_ports::ApiNodeLeaseObservations>,
    #[cfg(test)]
    pub(crate) node_lease_tracker: Arc<klights_controllers::node_lease::NodeLeaseTracker>,
}

impl ApiControllerReconcileServices {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        service_allocations: Arc<dyn klights_reconcile_api::ServiceWriteAllocator>,
        #[cfg(test)] service_ipam: Arc<klights_controllers::service::ServiceIpam>,
        #[cfg(test)] nodeport_alloc: Arc<klights_controllers::service::NodePortAllocator>,
        #[cfg(not(test))] controller_dispatcher: Arc<
            dyn klights_reconcile_api::ControllerDispatcherPort,
        >,
        #[cfg(test)] controller_dispatcher: Arc<crate::controllers::ControllerDispatcher>,
        #[cfg(not(test))] metrics: Arc<dyn crate::api::state_ports::ApiFailureMetrics>,
        #[cfg(test)] metrics: Arc<klights_controllers::side_effects::SideEffectMetrics>,
        #[cfg(not(test))] node_lease_tracker: Arc<
            dyn crate::api::state_ports::ApiNodeLeaseObservations,
        >,
        #[cfg(test)] node_lease_tracker: Arc<klights_controllers::node_lease::NodeLeaseTracker>,
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

impl k8s_native_service::generic_read::GenericReadControllerInputs
    for ApiControllerReconcileServices
{
    fn observed_node_renew_time(
        &self,
        node_name: &str,
    ) -> k8s_native_service::generic_read::GenericReadFuture<'_, Option<String>> {
        let node_name = node_name.to_string();
        Box::pin(async move {
            Ok(self
                .node_lease_tracker
                .observed_renew_time(&node_name)
                .await)
        })
    }
}

impl k8s_native_service::generic_command::GenericCommandReconcile
    for ApiControllerReconcileServices
{
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
    pub(crate) pod_logs: Arc<k8s_native_service::subresources::pod::logs::PodLogCapabilities>,
    #[cfg(test)]
    pub(crate) local_node_exec: Option<Arc<dyn klights_node_api::NodeExec>>,
    pub(crate) node_metrics: Arc<dyn klights_node_api::NodeMetrics>,
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
        pod_logs: Arc<k8s_native_service::subresources::pod::logs::PodLogCapabilities>,
        #[cfg(test)] local_node_exec: Option<Arc<dyn klights_node_api::NodeExec>>,
        node_metrics: Arc<dyn klights_node_api::NodeMetrics>,
        #[cfg(test)] pod_lifecycle_router: Option<
            Arc<crate::kubelet::pod_lifecycle_router::PodLifecycleRouter>,
        >,
        pod_lifecycle_diagnostics: Option<Arc<dyn klights_pod_api::PodLifecycleDiagnosticsQuery>>,
        pod_start_retry_state: Option<Arc<dyn klights_pod_api::PodStartRetryDiagnostics>>,
    ) -> Self {
        Self {
            services,
            pod_logs,
            #[cfg(test)]
            local_node_exec,
            node_metrics,
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
    #[cfg(test)]
    pub(crate) node_name: String,
    pub(crate) anonymous_auth: bool,
    pub(crate) runtime: ApiRuntimeInputs,
    pub(crate) version_info: klights_apiserver::VersionInfo,
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
        #[cfg(test)] node_name: String,
        anonymous_auth: bool,
        runtime: ApiRuntimeInputs,
        version_info: klights_apiserver::VersionInfo,
    ) -> Self {
        Self {
            #[cfg(test)]
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
            klights_apiserver::VersionInfo::new(
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
    pub(crate) signing_keys: Arc<dyn klights_leader_api::LeaderServiceAccountSigningKeyState>,
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
        signing_keys: Arc<dyn klights_leader_api::LeaderServiceAccountSigningKeyState>,
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

impl k8s_native_service::discovery::DiscoveryOperationalInputs for ApiOperationalServices {
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

impl k8s_native_service::generic_read::GenericReadOperationalInputs for ApiOperationalServices {
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

impl k8s_native_service::generic_command::GenericCommandRuntime for ApiOperationalServices {
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
    identity: Arc<dyn k8s_native_service::ApiIdentityGenerator>,
    authorizer: Arc<dyn klights_auth::authorizer::Authorizer>,
    rbac_policy_store: Arc<dyn klights_auth::rbac_policy_store::RbacPolicyStore>,
    bootstrap_token: Arc<dyn klights_leader_api::LeaderBootstrapTokenAuthentication>,
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
    admission: Arc<dyn k8s_native_service::generic_command::ResourceAdmissionPort>,
    custom_resource_reads: Arc<dyn crate::api::custom_resource_ports::CustomResourceReadPort>,
    builtin_admission_defaults: Arc<
        dyn k8s_native_service::generic_command::BuiltinAdmissionDefaultsPort,
    >,
    generated_lifecycle: Arc<dyn k8s_native_service::generic_command::GeneratedLifecyclePort>,
    generated_mutations: Arc<
        dyn k8s_native_service::generic_command::GeneratedResourceMutationPort,
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
    pod_logs: Arc<k8s_native_service::subresources::pod::logs::PodLogCapabilities>,
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
    version_info: klights_apiserver::VersionInfo,
    clock: Arc<dyn klights_auth::clock::Clock>,
    cluster_status: Arc<dyn klights_leader_api::LeaderClusterStatusMetadata>,
    task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
    signing_keys: Arc<dyn klights_leader_api::LeaderServiceAccountSigningKeyState>,
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
    let streaming = k8s_native_service::StreamingDependencies::new(
        pod_repository.clone(),
        local_node_exec.clone(),
        replication.as_ref().map(|services| services.exec.clone()),
        node_port_forward.clone(),
        Arc::<str>::from(node_name.as_str()),
        task_supervisor.clone(),
    );
    let state = ApiState::new(
        ApiAuthPolicy::new(
            authorizer,
            k8s_native_service::audit::default_audit_sink(),
            Arc::new(k8s_native_service::priority_fairness::ApiPriorityFairness::new()),
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
            pod_logs,
            node_metrics,
            pod_lifecycle_diagnostics,
            pod_start_retry_state,
        ),
        ApiOperationalServices::new(
            role,
            replication,
            Arc::new(ApiOperationalConfig::new(
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
        streaming,
    );
    crate::api::routes::build_router_parts(state)
}

#[cfg(not(test))]
pub(super) type ApiState = k8s_native_service::ApiState<
    ApiAuthPolicy,
    ApiResourceMutationServices,
    ApiDiscoveryAggregationServices,
    ApiControllerReconcileServices,
    ApiPodNodeSubresourceServices,
    ApiOperationalServices,
>;

#[cfg(test)]
pub(crate) type ApiState = k8s_native_service::ApiState<
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
