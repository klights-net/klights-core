#[cfg(test)]
pub(crate) fn resource_query_for_test_datastore(
    db: crate::datastore::sqlite::Datastore,
) -> std::sync::Arc<dyn klights_leader_api::LeaderResourceQuery> {
    let db_handle: crate::datastore::DatastoreHandle = std::sync::Arc::new(db);
    std::sync::Arc::new(crate::control_plane::client::local::LocalApiClient::new(
        db_handle,
        "test-query-node".to_string(),
        crate::control_plane::client::local::always_leader_watch(),
    ))
}

#[cfg(test)]
pub(crate) async fn build_test_app_state() -> crate::api::ApiState {
    // `test_support::in_memory()` already seeds the standard system namespaces.
    let db = crate::datastore::test_support::in_memory().await;
    build_test_app_state_with_db(std::sync::Arc::new(db)).await
}

#[cfg(test)]
pub(crate) async fn build_test_app_state_with_db(
    db_handle: crate::datastore::DatastoreHandle,
) -> crate::api::ApiState {
    use std::sync::Arc;

    let crd_registry = crate::controllers::crd::CrdRegistry::new();
    let config = Arc::new(crate::KlightsConfig::test_default());
    let service_ipam = Arc::new(crate::controllers::service::ServiceIpam::new(
        &config.service_cidr,
    ));
    // F6-02: Create NodePortAllocator and mark as ready for tests
    let nodeport_alloc = Arc::new(crate::controllers::service::NodePortAllocator::new());
    nodeport_alloc.set_ready();
    let task_supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let controller_dispatcher = Arc::new(
        crate::controllers::ControllerDispatcher::with_task_supervisor(
            service_ipam.clone(),
            task_supervisor.clone(),
        ),
    );

    // Unit tests do not run the async workqueue worker; wire sync fallback
    // so enqueue() still drives side-effect assertions in handler tests.
    controller_dispatcher
        .set_sync_context(db_handle.clone(), config.node_name.clone())
        .await;
    let metrics = crate::side_effects::SideEffectMetrics::new();
    let local_api = Arc::new(crate::control_plane::client::local::LocalApiClient::new(
        db_handle.clone(),
        config.node_name.clone(),
        crate::control_plane::client::local::always_leader_watch(),
    ));
    let resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery> = local_api.clone();
    let resource_command: Arc<dyn klights_leader_api::LeaderResourceCommand> = local_api;
    let side_effects =
        std::sync::Arc::new(crate::side_effect_registry_composition::default_registry(
            metrics.clone(),
            None,
            Some(task_supervisor.clone()),
            Some(db_handle.clone()),
        ));
    side_effects.set_controller_dispatcher(controller_dispatcher.clone());
    let pod_repository = std::sync::Arc::new(crate::kubelet::pod_repository::PodRepository::new(
        db_handle.clone(),
        task_supervisor.clone(),
        side_effects.clone(),
        metrics.clone(),
    ));
    // Bind the late-resolved `PodRepository` slot so PDB/ResourceQuota
    // side effects route pod listings through `PodReader::list_pods`.
    side_effects.set_pod_ports(pod_repository.clone(), pod_repository.clone());
    // Wire pod_repository into dispatcher so the synchronous-fallback path
    // can drive Deployment/ReplicaSet reconciliation in handler tests.
    controller_dispatcher
        .set_pod_repository(pod_repository.clone())
        .await;
    let finalizer_lifecycle =
        crate::bootstrap::finalizer_lifecycle_adapter::DatastoreFinalizerLifecycleAdapter::new(
            db_handle.clone(),
            pod_repository.clone(),
            side_effects.clone(),
            metrics.clone(),
        );
    let mutation_effects =
        crate::resource_mutation_effects_adapter::ResourceMutationEffectsAdapter::new(
            side_effects.clone(),
            metrics.clone(),
        );
    let list_resource_versions =
        crate::list_query_adapter::DatastoreListResourceVersionPort::new(db_handle.clone());
    let gc_owner_lifecycle = crate::gc_delete_adapter::GcOwnerLifecycleAdapter::new(
        db_handle.clone(),
        pod_repository.clone(),
    );
    let generated_handler_adapter = crate::generated_handler_adapter::GeneratedHandlerAdapter::new(
        db_handle.clone(),
        klights_supervisor::FileProcessExecutor::new(task_supervisor.clone()),
        task_supervisor.clone(),
        config.data_root.join("etc").join("ca.crt"),
    );
    let network = crate::networking::test_support::mock_network(db_handle.clone());
    let bootstrap_token_authenticator = Arc::new(
        crate::bootstrap::auth_adapters::DatastoreBootstrapTokenAuthenticator::new(
            db_handle.clone(),
        ),
    );
    crate::api::ApiState::new(
        crate::api::ApiAuthPolicy::new(
            std::sync::Arc::new(crate::auth::authorizer::AuthorizerChain::test_allow_all()),
            crate::audit::default_audit_sink(),
            std::sync::Arc::new(crate::api::priority_fairness::ApiPriorityFairness::new()),
            std::sync::Arc::new(
                crate::auth::rbac_policy_store::ReaderBackedRbacPolicyStore::new(
                    std::sync::Arc::new(
                        crate::bootstrap::auth_adapters::DatastoreRbacResourceReader::new(
                            db_handle.clone(),
                        ),
                    ),
                ),
            ),
            crate::api::ApiAuthenticators::new(bootstrap_token_authenticator, None, None),
            None,
        ),
        crate::api::ApiResourceMutationServices {
            db: db_handle.clone(),
            resource_query,
            resource_command,
            finalizer_lifecycle,
            mutation_effects,
            list_resource_versions,
            namespace_lists: crate::list_query_adapter::DatastoreNamespaceListPort::new(
                db_handle.clone(),
            ),
            quota_runtime:
                crate::resource_quota_admission_adapter::ResourceQuotaAdmissionAdapter::new(
                    db_handle.clone(),
                ),
            admission: crate::resource_admission_adapter::ResourceAdmissionAdapter::new(
                db_handle.clone(),
            ),
            custom_resource_reads:
                crate::custom_resource_read_adapter::CustomResourceReadAdapter::new(
                    db_handle.clone(),
                    task_supervisor.clone(),
                ),
            builtin_admission_defaults: generated_handler_adapter.clone(),
            generated_lifecycle: generated_handler_adapter.clone(),
            generated_mutations: generated_handler_adapter.clone(),
            generated_watch: generated_handler_adapter,
            gc_owner_lifecycle: std::sync::Arc::new(gc_owner_lifecycle),
            pod_repository,
        },
        crate::api::ApiDiscoveryAggregationServices::new(
            crd_registry,
            Arc::new(tokio::sync::OnceCell::new()),
            Arc::new(crate::api::apiservice_proxy::ApiServiceProxyCache::default()),
        ),
        crate::api::ApiControllerReconcileServices::new(
            crate::bootstrap::service_adapters::ApiServiceWriteAllocator::new(
                db_handle.clone(),
                service_ipam.clone(),
                nodeport_alloc.clone(),
            ),
            service_ipam,
            nodeport_alloc,
            controller_dispatcher,
            metrics,
            Arc::new(crate::node_lease_tracker::NodeLeaseTracker::new()),
        ),
        crate::api::ApiPodNodeSubresourceServices::new(
            Arc::new(
                crate::bootstrap::network_adapters::ApiServiceRoutingSyncAdapter::new(
                    network.services().clone(),
                ),
            ),
            crate::api::pod_subresources::logs::PodLogFollowWatchSource::new(Arc::new(
                crate::bootstrap::kubelet_ports::DatastorePodWatchSource::new(Arc::new(
                    crate::datastore::DatastoreBackendWatchStore::new(db_handle.clone()),
                )),
            )),
            None,
            Arc::new(crate::metrics::FallbackOnlyMetricsProvider),
            crate::portforward::local_node_port_forward(task_supervisor.clone()),
            None,
            None,
            None,
        ),
        crate::api::ApiOperationalServices::new(
            crate::api::ApiNodeRole::Leader,
            None,
            crate::api::ApiOperationalConfig::from_test(config.as_ref().clone()),
            crate::bootstrap::operational_adapters::ApiClusterStatusMetadata::new(
                db_handle.clone(),
            ),
            task_supervisor.clone(),
            klights_supervisor::FileProcessExecutor::new(task_supervisor),
            None,
        ),
    )
}

#[cfg(test)]
pub async fn build_test_router() -> axum::Router {
    crate::api::build_router(build_test_app_state().await)
}

#[cfg(test)]
pub async fn build_test_router_with_db() -> (axum::Router, crate::datastore::DatastoreHandle) {
    let state = build_test_app_state().await;
    let db = state.resource_mutation().db.clone();
    (crate::api::build_router(state), db)
}

#[cfg(test)]
pub(crate) async fn build_test_app_state_with_authorizer(
    authorizer: std::sync::Arc<dyn crate::auth::authorizer::Authorizer>,
) -> crate::api::ApiState {
    let mut state = build_test_app_state().await;
    state.authorizer = authorizer;
    state
}

#[cfg(test)]
pub async fn build_test_router_with_authorizer(
    authorizer: std::sync::Arc<dyn crate::auth::authorizer::Authorizer>,
) -> axum::Router {
    crate::api::build_router(build_test_app_state_with_authorizer(authorizer).await)
}
