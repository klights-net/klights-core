#[cfg(test)]
pub async fn build_test_app_state() -> crate::api::AppState {
    use std::sync::Arc;

    // `test_support::in_memory()` already seeds the standard system namespaces.
    let db = crate::datastore::test_support::in_memory().await;
    let crd_registry = crate::controllers::crd::CrdRegistry::new();
    let config = Arc::new(crate::KlightsConfig::test_default());
    let service_ipam = Arc::new(crate::controllers::service::ServiceIpam::new(
        &config.service_cidr,
    ));
    // F6-02: Create NodePortAllocator and mark as ready for tests
    let nodeport_alloc = Arc::new(crate::controllers::service::NodePortAllocator::new());
    nodeport_alloc.set_ready();
    let controller_dispatcher = Arc::new(crate::controller_dispatcher::ControllerDispatcher::new(
        service_ipam.clone(),
    ));
    let task_supervisor = Arc::new(crate::task_supervisor::TaskSupervisor::new(
        crate::task_supervisor::TaskCategoryConfig::default(),
    ));

    // Unit tests do not run the async workqueue worker; wire sync fallback
    // so enqueue() still drives side-effect assertions in handler tests.
    controller_dispatcher
        .set_sync_context(
            std::sync::Arc::new(db.clone()) as crate::datastore::DatastoreHandle,
            config.node_name.clone(),
        )
        .await;
    let metrics = crate::side_effects::SideEffectMetrics::new();
    let db_handle: crate::datastore::DatastoreHandle = std::sync::Arc::new(db.clone());
    let cluster_api: Arc<dyn crate::control_plane::client::LeaderApiClient> =
        Arc::new(crate::control_plane::client::local::LocalApiClient::new(
            db_handle.clone(),
            config.node_name.clone(),
            crate::control_plane::client::local::always_leader_watch(),
        ));
    let side_effects = std::sync::Arc::new(crate::side_effects::default_registry(
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
    side_effects.set_pod_repository(pod_repository.clone());
    // Wire pod_repository into dispatcher so the synchronous-fallback path
    // can drive Deployment/ReplicaSet reconciliation in handler tests.
    controller_dispatcher
        .set_pod_repository(pod_repository.clone())
        .await;
    crate::api::AppState {
        db: db_handle.clone(),
        cluster_api,
        crd_registry,
        mode: crate::bootstrap::NodeMode::Root,
        role: crate::bootstrap::NodeRole::Leader {
            bootstrap: crate::bootstrap::node_role::LeaderBootstrap::Seed,
        },
        replication: None,
        network: crate::networking::test_support::mock_network(db_handle.clone()),
        config,
        service_ipam,
        nodeport_alloc,
        cri: None,
        controller_dispatcher,
        side_effects,
        metrics,
        apiservice_proxy_identity_cache: Arc::new(tokio::sync::OnceCell::new()),
        apiservice_proxy_cache: Arc::new(
            crate::api::apiservice_proxy::ApiServiceProxyCache::default(),
        ),
        task_supervisor,
        pod_repository,
        outbox: std::sync::Arc::new(crate::kubelet::outbox::Outbox::test_outbox().await),
        node_lease_tracker: Arc::new(crate::node_lease_tracker::NodeLeaseTracker::new()),
        pod_lifecycle_router: None,
        pod_probe_manager: None,
        pod_lifecycle_rx: None,
        pod_start_retry_state: None,
        is_raft_leader_rx: None,
        authorizer: std::sync::Arc::new(crate::auth::authorizer::AuthorizerChain::test_allow_all()),
        rbac_policy_store: std::sync::Arc::new(
            crate::auth::rbac_policy_store::DatastoreRbacPolicyStore::new(db_handle.clone()),
        ),
        oidc_authenticator: None,
        webhook_authenticator: None,
        cluster_ca_pem: None,
    }
}

#[cfg(test)]
pub async fn build_test_router() -> axum::Router {
    crate::api::build_router(build_test_app_state().await)
}

#[cfg(test)]
pub async fn build_test_router_with_db() -> (axum::Router, crate::datastore::DatastoreHandle) {
    let state = build_test_app_state().await;
    let db = state.db.clone();
    (crate::api::build_router(state), db)
}

#[cfg(test)]
pub async fn build_test_app_state_with_authorizer(
    authorizer: std::sync::Arc<dyn crate::auth::authorizer::Authorizer>,
) -> crate::api::AppState {
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
