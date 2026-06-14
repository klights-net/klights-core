use crate::controllers::crd::{CrdRegistry, register_crd_from_value};
use crate::datastore::sqlite::Datastore;
use crate::watch::{EventType, WatchEvent};
use serde_json::json;

mod delete_cascade;
mod status;
mod subresources;
mod validation;

fn make_crd_value(group: &str, kind: &str, plural: &str, scope: &str) -> serde_json::Value {
    json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {
            "name": format!("{}.{}", plural, group)
        },
        "spec": {
            "group": group,
            "scope": scope,
            "names": {
                "kind": kind,
                "plural": plural,
                "singular": kind.to_lowercase()
            },
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {
                    "openAPIV3Schema": {
                        "type": "object",
                        "properties": {
                            "spec": {"type": "object"},
                            "status": {"type": "object"}
                        }
                    }
                }
            }]
        }
    })
}

/// Helper: build a minimal AppState for HTTP-level tests.
pub async fn build_test_app_state(db: Datastore, registry: CrdRegistry) -> crate::api::AppState {
    let service_ipam = std::sync::Arc::new(crate::controllers::service::ServiceIpam::new(
        "10.43.128.0/17",
    ));
    let controller_dispatcher = std::sync::Arc::new(
        crate::controller_dispatcher::ControllerDispatcher::new(service_ipam.clone()),
    );
    let task_supervisor = std::sync::Arc::new(crate::task_supervisor::TaskSupervisor::new(
        crate::task_supervisor::TaskCategoryConfig::default(),
    ));
    let metrics = crate::side_effects::SideEffectMetrics::new();
    let db_handle: crate::datastore::DatastoreHandle = std::sync::Arc::new(db.clone());
    let cluster_api: std::sync::Arc<dyn crate::control_plane::client::LeaderApiClient> =
        std::sync::Arc::new(crate::control_plane::client::local::LocalApiClient::new(
            db_handle.clone(),
            "test-node".to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        ));
    let side_effects = std::sync::Arc::new(crate::side_effects::default_registry(
        metrics.clone(),
        None,
        None,
        None,
    ));
    let pod_repository = std::sync::Arc::new(crate::kubelet::pod_repository::PodRepository::new(
        db_handle.clone(),
        task_supervisor.clone(),
        side_effects.clone(),
        metrics.clone(),
    ));
    crate::api::AppState {
        db: db_handle.clone(),
        cluster_api,
        crd_registry: registry,
        mode: crate::bootstrap::NodeMode::Root,
        role: crate::bootstrap::NodeRole::Leader {
            bootstrap: crate::bootstrap::node_role::LeaderBootstrap::Seed,
        },
        replication: None,
        network: crate::networking::test_support::mock_network(db_handle),
        config: std::sync::Arc::new({
            let ns = "klights-test";
            crate::KlightsConfig {
                bridge_name: ns.to_string(),
                pod_subnet: "10.43.0.0/17".to_string(),
                cluster_cidr: "10.42.0.0/16".to_string(),
                service_cidr: "10.43.128.0/17".to_string(),
                tls_port: 7443,
                api_fqdn: None,
                log_file: None,
                containerd_namespace: ns.to_string(),
                containerd_socket: None,
                node_name: "test-node".to_string(),
                node_ip: None,
                vxlan_vni: 1228,
                vxlan_port: 8472,
                vxlan_device: crate::networking::vxlan::DEFAULT_DEVICE.to_string(),
                dataplane_encryption: crate::networking::wireguard::DataplaneEncryption::Enabled,
                external_endpoint: None,
                worker_dataplane_no_ingress: false,
                wireguard_device: crate::networking::wireguard::DEFAULT_WIREGUARD_DEVICE
                    .to_string(),
                wireguard_port: crate::networking::wireguard::DEFAULT_WIREGUARD_PORT,
                cluster_db_path: crate::paths::test_data_root_path(ns)
                    .join("db")
                    .join("sqlite")
                    .join("cluster.db"),
                node_db_path: crate::paths::test_data_root_path(ns)
                    .join("db")
                    .join("sqlite")
                    .join("node.db"),
                in_memory: true,
                db_encryption: crate::DbEncryption::Disabled,
                db_key_file: None,
                datastore_backend: crate::datastore::backend_kind::BackendKind::Sqlite,
                node_local_backend: crate::datastore::backend_kind::BackendKind::Sqlite,
                oidc_issuer_url: None,
                oidc_client_id: None,
                oidc_username_claim: "sub".to_string(),
                oidc_groups_claim: "groups".to_string(),
                oidc_groups_prefix: String::new(),
                oidc_ca_bundle: None,
                webhook_auth_url: None,
                webhook_auth_client_cert: None,
                webhook_auth_client_key: None,
                webhook_auth_audiences: String::new(),
                webhook_auth_cache_authorized_ttl_secs: 300,
                webhook_auth_cache_unauthorized_ttl_secs: 30,
                webhook_auth_ca_bundle: None,
            }
        }),
        service_ipam,
        nodeport_alloc: std::sync::Arc::new(crate::controllers::service::NodePortAllocator::new()),
        cri: None,
        controller_dispatcher,
        side_effects,
        metrics,
        apiservice_proxy_identity_cache: std::sync::Arc::new(tokio::sync::OnceCell::new()),
        apiservice_proxy_cache: std::sync::Arc::new(
            crate::api::apiservice_proxy::ApiServiceProxyCache::default(),
        ),
        task_supervisor,
        pod_repository,
        outbox: std::sync::Arc::new(crate::kubelet::outbox::Outbox::test_outbox().await),
        node_lease_tracker: std::sync::Arc::new(crate::node_lease_tracker::NodeLeaseTracker::new()),
        pod_lifecycle_router: None,
        pod_probe_manager: None,
        pod_lifecycle_rx: None,
        pod_start_retry_state: None,
        is_raft_leader_rx: None,
        authorizer: std::sync::Arc::new(crate::auth::authorizer::AuthorizerChain::test_allow_all()),
        rbac_policy_store: std::sync::Arc::new(
            crate::auth::rbac_policy_store::InMemoryRbacPolicyStore::empty(),
        ),
        oidc_authenticator: None,
        webhook_authenticator: None,
        cluster_ca_pem: None,
    }
}

pub async fn build_test_router(db: Datastore, registry: CrdRegistry) -> axum::Router {
    crate::api::build_router(build_test_app_state(db, registry).await)
}
