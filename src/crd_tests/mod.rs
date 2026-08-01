use crate::datastore::sqlite::Datastore;
use crate::watch::{EventType, WatchEvent};
use klights_controllers::crd::{CrdRegistry, register_crd_from_value};
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

/// Helper: build a minimal ApiState for HTTP-level tests.
pub async fn build_test_app_state(db: Datastore, registry: CrdRegistry) -> crate::api::ApiState {
    let service_ipam = std::sync::Arc::new(klights_controllers::service::ServiceIpam::new(
        "10.43.128.0/17",
    ));
    let nodeport_alloc =
        std::sync::Arc::new(klights_controllers::service::NodePortAllocator::new());
    let controller_identity = crate::controllers::test_utils::deterministic_controller_identity();
    let controller_dispatcher =
        std::sync::Arc::new(crate::controllers::ControllerDispatcher::new_with_identity(
            service_ipam.clone(),
            controller_identity.clone(),
        ));
    let task_supervisor = std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let metrics = klights_controllers::side_effects::SideEffectMetrics::new();
    let passive_reads = crate::datastore::test_support::sqlite_passive_read_ports(&db);
    let db_handle: crate::datastore::DatastoreHandle = std::sync::Arc::new(db.clone());
    let local_api = std::sync::Arc::new(crate::control_plane::client::local::LocalApiClient::new(
        db_handle.clone(),
        "test-node".to_string(),
        crate::control_plane::client::local::always_leader_watch(),
    ));
    let resource_query: std::sync::Arc<dyn klights_leader_api::LeaderResourceQuery> =
        local_api.clone();
    let resource_command: std::sync::Arc<dyn klights_leader_api::LeaderResourceCommand> = local_api;
    let side_effects =
        std::sync::Arc::new(crate::side_effect_registry_composition::default_registry(
            metrics.clone(),
            None,
            None,
            Some(db_handle.clone()),
            controller_identity.clone(),
        ));
    let pod_repository = std::sync::Arc::new(crate::kubelet::pod_repository::PodRepository::new(
        db_handle.clone(),
        task_supervisor.clone(),
        side_effects.clone(),
        metrics.clone(),
    ));
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
    let positioned_watch =
        crate::positioned_watch_adapter::for_test(&passive_reads, db_handle.clone());
    let generated_handler_adapter = crate::generated_handler_adapter::GeneratedHandlerAdapter::new(
        db_handle.clone(),
        crate::watch_commit_observation_adapter::test_signal_source(&db_handle),
        positioned_watch.clone(),
        klights_supervisor::FileProcessExecutor::new(task_supervisor.clone()),
        task_supervisor.clone(),
        crate::KlightsConfig::test_default()
            .data_root
            .join("etc")
            .join("ca.crt"),
        controller_identity,
    );
    let bootstrap_token_authenticator = std::sync::Arc::new(
        crate::bootstrap::auth_adapters::DatastoreBootstrapTokenAuthenticator::new(
            db_handle.clone(),
        ),
    );
    let network = crate::networking::test_support::mock_network(db_handle.clone());
    crate::api::ApiState::new(
        crate::api::ApiAuthPolicy::new(
            std::sync::Arc::new(crate::api::test_support::AllowAllAuthorizer),
            crate::audit::default_audit_sink(),
            std::sync::Arc::new(crate::api::priority_fairness::ApiPriorityFairness::new()),
            std::sync::Arc::new(
                klights_auth::rbac_policy_store::ReaderBackedRbacPolicyStore::new(
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
            watch_stream: std::sync::Arc::new(
                crate::watch_stream_adapter::DatastoreWatchStreamAdapter::new(
                    db_handle.clone(),
                    crate::watch_commit_observation_adapter::test_signal_source(&db_handle),
                    positioned_watch.clone(),
                ),
            ),
            namespace_termination:
                crate::api_state_adapter_test_owner::RootNamespaceTerminationStore::new(
                    db_handle.clone(),
                ),
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
                    crate::watch_commit_observation_adapter::test_signal_source(&db_handle),
                    positioned_watch.clone(),
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
            registry,
            std::sync::Arc::new(tokio::sync::OnceCell::new()),
            std::sync::Arc::new(crate::api::apiservice_proxy::ApiServiceProxyCache::default()),
        ),
        crate::api::ApiControllerReconcileServices::new(
            crate::bootstrap::service_adapters::ApiServiceWriteAllocator::new(
                db_handle.clone(),
                service_ipam.clone(),
                nodeport_alloc.clone(),
                crate::controllers::test_utils::deterministic_controller_identity(),
            ),
            service_ipam,
            nodeport_alloc,
            controller_dispatcher,
            metrics,
            std::sync::Arc::new(klights_controllers::node_lease::NodeLeaseTracker::new_at(
                chrono::Utc::now(),
            )),
        ),
        crate::api::ApiPodNodeSubresourceServices::new(
            std::sync::Arc::new(
                crate::bootstrap::network_adapters::ApiServiceRoutingSyncAdapter::new(
                    network.services().clone(),
                ),
            ),
            klights_kubelet::node_api::logs::PodLogFollowWatchSource::new(std::sync::Arc::new(
                crate::bootstrap::kubelet_ports::DatastorePodWatchSource::new(std::sync::Arc::new(
                    positioned_watch,
                )),
            )),
            None,
            std::sync::Arc::new(crate::node_metrics_adapter::UnavailableNodeMetrics),
            klights_kubelet::node_api::port_forward::local_node_port_forward(
                task_supervisor.clone(),
            ),
            None,
            None,
            None,
        ),
        crate::api::ApiOperationalServices::new(
            crate::api::ApiNodeRole::Leader,
            None,
            {
                let config = crate::KlightsConfig::test_default();
                crate::api::ApiOperationalConfig::from_test(config)
            },
            std::sync::Arc::new(klights_auth::clock::SystemClock),
            crate::bootstrap::operational_adapters::ApiClusterStatusMetadata::new(
                db_handle.clone(),
            ),
            task_supervisor.clone(),
            klights_supervisor::FileProcessExecutor::new(task_supervisor),
            crate::signing_key_state_adapter::RootServiceAccountSigningKeyState::for_test(),
            None,
        ),
    )
}

pub async fn build_test_router(db: Datastore, registry: CrdRegistry) -> axum::Router {
    crate::api::build_router(build_test_app_state(db, registry).await)
}
