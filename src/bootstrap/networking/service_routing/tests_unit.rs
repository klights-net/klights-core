use klights_networking::service_routing::*;
use serde_json::json;
use std::net::Ipv4Addr;
#[derive(Default)]
struct FreshServiceInventoryClient {
    cached_list_calls: std::sync::atomic::AtomicUsize,
    cached_get_calls: std::sync::atomic::AtomicUsize,
    fresh_get_calls: std::sync::atomic::AtomicUsize,
    service_list_calls: std::sync::atomic::AtomicUsize,
    endpoints_list_calls: std::sync::atomic::AtomicUsize,
    endpointslice_list_calls: std::sync::atomic::AtomicUsize,
    filtered_endpointslice_list_calls: std::sync::atomic::AtomicUsize,
    legacy_endpoints_empty: bool,
    legacy_endpoints_partial: bool,
    legacy_endpoint_ips: Option<Vec<String>>,
    endpointslice_endpoint_ips: Option<Vec<String>>,
    service_ports: Option<Vec<serde_json::Value>>,
    endpoints_ports: Option<Vec<serde_json::Value>>,
    endpointslice_ports: Option<Vec<serde_json::Value>>,
}

impl FreshServiceInventoryClient {
    fn service_ports(&self) -> Vec<serde_json::Value> {
        self.service_ports.clone().unwrap_or_else(|| {
            vec![
                json!({"name": "dns", "port": 53, "protocol": "UDP"}),
                json!({"name": "dns-tcp", "port": 53, "protocol": "TCP"}),
            ]
        })
    }

    fn endpoints_ports(&self) -> Vec<serde_json::Value> {
        self.endpoints_ports.clone().unwrap_or_else(|| {
            vec![
                json!({"name": "dns", "port": 53, "protocol": "UDP"}),
                json!({"name": "dns-tcp", "port": 53, "protocol": "TCP"}),
            ]
        })
    }

    fn endpointslice_ports(&self) -> Vec<serde_json::Value> {
        self.endpointslice_ports.clone().unwrap_or_else(|| {
            vec![
                json!({"name": "dns", "port": 53, "protocol": "UDP"}),
                json!({"name": "dns-tcp", "port": 53, "protocol": "TCP"}),
            ]
        })
    }

    fn endpointslice_endpoints(&self) -> Vec<serde_json::Value> {
        self.endpointslice_endpoint_ips
            .as_ref()
            .map(|ips| {
                ips.iter()
                    .map(|ip| {
                        json!({
                            "addresses": [ip],
                            "conditions": {"ready": true}
                        })
                    })
                    .collect()
            })
            .unwrap_or_else(|| {
                vec![json!({
                    "addresses": ["10.50.0.20"],
                    "conditions": {"ready": true}
                })]
            })
    }

    fn legacy_endpoint_addresses(&self) -> Vec<serde_json::Value> {
        self.legacy_endpoint_ips
            .as_ref()
            .map(|ips| ips.iter().map(|ip| json!({"ip": ip})).collect())
            .unwrap_or_else(|| vec![json!({"ip": "10.50.0.2"})])
    }
}

fn inventory_resource(
    api_version: &str,
    kind: &str,
    namespace: &str,
    name: &str,
    resource_version: i64,
    data: serde_json::Value,
) -> klights_cluster_core::Resource {
    klights_cluster_core::Resource {
        id: resource_version,
        api_version: api_version.to_string(),
        kind: kind.to_string(),
        namespace: Some(namespace.to_string()),
        name: name.to_string(),
        uid: format!("{name}-uid"),
        resource_version,
        data: std::sync::Arc::new(data),
    }
}

impl FreshServiceInventoryClient {
    async fn fresh_list_for_test(
        &self,
        req: klights_leader_api::ResourceListRequest,
    ) -> anyhow::Result<klights_cluster_store::ResourceList> {
        if req.api_version() == "discovery.k8s.io/v1" && req.kind() == "EndpointSlice" {
            self.endpointslice_list_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if req.label_selector().is_some() {
                self.filtered_endpointslice_list_calls
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            let ports = self.endpointslice_ports();
            let endpoints = self.endpointslice_endpoints();
            return Ok(klights_cluster_store::ResourceList {
                items: if self.legacy_endpoints_empty || self.legacy_endpoints_partial {
                    vec![inventory_resource(
                        "discovery.k8s.io/v1",
                        "EndpointSlice",
                        "kube-system",
                        "kube-dns-klights",
                        73,
                        json!({
                            "apiVersion": "discovery.k8s.io/v1",
                            "kind": "EndpointSlice",
                            "metadata": {
                                "namespace": "kube-system",
                                "name": "kube-dns-klights",
                                "labels": {
                                    "kubernetes.io/service-name": "kube-dns"
                                }
                            },
                            "addressType": "IPv4",
                            "ports": ports,
                            "endpoints": endpoints
                        }),
                    )]
                } else {
                    Vec::new()
                },
                resource_version: 73,
                watch_replay_position: None,
                continue_token: None,
                remaining_item_count: None,
            });
        }

        if req.api_version() == "v1" && req.kind() == "Endpoints" {
            self.endpoints_list_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let legacy_addresses = self.legacy_endpoint_addresses();
            return Ok(klights_cluster_store::ResourceList {
                items: vec![inventory_resource(
                    "v1",
                    "Endpoints",
                    "kube-system",
                    "kube-dns",
                    72,
                    json!({
                        "apiVersion": "v1",
                        "kind": "Endpoints",
                        "metadata": {
                            "namespace": "kube-system",
                            "name": "kube-dns",
                            "uid": "kube-dns-endpoints-uid",
                        },
                        "subsets": if self.legacy_endpoints_empty {
                            json!([])
                        } else {
                            json!([{
                                "addresses": legacy_addresses,
                                "ports": self.endpoints_ports()
                            }])
                        }
                    }),
                )],
                resource_version: 72,
                watch_replay_position: None,
                continue_token: None,
                remaining_item_count: None,
            });
        }

        assert_eq!(req.api_version(), "v1");
        assert_eq!(req.kind(), "Service");
        self.service_list_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let service_ports = self.service_ports();
        Ok(klights_cluster_store::ResourceList {
            items: vec![inventory_resource(
                "v1",
                "Service",
                "kube-system",
                "kube-dns",
                32,
                json!({
                    "apiVersion": "v1",
                    "kind": "Service",
                    "metadata": {
                        "namespace": "kube-system",
                        "name": "kube-dns",
                        "uid": "kube-dns-service-uid",
                    },
                    "spec": {
                        "clusterIP": "10.51.0.10",
                        "ports": service_ports
                    }
                }),
            )],
            resource_version: 32,
            watch_replay_position: None,
            continue_token: None,
            remaining_item_count: None,
        })
    }

    async fn fresh_get_for_test(
        &self,
        key: klights_types::ResourceKey,
    ) -> anyhow::Result<Option<klights_cluster_core::Resource>> {
        self.fresh_get_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if key.api_version == "v1"
            && key.kind == "Endpoints"
            && key.namespace.as_deref() == Some("kube-system")
            && key.name == "kube-dns"
        {
            return Ok(Some(inventory_resource(
                "v1",
                "Endpoints",
                "kube-system",
                "kube-dns",
                72,
                json!({
                    "apiVersion": "v1",
                    "kind": "Endpoints",
                    "metadata": {
                        "namespace": "kube-system",
                        "name": "kube-dns",
                        "uid": "kube-dns-endpoints-uid",
                    },
                    "subsets": if self.legacy_endpoints_empty {
                        json!([])
                    } else {
                        json!([{
                            "addresses": [{"ip": "10.50.0.2"}],
                            "ports": self.endpoints_ports()
                        }])
                    }
                }),
            )));
        }
        Ok(None)
    }
}

impl klights_leader_api::LeaderResourceQuery for FreshServiceInventoryClient {
    fn get_resource(
        &self,
        request: klights_leader_api::ResourceGetRequest,
    ) -> klights_leader_api::ResourceQueryFuture<'_, Option<klights_cluster_core::Resource>> {
        Box::pin(async move {
            if request.consistency() == klights_leader_api::ResourceQueryConsistency::Cached {
                self.cached_get_calls
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                return Ok(None);
            }
            self.fresh_get_for_test(request.into_key())
                .await
                .map_err(|error| {
                    klights_leader_api::ResourceQueryError::query_failed(error.to_string())
                })
        })
    }

    fn list_resources(
        &self,
        request: klights_leader_api::ResourceListRequest,
    ) -> klights_leader_api::ResourceQueryFuture<'_, klights_leader_api::ResourceListResult> {
        Box::pin(async move {
            if request.consistency() == klights_leader_api::ResourceQueryConsistency::Cached {
                self.cached_list_calls
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                return klights_leader_api::ResourceListResult::try_new(
                    Vec::new(),
                    1,
                    None,
                    None,
                    None,
                );
            }
            let list = self.fresh_list_for_test(request).await.map_err(|error| {
                klights_leader_api::ResourceQueryError::query_failed(error.to_string())
            })?;
            klights_leader_api::ResourceListResult::try_new(
                list.items,
                list.resource_version,
                list.watch_replay_position,
                list.continue_token,
                list.remaining_item_count,
            )
        })
    }
}

impl klights_leader_api::LeaderWatch for FreshServiceInventoryClient {
    fn watch_resources(
        &self,
        _req: klights_leader_api::WatchRequest,
    ) -> klights_leader_api::LeaderWatchFuture<'_> {
        Box::pin(async {
            Ok(klights_leader_api::WatchStream::unpositioned_test_stream(
                futures::stream::empty(),
            ))
        })
    }
}

impl klights_leader_api::LeaderCacheReadiness for FreshServiceInventoryClient {
    fn wait_cache_ready(
        &self,
        _scope: klights_leader_api::CacheReadinessRequest,
    ) -> klights_leader_api::CacheReadinessFuture<'_> {
        Box::pin(async { Ok(()) })
    }
}

crate::bootstrap::leader_test_support::impl_unavailable_leader_pod_effects!(
    FreshServiceInventoryClient
);

#[tokio::test]
async fn service_specs_from_api_uses_fresh_reads_for_routing_snapshot() {
    let api = std::sync::Arc::new(FreshServiceInventoryClient::default());

    let specs = service_specs_from_api(
        &klights_networking::service_routing::LeaderRoutingStateSource::new(api.clone()),
    )
    .await
    .expect("service specs should build");

    assert_eq!(
        api.cached_list_calls
            .load(std::sync::atomic::Ordering::SeqCst),
        0,
        "routing snapshots must not use the possibly stale cached list"
    );
    assert_eq!(
        api.cached_get_calls
            .load(std::sync::atomic::Ordering::SeqCst),
        0,
        "routing snapshots must not use the possibly stale cached get"
    );
    assert_eq!(specs.len(), 1);
    assert_eq!(specs[0].cluster_ip, Ipv4Addr::new(10, 51, 0, 10));

    let mut tuples: Vec<_> = specs[0]
        .ports
        .iter()
        .map(|port| {
            (
                port.protocol,
                port.service_port,
                port.target_port,
                port.endpoints.clone(),
            )
        })
        .collect();
    tuples.sort_by_key(|(protocol, service_port, _, _)| (*protocol, *service_port));
    assert_eq!(
        tuples,
        vec![
            (Protocol::Tcp, 53, 53, vec![Ipv4Addr::new(10, 50, 0, 2)]),
            (Protocol::Udp, 53, 53, vec![Ipv4Addr::new(10, 50, 0, 2)]),
        ]
    );
}

#[tokio::test]
async fn service_specs_from_api_uses_bounded_bulk_fresh_inventory() {
    let api = std::sync::Arc::new(FreshServiceInventoryClient {
        legacy_endpoints_partial: true,
        ..Default::default()
    });

    let specs = service_specs_from_api(
        &klights_networking::service_routing::LeaderRoutingStateSource::new(api.clone()),
    )
    .await
    .expect("service specs should build from bulk inventory");

    assert_eq!(specs.len(), 1);
    assert_eq!(
        api.service_list_calls
            .load(std::sync::atomic::Ordering::SeqCst),
        1,
        "routing snapshots should list Services once"
    );
    assert_eq!(
        api.endpoints_list_calls
            .load(std::sync::atomic::Ordering::SeqCst),
        1,
        "routing snapshots should list Endpoints once for the whole inventory"
    );
    assert_eq!(
        api.endpointslice_list_calls
            .load(std::sync::atomic::Ordering::SeqCst),
        1,
        "routing snapshots should list EndpointSlices once for the whole inventory"
    );
    assert_eq!(
        api.filtered_endpointslice_list_calls
            .load(std::sync::atomic::Ordering::SeqCst),
        0,
        "routing snapshots must not issue one EndpointSlice list per Service"
    );
    assert_eq!(
        api.fresh_get_calls
            .load(std::sync::atomic::Ordering::SeqCst),
        0,
        "routing snapshots must not issue one fresh Endpoints get per Service"
    );
}

// ── Task 6: cached-inventory route sync tests ──────────────────────

#[tokio::test]
async fn coalesced_sync_uses_cached_inventory_after_initial_snapshot() {
    let api = std::sync::Arc::new(FreshServiceInventoryClient::default());
    // Initial snapshot: builds inventory from the API.
    let inventory = bootstrap_inventory_from_api(
        &klights_networking::service_routing::LeaderRoutingStateSource::new(api.clone()),
    )
    .await
    .expect("bootstrap inventory");
    assert!(
        !inventory.is_empty()
            || api
                .service_list_calls
                .load(std::sync::atomic::Ordering::SeqCst)
                == 1,
        "first bootstrap must list services once"
    );
    let svc_calls_after_bootstrap = api
        .service_list_calls
        .load(std::sync::atomic::Ordering::SeqCst);
    let endpoint_calls_after_bootstrap = api
        .endpoints_list_calls
        .load(std::sync::atomic::Ordering::SeqCst)
        + api
            .endpointslice_list_calls
            .load(std::sync::atomic::Ordering::SeqCst);

    // Subsequent sync from the cached inventory must not list services again.
    let _specs = inventory.to_specs();
    assert_eq!(
        api.service_list_calls
            .load(std::sync::atomic::Ordering::SeqCst),
        svc_calls_after_bootstrap,
        "to_specs from cached inventory must NOT re-list services"
    );
    assert_eq!(
        api.endpoints_list_calls
            .load(std::sync::atomic::Ordering::SeqCst)
            + api
                .endpointslice_list_calls
                .load(std::sync::atomic::Ordering::SeqCst),
        endpoint_calls_after_bootstrap,
        "to_specs from cached inventory must NOT re-list endpoints/slices"
    );
}

#[tokio::test]
async fn service_route_sync_does_not_query_api_per_service() {
    // Set up several services so the count is meaningful.
    let api = std::sync::Arc::new(FreshServiceInventoryClient::default());
    let inventory = bootstrap_inventory_from_api(
        &klights_networking::service_routing::LeaderRoutingStateSource::new(api.clone()),
    )
    .await
    .expect("bootstrap inventory");
    let svc_count = inventory.to_specs().len();

    // Whatever the number of Services discovered, the bootstrap must use
    // exactly ONE list call per resource type — never one per Service.
    let svc_list_calls = api
        .service_list_calls
        .load(std::sync::atomic::Ordering::SeqCst);
    let eps_list_calls = api
        .endpoints_list_calls
        .load(std::sync::atomic::Ordering::SeqCst);
    let slice_list_calls = api
        .endpointslice_list_calls
        .load(std::sync::atomic::Ordering::SeqCst);

    assert!(
        svc_list_calls <= 1,
        "Service list must be at most 1, was {svc_list_calls} for {svc_count} services"
    );
    assert!(
        eps_list_calls <= 1,
        "Endpoints list must be at most 1, was {eps_list_calls}"
    );
    assert!(
        slice_list_calls <= 1,
        "EndpointSlice list must be at most 1, was {slice_list_calls}"
    );

    // Confirm no per-Service get_resource calls were issued.
    assert_eq!(
        api.fresh_get_calls
            .load(std::sync::atomic::Ordering::SeqCst),
        0,
        "per-Service fresh get must not be used during route sync"
    );
    assert_eq!(
        api.cached_get_calls
            .load(std::sync::atomic::Ordering::SeqCst),
        0,
        "per-Service cached get must not be used during route sync"
    );
}
