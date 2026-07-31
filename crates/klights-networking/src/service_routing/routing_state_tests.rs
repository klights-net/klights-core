use super::*;
use serde_json::json;
use std::net::Ipv4Addr;
use std::sync::Arc;

#[derive(Default)]
struct Fixture {
    legacy_empty: bool,
    legacy_ips: Vec<&'static str>,
    slice_ips: Option<Vec<&'static str>>,
    service_ports: Option<Vec<serde_json::Value>>,
    legacy_ports: Option<Vec<serde_json::Value>>,
    slice_ports: Option<Vec<serde_json::Value>>,
}

impl Fixture {
    fn resource(
        api_version: &str,
        kind: &str,
        name: &str,
        data: serde_json::Value,
    ) -> ServiceRoutingResource {
        ServiceRoutingResource {
            api_version: api_version.to_string(),
            kind: kind.to_string(),
            namespace: Some("kube-system".to_string()),
            name: name.to_string(),
            resource_version: 1,
            data: Arc::new(data),
        }
    }

    fn snapshot(&self) -> ServiceRoutingSnapshot {
        let default_ports = || {
            vec![
                json!({"name": "dns", "port": 53, "protocol": "UDP"}),
                json!({"name": "dns-tcp", "port": 53, "protocol": "TCP"}),
            ]
        };
        let service_ports = self.service_ports.clone().unwrap_or_else(default_ports);
        let legacy_ports = self.legacy_ports.clone().unwrap_or_else(default_ports);
        let slice_ports = self.slice_ports.clone().unwrap_or_else(default_ports);
        let legacy_ips = if self.legacy_ips.is_empty() {
            vec!["10.50.0.2"]
        } else {
            self.legacy_ips.clone()
        };
        let slice_ips = self.slice_ips.clone().unwrap_or_else(|| vec!["10.50.0.20"]);

        ServiceRoutingSnapshot {
            services: vec![Self::resource(
                "v1",
                "Service",
                "kube-dns",
                json!({
                    "apiVersion": "v1",
                    "kind": "Service",
                    "metadata": {"namespace": "kube-system", "name": "kube-dns"},
                    "spec": {"clusterIP": "10.51.0.10", "ports": service_ports}
                }),
            )],
            endpoints: vec![Self::resource(
                "v1",
                "Endpoints",
                "kube-dns",
                json!({
                    "apiVersion": "v1",
                    "kind": "Endpoints",
                    "metadata": {"namespace": "kube-system", "name": "kube-dns"},
                    "subsets": if self.legacy_empty {
                        json!([])
                    } else {
                        json!([{
                            "addresses": legacy_ips.into_iter()
                                .map(|ip| json!({"ip": ip}))
                                .collect::<Vec<_>>(),
                            "ports": legacy_ports
                        }])
                    }
                }),
            )],
            endpoint_slices: vec![Self::resource(
                "discovery.k8s.io/v1",
                "EndpointSlice",
                "kube-dns-klights",
                json!({
                    "apiVersion": "discovery.k8s.io/v1",
                    "kind": "EndpointSlice",
                    "metadata": {
                        "namespace": "kube-system",
                        "name": "kube-dns-klights",
                        "labels": {"kubernetes.io/service-name": "kube-dns"}
                    },
                    "addressType": "IPv4",
                    "ports": slice_ports,
                    "endpoints": slice_ips.into_iter().map(|ip| json!({
                        "addresses": [ip],
                        "conditions": {"ready": true}
                    })).collect::<Vec<_>>()
                }),
            )],
        }
    }
}

impl RoutingStateSource for Fixture {
    fn service_routing_snapshot(&self) -> RoutingStateFuture<'_, ServiceRoutingSnapshot> {
        Box::pin(async { Ok(self.snapshot()) })
    }

    fn network_policy_snapshot(&self) -> RoutingStateFuture<'_, NetworkPolicySnapshot> {
        Box::pin(async { Ok(NetworkPolicySnapshot::default()) })
    }
}

async fn specs(fixture: Fixture) -> Vec<ServiceSpec> {
    service_specs_from_api(&fixture).await.unwrap()
}

fn tuples(specs: &[ServiceSpec]) -> Vec<(Protocol, u16, u16, Vec<Ipv4Addr>)> {
    let mut tuples = specs[0]
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
        .collect::<Vec<_>>();
    tuples.sort_by_key(|(protocol, service_port, _, _)| (*protocol, *service_port));
    tuples
}

#[tokio::test]
async fn service_specs_from_api_prefers_observed_endpointslice_state_over_larger_legacy_snapshot() {
    let specs = specs(Fixture {
        legacy_ips: vec!["10.50.0.2", "10.50.0.3"],
        slice_ips: Some(vec!["10.50.0.20"]),
        ..Default::default()
    })
    .await;
    assert_eq!(
        tuples(&specs),
        vec![
            (Protocol::Tcp, 53, 53, vec![Ipv4Addr::new(10, 50, 0, 20)]),
            (Protocol::Udp, 53, 53, vec![Ipv4Addr::new(10, 50, 0, 20)]),
        ]
    );
}

#[tokio::test]
async fn service_specs_from_api_observed_empty_endpointslice_does_not_revive_stale_legacy() {
    let specs = specs(Fixture {
        legacy_ips: vec!["10.50.0.2"],
        slice_ips: Some(Vec::new()),
        ..Default::default()
    })
    .await;
    assert!(specs[0].ports.iter().all(|port| port.endpoints.is_empty()));
}

#[tokio::test]
async fn service_specs_from_api_falls_back_to_ready_endpointslices_when_legacy_endpoints_empty() {
    let specs = specs(Fixture {
        legacy_empty: true,
        slice_ips: Some(vec!["10.50.0.20"]),
        ..Default::default()
    })
    .await;
    assert_eq!(
        tuples(&specs),
        vec![
            (Protocol::Tcp, 53, 53, vec![Ipv4Addr::new(10, 50, 0, 20)]),
            (Protocol::Udp, 53, 53, vec![Ipv4Addr::new(10, 50, 0, 20)]),
        ]
    );
}

#[tokio::test]
async fn service_specs_from_api_prefers_complete_endpointslices_over_partial_legacy_endpoints() {
    let specs = specs(Fixture {
        legacy_ips: vec!["10.50.0.2"],
        slice_ips: Some(vec!["10.50.0.20"]),
        ..Default::default()
    })
    .await;
    assert_eq!(
        tuples(&specs),
        vec![
            (Protocol::Tcp, 53, 53, vec![Ipv4Addr::new(10, 50, 0, 20)]),
            (Protocol::Udp, 53, 53, vec![Ipv4Addr::new(10, 50, 0, 20)]),
        ]
    );
}

#[tokio::test]
async fn service_specs_from_api_merges_protocol_ports_from_partial_endpoint_sources() {
    let specs = specs(Fixture {
        legacy_ips: vec!["10.50.0.2"],
        slice_ips: Some(vec!["10.50.0.20"]),
        service_ports: Some(vec![
            json!({"name": "tcp-port", "port": 80, "targetPort": 80, "protocol": "TCP"}),
            json!({"name": "udp-port", "port": 80, "targetPort": 80, "protocol": "UDP"}),
        ]),
        legacy_ports: Some(vec![
            json!({"name": "udp-port", "port": 80, "protocol": "UDP"}),
        ]),
        slice_ports: Some(vec![
            json!({"name": "tcp-port", "port": 80, "protocol": "TCP"}),
        ]),
        ..Default::default()
    })
    .await;
    assert_eq!(
        tuples(&specs),
        vec![
            (Protocol::Tcp, 80, 80, vec![Ipv4Addr::new(10, 50, 0, 20)]),
            (Protocol::Udp, 80, 80, vec![Ipv4Addr::new(10, 50, 0, 2)]),
        ]
    );
}

#[tokio::test]
async fn service_specs_from_api_preserves_sctp_endpointslice_ports() {
    let specs = specs(Fixture {
        legacy_empty: true,
        slice_ips: Some(vec!["10.50.0.20"]),
        service_ports: Some(vec![
            json!({"name": "sctp", "port": 5000, "protocol": "SCTP"}),
        ]),
        slice_ports: Some(vec![
            json!({"name": "sctp", "port": 5000, "protocol": "SCTP"}),
        ]),
        ..Default::default()
    })
    .await;
    assert_eq!(
        tuples(&specs),
        vec![(
            Protocol::Sctp,
            5000,
            5000,
            vec![Ipv4Addr::new(10, 50, 0, 20)]
        )]
    );
}
