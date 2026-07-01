//! Pure NetworkPolicy planning for nftables datapath enforcement.

use anyhow::Result;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::net::Ipv4Addr;

use super::Protocol;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NetworkPolicyPlan {
    pub isolated_ingress: BTreeSet<Ipv4Addr>,
    pub isolated_egress: BTreeSet<Ipv4Addr>,
    pub allowed_flows: BTreeSet<NetworkPolicyFlow>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct NetworkPolicyFlow {
    pub direction: NetworkPolicyDirection,
    pub pod_ip: Ipv4Addr,
    pub peer: NetworkPolicyPeerMatch,
    pub port: Option<NetworkPolicyPortMatch>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum NetworkPolicyDirection {
    Ingress,
    Egress,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum NetworkPolicyPeerMatch {
    Any,
    IpBlock {
        cidr: Ipv4CidrMatch,
        except: Vec<Ipv4CidrMatch>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct Ipv4CidrMatch {
    pub network: Ipv4Addr,
    pub mask: Ipv4Addr,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct NetworkPolicyPortMatch {
    pub protocol: Protocol,
    pub port: u16,
    pub end_port: u16,
}

impl NetworkPolicyPlan {
    pub fn from_resources(
        policies: &[Value],
        pods: &[Value],
        namespaces: &[Value],
    ) -> Result<Self> {
        let pod_infos = pod_infos_from_resources(pods);
        let namespace_labels = namespace_labels_from_resources(namespaces);
        let mut plan = Self::default();

        for policy in policies {
            let Some(spec) = policy.get("spec") else {
                continue;
            };
            let Some(policy_namespace) = policy
                .pointer("/metadata/namespace")
                .and_then(|v| v.as_str())
                .filter(|value| !value.is_empty())
            else {
                continue;
            };

            let selector = crate::label_selector::LabelSelector::from_k8s_selector(
                spec.get("podSelector").unwrap_or(&Value::Null),
            )?;
            let selected_pods: Vec<&PodInfo> = pod_infos
                .iter()
                .filter(|pod| {
                    pod.namespace == policy_namespace
                        && selector.matches_labels(pod.labels.as_ref())
                })
                .collect();
            if selected_pods.is_empty() {
                continue;
            }

            let policy_types = policy_types_for(spec);
            if policy_types.ingress {
                for pod in &selected_pods {
                    plan.isolated_ingress.insert(pod.ip);
                }
                append_direction_flows(
                    &mut plan,
                    NetworkPolicyDirection::Ingress,
                    spec.get("ingress"),
                    policy_namespace,
                    &selected_pods,
                    &pod_infos,
                    &namespace_labels,
                )?;
            }
            if policy_types.egress {
                for pod in &selected_pods {
                    plan.isolated_egress.insert(pod.ip);
                }
                append_direction_flows(
                    &mut plan,
                    NetworkPolicyDirection::Egress,
                    spec.get("egress"),
                    policy_namespace,
                    &selected_pods,
                    &pod_infos,
                    &namespace_labels,
                )?;
            }
        }

        Ok(plan)
    }
}

#[derive(Clone, Debug)]
struct PodInfo {
    namespace: String,
    ip: Ipv4Addr,
    labels: Option<serde_json::Map<String, Value>>,
    named_ports: BTreeMap<(String, Protocol), u16>,
}

#[derive(Clone, Debug)]
struct PeerCandidate {
    policy_match: NetworkPolicyPeerMatch,
    named_ports: BTreeMap<(String, Protocol), u16>,
}

#[derive(Clone, Copy)]
struct PolicyTypes {
    ingress: bool,
    egress: bool,
}

fn policy_types_for(spec: &Value) -> PolicyTypes {
    if let Some(types) = spec.get("policyTypes").and_then(|v| v.as_array()) {
        return PolicyTypes {
            ingress: types.iter().any(|value| value.as_str() == Some("Ingress")),
            egress: types.iter().any(|value| value.as_str() == Some("Egress")),
        };
    }
    PolicyTypes {
        ingress: true,
        egress: spec.get("egress").is_some(),
    }
}

fn pod_infos_from_resources(pods: &[Value]) -> Vec<PodInfo> {
    pods.iter()
        .filter_map(|pod| {
            let namespace = pod.pointer("/metadata/namespace")?.as_str()?.to_string();
            let ip = pod.pointer("/status/podIP")?.as_str()?.parse().ok()?;
            Some(PodInfo {
                namespace,
                ip,
                labels: pod
                    .pointer("/metadata/labels")
                    .and_then(|v| v.as_object())
                    .cloned(),
                named_ports: named_ports_from_pod(pod),
            })
        })
        .collect()
}

fn named_ports_from_pod(pod: &Value) -> BTreeMap<(String, Protocol), u16> {
    let mut out = BTreeMap::new();
    let Some(containers) = pod.pointer("/spec/containers").and_then(|v| v.as_array()) else {
        return out;
    };

    for container in containers {
        let Some(ports) = container.get("ports").and_then(|v| v.as_array()) else {
            continue;
        };
        for port in ports {
            let Some(name) = port
                .get("name")
                .and_then(|v| v.as_str())
                .filter(|value| !value.is_empty())
            else {
                continue;
            };
            let Some(protocol) = Protocol::parse(port.get("protocol").and_then(|v| v.as_str()))
            else {
                continue;
            };
            let Some(port_number) = port.get("containerPort").and_then(|v| v.as_u64()) else {
                continue;
            };
            let Ok(port_number) = u16::try_from(port_number) else {
                continue;
            };
            out.insert((name.to_string(), protocol), port_number);
        }
    }
    out
}

fn namespace_labels_from_resources(
    namespaces: &[Value],
) -> BTreeMap<String, Option<serde_json::Map<String, Value>>> {
    namespaces
        .iter()
        .filter_map(|namespace| {
            let name = namespace.pointer("/metadata/name")?.as_str()?.to_string();
            let labels = namespace
                .pointer("/metadata/labels")
                .and_then(|v| v.as_object())
                .cloned();
            Some((name, labels))
        })
        .collect()
}

fn append_direction_flows(
    plan: &mut NetworkPolicyPlan,
    direction: NetworkPolicyDirection,
    rules_value: Option<&Value>,
    policy_namespace: &str,
    selected_pods: &[&PodInfo],
    all_pods: &[PodInfo],
    namespaces: &BTreeMap<String, Option<serde_json::Map<String, Value>>>,
) -> Result<()> {
    let Some(rules) = rules_value.and_then(|v| v.as_array()) else {
        return Ok(());
    };

    for rule in rules {
        let peers_field = match direction {
            NetworkPolicyDirection::Ingress => "from",
            NetworkPolicyDirection::Egress => "to",
        };
        let peers = peers_for_rule(
            rule.get(peers_field),
            policy_namespace,
            all_pods,
            namespaces,
        )?;
        let port_specs = port_specs_for_rule(rule.get("ports"));

        for selected_pod in selected_pods {
            for peer in &peers {
                for port_spec in &port_specs {
                    let Some(port) = resolve_port_spec(port_spec, direction, selected_pod, peer)
                    else {
                        continue;
                    };
                    plan.allowed_flows.insert(NetworkPolicyFlow {
                        direction,
                        pod_ip: selected_pod.ip,
                        peer: peer.policy_match.clone(),
                        port,
                    });
                }
            }
        }
    }
    Ok(())
}

fn peers_for_rule(
    peers_value: Option<&Value>,
    policy_namespace: &str,
    all_pods: &[PodInfo],
    namespaces: &BTreeMap<String, Option<serde_json::Map<String, Value>>>,
) -> Result<Vec<PeerCandidate>> {
    let Some(peers) = peers_value.and_then(|v| v.as_array()) else {
        return Ok(vec![PeerCandidate::any()]);
    };
    if peers.is_empty() {
        return Ok(vec![PeerCandidate::any()]);
    }

    let mut out = BTreeMap::new();
    for peer in peers {
        if let Some(ip_block) = peer.get("ipBlock") {
            let cidr = ip_block
                .get("cidr")
                .and_then(|v| v.as_str())
                .map(parse_ipv4_cidr)
                .transpose()?;
            let Some(cidr) = cidr else {
                continue;
            };
            let except = ip_block
                .get("except")
                .and_then(|v| v.as_array())
                .map(|items| {
                    items
                        .iter()
                        .filter_map(|item| item.as_str())
                        .map(parse_ipv4_cidr)
                        .collect::<Result<Vec<_>>>()
                })
                .transpose()?
                .unwrap_or_default();
            let candidate = PeerCandidate {
                policy_match: NetworkPolicyPeerMatch::IpBlock { cidr, except },
                named_ports: BTreeMap::new(),
            };
            out.insert(candidate.policy_match.clone(), candidate);
            continue;
        }

        let has_namespace_selector = peer.get("namespaceSelector").is_some();
        let has_pod_selector = peer.get("podSelector").is_some();
        if !has_namespace_selector && !has_pod_selector {
            let candidate = PeerCandidate::any();
            out.insert(candidate.policy_match.clone(), candidate);
            continue;
        }

        let namespace_selector = peer
            .get("namespaceSelector")
            .map(crate::label_selector::LabelSelector::from_k8s_selector)
            .transpose()?;
        let pod_selector = peer
            .get("podSelector")
            .map(crate::label_selector::LabelSelector::from_k8s_selector)
            .transpose()?;

        for pod in all_pods {
            if let Some(selector) = &namespace_selector {
                let labels = namespaces
                    .get(&pod.namespace)
                    .and_then(|labels| labels.as_ref());
                if !selector.matches_labels(labels) {
                    continue;
                }
            } else if pod.namespace != policy_namespace {
                continue;
            }

            if let Some(selector) = &pod_selector
                && !selector.matches_labels(pod.labels.as_ref())
            {
                continue;
            }

            let candidate = PeerCandidate {
                policy_match: NetworkPolicyPeerMatch::IpBlock {
                    cidr: Ipv4CidrMatch::host(pod.ip),
                    except: Vec::new(),
                },
                named_ports: pod.named_ports.clone(),
            };
            out.insert(candidate.policy_match.clone(), candidate);
        }
    }

    Ok(out.into_values().collect())
}

#[derive(Clone, Debug)]
enum NetworkPolicyPortSpec {
    Any,
    Numeric(NetworkPolicyPortMatch),
    Named { protocol: Protocol, name: String },
}

fn port_specs_for_rule(ports_value: Option<&Value>) -> Vec<NetworkPolicyPortSpec> {
    let Some(ports) = ports_value.and_then(|v| v.as_array()) else {
        return vec![NetworkPolicyPortSpec::Any];
    };
    if ports.is_empty() {
        return vec![NetworkPolicyPortSpec::Any];
    }

    ports
        .iter()
        .filter_map(|port| {
            let protocol = Protocol::parse(port.get("protocol").and_then(|v| v.as_str()))?;
            let port_value = port.get("port")?;
            if let Some(port_number) = port_value.as_u64() {
                let port_number = u16::try_from(port_number).ok()?;
                let end_port = port
                    .get("endPort")
                    .and_then(|v| v.as_u64())
                    .and_then(|value| u16::try_from(value).ok())
                    .unwrap_or(port_number);
                if end_port < port_number {
                    return None;
                }
                return Some(NetworkPolicyPortSpec::Numeric(NetworkPolicyPortMatch {
                    protocol,
                    port: port_number,
                    end_port,
                }));
            }
            let name = port_value.as_str()?.to_string();
            Some(NetworkPolicyPortSpec::Named { protocol, name })
        })
        .collect()
}

fn resolve_port_spec(
    port_spec: &NetworkPolicyPortSpec,
    direction: NetworkPolicyDirection,
    selected_pod: &PodInfo,
    peer: &PeerCandidate,
) -> Option<Option<NetworkPolicyPortMatch>> {
    match port_spec {
        NetworkPolicyPortSpec::Any => Some(None),
        NetworkPolicyPortSpec::Numeric(port) => Some(Some(*port)),
        NetworkPolicyPortSpec::Named { protocol, name } => {
            let port_number = match direction {
                NetworkPolicyDirection::Ingress => selected_pod
                    .named_ports
                    .get(&(name.clone(), *protocol))
                    .copied(),
                NetworkPolicyDirection::Egress => {
                    peer.named_ports.get(&(name.clone(), *protocol)).copied()
                }
            }?;
            Some(Some(NetworkPolicyPortMatch {
                protocol: *protocol,
                port: port_number,
                end_port: port_number,
            }))
        }
    }
}

impl PeerCandidate {
    fn any() -> Self {
        Self {
            policy_match: NetworkPolicyPeerMatch::Any,
            named_ports: BTreeMap::new(),
        }
    }
}

fn parse_ipv4_cidr(raw: &str) -> Result<Ipv4CidrMatch> {
    let (addr, prefix) = raw
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("CIDR must be a.b.c.d/prefix: {raw}"))?;
    let ip: Ipv4Addr = addr.parse()?;
    let prefix: u8 = prefix.parse()?;
    if prefix > 32 {
        anyhow::bail!("IPv4 CIDR prefix must be <= 32: {raw}");
    }
    let mask_u32 = if prefix == 0 {
        0
    } else {
        u32::MAX << (32 - prefix)
    };
    let network = Ipv4Addr::from(u32::from(ip) & mask_u32);
    Ok(Ipv4CidrMatch {
        network,
        mask: Ipv4Addr::from(mask_u32),
    })
}

impl Ipv4CidrMatch {
    pub fn host(ip: Ipv4Addr) -> Self {
        Self {
            network: ip,
            mask: Ipv4Addr::from(u32::MAX),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn pod(namespace: &str, name: &str, ip: &str, labels: Value) -> Value {
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"namespace": namespace, "name": name, "labels": labels},
            "status": {"podIP": ip}
        })
    }

    fn pod_with_ports(namespace: &str, name: &str, ip: &str, labels: Value, ports: Value) -> Value {
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"namespace": namespace, "name": name, "labels": labels},
            "spec": {"containers": [{"name": "main", "ports": ports}]},
            "status": {"podIP": ip}
        })
    }

    fn namespace(name: &str, labels: Value) -> Value {
        json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {"name": name, "labels": labels}
        })
    }

    #[test]
    fn default_deny_ingress_isolates_only_selected_pods() {
        let policies = vec![json!({
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": {"namespace": "default", "name": "deny-web"},
            "spec": {
                "podSelector": {"matchLabels": {"app": "web"}},
                "policyTypes": ["Ingress"]
            }
        })];
        let pods = vec![
            pod("default", "web", "10.42.0.10", json!({"app": "web"})),
            pod("default", "api", "10.42.0.11", json!({"app": "api"})),
        ];
        let plan = NetworkPolicyPlan::from_resources(&policies, &pods, &[]).unwrap();

        assert_eq!(
            plan.isolated_ingress,
            BTreeSet::from(["10.42.0.10".parse().unwrap()])
        );
        assert!(plan.isolated_egress.is_empty());
        assert!(plan.allowed_flows.is_empty());
    }

    #[test]
    fn ingress_pod_selector_rule_allows_matching_peer_on_port() {
        let policies = vec![json!({
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": {"namespace": "default", "name": "allow-api-to-web"},
            "spec": {
                "podSelector": {"matchLabels": {"app": "web"}},
                "policyTypes": ["Ingress"],
                "ingress": [{
                    "from": [{"podSelector": {"matchLabels": {"role": "api"}}}],
                    "ports": [{"protocol": "TCP", "port": 8443}]
                }]
            }
        })];
        let pods = vec![
            pod("default", "web", "10.42.0.10", json!({"app": "web"})),
            pod("default", "api", "10.42.0.11", json!({"role": "api"})),
            pod("other", "api", "10.42.1.11", json!({"role": "api"})),
        ];
        let plan = NetworkPolicyPlan::from_resources(&policies, &pods, &[]).unwrap();

        assert_eq!(
            plan.allowed_flows,
            BTreeSet::from([NetworkPolicyFlow {
                direction: NetworkPolicyDirection::Ingress,
                pod_ip: "10.42.0.10".parse().unwrap(),
                peer: NetworkPolicyPeerMatch::IpBlock {
                    cidr: Ipv4CidrMatch {
                        network: "10.42.0.11".parse().unwrap(),
                        mask: "255.255.255.255".parse().unwrap(),
                    },
                    except: Vec::new(),
                },
                port: Some(NetworkPolicyPortMatch {
                    protocol: Protocol::Tcp,
                    port: 8443,
                    end_port: 8443,
                }),
            }])
        );
    }

    #[test]
    fn ingress_named_port_resolves_against_selected_pod_container_port() {
        let policies = vec![json!({
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": {"namespace": "default", "name": "allow-api-to-web-http"},
            "spec": {
                "podSelector": {"matchLabels": {"app": "web"}},
                "policyTypes": ["Ingress"],
                "ingress": [{
                    "from": [{"podSelector": {"matchLabels": {"role": "api"}}}],
                    "ports": [{"protocol": "TCP", "port": "http"}]
                }]
            }
        })];
        let pods = vec![
            pod_with_ports(
                "default",
                "web",
                "10.42.0.10",
                json!({"app": "web"}),
                json!([{"name": "http", "containerPort": 8080, "protocol": "TCP"}]),
            ),
            pod("default", "api", "10.42.0.11", json!({"role": "api"})),
        ];
        let plan = NetworkPolicyPlan::from_resources(&policies, &pods, &[]).unwrap();

        assert_eq!(
            plan.allowed_flows,
            BTreeSet::from([NetworkPolicyFlow {
                direction: NetworkPolicyDirection::Ingress,
                pod_ip: "10.42.0.10".parse().unwrap(),
                peer: NetworkPolicyPeerMatch::IpBlock {
                    cidr: Ipv4CidrMatch::host("10.42.0.11".parse().unwrap()),
                    except: Vec::new(),
                },
                port: Some(NetworkPolicyPortMatch {
                    protocol: Protocol::Tcp,
                    port: 8080,
                    end_port: 8080,
                }),
            }])
        );
    }

    #[test]
    fn ingress_sctp_port_rule_is_preserved() {
        let policies = vec![json!({
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": {"namespace": "default", "name": "allow-sctp"},
            "spec": {
                "podSelector": {"matchLabels": {"app": "web"}},
                "policyTypes": ["Ingress"],
                "ingress": [{
                    "from": [{"ipBlock": {"cidr": "10.42.0.0/16"}}],
                    "ports": [{"protocol": "SCTP", "port": 5000, "endPort": 5002}]
                }]
            }
        })];
        let pods = vec![pod("default", "web", "10.42.0.10", json!({"app": "web"}))];
        let plan = NetworkPolicyPlan::from_resources(&policies, &pods, &[]).unwrap();

        assert_eq!(
            plan.allowed_flows,
            BTreeSet::from([NetworkPolicyFlow {
                direction: NetworkPolicyDirection::Ingress,
                pod_ip: "10.42.0.10".parse().unwrap(),
                peer: NetworkPolicyPeerMatch::IpBlock {
                    cidr: Ipv4CidrMatch {
                        network: "10.42.0.0".parse().unwrap(),
                        mask: "255.255.0.0".parse().unwrap(),
                    },
                    except: Vec::new(),
                },
                port: Some(NetworkPolicyPortMatch {
                    protocol: Protocol::Sctp,
                    port: 5000,
                    end_port: 5002,
                }),
            }])
        );
    }

    #[test]
    fn ingress_named_sctp_port_resolves_against_selected_pod_container_port() {
        let policies = vec![json!({
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": {"namespace": "default", "name": "allow-sctp-named"},
            "spec": {
                "podSelector": {"matchLabels": {"app": "web"}},
                "policyTypes": ["Ingress"],
                "ingress": [{
                    "from": [{"podSelector": {"matchLabels": {"role": "client"}}}],
                    "ports": [{"protocol": "SCTP", "port": "sig"}]
                }]
            }
        })];
        let pods = vec![
            pod_with_ports(
                "default",
                "web",
                "10.42.0.10",
                json!({"app": "web"}),
                json!([{"name": "sig", "containerPort": 5000, "protocol": "SCTP"}]),
            ),
            pod("default", "client", "10.42.0.11", json!({"role": "client"})),
        ];
        let plan = NetworkPolicyPlan::from_resources(&policies, &pods, &[]).unwrap();

        assert!(plan.allowed_flows.contains(&NetworkPolicyFlow {
            direction: NetworkPolicyDirection::Ingress,
            pod_ip: "10.42.0.10".parse().unwrap(),
            peer: NetworkPolicyPeerMatch::IpBlock {
                cidr: Ipv4CidrMatch::host("10.42.0.11".parse().unwrap()),
                except: Vec::new(),
            },
            port: Some(NetworkPolicyPortMatch {
                protocol: Protocol::Sctp,
                port: 5000,
                end_port: 5000,
            }),
        }));
    }

    #[test]
    fn numeric_end_port_preserves_range_match() {
        let policies = vec![json!({
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": {"namespace": "default", "name": "allow-egress-range"},
            "spec": {
                "podSelector": {"matchLabels": {"app": "web"}},
                "policyTypes": ["Egress"],
                "egress": [{
                    "to": [{"ipBlock": {"cidr": "10.50.0.0/16"}}],
                    "ports": [{"protocol": "TCP", "port": 8000, "endPort": 8002}]
                }]
            }
        })];
        let pods = vec![pod("default", "web", "10.42.0.10", json!({"app": "web"}))];
        let plan = NetworkPolicyPlan::from_resources(&policies, &pods, &[]).unwrap();

        assert_eq!(
            plan.allowed_flows,
            BTreeSet::from([NetworkPolicyFlow {
                direction: NetworkPolicyDirection::Egress,
                pod_ip: "10.42.0.10".parse().unwrap(),
                peer: NetworkPolicyPeerMatch::IpBlock {
                    cidr: Ipv4CidrMatch {
                        network: "10.50.0.0".parse().unwrap(),
                        mask: "255.255.0.0".parse().unwrap(),
                    },
                    except: Vec::new(),
                },
                port: Some(NetworkPolicyPortMatch {
                    protocol: Protocol::Tcp,
                    port: 8000,
                    end_port: 8002,
                }),
            }])
        );
    }

    #[test]
    fn egress_ipblock_except_rule_preserves_except_matchers() {
        let policies = vec![json!({
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": {"namespace": "default", "name": "allow-db-egress"},
            "spec": {
                "podSelector": {"matchLabels": {"app": "web"}},
                "policyTypes": ["Egress"],
                "egress": [{
                    "to": [{"ipBlock": {"cidr": "10.50.0.0/16", "except": ["10.50.9.0/24"]}}],
                    "ports": [{"protocol": "UDP", "port": 5353}]
                }]
            }
        })];
        let pods = vec![pod("default", "web", "10.42.0.10", json!({"app": "web"}))];
        let plan = NetworkPolicyPlan::from_resources(&policies, &pods, &[]).unwrap();

        assert_eq!(
            plan.isolated_egress,
            BTreeSet::from(["10.42.0.10".parse().unwrap()])
        );
        assert_eq!(
            plan.allowed_flows,
            BTreeSet::from([NetworkPolicyFlow {
                direction: NetworkPolicyDirection::Egress,
                pod_ip: "10.42.0.10".parse().unwrap(),
                peer: NetworkPolicyPeerMatch::IpBlock {
                    cidr: Ipv4CidrMatch {
                        network: "10.50.0.0".parse().unwrap(),
                        mask: "255.255.0.0".parse().unwrap(),
                    },
                    except: vec![Ipv4CidrMatch {
                        network: "10.50.9.0".parse().unwrap(),
                        mask: "255.255.255.0".parse().unwrap(),
                    }],
                },
                port: Some(NetworkPolicyPortMatch {
                    protocol: Protocol::Udp,
                    port: 5353,
                    end_port: 5353,
                }),
            }])
        );
    }

    #[test]
    fn namespace_and_pod_selector_peer_requires_both_to_match() {
        let policies = vec![json!({
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": {"namespace": "default", "name": "allow-prod-clients"},
            "spec": {
                "podSelector": {"matchLabels": {"app": "web"}},
                "ingress": [{
                    "from": [{
                        "namespaceSelector": {"matchLabels": {"env": "prod"}},
                        "podSelector": {"matchLabels": {"role": "client"}}
                    }]
                }]
            }
        })];
        let pods = vec![
            pod("default", "web", "10.42.0.10", json!({"app": "web"})),
            pod("team-a", "client", "10.42.1.10", json!({"role": "client"})),
            pod("team-b", "client", "10.42.2.10", json!({"role": "client"})),
        ];
        let namespaces = vec![
            namespace("team-a", json!({"env": "prod"})),
            namespace("team-b", json!({"env": "dev"})),
        ];
        let plan = NetworkPolicyPlan::from_resources(&policies, &pods, &namespaces).unwrap();

        assert!(plan.allowed_flows.contains(&NetworkPolicyFlow {
            direction: NetworkPolicyDirection::Ingress,
            pod_ip: "10.42.0.10".parse().unwrap(),
            peer: NetworkPolicyPeerMatch::IpBlock {
                cidr: Ipv4CidrMatch {
                    network: "10.42.1.10".parse().unwrap(),
                    mask: "255.255.255.255".parse().unwrap(),
                },
                except: Vec::new(),
            },
            port: None,
        }));
        assert!(!plan.allowed_flows.iter().any(|flow| {
            matches!(
                &flow.peer,
                NetworkPolicyPeerMatch::IpBlock { cidr, .. }
                    if cidr.network == "10.42.2.10".parse::<Ipv4Addr>().unwrap()
            )
        }));
    }
}
