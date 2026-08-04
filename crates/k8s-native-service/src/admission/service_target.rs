use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use serde_json::Value;

use super::{AdmissionDependencyError, AdmissionQuery, WebhookTarget, WebhookTargetResolver};

/// Resolves admission webhook URL and Service references through focused
/// admission resource reads. Kubernetes Service routing remains authoritative:
/// the returned DNS override targets the ClusterIP, never an individual Pod.
pub struct ServiceWebhookTargetResolver {
    query: Arc<dyn AdmissionQuery>,
}

impl ServiceWebhookTargetResolver {
    pub fn new(query: Arc<dyn AdmissionQuery>) -> Arc<Self> {
        Arc::new(Self { query })
    }

    async fn service_target(
        &self,
        namespace: &str,
        name: &str,
        requested_port: u16,
    ) -> Result<SocketAddr, AdmissionDependencyError> {
        let service = self
            .query
            .get_resource("v1", "Service", Some(namespace), name)
            .await?
            .ok_or_else(|| {
                AdmissionDependencyError::new(format!("Service not found: {namespace}/{name}"))
            })?;
        let cluster_ip = routable_cluster_ip(&service.data).ok_or_else(|| {
            AdmissionDependencyError::new(format!(
                "Service {namespace}/{name} has no ready endpoints"
            ))
        })?;

        let label_selector = format!("kubernetes.io/service-name={name}");
        let endpoint_slices = self
            .query
            .list_resources(
                "discovery.k8s.io/v1",
                "EndpointSlice",
                Some(namespace),
                Some(&label_selector),
            )
            .await?;
        let slice_projection = (!endpoint_slices.is_empty())
            .then(|| endpoint_slice_ports(&service.data, &endpoint_slices))
            .flatten();
        let projected_ports = if let Some(ports) = slice_projection {
            Some(ports)
        } else {
            self.query
                .get_resource("v1", "Endpoints", Some(namespace), name)
                .await?
                .and_then(|endpoints| endpoint_ports(&service.data, &endpoints.data))
        };
        let Some(projected_ports) = projected_ports else {
            return Err(AdmissionDependencyError::new(format!(
                "Service {namespace}/{name} has no ready endpoints"
            )));
        };
        let has_ready_requested_port = projected_ports.iter().any(|port| {
            port.protocol == Protocol::Tcp
                && port.service_port == requested_port
                && port.has_ready_endpoint
        });
        if !has_ready_requested_port {
            return Err(AdmissionDependencyError::new(format!(
                "Service {namespace}/{name} has no ready TCP endpoint for port {requested_port}"
            )));
        }
        Ok(SocketAddr::new(IpAddr::V4(cluster_ip), requested_port))
    }
}

#[async_trait::async_trait]
impl WebhookTargetResolver for ServiceWebhookTargetResolver {
    async fn resolve(
        &self,
        client_config: &Value,
    ) -> Result<WebhookTarget, AdmissionDependencyError> {
        if let Some(url) = client_config.get("url").and_then(Value::as_str) {
            return Ok(WebhookTarget {
                base_url: url.to_string(),
                dns_override: None,
            });
        }

        let service_ref = client_config.get("service").ok_or_else(|| {
            AdmissionDependencyError::new("clientConfig must have either url or service field")
        })?;
        let name = service_ref
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| AdmissionDependencyError::new("Service reference missing name"))?;
        let namespace = service_ref
            .get("namespace")
            .and_then(Value::as_str)
            .ok_or_else(|| AdmissionDependencyError::new("Service reference missing namespace"))?;
        let requested_port = service_ref
            .get("port")
            .and_then(Value::as_u64)
            .map(|port| {
                u16::try_from(port).map_err(|_| {
                    AdmissionDependencyError::new("Service reference port out of range")
                })
            })
            .transpose()?
            .unwrap_or(443);
        let address = self.service_target(namespace, name, requested_port).await?;
        let path = service_ref
            .get("path")
            .and_then(Value::as_str)
            .unwrap_or("");
        let host = format!("{name}.{namespace}.svc");
        Ok(WebhookTarget {
            base_url: format!("https://{host}:{requested_port}{path}"),
            dns_override: Some((host, address)),
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Protocol {
    Tcp,
    Udp,
    Sctp,
}

impl Protocol {
    fn parse(value: Option<&str>) -> Option<Self> {
        match value
            .filter(|value| !value.is_empty())
            .unwrap_or("TCP")
            .to_ascii_uppercase()
            .as_str()
        {
            "TCP" => Some(Self::Tcp),
            "UDP" => Some(Self::Udp),
            "SCTP" => Some(Self::Sctp),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ProjectedPort {
    service_port: u16,
    protocol: Protocol,
    has_ready_endpoint: bool,
}

fn routable_cluster_ip(service: &Value) -> Option<Ipv4Addr> {
    let spec = service.get("spec")?;
    if spec.get("type").and_then(Value::as_str) == Some("ExternalName") {
        return None;
    }
    let cluster_ip = spec.get("clusterIP")?.as_str()?;
    if cluster_ip.is_empty() || cluster_ip == "None" {
        return None;
    }
    cluster_ip.parse().ok()
}

fn parse_port(value: Option<&Value>) -> Option<u16> {
    let port = value?.as_u64()?;
    if port == 0 {
        return None;
    }
    u16::try_from(port).ok()
}

fn service_target_port_number(service_port: &Value) -> Option<u16> {
    parse_port(service_port.get("targetPort"))
        .or_else(|| {
            service_port
                .get("targetPort")
                .and_then(Value::as_str)
                .and_then(|port| port.parse().ok())
        })
        .or_else(|| parse_port(service_port.get("port")))
}

fn matching_service_port<'a>(
    endpoint_port: &Value,
    service_ports: &'a [Value],
) -> Option<(&'a Value, Protocol)> {
    let target_port = parse_port(endpoint_port.get("port"))?;
    let protocol = Protocol::parse(endpoint_port.get("protocol").and_then(Value::as_str))?;
    let endpoint_name = endpoint_port.get("name").and_then(Value::as_str);
    let service_port = service_ports.iter().find(|service_port| {
        if Protocol::parse(service_port.get("protocol").and_then(Value::as_str)) != Some(protocol) {
            return false;
        }
        if let Some(endpoint_name) = endpoint_name
            && (service_port.get("name").and_then(Value::as_str) == Some(endpoint_name)
                || service_port.get("targetPort").and_then(Value::as_str) == Some(endpoint_name))
        {
            return true;
        }
        service_target_port_number(service_port) == Some(target_port)
    })?;
    Some((service_port, protocol))
}

fn projected_port(
    endpoint_port: &Value,
    service_ports: &[Value],
    has_ready_endpoint: bool,
) -> Option<ProjectedPort> {
    let (service_port, protocol) = matching_service_port(endpoint_port, service_ports)?;
    Some(ProjectedPort {
        service_port: parse_port(service_port.get("port"))?,
        protocol,
        has_ready_endpoint,
    })
}

fn endpoint_slice_ports(
    service: &Value,
    slices: &[super::AdmissionResource],
) -> Option<Vec<ProjectedPort>> {
    let service_ports = service.pointer("/spec/ports")?.as_array()?;
    let mut ports = Vec::new();
    for slice in slices {
        let Some(slice_ports) = slice.data.get("ports").and_then(Value::as_array) else {
            continue;
        };
        let Some(endpoints) = slice.data.get("endpoints").and_then(Value::as_array) else {
            continue;
        };
        let has_ready_endpoint = endpoints.iter().any(|endpoint| {
            endpoint
                .pointer("/conditions/ready")
                .and_then(Value::as_bool)
                .unwrap_or(true)
                && endpoint
                    .get("addresses")
                    .and_then(Value::as_array)
                    .and_then(|addresses| addresses.first())
                    .and_then(Value::as_str)
                    .and_then(|address| address.parse::<Ipv4Addr>().ok())
                    .is_some_and(|address| !address.is_unspecified())
        });
        ports.extend(
            slice_ports
                .iter()
                .filter_map(|port| projected_port(port, service_ports, has_ready_endpoint)),
        );
    }
    (!ports.is_empty()).then_some(ports)
}

fn endpoint_ports(service: &Value, endpoints: &Value) -> Option<Vec<ProjectedPort>> {
    let service_ports = service.pointer("/spec/ports")?.as_array()?;
    let subsets = endpoints.get("subsets")?.as_array()?;
    let mut ports = Vec::new();
    for subset in subsets {
        let has_ready_endpoint = subset
            .get("addresses")
            .and_then(Value::as_array)
            .is_some_and(|addresses| {
                addresses.iter().any(|address| {
                    address
                        .get("ip")
                        .and_then(Value::as_str)
                        .and_then(|address| address.parse::<Ipv4Addr>().ok())
                        .is_some_and(|address| !address.is_unspecified())
                })
            });
        if !has_ready_endpoint {
            continue;
        }
        let Some(endpoint_ports) = subset.get("ports").and_then(Value::as_array) else {
            continue;
        };
        ports.extend(
            endpoint_ports
                .iter()
                .filter_map(|port| projected_port(port, service_ports, true)),
        );
    }
    (!ports.is_empty()).then_some(ports)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::{Value, json};

    use super::*;
    use crate::admission::{AdmissionQuery, AdmissionResource};

    #[derive(Default)]
    struct FakeAdmissionQuery {
        resources: Vec<AdmissionResource>,
    }

    impl FakeAdmissionQuery {
        fn new(resources: Vec<Value>) -> Arc<Self> {
            Arc::new(Self {
                resources: resources
                    .into_iter()
                    .map(|data| AdmissionResource {
                        name: data
                            .pointer("/metadata/name")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string(),
                        data: Arc::new(data),
                    })
                    .collect(),
            })
        }
    }

    #[async_trait::async_trait]
    impl AdmissionQuery for FakeAdmissionQuery {
        async fn get_resource(
            &self,
            api_version: &str,
            kind: &str,
            namespace: Option<&str>,
            name: &str,
        ) -> Result<Option<AdmissionResource>, AdmissionDependencyError> {
            Ok(self
                .resources
                .iter()
                .find(|resource| {
                    resource.name == name
                        && resource.data["apiVersion"] == api_version
                        && resource.data["kind"] == kind
                        && resource
                            .data
                            .pointer("/metadata/namespace")
                            .and_then(Value::as_str)
                            == namespace
                })
                .cloned())
        }

        async fn list_resources(
            &self,
            api_version: &str,
            kind: &str,
            namespace: Option<&str>,
            label_selector: Option<&str>,
        ) -> Result<Vec<AdmissionResource>, AdmissionDependencyError> {
            let service_name = label_selector
                .and_then(|selector| selector.strip_prefix("kubernetes.io/service-name="));
            Ok(self
                .resources
                .iter()
                .filter(|resource| {
                    resource.data["apiVersion"] == api_version
                        && resource.data["kind"] == kind
                        && resource
                            .data
                            .pointer("/metadata/namespace")
                            .and_then(Value::as_str)
                            == namespace
                        && service_name.is_none_or(|service_name| {
                            resource
                                .data
                                .pointer("/metadata/labels/kubernetes.io~1service-name")
                                .and_then(Value::as_str)
                                == Some(service_name)
                        })
                })
                .cloned()
                .collect())
        }
    }

    async fn resolve(
        resources: Vec<Value>,
        client_config: &Value,
    ) -> Result<WebhookTarget, AdmissionDependencyError> {
        let query: Arc<dyn AdmissionQuery> = FakeAdmissionQuery::new(resources);
        ServiceWebhookTargetResolver::new(query)
            .resolve(client_config)
            .await
    }

    fn service(namespace: &str, ports: Value) -> Value {
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "webhook-service", "namespace": namespace},
            "spec": {"clusterIP": "10.43.128.100", "ports": ports}
        })
    }

    fn endpoints(namespace: &str, port: Value) -> Value {
        json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {"name": "webhook-service", "namespace": namespace},
            "subsets": [{
                "addresses": [{"ip": "10.42.1.20"}],
                "ports": [port]
            }]
        })
    }

    #[tokio::test]
    async fn test_resolve_webhook_target_from_url_field() {
        let target = resolve(
            Vec::new(),
            &json!({"url": "https://webhook.example.com/validate"}),
        )
        .await
        .unwrap();
        assert_eq!(target.base_url, "https://webhook.example.com/validate");
        assert_eq!(target.dns_override, None);
    }

    #[tokio::test]
    async fn test_resolve_webhook_target_from_service_reference() {
        let target = resolve(
            vec![
                service("cert-manager", json!([{"port": 443}])),
                endpoints("cert-manager", json!({"port": 443})),
            ],
            &json!({
                "service": {
                    "name": "webhook-service",
                    "namespace": "cert-manager",
                    "path": "/validate"
                }
            }),
        )
        .await
        .unwrap();
        assert_eq!(
            target.base_url,
            "https://webhook-service.cert-manager.svc:443/validate"
        );
        assert_eq!(
            target.dns_override,
            Some((
                "webhook-service.cert-manager.svc".to_string(),
                SocketAddr::from((Ipv4Addr::new(10, 43, 128, 100), 443)),
            ))
        );
    }

    #[tokio::test]
    async fn test_resolve_webhook_target_service_with_port_specified() {
        let target = resolve(
            vec![
                service("default", json!([{"port": 8443}, {"port": 9443}])),
                endpoints("default", json!({"port": 9443})),
            ],
            &json!({
                "service": {
                    "name": "webhook-service",
                    "namespace": "default",
                    "port": 9443
                }
            }),
        )
        .await
        .unwrap();
        assert_eq!(target.base_url, "https://webhook-service.default.svc:9443");
    }

    #[tokio::test]
    async fn test_resolve_webhook_target_leaves_target_port_translation_to_service_dataplane() {
        let target = resolve(
            vec![
                service("default", json!([{"name": "https", "port": 443}])),
                endpoints("default", json!({"name": "https", "port": 9443})),
            ],
            &json!({"service": {"name": "webhook-service", "namespace": "default"}}),
        )
        .await
        .unwrap();
        assert_eq!(target.base_url, "https://webhook-service.default.svc:443");
        assert_eq!(
            target.dns_override,
            Some((
                "webhook-service.default.svc".to_string(),
                SocketAddr::from((Ipv4Addr::new(10, 43, 128, 100), 443)),
            ))
        );
    }

    #[tokio::test]
    async fn test_resolve_webhook_target_keeps_remote_endpoint_behind_service_dataplane() {
        let target = resolve(
            vec![
                service(
                    "webhook-7540",
                    json!([{"name": "https", "port": 8443, "targetPort": 8444}]),
                ),
                json!({
                    "apiVersion": "discovery.k8s.io/v1",
                    "kind": "EndpointSlice",
                    "metadata": {
                        "name": "e2e-test-webhook-remote",
                        "namespace": "webhook-7540",
                        "labels": {"kubernetes.io/service-name": "webhook-service"}
                    },
                    "ports": [{"name": "https", "port": 8444, "protocol": "TCP"}],
                    "endpoints": [{
                        "addresses": ["10.42.2.55"],
                        "conditions": {"ready": true},
                        "nodeName": "mn-replica"
                    }]
                }),
            ],
            &json!({
                "service": {
                    "name": "webhook-service",
                    "namespace": "webhook-7540",
                    "path": "/always-allow-delay-5s",
                    "port": 8443
                }
            }),
        )
        .await
        .unwrap();
        assert_eq!(
            target.base_url,
            "https://webhook-service.webhook-7540.svc:8443/always-allow-delay-5s"
        );
        assert_eq!(
            target.dns_override,
            Some((
                "webhook-service.webhook-7540.svc".to_string(),
                SocketAddr::from((Ipv4Addr::new(10, 43, 128, 100), 8443)),
            )),
            "the apiserver must enter the Service dataplane; it must not pin the first remote Pod endpoint"
        );
    }

    #[tokio::test]
    async fn test_resolve_webhook_target_service_not_found_returns_error() {
        let error = resolve(
            Vec::new(),
            &json!({"service": {"name": "nonexistent", "namespace": "default"}}),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("Service not found"));
    }
}
