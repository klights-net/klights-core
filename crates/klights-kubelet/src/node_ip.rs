use klights_leader_api::LeaderResourceQuery;

fn first_non_loopback_ip_from_iter<I>(iter: I) -> Option<String>
where
    I: IntoIterator<Item = std::net::SocketAddr>,
{
    for addr in iter {
        let ip = addr.ip();
        if !ip.is_loopback() {
            return Some(ip.to_string());
        }
    }
    None
}

fn internal_ip_from_node(node: &serde_json::Value) -> Option<String> {
    node.pointer("/status/addresses")
        .and_then(|v| v.as_array())?
        .iter()
        .find_map(|addr| {
            let ty = addr.get("type").and_then(|v| v.as_str())?;
            if ty != "InternalIP" {
                return None;
            }
            let ip = addr
                .get("address")
                .and_then(|v| v.as_str())?
                .trim()
                .parse::<std::net::IpAddr>()
                .ok()?;
            if ip.is_loopback() {
                return None;
            }
            Some(ip.to_string())
        })
}

pub async fn resolve_node_ip_from_leader_api(
    cluster_api: &dyn LeaderResourceQuery,
    node_name: &str,
) -> Option<String> {
    let request = match klights_leader_api::node_get_request(
        node_name,
        klights_leader_api::ResourceQueryConsistency::Cached,
    ) {
        Ok(request) => request,
        Err(err) => {
            tracing::debug!(node_name, error = %err, "invalid node resource query");
            return None;
        }
    };
    match cluster_api.get_resource(request).await {
        Ok(Some(node)) => internal_ip_from_node(&node.data),
        Ok(None) => {
            tracing::debug!(node_name, "node resource not found for InternalIP lookup");
            None
        }
        Err(err) => {
            tracing::debug!(
                node_name,
                error = %err,
                "failed to read node resource through LeaderApiClient for InternalIP lookup"
            );
            None
        }
    }
}

pub async fn resolve_node_ip_from_leader_api_or_hostname(
    cluster_api: &dyn LeaderResourceQuery,
    node_name: &str,
) -> String {
    if let Some(ip) = resolve_node_ip_from_leader_api(cluster_api, node_name).await {
        return ip;
    }
    resolve_node_ip(node_name).await
}

pub async fn resolve_node_ip(node_name: &str) -> String {
    match tokio::net::lookup_host((node_name, 0)).await {
        Ok(addrs) => {
            if let Some(ip) = first_non_loopback_ip_from_iter(addrs) {
                return ip;
            }
        }
        Err(err) => {
            tracing::debug!(
                node_name = node_name,
                error = %err,
                "node name did not resolve to a usable InternalIP"
            );
        }
    }

    match discover_primary_route_ip().await {
        Ok(ip) => ip,
        Err(err) => {
            tracing::warn!(
                node_name = node_name,
                error = %err,
                "falling back to loopback Node InternalIP"
            );
            "127.0.0.1".to_string()
        }
    }
}

/// Discover the host's primary outgoing IPv4 address via a kernel route lookup.
/// `UdpSocket::connect` only asks the kernel to choose a route; it sends no
/// packet. The chosen local address is what pods must use for NodePort traffic.
pub async fn discover_primary_route_ip() -> anyhow::Result<String> {
    use anyhow::{Context, bail};

    let sock = tokio::net::UdpSocket::bind("0.0.0.0:0")
        .await
        .context("bind UDP socket for node IP discovery")?;
    sock.connect("192.0.2.1:80")
        .await
        .context("connect UDP socket for node IP discovery")?;
    let local = sock
        .local_addr()
        .context("read UDP socket local_addr for node IP discovery")?;
    let ip = local.ip();
    if ip.is_loopback() {
        bail!("primary route resolved to loopback {ip}");
    }
    Ok(ip.to_string())
}

#[cfg(test)]
mod tests {
    use super::first_non_loopback_ip_from_iter;

    struct ExactNodeQuery(klights_cluster_core::Resource);

    impl klights_leader_api::LeaderResourceQuery for ExactNodeQuery {
        fn get_resource(
            &self,
            request: klights_leader_api::ResourceGetRequest,
        ) -> klights_leader_api::ResourceQueryFuture<'_, Option<klights_cluster_core::Resource>>
        {
            Box::pin(async move {
                let key = request.into_key();
                Ok((key.api_version == "v1"
                    && key.kind == "Node"
                    && key.namespace.is_none()
                    && key.name == self.0.name)
                    .then(|| self.0.clone()))
            })
        }

        fn list_resources(
            &self,
            _request: klights_leader_api::ResourceListRequest,
        ) -> klights_leader_api::ResourceQueryFuture<'_, klights_leader_api::ResourceListResult>
        {
            Box::pin(async {
                Err(klights_leader_api::ResourceQueryError::query_failed(
                    "list is not used by node IP resolution",
                ))
            })
        }
    }

    #[test]
    fn picks_first_non_loopback_ip() {
        let addrs = vec![
            "127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap(),
            "[::1]:0".parse::<std::net::SocketAddr>().unwrap(),
            "10.0.0.5:0".parse::<std::net::SocketAddr>().unwrap(),
        ];
        assert_eq!(
            first_non_loopback_ip_from_iter(addrs).as_deref(),
            Some("10.0.0.5")
        );
    }

    #[test]
    fn returns_none_for_all_loopback() {
        let addrs = vec![
            "127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap(),
            "[::1]:0".parse::<std::net::SocketAddr>().unwrap(),
        ];
        assert!(first_non_loopback_ip_from_iter(addrs).is_none());
    }

    #[tokio::test]
    async fn resolve_node_ip_falls_back_to_primary_route_not_loopback() {
        let ip = super::resolve_node_ip("klights-unresolvable-node.invalid").await;
        let parsed = ip.parse::<std::net::IpAddr>().unwrap();

        assert!(
            !parsed.is_loopback(),
            "Node InternalIP must be pod-reachable; resolver returned loopback {ip}"
        );
    }

    #[tokio::test]
    async fn resolve_node_ip_from_leader_api_prefers_node_internal_ip() {
        let client = ExactNodeQuery(klights_cluster_core::Resource {
            id: 1,
            api_version: "v1".to_string(),
            kind: "Node".to_string(),
            namespace: None,
            name: "dp".to_string(),
            uid: "uid-dp".to_string(),
            resource_version: 1,
            data: std::sync::Arc::new(serde_json::json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "dp"},
                "status": {
                    "addresses": [
                        {"type": "Hostname", "address": "dp"},
                        {"type": "InternalIP", "address": "192.168.8.23"}
                    ]
                }
            })),
        });

        let ip = super::resolve_node_ip_from_leader_api(&client, "dp")
            .await
            .unwrap();

        assert_eq!(ip, "192.168.8.23");
    }
}
