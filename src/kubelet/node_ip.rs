use crate::control_plane::client::LeaderApiClient;
#[cfg(test)]
use crate::datastore::DatastoreBackend;

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

#[cfg(test)]
pub async fn resolve_node_ip_from_db(db: &dyn DatastoreBackend, node_name: &str) -> Option<String> {
    match db.get_resource("v1", "Node", None, node_name).await {
        Ok(Some(node)) => internal_ip_from_node(&node.data),
        Ok(None) => {
            tracing::debug!(node_name, "node resource not found for InternalIP lookup");
            None
        }
        Err(err) => {
            tracing::debug!(
                node_name,
                error = %err,
                "failed to read node resource for InternalIP lookup"
            );
            None
        }
    }
}

pub async fn resolve_node_ip_from_leader_api(
    cluster_api: &dyn LeaderApiClient,
    node_name: &str,
) -> Option<String> {
    match cluster_api.get_node(node_name).await {
        Ok(node) => internal_ip_from_node(&node.data),
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
    cluster_api: &dyn LeaderApiClient,
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
    async fn resolve_node_ip_from_db_prefers_node_internal_ip_over_hostname() {
        let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
        db.create_resource(
            "v1",
            "Node",
            None,
            "dp",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "dp"},
                "status": {
                    "addresses": [
                        {"type": "Hostname", "address": "dp"},
                        {"type": "InternalIP", "address": "192.168.8.22"}
                    ]
                }
            }),
        )
        .await
        .unwrap();

        let ip = super::resolve_node_ip_from_db(db.as_ref(), "dp")
            .await
            .unwrap();

        assert_eq!(ip, "192.168.8.22");
    }

    #[tokio::test]
    async fn resolve_node_ip_from_leader_api_prefers_node_internal_ip() {
        let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
        db.create_resource(
            "v1",
            "Node",
            None,
            "dp",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "dp"},
                "status": {
                    "addresses": [
                        {"type": "Hostname", "address": "dp"},
                        {"type": "InternalIP", "address": "192.168.8.23"}
                    ]
                }
            }),
        )
        .await
        .unwrap();
        let client = crate::control_plane::client::local::LocalApiClient::new(
            db,
            "dp".to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        );

        let ip = super::resolve_node_ip_from_leader_api(&client, "dp")
            .await
            .unwrap();

        assert_eq!(ip, "192.168.8.23");
    }
}
