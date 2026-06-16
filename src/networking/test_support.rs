use crate::networking::cni::PodNetwork;
use crate::networking::pod_endpoint_resolver::{Endpoint, EndpointEvent, PodEndpointResolver};
use crate::networking::types::NodeEndpoint;
use async_trait::async_trait;
use futures::stream::BoxStream;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug)]
pub enum NetworkCall {
    CniAdd {
        sandbox_id: String,
        namespace: String,
        pod_name: String,
        pod_uid: String,
    },
    CniDel {
        sandbox_id: String,
    },
    ApplyWireGuardPeerEndpoint {
        node_name: String,
        endpoint: String,
        allowed_pod_cidr: String,
    },
    RemoveWireGuardPeerEndpoint {
        node_name: String,
        public_key: String,
        allowed_pod_cidr: String,
    },
    ApplyUnencryptedPeerEndpoint {
        node_name: String,
        node_ip: String,
        allowed_pod_cidr: String,
    },
    RemoveUnencryptedPeerEndpoint {
        node_name: String,
        node_ip: String,
        allowed_pod_cidr: String,
    },
    /// F2-04: rootless peer apply/remove. The mock records the underlay node IP
    /// plus hostport range string so dispatch tests can assert the correct
    /// endpoint variant was reached without inspecting opaque trait state.
    ApplyRootlessPeerEndpoint {
        node_ip: String,
        hostport_range: String,
    },
    RemoveRootlessPeerEndpoint {
        node_ip: String,
        hostport_range: String,
    },
    Shutdown,
}

pub struct MockNetworkProvider {
    calls: Arc<Mutex<Vec<NetworkCall>>>,
    pod_ip: Arc<Mutex<Ipv4Addr>>,
    host_ip: Arc<Mutex<Ipv4Addr>>,
    pod_gateway_ip: Arc<Mutex<Ipv4Addr>>,
}

impl MockNetworkProvider {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_pod_ip(pod_ip: Ipv4Addr) -> Self {
        Self {
            calls: Arc::new(Mutex::new(Vec::new())),
            pod_ip: Arc::new(Mutex::new(pod_ip)),
            host_ip: Arc::new(Mutex::new(Ipv4Addr::new(127, 0, 0, 1))),
            pod_gateway_ip: Arc::new(Mutex::new(Ipv4Addr::new(10, 43, 0, 1))),
        }
    }

    pub fn set_host_ip(&self, host_ip: Ipv4Addr) {
        *self.host_ip.lock().expect("network calls mutex poisoned") = host_ip;
    }

    pub fn set_pod_gateway_ip(&self, pod_gateway_ip: Ipv4Addr) {
        *self
            .pod_gateway_ip
            .lock()
            .expect("pod_gateway_ip mutex poisoned") = pod_gateway_ip;
    }

    pub fn calls(&self) -> Vec<NetworkCall> {
        self.calls
            .lock()
            .expect("network calls mutex poisoned")
            .clone()
    }

    pub fn clear_calls(&self) {
        self.calls
            .lock()
            .expect("network calls mutex poisoned")
            .clear();
    }

    pub fn set_pod_ip(&self, pod_ip: Ipv4Addr) {
        *self.pod_ip.lock().expect("network calls mutex poisoned") = pod_ip;
    }

    fn pod_network_ip(&self) -> std::sync::MutexGuard<'_, Ipv4Addr> {
        self.pod_ip.lock().expect("network calls mutex poisoned")
    }
}

#[async_trait]
impl crate::networking::datapath::Datapath for MockNetworkProvider {
    async fn cni_add(
        &self,
        request: crate::networking::provider::CniAddRequest,
    ) -> anyhow::Result<PodNetwork> {
        self.calls
            .lock()
            .expect("network calls mutex poisoned")
            .push(NetworkCall::CniAdd {
                sandbox_id: request.sandbox_id,
                namespace: request.namespace,
                pod_name: request.pod_name,
                pod_uid: request.pod_uid,
            });
        Ok(PodNetwork {
            ip_addr: IpAddr::V4(*self.pod_network_ip()),
        })
    }

    async fn cni_del(&self, sandbox_id: &str) -> anyhow::Result<()> {
        self.calls
            .lock()
            .expect("network calls mutex poisoned")
            .push(NetworkCall::CniDel {
                sandbox_id: sandbox_id.to_string(),
            });
        Ok(())
    }

    async fn host_ip(&self) -> anyhow::Result<std::net::IpAddr> {
        Ok(std::net::IpAddr::V4(
            *self.host_ip.lock().expect("host_ip mutex poisoned"),
        ))
    }

    async fn pod_gateway_ip(&self) -> anyhow::Result<std::net::IpAddr> {
        Ok(std::net::IpAddr::V4(
            *self
                .pod_gateway_ip
                .lock()
                .expect("pod_gateway_ip mutex poisoned"),
        ))
    }

    async fn shutdown(&self) -> anyhow::Result<()> {
        self.calls
            .lock()
            .expect("network calls mutex poisoned")
            .push(NetworkCall::Shutdown);
        Ok(())
    }
}

#[async_trait]
impl crate::networking::peer_router::PeerRouter for MockNetworkProvider {
    async fn apply_peer_endpoint(&self, peer: &NodeEndpoint) -> anyhow::Result<()> {
        match peer {
            NodeEndpoint::WireGuard(plan) => {
                self.calls
                    .lock()
                    .expect("network calls mutex poisoned")
                    .push(NetworkCall::ApplyWireGuardPeerEndpoint {
                        node_name: plan.node_name.clone(),
                        endpoint: plan.endpoint.to_string(),
                        allowed_pod_cidr: plan.allowed_pod_cidr.clone(),
                    });
            }
            NodeEndpoint::UnencryptedDirect(plan) => {
                self.calls
                    .lock()
                    .expect("network calls mutex poisoned")
                    .push(NetworkCall::ApplyUnencryptedPeerEndpoint {
                        node_name: plan.node_name.clone(),
                        node_ip: plan.endpoint.to_string(),
                        allowed_pod_cidr: plan.allowed_pod_cidr.clone(),
                    });
            }
            NodeEndpoint::Rootless {
                node_ip,
                hostport_range,
            } => {
                self.calls
                    .lock()
                    .expect("network calls mutex poisoned")
                    .push(NetworkCall::ApplyRootlessPeerEndpoint {
                        node_ip: node_ip.to_string(),
                        hostport_range: hostport_range.to_string(),
                    });
            }
        }
        Ok(())
    }

    async fn remove_peer_endpoint(&self, peer: &NodeEndpoint) -> anyhow::Result<()> {
        match peer {
            NodeEndpoint::WireGuard(plan) => {
                self.calls
                    .lock()
                    .expect("network calls mutex poisoned")
                    .push(NetworkCall::RemoveWireGuardPeerEndpoint {
                        node_name: plan.node_name.clone(),
                        public_key: plan.public_key.to_string(),
                        allowed_pod_cidr: plan.allowed_pod_cidr.clone(),
                    });
            }
            NodeEndpoint::UnencryptedDirect(plan) => {
                self.calls
                    .lock()
                    .expect("network calls mutex poisoned")
                    .push(NetworkCall::RemoveUnencryptedPeerEndpoint {
                        node_name: plan.node_name.clone(),
                        node_ip: plan.endpoint.to_string(),
                        allowed_pod_cidr: plan.allowed_pod_cidr.clone(),
                    });
            }
            NodeEndpoint::Rootless {
                node_ip,
                hostport_range,
            } => {
                self.calls
                    .lock()
                    .expect("network calls mutex poisoned")
                    .push(NetworkCall::RemoveRootlessPeerEndpoint {
                        node_ip: node_ip.to_string(),
                        hostport_range: hostport_range.to_string(),
                    });
            }
        }
        Ok(())
    }
}

impl Default for MockNetworkProvider {
    fn default() -> Self {
        Self {
            calls: Arc::new(Mutex::new(Vec::new())),
            pod_ip: Arc::new(Mutex::new(Ipv4Addr::UNSPECIFIED)),
            // Loopback default — tests that assert "host IP not 0.0.0.0"
            // (e.g. kubernetes Endpoints bootstrap) want a usable value.
            host_ip: Arc::new(Mutex::new(Ipv4Addr::new(127, 0, 0, 1))),
            pod_gateway_ip: Arc::new(Mutex::new(Ipv4Addr::new(10, 43, 0, 1))),
        }
    }
}

/// Test-only `ServiceRouter` impl. Records each call so tests can
/// assert side-effect dispatch behaviour; never touches netlink.
pub struct MockServiceRouter {
    sync_count: std::sync::atomic::AtomicUsize,
    sync_now_count: std::sync::atomic::AtomicUsize,
    add_hostport_count: std::sync::atomic::AtomicUsize,
    remove_hostport_count: std::sync::atomic::AtomicUsize,
    cleanup_count: std::sync::atomic::AtomicUsize,
}

pub struct MockPodEndpointResolver;

#[async_trait]
impl PodEndpointResolver for MockPodEndpointResolver {
    async fn resolve(&self, _pod_ip: Ipv4Addr) -> anyhow::Result<Option<Endpoint>> {
        Ok(None)
    }

    fn watch(&self) -> BoxStream<'static, EndpointEvent> {
        Box::pin(futures::stream::empty())
    }
}

impl MockServiceRouter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn sync_count(&self) -> usize {
        self.sync_count.load(std::sync::atomic::Ordering::Relaxed)
    }
    pub fn sync_now_count(&self) -> usize {
        self.sync_now_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }
    pub fn add_hostport_count(&self) -> usize {
        self.add_hostport_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }
    pub fn remove_hostport_count(&self) -> usize {
        self.remove_hostport_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }
    pub fn cleanup_count(&self) -> usize {
        self.cleanup_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl Default for MockServiceRouter {
    fn default() -> Self {
        Self {
            sync_count: std::sync::atomic::AtomicUsize::new(0),
            sync_now_count: std::sync::atomic::AtomicUsize::new(0),
            add_hostport_count: std::sync::atomic::AtomicUsize::new(0),
            remove_hostport_count: std::sync::atomic::AtomicUsize::new(0),
            cleanup_count: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

/// Convenience: build a `Network` populated with mocks. Matches the
/// shape every test fixture used to wire `network: Arc<dyn ...>` and
/// `services: Arc<dyn ...>` separately, so the post-Task-7 AppState
/// fixture stays one line.
pub fn mock_network(
    _db: crate::datastore::DatastoreHandle,
) -> std::sync::Arc<crate::networking::Network> {
    let provider = Arc::new(MockNetworkProvider::new());
    std::sync::Arc::new(crate::networking::Network {
        datapath: provider.clone(),
        peering: provider,
        services: Arc::new(MockServiceRouter::new()),
        resolver: Arc::new(MockPodEndpointResolver),
    })
}

#[async_trait]
impl crate::networking::ServiceRouter for MockServiceRouter {
    fn request_services_sync(&self) {
        self.sync_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    async fn sync_services_now(&self) -> anyhow::Result<()> {
        self.sync_now_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
    async fn add_hostport_rules(
        &self,
        _pod: &serde_json::Value,
        _pod_ip: Ipv4Addr,
    ) -> anyhow::Result<()> {
        self.add_hostport_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
    async fn remove_hostport_rules(&self, _pod: &serde_json::Value) -> anyhow::Result<()> {
        self.remove_hostport_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
    async fn cleanup(&self) -> anyhow::Result<()> {
        self.cleanup_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
}

#[cfg(test)]
mod peer_endpoint_tests {
    use super::*;

    #[test]
    fn test_node_endpoint_is_non_exhaustive() {
        // Guards the `#[non_exhaustive]` shape: in-crate matches must
        // cover every known variant; external matches need a wildcard.
        let endpoint = NodeEndpoint::Rootless {
            node_ip: IpAddr::V4(Ipv4Addr::new(192, 168, 1, 5)),
            hostport_range: crate::networking::types::HostPortRange {
                start: 31000,
                end: 31999,
            },
        };
        match &endpoint {
            NodeEndpoint::WireGuard(_) | NodeEndpoint::UnencryptedDirect(_) => {
                panic!("constructed Rootless, not direct endpoint")
            }
            NodeEndpoint::Rootless { .. } => {}
        }
    }

    // ---------- trait-split tests (Task 4) ----------

    /// Compile-time check: `MockNetworkProvider` satisfies `Datapath`. If
    /// the trait split regresses (Datapath demands a method NetworkPlane /
    /// the mock can't provide), this fails at build time.
    #[test]
    fn test_network_plane_implements_datapath() {
        fn assert_impl<T: crate::networking::Datapath>() {}
        assert_impl::<MockNetworkProvider>();
        // NetworkPlane impls Datapath via the same trait surface — verify
        // by erasing through `dyn`.
        let _erase = |p: std::sync::Arc<crate::networking::plane::NetworkPlane>| {
            let _: std::sync::Arc<dyn crate::networking::Datapath> = p;
        };
    }

    /// Compile-time check: `MockNetworkProvider` satisfies `PeerRouter`.
    #[test]
    fn test_network_plane_implements_peer_router() {
        fn assert_impl<T: crate::networking::PeerRouter>() {}
        assert_impl::<MockNetworkProvider>();
        let _erase = |p: std::sync::Arc<crate::networking::plane::NetworkPlane>| {
            let _: std::sync::Arc<dyn crate::networking::PeerRouter> = p;
        };
    }

    /// Compile-time check: a kubelet-style caller takes only `&dyn Datapath`
    /// and never reaches peer-router methods. The test compiles iff the
    /// signature shape holds.
    #[test]
    fn test_kubelet_caller_takes_only_datapath() {
        fn kubelet_call(_dp: &dyn crate::networking::Datapath) {}
        let mock = MockNetworkProvider::new();
        kubelet_call(&mock);
    }

    /// Compile-time check: a node_subnet-style caller takes only
    /// `&dyn PeerRouter`.
    #[test]
    fn test_node_subnet_caller_takes_only_peer_router() {
        fn controller_call(_pr: &dyn crate::networking::PeerRouter) {}
        let mock = MockNetworkProvider::new();
        controller_call(&mock);
    }

    // ---------- Datapath::host_ip tests (Task 8) ----------

    /// Datapath::host_ip returns the configured value via the mock —
    /// in the production NetworkPlane it returns the cached field set
    /// at boot from config / discovery.
    #[tokio::test]
    async fn test_datapath_host_ip_returns_configured_node_ip() {
        use crate::networking::Datapath;
        let mock = MockNetworkProvider::new();
        mock.set_host_ip(std::net::Ipv4Addr::new(192, 168, 7, 42));
        let ip = Datapath::host_ip(&mock).await.unwrap();
        assert_eq!(
            ip,
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(192, 168, 7, 42))
        );
    }

    /// Datapath::host_ip is a no-I/O field load — calling it does not
    /// record any NetworkCall (no shell-out, no rtnetlink call).
    #[tokio::test]
    async fn test_datapath_host_ip_no_shell_command_invoked() {
        use crate::networking::Datapath;
        let mock = MockNetworkProvider::new();
        mock.clear_calls();
        let _ = Datapath::host_ip(&mock).await.unwrap();
        let calls = mock.calls();
        assert!(
            calls.is_empty(),
            "host_ip must be a no-I/O field load; recorded calls: {calls:?}"
        );
    }
}
