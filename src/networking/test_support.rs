use std::net::{IpAddr, Ipv4Addr};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

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
        public_key: [u8; 32],
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
    Shutdown,
}

pub struct MockNetworkProvider {
    calls: Arc<Mutex<Vec<NetworkCall>>>,
    pod_ip: Arc<Mutex<Ipv4Addr>>,
    host_ip: Arc<Mutex<Ipv4Addr>>,
    pod_gateway_ip: Arc<Mutex<Ipv4Addr>>,
    peer_apply_failures: Arc<std::sync::atomic::AtomicUsize>,
    peer_apply_successes: Arc<std::sync::atomic::AtomicUsize>,
    peer_apply_notify: Arc<tokio::sync::Notify>,
    peer_remove_failures: Arc<std::sync::atomic::AtomicUsize>,
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
            peer_apply_failures: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            peer_apply_successes: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            peer_apply_notify: Arc::new(tokio::sync::Notify::new()),
            peer_remove_failures: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
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

    pub fn fail_next_peer_remove(&self) {
        self.peer_remove_failures
            .fetch_add(1, std::sync::atomic::Ordering::Release);
    }

    pub fn fail_next_peer_apply(&self) {
        self.peer_apply_failures
            .fetch_add(1, std::sync::atomic::Ordering::Release);
    }

    pub fn peer_apply_call_count(&self) -> usize {
        self.calls()
            .iter()
            .filter(|call| {
                matches!(
                    call,
                    NetworkCall::ApplyWireGuardPeerEndpoint { .. }
                        | NetworkCall::ApplyUnencryptedPeerEndpoint { .. }
                )
            })
            .count()
    }

    pub async fn wait_for_peer_apply_calls(&self, expected: usize) {
        loop {
            let notified = self.peer_apply_notify.notified();
            if self.peer_apply_call_count() >= expected {
                return;
            }
            notified.await;
        }
    }

    pub async fn wait_for_peer_apply_successes(&self, expected: usize) {
        loop {
            let notified = self.peer_apply_notify.notified();
            if self
                .peer_apply_successes
                .load(std::sync::atomic::Ordering::Acquire)
                >= expected
            {
                return;
            }
            notified.await;
        }
    }

    fn pod_network_ip(&self) -> std::sync::MutexGuard<'_, Ipv4Addr> {
        self.pod_ip.lock().expect("network calls mutex poisoned")
    }
}

impl klights_network_api::Datapath for MockNetworkProvider {
    fn cni_add(
        &self,
        request: klights_network_api::CniAddRequest,
    ) -> klights_network_api::DatapathFuture<'_, klights_network_api::PodNetwork> {
        Box::pin(async move {
            let (sandbox_id, pod, _, _, _) = request.into_parts();
            self.calls
                .lock()
                .expect("network calls mutex poisoned")
                .push(NetworkCall::CniAdd {
                    sandbox_id: sandbox_id.to_string(),
                    namespace: pod.namespace,
                    pod_name: pod.name,
                    pod_uid: pod.uid,
                });
            Ok(klights_network_api::PodNetwork::new(IpAddr::V4(
                *self.pod_network_ip(),
            )))
        })
    }

    fn cni_del<'a>(
        &'a self,
        sandbox_id: &'a klights_network_api::SandboxId,
    ) -> klights_network_api::DatapathFuture<'a, ()> {
        Box::pin(async move {
            self.calls
                .lock()
                .expect("network calls mutex poisoned")
                .push(NetworkCall::CniDel {
                    sandbox_id: sandbox_id.to_string(),
                });
            Ok(())
        })
    }

    fn host_ip(&self) -> klights_network_api::DatapathFuture<'_, std::net::IpAddr> {
        Box::pin(async move {
            Ok(std::net::IpAddr::V4(
                *self.host_ip.lock().expect("host_ip mutex poisoned"),
            ))
        })
    }

    fn pod_gateway_ip(&self) -> klights_network_api::DatapathFuture<'_, std::net::IpAddr> {
        Box::pin(async move {
            Ok(std::net::IpAddr::V4(
                *self
                    .pod_gateway_ip
                    .lock()
                    .expect("pod_gateway_ip mutex poisoned"),
            ))
        })
    }

    fn shutdown(&self) -> klights_network_api::DatapathFuture<'_, ()> {
        Box::pin(async move {
            self.calls
                .lock()
                .expect("network calls mutex poisoned")
                .push(NetworkCall::Shutdown);
            Ok(())
        })
    }
}

impl klights_network_api::PeerRouter for MockNetworkProvider {
    fn apply_peer_route<'a>(
        &'a self,
        route: &'a klights_network_api::PeerRoute,
    ) -> klights_network_api::PeerRouterFuture<'a> {
        Box::pin(async move {
            match route {
                klights_network_api::PeerRoute::WireGuard(route) => {
                    self.calls
                        .lock()
                        .expect("network calls mutex poisoned")
                        .push(NetworkCall::ApplyWireGuardPeerEndpoint {
                            node_name: route.node_name().to_string(),
                            endpoint: route.endpoint().to_string(),
                            allowed_pod_cidr: route.allowed_pod_cidr().to_string(),
                        });
                }
                klights_network_api::PeerRoute::Direct(route) => {
                    self.calls
                        .lock()
                        .expect("network calls mutex poisoned")
                        .push(NetworkCall::ApplyUnencryptedPeerEndpoint {
                            node_name: route.node_name().to_string(),
                            node_ip: route.gateway().to_string(),
                            allowed_pod_cidr: route.allowed_pod_cidr().to_string(),
                        });
                }
            }
            if self
                .peer_apply_failures
                .fetch_update(
                    std::sync::atomic::Ordering::AcqRel,
                    std::sync::atomic::Ordering::Acquire,
                    |remaining| remaining.checked_sub(1),
                )
                .is_ok()
            {
                self.peer_apply_notify.notify_waiters();
                return Err(klights_network_api::PeerRouterError::apply(
                    "injected peer apply failure",
                ));
            }
            self.peer_apply_successes
                .fetch_add(1, std::sync::atomic::Ordering::Release);
            self.peer_apply_notify.notify_waiters();
            Ok(())
        })
    }

    fn remove_peer_route<'a>(
        &'a self,
        route: &'a klights_network_api::PeerRoute,
    ) -> klights_network_api::PeerRouterFuture<'a> {
        Box::pin(async move {
            match route {
                klights_network_api::PeerRoute::WireGuard(route) => {
                    self.calls
                        .lock()
                        .expect("network calls mutex poisoned")
                        .push(NetworkCall::RemoveWireGuardPeerEndpoint {
                            node_name: route.node_name().to_string(),
                            public_key: *route.public_key().as_bytes(),
                            allowed_pod_cidr: route.allowed_pod_cidr().to_string(),
                        });
                }
                klights_network_api::PeerRoute::Direct(route) => {
                    self.calls
                        .lock()
                        .expect("network calls mutex poisoned")
                        .push(NetworkCall::RemoveUnencryptedPeerEndpoint {
                            node_name: route.node_name().to_string(),
                            node_ip: route.gateway().to_string(),
                            allowed_pod_cidr: route.allowed_pod_cidr().to_string(),
                        });
                }
            }
            if self
                .peer_remove_failures
                .fetch_update(
                    std::sync::atomic::Ordering::AcqRel,
                    std::sync::atomic::Ordering::Acquire,
                    |remaining| remaining.checked_sub(1),
                )
                .is_ok()
            {
                return Err(klights_network_api::PeerRouterError::remove(
                    "injected peer removal failure",
                ));
            }
            Ok(())
        })
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
            peer_apply_failures: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            peer_apply_successes: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            peer_apply_notify: Arc::new(tokio::sync::Notify::new()),
            peer_remove_failures: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
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

impl klights_network_api::PodEndpointResolver for MockPodEndpointResolver {
    fn resolve(
        &self,
        _pod_ip: Ipv4Addr,
    ) -> klights_network_api::PodEndpointFuture<'_, Option<klights_network_api::PodEndpoint>> {
        Box::pin(async { Ok(None) })
    }
}

impl klights_network_api::PodEndpointEventSource for MockPodEndpointResolver {
    fn subscribe(
        &self,
    ) -> klights_network_api::PodEndpointFuture<'_, klights_network_api::PodEndpointEventStream>
    {
        Box::pin(async {
            Ok(Box::pin(EmptyPodEndpointSubscription { initial: true })
                as klights_network_api::PodEndpointEventStream)
        })
    }
}

struct EmptyPodEndpointSubscription {
    initial: bool,
}

impl klights_network_api::PodEndpointEventSubscription for EmptyPodEndpointSubscription {
    fn poll_next(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<
        Option<
            Result<klights_network_api::PodEndpointEvent, klights_network_api::PodEndpointError>,
        >,
    > {
        if std::mem::take(&mut self.initial) {
            Poll::Ready(Some(Ok(klights_network_api::PodEndpointEvent::Resync(
                Vec::new(),
            ))))
        } else {
            Poll::Ready(None)
        }
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
    std::sync::Arc::new(crate::networking::Network::new(
        provider.clone(),
        provider,
        Arc::new(MockServiceRouter::new()),
        Arc::new(MockPodEndpointResolver),
    ))
}

impl klights_network_api::ServiceRouter for MockServiceRouter {
    fn request_services_sync(&self) -> Result<(), klights_network_api::ServiceRouterError> {
        self.sync_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
    fn sync_services_now(&self) -> klights_network_api::ServiceRouterFuture<'_> {
        Box::pin(async move {
            self.sync_now_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        })
    }
    fn add_hostport_rules(
        &self,
        _request: klights_network_api::HostPortRules,
    ) -> klights_network_api::ServiceRouterFuture<'_> {
        Box::pin(async move {
            self.add_hostport_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        })
    }
    fn remove_hostport_rules(
        &self,
        _request: klights_network_api::HostPortRemoval,
    ) -> klights_network_api::ServiceRouterFuture<'_> {
        Box::pin(async move {
            self.remove_hostport_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        })
    }
    fn cleanup(&self) -> klights_network_api::ServiceRouterFuture<'_> {
        Box::pin(async move {
            self.cleanup_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        })
    }
}

impl klights_reconcile_api::ServiceRoutingSync for MockServiceRouter {
    fn request_service_routing_sync(
        &self,
    ) -> Result<(), klights_reconcile_api::ReconcileSinkError> {
        klights_network_api::ServiceRouter::request_services_sync(self).map_err(|error| {
            klights_reconcile_api::ReconcileSinkError::unavailable(error.to_string())
        })
    }
}

#[cfg(test)]
mod peer_endpoint_tests {
    use super::*;

    // ---------- trait-split tests (Task 4) ----------

    /// Compile-time check: `MockNetworkProvider` satisfies `Datapath`. If
    /// the trait split regresses (Datapath demands a method NetworkPlane /
    /// the mock can't provide), this fails at build time.
    #[test]
    fn test_network_plane_implements_datapath() {
        fn assert_impl<T: klights_network_api::Datapath>() {}
        assert_impl::<MockNetworkProvider>();
        // NetworkPlane impls Datapath via the same trait surface — verify
        // by erasing through `dyn`.
        let _erase = |p: std::sync::Arc<crate::networking::plane::NetworkPlane>| {
            let _: std::sync::Arc<dyn klights_network_api::Datapath> = p;
        };
    }

    /// Compile-time check: `MockNetworkProvider` satisfies `PeerRouter`.
    #[test]
    fn test_network_plane_implements_peer_router() {
        fn assert_impl<T: klights_network_api::PeerRouter>() {}
        assert_impl::<MockNetworkProvider>();
        let _erase = |p: std::sync::Arc<crate::networking::plane::NetworkPlane>| {
            let _: std::sync::Arc<dyn klights_network_api::PeerRouter> = p;
        };
    }

    /// Compile-time check: a kubelet-style caller takes only `&dyn Datapath`
    /// and never reaches peer-router methods. The test compiles iff the
    /// signature shape holds.
    #[test]
    fn test_kubelet_caller_takes_only_datapath() {
        fn kubelet_call(_dp: &dyn klights_network_api::Datapath) {}
        let mock = MockNetworkProvider::new();
        kubelet_call(&mock);
    }

    /// Compile-time check: a node_subnet-style caller takes only
    /// `&dyn PeerRouter`.
    #[test]
    fn test_node_subnet_caller_takes_only_peer_router() {
        fn controller_call(_pr: &dyn klights_network_api::PeerRouter) {}
        let mock = MockNetworkProvider::new();
        controller_call(&mock);
    }

    // ---------- Datapath::host_ip tests (Task 8) ----------

    /// Datapath::host_ip returns the configured value via the mock —
    /// in the production NetworkPlane it returns the cached field set
    /// at boot from config / discovery.
    #[tokio::test]
    async fn test_datapath_host_ip_returns_configured_node_ip() {
        use klights_network_api::Datapath;
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
        use klights_network_api::Datapath;
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
