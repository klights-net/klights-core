use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::sync::Mutex;
use std::task::{Context, Poll, Waker};

use klights_network_api::{
    CniAddRequest, Datapath, DatapathError, DatapathFuture, DirectPeerRoute, DirectPodEndpoint,
    HostPortBinding, HostPortPodEndpoint, HostPortProtocol, HostPortRemoval, HostPortRules,
    NetworkNamespacePath, PeerPodCidr, PeerRoute, PeerRouter, PeerRouterError, PeerRouterFuture,
    PodEndpoint, PodEndpointError, PodEndpointEvent, PodEndpointEventSource,
    PodEndpointEventStream, PodEndpointEventSubscription, PodEndpointFuture, PodEndpointResolver,
    PodEndpointTopology, PodHostPorts, PodNetwork, SandboxId, ServiceRouter, ServiceRouterError,
    ServiceRouterFuture, WireGuardPeerKey, WireGuardPeerRoute,
};
use klights_types::PodIdentity;

#[derive(Default)]
struct RecordingDatapath;

impl Datapath for RecordingDatapath {
    fn cni_add(&self, request: CniAddRequest) -> DatapathFuture<'_, PodNetwork> {
        Box::pin(async move {
            assert_eq!(request.sandbox_id().as_str(), "sandbox-a");
            Ok(PodNetwork::new(IpAddr::V4(Ipv4Addr::new(10, 42, 0, 9))))
        })
    }

    fn cni_del<'a>(&'a self, sandbox_id: &'a SandboxId) -> DatapathFuture<'a, ()> {
        Box::pin(async move {
            assert_eq!(sandbox_id.as_str(), "sandbox-a");
            Ok(())
        })
    }

    fn host_ip(&self) -> DatapathFuture<'_, IpAddr> {
        Box::pin(async { Ok(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 10))) })
    }

    fn pod_gateway_ip(&self) -> DatapathFuture<'_, IpAddr> {
        Box::pin(async { Ok(IpAddr::V4(Ipv4Addr::new(10, 42, 0, 1))) })
    }

    fn shutdown(&self) -> DatapathFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

fn poll_ready<T>(mut future: impl Future<Output = T> + Unpin) -> T {
    match Future::poll(
        std::pin::Pin::new(&mut future),
        &mut Context::from_waker(Waker::noop()),
    ) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("contract-test future unexpectedly suspended"),
    }
}

fn valid_request() -> CniAddRequest {
    CniAddRequest::try_new(
        "sandbox-a",
        PodIdentity::new("default", "web", "uid-web"),
        "/proc/self/fd/7",
        "/run/netns/cni-web",
        false,
    )
    .expect("valid CNI ADD request")
}

fn assert_object_safe(_: &dyn Datapath) {}

#[test]
fn datapath_is_object_safe_and_contract_values_are_send_sync() {
    assert_object_safe(&RecordingDatapath);

    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<SandboxId>();
    assert_send_sync::<NetworkNamespacePath>();
    assert_send_sync::<CniAddRequest>();
    assert_send_sync::<PodNetwork>();
    assert_send_sync::<DatapathError>();
}

#[test]
fn request_validates_every_identity_and_path_at_construction() {
    for (field, result) in [
        (
            "datapath.sandbox_id",
            CniAddRequest::try_new(
                "",
                PodIdentity::new("default", "web", "uid-web"),
                "/proc/self/fd/7",
                "/run/netns/cni-web",
                false,
            ),
        ),
        (
            "datapath.pod.namespace",
            CniAddRequest::try_new(
                "sandbox-a",
                PodIdentity::new("", "web", "uid-web"),
                "/proc/self/fd/7",
                "/run/netns/cni-web",
                false,
            ),
        ),
        (
            "datapath.pod.name",
            CniAddRequest::try_new(
                "sandbox-a",
                PodIdentity::new("default", "", "uid-web"),
                "/proc/self/fd/7",
                "/run/netns/cni-web",
                false,
            ),
        ),
        (
            "datapath.pod.uid",
            CniAddRequest::try_new(
                "sandbox-a",
                PodIdentity::new("default", "web", ""),
                "/proc/self/fd/7",
                "/run/netns/cni-web",
                false,
            ),
        ),
        (
            "datapath.netns_setns_path",
            CniAddRequest::try_new(
                "sandbox-a",
                PodIdentity::new("default", "web", "uid-web"),
                "",
                "/run/netns/cni-web",
                false,
            ),
        ),
        (
            "datapath.netns_record_path",
            CniAddRequest::try_new(
                "sandbox-a",
                PodIdentity::new("default", "web", "uid-web"),
                "/proc/self/fd/7",
                "",
                false,
            ),
        ),
    ] {
        assert!(matches!(
            result,
            Err(DatapathError::InvalidRequest { field: actual, .. }) if actual == field
        ));
    }
}

#[test]
fn request_and_result_preserve_validated_runtime_values() {
    let request = valid_request();
    assert_eq!(request.sandbox_id().as_str(), "sandbox-a");
    assert_eq!(
        request.pod(),
        &PodIdentity::new("default", "web", "uid-web")
    );
    assert_eq!(request.netns_setns_path().as_str(), "/proc/self/fd/7");
    assert_eq!(request.netns_record_path().as_str(), "/run/netns/cni-web");
    assert!(!request.host_network());

    let parts = request.into_parts();
    assert_eq!(parts.0.as_str(), "sandbox-a");
    assert_eq!(parts.1, PodIdentity::new("default", "web", "uid-web"));
    assert_eq!(parts.2.as_str(), "/proc/self/fd/7");
    assert_eq!(parts.3.as_str(), "/run/netns/cni-web");
    assert!(!parts.4);

    let network = poll_ready(RecordingDatapath.cni_add(valid_request())).unwrap();
    assert_eq!(network.ip_addr(), IpAddr::V4(Ipv4Addr::new(10, 42, 0, 9)));
}

#[test]
fn operational_errors_preserve_datapath_failure_categories() {
    for (error, expected) in [
        (DatapathError::setup("bridge missing"), "bridge missing"),
        (
            DatapathError::teardown("veth cleanup failed"),
            "veth cleanup failed",
        ),
        (
            DatapathError::address("host IP unavailable"),
            "host IP unavailable",
        ),
        (
            DatapathError::shutdown("netlink shutdown failed"),
            "netlink shutdown failed",
        ),
    ] {
        assert_eq!(error.to_string(), expected);
    }
}

#[derive(Default)]
struct RecordingPeerRouter {
    calls: Mutex<Vec<(bool, PeerRoute)>>,
}

impl PeerRouter for RecordingPeerRouter {
    fn apply_peer_route<'a>(&'a self, route: &'a PeerRoute) -> PeerRouterFuture<'a> {
        Box::pin(async move {
            self.calls
                .lock()
                .expect("peer calls mutex poisoned")
                .push((true, route.clone()));
            Ok(())
        })
    }

    fn remove_peer_route<'a>(&'a self, route: &'a PeerRoute) -> PeerRouterFuture<'a> {
        Box::pin(async move {
            self.calls
                .lock()
                .expect("peer calls mutex poisoned")
                .push((false, route.clone()));
            Ok(())
        })
    }
}

fn wireguard_route() -> PeerRoute {
    PeerRoute::WireGuard(
        WireGuardPeerRoute::try_new(
            "node-b",
            WireGuardPeerKey::new([7; 32]),
            "192.0.2.20:7679".parse::<SocketAddr>().unwrap(),
            "10.42.7.99/24",
        )
        .expect("valid WireGuard peer route"),
    )
}

fn assert_peer_router_object_safe(_: &dyn PeerRouter) {}

#[test]
fn peer_router_is_object_safe_and_contract_values_are_send_sync() {
    assert_peer_router_object_safe(&RecordingPeerRouter::default());

    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<PeerPodCidr>();
    assert_send_sync::<WireGuardPeerKey>();
    assert_send_sync::<WireGuardPeerRoute>();
    assert_send_sync::<DirectPeerRoute>();
    assert_send_sync::<PeerRoute>();
    assert_send_sync::<PeerRouterError>();
}

#[test]
fn peer_routes_validate_and_normalize_runtime_facts() {
    for (field, result) in [
        (
            "peer.node_name",
            WireGuardPeerRoute::try_new(
                " ",
                WireGuardPeerKey::new([1; 32]),
                "192.0.2.20:7679".parse().unwrap(),
                "10.42.7.0/24",
            )
            .map(PeerRoute::WireGuard),
        ),
        (
            "peer.wireguard.endpoint",
            WireGuardPeerRoute::try_new(
                "node-b",
                WireGuardPeerKey::new([1; 32]),
                "192.0.2.20:0".parse().unwrap(),
                "10.42.7.0/24",
            )
            .map(PeerRoute::WireGuard),
        ),
        (
            "peer.pod_cidr",
            WireGuardPeerRoute::try_new(
                "node-b",
                WireGuardPeerKey::new([1; 32]),
                "192.0.2.20:7679".parse().unwrap(),
                "not-a-cidr",
            )
            .map(PeerRoute::WireGuard),
        ),
        (
            "peer.pod_cidr",
            WireGuardPeerRoute::try_new(
                "node-b",
                WireGuardPeerKey::new([1; 32]),
                "192.0.2.20:7679".parse().unwrap(),
                "10.42.7.0/31",
            )
            .map(PeerRoute::WireGuard),
        ),
        (
            "peer.direct.gateway",
            DirectPeerRoute::try_new(
                "node-b",
                "2001:db8::20".parse::<IpAddr>().unwrap(),
                "10.42.7.0/24",
            )
            .map(PeerRoute::Direct),
        ),
    ] {
        assert!(matches!(
            result,
            Err(PeerRouterError::InvalidRequest { field: actual, .. }) if actual == field
        ));
    }

    let route = wireguard_route();
    let PeerRoute::WireGuard(route) = route else {
        panic!("expected WireGuard route");
    };
    assert_eq!(route.node_name(), "node-b");
    assert_eq!(route.public_key().as_bytes(), &[7; 32]);
    assert_eq!(route.endpoint(), "192.0.2.20:7679".parse().unwrap());
    assert_eq!(route.allowed_pod_cidr().to_string(), "10.42.7.0/24");

    let direct = DirectPeerRoute::try_new(
        "node-c",
        IpAddr::V4(Ipv4Addr::new(192, 0, 2, 30)),
        "10.42.8.19/24",
    )
    .expect("valid direct peer route");
    assert_eq!(direct.node_name(), "node-c");
    assert_eq!(direct.gateway(), Ipv4Addr::new(192, 0, 2, 30));
    assert_eq!(direct.allowed_pod_cidr().to_string(), "10.42.8.0/24");
}

#[test]
fn peer_router_dispatch_preserves_exact_route_for_apply_and_remove() {
    let router = RecordingPeerRouter::default();
    let route = wireguard_route();

    poll_ready(router.apply_peer_route(&route)).unwrap();
    poll_ready(router.remove_peer_route(&route)).unwrap();

    assert_eq!(
        router.calls.into_inner().unwrap(),
        vec![(true, route.clone()), (false, route)]
    );
}

#[test]
fn operational_errors_preserve_peer_routing_failure_categories() {
    for (error, expected) in [
        (
            PeerRouterError::apply("netlink apply failed"),
            "netlink apply failed",
        ),
        (
            PeerRouterError::remove("netlink remove failed"),
            "netlink remove failed",
        ),
    ] {
        assert_eq!(error.to_string(), expected);
    }
}

#[derive(Default)]
struct RecordingServiceRouter {
    calls: Mutex<Vec<&'static str>>,
    added: Mutex<Vec<HostPortRules>>,
    removed: Mutex<Vec<HostPortRemoval>>,
}

impl ServiceRouter for RecordingServiceRouter {
    fn request_services_sync(&self) -> Result<(), ServiceRouterError> {
        self.calls.lock().unwrap().push("request");
        Ok(())
    }

    fn sync_services_now(&self) -> ServiceRouterFuture<'_> {
        Box::pin(async {
            self.calls.lock().unwrap().push("sync");
            Ok(())
        })
    }

    fn add_hostport_rules(&self, request: HostPortRules) -> ServiceRouterFuture<'_> {
        Box::pin(async move {
            self.added.lock().unwrap().push(request);
            Ok(())
        })
    }

    fn remove_hostport_rules(&self, request: HostPortRemoval) -> ServiceRouterFuture<'_> {
        Box::pin(async move {
            self.removed.lock().unwrap().push(request);
            Ok(())
        })
    }

    fn cleanup(&self) -> ServiceRouterFuture<'_> {
        Box::pin(async {
            self.calls.lock().unwrap().push("cleanup");
            Ok(())
        })
    }
}

fn assert_service_router_object_safe(_: &dyn ServiceRouter) {}

#[test]
fn service_router_is_object_safe_and_contract_values_are_send_sync() {
    assert_service_router_object_safe(&RecordingServiceRouter::default());

    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<HostPortProtocol>();
    assert_send_sync::<HostPortBinding>();
    assert_send_sync::<HostPortRules>();
    assert_send_sync::<HostPortRemoval>();
    assert_send_sync::<ServiceRouterError>();
}

#[test]
fn hostport_requests_validate_ports_and_nonempty_rule_sets() {
    for (field, result) in [
        (
            "service.hostport.host_port",
            HostPortBinding::try_new(None, 0, 80, HostPortProtocol::Tcp).map(|binding| {
                HostPortRules::try_new(Ipv4Addr::new(10, 42, 0, 9), vec![binding]).unwrap()
            }),
        ),
        (
            "service.hostport.container_port",
            HostPortBinding::try_new(None, 8080, 0, HostPortProtocol::Tcp).map(|binding| {
                HostPortRules::try_new(Ipv4Addr::new(10, 42, 0, 9), vec![binding]).unwrap()
            }),
        ),
        (
            "service.hostport.bindings",
            HostPortRules::try_new(Ipv4Addr::new(10, 42, 0, 9), Vec::new()),
        ),
    ] {
        assert!(matches!(
            result,
            Err(ServiceRouterError::InvalidRequest { field: actual, .. }) if actual == field
        ));
    }
}

#[test]
fn hostport_values_and_router_dispatch_preserve_exact_runtime_facts() {
    let binding = HostPortBinding::try_new(
        Some(Ipv4Addr::new(192, 0, 2, 10)),
        8080,
        80,
        HostPortProtocol::Tcp,
    )
    .unwrap();
    assert_eq!(binding.host_ip(), Some(Ipv4Addr::new(192, 0, 2, 10)));
    assert_eq!(binding.host_port(), 8080);
    assert_eq!(binding.container_port(), 80);
    assert_eq!(binding.protocol(), HostPortProtocol::Tcp);

    let pod_ip = Ipv4Addr::new(10, 42, 0, 9);
    let rules = HostPortRules::try_new(pod_ip, vec![binding.clone()]).unwrap();
    assert_eq!(rules.pod_ip(), pod_ip);
    assert_eq!(rules.bindings(), &[binding]);

    let removal = HostPortRemoval::new(pod_ip);
    let router = RecordingServiceRouter::default();
    router.request_services_sync().unwrap();
    poll_ready(router.sync_services_now()).unwrap();
    poll_ready(router.add_hostport_rules(rules.clone())).unwrap();
    poll_ready(router.remove_hostport_rules(removal)).unwrap();
    poll_ready(router.cleanup()).unwrap();

    assert_eq!(
        router.calls.into_inner().unwrap(),
        vec!["request", "sync", "cleanup"]
    );
    assert_eq!(router.added.into_inner().unwrap(), vec![rules]);
    assert_eq!(router.removed.into_inner().unwrap(), vec![removal]);
}

#[test]
fn pod_hostports_preserve_identity_optional_ip_and_validated_bindings() {
    let binding = HostPortBinding::try_new(None, 8080, 80, HostPortProtocol::Tcp).unwrap();
    let pod = PodIdentity::new("default", "web", "uid-web");
    let state = PodHostPorts::try_new(
        pod.clone(),
        Some(Ipv4Addr::new(10, 42, 0, 9)),
        vec![binding.clone()],
    )
    .unwrap();
    assert_eq!(state.pod(), &pod);
    assert_eq!(state.pod_ip(), Some(Ipv4Addr::new(10, 42, 0, 9)));
    assert_eq!(state.bindings(), &[binding]);
}

#[test]
fn pod_hostports_reject_incomplete_pod_identity() {
    for (field, pod) in [
        (
            "service.hostport.pod.namespace",
            PodIdentity::new("", "web", "uid-web"),
        ),
        (
            "service.hostport.pod.name",
            PodIdentity::new("default", "", "uid-web"),
        ),
        (
            "service.hostport.pod.uid",
            PodIdentity::new("default", "web", ""),
        ),
        (
            "service.hostport.pod.namespace",
            PodIdentity::new(" \t", "web", "uid-web"),
        ),
        (
            "service.hostport.pod.name",
            PodIdentity::new("default", " \n", "uid-web"),
        ),
        (
            "service.hostport.pod.uid",
            PodIdentity::new("default", "web", " \r"),
        ),
    ] {
        assert!(matches!(
            PodHostPorts::try_new(pod, None, Vec::new()),
            Err(ServiceRouterError::InvalidRequest { field: actual, .. }) if actual == field
        ));
    }
}

struct FailingSyncRequestRouter;

impl ServiceRouter for FailingSyncRequestRouter {
    fn request_services_sync(&self) -> Result<(), ServiceRouterError> {
        Err(ServiceRouterError::sync("request queue unavailable"))
    }

    fn sync_services_now(&self) -> ServiceRouterFuture<'_> {
        Box::pin(async { Ok(()) })
    }

    fn add_hostport_rules(&self, _request: HostPortRules) -> ServiceRouterFuture<'_> {
        Box::pin(async { Ok(()) })
    }

    fn remove_hostport_rules(&self, _request: HostPortRemoval) -> ServiceRouterFuture<'_> {
        Box::pin(async { Ok(()) })
    }

    fn cleanup(&self) -> ServiceRouterFuture<'_> {
        Box::pin(async { Ok(()) })
    }
}

#[test]
fn service_sync_request_preserves_typed_failure() {
    assert!(matches!(
        FailingSyncRequestRouter.request_services_sync(),
        Err(ServiceRouterError::Sync { message }) if message == "request queue unavailable"
    ));
}

#[test]
fn operational_errors_preserve_service_routing_failure_categories() {
    for (error, expected) in [
        (
            ServiceRouterError::sync("API unavailable"),
            "API unavailable",
        ),
        (
            ServiceRouterError::hostport("nft batch failed"),
            "nft batch failed",
        ),
        (
            ServiceRouterError::cleanup("table cleanup failed"),
            "table cleanup failed",
        ),
    ] {
        assert_eq!(error.to_string(), expected);
    }
}

struct QueueEndpointSubscription {
    events: std::collections::VecDeque<Result<PodEndpointEvent, PodEndpointError>>,
    dropped: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl Drop for QueueEndpointSubscription {
    fn drop(&mut self) {
        self.dropped
            .store(true, std::sync::atomic::Ordering::Release);
    }
}

impl PodEndpointEventSubscription for QueueEndpointSubscription {
    fn poll_next(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Option<Result<PodEndpointEvent, PodEndpointError>>> {
        Poll::Ready(self.events.pop_front())
    }
}

struct RecordingEndpointResolver {
    endpoint: PodEndpoint,
    topology: PodEndpointTopology,
    dropped: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl PodEndpointResolver for RecordingEndpointResolver {
    fn resolve(&self, pod_ip: Ipv4Addr) -> PodEndpointFuture<'_, Option<PodEndpoint>> {
        Box::pin(async move {
            assert_eq!(pod_ip, self.endpoint.pod_ip());
            Ok(Some(self.endpoint.clone()))
        })
    }
}

impl PodEndpointEventSource for RecordingEndpointResolver {
    fn subscribe(&self) -> PodEndpointFuture<'_, PodEndpointEventStream> {
        Box::pin(async move {
            Ok(Box::pin(QueueEndpointSubscription {
                events: [
                    Ok(PodEndpointEvent::Resync(vec![self.topology.clone()])),
                    Ok(PodEndpointEvent::Upsert(self.topology.clone())),
                    Ok(PodEndpointEvent::Delete(self.topology.pod_ip())),
                    Err(PodEndpointError::event_source("re-list failed")),
                ]
                .into(),
                dropped: self.dropped.clone(),
            }) as PodEndpointEventStream)
        })
    }
}

fn assert_endpoint_resolver_object_safe(_: &dyn PodEndpointResolver) {}
fn assert_endpoint_source_object_safe(_: &dyn PodEndpointEventSource) {}
fn assert_endpoint_subscription_object_safe(_: Pin<&mut dyn PodEndpointEventSubscription>) {}

fn direct_endpoint() -> DirectPodEndpoint {
    DirectPodEndpoint::try_new(Ipv4Addr::new(10, 42, 7, 9), "node-b")
        .expect("valid direct pod endpoint")
}

#[test]
fn endpoint_ports_are_object_safe_and_contract_values_are_send_sync() {
    let direct = direct_endpoint();
    let dropped = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let resolver = RecordingEndpointResolver {
        endpoint: PodEndpoint::EncryptedDirect(direct.clone()),
        topology: PodEndpointTopology::Direct(direct),
        dropped,
    };
    assert_endpoint_resolver_object_safe(&resolver);
    assert_endpoint_source_object_safe(&resolver);

    let mut subscription = poll_ready(resolver.subscribe()).unwrap();
    assert_endpoint_subscription_object_safe(subscription.as_mut());

    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<DirectPodEndpoint>();
    assert_send_sync::<HostPortPodEndpoint>();
    assert_send_sync::<PodEndpoint>();
    assert_send_sync::<PodEndpointTopology>();
    assert_send_sync::<PodEndpointEvent>();
    assert_send_sync::<PodEndpointError>();
}

#[test]
fn endpoint_values_validate_identity_addresses_and_ports() {
    for (field, result) in [
        (
            "endpoint.pod_ip",
            DirectPodEndpoint::try_new(Ipv4Addr::UNSPECIFIED, "node-b")
                .map(PodEndpointTopology::Direct),
        ),
        (
            "endpoint.node_name",
            DirectPodEndpoint::try_new(Ipv4Addr::new(10, 42, 7, 9), " ")
                .map(PodEndpointTopology::Direct),
        ),
        (
            "endpoint.host_port_tcp",
            HostPortPodEndpoint::try_new(
                Ipv4Addr::new(10, 42, 7, 9),
                "node-b",
                Ipv4Addr::new(192, 0, 2, 20),
                Some(0),
                None,
            )
            .map(PodEndpointTopology::HostPort),
        ),
        (
            "endpoint.host_port_udp",
            HostPortPodEndpoint::try_new(
                Ipv4Addr::new(10, 42, 7, 9),
                "node-b",
                Ipv4Addr::new(192, 0, 2, 20),
                None,
                Some(0),
            )
            .map(PodEndpointTopology::HostPort),
        ),
    ] {
        assert!(matches!(
            result,
            Err(PodEndpointError::InvalidEndpoint { field: actual, .. }) if actual == field
        ));
    }
}

#[test]
fn endpoint_values_preserve_direct_and_dual_protocol_hostport_topology() {
    let direct = direct_endpoint();
    assert_eq!(direct.pod_ip(), Ipv4Addr::new(10, 42, 7, 9));
    assert_eq!(direct.node_name(), "node-b");

    let hostport = HostPortPodEndpoint::try_new(
        Ipv4Addr::new(10, 42, 8, 9),
        "rootless-b",
        Ipv4Addr::new(192, 0, 2, 44),
        Some(31_234),
        Some(31_235),
    )
    .unwrap();
    assert_eq!(hostport.pod_ip(), Ipv4Addr::new(10, 42, 8, 9));
    assert_eq!(hostport.node_name(), "rootless-b");
    assert_eq!(hostport.node_ip(), Ipv4Addr::new(192, 0, 2, 44));
    assert_eq!(hostport.host_port_tcp(), Some(31_234));
    assert_eq!(hostport.host_port_udp(), Some(31_235));

    assert_eq!(
        PodEndpointTopology::Direct(direct.clone()).pod_ip(),
        direct.pod_ip()
    );
    assert_eq!(
        PodEndpoint::UnencryptedDirect(direct.clone()).pod_ip(),
        direct.pod_ip()
    );
    assert_eq!(
        PodEndpointTopology::HostPort(hostport.clone()).pod_ip(),
        hostport.pod_ip()
    );
    assert_eq!(
        PodEndpoint::HostPort(hostport).pod_ip(),
        Ipv4Addr::new(10, 42, 8, 9)
    );
}

#[test]
fn endpoint_source_preserves_order_resync_and_drop_cancellation() {
    let direct = direct_endpoint();
    let topology = PodEndpointTopology::Direct(direct.clone());
    let dropped = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let resolver = RecordingEndpointResolver {
        endpoint: PodEndpoint::EncryptedDirect(direct),
        topology: topology.clone(),
        dropped: dropped.clone(),
    };

    assert_eq!(
        poll_ready(resolver.resolve(topology.pod_ip())).unwrap(),
        Some(resolver.endpoint.clone())
    );
    let mut subscription = poll_ready(resolver.subscribe()).unwrap();
    let mut context = Context::from_waker(Waker::noop());
    assert_eq!(
        subscription.as_mut().poll_next(&mut context),
        Poll::Ready(Some(Ok(PodEndpointEvent::Resync(vec![topology.clone()]))))
    );
    assert_eq!(
        subscription.as_mut().poll_next(&mut context),
        Poll::Ready(Some(Ok(PodEndpointEvent::Upsert(topology.clone()))))
    );
    assert_eq!(
        subscription.as_mut().poll_next(&mut context),
        Poll::Ready(Some(Ok(PodEndpointEvent::Delete(topology.pod_ip()))))
    );
    assert!(matches!(
        subscription.as_mut().poll_next(&mut context),
        Poll::Ready(Some(Err(PodEndpointError::EventSource { .. })))
    ));
    assert_eq!(
        subscription.as_mut().poll_next(&mut context),
        Poll::Ready(None)
    );
    assert_eq!(
        subscription.as_mut().poll_next(&mut context),
        Poll::Ready(None)
    );
    assert!(!dropped.load(std::sync::atomic::Ordering::Acquire));
    drop(subscription);
    assert!(dropped.load(std::sync::atomic::Ordering::Acquire));

    let resync = PodEndpointEvent::Resync(vec![topology.clone()]);
    assert_eq!(resync, PodEndpointEvent::Resync(vec![topology]));
}

#[test]
fn operational_errors_preserve_endpoint_failure_categories() {
    for (error, expected) in [
        (PodEndpointError::resolve("lookup failed"), "lookup failed"),
        (
            PodEndpointError::event_source("re-list failed"),
            "re-list failed",
        ),
    ] {
        assert_eq!(error.to_string(), expected);
    }
}
