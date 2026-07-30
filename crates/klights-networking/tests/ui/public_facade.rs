use klights_networking as networking;

fn assert_error<T: std::error::Error + Send + Sync + 'static>() {}

fn main() {
    assert_error::<networking::cni_plugin::CniSocketError>();
    assert_error::<networking::service_routing::RoutingStateError>();
    assert_error::<networking::wireguard::WireGuardBootConfigError>();

    let _socket = networking::cni_plugin::CniSocketPath::try_new("/run/klights/cni.sock").unwrap();
    let mtu = networking::PodLinkMtu::try_new(1280).unwrap();
    assert_eq!(mtu.get(), 1280);

    let wireguard = networking::wireguard::WireGuardBootConfig::try_new(
        "klights.wg",
        "/var/lib/klights/wireguard.key",
        7679,
    )
    .unwrap();
    let _ = wireguard;
    let _ = networking::RootPeerDataplaneBoot::new;
}
