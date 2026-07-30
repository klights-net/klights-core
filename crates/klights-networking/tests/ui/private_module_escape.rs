use klights_networking as networking;

fn main() {
    #[cfg(target_os = "linux")]
    {
        let _ = networking::pod_link::open_netns_file_blocking;
    }
    let _ = std::mem::size_of::<networking::root_datapath::RootDatapath>();
    let _ = std::mem::size_of::<networking::peer_dataplane::RootPeerDataplane>();
    let _ = std::mem::size_of::<networking::device_state::LinkKind>();
    let _ = networking::pod_link::open_netns_file_blocking;
    let _ = networking::netns_sync::new_route_socket;
}
