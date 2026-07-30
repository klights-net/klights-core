use std::net::Ipv4Addr;
use std::sync::Arc;

use klights_node_store::{PodIpamStore as _, PodNetworkCache as _};

#[tokio::test]
async fn real_adapter_exhaustion_reclaims_stale_row_and_retries_once() {
    let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let backend = crate::datastore::node_local::selector::open_node_local(
        crate::datastore::backend_kind::BackendKind::Sqlite,
        None,
        supervisor,
        None,
        "sqlite:cni-real-adapter-stale-reclaim",
    )
    .await
    .unwrap();
    let adapter = Arc::new(backend);
    let base = u32::from(Ipv4Addr::new(10, 42, 91, 0));
    let stale_pod = klights_types::PodIdentity::new("default", "stale", "uid-stale");
    let stale = adapter
        .reserve_ip_and_insert_network(
            klights_node_store::PodNetworkAllocationRequest::try_new(
                "sandbox-stale",
                stale_pod,
                base,
                4,
                "veth-stale",
                "/run/netns/stale",
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(stale.ip_int(), base + 2);

    let winner_pod = klights_types::PodIdentity::new("default", "winner", "uid-winner");
    let winner = klights_networking::test_support::allocate_ip_with_reclaim(
        adapter.as_ref(),
        adapter.as_ref(),
        adapter.as_ref(),
        "sandbox-winner",
        &winner_pod,
        base,
        4,
        "veth-winner",
        "/run/netns/winner",
    )
    .await
    .expect("typed exhaustion must trigger stale-row reclaim and one retry");
    assert_eq!(winner.1, base + 2);
    assert!(
        adapter
            .get_network_for_sandbox(
                klights_node_store::SandboxKey::try_new("sandbox-stale").unwrap()
            )
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        adapter
            .get_network_for_sandbox(
                klights_node_store::SandboxKey::try_new("sandbox-winner").unwrap()
            )
            .await
            .unwrap()
            .is_some()
    );
}
