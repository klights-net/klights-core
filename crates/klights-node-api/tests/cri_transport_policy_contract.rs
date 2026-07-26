use std::time::Duration;

use klights_node_api::CriTransportPolicy;

#[test]
fn cri_transport_policy_exposes_only_uds_dial_and_message_limits() {
    let policy = CriTransportPolicy::new(Duration::from_secs(7), 8 * 1024 * 1024);

    assert_eq!(policy.connect_timeout(), Duration::from_secs(7));
    assert_eq!(policy.max_message_bytes(), 8 * 1024 * 1024);
}
