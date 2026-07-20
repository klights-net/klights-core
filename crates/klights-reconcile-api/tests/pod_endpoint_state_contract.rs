use klights_reconcile_api::PodEndpointState;

#[test]
fn neutral_pod_endpoint_state_compares_all_endpoint_relevant_facts() {
    let labels = "app=web";
    let pod_ip = "10.42.0.2";
    let pod_ips = "[10.42.0.2]";
    let deletion_timestamp = "2026-07-20T00:00:00Z";

    let ready = PodEndpointState::new(
        true,
        false,
        Some(labels),
        Some(pod_ip),
        Some(pod_ips),
        None::<&str>,
    );
    assert!(ready.is_ready());
    assert!(!ready.is_terminal());
    assert!(!ready.differs_from(&ready));

    for changed in [
        PodEndpointState::new(
            false,
            false,
            Some(labels),
            Some(pod_ip),
            Some(pod_ips),
            None::<&str>,
        ),
        PodEndpointState::new(
            true,
            true,
            Some(labels),
            Some(pod_ip),
            Some(pod_ips),
            None::<&str>,
        ),
        PodEndpointState::new(
            true,
            false,
            Some("app=api"),
            Some(pod_ip),
            Some(pod_ips),
            None::<&str>,
        ),
        PodEndpointState::new(
            true,
            false,
            Some(labels),
            Some("10.42.0.3"),
            Some(pod_ips),
            None::<&str>,
        ),
        PodEndpointState::new(
            true,
            false,
            Some(labels),
            Some(pod_ip),
            Some("[10.42.0.3]"),
            None::<&str>,
        ),
        PodEndpointState::new(
            true,
            false,
            Some(labels),
            Some(pod_ip),
            Some(pod_ips),
            Some(deletion_timestamp),
        ),
    ] {
        assert!(ready.differs_from(&changed));
    }
}
