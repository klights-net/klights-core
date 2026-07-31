use klights_kubelet::context::{KubeletConfig, KubeletConfigError};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RotationPolicy;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct NodeCapacity;

#[derive(Clone, Debug, PartialEq, Eq)]
struct RuntimePaths;

#[test]
fn kubelet_config_accepts_validated_facts() {
    let config = KubeletConfig::try_new(
        "10.43.128.0/17".to_string(),
        "worker-a".to_string(),
        "klights".to_string(),
        RotationPolicy,
        NodeCapacity,
        RuntimePaths,
    )
    .unwrap();

    assert_eq!(config.service_cidr(), "10.43.128.0/17");
    assert_eq!(config.node_name(), "worker-a");
    assert_eq!(config.containerd_namespace(), "klights");
    assert_eq!(config.log_rotation(), RotationPolicy);
    assert_eq!(config.node_capacity(), NodeCapacity);
    assert_eq!(config.paths(), &RuntimePaths);
}

#[test]
fn kubelet_config_rejects_invalid_facts() {
    let invalid_cidrs = ["not-a-cidr", "192.0.2.1/33", "2001:db8::1/129"];
    for service_cidr in invalid_cidrs {
        assert!(matches!(
            KubeletConfig::try_new(
                service_cidr.to_string(),
                "worker-a".to_string(),
                "klights".to_string(),
                RotationPolicy,
                NodeCapacity,
                RuntimePaths,
            ),
            Err(KubeletConfigError::InvalidServiceCidr(value)) if value == service_cidr
        ));
    }

    for (node_name, namespace, field) in [
        ("", "klights", "node_name"),
        ("worker-a", "", "containerd_namespace"),
    ] {
        assert_eq!(
            KubeletConfig::try_new(
                "10.43.128.0/17".to_string(),
                node_name.to_string(),
                namespace.to_string(),
                RotationPolicy,
                NodeCapacity,
                RuntimePaths,
            ),
            Err(KubeletConfigError::Empty { field })
        );
    }
}
