use klights_watch::{WatchTarget, WatchTargetScope};

#[test]
fn watch_targets_preserve_cluster_and_namespace_scope() {
    assert_eq!(
        WatchTarget::cluster("v1", "Node").scope(),
        &WatchTargetScope::Cluster
    );
    assert_eq!(
        WatchTarget::namespaced("v1", "Pod").scope(),
        &WatchTargetScope::Namespaced(None)
    );
    assert_eq!(
        WatchTarget::namespaced_in_namespace("v1", "Pod", "default").scope(),
        &WatchTargetScope::Namespaced(Some("default".to_string()))
    );
}
