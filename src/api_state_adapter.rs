impl crate::api::state_ports::ApiStateCompositionTypes
    for crate::api::state_ports::ApiStateComposition
{
    type ResourceStore = crate::datastore::DatastoreHandle;
    type PodRepository = std::sync::Arc<crate::kubelet::pod_repository::PodRepository>;
    type PodLifecycleRouter =
        Option<std::sync::Arc<crate::kubelet::pod_lifecycle_router::PodLifecycleRouter>>;
    type FailureMetrics = std::sync::Arc<crate::side_effects::SideEffectMetrics>;
    type NodeLeaseObservations = std::sync::Arc<crate::node_lease_tracker::NodeLeaseTracker>;
}
