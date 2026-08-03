//! Canonical registration policy for the production side-effect set.

use std::sync::Arc;

use super::{
    ControllerDispatcherSlot, ErrorPolicy, PodSideEffectPortsSlot, SideEffect, SideEffectRegistry,
};

/// The twelve concrete effects selected by root composition.
///
/// Root constructs focused adapters and the controller-owned effect wrappers,
/// then hands this complete immutable bundle to [`default_registry`].
pub struct DefaultSideEffects {
    apiservice: Arc<dyn SideEffect>,
    daemonset_node: Arc<dyn SideEffect>,
    endpoint_mirror: Arc<dyn SideEffect>,
    endpoint_slice_sync: Arc<dyn SideEffect>,
    hpa: Arc<dyn SideEffect>,
    job: Arc<dyn SideEffect>,
    namespace_termination: Arc<dyn SideEffect>,
    node_taint_manager: Arc<dyn SideEffect>,
    pdb: Arc<dyn SideEffect>,
    resource_quota: Arc<dyn SideEffect>,
    service_account_defaults: Arc<dyn SideEffect>,
    workload_pod: Arc<dyn SideEffect>,
}

impl DefaultSideEffects {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        apiservice: Arc<dyn SideEffect>,
        daemonset_node: Arc<dyn SideEffect>,
        endpoint_mirror: Arc<dyn SideEffect>,
        endpoint_slice_sync: Arc<dyn SideEffect>,
        hpa: Arc<dyn SideEffect>,
        job: Arc<dyn SideEffect>,
        namespace_termination: Arc<dyn SideEffect>,
        node_taint_manager: Arc<dyn SideEffect>,
        pdb: Arc<dyn SideEffect>,
        resource_quota: Arc<dyn SideEffect>,
        service_account_defaults: Arc<dyn SideEffect>,
        workload_pod: Arc<dyn SideEffect>,
    ) -> Self {
        Self {
            apiservice,
            daemonset_node,
            endpoint_mirror,
            endpoint_slice_sync,
            hpa,
            job,
            namespace_termination,
            node_taint_manager,
            pdb,
            resource_quota,
            service_account_defaults,
            workload_pod,
        }
    }
}

/// Construct the canonical registry using root-provided focused effects and
/// the exact late-bound slots shared by those effects.
pub fn default_registry(
    effects: DefaultSideEffects,
    pod_ports: PodSideEffectPortsSlot,
    controller_dispatcher: ControllerDispatcherSlot,
) -> SideEffectRegistry {
    let mut registry = SideEffectRegistry::with_slots(pod_ports, controller_dispatcher);
    registry.register(
        "v1",
        "Endpoints",
        effects.endpoint_mirror,
        ErrorPolicy::Warn,
    );
    registry.register(
        "discovery.k8s.io/v1",
        "EndpointSlice",
        effects.endpoint_slice_sync,
        ErrorPolicy::Warn,
    );
    for (api_version, kind) in [
        ("v1", "Pod"),
        ("v1", "ConfigMap"),
        ("v1", "Secret"),
        ("v1", "PersistentVolumeClaim"),
        ("v1", "ServiceAccount"),
        ("v1", "Service"),
        ("v1", "ResourceQuota"),
        ("v1", "LimitRange"),
        ("v1", "ReplicationController"),
        ("apps/v1", "Deployment"),
        ("apps/v1", "ReplicaSet"),
        ("apps/v1", "StatefulSet"),
        ("apps/v1", "DaemonSet"),
        ("batch/v1", "Job"),
        ("batch/v1", "CronJob"),
        ("policy/v1", "PodDisruptionBudget"),
    ] {
        registry.register(
            api_version,
            kind,
            effects.resource_quota.clone(),
            ErrorPolicy::Warn,
        );
    }
    registry.register(
        "v1",
        "ServiceAccount",
        effects.service_account_defaults,
        ErrorPolicy::Warn,
    );
    registry.register("v1", "Pod", effects.workload_pod, ErrorPolicy::Warn);
    registry.register("v1", "Pod", effects.job, ErrorPolicy::Warn);
    registry.register("v1", "Pod", effects.pdb, ErrorPolicy::Warn);
    registry.register(
        "v1",
        "Pod",
        effects.namespace_termination,
        ErrorPolicy::Warn,
    );
    for (api_version, kind) in [
        ("v1", "Pod"),
        ("v1", "ReplicationController"),
        ("apps/v1", "Deployment"),
        ("apps/v1", "ReplicaSet"),
        ("apps/v1", "StatefulSet"),
    ] {
        registry.register(api_version, kind, effects.hpa.clone(), ErrorPolicy::Warn);
    }
    registry.register("v1", "Node", effects.daemonset_node, ErrorPolicy::Warn);
    for (api_version, kind) in [
        ("apiregistration.k8s.io/v1", "APIService"),
        ("v1", "Service"),
        ("v1", "Endpoints"),
        ("discovery.k8s.io/v1", "EndpointSlice"),
    ] {
        registry.register(
            api_version,
            kind,
            effects.apiservice.clone(),
            ErrorPolicy::Warn,
        );
    }
    registry.register("v1", "Node", effects.node_taint_manager, ErrorPolicy::Warn);
    registry
}
