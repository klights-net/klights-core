use klights_kubelet::outbox::Outbox;
use klights_kubelet::pod_creation_state::PodStartRetryTracker;
use klights_kubelet::pod_lifecycle_router::PodLifecycleRouter;

pub(crate) type KubeletConfig = klights_kubelet::context::KubeletConfig<
    klights_kubelet::log_rotation::LogRotationPolicy,
    klights_kubelet::node_capacity::NodeCapacity,
    klights_kubelet::runtime_paths::KubeletRuntimePaths,
>;

pub(crate) type KubeletLifecycleServices = klights_kubelet::context::LifecycleServices<
    PodLifecycleRouter,
    klights_kubelet::context::PodLifecycleReceiver,
    PodStartRetryTracker,
>;

pub(crate) type KubeletRuntimeNetworkServices = klights_kubelet::context::RuntimeNetworkServices;

pub(crate) type KubeletStatusDeliveryServices =
    klights_kubelet::context::StatusDeliveryServices<Outbox>;

pub(crate) type KubeletLocalExecutionServices = klights_kubelet::context::LocalExecutionServices<
    dyn klights_kubelet::runtime_clock::RuntimeClock,
    KubeletConfig,
>;

pub(crate) type KubeletContext = klights_kubelet::context::KubeletContext<
    KubeletLifecycleServices,
    KubeletRuntimeNetworkServices,
    KubeletStatusDeliveryServices,
    KubeletLocalExecutionServices,
>;
