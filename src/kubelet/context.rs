use std::sync::Arc;

use crate::kubelet::outbox::Outbox;
use crate::kubelet::pod_creation_state::PodStartRetryTracker;
use crate::kubelet::pod_lifecycle_router::PodLifecycleRouter;
use crate::kubelet::pod_repository::PodRepository;

pub(crate) type PodLifecycleReceiver = Arc<
    tokio::sync::Mutex<
        Option<tokio::sync::mpsc::Receiver<klights_kubelet::lifecycle::LifecycleCommand>>,
    >,
>;

#[derive(Clone, Default)]
pub(crate) struct HostIpState {
    value: Arc<std::sync::RwLock<Option<Arc<str>>>>,
}

impl HostIpState {
    pub(crate) fn publish(&self, value: String) {
        *self
            .value
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::from(value));
    }

    pub(crate) fn current(&self) -> Arc<str> {
        self.value
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .unwrap_or_else(|| Arc::from("127.0.0.1"))
    }
}

pub(crate) type KubeletConfig = klights_kubelet::context::KubeletConfig<
    crate::kubelet::log_rotation::LogRotationPolicy,
    klights_kubelet::node_capacity::NodeCapacity,
    crate::kubelet::runtime_paths::KubeletRuntimePaths,
>;

pub(crate) type KubeletLifecycleServices = klights_kubelet::context::LifecycleServices<
    PodRepository,
    PodLifecycleRouter,
    PodLifecycleReceiver,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_ip_state_is_instance_owned_and_defaults_to_loopback() {
        let first = HostIpState::default();
        let first_clone = first.clone();
        let second = HostIpState::default();

        assert_eq!(&*first.current(), "127.0.0.1");
        first.publish("192.0.2.10".to_string());

        assert_eq!(&*first.current(), "192.0.2.10");
        assert_eq!(
            &*first_clone.current(),
            "192.0.2.10",
            "clones of one injected instance must observe the same publication"
        );
        assert_eq!(&*second.current(), "127.0.0.1");
    }
}
