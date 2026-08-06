pub use klights_kubelet::pod_deletion_finalizer::{
    PodDeletionFinalizer, RealPodDeletionFinalizerDependencies, compose_real_pod_deletion_finalizer,
};

#[cfg(not(test))]
pub use klights_kubelet::pod_deletion_finalizer::RealPodDeletionFinalizer;
