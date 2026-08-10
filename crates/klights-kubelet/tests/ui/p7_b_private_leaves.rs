use klights_kubelet::pod_fs::PodFs;
use klights_kubelet::pod_subsystem::PodSubsystem;
use klights_kubelet::runtime::service::RealPodRuntimeService;

fn leak_subsystem(subsystem: &PodSubsystem) {
    let _ = &subsystem.runtime;
}

fn main() {
    let _ = std::any::type_name::<PodFs>();
    let _ = std::any::type_name::<RealPodRuntimeService>();
}
