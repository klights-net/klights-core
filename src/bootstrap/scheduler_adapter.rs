//! Concrete root construction for the controller-owned scheduler runtime.

use std::sync::Arc;

pub(crate) fn leader_scheduler_runtime(
    positioned_watch: klights_watch::PositionedWatchService,
    pods: Arc<dyn klights_pod_api::PodScheduling>,
) -> klights_controllers::scheduler::LeaderSchedulerRuntime {
    klights_controllers::scheduler::LeaderSchedulerRuntime::new(Arc::new(positioned_watch), pods)
}
