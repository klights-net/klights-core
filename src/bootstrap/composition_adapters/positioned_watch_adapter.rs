use std::sync::Arc;

use crate::bootstrap::cluster_store::selector::PassiveReadPorts;

pub(crate) fn datastore_positioned_watch_service(
    passive_reads: &PassiveReadPorts,
    watch_signals: Arc<dyn klights_watch::WatchSignalSubscribe>,
) -> klights_watch::PositionedWatchService {
    klights_watch::PositionedWatchService::new(
        passive_reads.resource_reads(),
        passive_reads.history_reads(),
        passive_reads.allocator_reads(),
        watch_signals,
    )
}

#[cfg(test)]
pub(crate) fn for_test(
    passive_reads: &PassiveReadPorts,
    db: &klights_cluster_datastore::sqlite::embedded::Datastore,
) -> klights_watch::PositionedWatchService {
    datastore_positioned_watch_service(
        passive_reads,
        crate::bootstrap::watch_commit_wiring::test_signal_source(db),
    )
}
