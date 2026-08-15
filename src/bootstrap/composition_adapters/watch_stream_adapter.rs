use k8s_native_service::watch::{WatchSourceListFuture, WatchSourceWaitFuture, WatchStreamSource};
use klights_cluster_store::DurableAllocatorRead;
pub(crate) struct DatastoreWatchStreamAdapter {
    resource_query: std::sync::Arc<dyn klights_leader_api::LeaderResourceQuery>,
    allocator_reads: std::sync::Arc<dyn DurableAllocatorRead>,
    signals: std::sync::Arc<dyn klights_watch::WatchSignalSubscribe>,
    positioned_watch: klights_watch::PositionedWatchService,
}

impl DatastoreWatchStreamAdapter {
    pub(crate) fn new(
        resource_query: std::sync::Arc<dyn klights_leader_api::LeaderResourceQuery>,
        allocator_reads: std::sync::Arc<dyn DurableAllocatorRead>,
        signals: std::sync::Arc<dyn klights_watch::WatchSignalSubscribe>,
        positioned_watch: klights_watch::PositionedWatchService,
    ) -> Self {
        Self {
            resource_query,
            allocator_reads,
            signals,
            positioned_watch,
        }
    }
}

impl WatchStreamSource for DatastoreWatchStreamAdapter {
    fn wait_until_fresh<'a>(
        &'a self,
        target_rv: i64,
        api_version: &'a str,
        kind: &'a str,
        task_supervisor: &'a klights_supervisor::TaskSupervisor,
    ) -> WatchSourceWaitFuture<'a> {
        Box::pin(klights_watch::wait_until_resource_version_fresh(
            self.allocator_reads.as_ref(),
            self.signals.as_ref(),
            target_rv,
            klights_watch::WatchTopic::new(api_version, kind),
            k8s_native_service::watch::READ_FRESHNESS_TIMEOUT,
            task_supervisor,
        ))
    }

    fn list_watch_resources<'a>(
        &'a self,
        api_version: &'a str,
        kind: &'a str,
        scope: klights_leader_api::ResourceListScope,
        label_selector: Option<&'a str>,
        field_selector: Option<&'a str>,
        limit: Option<i64>,
    ) -> WatchSourceListFuture<'a> {
        Box::pin(async move {
            let request = klights_leader_api::ResourceListRequest::try_new(
                api_version,
                kind,
                scope,
                label_selector.map(str::to_owned),
                field_selector.map(str::to_owned),
                limit,
                None,
                klights_leader_api::ResourceQueryConsistency::LeaderFresh,
            )?;
            self.resource_query.list_resources(request).await
        })
    }

    fn watch_resources(
        &self,
        request: klights_leader_api::WatchRequest,
    ) -> klights_leader_api::LeaderWatchFuture<'_> {
        let positioned_watch = self.positioned_watch.clone();
        Box::pin(async move {
            klights_leader_api::LeaderWatch::watch_resources(&positioned_watch, request).await
        })
    }
}
