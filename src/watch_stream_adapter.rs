use crate::api::watch_stream::{
    WatchSourceCurrentResourceVersionFuture, WatchSourceListFuture, WatchStreamSource,
};
use crate::datastore::{DatastoreBackend, DatastoreHandle, ResourceListQuery};

impl WatchStreamSource for DatastoreHandle {
    fn subscribe_watch_signals(
        &self,
        topic: klights_watch::WatchTopic,
    ) -> klights_watch::WatchSignalReceiver {
        DatastoreBackend::subscribe_watch_signals(self.as_ref(), topic)
    }

    fn current_resource_version(&self) -> WatchSourceCurrentResourceVersionFuture<'_> {
        Box::pin(async move { self.get_current_resource_version().await })
    }

    fn list_watch_resources<'a>(
        &'a self,
        api_version: &'a str,
        kind: &'a str,
        namespace: Option<&'a str>,
        label_selector: Option<&'a str>,
        field_selector: Option<&'a str>,
        limit: Option<i64>,
    ) -> WatchSourceListFuture<'a> {
        Box::pin(async move {
            let list = self
                .list_resources(
                    api_version,
                    kind,
                    namespace,
                    ResourceListQuery::new(label_selector, field_selector, limit, None),
                )
                .await?;
            klights_leader_api::ResourceListResult::try_new(
                list.items,
                list.resource_version,
                list.watch_replay_position,
                list.continue_token,
                list.remaining_item_count,
            )
            .map_err(crate::api::AppError::from)
        })
    }

    fn watch_resources(
        &self,
        request: klights_leader_api::WatchRequest,
    ) -> klights_leader_api::LeaderWatchFuture<'_> {
        let positioned_watch =
            crate::control_plane::client::local::datastore_positioned_watch_service(self.clone());
        Box::pin(async move {
            klights_leader_api::LeaderWatch::watch_resources(&positioned_watch, request).await
        })
    }
}
