use crate::api::watch_stream::{WatchSourceListFuture, WatchSourceWaitFuture, WatchStreamSource};
use crate::datastore::{DatastoreHandle, ResourceListQuery};

pub(crate) struct DatastoreWatchStreamAdapter {
    db: DatastoreHandle,
    signals: std::sync::Arc<dyn klights_watch::WatchSignalSubscribe>,
}

impl DatastoreWatchStreamAdapter {
    pub(crate) fn new(
        db: DatastoreHandle,
        signals: std::sync::Arc<dyn klights_watch::WatchSignalSubscribe>,
    ) -> Self {
        Self { db, signals }
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
        Box::pin(wait_until_fresh(
            &self.db,
            self.signals.as_ref(),
            target_rv,
            klights_watch::WatchTopic::new(api_version, kind),
            task_supervisor,
        ))
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
                .db
                .list_resources(
                    api_version,
                    kind,
                    namespace,
                    ResourceListQuery::new(label_selector, field_selector, limit, None),
                )
                .await
                .map_err(|error| {
                    klights_leader_api::ResourceQueryError::query_failed(error.to_string())
                })?;
            klights_leader_api::ResourceListResult::try_new(
                list.items,
                list.resource_version,
                list.watch_replay_position,
                list.continue_token,
                list.remaining_item_count,
            )
        })
    }

    fn watch_resources(
        &self,
        request: klights_leader_api::WatchRequest,
    ) -> klights_leader_api::LeaderWatchFuture<'_> {
        let positioned_watch =
            crate::control_plane::client::local::datastore_positioned_watch_service(
                self.db.clone(),
                self.signals.clone(),
            );
        Box::pin(async move {
            klights_leader_api::LeaderWatch::watch_resources(&positioned_watch, request).await
        })
    }
}

#[cfg(test)]
impl WatchStreamSource for DatastoreHandle {
    fn wait_until_fresh<'a>(
        &'a self,
        target_rv: i64,
        api_version: &'a str,
        kind: &'a str,
        task_supervisor: &'a klights_supervisor::TaskSupervisor,
    ) -> WatchSourceWaitFuture<'a> {
        let db = self.clone();
        let signals = crate::watch_commit_observation_adapter::test_signal_source(self);
        let topic = klights_watch::WatchTopic::new(api_version, kind);
        Box::pin(async move {
            wait_until_fresh(&db, signals.as_ref(), target_rv, topic, task_supervisor).await
        })
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
        let adapter = DatastoreWatchStreamAdapter::new(
            self.clone(),
            crate::watch_commit_observation_adapter::test_signal_source(self),
        );
        Box::pin(async move {
            adapter
                .list_watch_resources(
                    api_version,
                    kind,
                    namespace,
                    label_selector,
                    field_selector,
                    limit,
                )
                .await
        })
    }

    fn watch_resources(
        &self,
        request: klights_leader_api::WatchRequest,
    ) -> klights_leader_api::LeaderWatchFuture<'_> {
        let positioned = crate::control_plane::client::local::datastore_positioned_watch_service(
            self.clone(),
            crate::watch_commit_observation_adapter::test_signal_source(self),
        );
        Box::pin(async move {
            klights_leader_api::LeaderWatch::watch_resources(&positioned, request).await
        })
    }
}

async fn wait_until_fresh(
    db: &DatastoreHandle,
    signals: &dyn klights_watch::WatchSignalSubscribe,
    target_rv: i64,
    topic: klights_watch::WatchTopic,
    task_supervisor: &klights_supervisor::TaskSupervisor,
) {
    if target_rv <= 0 {
        return;
    }
    let mut fresh_rx = crate::watch_commit_observation_adapter::subscribe(signals, topic);
    if db.get_current_resource_version().await.unwrap_or(0) >= target_rv {
        return;
    }
    let sleep = task_supervisor.sleep(
        "watch_read_freshness_wait",
        crate::api::watch_stream::READ_FRESHNESS_TIMEOUT,
    );
    tokio::pin!(sleep);
    loop {
        tokio::select! {
            _ = &mut sleep => {
                tracing::warn!(
                    target_rv,
                    "watch read-freshness wait timed out; serving best-effort from local state"
                );
                return;
            }
            recv = fresh_rx.recv() => match recv {
                Ok(signal) => {
                    if signal.advances.iter().any(|advance| advance.high_rv >= target_rv) {
                        return;
                    }
                }
                Err(klights_watch::WatchSignalReceiveError::Lagged(_)) => {
                    if db.get_current_resource_version().await.unwrap_or(0) >= target_rv {
                        return;
                    }
                }
                Err(klights_watch::WatchSignalReceiveError::Closed) => return,
            },
        }
    }
}
