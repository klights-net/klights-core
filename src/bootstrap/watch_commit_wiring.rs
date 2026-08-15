use std::sync::Arc;

#[cfg(test)]
use klights_cluster_store::CommitObservationSink;
#[cfg(test)]
use klights_cluster_store::StagedPostCommit;
#[cfg(test)]
use klights_watch::WatchBus;

pub(crate) struct WatchCommitWiring {
    #[cfg(test)]
    pub(crate) sink: Arc<dyn CommitObservationSink>,
    pub(crate) signals: Arc<dyn klights_watch::WatchSignalSubscribe>,
    pub(crate) wakeups: Arc<dyn klights_leader_api::PostCommitWakeup>,
    pub(crate) follower_progress: Arc<klights_replication::FollowerProgressHub>,
}

pub(crate) fn new_wiring() -> WatchCommitWiring {
    let hub = Arc::new(klights_watch::WatchSignalHub::new(1024));
    let watch_wakeups: Arc<dyn klights_leader_api::PostCommitWakeup> =
        Arc::new(klights_watch::PostCommitWatchWakeup::new(hub.clone()));
    let follower_progress = Arc::new(klights_replication::FollowerProgressHub::new(0));
    let wakeups: Arc<dyn klights_leader_api::PostCommitWakeup> = Arc::new(ActivePostCommitWakeup {
        watch: watch_wakeups,
        follower_progress: follower_progress.clone(),
    });
    WatchCommitWiring {
        #[cfg(test)]
        sink: Arc::new(WatchCommitObservationSink::new(
            wakeups.clone(),
            hub.clone(),
        )),
        signals: hub,
        wakeups,
        follower_progress,
    }
}

/// Test-only conversion for the root watch harness.  The durable post-commit
/// record remains owned by the cluster-store contract; this helper merely
/// projects its optional test event without reintroducing a datastore facade.
#[cfg(test)]
pub(crate) fn staged_test_event(pending: &StagedPostCommit) -> Option<klights_watch::WatchEvent> {
    let staged = pending.test_event()?;
    let mut event = klights_watch::WatchEvent::from_type(
        staged.event_type(),
        staged.resource().data.as_ref().clone(),
    );
    event.encoded_payload =
        staged
            .encoded_json()
            .cloned()
            .map(|bytes| klights_watch::EncodedWatchPayload {
                content_type: klights_watch::WatchContentType::Json,
                bytes,
            });
    Some(event)
}

#[cfg(test)]
pub(crate) fn staged_post_commit_from_event(event: klights_watch::WatchEvent) -> StagedPostCommit {
    let resource = klights_cluster_core::Resource::try_from_data(event.object.clone())
        .expect("test watch event must carry canonical resource identity");
    let encoded_json = event
        .encoded_payload
        .as_ref()
        .filter(|payload| payload.content_type == klights_watch::WatchContentType::Json)
        .map(|payload| payload.bytes.clone());
    StagedPostCommit::new(
        &resource.api_version,
        &resource.kind,
        resource.namespace.as_deref(),
        resource.resource_version,
    )
    .with_test_event(event.event_type.to_string(), resource, encoded_json)
}

#[cfg(test)]
pub(crate) fn new_sink() -> Arc<WatchCommitObservationSink> {
    let hub = Arc::new(klights_watch::WatchSignalHub::new(1024));
    Arc::new(WatchCommitObservationSink::new(
        Arc::new(klights_watch::PostCommitWatchWakeup::new(hub.clone())),
        hub,
    ))
}

#[cfg(test)]
pub(crate) fn test_signal_source(
    db: &klights_cluster_datastore::sqlite::embedded::Datastore,
) -> Arc<dyn klights_watch::WatchSignalSubscribe> {
    let sink = db
        .commit_observation_sink()
        .expect("test datastore watch sink");
    sink.as_any()
        .downcast_ref::<WatchCommitObservationSink>()
        .expect("test datastore watch sink")
        .signal_source()
}

pub(crate) fn subscribe(
    source: &dyn klights_watch::WatchSignalSubscribe,
    topic: klights_watch::WatchTopic,
) -> klights_watch::WatchSignalReceiver {
    source.subscribe(topic)
}

#[cfg(test)]
pub(crate) struct WatchCommitObservationSink {
    wakeups: Arc<dyn klights_leader_api::PostCommitWakeup>,
    #[cfg(test)]
    signals: Arc<dyn klights_watch::WatchSignalSubscribe>,
    #[cfg(test)]
    bus: WatchBus,
}

#[cfg(test)]
impl WatchCommitObservationSink {
    #[cfg(test)]
    fn new(
        wakeups: Arc<dyn klights_leader_api::PostCommitWakeup>,
        signals: Arc<dyn klights_watch::WatchSignalSubscribe>,
    ) -> Self {
        Self {
            wakeups,
            #[cfg(test)]
            signals,
            #[cfg(test)]
            bus: WatchBus::new(1024),
        }
    }

    #[cfg(test)]
    pub(crate) fn signal_source(&self) -> Arc<dyn klights_watch::WatchSignalSubscribe> {
        self.signals.clone()
    }
}

#[cfg(test)]
impl CommitObservationSink for WatchCommitObservationSink {
    fn observe(&self, observations: &[StagedPostCommit]) {
        let advances = observations
            .iter()
            .map(|observation| {
                klights_leader_api::PostCommitAdvance::new(
                    observation.api_version(),
                    observation.kind(),
                    observation.namespace().map(str::to_string),
                    observation.resource_version(),
                )
            })
            .collect::<Vec<_>>();
        self.wakeups.wake(&advances);
        #[cfg(test)]
        for event in observations.iter().filter_map(staged_test_event) {
            self.bus.publish(event);
        }
    }

    #[cfg(test)]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

struct ActivePostCommitWakeup {
    watch: Arc<dyn klights_leader_api::PostCommitWakeup>,
    follower_progress: Arc<klights_replication::FollowerProgressHub>,
}

impl klights_leader_api::PostCommitWakeup for ActivePostCommitWakeup {
    fn wake(&self, observations: &[klights_leader_api::PostCommitAdvance]) {
        self.watch.wake(observations);
        if let Some(resource_version) = observations
            .iter()
            .map(klights_leader_api::PostCommitAdvance::resource_version)
            .max()
        {
            self.follower_progress.advance(resource_version);
        }
    }

    fn wake_namespace_contents(&self, namespace: &str, resource_version: i64) {
        self.watch
            .wake_namespace_contents(namespace, resource_version);
        self.follower_progress.advance(resource_version);
    }
}

#[cfg(test)]
pub(crate) fn publish_test_events(
    sink: &dyn CommitObservationSink,
    events: Vec<klights_watch::WatchEvent>,
) {
    if let Some(sink) = sink.as_any().downcast_ref::<WatchCommitObservationSink>() {
        for event in events {
            sink.bus.publish(event);
        }
    }
}

#[cfg(test)]
pub(crate) fn subscribe_test_events(
    sink: &dyn CommitObservationSink,
    topic: klights_watch::WatchTopic,
) -> tokio::sync::broadcast::Receiver<klights_watch::WatchEvent> {
    sink.as_any()
        .downcast_ref::<WatchCommitObservationSink>()
        .expect("test datastore watch sink")
        .bus
        .subscribe(topic)
}

#[cfg(test)]
pub(crate) fn subscribe_test_events_many(
    sink: &dyn CommitObservationSink,
    topics: Vec<klights_watch::WatchTopic>,
) -> klights_watch::WatchReceiver {
    sink.as_any()
        .downcast_ref::<WatchCommitObservationSink>()
        .expect("test datastore watch sink")
        .bus
        .subscribe_many(topics)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use serde_json::json;

    use super::*;
    use klights_cluster_store::ClusterResourceMutation;

    #[derive(Default)]
    struct RecordingSink {
        observations: Mutex<Vec<StagedPostCommit>>,
    }

    impl CommitObservationSink for RecordingSink {
        fn observe(&self, observations: &[StagedPostCommit]) {
            self.observations
                .lock()
                .unwrap()
                .extend_from_slice(observations);
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    fn config_map() -> serde_json::Value {
        json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "observed", "namespace": "default"},
            "data": {"key": "value"}
        })
    }

    #[test]
    fn durable_visible_commit_wakeup_advances_follower_progress_without_idle_work() {
        let wiring = new_wiring();
        let mut progress = wiring.follower_progress.subscribe();
        assert_eq!(*progress.borrow_and_update(), 0);
        assert!(!progress.has_changed().unwrap());

        wiring.wakeups.wake(&[
            klights_leader_api::PostCommitAdvance::new(
                "v1",
                "ConfigMap",
                Some("default".to_string()),
                12,
            ),
            klights_leader_api::PostCommitAdvance::new(
                "v1",
                "Secret",
                Some("default".to_string()),
                8,
            ),
        ]);
        assert!(progress.has_changed().unwrap());
        assert_eq!(*progress.borrow_and_update(), 12);

        wiring
            .wakeups
            .wake(&[klights_leader_api::PostCommitAdvance::new(
                "v1",
                "ConfigMap",
                Some("default".to_string()),
                12,
            )]);
        assert!(!progress.has_changed().unwrap());

        wiring.wakeups.wake_namespace_contents("default", 13);
        assert!(progress.has_changed().unwrap());
        assert_eq!(*progress.borrow_and_update(), 13);
        assert!(!progress.has_changed().unwrap());
    }

    async fn assert_success_only(store: &dyn ClusterResourceMutation, sink: &RecordingSink) {
        store
            .create_resource("v1", "ConfigMap", Some("default"), "observed", config_map())
            .await
            .expect("first committed create");
        assert_eq!(sink.observations.lock().unwrap().len(), 1);

        store
            .create_resource("v1", "ConfigMap", Some("default"), "observed", config_map())
            .await
            .expect_err("duplicate transaction must fail");
        assert_eq!(
            sink.observations.lock().unwrap().len(),
            1,
            "failed transaction must not emit an observation"
        );
    }

    async fn assert_sqlite_success_only(store: &dyn ClusterResourceMutation, sink: &RecordingSink) {
        store
            .create_resource("v1", "ConfigMap", Some("default"), "observed", config_map())
            .await
            .expect("first committed create");
        assert_eq!(sink.observations.lock().unwrap().len(), 1);

        store
            .create_resource("v1", "ConfigMap", Some("default"), "observed", config_map())
            .await
            .expect_err("duplicate transaction must fail");
        assert_eq!(
            sink.observations.lock().unwrap().len(),
            1,
            "failed transaction must not emit an observation"
        );
    }

    #[tokio::test]
    async fn sqlite_emits_commit_observations_only_after_successful_commit() {
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(Default::default()));
        let executor = klights_cluster_datastore::sqlite::open_in_memory(
            supervisor,
            "sqlite:commit-observation-test",
        )
        .await
        .unwrap();
        let sink = Arc::new(RecordingSink::default());
        let store = klights_cluster_datastore::sqlite::embedded::Datastore::new_in_memory_with_watch_and_executor_with_sink(
            executor,
            sink.clone(),
            crate::bootstrap::composition_adapters::outbox_response_codec_adapter::new_codec(),
            std::sync::Arc::new(klights_supervisor::SystemWallClock),
        )
        .await
        .unwrap();
        assert_sqlite_success_only(&store, sink.as_ref()).await;
    }

    #[tokio::test]
    async fn redb_emits_commit_observations_only_after_successful_commit() {
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(Default::default()));
        let sink = Arc::new(RecordingSink::default());
        let store = klights_cluster_datastore::redb::embedded::RedbDatastore::new_in_memory_with_supervisor_and_sink(
            supervisor,
            sink.clone(),
            std::sync::Arc::new(klights_supervisor::SystemWallClock),
        )
        .await
        .unwrap();
        assert_success_only(&store, sink.as_ref()).await;
    }

    #[test]
    fn redb_commit_observation_fixture_uses_the_canonical_store() {
        let source = include_str!("watch_commit_wiring.rs");
        let legacy_wrapper = ["crate::datastore::", "redb::RedbDatastore"].concat();
        assert!(
            !source.contains(&legacy_wrapper),
            "the root Redb wrapper must not compose commit-observation fixtures"
        );
    }
}
