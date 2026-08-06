use std::sync::Arc;

#[cfg(any(test, feature = "pod-repository-test-support"))]
use crate::datastore::CommitObservationSink;
#[cfg(any(test, feature = "pod-repository-test-support"))]
use klights_cluster_store::StagedPostCommit;
#[cfg(any(test, feature = "pod-repository-test-support"))]
use klights_watch::WatchBus;

pub(crate) struct WatchCommitWiring {
    #[cfg(any(test, feature = "pod-repository-test-support"))]
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
        #[cfg(any(test, feature = "pod-repository-test-support"))]
        sink: Arc::new(WatchCommitObservationSink::new(
            wakeups.clone(),
            hub.clone(),
        )),
        signals: hub,
        wakeups,
        follower_progress,
    }
}

#[cfg(any(test, feature = "pod-repository-test-support"))]
pub(crate) fn new_sink() -> Arc<WatchCommitObservationSink> {
    let hub = Arc::new(klights_watch::WatchSignalHub::new(1024));
    Arc::new(WatchCommitObservationSink::new(
        Arc::new(klights_watch::PostCommitWatchWakeup::new(hub.clone())),
        hub,
    ))
}

#[cfg(any(test, feature = "pod-repository-test-support"))]
pub(crate) fn test_signal_source(
    db: &crate::datastore::DatastoreHandle,
) -> Arc<dyn klights_watch::WatchSignalSubscribe> {
    let sink = db.commit_observation_sink();
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

#[cfg(any(test, feature = "pod-repository-test-support"))]
pub(crate) struct WatchCommitObservationSink {
    wakeups: Arc<dyn klights_leader_api::PostCommitWakeup>,
    #[cfg(any(test, feature = "pod-repository-test-support"))]
    signals: Arc<dyn klights_watch::WatchSignalSubscribe>,
    #[cfg(any(test, feature = "pod-repository-test-support"))]
    bus: WatchBus,
}

#[cfg(any(test, feature = "pod-repository-test-support"))]
impl WatchCommitObservationSink {
    fn new(
        wakeups: Arc<dyn klights_leader_api::PostCommitWakeup>,
        signals: Arc<dyn klights_watch::WatchSignalSubscribe>,
    ) -> Self {
        Self {
            wakeups,
            #[cfg(any(test, feature = "pod-repository-test-support"))]
            signals,
            #[cfg(any(test, feature = "pod-repository-test-support"))]
            bus: WatchBus::new(1024),
        }
    }

    #[cfg(any(test, feature = "pod-repository-test-support"))]
    pub(crate) fn signal_source(&self) -> Arc<dyn klights_watch::WatchSignalSubscribe> {
        self.signals.clone()
    }
}

#[cfg(any(test, feature = "pod-repository-test-support"))]
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
        #[cfg(any(test, feature = "pod-repository-test-support"))]
        for event in observations
            .iter()
            .filter_map(crate::datastore::staged_test_event)
        {
            self.bus.publish(event);
        }
    }

    #[cfg(any(test, feature = "pod-repository-test-support"))]
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

#[cfg(any(test, feature = "pod-repository-test-support"))]
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

#[cfg(any(test, feature = "pod-repository-test-support"))]
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

#[cfg(any(test, feature = "pod-repository-test-support"))]
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
    use crate::datastore::DatastoreBackend;

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

    async fn assert_success_only(store: &dyn DatastoreBackend, sink: &RecordingSink) {
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
        let store =
            crate::datastore::sqlite::Datastore::new_in_memory_with_watch_and_executor_with_sink(
                executor,
                sink.clone(),
                crate::bootstrap::composition_adapters::outbox_response_codec_adapter::new_codec(),
                std::sync::Arc::new(klights_supervisor::SystemWallClock),
            )
            .await
            .unwrap();
        assert_success_only(&store, sink.as_ref()).await;
    }

    #[tokio::test]
    async fn redb_emits_commit_observations_only_after_successful_commit() {
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(Default::default()));
        let sink = Arc::new(RecordingSink::default());
        let store = crate::datastore::redb::RedbDatastore::new_in_memory_with_supervisor_and_sink(
            supervisor,
            sink.clone(),
            std::sync::Arc::new(klights_supervisor::SystemWallClock),
        )
        .await
        .unwrap();
        assert_success_only(&store, sink.as_ref()).await;
    }
}
