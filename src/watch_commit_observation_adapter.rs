use std::collections::HashMap;
use std::sync::Arc;

use crate::datastore::{CommitObservation, CommitObservationSink};
#[cfg(test)]
use crate::watch::WatchBus;

pub(crate) struct WatchCommitWiring {
    pub(crate) sink: Arc<dyn CommitObservationSink>,
    pub(crate) signals: Arc<dyn klights_watch::WatchSignalSubscribe>,
}

pub(crate) fn new_wiring() -> WatchCommitWiring {
    let hub = Arc::new(klights_watch::WatchSignalHub::new(1024));
    let sink = Arc::new(WatchCommitObservationSink::new(hub.clone(), hub.clone()));
    WatchCommitWiring { sink, signals: hub }
}

#[cfg(test)]
pub(crate) fn new_sink() -> Arc<WatchCommitObservationSink> {
    let hub = Arc::new(klights_watch::WatchSignalHub::new(1024));
    Arc::new(WatchCommitObservationSink::new(hub.clone(), hub))
}

#[cfg(test)]
pub(crate) fn test_signal_source(
    db: &crate::datastore::DatastoreHandle,
) -> Arc<dyn klights_watch::WatchSignalSubscribe> {
    let sink = db.commit_observation_sink();
    sink.as_any()
        .downcast_ref::<WatchCommitObservationSink>()
        .expect("test datastore watch sink")
        .signal_source()
}

#[cfg(test)]
pub(crate) fn subscribe_from_db(
    db: &crate::datastore::DatastoreHandle,
    topic: klights_watch::WatchTopic,
) -> klights_watch::WatchSignalReceiver {
    test_signal_source(db).subscribe(topic)
}

pub(crate) fn subscribe(
    source: &dyn klights_watch::WatchSignalSubscribe,
    topic: klights_watch::WatchTopic,
) -> klights_watch::WatchSignalReceiver {
    source.subscribe(topic)
}

#[cfg(test)]
pub(crate) fn subscribe_from_sink(
    sink: &dyn CommitObservationSink,
    topic: klights_watch::WatchTopic,
) -> klights_watch::WatchSignalReceiver {
    sink.as_any()
        .downcast_ref::<WatchCommitObservationSink>()
        .expect("cluster datastore was not composed with the root watch observation sink")
        .signals
        .subscribe(topic)
}

pub(crate) struct WatchCommitObservationSink {
    publisher: Arc<dyn klights_watch::WatchSignalPublish>,
    #[cfg(test)]
    signals: Arc<dyn klights_watch::WatchSignalSubscribe>,
    #[cfg(test)]
    bus: WatchBus,
}

impl WatchCommitObservationSink {
    fn new(
        publisher: Arc<dyn klights_watch::WatchSignalPublish>,
        signals: Arc<dyn klights_watch::WatchSignalSubscribe>,
    ) -> Self {
        #[cfg(not(test))]
        let _ = signals;
        Self {
            publisher,
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

impl CommitObservationSink for WatchCommitObservationSink {
    fn observe(&self, observations: &[CommitObservation]) {
        let mut grouped: HashMap<klights_watch::WatchTopic, HashMap<Option<String>, (i64, i64)>> =
            HashMap::new();
        for observation in observations {
            if observation.resource_version <= 0 {
                continue;
            }
            let topic = klights_watch::WatchTopic::new(&observation.api_version, &observation.kind);
            let entry = grouped
                .entry(topic)
                .or_default()
                .entry(observation.namespace.clone())
                .or_insert((observation.resource_version, observation.resource_version));
            entry.0 = entry.0.min(observation.resource_version);
            entry.1 = entry.1.max(observation.resource_version);
        }
        let mut signals = Vec::new();
        for (topic, namespace_rvs) in grouped {
            let mut advances = namespace_rvs
                .into_iter()
                .map(
                    |(namespace, (low_rv, high_rv))| klights_watch::WatchAdvance {
                        namespace,
                        low_rv,
                        high_rv,
                    },
                )
                .collect::<Vec<_>>();
            advances.sort_by(|left, right| left.namespace.cmp(&right.namespace));
            for chunk in advances.chunks(klights_watch::DEFAULT_WATCH_ADVANCE_GROUP_LIMIT) {
                signals.push(klights_watch::WatchSignal {
                    topic: topic.clone(),
                    advances: chunk.to_vec(),
                });
            }
        }
        signals.sort_by(|left, right| {
            (left.topic.api_version(), left.topic.kind())
                .cmp(&(right.topic.api_version(), right.topic.kind()))
        });
        for signal in signals {
            self.publisher.publish(signal);
        }
    }

    #[cfg(test)]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
pub(crate) fn publish_test_events(
    sink: &dyn CommitObservationSink,
    events: Vec<crate::watch::WatchEvent>,
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
) -> tokio::sync::broadcast::Receiver<crate::watch::WatchEvent> {
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
) -> crate::watch::WatchReceiver {
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
        observations: Mutex<Vec<CommitObservation>>,
    }

    impl CommitObservationSink for RecordingSink {
        fn observe(&self, observations: &[CommitObservation]) {
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
        let executor = crate::sqlite_boundary::DbExecutor::open_in_memory(
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
                crate::outbox_response_codec_adapter::new_codec(),
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
        )
        .await
        .unwrap();
        assert_success_only(&store, sink.as_ref()).await;
    }
}
