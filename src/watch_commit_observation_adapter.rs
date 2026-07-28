use std::collections::HashMap;
use std::sync::Arc;

#[cfg(test)]
use crate::datastore::{CommitObservation, CommitObservationSink};
#[cfg(test)]
use crate::watch::WatchBus;

pub(crate) struct WatchCommitWiring {
    #[cfg(test)]
    pub(crate) sink: Arc<dyn CommitObservationSink>,
    pub(crate) signals: Arc<dyn klights_watch::WatchSignalSubscribe>,
    pub(crate) wakeups: Arc<dyn klights_leader_api::PostCommitWakeup>,
}

pub(crate) fn new_wiring() -> WatchCommitWiring {
    let hub = Arc::new(klights_watch::WatchSignalHub::new(1024));
    let wakeups: Arc<dyn klights_leader_api::PostCommitWakeup> =
        Arc::new(WatchPostCommitWakeup::new(hub.clone()));
    WatchCommitWiring {
        #[cfg(test)]
        sink: Arc::new(WatchCommitObservationSink::new(
            wakeups.clone(),
            hub.clone(),
        )),
        signals: hub,
        wakeups,
    }
}

#[cfg(test)]
pub(crate) fn new_sink() -> Arc<WatchCommitObservationSink> {
    let hub = Arc::new(klights_watch::WatchSignalHub::new(1024));
    Arc::new(WatchCommitObservationSink::new(
        Arc::new(WatchPostCommitWakeup::new(hub.clone())),
        hub,
    ))
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
    fn new(
        wakeups: Arc<dyn klights_leader_api::PostCommitWakeup>,
        signals: Arc<dyn klights_watch::WatchSignalSubscribe>,
    ) -> Self {
        #[cfg(not(test))]
        let _ = signals;
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
    fn observe(&self, observations: &[CommitObservation]) {
        let advances = observations
            .iter()
            .map(|observation| {
                klights_leader_api::PostCommitAdvance::new(
                    &observation.api_version,
                    &observation.kind,
                    observation.namespace.clone(),
                    observation.resource_version,
                )
            })
            .collect::<Vec<_>>();
        self.wakeups.wake(&advances);
    }

    #[cfg(test)]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

pub(crate) struct WatchPostCommitWakeup {
    publisher: Arc<klights_watch::WatchSignalHub>,
}

impl WatchPostCommitWakeup {
    fn new(publisher: Arc<klights_watch::WatchSignalHub>) -> Self {
        Self { publisher }
    }
}

impl klights_leader_api::PostCommitWakeup for WatchPostCommitWakeup {
    fn wake(&self, observations: &[klights_leader_api::PostCommitAdvance]) {
        let mut grouped: HashMap<klights_watch::WatchTopic, HashMap<Option<String>, (i64, i64)>> =
            HashMap::new();
        for observation in observations {
            if observation.resource_version() <= 0 {
                continue;
            }
            let topic =
                klights_watch::WatchTopic::new(observation.api_version(), observation.kind());
            let entry = grouped
                .entry(topic)
                .or_default()
                .entry(observation.namespace().map(str::to_string))
                .or_insert((
                    observation.resource_version(),
                    observation.resource_version(),
                ));
            entry.0 = entry.0.min(observation.resource_version());
            entry.1 = entry.1.max(observation.resource_version());
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

    fn wake_namespace_contents(&self, namespace: &str, resource_version: i64) {
        self.publisher
            .publish_namespace_advance(namespace, resource_version);
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

    #[tokio::test]
    async fn distinct_external_engine_endpoint_wakes_local_watch_over_transport() {
        enum TransportWakeup {
            Advances(Vec<klights_leader_api::PostCommitAdvance>),
            NamespaceContents(String, i64),
        }
        struct RemoteWakeupEndpoint {
            sender: tokio::sync::mpsc::UnboundedSender<TransportWakeup>,
        }
        impl klights_leader_api::PostCommitWakeup for RemoteWakeupEndpoint {
            fn wake(&self, advances: &[klights_leader_api::PostCommitAdvance]) {
                self.sender
                    .send(TransportWakeup::Advances(advances.to_vec()))
                    .expect("local transport endpoint must remain connected");
            }

            fn wake_namespace_contents(&self, namespace: &str, resource_version: i64) {
                self.sender
                    .send(TransportWakeup::NamespaceContents(
                        namespace.to_string(),
                        resource_version,
                    ))
                    .expect("local transport endpoint must remain connected");
            }
        }
        struct SimulatedExternalEngine {
            wakeups: Arc<dyn klights_leader_api::PostCommitWakeup>,
        }
        impl SimulatedExternalEngine {
            fn commit_config_map(&self) {
                self.wakeups
                    .wake(&[klights_leader_api::PostCommitAdvance::new(
                        "v1",
                        "ConfigMap",
                        Some("default".to_string()),
                        41,
                    )]);
            }
        }

        let hub = Arc::new(klights_watch::WatchSignalHub::new(4));
        let local_wakeup = WatchPostCommitWakeup::new(hub.clone());
        let topic = klights_watch::WatchTopic::new("v1", "ConfigMap");
        let mut local_watch = hub.subscribe(topic.clone());
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let external_engine = SimulatedExternalEngine {
            wakeups: Arc::new(RemoteWakeupEndpoint { sender }),
        };

        let bridge = async {
            let wakeup = receiver
                .recv()
                .await
                .expect("external endpoint must deliver one commit");
            match wakeup {
                TransportWakeup::Advances(advances) => {
                    klights_leader_api::PostCommitWakeup::wake(&local_wakeup, &advances);
                }
                TransportWakeup::NamespaceContents(namespace, resource_version) => {
                    klights_leader_api::PostCommitWakeup::wake_namespace_contents(
                        &local_wakeup,
                        &namespace,
                        resource_version,
                    );
                }
            }
        };
        let remote_commit = async {
            external_engine.commit_config_map();
        };
        tokio::join!(bridge, remote_commit);

        assert_eq!(
            local_watch.recv().await,
            Ok(klights_watch::WatchSignal {
                topic,
                advances: vec![klights_watch::WatchAdvance {
                    namespace: Some("default".to_string()),
                    low_rv: 41,
                    high_rv: 41,
                }],
            })
        );
    }
}
