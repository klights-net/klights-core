//! Per-topic watch fan-out (`WatchBus`).
//!
//! Today every watch subscriber — each HTTP watch, each gRPC server stream,
//! the scheduler, node_subnet, node_lifecycle, crd, cronjob_scheduler, and the
//! per-node kubelet pod watcher — subscribes to ONE global
//! `broadcast::channel(8192)` carrying every committed event of every
//! `(apiVersion, kind)`, then filters after `recv()`. With N subscribers and M
//! events that is N·M wakeups + N·M decode/filter even when each subscriber
//! cares about a single kind, and each subscriber holds an 8192-slot buffer.
//!
//! [`WatchBus`] routes at publish time: one broadcast sender per **topic**,
//! where a topic is the K8s watch scope `(apiVersion, kind)`. Publishers route
//! each event to exactly its topic; subscribers register only for the topic(s)
//! they want and never see anything else. Namespace and label/field selectors
//! stay consumer-side (too dynamic to be channels) but now run against a tiny
//! per-kind stream. Topics are created lazily and collected once they have zero
//! receivers, so an idle cluster holds no buffers (HR #1 / #3).
//!
//! This module is the publish/subscribe surface for Kubernetes watch events.
//! Datastore mutation paths publish through it after commit, and production
//! consumers subscribe by topic instead of receiving the full cluster firehose.

#[cfg(any(test, feature = "integration-test-harness"))]
use std::collections::HashMap;
#[cfg(any(test, feature = "integration-test-harness"))]
use std::sync::Mutex;

#[cfg(any(test, feature = "integration-test-harness"))]
use futures::future::select_all;
#[cfg(any(test, feature = "integration-test-harness"))]
use tokio::sync::broadcast;
#[cfg(any(test, feature = "integration-test-harness"))]
use tokio::sync::broadcast::error::RecvError;

#[cfg(test)]
use crate::{DEFAULT_WATCH_ADVANCE_GROUP_LIMIT, WatchAdvance, WatchSignalTryReceiveError};
use crate::{WatchEvent, WatchSignal, WatchSignalReceiver, WatchTopic};

impl crate::WatchSignalEvent for WatchEvent {
    fn watch_api_version(&self) -> Option<&str> {
        self.object
            .get("apiVersion")
            .and_then(|value| value.as_str())
    }

    fn watch_kind(&self) -> Option<&str> {
        self.object.get("kind").and_then(|value| value.as_str())
    }

    fn watch_namespace(&self) -> Option<&str> {
        self.object
            .pointer("/metadata/namespace")
            .and_then(|value| value.as_str())
    }

    fn watch_resource_version(&self) -> Option<i64> {
        self.resource_version()
    }
}

#[cfg(any(test, feature = "integration-test-harness"))]
fn event_topic(event: &WatchEvent) -> Option<WatchTopic> {
    Some(WatchTopic::new(
        event.object.get("apiVersion")?.as_str()?,
        event.object.get("kind")?.as_str()?,
    ))
}

/// Per-topic broadcast fan-out. This is the only Kubernetes watch
/// publish/subscribe surface.
pub struct WatchBus {
    #[cfg(any(test, feature = "integration-test-harness"))]
    topics: Mutex<HashMap<WatchTopic, broadcast::Sender<WatchEvent>>>,
    signal_hub: crate::WatchSignalHub,
    /// Per-topic buffer capacity. Far smaller than the old global 8192/kind is
    /// viable because a topic only carries its own kind's events; the durable
    /// `watch_events` replay still backstops a lagging receiver.
    #[cfg(any(test, feature = "integration-test-harness"))]
    capacity: usize,
}

impl WatchBus {
    pub fn new(capacity: usize) -> Self {
        Self {
            #[cfg(any(test, feature = "integration-test-harness"))]
            topics: Mutex::new(HashMap::new()),
            signal_hub: crate::WatchSignalHub::new(capacity),
            #[cfg(any(test, feature = "integration-test-harness"))]
            capacity: capacity.max(1),
        }
    }

    /// Subscribe to exactly one topic. The topic sender is created lazily on
    /// first subscribe. The returned receiver only ever observes events for
    /// `topic`; drop it to release the slot (the topic self-collects on the
    /// next publish once its receiver count reaches zero).
    #[cfg(any(test, feature = "integration-test-harness"))]
    pub fn subscribe(&self, topic: WatchTopic) -> broadcast::Receiver<WatchEvent> {
        let mut topics = self.lock();
        topics
            .entry(topic)
            .or_insert_with(|| broadcast::channel(self.capacity).0)
            .subscribe()
    }

    #[cfg(any(test, feature = "integration-test-harness"))]
    pub fn subscribe_many(&self, topics: impl IntoIterator<Item = WatchTopic>) -> WatchReceiver {
        WatchReceiver::new(
            topics
                .into_iter()
                .map(|topic| self.subscribe(topic))
                .collect(),
        )
    }

    pub fn subscribe_signals(&self, topic: WatchTopic) -> WatchSignalReceiver {
        self.signal_hub.subscribe(topic)
    }

    /// Route `event` to its own `(apiVersion, kind)` topic. A no-op when no
    /// subscriber is registered for that topic (idle-silent: no topic, no
    /// wakeups). Once a topic's last receiver has dropped, the send fails and
    /// the topic is collected so memory tracks only active kinds.
    #[cfg(any(test, feature = "integration-test-harness"))]
    pub fn publish(&self, event: WatchEvent) {
        let Some(topic) = event_topic(&event) else {
            return;
        };
        let mut topics = self.lock();
        let Some(sender) = topics.get(&topic) else {
            return;
        };
        // `send` errors only when there are no receivers; in that case the
        // topic is idle and is removed (re-created on the next subscribe).
        if sender.send(event).is_err() || sender.receiver_count() == 0 {
            topics.remove(&topic);
        }
    }

    pub fn publish_signal(&self, signal: WatchSignal) {
        self.signal_hub.publish(signal);
    }

    /// Test/observability seam: number of live topics currently held.
    #[cfg(any(test, feature = "integration-test-harness"))]
    pub fn topic_count(&self) -> usize {
        self.lock().len()
    }

    #[cfg(any(test, feature = "integration-test-harness"))]
    fn lock(
        &self,
    ) -> std::sync::MutexGuard<'_, HashMap<WatchTopic, broadcast::Sender<WatchEvent>>> {
        self.topics
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(any(test, feature = "integration-test-harness"))]
pub struct WatchReceiver {
    receivers: Vec<broadcast::Receiver<WatchEvent>>,
}

#[cfg(any(test, feature = "integration-test-harness"))]
impl WatchReceiver {
    pub fn new(receivers: Vec<broadcast::Receiver<WatchEvent>>) -> Self {
        Self { receivers }
    }

    pub fn from_receiver(receiver: broadcast::Receiver<WatchEvent>) -> Self {
        Self {
            receivers: vec![receiver],
        }
    }

    pub async fn recv(&mut self) -> Result<WatchEvent, RecvError> {
        if self.receivers.is_empty() {
            return Err(RecvError::Closed);
        }
        if self.receivers.len() == 1 {
            return self.receivers[0].recv().await;
        }

        let futures = self
            .receivers
            .iter_mut()
            .map(|receiver| Box::pin(receiver.recv()));
        let (result, _index, _remaining) = select_all(futures).await;
        result
    }
}

#[cfg(any(test, feature = "integration-test-harness"))]
impl From<broadcast::Receiver<WatchEvent>> for WatchReceiver {
    fn from(receiver: broadcast::Receiver<WatchEvent>) -> Self {
        Self::from_receiver(receiver)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn event(api_version: &str, kind: &str, name: &str) -> WatchEvent {
        WatchEvent::added(json!({
            "apiVersion": api_version,
            "kind": kind,
            "metadata": {"name": name},
        }))
    }

    fn name_of(event: &WatchEvent) -> Option<String> {
        event
            .object
            .pointer("/metadata/name")
            .and_then(|v| v.as_str())
            .map(str::to_string)
    }

    #[test]
    fn bus_delivers_only_subscribed_topic() {
        let bus = WatchBus::new(16);
        let mut pod_rx = bus.subscribe(WatchTopic::new("v1", "Pod"));

        // ConfigMap traffic with no ConfigMap subscriber: the Pod subscriber
        // must observe zero wakeups.
        for i in 0..5 {
            bus.publish(event("v1", "ConfigMap", &format!("cm-{i}")));
        }
        assert!(
            matches!(
                pod_rx.try_recv(),
                Err(broadcast::error::TryRecvError::Empty)
            ),
            "a Pod subscriber must not receive ConfigMap events"
        );

        // A Pod event is delivered.
        bus.publish(event("v1", "Pod", "p0"));
        let got = pod_rx.try_recv().expect("pod event must be delivered");
        assert_eq!(name_of(&got).as_deref(), Some("p0"));
    }

    #[test]
    fn bus_routes_event_to_its_topic_by_apiversion_kind() {
        let bus = WatchBus::new(16);
        let mut deploy_rx = bus.subscribe(WatchTopic::new("apps/v1", "Deployment"));
        let mut pod_rx = bus.subscribe(WatchTopic::new("v1", "Pod"));

        bus.publish(event("apps/v1", "Deployment", "web"));

        let got = deploy_rx
            .try_recv()
            .expect("apps/v1 Deployment event must reach the Deployment topic");
        assert_eq!(name_of(&got).as_deref(), Some("web"));
        assert!(
            matches!(
                pod_rx.try_recv(),
                Err(broadcast::error::TryRecvError::Empty)
            ),
            "a Deployment event must not reach the v1 Pod topic"
        );
    }

    #[test]
    fn bus_topic_self_collects_when_no_receivers() {
        let bus = WatchBus::new(16);
        let rx = bus.subscribe(WatchTopic::new("v1", "Pod"));
        assert_eq!(bus.topic_count(), 1);

        drop(rx);
        // Publishing to the now-receiverless topic collects it (memory bound).
        bus.publish(event("v1", "Pod", "p0"));
        assert_eq!(
            bus.topic_count(),
            0,
            "a topic with no receivers must be collected on publish"
        );

        // Re-created on the next subscribe.
        let _rx2 = bus.subscribe(WatchTopic::new("v1", "Pod"));
        assert_eq!(bus.topic_count(), 1);
    }

    #[test]
    fn publish_to_unsubscribed_topic_is_idle_noop() {
        let bus = WatchBus::new(16);
        // No subscribers at all: publishing creates no topic and never panics.
        bus.publish(event("v1", "Secret", "s0"));
        assert_eq!(bus.topic_count(), 0);
    }

    #[test]
    fn unroutable_event_is_dropped() {
        let bus = WatchBus::new(16);
        let _rx = bus.subscribe(WatchTopic::new("v1", "Pod"));
        // Event with no apiVersion/kind cannot be routed; must be a no-op.
        bus.publish(WatchEvent::added(json!({"metadata": {"name": "x"}})));
        assert_eq!(bus.topic_count(), 1);
    }

    #[test]
    fn watch_bus_signal_subscriber_receives_per_topic_advance() {
        let bus = WatchBus::new(16);
        let topic = WatchTopic::new("v1", "Pod");
        let mut rx = bus.subscribe_signals(topic.clone());

        bus.publish_signal(WatchSignal {
            topic,
            advances: vec![WatchAdvance {
                namespace: Some("default".to_string()),
                low_rv: 42,
                high_rv: 42,
            }],
        });

        let got = rx.try_recv().expect("signal must be delivered");
        assert_eq!(got.advances.len(), 1);
        assert_eq!(got.advances[0].high_rv, 42);
    }

    #[test]
    fn watch_bus_signal_does_not_reach_other_topics() {
        let bus = WatchBus::new(16);
        let mut cm_rx = bus.subscribe_signals(WatchTopic::new("v1", "ConfigMap"));

        bus.publish_signal(WatchSignal {
            topic: WatchTopic::new("v1", "Pod"),
            advances: vec![WatchAdvance {
                namespace: Some("default".to_string()),
                low_rv: 42,
                high_rv: 42,
            }],
        });

        assert!(matches!(
            cm_rx.try_recv(),
            Err(WatchSignalTryReceiveError::Empty)
        ));
    }

    fn watch_event_for_signal(
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        rv: i64,
    ) -> WatchEvent {
        let mut metadata = json!({
            "name": name,
            "resourceVersion": rv.to_string(),
        });
        if let Some(namespace) = namespace {
            metadata["namespace"] = serde_json::Value::String(namespace.to_string());
        }
        WatchEvent::modified(json!({
            "apiVersion": api_version,
            "kind": kind,
            "metadata": metadata,
        }))
    }

    #[test]
    fn watch_signal_from_events_groups_by_topic_and_namespace() {
        let events = [
            watch_event_for_signal("v1", "Pod", Some("default"), "pod-a", 10),
            watch_event_for_signal("v1", "Pod", Some("default"), "pod-b", 12),
            watch_event_for_signal("v1", "Pod", Some("kube-system"), "pod-c", 11),
            watch_event_for_signal("v1", "ConfigMap", Some("default"), "cm-a", 13),
        ];

        let mut signals = WatchSignal::from_events(events.iter());
        signals.sort_by(|left, right| {
            (
                left.topic.api_version(),
                left.topic.kind(),
                left.advances.len(),
            )
                .cmp(&(
                    right.topic.api_version(),
                    right.topic.kind(),
                    right.advances.len(),
                ))
        });

        let pod_signal = signals
            .iter()
            .find(|signal| signal.topic == WatchTopic::new("v1", "Pod"))
            .expect("pod signal");
        assert_eq!(pod_signal.advances.len(), 2);
        assert!(pod_signal.advances.contains(&WatchAdvance {
            namespace: Some("default".to_string()),
            low_rv: 10,
            high_rv: 12,
        }));
        assert!(pod_signal.advances.contains(&WatchAdvance {
            namespace: Some("kube-system".to_string()),
            low_rv: 11,
            high_rv: 11,
        }));

        let cm_signal = signals
            .iter()
            .find(|signal| signal.topic == WatchTopic::new("v1", "ConfigMap"))
            .expect("configmap signal");
        assert_eq!(
            cm_signal.advances,
            vec![WatchAdvance {
                namespace: Some("default".to_string()),
                low_rv: 13,
                high_rv: 13,
            }]
        );
    }

    #[test]
    fn watch_signal_from_events_chunks_advances_by_group_limit() {
        let events = [
            watch_event_for_signal("v1", "Pod", Some("ns-a"), "pod-a", 10),
            watch_event_for_signal("v1", "Pod", Some("ns-b"), "pod-b", 11),
            watch_event_for_signal("v1", "Pod", Some("ns-c"), "pod-c", 12),
            watch_event_for_signal("v1", "Pod", Some("ns-d"), "pod-d", 13),
        ];

        let signals = WatchSignal::from_events(events.iter());
        let pod_signals = signals
            .iter()
            .filter(|signal| signal.topic == WatchTopic::new("v1", "Pod"))
            .collect::<Vec<_>>();

        assert_eq!(pod_signals.len(), 2);
        assert!(
            pod_signals
                .iter()
                .all(|signal| signal.advances.len() <= DEFAULT_WATCH_ADVANCE_GROUP_LIMIT)
        );
        assert_eq!(
            pod_signals
                .iter()
                .flat_map(|signal| signal.advances.iter())
                .count(),
            4
        );
    }
}
