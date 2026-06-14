//! bug-grpc Task D: per-topic watch fan-out (`WatchBus`).
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

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use futures::future::select_all;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;

use super::events::WatchEvent;

/// The K8s watch scope a subscriber registers for: a `(apiVersion, kind)` pair.
/// `Arc<str>` keeps clones cheap (topics are hashed and cloned per lookup).
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct WatchTopic {
    api_version: Arc<str>,
    kind: Arc<str>,
}

impl WatchTopic {
    pub fn new(api_version: impl AsRef<str>, kind: impl AsRef<str>) -> Self {
        Self {
            api_version: Arc::from(api_version.as_ref()),
            kind: Arc::from(kind.as_ref()),
        }
    }

    pub fn api_version(&self) -> &str {
        &self.api_version
    }

    pub fn kind(&self) -> &str {
        &self.kind
    }

    /// The topic an event belongs to, read from its object's `apiVersion` and
    /// `kind`. `None` when the event object carries neither (cannot be routed).
    fn of_event(event: &WatchEvent) -> Option<Self> {
        let api_version = event.object.get("apiVersion").and_then(|v| v.as_str())?;
        let kind = event.object.get("kind").and_then(|v| v.as_str())?;
        Some(Self::new(api_version, kind))
    }
}

/// Per-topic broadcast fan-out. This is the only Kubernetes watch
/// publish/subscribe surface.
pub struct WatchBus {
    topics: Mutex<HashMap<WatchTopic, broadcast::Sender<WatchEvent>>>,
    /// Per-topic buffer capacity. Far smaller than the old global 8192/kind is
    /// viable because a topic only carries its own kind's events; the durable
    /// `watch_events` replay still backstops a lagging receiver.
    capacity: usize,
}

impl WatchBus {
    pub fn new(capacity: usize) -> Self {
        Self {
            topics: Mutex::new(HashMap::new()),
            capacity: capacity.max(1),
        }
    }

    /// Subscribe to exactly one topic. The topic sender is created lazily on
    /// first subscribe. The returned receiver only ever observes events for
    /// `topic`; drop it to release the slot (the topic self-collects on the
    /// next publish once its receiver count reaches zero).
    pub fn subscribe(&self, topic: WatchTopic) -> broadcast::Receiver<WatchEvent> {
        let mut topics = self.lock();
        topics
            .entry(topic)
            .or_insert_with(|| broadcast::channel(self.capacity).0)
            .subscribe()
    }

    pub fn subscribe_many(&self, topics: impl IntoIterator<Item = WatchTopic>) -> WatchReceiver {
        WatchReceiver::new(
            topics
                .into_iter()
                .map(|topic| self.subscribe(topic))
                .collect(),
        )
    }

    /// Route `event` to its own `(apiVersion, kind)` topic. A no-op when no
    /// subscriber is registered for that topic (idle-silent: no topic, no
    /// wakeups). Once a topic's last receiver has dropped, the send fails and
    /// the topic is collected so memory tracks only active kinds.
    pub fn publish(&self, event: WatchEvent) {
        let Some(topic) = WatchTopic::of_event(&event) else {
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

    /// Test/observability seam: number of live topics currently held.
    pub fn topic_count(&self) -> usize {
        self.lock().len()
    }

    fn lock(
        &self,
    ) -> std::sync::MutexGuard<'_, HashMap<WatchTopic, broadcast::Sender<WatchEvent>>> {
        self.topics
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

pub struct WatchReceiver {
    receivers: Vec<broadcast::Receiver<WatchEvent>>,
}

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
}
