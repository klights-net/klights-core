use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use futures::future::select_all;
use tokio::sync::broadcast;

/// One active watch topic. Namespace and selectors remain session-local so a
/// topic is shared by every watch of the same Kubernetes resource kind.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
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
}

/// Minimal event facts used to build post-commit signal batches without
/// coupling the leaf to a datastore- or transport-specific watch event.
pub trait WatchSignalEvent {
    fn watch_api_version(&self) -> Option<&str>;
    fn watch_kind(&self) -> Option<&str>;
    fn watch_namespace(&self) -> Option<&str>;
    fn watch_resource_version(&self) -> Option<i64>;
}

/// Public-RV range changed by one committed mutation batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WatchAdvance {
    pub namespace: Option<String>,
    pub low_rv: i64,
    pub high_rv: i64,
}

/// A bounded live hint. It carries no resource body; durable history is the
/// sole source of positioned events after both ordinary and lag recovery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WatchSignal {
    pub topic: WatchTopic,
    pub advances: Vec<WatchAdvance>,
}

pub const DEFAULT_WATCH_ADVANCE_GROUP_LIMIT: usize = 3;

impl WatchSignal {
    pub fn from_event<E>(event: &E) -> Option<Self>
    where
        E: WatchSignalEvent + ?Sized,
    {
        Self::from_events(std::iter::once(event)).into_iter().next()
    }

    pub fn from_events<'a, E>(events: impl IntoIterator<Item = &'a E>) -> Vec<Self>
    where
        E: WatchSignalEvent + ?Sized + 'a,
    {
        let mut grouped: HashMap<WatchTopic, HashMap<Option<String>, (i64, i64)>> = HashMap::new();
        for event in events {
            let (Some(api_version), Some(kind), Some(resource_version)) = (
                event.watch_api_version(),
                event.watch_kind(),
                event.watch_resource_version(),
            ) else {
                continue;
            };
            if resource_version <= 0 {
                continue;
            }
            let namespace = event.watch_namespace().map(str::to_owned);
            let entry = grouped
                .entry(WatchTopic::new(api_version, kind))
                .or_default()
                .entry(namespace)
                .or_insert((resource_version, resource_version));
            entry.0 = entry.0.min(resource_version);
            entry.1 = entry.1.max(resource_version);
        }

        let mut signals = Vec::new();
        for (topic, namespace_rvs) in grouped {
            let mut advances = namespace_rvs
                .into_iter()
                .map(|(namespace, (low_rv, high_rv))| WatchAdvance {
                    namespace,
                    low_rv,
                    high_rv,
                })
                .collect::<Vec<_>>();
            advances.sort_by(|left, right| left.namespace.cmp(&right.namespace));
            for chunk in advances.chunks(DEFAULT_WATCH_ADVANCE_GROUP_LIMIT) {
                signals.push(Self {
                    topic: topic.clone(),
                    advances: chunk.to_vec(),
                });
            }
        }
        signals.sort_by(|left, right| {
            (left.topic.api_version(), left.topic.kind())
                .cmp(&(right.topic.api_version(), right.topic.kind()))
        });
        signals
    }
}

/// Stable receive outcomes kept independent of Tokio's channel error type.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WatchSignalReceiveError {
    Lagged(u64),
    Closed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WatchSignalTryReceiveError {
    Empty,
    Lagged(u64),
    Closed,
}

pub type WatchSignalFuture<'a> =
    Pin<Box<dyn Future<Output = Result<WatchSignal, WatchSignalReceiveError>> + Send + 'a>>;

/// Fakeable subscription seam. Tokio broadcast types remain private to this
/// leaf rather than leaking through the public watch contract.
pub trait WatchSignalSubscription: Send {
    fn recv(&mut self) -> WatchSignalFuture<'_>;

    fn try_recv(&mut self) -> Result<WatchSignal, WatchSignalTryReceiveError> {
        Err(WatchSignalTryReceiveError::Empty)
    }
}

/// One or more topic subscriptions coordinated as one event-driven source.
pub struct WatchSignalReceiver {
    subscriptions: Vec<Box<dyn WatchSignalSubscription>>,
}

impl WatchSignalReceiver {
    pub fn new(receivers: Vec<Self>) -> Self {
        Self {
            subscriptions: receivers
                .into_iter()
                .flat_map(|receiver| receiver.subscriptions)
                .collect(),
        }
    }

    pub fn from_subscription(subscription: Box<dyn WatchSignalSubscription>) -> Self {
        Self {
            subscriptions: vec![subscription],
        }
    }

    pub fn closed() -> Self {
        Self::from_subscription(Box::new(ClosedSignalSubscription))
    }

    pub async fn recv(&mut self) -> Result<WatchSignal, WatchSignalReceiveError> {
        loop {
            if self.subscriptions.is_empty() {
                return Err(WatchSignalReceiveError::Closed);
            }
            if self.subscriptions.len() == 1 {
                match self.subscriptions[0].recv().await {
                    Err(WatchSignalReceiveError::Closed) => {
                        self.subscriptions.clear();
                        continue;
                    }
                    other => return other,
                }
            }
            let futures = self
                .subscriptions
                .iter_mut()
                .map(|subscription| subscription.recv())
                .collect::<Vec<_>>();
            let (result, index, remaining) = select_all(futures).await;
            drop(remaining);
            match result {
                Err(WatchSignalReceiveError::Closed) => {
                    self.subscriptions.swap_remove(index);
                }
                other => return other,
            }
        }
    }

    pub fn try_recv(&mut self) -> Result<WatchSignal, WatchSignalTryReceiveError> {
        let mut index = 0;
        let mut observed_empty = false;
        while index < self.subscriptions.len() {
            match self.subscriptions[index].try_recv() {
                Ok(signal) => return Ok(signal),
                Err(WatchSignalTryReceiveError::Lagged(skipped)) => {
                    return Err(WatchSignalTryReceiveError::Lagged(skipped));
                }
                Err(WatchSignalTryReceiveError::Empty) => {
                    observed_empty = true;
                    index += 1;
                }
                Err(WatchSignalTryReceiveError::Closed) => {
                    self.subscriptions.swap_remove(index);
                }
            }
        }
        if observed_empty {
            Err(WatchSignalTryReceiveError::Empty)
        } else {
            Err(WatchSignalTryReceiveError::Closed)
        }
    }
}

struct ClosedSignalSubscription;

impl WatchSignalSubscription for ClosedSignalSubscription {
    fn recv(&mut self) -> WatchSignalFuture<'_> {
        Box::pin(async { Err(WatchSignalReceiveError::Closed) })
    }

    fn try_recv(&mut self) -> Result<WatchSignal, WatchSignalTryReceiveError> {
        Err(WatchSignalTryReceiveError::Closed)
    }
}

struct BroadcastSignalSubscription {
    receiver: broadcast::Receiver<WatchSignal>,
}

impl WatchSignalSubscription for BroadcastSignalSubscription {
    fn recv(&mut self) -> WatchSignalFuture<'_> {
        Box::pin(async move { self.receiver.recv().await.map_err(map_receive_error) })
    }

    fn try_recv(&mut self) -> Result<WatchSignal, WatchSignalTryReceiveError> {
        self.receiver.try_recv().map_err(map_try_receive_error)
    }
}

fn map_receive_error(error: broadcast::error::RecvError) -> WatchSignalReceiveError {
    match error {
        broadcast::error::RecvError::Lagged(skipped) => WatchSignalReceiveError::Lagged(skipped),
        broadcast::error::RecvError::Closed => WatchSignalReceiveError::Closed,
    }
}

fn map_try_receive_error(error: broadcast::error::TryRecvError) -> WatchSignalTryReceiveError {
    match error {
        broadcast::error::TryRecvError::Empty => WatchSignalTryReceiveError::Empty,
        broadcast::error::TryRecvError::Lagged(skipped) => {
            WatchSignalTryReceiveError::Lagged(skipped)
        }
        broadcast::error::TryRecvError::Closed => WatchSignalTryReceiveError::Closed,
    }
}

/// Subscriber capability consumed by a positioned session. Establishment is
/// synchronous so the receiver is installed before the first awaited read.
pub trait WatchSignalSubscribe: Send + Sync {
    fn subscribe(&self, topic: WatchTopic) -> WatchSignalReceiver;
}

/// Backend-neutral post-commit wakeup publisher. An embedded commit hook,
/// remote notification adapter, or fake external engine may implement this
/// capability without exposing a datastore or subscriber bus.
pub trait WatchSignalPublish: Send + Sync {
    fn publish(&self, signal: WatchSignal);
}

/// Per-topic bounded signal fan-out. No task, timer, or polling loop is owned
/// by the hub; publishers synchronously wake only active topic subscribers.
pub struct WatchSignalHub {
    topics: Mutex<HashMap<WatchTopic, broadcast::Sender<WatchSignal>>>,
    capacity: usize,
}

impl WatchSignalHub {
    pub fn new(capacity: usize) -> Self {
        Self {
            topics: Mutex::new(HashMap::new()),
            capacity: capacity.max(1),
        }
    }

    pub fn subscribe(&self, topic: WatchTopic) -> WatchSignalReceiver {
        let mut topics = self.lock();
        let receiver = topics
            .entry(topic)
            .or_insert_with(|| broadcast::channel(self.capacity).0)
            .subscribe();
        WatchSignalReceiver::from_subscription(Box::new(BroadcastSignalSubscription { receiver }))
    }

    pub fn publish(&self, signal: WatchSignal) {
        if signal.advances.is_empty() {
            return;
        }
        let topic = signal.topic.clone();
        let mut topics = self.lock();
        let Some(sender) = topics.get(&topic) else {
            return;
        };
        // IF-6H-001: preserve lazy reclamation. A receiverless topic is
        // removed only when a subsequent publish observes it.
        if sender.send(signal).is_err() || sender.receiver_count() == 0 {
            topics.remove(&topic);
        }
    }

    pub fn publish_namespace_advance(&self, namespace: &str, resource_version: i64) {
        if resource_version <= 0 {
            return;
        }
        let mut topics = self.lock();
        topics.retain(|topic, sender| {
            let signal = WatchSignal {
                topic: topic.clone(),
                advances: vec![WatchAdvance {
                    namespace: Some(namespace.to_string()),
                    low_rv: resource_version,
                    high_rv: resource_version,
                }],
            };
            sender.send(signal).is_ok() && sender.receiver_count() > 0
        });
    }

    fn lock(
        &self,
    ) -> std::sync::MutexGuard<'_, HashMap<WatchTopic, broadcast::Sender<WatchSignal>>> {
        self.topics
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl WatchSignalSubscribe for WatchSignalHub {
    fn subscribe(&self, topic: WatchTopic) -> WatchSignalReceiver {
        Self::subscribe(self, topic)
    }
}

impl WatchSignalPublish for WatchSignalHub {
    fn publish(&self, signal: WatchSignal) {
        Self::publish(self, signal);
    }
}

impl WatchSignalEvent for klights_leader_api::PostCommitAdvance {
    fn watch_api_version(&self) -> Option<&str> {
        Some(self.api_version())
    }

    fn watch_kind(&self) -> Option<&str> {
        Some(self.kind())
    }

    fn watch_namespace(&self) -> Option<&str> {
        self.namespace()
    }

    fn watch_resource_version(&self) -> Option<i64> {
        Some(self.resource_version())
    }
}

/// Converts backend-neutral committed advances into bounded active-watch hints.
/// Durable history remains authoritative; this adapter only wakes subscribers.
pub struct PostCommitWatchWakeup {
    publisher: Arc<WatchSignalHub>,
}

impl PostCommitWatchWakeup {
    pub fn new(publisher: Arc<WatchSignalHub>) -> Self {
        Self { publisher }
    }
}

impl klights_leader_api::PostCommitWakeup for PostCommitWatchWakeup {
    fn wake(&self, observations: &[klights_leader_api::PostCommitAdvance]) {
        for signal in WatchSignal::from_events(observations) {
            self.publisher.publish(signal);
        }
    }

    fn wake_namespace_contents(&self, namespace: &str, resource_version: i64) {
        self.publisher
            .publish_namespace_advance(namespace, resource_version);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use klights_leader_api::PostCommitWakeup;

    use super::*;

    #[tokio::test]
    async fn closed_topic_does_not_terminate_a_multi_topic_subscription() {
        let closed = WatchSignalReceiver::closed();
        let hub = WatchSignalHub::new(1);
        let topic = WatchTopic::new("v1", "ConfigMap");
        let active = hub.subscribe(topic.clone());
        let expected = WatchSignal {
            topic,
            advances: vec![WatchAdvance {
                namespace: Some("default".to_string()),
                low_rv: 7,
                high_rv: 7,
            }],
        };
        hub.publish(expected.clone());
        let mut subscription = WatchSignalReceiver::new(vec![closed, active]);

        assert_eq!(subscription.recv().await, Ok(expected));
    }

    #[test]
    fn receiverless_topic_is_reclaimed_only_by_a_later_publish() {
        let hub = WatchSignalHub::new(1);
        let topic = WatchTopic::new("v1", "Pod");
        let receiver = hub.subscribe(topic.clone());
        assert_eq!(hub.lock().len(), 1);
        drop(receiver);
        assert_eq!(hub.lock().len(), 1, "drop alone preserves lazy reclamation");
        hub.publish(WatchSignal {
            topic,
            advances: vec![WatchAdvance {
                namespace: Some("default".to_string()),
                low_rv: 1,
                high_rv: 1,
            }],
        });
        assert!(hub.lock().is_empty());
    }

    #[tokio::test]
    async fn distinct_external_engine_endpoint_wakes_local_watch_over_transport() {
        let hub = Arc::new(WatchSignalHub::new(4));
        let local_wakeup = PostCommitWatchWakeup::new(hub.clone());
        let topic = WatchTopic::new("v1", "ConfigMap");
        let mut local_watch = hub.subscribe(topic.clone());
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();

        sender
            .send(vec![klights_leader_api::PostCommitAdvance::new(
                "v1",
                "ConfigMap",
                Some("default".to_string()),
                41,
            )])
            .expect("simulated external transport remains connected");
        local_wakeup.wake(
            &receiver
                .recv()
                .await
                .expect("external endpoint delivers one commit"),
        );

        assert_eq!(
            local_watch.recv().await,
            Ok(WatchSignal {
                topic,
                advances: vec![WatchAdvance {
                    namespace: Some("default".to_string()),
                    low_rv: 41,
                    high_rv: 41,
                }],
            })
        );
    }
}
