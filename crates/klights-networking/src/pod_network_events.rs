use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use klights_network_api::{
    PodNetworkAssignmentEventError, PodNetworkAssignmentKey, PodNetworkAssignmentPublisher,
    PodNetworkAssignmentSubscription, PodNetworkAssignmentWaitFuture, PodNetworkAssignmentWaiter,
};
use tokio::sync::watch;

struct Entry {
    id: u64,
    generation: u64,
    sender: watch::Sender<u64>,
}

#[derive(Default)]
struct BusState {
    closed: bool,
    next_entry_id: u64,
    entries: HashMap<PodNetworkAssignmentKey, Entry>,
}

/// Instance-owned, idle-silent rendezvous for durable CNI assignment hints.
///
/// Storage remains authoritative. The retained watch generation only closes
/// the registration-to-wait lost-wakeup gap and coalesces duplicate hints.
pub struct PodNetworkAssignmentBus {
    inner: Arc<Mutex<BusState>>,
}

impl Default for PodNetworkAssignmentBus {
    fn default() -> Self {
        Self::new()
    }
}

impl PodNetworkAssignmentBus {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(BusState::default())),
        }
    }

    fn state(&self) -> MutexGuard<'_, BusState> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Close the bus and wake every live subscription.
    pub fn close(&self) {
        let mut state = self.state();
        state.closed = true;
        state.entries.clear();
    }

    #[cfg(test)]
    pub fn entry_count_for_test(&self) -> usize {
        self.state().entries.len()
    }
}

impl PodNetworkAssignmentPublisher for PodNetworkAssignmentBus {
    fn publish_assignment(&self, key: &PodNetworkAssignmentKey) {
        let mut state = self.state();
        let Some(entry) = state.entries.get_mut(key) else {
            return;
        };
        entry.generation = entry.generation.wrapping_add(1);
        entry.sender.send_replace(entry.generation);
    }
}

impl PodNetworkAssignmentWaiter for PodNetworkAssignmentBus {
    fn subscribe(
        &self,
        key: PodNetworkAssignmentKey,
    ) -> Result<Box<dyn PodNetworkAssignmentSubscription>, PodNetworkAssignmentEventError> {
        let mut state = self.state();
        if state.closed {
            return Err(PodNetworkAssignmentEventError::closed());
        }

        let (entry_id, receiver) = if let Some(entry) = state.entries.get(&key) {
            (entry.id, entry.sender.subscribe())
        } else {
            state.next_entry_id = state.next_entry_id.wrapping_add(1);
            let entry_id = state.next_entry_id;
            let (sender, receiver) = watch::channel(0);
            state.entries.insert(
                key.clone(),
                Entry {
                    id: entry_id,
                    generation: 0,
                    sender,
                },
            );
            (entry_id, receiver)
        };
        Ok(Box::new(AssignmentSubscription {
            key,
            entry_id,
            receiver: Some(receiver),
            bus: Arc::downgrade(&self.inner),
        }))
    }
}

struct AssignmentSubscription {
    key: PodNetworkAssignmentKey,
    entry_id: u64,
    receiver: Option<watch::Receiver<u64>>,
    bus: Weak<Mutex<BusState>>,
}

impl PodNetworkAssignmentSubscription for AssignmentSubscription {
    fn wait(&mut self) -> PodNetworkAssignmentWaitFuture<'_> {
        Box::pin(async move {
            self.receiver
                .as_mut()
                .expect("assignment subscription receiver is live")
                .changed()
                .await
                .map_err(|_| PodNetworkAssignmentEventError::closed())
        })
    }
}

impl Drop for AssignmentSubscription {
    fn drop(&mut self) {
        drop(self.receiver.take());
        let Some(bus) = self.bus.upgrade() else {
            return;
        };
        let mut state = bus.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let remove = state
            .entries
            .get(&self.key)
            .is_some_and(|entry| entry.id == self.entry_id && entry.sender.receiver_count() == 0);
        if remove {
            state.entries.remove(&self.key);
        }
    }
}

#[cfg(test)]
mod retained_generation_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use futures::FutureExt;
    use klights_network_api::{
        PodNetworkAssignmentEventError, PodNetworkAssignmentKey, PodNetworkAssignmentPublisher,
        PodNetworkAssignmentWaiter,
    };

    use super::PodNetworkAssignmentBus;

    fn key(uid: &str) -> PodNetworkAssignmentKey {
        PodNetworkAssignmentKey::try_new("sandbox-a", "default", "pod-a", uid).unwrap()
    }

    #[tokio::test]
    async fn subscribe_then_publish_before_wait_retains_readiness() {
        let bus = PodNetworkAssignmentBus::new();
        let mut subscription = bus.subscribe(key("uid-a")).unwrap();
        bus.publish_assignment(&key("uid-a"));

        tokio::time::timeout(Duration::from_millis(100), subscription.wait())
            .await
            .expect("retained assignment signal")
            .unwrap();
    }

    #[tokio::test]
    async fn publish_without_subscriber_is_noop_and_not_replayed() {
        let bus = PodNetworkAssignmentBus::new();
        bus.publish_assignment(&key("uid-a"));
        let mut subscription = bus.subscribe(key("uid-a")).unwrap();

        assert!(subscription.wait().now_or_never().is_none());
    }

    #[tokio::test]
    async fn all_same_key_subscribers_wake_and_other_identity_does_not() {
        let bus = PodNetworkAssignmentBus::new();
        let mut first = bus.subscribe(key("uid-a")).unwrap();
        let mut second = bus.subscribe(key("uid-a")).unwrap();
        let mut replacement = bus.subscribe(key("uid-b")).unwrap();

        bus.publish_assignment(&key("uid-a"));

        first.wait().await.unwrap();
        second.wait().await.unwrap();
        assert!(replacement.wait().now_or_never().is_none());
    }

    #[tokio::test]
    async fn repeated_publish_coalesces_without_producer_backpressure() {
        let bus = PodNetworkAssignmentBus::new();
        let mut subscription = bus.subscribe(key("uid-a")).unwrap();

        for _ in 0..1024 {
            bus.publish_assignment(&key("uid-a"));
        }

        subscription.wait().await.unwrap();
        assert!(subscription.wait().now_or_never().is_none());
    }

    #[tokio::test]
    async fn dropping_one_subscriber_keeps_other_registration_live() {
        let bus = PodNetworkAssignmentBus::new();
        let first = bus.subscribe(key("uid-a")).unwrap();
        let mut second = bus.subscribe(key("uid-a")).unwrap();
        drop(first);

        assert_eq!(bus.entry_count_for_test(), 1);
        bus.publish_assignment(&key("uid-a"));
        second.wait().await.unwrap();
        drop(second);
        assert_eq!(bus.entry_count_for_test(), 0);
    }

    #[tokio::test]
    async fn close_wakes_live_subscribers_with_typed_error() {
        let bus = Arc::new(PodNetworkAssignmentBus::new());
        let mut subscription = bus.subscribe(key("uid-a")).unwrap();
        bus.close();

        assert_eq!(
            subscription.wait().await,
            Err(PodNetworkAssignmentEventError::Closed)
        );
        assert!(matches!(
            bus.subscribe(key("uid-b")),
            Err(PodNetworkAssignmentEventError::Closed)
        ));
    }
}
