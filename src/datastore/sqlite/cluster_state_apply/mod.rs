mod cluster_meta;
mod namespace;
mod network;
mod outbox;
mod pod_cleanup;
mod resource;
mod watch_history;

use crate::datastore::types::PendingWatchEvent;

#[derive(Default)]
pub(super) struct ApplyEffects {
    pending_watch_events: Vec<PendingWatchEvent>,
}

impl ApplyEffects {
    pub(super) fn new() -> Self {
        Self {
            pending_watch_events: Vec::new(),
        }
    }

    pub(super) fn push_watch_event(&mut self, event: PendingWatchEvent) {
        self.pending_watch_events.push(event);
    }

    pub(super) fn into_pending_watch_events(self) -> Vec<PendingWatchEvent> {
        self.pending_watch_events
    }
}

pub(super) struct RaftClusterStateApplier<'tx, 'conn> {
    cluster_meta: cluster_meta::ClusterMetaStateApplier<'tx, 'conn>,
    namespace: namespace::NamespaceStateApplier<'tx, 'conn>,
    network: network::NetworkStateApplier<'tx, 'conn>,
    outbox: outbox::OutboxLedgerStateApplier<'tx, 'conn>,
    pod_cleanup: pod_cleanup::PodCleanupStateApplier<'tx, 'conn>,
    resource: resource::ClusterStateApplier<'tx, 'conn>,
    watch_history: watch_history::WatchHistoryStateApplier<'tx, 'conn>,
}

impl<'tx, 'conn> RaftClusterStateApplier<'tx, 'conn> {
    pub(super) fn new(tx: &'tx rusqlite::Transaction<'conn>) -> Self {
        Self {
            cluster_meta: cluster_meta::ClusterMetaStateApplier::new(tx),
            namespace: namespace::NamespaceStateApplier::new(tx),
            network: network::NetworkStateApplier::new(tx),
            outbox: outbox::OutboxLedgerStateApplier::new(tx),
            pod_cleanup: pod_cleanup::PodCleanupStateApplier::new(tx),
            resource: resource::ClusterStateApplier::new(tx),
            watch_history: watch_history::WatchHistoryStateApplier::new(tx),
        }
    }

    pub(super) fn cluster_meta_mut(
        &mut self,
    ) -> &mut cluster_meta::ClusterMetaStateApplier<'tx, 'conn> {
        &mut self.cluster_meta
    }

    pub(super) fn namespace_mut(&mut self) -> &mut namespace::NamespaceStateApplier<'tx, 'conn> {
        &mut self.namespace
    }

    pub(super) fn network_mut(&mut self) -> &mut network::NetworkStateApplier<'tx, 'conn> {
        &mut self.network
    }

    pub(super) fn outbox_mut(&mut self) -> &mut outbox::OutboxLedgerStateApplier<'tx, 'conn> {
        &mut self.outbox
    }

    pub(super) fn pod_cleanup_mut(
        &mut self,
    ) -> &mut pod_cleanup::PodCleanupStateApplier<'tx, 'conn> {
        &mut self.pod_cleanup
    }

    pub(super) fn resource_mut(&mut self) -> &mut resource::ClusterStateApplier<'tx, 'conn> {
        &mut self.resource
    }

    pub(super) fn watch_history_mut(
        &mut self,
    ) -> &mut watch_history::WatchHistoryStateApplier<'tx, 'conn> {
        &mut self.watch_history
    }
}

#[cfg(test)]
mod tests {
    use super::ApplyEffects;
    use crate::datastore::types::PendingWatchEvent;
    use crate::watch::{EventType, WatchEvent};
    use serde_json::json;

    #[test]
    fn apply_effects_starts_empty_and_preserves_watch_event_order() {
        let effects = ApplyEffects::new();
        assert!(effects.into_pending_watch_events().is_empty());

        let mut effects = ApplyEffects::new();
        effects.push_watch_event(PendingWatchEvent {
            event: WatchEvent::added(json!({"kind":"ConfigMap","metadata":{"name":"first"}})),
        });
        effects.push_watch_event(PendingWatchEvent {
            event: WatchEvent::modified(json!({"kind":"ConfigMap","metadata":{"name":"second"}})),
        });
        effects.push_watch_event(PendingWatchEvent {
            event: WatchEvent::deleted(json!({"kind":"ConfigMap","metadata":{"name":"third"}})),
        });

        let events = effects.into_pending_watch_events();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].event.event_type, EventType::Added);
        assert_eq!(events[1].event.event_type, EventType::Modified);
        assert_eq!(events[2].event.event_type, EventType::Deleted);
        assert_eq!(events[0].event.object["metadata"]["name"], json!("first"));
        assert_eq!(events[1].event.object["metadata"]["name"], json!("second"));
        assert_eq!(events[2].event.object["metadata"]["name"], json!("third"));
    }
}
