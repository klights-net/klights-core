mod cluster_meta;
mod namespace;
mod network;
mod outbox;
mod pod_cleanup;
mod resource;
mod watch_history;

use crate::datastore::types::PendingWatchEvent;
use crate::log_apply::ClusterMutation;

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

    pub(super) fn apply_cluster_mutation(
        &mut self,
        commit_resource_version: i64,
        mutation: ClusterMutation,
        emit_watch_events: bool,
        raft_authoritative: bool,
        effects: &mut ApplyEffects,
    ) -> tokio_rusqlite::Result<()> {
        match mutation {
            ClusterMutation::Resource(mutation) => match mutation {
                crate::log_apply::ResourceMutation::PutResource(row) => {
                    if row.resource_version != commit_resource_version {
                        return Err(super::cluster_replace::other_error(
                            "resource row RV does not match commit RV",
                        ));
                    }
                    if let Some(event) = self.resource_mut().apply_put_resource(
                        row,
                        emit_watch_events,
                        raft_authoritative,
                    )? {
                        effects.push_watch_event(event);
                    }
                }
                crate::log_apply::ResourceMutation::PatchResourceLatest(patch) => {
                    if patch.resource_version != commit_resource_version {
                        return Err(super::cluster_replace::other_error(
                            "resource patch RV does not match commit RV",
                        ));
                    }
                    if let Some(event) = self.resource_mut().apply_patch_resource_latest(
                        patch,
                        emit_watch_events,
                        raft_authoritative,
                    )? {
                        effects.push_watch_event(event);
                    }
                }
                crate::log_apply::ResourceMutation::DeleteResource(key) => {
                    if let Some(event) = self.resource_mut().apply_delete_resource(
                        commit_resource_version,
                        key,
                        emit_watch_events,
                        raft_authoritative,
                    )? {
                        effects.push_watch_event(event);
                    }
                }
            },
            ClusterMutation::Namespace(mutation) => match mutation {
                crate::log_apply::NamespaceMutation::PutNamespace(row) => {
                    if row.resource_version != commit_resource_version {
                        return Err(super::cluster_replace::other_error(
                            "namespace row RV does not match commit RV",
                        ));
                    }
                    if let Some(event) =
                        self.namespace_mut().put_namespace(row, emit_watch_events)?
                    {
                        effects.push_watch_event(event);
                    }
                }
                crate::log_apply::NamespaceMutation::DeleteNamespace { name } => {
                    if let Some(event) = self.namespace_mut().delete_namespace(
                        commit_resource_version,
                        &name,
                        emit_watch_events,
                    )? {
                        effects.push_watch_event(event);
                    }
                }
                crate::log_apply::NamespaceMutation::DeleteNamespaceContents { name } => {
                    self.namespace_mut().delete_namespace_contents(&name)?;
                }
            },
            ClusterMutation::WatchHistory(mutation) => match mutation {
                crate::log_apply::WatchHistoryMutation::PutWatchEvent(row) => {
                    effects.push_watch_event(self.watch_history_mut().apply_put_watch_event(row)?);
                }
                crate::log_apply::WatchHistoryMutation::GcWatchEvents {
                    max_rows,
                    batch_cap,
                } => {
                    self.watch_history_mut()
                        .apply_gc_watch_events(max_rows, batch_cap)?;
                }
            },
            ClusterMutation::Network(mutation) => match mutation {
                crate::log_apply::NetworkMutation::PutNodeSubnet(row) => {
                    self.network_mut().put_node_subnet(row)?;
                }
                crate::log_apply::NetworkMutation::AllocateNodeSubnet(allocation) => {
                    self.network_mut().allocate_node_subnet(allocation)?;
                }
                crate::log_apply::NetworkMutation::DeleteNodeSubnet { node_name } => {
                    self.network_mut().delete_node_subnet(node_name)?;
                }
                crate::log_apply::NetworkMutation::PutNodeDataplane(row) => {
                    self.network_mut().put_node_dataplane(row)?;
                }
                crate::log_apply::NetworkMutation::DeleteNodeDataplane { node_name } => {
                    self.network_mut().delete_node_dataplane(node_name)?;
                }
            },
            ClusterMutation::OutboxLedger(mutation) => match mutation {
                crate::log_apply::OutboxLedgerMutation::PutAppliedOutbox(row) => {
                    self.outbox_mut().put_applied_outbox(row)?;
                }
                crate::log_apply::OutboxLedgerMutation::DeleteAppliedOutbox { idempotency_key } => {
                    self.outbox_mut().delete_applied_outbox(idempotency_key)?;
                }
                crate::log_apply::OutboxLedgerMutation::GcAppliedOutbox {
                    cutoff_ms,
                    operations: _,
                } => {
                    self.outbox_mut().gc_applied_outbox(cutoff_ms)?;
                }
            },
            ClusterMutation::ClusterMeta(mutation) => match mutation {
                crate::log_apply::ClusterMetaMutation::AdvanceResourceVersion {
                    resource_version: _,
                } => {}
                crate::log_apply::ClusterMetaMutation::PutKlightsMeta { key, value } => {
                    self.cluster_meta_mut().put_klights_meta(key, value)?;
                }
            },
            ClusterMutation::PodCleanup(mutation) => match mutation {
                crate::log_apply::PodCleanupMutation::PutPodCleanupIntent(row) => {
                    if row.resource_version != commit_resource_version {
                        return Err(super::cluster_replace::other_error(
                            "pod cleanup intent RV does not match commit RV",
                        ));
                    }
                    self.pod_cleanup_mut().put_pod_cleanup_intent(row)?;
                }
                crate::log_apply::PodCleanupMutation::DeletePodCleanupIntent(key) => {
                    self.pod_cleanup_mut().delete_pod_cleanup_intent(key)?;
                }
                crate::log_apply::PodCleanupMutation::DeletePodCleanupIntentsForNode {
                    node_name,
                } => {
                    self.pod_cleanup_mut()
                        .delete_pod_cleanup_intents_for_node(node_name)?;
                }
            },
        }
        Ok(())
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
