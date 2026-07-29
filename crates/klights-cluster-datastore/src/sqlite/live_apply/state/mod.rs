mod cluster_meta;
mod namespace;
mod network;
mod outbox;
mod pod_cleanup;
mod resource;
pub(super) mod watch_history;

use klights_cluster_core::ClusterMutation;
use klights_cluster_store::StagedPostCommit;

#[derive(Default)]
pub(super) struct ApplyEffects {
    pending_watch_events: Vec<StagedPostCommit>,
}

impl ApplyEffects {
    pub(super) fn new() -> Self {
        Self {
            pending_watch_events: Vec::new(),
        }
    }

    pub(super) fn push_watch_event(&mut self, event: StagedPostCommit) {
        self.pending_watch_events.push(event);
    }

    pub(super) fn into_pending_watch_events(self) -> Vec<StagedPostCommit> {
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

    fn cluster_meta_mut(&mut self) -> &mut cluster_meta::ClusterMetaStateApplier<'tx, 'conn> {
        &mut self.cluster_meta
    }

    fn namespace_mut(&mut self) -> &mut namespace::NamespaceStateApplier<'tx, 'conn> {
        &mut self.namespace
    }

    fn network_mut(&mut self) -> &mut network::NetworkStateApplier<'tx, 'conn> {
        &mut self.network
    }

    fn outbox_mut(&mut self) -> &mut outbox::OutboxLedgerStateApplier<'tx, 'conn> {
        &mut self.outbox
    }

    pub(super) fn put_applied_outbox(
        &mut self,
        row: klights_cluster_core::LogApplyAppliedOutboxRow,
    ) -> tokio_rusqlite::Result<()> {
        self.outbox_mut().put_applied_outbox(row)
    }

    fn pod_cleanup_mut(&mut self) -> &mut pod_cleanup::PodCleanupStateApplier<'tx, 'conn> {
        &mut self.pod_cleanup
    }

    fn resource_mut(&mut self) -> &mut resource::ClusterStateApplier<'tx, 'conn> {
        &mut self.resource
    }

    fn watch_history_mut(&mut self) -> &mut watch_history::WatchHistoryStateApplier<'tx, 'conn> {
        &mut self.watch_history
    }

    pub(super) fn apply_cluster_mutation(
        &mut self,
        commit_resource_version: i64,
        mutation: ClusterMutation,
        emit_watch_events: bool,
        effects: &mut ApplyEffects,
    ) -> tokio_rusqlite::Result<()> {
        match mutation {
            ClusterMutation::Resource(mutation) => match mutation {
                klights_cluster_core::ResourceMutation::PutResource(row) => {
                    if row.resource_version != commit_resource_version {
                        return Err(super::other_error(
                            "resource row RV does not match commit RV",
                        ));
                    }
                    if let Some(event) = self
                        .resource_mut()
                        .apply_put_resource(row, emit_watch_events)?
                    {
                        effects.push_watch_event(event);
                    }
                }
                klights_cluster_core::ResourceMutation::PatchResourceLatest(patch) => {
                    if patch.resource_version != commit_resource_version {
                        return Err(super::other_error(
                            "resource patch RV does not match commit RV",
                        ));
                    }
                    if let Some(event) = self
                        .resource_mut()
                        .apply_patch_resource_latest(patch, emit_watch_events)?
                    {
                        effects.push_watch_event(event);
                    }
                }
                klights_cluster_core::ResourceMutation::DeleteResource(key) => {
                    if let Some(event) = self.resource_mut().apply_delete_resource(
                        commit_resource_version,
                        key,
                        emit_watch_events,
                    )? {
                        effects.push_watch_event(event);
                    }
                }
                klights_cluster_core::ResourceMutation::FinalizeBoundPod(_) => {
                    return Err(super::other_error(
                        "bound Pod finalization was not resolved before state-machine apply",
                    ));
                }
            },
            ClusterMutation::Namespace(mutation) => match mutation {
                klights_cluster_core::NamespaceMutation::PutNamespace(row) => {
                    if row.resource_version != commit_resource_version {
                        return Err(super::other_error(
                            "namespace row RV does not match commit RV",
                        ));
                    }
                    if let Some(event) =
                        self.namespace_mut().put_namespace(row, emit_watch_events)?
                    {
                        effects.push_watch_event(event);
                    }
                }
                klights_cluster_core::NamespaceMutation::DeleteNamespace { name } => {
                    if let Some(event) = self.namespace_mut().delete_namespace(
                        commit_resource_version,
                        &name,
                        emit_watch_events,
                    )? {
                        effects.push_watch_event(event);
                    }
                }
                klights_cluster_core::NamespaceMutation::DeleteNamespaceContents { name } => {
                    self.namespace_mut().delete_namespace_contents(&name)?;
                }
            },
            ClusterMutation::WatchHistory(mutation) => match mutation {
                klights_cluster_core::WatchHistoryMutation::PutWatchEvent(row) => {
                    effects.push_watch_event(self.watch_history_mut().apply_put_watch_event(row)?);
                }
                klights_cluster_core::WatchHistoryMutation::GcWatchEvents {
                    max_rows,
                    batch_cap,
                } => {
                    self.watch_history_mut()
                        .apply_gc_watch_events(max_rows, batch_cap)?;
                }
            },
            ClusterMutation::Network(mutation) => match mutation {
                klights_cluster_core::NetworkMutation::PutNodeSubnet(row) => {
                    self.network_mut().put_node_subnet(row)?;
                }
                klights_cluster_core::NetworkMutation::AllocateNodeSubnet(allocation) => {
                    self.network_mut().allocate_node_subnet(allocation)?;
                }
                klights_cluster_core::NetworkMutation::DeleteNodeSubnet { node_name } => {
                    self.network_mut().delete_node_subnet(node_name)?;
                }
                klights_cluster_core::NetworkMutation::PutNodeDataplane(row) => {
                    self.network_mut().put_node_dataplane(row)?;
                }
                klights_cluster_core::NetworkMutation::DeleteNodeDataplane { node_name } => {
                    self.network_mut().delete_node_dataplane(node_name)?;
                }
            },
            ClusterMutation::OutboxLedger(mutation) => match mutation {
                klights_cluster_core::OutboxLedgerMutation::PutAppliedOutbox(row) => {
                    self.outbox_mut().put_applied_outbox(row)?;
                }
                klights_cluster_core::OutboxLedgerMutation::DeleteAppliedOutbox {
                    idempotency_key,
                } => {
                    self.outbox_mut().delete_applied_outbox(idempotency_key)?;
                }
                klights_cluster_core::OutboxLedgerMutation::GcAppliedOutbox {
                    cutoff_ms,
                    operations: _,
                } => {
                    self.outbox_mut().gc_applied_outbox(cutoff_ms)?;
                }
            },
            ClusterMutation::ClusterMeta(mutation) => match mutation {
                klights_cluster_core::ClusterMetaMutation::AdvanceResourceVersion {
                    resource_version: _,
                } => {}
                klights_cluster_core::ClusterMetaMutation::PutKlightsMeta { key, value } => {
                    self.cluster_meta_mut().put_klights_meta(key, value)?;
                }
            },
            ClusterMutation::PodCleanup(mutation) => match mutation {
                klights_cluster_core::PodCleanupMutation::PutPodCleanupIntent(row) => {
                    if row.resource_version != commit_resource_version {
                        return Err(super::other_error(
                            "pod cleanup intent RV does not match commit RV",
                        ));
                    }
                    self.pod_cleanup_mut().put_pod_cleanup_intent(row)?;
                }
                klights_cluster_core::PodCleanupMutation::DeletePodCleanupIntent(key) => {
                    self.pod_cleanup_mut().delete_pod_cleanup_intent(key)?;
                }
                klights_cluster_core::PodCleanupMutation::DeletePodCleanupIntentsForNode {
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
    use klights_cluster_store::StagedPostCommit;

    #[test]
    fn apply_effects_starts_empty_and_preserves_watch_event_order() {
        let effects = ApplyEffects::new();
        assert!(effects.into_pending_watch_events().is_empty());

        let mut effects = ApplyEffects::new();
        effects.push_watch_event(StagedPostCommit::new("v1", "ConfigMap", Some("first"), 1));
        effects.push_watch_event(StagedPostCommit::new("v1", "ConfigMap", Some("second"), 2));
        effects.push_watch_event(StagedPostCommit::new("v1", "ConfigMap", Some("third"), 3));

        let events = effects.into_pending_watch_events();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].namespace(), Some("first"));
        assert_eq!(events[1].namespace(), Some("second"));
        assert_eq!(events[2].namespace(), Some("third"));
        assert_eq!(events[0].resource_version(), 1);
        assert_eq!(events[1].resource_version(), 2);
        assert_eq!(events[2].resource_version(), 3);
    }
}
