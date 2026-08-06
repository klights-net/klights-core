//! Root-owned compatibility adapter for legacy broad datastore consumers.
//!
//! Normal application consumers receive this compatibility facade while Raft
//! proposal materialization, snapshotting, and committed state-machine apply
//! retain the passive backend. The proposal capability is immutable and
//! complete at construction, so there is no late-bound or fallback write path.
//!
//! ## Architecture
//!
//! ```text
//! API/controller write
//!         │
//!         ▼
//! SequencedDatastore (implements DatastoreBackend)
//!         │
//!         ├── immutable RaftProposal → propose through raft
//!         │
//!         ▼
//! passive DatastoreBackend (reads + committed persistence)
//! ```

use anyhow::Result;
#[cfg(test)]
use async_trait::async_trait;
#[cfg(any(test, feature = "pod-repository-test-support"))]
use klights_cluster_core::CommandMeta;
use klights_cluster_core::StorageCommand;
use std::sync::Arc;

use crate::datastore::DatastoreBackend;
#[cfg(any(test, feature = "pod-repository-test-support"))]
use crate::datastore::ReplicatedCreateOptions;
#[cfg(any(test, feature = "pod-repository-test-support"))]
use klights_cluster_core::ResourcePatchRequest;
use klights_replication::proposal::RaftProposal;

mod backend_impl;

// ---------------------------------------------------------------------------
// DatastoreApplier — deterministic local apply trait
// ---------------------------------------------------------------------------

/// TO-BE-CLEANUP: legacy replicated StorageCommand apply test support.
/// Trait for deterministic local apply of storage commands.
///
/// Root compatibility tests use this trait to compare deterministic local
/// application across passive storage engines.
#[cfg(test)]
#[async_trait]
pub(crate) trait DatastoreApplier: Send + Sync {
    async fn apply_command(&self, cmd: StorageCommand, meta: CommandMeta) -> Result<()>;
}

// `ForwardedWrite` and `CommandForwarder` removed in T6 — the legacy
// generic storage-forward shim. Workers now route writes through outbox
// -> ApplyOutbox via the LeaderApiClient surface.

// ---------------------------------------------------------------------------
// SequencedDatastore
// ---------------------------------------------------------------------------

pub(crate) struct SequencedDatastore {
    passive: Arc<dyn DatastoreBackend>,
    proposal: Arc<dyn RaftProposal>,
    wall_clock: Arc<dyn klights_supervisor::WallClock>,
}

impl SequencedDatastore {
    #[cfg(test)]
    pub(crate) fn new(passive: Arc<dyn DatastoreBackend>, proposal: Arc<dyn RaftProposal>) -> Self {
        Self::new_with_clock(
            passive,
            proposal,
            Arc::new(klights_supervisor::SystemWallClock),
        )
    }

    pub(crate) fn new_with_clock(
        passive: Arc<dyn DatastoreBackend>,
        proposal: Arc<dyn RaftProposal>,
        wall_clock: Arc<dyn klights_supervisor::WallClock>,
    ) -> Self {
        Self {
            passive,
            proposal,
            wall_clock,
        }
    }

    async fn propose_command(
        &self,
        command: StorageCommand,
    ) -> Result<klights_cluster_store::StorageCommandResult> {
        self.proposal.propose_command(command).await
    }
}

#[cfg(any(test, feature = "pod-repository-test-support"))]
/// TO-BE-CLEANUP: legacy replicated StorageCommand apply test support.
pub(crate) async fn apply_command_to_backend<B>(
    backend: &B,
    command: StorageCommand,
    meta: CommandMeta,
) -> Result<()>
where
    B: DatastoreBackend + ?Sized,
{
    align_resource_version_before_replicated_apply(backend, meta.resource_version).await?;
    match command {
        StorageCommand::CreateResource {
            api_version,
            kind,
            namespace,
            name,
            data,
        } => {
            backend
                .apply_replicated_create_resource(
                    &api_version,
                    &kind,
                    namespace.as_deref(),
                    &name,
                    data,
                    ReplicatedCreateOptions::new(meta.resource_version, meta.uid.clone()),
                )
                .await?;
        }
        StorageCommand::UpdateResource {
            api_version,
            kind,
            namespace,
            name,
            mut data,
            expected_rv: _,
            preconditions,
        } => {
            let current = backend
                .get_resource(&api_version, &kind, namespace.as_deref(), &name)
                .await?;
            if let Some(current) = current.as_ref()
                && current.resource_version >= meta.resource_version
                && meta
                    .uid
                    .as_ref()
                    .is_none_or(|expected_uid| current.uid == *expected_uid)
            {
                klights_types::preserve_status_subresource_on_main_update(
                    &api_version,
                    &kind,
                    &current.data,
                    &mut data,
                );
            }
            backend
                .update_main_resource_with_preconditions(
                    &api_version,
                    &kind,
                    namespace.as_deref(),
                    &name,
                    data,
                    preconditions,
                )
                .await?;
        }
        StorageCommand::DeleteResource {
            api_version,
            kind,
            namespace,
            name,
            preconditions,
        } => {
            backend
                .delete_resource_with_preconditions(
                    &api_version,
                    &kind,
                    namespace.as_deref(),
                    &name,
                    preconditions,
                )
                .await?;
        }
        StorageCommand::DeleteResourceWithTombstone {
            api_version,
            kind,
            namespace,
            name,
            preconditions,
            grace_seconds: _,
        } => {
            backend
                .delete_resource_with_preconditions(
                    &api_version,
                    &kind,
                    namespace.as_deref(),
                    &name,
                    preconditions,
                )
                .await?;
        }
        StorageCommand::PatchResource {
            api_version,
            kind,
            namespace,
            name,
            patch_kind,
            patch,
            preconditions,
            strict_resource_version,
        } => {
            backend
                .patch_resource_latest_with_preconditions(
                    &api_version,
                    &kind,
                    namespace.as_deref(),
                    &name,
                    ResourcePatchRequest {
                        patch_kind,
                        patch,
                        preconditions,
                        strict_resource_version,
                    },
                )
                .await?;
        }
        StorageCommand::UpdateStatus {
            api_version,
            kind,
            namespace,
            name,
            status,
            expected_rv: _,
            mut preconditions,
            observed_status_stamp,
        } => {
            let current = backend
                .get_resource(&api_version, &kind, namespace.as_deref(), &name)
                .await?;
            let mut status = status;
            // Route every kind through the single registry-owned merge
            // boundary (raft-fix.md): the prior `Pod || Node` gate (plus a
            // stale-RV fallback) left generic workload kinds path-dependent —
            // a stale ReplicaSet/Job/PDB status only converged if it happened
            // to carry a matching uid precondition. Merging whenever a live
            // row exists is safe: the merge is a no-op for a fresh non-Pod
            // apply, and Pod/Node stay typed-merged regardless of freshness.
            let mut clear_stale_resource_version = false;
            if let Some(current) = current.as_ref() {
                let freshness = klights_cluster_core::apply_status_merge(
                    &api_version,
                    &kind,
                    current.data.as_ref(),
                    &mut status,
                    preconditions.resource_version,
                    current.resource_version,
                    observed_status_stamp.is_some(),
                );
                // Pod status is deduped via the observed_status_stamp outbox,
                // so only a non-Pod stale rebase clears the resourceVersion
                // precondition (otherwise the stale write 409s instead of
                // converging).
                if freshness == klights_cluster_core::StatusApplyFreshness::Stale
                    && preconditions.uid.as_deref() == Some(current.uid.as_str())
                    && !(api_version == "v1" && kind == "Pod")
                {
                    clear_stale_resource_version = true;
                }
            }
            if clear_stale_resource_version {
                preconditions.resource_version = None;
            }
            backend
                .update_status_only_with_preconditions(
                    &api_version,
                    &kind,
                    namespace.as_deref(),
                    &name,
                    status,
                    preconditions,
                )
                .await?;
        }
        StorageCommand::ApplyResourceBatch { operations } => {
            backend.apply_resource_batch(operations).await?;
        }
        StorageCommand::CreateNamespace { name, data } => {
            if let Some(existing) = backend.get_namespace(&name).await? {
                backend
                    .update_namespace(&name, data, existing.resource_version)
                    .await?;
            } else {
                backend.create_namespace(&name, data).await?;
            }
        }
        StorageCommand::UpdateNamespace {
            name,
            data,
            expected_rv,
        } => {
            let expected_rv = backend
                .get_namespace(&name)
                .await?
                .map(|resource| resource.resource_version)
                .unwrap_or(expected_rv);
            backend.update_namespace(&name, data, expected_rv).await?;
        }
        StorageCommand::DeleteNamespace { name } => {
            backend.delete_namespace(&name).await?;
        }
        StorageCommand::DeleteNamespaceContents { name } => {
            backend.delete_namespace_contents(&name).await?;
        }
        StorageCommand::AllocateNodeSubnet {
            node_name,
            subnet,
            node_ip,
        } => {
            backend
                .allocate_node_subnet(&node_name, &subnet, &node_ip)
                .await?;
        }
        StorageCommand::UpdateNodePeerAttributes {
            node_name,
            mode,
            hostport_range,
        } => {
            let peer_mode = klights_types::parse_node_peer_mode(Some(&mode))
                .unwrap_or(klights_types::NodePeerMode::Root);
            let hpr = hostport_range
                .as_deref()
                .and_then(|value| klights_types::HostPortRange::parse(value).ok());
            backend
                .update_node_peer_attributes(&node_name, peer_mode, hpr)
                .await?;
        }
        StorageCommand::UpdateNodeDataplane {
            node_name,
            mode,
            encryption,
            public_key,
            endpoint,
            port,
        } => {
            let metadata = klights_cluster_store::DataplanePeerMetadata::try_new(
                node_name,
                klights_cluster_store::DataplaneMode::parse(&mode)?,
                klights_cluster_store::DataplaneEncryption::parse(Some(&encryption))?,
                public_key,
                Some(endpoint),
                port,
            )?;
            backend.update_node_dataplane(metadata).await?;
        }
        StorageCommand::DeleteNodeSubnet { node_name } => {
            backend.delete_node_subnet(&node_name).await?;
        }
        StorageCommand::PodSlotTryAdmit { .. }
        | StorageCommand::PodSlotMarkTerminating { .. }
        | StorageCommand::PodSlotClearIfUid { .. } => {
            anyhow::bail!("node-local pod-slot commands cannot apply to cluster storage");
        }
        StorageCommand::MovePodToCleanupIntent {
            node_name,
            namespace,
            pod_name,
            pod_uid,
            reason,
        } => {
            backend
                .move_pod_to_cleanup_intent(&node_name, &namespace, &pod_name, &pod_uid, &reason)
                .await?;
        }
        StorageCommand::DeletePodCleanupIntent {
            node_name,
            namespace,
            pod_name,
            pod_uid,
            reason,
        } => {
            backend
                .delete_pod_cleanup_intent(&node_name, &namespace, &pod_name, &pod_uid, &reason)
                .await?;
        }
        StorageCommand::DeletePodCleanupIntentsForNode { node_name } => {
            backend
                .delete_pod_cleanup_intents_for_node(&node_name)
                .await?;
        }
        StorageCommand::WatchEventAppend { event_bytes, rv } => {
            let _ = (event_bytes, rv);
        }
        StorageCommand::GcWatchEvents {
            max_rows,
            batch_cap,
        } => {
            backend.gc_watch_events(max_rows, batch_cap).await?;
        }
        StorageCommand::GcAppliedOutbox { cutoff_ms } => {
            backend.gc_applied_outbox(cutoff_ms, 0).await?;
        }
        StorageCommand::AdvanceResourceVersion { min_rv, .. } => {
            backend.advance_resource_version_after(min_rv).await?;
        }
        StorageCommand::EnsureClusterMetadata { cluster_id } => {
            let existing = backend
                .get_klights_meta(klights_cluster_store::CLUSTER_ID_META_KEY)
                .await?;
            if existing.is_none() {
                backend
                    .set_klights_meta(klights_cluster_store::CLUSTER_ID_META_KEY, &cluster_id)
                    .await?;
                backend
                    .set_klights_meta(klights_cluster_store::LEADER_EPOCH_META_KEY, "0")
                    .await?;
            }
            // If cluster_id already exists, this is idempotent: do not
            // overwrite. Followers replay this command but the seed
            // already wrote it, so the insert is a no-op.
        }
        StorageCommand::SetKlightsMeta { key, value } => {
            backend.set_klights_meta(&key, &value).await?;
        }
        _ => unreachable!("unsupported StorageCommand variant in legacy test adapter"),
    }

    let current_rv = backend.get_current_resource_version().await.unwrap_or(0);
    if current_rv < meta.resource_version {
        backend
            .advance_resource_version_after(meta.resource_version.saturating_sub(1))
            .await?;
    }
    Ok(())
}

#[cfg(any(test, feature = "pod-repository-test-support"))]
/// TO-BE-CLEANUP: legacy replicated StorageCommand apply test support.
async fn align_resource_version_before_replicated_apply<B>(
    backend: &B,
    target_rv: i64,
) -> Result<()>
where
    B: DatastoreBackend + ?Sized,
{
    if target_rv <= 0 {
        return Ok(());
    }
    let current_rv = backend.get_current_resource_version().await.unwrap_or(0);
    let desired_before = target_rv.saturating_sub(1);
    if current_rv < desired_before {
        backend
            .advance_resource_version_after(desired_before.saturating_sub(1))
            .await?;
    }
    Ok(())
}

#[cfg(test)]
#[async_trait]
impl DatastoreApplier for SequencedDatastore {
    async fn apply_command(&self, cmd: StorageCommand, meta: CommandMeta) -> Result<()> {
        apply_command_to_backend(self.passive.as_ref(), cmd, meta).await
    }
}

// Delegate every DatastoreBackend method to self.passive. Public mutation
// methods sequence through the immutable proposal capability; committed apply
// bypasses public admission and writes replicated data to the passive backend.
#[cfg(test)]
#[path = "sequenced_datastore/tests.rs"]
mod tests;
