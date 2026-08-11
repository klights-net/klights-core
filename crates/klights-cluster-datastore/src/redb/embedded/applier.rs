#![cfg(any(test, feature = "test-support"))]
//! TO-BE-CLEANUP: legacy replicated StorageCommand test support only.
//!
//! `DatastoreApplier` implementation for `RedbDatastore`.
//!
//! Delegates each `StorageCommand` variant to the appropriate domain store.

use anyhow::Result;
#[cfg(test)]
use async_trait::async_trait;

#[cfg(test)]
use crate::test_fixtures::live_apply::DatastoreApplier;
use klights_cluster_core::command::{CommandMeta, StorageCommand};
use klights_types::HostPortRange;
use klights_types::NodePeerMode;

use super::RedbDatastore;

impl RedbDatastore {
    /// Preserve the historical root test-command behavior while keeping its
    /// concrete persistence execution in the destination crate.
    pub async fn apply_legacy_test_command(
        &self,
        cmd: StorageCommand,
        _meta: CommandMeta,
    ) -> Result<()> {
        match cmd {
            StorageCommand::CreateResource {
                api_version,
                kind,
                namespace,
                name,
                data,
            } => {
                let committed = self
                    .resources
                    .create_res(&api_version, &kind, namespace.as_deref(), &name, data)
                    .await?;
                self.finish_post_commit(committed);
            }
            StorageCommand::UpdateResource {
                api_version,
                kind,
                namespace,
                name,
                data,
                expected_rv,
                preconditions,
                preserve_status,
            } => {
                let mut preconditions = preconditions;
                if preconditions.resource_version.is_none() && expected_rv > 0 {
                    preconditions.resource_version = Some(expected_rv);
                }
                let committed = if preserve_status {
                    self.resources
                        .update_main_res_with_preconditions(
                            &api_version,
                            &kind,
                            namespace.as_deref(),
                            &name,
                            data,
                            preconditions,
                        )
                        .await?
                } else {
                    self.resources
                        .update_res_with_preconditions(
                            &api_version,
                            &kind,
                            namespace.as_deref(),
                            &name,
                            data,
                            preconditions,
                        )
                        .await?
                };
                self.finish_post_commit(committed);
            }
            StorageCommand::DeleteResource {
                api_version,
                kind,
                namespace,
                name,
                preconditions,
            } => {
                let committed = self
                    .resources
                    .delete_res_with_preconditions(
                        &api_version,
                        &kind,
                        namespace.as_deref(),
                        &name,
                        preconditions,
                    )
                    .await?;
                self.finish_post_commit(committed);
            }
            StorageCommand::DeleteResourceWithTombstone {
                api_version,
                kind,
                namespace,
                name,
                preconditions,
                grace_seconds,
            } => {
                let committed = self
                    .resources
                    .delete_res_with_tombstone(
                        &api_version,
                        &kind,
                        namespace.as_deref(),
                        &name,
                        preconditions,
                        grace_seconds,
                    )
                    .await?;
                self.finish_post_commit(committed);
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
                let committed = self
                    .resources
                    .patch_with_preconditions(
                        &api_version,
                        &kind,
                        namespace.as_deref(),
                        &name,
                        klights_cluster_core::ResourcePatchRequest {
                            patch_kind,
                            patch,
                            preconditions,
                            strict_resource_version,
                        },
                    )
                    .await?;
                self.finish_post_commit(committed);
            }
            StorageCommand::UpdateStatus {
                api_version,
                kind,
                namespace,
                name,
                status,
                expected_rv,
                preconditions,
                observed_status_stamp,
            } => {
                let mut status = status;
                if observed_status_stamp.is_some()
                    && api_version == "v1"
                    && kind == "Pod"
                    && let Some(current) = self
                        .resources
                        .get_res(&api_version, &kind, namespace.as_deref(), &name)
                        .await?
                {
                    klights_cluster_core::merge_status_for_apply(
                        &api_version,
                        &kind,
                        current.data.as_ref(),
                        &mut status,
                        klights_cluster_core::StatusApplyFreshness::Stale,
                        klights_cluster_core::StatusApplyOrigin::KubeletOutbox,
                    );
                }
                let mut preconditions = preconditions;
                if preconditions.resource_version.is_none() {
                    preconditions.resource_version = expected_rv;
                }
                let committed = self
                    .resources
                    .update_status_only_with_preconditions_impl(
                        &api_version,
                        &kind,
                        namespace.as_deref(),
                        &name,
                        status,
                        preconditions,
                    )
                    .await?;
                self.finish_post_commit(committed);
            }
            StorageCommand::CreateNamespace { name, data } => {
                let committed = self.namespaces.create_ns(&name, data).await?;
                self.finish_post_commit(committed);
            }
            StorageCommand::UpdateNamespace {
                name,
                data,
                expected_rv,
            } => {
                self.namespaces
                    .update_ns_impl(&name, data, expected_rv)
                    .await?;
            }
            StorageCommand::DeleteNamespace { name } => {
                self.namespaces.delete_ns_impl(&name).await?;
            }
            StorageCommand::DeleteNamespaceContents { name } => {
                self.namespaces
                    .delete_namespace_contents_impl(&name)
                    .await?;
            }
            StorageCommand::AllocateNodeSubnet {
                node_name,
                subnet,
                node_ip,
            } => {
                self.network
                    .allocate_node_subnet(&node_name, &subnet, &node_ip)
                    .await?;
            }
            StorageCommand::UpdateNodePeerAttributes {
                node_name,
                mode,
                hostport_range,
            } => {
                let peer_mode = match mode.as_str() {
                    "rootless" => NodePeerMode::Rootless,
                    _ => NodePeerMode::Root,
                };
                let hpr = hostport_range
                    .as_deref()
                    .and_then(|s| HostPortRange::parse(s).ok());
                self.network
                    .update_peer_attrs(&node_name, peer_mode, hpr)
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
                self.network.update_node_dataplane(metadata).await?;
            }
            StorageCommand::DeleteNodeSubnet { node_name } => {
                self.network.delete_node_subnet(&node_name).await?;
            }
            StorageCommand::PodSlotTryAdmit { .. } => {
                anyhow::bail!("node-local pod-slot commands cannot apply to cluster redb");
            }
            StorageCommand::PodSlotMarkTerminating { .. } => {
                anyhow::bail!("node-local pod-slot commands cannot apply to cluster redb");
            }
            StorageCommand::PodSlotClearIfUid { .. } => {
                anyhow::bail!("node-local pod-slot commands cannot apply to cluster redb");
            }
            StorageCommand::AdvanceResourceVersion { min_rv, .. } => {
                self.rv_store.advance_rv(min_rv).await?;
            }
            StorageCommand::WatchEventAppend { .. }
            | StorageCommand::ApplyResourceBatch { .. }
            | StorageCommand::GcWatchEvents { .. }
            | StorageCommand::GcAppliedOutbox { .. }
            | StorageCommand::EnsureClusterMetadata { .. }
            | StorageCommand::SetKlightsMeta { .. }
            | StorageCommand::MovePodToCleanupIntent { .. }
            | StorageCommand::DeletePodCleanupIntent { .. }
            | StorageCommand::DeletePodCleanupIntentsForNode { .. } => {
                // Watch events are already recorded during CRUD operations.
                // GC is handled by the gc_watch method.
                // EnsureClusterMetadata is handled by the inner backend's
                // klights_meta table. Pod cleanup intents are SQLite
                // cluster.db state and are no-op for redb.
            }
            _ => unreachable!("unsupported StorageCommand variant in Redb test adapter"),
        }
        Ok(())
    }
}

#[cfg(test)]
#[async_trait]
impl DatastoreApplier for RedbDatastore {
    async fn apply_command(&self, cmd: StorageCommand, meta: CommandMeta) -> Result<()> {
        self.apply_legacy_test_command(cmd, meta).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use klights_cluster_core::{COMMAND_CODEC_VERSION, CommandId, ResourcePreconditions};

    #[tokio::test]
    async fn legacy_committed_apply_main_update_preserves_live_status() {
        let db = RedbDatastore::new_in_memory().await.unwrap();
        let created = db.finish_post_commit(
            db.resources
                .create_res(
                    "apps/v1",
                    "Deployment",
                    Some("default"),
                    "web",
                    serde_json::json!({
                        "apiVersion": "apps/v1",
                        "kind": "Deployment",
                        "metadata": {"name": "web", "namespace": "default", "uid": "web-uid"},
                        "spec": {"replicas": 1},
                        "status": {"replicas": 1}
                    }),
                )
                .await
                .unwrap(),
        );
        db.finish_post_commit(
            db.resources
                .update_status_only_impl(
                    "apps/v1",
                    "Deployment",
                    Some("default"),
                    "web",
                    serde_json::json!({"replicas": 6}),
                    Some(created.resource_version),
                )
                .await
                .unwrap(),
        );
        let mut stale_main = (*created.data).clone();
        stale_main["spec"]["replicas"] = serde_json::json!(2);
        db.apply_legacy_test_command(
            StorageCommand::UpdateResource {
                api_version: "apps/v1".into(),
                kind: "Deployment".into(),
                namespace: Some("default".into()),
                name: "web".into(),
                data: stale_main,
                expected_rv: 0,
                preconditions: ResourcePreconditions::default(),
                preserve_status: true,
            },
            CommandMeta {
                command_id: CommandId("redb-main-update".into()),
                codec_version: COMMAND_CODEC_VERSION,
                resource_version: 0,
                uid: Some("web-uid".into()),
                timestamp_ms: 0,
                authoring_node: "test".into(),
            },
        )
        .await
        .unwrap();

        let updated = db
            .resources
            .get_res("apps/v1", "Deployment", Some("default"), "web")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            updated.data.pointer("/spec/replicas"),
            Some(&serde_json::json!(2))
        );
        assert_eq!(
            updated.data.pointer("/status/replicas"),
            Some(&serde_json::json!(6))
        );
    }
}
