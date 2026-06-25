//! `DatastoreApplier` implementation for `RedbDatastore`.
//!
//! Delegates each `StorageCommand` variant to the appropriate domain store.

use anyhow::{Result, anyhow};
use async_trait::async_trait;

use crate::controllers::annotations::NodePeerMode;
use crate::datastore::backend::DatastoreBackend;
use crate::datastore::command::{CommandMeta, StorageCommand};
use crate::datastore::replicated::DatastoreApplier;
use crate::networking::VtepMac;
use crate::networking::types::HostPortRange;

use super::RedbDatastore;

#[async_trait]
impl DatastoreApplier for RedbDatastore {
    async fn apply_command(&self, cmd: StorageCommand, _meta: CommandMeta) -> Result<()> {
        match cmd {
            StorageCommand::CreateResource {
                api_version,
                kind,
                namespace,
                name,
                data,
            } => {
                self.resources
                    .create_res(&api_version, &kind, namespace.as_deref(), &name, data)
                    .await?;
            }
            StorageCommand::UpdateResource {
                api_version,
                kind,
                namespace,
                name,
                data,
                expected_rv,
                preconditions,
            } => {
                let mut preconditions = preconditions;
                if preconditions.resource_version.is_none() {
                    preconditions.resource_version = Some(expected_rv);
                }
                self.resources
                    .update_res_with_preconditions(
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
                self.delete_resource_with_preconditions(
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
                self.patch_resource_latest_with_preconditions(
                    &api_version,
                    &kind,
                    namespace.as_deref(),
                    &name,
                    crate::datastore::ResourcePatchRequest {
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
                expected_rv,
                preconditions,
                observed_status_stamp,
            } => {
                let mut status = status;
                if observed_status_stamp.is_some()
                    && api_version == "v1"
                    && kind == "Pod"
                    && let Some(current) = self
                        .get_resource(&api_version, &kind, namespace.as_deref(), &name)
                        .await?
                {
                    crate::pod_status_merge::merge_pod_status_for_update(
                        &api_version,
                        &kind,
                        current.data.as_ref(),
                        &mut status,
                        crate::pod_status_merge::PodStatusOwner::KubeletRuntime,
                    );
                }
                let mut preconditions = preconditions;
                if preconditions.resource_version.is_none() {
                    preconditions.resource_version = expected_rv;
                }
                self.update_status_only_with_preconditions(
                    &api_version,
                    &kind,
                    namespace.as_deref(),
                    &name,
                    status,
                    preconditions,
                )
                .await?;
            }
            StorageCommand::CreateNamespace { name, data } => {
                self.namespaces.create_ns(&name, data).await?;
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
            StorageCommand::UpdateNodeVtepMac {
                node_name,
                vtep_mac,
            } => {
                let mac =
                    VtepMac::parse(&vtep_mac).map_err(|e| anyhow!("invalid vtep_mac: {e}"))?;
                self.network.update_vtep_mac(&node_name, &mac).await?;
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
                let metadata = crate::networking::wireguard::DataplanePeerMetadata::try_new(
                    node_name,
                    crate::networking::wireguard::DataplaneMode::parse(&mode)?,
                    crate::networking::wireguard::DataplaneEncryption::parse(Some(&encryption))?,
                    public_key,
                    Some(endpoint),
                    port,
                )?;
                self.network.update_node_dataplane(metadata).await?;
            }
            StorageCommand::DeleteNodeSubnet { node_name } => {
                self.network.delete_node_subnet(&node_name).await?;
            }
            StorageCommand::PodSlotTryAdmit {
                namespace,
                pod_name,
                pod_uid,
                node_name,
            } => {
                self.pod_slots
                    .try_admit(&namespace, &pod_name, &pod_uid, &node_name)
                    .await?;
            }
            StorageCommand::PodSlotMarkTerminating {
                namespace,
                pod_name,
                pod_uid,
                node_name,
            } => {
                self.pod_slots
                    .mark_terminating(&namespace, &pod_name, &pod_uid, &node_name)
                    .await?;
            }
            StorageCommand::PodSlotClearIfUid {
                namespace,
                pod_name,
                pod_uid,
                node_name,
            } => {
                self.pod_slots
                    .clear_if_uid(&namespace, &pod_name, &pod_uid, &node_name)
                    .await?;
            }
            StorageCommand::AdvanceResourceVersion { min_rv, .. } => {
                self.rv_store.advance_rv(min_rv).await?;
            }
            StorageCommand::WatchEventAppend { .. }
            | StorageCommand::ApplyResourceBatch { .. }
            | StorageCommand::GcWatchEvents { .. }
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
        }
        Ok(())
    }
}
