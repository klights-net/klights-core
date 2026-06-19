//! `DatastoreApplier` implementation for the SQLite backend.
//!
//! Maps each `StorageCommand` variant to the corresponding `Datastore`
//! inherent CRUD method.  Node-local operations (sandbox, pod network,
//! workqueue, endpoints) are NOT in the command set and are never
//! routed through the replicated path — they stay as direct local
//! backend calls.

use anyhow::{Result, anyhow};
use async_trait::async_trait;

use crate::datastore::command::{CommandMeta, StorageCommand};
use crate::datastore::replicated::DatastoreApplier;

use super::Datastore;
use crate::networking::VtepMac;

#[async_trait]
impl DatastoreApplier for Datastore {
    async fn apply_command(&self, cmd: StorageCommand, _meta: CommandMeta) -> Result<()> {
        match cmd {
            // -- Resource CRUD --
            StorageCommand::CreateResource {
                api_version,
                kind,
                namespace,
                name,
                data,
            } => {
                self.create_resource(&api_version, &kind, namespace.as_deref(), &name, data)
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
                self.update_resource_with_preconditions(
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
            } => {
                self.patch_resource_latest_with_preconditions(
                    &api_version,
                    &kind,
                    namespace.as_deref(),
                    &name,
                    crate::datastore::ResourcePatchRequest::new(patch_kind, patch, preconditions),
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
                    crate::resource_semantics::preserve_non_kubelet_pod_conditions_on_kubelet_status_update(
                        &api_version,
                        &kind,
                        current.data.as_ref(),
                        &mut status,
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

            // -- Namespace operations --
            StorageCommand::CreateNamespace { name, data } => {
                self.create_namespace(&name, data).await?;
            }
            StorageCommand::UpdateNamespace {
                name,
                data,
                expected_rv,
            } => {
                self.update_namespace(&name, data, expected_rv).await?;
            }
            StorageCommand::DeleteNamespace { name } => {
                self.delete_namespace(&name).await?;
            }
            StorageCommand::DeleteNamespaceContents { name } => {
                self.delete_namespace_contents(&name).await?;
            }

            // -- Cluster-internal state --
            StorageCommand::AllocateNodeSubnet {
                node_name,
                subnet,
                node_ip,
            } => {
                self.allocate_node_subnet(&node_name, &subnet, &node_ip)
                    .await?;
            }
            StorageCommand::UpdateNodeVtepMac {
                node_name,
                vtep_mac,
            } => {
                let mac = VtepMac::parse(&vtep_mac)
                    .map_err(|e| anyhow!("invalid VTEP MAC '{}': {}", vtep_mac, e))?;
                self.update_node_vtep_mac(&node_name, &mac).await?;
            }
            StorageCommand::UpdateNodePeerAttributes {
                node_name,
                mode,
                hostport_range,
            } => {
                let peer_mode = crate::controllers::annotations::parse_node_peer_mode(Some(&mode))
                    .unwrap_or_else(|_| {
                        tracing::warn!(
                            "unknown peer mode '{}' in StorageCommand, defaulting to Root",
                            mode
                        );
                        crate::controllers::annotations::NodePeerMode::Root
                    });
                let hpr = hostport_range
                    .as_deref()
                    .and_then(|s| crate::networking::types::HostPortRange::parse(s).ok());
                self.update_node_peer_attributes(&node_name, peer_mode, hpr)
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
                let metadata = dataplane_metadata_from_parts(
                    node_name, mode, encryption, public_key, endpoint, port,
                )?;
                self.update_node_dataplane(metadata).await?;
            }
            StorageCommand::DeleteNodeSubnet { node_name } => {
                self.delete_node_subnet(&node_name).await?;
            }
            StorageCommand::PodSlotTryAdmit {
                namespace,
                pod_name,
                pod_uid,
                node_name,
            } => {
                self.pod_slot_try_admit(&namespace, &pod_name, &pod_uid, &node_name)
                    .await?;
            }
            StorageCommand::PodSlotMarkTerminating {
                namespace,
                pod_name,
                pod_uid,
                node_name,
            } => {
                self.pod_slot_mark_terminating(&namespace, &pod_name, &pod_uid, &node_name)
                    .await?;
            }
            StorageCommand::PodSlotClearIfUid {
                namespace,
                pod_name,
                pod_uid,
                node_name,
            } => {
                self.pod_slot_clear_if_uid(&namespace, &pod_name, &pod_uid, &node_name)
                    .await?;
            }
            StorageCommand::MovePodToCleanupIntent {
                node_name,
                namespace,
                pod_name,
                pod_uid,
                reason,
            } => {
                self.move_pod_to_cleanup_intent(
                    &node_name, &namespace, &pod_name, &pod_uid, &reason,
                )
                .await?;
            }
            StorageCommand::DeletePodCleanupIntent {
                node_name,
                namespace,
                pod_name,
                pod_uid,
                reason,
            } => {
                self.delete_pod_cleanup_intent(
                    &node_name, &namespace, &pod_name, &pod_uid, &reason,
                )
                .await?;
            }
            StorageCommand::DeletePodCleanupIntentsForNode { node_name } => {
                self.delete_pod_cleanup_intents_for_node(&node_name).await?;
            }

            // -- Watch history --
            StorageCommand::WatchEventAppend { event_bytes, rv } => {
                self.db_call("applier:watch_event_append", move |conn| {
                    // Idempotent: at-least-once snapshot replay can deliver
                    // the same WatchEventAppend twice. The shared helper
                    // verifies content equality on UNIQUE conflict.
                    super::crud::helpers::insert_watch_event_in_conn(
                        conn,
                        super::crud::helpers::WatchEventInsert::new(
                            "internal",
                            "StorageCommand",
                            None,
                            "cmd",
                            rv,
                            "APPLIED",
                            &event_bytes,
                        ),
                    )?;
                    Ok(())
                })
                .await
                .map_err(|e| anyhow!("failed to append watch event: {}", e))?;
            }

            // -- Maintenance --
            StorageCommand::GcWatchEvents {
                max_rows,
                batch_cap,
            } => {
                self.gc_watch_events(max_rows, batch_cap).await?;
            }
            StorageCommand::AdvanceResourceVersion { min_rv, new_rv: _ } => {
                // advance_resource_version_after bumps the RV if it's below min_rv.
                // The command's new_rv is ignored in single-node mode (it's for HA).
                self.advance_resource_version_after(min_rv).await?;
            }
            StorageCommand::EnsureClusterMetadata { .. } => {
                // Handled by apply_command_to_backend which calls
                // set_klights_meta on the real backend. The applier
                // runs inside that call.
            }
            StorageCommand::SetKlightsMeta { .. } => {
                // Same as EnsureClusterMetadata: applied by
                // apply_command_to_backend.
            }
        }
        Ok(())
    }
}

fn dataplane_metadata_from_parts(
    node_name: String,
    mode: String,
    encryption: String,
    public_key: Option<String>,
    endpoint: String,
    port: Option<u16>,
) -> Result<crate::networking::wireguard::DataplanePeerMetadata> {
    crate::networking::wireguard::DataplanePeerMetadata::try_new(
        node_name,
        crate::networking::wireguard::DataplaneMode::parse(&mode)?,
        crate::networking::wireguard::DataplaneEncryption::parse(Some(&encryption))?,
        public_key,
        Some(endpoint),
        port,
    )
}
