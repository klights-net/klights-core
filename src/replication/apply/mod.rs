//! Replica-side application of forwarded commands.
//!
//! This is the I/O shell of the apply path. Pure helpers (subject keys,
//! response shaping, RV math, meta construction) live in
//! [`core`]. The encode/decode boundary lives in [`codec`] behind a
//! [`codec::CommandCodec`] trait so tests can swap in a fake.
//!
//! The two entry points are:
//! - [`apply_forwarded_command_with_meta`] — full path with idempotency
//!   dedup against the applied-outbox.
//! - [`apply_forwarded_command`] — inner mutation path used by replicas
//!   that have already done their own dedup.

use anyhow::{Result, anyhow};

use crate::datastore::backend::DatastoreBackend;
use crate::datastore::command::{CommandMeta, StorageCommand, encode_response_protobuf};
use crate::datastore::{AppliedOutboxRecord, ResourcePatchRequest, ResourcePreconditions};
use crate::replication::protocol::ReplicationEntry;
use serde_json::Value;

pub mod codec;
mod core;

pub use core::ForwardedApply;

use core::{
    ack_apply, applied_resource_version, current_epoch_millis, entry_for_resource, meta_for_rv,
    storage_response_for_apply, subject_key_for_command,
};

pub async fn apply_forwarded_command_with_meta(
    db: &dyn DatastoreBackend,
    command: StorageCommand,
    meta: CommandMeta,
) -> Result<ForwardedApply> {
    if crate::node_lease_tracker::ensure_lease_renew_command(&command, &meta.authoring_node).is_ok()
    {
        return Ok(ForwardedApply::already_applied());
    }
    let idempotency_key = meta.command_id.0.as_str();
    if db.get_applied_outbox(idempotency_key).await?.is_some() {
        return Ok(ForwardedApply::already_applied());
    }

    let subject_key = subject_key_for_command(&command);
    let operation = command.variant_name().to_string();
    let applied = apply_forwarded_command(db, command, meta.authoring_node).await?;
    let applied_rv = applied_resource_version(&applied);
    let result_proto = encode_response_protobuf(&storage_response_for_apply(&applied, applied_rv))?;
    let inserted = db
        .insert_applied_outbox(AppliedOutboxRecord {
            idempotency_key: idempotency_key.to_string(),
            subject_key,
            operation,
            first_seen_ms: current_epoch_millis(),
            applied_rv,
            result_proto,
        })
        .await?;
    if inserted {
        Ok(applied)
    } else {
        Ok(ForwardedApply::already_applied())
    }
}

pub async fn apply_forwarded_command(
    db: &dyn DatastoreBackend,
    command: StorageCommand,
    authoring_node: String,
) -> Result<ForwardedApply> {
    if crate::node_lease_tracker::ensure_lease_renew_command(&command, &authoring_node).is_ok() {
        return Ok(ForwardedApply::already_applied());
    }
    match command {
        StorageCommand::CreateResource {
            api_version,
            kind,
            namespace,
            name,
            mut data,
        } => {
            apply_forwarded_node_routing_metadata(
                db,
                &api_version,
                &kind,
                namespace.as_deref(),
                &name,
                &authoring_node,
                &mut data,
            )
            .await?;
            let resource = db
                .create_resource(&api_version, &kind, namespace.as_deref(), &name, data)
                .await?;
            let entry = entry_for_resource(
                StorageCommand::CreateResource {
                    api_version,
                    kind,
                    namespace,
                    name,
                    data: (*resource.data).clone(),
                },
                &resource,
                authoring_node,
            );
            Ok(ForwardedApply {
                entry: Some(entry),
                resource: Some(resource.into()),
                node_subnet: None,
                pod_slot_admission: None,
                already_applied: false,
            })
        }
        StorageCommand::UpdateResource {
            api_version,
            kind,
            namespace,
            name,
            mut data,
            expected_rv,
            preconditions,
        } => {
            apply_forwarded_node_routing_metadata(
                db,
                &api_version,
                &kind,
                namespace.as_deref(),
                &name,
                &authoring_node,
                &mut data,
            )
            .await?;
            if should_apply_forwarded_update_against_latest(
                &api_version,
                &kind,
                namespace.as_deref(),
                &name,
                &authoring_node,
            ) {
                let (resource, applied_preconditions) = apply_forwarded_update_against_latest(
                    db,
                    &api_version,
                    &kind,
                    namespace.as_deref(),
                    &name,
                    data,
                    &preconditions,
                )
                .await?;
                let applied_expected_rv = applied_preconditions.resource_version.unwrap_or(0);
                let entry = entry_for_resource(
                    StorageCommand::UpdateResource {
                        api_version,
                        kind,
                        namespace,
                        name,
                        data: (*resource.data).clone(),
                        expected_rv: applied_expected_rv,
                        preconditions: applied_preconditions,
                    },
                    &resource,
                    authoring_node,
                );
                return Ok(ForwardedApply {
                    entry: Some(entry),
                    resource: Some(resource.into()),
                    node_subnet: None,
                    pod_slot_admission: None,
                    already_applied: false,
                });
            }
            let mut preconditions = preconditions;
            if preconditions.resource_version.is_none() && expected_rv > 0 {
                preconditions.resource_version = Some(expected_rv);
            }
            let resource = db
                .update_resource_with_preconditions(
                    &api_version,
                    &kind,
                    namespace.as_deref(),
                    &name,
                    data,
                    preconditions.clone(),
                )
                .await?;
            let entry = entry_for_resource(
                StorageCommand::UpdateResource {
                    api_version,
                    kind,
                    namespace,
                    name,
                    data: (*resource.data).clone(),
                    expected_rv,
                    preconditions,
                },
                &resource,
                authoring_node,
            );
            Ok(ForwardedApply {
                entry: Some(entry),
                resource: Some(resource.into()),
                node_subnet: None,
                pod_slot_admission: None,
                already_applied: false,
            })
        }
        StorageCommand::UpdateStatus {
            api_version,
            kind,
            namespace,
            name,
            status,
            expected_rv,
            preconditions,
        } => {
            let mut preconditions = preconditions;
            if preconditions.resource_version.is_none() {
                preconditions.resource_version = expected_rv;
            }
            let resource = db
                .update_status_only_with_preconditions(
                    &api_version,
                    &kind,
                    namespace.as_deref(),
                    &name,
                    status,
                    preconditions.clone(),
                )
                .await?;
            let entry = entry_for_resource(
                StorageCommand::UpdateStatus {
                    api_version,
                    kind,
                    namespace,
                    name,
                    status: resource
                        .data
                        .get("status")
                        .cloned()
                        .unwrap_or(serde_json::Value::Null),
                    expected_rv,
                    preconditions,
                },
                &resource,
                authoring_node,
            );
            Ok(ForwardedApply {
                entry: Some(entry),
                resource: Some(resource.into()),
                node_subnet: None,
                pod_slot_admission: None,
                already_applied: false,
            })
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
            let before_rv = db.get_current_resource_version().await.unwrap_or(0);
            let resource = db
                .patch_resource_latest_with_preconditions(
                    &api_version,
                    &kind,
                    namespace.as_deref(),
                    &name,
                    ResourcePatchRequest::new(patch_kind, patch.clone(), preconditions.clone()),
                )
                .await?;
            if let Some(resource) = resource {
                let entry = entry_for_resource(
                    StorageCommand::PatchResource {
                        api_version,
                        kind,
                        namespace,
                        name,
                        patch_kind,
                        patch,
                        preconditions,
                    },
                    &resource,
                    authoring_node,
                );
                Ok(ForwardedApply {
                    entry: Some(entry),
                    resource: Some(resource.into()),
                    node_subnet: None,
                    pod_slot_admission: None,
                    already_applied: false,
                })
            } else {
                let rv = resource_version_after_mutation(db, before_rv).await?;
                Ok(ForwardedApply {
                    entry: Some(ReplicationEntry {
                        command: StorageCommand::PatchResource {
                            api_version,
                            kind,
                            namespace,
                            name,
                            patch_kind,
                            patch,
                            preconditions,
                        },
                        meta: meta_for_rv(rv, None, authoring_node),
                    }),
                    resource: None,
                    node_subnet: None,
                    pod_slot_admission: None,
                    already_applied: false,
                })
            }
        }
        StorageCommand::DeleteResource {
            api_version,
            kind,
            namespace,
            name,
            preconditions,
        } => {
            // For v1/Pod deletes we need the pre-delete resource to fire
            // downstream side effects (workload-owner reconcile, Service
            // reconcile). Without it, the leader's outbox apply path has
            // no ownerReferences to derive the owning StatefulSet /
            // ReplicaSet / Deployment, so StS slot recreates after pod
            // finalization stall until the next unrelated reconcile.
            let pre_delete_resource = if api_version == "v1" && kind == "Pod" {
                db.get_resource(&api_version, &kind, namespace.as_deref(), &name)
                    .await
                    .ok()
                    .flatten()
            } else {
                None
            };
            let before_rv = db.get_current_resource_version().await.unwrap_or(0);
            db.delete_resource_with_preconditions(
                &api_version,
                &kind,
                namespace.as_deref(),
                &name,
                preconditions.clone(),
            )
            .await?;
            let rv = resource_version_after_mutation(db, before_rv).await?;
            let mut applied = ack_apply(
                StorageCommand::DeleteResource {
                    api_version,
                    kind,
                    namespace,
                    name,
                    preconditions,
                },
                rv,
                authoring_node,
            );
            if let Some(res) = pre_delete_resource {
                applied.resource = Some(res.into());
            }
            Ok(applied)
        }
        StorageCommand::CreateNamespace { name, data } => {
            let resource = db.create_namespace(&name, data).await?;
            let entry = entry_for_resource(
                StorageCommand::CreateNamespace {
                    name,
                    data: (*resource.data).clone(),
                },
                &resource,
                authoring_node,
            );
            Ok(ForwardedApply {
                entry: Some(entry),
                resource: Some(resource.into()),
                node_subnet: None,
                pod_slot_admission: None,
                already_applied: false,
            })
        }
        StorageCommand::UpdateNamespace {
            name,
            data,
            expected_rv,
        } => {
            let resource = db.update_namespace(&name, data, expected_rv).await?;
            let entry = entry_for_resource(
                StorageCommand::UpdateNamespace {
                    name,
                    data: (*resource.data).clone(),
                    expected_rv,
                },
                &resource,
                authoring_node,
            );
            Ok(ForwardedApply {
                entry: Some(entry),
                resource: Some(resource.into()),
                node_subnet: None,
                pod_slot_admission: None,
                already_applied: false,
            })
        }
        StorageCommand::DeleteNamespace { name } => {
            let before_rv = db.get_current_resource_version().await.unwrap_or(0);
            db.delete_namespace(&name).await?;
            let rv = resource_version_after_mutation(db, before_rv).await?;
            Ok(ack_apply(
                StorageCommand::DeleteNamespace { name },
                rv,
                authoring_node,
            ))
        }
        StorageCommand::DeleteNamespaceContents { name } => {
            let before_rv = db.get_current_resource_version().await.unwrap_or(0);
            db.delete_namespace_contents(&name).await?;
            let rv = resource_version_after_mutation(db, before_rv).await?;
            Ok(ack_apply(
                StorageCommand::DeleteNamespaceContents { name },
                rv,
                authoring_node,
            ))
        }
        StorageCommand::AllocateNodeSubnet {
            node_name,
            subnet,
            node_ip,
        } => {
            let before_rv = db.get_current_resource_version().await.unwrap_or(0);
            let node_subnet = db
                .allocate_node_subnet(&node_name, &subnet, &node_ip)
                .await?;
            let rv = resource_version_after_mutation(db, before_rv).await?;
            Ok(ForwardedApply {
                entry: Some(ReplicationEntry {
                    command: StorageCommand::AllocateNodeSubnet {
                        node_name,
                        subnet,
                        node_ip,
                    },
                    meta: meta_for_rv(rv, None, authoring_node),
                }),
                resource: None,
                node_subnet: Some(node_subnet.into()),
                pod_slot_admission: None,
                already_applied: false,
            })
        }
        StorageCommand::UpdateNodeVtepMac {
            node_name,
            vtep_mac,
        } => {
            let before_rv = db.get_current_resource_version().await.unwrap_or(0);
            let mac = crate::networking::VtepMac::parse(&vtep_mac)
                .map_err(|err| anyhow!("invalid VTEP MAC '{}': {}", vtep_mac, err))?;
            db.update_node_vtep_mac(&node_name, &mac).await?;
            let rv = resource_version_after_mutation(db, before_rv).await?;
            Ok(ack_apply(
                StorageCommand::UpdateNodeVtepMac {
                    node_name,
                    vtep_mac,
                },
                rv,
                authoring_node,
            ))
        }
        StorageCommand::UpdateNodePeerAttributes {
            node_name,
            mode,
            hostport_range,
        } => {
            let before_rv = db.get_current_resource_version().await.unwrap_or(0);
            let peer_mode = crate::controllers::annotations::parse_node_peer_mode(Some(&mode))
                .unwrap_or(crate::controllers::annotations::NodePeerMode::Root);
            let hpr = hostport_range
                .as_deref()
                .and_then(|value| crate::networking::types::HostPortRange::parse(value).ok());
            db.update_node_peer_attributes(&node_name, peer_mode, hpr)
                .await?;
            let rv = resource_version_after_mutation(db, before_rv).await?;
            Ok(ack_apply(
                StorageCommand::UpdateNodePeerAttributes {
                    node_name,
                    mode,
                    hostport_range,
                },
                rv,
                authoring_node,
            ))
        }
        StorageCommand::UpdateNodeDataplane {
            node_name,
            mode,
            encryption,
            public_key,
            endpoint,
            port,
        } => {
            let before_rv = db.get_current_resource_version().await.unwrap_or(0);
            let metadata = crate::networking::wireguard::DataplanePeerMetadata::try_new(
                node_name.clone(),
                crate::networking::wireguard::DataplaneMode::parse(&mode)?,
                crate::networking::wireguard::DataplaneEncryption::parse(Some(&encryption))?,
                public_key.clone(),
                Some(endpoint.clone()),
                port,
            )?;
            db.update_node_dataplane(metadata).await?;
            publish_node_routing_metadata_update(db, &node_name).await?;
            let rv = resource_version_after_mutation(db, before_rv).await?;
            Ok(ack_apply(
                StorageCommand::UpdateNodeDataplane {
                    node_name,
                    mode,
                    encryption,
                    public_key,
                    endpoint,
                    port,
                },
                rv,
                authoring_node,
            ))
        }
        StorageCommand::DeleteNodeSubnet { node_name } => {
            let before_rv = db.get_current_resource_version().await.unwrap_or(0);
            db.delete_node_subnet(&node_name).await?;
            let rv = resource_version_after_mutation(db, before_rv).await?;
            Ok(ack_apply(
                StorageCommand::DeleteNodeSubnet { node_name },
                rv,
                authoring_node,
            ))
        }
        StorageCommand::PodSlotTryAdmit {
            namespace,
            pod_name,
            pod_uid,
            node_name,
        } => {
            let result = db
                .pod_slot_try_admit(&namespace, &pod_name, &pod_uid, &node_name)
                .await?;
            let rv = match &result {
                crate::datastore::types::PodSlotAdmissionResult::Admitted { resource_version }
                | crate::datastore::types::PodSlotAdmissionResult::Blocked {
                    resource_version,
                    ..
                } => *resource_version,
            };
            let entry = if matches!(
                result,
                crate::datastore::types::PodSlotAdmissionResult::Admitted { .. }
            ) {
                Some(ReplicationEntry {
                    command: StorageCommand::PodSlotTryAdmit {
                        namespace,
                        pod_name,
                        pod_uid,
                        node_name,
                    },
                    meta: meta_for_rv(rv, None, authoring_node),
                })
            } else {
                None
            };
            Ok(ForwardedApply {
                entry,
                resource: None,
                node_subnet: None,
                pod_slot_admission: Some(result.into()),
                already_applied: false,
            })
        }
        StorageCommand::PodSlotMarkTerminating {
            namespace,
            pod_name,
            pod_uid,
            node_name,
        } => {
            let before_rv = db.get_current_resource_version().await.unwrap_or(0);
            db.pod_slot_mark_terminating(&namespace, &pod_name, &pod_uid, &node_name)
                .await?;
            let rv = resource_version_after_mutation(db, before_rv).await?;
            Ok(ack_apply(
                StorageCommand::PodSlotMarkTerminating {
                    namespace,
                    pod_name,
                    pod_uid,
                    node_name,
                },
                rv,
                authoring_node,
            ))
        }
        StorageCommand::PodSlotClearIfUid {
            namespace,
            pod_name,
            pod_uid,
            node_name,
        } => {
            let before_rv = db.get_current_resource_version().await.unwrap_or(0);
            db.pod_slot_clear_if_uid(&namespace, &pod_name, &pod_uid, &node_name)
                .await?;
            let rv = resource_version_after_mutation(db, before_rv).await?;
            Ok(ack_apply(
                StorageCommand::PodSlotClearIfUid {
                    namespace,
                    pod_name,
                    pod_uid,
                    node_name,
                },
                rv,
                authoring_node,
            ))
        }
        StorageCommand::MovePodToCleanupIntent {
            node_name,
            namespace,
            pod_name,
            pod_uid,
            reason,
        } => {
            let before_rv = db.get_current_resource_version().await.unwrap_or(0);
            db.move_pod_to_cleanup_intent(&node_name, &namespace, &pod_name, &pod_uid, &reason)
                .await?;
            let rv = resource_version_after_mutation(db, before_rv).await?;
            Ok(ack_apply(
                StorageCommand::MovePodToCleanupIntent {
                    node_name,
                    namespace,
                    pod_name,
                    pod_uid,
                    reason,
                },
                rv,
                authoring_node,
            ))
        }
        StorageCommand::DeletePodCleanupIntent {
            node_name,
            namespace,
            pod_name,
            pod_uid,
            reason,
        } => {
            let before_rv = db.get_current_resource_version().await.unwrap_or(0);
            db.delete_pod_cleanup_intent(&node_name, &namespace, &pod_name, &pod_uid, &reason)
                .await?;
            let rv = resource_version_after_mutation(db, before_rv).await?;
            Ok(ack_apply(
                StorageCommand::DeletePodCleanupIntent {
                    node_name,
                    namespace,
                    pod_name,
                    pod_uid,
                    reason,
                },
                rv,
                authoring_node,
            ))
        }
        StorageCommand::DeletePodCleanupIntentsForNode { node_name } => {
            let before_rv = db.get_current_resource_version().await.unwrap_or(0);
            db.delete_pod_cleanup_intents_for_node(&node_name).await?;
            let rv = resource_version_after_mutation(db, before_rv).await?;
            Ok(ack_apply(
                StorageCommand::DeletePodCleanupIntentsForNode { node_name },
                rv,
                authoring_node,
            ))
        }
        StorageCommand::GcWatchEvents {
            max_rows,
            batch_cap,
        } => {
            let before_rv = db.get_current_resource_version().await.unwrap_or(0);
            db.gc_watch_events(max_rows, batch_cap).await?;
            let rv = resource_version_after_mutation(db, before_rv).await?;
            Ok(ack_apply(
                StorageCommand::GcWatchEvents {
                    max_rows,
                    batch_cap,
                },
                rv,
                authoring_node,
            ))
        }
        StorageCommand::WatchEventAppend { .. }
        | StorageCommand::AdvanceResourceVersion { .. }
        | StorageCommand::EnsureClusterMetadata { .. }
        | StorageCommand::SetKlightsMeta { .. } => Err(anyhow!(
            "{} cannot be forwarded from a replica",
            command.variant_name()
        )),
    }
}

/// On Node create/update, stamp Kubernetes-visible routing metadata from the
/// internal node_subnet/node_dataplane stores. Workers consume the resulting
/// Node event through klights' internal watch path and reconcile routes locally.
async fn apply_forwarded_node_routing_metadata(
    db: &dyn DatastoreBackend,
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
    authoring_node: &str,
    data: &mut serde_json::Value,
) -> Result<()> {
    if api_version != "v1" || kind != "Node" || namespace.is_some() {
        return Ok(());
    }
    if name == authoring_node {
        crate::kubelet::node::stamp_node_routing_metadata_and_external_ip_from_store(
            db, name, data,
        )
        .await?;
    } else {
        crate::kubelet::node::stamp_node_routing_metadata_from_store(db, name, data).await?;
    }
    Ok(())
}

fn should_apply_forwarded_update_against_latest(
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
    authoring_node: &str,
) -> bool {
    name == authoring_node && api_version == "v1" && kind == "Node" && namespace.is_none()
}

async fn apply_forwarded_update_against_latest(
    db: &dyn DatastoreBackend,
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
    mut data: Value,
    forwarded_preconditions: &ResourcePreconditions,
) -> Result<(crate::datastore::Resource, ResourcePreconditions)> {
    let live = db
        .get_resource(api_version, kind, namespace, name)
        .await?
        .ok_or_else(|| anyhow!("{api_version}/{kind} {name} not found"))?;
    validate_forwarded_uid_precondition(forwarded_preconditions, &live)?;

    if api_version == "v1" && kind == "Node" && namespace.is_none() {
        crate::kubelet::node::merge_existing_node_mutable_fields(&mut data, &live.data);
    }

    let latest_preconditions = ResourcePreconditions::from_resource(&live);
    let resource = db
        .update_resource_with_preconditions(
            api_version,
            kind,
            namespace,
            name,
            data,
            latest_preconditions.clone(),
        )
        .await?;
    Ok((resource, latest_preconditions))
}

fn validate_forwarded_uid_precondition(
    preconditions: &ResourcePreconditions,
    live: &crate::datastore::Resource,
) -> Result<()> {
    let Some(expected_uid) = preconditions.uid.as_deref().filter(|uid| !uid.is_empty()) else {
        return Ok(());
    };
    if live.uid == expected_uid {
        return Ok(());
    }
    Err(anyhow!(
        "uid mismatch: expected {expected_uid} got {}",
        live.uid
    ))
}

async fn publish_node_routing_metadata_update(
    db: &dyn DatastoreBackend,
    node_name: &str,
) -> Result<()> {
    let Some(resource) = db.get_resource("v1", "Node", None, node_name).await? else {
        return Ok(());
    };
    let mut data = (*resource.data).clone();
    if !crate::kubelet::node::stamp_node_routing_metadata_and_external_ip_from_store(
        db, node_name, &mut data,
    )
    .await?
    {
        return Ok(());
    }
    db.update_resource_with_preconditions(
        "v1",
        "Node",
        None,
        node_name,
        data,
        ResourcePreconditions::from_resource(&resource),
    )
    .await?;
    Ok(())
}

/// Read the current RV; if it didn't advance past `before_rv`, force an
/// advance. Used after delete/gc/admin operations that don't return a
/// post-mutation RV directly.
async fn resource_version_after_mutation(db: &dyn DatastoreBackend, before_rv: i64) -> Result<i64> {
    let after_rv = db.get_current_resource_version().await.unwrap_or(before_rv);
    if after_rv > before_rv {
        Ok(after_rv)
    } else {
        db.advance_resource_version_after(before_rv.saturating_add(1))
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datastore::command::{COMMAND_CODEC_VERSION, CommandId, CommandMeta};
    use crate::datastore::types::ResourcePreconditions;
    use serde_json::json;

    #[tokio::test]
    async fn forwarded_running_state_wins_over_live_containercreating() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "starting-pod",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "starting-pod",
                    "namespace": "default",
                    "uid": "uid-starting-pod"
                },
                "spec": {
                    "nodeName": "worker-1",
                    "containers": [{"name": "c", "image": "busybox"}]
                },
                "status": {
                    "phase": "Running",
                    "conditions": [
                        {"type": "PodScheduled", "status": "True", "lastTransitionTime": "2026-05-17T16:17:39Z"},
                        {"type": "Initialized", "status": "True", "lastTransitionTime": "2026-05-17T16:17:39Z"},
                        {"type": "ContainersReady", "status": "False", "lastTransitionTime": "2026-05-17T16:18:10Z"},
                        {"type": "Ready", "status": "False", "lastTransitionTime": "2026-05-17T16:18:10Z"}
                    ],
                    "containerStatuses": [{
                        "name": "c",
                        "containerID": "containerd://same-container",
                        "ready": false,
                        "started": false,
                        "restartCount": 0,
                        "state": {"waiting": {"reason": "ContainerCreating"}}
                    }]
                }
            }),
        )
        .await
        .unwrap();

        apply_forwarded_command(
            &db,
            StorageCommand::UpdateStatus {
                api_version: "v1".into(),
                kind: "Pod".into(),
                namespace: Some("default".into()),
                name: "starting-pod".into(),
                status: json!({
                    "phase": "Running",
                    "conditions": [
                        {"type": "PodScheduled", "status": "True", "lastTransitionTime": "2026-05-17T16:17:39Z"},
                        {"type": "Initialized", "status": "True", "lastTransitionTime": "2026-05-17T16:17:39Z"},
                        {"type": "ContainersReady", "status": "True", "lastTransitionTime": "2026-05-17T16:18:00Z"},
                        {"type": "Ready", "status": "True", "lastTransitionTime": "2026-05-17T16:18:00Z"}
                    ],
                    "containerStatuses": [{
                        "name": "c",
                        "containerID": "containerd://same-container",
                        "image": "busybox",
                        "imageID": "sha256:test",
                        "ready": true,
                        "started": true,
                        "restartCount": 0,
                        "state": {"running": {"startedAt": "2026-05-17T16:18:00Z"}}
                    }]
                }),
                expected_rv: None,
                preconditions: ResourcePreconditions {
                    uid: Some("uid-starting-pod".into()),
                    resource_version: None,
                },
            },
            "worker-1".into(),
        )
        .await
        .unwrap();

        let stored = db
            .get_resource("v1", "Pod", Some("default"), "starting-pod")
            .await
            .unwrap()
            .unwrap();
        let status = &stored.data["status"]["containerStatuses"][0];
        assert_eq!(
            status.pointer("/state/running/startedAt"),
            Some(&json!("2026-05-17T16:18:00Z")),
            "runtime-confirmed running state must not be overwritten by stale ContainerCreating"
        );
        assert_eq!(status["ready"], json!(true));
        assert_eq!(status["started"], json!(true));
    }

    #[tokio::test]
    async fn forwarded_worker_status_restores_pod_unknown_after_node_reconnect() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "reconnected-pod",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "reconnected-pod",
                    "namespace": "default",
                    "uid": "uid-reconnected-pod"
                },
                "spec": {
                    "nodeName": "worker-1",
                    "containers": [{"name": "c", "image": "busybox"}]
                },
                "status": {
                    "phase": "Unknown",
                    "podIP": "10.50.1.9",
                    "podIPs": [{"ip": "10.50.1.9"}],
                    "hostIP": "10.99.0.11",
                    "hostIPs": [{"ip": "10.99.0.11"}],
                    "conditions": [
                        {"type": "PodScheduled", "status": "True", "lastTransitionTime": "2026-05-17T16:17:39Z"},
                        {"type": "Initialized", "status": "True", "lastTransitionTime": "2026-05-17T16:17:39Z"},
                        {"type": "ContainersReady", "status": "Unknown", "reason": "NodeStatusUnknown", "lastTransitionTime": "2026-05-17T16:18:00Z"},
                        {"type": "Ready", "status": "Unknown", "reason": "NodeStatusUnknown", "lastTransitionTime": "2026-05-17T16:18:00Z"}
                    ],
                    "containerStatuses": [{
                        "name": "c",
                        "ready": false,
                        "started": true,
                        "restartCount": 0,
                        "state": {"running": {"startedAt": "2026-05-17T16:17:39Z"}}
                    }]
                }
            }),
        )
        .await
        .unwrap();

        apply_forwarded_command(
            &db,
            StorageCommand::UpdateStatus {
                api_version: "v1".into(),
                kind: "Pod".into(),
                namespace: Some("default".into()),
                name: "reconnected-pod".into(),
                status: json!({
                    "phase": "Running",
                    "podIP": "10.50.1.9",
                    "podIPs": [{"ip": "10.50.1.9"}],
                    "hostIP": "10.99.0.11",
                    "hostIPs": [{"ip": "10.99.0.11"}],
                    "conditions": [
                        {"type": "PodScheduled", "status": "True", "lastTransitionTime": "2026-05-17T16:17:39Z"},
                        {"type": "Initialized", "status": "True", "lastTransitionTime": "2026-05-17T16:17:39Z"},
                        {"type": "ContainersReady", "status": "True", "lastTransitionTime": "2026-05-17T16:17:50Z"},
                        {"type": "Ready", "status": "True", "lastTransitionTime": "2026-05-17T16:17:50Z"}
                    ],
                    "containerStatuses": [{
                        "name": "c",
                        "ready": true,
                        "started": true,
                        "restartCount": 0,
                        "state": {"running": {"startedAt": "2026-05-17T16:17:39Z"}}
                    }]
                }),
                expected_rv: None,
                preconditions: ResourcePreconditions {
                    uid: Some("uid-reconnected-pod".into()),
                    resource_version: None,
                },
            },
            "worker-1".into(),
        )
        .await
        .unwrap();

        let stored = db
            .get_resource("v1", "Pod", Some("default"), "reconnected-pod")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.data["status"]["phase"], json!("Running"));
        assert_eq!(
            stored.data["status"]["containerStatuses"][0]["ready"],
            json!(true)
        );
        let conditions = stored.data["status"]["conditions"].as_array().unwrap();
        assert_eq!(
            conditions.iter().find(|c| c["type"] == "Ready").unwrap()["status"],
            json!("True")
        );
        assert_eq!(
            conditions
                .iter()
                .find(|c| c["type"] == "ContainersReady")
                .unwrap()["status"],
            json!("True")
        );
    }

    #[tokio::test]
    async fn forwarded_fresh_readiness_true_wins_over_live_false() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "fresh-true-pod",
            ready_pod_status("fresh-true-pod", false, "False", "2026-05-17T16:17:40Z"),
        )
        .await
        .unwrap();

        apply_forwarded_command(
            &db,
            StorageCommand::UpdateStatus {
                api_version: "v1".into(),
                kind: "Pod".into(),
                namespace: Some("default".into()),
                name: "fresh-true-pod".into(),
                status: json!({
                    "phase": "Running",
                    "podIP": "10.50.1.9",
                    "podIPs": [{"ip": "10.50.1.9"}],
                    "hostIP": "10.99.0.11",
                    "hostIPs": [{"ip": "10.99.0.11"}],
                    "conditions": [
                        {"type": "PodScheduled", "status": "True", "lastTransitionTime": "2026-05-17T16:17:39Z"},
                        {"type": "Initialized", "status": "True", "lastTransitionTime": "2026-05-17T16:17:39Z"},
                        {"type": "ContainersReady", "status": "True", "lastTransitionTime": "2026-05-17T16:17:50Z"},
                        {"type": "Ready", "status": "True", "lastTransitionTime": "2026-05-17T16:17:50Z"}
                    ],
                    "containerStatuses": [{
                        "name": "c",
                        "ready": true,
                        "started": true,
                        "restartCount": 0,
                        "state": {"running": {"startedAt": "2026-05-17T16:17:39Z"}}
                    }]
                }),
                expected_rv: None,
                preconditions: ResourcePreconditions {
                    uid: Some("uid-fresh-true-pod".into()),
                    resource_version: None,
                },
            },
            "worker-1".into(),
        )
        .await
        .unwrap();

        let stored = db
            .get_resource("v1", "Pod", Some("default"), "fresh-true-pod")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            stored.data["status"]["containerStatuses"][0]["ready"],
            json!(true)
        );
        let conditions = stored.data["status"]["conditions"].as_array().unwrap();
        assert_eq!(
            conditions.iter().find(|c| c["type"] == "Ready").unwrap()["status"],
            json!("True")
        );
    }

    #[tokio::test]
    async fn forwarded_fresh_readiness_false_wins_over_live_true() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "fresh-false-pod",
            ready_pod_status("fresh-false-pod", true, "True", "2026-05-17T16:17:40Z"),
        )
        .await
        .unwrap();

        apply_forwarded_command(
            &db,
            StorageCommand::UpdateStatus {
                api_version: "v1".into(),
                kind: "Pod".into(),
                namespace: Some("default".into()),
                name: "fresh-false-pod".into(),
                status: json!({
                    "phase": "Running",
                    "podIP": "10.50.1.9",
                    "podIPs": [{"ip": "10.50.1.9"}],
                    "hostIP": "10.99.0.11",
                    "hostIPs": [{"ip": "10.99.0.11"}],
                    "conditions": [
                        {"type": "PodScheduled", "status": "True", "lastTransitionTime": "2026-05-17T16:17:39Z"},
                        {"type": "Initialized", "status": "True", "lastTransitionTime": "2026-05-17T16:17:39Z"},
                        {"type": "ContainersReady", "status": "False", "lastTransitionTime": "2026-05-17T16:17:50Z"},
                        {"type": "Ready", "status": "False", "lastTransitionTime": "2026-05-17T16:17:50Z"}
                    ],
                    "containerStatuses": [{
                        "name": "c",
                        "ready": false,
                        "started": true,
                        "restartCount": 0,
                        "state": {"running": {"startedAt": "2026-05-17T16:17:39Z"}}
                    }]
                }),
                expected_rv: None,
                preconditions: ResourcePreconditions {
                    uid: Some("uid-fresh-false-pod".into()),
                    resource_version: None,
                },
            },
            "worker-1".into(),
        )
        .await
        .unwrap();

        let stored = db
            .get_resource("v1", "Pod", Some("default"), "fresh-false-pod")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            stored.data["status"]["containerStatuses"][0]["ready"],
            json!(false)
        );
        let conditions = stored.data["status"]["conditions"].as_array().unwrap();
        assert_eq!(
            conditions.iter().find(|c| c["type"] == "Ready").unwrap()["status"],
            json!("False")
        );
    }

    #[tokio::test]
    async fn forwarded_same_container_terminated_wins_even_when_running_started_at_is_later() {
        let db = crate::datastore::test_support::in_memory().await;
        let mut live = ready_pod_status(
            "same-container-terminal-pod",
            true,
            "True",
            "2026-05-17T16:18:10Z",
        );
        live["status"]["phase"] = json!("Running");
        live["status"]["containerStatuses"][0]["containerID"] = json!("containerd://same");
        live["status"]["containerStatuses"][0]["restartCount"] = json!(1);
        live["status"]["containerStatuses"][0]["state"] = json!({
            "running": {"startedAt": "2026-05-17T16:18:10Z"}
        });
        live["status"]["containerStatuses"][0]["lastState"] = json!({
            "terminated": {
                "exitCode": 1,
                "reason": "Error",
                "startedAt": "2026-05-17T16:17:39Z",
                "finishedAt": "2026-05-17T16:17:49Z"
            }
        });
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "same-container-terminal-pod",
            live,
        )
        .await
        .unwrap();

        apply_forwarded_command(
            &db,
            StorageCommand::UpdateStatus {
                api_version: "v1".into(),
                kind: "Pod".into(),
                namespace: Some("default".into()),
                name: "same-container-terminal-pod".into(),
                status: json!({
                    "phase": "Succeeded",
                    "podIP": "10.50.1.9",
                    "podIPs": [{"ip": "10.50.1.9"}],
                    "hostIP": "10.99.0.11",
                    "hostIPs": [{"ip": "10.99.0.11"}],
                    "conditions": [
                        {"type": "PodScheduled", "status": "True", "lastTransitionTime": "2026-05-17T16:17:39Z"},
                        {"type": "Initialized", "status": "True", "lastTransitionTime": "2026-05-17T16:17:39Z"},
                        {"type": "ContainersReady", "status": "False", "reason": "PodCompleted", "lastTransitionTime": "2026-05-17T16:18:05Z"},
                        {"type": "Ready", "status": "False", "reason": "PodCompleted", "lastTransitionTime": "2026-05-17T16:18:05Z"}
                    ],
                    "containerStatuses": [{
                        "name": "c",
                        "containerID": "containerd://same",
                        "ready": false,
                        "started": true,
                        "restartCount": 1,
                        "lastState": {
                            "terminated": {
                                "exitCode": 1,
                                "reason": "Error",
                                "startedAt": "2026-05-17T16:17:39Z",
                                "finishedAt": "2026-05-17T16:17:49Z"
                            }
                        },
                        "state": {"terminated": {
                            "exitCode": 0,
                            "reason": "Completed",
                            "startedAt": "2026-05-17T16:18:00Z",
                            "finishedAt": "2026-05-17T16:18:05Z"
                        }}
                    }]
                }),
                expected_rv: None,
                preconditions: ResourcePreconditions {
                    uid: Some("uid-same-container-terminal-pod".into()),
                    resource_version: None,
                },
            },
            "worker-1".into(),
        )
        .await
        .unwrap();

        let stored = db
            .get_resource("v1", "Pod", Some("default"), "same-container-terminal-pod")
            .await
            .unwrap()
            .unwrap();
        let cs0 = &stored.data["status"]["containerStatuses"][0];
        assert!(
            cs0.pointer("/state/terminated").is_some(),
            "same containerID terminal status must win over stale live running state; got {cs0}"
        );
        assert_eq!(stored.data["status"]["phase"], json!("Succeeded"));
    }

    #[tokio::test]
    async fn forwarded_complete_status_replaces_live_status_without_leader_side_merge() {
        let db = crate::datastore::test_support::in_memory().await;
        let mut live = ready_pod_status(
            "same-count-restart-pod",
            true,
            "True",
            "2026-05-17T16:18:10Z",
        );
        live["status"]["containerStatuses"][0]["containerID"] = json!("containerd://new-running");
        live["status"]["containerStatuses"][0]["restartCount"] = json!(2);
        live["status"]["containerStatuses"][0]["state"] = json!({
            "running": {"startedAt": "2026-05-17T16:18:10Z"}
        });
        live["status"]["containerStatuses"][0]["lastState"] = json!({
            "terminated": {
                "exitCode": 1,
                "reason": "Error",
                "startedAt": "2026-05-17T16:17:39Z",
                "finishedAt": "2026-05-17T16:17:49Z"
            }
        });
        db.create_resource("v1", "Pod", Some("default"), "same-count-restart-pod", live)
            .await
            .unwrap();

        apply_forwarded_command(
            &db,
            StorageCommand::UpdateStatus {
                api_version: "v1".into(),
                kind: "Pod".into(),
                namespace: Some("default".into()),
                name: "same-count-restart-pod".into(),
                status: json!({
                    "phase": "Running",
                    "podIP": "10.50.1.9",
                    "podIPs": [{"ip": "10.50.1.9"}],
                    "hostIP": "10.99.0.11",
                    "hostIPs": [{"ip": "10.99.0.11"}],
                    "conditions": [
                        {"type": "PodScheduled", "status": "True", "lastTransitionTime": "2026-05-17T16:17:39Z"},
                        {"type": "Initialized", "status": "True", "lastTransitionTime": "2026-05-17T16:17:39Z"},
                        {"type": "ContainersReady", "status": "False", "lastTransitionTime": "2026-05-17T16:17:49Z"},
                        {"type": "Ready", "status": "False", "lastTransitionTime": "2026-05-17T16:17:49Z"}
                    ],
                    "containerStatuses": [{
                        "name": "c",
                        "containerID": "containerd://old-terminated",
                        "ready": false,
                        "started": true,
                        "restartCount": 2,
                        "lastState": {
                            "terminated": {
                                "exitCode": 1,
                                "reason": "Error",
                                "startedAt": "2026-05-17T16:17:39Z",
                                "finishedAt": "2026-05-17T16:17:49Z"
                            }
                        },
                        "state": {"terminated": {
                            "exitCode": 0,
                            "reason": "Completed",
                            "startedAt": "2026-05-17T16:17:55Z",
                            "finishedAt": "2026-05-17T16:18:05Z"
                        }}
                    }]
                }),
                expected_rv: None,
                preconditions: ResourcePreconditions {
                    uid: Some("uid-same-count-restart-pod".into()),
                    resource_version: None,
                },
            },
            "worker-1".into(),
        )
        .await
        .unwrap();

        let stored = db
            .get_resource("v1", "Pod", Some("default"), "same-count-restart-pod")
            .await
            .unwrap()
            .unwrap();
        let cs0 = &stored.data["status"]["containerStatuses"][0];
        assert!(
            cs0.pointer("/state/terminated").is_some(),
            "complete forwarded status must be applied as-is instead of leader-merged with live status; got {cs0}"
        );
        assert_eq!(cs0["containerID"], json!("containerd://old-terminated"));
        assert_eq!(cs0["ready"], json!(false));
        let conditions = stored.data["status"]["conditions"].as_array().unwrap();
        assert_eq!(
            conditions.iter().find(|c| c["type"] == "Ready").unwrap()["status"],
            json!("False")
        );
    }

    #[tokio::test]
    async fn forwarded_terminated_state_wins_over_live_running_state() {
        // Forwarded Pod status entries are complete worker-authored status
        // objects. The leader apply path must not merge them with the live
        // Pod status, even when the live row currently reports Running.
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "term-pod",
            ready_pod_status("term-pod", true, "True", "2026-05-17T16:17:55Z"),
        )
        .await
        .unwrap();

        apply_forwarded_command(
            &db,
            StorageCommand::UpdateStatus {
                api_version: "v1".into(),
                kind: "Pod".into(),
                namespace: Some("default".into()),
                name: "term-pod".into(),
                status: json!({
                    "phase": "Failed",
                    "podIP": "10.50.1.9",
                    "podIPs": [{"ip": "10.50.1.9"}],
                    "hostIP": "10.99.0.11",
                    "hostIPs": [{"ip": "10.99.0.11"}],
                    "conditions": [
                        {"type": "PodScheduled", "status": "True", "lastTransitionTime": "2026-05-17T16:17:39Z"},
                        {"type": "Initialized", "status": "True", "lastTransitionTime": "2026-05-17T16:17:39Z"},
                        {"type": "ContainersReady", "status": "False", "reason": "PodFailed", "lastTransitionTime": "2026-05-17T16:17:45Z"},
                        {"type": "Ready", "status": "False", "reason": "PodFailed", "lastTransitionTime": "2026-05-17T16:17:45Z"}
                    ],
                    "containerStatuses": [{
                        "name": "c",
                        "ready": false,
                        "started": false,
                        "restartCount": 0,
                        "state": {"terminated": {
                            "exitCode": 1,
                            "reason": "Error",
                            "startedAt": "2026-05-17T16:17:39Z",
                            "finishedAt": "2026-05-17T16:17:44Z",
                            "containerID": "containerd://abc"
                        }}
                    }]
                }),
                expected_rv: None,
                preconditions: ResourcePreconditions {
                    uid: Some("uid-term-pod".into()),
                    resource_version: None,
                },
            },
            "worker-1".into(),
        )
        .await
        .unwrap();

        let stored = db
            .get_resource("v1", "Pod", Some("default"), "term-pod")
            .await
            .unwrap()
            .unwrap();
        let cs0 = &stored.data["status"]["containerStatuses"][0];
        assert!(
            cs0.pointer("/state/terminated").is_some(),
            "incoming state.terminated must replace live state.running; got {cs0}"
        );
        assert!(
            cs0.pointer("/state/running").is_none(),
            "live state.running must not survive when incoming reports terminated; got {cs0}"
        );
    }

    fn ready_pod_status(
        name: &str,
        container_ready: bool,
        condition_status: &str,
        transition_time: &str,
    ) -> serde_json::Value {
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": name,
                "namespace": "default",
                "uid": format!("uid-{name}")
            },
            "spec": {"containers": [{"name": "c", "image": "busybox"}]},
            "status": {
                "phase": "Running",
                "podIP": "10.50.1.9",
                "podIPs": [{"ip": "10.50.1.9"}],
                "hostIP": "10.99.0.11",
                "hostIPs": [{"ip": "10.99.0.11"}],
                "conditions": [
                    {"type": "PodScheduled", "status": "True", "lastTransitionTime": "2026-05-17T16:17:39Z"},
                    {"type": "Initialized", "status": "True", "lastTransitionTime": "2026-05-17T16:17:39Z"},
                    {"type": "ContainersReady", "status": condition_status, "lastTransitionTime": transition_time},
                    {"type": "Ready", "status": condition_status, "lastTransitionTime": transition_time}
                ],
                "containerStatuses": [{
                    "name": "c",
                    "ready": container_ready,
                    "started": true,
                    "restartCount": 0,
                    "state": {"running": {"startedAt": "2026-05-17T16:17:39Z"}}
                }]
            }
        })
    }

    #[tokio::test]
    async fn forwarded_update_with_uid_precondition_does_not_invent_zero_rv_cas() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "forwarded",
            json!({
                "metadata": {
                    "name": "forwarded",
                    "namespace": "default",
                    "uid": "uid-current"
                },
                "data": {"before": "true"}
            }),
        )
        .await
        .unwrap();

        apply_forwarded_command(
            &db,
            StorageCommand::UpdateResource {
                api_version: "v1".into(),
                kind: "ConfigMap".into(),
                namespace: Some("default".into()),
                name: "forwarded".into(),
                data: json!({
                    "metadata": {
                        "name": "forwarded",
                        "namespace": "default",
                        "uid": "uid-current"
                    },
                    "data": {"after": "true"}
                }),
                expected_rv: 0,
                preconditions: ResourcePreconditions {
                    uid: Some("uid-current".into()),
                    resource_version: None,
                },
            },
            "worker-1".into(),
        )
        .await
        .unwrap();

        let stored = db
            .get_resource("v1", "ConfigMap", Some("default"), "forwarded")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            stored.data.pointer("/data/after").and_then(|v| v.as_str()),
            Some("true")
        );
    }

    #[tokio::test]
    async fn forwarded_node_create_publishes_external_ip_from_dataplane_endpoint() {
        let db = crate::datastore::test_support::in_memory().await;
        db.update_node_dataplane(
            crate::networking::wireguard::DataplanePeerMetadata::try_new(
                "worker-1".to_string(),
                crate::networking::wireguard::DataplaneMode::Root,
                crate::networking::wireguard::DataplaneEncryption::Enabled,
                Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string()),
                Some("198.51.100.175".to_string()),
                Some(7679),
            )
            .unwrap(),
        )
        .await
        .unwrap();

        apply_forwarded_command(
            &db,
            StorageCommand::CreateResource {
                api_version: "v1".into(),
                kind: "Node".into(),
                namespace: None,
                name: "worker-1".into(),
                data: json!({
                    "apiVersion": "v1",
                    "kind": "Node",
                    "metadata": {"name": "worker-1"},
                    "status": {
                        "addresses": [
                            {"type": "Hostname", "address": "worker-1"},
                            {"type": "InternalIP", "address": "192.168.8.22"}
                        ]
                    }
                }),
            },
            "worker-1".into(),
        )
        .await
        .unwrap();

        let stored = db
            .get_resource("v1", "Node", None, "worker-1")
            .await
            .unwrap()
            .unwrap();
        let external_ip = stored.data["status"]["addresses"]
            .as_array()
            .unwrap()
            .iter()
            .find(|address| address["type"] == "ExternalIP")
            .and_then(|address| address["address"].as_str());
        assert_eq!(
            external_ip,
            Some("198.51.100.175"),
            "forwarded Node create must publish the leader-observed dataplane endpoint, not the InternalIP"
        );
    }

    #[tokio::test]
    async fn forwarded_node_status_refresh_ignores_stale_rv_and_updates_commit() {
        let db = crate::datastore::test_support::in_memory().await;
        let created = db
            .create_resource(
                "v1",
                "Node",
                None,
                "worker-1",
                json!({
                    "apiVersion": "v1",
                    "kind": "Node",
                    "metadata": {
                        "name": "worker-1",
                        "annotations": {
                            "klights.io/git-commit": "380f96e1"
                        }
                    },
                    "spec": {},
                    "status": {
                        "conditions": [{
                            "type": "Ready",
                            "status": "True",
                            "lastHeartbeatTime": "2026-05-22T12:00:00Z",
                            "lastTransitionTime": "2026-05-22T12:00:00Z"
                        }]
                    }
                }),
            )
            .await
            .unwrap();
        let mut leader_changed_node = (*created.data).clone();
        leader_changed_node["spec"] = json!({"unschedulable": true});
        db.update_resource_with_preconditions(
            "v1",
            "Node",
            None,
            "worker-1",
            leader_changed_node,
            ResourcePreconditions::from_resource(&created),
        )
        .await
        .unwrap();

        let mut stale_worker_node = (*created.data).clone();
        stale_worker_node["metadata"]["annotations"]["klights.io/git-commit"] = json!("ec502ffb");
        stale_worker_node["status"]["conditions"][0]["lastHeartbeatTime"] =
            json!("2026-05-22T12:00:10Z");

        apply_forwarded_command(
            &db,
            StorageCommand::UpdateResource {
                api_version: "v1".into(),
                kind: "Node".into(),
                namespace: None,
                name: "worker-1".into(),
                data: stale_worker_node,
                expected_rv: created.resource_version,
                preconditions: ResourcePreconditions::from_resource(&created),
            },
            "worker-1".into(),
        )
        .await
        .expect("NodeStatus refresh from worker must be merged with the current Node");

        let stored = db
            .get_resource("v1", "Node", None, "worker-1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            stored
                .data
                .pointer("/metadata/annotations/klights.io~1git-commit")
                .and_then(|v| v.as_str()),
            Some("ec502ffb")
        );
        assert_eq!(
            stored.data.pointer("/spec/unschedulable"),
            Some(&json!(true)),
            "stale worker NodeStatus must not roll back leader-owned spec"
        );
    }

    #[tokio::test]
    async fn forwarded_lease_renew_does_not_touch_cluster_db() {
        let db = crate::datastore::test_support::in_memory().await;
        let created = db
            .create_resource(
                "coordination.k8s.io/v1",
                "Lease",
                Some("kube-node-lease"),
                "worker-1",
                json!({
                    "apiVersion": "coordination.k8s.io/v1",
                    "kind": "Lease",
                    "metadata": {
                        "name": "worker-1",
                        "namespace": "kube-node-lease"
                    },
                    "spec": {
                        "holderIdentity": "worker-1",
                        "leaseDurationSeconds": 50,
                        "renewTime": "2026-05-22T12:00:00.000000Z"
                    }
                }),
            )
            .await
            .unwrap();
        let mut leader_changed_lease = (*created.data).clone();
        leader_changed_lease["spec"]["renewTime"] = json!("2026-05-22T12:00:05.000000Z");
        db.update_resource_with_preconditions(
            "coordination.k8s.io/v1",
            "Lease",
            Some("kube-node-lease"),
            "worker-1",
            leader_changed_lease,
            ResourcePreconditions::from_resource(&created),
        )
        .await
        .unwrap();
        let before_noop_rv = db.get_current_resource_version().await.unwrap();

        let mut stale_worker_lease = (*created.data).clone();
        stale_worker_lease["spec"]["renewTime"] = json!("2026-05-22T12:00:10.000000Z");

        let applied = apply_forwarded_command(
            &db,
            StorageCommand::UpdateResource {
                api_version: "coordination.k8s.io/v1".into(),
                kind: "Lease".into(),
                namespace: Some("kube-node-lease".into()),
                name: "worker-1".into(),
                data: stale_worker_lease,
                expected_rv: created.resource_version,
                preconditions: ResourcePreconditions::from_resource(&created),
            },
            "worker-1".into(),
        )
        .await
        .expect("legacy worker Lease renew should be accepted as a no-op");
        assert!(applied.already_applied);
        assert_eq!(
            db.get_current_resource_version().await.unwrap(),
            before_noop_rv
        );

        let mut meta_worker_lease = (*created.data).clone();
        meta_worker_lease["spec"]["renewTime"] = json!("2026-05-22T12:00:15.000000Z");
        let applied_with_meta = apply_forwarded_command_with_meta(
            &db,
            StorageCommand::UpdateResource {
                api_version: "coordination.k8s.io/v1".into(),
                kind: "Lease".into(),
                namespace: Some("kube-node-lease".into()),
                name: "worker-1".into(),
                data: meta_worker_lease,
                expected_rv: created.resource_version,
                preconditions: ResourcePreconditions::from_resource(&created),
            },
            CommandMeta {
                command_id: CommandId("lease-renew-meta-key".to_string()),
                codec_version: COMMAND_CODEC_VERSION,
                resource_version: 9_999,
                uid: None,
                timestamp_ms: 0,
                authoring_node: "worker-1".into(),
            },
        )
        .await
        .expect("legacy worker Lease renew with metadata should be accepted as a no-op");
        assert!(applied_with_meta.already_applied);
        assert_eq!(
            db.get_current_resource_version().await.unwrap(),
            before_noop_rv
        );
        assert!(
            db.get_applied_outbox("lease-renew-meta-key")
                .await
                .unwrap()
                .is_none(),
            "legacy LeaseRenew forwarded apply must not write applied_outbox"
        );

        let stored = db
            .get_resource(
                "coordination.k8s.io/v1",
                "Lease",
                Some("kube-node-lease"),
                "worker-1",
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            stored
                .data
                .pointer("/spec/renewTime")
                .and_then(|v| v.as_str()),
            Some("2026-05-22T12:00:05.000000Z")
        );
    }

    #[tokio::test]
    async fn forwarded_node_dataplane_update_publishes_node_routing_metadata() {
        let db = crate::datastore::test_support::in_memory().await;
        db.allocate_node_subnet("worker-1", "10.50.0.0/16", "192.0.2.10")
            .await
            .unwrap();
        let created = db
            .create_resource(
                "v1",
                "Node",
                None,
                "worker-1",
                json!({
                    "apiVersion": "v1",
                    "kind": "Node",
                    "metadata": {"name": "worker-1"},
                    "spec": {},
                    "status": {
                        "addresses": [
                            {"type": "Hostname", "address": "worker-1"},
                            {"type": "InternalIP", "address": "192.0.2.10"},
                            {"type": "ExternalIP", "address": "192.0.2.10"}
                        ]
                    }
                }),
            )
            .await
            .unwrap();

        apply_forwarded_command(
            &db,
            StorageCommand::UpdateNodeDataplane {
                node_name: "worker-1".into(),
                mode: "root".into(),
                encryption: "enabled".into(),
                public_key: Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".into()),
                endpoint: "192.0.2.10".into(),
                port: Some(7679),
            },
            "worker-1".into(),
        )
        .await
        .unwrap();

        let stored = db
            .get_resource("v1", "Node", None, "worker-1")
            .await
            .unwrap()
            .unwrap();
        assert!(
            stored.resource_version > created.resource_version,
            "dataplane metadata must emit a Node MODIFIED event even when ExternalIP is unchanged"
        );
        assert_eq!(
            stored
                .data
                .pointer("/metadata/annotations/klights.io~1dataplane-endpoint")
                .and_then(|v| v.as_str()),
            Some("192.0.2.10")
        );
        assert_eq!(
            stored
                .data
                .pointer("/spec/podCIDR")
                .and_then(|v| v.as_str()),
            Some("10.50.0.0/24")
        );
    }

    #[tokio::test]
    async fn forwarded_command_meta_is_leader_idempotency_key() {
        let db = crate::datastore::test_support::in_memory().await;
        let command = StorageCommand::CreateResource {
            api_version: "v1".into(),
            kind: "ConfigMap".into(),
            namespace: Some("default".into()),
            name: "idempotent-create".into(),
            data: json!({
                "metadata": {
                    "name": "idempotent-create",
                    "namespace": "default"
                },
                "data": {"value": "once"}
            }),
        };
        let meta = CommandMeta {
            command_id: CommandId("outbox-key-1".to_string()),
            codec_version: COMMAND_CODEC_VERSION,
            resource_version: 0,
            uid: None,
            timestamp_ms: 1_234,
            authoring_node: "worker-1".to_string(),
        };

        let first = apply_forwarded_command_with_meta(&db, command.clone(), meta.clone())
            .await
            .expect("first apply");
        let second = apply_forwarded_command_with_meta(&db, command, meta)
            .await
            .expect("duplicate apply");

        assert!(!first.already_applied);
        assert!(second.already_applied);
        let stored = db
            .get_applied_outbox("outbox-key-1")
            .await
            .expect("get applied")
            .expect("applied record");
        assert_eq!(
            stored.applied_rv,
            first
                .entry
                .as_ref()
                .map(|entry| entry.meta.resource_version)
        );
        let list = db
            .list_resources(
                "v1",
                "ConfigMap",
                Some("default"),
                crate::datastore::ResourceListQuery::all(),
            )
            .await
            .expect("list configmaps");
        assert_eq!(list.items.len(), 1);
    }
}
