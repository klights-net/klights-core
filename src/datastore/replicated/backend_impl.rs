//! `DatastoreBackend` impl for `ReplicatedDatastore` — extracted from replicated.rs.

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use std::net::Ipv4Addr;
use tokio::sync::broadcast;

use crate::datastore::backend::DatastoreBackend;
use crate::datastore::command::{CommandMeta, StorageCommand};
use crate::datastore::types::*;
use crate::networking::VtepMac;
use crate::watch::{WatchEvent, WatchReceiver, WatchTopic};

use super::{ReplicatedDatastore, apply_command_to_backend};

#[async_trait]
impl DatastoreBackend for ReplicatedDatastore {
    fn attach_raft_proposer(&self, proposer: std::sync::Arc<dyn super::RaftProposer>) {
        self.set_raft_proposer(proposer);
    }

    fn subscribe_watch(&self, topic: WatchTopic) -> broadcast::Receiver<WatchEvent> {
        if true {
            self.inner.subscribe_watch(topic)
        } else {
            let (_tx, rx) = broadcast::channel(1);
            rx
        }
    }
    fn subscribe_watch_many(&self, topics: Vec<WatchTopic>) -> WatchReceiver {
        if true {
            self.inner.subscribe_watch_many(topics)
        } else {
            WatchReceiver::from_receiver(broadcast::channel(1).1)
        }
    }
    fn broadcast_watch_event(&self, pending: PendingWatchEvent) {
        if true {
            self.inner.broadcast_watch_event(pending)
        }
    }

    async fn replace_replicated_resource_state(
        &self,
        entries: Vec<crate::log_apply::LogApplyCommit>,
        current_rv: i64,
        metadata: Option<ReplicatedSnapshotMetadata>,
    ) -> Result<()> {
        self.inner
            .replace_replicated_resource_state(entries, current_rv, metadata)
            .await
    }

    async fn apply_log_apply_commit(&self, commit: crate::log_apply::LogApplyCommit) -> Result<()> {
        self.inner.apply_log_apply_commit(commit).await
    }

    async fn apply_raft_log_apply_commit(
        &self,
        commit: crate::log_apply::LogApplyCommit,
    ) -> Result<crate::datastore::raft::types::StorageCommandResult> {
        self.inner.apply_raft_log_apply_commit(commit).await
    }

    async fn create_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        mut data: Value,
    ) -> Result<Resource> {
        let proposer = self.require_raft_proposer()?;
        if api_version == "v1"
            && kind == "Pod"
            && crate::datastore::pod_serviceaccount::should_inject_serviceaccount_volume(
                self.inner.as_ref(),
                &data,
                namespace,
            )
            .await
        {
            crate::datastore::pod_serviceaccount::inject_serviceaccount_volume(&mut data);
        }
        let command = StorageCommand::CreateResource {
            api_version: api_version.to_string(),
            kind: kind.to_string(),
            namespace: namespace.map(str::to_string),
            name: name.to_string(),
            data,
        };
        self.propose_command_via_raft(&proposer, command).await?;
        self
            .inner
            .get_resource(api_version, kind, namespace, name)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "raft-routed create_resource: row missing after commit for {api_version}/{kind}/{name}"
                )
            })
    }
    async fn get_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<Option<Resource>> {
        self.inner
            .get_resource(api_version, kind, namespace, name)
            .await
    }
    async fn list_resources(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        query: ResourceListQuery<'_>,
    ) -> Result<ResourceList> {
        self.inner
            .list_resources(api_version, kind, namespace, query)
            .await
    }
    async fn list_resources_page(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        page: ListPageRequest,
    ) -> Result<ResourceList> {
        self.inner
            .list_resources_page(
                api_version,
                kind,
                namespace,
                label_selector,
                field_selector,
                page,
            )
            .await
    }
    async fn list_resource_keys_for_scope(
        &self,
        api_version: String,
        kind: String,
        namespaced: bool,
    ) -> Result<Vec<(Option<String>, String)>> {
        self.inner
            .list_resource_keys_for_scope(api_version, kind, namespaced)
            .await
    }
    async fn update_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        expected_rv: i64,
    ) -> Result<Resource> {
        let preconditions = ResourcePreconditions {
            uid: None,
            resource_version: Some(expected_rv),
        };
        let proposer = self.require_raft_proposer()?;
        let command = StorageCommand::UpdateResource {
            api_version: api_version.to_string(),
            kind: kind.to_string(),
            namespace: namespace.map(str::to_string),
            name: name.to_string(),
            data,
            expected_rv,
            preconditions,
        };
        self.propose_command_via_raft(&proposer, command).await?;
        self
            .inner
            .get_resource(api_version, kind, namespace, name)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "raft-routed update_resource: row missing after commit for {api_version}/{kind}/{name}"
                )
            })
    }
    async fn update_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource> {
        let expected_rv = preconditions.resource_version.unwrap_or(0);
        let proposer = self.require_raft_proposer()?;
        let command = StorageCommand::UpdateResource {
            api_version: api_version.to_string(),
            kind: kind.to_string(),
            namespace: namespace.map(str::to_string),
            name: name.to_string(),
            data,
            expected_rv,
            preconditions,
        };
        self.propose_command_via_raft(&proposer, command).await?;
        self
            .inner
            .get_resource(api_version, kind, namespace, name)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "raft-routed update_resource_with_preconditions: row missing after commit for {api_version}/{kind}/{name}"
                )
            })
    }
    async fn update_main_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource> {
        let expected_rv = preconditions.resource_version.unwrap_or(0);
        let proposer = self.require_raft_proposer()?;
        let command = StorageCommand::UpdateResource {
            api_version: api_version.to_string(),
            kind: kind.to_string(),
            namespace: namespace.map(str::to_string),
            name: name.to_string(),
            data,
            expected_rv,
            preconditions,
        };
        self.propose_command_via_raft(&proposer, command).await?;
        self
            .inner
            .get_resource(api_version, kind, namespace, name)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "raft-routed update_main_resource_with_preconditions: row missing after commit for {api_version}/{kind}/{name}"
                )
            })
    }
    async fn update_status_only(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        status: Value,
        expected_rv: Option<i64>,
    ) -> Result<Resource> {
        let proposer = self.require_raft_proposer()?;
        let command = StorageCommand::UpdateStatus {
            api_version: api_version.to_string(),
            kind: kind.to_string(),
            namespace: namespace.map(str::to_string),
            name: name.to_string(),
            status,
            expected_rv,
            preconditions: ResourcePreconditions {
                uid: None,
                resource_version: expected_rv,
            },
            observed_status_stamp: None,
        };
        self.propose_command_via_raft(&proposer, command).await?;
        self
            .inner
            .get_resource(api_version, kind, namespace, name)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "raft-routed update_status_only: row missing after commit for {api_version}/{kind}/{name}"
                )
            })
    }
    async fn update_status_only_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        status: Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource> {
        let proposer = self.require_raft_proposer()?;
        let expected_rv = preconditions.resource_version;
        let command = StorageCommand::UpdateStatus {
            api_version: api_version.to_string(),
            kind: kind.to_string(),
            namespace: namespace.map(str::to_string),
            name: name.to_string(),
            status,
            expected_rv,
            preconditions,
            observed_status_stamp: None,
        };
        self.propose_command_via_raft(&proposer, command).await?;
        self
            .inner
            .get_resource(api_version, kind, namespace, name)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "raft-routed update_status_only_with_preconditions: row missing after commit for {api_version}/{kind}/{name}"
                )
            })
    }
    async fn delete_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<()> {
        self.delete_resource_with_preconditions(
            api_version,
            kind,
            namespace,
            name,
            ResourcePreconditions::default(),
        )
        .await
    }

    async fn delete_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
    ) -> Result<()> {
        self.delete_resource_with_preconditions_observed_rv(
            api_version,
            kind,
            namespace,
            name,
            preconditions,
        )
        .await
        .map(|_| ())
    }

    async fn delete_resource_with_preconditions_observed_rv(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
    ) -> Result<i64> {
        let proposer = self.require_raft_proposer()?;
        let command = StorageCommand::DeleteResource {
            api_version: api_version.to_string(),
            kind: kind.to_string(),
            namespace: namespace.map(str::to_string),
            name: name.to_string(),
            preconditions,
        };
        self.propose_command_via_raft(&proposer, command).await?;
        Ok(self.inner.get_current_resource_version().await.unwrap_or(0))
    }
    async fn get_current_resource_version(&self) -> Result<i64> {
        self.inner.get_current_resource_version().await
    }
    async fn create_namespace(&self, name: &str, data: Value) -> Result<Resource> {
        let proposer = self.require_raft_proposer()?;
        let command = StorageCommand::CreateNamespace {
            name: name.to_string(),
            data: data.clone(),
        };
        self.propose_command_via_raft(&proposer, command).await?;
        self.inner.get_namespace(name).await?.ok_or_else(|| {
            anyhow::anyhow!("raft-routed create_namespace: row missing after commit for {name}")
        })
    }
    async fn get_namespace(&self, name: &str) -> Result<Option<Resource>> {
        self.inner.get_namespace(name).await
    }
    async fn list_namespaces(
        &self,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
    ) -> Result<ResourceList> {
        self.inner
            .list_namespaces(label_selector, field_selector)
            .await
    }
    async fn list_namespaces_page(
        &self,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        page: ListPageRequest,
    ) -> Result<ResourceList> {
        self.inner
            .list_namespaces_page(label_selector, field_selector, page)
            .await
    }
    async fn update_namespace(
        &self,
        name: &str,
        data: Value,
        expected_rv: i64,
    ) -> Result<Resource> {
        let proposer = self.require_raft_proposer()?;
        let command = StorageCommand::UpdateNamespace {
            name: name.to_string(),
            data: data.clone(),
            expected_rv,
        };
        self.propose_command_via_raft(&proposer, command).await?;
        self.inner.get_namespace(name).await?.ok_or_else(|| {
            anyhow::anyhow!("raft-routed update_namespace: row missing after commit for {name}")
        })
    }
    async fn delete_namespace_contents(&self, name: &str) -> Result<()> {
        let proposer = self.require_raft_proposer()?;
        let command = StorageCommand::DeleteNamespaceContents {
            name: name.to_string(),
        };
        self.propose_command_via_raft(&proposer, command).await?;
        Ok(())
    }
    async fn delete_namespace(&self, name: &str) -> Result<()> {
        self.delete_namespace_observed_rv(name).await.map(|_| ())
    }

    async fn delete_namespace_observed_rv(&self, name: &str) -> Result<i64> {
        let proposer = self.require_raft_proposer()?;
        let command = StorageCommand::DeleteNamespace {
            name: name.to_string(),
        };
        self.propose_command_via_raft(&proposer, command).await?;
        self.inner.get_current_resource_version().await
    }
    async fn pod_workqueue_enqueue(
        &self,
        kind: PodWorkqueueKind,
        pod: &crate::pod_identity::PodIdentity,
        payload: Value,
        attempt_count: i64,
        min_delay_ms: i64,
        last_error: Option<&str>,
    ) -> Result<()> {
        self.inner
            .pod_workqueue_enqueue(kind, pod, payload, attempt_count, min_delay_ms, last_error)
            .await
    }
    async fn pod_workqueue_peek_next_due(&self) -> Result<Option<i64>> {
        self.inner.pod_workqueue_peek_next_due().await
    }
    async fn pod_workqueue_claim_due(&self, now_ms: i64) -> Result<Option<PodWorkqueueEntry>> {
        self.inner.pod_workqueue_claim_due(now_ms).await
    }
    async fn pod_workqueue_complete(&self, id: i64) -> Result<()> {
        self.inner.pod_workqueue_complete(id).await
    }
    async fn pod_workqueue_record_failure(
        &self,
        row: PodWorkqueueEntry,
        min_delay_ms: i64,
        error: &str,
    ) -> Result<()> {
        self.inner
            .pod_workqueue_record_failure(row, min_delay_ms, error)
            .await
    }
    async fn pod_workqueue_dead_letter(&self, id: i64, error: &str) -> Result<()> {
        self.inner.pod_workqueue_dead_letter(id, error).await
    }
    async fn record_sandbox(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        sandbox_id: &str,
    ) -> Result<()> {
        self.inner
            .record_sandbox(namespace, pod_name, pod_uid, sandbox_id)
            .await
    }
    async fn get_sandbox(&self, namespace: &str, pod_name: &str) -> Result<Option<String>> {
        self.inner.get_sandbox(namespace, pod_name).await
    }
    async fn get_sandbox_for_uid(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
    ) -> Result<Option<String>> {
        self.inner
            .get_sandbox_for_uid(namespace, pod_name, pod_uid)
            .await
    }
    async fn delete_sandbox(&self, namespace: &str, pod_name: &str) -> Result<()> {
        self.inner.delete_sandbox(namespace, pod_name).await
    }
    async fn delete_sandbox_for_uid(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        sandbox_id: &str,
    ) -> Result<()> {
        self.inner
            .delete_sandbox_for_uid(namespace, pod_name, pod_uid, sandbox_id)
            .await
    }
    async fn delete_pod_network(&self, sandbox_id: &str) -> Result<()> {
        self.inner.delete_pod_network(sandbox_id).await
    }
    async fn find_owned_resources(
        &self,
        owner_uid: &str,
        namespace: Option<&str>,
    ) -> Result<Vec<Resource>> {
        self.inner.find_owned_resources(owner_uid, namespace).await
    }
    async fn list_resources_by_owner_uid(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        owner_uid: &str,
    ) -> Result<Vec<Resource>> {
        self.inner
            .list_resources_by_owner_uid(api_version, kind, namespace, owner_uid)
            .await
    }
    async fn find_owned_by_name_kind_empty_uid(
        &self,
        owner_api_version: &str,
        owner_name: &str,
        owner_kind: &str,
        namespace: Option<&str>,
    ) -> Result<Vec<Resource>> {
        self.inner
            .find_owned_by_name_kind_empty_uid(owner_api_version, owner_name, owner_kind, namespace)
            .await
    }
    async fn list_cluster_resources_modified_since(
        &self,
        api_version: &str,
        kind: &str,
        since_rv: i64,
    ) -> Result<Vec<CatchUpResource>> {
        self.inner
            .list_cluster_resources_modified_since(api_version, kind, since_rv)
            .await
    }
    async fn list_cluster_resources(&self) -> Result<Vec<Resource>> {
        self.inner.list_cluster_resources().await
    }
    async fn list_resources_modified_since(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        since_rv: i64,
    ) -> Result<Vec<CatchUpResource>> {
        self.inner
            .list_resources_modified_since(api_version, kind, namespace, since_rv)
            .await
    }
    async fn advance_resource_version_after(&self, min_rv: i64) -> Result<i64> {
        let before_rv = self.inner.get_current_resource_version().await.unwrap_or(0);
        let new_rv = self.inner.advance_resource_version_after(min_rv).await?;
        if new_rv > before_rv {
            self.notify_if_configured(
                StorageCommand::AdvanceResourceVersion { min_rv, new_rv },
                self.meta_for_rv(new_rv, None),
            )
            .await;
        }
        Ok(new_rv)
    }
    async fn list_namespace_resources(&self, namespace: &str) -> Result<Vec<Resource>> {
        self.inner.list_namespace_resources(namespace).await
    }
    async fn list_namespace_resources_of_kind(
        &self,
        namespace: &str,
        kind: &str,
    ) -> Result<Vec<Resource>> {
        self.inner
            .list_namespace_resources_of_kind(namespace, kind)
            .await
    }
    async fn list_namespace_resources_excluding_kind(
        &self,
        namespace: &str,
        kind: &str,
    ) -> Result<Vec<Resource>> {
        self.inner
            .list_namespace_resources_excluding_kind(namespace, kind)
            .await
    }
    async fn count_namespace_resources(&self, namespace: &str) -> Result<i64> {
        self.inner.count_namespace_resources(namespace).await
    }
    async fn list_watch_events_since(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
    ) -> Result<Vec<CatchUpResource>> {
        self.inner.list_watch_events_since(targets, since_rv).await
    }

    async fn earliest_watch_event_rv(&self) -> Result<Option<i64>> {
        self.inner.earliest_watch_event_rv().await
    }

    async fn list_all_watch_events_since(&self, since_rv: i64) -> Result<Vec<CatchUpResource>> {
        self.inner.list_all_watch_events_since(since_rv).await
    }

    async fn list_deleted_watch_events_since(&self, since_rv: i64) -> Result<Vec<CatchUpResource>> {
        self.inner.list_deleted_watch_events_since(since_rv).await
    }

    async fn allocate_node_subnet(
        &self,
        node_name: &str,
        cluster_cidr: &str,
        node_ip: &str,
    ) -> Result<NodeSubnet> {
        let proposer = self.require_raft_proposer()?;
        self.propose_command_via_raft(
            &proposer,
            StorageCommand::AllocateNodeSubnet {
                node_name: node_name.to_string(),
                subnet: cluster_cidr.to_string(),
                node_ip: node_ip.to_string(),
            },
        )
        .await?;
        self.inner.get_node_subnet(node_name).await?.ok_or_else(|| {
            anyhow::anyhow!(
                "raft-routed allocate_node_subnet: row missing after commit for {node_name}"
            )
        })
    }
    async fn update_node_vtep_mac(&self, node_name: &str, vtep_mac: &VtepMac) -> Result<()> {
        let proposer = self.require_raft_proposer()?;
        self.propose_command_via_raft(
            &proposer,
            StorageCommand::UpdateNodeVtepMac {
                node_name: node_name.to_string(),
                vtep_mac: vtep_mac.to_string(),
            },
        )
        .await?;
        Ok(())
    }
    async fn update_node_peer_attributes(
        &self,
        node_name: &str,
        mode: crate::controllers::annotations::NodePeerMode,
        hostport_range: Option<crate::networking::types::HostPortRange>,
    ) -> Result<()> {
        let mode_value = match mode {
            crate::controllers::annotations::NodePeerMode::Root => "root",
            crate::controllers::annotations::NodePeerMode::Rootless => "rootless",
        }
        .to_string();
        let proposer = self.require_raft_proposer()?;
        self.propose_command_via_raft(
            &proposer,
            StorageCommand::UpdateNodePeerAttributes {
                node_name: node_name.to_string(),
                mode: mode_value,
                hostport_range: hostport_range.map(|range| range.to_string()),
            },
        )
        .await?;
        Ok(())
    }
    async fn update_node_dataplane(
        &self,
        metadata: crate::networking::wireguard::DataplanePeerMetadata,
    ) -> Result<()> {
        let command = StorageCommand::UpdateNodeDataplane {
            node_name: metadata.node_name.clone(),
            mode: metadata.mode.as_str().to_string(),
            encryption: metadata.encryption.as_str().to_string(),
            public_key: metadata.public_key.as_ref().map(ToString::to_string),
            endpoint: metadata.endpoint.to_string(),
            port: metadata.port,
        };
        let proposer = self.require_raft_proposer()?;
        self.propose_command_via_raft(&proposer, command).await?;
        Ok(())
    }

    async fn get_node_dataplane(
        &self,
        node_name: &str,
    ) -> Result<Option<crate::networking::wireguard::DataplanePeerMetadata>> {
        self.inner.get_node_dataplane(node_name).await
    }

    async fn get_node_subnet(&self, node_name: &str) -> Result<Option<NodeSubnet>> {
        self.inner.get_node_subnet(node_name).await
    }
    async fn list_peer_subnets(&self, my_node_name: &str) -> Result<Vec<NodeSubnet>> {
        self.inner.list_peer_subnets(my_node_name).await
    }
    async fn delete_node_subnet(&self, node_name: &str) -> Result<()> {
        let proposer = self.require_raft_proposer()?;
        self.propose_command_via_raft(
            &proposer,
            StorageCommand::DeleteNodeSubnet {
                node_name: node_name.to_string(),
            },
        )
        .await?;
        Ok(())
    }

    async fn move_pod_to_cleanup_intent(
        &self,
        node_name: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        reason: &str,
    ) -> Result<()> {
        let proposer = self.require_raft_proposer()?;
        self.propose_command_via_raft(
            &proposer,
            StorageCommand::MovePodToCleanupIntent {
                node_name: node_name.to_string(),
                namespace: namespace.to_string(),
                pod_name: pod_name.to_string(),
                pod_uid: pod_uid.to_string(),
                reason: reason.to_string(),
            },
        )
        .await?;
        Ok(())
    }

    async fn list_pod_cleanup_intents_for_node(
        &self,
        node_name: &str,
    ) -> Result<Vec<PodCleanupIntent>> {
        self.inner
            .list_pod_cleanup_intents_for_node(node_name)
            .await
    }

    async fn delete_pod_cleanup_intent(
        &self,
        node_name: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        reason: &str,
    ) -> Result<()> {
        let proposer = self.require_raft_proposer()?;
        self.propose_command_via_raft(
            &proposer,
            StorageCommand::DeletePodCleanupIntent {
                node_name: node_name.to_string(),
                namespace: namespace.to_string(),
                pod_name: pod_name.to_string(),
                pod_uid: pod_uid.to_string(),
                reason: reason.to_string(),
            },
        )
        .await?;
        Ok(())
    }

    async fn delete_pod_cleanup_intents_for_node(&self, node_name: &str) -> Result<()> {
        let proposer = self.require_raft_proposer()?;
        self.propose_command_via_raft(
            &proposer,
            StorageCommand::DeletePodCleanupIntentsForNode {
                node_name: node_name.to_string(),
            },
        )
        .await?;
        Ok(())
    }

    async fn pod_slot_try_admit(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        node_name: &str,
    ) -> Result<PodSlotAdmissionResult> {
        let command = StorageCommand::PodSlotTryAdmit {
            namespace: namespace.to_string(),
            pod_name: pod_name.to_string(),
            pod_uid: pod_uid.to_string(),
            node_name: node_name.to_string(),
        };
        let result = self
            .inner
            .pod_slot_try_admit(namespace, pod_name, pod_uid, node_name)
            .await?;
        let rv = match result {
            PodSlotAdmissionResult::Admitted { resource_version }
            | PodSlotAdmissionResult::Blocked {
                resource_version, ..
            } => resource_version,
        };
        if matches!(result, PodSlotAdmissionResult::Admitted { .. }) {
            self.notify_if_configured(command, self.meta_for_rv(rv, Some(pod_uid.to_string())))
                .await;
        }
        Ok(result)
    }

    async fn pod_slot_mark_terminating(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        node_name: &str,
    ) -> Result<()> {
        let command = StorageCommand::PodSlotMarkTerminating {
            namespace: namespace.to_string(),
            pod_name: pod_name.to_string(),
            pod_uid: pod_uid.to_string(),
            node_name: node_name.to_string(),
        };
        let before_rv = self.inner.get_current_resource_version().await.unwrap_or(0);
        self.inner
            .pod_slot_mark_terminating(namespace, pod_name, pod_uid, node_name)
            .await?;
        let rv = resource_version_after_non_resource_write(self.inner.as_ref(), before_rv).await?;
        self.notify_if_configured(command, self.meta_for_rv(rv, Some(pod_uid.to_string())))
            .await;
        Ok(())
    }

    async fn pod_slot_clear_if_uid(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        node_name: &str,
    ) -> Result<()> {
        let command = StorageCommand::PodSlotClearIfUid {
            namespace: namespace.to_string(),
            pod_name: pod_name.to_string(),
            pod_uid: pod_uid.to_string(),
            node_name: node_name.to_string(),
        };
        let before_rv = self.inner.get_current_resource_version().await.unwrap_or(0);
        self.inner
            .pod_slot_clear_if_uid(namespace, pod_name, pod_uid, node_name)
            .await?;
        let rv = resource_version_after_non_resource_write(self.inner.as_ref(), before_rv).await?;
        self.notify_if_configured(command, self.meta_for_rv(rv, Some(pod_uid.to_string())))
            .await;
        Ok(())
    }

    fn subscribe_pod_slot_admissions(&self) -> broadcast::Receiver<PodSlotAdmissionEvent> {
        if true {
            self.inner.subscribe_pod_slot_admissions()
        } else {
            let (_tx, rx) = broadcast::channel(1);
            rx
        }
    }
    async fn patch_resource_latest(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        patch_kind: PatchKind,
        patch: Value,
    ) -> Result<Option<Resource>> {
        let proposer = self.require_raft_proposer()?;
        let command = StorageCommand::PatchResource {
            api_version: api_version.to_string(),
            kind: kind.to_string(),
            namespace: namespace.map(str::to_string),
            name: name.to_string(),
            patch_kind,
            patch: patch.clone(),
            preconditions: ResourcePreconditions::default(),
        };
        self.propose_command_via_raft(&proposer, command).await?;
        self.inner
            .get_resource(api_version, kind, namespace, name)
            .await
    }
    async fn patch_resource_latest_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        request: ResourcePatchRequest,
    ) -> Result<Option<Resource>> {
        let ResourcePatchRequest {
            patch_kind,
            patch,
            preconditions,
        } = request;
        let proposer = self.require_raft_proposer()?;
        let command = StorageCommand::PatchResource {
            api_version: api_version.to_string(),
            kind: kind.to_string(),
            namespace: namespace.map(str::to_string),
            name: name.to_string(),
            patch_kind,
            patch: patch.clone(),
            preconditions: preconditions.clone(),
        };
        self.propose_command_via_raft(&proposer, command).await?;
        self.inner
            .get_resource(api_version, kind, namespace, name)
            .await
    }
    async fn get_pod_network(&self, sandbox_id: &str) -> Result<Option<PodNetworkEndpoint>> {
        self.inner.get_pod_network(sandbox_id).await
    }
    async fn get_pod_network_for_pod(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
    ) -> Result<Option<PodNetworkEndpoint>> {
        self.inner
            .get_pod_network_for_pod(namespace, pod_name, pod_uid)
            .await
    }
    async fn ipam_allocate_and_record_pod_network(
        &self,
        sandbox_id: &str,
        pod: &crate::pod_identity::PodIdentity,
        subnet_base_int: u32,
        subnet_size: u32,
        veth_host: &str,
        netns_path: &str,
    ) -> Result<(String, u32)> {
        self.inner
            .ipam_allocate_and_record_pod_network(
                sandbox_id,
                pod,
                subnet_base_int,
                subnet_size,
                veth_host,
                netns_path,
            )
            .await
    }
    async fn list_sandboxes(&self) -> Result<Vec<SandboxRef>> {
        self.inner.list_sandboxes().await
    }
    async fn list_pod_network_sandbox_ids(&self) -> Result<Vec<String>> {
        self.inner.list_pod_network_sandbox_ids().await
    }
    async fn gc_watch_events(&self, max_rows: i64, batch_cap: i64) -> Result<usize> {
        let before_rv = self.inner.get_current_resource_version().await.unwrap_or(0);
        let removed = self.inner.gc_watch_events(max_rows, batch_cap).await?;
        if removed == 0 {
            return Ok(0);
        }
        let rv = resource_version_after_non_resource_write(self.inner.as_ref(), before_rv).await?;
        self.notify_if_configured(
            StorageCommand::GcWatchEvents {
                max_rows,
                batch_cap,
            },
            self.meta_for_rv(rv, None),
        )
        .await;
        Ok(removed)
    }
    async fn pod_endpoint_get_by_pod_ip(&self, pod_ip: Ipv4Addr) -> Result<Option<PodEndpointRow>> {
        self.inner.pod_endpoint_get_by_pod_ip(pod_ip).await
    }

    async fn pod_endpoint_list_all(&self) -> Result<Vec<PodEndpointRow>> {
        self.inner.pod_endpoint_list_all().await
    }

    fn subscribe_pod_endpoints(&self) -> broadcast::Receiver<PodEndpointEvent> {
        if true {
            self.inner.subscribe_pod_endpoints()
        } else {
            let (_tx, rx) = broadcast::channel(1);
            rx
        }
    }

    async fn get_klights_meta(&self, key: &str) -> Result<Option<String>> {
        self.inner.get_klights_meta(key).await
    }

    async fn set_klights_meta(&self, key: &str, value: &str) -> Result<()> {
        let proposer = self.require_raft_proposer()?;
        let command = StorageCommand::SetKlightsMeta {
            key: key.to_string(),
            value: value.to_string(),
        };
        self.propose_command_via_raft(&proposer, command).await
    }

    async fn get_applied_outbox(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<AppliedOutboxRecord>> {
        self.inner.get_applied_outbox(idempotency_key).await
    }

    async fn insert_applied_outbox(&self, record: AppliedOutboxRecord) -> Result<bool> {
        self.inner.insert_applied_outbox(record).await
    }

    async fn list_applied_outbox(&self) -> Result<Vec<AppliedOutboxRecord>> {
        self.inner.list_applied_outbox().await
    }

    async fn delete_uncommitted_applied_outbox_placeholder(
        &self,
        idempotency_key: &str,
    ) -> Result<bool> {
        self.inner
            .delete_uncommitted_applied_outbox_placeholder(idempotency_key)
            .await
    }

    async fn apply_outbox_transactionally(
        &self,
        idempotency_key: &str,
        operation: &str,
        payload: &[u8],
        authoring_node: &str,
    ) -> std::result::Result<
        crate::kubelet::outbox::OutboxApplyResult,
        crate::kubelet::outbox::OutboxApplyError,
    > {
        let command = match crate::kubelet::outbox::payload::OutboxPayload::decode_protobuf(payload)
        {
            Ok(payload) => payload.command,
            Err(err) => {
                return Err(crate::kubelet::outbox::OutboxApplyError::Retryable(
                    err.to_string(),
                ));
            }
        };
        if operation == crate::kubelet::outbox::payload::OutboxOperation::LeaseRenew.as_str() {
            crate::node_lease_tracker::ensure_lease_renew_command(&command, authoring_node)
                .map_err(|err| {
                    crate::kubelet::outbox::OutboxApplyError::ConflictTerminal(err.to_string())
                })?;
            return Ok(crate::kubelet::outbox::OutboxApplyResult::Applied { applied_rv: 0 });
        }
        let proposer = self
            .require_raft_proposer()
            .map_err(|e| crate::kubelet::outbox::OutboxApplyError::Retryable(e.to_string()))?;
        proposer
            .propose_outbox_command(idempotency_key, operation, command, authoring_node)
            .await
    }

    async fn build_log_apply_commit_for_outbox(
        &self,
        idempotency_key: &str,
        operation: &str,
        payload: &[u8],
        authoring_node: &str,
    ) -> std::result::Result<
        crate::datastore::sqlite::BuildOutboxOutcome,
        crate::kubelet::outbox::OutboxApplyError,
    > {
        self.inner
            .build_log_apply_commit_for_outbox(idempotency_key, operation, payload, authoring_node)
            .await
    }

    async fn gc_applied_outbox(&self, now_ms: i64, ttl_ms: i64) -> Result<usize> {
        let removed = self.inner.gc_applied_outbox(now_ms, ttl_ms).await?;
        if removed > 0 {
            // T3: `append_log_apply_entry` removed. The outbox GC
            // commit was only needed for BackupApplier replay; raft
            // AppendEntries handles it. The observer still fires so
            // workers see the outbox cleanup via the Connect stream.
            if let Some(observer) = &self.observer {
                let current_rv = self.inner.get_current_resource_version().await.unwrap_or(0);
                observer
                    .notify(
                        StorageCommand::AdvanceResourceVersion {
                            min_rv: current_rv,
                            new_rv: current_rv,
                        },
                        self.meta_for_rv(current_rv, None),
                    )
                    .await;
            }
        }
        Ok(removed)
    }

    async fn apply_replicated_command(
        &self,
        command: StorageCommand,
        meta: CommandMeta,
    ) -> Result<()> {
        apply_command_to_backend(self.inner.as_ref(), command, meta).await
    }

    async fn current_log_apply_index(&self) -> Result<i64> {
        self.inner.current_log_apply_index().await
    }
}

async fn resource_version_after_non_resource_write<B>(backend: &B, before_rv: i64) -> Result<i64>
where
    B: DatastoreBackend + ?Sized,
{
    let after_rv = backend
        .get_current_resource_version()
        .await
        .unwrap_or(before_rv);
    if after_rv > before_rv {
        Ok(after_rv)
    } else {
        backend
            .advance_resource_version_after(before_rv.saturating_add(1))
            .await
    }
}
