//! `DatastoreBackend` implementation for `RedbDatastore`.
//!
//! Every trait method delegates to the appropriate composed domain store.
//! Methods that need combined logic (preconditions + delete, get_namespace, etc.)
//! are implemented inline.

use std::net::Ipv4Addr;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::broadcast;

use ::redb::{ReadableDatabase, ReadableTable};

use crate::controllers::annotations::NodePeerMode;
use crate::datastore::backend::DatastoreBackend;
use crate::datastore::redb::helpers;
use crate::datastore::redb::tables;
use crate::datastore::types::*;
use crate::networking::types::HostPortRange;
use crate::watch::{WatchSignal, WatchTopic};

use super::RedbDatastore;

#[async_trait]
impl DatastoreBackend for RedbDatastore {
    fn close(&self) {
        self.accessor.close();
    }

    fn subscribe_watch_signals(&self, topic: WatchTopic) -> broadcast::Receiver<WatchSignal> {
        self.watch_bus.subscribe_signals(topic)
    }

    #[cfg(test)]
    fn subscribe_watch(&self, topic: WatchTopic) -> broadcast::Receiver<crate::watch::WatchEvent> {
        self.watch_bus.subscribe(topic)
    }

    #[cfg(test)]
    fn subscribe_watch_many(&self, topics: Vec<WatchTopic>) -> crate::watch::WatchReceiver {
        self.watch_bus.subscribe_many(topics)
    }

    #[cfg(test)]
    fn broadcast_watch_event(&self, pending: PendingWatchEvent) {
        let event = pending.event;
        if let Some(signal) = WatchSignal::from_event(&event) {
            self.watch_bus.publish_signal(signal);
        }
        self.watch_bus.publish(event);
    }

    async fn apply_raft_log_apply_commit(
        &self,
        _commit: crate::log_apply::LogApplyCommit,
    ) -> Result<crate::datastore::raft::types::StorageCommandResult> {
        Err(anyhow!(
            "redb backend does not support raft log-apply commit replay"
        ))
    }

    async fn create_resource(
        &self,
        a: &str,
        k: &str,
        n: Option<&str>,
        m: &str,
        d: Value,
    ) -> Result<Resource> {
        self.resources.create_res(a, k, n, m, d).await
    }
    async fn get_resource(
        &self,
        a: &str,
        k: &str,
        n: Option<&str>,
        m: &str,
    ) -> Result<Option<Resource>> {
        self.resources.get_res(a, k, n, m).await
    }
    async fn update_resource(
        &self,
        a: &str,
        k: &str,
        n: Option<&str>,
        m: &str,
        d: Value,
        e: i64,
    ) -> Result<Resource> {
        self.resources.update_res(a, k, n, m, d, e).await
    }
    async fn update_resource_with_preconditions(
        &self,
        a: &str,
        k: &str,
        n: Option<&str>,
        m: &str,
        d: Value,
        p: ResourcePreconditions,
    ) -> Result<Resource> {
        self.resources
            .update_res_with_preconditions(a, k, n, m, d, p)
            .await
    }
    async fn delete_resource(&self, a: &str, k: &str, n: Option<&str>, m: &str) -> Result<()> {
        self.resources.delete_res(a, k, n, m).await
    }
    async fn delete_resource_with_preconditions(
        &self,
        a: &str,
        k: &str,
        n: Option<&str>,
        m: &str,
        p: ResourcePreconditions,
    ) -> Result<()> {
        if p.uid.is_some() || p.resource_version.is_some() {
            let Some(resource) = self.resources.get_res(a, k, n, m).await? else {
                return Err(anyhow!("not found"));
            };
            if let Some(expected_uid) = p.uid.as_deref() {
                let actual_uid = resource
                    .data
                    .pointer("/metadata/uid")
                    .and_then(|v| v.as_str());
                if actual_uid != Some(expected_uid) {
                    return Err(crate::datastore::errors::DatastoreError::conflict(
                        "UID precondition failed",
                    )
                    .into());
                }
            }
            if let Some(expected_rv) = p.resource_version
                && resource.resource_version != expected_rv
            {
                return Err(crate::datastore::errors::DatastoreError::conflict(
                    "resourceVersion precondition failed",
                )
                .into());
            }
        }
        self.resources.delete_res(a, k, n, m).await
    }
    async fn list_resources(
        &self,
        a: &str,
        k: &str,
        n: Option<&str>,
        query: ResourceListQuery<'_>,
    ) -> Result<ResourceList> {
        self.resources.list_res(a, k, n, query).await
    }
    async fn list_resources_page(
        &self,
        a: &str,
        k: &str,
        n: Option<&str>,
        ls: Option<&str>,
        fs: Option<&str>,
        page: ListPageRequest,
    ) -> Result<ResourceList> {
        self.resources.list_res_page(a, k, n, ls, fs, page).await
    }
    async fn list_resource_keys_for_scope(
        &self,
        a: String,
        k: String,
        namespaced: bool,
    ) -> Result<Vec<(Option<String>, String)>> {
        self.namespaces
            .list_resource_keys_for_scope_impl(&a, &k, namespaced)
            .await
    }
    async fn update_status_only(
        &self,
        a: &str,
        k: &str,
        n: Option<&str>,
        m: &str,
        s: Value,
        e: Option<i64>,
    ) -> Result<Resource> {
        self.resources
            .update_status_only_impl(a, k, n, m, s, e)
            .await
    }
    async fn update_status_only_with_preconditions(
        &self,
        a: &str,
        k: &str,
        n: Option<&str>,
        m: &str,
        s: Value,
        p: ResourcePreconditions,
    ) -> Result<Resource> {
        if let Some(expected_uid) = p.uid.as_deref() {
            let Some(resource) = self.resources.get_res(a, k, n, m).await? else {
                return Err(anyhow!("not found"));
            };
            let actual_uid = resource
                .data
                .pointer("/metadata/uid")
                .and_then(|v| v.as_str());
            if actual_uid != Some(expected_uid) {
                return Err(crate::datastore::errors::DatastoreError::conflict(
                    "UID precondition failed",
                )
                .into());
            }
        }
        self.resources
            .update_status_only_impl(a, k, n, m, s, p.resource_version)
            .await
    }
    async fn get_current_resource_version(&self) -> Result<i64> {
        self.accessor
            .call("get_current_resource_version", move |db| {
                let r = db.begin_read()?;
                let m = r.open_table(tables::META)?;
                Ok(m.get("rv")?
                    .map(|g| {
                        std::str::from_utf8(g.value())
                            .unwrap_or("0")
                            .parse()
                            .unwrap_or(0)
                    })
                    .unwrap_or(0))
            })
            .await
    }
    async fn create_namespace(&self, n: &str, d: Value) -> Result<Resource> {
        self.namespaces.create_ns(n, d).await
    }
    async fn get_namespace(&self, n: &str) -> Result<Option<Resource>> {
        let n_owned = n.to_string();
        self.accessor
            .call("get_namespace", move |db| {
                let n: &str = &n_owned;
                let r = db.begin_read()?;
                let t = r.open_table(tables::NAMESPACES)?;
                Ok(t.get(n)?.map(|g| {
                    let data = helpers::body_val(g.value());
                    Resource {
                        id: 0,
                        api_version: "v1".into(),
                        kind: "Namespace".into(),
                        namespace: None,
                        name: n.into(),
                        uid: Resource::uid_from_data(&data),
                        resource_version: 0,
                        data,
                    }
                }))
            })
            .await
    }
    async fn list_namespaces(&self, ls: Option<&str>, fs: Option<&str>) -> Result<ResourceList> {
        let ls_owned = ls.map(|s| s.to_string());
        let fs_owned = fs.map(|s| s.to_string());
        let parsed_label_reqs = if let Some(ref sel) = ls_owned {
            Some(crate::label_selector::parse_label_selector(sel)?)
        } else {
            None
        };
        let mut items = self
            .accessor
            .call("list_namespaces", move |db| {
                let r = db.begin_read()?;
                let t = r.open_table(tables::NAMESPACES)?;
                let items: Vec<_> = t
                    .iter()?
                    .filter_map(|e| e.ok())
                    .map(|(k, v)| Resource {
                        id: 0,
                        api_version: "v1".into(),
                        kind: "Namespace".into(),
                        namespace: None,
                        name: k.value().into(),
                        uid: Resource::uid_from_data(&helpers::body_val(v.value())),
                        resource_version: 0,
                        data: helpers::body_val(v.value()),
                    })
                    .collect();
                Ok(items)
            })
            .await?;

        if let Some(reqs) = &parsed_label_reqs {
            items.retain(|item| {
                let labels = item
                    .data
                    .get("metadata")
                    .and_then(|m| m.get("labels"))
                    .and_then(|l| l.as_object());
                reqs.iter().all(|req| req.matches(labels))
            });
        }
        if let Some(fs) = fs_owned.as_deref() {
            items = helpers::filter_by_field_selector(items, fs);
        }
        Ok(ResourceList {
            resource_version: 0,
            items,
            continue_token: None,
            remaining_item_count: None,
        })
    }
    async fn list_namespaces_page(
        &self,
        ls: Option<&str>,
        fs: Option<&str>,
        page: ListPageRequest,
    ) -> Result<ResourceList> {
        let list = self.list_namespaces(ls, fs).await?;
        Ok(page.apply_to_sorted_resource_list(list))
    }
    async fn update_namespace(&self, n: &str, d: Value, e: i64) -> Result<Resource> {
        self.namespaces.update_ns_impl(n, d, e).await
    }
    async fn delete_namespace_contents(&self, n: &str) -> Result<()> {
        self.namespaces.delete_namespace_contents_impl(n).await
    }
    async fn delete_namespace(&self, n: &str) -> Result<()> {
        self.namespaces.delete_ns_impl(n).await
    }
    async fn pod_workqueue_enqueue(
        &self,
        kind: PodWorkqueueKind,
        pod: &crate::pod_identity::PodIdentity,
        payload: Value,
        ac: i64,
        md: i64,
        le: Option<&str>,
    ) -> Result<()> {
        self.workqueue.enqueue(kind, pod, payload, ac, md, le).await
    }
    async fn pod_workqueue_peek_next_due(&self) -> Result<Option<i64>> {
        self.workqueue.peek_next_due().await
    }
    async fn pod_workqueue_claim_due(&self, now_ms: i64) -> Result<Option<PodWorkqueueEntry>> {
        self.workqueue.claim_due(now_ms).await
    }
    async fn pod_workqueue_complete(&self, id: i64) -> Result<()> {
        self.workqueue.complete(id).await
    }
    async fn pod_workqueue_record_failure(
        &self,
        row: PodWorkqueueEntry,
        md: i64,
        e: &str,
    ) -> Result<()> {
        self.workqueue.record_failure(row, md, e).await
    }
    async fn pod_workqueue_dead_letter(&self, id: i64, e: &str) -> Result<()> {
        self.workqueue.dead_letter(id, e).await
    }
    async fn record_sandbox(&self, ns: &str, pn: &str, pu: &str, si: &str) -> Result<()> {
        self.sandboxes.record(ns, pn, pu, si).await
    }
    async fn get_sandbox(&self, ns: &str, pn: &str) -> Result<Option<String>> {
        self.sandboxes.get_for_pod(ns, pn).await
    }
    async fn get_sandbox_for_uid(&self, ns: &str, pn: &str, pu: &str) -> Result<Option<String>> {
        self.sandboxes.get_for_uid(ns, pn, pu).await
    }
    async fn delete_sandbox(&self, ns: &str, pn: &str) -> Result<()> {
        self.sandboxes.delete_for_pod(ns, pn).await
    }
    async fn delete_sandbox_for_uid(&self, ns: &str, pn: &str, pu: &str, si: &str) -> Result<()> {
        self.sandboxes.delete_for_uid(ns, pn, pu, si).await
    }
    async fn delete_pod_network(&self, sid: &str) -> Result<()> {
        self.network.delete_pnet(sid).await
    }
    async fn find_owned_resources(&self, o: &str, ns: Option<&str>) -> Result<Vec<Resource>> {
        self.resources.find_owned(o, ns).await
    }
    async fn list_resources_by_owner_uid(
        &self,
        a: &str,
        k: &str,
        ns: Option<&str>,
        o: &str,
    ) -> Result<Vec<Resource>> {
        let mut resources = self.resources.find_owned(o, ns).await?;
        resources.retain(|r| r.api_version == a && r.kind == k);
        Ok(resources)
    }
    async fn find_owned_by_name_kind_empty_uid(
        &self,
        owner_api_version: &str,
        owner_name: &str,
        owner_kind: &str,
        ns: Option<&str>,
    ) -> Result<Vec<Resource>> {
        let candidates = self.resources.find_owned("", ns).await?;
        let filtered: Vec<Resource> = candidates
            .into_iter()
            .filter(|r| {
                let refs = r
                    .data
                    .get("metadata")
                    .and_then(|m| m.get("ownerReferences"))
                    .and_then(|v| v.as_array());
                match refs {
                    Some(refs) => refs.iter().any(|ore| {
                        ore.get("uid")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .is_empty()
                            && ore.get("apiVersion").and_then(|v| v.as_str())
                                == Some(owner_api_version)
                            && ore.get("kind").and_then(|v| v.as_str()) == Some(owner_kind)
                            && ore.get("name").and_then(|v| v.as_str()) == Some(owner_name)
                    }),
                    None => false,
                }
            })
            .collect();
        Ok(filtered)
    }
    async fn list_cluster_resources_modified_since(
        &self,
        a: &str,
        k: &str,
        s: i64,
    ) -> Result<Vec<CatchUpResource>> {
        self.watch_store.modified_since(a, k, None, s).await
    }
    async fn list_cluster_resources(&self) -> Result<Vec<Resource>> {
        self.namespaces.list_cluster_resources_impl().await
    }
    async fn list_resources_modified_since(
        &self,
        a: &str,
        k: &str,
        ns: Option<&str>,
        s: i64,
    ) -> Result<Vec<CatchUpResource>> {
        self.watch_store.modified_since(a, k, ns, s).await
    }
    async fn advance_resource_version_after(&self, min_rv: i64) -> Result<i64> {
        self.rv_store.advance_rv(min_rv).await
    }
    async fn list_namespace_resources(&self, ns: &str) -> Result<Vec<Resource>> {
        self.namespaces.list_namespace_resources_impl(ns).await
    }
    async fn list_namespace_resources_of_kind(&self, ns: &str, k: &str) -> Result<Vec<Resource>> {
        self.namespaces
            .list_namespace_resources_of_kind_impl(ns, k)
            .await
    }
    async fn list_namespace_resources_excluding_kind(
        &self,
        ns: &str,
        k: &str,
    ) -> Result<Vec<Resource>> {
        self.namespaces
            .list_namespace_resources_excluding_kind_impl(ns, k)
            .await
    }
    async fn count_namespace_resources(&self, ns: &str) -> Result<i64> {
        self.namespaces.count_namespace_resources_impl(ns).await
    }
    async fn list_watch_events_since(
        &self,
        t: &[WatchTarget],
        s: i64,
    ) -> Result<Vec<CatchUpResource>> {
        self.watch_store.watch_list(t, s).await
    }
    async fn list_watch_events_since_checked(
        &self,
        t: &[WatchTarget],
        s: i64,
    ) -> Result<WatchReplayRead> {
        self.watch_store.watch_list_checked(t, s).await
    }
    async fn list_watch_events_since_checked_bounded(
        &self,
        t: &[WatchTarget],
        s: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<WatchReplayRead> {
        self.watch_store
            .watch_list_checked_bounded(t, s, limit)
            .await
    }
    async fn list_all_watch_events_since(&self, s: i64) -> Result<Vec<CatchUpResource>> {
        self.watch_store.watch_list_all_since(s).await
    }
    async fn list_deleted_watch_events_since(&self, s: i64) -> Result<Vec<CatchUpResource>> {
        self.watch_store.watch_list_deleted_since(s).await
    }
    async fn allocate_node_subnet(&self, n: &str, c: &str, i: &str) -> Result<NodeSubnet> {
        self.network.allocate_node_subnet(n, c, i).await
    }
    async fn update_node_peer_attributes(
        &self,
        n: &str,
        mode: NodePeerMode,
        hpr: Option<HostPortRange>,
    ) -> Result<()> {
        self.network.update_peer_attrs(n, mode, hpr).await
    }
    async fn update_node_dataplane(
        &self,
        metadata: crate::networking::wireguard::DataplanePeerMetadata,
    ) -> Result<()> {
        self.network.update_node_dataplane(metadata).await
    }
    async fn get_node_dataplane(
        &self,
        node_name: &str,
    ) -> Result<Option<crate::networking::wireguard::DataplanePeerMetadata>> {
        self.network.get_node_dataplane(node_name).await
    }
    async fn get_node_subnet(&self, n: &str) -> Result<Option<NodeSubnet>> {
        self.network.get_node_subnet(n).await
    }
    async fn list_peer_subnets(&self, m: &str) -> Result<Vec<NodeSubnet>> {
        self.network.list_peer_subnets(m).await
    }
    async fn delete_node_subnet(&self, n: &str) -> Result<()> {
        self.network.delete_node_subnet(n).await
    }
    async fn pod_slot_try_admit(
        &self,
        ns: &str,
        pod: &str,
        uid: &str,
        node: &str,
    ) -> Result<PodSlotAdmissionResult> {
        self.pod_slots.try_admit(ns, pod, uid, node).await
    }
    async fn pod_slot_mark_terminating(
        &self,
        ns: &str,
        pod: &str,
        uid: &str,
        node: &str,
    ) -> Result<()> {
        self.pod_slots.mark_terminating(ns, pod, uid, node).await
    }
    async fn pod_slot_clear_if_uid(
        &self,
        ns: &str,
        pod: &str,
        uid: &str,
        node: &str,
    ) -> Result<()> {
        self.pod_slots.clear_if_uid(ns, pod, uid, node).await
    }
    fn subscribe_pod_slot_admissions(&self) -> broadcast::Receiver<PodSlotAdmissionEvent> {
        self.pod_slots.subscribe()
    }
    async fn patch_resource_latest(
        &self,
        a: &str,
        k: &str,
        ns: Option<&str>,
        n: &str,
        _pk: PatchKind,
        p: Value,
    ) -> Result<Option<Resource>> {
        self.resources.patch(a, k, ns, n, p).await
    }
    async fn patch_resource_latest_with_preconditions(
        &self,
        a: &str,
        k: &str,
        ns: Option<&str>,
        n: &str,
        request: ResourcePatchRequest,
    ) -> Result<Option<Resource>> {
        let ResourcePatchRequest {
            patch_kind,
            patch,
            preconditions,
            strict_resource_version: _,
        } = request;
        if preconditions.uid.is_some() || preconditions.resource_version.is_some() {
            let Some(resource) = self.resources.get_res(a, k, ns, n).await? else {
                return Ok(None);
            };
            if let Some(expected_uid) = preconditions.uid.as_deref() {
                let actual_uid = resource
                    .data
                    .pointer("/metadata/uid")
                    .and_then(|v| v.as_str());
                if actual_uid != Some(expected_uid) {
                    return Err(crate::datastore::errors::DatastoreError::conflict(
                        "UID precondition failed",
                    )
                    .into());
                }
            }
            if let Some(expected_rv) = preconditions.resource_version
                && resource.resource_version != expected_rv
            {
                return Err(crate::datastore::errors::DatastoreError::conflict(
                    "resourceVersion precondition failed",
                )
                .into());
            }
        }
        self.patch_resource_latest(a, k, ns, n, patch_kind, patch)
            .await
    }
    async fn get_pod_network(&self, sid: &str) -> Result<Option<PodNetworkEndpoint>> {
        self.network.get_pnet(sid).await
    }
    async fn get_pod_network_for_pod(
        &self,
        ns: &str,
        pn: &str,
        pu: &str,
    ) -> Result<Option<PodNetworkEndpoint>> {
        self.network.get_pnet_for_pod(ns, pn, pu).await
    }
    async fn ipam_allocate_and_record_pod_network(
        &self,
        sid: &str,
        pod: &crate::pod_identity::PodIdentity,
        sb: u32,
        ss: u32,
        vh: &str,
        np: &str,
    ) -> Result<(String, u32)> {
        self.network
            .ipam_alloc(PodNetworkAllocationRequest::new(
                sid,
                PodNetworkAllocationPod::new(&pod.namespace, &pod.name, &pod.uid),
                PodNetworkAllocationSubnet::new(sb, ss),
                PodNetworkAllocationLink::new(vh, np),
            ))
            .await
    }
    async fn list_sandboxes(&self) -> Result<Vec<SandboxRef>> {
        self.sandboxes.list_all().await
    }
    async fn list_pod_network_sandbox_ids(&self) -> Result<Vec<String>> {
        self.network.list_pnet_sandbox_ids().await
    }
    async fn watch_events_gc_prunable_count(&self, m: i64, b: i64) -> Result<usize> {
        self.watch_store.gc_watch_prunable_count(m, b).await
    }
    async fn gc_watch_events(&self, m: i64, b: i64) -> Result<usize> {
        self.watch_store.gc_watch(m, b).await
    }
    async fn applied_outbox_gc_prunable_count(&self, cutoff_ms: i64) -> Result<usize> {
        use crate::datastore::redb::tables::APPLIED_OUTBOX;
        self.accessor
            .call("redb_applied_outbox_prunable_count", move |db| {
                let read_txn = db
                    .begin_read()
                    .map_err(|e| anyhow::anyhow!("redb read: {}", e))?;
                let table = read_txn
                    .open_table(APPLIED_OUTBOX)
                    .map_err(|e| anyhow::anyhow!("redb open applied_outbox table: {}", e))?;
                let mut count = 0usize;
                for row in table
                    .iter()
                    .map_err(|e| anyhow::anyhow!("redb applied_outbox iter: {}", e))?
                {
                    let (_, value) =
                        row.map_err(|e| anyhow::anyhow!("redb applied_outbox row: {}", e))?;
                    let record: AppliedOutboxRecord = serde_json::from_slice(value.value())?;
                    if record.first_seen_ms < cutoff_ms {
                        count += 1;
                    }
                }
                Ok(count)
            })
            .await
    }
    async fn pod_endpoint_get_by_pod_ip(&self, ip: Ipv4Addr) -> Result<Option<PodEndpointRow>> {
        self.network.pod_endpoint_get_by_pod_ip(ip).await
    }
    async fn pod_endpoint_list_all(&self) -> Result<Vec<PodEndpointRow>> {
        self.network.pod_endpoint_list_all().await
    }
    fn subscribe_pod_endpoints(&self) -> broadcast::Receiver<PodEndpointEvent> {
        self.network.subscribe_endpoints()
    }

    async fn get_klights_meta(&self, key: &str) -> anyhow::Result<Option<String>> {
        use crate::datastore::redb::tables::KLIGHTS_META;
        let key_owned = key.to_string();
        self.accessor
            .call("redb_get_klights_meta", move |db| {
                let read_txn = db
                    .begin_read()
                    .map_err(|e| anyhow::anyhow!("redb read: {}", e))?;
                let table = read_txn
                    .open_table(KLIGHTS_META)
                    .map_err(|e| anyhow::anyhow!("redb open meta table: {}", e))?;
                let result = table
                    .get(key_owned.as_str())
                    .map_err(|e| anyhow::anyhow!("redb meta get: {}", e))?;
                Ok(result.map(|v| v.value().to_string()))
            })
            .await
    }

    async fn set_klights_meta(&self, key: &str, value: &str) -> anyhow::Result<()> {
        use crate::datastore::redb::tables::KLIGHTS_META;
        let key_owned = key.to_string();
        let value_owned = value.to_string();
        self.accessor
            .call("redb_set_klights_meta", move |db| {
                let write_txn = db
                    .begin_write()
                    .map_err(|e| anyhow::anyhow!("redb write: {}", e))?;
                {
                    let mut table = write_txn
                        .open_table(KLIGHTS_META)
                        .map_err(|e| anyhow::anyhow!("redb open meta table: {}", e,))?;
                    table
                        .insert(key_owned.as_str(), value_owned.as_str())
                        .map_err(|e| anyhow::anyhow!("redb meta insert: {}", e))?;
                }
                write_txn
                    .commit()
                    .map_err(|e| anyhow::anyhow!("redb commit: {}", e))?;
                Ok(())
            })
            .await
    }

    async fn get_applied_outbox(
        &self,
        idempotency_key: &str,
    ) -> anyhow::Result<Option<AppliedOutboxRecord>> {
        use crate::datastore::redb::tables::APPLIED_OUTBOX;
        let key = idempotency_key.to_string();
        self.accessor
            .call("redb_get_applied_outbox", move |db| {
                let read_txn = db
                    .begin_read()
                    .map_err(|e| anyhow::anyhow!("redb read: {}", e))?;
                let table = read_txn
                    .open_table(APPLIED_OUTBOX)
                    .map_err(|e| anyhow::anyhow!("redb open applied_outbox table: {}", e,))?;
                let Some(record) = table
                    .get(key.as_str())
                    .map_err(|e| anyhow::anyhow!("redb applied_outbox get: {}", e))?
                else {
                    return Ok(None);
                };
                Ok(Some(serde_json::from_slice(record.value())?))
            })
            .await
    }

    async fn insert_applied_outbox(&self, record: AppliedOutboxRecord) -> Result<bool> {
        use crate::datastore::redb::tables::APPLIED_OUTBOX;
        self.accessor
            .call("redb_insert_applied_outbox", move |db| {
                let write_txn = db
                    .begin_write()
                    .map_err(|e| anyhow::anyhow!("redb write: {}", e))?;
                let inserted = {
                    let mut table = write_txn
                        .open_table(APPLIED_OUTBOX)
                        .map_err(|e| anyhow::anyhow!("redb open applied_outbox table: {}", e,))?;
                    if table
                        .get(record.idempotency_key.as_str())
                        .map_err(|e| anyhow::anyhow!("redb applied_outbox get: {}", e))?
                        .is_some()
                    {
                        false
                    } else {
                        let bytes = serde_json::to_vec(&record)?;
                        table
                            .insert(record.idempotency_key.as_str(), bytes.as_slice())
                            .map_err(|e| anyhow::anyhow!("redb applied_outbox insert: {}", e,))?;
                        true
                    }
                };
                write_txn
                    .commit()
                    .map_err(|e| anyhow::anyhow!("redb commit: {}", e))?;
                Ok(inserted)
            })
            .await
    }

    async fn list_applied_outbox(&self) -> Result<Vec<AppliedOutboxRecord>> {
        use crate::datastore::redb::tables::APPLIED_OUTBOX;
        self.accessor
            .call("redb_list_applied_outbox", move |db| {
                let read_txn = db
                    .begin_read()
                    .map_err(|e| anyhow::anyhow!("redb read: {}", e))?;
                let table = read_txn
                    .open_table(APPLIED_OUTBOX)
                    .map_err(|e| anyhow::anyhow!("redb open applied_outbox table: {}", e))?;
                let mut rows = Vec::new();
                for row in table
                    .iter()
                    .map_err(|e| anyhow::anyhow!("redb applied_outbox iter: {}", e))?
                {
                    let (_key, value) =
                        row.map_err(|e| anyhow::anyhow!("redb applied_outbox row: {}", e))?;
                    rows.push(serde_json::from_slice(value.value())?);
                }
                rows.sort_by(|a: &AppliedOutboxRecord, b| {
                    a.idempotency_key.cmp(&b.idempotency_key)
                });
                Ok(rows)
            })
            .await
    }

    async fn apply_outbox_transactionally(
        &self,
        _idempotency_key: &str,
        _operation: &str,
        _payload: &[u8],
        _authoring_node: &str,
    ) -> std::result::Result<
        crate::kubelet::outbox::OutboxApplyResult,
        crate::kubelet::outbox::OutboxApplyError,
    > {
        Err(crate::kubelet::outbox::OutboxApplyError::Retryable(
            "redb: apply_outbox_transactionally not implemented".to_string(),
        ))
    }

    async fn build_log_apply_commit_for_outbox(
        &self,
        _idempotency_key: &str,
        _operation: &str,
        _payload: &[u8],
        _authoring_node: &str,
    ) -> std::result::Result<
        crate::datastore::sqlite::BuildOutboxOutcome,
        crate::kubelet::outbox::OutboxApplyError,
    > {
        Err(crate::kubelet::outbox::OutboxApplyError::Retryable(
            "redb: build_log_apply_commit_for_outbox not implemented".to_string(),
        ))
    }

    async fn gc_applied_outbox(&self, now_ms: i64, ttl_ms: i64) -> Result<usize> {
        use crate::datastore::redb::tables::APPLIED_OUTBOX;

        let cutoff = now_ms.saturating_sub(ttl_ms);
        self.accessor
            .call("redb_applied_outbox_gc", move |db| {
                let write_txn = db
                    .begin_write()
                    .map_err(|e| anyhow::anyhow!("redb write: {}", e))?;
                let keys_to_remove = {
                    let table = write_txn
                        .open_table(APPLIED_OUTBOX)
                        .map_err(|e| anyhow::anyhow!("redb open applied_outbox table: {}", e))?;
                    let mut keys = Vec::new();
                    for row in table
                        .iter()
                        .map_err(|e| anyhow::anyhow!("redb applied_outbox iter: {}", e))?
                    {
                        let (key, value) =
                            row.map_err(|e| anyhow::anyhow!("redb applied_outbox row: {}", e))?;
                        let record: AppliedOutboxRecord = serde_json::from_slice(value.value())?;
                        if record.first_seen_ms < cutoff {
                            keys.push(key.value().to_string());
                        }
                    }
                    keys
                };
                let removed = {
                    let mut table = write_txn
                        .open_table(APPLIED_OUTBOX)
                        .map_err(|e| anyhow::anyhow!("redb open applied_outbox table: {}", e))?;
                    let removed = keys_to_remove.len();
                    for key in keys_to_remove {
                        table
                            .remove(key.as_str())
                            .map_err(|e| anyhow::anyhow!("redb applied_outbox remove: {}", e))?;
                    }
                    removed
                };
                write_txn
                    .commit()
                    .map_err(|e| anyhow::anyhow!("redb commit: {}", e))?;
                Ok(removed)
            })
            .await
    }
}
