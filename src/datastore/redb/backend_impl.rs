//! `DatastoreBackend` implementation for `RedbDatastore`.
//!
//! Every trait method delegates to the appropriate composed domain store.
//! Methods that need combined logic (preconditions + delete, get_namespace, etc.)
//! are implemented inline.

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use serde_json::Value;
#[cfg(test)]
use tokio::sync::broadcast;

use ::redb::{ReadableDatabase, ReadableTable};

use crate::datastore::backend::DatastoreBackend;
use crate::datastore::types::*;
use klights_cluster_core::{
    PatchKind, Resource, ResourceBatchOperation, ResourcePatchRequest, ResourcePreconditions,
    WatchReplayPosition,
};
use klights_cluster_datastore::redb::read_core::RedbCheckedWatchRead;
use klights_cluster_datastore::redb::read_core::RedbCollectionScope;
use klights_cluster_datastore::redb::read_core::RedbListQuery;
use klights_cluster_datastore::redb::read_core::RedbPositionedWatchRead;
use klights_cluster_datastore::redb::read_core::RedbSnapshotRead;
use klights_cluster_datastore::redb::tables;
use klights_types::HostPortRange;
use klights_types::NodePeerMode;
#[cfg(test)]
use klights_watch::{WatchSignal, WatchTopic};

use super::RedbDatastore;

fn legacy_target_to_durable(target: &WatchTarget) -> klights_cluster_store::DurableWatchTarget {
    match &target.scope {
        WatchTargetScope::Cluster => {
            klights_cluster_store::DurableWatchTarget::cluster(&target.api_version, &target.kind)
        }
        WatchTargetScope::Namespaced(None) => {
            klights_cluster_store::DurableWatchTarget::namespaced(&target.api_version, &target.kind)
        }
        WatchTargetScope::Namespaced(Some(namespace)) => {
            klights_cluster_store::DurableWatchTarget::namespaced_in_namespace(
                &target.api_version,
                &target.kind,
                namespace,
            )
        }
    }
}

fn durable_to_catchup(event: klights_cluster_store::DurableWatchEvent) -> CatchUpResource {
    let event_type = std::borrow::Cow::Owned(event.event_type().to_string());
    CatchUpResource {
        resource: event.into_resource(),
        event_type,
    }
}

fn durable_floor_to_legacy(floor: klights_cluster_store::DurableReplayFloor) -> WatchReplayFloor {
    let (target, floor_resource_version, floor_event_id, position_is_exact) = floor.into_parts();
    let (api_version, kind, namespace_key) = match target {
        klights_cluster_store::DurableReplayTarget::All => {
            ("*".to_string(), "*".to_string(), "*".to_string())
        }
        klights_cluster_store::DurableReplayTarget::Cluster { api_version, kind } => {
            (api_version, kind, "#cluster".to_string())
        }
        klights_cluster_store::DurableReplayTarget::Namespaced {
            api_version,
            kind,
            namespace,
        } => (api_version, kind, namespace),
    };
    WatchReplayFloor {
        api_version,
        kind,
        namespace_key,
        floor_resource_version,
        floor_event_id,
        position_is_exact,
    }
}

fn outbox_watermark_key(client_id: &str, stream_id: i64) -> Result<Vec<u8>> {
    if client_id.is_empty() || client_id.contains('\0') || stream_id <= 0 {
        return Err(anyhow!(
            "outbox watermark requires a non-empty NUL-free client ID and positive stream ID"
        ));
    }
    let mut key = Vec::with_capacity(client_id.len() + 9);
    key.extend_from_slice(client_id.as_bytes());
    key.push(0);
    key.extend_from_slice(&(stream_id as u64).to_be_bytes());
    Ok(key)
}

pub(super) fn decode_outbox_watermark_key(
    key: &[u8],
    stream_seq: i64,
) -> Result<klights_cluster_core::OutboxStreamWatermark> {
    if key.len() < 10 || key[key.len() - 9] != 0 {
        return Err(anyhow!("corrupt redb outbox-watermark key"));
    }
    let client_id = std::str::from_utf8(&key[..key.len() - 9])
        .map_err(|error| anyhow!("corrupt redb outbox-watermark client ID: {error}"))?
        .to_string();
    let stream_id = u64::from_be_bytes(
        key[key.len() - 8..]
            .try_into()
            .expect("watermark key suffix is eight bytes"),
    );
    let stream_id =
        i64::try_from(stream_id).map_err(|_| anyhow!("redb outbox stream ID exceeds i64"))?;
    if stream_seq <= 0 {
        return Err(anyhow!("corrupt redb outbox stream sequence {stream_seq}"));
    }
    Ok(klights_cluster_core::OutboxStreamWatermark {
        client_id,
        stream_id,
        stream_seq,
    })
}

#[async_trait]
impl DatastoreBackend for RedbDatastore {
    #[cfg(test)]
    fn commit_observation_sink(
        &self,
    ) -> std::sync::Arc<dyn crate::datastore::CommitObservationSink> {
        self.commit_sink.clone()
    }

    async fn read_durable_allocator_observation(&self) -> Result<DurableAllocatorObservation> {
        use klights_cluster_store::DurableAllocatorRead;
        let state = self
            .focused_read_store()
            .read_allocator_state()
            .await
            .map_err(anyhow::Error::from)?;
        Ok(DurableAllocatorObservation {
            position: state.position(),
        })
    }

    async fn read_cluster_metadata_observation(&self) -> Result<ClusterMetadataObservation> {
        self.accessor
            .call("redb_atomic_cluster_metadata_observation", |db| {
                let read = db.begin_read()?;
                let klights = read.open_table(tables::KLIGHTS_META)?;
                let get = |key: &str| -> Result<Option<String>> {
                    Ok(klights.get(key)?.map(|value| value.value().to_string()))
                };
                let cluster_id = get(klights_cluster_store::CLUSTER_ID_META_KEY)?
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| anyhow!("cluster_id is missing or empty"))?;
                let raw_epoch = get(klights_cluster_store::LEADER_EPOCH_META_KEY)?
                    .ok_or_else(|| anyhow!("leader_epoch is missing"))?;
                let leader_epoch = raw_epoch
                    .parse::<i64>()
                    .map_err(|_| anyhow!("invalid leader_epoch {raw_epoch:?}"))?;
                let meta = read.open_table(tables::META)?;
                let current_rv = match meta.get("rv")? {
                    None => 0,
                    Some(value) => {
                        let raw = std::str::from_utf8(value.value())
                            .map_err(|error| anyhow!("invalid resource_version UTF-8: {error}"))?;
                        raw.parse::<i64>()
                            .map_err(|_| anyhow!("invalid resource_version {raw:?}"))?
                    }
                };
                if leader_epoch < 0 || current_rv < 0 {
                    return Err(anyhow!(
                        "cluster metadata numeric values must be non-negative"
                    ));
                }
                let membership = match (
                    get(klights_cluster_store::RAFT_VOTERS_META_KEY)?,
                    get(klights_cluster_store::RAFT_TERM_META_KEY)?,
                    get(klights_cluster_store::RAFT_LEADER_HINT_META_KEY)?,
                ) {
                    (None, None, None) => ReplicatedMembershipState::AuthoritativeAbsent,
                    (Some(raw_voters), Some(raw_term), Some(raw_hint)) => {
                        let voters: Vec<String> = serde_json::from_str(&raw_voters)?;
                        let term = raw_term
                            .parse::<i64>()
                            .map_err(|_| anyhow!("invalid raft term {raw_term:?}"))?;
                        let mut unique = std::collections::HashSet::with_capacity(voters.len());
                        if term < 0
                            || voters.is_empty()
                            || voters
                                .iter()
                                .any(|voter| voter.is_empty() || !unique.insert(voter.as_str()))
                        {
                            return Err(anyhow!(
                                "membership contains an invalid term or voter set"
                            ));
                        }
                        ReplicatedMembershipState::Present(
                            klights_cluster_core::ClusterMembership {
                                cluster_id: cluster_id.clone(),
                                voters,
                                term,
                                leader_hint: (!raw_hint.is_empty()).then_some(raw_hint),
                            },
                        )
                    }
                    _ => return Err(anyhow!("membership metadata is incomplete")),
                };
                Ok(ClusterMetadataObservation {
                    metadata: klights_cluster_core::ClusterMetadata {
                        cluster_id,
                        leader_epoch,
                        current_rv,
                    },
                    membership,
                })
            })
            .await
    }

    async fn acquire_snapshot_exclusive_fence(
        &self,
    ) -> Result<Option<crate::datastore::backend::SnapshotExclusiveFence>> {
        Ok(Some(
            crate::datastore::backend::SnapshotExclusiveFence::new(
                self.accessor.acquire_snapshot_exclusive().await,
            ),
        ))
    }

    async fn acquire_snapshot_mutation_fence(
        &self,
    ) -> Result<Option<crate::datastore::backend::SnapshotMutationFence>> {
        Ok(Some(crate::datastore::backend::SnapshotMutationFence::new(
            self.accessor.acquire_snapshot_mutation().await,
        )))
    }

    async fn begin_pinned_snapshot_capture(
        &self,
        request: klights_cluster_store::SnapshotCaptureRequest,
    ) -> Result<Box<dyn klights_cluster_store::SnapshotCaptureSession>> {
        self.begin_redb_snapshot(request, None).await
    }

    async fn begin_pinned_snapshot_capture_with_anchor(
        &self,
        request: klights_cluster_store::SnapshotCaptureRequest,
        anchor: &dyn crate::datastore::backend::SnapshotCaptureAnchor,
    ) -> Result<Box<dyn klights_cluster_store::SnapshotCaptureSession>> {
        self.begin_redb_snapshot(request, Some(anchor)).await
    }

    fn close(&self) {
        self.accessor.close();
    }

    #[cfg(test)]
    fn subscribe_watch(&self, topic: WatchTopic) -> broadcast::Receiver<crate::watch::WatchEvent> {
        crate::watch_commit_observation_adapter::subscribe_test_events(
            self.commit_sink.as_ref(),
            topic,
        )
    }

    #[cfg(test)]
    fn subscribe_watch_many(&self, topics: Vec<WatchTopic>) -> crate::watch::WatchReceiver {
        crate::watch_commit_observation_adapter::subscribe_test_events_many(
            self.commit_sink.as_ref(),
            topics,
        )
    }

    #[cfg(test)]
    fn broadcast_watch_event(&self, pending: PendingWatchEvent) {
        let event = pending.event;
        let _ = WatchSignal::from_event(&event);
        crate::watch_commit_observation_adapter::publish_test_events(
            self.commit_sink.as_ref(),
            vec![event],
        );
    }

    async fn apply_raft_log_apply_commit(
        &self,
        _commit: klights_cluster_core::LogApplyCommit,
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
    #[cfg(test)]
    async fn apply_replicated_create_resource(
        &self,
        a: &str,
        k: &str,
        n: Option<&str>,
        m: &str,
        d: Value,
        o: crate::datastore::types::ReplicatedCreateOptions,
    ) -> Result<Resource> {
        self.resources
            .apply_replicated_create_resource(a, k, n, m, d, o)
            .await
    }
    async fn get_resource(
        &self,
        a: &str,
        k: &str,
        n: Option<&str>,
        m: &str,
    ) -> Result<Option<Resource>> {
        self.read_store.core().get_resource(a, k, n, m).await
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
                    return Err(klights_cluster_datastore::errors::DatastoreError::conflict(
                        "UID precondition failed",
                    )
                    .into());
                }
            }
            if let Some(expected_rv) = p.resource_version
                && resource.resource_version != expected_rv
            {
                return Err(klights_cluster_datastore::errors::DatastoreError::conflict(
                    "resourceVersion precondition failed",
                )
                .into());
            }
        }
        self.resources.delete_res(a, k, n, m).await
    }

    async fn delete_resource_without_watch_with_tombstone(
        &self,
        a: &str,
        k: &str,
        n: Option<&str>,
        m: &str,
        p: ResourcePreconditions,
        grace_seconds: i64,
    ) -> Result<Resource> {
        self.resources
            .delete_res_with_tombstone(a, k, n, m, p, grace_seconds)
            .await
    }

    async fn list_resources(
        &self,
        a: &str,
        k: &str,
        n: Option<&str>,
        query: ResourceListQuery<'_>,
    ) -> Result<ResourceList> {
        let cursor = query.continue_token.map(|name| {
            klights_cluster_store::ResourceCollectionKey::new(
                n.map(str::to_string),
                name.to_string(),
            )
        });
        let page = self
            .read_store
            .core()
            .list_resources(
                a,
                k,
                n.map_or(RedbCollectionScope::LegacyAny, |namespace| {
                    RedbCollectionScope::Namespace(namespace.to_string())
                }),
                RedbListQuery {
                    label_selector: query.label_selector.map(str::to_string),
                    field_selector: query.field_selector.map(str::to_string),
                    limit: query.limit,
                    cursor,
                },
            )
            .await?;
        Ok(ResourceList {
            resource_version: page.position.resource_version,
            watch_replay_position: Some(page.position),
            items: page.items,
            continue_token: page
                .continuation
                .map(|continuation| continuation.name().to_string()),
            remaining_item_count: page.remaining_item_count,
        })
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
    async fn list_resources_for_watch_targets(
        &self,
        targets: &[WatchTarget],
        label_selector: Option<&str>,
    ) -> Result<ResourceList> {
        self.resources
            .list_resources_for_watch_targets(targets, label_selector)
            .await
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
                return Err(klights_cluster_datastore::errors::DatastoreError::conflict(
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
        self.read_store
            .core()
            .get_resource("v1", "Namespace", None, n)
            .await
    }
    async fn list_namespaces(&self, ls: Option<&str>, fs: Option<&str>) -> Result<ResourceList> {
        let page = self
            .read_store
            .core()
            .list_resources(
                "v1",
                "Namespace",
                RedbCollectionScope::Cluster,
                RedbListQuery {
                    label_selector: ls.map(str::to_string),
                    field_selector: fs.map(str::to_string),
                    limit: None,
                    cursor: None,
                },
            )
            .await?;
        Ok(ResourceList {
            resource_version: page.position.resource_version,
            watch_replay_position: Some(page.position),
            items: page.items,
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
    async fn find_owned_resources(&self, o: &str, ns: Option<&str>) -> Result<Vec<Resource>> {
        self.read_store.core().find_owned(o, ns).await
    }
    async fn list_resources_by_owner_uid(
        &self,
        a: &str,
        k: &str,
        ns: Option<&str>,
        o: &str,
    ) -> Result<Vec<Resource>> {
        let mut resources = self.read_store.core().find_owned(o, ns).await?;
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
        let candidates = self.read_store.core().find_owned("", ns).await?;
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
        self.read_store
            .core()
            .watch_events_since(
                &[klights_cluster_store::DurableWatchTarget::cluster(a, k)],
                s,
            )
            .await
            .map(|events| events.into_iter().map(durable_to_catchup).collect())
    }
    async fn list_cluster_resources(&self) -> Result<Vec<Resource>> {
        self.read_store.core().list_cluster_resources().await
    }
    async fn list_resources_modified_since(
        &self,
        a: &str,
        k: &str,
        ns: Option<&str>,
        s: i64,
    ) -> Result<Vec<CatchUpResource>> {
        let target = ns.map_or_else(
            || klights_cluster_store::DurableWatchTarget::cluster(a, k),
            |namespace| {
                klights_cluster_store::DurableWatchTarget::namespaced_in_namespace(a, k, namespace)
            },
        );
        self.read_store
            .core()
            .watch_events_since(&[target], s)
            .await
            .map(|events| events.into_iter().map(durable_to_catchup).collect())
    }
    async fn advance_resource_version_after(&self, min_rv: i64) -> Result<i64> {
        self.rv_store.advance_rv(min_rv).await
    }
    async fn list_namespace_resources(&self, ns: &str) -> Result<Vec<Resource>> {
        self.read_store
            .core()
            .list_namespace_resources(ns, None, false)
            .await
    }
    async fn list_namespace_resources_of_kind(&self, ns: &str, k: &str) -> Result<Vec<Resource>> {
        self.read_store
            .core()
            .list_namespace_resources(ns, Some(k), false)
            .await
    }
    async fn list_namespace_resources_excluding_kind(
        &self,
        ns: &str,
        k: &str,
    ) -> Result<Vec<Resource>> {
        self.read_store
            .core()
            .list_namespace_resources(ns, Some(k), true)
            .await
    }
    async fn count_namespace_resources(&self, ns: &str) -> Result<i64> {
        self.read_store.core().count_namespace_resources(ns).await
    }
    async fn list_watch_events_since(
        &self,
        t: &[WatchTarget],
        s: i64,
    ) -> Result<Vec<CatchUpResource>> {
        let targets: Vec<_> = t.iter().map(legacy_target_to_durable).collect();
        self.read_store
            .core()
            .watch_events_since(&targets, s)
            .await
            .map(|events| events.into_iter().map(durable_to_catchup).collect())
    }
    async fn list_watch_events_since_checked(
        &self,
        t: &[WatchTarget],
        s: i64,
    ) -> Result<WatchReplayRead> {
        let targets: Vec<_> = t.iter().map(legacy_target_to_durable).collect();
        match self
            .read_store
            .core()
            .watch_events_since_checked(&targets, s, None)
            .await?
        {
            RedbCheckedWatchRead::Events(events) => Ok(WatchReplayRead::Events(
                events.into_iter().map(durable_to_catchup).collect(),
            )),
            RedbCheckedWatchRead::Expired => Ok(WatchReplayRead::Expired),
        }
    }
    async fn list_watch_events_since_checked_bounded(
        &self,
        t: &[WatchTarget],
        s: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<WatchReplayRead> {
        let targets: Vec<_> = t.iter().map(legacy_target_to_durable).collect();
        match self
            .read_store
            .core()
            .watch_events_since_checked(&targets, s, Some(limit))
            .await?
        {
            RedbCheckedWatchRead::Events(events) => Ok(WatchReplayRead::Events(
                events.into_iter().map(durable_to_catchup).collect(),
            )),
            RedbCheckedWatchRead::Expired => Ok(WatchReplayRead::Expired),
        }
    }
    async fn list_watch_events_after_position_checked_bounded(
        &self,
        targets: &[WatchTarget],
        position: WatchReplayPosition,
        limit: std::num::NonZeroUsize,
    ) -> Result<PositionedWatchReplayRead<CatchUpResource>> {
        let targets: Vec<_> = targets.iter().map(legacy_target_to_durable).collect();
        match self
            .read_store
            .core()
            .positioned_watch_events(&targets, position, limit)
            .await?
        {
            RedbPositionedWatchRead::Expired => Ok(PositionedWatchReplayRead::Expired),
            RedbPositionedWatchRead::Events(page) => {
                Ok(PositionedWatchReplayRead::Events(PositionedWatchReplay {
                    events: page
                        .events
                        .into_iter()
                        .map(|event| klights_cluster_core::PositionedWatchEvent {
                            position: event.position,
                            event: durable_to_catchup(event.event),
                        })
                        .collect(),
                    next_position: page.next_position,
                }))
            }
        }
    }
    async fn current_watch_replay_position(&self) -> Result<WatchReplayPosition> {
        self.read_store.core().allocator_position().await
    }
    async fn snapshot_resources_at_position(
        &self,
        targets: &[WatchTarget],
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        position: WatchReplayPosition,
    ) -> Result<SnapshotAtRv> {
        let targets: Vec<_> = targets.iter().map(legacy_target_to_durable).collect();
        match self
            .read_store
            .core()
            .snapshot_at_position(&targets, label_selector, field_selector, position)
            .await?
        {
            RedbSnapshotRead::Expired => Ok(SnapshotAtRv::Expired),
            RedbSnapshotRead::Historical { items, position } => {
                Ok(SnapshotAtRv::List(ResourceList {
                    items,
                    resource_version: position.resource_version,
                    watch_replay_position: Some(position),
                    continue_token: None,
                    remaining_item_count: None,
                }))
            }
        }
    }
    async fn list_raw_watch_events_since_checked_bounded(
        &self,
        t: &[WatchTarget],
        s: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<WatchReplayRead<klights_cluster_store::DurableRawWatchEvent>> {
        let targets: Vec<_> = t.iter().map(legacy_target_to_durable).collect();
        match self
            .read_store
            .core()
            .raw_watch_events_since_checked(&targets, s, limit)
            .await?
        {
            RedbCheckedWatchRead::Events(events) => Ok(WatchReplayRead::Events(events)),
            RedbCheckedWatchRead::Expired => Ok(WatchReplayRead::Expired),
        }
    }
    async fn list_raw_watch_events_after_position_checked_bounded(
        &self,
        targets: &[WatchTarget],
        position: WatchReplayPosition,
        limit: std::num::NonZeroUsize,
    ) -> Result<PositionedWatchReplayRead<klights_cluster_store::DurableRawWatchEvent>> {
        let targets: Vec<_> = targets.iter().map(legacy_target_to_durable).collect();
        match self
            .read_store
            .core()
            .positioned_raw_watch_events(&targets, position, limit)
            .await?
        {
            RedbPositionedWatchRead::Expired => Ok(PositionedWatchReplayRead::Expired),
            RedbPositionedWatchRead::Events(page) => {
                Ok(PositionedWatchReplayRead::Events(PositionedWatchReplay {
                    events: page.events,
                    next_position: page.next_position,
                }))
            }
        }
    }
    async fn list_all_watch_events_since(&self, s: i64) -> Result<Vec<CatchUpResource>> {
        self.read_store
            .core()
            .all_watch_events_since(s, false)
            .await
            .map(|events| events.into_iter().map(durable_to_catchup).collect())
    }
    async fn list_all_watch_events_since_paged(
        &self,
        s: i64,
        after_resource_version: i64,
        after_id: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<(i64, CatchUpResource)>> {
        let _ = after_resource_version;
        self.read_store
            .core()
            .all_watch_events_since_paged(s, after_id, None, limit)
            .await
            .map(|events| {
                events
                    .into_iter()
                    .map(|(id, event)| (id, durable_to_catchup(event)))
                    .collect()
            })
    }
    async fn list_all_watch_events_after_id_bounded(
        &self,
        after_id: i64,
        through_id: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<(i64, CatchUpResource)>> {
        self.read_store
            .core()
            .all_watch_events_since_paged(0, after_id, Some(through_id), limit)
            .await
            .map(|events| {
                events
                    .into_iter()
                    .map(|(id, event)| (id, durable_to_catchup(event)))
                    .collect()
            })
    }
    async fn list_watch_replay_floors(&self) -> Result<Vec<WatchReplayFloor>> {
        self.read_store
            .core()
            .replay_floors()
            .await
            .map(|floors| floors.into_iter().map(durable_floor_to_legacy).collect())
    }

    async fn list_watch_replay_floors_paged(
        &self,
        after: Option<&klights_cluster_store::SnapshotReplayFloorCursor>,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<WatchReplayFloor>> {
        self.read_store
            .core()
            .replay_floors_paged(after, limit)
            .await
            .map(|floors| floors.into_iter().map(durable_floor_to_legacy).collect())
    }
    async fn list_deleted_watch_events_since(&self, s: i64) -> Result<Vec<CatchUpResource>> {
        self.read_store
            .core()
            .all_watch_events_since(s, true)
            .await
            .map(|events| events.into_iter().map(durable_to_catchup).collect())
    }
    async fn allocate_node_subnet(
        &self,
        n: &str,
        c: &str,
        i: &str,
    ) -> Result<klights_cluster_store::StoredNodeSubnet> {
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
        metadata: klights_cluster_store::DataplanePeerMetadata,
    ) -> Result<()> {
        self.network.update_node_dataplane(metadata).await
    }
    async fn get_node_dataplane(
        &self,
        node_name: &str,
    ) -> Result<Option<klights_cluster_store::DataplanePeerMetadata>> {
        self.read_store.core().get_node_dataplane(node_name).await
    }
    async fn get_node_subnet(
        &self,
        n: &str,
    ) -> Result<Option<klights_cluster_store::StoredNodeSubnet>> {
        self.read_store.core().get_node_subnet(n).await
    }
    async fn list_peer_subnets(
        &self,
        request: klights_cluster_store::PeerTopologyRequest,
    ) -> Result<Vec<klights_cluster_store::StoredNodeSubnet>> {
        self.read_store.core().list_peer_subnets(request).await
    }
    async fn delete_node_subnet(&self, n: &str) -> Result<()> {
        self.network.delete_node_subnet(n).await
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
                    return Err(klights_cluster_datastore::errors::DatastoreError::conflict(
                        "UID precondition failed",
                    )
                    .into());
                }
            }
            if let Some(expected_rv) = preconditions.resource_version
                && resource.resource_version != expected_rv
            {
                return Err(klights_cluster_datastore::errors::DatastoreError::conflict(
                    "resourceVersion precondition failed",
                )
                .into());
            }
        }
        self.patch_resource_latest(a, k, ns, n, patch_kind, patch)
            .await
    }
    async fn watch_events_gc_prunable_count(&self, m: i64, b: i64) -> Result<usize> {
        self.watch_store.gc_watch_prunable_count(m, b).await
    }
    async fn gc_watch_events(&self, m: i64, b: i64) -> Result<usize> {
        self.watch_store.gc_watch(m, b).await
    }
    async fn applied_outbox_gc_prunable_count(&self, cutoff_ms: i64) -> Result<usize> {
        use klights_cluster_datastore::redb::tables::APPLIED_OUTBOX;
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

    async fn list_outbox_stream_watermarks(
        &self,
    ) -> Result<Vec<klights_cluster_core::OutboxStreamWatermark>> {
        self.accessor
            .call("redb_outbox_stream_watermarks_list_all", |db| {
                let read = db.begin_read()?;
                let table = read.open_table(tables::OUTBOX_STREAM_WATERMARKS)?;
                let mut rows = Vec::new();
                for entry in table.iter()? {
                    let (key, value) = entry?;
                    rows.push(decode_outbox_watermark_key(key.value(), value.value())?);
                }
                Ok(rows)
            })
            .await
    }

    async fn list_outbox_stream_watermarks_paged(
        &self,
        after: Option<&klights_cluster_store::SnapshotOutboxWatermarkCursor>,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<klights_cluster_core::OutboxStreamWatermark>> {
        if limit.get() > klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE {
            return Err(anyhow::anyhow!(
                "outbox-watermark page limit {} exceeds {}",
                limit,
                klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE
            ));
        }
        let after = after
            .map(|cursor| outbox_watermark_key(cursor.client_id(), cursor.stream_id()))
            .transpose()?;
        let limit = limit.get();
        self.accessor
            .call("redb_outbox_stream_watermarks_list_paged", move |db| {
                let read = db.begin_read()?;
                let table = read.open_table(tables::OUTBOX_STREAM_WATERMARKS)?;
                let mut rows = Vec::with_capacity(limit);
                if let Some(after) = after.as_ref() {
                    for entry in table.range(after.as_slice()..)? {
                        let (key, value) = entry?;
                        if key.value() <= after.as_slice() {
                            continue;
                        }
                        rows.push(decode_outbox_watermark_key(key.value(), value.value())?);
                        if rows.len() == limit {
                            break;
                        }
                    }
                } else {
                    for entry in table.iter()? {
                        let (key, value) = entry?;
                        rows.push(decode_outbox_watermark_key(key.value(), value.value())?);
                        if rows.len() == limit {
                            break;
                        }
                    }
                }
                Ok(rows)
            })
            .await
    }

    async fn get_klights_meta(&self, key: &str) -> anyhow::Result<Option<String>> {
        use klights_cluster_datastore::redb::tables::KLIGHTS_META;
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
        use klights_cluster_datastore::redb::tables::KLIGHTS_META;
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
        use klights_cluster_datastore::redb::tables::APPLIED_OUTBOX;
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
        use klights_cluster_datastore::redb::tables::APPLIED_OUTBOX;
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
        use klights_cluster_datastore::redb::tables::APPLIED_OUTBOX;
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

    async fn list_applied_outbox_paged(
        &self,
        after_key: Option<&str>,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<AppliedOutboxRecord>> {
        use klights_cluster_datastore::redb::tables::APPLIED_OUTBOX;
        if limit.get() > klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE {
            return Err(anyhow::anyhow!(
                "applied-outbox page limit {} exceeds {}",
                limit,
                klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE
            ));
        }
        let after_key = after_key.map(str::to_owned);
        let limit = limit.get();
        self.accessor
            .call("redb_list_applied_outbox_paged", move |db| {
                let read_txn = db
                    .begin_read()
                    .map_err(|error| anyhow::anyhow!("redb read: {error}"))?;
                let table = read_txn
                    .open_table(APPLIED_OUTBOX)
                    .map_err(|error| anyhow::anyhow!("redb open applied_outbox table: {error}"))?;
                let mut rows = Vec::with_capacity(limit);
                if let Some(after_key) = after_key.as_deref() {
                    for row in table
                        .range(after_key..)
                        .map_err(|error| anyhow::anyhow!("redb applied_outbox range: {error}"))?
                    {
                        let (key, value) = row
                            .map_err(|error| anyhow::anyhow!("redb applied_outbox row: {error}"))?;
                        if key.value() <= after_key {
                            continue;
                        }
                        rows.push(serde_json::from_slice(value.value())?);
                        if rows.len() == limit {
                            break;
                        }
                    }
                } else {
                    for row in table
                        .iter()
                        .map_err(|error| anyhow::anyhow!("redb applied_outbox iter: {error}"))?
                    {
                        let (_, value) = row
                            .map_err(|error| anyhow::anyhow!("redb applied_outbox row: {error}"))?;
                        rows.push(serde_json::from_slice(value.value())?);
                        if rows.len() == limit {
                            break;
                        }
                    }
                }
                Ok(rows)
            })
            .await
    }

    async fn apply_outbox_transactionally(
        &self,
        _idempotency_key: &str,
        _operation: &str,
        _command: klights_cluster_core::command::StorageCommand,
        _authoring_node: &str,
    ) -> std::result::Result<
        klights_cluster_core::OutboxApplyOutcome,
        klights_cluster_core::OutboxApplyError,
    > {
        Err(klights_cluster_core::OutboxApplyError::Retryable(
            "redb: apply_outbox_transactionally not implemented".to_string(),
        ))
    }

    async fn build_log_apply_commit_for_outbox(
        &self,
        _idempotency_key: &str,
        _operation: &str,
        _command: klights_cluster_core::command::StorageCommand,
        _authoring_node: &str,
    ) -> std::result::Result<
        klights_cluster_core::BuildOutboxOutcome,
        klights_cluster_core::OutboxApplyError,
    > {
        Err(klights_cluster_core::OutboxApplyError::Retryable(
            "redb: build_log_apply_commit_for_outbox not implemented".to_string(),
        ))
    }

    async fn gc_applied_outbox(&self, now_ms: i64, ttl_ms: i64) -> Result<usize> {
        use klights_cluster_datastore::redb::tables::APPLIED_OUTBOX;

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

#[async_trait]
impl crate::datastore::ResourceStore for RedbDatastore {
    async fn create_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
    ) -> Result<Resource> {
        crate::datastore::DatastoreBackend::create_resource(
            self,
            api_version,
            kind,
            namespace,
            name,
            data,
        )
        .await
    }

    async fn get_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<Option<Resource>> {
        crate::datastore::DatastoreBackend::get_resource(self, api_version, kind, namespace, name)
            .await
    }

    async fn delete_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<()> {
        crate::datastore::DatastoreBackend::delete_resource(
            self,
            api_version,
            kind,
            namespace,
            name,
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
        crate::datastore::DatastoreBackend::delete_resource_with_preconditions(
            self,
            api_version,
            kind,
            namespace,
            name,
            preconditions,
        )
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
        crate::datastore::DatastoreBackend::update_resource(
            self,
            api_version,
            kind,
            namespace,
            name,
            data,
            expected_rv,
        )
        .await
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
        crate::datastore::DatastoreBackend::update_resource_with_preconditions(
            self,
            api_version,
            kind,
            namespace,
            name,
            data,
            preconditions,
        )
        .await
    }
}

#[async_trait]
impl crate::datastore::CurrentResourceVersionStore for RedbDatastore {
    async fn get_current_resource_version(&self) -> Result<i64> {
        crate::datastore::DatastoreBackend::get_current_resource_version(self).await
    }
}

#[async_trait]
impl crate::datastore::ResourceListStore for RedbDatastore {
    async fn list_resources_page(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        page: ListPageRequest,
    ) -> Result<ResourceList> {
        crate::datastore::DatastoreBackend::list_resources_page(
            self,
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
        crate::datastore::DatastoreBackend::list_resource_keys_for_scope(
            self,
            api_version,
            kind,
            namespaced,
        )
        .await
    }
}

#[async_trait]
impl crate::datastore::NamespaceStore for RedbDatastore {
    async fn create_namespace(&self, name: &str, data: Value) -> Result<Resource> {
        crate::datastore::DatastoreBackend::create_namespace(self, name, data).await
    }

    async fn get_namespace(&self, name: &str) -> Result<Option<Resource>> {
        crate::datastore::DatastoreBackend::get_namespace(self, name).await
    }

    #[cfg(test)]
    async fn seed_namespace_for_test(&self, name: &str) {
        crate::datastore::DatastoreBackend::seed_namespace_for_test(self, name).await
    }

    async fn list_namespaces(
        &self,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
    ) -> Result<ResourceList> {
        crate::datastore::DatastoreBackend::list_namespaces(self, label_selector, field_selector)
            .await
    }

    async fn list_namespaces_page(
        &self,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        page: ListPageRequest,
    ) -> Result<ResourceList> {
        crate::datastore::DatastoreBackend::list_namespaces_page(
            self,
            label_selector,
            field_selector,
            page,
        )
        .await
    }

    async fn update_namespace(
        &self,
        name: &str,
        data: Value,
        expected_rv: i64,
    ) -> Result<Resource> {
        crate::datastore::DatastoreBackend::update_namespace(self, name, data, expected_rv).await
    }

    async fn delete_namespace(&self, name: &str) -> Result<()> {
        crate::datastore::DatastoreBackend::delete_namespace(self, name).await
    }

    async fn delete_namespace_observed_rv(&self, name: &str) -> Result<i64> {
        crate::datastore::DatastoreBackend::delete_namespace_observed_rv(self, name).await
    }

    async fn delete_namespace_contents(&self, name: &str) -> Result<()> {
        crate::datastore::DatastoreBackend::delete_namespace_contents(self, name).await
    }
}

#[async_trait]
impl crate::datastore::WatchHistoryStore for RedbDatastore {
    async fn list_cluster_resources_modified_since(
        &self,
        api_version: &str,
        kind: &str,
        since_rv: i64,
    ) -> Result<Vec<CatchUpResource>> {
        crate::datastore::DatastoreBackend::list_cluster_resources_modified_since(
            self,
            api_version,
            kind,
            since_rv,
        )
        .await
    }

    async fn list_resources_modified_since(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        since_rv: i64,
    ) -> Result<Vec<CatchUpResource>> {
        crate::datastore::DatastoreBackend::list_resources_modified_since(
            self,
            api_version,
            kind,
            namespace,
            since_rv,
        )
        .await
    }

    async fn list_all_watch_events_since(&self, since_rv: i64) -> Result<Vec<CatchUpResource>> {
        crate::datastore::DatastoreBackend::list_all_watch_events_since(self, since_rv).await
    }

    async fn list_all_watch_events_since_paged(
        &self,
        since_rv: i64,
        after_resource_version: i64,
        after_id: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<(i64, CatchUpResource)>> {
        crate::datastore::DatastoreBackend::list_all_watch_events_since_paged(
            self,
            since_rv,
            after_resource_version,
            after_id,
            limit,
        )
        .await
    }

    async fn list_watch_replay_floors(&self) -> Result<Vec<WatchReplayFloor>> {
        crate::datastore::DatastoreBackend::list_watch_replay_floors(self).await
    }

    async fn list_watch_replay_floors_paged(
        &self,
        after: Option<&klights_cluster_store::SnapshotReplayFloorCursor>,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<WatchReplayFloor>> {
        crate::datastore::DatastoreBackend::list_watch_replay_floors_paged(self, after, limit).await
    }

    async fn list_deleted_watch_events_since(&self, since_rv: i64) -> Result<Vec<CatchUpResource>> {
        crate::datastore::DatastoreBackend::list_deleted_watch_events_since(self, since_rv).await
    }

    async fn advance_resource_version_after(&self, min_rv: i64) -> Result<i64> {
        crate::datastore::DatastoreBackend::advance_resource_version_after(self, min_rv).await
    }

    async fn watch_events_gc_prunable_count(&self, max_rows: i64, batch_cap: i64) -> Result<usize> {
        crate::datastore::DatastoreBackend::watch_events_gc_prunable_count(
            self, max_rows, batch_cap,
        )
        .await
    }

    async fn gc_watch_events(&self, max_rows: i64, batch_cap: i64) -> Result<usize> {
        crate::datastore::DatastoreBackend::gc_watch_events(self, max_rows, batch_cap).await
    }
}

#[async_trait]
impl crate::datastore::NamespaceContentStore for RedbDatastore {
    async fn list_namespace_resources(&self, namespace: &str) -> Result<Vec<Resource>> {
        crate::datastore::DatastoreBackend::list_namespace_resources(self, namespace).await
    }

    async fn list_namespace_resources_of_kind(
        &self,
        namespace: &str,
        kind: &str,
    ) -> Result<Vec<Resource>> {
        crate::datastore::DatastoreBackend::list_namespace_resources_of_kind(self, namespace, kind)
            .await
    }

    async fn list_namespace_resources_excluding_kind(
        &self,
        namespace: &str,
        kind: &str,
    ) -> Result<Vec<Resource>> {
        crate::datastore::DatastoreBackend::list_namespace_resources_excluding_kind(
            self, namespace, kind,
        )
        .await
    }

    async fn count_namespace_resources(&self, namespace: &str) -> Result<i64> {
        crate::datastore::DatastoreBackend::count_namespace_resources(self, namespace).await
    }
}

#[async_trait]
impl crate::datastore::OwnershipStore for RedbDatastore {
    async fn find_owned_resources(
        &self,
        owner_uid: &str,
        namespace: Option<&str>,
    ) -> Result<Vec<Resource>> {
        crate::datastore::DatastoreBackend::find_owned_resources(self, owner_uid, namespace).await
    }

    async fn list_resources_by_owner_uid(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        owner_uid: &str,
    ) -> Result<Vec<Resource>> {
        crate::datastore::DatastoreBackend::list_resources_by_owner_uid(
            self,
            api_version,
            kind,
            namespace,
            owner_uid,
        )
        .await
    }

    async fn find_owned_by_name_kind_empty_uid(
        &self,
        owner_api_version: &str,
        owner_name: &str,
        owner_kind: &str,
        namespace: Option<&str>,
    ) -> Result<Vec<Resource>> {
        crate::datastore::DatastoreBackend::find_owned_by_name_kind_empty_uid(
            self,
            owner_api_version,
            owner_name,
            owner_kind,
            namespace,
        )
        .await
    }
}

#[async_trait]
impl crate::datastore::StatusStore for RedbDatastore {
    async fn update_status_only(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        status: Value,
        expected_rv: Option<i64>,
    ) -> Result<Resource> {
        crate::datastore::DatastoreBackend::update_status_only(
            self,
            api_version,
            kind,
            namespace,
            name,
            status,
            expected_rv,
        )
        .await
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
        crate::datastore::DatastoreBackend::update_status_only_with_preconditions(
            self,
            api_version,
            kind,
            namespace,
            name,
            status,
            preconditions,
        )
        .await
    }
}

#[async_trait]
impl crate::datastore::MetaStore for RedbDatastore {
    async fn get_klights_meta(&self, key: &str) -> Result<Option<String>> {
        crate::datastore::DatastoreBackend::get_klights_meta(self, key).await
    }

    async fn set_klights_meta(&self, key: &str, value: &str) -> Result<()> {
        crate::datastore::DatastoreBackend::set_klights_meta(self, key, value).await
    }
}

#[async_trait]
impl crate::datastore::NetworkMetadataStore for RedbDatastore {
    async fn allocate_node_subnet(
        &self,
        node_name: &str,
        cluster_cidr: &str,
        node_ip: &str,
    ) -> Result<klights_cluster_store::StoredNodeSubnet> {
        crate::datastore::DatastoreBackend::allocate_node_subnet(
            self,
            node_name,
            cluster_cidr,
            node_ip,
        )
        .await
    }

    async fn update_node_peer_attributes(
        &self,
        node_name: &str,
        mode: klights_types::NodePeerMode,
        hostport_range: Option<klights_types::HostPortRange>,
    ) -> Result<()> {
        crate::datastore::DatastoreBackend::update_node_peer_attributes(
            self,
            node_name,
            mode,
            hostport_range,
        )
        .await
    }

    async fn update_node_dataplane(
        &self,
        metadata: klights_cluster_store::DataplanePeerMetadata,
    ) -> Result<()> {
        crate::datastore::DatastoreBackend::update_node_dataplane(self, metadata).await
    }

    async fn get_node_dataplane(
        &self,
        node_name: &str,
    ) -> Result<Option<klights_cluster_store::DataplanePeerMetadata>> {
        crate::datastore::DatastoreBackend::get_node_dataplane(self, node_name).await
    }

    async fn get_node_subnet(
        &self,
        node_name: &str,
    ) -> Result<Option<klights_cluster_store::StoredNodeSubnet>> {
        crate::datastore::DatastoreBackend::get_node_subnet(self, node_name).await
    }

    async fn list_peer_subnets(
        &self,
        request: klights_cluster_store::PeerTopologyRequest,
    ) -> Result<Vec<klights_cluster_store::StoredNodeSubnet>> {
        crate::datastore::DatastoreBackend::list_peer_subnets(self, request).await
    }

    async fn delete_node_subnet(&self, node_name: &str) -> Result<()> {
        crate::datastore::DatastoreBackend::delete_node_subnet(self, node_name).await
    }
}

#[async_trait]
impl crate::datastore::ReplicationStore for RedbDatastore {
    #[cfg(test)]
    async fn apply_replicated_command(
        &self,
        command: klights_cluster_core::command::StorageCommand,
        meta: klights_cluster_core::command::CommandMeta,
    ) -> Result<()> {
        crate::datastore::DatastoreBackend::apply_replicated_command(self, command, meta).await
    }

    async fn replace_replicated_resource_state(
        &self,
        entries: Vec<klights_cluster_core::SnapshotRestoreOperation>,
        current_rv: i64,
        watch_event_high_water: Option<i64>,
        watch_replay_floors: Option<Vec<WatchReplayFloor>>,
        metadata: Option<ReplicatedSnapshotMetadata>,
    ) -> Result<()> {
        crate::datastore::DatastoreBackend::replace_replicated_resource_state(
            self,
            entries,
            current_rv,
            watch_event_high_water,
            watch_replay_floors,
            metadata,
        )
        .await
    }

    async fn apply_log_apply_commit(
        &self,
        commit: klights_cluster_core::LogApplyCommit,
    ) -> Result<()> {
        crate::datastore::DatastoreBackend::apply_log_apply_commit(self, commit).await
    }

    async fn apply_raft_log_apply_commit(
        &self,
        commit: klights_cluster_core::LogApplyCommit,
    ) -> Result<crate::datastore::raft::types::StorageCommandResult> {
        crate::datastore::DatastoreBackend::apply_raft_log_apply_commit(self, commit).await
    }

    async fn apply_raft_log_apply_commit_outcome(
        &self,
        commit: klights_cluster_core::LogApplyCommit,
    ) -> Result<klights_cluster_core::CommittedApplyOutcome> {
        crate::datastore::DatastoreBackend::apply_raft_log_apply_commit_outcome(self, commit).await
    }

    #[cfg(test)]
    async fn apply_replicated_create_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        options: crate::datastore::types::ReplicatedCreateOptions,
    ) -> Result<Resource> {
        crate::datastore::DatastoreBackend::apply_replicated_create_resource(
            self,
            api_version,
            kind,
            namespace,
            name,
            data,
            options,
        )
        .await
    }
}

#[async_trait]
impl crate::datastore::DurableRecoveryStore for RedbDatastore {
    async fn read_durable_allocator_observation(
        &self,
    ) -> Result<crate::datastore::DurableAllocatorObservation> {
        crate::datastore::DatastoreBackend::read_durable_allocator_observation(self).await
    }

    async fn read_cluster_metadata_observation(
        &self,
    ) -> Result<crate::datastore::ClusterMetadataObservation> {
        crate::datastore::DatastoreBackend::read_cluster_metadata_observation(self).await
    }

    async fn begin_pinned_snapshot_capture(
        &self,
        request: klights_cluster_store::SnapshotCaptureRequest,
    ) -> Result<Box<dyn klights_cluster_store::SnapshotCaptureSession>> {
        crate::datastore::DatastoreBackend::begin_pinned_snapshot_capture(self, request).await
    }

    async fn begin_pinned_snapshot_capture_with_anchor(
        &self,
        request: klights_cluster_store::SnapshotCaptureRequest,
        anchor: &dyn crate::datastore::backend::SnapshotCaptureAnchor,
    ) -> Result<Box<dyn klights_cluster_store::SnapshotCaptureSession>> {
        crate::datastore::DatastoreBackend::begin_pinned_snapshot_capture_with_anchor(
            self, request, anchor,
        )
        .await
    }
}

#[async_trait]
impl crate::datastore::BackendLifecycleStore for RedbDatastore {
    async fn acquire_snapshot_exclusive_fence(
        &self,
    ) -> Result<Option<crate::datastore::backend::SnapshotExclusiveFence>> {
        crate::datastore::DatastoreBackend::acquire_snapshot_exclusive_fence(self).await
    }

    async fn acquire_snapshot_mutation_fence(
        &self,
    ) -> Result<Option<crate::datastore::backend::SnapshotMutationFence>> {
        crate::datastore::DatastoreBackend::acquire_snapshot_mutation_fence(self).await
    }

    fn close(&self) {
        crate::datastore::DatastoreBackend::close(self);
    }
}

#[cfg(test)]
impl crate::datastore::TestWatchStore for RedbDatastore {
    fn subscribe_watch_many(&self, topics: Vec<WatchTopic>) -> crate::watch::WatchReceiver {
        crate::datastore::DatastoreBackend::subscribe_watch_many(self, topics)
    }

    fn broadcast_watch_event(&self, pending: PendingWatchEvent) {
        crate::datastore::DatastoreBackend::broadcast_watch_event(self, pending);
    }
}

#[async_trait]
impl crate::datastore::ClusterResourceQueryStore for RedbDatastore {
    async fn list_resources(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        query: ResourceListQuery<'_>,
    ) -> Result<ResourceList> {
        crate::datastore::DatastoreBackend::list_resources(
            self,
            api_version,
            kind,
            namespace,
            query,
        )
        .await
    }

    async fn list_resources_for_watch_targets(
        &self,
        targets: &[WatchTarget],
        label_selector: Option<&str>,
    ) -> Result<ResourceList> {
        crate::datastore::DatastoreBackend::list_resources_for_watch_targets(
            self,
            targets,
            label_selector,
        )
        .await
    }

    async fn list_cluster_resources(&self) -> Result<Vec<Resource>> {
        crate::datastore::DatastoreBackend::list_cluster_resources(self).await
    }
}

#[async_trait]
impl crate::datastore::LeaderResourceMutationStore for RedbDatastore {
    async fn update_main_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource> {
        crate::datastore::DatastoreBackend::update_main_resource_with_preconditions(
            self,
            api_version,
            kind,
            namespace,
            name,
            data,
            preconditions,
        )
        .await
    }

    async fn apply_resource_batch(&self, operations: Vec<ResourceBatchOperation>) -> Result<()> {
        crate::datastore::DatastoreBackend::apply_resource_batch(self, operations).await
    }

    async fn delete_resource_with_preconditions_observed_rv(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
    ) -> Result<i64> {
        crate::datastore::DatastoreBackend::delete_resource_with_preconditions_observed_rv(
            self,
            api_version,
            kind,
            namespace,
            name,
            preconditions,
        )
        .await
    }

    async fn mark_for_delete_without_watch(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
        grace_seconds: i64,
    ) -> Result<Option<Resource>> {
        crate::datastore::DatastoreBackend::mark_for_delete_without_watch(
            self,
            api_version,
            kind,
            namespace,
            name,
            preconditions,
            grace_seconds,
        )
        .await
    }

    async fn delete_resource_without_watch_with_tombstone(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
        grace_seconds: i64,
    ) -> Result<Resource> {
        crate::datastore::DatastoreBackend::delete_resource_without_watch_with_tombstone(
            self,
            api_version,
            kind,
            namespace,
            name,
            preconditions,
            grace_seconds,
        )
        .await
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
        crate::datastore::DatastoreBackend::patch_resource_latest(
            self,
            api_version,
            kind,
            namespace,
            name,
            patch_kind,
            patch,
        )
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
        crate::datastore::DatastoreBackend::patch_resource_latest_with_preconditions(
            self,
            api_version,
            kind,
            namespace,
            name,
            request,
        )
        .await
    }
}

#[async_trait]
impl crate::datastore::WatchMaintenanceStore for RedbDatastore {
    async fn list_raw_watch_events_since_checked_bounded(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<crate::datastore::WatchReplayRead<klights_cluster_store::DurableRawWatchEvent>>
    {
        crate::datastore::DatastoreBackend::list_raw_watch_events_since_checked_bounded(
            self, targets, since_rv, limit,
        )
        .await
    }

    async fn snapshot_resources_at_rv(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        query: ResourceListQuery<'_>,
        snapshot_rv: i64,
    ) -> Result<SnapshotAtRv> {
        crate::datastore::DatastoreBackend::snapshot_resources_at_rv(
            self,
            api_version,
            kind,
            namespace,
            query,
            snapshot_rv,
        )
        .await
    }

    async fn list_all_watch_events_after_id_bounded(
        &self,
        after_id: i64,
        through_id: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<(i64, CatchUpResource)>> {
        crate::datastore::DatastoreBackend::list_all_watch_events_after_id_bounded(
            self, after_id, through_id, limit,
        )
        .await
    }
}

#[async_trait]
impl crate::datastore::PodCleanupStore for RedbDatastore {
    async fn move_pod_to_cleanup_intent(
        &self,
        node_name: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        reason: &str,
    ) -> Result<()> {
        crate::datastore::DatastoreBackend::move_pod_to_cleanup_intent(
            self, node_name, namespace, pod_name, pod_uid, reason,
        )
        .await
    }

    async fn list_pod_cleanup_intents_for_node(
        &self,
        node_name: &str,
    ) -> Result<Vec<PodCleanupIntent>> {
        crate::datastore::DatastoreBackend::list_pod_cleanup_intents_for_node(self, node_name).await
    }

    async fn delete_pod_cleanup_intent(
        &self,
        node_name: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        reason: &str,
    ) -> Result<()> {
        crate::datastore::DatastoreBackend::delete_pod_cleanup_intent(
            self, node_name, namespace, pod_name, pod_uid, reason,
        )
        .await
    }

    async fn delete_pod_cleanup_intents_for_node(&self, node_name: &str) -> Result<()> {
        crate::datastore::DatastoreBackend::delete_pod_cleanup_intents_for_node(self, node_name)
            .await
    }
}

#[async_trait]
impl crate::datastore::AppliedOutboxStore for RedbDatastore {
    async fn applied_outbox_gc_prunable_count(&self, cutoff_ms: i64) -> Result<usize> {
        crate::datastore::DatastoreBackend::applied_outbox_gc_prunable_count(self, cutoff_ms).await
    }

    async fn list_outbox_stream_watermarks(
        &self,
    ) -> Result<Vec<klights_cluster_core::OutboxStreamWatermark>> {
        crate::datastore::DatastoreBackend::list_outbox_stream_watermarks(self).await
    }

    async fn list_outbox_stream_watermarks_paged(
        &self,
        after: Option<&klights_cluster_store::SnapshotOutboxWatermarkCursor>,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<klights_cluster_core::OutboxStreamWatermark>> {
        crate::datastore::DatastoreBackend::list_outbox_stream_watermarks_paged(self, after, limit)
            .await
    }

    async fn get_applied_outbox(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<AppliedOutboxRecord>> {
        crate::datastore::DatastoreBackend::get_applied_outbox(self, idempotency_key).await
    }

    async fn insert_applied_outbox(&self, record: AppliedOutboxRecord) -> Result<bool> {
        crate::datastore::DatastoreBackend::insert_applied_outbox(self, record).await
    }

    async fn list_applied_outbox(&self) -> Result<Vec<AppliedOutboxRecord>> {
        crate::datastore::DatastoreBackend::list_applied_outbox(self).await
    }

    async fn list_applied_outbox_paged(
        &self,
        after_key: Option<&str>,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<AppliedOutboxRecord>> {
        crate::datastore::DatastoreBackend::list_applied_outbox_paged(self, after_key, limit).await
    }

    async fn apply_outbox_transactionally(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: klights_cluster_core::command::StorageCommand,
        authoring_node: &str,
    ) -> std::result::Result<
        klights_cluster_core::OutboxApplyOutcome,
        klights_cluster_core::OutboxApplyError,
    > {
        crate::datastore::DatastoreBackend::apply_outbox_transactionally(
            self,
            idempotency_key,
            operation,
            command,
            authoring_node,
        )
        .await
    }

    async fn apply_outbox_transactionally_with_watermark(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: klights_cluster_core::command::StorageCommand,
        authoring_node: &str,
        watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
    ) -> std::result::Result<
        klights_cluster_core::OutboxApplyOutcome,
        klights_cluster_core::OutboxApplyError,
    > {
        crate::datastore::DatastoreBackend::apply_outbox_transactionally_with_watermark(
            self,
            idempotency_key,
            operation,
            command,
            authoring_node,
            watermark,
        )
        .await
    }

    async fn apply_outbox_transactionally_with_watermark_effect(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: klights_cluster_core::command::StorageCommand,
        authoring_node: &str,
        watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
    ) -> std::result::Result<
        crate::datastore::CommittedOutboxApply,
        klights_cluster_core::OutboxApplyError,
    > {
        crate::datastore::DatastoreBackend::apply_outbox_transactionally_with_watermark_effect(
            self,
            idempotency_key,
            operation,
            command,
            authoring_node,
            watermark,
        )
        .await
    }

    async fn build_log_apply_commit_for_command(
        &self,
        command: klights_cluster_core::command::StorageCommand,
        operation: &str,
        authoring_node: &str,
    ) -> Result<klights_cluster_core::LogApplyCommit> {
        crate::datastore::DatastoreBackend::build_log_apply_commit_for_command(
            self,
            command,
            operation,
            authoring_node,
        )
        .await
    }

    async fn build_log_apply_commit_for_outbox(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: klights_cluster_core::command::StorageCommand,
        authoring_node: &str,
    ) -> std::result::Result<
        klights_cluster_core::BuildOutboxOutcome,
        klights_cluster_core::OutboxApplyError,
    > {
        crate::datastore::DatastoreBackend::build_log_apply_commit_for_outbox(
            self,
            idempotency_key,
            operation,
            command,
            authoring_node,
        )
        .await
    }

    async fn build_log_apply_commit_for_outbox_with_watermark(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: klights_cluster_core::command::StorageCommand,
        authoring_node: &str,
        watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
    ) -> std::result::Result<
        klights_cluster_core::BuildOutboxOutcome,
        klights_cluster_core::OutboxApplyError,
    > {
        crate::datastore::DatastoreBackend::build_log_apply_commit_for_outbox_with_watermark(
            self,
            idempotency_key,
            operation,
            command,
            authoring_node,
            watermark,
        )
        .await
    }

    async fn gc_applied_outbox(&self, now_ms: i64, ttl_ms: i64) -> Result<usize> {
        crate::datastore::DatastoreBackend::gc_applied_outbox(self, now_ms, ttl_ms).await
    }
}

#[cfg(test)]
mod snapshot_paging_tests {
    use super::*;

    #[tokio::test]
    async fn watermark_keyset_pages_are_bounded_complete_and_exclusive() {
        let store = RedbDatastore::new_in_memory().await.unwrap();
        let db = store.accessor.db().unwrap();
        let write = db.begin_write().unwrap();
        {
            let mut watermarks = write.open_table(tables::OUTBOX_STREAM_WATERMARKS).unwrap();
            for index in 0..=klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE {
                let client_id = format!("worker-{index:04}");
                let key = outbox_watermark_key(&client_id, 1).unwrap();
                watermarks.insert(key.as_slice(), index as i64 + 1).unwrap();
            }
        }
        write.commit().unwrap();

        let page_limit =
            std::num::NonZeroUsize::new(klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE).unwrap();
        let mut after = None;
        let mut delivered = Vec::new();
        let mut page_lengths = Vec::new();
        loop {
            let page = store
                .list_outbox_stream_watermarks_paged(after.as_ref(), page_limit)
                .await
                .unwrap();
            if page.is_empty() {
                break;
            }
            page_lengths.push(page.len());
            delivered.extend(page.iter().map(|row| row.client_id.clone()));
            after = Some(
                klights_cluster_store::SnapshotOutboxWatermarkCursor::from_watermark(
                    page.last().unwrap(),
                )
                .unwrap(),
            );
        }
        assert_eq!(
            page_lengths,
            vec![klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE, 1]
        );
        assert_eq!(
            delivered.len(),
            klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE + 1
        );
        assert!(delivered.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(
            store.list_outbox_stream_watermarks().await.unwrap().len(),
            delivered.len()
        );
    }
}
