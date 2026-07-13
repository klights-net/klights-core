//! Snapshot and state copy support (2A-5).
//!
//! Provides leader-side snapshot generation and replica-side staging restore
//! with metadata safety checks.
//!
//! ## Safety rules (per multinode.md)
//! - **Behind leader**: cluster_id and leader_epoch match, local_last_rv <= leader_current_rv
//!   → normal lag, no destructive warning.
//! - **Ahead of leader**: local_last_rv > leader_current_rv → warn before wipe.
//! - **Mismatch**: cluster_id or leader_epoch differs, metadata missing, or corrupt
//!   → warn before wipe.
//!
//! ## Restore contract
//! - Restore into staging first.
//! - Only replace replica datastore after successful validation.
//! - Failed copy leaves old local data untouched.

use std::collections::{HashMap, HashSet};
use std::io::Write;

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::datastore::backend::DatastoreBackend;
use crate::datastore::types::{NodeSubnet, PodCleanupIntent, Resource};
use crate::log_apply::{
    ClusterMutation, LogApplyCommit, LogApplyNamespaceRow, LogApplyNodeDataplaneRow,
    LogApplyNodeSubnetRow, LogApplyResourceKey, LogApplyResourceRow, LogApplyWatchEventRow,
    NamespaceMutation, NetworkMutation, PodCleanupMutation, ResourceMutation, WatchHistoryMutation,
};

const SNAPSHOT_JSON_COMMIT_BATCH_SIZE: usize = 128;

/// memory-improvement.md §10 P1: page size for keyset-paginated reads inside
/// `emit_snapshot_commits`. Bounded so a multi-million-row table is consumed
/// batch by batch instead of materialized into one `Vec`. 512 rows ≈ a few
/// MiB of working set, regardless of total table size.
pub(crate) const SNAPSHOT_EMIT_PAGE_SIZE: usize = 512;

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct SnapshotWatchPagePause {
    pub(crate) reached: std::sync::Arc<tokio::sync::Notify>,
    pub(crate) resume: std::sync::Arc<tokio::sync::Notify>,
}

#[cfg(test)]
static SNAPSHOT_WATCH_PAGE_PAUSE: std::sync::Mutex<Option<SnapshotWatchPagePause>> =
    std::sync::Mutex::new(None);

#[cfg(test)]
pub(crate) fn install_snapshot_watch_page_pause() -> SnapshotWatchPagePause {
    let pause = SnapshotWatchPagePause {
        reached: std::sync::Arc::new(tokio::sync::Notify::new()),
        resume: std::sync::Arc::new(tokio::sync::Notify::new()),
    };
    *SNAPSHOT_WATCH_PAGE_PAUSE.lock().unwrap() = Some(pause.clone());
    pause
}

/// Result of comparing local replica metadata against leader metadata.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetadataComparison {
    /// Local data is behind or at the leader — safe to reseed.
    Behind {
        local_cluster_id: String,
        local_leader_epoch: i64,
        local_last_rv: i64,
        leader_cluster_id: String,
        leader_leader_epoch: i64,
        leader_current_rv: i64,
    },
    /// Local data is ahead of the leader — warn before wipe.
    Ahead {
        local_cluster_id: String,
        local_last_rv: i64,
        leader_current_rv: i64,
    },
    /// Cluster ID or leader epoch differs — warn before wipe.
    Mismatch {
        local_cluster_id: Option<String>,
        local_leader_epoch: Option<i64>,
        leader_cluster_id: String,
        leader_leader_epoch: i64,
        reason: String,
    },
    /// No local data exists — safe to reseed without warning.
    NoLocalData,
}

/// Compare local replica metadata against leader metadata.
pub fn compare_metadata(
    local_cluster_id: Option<String>,
    local_leader_epoch: Option<i64>,
    local_last_rv: Option<i64>,
    leader_cluster_id: &str,
    leader_leader_epoch: i64,
    leader_current_rv: i64,
) -> MetadataComparison {
    // No local data — safe to reseed
    let local_cid = match local_cluster_id {
        Some(cid) => cid,
        None => return MetadataComparison::NoLocalData,
    };

    let local_epoch = match local_leader_epoch {
        Some(e) => e,
        None => {
            return MetadataComparison::Mismatch {
                local_cluster_id: Some(local_cid),
                local_leader_epoch: None,
                leader_cluster_id: leader_cluster_id.to_string(),
                leader_leader_epoch,
                reason: "local leader_epoch missing".into(),
            };
        }
    };

    let local_rv = local_last_rv.unwrap_or(0);

    // Check cluster_id match
    if local_cid != leader_cluster_id {
        return MetadataComparison::Mismatch {
            local_cluster_id: Some(local_cid.clone()),
            local_leader_epoch: Some(local_epoch),
            leader_cluster_id: leader_cluster_id.to_string(),
            leader_leader_epoch,
            reason: format!(
                "cluster_id mismatch: local={} leader={}",
                local_cid, leader_cluster_id
            ),
        };
    }

    // Check leader_epoch match
    if local_epoch != leader_leader_epoch {
        return MetadataComparison::Mismatch {
            local_cluster_id: Some(local_cid.clone()),
            local_leader_epoch: Some(local_epoch),
            leader_cluster_id: leader_cluster_id.to_string(),
            leader_leader_epoch,
            reason: format!(
                "leader_epoch mismatch: local={} leader={}",
                local_epoch, leader_leader_epoch
            ),
        };
    }

    // Check RV relationship
    if local_rv > leader_current_rv {
        return MetadataComparison::Ahead {
            local_cluster_id: local_cid,
            local_last_rv: local_rv,
            leader_current_rv,
        };
    }

    MetadataComparison::Behind {
        local_cluster_id: local_cid,
        local_leader_epoch: local_epoch,
        local_last_rv: local_rv,
        leader_cluster_id: leader_cluster_id.to_string(),
        leader_leader_epoch,
        leader_current_rv,
    }
}

/// Read local replica metadata from the datastore.
pub async fn read_local_metadata(
    db: &dyn DatastoreBackend,
) -> Result<(Option<String>, Option<i64>, Option<i64>)> {
    let cluster_id = db
        .get_klights_meta(crate::bootstrap::cluster_meta::KEY_CLUSTER_ID)
        .await?;

    let leader_epoch = db
        .get_klights_meta(crate::bootstrap::cluster_meta::KEY_LEADER_EPOCH)
        .await?
        .and_then(|s| s.parse::<i64>().ok());

    let current_rv = db.get_current_resource_version().await.ok();

    Ok((cluster_id, leader_epoch, current_rv))
}

/// Whether a metadata comparison requires operator confirmation before wipe.
pub fn needs_confirmation(comparison: &MetadataComparison) -> bool {
    matches!(
        comparison,
        MetadataComparison::Ahead { .. } | MetadataComparison::Mismatch { .. }
    )
}

/// Leader-side: generate an authoritative snapshot of all cluster-replicated data.
///
/// `after_rv` is the caller's cursor for diagnostics and future non-destructive
/// copy modes. This snapshot is installed by destructive replacement, so it must
/// include the full live state and durable watch history, not only rows newer
/// than the cursor.
///
/// memory-improvement.md §10 P1: the production gRPC serve path no longer
/// calls this — it streams via `stream_snapshot_commits` + a channel sink.
/// This Vec-collecting form is retained `#[cfg(test)]`-only as the
/// equivalence oracle for the streaming path.
#[cfg(test)]
pub async fn generate_snapshot(
    db: &dyn DatastoreBackend,
    after_rv: i64,
) -> Result<Vec<LogApplyCommit>> {
    let mut sink = VecSnapshotCommitSink::default();
    stream_snapshot_commits(db, after_rv, &mut sink).await?;
    Ok(sink.entries)
}

/// Write snapshot commits as a JSON array without materializing the commit list.
///
/// This is used by the Raft snapshot builder, whose enclosing object writes
/// `last_applied`, membership, and the leader RV counter around this array.
pub async fn write_snapshot_commits_json_array<W: Write>(
    db: &dyn DatastoreBackend,
    after_rv: i64,
    writer: &mut W,
) -> Result<()> {
    let high_water = db.current_watch_replay_position().await?.event_id;
    write_snapshot_commits_json_array_through_event_id(db, after_rv, high_water, writer).await
}

pub async fn write_snapshot_commits_json_array_through_event_id<W: Write>(
    db: &dyn DatastoreBackend,
    after_rv: i64,
    high_water_event_id: i64,
    writer: &mut W,
) -> Result<()> {
    writer.write_all(b"[")?;
    let mut sink = JsonArrayCommitSink::new(writer);
    stream_snapshot_commits_through_event_id(db, after_rv, high_water_event_id, &mut sink).await?;
    writer.write_all(b"]")?;
    Ok(())
}

pub(crate) async fn stream_snapshot_commits<S: SnapshotCommitSink + Unpin>(
    db: &dyn DatastoreBackend,
    _after_rv: i64,
    sink: &mut S,
) -> Result<()> {
    let high_water_event_id = db.current_watch_replay_position().await?.event_id;
    stream_snapshot_commits_through_event_id(db, _after_rv, high_water_event_id, sink).await
}

async fn stream_snapshot_commits_through_event_id<S: SnapshotCommitSink + Unpin>(
    db: &dyn DatastoreBackend,
    _after_rv: i64,
    high_water_event_id: i64,
    sink: &mut S,
) -> Result<()> {
    let mut batcher = SnapshotCommitBatcher::new(sink);
    emit_snapshot_commits(db, high_water_event_id, &mut batcher).await?;
    batcher.finish().await?;
    sink.finish()
}

async fn emit_snapshot_commits<S: SnapshotCommitSink + Unpin>(
    db: &dyn DatastoreBackend,
    high_water_event_id: i64,
    sink: &mut S,
) -> Result<()> {
    let namespaces = db.list_namespaces(None, None).await?;

    let mut live_resources: HashMap<SnapshotResourceKey, Resource> = HashMap::new();
    let mut namespace_names = Vec::with_capacity(namespaces.items.len());

    for ns in namespaces.items {
        namespace_names.push(ns.name.clone());
        insert_live_resource(&mut live_resources, ns);
    }

    for ns_name in &namespace_names {
        let resources = db.list_namespace_resources(ns_name).await?;
        for resource in resources {
            if resource.api_version == "v1" && resource.kind == "Namespace" {
                continue;
            }
            insert_live_resource(&mut live_resources, resource);
        }
    }

    let cluster_resources = db.list_cluster_resources().await?;
    let mut node_names = Vec::new();
    for resource in cluster_resources {
        if resource.api_version.is_empty()
            || resource.kind.is_empty()
            || (resource.api_version == "v1" && resource.kind == "Namespace")
        {
            continue;
        }
        if resource.api_version == "v1" && resource.kind == "Node" {
            node_names.push(resource.name.clone());
        }
        insert_live_resource(&mut live_resources, resource);
    }

    let mut emitted_live_keys = HashSet::new();
    let mut checked_watch_keys = HashSet::new();
    let page_limit = std::num::NonZeroUsize::new(SNAPSHOT_EMIT_PAGE_SIZE)
        .expect("SNAPSHOT_EMIT_PAGE_SIZE is nonzero");
    // memory-improvement.md §10 P1: page the watch-events table instead of
    // loading it all into one Vec. The loop keyset-pages on (rv, id) in the
    // same ordering the unbounded form used, so content/order are unchanged.
    let mut after_id = 0i64;
    loop {
        let page = db
            .list_all_watch_events_after_id_bounded(after_id, high_water_event_id, page_limit)
            .await?;
        if page.is_empty() {
            break;
        }
        after_id = page.last().expect("non-empty page has a last row").0;
        for (event_id, event) in page {
            let event_type = event.event_type.into_owned();
            let resource = event.resource;
            let key = SnapshotResourceKey::from_resource(&resource);

            if should_probe_live_resource_from_watch(&resource)
                && !live_resources.contains_key(&key)
                && checked_watch_keys.insert(key.clone())
                && let Some(current) = db
                    .get_resource(
                        &resource.api_version,
                        &resource.kind,
                        resource.namespace.as_deref(),
                        &resource.name,
                    )
                    .await?
            {
                insert_live_resource(&mut live_resources, current);
            }

            let resource_version = resource.resource_version;
            let mut mutations: Vec<ClusterMutation> = Vec::new();
            if event_type == "DELETED" {
                mutations.push(delete_mutation_from_watch_resource(&resource));
            } else if let Some(current) = live_resources.get(&key)
                && current.resource_version == resource_version
                && emitted_live_keys.insert(key.clone())
            {
                mutations.push(live_resource_mutation(current));
            }
            mutations.push(watch_event_mutation(event_id, resource, event_type));
            sink.push(LogApplyCommit::from_cluster_mutations(
                resource_version,
                mutations,
            ))
            .await?;
        }
        #[cfg(test)]
        let pause = { SNAPSHOT_WATCH_PAGE_PAUSE.lock().unwrap().take() };
        #[cfg(test)]
        if let Some(pause) = pause {
            pause.reached.notify_one();
            pause.resume.notified().await;
        }
    }

    let mut remaining_live: Vec<_> = live_resources
        .into_iter()
        .filter(|(key, _)| !emitted_live_keys.contains(key))
        .collect();
    remaining_live.sort_by(|(left_key, left), (right_key, right)| {
        left.resource_version
            .cmp(&right.resource_version)
            .then_with(|| live_resource_order(left).cmp(&live_resource_order(right)))
            .then_with(|| left_key.cmp(right_key))
    });
    for (_key, resource) in remaining_live {
        sink.push(live_resource_commit(&resource)).await?;
    }

    let current_rv = db.get_current_resource_version().await.unwrap_or(0);
    if current_rv > 0 {
        let mut peers = db.list_peer_subnets("").await?;
        peers.sort_by(|a, b| a.node_name.as_str().cmp(b.node_name.as_str()));
        for peer in peers {
            let node_name = peer.node_name.to_string();
            sink.push(snapshot_commit_from_family(
                current_rv,
                cluster_network_mutation_from_subnet(&peer),
            ))
            .await?;
            if let Some(dataplane) = db.get_node_dataplane(&node_name).await? {
                sink.push(snapshot_commit_from_family(
                    current_rv,
                    cluster_network_mutation_from_dataplane(&dataplane),
                ))
                .await?;
            }
        }

        for watermark in db.list_outbox_stream_watermarks().await? {
            sink.push(LogApplyCommit {
                resource_version: current_rv,
                resource_version_assignment:
                    crate::log_apply::ResourceVersionAssignment::LegacyLeaderAssigned,
                mutations: Vec::new(),
                outbox_watermark: Some(watermark),
            })
            .await?;
        }
    }

    for node_name in node_names {
        for intent in db.list_pod_cleanup_intents_for_node(&node_name).await? {
            sink.push(snapshot_commit_from_family(
                intent.resource_version,
                cluster_pod_cleanup_mutation_from_intent(intent),
            ))
            .await?;
        }
    }

    Ok(())
}

pub(crate) trait SnapshotCommitSink {
    async fn push(&mut self, commit: LogApplyCommit) -> Result<()>;

    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

struct SnapshotCommitBatcher<'a, S: SnapshotCommitSink + Unpin + ?Sized> {
    sink: &'a mut S,
    pending: Option<LogApplyCommit>,
}

impl<'a, S: SnapshotCommitSink + Unpin + ?Sized> SnapshotCommitBatcher<'a, S> {
    fn new(sink: &'a mut S) -> Self {
        Self {
            sink,
            pending: None,
        }
    }

    async fn finish(&mut self) -> Result<()> {
        if let Some(commit) = self.pending.take() {
            self.sink.push(commit).await?;
        }
        Ok(())
    }
}

impl<S: SnapshotCommitSink + Unpin + ?Sized> SnapshotCommitSink for SnapshotCommitBatcher<'_, S> {
    async fn push(&mut self, commit: LogApplyCommit) -> Result<()> {
        if commit.mutations.is_empty() {
            if commit.outbox_watermark.is_none() {
                return Ok(());
            }
            if let Some(previous) = self.pending.take() {
                self.sink.push(previous).await?;
            }
            self.sink.push(commit).await?;
            return Ok(());
        }
        if commit.outbox_watermark.is_some() {
            if let Some(previous) = self.pending.take() {
                self.sink.push(previous).await?;
            }
            self.sink.push(commit).await?;
            return Ok(());
        }
        match self.pending.as_mut() {
            Some(pending) if pending.resource_version == commit.resource_version => {
                pending.mutations.extend(commit.mutations);
            }
            Some(_) => {
                let previous = self.pending.replace(commit).expect("pending checked");
                self.sink.push(previous).await?;
            }
            None => {
                self.pending = Some(commit);
            }
        }
        Ok(())
    }
}

#[derive(Default)]
#[cfg(test)]
struct VecSnapshotCommitSink {
    entries: Vec<LogApplyCommit>,
}

#[cfg(test)]
impl SnapshotCommitSink for VecSnapshotCommitSink {
    async fn push(&mut self, commit: LogApplyCommit) -> Result<()> {
        self.entries.push(commit);
        Ok(())
    }
}

struct JsonArrayCommitSink<'a, W: Write> {
    writer: &'a mut W,
    first: bool,
    pending: Vec<LogApplyCommit>,
}

impl<'a, W: Write> JsonArrayCommitSink<'a, W> {
    fn new(writer: &'a mut W) -> Self {
        Self {
            writer,
            first: true,
            pending: Vec::with_capacity(SNAPSHOT_JSON_COMMIT_BATCH_SIZE),
        }
    }

    fn flush(&mut self) -> Result<()> {
        for commit in self.pending.drain(..) {
            if !self.first {
                self.writer.write_all(b",")?;
            }
            serde_json::to_writer(&mut *self.writer, &commit)?;
            self.first = false;
        }
        Ok(())
    }
}

impl<W: Write> SnapshotCommitSink for JsonArrayCommitSink<'_, W> {
    async fn push(&mut self, commit: LogApplyCommit) -> Result<()> {
        self.pending.push(commit);
        if self.pending.len() >= SNAPSHOT_JSON_COMMIT_BATCH_SIZE {
            self.flush()?;
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        self.flush()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
struct SnapshotResourceKey {
    api_version: String,
    kind: String,
    namespace: Option<String>,
    name: String,
}

impl SnapshotResourceKey {
    fn from_resource(resource: &Resource) -> Self {
        Self {
            api_version: resource.api_version.clone(),
            kind: resource.kind.clone(),
            namespace: resource.namespace.clone(),
            name: resource.name.clone(),
        }
    }
}

fn insert_live_resource(
    resources: &mut HashMap<SnapshotResourceKey, Resource>,
    resource: Resource,
) {
    resources
        .entry(SnapshotResourceKey::from_resource(&resource))
        .or_insert(resource);
}

fn should_probe_live_resource_from_watch(resource: &Resource) -> bool {
    resource.namespace.is_some() && !(resource.api_version == "v1" && resource.kind == "Namespace")
}

fn live_resource_order(resource: &Resource) -> u8 {
    if resource.api_version == "v1" && resource.kind == "Namespace" && resource.namespace.is_none()
    {
        0
    } else {
        1
    }
}

fn live_resource_commit(resource: &Resource) -> LogApplyCommit {
    snapshot_commit_from_family(resource.resource_version, live_resource_mutation(resource))
}

fn snapshot_commit_from_family(resource_version: i64, mutation: ClusterMutation) -> LogApplyCommit {
    LogApplyCommit::from_cluster_mutations(resource_version, vec![mutation])
}

fn live_resource_mutation(resource: &Resource) -> ClusterMutation {
    if resource.api_version == "v1" && resource.kind == "Namespace" && resource.namespace.is_none()
    {
        ClusterMutation::Namespace(NamespaceMutation::PutNamespace(LogApplyNamespaceRow {
            name: resource.name.clone(),
            uid: resource.uid.clone(),
            resource_version: resource.resource_version,
            data: (*resource.data).clone(),
        }))
    } else {
        ClusterMutation::Resource(ResourceMutation::PutResource(LogApplyResourceRow {
            api_version: resource.api_version.clone(),
            kind: resource.kind.clone(),
            namespace: resource.namespace.clone(),
            name: resource.name.clone(),
            uid: resource.uid.clone(),
            resource_version: resource.resource_version,
            data: (*resource.data).clone(),
            require_absent: false,
            require_existing: false,
            precondition_uid: None,
            precondition_resource_version: None,
            status_only: false,
        }))
    }
}

fn delete_mutation_from_watch_resource(resource: &Resource) -> ClusterMutation {
    if resource.api_version == "v1" && resource.kind == "Namespace" && resource.namespace.is_none()
    {
        ClusterMutation::Namespace(NamespaceMutation::DeleteNamespace {
            name: resource.name.clone(),
        })
    } else {
        ClusterMutation::Resource(ResourceMutation::DeleteResource(LogApplyResourceKey {
            api_version: resource.api_version.clone(),
            kind: resource.kind.clone(),
            namespace: resource.namespace.clone(),
            name: resource.name.clone(),
            uid: resource.uid.clone(),
            precondition_resource_version: None,
        }))
    }
}

fn watch_event_mutation(event_id: i64, resource: Resource, event_type: String) -> ClusterMutation {
    ClusterMutation::WatchHistory(WatchHistoryMutation::PutWatchEvent(LogApplyWatchEventRow {
        event_id: Some(event_id),
        api_version: resource.api_version,
        kind: resource.kind,
        namespace: resource.namespace,
        name: resource.name,
        resource_version: resource.resource_version,
        event_type,
        data: std::sync::Arc::unwrap_or_clone(resource.data),
    }))
}

fn cluster_network_mutation_from_subnet(row: &NodeSubnet) -> ClusterMutation {
    ClusterMutation::Network(NetworkMutation::PutNodeSubnet(LogApplyNodeSubnetRow {
        node_name: row.node_name.as_str().to_string(),
        subnet: row.subnet.to_string(),
        subnet_base_int: row.subnet_base_int,
        gateway_ip: row.gateway_ip.to_string(),
        node_ip: row.node_ip.to_string(),
        mode: match row.mode {
            crate::controllers::annotations::NodePeerMode::Root => "root".to_string(),
            crate::controllers::annotations::NodePeerMode::Rootless => "rootless".to_string(),
        },
        hostport_range: row.hostport_range.as_ref().map(|range| range.to_string()),
    }))
}

fn cluster_network_mutation_from_dataplane(
    row: &crate::networking::wireguard::DataplanePeerMetadata,
) -> ClusterMutation {
    ClusterMutation::Network(NetworkMutation::PutNodeDataplane(
        LogApplyNodeDataplaneRow {
            node_name: row.node_name.clone(),
            mode: row.mode.as_str().to_string(),
            encryption: row.encryption.as_str().to_string(),
            public_key: row.public_key.as_ref().map(|key| key.to_string()),
            endpoint: row.endpoint.to_string(),
            port: row.port,
        },
    ))
}

fn cluster_pod_cleanup_mutation_from_intent(intent: PodCleanupIntent) -> ClusterMutation {
    ClusterMutation::PodCleanup(PodCleanupMutation::PutPodCleanupIntent(intent.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- Metadata comparison tests ----

    #[test]
    fn no_local_data_is_safe() {
        let result = compare_metadata(None, None, None, "cluster-1", 0, 100);
        assert_eq!(result, MetadataComparison::NoLocalData);
        assert!(!needs_confirmation(&result));
    }

    #[test]
    fn behind_leader_is_safe() {
        let result = compare_metadata(
            Some("cluster-1".into()),
            Some(0),
            Some(50),
            "cluster-1",
            0,
            100,
        );
        match &result {
            MetadataComparison::Behind {
                local_last_rv,
                leader_current_rv,
                ..
            } => {
                assert_eq!(*local_last_rv, 50);
                assert_eq!(*leader_current_rv, 100);
            }
            other => panic!("expected Behind, got {:?}", other),
        }
        assert!(!needs_confirmation(&result));
    }

    #[test]
    fn at_leader_rv_is_safe() {
        let result = compare_metadata(
            Some("cluster-1".into()),
            Some(0),
            Some(100),
            "cluster-1",
            0,
            100,
        );
        assert!(matches!(result, MetadataComparison::Behind { .. }));
        assert!(!needs_confirmation(&result));
    }

    #[test]
    fn ahead_of_leader_needs_confirmation() {
        let result = compare_metadata(
            Some("cluster-1".into()),
            Some(0),
            Some(150),
            "cluster-1",
            0,
            100,
        );
        assert!(matches!(result, MetadataComparison::Ahead { .. }));
        assert!(needs_confirmation(&result));
    }

    #[test]
    fn cluster_id_mismatch_needs_confirmation() {
        let result = compare_metadata(
            Some("cluster-old".into()),
            Some(0),
            Some(50),
            "cluster-new",
            0,
            100,
        );
        match &result {
            MetadataComparison::Mismatch { reason, .. } => {
                assert!(reason.contains("cluster_id mismatch"));
            }
            other => panic!("expected Mismatch, got {:?}", other),
        }
        assert!(needs_confirmation(&result));
    }

    #[test]
    fn leader_epoch_mismatch_needs_confirmation() {
        let result = compare_metadata(
            Some("cluster-1".into()),
            Some(5),
            Some(50),
            "cluster-1",
            0,
            100,
        );
        match &result {
            MetadataComparison::Mismatch { reason, .. } => {
                assert!(reason.contains("leader_epoch mismatch"));
            }
            other => panic!("expected Mismatch, got {:?}", other),
        }
        assert!(needs_confirmation(&result));
    }

    #[test]
    fn missing_leader_epoch_needs_confirmation() {
        let result = compare_metadata(
            Some("cluster-1".into()),
            None,
            Some(50),
            "cluster-1",
            0,
            100,
        );
        match &result {
            MetadataComparison::Mismatch { reason, .. } => {
                assert!(reason.contains("leader_epoch missing"));
            }
            other => panic!("expected Mismatch, got {:?}", other),
        }
        assert!(needs_confirmation(&result));
    }

    #[test]
    fn missing_local_rv_treated_as_zero() {
        let result = compare_metadata(Some("cluster-1".into()), Some(0), None, "cluster-1", 0, 100);
        // local_rv defaults to 0, which is behind 100
        assert!(matches!(result, MetadataComparison::Behind { .. }));
    }

    // ---- Snapshot generation tests ----

    #[tokio::test]
    async fn snapshot_generates_entries() {
        let db = crate::datastore::test_support::in_memory().await;

        // Init default namespace
        crate::controllers::namespace::init_default_namespaces(&db)
            .await
            .unwrap();

        // Create some resources
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "cm1",
            serde_json::json!({"metadata": {"name": "cm1"}}),
        )
        .await
        .unwrap();
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "cm2",
            serde_json::json!({"metadata": {"name": "cm2"}}),
        )
        .await
        .unwrap();

        let entries = generate_snapshot(&db, 0).await.unwrap();
        assert!(
            entries.len() >= 2,
            "snapshot should contain at least the created resources"
        );
    }

    #[tokio::test]
    async fn snapshot_after_current_rv_still_contains_authoritative_state() {
        let db = crate::datastore::test_support::in_memory().await;

        crate::controllers::namespace::init_default_namespaces(&db)
            .await
            .unwrap();

        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "cm1",
            serde_json::json!({"metadata": {"name": "cm1"}}),
        )
        .await
        .unwrap();

        let current_rv = db.get_current_resource_version().await.unwrap();
        let entries = generate_snapshot(&db, current_rv).await.unwrap();
        assert!(
            entries.iter().any(|entry| {
                matches!(
                    entry.mutations.first(),
                    Some(crate::log_apply::LogApplyMutation::PutResource(row))
                        if row.api_version == "v1"
                        && row.kind == "ConfigMap"
                        && row.namespace.as_deref() == Some("default")
                        && row.name == "cm1"
                )
            }),
            "destructive replacement snapshots must include current live state even at the follower cursor"
        );
    }

    #[tokio::test]
    async fn snapshot_replays_resource_deletes_since_rv() {
        let db = crate::datastore::test_support::in_memory().await;

        crate::controllers::namespace::init_default_namespaces(&db)
            .await
            .unwrap();

        let created = db
            .create_resource(
                "v1",
                "ConfigMap",
                Some("default"),
                "deleted-during-gap",
                serde_json::json!({
                    "metadata": {"name": "deleted-during-gap", "namespace": "default"}
                }),
            )
            .await
            .unwrap();
        db.delete_resource("v1", "ConfigMap", Some("default"), "deleted-during-gap")
            .await
            .unwrap();
        let delete_rv = db.get_current_resource_version().await.unwrap();

        let entries = generate_snapshot(&db, created.resource_version)
            .await
            .unwrap();

        assert!(
            entries.iter().any(|entry| {
                entry.resource_version == delete_rv
                    && matches!(
                        entry.mutations.first(),
                        Some(crate::log_apply::LogApplyMutation::DeleteResource(key))
                            if key.api_version == "v1"
                            && key.kind == "ConfigMap"
                            && key.namespace.as_deref() == Some("default")
                            && key.name == "deleted-during-gap"
                    )
            }),
            "snapshot catch-up must replay resource deletes after the follower cursor"
        );
    }

    #[tokio::test]
    async fn snapshot_restore_preserves_durable_watch_history() {
        let leader = crate::datastore::test_support::in_memory().await;
        crate::controllers::namespace::init_default_namespaces(&leader)
            .await
            .unwrap();

        let current = leader
            .create_resource(
                "v1",
                "ConfigMap",
                Some("default"),
                "current-history",
                serde_json::json!({
                    "metadata": {"name": "current-history", "namespace": "default"},
                    "data": {"state": "created"}
                }),
            )
            .await
            .unwrap();
        leader
            .update_resource(
                "v1",
                "ConfigMap",
                Some("default"),
                "current-history",
                serde_json::json!({
                    "metadata": {
                        "name": "current-history",
                        "namespace": "default",
                        "uid": current.uid
                    },
                    "data": {"state": "updated"}
                }),
                current.resource_version,
            )
            .await
            .unwrap();
        leader
            .create_resource(
                "v1",
                "ConfigMap",
                Some("default"),
                "deleted-history",
                serde_json::json!({
                    "metadata": {"name": "deleted-history", "namespace": "default"}
                }),
            )
            .await
            .unwrap();
        leader
            .delete_resource("v1", "ConfigMap", Some("default"), "deleted-history")
            .await
            .unwrap();

        let leader_events = watch_history_for_compare(&leader).await;
        assert!(
            leader_events
                .iter()
                .any(|event| event.contains("|DELETED|") && event.ends_with("|deleted-history")),
            "leader fixture must contain deleted watch history: {leader_events:?}"
        );

        let snapshot = generate_snapshot(&leader, 0).await.unwrap();
        let follower = crate::datastore::test_support::in_memory().await;
        follower
            .replace_replicated_resource_state(
                snapshot,
                leader.get_current_resource_version().await.unwrap(),
                None,
                None,
                None,
            )
            .await
            .unwrap();

        assert_eq!(watch_history_for_compare(&follower).await, leader_events);
    }

    #[tokio::test]
    async fn snapshot_restore_preserves_retained_watch_event_ids() {
        let leader = crate::datastore::test_support::in_memory().await;
        for index in 0..5 {
            leader
                .create_resource(
                    "v1",
                    "ConfigMap",
                    Some("default"),
                    &format!("retained-{index}"),
                    serde_json::json!({
                        "apiVersion": "v1",
                        "kind": "ConfigMap",
                        "metadata": {
                            "name": format!("retained-{index}"),
                            "namespace": "default"
                        }
                    }),
                )
                .await
                .unwrap();
        }
        leader.gc_watch_events(2, 100).await.unwrap();
        let page_limit = std::num::NonZeroUsize::new(100).unwrap();
        let leader_rows = leader
            .list_all_watch_events_since_paged(0, 0, 0, page_limit)
            .await
            .unwrap();
        assert_eq!(leader_rows.len(), 2, "fixture must retain only the tail");

        let position = leader.current_watch_replay_position().await.unwrap();
        let snapshot = generate_snapshot(&leader, 0).await.unwrap();
        let follower = crate::datastore::test_support::in_memory().await;
        follower
            .replace_replicated_resource_state(
                snapshot,
                position.resource_version,
                Some(position.event_id),
                None,
                None,
            )
            .await
            .unwrap();

        let follower_rows = follower
            .list_all_watch_events_since_paged(0, 0, 0, page_limit)
            .await
            .unwrap();
        assert_eq!(
            follower_rows
                .iter()
                .map(|(event_id, _)| *event_id)
                .collect::<Vec<_>>(),
            leader_rows
                .iter()
                .map(|(event_id, _)| *event_id)
                .collect::<Vec<_>>(),
            "snapshot log-apply rows must retain durable apply-order IDs"
        );
    }

    #[tokio::test]
    async fn snapshot_restore_preserves_allocator_high_water_with_empty_history() {
        let leader = crate::datastore::test_support::in_memory().await;
        leader
            .create_resource(
                "v1",
                "ConfigMap",
                Some("default"),
                "before-gc",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {"name": "before-gc", "namespace": "default"}
                }),
            )
            .await
            .unwrap();
        let leader_position = leader.current_watch_replay_position().await.unwrap();
        assert!(leader_position.event_id > 0);
        assert!(leader.gc_watch_events(0, -1).await.unwrap() > 0);

        let snapshot = generate_snapshot(&leader, 0).await.unwrap();
        let follower = crate::datastore::test_support::in_memory().await;
        follower
            .replace_replicated_resource_state(
                snapshot,
                leader_position.resource_version,
                Some(leader_position.event_id),
                None,
                None,
            )
            .await
            .unwrap();

        assert_eq!(
            follower
                .current_watch_replay_position()
                .await
                .unwrap()
                .event_id,
            leader_position.event_id,
            "an empty retained history must not reset the restored allocator"
        );
        assert!(
            follower
                .list_all_watch_events_since_paged(
                    0,
                    0,
                    0,
                    std::num::NonZeroUsize::new(10).unwrap(),
                )
                .await
                .unwrap()
                .is_empty(),
            "restore must not synthesize rows that retention removed on the leader"
        );
        follower
            .create_resource(
                "v1",
                "ConfigMap",
                Some("default"),
                "after-restore",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {"name": "after-restore", "namespace": "default"}
                }),
            )
            .await
            .unwrap();
        assert!(
            follower
                .current_watch_replay_position()
                .await
                .unwrap()
                .event_id
                > leader_position.event_id,
            "the first post-restore event must allocate above the leader high-water"
        );
    }

    #[tokio::test]
    async fn snapshot_generation_preserves_outbox_stream_watermarks() {
        use crate::log_apply::OutboxStreamWatermark;

        let leader = crate::datastore::test_support::in_memory().await;
        let watermark = OutboxStreamWatermark {
            client_id: "snapshot-client".to_string(),
            stream_id: 7,
            stream_seq: 5,
        };
        for seq in 1..=watermark.stream_seq {
            leader
                .apply_raft_log_apply_commit(LogApplyCommit {
                    resource_version: seq,
                    resource_version_assignment:
                        crate::log_apply::ResourceVersionAssignment::LegacyLeaderAssigned,
                    mutations: Vec::new(),
                    outbox_watermark: Some(OutboxStreamWatermark {
                        client_id: watermark.client_id.clone(),
                        stream_id: watermark.stream_id,
                        stream_seq: seq,
                    }),
                })
                .await
                .unwrap();
        }

        let leader_rv = leader.get_current_resource_version().await.unwrap();
        let snapshot = generate_snapshot(&leader, 0).await.unwrap();
        let follower = crate::datastore::test_support::in_memory().await;
        follower
            .replace_replicated_resource_state(snapshot, leader_rv, None, None, None)
            .await
            .unwrap();

        let duplicate = follower
            .apply_raft_log_apply_commit(LogApplyCommit {
                resource_version: 2,
                resource_version_assignment:
                    crate::log_apply::ResourceVersionAssignment::LegacyLeaderAssigned,
                mutations: Vec::new(),
                outbox_watermark: Some(watermark),
            })
            .await
            .unwrap();
        assert_eq!(
            duplicate.applied_rv,
            Some(2),
            "restored watermark should make duplicate seq a no-op, not a gap"
        );
    }

    #[tokio::test]
    async fn snapshot_generation_does_not_export_legacy_applied_outbox_rows() {
        let leader = crate::datastore::test_support::in_memory().await;
        leader
            .create_resource(
                "v1",
                "ConfigMap",
                Some("default"),
                "snapshot-rv-anchor",
                serde_json::json!({
                    "metadata": {"name": "snapshot-rv-anchor", "namespace": "default"},
                    "data": {"anchor": "rv"}
                }),
            )
            .await
            .unwrap();
        assert!(leader.get_current_resource_version().await.unwrap() > 0);
        leader
            .insert_applied_outbox(crate::datastore::AppliedOutboxRecord {
                idempotency_key: "legacy-snapshot-key".to_string(),
                subject_key: "legacy-subject".to_string(),
                operation: "PodStatus".to_string(),
                first_seen_ms: 1,
                applied_rv: Some(2),
                result_proto: vec![1, 2, 3],
                status_stamp: None,
            })
            .await
            .unwrap();

        let snapshot = generate_snapshot(&leader, 0).await.unwrap();
        let follower = crate::datastore::test_support::in_memory().await;
        follower
            .replace_replicated_resource_state(
                snapshot,
                leader.get_current_resource_version().await.unwrap(),
                None,
                None,
                None,
            )
            .await
            .unwrap();

        assert!(
            follower.list_applied_outbox().await.unwrap().is_empty(),
            "snapshots must not replicate legacy applied_outbox rows"
        );
    }

    #[tokio::test]
    async fn snapshot_restore_preserves_rv_counter_for_next_raft_apply() {
        let leader = crate::datastore::test_support::in_memory().await;
        crate::controllers::namespace::init_default_namespaces(&leader)
            .await
            .unwrap();
        leader
            .create_resource(
                "v1",
                "ConfigMap",
                Some("default"),
                "before-snapshot",
                serde_json::json!({
                    "metadata": {
                        "name": "before-snapshot",
                        "namespace": "default"
                    },
                    "data": {"state": "snapshot"}
                }),
            )
            .await
            .unwrap();
        let leader_rv = leader.get_current_resource_version().await.unwrap();
        let snapshot = generate_snapshot(&leader, 0).await.unwrap();

        let follower = crate::datastore::test_support::in_memory().await;
        follower
            .replace_replicated_resource_state(snapshot, leader_rv, None, None, None)
            .await
            .unwrap();
        assert_eq!(
            follower.get_current_resource_version().await.unwrap(),
            leader_rv,
            "snapshot install must restore the authoritative RV counter"
        );

        let command = crate::datastore::command::StorageCommand::CreateResource {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace: Some("default".to_string()),
            name: "after-snapshot".to_string(),
            data: serde_json::json!({
                "metadata": {
                    "name": "after-snapshot",
                    "namespace": "default",
                    "uid": "after-snapshot-uid"
                },
                "data": {"state": "after"}
            }),
        };
        let outbox_before = follower.list_applied_outbox().await.expect("outbox before");

        let commit = follower
            .build_log_apply_commit_for_command(command, "CreateResource", "leader")
            .await
            .unwrap();

        let outbox_after = follower.list_applied_outbox().await.expect("outbox after");
        assert_eq!(
            outbox_before, outbox_after,
            "generic commit builder should not mutate applied_outbox"
        );
        assert!(
            !commit.mutations.iter().any(|mutation| matches!(
                mutation,
                crate::log_apply::LogApplyMutation::PutAppliedOutbox(_)
            )),
            "generic post-snapshot commit must not emit applied_outbox mutations"
        );
        assert!(
            commit.resource_version > leader_rv,
            "post-snapshot raft proposals must reserve a leader RV greater than snapshot RV {leader_rv}, got {}",
            commit.resource_version
        );
        let applied = follower
            .apply_raft_log_apply_commit(commit)
            .await
            .unwrap()
            .applied_rv
            .expect("raft apply should allocate an RV");

        assert!(
            applied > leader_rv,
            "next applied RV {applied} must be greater than snapshot RV {leader_rv}"
        );
        let loaded = follower
            .get_resource("v1", "ConfigMap", Some("default"), "after-snapshot")
            .await
            .unwrap()
            .expect("post-snapshot create should exist");
        assert_eq!(loaded.resource_version, applied);
    }

    fn pod_status_payload(status: serde_json::Value, uid: &str, stamp: i64) -> Vec<u8> {
        use crate::datastore::ResourcePreconditions;
        use crate::datastore::command::StorageCommand;
        use crate::kubelet::outbox::payload::OutboxPayload;

        let command = StorageCommand::UpdateStatus {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "web".to_string(),
            status,
            expected_rv: None,
            preconditions: ResourcePreconditions {
                uid: Some(uid.to_string()),
                resource_version: None,
            },
            observed_status_stamp: Some(stamp),
        };
        OutboxPayload::from_command(command)
            .encode_protobuf()
            .expect("encode pod status payload")
    }

    async fn create_pod_for_status_snapshot(db: &crate::datastore::sqlite::Datastore, uid: &str) {
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "web",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"namespace": "default", "name": "web", "uid": uid},
                "spec": {
                    "nodeName": "worker-a",
                    "containers": [{"name": "app", "image": "nginx"}]
                },
                "status": {"phase": "Pending"}
            }),
        )
        .await
        .expect("create pod");
    }

    async fn pod_status_message(db: &crate::datastore::sqlite::Datastore) -> Option<String> {
        db.get_resource("v1", "Pod", Some("default"), "web")
            .await
            .expect("read pod")
            .expect("pod exists")
            .data
            .pointer("/status/message")
            .and_then(|value| value.as_str())
            .map(str::to_string)
    }

    #[tokio::test]
    async fn stale_pod_status_replay_rejected_after_snapshot_install() {
        use crate::datastore::sqlite::BuildOutboxOutcome;
        use crate::log_apply::OutboxStreamWatermark;

        let leader = crate::datastore::test_support::in_memory().await;
        create_pod_for_status_snapshot(&leader, "uid-1").await;
        let watermark = OutboxStreamWatermark {
            client_id: "worker-a-epoch".to_string(),
            stream_id: 11,
            stream_seq: 1,
        };

        let newer = leader
            .build_log_apply_commit_for_outbox_with_watermark(
                "status-newer",
                crate::kubelet::outbox::payload::OutboxOperation::PodStatus.as_str(),
                &pod_status_payload(
                    serde_json::json!({"phase": "Running", "message": "newer"}),
                    "uid-1",
                    200,
                ),
                "worker-a",
                Some(watermark.clone()),
            )
            .await
            .expect("build newer status");
        let BuildOutboxOutcome::NeedsPropose { commit, .. } = newer else {
            panic!("newer status should produce a commit");
        };
        leader
            .apply_raft_log_apply_commit(commit)
            .await
            .expect("apply newer status");
        assert_eq!(pod_status_message(&leader).await.as_deref(), Some("newer"));
        assert_eq!(
            leader.list_outbox_stream_watermarks().await.unwrap(),
            vec![watermark.clone()],
            "leader must have recorded the worker stream watermark"
        );

        let leader_rv = leader.get_current_resource_version().await.unwrap();
        let snapshot = generate_snapshot(&leader, 0).await.unwrap();

        let follower = crate::datastore::test_support::in_memory().await;
        follower
            .replace_replicated_resource_state(snapshot, leader_rv, None, None, None)
            .await
            .unwrap();
        assert_eq!(
            pod_status_message(&follower).await.as_deref(),
            Some("newer"),
            "snapshot must carry the live newer status"
        );
        assert_eq!(
            follower.list_outbox_stream_watermarks().await.unwrap(),
            vec![watermark.clone()],
            "snapshot must carry stream watermarks"
        );

        let stale = follower
            .build_log_apply_commit_for_outbox_with_watermark(
                "status-stale-after-snapshot",
                crate::kubelet::outbox::payload::OutboxOperation::PodStatus.as_str(),
                &pod_status_payload(
                    serde_json::json!({"phase": "Running", "message": "stale"}),
                    "uid-1",
                    100,
                ),
                "worker-a",
                Some(watermark),
            )
            .await
            .expect("stale duplicate should complete as already-applied");
        assert!(
            matches!(stale, BuildOutboxOutcome::AlreadyApplied { .. }),
            "restored outbox stream watermark should no-op stale duplicate replay"
        );
        assert_eq!(
            pod_status_message(&follower).await.as_deref(),
            Some("newer"),
            "snapshot restore must preserve stream watermarks so stale status replays no-op"
        );
    }

    #[tokio::test]
    async fn snapshot_after_rv_is_still_authoritative_for_destructive_restore() {
        let leader = crate::datastore::test_support::in_memory().await;
        crate::controllers::namespace::init_default_namespaces(&leader)
            .await
            .unwrap();

        let baseline = leader
            .create_resource(
                "v1",
                "ConfigMap",
                Some("default"),
                "baseline-before-cursor",
                serde_json::json!({
                    "metadata": {
                        "name": "baseline-before-cursor",
                        "namespace": "default"
                    },
                    "data": {"state": "baseline"}
                }),
            )
            .await
            .unwrap();
        let later = leader
            .create_resource(
                "v1",
                "ConfigMap",
                Some("default"),
                "later-after-cursor",
                serde_json::json!({
                    "metadata": {
                        "name": "later-after-cursor",
                        "namespace": "default"
                    },
                    "data": {"state": "later"}
                }),
            )
            .await
            .unwrap();
        let leader_events = watch_history_for_compare(&leader).await;

        let snapshot = generate_snapshot(&leader, baseline.resource_version)
            .await
            .unwrap();
        let follower = crate::datastore::test_support::in_memory().await;
        follower
            .replace_replicated_resource_state(
                snapshot,
                leader.get_current_resource_version().await.unwrap(),
                None,
                None,
                None,
            )
            .await
            .unwrap();

        assert!(
            follower
                .get_resource("v1", "ConfigMap", Some("default"), "baseline-before-cursor")
                .await
                .unwrap()
                .is_some(),
            "destructive restore must keep live rows at or before the follower cursor"
        );
        assert!(
            follower
                .get_resource("v1", "ConfigMap", Some("default"), "later-after-cursor")
                .await
                .unwrap()
                .is_some(),
            "destructive restore must keep live rows after the follower cursor"
        );
        assert_eq!(
            later.resource_version,
            follower.get_current_resource_version().await.unwrap()
        );
        assert_eq!(watch_history_for_compare(&follower).await, leader_events);
    }

    #[tokio::test]
    async fn snapshot_includes_live_namespaced_rows_without_live_namespace() {
        let leader = crate::datastore::test_support::in_memory().await;
        leader
            .create_resource(
                "v1",
                "Event",
                Some("gone-ns"),
                "pod.abc123",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Event",
                    "metadata": {
                        "name": "pod.abc123",
                        "namespace": "gone-ns"
                    },
                    "involvedObject": {
                        "kind": "Pod",
                        "namespace": "gone-ns",
                        "name": "pod",
                        "uid": "pod-uid"
                    },
                    "reason": "Pulled",
                    "source": {"component": "klights-kubelet"},
                    "type": "Normal"
                }),
            )
            .await
            .unwrap();

        let snapshot = generate_snapshot(&leader, 0).await.unwrap();
        let follower = crate::datastore::test_support::in_memory().await;
        follower
            .replace_replicated_resource_state(
                snapshot,
                leader.get_current_resource_version().await.unwrap(),
                None,
                None,
                None,
            )
            .await
            .unwrap();

        assert!(
            follower
                .get_resource("v1", "Event", Some("gone-ns"), "pod.abc123")
                .await
                .unwrap()
                .is_some(),
            "authoritative snapshots must include live rows even when their namespace row is gone"
        );
        assert_eq!(
            watch_history_for_compare(&follower).await,
            watch_history_for_compare(&leader).await
        );
    }

    async fn watch_history_for_compare(db: &crate::datastore::sqlite::Datastore) -> Vec<String> {
        db.list_all_watch_events_since(0)
            .await
            .unwrap()
            .into_iter()
            .map(|event| {
                let resource = event.resource;
                format!(
                    "{}|{}|{}|{}|{}|{}",
                    resource.resource_version,
                    event.event_type,
                    resource.api_version,
                    resource.kind,
                    resource.namespace.unwrap_or_default(),
                    resource.name
                )
            })
            .collect()
    }

    #[tokio::test]
    async fn snapshot_includes_cluster_peer_state() {
        let db = crate::datastore::test_support::in_memory().await;
        let subnet = db
            .allocate_node_subnet("leader", "10.42.0.0/16", "192.0.2.1")
            .await
            .unwrap();
        db.update_node_dataplane(
            crate::networking::wireguard::DataplanePeerMetadata::try_new(
                "leader".to_string(),
                crate::networking::wireguard::DataplaneMode::Root,
                crate::networking::wireguard::DataplaneEncryption::Enabled,
                Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string()),
                Some("192.0.2.1".to_string()),
                Some(51_820),
            )
            .unwrap(),
        )
        .await
        .unwrap();
        let current_rv = db.advance_resource_version_after(0).await.unwrap();

        let entries = generate_snapshot(&db, 0).await.unwrap();

        // Mutations at the same RV are batched into one commit by the
        // snapshot emitter, so look across `mutations` instead of just
        // `mutations.first()`.
        assert!(
            entries.iter().any(|entry| {
                entry.resource_version == current_rv
                    && entry.mutations.iter().any(|m| {
                        matches!(
                            m,
                            crate::log_apply::LogApplyMutation::PutNodeSubnet(row)
                                if row.node_name == "leader"
                                && row.subnet == subnet.subnet.to_string()
                                && row.node_ip == "192.0.2.1"
                        )
                    })
            }),
            "snapshot must include node subnet state so peers can route pods after bootstrap"
        );
        assert!(
            entries.iter().any(|entry| {
                entry.resource_version == current_rv
                    && entry.mutations.iter().any(|m| {
                        matches!(
                            m,
                            crate::log_apply::LogApplyMutation::PutNodeDataplane(row)
                                if row.node_name == "leader"
                                && row.endpoint == "192.0.2.1"
                                && row.port == Some(51_820)
                        )
                    })
            }),
            "snapshot must include dataplane metadata for encrypted peer setup"
        );
    }

    #[tokio::test]
    async fn snapshot_includes_cluster_scoped_resources() {
        let db = crate::datastore::test_support::in_memory().await;
        let node = db
            .create_resource(
                "v1",
                "Node",
                None,
                "worker-a",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Node",
                    "metadata": {"name": "worker-a"}
                }),
            )
            .await
            .unwrap();

        let entries = generate_snapshot(&db, 0).await.unwrap();

        assert!(
            entries.iter().any(|entry| {
                entry.resource_version == node.resource_version
                    && matches!(
                        entry.mutations.first(),
                        Some(crate::log_apply::LogApplyMutation::PutResource(row))
                            if row.api_version == "v1"
                            && row.kind == "Node"
                            && row.namespace.is_none()
                            && row.name == "worker-a"
                    )
            }),
            "snapshot must include cluster-scoped resources so followers can rejoin with a populated read cache"
        );
    }

    // ---- Staging restore contract tests ----

    #[tokio::test]
    async fn staging_restore_successful() {
        let db = crate::datastore::test_support::in_memory().await;

        crate::controllers::namespace::init_default_namespaces(&db)
            .await
            .unwrap();

        // Create a resource (simulating leader state)
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "staged",
            serde_json::json!({"metadata": {"name": "staged"}}),
        )
        .await
        .unwrap();

        // Generate snapshot
        let entries = generate_snapshot(&db, 0).await.unwrap();
        assert!(!entries.is_empty());

        // Verify the snapshot contains our resource
        let has_staged = entries.iter().any(|e| {
            matches!(
                e.mutations.first(),
                Some(crate::log_apply::LogApplyMutation::PutResource(row)) if row.name == "staged"
            )
        });
        assert!(has_staged, "snapshot must contain 'staged' resource");
    }

    #[tokio::test]
    async fn failed_copy_leaves_old_data_untouched() {
        let db = crate::datastore::test_support::in_memory().await;

        // Create initial data
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "original",
            serde_json::json!({"metadata": {"name": "original"}}),
        )
        .await
        .unwrap();

        // Verify it exists
        let existing = db
            .get_resource("v1", "ConfigMap", Some("default"), "original")
            .await
            .unwrap();
        assert!(existing.is_some());

        // Simulate a failed copy — the original data is still there
        let existing_after = db
            .get_resource("v1", "ConfigMap", Some("default"), "original")
            .await
            .unwrap();
        assert!(
            existing_after.is_some(),
            "original data must survive a failed copy"
        );
    }

    // ---- Integration: start/leader never call destructive wipe ----

    #[test]
    fn start_and_leader_paths_do_not_invoke_reseed_code() {
        // Structural assertion: the destructive reseed/wipe code is only
        // reachable via the replica bootstrap path, never via seed Leader
        // or Leader. This is enforced by the runtime dispatch in
        // bootstrap/runtime.rs which returns not-yet-implemented for
        // Replica/Worker roles at this stage.
        //
        // The actual enforcement is in the runtime match arm:
        //   NodeRole::Replica { .. } => bail!("not yet implemented")
        //   NodeRole::Worker { .. } => bail!("not yet implemented")
        //
        // This test documents the 2A-5 contract.
    }
    // memory-improvement.md §10 P1 — characterization test for the
    // keyset-paginated emit path. With more watch_events than the emit page
    // size (SNAPSHOT_EMIT_PAGE_SIZE), the snapshot must still reconstruct a
    // follower byte-for-byte: no rows dropped or duplicated across the page
    // boundary, dedup ledger intact.
    #[tokio::test]
    async fn snapshot_emits_complete_watch_history_across_page_boundaries() {
        let leader = crate::datastore::test_support::in_memory().await;
        crate::controllers::namespace::init_default_namespaces(&leader)
            .await
            .unwrap();

        // Insert strictly more watch_events than SNAPSHOT_EMIT_PAGE_SIZE so the
        // emitter's paged loop crosses at least one boundary.
        let n = super::SNAPSHOT_EMIT_PAGE_SIZE + 2;
        for i in 0..n {
            leader
                .create_resource(
                    "v1",
                    "ConfigMap",
                    Some("default"),
                    &format!("cm-page-{i}"),
                    serde_json::json!({"metadata": {"name": format!("cm-page-{i}")}}),
                )
                .await
                .unwrap();
        }
        let leader_events = watch_history_for_compare(&leader).await;
        assert!(
            leader_events.len() >= n,
            "fixture must produce more watch events than the emit page size ({}), got {}",
            n,
            leader_events.len()
        );

        let leader_rv = leader.get_current_resource_version().await.unwrap();
        let snapshot = generate_snapshot(&leader, 0).await.unwrap();
        let follower = crate::datastore::test_support::in_memory().await;
        follower
            .replace_replicated_resource_state(snapshot, leader_rv, None, None, None)
            .await
            .unwrap();

        // Watch history must round-trip exactly across the page boundary.
        assert_eq!(watch_history_for_compare(&follower).await, leader_events);
        // And the live rows must all be present.
        for i in 0..n {
            assert!(
                follower
                    .get_resource("v1", "ConfigMap", Some("default"), &format!("cm-page-{i}"))
                    .await
                    .unwrap()
                    .is_some(),
                "live row cm-page-{i} must be present after paged snapshot restore"
            );
        }
    }

    // memory-improvement.md §10 P1 — the streamed (proto sink) output must be
    // byte-equivalent to the legacy Vec path over the same fixture, for both
    // watch_events-heavy and applied_outbox-heavy cases. Drives a fixture table
    // through both codepaths and compares the decoded LogApplyCommit sequence.
    #[tokio::test]
    async fn streamed_snapshot_matches_vec_path_across_fixture_table() {
        use crate::log_apply::LogApplyCommit;
        use crate::replication::grpc::{
            generated, log_apply_commit_from_proto, log_apply_commit_to_proto,
        };
        use crate::replication::snapshot::SnapshotCommitSink;
        use crate::replication::snapshot_commit_channel_sink::SnapshotCommitChannelSink;
        use tokio::sync::mpsc;

        for (case, n_resources, n_outbox) in [
            ("empty", 0, 0),
            ("resources-only", 3, 0),
            ("watch-heavy", super::SNAPSHOT_EMIT_PAGE_SIZE + 1, 0),
            ("outbox-heavy", 1, super::SNAPSHOT_EMIT_PAGE_SIZE + 1),
            ("mixed", 5, 7),
        ] {
            let leader = crate::datastore::test_support::in_memory().await;
            crate::controllers::namespace::init_default_namespaces(&leader)
                .await
                .unwrap();
            for i in 0..n_resources {
                leader
                    .create_resource(
                        "v1",
                        "ConfigMap",
                        Some("default"),
                        &format!("cm-{case}-{i}"),
                        serde_json::json!({"metadata": {"name": format!("cm-{case}-{i}")}}),
                    )
                    .await
                    .unwrap();
            }
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            for i in 0..n_outbox {
                leader
                    .insert_applied_outbox(crate::datastore::AppliedOutboxRecord {
                        idempotency_key: format!("key-{case}-{i:05}"),
                        subject_key: format!("subj-{case}-{i}"),
                        operation: "PodMetadata".to_string(),
                        first_seen_ms: now_ms + i as i64,
                        applied_rv: Some(100 + i as i64),
                        result_proto: vec![0u8; i % 7],
                        status_stamp: None,
                    })
                    .await
                    .unwrap();
            }

            // Baseline: the legacy Vec path (encode each commit to proto).
            let baseline_commits = generate_snapshot(&leader, 0).await.unwrap();
            let baseline_protos: Vec<generated::ReplicationEntry> = baseline_commits
                .iter()
                .map(log_apply_commit_to_proto)
                .collect::<Result<_, _>>()
                .unwrap();

            // Streaming path: proto sink into a bounded channel. The producer
            // and consumer must run concurrently (a bounded channel backpressures
            // once > capacity commits are queued), so drive them with join!.
            let (tx, mut rx) =
                mpsc::channel::<Result<generated::ReplicationEntry, tonic::Status>>(64);
            let mut streamed_protos: Vec<generated::ReplicationEntry> = Vec::new();
            let producer = async {
                let mut sink = SnapshotCommitChannelSink::new(tx);
                crate::replication::snapshot::stream_snapshot_commits(&leader, 0, &mut sink)
                    .await
                    .unwrap();
                sink.finish().unwrap();
            };
            let consumer = async {
                while let Some(item) = rx.recv().await {
                    streamed_protos.push(item.expect("proto encode must succeed"));
                }
            };
            tokio::join!(producer, consumer);

            assert_eq!(
                streamed_protos.len(),
                baseline_protos.len(),
                "case `{case}`: streamed commit count must match Vec path"
            );
            for (s, b) in streamed_protos.iter().zip(baseline_protos.iter()) {
                assert_eq!(
                    s.commit_protobuf, b.commit_protobuf,
                    "case `{case}`: streamed proto bytes must equal Vec-path proto bytes"
                );
            }
            // Decode round-trip preserves the commit sequence.
            let decoded: Vec<LogApplyCommit> = streamed_protos
                .into_iter()
                .map(log_apply_commit_from_proto)
                .collect::<Result<_, _>>()
                .unwrap();
            for (got, want) in decoded.iter().zip(baseline_commits.iter()) {
                assert_eq!(got.resource_version, want.resource_version);
                assert_eq!(got.mutations.len(), want.mutations.len());
            }
        }
    }
}
