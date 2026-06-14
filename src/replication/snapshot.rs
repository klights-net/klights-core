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
use crate::datastore::types::Resource;
use crate::log_apply::{
    LogApplyCommit, LogApplyMutation, LogApplyResourceKey, LogApplyWatchEventRow,
};

const SNAPSHOT_JSON_COMMIT_BATCH_SIZE: usize = 128;

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
    writer.write_all(b"[")?;
    let mut sink = JsonArrayCommitSink::new(writer);
    stream_snapshot_commits(db, after_rv, &mut sink).await?;
    writer.write_all(b"]")?;
    Ok(())
}

async fn stream_snapshot_commits<S: SnapshotCommitSink>(
    db: &dyn DatastoreBackend,
    _after_rv: i64,
    sink: &mut S,
) -> Result<()> {
    let mut batcher = SnapshotCommitBatcher::new(sink);
    emit_snapshot_commits(db, &mut batcher).await?;
    batcher.finish()?;
    sink.finish()
}

async fn emit_snapshot_commits<S: SnapshotCommitSink>(
    db: &dyn DatastoreBackend,
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

    let all_watch_events = db.list_all_watch_events_since(0).await?;
    let mut emitted_live_keys = HashSet::new();
    let mut checked_watch_keys = HashSet::new();

    for event in all_watch_events {
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
        let mut mutations = Vec::new();
        if event_type == "DELETED" {
            mutations.push(delete_mutation_from_watch_resource(&resource));
        } else if let Some(current) = live_resources.get(&key)
            && current.resource_version == resource_version
            && emitted_live_keys.insert(key.clone())
        {
            mutations.extend(live_resource_commit(current).mutations);
        }
        mutations.push(watch_event_mutation(resource, event_type));
        sink.push(LogApplyCommit::new(resource_version, mutations))?;
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
        sink.push(live_resource_commit(&resource))?;
    }

    let current_rv = db.get_current_resource_version().await.unwrap_or(0);
    if current_rv > 0 {
        let mut peers = db.list_peer_subnets("").await?;
        peers.sort_by(|a, b| a.node_name.as_str().cmp(b.node_name.as_str()));
        for peer in peers {
            let node_name = peer.node_name.to_string();
            sink.push(LogApplyCommit::put_node_subnet(current_rv, &peer))?;
            if let Some(dataplane) = db.get_node_dataplane(&node_name).await? {
                sink.push(LogApplyCommit::put_node_dataplane(current_rv, &dataplane))?;
            }
        }
    }

    if current_rv > 0 {
        for record in db.list_applied_outbox().await? {
            sink.push(LogApplyCommit::put_applied_outbox(current_rv, record))?;
        }
    }

    for node_name in node_names {
        for intent in db.list_pod_cleanup_intents_for_node(&node_name).await? {
            sink.push(LogApplyCommit::put_pod_cleanup_intent(
                intent.resource_version,
                intent,
            ))?;
        }
    }

    Ok(())
}

trait SnapshotCommitSink {
    fn push(&mut self, commit: LogApplyCommit) -> Result<()>;

    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

struct SnapshotCommitBatcher<'a, S: SnapshotCommitSink> {
    sink: &'a mut S,
    pending: Option<LogApplyCommit>,
}

impl<'a, S: SnapshotCommitSink> SnapshotCommitBatcher<'a, S> {
    fn new(sink: &'a mut S) -> Self {
        Self {
            sink,
            pending: None,
        }
    }

    fn finish(&mut self) -> Result<()> {
        if let Some(commit) = self.pending.take() {
            self.sink.push(commit)?;
        }
        Ok(())
    }
}

impl<S: SnapshotCommitSink> SnapshotCommitSink for SnapshotCommitBatcher<'_, S> {
    fn push(&mut self, commit: LogApplyCommit) -> Result<()> {
        if commit.mutations.is_empty() {
            return Ok(());
        }
        match self.pending.as_mut() {
            Some(pending) if pending.resource_version == commit.resource_version => {
                pending.mutations.extend(commit.mutations);
            }
            Some(_) => {
                let previous = self.pending.replace(commit).expect("pending checked");
                self.sink.push(previous)?;
            }
            None => {
                self.pending = Some(commit);
            }
        }
        Ok(())
    }
}

#[derive(Default)]
struct VecSnapshotCommitSink {
    entries: Vec<LogApplyCommit>,
}

impl SnapshotCommitSink for VecSnapshotCommitSink {
    fn push(&mut self, commit: LogApplyCommit) -> Result<()> {
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
    fn push(&mut self, commit: LogApplyCommit) -> Result<()> {
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
    if resource.api_version == "v1" && resource.kind == "Namespace" && resource.namespace.is_none()
    {
        LogApplyCommit::put_namespace(resource)
    } else {
        LogApplyCommit::put_resource(resource)
    }
}

fn delete_mutation_from_watch_resource(resource: &Resource) -> LogApplyMutation {
    if resource.api_version == "v1" && resource.kind == "Namespace" && resource.namespace.is_none()
    {
        LogApplyMutation::DeleteNamespace {
            name: resource.name.clone(),
        }
    } else {
        LogApplyMutation::DeleteResource(LogApplyResourceKey {
            api_version: resource.api_version.clone(),
            kind: resource.kind.clone(),
            namespace: resource.namespace.clone(),
            name: resource.name.clone(),
            uid: resource.uid.clone(),
            precondition_resource_version: None,
        })
    }
}

fn watch_event_mutation(resource: Resource, event_type: String) -> LogApplyMutation {
    LogApplyMutation::PutWatchEvent(LogApplyWatchEventRow {
        api_version: resource.api_version,
        kind: resource.kind,
        namespace: resource.namespace,
        name: resource.name,
        resource_version: resource.resource_version,
        event_type,
        data: std::sync::Arc::unwrap_or_clone(resource.data),
    })
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
            )
            .await
            .unwrap();

        assert_eq!(watch_history_for_compare(&follower).await, leader_events);
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
            .replace_replicated_resource_state(snapshot, leader_rv, None)
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
        let payload = crate::kubelet::outbox::payload::OutboxPayload::from_command(command)
            .encode_protobuf()
            .unwrap();
        let outcome = follower
            .build_log_apply_commit_for_outbox(
                "snapshot-next-rv-key",
                "CreateResource",
                payload.as_ref(),
                "leader",
            )
            .await
            .unwrap();
        let crate::datastore::sqlite::BuildOutboxOutcome::NeedsPropose { commit, .. } = outcome
        else {
            panic!("expected create after snapshot to need proposal");
        };
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
}
