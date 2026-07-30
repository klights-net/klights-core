//! Phase 3 Raft snapshot envelope and builder.
//!
//! openraft drives `RaftSnapshotBuilder::build_snapshot` on the leader
//! (and on followers that fall too far behind log retention) to package
//! the current state-machine view into a single transferable blob. The
//! follower receives the bytes via `RaftStateMachine::install_snapshot`
//! and atomically replays them, then resumes the log from the snapshot's
//! `last_log_id`.
//!
//! The on-the-wire payload reuses the existing
//! `datastore::snapshot_export::generate_snapshot` helper that already powers
//! the Phase 2 replica join path, so leader and follower share one
//! source of truth for "what makes up a cluster snapshot".

use std::io::Cursor;
use std::io::Write;
use std::sync::Arc;

use anyhow::Result;
use klights_cluster_store::{
    DurableReplayTarget, SnapshotCaptureHeader, SnapshotCapturePage, SnapshotCapturePageKind,
    SnapshotPersistenceError,
};
use klights_node_store::RaftAppliedStateDurability;
use openraft::storage::RaftSnapshotBuilder;
use openraft::{
    AnyError, LogId, Snapshot, SnapshotMeta, StorageError, StorageIOError, StoredMembership,
};
use serde::{Deserialize, Serialize};

#[cfg(test)]
use super::super::DatastoreHandle;
use super::super::DurableRecoveryStore;
use klights_cluster_store::BackendLifecycleStore;
use klights_replication::types::{NodeId, TypeConfig};

/// Self-describing snapshot envelope. Carries the `last_applied`
/// log-id, the membership configuration, and an ordered list of
/// `LogApplyCommit` rows that, when replayed via
/// `DatastoreBackend::apply_log_apply_commit`, reconstruct the cluster
/// data state.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct RaftSnapshotData {
    pub last_applied: Option<LogId<NodeId>>,
    pub membership: StoredMembership<NodeId, klights_replication::types::RaftMemberNode>,
    #[serde(default)]
    pub current_rv: i64,
    /// Exact-v3 activation proof captured from the same immutable backend
    /// view as the allocator and resource state. A missing proof is rejected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command_codec_activation_version: Option<u32>,
    /// Durable watch-log allocator boundary. `None` means no boundary was
    /// captured and is rejected where an authoritative boundary is required.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub watch_event_high_water: Option<i64>,
    /// Authoritative watch-compaction boundaries.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub watch_replay_floors: Option<Vec<super::super::WatchReplayFloor>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster_metadata: Option<klights_cluster_core::ClusterMetadata>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster_membership: Option<RaftSnapshotMembership>,
    #[serde(default, alias = "commits", with = "snapshot_operations_serde")]
    pub operations: Vec<klights_cluster_core::SnapshotRestoreOperation>,
}

#[derive(Serialize, Deserialize)]
struct SnapshotOperationWire {
    resource_version: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    outbox_watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
    mutations: Vec<klights_cluster_core::LogApplyMutation>,
}

mod snapshot_operations_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use super::SnapshotOperationWire;
    use klights_cluster_core::SnapshotRestoreOperation;

    pub(super) fn serialize<S>(
        operations: &[SnapshotRestoreOperation],
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        operations
            .iter()
            .map(|operation| SnapshotOperationWire {
                resource_version: operation.resource_version(),
                outbox_watermark: operation.outbox_watermark().cloned(),
                mutations: operation.mutations().to_vec(),
            })
            .collect::<Vec<_>>()
            .serialize(serializer)
    }

    pub(super) fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<Vec<SnapshotRestoreOperation>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Vec::<SnapshotOperationWire>::deserialize(deserializer)?
            .into_iter()
            .map(|wire| {
                SnapshotRestoreOperation::new(
                    wire.resource_version,
                    wire.outbox_watermark,
                    wire.mutations,
                )
            })
            .collect())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) enum RaftSnapshotMembership {
    AuthoritativeAbsent,
    Present(klights_cluster_core::ClusterMembership),
}

impl RaftSnapshotData {
    pub(crate) fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self> {
        super::compressed::decode_json(bytes)
    }

    #[cfg(test)]
    pub async fn serialize_from_backend_to_cursor(
        db: DatastoreHandle,
        last_applied: Option<LogId<NodeId>>,
        membership: &StoredMembership<NodeId, klights_replication::types::RaftMemberNode>,
    ) -> Result<Cursor<Vec<u8>>> {
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(Default::default()));
        let recovery = Arc::new(super::super::DatastoreDurableRecoveryPort::new(db.clone()));
        let lifecycle = Arc::new(super::super::DatastoreBackendLifecyclePort::new(db));
        let (snapshot, _, _) = Self::serialize_from_backend_to_cursor_inner(
            recovery,
            lifecycle,
            RaftSnapshotAppliedStateSource::Fixed {
                last_applied,
                membership: membership.clone(),
            },
            supervisor,
        )
        .await?;
        Ok(snapshot)
    }

    async fn serialize_from_backend_to_cursor_inner(
        recovery: Arc<dyn DurableRecoveryStore>,
        lifecycle: Arc<dyn BackendLifecycleStore>,
        applied_state_source: RaftSnapshotAppliedStateSource,
        supervisor: Arc<klights_supervisor::TaskSupervisor>,
    ) -> Result<(
        Cursor<Vec<u8>>,
        Option<LogId<NodeId>>,
        StoredMembership<NodeId, klights_replication::types::RaftMemberNode>,
    )> {
        let request = klights_cluster_store::SnapshotCaptureRequest::try_new(
            klights_cluster_store::SnapshotPageLimit::try_new(
                klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE,
            )?,
            std::time::Duration::from_secs(300),
        )?;
        let fence = lifecycle
            .acquire_snapshot_exclusive_fence()
            .await?
            .ok_or_else(|| anyhow::anyhow!("backend does not provide a snapshot capture fence"))?;
        let captured = applied_state_source.load().await?;
        let session = recovery.begin_pinned_snapshot_capture(request, fence).await;
        let (mut session, captured) = match session {
            Ok(session) => (session, captured),
            Err(error) => {
                if !is_pristine_raft_state(&captured) || !is_missing_cluster_identity_error(&error)
                {
                    return Err(error);
                }
                let allocator = recovery.read_durable_allocator_observation().await?;
                if allocator.position.resource_version != 0
                    || allocator.position.event_id != 0
                    || allocator.position.resource_version_filter_through_event_id != 0
                {
                    return Err(error);
                }
                let snapshot =
                    encode_pristine_bootstrap_snapshot(captured.clone(), supervisor.clone())
                        .await?;
                let (last_applied, membership) = captured;
                return Ok((Cursor::new(snapshot), last_applied, membership));
            }
        };
        let returned_header = session.header().clone();
        let (encoder_tx, mut encoder_rx) = tokio::sync::mpsc::channel(1);
        let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
        let encoder_header = returned_header.clone();
        let captured_for_encoder = captured.clone();
        let blocking_supervisor = supervisor.clone();
        supervisor
            .spawn_async(
                klights_supervisor::TaskCategory::Background,
                "raft-snapshot-encoder",
                async move {
                    let result = blocking_supervisor
                        .run_blocking(
                            klights_supervisor::TaskCategory::Others,
                            "raft-snapshot-json-zstd",
                            move || {
                                encode_snapshot_pages(
                                    encoder_header,
                                    captured_for_encoder,
                                    &mut encoder_rx,
                                )
                            },
                        )
                        .await
                        .map_err(|error| anyhow::anyhow!("snapshot encoder supervisor: {error}"))
                        .and_then(|result| result);
                    let _ = finished_tx.send(result);
                },
            )
            .await?;
        while let Some(page) = session.next_page().await? {
            encoder_tx
                .send(page)
                .await
                .map_err(|_| anyhow::anyhow!("snapshot encoder stopped before final page"))?;
        }
        drop(encoder_tx);
        let framed = finished_rx
            .await
            .map_err(|_| anyhow::anyhow!("snapshot encoder stopped without a result"))??;
        let (last_applied, membership) = captured;
        Ok((Cursor::new(framed), last_applied, membership))
    }
}

fn is_pristine_raft_state(captured: &CapturedRaftAppliedState) -> bool {
    captured.0.is_none() && captured.1 == StoredMembership::default()
}

fn is_missing_cluster_identity_error(error: &anyhow::Error) -> bool {
    error
        .chain()
        .any(|cause| cause.to_string() == "cluster_id is missing")
}

async fn encode_pristine_bootstrap_snapshot(
    captured: CapturedRaftAppliedState,
    supervisor: Arc<klights_supervisor::TaskSupervisor>,
) -> Result<Vec<u8>> {
    supervisor
        .run_blocking(
            klights_supervisor::TaskCategory::Others,
            "raft-pristine-bootstrap-snapshot-json-zstd",
            move || {
                let data = RaftSnapshotData {
                    last_applied: captured.0,
                    membership: captured.1,
                    current_rv: 0,
                    command_codec_activation_version: None,
                    watch_event_high_water: Some(0),
                    watch_replay_floors: Some(Vec::new()),
                    cluster_metadata: None,
                    cluster_membership: None,
                    operations: Vec::new(),
                };
                super::compressed::encode(&serde_json::to_vec(&data)?)
            },
        )
        .await
        .map_err(|error| anyhow::anyhow!("pristine bootstrap snapshot supervisor: {error}"))?
}

fn encode_snapshot_pages(
    header: SnapshotCaptureHeader,
    captured: CapturedRaftAppliedState,
    pages: &mut tokio::sync::mpsc::Receiver<SnapshotCapturePage>,
) -> Result<Vec<u8>> {
    let mut framed = vec![super::compressed::TAG_ZSTD];
    let mut encoder = zstd::Encoder::new(&mut framed, 3)?;
    let mut writer = RaftJsonSnapshotEncoder::new(&mut encoder);
    writer.begin_capture(&header, captured)?;
    while let Some(page) = pages.blocking_recv() {
        writer.push_page(page)?;
    }
    writer.finish(&header)?;
    drop(writer);
    encoder.finish()?;
    Ok(framed)
}

enum RaftSnapshotAppliedStateSource {
    #[cfg(test)]
    Fixed {
        last_applied: Option<LogId<NodeId>>,
        membership: StoredMembership<NodeId, klights_replication::types::RaftMemberNode>,
    },
    Durable(Arc<dyn RaftAppliedStateDurability>),
}

type CapturedRaftAppliedState = (
    Option<LogId<NodeId>>,
    StoredMembership<NodeId, klights_replication::types::RaftMemberNode>,
);

impl RaftSnapshotAppliedStateSource {
    async fn load(&self) -> Result<CapturedRaftAppliedState, SnapshotPersistenceError> {
        let (last_applied, membership) = match self {
            #[cfg(test)]
            Self::Fixed {
                last_applied,
                membership,
            } => return Ok((*last_applied, membership.clone())),
            Self::Durable(applied_state) => applied_state
                .load_applied_state()
                .await
                .map_err(map_raft_durability_snapshot_error)?
                .into_parts(),
        };
        let last_applied = last_applied
            .map(|bytes| serde_json::from_slice(bytes.as_slice()))
            .transpose()
            .map_err(|error| {
                SnapshotPersistenceError::persistence_failed(format!(
                    "failed to decode Raft last-applied state: {error}"
                ))
            })?;
        let membership = membership
            .map(|bytes| serde_json::from_slice(bytes.as_slice()))
            .transpose()
            .map_err(|error| {
                SnapshotPersistenceError::persistence_failed(format!(
                    "failed to decode Raft membership state: {error}"
                ))
            })?
            .unwrap_or_default();
        Ok((last_applied, membership))
    }
}

fn map_raft_durability_snapshot_error(
    error: klights_node_store::RaftDurabilityError,
) -> SnapshotPersistenceError {
    match error {
        klights_node_store::RaftDurabilityError::InvalidInput { message, .. }
        | klights_node_store::RaftDurabilityError::CorruptData { message, .. } => {
            SnapshotPersistenceError::CorruptData { message }
        }
        klights_node_store::RaftDurabilityError::Retryable { message, .. } => {
            SnapshotPersistenceError::Retryable { message }
        }
        klights_node_store::RaftDurabilityError::Timeout => SnapshotPersistenceError::Timeout,
        klights_node_store::RaftDurabilityError::Cancelled => SnapshotPersistenceError::Cancelled,
        klights_node_store::RaftDurabilityError::PersistenceFailed { operation, message } => {
            SnapshotPersistenceError::persistence_failed(format!("{operation}: {message}"))
        }
        _ => SnapshotPersistenceError::persistence_failed(error.to_string()),
    }
}

struct RaftJsonSnapshotEncoder<'a, W: Write + Send> {
    writer: &'a mut W,
    captured_applied_state: Option<CapturedRaftAppliedState>,
    header: Option<SnapshotCaptureHeader>,
    first_commit: bool,
    floors_open: bool,
    first_floor: bool,
}

impl<'a, W: Write + Send> RaftJsonSnapshotEncoder<'a, W> {
    fn new(writer: &'a mut W) -> Self {
        Self {
            writer,
            captured_applied_state: None,
            header: None,
            first_commit: true,
            floors_open: false,
            first_floor: true,
        }
    }

    fn write_json<T: Serialize + ?Sized>(
        &mut self,
        value: &T,
    ) -> Result<(), SnapshotPersistenceError> {
        serde_json::to_writer(&mut self.writer, value)
            .map_err(|error| SnapshotPersistenceError::persistence_failed(error.to_string()))
    }

    fn write_bytes(&mut self, bytes: &[u8]) -> Result<(), SnapshotPersistenceError> {
        self.writer
            .write_all(bytes)
            .map_err(|error| SnapshotPersistenceError::persistence_failed(error.to_string()))
    }

    fn write_operation(
        &mut self,
        operation: &klights_cluster_core::SnapshotRestoreOperation,
    ) -> Result<(), SnapshotPersistenceError> {
        if self.floors_open {
            return Err(SnapshotPersistenceError::persistence_failed(
                "snapshot commit page followed replay-floor pages",
            ));
        }
        if !self.first_commit {
            self.write_bytes(b",")?;
        }
        self.first_commit = false;
        self.write_json(&SnapshotOperationWire {
            resource_version: operation.resource_version(),
            outbox_watermark: operation.outbox_watermark().cloned(),
            mutations: operation.mutations().to_vec(),
        })
    }

    fn open_floors(&mut self) -> Result<(), SnapshotPersistenceError> {
        if !self.floors_open {
            self.write_bytes(b"],\"watch_replay_floors\":[")?;
            self.floors_open = true;
        }
        Ok(())
    }

    fn write_floor(
        &mut self,
        floor: klights_cluster_store::DurableReplayFloor,
    ) -> Result<(), SnapshotPersistenceError> {
        self.open_floors()?;
        if !self.first_floor {
            self.write_bytes(b",")?;
        }
        self.first_floor = false;
        let header = self.header.as_ref().expect("capture header precedes pages");
        let position = header.position();
        let (target, mut floor_resource_version, mut floor_event_id, position_is_exact) =
            floor.into_parts();
        if position_is_exact && floor_event_id > position.event_id {
            floor_event_id = position.event_id;
            floor_resource_version = position.resource_version;
        } else {
            floor_resource_version = floor_resource_version.min(position.resource_version);
        }
        let (api_version, kind, namespace_key) = match target {
            DurableReplayTarget::All => ("*".to_string(), "*".to_string(), "*".to_string()),
            DurableReplayTarget::Cluster { api_version, kind } => {
                (api_version, kind, "#cluster".to_string())
            }
            DurableReplayTarget::Namespaced {
                api_version,
                kind,
                namespace,
            } => (api_version, kind, namespace),
        };
        self.write_json(&super::super::WatchReplayFloor {
            api_version,
            kind,
            namespace_key,
            floor_resource_version,
            floor_event_id,
            position_is_exact,
        })
    }

    fn finish(&mut self, returned: &SnapshotCaptureHeader) -> Result<()> {
        let begun = self
            .header
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("snapshot capture omitted header"))?;
        anyhow::ensure!(
            begun.position() == returned.position()
                && begun.command_codec_activation_version()
                    == returned.command_codec_activation_version()
                && begun.metadata() == returned.metadata()
                && begun.membership() == returned.membership(),
            "snapshot capture returned a different header than it began"
        );
        self.open_floors()?;
        self.write_bytes(b"]}")?;
        Ok(())
    }

    fn begin_capture(
        &mut self,
        header: &SnapshotCaptureHeader,
        captured: CapturedRaftAppliedState,
    ) -> Result<(), SnapshotPersistenceError> {
        if self.header.is_some() {
            return Err(SnapshotPersistenceError::persistence_failed(
                "snapshot capture began more than once",
            ));
        }
        let (last_applied, membership) = captured;
        self.write_bytes(b"{\"last_applied\":")?;
        self.write_json(&last_applied)?;
        self.write_bytes(b",\"membership\":")?;
        self.write_json(&membership)?;
        self.captured_applied_state = Some((last_applied, membership));
        self.header = Some(header.clone());
        let position = header.position();
        self.write_bytes(b",\"current_rv\":")?;
        self.write_json(&position.resource_version)?;
        self.write_bytes(b",\"command_codec_activation_version\":")?;
        self.write_json(&header.command_codec_activation_version())?;
        self.write_bytes(b",\"watch_event_high_water\":")?;
        self.write_json(&position.event_id)?;
        self.write_bytes(b",\"cluster_metadata\":")?;
        self.write_json(&Some(header.metadata()))?;
        self.write_bytes(b",\"cluster_membership\":")?;
        let membership = match header.membership() {
            klights_cluster_store::SnapshotMembership::LegacyOmitted => None,
            klights_cluster_store::SnapshotMembership::AuthoritativeAbsent => {
                Some(RaftSnapshotMembership::AuthoritativeAbsent)
            }
            klights_cluster_store::SnapshotMembership::Present(membership) => {
                Some(RaftSnapshotMembership::Present(membership.clone()))
            }
        };
        self.write_json(&membership)?;
        self.write_bytes(b",\"operations\":[")
    }

    fn push_page(&mut self, page: SnapshotCapturePage) -> Result<(), SnapshotPersistenceError> {
        let current_rv = self
            .header
            .as_ref()
            .ok_or_else(|| {
                SnapshotPersistenceError::persistence_failed(
                    "snapshot page arrived before capture header",
                )
            })?
            .position()
            .resource_version;
        match page.kind() {
            SnapshotCapturePageKind::Commits => {
                for operation in page.into_operations().expect("page kind checked") {
                    self.write_operation(&operation)?;
                }
            }
            SnapshotCapturePageKind::AppliedOutbox => {
                for row in page.into_applied_outbox().expect("page kind checked") {
                    self.write_operation(&klights_cluster_core::SnapshotRestoreOperation::new(
                        current_rv,
                        None,
                        vec![klights_cluster_core::LogApplyMutation::PutAppliedOutbox(
                            row,
                        )],
                    ))?;
                }
            }
            SnapshotCapturePageKind::OutboxWatermarks => {
                for outbox_watermark in page.into_outbox_watermarks().expect("page kind checked") {
                    self.write_operation(&klights_cluster_core::SnapshotRestoreOperation::new(
                        current_rv,
                        Some(outbox_watermark),
                        Vec::new(),
                    ))?;
                }
            }
            SnapshotCapturePageKind::ReplayFloors => {
                for floor in page.into_replay_floors().expect("page kind checked") {
                    self.write_floor(floor)?;
                }
            }
        }
        Ok(())
    }
}

pub fn snapshot_id_for(last_applied: Option<LogId<NodeId>>) -> String {
    match last_applied {
        Some(id) => format!("raft-snapshot-t{}-i{}", id.leader_id.term, id.index),
        None => "raft-snapshot-empty".to_string(),
    }
}

fn snapshot_write_err<E: std::fmt::Display>(e: E) -> StorageError<NodeId> {
    StorageError::IO {
        source: StorageIOError::write_snapshot(None, AnyError::error(e.to_string())),
    }
}

/// Real snapshot builder used by `SqliteRaftStateMachine::get_snapshot_builder`.
/// Owns the cluster backend handle plus a snapshot of the engine's
/// `last_applied` / `membership` at build-request time so the produced
/// `SnapshotMeta` is consistent with the bytes it carries.
#[derive(Clone)]
pub struct SqliteRaftSnapshotBuilder {
    pub(crate) recovery: Arc<dyn DurableRecoveryStore>,
    pub(crate) lifecycle: Arc<dyn BackendLifecycleStore>,
    pub(crate) applied_state: Arc<dyn RaftAppliedStateDurability>,
    pub(crate) supervisor: Arc<klights_supervisor::TaskSupervisor>,
}

impl RaftSnapshotBuilder<TypeConfig> for SqliteRaftSnapshotBuilder {
    async fn build_snapshot(
        &mut self,
    ) -> std::result::Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let (snapshot, last_applied, membership) =
            RaftSnapshotData::serialize_from_backend_to_cursor_inner(
                self.recovery.clone(),
                self.lifecycle.clone(),
                RaftSnapshotAppliedStateSource::Durable(self.applied_state.clone()),
                self.supervisor.clone(),
            )
            .await
            .map_err(snapshot_write_err)?;
        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership: membership,
            snapshot_id: snapshot_id_for(last_applied),
        };
        Ok(Snapshot {
            meta,
            snapshot: Box::new(snapshot),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datastore::DatastoreBackend;
    use crate::datastore::test_support;
    use klights_node_store::{
        EncodedRaftAppliedState, RaftAppliedStateWrite, RaftDurabilityFuture,
    };

    struct BlockingAppliedState {
        reached: Arc<tokio::sync::Notify>,
        resume: Arc<tokio::sync::Notify>,
    }

    impl RaftAppliedStateDurability for BlockingAppliedState {
        fn load_applied_state(&self) -> RaftDurabilityFuture<'_, EncodedRaftAppliedState> {
            let reached = self.reached.clone();
            let resume = self.resume.clone();
            Box::pin(async move {
                reached.notify_one();
                resume.notified().await;
                Ok(EncodedRaftAppliedState::new(None, None))
            })
        }

        fn store_applied_state(
            &self,
            _state: RaftAppliedStateWrite,
        ) -> RaftDurabilityFuture<'_, ()> {
            Box::pin(async { Ok(()) })
        }
    }

    async fn seed_cluster_metadata(db: &dyn DatastoreBackend) {
        db.set_klights_meta(
            klights_cluster_store::CLUSTER_ID_META_KEY,
            "snapshot-test-cluster",
        )
        .await
        .unwrap();
        db.set_klights_meta(klights_cluster_store::LEADER_EPOCH_META_KEY, "1")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn pristine_pre_cluster_snapshot_is_canonical_and_identity_free() {
        let db = test_support::in_memory().await;
        let snapshot = RaftSnapshotData::serialize_from_backend_to_cursor(
            Arc::new(db),
            None,
            &StoredMembership::default(),
        )
        .await
        .expect("OpenRaft must be able to persist its initial Snapshot(None)");
        let decoded = RaftSnapshotData::deserialize_from_bytes(&snapshot.into_inner()).unwrap();

        assert_eq!(decoded.last_applied, None);
        assert_eq!(decoded.membership, StoredMembership::default());
        assert_eq!(decoded.current_rv, 0);
        assert_eq!(decoded.watch_event_high_water, Some(0));
        assert_eq!(decoded.watch_replay_floors, Some(Vec::new()));
        assert_eq!(decoded.cluster_metadata, None);
        assert_eq!(decoded.cluster_membership, None);
        assert!(decoded.operations.is_empty());
    }

    #[tokio::test]
    async fn missing_cluster_identity_is_rejected_after_raft_state_exists() {
        let log_id = LogId::new(openraft::LeaderId::new(3, 7), 11);
        let membership =
            StoredMembership::new(
                Some(log_id),
                openraft::Membership::new(
                    vec![std::collections::BTreeSet::from([7])],
                    std::collections::BTreeMap::<
                        NodeId,
                        klights_replication::types::RaftMemberNode,
                    >::new(),
                ),
            );
        let cases = [
            (
                "applied log",
                Some(log_id),
                StoredMembership::default(),
                false,
            ),
            ("membership", None, membership, false),
            (
                "non-empty allocator",
                None,
                StoredMembership::default(),
                true,
            ),
        ];

        for (case, last_applied, membership, seed_resource) in cases {
            let db = test_support::in_memory().await;
            if seed_resource {
                db.create_resource(
                    "v1",
                    "ConfigMap",
                    Some("default"),
                    "preexisting",
                    serde_json::json!({"metadata": {"name": "preexisting"}}),
                )
                .await
                .unwrap();
            }
            let error = RaftSnapshotData::serialize_from_backend_to_cursor(
                Arc::new(db),
                last_applied,
                &membership,
            )
            .await
            .expect_err(case);
            assert!(
                error.to_string().contains("cluster_id is missing"),
                "{case} must retain authoritative identity validation: {error:#}"
            );
        }
    }

    #[tokio::test]
    async fn builder_reads_applied_state_inside_authoritative_capture_fence() {
        let db = test_support::in_memory().await;
        seed_cluster_metadata(&db).await;
        let handle: DatastoreHandle = Arc::new(db.clone());
        let reached = Arc::new(tokio::sync::Notify::new());
        let resume = Arc::new(tokio::sync::Notify::new());
        let reached_wait = reached.notified();
        tokio::pin!(reached_wait);
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(Default::default()));

        let capture_task = tokio::spawn({
            let reached = reached.clone();
            let resume = resume.clone();
            async move {
                let recovery = Arc::new(crate::datastore::DatastoreDurableRecoveryPort::new(
                    handle.clone(),
                ));
                let lifecycle =
                    Arc::new(crate::datastore::DatastoreBackendLifecyclePort::new(handle));
                RaftSnapshotData::serialize_from_backend_to_cursor_inner(
                    recovery,
                    lifecycle,
                    RaftSnapshotAppliedStateSource::Durable(Arc::new(BlockingAppliedState {
                        reached,
                        resume,
                    })),
                    supervisor,
                )
                .await
            }
        });
        reached_wait.await;

        let mutation_task = tokio::spawn(async move {
            crate::datastore::DatastoreBackend::acquire_snapshot_mutation_fence(&db)
                .await
                .unwrap()
                .expect("sqlite supplies a mutation fence")
        });
        tokio::task::yield_now().await;
        assert!(
            !mutation_task.is_finished(),
            "committed apply must not cross the applied-state read in begin_capture"
        );

        resume.notify_one();
        let (_, last_applied, membership) = capture_task.await.unwrap().unwrap();
        assert_eq!(last_applied, None);
        assert_eq!(membership, StoredMembership::default());
        mutation_task.await.unwrap();
    }

    /// P2 (memory): serialize_from_backend_to_cursor must produce a valid
    /// zstd-framed payload that round-trips through deserialize_from_bytes.
    /// The streaming path always emits TAG_ZSTD (no RAW fallback), because
    /// the streaming encoder can't know the final size to decide fallback.
    #[tokio::test]
    async fn streaming_snapshot_round_trips_and_is_zstd_framed() {
        let db = test_support::in_memory().await;
        seed_cluster_metadata(&db).await;
        crate::controllers::namespace::init_default_namespaces(
            &crate::kubelet::file_blocking::test_file_process_executor(),
            &db,
        )
        .await
        .unwrap();
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "cm-stream-test",
            serde_json::json!({"metadata": {"name": "cm-stream-test"}}),
        )
        .await
        .unwrap();

        let membership =
            StoredMembership::<NodeId, klights_replication::types::RaftMemberNode>::default();
        let cursor = RaftSnapshotData::serialize_from_backend_to_cursor(
            Arc::new(db.clone()),
            None,
            &membership,
        )
        .await
        .unwrap();

        let framed = cursor.into_inner();
        assert_eq!(
            framed[0],
            crate::datastore::raft::compressed::TAG_ZSTD,
            "streaming snapshot must be zstd-framed (P2)"
        );

        let decoded = RaftSnapshotData::deserialize_from_bytes(&framed).unwrap();
        assert_eq!(
            decoded.current_rv,
            db.get_current_resource_version().await.unwrap()
        );
        assert_eq!(
            decoded.watch_event_high_water,
            Some(db.current_watch_replay_position().await.unwrap().event_id)
        );
        assert!(
            !decoded.operations.is_empty(),
            "snapshot must contain restore operations"
        );
        assert!(
            decoded
                .operations
                .iter()
                .any(|operation| operation.mutations().iter().any(|m| {
                    matches!(m, klights_cluster_core::LogApplyMutation::PutResource(row)
                    if row.name == "cm-stream-test")
                })),
            "snapshot must contain the ConfigMap"
        );
    }

    #[tokio::test]
    async fn authoritative_stream_preserves_rv_event_floor_and_outbox_families() {
        let db = test_support::in_memory().await;
        let outbox = klights_cluster_core::LogApplyAppliedOutboxRow {
            idempotency_key: "snapshot-ledger".into(),
            subject_key: "v1/ConfigMap/default/item/uid".into(),
            operation: "Update".into(),
            first_seen_ms: 10,
            applied_rv: Some(7),
            result_proto: vec![1, 2, 3],
            status_stamp: None,
        };
        let watermark = klights_cluster_core::OutboxStreamWatermark {
            client_id: "worker-a".into(),
            stream_id: 4,
            stream_seq: 1,
        };
        db.replace_replicated_resource_state(
            vec![klights_cluster_core::SnapshotRestoreOperation::new(
                7,
                Some(watermark.clone()),
                vec![klights_cluster_core::LogApplyMutation::PutAppliedOutbox(
                    outbox.clone(),
                )],
            )],
            7,
            Some(11),
            Some(vec![crate::datastore::WatchReplayFloor {
                api_version: "v1".into(),
                kind: "ConfigMap".into(),
                namespace_key: "default".into(),
                floor_resource_version: 6,
                floor_event_id: 10,
                position_is_exact: true,
            }]),
            Some(crate::datastore::ReplicatedSnapshotMetadata {
                cluster_id: "snapshot-exact-cluster".into(),
                leader_epoch: 3,
                membership: crate::datastore::ReplicatedMembershipState::AuthoritativeAbsent,
                command_codec_activation_version: None,
            }),
        )
        .await
        .unwrap();

        let encoded = RaftSnapshotData::serialize_from_backend_to_cursor(
            Arc::new(db),
            None,
            &StoredMembership::default(),
        )
        .await
        .unwrap();
        let decoded = RaftSnapshotData::deserialize_from_bytes(&encoded.into_inner()).unwrap();

        assert_eq!(decoded.current_rv, 7);
        assert_eq!(decoded.watch_event_high_water, Some(11));
        assert_eq!(
            decoded.cluster_metadata,
            Some(klights_cluster_core::ClusterMetadata {
                cluster_id: "snapshot-exact-cluster".into(),
                leader_epoch: 3,
                current_rv: 7,
            })
        );
        assert_eq!(
            decoded.cluster_membership,
            Some(RaftSnapshotMembership::AuthoritativeAbsent)
        );
        let floors = decoded.watch_replay_floors.unwrap();
        assert_eq!(floors.len(), 1);
        assert_eq!(floors[0].floor_resource_version, 6);
        assert_eq!(floors[0].floor_event_id, 10);
        assert!(
            decoded
                .operations
                .iter()
                .any(|operation| { operation.outbox_watermark() == Some(&watermark) })
        );
        assert!(decoded.operations.iter().any(|operation| {
            operation.mutations().iter().any(|mutation| {
                matches!(
                    mutation,
                    klights_cluster_core::LogApplyMutation::PutAppliedOutbox(row)
                        if row == &outbox
                )
            })
        }));
    }

    #[test]
    fn legacy_snapshot_without_watch_allocator_remains_decodable() {
        let legacy = serde_json::json!({
            "last_applied": null,
            "membership": StoredMembership::<NodeId, klights_replication::types::RaftMemberNode>::default(),
            "current_rv": 7,
            "commits": []
        });
        let framed = crate::datastore::raft::compressed::encode(
            serde_json::to_vec(&legacy).unwrap().as_slice(),
        )
        .unwrap();

        let decoded = RaftSnapshotData::deserialize_from_bytes(&framed).unwrap();
        assert_eq!(decoded.current_rv, 7);
        assert_eq!(decoded.watch_event_high_water, None);
        assert_eq!(decoded.watch_replay_floors, None);
        assert!(decoded.operations.is_empty());
    }
}
