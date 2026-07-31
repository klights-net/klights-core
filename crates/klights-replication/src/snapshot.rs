//! Phase 3 Raft snapshot envelope and builder.
//!
//! openraft drives `RaftSnapshotBuilder::build_snapshot` on the leader
//! (and on followers that fall too far behind log retention) to package
//! the current state-machine view into a single transferable blob. The
//! follower receives the bytes via `RaftStateMachine::install_snapshot`
//! and atomically replays them, then resumes the log from the snapshot's
//! `last_log_id`.
//!
//! Persistence is injected through the canonical cluster-store capture and
//! authoritative-restore capabilities. The private envelope remains the
//! embedded OpenRaft wire owner.

use std::io::Cursor;
use std::io::Write;
use std::sync::Arc;

use anyhow::Result;
use klights_cluster_store::{
    AuthoritativeSnapshot, AuthoritativeSnapshotCapture, AuthoritativeSnapshotPersistence,
    BackendLifecycleStore, DurableAllocatorRead, DurableReplayFloor, DurableReplayTarget,
    SnapshotCaptureHeader, SnapshotCapturePage, SnapshotCapturePageKind, SnapshotMembership,
    SnapshotPersistenceError,
};
use klights_node_store::RaftAppliedStateDurability;
use openraft::storage::RaftSnapshotBuilder;
use openraft::{
    AnyError, LogId, Snapshot, SnapshotMeta, StorageError, StorageIOError, StoredMembership,
};
use serde::{Deserialize, Serialize};

use crate::types::{RaftMemberNode, TypeConfig};
use klights_cluster_core::NodeId;

/// Self-describing snapshot envelope. Carries the `last_applied`
/// log-id, the membership configuration, and an ordered list of
/// `LogApplyCommit` rows that, when replayed via
/// `DatastoreBackend::apply_log_apply_commit`, reconstruct the cluster
/// data state.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct RaftSnapshotData {
    pub last_applied: Option<LogId<NodeId>>,
    pub membership: StoredMembership<NodeId, RaftMemberNode>,
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
    pub watch_replay_floors: Option<Vec<RaftSnapshotReplayFloor>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster_metadata: Option<klights_cluster_core::ClusterMetadata>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster_membership: Option<RaftSnapshotMembership>,
    #[serde(default, alias = "commits", with = "snapshot_operations_serde")]
    pub operations: Vec<klights_cluster_core::SnapshotRestoreOperation>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct RaftSnapshotReplayFloor {
    pub api_version: String,
    pub kind: String,
    pub namespace_key: String,
    pub floor_resource_version: i64,
    pub floor_event_id: i64,
    #[serde(default)]
    pub position_is_exact: bool,
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
        crate::compressed::decode_json(bytes)
    }

    fn into_authoritative_snapshot(self) -> Result<AuthoritativeSnapshot> {
        let position =
            self.watch_event_high_water
                .map(|event_id| klights_cluster_core::WatchReplayPosition {
                    resource_version: self.current_rv,
                    event_id,
                    resource_version_filter_through_event_id: 0,
                });
        let floors = self
            .watch_replay_floors
            .map(|floors| {
                floors
                    .into_iter()
                    .map(|floor| {
                        let target = match (
                            floor.api_version.as_str(),
                            floor.kind.as_str(),
                            floor.namespace_key.as_str(),
                        ) {
                            ("*", "*", "*") => DurableReplayTarget::All,
                            (_, _, "#cluster") => DurableReplayTarget::Cluster {
                                api_version: floor.api_version,
                                kind: floor.kind,
                            },
                            _ => DurableReplayTarget::Namespaced {
                                api_version: floor.api_version,
                                kind: floor.kind,
                                namespace: floor.namespace_key,
                            },
                        };
                        DurableReplayFloor::new(
                            target,
                            floor.floor_resource_version,
                            floor.floor_event_id,
                            floor.position_is_exact,
                        )
                        .map_err(|error| anyhow::anyhow!(error.to_string()))
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .transpose()?;
        let membership = match self.cluster_membership {
            None => SnapshotMembership::LegacyOmitted,
            Some(RaftSnapshotMembership::AuthoritativeAbsent) => {
                SnapshotMembership::AuthoritativeAbsent
            }
            Some(RaftSnapshotMembership::Present(value)) => SnapshotMembership::Present(value),
        };
        AuthoritativeSnapshot::try_new_restore_envelope(
            self.operations,
            self.current_rv,
            position,
            floors,
            self.cluster_metadata,
            membership,
            self.command_codec_activation_version,
        )
        .map_err(anyhow::Error::new)
    }

    async fn serialize_from_backend_to_cursor_inner(
        capture: Arc<dyn AuthoritativeSnapshotCapture>,
        allocator: Arc<dyn DurableAllocatorRead>,
        lifecycle: Arc<dyn BackendLifecycleStore>,
        applied_state_source: RaftSnapshotAppliedStateSource,
        supervisor: Arc<klights_supervisor::TaskSupervisor>,
    ) -> Result<(
        Cursor<Vec<u8>>,
        Option<LogId<NodeId>>,
        StoredMembership<NodeId, RaftMemberNode>,
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
        let session = capture.begin_capture_with_fence(request, fence).await;
        let (mut session, captured) = match session {
            Ok(session) => (session, captured),
            Err(error) => {
                let _fence = lifecycle
                    .acquire_snapshot_exclusive_fence()
                    .await?
                    .ok_or_else(|| {
                        anyhow::anyhow!("backend does not provide a snapshot capture fence")
                    })?;
                let captured = applied_state_source.load().await?;
                if !is_pristine_raft_state(&captured) || !is_missing_cluster_identity_error(&error)
                {
                    return Err(anyhow::Error::new(error));
                }
                let allocator = allocator.read_allocator_state().await?;
                let position = allocator.position();
                if position.resource_version != 0
                    || position.event_id != 0
                    || position.resource_version_filter_through_event_id != 0
                {
                    return Err(anyhow::Error::new(error));
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

fn is_missing_cluster_identity_error(error: &SnapshotPersistenceError) -> bool {
    error.to_string() == "cluster_id is missing"
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
                crate::compressed::encode(&serde_json::to_vec(&data)?)
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
    let mut framed = vec![crate::compressed::TAG_ZSTD];
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
    Durable(Arc<dyn RaftAppliedStateDurability>),
}

type CapturedRaftAppliedState = (
    Option<LogId<NodeId>>,
    StoredMembership<NodeId, RaftMemberNode>,
);

impl RaftSnapshotAppliedStateSource {
    async fn load(&self) -> Result<CapturedRaftAppliedState, SnapshotPersistenceError> {
        let (last_applied, membership) = match self {
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
        self.write_json(&RaftSnapshotReplayFloor {
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
    capture: Arc<dyn AuthoritativeSnapshotCapture>,
    allocator: Arc<dyn DurableAllocatorRead>,
    lifecycle: Arc<dyn BackendLifecycleStore>,
    applied_state: Arc<dyn RaftAppliedStateDurability>,
    supervisor: Arc<klights_supervisor::TaskSupervisor>,
}

impl SqliteRaftSnapshotBuilder {
    pub fn new(
        capture: Arc<dyn AuthoritativeSnapshotCapture>,
        allocator: Arc<dyn DurableAllocatorRead>,
        lifecycle: Arc<dyn BackendLifecycleStore>,
        applied_state: Arc<dyn RaftAppliedStateDurability>,
        supervisor: Arc<klights_supervisor::TaskSupervisor>,
    ) -> Self {
        Self {
            capture,
            allocator,
            lifecycle,
            applied_state,
            supervisor,
        }
    }
}

impl RaftSnapshotBuilder<TypeConfig> for SqliteRaftSnapshotBuilder {
    async fn build_snapshot(
        &mut self,
    ) -> std::result::Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let (snapshot, last_applied, membership) =
            RaftSnapshotData::serialize_from_backend_to_cursor_inner(
                self.capture.clone(),
                self.allocator.clone(),
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

pub struct RaftSnapshotRestoreAdapter {
    persistence: Arc<dyn AuthoritativeSnapshotPersistence>,
    supervisor: Arc<klights_supervisor::TaskSupervisor>,
}

impl RaftSnapshotRestoreAdapter {
    pub fn new(
        persistence: Arc<dyn AuthoritativeSnapshotPersistence>,
        supervisor: Arc<klights_supervisor::TaskSupervisor>,
    ) -> Self {
        Self {
            persistence,
            supervisor,
        }
    }
}

#[async_trait::async_trait]
impl crate::state_machine::RaftSnapshotRestore for RaftSnapshotRestoreAdapter {
    async fn restore_snapshot(
        &self,
        snapshot_bytes: Vec<u8>,
    ) -> Result<(), SnapshotPersistenceError> {
        let snapshot =
            self.supervisor
                .run_blocking(
                    klights_supervisor::TaskCategory::Others,
                    "raft-snapshot-json-zstd-decode",
                    move || {
                        let data = RaftSnapshotData::deserialize_from_bytes(&snapshot_bytes)
                            .map_err(|error| SnapshotPersistenceError::PersistenceFailed {
                                message: error.to_string(),
                            })?;
                        data.into_authoritative_snapshot().map_err(|error| {
                            SnapshotPersistenceError::CorruptData {
                                message: error.to_string(),
                            }
                        })
                    },
                )
                .await
                .map_err(|error| SnapshotPersistenceError::PersistenceFailed {
                    message: error.to_string(),
                })?;
        let snapshot = snapshot?;
        self.persistence
            .restore_authoritative_snapshot(snapshot)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use klights_cluster_store::{
        AllocatorStateFuture, DurableAllocatorState, SnapshotCaptureRequest,
        SnapshotCaptureSession, SnapshotPersistenceFuture,
    };
    use klights_node_store::{
        EncodedRaftAppliedState, RaftAppliedStateWrite, RaftDurabilityFuture,
    };
    use openraft::storage::RaftSnapshotBuilder;

    struct FakeCapture {
        result: Mutex<
            Option<std::result::Result<Box<dyn SnapshotCaptureSession>, SnapshotPersistenceError>>,
        >,
    }

    impl FakeCapture {
        fn success(header: SnapshotCaptureHeader, pages: Vec<SnapshotCapturePage>) -> Self {
            Self {
                result: Mutex::new(Some(Ok(Box::new(FakeSession {
                    header,
                    pages: pages.into(),
                })))),
            }
        }

        fn missing_identity() -> Self {
            Self {
                result: Mutex::new(Some(Err(SnapshotPersistenceError::CorruptData {
                    message: "cluster_id is missing".to_string(),
                }))),
            }
        }

        fn take_result(
            &self,
        ) -> std::result::Result<Box<dyn SnapshotCaptureSession>, SnapshotPersistenceError>
        {
            self.result.lock().unwrap().take().unwrap()
        }
    }

    impl AuthoritativeSnapshotCapture for FakeCapture {
        fn begin_capture(
            &self,
            _request: SnapshotCaptureRequest,
        ) -> SnapshotPersistenceFuture<'_, Box<dyn SnapshotCaptureSession>> {
            let result = self.take_result();
            Box::pin(async move { result })
        }

        fn begin_capture_with_fence(
            &self,
            _request: SnapshotCaptureRequest,
            _fence: klights_cluster_store::SnapshotExclusiveFence,
        ) -> SnapshotPersistenceFuture<'_, Box<dyn SnapshotCaptureSession>> {
            let result = self.take_result();
            Box::pin(async move { result })
        }
    }

    struct FakeSession {
        header: SnapshotCaptureHeader,
        pages: VecDeque<SnapshotCapturePage>,
    }

    impl SnapshotCaptureSession for FakeSession {
        fn header(&self) -> &SnapshotCaptureHeader {
            &self.header
        }

        fn next_page(&mut self) -> SnapshotPersistenceFuture<'_, Option<SnapshotCapturePage>> {
            let page = self.pages.pop_front();
            Box::pin(async move { Ok(page) })
        }

        fn cancel(&mut self) -> SnapshotPersistenceFuture<'_> {
            Box::pin(async { Ok(()) })
        }
    }

    struct FakeAllocator(DurableAllocatorState);

    impl FakeAllocator {
        fn at(resource_version: i64, event_id: i64) -> Self {
            Self(
                DurableAllocatorState::try_new(klights_cluster_core::WatchReplayPosition {
                    resource_version,
                    event_id,
                    resource_version_filter_through_event_id: 0,
                })
                .unwrap(),
            )
        }
    }

    impl DurableAllocatorRead for FakeAllocator {
        fn read_allocator_state(&self) -> AllocatorStateFuture<'_, DurableAllocatorState> {
            let state = self.0;
            Box::pin(async move { Ok(state) })
        }
    }

    struct FakeLifecycle;

    #[async_trait::async_trait]
    impl BackendLifecycleStore for FakeLifecycle {
        async fn acquire_snapshot_exclusive_fence(
            &self,
        ) -> Result<Option<klights_cluster_store::SnapshotExclusiveFence>> {
            Ok(Some(klights_cluster_store::SnapshotExclusiveFence::new(())))
        }

        async fn acquire_snapshot_mutation_fence(
            &self,
        ) -> Result<Option<klights_cluster_store::SnapshotMutationFence>> {
            Ok(Some(klights_cluster_store::SnapshotMutationFence::new(())))
        }

        fn close(&self) {}
    }

    struct FixedAppliedState {
        state: EncodedRaftAppliedState,
    }

    impl RaftAppliedStateDurability for FixedAppliedState {
        fn load_applied_state(&self) -> RaftDurabilityFuture<'_, EncodedRaftAppliedState> {
            let state = self.state.clone();
            Box::pin(async move { Ok(state) })
        }

        fn store_applied_state(
            &self,
            _state: RaftAppliedStateWrite,
        ) -> RaftDurabilityFuture<'_, ()> {
            Box::pin(async { Ok(()) })
        }
    }

    #[derive(Default)]
    struct RecordingPersistence {
        snapshot: Mutex<Option<AuthoritativeSnapshot>>,
    }

    impl AuthoritativeSnapshotPersistence for RecordingPersistence {
        fn restore_authoritative_snapshot(
            &self,
            snapshot: AuthoritativeSnapshot,
        ) -> SnapshotPersistenceFuture<'_> {
            *self.snapshot.lock().unwrap() = Some(snapshot);
            Box::pin(async { Ok(()) })
        }
    }

    fn supervisor() -> Arc<klights_supervisor::TaskSupervisor> {
        Arc::new(klights_supervisor::TaskSupervisor::new(Default::default()))
    }

    fn captured_header() -> SnapshotCaptureHeader {
        SnapshotCaptureHeader::try_new(
            Some(3),
            klights_cluster_core::WatchReplayPosition {
                resource_version: 1,
                event_id: 2,
                resource_version_filter_through_event_id: 0,
            },
            klights_cluster_core::ClusterMetadata {
                cluster_id: "snapshot-cluster".to_string(),
                leader_epoch: 4,
                current_rv: 1,
            },
            SnapshotMembership::AuthoritativeAbsent,
        )
        .unwrap()
    }

    fn captured_pages() -> Vec<SnapshotCapturePage> {
        vec![
            SnapshotCapturePage::try_operations(vec![
                klights_cluster_core::SnapshotRestoreOperation::new(
                    1,
                    None,
                    vec![klights_cluster_core::LogApplyMutation::PutNamespace(
                        klights_cluster_core::LogApplyNamespaceRow {
                            name: "captured".to_string(),
                            uid: "captured-uid".to_string(),
                            resource_version: 1,
                            data: serde_json::json!({
                                "apiVersion": "v1",
                                "kind": "Namespace",
                                "metadata": {"name": "captured", "uid": "captured-uid"}
                            }),
                        },
                    )],
                ),
            ])
            .unwrap(),
            SnapshotCapturePage::try_replay_floors(vec![
                DurableReplayFloor::all(1, 2, true).unwrap(),
            ])
            .unwrap(),
        ]
    }

    #[tokio::test]
    async fn builder_serializes_canonical_capture_without_changing_positions() {
        let mut builder = SqliteRaftSnapshotBuilder::new(
            Arc::new(FakeCapture::success(captured_header(), captured_pages())),
            Arc::new(FakeAllocator::at(1, 2)),
            Arc::new(FakeLifecycle),
            Arc::new(FixedAppliedState {
                state: EncodedRaftAppliedState::new(None, None),
            }),
            supervisor(),
        );

        let snapshot = builder.build_snapshot().await.unwrap();
        let decoded =
            RaftSnapshotData::deserialize_from_bytes(snapshot.snapshot.get_ref()).unwrap();
        assert_eq!(decoded.current_rv, 1);
        assert_eq!(decoded.watch_event_high_water, Some(2));
        assert_eq!(decoded.command_codec_activation_version, Some(3));
        assert_eq!(
            decoded
                .cluster_metadata
                .as_ref()
                .map(|metadata| metadata.cluster_id.as_str()),
            Some("snapshot-cluster")
        );
        assert_eq!(decoded.operations.len(), 1);
        assert_eq!(decoded.watch_replay_floors.as_ref().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn pristine_snapshot_is_identity_free_only_at_zero_allocators() {
        let mut builder = SqliteRaftSnapshotBuilder::new(
            Arc::new(FakeCapture::missing_identity()),
            Arc::new(FakeAllocator::at(0, 0)),
            Arc::new(FakeLifecycle),
            Arc::new(FixedAppliedState {
                state: EncodedRaftAppliedState::new(None, None),
            }),
            supervisor(),
        );
        let snapshot = builder.build_snapshot().await.unwrap();
        let decoded =
            RaftSnapshotData::deserialize_from_bytes(snapshot.snapshot.get_ref()).unwrap();
        assert_eq!(decoded.current_rv, 0);
        assert_eq!(decoded.watch_event_high_water, Some(0));
        assert_eq!(decoded.cluster_metadata, None);
        assert_eq!(decoded.cluster_membership, None);
        assert!(decoded.operations.is_empty());

        let mut non_pristine = SqliteRaftSnapshotBuilder::new(
            Arc::new(FakeCapture::missing_identity()),
            Arc::new(FakeAllocator::at(1, 0)),
            Arc::new(FakeLifecycle),
            Arc::new(FixedAppliedState {
                state: EncodedRaftAppliedState::new(None, None),
            }),
            supervisor(),
        );
        assert!(non_pristine.build_snapshot().await.is_err());
    }

    #[tokio::test]
    async fn restore_adapter_maps_private_wire_to_canonical_persistence() {
        let persistence = Arc::new(RecordingPersistence::default());
        let adapter = RaftSnapshotRestoreAdapter::new(persistence.clone(), supervisor());
        let data = RaftSnapshotData {
            last_applied: None,
            membership: StoredMembership::default(),
            current_rv: 1,
            command_codec_activation_version: Some(3),
            watch_event_high_water: Some(2),
            watch_replay_floors: Some(vec![RaftSnapshotReplayFloor {
                api_version: "*".to_string(),
                kind: "*".to_string(),
                namespace_key: "*".to_string(),
                floor_resource_version: 1,
                floor_event_id: 2,
                position_is_exact: true,
            }]),
            cluster_metadata: Some(klights_cluster_core::ClusterMetadata {
                cluster_id: "snapshot-cluster".to_string(),
                leader_epoch: 4,
                current_rv: 1,
            }),
            cluster_membership: Some(RaftSnapshotMembership::AuthoritativeAbsent),
            operations: captured_pages()
                .into_iter()
                .next()
                .unwrap()
                .into_operations()
                .unwrap(),
        };
        let encoded = crate::compressed::encode(&serde_json::to_vec(&data).unwrap()).unwrap();

        crate::state_machine::RaftSnapshotRestore::restore_snapshot(&adapter, encoded)
            .await
            .unwrap();

        let restored = persistence.snapshot.lock().unwrap().take().unwrap();
        assert_eq!(restored.current_rv(), 1);
        assert_eq!(restored.command_codec_activation_version(), Some(3));
        assert_eq!(
            restored
                .metadata()
                .map(|metadata| metadata.cluster_id.as_str()),
            Some("snapshot-cluster")
        );
        assert!(matches!(
            restored.membership(),
            SnapshotMembership::AuthoritativeAbsent
        ));
    }

    #[test]
    fn legacy_snapshot_alias_and_missing_allocator_fields_remain_decodable() {
        let legacy = serde_json::json!({
            "last_applied": null,
            "membership": StoredMembership::<NodeId, RaftMemberNode>::default(),
            "current_rv": 7,
            "commits": []
        });
        let framed = crate::compressed::encode(&serde_json::to_vec(&legacy).unwrap()).unwrap();

        let decoded = RaftSnapshotData::deserialize_from_bytes(&framed).unwrap();
        assert_eq!(decoded.current_rv, 7);
        assert_eq!(decoded.watch_event_high_water, None);
        assert_eq!(decoded.watch_replay_floors, None);
        assert!(decoded.operations.is_empty());
    }
}
