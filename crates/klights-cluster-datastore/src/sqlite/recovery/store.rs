//! Canonical SQLite recovery, metadata, capture, and restore ports.

use std::collections::HashSet;
use std::sync::Arc;

use klights_cluster_core::{ClusterMembership, ClusterMetadata};
use klights_cluster_store::{
    AuthoritativeSnapshot, AuthoritativeSnapshotCapture, AuthoritativeSnapshotPersistence,
    ClusterMetadataFuture, ClusterMetadataRead, ClusterMetadataStoreError, DurableReplayTarget,
    OutboxResponseCodec, PersistedClusterMetadata, SnapshotCaptureHeader, SnapshotCapturePage,
    SnapshotCapturePageKind, SnapshotCaptureRequest, SnapshotCaptureSession,
    SnapshotMembership as CanonicalSnapshotMembership, SnapshotPersistenceError,
    SnapshotPersistenceFuture,
};
use klights_supervisor::DbExecutor;
use rusqlite::OptionalExtension;

use super::{
    SnapshotMembership, SnapshotMetadata, SnapshotReplayFloor, SqliteSnapshotFactory,
    replace_resource_state_in_conn,
};
use crate::sqlite::live_apply::TransactionContext;

/// SQLite owner of canonical recovery and cluster-metadata capabilities.
#[derive(Clone)]
pub struct SqliteRecoveryStore {
    executor: DbExecutor,
    read_executor: DbExecutor,
    snapshot_factory: Option<SqliteSnapshotFactory>,
    snapshot_fence: Arc<tokio::sync::RwLock<()>>,
    outbox_codec: Arc<dyn OutboxResponseCodec>,
}

impl SqliteRecoveryStore {
    pub fn new(
        executor: DbExecutor,
        read_executor: DbExecutor,
        snapshot_factory: Option<SqliteSnapshotFactory>,
        snapshot_fence: Arc<tokio::sync::RwLock<()>>,
        outbox_codec: Arc<dyn OutboxResponseCodec>,
    ) -> Self {
        Self {
            executor,
            read_executor,
            snapshot_factory,
            snapshot_fence,
            outbox_codec,
        }
    }

    async fn restore_canonical_snapshot(
        &self,
        snapshot: AuthoritativeSnapshot,
    ) -> Result<(), SnapshotPersistenceError> {
        let mut parts = snapshot.into_parts();
        let current_rv = parts.current_rv();
        let position = parts.position();
        let command_codec_activation_version = parts.command_codec_activation_version();
        let operations = parts.take_operations();
        let replay_floors = parts.take_replay_floors();
        let (metadata, membership) = parts.into_metadata_and_membership();
        let watch_event_high_water = position.map(|position| position.event_id);
        let replay_floors = replay_floors.map(|floors| {
            floors
                .into_iter()
                .map(|floor| {
                    let (target, floor_resource_version, floor_event_id, position_is_exact) =
                        floor.into_parts();
                    let (api_version, kind, namespace_key) = match target {
                        DurableReplayTarget::All => {
                            ("*".to_string(), "*".to_string(), "*".to_string())
                        }
                        DurableReplayTarget::Cluster { api_version, kind } => {
                            (api_version, kind, "#cluster".to_string())
                        }
                        DurableReplayTarget::Namespaced {
                            api_version,
                            kind,
                            namespace,
                        } => (api_version, kind, namespace),
                    };
                    SnapshotReplayFloor {
                        api_version,
                        kind,
                        namespace_key,
                        floor_resource_version,
                        floor_event_id,
                        position_is_exact,
                    }
                })
                .collect()
        });
        let metadata = SnapshotMetadata {
            cluster_id: metadata
                .as_ref()
                .map(|metadata| metadata.cluster_id.clone())
                .unwrap_or_default(),
            leader_epoch: metadata
                .as_ref()
                .map_or(0, |metadata| metadata.leader_epoch),
            membership: match membership {
                CanonicalSnapshotMembership::LegacyOmitted => SnapshotMembership::LegacyOmitted,
                CanonicalSnapshotMembership::AuthoritativeAbsent => {
                    SnapshotMembership::AuthoritativeAbsent
                }
                CanonicalSnapshotMembership::Present(value) => SnapshotMembership::Present(value),
            },
            command_codec_activation_version,
        };
        self.restore_snapshot_parts(
            operations,
            current_rv,
            watch_event_high_water,
            replay_floors,
            Some(metadata),
        )
        .await
    }

    /// Persist already-decoded SQLite recovery parts from a root-owned
    /// consensus envelope. This retains legacy envelopes that predate the
    /// complete canonical snapshot metadata contract.
    pub async fn restore_snapshot_parts(
        &self,
        operations: Vec<klights_cluster_core::SnapshotRestoreOperation>,
        current_rv: i64,
        watch_event_high_water: Option<i64>,
        replay_floors: Option<Vec<SnapshotReplayFloor>>,
        metadata: Option<SnapshotMetadata>,
    ) -> Result<(), SnapshotPersistenceError> {
        let outbox_codec = self.outbox_codec.clone();
        self.executor
            .call_raw("restore_authoritative_snapshot", move |connection| {
                let context = TransactionContext::new(outbox_codec.as_ref());
                replace_resource_state_in_conn(
                    connection,
                    operations,
                    current_rv,
                    watch_event_high_water,
                    replay_floors,
                    metadata,
                    &context,
                )
                .map(|_| ())
            })
            .await
            .map_err(anyhow::Error::new)
            .map_err(map_snapshot_persistence_error)
    }
}

impl AuthoritativeSnapshotPersistence for SqliteRecoveryStore {
    fn restore_authoritative_snapshot(
        &self,
        snapshot: AuthoritativeSnapshot,
    ) -> SnapshotPersistenceFuture<'_> {
        Box::pin(self.restore_canonical_snapshot(snapshot))
    }
}

impl AuthoritativeSnapshotCapture for SqliteRecoveryStore {
    fn begin_capture(
        &self,
        request: SnapshotCaptureRequest,
    ) -> SnapshotPersistenceFuture<'_, Box<dyn SnapshotCaptureSession>> {
        Box::pin(async move {
            let fence = klights_cluster_store::SnapshotExclusiveFence::new(
                self.snapshot_fence.clone().write_owned().await,
            );
            self.begin_capture_with_fence(request, fence).await
        })
    }

    fn begin_capture_with_fence(
        &self,
        request: SnapshotCaptureRequest,
        fence: klights_cluster_store::SnapshotExclusiveFence,
    ) -> SnapshotPersistenceFuture<'_, Box<dyn SnapshotCaptureSession>> {
        Box::pin(async move {
            let factory = self.snapshot_factory.as_ref().ok_or_else(|| {
                SnapshotPersistenceError::UnsupportedMode {
                    message: "pinned SQLite capture requires a snapshot-only disk lane".to_string(),
                }
            })?;
            let session = factory
                .begin_snapshot(request, fence)
                .await
                .map_err(map_snapshot_persistence_error)?;
            Ok(Box::new(NormalizingSnapshotCaptureSession::new(
                session,
                request.page_limit().get(),
            )) as Box<dyn SnapshotCaptureSession>)
        })
    }
}

impl ClusterMetadataRead for SqliteRecoveryStore {
    fn read_cluster_metadata(&self) -> ClusterMetadataFuture<'_, PersistedClusterMetadata> {
        Box::pin(async move {
            self.read_executor
                .call_raw("read_cluster_metadata", |connection| {
                    let transaction = connection.transaction()?;
                    let get = |key: &str| -> rusqlite::Result<Option<String>> {
                        transaction
                            .query_row(crate::sqlite::META_SELECT, [key], |row| row.get(0))
                            .optional()
                    };
                    let cluster_id = get(klights_cluster_store::CLUSTER_ID_META_KEY)?
                        .filter(|value| !value.is_empty())
                        .ok_or_else(|| {
                            crate::sqlite::live_apply::other_error("cluster_id is missing or empty")
                        })?;
                    let raw_epoch =
                        get(klights_cluster_store::LEADER_EPOCH_META_KEY)?.ok_or_else(|| {
                            crate::sqlite::live_apply::other_error("leader_epoch is missing")
                        })?;
                    let leader_epoch = raw_epoch.parse::<i64>().map_err(|_| {
                        crate::sqlite::live_apply::other_error(format!(
                            "invalid leader_epoch {raw_epoch:?}"
                        ))
                    })?;
                    let raw_rv: String = transaction.query_row(
                        "SELECT value FROM metadata WHERE key = 'resource_version'",
                        [],
                        |row| row.get(0),
                    )?;
                    let current_rv = raw_rv.parse::<i64>().map_err(|_| {
                        crate::sqlite::live_apply::other_error(format!(
                            "invalid resource_version metadata {raw_rv:?}"
                        ))
                    })?;
                    if leader_epoch < 0 || current_rv < 0 {
                        return Err(crate::sqlite::live_apply::other_error(
                            "cluster metadata numeric values must be non-negative",
                        ));
                    }
                    let raw_voters = get(klights_cluster_store::RAFT_VOTERS_META_KEY)?;
                    let raw_term = get(klights_cluster_store::RAFT_TERM_META_KEY)?;
                    let raw_hint = get(klights_cluster_store::RAFT_LEADER_HINT_META_KEY)?;
                    let membership = match (raw_voters, raw_term, raw_hint) {
                        (None, None, None) => CanonicalSnapshotMembership::AuthoritativeAbsent,
                        (Some(raw_voters), Some(raw_term), Some(raw_hint)) => {
                            let voters: Vec<String> =
                                serde_json::from_str(&raw_voters).map_err(|error| {
                                    crate::sqlite::live_apply::other_error(format!(
                                        "invalid voters metadata: {error}"
                                    ))
                                })?;
                            let term = raw_term.parse::<i64>().map_err(|_| {
                                crate::sqlite::live_apply::other_error(format!(
                                    "invalid raft term {raw_term:?}"
                                ))
                            })?;
                            let mut unique = HashSet::with_capacity(voters.len());
                            if term < 0
                                || voters.is_empty()
                                || voters
                                    .iter()
                                    .any(|voter| voter.is_empty() || !unique.insert(voter.as_str()))
                            {
                                return Err(crate::sqlite::live_apply::other_error(
                                    "membership contains an invalid term or voter set",
                                ));
                            }
                            CanonicalSnapshotMembership::Present(ClusterMembership {
                                cluster_id: cluster_id.clone(),
                                voters,
                                term,
                                leader_hint: (!raw_hint.is_empty()).then_some(raw_hint),
                            })
                        }
                        _ => {
                            return Err(crate::sqlite::live_apply::other_error(
                                "membership metadata is incomplete",
                            ));
                        }
                    };
                    transaction.commit()?;
                    Ok(PersistedClusterMetadata::new(
                        ClusterMetadata {
                            cluster_id,
                            leader_epoch,
                            current_rv,
                        },
                        membership,
                    ))
                })
                .await
                .map_err(anyhow::Error::new)
                .map_err(map_cluster_metadata_error)
        })
    }
}

/// Coalesces physical SQLite commit families into bounded canonical pages.
struct NormalizingSnapshotCaptureSession {
    inner: Box<dyn SnapshotCaptureSession>,
    buffered: Option<SnapshotCapturePage>,
    page_limit: usize,
}

impl NormalizingSnapshotCaptureSession {
    fn new(inner: Box<dyn SnapshotCaptureSession>, page_limit: usize) -> Self {
        Self {
            inner,
            buffered: None,
            page_limit,
        }
    }

    async fn next_normalized_page(
        &mut self,
    ) -> Result<Option<SnapshotCapturePage>, SnapshotPersistenceError> {
        let Some(first) = (match self.buffered.take() {
            Some(page) => Some(page),
            None => self.inner.next_page().await?,
        }) else {
            return Ok(None);
        };
        if first.kind() != SnapshotCapturePageKind::Commits {
            return Ok(Some(first));
        }

        let mut operations = first
            .into_operations()
            .expect("commit page kind must contain snapshot restore operations");
        while operations.len() < self.page_limit {
            let Some(next) = self.inner.next_page().await? else {
                break;
            };
            if next.kind() != SnapshotCapturePageKind::Commits {
                self.buffered = Some(next);
                break;
            }
            let remaining = self.page_limit - operations.len();
            let mut next_operations = next
                .into_operations()
                .expect("commit page kind must contain snapshot restore operations");
            if next_operations.len() <= remaining {
                operations.append(&mut next_operations);
                continue;
            }
            let remainder = next_operations.split_off(remaining);
            operations.append(&mut next_operations);
            self.buffered = Some(SnapshotCapturePage::try_operations(remainder)?);
            break;
        }
        Ok(Some(SnapshotCapturePage::try_operations(operations)?))
    }
}

impl SnapshotCaptureSession for NormalizingSnapshotCaptureSession {
    fn header(&self) -> &SnapshotCaptureHeader {
        self.inner.header()
    }

    fn next_page(&mut self) -> SnapshotPersistenceFuture<'_, Option<SnapshotCapturePage>> {
        Box::pin(self.next_normalized_page())
    }

    fn cancel(&mut self) -> SnapshotPersistenceFuture<'_> {
        self.buffered = None;
        self.inner.cancel()
    }
}

fn map_snapshot_persistence_error(error: anyhow::Error) -> SnapshotPersistenceError {
    if let Some(error) = error.downcast_ref::<SnapshotPersistenceError>() {
        return error.clone();
    }
    let message = format!("{error:#}");
    let lower = message.to_ascii_lowercase();
    if lower.contains("corrupt") || lower.contains("decode") || lower.contains("invalid") {
        SnapshotPersistenceError::CorruptData { message }
    } else if lower.contains("unsupported")
        || lower.contains("does not support")
        || lower.contains("does not implement")
    {
        SnapshotPersistenceError::UnsupportedMode { message }
    } else if lower.contains("cancel") {
        SnapshotPersistenceError::Cancelled
    } else if lower.contains("timeout") || lower.contains("timed out") {
        SnapshotPersistenceError::Timeout
    } else if lower.contains("busy") || lower.contains("locked") {
        SnapshotPersistenceError::Retryable { message }
    } else {
        SnapshotPersistenceError::persistence_failed(message)
    }
}

fn map_cluster_metadata_error(error: anyhow::Error) -> ClusterMetadataStoreError {
    let message = format!("{error:#}");
    let lower = message.to_ascii_lowercase();
    if lower.contains("incomplete") || lower.contains("missing") || lower.contains("empty") {
        ClusterMetadataStoreError::Incomplete { message }
    } else if lower.contains("invalid")
        || lower.contains("malformed")
        || lower.contains("duplicate")
    {
        ClusterMetadataStoreError::CorruptData { message }
    } else if lower.contains("cancel") {
        ClusterMetadataStoreError::Cancelled
    } else if lower.contains("timeout") || lower.contains("timed out") {
        ClusterMetadataStoreError::Timeout
    } else if lower.contains("busy") || lower.contains("locked") {
        ClusterMetadataStoreError::Retryable { message }
    } else {
        ClusterMetadataStoreError::persistence_failed(message)
    }
}
