//! Passive SQLite cluster datastore implementation.
//!
//! Submodules (crud, schema, watch, etc.) reach back here for shared
//! types via `use super::*;` — the re-exports below make the `types::`
//! and `backend::` symbols visible to them.

#[cfg(test)]
mod applier;
mod cluster_replace;
mod crud;
mod focused_ports;
mod gc;
mod merge_patch;
mod outbox_codec;
mod rv_helpers;
mod watch;

#[cfg(test)]
use crate::test_fixtures::replicated_create::ReplicatedCreateOptions;
use anyhow::{Result, anyhow};
use klights_supervisor::TaskSupervisor;
use klights_types::{NodeName, PodSubnet};
use rusqlite::{OptionalExtension, TransactionBehavior};
use serde_json::Value;
use std::net::Ipv4Addr;

impl std::fmt::Debug for Datastore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Datastore").finish_non_exhaustive()
    }
}

// Re-export lower-owned values so `use super::*;` in cohesive persistence
// submodules stays allocation- and representation-neutral.
use klights_cluster_core::{
    LogApplyAppliedOutboxRow, LogApplyPodCleanupIntentRow, PatchKind, Resource,
    ResourceBatchOperation, ResourcePatchRequest, ResourcePreconditions, WatchReplayPosition,
};
#[cfg(any(test, feature = "test-support"))]
use klights_cluster_store::CommitObservationSink;
use klights_cluster_store::OutboxResponseCodec;
pub use klights_cluster_store::StagedPostCommit;
pub use klights_cluster_store::{
    CatchUpResource, ClusterMetadataObservation, DurableAllocatorObservation, ListPageRequest,
    PositionedWatchReplay, PositionedWatchReplayRead, ReplicatedMembershipState,
    ReplicatedSnapshotMetadata, ResourceList, ResourceListOptions, SnapshotAtRv, WatchTarget,
    WatchTargetScope,
};

struct AppliedOutboxLedgerInput<'a> {
    idempotency_key: String,
    subject_key: String,
    operation: String,
    first_seen_ms: i64,
    status_stamp: Option<i64>,
    terminal_error: Option<&'a klights_cluster_core::OutboxApplyError>,
}

use self::mutation_queries as queries;
use crate::sqlite::SqliteReadStore;
use crate::sqlite::live_apply;
use crate::sqlite::mutation_diagnostics;
use crate::sqlite::mutation_queries;
use crate::sqlite::ordinary;
#[cfg(test)]
use crate::sqlite::owner_ref_index;
use crate::sqlite::resource_shape;
use crate::sqlite::transaction_primitives;
use klights_supervisor::DbExecutor;
use klights_supervisor::sqlite_open as opener;
pub use watch::create_staged_post_commit;

fn focused_watch_targets(
    targets: &[WatchTarget],
) -> Vec<klights_cluster_store::DurableWatchTarget> {
    targets
        .iter()
        .map(|target| match &target.scope {
            WatchTargetScope::Cluster => klights_cluster_store::DurableWatchTarget::cluster(
                &target.api_version,
                &target.kind,
            ),
            WatchTargetScope::Namespaced(None) => {
                klights_cluster_store::DurableWatchTarget::namespaced(
                    &target.api_version,
                    &target.kind,
                )
            }
            WatchTargetScope::Namespaced(Some(namespace)) => {
                klights_cluster_store::DurableWatchTarget::namespaced_in_namespace(
                    &target.api_version,
                    &target.kind,
                    namespace,
                )
            }
        })
        .collect()
}

fn focused_events_to_catchup(
    events: Vec<klights_cluster_store::DurableWatchEvent>,
) -> Vec<CatchUpResource> {
    events
        .into_iter()
        .map(|event| {
            let event_type = event.event_type().to_string();
            CatchUpResource {
                resource: event.into_resource(),
                event_type: std::borrow::Cow::Owned(event_type),
            }
        })
        .collect()
}

impl Datastore {
    pub async fn snapshot_resources_at_rv(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        query: ResourceListOptions<'_>,
        snapshot_rv: i64,
    ) -> Result<SnapshotAtRv> {
        let snapshot = klights_cluster_store::ResourceListSnapshot::try_new(
            WatchReplayPosition::from_resource_version(snapshot_rv),
        )
        .map_err(anyhow::Error::new)?;
        let continuation = query.continue_token.map(|name| {
            klights_cluster_store::ResourceContinuation::new(
                klights_cluster_store::ResourceCollectionKey::new(
                    namespace.map(str::to_string),
                    name.to_string(),
                ),
                snapshot,
            )
        });
        let focused_query = klights_cluster_store::ResourceListQuery::try_new_borrowed(
            query.label_selector,
            query.field_selector,
            query.limit,
            continuation,
            klights_cluster_store::ResourceVersionMatch::Exact(snapshot_rv),
        )
        .map_err(anyhow::Error::new)?;
        match self
            .focused_reads
            .snapshot_resources_at_rv(api_version, kind, namespace, focused_query, snapshot_rv)
            .await?
        {
            crate::sqlite::ExactSnapshotRead::Current => Ok(SnapshotAtRv::Current),
            crate::sqlite::ExactSnapshotRead::Expired { .. } => Ok(SnapshotAtRv::Expired),
            crate::sqlite::ExactSnapshotRead::List(page) => {
                let position = page.snapshot().position();
                let continue_token = page
                    .continuation()
                    .map(|cursor| cursor.after().name().to_string());
                let remaining_item_count = page.remaining_item_count();
                Ok(SnapshotAtRv::List(ResourceList {
                    items: page.into_items(),
                    resource_version: position.resource_version,
                    watch_replay_position: Some(position),
                    continue_token,
                    remaining_item_count,
                }))
            }
        }
    }

    pub async fn snapshot_resources_at_position(
        &self,
        targets: &[WatchTarget],
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        position: WatchReplayPosition,
    ) -> Result<SnapshotAtRv> {
        match self
            .focused_reads
            .snapshot_resources_at_position(
                &focused_watch_targets(targets),
                label_selector,
                field_selector,
                position,
            )
            .await?
        {
            klights_cluster_store::ResourceSnapshotRead::Current => Ok(SnapshotAtRv::Current),
            klights_cluster_store::ResourceSnapshotRead::Expired => Ok(SnapshotAtRv::Expired),
            klights_cluster_store::ResourceSnapshotRead::Historical(snapshot) => {
                let position = snapshot.snapshot().position();
                Ok(SnapshotAtRv::List(ResourceList {
                    items: snapshot.into_items(),
                    resource_version: position.resource_version,
                    watch_replay_position: Some(position),
                    continue_token: None,
                    remaining_item_count: None,
                }))
            }
        }
    }
}
#[cfg(any(test, feature = "test-support"))]
pub use watch::publish_pending;
#[cfg(any(test, feature = "test-support"))]
pub use watch::publish_pending_batch;

#[cfg(test)]
use crate::sqlite::filters::filter_by_field_selector;
#[cfg(test)]
use crate::sqlite::filters::parse_label_selector;
#[cfg(test)]
use crate::sqlite::filters::{resolve_field_path, split_selector};
use crate::sqlite::scope::use_namespaced_table;
use resource_shape::hydrate_watch_event_data;
use resource_shape::{
    ensure_metadata_create_defaults, ensure_metadata_identity, ensure_metadata_uid,
    ensure_pod_status_ip_arrays, ensure_resource_type_meta, metadata_uid,
    preserve_server_metadata_fields_from_existing, resource_client_owned_state_equal,
    validate_metadata_uid_immutable, validate_resource_preconditions,
    warn_uid_precondition_mismatch,
};

#[cfg(any(test, feature = "test-support"))]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ResourceMutationPauseOperation {
    MainUpdate,
    PatchLatest,
    BuildPatchCommand,
}

#[cfg(any(test, feature = "test-support"))]
pub struct ResourceMutationPause {
    reached: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}

#[cfg(any(test, feature = "test-support"))]
impl ResourceMutationPause {
    pub async fn wait_until_reached(&self) {
        self.reached.notified().await;
    }

    pub fn resume(&self) {
        self.resume.notify_one();
    }
}

#[cfg(any(test, feature = "test-support"))]
struct ResourceMutationPauseRegistration {
    operation: ResourceMutationPauseOperation,
    api_version: String,
    kind: String,
    namespace: Option<String>,
    name: String,
    pause: std::sync::Arc<ResourceMutationPause>,
}

#[derive(Clone)]
pub struct Datastore {
    executor: DbExecutor,
    read_executor: DbExecutor,
    focused_reads: std::sync::Arc<SqliteReadStore>,
    live_committed_apply: std::sync::Arc<crate::sqlite::live_apply::SqliteLiveCommittedApplyStore>,
    focused_recovery: std::sync::Arc<crate::sqlite::recovery::SqliteRecoveryStore>,
    #[cfg(any(test, feature = "test-support"))]
    #[cfg_attr(all(feature = "test-support", not(test)), allow(dead_code))]
    commit_sink: Option<std::sync::Arc<dyn CommitObservationSink>>,
    outbox_codec: std::sync::Arc<dyn OutboxResponseCodec>,
    wall_clock: std::sync::Arc<dyn klights_supervisor::WallClock>,
    snapshot_fence: std::sync::Arc<tokio::sync::RwLock<()>>,
    #[cfg(test)]
    post_commit_publish_pause:
        std::sync::Arc<std::sync::Mutex<Option<cluster_replace::PostCommitPublishPause>>>,
    #[cfg(any(test, feature = "test-support"))]
    resource_mutation_pause:
        std::sync::Arc<std::sync::Mutex<Option<ResourceMutationPauseRegistration>>>,
}

struct AtomicOutboxMutation {
    applied_rv: Option<i64>,
    result_proto: Vec<u8>,
    pending: Option<StagedPostCommit>,
    committed_resource: Option<klights_cluster_core::Resource>,
}

// The additive test payload intentionally makes StagedPostCommit much larger;
// boxing it would add an allocation to the production commit path.
#[allow(clippy::large_enum_variant)]
enum OutboxTxnOutcome {
    Applied {
        applied_rv: i64,
        pending: Option<StagedPostCommit>,
        resource_changed: bool,
        pod_endpoint_effect: klights_cluster_core::PodEndpointEffect,
        committed_resource: Option<klights_cluster_core::Resource>,
    },
    AlreadyApplied(Option<LogApplyAppliedOutboxRow>),
}

use klights_cluster_core::BuildOutboxOutcome;

enum BuildOutboxTxnOutcome {
    Built {
        commit: klights_cluster_core::LogApplyCommit,
        rv: i64,
        terminal_error: Option<klights_cluster_core::OutboxApplyError>,
    },
    AlreadyApplied(Option<LogApplyAppliedOutboxRow>),
}

impl Datastore {
    #[cfg(any(test, feature = "test-support"))]
    pub fn install_resource_mutation_pause(
        &self,
        operation: ResourceMutationPauseOperation,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> std::sync::Arc<ResourceMutationPause> {
        let pause = std::sync::Arc::new(ResourceMutationPause {
            reached: tokio::sync::Notify::new(),
            resume: tokio::sync::Notify::new(),
        });
        let registration = ResourceMutationPauseRegistration {
            operation,
            api_version: api_version.to_string(),
            kind: kind.to_string(),
            namespace: namespace.map(str::to_string),
            name: name.to_string(),
            pause: pause.clone(),
        };
        *self
            .resource_mutation_pause
            .lock()
            .expect("resource mutation pause mutex poisoned") = Some(registration);
        pause
    }

    #[cfg(any(test, feature = "test-support"))]
    async fn pause_resource_mutation_if_requested(
        &self,
        operation: ResourceMutationPauseOperation,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) {
        let pause = {
            let mut slot = self
                .resource_mutation_pause
                .lock()
                .expect("resource mutation pause mutex poisoned");
            if slot.as_ref().is_some_and(|registration| {
                registration.operation == operation
                    && registration.api_version == api_version
                    && registration.kind == kind
                    && registration.namespace.as_deref() == namespace
                    && registration.name == name
            }) {
                slot.take().map(|registration| registration.pause)
            } else {
                None
            }
        };
        if let Some(pause) = pause {
            pause.reached.notify_one();
            pause.resume.notified().await;
        }
    }

    fn completed_outbox_record_in_tx(
        tx: &rusqlite::Transaction<'_>,
        idempotency_key: &str,
    ) -> tokio_rusqlite::Result<Option<LogApplyAppliedOutboxRow>> {
        let existing = tx
            .query_row(queries::APPLIED_OUTBOX_GET, [idempotency_key], |row| {
                Ok(LogApplyAppliedOutboxRow {
                    idempotency_key: row.get(0)?,
                    subject_key: row.get(1)?,
                    operation: row.get(2)?,
                    first_seen_ms: row.get(3)?,
                    applied_rv: row.get(4)?,
                    result_proto: row.get(5)?,
                    status_stamp: row.get(6)?,
                })
            })
            .optional()?;
        Ok(existing)
    }

    fn outbox_materialization_resource_version_hint_in_tx(
        tx: &rusqlite::Transaction<'_>,
    ) -> tokio_rusqlite::Result<Option<i64>> {
        Ok(Some(
            Self::current_resource_version_in_tx(tx)?.saturating_add(1),
        ))
    }

    fn normalize_ledger_only_outbox_commit_in_tx(
        tx: &rusqlite::Transaction<'_>,
        commit: &klights_cluster_core::LogApplyCommit,
        rv: &mut i64,
    ) -> tokio_rusqlite::Result<bool> {
        let ledger_only = commit.mutations().is_empty();
        if ledger_only {
            *rv = Self::current_resource_version_in_tx(tx)?;
        }
        Ok(ledger_only)
    }

    fn append_applied_outbox_ledger_mutation(
        commit: &mut klights_cluster_core::LogApplyCommit,
        input: AppliedOutboxLedgerInput<'_>,
        context: &live_apply::TransactionContext<'_>,
    ) {
        use klights_cluster_core::StorageResponse;
        use klights_cluster_core::{
            ClusterMutation, LogApplyAppliedOutboxRow, OutboxLedgerMutation,
        };

        let response = input.terminal_error.map_or_else(
            || StorageResponse::Ack {
                resource_version: 0,
            },
            |error| StorageResponse::Error {
                message: error.to_string(),
            },
        );

        let replacement = klights_cluster_core::LogApplyCommit::try_new(Vec::new())
            .expect("empty live commit is valid");
        let previous = std::mem::replace(commit, replacement);
        let (_, watermark, mut mutations) = previous.into_parts();
        mutations.push(
            ClusterMutation::OutboxLedger(OutboxLedgerMutation::PutAppliedOutbox(
                LogApplyAppliedOutboxRow {
                    idempotency_key: input.idempotency_key,
                    subject_key: input.subject_key,
                    operation: input.operation,
                    first_seen_ms: input.first_seen_ms,
                    applied_rv: None,
                    result_proto: context.encode(&response).unwrap_or_default(),
                    status_stamp: input.status_stamp.filter(|stamp| *stamp > 0),
                },
            ))
            .into_log_apply_mutation(),
        );
        *commit =
            klights_cluster_core::LogApplyCommit::try_new_with_watermark(mutations, watermark)
                .expect("appended outbox ledger row is RV-zero");
    }

    fn author_live_commit(
        candidate_resource_version: i64,
        mut mutations: Vec<klights_cluster_core::LogApplyMutation>,
    ) -> tokio_rusqlite::Result<klights_cluster_core::LogApplyCommit> {
        fn clear_metadata_resource_version(data: &mut serde_json::Value) {
            if let Some(metadata) = data
                .pointer_mut("/metadata")
                .and_then(serde_json::Value::as_object_mut)
            {
                metadata.remove("resourceVersion");
            }
        }

        for mutation in &mut mutations {
            let observed = match mutation {
                klights_cluster_core::LogApplyMutation::PutResource(row) => {
                    clear_metadata_resource_version(&mut row.data);
                    let observed = row.resource_version;
                    row.resource_version = 0;
                    Some(observed)
                }
                klights_cluster_core::LogApplyMutation::PatchResourceLatest(row) => {
                    let observed = row.resource_version;
                    row.resource_version = 0;
                    Some(observed)
                }
                klights_cluster_core::LogApplyMutation::PutNamespace(row) => {
                    clear_metadata_resource_version(&mut row.data);
                    let observed = row.resource_version;
                    row.resource_version = 0;
                    Some(observed)
                }
                klights_cluster_core::LogApplyMutation::PutWatchEvent(row) => {
                    clear_metadata_resource_version(&mut row.data);
                    let observed = row.resource_version;
                    row.resource_version = 0;
                    Some(observed)
                }
                klights_cluster_core::LogApplyMutation::PutPodCleanupIntent(row) => {
                    let observed = row.resource_version;
                    row.resource_version = 0;
                    Some(observed)
                }
                klights_cluster_core::LogApplyMutation::PutAppliedOutbox(row) => {
                    row.applied_rv = None;
                    None
                }
                klights_cluster_core::LogApplyMutation::AdvanceResourceVersion {
                    resource_version,
                } => {
                    let observed = *resource_version;
                    *resource_version = 0;
                    Some(observed)
                }
                _ => None,
            };
            if observed.is_some_and(|value| value != 0 && value != candidate_resource_version) {
                return Err(live_apply::other_error(
                    "materialized mutation resourceVersion differs from its private candidate",
                ));
            }
        }
        klights_cluster_core::LogApplyCommit::try_new(mutations)
            .map_err(|error| live_apply::other_error(error.to_string()))
    }

    fn author_live_commit_from_cluster_mutations(
        candidate_resource_version: i64,
        mutations: Vec<klights_cluster_core::ClusterMutation>,
    ) -> tokio_rusqlite::Result<klights_cluster_core::LogApplyCommit> {
        Self::author_live_commit(
            candidate_resource_version,
            mutations
                .into_iter()
                .map(klights_cluster_core::ClusterMutation::into_log_apply_mutation)
                .collect(),
        )
    }

    fn set_live_commit_watermark(
        commit: &mut klights_cluster_core::LogApplyCommit,
        watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
    ) {
        let replacement = klights_cluster_core::LogApplyCommit::try_new(Vec::new())
            .expect("empty live commit is valid");
        let previous = std::mem::replace(commit, replacement);
        let (_, _, mutations) = previous.into_parts();
        *commit =
            klights_cluster_core::LogApplyCommit::try_new_with_watermark(mutations, watermark)
                .expect("watermarked live commit remains RV-zero");
    }

    fn is_bound_pod_finalization_delivery(
        command: &klights_cluster_core::command::StorageCommand,
        operation: &str,
    ) -> bool {
        use klights_cluster_core::OutboxOperation;
        use klights_cluster_core::command::StorageCommand;

        if operation != OutboxOperation::PodMetadata.as_str() {
            return false;
        }
        matches!(command, StorageCommand::FinalizeBoundPod { .. })
    }

    fn build_log_apply_commit_in_tx_from_command(
        tx: &rusqlite::Transaction<'_>,
        command: klights_cluster_core::command::StorageCommand,
        operation: &str,
        authoring_node: &str,
        resource_version_hint: Option<i64>,
        operation_now: chrono::DateTime<chrono::Utc>,
    ) -> tokio_rusqlite::Result<(klights_cluster_core::LogApplyCommit, i64)> {
        use crate::sqlite::mutation_helpers::serde_to_sqlite_error;
        use crate::sqlite::resource_shape::{
            ensure_metadata_create_defaults, ensure_metadata_identity, ensure_metadata_uid,
            ensure_pod_status_ip_arrays, ensure_resource_type_meta,
            preserve_server_metadata_fields_from_existing, validate_metadata_uid_immutable,
            validate_resource_preconditions,
        };
        use klights_cluster_core::command::StorageCommand;
        use klights_cluster_core::{
            ClusterMetaMutation, ClusterMutation, LogApplyNamespaceRow, LogApplyNodeDataplaneRow,
            LogApplyNodeSubnetAllocation, LogApplyPodCleanupIntentKey, LogApplyPodCleanupIntentRow,
            LogApplyResourceKey, LogApplyResourcePatch, LogApplyResourceRow, LogApplyWatchEventRow,
            NamespaceMutation, NetworkMutation, OutboxLedgerMutation, PodCleanupMutation,
            ResourceMutation, WatchHistoryMutation,
        };
        use klights_cluster_core::{
            ResourceBatchOperation, ResourceBatchPutMode, ResourcePreconditions,
        };
        use serde_json::Value;

        let mut rv = match resource_version_hint {
            Some(rv) => rv,
            None => Self::current_resource_version_in_tx(tx)?.saturating_add(1),
        };
        if let StorageCommand::AdvanceResourceVersion { new_rv, .. } = &command {
            rv = rv.max(*new_rv);
        }

        let commit = match command {
            StorageCommand::CreateResource {
                api_version,
                kind,
                namespace,
                name,
                mut data,
            } => {
                if api_version == "v1" && kind == "Namespace" {
                    if namespace.is_some() {
                        return Err(Self::sqlite_conversion_error(anyhow!(
                            "Namespace is cluster-scoped"
                        )));
                    }
                    let exists = tx
                        .query_row(queries::NAMESPACE_GET, rusqlite::params![&name], |_| Ok(()))
                        .optional()?
                        .is_some();
                    if exists {
                        return Err(Self::sqlite_conversion_error(anyhow!(
                            "namespaces \"{name}\" already exists (409 Conflict)"
                        )));
                    }
                    ensure_resource_type_meta(&mut data, "v1", "Namespace");
                    ensure_metadata_identity(&mut data, None, &name);
                    ensure_metadata_create_defaults(&mut data, operation_now);
                    let uid = ensure_metadata_uid(&mut data);
                    return Ok((
                        Self::author_live_commit_from_cluster_mutations(
                            rv,
                            vec![ClusterMutation::Namespace(NamespaceMutation::PutNamespace(
                                LogApplyNamespaceRow {
                                    name,
                                    uid,
                                    resource_version: rv,
                                    data,
                                },
                            ))],
                        )?,
                        rv,
                    ));
                }
                ensure_resource_type_meta(&mut data, &api_version, &kind);
                ensure_metadata_identity(&mut data, namespace.as_deref(), &name);
                ensure_metadata_create_defaults(&mut data, operation_now);
                ensure_pod_status_ip_arrays(&mut data, &api_version, &kind);
                if operation == klights_cluster_core::OutboxOperation::NodeRegistration.as_str()
                    && api_version == "v1"
                    && kind == "Node"
                    && namespace.is_none()
                    && name == authoring_node
                {
                    klights_cluster_core::set_node_external_ip_from_dataplane_annotation(&mut data);
                }
                let uid = ensure_metadata_uid(&mut data);
                Self::author_live_commit_from_cluster_mutations(
                    rv,
                    vec![ClusterMutation::Resource(ResourceMutation::PutResource(
                        LogApplyResourceRow {
                            api_version,
                            kind,
                            namespace,
                            name,
                            uid,
                            resource_version: rv,
                            data,
                            require_absent: true,
                            require_existing: false,
                            precondition_uid: None,
                            precondition_resource_version: None,
                            status_only: false,
                        },
                    ))],
                )
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
                if api_version == "v1" && kind == "Namespace" {
                    if namespace.is_some() {
                        return Err(Self::sqlite_conversion_error(anyhow!(
                            "Namespace is cluster-scoped"
                        )));
                    }
                    let (live_rv, live_uid, live_data) = tx
                        .query_row(queries::NAMESPACE_GET, rusqlite::params![&name], |row| {
                            Ok((
                                row.get::<_, i64>(1)?,
                                row.get::<_, String>(2)?,
                                row.get::<_, Vec<u8>>(3)?,
                            ))
                        })
                        .map_err(|error| match error {
                            rusqlite::Error::QueryReturnedNoRows => {
                                Self::sqlite_conversion_error(anyhow!("Namespace {name} not found"))
                            }
                            other => tokio_rusqlite::Error::Rusqlite(other),
                        })?;
                    let live: Value =
                        serde_json::from_slice(&live_data).map_err(serde_to_sqlite_error)?;
                    let mut effective_preconditions = preconditions.clone();
                    effective_preconditions.resource_version = preconditions
                        .resource_version
                        .or_else(|| (expected_rv > 0).then_some(expected_rv));
                    validate_resource_preconditions(
                        &effective_preconditions,
                        Some(&live_uid),
                        live_rv,
                    )
                    .map_err(Self::sqlite_conversion_error)?;
                    validate_metadata_uid_immutable(&data, &live)
                        .map_err(Self::sqlite_conversion_error)?;
                    ensure_resource_type_meta(&mut data, "v1", "Namespace");
                    ensure_metadata_identity(&mut data, None, &name);
                    preserve_server_metadata_fields_from_existing(&mut data, &live);
                    let uid = ensure_metadata_uid(&mut data);
                    return Ok((
                        Self::author_live_commit_from_cluster_mutations(
                            rv,
                            vec![ClusterMutation::Namespace(NamespaceMutation::PutNamespace(
                                LogApplyNamespaceRow {
                                    name,
                                    uid,
                                    resource_version: rv,
                                    data,
                                },
                            ))],
                        )?,
                        rv,
                    ));
                }
                let apply_against_latest = Self::should_apply_outbox_update_against_latest(
                    operation,
                    &api_version,
                    &kind,
                    namespace.as_deref(),
                    &name,
                    authoring_node,
                );
                let (live_rv, live_uid, live_data) = Self::resource_row_for_update_in_tx(
                    tx,
                    &api_version,
                    &kind,
                    namespace.as_deref(),
                    &name,
                )?;
                let live: Value =
                    serde_json::from_slice(&live_data).map_err(serde_to_sqlite_error)?;
                let mut effective_preconditions = preconditions.clone();
                let mut pod_metadata_rebased_against_latest = false;
                let mut status_rebased_against_latest = false;
                if !apply_against_latest
                    && operation == klights_cluster_core::OutboxOperation::PodMetadata.as_str()
                    && api_version == "v1"
                    && kind == "Pod"
                    && let Some(expected) = effective_preconditions
                        .resource_version
                        .or_else(|| (expected_rv > 0).then_some(expected_rv))
                    && expected != live_rv
                    && let Some(base) = Self::resource_snapshot_for_key_at_rv_in_tx(
                        tx,
                        &api_version,
                        &kind,
                        namespace.as_deref(),
                        &name,
                        expected,
                    )?
                    && metadata_uid(&base) == Some(live_uid.as_str())
                {
                    let uid_preconditions = ResourcePreconditions {
                        uid: preconditions.uid.clone(),
                        resource_version: None,
                    };
                    validate_resource_preconditions(&uid_preconditions, Some(&live_uid), live_rv)
                        .map_err(Self::sqlite_conversion_error)?;
                    if let Some(rebased) =
                        Self::rebase_stale_pod_metadata_update(&base, &data, &live)
                    {
                        data = rebased;
                        pod_metadata_rebased_against_latest = true;
                    } else {
                        return Ok((Self::author_live_commit(rv, Vec::new())?, rv));
                    }
                }
                if apply_against_latest {
                    let uid_preconditions = ResourcePreconditions {
                        uid: preconditions.uid.clone(),
                        resource_version: None,
                    };
                    validate_resource_preconditions(&uid_preconditions, Some(&live_uid), live_rv)
                        .map_err(Self::sqlite_conversion_error)?;
                    if api_version == "v1" && kind == "Node" && namespace.is_none() {
                        klights_cluster_core::merge_existing_node_mutable_fields(&mut data, &live);
                    } else if api_version == "coordination.k8s.io/v1"
                        && kind == "Lease"
                        && namespace.as_deref() == Some("kube-node-lease")
                    {
                        Self::merge_forwarded_lease_with_live(&live, &mut data);
                    }
                } else if !pod_metadata_rebased_against_latest {
                    effective_preconditions.resource_version = preconditions
                        .resource_version
                        .or_else(|| (expected_rv > 0).then_some(expected_rv));
                    if let Some(expected) = effective_preconditions.resource_version
                        && expected != live_rv
                        && klights_types::has_builtin_status_subresource(&api_version, &kind)
                        && let Some(base) = Self::resource_snapshot_for_key_at_rv_in_tx(
                            tx,
                            &api_version,
                            &kind,
                            namespace.as_deref(),
                            &name,
                            expected,
                        )?
                        && metadata_uid(&base) == Some(live_uid.as_str())
                        && resource_client_owned_state_equal(&base, &live)
                    {
                        effective_preconditions.resource_version = Some(live_rv);
                        status_rebased_against_latest = data.get("status") == base.get("status");
                    }
                    validate_resource_preconditions(
                        &effective_preconditions,
                        Some(&live_uid),
                        live_rv,
                    )
                    .map_err(Self::sqlite_conversion_error)?;
                }
                ensure_resource_type_meta(&mut data, &api_version, &kind);
                ensure_metadata_identity(&mut data, namespace.as_deref(), &name);
                ensure_pod_status_ip_arrays(&mut data, &api_version, &kind);
                if status_rebased_against_latest
                    || operation == klights_cluster_core::OutboxOperation::PodMetadata.as_str()
                {
                    klights_types::preserve_status_subresource_on_main_update(
                        &api_version,
                        &kind,
                        &live,
                        &mut data,
                    );
                }
                preserve_server_metadata_fields_from_existing(&mut data, &live);
                let uid = ensure_metadata_uid(&mut data);
                let precondition_resource_version =
                    if apply_against_latest || pod_metadata_rebased_against_latest {
                        None
                    } else {
                        effective_preconditions.resource_version
                    };
                Self::author_live_commit_from_cluster_mutations(
                    rv,
                    vec![ClusterMutation::Resource(ResourceMutation::PutResource(
                        LogApplyResourceRow {
                            api_version,
                            kind,
                            namespace,
                            name,
                            uid,
                            resource_version: rv,
                            data,
                            require_absent: false,
                            require_existing: true,
                            precondition_uid: preconditions.uid,
                            precondition_resource_version,
                            status_only: false,
                        },
                    ))],
                )
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
                let apply_against_latest = Self::should_apply_outbox_status_against_latest(
                    operation,
                    &api_version,
                    &kind,
                    &preconditions,
                    observed_status_stamp,
                );
                let (live_rv, live_uid, live_data) = Self::resource_row_for_update_in_tx(
                    tx,
                    &api_version,
                    &kind,
                    namespace.as_deref(),
                    &name,
                )?;
                if apply_against_latest {
                    let uid_preconditions = ResourcePreconditions {
                        uid: preconditions.uid.clone(),
                        resource_version: None,
                    };
                    validate_resource_preconditions(&uid_preconditions, Some(&live_uid), live_rv)
                        .map_err(Self::sqlite_conversion_error)?;
                    // Lost-update guard for pipelined status dispatch. The
                    // status outbox drops the live-RV precondition (so a slow
                    // status no longer stalls behind a newer RV), which
                    // reopened the classic "an older snapshot retried after a
                    // newer one applied clobbers it" race. Each worker stamps
                    // its status snapshots monotonically; the leader records
                    // the highest stamp applied per Pod subject and no-ops any
                    // snapshot whose stamp is older-or-equal. UID is already
                    // validated above, so same-name replacement Pods (distinct
                    // subject key) are unaffected.
                    if let Some(incoming_stamp) = observed_status_stamp {
                        let subject_key = Self::pod_status_subject_key(
                            &api_version,
                            &kind,
                            namespace.as_deref(),
                            &name,
                            preconditions.uid.as_deref(),
                        );
                        let last_applied_stamp: Option<i64> = tx.query_row(
                            queries::APPLIED_OUTBOX_MAX_STATUS_STAMP_FOR_SUBJECT,
                            rusqlite::params![subject_key],
                            |row| row.get::<_, Option<i64>>(0),
                        )?;
                        if klights_cluster_core::decide_status_stamp(
                            last_applied_stamp,
                            Some(incoming_stamp),
                        ) == klights_cluster_core::StatusStampDecision::RecordLedgerOnly
                        {
                            // Stale snapshot: produce a commit with no resource
                            // mutation so the live status is preserved and no
                            // watch event is emitted. The outer apply still
                            // records the idempotency ledger row so the worker
                            // row completes instead of retrying forever.
                            return Ok((Self::author_live_commit(rv, Vec::new())?, rv));
                        }
                    }
                } else {
                    validate_resource_preconditions(&preconditions, Some(&live_uid), live_rv)
                        .map_err(Self::sqlite_conversion_error)?;
                    if let Some(expected_rv) = expected_rv
                        && expected_rv > 0
                        && expected_rv != live_rv
                    {
                        return Err(Self::sqlite_conversion_error(anyhow!(
                            "resourceVersion precondition failed: expected {expected_rv} got {live_rv} (409 Conflict)"
                        )));
                    }
                }
                let mut live: Value =
                    serde_json::from_slice(&live_data).map_err(serde_to_sqlite_error)?;
                let mut next_status = status;
                // Route every kind through the single registry-owned merge
                // boundary (raft-fix.md): the prior `apply_against_latest ||
                // kind == "Node"` gate skipped generic workload kinds, so a
                // stale status commit carried clobbered status (only recovered
                // later by the raft-apply safety net in live_apply).
                // The merge is a no-op for a fresh non-Pod apply, so merging
                // unconditionally is safe; Pod (`apply_against_latest`) and
                // Node stay typed-merged regardless of freshness.
                klights_cluster_core::apply_status_merge(
                    &api_version,
                    &kind,
                    &live,
                    &mut next_status,
                    if apply_against_latest {
                        None
                    } else {
                        expected_rv.filter(|rv| *rv > 0)
                    },
                    live_rv,
                    observed_status_stamp.is_some(),
                );
                if live.get("status") == Some(&next_status) {
                    crate::diagnostics::log_noop_resource_write(
                        crate::diagnostics::NoopResourceWrite {
                            operation: "build_raft_status_commit",
                            api_version: &api_version,
                            kind: &kind,
                            namespace: namespace.as_deref(),
                            name: &name,
                            uid: &live_uid,
                            resource_version: live_rv,
                            reason: "merged status unchanged",
                        },
                    );
                    return Ok((Self::author_live_commit(rv, Vec::new())?, rv));
                }
                live["status"] = next_status;
                ensure_resource_type_meta(&mut live, &api_version, &kind);
                let uid = ensure_metadata_uid(&mut live);
                let precondition_resource_version = if apply_against_latest {
                    None
                } else {
                    preconditions
                        .resource_version
                        .or_else(|| expected_rv.filter(|rv| *rv > 0))
                };
                Self::author_live_commit_from_cluster_mutations(
                    rv,
                    vec![ClusterMutation::Resource(ResourceMutation::PutResource(
                        LogApplyResourceRow {
                            api_version,
                            kind,
                            namespace,
                            name,
                            uid,
                            resource_version: rv,
                            data: live,
                            require_absent: false,
                            require_existing: true,
                            precondition_uid: preconditions.uid,
                            precondition_resource_version,
                            status_only: true,
                        },
                    ))],
                )
            }

            StorageCommand::ApplyResourceBatch { operations } => {
                let mut mutations: Vec<ClusterMutation> = Vec::with_capacity(operations.len());
                for operation in operations {
                    match operation {
                        ResourceBatchOperation::Put {
                            api_version,
                            kind,
                            namespace,
                            name,
                            mut data,
                            mode,
                            preconditions,
                        } => {
                            match mode {
                                ResourceBatchPutMode::Create => {
                                    if Self::resource_row_optional_for_update_in_tx(
                                        tx,
                                        &api_version,
                                        &kind,
                                        namespace.as_deref(),
                                        &name,
                                    )?
                                    .is_some()
                                    {
                                        return Err(Self::sqlite_conversion_error(anyhow!(
                                            "{}/{} {}/{} already exists",
                                            api_version,
                                            kind,
                                            namespace.as_deref().unwrap_or(""),
                                            name
                                        )));
                                    }
                                }
                                ResourceBatchPutMode::Update => {
                                    let (live_rv, live_uid, live_data) =
                                        Self::resource_row_for_update_in_tx(
                                            tx,
                                            &api_version,
                                            &kind,
                                            namespace.as_deref(),
                                            &name,
                                        )?;
                                    validate_resource_preconditions(
                                        &preconditions,
                                        Some(&live_uid),
                                        live_rv,
                                    )
                                    .map_err(Self::sqlite_conversion_error)?;
                                    let live: Value = serde_json::from_slice(&live_data)
                                        .map_err(serde_to_sqlite_error)?;
                                    validate_metadata_uid_immutable(&data, &live)
                                        .map_err(Self::sqlite_conversion_error)?;
                                    preserve_server_metadata_fields_from_existing(&mut data, &live);
                                }
                            }
                            ensure_resource_type_meta(&mut data, &api_version, &kind);
                            ensure_metadata_identity(&mut data, namespace.as_deref(), &name);
                            if mode == ResourceBatchPutMode::Create {
                                ensure_metadata_create_defaults(&mut data, operation_now);
                            }
                            ensure_pod_status_ip_arrays(&mut data, &api_version, &kind);
                            let uid = ensure_metadata_uid(&mut data);
                            mutations.push(ClusterMutation::Resource(
                                ResourceMutation::PutResource(LogApplyResourceRow {
                                    api_version,
                                    kind,
                                    namespace,
                                    name,
                                    uid,
                                    resource_version: rv,
                                    data,
                                    require_absent: mode == ResourceBatchPutMode::Create,
                                    require_existing: mode == ResourceBatchPutMode::Update,
                                    precondition_uid: preconditions.uid,
                                    precondition_resource_version: preconditions.resource_version,
                                    status_only: false,
                                }),
                            ));
                        }
                        ResourceBatchOperation::Delete {
                            api_version,
                            kind,
                            namespace,
                            name,
                            preconditions,
                        } => {
                            let (live_rv, live_uid, _) = Self::resource_row_for_update_in_tx(
                                tx,
                                &api_version,
                                &kind,
                                namespace.as_deref(),
                                &name,
                            )?;
                            validate_resource_preconditions(
                                &preconditions,
                                Some(&live_uid),
                                live_rv,
                            )
                            .map_err(Self::sqlite_conversion_error)?;
                            mutations.push(ClusterMutation::Resource(
                                ResourceMutation::DeleteResource(LogApplyResourceKey {
                                    api_version,
                                    kind,
                                    namespace,
                                    name,
                                    uid: live_uid,
                                    precondition_resource_version: Some(live_rv),
                                }),
                            ));
                        }
                    }
                }
                Self::author_live_commit_from_cluster_mutations(rv, mutations)
            }

            StorageCommand::DeleteResource {
                api_version,
                kind,
                namespace,
                name,
                preconditions,
            } => {
                if api_version == "v1" && kind == "Namespace" {
                    if namespace.is_some() {
                        return Err(Self::sqlite_conversion_error(anyhow!(
                            "Namespace is cluster-scoped"
                        )));
                    }
                    let (current_rv, current_uid) = tx
                        .query_row(queries::NAMESPACE_GET, rusqlite::params![&name], |row| {
                            Ok((row.get::<_, i64>(1)?, row.get::<_, String>(2)?))
                        })
                        .map_err(|error| match error {
                            rusqlite::Error::QueryReturnedNoRows => {
                                Self::sqlite_conversion_error(anyhow!("Namespace {name} not found"))
                            }
                            other => tokio_rusqlite::Error::Rusqlite(other),
                        })?;
                    validate_resource_preconditions(&preconditions, Some(&current_uid), current_rv)
                        .map_err(Self::sqlite_conversion_error)?;
                    let remaining: i64 = tx.query_row(
                        queries::NAMESPACE_RESOURCES_COUNT,
                        rusqlite::params![&name],
                        |row| row.get(0),
                    )?;
                    if remaining > 0 {
                        return Err(Self::sqlite_conversion_error(anyhow!(
                            "Namespace has remaining content (409 Conflict)"
                        )));
                    }
                    return Ok((
                        Self::author_live_commit_from_cluster_mutations(
                            rv,
                            vec![ClusterMutation::Namespace(
                                NamespaceMutation::DeleteNamespace { name },
                            )],
                        )?,
                        rv,
                    ));
                }
                let (current_rv, current_uid, data_bytes) = Self::resource_row_for_update_in_tx(
                    tx,
                    &api_version,
                    &kind,
                    namespace.as_deref(),
                    &name,
                )?;
                validate_resource_preconditions(&preconditions, Some(&current_uid), current_rv)
                    .map_err(Self::sqlite_conversion_error)?;
                let data: Value =
                    serde_json::from_slice(&data_bytes).map_err(serde_to_sqlite_error)?;
                let watch_event_row = LogApplyWatchEventRow {
                    event_id: None,
                    api_version: api_version.clone(),
                    kind: kind.clone(),
                    namespace: namespace.clone(),
                    name: name.clone(),
                    resource_version: rv,
                    event_type: "DELETED".to_string(),
                    data: hydrate_watch_event_data(
                        data,
                        &api_version,
                        &kind,
                        namespace.as_deref(),
                        &name,
                        rv,
                    ),
                };
                Self::author_live_commit_from_cluster_mutations(
                    rv,
                    vec![
                        ClusterMutation::WatchHistory(WatchHistoryMutation::PutWatchEvent(
                            watch_event_row,
                        )),
                        ClusterMutation::Resource(ResourceMutation::DeleteResource(
                            LogApplyResourceKey {
                                api_version,
                                kind,
                                namespace,
                                name,
                                uid: current_uid,
                                precondition_resource_version: Some(current_rv),
                            },
                        )),
                    ],
                )
            }
            StorageCommand::FinalizeBoundPod {
                namespace,
                name,
                pod_uid,
                node_name,
                observed_resource_version: _,
            } => {
                let Some((_current_rv, current_uid, data_bytes)) =
                    Self::resource_row_optional_for_update_in_tx(
                        tx,
                        "v1",
                        "Pod",
                        Some(namespace.as_str()),
                        &name,
                    )?
                else {
                    let current_public_rv = Self::current_resource_version_in_tx(tx)?;
                    return Ok((
                        Self::author_live_commit(current_public_rv, Vec::new())?,
                        current_public_rv,
                    ));
                };
                if current_uid != pod_uid {
                    let current_public_rv = Self::current_resource_version_in_tx(tx)?;
                    return Ok((
                        Self::author_live_commit(current_public_rv, Vec::new())?,
                        current_public_rv,
                    ));
                }
                let data: Value =
                    serde_json::from_slice(&data_bytes).map_err(serde_to_sqlite_error)?;
                let assigned_node = data
                    .pointer("/spec/nodeName")
                    .and_then(Value::as_str)
                    .filter(|value| !value.is_empty());
                let has_finalizers = data
                    .pointer("/metadata/finalizers")
                    .and_then(Value::as_array)
                    .is_some_and(|finalizers| !finalizers.is_empty());
                let terminating = data
                    .pointer("/metadata/deletionTimestamp")
                    .and_then(Value::as_str)
                    .is_some_and(|value| !value.is_empty())
                    || (data.pointer("/status/phase").and_then(Value::as_str) == Some("Failed")
                        && data.pointer("/status/reason").and_then(Value::as_str)
                            == Some("NodeLost"));
                if assigned_node != Some(node_name.as_str()) || has_finalizers || !terminating {
                    let current_public_rv = Self::current_resource_version_in_tx(tx)?;
                    return Ok((
                        Self::author_live_commit(current_public_rv, Vec::new())?,
                        current_public_rv,
                    ));
                }
                Self::author_live_commit_from_cluster_mutations(
                    rv,
                    vec![ClusterMutation::Resource(
                        ResourceMutation::FinalizeBoundPod(
                            klights_cluster_core::LogApplyPodActorFinalization {
                                namespace,
                                name,
                                pod_uid: current_uid,
                                node_name,
                            },
                        ),
                    )],
                )
            }
            StorageCommand::DeleteResourceWithTombstone {
                api_version,
                kind,
                namespace,
                name,
                preconditions,
                grace_seconds,
            } => {
                let (current_rv, current_uid, data_bytes) = Self::resource_row_for_update_in_tx(
                    tx,
                    &api_version,
                    &kind,
                    namespace.as_deref(),
                    &name,
                )?;
                validate_resource_preconditions(&preconditions, Some(&current_uid), current_rv)
                    .map_err(Self::sqlite_conversion_error)?;
                let mut data: Value =
                    serde_json::from_slice(&data_bytes).map_err(serde_to_sqlite_error)?;
                let Some(metadata) = data.get_mut("metadata").and_then(Value::as_object_mut) else {
                    return Err(Self::sqlite_conversion_error(anyhow::anyhow!(
                        "resource missing metadata for DeleteResourceWithTombstone: {api_version}/{kind}/{}",
                        name
                    )));
                };
                if metadata
                    .get("deletionTimestamp")
                    .and_then(|timestamp| timestamp.as_str())
                    .is_none_or(str::is_empty)
                {
                    metadata.insert(
                        "deletionTimestamp".to_string(),
                        serde_json::Value::String(
                            klights_cluster_core::k8s_time::format_legacy_timestamp(operation_now),
                        ),
                    );
                }
                metadata
                    .entry("deletionGracePeriodSeconds".to_string())
                    .or_insert_with(|| Value::from(grace_seconds));

                let watch_event_data = hydrate_watch_event_data(
                    data.clone(),
                    &api_version,
                    &kind,
                    namespace.as_deref(),
                    &name,
                    rv,
                );
                let watch_event_row = LogApplyWatchEventRow {
                    event_id: None,
                    api_version: api_version.clone(),
                    kind: kind.clone(),
                    namespace: namespace.clone(),
                    name: name.clone(),
                    resource_version: rv,
                    event_type: "DELETED".to_string(),
                    data: watch_event_data,
                };

                let delete_key = LogApplyResourceKey {
                    api_version,
                    kind,
                    namespace,
                    name,
                    uid: current_uid,
                    precondition_resource_version: Some(current_rv),
                };
                Self::author_live_commit_from_cluster_mutations(
                    rv,
                    vec![
                        ClusterMutation::WatchHistory(WatchHistoryMutation::PutWatchEvent(
                            watch_event_row,
                        )),
                        ClusterMutation::Resource(ResourceMutation::DeleteResource(delete_key)),
                    ],
                )
            }

            StorageCommand::UpdateNodeDataplane {
                node_name,
                mode,
                encryption,
                public_key,
                endpoint,
                port,
            } => {
                // Also stamp routing metadata in the cluster_resource row
                let metadata = klights_cluster_store::DataplanePeerMetadata::try_new(
                    node_name.clone(),
                    klights_cluster_store::DataplaneMode::parse(&mode)
                        .map_err(Self::sqlite_conversion_error)?,
                    klights_cluster_store::DataplaneEncryption::parse(Some(&encryption))
                        .map_err(Self::sqlite_conversion_error)?,
                    public_key.clone(),
                    Some(endpoint.clone()),
                    port,
                )
                .map_err(Self::sqlite_conversion_error)?;
                let stamped_node =
                    Self::node_routing_metadata_resource_row_in_tx(tx, &node_name, &metadata, rv)?;
                let mut mutations = vec![ClusterMutation::Network(
                    NetworkMutation::PutNodeDataplane(LogApplyNodeDataplaneRow {
                        node_name,
                        mode,
                        encryption,
                        public_key,
                        endpoint,
                        port,
                    }),
                )];
                if let Some(row) = stamped_node {
                    mutations.push(ClusterMutation::Resource(ResourceMutation::PutResource(
                        row,
                    )));
                }
                Self::author_live_commit_from_cluster_mutations(rv, mutations)
            }

            StorageCommand::CreateNamespace { name, mut data } => {
                let exists = tx
                    .query_row(queries::NAMESPACE_GET, rusqlite::params![&name], |_| Ok(()))
                    .optional()?
                    .is_some();
                if exists {
                    return Err(Self::sqlite_conversion_error(anyhow!(
                        "namespaces \"{name}\" already exists (409 Conflict)"
                    )));
                }
                ensure_resource_type_meta(&mut data, "v1", "Namespace");
                ensure_metadata_identity(&mut data, None, &name);
                ensure_metadata_create_defaults(&mut data, operation_now);
                let uid = ensure_metadata_uid(&mut data);
                Self::author_live_commit_from_cluster_mutations(
                    rv,
                    vec![ClusterMutation::Namespace(NamespaceMutation::PutNamespace(
                        LogApplyNamespaceRow {
                            name: name.clone(),
                            uid,
                            resource_version: rv,
                            data,
                        },
                    ))],
                )
            }

            StorageCommand::UpdateNamespace {
                name,
                mut data,
                expected_rv,
            } => {
                let (live_rv, live_uid, live_data) = tx
                    .query_row(queries::NAMESPACE_GET, rusqlite::params![&name], |row| {
                        Ok((
                            row.get::<_, i64>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, Vec<u8>>(3)?,
                        ))
                    })
                    .map_err(|error| match error {
                        rusqlite::Error::QueryReturnedNoRows => {
                            Self::sqlite_conversion_error(anyhow!("Namespace {name} not found"))
                        }
                        other => tokio_rusqlite::Error::Rusqlite(other),
                    })?;
                validate_resource_preconditions(
                    &ResourcePreconditions::resource_version(expected_rv),
                    Some(&live_uid),
                    live_rv,
                )
                .map_err(Self::sqlite_conversion_error)?;
                let live: Value =
                    serde_json::from_slice(&live_data).map_err(serde_to_sqlite_error)?;
                validate_metadata_uid_immutable(&data, &live)
                    .map_err(Self::sqlite_conversion_error)?;
                ensure_resource_type_meta(&mut data, "v1", "Namespace");
                ensure_metadata_identity(&mut data, None, &name);
                preserve_server_metadata_fields_from_existing(&mut data, &live);
                let uid = ensure_metadata_uid(&mut data);
                Self::author_live_commit_from_cluster_mutations(
                    rv,
                    vec![ClusterMutation::Namespace(NamespaceMutation::PutNamespace(
                        LogApplyNamespaceRow {
                            name: name.clone(),
                            uid,
                            resource_version: rv,
                            data,
                        },
                    ))],
                )
            }

            StorageCommand::DeleteNamespace { name } => {
                let exists = tx
                    .query_row(queries::NAMESPACE_GET, rusqlite::params![&name], |_| Ok(()))
                    .optional()?
                    .is_some();
                if !exists {
                    return Err(Self::sqlite_conversion_error(anyhow!(
                        "Namespace {name} not found"
                    )));
                }
                Self::author_live_commit_from_cluster_mutations(
                    rv,
                    vec![
                        ClusterMutation::Namespace(NamespaceMutation::DeleteNamespaceContents {
                            name: name.clone(),
                        }),
                        ClusterMutation::Namespace(NamespaceMutation::DeleteNamespace { name }),
                    ],
                )
            }

            StorageCommand::DeleteNamespaceContents { name } => {
                Self::author_live_commit_from_cluster_mutations(
                    rv,
                    vec![ClusterMutation::Namespace(
                        NamespaceMutation::DeleteNamespaceContents { name },
                    )],
                )
            }

            StorageCommand::PatchResource {
                api_version,
                kind,
                namespace,
                name,
                patch_kind,
                patch,
                preconditions,
                strict_resource_version,
            } => {
                let (live_rv, live_uid, live_data) = Self::resource_row_for_update_in_tx(
                    tx,
                    &api_version,
                    &kind,
                    namespace.as_deref(),
                    &name,
                )?;
                let live: Value =
                    serde_json::from_slice(&live_data).map_err(serde_to_sqlite_error)?;
                let mut effective_preconditions = preconditions.clone();
                if !strict_resource_version
                    && let Some(expected) = effective_preconditions.resource_version
                    && expected != live_rv
                    && klights_types::has_builtin_status_subresource(&api_version, &kind)
                    && let Some(base) = Self::resource_snapshot_for_key_at_rv_in_tx(
                        tx,
                        &api_version,
                        &kind,
                        namespace.as_deref(),
                        &name,
                        expected,
                    )?
                    && metadata_uid(&base) == Some(live_uid.as_str())
                    && resource_client_owned_state_equal(&base, &live)
                {
                    effective_preconditions.resource_version = Some(live_rv);
                }
                validate_resource_preconditions(&effective_preconditions, Some(&live_uid), live_rv)
                    .map_err(Self::sqlite_conversion_error)?;
                if Self::should_apply_outbox_patch_against_latest(
                    &api_version,
                    &kind,
                    patch_kind,
                    &patch,
                    &preconditions,
                ) {
                    let terminating_pod_unready_timestamp =
                        klights_types::is_zero_grace_pod_delete_mark_patch(
                            &api_version,
                            &kind,
                            &patch,
                        )
                        .then(|| {
                            klights_cluster_core::k8s_time::format_legacy_timestamp(operation_now)
                        });
                    return Ok((
                        Self::author_live_commit_from_cluster_mutations(
                            rv,
                            vec![ClusterMutation::Resource(
                                ResourceMutation::PatchResourceLatest(LogApplyResourcePatch {
                                    api_version,
                                    kind,
                                    namespace,
                                    name,
                                    resource_version: rv,
                                    patch_kind,
                                    patch,
                                    require_existing: true,
                                    precondition_uid: Some(live_uid),
                                    precondition_resource_version: None,
                                    terminating_pod_unready_timestamp,
                                }),
                            )],
                        )?,
                        rv,
                    ));
                }
                let live_before_patch = live.clone();
                let mut live = live;
                Self::apply_outbox_patch(&api_version, &kind, &mut live, patch_kind, patch)?;
                ensure_resource_type_meta(&mut live, &api_version, &kind);
                ensure_metadata_identity(&mut live, namespace.as_deref(), &name);
                ensure_pod_status_ip_arrays(&mut live, &api_version, &kind);
                klights_types::preserve_status_subresource_on_main_update(
                    &api_version,
                    &kind,
                    &live_before_patch,
                    &mut live,
                );
                preserve_server_metadata_fields_from_existing(&mut live, &live_before_patch);
                if live == live_before_patch {
                    return Ok((Self::author_live_commit(rv, Vec::new())?, rv));
                }
                let uid = ensure_metadata_uid(&mut live);
                Self::author_live_commit_from_cluster_mutations(
                    rv,
                    vec![ClusterMutation::Resource(ResourceMutation::PutResource(
                        LogApplyResourceRow {
                            api_version,
                            kind,
                            namespace,
                            name,
                            uid,
                            resource_version: rv,
                            data: live,
                            require_absent: false,
                            require_existing: true,
                            precondition_uid: effective_preconditions.uid,
                            precondition_resource_version: effective_preconditions
                                .resource_version
                                .or(Some(live_rv)),
                            status_only: false,
                        },
                    ))],
                )
            }

            StorageCommand::AllocateNodeSubnet {
                node_name,
                subnet,
                node_ip,
            } => Self::author_live_commit_from_cluster_mutations(
                rv,
                vec![ClusterMutation::Network(
                    NetworkMutation::AllocateNodeSubnet(LogApplyNodeSubnetAllocation {
                        node_name,
                        cluster_cidr: subnet,
                        node_ip,
                    }),
                )],
            ),

            StorageCommand::DeleteNodeSubnet { node_name } => {
                Self::author_live_commit_from_cluster_mutations(
                    rv,
                    vec![ClusterMutation::Network(
                        NetworkMutation::DeleteNodeSubnet { node_name },
                    )],
                )
            }

            StorageCommand::UpdateNodePeerAttributes {
                node_name,
                mode,
                hostport_range,
            } => {
                let Some(mut row) = tx
                    .query_row(
                        queries::NODE_SUBNET_SELECT_BY_NAME,
                        rusqlite::params![&node_name],
                        |row| {
                            Ok(klights_cluster_core::LogApplyNodeSubnetRow {
                                node_name: row.get(0)?,
                                subnet: row.get(1)?,
                                subnet_base_int: row.get::<_, i64>(2)? as u32,
                                gateway_ip: row.get(3)?,
                                node_ip: row.get(4)?,
                                mode: row.get(5)?,
                                hostport_range: row.get(6)?,
                            })
                        },
                    )
                    .optional()?
                else {
                    return Ok((Self::author_live_commit(rv, Vec::new())?, rv));
                };
                let mode = match klights_types::parse_node_peer_mode(Some(&mode))
                    .unwrap_or(klights_types::NodePeerMode::Root)
                {
                    klights_types::NodePeerMode::Root => "root".to_string(),
                    klights_types::NodePeerMode::Rootless => "rootless".to_string(),
                };
                let hostport_range = hostport_range
                    .as_deref()
                    .and_then(|value| klights_types::HostPortRange::parse(value).ok())
                    .map(|range| range.to_string());
                if row.mode == mode && row.hostport_range == hostport_range {
                    return Ok((Self::author_live_commit(rv, Vec::new())?, rv));
                }
                row.mode = mode;
                row.hostport_range = hostport_range;
                Self::author_live_commit_from_cluster_mutations(
                    rv,
                    vec![ClusterMutation::Network(NetworkMutation::PutNodeSubnet(
                        row,
                    ))],
                )
            }

            StorageCommand::PodSlotTryAdmit { .. }
            | StorageCommand::PodSlotMarkTerminating { .. }
            | StorageCommand::PodSlotClearIfUid { .. } => {
                // Pod slots are managed by the pod repository actors.
                Self::author_live_commit(rv, Vec::new())
            }

            StorageCommand::MovePodToCleanupIntent {
                node_name,
                namespace,
                pod_name,
                pod_uid,
                reason,
            } => {
                let (_live_rv, live_uid, pod_bytes) = Self::resource_row_for_update_in_tx(
                    tx,
                    "v1",
                    "Pod",
                    Some(namespace.as_str()),
                    &pod_name,
                )?;
                if live_uid != pod_uid {
                    return Err(tokio_rusqlite::Error::Rusqlite(
                        rusqlite::Error::QueryReturnedNoRows,
                    ));
                }
                let pod_data: Value =
                    serde_json::from_slice(&pod_bytes).map_err(serde_to_sqlite_error)?;
                if pod_data
                    .pointer("/spec/nodeName")
                    .and_then(|value| value.as_str())
                    != Some(node_name.as_str())
                {
                    return Err(tokio_rusqlite::Error::Rusqlite(
                        rusqlite::Error::QueryReturnedNoRows,
                    ));
                }
                let created_at_ms = operation_now.timestamp_millis();
                Self::author_live_commit_from_cluster_mutations(
                    rv,
                    vec![ClusterMutation::PodCleanup(
                        PodCleanupMutation::PutPodCleanupIntent(LogApplyPodCleanupIntentRow {
                            node_name,
                            namespace,
                            pod_name,
                            pod_uid,
                            reason,
                            resource_version: rv,
                            created_at_ms,
                            pod_data,
                        }),
                    )],
                )
            }

            StorageCommand::DeletePodCleanupIntent {
                node_name,
                namespace,
                pod_name,
                pod_uid,
                reason,
            } => Self::author_live_commit_from_cluster_mutations(
                rv,
                vec![ClusterMutation::PodCleanup(
                    PodCleanupMutation::DeletePodCleanupIntent(LogApplyPodCleanupIntentKey {
                        node_name,
                        namespace,
                        pod_name,
                        pod_uid,
                        reason,
                    }),
                )],
            ),

            StorageCommand::DeletePodCleanupIntentsForNode { node_name } => {
                Self::author_live_commit_from_cluster_mutations(
                    rv,
                    vec![ClusterMutation::PodCleanup(
                        PodCleanupMutation::DeletePodCleanupIntentsForNode { node_name },
                    )],
                )
            }

            StorageCommand::WatchEventAppend {
                event_bytes,
                rv: watch_rv,
            } => {
                // The event_bytes is a JSON WatchEvent; extract fields
                // for the LogApplyWatchEventRow
                let event: Value =
                    serde_json::from_slice(&event_bytes).map_err(serde_to_sqlite_error)?;
                let api_version = event["api_version"].as_str().unwrap_or("").to_string();
                let kind = event["kind"].as_str().unwrap_or("").to_string();
                let namespace = event["namespace"].as_str().map(str::to_string);
                let name = event["name"].as_str().unwrap_or("").to_string();
                let event_type = event["type"].as_str().unwrap_or("ADDED").to_string();
                let resource_version = watch_rv.max(rv);
                let data = hydrate_watch_event_data(
                    event["object"].clone(),
                    &api_version,
                    &kind,
                    namespace.as_deref(),
                    &name,
                    resource_version,
                );
                Self::author_live_commit_from_cluster_mutations(
                    rv,
                    vec![ClusterMutation::WatchHistory(
                        WatchHistoryMutation::PutWatchEvent(LogApplyWatchEventRow {
                            event_id: None,
                            api_version,
                            kind,
                            namespace,
                            name,
                            resource_version,
                            event_type,
                            data,
                        }),
                    )],
                )
            }

            StorageCommand::GcWatchEvents {
                max_rows,
                batch_cap,
            } => Self::author_live_commit_from_cluster_mutations(
                rv,
                vec![ClusterMutation::WatchHistory(
                    WatchHistoryMutation::GcWatchEvents {
                        max_rows,
                        batch_cap,
                    },
                )],
            ),
            StorageCommand::GcAppliedOutbox { cutoff_ms } => {
                Self::author_live_commit_from_cluster_mutations(
                    rv,
                    vec![ClusterMutation::OutboxLedger(
                        OutboxLedgerMutation::GcAppliedOutbox {
                            cutoff_ms,
                            operations: Vec::new(),
                        },
                    )],
                )
            }

            StorageCommand::AdvanceResourceVersion { min_rv: _, new_rv } => {
                Self::author_live_commit_from_cluster_mutations(
                    rv,
                    vec![ClusterMutation::ClusterMeta(
                        ClusterMetaMutation::AdvanceResourceVersion {
                            resource_version: new_rv.max(rv),
                        },
                    )],
                )
            }
            StorageCommand::EnsureClusterMetadata { cluster_id } => {
                let existing: Option<String> = tx
                    .query_row(
                        crate::sqlite::mutation_queries::SELECT_KLIGHTS_META,
                        [&"cluster_id"],
                        |row| row.get::<_, String>(0),
                    )
                    .ok();
                if existing.is_none() {
                    Self::author_live_commit_from_cluster_mutations(
                        rv,
                        vec![
                            ClusterMutation::ClusterMeta(ClusterMetaMutation::PutKlightsMeta {
                                key: klights_cluster_store::CLUSTER_ID_META_KEY.to_string(),
                                value: cluster_id,
                            }),
                            ClusterMutation::ClusterMeta(ClusterMetaMutation::PutKlightsMeta {
                                key: klights_cluster_store::LEADER_EPOCH_META_KEY.to_string(),
                                value: "0".to_string(),
                            }),
                        ],
                    )
                } else {
                    // cluster_id already set — idempotent no-op
                    Self::author_live_commit(rv, Vec::new())
                }
            }
            StorageCommand::SetKlightsMeta { key, value } => {
                Self::author_live_commit_from_cluster_mutations(
                    rv,
                    vec![ClusterMutation::ClusterMeta(
                        ClusterMetaMutation::PutKlightsMeta { key, value },
                    )],
                )
            }
            _ => {
                return Err(tokio_rusqlite::Error::Rusqlite(
                    rusqlite::Error::InvalidQuery,
                ));
            }
        }?;

        Ok((commit, rv))
    }

    pub async fn db_call<T, F>(&self, query_name: &'static str, f: F) -> tokio_rusqlite::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut rusqlite::Connection) -> tokio_rusqlite::Result<T> + Send + 'static,
    {
        self.executor.call_raw(query_name, f).await
    }

    pub async fn db_call_with_post_commit<T, P, F, C>(
        &self,
        query_name: &'static str,
        f: F,
        post_commit: C,
    ) -> tokio_rusqlite::Result<T>
    where
        T: Send + 'static,
        P: Send + 'static,
        F: FnOnce(&mut rusqlite::Connection) -> tokio_rusqlite::Result<(T, P)> + Send + 'static,
        C: FnOnce(P) + Send + 'static,
    {
        self.executor
            .call_raw_with_post_commit(query_name, f, post_commit)
            .await
    }

    pub async fn read_db_call<T, F>(
        &self,
        query_name: &'static str,
        f: F,
    ) -> tokio_rusqlite::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut rusqlite::Connection) -> tokio_rusqlite::Result<T> + Send + 'static,
    {
        self.read_executor.call_raw(query_name, f).await
    }

    pub async fn get_applied_outbox(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<LogApplyAppliedOutboxRow>> {
        let idempotency_key = idempotency_key.to_string();
        self.read_db_call("db_applied_outbox_get", move |conn| {
            conn.query_row(queries::APPLIED_OUTBOX_GET, [idempotency_key], |row| {
                Ok(LogApplyAppliedOutboxRow {
                    idempotency_key: row.get(0)?,
                    subject_key: row.get(1)?,
                    operation: row.get(2)?,
                    first_seen_ms: row.get(3)?,
                    applied_rv: row.get(4)?,
                    result_proto: row.get(5)?,
                    status_stamp: row.get(6)?,
                })
            })
            .optional()
            .map_err(tokio_rusqlite::Error::from)
        })
        .await
        .map_err(|e| anyhow!("applied outbox get failed: {e}"))
    }

    pub async fn insert_applied_outbox(&self, record: LogApplyAppliedOutboxRow) -> Result<bool> {
        self.db_call("db_applied_outbox_insert", move |conn| {
            let changed = conn.execute(
                queries::APPLIED_OUTBOX_INSERT,
                rusqlite::params![
                    record.idempotency_key,
                    record.subject_key,
                    record.operation,
                    record.first_seen_ms,
                    record.applied_rv,
                    record.result_proto,
                    record.status_stamp
                ],
            )?;
            Ok(changed > 0)
        })
        .await
        .map_err(|e| anyhow!("applied outbox insert failed: {e}"))
    }

    pub async fn list_outbox_stream_watermarks(
        &self,
    ) -> Result<Vec<klights_cluster_core::OutboxStreamWatermark>> {
        self.read_db_call("db_outbox_stream_watermarks_list_all", move |conn| {
            let mut stmt = conn.prepare(
                "SELECT client_id, stream_id, last_seq FROM outbox_stream_watermarks \
                 ORDER BY client_id ASC, stream_id ASC",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok(klights_cluster_core::OutboxStreamWatermark {
                    client_id: row.get(0)?,
                    stream_id: row.get(1)?,
                    stream_seq: row.get(2)?,
                })
            })?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })
        .await
        .map_err(|e| anyhow!("list outbox stream watermarks failed: {e}"))
    }

    pub async fn list_outbox_stream_watermarks_paged(
        &self,
        after: Option<&klights_cluster_store::SnapshotOutboxWatermarkCursor>,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<klights_cluster_core::OutboxStreamWatermark>> {
        if limit.get() > klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE {
            return Err(anyhow!(
                "outbox-watermark page limit {} exceeds {}",
                limit,
                klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE
            ));
        }
        let after = after.map(|cursor| (cursor.client_id().to_string(), cursor.stream_id()));
        let limit = i64::try_from(limit.get())?;
        self.read_db_call("db_outbox_stream_watermarks_list_paged", move |conn| {
            let mut stmt = conn.prepare(
                "SELECT client_id, stream_id, last_seq FROM outbox_stream_watermarks
                 WHERE ?1 IS NULL
                    OR client_id > ?1
                    OR (client_id = ?1 AND stream_id > ?2)
                 ORDER BY client_id ASC, stream_id ASC
                 LIMIT ?3",
            )?;
            let rows = stmt.query_map(
                rusqlite::params![
                    after.as_ref().map(|cursor| cursor.0.as_str()),
                    after.as_ref().map(|cursor| cursor.1),
                    limit,
                ],
                |row| {
                    Ok(klights_cluster_core::OutboxStreamWatermark {
                        client_id: row.get(0)?,
                        stream_id: row.get(1)?,
                        stream_seq: row.get(2)?,
                    })
                },
            )?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })
        .await
        .map_err(|e| anyhow!("list paged outbox stream watermarks failed: {e}"))
    }

    pub async fn list_applied_outbox(&self) -> Result<Vec<LogApplyAppliedOutboxRow>> {
        self.read_db_call("db_applied_outbox_list_all", move |conn| {
            let rows = conn
                .prepare(queries::APPLIED_OUTBOX_LIST_ALL)?
                .query_map([], |row| {
                    Ok(LogApplyAppliedOutboxRow {
                        idempotency_key: row.get(0)?,
                        subject_key: row.get(1)?,
                        operation: row.get(2)?,
                        first_seen_ms: row.get(3)?,
                        applied_rv: row.get(4)?,
                        result_proto: row.get(5)?,
                        status_stamp: row.get(6)?,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        })
        .await
        .map_err(|e| anyhow!("applied outbox list failed: {e}"))
    }

    /// memory-improvement.md §10 P1: keyset-paginated form of
    /// `list_applied_outbox`. Returns up to `limit` rows whose
    /// `idempotency_key > after_key` (pass `None` for the first page), in the
    /// same `ORDER BY idempotency_key ASC` ordering as the full-list form.
    /// Lets the snapshot emitter stream the dedup ledger batch by batch
    /// instead of materializing the whole table into one `Vec`.
    pub async fn list_applied_outbox_paged(
        &self,
        after_key: Option<&str>,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<LogApplyAppliedOutboxRow>> {
        let after = after_key.unwrap_or("").to_string();
        let limit_i64 = limit.get() as i64;
        self.read_db_call("db_applied_outbox_list_all_paged", move |conn| {
            let rows = conn
                .prepare(queries::APPLIED_OUTBOX_LIST_ALL_PAGED)?
                .query_map(rusqlite::params![after, limit_i64], |row| {
                    Ok(LogApplyAppliedOutboxRow {
                        idempotency_key: row.get(0)?,
                        subject_key: row.get(1)?,
                        operation: row.get(2)?,
                        first_seen_ms: row.get(3)?,
                        applied_rv: row.get(4)?,
                        result_proto: row.get(5)?,
                        status_stamp: row.get(6)?,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        })
        .await
        .map_err(|e| anyhow!("applied outbox paged list failed: {e}"))
    }

    pub async fn apply_resource_batch(
        &self,
        operations: Vec<ResourceBatchOperation>,
    ) -> Result<()> {
        if operations.is_empty() {
            return Ok(());
        }
        let command =
            klights_cluster_core::command::StorageCommand::ApplyResourceBatch { operations };
        let outbox_codec = self.outbox_codec.clone();
        let operation_now = self.wall_clock.now_utc();
        // Build + apply in one IMMEDIATE transaction. The builder authors only
        // an RV-zero template; committed apply allocates the public RV in the
        // same transaction that writes the rows.
        let _pending = self
            .db_call("db_apply_resource_batch", move |conn| {
                let context = live_apply::TransactionContext::new(outbox_codec.as_ref());
                let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let (commit, _rv) = Self::build_log_apply_commit_in_tx_from_command(
                    &tx,
                    command,
                    "ResourceBatch",
                    "",
                    None,
                    operation_now,
                )?;
                let pending = live_apply::apply_commit_in_tx_with_context(&tx, commit, &context)?;
                tx.commit()?;
                Ok(pending)
            })
            .await
            .map_err(|e| anyhow!("apply resource batch failed: {e}"))?;
        #[cfg(test)]
        self.publish_watch_events(_pending);
        Ok(())
    }

    pub async fn move_pod_to_cleanup_intent(
        &self,
        node_name: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        reason: &str,
    ) -> Result<()> {
        let command = klights_cluster_core::command::StorageCommand::MovePodToCleanupIntent {
            node_name: node_name.to_string(),
            namespace: namespace.to_string(),
            pod_name: pod_name.to_string(),
            pod_uid: pod_uid.to_string(),
            reason: reason.to_string(),
        };
        let authoring_node = node_name.to_string();
        let outbox_codec = self.outbox_codec.clone();
        let operation_now = self.wall_clock.now_utc();
        let _pending = self
            .db_call("db_move_pod_to_cleanup_intent", move |conn| {
                let context = live_apply::TransactionContext::new(outbox_codec.as_ref());
                let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let (commit, _rv) = Self::build_log_apply_commit_in_tx_from_command(
                    &tx,
                    command,
                    "ClusterMaintenance",
                    &authoring_node,
                    None,
                    operation_now,
                )?;
                let pending = live_apply::apply_commit_in_tx_with_context(&tx, commit, &context)?;
                tx.commit()?;
                Ok(pending)
            })
            .await
            .map_err(|e| anyhow!("move pod to cleanup intent failed: {e}"))?;
        #[cfg(test)]
        self.publish_watch_events(_pending);
        Ok(())
    }

    pub async fn list_pod_cleanup_intents_for_node(
        &self,
        node_name: &str,
    ) -> Result<Vec<LogApplyPodCleanupIntentRow>> {
        let node_name = node_name.to_string();
        self.read_db_call("db_list_pod_cleanup_intents_for_node", move |conn| {
            let rows = conn
                .prepare(queries::POD_CLEANUP_INTENT_LIST_BY_NODE)?
                .query_map([node_name], |row| {
                    let pod_data_bytes: Vec<u8> = row.get(7)?;
                    let pod_data = serde_json::from_slice(&pod_data_bytes).map_err(|err| {
                        rusqlite::Error::FromSqlConversionFailure(
                            7,
                            rusqlite::types::Type::Blob,
                            Box::new(err),
                        )
                    })?;
                    Ok(LogApplyPodCleanupIntentRow {
                        node_name: row.get(0)?,
                        namespace: row.get(1)?,
                        pod_name: row.get(2)?,
                        pod_uid: row.get(3)?,
                        reason: row.get(4)?,
                        resource_version: row.get(5)?,
                        created_at_ms: row.get(6)?,
                        pod_data,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        })
        .await
        .map_err(|e| anyhow!("list pod cleanup intents failed: {e}"))
    }

    pub async fn delete_pod_cleanup_intent(
        &self,
        node_name: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        reason: &str,
    ) -> Result<()> {
        let command = klights_cluster_core::command::StorageCommand::DeletePodCleanupIntent {
            node_name: node_name.to_string(),
            namespace: namespace.to_string(),
            pod_name: pod_name.to_string(),
            pod_uid: pod_uid.to_string(),
            reason: reason.to_string(),
        };
        self.apply_cluster_maintenance_command(command, node_name)
            .await
    }

    pub async fn delete_pod_cleanup_intents_for_node(&self, node_name: &str) -> Result<()> {
        let command =
            klights_cluster_core::command::StorageCommand::DeletePodCleanupIntentsForNode {
                node_name: node_name.to_string(),
            };
        self.apply_cluster_maintenance_command(command, node_name)
            .await
    }

    async fn apply_cluster_maintenance_command(
        &self,
        command: klights_cluster_core::command::StorageCommand,
        authoring_node: &str,
    ) -> Result<()> {
        let authoring_node = authoring_node.to_string();
        let outbox_codec = self.outbox_codec.clone();
        let operation_now = self.wall_clock.now_utc();
        let _pending = self
            .db_call("db_apply_cluster_maintenance_command", move |conn| {
                let context = live_apply::TransactionContext::new(outbox_codec.as_ref());
                let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let (commit, _rv) = Self::build_log_apply_commit_in_tx_from_command(
                    &tx,
                    command,
                    "ClusterMaintenance",
                    &authoring_node,
                    None,
                    operation_now,
                )?;
                let pending = live_apply::apply_commit_in_tx_with_context(&tx, commit, &context)?;
                tx.commit()?;
                Ok(pending)
            })
            .await
            .map_err(|e| anyhow!("apply cluster maintenance command failed: {e}"))?;
        #[cfg(test)]
        self.publish_watch_events(_pending);
        Ok(())
    }

    /// T1.4: build (without applying) a `LogApplyCommit` for regular raft writes.
    ///
    /// This variant intentionally skips outbox idempotency side effects and is used
    /// by `propose_command` for non-outbox writes. It uses a private candidate
    /// RV for materialization and applies the same operation-specific behavior through
    /// `build_log_apply_commit_in_tx_from_command`.
    pub async fn build_log_apply_commit_for_command(
        &self,
        command: klights_cluster_core::command::StorageCommand,
        operation: &str,
        authoring_node: &str,
    ) -> Result<klights_cluster_core::LogApplyCommit> {
        #[cfg(any(test, feature = "test-support"))]
        match &command {
            klights_cluster_core::command::StorageCommand::PatchResource {
                api_version,
                kind,
                namespace,
                name,
                ..
            } => {
                self.pause_resource_mutation_if_requested(
                    ResourceMutationPauseOperation::BuildPatchCommand,
                    api_version,
                    kind,
                    namespace.as_deref(),
                    name,
                )
                .await;
            }
            klights_cluster_core::command::StorageCommand::UpdateResource {
                api_version,
                kind,
                namespace,
                name,
                ..
            } => {
                self.pause_resource_mutation_if_requested(
                    ResourceMutationPauseOperation::MainUpdate,
                    api_version,
                    kind,
                    namespace.as_deref(),
                    name,
                )
                .await;
            }
            _ => {}
        }
        let operation = operation.to_string();
        let authoring_node_owned = authoring_node.to_string();
        let operation_now = self.wall_clock.now_utc();

        self.db_call("db_build_log_apply_commit_for_command", move |conn| {
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let resource_version_hint =
                Self::outbox_materialization_resource_version_hint_in_tx(&tx)?;
            let (commit, _rv) = Self::build_log_apply_commit_in_tx_from_command(
                &tx,
                command,
                &operation,
                &authoring_node_owned,
                resource_version_hint,
                operation_now,
            )?;
            tx.commit()?;
            Ok(commit)
        })
        .await
        .map_err(|e| anyhow::anyhow!("build log_apply commit failed: {e}"))
    }

    /// T1.3/T1.4: build (without applying) the `LogApplyCommit` that the
    /// leader's raft proposer should submit. Mirrors the early stages of
    /// `apply_outbox_transactionally` (decode payload, lease-renew shortcut,
    /// validation but stops short of calling `apply_commit_in_tx`. It performs
    /// no proposal-time applied-outbox claim or metadata RV reservation;
    /// committed apply owns durable idempotency and the final result.
    pub async fn build_log_apply_commit_for_outbox(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: klights_cluster_core::command::StorageCommand,
        authoring_node: &str,
    ) -> std::result::Result<BuildOutboxOutcome, klights_cluster_core::OutboxApplyError> {
        use klights_cluster_core::{OutboxApplyError, OutboxOperation, subject_key_for_command};

        if operation == OutboxOperation::LeaseRenew.as_str() {
            klights_cluster_core::validate_lease_renew_command(&command, authoring_node)
                .map_err(|err| OutboxApplyError::ConflictTerminal(err.to_string()))?;
            return Ok(BuildOutboxOutcome::LeaseRenewShortcircuit);
        }
        let subject_key = subject_key_for_command(&command);
        let status_stamp = Self::pod_status_stamp_of(&command);
        let operation_now = self.wall_clock.now_utc();
        let now = operation_now.timestamp_millis();
        let claim_key = idempotency_key.to_string();
        let claim_operation = operation.to_string();
        let authoring_node_owned = authoring_node.to_string();
        let outbox_codec = self.outbox_codec.clone();

        let outcome = self
            .db_call("db_build_log_apply_commit_for_outbox", move |conn| {
                let context = live_apply::TransactionContext::new(outbox_codec.as_ref());
                let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
                if let Some(existing) = Self::completed_outbox_record_in_tx(&tx, &claim_key)? {
                    tx.commit()?;
                    return Ok(BuildOutboxTxnOutcome::AlreadyApplied(Some(existing)));
                }
                // This private, read-only candidate validates
                // the command materialization. Committed apply assigns the
                // durable resourceVersion after Raft commits the template.
                let resource_version_hint =
                    Self::outbox_materialization_resource_version_hint_in_tx(&tx)?;
                let (mut commit, mut rv) = Self::build_log_apply_commit_in_tx_from_command(
                    &tx,
                    command,
                    &claim_operation,
                    &authoring_node_owned,
                    resource_version_hint,
                    operation_now,
                )?;
                let _ledger_only =
                    Self::normalize_ledger_only_outbox_commit_in_tx(&tx, &commit, &mut rv)?;

                // The state-machine apply records the final idempotency
                // outcome. No proposal-time cluster DB claim is created;
                // duplicate proposals converge at apply time.
                Self::append_applied_outbox_ledger_mutation(
                    &mut commit,
                    AppliedOutboxLedgerInput {
                        idempotency_key: claim_key.clone(),
                        subject_key: subject_key.clone(),
                        operation: claim_operation.clone(),
                        first_seen_ms: now,
                        status_stamp,
                        terminal_error: None,
                    },
                    &context,
                );
                tx.commit()?;
                Ok(BuildOutboxTxnOutcome::Built {
                    commit,
                    rv,
                    terminal_error: None,
                })
            })
            .await
            .map_err(Self::outbox_apply_error_from_db_error)?;

        match outcome {
            BuildOutboxTxnOutcome::Built {
                commit,
                rv,
                terminal_error,
            } => Ok(BuildOutboxOutcome::NeedsPropose {
                commit,
                applied_rv: rv,
                terminal_error,
            }),
            BuildOutboxTxnOutcome::AlreadyApplied(record) => {
                let context = live_apply::TransactionContext::new(self.outbox_codec.as_ref());
                if let Some(message) =
                    Self::cached_outbox_terminal_error(record.as_ref(), &context)?
                {
                    return Err(klights_cluster_core::OutboxApplyError::ConflictTerminal(
                        message,
                    ));
                }
                let committed_resource =
                    Self::cached_outbox_committed_resource(record.as_ref(), &context)?;
                Ok(BuildOutboxOutcome::AlreadyApplied {
                    applied_rv: record.as_ref().and_then(|r| r.applied_rv),
                    committed_resource,
                })
            }
        }
    }

    pub async fn build_log_apply_commit_for_outbox_with_watermark(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: klights_cluster_core::command::StorageCommand,
        authoring_node: &str,
        watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
    ) -> std::result::Result<BuildOutboxOutcome, klights_cluster_core::OutboxApplyError> {
        if watermark.is_none() {
            return self
                .build_log_apply_commit_for_outbox(
                    idempotency_key,
                    operation,
                    command,
                    authoring_node,
                )
                .await;
        }
        use klights_cluster_core::{OutboxApplyError, OutboxOperation, subject_key_for_command};

        if operation == OutboxOperation::LeaseRenew.as_str() {
            klights_cluster_core::validate_lease_renew_command(&command, authoring_node)
                .map_err(|err| OutboxApplyError::ConflictTerminal(err.to_string()))?;
            return Ok(BuildOutboxOutcome::LeaseRenewShortcircuit);
        }
        let subject_key = subject_key_for_command(&command);
        let status_stamp = Self::pod_status_stamp_of(&command);
        let operation_now = self.wall_clock.now_utc();
        let now = operation_now.timestamp_millis();
        let claim_key = idempotency_key.to_string();
        let claim_operation = operation.to_string();
        let authoring_node_owned = authoring_node.to_string();
        let watermark_for_tx = watermark.clone();
        let outbox_codec = self.outbox_codec.clone();

        let outcome = self
            .db_call("db_build_log_apply_commit_for_outbox_watermark", move |conn| {
                let context = live_apply::TransactionContext::new(outbox_codec.as_ref());
                let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
                if let Some(existing) =
                    Self::completed_outbox_record_in_tx(&tx, &claim_key)?
                {
                    tx.commit()?;
                    return Ok(BuildOutboxTxnOutcome::AlreadyApplied(Some(existing)));
                }
                // This candidate only validates/materializes the command.
                // Proposal build never reserves public allocator state.
                let resource_version_hint =
                    Self::outbox_materialization_resource_version_hint_in_tx(&tx)?;
                if let Some(ref watermark) = watermark_for_tx {
                    let last_seq: Option<i64> = tx
                        .query_row(
                            "SELECT last_seq FROM outbox_stream_watermarks WHERE client_id = ?1 AND stream_id = ?2",
                            rusqlite::params![&watermark.client_id, watermark.stream_id],
                            |row| row.get(0),
                        )
                        .optional()?;
                    if watermark.stream_seq <= 0 {
                        return Err(live_apply::other_error(
                            "outbox stream seq must be positive",
                        ));
                    }
                    if let Some(last_seq) = last_seq {
                        if watermark.stream_seq <= last_seq {
                            tx.commit()?;
                            return Ok(BuildOutboxTxnOutcome::AlreadyApplied(None));
                        }
                        if watermark.stream_seq != last_seq.saturating_add(1) {
                            return Err(live_apply::other_error(
                                format!(
                                    "outbox stream gap for seq {}: last committed seq is {}",
                                    watermark.stream_seq, last_seq
                                ),
                            ));
                        }
                    } else if watermark.stream_seq != 1 {
                        return Err(live_apply::other_error(
                            format!(
                                "outbox stream gap for seq {}: last committed seq is 0",
                                watermark.stream_seq
                            ),
                        ));
                    }
                }
                if let Some(terminal_error) =
                    Self::terminal_error_for_stale_uid_bound_pod_in_tx(
                        &tx,
                        &command,
                    )?
                {
                    // A stale UID-bound Pod delivery consumes only its durable
                    // outbox ledger position. It has no public resource or
                    // watch effect, so proposal build must not reserve a
                    // public resourceVersion while materializing the commit.
                    let rv = Self::current_resource_version_in_tx(&tx)?;
                    let mut commit = Self::author_live_commit(rv, Vec::new())?;
                    Self::set_live_commit_watermark(&mut commit, watermark_for_tx);
                    Self::append_applied_outbox_ledger_mutation(
                        &mut commit,
                        AppliedOutboxLedgerInput {
                            idempotency_key: claim_key.clone(),
                            subject_key: subject_key.clone(),
                            operation: claim_operation.clone(),
                            first_seen_ms: now,
                            status_stamp,
                            terminal_error: Some(&terminal_error),
                        },
                        &context,
                    );
                    tx.commit()?;
                    return Ok(BuildOutboxTxnOutcome::Built {
                        commit,
                        rv,
                        terminal_error: Some(terminal_error),
                    });
                }
                if Self::should_consume_watermark_for_idempotent_existing_create_in_tx(
                    &tx,
                    &claim_operation,
                    &command,
                )? {
                    let rv = resource_version_hint.expect("fixed contract always has a candidate");
                    let mut commit = Self::author_live_commit(rv, Vec::new())?;
                    Self::set_live_commit_watermark(&mut commit, watermark_for_tx);
                    Self::append_applied_outbox_ledger_mutation(
                        &mut commit,
                        AppliedOutboxLedgerInput {
                            idempotency_key: claim_key.clone(),
                            subject_key: subject_key.clone(),
                            operation: claim_operation.clone(),
                            first_seen_ms: now,
                            status_stamp,
                            terminal_error: None,
                        },
                        &context,
                    );
                    tx.commit()?;
                    return Ok(BuildOutboxTxnOutcome::Built {
                        commit,
                        rv,
                        terminal_error: None,
                    });
                }
                let (mut commit, mut rv) = match Self::build_log_apply_commit_in_tx_from_command(
                    &tx,
                    command,
                    &claim_operation,
                    &authoring_node_owned,
                    resource_version_hint,
                    operation_now,
                ) {
                    Ok(built) => built,
                    Err(error) if error.to_string().contains("409 Conflict") => {
                        let terminal_error = OutboxApplyError::ConflictTerminal(error.to_string());
                        let rv = Self::current_resource_version_in_tx(&tx)?;
                        let mut commit = Self::author_live_commit(rv, Vec::new())?;
                        Self::set_live_commit_watermark(&mut commit, watermark_for_tx);
                        Self::append_applied_outbox_ledger_mutation(
                            &mut commit,
                            AppliedOutboxLedgerInput {
                                idempotency_key: claim_key.clone(),
                                subject_key: subject_key.clone(),
                                operation: claim_operation.clone(),
                                first_seen_ms: now,
                                status_stamp,
                                terminal_error: Some(&terminal_error),
                            },
                            &context,
                        );
                        tx.commit()?;
                        return Ok(BuildOutboxTxnOutcome::Built {
                            commit,
                            rv,
                            terminal_error: Some(terminal_error),
                        });
                    }
                    Err(error) => return Err(error),
                };
                let _ledger_only =
                    Self::normalize_ledger_only_outbox_commit_in_tx(&tx, &commit, &mut rv)?;
                Self::set_live_commit_watermark(&mut commit, watermark_for_tx);
                Self::append_applied_outbox_ledger_mutation(
                    &mut commit,
                    AppliedOutboxLedgerInput {
                        idempotency_key: claim_key.clone(),
                        subject_key: subject_key.clone(),
                        operation: claim_operation.clone(),
                        first_seen_ms: now,
                        status_stamp,
                        terminal_error: None,
                    },
                    &context,
                );
                tx.commit()?;
                Ok(BuildOutboxTxnOutcome::Built {
                    commit,
                    rv,
                    terminal_error: None,
                })
            })
            .await
            .map_err(Self::outbox_apply_error_from_db_error)?;

        match outcome {
            BuildOutboxTxnOutcome::Built {
                commit,
                rv,
                terminal_error,
            } => Ok(BuildOutboxOutcome::NeedsPropose {
                commit,
                applied_rv: rv,
                terminal_error,
            }),
            BuildOutboxTxnOutcome::AlreadyApplied(record) => {
                let context = live_apply::TransactionContext::new(self.outbox_codec.as_ref());
                let committed_resource =
                    Self::cached_outbox_committed_resource(record.as_ref(), &context)?;
                Ok(BuildOutboxOutcome::AlreadyApplied {
                    applied_rv: record.as_ref().and_then(|r| r.applied_rv),
                    committed_resource,
                })
            }
        }
    }

    pub async fn apply_outbox_transactionally(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: klights_cluster_core::command::StorageCommand,
        authoring_node: &str,
    ) -> std::result::Result<
        klights_cluster_core::OutboxApplyOutcome,
        klights_cluster_core::OutboxApplyError,
    > {
        self.apply_outbox_transactionally_effect(
            idempotency_key,
            operation,
            command,
            authoring_node,
        )
        .await
        .map(|effect| effect.into_parts().0)
    }

    async fn apply_outbox_transactionally_effect(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: klights_cluster_core::command::StorageCommand,
        authoring_node: &str,
    ) -> std::result::Result<
        klights_cluster_store::CommittedOutboxApply,
        klights_cluster_core::OutboxApplyError,
    > {
        use klights_cluster_core::{
            OutboxApplyError, OutboxApplyOutcome, OutboxOperation, subject_key_for_command,
        };
        if operation == OutboxOperation::LeaseRenew.as_str() {
            klights_cluster_core::validate_lease_renew_command(&command, authoring_node)
                .map_err(|err| OutboxApplyError::ConflictTerminal(err.to_string()))?;
            return Ok(klights_cluster_store::CommittedOutboxApply::new(
                OutboxApplyOutcome::Applied { applied_rv: 0 },
                klights_cluster_core::ResourceMutationEffect::Unchanged,
                klights_cluster_core::PodEndpointEffect::NotApplicable,
            ));
        }
        let pod_target = match &command {
            klights_cluster_core::command::StorageCommand::UpdateStatus {
                api_version,
                kind,
                namespace,
                name,
                ..
            } if api_version == "v1" && kind == "Pod" => Some((namespace.clone(), name.clone())),
            _ => None,
        };
        let is_pod_status = pod_target.is_some();
        let subject_key = subject_key_for_command(&command);
        let status_stamp = Self::pod_status_stamp_of(&command);
        let operation_now = self.wall_clock.now_utc();
        let now = operation_now.timestamp_millis();

        let claim_key = idempotency_key.to_string();
        let claim_operation = operation.to_string();
        let authoring_node = authoring_node.to_string();
        let outbox_codec = self.outbox_codec.clone();
        let outcome = self
            .db_call("db_apply_outbox_atomic", move |conn| {
                let context = live_apply::TransactionContext::new(outbox_codec.as_ref());
                let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let pod_state = |tx: &rusqlite::Transaction<'_>| {
                    let Some((namespace, name)) = pod_target.as_ref() else {
                        return Ok(None);
                    };
                    let bytes = match namespace.as_deref() {
                        Some(namespace) => tx
                            .query_row(
                                queries::NAMESPACED_GET_DATA_FOR_DELETE,
                                rusqlite::params!["v1", "Pod", namespace, name],
                                |row| row.get::<_, Vec<u8>>(2),
                            )
                            .optional()?,
                        None => tx
                            .query_row(
                                queries::CLUSTER_GET_DATA_FOR_DELETE,
                                rusqlite::params!["v1", "Pod", name],
                                |row| row.get::<_, Vec<u8>>(2),
                            )
                            .optional()?,
                    };
                    bytes
                        .map(|bytes| {
                            serde_json::from_slice(&bytes)
                                .map_err(crate::sqlite::mutation_helpers::serde_to_sqlite_error)
                        })
                        .transpose()
                };
                let pod_before = pod_state(&tx)?;

                let mut existing: Option<LogApplyAppliedOutboxRow> = tx
                    .query_row(queries::APPLIED_OUTBOX_GET, [&claim_key], |row| {
                        Ok(LogApplyAppliedOutboxRow {
                            idempotency_key: row.get(0)?,
                            subject_key: row.get(1)?,
                            operation: row.get(2)?,
                            first_seen_ms: row.get(3)?,
                            applied_rv: row.get(4)?,
                            result_proto: row.get(5)?,
                            status_stamp: row.get(6)?,
                        })
                    })
                    .optional()?;

                if existing.is_some() {
                    tx.commit()?;
                    return Ok(OutboxTxnOutcome::AlreadyApplied(existing));
                }

                tx.execute(
                    queries::APPLIED_OUTBOX_INSERT,
                    rusqlite::params![
                        &claim_key,
                        "",
                        &claim_operation,
                        now,
                        Option::<i64>::None,
                        Vec::<u8>::new(),
                        Option::<i64>::None
                    ],
                )?;
                let mutation = Self::apply_outbox_command_in_tx_with_context(
                    &tx,
                    command,
                    &claim_operation,
                    &authoring_node,
                    &context,
                    operation_now,
                )?;
                let pod_after = pod_state(&tx)?;
                let pod_endpoint_effect = if pod_target.is_none() {
                    klights_cluster_core::PodEndpointEffect::NotApplicable
                } else if pod_before.as_ref().zip(pod_after.as_ref()).is_some_and(
                    |(before, after)| {
                        klights_cluster_core::pod_endpoint_state(before)
                            .differs_from(&klights_cluster_core::pod_endpoint_state(after))
                    },
                ) {
                    klights_cluster_core::PodEndpointEffect::Changed
                } else {
                    klights_cluster_core::PodEndpointEffect::Unchanged
                };
                tx.execute(
                    queries::APPLIED_OUTBOX_UPDATE_RESULT,
                    rusqlite::params![
                        &claim_key,
                        subject_key,
                        mutation.applied_rv,
                        mutation.result_proto,
                        status_stamp
                    ],
                )?;
                if tx.changes() == 0 {
                    existing = tx
                        .query_row(queries::APPLIED_OUTBOX_GET, [&claim_key], |row| {
                            Ok(LogApplyAppliedOutboxRow {
                                idempotency_key: row.get(0)?,
                                subject_key: row.get(1)?,
                                operation: row.get(2)?,
                                first_seen_ms: row.get(3)?,
                                applied_rv: row.get(4)?,
                                result_proto: row.get(5)?,
                                status_stamp: row.get(6)?,
                            })
                        })
                        .optional()?;
                    tx.commit()?;
                    return Ok(OutboxTxnOutcome::AlreadyApplied(existing));
                }
                tx.commit()?;
                Ok(OutboxTxnOutcome::Applied {
                    applied_rv: mutation.applied_rv.unwrap_or(0),
                    resource_changed: mutation.pending.is_some(),
                    pending: mutation.pending,
                    pod_endpoint_effect,
                    committed_resource: mutation.committed_resource,
                })
            })
            .await
            .map_err(Self::outbox_apply_error_from_db_error)?;

        match outcome {
            OutboxTxnOutcome::Applied {
                applied_rv,
                pending,
                resource_changed,
                pod_endpoint_effect,
                committed_resource,
            } => {
                if let Some(_pending) = pending {
                    #[cfg(test)]
                    self.publish_watch_event(_pending);
                }
                Ok(klights_cluster_store::CommittedOutboxApply::new(
                    klights_cluster_core::OutboxApplyOutcome::Applied { applied_rv },
                    if resource_changed {
                        klights_cluster_core::ResourceMutationEffect::Changed
                    } else {
                        klights_cluster_core::ResourceMutationEffect::Unchanged
                    },
                    pod_endpoint_effect,
                )
                .with_committed_resource(committed_resource))
            }
            OutboxTxnOutcome::AlreadyApplied(record) => {
                let context = live_apply::TransactionContext::new(self.outbox_codec.as_ref());
                if let Some(message) =
                    Self::cached_outbox_terminal_error(record.as_ref(), &context)?
                {
                    return Err(OutboxApplyError::ConflictTerminal(message));
                }
                let committed_resource =
                    Self::cached_outbox_committed_resource(record.as_ref(), &context)?;
                let applied_rv = record.as_ref().and_then(|record| record.applied_rv);
                Ok(klights_cluster_store::CommittedOutboxApply::new(
                    klights_cluster_core::OutboxApplyOutcome::AlreadyApplied { applied_rv },
                    klights_cluster_core::ResourceMutationEffect::Unchanged,
                    if is_pod_status {
                        klights_cluster_core::PodEndpointEffect::Unchanged
                    } else {
                        klights_cluster_core::PodEndpointEffect::NotApplicable
                    },
                )
                .with_committed_resource(committed_resource))
            }
        }
    }

    /// Apply a StorageCommand within a transaction by converting it to
    /// a LogApplyCommit and routing through `apply_commit_in_tx`. This ensures:
    ///   - All StorageCommand variants are supported (no "unsupported" gap)
    ///   - Leader-local outbox apply and raft state-machine replay share row semantics
    ///   - The applied outbox result is derived from the same committed mutation data
    fn apply_outbox_command_in_tx_with_context(
        tx: &rusqlite::Transaction<'_>,
        command: klights_cluster_core::command::StorageCommand,
        operation: &str,
        authoring_node: &str,
        context: &live_apply::TransactionContext<'_>,
        operation_now: chrono::DateTime<chrono::Utc>,
    ) -> tokio_rusqlite::Result<AtomicOutboxMutation> {
        use klights_cluster_core::StorageResponse;

        let durable_actor_cascade = Self::is_bound_pod_finalization_delivery(&command, operation);
        let candidate_rv = Self::current_resource_version_in_tx(tx)?.saturating_add(1);
        let (commit, _provisional_rv) = Self::build_log_apply_commit_in_tx_from_command(
            tx,
            command,
            operation,
            authoring_node,
            Some(candidate_rv),
            operation_now,
        )?;

        let (applied_rv, pending, applied_mutation) =
            live_apply::apply_commit_in_tx_returning_rv_and_mutation_with_context(
                tx, commit, context,
            )?;

        let pending_event = pending.into_iter().next();
        let committed_resource = applied_mutation;
        let response = if durable_actor_cascade {
            committed_resource.as_ref().map_or(
                StorageResponse::Ack {
                    resource_version: applied_rv,
                },
                |resource| StorageResponse::Resource {
                    resource_version: resource.resource_version,
                    data: (*resource.data).clone(),
                },
            )
        } else {
            StorageResponse::Ack {
                resource_version: applied_rv,
            }
        };
        let result_proto = context.encode(&response).unwrap_or_default();
        Ok(AtomicOutboxMutation {
            applied_rv: Some(applied_rv),
            result_proto,
            pending: pending_event,
            committed_resource,
        })
    }

    #[cfg(test)]
    fn apply_outbox_command_in_tx(
        tx: &rusqlite::Transaction<'_>,
        command: klights_cluster_core::command::StorageCommand,
        operation: &str,
        authoring_node: &str,
    ) -> tokio_rusqlite::Result<AtomicOutboxMutation> {
        let codec = crate::test_fixtures::outbox::new_codec();
        let context = live_apply::TransactionContext::new(codec.as_ref());
        Self::apply_outbox_command_in_tx_with_context(
            tx,
            command,
            operation,
            authoring_node,
            &context,
            chrono::DateTime::<chrono::Utc>::from_timestamp(0, 0)
                .expect("Unix epoch is a valid test timestamp"),
        )
    }

    fn resource_snapshot_for_key_at_rv_in_tx(
        tx: &rusqlite::Transaction<'_>,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        resource_version: i64,
    ) -> tokio_rusqlite::Result<Option<Value>> {
        transaction_primitives::resource_snapshot_for_key_at_rv(
            tx,
            api_version,
            kind,
            namespace,
            name,
            resource_version,
        )
    }

    fn rebase_stale_pod_metadata_update(
        base: &Value,
        incoming: &Value,
        live: &Value,
    ) -> Option<Value> {
        let mut rebased = live.clone();
        let mut changed = false;

        changed |=
            Self::apply_metadata_field_delta(&mut rebased, base, incoming, "ownerReferences");
        changed |= Self::apply_metadata_map_delta(&mut rebased, base, incoming, "labels");
        changed |= Self::apply_metadata_map_delta(&mut rebased, base, incoming, "annotations");
        changed |=
            Self::apply_metadata_field_delta(&mut rebased, base, incoming, "deletionTimestamp");
        changed |= Self::apply_metadata_field_delta(
            &mut rebased,
            base,
            incoming,
            "deletionGracePeriodSeconds",
        );

        changed.then_some(rebased)
    }

    fn metadata_field<'a>(data: &'a Value, field: &str) -> Option<&'a Value> {
        data.get("metadata")
            .and_then(Value::as_object)
            .and_then(|metadata| metadata.get(field))
    }

    fn metadata_object_mut(
        data: &mut Value,
    ) -> Option<&mut serde_json::Map<String, serde_json::Value>> {
        let object = data.as_object_mut()?;
        let metadata = object
            .entry("metadata".to_string())
            .or_insert_with(|| serde_json::json!({}));
        metadata.as_object_mut()
    }

    fn apply_metadata_field_delta(
        rebased: &mut Value,
        base: &Value,
        incoming: &Value,
        field: &str,
    ) -> bool {
        let base_value = Self::metadata_field(base, field);
        let incoming_value = Self::metadata_field(incoming, field);
        if base_value == incoming_value {
            return false;
        }
        let Some(metadata) = Self::metadata_object_mut(rebased) else {
            return false;
        };
        match incoming_value {
            Some(value) => {
                if metadata.get(field) == Some(value) {
                    false
                } else {
                    metadata.insert(field.to_string(), value.clone());
                    true
                }
            }
            None => metadata.remove(field).is_some(),
        }
    }

    fn apply_metadata_map_delta(
        rebased: &mut Value,
        base: &Value,
        incoming: &Value,
        field: &str,
    ) -> bool {
        let base_value = Self::metadata_field(base, field);
        let incoming_value = Self::metadata_field(incoming, field);
        if base_value == incoming_value {
            return false;
        }
        let Some(base_map) = base_value.and_then(Value::as_object) else {
            return Self::apply_metadata_field_delta(rebased, base, incoming, field);
        };
        let Some(incoming_map) = incoming_value.and_then(Value::as_object) else {
            return Self::apply_metadata_field_delta(rebased, base, incoming, field);
        };
        let Some(metadata) = Self::metadata_object_mut(rebased) else {
            return false;
        };
        let entries = metadata
            .entry(field.to_string())
            .or_insert_with(|| serde_json::json!({}));
        if !entries.is_object() {
            *entries = serde_json::json!({});
        }
        let Some(entries) = entries.as_object_mut() else {
            return false;
        };

        let mut changed = false;
        for key in base_map.keys() {
            if !incoming_map.contains_key(key) {
                changed |= entries.remove(key).is_some();
            }
        }
        for (key, value) in incoming_map {
            if base_map.get(key) != Some(value) && entries.get(key) != Some(value) {
                entries.insert(key.clone(), value.clone());
                changed = true;
            }
        }
        changed
    }

    fn should_apply_outbox_patch_against_latest(
        api_version: &str,
        kind: &str,
        patch_kind: PatchKind,
        patch: &Value,
        preconditions: &ResourcePreconditions,
    ) -> bool {
        if patch_kind != PatchKind::Merge
            || preconditions.uid.is_none()
            || preconditions.resource_version.is_some()
        {
            return false;
        }
        Self::is_unconditional_workload_scale_patch(api_version, kind, patch)
            || Self::is_pod_delete_mark_patch(api_version, kind, patch)
    }

    fn is_unconditional_workload_scale_patch(api_version: &str, kind: &str, patch: &Value) -> bool {
        if !matches!(
            (api_version, kind),
            ("apps/v1", "Deployment")
                | ("apps/v1", "ReplicaSet")
                | ("apps/v1", "StatefulSet")
                | ("v1", "ReplicationController")
        ) {
            return false;
        }
        let Some(patch_obj) = patch.as_object() else {
            return false;
        };
        if patch_obj.len() != 1 {
            return false;
        }
        let Some(spec_obj) = patch_obj.get("spec").and_then(Value::as_object) else {
            return false;
        };
        spec_obj.len() == 1 && spec_obj.contains_key("replicas")
    }

    fn is_pod_delete_mark_patch(api_version: &str, kind: &str, patch: &Value) -> bool {
        klights_types::is_pod_delete_mark_patch(api_version, kind, patch)
    }

    fn apply_outbox_patch(
        api_version: &str,
        kind: &str,
        live: &mut Value,
        patch_kind: klights_cluster_core::PatchKind,
        patch: Value,
    ) -> tokio_rusqlite::Result<()> {
        let _ = (api_version, kind);
        let existing = live.clone();
        match patch_kind {
            klights_cluster_core::PatchKind::Merge => {
                klights_types::apply_merge_patch(live, &patch);
            }
        }
        crate::sqlite::resource_shape::validate_metadata_uid_immutable(live, &existing)
            .map_err(Self::sqlite_conversion_error)?;
        crate::sqlite::resource_shape::preserve_server_metadata_fields_from_existing(
            live, &existing,
        );
        Ok(())
    }

    // merge_forwarded_lease_with_live is defined later in this file

    fn node_routing_metadata_resource_row_in_tx(
        tx: &rusqlite::Transaction<'_>,
        node_name: &str,
        metadata: &klights_cluster_store::DataplanePeerMetadata,
        resource_version: i64,
    ) -> tokio_rusqlite::Result<Option<klights_cluster_core::LogApplyResourceRow>> {
        let Some((_current_rv, current_uid, current_bytes)) = tx
            .query_row(
                queries::CLUSTER_GET_DATA_FOR_DELETE,
                rusqlite::params!["v1", "Node", node_name],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                    ))
                },
            )
            .optional()?
        else {
            return Ok(None);
        };
        let mut node: Value = serde_json::from_slice(&current_bytes)
            .map_err(crate::sqlite::mutation_helpers::serde_to_sqlite_error)?;
        let mut changed = false;
        let pod_cidr = tx
            .query_row(queries::NODE_SUBNET_SELECT_BY_NAME, [node_name], |row| {
                row.get::<_, String>(1)
            })
            .optional()?;
        if let Some(pod_cidr) = pod_cidr.as_deref() {
            changed |= klights_cluster_core::set_node_pod_cidr(&mut node, pod_cidr);
        }
        changed |=
            klights_cluster_core::set_node_external_ip(&mut node, &metadata.endpoint.to_string());
        changed |= klights_types::set_node_dataplane_annotations(
            &mut node,
            &metadata.endpoint.to_string(),
            metadata.mode.as_str(),
            metadata.encryption.as_str(),
            metadata.public_key.as_ref().map(|key| key.as_str()),
            metadata.port,
        );
        if !changed {
            return Ok(None);
        }

        Ok(Some(klights_cluster_core::LogApplyResourceRow {
            api_version: "v1".to_string(),
            kind: "Node".to_string(),
            namespace: None,
            name: node_name.to_string(),
            uid: current_uid,
            resource_version,
            data: node,
            require_absent: false,
            require_existing: true,
            precondition_uid: None,
            precondition_resource_version: None,
            status_only: false,
        }))
    }

    pub async fn apply_outbox_transactionally_with_watermark(
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
        self.apply_outbox_transactionally_with_watermark_effect(
            idempotency_key,
            operation,
            command,
            authoring_node,
            watermark,
        )
        .await
        .map(|effect| effect.into_parts().0)
    }

    pub async fn apply_outbox_transactionally_with_watermark_effect(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: klights_cluster_core::command::StorageCommand,
        authoring_node: &str,
        watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
    ) -> std::result::Result<
        klights_cluster_store::CommittedOutboxApply,
        klights_cluster_core::OutboxApplyError,
    > {
        use klights_cluster_core::BuildOutboxOutcome;
        use klights_cluster_core::{OutboxApplyError, OutboxApplyOutcome};

        if watermark.is_none() {
            return self
                .apply_outbox_transactionally_effect(
                    idempotency_key,
                    operation,
                    command,
                    authoring_node,
                )
                .await;
        }
        match self
            .build_log_apply_commit_for_outbox_with_watermark(
                idempotency_key,
                operation,
                command,
                authoring_node,
                watermark,
            )
            .await?
        {
            BuildOutboxOutcome::NeedsPropose {
                commit,
                applied_rv,
                terminal_error,
            } => {
                use klights_cluster_store::PrivilegedCommittedRaftApply;
                let receipt = self
                    .live_committed_apply
                    .apply_committed_raft(klights_cluster_store::CommittedRaftApplyRequest::new(
                        commit,
                    ))
                    .await
                    .map_err(|err| OutboxApplyError::Retryable(err.to_string()))?;
                if let Some(error) = terminal_error {
                    return Err(error);
                }
                if let Some(message) = receipt.terminal_rejection() {
                    return Err(OutboxApplyError::ConflictTerminal(message.to_string()));
                }
                let committed_resource = receipt.applied_resource().cloned();
                let public_resource_changed = matches!(
                    receipt.outcome(),
                    klights_cluster_core::CommittedApplyOutcome::Visible { .. }
                );
                Ok(klights_cluster_store::CommittedOutboxApply::new(
                    OutboxApplyOutcome::Applied {
                        applied_rv: receipt.applied_resource_version().unwrap_or(applied_rv),
                    },
                    if public_resource_changed {
                        klights_cluster_core::ResourceMutationEffect::Changed
                    } else {
                        klights_cluster_core::ResourceMutationEffect::Unchanged
                    },
                    receipt.pod_endpoint_effect(),
                )
                .with_committed_resource(committed_resource))
            }
            BuildOutboxOutcome::AlreadyApplied {
                applied_rv,
                committed_resource,
            } => Ok(klights_cluster_store::CommittedOutboxApply::new(
                OutboxApplyOutcome::AlreadyApplied { applied_rv },
                klights_cluster_core::ResourceMutationEffect::Unchanged,
                klights_cluster_core::PodEndpointEffect::Unchanged,
            )
            .with_committed_resource(committed_resource)),
            BuildOutboxOutcome::LeaseRenewShortcircuit => {
                Ok(klights_cluster_store::CommittedOutboxApply::new(
                    OutboxApplyOutcome::Applied { applied_rv: 0 },
                    klights_cluster_core::ResourceMutationEffect::Unchanged,
                    klights_cluster_core::PodEndpointEffect::NotApplicable,
                ))
            }
        }
    }

    fn should_consume_watermark_for_idempotent_existing_create_in_tx(
        tx: &rusqlite::Transaction<'_>,
        operation: &str,
        command: &klights_cluster_core::command::StorageCommand,
    ) -> tokio_rusqlite::Result<bool> {
        let idempotent_create = operation
            == klights_cluster_core::OutboxOperation::EventCreate.as_str()
            || operation == klights_cluster_core::OutboxOperation::NodeRegistration.as_str();
        if !idempotent_create {
            return Ok(false);
        }
        let klights_cluster_core::command::StorageCommand::CreateResource {
            api_version,
            kind,
            namespace,
            name,
            ..
        } = command
        else {
            return Ok(false);
        };
        Self::resource_row_optional_for_update_in_tx(
            tx,
            api_version,
            kind,
            namespace.as_deref(),
            name,
        )
        .map(|existing| existing.is_some())
    }

    fn terminal_error_for_stale_uid_bound_pod_in_tx(
        tx: &rusqlite::Transaction<'_>,
        command: &klights_cluster_core::command::StorageCommand,
    ) -> tokio_rusqlite::Result<Option<klights_cluster_core::OutboxApplyError>> {
        let Some((namespace, name, expected_uid)) = Self::uid_bound_pod_target(command) else {
            return Ok(None);
        };
        match Self::resource_row_optional_for_update_in_tx(tx, "v1", "Pod", Some(namespace), name)?
        {
            Some((_rv, live_uid, _data)) if live_uid != expected_uid => {
                Ok(Some(klights_cluster_core::OutboxApplyError::UidMismatch {
                    expected: expected_uid.to_string(),
                    actual: live_uid,
                }))
            }
            Some(_) => Ok(None),
            None => Ok(Some(klights_cluster_core::OutboxApplyError::NotFound(
                format!("Pod {namespace}/{name} not found"),
            ))),
        }
    }

    fn uid_bound_pod_target(
        command: &klights_cluster_core::command::StorageCommand,
    ) -> Option<(&str, &str, &str)> {
        use klights_cluster_core::command::StorageCommand;
        let (api_version, kind, namespace, name, preconditions) = match command {
            StorageCommand::UpdateStatus {
                api_version,
                kind,
                namespace,
                name,
                preconditions,
                ..
            }
            | StorageCommand::UpdateResource {
                api_version,
                kind,
                namespace,
                name,
                preconditions,
                ..
            }
            | StorageCommand::PatchResource {
                api_version,
                kind,
                namespace,
                name,
                preconditions,
                ..
            }
            | StorageCommand::DeleteResource {
                api_version,
                kind,
                namespace,
                name,
                preconditions,
            }
            | StorageCommand::DeleteResourceWithTombstone {
                api_version,
                kind,
                namespace,
                name,
                preconditions,
                grace_seconds: _,
            } => (api_version, kind, namespace, name, preconditions),
            _ => return None,
        };
        if api_version != "v1" || kind != "Pod" {
            return None;
        }
        let namespace = namespace.as_deref()?;
        let expected_uid = preconditions.uid.as_deref().filter(|uid| !uid.is_empty())?;
        Some((namespace, name.as_str(), expected_uid))
    }

    fn sqlite_conversion_error(err: impl std::fmt::Display) -> tokio_rusqlite::Error {
        tokio_rusqlite::Error::Rusqlite(rusqlite::Error::ToSqlConversionFailure(Box::new(
            std::io::Error::other(err.to_string()),
        )))
    }

    fn outbox_apply_error_from_db_error(
        err: tokio_rusqlite::Error,
    ) -> klights_cluster_core::OutboxApplyError {
        let msg = err.to_string();
        if msg.contains("409 Conflict") {
            klights_cluster_core::OutboxApplyError::ConflictTerminal(msg)
        } else {
            klights_cluster_core::OutboxApplyError::Retryable(msg)
        }
    }

    fn cached_outbox_terminal_error(
        record: Option<&LogApplyAppliedOutboxRow>,
        context: &live_apply::TransactionContext<'_>,
    ) -> std::result::Result<Option<String>, klights_cluster_core::OutboxApplyError> {
        let Some(record) = record else {
            return Ok(None);
        };
        if record.result_proto.is_empty() {
            return Ok(None);
        }
        match context.decode(&record.result_proto) {
            Ok(klights_cluster_core::command::StorageResponse::Error { message }) => {
                Ok(Some(message))
            }
            Ok(_) => Ok(None),
            Err(err) => Err(klights_cluster_core::OutboxApplyError::Retryable(format!(
                "decode cached applied_outbox response: {err}"
            ))),
        }
    }

    fn cached_outbox_committed_resource(
        record: Option<&LogApplyAppliedOutboxRow>,
        context: &live_apply::TransactionContext<'_>,
    ) -> std::result::Result<
        Option<klights_cluster_core::Resource>,
        klights_cluster_core::OutboxApplyError,
    > {
        let Some(record) = record else {
            return Ok(None);
        };
        match context.decode(&record.result_proto) {
            Ok(klights_cluster_core::command::StorageResponse::Resource {
                resource_version,
                data,
            }) => {
                let mut resource =
                    klights_cluster_core::Resource::try_from_data(std::sync::Arc::new(data))
                        .map_err(|error| {
                            klights_cluster_core::OutboxApplyError::Retryable(format!(
                                "decode cached applied_outbox resource identity: {error}"
                            ))
                        })?;
                resource.resource_version = resource_version;
                Ok(Some(resource))
            }
            Ok(_) => Ok(None),
            Err(error) => Err(klights_cluster_core::OutboxApplyError::Retryable(format!(
                "decode cached applied_outbox response: {error}"
            ))),
        }
    }

    fn should_apply_outbox_update_against_latest(
        operation: &str,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        authoring_node: &str,
    ) -> bool {
        name == authoring_node
            && ((operation == klights_cluster_core::OutboxOperation::NodeStatus.as_str()
                && api_version == "v1"
                && kind == "Node"
                && namespace.is_none())
                || (operation == klights_cluster_core::OutboxOperation::LeaseRenew.as_str()
                    && api_version == "coordination.k8s.io/v1"
                    && kind == "Lease"
                    && namespace == Some("kube-node-lease")))
    }

    /// Reconstruct the applied_outbox `subject_key` for a Pod status command,
    /// matching `subject_key_for_command` so the stale-stamp gate reads the
    /// same ledger rows the outbox apply writes.
    fn pod_status_subject_key(
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        uid: Option<&str>,
    ) -> String {
        let mut key = match namespace {
            Some(namespace) => format!("{api_version}/{kind}/{namespace}/{name}"),
            None => format!("{api_version}/{kind}/{name}"),
        };
        if let Some(uid) = uid.filter(|uid| !uid.is_empty()) {
            key.push('/');
            key.push_str(uid);
        }
        key
    }

    /// Worker-observed status stamp carried by a Pod status outbox command, if
    /// any. Used by the outer apply paths to persist the stamp in the
    /// idempotency ledger so the gate can compare future snapshots.
    fn pod_status_stamp_of(command: &klights_cluster_core::command::StorageCommand) -> Option<i64> {
        match command {
            klights_cluster_core::command::StorageCommand::UpdateStatus {
                observed_status_stamp,
                ..
            } => *observed_status_stamp,
            _ => None,
        }
    }

    fn should_apply_outbox_status_against_latest(
        operation: &str,
        api_version: &str,
        kind: &str,
        preconditions: &ResourcePreconditions,
        observed_status_stamp: Option<i64>,
    ) -> bool {
        api_version == "v1"
            && kind == "Pod"
            && preconditions
                .uid
                .as_deref()
                .is_some_and(|uid| !uid.is_empty())
            && observed_status_stamp.is_some_and(|stamp| stamp > 0)
            && matches!(
                operation,
                "PodStatus"
                    | "RuntimeReconcile"
                    | "ProbeReadiness"
                    | "DeadlineExceeded"
                    | "ContainerStatusSnapshot"
                    | "EphemeralContainerStatuses"
            )
    }

    fn resource_row_for_update_in_tx(
        tx: &rusqlite::Transaction<'_>,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> tokio_rusqlite::Result<(i64, String, Vec<u8>)> {
        if use_namespaced_table(api_version, kind, &namespace) {
            tx.query_row(
                queries::NAMESPACED_GET_DATA_FOR_DELETE,
                rusqlite::params![api_version, kind, namespace.unwrap_or("default"), name],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(|err| match err {
                rusqlite::Error::QueryReturnedNoRows => Self::sqlite_conversion_error(anyhow!(
                    "Resource not found: {api_version}/{kind} {}/{}",
                    namespace.unwrap_or("default"),
                    name
                )),
                other => tokio_rusqlite::Error::Rusqlite(other),
            })
        } else {
            tx.query_row(
                queries::CLUSTER_GET_DATA_FOR_DELETE,
                rusqlite::params![api_version, kind, name],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(|err| match err {
                rusqlite::Error::QueryReturnedNoRows => Self::sqlite_conversion_error(anyhow!(
                    "Resource not found: {api_version}/{kind} {name}"
                )),
                other => tokio_rusqlite::Error::Rusqlite(other),
            })
        }
    }

    fn resource_row_optional_for_update_in_tx(
        tx: &rusqlite::Transaction<'_>,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> tokio_rusqlite::Result<Option<(i64, String, Vec<u8>)>> {
        if use_namespaced_table(api_version, kind, &namespace) {
            tx.query_row(
                queries::NAMESPACED_GET_DATA_FOR_DELETE,
                rusqlite::params![api_version, kind, namespace.unwrap_or("default"), name],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(tokio_rusqlite::Error::Rusqlite)
        } else {
            tx.query_row(
                queries::CLUSTER_GET_DATA_FOR_DELETE,
                rusqlite::params![api_version, kind, name],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(tokio_rusqlite::Error::Rusqlite)
        }
    }

    fn merge_forwarded_lease_with_live(live: &Value, incoming: &mut Value) {
        if let Some(metadata) = live.get("metadata").cloned() {
            incoming["metadata"] = metadata;
        }

        let live_renew_time = live.pointer("/spec/renewTime").and_then(|v| v.as_str());
        let incoming_renew_time = incoming.pointer("/spec/renewTime").and_then(|v| v.as_str());
        if Self::lease_renew_time_newer(live_renew_time, incoming_renew_time)
            && let Some(live_renew_time) = live_renew_time
        {
            incoming["spec"]["renewTime"] = Value::String(live_renew_time.to_string());
        }
    }

    fn lease_renew_time_newer(left: Option<&str>, right: Option<&str>) -> bool {
        let Some(left) = left else {
            return false;
        };
        let Some(right) = right else {
            return true;
        };
        match (
            chrono::DateTime::parse_from_rfc3339(left),
            chrono::DateTime::parse_from_rfc3339(right),
        ) {
            (Ok(left), Ok(right)) => left > right,
            _ => left > right,
        }
    }

    pub async fn gc_applied_outbox(&self, now_ms: i64, ttl_ms: i64) -> Result<usize> {
        let cutoff = now_ms.saturating_sub(ttl_ms);
        self.db_call("db_applied_outbox_gc", move |conn| {
            conn.execute(
                queries::APPLIED_OUTBOX_DELETE_EXPIRED,
                rusqlite::params![cutoff],
            )
            .map_err(tokio_rusqlite::Error::from)
        })
        .await
        .map_err(|e| anyhow!("applied outbox gc failed: {e}"))
    }

    // -------------------------------------------------------------------
    // Constructors — every path funnels through `from_executor` so the
    // shared body (broadcast channels, schema init if not already done)
    // is never duplicated. DSB-03 makes this the single source of truth.
    // -------------------------------------------------------------------

    /// Shared constructor body called by every public constructor.
    async fn from_executors(
        executor: DbExecutor,
        read_executor: DbExecutor,
        snapshot_factory: Option<crate::sqlite::recovery::SqliteSnapshotFactory>,
        #[cfg(any(test, feature = "test-support"))] commit_sink: Option<
            std::sync::Arc<dyn CommitObservationSink>,
        >,
        outbox_codec: std::sync::Arc<dyn OutboxResponseCodec>,
        wall_clock: std::sync::Arc<dyn klights_supervisor::WallClock>,
    ) -> Result<Self> {
        // Schema + fingerprint are applied by the cluster-owned open adapter.
        #[cfg(test)]
        let resource_get_call_count = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        #[cfg(test)]
        let fail_next_watch_position_observation =
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        #[cfg(test)]
        let focused_reads = std::sync::Arc::new(SqliteReadStore::new_with_test_instrumentation(
            read_executor.clone(),
            fail_next_watch_position_observation,
            resource_get_call_count.clone(),
        ));
        #[cfg(not(test))]
        let focused_reads = std::sync::Arc::new(SqliteReadStore::new(read_executor.clone()));
        let live_committed_apply = std::sync::Arc::new(
            crate::sqlite::live_apply::SqliteLiveCommittedApplyStore::new(
                executor.clone(),
                outbox_codec.clone(),
            ),
        );
        let snapshot_fence = std::sync::Arc::new(tokio::sync::RwLock::new(()));
        let focused_recovery =
            std::sync::Arc::new(crate::sqlite::recovery::SqliteRecoveryStore::new(
                executor.clone(),
                read_executor.clone(),
                snapshot_factory.clone(),
                snapshot_fence.clone(),
                outbox_codec.clone(),
            ));
        Ok(Self {
            executor,
            read_executor,
            focused_reads,
            live_committed_apply,
            focused_recovery,
            #[cfg(any(test, feature = "test-support"))]
            commit_sink,
            outbox_codec,
            wall_clock,
            snapshot_fence,
            #[cfg(test)]
            post_commit_publish_pause: std::sync::Arc::new(std::sync::Mutex::new(None)),
            #[cfg(any(test, feature = "test-support"))]
            resource_mutation_pause: std::sync::Arc::new(std::sync::Mutex::new(None)),
        })
    }

    pub fn focused_read_store(&self) -> std::sync::Arc<SqliteReadStore> {
        self.focused_reads.clone()
    }

    pub fn focused_committed_apply(
        &self,
    ) -> std::sync::Arc<dyn klights_cluster_store::PrivilegedCommittedRaftApply> {
        self.live_committed_apply.clone()
    }

    pub fn focused_recovery_store(
        &self,
    ) -> std::sync::Arc<crate::sqlite::recovery::SqliteRecoveryStore> {
        self.focused_recovery.clone()
    }

    async fn from_executor(
        executor: DbExecutor,
        #[cfg(any(test, feature = "test-support"))] commit_sink: Option<
            std::sync::Arc<dyn CommitObservationSink>,
        >,
        outbox_codec: std::sync::Arc<dyn OutboxResponseCodec>,
        wall_clock: std::sync::Arc<dyn klights_supervisor::WallClock>,
    ) -> Result<Self> {
        let snapshot_factory = executor.snapshot_open_opts().map(|opts| {
            crate::sqlite::recovery::SqliteSnapshotFactory::new(opts, executor.task_supervisor())
        });
        let read_executor = executor.read_lane_clone();
        Self::from_executors(
            executor,
            read_executor,
            snapshot_factory,
            #[cfg(any(test, feature = "test-support"))]
            commit_sink,
            outbox_codec,
            wall_clock,
        )
        .await
    }

    /// Production constructor — open a persistent on-disk database.
    ///
    /// Opens explicit cluster and node-local SQLite database files.
    ///
    /// If `key_file` is `Some`, the DB is opened with SQLCipher encryption
    /// (requires the `sqlcipher` cargo feature).
    async fn new_persistent_paths_inner(
        cluster_db_path: &std::path::Path,
        supervisor: std::sync::Arc<TaskSupervisor>,
        key_file: Option<&std::path::Path>,
        #[cfg(any(test, feature = "test-support"))] commit_sink: Option<
            std::sync::Arc<dyn CommitObservationSink>,
        >,
        outbox_codec: std::sync::Arc<dyn OutboxResponseCodec>,
        wall_clock: std::sync::Arc<dyn klights_supervisor::WallClock>,
    ) -> Result<Self> {
        let db_path = cluster_db_path.to_path_buf();
        let opts = opener::OpenOpts::disk(db_path.clone()).with_key_file(key_file)?;
        if let Some(kf) = key_file {
            tracing::info!(
                key_file = %kf.display(),
                "opening encrypted datastore"
            );
        }

        let executor = crate::sqlite::open_with_opts(opts, supervisor.clone(), "sqlite:cluster")
            .await
            .map_err(|e| {
                anyhow!(
                    "failed to open persistent cluster datastore at {}: {}",
                    db_path.display(),
                    e
                )
            })?;
        let read_opts = opener::OpenOpts::disk(db_path.clone()).with_key_file(key_file)?;
        let read_executor = crate::sqlite::open_read_only_with_opts(
            read_opts.clone(),
            supervisor.clone(),
            "sqlite:cluster-read",
        )
        .await
        .map_err(|e| {
            anyhow!(
                "failed to open read-only cluster datastore at {}: {}",
                db_path.display(),
                e
            )
        })?;
        let snapshot_factory =
            crate::sqlite::recovery::SqliteSnapshotFactory::new(read_opts, supervisor.clone());
        let ds = Self::from_executors(
            executor,
            read_executor,
            Some(snapshot_factory),
            #[cfg(any(test, feature = "test-support"))]
            commit_sink,
            outbox_codec,
            wall_clock,
        )
        .await?;

        // Log DB size at startup for operator triage (DSB-05).
        let (db_size, wal_size) = opener::persistent_datastore_sizes(&supervisor, &db_path).await?;
        tracing::info!(
            db_path = %db_path.display(),
            db_size_bytes = db_size,
            wal_size_bytes = wal_size,
            total_kb = (db_size + wal_size) / 1024,
            "persistent datastore opened"
        );

        Ok(ds)
    }

    /// Open a persistent datastore without installing test observation hooks.
    ///
    /// This production constructor has one invariant signature even when a
    /// downstream package also enables this crate's `test-support` feature.
    pub async fn new_persistent_paths(
        cluster_db_path: &std::path::Path,
        supervisor: std::sync::Arc<TaskSupervisor>,
        key_file: Option<&std::path::Path>,
        outbox_codec: std::sync::Arc<dyn OutboxResponseCodec>,
        wall_clock: std::sync::Arc<dyn klights_supervisor::WallClock>,
    ) -> Result<Self> {
        Self::new_persistent_paths_inner(
            cluster_db_path,
            supervisor,
            key_file,
            #[cfg(any(test, feature = "test-support"))]
            None,
            outbox_codec,
            wall_clock,
        )
        .await
    }

    /// Open a persistent datastore with a test commit-observation hook.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn new_persistent_paths_with_sink(
        cluster_db_path: &std::path::Path,
        supervisor: std::sync::Arc<TaskSupervisor>,
        key_file: Option<&std::path::Path>,
        commit_sink: std::sync::Arc<dyn CommitObservationSink>,
        outbox_codec: std::sync::Arc<dyn OutboxResponseCodec>,
        wall_clock: std::sync::Arc<dyn klights_supervisor::WallClock>,
    ) -> Result<Self> {
        Self::new_persistent_paths_inner(
            cluster_db_path,
            supervisor,
            key_file,
            Some(commit_sink),
            outbox_codec,
            wall_clock,
        )
        .await
    }

    /// Compatibility constructor for tests and helper call sites that still pass
    /// the DB root. Production bootstrap uses `new_persistent_paths`.
    #[cfg(test)]
    pub async fn new_persistent(
        db_root: &std::path::Path,
        supervisor: std::sync::Arc<TaskSupervisor>,
        key_file: Option<&std::path::Path>,
    ) -> Result<Self> {
        Self::new_persistent_paths_with_sink(
            &db_root.join("sqlite").join("cluster.db"),
            supervisor,
            key_file,
            crate::test_fixtures::commit_observation::new_sink(),
            crate::test_fixtures::outbox::new_codec(),
            std::sync::Arc::new(klights_supervisor::SystemWallClock),
        )
        .await
    }

    /// Test-only convenience constructor.
    #[cfg(test)]
    pub async fn new_in_memory() -> Result<Self> {
        let supervisor = std::sync::Arc::new(TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let executor =
            crate::sqlite::open_in_memory(supervisor.clone(), "sqlite:memory:cluster").await?;
        let snapshot_factory = executor.snapshot_open_opts().map(|opts| {
            crate::sqlite::recovery::SqliteSnapshotFactory::new(opts, supervisor.clone())
        });
        let read_executor = executor.read_lane_clone();
        Self::from_executors(
            executor,
            read_executor,
            snapshot_factory,
            Some(crate::test_fixtures::commit_observation::new_sink()),
            crate::test_fixtures::outbox::new_codec(),
            std::sync::Arc::new(klights_supervisor::SystemWallClock),
        )
        .await
    }

    /// Production constructor for an externally-created `DbExecutor`.
    pub async fn new_in_memory_with_watch_and_executor(
        executor: DbExecutor,
        outbox_codec: std::sync::Arc<dyn OutboxResponseCodec>,
        wall_clock: std::sync::Arc<dyn klights_supervisor::WallClock>,
    ) -> Result<Self> {
        Self::from_executor(
            executor,
            #[cfg(any(test, feature = "test-support"))]
            None,
            outbox_codec,
            wall_clock,
        )
        .await
    }

    /// Test-support constructor for an externally-created `DbExecutor`.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn new_in_memory_with_watch_and_executor_with_sink(
        executor: DbExecutor,
        commit_sink: std::sync::Arc<dyn CommitObservationSink>,
        outbox_codec: std::sync::Arc<dyn OutboxResponseCodec>,
        wall_clock: std::sync::Arc<dyn klights_supervisor::WallClock>,
    ) -> Result<Self> {
        Self::from_executor(executor, Some(commit_sink), outbox_codec, wall_clock).await
    }
}
