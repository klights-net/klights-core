use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use ::redb::{ReadableDatabase, ReadableTable};
use klights_cluster_core::{
    ClusterMembership, ClusterMetadata, ClusterMutation, LogApplyAppliedOutboxRow,
    LogApplyNodeDataplaneRow, LogApplyNodeSubnetRow, LogApplyWatchEventRow, NetworkMutation,
    Resource, SnapshotRestoreOperation, WatchHistoryMutation, WatchReplayPosition,
};
use klights_cluster_store::{
    DurableReplayFloor, DurableReplayTarget, SnapshotCaptureHeader, SnapshotCapturePage,
    SnapshotCaptureRequest, SnapshotCaptureSession, SnapshotMembership, SnapshotPersistenceError,
    SnapshotPersistenceFuture,
};
use tokio::sync::oneshot;

use klights_cluster_datastore::redb::key_codec;

use super::RedbRecoveryStore;
use klights_cluster_datastore::redb::tables;

enum Command {
    Next(oneshot::Sender<Result<Option<SnapshotCapturePage>, SnapshotPersistenceError>>),
    Cancel,
    Expire,
}

const SESSION_ACTIVE: u8 = 0;
const SESSION_EXPIRED: u8 = 1;
const SESSION_CANCELLED: u8 = 2;
const SESSION_COMPLETE: u8 = 3;

pub(super) struct RedbSnapshotSession {
    header: SnapshotCaptureHeader,
    tx: tokio::sync::mpsc::Sender<Command>,
    expiry: Option<klights_supervisor::SupervisedJoinHandle<()>>,
    state: Arc<AtomicU8>,
}

impl SnapshotCaptureSession for RedbSnapshotSession {
    fn header(&self) -> &SnapshotCaptureHeader {
        &self.header
    }

    fn next_page(&mut self) -> SnapshotPersistenceFuture<'_, Option<SnapshotCapturePage>> {
        Box::pin(async move {
            match self.state.load(Ordering::Acquire) {
                SESSION_EXPIRED => return Err(SnapshotPersistenceError::Timeout),
                SESSION_CANCELLED => return Err(SnapshotPersistenceError::Cancelled),
                SESSION_COMPLETE => return Ok(None),
                _ => {}
            }
            let (tx, rx) = oneshot::channel();
            self.tx
                .send(Command::Next(tx))
                .await
                .map_err(|_| session_terminal_error(&self.state))?;
            let page = rx
                .await
                .map_err(|_| session_terminal_error(&self.state))??;
            if page.is_none() {
                self.state.store(SESSION_COMPLETE, Ordering::Release);
                if let Some(expiry) = self.expiry.take() {
                    expiry.abort();
                }
            }
            Ok(page)
        })
    }

    fn cancel(&mut self) -> SnapshotPersistenceFuture<'_> {
        Box::pin(async move {
            if let Some(expiry) = self.expiry.take() {
                expiry.abort();
            }
            let _ = self.state.compare_exchange(
                SESSION_ACTIVE,
                SESSION_CANCELLED,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
            self.tx
                .send(Command::Cancel)
                .await
                .map_err(|_| SnapshotPersistenceError::Cancelled)
        })
    }
}

impl Drop for RedbSnapshotSession {
    fn drop(&mut self) {
        if let Some(expiry) = self.expiry.take() {
            expiry.abort();
        }
        let _ = self.state.compare_exchange(
            SESSION_ACTIVE,
            SESSION_CANCELLED,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        let _ = self.tx.try_send(Command::Cancel);
    }
}

fn session_terminal_error(state: &AtomicU8) -> SnapshotPersistenceError {
    if state.load(Ordering::Acquire) == SESSION_EXPIRED {
        SnapshotPersistenceError::Timeout
    } else {
        SnapshotPersistenceError::Cancelled
    }
}

impl RedbRecoveryStore {
    pub(crate) async fn begin_snapshot(
        &self,
        request: SnapshotCaptureRequest,
        fence: klights_cluster_store::SnapshotExclusiveFence,
    ) -> anyhow::Result<Box<dyn SnapshotCaptureSession>> {
        let permit = self
            .snapshot_sessions
            .clone()
            .try_acquire_owned()
            .map_err(|_| {
                anyhow::Error::new(SnapshotPersistenceError::ResourceExhausted {
                    message: "redb snapshot session capacity exhausted".to_string(),
                })
            })?;
        let db = self.accessor.db()?;
        let supervisor = self.accessor.supervisor();
        let (command_tx, mut command_rx) = tokio::sync::mpsc::channel(1);
        let (start_tx, mut start_rx) = tokio::sync::mpsc::channel(1);
        let (ready_tx, ready_rx) = oneshot::channel();
        let page_limit = request.page_limit().get();
        let session_state = Arc::new(AtomicU8::new(SESSION_ACTIVE));
        let actor_state = session_state.clone();
        let actor_supervisor = supervisor.clone();
        supervisor
            .spawn_async(
                klights_supervisor::TaskCategory::Background,
                "redb-snapshot-session",
                async move {
                    let _permit = permit;
                    let _ = actor_supervisor
                        .run_blocking(
                            klights_supervisor::TaskCategory::DbRead,
                            "redb-snapshot-reader",
                            move || {
                                if start_rx.blocking_recv().is_none() {
                                    return;
                                }
                                let read = match db.begin_read() {
                                    Ok(read) => read,
                                    Err(error) => {
                                        let _ = ready_tx.send(Err(persistence(error)));
                                        return;
                                    }
                                };
                                let header = match read_header(&read) {
                                    Ok(header) => header,
                                    Err(error) => {
                                        let _ = ready_tx.send(Err(error));
                                        return;
                                    }
                                };
                                let _ = ready_tx.send(Ok(header));
                                let mut phase = Phase::Namespace(None);
                                while let Some(command) = command_rx.blocking_recv() {
                                    match command {
                                        Command::Cancel => break,
                                        Command::Expire => break,
                                        Command::Next(reply) => {
                                            match actor_state.load(Ordering::Acquire) {
                                                SESSION_EXPIRED => {
                                                    let _ = reply.send(Err(
                                                        SnapshotPersistenceError::Timeout,
                                                    ));
                                                    break;
                                                }
                                                SESSION_CANCELLED => {
                                                    let _ = reply.send(Err(
                                                        SnapshotPersistenceError::Cancelled,
                                                    ));
                                                    break;
                                                }
                                                _ => {}
                                            }
                                            let result =
                                                next_nonempty_page(&read, &mut phase, page_limit);
                                            if actor_state.load(Ordering::Acquire)
                                                == SESSION_EXPIRED
                                            {
                                                let _ = reply
                                                    .send(Err(SnapshotPersistenceError::Timeout));
                                                break;
                                            }
                                            let terminal =
                                                result.as_ref().is_ok_and(|page| page.is_none());
                                            if terminal {
                                                actor_state
                                                    .store(SESSION_COMPLETE, Ordering::Release);
                                            }
                                            let _ = reply.send(result);
                                            if terminal {
                                                break;
                                            }
                                        }
                                    }
                                }
                            },
                        )
                        .await;
                },
            )
            .await?;
        start_tx
            .send(())
            .await
            .map_err(|_| anyhow::anyhow!("redb snapshot actor stopped before pin"))?;
        let header = ready_rx.await??;
        drop(fence);
        let expiry_tx = command_tx.clone();
        let expiry_state = session_state.clone();
        let lifetime = request.max_lifetime().min(Duration::from_secs(300));
        let expiry = supervisor
            .spawn_delay("redb-snapshot-expiry", lifetime, async move {
                if expiry_state
                    .compare_exchange(
                        SESSION_ACTIVE,
                        SESSION_EXPIRED,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    let _ = expiry_tx.send(Command::Expire).await;
                }
            })
            .await?;
        Ok(Box::new(RedbSnapshotSession {
            header,
            tx: command_tx,
            expiry: Some(expiry),
            state: session_state,
        }))
    }
}

#[derive(Clone)]
enum Phase {
    Namespace(Option<String>),
    Cluster(Option<Vec<u8>>),
    Namespaced(Option<Vec<u8>>),
    Watch(u64),
    Subnet(Option<String>),
    Dataplane(Option<String>),
    PodCleanup(Option<Vec<u8>>),
    Watermark(Option<Vec<u8>>),
    Applied(Option<String>),
    Floor(Option<Vec<u8>>),
    Complete,
}

fn next_nonempty_page(
    read: &::redb::ReadTransaction,
    phase: &mut Phase,
    limit: usize,
) -> Result<Option<SnapshotCapturePage>, SnapshotPersistenceError> {
    loop {
        let (page, next) = read_page(read, phase.clone(), limit)?;
        *phase = next;
        if page.is_some() || matches!(phase, Phase::Complete) {
            return Ok(page);
        }
    }
}

fn read_header(
    read: &::redb::ReadTransaction,
) -> Result<SnapshotCaptureHeader, SnapshotPersistenceError> {
    let meta = read.open_table(tables::META).map_err(persistence)?;
    let number = |key: &str| -> Result<i64, SnapshotPersistenceError> {
        let Some(value) = meta.get(key).map_err(persistence)? else {
            return Ok(0);
        };
        std::str::from_utf8(value.value())
            .map_err(corrupt)?
            .parse()
            .map_err(corrupt)
    };
    let current_rv = number("rv")?;
    let event_id = number("watch_event_id")?;
    let klights = read.open_table(tables::KLIGHTS_META).map_err(persistence)?;
    let get = |key: &str| {
        klights
            .get(key)
            .map(|value| value.map(|value| value.value().to_string()))
            .map_err(persistence)
    };
    let cluster_id = get(klights_cluster_store::CLUSTER_ID_META_KEY)?
        .filter(|value| !value.is_empty())
        .ok_or_else(|| corrupt("cluster_id is missing"))?;
    let leader_epoch = get(klights_cluster_store::LEADER_EPOCH_META_KEY)?
        .ok_or_else(|| corrupt("leader_epoch is missing"))?
        .parse()
        .map_err(corrupt)?;
    let command_codec_activation_version =
        get(klights_cluster_store::COMMAND_CODEC_ACTIVATION_VERSION_META_KEY)?
            .map(|raw| raw.parse::<u32>().map_err(corrupt))
            .transpose()?;
    let membership = match (
        get(klights_cluster_store::RAFT_VOTERS_META_KEY)?,
        get(klights_cluster_store::RAFT_TERM_META_KEY)?,
        get(klights_cluster_store::RAFT_LEADER_HINT_META_KEY)?,
    ) {
        (None, None, None) => SnapshotMembership::AuthoritativeAbsent,
        (Some(voters), Some(term), Some(hint)) => SnapshotMembership::Present(ClusterMembership {
            cluster_id: cluster_id.clone(),
            voters: serde_json::from_str(&voters).map_err(corrupt)?,
            term: term.parse().map_err(corrupt)?,
            leader_hint: (!hint.is_empty()).then_some(hint),
        }),
        _ => return Err(corrupt("membership metadata is incomplete")),
    };
    SnapshotCaptureHeader::try_new(
        command_codec_activation_version,
        WatchReplayPosition {
            resource_version: current_rv,
            event_id,
            resource_version_filter_through_event_id: 0,
        },
        ClusterMetadata {
            cluster_id,
            leader_epoch,
            current_rv,
        },
        membership,
    )
    .map_err(|error| corrupt(error.to_string()))
}

fn read_page(
    read: &::redb::ReadTransaction,
    phase: Phase,
    limit: usize,
) -> Result<(Option<SnapshotCapturePage>, Phase), SnapshotPersistenceError> {
    match phase {
        Phase::Namespace(after) => {
            let table = read.open_table(tables::NAMESPACES).map_err(persistence)?;
            let mut rows = Vec::new();
            for entry in table.iter().map_err(persistence)? {
                let (key, value) = entry.map_err(persistence)?;
                if after.as_deref().is_some_and(|after| key.value() <= after) {
                    continue;
                }
                rows.push(validated_resource(
                    value.value(),
                    "v1",
                    "Namespace",
                    None,
                    key.value(),
                    None,
                )?);
                if rows.len() == limit {
                    break;
                }
            }
            resource_page(rows, Phase::Cluster(None), |row| {
                Phase::Namespace(Some(row.name.clone()))
            })
        }
        Phase::Cluster(after) => resource_table_page(read, false, after, limit),
        Phase::Namespaced(after) => resource_table_page(read, true, after, limit),
        Phase::Watch(after) => watch_page(read, after, limit),
        Phase::Subnet(after) => json_network_page(read, tables::NODE_SUBNETS, after, limit, true),
        Phase::Dataplane(after) => {
            json_network_page(read, tables::NODE_DATAPLANE, after, limit, false)
        }
        Phase::PodCleanup(after) => pod_cleanup_page(read, after, limit),
        Phase::Watermark(after) => watermark_page(read, after, limit),
        Phase::Applied(after) => applied_page(read, after, limit),
        Phase::Floor(after) => floor_page(read, after, limit),
        Phase::Complete => Ok((None, Phase::Complete)),
    }
}

fn resource_table_page(
    read: &::redb::ReadTransaction,
    namespaced: bool,
    after: Option<Vec<u8>>,
    limit: usize,
) -> Result<(Option<SnapshotCapturePage>, Phase), SnapshotPersistenceError> {
    let table = read
        .open_table(if namespaced {
            tables::RES_NS
        } else {
            tables::RES_CLUSTER
        })
        .map_err(persistence)?;
    let mut rows = Vec::new();
    for entry in table.iter().map_err(persistence)? {
        let (key, value) = entry.map_err(persistence)?;
        if after
            .as_ref()
            .is_some_and(|after| key.value() <= after.as_slice())
        {
            continue;
        }
        let (api, kind, namespace, name) = key_codec::decode_resource_key(key.value(), namespaced)
            .ok_or_else(|| corrupt("malformed redb resource key"))?;
        let stored_rv = i64::try_from(value.value().0).map_err(corrupt)?;
        rows.push(validated_resource(
            value.value().1,
            api,
            kind,
            namespace,
            name,
            Some(stored_rv),
        )?);
        if rows.len() == limit {
            break;
        }
    }
    if rows.is_empty() {
        return Ok((
            None,
            if namespaced {
                Phase::Watch(0)
            } else {
                Phase::Namespaced(None)
            },
        ));
    }
    let last = rows.last().unwrap();
    let key = key_codec::resource_key(
        &last.api_version,
        &last.kind,
        last.namespace.as_deref(),
        &last.name,
    );
    resource_page(rows, Phase::Complete, |_| {
        if namespaced {
            Phase::Namespaced(Some(key.clone()))
        } else {
            Phase::Cluster(Some(key.clone()))
        }
    })
}

fn validated_resource(
    body: &[u8],
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
    stored_rv: Option<i64>,
) -> Result<Resource, SnapshotPersistenceError> {
    let data: serde_json::Value = serde_json::from_slice(body).map_err(corrupt)?;
    let identity = |pointer: &str, label: &str| {
        data.pointer(pointer)
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| corrupt(format!("resource {label} is missing")))
    };
    if identity("/apiVersion", "apiVersion")? != api_version
        || identity("/kind", "kind")? != kind
        || identity("/metadata/name", "name")? != name
    {
        return Err(corrupt("resource body identity does not match redb key"));
    }
    match namespace {
        Some(expected)
            if data
                .pointer("/metadata/namespace")
                .and_then(serde_json::Value::as_str)
                != Some(expected) =>
        {
            return Err(corrupt(
                "namespaced resource body namespace does not match redb key",
            ));
        }
        None if data
            .pointer("/metadata/namespace")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|value| !value.is_empty()) =>
        {
            return Err(corrupt(
                "cluster resource body contains a namespaced identity",
            ));
        }
        _ => {}
    }
    let uid = identity("/metadata/uid", "UID")?.to_string();
    let resource_version = identity("/metadata/resourceVersion", "resourceVersion")?
        .parse::<i64>()
        .map_err(corrupt)?;
    if resource_version < 0 || stored_rv.is_some_and(|stored| stored != resource_version) {
        return Err(corrupt(
            "resourceVersion is negative or differs from the redb value",
        ));
    }
    Ok(Resource {
        id: 0,
        api_version: api_version.to_string(),
        kind: kind.to_string(),
        namespace: namespace.map(str::to_string),
        name: name.to_string(),
        uid,
        resource_version,
        data: std::sync::Arc::new(data),
    })
}

fn resource_page(
    rows: Vec<Resource>,
    empty: Phase,
    cursor: impl Fn(&Resource) -> Phase,
) -> Result<(Option<SnapshotCapturePage>, Phase), SnapshotPersistenceError> {
    if rows.is_empty() {
        return Ok((None, empty));
    }
    let next = cursor(rows.last().unwrap());
    let operations = rows
        .iter()
        .map(klights_cluster_core::resource_snapshot_restore_operation)
        .collect();
    Ok((Some(SnapshotCapturePage::try_operations(operations)?), next))
}

fn watch_page(
    read: &::redb::ReadTransaction,
    after: u64,
    limit: usize,
) -> Result<(Option<SnapshotCapturePage>, Phase), SnapshotPersistenceError> {
    let table = read.open_table(tables::WATCH_EVENTS).map_err(persistence)?;
    let mut commits = Vec::new();
    let mut next = after;
    for entry in table.range(after..).map_err(persistence)? {
        let (id, value) = entry.map_err(persistence)?;
        if id.value() <= after {
            continue;
        }
        let body: serde_json::Value = serde_json::from_slice(value.value()).map_err(corrupt)?;
        let data = body
            .get("data")
            .cloned()
            .ok_or_else(|| corrupt("watch event data missing"))?;
        let rv = body
            .get("resourceVersion")
            .and_then(|v| v.as_i64())
            .ok_or_else(|| corrupt("watch event RV missing"))?;
        let api_version = body
            .get("apiVersion")
            .and_then(|v| v.as_str())
            .filter(|v| !v.is_empty())
            .ok_or_else(|| corrupt("watch event apiVersion missing"))?;
        let kind = body
            .get("kind")
            .and_then(|v| v.as_str())
            .filter(|v| !v.is_empty())
            .ok_or_else(|| corrupt("watch event kind missing"))?;
        let name = body
            .get("name")
            .and_then(|v| v.as_str())
            .filter(|v| !v.is_empty())
            .ok_or_else(|| corrupt("watch event name missing"))?;
        let event_type = body
            .get("eventType")
            .and_then(|v| v.as_str())
            .filter(|v| !v.is_empty())
            .ok_or_else(|| corrupt("watch event type missing"))?;
        let row = LogApplyWatchEventRow {
            event_id: Some(i64::try_from(id.value()).map_err(corrupt)?),
            api_version: api_version.into(),
            kind: kind.into(),
            namespace: body
                .get("namespace")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            name: name.into(),
            resource_version: rv,
            event_type: event_type.into(),
            data,
        };
        commits.push(snapshot_operation(
            rv,
            vec![ClusterMutation::WatchHistory(
                WatchHistoryMutation::PutWatchEvent(row),
            )],
        ));
        next = id.value();
        if commits.len() == limit {
            break;
        }
    }
    if commits.is_empty() {
        return Ok((None, Phase::Subnet(None)));
    }
    Ok((
        Some(SnapshotCapturePage::try_operations(commits)?),
        Phase::Watch(next),
    ))
}

fn json_network_page(
    read: &::redb::ReadTransaction,
    definition: ::redb::TableDefinition<&str, &[u8]>,
    after: Option<String>,
    limit: usize,
    subnet: bool,
) -> Result<(Option<SnapshotCapturePage>, Phase), SnapshotPersistenceError> {
    let table = match read.open_table(definition) {
        Ok(table) => table,
        Err(::redb::TableError::TableDoesNotExist(_)) => {
            return Ok((
                None,
                if subnet {
                    Phase::Dataplane(None)
                } else {
                    Phase::PodCleanup(None)
                },
            ));
        }
        Err(error) => return Err(persistence(error)),
    };
    let current_rv = read_header(read)?.position().resource_version;
    let mut commits = Vec::new();
    let mut next = None;
    for entry in table.iter().map_err(persistence)? {
        let (key, value) = entry.map_err(persistence)?;
        if after.as_deref().is_some_and(|after| key.value() <= after) {
            continue;
        }
        let body: serde_json::Value = serde_json::from_slice(value.value()).map_err(corrupt)?;
        let mutation = if subnet {
            NetworkMutation::PutNodeSubnet(LogApplyNodeSubnetRow {
                node_name: key.value().into(),
                subnet: required(&body, "subnet")?.into(),
                subnet_base_int: u32::try_from(
                    body.get("subnet_base_int")
                        .and_then(|v| v.as_u64())
                        .ok_or_else(|| corrupt("subnet base missing"))?,
                )
                .map_err(corrupt)?,
                gateway_ip: body
                    .get("gateway_ip")
                    .or_else(|| body.get("vtep_ip"))
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| corrupt("gateway missing"))?
                    .into(),
                node_ip: required(&body, "node_ip")?.into(),
                mode: required(&body, "mode")?.into(),
                hostport_range: body
                    .get("hostport_range")
                    .and_then(|v| v.as_str())
                    .map(str::to_string),
            })
        } else {
            let port = body
                .get("port")
                .and_then(|v| v.as_u64())
                .map(u16::try_from)
                .transpose()
                .map_err(corrupt)?;
            NetworkMutation::PutNodeDataplane(LogApplyNodeDataplaneRow {
                node_name: key.value().into(),
                mode: required(&body, "mode")?.into(),
                encryption: required(&body, "encryption")?.into(),
                public_key: body
                    .get("public_key")
                    .and_then(|v| v.as_str())
                    .map(str::to_string),
                endpoint: required(&body, "endpoint")?.into(),
                port,
            })
        };
        commits.push(snapshot_operation(
            current_rv,
            vec![ClusterMutation::Network(mutation)],
        ));
        next = Some(key.value().to_string());
        if commits.len() == limit {
            break;
        }
    }
    if commits.is_empty() {
        return Ok((
            None,
            if subnet {
                Phase::Dataplane(None)
            } else {
                Phase::PodCleanup(None)
            },
        ));
    }
    Ok((
        Some(SnapshotCapturePage::try_operations(commits)?),
        if subnet {
            Phase::Subnet(next)
        } else {
            Phase::Dataplane(next)
        },
    ))
}

fn pod_cleanup_page(
    read: &::redb::ReadTransaction,
    after: Option<Vec<u8>>,
    limit: usize,
) -> Result<(Option<SnapshotCapturePage>, Phase), SnapshotPersistenceError> {
    let table = match read.open_table(tables::POD_CLEANUP_INTENTS) {
        Ok(table) => table,
        Err(::redb::TableError::TableDoesNotExist(_)) => {
            return Ok((None, Phase::Watermark(None)));
        }
        Err(error) => return Err(persistence(error)),
    };
    let mut commits = Vec::new();
    let mut next = None;
    for entry in table.iter().map_err(persistence)? {
        let (key, value) = entry.map_err(persistence)?;
        if after
            .as_ref()
            .is_some_and(|after| key.value() <= after.as_slice())
        {
            continue;
        }
        let intent: klights_cluster_core::LogApplyPodCleanupIntentRow =
            serde_json::from_slice(value.value()).map_err(corrupt)?;
        commits.push(snapshot_operation(
            intent.resource_version,
            vec![ClusterMutation::PodCleanup(
                klights_cluster_core::PodCleanupMutation::PutPodCleanupIntent(intent),
            )],
        ));
        next = Some(key.value().to_vec());
        if commits.len() == limit {
            break;
        }
    }
    if commits.is_empty() {
        return Ok((None, Phase::Watermark(None)));
    }
    Ok((
        Some(SnapshotCapturePage::try_operations(commits)?),
        Phase::PodCleanup(next),
    ))
}

fn watermark_page(
    read: &::redb::ReadTransaction,
    after: Option<Vec<u8>>,
    limit: usize,
) -> Result<(Option<SnapshotCapturePage>, Phase), SnapshotPersistenceError> {
    let table = read
        .open_table(tables::OUTBOX_STREAM_WATERMARKS)
        .map_err(persistence)?;
    let mut rows = Vec::new();
    let mut next = None;
    for entry in table.iter().map_err(persistence)? {
        let (key, value) = entry.map_err(persistence)?;
        if after
            .as_ref()
            .is_some_and(|after| key.value() <= after.as_slice())
        {
            continue;
        }
        rows.push(
            klights_cluster_datastore::redb::live_committed_apply::decode_outbox_watermark_key(
                key.value(),
                value.value(),
            )
            .map_err(corrupt)?,
        );
        next = Some(key.value().to_vec());
        if rows.len() == limit {
            break;
        }
    }
    if rows.is_empty() {
        return Ok((None, Phase::Applied(None)));
    }
    Ok((
        Some(SnapshotCapturePage::try_outbox_watermarks(rows)?),
        Phase::Watermark(next),
    ))
}

fn applied_page(
    read: &::redb::ReadTransaction,
    after: Option<String>,
    limit: usize,
) -> Result<(Option<SnapshotCapturePage>, Phase), SnapshotPersistenceError> {
    let table = read
        .open_table(tables::APPLIED_OUTBOX)
        .map_err(persistence)?;
    let mut rows: Vec<LogApplyAppliedOutboxRow> = Vec::new();
    let mut next = None;
    for entry in table.iter().map_err(persistence)? {
        let (key, value) = entry.map_err(persistence)?;
        if after.as_deref().is_some_and(|after| key.value() <= after) {
            continue;
        }
        let row: LogApplyAppliedOutboxRow =
            serde_json::from_slice(value.value()).map_err(corrupt)?;
        next = Some(key.value().to_string());
        rows.push(row);
        if rows.len() == limit {
            break;
        }
    }
    if rows.is_empty() {
        return Ok((None, Phase::Floor(None)));
    }
    Ok((
        Some(SnapshotCapturePage::try_applied_outbox(rows)?),
        Phase::Applied(next),
    ))
}

fn floor_page(
    read: &::redb::ReadTransaction,
    after: Option<Vec<u8>>,
    limit: usize,
) -> Result<(Option<SnapshotCapturePage>, Phase), SnapshotPersistenceError> {
    let table = read
        .open_table(tables::WATCH_REPLAY_POSITION_FLOORS)
        .map_err(persistence)?;
    let mut rows = Vec::new();
    let mut next = None;
    for entry in table.iter().map_err(persistence)? {
        let (key, value) = entry.map_err(persistence)?;
        if after
            .as_ref()
            .is_some_and(|after| key.value() <= after.as_slice())
        {
            continue;
        }
        let mut parts = key.value().splitn(3, |byte| *byte == 0);
        let api = String::from_utf8(parts.next().ok_or_else(|| corrupt("floor key"))?.to_vec())
            .map_err(corrupt)?;
        let kind = String::from_utf8(parts.next().ok_or_else(|| corrupt("floor key"))?.to_vec())
            .map_err(corrupt)?;
        let ns = String::from_utf8(parts.next().ok_or_else(|| corrupt("floor key"))?.to_vec())
            .map_err(corrupt)?;
        if value.value().len() != 16 {
            return Err(corrupt("floor value length"));
        }
        let rv = i64::try_from(u64::from_be_bytes(value.value()[..8].try_into().unwrap()))
            .map_err(corrupt)?;
        let event = i64::try_from(u64::from_be_bytes(value.value()[8..].try_into().unwrap()))
            .map_err(corrupt)?;
        let target = if api == "*" && kind == "*" && ns == "*" {
            DurableReplayTarget::All
        } else if ns == "#cluster" {
            DurableReplayTarget::Cluster {
                api_version: api,
                kind,
            }
        } else {
            DurableReplayTarget::Namespaced {
                api_version: api,
                kind,
                namespace: ns,
            }
        };
        rows.push(DurableReplayFloor::new(target, rv, event, true).map_err(corrupt)?);
        next = Some(key.value().to_vec());
        if rows.len() == limit {
            break;
        }
    }
    if rows.is_empty() {
        return Ok((None, Phase::Complete));
    }
    Ok((
        Some(SnapshotCapturePage::try_replay_floors(rows)?),
        Phase::Floor(next),
    ))
}

fn required<'a>(
    body: &'a serde_json::Value,
    key: &str,
) -> Result<&'a str, SnapshotPersistenceError> {
    body.get(key)
        .and_then(|v| v.as_str())
        .ok_or_else(|| corrupt(format!("{key} missing")))
}
fn snapshot_operation(
    resource_version: i64,
    mutations: Vec<ClusterMutation>,
) -> SnapshotRestoreOperation {
    SnapshotRestoreOperation::new(
        resource_version,
        None,
        mutations
            .into_iter()
            .map(ClusterMutation::into_log_apply_mutation)
            .collect(),
    )
}

fn persistence(error: impl std::fmt::Display) -> SnapshotPersistenceError {
    SnapshotPersistenceError::PersistenceFailed {
        message: error.to_string(),
    }
}
fn corrupt(error: impl std::fmt::Display) -> SnapshotPersistenceError {
    SnapshotPersistenceError::CorruptData {
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datastore::DatastoreBackend;
    use crate::datastore::redb::RedbDatastore;
    use klights_cluster_core::LogApplyAppliedOutboxRow;
    use std::sync::Arc;

    async fn store() -> RedbDatastore {
        let db = RedbDatastore::new_in_memory().await.unwrap();
        db.set_klights_meta(klights_cluster_store::CLUSTER_ID_META_KEY, "redb-pinned")
            .await
            .unwrap();
        db.set_klights_meta(klights_cluster_store::LEADER_EPOCH_META_KEY, "1")
            .await
            .unwrap();
        db
    }

    fn request(lifetime: Duration) -> SnapshotCaptureRequest {
        SnapshotCaptureRequest::try_new(
            klights_cluster_store::SnapshotPageLimit::try_new(
                klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE,
            )
            .unwrap(),
            lifetime,
        )
        .unwrap()
    }

    async fn begin_capture(
        db: &RedbDatastore,
        request: SnapshotCaptureRequest,
    ) -> Box<dyn SnapshotCaptureSession> {
        let fence = DatastoreBackend::acquire_snapshot_exclusive_fence(db)
            .await
            .unwrap()
            .expect("Redb supplies a snapshot capture fence");
        db.begin_pinned_snapshot_capture(request, fence)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn redb_mvcc_session_pages_513_and_excludes_post_anchor_row() {
        let db = store().await;
        for index in 0..=klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE {
            db.insert_applied_outbox(LogApplyAppliedOutboxRow {
                idempotency_key: format!("before-{index:04}"),
                subject_key: "subject".into(),
                operation: "Update".into(),
                first_seen_ms: index as i64,
                applied_rv: Some(1),
                result_proto: Vec::new(),
                status_stamp: None,
            })
            .await
            .unwrap();
        }
        let fence = DatastoreBackend::acquire_snapshot_exclusive_fence(&db)
            .await
            .unwrap()
            .expect("Redb supplies a snapshot capture fence");
        let mutation_db = db.clone();
        let mutation = tokio::spawn(async move {
            mutation_db
                .insert_applied_outbox(LogApplyAppliedOutboxRow {
                    idempotency_key: "post-anchor".into(),
                    subject_key: "subject".into(),
                    operation: "Update".into(),
                    first_seen_ms: 999,
                    applied_rv: Some(1),
                    result_proto: Vec::new(),
                    status_stamp: None,
                })
                .await
                .unwrap();
        });
        tokio::task::yield_now().await;
        assert!(
            !mutation.is_finished(),
            "real accessor mutation must wait while root owns the capture fence"
        );
        let mut session = db
            .begin_pinned_snapshot_capture(request(Duration::from_secs(30)), fence)
            .await
            .unwrap();
        mutation.await.unwrap();
        let mut lengths = Vec::new();
        let mut keys = Vec::new();
        while let Some(page) = session.next_page().await.unwrap() {
            if let Some(rows) = page.applied_outbox() {
                lengths.push(rows.len());
                keys.extend(rows.iter().map(|row| row.idempotency_key.clone()));
            }
        }
        assert_eq!(
            lengths,
            vec![klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE, 1]
        );
        assert!(!keys.iter().any(|key| key == "post-anchor"));
        assert!(session.next_page().await.unwrap().is_none());
        assert!(session.next_page().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn redb_mvcc_session_deadline_releases_actor_without_page_poll() {
        let db = store().await;
        let mut session = begin_capture(&db, request(Duration::from_millis(20))).await;
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(matches!(
            session.next_page().await,
            Err(SnapshotPersistenceError::Timeout)
        ));
        begin_capture(&db, request(Duration::from_secs(1))).await;
        drop(session);
    }

    #[tokio::test]
    async fn redb_expiry_command_waits_for_a_full_actor_queue() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let (reply, _reply_rx) = oneshot::channel();
        tx.send(Command::Next(reply)).await.unwrap();
        let state = Arc::new(AtomicU8::new(SESSION_ACTIVE));
        let expiry_state = state.clone();
        let expiry = tokio::spawn(async move {
            expiry_state.store(SESSION_EXPIRED, Ordering::Release);
            tx.send(Command::Expire).await.unwrap();
        });
        tokio::task::yield_now().await;
        assert_eq!(state.load(Ordering::Acquire), SESSION_EXPIRED);
        assert!(
            !expiry.is_finished(),
            "expiry delivery must wait instead of dropping a full-queue signal"
        );
        assert!(matches!(rx.recv().await, Some(Command::Next(_))));
        expiry.await.unwrap();
        assert!(matches!(rx.recv().await, Some(Command::Expire)));
    }

    #[tokio::test]
    async fn redb_snapshot_rejects_corrupt_namespace_body() {
        let db = store().await;
        db.accessor
            .call("seed_corrupt_snapshot_namespace", |database| {
                let write = database.begin_write()?;
                {
                    let mut table = write.open_table(tables::NAMESPACES)?;
                    table.insert("broken", b"{not-json".as_slice())?;
                }
                write.commit()?;
                Ok(())
            })
            .await
            .unwrap();
        let mut session = begin_capture(&db, request(Duration::from_secs(30))).await;
        assert!(matches!(
            session.next_page().await,
            Err(SnapshotPersistenceError::CorruptData { .. })
        ));
    }

    #[tokio::test]
    async fn redb_snapshot_rejects_resource_identity_or_rv_mismatch() {
        let db = store().await;
        db.accessor
            .call("seed_corrupt_snapshot_resource", |database| {
                let write = database.begin_write()?;
                {
                    let mut table = write.open_table(tables::RES_CLUSTER)?;
                    let key = key_codec::resource_key("v1", "Node", None, "node-a");
                    let body = br#"{"apiVersion":"v1","kind":"Node","metadata":{"name":"node-a","uid":"uid-a","resourceVersion":"8"}}"#;
                    table.insert(key.as_slice(), (7, body.as_slice()))?;
                }
                write.commit()?;
                Ok(())
            })
            .await
            .unwrap();
        let mut session = begin_capture(&db, request(Duration::from_secs(30))).await;
        assert!(matches!(
            session.next_page().await,
            Err(SnapshotPersistenceError::CorruptData { .. })
        ));
    }
}
