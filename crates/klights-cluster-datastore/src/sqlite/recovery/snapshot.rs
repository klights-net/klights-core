//! SQLite snapshot capture algorithms.

use std::sync::Arc;

use klights_cluster_core::{
    ClusterMembership, ClusterMetadata, ClusterMutation, LogApplyNodeDataplaneRow,
    LogApplyNodeSubnetRow, NetworkMutation, Resource, SnapshotRestoreOperation,
    WatchReplayPosition,
};
use klights_cluster_store::{
    DurableReplayFloor, DurableReplayTarget, SnapshotCaptureHeader, SnapshotCapturePage,
    SnapshotMembership, SnapshotPersistenceError,
};
use rusqlite::{OptionalExtension, params};

#[derive(Clone)]
pub(crate) enum Phase {
    Namespace(Option<String>),
    ClusterResource(Option<(String, String, String)>),
    NamespacedResource(Option<(String, String, String, String)>),
    WatchEvent(i64),
    NodeSubnet(Option<String>),
    NodeDataplane(Option<String>),
    PodCleanup(Option<(String, String, String, String, String)>),
    AppliedOutbox(Option<String>),
    Watermark(Option<(String, i64)>),
    ReplayFloor(Option<(String, String, String)>),
    Complete,
}

pub(crate) fn read_header(conn: &rusqlite::Connection) -> rusqlite::Result<SnapshotCaptureHeader> {
    let current_rv = conn
        .query_row(
            "SELECT value FROM metadata WHERE key='resource_version'",
            [],
            |row| row.get::<_, String>(0),
        )?
        .parse::<i64>()
        .map_err(text_error)?;
    let event_id = conn
        .query_row(
            "SELECT seq FROM sqlite_sequence WHERE name='watch_events'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
        .unwrap_or(0);
    let get_meta = |key: &str| {
        conn.query_row(
            "SELECT value FROM _klights_meta WHERE key=?1",
            [key],
            |row| row.get::<_, String>(0),
        )
        .optional()
    };
    let command_codec_activation_version =
        get_meta(klights_cluster_store::COMMAND_CODEC_ACTIVATION_VERSION_META_KEY)?
            .map(|raw| raw.parse::<u32>().map_err(text_error))
            .transpose()?;
    let cluster_id = get_meta(klights_cluster_store::CLUSTER_ID_META_KEY)?
        .filter(|value| !value.is_empty())
        .ok_or_else(|| text_error("cluster_id is missing"))?;
    let leader_epoch = get_meta(klights_cluster_store::LEADER_EPOCH_META_KEY)?
        .ok_or_else(|| text_error("leader_epoch is missing"))?
        .parse::<i64>()
        .map_err(text_error)?;
    let membership = match (
        get_meta(klights_cluster_store::RAFT_VOTERS_META_KEY)?,
        get_meta(klights_cluster_store::RAFT_TERM_META_KEY)?,
        get_meta(klights_cluster_store::RAFT_LEADER_HINT_META_KEY)?,
    ) {
        (None, None, None) => SnapshotMembership::AuthoritativeAbsent,
        (Some(voters), Some(term), Some(hint)) => SnapshotMembership::Present(ClusterMembership {
            cluster_id: cluster_id.clone(),
            voters: serde_json::from_str(&voters).map_err(text_error)?,
            term: term.parse().map_err(text_error)?,
            leader_hint: (!hint.is_empty()).then_some(hint),
        }),
        _ => return Err(text_error("membership metadata is incomplete")),
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
    .map_err(text_error)
}

pub(crate) fn read_page(
    conn: &rusqlite::Connection,
    phase: Phase,
    limit: usize,
    current_rv: i64,
    high_event_id: i64,
) -> rusqlite::Result<(Option<SnapshotCapturePage>, Phase)> {
    match phase {
        Phase::Namespace(after) => {
            let mut stmt = conn.prepare(
                "SELECT name, uid, resource_version, data FROM namespaces
                 WHERE name > ?1 ORDER BY name LIMIT ?2",
            )?;
            let rows = stmt
                .query_map(
                    params![after.as_deref().unwrap_or(""), limit as i64],
                    |row| resource_from_row(row, "v1", "Namespace", None),
                )?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            commit_page(rows, Phase::ClusterResource(None), |row| {
                Phase::Namespace(Some(row.name.clone()))
            })
        }
        Phase::ClusterResource(after) => {
            let after = after.unwrap_or_default();
            let mut stmt = conn.prepare(
                "SELECT name, uid, resource_version, data, api_version, kind
                 FROM cluster_resources
                 WHERE (api_version,kind,name) > (?1,?2,?3)
                 ORDER BY api_version,kind,name LIMIT ?4",
            )?;
            let rows = stmt
                .query_map(params![after.0, after.1, after.2, limit as i64], |row| {
                    let api: String = row.get(4)?;
                    let kind: String = row.get(5)?;
                    resource_from_row(row, &api, &kind, None)
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            commit_page(rows, Phase::NamespacedResource(None), |row| {
                Phase::ClusterResource(Some((
                    row.api_version.clone(),
                    row.kind.clone(),
                    row.name.clone(),
                )))
            })
        }
        Phase::NamespacedResource(after) => {
            let after = after.unwrap_or_default();
            let mut stmt = conn.prepare(
                "SELECT name, uid, resource_version, data, api_version, kind, namespace
                 FROM namespaced_resources
                 WHERE (api_version,kind,namespace,name) > (?1,?2,?3,?4)
                 ORDER BY api_version,kind,namespace,name LIMIT ?5",
            )?;
            let rows = stmt
                .query_map(
                    params![after.0, after.1, after.2, after.3, limit as i64],
                    |row| {
                        let api: String = row.get(4)?;
                        let kind: String = row.get(5)?;
                        let namespace: String = row.get(6)?;
                        resource_from_row(row, &api, &kind, Some(namespace))
                    },
                )?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            commit_page(rows, Phase::WatchEvent(0), |row| {
                Phase::NamespacedResource(Some((
                    row.api_version.clone(),
                    row.kind.clone(),
                    row.namespace.clone().unwrap_or_default(),
                    row.name.clone(),
                )))
            })
        }
        Phase::WatchEvent(after) => {
            let mut stmt = conn.prepare(
                "SELECT id,api_version,kind,namespace,name,resource_version,event_type,data
                 FROM watch_events WHERE id>?1 AND id<=?2 ORDER BY id LIMIT ?3",
            )?;
            let rows = stmt
                .query_map(params![after, high_event_id, limit as i64], |row| {
                    let id: i64 = row.get(0)?;
                    let data: Vec<u8> = row.get(7)?;
                    let data = serde_json::from_slice(&data).map_err(text_error)?;
                    let resource = Resource {
                        id: 0,
                        api_version: row.get(1)?,
                        kind: row.get(2)?,
                        namespace: row.get(3)?,
                        name: row.get(4)?,
                        uid: Resource::uid_from_data(&data),
                        resource_version: row.get(5)?,
                        data: Arc::new(data),
                    };
                    let event_type: String = row.get(6)?;
                    Ok((id, resource, event_type))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            if rows.is_empty() {
                return Ok((None, Phase::NodeSubnet(None)));
            }
            let next = Phase::WatchEvent(rows.last().unwrap().0);
            let commits = rows
                .into_iter()
                .map(|(id, resource, event_type)| {
                    snapshot_operation(
                        resource.resource_version,
                        vec![watch_event_mutation(id, resource, event_type)],
                    )
                })
                .collect();
            Ok((Some(page_commits(commits)?), next))
        }
        Phase::NodeSubnet(after) => {
            let mut stmt = conn.prepare(
                "SELECT node_name,subnet,subnet_base_int,gateway_ip,node_ip,mode,hostport_range
                 FROM node_subnets WHERE node_name>?1 ORDER BY node_name LIMIT ?2",
            )?;
            let rows = stmt
                .query_map(
                    params![after.as_deref().unwrap_or(""), limit as i64],
                    |row| {
                        Ok(LogApplyNodeSubnetRow {
                            node_name: row.get(0)?,
                            subnet: row.get(1)?,
                            subnet_base_int: row.get(2)?,
                            gateway_ip: row.get(3)?,
                            node_ip: row.get(4)?,
                            mode: row.get(5)?,
                            hostport_range: row.get(6)?,
                        })
                    },
                )?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            network_page(
                rows,
                current_rv,
                Phase::NodeDataplane(None),
                |row| Phase::NodeSubnet(Some(row.node_name.clone())),
                NetworkMutation::PutNodeSubnet,
            )
        }
        Phase::NodeDataplane(after) => {
            let mut stmt = conn.prepare(
                "SELECT node_name,mode,encryption,public_key,endpoint,port
                 FROM node_dataplane WHERE node_name>?1 ORDER BY node_name LIMIT ?2",
            )?;
            let rows = stmt
                .query_map(
                    params![after.as_deref().unwrap_or(""), limit as i64],
                    |row| {
                        Ok(LogApplyNodeDataplaneRow {
                            node_name: row.get(0)?,
                            mode: row.get(1)?,
                            encryption: row.get(2)?,
                            public_key: row.get(3)?,
                            endpoint: row.get(4)?,
                            port: row
                                .get::<_, Option<i64>>(5)?
                                .map(u16::try_from)
                                .transpose()
                                .map_err(text_error)?,
                        })
                    },
                )?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            network_page(
                rows,
                current_rv,
                Phase::PodCleanup(None),
                |row| Phase::NodeDataplane(Some(row.node_name.clone())),
                NetworkMutation::PutNodeDataplane,
            )
        }
        Phase::PodCleanup(after) => read_pod_cleanup(conn, after, limit, current_rv),
        Phase::Watermark(after) => read_watermarks(conn, after, limit),
        Phase::AppliedOutbox(after) => read_applied(conn, after, limit),
        Phase::ReplayFloor(after) => read_floors(conn, after, limit),
        Phase::Complete => Ok((None, Phase::Complete)),
    }
}

fn resource_from_row(
    row: &rusqlite::Row<'_>,
    api_version: &str,
    kind: &str,
    namespace: Option<String>,
) -> rusqlite::Result<Resource> {
    let data: Vec<u8> = row.get(3)?;
    Ok(Resource {
        id: 0,
        api_version: api_version.to_string(),
        kind: kind.to_string(),
        namespace,
        name: row.get(0)?,
        uid: row.get(1)?,
        resource_version: row.get(2)?,
        data: Arc::new(serde_json::from_slice(&data).map_err(text_error)?),
    })
}

fn commit_page(
    rows: Vec<Resource>,
    empty_next: Phase,
    cursor: impl Fn(&Resource) -> Phase,
) -> rusqlite::Result<(Option<SnapshotCapturePage>, Phase)> {
    if rows.is_empty() {
        return Ok((None, empty_next));
    }
    let next = cursor(rows.last().unwrap());
    let commits = rows
        .iter()
        .map(klights_cluster_core::resource_snapshot_restore_operation)
        .collect();
    Ok((Some(page_commits(commits)?), next))
}

fn network_page<T>(
    rows: Vec<T>,
    current_rv: i64,
    empty_next: Phase,
    cursor: impl Fn(&T) -> Phase,
    mutation: impl Fn(T) -> NetworkMutation,
) -> rusqlite::Result<(Option<SnapshotCapturePage>, Phase)> {
    if rows.is_empty() {
        return Ok((None, empty_next));
    }
    let next = cursor(rows.last().unwrap());
    let commits = rows
        .into_iter()
        .map(|row| snapshot_operation(current_rv, vec![ClusterMutation::Network(mutation(row))]))
        .collect();
    Ok((Some(page_commits(commits)?), next))
}

fn read_pod_cleanup(
    conn: &rusqlite::Connection,
    after: Option<(String, String, String, String, String)>,
    limit: usize,
    _current_rv: i64,
) -> rusqlite::Result<(Option<SnapshotCapturePage>, Phase)> {
    let after = after.unwrap_or_default();
    let mut stmt = conn.prepare(
        "SELECT node_name,namespace,pod_name,pod_uid,reason,resource_version,created_at_ms,pod_data
         FROM pod_cleanup_intents
         WHERE (node_name,namespace,pod_name,pod_uid,reason)>(?1,?2,?3,?4,?5)
         ORDER BY node_name,namespace,pod_name,pod_uid,reason LIMIT ?6",
    )?;
    let rows = stmt
        .query_map(
            params![after.0, after.1, after.2, after.3, after.4, limit as i64],
            |row| {
                let data: Vec<u8> = row.get(7)?;
                Ok(klights_cluster_core::LogApplyPodCleanupIntentRow {
                    node_name: row.get(0)?,
                    namespace: row.get(1)?,
                    pod_name: row.get(2)?,
                    pod_uid: row.get(3)?,
                    reason: row.get(4)?,
                    resource_version: row.get(5)?,
                    created_at_ms: row.get(6)?,
                    pod_data: serde_json::from_slice(&data).map_err(text_error)?,
                })
            },
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if rows.is_empty() {
        return Ok((None, Phase::Watermark(None)));
    }
    let last = rows.last().unwrap();
    let next = Phase::PodCleanup(Some((
        last.node_name.clone(),
        last.namespace.clone(),
        last.pod_name.clone(),
        last.pod_uid.clone(),
        last.reason.clone(),
    )));
    let commits = rows
        .into_iter()
        .map(|intent| {
            snapshot_operation(
                intent.resource_version,
                vec![ClusterMutation::PodCleanup(
                    klights_cluster_core::PodCleanupMutation::PutPodCleanupIntent(intent),
                )],
            )
        })
        .collect();
    Ok((Some(page_commits(commits)?), next))
}

fn read_applied(
    conn: &rusqlite::Connection,
    after: Option<String>,
    limit: usize,
) -> rusqlite::Result<(Option<SnapshotCapturePage>, Phase)> {
    let mut stmt = conn.prepare(
        "SELECT idempotency_key,subject_key,operation,first_seen_ms,applied_rv,result_proto,status_stamp
         FROM applied_outbox WHERE idempotency_key>?1 ORDER BY idempotency_key LIMIT ?2",
    )?;
    let rows = stmt
        .query_map(
            params![after.as_deref().unwrap_or(""), limit as i64],
            |row| {
                Ok(klights_cluster_core::LogApplyAppliedOutboxRow {
                    idempotency_key: row.get(0)?,
                    subject_key: row.get(1)?,
                    operation: row.get(2)?,
                    first_seen_ms: row.get(3)?,
                    applied_rv: row.get(4)?,
                    result_proto: row.get(5)?,
                    status_stamp: row.get(6)?,
                })
            },
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if rows.is_empty() {
        return Ok((None, Phase::ReplayFloor(None)));
    }
    let next = Phase::AppliedOutbox(Some(rows.last().unwrap().idempotency_key.clone()));
    let page = SnapshotCapturePage::try_applied_outbox(rows).map_err(text_error)?;
    Ok((Some(page), next))
}

fn read_watermarks(
    conn: &rusqlite::Connection,
    after: Option<(String, i64)>,
    limit: usize,
) -> rusqlite::Result<(Option<SnapshotCapturePage>, Phase)> {
    let after = after.unwrap_or_default();
    let mut stmt = conn.prepare(
        "SELECT client_id,stream_id,last_seq FROM outbox_stream_watermarks
         WHERE (client_id,stream_id)>(?1,?2) ORDER BY client_id,stream_id LIMIT ?3",
    )?;
    let rows = stmt
        .query_map(params![after.0, after.1, limit as i64], |row| {
            Ok(klights_cluster_core::OutboxStreamWatermark {
                client_id: row.get(0)?,
                stream_id: row.get(1)?,
                stream_seq: row.get(2)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if rows.is_empty() {
        return Ok((None, Phase::AppliedOutbox(None)));
    }
    let last = rows.last().unwrap();
    let next = Phase::Watermark(Some((last.client_id.clone(), last.stream_id)));
    let page = SnapshotCapturePage::try_outbox_watermarks(rows).map_err(text_error)?;
    Ok((Some(page), next))
}

fn read_floors(
    conn: &rusqlite::Connection,
    after: Option<(String, String, String)>,
    limit: usize,
) -> rusqlite::Result<(Option<SnapshotCapturePage>, Phase)> {
    let after = after.unwrap_or_default();
    let mut stmt = conn.prepare(
        "SELECT api_version,kind,namespace_key,floor_rv,floor_event_id,floor_position_exact
         FROM watch_replay_floors
         WHERE (api_version,kind,namespace_key)>(?1,?2,?3)
         ORDER BY api_version,kind,namespace_key LIMIT ?4",
    )?;
    let rows = stmt
        .query_map(params![after.0, after.1, after.2, limit as i64], |row| {
            let api: String = row.get(0)?;
            let kind: String = row.get(1)?;
            let namespace: String = row.get(2)?;
            let target = match (api.as_str(), kind.as_str(), namespace.as_str()) {
                ("*", "*", "*") => DurableReplayTarget::All,
                (_, _, "#cluster") => DurableReplayTarget::Cluster {
                    api_version: api,
                    kind,
                },
                _ => DurableReplayTarget::Namespaced {
                    api_version: api,
                    kind,
                    namespace,
                },
            };
            DurableReplayFloor::new(target, row.get(3)?, row.get(4)?, row.get::<_, i64>(5)? != 0)
                .map_err(text_error)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if rows.is_empty() {
        return Ok((None, Phase::Complete));
    }
    let last = rows.last().unwrap();
    let target = last.target();
    let key = match target {
        DurableReplayTarget::All => ("*".to_string(), "*".to_string(), "*".to_string()),
        DurableReplayTarget::Cluster { api_version, kind } => {
            (api_version.clone(), kind.clone(), "#cluster".to_string())
        }
        DurableReplayTarget::Namespaced {
            api_version,
            kind,
            namespace,
        } => (api_version.clone(), kind.clone(), namespace.clone()),
    };
    let page = SnapshotCapturePage::try_replay_floors(rows).map_err(text_error)?;
    Ok((Some(page), Phase::ReplayFloor(Some(key))))
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

fn page_commits(
    operations: Vec<SnapshotRestoreOperation>,
) -> rusqlite::Result<SnapshotCapturePage> {
    SnapshotCapturePage::try_operations(operations).map_err(text_error)
}

fn text_error(error: impl std::fmt::Display + Send + Sync + 'static) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(SnapshotPersistenceError::CorruptData {
        message: error.to_string(),
    }))
}

pub(crate) fn map_sqlite_snapshot_error(
    error: tokio_rusqlite::Error<klights_supervisor::DbError>,
) -> SnapshotPersistenceError {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(&error);
    while let Some(current) = source {
        if let Some(snapshot) = current.downcast_ref::<SnapshotPersistenceError>() {
            return snapshot.clone();
        }
        source = current.source();
    }
    SnapshotPersistenceError::PersistenceFailed {
        message: error.to_string(),
    }
}

fn watch_event_mutation(event_id: i64, resource: Resource, event_type: String) -> ClusterMutation {
    ClusterMutation::WatchHistory(klights_cluster_core::WatchHistoryMutation::PutWatchEvent(
        klights_cluster_core::LogApplyWatchEventRow {
            event_id: Some(event_id),
            api_version: resource.api_version,
            kind: resource.kind,
            namespace: resource.namespace,
            name: resource.name,
            resource_version: resource.resource_version,
            event_type,
            data: Arc::unwrap_or_clone(resource.data),
        },
    ))
}
