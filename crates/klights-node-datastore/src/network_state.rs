//! SQLite persistence for node-local pod network allocations and endpoints.

use std::net::Ipv4Addr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::{Stream, StreamExt};
use klights_node_store::{
    CacheNetworkError, CacheNetworkFuture, EndpointDeleteOutcome, EndpointUpsertOutcome, NodeKey,
    PodEndpointMode, PodEndpointRecord, PodEndpointStore, PodEndpointStoreEvent,
    PodEndpointStoreEventSource, PodEndpointStoreEventStream, PodEndpointStoreEventSubscription,
    PodIpamStore, PodNetworkAllocation, PodNetworkAllocationRequest, PodNetworkAssignmentSnapshot,
    PodNetworkCache, PodNetworkEndpoint, PodUidKey, SandboxKey,
};
use klights_supervisor::{DbExecutor, WallClock};
use klights_types::PodIdentity;
use rusqlite::OptionalExtension;
use tokio::sync::{Mutex, broadcast};

const POD_ENDPOINT_CHANNEL_BOUND: usize = 4_096;

/// Passive SQLite adapter for the focused node cache, endpoint, and IPAM ports.
#[derive(Clone)]
pub struct SqliteNodeNetworkStateStore {
    executor: DbExecutor,
    wall_clock: Arc<dyn WallClock>,
    pod_endpoint_tx: broadcast::Sender<PodEndpointStoreEvent>,
    pod_endpoint_handoff: Arc<Mutex<()>>,
}

impl SqliteNodeNetworkStateStore {
    pub fn new(executor: DbExecutor, wall_clock: Arc<dyn WallClock>) -> Self {
        let (pod_endpoint_tx, _) = broadcast::channel(POD_ENDPOINT_CHANNEL_BOUND);
        Self {
            executor,
            wall_clock,
            pod_endpoint_tx,
            pod_endpoint_handoff: Arc::new(Mutex::new(())),
        }
    }

    fn now_ms(&self) -> i64 {
        self.wall_clock
            .now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(i64::MAX as u128) as i64
    }

    async fn db_call<T, F>(&self, query_name: &'static str, call: F) -> tokio_rusqlite::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut rusqlite::Connection) -> tokio_rusqlite::Result<T> + Send + 'static,
    {
        self.executor.call_raw(query_name, call).await
    }

    async fn endpoint_snapshot_subscription(
        &self,
    ) -> Result<
        (
            Vec<PodEndpointRecord>,
            broadcast::Receiver<PodEndpointStoreEvent>,
        ),
        CacheNetworkError,
    > {
        let _handoff = self.pod_endpoint_handoff.lock().await;
        let receiver = self.pod_endpoint_tx.subscribe();
        let snapshot = self.list_endpoint_records().await?;
        Ok((snapshot, receiver))
    }

    async fn list_endpoint_records(&self) -> Result<Vec<PodEndpointRecord>, CacheNetworkError> {
        let rows = self
            .db_call("node_local:list_endpoints_all", move |conn| {
                conn.prepare(
                    "SELECT pod_uid, namespace, pod_name, node_name, mode, pod_ip, node_ip, \
                            host_port_tcp, host_port_udp, generation, updated_ms \
                       FROM pod_endpoints ORDER BY node_name, pod_uid",
                )?
                .query_map([], endpoint_row)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(tokio_rusqlite::Error::from)
            })
            .await
            .map_err(|error| persistence_error("pod endpoint list all", error))?;
        rows.into_iter().map(endpoint_record).collect()
    }
}

impl PodNetworkCache for SqliteNodeNetworkStateStore {
    fn get_network_for_uid(
        &self,
        pod_uid: PodUidKey,
    ) -> CacheNetworkFuture<'_, Option<PodNetworkEndpoint>> {
        Box::pin(async move {
            let pod_uid = pod_uid.into_inner();
            let row = self
                .db_call("node_local:get_network_uid", move |conn| {
                    conn.query_row(
                        "SELECT ip_addr, veth_host, netns_path FROM pod_networks \
                         WHERE pod_uid = ?1 ORDER BY created_ms DESC LIMIT 1",
                        [pod_uid],
                        network_endpoint_row,
                    )
                    .optional()
                    .map_err(tokio_rusqlite::Error::from)
                })
                .await
                .map_err(|error| persistence_error("pod network get uid", error))?;
            row.map(network_endpoint).transpose()
        })
    }

    fn get_network_for_pod(
        &self,
        pod: PodIdentity,
    ) -> CacheNetworkFuture<'_, Option<PodNetworkEndpoint>> {
        Box::pin(async move {
            let row = self
                .db_call("node_local:get_network_assignment_pod", move |conn| {
                    conn.query_row(
                        "SELECT sandbox_id, namespace, pod_name, pod_uid, subnet_base_int, \
                                subnet_size, ip_addr, ip_int, veth_host, netns_path \
                           FROM pod_networks \
                          WHERE namespace = ?1 AND pod_name = ?2 AND pod_uid = ?3 \
                          ORDER BY created_ms DESC LIMIT 1",
                        rusqlite::params![pod.namespace, pod.name, pod.uid],
                        network_assignment_row,
                    )
                    .optional()
                    .map_err(tokio_rusqlite::Error::from)
                })
                .await
                .map_err(|error| persistence_error("pod network assignment get pod", error))?;
            row.map(|row| network_endpoint(row.endpoint_row()))
                .transpose()
        })
    }

    fn get_network_for_sandbox(
        &self,
        sandbox_id: SandboxKey,
    ) -> CacheNetworkFuture<'_, Option<PodNetworkEndpoint>> {
        Box::pin(async move {
            let sandbox_id = sandbox_id.into_inner();
            let row = self
                .db_call("node_local:get_network_sandbox", move |conn| {
                    conn.query_row(
                        "SELECT ip_addr, veth_host, netns_path FROM pod_networks \
                         WHERE sandbox_id = ?1",
                        [sandbox_id],
                        network_endpoint_row,
                    )
                    .optional()
                    .map_err(tokio_rusqlite::Error::from)
                })
                .await
                .map_err(|error| persistence_error("pod network get sandbox", error))?;
            row.map(network_endpoint).transpose()
        })
    }

    fn get_network_for_assignment(
        &self,
        sandbox_id: SandboxKey,
        pod: PodIdentity,
    ) -> CacheNetworkFuture<'_, Option<PodNetworkEndpoint>> {
        Box::pin(async move {
            let sandbox_id = sandbox_id.into_inner();
            let row = self
                .db_call("node_local:get_network_assignment_sandbox", move |conn| {
                    conn.query_row(
                        "SELECT sandbox_id, namespace, pod_name, pod_uid, subnet_base_int, \
                                subnet_size, ip_addr, ip_int, veth_host, netns_path \
                           FROM pod_networks WHERE sandbox_id = ?1",
                        [sandbox_id],
                        network_assignment_row,
                    )
                    .optional()
                    .map_err(tokio_rusqlite::Error::from)
                })
                .await
                .map_err(|error| persistence_error("pod network assignment get sandbox", error))?;
            let Some(row) = row else {
                return Ok(None);
            };
            if row.namespace != pod.namespace || row.pod_name != pod.name || row.pod_uid != pod.uid
            {
                return Ok(None);
            }
            network_endpoint(row.endpoint_row()).map(Some)
        })
    }

    fn delete_network_for_sandbox(&self, sandbox_id: SandboxKey) -> CacheNetworkFuture<'_, ()> {
        Box::pin(async move {
            let sandbox_id = sandbox_id.into_inner();
            self.db_call("node_local:delete_network_sandbox", move |conn| {
                conn.execute(
                    "DELETE FROM pod_networks WHERE sandbox_id = ?1",
                    [sandbox_id],
                )?;
                Ok(())
            })
            .await
            .map_err(|error| persistence_error("pod network delete", error))
        })
    }

    fn delete_network_if_matches(
        &self,
        request: PodNetworkAllocationRequest,
    ) -> CacheNetworkFuture<'_, bool> {
        Box::pin(async move {
            let (sandbox_id, pod, subnet_base, subnet_size, veth_host, netns_path) =
                request.into_parts();
            self.db_call(
                "node_local:delete_network_assignment_if_matches",
                move |conn| {
                    let deleted = conn.execute(
                        "DELETE FROM pod_networks \
                          WHERE sandbox_id = ?1 AND namespace = ?2 AND pod_name = ?3 \
                            AND pod_uid = ?4 AND subnet_base_int = ?5 AND subnet_size = ?6 \
                            AND veth_host = ?7 AND netns_path = ?8",
                        rusqlite::params![
                            sandbox_id,
                            pod.namespace,
                            pod.name,
                            pod.uid,
                            i64::from(subnet_base),
                            i64::from(subnet_size),
                            veth_host,
                            netns_path,
                        ],
                    )?;
                    Ok(deleted == 1)
                },
            )
            .await
            .map_err(|error| persistence_error("pod network assignment conditional delete", error))
        })
    }

    fn list_network_assignments(
        &self,
    ) -> CacheNetworkFuture<'_, Vec<PodNetworkAssignmentSnapshot>> {
        Box::pin(async move {
            let rows = self
                .db_call("node_local:list_network_assignments", move |conn| {
                    conn.prepare(
                        "SELECT sandbox_id, namespace, pod_name, pod_uid, subnet_base_int, \
                                subnet_size, ip_addr, ip_int, veth_host, netns_path \
                           FROM pod_networks ORDER BY sandbox_id",
                    )?
                    .query_map([], network_assignment_row)?
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .map_err(tokio_rusqlite::Error::from)
                })
                .await
                .map_err(|error| persistence_error("pod network list", error))?;
            rows.into_iter().map(network_assignment).collect()
        })
    }
}

impl PodIpamStore for SqliteNodeNetworkStateStore {
    fn reserve_ip_and_insert_network(
        &self,
        request: PodNetworkAllocationRequest,
    ) -> CacheNetworkFuture<'_, PodNetworkAllocation> {
        Box::pin(async move {
            let subnet_base_int = request.subnet_base_int();
            let subnet_size = request.subnet_size();
            let sandbox_id = request.sandbox_id().to_string();
            let now_ms = self.now_ms();
            let outcome = self
                .db_call("node_local:reserve_ip_network", move |conn| {
                    reserve_ip_and_insert_network_in_conn(conn, &request, now_ms)
                })
                .await
                .map_err(|error| persistence_error("pod network reserve", error))?;
            match outcome {
                PodNetworkReservationOutcome::Reserved(allocation) => Ok(allocation),
                PodNetworkReservationOutcome::IdentityConflict => {
                    Err(CacheNetworkError::IdentityConflict { sandbox_id })
                }
                PodNetworkReservationOutcome::AddressExhausted => {
                    Err(CacheNetworkError::AddressExhausted {
                        subnet_base_int,
                        subnet_size,
                    })
                }
            }
        })
    }
}

impl PodEndpointStore for SqliteNodeNetworkStateStore {
    fn upsert_endpoint(
        &self,
        record: PodEndpointRecord,
    ) -> CacheNetworkFuture<'_, EndpointUpsertOutcome> {
        Box::pin(async move {
            let _handoff = self.pod_endpoint_handoff.lock().await;
            let current = record.clone();
            let row = EndpointRow::from_record(record);
            let previous = self
                .db_call("node_local:upsert_endpoint", move |conn| {
                    let transaction = conn.transaction()?;
                    let previous = transaction
                        .query_row(
                            "SELECT pod_uid, namespace, pod_name, node_name, mode, pod_ip, node_ip, \
                                    host_port_tcp, host_port_udp, generation, updated_ms \
                               FROM pod_endpoints WHERE pod_uid = ?1",
                            [&row.pod_uid],
                            endpoint_row,
                        )
                        .optional()?;
                    let previous = match previous.map(endpoint_record).transpose() {
                        Ok(previous) => previous,
                        Err(error) => return Ok(Err(error)),
                    };
                    transaction.execute(
                        "INSERT OR REPLACE INTO pod_endpoints \
                         (pod_uid, namespace, pod_name, node_name, mode, pod_ip, node_ip, \
                          host_port_tcp, host_port_udp, generation, updated_ms) \
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                        rusqlite::params![
                            row.pod_uid,
                            row.namespace,
                            row.pod_name,
                            row.node_name,
                            row.mode,
                            row.pod_ip,
                            row.node_ip,
                            row.host_port_tcp,
                            row.host_port_udp,
                            row.generation,
                            row.updated_at_ms,
                        ],
                    )?;
                    transaction.commit()?;
                    Ok(Ok(previous))
                })
                .await
                .map_err(|error| persistence_error("pod endpoint upsert", error))??;

            if let Some(previous) = &previous
                && previous.pod_ip() != current.pod_ip()
            {
                let _ = self.pod_endpoint_tx.send(PodEndpointStoreEvent::Delete {
                    pod_ip: previous.pod_ip(),
                });
            }
            let _ = self
                .pod_endpoint_tx
                .send(PodEndpointStoreEvent::Upsert(current.clone()));
            Ok(EndpointUpsertOutcome::new(previous, current))
        })
    }

    fn delete_endpoint_for_uid(
        &self,
        pod_uid: PodUidKey,
    ) -> CacheNetworkFuture<'_, EndpointDeleteOutcome> {
        Box::pin(async move {
            let _handoff = self.pod_endpoint_handoff.lock().await;
            let pod_uid = pod_uid.into_inner();
            let removed = self
                .db_call("node_local:delete_endpoint", move |conn| {
                    let transaction = conn.transaction()?;
                    let removed = transaction
                        .query_row(
                            "SELECT pod_uid, namespace, pod_name, node_name, mode, pod_ip, node_ip, \
                                    host_port_tcp, host_port_udp, generation, updated_ms \
                               FROM pod_endpoints WHERE pod_uid = ?1",
                            [&pod_uid],
                            endpoint_row,
                        )
                        .optional()?;
                    let removed = match removed.map(endpoint_record).transpose() {
                        Ok(removed) => removed,
                        Err(error) => return Ok(Err(error)),
                    };
                    transaction
                        .execute("DELETE FROM pod_endpoints WHERE pod_uid = ?1", [&pod_uid])?;
                    transaction.commit()?;
                    Ok(Ok(removed))
                })
                .await
                .map_err(|error| persistence_error("pod endpoint delete", error))??;
            if let Some(removed) = &removed {
                let _ = self.pod_endpoint_tx.send(PodEndpointStoreEvent::Delete {
                    pod_ip: removed.pod_ip(),
                });
            }
            Ok(EndpointDeleteOutcome::new(removed))
        })
    }

    fn get_endpoint_by_pod_ip(
        &self,
        pod_ip: Ipv4Addr,
    ) -> CacheNetworkFuture<'_, Option<PodEndpointRecord>> {
        Box::pin(async move {
            let pod_ip = pod_ip.to_string();
            self.db_call("node_local:get_endpoint_by_pod_ip", move |conn| {
                conn.query_row(
                    "SELECT pod_uid, namespace, pod_name, node_name, mode, pod_ip, node_ip, \
                            host_port_tcp, host_port_udp, generation, updated_ms \
                       FROM pod_endpoints WHERE pod_ip = ?1",
                    [&pod_ip],
                    endpoint_row,
                )
                .optional()
                .map_err(tokio_rusqlite::Error::from)
            })
            .await
            .map_err(|error| persistence_error("pod endpoint get by pod_ip", error))?
            .map(endpoint_record)
            .transpose()
        })
    }

    fn list_endpoints_all(&self) -> CacheNetworkFuture<'_, Vec<PodEndpointRecord>> {
        Box::pin(async move { self.list_endpoint_records().await })
    }

    fn list_endpoints_for_node(
        &self,
        node_name: NodeKey,
    ) -> CacheNetworkFuture<'_, Vec<PodEndpointRecord>> {
        Box::pin(async move {
            let node_name = node_name.into_inner();
            let rows = self
                .db_call("node_local:list_endpoints_node", move |conn| {
                    conn.prepare(
                        "SELECT pod_uid, namespace, pod_name, node_name, mode, pod_ip, node_ip, \
                                host_port_tcp, host_port_udp, generation, updated_ms \
                           FROM pod_endpoints WHERE node_name = ?1 ORDER BY pod_uid",
                    )?
                    .query_map([node_name], endpoint_row)?
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .map_err(tokio_rusqlite::Error::from)
                })
                .await
                .map_err(|error| persistence_error("pod endpoint list by node", error))?;
            rows.into_iter().map(endpoint_record).collect()
        })
    }
}

struct EndpointStoreEventSubscription {
    inner: futures::stream::BoxStream<'static, Result<PodEndpointStoreEvent, CacheNetworkError>>,
}

impl PodEndpointStoreEventSubscription for EndpointStoreEventSubscription {
    fn poll_next(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<PodEndpointStoreEvent, CacheNetworkError>>> {
        Stream::poll_next(Pin::new(&mut self.get_mut().inner), context)
    }
}

impl PodEndpointStoreEventSource for SqliteNodeNetworkStateStore {
    fn subscribe_endpoint_events(&self) -> CacheNetworkFuture<'_, PodEndpointStoreEventStream> {
        Box::pin(async move {
            let (snapshot, receiver) = self.endpoint_snapshot_subscription().await?;
            let store = self.clone();
            let stream = futures::stream::unfold(
                (Some(snapshot), Some(receiver), store),
                |(initial, receiver, store)| async move {
                    if let Some(initial) = initial {
                        return Some((
                            Ok(PodEndpointStoreEvent::Resync(initial)),
                            (None, receiver, store),
                        ));
                    }
                    let mut receiver = receiver?;
                    match receiver.recv().await {
                        Ok(event) => Some((Ok(event), (None, Some(receiver), store))),
                        Err(broadcast::error::RecvError::Closed) => None,
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            match store.endpoint_snapshot_subscription().await {
                                Ok((snapshot, fresh_receiver)) => Some((
                                    Ok(PodEndpointStoreEvent::Resync(snapshot)),
                                    (None, Some(fresh_receiver), store),
                                )),
                                Err(error) => Some((Err(error), (None, None, store))),
                            }
                        }
                    }
                },
            )
            .boxed();
            Ok(Box::pin(EndpointStoreEventSubscription { inner: stream })
                as PodEndpointStoreEventStream)
        })
    }
}

#[derive(Debug, Eq, PartialEq)]
enum PodNetworkReservationOutcome {
    Reserved(PodNetworkAllocation),
    IdentityConflict,
    AddressExhausted,
}

fn reserve_ip_and_insert_network_in_conn(
    conn: &rusqlite::Connection,
    request: &PodNetworkAllocationRequest,
    now_ms: i64,
) -> tokio_rusqlite::Result<PodNetworkReservationOutcome> {
    if let Some(existing) = conn
        .query_row(
            "SELECT namespace, pod_name, pod_uid, subnet_base_int, subnet_size, \
                    ip_addr, ip_int, veth_host, netns_path \
               FROM pod_networks WHERE sandbox_id = ?1",
            [request.sandbox_id()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)? as u32,
                    row.get::<_, i64>(4)? as u32,
                    row.get::<_, String>(5)?,
                    row.get::<_, i64>(6)? as u32,
                    row.get::<_, String>(7)?,
                    row.get::<_, String>(8)?,
                ))
            },
        )
        .optional()?
    {
        let exact_identity = existing.0 == request.pod().namespace
            && existing.1 == request.pod().name
            && existing.2 == request.pod().uid
            && existing.7 == request.veth_host()
            && existing.8 == request.netns_path();
        let exact_subnet =
            existing.3 == request.subnet_base_int() && existing.4 == request.subnet_size();
        if exact_identity && exact_subnet {
            return Ok(PodNetworkReservationOutcome::Reserved(
                PodNetworkAllocation::try_new(existing.5, existing.6)
                    .map_err(cache_network_db_error)?,
            ));
        }

        let usable_start = request.subnet_base_int() + 2;
        let usable_end = request.subnet_base_int() + request.subnet_size() - 2;
        let legacy_subnet = existing.3 == 0 && existing.4 == 0;
        let stored_ip_matches = existing
            .5
            .parse::<Ipv4Addr>()
            .is_ok_and(|ip| u32::from(ip) == existing.6);
        if exact_identity
            && legacy_subnet
            && stored_ip_matches
            && existing.6 >= usable_start
            && existing.6 <= usable_end
        {
            let adopted = conn.execute(
                "UPDATE pod_networks \
                    SET subnet_base_int = ?1, subnet_size = ?2 \
                  WHERE sandbox_id = ?3 AND namespace = ?4 AND pod_name = ?5 AND pod_uid = ?6 \
                    AND subnet_base_int = 0 AND subnet_size = 0 \
                    AND ip_addr = ?7 AND ip_int = ?8 AND veth_host = ?9 AND netns_path = ?10 \
                    AND ip_int >= ?11 AND ip_int <= ?12",
                rusqlite::params![
                    i64::from(request.subnet_base_int()),
                    i64::from(request.subnet_size()),
                    request.sandbox_id(),
                    request.pod().namespace,
                    request.pod().name,
                    request.pod().uid,
                    existing.5,
                    i64::from(existing.6),
                    request.veth_host(),
                    request.netns_path(),
                    i64::from(usable_start),
                    i64::from(usable_end),
                ],
            )?;
            if adopted == 1 {
                return Ok(PodNetworkReservationOutcome::Reserved(
                    PodNetworkAllocation::try_new(existing.5, existing.6)
                        .map_err(cache_network_db_error)?,
                ));
            }
        }

        return Ok(PodNetworkReservationOutcome::IdentityConflict);
    }

    let start = request.subnet_base_int() + 2;
    let end = request.subnet_base_int() + request.subnet_size() - 2;
    let max_allocated: Option<i64> = conn.query_row(
        "SELECT MAX(ip_int) FROM pod_networks WHERE ip_int >= ?1 AND ip_int <= ?2",
        rusqlite::params![i64::from(start), i64::from(end)],
        |row| row.get(0),
    )?;
    let next_after_max = max_allocated
        .map(|value| value as u32 + 1)
        .filter(|candidate| *candidate <= end)
        .unwrap_or(start);
    let usable_count = end - start + 1;

    for offset in 0..usable_count {
        let candidate = start + ((next_after_max - start + offset) % usable_count);
        let ip_addr = klights_types::ipv4_from_u32(candidate);
        let inserted = conn.execute(
            "INSERT INTO pod_networks \
             (sandbox_id, namespace, pod_name, pod_uid, subnet_base_int, subnet_size, \
              ip_addr, ip_int, veth_host, netns_path, created_ms) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11) \
             ON CONFLICT(ip_int) DO NOTHING",
            rusqlite::params![
                request.sandbox_id(),
                request.pod().namespace,
                request.pod().name,
                request.pod().uid,
                i64::from(request.subnet_base_int()),
                i64::from(request.subnet_size()),
                ip_addr,
                i64::from(candidate),
                request.veth_host(),
                request.netns_path(),
                now_ms,
            ],
        )?;
        if inserted > 0 {
            return Ok(PodNetworkReservationOutcome::Reserved(
                PodNetworkAllocation::try_new(klights_types::ipv4_from_u32(candidate), candidate)
                    .map_err(cache_network_db_error)?,
            ));
        }
    }

    Ok(PodNetworkReservationOutcome::AddressExhausted)
}

#[derive(Clone)]
struct NetworkEndpointRow {
    ip_addr: String,
    veth_host: String,
    netns_path: String,
}

fn network_endpoint_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<NetworkEndpointRow> {
    Ok(NetworkEndpointRow {
        ip_addr: row.get(0)?,
        veth_host: row.get(1)?,
        netns_path: row.get(2)?,
    })
}

fn network_endpoint(row: NetworkEndpointRow) -> Result<PodNetworkEndpoint, CacheNetworkError> {
    PodNetworkEndpoint::try_new(row.ip_addr, row.veth_host, row.netns_path)
        .map_err(|error| CacheNetworkError::corrupt_data(error.to_string()))
}

#[derive(Clone)]
struct NetworkAssignmentRow {
    sandbox_id: String,
    namespace: String,
    pod_name: String,
    pod_uid: String,
    subnet_base_int: u32,
    subnet_size: u32,
    ip_addr: String,
    ip_int: u32,
    veth_host: String,
    netns_path: String,
}

impl NetworkAssignmentRow {
    fn endpoint_row(&self) -> NetworkEndpointRow {
        NetworkEndpointRow {
            ip_addr: self.ip_addr.clone(),
            veth_host: self.veth_host.clone(),
            netns_path: self.netns_path.clone(),
        }
    }
}

fn network_assignment_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<NetworkAssignmentRow> {
    Ok(NetworkAssignmentRow {
        sandbox_id: row.get(0)?,
        namespace: row.get(1)?,
        pod_name: row.get(2)?,
        pod_uid: row.get(3)?,
        subnet_base_int: row.get::<_, i64>(4)? as u32,
        subnet_size: row.get::<_, i64>(5)? as u32,
        ip_addr: row.get(6)?,
        ip_int: row.get::<_, i64>(7)? as u32,
        veth_host: row.get(8)?,
        netns_path: row.get(9)?,
    })
}

fn network_assignment(
    row: NetworkAssignmentRow,
) -> Result<PodNetworkAssignmentSnapshot, CacheNetworkError> {
    let request = PodNetworkAllocationRequest::try_from_persisted(
        row.sandbox_id,
        PodIdentity::new(&row.namespace, &row.pod_name, &row.pod_uid),
        row.subnet_base_int,
        row.subnet_size,
        row.veth_host,
        row.netns_path,
    )
    .map_err(|error| CacheNetworkError::corrupt_data(error.to_string()))?;
    let allocation = PodNetworkAllocation::try_new(row.ip_addr, row.ip_int)
        .map_err(|error| CacheNetworkError::corrupt_data(error.to_string()))?;
    PodNetworkAssignmentSnapshot::try_new(request, allocation)
        .map_err(|error| CacheNetworkError::corrupt_data(error.to_string()))
}

#[derive(Clone)]
struct EndpointRow {
    pod_uid: String,
    namespace: String,
    pod_name: String,
    node_name: String,
    mode: String,
    pod_ip: String,
    node_ip: Option<String>,
    host_port_tcp: Option<i64>,
    host_port_udp: Option<i64>,
    generation: i64,
    updated_at_ms: i64,
}

impl EndpointRow {
    fn from_record(record: PodEndpointRecord) -> Self {
        let (pod, node_name, mode, pod_ip, node_ip, tcp, udp, generation, updated_at_ms) =
            record.into_parts();
        Self {
            pod_uid: pod.uid,
            namespace: pod.namespace,
            pod_name: pod.name,
            node_name,
            mode: match mode {
                PodEndpointMode::EncryptedDirect => "encrypted_direct",
                PodEndpointMode::Hostport => "hostport",
            }
            .to_string(),
            pod_ip: pod_ip.to_string(),
            node_ip: Some(node_ip.to_string()),
            host_port_tcp: tcp.map(i64::from),
            host_port_udp: udp.map(i64::from),
            generation,
            updated_at_ms,
        }
    }
}

fn endpoint_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<EndpointRow> {
    Ok(EndpointRow {
        pod_uid: row.get(0)?,
        namespace: row.get(1)?,
        pod_name: row.get(2)?,
        node_name: row.get(3)?,
        mode: row.get(4)?,
        pod_ip: row.get(5)?,
        node_ip: row.get(6)?,
        host_port_tcp: row.get(7)?,
        host_port_udp: row.get(8)?,
        generation: row.get(9)?,
        updated_at_ms: row.get(10)?,
    })
}

fn endpoint_record(row: EndpointRow) -> Result<PodEndpointRecord, CacheNetworkError> {
    let mode = match row.mode.as_str() {
        "encrypted_direct" => PodEndpointMode::EncryptedDirect,
        "hostport" => PodEndpointMode::Hostport,
        other => {
            return Err(CacheNetworkError::corrupt_data(format!(
                "invalid pod endpoint mode {other}"
            )));
        }
    };
    let pod_ip = row.pod_ip.parse::<Ipv4Addr>().map_err(|error| {
        CacheNetworkError::corrupt_data(format!("invalid persisted pod IP: {error}"))
    })?;
    let node_ip = row
        .node_ip
        .as_deref()
        .filter(|value| !value.is_empty())
        .unwrap_or(&row.pod_ip)
        .parse::<Ipv4Addr>()
        .map_err(|error| {
            CacheNetworkError::corrupt_data(format!("invalid persisted node IP: {error}"))
        })?;
    let host_port_tcp = persisted_endpoint_port(row.host_port_tcp)?;
    let host_port_udp = persisted_endpoint_port(row.host_port_udp)?;
    PodEndpointRecord::try_from_persisted(
        PodIdentity::new(&row.namespace, &row.pod_name, &row.pod_uid),
        row.node_name,
        mode,
        pod_ip,
        node_ip,
        host_port_tcp,
        host_port_udp,
        row.generation,
        row.updated_at_ms,
    )
    .map_err(|error| CacheNetworkError::corrupt_data(error.to_string()))
}

fn persisted_endpoint_port(value: Option<i64>) -> Result<Option<i64>, CacheNetworkError> {
    match value {
        Some(value) if !(1..=i64::from(u16::MAX)).contains(&value) => Err(
            CacheNetworkError::corrupt_data("pod endpoint port outside 1..=65535"),
        ),
        value => Ok(value),
    }
}

fn persistence_error(operation: &str, error: tokio_rusqlite::Error) -> CacheNetworkError {
    CacheNetworkError::persistence_failed(format!("{operation} failed: {error}"))
}

fn cache_network_db_error(error: CacheNetworkError) -> tokio_rusqlite::Error {
    tokio_rusqlite::Error::Other(Box::new(error))
}

#[cfg(test)]
mod tests {
    use super::*;
    use klights_supervisor::{SystemWallClock, TaskCategoryConfig, TaskSupervisor};

    async fn store(name: &'static str) -> SqliteNodeNetworkStateStore {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let executor = crate::open::open_with_opts(crate::open::in_memory_opts(), supervisor, name)
            .await
            .unwrap();
        SqliteNodeNetworkStateStore::new(executor, Arc::new(SystemWallClock))
    }

    fn allocation_request(
        sandbox_id: &str,
        pod_uid: &str,
        base: u32,
        size: u32,
    ) -> PodNetworkAllocationRequest {
        PodNetworkAllocationRequest::try_new(
            sandbox_id,
            PodIdentity::new("default", &format!("pod-{pod_uid}"), pod_uid),
            base,
            size,
            format!("veth-{pod_uid}"),
            format!("/run/netns/{pod_uid}"),
        )
        .unwrap()
    }

    fn endpoint(uid: &str, pod_ip: Ipv4Addr, generation: i64) -> PodEndpointRecord {
        PodEndpointRecord::try_new(
            PodIdentity::new("default", &format!("pod-{uid}"), uid),
            "node-a",
            PodEndpointMode::EncryptedDirect,
            pod_ip,
            Ipv4Addr::new(192, 0, 2, 10),
            None,
            None,
            generation,
            generation,
        )
        .unwrap()
    }

    async fn replace_with_legacy_network(store: &SqliteNodeNetworkStateStore, ip_addr: &str) {
        let ip_addr = ip_addr.to_string();
        let ip_int = i64::from(u32::from(Ipv4Addr::new(10, 42, 89, 2)));
        store
            .db_call("test_replace_legacy_network", move |conn| {
                conn.execute("DELETE FROM pod_networks", [])?;
                conn.execute(
                    "INSERT INTO pod_networks \
                     (sandbox_id, namespace, pod_name, pod_uid, ip_addr, ip_int, \
                      veth_host, netns_path, created_ms) \
                     VALUES ('sandbox-legacy', 'default', 'pod-legacy', 'uid-legacy', \
                             ?1, ?2, 'veth-legacy', '/run/netns/legacy', 1)",
                    rusqlite::params![ip_addr, ip_int],
                )?;
                Ok(())
            })
            .await
            .unwrap();
    }

    async fn stored_subnet(store: &SqliteNodeNetworkStateStore) -> (u32, u32) {
        store
            .db_call("test_read_network_subnet", move |conn| {
                conn.query_row(
                    "SELECT subnet_base_int, subnet_size FROM pod_networks",
                    [],
                    |row| Ok((row.get::<_, i64>(0)? as u32, row.get::<_, i64>(1)? as u32)),
                )
                .map_err(tokio_rusqlite::Error::from)
            })
            .await
            .unwrap()
    }

    async fn next_event(
        stream: &mut PodEndpointStoreEventStream,
    ) -> Result<PodEndpointStoreEvent, CacheNetworkError> {
        std::future::poll_fn(|context| stream.as_mut().poll_next(context))
            .await
            .expect("endpoint stream closed")
    }

    #[tokio::test]
    async fn sqlite_ipam_preserves_identity_idempotency_exhaustion_and_exact_delete() {
        let store = store("sqlite:node-network-ipam").await;
        let base = u32::from(Ipv4Addr::new(10, 42, 90, 0));
        let request = allocation_request("sandbox-a", "uid-a", base, 4);
        let allocation = store
            .reserve_ip_and_insert_network(request.clone())
            .await
            .unwrap();
        assert_eq!(allocation.ip_int(), base + 2);
        assert_eq!(
            store
                .reserve_ip_and_insert_network(request.clone())
                .await
                .unwrap(),
            allocation
        );
        assert!(matches!(
            store
                .reserve_ip_and_insert_network(allocation_request("sandbox-a", "uid-b", base, 4,))
                .await,
            Err(CacheNetworkError::IdentityConflict { .. })
        ));
        assert!(matches!(
            store
                .reserve_ip_and_insert_network(allocation_request("sandbox-b", "uid-b", base, 4,))
                .await,
            Err(CacheNetworkError::AddressExhausted { .. })
        ));
        let snapshots = store.list_network_assignments().await.unwrap();
        assert_eq!(snapshots.len(), 1);
        assert!(
            !store
                .delete_network_if_matches(allocation_request("sandbox-a", "uid-b", base, 4,))
                .await
                .unwrap()
        );
        assert!(store.delete_network_if_matches(request).await.unwrap());
        assert!(store.list_network_assignments().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn legacy_zero_subnet_is_adopted_only_for_the_exact_durable_identity() {
        let store = store("sqlite:node-network-legacy-adopt").await;
        let base = u32::from(Ipv4Addr::new(10, 42, 89, 0));
        replace_with_legacy_network(&store, "10.42.89.2").await;
        assert_eq!(stored_subnet(&store).await, (0, 0));

        let request = PodNetworkAllocationRequest::try_new(
            "sandbox-legacy",
            PodIdentity::new("default", "pod-legacy", "uid-legacy"),
            base,
            256,
            "veth-legacy",
            "/run/netns/legacy",
        )
        .unwrap();
        let allocation = store.reserve_ip_and_insert_network(request).await.unwrap();
        assert_eq!(allocation.ip_int(), base + 2);
        assert_eq!(stored_subnet(&store).await, (base, 256));
    }

    #[tokio::test]
    async fn legacy_zero_subnet_rejects_identity_range_and_stored_ip_mismatches() {
        let store = store("sqlite:node-network-legacy-conflict").await;
        let base = u32::from(Ipv4Addr::new(10, 42, 89, 0));
        let cases = [
            (
                "other",
                "pod-legacy",
                "uid-legacy",
                base,
                "veth-legacy",
                "/run/netns/legacy",
                "10.42.89.2",
            ),
            (
                "default",
                "other",
                "uid-legacy",
                base,
                "veth-legacy",
                "/run/netns/legacy",
                "10.42.89.2",
            ),
            (
                "default",
                "pod-legacy",
                "uid-legacy",
                base,
                "veth-legacy",
                "/run/netns/other",
                "10.42.89.2",
            ),
            (
                "default",
                "pod-legacy",
                "other",
                base,
                "veth-legacy",
                "/run/netns/legacy",
                "10.42.89.2",
            ),
            (
                "default",
                "pod-legacy",
                "uid-legacy",
                base,
                "veth-other",
                "/run/netns/legacy",
                "10.42.89.2",
            ),
            (
                "default",
                "pod-legacy",
                "uid-legacy",
                u32::from(Ipv4Addr::new(10, 42, 90, 0)),
                "veth-legacy",
                "/run/netns/legacy",
                "10.42.89.2",
            ),
            (
                "default",
                "pod-legacy",
                "uid-legacy",
                base,
                "veth-legacy",
                "/run/netns/legacy",
                "192.0.2.2",
            ),
        ];

        for (namespace, name, uid, requested_base, veth, netns, stored_ip) in cases {
            replace_with_legacy_network(&store, stored_ip).await;
            let request = PodNetworkAllocationRequest::try_new(
                "sandbox-legacy",
                PodIdentity::new(namespace, name, uid),
                requested_base,
                256,
                veth,
                netns,
            )
            .unwrap();
            assert!(matches!(
                store.reserve_ip_and_insert_network(request).await,
                Err(CacheNetworkError::IdentityConflict { .. })
            ));
            assert_eq!(
                stored_subnet(&store).await,
                (0, 0),
                "mismatch must not backfill the legacy row"
            );
        }
    }

    #[tokio::test]
    async fn endpoint_outcomes_and_notifications_share_committed_facts_and_order() {
        let store = store("sqlite:node-network-endpoint").await;
        let mut stream = store.subscribe_endpoint_events().await.unwrap();
        assert_eq!(
            next_event(&mut stream).await.unwrap(),
            PodEndpointStoreEvent::Resync(Vec::new())
        );

        let first = endpoint("uid-a", Ipv4Addr::new(10, 42, 0, 2), 1);
        let inserted = store.upsert_endpoint(first.clone()).await.unwrap();
        assert_eq!(inserted.previous(), None);
        assert_eq!(inserted.current(), &first);
        assert_eq!(
            next_event(&mut stream).await.unwrap(),
            PodEndpointStoreEvent::Upsert(first.clone())
        );

        let replacement = endpoint("uid-a", Ipv4Addr::new(10, 42, 0, 3), 2);
        let replaced = store.upsert_endpoint(replacement.clone()).await.unwrap();
        assert_eq!(replaced.previous(), Some(&first));
        assert_eq!(replaced.current(), &replacement);
        assert_eq!(
            next_event(&mut stream).await.unwrap(),
            PodEndpointStoreEvent::Delete {
                pod_ip: first.pod_ip(),
            }
        );
        assert_eq!(
            next_event(&mut stream).await.unwrap(),
            PodEndpointStoreEvent::Upsert(replacement.clone())
        );

        let deleted = store
            .delete_endpoint_for_uid(PodUidKey::try_new("uid-a").unwrap())
            .await
            .unwrap();
        assert_eq!(deleted.removed(), Some(&replacement));
        assert_eq!(
            next_event(&mut stream).await.unwrap(),
            PodEndpointStoreEvent::Delete {
                pod_ip: replacement.pod_ip(),
            }
        );
        assert!(
            store
                .delete_endpoint_for_uid(PodUidKey::try_new("uid-a").unwrap())
                .await
                .unwrap()
                .removed()
                .is_none()
        );
    }

    #[tokio::test]
    async fn endpoint_subscription_handoff_is_snapshot_then_mutation() {
        let store = store("sqlite:node-network-handoff").await;
        let handoff = store.pod_endpoint_handoff.clone().lock_owned().await;
        let mut subscribe = Box::pin(store.subscribe_endpoint_events());
        assert!(matches!(futures::poll!(subscribe.as_mut()), Poll::Pending));
        let row = endpoint("uid-a", Ipv4Addr::new(10, 42, 0, 2), 1);
        let mut upsert = Box::pin(store.upsert_endpoint(row.clone()));
        assert!(matches!(futures::poll!(upsert.as_mut()), Poll::Pending));

        drop(handoff);
        let mut events = subscribe.await.unwrap();
        assert_eq!(
            next_event(&mut events).await.unwrap(),
            PodEndpointStoreEvent::Resync(Vec::new())
        );
        upsert.await.unwrap();
        assert_eq!(
            next_event(&mut events).await.unwrap(),
            PodEndpointStoreEvent::Upsert(row)
        );
    }

    #[tokio::test]
    async fn malformed_endpoint_ports_are_corrupt_instead_of_wrapping() {
        let store = store("sqlite:node-network-corrupt-port").await;
        for (tcp, udp) in [
            (Some(65_536_i64), None),
            (None, Some(-1_i64)),
            (Some(0_i64), None),
        ] {
            store
                .db_call("test_insert_malformed_endpoint_port", move |conn| {
                    conn.execute("DELETE FROM pod_endpoints", [])?;
                    conn.execute(
                        "INSERT INTO pod_endpoints \
                         (pod_uid, namespace, pod_name, node_name, mode, pod_ip, node_ip, \
                          host_port_tcp, host_port_udp, generation, updated_ms) \
                         VALUES ('bad-port', 'default', 'bad-port', 'node-a', 'hostport', \
                                 '10.42.0.10', '192.0.2.10', ?1, ?2, 1, 1)",
                        rusqlite::params![tcp, udp],
                    )?;
                    Ok(())
                })
                .await
                .unwrap();
            let error = store
                .get_endpoint_by_pod_ip(Ipv4Addr::new(10, 42, 0, 10))
                .await
                .expect_err("invalid persisted port must fail decoding");
            assert!(
                error
                    .to_string()
                    .contains("pod endpoint port outside 1..=65535"),
                "unexpected decode error: {error}"
            );
        }
    }

    #[tokio::test]
    async fn corrupt_target_rolls_back_endpoint_upsert_and_delete_without_notification() {
        let store = store("sqlite:node-network-corrupt-target").await;
        let mut events = store.subscribe_endpoint_events().await.unwrap();
        assert_eq!(
            next_event(&mut events).await.unwrap(),
            PodEndpointStoreEvent::Resync(Vec::new())
        );
        store
            .db_call("test_insert_corrupt_target_endpoint", move |conn| {
                conn.execute(
                    "INSERT INTO pod_endpoints \
                     (pod_uid, namespace, pod_name, node_name, mode, pod_ip, node_ip, \
                      host_port_tcp, host_port_udp, generation, updated_ms) \
                     VALUES ('corrupt-target', 'default', 'corrupt-target', 'node-a', \
                             'hostport', '10.42.0.10', '192.0.2.10', 65536, NULL, 1, 1)",
                    [],
                )?;
                Ok(())
            })
            .await
            .unwrap();

        let replacement = endpoint("corrupt-target", Ipv4Addr::new(10, 42, 0, 11), 2);
        assert!(matches!(
            store.upsert_endpoint(replacement).await,
            Err(CacheNetworkError::CorruptData { .. })
        ));
        assert!(matches!(
            store
                .delete_endpoint_for_uid(PodUidKey::try_new("corrupt-target").unwrap())
                .await,
            Err(CacheNetworkError::CorruptData { .. })
        ));

        let persisted = store
            .db_call("test_read_corrupt_target_endpoint", move |conn| {
                conn.query_row(
                    "SELECT pod_ip, host_port_tcp FROM pod_endpoints \
                     WHERE pod_uid = 'corrupt-target'",
                    [],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<i64>>(1)?)),
                )
                .map_err(tokio_rusqlite::Error::from)
            })
            .await
            .unwrap();
        assert_eq!(persisted, ("10.42.0.10".to_string(), Some(65_536)));

        let mut next = Box::pin(std::future::poll_fn(|context| {
            events.as_mut().poll_next(context)
        }));
        assert!(matches!(futures::poll!(next.as_mut()), Poll::Pending));
    }
}
