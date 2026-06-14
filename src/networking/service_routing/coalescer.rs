// Native nftables service routing for klights.
//
// Owns the `inet <containerd_namespace>` table. All rule writes go through
// [`crate::networking::netfilter`] over a persistent netlink socket —
// no `iptables` / `nft` binary fork on the datapath.
//
// Table layout (mirrors the kube-proxy nftables-mode design from
// `pkg/proxy/nftables/proxier.go`):
//
// ```text
// table inet klights {
//     chain filter-forward {
//         type filter hook forward priority filter;
//         ct state invalid drop
//         ct state related,established accept
//         ip saddr <pod-subnet> accept
//         ip daddr <pod-subnet> accept
//     }
//
//     chain nat-postrouting {
//         type nat hook postrouting priority srcnat;
//         ct status dnat ip saddr <pod-subnet> ip daddr <pod-subnet> masquerade
//         ct status dnat ip saddr != <pod-subnet> ip daddr <cluster-cidr> snat to <pod-gateway>
//         ip saddr <pod-subnet> ip daddr != <cluster-cidr> masquerade
//     }
//
//     chain nat-prerouting {
//         type nat hook prerouting priority dstnat;
//         jump hostports
//         jump services
//     }
//
//     chain nat-output {
//         type nat hook output priority dstnat;
//         jump hostports
//         jump services
//     }
//
//     chain services {
//         # Populated by replace_services. One rule per service-port-endpoint:
//         #   ip daddr <vip> meta l4proto <proto> th dport <port> dnat to <ep>:<tport>
//         # Multi-endpoint uses a probability ladder via `meta random`:
//         #   ip daddr <vip> ... meta random < threshold dnat to <ep0>:<tport>
//         #   ip daddr <vip> ... meta random < threshold dnat to <ep1>:<tport>
//         #   ip daddr <vip> ... dnat to <ep_last>:<tport>     # catches rest
//         # NodePort drops the `ip daddr` match.
//     }
//
//     chain hostports {
//         # Populated incrementally as pods with hostPort declarations
//         # come and go. One DNAT rule per (pod_ip, host_port, container_port).
//     }
// }
// ```
//
// ## Public API for the rest of klights
//
// Top-level functions at the bottom of this file — [`init_service_chains`],
// [`sync_service_rules`], [`add_hostport_rules`], [`remove_hostport_rules`],
// [`remove_service_rules`], [`cleanup_service_chains`], [`get_host_ip`] —
// are the only surface API handlers and the kubelet should call. They wrap
// the persistent [`KlightsRuntime`] (one socket, one coalescer worker).

use super::prelude::*;
use super::*;
use crate::networking::service_router::ServiceRouter;
use async_trait::async_trait;

/// Default coalescing window for the services-sync worker. Matches
/// kube-proxy's `--iptables-min-sync-period 1s` default in spirit but
/// shorter — we choose 200ms because klights is single-node and
/// endpoint events arrive in tighter bursts (no controller-manager
/// queueing buffering them upstream). Not currently env-tunable.
const DEFAULT_MIN_SYNC_PERIOD: std::time::Duration = std::time::Duration::from_millis(200);

/// Initial backoff between retries after a failed sync. The worker
/// re-notifies itself after this delay so the next iteration retries
/// the sync. Doubles each consecutive failure up to `MAX_RETRY_BACKOFF`,
/// resets to this on success. Pure event-driven (no polling): the
/// retry is delivered as a self-Notify, not a wakeup timer.
const INITIAL_RETRY_BACKOFF: std::time::Duration = std::time::Duration::from_secs(1);

/// Cap on retry backoff. Prevents pathological exponential growth
/// from leaving the chain stale forever — at most one retry attempt
/// per minute under sustained failure.
const MAX_RETRY_BACKOFF: std::time::Duration = std::time::Duration::from_secs(60);

pub struct NftServiceRouterStores {
    pub cluster_api: std::sync::Arc<dyn LeaderApiClient>,
    pub node_local: NodeLocalHandle,
}

impl NftServiceRouterStores {
    pub fn new(
        cluster_api: std::sync::Arc<dyn LeaderApiClient>,
        node_local: NodeLocalHandle,
    ) -> Self {
        Self {
            cluster_api,
            node_local,
        }
    }
}

pub struct NftServiceRouterTableConfig<'a> {
    pub local_node_name: &'a str,
    pub table_name: &'a str,
    pub bridge_ifname: &'a str,
}

impl<'a> NftServiceRouterTableConfig<'a> {
    pub fn new(local_node_name: &'a str, table_name: &'a str, bridge_ifname: &'a str) -> Self {
        Self {
            local_node_name,
            table_name,
            bridge_ifname,
        }
    }
}

pub struct NftServiceRouterNetworkConfig {
    pub pod_subnet: PodSubnet,
    pub cluster_cidr: ClusterCidr,
    pub service_cidr: ClusterCidr,
    pub mode: ServiceRoutingMode,
}

impl NftServiceRouterNetworkConfig {
    pub fn new(
        pod_subnet: PodSubnet,
        cluster_cidr: ClusterCidr,
        service_cidr: ClusterCidr,
        mode: ServiceRoutingMode,
    ) -> Self {
        Self {
            pod_subnet,
            cluster_cidr,
            service_cidr,
            mode,
        }
    }
}

pub struct NftServiceRouterRuntime {
    pub min_sync_period: std::time::Duration,
    pub cancel: CancellationToken,
    pub task_supervisor: std::sync::Arc<crate::task_supervisor::TaskSupervisor>,
}

impl NftServiceRouterRuntime {
    pub fn new(
        min_sync_period: std::time::Duration,
        cancel: CancellationToken,
        task_supervisor: std::sync::Arc<crate::task_supervisor::TaskSupervisor>,
    ) -> Self {
        Self {
            min_sync_period,
            cancel,
            task_supervisor,
        }
    }

    pub fn default_window(
        cancel: CancellationToken,
        task_supervisor: std::sync::Arc<crate::task_supervisor::TaskSupervisor>,
    ) -> Self {
        Self::new(DEFAULT_MIN_SYNC_PERIOD, cancel, task_supervisor)
    }
}

pub struct NftServiceRouterBoot<'a> {
    pub stores: NftServiceRouterStores,
    pub table: NftServiceRouterTableConfig<'a>,
    pub network: NftServiceRouterNetworkConfig,
    pub runtime: NftServiceRouterRuntime,
}

impl<'a> NftServiceRouterBoot<'a> {
    pub fn new(
        stores: NftServiceRouterStores,
        table: NftServiceRouterTableConfig<'a>,
        network: NftServiceRouterNetworkConfig,
        runtime: NftServiceRouterRuntime,
    ) -> Self {
        Self {
            stores,
            table,
            network,
            runtime,
        }
    }
}

pub struct NftServiceRouterDefaultBoot<'a> {
    pub stores: NftServiceRouterStores,
    pub table: NftServiceRouterTableConfig<'a>,
    pub network: NftServiceRouterNetworkConfig,
    pub cancel: CancellationToken,
    pub task_supervisor: std::sync::Arc<crate::task_supervisor::TaskSupervisor>,
}

impl<'a> NftServiceRouterDefaultBoot<'a> {
    pub fn new(
        stores: NftServiceRouterStores,
        table: NftServiceRouterTableConfig<'a>,
        network: NftServiceRouterNetworkConfig,
        cancel: CancellationToken,
        task_supervisor: std::sync::Arc<crate::task_supervisor::TaskSupervisor>,
    ) -> Self {
        Self {
            stores,
            table,
            network,
            cancel,
            task_supervisor,
        }
    }

    fn into_boot(self) -> NftServiceRouterBoot<'a> {
        NftServiceRouterBoot::new(
            self.stores,
            self.table,
            self.network,
            NftServiceRouterRuntime::default_window(self.cancel, self.task_supervisor),
        )
    }
}

/// App-owned implementation of the [`ServiceRouter`] trait — owns the
/// persistent netlink socket via [`KlightsTable`], the spawned
/// coalescer worker, and the per-instance hostport state. One instance
/// per process; instantiated by [`NftServiceRouter::boot`] during
/// bootstrap and held as `Arc<dyn ServiceRouter>` on AppState.
///
/// The coalescer is a `tokio::sync::Notify`-driven loop. Callers signal
/// "services need re-sync" via `request_services_sync`; the worker
/// waits on the Notify, then sleeps `min_sync_period`, then runs ONE
/// `sync_services_from_db` covering all collapsed events.
/// `Notify::notify_one` is idempotent if no waiter, so N rapid calls
/// between two ticks coalesce into one sync.
///
/// Hostport mutations are NOT coalesced — they're per-pod-create/
/// delete and use the [`KlightsTable`]'s direct add/remove methods
/// (which serialize per-table via the table's instance-owned hostport
/// lock).
pub struct NftServiceRouter {
    table: std::sync::Arc<KlightsTable>,
    cluster_api: std::sync::Arc<dyn LeaderApiClient>,
    notify: std::sync::Arc<tokio::sync::Notify>,
    /// Cancellation token observed by the coalescer worker. Cancelled
    /// by `cleanup` so the worker exits its `tokio::select!` arms
    /// cleanly instead of being aborted mid-batch.
    cancel: CancellationToken,
    /// JoinHandle for the spawned coalescer worker. `Mutex<Option<_>>`
    /// so `cleanup` can `take()` ownership and `.await` the handle
    /// without holding `&mut self`.
    worker: tokio::sync::Mutex<Option<crate::task_supervisor::SupervisedJoinHandle<()>>>,
    service_watch_worker:
        tokio::sync::Mutex<Option<crate::task_supervisor::SupervisedJoinHandle<()>>>,
    remote_endpoint_worker:
        tokio::sync::Mutex<Option<crate::task_supervisor::SupervisedJoinHandle<()>>>,
    /// Used by `cleanup` to construct a fresh netlink socket when the
    /// runtime is being torn down. Stored on the struct so callers
    /// don't have to re-thread the supervisor.
    task_supervisor: std::sync::Arc<crate::task_supervisor::TaskSupervisor>,
    table_name_str: String,
}

impl NftServiceRouter {
    /// Build the router: open one persistent netlink socket, construct
    /// the table, run `init()` on it, spawn the coalescer worker, and
    /// return the constructed instance ready to be erased into
    /// `Arc<dyn ServiceRouter>`.
    pub async fn boot(request: NftServiceRouterBoot<'_>) -> Result<std::sync::Arc<Self>> {
        let NftServiceRouterBoot {
            stores,
            table: table_config,
            network,
            runtime,
        } = request;
        let NftServiceRouterStores {
            cluster_api,
            node_local,
        } = stores;
        let NftServiceRouterTableConfig {
            local_node_name,
            table_name,
            bridge_ifname,
        } = table_config;
        let NftServiceRouterNetworkConfig {
            pod_subnet,
            cluster_cidr,
            service_cidr,
            mode,
        } = network;
        let NftServiceRouterRuntime {
            min_sync_period,
            cancel,
            task_supervisor,
        } = runtime;

        let nf =
            Netfilter::new(task_supervisor.clone()).context("open persistent netlink socket")?;
        let table = std::sync::Arc::new(
            KlightsTable::with_name_and_bridge(
                nf,
                table_name,
                bridge_ifname,
                pod_subnet,
                cluster_cidr,
                service_cidr,
                mode.clone(),
            )
            .context("construct KlightsTable")?,
        );
        table.init().await.context("init klights table chains")?;
        table
            .sync_remote_pod_endpoints_from_node_local(node_local.as_ref(), local_node_name)
            .await
            .context("initial remote pod endpoint DNAT sync")?;

        let notify = std::sync::Arc::new(tokio::sync::Notify::new());
        let worker_table = table.clone();
        let worker_notify = notify.clone();
        let worker_cancel = cancel.clone();
        let worker_task_supervisor = task_supervisor.clone();
        let worker_cluster_api = cluster_api.clone();
        let worker = task_supervisor
            .spawn_async(
                crate::task_supervisor::TaskCategory::Network,
                "service_routing_coalescer_worker",
                async move {
                    tracing::info!(
                        "nft services-sync coalescer started (min_sync_period={:?})",
                        min_sync_period
                    );
                    // Tracks the current retry backoff. Resets to
                    // INITIAL_RETRY_BACKOFF on every successful sync; doubles
                    // up to MAX_RETRY_BACKOFF on consecutive failures.
                    let mut backoff = INITIAL_RETRY_BACKOFF;
                    loop {
                        // Wait for either a sync request or shutdown.
                        tokio::select! {
                            _ = worker_cancel.cancelled() => break,
                            _ = worker_notify.notified() => {}
                        }
                        // Coalesce: brief sleep collapses bursty notifies into
                        // one sync. Cancellable so SIGTERM never waits the full
                        // min_sync_period before exiting.
                        tokio::select! {
                            _ = worker_cancel.cancelled() => break,
                            _ = worker_task_supervisor
                                .sleep("service_routing_coalescer_min_sync_period", min_sync_period) => {}
                        }
                        match worker_table.sync_services_from_api(worker_cluster_api.as_ref()).await {
                            Ok(_n) => {
                                backoff = INITIAL_RETRY_BACKOFF;
                            }
                            Err(e) => {
                                let next_backoff =
                                    std::cmp::min(backoff.saturating_mul(2), MAX_RETRY_BACKOFF);
                                tracing::warn!(
                                    "coalesced services sync failed (retry in {:?}, next backoff {:?}): {e}",
                                    backoff,
                                    next_backoff,
                                );
                                // Inline the retry delay through the supervisor.
                                // Cancellable so a stuck retry can't delay
                                // shutdown. Re-arm the notify so the next loop
                                // iteration picks up the retry.
                                tokio::select! {
                                    _ = worker_cancel.cancelled() => break,
                                    _ = worker_task_supervisor
                                        .sleep("service_routing_coalescer_retry_backoff", backoff) => {}
                                }
                                backoff = next_backoff;
                                worker_notify.notify_one();
                            }
                        }
                    }
                    tracing::info!("nft services-sync coalescer exited");
                },
            )
            .await
            .context("failed to spawn service routing coalescer worker")?;

        let remote_table = table.clone();
        let remote_node_local = node_local.clone();
        let remote_cancel = cancel.clone();
        let remote_local_node = local_node_name.to_string();
        let service_watch_notify = notify.clone();
        let service_watch_cancel = cancel.clone();
        let service_watch_cluster_api = cluster_api.clone();
        let service_watch_task_supervisor = task_supervisor.clone();
        let service_watch_worker = task_supervisor
            .spawn_async(
                crate::task_supervisor::TaskCategory::Network,
                "service_routing_watch_worker",
                async move {
                    run_service_routing_watch_worker(
                        service_watch_cluster_api,
                        service_watch_notify,
                        service_watch_cancel,
                        service_watch_task_supervisor,
                    )
                    .await;
                },
            )
            .await
            .context("failed to spawn service routing watch worker")?;
        let remote_endpoint_worker = task_supervisor
            .spawn_async(
                crate::task_supervisor::TaskCategory::Network,
                "service_routing_remote_pod_endpoint_worker",
                async move {
                    run_remote_pod_endpoint_worker(
                        remote_table,
                        remote_node_local,
                        remote_local_node,
                        remote_cancel,
                    )
                    .await;
                },
            )
            .await
            .context("failed to spawn remote pod endpoint worker")?;

        Ok(std::sync::Arc::new(Self {
            table,
            cluster_api,
            notify,
            cancel,
            worker: tokio::sync::Mutex::new(Some(worker)),
            service_watch_worker: tokio::sync::Mutex::new(Some(service_watch_worker)),
            remote_endpoint_worker: tokio::sync::Mutex::new(Some(remote_endpoint_worker)),
            task_supervisor,
            table_name_str: table_name.to_string(),
        }))
    }

    /// Boot using the default coalescing window. Convenience wrapper
    /// for the bootstrap path; tests that need a custom window call
    /// `boot` directly.
    pub async fn boot_with_defaults(
        request: NftServiceRouterDefaultBoot<'_>,
    ) -> Result<std::sync::Arc<Self>> {
        let table_name = request.table.table_name.to_string();
        let pod_subnet = request.network.pod_subnet.to_string();
        ensure_service_routing_sysctls(&request.task_supervisor).await?;
        let rt = Self::boot(request.into_boot()).await.with_context(|| {
            format!("boot NftServiceRouter for inet {table_name} (pod subnet {pod_subnet})")
        })?;
        tracing::info!(
            "Initialized nft runtime (table: inet {table_name}, pod subnet: {pod_subnet})"
        );
        Ok(rt)
    }
}

#[async_trait]
impl ServiceRouter for NftServiceRouter {
    fn request_services_sync(&self) {
        self.notify.notify_one();
    }

    async fn sync_services_now(&self) -> Result<()> {
        self.table
            .sync_services_from_api(self.cluster_api.as_ref())
            .await
            .context("sync_services_now: rebuild services chain")?;
        Ok(())
    }

    async fn add_hostport_rules(&self, pod: &serde_json::Value, pod_ip: Ipv4Addr) -> Result<()> {
        let specs = HostPortSpec::from_pod(pod);
        if specs.is_empty() {
            return Ok(());
        }
        self.table.add_hostports_for_pod(pod_ip, specs).await
    }

    async fn remove_hostport_rules(&self, pod: &serde_json::Value) -> Result<()> {
        let specs = HostPortSpec::from_pod(pod);
        if specs.is_empty() {
            return Ok(());
        }
        let pod_ip = match pod.pointer("/status/podIP").and_then(|v| v.as_str()) {
            Some(ip) if !ip.is_empty() => ip,
            _ => return Ok(()),
        };
        let pod_ip = Ipv4Addr::from_str(pod_ip).context("parse pod IP")?;
        self.table.remove_hostports_for_pod(pod_ip).await
    }

    async fn cleanup(&self) -> Result<()> {
        // 1. Stop the coalescer worker so it isn't mid-batch when the
        //    table is dropped.
        self.cancel.cancel();
        let handle = self.worker.lock().await.take();
        if let Some(h) = handle
            && let Err(e) = h.join().await
        {
            tracing::warn!("coalescer worker join failed: {e}");
        }
        let service_watch_handle = self.service_watch_worker.lock().await.take();
        if let Some(h) = service_watch_handle
            && let Err(e) = h.join().await
        {
            tracing::warn!("service routing watch worker join failed: {e}");
        }
        let remote_handle = self.remote_endpoint_worker.lock().await.take();
        if let Some(h) = remote_handle
            && let Err(e) = h.join().await
        {
            tracing::warn!("remote pod endpoint worker join failed: {e}");
        }

        // 2. Drop the `inet <table>` table on a fresh netlink socket.
        //    Best-effort — missing tables are tolerated.
        let nf = Netfilter::new(self.task_supervisor.clone())
            .context("open netlink socket for cleanup")?;
        let placeholder_pod = PodSubnet::parse("0.0.0.0/30").expect("static placeholder");
        let placeholder_cluster = ClusterCidr::parse("0.0.0.0/0").expect("static placeholder");
        let placeholder_service = ClusterCidr::parse("0.0.0.0/0").expect("static placeholder");
        // Cleanup only deletes the table; the mode field never drives kernel
        // calls on the cleanup path, so a default placeholder is safe here.
        let placeholder_mode =
            ServiceRoutingMode::new(crate::bootstrap::NodeMode::Root, "klights.vxlan");
        let table = KlightsTable::with_name_and_bridge(
            nf,
            &self.table_name_str,
            &self.table_name_str,
            placeholder_pod,
            placeholder_cluster,
            placeholder_service,
            placeholder_mode,
        )
        .context("construct KlightsTable for cleanup")?;
        table.cleanup().await
    }
}

#[derive(Clone, Copy, Debug)]
struct ServiceRoutingWatchTarget {
    api_version: &'static str,
    kind: &'static str,
}

impl ServiceRoutingWatchTarget {
    fn request(self) -> crate::control_plane::client::WatchRequest {
        crate::control_plane::client::WatchRequest {
            api_version: self.api_version.to_string(),
            kind: self.kind.to_string(),
            namespace: None,
            label_selector: None,
            field_selector: None,
            start_resource_version: None,
        }
    }
}

const SERVICE_ROUTING_WATCH_TARGETS: [ServiceRoutingWatchTarget; 3] = [
    ServiceRoutingWatchTarget {
        api_version: "v1",
        kind: "Service",
    },
    ServiceRoutingWatchTarget {
        api_version: "v1",
        kind: "Endpoints",
    },
    ServiceRoutingWatchTarget {
        api_version: "discovery.k8s.io/v1",
        kind: "EndpointSlice",
    },
];

enum ServiceRoutingWatchItem {
    Event {
        target: ServiceRoutingWatchTarget,
        event: anyhow::Result<crate::control_plane::client::ResourceEvent>,
    },
    Closed {
        target: ServiceRoutingWatchTarget,
    },
}

type ServiceRoutingWatchStream =
    std::pin::Pin<Box<dyn futures::Stream<Item = ServiceRoutingWatchItem> + Send>>;

fn wrap_service_routing_watch_stream(
    target: ServiceRoutingWatchTarget,
    mut stream: crate::control_plane::client::WatchStream<
        crate::control_plane::client::ResourceEvent,
    >,
) -> ServiceRoutingWatchStream {
    use futures::StreamExt;

    Box::pin(async_stream::stream! {
        while let Some(event) = stream.next().await {
            yield ServiceRoutingWatchItem::Event { target, event };
        }
        yield ServiceRoutingWatchItem::Closed { target };
    })
}

async fn open_service_routing_watch_set(
    cluster_api: &std::sync::Arc<dyn LeaderApiClient>,
) -> Result<futures::stream::SelectAll<ServiceRoutingWatchStream>> {
    let mut streams = futures::stream::SelectAll::new();
    for target in SERVICE_ROUTING_WATCH_TARGETS {
        let stream = cluster_api
            .watch_resources(target.request())
            .await
            .with_context(|| {
                format!(
                    "open service routing watch for {}/{}",
                    target.api_version, target.kind
                )
            })?;
        streams.push(wrap_service_routing_watch_stream(target, stream));
    }
    Ok(streams)
}

async fn service_routing_watch_reconnect_delay(
    task_supervisor: &std::sync::Arc<crate::task_supervisor::TaskSupervisor>,
    cancel: &CancellationToken,
    attempt: u32,
) -> bool {
    tokio::select! {
        _ = cancel.cancelled() => false,
        result = task_supervisor.sleep(
            "service_routing_watch_reconnect_backoff",
            crate::utils::watch_reconnect_delay(attempt),
        ) => {
            if let Err(err) = result {
                tracing::warn!(
                    error = %err,
                    "service routing watch reconnect timer failed; stopping watch worker"
                );
                return false;
            }
            true
        }
    }
}

async fn run_service_routing_watch_worker(
    cluster_api: std::sync::Arc<dyn LeaderApiClient>,
    notify: std::sync::Arc<tokio::sync::Notify>,
    cancel: CancellationToken,
    task_supervisor: std::sync::Arc<crate::task_supervisor::TaskSupervisor>,
) {
    use futures::StreamExt;

    tracing::info!("nft service routing watch worker started");

    // Consecutive failed reconnects; reset to 0 once a watch event is received.
    // Drives the shared exponential reconnect backoff (500ms→60s).
    let mut reconnect_attempt: u32 = 0;
    loop {
        let mut streams = match open_service_routing_watch_set(&cluster_api).await {
            Ok(streams) => streams,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "service routing failed to open cluster watches; scheduling full service sync"
                );
                notify.notify_one();
                if !service_routing_watch_reconnect_delay(
                    &task_supervisor,
                    &cancel,
                    reconnect_attempt,
                )
                .await
                {
                    break;
                }
                reconnect_attempt = reconnect_attempt.saturating_add(1);
                continue;
            }
        };

        // The watches above only deliver events observed after each stream is
        // opened. A full sync after the watch set is established closes the
        // bootstrap race where an existing Service, such as kube-dns, gains
        // ready Endpoints before this node's watch streams are active.
        notify.notify_one();

        let mut reopen_watch_set = false;
        while !reopen_watch_set {
            tokio::select! {
                _ = cancel.cancelled() => {
                    tracing::info!("nft service routing watch worker exited");
                    return;
                }
                event = streams.next() => {
                    match event {
                        Some(ServiceRoutingWatchItem::Event { event: Ok(_), .. }) => {
                            reconnect_attempt = 0;
                            notify.notify_one();
                        }
                        Some(ServiceRoutingWatchItem::Event { target, event: Err(err) }) => {
                            tracing::warn!(
                                api_version = target.api_version,
                                kind = target.kind,
                                error = %err,
                                "service routing watch stream failed; scheduling full service sync"
                            );
                            notify.notify_one();
                            reopen_watch_set = true;
                        }
                        Some(ServiceRoutingWatchItem::Closed { target }) => {
                            tracing::warn!(
                                api_version = target.api_version,
                                kind = target.kind,
                                "service routing watch stream closed; scheduling full service sync"
                            );
                            notify.notify_one();
                            reopen_watch_set = true;
                        }
                        None => {
                            tracing::warn!(
                                "service routing watch set closed; scheduling full service sync"
                            );
                            notify.notify_one();
                            reopen_watch_set = true;
                        }
                    }
                }
            }
        }

        if !service_routing_watch_reconnect_delay(&task_supervisor, &cancel, reconnect_attempt)
            .await
        {
            break;
        }
        reconnect_attempt = reconnect_attempt.saturating_add(1);
    }
    tracing::info!("nft service routing watch worker exited");
}

async fn run_remote_pod_endpoint_worker(
    table: std::sync::Arc<KlightsTable>,
    node_local: NodeLocalHandle,
    local_node_name: String,
    cancel: CancellationToken,
) {
    tracing::info!("nft remote pod endpoint worker started");
    let mut rx = node_local.subscribe_pod_endpoints();
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            event = rx.recv() => {
                match event {
                    Ok(crate::datastore::PodEndpointEvent::Upsert(row)) => {
                        if let Err(e) = table
                            .upsert_remote_pod_endpoint_row(&local_node_name, row)
                            .await
                        {
                            tracing::warn!("remote pod endpoint upsert sync failed: {e:#}");
                        }
                    }
                    Ok(crate::datastore::PodEndpointEvent::Delete { pod_ip, .. }) => {
                        if let Err(e) = table.remove_remote_pod_endpoint(pod_ip).await {
                            tracing::warn!("remote pod endpoint delete sync failed: {e:#}");
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(
                            skipped,
                            "remote pod endpoint worker lagged; rebuilding from datastore"
                        );
                        if let Err(e) = table
                            .sync_remote_pod_endpoints_from_node_local(
                                node_local.as_ref(),
                                &local_node_name,
                            )
                            .await
                        {
                            tracing::warn!("remote pod endpoint lag rebuild failed: {e:#}");
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }
    tracing::info!("nft remote pod endpoint worker exited");
}

async fn write_proc_sysctl(path: &str, value: &str) -> Result<()> {
    crate::utils::write_file_async(path, value)
        .await
        .with_context(|| format!("write sysctl {path}={}", value.trim_end()))
}

/// Sysctls klights must enable for native service routing and hostPort DNAT.
///
/// `route_localnet` is required so hostPort/NodePort DNAT works when the
/// destination is a loopback address. The hostPort conformance test
/// (`[sig-network] HostPort ... different hostIP and protocol`) curls
/// `127.0.0.1:<hostPort>` from a host-network pod while binding the source
/// to the node IP — this only routes if `net.ipv4.conf.all.route_localnet`
/// is enabled (the "route_localnet kernel hack" the test comment references
/// and which kube-proxy sets for the same reason). A freshly provisioned
/// node has it disabled by default, so klights must set it on every node.
const REQUIRED_SERVICE_ROUTING_SYSCTLS: &[(&str, &str)] = &[
    ("/proc/sys/net/ipv4/ip_forward", "1\n"),
    ("/proc/sys/net/bridge/bridge-nf-call-iptables", "1\n"),
    ("/proc/sys/net/ipv4/conf/all/route_localnet", "1\n"),
];

async fn ensure_service_routing_sysctls(
    task_supervisor: &std::sync::Arc<crate::task_supervisor::TaskSupervisor>,
) -> Result<()> {
    let modprobe = task_supervisor
        .run_blocking_file("service_routing_modprobe_br_netfilter", || {
            std::process::Command::new("modprobe")
                .arg("br_netfilter")
                .status()
        })
        .await
        .context("supervised modprobe br_netfilter task failed")?;

    match modprobe {
        Ok(status) if !status.success() => {
            tracing::warn!("modprobe br_netfilter exited with status {}", status);
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            tracing::warn!(
                "modprobe not found while enabling br_netfilter; continuing to sysctl read-back"
            );
        }
        Err(e) => return Err(e).context("run modprobe br_netfilter"),
        _ => {}
    }

    for (path, value) in REQUIRED_SERVICE_ROUTING_SYSCTLS {
        ensure_sysctl_value(path, value).await?;
    }
    Ok(())
}

async fn ensure_sysctl_value(path: &str, expected: &str) -> Result<()> {
    write_proc_sysctl(path, expected).await?;
    let actual = crate::utils::read_utf8_file_async(path)
        .await
        .with_context(|| format!("read sysctl {path}"))?;
    if actual != expected {
        let actual_trimmed = actual.trim_end();
        let expected_trimmed = expected.trim_end();
        tracing::error!(
            "service routing sysctl verification failed: {} expected {} got {}",
            path,
            expected_trimmed,
            actual_trimmed
        );
        anyhow::bail!(
            "sysctl verification failed for {path}: expected {expected_trimmed}, got {actual_trimmed}"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_plane::client::{
        CacheScope, LeaderApiClient, ListRequest, ListResponse, ResourceEvent, ResourceKey,
        WatchRequest, WatchStream,
    };
    use crate::datastore::{NodeSubnet, Resource};
    use crate::kubelet::outbox::payload::OutboxOperation;
    use crate::kubelet::outbox::{OutboxApplyError, OutboxApplyResult};
    use crate::networking::wireguard::DataplanePeerMetadata;
    use anyhow::{Result, anyhow};
    use async_trait::async_trait;
    use bytes::Bytes;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::sync::Notify;
    use tokio_util::sync::CancellationToken;

    #[derive(Default)]
    struct WatchOnlyLeaderApiClient {
        watches_opened: AtomicUsize,
    }

    #[derive(Default)]
    struct ReopeningLeaderApiClient {
        watches_opened: AtomicUsize,
        opened_notify: Notify,
        closed_endpoints_once: std::sync::atomic::AtomicBool,
    }

    impl ReopeningLeaderApiClient {
        async fn wait_for_opened(&self, expected: usize) {
            loop {
                if self.watches_opened.load(Ordering::SeqCst) >= expected {
                    return;
                }
                self.opened_notify.notified().await;
            }
        }
    }

    #[async_trait]
    impl LeaderApiClient for WatchOnlyLeaderApiClient {
        async fn get_resource(&self, key: ResourceKey) -> Result<Option<Resource>> {
            Err(anyhow!("unexpected get_resource for {key:?}"))
        }

        async fn list_resources(&self, req: ListRequest) -> Result<ListResponse> {
            Err(anyhow!("unexpected list_resources for {req:?}"))
        }

        async fn watch_resources(&self, _req: WatchRequest) -> Result<WatchStream<ResourceEvent>> {
            self.watches_opened.fetch_add(1, Ordering::SeqCst);
            Ok(Box::pin(futures::stream::pending()))
        }

        async fn wait_cache_ready(&self, scope: CacheScope) -> Result<()> {
            Err(anyhow!("unexpected wait_cache_ready for {scope:?}"))
        }

        async fn get_pod(&self, ns: &str, name: &str) -> Result<Option<Resource>> {
            Err(anyhow!("unexpected get_pod for {ns}/{name}"))
        }

        async fn get_pod_for_uid(
            &self,
            ns: &str,
            name: &str,
            uid: &str,
        ) -> Result<Option<Resource>> {
            Err(anyhow!("unexpected get_pod_for_uid for {ns}/{name}/{uid}"))
        }

        async fn watch_pods_on_node(&self, node_name: &str) -> Result<WatchStream<Resource>> {
            Err(anyhow!("unexpected watch_pods_on_node for {node_name}"))
        }

        async fn list_pods_on_node(&self, node_name: &str) -> Result<Vec<Resource>> {
            Err(anyhow!("unexpected list_pods_on_node for {node_name}"))
        }

        async fn get_configmap(&self, ns: &str, name: &str) -> Result<Option<Resource>> {
            Err(anyhow!("unexpected get_configmap for {ns}/{name}"))
        }

        async fn get_secret(&self, ns: &str, name: &str) -> Result<Option<Resource>> {
            Err(anyhow!("unexpected get_secret for {ns}/{name}"))
        }

        async fn get_node(&self, name: &str) -> Result<Resource> {
            Err(anyhow!("unexpected get_node for {name}"))
        }

        async fn watch_node(&self, name: &str) -> Result<WatchStream<Resource>> {
            Err(anyhow!("unexpected watch_node for {name}"))
        }

        async fn allocate_node_subnet(
            &self,
            node_name: &str,
            _cluster_cidr: &str,
            _node_ip: &str,
        ) -> Result<NodeSubnet> {
            Err(anyhow!("unexpected allocate_node_subnet for {node_name}"))
        }

        async fn get_node_subnet(&self, node_name: &str) -> Result<Option<NodeSubnet>> {
            Err(anyhow!("unexpected get_node_subnet for {node_name}"))
        }

        async fn list_peer_subnets(&self, my_node_name: &str) -> Result<Vec<NodeSubnet>> {
            Err(anyhow!("unexpected list_peer_subnets for {my_node_name}"))
        }

        async fn get_node_dataplane(
            &self,
            node_name: &str,
        ) -> Result<Option<DataplanePeerMetadata>> {
            Err(anyhow!("unexpected get_node_dataplane for {node_name}"))
        }

        async fn apply_outbox(
            &self,
            idempotency_key: &str,
            _operation: OutboxOperation,
            _payload: Bytes,
        ) -> std::result::Result<OutboxApplyResult, OutboxApplyError> {
            Err(OutboxApplyError::Retryable(format!(
                "unexpected apply_outbox for {idempotency_key}"
            )))
        }
    }

    #[async_trait]
    impl LeaderApiClient for ReopeningLeaderApiClient {
        async fn get_resource(&self, key: ResourceKey) -> Result<Option<Resource>> {
            Err(anyhow!("unexpected get_resource for {key:?}"))
        }

        async fn list_resources(&self, req: ListRequest) -> Result<ListResponse> {
            Err(anyhow!("unexpected list_resources for {req:?}"))
        }

        async fn watch_resources(&self, req: WatchRequest) -> Result<WatchStream<ResourceEvent>> {
            self.watches_opened.fetch_add(1, Ordering::SeqCst);
            self.opened_notify.notify_waiters();
            if req.kind == "Endpoints"
                && !self
                    .closed_endpoints_once
                    .swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                return Ok(Box::pin(futures::stream::empty()));
            }
            Ok(Box::pin(futures::stream::pending()))
        }

        async fn wait_cache_ready(&self, scope: CacheScope) -> Result<()> {
            Err(anyhow!("unexpected wait_cache_ready for {scope:?}"))
        }

        async fn get_pod(&self, ns: &str, name: &str) -> Result<Option<Resource>> {
            Err(anyhow!("unexpected get_pod for {ns}/{name}"))
        }

        async fn get_pod_for_uid(
            &self,
            ns: &str,
            name: &str,
            uid: &str,
        ) -> Result<Option<Resource>> {
            Err(anyhow!("unexpected get_pod_for_uid for {ns}/{name}/{uid}"))
        }

        async fn watch_pods_on_node(&self, node_name: &str) -> Result<WatchStream<Resource>> {
            Err(anyhow!("unexpected watch_pods_on_node for {node_name}"))
        }

        async fn list_pods_on_node(&self, node_name: &str) -> Result<Vec<Resource>> {
            Err(anyhow!("unexpected list_pods_on_node for {node_name}"))
        }

        async fn get_configmap(&self, ns: &str, name: &str) -> Result<Option<Resource>> {
            Err(anyhow!("unexpected get_configmap for {ns}/{name}"))
        }

        async fn get_secret(&self, ns: &str, name: &str) -> Result<Option<Resource>> {
            Err(anyhow!("unexpected get_secret for {ns}/{name}"))
        }

        async fn get_node(&self, name: &str) -> Result<Resource> {
            Err(anyhow!("unexpected get_node for {name}"))
        }

        async fn watch_node(&self, name: &str) -> Result<WatchStream<Resource>> {
            Err(anyhow!("unexpected watch_node for {name}"))
        }

        async fn allocate_node_subnet(
            &self,
            node_name: &str,
            _cluster_cidr: &str,
            _node_ip: &str,
        ) -> Result<NodeSubnet> {
            Err(anyhow!("unexpected allocate_node_subnet for {node_name}"))
        }

        async fn get_node_subnet(&self, node_name: &str) -> Result<Option<NodeSubnet>> {
            Err(anyhow!("unexpected get_node_subnet for {node_name}"))
        }

        async fn list_peer_subnets(&self, my_node_name: &str) -> Result<Vec<NodeSubnet>> {
            Err(anyhow!("unexpected list_peer_subnets for {my_node_name}"))
        }

        async fn get_node_dataplane(
            &self,
            node_name: &str,
        ) -> Result<Option<DataplanePeerMetadata>> {
            Err(anyhow!("unexpected get_node_dataplane for {node_name}"))
        }

        async fn apply_outbox(
            &self,
            idempotency_key: &str,
            _operation: OutboxOperation,
            _payload: Bytes,
        ) -> std::result::Result<OutboxApplyResult, OutboxApplyError> {
            Err(OutboxApplyError::Retryable(format!(
                "unexpected apply_outbox for {idempotency_key}"
            )))
        }
    }

    #[tokio::test]
    async fn service_routing_watch_worker_requests_full_sync_after_watches_open() {
        let client = Arc::new(WatchOnlyLeaderApiClient::default());
        let notify = Arc::new(Notify::new());
        let cancel = CancellationToken::new();
        let supervisor = Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ));

        let worker = supervisor
            .spawn_async(
                crate::task_supervisor::TaskCategory::Network,
                "test_service_routing_watch_worker",
                run_service_routing_watch_worker(
                    client.clone(),
                    notify.clone(),
                    cancel.clone(),
                    supervisor.clone(),
                ),
            )
            .await
            .expect("spawn watch worker under task supervisor");

        tokio::time::timeout(std::time::Duration::from_millis(100), notify.notified())
            .await
            .expect("watch worker must request an initial full service sync");

        cancel.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(1), worker.join())
            .await
            .expect("watch worker must exit after cancellation")
            .expect("watch worker task must not panic");

        assert_eq!(
            client.watches_opened.load(Ordering::SeqCst),
            3,
            "service routing must open Service, Endpoints, and EndpointSlice watches"
        );
    }

    #[tokio::test]
    async fn service_routing_watch_worker_reopens_when_one_watch_stream_closes() {
        let client = Arc::new(ReopeningLeaderApiClient::default());
        let notify = Arc::new(Notify::new());
        let cancel = CancellationToken::new();
        let supervisor = Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ));

        let worker = supervisor
            .spawn_async(
                crate::task_supervisor::TaskCategory::Network,
                "test_service_routing_watch_worker_reopen",
                run_service_routing_watch_worker(
                    client.clone(),
                    notify.clone(),
                    cancel.clone(),
                    supervisor.clone(),
                ),
            )
            .await
            .expect("spawn watch worker under task supervisor");

        tokio::time::timeout(std::time::Duration::from_secs(1), client.wait_for_opened(3))
            .await
            .expect("watch worker must open the initial watch set");
        tokio::time::timeout(std::time::Duration::from_millis(100), notify.notified())
            .await
            .expect("initial watch set must request a full service sync");

        tokio::time::timeout(std::time::Duration::from_secs(1), client.wait_for_opened(6))
            .await
            .expect("watch worker must reopen all watches after one stream closes");

        cancel.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(1), worker.join())
            .await
            .expect("watch worker must exit after cancellation")
            .expect("watch worker task must not panic");
    }

    // hostPort/NodePort DNAT to a loopback destination (the e2e hostPort
    // conformance test curls 127.0.0.1 from a host-network pod) only routes
    // when net.ipv4.conf.all.route_localnet is enabled. klights must set it
    // on every node, exactly as kube-proxy does.
    #[test]
    fn required_sysctls_enable_route_localnet() {
        assert!(
            REQUIRED_SERVICE_ROUTING_SYSCTLS
                .iter()
                .any(
                    |(path, value)| *path == "/proc/sys/net/ipv4/conf/all/route_localnet"
                        && *value == "1\n"
                ),
            "service routing must enable route_localnet for loopback hostPort DNAT; got {REQUIRED_SERVICE_ROUTING_SYSCTLS:?}"
        );
    }
}
