// Native nftables service routing for klights.
//
// Owns the `inet <containerd_namespace>` table. All rule writes go through
// [`crate::netfilter`] over a persistent netlink socket —
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
use klights_network_api::{
    HostPortRemoval, HostPortRules, ServiceRouter, ServiceRouterError, ServiceRouterFuture,
};
use std::sync::atomic::{AtomicBool, Ordering};

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
    pub routing_state: std::sync::Arc<dyn RoutingStateSource>,
    pub watch: std::sync::Arc<dyn LeaderWatch>,
    pub endpoint_source: std::sync::Arc<dyn klights_network_api::PodEndpointEventSource>,
}

impl NftServiceRouterStores {
    pub fn new(
        routing_state: std::sync::Arc<dyn RoutingStateSource>,
        watch: std::sync::Arc<dyn LeaderWatch>,
        endpoint_source: std::sync::Arc<dyn klights_network_api::PodEndpointEventSource>,
    ) -> Self {
        Self {
            routing_state,
            watch,
            endpoint_source,
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
    pub task_supervisor: std::sync::Arc<klights_supervisor::TaskSupervisor>,
}

impl NftServiceRouterRuntime {
    pub fn new(
        min_sync_period: std::time::Duration,
        cancel: CancellationToken,
        task_supervisor: std::sync::Arc<klights_supervisor::TaskSupervisor>,
    ) -> Self {
        Self {
            min_sync_period,
            cancel,
            task_supervisor,
        }
    }

    pub fn default_window(
        cancel: CancellationToken,
        task_supervisor: std::sync::Arc<klights_supervisor::TaskSupervisor>,
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
    pub task_supervisor: std::sync::Arc<klights_supervisor::TaskSupervisor>,
}

impl<'a> NftServiceRouterDefaultBoot<'a> {
    pub fn new(
        stores: NftServiceRouterStores,
        table: NftServiceRouterTableConfig<'a>,
        network: NftServiceRouterNetworkConfig,
        cancel: CancellationToken,
        task_supervisor: std::sync::Arc<klights_supervisor::TaskSupervisor>,
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
/// bootstrap and held as `Arc<dyn ServiceRouter>` on ApiState.
///
/// The coalescer is a `tokio::sync::Notify`-driven loop. Callers signal
/// "services need re-sync" via `request_services_sync`; the worker
/// waits on the Notify, then sleeps `min_sync_period`, then runs ONE
/// `sync_services_from_api` covering all collapsed events.
/// `Notify::notify_one` is idempotent if no waiter, so N rapid calls
/// between two ticks coalesce into one sync.
///
/// Hostport mutations are NOT coalesced — they're per-pod-create/
/// delete and use the [`KlightsTable`]'s direct add/remove methods
/// (which serialize per-table via the table's instance-owned hostport
/// lock).
pub struct NftServiceRouter {
    table: std::sync::Arc<KlightsTable>,
    routing_state: std::sync::Arc<dyn RoutingStateSource>,
    notify: std::sync::Arc<tokio::sync::Notify>,
    force_full_sync: std::sync::Arc<AtomicBool>,
    /// Cancellation token observed by the coalescer worker. Cancelled
    /// by `cleanup` so the worker exits its `tokio::select!` arms
    /// cleanly instead of being aborted mid-batch.
    cancel: CancellationToken,
    /// JoinHandle for the spawned coalescer worker. `Mutex<Option<_>>`
    /// so `cleanup` can `take()` ownership and `.await` the handle
    /// without holding `&mut self`.
    worker: tokio::sync::Mutex<Option<klights_supervisor::SupervisedJoinHandle<()>>>,
    service_watch_worker: tokio::sync::Mutex<Option<klights_supervisor::SupervisedJoinHandle<()>>>,
    remote_endpoint_worker:
        tokio::sync::Mutex<Option<klights_supervisor::SupervisedJoinHandle<()>>>,
    /// Used by `cleanup` to construct a fresh netlink socket when the
    /// runtime is being torn down. Stored on the struct so callers
    /// don't have to re-thread the supervisor.
    task_supervisor: std::sync::Arc<klights_supervisor::TaskSupervisor>,
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
            routing_state,
            watch,
            endpoint_source,
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
        let notify = std::sync::Arc::new(tokio::sync::Notify::new());
        let force_full_sync = std::sync::Arc::new(AtomicBool::new(true));
        let worker_table = table.clone();
        let worker_notify = notify.clone();
        let worker_cancel = cancel.clone();
        let worker_task_supervisor = task_supervisor.clone();
        let worker_routing_state = routing_state.clone();
        let worker_force_full_sync = force_full_sync.clone();
        let worker = task_supervisor
            .spawn_async(
                klights_supervisor::TaskCategory::Network,
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
                        let full_sync = worker_force_full_sync.swap(false, Ordering::AcqRel);
                        let service_sync_result = if full_sync {
                            worker_table.sync_services_from_api(worker_routing_state.as_ref()).await
                        } else {
                            match worker_table.sync_services_from_cached_inventory().await {
                                Ok(count) => Ok(count),
                                Err(err) => {
                                    tracing::warn!(
                                        error = %err,
                                        "service routing cached inventory unavailable; falling back to full sync"
                                    );
                                    worker_table.sync_services_from_api(worker_routing_state.as_ref()).await
                                }
                            }
                        };
                        let sync_result = match service_sync_result {
                            Ok(service_count) => worker_table
                                .sync_network_policies_from_api(worker_routing_state.as_ref())
                                .await
                                .context("rebuild network-policy chain")
                                .map(|_| service_count),
                            Err(err) => Err(err),
                        };
                        match sync_result {
                            Ok(_n) => {
                                backoff = INITIAL_RETRY_BACKOFF;
                            }
                            Err(e) => {
                                worker_force_full_sync.store(true, Ordering::Release);
                                let next_backoff =
                                    std::cmp::min(backoff.saturating_mul(2), MAX_RETRY_BACKOFF);
                                tracing::warn!(
                                    "coalesced routing sync failed (retry in {:?}, next backoff {:?}): {e:#}",
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
                    tracing::info!("nft routing-sync coalescer exited");
                },
            )
            .await
            .context("failed to spawn service routing coalescer worker")?;

        let remote_table = table.clone();
        let remote_endpoint_source = endpoint_source.clone();
        let remote_cancel = cancel.clone();
        let remote_local_node = local_node_name.to_string();
        let remote_task_supervisor = task_supervisor.clone();
        let service_watch_notify = notify.clone();
        let service_watch_cancel = cancel.clone();
        let service_watch = watch;
        let service_watch_task_supervisor = task_supervisor.clone();
        let service_watch_table = table.clone();
        let service_watch_force_full_sync = force_full_sync.clone();
        let service_watch_worker = task_supervisor
            .spawn_async(
                klights_supervisor::TaskCategory::Network,
                "service_routing_watch_worker",
                async move {
                    run_service_routing_watch_worker(
                        service_watch,
                        service_watch_table,
                        service_watch_notify,
                        service_watch_cancel,
                        service_watch_task_supervisor,
                        service_watch_force_full_sync,
                    )
                    .await;
                },
            )
            .await
            .context("failed to spawn service routing watch worker")?;
        let remote_endpoint_worker = task_supervisor
            .spawn_async(
                klights_supervisor::TaskCategory::Network,
                "service_routing_remote_pod_endpoint_worker",
                async move {
                    run_remote_pod_endpoint_worker(
                        remote_table,
                        remote_endpoint_source,
                        remote_local_node,
                        remote_cancel,
                        remote_task_supervisor,
                    )
                    .await;
                },
            )
            .await
            .context("failed to spawn remote pod endpoint worker")?;

        Ok(std::sync::Arc::new(Self {
            table,
            routing_state,
            notify,
            force_full_sync,
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
        let bridge_ifname = request.table.bridge_ifname.to_string();
        let pod_subnet = request.network.pod_subnet.to_string();
        ensure_service_routing_sysctls(&request.task_supervisor, &bridge_ifname).await?;
        let rt = Self::boot(request.into_boot()).await.with_context(|| {
            format!("boot NftServiceRouter for inet {table_name} (pod subnet {pod_subnet})")
        })?;
        tracing::info!(
            "Initialized nft runtime (table: inet {table_name}, pod subnet: {pod_subnet})"
        );
        Ok(rt)
    }
}

impl ServiceRouter for NftServiceRouter {
    fn request_services_sync(&self) -> Result<(), ServiceRouterError> {
        self.force_full_sync.store(true, Ordering::Release);
        self.notify.notify_one();
        Ok(())
    }

    fn sync_services_now(&self) -> ServiceRouterFuture<'_> {
        Box::pin(async move {
            sync_routing_from_api(self.table.as_ref(), self.routing_state.as_ref())
                .await
                .context("sync_services_now: rebuild routing chains")
                .map_err(|error| ServiceRouterError::sync(error.to_string()))?;
            Ok(())
        })
    }

    fn add_hostport_rules(&self, request: HostPortRules) -> ServiceRouterFuture<'_> {
        Box::pin(async move {
            let (pod_ip, bindings) = request.into_parts();
            let specs = bindings.into_iter().map(Into::into).collect();
            self.table
                .add_hostports_for_pod(pod_ip, specs)
                .await
                .map_err(|error| ServiceRouterError::hostport(error.to_string()))
        })
    }

    fn remove_hostport_rules(&self, request: HostPortRemoval) -> ServiceRouterFuture<'_> {
        Box::pin(async move {
            self.table
                .remove_hostports_for_pod(request.pod_ip())
                .await
                .map_err(|error| ServiceRouterError::hostport(error.to_string()))
        })
    }

    fn cleanup(&self) -> ServiceRouterFuture<'_> {
        Box::pin(async move {
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
                .context("open netlink socket for cleanup")
                .map_err(|error| ServiceRouterError::cleanup(error.to_string()))?;
            let placeholder_pod = PodSubnet::parse("0.0.0.0/30").expect("static placeholder");
            let placeholder_cluster = ClusterCidr::parse("0.0.0.0/0").expect("static placeholder");
            let placeholder_service = ClusterCidr::parse("0.0.0.0/0").expect("static placeholder");
            // Cleanup only deletes the table; the mode field never drives kernel
            // calls on the cleanup path, so a default placeholder is safe here.
            let placeholder_mode = ServiceRoutingMode::new();
            let table = KlightsTable::with_name_and_bridge(
                nf,
                &self.table_name_str,
                &self.table_name_str,
                placeholder_pod,
                placeholder_cluster,
                placeholder_service,
                placeholder_mode,
            )
            .context("construct KlightsTable for cleanup")
            .map_err(|error| ServiceRouterError::cleanup(error.to_string()))?;
            table
                .cleanup()
                .await
                .map_err(|error| ServiceRouterError::cleanup(error.to_string()))
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct ServiceRoutingWatchTarget {
    api_version: &'static str,
    kind: &'static str,
}

impl ServiceRoutingWatchTarget {
    fn request(self) -> klights_leader_api::WatchRequest {
        klights_leader_api::WatchRequest::try_new_with_scope(
            self.api_version,
            self.kind,
            None,
            if self.kind == "Namespace" {
                klights_leader_api::ResourceListScope::Cluster
            } else {
                klights_leader_api::ResourceListScope::AllNamespaces
            },
            None,
            None,
            None,
            None,
        )
        .expect("static service-routing watch target is valid")
    }
}

const SERVICE_ROUTING_WATCH_TARGETS: [ServiceRoutingWatchTarget; 6] = [
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
    ServiceRoutingWatchTarget {
        api_version: "networking.k8s.io/v1",
        kind: "NetworkPolicy",
    },
    ServiceRoutingWatchTarget {
        api_version: "v1",
        kind: "Pod",
    },
    ServiceRoutingWatchTarget {
        api_version: "v1",
        kind: "Namespace",
    },
];

fn service_inventory_watch_target_requires_full_sync(target: ServiceRoutingWatchTarget) -> bool {
    matches!(
        (target.api_version, target.kind),
        ("v1", "Service") | ("v1", "Endpoints") | ("discovery.k8s.io/v1", "EndpointSlice")
    )
}

fn policy_watch_target_triggers_sync(target: ServiceRoutingWatchTarget) -> bool {
    matches!(
        (target.api_version, target.kind),
        ("networking.k8s.io/v1", "NetworkPolicy") | ("v1", "Pod") | ("v1", "Namespace")
    )
}

enum ServiceRoutingWatchItem {
    Event {
        target: ServiceRoutingWatchTarget,
        event: std::result::Result<
            klights_leader_api::ResourceEvent,
            klights_leader_api::LeaderWatchError,
        >,
    },
    Closed {
        target: ServiceRoutingWatchTarget,
    },
}

type ServiceRoutingWatchStream =
    std::pin::Pin<Box<dyn futures::Stream<Item = ServiceRoutingWatchItem> + Send>>;

fn wrap_service_routing_watch_stream(
    target: ServiceRoutingWatchTarget,
    mut stream: klights_leader_api::WatchStream,
) -> ServiceRoutingWatchStream {
    use futures::StreamExt;

    Box::pin(async_stream::stream! {
        while let Some(event) = stream.next().await {
            match event {
                Ok(event) => {
                    yield ServiceRoutingWatchItem::Event { target, event: Ok(event) };
                }
                Err(err) => {
                    // A decode/transport error means the stream can no longer
                    // be trusted to stay in sync. Terminate it so the worker
                    // performs a single, backoff-throttled reconnect via the
                    // Closed item below, instead of layering a second live
                    // watch on top of this one. Keeping the loop alive after an
                    // error (the f46660f regression) leaked duplicate watches
                    // under the sustained errors the high-volume Pod watch
                    // produces under e2e churn, eventually starving the runtime.
                    yield ServiceRoutingWatchItem::Event { target, event: Err(err) };
                    break;
                }
            }
        }
        yield ServiceRoutingWatchItem::Closed { target };
    })
}

async fn open_service_routing_watch_set(
    leader_watch: &std::sync::Arc<dyn LeaderWatch>,
) -> Result<futures::stream::SelectAll<ServiceRoutingWatchStream>> {
    let mut streams = futures::stream::SelectAll::new();
    for target in SERVICE_ROUTING_WATCH_TARGETS {
        let stream = leader_watch
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
    task_supervisor: &std::sync::Arc<klights_supervisor::TaskSupervisor>,
    cancel: &CancellationToken,
    attempt: u32,
) -> bool {
    tokio::select! {
        _ = cancel.cancelled() => false,
        result = task_supervisor.sleep(
            "service_routing_watch_reconnect_backoff",
            klights_supervisor::reconnect_backoff::delay(attempt),
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

fn watch_event_object_identity(object: &serde_json::Value) -> Result<(&str, &str, i64)> {
    let namespace = object
        .pointer("/metadata/namespace")
        .and_then(|value| value.as_str())
        .context("service routing watch event missing metadata.namespace")?;
    let name = object
        .pointer("/metadata/name")
        .and_then(|value| value.as_str())
        .context("service routing watch event missing metadata.name")?;
    let resource_version = klights_types::resource_metadata::object_resource_version(object);
    if resource_version <= 0 {
        anyhow::bail!("service routing watch event missing metadata.resourceVersion");
    }
    Ok((namespace, name, resource_version))
}

fn apply_service_routing_watch_event_to_inventory(
    table: &KlightsTable,
    target: ServiceRoutingWatchTarget,
    event: klights_leader_api::ResourceEvent,
) -> Result<Option<super::inventory::InventoryApply>> {
    use klights_leader_api::WatchEventType;

    match event.event_type() {
        WatchEventType::Bookmark => return Ok(Some(super::inventory::InventoryApply::NoChange)),
        WatchEventType::Error => anyhow::bail!("service routing watch delivered ERROR event"),
        WatchEventType::Added | WatchEventType::Modified | WatchEventType::Deleted => {}
    }

    let object = event.resource().data.as_ref();

    match (target.api_version, target.kind) {
        _ if policy_watch_target_triggers_sync(target) => {
            Ok(Some(super::inventory::InventoryApply::Applied))
        }
        ("v1", "Service") => {
            let deleted = event.event_type() == WatchEventType::Deleted;
            let (namespace, name, resource_version) = watch_event_object_identity(object)?;
            let data = (!deleted).then(|| (*object).clone());
            Ok(table.apply_service_event_to_inventory(
                namespace,
                name,
                resource_version,
                deleted,
                data,
            ))
        }
        ("v1", "Endpoints") => {
            let deleted = event.event_type() == WatchEventType::Deleted;
            let (namespace, name, resource_version) = watch_event_object_identity(object)?;
            let data = (!deleted).then(|| (*object).clone());
            Ok(table.apply_endpoints_event_to_inventory(
                namespace,
                name,
                resource_version,
                deleted,
                data,
            ))
        }
        ("discovery.k8s.io/v1", "EndpointSlice") => {
            let deleted = event.event_type() == WatchEventType::Deleted;
            let (namespace, name, resource_version) = watch_event_object_identity(object)?;
            let data = (!deleted).then(|| (*object).clone());
            let Some(service_name) = object
                .pointer("/metadata/labels/kubernetes.io~1service-name")
                .and_then(|value| value.as_str())
            else {
                return Ok(Some(super::inventory::InventoryApply::NoChange));
            };
            Ok(table.apply_endpoint_slice_event_to_inventory(
                namespace,
                service_name,
                name,
                resource_version,
                deleted,
                data,
            ))
        }
        _ => Ok(Some(super::inventory::InventoryApply::NoChange)),
    }
}

async fn run_service_routing_watch_worker(
    leader_watch: std::sync::Arc<dyn LeaderWatch>,
    table: std::sync::Arc<KlightsTable>,
    notify: std::sync::Arc<tokio::sync::Notify>,
    cancel: CancellationToken,
    task_supervisor: std::sync::Arc<klights_supervisor::TaskSupervisor>,
    force_full_sync: std::sync::Arc<AtomicBool>,
) {
    use futures::StreamExt;

    tracing::info!("nft service routing watch worker started");

    // Each watch target owns its own reconnect history. A healthy Service
    // event must not erase a flapping Pod watch's backoff, and a Pod closure
    // must not delay the first reconnect for an independently closed
    // EndpointSlice watch after a real leader transition.
    let mut reconnect_attempts = std::collections::HashMap::<ServiceRoutingWatchTarget, u32>::new();
    let mut watch_set_reconnect_attempt: u32 = 0;
    loop {
        let mut streams = match open_service_routing_watch_set(&leader_watch).await {
            Ok(streams) => streams,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "service routing failed to open cluster watches; scheduling full service sync"
                );
                force_full_sync.store(true, Ordering::Release);
                notify.notify_one();
                if !service_routing_watch_reconnect_delay(
                    &task_supervisor,
                    &cancel,
                    watch_set_reconnect_attempt,
                )
                .await
                {
                    break;
                }
                watch_set_reconnect_attempt = watch_set_reconnect_attempt.saturating_add(1);
                continue;
            }
        };
        watch_set_reconnect_attempt = 0;

        // The watches above only deliver events observed after each stream is
        // opened. A full sync after the watch set is established closes the
        // bootstrap race where an existing Service, such as kube-dns, gains
        // ready Endpoints before this node's watch streams are active.
        force_full_sync.store(true, Ordering::Release);
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
                        Some(ServiceRoutingWatchItem::Event { target, event: Ok(event) }) => {
                            match apply_service_routing_watch_event_to_inventory(table.as_ref(), target, event) {
                                Ok(Some(super::inventory::InventoryApply::Applied | super::inventory::InventoryApply::Removed)) => {
                                    reconnect_attempts.insert(target, 0);
                                    if service_inventory_watch_target_requires_full_sync(target) {
                                        force_full_sync.store(true, Ordering::Release);
                                    }
                                    notify.notify_one();
                                }
                                Ok(Some(super::inventory::InventoryApply::NoChange)) => {
                                    reconnect_attempts.insert(target, 0);
                                }
                                Ok(None) => {
                                    force_full_sync.store(true, Ordering::Release);
                                    notify.notify_one();
                                }
                                Err(err) => {
                                    tracing::warn!(
                                        api_version = target.api_version,
                                        kind = target.kind,
                                        error = %err,
                                        "service routing watch event could not update inventory; scheduling full service sync"
                                    );
                                    force_full_sync.store(true, Ordering::Release);
                                    notify.notify_one();
                                    reopen_watch_set = true;
                                }
                            }
                        }
                        Some(ServiceRoutingWatchItem::Event { target, event: Err(err) }) => {
                            tracing::warn!(
                                api_version = target.api_version,
                                kind = target.kind,
                                error = %err,
                                "service routing watch stream returned an error; closing it for a single reconnect"
                            );
                            force_full_sync.store(true, Ordering::Release);
                            notify.notify_one();
                            // The wrapper terminates the stream after an error,
                            // so the single reconnect happens in the `Closed` arm
                            // below with backoff. Do NOT open a replacement here:
                            // the errored stream is still alive at this point, and
                            // pushing a second watch would leak duplicate streams
                            // under sustained errors (e.g. the Pod watch under e2e
                            // churn), eventually starving the runtime and breaking
                            // time-sensitive admission webhook callouts.
                        }
                        Some(ServiceRoutingWatchItem::Closed { target }) => {
                            tracing::warn!(
                                api_version = target.api_version,
                                kind = target.kind,
                                "service routing watch stream closed; reconnecting that one watch and scheduling full sync"
                            );
                            force_full_sync.store(true, Ordering::Release);
                            notify.notify_one();

                            // Back off before reconnecting when watch streams
                            // flap. The first close (no prior failed reconnect)
                            // reconnects immediately so a single transient close
                            // is not penalized; consecutive closures with no
                            // successful event in between back off exponentially
                            // (500ms -> 60s) to avoid a tight reconnect loop
                            // that would starve the async runtime — the same
                            // backoff the old full-reopen path applied via this
                            // loop's outer arm. The attempt is target-local and
                            // resets only when this target delivers a healthy
                            // event (see the Ok arm above).
                            let attempt = reconnect_attempts.get(&target).copied().unwrap_or(0);
                            reconnect_attempts
                                .insert(target, attempt.saturating_add(1));
                            let backoff_cancelled = attempt > 0
                                && !service_routing_watch_reconnect_delay(
                                    &task_supervisor,
                                    &cancel,
                                    attempt,
                                )
                                .await;
                            if backoff_cancelled {
                                // Cancelled during backoff; fall through to the
                                // outer loop which also exits on cancel.
                                reopen_watch_set = true;
                            } else {
                                let reopened = match leader_watch
                                    .watch_resources(target.request())
                                    .await
                                {
                                    Ok(new_stream) => {
                                        streams.push(wrap_service_routing_watch_stream(
                                            target,
                                            new_stream,
                                        ));
                                        true
                                    }
                                    Err(reopen_err) => {
                                        tracing::warn!(
                                            api_version = target.api_version,
                                            kind = target.kind,
                                            error = %reopen_err,
                                            "target watch stream reopen after close failed; scheduling full watch set reopen"
                                        );
                                        false
                                    }
                                };
                                if !reopened {
                                    reopen_watch_set = true;
                                }
                            }
                        }
                        None => {
                            tracing::warn!(
                                "service routing watch set closed; scheduling full service sync"
                            );
                            force_full_sync.store(true, Ordering::Release);
                            notify.notify_one();
                            reopen_watch_set = true;
                        }
                    }
                }
            }
        }

        if !service_routing_watch_reconnect_delay(
            &task_supervisor,
            &cancel,
            watch_set_reconnect_attempt,
        )
        .await
        {
            break;
        }
        watch_set_reconnect_attempt = watch_set_reconnect_attempt.saturating_add(1);
    }
    tracing::info!("nft service routing watch worker exited");
}

async fn sync_routing_from_api(
    table: &KlightsTable,
    routing_state: &dyn RoutingStateSource,
) -> Result<usize> {
    let service_count = table
        .sync_services_from_api(routing_state)
        .await
        .context("rebuild services chain")?;
    table
        .sync_network_policies_from_api(routing_state)
        .await
        .context("rebuild network-policy chain")?;
    Ok(service_count)
}

type RemotePodEndpointRuleFuture<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>>;

trait RemotePodEndpointRuleSync: Send + Sync {
    fn apply_event<'a>(
        &'a self,
        local_node_name: &'a str,
        event: klights_network_api::PodEndpointEvent,
    ) -> RemotePodEndpointRuleFuture<'a>;
}

impl RemotePodEndpointRuleSync for KlightsTable {
    fn apply_event<'a>(
        &'a self,
        local_node_name: &'a str,
        event: klights_network_api::PodEndpointEvent,
    ) -> RemotePodEndpointRuleFuture<'a> {
        Box::pin(async move {
            match event {
                klights_network_api::PodEndpointEvent::Upsert(endpoint) => {
                    self.upsert_remote_pod_endpoint(local_node_name, endpoint)
                        .await
                }
                klights_network_api::PodEndpointEvent::Delete(pod_ip) => {
                    self.remove_remote_pod_endpoint(pod_ip).await
                }
                klights_network_api::PodEndpointEvent::Resync(snapshot) => self
                    .sync_remote_pod_endpoints_from_topology(local_node_name, &snapshot)
                    .await
                    .map(|_| ()),
            }
        })
    }
}

async fn run_remote_pod_endpoint_worker(
    endpoint_rules: std::sync::Arc<dyn RemotePodEndpointRuleSync>,
    endpoint_source: std::sync::Arc<dyn klights_network_api::PodEndpointEventSource>,
    local_node_name: String,
    cancel: CancellationToken,
    task_supervisor: std::sync::Arc<klights_supervisor::TaskSupervisor>,
) {
    tracing::info!("nft remote pod endpoint worker started");
    let mut retry_backoff = INITIAL_RETRY_BACKOFF;
    'subscribe: loop {
        let subscription = tokio::select! {
            _ = cancel.cancelled() => break,
            subscription = endpoint_source.subscribe() => subscription,
        };
        let mut events = match subscription {
            Ok(events) => {
                retry_backoff = INITIAL_RETRY_BACKOFF;
                events
            }
            Err(error) => {
                tracing::warn!(error = %error, "remote pod endpoint subscription failed");
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = task_supervisor.sleep(
                        "service_routing_endpoint_subscription_retry",
                        retry_backoff,
                    ) => {}
                }
                retry_backoff = std::cmp::min(retry_backoff.saturating_mul(2), MAX_RETRY_BACKOFF);
                continue;
            }
        };
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break 'subscribe,
                event = std::future::poll_fn(|context| events.as_mut().poll_next(context)) => {
                    match event {
                    Some(Ok(event)) => {
                        if let Err(error) = endpoint_rules
                            .apply_event(&local_node_name, event)
                            .await
                        {
                            tracing::warn!(
                                error = %error,
                                "remote pod endpoint nft apply lost authority; resubscribing for authoritative resync"
                            );
                            break;
                        }
                        retry_backoff = INITIAL_RETRY_BACKOFF;
                    }
                    Some(Err(error)) => {
                        tracing::warn!(error = %error, "remote pod endpoint subscription lost authority; retrying");
                        break;
                    }
                    None => break,
                    }
                }
            }
        }
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = task_supervisor.sleep(
                "service_routing_endpoint_stream_retry",
                retry_backoff,
            ) => {}
        }
        retry_backoff = std::cmp::min(retry_backoff.saturating_mul(2), MAX_RETRY_BACKOFF);
    }
    tracing::info!("nft remote pod endpoint worker exited");
}

async fn write_proc_sysctl(
    file_process: &klights_supervisor::FileProcessExecutor,
    path: &str,
    value: &str,
) -> Result<()> {
    klights_supervisor::runtime_fs::write_async(file_process, path, value)
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
    ("/proc/sys/net/ipv4/conf/default/route_localnet", "1\n"),
];

fn required_service_routing_sysctl_entries(bridge_ifname: &str) -> Vec<(String, &'static str)> {
    let mut entries = Vec::with_capacity(REQUIRED_SERVICE_ROUTING_SYSCTLS.len() + 1);
    entries.extend(
        REQUIRED_SERVICE_ROUTING_SYSCTLS
            .iter()
            .map(|(path, value)| ((*path).to_string(), *value)),
    );
    if !bridge_ifname.is_empty() {
        entries.push((
            format!("/proc/sys/net/ipv4/conf/{bridge_ifname}/route_localnet"),
            "1\n",
        ));
    }
    entries
}

async fn ensure_service_routing_sysctls(
    task_supervisor: &std::sync::Arc<klights_supervisor::TaskSupervisor>,
    bridge_ifname: &str,
) -> Result<()> {
    let file_process = klights_supervisor::FileProcessExecutor::new(task_supervisor.clone());
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

    for (path, value) in required_service_routing_sysctl_entries(bridge_ifname) {
        ensure_sysctl_value(&file_process, &path, value).await?;
    }
    Ok(())
}

async fn ensure_sysctl_value(
    file_process: &klights_supervisor::FileProcessExecutor,
    path: &str,
    expected: &str,
) -> Result<()> {
    write_proc_sysctl(file_process, path, expected).await?;
    let actual = klights_supervisor::runtime_fs::read_utf8_async(file_process, path)
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
    use klights_leader_api::{
        LeaderWatch, LeaderWatchError, LeaderWatchFuture, ResourceEvent, WatchRequest, WatchStream,
    };
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    use tokio::sync::Notify;

    use tokio_util::sync::CancellationToken;

    fn test_service_table(
        task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
    ) -> Arc<KlightsTable> {
        let nf = Netfilter::new(task_supervisor).expect("Netfilter::new");
        Arc::new(
            KlightsTable::with_name(
                nf,
                "klights-test-watch",
                PodSubnet::parse("10.99.0.0/24").expect("static pod cidr"),
                ClusterCidr::parse("10.99.0.0/16").expect("static cluster cidr"),
                ClusterCidr::parse("10.99.128.0/17").expect("static service cidr"),
                ServiceRoutingMode::default_root_for_test(),
            )
            .expect("build service routing table"),
        )
    }

    #[derive(Default)]
    struct WatchOnlyLeaderApiClient {
        watches_opened: AtomicUsize,
        watched_targets: Mutex<Vec<(String, String)>>,
    }

    #[derive(Default)]
    struct ReopeningLeaderApiClient {
        watches_opened: AtomicUsize,
        opened_notify: Notify,
        closed_endpoints_once: std::sync::atomic::AtomicBool,
    }

    #[derive(Default)]
    struct ClosingOnceAllLeaderApiClient {
        watches_opened: AtomicUsize,
        opened_notify: Notify,
        opens_by_target: Mutex<std::collections::HashMap<(String, String), usize>>,
    }

    #[derive(Default)]
    struct ReopeningEndpointSource {
        subscriptions: AtomicUsize,
    }

    struct OneResyncSubscription {
        delivered: bool,
    }

    impl klights_network_api::PodEndpointEventSubscription for OneResyncSubscription {
        fn poll_next(
            mut self: std::pin::Pin<&mut Self>,
            _context: &mut std::task::Context<'_>,
        ) -> std::task::Poll<
            Option<
                std::result::Result<
                    klights_network_api::PodEndpointEvent,
                    klights_network_api::PodEndpointError,
                >,
            >,
        > {
            if self.delivered {
                std::task::Poll::Ready(None)
            } else {
                self.delivered = true;
                std::task::Poll::Ready(Some(Ok(klights_network_api::PodEndpointEvent::Resync(
                    Vec::new(),
                ))))
            }
        }
    }

    impl klights_network_api::PodEndpointEventSource for ReopeningEndpointSource {
        fn subscribe(
            &self,
        ) -> klights_network_api::PodEndpointFuture<'_, klights_network_api::PodEndpointEventStream>
        {
            self.subscriptions.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {
                Ok(Box::pin(OneResyncSubscription { delivered: false })
                    as klights_network_api::PodEndpointEventStream)
            })
        }
    }

    #[derive(Default)]
    struct FailOnceRemoteEndpointSync {
        attempts: AtomicUsize,
        attempted: Notify,
    }

    impl RemotePodEndpointRuleSync for FailOnceRemoteEndpointSync {
        fn apply_event<'a>(
            &'a self,
            _local_node_name: &'a str,
            _event: klights_network_api::PodEndpointEvent,
        ) -> RemotePodEndpointRuleFuture<'a> {
            Box::pin(async move {
                let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
                self.attempted.notify_waiters();
                if attempt == 0 {
                    Err(anyhow::anyhow!("deterministic nft apply failure"))
                } else {
                    Ok(())
                }
            })
        }
    }

    #[tokio::test]
    async fn remote_endpoint_nft_failure_resubscribes_for_authoritative_resync() {
        let source = Arc::new(ReopeningEndpointSource::default());
        let sink = Arc::new(FailOnceRemoteEndpointSync::default());
        let cancel = CancellationToken::new();
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let worker = supervisor
            .spawn_async(
                klights_supervisor::TaskCategory::Network,
                "test_remote_endpoint_apply_retry",
                run_remote_pod_endpoint_worker(
                    sink.clone(),
                    source.clone(),
                    "node-a".to_string(),
                    cancel.clone(),
                    supervisor.clone(),
                ),
            )
            .await
            .unwrap();

        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            loop {
                if sink.attempts.load(Ordering::SeqCst) >= 2 {
                    break;
                }
                sink.attempted.notified().await;
            }
        })
        .await
        .expect("failed nft apply must be followed by a supervised resubscribe/resync retry");
        cancel.cancel();
        worker.join().await.unwrap();
        assert!(source.subscriptions.load(Ordering::SeqCst) >= 2);
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

    impl ClosingOnceAllLeaderApiClient {
        async fn wait_for_opened(&self, expected: usize) {
            loop {
                if self.watches_opened.load(Ordering::SeqCst) >= expected {
                    return;
                }
                self.opened_notify.notified().await;
            }
        }
    }

    fn expected_service_routing_watch_targets() -> Vec<(String, String)> {
        SERVICE_ROUTING_WATCH_TARGETS
            .iter()
            .map(|target| (target.api_version.to_string(), target.kind.to_string()))
            .collect()
    }

    #[test]
    fn policy_only_watch_targets_trigger_sync_without_service_inventory_identity() {
        for target in [
            ServiceRoutingWatchTarget {
                api_version: "networking.k8s.io/v1",
                kind: "NetworkPolicy",
            },
            ServiceRoutingWatchTarget {
                api_version: "v1",
                kind: "Pod",
            },
            ServiceRoutingWatchTarget {
                api_version: "v1",
                kind: "Namespace",
            },
        ] {
            assert!(
                policy_watch_target_triggers_sync(target),
                "{}/{} event should wake network-policy reconcile without requiring a Service inventory key",
                target.api_version,
                target.kind
            );
        }
    }

    impl LeaderWatch for WatchOnlyLeaderApiClient {
        fn watch_resources(&self, req: WatchRequest) -> LeaderWatchFuture<'_> {
            self.watches_opened.fetch_add(1, Ordering::SeqCst);
            self.watched_targets
                .lock()
                .expect("watch target record lock not poisoned")
                .push((req.api_version().to_string(), req.kind().to_string()));
            Box::pin(async {
                Ok(WatchStream::unpositioned_test_stream(
                    futures::stream::pending(),
                ))
            })
        }
    }

    impl LeaderWatch for ReopeningLeaderApiClient {
        fn watch_resources(&self, req: WatchRequest) -> LeaderWatchFuture<'_> {
            self.watches_opened.fetch_add(1, Ordering::SeqCst);
            self.opened_notify.notify_waiters();
            let close = req.kind() == "Endpoints"
                && !self
                    .closed_endpoints_once
                    .swap(true, std::sync::atomic::Ordering::SeqCst);
            Box::pin(async move {
                if close {
                    Ok(WatchStream::unpositioned_test_stream(
                        futures::stream::empty(),
                    ))
                } else {
                    Ok(WatchStream::unpositioned_test_stream(
                        futures::stream::pending(),
                    ))
                }
            })
        }
    }

    impl LeaderWatch for ClosingOnceAllLeaderApiClient {
        fn watch_resources(&self, req: WatchRequest) -> LeaderWatchFuture<'_> {
            self.watches_opened.fetch_add(1, Ordering::SeqCst);
            self.opened_notify.notify_waiters();
            let key = (req.api_version().to_string(), req.kind().to_string());
            let close = {
                let mut opens = self
                    .opens_by_target
                    .lock()
                    .expect("watch target count lock not poisoned");
                let count = opens.entry(key).or_default();
                *count += 1;
                *count == 1
            };
            Box::pin(async move {
                if close {
                    Ok(WatchStream::unpositioned_test_stream(
                        futures::stream::empty(),
                    ))
                } else {
                    Ok(WatchStream::unpositioned_test_stream(
                        futures::stream::pending(),
                    ))
                }
            })
        }
    }

    #[tokio::test]
    async fn service_routing_watch_worker_requests_full_sync_after_watches_open() {
        let client = Arc::new(WatchOnlyLeaderApiClient::default());
        let notify = Arc::new(Notify::new());
        let cancel = CancellationToken::new();
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let table = test_service_table(supervisor.clone());
        let force_full_sync = Arc::new(AtomicBool::new(false));

        let worker = supervisor
            .spawn_async(
                klights_supervisor::TaskCategory::Network,
                "test_service_routing_watch_worker",
                run_service_routing_watch_worker(
                    client.clone(),
                    table,
                    notify.clone(),
                    cancel.clone(),
                    supervisor.clone(),
                    force_full_sync.clone(),
                ),
            )
            .await
            .expect("spawn watch worker under task supervisor");

        tokio::time::timeout(std::time::Duration::from_millis(100), notify.notified())
            .await
            .expect("watch worker must request an initial full service sync");
        assert!(
            force_full_sync.load(Ordering::SeqCst),
            "watch worker must mark the initial notification as a full sync"
        );

        cancel.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(1), worker.join())
            .await
            .expect("watch worker must exit after cancellation")
            .expect("watch worker task must not panic");

        assert_eq!(
            client.watches_opened.load(Ordering::SeqCst),
            SERVICE_ROUTING_WATCH_TARGETS.len(),
            "service routing must open every routing-affecting watch"
        );
        assert_eq!(
            *client
                .watched_targets
                .lock()
                .expect("watch target record lock not poisoned"),
            expected_service_routing_watch_targets()
        );
    }

    struct EmptyRoutingState;

    impl RoutingStateSource for EmptyRoutingState {
        fn service_routing_snapshot(&self) -> RoutingStateFuture<'_, ServiceRoutingSnapshot> {
            Box::pin(async { Ok(ServiceRoutingSnapshot::default()) })
        }

        fn network_policy_snapshot(&self) -> RoutingStateFuture<'_, NetworkPolicySnapshot> {
            Box::pin(async { Ok(NetworkPolicySnapshot::default()) })
        }
    }

    #[test]
    fn request_services_sync_marks_next_coalesced_pass_as_full_sync() {
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let force_full_sync = Arc::new(AtomicBool::new(false));
        let router = NftServiceRouter {
            table: test_service_table(supervisor.clone()),
            routing_state: Arc::new(EmptyRoutingState),
            notify: Arc::new(Notify::new()),
            force_full_sync: force_full_sync.clone(),
            cancel: CancellationToken::new(),
            worker: tokio::sync::Mutex::new(None),
            service_watch_worker: tokio::sync::Mutex::new(None),
            remote_endpoint_worker: tokio::sync::Mutex::new(None),
            task_supervisor: supervisor,
            table_name_str: "klights-test-watch".to_string(),
        };

        router.request_services_sync().unwrap();

        assert!(
            force_full_sync.load(Ordering::SeqCst),
            "external service sync requests must force a fresh API snapshot so missed watch events or stale cached inventory cannot leave nft rules stale"
        );
    }

    #[test]
    fn service_inventory_watch_targets_force_fresh_service_snapshot() {
        for target in [
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
        ] {
            assert!(
                service_inventory_watch_target_requires_full_sync(target),
                "{}/{} changes must force a fresh service-route snapshot",
                target.api_version,
                target.kind
            );
        }

        for target in [
            ServiceRoutingWatchTarget {
                api_version: "networking.k8s.io/v1",
                kind: "NetworkPolicy",
            },
            ServiceRoutingWatchTarget {
                api_version: "v1",
                kind: "Pod",
            },
            ServiceRoutingWatchTarget {
                api_version: "v1",
                kind: "Namespace",
            },
        ] {
            assert!(
                !service_inventory_watch_target_requires_full_sync(target),
                "{}/{} changes should not force a service inventory re-list",
                target.api_version,
                target.kind
            );
        }
    }

    #[tokio::test]
    async fn service_routing_watch_worker_reopens_when_one_watch_stream_closes() {
        let client = Arc::new(ReopeningLeaderApiClient::default());
        let notify = Arc::new(Notify::new());
        let cancel = CancellationToken::new();
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let table = test_service_table(supervisor.clone());
        let force_full_sync = Arc::new(AtomicBool::new(false));

        let worker = supervisor
            .spawn_async(
                klights_supervisor::TaskCategory::Network,
                "test_service_routing_watch_worker_reopen",
                run_service_routing_watch_worker(
                    client.clone(),
                    table,
                    notify.clone(),
                    cancel.clone(),
                    supervisor.clone(),
                    force_full_sync.clone(),
                ),
            )
            .await
            .expect("spawn watch worker under task supervisor");

        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            client.wait_for_opened(SERVICE_ROUTING_WATCH_TARGETS.len()),
        )
        .await
        .expect("watch worker must open the initial watch set");
        tokio::time::timeout(std::time::Duration::from_millis(100), notify.notified())
            .await
            .expect("initial watch set must request a full service sync");
        assert!(
            force_full_sync.load(Ordering::SeqCst),
            "initial watch set must request a full service sync flag"
        );

        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            client.wait_for_opened(SERVICE_ROUTING_WATCH_TARGETS.len() + 1),
        )
        .await
        .expect("watch worker must reconnect the closed watch without reopening all watches");

        cancel.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(1), worker.join())
            .await
            .expect("watch worker must exit after cancellation")
            .expect("watch worker task must not panic");
    }

    #[tokio::test]
    async fn independent_watch_closures_reopen_without_cross_target_backoff() {
        let client = Arc::new(ClosingOnceAllLeaderApiClient::default());
        let notify = Arc::new(Notify::new());
        let cancel = CancellationToken::new();
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let table = test_service_table(supervisor.clone());
        let force_full_sync = Arc::new(AtomicBool::new(false));

        let worker = supervisor
            .spawn_async(
                klights_supervisor::TaskCategory::Network,
                "test_service_routing_independent_watch_reopen",
                run_service_routing_watch_worker(
                    client.clone(),
                    table,
                    notify,
                    cancel.clone(),
                    supervisor.clone(),
                    force_full_sync,
                ),
            )
            .await
            .expect("spawn watch worker under task supervisor");

        tokio::time::timeout(
            std::time::Duration::from_millis(250),
            client.wait_for_opened(SERVICE_ROUTING_WATCH_TARGETS.len() * 2),
        )
        .await
        .expect(
            "each independently closed watch must get its first reconnect without waiting for another target's backoff",
        );

        cancel.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(1), worker.join())
            .await
            .expect("watch worker must exit after cancellation")
            .expect("watch worker task must not panic");
    }

    /// Fake leader API client whose `Pod` watch stream yields a burst of
    /// errors and then ends, every time it is opened. Models the
    /// high-volume Pod watch flapping under e2e churn that f46660f
    /// identified as unreliable.
    #[derive(Default)]
    struct ErroringLeaderApiClient {
        watches_opened: AtomicUsize,
        opened_notify: Notify,
    }

    impl ErroringLeaderApiClient {
        async fn wait_for_opened(&self, expected: usize) {
            loop {
                if self.watches_opened.load(Ordering::SeqCst) >= expected {
                    return;
                }
                self.opened_notify.notified().await;
            }
        }
    }

    impl LeaderWatch for ErroringLeaderApiClient {
        fn watch_resources(&self, req: WatchRequest) -> LeaderWatchFuture<'_> {
            self.watches_opened.fetch_add(1, Ordering::SeqCst);
            self.opened_notify.notify_waiters();
            let is_pod = req.kind() == "Pod";
            Box::pin(async move {
                if is_pod {
                    // Each open of the Pod watch immediately delivers a burst of
                    // errors, then ends. A leaky reconnect (one new open per error,
                    // keeping the old stream alive) would fan out into hundreds of
                    // duplicate opens; a backoff-less reconnect would tight-loop.
                    let burst: Vec<std::result::Result<ResourceEvent, LeaderWatchError>> = (0..20)
                        .map(|_| Err(LeaderWatchError::transport("simulated watch error")))
                        .collect();
                    return Ok(WatchStream::unpositioned_test_stream(
                        futures::stream::iter(burst),
                    ));
                }
                Ok(WatchStream::unpositioned_test_stream(
                    futures::stream::pending(),
                ))
            })
        }
    }

    /// Regression test for the per-watch reconnect introduced in f46660f: an
    /// erroring watch stream must NOT leak duplicate watches (one new open per
    /// error while the old stream stays alive) nor tight-loop reconnect
    /// without backoff. The worker must terminate the errored stream after the
    /// first error and perform a single backoff-throttled reconnect, keeping
    /// total watch opens bounded even under a sustained error burst.
    #[tokio::test]
    async fn service_routing_watch_worker_bounds_reconnects_on_repeated_watch_errors() {
        let client = Arc::new(ErroringLeaderApiClient::default());
        let notify = Arc::new(Notify::new());
        let cancel = CancellationToken::new();
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let table = test_service_table(supervisor.clone());
        let force_full_sync = Arc::new(AtomicBool::new(false));

        let worker = supervisor
            .spawn_async(
                klights_supervisor::TaskCategory::Network,
                "test_service_routing_watch_worker_bounds_errors",
                run_service_routing_watch_worker(
                    client.clone(),
                    table,
                    notify.clone(),
                    cancel.clone(),
                    supervisor.clone(),
                    force_full_sync.clone(),
                ),
            )
            .await
            .expect("spawn watch worker under task supervisor");

        // Initial 6 opens + the first immediate Pod reconnect (attempt 0, no
        // backoff). A leaky implementation would already have opened far more
        // because each Pod stream delivers a 20-error burst.
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            client.wait_for_opened(SERVICE_ROUTING_WATCH_TARGETS.len() + 1),
        )
        .await
        .expect(
            "watch worker must open the initial watch set and reconnect the errored Pod watch once",
        );

        // Give a window for any leak / busy-loop to manifest. The buggy
        // per-watch reconnect (one new open per error, no backoff) opens
        // hundreds here; the fixed worker terminates the stream after the first
        // error and backs off (1s) before the next reconnect.
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;

        let opens = client.watches_opened.load(Ordering::SeqCst);
        assert!(
            opens <= SERVICE_ROUTING_WATCH_TARGETS.len() + 2,
            "repeated watch errors must not leak duplicate watches or busy-loop \
             reconnect; expected at most {} opens (initial set + immediate \
             reconnect), got {opens}",
            SERVICE_ROUTING_WATCH_TARGETS.len() + 2
        );

        cancel.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(2), worker.join())
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

    #[test]
    fn required_sysctls_enable_default_route_localnet_for_new_links() {
        assert!(
            REQUIRED_SERVICE_ROUTING_SYSCTLS
                .iter()
                .any(
                    |(path, value)| *path == "/proc/sys/net/ipv4/conf/default/route_localnet"
                        && *value == "1\n"
                ),
            "service routing must enable default route_localnet so hostPort loopback DNAT works on newly-created bridge links; got {REQUIRED_SERVICE_ROUTING_SYSCTLS:?}"
        );
    }

    #[test]
    fn required_sysctls_enable_bridge_route_localnet_for_hostport_loopback_dnat() {
        let entries = required_service_routing_sysctl_entries("klights0");

        assert!(
            entries.iter().any(|(path, value)| {
                path == "/proc/sys/net/ipv4/conf/klights0/route_localnet" && *value == "1\n"
            }),
            "service routing must enable route_localnet on the pod bridge for loopback hostPort DNAT; got {entries:?}"
        );
    }
}
