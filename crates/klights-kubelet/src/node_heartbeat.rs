use anyhow::Result;
use futures::StreamExt as _;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeHeartbeatEvent {
    NodeChanged { node_name: String },
    Other,
    ReplayExpired,
}

pub type NodeHeartbeatEventFuture<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<NodeHeartbeatEvent>> + Send + 'a>>;

pub trait NodeHeartbeatEventSource: Send + Sync {
    fn next_node_event(&self) -> NodeHeartbeatEventFuture<'_>;
}

pub trait NodeHeartbeatClock: Send + Sync {
    fn now_microtime(&self) -> String;
}

pub struct SystemNodeHeartbeatClock {
    wall_clock: Arc<dyn klights_supervisor::WallClock>,
}

impl SystemNodeHeartbeatClock {
    pub fn new(wall_clock: Arc<dyn klights_supervisor::WallClock>) -> Self {
        Self { wall_clock }
    }
}

impl NodeHeartbeatClock for SystemNodeHeartbeatClock {
    fn now_microtime(&self) -> String {
        klights_cluster_core::k8s_time::format_microtime(self.wall_clock.now_utc())
    }
}

/// Kubelet-owned positioned Node-watch acceleration source.
pub struct LeaderNodeHeartbeatEventSource {
    leader_watch: Arc<dyn klights_leader_api::LeaderWatch>,
    watch: tokio::sync::Mutex<LeaderNodeHeartbeatWatchState>,
}

#[derive(Default)]
struct LeaderNodeHeartbeatWatchState {
    stream: Option<klights_leader_api::WatchStream>,
    cursor: Option<klights_leader_api::WatchResumeCursor>,
}

impl LeaderNodeHeartbeatEventSource {
    pub fn new(leader_watch: Arc<dyn klights_leader_api::LeaderWatch>) -> Self {
        Self {
            leader_watch,
            watch: tokio::sync::Mutex::new(LeaderNodeHeartbeatWatchState::default()),
        }
    }
}

impl NodeHeartbeatEventSource for LeaderNodeHeartbeatEventSource {
    fn next_node_event(&self) -> NodeHeartbeatEventFuture<'_> {
        Box::pin(async move {
            let mut state = self.watch.lock().await;
            if state.stream.is_none() {
                let request = klights_leader_api::WatchRequest::try_new_with_scope(
                    "v1",
                    "Node",
                    None,
                    klights_leader_api::ResourceListScope::Cluster,
                    None,
                    None,
                    None,
                    None,
                )
                .expect("Node heartbeat watch identity is valid");
                let request = if let Some(cursor) = state.cursor {
                    request.with_resume_cursor(cursor)?
                } else {
                    request
                };
                match self.leader_watch.watch_resources(request).await {
                    Ok(stream) => {
                        if let Some(cursor) = stream.accepted_cursor() {
                            state.cursor = Some(cursor);
                        }
                        state.stream = Some(stream);
                    }
                    Err(klights_leader_api::LeaderWatchError::ReplayExpired { .. }) => {
                        state.cursor = None;
                        return Ok(NodeHeartbeatEvent::ReplayExpired);
                    }
                    Err(error) => return Err(anyhow::Error::from(error)),
                }
            }
            let event = state
                .stream
                .as_mut()
                .expect("heartbeat stream initialized")
                .next()
                .await;
            match event {
                Some(Ok(event)) => {
                    let cursor = state.cursor.get_or_insert_default();
                    cursor.advance_after_apply(&event)?;
                    if matches!(
                        event.event_type(),
                        klights_leader_api::WatchEventType::Bookmark
                            | klights_leader_api::WatchEventType::Deleted
                    ) || event.resource().kind != "Node"
                    {
                        return Ok(NodeHeartbeatEvent::Other);
                    }
                    let node_name = event.resource().name.as_str();
                    if node_name.is_empty() {
                        return Ok(NodeHeartbeatEvent::Other);
                    }
                    Ok(NodeHeartbeatEvent::NodeChanged {
                        node_name: node_name.to_string(),
                    })
                }
                Some(Err(klights_leader_api::LeaderWatchError::ReplayExpired { .. })) => {
                    state.stream = None;
                    state.cursor = None;
                    Ok(NodeHeartbeatEvent::ReplayExpired)
                }
                Some(Err(error)) => {
                    state.stream = None;
                    Err(anyhow::Error::from(error))
                }
                None => {
                    state.stream = None;
                    anyhow::bail!("Node heartbeat positioned watch stream closed")
                }
            }
        })
    }
}

// Derived from the canonical node-lease cadence so the renewal timer and the
// staleness grace (GRACE = HEARTBEAT * MISSED) can never drift apart. Change
// the cadence in one place: cluster-core's node lease contract.
pub const NODE_HEARTBEAT_INTERVAL: Duration =
    Duration::from_secs(klights_cluster_core::DEFAULT_NODE_HEARTBEAT_INTERVAL_SECONDS as u64);
const NODE_HEARTBEAT_EVENT_RETRY_DELAY: Duration = Duration::from_secs(1);

/// Run the node heartbeat loop: renews the kube-node-lease every
/// NODE_HEARTBEAT_INTERVAL (and on Node watch events) via the memory-only
/// lease client (worker -> leader RPC, or the leader's local tracker). This
/// is the only production heartbeat entry point; it never writes a Lease to
/// cluster.db.
pub async fn run_heartbeat_with_lease_client(
    event_source: std::sync::Arc<dyn NodeHeartbeatEventSource>,
    lease_client: std::sync::Arc<dyn klights_leader_api::LeaderNodeLeaseRenewal>,
    clock: std::sync::Arc<dyn NodeHeartbeatClock>,
    node_name: String,
    cancel_token: tokio_util::sync::CancellationToken,
    task_supervisor: std::sync::Arc<klights_supervisor::TaskSupervisor>,
) {
    run_heartbeat_with_interval(
        event_source,
        lease_client,
        clock,
        node_name,
        cancel_token,
        task_supervisor,
        NODE_HEARTBEAT_INTERVAL,
    )
    .await;
}

pub(crate) async fn run_heartbeat_with_interval(
    event_source: std::sync::Arc<dyn NodeHeartbeatEventSource>,
    lease_client: std::sync::Arc<dyn klights_leader_api::LeaderNodeLeaseRenewal>,
    clock: std::sync::Arc<dyn NodeHeartbeatClock>,
    node_name: String,
    cancel_token: tokio_util::sync::CancellationToken,
    task_supervisor: std::sync::Arc<klights_supervisor::TaskSupervisor>,
    heartbeat_interval: Duration,
) {
    tracing::info!("Starting node heartbeat for {}", node_name);

    // Memory-only heartbeat (T6): renew via the lease client (worker -> leader
    // RPC, or the leader's local NodeLeaseTracker). This path never writes a
    // Lease to cluster.db; the dead outbox/direct-db renewal helpers were
    // removed. The watch source is retained only to drive the Node watch
    // cursor below.
    if let Err(err) =
        renew_lease_with_client(lease_client.as_ref(), clock.as_ref(), &node_name).await
    {
        tracing::warn!("Failed to send initial node heartbeat: {}", err);
    }

    let mut next_heartbeat = tokio::time::Instant::now() + heartbeat_interval;
    let mut next_event_attempt = None;
    loop {
        let delay = next_heartbeat.saturating_duration_since(tokio::time::Instant::now());
        tokio::select! {
            _ = cancel_token.cancelled() => {
                tracing::info!("Node heartbeat cancelled, shutting down");
                break;
            }
            sleep = task_supervisor.sleep("node_heartbeat_interval", delay) => {
                if let Err(err) = sleep {
                    tracing::warn!("Node heartbeat timer failed: {err:#}");
                }
                if let Err(err) =
                    renew_lease_with_client(lease_client.as_ref(), clock.as_ref(), &node_name).await
                {
                    tracing::warn!("Failed to send node heartbeat: {}", err);
                }
                next_heartbeat = tokio::time::Instant::now() + heartbeat_interval;
                tracing::debug!("Node heartbeat sent for {}", node_name);
            }
            event = next_heartbeat_event(
                event_source.as_ref(),
                task_supervisor.as_ref(),
                next_event_attempt,
            ) => {
                match event {
                    Ok(NodeHeartbeatEvent::NodeChanged { node_name: changed_node })
                        if changed_node == node_name => {
                        next_event_attempt = None;
                        if let Err(err) =
                            renew_lease_with_client(
                                lease_client.as_ref(),
                                clock.as_ref(),
                                &node_name,
                            ).await
                        {
                            tracing::warn!("Failed to send node heartbeat: {}", err);
                        }
                        next_heartbeat = tokio::time::Instant::now() + heartbeat_interval;
                        tracing::debug!("Node heartbeat sent for {}", node_name);
                    }
                    Ok(NodeHeartbeatEvent::ReplayExpired) => {
                        next_event_attempt = None;
                        tracing::warn!("Node heartbeat replay window expired; waiting for next signal");
                    }
                    Ok(_) => {
                        next_event_attempt = None;
                    }
                    Err(err) => {
                        // Node observation is only an acceleration signal for
                        // the periodic lease renewal. Leader-proxy watches
                        // intentionally close when authority changes, and
                        // transport watches can fail transiently. Neither may
                        // terminate kubelet liveness. Reconnect after a
                        // supervised one-shot delay so persistent failures
                        // remain idle-silent rather than spinning.
                        tracing::warn!(
                            retry_after = ?NODE_HEARTBEAT_EVENT_RETRY_DELAY,
                            "Node heartbeat event source failed; periodic renewal remains active: {err:#}"
                        );
                        next_event_attempt =
                            Some(tokio::time::Instant::now() + NODE_HEARTBEAT_EVENT_RETRY_DELAY);
                    }
                }
            }
        };
    }
}

#[cfg(feature = "test-support")]
pub async fn run_heartbeat_with_interval_for_integration_test(
    event_source: std::sync::Arc<dyn NodeHeartbeatEventSource>,
    lease_client: std::sync::Arc<dyn klights_leader_api::LeaderNodeLeaseRenewal>,
    clock: std::sync::Arc<dyn NodeHeartbeatClock>,
    node_name: String,
    cancel_token: tokio_util::sync::CancellationToken,
    task_supervisor: std::sync::Arc<klights_supervisor::TaskSupervisor>,
    heartbeat_interval: Duration,
) {
    run_heartbeat_with_interval(
        event_source,
        lease_client,
        clock,
        node_name,
        cancel_token,
        task_supervisor,
        heartbeat_interval,
    )
    .await;
}

async fn next_heartbeat_event(
    event_source: &dyn NodeHeartbeatEventSource,
    task_supervisor: &klights_supervisor::TaskSupervisor,
    retry_at: Option<tokio::time::Instant>,
) -> Result<NodeHeartbeatEvent> {
    if let Some(retry_at) = retry_at {
        let delay = retry_at.saturating_duration_since(tokio::time::Instant::now());
        task_supervisor
            .sleep("node_heartbeat_event_retry", delay)
            .await?;
    }
    event_source.next_node_event().await
}

async fn renew_lease_with_client(
    client: &dyn klights_leader_api::LeaderNodeLeaseRenewal,
    clock: &dyn NodeHeartbeatClock,
    node_name: &str,
) -> Result<()> {
    let request = klights_leader_api::NodeLeaseRenewalRequest::try_new(
        node_name,
        clock.now_microtime(),
        klights_cluster_core::DEFAULT_NODE_LEASE_DURATION_SECONDS,
    )?;
    client
        .renew_node_lease(request)
        .await
        .map(|_| ())
        .map_err(anyhow::Error::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    struct RecordingLeaderWatch {
        requests: Mutex<Vec<klights_leader_api::WatchRequest>>,
        events: Mutex<Option<Vec<klights_leader_api::ResourceEvent>>>,
    }

    impl RecordingLeaderWatch {
        fn with_events(events: Vec<klights_leader_api::ResourceEvent>) -> Arc<Self> {
            Arc::new(Self {
                requests: Mutex::new(Vec::new()),
                events: Mutex::new(Some(events)),
            })
        }
    }

    impl klights_leader_api::LeaderWatch for RecordingLeaderWatch {
        fn watch_resources(
            &self,
            request: klights_leader_api::WatchRequest,
        ) -> klights_leader_api::LeaderWatchFuture<'_> {
            self.requests
                .lock()
                .expect("watch request mutex")
                .push(request);
            let events = self
                .events
                .lock()
                .expect("watch event mutex")
                .take()
                .expect("heartbeat must establish only one stream");
            Box::pin(async move {
                Ok(klights_leader_api::WatchStream::unpositioned_test_stream(
                    futures::stream::iter(events.into_iter().map(Ok)),
                ))
            })
        }
    }

    fn node_event(name: &str, resource_version: i64) -> klights_leader_api::ResourceEvent {
        let resource = klights_cluster_core::Resource::try_from_data(Arc::new(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {
                "name": name,
                "resourceVersion": resource_version.to_string(),
            }
        })))
        .expect("valid Node resource");
        klights_leader_api::ResourceEvent::try_new(
            klights_leader_api::WatchEventType::Modified,
            resource,
            None,
        )
        .expect("valid Node watch event")
    }

    #[test]
    fn leader_node_event_source_and_clock_are_owned_by_kubelet() {
        fn assert_source(_: &LeaderNodeHeartbeatEventSource) {}
        fn assert_clock(_: &SystemNodeHeartbeatClock) {}
        let _ = (assert_source, assert_clock);
    }

    #[tokio::test]
    async fn leader_node_event_source_retains_one_positioned_watch_stream() {
        let leader = RecordingLeaderWatch::with_events(vec![
            node_event("node-a", 11),
            node_event("node-b", 12),
        ]);
        let source = LeaderNodeHeartbeatEventSource::new(leader.clone());

        assert_eq!(
            source.next_node_event().await.expect("first Node event"),
            NodeHeartbeatEvent::NodeChanged {
                node_name: "node-a".to_string()
            }
        );
        assert_eq!(
            source.next_node_event().await.expect("second Node event"),
            NodeHeartbeatEvent::NodeChanged {
                node_name: "node-b".to_string()
            }
        );

        let requests = leader.requests.lock().expect("watch request mutex");
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].kind(), "Node");
        assert_eq!(
            requests[0].scope(),
            &klights_leader_api::ResourceListScope::Cluster
        );
        assert_eq!(requests[0].start_resource_version(), None);
        assert_eq!(requests[0].start_watch_replay_position(), None);
    }

    struct FixedClock;

    impl NodeHeartbeatClock for FixedClock {
        fn now_microtime(&self) -> String {
            "2026-07-27T12:34:56.123456Z".to_string()
        }
    }

    struct ChannelEventSource {
        events: tokio::sync::Mutex<tokio::sync::mpsc::Receiver<anyhow::Result<NodeHeartbeatEvent>>>,
    }

    impl NodeHeartbeatEventSource for ChannelEventSource {
        fn next_node_event(&self) -> NodeHeartbeatEventFuture<'_> {
            Box::pin(async move {
                self.events
                    .lock()
                    .await
                    .recv()
                    .await
                    .unwrap_or_else(|| anyhow::bail!("event source closed"))
            })
        }
    }

    struct RecordingLeaseClient {
        requests: Mutex<Vec<(String, String)>>,
        count: tokio::sync::watch::Sender<usize>,
    }

    impl RecordingLeaseClient {
        fn new() -> (Arc<Self>, tokio::sync::watch::Receiver<usize>) {
            let (count, receiver) = tokio::sync::watch::channel(0);
            (
                Arc::new(Self {
                    requests: Mutex::new(Vec::new()),
                    count,
                }),
                receiver,
            )
        }
    }

    impl klights_leader_api::LeaderNodeLeaseRenewal for RecordingLeaseClient {
        fn renew_node_lease(
            &self,
            request: klights_leader_api::NodeLeaseRenewalRequest,
        ) -> klights_leader_api::NodeLeaseRenewalFuture<
            '_,
            klights_leader_api::NodeLeaseRenewalResult,
        > {
            let mut requests = self.requests.lock().expect("request lock");
            requests.push((
                request.node_name().to_string(),
                request.renew_time().to_string(),
            ));
            self.count.send_replace(requests.len());
            Box::pin(async { Ok(klights_leader_api::NodeLeaseRenewalResult::Renewed) })
        }
    }

    fn supervisor() -> Arc<klights_supervisor::TaskSupervisor> {
        Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ))
    }

    #[tokio::test]
    async fn matching_node_event_renews_with_injected_clock_timestamp() {
        let (event_tx, event_rx) = tokio::sync::mpsc::channel(1);
        event_tx
            .send(Ok(NodeHeartbeatEvent::NodeChanged {
                node_name: "worker-a".to_string(),
            }))
            .await
            .expect("queue node event");
        let source = Arc::new(ChannelEventSource {
            events: tokio::sync::Mutex::new(event_rx),
        });
        let (client, mut count) = RecordingLeaseClient::new();
        let cancel = tokio_util::sync::CancellationToken::new();
        let task = tokio::spawn(run_heartbeat_with_interval(
            source,
            client.clone(),
            Arc::new(FixedClock),
            "worker-a".to_string(),
            cancel.clone(),
            supervisor(),
            Duration::from_secs(60),
        ));

        count
            .wait_for(|value| *value >= 2)
            .await
            .expect("initial and event renewals");
        cancel.cancel();
        task.await.expect("heartbeat task");

        let requests = client.requests.lock().expect("request lock");
        assert_eq!(requests.len(), 2);
        assert!(requests.iter().all(|(node, timestamp)| {
            node == "worker-a" && timestamp == "2026-07-27T12:34:56.123456Z"
        }));
    }

    #[tokio::test(start_paused = true)]
    async fn leadership_watch_closure_does_not_stop_controlplane_renewal() {
        let (event_tx, event_rx) = tokio::sync::mpsc::channel(1);
        event_tx
            .send(Err(anyhow::anyhow!(
                "leadership-switching Node watch closed"
            )))
            .await
            .expect("queue source error");
        let source = Arc::new(ChannelEventSource {
            events: tokio::sync::Mutex::new(event_rx),
        });
        let (client, count) = RecordingLeaseClient::new();
        let cancel = tokio_util::sync::CancellationToken::new();
        let task = tokio::spawn(run_heartbeat_with_interval(
            source,
            client.clone(),
            Arc::new(FixedClock),
            "controlplane-a".to_string(),
            cancel.clone(),
            supervisor(),
            Duration::from_secs(10),
        ));

        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(10)).await;
        tokio::task::yield_now().await;
        assert!(
            *count.borrow() >= 2,
            "periodic heartbeat must survive the leadership watch closure"
        );
        cancel.cancel();
        task.await.expect("heartbeat task");

        assert!(
            client.requests.lock().expect("request lock").len() >= 2,
            "initial and periodic renewals must both reach the leader capability"
        );
    }
}
