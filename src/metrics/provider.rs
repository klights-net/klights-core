use super::model::RuntimeMetricsSnapshot;
use super::sampling::collect_local_cri_node_metrics;
use crate::datastore::Resource;
use async_trait::async_trait;
use klights_node_api::{
    NodeMetrics, NodeMetricsError, NodeMetricsRequest, NodeMetricsResult, NodeMetricsTarget,
};
use serde_json::Value;
use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::sync::Arc;
use tokio::sync::{Mutex, watch};

#[async_trait]
pub trait MetricsProvider: Send + Sync {
    async fn runtime_snapshot_for_pods(&self, pods: &[Resource]) -> RuntimeMetricsSnapshot;
}

#[derive(Default)]
pub struct FallbackOnlyMetricsProvider;

#[async_trait]
impl MetricsProvider for FallbackOnlyMetricsProvider {
    async fn runtime_snapshot_for_pods(&self, _pods: &[Resource]) -> RuntimeMetricsSnapshot {
        RuntimeMetricsSnapshot::default()
    }
}

#[derive(Default)]
pub(super) struct NodeMetricsRequestCoalescer {
    in_flight: Mutex<HashMap<String, NodeMetricsResponseWatch>>,
}

type NodeMetricsResponseWatch =
    watch::Receiver<Option<Result<NodeMetricsResult, NodeMetricsError>>>;

impl NodeMetricsRequestCoalescer {
    pub(super) async fn get_or_spawn<F>(
        self: &Arc<Self>,
        node_name: String,
        supervisor: Arc<klights_supervisor::TaskSupervisor>,
        fetch: F,
    ) -> Result<NodeMetricsResult, NodeMetricsError>
    where
        F: Future<Output = Result<NodeMetricsResult, NodeMetricsError>> + Send + 'static,
    {
        let (receiver, should_spawn, sender) = {
            let mut in_flight = self.in_flight.lock().await;
            if let Some(receiver) = in_flight.get(&node_name) {
                (receiver.clone(), false, None)
            } else {
                let (sender, receiver) = watch::channel(None);
                in_flight.insert(node_name.clone(), receiver.clone());
                (receiver, true, Some(sender))
            }
        };

        if should_spawn {
            let coalescer = self.clone();
            let cleanup_node = node_name.clone();
            let sender = sender.expect("sender exists for newly spawned metrics request");
            let spawn_result = supervisor
                .spawn_async(
                    klights_supervisor::TaskCategory::Network,
                    "metrics_node_runtime_sample",
                    async move {
                        let response = fetch.await;
                        let _ = sender.send(Some(response));
                        coalescer.in_flight.lock().await.remove(&cleanup_node);
                    },
                )
                .await;
            if let Err(error) = spawn_result {
                self.in_flight.lock().await.remove(&node_name);
                return Err(NodeMetricsError::unavailable(format!(
                    "failed to spawn node metrics request for '{node_name}': {error:#}"
                )));
            }
        }

        await_node_metrics_response(receiver, node_name).await
    }
}

async fn await_node_metrics_response(
    mut receiver: watch::Receiver<Option<Result<NodeMetricsResult, NodeMetricsError>>>,
    node_name: String,
) -> Result<NodeMetricsResult, NodeMetricsError> {
    loop {
        if let Some(response) = receiver.borrow().clone() {
            return response;
        }
        if receiver.changed().await.is_err() {
            return Err(NodeMetricsError::closed(format!(
                "node '{node_name}' metrics request closed before response"
            )));
        }
    }
}

pub struct OnDemandMetricsProvider {
    local_node_name: String,
    cri: Option<Arc<tokio::sync::Mutex<crate::kubelet::cri::CriClient>>>,
    node_metrics: Option<Arc<dyn NodeMetrics>>,
    supervisor: Arc<klights_supervisor::TaskSupervisor>,
    coalescer: Arc<NodeMetricsRequestCoalescer>,
}

impl OnDemandMetricsProvider {
    pub fn new(
        local_node_name: String,
        cri: Option<Arc<tokio::sync::Mutex<crate::kubelet::cri::CriClient>>>,
        node_metrics: Option<Arc<dyn NodeMetrics>>,
        supervisor: Arc<klights_supervisor::TaskSupervisor>,
    ) -> Self {
        Self {
            local_node_name,
            cri,
            node_metrics,
            supervisor,
            coalescer: Arc::new(NodeMetricsRequestCoalescer::default()),
        }
    }

    async fn collect_node_metrics(
        &self,
        node_name: String,
    ) -> Result<NodeMetricsResult, NodeMetricsError> {
        let local_node_name = self.local_node_name.clone();
        let cri = self.cri.clone();
        let node_metrics = self.node_metrics.clone();
        let request_node_name = node_name.clone();
        let supervisor = self.supervisor.clone();
        let fetch_supervisor = supervisor.clone();
        self.coalescer
            .get_or_spawn(node_name.clone(), supervisor, async move {
                if request_node_name == local_node_name {
                    collect_local_cri_node_metrics(cri, request_node_name, fetch_supervisor).await
                } else if let Some(node_metrics) = node_metrics {
                    let target = NodeMetricsTarget::try_new(request_node_name)?;
                    node_metrics
                        .collect_metrics(NodeMetricsRequest::new(target, Vec::new()))
                        .await
                } else {
                    Err(NodeMetricsError::unavailable(
                        "replication service is not available for remote node metrics",
                    ))
                }
            })
            .await
    }
}

#[async_trait]
impl MetricsProvider for OnDemandMetricsProvider {
    async fn runtime_snapshot_for_pods(&self, pods: &[Resource]) -> RuntimeMetricsSnapshot {
        let nodes = metric_nodes_for_pods(pods);
        if nodes.is_empty() {
            return RuntimeMetricsSnapshot::default();
        }

        let responses = futures::future::join_all(
            nodes
                .into_iter()
                .map(|node| self.collect_node_metrics(node)),
        )
        .await;
        RuntimeMetricsSnapshot::from_node_metrics_results(responses)
    }
}

fn metric_nodes_for_pods(pods: &[Resource]) -> Vec<String> {
    pods.iter()
        .filter(|pod| !pod_is_terminal(pod.data.as_ref()))
        .filter_map(|pod| {
            pod.data
                .pointer("/spec/nodeName")
                .and_then(Value::as_str)
                .filter(|node| !node.is_empty())
                .map(str::to_string)
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn pod_is_terminal(pod: &Value) -> bool {
    matches!(
        pod.pointer("/status/phase").and_then(Value::as_str),
        Some("Succeeded" | "Failed")
    )
}
