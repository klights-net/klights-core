use klights_node_api::{
    NodeMetrics, NodeMetricsError, NodeMetricsFuture, NodeMetricsRequest, NodeMetricsResult,
    NodeMetricsSampler,
};
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::{Mutex, watch};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct RequestKey {
    node_name: String,
    pod_uids: Vec<String>,
}

impl From<&NodeMetricsRequest> for RequestKey {
    fn from(request: &NodeMetricsRequest) -> Self {
        Self {
            node_name: request.target().node_name().to_string(),
            pod_uids: request.pod_uids().to_vec(),
        }
    }
}

type ResponseWatch = watch::Receiver<Option<Result<NodeMetricsResult, NodeMetricsError>>>;

#[derive(Default)]
struct RequestCoalescer {
    in_flight: Mutex<HashMap<RequestKey, ResponseWatch>>,
}

impl RequestCoalescer {
    async fn get_or_spawn<F>(
        self: &Arc<Self>,
        key: RequestKey,
        supervisor: Arc<klights_supervisor::TaskSupervisor>,
        fetch: F,
    ) -> Result<NodeMetricsResult, NodeMetricsError>
    where
        F: Future<Output = Result<NodeMetricsResult, NodeMetricsError>> + Send + 'static,
    {
        let (receiver, should_spawn, sender) = {
            let mut in_flight = self.in_flight.lock().await;
            if let Some(receiver) = in_flight.get(&key) {
                (receiver.clone(), false, None)
            } else {
                let (sender, receiver) = watch::channel(None);
                in_flight.insert(key.clone(), receiver.clone());
                (receiver, true, Some(sender))
            }
        };

        if should_spawn {
            let coalescer = self.clone();
            let cleanup_key = key.clone();
            let sender = sender.expect("sender exists for newly spawned metrics request");
            if let Err(error) = supervisor
                .spawn_async(
                    klights_supervisor::TaskCategory::Network,
                    "metrics_node_runtime_sample",
                    async move {
                        let response = fetch.await;
                        let _ = sender.send(Some(response));
                        coalescer.in_flight.lock().await.remove(&cleanup_key);
                    },
                )
                .await
            {
                self.in_flight.lock().await.remove(&key);
                return Err(NodeMetricsError::unavailable(format!(
                    "failed to spawn node metrics request for '{}': {error:#}",
                    key.node_name
                )));
            }
        }

        await_response(receiver, key.node_name).await
    }
}

async fn await_response(
    mut receiver: ResponseWatch,
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

/// Root-owned routing adapter. It is the sole place that selects between a
/// kubelet-local sampler and a remote node transport.
pub(crate) struct RootNodeMetrics {
    local_node_name: String,
    local_sampler: Option<Arc<dyn NodeMetricsSampler>>,
    remote: Option<Arc<dyn NodeMetrics>>,
    supervisor: Arc<klights_supervisor::TaskSupervisor>,
    coalescer: Arc<RequestCoalescer>,
}

impl RootNodeMetrics {
    pub(crate) fn new(
        local_node_name: String,
        local_sampler: Option<Arc<dyn NodeMetricsSampler>>,
        remote: Option<Arc<dyn NodeMetrics>>,
        supervisor: Arc<klights_supervisor::TaskSupervisor>,
    ) -> Self {
        Self {
            local_node_name,
            local_sampler,
            remote,
            supervisor,
            coalescer: Arc::new(RequestCoalescer::default()),
        }
    }
}

impl NodeMetrics for RootNodeMetrics {
    fn collect_metrics(
        &self,
        request: NodeMetricsRequest,
    ) -> NodeMetricsFuture<'_, NodeMetricsResult> {
        Box::pin(async move {
            let key = RequestKey::from(&request);
            let local = request.target().node_name() == self.local_node_name;
            let local_sampler = self.local_sampler.clone();
            let remote = self.remote.clone();
            let supervisor = self.supervisor.clone();
            self.coalescer
                .get_or_spawn(key, supervisor.clone(), async move {
                    if local {
                        match local_sampler {
                            Some(sampler) => sampler.sample_metrics(request).await,
                            None => Err(NodeMetricsError::unavailable(
                                "local node metrics sampler is not available",
                            )),
                        }
                    } else {
                        match remote {
                            Some(remote) => remote.collect_metrics(request).await,
                            None => Err(NodeMetricsError::unavailable(
                                "remote node metrics transport is not available",
                            )),
                        }
                    }
                })
                .await
        })
    }
}

#[cfg(test)]
pub(crate) struct UnavailableNodeMetrics;

#[cfg(test)]
impl NodeMetrics for UnavailableNodeMetrics {
    fn collect_metrics(
        &self,
        _request: NodeMetricsRequest,
    ) -> NodeMetricsFuture<'_, NodeMetricsResult> {
        Box::pin(async {
            Err(NodeMetricsError::unavailable(
                "node metrics are not configured for this test",
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingSampler {
        calls: Arc<AtomicUsize>,
        started: Arc<tokio::sync::Barrier>,
        release: Arc<tokio::sync::Notify>,
    }

    impl NodeMetricsSampler for CountingSampler {
        fn sample_metrics(
            &self,
            request: NodeMetricsRequest,
        ) -> NodeMetricsFuture<'_, NodeMetricsResult> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                self.started.wait().await;
                self.release.notified().await;
                Ok(NodeMetricsResult::new(
                    request.target().clone(),
                    None,
                    Vec::new(),
                ))
            })
        }
    }

    #[tokio::test]
    async fn coalesces_identical_local_requests() {
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let calls = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(tokio::sync::Barrier::new(2));
        let release = Arc::new(tokio::sync::Notify::new());
        let metrics = Arc::new(RootNodeMetrics::new(
            "node-a".to_string(),
            Some(Arc::new(CountingSampler {
                calls: calls.clone(),
                started: started.clone(),
                release: release.clone(),
            })),
            None,
            supervisor.clone(),
        ));
        let request = || {
            NodeMetricsRequest::new(
                klights_node_api::NodeMetricsTarget::try_new("node-a").unwrap(),
                Vec::new(),
            )
        };

        let first = {
            let metrics = metrics.clone();
            tokio::spawn(async move { metrics.collect_metrics(request()).await })
        };
        started.wait().await;
        let second = {
            let metrics = metrics.clone();
            tokio::spawn(async move { metrics.collect_metrics(request()).await })
        };
        release.notify_waiters();

        let (first, second) = tokio::join!(first, second);
        assert!(first.unwrap().is_ok());
        assert!(second.unwrap().is_ok());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let _ = supervisor.shutdown(std::time::Duration::from_secs(1)).await;
    }
}
