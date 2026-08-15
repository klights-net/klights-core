use anyhow::{Context as _, anyhow};
use async_trait::async_trait;
use klights_node_api::{
    NodeMetrics, NodeMetricsContainerSample, NodeMetricsError, NodeMetricsFuture,
    NodeMetricsNodeSample, NodeMetricsPodSample, NodeMetricsRequest, NodeMetricsResult,
    NodeMetricsSampler as NodeApiMetricsSampler,
};
use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, watch};

const NODE_CPU_SAMPLE_DELAY: Duration = Duration::from_millis(100);

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct MetricsRequestKey {
    node_name: String,
    pod_uids: Vec<String>,
}

impl From<&NodeMetricsRequest> for MetricsRequestKey {
    fn from(request: &NodeMetricsRequest) -> Self {
        Self {
            node_name: request.target().node_name().to_string(),
            pod_uids: request.pod_uids().to_vec(),
        }
    }
}

type MetricsResponseWatch = watch::Receiver<Option<Result<NodeMetricsResult, NodeMetricsError>>>;

#[derive(Default)]
struct MetricsRequestCoalescer {
    in_flight: Mutex<HashMap<MetricsRequestKey, MetricsResponseWatch>>,
}

impl MetricsRequestCoalescer {
    async fn collect<F>(
        self: &Arc<Self>,
        key: MetricsRequestKey,
        supervisor: Arc<klights_supervisor::TaskSupervisor>,
        fetch: F,
    ) -> Result<NodeMetricsResult, NodeMetricsError>
    where
        F: Future<Output = Result<NodeMetricsResult, NodeMetricsError>> + Send + 'static,
    {
        let (mut receiver, spawn, sender) = {
            let mut in_flight = self.in_flight.lock().await;
            if let Some(rx) = in_flight.get(&key) {
                (rx.clone(), false, None)
            } else {
                let (tx, rx) = watch::channel(None);
                in_flight.insert(key.clone(), rx.clone());
                (rx, true, Some(tx))
            }
        };
        if spawn {
            let coalescer = self.clone();
            let cleanup = key.clone();
            let sender = sender.expect("new request sender");
            if let Err(error) = supervisor
                .spawn_async(
                    klights_supervisor::TaskCategory::Network,
                    "metrics_node_runtime_sample",
                    async move {
                        let result = fetch.await;
                        let _ = sender.send(Some(result));
                        coalescer.in_flight.lock().await.remove(&cleanup);
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
        loop {
            if let Some(result) = receiver.borrow().clone() {
                return result;
            }
            if receiver.changed().await.is_err() {
                self.in_flight.lock().await.remove(&key);
                return Err(NodeMetricsError::closed(format!(
                    "node '{}' metrics request closed before response",
                    key.node_name
                )));
            }
        }
    }
}

/// Kubelet-owned local/remote metrics routing and identical-request coalescing.
pub struct RoutedNodeMetrics {
    local_node_name: String,
    local_sampler: Option<Arc<dyn NodeApiMetricsSampler>>,
    remote: Option<Arc<dyn NodeMetrics>>,
    supervisor: Arc<klights_supervisor::TaskSupervisor>,
    coalescer: Arc<MetricsRequestCoalescer>,
}

impl RoutedNodeMetrics {
    pub fn new(
        local_node_name: String,
        local_sampler: Option<Arc<dyn NodeApiMetricsSampler>>,
        remote: Option<Arc<dyn NodeMetrics>>,
        supervisor: Arc<klights_supervisor::TaskSupervisor>,
    ) -> Self {
        Self {
            local_node_name,
            local_sampler,
            remote,
            supervisor,
            coalescer: Arc::new(MetricsRequestCoalescer::default()),
        }
    }
}

impl NodeMetrics for RoutedNodeMetrics {
    fn collect_metrics(
        &self,
        request: NodeMetricsRequest,
    ) -> NodeMetricsFuture<'_, NodeMetricsResult> {
        Box::pin(async move {
            let key = MetricsRequestKey::from(&request);
            let local = request.target().node_name() == self.local_node_name;
            let sampler = self.local_sampler.clone();
            let remote = self.remote.clone();
            self.coalescer
                .collect(key, self.supervisor.clone(), async move {
                    if local {
                        match sampler {
                            Some(s) => s.sample_metrics(request).await,
                            None => Err(NodeMetricsError::unavailable(
                                "local node metrics sampler is not available",
                            )),
                        }
                    } else {
                        match remote {
                            Some(r) => r.collect_metrics(request).await,
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

async fn collect_local_cri_node_metrics_request(
    cri: Option<Arc<tokio::sync::Mutex<crate::cri::CriClient>>>,
    request: NodeMetricsRequest,
    supervisor: Arc<klights_supervisor::TaskSupervisor>,
) -> Result<NodeMetricsResult, NodeMetricsError> {
    let target = request.target().clone();
    let node = match LinuxProcNodeMetricsSampler::new(supervisor)
        .sample_node()
        .await
    {
        Ok(sample) => Some(sample),
        Err(error) => {
            tracing::debug!(%error, "node resource metrics unavailable");
            None
        }
    };
    let Some(cri) = cri else {
        if let Some(node) = node {
            return Ok(NodeMetricsResult::new(target, Some(node), Vec::new()));
        }
        return Err(NodeMetricsError::unavailable(
            "CRI client is not available for local node metrics",
        ));
    };
    let mut client = {
        let guard = cri.lock().await;
        guard.clone()
    };
    match client.list_pod_sandbox_stats(None).await {
        Ok(stats) => Ok(node_metrics_result_from_pod_sandbox_stats(
            &request, node, stats,
        )),
        Err(error) => {
            tracing::debug!(%error, "CRI pod sandbox metrics unavailable");
            if let Some(node) = node {
                Ok(NodeMetricsResult::new(target, Some(node), Vec::new()))
            } else {
                Err(NodeMetricsError::unavailable(format!("{error:#}")))
            }
        }
    }
}

pub struct CriNodeMetricsSampler {
    cri: Arc<tokio::sync::Mutex<crate::cri::CriClient>>,
    supervisor: Arc<klights_supervisor::TaskSupervisor>,
}

impl CriNodeMetricsSampler {
    pub fn new(
        cri: Arc<tokio::sync::Mutex<crate::cri::CriClient>>,
        supervisor: Arc<klights_supervisor::TaskSupervisor>,
    ) -> Self {
        Self { cri, supervisor }
    }
}

impl klights_node_api::NodeMetricsSampler for CriNodeMetricsSampler {
    fn sample_metrics(
        &self,
        request: NodeMetricsRequest,
    ) -> klights_node_api::NodeMetricsFuture<'_, NodeMetricsResult> {
        Box::pin(async move {
            collect_local_cri_node_metrics_request(
                Some(self.cri.clone()),
                request,
                self.supervisor.clone(),
            )
            .await
        })
    }
}

fn node_metrics_result_from_pod_sandbox_stats(
    request: &NodeMetricsRequest,
    node: Option<NodeMetricsNodeSample>,
    stats: Vec<k8s_cri::v1::PodSandboxStats>,
) -> NodeMetricsResult {
    let wanted_uids: BTreeSet<&str> = request
        .pod_uids()
        .iter()
        .map(String::as_str)
        .filter(|uid| !uid.is_empty())
        .collect();
    let pods = stats
        .into_iter()
        .filter_map(node_metrics_pod_sample_from_cri)
        .filter(|sample| wanted_uids.is_empty() || wanted_uids.contains(sample.uid()))
        .collect();

    NodeMetricsResult::new(request.target().clone(), node, pods)
}

fn node_metrics_pod_sample_from_cri(
    stats: k8s_cri::v1::PodSandboxStats,
) -> Option<NodeMetricsPodSample> {
    let attributes = stats.attributes?;
    let metadata = attributes.metadata?;
    let containers = stats
        .linux
        .into_iter()
        .flat_map(|linux| linux.containers)
        .filter_map(|container| {
            let name = container
                .attributes
                .as_ref()
                .and_then(|attrs| attrs.metadata.as_ref())
                .map(|metadata| metadata.name.as_str())
                .filter(|name| !name.is_empty())?;
            let cpu_nanos = container
                .cpu
                .as_ref()
                .and_then(|cpu| cpu.usage_nano_cores)
                .map(|value| value.value)?;
            let memory_bytes = container
                .memory
                .as_ref()
                .and_then(|memory| memory.working_set_bytes.or(memory.usage_bytes))
                .map(|value| value.value)?;
            Some(NodeMetricsContainerSample::new(
                name,
                cpu_nanos,
                memory_bytes,
            ))
        })
        .collect();
    Some(NodeMetricsPodSample::new(
        metadata.namespace,
        metadata.name,
        metadata.uid,
        containers,
    ))
}

#[async_trait]
pub trait NodeMetricsSampler: Send + Sync {
    async fn sample_node(&self) -> anyhow::Result<NodeMetricsNodeSample>;
}

pub struct LinuxProcNodeMetricsSampler {
    supervisor: Arc<klights_supervisor::TaskSupervisor>,
}

impl LinuxProcNodeMetricsSampler {
    pub fn new(supervisor: Arc<klights_supervisor::TaskSupervisor>) -> Self {
        Self { supervisor }
    }
}

#[async_trait]
impl NodeMetricsSampler for LinuxProcNodeMetricsSampler {
    async fn sample_node(&self) -> anyhow::Result<NodeMetricsNodeSample> {
        let first = read_proc_node_usage(self.supervisor.clone()).await?;
        self.supervisor
            .sleep("node_metrics_cpu_sample_delay", NODE_CPU_SAMPLE_DELAY)
            .await?;
        let second = read_proc_node_usage(self.supervisor.clone()).await?;
        let elapsed_jiffies = second
            .cpu_total_jiffies
            .checked_sub(first.cpu_total_jiffies)
            .ok_or_else(|| anyhow!("node CPU counter moved backwards"))?;
        let idle_jiffies = second
            .cpu_idle_jiffies
            .checked_sub(first.cpu_idle_jiffies)
            .ok_or_else(|| anyhow!("node idle CPU counter moved backwards"))?;
        let active_jiffies = elapsed_jiffies.saturating_sub(idle_jiffies);
        let cpu_nanos = if elapsed_jiffies == 0 {
            0
        } else {
            active_jiffies
                .saturating_mul(1_000_000_000)
                .div_ceil(elapsed_jiffies)
        };

        Ok(NodeMetricsNodeSample::new(
            cpu_nanos,
            second.memory_used_bytes,
        ))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ProcNodeUsage {
    pub(super) cpu_total_jiffies: u64,
    pub(super) cpu_idle_jiffies: u64,
    pub(super) memory_used_bytes: u64,
}

async fn read_proc_node_usage(
    supervisor: Arc<klights_supervisor::TaskSupervisor>,
) -> anyhow::Result<ProcNodeUsage> {
    supervisor
        .run_blocking_file("node_metrics_read_proc", read_proc_node_usage_blocking)
        .await?
}

fn read_proc_node_usage_blocking() -> anyhow::Result<ProcNodeUsage> {
    let stat =
        klights_supervisor::runtime_fs::read_utf8("/proc/stat").context("read /proc/stat")?;
    let meminfo =
        klights_supervisor::runtime_fs::read_utf8("/proc/meminfo").context("read /proc/meminfo")?;
    parse_proc_node_usage(&stat, &meminfo)
}

pub(super) fn parse_proc_node_usage(stat: &str, meminfo: &str) -> anyhow::Result<ProcNodeUsage> {
    let cpu_line = stat
        .lines()
        .find(|line| line.starts_with("cpu "))
        .ok_or_else(|| anyhow!("missing aggregate cpu line in /proc/stat"))?;
    let fields: Vec<u64> = cpu_line
        .split_whitespace()
        .skip(1)
        .map(|field| {
            field
                .parse::<u64>()
                .with_context(|| format!("invalid /proc/stat cpu field '{field}'"))
        })
        .collect::<anyhow::Result<_>>()?;
    if fields.len() < 5 {
        return Err(anyhow!("aggregate cpu line has too few fields"));
    }
    let idle_jiffies = fields[3].saturating_add(fields[4]);
    let total_jiffies = fields
        .iter()
        .copied()
        .fold(0_u64, |total, value| total.saturating_add(value));

    let mem_total = parse_meminfo_kib(meminfo, "MemTotal")?;
    let mem_available = parse_meminfo_kib(meminfo, "MemAvailable")?;
    let memory_used_bytes = mem_total.saturating_sub(mem_available).saturating_mul(1024);

    Ok(ProcNodeUsage {
        cpu_total_jiffies: total_jiffies,
        cpu_idle_jiffies: idle_jiffies,
        memory_used_bytes,
    })
}

fn parse_meminfo_kib(meminfo: &str, key: &str) -> anyhow::Result<u64> {
    let prefix = format!("{key}:");
    let line = meminfo
        .lines()
        .find(|line| line.starts_with(&prefix))
        .ok_or_else(|| anyhow!("missing {key} in /proc/meminfo"))?;
    line.split_whitespace()
        .nth(1)
        .ok_or_else(|| anyhow!("missing {key} value in /proc/meminfo"))?
        .parse::<u64>()
        .with_context(|| format!("invalid {key} value in /proc/meminfo"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn converts_cri_pod_stats_to_transport_neutral_sample() {
        let stats = k8s_cri::v1::PodSandboxStats {
            attributes: Some(k8s_cri::v1::PodSandboxAttributes {
                metadata: Some(k8s_cri::v1::PodSandboxMetadata {
                    name: "pod-a".to_string(),
                    namespace: "default".to_string(),
                    uid: "uid-a".to_string(),
                    attempt: 0,
                }),
                ..Default::default()
            }),
            linux: Some(k8s_cri::v1::LinuxPodSandboxStats {
                containers: vec![k8s_cri::v1::ContainerStats {
                    attributes: Some(k8s_cri::v1::ContainerAttributes {
                        metadata: Some(k8s_cri::v1::ContainerMetadata {
                            name: "app".to_string(),
                            attempt: 0,
                        }),
                        ..Default::default()
                    }),
                    cpu: Some(k8s_cri::v1::CpuUsage {
                        usage_nano_cores: Some(k8s_cri::v1::UInt64Value { value: 123_000_000 }),
                        ..Default::default()
                    }),
                    memory: Some(k8s_cri::v1::MemoryUsage {
                        working_set_bytes: Some(k8s_cri::v1::UInt64Value {
                            value: 9 * 1024 * 1024,
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        };

        let sample = node_metrics_pod_sample_from_cri(stats).unwrap();
        assert_eq!(sample.uid(), "uid-a");
        assert_eq!(sample.containers()[0].name(), "app");
        assert_eq!(sample.containers()[0].cpu_nanos(), 123_000_000);
        assert_eq!(sample.containers()[0].memory_bytes(), 9 * 1024 * 1024);
    }

    #[test]
    fn proc_node_usage_parser_reports_used_memory_and_cpu_counters() {
        let usage = parse_proc_node_usage(
            "cpu 10 20 30 100 5 2 3 0 0 0\ncpu0 1 2 3 4 5 6 7 0 0 0\n",
            "MemTotal:       1024 kB\nMemAvailable:    256 kB\n",
        )
        .unwrap();

        assert_eq!(usage.cpu_total_jiffies, 170);
        assert_eq!(usage.cpu_idle_jiffies, 105);
        assert_eq!(usage.memory_used_bytes, 768 * 1024);
    }

    struct CountingSampler(Arc<AtomicUsize>);
    impl klights_node_api::NodeMetricsSampler for CountingSampler {
        fn sample_metrics(
            &self,
            request: NodeMetricsRequest,
        ) -> NodeMetricsFuture<'_, NodeMetricsResult> {
            let calls = self.0.clone();
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(NodeMetricsResult::new(
                    request.target().clone(),
                    None,
                    Vec::new(),
                ))
            })
        }
    }

    struct BlockingSampler {
        calls: Arc<AtomicUsize>,
        started: Arc<tokio::sync::Barrier>,
        release: Arc<tokio::sync::Notify>,
    }

    impl klights_node_api::NodeMetricsSampler for BlockingSampler {
        fn sample_metrics(
            &self,
            request: NodeMetricsRequest,
        ) -> NodeMetricsFuture<'_, NodeMetricsResult> {
            let calls = self.calls.clone();
            let started = self.started.clone();
            let release = self.release.clone();
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                started.wait().await;
                release.notified().await;
                Ok(NodeMetricsResult::new(
                    request.target().clone(),
                    None,
                    Vec::new(),
                ))
            })
        }
    }
    struct CountingRemote(Arc<AtomicUsize>);
    impl NodeMetrics for CountingRemote {
        fn collect_metrics(
            &self,
            request: NodeMetricsRequest,
        ) -> NodeMetricsFuture<'_, NodeMetricsResult> {
            let calls = self.0.clone();
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(NodeMetricsResult::new(
                    request.target().clone(),
                    None,
                    Vec::new(),
                ))
            })
        }
    }
    fn request(node: &str, pods: Vec<&str>) -> NodeMetricsRequest {
        NodeMetricsRequest::new(
            klights_node_api::NodeMetricsTarget::try_new(node).unwrap(),
            pods.into_iter().map(str::to_string).collect(),
        )
    }
    #[tokio::test]
    async fn routed_metrics_coalesces_identical_and_retries_after_completion() {
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let calls = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(tokio::sync::Barrier::new(2));
        let release = Arc::new(tokio::sync::Notify::new());
        let metrics = Arc::new(RoutedNodeMetrics::new(
            "local".into(),
            Some(Arc::new(BlockingSampler {
                calls: calls.clone(),
                started: started.clone(),
                release: release.clone(),
            })),
            None,
            supervisor.clone(),
        ));
        let first = {
            let metrics = metrics.clone();
            tokio::spawn(async move { metrics.collect_metrics(request("local", vec!["p"])).await })
        };
        started.wait().await;
        let second = {
            let metrics = metrics.clone();
            tokio::spawn(async move { metrics.collect_metrics(request("local", vec!["p"])).await })
        };
        release.notify_waiters();
        let (a, b) = tokio::join!(first, second);
        assert!(a.unwrap().is_ok() && b.unwrap().is_ok());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let retry = tokio::spawn({
            let metrics = metrics.clone();
            async move { metrics.collect_metrics(request("local", vec!["p"])).await }
        });
        started.wait().await;
        release.notify_waiters();
        assert!(retry.await.unwrap().is_ok());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        let _ = supervisor.shutdown(Duration::from_secs(1)).await;
    }
    #[tokio::test]
    async fn routed_metrics_routes_distinct_local_remote_and_unavailable_requests() {
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let local = Arc::new(AtomicUsize::new(0));
        let remote = Arc::new(AtomicUsize::new(0));
        let metrics = RoutedNodeMetrics::new(
            "local".into(),
            Some(Arc::new(CountingSampler(local.clone()))),
            Some(Arc::new(CountingRemote(remote.clone()))),
            supervisor.clone(),
        );
        assert!(
            metrics
                .collect_metrics(request("local", vec!["a"]))
                .await
                .is_ok()
        );
        assert!(
            metrics
                .collect_metrics(request("remote", vec!["a"]))
                .await
                .is_ok()
        );
        assert!(
            metrics
                .collect_metrics(request("local", vec!["b"]))
                .await
                .is_ok()
        );
        assert_eq!(
            (local.load(Ordering::SeqCst), remote.load(Ordering::SeqCst)),
            (2, 1)
        );
        let none = RoutedNodeMetrics::new("local".into(), None, None, supervisor.clone());
        assert!(
            none.collect_metrics(request("local", vec![]))
                .await
                .is_err()
        );
        assert!(
            none.collect_metrics(request("remote", vec![]))
                .await
                .is_err()
        );
        let _ = supervisor.shutdown(Duration::from_secs(1)).await;
    }
}
