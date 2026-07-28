use super::model::RuntimePodSample;
use anyhow::{Context as _, anyhow};
use async_trait::async_trait;
use klights_node_api::{
    NodeMetricsError, NodeMetricsNodeSample, NodeMetricsPodSample, NodeMetricsRequest,
    NodeMetricsResult, NodeMetricsTarget,
};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

const NODE_CPU_SAMPLE_DELAY: Duration = Duration::from_millis(100);

pub(super) async fn collect_local_cri_node_metrics(
    cri: Option<Arc<tokio::sync::Mutex<crate::kubelet::cri::CriClient>>>,
    node_name: String,
    supervisor: Arc<klights_supervisor::TaskSupervisor>,
) -> Result<NodeMetricsResult, NodeMetricsError> {
    let target = NodeMetricsTarget::try_new(node_name)?;
    collect_local_cri_node_metrics_request(
        cri,
        NodeMetricsRequest::new(target, Vec::new()),
        supervisor,
    )
    .await
}

async fn collect_local_cri_node_metrics_request(
    cri: Option<Arc<tokio::sync::Mutex<crate::kubelet::cri::CriClient>>>,
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

pub(crate) struct CriNodeMetricsSampler {
    cri: Arc<tokio::sync::Mutex<crate::kubelet::cri::CriClient>>,
    supervisor: Arc<klights_supervisor::TaskSupervisor>,
}

impl CriNodeMetricsSampler {
    pub(crate) fn new(
        cri: Arc<tokio::sync::Mutex<crate::kubelet::cri::CriClient>>,
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
        .filter_map(RuntimePodSample::from_cri)
        .filter(|sample| wanted_uids.is_empty() || wanted_uids.contains(sample.uid.as_str()))
        .map(NodeMetricsPodSample::from)
        .collect();

    NodeMetricsResult::new(request.target().clone(), node, pods)
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
    let stat = crate::runtime_fs::read_utf8("/proc/stat").context("read /proc/stat")?;
    let meminfo = crate::runtime_fs::read_utf8("/proc/meminfo").context("read /proc/meminfo")?;
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
