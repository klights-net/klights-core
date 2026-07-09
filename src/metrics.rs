use crate::datastore::Resource;
use crate::kubelet::pod_resources::{parse_cpu_resource, parse_memory_resource};
use anyhow::{Context as _, anyhow};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, watch};

pub const METRICS_API_VERSION: &str = "metrics.k8s.io/v1beta1";
pub const METRICS_WINDOW: &str = "30s";

const NODE_CPU_SAMPLE_DELAY: Duration = Duration::from_millis(100);

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NodeMetricsRequest {
    pub request_id: String,
    pub node_name: String,
    pub pod_uids: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NodeMetricsResponse {
    pub request_id: String,
    pub node_name: String,
    pub node: Option<NodeMetricsNodeSample>,
    pub pods: Vec<NodeMetricsPodSample>,
    pub error: Option<String>,
}

impl NodeMetricsResponse {
    pub fn error(request_id: String, node_name: String, error: impl Into<String>) -> Self {
        Self {
            request_id,
            node_name,
            node: None,
            pods: Vec::new(),
            error: Some(error.into()),
        }
    }

    pub fn from_node_sample(
        request_id: String,
        node_name: String,
        node: NodeMetricsNodeSample,
    ) -> Self {
        Self {
            request_id,
            node_name,
            node: Some(node),
            pods: Vec::new(),
            error: None,
        }
    }

    pub fn from_pod_sandbox_stats(
        request: &NodeMetricsRequest,
        node: Option<NodeMetricsNodeSample>,
        stats: Vec<k8s_cri::v1::PodSandboxStats>,
    ) -> Self {
        let wanted_uids: BTreeSet<&str> = request
            .pod_uids
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

        Self {
            request_id: request.request_id.clone(),
            node_name: request.node_name.clone(),
            node,
            pods,
            error: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NodeMetricsNodeSample {
    pub cpu_nanos: u64,
    pub memory_bytes: u64,
}

impl From<NodeMetricsNodeSample> for ResourceUsage {
    fn from(sample: NodeMetricsNodeSample) -> Self {
        Self::new(sample.cpu_nanos, sample.memory_bytes)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NodeMetricsPodSample {
    pub namespace: String,
    pub name: String,
    pub uid: String,
    pub containers: Vec<NodeMetricsContainerSample>,
}

impl From<RuntimePodSample> for NodeMetricsPodSample {
    fn from(sample: RuntimePodSample) -> Self {
        Self {
            namespace: sample.namespace,
            name: sample.name,
            uid: sample.uid,
            containers: sample
                .containers
                .into_iter()
                .map(|(name, usage)| NodeMetricsContainerSample {
                    name,
                    cpu_nanos: usage.cpu_nanos,
                    memory_bytes: usage.memory_bytes,
                })
                .collect(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NodeMetricsContainerSample {
    pub name: String,
    pub cpu_nanos: u64,
    pub memory_bytes: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ResourceUsage {
    cpu_nanos: u64,
    memory_bytes: u64,
}

impl ResourceUsage {
    pub fn new(cpu_nanos: u64, memory_bytes: u64) -> Self {
        Self {
            cpu_nanos,
            memory_bytes,
        }
    }

    pub fn add_assign(&mut self, other: Self) {
        self.cpu_nanos = self.cpu_nanos.saturating_add(other.cpu_nanos);
        self.memory_bytes = self.memory_bytes.saturating_add(other.memory_bytes);
    }

    pub fn cpu_nanos(&self) -> u64 {
        self.cpu_nanos
    }

    pub fn memory_bytes(&self) -> u64 {
        self.memory_bytes
    }

    pub fn resource_value(&self, resource: &str) -> Option<u64> {
        match resource {
            "cpu" => Some(self.cpu_nanos),
            "memory" => Some(self.memory_bytes),
            _ => None,
        }
    }

    pub fn as_metrics_usage(&self) -> Value {
        json!({
            "cpu": format_cpu_quantity(self.cpu_nanos),
            "memory": format_memory_quantity(self.memory_bytes),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContainerMetric {
    name: String,
    usage: ResourceUsage,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodMetric {
    name: String,
    namespace: String,
    node_name: Option<String>,
    containers: Vec<ContainerMetric>,
}

impl PodMetric {
    pub fn usage(&self) -> ResourceUsage {
        let mut total = ResourceUsage::default();
        for container in &self.containers {
            total.add_assign(container.usage);
        }
        total
    }

    pub fn node_name(&self) -> Option<&str> {
        self.node_name.as_deref()
    }

    pub fn usage_for_resource(&self, resource: &str) -> Option<u64> {
        let mut total = 0_u64;
        for container in &self.containers {
            total = total.saturating_add(container.usage.resource_value(resource)?);
        }
        Some(total)
    }
}

#[derive(Clone, Debug, Default)]
pub struct MetricsSnapshot {
    node_usage: BTreeMap<String, ResourceUsage>,
}

impl MetricsSnapshot {
    pub fn from_runtime_nodes(runtime: &RuntimeMetricsSnapshot) -> Self {
        Self {
            node_usage: runtime.node_usage.clone(),
        }
    }

    pub fn available_node_usage(&self, node_name: &str) -> Option<ResourceUsage> {
        self.node_usage.get(node_name).copied()
    }
}

impl PodMetric {
    pub fn from_resource(pod: &Resource, runtime: &RuntimeMetricsSnapshot) -> Option<Self> {
        let namespace = pod_namespace(pod);
        let name = pod.name.clone();
        let node_name = pod
            .data
            .pointer("/spec/nodeName")
            .and_then(Value::as_str)
            .filter(|node| !node.is_empty())
            .map(str::to_string);
        let containers = pod
            .data
            .pointer("/spec/containers")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .map(|container| {
                let name = container.get("name").and_then(Value::as_str)?;
                let usage = runtime.container_usage(pod, name)?;
                Some(ContainerMetric {
                    name: name.to_string(),
                    usage,
                })
            })
            .collect::<Option<Vec<_>>>()?;

        if containers.is_empty() {
            return None;
        }

        Some(Self {
            name,
            namespace,
            node_name,
            containers,
        })
    }
}

#[derive(Clone, Debug, Default)]
pub struct RuntimeMetricsSnapshot {
    node_usage: BTreeMap<String, ResourceUsage>,
    by_uid: BTreeMap<String, RuntimePodSample>,
    by_namespace_name: BTreeMap<(String, String), RuntimePodSample>,
}

#[derive(Clone, Debug, Default)]
struct RuntimePodSample {
    uid: String,
    namespace: String,
    name: String,
    containers: BTreeMap<String, ResourceUsage>,
}

impl RuntimeMetricsSnapshot {
    pub async fn collect_from_cri(
        cri: Option<&Arc<tokio::sync::Mutex<crate::kubelet::cri::CriClient>>>,
    ) -> Self {
        let Some(cri) = cri else {
            return Self::default();
        };
        let mut client = {
            let guard = cri.lock().await;
            guard.clone()
        };
        match client.list_pod_sandbox_stats(None).await {
            Ok(stats) => Self::from_pod_sandbox_stats(stats),
            Err(error) => {
                tracing::debug!(%error, "CRI pod sandbox metrics unavailable");
                Self::default()
            }
        }
    }

    pub fn from_pod_sandbox_stats(stats: Vec<k8s_cri::v1::PodSandboxStats>) -> Self {
        let mut snapshot = Self::default();
        for stat in stats {
            let Some(sample) = RuntimePodSample::from_cri(stat) else {
                continue;
            };
            if !sample.uid.is_empty() {
                snapshot.by_uid.insert(sample.uid.clone(), sample.clone());
            }
            if !sample.namespace.is_empty() && !sample.name.is_empty() {
                snapshot
                    .by_namespace_name
                    .insert((sample.namespace.clone(), sample.name.clone()), sample);
            }
        }
        snapshot
    }

    pub fn from_node_metrics_responses(
        responses: impl IntoIterator<Item = NodeMetricsResponse>,
    ) -> Self {
        let mut snapshot = Self::default();
        for response in responses {
            if let Some(error) = response.error {
                tracing::debug!(
                    node = %response.node_name,
                    %error,
                    "node runtime metrics unavailable"
                );
                continue;
            }
            if let Some(node) = response.node {
                snapshot
                    .node_usage
                    .insert(response.node_name.clone(), ResourceUsage::from(node));
            }
            for sample in response.pods {
                snapshot.insert_sample(RuntimePodSample::from(sample));
            }
        }
        snapshot
    }

    fn insert_sample(&mut self, sample: RuntimePodSample) {
        if !sample.uid.is_empty() {
            self.by_uid.insert(sample.uid.clone(), sample.clone());
        }
        if !sample.namespace.is_empty() && !sample.name.is_empty() {
            self.by_namespace_name
                .insert((sample.namespace.clone(), sample.name.clone()), sample);
        }
    }

    fn container_usage(&self, pod: &Resource, container_name: &str) -> Option<ResourceUsage> {
        let uid = pod_uid(pod);
        if !uid.is_empty()
            && let Some(sample) = self.by_uid.get(&uid)
            && let Some(usage) = sample.containers.get(container_name)
        {
            return Some(*usage);
        }

        let key = (pod_namespace(pod), pod.name.clone());
        self.by_namespace_name
            .get(&key)
            .and_then(|sample| sample.containers.get(container_name))
            .copied()
    }
}

impl RuntimePodSample {
    fn from_cri(stats: k8s_cri::v1::PodSandboxStats) -> Option<Self> {
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
                Some((
                    name.to_string(),
                    ResourceUsage::from_cri_container(&container)?,
                ))
            })
            .collect::<BTreeMap<_, _>>();

        Some(Self {
            uid: metadata.uid,
            namespace: metadata.namespace,
            name: metadata.name,
            containers,
        })
    }
}

impl From<NodeMetricsPodSample> for RuntimePodSample {
    fn from(sample: NodeMetricsPodSample) -> Self {
        Self {
            uid: sample.uid,
            namespace: sample.namespace,
            name: sample.name,
            containers: sample
                .containers
                .into_iter()
                .map(|container| {
                    (
                        container.name,
                        ResourceUsage::new(container.cpu_nanos, container.memory_bytes),
                    )
                })
                .collect(),
        }
    }
}

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
struct NodeMetricsRequestCoalescer {
    in_flight: Mutex<HashMap<String, watch::Receiver<Option<NodeMetricsResponse>>>>,
}

impl NodeMetricsRequestCoalescer {
    async fn get_or_spawn<F>(
        self: &Arc<Self>,
        node_name: String,
        supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
        fetch: F,
    ) -> NodeMetricsResponse
    where
        F: Future<Output = NodeMetricsResponse> + Send + 'static,
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
                    crate::task_supervisor::TaskCategory::Network,
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
                return NodeMetricsResponse::error(
                    String::new(),
                    node_name,
                    format!("failed to spawn node metrics request: {error:#}"),
                );
            }
        }

        await_node_metrics_response(receiver, node_name).await
    }
}

async fn await_node_metrics_response(
    mut receiver: watch::Receiver<Option<NodeMetricsResponse>>,
    node_name: String,
) -> NodeMetricsResponse {
    loop {
        if let Some(response) = receiver.borrow().clone() {
            return response;
        }
        if receiver.changed().await.is_err() {
            return NodeMetricsResponse::error(
                String::new(),
                node_name,
                "node metrics request closed before response",
            );
        }
    }
}

pub struct OnDemandMetricsProvider {
    local_node_name: String,
    cri: Option<Arc<tokio::sync::Mutex<crate::kubelet::cri::CriClient>>>,
    replication: Option<Arc<crate::replication::ReplicationService>>,
    supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
    coalescer: Arc<NodeMetricsRequestCoalescer>,
}

impl OnDemandMetricsProvider {
    pub fn new(
        local_node_name: String,
        cri: Option<Arc<tokio::sync::Mutex<crate::kubelet::cri::CriClient>>>,
        replication: Option<Arc<crate::replication::ReplicationService>>,
        supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
    ) -> Self {
        Self {
            local_node_name,
            cri,
            replication,
            supervisor,
            coalescer: Arc::new(NodeMetricsRequestCoalescer::default()),
        }
    }

    async fn collect_node_metrics(&self, node_name: String) -> NodeMetricsResponse {
        let local_node_name = self.local_node_name.clone();
        let cri = self.cri.clone();
        let replication = self.replication.clone();
        let request_node_name = node_name.clone();
        let supervisor = self.supervisor.clone();
        let fetch_supervisor = supervisor.clone();
        self.coalescer
            .get_or_spawn(node_name.clone(), supervisor, async move {
                if request_node_name == local_node_name {
                    collect_local_cri_node_metrics(cri, request_node_name, fetch_supervisor).await
                } else if let Some(replication) = replication {
                    let request = NodeMetricsRequest {
                        request_id: String::new(),
                        node_name: request_node_name.clone(),
                        pod_uids: Vec::new(),
                    };
                    match replication.request_node_metrics(request).await {
                        Ok(response) => response,
                        Err(error) => NodeMetricsResponse::error(
                            String::new(),
                            request_node_name,
                            format!("{error:#}"),
                        ),
                    }
                } else {
                    NodeMetricsResponse::error(
                        String::new(),
                        request_node_name,
                        "replication service is not available for remote node metrics",
                    )
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
        RuntimeMetricsSnapshot::from_node_metrics_responses(responses)
    }
}

async fn collect_local_cri_node_metrics(
    cri: Option<Arc<tokio::sync::Mutex<crate::kubelet::cri::CriClient>>>,
    node_name: String,
    supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
) -> NodeMetricsResponse {
    let request = NodeMetricsRequest {
        request_id: String::new(),
        node_name: node_name.clone(),
        pod_uids: Vec::new(),
    };
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
            return NodeMetricsResponse::from_node_sample(String::new(), node_name, node);
        }
        return NodeMetricsResponse::error(
            String::new(),
            node_name,
            "CRI client is not available for local node metrics",
        );
    };
    let mut client = {
        let guard = cri.lock().await;
        guard.clone()
    };
    match client.list_pod_sandbox_stats(None).await {
        Ok(stats) => NodeMetricsResponse::from_pod_sandbox_stats(&request, node, stats),
        Err(error) => {
            tracing::debug!(%error, "CRI pod sandbox metrics unavailable");
            if let Some(node) = node {
                NodeMetricsResponse::from_node_sample(String::new(), node_name, node)
            } else {
                NodeMetricsResponse::error(String::new(), node_name, format!("{error:#}"))
            }
        }
    }
}

#[async_trait]
pub trait NodeMetricsSampler: Send + Sync {
    async fn sample_node(&self) -> anyhow::Result<NodeMetricsNodeSample>;
}

pub struct LinuxProcNodeMetricsSampler {
    supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
}

impl LinuxProcNodeMetricsSampler {
    pub fn new(supervisor: Arc<crate::task_supervisor::TaskSupervisor>) -> Self {
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

        Ok(NodeMetricsNodeSample {
            cpu_nanos,
            memory_bytes: second.memory_used_bytes,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ProcNodeUsage {
    cpu_total_jiffies: u64,
    cpu_idle_jiffies: u64,
    memory_used_bytes: u64,
}

async fn read_proc_node_usage(
    supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
) -> anyhow::Result<ProcNodeUsage> {
    supervisor
        .run_blocking_file("node_metrics_read_proc", read_proc_node_usage_blocking)
        .await?
}

fn read_proc_node_usage_blocking() -> anyhow::Result<ProcNodeUsage> {
    let stat = crate::utils::read_utf8_file("/proc/stat").context("read /proc/stat")?;
    let meminfo = crate::utils::read_utf8_file("/proc/meminfo").context("read /proc/meminfo")?;
    parse_proc_node_usage(&stat, &meminfo)
}

fn parse_proc_node_usage(stat: &str, meminfo: &str) -> anyhow::Result<ProcNodeUsage> {
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

pub fn pod_request_for_resource(pod: &Resource, resource: &str) -> Option<u64> {
    let mut total = 0_u64;
    let mut seen = false;
    for container in pod
        .data
        .pointer("/spec/containers")
        .and_then(Value::as_array)?
    {
        let request = container_request_for_resource(container, resource)?;
        total = total.saturating_add(request);
        seen = true;
    }
    seen.then_some(total)
}

fn container_request_for_resource(container: &Value, resource: &str) -> Option<u64> {
    let raw = container
        .pointer(&format!("/resources/requests/{resource}"))
        .and_then(Value::as_str)?;
    parse_resource_quantity(resource, raw)
}

pub fn parse_resource_quantity(resource: &str, raw: &str) -> Option<u64> {
    match resource {
        "cpu" => parse_cpu_resource(raw),
        "memory" => parse_memory_resource(raw),
        _ => return None,
    }
    .and_then(|value| u64::try_from(value).ok())
}

pub fn parse_resource_quantity_value(resource: &str, value: &Value) -> Option<u64> {
    if let Some(raw) = value.as_str() {
        return parse_resource_quantity(resource, raw);
    }
    parse_resource_quantity(resource, &value.to_string())
}

pub fn format_resource_quantity(resource: &str, value: u64) -> Result<String, anyhow::Error> {
    match resource {
        "cpu" => Ok(format_cpu_quantity(value)),
        "memory" => Ok(format_memory_quantity(value)),
        _ => Err(anyhow!("unsupported resource metric '{resource}'")),
    }
}

impl ResourceUsage {
    fn from_cri_container(container: &k8s_cri::v1::ContainerStats) -> Option<Self> {
        let cpu_nanos = container
            .cpu
            .as_ref()
            .and_then(|cpu| cpu.usage_nano_cores)
            .map(|value| value.value);
        let memory_bytes = container
            .memory
            .as_ref()
            .and_then(|memory| memory.working_set_bytes.or(memory.usage_bytes))
            .map(|value| value.value);

        match (cpu_nanos, memory_bytes) {
            (Some(cpu_nanos), Some(memory_bytes)) => Some(Self::new(cpu_nanos, memory_bytes)),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct MetricsObjectBuilder {
    timestamp: String,
}

impl MetricsObjectBuilder {
    pub fn new(timestamp: String) -> Self {
        Self { timestamp }
    }

    pub fn node_metrics_object(&self, name: &str, usage: ResourceUsage) -> Value {
        json!({
            "apiVersion": METRICS_API_VERSION,
            "kind": "NodeMetrics",
            "metadata": {"name": name},
            "timestamp": self.timestamp.as_str(),
            "window": METRICS_WINDOW,
            "usage": usage.as_metrics_usage(),
        })
    }

    pub fn pod_metrics_object(&self, pod: &PodMetric) -> Value {
        let containers: Vec<Value> = pod
            .containers
            .iter()
            .map(|container| {
                json!({
                    "name": container.name.as_str(),
                    "usage": container.usage.as_metrics_usage(),
                })
            })
            .collect();

        json!({
            "apiVersion": METRICS_API_VERSION,
            "kind": "PodMetrics",
            "metadata": {
                "name": pod.name.as_str(),
                "namespace": pod.namespace.as_str(),
            },
            "timestamp": self.timestamp.as_str(),
            "window": METRICS_WINDOW,
            "containers": containers,
        })
    }
}

fn pod_namespace(pod: &Resource) -> String {
    pod.namespace
        .clone()
        .or_else(|| {
            pod.data
                .pointer("/metadata/namespace")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .unwrap_or_default()
}

fn pod_uid(pod: &Resource) -> String {
    if !pod.uid.is_empty() {
        return pod.uid.clone();
    }
    pod.data
        .pointer("/metadata/uid")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn pod_is_terminal(pod: &Value) -> bool {
    matches!(
        pod.pointer("/status/phase").and_then(Value::as_str),
        Some("Succeeded" | "Failed")
    )
}

fn format_cpu_quantity(cpu_nanos: u64) -> String {
    if cpu_nanos == 0 {
        return "0".to_string();
    }
    if cpu_nanos.is_multiple_of(1_000_000) {
        return format!("{}m", cpu_nanos / 1_000_000);
    }
    format!("{cpu_nanos}n")
}

fn format_memory_quantity(memory_bytes: u64) -> String {
    if memory_bytes == 0 {
        return "0".to_string();
    }
    format!("{}Ki", memory_bytes.div_ceil(1024))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pod_resource(body: Value) -> Resource {
        Resource {
            id: 1,
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "pod-a".to_string(),
            uid: "uid-a".to_string(),
            resource_version: 1,
            data: Arc::new(body),
        }
    }

    #[test]
    fn pod_metric_is_unavailable_without_runtime_sample() {
        let pod = pod_resource(json!({
            "metadata": {"name": "pod-a", "namespace": "default", "uid": "uid-a"},
            "spec": {
                "nodeName": "node-a",
                "containers": [
                    {"name": "app", "resources": {"requests": {"cpu": "250m", "memory": "64Mi"}}},
                    {"name": "besteffort"}
                ]
            }
        }));

        assert!(PodMetric::from_resource(&pod, &RuntimeMetricsSnapshot::default()).is_none());
    }

    #[test]
    fn runtime_sample_overrides_spec_requests_by_pod_uid_and_container_name() {
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
        let runtime = RuntimeMetricsSnapshot::from_pod_sandbox_stats(vec![stats]);
        let pod = pod_resource(json!({
            "metadata": {"name": "pod-a", "namespace": "default", "uid": "uid-a"},
            "spec": {
                "containers": [
                    {"name": "app", "resources": {"requests": {"cpu": "250m", "memory": "64Mi"}}}
                ]
            }
        }));

        let metric = PodMetric::from_resource(&pod, &runtime).unwrap();
        let object = MetricsObjectBuilder::new("2026-01-01T00:00:00Z".to_string())
            .pod_metrics_object(&metric);

        assert_eq!(object["containers"][0]["usage"]["cpu"], "123m");
        assert_eq!(object["containers"][0]["usage"]["memory"], "9216Ki");
    }

    #[test]
    fn node_metrics_response_merges_into_runtime_snapshot() {
        let runtime = RuntimeMetricsSnapshot::from_node_metrics_responses([NodeMetricsResponse {
            request_id: "metrics-1".to_string(),
            node_name: "node-b".to_string(),
            node: Some(NodeMetricsNodeSample {
                cpu_nanos: 222_000_000,
                memory_bytes: 99 * 1024 * 1024,
            }),
            pods: vec![NodeMetricsPodSample {
                namespace: "default".to_string(),
                name: "pod-a".to_string(),
                uid: "uid-a".to_string(),
                containers: vec![NodeMetricsContainerSample {
                    name: "app".to_string(),
                    cpu_nanos: 88_000_000,
                    memory_bytes: 7 * 1024 * 1024,
                }],
            }],
            error: None,
        }]);
        let snapshot = MetricsSnapshot::from_runtime_nodes(&runtime);
        assert_eq!(
            snapshot.available_node_usage("node-b"),
            Some(ResourceUsage::new(222_000_000, 99 * 1024 * 1024))
        );
        let pod = pod_resource(json!({
            "metadata": {"name": "pod-a", "namespace": "default", "uid": "uid-a"},
            "spec": {
                "nodeName": "node-b",
                "containers": [
                    {"name": "app", "resources": {"requests": {"cpu": "250m", "memory": "64Mi"}}}
                ]
            }
        }));

        let metric = PodMetric::from_resource(&pod, &runtime).unwrap();
        let object = MetricsObjectBuilder::new("2026-01-01T00:00:00Z".to_string())
            .pod_metrics_object(&metric);

        assert_eq!(object["containers"][0]["usage"]["cpu"], "88m");
        assert_eq!(object["containers"][0]["usage"]["memory"], "7168Ki");
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

    #[tokio::test]
    async fn coalesces_concurrent_node_metrics_fetches() {
        let supervisor = Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ));
        let coalescer = Arc::new(NodeMetricsRequestCoalescer::default());
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let started = Arc::new(tokio::sync::Barrier::new(2));
        let release = Arc::new(tokio::sync::Notify::new());

        let first = {
            let coalescer = coalescer.clone();
            let supervisor = supervisor.clone();
            let calls = calls.clone();
            let started = started.clone();
            let release = release.clone();
            tokio::spawn(async move {
                coalescer
                    .get_or_spawn("node-a".to_string(), supervisor, async move {
                        calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        started.wait().await;
                        release.notified().await;
                        NodeMetricsResponse {
                            request_id: "first".to_string(),
                            node_name: "node-a".to_string(),
                            node: None,
                            pods: Vec::new(),
                            error: None,
                        }
                    })
                    .await
            })
        };

        started.wait().await;
        let second = {
            let coalescer = coalescer.clone();
            let supervisor = supervisor.clone();
            let calls = calls.clone();
            tokio::spawn(async move {
                coalescer
                    .get_or_spawn("node-a".to_string(), supervisor, async move {
                        calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        NodeMetricsResponse::error(
                            "second".to_string(),
                            "node-a".to_string(),
                            "must not run",
                        )
                    })
                    .await
            })
        };

        release.notify_waiters();
        let (first, second) = tokio::join!(first, second);
        let first = first.unwrap();
        let second = second.unwrap();

        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(first.request_id, "first");
        assert_eq!(second.request_id, "first");

        let _ = supervisor.shutdown(std::time::Duration::from_secs(1)).await;
    }
}
