use crate::datastore::Resource;
use klights_node_api::{
    NodeMetricsContainerSample, NodeMetricsError, NodeMetricsNodeSample, NodeMetricsPodSample,
    NodeMetricsResult,
};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;

impl From<NodeMetricsNodeSample> for ResourceUsage {
    fn from(sample: NodeMetricsNodeSample) -> Self {
        Self::new(sample.cpu_nanos(), sample.memory_bytes())
    }
}

impl From<RuntimePodSample> for NodeMetricsPodSample {
    fn from(sample: RuntimePodSample) -> Self {
        Self::new(
            sample.namespace,
            sample.name,
            sample.uid,
            sample
                .containers
                .into_iter()
                .map(|(name, usage)| {
                    NodeMetricsContainerSample::new(name, usage.cpu_nanos, usage.memory_bytes)
                })
                .collect(),
        )
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ResourceUsage {
    pub(super) cpu_nanos: u64,
    pub(super) memory_bytes: u64,
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

    pub(super) fn from_cri_container(container: &k8s_cri::v1::ContainerStats) -> Option<Self> {
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContainerMetric {
    pub(super) name: String,
    pub(super) usage: ResourceUsage,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodMetric {
    pub(super) name: String,
    pub(super) namespace: String,
    node_name: Option<String>,
    pub(super) containers: Vec<ContainerMetric>,
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

#[derive(Clone, Debug, Default)]
pub struct RuntimeMetricsSnapshot {
    pub(super) node_usage: BTreeMap<String, ResourceUsage>,
    pub(super) by_uid: BTreeMap<String, RuntimePodSample>,
    pub(super) by_namespace_name: BTreeMap<(String, String), RuntimePodSample>,
}

#[derive(Clone, Debug, Default)]
pub(super) struct RuntimePodSample {
    pub(super) uid: String,
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

    pub fn from_node_metrics_results(
        results: impl IntoIterator<Item = Result<NodeMetricsResult, NodeMetricsError>>,
    ) -> Self {
        let mut snapshot = Self::default();
        for result in results {
            let result = match result {
                Ok(result) => result,
                Err(error) => {
                    tracing::debug!(%error, "node runtime metrics unavailable");
                    continue;
                }
            };
            let (target, node, pods) = result.into_parts();
            if let Some(node) = node {
                snapshot
                    .node_usage
                    .insert(target.into_node_name(), ResourceUsage::from(node));
            }
            for sample in pods {
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
    pub(super) fn from_cri(stats: k8s_cri::v1::PodSandboxStats) -> Option<Self> {
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
        let (namespace, name, uid, containers) = sample.into_parts();
        Self {
            uid,
            namespace,
            name,
            containers: containers
                .into_iter()
                .map(|container| {
                    let (name, cpu_nanos, memory_bytes) = container.into_parts();
                    (name, ResourceUsage::new(cpu_nanos, memory_bytes))
                })
                .collect(),
        }
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
