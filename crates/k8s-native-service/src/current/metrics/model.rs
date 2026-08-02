use klights_cluster_core::Resource;
use klights_node_api::{NodeMetricsSnapshot, NodeMetricsUsage};
use serde_json::Value;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ContainerMetric {
    pub(super) name: String,
    pub(super) usage: NodeMetricsUsage,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::current) struct PodMetric {
    pub(super) name: String,
    pub(super) namespace: String,
    pub(super) containers: Vec<ContainerMetric>,
}

impl PodMetric {
    pub(in crate::current) fn from_resource(
        pod: &Resource,
        snapshot: &NodeMetricsSnapshot,
    ) -> Option<Self> {
        let namespace = pod_namespace(pod);
        let uid = pod_uid(pod);
        let containers = pod
            .data
            .pointer("/spec/containers")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .map(|container| {
                let name = container.get("name").and_then(Value::as_str)?;
                let usage = snapshot.container_usage(&uid, &namespace, &pod.name, name)?;
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
            name: pod.name.clone(),
            namespace,
            containers,
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
