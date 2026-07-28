use super::model::{PodMetric, ResourceUsage};
use crate::datastore::Resource;
use crate::kubelet::pod_resources::{parse_cpu_resource, parse_memory_resource};
use anyhow::anyhow;
use serde_json::{Value, json};

pub const METRICS_API_VERSION: &str = "metrics.k8s.io/v1beta1";
pub const METRICS_WINDOW: &str = "30s";

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
    pub fn as_metrics_usage(&self) -> Value {
        json!({
            "cpu": format_cpu_quantity(self.cpu_nanos),
            "memory": format_memory_quantity(self.memory_bytes),
        })
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
