use super::model::PodMetric;
use klights_node_api::NodeMetricsUsage;
use serde_json::{Value, json};

pub(in crate::current) const METRICS_API_VERSION: &str = "metrics.k8s.io/v1beta1";
const METRICS_WINDOW: &str = "30s";

pub(in crate::current) struct MetricsObjectBuilder {
    timestamp: String,
}

impl MetricsObjectBuilder {
    pub(in crate::current) fn new(timestamp: String) -> Self {
        Self { timestamp }
    }

    pub(in crate::current) fn node_metrics_object(
        &self,
        name: &str,
        usage: NodeMetricsUsage,
    ) -> Value {
        json!({
            "apiVersion": METRICS_API_VERSION,
            "kind": "NodeMetrics",
            "metadata": {"name": name},
            "timestamp": self.timestamp.as_str(),
            "window": METRICS_WINDOW,
            "usage": metrics_usage(usage),
        })
    }

    pub(in crate::current) fn pod_metrics_object(&self, pod: &PodMetric) -> Value {
        let containers: Vec<Value> = pod
            .containers
            .iter()
            .map(|container| {
                json!({
                    "name": container.name.as_str(),
                    "usage": metrics_usage(container.usage),
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

fn metrics_usage(usage: NodeMetricsUsage) -> Value {
    json!({
        "cpu": format_cpu_quantity(usage.cpu_nanos()),
        "memory": format_memory_quantity(usage.memory_bytes()),
    })
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
