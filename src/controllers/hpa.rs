//! HorizontalPodAutoscaler controller reconcile logic.

use crate::metrics::{
    MetricsProvider, PodMetric, RuntimeMetricsSnapshot, format_resource_quantity,
    parse_resource_quantity_value, pod_request_for_resource,
};
use anyhow::{Context as _, Result};
use async_trait::async_trait;
use klights_cluster_core::Resource;
use serde_json::{Value, json};

const MAX_RETRIES: u32 = 5;

#[async_trait]
pub(crate) trait HpaRuntime: Send + Sync {
    async fn get_hpa(
        &self,
        api_version: &str,
        namespace: &str,
        name: &str,
    ) -> Result<Option<Resource>>;
    async fn get_scale_target(
        &self,
        api_version: &str,
        kind: &str,
        namespace: &str,
        name: &str,
    ) -> Result<Option<Resource>>;
    async fn list_pods(&self, namespace: &str) -> Result<Vec<Resource>>;
    async fn patch_scale_target(&self, target: &ScaleTarget, replicas: i64) -> Result<Resource>;
    async fn reconcile_scaled_target(
        &self,
        target: &ScaleTarget,
        resource: &Value,
        node_name: &str,
    ) -> Result<()>;
    async fn update_hpa_status(&self, current: &Resource, status: Value) -> Result<()>;
    fn is_conflict(&self, error: &anyhow::Error) -> bool;
}

pub(crate) async fn reconcile_hpa_with_runtime(
    runtime: &dyn HpaRuntime,
    hpa: &Value,
    node_name: &str,
    metrics_provider: &dyn MetricsProvider,
) -> Result<()> {
    let api_version = hpa
        .get("apiVersion")
        .and_then(|v| v.as_str())
        .filter(|value| matches!(*value, "autoscaling/v1" | "autoscaling/v2"))
        .context("HPA missing supported apiVersion")?;
    let metadata = hpa.get("metadata").context("HPA missing metadata")?;
    let name = metadata
        .get("name")
        .and_then(|v| v.as_str())
        .context("HPA missing metadata.name")?;
    let namespace = metadata
        .get("namespace")
        .and_then(|v| v.as_str())
        .context("HPA missing metadata.namespace")?;

    let mut last_conflict = None;
    for _ in 0..MAX_RETRIES {
        let current = runtime
            .get_hpa(api_version, namespace, name)
            .await?
            .context("HPA not found")?;

        let decision = evaluate_hpa(runtime, metrics_provider, &current.data, namespace).await?;
        if decision.scale_active
            && let Some(target) = &decision.target
            && target.spec_replicas != decision.desired_replicas
        {
            let patched_target = runtime
                .patch_scale_target(target, decision.desired_replicas)
                .await?;
            runtime
                .reconcile_scaled_target(target, &patched_target.data, node_name)
                .await?;
        }

        let status = build_status(&current.data, &decision);
        if current.data.get("status") == Some(&status) {
            return Ok(());
        }

        match runtime.update_hpa_status(&current, status).await {
            Ok(()) => return Ok(()),
            Err(err) if runtime.is_conflict(&err) => {
                last_conflict = Some(err);
                continue;
            }
            Err(err) => return Err(err),
        }
    }

    match last_conflict {
        Some(err) => Err(err).context("HPA status update conflict retries exhausted"),
        None => Ok(()),
    }
}

#[derive(Clone, Copy)]
pub(crate) enum ScaleTargetKind {
    Deployment,
    ReplicaSet,
    StatefulSet,
    ReplicationController,
}

pub(crate) struct ScaleTarget {
    pub(crate) api_version: &'static str,
    pub(crate) kind: &'static str,
    pub(crate) name: String,
    pub(crate) namespace: String,
    pub(crate) uid: String,
    selector: klights_types::LabelSelector,
    spec_replicas: i64,
    status_replicas: i64,
    pub(crate) kind_tag: ScaleTargetKind,
}

struct MetricObservation {
    current_metric: Value,
    desired_replicas: i64,
    current_utilization: Option<i64>,
}

struct HpaDecision {
    target: Option<ScaleTarget>,
    current_replicas: i64,
    desired_replicas: i64,
    raw_desired_replicas: i64,
    min_replicas: i64,
    max_replicas: i64,
    scale_active: bool,
    current_metrics: Vec<Value>,
    current_cpu_utilization: Option<i64>,
    inactive_reason: Option<&'static str>,
}

async fn evaluate_hpa(
    runtime: &dyn HpaRuntime,
    metrics_provider: &dyn MetricsProvider,
    hpa: &Value,
    namespace: &str,
) -> Result<HpaDecision> {
    let spec = hpa.get("spec").context("HPA missing spec")?;
    let min_replicas = spec
        .get("minReplicas")
        .and_then(|v| v.as_i64())
        .unwrap_or(1)
        .max(1);
    let max_replicas = spec
        .get("maxReplicas")
        .and_then(|v| v.as_i64())
        .unwrap_or(min_replicas)
        .max(min_replicas);

    let Some(target) = get_scale_target(runtime, spec, namespace).await? else {
        return Ok(HpaDecision {
            target: None,
            current_replicas: 0,
            desired_replicas: 0,
            raw_desired_replicas: 0,
            min_replicas,
            max_replicas,
            scale_active: false,
            current_metrics: Vec::new(),
            current_cpu_utilization: None,
            inactive_reason: Some("FailedGetScale"),
        });
    };

    let observations = observe_metrics(runtime, metrics_provider, hpa, spec, &target).await?;
    if observations.is_empty() {
        let current = target.status_replicas;
        return Ok(HpaDecision {
            target: Some(target),
            current_replicas: current,
            desired_replicas: current.clamp(min_replicas, max_replicas),
            raw_desired_replicas: current,
            min_replicas,
            max_replicas,
            scale_active: false,
            current_metrics: Vec::new(),
            current_cpu_utilization: None,
            inactive_reason: Some("FailedGetResourceMetric"),
        });
    }

    let raw_desired = observations
        .iter()
        .map(|metric| metric.desired_replicas)
        .max()
        .unwrap_or(target.status_replicas);
    let desired = raw_desired.clamp(min_replicas, max_replicas);
    let current_cpu_utilization = observations
        .iter()
        .find_map(|metric| metric.current_utilization);
    let current_metrics = observations
        .into_iter()
        .map(|metric| metric.current_metric)
        .collect();

    Ok(HpaDecision {
        current_replicas: target.status_replicas,
        target: Some(target),
        desired_replicas: desired,
        raw_desired_replicas: raw_desired,
        min_replicas,
        max_replicas,
        scale_active: true,
        current_metrics,
        current_cpu_utilization,
        inactive_reason: None,
    })
}

async fn get_scale_target(
    runtime: &dyn HpaRuntime,
    spec: &Value,
    namespace: &str,
) -> Result<Option<ScaleTarget>> {
    let target_ref = spec
        .get("scaleTargetRef")
        .context("HPA missing spec.scaleTargetRef")?;
    let api_version = target_ref
        .get("apiVersion")
        .and_then(|v| v.as_str())
        .unwrap_or("apps/v1");
    let kind = target_ref
        .get("kind")
        .and_then(|v| v.as_str())
        .context("HPA scaleTargetRef missing kind")?;
    let name = target_ref
        .get("name")
        .and_then(|v| v.as_str())
        .context("HPA scaleTargetRef missing name")?;

    let (api_version, kind, kind_tag) = match (api_version, kind) {
        ("apps/v1", "Deployment") => ("apps/v1", "Deployment", ScaleTargetKind::Deployment),
        ("apps/v1", "ReplicaSet") => ("apps/v1", "ReplicaSet", ScaleTargetKind::ReplicaSet),
        ("apps/v1", "StatefulSet") => ("apps/v1", "StatefulSet", ScaleTargetKind::StatefulSet),
        ("v1", "ReplicationController") => (
            "v1",
            "ReplicationController",
            ScaleTargetKind::ReplicationController,
        ),
        _ => return Ok(None),
    };

    let Some(resource) = runtime
        .get_scale_target(api_version, kind, namespace, name)
        .await?
    else {
        return Ok(None);
    };

    let selector = match kind_tag {
        ScaleTargetKind::ReplicationController => {
            klights_types::LabelSelector::from_flat_match_labels(
                resource
                    .data
                    .pointer("/spec/selector")
                    .unwrap_or(&Value::Null),
            )?
        }
        _ => klights_types::LabelSelector::from_k8s_selector(
            resource
                .data
                .pointer("/spec/selector")
                .unwrap_or(&Value::Null),
        )?,
    };

    Ok(Some(ScaleTarget {
        api_version,
        kind,
        name: name.to_string(),
        namespace: namespace.to_string(),
        uid: resource.uid,
        selector,
        spec_replicas: resource
            .data
            .pointer("/spec/replicas")
            .and_then(|v| v.as_i64())
            .unwrap_or(1)
            .max(0),
        status_replicas: resource
            .data
            .pointer("/status/replicas")
            .and_then(|v| v.as_i64())
            .unwrap_or_else(|| {
                resource
                    .data
                    .pointer("/spec/replicas")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(1)
            })
            .max(0),
        kind_tag,
    }))
}

async fn observe_metrics(
    runtime: &dyn HpaRuntime,
    metrics_provider: &dyn MetricsProvider,
    hpa: &Value,
    spec: &Value,
    target: &ScaleTarget,
) -> Result<Vec<MetricObservation>> {
    let pods = runtime.list_pods(&target.namespace).await?;
    let matching_ready_pods: Vec<Resource> = pods
        .iter()
        .filter(|pod| {
            pod.data.pointer("/metadata/deletionTimestamp").is_none()
                && target.selector.matches_resource(&pod.data)
                && crate::controllers::common::is_pod_ready_value(&pod.data)
        })
        .cloned()
        .collect();

    if matching_ready_pods.is_empty() {
        return Ok(Vec::new());
    }
    let runtime = metrics_provider
        .runtime_snapshot_for_pods(&matching_ready_pods)
        .await;

    if hpa.get("apiVersion").and_then(|v| v.as_str()) == Some("autoscaling/v1") {
        if let Some(target_utilization) = spec
            .get("targetCPUUtilizationPercentage")
            .and_then(|v| v.as_i64())
            .filter(|value| *value > 0)
        {
            return Ok(observe_resource_metric(
                "cpu",
                "Utilization",
                target_utilization,
                target,
                &matching_ready_pods,
                &runtime,
            )
            .into_iter()
            .collect());
        }
        return Ok(Vec::new());
    }

    let Some(metrics) = spec.get("metrics").and_then(|v| v.as_array()) else {
        return Ok(Vec::new());
    };

    let mut observations = Vec::new();
    for metric in metrics {
        if metric.get("type").and_then(|v| v.as_str()) != Some("Resource") {
            continue;
        }
        let Some(resource_metric) = metric.get("resource") else {
            continue;
        };
        let name = resource_metric
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("cpu");
        let Some(target_metric) = resource_metric.get("target") else {
            continue;
        };
        let target_type = target_metric
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("Utilization");
        let target_value = match target_type {
            "Utilization" => target_metric
                .get("averageUtilization")
                .and_then(|v| v.as_i64())
                .filter(|value| *value > 0),
            "AverageValue" => target_metric
                .get("averageValue")
                .and_then(|value| parse_resource_quantity_value(name, value))
                .and_then(|value| i64::try_from(value).ok())
                .filter(|value| *value > 0),
            "Value" => target_metric
                .get("value")
                .and_then(|value| parse_resource_quantity_value(name, value))
                .and_then(|value| i64::try_from(value).ok())
                .filter(|value| *value > 0),
            _ => None,
        };

        if let Some(target_value) = target_value
            && let Some(observation) = observe_resource_metric(
                name,
                target_type,
                target_value,
                target,
                &matching_ready_pods,
                &runtime,
            )
        {
            observations.push(observation);
        }
    }
    Ok(observations)
}

fn observe_resource_metric(
    name: &str,
    target_type: &str,
    target_value: i64,
    target: &ScaleTarget,
    pods: &[Resource],
    runtime: &RuntimeMetricsSnapshot,
) -> Option<MetricObservation> {
    let summary = ResourceMetricSummary::from_pods(name, pods, runtime)?;
    let target_value = u64::try_from(target_value).ok()?;
    let desired_replicas = match target_type {
        "Utilization" => {
            let current_utilization = summary.average_utilization?;
            desired_from_ratio(
                target.status_replicas,
                i64::try_from(current_utilization).ok()?,
                i64::try_from(target_value).ok()?,
            )
        }
        "AverageValue" => desired_from_ratio(
            target.status_replicas,
            i64::try_from(summary.average_usage()).ok()?,
            i64::try_from(target_value).ok()?,
        ),
        "Value" => desired_from_ratio(
            target.status_replicas,
            i64::try_from(summary.total_usage).ok()?,
            i64::try_from(target_value).ok()?,
        ),
        _ => return None,
    };

    Some(MetricObservation {
        current_metric: resource_current_metric(name, target_type, &summary).ok()?,
        desired_replicas,
        current_utilization: summary
            .average_utilization
            .and_then(|value| value.try_into().ok()),
    })
}

struct ResourceMetricSummary {
    pod_count: u64,
    total_usage: u64,
    average_utilization: Option<u64>,
}

impl ResourceMetricSummary {
    fn from_pods(
        resource: &str,
        pods: &[Resource],
        runtime: &RuntimeMetricsSnapshot,
    ) -> Option<Self> {
        let mut pod_count = 0_u64;
        let mut total_usage = 0_u64;
        let mut total_request = 0_u64;
        let mut request_complete = true;
        for pod in pods {
            let metric = PodMetric::from_resource(pod, runtime)?;
            total_usage = total_usage.saturating_add(metric.usage_for_resource(resource)?);
            if let Some(request) = pod_request_for_resource(pod, resource) {
                total_request = total_request.saturating_add(request);
            } else {
                request_complete = false;
            }
            pod_count += 1;
        }
        if pod_count == 0 {
            return None;
        }
        let average_utilization = if request_complete && total_request > 0 {
            Some(total_usage.saturating_mul(100).div_ceil(total_request))
        } else {
            None
        };
        Some(Self {
            pod_count,
            total_usage,
            average_utilization,
        })
    }

    fn average_usage(&self) -> u64 {
        self.total_usage.div_ceil(self.pod_count)
    }
}

fn resource_current_metric(
    name: &str,
    target_type: &str,
    summary: &ResourceMetricSummary,
) -> Result<Value> {
    let current = match target_type {
        "Value" => json!({"value": format_resource_quantity(name, summary.total_usage)?}),
        "AverageValue" => {
            json!({"averageValue": format_resource_quantity(name, summary.average_usage())?})
        }
        _ => json!({
            "averageUtilization": summary.average_utilization.unwrap_or(0),
            "averageValue": format_resource_quantity(name, summary.average_usage())?
        }),
    };
    Ok(json!({
        "type": "Resource",
        "resource": {
            "name": name,
            "current": current
        }
    }))
}

fn desired_from_ratio(current_replicas: i64, current_value: i64, target_value: i64) -> i64 {
    if target_value <= 0 {
        return current_replicas;
    }
    ((current_replicas.max(0) * current_value.max(0)) + target_value - 1) / target_value
}

fn build_status(hpa: &Value, decision: &HpaDecision) -> Value {
    let mut status = json!({
        "currentReplicas": decision.current_replicas,
        "desiredReplicas": decision.desired_replicas,
        "observedGeneration": hpa.pointer("/metadata/generation").and_then(|v| v.as_i64()).unwrap_or(1),
        "conditions": build_conditions(hpa, decision)
    });

    if hpa.get("apiVersion").and_then(|v| v.as_str()) == Some("autoscaling/v1") {
        if decision.scale_active {
            status["currentCPUUtilizationPercentage"] =
                json!(decision.current_cpu_utilization.unwrap_or(0));
        }
    } else if !decision.current_metrics.is_empty() {
        status["currentMetrics"] = Value::Array(decision.current_metrics.clone());
    }
    status
}

fn build_conditions(hpa: &Value, decision: &HpaDecision) -> Value {
    let able_status = if decision.target.is_some() {
        "True"
    } else {
        "False"
    };
    let able_reason = decision.inactive_reason.unwrap_or("SucceededGetScale");
    let active_status = if decision.scale_active {
        "True"
    } else {
        "False"
    };
    let active_reason = if decision.scale_active {
        "ValidMetricFound"
    } else {
        decision
            .inactive_reason
            .unwrap_or("FailedGetResourceMetric")
    };
    let (limited_status, limited_reason) = if decision.raw_desired_replicas < decision.min_replicas
    {
        ("True", "TooFewReplicas")
    } else if decision.raw_desired_replicas > decision.max_replicas {
        ("True", "TooManyReplicas")
    } else {
        ("False", "DesiredWithinRange")
    };

    json!([
        condition(
            hpa,
            "AbleToScale",
            able_status,
            able_reason,
            if able_status == "True" {
                "the HPA controller was able to get the target's current scale"
            } else {
                "the HPA controller was unable to get the target's current scale"
            },
        ),
        condition(
            hpa,
            "ScalingActive",
            active_status,
            active_reason,
            if active_status == "True" {
                "the HPA controller calculated replica count from resource metrics"
            } else {
                "the HPA controller was unable to calculate replica count from resource metrics"
            },
        ),
        condition(
            hpa,
            "ScalingLimited",
            limited_status,
            limited_reason,
            if limited_status == "True" {
                "the desired replica count was limited by minReplicas or maxReplicas"
            } else {
                "the desired replica count is within the acceptable range"
            },
        )
    ])
}

fn condition(
    hpa: &Value,
    condition_type: &str,
    status: &str,
    reason: &str,
    message: &str,
) -> Value {
    json!({
        "type": condition_type,
        "status": status,
        "reason": reason,
        "message": message,
        "lastTransitionTime": existing_transition_time(hpa, condition_type, status, reason)
            .unwrap_or_else(crate::utils::k8s_timestamp)
    })
}

fn existing_transition_time(
    hpa: &Value,
    condition_type: &str,
    status: &str,
    reason: &str,
) -> Option<String> {
    hpa.pointer("/status/conditions")
        .and_then(|value| value.as_array())
        .and_then(|conditions| {
            conditions.iter().find_map(|condition| {
                let same_type =
                    condition.get("type").and_then(|v| v.as_str()) == Some(condition_type);
                let same_status = condition.get("status").and_then(|v| v.as_str()) == Some(status);
                let same_reason = condition.get("reason").and_then(|v| v.as_str()) == Some(reason);
                if same_type && same_status && same_reason {
                    condition
                        .get("lastTransitionTime")
                        .and_then(|v| v.as_str())
                        .map(str::to_string)
                } else {
                    None
                }
            })
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datastore::DatastoreBackend;
    use crate::hpa_controller_adapter::{
        reconcile_hpa as reconcile_hpa_root,
        reconcile_hpa_with_metrics as reconcile_hpa_with_metrics_root,
    };
    use klights_node_api::{
        NodeMetricsContainerSample, NodeMetricsPodSample, NodeMetricsResult, NodeMetricsTarget,
    };
    use serde_json::json;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    async fn reconcile_hpa(
        db: &dyn crate::datastore::DatastoreBackend,
        pod_repository: &crate::kubelet::pod_repository::PodRepository,
        hpa: &serde_json::Value,
        node_name: &str,
    ) -> anyhow::Result<()> {
        reconcile_hpa_root(
            db,
            pod_repository,
            crate::controllers::test_utils::non_pod_finalization_port_for_test(),
            hpa,
            node_name,
        )
        .await
    }

    async fn reconcile_hpa_with_metrics(
        db: &dyn crate::datastore::DatastoreBackend,
        pod_repository: &crate::kubelet::pod_repository::PodRepository,
        hpa: &serde_json::Value,
        node_name: &str,
        metrics_provider: &dyn crate::metrics::MetricsProvider,
    ) -> anyhow::Result<()> {
        reconcile_hpa_with_metrics_root(
            db,
            pod_repository,
            crate::controllers::test_utils::non_pod_finalization_port_for_test(),
            &crate::controllers::ControllerCoordination::new(),
            hpa,
            node_name,
            metrics_provider,
        )
        .await
    }

    struct MissingTargetRuntime {
        current: Mutex<Resource>,
        conflict_updates_remaining: AtomicUsize,
        successful_updates: AtomicUsize,
    }

    #[async_trait]
    impl HpaRuntime for MissingTargetRuntime {
        async fn get_hpa(
            &self,
            _api_version: &str,
            _namespace: &str,
            _name: &str,
        ) -> Result<Option<Resource>> {
            Ok(Some(self.current.lock().unwrap().clone()))
        }

        async fn get_scale_target(
            &self,
            _api_version: &str,
            _kind: &str,
            _namespace: &str,
            _name: &str,
        ) -> Result<Option<Resource>> {
            Ok(None)
        }

        async fn list_pods(&self, _namespace: &str) -> Result<Vec<Resource>> {
            unreachable!("missing target never lists Pods")
        }

        async fn patch_scale_target(
            &self,
            _target: &ScaleTarget,
            _replicas: i64,
        ) -> Result<Resource> {
            unreachable!("missing target never scales")
        }

        async fn reconcile_scaled_target(
            &self,
            _target: &ScaleTarget,
            _resource: &Value,
            _node_name: &str,
        ) -> Result<()> {
            unreachable!("missing target never reconciles")
        }

        async fn update_hpa_status(&self, _current: &Resource, status: Value) -> Result<()> {
            if self
                .conflict_updates_remaining
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                anyhow::bail!("synthetic HPA conflict")
            }
            let mut current = self.current.lock().unwrap();
            std::sync::Arc::make_mut(&mut current.data)["status"] = status;
            self.successful_updates.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        fn is_conflict(&self, error: &anyhow::Error) -> bool {
            error.to_string().contains("HPA conflict")
        }
    }

    fn missing_target_runtime(conflicts: usize) -> MissingTargetRuntime {
        let hpa = json!({
            "apiVersion": "autoscaling/v2",
            "kind": "HorizontalPodAutoscaler",
            "metadata": {
                "name": "missing",
                "namespace": "default",
                "uid": "hpa-missing",
                "generation": 1
            },
            "spec": {
                "scaleTargetRef": {
                    "apiVersion": "apps/v1",
                    "kind": "Deployment",
                    "name": "missing"
                },
                "minReplicas": 1,
                "maxReplicas": 3
            }
        });
        MissingTargetRuntime {
            current: Mutex::new(Resource::try_from_data(std::sync::Arc::new(hpa)).unwrap()),
            conflict_updates_remaining: AtomicUsize::new(conflicts),
            successful_updates: AtomicUsize::new(0),
        }
    }

    #[tokio::test]
    async fn missing_target_retries_status_conflict_and_then_stabilizes_as_noop() {
        let runtime = missing_target_runtime(1);
        let hpa = (*runtime.current.lock().unwrap().data).clone();
        let metrics = crate::metrics::FallbackOnlyMetricsProvider;
        reconcile_hpa_with_runtime(&runtime, &hpa, "node-a", &metrics)
            .await
            .unwrap();
        assert_eq!(runtime.successful_updates.load(Ordering::Relaxed), 1);
        assert_eq!(
            runtime
                .current
                .lock()
                .unwrap()
                .data
                .pointer("/status/conditions/0/reason")
                .and_then(Value::as_str),
            Some("FailedGetScale")
        );

        let current = (*runtime.current.lock().unwrap().data).clone();
        reconcile_hpa_with_runtime(&runtime, &current, "node-a", &metrics)
            .await
            .unwrap();
        assert_eq!(
            runtime.successful_updates.load(Ordering::Relaxed),
            1,
            "stable missing-target status must not write again"
        );
    }

    async fn create_ready_pod(
        db: &dyn DatastoreBackend,
        namespace: &str,
        name: &str,
        labels: serde_json::Value,
    ) {
        db.create_resource(
            "v1",
            "Pod",
            Some(namespace),
            name,
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": name,
                    "namespace": namespace,
                    "labels": labels
                },
                "spec": {
                    "containers": [{
                        "name": "app",
                        "image": "nginx",
                        "resources": {"requests": {"cpu": "100m"}}
                    }]
                },
                "status": {
                    "phase": "Running",
                    "conditions": [{"type": "Ready", "status": "True"}],
                    "containerStatuses": [{"name": "app", "ready": true}]
                }
            }),
        )
        .await
        .unwrap();
    }

    #[derive(Clone)]
    struct StaticMetricsProvider {
        runtime: RuntimeMetricsSnapshot,
    }

    #[async_trait::async_trait]
    impl MetricsProvider for StaticMetricsProvider {
        async fn runtime_snapshot_for_pods(&self, _pods: &[Resource]) -> RuntimeMetricsSnapshot {
            self.runtime.clone()
        }
    }

    fn runtime_metrics_for_pods<'a>(
        namespace: &str,
        pod_names: impl IntoIterator<Item = &'a str>,
        cpu_nanos: u64,
        memory_bytes: u64,
    ) -> RuntimeMetricsSnapshot {
        RuntimeMetricsSnapshot::from_node_metrics_results([Ok(NodeMetricsResult::new(
            NodeMetricsTarget::try_new("node-a").unwrap(),
            None,
            pod_names
                .into_iter()
                .map(|name| {
                    NodeMetricsPodSample::new(
                        namespace,
                        name,
                        "",
                        vec![NodeMetricsContainerSample::new(
                            "app",
                            cpu_nanos,
                            memory_bytes,
                        )],
                    )
                })
                .collect(),
        ))])
    }

    #[tokio::test]
    async fn hpa_v2_resource_metric_scales_deployment_from_resource_usage() {
        let db = crate::datastore::test_support::in_memory().await;
        let pod_repository = crate::controllers::test_utils::pod_repository_for_test(&db);

        let _deployment = crate::controllers::test_utils::store_and_prepare(
            &db,
            "apps/v1",
            "Deployment",
            Some("default"),
            "web",
            json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {"name": "web", "namespace": "default", "uid": "deploy-web"},
                "spec": {
                    "replicas": 4,
                    "selector": {"matchLabels": {"app": "web"}},
                    "template": {
                        "metadata": {"labels": {"app": "web"}},
                        "spec": {"containers": [{"name": "app", "image": "nginx"}]}
                    }
                },
                "status": {"replicas": 4, "readyReplicas": 4}
            }),
        )
        .await;

        for index in 0..4 {
            create_ready_pod(
                &db,
                "default",
                &format!("web-{index}"),
                json!({"app": "web"}),
            )
            .await;
        }

        let hpa = crate::controllers::test_utils::store_and_prepare(
            &db,
            "autoscaling/v2",
            "HorizontalPodAutoscaler",
            Some("default"),
            "web",
            json!({
                "apiVersion": "autoscaling/v2",
                "kind": "HorizontalPodAutoscaler",
                "metadata": {"name": "web", "namespace": "default", "uid": "hpa-web", "generation": 1},
                "spec": {
                    "scaleTargetRef": {"apiVersion": "apps/v1", "kind": "Deployment", "name": "web"},
                    "minReplicas": 2,
                    "maxReplicas": 8,
                    "metrics": [{
                        "type": "Resource",
                        "resource": {
                            "name": "cpu",
                            "target": {"type": "Utilization", "averageUtilization": 50}
                        }
                    }]
                }
            }),
        )
        .await;

        let pod_names: Vec<String> = (0..4).map(|index| format!("web-{index}")).collect();
        let pod_name_refs: Vec<&str> = pod_names.iter().map(String::as_str).collect();
        let metrics_provider = StaticMetricsProvider {
            runtime: runtime_metrics_for_pods(
                "default",
                pod_name_refs.iter().copied(),
                100_000_000,
                64 * 1024 * 1024,
            ),
        };
        reconcile_hpa_with_metrics(
            &db,
            pod_repository.as_ref(),
            &hpa,
            "node-a",
            &metrics_provider,
        )
        .await
        .unwrap();

        let deployment = db
            .get_resource("apps/v1", "Deployment", Some("default"), "web")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(deployment.data.pointer("/spec/replicas"), Some(&json!(8)));

        let hpa = db
            .get_resource(
                "autoscaling/v2",
                "HorizontalPodAutoscaler",
                Some("default"),
                "web",
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(hpa.data.pointer("/status/currentReplicas"), Some(&json!(4)));
        assert_eq!(hpa.data.pointer("/status/desiredReplicas"), Some(&json!(8)));
        assert_eq!(
            hpa.data
                .pointer("/status/currentMetrics/0/resource/current/averageUtilization"),
            Some(&json!(100))
        );
        assert_eq!(
            hpa.data.pointer("/status/conditions/0/type"),
            Some(&json!("AbleToScale"))
        );
        assert_eq!(
            hpa.data.pointer("/status/conditions/0/status"),
            Some(&json!("True"))
        );

        let _ = deployment;
    }

    #[tokio::test]
    async fn hpa_v1_cpu_metric_scales_replicationcontroller_from_resource_usage() {
        let db = crate::datastore::test_support::in_memory().await;
        let pod_repository = crate::controllers::test_utils::pod_repository_for_test(&db);

        let _rc = crate::controllers::test_utils::store_and_prepare(
            &db,
            "v1",
            "ReplicationController",
            Some("default"),
            "legacy",
            json!({
                "apiVersion": "v1",
                "kind": "ReplicationController",
                "metadata": {"name": "legacy", "namespace": "default", "uid": "rc-legacy"},
                "spec": {
                    "replicas": 3,
                    "selector": {"app": "legacy"},
                    "template": {
                        "metadata": {"labels": {"app": "legacy"}},
                        "spec": {"containers": [{"name": "app", "image": "nginx"}]}
                    }
                },
                "status": {"replicas": 3, "readyReplicas": 3}
            }),
        )
        .await;

        for index in 0..3 {
            create_ready_pod(
                &db,
                "default",
                &format!("legacy-{index}"),
                json!({"app": "legacy"}),
            )
            .await;
        }

        let hpa = crate::controllers::test_utils::store_and_prepare(
            &db,
            "autoscaling/v1",
            "HorizontalPodAutoscaler",
            Some("default"),
            "legacy",
            json!({
                "apiVersion": "autoscaling/v1",
                "kind": "HorizontalPodAutoscaler",
                "metadata": {"name": "legacy", "namespace": "default", "uid": "hpa-legacy", "generation": 1},
                "spec": {
                    "scaleTargetRef": {"apiVersion": "v1", "kind": "ReplicationController", "name": "legacy"},
                    "minReplicas": 1,
                    "maxReplicas": 5,
                    "targetCPUUtilizationPercentage": 60
                }
            }),
        )
        .await;

        let pod_names: Vec<String> = (0..3).map(|index| format!("legacy-{index}")).collect();
        let pod_name_refs: Vec<&str> = pod_names.iter().map(String::as_str).collect();
        let metrics_provider = StaticMetricsProvider {
            runtime: runtime_metrics_for_pods(
                "default",
                pod_name_refs.iter().copied(),
                100_000_000,
                64 * 1024 * 1024,
            ),
        };
        reconcile_hpa_with_metrics(
            &db,
            pod_repository.as_ref(),
            &hpa,
            "node-a",
            &metrics_provider,
        )
        .await
        .unwrap();

        let rc = db
            .get_resource("v1", "ReplicationController", Some("default"), "legacy")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(rc.data.pointer("/spec/replicas"), Some(&json!(5)));

        let hpa = db
            .get_resource(
                "autoscaling/v1",
                "HorizontalPodAutoscaler",
                Some("default"),
                "legacy",
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(hpa.data.pointer("/status/currentReplicas"), Some(&json!(3)));
        assert_eq!(hpa.data.pointer("/status/desiredReplicas"), Some(&json!(5)));
        assert_eq!(
            hpa.data.pointer("/status/currentCPUUtilizationPercentage"),
            Some(&json!(100))
        );
    }

    #[tokio::test]
    async fn hpa_does_not_scale_when_runtime_metrics_are_unavailable() {
        let db = crate::datastore::test_support::in_memory().await;
        let pod_repository = crate::controllers::test_utils::pod_repository_for_test(&db);

        let _deployment = crate::controllers::test_utils::store_and_prepare(
            &db,
            "apps/v1",
            "Deployment",
            Some("default"),
            "web",
            json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {"name": "web", "namespace": "default", "uid": "deploy-web"},
                "spec": {
                    "replicas": 4,
                    "selector": {"matchLabels": {"app": "web"}},
                    "template": {
                        "metadata": {"labels": {"app": "web"}},
                        "spec": {"containers": [{"name": "app", "image": "nginx"}]}
                    }
                },
                "status": {"replicas": 4, "readyReplicas": 4}
            }),
        )
        .await;

        for index in 0..4 {
            create_ready_pod(
                &db,
                "default",
                &format!("web-{index}"),
                json!({"app": "web"}),
            )
            .await;
        }

        let hpa = crate::controllers::test_utils::store_and_prepare(
            &db,
            "autoscaling/v2",
            "HorizontalPodAutoscaler",
            Some("default"),
            "web",
            json!({
                "apiVersion": "autoscaling/v2",
                "kind": "HorizontalPodAutoscaler",
                "metadata": {"name": "web", "namespace": "default", "uid": "hpa-web", "generation": 1},
                "spec": {
                    "scaleTargetRef": {"apiVersion": "apps/v1", "kind": "Deployment", "name": "web"},
                    "minReplicas": 2,
                    "maxReplicas": 8,
                    "metrics": [{
                        "type": "Resource",
                        "resource": {
                            "name": "cpu",
                            "target": {"type": "Utilization", "averageUtilization": 50}
                        }
                    }]
                }
            }),
        )
        .await;

        reconcile_hpa(&db, pod_repository.as_ref(), &hpa, "node-a")
            .await
            .unwrap();

        let deployment = db
            .get_resource("apps/v1", "Deployment", Some("default"), "web")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(deployment.data.pointer("/spec/replicas"), Some(&json!(4)));

        let hpa = db
            .get_resource(
                "autoscaling/v2",
                "HorizontalPodAutoscaler",
                Some("default"),
                "web",
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(hpa.data.pointer("/status/currentReplicas"), Some(&json!(4)));
        assert_eq!(hpa.data.pointer("/status/desiredReplicas"), Some(&json!(4)));
        assert_eq!(
            hpa.data.pointer("/status/conditions/1/reason"),
            Some(&json!("FailedGetResourceMetric"))
        );
        assert!(hpa.data.pointer("/status/currentMetrics").is_none());
    }
}
