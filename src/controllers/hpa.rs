//! HorizontalPodAutoscaler controller reconcile logic.

use crate::datastore::{
    DatastoreBackend, PatchKind, Resource, ResourcePatchRequest, ResourcePreconditions,
};
use crate::kubelet::pod_repository::{PodReader, PodRepository};
use anyhow::{Context as _, Result, anyhow};
use serde_json::{Value, json};

const MAX_RETRIES: u32 = 5;

pub async fn reconcile_hpa(
    db: &dyn DatastoreBackend,
    pod_repository: &PodRepository,
    hpa: &Value,
    node_name: &str,
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
        let current = db
            .get_resource(
                api_version,
                "HorizontalPodAutoscaler",
                Some(namespace),
                name,
            )
            .await?
            .context("HPA not found")?;

        let decision = evaluate_hpa(db, pod_repository, &current.data, namespace).await?;
        if decision.scale_active
            && let Some(target) = &decision.target
            && target.spec_replicas != decision.desired_replicas
        {
            let patched_target = patch_scale_target(db, target, decision.desired_replicas).await?;
            reconcile_scaled_target(db, pod_repository, &patched_target.data, target, node_name)
                .await?;
        }

        let status = build_status(&current.data, &decision);
        if current.data.get("status") == Some(&status) {
            return Ok(());
        }

        match db
            .update_status_only_with_preconditions(
                api_version,
                "HorizontalPodAutoscaler",
                Some(namespace),
                name,
                status,
                ResourcePreconditions::from_resource(&current),
            )
            .await
        {
            Ok(_) => return Ok(()),
            Err(err) if crate::datastore::errors::is_conflict_error(&err) => {
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
enum ScaleTargetKind {
    Deployment,
    ReplicaSet,
    StatefulSet,
    ReplicationController,
}

struct ScaleTarget {
    api_version: &'static str,
    kind: &'static str,
    name: String,
    namespace: String,
    uid: String,
    selector: crate::label_selector::LabelSelector,
    spec_replicas: i64,
    status_replicas: i64,
    kind_tag: ScaleTargetKind,
}

struct MetricObservation {
    current_metric: Value,
    desired_replicas: i64,
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
    inactive_reason: Option<&'static str>,
}

async fn evaluate_hpa(
    db: &dyn DatastoreBackend,
    pod_reader: &dyn PodReader,
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

    let Some(target) = get_scale_target(db, spec, namespace).await? else {
        return Ok(HpaDecision {
            target: None,
            current_replicas: 0,
            desired_replicas: 0,
            raw_desired_replicas: 0,
            min_replicas,
            max_replicas,
            scale_active: false,
            current_metrics: Vec::new(),
            inactive_reason: Some("FailedGetScale"),
        });
    };

    let observations = observe_metrics(pod_reader, hpa, spec, &target).await?;
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
            inactive_reason: Some("FailedGetResourceMetric"),
        });
    }

    let raw_desired = observations
        .iter()
        .map(|metric| metric.desired_replicas)
        .max()
        .unwrap_or(target.status_replicas);
    let desired = raw_desired.clamp(min_replicas, max_replicas);
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
        inactive_reason: None,
    })
}

async fn get_scale_target(
    db: &dyn DatastoreBackend,
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

    let Some(resource) = db
        .get_resource(api_version, kind, Some(namespace), name)
        .await?
    else {
        return Ok(None);
    };

    let selector = match kind_tag {
        ScaleTargetKind::ReplicationController => {
            crate::label_selector::LabelSelector::from_flat_match_labels(
                resource
                    .data
                    .pointer("/spec/selector")
                    .unwrap_or(&Value::Null),
            )?
        }
        _ => crate::label_selector::LabelSelector::from_k8s_selector(
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
    pod_reader: &dyn PodReader,
    hpa: &Value,
    spec: &Value,
    target: &ScaleTarget,
) -> Result<Vec<MetricObservation>> {
    let pods = pod_reader
        .list_pods(Some(&target.namespace), None, None, None, None)
        .await?;
    let matching_ready_pods = pods
        .items
        .iter()
        .filter(|pod| {
            pod.data.pointer("/metadata/deletionTimestamp").is_none()
                && target.selector.matches_resource(&pod.data)
                && crate::controllers::common::is_pod_ready_value(&pod.data)
        })
        .count() as i64;

    if matching_ready_pods == 0 {
        return Ok(Vec::new());
    }

    if hpa.get("apiVersion").and_then(|v| v.as_str()) == Some("autoscaling/v1") {
        if let Some(target_utilization) = spec
            .get("targetCPUUtilizationPercentage")
            .and_then(|v| v.as_i64())
            .filter(|value| *value > 0)
        {
            return Ok(vec![MetricObservation {
                current_metric: resource_current_metric("cpu", "Utilization"),
                desired_replicas: desired_from_ratio(target.status_replicas, 0, target_utilization),
            }]);
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
        let desired = match target_metric.get("type").and_then(|v| v.as_str()) {
            Some("Utilization") => target_metric
                .get("averageUtilization")
                .and_then(|v| v.as_i64())
                .filter(|value| *value > 0)
                .map(|target_value| desired_from_ratio(target.status_replicas, 0, target_value)),
            Some("AverageValue") => target_metric
                .get("averageValue")
                .and_then(quantity_milli_value)
                .filter(|value| *value > 0)
                .map(|target_value| desired_from_ratio(target.status_replicas, 0, target_value)),
            Some("Value") => target_metric
                .get("value")
                .and_then(quantity_milli_value)
                .filter(|value| *value > 0)
                .map(|target_value| desired_from_ratio(target.status_replicas, 0, target_value)),
            _ => None,
        };

        if let Some(desired_replicas) = desired {
            observations.push(MetricObservation {
                current_metric: resource_current_metric(
                    name,
                    target_metric
                        .get("type")
                        .and_then(|v| v.as_str())
                        .unwrap_or("Utilization"),
                ),
                desired_replicas,
            });
        }
    }
    Ok(observations)
}

fn resource_current_metric(name: &str, target_type: &str) -> Value {
    let current = match target_type {
        "Value" => json!({"value": "0"}),
        "AverageValue" => json!({"averageValue": "0"}),
        _ => json!({"averageUtilization": 0, "averageValue": "0"}),
    };
    json!({
        "type": "Resource",
        "resource": {
            "name": name,
            "current": current
        }
    })
}

fn desired_from_ratio(current_replicas: i64, current_value: i64, target_value: i64) -> i64 {
    if target_value <= 0 {
        return current_replicas;
    }
    ((current_replicas.max(0) * current_value.max(0)) + target_value - 1) / target_value
}

fn quantity_milli_value(value: &Value) -> Option<i64> {
    if let Some(number) = value.as_i64() {
        return Some(number * 1000);
    }
    let raw = value.as_str()?.trim();
    if let Some(milli) = raw.strip_suffix('m') {
        return milli.parse::<i64>().ok();
    }
    raw.parse::<i64>().ok().map(|value| value * 1000)
}

async fn patch_scale_target(
    db: &dyn DatastoreBackend,
    target: &ScaleTarget,
    replicas: i64,
) -> Result<Resource> {
    db.patch_resource_latest_with_preconditions(
        target.api_version,
        target.kind,
        Some(&target.namespace),
        &target.name,
        ResourcePatchRequest::new(
            PatchKind::Merge,
            json!({"spec": {"replicas": replicas.max(0)}}),
            ResourcePreconditions::uid(target.uid.clone()),
        ),
    )
    .await?
    .ok_or_else(|| {
        anyhow!(
            "{} {} disappeared during HPA scale",
            target.kind,
            target.name
        )
    })
}

async fn reconcile_scaled_target(
    db: &dyn DatastoreBackend,
    pod_repository: &PodRepository,
    target_resource: &Value,
    target: &ScaleTarget,
    node_name: &str,
) -> Result<()> {
    match target.kind_tag {
        ScaleTargetKind::Deployment => {
            crate::controllers::deployment::reconcile_deployment(
                db,
                pod_repository,
                pod_repository,
                pod_repository,
                target_resource,
                node_name,
            )
            .await
        }
        ScaleTargetKind::ReplicaSet => {
            crate::controllers::replicaset::reconcile_replicaset(
                db,
                pod_repository,
                pod_repository,
                pod_repository,
                target_resource,
                node_name,
            )
            .await
        }
        ScaleTargetKind::StatefulSet => {
            crate::controllers::statefulset::reconcile_statefulset(
                db,
                pod_repository,
                pod_repository,
                pod_repository,
                target_resource,
                node_name,
            )
            .await
        }
        ScaleTargetKind::ReplicationController => {
            crate::controllers::replicationcontroller::reconcile_replicationcontroller(
                db,
                pod_repository,
                pod_repository,
                pod_repository,
                target_resource,
                node_name,
            )
            .await
        }
    }
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
            status["currentCPUUtilizationPercentage"] = json!(0);
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
    use serde_json::json;

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

    #[tokio::test]
    async fn hpa_v2_resource_metric_scales_deployment_to_min_replicas() {
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
        assert_eq!(deployment.data.pointer("/spec/replicas"), Some(&json!(2)));

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
        assert_eq!(hpa.data.pointer("/status/desiredReplicas"), Some(&json!(2)));
        assert_eq!(
            hpa.data
                .pointer("/status/currentMetrics/0/resource/current/averageUtilization"),
            Some(&json!(0))
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
    async fn hpa_v1_cpu_metric_scales_replicationcontroller_to_min_replicas() {
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

        reconcile_hpa(&db, pod_repository.as_ref(), &hpa, "node-a")
            .await
            .unwrap();

        let rc = db
            .get_resource("v1", "ReplicationController", Some("default"), "legacy")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(rc.data.pointer("/spec/replicas"), Some(&json!(1)));

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
        assert_eq!(hpa.data.pointer("/status/desiredReplicas"), Some(&json!(1)));
        assert_eq!(
            hpa.data.pointer("/status/currentCPUUtilizationPercentage"),
            Some(&json!(0))
        );
    }
}
