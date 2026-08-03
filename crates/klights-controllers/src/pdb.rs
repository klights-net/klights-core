//! PodDisruptionBudget controller reconcile logic
//!
//! Computes PDB status fields (expectedPods, currentHealthy, desiredHealthy,
//! disruptionsAllowed) by scanning pods matching the PDB selector.

use crate::common::ControllerStatusStore;
use anyhow::{Context as _, Result};
use async_trait::async_trait;
use klights_cluster_core::{Resource, ResourcePreconditions};
use klights_reconcile_api::{ControllerStoreResult, PodEvictionAdmissionOutcome};
use serde_json::{Value, json};
use std::collections::HashSet;

/// Reconcile a PodDisruptionBudget — update its status fields.
#[async_trait]
pub trait PdbStore: ControllerStatusStore {
    async fn list_pdbs(&self, namespace: &str) -> ControllerStoreResult<Vec<Resource>>;
}

#[async_trait]
pub trait PdbPodReader: Send + Sync {
    async fn list_namespace_pods(&self, namespace: &str) -> ControllerStoreResult<Vec<Resource>>;
}

pub async fn reconcile_pdb_at<Store: PdbStore + ?Sized, Pods: PdbPodReader + ?Sized>(
    store: &Store,
    pod_reader: &Pods,
    pdb: &Value,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    let metadata = pdb.get("metadata").context("PDB missing metadata")?;
    let name = metadata
        .get("name")
        .and_then(|n| n.as_str())
        .context("PDB missing name")?;
    let namespace = metadata
        .get("namespace")
        .and_then(|n| n.as_str())
        .context("PDB missing namespace")?;

    const MAX_RETRIES: u32 = 5;
    let mut last_conflict: Option<anyhow::Error> = None;

    for _ in 0..MAX_RETRIES {
        // Read the PDB before listing pods. If another reconcile writes a
        // fresher status while this attempt is using an older pod snapshot,
        // the status CAS below conflicts and this loop recomputes from a
        // fresh pod list instead of regressing status.
        let current = store
            .get_status_resource("policy/v1", "PodDisruptionBudget", Some(namespace), name)
            .await?
            .context("PDB not found")?;
        let current_metadata = current
            .data
            .get("metadata")
            .context("PDB missing metadata")?;
        let spec = current.data.get("spec").context("PDB missing spec")?;

        // Parse selector — PodDisruptionBudget supports the full LabelSelector
        // shape (matchLabels + matchExpressions) per K8s spec. A missing or
        // null selector matches every pod (caller decides via separate spec
        // validation whether the empty case is meaningful).
        let parsed_selector = match klights_types::LabelSelector::from_k8s_selector(
            spec.get("selector").unwrap_or(&Value::Null),
        ) {
            Ok(s) => s,
            Err(err) => {
                tracing::warn!(?err, pdb = %name, namespace = %namespace, "PDB selector parse failed; treating as match-none");
                return Ok(());
            }
        };

        // List all pods in the namespace
        let pod_list = pod_reader.list_namespace_pods(namespace).await?;

        // Preserve disruptedPods for selected pods that still exist, including
        // the normal eviction window where the pod is terminating.
        let selector_matching_pods: Vec<&klights_cluster_core::Resource> = pod_list
            .iter()
            .filter(|pod| parsed_selector.matches_resource(&pod.data))
            .collect();
        let live_matching_pod_names = selector_matching_pods
            .iter()
            .filter_map(|pod| {
                pod.data
                    .pointer("/metadata/name")
                    .and_then(|name| name.as_str())
                    .map(str::to_string)
            })
            .collect::<HashSet<_>>();
        let disrupted_pods =
            disrupted_pods_for_live_matching_pods(&current.data, &live_matching_pod_names);

        // Filter pods matching the selector (non-terminating)
        let matching_pods: Vec<&klights_cluster_core::Resource> = selector_matching_pods
            .into_iter()
            .filter(|pod| {
                // Exclude terminating pods
                if pod.data.pointer("/metadata/deletionTimestamp").is_some() {
                    return false;
                }
                true
            })
            .collect();

        let expected_pods = matching_pods.len() as i64;

        // Count healthy pods: Running phase with Ready condition True, or Succeeded
        let current_healthy = matching_pods
            .iter()
            .filter(|pod| is_pod_healthy(&pod.data))
            .count() as i64;

        // Compute desiredHealthy from minAvailable or maxUnavailable
        let desired_healthy = compute_desired_healthy(spec, expected_pods);

        let status = build_pdb_status(
            &current.data,
            current_metadata,
            expected_pods,
            current_healthy,
            desired_healthy,
            disrupted_pods,
            now,
        );

        if current.data.get("status") == Some(&status) {
            return Ok(());
        }

        match store
            .update_status(
                "policy/v1",
                "PodDisruptionBudget",
                Some(namespace),
                name,
                status,
                ResourcePreconditions {
                    uid: Some(current.uid.clone()),
                    resource_version: Some(current.resource_version),
                },
            )
            .await
        {
            Ok(_) => return Ok(()),
            Err(err) if store.is_conflict(&err) => {
                last_conflict = Some(err.into());
                continue;
            }
            Err(err) => return Err(err.into()),
        }
    }

    match last_conflict {
        Some(err) => Err(err).context("PDB status update conflict retries exhausted"),
        None => Ok(()),
    }
}

fn build_pdb_status(
    current_pdb: &Value,
    metadata: &Value,
    expected_pods: i64,
    current_healthy: i64,
    desired_healthy: i64,
    disrupted_pods: serde_json::Map<String, Value>,
    now: chrono::DateTime<chrono::Utc>,
) -> Value {
    let disruptions_allowed =
        (current_healthy - desired_healthy - disrupted_pods.len() as i64).max(0);

    // SufficientPods condition: True when currentHealthy >= desiredHealthy.
    let sufficient = current_healthy >= desired_healthy;
    let condition_status = if sufficient { "True" } else { "False" };
    let mut condition = json!({
        "type": "SufficientPods",
        "status": condition_status,
        "reason": if sufficient { "SufficientPods" } else { "InsufficientPods" },
        "message": if sufficient {
            format!("{} pods are available, {} required", current_healthy, desired_healthy)
        } else {
            format!("Have {} healthy pods, need {}", current_healthy, desired_healthy)
        }
    });
    let previous = current_pdb
        .pointer("/status/conditions")
        .and_then(|conditions| conditions.as_array())
        .and_then(|conditions| crate::common::condition_by_type(conditions, "SufficientPods"));
    let now = klights_cluster_core::k8s_time::format_legacy_timestamp(now);
    crate::common::preserve_condition_transition_time(&mut condition, previous, &now);

    let mut status = json!({
        "expectedPods": expected_pods,
        "currentHealthy": current_healthy,
        "desiredHealthy": desired_healthy,
        "disruptionsAllowed": disruptions_allowed,
        "conditions": [condition],
        "observedGeneration": metadata.get("generation").and_then(|g| g.as_i64()).unwrap_or(1)
    });
    if !disrupted_pods.is_empty() {
        status["disruptedPods"] = Value::Object(disrupted_pods);
    }
    status
}

fn disrupted_pods_for_live_matching_pods(
    current_pdb: &Value,
    live_matching_pod_names: &HashSet<String>,
) -> serde_json::Map<String, Value> {
    let Some(disrupted_pods) = current_pdb
        .pointer("/status/disruptedPods")
        .and_then(|value| value.as_object())
    else {
        return serde_json::Map::new();
    };

    disrupted_pods
        .iter()
        .filter(|(pod_name, _)| live_matching_pod_names.contains(*pod_name))
        .map(|(pod_name, disrupted_at)| (pod_name.clone(), disrupted_at.clone()))
        .collect()
}

/// Trigger PDB status reconcile for all PodDisruptionBudgets in a namespace.
/// Called when pods in the namespace are created, updated, or deleted — so PDB
/// status (disruptionsAllowed, currentHealthy, expectedPods) stays current.
pub async fn reconcile_pdbs_for_namespace<Store: PdbStore + ?Sized, Pods: PdbPodReader + ?Sized>(
    store: &Store,
    pod_reader: &Pods,
    namespace: &str,
    now: chrono::DateTime<chrono::Utc>,
) {
    let pdb_list = match store.list_pdbs(namespace).await {
        Ok(list) => list,
        Err(e) => {
            tracing::warn!("Failed to list PDBs in {}: {}", namespace, e);
            return;
        }
    };

    for pdb_resource in pdb_list {
        if let Err(e) = reconcile_pdb_at(store, pod_reader, &pdb_resource.data, now).await {
            tracing::warn!(
                "Failed to reconcile PDB {}/{}: {}",
                namespace,
                pdb_resource
                    .data
                    .pointer("/metadata/name")
                    .and_then(|n| n.as_str())
                    .unwrap_or("<unknown>"),
                e
            );
        }
    }
}

pub async fn reconcile_pdbs_for_namespace_checked<
    Store: PdbStore + ?Sized,
    Pods: PdbPodReader + ?Sized,
>(
    store: &Store,
    pod_reader: &Pods,
    namespace: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    for pdb in store.list_pdbs(namespace).await? {
        reconcile_pdb_at(store, pod_reader, &pdb.data, now).await?;
    }
    Ok(())
}

pub async fn admit_pod_eviction_at<Store: PdbStore + ?Sized>(
    store: &Store,
    pod: &Resource,
    dry_run: bool,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<PodEvictionAdmissionOutcome> {
    if can_ignore_pdb_for_eviction(&pod.data) {
        return Ok(PodEvictionAdmissionOutcome::Allowed);
    }
    let namespace = pod
        .namespace
        .as_deref()
        .context("stored Pod is missing metadata.namespace")?;
    let pod_name = pod
        .data
        .pointer("/metadata/name")
        .and_then(Value::as_str)
        .context("stored Pod is missing metadata.name")?;

    let matching = store
        .list_pdbs(namespace)
        .await?
        .into_iter()
        .filter(|pdb| pod_matches_selector(&pod.data, &pdb.data))
        .collect::<Vec<_>>();
    if matching.len() > 1 {
        return Ok(PodEvictionAdmissionOutcome::MultipleDisruptionBudgets {
            pdb_names: matching
                .iter()
                .filter_map(|pdb| {
                    pdb.data
                        .pointer("/metadata/name")
                        .and_then(Value::as_str)
                        .map(str::to_string)
                })
                .collect(),
        });
    }
    let Some(mut pdb) = matching.into_iter().next() else {
        return Ok(PodEvictionAdmissionOutcome::Allowed);
    };

    const MAX_CONFLICT_RETRIES: usize = 5;
    for attempt in 0..MAX_CONFLICT_RETRIES {
        let pdb_name = pdb
            .data
            .pointer("/metadata/name")
            .and_then(Value::as_str)
            .context("PDB missing metadata.name")?
            .to_string();
        let generation = pdb
            .data
            .pointer("/metadata/generation")
            .and_then(Value::as_i64)
            .unwrap_or(1);
        let observed_generation = pdb
            .data
            .pointer("/status/observedGeneration")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        let current_healthy = pdb
            .data
            .pointer("/status/currentHealthy")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        let desired_healthy = pdb
            .data
            .pointer("/status/desiredHealthy")
            .and_then(Value::as_i64)
            .unwrap_or(0);

        if observed_generation < generation {
            return Ok(PodEvictionAdmissionOutcome::DisruptionBudgetDenied {
                pdb_name,
                desired_healthy,
                current_healthy,
            });
        }

        if !is_pod_healthy(&pod.data)
            && (pdb
                .data
                .pointer("/spec/unhealthyPodEvictionPolicy")
                .and_then(Value::as_str)
                == Some("AlwaysAllow")
                || (current_healthy >= desired_healthy && desired_healthy > 0))
        {
            return Ok(PodEvictionAdmissionOutcome::Allowed);
        }

        let disruptions_allowed = pdb
            .data
            .pointer("/status/disruptionsAllowed")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        if disruptions_allowed < 0 {
            return Ok(PodEvictionAdmissionOutcome::InvalidDisruptionBudget {
                pdb_name,
                message: "disruptionsAllowed is negative".to_string(),
            });
        }
        let disrupted_count = pdb
            .data
            .pointer("/status/disruptedPods")
            .and_then(Value::as_object)
            .map_or(0, serde_json::Map::len);
        if disrupted_count > 2000 {
            return Ok(PodEvictionAdmissionOutcome::InvalidDisruptionBudget {
                pdb_name,
                message: "disruptedPods map is too large".to_string(),
            });
        }
        if disruptions_allowed == 0 {
            return Ok(PodEvictionAdmissionOutcome::DisruptionBudgetDenied {
                pdb_name,
                desired_healthy,
                current_healthy,
            });
        }
        if dry_run {
            return Ok(PodEvictionAdmissionOutcome::Allowed);
        }

        let mut status = pdb.data.get("status").cloned().unwrap_or_else(|| json!({}));
        status["disruptionsAllowed"] = json!(disruptions_allowed - 1);
        if !status.get("disruptedPods").is_some_and(Value::is_object) {
            status["disruptedPods"] = json!({});
        }
        status["disruptedPods"][pod_name] =
            json!(klights_cluster_core::k8s_time::format_legacy_timestamp(now));

        match store
            .update_status(
                "policy/v1",
                "PodDisruptionBudget",
                Some(namespace),
                &pdb_name,
                status,
                ResourcePreconditions {
                    uid: Some(pdb.uid.clone()),
                    resource_version: Some(pdb.resource_version),
                },
            )
            .await
        {
            Ok(_) => return Ok(PodEvictionAdmissionOutcome::Allowed),
            Err(error) if store.is_conflict(&error) && attempt + 1 < MAX_CONFLICT_RETRIES => {
                pdb = store
                    .get_status_resource(
                        "policy/v1",
                        "PodDisruptionBudget",
                        Some(namespace),
                        &pdb_name,
                    )
                    .await?
                    .context("PDB disappeared during eviction admission")?;
            }
            Err(error) => return Err(error.into()),
        }
    }

    unreachable!("bounded PDB admission retry loop always returns")
}

fn pod_matches_selector(pod: &Value, pdb: &Value) -> bool {
    let selector = pdb.pointer("/spec/selector").unwrap_or(&Value::Null);
    klights_types::LabelSelector::from_k8s_selector(selector)
        .is_ok_and(|selector| selector.matches_resource(pod))
}

fn can_ignore_pdb_for_eviction(pod: &Value) -> bool {
    matches!(
        pod.pointer("/status/phase").and_then(Value::as_str),
        Some("Succeeded" | "Failed" | "Pending")
    ) || pod.pointer("/metadata/deletionTimestamp").is_some()
}

/// A pod is healthy if it is Running with Ready=True, or Succeeded.
fn is_pod_healthy(pod: &Value) -> bool {
    let phase = pod
        .pointer("/status/phase")
        .and_then(|p| p.as_str())
        .unwrap_or("");

    if phase == "Succeeded" {
        return true;
    }

    if phase != "Running" {
        return false;
    }

    crate::common::is_pod_ready_value(pod)
}

/// Compute desiredHealthy from spec.minAvailable or spec.maxUnavailable.
/// minAvailable takes precedence. Both support integer or percentage string ("50%").
fn compute_desired_healthy(spec: &Value, expected_pods: i64) -> i64 {
    if let Some(min_available) = spec.get("minAvailable") {
        return parse_int_or_percent(min_available, expected_pods);
    }

    if let Some(max_unavailable) = spec.get("maxUnavailable") {
        let unavailable = parse_int_or_percent(max_unavailable, expected_pods);
        return (expected_pods - unavailable).max(0);
    }

    // Default: protect 1 pod (minAvailable=1)
    1
}

/// Parse an IntOrString value: integer or "N%" percentage of total.
fn parse_int_or_percent(value: &Value, total: i64) -> i64 {
    if let Some(n) = value.as_i64() {
        return n;
    }
    if let Some(s) = value.as_str()
        && let Some(pct_str) = s.strip_suffix('%')
        && let Ok(pct) = pct_str.parse::<i64>()
    {
        return (total * pct + 99) / 100; // ceiling division
    }
    0
}

#[cfg(test)]
#[path = "pdb_tests.rs"]
mod tests;
