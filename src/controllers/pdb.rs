//! PodDisruptionBudget controller reconcile logic
//!
//! Computes PDB status fields (expectedPods, currentHealthy, desiredHealthy,
//! disruptionsAllowed) by scanning pods matching the PDB selector.

use crate::controllers::common::ControllerStatusStore;
use anyhow::{Context as _, Result};
use async_trait::async_trait;
use klights_cluster_core::{Resource, ResourcePreconditions};
use klights_reconcile_api::{ControllerStoreResult, PodEvictionAdmissionOutcome};
use serde_json::{Value, json};
use std::collections::HashSet;

/// Reconcile a PodDisruptionBudget — update its status fields.
#[async_trait]
pub(crate) trait PdbStore: ControllerStatusStore {
    async fn list_pdbs(&self, namespace: &str) -> ControllerStoreResult<Vec<Resource>>;
}

#[async_trait]
pub(crate) trait PdbPodReader: Send + Sync {
    async fn list_namespace_pods(&self, namespace: &str) -> ControllerStoreResult<Vec<Resource>>;
}

pub(crate) async fn reconcile_pdb<Store: PdbStore + ?Sized, Pods: PdbPodReader + ?Sized>(
    store: &Store,
    pod_reader: &Pods,
    pdb: &Value,
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
        .and_then(|conditions| {
            crate::controllers::common::condition_by_type(conditions, "SufficientPods")
        });
    let now = crate::utils::k8s_timestamp();
    crate::controllers::common::preserve_condition_transition_time(&mut condition, previous, &now);

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
pub(crate) async fn reconcile_pdbs_for_namespace<
    Store: PdbStore + ?Sized,
    Pods: PdbPodReader + ?Sized,
>(
    store: &Store,
    pod_reader: &Pods,
    namespace: &str,
) {
    let pdb_list = match store.list_pdbs(namespace).await {
        Ok(list) => list,
        Err(e) => {
            tracing::warn!("Failed to list PDBs in {}: {}", namespace, e);
            return;
        }
    };

    for pdb_resource in pdb_list {
        if let Err(e) = reconcile_pdb(store, pod_reader, &pdb_resource.data).await {
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

pub(crate) async fn reconcile_pdbs_for_namespace_checked<
    Store: PdbStore + ?Sized,
    Pods: PdbPodReader + ?Sized,
>(
    store: &Store,
    pod_reader: &Pods,
    namespace: &str,
) -> Result<()> {
    for pdb in store.list_pdbs(namespace).await? {
        reconcile_pdb(store, pod_reader, &pdb.data).await?;
    }
    Ok(())
}

pub(crate) async fn admit_pod_eviction<Store: PdbStore + ?Sized>(
    store: &Store,
    pod: &Resource,
    dry_run: bool,
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
        status["disruptedPods"][pod_name] = json!(crate::utils::k8s_timestamp());

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

    crate::controllers::common::is_pod_ready_value(pod)
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
mod tests {
    use super::*;

    use crate::datastore::DatastoreBackend;
    use crate::kubelet::pod_repository::PodReader;
    use async_trait::async_trait;
    use serde_json::json;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use tokio::sync::Notify;

    async fn create_pdb(
        db: &dyn DatastoreBackend,
        name: &str,
        namespace: &str,
        spec: Value,
    ) -> Value {
        let pdb = json!({
            "apiVersion": "policy/v1",
            "kind": "PodDisruptionBudget",
            "metadata": {"name": name, "namespace": namespace, "uid": format!("pdb-uid-{}", name)},
            "spec": spec
        });
        db.create_resource(
            "policy/v1",
            "PodDisruptionBudget",
            Some(namespace),
            name,
            pdb.clone(),
        )
        .await
        .unwrap();
        pdb
    }

    async fn create_pod(
        db: &dyn DatastoreBackend,
        name: &str,
        namespace: &str,
        labels: Value,
        phase: &str,
        ready: bool,
    ) {
        let conditions = if ready {
            json!([{"type": "Ready", "status": "True"}])
        } else {
            json!([{"type": "Ready", "status": "False"}])
        };
        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": name, "namespace": namespace, "labels": labels},
            "spec": {"containers": [{"name": "c", "image": "nginx"}]},
            "status": {"phase": phase, "conditions": conditions}
        });
        db.create_resource("v1", "Pod", Some(namespace), name, pod)
            .await
            .unwrap();
    }

    async fn set_pod_ready(db: &dyn DatastoreBackend, namespace: &str, name: &str) {
        let current = db
            .get_resource("v1", "Pod", Some(namespace), name)
            .await
            .unwrap()
            .unwrap();
        let mut pod: serde_json::Value = (*current.data).clone();
        pod["status"] = json!({
            "phase": "Running",
            "conditions": [{"type": "Ready", "status": "True"}]
        });
        db.update_resource(
            "v1",
            "Pod",
            Some(namespace),
            name,
            pod,
            current.resource_version,
        )
        .await
        .unwrap();
    }

    async fn set_pod_terminating(db: &dyn DatastoreBackend, namespace: &str, name: &str) {
        let current = db
            .get_resource("v1", "Pod", Some(namespace), name)
            .await
            .unwrap()
            .unwrap();
        let mut pod: serde_json::Value = (*current.data).clone();
        pod["metadata"]["deletionTimestamp"] = json!("2026-05-05T20:00:10Z");
        db.update_resource(
            "v1",
            "Pod",
            Some(namespace),
            name,
            pod,
            current.resource_version,
        )
        .await
        .unwrap();
    }

    async fn get_pdb_status(db: &dyn DatastoreBackend, namespace: &str, name: &str) -> Value {
        let r = db
            .get_resource("policy/v1", "PodDisruptionBudget", Some(namespace), name)
            .await
            .unwrap()
            .unwrap();
        r.data["status"].clone()
    }

    struct BlockingOncePodReader {
        inner: Arc<crate::kubelet::pod_repository::PodRepository>,
        listed: Arc<Notify>,
        release: Arc<Notify>,
        block_next_list: AtomicBool,
    }

    #[async_trait]
    impl PodReader for BlockingOncePodReader {
        async fn get_pod(
            &self,
            ns: &str,
            name: &str,
        ) -> Result<Option<klights_cluster_core::Resource>> {
            self.inner.get_pod(ns, name).await
        }

        async fn get_pod_for_uid(
            &self,
            ns: &str,
            name: &str,
            uid: &str,
        ) -> Result<Option<klights_cluster_core::Resource>> {
            self.inner.get_pod_for_uid(ns, name, uid).await
        }

        async fn list_pods(
            &self,
            ns: Option<&str>,
            label_selector: Option<&str>,
            field_selector: Option<&str>,
            limit: Option<i64>,
            continue_token: Option<&str>,
        ) -> Result<crate::kubelet::pod_repository::PodResourceList> {
            let pods = self
                .inner
                .list_pods(ns, label_selector, field_selector, limit, continue_token)
                .await?;
            if self.block_next_list.swap(false, Ordering::SeqCst) {
                self.listed.notify_one();
                self.release.notified().await;
            }
            Ok(pods)
        }

        async fn list_pods_by_owner_uid(
            &self,
            ns: &str,
            owner_uid: &str,
        ) -> Result<Vec<klights_cluster_core::Resource>> {
            self.inner.list_pods_by_owner_uid(ns, owner_uid).await
        }
    }

    #[tokio::test]
    async fn test_pdb_reconcile_does_not_overwrite_fresher_status_after_stale_pod_list() {
        let db = crate::datastore::test_support::in_memory().await;

        let pdb = create_pdb(
            &db,
            "race-pdb",
            "default",
            json!({
                "minAvailable": 2,
                "selector": {"matchLabels": {"app": "race"}}
            }),
        )
        .await;

        create_pod(
            &db,
            "pod-0",
            "default",
            json!({"app": "race"}),
            "Running",
            true,
        )
        .await;
        create_pod(
            &db,
            "pod-1",
            "default",
            json!({"app": "race"}),
            "Pending",
            false,
        )
        .await;
        create_pod(
            &db,
            "pod-2",
            "default",
            json!({"app": "race"}),
            "Pending",
            false,
        )
        .await;

        let repo = crate::controllers::test_utils::pod_repository_for_test(&db);
        let listed = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let stale_reader = Arc::new(BlockingOncePodReader {
            inner: repo.clone(),
            listed: listed.clone(),
            release: release.clone(),
            block_next_list: AtomicBool::new(true),
        });

        let stale_db = db.clone();
        let stale_pdb = pdb.clone();
        let stale_task = tokio::spawn(async move {
            reconcile_pdb(&stale_db, stale_reader.as_ref(), &stale_pdb).await
        });

        listed.notified().await;

        set_pod_ready(&db, "default", "pod-1").await;
        set_pod_ready(&db, "default", "pod-2").await;
        reconcile_pdb(&db, repo.as_ref(), &pdb).await.unwrap();

        let fresh_status = get_pdb_status(&db, "default", "race-pdb").await;
        assert_eq!(fresh_status["currentHealthy"], 3);

        release.notify_one();
        stale_task.await.unwrap().unwrap();

        let final_status = get_pdb_status(&db, "default", "race-pdb").await;
        assert_eq!(
            final_status["currentHealthy"], 3,
            "a stale pod snapshot must not overwrite a fresher PDB status"
        );
    }

    #[tokio::test]
    async fn test_pdb_reconcile_preserves_disrupted_pods_for_existing_pods() {
        let db = crate::datastore::test_support::in_memory().await;

        let pdb = create_pdb(
            &db,
            "disrupted-pdb",
            "default",
            json!({
                "minAvailable": 0,
                "selector": {"matchLabels": {"app": "disrupted"}}
            }),
        )
        .await;
        create_pod(
            &db,
            "pod-0",
            "default",
            json!({"app": "disrupted"}),
            "Running",
            true,
        )
        .await;

        db.update_status_only(
            "policy/v1",
            "PodDisruptionBudget",
            Some("default"),
            "disrupted-pdb",
            json!({
                "disruptedPods": {
                    "pod-0": "2026-05-05T20:00:00Z"
                }
            }),
            None,
        )
        .await
        .unwrap();

        reconcile_pdb(
            &db,
            crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
            &pdb,
        )
        .await
        .unwrap();

        let status = get_pdb_status(&db, "default", "disrupted-pdb").await;
        assert_eq!(
            status.pointer("/disruptedPods/pod-0"),
            Some(&json!("2026-05-05T20:00:00Z")),
            "PDB reconcile must preserve disruptedPods entries while the named pod still exists"
        );
        assert_eq!(
            status["disruptionsAllowed"],
            json!(0),
            "in-flight disrupted pods must consume otherwise allowed disruptions"
        );
    }

    #[tokio::test]
    async fn test_pdb_reconcile_preserves_disrupted_pods_for_terminating_pods() {
        let db = crate::datastore::test_support::in_memory().await;

        let pdb = create_pdb(
            &db,
            "terminating-disrupted-pdb",
            "default",
            json!({
                "minAvailable": 0,
                "selector": {"matchLabels": {"app": "terminating-disrupted"}}
            }),
        )
        .await;
        create_pod(
            &db,
            "pod-0",
            "default",
            json!({"app": "terminating-disrupted"}),
            "Running",
            true,
        )
        .await;
        set_pod_terminating(&db, "default", "pod-0").await;

        db.update_status_only(
            "policy/v1",
            "PodDisruptionBudget",
            Some("default"),
            "terminating-disrupted-pdb",
            json!({
                "disruptedPods": {
                    "pod-0": "2026-05-05T20:00:00Z"
                }
            }),
            None,
        )
        .await
        .unwrap();

        reconcile_pdb(
            &db,
            crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
            &pdb,
        )
        .await
        .unwrap();

        let status = get_pdb_status(&db, "default", "terminating-disrupted-pdb").await;
        assert_eq!(
            status.pointer("/disruptedPods/pod-0"),
            Some(&json!("2026-05-05T20:00:00Z")),
            "PDB reconcile must preserve disruptedPods entries while the named pod is terminating"
        );
        assert_eq!(
            status["disruptionsAllowed"],
            json!(0),
            "terminating disrupted pods must still consume otherwise allowed disruptions"
        );
    }

    #[tokio::test]
    async fn test_pdb_reconcile_preserves_condition_transition_time_when_status_unchanged() {
        let db = crate::datastore::test_support::in_memory().await;

        let pdb = create_pdb(
            &db,
            "stable-condition-pdb",
            "default",
            json!({
                "minAvailable": 1,
                "selector": {"matchLabels": {"app": "stable-pdb"}}
            }),
        )
        .await;
        create_pod(
            &db,
            "pod-0",
            "default",
            json!({"app": "stable-pdb"}),
            "Running",
            true,
        )
        .await;

        db.update_status_only(
            "policy/v1",
            "PodDisruptionBudget",
            Some("default"),
            "stable-condition-pdb",
            json!({
                "expectedPods": 1,
                "currentHealthy": 1,
                "desiredHealthy": 1,
                "disruptionsAllowed": 0,
                "conditions": [{
                    "type": "SufficientPods",
                    "status": "True",
                    "reason": "SufficientPods",
                    "message": "1 pods are available, 1 required",
                    "lastTransitionTime": "2026-06-01T00:00:00Z"
                }],
                "observedGeneration": 1
            }),
            None,
        )
        .await
        .unwrap();

        reconcile_pdb(
            &db,
            crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
            &pdb,
        )
        .await
        .unwrap();

        let status = get_pdb_status(&db, "default", "stable-condition-pdb").await;
        assert_eq!(
            status.pointer("/conditions/0/lastTransitionTime"),
            Some(&json!("2026-06-01T00:00:00Z")),
            "PDB condition transition time must remain stable while status stays True"
        );
        assert!(
            status.pointer("/conditions/0/lastUpdateTime").is_none(),
            "PDB metav1.Condition output must not gain deployment-style lastUpdateTime"
        );
    }

    #[tokio::test]
    async fn test_pdb_reconcile_sets_status_fields() {
        // PDB with minAvailable=1, 3 matching pods (2 healthy, 1 not ready)
        // Expected: expectedPods=3, currentHealthy=2, desiredHealthy=1, disruptionsAllowed=1
        let db = crate::datastore::test_support::in_memory().await;

        let pdb = create_pdb(
            &db,
            "test-pdb",
            "default",
            json!({
                "minAvailable": 1,
                "selector": {"matchLabels": {"app": "myapp"}}
            }),
        )
        .await;

        create_pod(
            &db,
            "pod-1",
            "default",
            json!({"app": "myapp"}),
            "Running",
            true,
        )
        .await;
        create_pod(
            &db,
            "pod-2",
            "default",
            json!({"app": "myapp"}),
            "Running",
            true,
        )
        .await;
        create_pod(
            &db,
            "pod-3",
            "default",
            json!({"app": "myapp"}),
            "Pending",
            false,
        )
        .await;

        reconcile_pdb(
            &db,
            crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
            &pdb,
        )
        .await
        .unwrap();

        let status = get_pdb_status(&db, "default", "test-pdb").await;
        assert_eq!(status["expectedPods"], 3, "expectedPods should be 3");
        assert_eq!(
            status["currentHealthy"], 2,
            "currentHealthy should be 2 (Running+Ready)"
        );
        assert_eq!(
            status["desiredHealthy"], 1,
            "desiredHealthy should be 1 (minAvailable)"
        );
        assert_eq!(
            status["disruptionsAllowed"], 1,
            "disruptionsAllowed = currentHealthy - desiredHealthy = 1"
        );
    }

    #[tokio::test]
    async fn test_pdb_reconcile_zero_disruptions_when_below_min_available() {
        // PDB with minAvailable=3, only 2 healthy pods → disruptionsAllowed=0
        let db = crate::datastore::test_support::in_memory().await;

        let pdb = create_pdb(
            &db,
            "strict-pdb",
            "default",
            json!({
                "minAvailable": 3,
                "selector": {"matchLabels": {"app": "strict"}}
            }),
        )
        .await;

        create_pod(
            &db,
            "pod-a",
            "default",
            json!({"app": "strict"}),
            "Running",
            true,
        )
        .await;
        create_pod(
            &db,
            "pod-b",
            "default",
            json!({"app": "strict"}),
            "Running",
            true,
        )
        .await;

        reconcile_pdb(
            &db,
            crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
            &pdb,
        )
        .await
        .unwrap();

        let status = get_pdb_status(&db, "default", "strict-pdb").await;
        assert_eq!(status["expectedPods"], 2);
        assert_eq!(status["currentHealthy"], 2);
        assert_eq!(status["desiredHealthy"], 3);
        assert_eq!(
            status["disruptionsAllowed"], 0,
            "Cannot disrupt when below minAvailable"
        );
    }

    #[tokio::test]
    async fn test_pdb_reconcile_max_unavailable() {
        // PDB with maxUnavailable=1, 4 pods all healthy
        // desiredHealthy = 4 - 1 = 3, disruptionsAllowed = 4 - 3 = 1
        let db = crate::datastore::test_support::in_memory().await;

        let pdb = create_pdb(
            &db,
            "max-unavail-pdb",
            "default",
            json!({
                "maxUnavailable": 1,
                "selector": {"matchLabels": {"app": "webapp"}}
            }),
        )
        .await;

        for i in 0..4 {
            create_pod(
                &db,
                &format!("pod-{}", i),
                "default",
                json!({"app": "webapp"}),
                "Running",
                true,
            )
            .await;
        }

        reconcile_pdb(
            &db,
            crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
            &pdb,
        )
        .await
        .unwrap();

        let status = get_pdb_status(&db, "default", "max-unavail-pdb").await;
        assert_eq!(status["expectedPods"], 4);
        assert_eq!(status["currentHealthy"], 4);
        assert_eq!(status["desiredHealthy"], 3);
        assert_eq!(status["disruptionsAllowed"], 1);
    }

    #[tokio::test]
    async fn test_pdb_reconcile_selector_match_expressions_in_operator() {
        // PDB with matchExpressions In — must match pods with tier in {fe, be}.
        // Without LabelSelector::from_k8s_selector, the controller silently
        // matches no pods (matchLabels is missing) and disruptionsAllowed is wrong.
        let db = crate::datastore::test_support::in_memory().await;

        let pdb = create_pdb(
            &db,
            "expr-pdb",
            "default",
            json!({
                "minAvailable": 1,
                "selector": {
                    "matchExpressions": [
                        {"key": "tier", "operator": "In", "values": ["fe", "be"]}
                    ]
                }
            }),
        )
        .await;

        create_pod(
            &db,
            "fe-pod",
            "default",
            json!({"tier": "fe"}),
            "Running",
            true,
        )
        .await;
        create_pod(
            &db,
            "be-pod",
            "default",
            json!({"tier": "be"}),
            "Running",
            true,
        )
        .await;
        // Unrelated tier — should not be counted
        create_pod(
            &db,
            "data-pod",
            "default",
            json!({"tier": "data"}),
            "Running",
            true,
        )
        .await;

        reconcile_pdb(
            &db,
            crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
            &pdb,
        )
        .await
        .unwrap();

        let status = get_pdb_status(&db, "default", "expr-pdb").await;
        assert_eq!(
            status["expectedPods"], 2,
            "matchExpressions In should match 2 pods (fe + be)"
        );
        assert_eq!(status["currentHealthy"], 2);
        assert_eq!(status["disruptionsAllowed"], 1);
    }

    #[tokio::test]
    async fn test_pdb_reconcile_selector_match_expressions_exists_operator() {
        // PDB with Exists operator — must match all pods that have the key set.
        let db = crate::datastore::test_support::in_memory().await;

        let pdb = create_pdb(
            &db,
            "exists-pdb",
            "default",
            json!({
                "minAvailable": 1,
                "selector": {
                    "matchExpressions": [
                        {"key": "has-gpu", "operator": "Exists"}
                    ]
                }
            }),
        )
        .await;

        create_pod(
            &db,
            "gpu-pod",
            "default",
            json!({"has-gpu": "true"}),
            "Running",
            true,
        )
        .await;
        create_pod(
            &db,
            "cpu-pod",
            "default",
            json!({"role": "worker"}),
            "Running",
            true,
        )
        .await;

        reconcile_pdb(
            &db,
            crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
            &pdb,
        )
        .await
        .unwrap();

        let status = get_pdb_status(&db, "default", "exists-pdb").await;
        assert_eq!(
            status["expectedPods"], 1,
            "Exists operator should match only the labeled pod"
        );
    }

    #[tokio::test]
    async fn test_pdb_reconcile_selector_match_expressions_does_not_exist_operator() {
        // PDB with DoesNotExist operator — must match all pods missing the key.
        let db = crate::datastore::test_support::in_memory().await;

        let pdb = create_pdb(
            &db,
            "dne-pdb",
            "default",
            json!({
                "minAvailable": 1,
                "selector": {
                    "matchExpressions": [
                        {"key": "deprecated", "operator": "DoesNotExist"}
                    ]
                }
            }),
        )
        .await;

        create_pod(
            &db,
            "current-pod",
            "default",
            json!({"role": "worker"}),
            "Running",
            true,
        )
        .await;
        create_pod(
            &db,
            "deprecated-pod",
            "default",
            json!({"deprecated": "true"}),
            "Running",
            true,
        )
        .await;

        reconcile_pdb(
            &db,
            crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
            &pdb,
        )
        .await
        .unwrap();

        let status = get_pdb_status(&db, "default", "dne-pdb").await;
        assert_eq!(
            status["expectedPods"], 1,
            "DoesNotExist operator should match only the pod without the key"
        );
    }

    #[tokio::test]
    async fn test_pdb_reconcile_selector_match_expressions_not_in_operator() {
        let db = crate::datastore::test_support::in_memory().await;

        let pdb = create_pdb(
            &db,
            "notin-pdb",
            "default",
            json!({
                "minAvailable": 1,
                "selector": {
                    "matchExpressions": [
                        {"key": "tier", "operator": "NotIn", "values": ["fe"]}
                    ]
                }
            }),
        )
        .await;

        create_pod(
            &db,
            "be-pod",
            "default",
            json!({"tier": "be"}),
            "Running",
            true,
        )
        .await;
        create_pod(
            &db,
            "fe-pod",
            "default",
            json!({"tier": "fe"}),
            "Running",
            true,
        )
        .await;
        // Pod missing the key — NotIn matches "absent" per K8s semantics.
        create_pod(
            &db,
            "no-tier-pod",
            "default",
            json!({"role": "worker"}),
            "Running",
            true,
        )
        .await;

        reconcile_pdb(
            &db,
            crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
            &pdb,
        )
        .await
        .unwrap();

        let status = get_pdb_status(&db, "default", "notin-pdb").await;
        assert_eq!(
            status["expectedPods"], 2,
            "NotIn should match be-pod and the no-tier pod (absent label = NotIn match)"
        );
    }

    #[tokio::test]
    async fn test_pdb_reconcile_selector_filters_unrelated_pods() {
        // PDB selector only matches "app=myapp" — unrelated pods should not count
        let db = crate::datastore::test_support::in_memory().await;

        let pdb = create_pdb(
            &db,
            "select-pdb",
            "default",
            json!({
                "minAvailable": 1,
                "selector": {"matchLabels": {"app": "myapp"}}
            }),
        )
        .await;

        create_pod(
            &db,
            "myapp-pod",
            "default",
            json!({"app": "myapp"}),
            "Running",
            true,
        )
        .await;
        // Unrelated pod — should not be counted
        create_pod(
            &db,
            "other-pod",
            "default",
            json!({"app": "other"}),
            "Running",
            true,
        )
        .await;

        reconcile_pdb(
            &db,
            crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
            &pdb,
        )
        .await
        .unwrap();

        let status = get_pdb_status(&db, "default", "select-pdb").await;
        assert_eq!(
            status["expectedPods"], 1,
            "Only pods matching selector should be counted"
        );
        assert_eq!(status["currentHealthy"], 1);
        assert_eq!(
            status["disruptionsAllowed"], 0,
            "1 healthy - 1 minAvailable = 0"
        );
    }

    #[tokio::test]
    async fn eviction_admission_atomically_records_live_disruption_but_not_dry_run() {
        let db = crate::datastore::test_support::in_memory().await;
        let pdb = create_pdb(
            &db,
            "admission-pdb",
            "default",
            json!({
                "minAvailable": 0,
                "selector": {"matchLabels": {"app": "admission"}}
            }),
        )
        .await;
        create_pod(
            &db,
            "victim",
            "default",
            json!({"app": "admission"}),
            "Running",
            true,
        )
        .await;
        let pods = crate::controllers::test_utils::pod_repository_for_test(&db);
        reconcile_pdb(&db, pods.as_ref(), &pdb).await.unwrap();
        let pod = db
            .get_resource("v1", "Pod", Some("default"), "victim")
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            admit_pod_eviction(&db, &pod, true).await.unwrap(),
            PodEvictionAdmissionOutcome::Allowed
        );
        let dry_status = get_pdb_status(&db, "default", "admission-pdb").await;
        assert_eq!(dry_status["disruptionsAllowed"], 1);
        assert!(dry_status.get("disruptedPods").is_none());

        assert_eq!(
            admit_pod_eviction(&db, &pod, false).await.unwrap(),
            PodEvictionAdmissionOutcome::Allowed
        );
        let live_status = get_pdb_status(&db, "default", "admission-pdb").await;
        assert_eq!(live_status["disruptionsAllowed"], 0);
        assert!(
            live_status
                .pointer("/disruptedPods/victim")
                .and_then(Value::as_str)
                .is_some(),
            "live admission must reserve the disruption before Pod termination"
        );
    }

    #[tokio::test]
    async fn unhealthy_pod_policy_allows_only_spec_permitted_budget_safe_evictions() {
        for (name, policy, healthy_count, expected) in [
            ("if-healthy", None, 2, true),
            ("if-unhealthy", None, 1, false),
            ("always", Some("AlwaysAllow"), 1, true),
        ] {
            let db = crate::datastore::test_support::in_memory().await;
            let mut spec = json!({
                "minAvailable": 2,
                "selector": {"matchLabels": {"app": name}}
            });
            if let Some(policy) = policy {
                spec["unhealthyPodEvictionPolicy"] = json!(policy);
            }
            let pdb = create_pdb(&db, name, "default", spec).await;
            for index in 0..healthy_count {
                create_pod(
                    &db,
                    &format!("healthy-{index}"),
                    "default",
                    json!({"app": name}),
                    "Running",
                    true,
                )
                .await;
            }
            create_pod(
                &db,
                "victim",
                "default",
                json!({"app": name}),
                "Running",
                false,
            )
            .await;
            let pods = crate::controllers::test_utils::pod_repository_for_test(&db);
            reconcile_pdb(&db, pods.as_ref(), &pdb).await.unwrap();
            let pod = db
                .get_resource("v1", "Pod", Some("default"), "victim")
                .await
                .unwrap()
                .unwrap();

            let outcome = admit_pod_eviction(&db, &pod, false).await.unwrap();
            assert_eq!(
                matches!(outcome, PodEvictionAdmissionOutcome::Allowed),
                expected,
                "unexpected unhealthy admission outcome for {name}: {outcome:?}"
            );
        }
    }
}
