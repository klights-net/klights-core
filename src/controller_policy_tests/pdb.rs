use klights_controllers::pdb::*;

use crate::datastore::DatastoreBackend;
use crate::kubelet::pod_repository::PodReader;
use anyhow::Result;
use async_trait::async_trait;
use klights_reconcile_api::PodEvictionAdmissionOutcome;
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::Notify;

async fn reconcile_pdb(
    store: &(impl PdbStore + ?Sized),
    pods: &(impl PdbPodReader + ?Sized),
    pdb: &Value,
) -> Result<()> {
    reconcile_pdb_at(store, pods, pdb, chrono::Utc::now()).await
}

async fn admit_pod_eviction(
    store: &(impl PdbStore + ?Sized),
    pod: &klights_cluster_core::Resource,
    dry_run: bool,
) -> Result<PodEvictionAdmissionOutcome> {
    admit_pod_eviction_at(store, pod, dry_run, chrono::Utc::now()).await
}

async fn create_pdb(db: &dyn DatastoreBackend, name: &str, namespace: &str, spec: Value) -> Value {
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
        PodReader::get_pod(self.inner.as_ref(), ns, name).await
    }

    async fn get_pod_for_uid(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
    ) -> Result<Option<klights_cluster_core::Resource>> {
        PodReader::get_pod_for_uid(self.inner.as_ref(), ns, name, uid).await
    }

    async fn list_pods(
        &self,
        ns: Option<&str>,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        limit: Option<i64>,
        continue_token: Option<&str>,
    ) -> Result<crate::kubelet::pod_repository::PodResourceList> {
        let pods = PodReader::list_pods(
            self.inner.as_ref(),
            ns,
            label_selector,
            field_selector,
            limit,
            continue_token,
        )
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
        PodReader::list_pods_by_owner_uid(self.inner.as_ref(), ns, owner_uid).await
    }
}

#[async_trait]
impl PdbPodReader for BlockingOncePodReader {
    async fn list_namespace_pods(
        &self,
        namespace: &str,
    ) -> klights_reconcile_api::ControllerStoreResult<Vec<klights_cluster_core::Resource>> {
        PodReader::list_pods(self, Some(namespace), None, None, None, None)
            .await
            .map(|listing| listing.items)
            .map_err(crate::bootstrap::controller_adapters::controller_store_error_adapter::map_controller_store_error)
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

    let repo = crate::controller_test_support::pod_repository_for_test(&db);
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
    let stale_task =
        tokio::spawn(
            async move { reconcile_pdb(&stale_db, stale_reader.as_ref(), &stale_pdb).await },
        );

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
        crate::controller_test_support::pod_repository_for_test(&db).as_ref(),
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
        crate::controller_test_support::pod_repository_for_test(&db).as_ref(),
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
        crate::controller_test_support::pod_repository_for_test(&db).as_ref(),
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
        crate::controller_test_support::pod_repository_for_test(&db).as_ref(),
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
        crate::controller_test_support::pod_repository_for_test(&db).as_ref(),
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
        crate::controller_test_support::pod_repository_for_test(&db).as_ref(),
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
        crate::controller_test_support::pod_repository_for_test(&db).as_ref(),
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
        crate::controller_test_support::pod_repository_for_test(&db).as_ref(),
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
        crate::controller_test_support::pod_repository_for_test(&db).as_ref(),
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
        crate::controller_test_support::pod_repository_for_test(&db).as_ref(),
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
        crate::controller_test_support::pod_repository_for_test(&db).as_ref(),
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
    let pods = crate::controller_test_support::pod_repository_for_test(&db);
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
        let pods = crate::controller_test_support::pod_repository_for_test(&db);
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
