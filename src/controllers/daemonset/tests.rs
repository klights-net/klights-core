use super::*;
use crate::datastore::Resource;
use crate::datastore::sqlite::Datastore;

fn active_pods(items: &[Resource]) -> Vec<&Resource> {
    items
        .iter()
        .filter(|pod| pod.data.pointer("/metadata/deletionTimestamp").is_none())
        .collect()
}

/// Test-only shim that mirrors the public `reconcile_daemonset` signature
/// before the Task 18 migration. Builds a `PodRepository` over the supplied
/// in-memory `Datastore`.
async fn reconcile_daemonset_test(db: &Datastore, daemonset: &Value) -> anyhow::Result<()> {
    let repo = crate::controllers::test_utils::pod_repository_for_test(db);
    super::reconcile_daemonset(db, repo.as_ref(), repo.as_ref(), repo.as_ref(), daemonset).await
}

async fn setup_db_with_node(node_name: &str) -> Datastore {
    let db = crate::datastore::test_support::in_memory().await;
    db.create_resource(
        "v1",
        "Namespace",
        None,
        "test-ns",
        json!({"metadata": {"name": "test-ns"}}),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Node",
        None,
        node_name,
        json!({"metadata": {"name": node_name}, "spec": {}}),
    )
    .await
    .unwrap();
    db
}

async fn setup_db_with_nodes(node_names: &[&str]) -> Datastore {
    let db = crate::datastore::test_support::in_memory().await;
    db.create_namespace("test-ns", json!({"metadata": {"name": "test-ns"}}))
        .await
        .unwrap();
    for node_name in node_names {
        db.create_resource(
            "v1",
            "Node",
            None,
            node_name,
            json!({"metadata": {"name": node_name}, "spec": {}}),
        )
        .await
        .unwrap();
    }
    db
}

fn make_daemonset(name: &str, uid: &str) -> Value {
    json!({
        "apiVersion": "apps/v1",
        "kind": "DaemonSet",
        "metadata": {"name": name, "namespace": "test-ns", "uid": uid},
        "spec": {
            "selector": {"matchLabels": {"app": "daemon"}},
            "template": {
                "metadata": {"labels": {"app": "daemon"}},
                "spec": {"containers": [{"name": "agent", "image": "busybox"}]}
            }
        }
    })
}

fn daemonset_revision_status(db_ds: &Value) -> (String, String) {
    (
        db_ds
            .pointer("/status/currentRevision")
            .and_then(|v| v.as_str())
            .expect("currentRevision")
            .to_string(),
        db_ds
            .pointer("/status/updateRevision")
            .and_then(|v| v.as_str())
            .expect("updateRevision")
            .to_string(),
    )
}

async fn mark_all_daemonset_pods_ready(db: &Datastore, namespace: &str) {
    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some(namespace),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    for pod in pods.items {
        let mut pod_data: serde_json::Value = (*pod.data).clone();
        pod_data["status"] = json!({
            "phase": "Running",
            "conditions": [
                {"type": "Ready", "status": "True"},
                {"type": "ContainersReady", "status": "True"}
            ],
            "containerStatuses": [{"name": "agent", "ready": true}]
        });
        db.update_resource(
            "v1",
            "Pod",
            Some(namespace),
            &pod.name,
            pod_data,
            pod.resource_version,
        )
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn test_daemonset_stale_snapshot_after_delete_does_not_recreate_pods() {
    let db = setup_db_with_node("node-1").await;

    let ds = make_daemonset("stale-ds", "ds-uid-stale");
    let created = db
        .create_resource("apps/v1", "DaemonSet", Some("test-ns"), "stale-ds", ds)
        .await
        .unwrap();
    let stale_snapshot = created.data.clone();

    db.delete_resource("apps/v1", "DaemonSet", Some("test-ns"), "stale-ds")
        .await
        .unwrap();

    reconcile_daemonset_test(&db, &stale_snapshot)
        .await
        .unwrap();

    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert!(
        pods.items.is_empty(),
        "stale DaemonSet reconcile after delete must not recreate Pods"
    );

    let revisions = db
        .list_resources(
            "apps/v1",
            "ControllerRevision",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert!(
        revisions.items.is_empty(),
        "stale DaemonSet reconcile after delete must not create ControllerRevisions"
    );
}

#[tokio::test]
async fn test_daemonset_creates_one_pod_per_node() {
    let db = setup_db_with_node("node-1").await;

    let ds = make_daemonset("test-ds", "ds-uid-001");
    let created = db
        .create_resource("apps/v1", "DaemonSet", Some("test-ns"), "test-ds", ds)
        .await
        .unwrap();

    let mut ds_with_rv: serde_json::Value = (*created.data).clone();
    if let Some(meta) = ds_with_rv
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(created.resource_version.to_string()),
        );
    }

    reconcile_daemonset_test(&db, &ds_with_rv).await.unwrap();

    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(
        pods.items.len(),
        1,
        "DaemonSet should create 1 pod for 1 node"
    );

    // Verify pod is scheduled on the node
    let pod_node = pods.items[0].data["spec"]["nodeName"].as_str().unwrap();
    assert_eq!(pod_node, "node-1");

    // Verify owner reference
    let owner_refs = pods.items[0].data["metadata"]["ownerReferences"]
        .as_array()
        .unwrap();
    assert_eq!(owner_refs[0]["kind"], "DaemonSet");
    assert_eq!(owner_refs[0]["uid"], "ds-uid-001");
}

#[tokio::test]
async fn test_daemonset_idempotent_no_duplicate_pods() {
    let db = setup_db_with_node("node-1").await;

    let ds = make_daemonset("idem-ds", "ds-uid-002");
    let created = db
        .create_resource("apps/v1", "DaemonSet", Some("test-ns"), "idem-ds", ds)
        .await
        .unwrap();

    let mut ds_with_rv: serde_json::Value = (*created.data).clone();
    if let Some(meta) = ds_with_rv
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(created.resource_version.to_string()),
        );
    }

    reconcile_daemonset_test(&db, &ds_with_rv).await.unwrap();

    // Re-fetch and reconcile again
    let current = db
        .get_resource("apps/v1", "DaemonSet", Some("test-ns"), "idem-ds")
        .await
        .unwrap()
        .unwrap();

    let mut ds_with_rv2: serde_json::Value = (*current.data).clone();
    if let Some(meta) = ds_with_rv2
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(current.resource_version.to_string()),
        );
    }

    reconcile_daemonset_test(&db, &ds_with_rv2).await.unwrap();

    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(
        pods.items.len(),
        1,
        "Idempotent reconcile must not create duplicates"
    );
}

#[tokio::test]
async fn test_daemonset_prunes_duplicate_active_pods_per_node() {
    let db = setup_db_with_nodes(&["node-1", "node-2"]).await;

    let ds = make_daemonset("prune-ds", "ds-uid-prune");
    let created = db
        .create_resource("apps/v1", "DaemonSet", Some("test-ns"), "prune-ds", ds)
        .await
        .unwrap();

    reconcile_daemonset_test(&db, &created.data).await.unwrap();

    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(pods.items.len(), 2);
    let hash = pods.items[0]
        .data
        .pointer("/metadata/labels/controller-revision-hash")
        .and_then(|v| v.as_str())
        .expect("daemonset pod revision hash")
        .to_string();

    for (pod_name, node_name) in [
        ("prune-ds-extra-a", "node-1"),
        ("prune-ds-extra-b", "node-1"),
        ("prune-ds-extra-c", "node-2"),
    ] {
        db.create_resource(
            "v1",
            "Pod",
            Some("test-ns"),
            pod_name,
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": pod_name,
                    "namespace": "test-ns",
                    "labels": {
                        "app": "daemon",
                        "controller-revision-hash": hash
                    },
                    "ownerReferences": [{
                        "apiVersion": "apps/v1",
                        "kind": "DaemonSet",
                        "name": "prune-ds",
                        "uid": "ds-uid-prune",
                        "controller": true,
                        "blockOwnerDeletion": true
                    }]
                },
                "spec": {
                    "nodeName": node_name,
                    "containers": [{"name": "agent", "image": "busybox"}]
                },
                "status": {"phase": "Running"}
            }),
        )
        .await
        .unwrap();
    }

    let current = db
        .get_resource("apps/v1", "DaemonSet", Some("test-ns"), "prune-ds")
        .await
        .unwrap()
        .unwrap();
    reconcile_daemonset_test(&db, &current.data).await.unwrap();

    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    let active = active_pods(&pods.items);
    assert_eq!(
        active.len(),
        2,
        "DaemonSet reconcile must mark duplicate active pods terminating"
    );
    for node_name in ["node-1", "node-2"] {
        let count = active
            .iter()
            .filter(|pod| {
                pod.data.pointer("/spec/nodeName").and_then(|v| v.as_str()) == Some(node_name)
            })
            .count();
        assert_eq!(count, 1, "expected one DaemonSet pod on {node_name}");
    }

    let ds = db
        .get_resource("apps/v1", "DaemonSet", Some("test-ns"), "prune-ds")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ds.data["status"]["desiredNumberScheduled"], json!(2));
    assert_eq!(ds.data["status"]["currentNumberScheduled"], json!(2));
    assert_eq!(ds.data["status"]["updatedNumberScheduled"], json!(2));
}

#[tokio::test]
async fn test_daemonset_creates_controller_revision_for_template() {
    let db = setup_db_with_node("node-1").await;

    let ds = make_daemonset("revision-ds", "ds-uid-revision");
    let created = db
        .create_resource("apps/v1", "DaemonSet", Some("test-ns"), "revision-ds", ds)
        .await
        .unwrap();

    reconcile_daemonset_test(&db, &created.data).await.unwrap();

    let revisions = db
        .list_resources(
            "apps/v1",
            "ControllerRevision",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::new(
                Some("daemonset-name=revision-ds"),
                None,
                None,
                None,
            ),
        )
        .await
        .unwrap();
    assert_eq!(revisions.items.len(), 1);
    let revision = &revisions.items[0].data;
    assert_eq!(
        revision["metadata"]["ownerReferences"][0]["uid"],
        "ds-uid-revision"
    );
    assert_eq!(
        revision["metadata"]["labels"]["daemonset-name"],
        "revision-ds"
    );
    assert!(
        revision["metadata"]["labels"]["controller-revision-hash"]
            .as_str()
            .is_some_and(|hash| !hash.is_empty())
    );
    assert_eq!(
        revision["data"],
        super::daemonset_template_patch(&created.data["spec"]["template"])
    );
    assert_eq!(revision["data"]["spec"]["template"]["$patch"], "replace");
    assert!(
        revision["data"]["spec"]["template"]["metadata"]
            .get("creationTimestamp")
            .is_none(),
        "ControllerRevision data must match Kubernetes daemon getPatch output and must not synthesize omitted ObjectMeta zero fields"
    );
    assert_eq!(
        revision["data"]["spec"]["template"]["spec"]["containers"][0]["resources"],
        json!({}),
        "Kubernetes daemon getPatch output includes empty ResourceRequirements for typed containers"
    );
}

#[tokio::test]
async fn test_daemonset_rollback_reuses_revision_and_preserves_matching_pod() {
    let db = setup_db_with_nodes(&["node-1", "node-2"]).await;

    let ds_v1 = json!({
        "apiVersion": "apps/v1",
        "kind": "DaemonSet",
        "metadata": {"name": "rollback-ds", "namespace": "test-ns", "uid": "ds-rollback-uid"},
        "spec": {
            "updateStrategy": {
                "type": "RollingUpdate",
                "rollingUpdate": {"maxUnavailable": 1}
            },
            "selector": {"matchLabels": {"app": "daemon"}},
            "template": {
                "metadata": {"labels": {"app": "daemon", "version": "v1"}},
                "spec": {"containers": [{"name": "agent", "image": "busybox:v1"}]}
            }
        }
    });
    let created = db
        .create_resource(
            "apps/v1",
            "DaemonSet",
            Some("test-ns"),
            "rollback-ds",
            ds_v1.clone(),
        )
        .await
        .unwrap();
    reconcile_daemonset_test(&db, &created.data).await.unwrap();

    let after_v1 = db
        .get_resource("apps/v1", "DaemonSet", Some("test-ns"), "rollback-ds")
        .await
        .unwrap()
        .unwrap();
    let (v1_current_revision, v1_update_revision) = daemonset_revision_status(&after_v1.data);
    assert_eq!(v1_current_revision, v1_update_revision);
    mark_all_daemonset_pods_ready(&db, "test-ns").await;

    let current_ds = db
        .get_resource("apps/v1", "DaemonSet", Some("test-ns"), "rollback-ds")
        .await
        .unwrap()
        .unwrap();
    let mut ds_v2 = ds_v1.clone();
    ds_v2["spec"]["template"]["metadata"]["labels"]["version"] = json!("v2");
    ds_v2["spec"]["template"]["spec"]["containers"][0]["image"] = json!("busybox:v2");
    let updated_v2 = db
        .update_resource(
            "apps/v1",
            "DaemonSet",
            Some("test-ns"),
            "rollback-ds",
            ds_v2,
            current_ds.resource_version,
        )
        .await
        .unwrap();
    reconcile_daemonset_test(&db, &updated_v2.data)
        .await
        .unwrap();

    let after_v2 = db
        .get_resource("apps/v1", "DaemonSet", Some("test-ns"), "rollback-ds")
        .await
        .unwrap()
        .unwrap();
    let (current_during_rollout, v2_update_revision) = daemonset_revision_status(&after_v2.data);
    assert_eq!(
        current_during_rollout, v1_current_revision,
        "currentRevision should remain on the pod revision still present during rollout"
    );
    assert_ne!(v2_update_revision, v1_update_revision);

    let pods_during_rollout = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    let preserved_v1_pod = pods_during_rollout
        .items
        .iter()
        .find(|pod| {
            pod.data["spec"]["containers"][0]["image"] == "busybox:v1"
                && pod
                    .data
                    .pointer("/metadata/name")
                    .and_then(|v| v.as_str())
                    .is_some()
        })
        .expect("one v1 pod should remain during maxUnavailable=1 rollout");
    let preserved_v1_pod_name = preserved_v1_pod.data["metadata"]["name"]
        .as_str()
        .unwrap()
        .to_string();

    let current_ds = db
        .get_resource("apps/v1", "DaemonSet", Some("test-ns"), "rollback-ds")
        .await
        .unwrap()
        .unwrap();
    let rollback = db
        .update_resource(
            "apps/v1",
            "DaemonSet",
            Some("test-ns"),
            "rollback-ds",
            ds_v1,
            current_ds.resource_version,
        )
        .await
        .unwrap();
    reconcile_daemonset_test(&db, &rollback.data).await.unwrap();

    let pods_after_rollback = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert!(
        pods_after_rollback
            .items
            .iter()
            .any(|pod| pod.data["metadata"]["name"] == preserved_v1_pod_name),
        "rollback must not delete a pod that already matches the rollback target"
    );

    let after_rollback = db
        .get_resource("apps/v1", "DaemonSet", Some("test-ns"), "rollback-ds")
        .await
        .unwrap()
        .unwrap();
    let (rollback_current_revision, rollback_update_revision) =
        daemonset_revision_status(&after_rollback.data);
    assert_eq!(rollback_update_revision, v1_update_revision);
    assert_eq!(rollback_current_revision, v1_current_revision);

    let revisions = db
        .list_resources(
            "apps/v1",
            "ControllerRevision",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::new(
                Some("daemonset-name=rollback-ds"),
                None,
                None,
                None,
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        revisions.items.len(),
        2,
        "rollback to an existing template must reuse the existing ControllerRevision"
    );
    let original_revision = revisions
        .items
        .iter()
        .find(|revision| revision.name == v1_update_revision)
        .expect("original revision should still exist");
    assert_eq!(
        original_revision.data["data"],
        super::daemonset_template_patch(&rollback.data["spec"]["template"])
    );
}

#[test]
fn daemonset_controller_revision_patch_prunes_protobuf_zero_noise() {
    let template = json!({
        "metadata": {
            "name": "",
            "namespace": "",
            "uid": "",
            "resourceVersion": "",
            "generation": 0,
            "labels": {"daemonset-name": "daemon-set"}
        },
        "spec": {
            "restartPolicy": "",
            "dnsPolicy": "",
            "hostNetwork": false,
            "nodeName": "",
            "serviceAccountName": "",
            "schedulerName": "",
            "securityContext": {},
            "volumes": [],
            "containers": [{
                "name": "app",
                "image": "registry.k8s.io/e2e-test-images/agnhost:2.56",
                "imagePullPolicy": "",
                "terminationMessagePath": "",
                "terminationMessagePolicy": "",
                "securityContext": {},
                "ports": [{
                    "name": "",
                    "containerPort": 9376,
                    "protocol": "",
                    "hostPort": 0
                }]
            }]
        }
    });

    assert_eq!(
        super::daemonset_template_patch(&template),
        json!({
            "spec": {"template": {
                "$patch": "replace",
                "metadata": {"labels": {"daemonset-name": "daemon-set"}},
                "spec": {
                    "containers": [{
                        "image": "registry.k8s.io/e2e-test-images/agnhost:2.56",
                        "name": "app",
                        "ports": [{"containerPort": 9376}],
                        "resources": {},
                        "securityContext": {}
                    }],
                    "securityContext": {}
                }
            }}
        })
    );
}

#[tokio::test]
async fn test_daemonset_status_updated() {
    let db = setup_db_with_node("node-1").await;

    let ds = make_daemonset("status-ds", "ds-uid-003");
    let created = db
        .create_resource("apps/v1", "DaemonSet", Some("test-ns"), "status-ds", ds)
        .await
        .unwrap();

    let mut ds_with_rv: serde_json::Value = (*created.data).clone();
    if let Some(meta) = ds_with_rv
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(created.resource_version.to_string()),
        );
    }

    reconcile_daemonset_test(&db, &ds_with_rv).await.unwrap();

    let updated = db
        .get_resource("apps/v1", "DaemonSet", Some("test-ns"), "status-ds")
        .await
        .unwrap()
        .unwrap();

    let status = &updated.data["status"];
    assert_eq!(status["desiredNumberScheduled"], 1);
    assert_eq!(status["currentNumberScheduled"], 1);
}

#[tokio::test]
async fn test_daemonset_respects_template_node_selector() {
    let db = setup_db_with_node("node-1").await;

    let mut ds = make_daemonset("selector-ds", "ds-uid-selector");
    ds["spec"]["template"]["spec"]["nodeSelector"] = json!({"daemonset-color": "blue"});
    let created = db
        .create_resource("apps/v1", "DaemonSet", Some("test-ns"), "selector-ds", ds)
        .await
        .unwrap();

    reconcile_daemonset_test(&db, &created.data).await.unwrap();
    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(
        pods.items.len(),
        0,
        "unmatched nodeSelector must not create pods"
    );

    let node = db
        .get_resource("v1", "Node", None, "node-1")
        .await
        .unwrap()
        .unwrap();
    let mut node_body: serde_json::Value = (*node.data).clone();
    node_body["metadata"]["labels"] = json!({"daemonset-color": "blue"});
    db.update_resource(
        "v1",
        "Node",
        None,
        "node-1",
        node_body,
        node.resource_version,
    )
    .await
    .unwrap();

    let current = db
        .get_resource("apps/v1", "DaemonSet", Some("test-ns"), "selector-ds")
        .await
        .unwrap()
        .unwrap();
    reconcile_daemonset_test(&db, &current.data).await.unwrap();
    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(
        pods.items.len(),
        1,
        "matching nodeSelector must create one pod"
    );
}

#[tokio::test]
async fn test_daemonset_reconcile_preserves_external_status_conditions() {
    let db = setup_db_with_node("node-1").await;

    let mut ds = make_daemonset("condition-ds", "ds-uid-condition");
    ds["status"] = json!({
        "conditions": [{
            "type": "StatusUpdate",
            "status": "True",
            "reason": "E2E",
            "message": "Set from e2e test"
        }]
    });
    let created = db
        .create_resource("apps/v1", "DaemonSet", Some("test-ns"), "condition-ds", ds)
        .await
        .unwrap();

    reconcile_daemonset_test(&db, &created.data).await.unwrap();

    let updated = db
        .get_resource("apps/v1", "DaemonSet", Some("test-ns"), "condition-ds")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        updated.data["status"]["conditions"][0]["type"],
        json!("StatusUpdate")
    );
    assert_eq!(
        updated.data["status"]["conditions"][0]["reason"],
        json!("E2E")
    );
}

#[tokio::test]
async fn test_daemonset_no_nodes_creates_no_pods() {
    let db = crate::datastore::test_support::in_memory().await;
    db.create_resource(
        "v1",
        "Namespace",
        None,
        "test-ns",
        json!({"metadata": {"name": "test-ns"}}),
    )
    .await
    .unwrap();
    // No nodes created

    let ds = make_daemonset("empty-ds", "ds-uid-004");
    let created = db
        .create_resource("apps/v1", "DaemonSet", Some("test-ns"), "empty-ds", ds)
        .await
        .unwrap();

    let mut ds_with_rv: serde_json::Value = (*created.data).clone();
    if let Some(meta) = ds_with_rv
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(created.resource_version.to_string()),
        );
    }

    reconcile_daemonset_test(&db, &ds_with_rv).await.unwrap();

    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(pods.items.len(), 0, "No nodes = no pods");
}

// S4.5 DaemonSet update strategy tests

#[tokio::test]
async fn test_daemonset_on_delete_strategy() {
    // OnDelete: template changes do NOT trigger automatic pod updates
    // Users must manually delete pods to get new template
    let db = setup_db_with_node("node-1").await;

    let ds = json!({
        "apiVersion": "apps/v1",
        "kind": "DaemonSet",
        "metadata": {"name": "ondelete-ds", "namespace": "test-ns", "uid": "ds-ondelete-001"},
        "spec": {
            "updateStrategy": {"type": "OnDelete"},
            "selector": {"matchLabels": {"app": "daemon"}},
            "template": {
                "metadata": {"labels": {"app": "daemon"}},
                "spec": {"containers": [{"name": "agent", "image": "busybox:v1"}]}
            }
        }
    });

    let created = db
        .create_resource("apps/v1", "DaemonSet", Some("test-ns"), "ondelete-ds", ds)
        .await
        .unwrap();

    let mut ds_with_rv: serde_json::Value = (*created.data).clone();
    if let Some(meta) = ds_with_rv
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(created.resource_version.to_string()),
        );
    }

    // First reconcile - creates pod with v1 image
    reconcile_daemonset_test(&db, &ds_with_rv).await.unwrap();

    let pods_v1 = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(pods_v1.items.len(), 1);
    let pod_v1_name = pods_v1.items[0].data["metadata"]["name"].as_str().unwrap();

    // Update DaemonSet template to v2 image
    let current_ds = db
        .get_resource("apps/v1", "DaemonSet", Some("test-ns"), "ondelete-ds")
        .await
        .unwrap()
        .unwrap();

    let updated_ds = db.update_resource(
            "apps/v1",
            "DaemonSet",
            Some("test-ns"),
            "ondelete-ds",
            json!({
                "apiVersion": "apps/v1",
                "kind": "DaemonSet",
                "metadata": {"name": "ondelete-ds", "namespace": "test-ns", "uid": "ds-ondelete-001"},
                "spec": {
                    "updateStrategy": {"type": "OnDelete"},
                    "selector": {"matchLabels": {"app": "daemon"}},
                    "template": {
                        "metadata": {"labels": {"app": "daemon"}},
                        "spec": {"containers": [{"name": "agent", "image": "busybox:v2"}]}
                    }
                }
            }),
            current_ds.resource_version,
        )
        .await
        .unwrap();

    let mut ds_with_rv2: serde_json::Value = (*updated_ds.data).clone();
    if let Some(meta) = ds_with_rv2
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(updated_ds.resource_version.to_string()),
        );
    }

    // Reconcile after template change - OnDelete should NOT delete old pod
    reconcile_daemonset_test(&db, &ds_with_rv2).await.unwrap();

    let pods_after = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(
        pods_after.items.len(),
        1,
        "OnDelete: old pod should remain after template change"
    );
    assert_eq!(
        pods_after.items[0].data["metadata"]["name"]
            .as_str()
            .unwrap(),
        pod_v1_name,
        "OnDelete: same pod name should exist (not recreated)"
    );
}

#[tokio::test]
async fn test_daemonset_rolling_update_strategy() {
    // RollingUpdate: template changes trigger automatic pod deletion and recreation
    let db = setup_db_with_node("node-1").await;

    let ds = json!({
        "apiVersion": "apps/v1",
        "kind": "DaemonSet",
        "metadata": {"name": "rolling-ds", "namespace": "test-ns", "uid": "ds-rolling-001"},
        "spec": {
            "updateStrategy": {
                "type": "RollingUpdate",
                "rollingUpdate": {"maxUnavailable": 1}
            },
            "selector": {"matchLabels": {"app": "daemon"}},
            "template": {
                "metadata": {"labels": {"app": "daemon", "version": "v1"}},
                "spec": {"containers": [{"name": "agent", "image": "busybox:v1"}]}
            }
        }
    });

    let created = db
        .create_resource("apps/v1", "DaemonSet", Some("test-ns"), "rolling-ds", ds)
        .await
        .unwrap();

    let mut ds_with_rv: serde_json::Value = (*created.data).clone();
    if let Some(meta) = ds_with_rv
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(created.resource_version.to_string()),
        );
    }

    // First reconcile - creates pod with v1
    reconcile_daemonset_test(&db, &ds_with_rv).await.unwrap();

    let pods_v1 = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(pods_v1.items.len(), 1);
    let pod_v1_name = pods_v1.items[0].data["metadata"]["name"]
        .as_str()
        .unwrap()
        .to_string();

    // Update template to v2
    let current_ds = db
        .get_resource("apps/v1", "DaemonSet", Some("test-ns"), "rolling-ds")
        .await
        .unwrap()
        .unwrap();

    let updated_ds = db
        .update_resource(
            "apps/v1",
            "DaemonSet",
            Some("test-ns"),
            "rolling-ds",
            json!({
                "apiVersion": "apps/v1",
                "kind": "DaemonSet",
                "metadata": {"name": "rolling-ds", "namespace": "test-ns", "uid": "ds-rolling-001"},
                "spec": {
                    "updateStrategy": {
                        "type": "RollingUpdate",
                        "rollingUpdate": {"maxUnavailable": 1}
                    },
                    "selector": {"matchLabels": {"app": "daemon"}},
                    "template": {
                        "metadata": {"labels": {"app": "daemon", "version": "v2"}},
                        "spec": {"containers": [{"name": "agent", "image": "busybox:v2"}]}
                    }
                }
            }),
            current_ds.resource_version,
        )
        .await
        .unwrap();

    let mut ds_with_rv2: serde_json::Value = (*updated_ds.data).clone();
    if let Some(meta) = ds_with_rv2
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(updated_ds.resource_version.to_string()),
        );
    }

    // Reconcile after template change - RollingUpdate should delete old pod
    reconcile_daemonset_test(&db, &ds_with_rv2).await.unwrap();

    let pods_after_update = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    let active_after_update = active_pods(&pods_after_update.items);

    // RollingUpdate should either:
    // 1. Have recreated the pod (1 pod with different name), OR
    // 2. Be in the process (0 pods temporarily if old deleted, new not yet created)
    // For this test, we expect the old pod to be deleted (different pod name or count)
    if active_after_update.len() == 1 {
        let new_pod_name = active_after_update[0].data["metadata"]["name"]
            .as_str()
            .unwrap();
        assert_ne!(
            new_pod_name, pod_v1_name,
            "RollingUpdate: pod should be recreated with new name"
        );
    } else {
        // If 0 active pods, old was marked terminating (acceptable for rolling update)
        assert_eq!(
            active_after_update.len(),
            0,
            "RollingUpdate: old pod marked terminating"
        );
    }
}

#[tokio::test]
async fn test_daemonset_rolling_update_max_unavailable() {
    // maxUnavailable controls how many pods can be down during rolling update
    // This test verifies that with maxUnavailable=1, only 1 pod is deleted at a time
    let db = crate::datastore::test_support::in_memory().await;
    db.create_namespace("test-ns", json!({"metadata": {"name": "test-ns"}}))
        .await
        .unwrap();

    // Create 2 nodes
    db.create_resource(
        "v1",
        "Node",
        None,
        "node-1",
        json!({"metadata": {"name": "node-1"}, "spec": {}}),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Node",
        None,
        "node-2",
        json!({"metadata": {"name": "node-2"}, "spec": {}}),
    )
    .await
    .unwrap();

    let ds = json!({
        "apiVersion": "apps/v1",
        "kind": "DaemonSet",
        "metadata": {"name": "maxu-ds", "namespace": "test-ns", "uid": "ds-maxu-001"},
        "spec": {
            "updateStrategy": {
                "type": "RollingUpdate",
                "rollingUpdate": {"maxUnavailable": 1}
            },
            "selector": {"matchLabels": {"app": "daemon"}},
            "template": {
                "metadata": {"labels": {"app": "daemon"}},
                "spec": {"containers": [{"name": "agent", "image": "busybox:v1"}]}
            }
        }
    });

    let created = db
        .create_resource("apps/v1", "DaemonSet", Some("test-ns"), "maxu-ds", ds)
        .await
        .unwrap();

    let mut ds_with_rv: serde_json::Value = (*created.data).clone();
    if let Some(meta) = ds_with_rv
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(created.resource_version.to_string()),
        );
    }

    // Create initial pods
    reconcile_daemonset_test(&db, &ds_with_rv).await.unwrap();
    mark_all_daemonset_pods_ready(&db, "test-ns").await;

    let pods_initial = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(pods_initial.items.len(), 2, "Should have 2 pods initially");

    // Update template
    let current_ds = db
        .get_resource("apps/v1", "DaemonSet", Some("test-ns"), "maxu-ds")
        .await
        .unwrap()
        .unwrap();

    let updated_ds = db
        .update_resource(
            "apps/v1",
            "DaemonSet",
            Some("test-ns"),
            "maxu-ds",
            json!({
                "apiVersion": "apps/v1",
                "kind": "DaemonSet",
                "metadata": {"name": "maxu-ds", "namespace": "test-ns", "uid": "ds-maxu-001"},
                "spec": {
                    "updateStrategy": {
                        "type": "RollingUpdate",
                        "rollingUpdate": {"maxUnavailable": 1}
                    },
                    "selector": {"matchLabels": {"app": "daemon"}},
                    "template": {
                        "metadata": {"labels": {"app": "daemon"}},
                        "spec": {"containers": [{"name": "agent", "image": "busybox:v2"}]}
                    }
                }
            }),
            current_ds.resource_version,
        )
        .await
        .unwrap();

    let mut ds_with_rv2: serde_json::Value = (*updated_ds.data).clone();
    if let Some(meta) = ds_with_rv2
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(updated_ds.resource_version.to_string()),
        );
    }

    // First reconcile after update - should delete at most 1 pod (maxUnavailable=1)
    reconcile_daemonset_test(&db, &ds_with_rv2).await.unwrap();

    let pods_after = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();

    // With maxUnavailable=1, should mark exactly 1 old pod terminating.
    // Non-surge replacement waits until actor finalization removes that old
    // row so e2e rollback checks cannot observe a stale terminating pod as a
    // still-preserved old instance.
    let active_after = active_pods(&pods_after.items);
    assert_eq!(
        active_after.len(),
        1,
        "maxUnavailable=1: should have exactly 1 active old pod while the other old pod terminates"
    );
    let terminating_old = pods_after
        .items
        .iter()
        .filter(|pod| pod.data.pointer("/metadata/deletionTimestamp").is_some())
        .filter(|pod| {
            pod.data
                .pointer("/spec/containers/0/image")
                .and_then(|image| image.as_str())
                == Some("busybox:v1")
        })
        .count();
    let active_new = pods_after
        .items
        .iter()
        .filter(|pod| pod.data.pointer("/metadata/deletionTimestamp").is_none())
        .filter(|pod| {
            pod.data
                .pointer("/spec/containers/0/image")
                .and_then(|image| image.as_str())
                == Some("busybox:v2")
        })
        .count();
    assert_eq!(
        terminating_old, 1,
        "maxUnavailable=1: should have exactly 1 terminating old pod"
    );
    assert_eq!(
        active_new, 0,
        "maxUnavailable=1: should not create a replacement until old pod finalization"
    );
}

#[tokio::test]
async fn test_daemonset_rolling_update_waits_for_old_pod_final_delete_before_replacement() {
    let db = setup_db_with_nodes(&["node-1", "node-2"]).await;

    let mut ds = make_daemonset("nonsurge-ds", "ds-nonsurge-rollout");
    ds["spec"]["updateStrategy"] = json!({
        "type": "RollingUpdate",
        "rollingUpdate": {"maxUnavailable": 1}
    });
    ds["spec"]["template"]["spec"]["containers"][0]["image"] = json!("busybox:v1");
    let created = db
        .create_resource("apps/v1", "DaemonSet", Some("test-ns"), "nonsurge-ds", ds)
        .await
        .unwrap();
    let ds_with_rv = crate::api::inject_resource_version(created.data, created.resource_version);
    reconcile_daemonset_test(&db, &ds_with_rv).await.unwrap();
    mark_all_daemonset_pods_ready(&db, "test-ns").await;

    let current_ds = db
        .get_resource("apps/v1", "DaemonSet", Some("test-ns"), "nonsurge-ds")
        .await
        .unwrap()
        .unwrap();
    let mut updated_body: serde_json::Value = (*current_ds.data).clone();
    updated_body["spec"]["template"]["spec"]["containers"][0]["image"] = json!("busybox:v2");
    let updated_ds = db
        .update_resource(
            "apps/v1",
            "DaemonSet",
            Some("test-ns"),
            "nonsurge-ds",
            updated_body,
            current_ds.resource_version,
        )
        .await
        .unwrap();
    let updated_for_reconcile =
        crate::api::inject_resource_version(updated_ds.data, updated_ds.resource_version);
    reconcile_daemonset_test(&db, &updated_for_reconcile)
        .await
        .unwrap();

    let pods_after_delete = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    let terminating_old: Vec<_> = pods_after_delete
        .items
        .iter()
        .filter(|pod| pod.data.pointer("/metadata/deletionTimestamp").is_some())
        .filter(|pod| {
            pod.data
                .pointer("/spec/containers/0/image")
                .and_then(|image| image.as_str())
                == Some("busybox:v1")
        })
        .collect();
    assert_eq!(
        terminating_old.len(),
        1,
        "rolling update should mark one old pod terminating"
    );
    let active_new = pods_after_delete
        .items
        .iter()
        .filter(|pod| pod.data.pointer("/metadata/deletionTimestamp").is_none())
        .filter(|pod| {
            pod.data
                .pointer("/spec/containers/0/image")
                .and_then(|image| image.as_str())
                == Some("busybox:v2")
        })
        .count();
    assert_eq!(
        active_new, 0,
        "maxSurge=0 rollout must not create a replacement while the old pod still occupies the node"
    );

    let deleted_name = terminating_old[0].name.clone();
    db.delete_resource("v1", "Pod", Some("test-ns"), &deleted_name)
        .await
        .unwrap();
    let current_ds = db
        .get_resource("apps/v1", "DaemonSet", Some("test-ns"), "nonsurge-ds")
        .await
        .unwrap()
        .unwrap();
    let ds_after_final_delete =
        crate::api::inject_resource_version(current_ds.data, current_ds.resource_version);
    reconcile_daemonset_test(&db, &ds_after_final_delete)
        .await
        .unwrap();

    let pods_after_final_delete = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    let active_new_after_final_delete = pods_after_final_delete
        .items
        .iter()
        .filter(|pod| pod.data.pointer("/metadata/deletionTimestamp").is_none())
        .filter(|pod| {
            pod.data
                .pointer("/spec/containers/0/image")
                .and_then(|image| image.as_str())
                == Some("busybox:v2")
        })
        .count();
    assert_eq!(
        active_new_after_final_delete, 1,
        "replacement should be created after actor finalization removes the old pod row"
    );
}

#[tokio::test]
async fn test_daemonset_rolling_update_waits_when_new_pod_unavailable() {
    let db = setup_db_with_nodes(&["node-1", "node-2"]).await;

    let mut ds = make_daemonset("blocked-rollout-ds", "ds-blocked-rollout");
    ds["spec"]["updateStrategy"] = json!({
        "type": "RollingUpdate",
        "rollingUpdate": {"maxUnavailable": 1}
    });
    ds["spec"]["template"]["spec"]["containers"][0]["image"] = json!("busybox:v1");

    let created = db
        .create_resource(
            "apps/v1",
            "DaemonSet",
            Some("test-ns"),
            "blocked-rollout-ds",
            ds,
        )
        .await
        .unwrap();
    let mut ds_with_rv: serde_json::Value = (*created.data).clone();
    ds_with_rv["metadata"]["resourceVersion"] = json!(created.resource_version.to_string());

    reconcile_daemonset_test(&db, &ds_with_rv).await.unwrap();

    let initial_pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(initial_pods.items.len(), 2);
    mark_all_daemonset_pods_ready(&db, "test-ns").await;

    let current_ds = db
        .get_resource(
            "apps/v1",
            "DaemonSet",
            Some("test-ns"),
            "blocked-rollout-ds",
        )
        .await
        .unwrap()
        .unwrap();
    let mut updated_body: serde_json::Value = (*current_ds.data).clone();
    updated_body["spec"]["template"]["spec"]["containers"][0]["image"] = json!("foo:non-existent");
    let updated_ds = db
        .update_resource(
            "apps/v1",
            "DaemonSet",
            Some("test-ns"),
            "blocked-rollout-ds",
            updated_body,
            current_ds.resource_version,
        )
        .await
        .unwrap();

    let mut updated_for_reconcile: serde_json::Value = (*updated_ds.data).clone();
    updated_for_reconcile["metadata"]["resourceVersion"] =
        json!(updated_ds.resource_version.to_string());
    reconcile_daemonset_test(&db, &updated_for_reconcile)
        .await
        .unwrap();

    let pods_after_first = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    let terminating_old = pods_after_first
        .items
        .iter()
        .find(|pod| {
            pod.data.pointer("/metadata/deletionTimestamp").is_some()
                && pod
                    .data
                    .pointer("/spec/containers/0/image")
                    .and_then(|image| image.as_str())
                    == Some("busybox:v1")
        })
        .expect("first reconcile should mark one old pod terminating")
        .name
        .clone();
    db.delete_resource("v1", "Pod", Some("test-ns"), &terminating_old)
        .await
        .unwrap();

    let ds_after_first = db
        .get_resource(
            "apps/v1",
            "DaemonSet",
            Some("test-ns"),
            "blocked-rollout-ds",
        )
        .await
        .unwrap()
        .unwrap();
    let mut ds_for_second: serde_json::Value = (*ds_after_first.data).clone();
    ds_for_second["metadata"]["resourceVersion"] =
        json!(ds_after_first.resource_version.to_string());
    reconcile_daemonset_test(&db, &ds_for_second).await.unwrap();

    let ds_after_second = db
        .get_resource(
            "apps/v1",
            "DaemonSet",
            Some("test-ns"),
            "blocked-rollout-ds",
        )
        .await
        .unwrap()
        .unwrap();
    let mut ds_for_third: serde_json::Value = (*ds_after_second.data).clone();
    ds_for_third["metadata"]["resourceVersion"] =
        json!(ds_after_second.resource_version.to_string());
    reconcile_daemonset_test(&db, &ds_for_third).await.unwrap();

    let pods_after_second = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    let old_pods = pods_after_second
        .items
        .iter()
        .filter(|pod| pod.data.pointer("/metadata/deletionTimestamp").is_none())
        .filter(|pod| {
            pod.data
                .pointer("/spec/containers/0/image")
                .and_then(|image| image.as_str())
                == Some("busybox:v1")
        })
        .count();
    let new_pods = pods_after_second
        .items
        .iter()
        .filter(|pod| pod.data.pointer("/metadata/deletionTimestamp").is_none())
        .filter(|pod| {
            pod.data
                .pointer("/spec/containers/0/image")
                .and_then(|image| image.as_str())
                == Some("foo:non-existent")
        })
        .count();

    assert_eq!(old_pods, 1, "one old available pod must remain");
    assert_eq!(new_pods, 1, "one unavailable new pod should be in flight");
}

#[tokio::test]
async fn test_daemonset_template_change_detection() {
    // Verify template hash annotation is added and changes are detected
    let db = setup_db_with_node("node-1").await;

    let ds = json!({
        "apiVersion": "apps/v1",
        "kind": "DaemonSet",
        "metadata": {"name": "hash-ds", "namespace": "test-ns", "uid": "ds-hash-001"},
        "spec": {
            "updateStrategy": {"type": "OnDelete"},
            "selector": {"matchLabels": {"app": "daemon"}},
            "template": {
                "metadata": {"labels": {"app": "daemon"}},
                "spec": {"containers": [{"name": "agent", "image": "busybox:v1"}]}
            }
        }
    });

    let created = db
        .create_resource("apps/v1", "DaemonSet", Some("test-ns"), "hash-ds", ds)
        .await
        .unwrap();

    let mut ds_with_rv: serde_json::Value = (*created.data).clone();
    if let Some(meta) = ds_with_rv
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(created.resource_version.to_string()),
        );
    }

    // First reconcile
    reconcile_daemonset_test(&db, &ds_with_rv).await.unwrap();

    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(pods.items.len(), 1);

    // Verify pod has template hash annotation
    let annotations = pods.items[0].data["metadata"].get("annotations");
    assert!(
        annotations.is_some(),
        "Pod should have annotations with template hash"
    );

    // Verify DaemonSet status has currentRevision and updateRevision
    let updated_ds = db
        .get_resource("apps/v1", "DaemonSet", Some("test-ns"), "hash-ds")
        .await
        .unwrap()
        .unwrap();

    let status = &updated_ds.data["status"];
    assert!(
        status.get("currentRevision").is_some(),
        "DaemonSet status should have currentRevision"
    );
    assert!(
        status.get("updateRevision").is_some(),
        "DaemonSet status should have updateRevision"
    );
}

#[tokio::test]
async fn test_daemonset_status_number_ready_counts_ready_pods() {
    let db = setup_db_with_node("node-1").await;

    let ds = make_daemonset("ready-ds", "ds-uid-ready");
    let created = db
        .create_resource("apps/v1", "DaemonSet", Some("test-ns"), "ready-ds", ds)
        .await
        .unwrap();

    let mut ds_with_rv: serde_json::Value = (*created.data).clone();
    if let Some(meta) = ds_with_rv
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(created.resource_version.to_string()),
        );
    }

    // First reconcile — creates pod in Pending state
    reconcile_daemonset_test(&db, &ds_with_rv).await.unwrap();

    // Check status: pod exists but not ready yet
    let ds_after_create = db
        .get_resource("apps/v1", "DaemonSet", Some("test-ns"), "ready-ds")
        .await
        .unwrap()
        .unwrap();
    let status = &ds_after_create.data["status"];
    assert_eq!(status["currentNumberScheduled"], 1);
    assert_eq!(
        status["numberReady"], 0,
        "Pending pod should not be counted as ready"
    );
    assert_eq!(
        status["numberAvailable"], 0,
        "Pending pod should not be available"
    );

    // Now simulate the pod becoming Ready by updating its status
    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    let pod = &pods.items[0];
    let mut pod_data: serde_json::Value = (*pod.data).clone();
    if let Some(status_obj) = pod_data.get_mut("status").and_then(|s| s.as_object_mut()) {
        status_obj.insert("phase".to_string(), json!("Running"));
        status_obj.insert(
            "conditions".to_string(),
            json!([
                {"type": "Ready", "status": "True"},
                {"type": "ContainersReady", "status": "True"}
            ]),
        );
    }
    db.update_resource(
        "v1",
        "Pod",
        Some("test-ns"),
        &pod.name.clone(),
        pod_data,
        pod.resource_version,
    )
    .await
    .unwrap();

    // Re-reconcile — should now count the ready pod
    let ds_for_rereconcile = db
        .get_resource("apps/v1", "DaemonSet", Some("test-ns"), "ready-ds")
        .await
        .unwrap()
        .unwrap();

    let mut ds_data: serde_json::Value = (*ds_for_rereconcile.data).clone();
    if let Some(meta) = ds_data.get_mut("metadata").and_then(|m| m.as_object_mut()) {
        meta.insert(
            "resourceVersion".to_string(),
            json!(ds_for_rereconcile.resource_version.to_string()),
        );
    }
    reconcile_daemonset_test(&db, &ds_data).await.unwrap();

    let ds_final = db
        .get_resource("apps/v1", "DaemonSet", Some("test-ns"), "ready-ds")
        .await
        .unwrap()
        .unwrap();
    let final_status = &ds_final.data["status"];
    assert_eq!(
        final_status["numberReady"], 1,
        "Ready pod should be counted"
    );
    assert_eq!(
        final_status["numberAvailable"], 1,
        "Ready pod should be available"
    );
}

#[tokio::test]
async fn test_daemonset_status_observed_generation() {
    let db = setup_db_with_node("node-1").await;

    let mut ds = make_daemonset("gen-ds", "ds-uid-gen");
    // Set generation on the DaemonSet
    if let Some(meta) = ds.get_mut("metadata").and_then(|m| m.as_object_mut()) {
        meta.insert("generation".to_string(), json!(3));
    }

    let created = db
        .create_resource("apps/v1", "DaemonSet", Some("test-ns"), "gen-ds", ds)
        .await
        .unwrap();

    let mut ds_with_rv: serde_json::Value = (*created.data).clone();
    if let Some(meta) = ds_with_rv
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(created.resource_version.to_string()),
        );
    }

    reconcile_daemonset_test(&db, &ds_with_rv).await.unwrap();

    let updated = db
        .get_resource("apps/v1", "DaemonSet", Some("test-ns"), "gen-ds")
        .await
        .unwrap()
        .unwrap();

    let status = &updated.data["status"];
    assert_eq!(
        status["observedGeneration"], 3,
        "observedGeneration should match metadata.generation"
    );
}

#[tokio::test]
async fn test_daemonset_skips_reconcile_when_deletion_timestamp_set() {
    let db = setup_db_with_node("node-1").await;

    let ds = json!({
        "apiVersion": "apps/v1",
        "kind": "DaemonSet",
        "metadata": {
            "name": "deleting-ds",
            "namespace": "test-ns",
            "uid": "ds-uid-del",
            "deletionTimestamp": "2026-04-12T00:00:00Z"
        },
        "spec": {
            "selector": {"matchLabels": {"app": "daemon"}},
            "template": {
                "metadata": {"labels": {"app": "daemon"}},
                "spec": {"containers": [{"name": "agent", "image": "busybox"}]}
            }
        }
    });

    db.create_resource(
        "apps/v1",
        "DaemonSet",
        Some("test-ns"),
        "deleting-ds",
        ds.clone(),
    )
    .await
    .unwrap();

    reconcile_daemonset_test(&db, &ds).await.unwrap();

    // No pods should be created
    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(
        pods.items.len(),
        0,
        "No pods should be created for a DaemonSet being deleted"
    );
}

#[tokio::test]
async fn test_daemonset_replaces_failed_pod() {
    let db = setup_db_with_node("node-1").await;

    let ds = make_daemonset("fail-ds", "ds-uid-fail");
    let created = db
        .create_resource(
            "apps/v1",
            "DaemonSet",
            Some("test-ns"),
            "fail-ds",
            ds.clone(),
        )
        .await
        .unwrap();
    let ds_with_rv = crate::api::inject_resource_version(created.data, created.resource_version);

    // First reconcile: creates 1 pod
    reconcile_daemonset_test(&db, &ds_with_rv).await.unwrap();

    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(pods.items.len(), 1, "Should have 1 pod");

    // Mark the pod as Failed
    let pod = &pods.items[0];
    let mut failed_pod: serde_json::Value = (*pod.data).clone();
    if let Some(status) = failed_pod.get_mut("status").and_then(|s| s.as_object_mut()) {
        status.insert("phase".to_string(), json!("Failed"));
    }
    db.update_resource(
        "v1",
        "Pod",
        Some("test-ns"),
        &pod.name.clone(),
        failed_pod,
        pod.resource_version,
    )
    .await
    .unwrap();

    // Re-reconcile: should delete Failed pod and create replacement
    let current_ds = db
        .get_resource("apps/v1", "DaemonSet", Some("test-ns"), "fail-ds")
        .await
        .unwrap()
        .unwrap();
    let ds_with_rv2 =
        crate::api::inject_resource_version(current_ds.data, current_ds.resource_version);
    reconcile_daemonset_test(&db, &ds_with_rv2).await.unwrap();

    let pods_after = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    let active_after = active_pods(&pods_after.items);

    // Should have exactly 1 active pod (failed one marked terminating, new one created)
    assert_eq!(
        active_after.len(),
        1,
        "Should have 1 replacement pod after failed pod cleanup is queued"
    );

    // The replacement pod should not be in Failed phase
    let phase = active_after[0]
        .data
        .pointer("/status/phase")
        .and_then(|p| p.as_str())
        .unwrap_or("");
    assert_ne!(
        phase, "Failed",
        "Replacement pod should not be in Failed phase"
    );
}

#[tokio::test]
async fn test_daemonset_respects_pod_resourcequota() {
    let db = setup_db_with_node("node-1").await;

    db.create_resource(
        "v1",
        "ResourceQuota",
        Some("test-ns"),
        "pods-zero",
        json!({
            "apiVersion": "v1",
            "kind": "ResourceQuota",
            "metadata": {"name": "pods-zero", "namespace": "test-ns"},
            "spec": {"hard": {"pods": "0"}},
            "status": {"hard": {"pods": "0"}, "used": {"pods": "0"}}
        }),
    )
    .await
    .unwrap();

    let ds = make_daemonset("quota-ds", "ds-uid-quota");
    let created = db
        .create_resource("apps/v1", "DaemonSet", Some("test-ns"), "quota-ds", ds)
        .await
        .unwrap();
    let ds_with_rv = crate::api::inject_resource_version(created.data, created.resource_version);

    let result = reconcile_daemonset_test(&db, &ds_with_rv).await;
    assert!(
        result.is_err(),
        "DaemonSet reconcile should fail on quota deny"
    );

    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(pods.items.len(), 0, "quota deny must prevent pod creation");
}
