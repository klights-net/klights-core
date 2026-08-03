use super::*;
use serde_json::{Value, json};

fn coordination() -> &'static klights_controllers::ControllerCoordination {
    static COORDINATION: std::sync::LazyLock<klights_controllers::ControllerCoordination> =
        std::sync::LazyLock::new(klights_controllers::ControllerCoordination::new);
    &COORDINATION
}

async fn reconcile_replicationcontroller(
    db: &crate::datastore::sqlite::Datastore,
    pod_reader: &(impl klights_pod_api::PodQuery + ?Sized),
    pod_writer: &(impl ReplicationControllerPodMutation + ?Sized),
    pod_delete_sink: &dyn klights_reconcile_api::GcPodDeleteSink,
    non_pod_finalization: &dyn klights_reconcile_api::GcNonPodFinalizationPort,
    rc: &Value,
    node_name: &str,
) -> anyhow::Result<()> {
    let store = crate::controller_test_support::controller_store_for_test(db);
    super::reconcile_replicationcontroller(
        &store,
        pod_reader,
        pod_writer,
        crate::controller_test_support::deterministic_controller_identity().as_ref(),
        pod_delete_sink,
        non_pod_finalization,
        rc,
        crate::controller_test_support::test_reconcile_context(coordination(), node_name),
    )
    .await
}

async fn reconcile_rc_test(
    db: &crate::datastore::sqlite::Datastore,
    rc: &Value,
    node_name: &str,
) -> anyhow::Result<()> {
    let identity = crate::controller_test_support::deterministic_controller_identity();
    let repo = crate::controller_test_support::pod_repository_for_test(db);
    let store = crate::controller_test_support::controller_store_for_test(db);
    super::reconcile_replicationcontroller(
        &store,
        repo.as_ref(),
        repo.as_ref(),
        identity.as_ref(),
        repo.as_ref(),
        crate::controller_test_support::non_pod_finalization_port_for_test(),
        rc,
        crate::controller_test_support::test_reconcile_context(coordination(), node_name),
    )
    .await
}

#[tokio::test]
async fn test_rc_adopts_and_releases_through_leader_repository_with_worker_outbox() {
    let db = crate::datastore::test_support::in_memory().await;
    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({"metadata": {"name": "default"}}),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "orphan",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "orphan",
                "namespace": "default",
                "uid": "orphan-uid",
                "labels": {"app": "rc"}
            },
            "spec": {
                "nodeName": "worker-b",
                "containers": [{"name": "app", "image": "nginx"}]
            },
            "status": {"phase": "Running"}
        }),
    )
    .await
    .unwrap();
    let rc = json!({
        "apiVersion": "v1",
        "kind": "ReplicationController",
        "metadata": {"name": "rc", "namespace": "default", "uid": "rc-uid"},
        "spec": {
            "replicas": 1,
            "selector": {"app": "rc"},
            "template": {
                "metadata": {"labels": {"app": "rc"}},
                "spec": {"containers": [{"name": "app", "image": "nginx"}]}
            }
        }
    });
    db.create_resource(
        "v1",
        "ReplicationController",
        Some("default"),
        "rc",
        rc.clone(),
    )
    .await
    .unwrap();
    let repository =
        crate::controller_test_support::deferred_outbox_pod_repository_for_test(&db).await;
    let delete_sink = crate::gc_ownership_integration_tests::NoOpGcPodDeleteSink;

    reconcile_replicationcontroller(
        &db,
        repository.as_ref(),
        repository.as_ref(),
        &delete_sink,
        crate::controller_test_support::non_pod_finalization_port_for_test(),
        &rc,
        "leader",
    )
    .await
    .expect("adopt orphan Pod");

    let adopted = db
        .get_resource("v1", "Pod", Some("default"), "orphan")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        adopted
            .data
            .pointer("/metadata/ownerReferences/0/uid")
            .and_then(serde_json::Value::as_str),
        Some("rc-uid"),
        "RC adoption must be visible in leader storage"
    );

    let mut relabeled = (*adopted.data).clone();
    relabeled["metadata"]["labels"]["app"] = json!("other");
    db.update_resource(
        "v1",
        "Pod",
        Some("default"),
        "orphan",
        relabeled,
        adopted.resource_version,
    )
    .await
    .unwrap();
    reconcile_replicationcontroller(
        &db,
        repository.as_ref(),
        repository.as_ref(),
        &delete_sink,
        crate::controller_test_support::non_pod_finalization_port_for_test(),
        &rc,
        "leader",
    )
    .await
    .expect("release non-matching Pod");

    let released = db
        .get_resource("v1", "Pod", Some("default"), "orphan")
        .await
        .unwrap()
        .unwrap();
    assert!(
        released
            .data
            .pointer("/metadata/ownerReferences")
            .and_then(serde_json::Value::as_array)
            .is_none_or(Vec::is_empty),
        "RC release must be visible in leader storage"
    );
}

#[tokio::test]
async fn rc_reconcile_created_pod_remains_selector_visible_after_annotation_patch() {
    let db = crate::datastore::test_support::in_memory().await;

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "kubectl-rc",
        json!({"apiVersion":"v1","kind":"Namespace","metadata":{"name":"kubectl-rc"}}),
    )
    .await
    .unwrap();

    let rc = json!({
        "apiVersion": "v1",
        "kind": "ReplicationController",
        "metadata": {"name": "agnhost-primary", "namespace": "kubectl-rc", "uid": "agnhost-rc-uid"},
        "spec": {
            "replicas": 1,
            "selector": {"name": "agnhost-primary"},
            "template": {
                "metadata": {"labels": {"name": "agnhost-primary"}},
                "spec": {"containers": [{"name": "agnhost", "image": "registry.k8s.io/e2e-test-images/agnhost:2.56"}]}
            }
        }
    });
    db.create_resource(
        "v1",
        "ReplicationController",
        Some("kubectl-rc"),
        "agnhost-primary",
        rc.clone(),
    )
    .await
    .unwrap();

    reconcile_rc_test(&db, &rc, "worker-a").await.unwrap();

    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("kubectl-rc"),
            crate::datastore::ResourceListQuery::new(
                Some("name=agnhost-primary"),
                None,
                None,
                None,
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        pods.items.len(),
        1,
        "RC reconcile must create one matching pod"
    );
    let pod_name = pods.items[0].name.clone();

    db.patch_resource_latest(
        "v1",
        "Pod",
        Some("kubectl-rc"),
        &pod_name,
        crate::datastore::PatchKind::Merge,
        json!({"metadata": {"annotations": {"patched": "true"}}}),
    )
    .await
    .unwrap();

    let patched = db
        .list_resources(
            "v1",
            "Pod",
            Some("kubectl-rc"),
            crate::datastore::ResourceListQuery::new(
                Some("name=agnhost-primary"),
                None,
                None,
                None,
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        patched.items.len(),
        1,
        "patched RC-owned pod must remain selector-visible"
    );
    assert_eq!(
        patched.items[0].data.pointer("/metadata/labels/name"),
        Some(&json!("agnhost-primary")),
        "metadata annotation patch must preserve selector label"
    );
}
