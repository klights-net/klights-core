use serde_json::json;

fn coordination() -> &'static klights_controllers::ControllerCoordination {
    static COORDINATION: std::sync::LazyLock<klights_controllers::ControllerCoordination> =
        std::sync::LazyLock::new(klights_controllers::ControllerCoordination::new);
    &COORDINATION
}

async fn reconcile_deployment<T>(
    db: &T,
    pod_reader: &dyn crate::kubelet::pod_repository::PodReader,
    pod_writer: &dyn crate::kubelet::pod_repository::PodObjectWriter,
    pod_delete_sink: &dyn klights_reconcile_api::GcPodDeleteSink,
    deployment: &serde_json::Value,
    node_name: &str,
) -> anyhow::Result<()>
where
    T: crate::datastore::DatastoreBackend + Clone + 'static,
{
    let non_pod_finalization =
        crate::bootstrap::controller_adapters::gc_delete_adapter::GcNonPodFinalizationAdapter::new(
            std::sync::Arc::new(db.clone()),
        );
    let store = crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerLeaderPort::new(
        std::sync::Arc::new(db.clone()),
    );
    super::reconcile_deployment(
        &store,
        pod_reader,
        pod_writer,
        crate::controller_test_support::deterministic_controller_identity().as_ref(),
        pod_delete_sink,
        &non_pod_finalization,
        deployment,
        crate::controller_test_support::test_reconcile_context(coordination(), node_name),
    )
    .await
}

#[tokio::test]
async fn test_rollover_adoption_redrives_zero_replica_old_rs_pod_delete() {
    let db = crate::datastore::test_support::in_memory().await;
    let pod_repo =
        crate::controller_test_support::deferred_outbox_pod_repository_for_test(&db).await;
    let deploy_uid = "deploy-uid-adopted-rollover";
    let old_rs_uid = "old-rs-uid-adopted-rollover";

    let deployment = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "test-rolling-update-deployment",
            "namespace": "default",
            "uid": deploy_uid,
            "resourceVersion": "0"
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"name": "sample-pod"}},
            "strategy": {
                "type": "RollingUpdate",
                "rollingUpdate": {
                    "maxSurge": 1,
                    "maxUnavailable": 0
                }
            },
            "template": {
                "metadata": {"labels": {"name": "sample-pod"}},
                "spec": {
                    "containers": [{
                        "name": "agnhost",
                        "image": "registry.k8s.io/e2e-test-images/agnhost:2.56"
                    }]
                }
            }
        }
    });
    let created_deploy = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "test-rolling-update-deployment",
            deployment,
        )
        .await
        .unwrap();

    let old_rs = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {
            "name": "test-rolling-update-controller",
            "namespace": "default",
            "uid": old_rs_uid,
            "labels": {"name": "sample-pod", "pod": "httpd"},
            "annotations": {"deployment.kubernetes.io/revision": "1"}
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"name": "sample-pod", "pod": "httpd"}},
            "template": {
                "metadata": {"labels": {"name": "sample-pod", "pod": "httpd"}},
                "spec": {
                    "containers": [{
                        "name": "httpd",
                        "image": "registry.k8s.io/e2e-test-images/httpd:2.4.38-4"
                    }]
                }
            }
        },
        "status": {
            "replicas": 1,
            "readyReplicas": 1,
            "availableReplicas": 1,
            "observedGeneration": 1
        }
    });
    db.create_resource(
        "apps/v1",
        "ReplicaSet",
        Some("default"),
        "test-rolling-update-controller",
        old_rs,
    )
    .await
    .unwrap();

    let old_pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "test-rolling-update-controller-130dc",
            "namespace": "default",
            "uid": "old-pod-uid-adopted-rollover",
            "labels": {"name": "sample-pod", "pod": "httpd"},
            "ownerReferences": [{
                "apiVersion": "apps/v1",
                "kind": "ReplicaSet",
                "name": "test-rolling-update-controller",
                "uid": old_rs_uid,
                "controller": true,
                "blockOwnerDeletion": true
            }]
        },
        "spec": {"containers": [{"name": "httpd", "image": "registry.k8s.io/e2e-test-images/httpd:2.4.38-4"}]},
        "status": {
            "phase": "Running",
            "conditions": [
                {"type": "Ready", "status": "True"},
                {"type": "ContainersReady", "status": "True"}
            ]
        }
    });
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "test-rolling-update-controller-130dc",
        old_pod,
    )
    .await
    .unwrap();

    let deployment_with_rv = crate::controller_test_support::inject_resource_version(
        created_deploy.data,
        created_deploy.resource_version,
    );
    reconcile_deployment(
        &db,
        pod_repo.as_ref(),
        pod_repo.as_ref(),
        pod_repo.as_ref(),
        &deployment_with_rv,
        "test-node",
    )
    .await
    .unwrap();

    let created_pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::new(Some("name=sample-pod"), None, None, None),
        )
        .await
        .unwrap();
    let new_pod = created_pods
        .items
        .iter()
        .find(|pod| pod.uid != "old-pod-uid-adopted-rollover")
        .expect("first rollout reconcile must create a new ReplicaSet pod");
    let mut ready_new_pod = (*new_pod.data).clone();
    ready_new_pod["status"] = json!({
        "phase": "Running",
        "conditions": [
            {"type": "Ready", "status": "True"},
            {"type": "ContainersReady", "status": "True"}
        ]
    });
    db.update_resource(
        "v1",
        "Pod",
        Some("default"),
        &new_pod.name,
        ready_new_pod,
        new_pod.resource_version,
    )
    .await
    .unwrap();

    let current_deployment = db
        .get_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "test-rolling-update-deployment",
        )
        .await
        .unwrap()
        .unwrap();
    let current_deployment_with_rv = crate::controller_test_support::inject_resource_version(
        current_deployment.data,
        current_deployment.resource_version,
    );
    reconcile_deployment(
        &db,
        pod_repo.as_ref(),
        pod_repo.as_ref(),
        pod_repo.as_ref(),
        &current_deployment_with_rv,
        "test-node",
    )
    .await
    .unwrap();

    let live_old_rs = db
        .get_resource(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            "test-rolling-update-controller",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        live_old_rs.data["spec"]["replicas"],
        json!(0),
        "Deployment must scale the adopted old ReplicaSet down during rollout"
    );

    let old_pod_after = db
        .get_resource(
            "v1",
            "Pod",
            Some("default"),
            "test-rolling-update-controller-130dc",
        )
        .await
        .unwrap()
        .unwrap();
    assert!(
        old_pod_after
            .data
            .pointer("/metadata/deletionTimestamp")
            .is_some(),
        "adopted old ReplicaSet pod must be marked terminating through the PodRepository actor-owned delete path"
    );

    let deployment_after = db
        .get_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "test-rolling-update-deployment",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        deployment_after.data["status"]["updatedReplicas"],
        json!(1),
        "rollout status must be able to reach completion after the old pod is terminating"
    );
}
