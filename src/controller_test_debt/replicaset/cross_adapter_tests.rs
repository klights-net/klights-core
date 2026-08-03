use crate::kubelet::pod_repository::PodApiWriter;
use serde_json::json;

fn coordination() -> &'static klights_controllers::ControllerCoordination {
    static COORDINATION: std::sync::LazyLock<klights_controllers::ControllerCoordination> =
        std::sync::LazyLock::new(klights_controllers::ControllerCoordination::new);
    &COORDINATION
}

async fn reconcile_replicaset<T>(
    db: &T,
    pod_reader: &dyn crate::kubelet::pod_repository::PodReader,
    pod_writer: &dyn crate::kubelet::pod_repository::PodObjectWriter,
    pod_delete_sink: &dyn klights_reconcile_api::GcPodDeleteSink,
    replicaset: &serde_json::Value,
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
    super::reconcile_replicaset(
        &store,
        pod_reader,
        pod_writer,
        crate::controller_test_support::deterministic_controller_identity().as_ref(),
        pod_delete_sink,
        &non_pod_finalization,
        replicaset,
        crate::controller_test_support::test_reconcile_context(coordination(), node_name),
    )
    .await
}

#[tokio::test]
async fn test_replicaset_child_pods_are_scheduled_by_pod_create_pipeline() {
    let db = crate::datastore::test_support::in_memory().await;
    let pod_repo = crate::controller_test_support::pod_repository_for_test(&db);

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
        "test-node",
        json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "test-node"},
            "spec": {},
            "status": {
                "conditions": [{"type": "Ready", "status": "True"}],
                "allocatable": {
                    "cpu": "8",
                    "memory": "8Gi",
                    "pods": "110",
                    "example.com/fakecpu": "0"
                }
            }
        }),
    )
    .await
    .unwrap();

    let rs = db
        .create_resource(
            "apps/v1",
            "ReplicaSet",
            Some("test-ns"),
            "test-rs",
            json!({
                "apiVersion": "apps/v1",
                "kind": "ReplicaSet",
                "metadata": {
                    "name": "test-rs",
                    "namespace": "test-ns",
                    "uid": "rs-test-uid-scheduler"
                },
                "spec": {
                    "replicas": 1,
                    "selector": {"matchLabels": {"app": "test"}},
                    "template": {
                        "metadata": {"labels": {"app": "test"}},
                        "spec": {
                            "containers": [{
                                "name": "nginx",
                                "image": "nginx",
                                "resources": {
                                    "requests": {"example.com/fakecpu": "1"}
                                }
                            }]
                        }
                    }
                }
            }),
        )
        .await
        .unwrap();

    reconcile_replicaset(
        &db,
        pod_repo.as_ref(),
        pod_repo.as_ref(),
        pod_repo.as_ref(),
        &rs.data,
        "test-node",
    )
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
    assert_eq!(pods.items.len(), 1);
    assert!(
        pods.items[0].data.pointer("/spec/nodeName").is_none(),
        "ReplicaSet child pods must not bypass scheduler resource fit by pre-setting nodeName: {:?}",
        pods.items[0].data
    );
    assert_eq!(
        pods.items[0]
            .data
            .pointer("/status/conditions")
            .and_then(|v| v.as_array())
            .and_then(|conditions| {
                conditions.iter().find(|condition| {
                    condition.get("type").and_then(|v| v.as_str()) == Some("PodScheduled")
                })
            })
            .and_then(|condition| condition.get("status"))
            .and_then(|v| v.as_str()),
        Some("False")
    );
}

#[tokio::test]
async fn test_replicaset_child_pods_participate_in_priority_preemption() {
    let db = crate::datastore::test_support::in_memory().await;
    let pod_repo = crate::controller_test_support::pod_repository_for_test(&db);

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
        "test-node",
        json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "test-node"},
            "spec": {"unschedulable": false},
            "status": {
                "conditions": [{"type": "Ready", "status": "True"}],
                "allocatable": {
                    "cpu": "8",
                    "memory": "32Gi",
                    "pods": "110",
                    "example.com/fakecpu": "1k"
                }
            }
        }),
    )
    .await
    .unwrap();

    for (name, value) in [("p1", 1), ("p2", 2), ("p3", 3), ("p4", 4)] {
        db.create_resource(
            "scheduling.k8s.io/v1",
            "PriorityClass",
            None,
            name,
            json!({
                "apiVersion": "scheduling.k8s.io/v1",
                "kind": "PriorityClass",
                "metadata": {"name": name},
                "value": value
            }),
        )
        .await
        .unwrap();
    }

    for (rs_name, uid, request, priority_class) in [
        ("rs-one", "rs-one-uid", "200", "p1"),
        ("rs-two", "rs-two-uid", "300", "p2"),
        ("rs-three", "rs-three-uid", "450", "p3"),
    ] {
        let rs = db
            .create_resource(
                "apps/v1",
                "ReplicaSet",
                Some("test-ns"),
                rs_name,
                json!({
                    "apiVersion": "apps/v1",
                    "kind": "ReplicaSet",
                    "metadata": {
                        "name": rs_name,
                        "namespace": "test-ns",
                        "uid": uid
                    },
                    "spec": {
                        "replicas": 1,
                        "selector": {"matchLabels": {"app": rs_name}},
                        "template": {
                            "metadata": {"labels": {"app": rs_name}},
                            "spec": {
                                "priorityClassName": priority_class,
                                "containers": [{
                                    "name": "c",
                                    "image": "registry.k8s.io/pause:3.10",
                                    "resources": {
                                        "requests": {"example.com/fakecpu": request}
                                    }
                                }]
                            }
                        }
                    }
                }),
            )
            .await
            .unwrap();

        reconcile_replicaset(
            &db,
            pod_repo.as_ref(),
            pod_repo.as_ref(),
            pod_repo.as_ref(),
            &rs.data,
            "test-node",
        )
        .await
        .unwrap();
    }

    pod_repo.schedule_all_unbound_pods().await.unwrap();

    pod_repo
        .api_create_pod(crate::kubelet::pod_repository::PodApiCreateRequest {
            namespace: "test-ns".to_string(),
            name: "pod4".to_string(),
            body: json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": "pod4", "namespace": "test-ns"},
                "spec": {
                    "priorityClassName": "p4",
                    "containers": [{
                        "name": "c",
                        "image": "registry.k8s.io/pause:3.10",
                        "resources": {
                            "requests": {"example.com/fakecpu": "500"}
                        }
                    }]
                }
            }),
            dry_run: false,
            run_admission: true,
        })
        .await
        .unwrap();

    pod_repo.schedule_all_unbound_pods().await.unwrap();
    let scheduled = db
        .get_resource("v1", "Pod", Some("test-ns"), "pod4")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        scheduled
            .data
            .pointer("/spec/nodeName")
            .and_then(|v| v.as_str()),
        Some("test-node")
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
    let active_rs_pods: Vec<_> = pods
        .items
        .iter()
        .filter(|pod| {
            pod.name != "pod4" && pod.data.pointer("/metadata/deletionTimestamp").is_none()
        })
        .collect();
    assert_eq!(
        active_rs_pods.len(),
        1,
        "high-priority pod must preempt enough lower-priority ReplicaSet children, got {:?}",
        pods.items
    );
    assert_eq!(
        active_rs_pods[0]
            .data
            .pointer("/spec/priorityClassName")
            .and_then(|v| v.as_str()),
        Some("p3")
    );
}
#[tokio::test]
async fn test_replicaset_created_pod_gets_api_pipeline_defaults() {
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controller_test_support::pod_repository_for_test(&db);

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "test-ns",
        json!({"metadata": {"name": "test-ns"}}),
    )
    .await
    .unwrap();

    let rs = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {
            "name": "defaults-rs",
            "namespace": "test-ns",
            "uid": "rs-uid-defaults"
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": "demo"}},
            "template": {
                "metadata": {"labels": {"app": "demo"}},
                "spec": {
                    "containers": [{
                        "name": "app",
                        "image": "busybox",
                        "terminationMessagePath": "",
                        "terminationMessagePolicy": "",
                        "livenessProbe": {"httpGet": {"port": 8080, "path": "", "scheme": ""}}
                    }]
                }
            }
        }
    });

    let created = db
        .create_resource("apps/v1", "ReplicaSet", Some("test-ns"), "defaults-rs", rs)
        .await
        .unwrap();
    let mut rs_with_rv: serde_json::Value = (*created.data).clone();
    if let Some(meta) = rs_with_rv
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(created.resource_version.to_string()),
        );
    }

    reconcile_replicaset(
        &db,
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        &rs_with_rv,
        "test-node",
    )
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
    assert_eq!(pods.items.len(), 1);
    let pod = &pods.items[0].data;
    assert_eq!(
        pod.pointer("/status/phase").and_then(|v| v.as_str()),
        Some("Pending")
    );
    assert_eq!(
        pod.pointer("/status/qosClass").and_then(|v| v.as_str()),
        Some("BestEffort")
    );
    assert!(
        pod.pointer("/status/conditions")
            .and_then(|v| v.as_array())
            .is_some_and(|c| !c.is_empty())
    );
    assert_eq!(
        pod.pointer("/spec/containers/0/terminationMessagePath")
            .and_then(|v| v.as_str()),
        Some("/dev/termination-log")
    );
    assert_eq!(
        pod.pointer("/spec/containers/0/terminationMessagePolicy")
            .and_then(|v| v.as_str()),
        Some("File")
    );
    assert_eq!(
        pod.pointer("/spec/containers/0/livenessProbe/httpGet/path")
            .and_then(|v| v.as_str()),
        Some("/")
    );
    assert_eq!(
        pod.pointer("/spec/containers/0/livenessProbe/httpGet/scheme")
            .and_then(|v| v.as_str()),
        Some("HTTP")
    );
}
