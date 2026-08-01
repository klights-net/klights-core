use super::*;
use serde_json::Value;
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
    deployment: &Value,
    node_name: &str,
) -> anyhow::Result<()>
where
    T: crate::datastore::DatastoreBackend + Clone + 'static,
{
    let non_pod_finalization =
        crate::gc_delete_adapter::GcNonPodFinalizationAdapter::new(std::sync::Arc::new(db.clone()));
    let store = crate::controller_runtime_adapter::RootControllerLeaderPort::new(
        std::sync::Arc::new(db.clone()),
    );
    super::reconcile_deployment(
        &store,
        pod_reader,
        pod_writer,
        pod_delete_sink,
        &non_pod_finalization,
        deployment,
        crate::controllers::test_reconcile_context(coordination(), node_name),
    )
    .await
}

fn make_deployment(name: &str, namespace: &str, uid: &str, replicas: i64, rv: &str) -> Value {
    json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "uid": uid,
            "resourceVersion": rv,
            "labels": {"app": name}
        },
        "spec": {
            "replicas": replicas,
            "selector": {"matchLabels": {"app": name}},
            "template": {
                "metadata": {"labels": {"app": name}},
                "spec": {
                    "containers": [{
                        "name": "nginx",
                        "image": "nginx:latest"
                    }]
                }
            }
        }
    })
}

fn make_deployment_with_image(
    name: &str,
    namespace: &str,
    uid: &str,
    replicas: i64,
    rv: &str,
    image: &str,
) -> Value {
    json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "uid": uid,
            "resourceVersion": rv,
            "labels": {"app": name}
        },
        "spec": {
            "replicas": replicas,
            "selector": {"matchLabels": {"app": name}},
            "strategy": {
                "type": "RollingUpdate",
                "rollingUpdate": {
                    "maxSurge": 1,
                    "maxUnavailable": 1
                }
            },
            "template": {
                "metadata": {"labels": {"app": name}},
                "spec": {
                    "containers": [{
                        "name": "nginx",
                        "image": image
                    }]
                }
            }
        }
    })
}
mod core_reconcile_tests;
mod progression_and_rollback_tests;
mod rolling_update_tests;
