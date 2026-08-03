use serde_json::Value;
use serde_json::json;
use sha2::{Digest, Sha256};

fn compute_pod_template_hash(template: &Value) -> String {
    let mut hasher = Sha256::new();
    hasher.update(serde_json::to_vec(template).unwrap_or_default());
    let digest = hasher.finalize();
    digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()[..10]
        .to_string()
}

fn coordination() -> &'static crate::ControllerCoordination {
    static COORDINATION: std::sync::LazyLock<crate::ControllerCoordination> =
        std::sync::LazyLock::new(crate::ControllerCoordination::new);
    &COORDINATION
}

async fn reconcile_deployment(
    db: &crate::test_support::TestStore,
    pod_reader: &(impl crate::deployment::DeploymentPodReader + klights_pod_api::PodQuery + ?Sized),
    pod_writer: &(
         impl crate::deployment::DeploymentPodMutation
         + crate::replicaset::ReplicaSetPodMutation
         + ?Sized
     ),
    pod_delete_sink: &dyn klights_reconcile_api::GcPodDeleteSink,
    deployment: &Value,
    node_name: &str,
) -> anyhow::Result<()> {
    super::reconcile_deployment(
        db,
        pod_reader,
        pod_writer,
        crate::test_support::deterministic_controller_identity().as_ref(),
        pod_delete_sink,
        db,
        deployment,
        crate::test_support::test_reconcile_context(coordination(), node_name),
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
