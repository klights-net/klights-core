use super::*;
use serde_json::Value;
use serde_json::json;

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
