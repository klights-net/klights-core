//! Full-stack protobuf/controller characterization retained by the root until
//! Phase 8.4 assigns cross-adapter parity fixtures to the base harness.

use crate::protobuf::{decode_protobuf, encode_protobuf};
use serde_json::json;

#[tokio::test]
async fn test_deployment_rs_template_match_full_flow() {
    let db = crate::datastore::test_support::in_memory().await;
    let pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);
    let original_deployment = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "sample-webhook-deployment",
            "namespace": "default",
            "uid": "deploy-webhook-uid-001",
            "labels": {"app": "sample-webhook", "webhook": "true"}
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": "sample-webhook", "webhook": "true"}},
            "strategy": {"type": "RollingUpdate"},
            "template": {
                "metadata": {"labels": {"app": "sample-webhook", "webhook": "true"}},
                "spec": {
                    "terminationGracePeriodSeconds": 0,
                    "containers": [{
                        "name": "sample-webhook",
                        "image": "registry.k8s.io/e2e-test-images/agnhost:2.56",
                        "args": ["webhook", "--tls-cert-file=/certs/tls.crt"],
                        "readinessProbe": {
                            "httpGet": {"scheme": "HTTPS", "port": 8444, "path": "/readyz"},
                            "periodSeconds": 1,
                            "successThreshold": 1,
                            "failureThreshold": 30
                        },
                        "ports": [{"containerPort": 8444}],
                        "volumeMounts": [{
                            "name": "webhook-certs",
                            "readOnly": true,
                            "mountPath": "/certs"
                        }]
                    }],
                    "volumes": [{
                        "name": "webhook-certs",
                        "secret": {"secretName": "sample-webhook-secret"}
                    }]
                }
            }
        }
    });

    let protobuf = encode_protobuf(&original_deployment).unwrap();
    let decoded_deployment = decode_protobuf(&protobuf[4..]).unwrap();
    let created = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "sample-webhook-deployment",
            decoded_deployment,
        )
        .await
        .unwrap();

    let deployment = crate::api::inject_resource_version(created.data, created.resource_version);
    crate::controllers::deployment::reconcile_deployment(
        &db,
        pod_repo.as_ref(),
        pod_repo.as_ref(),
        pod_repo.as_ref(),
        &deployment,
        "test-node",
    )
    .await
    .unwrap();

    let stored = db
        .get_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "sample-webhook-deployment",
        )
        .await
        .unwrap()
        .unwrap();
    let deployment = crate::api::inject_resource_version(stored.data, stored.resource_version);
    let deployment = decode_protobuf(&encode_protobuf(&deployment).unwrap()[4..]).unwrap();
    let replica_sets = db
        .list_resources(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(replica_sets.items.len(), 1);

    let mut replica_set_template = replica_sets.items[0].data["spec"]["template"].clone();
    if let Some(labels) = replica_set_template
        .pointer_mut("/metadata/labels")
        .and_then(serde_json::Value::as_object_mut)
    {
        labels.remove("pod-template-hash");
    }
    assert_eq!(deployment["spec"]["template"], replica_set_template);
}
