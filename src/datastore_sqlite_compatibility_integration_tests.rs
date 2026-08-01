//! Root-owned SQLite compatibility composition tests.

use klights_cluster_core::{PatchKind, ResourcePreconditions, StorageCommand};
use serde_json::json;

#[tokio::test]
async fn raft_patch_apply_built_before_spec_update_does_not_revert_live_spec() {
    let db = crate::datastore::test_support::in_memory().await;
    let created = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "web",
            json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {
                    "name": "web",
                    "namespace": "default",
                    "uid": "web-deploy-uid",
                    "generation": 2,
                    "annotations": {
                        "deployment.kubernetes.io/revision": "2"
                    }
                },
                "spec": {
                    "replicas": 10,
                    "selector": {"matchLabels": {"name": "httpd"}},
                    "template": {
                        "metadata": {"labels": {"name": "httpd"}},
                        "spec": {
                            "containers": [{"name": "httpd", "image": "webserver:404"}]
                        }
                    }
                },
                "status": {
                    "observedGeneration": 2,
                    "replicas": 13,
                    "updatedReplicas": 5,
                    "readyReplicas": 8,
                    "availableReplicas": 8
                }
            }),
        )
        .await
        .unwrap();

    let command = StorageCommand::PatchResource {
        api_version: "apps/v1".to_string(),
        kind: "Deployment".to_string(),
        namespace: Some("default".to_string()),
        name: "web".to_string(),
        patch_kind: PatchKind::Merge,
        patch: json!({
            "metadata": {
                "annotations": {
                    "deployment.kubernetes.io/revision": "2"
                }
            }
        }),
        preconditions: ResourcePreconditions::uid(created.uid.clone()),
        strict_resource_version: false,
    };
    let payload = crate::outbox_test_support::OutboxPayload::from_command(command)
        .encode_protobuf()
        .unwrap();
    let outcome = db
        .build_log_apply_commit_for_outbox(
            "raft-stale-deployment-revision-patch",
            "PatchResource",
            klights_leader_rpc::storage_wire_codec::test_outbox_command(payload.as_ref()),
            "leader",
        )
        .await
        .expect("build stale patch commit");
    let klights_cluster_core::BuildOutboxOutcome::NeedsPropose { commit, .. } = outcome else {
        panic!("expected a fresh commit");
    };

    let mut scaled = (*created.data).clone();
    scaled["metadata"]["generation"] = json!(3);
    scaled["spec"]["replicas"] = json!(30);
    db.update_resource(
        "apps/v1",
        "Deployment",
        Some("default"),
        "web",
        scaled,
        created.resource_version,
    )
    .await
    .expect("client scale update applies before stale patch commit");

    let apply_result = crate::datastore::DatastoreBackend::apply_raft_log_apply_commit(&db, commit)
        .await
        .expect("stale committed apply returns a deterministic outcome");
    assert!(
        apply_result.error_message.is_some(),
        "stale committed apply must fail strict RV validation: {apply_result:?}"
    );
    assert_eq!(apply_result.applied_rv, None);

    let live = db
        .get_resource("apps/v1", "Deployment", Some("default"), "web")
        .await
        .unwrap()
        .expect("deployment remains after authoritative apply");
    assert_eq!(
        live.data
            .pointer("/spec/replicas")
            .and_then(serde_json::Value::as_i64),
        Some(30),
        "same-UID stale raft PUT must not roll back newer client-owned state"
    );
    assert_eq!(
        live.data
            .pointer("/metadata/annotations/deployment.kubernetes.io~1revision")
            .and_then(serde_json::Value::as_str),
        Some("2"),
    );
}
