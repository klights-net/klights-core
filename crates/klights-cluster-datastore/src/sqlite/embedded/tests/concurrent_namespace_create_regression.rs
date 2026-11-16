#![cfg(test)]

use super::*;
use klights_cluster_core::{StorageCommand, StorageCommandRejectionCode};

/// Both commands can observe absence during materialization. The committed
/// transaction must reject the second create without changing the winner or RV,
/// including when both requests supplied the same UID.
#[tokio::test]
async fn concurrent_explicit_name_namespace_create_rejects_duplicate() {
    for generic in [false, true] {
        for second_uid in ["uid-a", "uid-b"] {
            let db = Datastore::new_in_memory().await.unwrap();
            let command = |uid: &str| {
                let name = "ns-concurrent-1".to_string();
                let data = serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Namespace",
                    "metadata": {"name": name, "uid": uid}
                });
                if generic {
                    StorageCommand::CreateResource {
                        api_version: "v1".to_string(),
                        kind: "Namespace".to_string(),
                        namespace: None,
                        name,
                        data,
                    }
                } else {
                    StorageCommand::CreateNamespace { name, data }
                }
            };
            let commit_a = db
                .build_log_apply_commit_for_command(command("uid-a"), "namespace-create", "leader")
                .await
                .unwrap();
            let commit_b = db
                .build_log_apply_commit_for_command(
                    command(second_uid),
                    "namespace-create",
                    "leader",
                )
                .await
                .unwrap();

            let result_a = db.apply_raft_log_apply_commit(commit_a).await.unwrap();
            assert!(
                result_a.error_message.is_none(),
                "first namespace create must succeed: {:?}",
                result_a.error_message
            );
            let winner = db.get_namespace("ns-concurrent-1").await.unwrap().unwrap();
            let public_rv = db.get_current_resource_version().await.unwrap();

            let result_b = db.apply_raft_log_apply_commit(commit_b).await.unwrap();
            assert_eq!(
                result_b.rejection_code,
                Some(StorageCommandRejectionCode::AlreadyExists),
                "generic={generic}, second_uid={second_uid}: {:?}",
                result_b.error_message
            );
            assert!(!result_b.public_resource_changed);
            let retained = db.get_namespace("ns-concurrent-1").await.unwrap().unwrap();
            assert_eq!(retained.uid, winner.uid);
            assert_eq!(retained.resource_version, winner.resource_version);
            assert_eq!(retained.data, winner.data);
            assert_eq!(db.get_current_resource_version().await.unwrap(), public_rv);
        }
    }
}
