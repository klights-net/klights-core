#![cfg(test)]

use super::*;
use klights_cluster_core::{
    LogApplyCommit, LogApplyMutation, LogApplyNamespaceRow,
};

/// Regression: concurrent explicit-name Namespace create must preserve
/// create-only and AlreadyExists semantics across committed Raft apply.
///
/// The run-17 defect: two parallel Ginkgo processes both received
/// `webhook-9071` because klights let both concurrent explicit-name creates
/// succeed instead of rejecting the second with AlreadyExists (409 Conflict).
///
/// This test drives the state-machine apply path directly: two PutNamespace
/// commits with the same name. Before the fix, both succeed (UPSERT — the
/// second silently overwrites the first). After the fix, the second must be
/// rejected with AlreadyExists.
#[tokio::test]
async fn concurrent_explicit_name_namespace_create_rejects_duplicate() {
    let db = Datastore::new_in_memory().await.unwrap();

    let row_a = LogApplyNamespaceRow {
        name: "ns-concurrent-1".to_string(),
        uid: "uid-a".to_string(),
        resource_version: 0,
        data: serde_json::json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {"name": "ns-concurrent-1"}
        }),
    };

    let commit_a =
        LogApplyCommit::try_new(vec![LogApplyMutation::PutNamespace(row_a)])
            .unwrap();
    let result_a = db.apply_raft_log_apply_commit(commit_a).await.unwrap();
    assert!(
        result_a.error_message.is_none(),
        "first explicit-name namespace create must succeed: {:?}",
        result_a.error_message
    );

    // A second create for the same name must be rejected as AlreadyExists,
    // not silently overwrite the first via UPSERT.
    let row_b = LogApplyNamespaceRow {
        name: "ns-concurrent-1".to_string(),
        uid: "uid-b".to_string(),
        resource_version: 0,
        data: serde_json::json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {"name": "ns-concurrent-1"}
        }),
    };

    let commit_b =
        LogApplyCommit::try_new(vec![LogApplyMutation::PutNamespace(row_b)])
            .unwrap();
    let result_b = db.apply_raft_log_apply_commit(commit_b).await.unwrap();
    assert!(
        result_b.error_message.is_some(),
        "second explicit-name namespace create must be rejected as AlreadyExists"
    );
    let msg = result_b.error_message.unwrap();
    assert!(
        msg.contains("already exists") || msg.contains("AlreadyExists"),
        "rejection message must indicate already-exists, got: {msg}"
    );

    let ns = db.get_namespace("ns-concurrent-1").await.unwrap();
    let ns = ns.expect("namespace must still exist after rejected second create");
    assert_eq!(ns.uid, "uid-a", "first create must be retained");
}
