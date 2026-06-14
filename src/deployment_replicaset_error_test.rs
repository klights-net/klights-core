// Test for Deployment controller handling ReplicaSet creation failures
//
// This test verifies that when a Deployment controller fails to create a ReplicaSet,
// it sets an appropriate failure condition on the Deployment status.

#[cfg(test)]
mod tests {
    use crate::controllers::deployment::reconcile_deployment;

    use serde_json::json;

    #[tokio::test]
    async fn test_deployment_replicaset_creation_failure_sets_condition() {
        let db = crate::datastore::test_support::in_memory().await;
        let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);

        // Create a Deployment with missing spec fields (will fail reconcile)
        let deployment = json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": "webhook-test",
                "namespace": "default",
                "uid": "test-uid-123",
                "resourceVersion": "1"
            },
            "spec": {
                "replicas": 1,
                // Missing selector and template - will cause reconcile to fail
            }
        });

        // Store deployment first
        let created = db
            .create_resource(
                "apps/v1",
                "Deployment",
                Some("default"),
                "webhook-test",
                deployment.clone(),
            )
            .await
            .unwrap();

        let deployment_with_rv =
            crate::api::inject_resource_version(created.data, created.resource_version);

        // Reconcile should succeed (not return error) and set failure condition
        let result = reconcile_deployment(
            &db,
            __pod_repo.as_ref(),
            __pod_repo.as_ref(),
            __pod_repo.as_ref(),
            &deployment_with_rv,
            "test-node",
        )
        .await;

        assert!(
            result.is_ok(),
            "Reconcile should succeed and set condition instead of returning error"
        );

        // Verify Deployment status has ReplicaFailure condition
        let updated = db
            .get_resource("apps/v1", "Deployment", Some("default"), "webhook-test")
            .await
            .unwrap()
            .unwrap();
        let conditions = updated.data["status"]["conditions"].as_array().unwrap();
        let replica_failure = conditions.iter().find(|c| c["type"] == "ReplicaFailure");
        assert!(
            replica_failure.is_some(),
            "Should have ReplicaFailure condition"
        );
        let condition = replica_failure.unwrap();
        assert_eq!(condition["status"], "True");

        // Message should indicate the failure (either "missing" or "Failed")
        let message = condition["message"].as_str().unwrap();
        assert!(
            message.contains("missing") || message.contains("Failed"),
            "Message should contain 'missing' or 'Failed', got: {}",
            message
        );
    }
}
