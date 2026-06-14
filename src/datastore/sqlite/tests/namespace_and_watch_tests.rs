use super::*;
use serde_json::json;
#[tokio::test]
async fn test_create_resource_injects_creation_timestamp() {
    let db = Datastore::new_in_memory().await.unwrap();
    let resource = db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "test-pod",
            json!({"metadata": {"name": "test-pod"}}),
        )
        .await
        .unwrap();

    // creationTimestamp should be injected
    let creation_timestamp = resource
        .data
        .get("metadata")
        .and_then(|m| m.get("creationTimestamp"))
        .and_then(|ct| ct.as_str());
    assert!(
        creation_timestamp.is_some(),
        "creationTimestamp should be injected by db.create_resource"
    );
}

#[tokio::test]
async fn test_create_resource_injects_uid() {
    let db = Datastore::new_in_memory().await.unwrap();
    let resource = db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "test-pod",
            json!({"metadata": {"name": "test-pod"}}),
        )
        .await
        .unwrap();

    // uid should be injected
    let uid = resource
        .data
        .get("metadata")
        .and_then(|m| m.get("uid"))
        .and_then(|u| u.as_str());
    assert!(
        uid.is_some(),
        "uid should be injected by db.create_resource"
    );
    assert_eq!(uid.unwrap().len(), 36, "uid should be a valid UUID");
}

#[tokio::test]
async fn test_create_resource_preserves_existing_uid_and_timestamp() {
    let db = Datastore::new_in_memory().await.unwrap();
    let existing_uid = "12345678-1234-1234-1234-123456789012";
    let existing_timestamp = "2024-01-01T00:00:00Z";
    let resource = db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "test-pod",
            json!({
                "metadata": {
                    "name": "test-pod",
                    "uid": existing_uid,
                    "creationTimestamp": existing_timestamp
                }
            }),
        )
        .await
        .unwrap();

    // Should preserve existing values
    let uid = resource
        .data
        .get("metadata")
        .and_then(|m| m.get("uid"))
        .and_then(|u| u.as_str());
    let timestamp = resource
        .data
        .get("metadata")
        .and_then(|m| m.get("creationTimestamp"))
        .and_then(|ct| ct.as_str());

    assert_eq!(uid, Some(existing_uid), "Should preserve existing uid");
    assert_eq!(
        timestamp,
        Some(existing_timestamp),
        "Should preserve existing creationTimestamp"
    );
}

#[tokio::test]
async fn test_create_resource_replaces_empty_uid_and_timestamp() {
    let db = Datastore::new_in_memory().await.unwrap();
    let resource = db
        .create_resource(
            "networking.k8s.io/v1",
            "IngressClass",
            None,
            "test-ingressclass",
            json!({
                "apiVersion": "networking.k8s.io/v1",
                "kind": "IngressClass",
                "metadata": {
                    "name": "test-ingressclass",
                    "uid": "",
                    "creationTimestamp": ""
                },
                "spec": {
                    "controller": "example.com/ingress-controller"
                }
            }),
        )
        .await
        .unwrap();

    let uid = resource
        .data
        .get("metadata")
        .and_then(|m| m.get("uid"))
        .and_then(|u| u.as_str())
        .unwrap_or("");
    let timestamp = resource
        .data
        .get("metadata")
        .and_then(|m| m.get("creationTimestamp"))
        .and_then(|ct| ct.as_str())
        .unwrap_or("");

    assert!(!uid.is_empty(), "uid should not remain empty");
    assert_eq!(uid.len(), 36, "uid should be a valid UUID");
    assert!(
        !timestamp.is_empty(),
        "creationTimestamp should not remain empty"
    );
}

#[tokio::test]
async fn test_create_duplicate_namespace_returns_conflict() {
    let db = Datastore::new_in_memory().await.unwrap();
    let data = json!({"metadata": {"name": "test-ns"}});

    // First creation should succeed
    db.create_resource("v1", "Namespace", None, "test-ns", data.clone())
        .await
        .unwrap();

    // Second creation with same name should fail with 409
    let result = db
        .create_resource("v1", "Namespace", None, "test-ns", data)
        .await;

    assert!(
        result.is_err(),
        "Should return error for duplicate namespace"
    );
    assert!(
        result.unwrap_err().to_string().contains("409"),
        "Error should contain 409 status code"
    );
}

#[tokio::test]
async fn test_generic_create_namespace_uses_namespace_table() {
    let db = Datastore::new_in_memory().await.unwrap();

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "kube-node-lease",
        json!({"metadata": {"name": "kube-node-lease"}}),
    )
    .await
    .unwrap();

    assert!(
        db.get_namespace("kube-node-lease").await.unwrap().is_some(),
        "generic Namespace creates must be stored in the namespaces table"
    );

    let wrong_table_rows: i64 = db
        .db_call("test_generic_create_namespace_wrong_table_rows", |conn| {
            Ok(conn.query_row(
                "SELECT COUNT(*) FROM namespaced_resources WHERE api_version = 'v1' AND kind = 'Namespace'",
                [],
                |row| row.get(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(
        wrong_table_rows, 0,
        "Namespace rows must never be inserted into namespaced_resources"
    );
}

#[tokio::test]
async fn test_create_duplicate_node_returns_conflict() {
    let db = Datastore::new_in_memory().await.unwrap();
    let data = json!({"metadata": {"name": "test-node"}});

    // First creation should succeed
    db.create_resource("v1", "Node", None, "test-node", data.clone())
        .await
        .unwrap();

    // Second creation with same name should fail with 409
    let result = db
        .create_resource("v1", "Node", None, "test-node", data)
        .await;

    assert!(result.is_err(), "Should return error for duplicate node");
    assert!(
        result.unwrap_err().to_string().contains("409"),
        "Error should contain 409 status code"
    );
}

#[tokio::test]
async fn test_create_namespace_uniqueness() {
    let db = Datastore::new_in_memory().await.unwrap();
    let data = json!({"metadata": {"name": "test-ns"}});

    // First creation should succeed
    db.create_namespace("test-ns", data.clone()).await.unwrap();

    // Second creation with same name should fail (PRIMARY KEY violation)
    let result = db.create_namespace("test-ns", data).await;
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Namespace already exists")
    );
}

#[tokio::test]
async fn test_namespace_crud_lifecycle() {
    let db = Datastore::new_in_memory().await.unwrap();

    // Create
    let ns = db
        .create_namespace("test-ns", json!({"metadata": {"name": "test-ns"}}))
        .await
        .unwrap();
    assert_eq!(ns.name, "test-ns");
    assert_eq!(ns.kind, "Namespace");
    assert_eq!(ns.api_version, "v1");

    // Get
    let fetched = db.get_namespace("test-ns").await.unwrap();
    assert!(fetched.is_some());
    assert_eq!(fetched.unwrap().name, "test-ns");

    // List
    let list = db.list_namespaces(None, None).await.unwrap();
    assert_eq!(list.items.len(), 1);
    assert_eq!(list.items[0].name, "test-ns");

    // Update
    let updated = db
        .update_namespace(
            "test-ns",
            json!({"metadata": {"name": "test-ns", "labels": {"foo": "bar"}}}),
            ns.resource_version,
        )
        .await
        .unwrap();
    assert!(updated.resource_version > ns.resource_version);

    // Delete
    db.delete_namespace("test-ns").await.unwrap();

    // Get after delete should return None
    let deleted = db.get_namespace("test-ns").await.unwrap();
    assert!(deleted.is_none());
}

#[tokio::test]
async fn test_list_namespaces_page_uses_name_continue_token_after_selectors() {
    let db = Datastore::new_in_memory().await.unwrap();

    for (name, env) in [
        ("alpha", "prod"),
        ("beta", "dev"),
        ("gamma", "prod"),
        ("omega", "prod"),
    ] {
        db.create_namespace(
            name,
            json!({"metadata": {"name": name, "labels": {"env": env}}}),
        )
        .await
        .unwrap();
    }

    let page1 = db
        .list_namespaces_page(
            Some("env=prod"),
            None,
            crate::datastore::ListPageRequest::try_new(Some(2), None).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        page1
            .items
            .iter()
            .map(|item| item.name.as_str())
            .collect::<Vec<_>>(),
        vec!["alpha", "gamma"]
    );
    assert_eq!(page1.continue_token.as_deref(), Some("gamma"));
    assert_eq!(page1.remaining_item_count, Some(1));

    let page2 = db
        .list_namespaces_page(
            Some("env=prod"),
            None,
            crate::datastore::ListPageRequest::try_new(Some(2), page1.continue_token.clone())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        page2
            .items
            .iter()
            .map(|item| item.name.as_str())
            .collect::<Vec<_>>(),
        vec!["omega"]
    );
    assert_eq!(page2.continue_token, None);
    assert_eq!(page2.remaining_item_count, None);
}

// ========================
// Update version conflict tests
// ========================

#[tokio::test]
async fn test_update_resource_version_conflict_returns_error() {
    let db = Datastore::new_in_memory().await.unwrap();
    let created = db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "test-pod",
            json!({"metadata": {"name": "test-pod"}}),
        )
        .await
        .unwrap();

    // First update succeeds
    let updated = db
        .update_resource(
            "v1",
            "Pod",
            Some("default"),
            "test-pod",
            json!({"metadata": {"name": "test-pod"}, "spec": {"v": 2}}),
            created.resource_version,
        )
        .await
        .unwrap();
    assert!(updated.resource_version > created.resource_version);

    // Second update with stale resource version should fail
    let result = db
        .update_resource(
            "v1",
            "Pod",
            Some("default"),
            "test-pod",
            json!({"metadata": {"name": "test-pod"}, "spec": {"v": 3}}),
            created.resource_version, // stale
        )
        .await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("409"));
}

#[tokio::test]
async fn test_update_resource_defaults_empty_metadata_namespace_to_request_namespace() {
    let db = Datastore::new_in_memory().await.unwrap();
    let created = db
        .create_resource(
            "v1",
            "Endpoints",
            Some("test-ns"),
            "example-custom-endpoints",
            json!({
                "metadata": {
                    "name": "example-custom-endpoints",
                    "namespace": "test-ns"
                },
                "subsets": [{
                    "addresses": [{"ip": "10.1.2.3"}],
                    "ports": [{"port": 80}]
                }]
            }),
        )
        .await
        .unwrap();

    let updated = db
        .update_resource(
            "v1",
            "Endpoints",
            Some("test-ns"),
            "example-custom-endpoints",
            json!({
                "metadata": {
                    "name": "example-custom-endpoints",
                    "namespace": ""
                },
                "subsets": [{
                    "addresses": [{"ip": "10.2.3.4"}],
                    "ports": [{"port": 80}]
                }]
            }),
            created.resource_version,
        )
        .await
        .unwrap();

    assert_eq!(
        updated
            .data
            .pointer("/metadata/namespace")
            .and_then(|v| v.as_str()),
        Some("test-ns")
    );
    assert_eq!(
        updated
            .data
            .pointer("/subsets/0/addresses/0/ip")
            .and_then(|v| v.as_str()),
        Some("10.2.3.4")
    );
}

#[tokio::test]
async fn test_update_resource_preserves_uid_and_creation_timestamp_when_omitted() {
    let db = Datastore::new_in_memory().await.unwrap();
    let created = db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "meta-preserve",
            json!({
                "metadata": {"name": "meta-preserve"},
                "spec": {"containers": [{"name": "c", "image": "busybox"}]}
            }),
        )
        .await
        .unwrap();
    let created_uid = created
        .data
        .pointer("/metadata/uid")
        .and_then(|v| v.as_str())
        .unwrap()
        .to_string();
    let created_ts = created
        .data
        .pointer("/metadata/creationTimestamp")
        .and_then(|v| v.as_str())
        .unwrap()
        .to_string();

    let updated = db
        .update_resource(
            "v1",
            "Pod",
            Some("default"),
            "meta-preserve",
            json!({
                "metadata": {"name": "meta-preserve"},
                "spec": {"containers": [{"name": "c", "image": "busybox:1.36"}]}
            }),
            created.resource_version,
        )
        .await
        .unwrap();

    assert_eq!(
        updated
            .data
            .pointer("/metadata/uid")
            .and_then(|v| v.as_str()),
        Some(created_uid.as_str())
    );
    assert_eq!(
        updated
            .data
            .pointer("/metadata/creationTimestamp")
            .and_then(|v| v.as_str()),
        Some(created_ts.as_str())
    );
}

#[tokio::test]
async fn test_update_nonexistent_resource_returns_error() {
    let db = Datastore::new_in_memory().await.unwrap();
    let result = db
        .update_resource(
            "v1",
            "Pod",
            Some("default"),
            "nonexistent",
            json!({"metadata": {"name": "nonexistent"}}),
            1,
        )
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_update_deleted_resource_returns_error() {
    let db = Datastore::new_in_memory().await.unwrap();
    let created = db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "test-pod",
            json!({"metadata": {"name": "test-pod"}}),
        )
        .await
        .unwrap();

    db.delete_resource("v1", "Pod", Some("default"), "test-pod")
        .await
        .unwrap();

    // Update after delete should fail
    let result = db
        .update_resource(
            "v1",
            "Pod",
            Some("default"),
            "test-pod",
            json!({"metadata": {"name": "test-pod"}}),
            created.resource_version,
        )
        .await;
    assert!(result.is_err());
}

// ========================
// Namespace field selector tests
// ========================

#[tokio::test]
async fn test_list_namespaces_field_selector_metadata_name() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.create_namespace("alpha", json!({"metadata": {"name": "alpha"}}))
        .await
        .unwrap();
    db.create_namespace("beta", json!({"metadata": {"name": "beta"}}))
        .await
        .unwrap();
    db.create_namespace("gamma", json!({"metadata": {"name": "gamma"}}))
        .await
        .unwrap();

    let list = db
        .list_namespaces(None, Some("metadata.name=beta"))
        .await
        .unwrap();
    assert_eq!(list.items.len(), 1);
    assert_eq!(list.items[0].name, "beta");
}

#[tokio::test]
async fn test_list_namespaces_field_selector_sql_injection_safe() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.create_namespace("target", json!({"metadata": {"name": "target"}}))
        .await
        .unwrap();

    // Attempt SQL injection via field selector — should return 0 results, not error or leak data
    let result = db
        .list_namespaces(None, Some("metadata.name=' OR '1'='1"))
        .await
        .unwrap();
    assert_eq!(
        result.items.len(),
        0,
        "SQL injection attempt should match nothing"
    );
}

#[tokio::test]
async fn test_list_namespaces_label_selector_matrix_and_field_interaction() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.create_namespace(
        "alpha",
        json!({"metadata": {"name": "alpha", "labels": {"env": "prod", "team": "core"}}}),
    )
    .await
    .unwrap();
    db.create_namespace(
        "beta",
        json!({"metadata": {"name": "beta", "labels": {"env": "staging", "deprecated": "true"}}}),
    )
    .await
    .unwrap();
    db.create_namespace(
        "gamma",
        json!({"metadata": {"name": "gamma", "labels": {"team": "edge"}}}),
    )
    .await
    .unwrap();
    db.create_namespace(
        "delta",
        json!({"metadata": {"name": "delta", "labels": {"env": "dev", "team": "core"}}}),
    )
    .await
    .unwrap();
    db.create_namespace("epsilon", json!({"metadata": {"name": "epsilon"}}))
        .await
        .unwrap();

    let env_prod = db.list_namespaces(Some("env=prod"), None).await.unwrap();
    assert_eq!(
        env_prod
            .items
            .iter()
            .map(|r| r.name.as_str())
            .collect::<Vec<_>>(),
        vec!["alpha"]
    );

    let env_not_prod = db.list_namespaces(Some("env!=prod"), None).await.unwrap();
    let mut env_not_prod_names: Vec<String> =
        env_not_prod.items.into_iter().map(|r| r.name).collect();
    env_not_prod_names.sort();
    assert_eq!(
        env_not_prod_names,
        vec!["beta", "delta", "epsilon", "gamma"]
    );

    let has_team = db.list_namespaces(Some("team"), None).await.unwrap();
    let mut has_team_names: Vec<String> = has_team.items.into_iter().map(|r| r.name).collect();
    has_team_names.sort();
    assert_eq!(has_team_names, vec!["alpha", "delta", "gamma"]);

    let not_deprecated = db.list_namespaces(Some("!deprecated"), None).await.unwrap();
    let mut not_deprecated_names: Vec<String> =
        not_deprecated.items.into_iter().map(|r| r.name).collect();
    not_deprecated_names.sort();
    assert_eq!(
        not_deprecated_names,
        vec!["alpha", "delta", "epsilon", "gamma"]
    );

    let env_in = db
        .list_namespaces(Some("env in (prod,staging)"), None)
        .await
        .unwrap();
    let mut env_in_names: Vec<String> = env_in.items.into_iter().map(|r| r.name).collect();
    env_in_names.sort();
    assert_eq!(env_in_names, vec!["alpha", "beta"]);

    let env_notin = db
        .list_namespaces(Some("env notin (dev,test)"), None)
        .await
        .unwrap();
    let mut env_notin_names: Vec<String> = env_notin.items.into_iter().map(|r| r.name).collect();
    env_notin_names.sort();
    assert_eq!(env_notin_names, vec!["alpha", "beta", "epsilon", "gamma"]);

    let combined = db
        .list_namespaces(Some("team,env in (prod,dev),!deprecated"), None)
        .await
        .unwrap();
    let mut combined_names: Vec<String> = combined.items.into_iter().map(|r| r.name).collect();
    combined_names.sort();
    assert_eq!(combined_names, vec!["alpha", "delta"]);

    let with_field_selector = db
        .list_namespaces(Some("team=core"), Some("metadata.name=alpha"))
        .await
        .unwrap();
    assert_eq!(
        with_field_selector
            .items
            .iter()
            .map(|r| r.name.as_str())
            .collect::<Vec<_>>(),
        vec!["alpha"]
    );

    let field_plus_mismatch_label = db
        .list_namespaces(Some("team=core"), Some("metadata.name=beta"))
        .await
        .unwrap();
    assert!(field_plus_mismatch_label.items.is_empty());
}

#[tokio::test]
async fn test_list_namespaces_invalid_label_selector_returns_error() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.create_namespace("alpha", json!({"metadata": {"name": "alpha"}}))
        .await
        .unwrap();

    let result = db.list_namespaces(Some("env in (prod"), None).await;
    assert!(result.is_err(), "invalid selector must return an error");
    let message = result.unwrap_err().to_string();
    assert!(
        message.contains("Invalid label selector"),
        "unexpected error: {message}"
    );
}

#[tokio::test]
async fn test_list_namespaces_excludes_deleted() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.create_namespace("keep", json!({"metadata": {"name": "keep"}}))
        .await
        .unwrap();
    db.create_namespace("remove", json!({"metadata": {"name": "remove"}}))
        .await
        .unwrap();

    db.delete_namespace("remove").await.unwrap();

    let list = db.list_namespaces(None, None).await.unwrap();
    assert_eq!(list.items.len(), 1);
    assert_eq!(list.items[0].name, "keep");
}

// ========================
// Namespace update version conflict
// ========================

#[tokio::test]
async fn test_update_namespace_version_conflict_returns_error() {
    let db = Datastore::new_in_memory().await.unwrap();
    let ns = db
        .create_namespace("test", json!({"metadata": {"name": "test"}}))
        .await
        .unwrap();

    // First update
    let updated = db
        .update_namespace(
            "test",
            json!({"metadata": {"name": "test", "labels": {"a": "1"}}}),
            ns.resource_version,
        )
        .await
        .unwrap();

    // Second update with stale rv
    let result = db
        .update_namespace(
            "test",
            json!({"metadata": {"name": "test", "labels": {"a": "2"}}}),
            ns.resource_version, // stale
        )
        .await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("conflict"));

    // Verify first update value is preserved
    let current = db.get_namespace("test").await.unwrap().unwrap();
    assert_eq!(current.resource_version, updated.resource_version);
}

#[tokio::test]
async fn test_delete_namespace_nonexistent_returns_error() {
    let db = Datastore::new_in_memory().await.unwrap();
    let result = db.delete_namespace("nonexistent").await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("not found"));
}

#[tokio::test]
async fn test_create_namespace_after_delete_succeeds() {
    // Regression test: ghost namespace bug
    // After deleting a namespace, creating one with the same name must succeed.
    // Previously failed because deleted row blocked INSERT on PRIMARY KEY.
    let db = Datastore::new_in_memory().await.unwrap();

    // Create namespace
    let ns_data =
        json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "sonobuoy"}});
    db.create_namespace("sonobuoy", ns_data.clone())
        .await
        .unwrap();

    // Delete namespace (hard-delete)
    db.delete_namespace("sonobuoy").await.unwrap();

    // Verify it's gone from GET
    let result = db.get_namespace("sonobuoy").await.unwrap();
    assert!(
        result.is_none(),
        "Deleted namespace should not be visible via get"
    );

    // Verify it's gone from LIST
    let list = db.list_namespaces(None, None).await.unwrap();
    assert!(
        !list.items.iter().any(|r| r.name == "sonobuoy"),
        "Deleted namespace should not appear in list"
    );

    // Create namespace with same name again — this must succeed
    let ns_data2 =
        json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "sonobuoy"}});
    let result = db.create_namespace("sonobuoy", ns_data2).await;
    assert!(
        result.is_ok(),
        "Creating namespace after deletion should succeed, got: {:?}",
        result.unwrap_err()
    );

    // Verify it's visible again
    let ns = db.get_namespace("sonobuoy").await.unwrap();
    assert!(ns.is_some(), "Re-created namespace should be visible");
}

/// Namespace hard-delete must not free Pod name slots. Pod rows are removed
/// only by actor-owned UID finalization after runtime and local cache cleanup.
#[tokio::test]
async fn test_delete_namespace_refuses_remaining_pods_and_other_resources() {
    let db = Datastore::new_in_memory().await.unwrap();

    db.create_namespace(
        "order-ns",
        json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "order-ns"}}),
    )
    .await
    .unwrap();
    let pod = db
        .create_resource(
            "v1",
            "Pod",
            Some("order-ns"),
            "p0-order-pod",
            json!({"apiVersion":"v1","kind":"Pod","metadata":{"name":"p0-order-pod","namespace":"order-ns"}}),
        )
        .await
        .unwrap();
    db.create_resource(
            "v1",
            "ConfigMap",
            Some("order-ns"),
            "p0-order-cm",
            json!({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"p0-order-cm","namespace":"order-ns"}}),
        )
        .await
        .unwrap();

    let result = db.delete_namespace("order-ns").await;
    assert!(
        result.is_err(),
        "namespace hard-delete must fail while namespaced content remains"
    );

    assert!(
        db.get_namespace("order-ns").await.unwrap().is_some(),
        "namespace must remain while content remains"
    );
    assert!(
        db.get_resource("v1", "Pod", Some("order-ns"), "p0-order-pod",)
            .await
            .unwrap()
            .is_some(),
        "pod must remain until actor-owned UID finalization"
    );
    assert!(
        db.get_resource("v1", "ConfigMap", Some("order-ns"), "p0-order-cm",)
            .await
            .unwrap()
            .is_some(),
        "non-pod content remains when namespace hard-delete is refused"
    );

    db.delete_namespace_contents("order-ns").await.unwrap();
    assert!(
        db.get_resource("v1", "ConfigMap", Some("order-ns"), "p0-order-cm",)
            .await
            .unwrap()
            .is_none(),
        "namespace content cleanup may remove non-Pod resources"
    );
    assert!(
        db.get_resource("v1", "Pod", Some("order-ns"), "p0-order-pod",)
            .await
            .unwrap()
            .is_some(),
        "namespace content cleanup must not remove Pod rows"
    );
    db.delete_resource_with_preconditions(
        "v1",
        "Pod",
        Some("order-ns"),
        "p0-order-pod",
        crate::datastore::ResourcePreconditions::uid(&pod.uid),
    )
    .await
    .unwrap();
    db.delete_namespace("order-ns").await.unwrap();
    assert!(db.get_namespace("order-ns").await.unwrap().is_none());
}

#[tokio::test]
async fn test_namespaced_watch_catchup_replays_intermediate_events_before_delete() {
    let db = Datastore::new_in_memory().await.unwrap();

    let created = db
        .create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "cm-watch",
            json!({"metadata":{"name":"cm-watch","namespace":"default"},"data":{"k":"v1"}}),
        )
        .await
        .unwrap();
    let updated1 = db
        .update_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "cm-watch",
            json!({"metadata":{"name":"cm-watch","namespace":"default"},"data":{"k":"v2"}}),
            created.resource_version,
        )
        .await
        .unwrap();
    let updated2 = db
        .update_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "cm-watch",
            json!({"metadata":{"name":"cm-watch","namespace":"default"},"data":{"k":"v3"}}),
            updated1.resource_version,
        )
        .await
        .unwrap();
    db.delete_resource("v1", "ConfigMap", Some("default"), "cm-watch")
        .await
        .unwrap();

    let replay = db
        .list_resources_modified_since(
            "v1",
            "ConfigMap",
            Some("default"),
            updated1.resource_version,
        )
        .await
        .unwrap();

    assert_eq!(replay.len(), 2);
    assert_eq!(replay[0].event_type, "MODIFIED");
    assert_eq!(
        replay[0].resource.resource_version,
        updated2.resource_version
    );
    assert_eq!(replay[1].event_type, "DELETED");
}

#[tokio::test]
async fn test_cluster_watch_catchup_replays_intermediate_events_before_delete() {
    let db = Datastore::new_in_memory().await.unwrap();

    let created = db
        .create_resource(
            "v1",
            "Node",
            None,
            "node-watch",
            json!({"metadata":{"name":"node-watch"},"spec":{"podCIDR":"10.42.0.0/24"}}),
        )
        .await
        .unwrap();
    let updated1 = db
        .update_resource(
            "v1",
            "Node",
            None,
            "node-watch",
            json!({"metadata":{"name":"node-watch"},"spec":{"podCIDR":"10.43.0.0/24"}}),
            created.resource_version,
        )
        .await
        .unwrap();
    let updated2 = db
        .update_resource(
            "v1",
            "Node",
            None,
            "node-watch",
            json!({"metadata":{"name":"node-watch"},"spec":{"podCIDR":"10.44.0.0/24"}}),
            updated1.resource_version,
        )
        .await
        .unwrap();
    db.delete_resource("v1", "Node", None, "node-watch")
        .await
        .unwrap();

    let replay = db
        .list_cluster_resources_modified_since("v1", "Node", updated1.resource_version)
        .await
        .unwrap();

    assert_eq!(replay.len(), 2);
    assert_eq!(replay[0].event_type, "MODIFIED");
    assert_eq!(
        replay[0].resource.resource_version,
        updated2.resource_version
    );
    assert_eq!(replay[1].event_type, "DELETED");
}
