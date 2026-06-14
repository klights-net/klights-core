use serde_json::json;

#[tokio::test]
async fn test_resourcequota_create_and_get() {
    // Setup
    let db = crate::datastore::test_support::in_memory().await;

    // Create a ResourceQuota
    let quota = json!({
        "apiVersion": "v1",
        "kind": "ResourceQuota",
        "metadata": {
            "name": "test-quota",
            "namespace": "default"
        },
        "spec": {
            "hard": {
                "requests.cpu": "4",
                "requests.memory": "8Gi",
                "pods": "10"
            }
        }
    });

    let created = db
        .create_resource(
            "v1",
            "ResourceQuota",
            Some("default"),
            "test-quota",
            quota.clone(),
        )
        .await
        .unwrap();

    assert_eq!(created.name, "test-quota");
    assert_eq!(created.namespace, Some("default".to_string()));
    assert_eq!(created.kind, "ResourceQuota");
    assert_eq!(created.api_version, "v1");

    // Verify spec.hard was preserved
    let spec = created.data.get("spec").unwrap();
    let hard = spec.get("hard").unwrap();
    assert_eq!(hard.get("requests.cpu").unwrap(), "4");
    assert_eq!(hard.get("requests.memory").unwrap(), "8Gi");
    assert_eq!(hard.get("pods").unwrap(), "10");

    // Get the created ResourceQuota
    let retrieved = db
        .get_resource("v1", "ResourceQuota", Some("default"), "test-quota")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(retrieved.name, "test-quota");
    assert_eq!(retrieved.kind, "ResourceQuota");
}

#[tokio::test]
async fn test_resourcequota_list() {
    // Setup
    let db = crate::datastore::test_support::in_memory().await;

    // Create multiple ResourceQuotas
    for i in 1..=3 {
        let quota = json!({
            "apiVersion": "v1",
            "kind": "ResourceQuota",
            "metadata": {
                "name": format!("quota-{}", i),
                "namespace": "default"
            },
            "spec": {
                "hard": {
                    "pods": format!("{}", i * 10)
                }
            }
        });

        db.create_resource(
            "v1",
            "ResourceQuota",
            Some("default"),
            &format!("quota-{}", i),
            quota,
        )
        .await
        .unwrap();
    }

    // List all ResourceQuotas in namespace
    let list = db
        .list_resources(
            "v1",
            "ResourceQuota",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();

    assert_eq!(list.items.len(), 3);
    assert!(list.items.iter().any(|r| r.name == "quota-1"));
    assert!(list.items.iter().any(|r| r.name == "quota-2"));
    assert!(list.items.iter().any(|r| r.name == "quota-3"));
}

#[tokio::test]
async fn test_resourcequota_delete() {
    // Setup
    let db = crate::datastore::test_support::in_memory().await;

    // Create a ResourceQuota
    let quota = json!({
        "apiVersion": "v1",
        "kind": "ResourceQuota",
        "metadata": {
            "name": "to-delete",
            "namespace": "default"
        },
        "spec": {
            "hard": {
                "pods": "5"
            }
        }
    });

    db.create_resource("v1", "ResourceQuota", Some("default"), "to-delete", quota)
        .await
        .unwrap();

    // Delete the ResourceQuota
    db.delete_resource("v1", "ResourceQuota", Some("default"), "to-delete")
        .await
        .unwrap();

    // Verify it's gone (hard-delete returns None)
    let result = db
        .get_resource("v1", "ResourceQuota", Some("default"), "to-delete")
        .await
        .unwrap();

    assert!(result.is_none(), "Deleted resource should return None");
}

#[tokio::test]
async fn test_resourcequota_update() {
    // Setup
    let db = crate::datastore::test_support::in_memory().await;

    // Create a ResourceQuota
    let quota = json!({
        "apiVersion": "v1",
        "kind": "ResourceQuota",
        "metadata": {
            "name": "update-test",
            "namespace": "default"
        },
        "spec": {
            "hard": {
                "pods": "10"
            }
        }
    });

    let created = db
        .create_resource("v1", "ResourceQuota", Some("default"), "update-test", quota)
        .await
        .unwrap();

    // Update the ResourceQuota spec
    let mut updated_body: serde_json::Value = (*created.data).clone();
    updated_body["spec"]["hard"]["pods"] = json!("20");
    updated_body["spec"]["hard"]["requests.cpu"] = json!("8");

    let updated = db
        .update_resource(
            "v1",
            "ResourceQuota",
            Some("default"),
            "update-test",
            updated_body,
            created.resource_version, // expected_rv
        )
        .await
        .unwrap();

    // Verify updates
    let hard = updated.data["spec"]["hard"].as_object().unwrap();
    assert_eq!(hard.get("pods").unwrap(), "20");
    assert_eq!(hard.get("requests.cpu").unwrap(), "8");

    // Resource version should increment
    assert!(updated.resource_version > created.resource_version);
}
