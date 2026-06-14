use super::*;
use serde_json::json;

#[tokio::test]
async fn test_create_crd_registers_in_registry() {
    let db = crate::datastore::test_support::in_memory().await;
    let registry = CrdRegistry::new();

    // Create a CRD
    let crd = make_crd_value(
        "cert-manager.io",
        "Certificate",
        "certificates",
        "Namespaced",
    );

    db.create_resource(
        "apiextensions.k8s.io/v1",
        "CustomResourceDefinition",
        None,
        "certificates.cert-manager.io",
        crd.clone(),
    )
    .await
    .unwrap();

    // Register it
    register_crd_from_value(&registry, &crd).await.unwrap();

    // Verify the CRD group appears in the registry
    let info = registry.get("cert-manager.io", "v1", "certificates").await;
    assert!(info.is_some(), "CRD should be registered in the registry");
    let info = info.unwrap();
    assert_eq!(info.kind, "Certificate");
    assert_eq!(info.plural, "certificates");
    assert_eq!(info.singular, "certificate");
    assert!(info.namespaced);
}

#[tokio::test]
async fn test_crd_custom_resource_crud() {
    let db = crate::datastore::test_support::in_memory().await;
    let registry = CrdRegistry::new();

    // Create namespace
    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({"metadata": {"name": "default"}}),
    )
    .await
    .unwrap();

    // Register a simple CRD
    let crd = make_crd_value(
        "cert-manager.io",
        "Certificate",
        "certificates",
        "Namespaced",
    );
    register_crd_from_value(&registry, &crd).await.unwrap();

    // Create a custom resource
    let cert = json!({
        "apiVersion": "cert-manager.io/v1",
        "kind": "Certificate",
        "metadata": {
            "name": "test-cert",
            "namespace": "default"
        },
        "spec": {
            "secretName": "test-secret",
            "dnsNames": ["example.com"]
        }
    });

    let created = db
        .create_resource(
            "cert-manager.io/v1",
            "Certificate",
            Some("default"),
            "test-cert",
            cert.clone(),
        )
        .await
        .unwrap();

    // Get it back
    let retrieved = db
        .get_resource(
            "cert-manager.io/v1",
            "Certificate",
            Some("default"),
            "test-cert",
        )
        .await
        .unwrap();
    assert!(retrieved.is_some());
    let retrieved = retrieved.unwrap();
    assert_eq!(retrieved.data["spec"]["secretName"], "test-secret");

    // Update it
    let mut updated_cert = cert.clone();
    if let Some(spec) = updated_cert.get_mut("spec").and_then(|s| s.as_object_mut()) {
        spec.insert("issuerRef".to_string(), json!({"name": "letsencrypt"}));
    }

    let updated = db
        .update_resource(
            "cert-manager.io/v1",
            "Certificate",
            Some("default"),
            "test-cert",
            updated_cert,
            created.resource_version,
        )
        .await
        .unwrap();
    assert_eq!(updated.data["spec"]["issuerRef"]["name"], "letsencrypt");

    // Delete it
    db.delete_resource(
        "cert-manager.io/v1",
        "Certificate",
        Some("default"),
        "test-cert",
    )
    .await
    .unwrap();

    // Verify it's gone
    let deleted = db
        .get_resource(
            "cert-manager.io/v1",
            "Certificate",
            Some("default"),
            "test-cert",
        )
        .await
        .unwrap();
    assert!(deleted.is_none(), "Resource should be deleted");
}

#[tokio::test]
async fn test_crd_cluster_scoped_crud() {
    let db = crate::datastore::test_support::in_memory().await;
    let registry = CrdRegistry::new();

    // Register a cluster-scoped CRD
    let crd = make_crd_value(
        "cert-manager.io",
        "ClusterIssuer",
        "clusterissuers",
        "Cluster",
    );
    register_crd_from_value(&registry, &crd).await.unwrap();

    // Create resource with namespace=None
    let cluster_issuer = json!({
        "apiVersion": "cert-manager.io/v1",
        "kind": "ClusterIssuer",
        "metadata": {
            "name": "letsencrypt"
        },
        "spec": {
            "acme": {
                "server": "https://acme-v02.api.letsencrypt.org/directory"
            }
        }
    });

    db.create_resource(
        "cert-manager.io/v1",
        "ClusterIssuer",
        None,
        "letsencrypt",
        cluster_issuer.clone(),
    )
    .await
    .unwrap();

    // Verify get works without namespace
    let retrieved = db
        .get_resource("cert-manager.io/v1", "ClusterIssuer", None, "letsencrypt")
        .await
        .unwrap();
    assert!(retrieved.is_some());
    assert_eq!(
        retrieved.unwrap().data["spec"]["acme"]["server"],
        "https://acme-v02.api.letsencrypt.org/directory"
    );

    // Verify list works without namespace
    let list = db
        .list_resources(
            "cert-manager.io/v1",
            "ClusterIssuer",
            None,
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(list.items.len(), 1);

    // Verify delete works without namespace
    db.delete_resource("cert-manager.io/v1", "ClusterIssuer", None, "letsencrypt")
        .await
        .unwrap();

    let deleted = db
        .get_resource("cert-manager.io/v1", "ClusterIssuer", None, "letsencrypt")
        .await
        .unwrap();
    assert!(deleted.is_none());
}

#[tokio::test]
async fn test_crd_namespaced_crud() {
    let db = crate::datastore::test_support::in_memory().await;
    let registry = CrdRegistry::new();

    // Create namespaces
    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({"metadata": {"name": "default"}}),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Namespace",
        None,
        "other",
        json!({"metadata": {"name": "other"}}),
    )
    .await
    .unwrap();

    // Register a namespaced CRD
    let crd = make_crd_value("argoproj.io", "Application", "applications", "Namespaced");
    register_crd_from_value(&registry, &crd).await.unwrap();

    // Create resource with namespace=Some("default")
    let app = json!({
        "apiVersion": "argoproj.io/v1",
        "kind": "Application",
        "metadata": {
            "name": "my-app",
            "namespace": "default"
        },
        "spec": {
            "project": "default"
        }
    });

    db.create_resource(
        "argoproj.io/v1",
        "Application",
        Some("default"),
        "my-app",
        app,
    )
    .await
    .unwrap();

    // Verify namespace isolation (list in different namespace returns empty)
    let list_default = db
        .list_resources(
            "argoproj.io/v1",
            "Application",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(list_default.items.len(), 1);

    let list_other = db
        .list_resources(
            "argoproj.io/v1",
            "Application",
            Some("other"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(list_other.items.len(), 0, "Other namespace should be empty");
}

#[tokio::test]
async fn test_crd_list_resources() {
    let db = crate::datastore::test_support::in_memory().await;
    let registry = CrdRegistry::new();

    // Create namespace
    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({"metadata": {"name": "default"}}),
    )
    .await
    .unwrap();

    // Register CRD
    let crd = make_crd_value("traefik.io", "IngressRoute", "ingressroutes", "Namespaced");
    register_crd_from_value(&registry, &crd).await.unwrap();

    // Create multiple custom resources
    for i in 1..=3 {
        let route = json!({
            "apiVersion": "traefik.io/v1",
            "kind": "IngressRoute",
            "metadata": {
                "name": format!("route-{}", i),
                "namespace": "default"
            },
            "spec": {
                "entryPoints": ["web"]
            }
        });

        db.create_resource(
            "traefik.io/v1",
            "IngressRoute",
            Some("default"),
            &format!("route-{}", i),
            route,
        )
        .await
        .unwrap();
    }

    // List them
    let list = db
        .list_resources(
            "traefik.io/v1",
            "IngressRoute",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();

    // Verify count and contents
    assert_eq!(list.items.len(), 3);
    let names: Vec<String> = list.items.iter().map(|r| r.name.clone()).collect();
    assert!(names.contains(&"route-1".to_string()));
    assert!(names.contains(&"route-2".to_string()));
    assert!(names.contains(&"route-3".to_string()));
}
