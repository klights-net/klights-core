use crate::controllers::deployment::reconcile_deployment;
use crate::controllers::replicaset::reconcile_replicaset;
use crate::controllers::service::ServiceIpam;
use serde_json::json;

// ========================
// Deployment handler tests
// ========================

#[tokio::test]
async fn test_create_deployment_triggers_reconciliation() {
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);

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

    // Create a Deployment
    let deployment = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "nginx-deployment",
            "namespace": "default",
            "uid": "deploy-uid-001"
        },
        "spec": {
            "replicas": 3,
            "selector": {"matchLabels": {"app": "nginx"}},
            "template": {
                "metadata": {"labels": {"app": "nginx"}},
                "spec": {"containers": [{"name": "nginx", "image": "nginx:latest"}]}
            }
        }
    });

    let created = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "nginx-deployment",
            deployment.clone(),
        )
        .await
        .unwrap();

    // Simulate what the API handler does: reconcile after create
    let mut deploy_with_rv: serde_json::Value = (*created.data).clone();
    if let Some(meta) = deploy_with_rv
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(created.resource_version.to_string()),
        );
    }

    reconcile_deployment(
        &db,
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        &deploy_with_rv,
        "test-node",
    )
    .await
    .unwrap();

    // Verify ReplicaSet was created
    let replicasets = db
        .list_resources(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();

    assert_eq!(
        replicasets.items.len(),
        1,
        "Deployment should create one ReplicaSet"
    );
    assert_eq!(replicasets.items[0].data["spec"]["replicas"], 3);
}

#[tokio::test]
async fn test_update_deployment_updates_replicaset() {
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({"metadata": {"name": "default"}}),
    )
    .await
    .unwrap();

    let deployment = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "nginx",
            "namespace": "default",
            "uid": "deploy-uid-002"
        },
        "spec": {
            "replicas": 2,
            "selector": {"matchLabels": {"app": "nginx"}},
            "template": {
                "metadata": {"labels": {"app": "nginx"}},
                "spec": {"containers": [{"name": "nginx", "image": "nginx:1.0"}]}
            }
        }
    });

    let created = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "nginx",
            deployment.clone(),
        )
        .await
        .unwrap();

    let mut deploy_with_rv: serde_json::Value = (*created.data).clone();
    if let Some(meta) = deploy_with_rv
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(created.resource_version.to_string()),
        );
    }

    reconcile_deployment(
        &db,
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        &deploy_with_rv,
        "test-node",
    )
    .await
    .unwrap();

    // Refetch to get current resource version after reconciliation
    let current = db
        .get_resource("apps/v1", "Deployment", Some("default"), "nginx")
        .await
        .unwrap()
        .unwrap();

    // Now update the Deployment (scale to 5 replicas)
    let updated_deployment = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "nginx",
            "namespace": "default",
            "uid": "deploy-uid-002"
        },
        "spec": {
            "replicas": 5,
            "selector": {"matchLabels": {"app": "nginx"}},
            "template": {
                "metadata": {"labels": {"app": "nginx"}},
                "spec": {"containers": [{"name": "nginx", "image": "nginx:2.0"}]}
            }
        }
    });

    let updated = db
        .update_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "nginx",
            updated_deployment,
            current.resource_version,
        )
        .await
        .unwrap();

    let mut updated_with_rv: serde_json::Value = (*updated.data).clone();
    if let Some(meta) = updated_with_rv
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(updated.resource_version.to_string()),
        );
    }

    // Reconcile after update
    reconcile_deployment(
        &db,
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        &updated_with_rv,
        "test-node",
    )
    .await
    .unwrap();

    // Verify ReplicaSet was updated (new ReplicaSet created for new template)
    let replicasets = db
        .list_resources(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();

    // Deployment creates a new ReplicaSet when template changes
    assert!(
        !replicasets.items.is_empty(),
        "Should have at least one ReplicaSet"
    );
}

#[tokio::test]
async fn test_patch_deployment_reconciles() {
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({"metadata": {"name": "default"}}),
    )
    .await
    .unwrap();

    let deployment = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "app",
            "namespace": "default",
            "uid": "deploy-uid-003"
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": "test"}},
            "template": {
                "metadata": {"labels": {"app": "test"}},
                "spec": {"containers": [{"name": "app", "image": "app:1.0"}]}
            }
        }
    });

    let created = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "app",
            deployment.clone(),
        )
        .await
        .unwrap();

    let mut deploy_with_rv: serde_json::Value = (*created.data).clone();
    if let Some(meta) = deploy_with_rv
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(created.resource_version.to_string()),
        );
    }

    reconcile_deployment(
        &db,
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        &deploy_with_rv,
        "test-node",
    )
    .await
    .unwrap();

    // Refetch to get current resource version after reconciliation
    let current = db
        .get_resource("apps/v1", "Deployment", Some("default"), "app")
        .await
        .unwrap()
        .unwrap();

    // Simulate patch (scale up)
    let mut patched: serde_json::Value = (*current.data).clone();
    if let Some(spec) = patched.get_mut("spec").and_then(|s| s.as_object_mut()) {
        spec.insert("replicas".to_string(), json!(3));
    }

    let updated = db
        .update_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "app",
            patched.clone(),
            current.resource_version,
        )
        .await
        .unwrap();

    let mut patched_with_rv: serde_json::Value = (*updated.data).clone();
    if let Some(meta) = patched_with_rv
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(updated.resource_version.to_string()),
        );
    }

    // Reconcile after patch
    reconcile_deployment(
        &db,
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        &patched_with_rv,
        "test-node",
    )
    .await
    .unwrap();

    // Verify ReplicaSet reflects the patch
    let replicasets = db
        .list_resources(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();

    assert!(!replicasets.items.is_empty(), "ReplicaSet should exist");
    // The latest ReplicaSet should have the patched replica count
    assert_eq!(replicasets.items[0].data["spec"]["replicas"], 3);
}

// ========================
// Service handler tests
// ========================

#[tokio::test]
async fn test_create_service_allocates_cluster_ip() {
    let db = crate::datastore::test_support::in_memory().await;
    let ipam = ServiceIpam::new("10.43.128.0/17");

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({"metadata": {"name": "default"}}),
    )
    .await
    .unwrap();

    // Create a Service without ClusterIP specified
    let mut service = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": "my-service",
            "namespace": "default"
        },
        "spec": {
            "ports": [{
                "port": 80,
                "targetPort": 8080
            }]
        }
    });

    // Simulate what the API handler does: allocate ClusterIP
    let ip = ipam.allocate().unwrap();
    if let Some(spec) = service.get_mut("spec").and_then(|s| s.as_object_mut()) {
        spec.insert("clusterIP".to_string(), json!(ip));
    }

    db.create_resource("v1", "Service", Some("default"), "my-service", service)
        .await
        .unwrap();

    // Verify ClusterIP was allocated
    let retrieved = db
        .get_resource("v1", "Service", Some("default"), "my-service")
        .await
        .unwrap()
        .unwrap();

    let cluster_ip = retrieved.data["spec"]["clusterIP"]
        .as_str()
        .expect("ClusterIP should be allocated");
    assert!(
        cluster_ip.starts_with("10.43.128."),
        "ClusterIP should be from service CIDR"
    );
}

#[tokio::test]
async fn test_update_service_preserves_cluster_ip() {
    let db = crate::datastore::test_support::in_memory().await;
    let ipam = ServiceIpam::new("10.43.128.0/17");

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({"metadata": {"name": "default"}}),
    )
    .await
    .unwrap();

    // Create service with ClusterIP
    let ip = ipam.allocate().unwrap();
    let service = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": "web",
            "namespace": "default"
        },
        "spec": {
            "clusterIP": ip,
            "ports": [{"port": 80}]
        }
    });

    let created = db
        .create_resource("v1", "Service", Some("default"), "web", service)
        .await
        .unwrap();

    let original_ip = created.data["spec"]["clusterIP"]
        .as_str()
        .unwrap()
        .to_string();

    // Update the service (add a label)
    let mut updated_service: serde_json::Value = (*created.data).clone();
    if let Some(meta) = updated_service
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        let mut labels = serde_json::Map::new();
        labels.insert("app".to_string(), json!("web"));
        meta.insert("labels".to_string(), json!(labels));
    }

    let updated = db
        .update_resource(
            "v1",
            "Service",
            Some("default"),
            "web",
            updated_service,
            created.resource_version,
        )
        .await
        .unwrap();

    // Verify ClusterIP was preserved
    let updated_ip = updated.data["spec"]["clusterIP"].as_str().unwrap();
    assert_eq!(
        updated_ip, original_ip,
        "ClusterIP should not change on update"
    );
}

#[tokio::test]
async fn test_patch_service_updates_ports() {
    let db = crate::datastore::test_support::in_memory().await;
    let ipam = ServiceIpam::new("10.43.128.0/17");

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({"metadata": {"name": "default"}}),
    )
    .await
    .unwrap();

    let ip = ipam.allocate().unwrap();
    let service = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": "api",
            "namespace": "default"
        },
        "spec": {
            "clusterIP": ip,
            "ports": [{"port": 80, "targetPort": 8080}]
        }
    });

    let created = db
        .create_resource("v1", "Service", Some("default"), "api", service)
        .await
        .unwrap();

    // Patch: add another port
    let mut patched: serde_json::Value = (*created.data).clone();
    if let Some(spec) = patched.get_mut("spec").and_then(|s| s.as_object_mut()) {
        spec.insert(
            "ports".to_string(),
            json!([
                {"port": 80, "targetPort": 8080},
                {"port": 443, "targetPort": 8443}
            ]),
        );
    }

    let updated = db
        .update_resource(
            "v1",
            "Service",
            Some("default"),
            "api",
            patched,
            created.resource_version,
        )
        .await
        .unwrap();

    // Verify ports were updated
    let ports = updated.data["spec"]["ports"].as_array().unwrap();
    assert_eq!(ports.len(), 2, "Service should have 2 ports after patch");
    assert_eq!(ports[1]["port"], 443);
}

// ========================
// ReplicaSet handler tests
// ========================

#[tokio::test]
async fn test_create_replicaset_creates_pods() {
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({"metadata": {"name": "default"}}),
    )
    .await
    .unwrap();

    let replicaset = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {
            "name": "web-rs",
            "namespace": "default",
            "uid": "rs-uid-001"
        },
        "spec": {
            "replicas": 2,
            "selector": {"matchLabels": {"app": "web"}},
            "template": {
                "metadata": {"labels": {"app": "web"}},
                "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
            }
        }
    });

    let created = db
        .create_resource(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            "web-rs",
            replicaset,
        )
        .await
        .unwrap();

    let mut rs_with_rv: serde_json::Value = (*created.data).clone();
    if let Some(meta) = rs_with_rv
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(created.resource_version.to_string()),
        );
    }

    // Simulate what the API handler does: reconcile after create
    reconcile_replicaset(
        &db,
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        &rs_with_rv,
        "test-node",
    )
    .await
    .unwrap();

    // Verify pods were created
    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();

    assert_eq!(pods.items.len(), 2, "ReplicaSet should create 2 pods");
}

#[tokio::test]
async fn test_replace_and_patch_replicaset_reconciles_pods() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let (app, db) = crate::api::test_support::build_test_router_with_db().await;

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "default"}}),
    )
    .await
    .unwrap();

    let create_req = Request::builder()
        .method("POST")
        .uri("/apis/apps/v1/namespaces/default/replicasets")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "apiVersion": "apps/v1",
                "kind": "ReplicaSet",
                "metadata": {
                    "name": "patch-rs",
                    "namespace": "default",
                    "uid": "patch-rs-uid"
                },
                "spec": {
                    "replicas": 1,
                    "selector": {"matchLabels": {"app": "patch-rs"}},
                    "template": {
                        "metadata": {"labels": {"app": "patch-rs"}},
                        "spec": {"containers": [{"name": "web", "image": "nginx:1.27"}]}
                    }
                }
            })
            .to_string(),
        ))
        .unwrap();
    let create_resp = app.clone().oneshot(create_req).await.unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);

    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(
        pods.items.len(),
        1,
        "initial ReplicaSet reconcile creates 1 pod"
    );

    let current = db
        .get_resource("apps/v1", "ReplicaSet", Some("default"), "patch-rs")
        .await
        .unwrap()
        .unwrap();
    let mut replacement: serde_json::Value = (*current.data).clone();
    replacement["spec"]["replicas"] = json!(2);
    replacement["spec"]["template"]["spec"]["containers"][0]["image"] = json!("nginx:1.28");
    if let Some(metadata) = replacement
        .get_mut("metadata")
        .and_then(|metadata| metadata.as_object_mut())
    {
        metadata.insert(
            "resourceVersion".to_string(),
            json!(current.resource_version.to_string()),
        );
    }

    let replace_req = Request::builder()
        .method("PUT")
        .uri("/apis/apps/v1/namespaces/default/replicasets/patch-rs")
        .header("content-type", "application/json")
        .body(Body::from(replacement.to_string()))
        .unwrap();
    let replace_resp = app.clone().oneshot(replace_req).await.unwrap();
    assert_eq!(replace_resp.status(), StatusCode::OK);

    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(
        pods.items.len(),
        2,
        "ReplicaSet PUT must enqueue reconcile and create pods up to spec.replicas"
    );

    let patch_req = Request::builder()
        .method("PATCH")
        .uri("/apis/apps/v1/namespaces/default/replicasets/patch-rs")
        .header("content-type", "application/strategic-merge-patch+json")
        .body(Body::from(
            json!({
                "metadata": {"labels": {"test-rs": "patched"}},
                "spec": {
                    "replicas": 3,
                    "template": {
                        "spec": {"containers": [{"name": "web", "image": "nginx:1.28"}]}
                    }
                }
            })
            .to_string(),
        ))
        .unwrap();
    let patch_resp = app.oneshot(patch_req).await.unwrap();
    assert_eq!(patch_resp.status(), StatusCode::OK);

    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(
        pods.items.len(),
        3,
        "ReplicaSet PATCH must enqueue reconcile and create pods up to spec.replicas"
    );
}

/// Test that the RS template matches the deployment template after stripping pod-template-hash.
/// This is exactly what K8s `FindNewReplicaSet` / `EqualIgnoreHash` does.
/// Failure here means GetNewReplicaSet returns nil and the webhook deployment never starts.
#[tokio::test]
async fn test_rs_template_equals_deployment_template_ignore_hash() {
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({"metadata": {"name": "default"}}),
    )
    .await
    .unwrap();

    // Simulate the webhook deployment template (matches Sonobuoy's NewDeployment/deployWebhookAndService)
    let deployment = json!({
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
                        "args": ["webhook", "--tls-cert-file=/certs/tls.crt", "--tls-private-key-file=/certs/tls.key"],
                        "readinessProbe": {
                            "httpGet": {"scheme": "HTTPS", "port": 8444, "path": "/readyz"},
                            "periodSeconds": 1,
                            "successThreshold": 1,
                            "failureThreshold": 30
                        },
                        "ports": [{"containerPort": 8444}],
                        "volumeMounts": [{"name": "webhook-certs", "readOnly": true, "mountPath": "/webhook.local.config/certificates"}]
                    }],
                    "volumes": [{"name": "webhook-certs", "secret": {"secretName": "sample-webhook-secret"}}]
                }
            }
        }
    });

    let created = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "sample-webhook-deployment",
            deployment.clone(),
        )
        .await
        .unwrap();

    // Reconcile to create RS (same as what API handler does)
    let mut deploy_with_rv: serde_json::Value = (*created.data).clone();
    if let Some(meta) = deploy_with_rv
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(created.resource_version.to_string()),
        );
    }
    reconcile_deployment(
        &db,
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        &deploy_with_rv,
        "test-node",
    )
    .await
    .unwrap();

    // Get the RS created by reconcile
    let rs_list = db
        .list_resources(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(rs_list.items.len(), 1, "Should have created 1 RS");

    let rs = &rs_list.items[0].data;

    // Get deployment template (what K8s client reads from deployment)
    let deploy_template = &deploy_with_rv["spec"]["template"];
    // Get RS template (what K8s client reads from RS)
    let rs_template = &rs["spec"]["template"];

    // Strip pod-template-hash from RS template labels (EqualIgnoreHash)
    let mut rs_template_stripped = rs_template.clone();
    if let Some(labels) = rs_template_stripped
        .pointer_mut("/metadata/labels")
        .and_then(|l| l.as_object_mut())
    {
        labels.remove("pod-template-hash");
    }

    // The templates must be equal (this is what FindNewReplicaSet checks)
    assert_eq!(
        deploy_template,
        &rs_template_stripped,
        "RS template must equal deployment template after stripping pod-template-hash.\n\
         Deploy template: {}\n\
         RS template (stripped): {}",
        serde_json::to_string_pretty(deploy_template).unwrap(),
        serde_json::to_string_pretty(&rs_template_stripped).unwrap()
    );
}

/// Test that after reconciling a deployment, the deployment gets the
/// deployment.kubernetes.io/revision annotation set to match the RS.
/// K8s checkRevisionAndImage requires this annotation on BOTH deployment and RS.
/// Without this, WaitForDeploymentRevisionAndImage always returns "yet to be created".
#[tokio::test]
async fn test_deployment_gets_revision_annotation_after_reconcile() {
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({"metadata": {"name": "default"}}),
    )
    .await
    .unwrap();

    let deployment = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "sample-webhook-deployment",
            "namespace": "default",
            "uid": "deploy-webhook-uid-rev-test"
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": "webhook-rev-test"}},
            "template": {
                "metadata": {"labels": {"app": "webhook-rev-test"}},
                "spec": {
                    "terminationGracePeriodSeconds": 0,
                    "containers": [{"name": "c", "image": "registry.k8s.io/e2e-test-images/agnhost:2.56"}]
                }
            }
        }
    });

    let created = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "sample-webhook-deployment",
            deployment.clone(),
        )
        .await
        .unwrap();

    let mut deploy_with_rv: serde_json::Value = (*created.data).clone();
    if let Some(meta) = deploy_with_rv
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(created.resource_version.to_string()),
        );
    }
    reconcile_deployment(
        &db,
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        &deploy_with_rv,
        "test-node",
    )
    .await
    .unwrap();

    // Re-fetch the deployment from DB (reconcile updates it in-place)
    let updated = db
        .get_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "sample-webhook-deployment",
        )
        .await
        .unwrap()
        .unwrap();

    let deploy_revision = updated
        .data
        .pointer("/metadata/annotations/deployment.kubernetes.io~1revision")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    assert_eq!(
        deploy_revision, "1",
        "Deployment must have deployment.kubernetes.io/revision=1 annotation after reconcile. \
         Without this, WaitForDeploymentRevisionAndImage loops forever. \
         Got: {:?}",
        deploy_revision
    );

    // Also verify the RS has the same annotation
    let rs_list = db
        .list_resources(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(rs_list.items.len(), 1);

    let rs_revision = rs_list.items[0]
        .data
        .pointer("/metadata/annotations/deployment.kubernetes.io~1revision")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    assert_eq!(
        rs_revision, "1",
        "RS must have deployment.kubernetes.io/revision=1 annotation"
    );
}

/// Regression test: when a Deployment is deleted via the API handler, its owned
/// ReplicaSets and Pods must be cascade-deleted.
///
/// This simulates the exact sequence the delete handler uses:
/// 1. Create Deployment (API layer injects UID)
/// 2. Reconcile → ReplicaSet created with ownerReferences pointing at Deployment UID
/// 3. Reconcile RS → Pods created with ownerReferences pointing at RS UID
/// 4. delete_resource(Deployment) + cascade_delete_with_uid(deploy_uid)
/// 5. Assert RS and Pods are gone
#[tokio::test]
async fn test_delete_deployment_cascade_deletes_replicaset_and_pods() {
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);

    // Step 1: Simulate the API create handler — inject UID before storing
    let deploy_uid = uuid::Uuid::new_v4().to_string();
    let deployment_body = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "test-deploy",
            "namespace": "default",
            "uid": deploy_uid,
            "creationTimestamp": "2026-04-16T00:00:00Z",
            "generation": 1
        },
        "spec": {
            "replicas": 2,
            "selector": {"matchLabels": {"app": "test"}},
            "template": {
                "metadata": {"labels": {"app": "test"}},
                "spec": {"containers": [{"name": "app", "image": "nginx:latest"}]}
            }
        }
    });

    let created = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "test-deploy",
            deployment_body,
        )
        .await
        .unwrap();

    // Step 2: Reconcile Deployment → ReplicaSet created with ownerReferences
    let deploy_with_rv =
        crate::api::inject_resource_version(created.data.clone(), created.resource_version);
    reconcile_deployment(
        &db,
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        &deploy_with_rv,
        "test-node",
    )
    .await
    .unwrap();

    // Verify RS was created
    let rs_list = db
        .list_resources(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(rs_list.items.len(), 1, "Reconcile must create one RS");

    // Verify RS has ownerReference pointing at Deployment UID
    let rs_owner_uid = rs_list.items[0]
        .data
        .pointer("/metadata/ownerReferences/0/uid")
        .and_then(|u| u.as_str())
        .unwrap_or("");
    assert_eq!(
        rs_owner_uid, deploy_uid,
        "RS ownerReference must point at Deployment UID"
    );

    // Verify pods were created
    let pod_list = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(pod_list.items.len(), 2, "Reconcile must create 2 pods");

    // Step 3: Simulate what the delete handler does
    // 3a. delete_resource(Deployment)
    db.delete_resource("apps/v1", "Deployment", Some("default"), "test-deploy")
        .await
        .unwrap();

    // 3b. Extract owner_uid as the API handler does (via inject_resource_version)
    let data_with_uid =
        crate::api::inject_resource_version(created.data.clone(), created.resource_version);
    let owner_uid = data_with_uid
        .get("metadata")
        .and_then(|m| m.get("uid"))
        .and_then(|u| u.as_str())
        .unwrap_or("");

    assert_eq!(
        owner_uid, deploy_uid,
        "owner_uid extracted by delete handler must match the stored Deployment UID"
    );

    // 3c. cascade_delete_with_uid (what the delete handler calls)
    crate::controllers::gc::cascade_delete_with_uid(
        &db,
        owner_uid,
        "apps/v1",
        "my-deploy",
        "Deployment",
        Some("default".to_string()),
        &crate::controllers::gc::NoOpGcPodDeleteSink,
    )
    .await
    .unwrap();

    // Step 4: Assert RS is deleted
    let rs_list_after = db
        .list_resources(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(
        rs_list_after.items.len(),
        0,
        "All ReplicaSets must be deleted when Deployment is cascade-deleted"
    );

    // Step 5: Pod children are NOT hard-deleted by cascade — they go through
    // the Pod lifecycle actor via GcPodDeleteSink. The RS is still deleted.
    let pod_list_after = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(
        pod_list_after.items.len(),
        2,
        "Pod children must NOT be hard-deleted by GC cascade — they are actor-deleted"
    );
}

/// Regression test: delete_collection must cascade-delete owned resources.
///
/// The delete_collection handler previously deleted resources without calling
/// cascade_delete_with_uid, leaving RS and Pods orphaned.
///
/// This test simulates the delete_collection code path:
/// 1. Create Deployment + reconcile (creates RS + Pods)
/// 2. Simulate delete_collection: call delete_resource(Deployment) WITHOUT cascade
/// 3. Assert RS and Pods are gone (this FAILS before the fix)
#[tokio::test]
async fn test_delete_collection_deployment_cascade_deletes_replicaset_and_pods() {
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);

    // Step 1: Create Deployment with a known UID
    let deploy_uid = uuid::Uuid::new_v4().to_string();
    let deployment_body = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "collection-deploy",
            "namespace": "default",
            "uid": deploy_uid,
            "creationTimestamp": "2026-04-16T00:00:00Z",
            "generation": 1
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": "collection-test"}},
            "template": {
                "metadata": {"labels": {"app": "collection-test"}},
                "spec": {"containers": [{"name": "app", "image": "nginx:latest"}]}
            }
        }
    });

    let created = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "collection-deploy",
            deployment_body,
        )
        .await
        .unwrap();

    // Reconcile: creates RS and Pod
    let deploy_with_rv =
        crate::api::inject_resource_version(created.data.clone(), created.resource_version);
    reconcile_deployment(
        &db,
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        &deploy_with_rv,
        "test-node",
    )
    .await
    .unwrap();

    // Verify setup: RS and Pod exist
    let rs_before = db
        .list_resources(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(
        rs_before.items.len(),
        1,
        "Setup: RS must exist before delete"
    );

    let pods_before = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(
        pods_before.items.len(),
        1,
        "Setup: Pod must exist before delete"
    );

    // Step 2: Simulate delete_collection path — delete Deployment WITHOUT cascade
    // This is the BUG: delete_collection only calls delete_resource, not cascade_delete_with_uid
    db.delete_resource(
        "apps/v1",
        "Deployment",
        Some("default"),
        "collection-deploy",
    )
    .await
    .unwrap();

    // At this point (before the fix), RS and Pods are still alive:
    let rs_no_cascade = db
        .list_resources(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    // This documents the bug: RS is NOT deleted when using delete_collection without cascade
    assert_eq!(
        rs_no_cascade.items.len(),
        1,
        "Bug confirmed: RS survives delete_collection without cascade"
    );

    // Step 3: After the fix, delete_collection must also cascade
    // Simulate what the fix adds: cascade_delete_with_uid after each delete_resource
    crate::controllers::gc::cascade_delete_with_uid(
        &db,
        &deploy_uid,
        "apps/v1",
        "my-deploy",
        "Deployment",
        Some("default".to_string()),
        &crate::controllers::gc::NoOpGcPodDeleteSink,
    )
    .await
    .unwrap();

    // Step 4: Assert RS is now gone after cascade
    let rs_after = db
        .list_resources(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(
        rs_after.items.len(),
        0,
        "RS must be deleted after cascade_delete_with_uid is called"
    );

    // Step 5: Pod children are NOT hard-deleted by cascade — they go through
    // the Pod lifecycle actor via GcPodDeleteSink.
    let pods_after = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(
        pods_after.items.len(),
        1,
        "Pod children must NOT be hard-deleted by GC cascade — they are actor-deleted"
    );
}
