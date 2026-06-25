use super::*;

#[tokio::test]
async fn test_reconcile_deployment_creates_replicaset() {
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);
    let deploy_uid = "deploy-uid-1234";

    // Create the deployment in DB first
    let deploy = make_deployment("nginx", "default", deploy_uid, 3, "0");
    let created = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "nginx",
            deploy.clone(),
        )
        .await
        .unwrap();

    // Inject resource version for reconcile
    let deploy_with_rv =
        crate::api::inject_resource_version(created.data, created.resource_version);

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
    let rs_list = db
        .list_resources(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(
        rs_list.items.len(),
        1,
        "Should create exactly one ReplicaSet"
    );

    let rs = &rs_list.items[0];
    // Verify RS spec matches deployment
    assert_eq!(rs.data["spec"]["replicas"], 3);
    assert_eq!(rs.data["spec"]["selector"]["matchLabels"]["app"], "nginx");

    // Verify ownerReferences point back to deployment
    let owner_refs = rs.data["metadata"]["ownerReferences"].as_array().unwrap();
    assert_eq!(owner_refs.len(), 1);
    assert_eq!(owner_refs[0]["uid"], deploy_uid);
    assert_eq!(owner_refs[0]["kind"], "Deployment");
    assert_eq!(owner_refs[0]["controller"], true);

    // Verify pods were created (3 replicas)
    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(pods.items.len(), 3, "Should create 3 pods");
}

#[test]
fn test_templates_match_ignores_pod_template_hash() {
    let t1 = json!({
        "metadata": {"labels": {"app": "nginx"}},
        "spec": {"containers": [{"name": "nginx", "image": "nginx:1.0"}]}
    });
    let t2 = json!({
        "metadata": {"labels": {"app": "nginx", "pod-template-hash": "abc123"}},
        "spec": {"containers": [{"name": "nginx", "image": "nginx:1.0"}]}
    });
    assert!(
        templates_match(&t1, &t2),
        "Should match when only difference is pod-template-hash"
    );

    let t3 = json!({
        "metadata": {"labels": {"app": "nginx", "pod-template-hash": "xyz"}},
        "spec": {"containers": [{"name": "nginx", "image": "nginx:2.0"}]}
    });
    assert!(
        !templates_match(&t1, &t3),
        "Should not match when spec differs"
    );
}

#[tokio::test]
async fn test_reconcile_deployment_rs_has_pod_template_hash() {
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);
    let deploy = make_deployment("myapp", "default", "uid-hash-test", 1, "0");
    let created = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "myapp",
            deploy.clone(),
        )
        .await
        .unwrap();
    let deploy_with_rv =
        crate::api::inject_resource_version(created.data, created.resource_version);

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
    let rs = &rs_list.items[0];

    let rs_name = rs.data["metadata"]["name"].as_str().unwrap();
    let expected_prefix = "myapp-";
    let hash = rs_name
        .strip_prefix(expected_prefix)
        .expect("RS name must start with deployment name plus '-'");
    assert!(
        !hash.is_empty(),
        "RS name must be [deployment-name]-[pod-template-hash]"
    );

    // RS metadata.labels has pod-template-hash
    let rs_labels = rs.data["metadata"]["labels"].as_object().unwrap();
    assert_eq!(
        rs_labels.get("pod-template-hash").and_then(|v| v.as_str()),
        Some(hash),
        "RS metadata.labels must include pod-template-hash"
    );

    // RS metadata.labels also has deployment's selector matchLabels
    assert_eq!(
        rs_labels.get("app").and_then(|v| v.as_str()),
        Some("myapp"),
        "RS metadata.labels must include deployment selector labels"
    );

    // RS spec.selector.matchLabels has pod-template-hash
    let selector_labels = rs.data["spec"]["selector"]["matchLabels"]
        .as_object()
        .unwrap();
    assert_eq!(
        selector_labels
            .get("pod-template-hash")
            .and_then(|v| v.as_str()),
        Some(hash),
        "RS spec.selector.matchLabels must include pod-template-hash"
    );

    // RS spec.template.metadata.labels has pod-template-hash
    let template_labels = rs.data["spec"]["template"]["metadata"]["labels"]
        .as_object()
        .unwrap();
    assert_eq!(
        template_labels
            .get("pod-template-hash")
            .and_then(|v| v.as_str()),
        Some(hash),
        "RS spec.template.metadata.labels must include pod-template-hash"
    );

    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(pods.items.len(), 1);
    let pod = &pods.items[0].data;
    let pod_name = pod["metadata"]["name"].as_str().unwrap();
    assert!(
        pod_name.starts_with(&format!("{rs_name}-")),
        "Pod name must be [replicaset-name]-[suffix], got {pod_name}"
    );
    assert_eq!(
        pod["metadata"]["labels"]["pod-template-hash"].as_str(),
        Some(hash),
        "Pods created by the RS must carry the same pod-template-hash"
    );
}

#[tokio::test]
async fn test_reconcile_deployment_scales_replicaset() {
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);
    let deploy_uid = "deploy-uid-scale";

    // Create deployment with 2 replicas
    let deploy = make_deployment("web", "default", deploy_uid, 2, "0");
    let created = db
        .create_resource("apps/v1", "Deployment", Some("default"), "web", deploy)
        .await
        .unwrap();

    let deploy_with_rv =
        crate::api::inject_resource_version(created.data, created.resource_version);
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

    // Verify 2 pods
    let pods_before = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(pods_before.items.len(), 2);

    // Now scale up to 5 by updating deployment in DB
    let current_deploy = db
        .get_resource("apps/v1", "Deployment", Some("default"), "web")
        .await
        .unwrap()
        .unwrap();

    let scaled_deploy = make_deployment("web", "default", deploy_uid, 5, "0");
    let updated = db
        .update_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "web",
            scaled_deploy,
            current_deploy.resource_version,
        )
        .await
        .unwrap();

    let deploy_with_rv =
        crate::api::inject_resource_version(updated.data, updated.resource_version);
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

    // Verify 5 pods
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
        5,
        "Should have 5 pods after scale up"
    );
}

#[tokio::test]
async fn test_reconcile_deployment_no_duplicate_replicasets() {
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);
    let deploy_uid = "deploy-uid-nodup";

    // Create and reconcile deployment
    let deploy = make_deployment("api", "default", deploy_uid, 1, "0");
    let created = db
        .create_resource("apps/v1", "Deployment", Some("default"), "api", deploy)
        .await
        .unwrap();

    let deploy_with_rv =
        crate::api::inject_resource_version(created.data, created.resource_version);
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

    // Re-fetch deployment (status was updated by reconcile)
    let current = db
        .get_resource("apps/v1", "Deployment", Some("default"), "api")
        .await
        .unwrap()
        .unwrap();

    // Reconcile again with same replicas
    let deploy_with_rv2 =
        crate::api::inject_resource_version(current.data, current.resource_version);
    reconcile_deployment(
        &db,
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        &deploy_with_rv2,
        "test-node",
    )
    .await
    .unwrap();

    // Should still have exactly 1 ReplicaSet
    let rs_list = db
        .list_resources(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(
        rs_list.items.len(),
        1,
        "Should not create duplicate ReplicaSets on re-reconcile"
    );

    // Should still have exactly 1 pod
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
        "Should not create duplicate pods on re-reconcile"
    );
}

#[tokio::test]
async fn test_reconcile_deployment_zero_replicas_creates_no_pods() {
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);
    let deploy_uid = "deploy-uid-zero";

    let deploy = make_deployment("paused", "default", deploy_uid, 0, "0");
    let created = db
        .create_resource("apps/v1", "Deployment", Some("default"), "paused", deploy)
        .await
        .unwrap();

    let deploy_with_rv =
        crate::api::inject_resource_version(created.data, created.resource_version);
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

    // ReplicaSet should exist with 0 replicas
    let rs_list = db
        .list_resources(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(
        rs_list.items.len(),
        1,
        "Should create ReplicaSet even with 0 replicas"
    );
    assert_eq!(rs_list.items[0].data["spec"]["replicas"], 0);

    // No pods should be created
    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(pods.items.len(), 0, "Zero replicas should create zero pods");
}

#[tokio::test]
async fn test_reconcile_deployment_status_has_progressing_condition() {
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);
    let deploy_uid = "deploy-uid-status";

    let deploy = make_deployment("web", "default", deploy_uid, 2, "0");
    let created = db
        .create_resource("apps/v1", "Deployment", Some("default"), "web", deploy)
        .await
        .unwrap();

    let deploy_with_rv =
        crate::api::inject_resource_version(created.data, created.resource_version);
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

    // Re-fetch deployment to check status
    let updated = db
        .get_resource("apps/v1", "Deployment", Some("default"), "web")
        .await
        .unwrap()
        .unwrap();

    let status = &updated.data["status"];
    // status.replicas reflects pods created by the new RS (reconcile_replicaset runs inline)
    assert_eq!(status["replicas"], 2);
    // status.observedGeneration should match metadata.generation
    assert_eq!(status["observedGeneration"], 1);

    let conditions = status["conditions"].as_array().unwrap();
    assert!(!conditions.is_empty(), "Status must have conditions");

    let progressing = conditions.iter().find(|c| c["type"] == "Progressing");
    assert!(progressing.is_some(), "Must have Progressing condition");
    let progressing = progressing.unwrap();
    assert_eq!(progressing["status"], "True");
    assert_eq!(progressing["reason"], "NewReplicaSetCreated");

    // The message must contain the actual RS name, not "unknown"
    let message = progressing["message"].as_str().unwrap();
    assert!(
        !message.contains("unknown"),
        "Progressing message must contain actual RS name, got: {}",
        message
    );
    assert!(
        message.starts_with("Created new replica set \"web-"),
        "Progressing message must reference RS named after deployment, got: {}",
        message
    );
}

#[tokio::test]
async fn test_reconcile_deployment_status_noops_when_condition_state_unchanged() {
    let db = crate::datastore::test_support::in_memory().await;
    let pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);
    let deploy_uid = "deploy-uid-status-noop";

    let deploy = make_deployment("web-noop", "default", deploy_uid, 2, "0");
    let created = db
        .create_resource("apps/v1", "Deployment", Some("default"), "web-noop", deploy)
        .await
        .unwrap();

    let deploy_with_rv =
        crate::api::inject_resource_version(created.data, created.resource_version);
    reconcile_deployment(
        &db,
        pod_repo.as_ref(),
        pod_repo.as_ref(),
        pod_repo.as_ref(),
        &deploy_with_rv,
        "test-node",
    )
    .await
    .unwrap();

    let first = db
        .get_resource("apps/v1", "Deployment", Some("default"), "web-noop")
        .await
        .unwrap()
        .unwrap();

    let first_with_rv = crate::api::inject_resource_version(first.data, first.resource_version);
    reconcile_deployment(
        &db,
        pod_repo.as_ref(),
        pod_repo.as_ref(),
        pod_repo.as_ref(),
        &first_with_rv,
        "test-node",
    )
    .await
    .unwrap();

    let second = db
        .get_resource("apps/v1", "Deployment", Some("default"), "web-noop")
        .await
        .unwrap()
        .unwrap();
    let second_rv = second.resource_version;
    let second_status = second.data["status"].clone();
    assert_eq!(
        second_status
            .pointer("/conditions/1/reason")
            .and_then(|value| value.as_str()),
        Some("NewReplicaSetAvailable"),
        "test must settle the legitimate Progressing condition transition"
    );

    let second_with_rv = crate::api::inject_resource_version(second.data, second.resource_version);
    reconcile_deployment(
        &db,
        pod_repo.as_ref(),
        pod_repo.as_ref(),
        pod_repo.as_ref(),
        &second_with_rv,
        "test-node",
    )
    .await
    .unwrap();

    let third = db
        .get_resource("apps/v1", "Deployment", Some("default"), "web-noop")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        third.resource_version, second_rv,
        "unchanged Deployment status must not bump resourceVersion"
    );
    assert_eq!(
        third.data["status"], second_status,
        "unchanged condition state must preserve existing timestamps"
    );
}

struct ObservedGenerationBeforeCreateWriter {
    db: crate::datastore::sqlite::Datastore,
    inner: std::sync::Arc<crate::kubelet::pod_repository::PodRepository>,
    namespace: &'static str,
    deployment_name: &'static str,
    checked: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl crate::kubelet::pod_repository::PodObjectWriter for ObservedGenerationBeforeCreateWriter {
    async fn create_controller_pod(
        &self,
        ns: &str,
        name: &str,
        node_name: &str,
        pod: Value,
    ) -> anyhow::Result<crate::datastore::Resource> {
        if !self.checked.swap(true, std::sync::atomic::Ordering::SeqCst) {
            let deployment = self
                .db
                .get_resource(
                    "apps/v1",
                    "Deployment",
                    Some(self.namespace),
                    self.deployment_name,
                )
                .await?
                .expect("deployment must exist while creating child pods");
            let observed = deployment
                .data
                .pointer("/status/observedGeneration")
                .and_then(|v| v.as_i64());
            anyhow::ensure!(
                observed == Some(1),
                "deployment status must observe generation before pod create, got {:?}",
                observed
            );
        }
        self.inner
            .create_controller_pod(ns, name, node_name, pod)
            .await
    }

    async fn delete_pod(&self, ns: &str, name: &str) -> anyhow::Result<()> {
        self.inner.delete_pod(ns, name).await
    }

    async fn update_pod_owner_references(
        &self,
        ns: &str,
        name: &str,
        owner_refs: Vec<Value>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.inner
            .update_pod_owner_references(ns, name, owner_refs)
            .await
    }

    async fn merge_pod_labels(
        &self,
        ns: &str,
        name: &str,
        labels: Vec<(String, String)>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.inner.merge_pod_labels(ns, name, labels).await
    }
}

#[tokio::test]
async fn test_reconcile_deployment_observes_generation_before_pod_create() {
    let db = crate::datastore::test_support::in_memory().await;
    let pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);

    let deploy = make_deployment("web-observed", "default", "deploy-uid-observed", 3, "0");
    let created = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "web-observed",
            deploy,
        )
        .await
        .unwrap();

    let deploy_with_rv =
        crate::api::inject_resource_version(created.data, created.resource_version);
    let writer = ObservedGenerationBeforeCreateWriter {
        db: db.clone(),
        inner: pod_repo.clone(),
        namespace: "default",
        deployment_name: "web-observed",
        checked: std::sync::atomic::AtomicBool::new(false),
    };

    reconcile_deployment(
        &db,
        pod_repo.as_ref(),
        &writer,
        pod_repo.as_ref(),
        &deploy_with_rv,
        "test-node",
    )
    .await
    .expect("deployment reconcile should acknowledge observedGeneration before pod creation");

    assert!(
        writer.checked.load(std::sync::atomic::Ordering::SeqCst),
        "test must exercise the controller pod create path"
    );
}

struct RollingUpdateObservedGenerationReader {
    db: crate::datastore::sqlite::Datastore,
    inner: std::sync::Arc<crate::kubelet::pod_repository::PodRepository>,
    namespace: &'static str,
    deployment_name: &'static str,
    new_image: &'static str,
    checked: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl crate::kubelet::pod_repository::PodReader for RollingUpdateObservedGenerationReader {
    async fn get_pod(
        &self,
        ns: &str,
        name: &str,
    ) -> anyhow::Result<Option<crate::datastore::Resource>> {
        self.inner.get_pod(ns, name).await
    }

    async fn get_pod_for_uid(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
    ) -> anyhow::Result<Option<crate::datastore::Resource>> {
        self.inner.get_pod_for_uid(ns, name, uid).await
    }

    async fn list_pods(
        &self,
        ns: Option<&str>,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        limit: Option<i64>,
        continue_token: Option<&str>,
    ) -> anyhow::Result<crate::datastore::ResourceList> {
        self.inner
            .list_pods(ns, label_selector, field_selector, limit, continue_token)
            .await
    }

    async fn list_pods_by_owner_uid(
        &self,
        ns: &str,
        owner_uid: &str,
    ) -> anyhow::Result<Vec<crate::datastore::Resource>> {
        if !self.checked.swap(true, std::sync::atomic::Ordering::SeqCst) {
            let deployment = self
                .db
                .get_resource(
                    "apps/v1",
                    "Deployment",
                    Some(self.namespace),
                    self.deployment_name,
                )
                .await?
                .expect("deployment must exist while planning rollout");
            let observed = deployment
                .data
                .pointer("/status/observedGeneration")
                .and_then(|v| v.as_i64());
            let has_new_rs = self
                .db
                .list_resources(
                    "apps/v1",
                    "ReplicaSet",
                    Some(self.namespace),
                    crate::datastore::ResourceListQuery::all(),
                )
                .await?
                .items
                .iter()
                .any(|rs| {
                    rs.data
                        .pointer("/spec/template/spec/containers/0/image")
                        .and_then(|v| v.as_str())
                        == Some(self.new_image)
                });
            anyhow::ensure!(
                observed != Some(2) || has_new_rs,
                "deployment observedGeneration advanced to 2 before matching new ReplicaSet existed"
            );
        }
        self.inner.list_pods_by_owner_uid(ns, owner_uid).await
    }
}

#[tokio::test]
async fn test_reconcile_deployment_rollout_observed_generation_waits_for_new_replicaset() {
    let db = crate::datastore::test_support::in_memory().await;
    let pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);
    let deploy_uid = "deploy-uid-rollout-observed";

    let deploy = make_deployment_with_image(
        "web-observed-rollout",
        "default",
        deploy_uid,
        3,
        "0",
        "nginx:1.14",
    );
    let created = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "web-observed-rollout",
            deploy,
        )
        .await
        .unwrap();
    let deploy_with_rv =
        crate::api::inject_resource_version(created.data, created.resource_version);
    reconcile_deployment(
        &db,
        pod_repo.as_ref(),
        pod_repo.as_ref(),
        pod_repo.as_ref(),
        &deploy_with_rv,
        "test-node",
    )
    .await
    .unwrap();

    let current_deploy = db
        .get_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "web-observed-rollout",
        )
        .await
        .unwrap()
        .unwrap();
    let mut updated_deploy = make_deployment_with_image(
        "web-observed-rollout",
        "default",
        deploy_uid,
        3,
        "0",
        "nginx:1.16",
    );
    updated_deploy["metadata"]["generation"] = json!(2);
    let updated = db
        .update_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "web-observed-rollout",
            updated_deploy,
            current_deploy.resource_version,
        )
        .await
        .unwrap();

    let reader = RollingUpdateObservedGenerationReader {
        db: db.clone(),
        inner: pod_repo.clone(),
        namespace: "default",
        deployment_name: "web-observed-rollout",
        new_image: "nginx:1.16",
        checked: std::sync::atomic::AtomicBool::new(false),
    };
    let deploy_with_rv =
        crate::api::inject_resource_version(updated.data, updated.resource_version);
    reconcile_deployment(
        &db,
        &reader,
        pod_repo.as_ref(),
        pod_repo.as_ref(),
        &deploy_with_rv,
        "test-node",
    )
    .await
    .expect("rollout reconcile must not acknowledge generation before new RS creation");

    assert!(
        reader.checked.load(std::sync::atomic::Ordering::SeqCst),
        "test must exercise the rolling update pod reader path before new RS creation"
    );
    let deployment = db
        .get_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "web-observed-rollout",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        deployment
            .data
            .pointer("/status/observedGeneration")
            .and_then(|v| v.as_i64()),
        Some(2),
        "final rollout status should still observe generation 2"
    );
}

#[tokio::test]
async fn test_reconcile_deployment_status_replicas_reflects_new_rs_pods() {
    // Regression test: after creating a new RS and reconciling it, status.replicas
    // must count the pods created by the new RS — not use the stale owned_rs_list
    // captured before RS creation (which had no entries).
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);
    let deploy_uid = "deploy-uid-rs-pods";

    let deploy = make_deployment("web2", "default", deploy_uid, 2, "0");
    let created = db
        .create_resource("apps/v1", "Deployment", Some("default"), "web2", deploy)
        .await
        .unwrap();

    let deploy_with_rv =
        crate::api::inject_resource_version(created.data, created.resource_version);
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

    // Verify 2 pods were created by the RS reconciliation
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
        "reconcile_replicaset must create 2 pods"
    );

    // status.replicas must reflect pods created by the new RS, not the stale count
    let updated = db
        .get_resource("apps/v1", "Deployment", Some("default"), "web2")
        .await
        .unwrap()
        .unwrap();

    let status = &updated.data["status"];
    assert_eq!(
        status["replicas"], 2,
        "status.replicas must equal pod count after new RS creation"
    );
}

#[tokio::test]
async fn test_reconcile_deployment_status_ignores_terminal_node_lost_pods() {
    let db = crate::datastore::test_support::in_memory().await;
    let pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);
    let deploy_uid = "deploy-uid-node-lost";
    let rs_uid = "rs-uid-node-lost";

    let deploy = make_deployment("dns", "default", deploy_uid, 1, "0");
    let created = db
        .create_resource("apps/v1", "Deployment", Some("default"), "dns", deploy)
        .await
        .unwrap();

    db.create_resource(
        "apps/v1",
        "ReplicaSet",
        Some("default"),
        "dns-rs",
        json!({
            "apiVersion": "apps/v1",
            "kind": "ReplicaSet",
            "metadata": {
                "name": "dns-rs",
                "namespace": "default",
                "uid": rs_uid,
                "ownerReferences": [{
                    "apiVersion": "apps/v1",
                    "kind": "Deployment",
                    "name": "dns",
                    "uid": deploy_uid,
                    "controller": true,
                    "blockOwnerDeletion": true
                }]
            },
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": {"app": "dns"}},
                "template": {
                    "metadata": {"labels": {"app": "dns"}},
                    "spec": {"containers": [{"name": "nginx", "image": "nginx:latest"}]}
                }
            }
        }),
    )
    .await
    .unwrap();

    for (name, phase, ready) in [("dns-old", "Failed", false), ("dns-new", "Running", true)] {
        let conditions = if ready {
            json!([
                {"type": "Ready", "status": "True"},
                {"type": "ContainersReady", "status": "True"}
            ])
        } else {
            json!([
                {"type": "Ready", "status": "False", "reason": "NodeLost"},
                {"type": "ContainersReady", "status": "False", "reason": "NodeLost"}
            ])
        };
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            name,
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": name,
                    "namespace": "default",
                    "labels": {"app": "dns"},
                    "ownerReferences": [{
                        "apiVersion": "apps/v1",
                        "kind": "ReplicaSet",
                        "name": "dns-rs",
                        "uid": rs_uid,
                        "controller": true,
                        "blockOwnerDeletion": true
                    }]
                },
                "spec": {"containers": [{"name": "nginx", "image": "nginx:latest"}]},
                "status": {
                    "phase": phase,
                    "reason": if phase == "Failed" { "NodeLost" } else { "" },
                    "conditions": conditions
                }
            }),
        )
        .await
        .unwrap();
    }

    let deploy_with_rv =
        crate::api::inject_resource_version(created.data, created.resource_version);
    reconcile_deployment(
        &db,
        pod_repo.as_ref(),
        pod_repo.as_ref(),
        pod_repo.as_ref(),
        &deploy_with_rv,
        "test-node",
    )
    .await
    .unwrap();

    let updated = db
        .get_resource("apps/v1", "Deployment", Some("default"), "dns")
        .await
        .unwrap()
        .unwrap();
    let status = &updated.data["status"];
    assert_eq!(status["replicas"], json!(1));
    assert_eq!(status["updatedReplicas"], json!(1));
    assert_eq!(status["readyReplicas"], json!(1));
    assert_eq!(status["availableReplicas"], json!(1));
}

#[tokio::test]
async fn test_reconcile_deployment_status_includes_newly_created_rs() {
    // Regression: first reconcile with no prior RS must show replicas >= 1 in status
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);
    let deploy_uid = "deploy-uid-new-rs-incl";

    let deploy = make_deployment("webx", "default", deploy_uid, 2, "0");
    let created = db
        .create_resource("apps/v1", "Deployment", Some("default"), "webx", deploy)
        .await
        .unwrap();

    let deploy_with_rv =
        crate::api::inject_resource_version(created.data, created.resource_version);
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

    let updated = db
        .get_resource("apps/v1", "Deployment", Some("default"), "webx")
        .await
        .unwrap()
        .unwrap();

    let status = &updated.data["status"];
    let replicas = status["replicas"].as_i64().unwrap_or(0);
    assert!(
        replicas >= 1,
        "status.replicas must be >= 1 after first reconcile with new RS, got {}",
        replicas
    );
}

#[tokio::test]
async fn test_reconcile_deployment_scale_down_to_zero() {
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);
    let deploy_uid = "deploy-uid-scaledown0";

    // Create deployment with 3 replicas
    let deploy = make_deployment("app", "default", deploy_uid, 3, "0");
    let created = db
        .create_resource("apps/v1", "Deployment", Some("default"), "app", deploy)
        .await
        .unwrap();

    let deploy_with_rv =
        crate::api::inject_resource_version(created.data, created.resource_version);
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

    let pods_before = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(pods_before.items.len(), 3);

    // Scale down to 0
    let current = db
        .get_resource("apps/v1", "Deployment", Some("default"), "app")
        .await
        .unwrap()
        .unwrap();

    let scaled = make_deployment("app", "default", deploy_uid, 0, "0");
    let updated = db
        .update_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "app",
            scaled,
            current.resource_version,
        )
        .await
        .unwrap();

    let deploy_with_rv =
        crate::api::inject_resource_version(updated.data, updated.resource_version);
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

    let pods_after = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    let active_after = pods_after
        .items
        .iter()
        .filter(|pod| pod.data.pointer("/metadata/deletionTimestamp").is_none())
        .count();
    assert_eq!(
        active_after, 0,
        "Scale to 0 should mark all active pods terminating"
    );
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

#[tokio::test]
async fn test_reconcile_deployment_rolling_update_creates_new_replicaset() {
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);
    let deploy_uid = "deploy-uid-rolling";

    // Create deployment with nginx:1.14 and 3 replicas
    let deploy = make_deployment_with_image("web", "default", deploy_uid, 3, "0", "nginx:1.14");
    let created = db
        .create_resource("apps/v1", "Deployment", Some("default"), "web", deploy)
        .await
        .unwrap();

    let deploy_with_rv =
        crate::api::inject_resource_version(created.data, created.resource_version);
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

    // Verify initial state: 1 ReplicaSet with 3 replicas
    let rs_list = db
        .list_resources(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(rs_list.items.len(), 1, "Should have 1 ReplicaSet initially");
    assert_eq!(rs_list.items[0].data["spec"]["replicas"], 3);

    // Update deployment to use nginx:1.16 (trigger rolling update)
    let current_deploy = db
        .get_resource("apps/v1", "Deployment", Some("default"), "web")
        .await
        .unwrap()
        .unwrap();

    let updated_deploy =
        make_deployment_with_image("web", "default", deploy_uid, 3, "0", "nginx:1.16");
    let updated = db
        .update_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "web",
            updated_deploy,
            current_deploy.resource_version,
        )
        .await
        .unwrap();

    let deploy_with_rv =
        crate::api::inject_resource_version(updated.data, updated.resource_version);
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

    // Verify rolling update behavior: should have 2 ReplicaSets
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
        2,
        "Rolling update should create new ReplicaSet, keeping old one"
    );

    // Find old and new ReplicaSets by checking template spec
    let old_rs = rs_list_after
        .items
        .iter()
        .find(|rs| rs.data["spec"]["template"]["spec"]["containers"][0]["image"] == "nginx:1.14")
        .expect("Old ReplicaSet with nginx:1.14 should exist");
    let new_rs = rs_list_after
        .items
        .iter()
        .find(|rs| rs.data["spec"]["template"]["spec"]["containers"][0]["image"] == "nginx:1.16")
        .expect("New ReplicaSet with nginx:1.16 should exist");

    // Kubernetes rolling update keeps the old RS controlled and creates only
    // the surge budget of new replicas on the first step.
    let old_replicas = old_rs.data["spec"]["replicas"].as_i64().unwrap();
    let new_replicas = new_rs.data["spec"]["replicas"].as_i64().unwrap();

    assert_eq!(new_replicas, 1, "new RS should start at maxSurge");
    assert_eq!(
        old_replicas, 2,
        "old RS may scale down to preserve maxUnavailable during the first rollout step"
    );

    // Verify the old RS remains controlled so its availability still counts.
    let old_controlled = old_rs
        .data
        .pointer("/metadata/ownerReferences")
        .and_then(|v| v.as_array())
        .map(|refs| {
            refs.iter().any(|r| {
                r.get("controller").and_then(|c| c.as_bool()) == Some(true)
                    && r.get("uid").and_then(|u| u.as_str()) == Some(deploy_uid)
            })
        })
        .unwrap_or(false);
    assert!(old_controlled, "old RS controller flag should stay true");
}

#[tokio::test]
async fn test_recreate_strategy_does_not_scale_new_rs_until_old_rs_zero() {
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);
    let deploy_uid = "deploy-uid-recreate";

    let mut deploy =
        make_deployment_with_image("web-recreate", "default", deploy_uid, 2, "0", "nginx:1.14");
    deploy["spec"]["strategy"] = json!({"type": "Recreate"});
    let created = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "web-recreate",
            deploy,
        )
        .await
        .unwrap();

    let deploy_with_rv =
        crate::api::inject_resource_version(created.data, created.resource_version);
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

    // Trigger a template change to force a new ReplicaSet.
    let current_deploy = db
        .get_resource("apps/v1", "Deployment", Some("default"), "web-recreate")
        .await
        .unwrap()
        .unwrap();
    let mut updated_deploy =
        make_deployment_with_image("web-recreate", "default", deploy_uid, 2, "0", "nginx:1.16");
    updated_deploy["spec"]["strategy"] = json!({"type": "Recreate"});
    let updated = db
        .update_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "web-recreate",
            updated_deploy,
            current_deploy.resource_version,
        )
        .await
        .unwrap();

    let deploy_with_rv =
        crate::api::inject_resource_version(updated.data, updated.resource_version);
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

    let rs_list = db
        .list_resources(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(rs_list.items.len(), 2, "must have old and new ReplicaSets");

    let mut old_replicas = None;
    let mut new_replicas = None;
    for rs in &rs_list.items {
        let image = rs.data["spec"]["template"]["spec"]["containers"][0]["image"]
            .as_str()
            .unwrap_or("");
        let replicas = rs.data["spec"]["replicas"].as_i64().unwrap_or(-1);
        if image == "nginx:1.14" {
            old_replicas = Some(replicas);
        } else if image == "nginx:1.16" {
            new_replicas = Some(replicas);
        }
    }

    assert_eq!(old_replicas, Some(0), "old RS must be scaled to zero first");
    assert_eq!(
        new_replicas,
        Some(0),
        "new RS must remain at zero during Recreate handoff"
    );
}
