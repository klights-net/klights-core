use super::*;
use crate::datastore::{Resource, ResourceList};
use crate::kubelet::pod_repository::{PodObjectWriter, PodReader};
use anyhow::Result;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

// The "no `for _iteration in 0..20`" rollout completion loop invariant
// is enforced by `scripts/check_controllers_invariants.sh`,
// run as part of `./build.sh`.

struct CountingPodWriter {
    db: crate::datastore::sqlite::Datastore,
    creates: AtomicUsize,
}

struct ReplicaSetStatusRacingPodReader {
    db: crate::datastore::sqlite::Datastore,
    namespace: String,
    replica_set_name: String,
    replica_set_uid: String,
    bumped: AtomicBool,
}

#[async_trait::async_trait]
impl PodReader for ReplicaSetStatusRacingPodReader {
    async fn get_pod(&self, ns: &str, name: &str) -> Result<Option<Resource>> {
        self.db.get_resource("v1", "Pod", Some(ns), name).await
    }

    async fn get_pod_for_uid(&self, ns: &str, name: &str, uid: &str) -> Result<Option<Resource>> {
        Ok(self
            .db
            .get_resource("v1", "Pod", Some(ns), name)
            .await?
            .filter(|pod| pod.uid == uid))
    }

    async fn list_pods(
        &self,
        ns: Option<&str>,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        limit: Option<i64>,
        continue_token: Option<&str>,
    ) -> Result<ResourceList> {
        self.db
            .list_resources(
                "v1",
                "Pod",
                ns,
                crate::datastore::ResourceListQuery::new(
                    label_selector,
                    field_selector,
                    limit,
                    continue_token,
                ),
            )
            .await
    }

    async fn list_pods_by_owner_uid(&self, ns: &str, owner_uid: &str) -> Result<Vec<Resource>> {
        if ns == self.namespace
            && owner_uid == self.replica_set_uid
            && !self.bumped.swap(true, Ordering::SeqCst)
        {
            let rs = self
                .db
                .get_resource("apps/v1", "ReplicaSet", Some(ns), &self.replica_set_name)
                .await?
                .expect("racing ReplicaSet should exist");
            let rs_with_rv = crate::api::inject_resource_version(rs.data, rs.resource_version);
            crate::controllers::common::write_status(
                &self.db,
                &rs_with_rv,
                &json!({
                    "replicas": 1,
                    "readyReplicas": 1,
                    "availableReplicas": 1,
                    "fullyLabeledReplicas": 1,
                    "observedGeneration": 1,
                    "conditions": [{
                        "type": "ReplicaSetReplicaFailure",
                        "status": "False",
                        "reason": "RaceBump",
                        "message": "test-only status write racing Deployment scale"
                    }]
                }),
            )
            .await?;
        }

        let pods = self
            .db
            .list_resources(
                "v1",
                "Pod",
                Some(ns),
                crate::datastore::ResourceListQuery::all(),
            )
            .await?;
        Ok(pods
            .items
            .into_iter()
            .filter(|pod| {
                pod.data
                    .pointer("/metadata/ownerReferences")
                    .and_then(|v| v.as_array())
                    .is_some_and(|owners| {
                        owners.iter().any(|owner| {
                            owner.get("uid").and_then(|v| v.as_str()) == Some(owner_uid)
                        })
                    })
            })
            .collect())
    }
}

#[async_trait::async_trait]
impl PodObjectWriter for CountingPodWriter {
    async fn create_controller_pod(
        &self,
        ns: &str,
        name: &str,
        _node_name: &str,
        pod: serde_json::Value,
    ) -> Result<Resource> {
        self.creates.fetch_add(1, Ordering::SeqCst);
        self.db
            .create_resource("v1", "Pod", Some(ns), name, pod)
            .await
    }

    async fn delete_pod(&self, ns: &str, name: &str) -> Result<()> {
        self.db.delete_resource("v1", "Pod", Some(ns), name).await
    }

    async fn update_pod_owner_references(
        &self,
        ns: &str,
        name: &str,
        owner_refs: Vec<serde_json::Value>,
    ) -> Result<Resource> {
        let current = self
            .db
            .get_resource("v1", "Pod", Some(ns), name)
            .await?
            .expect("Pod should exist");
        let mut pod: serde_json::Value = (*current.data).clone();
        pod["metadata"]["ownerReferences"] = serde_json::Value::Array(owner_refs);
        self.db
            .update_resource("v1", "Pod", Some(ns), name, pod, current.resource_version)
            .await
    }

    async fn merge_pod_labels(
        &self,
        ns: &str,
        name: &str,
        labels: Vec<(String, String)>,
    ) -> Result<Resource> {
        let current = self
            .db
            .get_resource("v1", "Pod", Some(ns), name)
            .await?
            .expect("Pod should exist");
        let mut pod: serde_json::Value = (*current.data).clone();
        let label_map = pod["metadata"]
            .as_object_mut()
            .unwrap()
            .entry("labels".to_string())
            .or_insert_with(|| json!({}))
            .as_object_mut()
            .unwrap();
        for (key, value) in labels {
            label_map.insert(key, json!(value));
        }
        self.db
            .update_resource("v1", "Pod", Some(ns), name, pod, current.resource_version)
            .await
    }
}

#[tokio::test]
async fn test_rolling_update_does_not_resurge_new_rs_after_old_rs_reaches_zero() {
    let db = crate::datastore::test_support::in_memory().await;
    let pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);
    let pod_writer = CountingPodWriter {
        db: db.clone(),
        creates: AtomicUsize::new(0),
    };
    let deploy_uid = "deploy-uid-cleanup-zero-old";
    let new_rs_uid = "new-rs-cleanup-zero-old";

    let deployment = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "cleanup",
            "namespace": "default",
            "uid": deploy_uid,
            "resourceVersion": "0"
        },
        "spec": {
            "replicas": 1,
            "revisionHistoryLimit": 0,
            "selector": {"matchLabels": {"app": "cleanup"}},
            "strategy": {
                "type": "RollingUpdate",
                "rollingUpdate": {
                    "maxSurge": "25%",
                    "maxUnavailable": "25%"
                }
            },
            "template": {
                "metadata": {"labels": {"app": "cleanup"}},
                "spec": {
                    "containers": [{
                        "name": "agnhost",
                        "image": "registry.k8s.io/e2e-test-images/agnhost:2.56"
                    }]
                }
            }
        }
    });
    let created_deploy = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "cleanup",
            deployment,
        )
        .await
        .unwrap();

    let old_rs = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {
            "name": "cleanup-old",
            "namespace": "default",
            "uid": "old-rs-cleanup-zero-old",
            "labels": {"app": "cleanup", "pod": "httpd"},
            "annotations": {"deployment.kubernetes.io/revision": "1"},
            "ownerReferences": [{
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "name": "cleanup",
                "uid": deploy_uid,
                "controller": true,
                "blockOwnerDeletion": true
            }]
        },
        "spec": {
            "replicas": 0,
            "selector": {"matchLabels": {"app": "cleanup", "pod": "httpd"}},
            "template": {
                "metadata": {"labels": {"app": "cleanup", "pod": "httpd"}},
                "spec": {"containers": [{"name": "httpd", "image": "httpd"}]}
            }
        }
    });
    db.create_resource(
        "apps/v1",
        "ReplicaSet",
        Some("default"),
        "cleanup-old",
        old_rs,
    )
    .await
    .unwrap();

    let new_template_hash = compute_pod_template_hash(&created_deploy.data["spec"]["template"]);
    let new_rs_name = format!("cleanup-{new_template_hash}");
    let new_rs = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {
            "name": new_rs_name,
            "namespace": "default",
            "uid": new_rs_uid,
            "labels": {"app": "cleanup", "pod-template-hash": new_template_hash},
            "annotations": {"deployment.kubernetes.io/revision": "2"},
            "ownerReferences": [{
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "name": "cleanup",
                "uid": deploy_uid,
                "controller": true,
                "blockOwnerDeletion": true
            }]
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": "cleanup", "pod-template-hash": new_template_hash}},
            "template": {
                "metadata": {"labels": {"app": "cleanup", "pod-template-hash": new_template_hash}},
                "spec": {
                    "containers": [{
                        "name": "agnhost",
                        "image": "registry.k8s.io/e2e-test-images/agnhost:2.56"
                    }]
                }
            }
        }
    });
    db.create_resource(
        "apps/v1",
        "ReplicaSet",
        Some("default"),
        &new_rs_name,
        new_rs,
    )
    .await
    .unwrap();

    let existing_pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "cleanup-existing",
            "namespace": "default",
            "labels": {"app": "cleanup", "pod-template-hash": new_template_hash},
            "ownerReferences": [{
                "apiVersion": "apps/v1",
                "kind": "ReplicaSet",
                "name": new_rs_name,
                "uid": new_rs_uid,
                "controller": true,
                "blockOwnerDeletion": true
            }]
        },
        "spec": {
            "containers": [{
                "name": "agnhost",
                "image": "registry.k8s.io/e2e-test-images/agnhost:2.56"
            }]
        },
        "status": {
            "phase": "Running",
            "conditions": [
                {"type": "Ready", "status": "True"},
                {"type": "ContainersReady", "status": "True"}
            ]
        }
    });
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "cleanup-existing",
        existing_pod,
    )
    .await
    .unwrap();

    let deployment_with_rv =
        crate::api::inject_resource_version(created_deploy.data, created_deploy.resource_version);
    reconcile_deployment(
        &db,
        pod_repo.as_ref(),
        &pod_writer,
        pod_repo.as_ref(),
        &deployment_with_rv,
        "test-node",
    )
    .await
    .unwrap();

    assert_eq!(
        pod_writer.creates.load(Ordering::SeqCst),
        0,
        "Deployment cleanup reconcile must not create a transient surge pod once old RS replicas are zero"
    );

    let live_new_rs = db
        .get_resource("apps/v1", "ReplicaSet", Some("default"), &new_rs_name)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(live_new_rs.data["spec"]["replicas"], json!(1));

    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::new(Some("app=cleanup"), None, None, None),
        )
        .await
        .unwrap();
    assert_eq!(pods.items.len(), 1);
}

#[tokio::test]
async fn test_rolling_update_scales_new_rs_after_simultaneous_template_update_and_scale_up() {
    let db = crate::datastore::test_support::in_memory().await;
    let pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);
    let deploy_uid = "deploy-uid-lifecycle-scale-up";
    let old_rs_uid = "old-rs-lifecycle-scale-up";
    let new_rs_uid = "new-rs-lifecycle-scale-up";

    let deployment = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "lifecycle",
            "namespace": "default",
            "uid": deploy_uid,
            "resourceVersion": "0"
        },
        "spec": {
            "replicas": 2,
            "selector": {"matchLabels": {"app": "lifecycle"}},
            "template": {
                "metadata": {"labels": {"app": "lifecycle"}},
                "spec": {
                    "containers": [{
                        "name": "app",
                        "image": "registry.k8s.io/e2e-test-images/agnhost:2.56"
                    }]
                }
            }
        }
    });
    let created_deploy = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "lifecycle",
            deployment,
        )
        .await
        .unwrap();
    let owner_ref = json!([{
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "name": "lifecycle",
        "uid": deploy_uid,
        "controller": true,
        "blockOwnerDeletion": true
    }]);

    let old_template = json!({
        "metadata": {"labels": {"app": "lifecycle"}},
        "spec": {
            "containers": [{
                "name": "app",
                "image": "registry.k8s.io/pause:3.10"
            }]
        }
    });
    let old_hash = compute_pod_template_hash(&old_template);
    let old_rs_name = format!("lifecycle-{old_hash}");
    let old_rs = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {
            "name": old_rs_name,
            "namespace": "default",
            "uid": old_rs_uid,
            "labels": {"app": "lifecycle", "pod-template-hash": old_hash},
            "annotations": {
                "deployment.kubernetes.io/revision": "2",
                "deployment.kubernetes.io/desired-replicas": "1",
                "deployment.kubernetes.io/max-replicas": "2"
            },
            "ownerReferences": owner_ref
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": "lifecycle", "pod-template-hash": old_hash}},
            "template": {
                "metadata": {"labels": {"app": "lifecycle", "pod-template-hash": old_hash}},
                "spec": old_template["spec"].clone()
            }
        },
        "status": {
            "replicas": 1,
            "readyReplicas": 1,
            "availableReplicas": 1,
            "observedGeneration": 1
        }
    });
    db.create_resource(
        "apps/v1",
        "ReplicaSet",
        Some("default"),
        &old_rs_name,
        old_rs,
    )
    .await
    .unwrap();

    let new_hash = compute_pod_template_hash(&created_deploy.data["spec"]["template"]);
    let new_rs_name = format!("lifecycle-{new_hash}");
    let new_rs = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {
            "name": new_rs_name,
            "namespace": "default",
            "uid": new_rs_uid,
            "labels": {"app": "lifecycle", "pod-template-hash": new_hash},
            "annotations": {
                "deployment.kubernetes.io/revision": "3",
                "deployment.kubernetes.io/desired-replicas": "2",
                "deployment.kubernetes.io/max-replicas": "3"
            },
            "ownerReferences": [{
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "name": "lifecycle",
                "uid": deploy_uid,
                "controller": true,
                "blockOwnerDeletion": true
            }]
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": "lifecycle", "pod-template-hash": new_hash}},
            "template": {
                "metadata": {"labels": {"app": "lifecycle", "pod-template-hash": new_hash}},
                "spec": created_deploy.data["spec"]["template"]["spec"].clone()
            }
        },
        "status": {
            "replicas": 1,
            "readyReplicas": 1,
            "availableReplicas": 1,
            "observedGeneration": 1
        }
    });
    db.create_resource(
        "apps/v1",
        "ReplicaSet",
        Some("default"),
        &new_rs_name,
        new_rs,
    )
    .await
    .unwrap();

    for (pod_name, rs_name, rs_uid, hash) in [
        (
            "lifecycle-old-ready",
            old_rs_name.as_str(),
            old_rs_uid,
            old_hash.as_str(),
        ),
        (
            "lifecycle-new-ready",
            new_rs_name.as_str(),
            new_rs_uid,
            new_hash.as_str(),
        ),
    ] {
        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": pod_name,
                "namespace": "default",
                "uid": format!("{pod_name}-uid"),
                "labels": {"app": "lifecycle", "pod-template-hash": hash},
                "ownerReferences": [{
                    "apiVersion": "apps/v1",
                    "kind": "ReplicaSet",
                    "name": rs_name,
                    "uid": rs_uid,
                    "controller": true,
                    "blockOwnerDeletion": true
                }]
            },
            "spec": {"containers": [{"name": "app", "image": "unused"}]},
            "status": {
                "phase": "Running",
                "conditions": [
                    {"type": "Ready", "status": "True"},
                    {"type": "ContainersReady", "status": "True"}
                ]
            }
        });
        db.create_resource("v1", "Pod", Some("default"), pod_name, pod)
            .await
            .unwrap();
    }

    let deployment_with_rv =
        crate::api::inject_resource_version(created_deploy.data, created_deploy.resource_version);
    reconcile_deployment(
        &db,
        pod_repo.as_ref(),
        pod_repo.as_ref(),
        pod_repo.as_ref(),
        &deployment_with_rv,
        "test-node",
    )
    .await
    .unwrap();

    let live_new_rs = db
        .get_resource("apps/v1", "ReplicaSet", Some("default"), &new_rs_name)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        live_new_rs.data["spec"]["replicas"],
        json!(2),
        "normal rolling-update progress must scale the current-template ReplicaSet before old ReplicaSets"
    );
}

#[tokio::test]
async fn test_rolling_update_uses_live_new_rs_pod_readiness_for_old_rs_scale_down() {
    let db = crate::datastore::test_support::in_memory().await;
    let pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);
    let deploy_uid = "deploy-uid-live-new-ready";
    let first_old_rs_uid = "first-old-rs-live-new-ready";
    let second_old_rs_uid = "second-old-rs-live-new-ready";
    let new_rs_uid = "new-rs-live-new-ready";

    let deployment = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "lifecycle",
            "namespace": "default",
            "uid": deploy_uid,
            "resourceVersion": "0"
        },
        "spec": {
            "replicas": 2,
            "selector": {"matchLabels": {"app": "lifecycle"}},
            "strategy": {
                "type": "RollingUpdate",
                "rollingUpdate": {
                    "maxSurge": 1,
                    "maxUnavailable": 0
                }
            },
            "template": {
                "metadata": {"labels": {"app": "lifecycle"}},
                "spec": {
                    "containers": [{
                        "name": "app",
                        "image": "registry.k8s.io/e2e-test-images/httpd:2.4.38-4"
                    }]
                }
            }
        }
    });
    let created_deploy = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "lifecycle",
            deployment,
        )
        .await
        .unwrap();
    let owner_ref = json!([{
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "name": "lifecycle",
        "uid": deploy_uid,
        "controller": true,
        "blockOwnerDeletion": true
    }]);

    for (rs_name, rs_uid, hash, image, revision) in [
        (
            "lifecycle-old-one",
            first_old_rs_uid,
            "oldonehash",
            "registry.k8s.io/e2e-test-images/agnhost:2.56",
            "1",
        ),
        (
            "lifecycle-old-two",
            second_old_rs_uid,
            "oldtwohash",
            "registry.k8s.io/pause:3.10.1",
            "2",
        ),
    ] {
        let old_rs = json!({
            "apiVersion": "apps/v1",
            "kind": "ReplicaSet",
            "metadata": {
                "name": rs_name,
                "namespace": "default",
                "uid": rs_uid,
                "labels": {"app": "lifecycle", "pod-template-hash": hash},
                "annotations": {
                    "deployment.kubernetes.io/revision": revision,
                    "deployment.kubernetes.io/desired-replicas": "1",
                    "deployment.kubernetes.io/max-replicas": "2"
                },
                "ownerReferences": owner_ref
            },
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": {"app": "lifecycle", "pod-template-hash": hash}},
                "template": {
                    "metadata": {"labels": {"app": "lifecycle", "pod-template-hash": hash}},
                    "spec": {"containers": [{"name": "app", "image": image}]}
                }
            },
            "status": {
                "replicas": 1,
                "readyReplicas": 1,
                "availableReplicas": 1,
                "observedGeneration": 1
            }
        });
        db.create_resource("apps/v1", "ReplicaSet", Some("default"), rs_name, old_rs)
            .await
            .unwrap();

        let pod_name = format!("{rs_name}-ready");
        let old_pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": pod_name,
                "namespace": "default",
                "uid": format!("{pod_name}-uid"),
                "labels": {"app": "lifecycle", "pod-template-hash": hash},
                "ownerReferences": [{
                    "apiVersion": "apps/v1",
                    "kind": "ReplicaSet",
                    "name": rs_name,
                    "uid": rs_uid,
                    "controller": true,
                    "blockOwnerDeletion": true
                }]
            },
            "spec": {"containers": [{"name": "app", "image": image}]},
            "status": {
                "phase": "Running",
                "conditions": [
                    {"type": "Ready", "status": "True"},
                    {"type": "ContainersReady", "status": "True"}
                ]
            }
        });
        db.create_resource("v1", "Pod", Some("default"), &pod_name, old_pod)
            .await
            .unwrap();
    }

    let new_hash = compute_pod_template_hash(&created_deploy.data["spec"]["template"]);
    let new_rs_name = format!("lifecycle-{new_hash}");
    let new_rs = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {
            "name": new_rs_name,
            "namespace": "default",
            "uid": new_rs_uid,
            "labels": {"app": "lifecycle", "pod-template-hash": new_hash},
            "annotations": {
                "deployment.kubernetes.io/revision": "3",
                "deployment.kubernetes.io/desired-replicas": "2",
                "deployment.kubernetes.io/max-replicas": "3"
            },
            "ownerReferences": [{
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "name": "lifecycle",
                "uid": deploy_uid,
                "controller": true,
                "blockOwnerDeletion": true
            }]
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": "lifecycle", "pod-template-hash": new_hash}},
            "template": {
                "metadata": {"labels": {"app": "lifecycle", "pod-template-hash": new_hash}},
                "spec": created_deploy.data["spec"]["template"]["spec"].clone()
            }
        },
        "status": {
            "replicas": 1,
            "readyReplicas": 0,
            "availableReplicas": 0,
            "observedGeneration": 1
        }
    });
    db.create_resource(
        "apps/v1",
        "ReplicaSet",
        Some("default"),
        &new_rs_name,
        new_rs,
    )
    .await
    .unwrap();

    let new_pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "lifecycle-new-ready",
            "namespace": "default",
            "uid": "lifecycle-new-ready-uid",
            "labels": {"app": "lifecycle", "pod-template-hash": new_hash},
            "ownerReferences": [{
                "apiVersion": "apps/v1",
                "kind": "ReplicaSet",
                "name": new_rs_name,
                "uid": new_rs_uid,
                "controller": true,
                "blockOwnerDeletion": true
            }]
        },
        "spec": {"containers": [{"name": "app", "image": "registry.k8s.io/e2e-test-images/httpd:2.4.38-4"}]},
        "status": {
            "phase": "Running",
            "conditions": [
                {"type": "Ready", "status": "True"},
                {"type": "ContainersReady", "status": "True"}
            ]
        }
    });
    db.create_resource("v1", "Pod", Some("default"), "lifecycle-new-ready", new_pod)
        .await
        .unwrap();

    let deployment_with_rv =
        crate::api::inject_resource_version(created_deploy.data, created_deploy.resource_version);
    reconcile_deployment(
        &db,
        pod_repo.as_ref(),
        pod_repo.as_ref(),
        pod_repo.as_ref(),
        &deployment_with_rv,
        "test-node",
    )
    .await
    .unwrap();

    let first_old_rs = db
        .get_resource(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            "lifecycle-old-one",
        )
        .await
        .unwrap()
        .unwrap();
    let second_old_rs = db
        .get_resource(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            "lifecycle-old-two",
        )
        .await
        .unwrap()
        .unwrap();
    let old_total = first_old_rs.data["spec"]["replicas"].as_i64().unwrap()
        + second_old_rs.data["spec"]["replicas"].as_i64().unwrap();
    assert_eq!(
        old_total, 1,
        "live Ready pods in the new ReplicaSet should permit one old ReplicaSet replica to scale down even when new RS status lags"
    );
}

#[tokio::test]
async fn test_rollover_scales_unavailable_old_rs_before_available_old_rs() {
    let db = crate::datastore::test_support::in_memory().await;
    let pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);
    let deploy_uid = "deploy-uid-rollover-preserve-availability";
    let bad_rs_uid = "bad-rs-rollover-preserve-availability";
    let healthy_rs_uid = "healthy-rs-rollover-preserve-availability";
    let new_rs_uid = "new-rs-rollover-preserve-availability";

    let deployment = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "rollover",
            "namespace": "default",
            "uid": deploy_uid,
            "resourceVersion": "0"
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"name": "rollover-pod"}},
            "strategy": {
                "type": "RollingUpdate",
                "rollingUpdate": {
                    "maxSurge": 1,
                    "maxUnavailable": 0
                }
            },
            "template": {
                "metadata": {"labels": {"name": "rollover-pod"}},
                "spec": {
                    "containers": [{
                        "name": "agnhost",
                        "image": "registry.k8s.io/e2e-test-images/agnhost:2.56"
                    }]
                }
            }
        }
    });
    let created_deploy = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "rollover",
            deployment,
        )
        .await
        .unwrap();

    let owner_ref = json!([{
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "name": "rollover",
        "uid": deploy_uid,
        "controller": true,
        "blockOwnerDeletion": true
    }]);

    let bad_rs = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {
            "name": "rollover-a-bad",
            "namespace": "default",
            "uid": bad_rs_uid,
            "labels": {"name": "rollover-pod", "pod-template-hash": "bad"},
            "annotations": {"deployment.kubernetes.io/revision": "1"},
            "ownerReferences": owner_ref
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"name": "rollover-pod", "pod-template-hash": "bad"}},
            "template": {
                "metadata": {"labels": {"name": "rollover-pod", "pod-template-hash": "bad"}},
                "spec": {"containers": [{"name": "redis-slave", "image": "gcr.io/google_samples/gb-redisslave:nonexistent"}]}
            }
        },
        "status": {
            "replicas": 1,
            "readyReplicas": 0,
            "availableReplicas": 0
        }
    });
    db.create_resource(
        "apps/v1",
        "ReplicaSet",
        Some("default"),
        "rollover-a-bad",
        bad_rs,
    )
    .await
    .unwrap();

    let healthy_rs = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {
            "name": "rollover-z-healthy",
            "namespace": "default",
            "uid": healthy_rs_uid,
            "labels": {"name": "rollover-pod", "pod": "httpd"},
            "annotations": {"deployment.kubernetes.io/revision": "0"},
            "ownerReferences": owner_ref
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"name": "rollover-pod", "pod": "httpd"}},
            "template": {
                "metadata": {"labels": {"name": "rollover-pod", "pod": "httpd"}},
                "spec": {"containers": [{"name": "httpd", "image": "httpd"}]}
            }
        },
        "status": {
            "replicas": 1,
            "readyReplicas": 0,
            "availableReplicas": 0
        }
    });
    db.create_resource(
        "apps/v1",
        "ReplicaSet",
        Some("default"),
        "rollover-z-healthy",
        healthy_rs,
    )
    .await
    .unwrap();

    let new_template_hash = compute_pod_template_hash(&created_deploy.data["spec"]["template"]);
    let new_rs_name = format!("rollover-{new_template_hash}");
    let new_rs = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {
            "name": new_rs_name,
            "namespace": "default",
            "uid": new_rs_uid,
            "labels": {"name": "rollover-pod", "pod-template-hash": new_template_hash},
            "annotations": {"deployment.kubernetes.io/revision": "2"},
            "ownerReferences": owner_ref
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"name": "rollover-pod", "pod-template-hash": new_template_hash}},
            "template": {
                "metadata": {"labels": {"name": "rollover-pod", "pod-template-hash": new_template_hash}},
                "spec": {"containers": [{"name": "agnhost", "image": "registry.k8s.io/e2e-test-images/agnhost:2.56"}]}
            }
        },
        "status": {
            "replicas": 1,
            "readyReplicas": 0,
            "availableReplicas": 0
        }
    });
    db.create_resource(
        "apps/v1",
        "ReplicaSet",
        Some("default"),
        &new_rs_name,
        new_rs,
    )
    .await
    .unwrap();

    let healthy_pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "rollover-healthy-pod",
            "namespace": "default",
            "uid": "rollover-healthy-pod-uid",
            "labels": {"name": "rollover-pod", "pod": "httpd"},
            "ownerReferences": [{
                "apiVersion": "apps/v1",
                "kind": "ReplicaSet",
                "name": "rollover-z-healthy",
                "uid": healthy_rs_uid,
                "controller": true,
                "blockOwnerDeletion": true
            }]
        },
        "spec": {"containers": [{"name": "httpd", "image": "httpd"}]},
        "status": {
            "phase": "Running",
            "conditions": [{"type": "Ready", "status": "True"}]
        }
    });
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "rollover-healthy-pod",
        healthy_pod,
    )
    .await
    .unwrap();

    let bad_pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "rollover-bad-pod",
            "namespace": "default",
            "uid": "rollover-bad-pod-uid",
            "labels": {"name": "rollover-pod", "pod-template-hash": "bad"},
            "ownerReferences": [{
                "apiVersion": "apps/v1",
                "kind": "ReplicaSet",
                "name": "rollover-a-bad",
                "uid": bad_rs_uid,
                "controller": true,
                "blockOwnerDeletion": true
            }]
        },
        "spec": {"containers": [{"name": "redis-slave", "image": "gcr.io/google_samples/gb-redisslave:nonexistent"}]},
        "status": {
            "phase": "Pending",
            "conditions": [{"type": "Ready", "status": "False"}]
        }
    });
    db.create_resource("v1", "Pod", Some("default"), "rollover-bad-pod", bad_pod)
        .await
        .unwrap();

    let new_pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "rollover-new-pod",
            "namespace": "default",
            "uid": "rollover-new-pod-uid",
            "labels": {"name": "rollover-pod", "pod-template-hash": new_template_hash},
            "ownerReferences": [{
                "apiVersion": "apps/v1",
                "kind": "ReplicaSet",
                "name": new_rs_name,
                "uid": new_rs_uid,
                "controller": true,
                "blockOwnerDeletion": true
            }]
        },
        "spec": {"containers": [{"name": "agnhost", "image": "registry.k8s.io/e2e-test-images/agnhost:2.56"}]},
        "status": {
            "phase": "Pending",
            "conditions": [{"type": "Ready", "status": "False"}]
        }
    });
    db.create_resource("v1", "Pod", Some("default"), "rollover-new-pod", new_pod)
        .await
        .unwrap();

    let deployment_with_rv =
        crate::api::inject_resource_version(created_deploy.data, created_deploy.resource_version);
    reconcile_deployment(
        &db,
        pod_repo.as_ref(),
        pod_repo.as_ref(),
        pod_repo.as_ref(),
        &deployment_with_rv,
        "test-node",
    )
    .await
    .unwrap();

    let live_bad_rs = db
        .get_resource("apps/v1", "ReplicaSet", Some("default"), "rollover-a-bad")
        .await
        .unwrap()
        .unwrap();
    let live_healthy_rs = db
        .get_resource(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            "rollover-z-healthy",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        live_bad_rs.data["spec"]["replicas"],
        json!(0),
        "rollover should consume scale-down budget on unavailable old ReplicaSets first"
    );
    assert_eq!(
        live_healthy_rs.data["spec"]["replicas"],
        json!(1),
        "rollover must preserve the only available old ReplicaSet while the new ReplicaSet is unavailable"
    );
}

#[tokio::test]
async fn test_rollover_adoption_redrives_zero_replica_old_rs_pod_delete() {
    let db = crate::datastore::test_support::in_memory().await;
    let pod_repo =
        crate::controllers::test_utils::deferred_outbox_pod_repository_for_test(&db).await;
    let deploy_uid = "deploy-uid-adopted-rollover";
    let old_rs_uid = "old-rs-uid-adopted-rollover";

    let deployment = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "test-rolling-update-deployment",
            "namespace": "default",
            "uid": deploy_uid,
            "resourceVersion": "0"
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"name": "sample-pod"}},
            "strategy": {
                "type": "RollingUpdate",
                "rollingUpdate": {
                    "maxSurge": 1,
                    "maxUnavailable": 0
                }
            },
            "template": {
                "metadata": {"labels": {"name": "sample-pod"}},
                "spec": {
                    "containers": [{
                        "name": "agnhost",
                        "image": "registry.k8s.io/e2e-test-images/agnhost:2.56"
                    }]
                }
            }
        }
    });
    let created_deploy = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "test-rolling-update-deployment",
            deployment,
        )
        .await
        .unwrap();

    let old_rs = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {
            "name": "test-rolling-update-controller",
            "namespace": "default",
            "uid": old_rs_uid,
            "labels": {"name": "sample-pod", "pod": "httpd"},
            "annotations": {"deployment.kubernetes.io/revision": "1"}
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"name": "sample-pod", "pod": "httpd"}},
            "template": {
                "metadata": {"labels": {"name": "sample-pod", "pod": "httpd"}},
                "spec": {
                    "containers": [{
                        "name": "httpd",
                        "image": "registry.k8s.io/e2e-test-images/httpd:2.4.38-4"
                    }]
                }
            }
        },
        "status": {
            "replicas": 1,
            "readyReplicas": 1,
            "availableReplicas": 1,
            "observedGeneration": 1
        }
    });
    db.create_resource(
        "apps/v1",
        "ReplicaSet",
        Some("default"),
        "test-rolling-update-controller",
        old_rs,
    )
    .await
    .unwrap();

    let old_pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "test-rolling-update-controller-130dc",
            "namespace": "default",
            "uid": "old-pod-uid-adopted-rollover",
            "labels": {"name": "sample-pod", "pod": "httpd"},
            "ownerReferences": [{
                "apiVersion": "apps/v1",
                "kind": "ReplicaSet",
                "name": "test-rolling-update-controller",
                "uid": old_rs_uid,
                "controller": true,
                "blockOwnerDeletion": true
            }]
        },
        "spec": {"containers": [{"name": "httpd", "image": "registry.k8s.io/e2e-test-images/httpd:2.4.38-4"}]},
        "status": {
            "phase": "Running",
            "conditions": [
                {"type": "Ready", "status": "True"},
                {"type": "ContainersReady", "status": "True"}
            ]
        }
    });
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "test-rolling-update-controller-130dc",
        old_pod,
    )
    .await
    .unwrap();

    let deployment_with_rv =
        crate::api::inject_resource_version(created_deploy.data, created_deploy.resource_version);
    reconcile_deployment(
        &db,
        pod_repo.as_ref(),
        pod_repo.as_ref(),
        pod_repo.as_ref(),
        &deployment_with_rv,
        "test-node",
    )
    .await
    .unwrap();

    let created_pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::new(Some("name=sample-pod"), None, None, None),
        )
        .await
        .unwrap();
    let new_pod = created_pods
        .items
        .iter()
        .find(|pod| pod.uid != "old-pod-uid-adopted-rollover")
        .expect("first rollout reconcile must create a new ReplicaSet pod");
    let mut ready_new_pod = (*new_pod.data).clone();
    ready_new_pod["status"] = json!({
        "phase": "Running",
        "conditions": [
            {"type": "Ready", "status": "True"},
            {"type": "ContainersReady", "status": "True"}
        ]
    });
    db.update_resource(
        "v1",
        "Pod",
        Some("default"),
        &new_pod.name,
        ready_new_pod,
        new_pod.resource_version,
    )
    .await
    .unwrap();

    let current_deployment = db
        .get_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "test-rolling-update-deployment",
        )
        .await
        .unwrap()
        .unwrap();
    let current_deployment_with_rv = crate::api::inject_resource_version(
        current_deployment.data,
        current_deployment.resource_version,
    );
    reconcile_deployment(
        &db,
        pod_repo.as_ref(),
        pod_repo.as_ref(),
        pod_repo.as_ref(),
        &current_deployment_with_rv,
        "test-node",
    )
    .await
    .unwrap();

    let live_old_rs = db
        .get_resource(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            "test-rolling-update-controller",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        live_old_rs.data["spec"]["replicas"],
        json!(0),
        "Deployment must scale the adopted old ReplicaSet down during rollout"
    );

    let old_pod_after = db
        .get_resource(
            "v1",
            "Pod",
            Some("default"),
            "test-rolling-update-controller-130dc",
        )
        .await
        .unwrap()
        .unwrap();
    assert!(
        old_pod_after
            .data
            .pointer("/metadata/deletionTimestamp")
            .is_some(),
        "adopted old ReplicaSet pod must be marked terminating through the PodRepository actor-owned delete path"
    );

    let deployment_after = db
        .get_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "test-rolling-update-deployment",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        deployment_after.data["status"]["updatedReplicas"],
        json!(1),
        "rollout status must be able to reach completion after the old pod is terminating"
    );
}

#[tokio::test]
async fn test_rolling_update_initial_step_keeps_min_available_on_old_rs() {
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);
    let deploy_uid = "deploy-uid-partial-rollout";

    let deployment = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "web",
            "namespace": "default",
            "uid": deploy_uid,
            "resourceVersion": "0"
        },
        "spec": {
            "replicas": 10,
            "selector": {"matchLabels": {"app": "web"}},
            "strategy": {
                "type": "RollingUpdate",
                "rollingUpdate": {
                    "maxSurge": 3,
                    "maxUnavailable": 2
                }
            },
            "template": {
                "metadata": {"labels": {"app": "web"}},
                "spec": {
                    "containers": [{
                        "name": "webserver",
                        "image": "nginx:1.14"
                    }]
                }
            }
        }
    });

    let created = db
        .create_resource("apps/v1", "Deployment", Some("default"), "web", deployment)
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

    let current_deploy = db
        .get_resource("apps/v1", "Deployment", Some("default"), "web")
        .await
        .unwrap()
        .unwrap();

    let rollout_with_bad_image = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "web",
            "namespace": "default",
            "uid": deploy_uid,
            "resourceVersion": "0"
        },
        "spec": {
            "replicas": 10,
            "selector": {"matchLabels": {"app": "web"}},
            "strategy": {
                "type": "RollingUpdate",
                "rollingUpdate": {
                    "maxSurge": 3,
                    "maxUnavailable": 2
                }
            },
            "template": {
                "metadata": {"labels": {"app": "web"}},
                "spec": {
                    "containers": [{
                        "name": "webserver",
                        "image": "webserver:404"
                    }]
                }
            }
        }
    });

    let updated = db
        .update_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "web",
            rollout_with_bad_image,
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
    assert_eq!(rs_list.items.len(), 2, "rollout should have old and new RS");

    let old_rs = rs_list
        .items
        .iter()
        .find(|rs| rs.data["spec"]["template"]["spec"]["containers"][0]["image"] == "nginx:1.14")
        .expect("old ReplicaSet should exist");
    let new_rs = rs_list
        .items
        .iter()
        .find(|rs| rs.data["spec"]["template"]["spec"]["containers"][0]["image"] == "webserver:404")
        .expect("new ReplicaSet should exist");

    // Old RS remains controlled during rolling update so its availability
    // continues to count while the new RS consumes only the surge budget.
    let old_owner_refs = old_rs
        .data
        .pointer("/metadata/ownerReferences")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let has_controller = old_owner_refs.iter().any(|r| {
        r.get("controller").and_then(|c| c.as_bool()) == Some(true)
            && r.get("uid").and_then(|u| u.as_str()) == Some(deploy_uid)
    });
    let owned_by_us = old_owner_refs
        .iter()
        .any(|r| r.get("uid").and_then(|u| u.as_str()) == Some(deploy_uid));
    assert!(
        owned_by_us,
        "old RS should retain ownerReference to deployment"
    );
    assert!(has_controller, "old RS controller flag should stay true");
    assert_eq!(
        old_rs.data["spec"]["replicas"], 8,
        "old RS scales down only to minAvailable"
    );
    assert_eq!(
        new_rs.data["spec"]["replicas"], 3,
        "new RS starts within maxSurge"
    );
}

#[tokio::test]
async fn test_proportional_scaling_keeps_unavailable_new_rs_within_surge_budget() {
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);
    let deploy_uid = "deploy-uid-proportional-scaling";

    let deployment = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "webserver-deployment",
            "namespace": "default",
            "uid": deploy_uid,
            "resourceVersion": "0"
        },
        "spec": {
            "replicas": 10,
            "selector": {"matchLabels": {"name": "httpd"}},
            "strategy": {
                "type": "RollingUpdate",
                "rollingUpdate": {
                    "maxSurge": 3,
                    "maxUnavailable": 2
                }
            },
            "template": {
                "metadata": {"labels": {"name": "httpd"}},
                "spec": {
                    "containers": [{
                        "name": "httpd",
                        "image": "registry.k8s.io/e2e-test-images/httpd:2.4.38-4"
                    }]
                }
            }
        }
    });

    let created = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "webserver-deployment",
            deployment,
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

    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    for pod in pods.items {
        let mut ready_pod: serde_json::Value = (*pod.data).clone();
        ready_pod["status"] = json!({
            "phase": "Running",
            "conditions": [{"type": "Ready", "status": "True"}]
        });
        db.update_resource(
            "v1",
            "Pod",
            Some("default"),
            &pod.name,
            ready_pod,
            pod.resource_version,
        )
        .await
        .unwrap();
    }

    let rs_after_ready = db
        .list_resources(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    for rs in rs_after_ready.items {
        let rs_with_rv = crate::api::inject_resource_version(rs.data, rs.resource_version);
        crate::controllers::replicaset::reconcile_replicaset(
            &db,
            __pod_repo.as_ref(),
            __pod_repo.as_ref(),
            __pod_repo.as_ref(),
            &rs_with_rv,
            "test-node",
        )
        .await
        .unwrap();
    }

    let current_deploy = db
        .get_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "webserver-deployment",
        )
        .await
        .unwrap()
        .unwrap();
    let mut updated_deploy: serde_json::Value = (*current_deploy.data).clone();
    updated_deploy["spec"]["template"]["spec"]["containers"][0]["image"] = json!("webserver:404");
    let updated = db
        .update_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "webserver-deployment",
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

    let current_deploy = db
        .get_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "webserver-deployment",
        )
        .await
        .unwrap()
        .unwrap();
    let deploy_with_rv =
        crate::api::inject_resource_version(current_deploy.data, current_deploy.resource_version);
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
    assert_eq!(rs_list.items.len(), 2, "rollout should have old and new RS");

    let old_rs = rs_list
        .items
        .iter()
        .find(|rs| {
            rs.data["spec"]["template"]["spec"]["containers"][0]["image"]
                == "registry.k8s.io/e2e-test-images/httpd:2.4.38-4"
        })
        .expect("old ReplicaSet should exist");
    let new_rs = rs_list
        .items
        .iter()
        .find(|rs| rs.data["spec"]["template"]["spec"]["containers"][0]["image"] == "webserver:404")
        .expect("new ReplicaSet should exist");

    // Old RS remains controlled and can be scaled down once enough available
    // replicas remain.
    let old_owner_refs = old_rs
        .data
        .pointer("/metadata/ownerReferences")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let old_controlled = old_owner_refs.iter().any(|r| {
        r.get("controller").and_then(|c| c.as_bool()) == Some(true)
            && r.get("uid").and_then(|u| u.as_str()) == Some(deploy_uid)
    });
    let old_owned = old_owner_refs
        .iter()
        .any(|r| r.get("uid").and_then(|u| u.as_str()) == Some(deploy_uid));
    assert!(
        old_owned,
        "old RS should retain ownerReference to deployment"
    );
    assert!(old_controlled, "old RS controller=true during rollout");
    assert_eq!(
        old_rs.data["spec"]["replicas"], 8,
        "old RS scales down only to minAvailable"
    );
    assert_eq!(
        new_rs.data["spec"]["replicas"], 5,
        "new RS uses remaining surge capacity while unavailable"
    );

    let current_deploy = db
        .get_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "webserver-deployment",
        )
        .await
        .unwrap()
        .unwrap();
    let mut scaled_deploy: serde_json::Value = (*current_deploy.data).clone();
    scaled_deploy["spec"]["replicas"] = json!(30);
    let scaled = db
        .update_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "webserver-deployment",
            scaled_deploy,
            current_deploy.resource_version,
        )
        .await
        .unwrap();
    let deploy_with_rv = crate::api::inject_resource_version(scaled.data, scaled.resource_version);
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
    let old_rs = rs_list
        .items
        .iter()
        .find(|rs| {
            rs.data["spec"]["template"]["spec"]["containers"][0]["image"]
                == "registry.k8s.io/e2e-test-images/httpd:2.4.38-4"
        })
        .expect("old ReplicaSet should exist after scale up");
    let new_rs = rs_list
        .items
        .iter()
        .find(|rs| rs.data["spec"]["template"]["spec"]["containers"][0]["image"] == "webserver:404")
        .expect("new ReplicaSet should exist after scale up");

    // Proportional scale-up applies across active controlled ReplicaSets.
    assert_eq!(
        old_rs.data["spec"]["replicas"], 20,
        "old RS receives its proportional share during scale-up"
    );
    assert_eq!(
        new_rs.data["spec"]["replicas"], 13,
        "new RS receives its proportional share during scale-up"
    );
}

#[tokio::test]
async fn test_proportional_scaling_is_idempotent_when_previous_attempt_partially_applied() {
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);
    let deploy_uid = "deploy-uid-proportional-partial";
    let old_rs_uid = "old-rs-uid-proportional-partial";
    let new_rs_uid = "new-rs-uid-proportional-partial";

    let deployment = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "webserver-deployment",
            "namespace": "default",
            "uid": deploy_uid,
            "resourceVersion": "0"
        },
        "spec": {
            "replicas": 30,
            "selector": {"matchLabels": {"name": "httpd"}},
            "strategy": {
                "type": "RollingUpdate",
                "rollingUpdate": {
                    "maxSurge": 3,
                    "maxUnavailable": 2
                }
            },
            "template": {
                "metadata": {"labels": {"name": "httpd"}},
                "spec": {
                    "containers": [{
                        "name": "httpd",
                        "image": "webserver:404"
                    }]
                }
            }
        },
        "status": {"replicas": 21}
    });
    let created_deploy = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "webserver-deployment",
            deployment,
        )
        .await
        .unwrap();

    let old_rs = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {
            "name": "webserver-deployment-old",
            "namespace": "default",
            "uid": old_rs_uid,
            "labels": {"name": "httpd", "pod-template-hash": "oldhash"},
            "annotations": {
                "deployment.kubernetes.io/revision": "1",
                "deployment.kubernetes.io/desired-replicas": "10",
                "deployment.kubernetes.io/max-replicas": "13"
            },
            "ownerReferences": [{
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "name": "webserver-deployment",
                "uid": deploy_uid,
                "controller": true,
                "blockOwnerDeletion": true
            }]
        },
        "spec": {
            "replicas": 8,
            "selector": {"matchLabels": {"name": "httpd", "pod-template-hash": "oldhash"}},
            "template": {
                "metadata": {"labels": {"name": "httpd", "pod-template-hash": "oldhash"}},
                "spec": {"containers": [{"name": "httpd", "image": "registry.k8s.io/e2e-test-images/httpd:2.4.38-4"}]}
            }
        },
        "status": {"replicas": 8, "readyReplicas": 8, "availableReplicas": 8}
    });
    db.create_resource(
        "apps/v1",
        "ReplicaSet",
        Some("default"),
        "webserver-deployment-old",
        old_rs,
    )
    .await
    .unwrap();

    for i in 0..8 {
        let pod_name = format!("webserver-deployment-old-{i}");
        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": pod_name,
                "namespace": "default",
                "uid": format!("old-pod-{i}"),
                "labels": {"name": "httpd", "pod-template-hash": "oldhash"},
                "ownerReferences": [{
                    "apiVersion": "apps/v1",
                    "kind": "ReplicaSet",
                    "name": "webserver-deployment-old",
                    "uid": old_rs_uid,
                    "controller": true,
                    "blockOwnerDeletion": true
                }]
            },
            "spec": {"containers": [{"name": "httpd", "image": "registry.k8s.io/e2e-test-images/httpd:2.4.38-4"}]},
            "status": {"phase": "Running", "conditions": [{"type": "Ready", "status": "True"}]}
        });
        db.create_resource("v1", "Pod", Some("default"), &pod_name, pod)
            .await
            .unwrap();
    }

    let new_hash = compute_pod_template_hash(&created_deploy.data["spec"]["template"]);
    let new_rs_name = format!("webserver-deployment-{new_hash}");
    let new_rs = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {
            "name": new_rs_name,
            "namespace": "default",
            "uid": new_rs_uid,
            "labels": {"name": "httpd", "pod-template-hash": new_hash},
            "annotations": {
                "deployment.kubernetes.io/revision": "2",
                "deployment.kubernetes.io/desired-replicas": "30",
                "deployment.kubernetes.io/max-replicas": "33"
            },
            "ownerReferences": [{
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "name": "webserver-deployment",
                "uid": deploy_uid,
                "controller": true,
                "blockOwnerDeletion": true
            }]
        },
        "spec": {
            "replicas": 13,
            "selector": {"matchLabels": {"name": "httpd", "pod-template-hash": new_hash}},
            "template": {
                "metadata": {"labels": {"name": "httpd", "pod-template-hash": new_hash}},
                "spec": {"containers": [{"name": "httpd", "image": "webserver:404"}]}
            }
        },
        "status": {"replicas": 13, "readyReplicas": 0, "availableReplicas": 0}
    });
    db.create_resource(
        "apps/v1",
        "ReplicaSet",
        Some("default"),
        &new_rs_name,
        new_rs,
    )
    .await
    .unwrap();

    let deploy_with_rv =
        crate::api::inject_resource_version(created_deploy.data, created_deploy.resource_version);
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

    let old_rs = db
        .get_resource(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            "webserver-deployment-old",
        )
        .await
        .unwrap()
        .unwrap();
    let new_rs = db
        .get_resource("apps/v1", "ReplicaSet", Some("default"), &new_rs_name)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        old_rs.data["spec"]["replicas"],
        json!(20),
        "a retry after a partially applied proportional scale must not re-proportion from the partial 13/8 state"
    );
    assert_eq!(
        new_rs.data["spec"]["replicas"],
        json!(13),
        "unavailable new RS must remain at its proportional target across retry/reconcile races"
    );
}

#[tokio::test]
async fn test_rollout_replicasets_record_desired_and_max_replica_annotations() {
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);
    let deploy_uid = "deploy-uid-rs-scale-annotations";

    let deployment = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "annotated",
            "namespace": "default",
            "uid": deploy_uid,
            "resourceVersion": "0"
        },
        "spec": {
            "replicas": 10,
            "selector": {"matchLabels": {"app": "annotated"}},
            "strategy": {
                "type": "RollingUpdate",
                "rollingUpdate": {
                    "maxSurge": 3,
                    "maxUnavailable": 2
                }
            },
            "template": {
                "metadata": {"labels": {"app": "annotated"}},
                "spec": {"containers": [{"name": "httpd", "image": "httpd:2.4"}]}
            }
        }
    });
    let created = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "annotated",
            deployment,
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
    let annotations = rs_list.items[0]
        .data
        .pointer("/metadata/annotations")
        .and_then(|v| v.as_object())
        .expect("ReplicaSet annotations should be present");
    assert_eq!(
        annotations.get("deployment.kubernetes.io/desired-replicas"),
        Some(&json!("10"))
    );
    assert_eq!(
        annotations.get("deployment.kubernetes.io/max-replicas"),
        Some(&json!("13"))
    );
}

#[tokio::test]
async fn test_rollover_with_unavailable_new_rs_keeps_old_rs_at_max_unavailable_zero() {
    // Regression: with maxUnavailable=0, a stuck new RS rollout must not
    // scale the adopted old RS below 1 while new RS pods are unavailable.
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);
    let deploy_uid = "deploy-uid-rollover-maxunavail-zero";
    let old_rs_uid = "rs-uid-rollover-old";

    let old_rs = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {
            "name": "test-rollover-controller",
            "namespace": "default",
            "uid": old_rs_uid,
            "labels": {
                "name": "rollover-pod",
                "pod": "webserver"
            }
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"name": "rollover-pod", "pod": "webserver"}},
            "template": {
                "metadata": {"labels": {"name": "rollover-pod", "pod": "webserver"}},
                "spec": {
                    "containers": [{
                        "name": "webserver",
                        "image": "nginx:1.14"
                    }]
                }
            }
        },
        "status": {
            "replicas": 1,
            "readyReplicas": 1,
            "availableReplicas": 1
        }
    });
    db.create_resource(
        "apps/v1",
        "ReplicaSet",
        Some("default"),
        "test-rollover-controller",
        old_rs,
    )
    .await
    .unwrap();

    let old_pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "test-rollover-controller-abcde",
            "namespace": "default",
            "labels": {"name": "rollover-pod", "pod": "webserver"},
            "ownerReferences": [{
                "apiVersion": "apps/v1",
                "kind": "ReplicaSet",
                "name": "test-rollover-controller",
                "uid": old_rs_uid,
                "controller": true,
                "blockOwnerDeletion": true
            }]
        },
        "spec": {"containers": [{"name": "webserver", "image": "nginx:1.14"}]},
        "status": {
            "phase": "Running",
            "conditions": [{
                "type": "Ready",
                "status": "True"
            }]
        }
    });
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "test-rollover-controller-abcde",
        old_pod,
    )
    .await
    .unwrap();

    let deployment = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "test-rollover-deployment",
            "namespace": "default",
            "uid": deploy_uid,
            "resourceVersion": "0"
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"name": "rollover-pod"}},
            "strategy": {
                "type": "RollingUpdate",
                "rollingUpdate": {
                    "maxUnavailable": 0,
                    "maxSurge": 1
                }
            },
            "template": {
                "metadata": {"labels": {"name": "rollover-pod"}},
                "spec": {
                    "containers": [{
                        "name": "redis-slave",
                        "image": "gcr.io/google_samples/gb-redisslave:nonexistent"
                    }]
                }
            }
        }
    });
    let created = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "test-rollover-deployment",
            deployment,
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

    // Reconcile again to exercise subsequent rolling-update iterations.
    let current_deploy = db
        .get_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "test-rollover-deployment",
        )
        .await
        .unwrap()
        .unwrap();
    let deploy_with_rv2 =
        crate::api::inject_resource_version(current_deploy.data, current_deploy.resource_version);
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

    let rs_list = db
        .list_resources(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(rs_list.items.len(), 2, "rollout must have old and new RS");

    let old_rs = rs_list
        .items
        .iter()
        .find(|rs| rs.data["metadata"]["name"] == "test-rollover-controller")
        .expect("old ReplicaSet should exist");
    let new_rs = rs_list
        .items
        .iter()
        .find(|rs| {
            rs.data["metadata"]["name"]
                .as_str()
                .is_some_and(|n| n.starts_with("test-rollover-deployment-"))
        })
        .expect("new ReplicaSet should exist");

    assert_eq!(
        old_rs.data["spec"]["replicas"], 1,
        "old RS must stay at 1 when new RS is unavailable and maxUnavailable=0"
    );
    assert_eq!(
        new_rs.data["spec"]["replicas"], 1,
        "new RS should stay at surge=1 during stuck rollout"
    );

    let current_deploy = db
        .get_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "test-rollover-deployment",
        )
        .await
        .unwrap()
        .unwrap();
    let mut updated_deploy: serde_json::Value = (*current_deploy.data).clone();
    updated_deploy["metadata"]["generation"] = json!(2);
    updated_deploy["spec"]["template"]["spec"]["containers"][0]["image"] =
        json!("registry.k8s.io/e2e-test-images/agnhost:2.56");
    let updated_deploy = db
        .update_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "test-rollover-deployment",
            updated_deploy,
            current_deploy.resource_version,
        )
        .await
        .unwrap();
    let deploy_with_rv3 =
        crate::api::inject_resource_version(updated_deploy.data, updated_deploy.resource_version);
    reconcile_deployment(
        &db,
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        &deploy_with_rv3,
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
    let adopted_old = rs_list
        .items
        .iter()
        .find(|rs| rs.data["metadata"]["name"] == "test-rollover-controller")
        .expect("adopted old ReplicaSet should exist");
    let failed_image_rs = rs_list
        .items
        .iter()
        .find(|rs| {
            rs.data
                .pointer("/spec/template/spec/containers/0/image")
                .and_then(|v| v.as_str())
                == Some("gcr.io/google_samples/gb-redisslave:nonexistent")
        })
        .expect("failed-image ReplicaSet should still exist during rollover");
    let replacement_rs = rs_list
        .items
        .iter()
        .find(|rs| {
            rs.data
                .pointer("/spec/template/spec/containers/0/image")
                .and_then(|v| v.as_str())
                == Some("registry.k8s.io/e2e-test-images/agnhost:2.56")
        })
        .expect("replacement ReplicaSet should exist during rollover");

    let old_controlled = adopted_old
        .data
        .pointer("/metadata/ownerReferences")
        .and_then(|v| v.as_array())
        .is_some_and(|refs| {
            refs.iter().any(|r| {
                r.get("uid").and_then(|u| u.as_str()) == Some(deploy_uid)
                    && r.get("controller").and_then(|c| c.as_bool()) == Some(true)
            })
        });
    assert!(
        old_controlled,
        "rollover must keep the available old RS controlled so deployment availability is preserved"
    );
    assert_eq!(
        adopted_old.data["spec"]["replicas"], 1,
        "rollover must preserve the available adopted RS while the replacement RS is unavailable"
    );
    assert_eq!(
        failed_image_rs.data["spec"]["replicas"], 0,
        "rollover should scale down the unavailable intermediate RS before creating replacement pods"
    );
    assert_eq!(
        replacement_rs.data["spec"]["replicas"], 1,
        "replacement RS should be allowed to use the freed surge slot"
    );

    let current_deploy = db
        .get_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "test-rollover-deployment",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        current_deploy.data["status"]["availableReplicas"],
        json!(1),
        "deployment availability must include the still-controlled available old RS"
    );
}

#[tokio::test]
async fn test_rollover_redrives_zero_replica_old_rs_with_live_pods() {
    let db = crate::datastore::test_support::in_memory().await;
    let pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);
    let namespace = "default";
    let deploy_uid = "deploy-uid-rollover-stuck-old-pod";
    let old_rs_uid = "old-rs-uid-rollover-stuck-old-pod";
    let current_rs_uid = "current-rs-uid-rollover-stuck-old-pod";

    let deployment = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "rollover-stuck",
            "namespace": namespace,
            "uid": deploy_uid,
            "generation": 2
        },
        "spec": {
            "replicas": 1,
            "minReadySeconds": 10,
            "selector": {"matchLabels": {"name": "rollover-pod"}},
            "strategy": {
                "type": "RollingUpdate",
                "rollingUpdate": {"maxUnavailable": 0, "maxSurge": 1}
            },
            "template": {
                "metadata": {"labels": {"name": "rollover-pod"}},
                "spec": {
                    "containers": [{
                        "name": "agnhost",
                        "image": "registry.k8s.io/e2e-test-images/agnhost:2.56"
                    }]
                }
            }
        }
    });
    let created_deployment = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some(namespace),
            "rollover-stuck",
            deployment,
        )
        .await
        .unwrap();
    let owner_ref = json!([{
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "name": "rollover-stuck",
        "uid": deploy_uid,
        "controller": true,
        "blockOwnerDeletion": true
    }]);
    let current_hash = compute_pod_template_hash(&created_deployment.data["spec"]["template"]);
    let current_rs_name = format!("rollover-stuck-{current_hash}");

    let current_rs = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {
            "name": current_rs_name,
            "namespace": namespace,
            "uid": current_rs_uid,
            "labels": {"name": "rollover-pod", "pod-template-hash": current_hash},
            "annotations": {
                "deployment.kubernetes.io/desired-replicas": "1",
                "deployment.kubernetes.io/max-replicas": "2",
                "deployment.kubernetes.io/revision": "2"
            },
            "ownerReferences": owner_ref.clone()
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"name": "rollover-pod", "pod-template-hash": current_hash}},
            "template": {
                "metadata": {"labels": {"name": "rollover-pod", "pod-template-hash": current_hash}},
                "spec": created_deployment.data["spec"]["template"]["spec"].clone()
            }
        },
        "status": {"replicas": 1, "readyReplicas": 1, "availableReplicas": 1, "fullyLabeledReplicas": 1, "observedGeneration": 1}
    });
    db.create_resource(
        "apps/v1",
        "ReplicaSet",
        Some(namespace),
        &current_rs_name,
        current_rs,
    )
    .await
    .unwrap();

    let old_hash = "oldhash";
    let old_rs = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {
            "name": "rollover-stuck-old",
            "namespace": namespace,
            "uid": old_rs_uid,
            "labels": {"name": "rollover-pod", "pod": "httpd", "pod-template-hash": old_hash},
            "annotations": {
                "deployment.kubernetes.io/desired-replicas": "1",
                "deployment.kubernetes.io/max-replicas": "2",
                "deployment.kubernetes.io/revision": "1"
            },
            "ownerReferences": owner_ref
        },
        "spec": {
            "replicas": 0,
            "selector": {"matchLabels": {"name": "rollover-pod", "pod": "httpd", "pod-template-hash": old_hash}},
            "template": {
                "metadata": {"labels": {"name": "rollover-pod", "pod": "httpd", "pod-template-hash": old_hash}},
                "spec": {"containers": [{"name": "httpd", "image": "registry.k8s.io/e2e-test-images/httpd:2.4.38-4"}]}
            }
        },
        "status": {"replicas": 1, "readyReplicas": 1, "availableReplicas": 1, "fullyLabeledReplicas": 1, "observedGeneration": 1}
    });
    db.create_resource(
        "apps/v1",
        "ReplicaSet",
        Some(namespace),
        "rollover-stuck-old",
        old_rs,
    )
    .await
    .unwrap();

    for (pod_name, rs_name, rs_uid, labels, image) in [
        (
            "rollover-stuck-current-pod",
            current_rs_name.as_str(),
            current_rs_uid,
            json!({"name": "rollover-pod", "pod-template-hash": current_hash}),
            "registry.k8s.io/e2e-test-images/agnhost:2.56",
        ),
        (
            "rollover-stuck-old-pod",
            "rollover-stuck-old",
            old_rs_uid,
            json!({"name": "rollover-pod", "pod": "httpd", "pod-template-hash": old_hash}),
            "registry.k8s.io/e2e-test-images/httpd:2.4.38-4",
        ),
    ] {
        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": pod_name,
                "namespace": namespace,
                "uid": format!("{pod_name}-uid"),
                "labels": labels,
                "ownerReferences": [{
                    "apiVersion": "apps/v1",
                    "kind": "ReplicaSet",
                    "name": rs_name,
                    "uid": rs_uid,
                    "controller": true,
                    "blockOwnerDeletion": true
                }]
            },
            "spec": {"containers": [{"name": "app", "image": image}]},
            "status": {
                "phase": "Running",
                "conditions": [
                    {"type": "Ready", "status": "True"},
                    {"type": "ContainersReady", "status": "True"}
                ]
            }
        });
        db.create_resource("v1", "Pod", Some(namespace), pod_name, pod)
            .await
            .unwrap();
    }

    let deployment_with_rv = crate::api::inject_resource_version(
        created_deployment.data,
        created_deployment.resource_version,
    );
    reconcile_deployment(
        &db,
        pod_repo.as_ref(),
        pod_repo.as_ref(),
        pod_repo.as_ref(),
        &deployment_with_rv,
        "test-node",
    )
    .await
    .unwrap();

    let old_pod = db
        .get_resource("v1", "Pod", Some(namespace), "rollover-stuck-old-pod")
        .await
        .unwrap()
        .unwrap();
    assert!(
        old_pod
            .data
            .pointer("/metadata/deletionTimestamp")
            .is_some(),
        "Deployment reconcile must re-drive a zero-replica old RS that still has live pods"
    );

    let live_deployment = db
        .get_resource("apps/v1", "Deployment", Some(namespace), "rollover-stuck")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        live_deployment.data["status"]["updatedReplicas"],
        json!(1),
        "current-template pod should satisfy desired replicas"
    );
    assert_eq!(
        live_deployment.data["status"]["replicas"],
        json!(1),
        "terminating old pods must not keep Deployment status above desired replicas"
    );
}

#[tokio::test]
async fn test_rolling_update_scale_down_tolerates_replicaset_status_rv_race() {
    let db = crate::datastore::test_support::in_memory().await;
    let pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);
    let namespace = "default";
    let deploy_uid = "deploy-uid-rs-status-race";
    let old_rs_uid = "old-rs-uid-rs-status-race";
    let new_rs_uid = "new-rs-uid-rs-status-race";

    let deployment = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "race-rollout",
            "namespace": namespace,
            "uid": deploy_uid,
            "generation": 2
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"name": "race-rollout-pod"}},
            "strategy": {
                "type": "RollingUpdate",
                "rollingUpdate": {"maxUnavailable": 0, "maxSurge": 1}
            },
            "template": {
                "metadata": {"labels": {"name": "race-rollout-pod"}},
                "spec": {
                    "containers": [{
                        "name": "agnhost",
                        "image": "registry.k8s.io/e2e-test-images/agnhost:2.56"
                    }]
                }
            }
        }
    });
    let created_deployment = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some(namespace),
            "race-rollout",
            deployment,
        )
        .await
        .unwrap();
    let owner_ref = json!([{
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "name": "race-rollout",
        "uid": deploy_uid,
        "controller": true,
        "blockOwnerDeletion": true
    }]);
    let new_hash = compute_pod_template_hash(&created_deployment.data["spec"]["template"]);
    let new_rs_name = format!("race-rollout-{new_hash}");

    let old_rs = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {
            "name": "race-rollout-old",
            "namespace": namespace,
            "uid": old_rs_uid,
            "labels": {"name": "race-rollout-pod", "pod": "httpd"},
            "annotations": {
                "deployment.kubernetes.io/desired-replicas": "1",
                "deployment.kubernetes.io/max-replicas": "2",
                "deployment.kubernetes.io/revision": "1"
            },
            "ownerReferences": owner_ref.clone()
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"name": "race-rollout-pod", "pod": "httpd"}},
            "template": {
                "metadata": {"labels": {"name": "race-rollout-pod", "pod": "httpd"}},
                "spec": {"containers": [{"name": "httpd", "image": "registry.k8s.io/e2e-test-images/httpd:2.4.38-4"}]}
            }
        },
        "status": {"replicas": 1, "readyReplicas": 1, "availableReplicas": 1, "fullyLabeledReplicas": 1, "observedGeneration": 1}
    });
    db.create_resource(
        "apps/v1",
        "ReplicaSet",
        Some(namespace),
        "race-rollout-old",
        old_rs,
    )
    .await
    .unwrap();

    let new_rs = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {
            "name": new_rs_name,
            "namespace": namespace,
            "uid": new_rs_uid,
            "labels": {"name": "race-rollout-pod", "pod-template-hash": new_hash},
            "annotations": {
                "deployment.kubernetes.io/desired-replicas": "1",
                "deployment.kubernetes.io/max-replicas": "2",
                "deployment.kubernetes.io/revision": "2"
            },
            "ownerReferences": owner_ref
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"name": "race-rollout-pod", "pod-template-hash": new_hash}},
            "template": {
                "metadata": {"labels": {"name": "race-rollout-pod", "pod-template-hash": new_hash}},
                "spec": created_deployment.data["spec"]["template"]["spec"].clone()
            }
        },
        "status": {"replicas": 1, "readyReplicas": 1, "availableReplicas": 1, "fullyLabeledReplicas": 1, "observedGeneration": 1}
    });
    db.create_resource(
        "apps/v1",
        "ReplicaSet",
        Some(namespace),
        &new_rs_name,
        new_rs,
    )
    .await
    .unwrap();

    for (pod_name, rs_name, rs_uid, labels, image) in [
        (
            "race-rollout-old-pod",
            "race-rollout-old",
            old_rs_uid,
            json!({"name": "race-rollout-pod", "pod": "httpd"}),
            "registry.k8s.io/e2e-test-images/httpd:2.4.38-4",
        ),
        (
            "race-rollout-new-pod",
            new_rs_name.as_str(),
            new_rs_uid,
            json!({"name": "race-rollout-pod", "pod-template-hash": new_hash}),
            "registry.k8s.io/e2e-test-images/agnhost:2.56",
        ),
    ] {
        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": pod_name,
                "namespace": namespace,
                "uid": format!("{pod_name}-uid"),
                "labels": labels,
                "ownerReferences": [{
                    "apiVersion": "apps/v1",
                    "kind": "ReplicaSet",
                    "name": rs_name,
                    "uid": rs_uid,
                    "controller": true,
                    "blockOwnerDeletion": true
                }]
            },
            "spec": {"containers": [{"name": "app", "image": image}]},
            "status": {
                "phase": "Running",
                "conditions": [
                    {"type": "Ready", "status": "True"},
                    {"type": "ContainersReady", "status": "True"}
                ]
            }
        });
        db.create_resource("v1", "Pod", Some(namespace), pod_name, pod)
            .await
            .unwrap();
    }

    let racing_reader = ReplicaSetStatusRacingPodReader {
        db: db.clone(),
        namespace: namespace.to_string(),
        replica_set_name: "race-rollout-old".to_string(),
        replica_set_uid: old_rs_uid.to_string(),
        bumped: AtomicBool::new(false),
    };
    let deployment_with_rv = crate::api::inject_resource_version(
        created_deployment.data,
        created_deployment.resource_version,
    );

    let result = reconcile_deployment(
        &db,
        &racing_reader,
        pod_repo.as_ref(),
        pod_repo.as_ref(),
        &deployment_with_rv,
        "test-node",
    )
    .await;
    assert!(
        result.is_ok(),
        "Deployment RS scale-down must tolerate concurrent RS status writes: {result:?}"
    );

    let old_rs = db
        .get_resource("apps/v1", "ReplicaSet", Some(namespace), "race-rollout-old")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        old_rs.data["spec"]["replicas"], 0,
        "available replacement pods should let Deployment scale the old RS down"
    );
}

#[tokio::test]
async fn test_rolling_update_creation_redrives_adopted_zero_replica_old_rs_with_live_pod() {
    let db = crate::datastore::test_support::in_memory().await;
    let pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);
    let namespace = "default";
    let deploy_uid = "deploy-uid-adopted-zero-old-rs";
    let old_rs_uid = "old-rs-uid-adopted-zero-old-rs";

    let old_rs = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {
            "name": "test-rolling-update-controller",
            "namespace": namespace,
            "uid": old_rs_uid,
            "labels": {"name": "sample-pod", "pod": "httpd"},
            "annotations": {
                "deployment.kubernetes.io/revision": "3546343826724305832",
                "deployment.kubernetes.io/desired-replicas": "1",
                "deployment.kubernetes.io/max-replicas": "2"
            }
        },
        "spec": {
            "replicas": 0,
            "selector": {"matchLabels": {"name": "sample-pod", "pod": "httpd"}},
            "template": {
                "metadata": {"labels": {"name": "sample-pod", "pod": "httpd"}},
                "spec": {"containers": [{"name": "httpd", "image": "registry.k8s.io/e2e-test-images/httpd:2.4.38-4"}]}
            }
        },
        "status": {
            "replicas": 1,
            "readyReplicas": 1,
            "availableReplicas": 1,
            "fullyLabeledReplicas": 1,
            "observedGeneration": 1
        }
    });
    db.create_resource(
        "apps/v1",
        "ReplicaSet",
        Some(namespace),
        "test-rolling-update-controller",
        old_rs,
    )
    .await
    .unwrap();

    let old_pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "test-rolling-update-controller-abcde",
            "namespace": namespace,
            "uid": "old-pod-uid-adopted-zero-old-rs",
            "labels": {"name": "sample-pod", "pod": "httpd"},
            "ownerReferences": [{
                "apiVersion": "apps/v1",
                "kind": "ReplicaSet",
                "name": "test-rolling-update-controller",
                "uid": old_rs_uid,
                "controller": true,
                "blockOwnerDeletion": true
            }]
        },
        "spec": {"containers": [{"name": "httpd", "image": "registry.k8s.io/e2e-test-images/httpd:2.4.38-4"}]},
        "status": {
            "phase": "Running",
            "conditions": [
                {"type": "Ready", "status": "True"},
                {"type": "ContainersReady", "status": "True"}
            ]
        }
    });
    db.create_resource(
        "v1",
        "Pod",
        Some(namespace),
        "test-rolling-update-controller-abcde",
        old_pod,
    )
    .await
    .unwrap();

    let deployment = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "test-rolling-update-deployment",
            "namespace": namespace,
            "uid": deploy_uid,
            "generation": 1
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"name": "sample-pod"}},
            "strategy": {"type": "RollingUpdate"},
            "template": {
                "metadata": {"labels": {"name": "sample-pod"}},
                "spec": {"containers": [{"name": "agnhost", "image": "registry.k8s.io/e2e-test-images/agnhost:2.56"}]}
            }
        }
    });
    let created_deployment = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some(namespace),
            "test-rolling-update-deployment",
            deployment,
        )
        .await
        .unwrap();
    let deployment_with_rv = crate::api::inject_resource_version(
        created_deployment.data,
        created_deployment.resource_version,
    );

    reconcile_deployment(
        &db,
        pod_repo.as_ref(),
        pod_repo.as_ref(),
        pod_repo.as_ref(),
        &deployment_with_rv,
        "test-node",
    )
    .await
    .unwrap();

    let old_pod = db
        .get_resource(
            "v1",
            "Pod",
            Some(namespace),
            "test-rolling-update-controller-abcde",
        )
        .await
        .unwrap()
        .unwrap();
    assert!(
        old_pod
            .data
            .pointer("/metadata/deletionTimestamp")
            .is_some(),
        "initial Deployment adoption must re-drive zero-replica old ReplicaSets that still own live pods"
    );
}

#[tokio::test]
async fn test_adopted_replicaset_and_existing_pods_get_matching_pod_template_hash() {
    let db = crate::datastore::test_support::in_memory().await;
    let pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);
    let deploy_uid = "deploy-uid-adopt-hash";
    let rs_uid = "rs-uid-adopt-hash";

    let orphan_rs = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {
            "name": "adopt-me",
            "namespace": "default",
            "uid": rs_uid,
            "labels": {"app": "adopted"}
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": "adopted"}},
            "template": {
                "metadata": {"labels": {"app": "adopted"}},
                "spec": {"containers": [{"name": "web", "image": "nginx:1.25"}]}
            }
        },
        "status": {"replicas": 1, "readyReplicas": 1, "availableReplicas": 1}
    });
    db.create_resource(
        "apps/v1",
        "ReplicaSet",
        Some("default"),
        "adopt-me",
        orphan_rs,
    )
    .await
    .unwrap();

    let old_pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "adopt-me-abcde",
            "namespace": "default",
            "uid": "pod-uid-adopt-hash",
            "labels": {"app": "adopted"},
            "ownerReferences": [{
                "apiVersion": "apps/v1",
                "kind": "ReplicaSet",
                "name": "adopt-me",
                "uid": rs_uid,
                "controller": true,
                "blockOwnerDeletion": true
            }]
        },
        "spec": {"containers": [{"name": "web", "image": "nginx:1.25"}]},
        "status": {"phase": "Running", "conditions": [{"type": "Ready", "status": "True"}]}
    });
    db.create_resource("v1", "Pod", Some("default"), "adopt-me-abcde", old_pod)
        .await
        .unwrap();

    let deployment = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "adopter",
            "namespace": "default",
            "uid": deploy_uid,
            "resourceVersion": "0"
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": "adopted"}},
            "template": {
                "metadata": {"labels": {"app": "adopted"}},
                "spec": {"containers": [{"name": "web", "image": "nginx:1.25"}]}
            }
        }
    });
    let created = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "adopter",
            deployment,
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

    let adopted_rs = db
        .get_resource("apps/v1", "ReplicaSet", Some("default"), "adopt-me")
        .await
        .unwrap()
        .unwrap();
    let hash = adopted_rs
        .data
        .pointer("/metadata/labels/pod-template-hash")
        .and_then(|v| v.as_str())
        .expect("adopted RS must get pod-template-hash label");
    assert_eq!(
        adopted_rs
            .data
            .pointer("/spec/selector/matchLabels/pod-template-hash")
            .and_then(|v| v.as_str()),
        Some(hash)
    );
    assert_eq!(
        adopted_rs
            .data
            .pointer("/spec/template/metadata/labels/pod-template-hash")
            .and_then(|v| v.as_str()),
        Some(hash)
    );

    let adopted_pod = db
        .get_resource("v1", "Pod", Some("default"), "adopt-me-abcde")
        .await
        .unwrap()
        .unwrap();
    assert!(
        adopted_pod
            .data
            .pointer("/metadata/name")
            .and_then(|v| v.as_str())
            .is_some_and(|name| name.starts_with("adopt-me-")),
        "adopted Pod name must keep ReplicaSet-name prefix"
    );
    assert_eq!(
        adopted_pod
            .data
            .pointer("/metadata/labels/pod-template-hash")
            .and_then(|v| v.as_str()),
        Some(hash),
        "existing Pods owned by the adopted RS must get the matching pod-template-hash"
    );
}

#[tokio::test]
async fn test_completed_rollover_deletes_multiple_previous_hash_replicasets() {
    let db = crate::datastore::test_support::in_memory().await;
    let pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);
    let deploy_uid = "deploy-uid-clean-prev-rs";

    let deployment = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "rollover-clean",
            "namespace": "default",
            "uid": deploy_uid,
            "resourceVersion": "0"
        },
        "spec": {
            "replicas": 1,
            "revisionHistoryLimit": 0,
            "selector": {"matchLabels": {"app": "rollover-clean"}},
            "strategy": {"type": "RollingUpdate", "rollingUpdate": {"maxSurge": 1, "maxUnavailable": 1}},
            "template": {
                "metadata": {"labels": {"app": "rollover-clean"}},
                "spec": {"containers": [{"name": "web", "image": "nginx:1.27"}]}
            }
        }
    });
    let created = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "rollover-clean",
            deployment,
        )
        .await
        .unwrap();

    for (hash, image) in [("old111", "nginx:1.25"), ("old222", "nginx:1.26")] {
        let rs_name = format!("rollover-clean-{hash}");
        let old_rs = json!({
            "apiVersion": "apps/v1",
            "kind": "ReplicaSet",
            "metadata": {
                "name": rs_name,
                "namespace": "default",
                "uid": format!("rs-uid-{hash}"),
                "labels": {"app": "rollover-clean", "pod-template-hash": hash},
                "annotations": {"deployment.kubernetes.io/revision": "1"},
                "ownerReferences": [{
                    "apiVersion": "apps/v1",
                    "kind": "Deployment",
                    "name": "rollover-clean",
                    "uid": deploy_uid,
                    "controller": true,
                    "blockOwnerDeletion": true
                }]
            },
            "spec": {
                "replicas": 0,
                "selector": {"matchLabels": {"app": "rollover-clean", "pod-template-hash": hash}},
                "template": {
                    "metadata": {"labels": {"app": "rollover-clean", "pod-template-hash": hash}},
                    "spec": {"containers": [{"name": "web", "image": image}]}
                }
            },
            "status": {"replicas": 0, "readyReplicas": 0, "availableReplicas": 0}
        });
        db.create_resource("apps/v1", "ReplicaSet", Some("default"), &rs_name, old_rs)
            .await
            .unwrap();
    }

    let current_hash = compute_pod_template_hash(&created.data["spec"]["template"]);
    let current_rs_name = format!("rollover-clean-{current_hash}");
    let current_rs_uid = "rs-uid-current-clean";
    let current_rs = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {
            "name": current_rs_name,
            "namespace": "default",
            "uid": current_rs_uid,
            "labels": {"app": "rollover-clean", "pod-template-hash": current_hash},
            "annotations": {"deployment.kubernetes.io/revision": "3"},
            "ownerReferences": [{
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "name": "rollover-clean",
                "uid": deploy_uid,
                "controller": true,
                "blockOwnerDeletion": true
            }]
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": "rollover-clean", "pod-template-hash": current_hash}},
            "template": {
                "metadata": {"labels": {"app": "rollover-clean", "pod-template-hash": current_hash}},
                "spec": {"containers": [{"name": "web", "image": "nginx:1.27"}]}
            }
        },
        "status": {"replicas": 1, "readyReplicas": 1, "availableReplicas": 1}
    });
    db.create_resource(
        "apps/v1",
        "ReplicaSet",
        Some("default"),
        &current_rs_name,
        current_rs,
    )
    .await
    .unwrap();

    let current_pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": format!("{current_rs_name}-abcde"),
            "namespace": "default",
            "uid": "pod-uid-current-clean",
            "labels": {"app": "rollover-clean", "pod-template-hash": current_hash},
            "ownerReferences": [{
                "apiVersion": "apps/v1",
                "kind": "ReplicaSet",
                "name": current_rs_name,
                "uid": current_rs_uid,
                "controller": true,
                "blockOwnerDeletion": true
            }]
        },
        "spec": {"containers": [{"name": "web", "image": "nginx:1.27"}]},
        "status": {"phase": "Running", "conditions": [{"type": "Ready", "status": "True"}]}
    });
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        &format!("{current_rs_name}-abcde"),
        current_pod,
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
    assert_eq!(rs_list.items[0].name, current_rs_name);
}

#[tokio::test]
async fn test_reconcile_deployment_rolling_update_completes() {
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);
    let deploy_uid = "deploy-uid-complete";

    // Create initial deployment
    let deploy = make_deployment_with_image("app", "default", deploy_uid, 2, "0", "nginx:1.14");
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

    // Trigger rolling update
    let current_deploy = db
        .get_resource("apps/v1", "Deployment", Some("default"), "app")
        .await
        .unwrap()
        .unwrap();

    let updated_deploy =
        make_deployment_with_image("app", "default", deploy_uid, 2, "0", "nginx:1.16");
    let updated = db
        .update_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "app",
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

    // Simulate kubelet readiness so rollout can progress under maxUnavailable=0.
    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    for pod in pods.items {
        let mut ready_pod: serde_json::Value = (*pod.data).clone();
        ready_pod["status"] = json!({
            "phase": "Running",
            "conditions": [{"type": "Ready", "status": "True"}]
        });
        db.update_resource(
            "v1",
            "Pod",
            Some("default"),
            &pod.name,
            ready_pod,
            pod.resource_version,
        )
        .await
        .unwrap();
    }
    // Refresh RS status from the updated pod readiness.
    let rs_after_ready = db
        .list_resources(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    for rs in rs_after_ready.items {
        let rs_with_rv = crate::api::inject_resource_version(rs.data, rs.resource_version);
        crate::controllers::replicaset::reconcile_replicaset(
            &db,
            __pod_repo.as_ref(),
            __pod_repo.as_ref(),
            __pod_repo.as_ref(),
            &rs_with_rv,
            "test-node",
        )
        .await
        .unwrap();
    }

    // After rollout completes, reconcile again with the same spec
    // Should scale new RS to full replicas and old RS to 0
    let current_deploy2 = db
        .get_resource("apps/v1", "Deployment", Some("default"), "app")
        .await
        .unwrap()
        .unwrap();

    let deploy_with_rv2 =
        crate::api::inject_resource_version(current_deploy2.data, current_deploy2.resource_version);
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

    // Verify final state
    let rs_list = db
        .list_resources(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();

    let new_rs = rs_list
        .items
        .iter()
        .find(|rs| rs.data["spec"]["template"]["spec"]["containers"][0]["image"] == "nginx:1.16")
        .expect("New ReplicaSet should exist");
    let old_rs = rs_list
        .items
        .iter()
        .find(|rs| rs.data["spec"]["template"]["spec"]["containers"][0]["image"] == "nginx:1.14")
        .expect("Old ReplicaSet should still exist");

    // Old RS remains controlled until it reaches zero.
    let old_owner_refs = old_rs
        .data
        .pointer("/metadata/ownerReferences")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let old_controlled = old_owner_refs.iter().any(|r| {
        r.get("controller").and_then(|c| c.as_bool()) == Some(true)
            && r.get("uid").and_then(|u| u.as_str()) == Some(deploy_uid)
    });
    let old_owned = old_owner_refs
        .iter()
        .any(|r| r.get("uid").and_then(|u| u.as_str()) == Some(deploy_uid));
    assert!(
        old_owned,
        "old RS should retain ownerReference to deployment"
    );
    assert!(old_controlled, "old RS controller=true after rollout");
    assert_eq!(
        old_rs.data["spec"]["replicas"], 0,
        "old RS should be scaled down after replacement pods are ready"
    );
    assert_eq!(
        new_rs.data["spec"]["replicas"], 2,
        "new RS should be at full desired replicas"
    );
}

struct DeploymentStrategyFixture<'a> {
    name: &'a str,
    namespace: &'a str,
    uid: &'a str,
    replicas: i64,
    resource_version: &'a str,
    image: &'a str,
    max_surge: &'a str,
    max_unavailable: &'a str,
}

fn make_deployment_with_strategy(fixture: DeploymentStrategyFixture<'_>) -> Value {
    let DeploymentStrategyFixture {
        name,
        namespace,
        uid,
        replicas,
        resource_version,
        image,
        max_surge,
        max_unavailable,
    } = fixture;
    json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "uid": uid,
            "resourceVersion": resource_version,
            "labels": {"app": name}
        },
        "spec": {
            "replicas": replicas,
            "selector": {"matchLabels": {"app": name}},
            "strategy": {
                "type": "RollingUpdate",
                "rollingUpdate": {
                    "maxSurge": max_surge,
                    "maxUnavailable": max_unavailable
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
async fn test_reconcile_deployment_rolling_update_percentage_strategy() {
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);
    let deploy_uid = "deploy-uid-percent";

    // Create deployment with 10 replicas and 50% maxSurge/maxUnavailable
    let deploy = make_deployment_with_strategy(DeploymentStrategyFixture {
        name: "api",
        namespace: "default",
        uid: deploy_uid,
        replicas: 10,
        resource_version: "0",
        image: "nginx:1.14",
        max_surge: "50%",
        max_unavailable: "50%",
    });
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

    // Trigger rolling update
    let current_deploy = db
        .get_resource("apps/v1", "Deployment", Some("default"), "api")
        .await
        .unwrap()
        .unwrap();

    let updated_deploy = make_deployment_with_strategy(DeploymentStrategyFixture {
        name: "api",
        namespace: "default",
        uid: deploy_uid,
        replicas: 10,
        resource_version: "0",
        image: "nginx:1.16",
        max_surge: "50%",
        max_unavailable: "50%",
    });
    let updated = db
        .update_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "api",
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

    assert_eq!(rs_list.items.len(), 2);

    let new_rs = rs_list
        .items
        .iter()
        .find(|rs| rs.data["spec"]["template"]["spec"]["containers"][0]["image"] == "nginx:1.16")
        .unwrap();
    let old_rs = rs_list
        .items
        .iter()
        .find(|rs| rs.data["spec"]["template"]["spec"]["containers"][0]["image"] == "nginx:1.14")
        .unwrap();

    let new_replicas = new_rs.data["spec"]["replicas"].as_i64().unwrap();
    let old_replicas = old_rs.data["spec"]["replicas"].as_i64().unwrap();

    assert_eq!(new_replicas, 5, "new RS should start at maxSurge");
    assert_eq!(old_replicas, 5, "old RS should scale down to minAvailable");
}

#[tokio::test]
async fn test_reconcile_deployment_tracks_revision_numbers() {
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);
    let deploy_uid = "deploy-uid-revision";

    // Create initial deployment with nginx:1.14
    let deploy = make_deployment_with_image("web", "default", deploy_uid, 2, "0", "nginx:1.14");
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

    // Check that the created ReplicaSet has revision annotation
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
    let revision = rs.data["metadata"]["annotations"]["deployment.kubernetes.io/revision"]
        .as_str()
        .expect("ReplicaSet should have revision annotation");
    assert_eq!(revision, "1", "First ReplicaSet should have revision 1");
}

#[tokio::test]
async fn test_reconcile_deployment_rollback_to_previous_revision() {
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);
    let deploy_uid = "deploy-uid-rollback";

    // Create deployment with nginx:1.14 (revision 1)
    let deploy_v1 = make_deployment_with_image("app", "default", deploy_uid, 3, "0", "nginx:1.14");
    let created = db
        .create_resource("apps/v1", "Deployment", Some("default"), "app", deploy_v1)
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

    // Update to nginx:1.16 (revision 2)
    let current = db
        .get_resource("apps/v1", "Deployment", Some("default"), "app")
        .await
        .unwrap()
        .unwrap();

    let deploy_v2 = make_deployment_with_image("app", "default", deploy_uid, 3, "0", "nginx:1.16");
    let updated = db
        .update_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "app",
            deploy_v2,
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

    // Verify we have 2 ReplicaSets with different revisions
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
        2,
        "Should have 2 ReplicaSets after rolling update"
    );

    // Now rollback to revision 1 by adding rollback annotation
    let current2 = db
        .get_resource("apps/v1", "Deployment", Some("default"), "app")
        .await
        .unwrap()
        .unwrap();

    let mut deploy_rollback: serde_json::Value = (*current2.data).clone();
    if let Some(metadata) = deploy_rollback
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        let annotations = metadata.entry("annotations").or_insert(json!({}));
        if let Some(ann_obj) = annotations.as_object_mut() {
            ann_obj.insert(
                "deployment.kubernetes.io/rollback-to".to_string(),
                json!("1"),
            );
        }
    }

    let updated2 = db
        .update_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "app",
            deploy_rollback,
            current2.resource_version,
        )
        .await
        .unwrap();

    let deploy_with_rv2 =
        crate::api::inject_resource_version(updated2.data, updated2.resource_version);
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

    // Verify that the deployment template now matches revision 1 (nginx:1.14)
    let final_deploy = db
        .get_resource("apps/v1", "Deployment", Some("default"), "app")
        .await
        .unwrap()
        .unwrap();

    let image = final_deploy.data["spec"]["template"]["spec"]["containers"][0]["image"]
        .as_str()
        .unwrap();
    assert_eq!(
        image, "nginx:1.14",
        "Deployment should be rolled back to nginx:1.14"
    );

    // Verify rollback annotation is removed
    let annotations = final_deploy.data["metadata"]["annotations"].as_object();
    assert!(
        annotations.is_none()
            || !annotations
                .unwrap()
                .contains_key("deployment.kubernetes.io/rollback-to"),
        "Rollback annotation should be removed after rollback"
    );
}
