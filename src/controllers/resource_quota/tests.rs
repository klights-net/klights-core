use super::*;

use async_trait::async_trait;
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

async fn make_raft_resourcequota_datastore() -> (
    crate::datastore::replicated::ReplicatedDatastore,
    crate::datastore::sqlite::Datastore,
) {
    use crate::datastore::backend::DatastoreHandle;
    use crate::datastore::command::StorageCommand;
    use crate::kubelet::outbox::payload::{OutboxOperation, OutboxPayload};

    struct InlineProposer {
        inner: DatastoreHandle,
    }

    #[async_trait]
    impl crate::datastore::replicated::RaftProposer for InlineProposer {
        async fn propose_command(&self, command: StorageCommand) -> anyhow::Result<()> {
            let payload = OutboxPayload::from_command(command).encode_protobuf()?;
            let key = format!("resource-quota-inline-{}", uuid::Uuid::new_v4());
            crate::datastore::raft::state_machine::propose_outbox_on_backend(
                self.inner.as_ref(),
                &key,
                OutboxOperation::PodStatus,
                bytes::Bytes::from(payload),
                "resource-quota-inline-proposer",
            )
            .await
            .map_err(|err| anyhow::anyhow!("inline resource quota propose: {err}"))?;
            Ok(())
        }

        async fn propose_outbox_command(
            &self,
            idempotency_key: &str,
            operation: &str,
            command: StorageCommand,
            authoring_node: &str,
        ) -> std::result::Result<
            crate::kubelet::outbox::OutboxApplyResult,
            crate::kubelet::outbox::OutboxApplyError,
        > {
            let payload = OutboxPayload::from_command(command)
                .encode_protobuf()
                .map_err(|err| {
                    crate::kubelet::outbox::OutboxApplyError::Retryable(err.to_string())
                })?;
            let outcome = crate::datastore::raft::state_machine::propose_outbox_on_backend(
                self.inner.as_ref(),
                idempotency_key,
                OutboxOperation::try_from(operation).map_err(|err| {
                    crate::kubelet::outbox::OutboxApplyError::Retryable(err.to_string())
                })?,
                bytes::Bytes::from(payload),
                authoring_node,
            )
            .await?;
            Ok(outcome.result)
        }
    }

    let inner = crate::datastore::test_support::in_memory().await;
    let handle: DatastoreHandle = Arc::new(inner.clone());
    let ds = crate::datastore::replicated::ReplicatedDatastore::new(
        handle.clone(),
        crate::datastore::replicated::ReplicationMode::Raft {
            node_name: "resource-quota-test-node".to_string(),
        },
    );
    ds.set_raft_proposer(Arc::new(InlineProposer { inner: handle }));
    (ds, inner)
}

struct ResourceQuotaConflictPodReader {
    inner: Arc<dyn crate::kubelet::pod_repository::PodReader>,
    db: crate::datastore::sqlite::Datastore,
    updated: AtomicBool,
}

#[async_trait]
impl crate::kubelet::pod_repository::PodReader for ResourceQuotaConflictPodReader {
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
        if ns == Some("default") && !self.updated.swap(true, Ordering::SeqCst) {
            let current = self
                .db
                .get_resource("v1", "ResourceQuota", Some("default"), "test-rq")
                .await?
                .expect("test ResourceQuota should exist");
            let mut updated = (*current.data).clone();
            updated["spec"]["hard"] = json!({
                "pods": "10",
                "secrets": "5"
            });
            self.db
                .update_resource(
                    "v1",
                    "ResourceQuota",
                    Some("default"),
                    "test-rq",
                    updated,
                    current.resource_version,
                )
                .await?;
        }

        self.inner
            .list_pods(ns, label_selector, field_selector, limit, continue_token)
            .await
    }

    async fn list_pods_by_owner_uid(
        &self,
        ns: &str,
        owner_uid: &str,
    ) -> anyhow::Result<Vec<crate::datastore::Resource>> {
        self.inner.list_pods_by_owner_uid(ns, owner_uid).await
    }
}

struct ResourceQuotaStatusConflictPodReader {
    inner: Arc<dyn crate::kubelet::pod_repository::PodReader>,
    db: crate::datastore::sqlite::Datastore,
    updated: AtomicBool,
}

#[async_trait]
impl crate::kubelet::pod_repository::PodReader for ResourceQuotaStatusConflictPodReader {
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
        if ns == Some("default") && !self.updated.swap(true, Ordering::SeqCst) {
            let current = self
                .db
                .get_resource("v1", "ResourceQuota", Some("default"), "test-rq")
                .await?
                .expect("test ResourceQuota should exist");
            self.db
                .update_status_only(
                    "v1",
                    "ResourceQuota",
                    Some("default"),
                    "test-rq",
                    json!({
                        "hard": {"pods": "4"},
                        "used": {"pods": "77"}
                    }),
                    Some(current.resource_version),
                )
                .await?;
        }

        self.inner
            .list_pods(ns, label_selector, field_selector, limit, continue_token)
            .await
    }

    async fn list_pods_by_owner_uid(
        &self,
        ns: &str,
        owner_uid: &str,
    ) -> anyhow::Result<Vec<crate::datastore::Resource>> {
        self.inner.list_pods_by_owner_uid(ns, owner_uid).await
    }
}

#[test]
fn test_parse_scalar_resource_accepts_decimal_si_suffix() {
    assert_eq!(
        parse_resource_quantity("example.com/fakecpu", "1k"),
        Some(1000)
    );
    assert_eq!(
        parse_resource_quantity("example.com/fakecpu", "1.5k"),
        Some(1500)
    );
}

#[test]
fn test_resource_quota_scope_selector_matches_priority_class_and_cross_namespace_affinity() {
    let high_priority_quota = json!({
        "spec": {
            "scopeSelector": {
                "matchExpressions": [{
                    "scopeName": "PriorityClass",
                    "operator": "In",
                    "values": ["high"]
                }]
            }
        }
    });
    let high_pod = json!({"spec": {"priorityClassName": "high", "containers": []}});
    let low_pod = json!({"spec": {"priorityClassName": "low", "containers": []}});
    assert!(pod_matches_resource_quota_scopes(
        &high_pod,
        &high_priority_quota
    ));
    assert!(!pod_matches_resource_quota_scopes(
        &low_pod,
        &high_priority_quota
    ));

    let cross_namespace_quota = json!({
        "spec": {
            "scopeSelector": {
                "matchExpressions": [{
                    "scopeName": "CrossNamespacePodAffinity",
                    "operator": "Exists"
                }]
            }
        }
    });
    let cross_namespace_pod = json!({
        "spec": {
            "affinity": {
                "podAffinity": {
                    "requiredDuringSchedulingIgnoredDuringExecution": [{
                        "labelSelector": {"matchLabels": {"app": "db"}},
                        "namespaces": ["shared"]
                    }]
                }
            },
            "containers": []
        }
    });
    let same_namespace_pod = json!({
        "spec": {
            "affinity": {
                "podAffinity": {
                    "requiredDuringSchedulingIgnoredDuringExecution": [{
                        "labelSelector": {"matchLabels": {"app": "db"}}
                    }]
                }
            },
            "containers": []
        }
    });
    assert!(pod_matches_resource_quota_scopes(
        &cross_namespace_pod,
        &cross_namespace_quota
    ));
    assert!(!pod_matches_resource_quota_scopes(
        &same_namespace_pod,
        &cross_namespace_quota
    ));
}

#[tokio::test]
async fn test_reconcile_resource_quota_rejects_stale_status_overlap() {
    let db = crate::datastore::test_support::in_memory().await;

    db.create_resource(
        "v1",
        "ResourceQuota",
        Some("default"),
        "test-rq",
        json!({
            "apiVersion": "v1",
            "kind": "ResourceQuota",
            "metadata": {"name": "test-rq", "namespace": "default"},
            "spec": {"hard": {"pods": "4"}},
            "status": {"hard": {"pods": "4"}, "used": {"pods": "0"}}
        }),
    )
    .await
    .unwrap();

    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "test-pod",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "test-pod", "namespace": "default"},
            "spec": {"containers": [{"name": "c", "image": "busybox"}]}
        }),
    )
    .await
    .unwrap();

    let pod_reader = ResourceQuotaStatusConflictPodReader {
        inner: crate::controllers::test_utils::pod_repository_for_test(&db),
        db: db.clone(),
        updated: AtomicBool::new(false),
    };

    let result = reconcile_resource_quotas_for_namespace(&db, &pod_reader, "default").await;
    let err = result.expect_err("stale ResourceQuota status overlap must be rejected");
    assert!(
        crate::datastore::errors::is_conflict_error(&err),
        "expected status conflict, got {err:#}"
    );

    let rq = db
        .get_resource("v1", "ResourceQuota", Some("default"), "test-rq")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        rq.data
            .pointer("/status/used/pods")
            .and_then(|v| v.as_str()),
        Some("77"),
        "ResourceQuota reconcile must not overwrite a live status change from a stale snapshot"
    );
}

#[tokio::test]
async fn test_reconcile_resource_quota_rejects_stale_spec_overlap() {
    let db = crate::datastore::test_support::in_memory().await;

    db.create_resource(
        "v1",
        "ResourceQuota",
        Some("default"),
        "test-rq",
        json!({
            "apiVersion": "v1",
            "kind": "ResourceQuota",
            "metadata": {"name": "test-rq", "namespace": "default"},
            "spec": {"hard": {"pods": "4"}},
            "status": {"hard": {"pods": "4"}, "used": {"pods": "0"}}
        }),
    )
    .await
    .unwrap();

    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "test-pod",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "test-pod", "namespace": "default"},
            "spec": {"containers": [{"name": "c", "image": "busybox"}]}
        }),
    )
    .await
    .unwrap();

    let pod_reader = ResourceQuotaConflictPodReader {
        inner: crate::controllers::test_utils::pod_repository_for_test(&db),
        db: db.clone(),
        updated: AtomicBool::new(false),
    };

    let result = reconcile_resource_quotas_for_namespace(&db, &pod_reader, "default").await;
    let err = result.expect_err("stale ResourceQuota spec overlap must be rejected");
    assert!(
        crate::datastore::errors::is_conflict_error(&err),
        "expected status conflict, got {err:#}"
    );

    let rq = db
        .get_resource("v1", "ResourceQuota", Some("default"), "test-rq")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        rq.data
            .pointer("/spec/hard/secrets")
            .and_then(|v| v.as_str()),
        Some("5"),
        "test mutation should leave the live spec.hard changed"
    );
    assert_eq!(
        rq.data
            .pointer("/status/used/secrets")
            .and_then(|v| v.as_str()),
        None,
        "ResourceQuota reconcile must not rebase stale status across a live spec change"
    );
}

#[tokio::test]
async fn test_reconcile_resource_quota_writes_status_through_raft_status_subresource() {
    let (db, inner) = make_raft_resourcequota_datastore().await;

    db.create_resource(
        "v1",
        "ResourceQuota",
        Some("default"),
        "test-quota",
        json!({
            "apiVersion": "v1",
            "kind": "ResourceQuota",
            "metadata": {"name": "test-quota", "namespace": "default"},
            "spec": {"hard": {"resourcequotas": "1", "secrets": "1"}},
            "status": {
                "hard": {"resourcequotas": "1", "secrets": "1"},
                "used": {"resourcequotas": "0", "secrets": "0"}
            }
        }),
    )
    .await
    .expect("create ResourceQuota through raft");

    reconcile_resource_quotas_for_namespace(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&inner).as_ref(),
        "default",
    )
    .await
    .expect("reconcile ResourceQuota through raft");

    let rq = db
        .get_resource("v1", "ResourceQuota", Some("default"), "test-quota")
        .await
        .expect("get ResourceQuota")
        .expect("ResourceQuota exists");

    assert_eq!(
        rq.data
            .pointer("/status/used/resourcequotas")
            .and_then(|v| v.as_str()),
        Some("1"),
        "raft-routed ResourceQuota reconcile must update status.used.resourcequotas via status subresource"
    );
}

#[tokio::test]
async fn test_reconcile_resource_quotas_updates_secret_count() {
    let db = crate::datastore::test_support::in_memory().await;

    // Create a ResourceQuota tracking secrets
    db.create_resource(
        "v1",
        "ResourceQuota",
        Some("default"),
        "test-quota",
        json!({
            "apiVersion": "v1",
            "kind": "ResourceQuota",
            "metadata": {"name": "test-quota", "namespace": "default"},
            "spec": {"hard": {"secrets": "10"}},
            "status": {"hard": {"secrets": "10"}, "used": {"secrets": "0"}}
        }),
    )
    .await
    .unwrap();

    // Create 2 secrets
    for i in 0..2 {
        db.create_resource(
            "v1",
            "Secret",
            Some("default"),
            &format!("secret-{}", i),
            json!({
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {"name": format!("secret-{}", i), "namespace": "default"}
            }),
        )
        .await
        .unwrap();
    }

    // Reconcile
    reconcile_resource_quotas_for_namespace(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        "default",
    )
    .await
    .unwrap();

    // Check status.used.secrets = "2"
    let rq = db
        .get_resource("v1", "ResourceQuota", Some("default"), "test-quota")
        .await
        .unwrap()
        .unwrap();

    let used_secrets = rq
        .data
        .pointer("/status/used/secrets")
        .and_then(|v| v.as_str())
        .unwrap_or("missing");
    assert_eq!(
        used_secrets, "2",
        "status.used.secrets must be 2 after creating 2 secrets"
    );
}

#[tokio::test]
async fn test_reconcile_resource_quotas_decrements_on_delete() {
    let db = crate::datastore::test_support::in_memory().await;

    db.create_resource(
        "v1",
        "ResourceQuota",
        Some("default"),
        "test-quota",
        json!({
            "apiVersion": "v1",
            "kind": "ResourceQuota",
            "metadata": {"name": "test-quota", "namespace": "default"},
            "spec": {"hard": {"secrets": "10"}},
            "status": {"hard": {"secrets": "10"}, "used": {"secrets": "0"}}
        }),
    )
    .await
    .unwrap();

    // Create then delete a secret
    db.create_resource(
        "v1",
        "Secret",
        Some("default"),
        "to-delete",
        json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": "to-delete", "namespace": "default"}
        }),
    )
    .await
    .unwrap();

    db.delete_resource("v1", "Secret", Some("default"), "to-delete")
        .await
        .unwrap();

    reconcile_resource_quotas_for_namespace(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        "default",
    )
    .await
    .unwrap();

    let rq = db
        .get_resource("v1", "ResourceQuota", Some("default"), "test-quota")
        .await
        .unwrap()
        .unwrap();

    let used_secrets = rq
        .data
        .pointer("/status/used/secrets")
        .and_then(|v| v.as_str())
        .unwrap_or("missing");
    assert_eq!(
        used_secrets, "0",
        "status.used.secrets must be 0 after deleting the only secret"
    );
}

#[tokio::test]
async fn test_reconcile_resource_quotas_no_quota_is_noop() {
    let db = crate::datastore::test_support::in_memory().await;
    // Should not panic/error when no ResourceQuota exists
    let result = reconcile_resource_quotas_for_namespace(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        "default",
    )
    .await;
    assert!(result.is_ok());
}

#[test]
fn test_pod_is_terminating_uses_active_deadline_seconds() {
    let terminating = json!({
        "spec": {"activeDeadlineSeconds": 30},
        "metadata": {}
    });
    let not_terminating = json!({
        "spec": {"containers": [{"name": "c", "image": "busybox"}]},
        "metadata": {"deletionTimestamp": "2026-01-01T00:00:00Z"}
    });
    assert!(pod_is_terminating(&terminating));
    assert!(
        !pod_is_terminating(&not_terminating),
        "deletionTimestamp alone should not satisfy Terminating scope"
    );
}

#[tokio::test]
async fn test_reconcile_resource_quota_notterminating_tracks_pod_compute_usage() {
    let db = crate::datastore::test_support::in_memory().await;

    db.create_resource(
        "v1",
        "ResourceQuota",
        Some("default"),
        "quota-not-terminating",
        json!({
            "apiVersion": "v1",
            "kind": "ResourceQuota",
            "metadata": {"name": "quota-not-terminating", "namespace": "default"},
            "spec": {
                "hard": {
                    "pods": "5",
                    "requests.cpu": "1",
                    "requests.memory": "500Mi",
                    "limits.cpu": "2",
                    "limits.memory": "1Gi"
                },
                "scopes": ["NotTerminating"]
            },
            "status": {"hard": {}, "used": {}}
        }),
    )
    .await
    .unwrap();

    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "test-pod",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "test-pod", "namespace": "default"},
            "spec": {
                "containers": [{
                    "name": "pause",
                    "image": "registry.k8s.io/pause:3.10",
                    "resources": {
                        "requests": {"cpu": "500m", "memory": "200Mi"},
                        "limits": {"cpu": "1", "memory": "400Mi"}
                    }
                }]
            }
        }),
    )
    .await
    .unwrap();

    reconcile_resource_quotas_for_namespace(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        "default",
    )
    .await
    .unwrap();

    let rq = db
        .get_resource(
            "v1",
            "ResourceQuota",
            Some("default"),
            "quota-not-terminating",
        )
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        rq.data
            .pointer("/status/used/pods")
            .and_then(|v| v.as_str()),
        Some("1")
    );
    assert_eq!(
        rq.data
            .pointer("/status/used/requests.cpu")
            .and_then(|v| v.as_str()),
        Some("500m")
    );
    assert_eq!(
        rq.data
            .pointer("/status/used/requests.memory")
            .and_then(|v| v.as_str()),
        Some("200Mi")
    );
    assert_eq!(
        rq.data
            .pointer("/status/used/limits.cpu")
            .and_then(|v| v.as_str()),
        Some("1")
    );
    assert_eq!(
        rq.data
            .pointer("/status/used/limits.memory")
            .and_then(|v| v.as_str()),
        Some("400Mi")
    );
}

/// P0-S13-4: status.used.pods must reflect pod count immediately after reconcile is called
/// following pod creation — verifies the core counting logic used by the pod-create HTTP path.
/// Mirrors K8s conformance test resource_quota.go:280 "should create a ResourceQuota and
/// capture the life of a pod".
#[tokio::test]
async fn test_reconcile_resource_quotas_pod_create_updates_used_pods_immediately() {
    let db = crate::datastore::test_support::in_memory().await;

    db.create_resource(
        "v1",
        "ResourceQuota",
        Some("default"),
        "test-rq",
        json!({
            "apiVersion": "v1",
            "kind": "ResourceQuota",
            "metadata": {"name": "test-rq", "namespace": "default"},
            "spec": {"hard": {"pods": "4"}},
            "status": {"hard": {"pods": "4"}, "used": {"pods": "0"}}
        }),
    )
    .await
    .unwrap();

    for i in 0..3u8 {
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            &format!("pod-{i}"),
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": format!("pod-{i}"), "namespace": "default"},
                "spec": {"containers": [{"name": "c", "image": "busybox"}]}
            }),
        )
        .await
        .unwrap();
    }

    reconcile_resource_quotas_for_namespace(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        "default",
    )
    .await
    .unwrap();

    let rq = db
        .get_resource("v1", "ResourceQuota", Some("default"), "test-rq")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        rq.data
            .pointer("/status/used/pods")
            .and_then(|v| v.as_str()),
        Some("3"),
        "status.used.pods must equal 3 immediately after creating 3 pods"
    );
}

/// P0-S17-33 regression: unscoped ResourceQuota must account pod compute and extended
/// resource requests (including ephemeral-storage and custom requests.* keys).
#[tokio::test]
async fn test_reconcile_resource_quota_unscoped_pod_compute_and_extended_requests() {
    let db = crate::datastore::test_support::in_memory().await;

    db.create_resource(
        "v1",
        "ResourceQuota",
        Some("default"),
        "test-quota",
        json!({
            "apiVersion": "v1",
            "kind": "ResourceQuota",
            "metadata": {"name": "test-quota", "namespace": "default"},
            "spec": {
                "hard": {
                    "pods": "5",
                    "requests.cpu": "1",
                    "requests.memory": "500Mi",
                    "requests.ephemeral-storage": "50Gi",
                    "requests.example.com/dongle": "3",
                    "limits.cpu": "2",
                    "limits.memory": "1Gi",
                    "ephemeral-storage": "50Gi"
                }
            },
            "status": {
                "hard": {
                    "pods": "5",
                    "requests.cpu": "1",
                    "requests.memory": "500Mi",
                    "requests.ephemeral-storage": "50Gi",
                    "requests.example.com/dongle": "3",
                    "limits.cpu": "2",
                    "limits.memory": "1Gi",
                    "ephemeral-storage": "50Gi"
                },
                "used": {
                    "pods": "0",
                    "requests.cpu": "0",
                    "requests.memory": "0",
                    "requests.ephemeral-storage": "0",
                    "requests.example.com/dongle": "0",
                    "limits.cpu": "0",
                    "limits.memory": "0",
                    "ephemeral-storage": "0"
                }
            }
        }),
    )
    .await
    .unwrap();

    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "test-pod",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "test-pod", "namespace": "default"},
            "spec": {
                "containers": [{
                    "name": "pause",
                    "image": "registry.k8s.io/pause:3.10",
                    "resources": {
                        "requests": {
                            "cpu": "500m",
                            "memory": "252Mi",
                            "ephemeral-storage": "30Gi",
                            "example.com/dongle": "2"
                        },
                        "limits": {
                            "cpu": "1",
                            "memory": "400Mi"
                        }
                    }
                }]
            }
        }),
    )
    .await
    .unwrap();

    reconcile_resource_quotas_for_namespace(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        "default",
    )
    .await
    .unwrap();

    let rq = db
        .get_resource("v1", "ResourceQuota", Some("default"), "test-quota")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        rq.data
            .pointer("/status/used/pods")
            .and_then(|v| v.as_str()),
        Some("1")
    );
    assert_eq!(
        rq.data
            .pointer("/status/used/requests.cpu")
            .and_then(|v| v.as_str()),
        Some("500m")
    );
    assert_eq!(
        rq.data
            .pointer("/status/used/requests.memory")
            .and_then(|v| v.as_str()),
        Some("252Mi")
    );
    assert_eq!(
        rq.data
            .pointer("/status/used/requests.ephemeral-storage")
            .and_then(|v| v.as_str()),
        Some("30Gi")
    );
    assert_eq!(
        rq.data
            .pointer("/status/used/requests.example.com~1dongle")
            .and_then(|v| v.as_str()),
        Some("2")
    );
    assert_eq!(
        rq.data
            .pointer("/status/used/limits.cpu")
            .and_then(|v| v.as_str()),
        Some("1")
    );
    assert_eq!(
        rq.data
            .pointer("/status/used/limits.memory")
            .and_then(|v| v.as_str()),
        Some("400Mi")
    );
}

/// P0-S18-32 regression: legacy `cpu`/`memory` hard keys must be populated from pod
/// requests in status.used, matching upstream resource_quota.go:280.
#[tokio::test]
async fn test_reconcile_resource_quota_unscoped_legacy_cpu_memory_keys() {
    let db = crate::datastore::test_support::in_memory().await;

    db.create_resource(
        "v1",
        "ResourceQuota",
        Some("default"),
        "legacy-quota",
        json!({
            "apiVersion": "v1",
            "kind": "ResourceQuota",
            "metadata": {"name": "legacy-quota", "namespace": "default"},
            "spec": {
                "hard": {
                    "pods": "5",
                    "cpu": "1",
                    "memory": "500Mi",
                    "ephemeral-storage": "50Gi",
                    "requests.example.com/dongle": "3",
                    "resourcequotas": "1"
                }
            },
            "status": {
                "hard": {
                    "pods": "5",
                    "cpu": "1",
                    "memory": "500Mi",
                    "ephemeral-storage": "50Gi",
                    "requests.example.com/dongle": "3",
                    "resourcequotas": "1"
                },
                "used": {
                    "pods": "0",
                    "cpu": "0",
                    "memory": "0",
                    "ephemeral-storage": "0",
                    "requests.example.com/dongle": "0",
                    "resourcequotas": "1"
                }
            }
        }),
    )
    .await
    .unwrap();

    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "test-pod",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "test-pod", "namespace": "default"},
            "spec": {
                "containers": [{
                    "name": "pause",
                    "image": "registry.k8s.io/pause:3.10",
                    "resources": {
                        "requests": {
                            "cpu": "500m",
                            "memory": "252Mi",
                            "ephemeral-storage": "30Gi",
                            "example.com/dongle": "2"
                        }
                    }
                }]
            }
        }),
    )
    .await
    .unwrap();

    reconcile_resource_quotas_for_namespace(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        "default",
    )
    .await
    .unwrap();

    let rq = db
        .get_resource("v1", "ResourceQuota", Some("default"), "legacy-quota")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        rq.data
            .pointer("/status/used/pods")
            .and_then(|v| v.as_str()),
        Some("1")
    );
    assert_eq!(
        rq.data.pointer("/status/used/cpu").and_then(|v| v.as_str()),
        Some("500m")
    );
    assert_eq!(
        rq.data
            .pointer("/status/used/memory")
            .and_then(|v| v.as_str()),
        Some("252Mi")
    );
    assert_eq!(
        rq.data
            .pointer("/status/used/ephemeral-storage")
            .and_then(|v| v.as_str()),
        Some("30Gi")
    );
    assert_eq!(
        rq.data
            .pointer("/status/used/requests.example.com~1dongle")
            .and_then(|v| v.as_str()),
        Some("2")
    );
    assert_eq!(
        rq.data
            .pointer("/status/used/resourcequotas")
            .and_then(|v| v.as_str()),
        Some("1")
    );
}

/// P0-S18-33 regression: terminating-scoped quotas must count terminating pods and ignore
/// non-terminating pods for requests/limits accounting.
#[tokio::test]
async fn test_reconcile_resource_quota_terminating_scope_tracks_only_terminating_pods() {
    let db = crate::datastore::test_support::in_memory().await;

    for (name, scope) in [
        ("quota-terminating", "Terminating"),
        ("quota-not-terminating", "NotTerminating"),
    ] {
        db.create_resource(
            "v1",
            "ResourceQuota",
            Some("default"),
            name,
            json!({
                "apiVersion": "v1",
                "kind": "ResourceQuota",
                "metadata": {"name": name, "namespace": "default"},
                "spec": {
                    "hard": {
                        "pods": "5",
                        "requests.cpu": "1",
                        "requests.memory": "500Mi",
                        "limits.cpu": "2",
                        "limits.memory": "1Gi"
                    },
                    "scopes": [scope]
                },
                "status": {"hard": {}, "used": {}}
            }),
        )
        .await
        .unwrap();
    }

    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "long-running",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "long-running", "namespace": "default"},
            "spec": {
                "containers": [{
                    "name": "pause",
                    "image": "registry.k8s.io/pause:3.10",
                    "resources": {
                        "requests": {"cpu": "500m", "memory": "200Mi"},
                        "limits": {"cpu": "1", "memory": "400Mi"}
                    }
                }]
            }
        }),
    )
    .await
    .unwrap();

    reconcile_resource_quotas_for_namespace(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        "default",
    )
    .await
    .unwrap();

    let term = db
        .get_resource("v1", "ResourceQuota", Some("default"), "quota-terminating")
        .await
        .unwrap()
        .unwrap();
    let not_term = db
        .get_resource(
            "v1",
            "ResourceQuota",
            Some("default"),
            "quota-not-terminating",
        )
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        term.data
            .pointer("/status/used/pods")
            .and_then(|v| v.as_str()),
        Some("0")
    );
    assert_eq!(
        not_term
            .data
            .pointer("/status/used/requests.cpu")
            .and_then(|v| v.as_str()),
        Some("500m")
    );

    db.delete_resource("v1", "Pod", Some("default"), "long-running")
        .await
        .unwrap();
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "terminating",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "terminating", "namespace": "default"},
            "spec": {
                "activeDeadlineSeconds": 3600,
                "containers": [{
                    "name": "pause",
                    "image": "registry.k8s.io/pause:3.10",
                    "resources": {
                        "requests": {"cpu": "500m", "memory": "200Mi"},
                        "limits": {"cpu": "1", "memory": "400Mi"}
                    }
                }]
            }
        }),
    )
    .await
    .unwrap();

    reconcile_resource_quotas_for_namespace(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        "default",
    )
    .await
    .unwrap();

    let term = db
        .get_resource("v1", "ResourceQuota", Some("default"), "quota-terminating")
        .await
        .unwrap()
        .unwrap();
    let not_term = db
        .get_resource(
            "v1",
            "ResourceQuota",
            Some("default"),
            "quota-not-terminating",
        )
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        term.data
            .pointer("/status/used/pods")
            .and_then(|v| v.as_str()),
        Some("1")
    );
    assert_eq!(
        term.data
            .pointer("/status/used/requests.cpu")
            .and_then(|v| v.as_str()),
        Some("500m")
    );
    assert_eq!(
        term.data
            .pointer("/status/used/requests.memory")
            .and_then(|v| v.as_str()),
        Some("200Mi")
    );
    assert_eq!(
        not_term
            .data
            .pointer("/status/used/requests.cpu")
            .and_then(|v| v.as_str()),
        Some("0")
    );
    assert_eq!(
        not_term
            .data
            .pointer("/status/used/pods")
            .and_then(|v| v.as_str()),
        Some("0")
    );
}

/// P0-S13-4: status.used.pods must decrement immediately after pod deletion —
/// the background tokio::spawn that performs the actual pod removal MUST call
/// reconcile_resource_quotas_for_namespace after db.delete_resource.
/// Without this, status.used.pods stays inflated until the 30s periodic reconciler fires,
/// causing resource_quota.go:280 to time out at 300s.
#[tokio::test]
async fn test_reconcile_resource_quotas_pod_delete_decrements_used_pods_immediately() {
    let db = crate::datastore::test_support::in_memory().await;

    db.create_resource(
        "v1",
        "ResourceQuota",
        Some("default"),
        "test-rq",
        json!({
            "apiVersion": "v1",
            "kind": "ResourceQuota",
            "metadata": {"name": "test-rq", "namespace": "default"},
            "spec": {"hard": {"pods": "4"}},
            "status": {"hard": {"pods": "4"}, "used": {"pods": "0"}}
        }),
    )
    .await
    .unwrap();

    for i in 0..3u8 {
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            &format!("pod-{i}"),
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": format!("pod-{i}"), "namespace": "default"},
                "spec": {"containers": [{"name": "c", "image": "busybox"}]}
            }),
        )
        .await
        .unwrap();
    }

    reconcile_resource_quotas_for_namespace(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        "default",
    )
    .await
    .unwrap();

    // Simulate the actual pod removal that the background spawn performs
    db.delete_resource("v1", "Pod", Some("default"), "pod-0")
        .await
        .unwrap();
    db.delete_resource("v1", "Pod", Some("default"), "pod-1")
        .await
        .unwrap();

    // The background spawn must call reconcile after delete_resource
    reconcile_resource_quotas_for_namespace(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        "default",
    )
    .await
    .unwrap();

    let rq = db
        .get_resource("v1", "ResourceQuota", Some("default"), "test-rq")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        rq.data
            .pointer("/status/used/pods")
            .and_then(|v| v.as_str()),
        Some("1"),
        "status.used.pods must decrement to 1 immediately after deleting 2 of 3 pods"
    );
}

/// Regression test for P0-1: after a /status PATCH diverges Status.Hard from Spec.Hard,
/// calling reconcile must re-sync Status.Hard back to Spec.Hard.
/// This models the K8s conformance test "should apply changes to a resourcequota status".
#[tokio::test]
async fn test_reconcile_resets_status_hard_to_spec_hard_after_status_patch() {
    let db = crate::datastore::test_support::in_memory().await;

    // Create RQ with Spec.Hard = {pods: "5"}
    db.create_resource(
        "v1",
        "ResourceQuota",
        Some("default"),
        "e2e-rq",
        json!({
            "apiVersion": "v1",
            "kind": "ResourceQuota",
            "metadata": {"name": "e2e-rq", "namespace": "default"},
            "spec": {"hard": {"pods": "5"}},
            "status": {"hard": {"pods": "5"}, "used": {"pods": "0"}}
        }),
    )
    .await
    .unwrap();

    // Simulate /status PATCH: diverge Status.Hard to {pods: "10"} (different from Spec)
    let rq = db
        .get_resource("v1", "ResourceQuota", Some("default"), "e2e-rq")
        .await
        .unwrap()
        .unwrap();
    let mut patched: serde_json::Value = (*rq.data).clone();
    patched["status"]["hard"]["pods"] = json!("10");
    db.update_resource(
        "v1",
        "ResourceQuota",
        Some("default"),
        "e2e-rq",
        patched,
        rq.resource_version,
    )
    .await
    .unwrap();

    // Verify divergence was stored
    let before = db
        .get_resource("v1", "ResourceQuota", Some("default"), "e2e-rq")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        before
            .data
            .pointer("/status/hard/pods")
            .and_then(|v| v.as_str()),
        Some("10"),
        "status.hard.pods should be 10 after status patch"
    );

    // Reconcile: should reset Status.Hard back to Spec.Hard = {pods: "5"}
    reconcile_resource_quotas_for_namespace(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        "default",
    )
    .await
    .unwrap();

    let after = db
        .get_resource("v1", "ResourceQuota", Some("default"), "e2e-rq")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        after
            .data
            .pointer("/status/hard/pods")
            .and_then(|v| v.as_str()),
        Some("5"),
        "reconcile must reset status.hard.pods back to spec.hard.pods=5"
    );
}

/// Pods with deletionTimestamp set must not count against quota.
/// This mirrors upstream K8s where the quota controller excludes
/// terminating pods from status.used.
#[tokio::test]
async fn test_terminating_pod_excluded_from_pod_count() {
    let db = crate::datastore::test_support::in_memory().await;

    db.create_resource(
        "v1", "ResourceQuota", Some("default"), "test-rq",
        json!({
            "apiVersion": "v1", "kind": "ResourceQuota",
            "metadata": {"name": "test-rq", "namespace": "default"},
            "spec": {"hard": {"pods": "5", "cpu": "1", "memory": "500Mi"}},
            "status": {"hard": {"pods": "5", "cpu": "1", "memory": "500Mi"}, "used": {"pods": "0", "cpu": "0", "memory": "0"}}
        }),
    ).await.unwrap();

    // Active pod — should count
    db.create_resource(
        "v1", "Pod", Some("default"), "active-pod",
        json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "active-pod", "namespace": "default"},
            "spec": {"containers": [{"name": "c", "image": "busybox", "resources": {"requests": {"cpu": "200m", "memory": "100Mi"}}}]}
        }),
    ).await.unwrap();

    // Terminating pod (deletionTimestamp set) — must NOT count
    db.create_resource(
        "v1", "Pod", Some("default"), "terminating-pod",
        json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "terminating-pod", "namespace": "default", "deletionTimestamp": "2026-01-01T00:00:00Z"},
            "spec": {"containers": [{"name": "c", "image": "busybox", "resources": {"requests": {"cpu": "300m", "memory": "200Mi"}}}]}
        }),
    ).await.unwrap();

    reconcile_resource_quotas_for_namespace(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        "default",
    )
    .await
    .unwrap();

    let rq = db
        .get_resource("v1", "ResourceQuota", Some("default"), "test-rq")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        rq.data
            .pointer("/status/used/pods")
            .and_then(|v| v.as_str()),
        Some("1"),
        "terminating pod must be excluded from pod count"
    );
    assert_eq!(
        rq.data.pointer("/status/used/cpu").and_then(|v| v.as_str()),
        Some("200m"),
        "terminating pod CPU must be excluded"
    );
    assert_eq!(
        rq.data
            .pointer("/status/used/memory")
            .and_then(|v| v.as_str()),
        Some("100Mi"),
        "terminating pod memory must be excluded"
    );
}

/// When a pod transitions from active to terminating (deletionTimestamp added),
/// a subsequent reconcile must release all its resources from quota.
#[tokio::test]
async fn test_pod_becomes_terminating_releases_quota() {
    let db = crate::datastore::test_support::in_memory().await;

    db.create_resource(
        "v1", "ResourceQuota", Some("default"), "test-rq",
        json!({
            "apiVersion": "v1", "kind": "ResourceQuota",
            "metadata": {"name": "test-rq", "namespace": "default"},
            "spec": {"hard": {"pods": "5", "cpu": "1", "memory": "500Mi", "ephemeral-storage": "50Gi"}},
            "status": {"hard": {"pods": "5", "cpu": "1", "memory": "500Mi", "ephemeral-storage": "50Gi"}, "used": {"pods": "0", "cpu": "0", "memory": "0", "ephemeral-storage": "0"}}
        }),
    ).await.unwrap();

    db.create_resource(
        "v1", "Pod", Some("default"), "test-pod",
        json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "test-pod", "namespace": "default"},
            "spec": {"containers": [{"name": "c", "image": "busybox", "resources": {"requests": {"cpu": "500m", "memory": "252Mi", "ephemeral-storage": "30Gi"}}}]}
        }),
    ).await.unwrap();

    reconcile_resource_quotas_for_namespace(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        "default",
    )
    .await
    .unwrap();

    let rq = db
        .get_resource("v1", "ResourceQuota", Some("default"), "test-rq")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        rq.data
            .pointer("/status/used/pods")
            .and_then(|v| v.as_str()),
        Some("1")
    );
    assert_eq!(
        rq.data.pointer("/status/used/cpu").and_then(|v| v.as_str()),
        Some("500m")
    );
    assert_eq!(
        rq.data
            .pointer("/status/used/memory")
            .and_then(|v| v.as_str()),
        Some("252Mi")
    );
    assert_eq!(
        rq.data
            .pointer("/status/used/ephemeral-storage")
            .and_then(|v| v.as_str()),
        Some("30Gi")
    );

    // Simulate API delete: set deletionTimestamp
    let pod = db
        .get_resource("v1", "Pod", Some("default"), "test-pod")
        .await
        .unwrap()
        .unwrap();
    let mut updated = (*pod.data).clone();
    updated["metadata"]["deletionTimestamp"] = json!("2026-01-01T00:00:00Z");
    db.update_resource(
        "v1",
        "Pod",
        Some("default"),
        "test-pod",
        updated,
        pod.resource_version,
    )
    .await
    .unwrap();

    // Reconcile again (side effect fires after deletionTimestamp is set)
    reconcile_resource_quotas_for_namespace(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        "default",
    )
    .await
    .unwrap();

    let rq = db
        .get_resource("v1", "ResourceQuota", Some("default"), "test-rq")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        rq.data
            .pointer("/status/used/pods")
            .and_then(|v| v.as_str()),
        Some("0"),
        "pods must be 0 after pod becomes terminating"
    );
    assert_eq!(
        rq.data.pointer("/status/used/cpu").and_then(|v| v.as_str()),
        Some("0"),
        "cpu must be 0 after pod becomes terminating"
    );
    assert_eq!(
        rq.data
            .pointer("/status/used/memory")
            .and_then(|v| v.as_str()),
        Some("0"),
        "memory must be 0 after pod becomes terminating"
    );
    assert_eq!(
        rq.data
            .pointer("/status/used/ephemeral-storage")
            .and_then(|v| v.as_str()),
        Some("0"),
        "ephemeral-storage must be 0 after pod becomes terminating"
    );
}

/// Unit test for pod_has_deletion_timestamp helper.
#[test]
fn test_pod_has_deletion_timestamp_helper() {
    let with_ts = json!({"metadata": {"deletionTimestamp": "2026-01-01T00:00:00Z"}});
    let with_empty = json!({"metadata": {"deletionTimestamp": ""}});
    let without = json!({"metadata": {"name": "foo"}});
    let no_meta = json!({"spec": {}});

    assert!(pod_has_deletion_timestamp(&with_ts));
    assert!(!pod_has_deletion_timestamp(&with_empty));
    assert!(!pod_has_deletion_timestamp(&without));
    assert!(!pod_has_deletion_timestamp(&no_meta));
}
