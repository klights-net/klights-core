#[cfg(test)]
mod tests {
    use super::super::assembly_support::support::*;

    #[tokio::test]
    async fn local_status_with_outbox_persists_locally_without_enqueuing_worker_command() {
        let repo = IntegrationPodWorkerScenarioFixture::new_with_node_outbox().await;
        repo.persistence
            .seed_pod(
                "default",
                "outbox-status",
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "namespace": "default",
                        "name": "outbox-status",
                        "uid": "uid-outbox-status"
                    },
                    "spec": {"containers": [{"name": "app", "image": "nginx"}]},
                    "status": {"phase": "Pending"}
                }),
            )
            .await
            .expect("create pod");

        let returned = repo
            .status_ports()
            .set_pod_status_for_uid(
                "default",
                "outbox-status",
                "uid-outbox-status",
                super::super::assembly_support::support::PodStatusUpdate {
                    phase: "Running".to_string(),
                    pod_ip: klights_kubelet::pod_repository::PublishedAddress::must("10.42.0.8"),
                    host_ip: klights_kubelet::pod_repository::PublishedAddress::must("192.0.2.10"),
                    container_statuses: vec![],
                    init_container_statuses: None,
                    qos_class: None,
                },
                None,
            )
            .await
            .expect("enqueue status");

        assert_eq!(
            returned
                .data
                .pointer("/status/phase")
                .and_then(|value| value.as_str()),
            Some("Running"),
            "kubelet callers receive a synthetic status view for local follow-up"
        );
        let stored = repo
            .query
            .get_pod_by_name("default", "outbox-status")
            .await
            .expect("read stored pod")
            .expect("pod exists");
        assert_eq!(
            stored
                .data
                .pointer("/status/phase")
                .and_then(|value| value.as_str()),
            Some("Running"),
            "local role must persist status even when a node outbox is available"
        );
        let row = repo
            .claim_next_due_outbox(i64::MAX / 4, 1_000, "assert")
            .await
            .expect("claim outbox");
        assert!(
            row.is_none(),
            "outbox availability must not select remote delivery for a local role"
        );
    }
    #[tokio::test]
    async fn kubelet_pod_reader_uses_leader_api_when_configured() {
        let pod = klights_cluster_core::Resource {
            id: 1,
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "leader-only".to_string(),
            uid: "uid-leader-only".to_string(),
            resource_version: 42,
            data: Arc::new(json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "leader-only",
                    "uid": "uid-leader-only",
                    "resourceVersion": "42"
                },
                "spec": {"nodeName": "node-a", "containers": [{"name": "app", "image": "nginx"}]}
            })),
        };
        let repo =
            IntegrationPodWorkerFixture::new(Arc::new(FakeLeaderApiClient::new(pod.clone()))).await;

        let got = repo
            .query
            .get_pod_by_name("default", "leader-only")
            .await
            .expect("leader pod read");
        assert_eq!(got.map(|pod| pod.uid), Some("uid-leader-only".to_string()));
    }
    #[tokio::test]
    async fn kubelet_pod_reader_uses_fresh_leader_api_for_single_pod_reads() {
        let stale_pod = klights_cluster_core::Resource {
            id: 1,
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "worker-probed".to_string(),
            uid: "uid-worker-probed".to_string(),
            resource_version: 11,
            data: Arc::new(json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "worker-probed",
                    "uid": "uid-worker-probed",
                    "resourceVersion": "11"
                },
                "spec": {
                    "nodeName": "worker-a",
                    "containers": [{"name": "web", "image": "registry.k8s.io/e2e-test-images/agnhost:2.56"}]
                },
                "status": {"phase": "Pending"}
            })),
        };
        let fresh_pod = klights_cluster_core::Resource {
            resource_version: 12,
            data: Arc::new(json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "worker-probed",
                    "uid": "uid-worker-probed",
                    "resourceVersion": "12"
                },
                "spec": {
                    "nodeName": "worker-a",
                    "containers": [{"name": "web", "image": "registry.k8s.io/e2e-test-images/agnhost:2.56"}]
                },
                "status": {
                    "phase": "Running",
                    "podIP": "10.50.2.2",
                    "containerStatuses": [{
                        "name": "web",
                        "containerID": "containerd://running-container",
                        "ready": false,
                        "started": true,
                        "state": {"running": {"startedAt": "2026-05-18T19:35:03Z"}}
                    }]
                }
            })),
            ..stale_pod.clone()
        };
        let repo = IntegrationPodWorkerFixture::new(Arc::new(
            FakeLeaderApiClient::new(stale_pod).with_fresh_pod(fresh_pod),
        ))
        .await;

        let got = repo
            .query
            .get_pod_by_name("default", "worker-probed")
            .await
            .expect("fresh leader pod read")
            .expect("pod exists");
        assert_eq!(
            got.data
                .pointer("/status/phase")
                .and_then(|value| value.as_str()),
            Some("Running"),
            "probe and lifecycle code must not make decisions from a stale informer-cache pod"
        );

        let got_for_uid = repo
            .query
            .get_pod_for_uid("default", "worker-probed", "uid-worker-probed")
            .await
            .expect("fresh uid-bound pod read")
            .expect("pod exists for uid");
        assert_eq!(
            got_for_uid
                .data
                .pointer("/status/containerStatuses/0/state/running/startedAt")
                .and_then(|value| value.as_str()),
            Some("2026-05-18T19:35:03Z"),
            "uid-bound reads need the same fresh status for readiness-probe initialDelaySeconds"
        );
    }
    #[tokio::test]
    async fn runtime_reconcile_reads_pending_status_checkpoint_from_node_db() {
        let stale_pod = klights_cluster_core::Resource {
            id: 1,
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "checkpoint-status".to_string(),
            uid: "uid-checkpoint-status".to_string(),
            resource_version: 12,
            data: Arc::new(json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "checkpoint-status",
                    "uid": "uid-checkpoint-status",
                    "resourceVersion": "12"
                },
                "spec": {
                    "nodeName": "node-a",
                    "containers": [{"name": "app", "image": "nginx"}]
                },
                "status": {"phase": "Pending"}
            })),
        };
        let repo =
            IntegrationPodWorkerFixture::new(Arc::new(FakeLeaderApiClient::new(stale_pod))).await;

        repo.set_pod_status_for_uid(
            "default",
            "checkpoint-status",
            "uid-checkpoint-status",
            super::super::assembly_support::support::PodStatusUpdate {
                phase: "Pending".to_string(),
                pod_ip: klights_kubelet::pod_repository::PublishedAddress::must("10.42.0.9"),
                host_ip: klights_kubelet::pod_repository::PublishedAddress::must("192.0.2.9"),
                container_statuses: vec![],
                init_container_statuses: None,
                qos_class: None,
            },
            None,
        )
        .await
        .expect("enqueue podIP status");

        let reconciled = repo
            .apply_runtime_reconcile_status_for_uid(
                "default",
                "checkpoint-status",
                "uid-checkpoint-status",
                super::super::assembly_support::support::RuntimeReconcileStatus {
                    phase: "Running".to_string(),
                    container_statuses: vec![json!({
                        "name": "app",
                        "ready": false,
                        "restartCount": 0,
                        "state": {"running": {"startedAt": "2026-06-01T00:00:00Z"}}
                    })],
                },
                None,
            )
            .await
            .expect("runtime reconcile should use checkpointed podIP");

        assert_eq!(
            reconciled
                .data
                .pointer("/status/phase")
                .and_then(|value| value.as_str()),
            Some("Running"),
            "runtime reconcile must not defer Running when node.db has the prior podIP status"
        );
        assert_eq!(
            reconciled
                .data
                .pointer("/status/podIP")
                .and_then(|value| value.as_str()),
            Some("10.42.0.9")
        );
    }
    #[tokio::test]
    async fn get_pod_for_uid_overlays_local_status_checkpoint_for_read_your_own_write() {
        let stale_pod = klights_cluster_core::Resource {
            id: 1,
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "ryow-pod".to_string(),
            uid: "uid-ryow".to_string(),
            resource_version: 12,
            data: Arc::new(json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "ryow-pod",
                    "uid": "uid-ryow",
                    "resourceVersion": "12"
                },
                "spec": {
                    "nodeName": "node-a",
                    "containers": [{"name": "app", "image": "nginx"}]
                },
                "status": {"phase": "Pending"}
            })),
        };
        let repo =
            IntegrationPodWorkerFixture::new(Arc::new(FakeLeaderApiClient::new(stale_pod))).await;

        // Worker records its own authoritative status node-locally: podIP from CNI,
        // then Running from runtime reconcile. The leader copy stays Pending (the
        // fake leader keeps returning the stale rv=12 Pending object).
        repo.set_pod_status_for_uid(
            "default",
            "ryow-pod",
            "uid-ryow",
            super::super::assembly_support::support::PodStatusUpdate {
                phase: "Pending".to_string(),
                pod_ip: klights_kubelet::pod_repository::PublishedAddress::must("10.42.0.9"),
                host_ip: klights_kubelet::pod_repository::PublishedAddress::must("192.0.2.9"),
                container_statuses: vec![],
                init_container_statuses: None,
                qos_class: None,
            },
            None,
        )
        .await
        .expect("record podIP checkpoint");
        repo.apply_runtime_reconcile_status_for_uid(
            "default",
            "ryow-pod",
            "uid-ryow",
            super::super::assembly_support::support::RuntimeReconcileStatus {
                phase: "Running".to_string(),
                container_statuses: vec![json!({
                    "name": "app",
                    "ready": true,
                    "restartCount": 0,
                    "state": {"running": {"startedAt": "2026-06-01T00:00:00Z"}}
                })],
            },
            None,
        )
        .await
        .expect("record Running checkpoint");

        // The confirm read must overlay the node-local checkpoint, not return the
        // stale leader Pending. This is the read finalize_startup depends on.
        let read = repo
            .query
            .get_pod_for_uid("default", "ryow-pod", "uid-ryow")
            .await
            .expect("get_pod_for_uid")
            .expect("pod present");
        assert_eq!(
            read.data.pointer("/status/phase").and_then(|v| v.as_str()),
            Some("Running"),
            "get_pod_for_uid must overlay the node-local Running checkpoint (read-your-own-write), \
             not return the stale leader Pending copy"
        );
        assert_eq!(
            read.data.pointer("/status/podIP").and_then(|v| v.as_str()),
            Some("10.42.0.9"),
            "get_pod_for_uid must overlay the node-local podIP checkpoint"
        );
    }
    #[tokio::test]
    async fn outbox_status_reads_current_pod_through_leader_api() {
        let pod = klights_cluster_core::Resource {
            id: 1,
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "leader-status".to_string(),
            uid: "uid-leader-status".to_string(),
            resource_version: 7,
            data: Arc::new(json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "leader-status",
                    "uid": "uid-leader-status",
                    "resourceVersion": "7"
                },
                "spec": {"nodeName": "node-a", "containers": [{"name": "app", "image": "nginx"}]},
                "status": {"phase": "Pending"}
            })),
        };
        let repo = IntegrationPodWorkerFixture::new(Arc::new(FakeLeaderApiClient::new(pod))).await;

        let returned = repo
            .set_pod_status_for_uid(
                "default",
                "leader-status",
                "uid-leader-status",
                super::super::assembly_support::support::PodStatusUpdate {
                    phase: "Running".to_string(),
                    pod_ip: klights_kubelet::pod_repository::PublishedAddress::must("10.42.0.9"),
                    host_ip: klights_kubelet::pod_repository::PublishedAddress::must("192.0.2.20"),
                    container_statuses: vec![],
                    init_container_statuses: None,
                    qos_class: None,
                },
                None,
            )
            .await
            .expect("enqueue status from leader snapshot");

        assert_eq!(
            returned
                .data
                .pointer("/status/phase")
                .and_then(|value| value.as_str()),
            Some("Running")
        );
        assert!(
            repo.claim_next_due_outbox(i64::MAX / 4, 1_000, "assert")
                .await
                .expect("claim outbox")
                .is_some()
        );
    }
    #[tokio::test]
    async fn outbox_sandbox_annotation_uses_leader_api_and_outbox() {
        let pod = klights_cluster_core::Resource {
            id: 1,
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "leader-sandbox".to_string(),
            uid: "uid-leader-sandbox".to_string(),
            resource_version: 11,
            data: Arc::new(json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "leader-sandbox",
                    "uid": "uid-leader-sandbox",
                    "resourceVersion": "11"
                },
                "spec": {"nodeName": "node-a", "containers": [{"name": "app", "image": "nginx"}]}
            })),
        };
        let repo = IntegrationPodWorkerFixture::new(Arc::new(FakeLeaderApiClient::new(pod))).await;

        let returned = repo
            .record_sandbox_id_for_uid(
                "default",
                "leader-sandbox",
                "uid-leader-sandbox",
                "sandbox-abc",
            )
            .await
            .expect("enqueue sandbox annotation");

        assert_eq!(
            returned
                .data
                .pointer("/metadata/annotations/klights.dev~1sandbox-id")
                .and_then(|value| value.as_str()),
            Some("sandbox-abc")
        );
        let row = repo
            .claim_next_due_outbox(i64::MAX / 4, 1_000, "assert")
            .await
            .expect("claim outbox")
            .expect("metadata row enqueued");
        assert_eq!(row.operation, "PodMetadata");
        assert_eq!(row.pod_uid, "uid-leader-sandbox");
        assert_eq!(
            row.command,
            super::super::assembly_support::support::PodOutboxCommand::SandboxAnnotationPatch {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: Some("default".to_string()),
                name: "leader-sandbox".to_string(),
                patch_kind: klights_cluster_core::PatchKind::Merge,
                pod_uid: "uid-leader-sandbox".to_string(),
                resource_version: 11,
                strict_resource_version: true,
                sandbox_id: "sandbox-abc".to_string(),
            },
            "worker sandbox publication must enqueue only its owned annotation patch"
        );
    }
    #[tokio::test]
    async fn controller_owner_reference_update_commits_to_leader_store_not_node_outbox() {
        let repo = IntegrationPodWorkerScenarioFixture::new_with_node_outbox().await;
        let _created = repo
            .persistence
            .seed_pod(
                "default",
                "controller-owned",
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "namespace": "default",
                        "name": "controller-owned",
                        "uid": "uid-controller-owned",
                        "labels": {"app": "rc"}
                    },
                    "spec": {
                        "nodeName": "worker-b",
                        "containers": [{"name": "app", "image": "nginx"}]
                    }
                }),
            )
            .await
            .expect("create controller-owned Pod");
        let owner = json!({
            "apiVersion": "v1",
            "kind": "ReplicationController",
            "name": "rc",
            "uid": "uid-rc",
            "controller": true,
            "blockOwnerDeletion": true
        });

        repo.update_pod_owner_references("default", "controller-owned", vec![owner.clone()])
            .await
            .expect("leader controller owner-reference update");

        let live = repo
            .query
            .get_pod_by_name("default", "controller-owned")
            .await
            .expect("read Pod")
            .expect("Pod remains");
        assert_eq!(
            live.data.pointer("/metadata/ownerReferences/0"),
            Some(&owner),
            "controller metadata must be committed through the leader store"
        );
        assert!(
            repo.claim_next_due_outbox(i64::MAX / 4, 1_000, "assert")
                .await
                .expect("inspect node outbox")
                .is_none(),
            "leader controller writes must not enter the node-authenticated worker outbox"
        );
    }
    #[tokio::test]
    async fn non_leader_pod_object_writer_without_outbox_retries_later() {
        let pod = klights_cluster_core::Resource {
            id: 1,
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "leader-metadata-no-outbox".to_string(),
            uid: "uid-leader-metadata-no-outbox".to_string(),
            resource_version: 12,
            data: Arc::new(json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "leader-metadata-no-outbox",
                    "uid": "uid-leader-metadata-no-outbox",
                    "resourceVersion": "12"
                },
                "spec": {"nodeName": "node-a", "containers": [{"name": "app", "image": "nginx"}]},
                "status": {"phase": "Pending"}
            })),
        };
        let repo = IntegrationPodWorkerScenarioFixture::new_cluster_backed(Arc::new(
            FakeLeaderApiClient::new(pod),
        ))
        .await;

        let owner_result = repo
            .update_pod_owner_references_for_uid(
                "default",
                "leader-metadata-no-outbox",
                "uid-leader-metadata-no-outbox",
                vec![json!({"apiVersion": "v1", "kind": "ReplicaSet", "name": "rs", "uid": "uid-rs"})],
            )
            .await;
        assert!(
            owner_result.is_err(),
            "owner reference update must be rejected without outbox"
        );
        assert!(
            owner_result.unwrap_err().to_string().contains("outbox"),
            "missing outbox should return retry guidance"
        );

        let labels_result = repo
            .merge_pod_labels_for_uid(
                "default",
                "leader-metadata-no-outbox",
                "uid-leader-metadata-no-outbox",
                vec![("app".to_string(), "changed".to_string())],
            )
            .await;
        assert!(
            labels_result.is_err(),
            "label merge must be rejected without outbox"
        );
        assert!(
            labels_result.unwrap_err().to_string().contains("outbox"),
            "missing outbox should return retry guidance"
        );

        let sandbox_result = repo
            .record_sandbox_id_for_uid(
                "default",
                "leader-metadata-no-outbox",
                "uid-leader-metadata-no-outbox",
                "sandbox-missing-outbox",
            )
            .await;
        assert!(
            sandbox_result.is_err(),
            "sandbox annotation must be rejected without outbox"
        );
        assert!(
            sandbox_result.unwrap_err().to_string().contains("outbox"),
            "missing outbox should return retry guidance"
        );

        let live = repo
            .query
            .get_pod_by_name("default", "leader-metadata-no-outbox")
            .await
            .unwrap()
            .unwrap();
        assert!(
            live.data["metadata"].get("labels").is_none(),
            "labels should remain unchanged in local DB when non-leader outbox is unavailable"
        );
        assert!(
            live.data["metadata"]
                .get("annotations")
                .and_then(|annotations| annotations.get("klights.dev/sandbox-id"))
                .is_none(),
            "sandbox id annotation should remain absent in local DB when non-leader outbox is unavailable"
        );
        let status = live.data;
        assert!(
            status["metadata"].get("ownerReferences").is_none(),
            "owner references should not be persisted without outbox"
        );
        assert!(
            status["metadata"].get("labels").is_none(),
            "labels should not be changed without outbox"
        );
    }
    #[tokio::test]
    async fn non_leader_pod_status_writer_without_outbox_retries_later() {
        let pod = klights_cluster_core::Resource {
            id: 1,
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "leader-status-no-outbox".to_string(),
            uid: "uid-leader-status-no-outbox".to_string(),
            resource_version: 7,
            data: Arc::new(json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "leader-status-no-outbox",
                    "uid": "uid-leader-status-no-outbox",
                    "resourceVersion": "7"
                },
                "spec": {"nodeName": "node-a", "containers": [{"name": "app", "image": "nginx"}]},
                "status": {"phase": "Pending"}
            })),
        };
        let repo = IntegrationPodWorkerScenarioFixture::new_cluster_backed(Arc::new(
            FakeLeaderApiClient::new(pod),
        ))
        .await;

        let status_res = repo
            .status_ports()
            .set_pod_status_for_uid(
                "default",
                "leader-status-no-outbox",
                "uid-leader-status-no-outbox",
                super::super::assembly_support::support::PodStatusUpdate {
                    phase: "Running".to_string(),
                    pod_ip: klights_kubelet::pod_repository::PublishedAddress::must("10.0.0.11"),
                    host_ip: klights_kubelet::pod_repository::PublishedAddress::must("192.0.2.30"),
                    container_statuses: vec![],
                    init_container_statuses: None,
                    qos_class: None,
                },
                None,
            )
            .await;
        assert!(
            status_res.is_err(),
            "status update must be rejected without outbox"
        );
        assert!(
            status_res.unwrap_err().to_string().contains("outbox"),
            "missing outbox should return retry guidance"
        );

        let live = repo
            .query
            .get_pod_by_name("default", "leader-status-no-outbox")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            live.data["status"]["phase"].as_str(),
            Some("Pending"),
            "status phase should remain unchanged in local DB without outbox"
        );
    }
    #[tokio::test]
    async fn worker_actor_finalization_enqueues_uid_qualified_pod_delete_outbox() {
        let pod = klights_cluster_core::Resource {
            id: 1,
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "leader-finalize".to_string(),
            uid: "uid-leader-finalize".to_string(),
            resource_version: 13,
            data: Arc::new(json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "leader-finalize",
                    "uid": "uid-leader-finalize",
                    "resourceVersion": "13",
                    "deletionTimestamp": "2026-05-13T00:00:00Z",
                    "deletionGracePeriodSeconds": 0
                },
                "spec": {"nodeName": "worker-1", "containers": [{"name": "app", "image": "nginx"}]},
                "status": {"phase": "Running"}
            })),
        };
        let repo = IntegrationPodWorkerFixture::new(Arc::new(FakeLeaderApiClient::new(pod))).await;

        let finalized = repo
            .finalize_pod_deletion_after_actor_cleanup(
                "default",
                "leader-finalize",
                "uid-leader-finalize",
            )
            .await
            .expect("worker finalization should enqueue a leader delete");

        assert_eq!(
            finalized,
            super::super::assembly_support::support::PodFinalizationOutcome::Queued,
            "remote acceptance only durably queues FinalizeBoundPod; the actor must retry \
             until a committed delete watch makes the row absent"
        );
        let row = repo
            .claim_next_due_outbox(i64::MAX / 4, 1_000, "assert")
            .await
            .expect("claim outbox")
            .expect("delete row enqueued");
        assert_eq!(row.operation, "PodMetadata");
        assert_eq!(row.pod_uid, "uid-leader-finalize");
        match &row.command {
            super::super::assembly_support::support::PodOutboxCommand::FinalizeBoundPod {
                namespace,
                name,
                pod_uid,
                node_name,
                observed_resource_version,
            } => {
                assert_eq!(namespace, "default");
                assert_eq!(name, "leader-finalize");
                assert_eq!(pod_uid, "uid-leader-finalize");
                assert_eq!(node_name, "worker-1");
                assert!(
                    *observed_resource_version > 0,
                    "actor finalization must carry its leader-fresh Pod generation"
                );
            }
            other => panic!("expected FinalizeBoundPod outbox command, got {other:?}"),
        }
        let delivery =
            super::super::assembly_support::support::run_worker_actor_finalization_delivery_scenario()
                .await
                .expect("apply actor-authored command through committed outbox reducer");
        assert!(delivery.queued);
        assert!(delivery.exact_uid_bound_command);
        assert!(delivery.committed_resource_receipt);
        assert!(delivery.authoritative_pod_removed);
    }
    #[tokio::test]
    async fn worker_actor_finalization_preserves_checkpoint_until_committed_removal() {
        let pod = klights_cluster_core::Resource {
            id: 1,
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "finalize-checkpoint".to_string(),
            uid: "uid-finalize-checkpoint".to_string(),
            resource_version: 13,
            data: Arc::new(json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "finalize-checkpoint",
                    "uid": "uid-finalize-checkpoint",
                    "resourceVersion": "13",
                    "deletionTimestamp": "2026-06-14T00:00:00Z",
                    "deletionGracePeriodSeconds": 0
                },
                "spec": {"nodeName": "worker-1", "containers": [{"name": "app", "image": "nginx"}]},
                "status": {"phase": "Running", "podIP": "10.42.0.7"}
            })),
        };
        let repo = IntegrationPodWorkerFixture::new(Arc::new(FakeLeaderApiClient::new(pod))).await;
        repo.seed_status_checkpoint(
            "default",
            "finalize-checkpoint",
            "uid-finalize-checkpoint",
            13,
            json!({"phase": "Running", "podIP": "10.42.0.7"}),
            100,
        )
        .await
        .expect("seed status checkpoint");

        let finalized = repo
            .finalize_pod_deletion_after_actor_cleanup(
                "default",
                "finalize-checkpoint",
                "uid-finalize-checkpoint",
            )
            .await
            .expect("actor finalization should enqueue delete and remain pending");

        assert_eq!(
            finalized,
            super::super::assembly_support::support::PodFinalizationOutcome::Queued,
            "remote acceptance must not complete actor finalization before committed removal"
        );
        assert!(
            repo.has_status_checkpoint("uid-finalize-checkpoint")
                .await
                .expect("read status checkpoint"),
            "the UID-scoped checkpoint must remain until committed removal completes the actor"
        );
    }
    #[tokio::test]
    async fn worker_actor_finalization_uses_fresh_leader_read_before_emitting_finalize() {
        let stale_pod = klights_cluster_core::Resource {
            id: 1,
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "stale-finalize".to_string(),
            uid: "uid-stale-finalize".to_string(),
            resource_version: 13,
            data: Arc::new(json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "stale-finalize",
                    "uid": "uid-stale-finalize",
                    "resourceVersion": "13"
                },
                "spec": {"nodeName": "worker-1", "containers": [{"name": "app", "image": "nginx"}]},
                "status": {"phase": "Running"}
            })),
        };
        let mut fresh_pod = stale_pod.clone();
        fresh_pod.resource_version = 14;
        let mut fresh_data = (*fresh_pod.data).clone();
        fresh_data["metadata"]["resourceVersion"] = json!("14");
        fresh_data["metadata"]["deletionTimestamp"] = json!("2026-05-15T01:37:41Z");
        fresh_data["metadata"]["deletionGracePeriodSeconds"] = json!(0);
        fresh_pod.data = Arc::new(fresh_data);
        let repo = IntegrationPodWorkerFixture::new(Arc::new(
            FakeLeaderApiClient::new(stale_pod).with_fresh_pod(fresh_pod),
        ))
        .await;

        let finalized = repo
            .finalize_pod_deletion_after_actor_cleanup(
                "default",
                "stale-finalize",
                "uid-stale-finalize",
            )
            .await
            .expect("fresh terminating leader read should allow opaque finalization");

        assert_eq!(
            finalized,
            super::super::assembly_support::support::PodFinalizationOutcome::Queued,
            "fresh leader observation only queues the exact-RV command; committed removal is still pending"
        );
        let row = repo
            .claim_next_due_outbox(i64::MAX / 4, 1_000, "assert")
            .await
            .expect("claim outbox");
        assert!(
            row.is_some(),
            "stale cached non-terminating Pod must not suppress FinalizeBoundPod emission"
        );
    }
    #[tokio::test]
    async fn worker_actor_finalization_serializes_same_uid_write_without_actor_retry() {
        let outcome = super::super::assembly_support::support::run_worker_actor_finalization_race()
            .await
            .expect("run same-UID finalization race");
        assert!(
            outcome.initially_pending,
            "queued finalization must keep the actor pending"
        );
        assert!(outcome.resource_version_advanced);
        assert!(outcome.dispatched);
        assert!(
            outcome.removed_after_dispatch,
            "the first semantic command must serialize after the status write and remove the Pod"
        );
        assert!(
            outcome.completed_after_committed_absence,
            "only committed absence may complete the actor"
        );
        assert!(
            outcome.node_mismatch_rejected,
            "a fixed worker-1 identity must not authorize worker-2 finalization"
        );
    }
}
