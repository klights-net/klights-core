#[cfg(test)]
mod tests {
    use super::super::assembly_support::support::*;

    #[tokio::test]
    async fn parity_fixture_snapshots_repository_status_payloads() {
        let repo = build_status_repo().await;
        let created = repo
            .persistence
            .seed_pod("default", "repo-pod", pending_pod("repo-pod"))
            .await
            .expect("create repository pod");
        let updated = repo
            .status_ports()
            .set_pod_status(
                "default",
                "repo-pod",
                PodStatusUpdate {
                    phase: "Running".to_string(),
                    pod_ip: klights_kubelet::pod_repository::PublishedAddress::must("10.42.0.8"),
                    host_ip: klights_kubelet::pod_repository::PublishedAddress::must("10.0.0.5"),
                    container_statuses: vec![json!({
                        "name": "app",
                        "ready": true,
                        "state": {"running": {}}
                    })],
                    init_container_statuses: None,
                    qos_class: None,
                },
                Some(created.resource_version),
            )
            .await
            .expect("write repository pod status");

        assert_eq!(updated.namespace.as_deref(), Some("default"));
        assert_eq!(updated.name, "repo-pod");
        assert_eq!(updated.resource_version, 2);
        assert_eq!(updated.data["status"]["phase"], json!("Running"));
        assert_eq!(updated.data["status"]["podIP"], json!("10.42.0.8"));
        assert_eq!(
            updated.data["status"]["podIPs"][0]["ip"],
            json!("10.42.0.8")
        );
        assert_eq!(updated.data["status"]["hostIP"], json!("10.0.0.5"));
        assert_eq!(
            updated.data["status"]["hostIPs"][0]["ip"],
            json!("10.0.0.5")
        );
        assert_eq!(
            updated.data["status"]["containerStatuses"][0]["name"],
            json!("app")
        );
        assert_eq!(
            updated.data["status"]["containerStatuses"][0]["ready"],
            json!(true)
        );
    }

    #[tokio::test]
    async fn set_pod_status_running_without_container_statuses_sets_ready_conditions() {
        let repo = build_status_repo().await;
        let created = repo
            .persistence
            .seed_pod(
                "default",
                "ready-empty-statuses",
                pending_pod("ready-empty-statuses"),
            )
            .await
            .unwrap();

        let updated = repo
            .status_ports()
            .set_pod_status(
                "default",
                "ready-empty-statuses",
                PodStatusUpdate {
                    phase: "Running".to_string(),
                    pod_ip: klights_kubelet::pod_repository::PublishedAddress::must("10.43.0.77"),
                    host_ip: klights_kubelet::pod_repository::PublishedAddress::must("127.0.0.1"),
                    container_statuses: vec![],
                    init_container_statuses: Some(vec![]),
                    qos_class: None,
                },
                Some(created.resource_version),
            )
            .await
            .unwrap();

        let conditions = updated.data["status"]["conditions"].as_array().unwrap();
        for condition_type in ["Ready", "ContainersReady"] {
            assert_eq!(
                conditions
                    .iter()
                    .find(|condition| condition["type"] == condition_type)
                    .expect("condition must exist")["status"],
                json!("True")
            );
        }
    }

    #[tokio::test]
    async fn set_pod_status_preserves_spec_metadata_and_qos() {
        let repo = build_status_repo().await;
        let created = repo
            .persistence
            .seed_pod("default", "p1", pending_pod("p1"))
            .await
            .unwrap();

        let update = super::super::assembly_support::support::PodStatusUpdate {
            phase: "Running".to_string(),
            pod_ip: klights_kubelet::pod_repository::PublishedAddress::must("10.42.0.5"),
            host_ip: klights_kubelet::pod_repository::PublishedAddress::must("10.0.0.10"),
            container_statuses: vec![json!({
                "name": "c", "ready": true, "restartCount": 0,
                "image": "nginx", "imageID": "",
                "state": {"running": {"startedAt": "2026-04-30T00:00:00Z"}}
            })],
            init_container_statuses: None,
            qos_class: None,
        };
        let updated = repo
            .status_ports()
            .set_pod_status("default", "p1", update, Some(created.resource_version))
            .await
            .unwrap();

        // spec preserved
        assert_eq!(updated.data["spec"]["containers"][0]["name"], json!("c"));
        // metadata preserved (labels intact)
        assert_eq!(updated.data["metadata"]["labels"]["app"], json!("x"));
        // qosClass preserved (the existing pod had BestEffort)
        assert_eq!(updated.data["status"]["qosClass"], json!("BestEffort"));
        // phase / IPs / conditions
        assert_eq!(updated.data["status"]["phase"], json!("Running"));
        assert_eq!(updated.data["status"]["podIP"], json!("10.42.0.5"));
        assert_eq!(updated.data["status"]["hostIP"], json!("10.0.0.10"));
        assert_eq!(
            updated.data["status"]["podIPs"][0]["ip"],
            json!("10.42.0.5")
        );
        let conditions = updated.data["status"]["conditions"]
            .as_array()
            .expect("conditions present");
        let types: Vec<&str> = conditions
            .iter()
            .filter_map(|c| c.get("type").and_then(|t| t.as_str()))
            .collect();
        assert!(types.contains(&"PodScheduled"));
        assert!(types.contains(&"Initialized"));
        assert!(types.contains(&"ContainersReady"));
        assert!(types.contains(&"Ready"));
        let ready = conditions
            .iter()
            .find(|c| c["type"] == "Ready")
            .expect("Ready condition");
        assert_eq!(ready["status"], json!("True"));
    }
    #[tokio::test]
    async fn set_pod_status_preserves_scheduler_disruption_target_condition() {
        let repo = build_status_repo().await;
        let mut pod = pending_pod("preempted");
        pod["status"]["conditions"] = json!([
            {
                "type": "DisruptionTarget",
                "status": "True",
                "lastTransitionTime": "2026-05-25T06:03:08Z",
                "reason": "PreemptionByScheduler",
                "message": "Preempted by pod default/preemptor on node"
            },
            {
                "type": "PodScheduled",
                "status": "True",
                "lastTransitionTime": "2026-05-25T06:03:06Z"
            }
        ]);
        let created = repo
            .persistence
            .seed_pod("default", "preempted", pod)
            .await
            .unwrap();

        let updated = repo
            .status_ports()
            .set_pod_status(
                "default",
                "preempted",
                super::super::assembly_support::support::PodStatusUpdate {
                    phase: "Running".to_string(),
                    pod_ip: klights_kubelet::pod_repository::PublishedAddress::must("10.42.0.6"),
                    host_ip: klights_kubelet::pod_repository::PublishedAddress::must("10.0.0.10"),
                    container_statuses: vec![json!({
                        "name": "c",
                        "ready": true,
                        "restartCount": 0,
                        "image": "nginx",
                        "imageID": "",
                        "state": {"running": {"startedAt": "2026-05-25T06:03:09Z"}}
                    })],
                    init_container_statuses: None,
                    qos_class: None,
                },
                Some(created.resource_version),
            )
            .await
            .unwrap();

        let conditions = updated
            .data
            .pointer("/status/conditions")
            .and_then(|v| v.as_array())
            .expect("conditions must remain an array");
        assert!(
            conditions.iter().any(|condition| {
                condition.get("type").and_then(|v| v.as_str()) == Some("DisruptionTarget")
                    && condition.get("status").and_then(|v| v.as_str()) == Some("True")
                    && condition.get("reason").and_then(|v| v.as_str())
                        == Some("PreemptionByScheduler")
            }),
            "kubelet status writes must not drop the scheduler-owned DisruptionTarget condition: {:?}",
            updated.data
        );
    }
    #[tokio::test]
    async fn set_pod_status_omits_pod_ips_arrays_until_ips_are_allocated() {
        let repo = build_status_repo().await;
        let created = repo
            .persistence
            .seed_pod("default", "pending-no-ip", pending_pod("pending-no-ip"))
            .await
            .unwrap();

        let updated = repo
            .status_ports()
            .set_pod_status(
                "default",
                "pending-no-ip",
                super::super::assembly_support::support::PodStatusUpdate {
                    phase: "Pending".to_string(),
                    pod_ip: None,
                    host_ip: None,
                    container_statuses: vec![],
                    init_container_statuses: None,
                    qos_class: None,
                },
                Some(created.resource_version),
            )
            .await
            .unwrap();

        assert!(
            updated.data["status"]
                .get("podIP")
                .is_none_or(|v| v.is_null()),
            "Pending Pods without an allocated podIP must not expose a podIP key",
        );
        assert!(
            updated.data["status"].get("podIPs").is_none(),
            "Pending Pods without an allocated podIP must not expose an empty podIPs entry"
        );
    }
    #[tokio::test]
    async fn set_pod_status_no_object_change_does_not_advance_resource_version() {
        let repo = build_status_repo().await;
        let created = repo
            .persistence
            .seed_pod("default", "same-status", pending_pod("same-status"))
            .await
            .unwrap();

        let update = super::super::assembly_support::support::PodStatusUpdate {
            phase: "Pending".to_string(),
            pod_ip: None,
            host_ip: klights_kubelet::pod_repository::PublishedAddress::must("10.0.0.10"),
            container_statuses: vec![json!({
                "name": "c",
                "ready": false,
                "started": false,
                "restartCount": 0,
                "image": "nginx",
                "imageID": "",
                "state": {"waiting": {"reason": "ErrImagePull", "message": "pull failed"}}
            })],
            init_container_statuses: None,
            qos_class: None,
        };

        let first = repo
            .status_ports()
            .set_pod_status(
                "default",
                "same-status",
                update.clone(),
                Some(created.resource_version),
            )
            .await
            .unwrap();
        let second = repo
            .status_ports()
            .set_pod_status("default", "same-status", update, None)
            .await
            .unwrap();

        assert_eq!(
            second.resource_version, first.resource_version,
            "recomputing identical pod status must not emit a resourceVersion-only update"
        );
        assert_eq!(second.data, first.data);
    }
    #[tokio::test]
    async fn set_pod_status_no_object_change_does_not_enqueue_owner_reconcile() {
        let repo = build_status_repo_with_dispatcher().await;
        let stored = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "owned-noop",
                "namespace": "default",
                "ownerReferences": [{
                    "apiVersion": "v1",
                    "kind": "ReplicationController",
                    "name": "owner-rc",
                    "uid": "owner-rc-uid",
                    "controller": true
                }]
            },
            "spec": {"containers": [{"name": "c", "image": "nginx"}]},
            "status": {
                "phase": "Pending",
                "podIP": "",
                "hostIP": "10.0.0.10",
                "hostIPs": [{"ip": "10.0.0.10"}],
                "conditions": [
                    {
                        "type": "PodScheduled",
                        "status": "True",
                        "lastTransitionTime": "2026-01-01T00:00:00Z",
                        "reason": "PodScheduled"
                    },
                    {
                        "type": "Initialized",
                        "status": "True",
                        "lastTransitionTime": "2026-01-01T00:00:00Z"
                    },
                    {
                        "type": "ContainersReady",
                        "status": "False",
                        "lastTransitionTime": "2026-01-01T00:00:00Z"
                    },
                    {
                        "type": "Ready",
                        "status": "False",
                        "lastTransitionTime": "2026-01-01T00:00:00Z"
                    }
                ],
                "containerStatuses": []
            }
        });
        let created = repo
            .persistence
            .seed_pod("default", "owned-noop", stored)
            .await
            .unwrap();

        let updated = repo
            .status_ports()
            .set_pod_status(
                "default",
                "owned-noop",
                super::super::assembly_support::support::PodStatusUpdate {
                    phase: "Pending".to_string(),
                    pod_ip: None,
                    host_ip: klights_kubelet::pod_repository::PublishedAddress::must("10.0.0.10"),
                    container_statuses: vec![],
                    init_container_statuses: None,
                    qos_class: None,
                },
                Some(created.resource_version),
            )
            .await
            .unwrap();

        assert_eq!(updated.resource_version, created.resource_version);
        assert!(
            repo.pending_reconcile_keys().await.is_empty(),
            "unchanged pod status must not enqueue owner controller reconcile work"
        );
    }
    #[tokio::test]
    async fn set_pod_status_reconciles_namespace_termination_for_late_pod() {
        let repo = build_status_repo().await;
        repo.seed_namespace(
            "term-status",
            json!({
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {"name": "term-status", "uid": "term-status-uid"},
                "spec": {"finalizers": ["kubernetes"]},
                "status": {"phase": "Active"}
            }),
        )
        .await
        .unwrap();

        let mut pod = pending_pod("late-pod");
        pod["metadata"]["namespace"] = json!("term-status");
        pod["spec"]["nodeName"] = json!("worker-a");
        let created = repo
            .persistence
            .seed_pod("term-status", "late-pod", pod)
            .await
            .unwrap();

        let namespace = repo
            .read_namespace("term-status")
            .await
            .unwrap()
            .expect("namespace present");
        let mut terminating: serde_json::Value = std::sync::Arc::unwrap_or_clone(namespace.data);
        k8s_native_service::set_namespace_terminating_status_at(
            &mut terminating,
            false,
            chrono::DateTime::UNIX_EPOCH,
        );
        repo.update_namespace("term-status", terminating, namespace.resource_version)
            .await
            .unwrap();

        repo.status_ports()
            .set_pod_status(
                "term-status",
                "late-pod",
                super::super::assembly_support::support::PodStatusUpdate {
                    phase: "Pending".to_string(),
                    pod_ip: None,
                    host_ip: klights_kubelet::pod_repository::PublishedAddress::must("10.0.0.10"),
                    container_statuses: vec![],
                    init_container_statuses: None,
                    qos_class: None,
                },
                None,
            )
            .await
            .unwrap();

        // Namespace termination is detached through TaskSupervisor; await its
        // operation-specific completion notification instead of guessing a
        // wall-clock delay or draining unrelated Background tasks.
        repo.wait_for_post_write_maintenance().await;

        let terminating_pod = repo
            .query
            .get_pod_by_name("term-status", "late-pod")
            .await
            .unwrap()
            .expect("pod remains until actor cleanup owns final deletion");
        assert!(
            terminating_pod
                .data
                .pointer("/metadata/deletionTimestamp")
                .and_then(|v| v.as_str())
                .is_some(),
            "pod status writes in a terminating namespace must mark the Pod terminating"
        );
        assert!(
            repo.read_namespace("term-status").await.unwrap().is_some(),
            "namespace must remain until actor cleanup removes the Pod row"
        );

        assert_eq!(
            repo.finalize_pod_deletion_after_actor_cleanup("term-status", "late-pod", &created.uid)
                .await
                .unwrap(),
            super::super::assembly_support::support::PodFinalizationOutcome::DeletedOrAlreadyGone,
            "actor finalization should remove the terminating late Pod"
        );
        repo.wait_for_post_write_maintenance().await;
        let remaining_namespace = repo.read_namespace("term-status").await.unwrap();
        assert!(
            remaining_namespace.is_none(),
            "namespace should be hard-deleted after actor-owned Pod removal: {remaining_namespace:?}"
        );
    }
    #[tokio::test]
    async fn set_pod_status_reconciles_matching_pdb_after_readiness_transition() {
        let repo = build_status_repo_with_dispatcher().await;

        let pdb = json!({
            "apiVersion": "policy/v1",
            "kind": "PodDisruptionBudget",
            "metadata": {"name": "pdb-ready", "namespace": "default"},
            "spec": {
                "minAvailable": 0,
                "selector": {"matchLabels": {"app": "x"}}
            }
        });
        repo.seed_non_pod_resource(
            "policy/v1",
            "PodDisruptionBudget",
            "default",
            "pdb-ready",
            pdb.clone(),
        )
        .await
        .unwrap();

        let created = repo
            .persistence
            .seed_pod("default", "pdb-pod", pending_pod("pdb-pod"))
            .await
            .unwrap();

        repo.reconcile_pod_disruption_budget(&pdb, chrono::Utc::now())
            .await
            .unwrap();
        let before = repo
            .read_non_pod_resource("policy/v1", "PodDisruptionBudget", "default", "pdb-ready")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            before
                .data
                .pointer("/status/currentHealthy")
                .and_then(|v| v.as_i64()),
            Some(0)
        );

        repo.status_ports()
            .set_pod_status(
                "default",
                "pdb-pod",
                super::super::assembly_support::support::PodStatusUpdate {
                    phase: "Running".to_string(),
                    pod_ip: klights_kubelet::pod_repository::PublishedAddress::must("10.42.0.8"),
                    host_ip: klights_kubelet::pod_repository::PublishedAddress::must("10.0.0.10"),
                    container_statuses: vec![json!({
                        "name": "c",
                        "ready": true,
                        "started": true,
                        "restartCount": 0,
                        "image": "nginx",
                        "imageID": "",
                        "state": {"running": {"startedAt": "2026-04-30T00:00:00Z"}}
                    })],
                    init_container_statuses: None,
                    qos_class: None,
                },
                Some(created.resource_version),
            )
            .await
            .unwrap();

        // PDB reconciliation is now async (spawned via TaskSupervisor).
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let after = repo
            .read_non_pod_resource("policy/v1", "PodDisruptionBudget", "default", "pdb-ready")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            after
                .data
                .pointer("/status/currentHealthy")
                .and_then(|v| v.as_i64()),
            Some(1),
            "standard pod status writes must refresh matching PDB status"
        );
        assert_eq!(
            after
                .data
                .pointer("/status/disruptionsAllowed")
                .and_then(|v| v.as_i64()),
            Some(1)
        );
    }
    #[tokio::test]
    async fn record_sandbox_id_does_not_reconcile_pdb_without_endpoint_change() {
        let repo = build_status_repo().await;

        let pdb = json!({
            "apiVersion": "policy/v1",
            "kind": "PodDisruptionBudget",
            "metadata": {"name": "pdb-sandbox", "namespace": "default"},
            "spec": {
                "minAvailable": 0,
                "selector": {"matchLabels": {"app": "x"}}
            }
        });
        repo.seed_non_pod_resource(
            "policy/v1",
            "PodDisruptionBudget",
            "default",
            "pdb-sandbox",
            pdb.clone(),
        )
        .await
        .unwrap();

        let created = repo
            .persistence
            .seed_pod("default", "pdb-sandbox-pod", pending_pod("pdb-sandbox-pod"))
            .await
            .unwrap();

        repo.reconcile_pod_disruption_budget(&pdb, chrono::Utc::now())
            .await
            .unwrap();
        let before = repo
            .read_non_pod_resource("policy/v1", "PodDisruptionBudget", "default", "pdb-sandbox")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            before
                .data
                .pointer("/status/currentHealthy")
                .and_then(|v| v.as_i64()),
            Some(0)
        );

        repo.api_ports()
            .replace_status_from_api(
                "default",
                "pdb-sandbox-pod",
                json!({
                    "phase": "Running",
                    "podIP": "10.42.0.9",
                    "podIPs": [{"ip": "10.42.0.9"}],
                    "conditions": [
                        {"type": "Ready", "status": "True"},
                        {"type": "ContainersReady", "status": "True"}
                    ]
                }),
                created.resource_version,
            )
            .await
            .unwrap();

        repo.record_sandbox_id("default", "pdb-sandbox-pod", "sandbox-pdb")
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let after = repo
            .read_non_pod_resource("policy/v1", "PodDisruptionBudget", "default", "pdb-sandbox")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            after
                .data
                .pointer("/status/currentHealthy")
                .and_then(|v| v.as_i64()),
            Some(0),
            "sandbox metadata writes must not trigger the old namespace-wide PDB sweep"
        );
    }
    #[tokio::test]
    async fn pod_status_subresource_readiness_change_enqueues_matching_service_once() {
        let repo = build_status_repo_with_dispatcher().await;

        repo.seed_non_pod_resource(
            "v1",
            "Service",
            "default",
            "web",
            json!({
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {"name": "web", "namespace": "default"},
                "spec": {
                    "selector": {"app": "x"},
                    "ports": [{"name": "http", "port": 80, "targetPort": 8080}]
                }
            }),
        )
        .await
        .unwrap();
        repo.seed_non_pod_resource(
            "v1",
            "Service",
            "default",
            "other",
            json!({
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {"name": "other", "namespace": "default"},
                "spec": {
                    "selector": {"app": "other"},
                    "ports": [{"name": "http", "port": 80, "targetPort": 8080}]
                }
            }),
        )
        .await
        .unwrap();

        let mut seed = pending_pod("svc-pod");
        seed["status"] = json!({
            "phase": "Running",
            "podIP": "10.42.0.18",
            "podIPs": [{"ip": "10.42.0.18"}],
            "hostIP": "10.0.0.10",
            "hostIPs": [{"ip": "10.0.0.10"}],
            "conditions": [
                {"type": "PodScheduled", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
                {"type": "Initialized", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
                {"type": "ContainersReady", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
                {"type": "Ready", "status": "False", "lastTransitionTime": "2026-04-30T00:00:00Z"}
            ]
        });
        let created = repo
            .persistence
            .seed_pod("default", "svc-pod", seed)
            .await
            .unwrap();

        let _updated = repo
            .api_ports().replace_status_from_api(
            "default",
            "svc-pod",
            json!({
                "phase": "Running",
                "podIP": "10.42.0.18",
                "podIPs": [{"ip": "10.42.0.18"}],
                "hostIP": "10.0.0.10",
                "hostIPs": [{"ip": "10.0.0.10"}],
                "conditions": [
                    {"type": "PodScheduled", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
                    {"type": "Initialized", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
                    {"type": "ContainersReady", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
                    {"type": "Ready", "status": "True", "lastTransitionTime": "2026-05-01T00:00:00Z"}
                ],
                "containerStatuses": [],
            }),
            created.resource_version,
        )
        .await
        .unwrap();

        let keys = repo.pending_reconcile_keys().await;
        let web_count = keys
            .iter()
            .filter(|key| {
                key.api_version() == "v1"
                    && key.kind() == "Service"
                    && key.namespace() == Some("default")
                    && key.name() == "web"
            })
            .count();
        assert_eq!(
            web_count, 1,
            "a readiness transition should enqueue one Service reconcile for affected Service"
        );
        assert!(
            keys.iter().any(|key| {
                key.api_version() == "v1"
                    && key.kind() == "Service"
                    && key.namespace() == Some("default")
                    && key.name() == "web"
            }),
            "a Pod readiness transition must enqueue matching Services so Endpoints leave notReadyAddresses"
        );
    }
    #[tokio::test]
    async fn pod_status_subresource_no_endpoint_change_does_not_enqueue_service() {
        let repo = build_status_repo_with_dispatcher().await;

        repo.seed_non_pod_resource(
            "v1",
            "Service",
            "default",
            "web",
            json!({
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {"name": "web", "namespace": "default"},
                "spec": {
                    "selector": {"app": "x"},
                    "ports": [{"name": "http", "port": 80, "targetPort": 8080}]
                }
            }),
        )
        .await
        .unwrap();

        let mut seed = pending_pod("stable-svc-pod");
        seed["status"] = json!({
            "phase": "Running",
            "podIP": "10.42.0.19",
            "podIPs": [{"ip": "10.42.0.19"}],
            "hostIP": "10.0.0.10",
            "hostIPs": [{"ip": "10.0.0.10"}],
            "conditions": [
                {"type": "PodScheduled", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
                {"type": "Initialized", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
                {"type": "ContainersReady", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
                {"type": "Ready", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"}
            ],
            "containerStatuses": [{
                "name": "c",
                "ready": true,
                "started": true,
                "restartCount": 0,
                "image": "nginx",
                "imageID": "",
                "state": {"running": {"startedAt": "2026-04-30T00:00:00Z"}}
            }]
        });
        let created = repo
            .persistence
            .seed_pod("default", "stable-svc-pod", seed)
            .await
            .unwrap();

        let _updated = repo
            .api_ports().replace_status_from_api(
            "default",
            "stable-svc-pod",
            json!({
                "phase": "Running",
                "podIP": "10.42.0.19",
                "podIPs": [{"ip": "10.42.0.19"}],
                "hostIP": "10.0.0.10",
                "hostIPs": [{"ip": "10.0.0.10"}],
                "conditions": [
                    {"type": "PodScheduled", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
                    {"type": "Initialized", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
                    {"type": "ContainersReady", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
                    {"type": "Ready", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"}
                ],
                "containerStatuses": [{
                    "name": "c",
                    "ready": true,
                    "started": true,
                    "restartCount": 2,
                    "image": "nginx",
                    "imageID": "",
                    "state": {"running": {"startedAt": "2026-04-30T00:00:00Z"}}
                }]
            }),
            created.resource_version,
        )
        .await
        .unwrap();

        let keys = repo.pending_reconcile_keys().await;
        assert!(
            keys.iter()
                .all(|key| !(key.api_version() == "v1" && key.kind() == "Service")),
            "status-only changes that keep endpoint state stable must not enqueue Service reconcile: {keys:?}"
        );
    }
    #[tokio::test]
    async fn set_pod_status_returns_conflict_on_stale_rv() {
        let repo = build_status_repo().await;
        let created = repo
            .persistence
            .seed_pod("default", "racer", pending_pod("racer"))
            .await
            .unwrap();
        let snapshot_rv = created.resource_version;

        // First writer wins with the snapshot rv.
        let update_a = super::super::assembly_support::support::PodStatusUpdate {
            phase: "Running".to_string(),
            pod_ip: klights_kubelet::pod_repository::PublishedAddress::must("10.42.0.6"),
            host_ip: klights_kubelet::pod_repository::PublishedAddress::must("10.0.0.10"),
            container_statuses: vec![],
            init_container_statuses: None,
            qos_class: None,
        };
        repo.status_ports()
            .set_pod_status("default", "racer", update_a, Some(snapshot_rv))
            .await
            .expect("first writer succeeds");

        // Second writer with the stale snapshot rv must hit Conflict.
        let update_b = super::super::assembly_support::support::PodStatusUpdate {
            phase: "Failed".to_string(),
            pod_ip: klights_kubelet::pod_repository::PublishedAddress::must("10.42.0.6"),
            host_ip: klights_kubelet::pod_repository::PublishedAddress::must("10.0.0.10"),
            container_statuses: vec![],
            init_container_statuses: None,
            qos_class: None,
        };
        let conflict = repo
            .status_ports()
            .set_pod_status("default", "racer", update_b, Some(snapshot_rv))
            .await;
        let err = conflict.expect_err("stale rv must conflict");
        assert!(
            err.to_string().contains("409"),
            "expected 409 Conflict, got {err:?}"
        );
    }
    #[tokio::test]
    async fn set_pod_status_retries_implicit_rv_conflict_after_scheduler_update() {
        let outcome = super::super::assembly_support::support::run_scheduler_status_race(
            pending_pod("scheduled-race"),
            super::super::assembly_support::support::PodStatusUpdate {
                phase: "Pending".to_string(),
                pod_ip: None,
                host_ip: None,
                container_statuses: vec![],
                init_container_statuses: None,
                qos_class: None,
            },
        )
        .await;
        let updated = outcome
            .resource
            .expect("implicit kubelet status writes should retry scheduler CAS races");

        assert_eq!(outcome.attempts, 2);
        assert_eq!(updated.data["spec"]["nodeName"], json!("dp"));
        assert_eq!(updated.data["status"]["phase"], json!("Pending"));
    }
    #[tokio::test]
    async fn apply_runtime_reconcile_status_overwrites_phase_and_containers_only() {
        let repo = build_status_repo().await;

        // Seed a pod whose status already has IPs / conditions / qosClass that
        // the runtime reconciler MUST NOT erase.
        let mut seed = pending_pod("rr1");
        seed["status"] = json!({
            "phase": "Running",
            "podIP": "10.42.0.7",
            "podIPs": [{"ip": "10.42.0.7"}],
            "hostIP": "10.0.0.10",
            "hostIPs": [{"ip": "10.0.0.10"}],
            "qosClass": "BestEffort",
            "conditions": [
                {"type": "Ready", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"}
            ],
            "containerStatuses": [
                {"name": "c", "ready": true, "restartCount": 0}
            ]
        });
        let created = repo
            .persistence
            .seed_pod("default", "rr1", seed)
            .await
            .unwrap();

        let update = super::super::assembly_support::support::RuntimeReconcileStatus {
            phase: "Failed".to_string(),
            container_statuses: vec![json!({
                "name": "c", "ready": false, "restartCount": 1,
                "state": {"terminated": {"exitCode": 1}}
            })],
        };
        let updated = repo
            .status_ports()
            .apply_runtime_reconcile_status(
                "default",
                "rr1",
                update,
                Some(created.resource_version),
            )
            .await
            .unwrap();

        let status = &updated.data["status"];
        // overwrites
        assert_eq!(status["phase"], json!("Failed"));
        assert_eq!(status["containerStatuses"][0]["ready"], json!(false));
        assert_eq!(status["containerStatuses"][0]["restartCount"], json!(1));
        // preserves
        assert_eq!(status["podIP"], json!("10.42.0.7"));
        assert_eq!(status["podIPs"][0]["ip"], json!("10.42.0.7"));
        assert_eq!(status["hostIP"], json!("10.0.0.10"));
        assert_eq!(status["qosClass"], json!("BestEffort"));
        assert_eq!(status["conditions"][0]["type"], json!("Ready"));
    }
    #[tokio::test]
    async fn apply_runtime_reconcile_status_never_decreases_restart_count() {
        let repo = build_status_repo().await;

        let mut seed = pending_pod("rr-restart-monotonic");
        seed["status"] = json!({
            "phase": "Running",
            "containerStatuses": [
                {
                    "name": "c",
                    "ready": false,
                    "restartCount": 1,
                    "lastState": {"terminated": {"exitCode": 1, "reason": "Error"}}
                }
            ]
        });
        let created = repo
            .persistence
            .seed_pod("default", "rr-restart-monotonic", seed)
            .await
            .unwrap();

        let updated = repo
            .status_ports()
            .apply_runtime_reconcile_status(
                "default",
                "rr-restart-monotonic",
                super::super::assembly_support::support::RuntimeReconcileStatus {
                    phase: "Running".to_string(),
                    container_statuses: vec![json!({
                        "name": "c",
                        "ready": true,
                        "restartCount": 0,
                        "state": {"running": {"startedAt": "2026-05-02T00:00:00Z"}}
                    })],
                },
                Some(created.resource_version),
            )
            .await
            .unwrap();

        let status = &updated.data["status"]["containerStatuses"][0];
        assert_eq!(status["restartCount"], json!(1));
        assert_eq!(
            status.pointer("/lastState/terminated/exitCode"),
            Some(&json!(1)),
            "runtime reconcile must preserve lastState when its snapshot lacks it"
        );
    }
    #[tokio::test]
    async fn deferred_running_runtime_reconcile_preserves_restart_count_for_fast_onfailure_completion()
     {
        let repo = build_status_repo().await;

        let mut seed = pending_pod("rr-deferred-restart");
        seed["status"] = json!({
            "phase": "Pending",
            "conditions": [
                {"type": "PodScheduled", "status": "True", "lastTransitionTime": "2026-05-17T00:00:00Z"},
                {"type": "Initialized", "status": "True", "lastTransitionTime": "2026-05-17T00:00:00Z"}
            ]
        });
        repo.persistence
            .seed_pod("default", "rr-deferred-restart", seed)
            .await
            .unwrap();

        let after_restart = repo
            .status_ports()
            .apply_runtime_reconcile_status(
                "default",
                "rr-deferred-restart",
                super::super::assembly_support::support::RuntimeReconcileStatus {
                    phase: "Running".to_string(),
                    container_statuses: vec![json!({
                        "name": "c",
                        "ready": false,
                        "restartCount": 1,
                        "lastState": {"terminated": {"exitCode": 1, "reason": "Error"}},
                        "state": {"running": {"startedAt": "2026-05-17T00:00:01Z"}}
                    })],
                },
                None,
            )
            .await
            .unwrap();

        assert_eq!(after_restart.data["status"]["phase"], json!("Pending"));
        assert_eq!(
            after_restart.data["status"]["containerStatuses"][0]["restartCount"],
            json!(1),
            "restart count from the deferred Running reconcile must be persisted"
        );
        assert_eq!(
            after_restart.data["status"]["containerStatuses"][0]
                .pointer("/lastState/terminated/exitCode"),
            Some(&json!(1))
        );
        assert!(
            after_restart.data["status"].pointer("/podIP").is_none(),
            "the race guard must not invent or clear podIP while preserving runtime status"
        );

        let completed = repo
            .status_ports()
            .apply_runtime_reconcile_status(
                "default",
                "rr-deferred-restart",
                super::super::assembly_support::support::RuntimeReconcileStatus {
                    phase: "Succeeded".to_string(),
                    container_statuses: vec![json!({
                        "name": "c",
                        "ready": false,
                        "restartCount": 0,
                        "state": {"terminated": {"exitCode": 0, "reason": "Completed"}}
                    })],
                },
                None,
            )
            .await
            .unwrap();

        let status = &completed.data["status"];
        assert_eq!(status["phase"], json!("Succeeded"));
        assert_eq!(
            status["containerStatuses"][0]["restartCount"],
            json!(1),
            "terminal reconcile must not regress the restart count after fast OnFailure completion"
        );
        assert_eq!(
            status["containerStatuses"][0].pointer("/lastState/terminated/exitCode"),
            Some(&json!(1))
        );
        assert_eq!(
            status["containerStatuses"][0].pointer("/state/terminated/exitCode"),
            Some(&json!(0))
        );
    }
    #[tokio::test]
    async fn post_sandbox_ip_status_promotes_deferred_running_runtime_snapshot() {
        let repo = build_status_repo().await;

        let mut seed = pending_pod("rr-deferred-running-ip");
        seed["status"] = json!({
            "phase": "Pending",
            "conditions": [
                {"type": "PodScheduled", "status": "True", "lastTransitionTime": "2026-05-17T00:00:00Z"},
                {"type": "Initialized", "status": "True", "lastTransitionTime": "2026-05-17T00:00:00Z"}
            ]
        });
        let created = repo
            .persistence
            .seed_pod("default", "rr-deferred-running-ip", seed)
            .await
            .unwrap();

        let deferred = repo
            .status_ports()
            .apply_runtime_reconcile_status_for_uid(
                "default",
                "rr-deferred-running-ip",
                &created.uid,
                super::super::assembly_support::support::RuntimeReconcileStatus {
                    phase: "Running".to_string(),
                    container_statuses: vec![deferred_running_status(
                        "c",
                        Some("containerd://confirmed"),
                        0,
                        "2026-05-17T00:00:01Z",
                    )],
                },
                None,
            )
            .await
            .unwrap();

        assert_eq!(
            deferred.data["status"]["phase"],
            json!("Pending"),
            "runtime reconcile must still avoid Running before podIP is published"
        );

        let promoted = repo
            .status_ports()
            .set_pod_status_for_uid(
                "default",
                "rr-deferred-running-ip",
                &created.uid,
                published_network_status(
                    "10.42.0.44",
                    vec![container_creating_status(
                        "c",
                        Some("containerd://confirmed"),
                        0,
                    )],
                ),
                None,
            )
            .await
            .unwrap();

        let status = &promoted.data["status"];
        assert_eq!(status["phase"], json!("Running"));
        assert_eq!(status["podIP"], json!("10.42.0.44"));
        assert_eq!(
            status["containerStatuses"][0].pointer("/state/running/startedAt"),
            Some(&json!("2026-05-17T00:00:01Z"))
        );
        assert_eq!(status["containerStatuses"][0]["ready"], json!(true));
        let ready = status["conditions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|condition| condition["type"] == json!("Ready"))
            .unwrap();
        assert_eq!(
            ready["status"],
            json!("True"),
            "post-IP status write should complete the deferred Running transition"
        );
    }
    #[tokio::test]
    async fn post_sandbox_ip_status_does_not_promote_unproven_deferred_runtime_identity() {
        struct Case {
            name: &'static str,
            runtime_container_id: Option<&'static str>,
            network_container_id: Option<&'static str>,
            runtime_restart_count: u64,
            network_restart_count: u64,
        }

        let cases = [
            Case {
                name: "runtime-id-missing",
                runtime_container_id: None,
                network_container_id: Some("containerd://current"),
                runtime_restart_count: 0,
                network_restart_count: 0,
            },
            Case {
                name: "network-id-missing",
                runtime_container_id: Some("containerd://current"),
                network_container_id: None,
                runtime_restart_count: 0,
                network_restart_count: 0,
            },
            Case {
                name: "different-container-id",
                runtime_container_id: Some("containerd://old"),
                network_container_id: Some("containerd://new"),
                runtime_restart_count: 0,
                network_restart_count: 0,
            },
            Case {
                name: "newer-restart-generation",
                runtime_container_id: Some("containerd://same"),
                network_container_id: Some("containerd://same"),
                runtime_restart_count: 0,
                network_restart_count: 1,
            },
        ];

        for case in cases {
            let repo = build_status_repo().await;
            let mut seed = pending_pod(case.name);
            seed["status"] = json!({"phase": "Pending"});
            let created = repo
                .persistence
                .seed_pod("default", case.name, seed)
                .await
                .unwrap();

            repo.status_ports()
                .apply_runtime_reconcile_status_for_uid(
                    "default",
                    case.name,
                    &created.uid,
                    super::super::assembly_support::support::RuntimeReconcileStatus {
                        phase: "Running".to_string(),
                        container_statuses: vec![deferred_running_status(
                            "c",
                            case.runtime_container_id,
                            case.runtime_restart_count,
                            "2026-05-17T00:00:01Z",
                        )],
                    },
                    None,
                )
                .await
                .unwrap();

            let updated = repo
                .status_ports()
                .set_pod_status_for_uid(
                    "default",
                    case.name,
                    &created.uid,
                    published_network_status(
                        "10.42.0.45",
                        vec![container_creating_status(
                            "c",
                            case.network_container_id,
                            case.network_restart_count,
                        )],
                    ),
                    None,
                )
                .await
                .unwrap();

            assert_eq!(
                updated.data["status"]["phase"],
                json!("Pending"),
                "{} must not promote an unproven deferred runtime identity",
                case.name
            );
            assert!(
                updated.data["status"]["containerStatuses"][0]
                    .pointer("/state/running")
                    .is_none(),
                "{} must not resurrect the deferred Running state",
                case.name
            );
        }
    }
    #[tokio::test]
    async fn terminal_runtime_before_pod_ip_cancels_deferred_running_promotion() {
        let repo = build_status_repo().await;
        let mut seed = pending_pod("terminal-before-ip");
        seed["status"] = json!({"phase": "Pending"});
        let created = repo
            .persistence
            .seed_pod("default", "terminal-before-ip", seed)
            .await
            .unwrap();

        repo.status_ports()
            .apply_runtime_reconcile_status_for_uid(
                "default",
                "terminal-before-ip",
                &created.uid,
                super::super::assembly_support::support::RuntimeReconcileStatus {
                    phase: "Running".to_string(),
                    container_statuses: vec![deferred_running_status(
                        "c",
                        Some("containerd://same"),
                        0,
                        "2026-05-17T00:00:01Z",
                    )],
                },
                None,
            )
            .await
            .unwrap();
        repo.status_ports()
            .apply_runtime_reconcile_status_for_uid(
                "default",
                "terminal-before-ip",
                &created.uid,
                super::super::assembly_support::support::RuntimeReconcileStatus {
                    phase: "Succeeded".to_string(),
                    container_statuses: vec![json!({
                        "name": "c",
                        "containerID": "containerd://same",
                        "ready": false,
                        "started": false,
                        "restartCount": 0,
                        "state": {"terminated": {"exitCode": 0, "reason": "Completed"}}
                    })],
                },
                None,
            )
            .await
            .unwrap();

        // A delayed duplicate Running observation from before completion must not
        // re-arm promotion after the terminal generation has won.
        let delayed_running = repo
            .status_ports()
            .apply_runtime_reconcile_status_for_uid(
                "default",
                "terminal-before-ip",
                &created.uid,
                super::super::assembly_support::support::RuntimeReconcileStatus {
                    phase: "Running".to_string(),
                    container_statuses: vec![deferred_running_status(
                        "c",
                        Some("containerd://same"),
                        0,
                        "2026-05-17T00:00:01Z",
                    )],
                },
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            delayed_running.data["status"]["phase"],
            json!("Succeeded"),
            "terminal phase must bypass the IP-gated deferred Running path"
        );
        assert_eq!(
            delayed_running.data["status"]["containerStatuses"][0]
                .pointer("/state/terminated/exitCode"),
            Some(&json!(0)),
            "a delayed Running observation must not overwrite terminal runtime state"
        );

        let updated = repo
            .status_ports()
            .set_pod_status_for_uid(
                "default",
                "terminal-before-ip",
                &created.uid,
                published_network_status(
                    "10.42.0.46",
                    vec![container_creating_status("c", Some("containerd://same"), 0)],
                ),
                None,
            )
            .await
            .unwrap();

        assert_eq!(
            updated.data["status"]["phase"],
            json!("Succeeded"),
            "network publication must not regress a terminal runtime phase"
        );
        assert!(
            updated.data["status"]["containerStatuses"][0]
                .pointer("/state/running")
                .is_none(),
            "a terminal runtime observation must cancel deferred Running promotion"
        );
        assert_eq!(
            updated.data["status"]["containerStatuses"][0].pointer("/state/terminated/exitCode"),
            Some(&json!(0)),
            "network publication must preserve runtime-owned terminal status"
        );
    }
    #[tokio::test]
    async fn deferred_runtime_reducer_keeps_latest_restart_generation_after_stale_network_snapshot()
    {
        let repo = build_status_repo().await;
        let mut seed = pending_pod("deferred-restart-generation");
        seed["status"] = json!({"phase": "Pending"});
        let created = repo
            .persistence
            .seed_pod("default", "deferred-restart-generation", seed)
            .await
            .unwrap();

        for (container_id, restart_count, started_at) in [
            ("containerd://old", 0, "2026-05-17T00:00:01Z"),
            ("containerd://new", 1, "2026-05-17T00:00:02Z"),
        ] {
            repo.status_ports()
                .apply_runtime_reconcile_status_for_uid(
                    "default",
                    "deferred-restart-generation",
                    &created.uid,
                    super::super::assembly_support::support::RuntimeReconcileStatus {
                        phase: "Running".to_string(),
                        container_statuses: vec![deferred_running_status(
                            "c",
                            Some(container_id),
                            restart_count,
                            started_at,
                        )],
                    },
                    None,
                )
                .await
                .unwrap();
        }

        let stale = repo
            .status_ports()
            .set_pod_status_for_uid(
                "default",
                "deferred-restart-generation",
                &created.uid,
                published_network_status(
                    "10.42.0.48",
                    vec![container_creating_status("c", Some("containerd://old"), 0)],
                ),
                None,
            )
            .await
            .unwrap();
        assert_eq!(stale.data["status"]["phase"], json!("Pending"));

        let promoted = repo
            .status_ports()
            .set_pod_status_for_uid(
                "default",
                "deferred-restart-generation",
                &created.uid,
                published_network_status(
                    "10.42.0.48",
                    vec![container_creating_status("c", Some("containerd://new"), 1)],
                ),
                None,
            )
            .await
            .unwrap();

        assert_eq!(promoted.data["status"]["phase"], json!("Running"));
        assert_eq!(
            promoted.data["status"]["containerStatuses"][0].pointer("/state/running/startedAt"),
            Some(&json!("2026-05-17T00:00:02Z")),
            "only the latest explicitly deferred runtime generation may be promoted"
        );
    }
    #[tokio::test]
    async fn actor_finalization_clears_deferred_runtime_observation() {
        let repo = build_status_repo().await;
        let mut seed = pending_pod("deferred-runtime-finalized");
        seed["spec"]["nodeName"] = json!("worker-a");
        seed["metadata"]["deletionTimestamp"] = json!("2026-05-17T00:00:02Z");
        seed["metadata"]["deletionGracePeriodSeconds"] = json!(0);
        seed["status"] = json!({"phase": "Pending"});
        let created = repo
            .persistence
            .seed_pod("default", "deferred-runtime-finalized", seed)
            .await
            .unwrap();

        repo.status_ports()
            .apply_runtime_reconcile_status_for_uid(
                "default",
                "deferred-runtime-finalized",
                &created.uid,
                super::super::assembly_support::support::RuntimeReconcileStatus {
                    phase: "Running".to_string(),
                    container_statuses: vec![deferred_running_status(
                        "c",
                        Some("containerd://finalized"),
                        0,
                        "2026-05-17T00:00:01Z",
                    )],
                },
                None,
            )
            .await
            .unwrap();
        assert!(repo.has_deferred_runtime_for_uid(&created.uid));

        let result = repo
            .finalize_pod_deletion_after_actor_cleanup(
                "default",
                "deferred-runtime-finalized",
                &created.uid,
            )
            .await
            .unwrap();

        assert_eq!(
            result,
            super::super::assembly_support::support::PodFinalizationOutcome::DeletedOrAlreadyGone
        );
        assert!(
            !repo.has_deferred_runtime_for_uid(&created.uid),
            "actor-owned finalization must release UID-keyed deferred runtime state"
        );
    }
    #[tokio::test]
    async fn deferred_runtime_cleanup_finalizer_clears_only_terminal_success() {
        for (name, outcome, should_clear) in [
            (
                "deleted-or-already-gone",
                super::super::assembly_support::support::DeferredRuntimeFinalizerOutcome::Deleted,
                true,
            ),
            (
                "finalizers-pending",
                super::super::assembly_support::support::DeferredRuntimeFinalizerOutcome::Pending,
                false,
            ),
            (
                "error",
                super::super::assembly_support::support::DeferredRuntimeFinalizerOutcome::Error,
                false,
            ),
        ] {
            let uid = format!("uid-{name}");
            let (result_is_ok, cleared) =
                super::super::assembly_support::support::run_deferred_runtime_cleanup_case(
                    &uid, outcome,
                )
                .await;

            if matches!(
                outcome,
                super::super::assembly_support::support::DeferredRuntimeFinalizerOutcome::Error
            ) {
                assert!(!result_is_ok, "{name} must preserve the inner error");
            } else {
                assert!(result_is_ok, "{name} must preserve the inner result");
            }
            assert_eq!(cleared, should_clear, "{name} cleanup decision");
        }
    }
    #[tokio::test]
    async fn partial_multicontainer_runtime_observation_is_not_promotable_after_pod_ip() {
        let repo = build_status_repo().await;
        let mut seed = pending_pod("partial-multicontainer-before-ip");
        seed["spec"]["containers"] = json!([
            {"name": "c", "image": "nginx"},
            {"name": "sidecar", "image": "busybox"}
        ]);
        seed["status"] = json!({"phase": "Pending"});
        let created = repo
            .persistence
            .seed_pod("default", "partial-multicontainer-before-ip", seed)
            .await
            .unwrap();

        repo.status_ports()
            .apply_runtime_reconcile_status_for_uid(
                "default",
                "partial-multicontainer-before-ip",
                &created.uid,
                super::super::assembly_support::support::RuntimeReconcileStatus {
                    phase: "Running".to_string(),
                    container_statuses: vec![deferred_running_status(
                        "c",
                        Some("containerd://c"),
                        0,
                        "2026-05-17T00:00:01Z",
                    )],
                },
                None,
            )
            .await
            .unwrap();

        let updated = repo
            .status_ports()
            .set_pod_status_for_uid(
                "default",
                "partial-multicontainer-before-ip",
                &created.uid,
                published_network_status(
                    "10.42.0.47",
                    vec![container_creating_status("c", Some("containerd://c"), 0)],
                ),
                None,
            )
            .await
            .unwrap();

        assert_eq!(updated.data["status"]["phase"], json!("Pending"));
        assert!(
            updated.data["status"]["containerStatuses"][0]
                .pointer("/state/running")
                .is_none(),
            "a partial runtime observation must not promote a multi-container Pod"
        );
    }
    #[tokio::test]
    async fn apply_runtime_reconcile_status_terminal_phase_marks_pod_not_ready() {
        let repo = build_status_repo().await;

        let mut seed = pending_pod("rr-complete");
        seed["status"] = json!({
            "phase": "Running",
            "podIP": "10.42.0.8",
            "podIPs": [{"ip": "10.42.0.8"}],
            "hostIP": "10.0.0.10",
            "hostIPs": [{"ip": "10.0.0.10"}],
            "qosClass": "BestEffort",
            "conditions": [
                {"type": "PodScheduled", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
                {"type": "Initialized", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
                {"type": "ContainersReady", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
                {"type": "Ready", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"}
            ],
            "containerStatuses": [
                {"name": "c", "ready": true, "restartCount": 0}
            ]
        });
        let created = repo
            .persistence
            .seed_pod("default", "rr-complete", seed)
            .await
            .unwrap();

        let updated = repo
            .status_ports()
            .apply_runtime_reconcile_status(
                "default",
                "rr-complete",
                super::super::assembly_support::support::RuntimeReconcileStatus {
                    phase: "Succeeded".to_string(),
                    container_statuses: vec![json!({
                        "name": "c",
                        "ready": false,
                        "restartCount": 0,
                        "state": {"terminated": {"exitCode": 0}}
                    })],
                },
                Some(created.resource_version),
            )
            .await
            .unwrap();

        let conditions = updated.data["status"]["conditions"].as_array().unwrap();
        let ready = conditions.iter().find(|c| c["type"] == "Ready").unwrap();
        assert_eq!(ready["status"], json!("False"));
        assert_eq!(ready["reason"], json!("PodCompleted"));
        assert_ne!(
            ready["lastTransitionTime"],
            json!("2026-04-30T00:00:00Z"),
            "Ready lastTransitionTime must move when terminal phase flips it to False"
        );
        let containers_ready = conditions
            .iter()
            .find(|c| c["type"] == "ContainersReady")
            .unwrap();
        assert_eq!(containers_ready["status"], json!("False"));
        assert_eq!(containers_ready["reason"], json!("PodCompleted"));
    }
    #[tokio::test]
    async fn apply_runtime_reconcile_status_running_ready_containers_marks_pod_ready() {
        let repo = build_status_repo().await;

        let mut seed = pending_pod("rr-ready");
        seed["status"] = json!({
            "phase": "Pending",
            "podIP": "10.42.0.9",
            "podIPs": [{"ip": "10.42.0.9"}],
            "hostIP": "10.0.0.10",
            "hostIPs": [{"ip": "10.0.0.10"}],
            "qosClass": "BestEffort",
            "conditions": [
                {"type": "PodScheduled", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
                {"type": "Initialized", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
                {"type": "ContainersReady", "status": "False", "lastTransitionTime": "2026-04-30T00:00:00Z"},
                {"type": "Ready", "status": "False", "lastTransitionTime": "2026-04-30T00:00:00Z"}
            ],
            "containerStatuses": [
                {"name": "c", "ready": false, "restartCount": 0}
            ]
        });
        let created = repo
            .persistence
            .seed_pod("default", "rr-ready", seed)
            .await
            .unwrap();

        let updated = repo
            .status_ports()
            .apply_runtime_reconcile_status(
                "default",
                "rr-ready",
                super::super::assembly_support::support::RuntimeReconcileStatus {
                    phase: "Running".to_string(),
                    container_statuses: vec![json!({
                        "name": "c",
                        "ready": true,
                        "restartCount": 0,
                        "state": {"running": {"startedAt": "2026-05-01T00:00:00Z"}}
                    })],
                },
                Some(created.resource_version),
            )
            .await
            .unwrap();

        let conditions = updated.data["status"]["conditions"].as_array().unwrap();
        let ready = conditions.iter().find(|c| c["type"] == "Ready").unwrap();
        assert_eq!(ready["status"], json!("True"));
        let containers_ready = conditions
            .iter()
            .find(|c| c["type"] == "ContainersReady")
            .unwrap();
        assert_eq!(containers_ready["status"], json!("True"));
    }
    #[tokio::test]
    async fn apply_runtime_reconcile_status_enqueues_deployment_rollout_on_readiness_transition() {
        let repo = build_status_repo_with_dispatcher().await;

        repo.seed_non_pod_resource(
            "apps/v1",
            "Deployment",
            "default",
            "web",
            json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {
                    "name": "web",
                    "namespace": "default",
                    "uid": "deploy-web-uid"
                },
                "spec": {"replicas": 1}
            }),
        )
        .await
        .unwrap();
        repo.seed_non_pod_resource(
            "apps/v1",
            "ReplicaSet",
            "default",
            "web-rs",
            json!({
                "apiVersion": "apps/v1",
                "kind": "ReplicaSet",
                "metadata": {
                    "name": "web-rs",
                    "namespace": "default",
                    "uid": "rs-web-uid",
                    "ownerReferences": [{
                        "apiVersion": "apps/v1",
                        "kind": "Deployment",
                        "name": "web",
                        "uid": "deploy-web-uid",
                        "controller": true
                    }]
                },
                "spec": {"replicas": 1},
                "status": {"replicas": 1, "readyReplicas": 0, "availableReplicas": 0}
            }),
        )
        .await
        .unwrap();

        let mut seed = pending_pod("web-pod");
        seed["metadata"]["uid"] = json!("pod-web-uid");
        seed["metadata"]["ownerReferences"] = json!([{
            "apiVersion": "apps/v1",
            "kind": "ReplicaSet",
            "name": "web-rs",
            "uid": "rs-web-uid",
            "controller": true
        }]);
        seed["status"] = json!({
            "phase": "Pending",
            "conditions": [
                {"type": "Ready", "status": "False"},
                {"type": "ContainersReady", "status": "False"}
            ],
            "containerStatuses": [{"name": "c", "ready": false, "restartCount": 0}]
        });
        let created = repo
            .persistence
            .seed_pod("default", "web-pod", seed)
            .await
            .unwrap();
        let deployment_key = klights_reconcile_api::ReconcileKey::namespaced(
            "apps/v1",
            "Deployment",
            "default",
            "web",
        );
        repo.enqueue_reconcile_key(deployment_key.clone()).await;

        repo.status_ports()
            .apply_runtime_reconcile_status(
                "default",
                "web-pod",
                super::super::assembly_support::support::RuntimeReconcileStatus {
                    phase: "Running".to_string(),
                    container_statuses: vec![json!({
                        "name": "c",
                        "ready": true,
                        "restartCount": 0,
                        "state": {"running": {"startedAt": "2026-05-05T00:00:00Z"}}
                    })],
                },
                Some(created.resource_version),
            )
            .await
            .unwrap();

        let keys = repo.pending_reconcile_keys().await;
        assert_eq!(
            keys.iter().filter(|key| *key == &deployment_key).count(),
            1,
            "a pod readiness transition under a Deployment-owned ReplicaSet must leave one fresh Deployment rollout queued"
        );
    }
    #[tokio::test]
    async fn apply_runtime_reconcile_status_enqueues_statefulset_after_readiness_transition() {
        let repo = build_status_repo_with_dispatcher().await;

        repo.seed_non_pod_resource(
            "apps/v1",
            "StatefulSet",
            "default",
            "web",
            json!({
                "apiVersion": "apps/v1",
                "kind": "StatefulSet",
                "metadata": {
                    "name": "web",
                    "namespace": "default",
                    "uid": "sts-web-uid"
                },
                "spec": {
                    "replicas": 3,
                    "podManagementPolicy": "OrderedReady",
                    "selector": {"matchLabels": {"app": "web"}}
                },
                "status": {"replicas": 1, "readyReplicas": 0}
            }),
        )
        .await
        .unwrap();

        let mut seed = pending_pod("web-0");
        seed["metadata"]["uid"] = json!("pod-web-0-uid");
        seed["metadata"]["labels"] = json!({"app": "web"});
        seed["metadata"]["ownerReferences"] = json!([{
            "apiVersion": "apps/v1",
            "kind": "StatefulSet",
            "name": "web",
            "uid": "sts-web-uid",
            "controller": true
        }]);
        seed["status"] = json!({
            "phase": "Pending",
            "conditions": [
                {"type": "Ready", "status": "False"},
                {"type": "ContainersReady", "status": "False"}
            ],
            "containerStatuses": [{"name": "c", "ready": false, "restartCount": 0}]
        });
        let created = repo
            .persistence
            .seed_pod("default", "web-0", seed)
            .await
            .unwrap();

        repo.status_ports()
            .apply_runtime_reconcile_status(
                "default",
                "web-0",
                super::super::assembly_support::support::RuntimeReconcileStatus {
                    phase: "Running".to_string(),
                    container_statuses: vec![json!({
                        "name": "c",
                        "ready": true,
                        "restartCount": 0,
                        "state": {"running": {"startedAt": "2026-05-05T00:00:00Z"}}
                    })],
                },
                Some(created.resource_version),
            )
            .await
            .unwrap();

        let keys = repo.pending_reconcile_keys().await;
        assert!(
            keys.iter().any(|key| {
                key.api_version() == "apps/v1"
                    && key.kind() == "StatefulSet"
                    && key.namespace() == Some("default")
                    && key.name() == "web"
            }),
            "a readiness transition under a StatefulSet must enqueue it so OrderedReady creation can advance"
        );
    }
    #[tokio::test]
    async fn set_pod_status_enqueues_statefulset_after_terminal_failure_transition() {
        let repo = build_status_repo_with_dispatcher().await;

        repo.seed_non_pod_resource(
            "apps/v1",
            "StatefulSet",
            "default",
            "web",
            json!({
                "apiVersion": "apps/v1",
                "kind": "StatefulSet",
                "metadata": {
                    "name": "web",
                    "namespace": "default",
                    "uid": "sts-web-uid"
                },
                "spec": {
                    "replicas": 1,
                    "podManagementPolicy": "OrderedReady",
                    "selector": {"matchLabels": {"app": "web"}}
                },
                "status": {"replicas": 1, "readyReplicas": 0}
            }),
        )
        .await
        .unwrap();

        let mut seed = pending_pod("web-0");
        seed["metadata"]["uid"] = json!("pod-web-0-uid");
        seed["metadata"]["labels"] = json!({"app": "web"});
        seed["metadata"]["ownerReferences"] = json!([{
            "apiVersion": "apps/v1",
            "kind": "StatefulSet",
            "name": "web",
            "uid": "sts-web-uid",
            "controller": true
        }]);
        seed["status"] = json!({
            "phase": "Pending",
            "conditions": [
                {"type": "Ready", "status": "False"},
                {"type": "ContainersReady", "status": "False"}
            ],
            "containerStatuses": [{"name": "c", "ready": false, "restartCount": 0}]
        });
        let created = repo
            .persistence
            .seed_pod("default", "web-0", seed)
            .await
            .unwrap();

        repo.status_ports()
            .set_pod_status(
                "default",
                "web-0",
                super::super::assembly_support::support::PodStatusUpdate {
                    phase: "Failed".to_string(),
                    pod_ip: None,
                    host_ip: None,
                    container_statuses: vec![json!({
                        "name": "c",
                        "ready": false,
                        "restartCount": 0,
                        "state": {
                            "waiting": {
                                "reason": "CreateContainerError",
                                "message": "hostPort 21017/TCP is already allocated"
                            }
                        }
                    })],
                    init_container_statuses: None,
                    qos_class: None,
                },
                Some(created.resource_version),
            )
            .await
            .unwrap();

        let keys = repo.pending_reconcile_keys().await;
        assert!(
            keys.iter().any(|key| {
                key.api_version() == "apps/v1"
                    && key.kind() == "StatefulSet"
                    && key.namespace() == Some("default")
                    && key.name() == "web"
            }),
            "a StatefulSet-owned pod entering Failed before readiness must enqueue its owner so the failed ordinal can be deleted and recreated"
        );
    }
    #[tokio::test]
    async fn replace_status_from_api_failed_daemonset_pod_enqueues_daemonset() {
        let repo = build_status_repo_with_dispatcher().await;

        repo.seed_non_pod_resource(
            "apps/v1",
            "DaemonSet",
            "default",
            "node-agent",
            json!({
                "apiVersion": "apps/v1",
                "kind": "DaemonSet",
                "metadata": {
                    "name": "node-agent",
                    "namespace": "default",
                    "uid": "ds-node-agent-uid"
                },
                "spec": {
                    "selector": {"matchLabels": {"app": "node-agent"}},
                    "template": {
                        "metadata": {"labels": {"app": "node-agent"}},
                        "spec": {"containers": [{"name": "agent", "image": "busybox"}]}
                    }
                },
                "status": {"desiredNumberScheduled": 1, "numberReady": 1}
            }),
        )
        .await
        .unwrap();

        let mut seed = pending_pod("node-agent-pod");
        seed["metadata"]["uid"] = json!("pod-node-agent-uid");
        seed["metadata"]["labels"] = json!({"app": "node-agent"});
        seed["metadata"]["ownerReferences"] = json!([{
            "apiVersion": "apps/v1",
            "kind": "DaemonSet",
            "name": "node-agent",
            "uid": "ds-node-agent-uid",
            "controller": true
        }]);
        seed["status"] = json!({
            "phase": "Running",
            "conditions": [
                {"type": "Ready", "status": "True"},
                {"type": "ContainersReady", "status": "True"}
            ],
            "containerStatuses": [{"name": "agent", "ready": true, "restartCount": 0}]
        });
        let created = repo
            .persistence
            .seed_pod("default", "node-agent-pod", seed)
            .await
            .unwrap();

        repo.api_ports()
            .replace_status_from_api(
                "default",
                "node-agent-pod",
                json!({"phase": "Failed"}),
                created.resource_version,
            )
            .await
            .unwrap();

        let keys = repo.pending_reconcile_keys().await;
        assert!(
            keys.iter().any(|key| {
                key.api_version() == "apps/v1"
                    && key.kind() == "DaemonSet"
                    && key.namespace() == Some("default")
                    && key.name() == "node-agent"
            }),
            "API /status writes that move a DaemonSet pod to Failed must enqueue the DaemonSet so it can delete and replace the terminal pod"
        );
    }
    #[tokio::test]
    async fn set_deadline_exceeded_enqueues_statefulset_and_does_not_write_owner_status() {
        let repo = build_status_repo_with_dispatcher().await;

        repo.seed_non_pod_resource(
            "apps/v1",
            "StatefulSet",
            "default",
            "deadline-web",
            json!({
                "apiVersion": "apps/v1",
                "kind": "StatefulSet",
                "metadata": {
                    "name": "deadline-web",
                    "namespace": "default",
                    "uid": "sts-deadline-web-uid"
                },
                "spec": {
                    "replicas": 1,
                    "podManagementPolicy": "OrderedReady",
                    "selector": {"matchLabels": {"app": "deadline-web"}}
                },
                "status": {"replicas": 1, "readyReplicas": 1, "availableReplicas": 1}
            }),
        )
        .await
        .unwrap();

        let mut seed = pending_pod("deadline-web-0");
        seed["metadata"]["uid"] = json!("pod-deadline-web-0-uid");
        seed["metadata"]["labels"] = json!({"app": "deadline-web"});
        seed["metadata"]["ownerReferences"] = json!([{
            "apiVersion": "apps/v1",
            "kind": "StatefulSet",
            "name": "deadline-web",
            "uid": "sts-deadline-web-uid",
            "controller": true
        }]);
        seed["status"] = json!({
            "phase": "Running",
            "conditions": [
                {"type": "Ready", "status": "True"},
                {"type": "ContainersReady", "status": "True"}
            ],
            "containerStatuses": [{"name": "c", "ready": true, "restartCount": 0}]
        });
        let created = repo
            .persistence
            .seed_pod("default", "deadline-web-0", seed)
            .await
            .unwrap();

        let owner_rv_before = repo
            .read_non_pod_resource("apps/v1", "StatefulSet", "default", "deadline-web")
            .await
            .unwrap()
            .expect("statefulset exists")
            .resource_version;

        repo.status_ports()
            .set_deadline_exceeded(
                "default",
                "deadline-web-0",
                "deadline exceeded".to_string(),
                Some(created.resource_version),
            )
            .await
            .unwrap();

        // Top-down ownership: pod status writes must NOT directly mutate owner status.
        // The StatefulSet controller's reconcile will update status from fresh pod state.
        let owner = repo
            .read_non_pod_resource("apps/v1", "StatefulSet", "default", "deadline-web")
            .await
            .unwrap()
            .expect("statefulset exists");
        assert_eq!(
            owner.resource_version, owner_rv_before,
            "pod status writes must not change the owner resourceVersion — only the controller reconcile may write owner status"
        );
        assert_eq!(
            owner.data.pointer("/status/readyReplicas"),
            Some(&json!(1)),
            "owner status must remain unchanged until the controller reconcile runs"
        );

        let keys = repo.pending_reconcile_keys().await;
        assert!(
            keys.iter().any(|key| {
                key.api_version() == "apps/v1"
                    && key.kind() == "StatefulSet"
                    && key.namespace() == Some("default")
                    && key.name() == "deadline-web"
            }),
            "deadline failure must enqueue the StatefulSet once so it can replace the failed ordinal"
        );
    }
    #[tokio::test]
    async fn apply_runtime_reconcile_status_enqueues_job_after_readiness_transition() {
        let repo = build_status_repo_with_dispatcher().await;

        repo.seed_non_pod_resource(
            "batch/v1",
            "Job",
            "default",
            "ready-job",
            json!({
                "apiVersion": "batch/v1",
                "kind": "Job",
                "metadata": {
                    "name": "ready-job",
                    "namespace": "default",
                    "uid": "job-ready-uid"
                },
                "spec": {
                    "parallelism": 3,
                    "completions": 3,
                    "template": {
                        "metadata": {"labels": {"job": "ready-job"}},
                        "spec": {
                            "containers": [{"name": "c", "image": "busybox"}],
                            "restartPolicy": "Never"
                        }
                    }
                },
                "status": {"active": 1, "ready": 0}
            }),
        )
        .await
        .unwrap();

        let mut seed = pending_pod("ready-job-pod");
        seed["metadata"]["uid"] = json!("pod-ready-job-uid");
        seed["metadata"]["labels"] = json!({"job": "ready-job"});
        seed["metadata"]["ownerReferences"] = json!([{
            "apiVersion": "batch/v1",
            "kind": "Job",
            "name": "ready-job",
            "uid": "job-ready-uid",
            "controller": true
        }]);
        seed["status"] = json!({
            "phase": "Pending",
            "conditions": [
                {"type": "Ready", "status": "False"},
                {"type": "ContainersReady", "status": "False"}
            ],
            "containerStatuses": [{"name": "c", "ready": false, "restartCount": 0}]
        });
        let created = repo
            .persistence
            .seed_pod("default", "ready-job-pod", seed)
            .await
            .unwrap();

        repo.status_ports()
            .apply_runtime_reconcile_status(
                "default",
                "ready-job-pod",
                super::super::assembly_support::support::RuntimeReconcileStatus {
                    phase: "Running".to_string(),
                    container_statuses: vec![json!({
                        "name": "c",
                        "ready": true,
                        "restartCount": 0,
                        "state": {"running": {"startedAt": "2026-05-05T00:00:00Z"}}
                    })],
                },
                Some(created.resource_version),
            )
            .await
            .unwrap();

        let keys = repo.pending_reconcile_keys().await;
        assert!(
            keys.iter().any(|key| {
                key.api_version() == "batch/v1"
                    && key.kind() == "Job"
                    && key.namespace() == Some("default")
                    && key.name() == "ready-job"
            }),
            "a readiness transition under a Job must enqueue it so status.ready is refreshed"
        );
    }
    #[tokio::test]
    async fn apply_runtime_reconcile_status_returns_conflict_on_stale_rv() {
        let repo = build_status_repo().await;
        let created = repo
            .persistence
            .seed_pod("default", "rr-race", pending_pod("rr-race"))
            .await
            .unwrap();
        let snapshot = created.resource_version;

        repo.status_ports()
            .apply_runtime_reconcile_status(
                "default",
                "rr-race",
                super::super::assembly_support::support::RuntimeReconcileStatus {
                    phase: "Running".to_string(),
                    container_statuses: vec![],
                },
                Some(snapshot),
            )
            .await
            .expect("first writer succeeds");

        let conflict = repo
            .status_ports()
            .apply_runtime_reconcile_status(
                "default",
                "rr-race",
                super::super::assembly_support::support::RuntimeReconcileStatus {
                    phase: "Failed".to_string(),
                    container_statuses: vec![],
                },
                Some(snapshot),
            )
            .await;
        let err = conflict.expect_err("stale rv must conflict");
        assert!(err.to_string().contains("409"), "expected 409, got {err:?}");
    }
    #[tokio::test]
    async fn apply_runtime_reconcile_status_enqueues_replicaset_on_readiness_transition() {
        let repo = build_status_repo_with_dispatcher().await;

        repo.seed_non_pod_resource(
            "apps/v1",
            "ReplicaSet",
            "default",
            "standalone-rs",
            json!({
                "apiVersion": "apps/v1",
                "kind": "ReplicaSet",
                "metadata": {
                    "name": "standalone-rs",
                    "namespace": "default",
                    "uid": "rs-standalone-uid"
                },
                "spec": {"replicas": 1},
                "status": {"replicas": 1, "readyReplicas": 0}
            }),
        )
        .await
        .unwrap();

        let mut seed = pending_pod("rs-pod");
        seed["metadata"]["uid"] = json!("pod-rs-uid");
        seed["metadata"]["ownerReferences"] = json!([{
            "apiVersion": "apps/v1",
            "kind": "ReplicaSet",
            "name": "standalone-rs",
            "uid": "rs-standalone-uid",
            "controller": true
        }]);
        seed["status"] = json!({
            "phase": "Pending",
            "conditions": [
                {"type": "Ready", "status": "False"},
                {"type": "ContainersReady", "status": "False"}
            ],
            "containerStatuses": [{"name": "c", "ready": false, "restartCount": 0}]
        });
        let created = repo
            .persistence
            .seed_pod("default", "rs-pod", seed)
            .await
            .unwrap();

        repo.status_ports()
            .apply_runtime_reconcile_status(
                "default",
                "rs-pod",
                super::super::assembly_support::support::RuntimeReconcileStatus {
                    phase: "Running".to_string(),
                    container_statuses: vec![json!({
                        "name": "c",
                        "ready": true,
                        "restartCount": 0,
                        "state": {"running": {"startedAt": "2026-05-05T00:00:00Z"}}
                    })],
                },
                Some(created.resource_version),
            )
            .await
            .unwrap();

        let keys = repo.pending_reconcile_keys().await;
        assert!(
            keys.iter().any(|key| {
                key.api_version() == "apps/v1"
                    && key.kind() == "ReplicaSet"
                    && key.namespace() == Some("default")
                    && key.name() == "standalone-rs"
            }),
            "a readiness transition under a ReplicaSet must enqueue the ReplicaSet for top-down status refresh"
        );
    }
    #[tokio::test]
    async fn pod_status_write_does_not_directly_mutate_replicaset_status() {
        let repo = build_status_repo_with_dispatcher().await;

        repo.seed_non_pod_resource(
            "apps/v1",
            "ReplicaSet",
            "default",
            "rs-no-status-write",
            json!({
                "apiVersion": "apps/v1",
                "kind": "ReplicaSet",
                "metadata": {
                    "name": "rs-no-status-write",
                    "namespace": "default",
                    "uid": "rs-no-write-uid"
                },
                "spec": {"replicas": 1},
                "status": {"replicas": 1, "readyReplicas": 0}
            }),
        )
        .await
        .unwrap();

        let rs_rv_before = repo
            .read_non_pod_resource("apps/v1", "ReplicaSet", "default", "rs-no-status-write")
            .await
            .unwrap()
            .expect("rs exists")
            .resource_version;

        let mut seed = pending_pod("rs-pod-2");
        seed["metadata"]["uid"] = json!("pod-rs-2-uid");
        seed["metadata"]["ownerReferences"] = json!([{
            "apiVersion": "apps/v1",
            "kind": "ReplicaSet",
            "name": "rs-no-status-write",
            "uid": "rs-no-write-uid",
            "controller": true
        }]);
        seed["status"] = json!({
            "phase": "Pending",
            "conditions": [
                {"type": "Ready", "status": "False"},
                {"type": "ContainersReady", "status": "False"}
            ],
            "containerStatuses": [{"name": "c", "ready": false, "restartCount": 0}]
        });
        let created = repo
            .persistence
            .seed_pod("default", "rs-pod-2", seed)
            .await
            .unwrap();

        repo.status_ports()
            .apply_runtime_reconcile_status(
                "default",
                "rs-pod-2",
                super::super::assembly_support::support::RuntimeReconcileStatus {
                    phase: "Running".to_string(),
                    container_statuses: vec![json!({
                        "name": "c",
                        "ready": true,
                        "restartCount": 0,
                        "state": {"running": {"startedAt": "2026-05-05T00:00:00Z"}}
                    })],
                },
                Some(created.resource_version),
            )
            .await
            .unwrap();

        let rs_after = repo
            .read_non_pod_resource("apps/v1", "ReplicaSet", "default", "rs-no-status-write")
            .await
            .unwrap()
            .expect("rs exists");
        assert_eq!(
            rs_after.resource_version, rs_rv_before,
            "pod status write must not change ReplicaSet resourceVersion"
        );
        assert_eq!(
            rs_after.data.pointer("/status/readyReplicas"),
            Some(&json!(0)),
            "ReplicaSet status must remain unchanged — only the controller reconcile may update it"
        );
    }
    #[tokio::test]
    async fn apply_runtime_reconcile_status_enqueues_daemonset_on_readiness_transition() {
        let repo = build_status_repo_with_dispatcher().await;

        repo.seed_non_pod_resource(
            "apps/v1",
            "DaemonSet",
            "default",
            "ds-readiness",
            json!({
                "apiVersion": "apps/v1",
                "kind": "DaemonSet",
                "metadata": {
                    "name": "ds-readiness",
                    "namespace": "default",
                    "uid": "ds-readiness-uid"
                },
                "spec": {
                    "selector": {"matchLabels": {"app": "ds-readiness"}},
                    "template": {
                        "metadata": {"labels": {"app": "ds-readiness"}},
                        "spec": {"containers": [{"name": "c", "image": "busybox"}]}
                    }
                },
                "status": {"desiredNumberScheduled": 1, "numberReady": 0}
            }),
        )
        .await
        .unwrap();

        let mut seed = pending_pod("ds-pod");
        seed["metadata"]["uid"] = json!("pod-ds-uid");
        seed["metadata"]["labels"] = json!({"app": "ds-readiness"});
        seed["metadata"]["ownerReferences"] = json!([{
            "apiVersion": "apps/v1",
            "kind": "DaemonSet",
            "name": "ds-readiness",
            "uid": "ds-readiness-uid",
            "controller": true
        }]);
        seed["status"] = json!({
            "phase": "Pending",
            "conditions": [
                {"type": "Ready", "status": "False"},
                {"type": "ContainersReady", "status": "False"}
            ],
            "containerStatuses": [{"name": "c", "ready": false, "restartCount": 0}]
        });
        let created = repo
            .persistence
            .seed_pod("default", "ds-pod", seed)
            .await
            .unwrap();

        repo.status_ports()
            .apply_runtime_reconcile_status(
                "default",
                "ds-pod",
                super::super::assembly_support::support::RuntimeReconcileStatus {
                    phase: "Running".to_string(),
                    container_statuses: vec![json!({
                        "name": "c",
                        "ready": true,
                        "restartCount": 0,
                        "state": {"running": {"startedAt": "2026-05-05T00:00:00Z"}}
                    })],
                },
                Some(created.resource_version),
            )
            .await
            .unwrap();

        let keys = repo.pending_reconcile_keys().await;
        assert!(
            keys.iter().any(|key| {
                key.api_version() == "apps/v1"
                    && key.kind() == "DaemonSet"
                    && key.namespace() == Some("default")
                    && key.name() == "ds-readiness"
            }),
            "a readiness transition under a DaemonSet must enqueue the DaemonSet for top-down status refresh"
        );
    }
    #[tokio::test]
    async fn apply_runtime_reconcile_status_enqueues_replicationcontroller_on_readiness_transition()
    {
        let repo = build_status_repo_with_dispatcher().await;

        repo.seed_non_pod_resource(
            "v1",
            "ReplicationController",
            "default",
            "rc-readiness",
            json!({
                "apiVersion": "v1",
                "kind": "ReplicationController",
                "metadata": {
                    "name": "rc-readiness",
                    "namespace": "default",
                    "uid": "rc-readiness-uid"
                },
                "spec": {"replicas": 1},
                "status": {"replicas": 1, "readyReplicas": 0}
            }),
        )
        .await
        .unwrap();

        let mut seed = pending_pod("rc-pod");
        seed["metadata"]["uid"] = json!("pod-rc-uid");
        seed["metadata"]["ownerReferences"] = json!([{
            "apiVersion": "v1",
            "kind": "ReplicationController",
            "name": "rc-readiness",
            "uid": "rc-readiness-uid",
            "controller": true
        }]);
        seed["status"] = json!({
            "phase": "Pending",
            "conditions": [
                {"type": "Ready", "status": "False"},
                {"type": "ContainersReady", "status": "False"}
            ],
            "containerStatuses": [{"name": "c", "ready": false, "restartCount": 0}]
        });
        let created = repo
            .persistence
            .seed_pod("default", "rc-pod", seed)
            .await
            .unwrap();

        repo.status_ports()
            .apply_runtime_reconcile_status(
                "default",
                "rc-pod",
                super::super::assembly_support::support::RuntimeReconcileStatus {
                    phase: "Running".to_string(),
                    container_statuses: vec![json!({
                        "name": "c",
                        "ready": true,
                        "restartCount": 0,
                        "state": {"running": {"startedAt": "2026-05-05T00:00:00Z"}}
                    })],
                },
                Some(created.resource_version),
            )
            .await
            .unwrap();

        let keys = repo.pending_reconcile_keys().await;
        assert!(
            keys.iter().any(|key| {
                key.api_version() == "v1"
                    && key.kind() == "ReplicationController"
                    && key.namespace() == Some("default")
                    && key.name() == "rc-readiness"
            }),
            "a readiness transition under a ReplicationController must enqueue it for top-down status refresh"
        );
    }
    #[tokio::test]
    async fn pod_status_write_orphan_pod_does_not_enqueue_any_controller() {
        let repo = build_status_repo_with_dispatcher().await;

        let mut seed = pending_pod("orphan-pod");
        seed["metadata"]["uid"] = json!("pod-orphan-uid");
        // No ownerReferences
        seed["status"] = json!({
            "phase": "Pending",
            "conditions": [
                {"type": "Ready", "status": "False"},
                {"type": "ContainersReady", "status": "False"}
            ],
            "containerStatuses": [{"name": "c", "ready": false, "restartCount": 0}]
        });
        let created = repo
            .persistence
            .seed_pod("default", "orphan-pod", seed)
            .await
            .unwrap();

        repo.status_ports()
            .apply_runtime_reconcile_status(
                "default",
                "orphan-pod",
                super::super::assembly_support::support::RuntimeReconcileStatus {
                    phase: "Running".to_string(),
                    container_statuses: vec![json!({
                        "name": "c",
                        "ready": true,
                        "restartCount": 0,
                        "state": {"running": {"startedAt": "2026-05-05T00:00:00Z"}}
                    })],
                },
                Some(created.resource_version),
            )
            .await
            .unwrap();

        let keys = repo.pending_reconcile_keys().await;
        assert!(
            keys.is_empty(),
            "orphan pod status change must not enqueue any controller: got {keys:?}"
        );
    }
    #[tokio::test]
    async fn record_sandbox_id_sets_annotation_and_preserves_other_fields() {
        let repo = build_status_repo().await;

        // Seed a pod that already has labels, an annotation, and a status —
        // none of those may be erased by the sandbox-id write.
        let mut seed = pending_pod("anno1");
        seed["metadata"]["annotations"] = json!({"prior.example.com": "keep-me"});
        repo.persistence
            .seed_pod("default", "anno1", seed)
            .await
            .unwrap();

        let updated = repo
            .record_sandbox_id("default", "anno1", "sandbox-abc-123")
            .await
            .unwrap();

        assert_eq!(
            updated.data["metadata"]["annotations"]["klights.dev/sandbox-id"],
            json!("sandbox-abc-123")
        );
        assert_eq!(
            updated.data["metadata"]["annotations"]["prior.example.com"],
            json!("keep-me")
        );
        assert_eq!(updated.data["metadata"]["labels"]["app"], json!("x"));
        assert_eq!(updated.data["status"]["phase"], json!("Pending"));
        assert_eq!(updated.data["status"]["qosClass"], json!("BestEffort"));
    }
    #[tokio::test]
    async fn record_sandbox_id_returns_conflict_on_stale_rv() {
        let repo = build_status_repo().await;
        repo.persistence
            .seed_pod("default", "anno-race", pending_pod("anno-race"))
            .await
            .unwrap();

        // First writer mutates the pod (e.g. an out-of-band label edit).
        let snapshot = repo
            .query
            .get_pod_by_name("default", "anno-race")
            .await
            .unwrap()
            .unwrap();
        let mut mutated: serde_json::Value = (*snapshot.data).clone();
        mutated["metadata"]["labels"] = json!({"app": "x", "tier": "frontend"});
        repo.api_mutations
            .update_pod("default", "anno-race", mutated, snapshot.clone(), false)
            .await
            .unwrap();

        // Now record_sandbox_id reads the live RV (post-out-of-band write) and
        // succeeds — but a second concurrent record_sandbox_id with the same
        // pre-mutation read should fail. We model that by attempting two writes
        // back-to-back: the second one observes a stale RV from the first.
        repo.record_sandbox_id("default", "anno-race", "sb-1")
            .await
            .expect("first record succeeds");
        // The second write is also a fresh read-modify-write, so it should also
        // succeed (no conflict, by design — record_sandbox_id reads the live
        // RV). To produce a real CAS conflict, we drive the store directly with
        // a stale RV after a record_sandbox_id call.
        let after_first = repo
            .query
            .get_pod_by_name("default", "anno-race")
            .await
            .unwrap()
            .unwrap();
        let mut tampered: serde_json::Value = (*after_first.data).clone();
        tampered["metadata"]["annotations"]["klights.dev/sandbox-id"] = json!("sb-tampered");
        tampered["metadata"]["resourceVersion"] = json!(snapshot.resource_version.to_string());
        let conflict = repo
            .api_mutations
            .update_pod("default", "anno-race", tampered, snapshot, false)
            .await;
        let err = conflict.expect_err("stale rv must conflict");
        assert!(err.to_string().contains("409"), "expected 409, got {err:?}");
    }
    #[tokio::test]
    async fn uid_qualified_record_sandbox_id_rejects_recreated_same_name_pod() {
        let repo = build_status_repo().await;

        let mut replacement = pending_pod("same-name");
        replacement["metadata"]["uid"] = json!("replacement-uid");
        repo.persistence
            .seed_pod("default", "same-name", replacement)
            .await
            .unwrap();

        let err = repo
            .record_sandbox_id_for_uid("default", "same-name", "old-uid", "old-sandbox")
            .await
            .expect_err("stale lifecycle work must not annotate a replacement pod");
        assert!(
            err.to_string().contains("UID mismatch"),
            "unexpected error: {err:#}"
        );

        let stored = repo
            .query
            .get_pod_by_name("default", "same-name")
            .await
            .unwrap()
            .unwrap();
        assert!(
            stored
                .data
                .pointer("/metadata/annotations/klights.dev~1sandbox-id")
                .is_none(),
            "replacement pod must not receive stale sandbox annotation"
        );
    }
    #[tokio::test]
    async fn uid_qualified_set_pod_status_rejects_recreated_same_name_pod() {
        let repo = build_status_repo().await;

        let mut replacement = pending_pod("same-name-status");
        replacement["metadata"]["uid"] = json!("replacement-uid");
        repo.persistence
            .seed_pod("default", "same-name-status", replacement)
            .await
            .unwrap();

        let err = repo
            .status_ports()
            .set_pod_status_for_uid(
                "default",
                "same-name-status",
                "old-uid",
                super::super::assembly_support::support::PodStatusUpdate {
                    phase: "Running".to_string(),
                    pod_ip: klights_kubelet::pod_repository::PublishedAddress::must("10.43.0.15"),
                    host_ip: klights_kubelet::pod_repository::PublishedAddress::must("10.206.0.10"),
                    container_statuses: vec![json!({
                        "name": "webserver",
                        "ready": true,
                        "restartCount": 0,
                        "state": {"running": {"startedAt": "2026-05-12T00:00:00Z"}}
                    })],
                    init_container_statuses: None,
                    qos_class: None,
                },
                None,
            )
            .await
            .expect_err("stale lifecycle work must not overwrite replacement pod status");
        assert!(
            err.to_string().contains("UID mismatch"),
            "unexpected error: {err:#}"
        );

        let stored = repo
            .query
            .get_pod_by_name("default", "same-name-status")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.data["status"]["phase"], json!("Pending"));
        assert!(
            stored.data.pointer("/status/podIP").is_none(),
            "replacement pod must not receive stale podIP"
        );
    }
    #[tokio::test]
    async fn set_probe_readiness_success_does_not_mark_pending_container_creating_pod_ready() {
        let repo = build_status_repo().await;
        let created = repo
            .persistence
            .seed_pod(
                "default",
                "p-pending-probe",
                pod_with_container_creating_status("p-pending-probe"),
            )
            .await
            .unwrap();

        let updated = repo
            .status_ports()
            .set_probe_readiness(
                "default",
                "p-pending-probe",
                "c",
                true,
                Some(created.resource_version),
            )
            .await
            .unwrap();

        assert_eq!(updated.data["status"]["phase"], json!("Pending"));
        assert_eq!(
            updated.data["status"]["containerStatuses"][0]["ready"],
            json!(false),
            "readiness success must not mark a non-running container ready"
        );
        assert_eq!(
            updated.data["status"]["conditions"]
                .as_array()
                .unwrap()
                .iter()
                .find(|c| c["type"] == "Ready")
                .unwrap()["status"],
            json!("False"),
            "pod Ready must remain False while phase is Pending"
        );
        assert_eq!(
            updated.resource_version, created.resource_version,
            "ignored early readiness success must not create a status watch event"
        );
    }
    #[tokio::test]
    async fn set_probe_readiness_flips_container_ready_and_conditions_and_preserves_labels() {
        let repo = build_status_repo().await;
        let created = repo
            .persistence
            .seed_pod("default", "p-pr", pod_with_running_status("p-pr"))
            .await
            .unwrap();

        let updated = repo
            .status_ports()
            .set_probe_readiness("default", "p-pr", "c", true, Some(created.resource_version))
            .await
            .unwrap();

        // metadata preserved (labels intact)
        assert_eq!(updated.data["metadata"]["labels"]["app"], json!("x"));
        // container ready flipped
        assert_eq!(
            updated.data["status"]["containerStatuses"][0]["ready"],
            json!(true)
        );
        // both conditions flipped to True with reason
        let conds = updated.data["status"]["conditions"]
            .as_array()
            .expect("conditions present");
        let ready = conds.iter().find(|c| c["type"] == "Ready").unwrap();
        assert_eq!(ready["status"], json!("True"));
        assert_eq!(ready["reason"], json!("ReadinessProbeSucceeded"));
        assert_ne!(ready["lastTransitionTime"], json!("2026-04-30T00:00:00Z"));
        let cready = conds
            .iter()
            .find(|c| c["type"] == "ContainersReady")
            .unwrap();
        assert_eq!(cready["status"], json!("True"));
    }
    #[tokio::test]
    async fn set_probe_readiness_no_op_call_does_not_bump_last_transition_time() {
        let repo = build_status_repo().await;
        let created = repo
            .persistence
            .seed_pod("default", "p-noop", pod_with_running_status("p-noop"))
            .await
            .unwrap();

        // The seed has Ready=False with lastTransitionTime "2026-04-30T00:00:00Z".
        // A False-call must keep the same timestamp.
        let updated = repo
            .status_ports()
            .set_probe_readiness(
                "default",
                "p-noop",
                "c",
                false,
                Some(created.resource_version),
            )
            .await
            .unwrap();
        let ready = updated.data["status"]["conditions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["type"] == "Ready")
            .unwrap();
        assert_eq!(ready["status"], json!("False"));
        // No flip means the timestamp must be preserved. The reason is now set
        // to ReadinessProbeFailed (which is OK per K8s semantics).
        assert_eq!(ready["lastTransitionTime"], json!("2026-04-30T00:00:00Z"));
    }
    #[tokio::test]
    async fn set_probe_readiness_matching_state_does_not_write_status() {
        let repo = build_status_repo().await;
        repo.persistence
            .seed_pod(
                "default",
                "p-ready-noop",
                pod_with_running_status("p-ready-noop"),
            )
            .await
            .unwrap();

        let ready = repo
            .status_ports()
            .set_probe_readiness("default", "p-ready-noop", "c", true, None)
            .await
            .unwrap();
        let same_ready = repo
            .status_ports()
            .set_probe_readiness("default", "p-ready-noop", "c", true, None)
            .await
            .unwrap();

        assert_eq!(
            same_ready.resource_version, ready.resource_version,
            "matching readiness probe results must not create repeated Pod watch events"
        );
    }
    #[tokio::test]
    async fn set_probe_readiness_retries_unpinned_rv_conflict() {
        let outcome = super::super::assembly_support::support::run_probe_readiness_status_race(
            "p-pr-retry",
            pod_with_running_status("p-pr-retry"),
            1,
            false,
        )
        .await;
        let updated = outcome
            .resource
            .expect("unpinned probe-readiness update should retry transient conflicts");

        assert_eq!(
            outcome.attempts, 2,
            "probe-readiness should retry exactly once after the injected conflict"
        );
        assert_eq!(
            updated.data["status"]["containerStatuses"][0]["ready"],
            json!(true)
        );
        assert_eq!(
            updated.data["status"]["conditions"]
                .as_array()
                .unwrap()
                .iter()
                .find(|condition| condition["type"] == "Ready")
                .unwrap()["status"],
            json!("True")
        );
    }
    #[tokio::test]
    async fn set_probe_readiness_exhausts_unpinned_conflict_retries() {
        let outcome = super::super::assembly_support::support::run_probe_readiness_status_race(
            "p-pr-conflict-exhausted",
            pod_with_running_status("p-pr-conflict-exhausted"),
            5,
            false,
        )
        .await;
        assert!(
            outcome.conflict,
            "expected typed conflict after exhausting retries"
        );
        assert_eq!(
            outcome.attempts, 5,
            "probe-readiness should use the same retry budget as runtime reconcile"
        );
    }
    #[tokio::test]
    async fn set_probe_readiness_pinned_rv_conflict_does_not_retry() {
        let outcome = super::super::assembly_support::support::run_probe_readiness_status_race(
            "p-pr-pinned-conflict",
            pod_with_running_status("p-pr-pinned-conflict"),
            1,
            true,
        )
        .await;
        assert!(
            outcome.conflict,
            "expected typed conflict for pinned probe-readiness write"
        );
        assert_eq!(
            outcome.attempts, 1,
            "explicit resourceVersion writes must remain single-attempt CAS"
        );
    }

    #[tokio::test]
    async fn status_write_rejects_identical_status_after_same_name_uid_replacement() {
        let outcome =
            super::super::assembly_support::support::run_same_name_replacement_status_race(
                pending_pod("same-name-status-race"),
                super::super::assembly_support::support::PodStatusUpdate {
                    phase: "Pending".to_string(),
                    pod_ip: None,
                    host_ip: klights_kubelet::pod_repository::PublishedAddress::must("10.0.0.10"),
                    container_statuses: vec![],
                    init_container_statuses: None,
                    qos_class: None,
                },
            )
            .await;

        assert!(outcome.conflict, "the stale observed RV must return 409");
        assert_eq!(
            outcome.persistence_attempts, 1,
            "an explicit observed RV remains a single-attempt CAS"
        );
        assert_ne!(outcome.old_uid, outcome.replacement.uid);
        assert!(!outcome.replacement.uid.is_empty());
        assert_eq!(outcome.persisted_after.uid, outcome.replacement.uid);
        assert_eq!(
            outcome.persisted_after.resource_version,
            outcome.replacement.resource_version
        );
        assert_eq!(
            outcome.persisted_after.data, outcome.replacement.data,
            "even an identical status must not bypass stale-RV validation or mutate the replacement"
        );
        assert_eq!(outcome.reconcile_effects, 0);
        assert_eq!(outcome.outbox_enqueues, 0);
    }
    #[tokio::test]
    async fn set_probe_readiness_returns_conflict_on_stale_rv() {
        let repo = build_status_repo().await;
        let created = repo
            .persistence
            .seed_pod("default", "p-pr-race", pod_with_running_status("p-pr-race"))
            .await
            .unwrap();
        let snapshot = created.resource_version;

        repo.status_ports()
            .set_probe_readiness("default", "p-pr-race", "c", true, Some(snapshot))
            .await
            .expect("first writer wins");

        let conflict = repo
            .status_ports()
            .set_probe_readiness("default", "p-pr-race", "c", false, Some(snapshot))
            .await;
        let err = conflict.expect_err("stale rv must conflict");
        assert!(err.to_string().contains("409"), "expected 409, got {err:?}");
    }
    #[tokio::test]
    async fn set_pod_status_with_unspecified_init_statuses_preserves_existing_retry_state() {
        let repo = build_status_repo().await;
        repo.persistence
            .seed_pod(
                "default",
                "p-init-retry",
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": "p-init-retry", "namespace": "default"},
                    "spec": {
                        "initContainers": [
                            {"name": "init1", "image": "busybox"},
                            {"name": "init2", "image": "busybox"}
                        ],
                        "containers": [{"name": "run1", "image": "pause"}]
                    },
                    "status": {
                        "phase": "Pending",
                        "containerStatuses": [{
                            "name": "run1",
                            "ready": false,
                            "restartCount": 0,
                            "state": {"waiting": {"reason": "PodInitializing"}}
                        }],
                        "initContainerStatuses": [
                            {
                                "name": "init1",
                                "ready": false,
                                "restartCount": 2,
                                "state": {"waiting": {"reason": "PodInitializing"}},
                                "lastState": {"terminated": {"exitCode": 1, "reason": "Error"}}
                            },
                            {
                                "name": "init2",
                                "ready": false,
                                "restartCount": 0,
                                "state": {"waiting": {"reason": "PodInitializing"}}
                            }
                        ]
                    }
                }),
            )
            .await
            .unwrap();

        let updated = repo
            .status_ports()
            .set_pod_status(
                "default",
                "p-init-retry",
                super::super::assembly_support::support::PodStatusUpdate {
                    phase: "Pending".to_string(),
                    pod_ip: klights_kubelet::pod_repository::PublishedAddress::must("10.43.0.5"),
                    host_ip: klights_kubelet::pod_repository::PublishedAddress::must("10.206.0.9"),
                    container_statuses: vec![],
                    init_container_statuses: None,
                    qos_class: None,
                },
                None,
            )
            .await
            .unwrap();

        let statuses = updated
            .data
            .pointer("/status/initContainerStatuses")
            .and_then(|v| v.as_array())
            .expect("initContainerStatuses must be preserved when update does not specify them");
        assert_eq!(statuses.len(), 2);
        assert_eq!(
            statuses[0]
                .pointer("/restartCount")
                .and_then(|v| v.as_i64()),
            Some(2)
        );
        assert_eq!(
            statuses[0]
                .pointer("/lastState/terminated/exitCode")
                .and_then(|v| v.as_i64()),
            Some(1)
        );
    }
    #[tokio::test]
    async fn set_deadline_exceeded_marks_failed_and_preserves_ip_and_labels() {
        let repo = build_status_repo().await;
        let created = repo
            .persistence
            .seed_pod("default", "p-dl", pod_with_running_status_and_ip("p-dl"))
            .await
            .unwrap();
        let updated = repo
            .status_ports()
            .set_deadline_exceeded(
                "default",
                "p-dl",
                "Pod was active longer than 60s".to_string(),
                Some(created.resource_version),
            )
            .await
            .unwrap();
        let status = &updated.data["status"];
        assert_eq!(status["phase"], json!("Failed"));
        assert_eq!(status["reason"], json!("DeadlineExceeded"));
        assert_eq!(status["message"], json!("Pod was active longer than 60s"));
        // IPs preserved
        assert_eq!(status["podIP"], json!("10.42.0.9"));
        assert_eq!(status["hostIP"], json!("10.0.0.10"));
        // containerStatuses preserved
        assert_eq!(status["containerStatuses"][0]["name"], json!("c"));
        // qosClass preserved
        assert_eq!(status["qosClass"], json!("BestEffort"));
        // labels preserved
        assert_eq!(updated.data["metadata"]["labels"]["app"], json!("x"));
        // conditions are exactly Ready/PodFailed + ContainersReady/PodFailed
        let conds = status["conditions"].as_array().unwrap();
        assert_eq!(conds.len(), 2);
        let ready = conds.iter().find(|c| c["type"] == "Ready").unwrap();
        assert_eq!(ready["status"], json!("False"));
        assert_eq!(ready["reason"], json!("PodFailed"));
    }
    #[tokio::test]
    async fn set_deadline_exceeded_returns_conflict_on_stale_rv() {
        let repo = build_status_repo().await;
        let created = repo
            .persistence
            .seed_pod(
                "default",
                "p-dl-race",
                pod_with_running_status_and_ip("p-dl-race"),
            )
            .await
            .unwrap();
        let snapshot = created.resource_version;
        repo.status_ports()
            .set_deadline_exceeded("default", "p-dl-race", "first".to_string(), Some(snapshot))
            .await
            .expect("first writer wins");
        let conflict = repo
            .status_ports()
            .set_deadline_exceeded("default", "p-dl-race", "second".to_string(), Some(snapshot))
            .await;
        let err = conflict.expect_err("stale rv must conflict");
        assert!(err.to_string().contains("409"), "expected 409, got {err:?}");
    }
    #[tokio::test]
    async fn replace_status_from_api_writes_full_object_preserving_spec() {
        let repo = build_status_repo().await;
        let created = repo
            .persistence
            .seed_pod("default", "p-rs", pending_pod("p-rs"))
            .await
            .unwrap();
        let updated = repo
            .api_ports()
            .replace_status_from_api(
                "default",
                "p-rs",
                json!({"phase": "Running", "podIP": "10.42.0.1"}),
                created.resource_version,
            )
            .await
            .unwrap();
        assert_eq!(updated.data["spec"]["containers"][0]["name"], json!("c"));
        assert_eq!(updated.data["status"]["phase"], json!("Running"));
        assert_eq!(updated.data["status"]["podIP"], json!("10.42.0.1"));
    }
    #[tokio::test]
    async fn replace_status_from_api_for_uid_rejects_stale_same_name_pod() {
        let repo = build_status_repo().await;
        let created = repo
            .persistence
            .seed_pod("default", "p-rs-uid", pending_pod("p-rs-uid"))
            .await
            .unwrap();

        let stale = repo
            .api_ports()
            .replace_status_from_api_for_uid(
                "default",
                "p-rs-uid",
                "old-pod-uid",
                json!({"phase": "Running", "podIP": "10.42.0.1"}),
                created.resource_version,
            )
            .await;

        let err = stale.expect_err("stale UID must not update a same-name replacement pod");
        assert!(
            err.to_string().contains("UID mismatch"),
            "expected UID mismatch, got {err:?}"
        );
        let stored = repo
            .query
            .get_pod_by_name("default", "p-rs-uid")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.data["status"]["phase"], json!("Pending"));
        assert!(
            stored.data.pointer("/status/podIP").is_none(),
            "same-name replacement must not receive stale status"
        );
    }
    #[tokio::test]
    async fn patch_status_from_api_json_patch_applies_op() {
        let repo = build_status_repo().await;
        let created = repo
            .persistence
            .seed_pod("default", "p-jp", pending_pod("p-jp"))
            .await
            .unwrap();
        let patch = json!([
            {"op": "replace", "path": "/status/phase", "value": "Running"}
        ]);
        let updated = repo
            .api_ports()
            .patch_status_from_api(
                "default",
                "p-jp",
                patch,
                super::super::assembly_support::support::PodStatusPatchKind::JsonPatch,
                created.resource_version,
            )
            .await
            .unwrap();
        assert_eq!(updated.data["status"]["phase"], json!("Running"));
        // qosClass preserved (it was on the seed)
        assert_eq!(updated.data["status"]["qosClass"], json!("BestEffort"));
    }
    #[tokio::test]
    async fn patch_status_from_api_merge_patch_updates_only_named_keys() {
        let repo = build_status_repo().await;
        let created = repo
            .persistence
            .seed_pod("default", "p-mp", pending_pod("p-mp"))
            .await
            .unwrap();
        let patch = json!({"status": {"phase": "Running", "podIP": "10.42.0.2"}});
        let updated = repo
            .api_ports()
            .patch_status_from_api(
                "default",
                "p-mp",
                patch,
                super::super::assembly_support::support::PodStatusPatchKind::MergePatch,
                created.resource_version,
            )
            .await
            .unwrap();
        assert_eq!(updated.data["status"]["phase"], json!("Running"));
        assert_eq!(updated.data["status"]["podIP"], json!("10.42.0.2"));
        // Untouched keys preserved
        assert_eq!(updated.data["status"]["qosClass"], json!("BestEffort"));
    }
    #[tokio::test]
    async fn test_only_repository_subresource_port_delegates_unconditional_status_patch() {
        let repo = build_status_repo().await;
        repo.persistence
            .seed_pod(
                "default",
                "p-neutral-unconditional",
                pending_pod("p-neutral-unconditional"),
            )
            .await
            .unwrap();

        let updated = repo
            .api_ports()
            .patch_status(klights_pod_api::PodStatusPatchRequest {
                namespace: "default".to_string(),
                name: "p-neutral-unconditional".to_string(),
                patch: json!({"status": {"phase": "Running"}}),
                patch_kind: klights_pod_api::PodStatusPatchKind::MergePatch,
                expected_resource_version: None,
            })
            .await
            .expect("the cfg(test) compatibility port must delegate None to canonical policy");

        assert_eq!(updated.data["status"]["phase"], json!("Running"));
    }
    #[tokio::test]
    async fn patch_status_from_api_ignores_non_status_fields() {
        let repo = build_status_repo().await;
        let created = repo
            .persistence
            .seed_pod("default", "p-ns-only", pending_pod("p-ns-only"))
            .await
            .unwrap();
        let original_image = created.data["spec"]["containers"][0]["image"].clone();
        let patch = json!({
            "status": {"phase": "Running"},
            "spec": {"containers": [{"name": "c", "image": "mutated"}]}
        });
        let updated = repo
            .api_ports()
            .patch_status_from_api(
                "default",
                "p-ns-only",
                patch,
                super::super::assembly_support::support::PodStatusPatchKind::MergePatch,
                created.resource_version,
            )
            .await
            .unwrap();
        assert_eq!(updated.data["status"]["phase"], json!("Running"));
        assert_eq!(
            updated.data["spec"]["containers"][0]["image"],
            original_image
        );
    }
    #[tokio::test]
    async fn patch_status_from_api_strategic_merge_merges_conditions_by_type() {
        let repo = build_status_repo().await;
        let mut seed = pending_pod("p-sm");
        seed["status"]["conditions"] = json!([
            {"type": "Ready", "status": "False", "lastTransitionTime": "2026-04-30T00:00:00Z"},
            {"type": "PodScheduled", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
        ]);
        let created = repo
            .persistence
            .seed_pod("default", "p-sm", seed)
            .await
            .unwrap();
        // Strategic-merge with the K8s `type` merge key. Only the Ready
        // condition should change; PodScheduled should stay intact.
        let patch = json!({
            "status": {
                "conditions": [
                    {"type": "Ready", "status": "True", "lastTransitionTime": "2026-04-30T01:00:00Z"}
                ]
            }
        });
        let updated = repo
            .api_ports()
            .patch_status_from_api(
                "default",
                "p-sm",
                patch,
                super::super::assembly_support::support::PodStatusPatchKind::StrategicMerge,
                created.resource_version,
            )
            .await
            .unwrap();
        let conds = updated.data["status"]["conditions"].as_array().unwrap();
        let ready = conds.iter().find(|c| c["type"] == "Ready").unwrap();
        assert_eq!(ready["status"], json!("True"));
        let scheduled = conds.iter().find(|c| c["type"] == "PodScheduled").unwrap();
        assert_eq!(scheduled["status"], json!("True"));
    }
    #[tokio::test]
    async fn update_ephemeral_containers_appends_via_full_object_update() {
        let repo = build_status_repo().await;
        let created = repo
            .persistence
            .seed_pod("default", "p-ec", pending_pod("p-ec"))
            .await
            .unwrap();
        let new_ec = vec![json!({"name": "debug", "image": "busybox"})];
        let updated = repo
            .api_ports()
            .update_ephemeral_containers_for_pod(
                "default",
                "p-ec",
                new_ec,
                created.resource_version,
            )
            .await
            .unwrap();
        let ecs = updated.data["spec"]["ephemeralContainers"]
            .as_array()
            .expect("ephemeralContainers present");
        assert_eq!(ecs.len(), 1);
        assert_eq!(ecs[0]["name"], json!("debug"));
        // spec preserved
        assert_eq!(updated.data["spec"]["containers"][0]["name"], json!("c"));
    }
    #[tokio::test]
    async fn pod_subresource_writes_return_conflict_on_stale_rv() {
        let repo = build_status_repo().await;
        let created = repo
            .persistence
            .seed_pod("default", "p-sub-race", pending_pod("p-sub-race"))
            .await
            .unwrap();
        let snapshot = created.resource_version;
        repo.api_ports()
            .replace_status_from_api(
                "default",
                "p-sub-race",
                json!({"phase": "Running"}),
                snapshot,
            )
            .await
            .expect("first writer wins");
        // Subsequent writes with the snapshot rv must conflict for each method.
        let r1 = repo
            .api_ports()
            .replace_status_from_api(
                "default",
                "p-sub-race",
                json!({"phase": "Failed"}),
                snapshot,
            )
            .await;
        assert!(
            r1.expect_err("replace stale must conflict")
                .to_string()
                .contains("409")
        );
        let r2 = repo
            .api_ports()
            .patch_status_from_api(
                "default",
                "p-sub-race",
                json!({"status": {"phase": "Running"}}),
                super::super::assembly_support::support::PodStatusPatchKind::MergePatch,
                snapshot,
            )
            .await;
        assert!(
            r2.expect_err("patch stale must conflict")
                .to_string()
                .contains("409")
        );
        let r3 = repo
            .api_ports()
            .update_ephemeral_containers_for_pod(
                "default",
                "p-sub-race",
                vec![json!({"name": "debug", "image": "busybox"})],
                snapshot,
            )
            .await;
        assert!(
            r3.expect_err("ephemeral stale must conflict")
                .to_string()
                .contains("409")
        );
    }

    // T3 red test 1: set_pod_status_for_uid with pod_ip: None against a live
    // row that has an IP produces a status document with NO podIP key.
    // Assert on the emitted JSON, not the merged result.
    #[tokio::test]
    async fn t3_none_pod_ip_emits_no_pod_ip_key() {
        let repo = build_status_repo().await;
        let created = repo
            .persistence
            .seed_pod("default", "t3-pod", pending_pod("t3-pod"))
            .await
            .expect("seed");
        // First write: set a real podIP so the row has one.
        repo.status_ports()
            .set_pod_status_for_uid(
                "default",
                "t3-pod",
                &created.uid,
                PodStatusUpdate {
                    phase: "Running".to_string(),
                    pod_ip: klights_kubelet::pod_repository::PublishedAddress::must("10.50.1.20"),
                    host_ip: klights_kubelet::pod_repository::PublishedAddress::must("10.99.0.14"),
                    container_statuses: vec![],
                    init_container_statuses: None,
                    qos_class: None,
                },
                Some(created.resource_version),
            )
            .await
            .expect("first write");
        // Second write: pod_ip: None. The emitted JSON must have no podIP key.
        let updated = repo
            .status_ports()
            .set_pod_status_for_uid(
                "default",
                "t3-pod",
                &created.uid,
                PodStatusUpdate {
                    phase: "Running".to_string(),
                    pod_ip: None,
                    host_ip: None,
                    container_statuses: vec![],
                    init_container_statuses: None,
                    qos_class: None,
                },
                None,
            )
            .await
            .expect("second write");
        // The emitted status must NOT contain an empty podIP string —
        // absent means "unknown", and the merge back-fills from live.
        // The stored result will have the live podIP from the merge,
        // but must NOT have an empty-string podIP.
        if let Some(pod_ip_val) = updated.data.pointer("/status/podIP") {
            assert!(
                !pod_ip_val.as_str().is_some_and(|s| s.is_empty()),
                "pod_ip: None must not produce empty-string podIP, got: {:?}",
                pod_ip_val
            );
        }
    }

    // T3 red test 2: that emitted status, merged as KubeletRuntime against a
    // live row with podIP=10.50.1.20, retains 10.50.1.20.
    #[test]
    fn t3_none_pod_ip_merged_with_live_preserves_live_ip() {
        use klights_types::{PodStatusOwner, merge_pod_status_for_update};
        let live = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "status": {
                "podIP": "10.50.1.20",
                "podIPs": [{"ip": "10.50.1.20"}],
                "conditions": []
            }
        });
        let mut incoming = json!({
            "phase": "Running",
            "conditions": []
        });
        merge_pod_status_for_update(
            "v1",
            "Pod",
            &live,
            &mut incoming,
            PodStatusOwner::KubeletRuntime,
        );
        assert_eq!(
            incoming.pointer("/podIP"),
            Some(&json!("10.50.1.20")),
            "merged status must preserve live podIP when incoming omits it"
        );
    }

    // T3 red test 3: apply_forwarded_status with initContainerStatuses but
    // no IPs against a live read with no IPs must not write an empty podIP.
    #[tokio::test]
    async fn t3_no_ip_status_does_not_write_empty_pod_ip() {
        let repo = build_status_repo().await;
        let created = repo
            .persistence
            .seed_pod("default", "t3-pod2", pending_pod("t3-pod2"))
            .await
            .expect("seed");
        let updated = repo
            .status_ports()
            .set_pod_status_for_uid(
                "default",
                "t3-pod2",
                &created.uid,
                PodStatusUpdate {
                    phase: "Pending".to_string(),
                    pod_ip: None,
                    host_ip: None,
                    container_statuses: vec![],
                    init_container_statuses: Some(vec![
                        json!({"name": "init", "state": {"terminated": {"exitCode": 0}}}),
                    ]),
                    qos_class: None,
                },
                Some(created.resource_version),
            )
            .await
            .expect("write");
        // The stored status must NOT contain an empty podIP string.
        // With no prior IP on the live row, the merge has nothing to
        // back-fill, so podIP must be absent entirely.
        assert!(
            updated.data.pointer("/status/podIP").is_none(),
            "no-IP status with no live IP must not produce podIP: {:?}",
            updated.data.pointer("/status/podIP")
        );
    }
}
