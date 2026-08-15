#[cfg(test)]
mod tests {
    use super::super::assembly_support::support::*;

    #[tokio::test]
    async fn api_create_pod_inline_mode_leaves_empty_node_name_unbound_until_scheduler_runs() {
        let repo = build_scheduling_repo().await;
        repo.seed_scheduling_resource(
            "v1",
            "Node",
            None,
            "test-node",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "test-node"},
                "spec": {"unschedulable": false},
                "status": {
                    "allocatable": {"cpu": "8", "memory": "32Gi", "pods": "110"},
                    "conditions": [{"type": "Ready", "status": "True"}]
                }
            }),
        )
        .await
        .unwrap();

        let result = repo
            .api_mutations
            .create(api_create_request(
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": { "name": "node-name-default" },
                    "spec": {
                        "nodeName": "",
                        "containers": [{ "name": "c", "image": "busybox" }]
                    }
                }),
                false,
            ))
            .await
            .unwrap();

        assert!(
            result
                .body
                .pointer("/spec/nodeName")
                .and_then(|v| v.as_str())
                .is_none(),
            "API create must leave implicit scheduling to the scheduler path"
        );
        let pod_scheduled = result
            .body
            .pointer("/status/conditions")
            .and_then(|v| v.as_array())
            .and_then(|conditions| {
                conditions.iter().find(|condition| {
                    condition.get("type").and_then(|v| v.as_str()) == Some("PodScheduled")
                })
            })
            .expect("PodScheduled condition present");
        assert_eq!(
            pod_scheduled.get("status").and_then(|v| v.as_str()),
            Some("False")
        );
        assert_eq!(
            pod_scheduled.get("reason").and_then(|v| v.as_str()),
            Some("SchedulingPending")
        );

        repo.scheduling_ports()
            .schedule_all_unbound_pods()
            .await
            .unwrap();
        let scheduled = repo
            .query
            .get_pod_by_name("default", "node-name-default")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            scheduled
                .data
                .pointer("/spec/nodeName")
                .and_then(|v| v.as_str()),
            Some("test-node")
        );
    }
    #[tokio::test]
    async fn api_create_pod_leader_mode_leaves_pod_unbound_until_scheduler_controller_binds() {
        let repo = build_repo_with_scheduling_mode(()).await;
        for node_name in ["lead-123456az", "work-654321za"] {
            repo.seed_scheduling_resource(
                "v1",
                "Node",
                None,
                node_name,
                json!({
                    "apiVersion": "v1",
                    "kind": "Node",
                    "metadata": {"name": node_name},
                    "spec": {"unschedulable": false},
                    "status": {
                        "allocatable": {"cpu": "8", "memory": "32Gi", "pods": "110"},
                        "conditions": [{"type": "Ready", "status": "True"}]
                    }
                }),
            )
            .await
            .unwrap();
        }

        let result = repo
            .api_mutations
            .create(api_create_request(
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": { "name": "deferred-schedule" },
                    "spec": {
                        "containers": [{ "name": "c", "image": "busybox" }]
                    }
                }),
                false,
            ))
            .await
            .unwrap();

        assert!(
            result
                .body
                .pointer("/spec/nodeName")
                .and_then(|v| v.as_str())
                .is_none(),
            "leader mode pod should remain unbound until scheduler controller binds it"
        );
        assert_eq!(
            result
                .body
                .pointer("/status/conditions")
                .and_then(|v| v.as_array())
                .and_then(|conds| conds.iter().find(|c| c["type"] == "PodScheduled"))
                .and_then(|c| c.get("status"))
                .and_then(|v| v.as_str()),
            Some("False")
        );

        repo.scheduling_ports()
            .schedule_all_unbound_pods()
            .await
            .unwrap();
        let scheduled = repo
            .query
            .get_pod_by_name("default", "deferred-schedule")
            .await
            .unwrap()
            .unwrap();
        let node_name = scheduled
            .data
            .pointer("/spec/nodeName")
            .and_then(|v| v.as_str());
        assert!(
            node_name == Some("lead-123456az") || node_name == Some("work-654321za"),
            "pod should be bound to one of the two available nodes, got {node_name:?}"
        );
    }
    #[tokio::test]
    async fn leader_scheduler_orders_unbound_pods_by_priority_creation_and_name() {
        let repo = build_repo_with_scheduling_mode(()).await;
        repo.seed_scheduling_resource(
            "v1",
            "Node",
            None,
            "test-node",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "test-node"},
                "spec": {"unschedulable": false},
                "status": {
                    "allocatable": {"cpu": "8", "memory": "32Gi", "pods": "1"},
                    "conditions": [{"type": "Ready", "status": "True"}]
                }
            }),
        )
        .await
        .unwrap();

        let mut later = pending_pod("a-later");
        later["metadata"]["creationTimestamp"] = json!("2026-06-01T00:00:02Z");
        repo.persistence
            .seed_pod("default", "a-later", later)
            .await
            .unwrap();

        let mut earlier = pending_pod("z-earlier");
        earlier["metadata"]["creationTimestamp"] = json!("2026-06-01T00:00:01Z");
        repo.persistence
            .seed_pod("default", "z-earlier", earlier)
            .await
            .unwrap();

        repo.scheduling_ports()
            .schedule_all_unbound_pods()
            .await
            .unwrap();

        let earlier = repo
            .query
            .get_pod_by_name("default", "z-earlier")
            .await
            .unwrap()
            .unwrap();
        let later = repo
            .query
            .get_pod_by_name("default", "a-later")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            earlier
                .data
                .pointer("/spec/nodeName")
                .and_then(|v| v.as_str()),
            Some("test-node"),
            "earlier pod in the same priority band must get the only slot"
        );
        assert!(
            later
                .data
                .pointer("/spec/nodeName")
                .and_then(|v| v.as_str())
                .is_none(),
            "later pod must remain unbound when the earlier pod consumed capacity"
        );
    }
    #[tokio::test]
    async fn leader_scheduler_snapshot_uses_namespace_labels_for_pod_affinity() {
        let repo = build_repo_with_scheduling_mode(()).await;

        repo.seed_scheduling_resource(
            "v1",
            "Namespace",
            None,
            "peer-ns",
            json!({
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {
                    "name": "peer-ns",
                    "labels": {"team": "storage"}
                }
            }),
        )
        .await
        .unwrap();

        for (node, zone) in [("node-a", "zone-a"), ("node-b", "zone-b")] {
            repo.seed_scheduling_resource(
                "v1",
                "Node",
                None,
                node,
                json!({
                    "apiVersion": "v1",
                    "kind": "Node",
                    "metadata": {
                        "name": node,
                        "labels": {"topology.kubernetes.io/zone": zone}
                    },
                    "spec": {"unschedulable": false},
                    "status": {
                        "allocatable": {"cpu": "8", "memory": "32Gi", "pods": "110"},
                        "conditions": [{"type": "Ready", "status": "True"}]
                    }
                }),
            )
            .await
            .unwrap();
        }

        repo.persistence
            .seed_pod(
                "peer-ns",
                "peer",
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "namespace": "peer-ns",
                        "name": "peer",
                        "labels": {"app": "database"}
                    },
                    "spec": {
                        "nodeName": "node-b",
                        "containers": [{"name": "main", "image": "pause"}]
                    },
                    "status": {"phase": "Running"}
                }),
            )
            .await
            .unwrap();

        repo.persistence
            .seed_pod(
                "default",
                "client",
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"namespace": "default", "name": "client"},
                    "spec": {
                        "containers": [{"name": "main", "image": "pause"}],
                        "affinity": {
                            "podAffinity": {
                                "requiredDuringSchedulingIgnoredDuringExecution": [{
                                    "labelSelector": {"matchLabels": {"app": "database"}},
                                    "namespaceSelector": {"matchLabels": {"team": "storage"}},
                                    "topologyKey": "topology.kubernetes.io/zone"
                                }]
                            }
                        }
                    },
                    "status": {"phase": "Pending"}
                }),
            )
            .await
            .unwrap();

        repo.scheduling_ports()
            .schedule_all_unbound_pods()
            .await
            .unwrap();

        let scheduled = repo
            .query
            .get_pod_by_name("default", "client")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            scheduled
                .data
                .pointer("/spec/nodeName")
                .and_then(|v| v.as_str()),
            Some("node-b"),
            "snapshot scheduling must include Namespace labels for namespaceSelector affinity: {:?}",
            scheduled.data
        );
    }
    #[tokio::test]
    async fn leader_scheduler_concurrent_wave_reserves_node_capacity_once() {
        let repo = build_repo_with_scheduling_mode(()).await;
        repo.seed_scheduling_resource(
            "v1",
            "Node",
            None,
            "test-node",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "test-node"},
                "spec": {"unschedulable": false},
                "status": {
                    "allocatable": {"cpu": "8", "memory": "32Gi", "pods": "1"},
                    "conditions": [{"type": "Ready", "status": "True"}]
                }
            }),
        )
        .await
        .unwrap();

        for idx in 0..8 {
            let name = format!("wave-{idx}");
            repo.persistence
                .seed_pod("default", &name, pending_pod(&name))
                .await
                .unwrap();
        }

        repo.scheduling_ports()
            .schedule_all_unbound_pods()
            .await
            .unwrap();

        let pods = repo
            .query
            .list_pods_exact(Some("default"), None, None, None, None)
            .await
            .unwrap();
        let bound: Vec<_> = pods
            .items()
            .iter()
            .filter(|pod| {
                pod.data.pointer("/spec/nodeName").and_then(|v| v.as_str()) == Some("test-node")
            })
            .collect();
        assert_eq!(
            bound.len(),
            1,
            "bounded concurrent scheduling must not double-allocate the only node slot"
        );
    }
    #[tokio::test]
    async fn leader_scheduler_starts_bounded_bind_wave_concurrently() {
        let (repo, gate) =
            IntegrationPodSchedulingFixture::new_deferred_leader_with_bind_gate().await;
        let repo = Arc::new(repo);

        repo.seed_scheduling_resource(
            "v1",
            "Node",
            None,
            "test-node",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "test-node"},
                "spec": {"unschedulable": false},
                "status": {
                    "allocatable": {"cpu": "8", "memory": "32Gi", "pods": "20"},
                    "conditions": [{"type": "Ready", "status": "True"}]
                }
            }),
        )
        .await
        .unwrap();

        for idx in 0..klights_controllers::scheduler::SCHED_BIND_CONCURRENCY {
            let name = format!("parallel-{idx}");
            repo.persistence
                .seed_pod("default", &name, pending_pod(&name))
                .await
                .unwrap();
        }

        let scheduling_repo = repo.clone();
        let schedule_task = tokio::spawn(async move {
            scheduling_repo
                .scheduling_ports()
                .schedule_all_unbound_pods()
                .await
        });

        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            gate.wait_for_entered_at_least(klights_controllers::scheduler::SCHED_BIND_CONCURRENCY),
        )
        .await
        .expect("the whole first scheduling wave should reach the bind gate concurrently");
        gate.release_all();
        schedule_task.await.unwrap().unwrap();

        let pods = repo
            .query
            .list_pods_exact(Some("default"), None, None, None, None)
            .await
            .unwrap();
        let bound = pods
            .items()
            .iter()
            .filter(|pod| {
                pod.data.pointer("/spec/nodeName").and_then(|v| v.as_str()) == Some("test-node")
            })
            .count();
        assert_eq!(
            bound,
            klights_controllers::scheduler::SCHED_BIND_CONCURRENCY
        );
    }
    #[tokio::test]
    async fn leader_scheduler_binds_node_and_podscheduled_condition_in_one_pod_event() {
        let repo = build_repo_with_scheduling_mode(()).await;
        repo.seed_scheduling_resource(
            "v1",
            "Node",
            None,
            "test-node",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "test-node"},
                "spec": {"unschedulable": false},
                "status": {
                    "allocatable": {"cpu": "8", "memory": "32Gi", "pods": "110"},
                    "conditions": [{"type": "Ready", "status": "True"}]
                }
            }),
        )
        .await
        .unwrap();

        let created = repo
            .api_mutations
            .create(api_create_request(
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": { "name": "single-bind-event" },
                    "spec": {
                        "containers": [{ "name": "c", "image": "busybox" }]
                    }
                }),
                false,
            ))
            .await
            .unwrap()
            .resource
            .expect("pod create persists");
        assert!(
            created
                .data
                .pointer("/spec/nodeName")
                .and_then(|v| v.as_str())
                .is_none(),
            "deferred leader mode starts unbound"
        );

        repo.scheduling_ports()
            .schedule_all_unbound_pods()
            .await
            .unwrap();
        let scheduled = repo
            .query
            .get_pod_by_name("default", "single-bind-event")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            scheduled
                .data
                .pointer("/spec/nodeName")
                .and_then(|v| v.as_str()),
            Some("test-node")
        );
        assert_eq!(
            scheduled
                .data
                .pointer("/status/conditions")
                .and_then(|v| v.as_array())
                .and_then(|conditions| {
                    conditions.iter().find(|condition| {
                        condition.get("type").and_then(|v| v.as_str()) == Some("PodScheduled")
                    })
                })
                .and_then(|condition| condition.get("status"))
                .and_then(|v| v.as_str()),
            Some("True")
        );

        let pod_events = repo
            .watch
            .pod_events_since(created.resource_version)
            .await
            .unwrap();
        let schedule_events: Vec<_> = pod_events
            .into_iter()
            .filter(|event| {
                event
                    .resource
                    .data
                    .pointer("/metadata/name")
                    .and_then(|v| v.as_str())
                    == Some("single-bind-event")
            })
            .collect();

        assert_eq!(
            schedule_events.len(),
            1,
            "scheduler bind and PodScheduled=True status must be one logical pod update"
        );
        assert_eq!(schedule_events[0].event_type, "MODIFIED");
        assert_eq!(
            schedule_events[0]
                .resource
                .data
                .pointer("/spec/nodeName")
                .and_then(|v| v.as_str()),
            Some("test-node")
        );
        assert_eq!(
            schedule_events[0]
                .resource
                .data
                .pointer("/status/conditions")
                .and_then(|v| v.as_array())
                .and_then(|conditions| {
                    conditions.iter().find(|condition| {
                        condition.get("type").and_then(|v| v.as_str()) == Some("PodScheduled")
                    })
                })
                .and_then(|condition| condition.get("status"))
                .and_then(|v| v.as_str()),
            Some("True")
        );
    }
    #[tokio::test]
    async fn leader_scheduler_marks_unschedulable_pod_and_emits_failed_scheduling_event() {
        let repo = IntegrationPodSchedulingFixture::new_deferred_leader_with_node_outbox().await;
        repo.seed_scheduling_resource(
            "v1",
            "Node",
            None,
            "test-node",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "test-node"},
                "spec": {"unschedulable": false},
                "status": {
                    "allocatable": {"cpu": "8", "memory": "32Gi", "pods": "110"},
                    "conditions": [{"type": "Ready", "status": "True"}]
                }
            }),
        )
        .await
        .unwrap();
        repo.persistence
            .seed_pod(
                "default",
                "filler",
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": "filler", "namespace": "default"},
                    "spec": {
                        "nodeName": "test-node",
                        "containers": [{
                            "name": "c",
                            "image": "registry.k8s.io/pause:3.10",
                            "resources": {"requests": {"cpu": "5600m"}}
                        }]
                    },
                    "status": {"phase": "Running"}
                }),
            )
            .await
            .unwrap();

        repo.api_mutations
            .create(api_create_request(
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": "additional-pod"},
                    "spec": {
                        "containers": [{
                            "name": "c",
                            "image": "registry.k8s.io/pause:3.10",
                            "resources": {"requests": {"cpu": "3"}}
                        }]
                    }
                }),
                false,
            ))
            .await
            .unwrap();

        repo.scheduling_ports()
            .schedule_all_unbound_pods()
            .await
            .unwrap();

        let events_before_outbox_dispatch = repo
            .list_scheduling_resources("v1", "Event", Some("default"))
            .await
            .unwrap();
        assert!(
            events_before_outbox_dispatch.items.iter().any(|event| {
                event.data.get("reason").and_then(|v| v.as_str()) == Some("FailedScheduling")
                    && event
                        .data
                        .pointer("/involvedObject/name")
                        .and_then(|v| v.as_str())
                        == Some("additional-pod")
            }),
            "leader scheduler must commit FailedScheduling directly instead of routing a scheduler-authored Event through the node outbox: {:?}",
            events_before_outbox_dispatch.items
        );

        repo.drain_node_outbox_to_local_leader().await.unwrap();
        let pod = repo
            .query
            .get_pod_by_name("default", "additional-pod")
            .await
            .unwrap()
            .unwrap();
        let scheduled = pod
            .data
            .pointer("/status/conditions")
            .and_then(|v| v.as_array())
            .and_then(|conditions| {
                conditions.iter().find(|condition| {
                    condition.get("type").and_then(|v| v.as_str()) == Some("PodScheduled")
                })
            })
            .expect("PodScheduled condition present after scheduler retry");
        assert_eq!(
            scheduled.get("reason").and_then(|v| v.as_str()),
            Some("Unschedulable")
        );

        let events = repo
            .list_scheduling_resources("v1", "Event", Some("default"))
            .await
            .unwrap();
        assert!(
            events.items.iter().any(|event| {
                event.data.get("reason").and_then(|v| v.as_str()) == Some("FailedScheduling")
                    && event
                        .data
                        .pointer("/involvedObject/name")
                        .and_then(|v| v.as_str())
                        == Some("additional-pod")
            }),
            "leader scheduler retry must emit FailedScheduling event: {:?}",
            events.items
        );

        let rv_after_first_retry = pod.resource_version;
        let event_count_after_first_retry = events.items.len();
        repo.scheduling_ports()
            .schedule_all_unbound_pods()
            .await
            .unwrap();
        repo.drain_node_outbox_to_local_leader().await.unwrap();
        let pod_after_second_retry = repo
            .query
            .get_pod_by_name("default", "additional-pod")
            .await
            .unwrap()
            .unwrap();
        let events_after_second_retry = repo
            .list_scheduling_resources("v1", "Event", Some("default"))
            .await
            .unwrap();
        assert_eq!(
            pod_after_second_retry.resource_version, rv_after_first_retry,
            "scheduler must not rewrite an unchanged unschedulable pod and wake itself again"
        );
        assert_eq!(
            events_after_second_retry.items.len(),
            event_count_after_first_retry,
            "scheduler must not emit duplicate FailedScheduling events for an unchanged pod"
        );
    }
    #[tokio::test]
    async fn leader_scheduler_applies_preemption_victims_for_extended_resource_fit() {
        let repo = build_repo_with_scheduling_mode(()).await;
        repo.seed_scheduling_resource(
            "v1",
            "Node",
            None,
            "test-node",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "test-node"},
                "spec": {"unschedulable": false},
                "status": {
                    "allocatable": {
                        "cpu": "8",
                        "memory": "32Gi",
                        "pods": "110",
                        "scheduling.k8s.io/foo": "5"
                    },
                    "conditions": [{"type": "Ready", "status": "True"}]
                }
            }),
        )
        .await
        .unwrap();
        for (name, priority) in [("low-priority", 1), ("medium-priority", 2)] {
            repo.persistence
                .seed_pod(
                    "default",
                    name,
                    json!({
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "metadata": {"name": name, "namespace": "default"},
                        "spec": {
                            "nodeName": "test-node",
                            "priority": priority,
                            "containers": [{
                                "name": "c",
                                "image": "registry.k8s.io/pause:3.10",
                                "resources": {"requests": {"scheduling.k8s.io/foo": "2"}}
                            }]
                        },
                        "status": {"phase": "Running"}
                    }),
                )
                .await
                .unwrap();
        }

        repo.api_mutations
            .create(api_create_request(
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": "preemptor"},
                    "spec": {
                        "priority": 3,
                        "containers": [{
                            "name": "c",
                            "image": "registry.k8s.io/pause:3.10",
                            "resources": {"requests": {"scheduling.k8s.io/foo": "2"}}
                        }]
                    }
                }),
                false,
            ))
            .await
            .unwrap();

        repo.scheduling_ports()
            .schedule_all_unbound_pods()
            .await
            .unwrap();
        let preemptor = repo
            .query
            .get_pod_by_name("default", "preemptor")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            preemptor
                .data
                .pointer("/spec/nodeName")
                .and_then(|v| v.as_str()),
            Some("test-node")
        );
        let low_priority = repo
            .query
            .get_pod_by_name("default", "low-priority")
            .await
            .unwrap()
            .expect("deferred scheduler must leave the victim row until actor finalization");
        assert!(
            low_priority
                .data
                .pointer("/metadata/deletionTimestamp")
                .and_then(|v| v.as_str())
                .is_some(),
            "deferred scheduler must mark the lowest-priority victim terminating before binding the preemptor"
        );
        assert!(
            repo.query
                .get_pod_by_name("default", "medium-priority")
                .await
                .unwrap()
                .is_some(),
            "deferred scheduler should only remove enough lower-priority victims to fit"
        );
    }
    #[tokio::test]
    async fn leader_scheduler_marks_finalized_preemption_victim_terminating() {
        let repo = build_repo_with_scheduling_mode(()).await;
        repo.seed_scheduling_resource(
            "v1",
            "Node",
            None,
            "test-node",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "test-node"},
                "spec": {"unschedulable": false},
                "status": {
                    "allocatable": {
                        "cpu": "8",
                        "memory": "32Gi",
                        "pods": "110",
                        "scheduling.k8s.io/foo": "5"
                    },
                    "conditions": [{"type": "Ready", "status": "True"}]
                }
            }),
        )
        .await
        .unwrap();
        repo.persistence
            .seed_pod(
                "default",
                "victim",
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "name": "victim",
                        "namespace": "default",
                        "finalizers": ["example.com/test-finalizer"]
                    },
                    "spec": {
                        "nodeName": "test-node",
                        "priority": 1,
                        "containers": [{
                            "name": "c",
                            "image": "registry.k8s.io/pause:3.10",
                            "resources": {"requests": {"scheduling.k8s.io/foo": "1"}}
                        }]
                    },
                    "status": {"phase": "Running"}
                }),
            )
            .await
            .unwrap();

        repo.api_mutations
            .create(api_create_request(
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": "preemptor"},
                    "spec": {
                        "priority": 2,
                        "containers": [{
                            "name": "c",
                            "image": "registry.k8s.io/pause:3.10",
                            "resources": {"requests": {"scheduling.k8s.io/foo": "5"}}
                        }]
                    }
                }),
                false,
            ))
            .await
            .unwrap();

        repo.scheduling_ports()
            .schedule_all_unbound_pods()
            .await
            .unwrap();
        let preemptor = repo
            .query
            .get_pod_by_name("default", "preemptor")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            preemptor
                .data
                .pointer("/spec/nodeName")
                .and_then(|v| v.as_str()),
            Some("test-node")
        );

        let victim = repo
            .query
            .get_pod_by_name("default", "victim")
            .await
            .unwrap()
            .unwrap();
        assert!(
            victim
                .data
                .pointer("/metadata/deletionTimestamp")
                .and_then(|v| v.as_str())
                .is_some(),
            "finalized preemption victim must be marked terminating, not hard-deleted or left running"
        );
        let disruption_target = victim
            .data
            .pointer("/status/conditions")
            .and_then(|v| v.as_array())
            .unwrap()
            .iter()
            .find(|condition| {
                condition.get("type").and_then(|v| v.as_str()) == Some("DisruptionTarget")
            })
            .expect("preempted victim must get DisruptionTarget condition");
        assert_eq!(
            disruption_target.get("status").and_then(|v| v.as_str()),
            Some("True")
        );
    }
    #[tokio::test]
    async fn preemption_victim_termination_preserves_bound_podscheduled_true() {
        let repo = build_repo_with_scheduling_mode(()).await;
        repo.seed_scheduling_resource(
            "v1",
            "Node",
            None,
            "test-node",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "test-node"},
                "spec": {"unschedulable": false},
                "status": {
                    "allocatable": {
                        "cpu": "8",
                        "memory": "32Gi",
                        "pods": "110",
                        "scheduling.k8s.io/foo": "5"
                    },
                    "conditions": [{"type": "Ready", "status": "True"}]
                }
            }),
        )
        .await
        .unwrap();
        repo.persistence
            .seed_pod(
                "default",
                "victim",
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": "victim", "namespace": "default"},
                    "spec": {
                        "nodeName": "test-node",
                        "priority": 1,
                        "containers": [{
                            "name": "c",
                            "image": "registry.k8s.io/pause:3.10",
                            "resources": {"requests": {"scheduling.k8s.io/foo": "5"}}
                        }]
                    },
                    "status": {
                        "phase": "Running",
                        "conditions": [{
                            "type": "PodScheduled",
                            "status": "False",
                            "reason": "Unschedulable",
                            "message": "stale scheduler status from an older unschedulable write"
                        }]
                    }
                }),
            )
            .await
            .unwrap();

        repo.api_mutations
            .create(api_create_request(
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": "preemptor"},
                    "spec": {
                        "priority": 2,
                        "containers": [{
                            "name": "c",
                            "image": "registry.k8s.io/pause:3.10",
                            "resources": {"requests": {"scheduling.k8s.io/foo": "5"}}
                        }]
                    }
                }),
                false,
            ))
            .await
            .unwrap();

        repo.scheduling_ports()
            .schedule_all_unbound_pods()
            .await
            .unwrap();

        let victim = repo
            .query
            .get_pod_by_name("default", "victim")
            .await
            .unwrap()
            .unwrap();
        assert!(
            victim
                .data
                .pointer("/metadata/deletionTimestamp")
                .and_then(|v| v.as_str())
                .is_some(),
            "preemption victim must be marked terminating"
        );
        let conditions = victim
            .data
            .pointer("/status/conditions")
            .and_then(|v| v.as_array())
            .expect("victim status conditions present");
        let scheduled = conditions
            .iter()
            .find(|condition| {
                condition.get("type").and_then(|v| v.as_str()) == Some("PodScheduled")
            })
            .expect("PodScheduled condition present");
        assert_eq!(
            scheduled.get("status").and_then(|v| v.as_str()),
            Some("True"),
            "a bound preemption victim must not be persisted as PodScheduled=False: {victim:?}"
        );
        assert!(
            conditions.iter().any(|condition| {
                condition.get("type").and_then(|v| v.as_str()) == Some("DisruptionTarget")
                    && condition.get("status").and_then(|v| v.as_str()) == Some("True")
            }),
            "preemption must still add DisruptionTarget: {victim:?}"
        );
    }
    #[tokio::test]
    async fn api_create_pod_leader_mode_respects_explicit_node_name() {
        let repo = build_repo_with_scheduling_mode(()).await;
        repo.seed_scheduling_resource(
            "v1",
            "Node",
            None,
            "lead-123456az",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "lead-123456az"},
                "spec": {"unschedulable": false},
                "status": {
                    "allocatable": {"cpu": "8", "memory": "32Gi", "pods": "110"},
                    "conditions": [{"type": "Ready", "status": "True"}]
                }
            }),
        )
        .await
        .unwrap();

        let result = repo
            .api_mutations
            .create(api_create_request(
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": { "name": "explicit-bind" },
                    "spec": {
                        "nodeName": "lead-123456az",
                        "containers": [{ "name": "c", "image": "busybox" }]
                    }
                }),
                false,
            ))
            .await
            .unwrap();

        assert_eq!(
            result
                .body
                .pointer("/spec/nodeName")
                .and_then(|v| v.as_str()),
            Some("lead-123456az")
        );
    }
    #[tokio::test]
    async fn scheduler_marks_pod_unschedulable_when_cpu_request_exceeds_allocatable() {
        let repo = IntegrationPodSchedulingFixture::new_deferred_leader_with_node_outbox().await;
        repo.seed_scheduling_resource(
            "v1",
            "Node",
            None,
            "test-node",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "test-node"},
                "spec": {"unschedulable": false},
                "status": {
                    "allocatable": {"cpu": "8", "memory": "32Gi", "pods": "110"},
                    "conditions": [{"type": "Ready", "status": "True"}]
                }
            }),
        )
        .await
        .unwrap();
        repo.persistence
            .seed_pod(
                "default",
                "filler",
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": "filler", "namespace": "default"},
                    "spec": {
                        "nodeName": "test-node",
                        "containers": [{
                            "name": "c",
                            "image": "registry.k8s.io/pause:3.10",
                            "resources": {"requests": {"cpu": "5600m"}}
                        }]
                    },
                    "status": {"phase": "Running"}
                }),
            )
            .await
            .unwrap();

        repo.api_mutations
            .create(api_create_request(
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": "additional-pod"},
                    "spec": {
                        "containers": [{
                            "name": "c",
                            "image": "registry.k8s.io/pause:3.10",
                            "resources": {"requests": {"cpu": "3"}}
                        }]
                    }
                }),
                false,
            ))
            .await
            .unwrap();

        repo.scheduling_ports()
            .schedule_all_unbound_pods()
            .await
            .unwrap();
        repo.drain_node_outbox_to_local_leader().await.unwrap();
        let created = repo
            .query
            .get_pod_by_name("default", "additional-pod")
            .await
            .unwrap()
            .unwrap()
            .data;
        assert!(
            created
                .pointer("/spec/nodeName")
                .and_then(|v| v.as_str())
                .is_none(),
            "pod requiring more CPU than remaining allocatable must not be assigned: {created:?}"
        );
        let scheduled = created
            .pointer("/status/conditions")
            .and_then(|v| v.as_array())
            .and_then(|conditions| {
                conditions.iter().find(|condition| {
                    condition.get("type").and_then(|v| v.as_str()) == Some("PodScheduled")
                })
            })
            .expect("PodScheduled condition present");
        assert_eq!(
            scheduled.get("status").and_then(|v| v.as_str()),
            Some("False")
        );
        assert_eq!(
            scheduled.get("reason").and_then(|v| v.as_str()),
            Some("Unschedulable")
        );

        let events = repo
            .list_scheduling_resources("v1", "Event", Some("default"))
            .await
            .unwrap();
        assert!(
            events.items.iter().any(|event| {
                event.data.get("reason").and_then(|v| v.as_str()) == Some("FailedScheduling")
                    && event
                        .data
                        .pointer("/involvedObject/name")
                        .and_then(|v| v.as_str())
                        == Some("additional-pod")
                    && event
                        .data
                        .get("message")
                        .and_then(|v| v.as_str())
                        .is_some_and(|message| message.contains("Insufficient cpu"))
            }),
            "unschedulable pod should receive a FailedScheduling event: {:?}",
            events.items
        );
    }
    #[tokio::test]
    async fn scheduler_marks_pod_unschedulable_when_node_selector_does_not_match() {
        let repo = build_scheduling_repo().await;
        repo.seed_scheduling_resource(
            "v1",
            "Node",
            None,
            "test-node",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {
                    "name": "test-node",
                    "labels": {
                        "kubernetes.io/os": "linux",
                        "disktype": "ssd"
                    }
                },
                "spec": {"unschedulable": false},
                "status": {
                    "allocatable": {"cpu": "8", "memory": "32Gi", "pods": "110"},
                    "conditions": [{"type": "Ready", "status": "True"}]
                }
            }),
        )
        .await
        .unwrap();

        repo.api_mutations
            .create(api_create_request(
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": "restricted-pod"},
                    "spec": {
                        "nodeSelector": {"disktype": "hdd"},
                        "containers": [{"name": "c", "image": "registry.k8s.io/pause:3.10"}]
                    }
                }),
                false,
            ))
            .await
            .unwrap();

        repo.scheduling_ports()
            .schedule_all_unbound_pods()
            .await
            .unwrap();
        let created = repo
            .query
            .get_pod_by_name("default", "restricted-pod")
            .await
            .unwrap()
            .unwrap()
            .data;
        assert!(
            created
                .pointer("/spec/nodeName")
                .and_then(|v| v.as_str())
                .is_none(),
            "pod with a non-matching nodeSelector must not be assigned: {created:?}"
        );
        let scheduled = created
            .pointer("/status/conditions")
            .and_then(|v| v.as_array())
            .and_then(|conditions| {
                conditions.iter().find(|condition| {
                    condition.get("type").and_then(|v| v.as_str()) == Some("PodScheduled")
                })
            })
            .expect("PodScheduled condition present");
        assert_eq!(
            scheduled.get("status").and_then(|v| v.as_str()),
            Some("False")
        );
        assert!(
            scheduled
                .get("message")
                .and_then(|v| v.as_str())
                .is_some_and(|message| message.contains("node affinity/selector")),
            "expected node selector failure message, got {scheduled:?}"
        );

        let events = repo
            .list_scheduling_resources("v1", "Event", Some("default"))
            .await
            .unwrap();
        assert!(
            events.items.iter().any(|event| {
                event.data.get("reason").and_then(|v| v.as_str()) == Some("FailedScheduling")
                    && event
                        .data
                        .pointer("/involvedObject/name")
                        .and_then(|v| v.as_str())
                        == Some("restricted-pod")
                    && event
                        .data
                        .get("message")
                        .and_then(|v| v.as_str())
                        .is_some_and(|message| message.contains("node affinity/selector"))
            }),
            "nodeSelector rejection must publish a FailedScheduling event directly from the leader: {:?}",
            events.items
        );
    }
    #[tokio::test]
    async fn scheduler_counts_extended_resource_requests_for_node_fit() {
        let repo = build_scheduling_repo().await;
        repo.seed_scheduling_resource(
            "v1",
            "Node",
            None,
            "test-node",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "test-node"},
                "spec": {"unschedulable": false},
                "status": {
                    "allocatable": {
                        "cpu": "8",
                        "memory": "32Gi",
                        "pods": "110",
                        "scheduling.k8s.io/foo": "5"
                    },
                    "conditions": [{"type": "Ready", "status": "True"}]
                }
            }),
        )
        .await
        .unwrap();
        repo.persistence
            .seed_pod(
                "default",
                "filler",
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": "filler", "namespace": "default"},
                    "spec": {
                        "nodeName": "test-node",
                        "containers": [{
                            "name": "c",
                            "image": "registry.k8s.io/pause:3.10",
                            "resources": {"requests": {"scheduling.k8s.io/foo": "4"}}
                        }]
                    },
                    "status": {"phase": "Running"}
                }),
            )
            .await
            .unwrap();

        repo.api_mutations
            .create(api_create_request(
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": "needs-extended"},
                    "spec": {
                        "containers": [{
                            "name": "c",
                            "image": "registry.k8s.io/pause:3.10",
                            "resources": {"requests": {"scheduling.k8s.io/foo": "2"}}
                        }]
                    }
                }),
                false,
            ))
            .await
            .unwrap();

        repo.scheduling_ports()
            .schedule_all_unbound_pods()
            .await
            .unwrap();
        let created = repo
            .query
            .get_pod_by_name("default", "needs-extended")
            .await
            .unwrap()
            .unwrap()
            .data;
        assert!(
            created
                .pointer("/spec/nodeName")
                .and_then(|v| v.as_str())
                .is_none(),
            "pod exceeding extended-resource allocatable must not be assigned: {created:?}"
        );
        assert!(
            created
                .pointer("/status/conditions")
                .and_then(|v| v.as_array())
                .and_then(|conditions| conditions.iter().find(|condition| {
                    condition.get("type").and_then(|v| v.as_str()) == Some("PodScheduled")
                }))
                .and_then(|condition| condition.get("message"))
                .and_then(|v| v.as_str())
                .is_some_and(|message| message.contains("Insufficient scheduling.k8s.io/foo")),
            "expected extended-resource scheduling failure, got {created:?}"
        );
    }
    #[tokio::test]
    async fn scheduler_preemption_marks_victim_terminating_and_enqueues_replicaset() {
        let repo = build_scheduling_repo_with_dispatcher().await;
        repo.seed_scheduling_resource(
            "v1",
            "Node",
            None,
            "test-node",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "test-node"},
                "spec": {"unschedulable": false},
                "status": {
                    "allocatable": {"cpu": "8", "memory": "32Gi", "pods": "110", "example.com/foo": "1"},
                    "conditions": [{"type": "Ready", "status": "True"}]
                }
            }),
        )
        .await
        .unwrap();
        repo.seed_scheduling_resource(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            "low-rs",
            json!({
                "apiVersion": "apps/v1",
                "kind": "ReplicaSet",
                "metadata": {"name": "low-rs", "namespace": "default", "uid": "low-rs-uid"},
                "spec": {
                    "replicas": 1,
                    "selector": {"matchLabels": {"app": "low"}},
                    "template": {
                        "metadata": {"labels": {"app": "low"}},
                        "spec": {"containers": [{"name": "c", "image": "registry.k8s.io/pause:3.10"}]}
                    }
                },
                "status": {"replicas": 1, "readyReplicas": 1, "availableReplicas": 1}
            }),
        )
        .await
        .unwrap();
        repo.persistence
            .seed_pod(
                "default",
                "low-rs-pod",
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "name": "low-rs-pod",
                        "namespace": "default",
                        "uid": "low-rs-pod-uid",
                        "labels": {"app": "low"},
                        "ownerReferences": [{
                            "apiVersion": "apps/v1",
                            "kind": "ReplicaSet",
                            "name": "low-rs",
                            "uid": "low-rs-uid",
                            "controller": true
                        }]
                    },
                    "spec": {
                        "nodeName": "test-node",
                        "priority": 1,
                        "containers": [{
                            "name": "c",
                            "image": "registry.k8s.io/pause:3.10",
                            "resources": {"requests": {"example.com/foo": "1"}}
                        }]
                    },
                    "status": {"phase": "Running"}
                }),
            )
            .await
            .unwrap();

        repo.api_mutations
            .create(api_create_request(
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": "preemptor"},
                    "spec": {
                        "priority": 2,
                        "containers": [{
                            "name": "c",
                            "image": "registry.k8s.io/pause:3.10",
                            "resources": {"requests": {"example.com/foo": "1"}}
                        }]
                    }
                }),
                false,
            ))
            .await
            .unwrap();

        repo.scheduling_ports()
            .schedule_all_unbound_pods()
            .await
            .unwrap();
        let low_rs_pod = repo
            .query
            .get_pod_by_name("default", "low-rs-pod")
            .await
            .unwrap()
            .expect("preempted victim remains until actor finalization");
        assert!(
            low_rs_pod
                .data
                .pointer("/metadata/deletionTimestamp")
                .and_then(|v| v.as_str())
                .is_some(),
            "scheduler preemption must mark the victim terminating"
        );
        let keys = repo.pending_reconcile_keys().await;
        assert!(
            keys.iter().any(|key| {
                key.api_version() == "apps/v1"
                    && key.kind() == "ReplicaSet"
                    && key.namespace() == Some("default")
                    && key.name() == "low-rs"
            }),
            "scheduler preemption must enqueue the owning ReplicaSet so it observes the terminating pod and creates a replacement"
        );
    }
    #[tokio::test]
    async fn scheduler_preempts_lowest_priority_victims_for_extended_resource_fit() {
        let repo = build_scheduling_repo().await;
        repo.seed_scheduling_resource(
            "v1",
            "Node",
            None,
            "test-node",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "test-node"},
                "spec": {"unschedulable": false},
                "status": {
                    "allocatable": {
                        "cpu": "8",
                        "memory": "32Gi",
                        "pods": "110",
                        "example.com/fakecpu": "1k"
                    },
                    "conditions": [{"type": "Ready", "status": "True"}]
                }
            }),
        )
        .await
        .unwrap();
        for (name, request, priority) in
            [("pod1", "200", 1), ("pod2", "300", 2), ("pod3", "450", 3)]
        {
            repo.persistence
                .seed_pod(
                    "default",
                    name,
                    json!({
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "metadata": {"name": name, "namespace": "default"},
                        "spec": {
                            "nodeName": "test-node",
                            "priority": priority,
                            "containers": [{
                                "name": "c",
                                "image": "registry.k8s.io/pause:3.10",
                                "resources": {"requests": {"example.com/fakecpu": request}}
                            }]
                        },
                        "status": {"phase": "Running"}
                    }),
                )
                .await
                .unwrap();
        }
        repo.persistence
            .seed_pod(
                "kube-system",
                "unrelated-low-priority",
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": "unrelated-low-priority", "namespace": "kube-system"},
                    "spec": {
                        "nodeName": "test-node",
                        "priority": 0,
                        "containers": [{
                            "name": "c",
                            "image": "registry.k8s.io/pause:3.10"
                        }]
                    },
                    "status": {"phase": "Running"}
                }),
            )
            .await
            .unwrap();

        repo.api_mutations
            .create(api_create_request(
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": "pod4"},
                    "spec": {
                        "priority": 4,
                        "containers": [{
                            "name": "c",
                            "image": "registry.k8s.io/pause:3.10",
                            "resources": {"requests": {"example.com/fakecpu": "500"}}
                        }]
                    }
                }),
                false,
            ))
            .await
            .unwrap();

        repo.scheduling_ports()
            .schedule_all_unbound_pods()
            .await
            .unwrap();
        let scheduled = repo
            .query
            .get_pod_by_name("default", "pod4")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            scheduled
                .data
                .pointer("/spec/nodeName")
                .and_then(|v| v.as_str()),
            Some("test-node")
        );
        for name in ["pod1", "pod2"] {
            let victim = repo
                .query
                .get_pod_by_name("default", name)
                .await
                .unwrap()
                .expect("preempted victim remains until actor finalization");
            assert!(
                victim
                    .data
                    .pointer("/metadata/deletionTimestamp")
                    .and_then(|v| v.as_str())
                    .is_some(),
                "preempted victim {name} must be marked terminating"
            );
        }
        assert!(
            repo.query
                .get_pod_by_name("default", "pod3")
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            repo.query
                .get_pod_by_name("kube-system", "unrelated-low-priority")
                .await
                .unwrap()
                .is_some(),
            "pod without the constrained extended resource must not be preempted"
        );
    }
    #[tokio::test]
    async fn scheduler_preempts_controller_created_priority_class_pods() {
        let repo = build_scheduling_repo().await;
        repo.seed_scheduling_resource(
            "v1",
            "Node",
            None,
            "test-node",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "test-node"},
                "spec": {"unschedulable": false},
                "status": {
                    "allocatable": {
                        "cpu": "8",
                        "memory": "32Gi",
                        "pods": "110",
                        "example.com/fakecpu": "1k"
                    },
                    "conditions": [{"type": "Ready", "status": "True"}]
                }
            }),
        )
        .await
        .unwrap();
        for (name, value) in [("p1", 1), ("p2", 2), ("p3", 3), ("p4", 4)] {
            repo.seed_scheduling_resource(
                "scheduling.k8s.io/v1",
                "PriorityClass",
                None,
                name,
                json!({
                    "apiVersion": "scheduling.k8s.io/v1",
                    "kind": "PriorityClass",
                    "metadata": {"name": name},
                    "value": value
                }),
            )
            .await
            .unwrap();
        }

        for (name, request, class_name) in [
            ("rs-pod1", "200", "p1"),
            ("rs-pod2", "300", "p2"),
            ("rs-pod3", "450", "p3"),
        ] {
            repo.create_controller_pod(
                "default",
                name,
                "test-node",
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": name, "namespace": "default"},
                    "spec": {
                        "nodeName": "test-node",
                        "priorityClassName": class_name,
                        "containers": [{
                            "name": "c",
                            "image": "registry.k8s.io/pause:3.10",
                            "resources": {"requests": {"example.com/fakecpu": request}}
                        }]
                    }
                }),
            )
            .await
            .unwrap();
        }

        repo.api_mutations
            .create(api_create_request(
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": "pod4"},
                    "spec": {
                        "priorityClassName": "p4",
                        "containers": [{
                            "name": "c",
                            "image": "registry.k8s.io/pause:3.10",
                            "resources": {"requests": {"example.com/fakecpu": "500"}}
                        }]
                    }
                }),
                false,
            ))
            .await
            .unwrap();

        repo.scheduling_ports()
            .schedule_all_unbound_pods()
            .await
            .unwrap();
        let scheduled = repo
            .query
            .get_pod_by_name("default", "pod4")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            scheduled
                .data
                .pointer("/spec/nodeName")
                .and_then(|v| v.as_str()),
            Some("test-node")
        );
        for name in ["rs-pod1", "rs-pod2"] {
            let victim = repo
                .query
                .get_pod_by_name("default", name)
                .await
                .unwrap()
                .expect("preempted controller-created victim remains until actor finalization");
            assert!(
                victim
                    .data
                    .pointer("/metadata/deletionTimestamp")
                    .and_then(|v| v.as_str())
                    .is_some(),
                "preempted controller-created victim {name} must be marked terminating"
            );
        }
        assert!(
            repo.query
                .get_pod_by_name("default", "rs-pod3")
                .await
                .unwrap()
                .is_some()
        );
    }
    #[tokio::test]
    async fn scheduler_preemption_marks_api_created_priority_class_victim_disruption_target() {
        let repo = build_repo_with_scheduling_mode(()).await;
        repo.seed_scheduling_resource(
            "v1",
            "Node",
            None,
            "test-node",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "test-node"},
                "spec": {"unschedulable": false},
                "status": {
                    "capacity": {
                        "cpu": "8",
                        "memory": "32Gi",
                        "pods": "110",
                        "scheduling.k8s.io/foo": "1"
                    },
                    "allocatable": {
                        "cpu": "8",
                        "memory": "32Gi",
                        "pods": "110",
                        "scheduling.k8s.io/foo": "1"
                    },
                    "conditions": [{"type": "Ready", "status": "True"}]
                }
            }),
        )
        .await
        .unwrap();
        for (name, value) in [("low", 1), ("high", 1000)] {
            repo.seed_scheduling_resource(
                "scheduling.k8s.io/v1",
                "PriorityClass",
                None,
                name,
                json!({
                    "apiVersion": "scheduling.k8s.io/v1",
                    "kind": "PriorityClass",
                    "metadata": {"name": name},
                    "value": value
                }),
            )
            .await
            .unwrap();
        }
        let node_affinity = json!({
            "nodeAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": {
                    "nodeSelectorTerms": [{
                        "matchFields": [{
                            "key": "metadata.name",
                            "operator": "In",
                            "values": ["test-node"]
                        }]
                    }]
                }
            }
        });

        repo.api_mutations
            .create(api_create_request(
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "name": "victim",
                        "namespace": "default",
                        "finalizers": ["example.com/test-finalizer"]
                    },
                    "spec": {
                        "priorityClassName": "low",
                        "affinity": node_affinity.clone(),
                        "containers": [{
                            "name": "c",
                            "image": "registry.k8s.io/pause:3.10",
                            "resources": {
                                "requests": {"scheduling.k8s.io/foo": "1"},
                                "limits": {"scheduling.k8s.io/foo": "1"}
                            }
                        }]
                    }
                }),
                false,
            ))
            .await
            .unwrap();
        repo.scheduling_ports()
            .schedule_all_unbound_pods()
            .await
            .unwrap();
        let victim = repo
            .query
            .get_pod_by_name("default", "victim")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            victim
                .data
                .pointer("/spec/nodeName")
                .and_then(|v| v.as_str()),
            Some("test-node")
        );

        repo.api_mutations
            .create(api_create_request(
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": "preemptor", "namespace": "default"},
                    "spec": {
                        "priorityClassName": "high",
                        "affinity": node_affinity,
                        "containers": [{
                            "name": "c",
                            "image": "registry.k8s.io/pause:3.10",
                            "resources": {
                                "requests": {"scheduling.k8s.io/foo": "1"},
                                "limits": {"scheduling.k8s.io/foo": "1"}
                            }
                        }]
                    }
                }),
                false,
            ))
            .await
            .unwrap();
        repo.scheduling_ports()
            .schedule_all_unbound_pods()
            .await
            .unwrap();

        let preemptor = repo
            .query
            .get_pod_by_name("default", "preemptor")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            preemptor
                .data
                .pointer("/spec/nodeName")
                .and_then(|v| v.as_str()),
            Some("test-node")
        );
        let victim = repo
            .query
            .get_pod_by_name("default", "victim")
            .await
            .unwrap()
            .expect("preempted victim remains until actor finalization");
        assert!(
            victim
                .data
                .pointer("/metadata/deletionTimestamp")
                .and_then(|v| v.as_str())
                .is_some(),
            "preempted victim must be marked terminating: {:?}",
            victim.data
        );
        assert!(
            victim
                .data
                .pointer("/status/conditions")
                .and_then(|v| v.as_array())
                .is_some_and(|conditions| conditions.iter().any(|condition| {
                    condition.get("type").and_then(|v| v.as_str()) == Some("DisruptionTarget")
                        && condition.get("status").and_then(|v| v.as_str()) == Some("True")
                        && condition.get("reason").and_then(|v| v.as_str())
                            == Some("PreemptionByScheduler")
                })),
            "preempted victim must include DisruptionTarget condition: {:?}",
            victim.data
        );
    }
    #[tokio::test]
    async fn scheduler_marks_finalized_preemption_victim_disruption_target() {
        let repo = build_scheduling_repo().await;
        repo.seed_scheduling_resource(
                "v1",
                "Node",
                None,
                "test-node",
                json!({
                    "apiVersion": "v1",
                    "kind": "Node",
                    "metadata": {"name": "test-node"},
                    "spec": {"unschedulable": false},
                    "status": {
                        "allocatable": {"cpu": "8", "memory": "32Gi", "pods": "110", "example.com/foo": "1"},
                        "conditions": [{"type": "Ready", "status": "True"}]
                    }
                }),
            )
            .await
            .unwrap();
        repo.persistence
            .seed_pod(
                "default",
                "victim",
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "name": "victim",
                        "namespace": "default",
                        "finalizers": ["example.com/test-finalizer"]
                    },
                    "spec": {
                        "nodeName": "test-node",
                        "priority": 1,
                        "containers": [{
                            "name": "c",
                            "image": "registry.k8s.io/pause:3.10",
                            "resources": {"requests": {"example.com/foo": "1"}}
                        }]
                    },
                    "status": {"phase": "Running"}
                }),
            )
            .await
            .unwrap();

        repo.api_mutations
            .create(api_create_request(
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": "preemptor"},
                    "spec": {
                        "priority": 2,
                        "containers": [{
                            "name": "c",
                            "image": "registry.k8s.io/pause:3.10",
                            "resources": {"requests": {"example.com/foo": "1"}}
                        }]
                    }
                }),
                false,
            ))
            .await
            .unwrap();

        repo.scheduling_ports()
            .schedule_all_unbound_pods()
            .await
            .unwrap();
        let scheduled = repo
            .query
            .get_pod_by_name("default", "preemptor")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            scheduled
                .data
                .pointer("/spec/nodeName")
                .and_then(|v| v.as_str()),
            Some("test-node")
        );
        let victim = repo
            .query
            .get_pod_by_name("default", "victim")
            .await
            .unwrap()
            .expect("finalized victim remains terminating");
        assert!(
            victim
                .data
                .pointer("/metadata/deletionTimestamp")
                .and_then(|v| v.as_str())
                .is_some(),
            "preempted victim must be marked terminating: {:?}",
            victim.data
        );
        assert!(
            victim
                .data
                .pointer("/status/conditions")
                .and_then(|v| v.as_array())
                .is_some_and(|conditions| conditions.iter().any(|condition| {
                    condition.get("type").and_then(|v| v.as_str()) == Some("DisruptionTarget")
                        && condition.get("status").and_then(|v| v.as_str()) == Some("True")
                        && condition.get("reason").and_then(|v| v.as_str())
                            == Some("PreemptionByScheduler")
                })),
            "preempted victim must include DisruptionTarget condition: {:?}",
            victim.data
        );
    }
    #[tokio::test]
    async fn scheduler_preemption_victim_terminating_event_includes_disruption_target() {
        let repo = build_repo_with_scheduling_mode(()).await;
        repo.seed_scheduling_resource(
                "v1",
                "Node",
                None,
                "test-node",
                json!({
                    "apiVersion": "v1",
                    "kind": "Node",
                    "metadata": {"name": "test-node"},
                    "spec": {"unschedulable": false},
                    "status": {
                        "allocatable": {"cpu": "8", "memory": "32Gi", "pods": "110", "example.com/foo": "1"},
                        "conditions": [{"type": "Ready", "status": "True"}]
                    }
                }),
            )
            .await
            .unwrap();
        let victim = repo
            .persistence
            .seed_pod(
                "default",
                "victim",
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "name": "victim",
                        "namespace": "default",
                        "finalizers": ["example.com/test-finalizer"]
                    },
                    "spec": {
                        "nodeName": "test-node",
                        "priority": 1,
                        "containers": [{
                            "name": "c",
                            "image": "registry.k8s.io/pause:3.10",
                            "resources": {"requests": {"example.com/foo": "1"}}
                        }]
                    },
                    "status": {"phase": "Running"}
                }),
            )
            .await
            .unwrap();

        repo.api_mutations
            .create(api_create_request(
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": "preemptor"},
                    "spec": {
                        "priority": 2,
                        "containers": [{
                            "name": "c",
                            "image": "registry.k8s.io/pause:3.10",
                            "resources": {"requests": {"example.com/foo": "1"}}
                        }]
                    }
                }),
                false,
            ))
            .await
            .unwrap();

        repo.scheduling_ports()
            .schedule_all_unbound_pods()
            .await
            .unwrap();
        let scheduled = repo
            .query
            .get_pod_by_name("default", "preemptor")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            scheduled
                .data
                .pointer("/spec/nodeName")
                .and_then(|v| v.as_str()),
            Some("test-node")
        );

        let pod_events = repo
            .watch
            .pod_events_since(victim.resource_version)
            .await
            .unwrap();
        let terminating_victim_events: Vec<_> = pod_events
            .into_iter()
            .filter(|event| {
                event.event_type == "MODIFIED"
                    && event
                        .resource
                        .data
                        .pointer("/metadata/name")
                        .and_then(|v| v.as_str())
                        == Some("victim")
                    && event
                        .resource
                        .data
                        .pointer("/metadata/deletionTimestamp")
                        .and_then(|v| v.as_str())
                        .is_some()
            })
            .collect();

        assert!(
            !terminating_victim_events.is_empty(),
            "preemption must publish a terminating victim event"
        );
        assert!(
            terminating_victim_events.iter().all(|event| {
                event
                    .resource
                    .data
                    .pointer("/status/conditions")
                    .and_then(|v| v.as_array())
                    .is_some_and(|conditions| {
                        conditions.iter().any(|condition| {
                            condition.get("type").and_then(|v| v.as_str())
                                == Some("DisruptionTarget")
                                && condition.get("status").and_then(|v| v.as_str()) == Some("True")
                                && condition.get("reason").and_then(|v| v.as_str())
                                    == Some("PreemptionByScheduler")
                        })
                    })
            }),
            "preemption victim must not be observable as terminating before DisruptionTarget is set: {:?}",
            terminating_victim_events
                .iter()
                .map(|event| event.resource.data.clone())
                .collect::<Vec<_>>()
        );
    }
    #[tokio::test]
    async fn scheduler_preemption_condition_survives_interleaved_worker_status_and_get() {
        let repo = build_repo_with_scheduling_mode(()).await;
        repo.seed_scheduling_resource(
            "v1",
            "Node",
            None,
            "worker-a",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "worker-a"},
                "spec": {"unschedulable": false},
                "status": {
                    "allocatable": {"cpu": "1", "memory": "32Gi", "pods": "110"},
                    "capacity": {"cpu": "1", "memory": "32Gi", "pods": "110"},
                    "conditions": [{"type": "Ready", "status": "True"}]
                }
            }),
        )
        .await
        .unwrap();
        for (name, value) in [("low-priority", 10), ("high-priority", 1000)] {
            repo.seed_scheduling_resource(
                "scheduling.k8s.io/v1",
                "PriorityClass",
                None,
                name,
                json!({
                    "apiVersion": "scheduling.k8s.io/v1",
                    "kind": "PriorityClass",
                    "metadata": {"name": name},
                    "value": value
                }),
            )
            .await
            .unwrap();
        }

        repo.persistence
            .seed_pod(
                "default",
                "victim-pod",
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "name": "victim-pod",
                        "namespace": "default",
                        "uid": "victim-uid",
                        "finalizers": ["example.com/test-finalizer"]
                    },
                    "spec": {
                        "nodeName": "worker-a",
                        "priorityClassName": "low-priority",
                        "priority": 10,
                        "containers": [{
                            "name": "app",
                            "image": "registry.k8s.io/pause:3.10",
                            "resources": {"requests": {"cpu": "900m"}}
                        }]
                    },
                    "status": {"phase": "Running"}
                }),
            )
            .await
            .unwrap();

        repo.api_mutations
            .create(api_create_request(
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": "preemptor-pod", "namespace": "default"},
                    "spec": {
                        "priorityClassName": "high-priority",
                        "containers": [{
                            "name": "app",
                            "image": "registry.k8s.io/pause:3.10",
                            "resources": {"requests": {"cpu": "900m"}}
                        }]
                    }
                }),
                false,
            ))
            .await
            .unwrap();
        repo.scheduling_ports()
            .schedule_all_unbound_pods()
            .await
            .unwrap();
        let scheduled = repo
            .query
            .get_pod_by_name("default", "preemptor-pod")
            .await
            .unwrap()
            .expect("preemptor must be scheduled");
        assert_eq!(
            scheduled
                .data
                .pointer("/spec/nodeName")
                .and_then(|value| value.as_str()),
            Some("worker-a"),
            "preemptor should win the node via preemption"
        );

        // Simulate a lagged kubelet status outbox apply landing after preemption:
        // a Running status snapshot (without DisruptionTarget) encoded as a worker
        // PodStatus outbox command and applied through the leader raft-apply path.
        let stale_status = json!({
            "phase": "Running",
            "conditions": [
                {"type": "PodScheduled", "status": "True"},
                {"type": "Initialized", "status": "True"},
                {"type": "ContainersReady", "status": "True"},
                {"type": "Ready", "status": "True"}
            ],
            "containerStatuses": [{
                "name": "app",
                "containerID": "containerd://victim-ctr",
                "ready": true,
                "started": true,
                "restartCount": 0,
                "state": {"running": {"startedAt": "2026-06-22T12:08:53Z"}}
            }]
        });
        repo.apply_uid_bound_worker_status_reducer_scenario(
            "default",
            "victim-pod",
            "victim-uid",
            "worker-a",
            stale_status,
        )
        .await
        .expect("stale worker status apply must not strand the outbox row");

        let victim = repo
            .query
            .get_pod_by_name("default", "victim-pod")
            .await
            .unwrap()
            .expect("victim remains until actor finalization");
        assert!(
            victim
                .data
                .pointer("/metadata/deletionTimestamp")
                .and_then(|value| value.as_str())
                .is_some(),
            "victim must be terminating: {:?}",
            victim.data.pointer("/metadata")
        );
        assert!(
            victim
                .data
                .pointer("/status/conditions")
                .and_then(|value| value.as_array())
                .unwrap_or(&Vec::new())
                .iter()
                .any(|condition| {
                    condition.pointer("/type").and_then(|value| value.as_str())
                        == Some("DisruptionTarget")
                        && condition
                            .pointer("/reason")
                            .and_then(|value| value.as_str())
                            == Some("PreemptionByScheduler")
                }),
            "terminating preemption victim must include DisruptionTarget after stale worker status: {:?}",
            victim.data.pointer("/status/conditions")
        );
    }
    #[tokio::test]
    async fn api_create_pod_resolves_priority_class_name_before_storage() {
        let repo = build_scheduling_repo().await;
        repo.seed_scheduling_resource(
            "scheduling.k8s.io/v1",
            "PriorityClass",
            None,
            "high",
            json!({
                "apiVersion": "scheduling.k8s.io/v1",
                "kind": "PriorityClass",
                "metadata": {"name": "high"},
                "value": 1000,
                "preemptionPolicy": "Never"
            }),
        )
        .await
        .unwrap();

        let result = repo
            .api_mutations
            .create(api_create_request(
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": "classed"},
                    "spec": {
                        "priorityClassName": "high",
                        "containers": [{"name": "c", "image": "registry.k8s.io/pause:3.10"}]
                    }
                }),
                false,
            ))
            .await
            .unwrap();

        assert_eq!(result.body.pointer("/spec/priority"), Some(&json!(1000)));
        assert_eq!(
            result.body.pointer("/spec/preemptionPolicy"),
            Some(&json!("Never"))
        );
    }
    #[tokio::test]
    async fn api_create_pod_applies_global_default_priority_class() {
        let repo = build_scheduling_repo().await;
        repo.seed_scheduling_resource(
            "scheduling.k8s.io/v1",
            "PriorityClass",
            None,
            "default-high",
            json!({
                "apiVersion": "scheduling.k8s.io/v1",
                "kind": "PriorityClass",
                "metadata": {"name": "default-high"},
                "value": 500,
                "globalDefault": true,
                "preemptionPolicy": "Never"
            }),
        )
        .await
        .unwrap();

        let result = repo
            .api_mutations
            .create(api_create_request(
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": "default-classed"},
                    "spec": {
                        "containers": [{"name": "c", "image": "registry.k8s.io/pause:3.10"}]
                    }
                }),
                false,
            ))
            .await
            .unwrap();

        assert_eq!(result.body.pointer("/spec/priority"), Some(&json!(500)));
        assert_eq!(
            result.body.pointer("/spec/preemptionPolicy"),
            Some(&json!("Never"))
        );
    }
    #[tokio::test]
    async fn api_create_pod_priority_class_overrides_wire_zero_priority() {
        let repo = build_scheduling_repo().await;
        repo.seed_scheduling_resource(
            "scheduling.k8s.io/v1",
            "PriorityClass",
            None,
            "high",
            json!({
                "apiVersion": "scheduling.k8s.io/v1",
                "kind": "PriorityClass",
                "metadata": {"name": "high"},
                "value": 1000,
                "preemptionPolicy": "PreemptLowerPriority"
            }),
        )
        .await
        .unwrap();

        let result = repo
            .api_mutations
            .create(api_create_request(
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": "classed-zero"},
                    "spec": {
                        "priorityClassName": "high",
                        "priority": 0,
                        "containers": [{"name": "c", "image": "registry.k8s.io/pause:3.10"}]
                    }
                }),
                false,
            ))
            .await
            .unwrap();

        assert_eq!(result.body.pointer("/spec/priority"), Some(&json!(1000)));
        assert_eq!(
            result.body.pointer("/spec/preemptionPolicy"),
            Some(&json!("PreemptLowerPriority"))
        );
    }
    #[tokio::test]
    async fn api_create_pod_rejects_restricted_pod_security_violation() {
        let repo = build_scheduling_repo().await;
        repo.seed_scheduling_resource(
            "v1",
            "Namespace",
            None,
            "restricted",
            json!({
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {
                    "name": "restricted",
                    "labels": {"pod-security.kubernetes.io/enforce": "restricted"}
                },
                "status": {"phase": "Active"}
            }),
        )
        .await
        .unwrap();

        let err = repo
            .api_mutations
            .create(
                super::super::assembly_support::support::PodApiCreateRequest {
                    namespace: "restricted".to_string(),
                    body: json!({
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "metadata": {"name": "privileged"},
                        "spec": {
                            "containers": [{
                                "name": "c",
                                "image": "registry.k8s.io/pause:3.10",
                                "securityContext": {"privileged": true}
                            }]
                        }
                    }),
                    dry_run: false,
                },
            )
            .await
            .expect_err("restricted namespace must reject privileged pod");

        let msg = match err {
            klights_pod_api::PodRepositoryError::Forbidden { message }
            | klights_pod_api::PodRepositoryError::InvalidRequest { message, .. } => message,
            other => panic!("unexpected error: {other:?}"),
        };
        assert!(
            msg.contains("PodSecurity") && msg.contains("restricted") && msg.contains("privileged"),
            "unexpected error: {msg}"
        );
    }
    #[tokio::test]
    async fn api_create_pod_defaults_container_fields() {
        let repo = build_scheduling_repo().await;
        let result = repo
            .api_mutations
            .create(api_create_request(
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": { "name": "container-defaults" },
                    "spec": {
                        "initContainers": [{
                            "name": "init",
                            "image": "busybox"
                        }],
                        "containers": [{
                            "name": "c",
                            "image": "nginx:1.25",
                            "terminationMessagePath": "",
                            "terminationMessagePolicy": "",
                            "livenessProbe": { "httpGet": { "port": 8080, "path": "", "scheme": "" } }
                        }]
                    }
                }),
                false,
            ))
            .await
            .unwrap();
        let container = result.body.pointer("/spec/containers/0").unwrap();
        assert_eq!(
            container
                .get("terminationMessagePath")
                .and_then(|v| v.as_str()),
            Some("/dev/termination-log")
        );
        assert_eq!(
            container
                .get("terminationMessagePolicy")
                .and_then(|v| v.as_str()),
            Some("File")
        );
        assert_eq!(
            container
                .pointer("/livenessProbe/httpGet/path")
                .and_then(|v| v.as_str()),
            Some("/")
        );
        assert_eq!(
            container
                .pointer("/livenessProbe/httpGet/scheme")
                .and_then(|v| v.as_str()),
            Some("HTTP")
        );
        assert_eq!(
            result.body.pointer("/spec/restartPolicy"),
            Some(&json!("Always"))
        );
        assert_eq!(
            result.body.pointer("/spec/dnsPolicy"),
            Some(&json!("ClusterFirst"))
        );
        assert_eq!(
            result.body.pointer("/spec/schedulerName"),
            Some(&json!("default-scheduler"))
        );
        assert_eq!(
            result
                .body
                .pointer("/spec/initContainers/0/imagePullPolicy"),
            Some(&json!("Always"))
        );
        assert_eq!(
            result.body.pointer("/spec/containers/0/imagePullPolicy"),
            Some(&json!("IfNotPresent"))
        );

        let stored = repo
            .query
            .get_pod_by_name("default", "container-defaults")
            .await
            .unwrap()
            .expect("created pod stored");
        assert_eq!(
            stored.data.pointer("/spec/dnsPolicy"),
            Some(&json!("ClusterFirst"))
        );
        assert_eq!(
            stored.data.pointer("/spec/schedulerName"),
            Some(&json!("default-scheduler"))
        );
        assert_eq!(
            stored.data.pointer("/spec/containers/0/imagePullPolicy"),
            Some(&json!("IfNotPresent"))
        );
    }
    #[tokio::test]
    async fn api_create_pod_applies_serviceaccount_image_pull_secrets_and_deprecated_alias() {
        let repo = build_scheduling_repo().await;
        repo.seed_scheduling_resource(
            "v1",
            "ServiceAccount",
            Some("default"),
            "default",
            json!({
                "apiVersion": "v1",
                "kind": "ServiceAccount",
                "metadata": {"name": "default", "namespace": "default"},
                "imagePullSecrets": [{"name": "registry-cred"}]
            }),
        )
        .await
        .unwrap();

        let result = repo
            .api_mutations
            .create(api_create_request(
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": { "name": "sa-image-pull" },
                    "spec": {
                        "containers": [{"name": "c", "image": "private.example.com/app:1"}]
                    }
                }),
                false,
            ))
            .await
            .unwrap();

        assert_eq!(
            result.body.pointer("/spec/serviceAccountName"),
            Some(&json!("default"))
        );
        assert_eq!(
            result.body.pointer("/spec/serviceAccount"),
            Some(&json!("default")),
            "deprecated serviceAccount field must mirror serviceAccountName"
        );
        assert_eq!(
            result.body.pointer("/spec/imagePullSecrets"),
            Some(&json!([{"name": "registry-cred"}]))
        );

        let stored = repo
            .query
            .get_pod_by_name("default", "sa-image-pull")
            .await
            .unwrap()
            .expect("created pod stored");
        assert_eq!(
            stored.data.pointer("/spec/imagePullSecrets"),
            Some(&json!([{"name": "registry-cred"}]))
        );
        assert_eq!(
            stored.data.pointer("/spec/serviceAccount"),
            Some(&json!("default"))
        );
    }
    #[tokio::test]
    async fn api_create_pod_sets_pending_status_and_qos() {
        let repo = build_scheduling_repo().await;
        let result = repo
            .api_mutations
            .create(api_create_request(
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": { "name": "status-and-qos" },
                    "spec": {
                        "containers": [{ "name": "c", "image": "busybox" }]
                    }
                }),
                false,
            ))
            .await
            .unwrap();
        assert_eq!(
            result
                .body
                .pointer("/status/phase")
                .and_then(|v| v.as_str()),
            Some("Pending")
        );
        assert_eq!(
            result
                .body
                .pointer("/status/containerStatuses")
                .and_then(|v| v.as_array())
                .map(std::vec::Vec::len),
            Some(0)
        );
        assert_eq!(
            result
                .body
                .pointer("/status/qosClass")
                .and_then(|v| v.as_str()),
            Some("BestEffort")
        );
        let conditions = result
            .body
            .pointer("/status/conditions")
            .and_then(|v| v.as_array())
            .unwrap();
        let has = |ty: &str, status: &str| {
            conditions.iter().any(|cond| {
                cond.get("type").and_then(|v| v.as_str()) == Some(ty)
                    && cond.get("status").and_then(|v| v.as_str()) == Some(status)
            })
        };
        assert!(has("Initialized", "True"));
        assert!(has("Ready", "False"));
        assert!(has("ContainersReady", "False"));
        assert!(has("PodScheduled", "False"));
        assert!(
            conditions.iter().any(|cond| {
                cond.get("type").and_then(|v| v.as_str()) == Some("PodScheduled")
                    && cond.get("reason").and_then(|v| v.as_str()) == Some("SchedulingPending")
            }),
            "implicit scheduling should remain pending at API create"
        );
    }
    #[tokio::test]
    async fn api_create_pod_dry_run_does_not_persist() {
        let repo = build_scheduling_repo().await;
        let result = repo
            .api_mutations
            .create(api_create_request(
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": { "name": "dry-run-pod" },
                    "spec": {
                        "containers": [{ "name": "c", "image": "busybox" }]
                    }
                }),
                true,
            ))
            .await
            .unwrap();
        assert!(result.resource.is_none());
        assert!(
            repo.query
                .get_pod_by_name("default", "dry-run-pod")
                .await
                .unwrap()
                .is_none()
        );
    }
    #[tokio::test]
    async fn api_update_pod_persists_full_object_changes() {
        let repo = build_scheduling_repo().await;
        let created = create_scheduling_pod_via_api(&repo, "u-pod").await;
        let mut body: serde_json::Value = (*created.data).clone();
        if let Some(meta) = body.get_mut("metadata").and_then(|m| m.as_object_mut()) {
            meta.insert(
                "labels".to_string(),
                json!({"app": "x", "tier": "frontend"}),
            );
        }
        let outcome = repo
            .api_mutations
            .update_pod("default", "u-pod", body, created.clone(), false)
            .await
            .unwrap();
        let resource = match outcome {
            super::super::assembly_support::support::PodApiWriteOutcome::Persisted(r) => r,
            super::super::assembly_support::support::PodApiWriteOutcome::DryRun(_) => {
                panic!("expected Persisted")
            }
        };
        assert_eq!(
            resource.data["metadata"]["labels"]["tier"],
            json!("frontend")
        );
        assert!(resource.resource_version > created.resource_version);
    }
    #[tokio::test]
    async fn api_update_pod_preserves_existing_status() {
        let repo = build_scheduling_repo().await;
        let created = create_scheduling_pod_via_api(&repo, "u-status").await;
        let status_updated = repo
            .api_ports()
            .replace_status_from_api(
                "default",
                "u-status",
                json!({"phase": "Running", "podIP": "10.42.0.10"}),
                created.resource_version,
            )
            .await
            .unwrap();

        let mut body: serde_json::Value = (*status_updated.data).clone();
        body["metadata"]["labels"] = json!({"tier": "frontend"});
        body["status"] = json!({"phase": "Failed", "podIP": "10.42.0.99"});

        let outcome = repo
            .api_mutations
            .update_pod("default", "u-status", body, status_updated.clone(), false)
            .await
            .unwrap();
        let resource = match outcome {
            super::super::assembly_support::support::PodApiWriteOutcome::Persisted(r) => r,
            super::super::assembly_support::support::PodApiWriteOutcome::DryRun(_) => {
                panic!("expected Persisted")
            }
        };
        assert_eq!(
            resource.data["metadata"]["labels"]["tier"],
            json!("frontend")
        );
        assert_eq!(resource.data["status"]["phase"], json!("Running"));
        assert_eq!(resource.data["status"]["podIP"], json!("10.42.0.10"));
    }
    #[tokio::test]
    async fn api_update_pod_dry_run_does_not_persist() {
        let repo = build_scheduling_repo().await;
        let created = create_scheduling_pod_via_api(&repo, "u-dry").await;
        let mut body: serde_json::Value = (*created.data).clone();
        body["metadata"]["labels"] = json!({"app": "x", "tier": "dry"});
        let outcome = repo
            .api_mutations
            .update_pod("default", "u-dry", body, created.clone(), true)
            .await
            .unwrap();
        assert!(matches!(
            outcome,
            super::super::assembly_support::support::PodApiWriteOutcome::DryRun(_)
        ));
        let after = repo
            .query
            .get_pod_by_name("default", "u-dry")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after.resource_version, created.resource_version);
        assert!(
            after.data["metadata"].get("labels").is_none()
                || after.data["metadata"]["labels"].get("tier").is_none()
        );
    }
    #[tokio::test]
    async fn api_update_pod_returns_conflict_on_stale_rv() {
        let repo = build_scheduling_repo().await;
        let created = create_scheduling_pod_via_api(&repo, "u-race").await;

        // First writer wins.
        let mut body1: serde_json::Value = (*created.data).clone();
        body1["metadata"]["labels"] = json!({"app": "x", "tier": "first"});
        repo.api_mutations
            .update(klights_pod_api::PodApiUpdateRequest {
                namespace: "default".to_string(),
                name: "u-race".to_string(),
                body: body1,
                current: created.clone(),
                dry_run: false,
            })
            .await
            .expect("first writer wins");

        // Second writer with the stale read object.
        let mut body2: serde_json::Value = (*created.data).clone();
        body2["metadata"]["labels"] = json!({"app": "x", "tier": "second"});
        let conflict = repo
            .api_mutations
            .update(klights_pod_api::PodApiUpdateRequest {
                namespace: "default".to_string(),
                name: "u-race".to_string(),
                body: body2,
                current: created,
                dry_run: false,
            })
            .await;
        let err = conflict.expect_err("stale rv must conflict");
        assert!(
            format!("{err:?}").contains("409") || format!("{err:?}").contains("Conflict"),
            "expected 409 Conflict, got {err:?}"
        );
    }
    #[tokio::test]
    async fn api_patch_pod_json_patch_applies_op() {
        let repo = build_scheduling_repo().await;
        let _ = create_scheduling_pod_via_api(&repo, "p-jp").await;
        let patch = json!([
            {"op": "add", "path": "/metadata/labels", "value": {"tier": "frontend"}}
        ]);
        let outcome = repo
            .api_mutations
            .patch_pod(
                "default",
                "p-jp",
                patch,
                super::super::assembly_support::support::PodStatusPatchKind::JsonPatch,
                false,
            )
            .await
            .unwrap();
        let resource = match outcome {
            super::super::assembly_support::support::PodApiWriteOutcome::Persisted(r) => r,
            _ => panic!("expected Persisted"),
        };
        assert_eq!(
            resource.data["metadata"]["labels"]["tier"],
            json!("frontend")
        );
    }
    #[tokio::test]
    async fn api_patch_pod_merge_patch_updates_only_named_keys() {
        let repo = build_scheduling_repo().await;
        let _ = create_scheduling_pod_via_api(&repo, "p-mp").await;
        let patch = json!({"metadata": {"labels": {"tier": "frontend"}}});
        let outcome = repo
            .api_mutations
            .patch_pod(
                "default",
                "p-mp",
                patch,
                super::super::assembly_support::support::PodStatusPatchKind::MergePatch,
                false,
            )
            .await
            .unwrap();
        let resource = match outcome {
            super::super::assembly_support::support::PodApiWriteOutcome::Persisted(r) => r,
            _ => panic!("expected Persisted"),
        };
        assert_eq!(
            resource.data["metadata"]["labels"]["tier"],
            json!("frontend")
        );
        // Spec preserved
        assert_eq!(resource.data["spec"]["containers"][0]["name"], json!("c"));
    }
    #[tokio::test]
    async fn pod_annotation_patch_does_not_scan_services_or_enqueue_service() {
        let repo = build_scheduling_repo_with_dispatcher().await;

        repo.seed_scheduling_resource(
            "v1",
            "Service",
            Some("default"),
            "web",
            json!({
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {"name": "web", "namespace": "default"},
                "spec": {
                    "selector": {"app": "web"},
                    "ports": [{"name": "http", "port": 80, "targetPort": 8080}]
                }
            }),
        )
        .await
        .unwrap();

        let mut seed = pending_pod("anno-pod");
        seed["metadata"]["labels"] = json!({"app": "web"});
        seed["status"] = json!({
            "phase": "Running",
            "podIP": "10.42.0.35",
            "podIPs": [{"ip": "10.42.0.35"}],
            "hostIP": "10.0.0.10",
            "hostIPs": [{"ip": "10.0.0.10"}],
            "conditions": [
                {"type": "PodScheduled", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
                {"type": "Initialized", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
                {"type": "ContainersReady", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
                {"type": "Ready", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"}
            ]
        });
        repo.persistence
            .seed_pod("default", "anno-pod", seed)
            .await
            .unwrap();
        let managed_tasks_before = repo.active_supervised_task_count();

        for i in 0..50 {
            let patch = json!({
                "metadata": {
                    "annotations": {
                        "note": format!("scan-check-{i}")
                    }
                }
            });
            let outcome = repo
                .api_mutations
                .patch_pod(
                    "default",
                    "anno-pod",
                    patch,
                    super::super::assembly_support::support::PodStatusPatchKind::MergePatch,
                    false,
                )
                .await
                .unwrap();
            assert!(matches!(
                outcome,
                super::super::assembly_support::support::PodApiWriteOutcome::Persisted(_)
            ));
        }

        let keys = repo.pending_reconcile_keys().await;
        assert!(
            keys.iter()
                .all(|key| !(key.api_version() == "v1" && key.kind() == "Service")),
            "annotation-only patches must not enqueue Service reconciles"
        );
        assert_eq!(
            repo.active_supervised_task_count(),
            managed_tasks_before,
            "annotation-only patches must not create background retry or timer tasks"
        );
    }
    #[tokio::test]
    async fn api_patch_pod_preserves_existing_status() {
        let repo = build_scheduling_repo().await;
        let created = create_scheduling_pod_via_api(&repo, "p-status").await;
        let status_updated = repo
            .api_ports()
            .replace_status_from_api(
                "default",
                "p-status",
                json!({"phase": "Running", "podIP": "10.42.0.20"}),
                created.resource_version,
            )
            .await
            .unwrap();

        let outcome = repo
            .api_mutations
            .patch_pod(
                "default",
                "p-status",
                json!({
                    "metadata": {"labels": {"tier": "frontend"}},
                    "status": {"phase": "Failed", "podIP": "10.42.0.99"}
                }),
                super::super::assembly_support::support::PodStatusPatchKind::MergePatch,
                false,
            )
            .await
            .unwrap();
        let resource = match outcome {
            super::super::assembly_support::support::PodApiWriteOutcome::Persisted(r) => r,
            _ => panic!("expected Persisted"),
        };
        assert!(resource.resource_version > status_updated.resource_version);
        assert_eq!(
            resource.data["metadata"]["labels"]["tier"],
            json!("frontend")
        );
        assert_eq!(resource.data["status"]["phase"], json!("Running"));
        assert_eq!(resource.data["status"]["podIP"], json!("10.42.0.20"));
    }
    #[tokio::test]
    async fn api_patch_pod_strategic_merge_merges_conditions_by_type() {
        let repo = build_scheduling_repo().await;
        // Strategic-merge on metadata.labels (no merge-key field there) just
        // merges the two maps.
        let _ = create_scheduling_pod_via_api(&repo, "p-sm").await;
        let patch = json!({"metadata": {"labels": {"tier": "frontend"}}});
        let outcome = repo
            .api_mutations
            .patch_pod(
                "default",
                "p-sm",
                patch,
                super::super::assembly_support::support::PodStatusPatchKind::StrategicMerge,
                false,
            )
            .await
            .unwrap();
        let resource = match outcome {
            super::super::assembly_support::support::PodApiWriteOutcome::Persisted(r) => r,
            _ => panic!("expected Persisted"),
        };
        assert_eq!(
            resource.data["metadata"]["labels"]["tier"],
            json!("frontend")
        );
    }
    #[tokio::test]
    async fn api_patch_pod_apply_patch_against_missing_pod_creates_via_ssa() {
        let repo = build_scheduling_repo().await;
        // No pre-existing pod.
        let patch = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "name": "ssa-new" },
            "spec": { "containers": [{ "name": "c", "image": "busybox" }] }
        });
        let outcome = repo
            .api_mutations
            .patch_pod(
                "default",
                "ssa-new",
                patch,
                super::super::assembly_support::support::PodStatusPatchKind::ApplyPatch,
                false,
            )
            .await
            .unwrap();
        assert!(matches!(
            outcome,
            super::super::assembly_support::support::PodApiWriteOutcome::Persisted(_)
        ));
        let exists = repo
            .query
            .get_pod_by_name("default", "ssa-new")
            .await
            .unwrap();
        assert!(exists.is_some(), "SSA-create must persist the pod");
    }
    #[tokio::test]
    async fn api_patch_pod_merge_patch_against_missing_pod_returns_404() {
        let repo = build_scheduling_repo().await;
        let patch = json!({"metadata": {"labels": {"tier": "x"}}});
        let err = repo
            .api_mutations
            .patch_pod(
                "default",
                "missing-pod",
                patch,
                super::super::assembly_support::support::PodStatusPatchKind::MergePatch,
                false,
            )
            .await
            .expect_err("missing pod under merge patch must 404");
        assert!(
            matches!(err, klights_pod_api::PodRepositoryError::NotFound { .. }),
            "expected NotFound, got {err:?}"
        );
    }
}
