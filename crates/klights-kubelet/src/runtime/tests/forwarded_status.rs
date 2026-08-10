use super::*;
use serde_json::json;

#[tokio::test]
async fn forwarded_full_status_preserves_completed_init_container_statuses() {
    let repo = build_test_pod_repository();
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "default",
            "name": "init-forwarded",
            "uid": "uid-init-forwarded",
            "resourceVersion": "1"
        },
        "spec": {
            "restartPolicy": "Never",
            "initContainers": [
                {"name": "init1", "image": "busybox"},
                {"name": "init2", "image": "busybox"}
            ],
            "containers": [{"name": "run1", "image": "busybox"}]
        },
        "status": {
            "phase": "Pending",
            "conditions": [
                {"type": "Initialized", "status": "False", "reason": "ContainersNotInitialized"}
            ]
        }
    });
    let created = repo
        .test_create_pod("default", "init-forwarded", "worker-1", pod)
        .await
        .unwrap();
    let key = PodRuntimeKey::new("default", "init-forwarded", &created.uid);

    crate::runtime::cluster_policy::apply_forwarded_status(
        repo.pod_query.as_ref(),
        repo.pod_status_writer.as_ref(),
        &key,
        json!({
            "phase": "Succeeded",
            "podIP": "10.50.0.17",
            "hostIP": "192.0.2.10",
            "initContainerStatuses": [
                {
                    "name": "init1",
                    "ready": true,
                    "restartCount": 0,
                    "state": {"terminated": {"exitCode": 0, "reason": "Completed"}}
                },
                {
                    "name": "init2",
                    "ready": true,
                    "restartCount": 0,
                    "state": {"terminated": {"exitCode": 0, "reason": "Completed"}}
                }
            ],
            "containerStatuses": [
                {
                    "name": "run1",
                    "ready": false,
                    "restartCount": 0,
                    "state": {"terminated": {"exitCode": 0, "reason": "Completed"}}
                }
            ]
        }),
    )
    .await
    .unwrap();

    let stored = repo
        .pod_query
        .get_pod(
            klights_pod_api::PodGetRequest::try_by_identity(klights_types::PodIdentity::new(
                "default",
                "init-forwarded",
                &created.uid,
            ))
            .unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    let init_statuses = stored
        .data
        .pointer("/status/initContainerStatuses")
        .and_then(|value| value.as_array())
        .expect("forwarded full status must keep initContainerStatuses");
    assert_eq!(init_statuses.len(), 2);
    let initialized = stored
        .data
        .pointer("/status/conditions")
        .and_then(|value| value.as_array())
        .and_then(|conditions| {
            conditions.iter().find(|condition| {
                condition.pointer("/type").and_then(|value| value.as_str()) == Some("Initialized")
            })
        })
        .expect("Initialized condition must exist");
    assert_eq!(
        initialized
            .pointer("/status")
            .and_then(|value| value.as_str()),
        Some("True"),
        "completed forwarded init statuses must make Initialized=True"
    );
}

#[tokio::test]
async fn forwarded_init_status_without_network_fields_preserves_init_statuses() {
    let repo = build_test_pod_repository();
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "default",
            "name": "init-retry-forwarded",
            "uid": "uid-init-retry-forwarded",
            "resourceVersion": "1"
        },
        "spec": {
            "restartPolicy": "Always",
            "initContainers": [
                {"name": "init1", "image": "busybox"},
                {"name": "init2", "image": "busybox"}
            ],
            "containers": [{"name": "run1", "image": "busybox"}]
        },
        "status": {
            "phase": "Pending",
            "podIP": "10.50.0.17",
            "podIPs": [{"ip": "10.50.0.17"}],
            "hostIP": "192.0.2.10",
            "hostIPs": [{"ip": "192.0.2.10"}],
            "conditions": [
                {"type": "Initialized", "status": "False", "reason": "ContainersNotInitialized"}
            ],
            "containerStatuses": []
        }
    });
    let created = repo
        .test_create_pod("default", "init-retry-forwarded", "worker-1", pod)
        .await
        .unwrap();
    let key = PodRuntimeKey::new("default", "init-retry-forwarded", &created.uid);
    crate::runtime::cluster_policy::apply_forwarded_status(
        repo.pod_query.as_ref(),
        repo.pod_status_writer.as_ref(),
        &key,
        json!({
            "phase": "Pending",
            "podIP": "10.50.0.17",
            "hostIP": "192.0.2.10",
            "containerStatuses": []
        }),
    )
    .await
    .unwrap();

    crate::runtime::cluster_policy::apply_forwarded_status(
        repo.pod_query.as_ref(),
        repo.pod_status_writer.as_ref(),
        &key,
        json!({
            "phase": "Pending",
            "initContainerStatuses": [
                {
                    "name": "init1",
                    "ready": false,
                    "restartCount": 1,
                    "state": {"waiting": {"reason": "PodInitializing"}},
                    "lastState": {"terminated": {"exitCode": 1, "reason": "Error"}}
                },
                {
                    "name": "init2",
                    "ready": false,
                    "restartCount": 0,
                    "state": {"waiting": {"reason": "PodInitializing"}}
                }
            ],
            "containerStatuses": [
                {
                    "name": "run1",
                    "ready": false,
                    "restartCount": 0,
                    "state": {"waiting": {"reason": "PodInitializing"}}
                }
            ]
        }),
    )
    .await
    .unwrap();

    let stored = repo
        .pod_query
        .get_pod(
            klights_pod_api::PodGetRequest::try_by_identity(klights_types::PodIdentity::new(
                "default",
                "init-retry-forwarded",
                &created.uid,
            ))
            .unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        stored
            .data
            .pointer("/status/podIP")
            .and_then(|value| value.as_str()),
        Some("10.50.0.17"),
        "forwarded retry status without network fields must not clear podIP"
    );
    assert_eq!(
        stored
            .data
            .pointer("/status/initContainerStatuses/0/restartCount")
            .and_then(|value| value.as_i64()),
        Some(1),
        "forwarded init retry status must reach the leader"
    );
}

#[tokio::test]
async fn forwarded_network_status_without_init_statuses_preserves_existing_init_state() {
    let repo = build_test_pod_repository();
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "default",
            "name": "split-init-forwarded",
            "uid": "uid-split-init-forwarded",
            "resourceVersion": "1"
        },
        "spec": {
            "restartPolicy": "Always",
            "initContainers": [
                {"name": "init1", "image": "busybox"},
                {"name": "init2", "image": "busybox"}
            ],
            "containers": [{"name": "run1", "image": "busybox"}]
        },
        "status": {
            "phase": "Pending",
            "conditions": [
                {"type": "Initialized", "status": "False", "reason": "ContainersNotInitialized"}
            ],
            "containerStatuses": []
        }
    });
    let created = repo
        .test_create_pod("default", "split-init-forwarded", "worker-1", pod)
        .await
        .unwrap();
    let key = PodRuntimeKey::new("default", "split-init-forwarded", &created.uid);

    crate::runtime::cluster_policy::apply_forwarded_status(
        repo.pod_query.as_ref(),
        repo.pod_status_writer.as_ref(),
        &key,
        json!({
            "phase": "Pending",
            "initContainerStatuses": [
                {
                    "name": "init1",
                    "ready": false,
                    "restartCount": 1,
                    "state": {"waiting": {"reason": "PodInitializing"}},
                    "lastState": {"terminated": {"exitCode": 1, "reason": "Error"}}
                },
                {
                    "name": "init2",
                    "ready": false,
                    "restartCount": 0,
                    "state": {"waiting": {"reason": "PodInitializing"}}
                }
            ],
            "containerStatuses": [
                {
                    "name": "run1",
                    "ready": false,
                    "restartCount": 0,
                    "state": {"waiting": {"reason": "PodInitializing"}}
                }
            ]
        }),
    )
    .await
    .unwrap();

    crate::runtime::cluster_policy::apply_forwarded_status(
        repo.pod_query.as_ref(),
        repo.pod_status_writer.as_ref(),
        &key,
        json!({
            "phase": "Pending",
            "podIP": "10.50.0.18",
            "hostIP": "192.0.2.11",
            "containerStatuses": [
                {
                    "name": "run1",
                    "ready": false,
                    "restartCount": 0,
                    "state": {"waiting": {"reason": "PodInitializing"}}
                }
            ]
        }),
    )
    .await
    .unwrap();

    let stored = repo
        .pod_query
        .get_pod(
            klights_pod_api::PodGetRequest::try_by_identity(klights_types::PodIdentity::new(
                "default",
                "split-init-forwarded",
                &created.uid,
            ))
            .unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        stored
            .data
            .pointer("/status/initContainerStatuses/0/restartCount")
            .and_then(|value| value.as_i64()),
        Some(1),
        "network-bearing forwarded status must not clear prior init retry state"
    );
    assert_eq!(
        stored
            .data
            .pointer("/status/podIP")
            .and_then(|value| value.as_str()),
        Some("10.50.0.18")
    );
    let initialized = stored
        .data
        .pointer("/status/conditions")
        .and_then(|value| value.as_array())
        .and_then(|conditions| {
            conditions.iter().find(|condition| {
                condition.pointer("/type").and_then(|value| value.as_str()) == Some("Initialized")
            })
        })
        .expect("Initialized condition must exist");
    assert_eq!(
        initialized
            .pointer("/status")
            .and_then(|value| value.as_str()),
        Some("False"),
        "preserved retrying init status must keep Initialized=False"
    );
}
