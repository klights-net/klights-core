// P12.1f: relocated from base:tests/pod_delete_deadline_semantics.rs.
// That external binary imported
// `klights::pod_repository_composition_test_support::IntegrationPodApiFixture`,
// which is now private root composition support (`assembly_support.rs`)
// and unreachable from outside the crate; the test bodies move with it.
#[cfg(test)]
mod tests {
    use super::super::assembly_support::support::IntegrationPodApiFixture;
    use chrono::{DateTime, Duration, Utc};
    use klights_pod_api::PodApiDeleteOutcome;
    use serde_json::{Value, json};

    fn options(grace_period_seconds: Option<i64>) -> k8s_native_service::DeleteOptions {
        k8s_native_service::DeleteOptions {
            propagation_policy: None,
            orphan_dependents: None,
            _grace_period_seconds: grace_period_seconds,
            preconditions: None,
        }
    }

    fn pod(name: &str) -> Value {
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": name,
                "namespace": "default",
                "uid": format!("uid-{name}"),
                "generation": 7,
                "finalizers": ["example.test/hold"]
            },
            "spec": {
                "nodeName": "test-node",
                "terminationGracePeriodSeconds": 30,
                "containers": [{"name": "app", "image": "registry.k8s.io/pause:3.10"}]
            },
            "status": {"phase": "Running"}
        })
    }

    fn graceful(outcome: PodApiDeleteOutcome) -> klights_cluster_core::Resource {
        match outcome {
            PodApiDeleteOutcome::GracefulSet(resource) => resource,
            other => panic!("expected graceful Pod mark, got {other:?}"),
        }
    }

    fn deadline(resource: &klights_cluster_core::Resource) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(
            resource
                .data
                .pointer("/metadata/deletionTimestamp")
                .and_then(Value::as_str)
                .expect("deletionTimestamp"),
        )
        .expect("RFC3339 deadline")
        .with_timezone(&Utc)
    }

    async fn named_events_since(
        repo: &IntegrationPodApiFixture,
        resource_version: i64,
        name: &str,
    ) -> Vec<klights_watch::test_support::WatchFixtureEvent> {
        repo.watch
            .pod_events_since(resource_version)
            .await
            .expect("Pod watch history")
            .into_iter()
            .filter(|event| {
                event
                    .resource
                    .data
                    .pointer("/metadata/name")
                    .and_then(Value::as_str)
                    == Some(name)
            })
            .collect()
    }

    #[tokio::test]
    async fn first_repeat_and_shorten_have_exact_resource_version_and_watch_semantics() {
        let repo = IntegrationPodApiFixture::new_inline().await;
        let created = repo
            .persistence
            .seed_pod("default", "deadline-pod", pod("deadline-pod"))
            .await
            .expect("seed Pod");
        let before = Utc::now();
        let first = graceful(
            repo.deletion
                .delete_pod("default", "deadline-pod", options(None), false)
                .await
                .expect("first delete"),
        );
        let after = Utc::now();

        assert!(first.resource_version > created.resource_version);
        assert!(deadline(&first) >= before + Duration::seconds(29));
        assert!(deadline(&first) <= after + Duration::seconds(30));
        assert_eq!(
            first
                .data
                .pointer("/metadata/deletionGracePeriodSeconds")
                .and_then(Value::as_i64),
            Some(30)
        );
        assert_eq!(
            first
                .data
                .pointer("/metadata/generation")
                .and_then(Value::as_i64),
            Some(8)
        );
        assert_eq!(
            first
                .data
                .pointer("/metadata/finalizers/0")
                .and_then(Value::as_str),
            Some("example.test/hold")
        );
        let first_events =
            named_events_since(&repo, created.resource_version, "deadline-pod").await;
        assert_eq!(first_events.len(), 1);
        assert_eq!(first_events[0].event_type, "MODIFIED");
        assert_eq!(
            first_events[0].resource.resource_version,
            first.resource_version
        );

        for repeated in [options(None), options(Some(30)), options(Some(60))] {
            let outcome = graceful(
                repo.deletion
                    .delete_pod("default", "deadline-pod", repeated, false)
                    .await
                    .expect("repeated delete"),
            );
            assert_eq!(outcome.resource_version, first.resource_version);
            assert_eq!(outcome.data, first.data);
        }
        assert!(
            named_events_since(&repo, first.resource_version, "deadline-pod")
                .await
                .is_empty()
        );

        let shortened = graceful(
            repo.deletion
                .delete_pod(
                    "default",
                    "deadline-pod",
                    klights_pod_api::PodDeleteOptions::new(
                        None,
                        None,
                        Some(5),
                        klights_pod_api::PodDeletePreconditions::default(),
                    ),
                    false,
                )
                .await
                .expect("shorter delete"),
        );
        assert!(
            shortened.resource_version > first.resource_version,
            "shorten returned RV {} from first RV {} with body {}",
            shortened.resource_version,
            first.resource_version,
            shortened.data
        );
        assert_eq!(
            deadline(&shortened),
            deadline(&first) - Duration::seconds(25)
        );
        assert_eq!(
            shortened
                .data
                .pointer("/metadata/deletionGracePeriodSeconds")
                .and_then(Value::as_i64),
            Some(5)
        );
        assert_eq!(
            shortened
                .data
                .pointer("/metadata/generation")
                .and_then(Value::as_i64),
            Some(8)
        );
        assert_eq!(
            shortened
                .data
                .pointer("/metadata/finalizers/0")
                .and_then(Value::as_str),
            Some("example.test/hold")
        );
        let shortened_events =
            named_events_since(&repo, first.resource_version, "deadline-pod").await;
        assert_eq!(shortened_events.len(), 1);
        assert_eq!(shortened_events[0].event_type, "MODIFIED");
        assert_eq!(
            shortened_events[0].resource.resource_version,
            shortened.resource_version
        );
    }

    #[tokio::test]
    async fn negative_grace_and_dry_run_share_planning_without_dry_run_effects() {
        let repo = IntegrationPodApiFixture::new_inline().await;
        let created = repo
            .persistence
            .seed_pod("default", "dry-deadline-pod", pod("dry-deadline-pod"))
            .await
            .expect("seed Pod");
        let before = Utc::now();
        let dry_run = match repo
            .deletion
            .delete_pod("default", "dry-deadline-pod", options(Some(-9)), true)
            .await
            .expect("dry-run delete")
        {
            PodApiDeleteOutcome::DryRun(body) => body,
            other => panic!("expected dry-run outcome, got {other:?}"),
        };
        let after = Utc::now();
        let dry_deadline = DateTime::parse_from_rfc3339(
            dry_run
                .pointer("/metadata/deletionTimestamp")
                .and_then(Value::as_str)
                .expect("dry-run deadline"),
        )
        .expect("RFC3339 dry-run deadline")
        .with_timezone(&Utc);
        assert!(dry_deadline >= before);
        assert!(dry_deadline <= after + Duration::seconds(1));
        assert_eq!(
            dry_run
                .pointer("/metadata/deletionGracePeriodSeconds")
                .and_then(Value::as_i64),
            Some(1)
        );

        let persisted = repo
            .query
            .get_pod(
                klights_pod_api::PodGetRequest::try_by_name("default", "dry-deadline-pod")
                    .expect("Pod request"),
            )
            .await
            .expect("read Pod")
            .expect("Pod remains live");
        assert_eq!(persisted.resource_version, created.resource_version);
        assert!(
            persisted
                .data
                .pointer("/metadata/deletionTimestamp")
                .is_none()
        );
        assert!(
            named_events_since(&repo, created.resource_version, "dry-deadline-pod")
                .await
                .is_empty()
        );
    }
}
