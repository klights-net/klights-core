use std::sync::Arc;

use klights_cluster_core::Resource;
use klights_leader_api::{
    LeaderAuthenticatedProjectedServiceAccountToken, LeaderPodCleanupIntents,
    LeaderProjectedServiceAccountToken, PodCleanupIntent, PodCleanupIntentAckRequest,
    PodCleanupIntentError, PodCleanupIntentFuture, PodCleanupIntentListRequest,
    ProjectedServiceAccountToken, ProjectedServiceAccountTokenError,
    ProjectedServiceAccountTokenFuture, ProjectedServiceAccountTokenRequest,
};
use serde_json::{Value, json};

fn pod_snapshot(node_name: &str) -> Resource {
    Resource::try_from_data(Arc::new(json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "default",
            "name": "web",
            "uid": "pod-uid",
            "resourceVersion": "17"
        },
        "spec": {
            "nodeName": node_name,
            "serviceAccountName": "workload"
        }
    })))
    .expect("canonical Pod snapshot")
}

fn token_request() -> ProjectedServiceAccountTokenRequest {
    ProjectedServiceAccountTokenRequest::try_new(
        "default",
        "workload",
        vec!["https://kubernetes.default.svc.cluster.local".to_string()],
        3_600,
        "web",
        "pod-uid",
        "worker-a",
        Some("node-uid".to_string()),
    )
    .expect("valid node-originated token request")
}

#[test]
fn projected_token_request_requires_exact_owned_pod_and_node_binding() {
    let request = token_request();
    assert_eq!(request.namespace(), "default");
    assert_eq!(request.service_account_name(), "workload");
    assert_eq!(
        request.audiences(),
        &["https://kubernetes.default.svc.cluster.local"]
    );
    assert_eq!(request.expiration_seconds(), 3_600);
    assert_eq!(request.bound_pod_name(), "web");
    assert_eq!(request.bound_pod_uid(), "pod-uid");
    assert_eq!(request.bound_node_name(), "worker-a");
    assert_eq!(request.bound_node_uid(), Some("node-uid"));

    for invalid in [
        ProjectedServiceAccountTokenRequest::try_new(
            "",
            "workload",
            vec!["aud".to_string()],
            3_600,
            "web",
            "pod-uid",
            "worker-a",
            None,
        ),
        ProjectedServiceAccountTokenRequest::try_new(
            "default",
            "",
            vec!["aud".to_string()],
            3_600,
            "web",
            "pod-uid",
            "worker-a",
            None,
        ),
        ProjectedServiceAccountTokenRequest::try_new(
            "default",
            "workload",
            Vec::new(),
            3_600,
            "web",
            "pod-uid",
            "worker-a",
            None,
        ),
        ProjectedServiceAccountTokenRequest::try_new(
            "default",
            "workload",
            vec![String::new()],
            3_600,
            "web",
            "pod-uid",
            "worker-a",
            None,
        ),
        ProjectedServiceAccountTokenRequest::try_new(
            "default",
            "workload",
            vec!["aud".to_string()],
            0,
            "web",
            "pod-uid",
            "worker-a",
            None,
        ),
        ProjectedServiceAccountTokenRequest::try_new(
            "default",
            "workload",
            vec!["aud".to_string()],
            3_600,
            "",
            "pod-uid",
            "worker-a",
            None,
        ),
        ProjectedServiceAccountTokenRequest::try_new(
            "default",
            "workload",
            vec!["aud".to_string()],
            3_600,
            "web",
            "",
            "worker-a",
            None,
        ),
        ProjectedServiceAccountTokenRequest::try_new(
            "default",
            "workload",
            vec!["aud".to_string()],
            3_600,
            "web",
            "pod-uid",
            "",
            None,
        ),
        ProjectedServiceAccountTokenRequest::try_new(
            "default",
            "workload",
            vec!["aud".to_string()],
            3_600,
            "web",
            "pod-uid",
            "worker-a",
            Some(String::new()),
        ),
    ] {
        assert!(matches!(
            invalid,
            Err(ProjectedServiceAccountTokenError::InvalidRequest { .. })
        ));
    }
}

#[test]
fn projected_token_result_rejects_empty_signing_output() {
    let token =
        ProjectedServiceAccountToken::try_new("header.claims.signature").expect("non-empty token");
    assert_eq!(token.token(), "header.claims.signature");
    assert_eq!(token.into_token(), "header.claims.signature");
    assert!(matches!(
        ProjectedServiceAccountToken::try_new(""),
        Err(ProjectedServiceAccountTokenError::CorruptResponse { .. })
    ));
}

#[test]
fn cleanup_intent_validates_exact_tuple_and_canonical_bound_pod_snapshot() {
    let intent = PodCleanupIntent::try_new(
        "worker-a",
        "default",
        "web",
        "pod-uid",
        "NodeLost",
        22,
        1_700_000_000_000,
        pod_snapshot("worker-a"),
    )
    .expect("valid cleanup intent");
    assert_eq!(intent.node_name(), "worker-a");
    assert_eq!(intent.namespace(), "default");
    assert_eq!(intent.pod_name(), "web");
    assert_eq!(intent.pod_uid(), "pod-uid");
    assert_eq!(intent.reason(), "NodeLost");
    assert_eq!(intent.resource_version(), 22);
    assert_eq!(intent.created_at_ms(), 1_700_000_000_000);
    assert_eq!(intent.pod_snapshot().resource_version, 17);

    let ack = intent.ack_request().expect("exact acknowledgement key");
    assert_eq!(ack.node_name(), "worker-a");
    assert_eq!(ack.namespace(), "default");
    assert_eq!(ack.pod_name(), "web");
    assert_eq!(ack.pod_uid(), "pod-uid");
    assert_eq!(ack.reason(), "NodeLost");
}

#[test]
fn cleanup_intent_rejects_missing_or_mismatched_identity_and_node_binding() {
    let cases = [
        PodCleanupIntent::try_new(
            "worker-b",
            "default",
            "web",
            "pod-uid",
            "NodeLost",
            22,
            1_700_000_000_000,
            pod_snapshot("worker-a"),
        ),
        PodCleanupIntent::try_new(
            "worker-a",
            "default",
            "web",
            "other-uid",
            "NodeLost",
            22,
            1_700_000_000_000,
            pod_snapshot("worker-a"),
        ),
        PodCleanupIntent::try_new(
            "worker-a",
            "default",
            "web",
            "pod-uid",
            "NodeLost",
            -1,
            1_700_000_000_000,
            pod_snapshot("worker-a"),
        ),
        PodCleanupIntent::try_new(
            "worker-a",
            "default",
            "web",
            "pod-uid",
            "NodeLost",
            22,
            -1,
            pod_snapshot("worker-a"),
        ),
    ];
    for invalid in cases {
        assert!(matches!(
            invalid,
            Err(PodCleanupIntentError::CorruptIntent { .. })
                | Err(PodCleanupIntentError::InvalidRequest { .. })
        ));
    }

    let wrong_kind = Resource::try_from_data(Arc::new(json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "namespace": "default",
            "name": "web",
            "uid": "pod-uid",
            "resourceVersion": "17"
        },
        "spec": {"nodeName": "worker-a"}
    })))
    .unwrap();
    assert!(matches!(
        PodCleanupIntent::try_new(
            "worker-a",
            "default",
            "web",
            "pod-uid",
            "NodeLost",
            22,
            1_700_000_000_000,
            wrong_kind,
        ),
        Err(PodCleanupIntentError::CorruptIntent { .. })
    ));

    let mut inconsistent = pod_snapshot("worker-a");
    inconsistent.name = "forged".to_string();
    assert!(matches!(
        PodCleanupIntent::try_new(
            "worker-a",
            "default",
            "web",
            "pod-uid",
            "NodeLost",
            22,
            1_700_000_000_000,
            inconsistent,
        ),
        Err(PodCleanupIntentError::CorruptIntent { .. })
    ));
}

#[test]
fn cleanup_requests_reject_widened_or_partial_keys() {
    assert_eq!(
        PodCleanupIntentListRequest::try_new("worker-a")
            .unwrap()
            .node_name(),
        "worker-a"
    );
    assert!(matches!(
        PodCleanupIntentListRequest::try_new(""),
        Err(PodCleanupIntentError::InvalidRequest { .. })
    ));
    assert!(matches!(
        PodCleanupIntentAckRequest::try_new("worker-a", "default", "web", "", "NodeLost"),
        Err(PodCleanupIntentError::InvalidRequest { .. })
    ));
}

struct ObjectSafePodEffects;

impl LeaderProjectedServiceAccountToken for ObjectSafePodEffects {
    fn issue_projected_service_account_token(
        &self,
        _request: ProjectedServiceAccountTokenRequest,
    ) -> ProjectedServiceAccountTokenFuture<'_> {
        Box::pin(async { ProjectedServiceAccountToken::try_new("header.claims.signature") })
    }
}

impl LeaderAuthenticatedProjectedServiceAccountToken for ObjectSafePodEffects {
    fn issue_authenticated_projected_service_account_token(
        &self,
        _request: ProjectedServiceAccountTokenRequest,
    ) -> ProjectedServiceAccountTokenFuture<'_> {
        Box::pin(async { ProjectedServiceAccountToken::try_new("header.claims.signature") })
    }
}

impl LeaderPodCleanupIntents for ObjectSafePodEffects {
    fn list_pod_cleanup_intents(
        &self,
        _request: PodCleanupIntentListRequest,
    ) -> PodCleanupIntentFuture<'_, Vec<PodCleanupIntent>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn acknowledge_pod_cleanup_intent(
        &self,
        _request: PodCleanupIntentAckRequest,
    ) -> PodCleanupIntentFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

#[test]
fn pod_effect_capabilities_are_object_safe_and_errors_stay_typed() {
    let token_client: &dyn LeaderProjectedServiceAccountToken = &ObjectSafePodEffects;
    drop(token_client.issue_projected_service_account_token(token_request()));
    let authenticated_token_client: &dyn LeaderAuthenticatedProjectedServiceAccountToken =
        &ObjectSafePodEffects;
    drop(
        authenticated_token_client
            .issue_authenticated_projected_service_account_token(token_request()),
    );

    let cleanup_client: &dyn LeaderPodCleanupIntents = &ObjectSafePodEffects;
    drop(
        cleanup_client
            .list_pod_cleanup_intents(PodCleanupIntentListRequest::try_new("worker-a").unwrap()),
    );
    drop(
        cleanup_client.acknowledge_pod_cleanup_intent(
            PodCleanupIntentAckRequest::try_new(
                "worker-a", "default", "web", "pod-uid", "NodeLost",
            )
            .unwrap(),
        ),
    );

    let typed_errors: [Box<dyn std::error::Error>; 2] = [
        Box::new(ProjectedServiceAccountTokenError::NotLeader),
        Box::new(PodCleanupIntentError::Unauthorized),
    ];
    assert_eq!(typed_errors.len(), 2);
}

#[test]
fn cleanup_snapshot_body_remains_shared_without_json_reencoding() {
    let snapshot = pod_snapshot("worker-a");
    let body = snapshot.data.clone();
    let intent = PodCleanupIntent::try_new(
        "worker-a",
        "default",
        "web",
        "pod-uid",
        "NodeLost",
        22,
        1_700_000_000_000,
        snapshot,
    )
    .unwrap();
    assert!(Arc::ptr_eq(&body, &intent.pod_snapshot().data));
    let _: &Value = intent.pod_snapshot().data.as_ref();
}
