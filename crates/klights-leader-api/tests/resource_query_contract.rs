use std::sync::Arc;

use klights_cluster_core::{Resource, WatchReplayPosition};
use klights_leader_api::{
    LeaderResourceQuery, ResourceGetRequest, ResourceListContinuationMode, ResourceListRequest,
    ResourceListResult, ResourceListScope, ResourceQueryConsistency, ResourceQueryError,
    ResourceQueryFuture, config_map_get_request, node_get_request, pod_get_request,
    pods_on_node_list_request, secret_get_request,
};
use klights_types::ResourceKey;

struct EmptyQuery;

impl LeaderResourceQuery for EmptyQuery {
    fn get_resource(
        &self,
        _request: ResourceGetRequest,
    ) -> ResourceQueryFuture<'_, Option<Resource>> {
        Box::pin(async { Ok(None) })
    }

    fn list_resources(
        &self,
        request: ResourceListRequest,
    ) -> ResourceQueryFuture<'_, ResourceListResult> {
        Box::pin(async move {
            ResourceListResult::try_new(
                Vec::new(),
                0,
                None,
                request.continue_token().map(str::to_owned),
                None,
            )
        })
    }
}

fn assert_object_safe(_: &dyn LeaderResourceQuery) {}

#[test]
fn resource_query_port_is_object_safe_and_leaf_values_are_send_sync() {
    assert_object_safe(&EmptyQuery);
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<ResourceGetRequest>();
    assert_send_sync::<ResourceListRequest>();
    assert_send_sync::<ResourceListResult>();
    assert_send_sync::<ResourceQueryError>();
}

#[test]
fn get_request_validates_identity_and_preserves_consistency() {
    for consistency in [
        ResourceQueryConsistency::Cached,
        ResourceQueryConsistency::LeaderFresh,
    ] {
        let key = ResourceKey::new("apps/v1", "Deployment", Some("team-a".into()), "web");
        let request = ResourceGetRequest::try_new(key.clone(), consistency).unwrap();
        assert_eq!(request.key(), &key);
        assert_eq!(request.consistency(), consistency);
        assert_eq!(request.into_key(), key);
    }

    for key in [
        ResourceKey::new("", "Pod", Some("default".into()), "p"),
        ResourceKey::new("v1", "", Some("default".into()), "p"),
        ResourceKey::new("v1", "Pod", Some(String::new()), "p"),
        ResourceKey::new("v1", "Pod", Some("default".into()), ""),
    ] {
        assert!(matches!(
            ResourceGetRequest::try_new(key, ResourceQueryConsistency::Cached),
            Err(ResourceQueryError::InvalidRequest { .. })
        ));
    }
}

#[test]
fn list_request_preserves_selectors_pagination_and_consistency_exactly() {
    let request = ResourceListRequest::try_new(
        "v1",
        "Pod",
        ResourceListScope::Namespace("default".to_string()),
        Some(" app in (web,api) ".to_string()),
        Some("spec.nodeName=node-a".to_string()),
        Some(37),
        Some("opaque/+continue==".to_string()),
        ResourceQueryConsistency::LeaderFresh,
    )
    .unwrap();
    assert_eq!(request.api_version(), "v1");
    assert_eq!(request.kind(), "Pod");
    assert_eq!(request.namespace(), Some("default"));
    assert_eq!(request.label_selector(), Some(" app in (web,api) "));
    assert_eq!(request.field_selector(), Some("spec.nodeName=node-a"));
    assert_eq!(request.limit(), Some(37));
    assert_eq!(request.continue_token(), Some("opaque/+continue=="));
    assert_eq!(request.consistency(), ResourceQueryConsistency::LeaderFresh);

    for invalid in [
        ResourceListRequest::try_new(
            "",
            "Pod",
            ResourceListScope::Cluster,
            None,
            None,
            None,
            None,
            ResourceQueryConsistency::Cached,
        ),
        ResourceListRequest::try_new(
            "v1",
            "",
            ResourceListScope::Cluster,
            None,
            None,
            None,
            None,
            ResourceQueryConsistency::Cached,
        ),
        ResourceListRequest::try_new(
            "v1",
            "Pod",
            ResourceListScope::Namespace(String::new()),
            None,
            None,
            None,
            None,
            ResourceQueryConsistency::Cached,
        ),
        ResourceListRequest::try_new(
            "v1",
            "Pod",
            ResourceListScope::Cluster,
            None,
            None,
            Some(-1),
            None,
            ResourceQueryConsistency::Cached,
        ),
    ] {
        assert!(matches!(
            invalid,
            Err(ResourceQueryError::InvalidRequest { .. })
        ));
    }
}

#[test]
fn list_continuation_mode_is_explicit_and_expiry_keeps_a_recovery_token() {
    let request = ResourceListRequest::try_new_with_continuation_mode(
        "v1",
        "ConfigMap",
        ResourceListScope::AllNamespaces,
        Some("purpose=\u{1f680}/prod".into()),
        None,
        Some(1),
        Some("opaque/\u{1f984}?n=1".into()),
        ResourceListContinuationMode::Recovery,
        ResourceQueryConsistency::LeaderFresh,
    )
    .unwrap();
    assert_eq!(
        request.continuation_mode(),
        ResourceListContinuationMode::Recovery
    );
    assert_eq!(request.continue_token(), Some("opaque/\u{1f984}?n=1"));

    assert!(matches!(
        ResourceListRequest::try_new_with_continuation_mode(
            "v1",
            "ConfigMap",
            ResourceListScope::AllNamespaces,
            None,
            None,
            Some(1),
            Some("opaque".into()),
            ResourceListContinuationMode::Initial,
            ResourceQueryConsistency::LeaderFresh,
        ),
        Err(ResourceQueryError::InvalidRequest { .. })
    ));

    let expired = ResourceQueryError::expired(41, 53, Some("recovery/\u{1f680}".to_string()));
    assert!(matches!(
        expired,
        ResourceQueryError::Expired {
            requested: 41,
            oldest_available: 53,
            replacement_continue_token,
        } if replacement_continue_token.as_deref() == Some("recovery/\u{1f680}")
    ));
}

#[test]
fn list_result_preserves_resources_public_rv_and_watch_handoff() {
    let first = Resource::try_from_data(Arc::new(serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "namespace": "default",
            "name": "first",
            "uid": "uid-first",
            "resourceVersion": "41"
        }
    })))
    .unwrap();
    let position = WatchReplayPosition {
        resource_version: 41,
        event_id: 91,
        resource_version_filter_through_event_id: 0,
    };
    let result = ResourceListResult::try_new(
        vec![first.clone()],
        41,
        Some(position),
        Some("next-page".to_string()),
        Some(8),
    )
    .unwrap();
    assert_eq!(result.items().len(), 1);
    assert!(Arc::ptr_eq(&result.items()[0].data, &first.data));
    assert_eq!(result.resource_version(), 41);
    assert_eq!(result.watch_replay_position(), Some(position));
    assert_eq!(result.continue_token(), Some("next-page"));
    assert_eq!(result.remaining_item_count(), Some(8));

    assert!(matches!(
        ResourceListResult::try_new(Vec::new(), -1, None, None, None),
        Err(ResourceQueryError::CorruptResponse { .. })
    ));
    assert!(matches!(
        ResourceListResult::try_new(Vec::new(), 41, Some(position), None, Some(-1)),
        Err(ResourceQueryError::CorruptResponse { .. })
    ));
}

#[test]
fn typed_resource_helpers_are_pure_request_constructors() {
    for (request, kind, namespace, name) in [
        (
            pod_get_request("default", "web", ResourceQueryConsistency::LeaderFresh).unwrap(),
            "Pod",
            Some("default"),
            "web",
        ),
        (
            config_map_get_request("default", "settings", ResourceQueryConsistency::Cached)
                .unwrap(),
            "ConfigMap",
            Some("default"),
            "settings",
        ),
        (
            secret_get_request("default", "pull", ResourceQueryConsistency::Cached).unwrap(),
            "Secret",
            Some("default"),
            "pull",
        ),
        (
            node_get_request("node-a", ResourceQueryConsistency::LeaderFresh).unwrap(),
            "Node",
            None,
            "node-a",
        ),
    ] {
        assert_eq!(request.key().api_version, "v1");
        assert_eq!(request.key().kind, kind);
        assert_eq!(request.key().namespace.as_deref(), namespace);
        assert_eq!(request.key().name, name);
    }

    let pods = pods_on_node_list_request("node-a", ResourceQueryConsistency::Cached).unwrap();
    assert_eq!(pods.api_version(), "v1");
    assert_eq!(pods.kind(), "Pod");
    assert_eq!(pods.namespace(), None);
    assert_eq!(pods.field_selector(), Some("spec.nodeName=node-a"));
    assert_eq!(pods.limit(), None);
    assert_eq!(pods.continue_token(), None);
}

#[test]
fn query_errors_keep_absence_retry_timeout_and_cancellation_distinct() {
    let missing = ResourceQueryError::NotFound {
        key: ResourceKey::new("v1", "Node", None, "missing"),
    };
    let errors = [
        missing,
        ResourceQueryError::QueryFailed {
            message: "backend".into(),
        },
        ResourceQueryError::Retryable {
            message: "leader unavailable".into(),
        },
        ResourceQueryError::Timeout,
        ResourceQueryError::Cancelled,
    ];
    assert!(matches!(errors[0], ResourceQueryError::NotFound { .. }));
    assert!(matches!(errors[2], ResourceQueryError::Retryable { .. }));
    assert_ne!(errors[3], errors[4]);
}
