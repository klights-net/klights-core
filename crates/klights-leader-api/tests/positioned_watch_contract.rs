use std::sync::Arc;

use klights_cluster_core::{Resource, WatchReplayPosition};
use klights_leader_api::{
    CacheReadinessError, CacheReadinessFuture, CacheReadinessRequest, LeaderCacheReadiness,
    LeaderWatch, LeaderWatchError, LeaderWatchFuture, ResourceEvent, ResourceListScope,
    WatchEventType, WatchRequest, WatchResumeCursor,
};
use serde_json::json;

struct ObjectSafeWatch;

impl LeaderWatch for ObjectSafeWatch {
    fn watch_resources(&self, _request: WatchRequest) -> LeaderWatchFuture<'_> {
        panic!("object-safety check does not poll the future")
    }
}

impl LeaderCacheReadiness for ObjectSafeWatch {
    fn wait_cache_ready(&self, _request: CacheReadinessRequest) -> CacheReadinessFuture<'_> {
        panic!("object-safety check does not poll the future")
    }
}

fn assert_watch_object_safe(_: &dyn LeaderWatch) {}
fn assert_readiness_object_safe(_: &dyn LeaderCacheReadiness) {}

fn resource(
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
    resource_version: i64,
) -> Resource {
    Resource::try_from_data(Arc::new(json!({
        "apiVersion": api_version,
        "kind": kind,
        "metadata": {
            "namespace": namespace,
            "name": name,
            "uid": format!("uid-{name}"),
            "resourceVersion": resource_version.to_string()
        }
    })))
    .expect("valid resource")
}

#[test]
fn positioned_watch_ports_are_object_safe_and_values_are_send_sync() {
    assert_watch_object_safe(&ObjectSafeWatch);
    assert_readiness_object_safe(&ObjectSafeWatch);

    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<WatchRequest>();
    assert_send_sync::<ResourceEvent>();
    assert_send_sync::<LeaderWatchError>();
    assert_send_sync::<CacheReadinessRequest>();
    assert_send_sync::<CacheReadinessError>();
    assert_send_sync::<WatchResumeCursor>();
}

#[test]
fn request_preserves_selectors_and_prefers_exact_event_id_intent() {
    let position = WatchReplayPosition {
        resource_version: 41,
        event_id: 91,
        resource_version_filter_through_event_id: 173,
    };
    let request = WatchRequest::try_new(
        "apps/v1",
        "Deployment",
        Some("team-a".to_string()),
        Some(" app in (web,api) ".to_string()),
        Some("metadata.name!=old".to_string()),
        Some(41),
        Some(position),
    )
    .expect("valid positioned request");

    assert_eq!(request.api_version(), "apps/v1");
    assert_eq!(request.kind(), "Deployment");
    assert_eq!(request.namespace(), Some("team-a"));
    assert_eq!(request.label_selector(), Some(" app in (web,api) "));
    assert_eq!(request.field_selector(), Some("metadata.name!=old"));
    assert_eq!(request.start_resource_version(), Some(41));
    assert_eq!(request.start_watch_replay_position(), Some(position));
    assert_eq!(request.preferred_replay_position(), Some(position));

    let scalar_only = WatchRequest::try_new("v1", "Pod", None, None, None, Some(17), None)
        .expect("legacy scalar request");
    assert_eq!(scalar_only.start_resource_version(), Some(17));
    assert_eq!(scalar_only.preferred_replay_position(), None);
}

#[test]
fn watch_scope_is_explicit_and_cannot_alias_collection_shapes() {
    let namespaced = WatchRequest::try_new_with_scope(
        "v1",
        "Pod",
        Some("team-a".into()),
        ResourceListScope::Namespace("team-a".into()),
        None,
        None,
        None,
        None,
    )
    .expect("matching namespace scope");
    assert_eq!(
        namespaced.scope(),
        &ResourceListScope::Namespace("team-a".into())
    );

    for (scope, namespace) in [
        (ResourceListScope::Cluster, Some("team-a".to_string())),
        (ResourceListScope::AllNamespaces, Some("team-a".to_string())),
        (ResourceListScope::Namespace("team-a".to_string()), None),
        (
            ResourceListScope::Namespace("other".to_string()),
            Some("team-a".to_string()),
        ),
    ] {
        assert!(matches!(
            WatchRequest::try_new_with_scope("v1", "Pod", namespace, scope, None, None, None, None),
            Err(LeaderWatchError::InvalidRequest {
                field: "watch.scope",
                ..
            })
        ));
    }
}

#[test]
fn request_rejects_a_completed_composite_filter_before_opening_history() {
    for position in [
        WatchReplayPosition {
            resource_version: 41,
            event_id: 91,
            resource_version_filter_through_event_id: 91,
        },
        WatchReplayPosition {
            resource_version: 41,
            event_id: 92,
            resource_version_filter_through_event_id: 91,
        },
    ] {
        assert!(matches!(
            WatchRequest::try_new("v1", "Pod", None, None, None, Some(41), Some(position),),
            Err(LeaderWatchError::InvalidRequest {
                field: "watch.start_watch_replay_position",
                ..
            })
        ));
    }
}

#[test]
fn request_rejects_malformed_field_selectors_at_the_internal_boundary() {
    for selector in ["metadata.name", "metadata.name>pod", "metadata.name===pod"] {
        assert!(matches!(
            WatchRequest::try_new(
                "v1",
                "Pod",
                None,
                None,
                Some(selector.to_string()),
                None,
                None,
            ),
            Err(LeaderWatchError::InvalidRequest {
                field: "watch.field_selector",
                ..
            })
        ));
    }
}

#[test]
fn request_and_readiness_validation_is_typed_and_selector_aware() {
    for request in [
        WatchRequest::try_new("", "Pod", None, None, None, None, None),
        WatchRequest::try_new("v1", "", None, None, None, None, None),
        WatchRequest::try_new("v1", "Pod", Some(String::new()), None, None, None, None),
        WatchRequest::try_new("v1", "Pod", None, None, None, Some(-1), None),
        WatchRequest::try_new(
            "v1",
            "Pod",
            None,
            None,
            None,
            Some(1),
            Some(WatchReplayPosition {
                resource_version: 1,
                event_id: -1,
                resource_version_filter_through_event_id: 0,
            }),
        ),
    ] {
        assert!(matches!(
            request,
            Err(LeaderWatchError::InvalidRequest { .. })
        ));
    }

    let readiness = CacheReadinessRequest::try_new(
        "v1",
        "Pod",
        None,
        Some("app=web".to_string()),
        Some("spec.nodeName=node-a".to_string()),
    )
    .expect("selector-aware readiness");
    assert_eq!(readiness.api_version(), "v1");
    assert_eq!(readiness.kind(), "Pod");
    assert_eq!(readiness.namespace(), None);
    assert_eq!(readiness.label_selector(), Some("app=web"));
    assert_eq!(readiness.field_selector(), Some("spec.nodeName=node-a"));
}

#[test]
fn events_preserve_canonical_resource_arc_and_reject_bad_wire_semantics() {
    let pod = resource("v1", "Pod", Some("default"), "web", 42);
    let position = WatchReplayPosition {
        resource_version: 42,
        event_id: 92,
        resource_version_filter_through_event_id: 0,
    };
    let event = ResourceEvent::try_new(WatchEventType::Modified, pod.clone(), Some(position))
        .expect("valid event");
    assert_eq!(event.event_type(), WatchEventType::Modified);
    assert!(Arc::ptr_eq(&event.resource().data, &pod.data));
    assert_eq!(event.resume_position(), Some(position));

    let request = WatchRequest::try_new(
        "v1",
        "Pod",
        Some("default".to_string()),
        None,
        None,
        Some(41),
        None,
    )
    .unwrap();
    event.validate_for(&request).expect("matching event");

    let wrong_kind = resource("v1", "Service", Some("default"), "web", 42);
    assert!(matches!(
        ResourceEvent::try_new(WatchEventType::Modified, wrong_kind, Some(position))
            .and_then(|event| event.validate_for(&request)),
        Err(LeaderWatchError::MismatchedEvent { .. })
    ));
    assert!(matches!(
        ResourceEvent::try_from_wire_type("RENAMED", pod, Some(position)),
        Err(LeaderWatchError::UnknownEventType { .. })
    ));

    let malformed = Resource::from_data_lossy(Arc::new(json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"resourceVersion": "42"}
    })));
    assert!(matches!(
        ResourceEvent::try_new(WatchEventType::Added, malformed, Some(position)),
        Err(LeaderWatchError::MalformedEvent { .. })
    ));
}

#[test]
fn namespaced_watch_accepts_namespace_free_bookmark_progress() {
    let request = WatchRequest::try_new(
        "coordination.k8s.io/v1",
        "Lease",
        Some("kube-node-lease".to_string()),
        None,
        None,
        Some(41),
        None,
    )
    .expect("valid namespaced Lease watch");
    let bookmark = ResourceEvent::try_new(
        WatchEventType::Bookmark,
        resource("coordination.k8s.io/v1", "Lease", None, "", 42),
        Some(WatchReplayPosition {
            resource_version: 42,
            event_id: 92,
            resource_version_filter_through_event_id: 0,
        }),
    )
    .expect("valid bookmark");

    bookmark
        .validate_for(&request)
        .expect("BOOKMARK progress is scoped by its stream and need not carry metadata.namespace");
}

#[test]
fn cursor_advances_only_after_explicit_apply_and_keeps_event_id_order() {
    let initial_position = WatchReplayPosition {
        resource_version: 41,
        event_id: 91,
        resource_version_filter_through_event_id: 0,
    };
    let mut cursor = WatchResumeCursor::try_new(Some(41), Some(initial_position)).unwrap();
    let later_event_id_lower_public_rv = ResourceEvent::try_new(
        WatchEventType::Modified,
        resource("v1", "Pod", Some("default"), "web", 39),
        Some(WatchReplayPosition {
            resource_version: 39,
            event_id: 92,
            resource_version_filter_through_event_id: 0,
        }),
    )
    .unwrap();

    assert_eq!(cursor.resource_version(), Some(41));
    assert_eq!(cursor.replay_position(), Some(initial_position));
    cursor
        .advance_after_apply(&later_event_id_lower_public_rv)
        .expect("later event ID is authoritative");
    assert_eq!(cursor.resource_version(), Some(41));
    assert_eq!(cursor.replay_position().unwrap().event_id, 92);

    let stale_event_id = ResourceEvent::try_new(
        WatchEventType::Modified,
        resource("v1", "Pod", Some("default"), "web", 50),
        Some(WatchReplayPosition {
            resource_version: 50,
            event_id: 90,
            resource_version_filter_through_event_id: 0,
        }),
    )
    .unwrap();
    assert!(matches!(
        cursor.advance_after_apply(&stale_event_id),
        Err(LeaderWatchError::OutOfOrderEvent { .. })
    ));

    let legacy = ResourceEvent::try_new(
        WatchEventType::Modified,
        resource("v1", "Pod", Some("default"), "web", 43),
        None,
    )
    .unwrap();
    cursor.advance_after_apply(&legacy).unwrap();
    assert_eq!(cursor.resource_version(), Some(43));
    assert_eq!(cursor.replay_position(), None);
}

#[test]
fn resume_cursor_rejects_every_noncanonical_position_transition_atomically() {
    struct Case {
        name: &'static str,
        current: WatchReplayPosition,
        delivered: WatchReplayPosition,
    }
    let cases = [
        Case {
            name: "equal event id mutation",
            current: WatchReplayPosition {
                resource_version: 10,
                event_id: 3,
                resource_version_filter_through_event_id: 0,
            },
            delivered: WatchReplayPosition {
                resource_version: 11,
                event_id: 3,
                resource_version_filter_through_event_id: 0,
            },
        },
        Case {
            name: "premature composite filter clearing",
            current: WatchReplayPosition {
                resource_version: 10,
                event_id: 1,
                resource_version_filter_through_event_id: 5,
            },
            delivered: WatchReplayPosition {
                resource_version: 11,
                event_id: 2,
                resource_version_filter_through_event_id: 0,
            },
        },
        Case {
            name: "resource version changes before composite boundary",
            current: WatchReplayPosition {
                resource_version: 10,
                event_id: 1,
                resource_version_filter_through_event_id: 5,
            },
            delivered: WatchReplayPosition {
                resource_version: 11,
                event_id: 2,
                resource_version_filter_through_event_id: 5,
            },
        },
        Case {
            name: "new composite filter appears",
            current: WatchReplayPosition {
                resource_version: 10,
                event_id: 1,
                resource_version_filter_through_event_id: 0,
            },
            delivered: WatchReplayPosition {
                resource_version: 11,
                event_id: 2,
                resource_version_filter_through_event_id: 5,
            },
        },
    ];

    for case in cases {
        let mut cursor = WatchResumeCursor::try_new(Some(10), Some(case.current)).unwrap();
        let before = cursor;
        let event = ResourceEvent::try_new(
            WatchEventType::Modified,
            resource("v1", "Pod", Some("default"), "web", 99),
            Some(case.delivered),
        )
        .unwrap();
        assert!(cursor.advance_after_apply(&event).is_err(), "{}", case.name,);
        assert_eq!(cursor, before, "{} changed cursor on rejection", case.name);
    }
}

#[test]
fn replay_expiry_unavailability_and_cancellation_remain_typed() {
    let errors = [
        LeaderWatchError::ReplayExpired {
            accepted_resource_version: 41,
        },
        LeaderWatchError::Unavailable {
            message: "no leader".to_string(),
        },
        LeaderWatchError::Cancelled,
    ];
    assert!(matches!(
        errors[0],
        LeaderWatchError::ReplayExpired {
            accepted_resource_version: 41
        }
    ));
    assert!(matches!(errors[1], LeaderWatchError::Unavailable { .. }));
    assert_eq!(errors[2], LeaderWatchError::Cancelled);

    assert!(matches!(
        CacheReadinessError::Unavailable {
            message: "not wired".to_string()
        },
        CacheReadinessError::Unavailable { .. }
    ));
}
