use klights_cluster_core::{Resource, WatchReplayPosition};
use klights_cluster_store::{
    ClusterResourceRead, ResourceCollectionKey, ResourceCollectionScope, ResourceContinuation,
    ResourceGetRequest, ResourceListPage, ResourceListQuery, ResourceListRead, ResourceListRequest,
    ResourceListSnapshot, ResourceReadError, ResourceReadFuture, ResourceVersionMatch,
};

struct EmptyReader;

impl ClusterResourceRead for EmptyReader {
    fn get_resource(
        &self,
        _request: ResourceGetRequest,
    ) -> ResourceReadFuture<'_, Option<Resource>> {
        Box::pin(async { Ok(None) })
    }

    fn list_resources(
        &self,
        _request: ResourceListRequest,
    ) -> ResourceReadFuture<'_, ResourceListRead> {
        Box::pin(async {
            Ok(ResourceListRead::Current(
                ResourceListPage::try_new(
                    Vec::new(),
                    ResourceListSnapshot::try_new(WatchReplayPosition::default()).unwrap(),
                    None,
                    None,
                )
                .unwrap(),
            ))
        })
    }
}

fn assert_object_safe(_: &dyn ClusterResourceRead) {}

#[test]
fn resource_read_capability_is_object_safe() {
    assert_object_safe(&EmptyReader);
}

#[test]
fn get_and_list_requests_preserve_resource_identity() {
    let get = ResourceGetRequest::new(
        "group/version/raw",
        "kind/raw",
        Some(String::new()),
        "name/raw",
    );
    assert_eq!(get.key().api_version, "group/version/raw");
    assert_eq!(get.key().kind, "kind/raw");
    assert_eq!(get.key().namespace.as_deref(), Some(""));
    assert_eq!(get.key().name, "name/raw");

    let list = ResourceListRequest::new(
        "apps/v1",
        "Deployment",
        ResourceCollectionScope::Namespace("tenant-a".to_string()),
        ResourceListQuery::all(),
    );
    assert_eq!(list.api_version(), "apps/v1");
    assert_eq!(list.kind(), "Deployment");
    assert_eq!(
        list.scope(),
        &ResourceCollectionScope::Namespace("tenant-a".to_string())
    );
}

#[test]
fn list_query_pagination_contract_is_table_driven() {
    struct Case {
        name: &'static str,
        limit: Option<i64>,
        continue_token: Option<ResourceContinuation>,
        expected_limit: Option<i64>,
        expected_continue: bool,
        invalid_limit: Option<i64>,
    }

    let cases = [
        Case {
            name: "omitted pagination is unbounded",
            limit: None,
            continue_token: None,
            expected_limit: None,
            expected_continue: false,
            invalid_limit: None,
        },
        Case {
            name: "zero limit is Kubernetes unbounded",
            limit: Some(0),
            continue_token: None,
            expected_limit: None,
            expected_continue: false,
            invalid_limit: None,
        },
        Case {
            name: "positive limit and token are exact",
            limit: Some(25),
            continue_token: Some(ResourceContinuation::new(
                ResourceCollectionKey::new(Some("tenant-a"), "same-name"),
                ResourceListSnapshot::try_new(WatchReplayPosition {
                    resource_version: 31,
                    event_id: 47,
                    resource_version_filter_through_event_id: 0,
                })
                .unwrap(),
            )),
            expected_limit: Some(25),
            expected_continue: true,
            invalid_limit: None,
        },
        Case {
            name: "negative limit is rejected",
            limit: Some(-1),
            continue_token: None,
            expected_limit: None,
            expected_continue: false,
            invalid_limit: Some(-1),
        },
    ];

    for case in cases {
        let result = ResourceListQuery::try_new_borrowed(
            Some("app in (api,worker)"),
            Some("metadata.name!=old"),
            case.limit,
            case.continue_token.clone(),
            ResourceVersionMatch::NotOlderThan(17),
        );
        if let Some(limit) = case.invalid_limit {
            assert_eq!(
                result.unwrap_err(),
                ResourceReadError::InvalidLimit { limit },
                "{}",
                case.name
            );
            continue;
        }

        let query = result.unwrap_or_else(|error| panic!("{}: {error}", case.name));
        assert_eq!(
            query.label_selector(),
            Some("app in (api,worker)"),
            "{}",
            case.name
        );
        assert_eq!(
            query.field_selector(),
            Some("metadata.name!=old"),
            "{}",
            case.name
        );
        assert_eq!(query.limit(), case.expected_limit, "{}", case.name);
        assert_eq!(
            query.continuation().is_some(),
            case.expected_continue,
            "{}",
            case.name
        );
        assert_eq!(
            query.resource_version_match(),
            ResourceVersionMatch::NotOlderThan(17)
        );
    }
}

#[test]
fn all_namespace_continuation_is_composite_and_pins_one_snapshot() {
    let snapshot = ResourceListSnapshot::try_new(WatchReplayPosition {
        resource_version: 77,
        event_id: 101,
        resource_version_filter_through_event_id: 0,
    })
    .unwrap();
    let continuation = ResourceContinuation::new(
        ResourceCollectionKey::new(Some("tenant-b"), "same-name"),
        snapshot,
    );
    assert_eq!(continuation.after().namespace(), Some("tenant-b"));
    assert_eq!(continuation.after().name(), "same-name");
    assert_eq!(continuation.snapshot(), snapshot);

    let request = ResourceListRequest::new(
        "v1",
        "ConfigMap",
        ResourceCollectionScope::AllNamespaces,
        ResourceListQuery::try_new(
            None,
            None,
            Some(i64::MAX),
            Some(continuation),
            ResourceVersionMatch::Exact(77),
        )
        .unwrap(),
    );
    assert_eq!(request.scope(), &ResourceCollectionScope::AllNamespaces);
    assert_eq!(request.query().limit(), Some(i64::MAX));

    assert!(matches!(
        ResourceListQuery::try_new(
            None,
            None,
            Some(1),
            Some(ResourceContinuation::new(
                ResourceCollectionKey::new(Some("tenant-b"), "same-name"),
                snapshot,
            )),
            ResourceVersionMatch::NotOlderThan(78),
        ),
        Err(ResourceReadError::InvalidContinuation { .. })
    ));
}

#[test]
fn list_snapshot_rejects_inexact_or_negative_positions() {
    for position in [
        WatchReplayPosition {
            resource_version: -1,
            event_id: 0,
            resource_version_filter_through_event_id: 0,
        },
        WatchReplayPosition {
            resource_version: 1,
            event_id: -1,
            resource_version_filter_through_event_id: 0,
        },
        WatchReplayPosition {
            resource_version: 1,
            event_id: 2,
            resource_version_filter_through_event_id: 1,
        },
    ] {
        assert!(matches!(
            ResourceListSnapshot::try_new(position),
            Err(ResourceReadError::CorruptData { .. })
        ));
    }
}

#[test]
fn list_snapshot_accepts_a_valid_scalar_rv_handoff_position() {
    let position = WatchReplayPosition::from_resource_version_through_event_id(17, 29);
    let snapshot = ResourceListSnapshot::try_new(position)
        .expect("a scalar resourceVersion handoff is an exact composite LIST boundary");

    assert_eq!(snapshot.position(), position);
}

#[test]
fn resource_read_errors_preserve_kubernetes_semantics() {
    let errors = [
        ResourceReadError::InvalidRequest {
            message: "bad request".into(),
        },
        ResourceReadError::InvalidSelector {
            message: "bad selector".into(),
        },
        ResourceReadError::InvalidContinuation {
            message: "bad cursor".into(),
        },
        ResourceReadError::Expired {
            requested: 9,
            oldest_available: 12,
        },
        ResourceReadError::Conflict {
            message: "rv conflict".into(),
        },
        ResourceReadError::UnsupportedMode {
            message: "unsupported".into(),
        },
        ResourceReadError::CorruptData {
            message: "corrupt".into(),
        },
        ResourceReadError::Retryable {
            message: "retry".into(),
        },
        ResourceReadError::Timeout,
        ResourceReadError::Cancelled,
    ];
    assert_eq!(errors.len(), 10);
    let statuses = errors
        .iter()
        .map(ResourceReadError::status)
        .collect::<Vec<_>>();
    assert_eq!(
        statuses
            .iter()
            .map(|status| (status.code, status.reason))
            .collect::<Vec<_>>(),
        vec![
            (400, "BadRequest"),
            (400, "BadRequest"),
            (400, "BadRequest"),
            (410, "Expired"),
            (409, "Conflict"),
            (501, "NotImplemented"),
            (500, "InternalError"),
            (503, "ServiceUnavailable"),
            (504, "Timeout"),
            (499, "Cancelled"),
        ]
    );
    assert!(statuses[7].retryable && statuses[8].retryable);
}
