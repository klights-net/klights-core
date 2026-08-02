use super::*;
use crate::AppError;
use crate::generic_read::{
    ContinueResourceVersion, GenericReadSnapshot, ListQuery, ListResourceVersionFuture,
    ListResourceVersionMatch, ListResourceVersionPort, resolve_list_response_resource_version,
};
use klights_leader_api::ResourceListResult;

struct Advancer;

impl ListResourceVersionPort for Advancer {
    fn advance_after(&self, minimum_resource_version: i64) -> ListResourceVersionFuture<'_> {
        Box::pin(async move { Ok(minimum_resource_version) })
    }
}

fn empty_list(rv: i64, continue_token: Option<&str>) -> ResourceListResult {
    ResourceListResult::try_new(
        Vec::new(),
        rv,
        None,
        continue_token.map(str::to_string),
        continue_token.map(|_| 1),
    )
    .unwrap()
}

#[tokio::test]
async fn exact_serves_snapshot_and_pins_response_rv() {
    let db = Advancer;
    let page = resolve_list_page(
        &db,
        ListResourceVersionMatch::Exact(7),
        ContinueResourceVersion::Current,
        |srv| async move {
            assert_eq!(srv, 7);
            Ok(GenericReadSnapshot::List(empty_list(7, None)))
        },
        || async { panic!("live_fetch must not run when a snapshot is served") },
    )
    .await
    .unwrap();
    assert_eq!(page.response_rv, 7);
    assert_eq!(
        page.continue_resource_version,
        ContinueResourceVersion::Current
    );
}

#[tokio::test]
async fn exact_against_expired_window_is_410() {
    let db = Advancer;
    let err = resolve_list_page(
        &db,
        ListResourceVersionMatch::Exact(3),
        ContinueResourceVersion::Current,
        |_srv| async { Ok(GenericReadSnapshot::Expired) },
        || async { panic!("live_fetch must not run") },
    )
    .await
    .unwrap_err();
    assert!(matches!(
        err,
        AppError::Status {
            code: axum::http::StatusCode::GONE,
            reason: "Expired",
            ..
        }
    ));
}

#[tokio::test]
async fn session_continuation_uses_snapshot() {
    let db = Advancer;
    let page = resolve_list_page(
        &db,
        ListResourceVersionMatch::Any,
        ContinueResourceVersion::Session(42),
        |srv| async move {
            assert_eq!(srv, 42);
            Ok(GenericReadSnapshot::List(empty_list(42, Some("z"))))
        },
        || async { panic!("live_fetch must not run when a snapshot is served") },
    )
    .await
    .unwrap();
    assert_eq!(page.response_rv, 42);
    assert_eq!(
        page.continue_resource_version,
        ContinueResourceVersion::Session(42)
    );
}

#[tokio::test]
async fn session_continuation_after_compaction_downgrades_to_inconsistent() {
    let db = Advancer;
    let page = resolve_list_page(
        &db,
        ListResourceVersionMatch::Any,
        ContinueResourceVersion::Session(42),
        |_srv| async { Ok(GenericReadSnapshot::Expired) },
        || async { Ok(empty_list(99, Some("z"))) },
    )
    .await
    .unwrap();
    // Downgraded continuation pins the original session rv and is reported
    // as inconsistent so subsequent page tokens stay inconsistent.
    assert_eq!(page.response_rv, 42);
    assert_eq!(
        page.continue_resource_version,
        ContinueResourceVersion::InconsistentSession(42)
    );
}

#[tokio::test]
async fn current_snapshot_falls_through_to_live() {
    let db = Advancer;
    let page = resolve_list_page(
        &db,
        ListResourceVersionMatch::Exact(5),
        ContinueResourceVersion::Current,
        |_srv| async { Ok(GenericReadSnapshot::Current) },
        || async { Ok(empty_list(5, None)) },
    )
    .await
    .unwrap();
    // Exact still pins the reported rv even when served live.
    assert_eq!(page.response_rv, 5);
}

#[tokio::test]
async fn not_older_than_is_served_live_and_floored() {
    let db = Advancer;
    let page = resolve_list_page(
        &db,
        ListResourceVersionMatch::NotOlderThan(100),
        ContinueResourceVersion::Current,
        |_srv| async {
            Err::<GenericReadSnapshot, AppError>(AppError::Internal(
                "NotOlderThan unexpectedly pinned a snapshot".to_string(),
            ))
        },
        || async { Ok(empty_list(50, None)) },
    )
    .await
    .unwrap();
    assert_eq!(
        page.response_rv, 100,
        "response rv must be floored to NotOlderThan"
    );
}

fn list_query_with_limit(limit: Option<i64>) -> ListQuery {
    ListQuery {
        label_selector: None,
        field_selector: None,
        limit,
        continue_token: None,
        watch: None,
        resource_version: None,
        resource_version_match: None,
        allow_watch_bookmarks: None,
        send_initial_events: None,
        timeout_seconds: None,
    }
}

#[test]
fn test_list_query_limit_zero_normalizes_to_unbounded() {
    assert_eq!(
        list_query_with_limit(Some(0)).normalized_limit().unwrap(),
        None
    );
}

#[test]
fn test_list_query_negative_limit_returns_bad_request() {
    assert!(matches!(
        list_query_with_limit(Some(-1)).normalized_limit(),
        Err(AppError::BadRequest(message)) if message.contains("limit")
    ));
}

struct FreshAdvancer;

impl ListResourceVersionPort for FreshAdvancer {
    fn advance_after(&self, minimum_resource_version: i64) -> ListResourceVersionFuture<'_> {
        Box::pin(async move { Ok(minimum_resource_version + 1) })
    }
}

#[tokio::test]
async fn test_inconsistent_continue_token_uses_fresh_resource_version_without_writes() {
    let response_rv = resolve_list_response_resource_version(
        &FreshAdvancer,
        ContinueResourceVersion::Inconsistent {
            expired_rv: Some(41),
        },
        41,
    )
    .await
    .unwrap();
    assert_eq!(response_rv, 42);

    let pinned = resolve_list_response_resource_version(
        &FreshAdvancer,
        ContinueResourceVersion::InconsistentSession(response_rv),
        response_rv,
    )
    .await
    .unwrap();
    assert_eq!(pinned, response_rv);
}
