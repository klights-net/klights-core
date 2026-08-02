#[cfg(test)]
use crate::api::AppError;
pub use k8s_native_service::generic_command::{CreateUpdateQuery, DeleteCollectionQuery};
#[cfg(test)]
pub use k8s_native_service::generic_read::{
    CONTINUE_TOKEN_TTL_SECS, ContinueResourceVersion, ContinueTokenData, ListResourceVersionMatch,
    encode_continue_token_at, encode_inconsistent_continue_token,
    resolve_list_response_resource_version,
};
pub use k8s_native_service::generic_read::{
    ListQuery, ListResourceVersionFuture, ListResourceVersionPort, ListSnapshotResolution,
    ListSnapshotResult, NamespaceListFuture, NamespaceListPage, NamespaceListPort,
    NamespaceListRequest, NamespaceListSnapshot, ResolvedListPage,
    encode_response_continue_token_at, process_continue_token_at, resolve_list_page,
};

impl ListSnapshotResult<klights_leader_api::ResourceListResult>
    for crate::api::custom_resource_ports::CustomResourceListSnapshot
{
    fn into_list_snapshot_resolution(
        self,
    ) -> ListSnapshotResolution<klights_leader_api::ResourceListResult> {
        match self {
            Self::List(list) => ListSnapshotResolution::List(list),
            Self::Current => ListSnapshotResolution::Current,
            Self::Expired => ListSnapshotResolution::Expired,
        }
    }
}

#[cfg(test)]
pub fn encode_continue_token(last_name: &str, session_rv: i64) -> String {
    encode_continue_token_at(last_name, session_rv, 1_700_000_000)
}

#[cfg(test)]
pub fn encode_response_continue_token(
    last_name: &str,
    response_rv: i64,
    continue_resource_version: ContinueResourceVersion,
) -> String {
    encode_response_continue_token_at(
        last_name,
        response_rv,
        continue_resource_version,
        1_700_000_000,
    )
}

#[cfg(test)]
pub fn process_continue_token(
    raw: Option<String>,
) -> Result<(Option<String>, ContinueResourceVersion), AppError> {
    process_continue_token_at(raw, 1_700_000_000)
}

#[cfg(test)]
mod resolve_list_page_tests {
    use super::*;
    use crate::datastore::sqlite::Datastore;
    use k8s_native_service::generic_read::GenericReadSnapshot;
    use klights_leader_api::ResourceListResult;

    impl ListResourceVersionPort for Datastore {
        fn advance_after(&self, minimum_resource_version: i64) -> ListResourceVersionFuture<'_> {
            Box::pin(async move {
                self.advance_resource_version_after(minimum_resource_version)
                    .await
            })
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
        let db = Datastore::new_in_memory().await.unwrap();
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
        let db = Datastore::new_in_memory().await.unwrap();
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
        let db = Datastore::new_in_memory().await.unwrap();
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
        let db = Datastore::new_in_memory().await.unwrap();
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
        let db = Datastore::new_in_memory().await.unwrap();
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
        let db = Datastore::new_in_memory().await.unwrap();
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
}
