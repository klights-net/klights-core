use crate::api::AppError;
use crate::datastore::{DatastoreBackend, ResourceList, SnapshotAtRv};
use serde::Deserialize;
use std::future::Future;

#[derive(Deserialize)]
pub struct ListQuery {
    #[serde(rename = "labelSelector")]
    pub label_selector: Option<String>,
    #[serde(rename = "fieldSelector")]
    pub field_selector: Option<String>,
    pub limit: Option<i64>,
    #[serde(rename = "continue")]
    pub continue_token: Option<String>,
    pub watch: Option<String>,
    #[serde(rename = "resourceVersion")]
    pub resource_version: Option<String>,
    #[serde(rename = "resourceVersionMatch")]
    pub resource_version_match: Option<String>,
    #[serde(rename = "allowWatchBookmarks")]
    pub allow_watch_bookmarks: Option<String>,
    #[serde(rename = "sendInitialEvents")]
    pub send_initial_events: Option<String>,
    #[serde(rename = "timeoutSeconds")]
    pub timeout_seconds: Option<u64>,
}

/// How a plain (non-watch) LIST should interpret `resourceVersion`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListResourceVersionMatch {
    /// No `resourceVersionMatch` and no `resourceVersion` (or `rv=0`): serve the
    /// freshest available state ("any").
    Any,
    /// Return state at least as fresh as the requested `resourceVersion`. This
    /// is also the legacy meaning of a bare `resourceVersion` without a match.
    NotOlderThan(i64),
    /// Return state exactly at the requested `resourceVersion`.
    Exact(i64),
}

impl ListQuery {
    /// Parse and validate `resourceVersion` + `resourceVersionMatch` for a plain
    /// LIST, returning the resolved semantics. Mirrors upstream apimachinery
    /// validation (see `k8s.io/apimachinery/.../validation`):
    ///
    /// * `resourceVersionMatch` must be empty, `Exact`, or `NotOlderThan`.
    /// * `resourceVersionMatch` is forbidden together with `continue`.
    /// * `resourceVersionMatch` requires `resourceVersion` to be set.
    /// * `resourceVersionMatch=Exact` requires a non-zero `resourceVersion`.
    /// * `resourceVersion`, if present, must be a non-negative integer.
    pub fn resolve_resource_version_match(
        &self,
        has_continue: bool,
    ) -> Result<ListResourceVersionMatch, AppError> {
        let rv_match = self
            .resource_version_match
            .as_deref()
            .filter(|s| !s.is_empty());

        let parsed_rv: Option<i64> = match self.resource_version.as_deref() {
            None | Some("") => None,
            Some(raw) => Some(raw.parse::<i64>().map_err(|_| {
                AppError::BadRequest(format!(
                    "Invalid value: \"{raw}\": resourceVersion: must be a non-negative integer"
                ))
            })?),
        };
        if let Some(rv) = parsed_rv
            && rv < 0
        {
            return Err(AppError::BadRequest(format!(
                "Invalid value: \"{rv}\": resourceVersion: must be a non-negative integer"
            )));
        }

        let Some(rv_match) = rv_match else {
            // Legacy: a bare resourceVersion means "not older than".
            return Ok(match parsed_rv {
                Some(rv) if rv > 0 => ListResourceVersionMatch::NotOlderThan(rv),
                _ => ListResourceVersionMatch::Any,
            });
        };

        if has_continue {
            return Err(AppError::BadRequest(
                "Invalid value: resourceVersionMatch is forbidden when continue is provided"
                    .to_string(),
            ));
        }
        if parsed_rv.is_none() {
            return Err(AppError::BadRequest(
                "Invalid value: resourceVersionMatch is forbidden unless resourceVersion is provided"
                    .to_string(),
            ));
        }
        match rv_match {
            "NotOlderThan" => Ok(match parsed_rv {
                Some(rv) if rv > 0 => ListResourceVersionMatch::NotOlderThan(rv),
                _ => ListResourceVersionMatch::Any,
            }),
            "Exact" => match parsed_rv {
                Some(rv) if rv > 0 => Ok(ListResourceVersionMatch::Exact(rv)),
                _ => Err(AppError::BadRequest(
                    "Invalid value: resourceVersionMatch \"Exact\" is forbidden unless a non-zero resourceVersion is provided"
                        .to_string(),
                )),
            },
            other => Err(AppError::BadRequest(format!(
                "Unsupported value: \"{other}\": supported values: \"Exact\", \"NotOlderThan\""
            ))),
        }
    }

    pub fn normalized_limit(&self) -> Result<Option<i64>, AppError> {
        match self.limit {
            None | Some(0) => Ok(None),
            Some(limit) if limit > 0 => Ok(Some(limit)),
            Some(limit) => Err(AppError::BadRequest(format!(
                "Invalid list limit {limit}: limit must be greater than or equal to 0"
            ))),
        }
    }
}

#[derive(Deserialize)]
pub struct CreateUpdateQuery {
    #[serde(rename = "dryRun")]
    pub dry_run: Option<String>,
    #[serde(rename = "fieldManager")]
    pub field_manager: Option<String>,
    #[serde(rename = "fieldValidation")]
    pub field_validation: Option<String>,
    /// Server-side apply: take ownership of conflicting fields instead of
    /// returning 409. Accepts `?force=true`.
    pub force: Option<bool>,
    #[serde(rename = "orphanDependents")]
    pub orphan_dependents: Option<bool>,
    #[serde(rename = "propagationPolicy")]
    pub propagation_policy: Option<String>,
    #[serde(rename = "gracePeriodSeconds")]
    pub grace_period_seconds: Option<i64>,
}

#[derive(Deserialize)]
pub struct DeleteCollectionQuery {
    #[serde(rename = "labelSelector")]
    pub label_selector: Option<String>,
    #[serde(rename = "fieldSelector")]
    pub field_selector: Option<String>,
}

pub const CONTINUE_TOKEN_TTL_SECS: i64 = 60;

#[derive(serde::Serialize, serde::Deserialize)]
pub struct ContinueTokenData {
    pub n: String,
    #[serde(default)]
    pub rv: i64,
    pub ts: Option<i64>,
    #[serde(default)]
    pub session: bool,
}

impl ContinueTokenData {
    fn is_inconsistent(&self) -> bool {
        self.ts.is_none()
    }

    fn is_expired(&self) -> bool {
        if let Some(ts) = self.ts {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64;
            now - ts > CONTINUE_TOKEN_TTL_SECS
        } else {
            false
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContinueResourceVersion {
    Current,
    Session(i64),
    Inconsistent { expired_rv: Option<i64> },
    InconsistentSession(i64),
}

fn decode_continue_token_data(raw: &str) -> Option<ContinueTokenData> {
    use base64::Engine as _;
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(raw)
        .ok()?;
    serde_json::from_slice(&decoded).ok()
}

pub fn encode_continue_token(last_name: &str, session_rv: i64) -> String {
    use base64::Engine as _;
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let data = ContinueTokenData {
        n: last_name.to_string(),
        rv: session_rv,
        ts: Some(ts),
        session: false,
    };
    let json = serde_json::to_vec(&data).unwrap_or_default();
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(json)
}

pub fn encode_inconsistent_continue_token(last_name: &str, expired_rv: i64) -> String {
    use base64::Engine as _;
    let data = ContinueTokenData {
        n: last_name.to_string(),
        rv: expired_rv,
        ts: None,
        session: false,
    };
    let json = serde_json::to_vec(&data).unwrap_or_default();
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(json)
}

pub fn encode_inconsistent_session_continue_token(last_name: &str, session_rv: i64) -> String {
    use base64::Engine as _;
    let data = ContinueTokenData {
        n: last_name.to_string(),
        rv: session_rv,
        ts: None,
        session: true,
    };
    let json = serde_json::to_vec(&data).unwrap_or_default();
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(json)
}

pub fn encode_response_continue_token(
    last_name: &str,
    response_rv: i64,
    continue_resource_version: ContinueResourceVersion,
) -> String {
    match continue_resource_version {
        ContinueResourceVersion::Inconsistent { .. }
        | ContinueResourceVersion::InconsistentSession(_) => {
            encode_inconsistent_session_continue_token(last_name, response_rv)
        }
        ContinueResourceVersion::Current | ContinueResourceVersion::Session(_) => {
            encode_continue_token(last_name, response_rv)
        }
    }
}

pub fn process_continue_token(
    raw: Option<String>,
) -> Result<(Option<String>, ContinueResourceVersion), AppError> {
    let raw = match raw {
        None => return Ok((None, ContinueResourceVersion::Current)),
        Some(s) if s.is_empty() => return Ok((None, ContinueResourceVersion::Current)),
        Some(s) => s,
    };

    if let Some(data) = decode_continue_token_data(&raw) {
        if !data.is_inconsistent() && data.is_expired() {
            let inconsistent = encode_inconsistent_continue_token(&data.n, data.rv);
            return Err(AppError::ResourceExpired(inconsistent));
        }
        if data.is_inconsistent() {
            if data.session && data.rv > 0 {
                return Ok((
                    Some(data.n),
                    ContinueResourceVersion::InconsistentSession(data.rv),
                ));
            }
            let expired_rv = if data.rv > 0 { Some(data.rv) } else { None };
            return Ok((
                Some(data.n),
                ContinueResourceVersion::Inconsistent { expired_rv },
            ));
        }
        let resource_version = if data.rv > 0 {
            ContinueResourceVersion::Session(data.rv)
        } else {
            ContinueResourceVersion::Current
        };
        return Ok((Some(data.n), resource_version));
    }

    Ok((Some(raw), ContinueResourceVersion::Current))
}

#[cfg(test)]
mod list_rv_match_tests {
    use super::*;

    fn q(rv: Option<&str>, rv_match: Option<&str>) -> ListQuery {
        ListQuery {
            label_selector: None,
            field_selector: None,
            limit: None,
            continue_token: None,
            watch: None,
            resource_version: rv.map(str::to_string),
            resource_version_match: rv_match.map(str::to_string),
            allow_watch_bookmarks: None,
            send_initial_events: None,
            timeout_seconds: None,
        }
    }

    #[test]
    fn unset_is_any() {
        assert_eq!(
            q(None, None).resolve_resource_version_match(false).unwrap(),
            ListResourceVersionMatch::Any
        );
        assert_eq!(
            q(Some("0"), None)
                .resolve_resource_version_match(false)
                .unwrap(),
            ListResourceVersionMatch::Any
        );
    }

    #[test]
    fn bare_rv_is_not_older_than() {
        assert_eq!(
            q(Some("42"), None)
                .resolve_resource_version_match(false)
                .unwrap(),
            ListResourceVersionMatch::NotOlderThan(42)
        );
    }

    #[test]
    fn explicit_not_older_than_and_exact() {
        assert_eq!(
            q(Some("7"), Some("NotOlderThan"))
                .resolve_resource_version_match(false)
                .unwrap(),
            ListResourceVersionMatch::NotOlderThan(7)
        );
        assert_eq!(
            q(Some("7"), Some("Exact"))
                .resolve_resource_version_match(false)
                .unwrap(),
            ListResourceVersionMatch::Exact(7)
        );
    }

    #[test]
    fn unsupported_match_value_is_400() {
        let err = q(Some("1"), Some("Bogus"))
            .resolve_resource_version_match(false)
            .unwrap_err();
        assert!(matches!(err, AppError::BadRequest(_)));
    }

    #[test]
    fn match_requires_resource_version() {
        assert!(matches!(
            q(None, Some("NotOlderThan")).resolve_resource_version_match(false),
            Err(AppError::BadRequest(_))
        ));
    }

    #[test]
    fn exact_forbids_zero_rv() {
        assert!(matches!(
            q(Some("0"), Some("Exact")).resolve_resource_version_match(false),
            Err(AppError::BadRequest(_))
        ));
    }

    #[test]
    fn match_forbidden_with_continue() {
        assert!(matches!(
            q(Some("5"), Some("NotOlderThan")).resolve_resource_version_match(true),
            Err(AppError::BadRequest(_))
        ));
    }

    #[test]
    fn invalid_rv_string_is_400() {
        assert!(matches!(
            q(Some("abc"), None).resolve_resource_version_match(false),
            Err(AppError::BadRequest(_))
        ));
    }
}

#[cfg(test)]
mod resolve_list_page_tests {
    use super::*;
    use crate::datastore::sqlite::Datastore;

    fn empty_list(rv: i64, continue_token: Option<&str>) -> ResourceList {
        ResourceList {
            items: Vec::new(),
            resource_version: rv,
            continue_token: continue_token.map(str::to_string),
            remaining_item_count: continue_token.map(|_| 1),
        }
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
                Ok(SnapshotAtRv::List(empty_list(7, None)))
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
            |_srv| async { Ok(SnapshotAtRv::Expired) },
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
                Ok(SnapshotAtRv::List(empty_list(42, Some("z"))))
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
            |_srv| async { Ok(SnapshotAtRv::Expired) },
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
            |_srv| async { Ok(SnapshotAtRv::Current) },
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
            |_srv| async { panic!("NotOlderThan must not pin a snapshot") },
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

pub async fn resolve_list_response_resource_version(
    db: &dyn DatastoreBackend,
    continue_resource_version: ContinueResourceVersion,
    current_resource_version: i64,
) -> Result<i64, AppError> {
    match continue_resource_version {
        ContinueResourceVersion::Current => Ok(current_resource_version),
        ContinueResourceVersion::Session(rv) => Ok(rv),
        ContinueResourceVersion::Inconsistent { expired_rv } => {
            let min_rv = expired_rv.unwrap_or(current_resource_version);
            db.advance_resource_version_after(min_rv)
                .await
                .map_err(AppError::from)
        }
        ContinueResourceVersion::InconsistentSession(rv) => Ok(rv),
    }
}

/// The consistent page a plain (non-watch) LIST resolved to, plus the metadata
/// every list handler needs to render its response identically.
#[derive(Debug)]
pub struct ResolvedListPage {
    /// The page of resources to serve — from a pinned historical snapshot when
    /// the read is consistent, otherwise from the live store.
    pub list: ResourceList,
    /// The `metadata.resourceVersion` to report (already adjusted for
    /// `resourceVersionMatch` and continuation-session pinning).
    pub response_rv: i64,
    /// The continuation mode to encode into the next page's `continue` token.
    /// May have been downgraded to inconsistent if the snapshot window expired.
    pub continue_resource_version: ContinueResourceVersion,
}

/// Shared consistent-snapshot selection for plain (non-watch) LISTs.
///
/// This centralizes the consistency logic that previously lived only in the
/// generated list handler (`generated_handlers::inners::list_inner`). Every
/// list handler — generated, Pods, Namespaces, and custom resources — must run
/// pages 2+ through this so the page body and the reported `resourceVersion`
/// come from the same consistency mode.
///
/// A historical snapshot is pinned when the read references an older
/// `resourceVersion`: an explicit `resourceVersionMatch=Exact`, or a paginated
/// continuation carrying its session rv (so every page reflects one consistent
/// view). `NotOlderThan`/`Any` are served live (a single consistent store is
/// always at least as fresh as any past rv).
///
/// When the pinned snapshot can no longer be reconstructed
/// ([`SnapshotAtRv::Expired`]): an explicit `Exact` answers `410 Gone`, while a
/// still-fresh paginated continuation that merely outran the retained
/// watch-event window continues from the last key against the live state and
/// keeps subsequent page tokens marked inconsistent.
///
/// `snapshot_fetch` is invoked only when a pin is required; callers without a
/// real reconstruction (e.g. conversion-backed CRDs) may return
/// [`SnapshotAtRv::Expired`] to opt into the inconsistent-continuation
/// fallback. `live_fetch` is invoked only when no pinned snapshot is served.
pub async fn resolve_list_page<SFut, LFut>(
    db: &dyn DatastoreBackend,
    rv_match: ListResourceVersionMatch,
    mut continue_resource_version: ContinueResourceVersion,
    snapshot_fetch: impl FnOnce(i64) -> SFut,
    live_fetch: impl FnOnce() -> LFut,
) -> Result<ResolvedListPage, AppError>
where
    SFut: Future<Output = Result<SnapshotAtRv, AppError>>,
    LFut: Future<Output = Result<ResourceList, AppError>>,
{
    let snapshot_rv = match rv_match {
        ListResourceVersionMatch::Exact(rv) => Some(rv),
        _ => match continue_resource_version {
            ContinueResourceVersion::Session(rv) => Some(rv),
            _ => None,
        },
    };

    let snapshot_list = if let Some(srv) = snapshot_rv {
        match snapshot_fetch(srv).await? {
            SnapshotAtRv::List(list) => Some(list),
            // rv is at/after current state — fall through to the live list.
            SnapshotAtRv::Current => None,
            SnapshotAtRv::Expired => match rv_match {
                ListResourceVersionMatch::Exact(rv) => {
                    return Err(AppError::expired(format!(
                        "too old resource version: {rv} (the requested resourceVersion is older than the server's retained history)"
                    )));
                }
                // A still-fresh paginated continuation can outlive the retained
                // watch_events window under unrelated API churn. Continue from
                // the last key against current state and keep subsequent page
                // tokens marked inconsistent.
                _ => {
                    continue_resource_version = ContinueResourceVersion::InconsistentSession(srv);
                    None
                }
            },
        }
    } else {
        None
    };

    let list = match snapshot_list {
        Some(list) => list,
        None => live_fetch().await?,
    };

    let mut response_rv = resolve_list_response_resource_version(
        db,
        continue_resource_version,
        list.resource_version,
    )
    .await?;
    // resourceVersionMatch handling. Exact pins the reported list rv to the
    // requested version; NotOlderThan guarantees the response is at least that
    // fresh (always true here — a single consistent store serves current state).
    match rv_match {
        ListResourceVersionMatch::Exact(rv) => response_rv = rv,
        ListResourceVersionMatch::NotOlderThan(rv) => response_rv = response_rv.max(rv),
        ListResourceVersionMatch::Any => {}
    }

    Ok(ResolvedListPage {
        list,
        response_rv,
        continue_resource_version,
    })
}
