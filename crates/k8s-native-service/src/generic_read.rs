//! Generic Kubernetes GET/LIST orchestration and consistent-list semantics.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use axum::http::HeaderMap;
use axum::response::Response;
use base64::Engine as _;
use klights_cluster_core::Resource;
use klights_leader_api::{
    LeaderResourceQuery, ResourceGetRequest, ResourceListRequest, ResourceListResult,
    ResourceQueryConsistency,
};
use klights_types::ResourceKey;
use serde::Deserialize;
use serde_json::Value;

use crate::{ApiState, AppError};

pub type ListResourceVersionFuture<'a> =
    Pin<Box<dyn Future<Output = anyhow::Result<i64>> + Send + 'a>>;

pub trait ListResourceVersionPort: Send + Sync {
    fn advance_after(&self, minimum_resource_version: i64) -> ListResourceVersionFuture<'_>;
}

pub trait ListPageMetadata {
    fn list_resource_version(&self) -> i64;
}

pub enum ListSnapshotResolution<Page> {
    List(Page),
    Current,
    Expired,
}

pub trait ListSnapshotResult<Page> {
    fn into_list_snapshot_resolution(self) -> ListSnapshotResolution<Page>;
}

#[derive(Clone, Debug)]
pub struct NamespaceListRequest {
    pub label_selector: Option<String>,
    pub field_selector: Option<String>,
    pub limit: Option<i64>,
    pub continue_token: Option<String>,
}

#[derive(Clone, Debug)]
pub struct NamespaceListPage {
    pub items: Vec<Resource>,
    pub resource_version: i64,
    pub continue_token: Option<String>,
    pub remaining_item_count: Option<i64>,
}

pub enum NamespaceListSnapshot {
    List(NamespaceListPage),
    Current,
    Expired,
}

pub type NamespaceListFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, AppError>> + Send + 'a>>;

pub trait NamespaceListPort: Send + Sync {
    fn list_namespaces(
        &self,
        request: NamespaceListRequest,
    ) -> NamespaceListFuture<'_, NamespaceListPage>;

    fn snapshot_namespaces(
        &self,
        request: NamespaceListRequest,
        snapshot_resource_version: i64,
    ) -> NamespaceListFuture<'_, NamespaceListSnapshot>;
}

impl ListPageMetadata for NamespaceListPage {
    fn list_resource_version(&self) -> i64 {
        self.resource_version
    }
}

impl ListSnapshotResult<NamespaceListPage> for NamespaceListSnapshot {
    fn into_list_snapshot_resolution(self) -> ListSnapshotResolution<NamespaceListPage> {
        match self {
            Self::List(list) => ListSnapshotResolution::List(list),
            Self::Current => ListSnapshotResolution::Current,
            Self::Expired => ListSnapshotResolution::Expired,
        }
    }
}

impl ListPageMetadata for klights_pod_api::PodListResult {
    fn list_resource_version(&self) -> i64 {
        self.resource_version()
    }
}

impl ListSnapshotResult<klights_pod_api::PodListResult>
    for klights_pod_api::PodSnapshotListOutcome
{
    fn into_list_snapshot_resolution(
        self,
    ) -> ListSnapshotResolution<klights_pod_api::PodListResult> {
        match self {
            Self::List(list) => ListSnapshotResolution::List(list),
            Self::Current => ListSnapshotResolution::Current,
            Self::Expired => ListSnapshotResolution::Expired,
        }
    }
}

impl ListPageMetadata for ResourceListResult {
    fn list_resource_version(&self) -> i64 {
        self.resource_version()
    }
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListResourceVersionMatch {
    Any,
    NotOlderThan(i64),
    Exact(i64),
}

impl ListQuery {
    pub fn resolve_resource_version_match(
        &self,
        has_continue: bool,
    ) -> Result<ListResourceVersionMatch, AppError> {
        let rv_match = self
            .resource_version_match
            .as_deref()
            .filter(|value| !value.is_empty());
        let parsed_rv = match self.resource_version.as_deref() {
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

    pub fn validate_send_initial_events_watch(&self) -> Result<(), AppError> {
        if self.send_initial_events.as_deref() != Some("true") {
            return Ok(());
        }
        match self
            .resource_version_match
            .as_deref()
            .filter(|value| !value.is_empty())
        {
            Some("NotOlderThan") => Ok(()),
            Some(other) => Err(AppError::BadRequest(format!(
                "Invalid value: resourceVersionMatch \"{other}\": sendInitialEvents=true requires resourceVersionMatch=NotOlderThan"
            ))),
            None => Err(AppError::BadRequest(
                "Invalid value: sendInitialEvents=true requires resourceVersionMatch=NotOlderThan"
                    .to_string(),
            )),
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

    fn is_expired_at(&self, now_unix_seconds: i64) -> bool {
        self.ts
            .is_some_and(|timestamp| now_unix_seconds - timestamp > CONTINUE_TOKEN_TTL_SECS)
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
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(raw)
        .ok()?;
    serde_json::from_slice(&decoded).ok()
}

pub fn encode_continue_token_at(last_name: &str, session_rv: i64, now: i64) -> String {
    let data = ContinueTokenData {
        n: last_name.to_string(),
        rv: session_rv,
        ts: Some(now),
        session: false,
    };
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(serde_json::to_vec(&data).unwrap_or_default())
}

pub fn encode_inconsistent_continue_token(last_name: &str, expired_rv: i64) -> String {
    let data = ContinueTokenData {
        n: last_name.to_string(),
        rv: expired_rv,
        ts: None,
        session: false,
    };
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(serde_json::to_vec(&data).unwrap_or_default())
}

pub fn encode_inconsistent_session_continue_token(last_name: &str, session_rv: i64) -> String {
    let data = ContinueTokenData {
        n: last_name.to_string(),
        rv: session_rv,
        ts: None,
        session: true,
    };
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(serde_json::to_vec(&data).unwrap_or_default())
}

pub fn encode_response_continue_token_at(
    last_name: &str,
    response_rv: i64,
    continuation: ContinueResourceVersion,
    now: i64,
) -> String {
    match continuation {
        ContinueResourceVersion::Inconsistent { .. }
        | ContinueResourceVersion::InconsistentSession(_) => {
            encode_inconsistent_session_continue_token(last_name, response_rv)
        }
        ContinueResourceVersion::Current | ContinueResourceVersion::Session(_) => {
            encode_continue_token_at(last_name, response_rv, now)
        }
    }
}

pub fn process_continue_token_at(
    raw: Option<String>,
    now: i64,
) -> Result<(Option<String>, ContinueResourceVersion), AppError> {
    let raw = match raw {
        None => return Ok((None, ContinueResourceVersion::Current)),
        Some(value) if value.is_empty() => {
            return Ok((None, ContinueResourceVersion::Current));
        }
        Some(value) => value,
    };
    if let Some(data) = decode_continue_token_data(&raw) {
        if !data.is_inconsistent() && data.is_expired_at(now) {
            return Err(AppError::ResourceExpired(
                encode_inconsistent_continue_token(&data.n, data.rv),
            ));
        }
        if data.is_inconsistent() {
            if data.session && data.rv > 0 {
                return Ok((
                    Some(data.n),
                    ContinueResourceVersion::InconsistentSession(data.rv),
                ));
            }
            let expired_rv = (data.rv > 0).then_some(data.rv);
            return Ok((
                Some(data.n),
                ContinueResourceVersion::Inconsistent { expired_rv },
            ));
        }
        return Ok((
            Some(data.n),
            if data.rv > 0 {
                ContinueResourceVersion::Session(data.rv)
            } else {
                ContinueResourceVersion::Current
            },
        ));
    }
    Ok((Some(raw), ContinueResourceVersion::Current))
}

pub async fn resolve_list_response_resource_version(
    resource_versions: &dyn ListResourceVersionPort,
    continuation: ContinueResourceVersion,
    current_resource_version: i64,
) -> Result<i64, AppError> {
    match continuation {
        ContinueResourceVersion::Current => Ok(current_resource_version),
        ContinueResourceVersion::Session(rv) | ContinueResourceVersion::InconsistentSession(rv) => {
            Ok(rv)
        }
        ContinueResourceVersion::Inconsistent { expired_rv } => resource_versions
            .advance_after(expired_rv.unwrap_or(current_resource_version))
            .await
            .map_err(|error| AppError::Internal(error.to_string())),
    }
}

#[derive(Debug)]
pub struct ResolvedListPage<Page> {
    pub list: Page,
    pub response_rv: i64,
    pub continue_resource_version: ContinueResourceVersion,
}

pub async fn resolve_list_page<Page, Snapshot, SFut, LFut>(
    resource_versions: &dyn ListResourceVersionPort,
    rv_match: ListResourceVersionMatch,
    mut continuation: ContinueResourceVersion,
    snapshot_fetch: impl FnOnce(i64) -> SFut,
    live_fetch: impl FnOnce() -> LFut,
) -> Result<ResolvedListPage<Page>, AppError>
where
    Page: ListPageMetadata,
    Snapshot: ListSnapshotResult<Page>,
    SFut: Future<Output = Result<Snapshot, AppError>>,
    LFut: Future<Output = Result<Page, AppError>>,
{
    let snapshot_rv = match rv_match {
        ListResourceVersionMatch::Exact(rv) => Some(rv),
        _ => match continuation {
            ContinueResourceVersion::Session(rv) => Some(rv),
            _ => None,
        },
    };
    let snapshot_list = if let Some(snapshot_rv) = snapshot_rv {
        match snapshot_fetch(snapshot_rv)
            .await?
            .into_list_snapshot_resolution()
        {
            ListSnapshotResolution::List(list) => Some(list),
            ListSnapshotResolution::Current => None,
            ListSnapshotResolution::Expired => match rv_match {
                ListResourceVersionMatch::Exact(rv) => {
                    return Err(AppError::expired(format!(
                        "too old resource version: {rv} (the requested resourceVersion is older than the server's retained history)"
                    )));
                }
                _ => {
                    continuation = ContinueResourceVersion::InconsistentSession(snapshot_rv);
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
        resource_versions,
        continuation,
        list.list_resource_version(),
    )
    .await?;
    match rv_match {
        ListResourceVersionMatch::Exact(rv) => response_rv = rv,
        ListResourceVersionMatch::NotOlderThan(rv) => response_rv = response_rv.max(rv),
        ListResourceVersionMatch::Any => {}
    }
    Ok(ResolvedListPage {
        list,
        response_rv,
        continue_resource_version: continuation,
    })
}

#[derive(Clone, Debug)]
pub struct GenericReadSnapshotRequest {
    pub api_version: String,
    pub kind: String,
    pub namespace: Option<String>,
    pub label_selector: Option<String>,
    pub field_selector: Option<String>,
    pub limit: Option<i64>,
    pub continue_token: Option<String>,
    pub resource_version: i64,
}

pub enum GenericReadSnapshot {
    Current,
    Expired,
    List(ResourceListResult),
}

impl ListSnapshotResult<ResourceListResult> for GenericReadSnapshot {
    fn into_list_snapshot_resolution(self) -> ListSnapshotResolution<ResourceListResult> {
        match self {
            Self::List(list) => ListSnapshotResolution::List(list),
            Self::Current => ListSnapshotResolution::Current,
            Self::Expired => ListSnapshotResolution::Expired,
        }
    }
}

pub type GenericReadFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, AppError>> + Send + 'a>>;

pub trait GenericReadSnapshotPort: Send + Sync {
    fn snapshot_resources_at_rv(
        &self,
        request: GenericReadSnapshotRequest,
    ) -> GenericReadFuture<'_, GenericReadSnapshot>;
}

pub struct GenericReadWatchRequest {
    pub api_version: &'static str,
    pub kind: &'static str,
    pub namespace: Option<String>,
    pub query: ListQuery,
    pub headers: HeaderMap,
    pub wall_clock: Arc<dyn klights_auth::clock::Clock>,
}

pub struct GenericListResponse {
    pub api_version: &'static str,
    pub kind: &'static str,
    pub list_kind: &'static str,
    pub items: Vec<Value>,
    pub response_rv: i64,
    pub continue_token: Option<String>,
    pub remaining_item_count: Option<i64>,
    pub headers: HeaderMap,
    pub operation_unix_timestamp_nanos: i128,
}

pub trait GenericReadResourceInputs: Send + Sync {
    fn resource_query(&self) -> &dyn LeaderResourceQuery;
    fn snapshot_port(&self) -> &dyn GenericReadSnapshotPort;
    fn resource_versions(&self) -> &dyn ListResourceVersionPort;
    fn prepare_resource_for_read(
        &self,
        api_version: &'static str,
        kind: &'static str,
        resource: Resource,
        is_get: bool,
    ) -> GenericReadFuture<'_, Value>;
    fn build_watch(&self, request: GenericReadWatchRequest) -> GenericReadFuture<'_, Response>;
    fn render_list(&self, response: GenericListResponse) -> Result<Response, AppError>;
    fn render_get(&self, value: Value, headers: HeaderMap) -> Response;
}

pub trait GenericReadControllerInputs: Send + Sync {
    fn observed_node_renew_time(&self, node_name: &str) -> GenericReadFuture<'_, Option<String>>;
}

pub trait GenericReadOperationalInputs: Send + Sync {
    fn operation_unix_timestamp_nanos(&self) -> i128;
    fn wall_clock(&self) -> Arc<dyn klights_auth::clock::Clock>;
    fn has_local_authority(&self) -> bool;
}

pub trait GenericReadState: Send + Sync {
    fn read_resources(&self) -> &dyn GenericReadResourceInputs;
    fn read_controllers(&self) -> &dyn GenericReadControllerInputs;
    fn read_operational(&self) -> &dyn GenericReadOperationalInputs;
}

impl<Auth, Resources, Discovery, Controllers, PodNode, Operational> GenericReadState
    for ApiState<Auth, Resources, Discovery, Controllers, PodNode, Operational>
where
    Auth: Send + Sync,
    Resources: GenericReadResourceInputs,
    Discovery: Send + Sync,
    Controllers: GenericReadControllerInputs,
    PodNode: Send + Sync,
    Operational: GenericReadOperationalInputs,
{
    fn read_resources(&self) -> &dyn GenericReadResourceInputs {
        self.resource_mutation()
    }

    fn read_controllers(&self) -> &dyn GenericReadControllerInputs {
        self.controller_reconcile()
    }

    fn read_operational(&self) -> &dyn GenericReadOperationalInputs {
        self.operational()
    }
}

pub struct GeneratedListInnerRequest {
    pub api_version: &'static str,
    pub kind: &'static str,
    pub list_kind: &'static str,
    pub namespace: Option<String>,
    pub namespaced: bool,
    pub query: ListQuery,
    pub headers: HeaderMap,
}

pub async fn get_resource(
    query: &dyn LeaderResourceQuery,
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
) -> Result<Option<Resource>, AppError> {
    let request = ResourceGetRequest::try_new(
        ResourceKey::new(api_version, kind, namespace.map(str::to_string), name),
        ResourceQueryConsistency::LeaderFresh,
    )?;
    query.get_resource(request).await.map_err(AppError::from)
}

#[allow(clippy::too_many_arguments)]
pub async fn list_resources(
    query: &dyn LeaderResourceQuery,
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    label_selector: Option<&str>,
    field_selector: Option<&str>,
    limit: Option<i64>,
    continue_token: Option<&str>,
) -> Result<ResourceListResult, AppError> {
    let request = ResourceListRequest::try_new(
        api_version,
        kind,
        namespace.map(str::to_string),
        label_selector.map(str::to_string),
        field_selector.map(str::to_string),
        limit,
        continue_token.map(str::to_string),
        ResourceQueryConsistency::LeaderFresh,
    )?;
    query.list_resources(request).await.map_err(AppError::from)
}

pub async fn list_all_resources(
    query: &dyn LeaderResourceQuery,
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
) -> Result<ResourceListResult, AppError> {
    list_resources(query, api_version, kind, namespace, None, None, None, None).await
}

pub async fn list_inner<S: GenericReadState + 'static>(
    state: Arc<S>,
    _identity: &klights_auth::AuthenticatedIdentity,
    request: GeneratedListInnerRequest,
) -> Result<Response, AppError> {
    let GeneratedListInnerRequest {
        api_version,
        kind,
        list_kind,
        namespace,
        namespaced,
        query,
        headers,
    } = request;
    validate_builtin_field_selector(
        api_version,
        kind,
        query.label_selector.as_deref(),
        query.field_selector.as_deref(),
        namespaced,
    )?;
    if query.watch.as_deref() == Some("true") {
        query.validate_send_initial_events_watch()?;
        return state
            .read_resources()
            .build_watch(GenericReadWatchRequest {
                api_version,
                kind,
                namespace,
                query,
                headers,
                wall_clock: state.read_operational().wall_clock(),
            })
            .await;
    }

    let operation_unix_timestamp_nanos = state.read_operational().operation_unix_timestamp_nanos();
    let operation_unix_timestamp =
        i64::try_from(operation_unix_timestamp_nanos.div_euclid(1_000_000_000)).map_err(|_| {
            AppError::Internal("operation time is outside the supported timestamp range".into())
        })?;
    let normalized_limit = query.normalized_limit()?;
    let has_continue = query
        .continue_token
        .as_deref()
        .is_some_and(|token| !token.is_empty());
    let rv_match = query.resolve_resource_version_match(has_continue)?;
    let (continue_name, continuation) =
        process_continue_token_at(query.continue_token, operation_unix_timestamp)?;

    let snapshot_request = GenericReadSnapshotRequest {
        api_version: api_version.to_string(),
        kind: kind.to_string(),
        namespace: namespace.clone(),
        label_selector: query.label_selector.clone(),
        field_selector: query.field_selector.clone(),
        limit: normalized_limit,
        continue_token: continue_name.clone(),
        resource_version: 0,
    };
    let resources = state.read_resources();
    let query_port = resources.resource_query();
    let snapshot_port = resources.snapshot_port();
    let resource_versions = resources.resource_versions();
    let live_request = ResourceListRequest::try_new(
        api_version,
        kind,
        namespace,
        query.label_selector,
        query.field_selector,
        normalized_limit,
        continue_name,
        ResourceQueryConsistency::LeaderFresh,
    )?;
    let ResolvedListPage {
        list,
        response_rv,
        continue_resource_version,
    } = resolve_list_page(
        resource_versions,
        rv_match,
        continuation,
        |snapshot_rv| {
            let mut request = snapshot_request;
            request.resource_version = snapshot_rv;
            snapshot_port.snapshot_resources_at_rv(request)
        },
        || async move {
            query_port
                .list_resources(live_request)
                .await
                .map_err(AppError::from)
        },
    )
    .await?;

    let (listed, _, _, next_name, remaining_item_count) = list.into_parts();
    let mut items = Vec::with_capacity(listed.len());
    for resource in listed {
        let mut value = resources
            .prepare_resource_for_read(api_version, kind, resource, false)
            .await?;
        inject_node_last_heartbeat(state.as_ref(), api_version, kind, &mut value).await;
        items.push(value);
    }
    let continue_token = next_name.map(|name| {
        encode_response_continue_token_at(
            &name,
            response_rv,
            continue_resource_version,
            operation_unix_timestamp,
        )
    });
    resources.render_list(GenericListResponse {
        api_version,
        kind,
        list_kind,
        items,
        response_rv,
        continue_token,
        remaining_item_count,
        headers,
        operation_unix_timestamp_nanos,
    })
}

pub async fn get_inner<S: GenericReadState + 'static>(
    state: Arc<S>,
    _identity: &klights_auth::AuthenticatedIdentity,
    api_version: &'static str,
    kind: &'static str,
    namespace: Option<&str>,
    name: &str,
    headers: HeaderMap,
) -> Result<Response, AppError> {
    let request = ResourceGetRequest::try_new(
        ResourceKey::new(api_version, kind, namespace.map(str::to_string), name),
        ResourceQueryConsistency::LeaderFresh,
    )?;
    let Some(resource) = state
        .read_resources()
        .resource_query()
        .get_resource(request)
        .await
        .map_err(AppError::from)?
    else {
        return Err(AppError::not_found(api_version, kind, name));
    };
    let mut value = state
        .read_resources()
        .prepare_resource_for_read(api_version, kind, resource, true)
        .await?;
    inject_node_last_heartbeat(state.as_ref(), api_version, kind, &mut value).await;
    Ok(state.read_resources().render_get(value, headers))
}

async fn inject_node_last_heartbeat<S: GenericReadState + ?Sized>(
    state: &S,
    api_version: &str,
    kind: &str,
    node: &mut Value,
) {
    if api_version != "v1" || kind != "Node" {
        return;
    }
    let node_name = node
        .pointer("/metadata/name")
        .and_then(Value::as_str)
        .filter(|name| !name.is_empty())
        .map(str::to_string);
    let Some(ready) = node
        .pointer_mut("/status/conditions")
        .and_then(Value::as_array_mut)
        .and_then(|conditions| {
            conditions
                .iter_mut()
                .find(|condition| condition.get("type").and_then(Value::as_str) == Some("Ready"))
        })
    else {
        return;
    };
    if let Some(object) = ready.as_object_mut() {
        object.remove("lastHeartbeatTime");
    }
    if !state.read_operational().has_local_authority() {
        return;
    }
    let Some(node_name) = node_name.as_deref() else {
        return;
    };
    if let Ok(Some(renew_time)) = state
        .read_controllers()
        .observed_node_renew_time(node_name)
        .await
    {
        ready["lastHeartbeatTime"] = serde_json::json!(renew_time);
    }
}

pub fn validate_builtin_field_selector(
    api_version: &str,
    kind: &str,
    label_selector: Option<&str>,
    field_selector: Option<&str>,
    namespaced: bool,
) -> Result<(), AppError> {
    let Some(selector) = field_selector
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(());
    };
    let parsed = klights_types::FieldSelector::parse(selector)
        .map_err(|error| AppError::BadRequest(error.to_string()))?;
    let mut supported = std::collections::HashSet::from(["metadata.name"]);
    if namespaced {
        supported.insert("metadata.namespace");
    }
    supported.extend(builtin_selectable_fields(api_version, kind));
    for requirement in parsed.requirements() {
        if !supported.contains(requirement.field()) {
            return Err(AppError::BadRequest(format!(
                "Unable to find \"{api_version}, Resource={kind}\" that match label selector \"{}\", field selector \"{selector}\": field label not supported: {}",
                label_selector.unwrap_or_default(),
                requirement.field()
            )));
        }
    }
    Ok(())
}

fn builtin_selectable_fields(api_version: &str, kind: &str) -> &'static [&'static str] {
    match (api_version, kind) {
        ("v1", "Pod") => &[
            "spec.nodeName",
            "status.phase",
            "spec.restartPolicy",
            "spec.schedulerName",
            "spec.serviceAccountName",
            "status.podIP",
        ],
        ("v1", "Namespace") => &["status.phase"],
        ("v1", "Node") => &["spec.unschedulable"],
        ("v1", "PersistentVolume") | ("v1", "PersistentVolumeClaim") => &["status.phase"],
        ("v1", "Secret") => &["type"],
        ("v1", "Event") | ("events.k8s.io/v1", "Event") => &[
            "reason",
            "type",
            "source",
            "reportingController",
            "reportingInstance",
            "involvedObject.kind",
            "involvedObject.uid",
            "involvedObject.name",
            "involvedObject.namespace",
        ],
        ("certificates.k8s.io/v1", "CertificateSigningRequest") => &["spec.signerName"],
        _ => &[],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query(rv: Option<&str>, rv_match: Option<&str>) -> ListQuery {
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
    fn resource_version_match_behavior_is_preserved() {
        assert_eq!(
            query(None, None)
                .resolve_resource_version_match(false)
                .unwrap(),
            ListResourceVersionMatch::Any
        );
        assert_eq!(
            query(Some("42"), None)
                .resolve_resource_version_match(false)
                .unwrap(),
            ListResourceVersionMatch::NotOlderThan(42)
        );
        assert_eq!(
            query(Some("7"), Some("Exact"))
                .resolve_resource_version_match(false)
                .unwrap(),
            ListResourceVersionMatch::Exact(7)
        );
        assert!(matches!(
            query(Some("0"), Some("Exact")).resolve_resource_version_match(false),
            Err(AppError::BadRequest(_))
        ));
    }

    #[test]
    fn continuation_tokens_preserve_session_and_inconsistent_modes() {
        let token = encode_continue_token_at("pod-b", 42, 100);
        assert_eq!(
            process_continue_token_at(Some(token), 101).unwrap(),
            (
                Some("pod-b".to_string()),
                ContinueResourceVersion::Session(42)
            )
        );
        let token = encode_inconsistent_session_continue_token("pod-c", 77);
        assert_eq!(
            process_continue_token_at(Some(token), 200).unwrap(),
            (
                Some("pod-c".to_string()),
                ContinueResourceVersion::InconsistentSession(77)
            )
        );
    }
}
