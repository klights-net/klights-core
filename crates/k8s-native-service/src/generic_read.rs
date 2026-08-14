//! Generic Kubernetes GET/LIST orchestration and consistent-list semantics.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use axum::http::HeaderMap;
use axum::response::Response;
use base64::Engine as _;
use klights_cluster_core::Resource;
use klights_leader_api::{
    LeaderResourceQuery, ResourceGetRequest, ResourceListContinuationMode, ResourceListRequest,
    ResourceListResult, ResourceListScope, ResourceQueryConsistency, ResourceQueryError,
};
use klights_types::ResourceKey;
use serde::Deserialize;
use serde_json::Value;

use crate::{ApiState, AppError};

pub type GenericReadFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, AppError>> + Send + 'a>>;

#[derive(Clone, Deserialize)]
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

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContinueTokenData {
    pub n: String,
    #[serde(default)]
    pub rv: i64,
    pub ts: Option<i64>,
    /// The HTTP-owned envelope declares how its opaque `n` is to be routed;
    /// it never reveals or interprets the private root cursor bytes.
    pub continuation_mode: PublicListContinuationMode,
}

#[derive(Clone, Copy, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicListContinuationMode {
    Pinned,
    Recovery,
}

fn decode_continue_token_data(raw: &str) -> Option<ContinueTokenData> {
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(raw)
        .ok()?;
    serde_json::from_slice(&decoded).ok()
}

/// Decode only the native HTTP envelope. `n` remains opaque and is forwarded
/// byte-for-byte to leader RPC/root, whose private codec owns its meaning.
pub fn process_generic_list_continue_token(
    raw: Option<String>,
) -> Result<(Option<String>, ResourceListContinuationMode), AppError> {
    let Some(raw) = raw.filter(|token| !token.is_empty()) else {
        return Ok((None, ResourceListContinuationMode::Initial));
    };
    if let Some(data) = decode_continue_token_data(&raw) {
        if data.n.is_empty() {
            return Err(AppError::BadRequest(
                "Invalid value: continue token has an empty opaque cursor".to_string(),
            ));
        }
        let mode = match data.continuation_mode {
            PublicListContinuationMode::Pinned => ResourceListContinuationMode::Pinned,
            PublicListContinuationMode::Recovery => ResourceListContinuationMode::Recovery,
        };
        return Ok((Some(data.n), mode));
    }
    Err(AppError::BadRequest(
        "Invalid value: continue token is not a native LIST envelope".to_string(),
    ))
}

pub fn encode_generic_list_continue_token(
    inner: &str,
    response_rv: i64,
    now: i64,
    mode: PublicListContinuationMode,
) -> String {
    let data = ContinueTokenData {
        n: inner.to_string(),
        rv: response_rv,
        ts: Some(now),
        continuation_mode: mode,
    };
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(serde_json::to_vec(&data).unwrap_or_default())
}

pub(crate) fn generic_list_query_error(error: ResourceQueryError, now: i64) -> AppError {
    match error {
        ResourceQueryError::Expired {
            replacement_continue_token,
            ..
        } if replacement_continue_token
            .as_deref()
            .is_some_and(|token| !token.is_empty()) =>
        {
            AppError::ResourceExpired(Some(encode_generic_list_continue_token(
                replacement_continue_token
                    .as_deref()
                    .expect("guarded above"),
                0,
                now,
                PublicListContinuationMode::Recovery,
            )))
        }
        error => AppError::from(error),
    }
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
    scope: ResourceListScope,
    label_selector: Option<&str>,
    field_selector: Option<&str>,
    limit: Option<i64>,
    continue_token: Option<&str>,
) -> Result<ResourceListResult, AppError> {
    let request = ResourceListRequest::try_new(
        api_version,
        kind,
        scope,
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
    scope: ResourceListScope,
) -> Result<ResourceListResult, AppError> {
    list_resources(query, api_version, kind, scope, None, None, None, None).await
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
    let collection_scope = match (namespaced, namespace.clone()) {
        (true, Some(namespace)) => ResourceListScope::Namespace(namespace),
        (true, None) => ResourceListScope::AllNamespaces,
        (false, None) => ResourceListScope::Cluster,
        (false, Some(_)) => {
            return Err(AppError::BadRequest(
                "cluster-scoped LIST route must not carry a namespace".to_string(),
            ));
        }
    };
    let (continue_token, continuation_mode) =
        process_generic_list_continue_token(query.continue_token.clone())?;
    let resource_version_match = match query.resolve_resource_version_match(
        continuation_mode != ResourceListContinuationMode::Initial,
    )? {
        ListResourceVersionMatch::Any => klights_leader_api::ResourceListResourceVersionMatch::Any,
        ListResourceVersionMatch::NotOlderThan(rv) => {
            klights_leader_api::ResourceListResourceVersionMatch::NotOlderThan(rv)
        }
        ListResourceVersionMatch::Exact(rv) => {
            klights_leader_api::ResourceListResourceVersionMatch::Exact(rv)
        }
    };
    let resources = state.read_resources();
    let query_port = resources.resource_query();
    let live_request = ResourceListRequest::try_new_with_continuation_mode(
        api_version,
        kind,
        collection_scope,
        query.label_selector,
        query.field_selector,
        normalized_limit,
        continue_token,
        continuation_mode,
        ResourceQueryConsistency::LeaderFresh,
    )?
    .with_resource_version_match(resource_version_match)?;
    let list = query_port
        .list_resources(live_request)
        .await
        .map_err(|error| generic_list_query_error(error, operation_unix_timestamp))?;

    let (listed, response_rv, _, next_inner, remaining_item_count) = list.into_parts();
    let mut items = Vec::with_capacity(listed.len());
    for resource in listed {
        let mut value = resources
            .prepare_resource_for_read(api_version, kind, resource, false)
            .await?;
        inject_node_last_heartbeat(state.as_ref(), api_version, kind, &mut value).await;
        items.push(value);
    }
    let continue_token = next_inner.map(|inner| {
        encode_generic_list_continue_token(
            &inner,
            response_rv,
            operation_unix_timestamp,
            PublicListContinuationMode::Pinned,
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
    fn generic_list_outer_envelope_preserves_opaque_inner_and_recovery_mode() {
        let inner = "root-private/\u{1f680}?namespace=a/b&name=equal.name";
        let outer = encode_generic_list_continue_token(
            inner,
            41,
            100,
            PublicListContinuationMode::Recovery,
        );
        assert_eq!(
            process_generic_list_continue_token(Some(outer)).unwrap(),
            (
                Some(inner.to_string()),
                ResourceListContinuationMode::Recovery,
            )
        );
    }

    #[test]
    fn generic_list_expiry_wraps_the_typed_recovery_cursor() {
        let error = generic_list_query_error(
            ResourceQueryError::Expired {
                requested: 7,
                oldest_available: 11,
                replacement_continue_token: Some("private-recovery/\u{1f680}".to_string()),
            },
            100,
        );
        let AppError::ResourceExpired(outer) = error else {
            panic!("typed expiry must remain a Kubernetes 410")
        };
        let outer = outer.expect("typed recovery cursor must have an outer envelope");
        assert_eq!(
            process_generic_list_continue_token(Some(outer)).unwrap(),
            (
                Some("private-recovery/\u{1f680}".to_string()),
                ResourceListContinuationMode::Recovery,
            )
        );
    }

    #[test]
    fn generic_list_rejects_legacy_or_incomplete_public_envelopes() {
        use base64::Engine as _;
        for payload in [
            serde_json::json!({"n": "opaque", "rv": 1, "ts": 2, "session": true}),
            serde_json::json!({"n": "opaque", "rv": 1, "ts": 2}),
            serde_json::json!({"n": "opaque", "rv": 1, "ts": 2, "continuation_mode": null}),
        ] {
            let token = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(serde_json::to_vec(&payload).unwrap());
            assert!(matches!(
                process_generic_list_continue_token(Some(token)),
                Err(AppError::BadRequest(_))
            ));
        }
    }

    #[test]
    fn generic_list_rejects_empty_opaque_cursors_in_both_typed_modes() {
        for mode in [
            PublicListContinuationMode::Pinned,
            PublicListContinuationMode::Recovery,
        ] {
            let token = encode_generic_list_continue_token("", 1, 2, mode);
            assert!(matches!(
                process_generic_list_continue_token(Some(token)),
                Err(AppError::BadRequest(_))
            ));
        }
    }

    #[test]
    fn generic_list_expiry_omits_an_empty_recovery_cursor() {
        let error = generic_list_query_error(
            ResourceQueryError::Expired {
                requested: 7,
                oldest_available: 11,
                replacement_continue_token: Some(String::new()),
            },
            100,
        );
        assert!(matches!(error, AppError::ResourceExpired(None)));
    }
}
