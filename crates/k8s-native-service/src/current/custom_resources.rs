use super::*;
use crate::generic_command::{
    CreateStrategy, PatchStrategy, UpdateStrategy, WriteResult, create_with_strategy,
    patch_with_strategy, update_with_strategy,
};
use crate::generic_read::ListResourceVersionMatch;
use async_trait::async_trait;
use axum::Extension;
use klights_auth::AuthenticatedIdentity;

// Custom-resource authorization is enforced by the global `authorize_request`
// middleware chokepoint in `crate::auth_http`. The CRD handlers still
// receive the authenticated identity because it is forwarded to APIService
// backends during aggregation proxying (see lookup_crd_or_proxy).

pub enum CrdLookup {
    Found(klights_leader_api::CrdResourceInfo),
    Proxied(Response),
}

pub struct CrdLookupRequest<'a> {
    pub group: &'a str,
    pub version: &'a str,
    pub plural: &'a str,
    pub method: Method,
    pub uri: &'a axum::http::Uri,
    pub headers: &'a HeaderMap,
}

impl<'a> CrdLookupRequest<'a> {
    pub fn new(
        group: &'a str,
        version: &'a str,
        plural: &'a str,
        method: Method,
        uri: &'a axum::http::Uri,
        headers: &'a HeaderMap,
    ) -> Self {
        Self {
            group,
            version,
            plural,
            method,
            uri,
            headers,
        }
    }
}

#[derive(Clone, Copy)]
struct CustomResourceType<'a> {
    info: &'a klights_leader_api::CrdResourceInfo,
    group: &'a str,
    version: &'a str,
    plural: &'a str,
}

impl<'a> CustomResourceType<'a> {
    fn new(
        info: &'a klights_leader_api::CrdResourceInfo,
        group: &'a str,
        version: &'a str,
        plural: &'a str,
    ) -> Self {
        Self {
            info,
            group,
            version,
            plural,
        }
    }

    fn api_version(&self) -> String {
        format!("{}/{}", self.group, self.version)
    }

    fn scoped(self, namespace: Option<&'a str>, is_cluster_scope: bool) -> CustomResourceScope<'a> {
        CustomResourceScope {
            resource_type: self,
            namespace,
            is_cluster_scope,
        }
    }

    fn named(
        self,
        namespace: Option<&'a str>,
        name: &'a str,
        is_cluster_scope: bool,
    ) -> CustomResourceName<'a> {
        CustomResourceName {
            scope: self.scoped(namespace, is_cluster_scope),
            name,
        }
    }
}

#[derive(Clone, Copy)]
struct CustomResourceScope<'a> {
    resource_type: CustomResourceType<'a>,
    namespace: Option<&'a str>,
    is_cluster_scope: bool,
}

#[derive(Clone, Copy)]
struct CustomResourceName<'a> {
    scope: CustomResourceScope<'a>,
    name: &'a str,
}

struct CustomResourceListRequest<'a> {
    scope: CustomResourceScope<'a>,
    query: &'a ListQuery,
    headers: &'a HeaderMap,
}

struct CustomResourceCollectionDeleteRequest<'a> {
    scope: CustomResourceScope<'a>,
    query: &'a DeleteCollectionQuery,
    body: Bytes,
    log_context: &'static str,
}

struct CustomResourceCreateRequest<'a> {
    scope: CustomResourceScope<'a>,
    query: &'a CreateUpdateQuery,
    body: Value,
    log_context: &'static str,
}

struct CustomResourceDeleteRequest<'a> {
    target: CustomResourceName<'a>,
    query: &'a CreateUpdateQuery,
    body: Bytes,
}

struct CustomResourceUpdateRequest<'a> {
    target: CustomResourceName<'a>,
    body: Value,
    log_context: &'static str,
}

struct CustomResourcePatchRequest<'a> {
    target: CustomResourceName<'a>,
    query: &'a CreateUpdateQuery,
    headers: &'a HeaderMap,
    body: Bytes,
}

pub async fn lookup_crd_or_proxy(
    state: &Arc<ApiState>,
    identity: &AuthenticatedIdentity,
    request: CrdLookupRequest<'_>,
    body_for_proxy: impl FnOnce() -> Result<Bytes, AppError>,
) -> Result<CrdLookup, AppError> {
    if let Some(info) = state
        .discovery()
        .crd_registry
        .get(request.group, request.version, request.plural)
        .await
    {
        return Ok(CrdLookup::Found(info));
    }
    let path_and_query = request
        .uri
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or_else(|| request.uri.path());
    let body = body_for_proxy()?;
    if let Some(resp) = proxy_apiservice_request(
        state,
        request.group,
        request.version,
        request.method,
        path_and_query,
        body,
        Some(request.headers),
        identity,
    )
    .await?
    {
        return Ok(CrdLookup::Proxied(resp));
    }
    Err(AppError::NotFound(format!(
        "resource {} not found",
        request.plural
    )))
}

pub async fn get_existing_custom_resource_for_write(
    state: &Arc<ApiState>,
    group: &str,
    version: &str,
    plural: &str,
    kind: &str,
    namespace: Option<String>,
    name: &str,
) -> Result<(Resource, String), AppError> {
    let requested_api_version = format!("{group}/{version}");
    if let Some(resource) = crate::current::resource_query_ports::get_resource(
        state.resource_mutation().resource_query.as_ref(),
        &requested_api_version,
        kind,
        namespace.as_deref(),
        name,
    )
    .await?
    {
        return Ok((resource, requested_api_version));
    }

    if let Some(conversion) = load_crd_conversion_config(
        state.resource_mutation().resource_query.as_ref(),
        group,
        plural,
    )
    .await?
    {
        for served_version in &conversion.served_versions {
            if served_version == version {
                continue;
            }
            let candidate_api_version = format!("{group}/{served_version}");
            if let Some(resource) = crate::current::resource_query_ports::get_resource(
                state.resource_mutation().resource_query.as_ref(),
                &candidate_api_version,
                kind,
                namespace.as_deref(),
                name,
            )
            .await?
            {
                return Ok((resource, candidate_api_version));
            }
        }
    }

    Err(AppError::NotFound(format!("{kind} not found")))
}

fn storage_api_version_for_request(
    group: &str,
    requested_version: &str,
    conversion: Option<&crate::current::crd_conversion::CrdConversionConfig>,
) -> String {
    conversion
        .map(|conversion| format!("{}/{}", group, conversion.storage_version))
        .unwrap_or_else(|| format!("{}/{}", group, requested_version))
}

fn crd_watch_versions(
    conversion: Option<&crate::current::crd_conversion::CrdConversionConfig>,
    requested_version: &str,
) -> Vec<String> {
    let mut versions = Vec::new();
    let mut push_unique = |version: &str| {
        if !versions.iter().any(|candidate| candidate == version) {
            versions.push(version.to_string());
        }
    };
    if let Some(conversion) = conversion {
        push_unique(&conversion.storage_version);
        push_unique(requested_version);
        for version in &conversion.served_versions {
            push_unique(version);
        }
    } else {
        push_unique(requested_version);
    }
    versions
}

fn merge_custom_resource_watch_baseline(
    resources: Vec<klights_cluster_core::Resource>,
) -> Vec<klights_cluster_core::Resource> {
    if resources.first().is_none_or(|first| {
        resources
            .iter()
            .all(|item| item.api_version == first.api_version)
    }) {
        return resources;
    }
    let mut merged: std::collections::HashMap<
        (Option<String>, String),
        klights_cluster_core::Resource,
    > = std::collections::HashMap::with_capacity(resources.len());
    for resource in resources {
        let key = (resource.namespace.clone(), resource.name.clone());
        match merged.get(&key) {
            Some(existing) if existing.resource_version >= resource.resource_version => {}
            _ => {
                merged.insert(key, resource);
            }
        }
    }
    let mut resources = merged.into_values().collect::<Vec<_>>();
    resources
        .sort_by(|left, right| (&left.namespace, &left.name).cmp(&(&right.namespace, &right.name)));
    resources
}

fn crd_watch_frame_response(
    stream_format: crate::current::watch_stream::WatchStreamFormat,
    frame: Vec<u8>,
) -> Response {
    let body = Body::from_stream(futures::stream::once(async move {
        Ok::<_, std::convert::Infallible>(frame)
    }));
    Response::builder()
        .header("Content-Type", stream_format.content_type())
        .header("Transfer-Encoding", "chunked")
        .body(body)
        .expect("CRD watch response headers are valid")
}

struct CrdWatchProjection {
    identity: Arc<dyn crate::ApiIdentityGenerator>,
    resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
    conversion: Option<crate::current::crd_conversion::CrdConversionConfig>,
    group: String,
    plural: String,
    requested_api_version: String,
}

impl crate::current::custom_resource_ports::CustomResourceProjection for CrdWatchProjection {
    fn project_resources(
        &self,
        resources: Vec<klights_cluster_core::Resource>,
    ) -> futures::future::BoxFuture<
        '_,
        Result<Vec<klights_cluster_core::Resource>, klights_leader_api::LeaderWatchError>,
    > {
        Box::pin(async move {
            let mut projected = Vec::new();
            for resource in merge_custom_resource_watch_baseline(resources) {
                let event = crate::current::custom_resource_ports::added_watch_event(resource);
                let event = convert_custom_resource_watch_event_to_requested_version(
                    self.identity.as_ref(),
                    self.resource_query.as_ref(),
                    self.conversion.as_ref(),
                    &self.group,
                    &self.plural,
                    &self.requested_api_version,
                    event,
                )
                .await
                .map_err(|error| {
                    klights_leader_api::LeaderWatchError::unavailable(format!("{error:?}"))
                })?;
                projected.push(klights_cluster_core::Resource::from_data_lossy(
                    event.object.clone(),
                ));
            }
            Ok(projected)
        })
    }
}

#[cfg(test)]
mod crd_watch_topic_tests {
    use super::*;

    #[test]
    fn conversion_crd_watch_versions_cover_storage_and_requested_versions() {
        let conversion = crate::current::crd_conversion::CrdConversionConfig {
            namespaced: true,
            storage_version: "v1".to_string(),
            served_versions: vec!["v1".to_string(), "v2".to_string()],
            strategy: Some("Webhook".to_string()),
            webhook_client_config: None,
            webhook_review_versions: vec!["v1".to_string()],
        };

        let versions = crd_watch_versions(Some(&conversion), "v2");

        assert_eq!(
            versions[0], "v1",
            "storage-version collection must be the deterministic baseline precedence"
        );
        assert!(
            versions.iter().any(|version| version == "v1"),
            "a v2 CRD watch must subscribe to storage-version live events"
        );
        assert!(
            versions.iter().any(|version| version == "v2"),
            "a v2 CRD watch must subscribe to requested-version live events"
        );
    }

    #[test]
    fn conversion_watch_baseline_merges_same_object_across_served_versions() {
        let resource = |api_version: &str, resource_version: i64| klights_cluster_core::Resource {
            id: 0,
            api_version: api_version.to_string(),
            kind: "Widget".to_string(),
            namespace: Some("default".to_string()),
            name: "same".to_string(),
            uid: "uid-same".to_string(),
            resource_version,
            data: std::sync::Arc::new(serde_json::json!({
                "apiVersion": api_version,
                "kind": "Widget",
                "metadata": {
                    "namespace": "default",
                    "name": "same",
                    "uid": "uid-same",
                    "resourceVersion": resource_version.to_string()
                }
            })),
        };
        let merged = merge_custom_resource_watch_baseline(vec![
            resource("widgets.test/v1", 12),
            resource("widgets.test/v2", 9),
        ]);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].api_version, "widgets.test/v1");
        assert_eq!(merged[0].resource_version, 12);
    }

    #[test]
    fn candidate_boundary_rejects_empty_and_repeated_opaque_tokens() {
        // A resumed public page must seed the consumed set with its incoming
        // opaque boundary. Returning that boundary again is no safer than
        // repeating one produced in this process.
        let mut last = Some("incoming".to_string());
        let mut consumed = std::collections::BTreeSet::new();
        consumed.insert("incoming".to_string());

        assert!(matches!(
            advance_crd_candidate_boundary(Some(String::new()), &mut last, &mut consumed),
            Err(klights_leader_api::ResourceQueryError::CorruptResponse { .. })
        ));
        assert_eq!(
            advance_crd_candidate_boundary(Some("opaque-a".into()), &mut last, &mut consumed)
                .unwrap(),
            Some("opaque-a".into())
        );
        assert!(matches!(
            advance_crd_candidate_boundary(Some("opaque-a".into()), &mut last, &mut consumed),
            Err(klights_leader_api::ResourceQueryError::Conflict { .. })
        ));
        assert!(matches!(
            advance_crd_candidate_boundary(Some("incoming".into()), &mut last, &mut consumed),
            Err(klights_leader_api::ResourceQueryError::Conflict { .. })
        ));
    }
}

async fn normalize_custom_resource_response_data(
    identity: &dyn crate::ApiIdentityGenerator,
    query: &dyn klights_leader_api::LeaderResourceQuery,
    conversion: Option<&crate::current::crd_conversion::CrdConversionConfig>,
    group: &str,
    plural: &str,
    requested_api_version: &str,
    mut data: serde_json::Value,
) -> Result<Value, AppError> {
    if conversion.is_none() {
        return Ok(std::mem::take(&mut data));
    }
    let conversion = conversion
        .expect("conversion.checked in branch above is equivalent; kept for type narrowing");
    let mut objects = vec![std::mem::take(&mut data)];
    let normalized = convert_crd_objects_to_requested_version(
        identity,
        query,
        conversion,
        group,
        plural,
        requested_api_version,
        std::mem::take(&mut objects),
    )
    .await?;
    normalized.into_iter().next().ok_or_else(|| {
        AppError::Internal("failed to normalize custom-resource response".to_string())
    })
}

async fn normalize_custom_resource_storage_data(
    identity: &dyn crate::ApiIdentityGenerator,
    query: &dyn klights_leader_api::LeaderResourceQuery,
    conversion: Option<&crate::current::crd_conversion::CrdConversionConfig>,
    group: &str,
    plural: &str,
    storage_api_version: &str,
    data: Value,
) -> Result<Value, AppError> {
    let Some(conversion) = conversion else {
        return Ok(data);
    };
    convert_crd_objects_to_requested_version(
        identity,
        query,
        conversion,
        group,
        plural,
        storage_api_version,
        vec![data],
    )
    .await?
    .into_iter()
    .next()
    .ok_or_else(|| {
        AppError::Internal("failed to normalize custom-resource storage data".to_string())
    })
}

async fn reconcile_custom_resource_owner_refs(
    state: &Arc<ApiState>,
    resource: &Resource,
    context: &'static str,
) {
    if resource
        .data
        .pointer("/metadata/ownerReferences")
        .and_then(|v| v.as_array())
        .is_none_or(|refs| refs.is_empty())
    {
        return;
    }

    if let Err(e) = crate::current::gc_ports::reconcile_owner_references(
        state.resource_mutation().gc_owner_lifecycle.as_ref(),
        resource.clone(),
    )
    .await
    {
        state
            .controller_reconcile()
            .metrics
            .record_cascade_delete_failure();
        tracing::error!(
            context,
            api_version = %resource.api_version,
            kind = %resource.kind,
            namespace = ?resource.namespace,
            name = %resource.name,
            error = %e,
            "custom resource ownerReference GC reconciliation failed"
        );
    }
}

async fn get_cr_inner(
    state: &Arc<ApiState>,
    info: &klights_leader_api::CrdResourceInfo,
    group: &str,
    version: &str,
    plural: &str,
    name: &str,
    ns: Option<&str>,
) -> Result<Response, AppError> {
    if name.trim().is_empty() {
        return Err(AppError::BadRequest("resource name required".to_string()));
    }

    let api_version = format!("{}/{}", group, version);
    let conversion = load_crd_conversion_config(
        state.resource_mutation().resource_query.as_ref(),
        group,
        plural,
    )
    .await?;
    let mut resource_opt = crate::current::resource_query_ports::get_resource(
        state.resource_mutation().resource_query.as_ref(),
        &api_version,
        &info.kind,
        ns,
        name,
    )
    .await?;
    if resource_opt.is_none()
        && let Some(conversion) = conversion.as_ref()
    {
        for served_version in &conversion.served_versions {
            if served_version == version {
                continue;
            }
            let candidate = crate::current::resource_query_ports::get_resource(
                state.resource_mutation().resource_query.as_ref(),
                &format!("{}/{}", group, served_version),
                &info.kind,
                ns,
                name,
            )
            .await?;
            if candidate.is_some() {
                resource_opt = candidate;
                break;
            }
        }
    }
    let resource =
        resource_opt.ok_or_else(|| AppError::NotFound("resource not found".to_string()))?;

    let mut data = std::sync::Arc::unwrap_or_clone(resource.data);
    data = normalize_custom_resource_response_data(
        state.resource_mutation().identity.as_ref(),
        state.resource_mutation().resource_query.as_ref(),
        conversion.as_ref(),
        group,
        plural,
        &api_version,
        data,
    )
    .await?;
    apply_crd_defaults(
        state.resource_mutation().resource_query.as_ref(),
        group,
        version,
        &info.kind,
        &mut data,
    )
    .await;
    Ok(Json(data).into_response())
}

pub async fn get_custom_resource(
    State(state): State<Arc<ApiState>>,
    Extension(identity): Extension<AuthenticatedIdentity>,
    headers: HeaderMap,
    Path((group, version, namespace, plural, name)): Path<(String, String, String, String, String)>,
    OriginalUri(uri): OriginalUri,
) -> Result<Response, AppError> {
    let info = match lookup_crd_or_proxy(
        &state,
        &identity,
        CrdLookupRequest::new(&group, &version, &plural, Method::GET, &uri, &headers),
        || Ok(Bytes::new()),
    )
    .await?
    {
        CrdLookup::Found(info) => info,
        CrdLookup::Proxied(resp) => return Ok(resp),
    };

    let ns = if info.namespaced {
        Some(namespace.as_str())
    } else {
        None
    };
    get_cr_inner(&state, &info, &group, &version, &plural, &name, ns).await
}

pub async fn proxy_namespaced_custom_resource_subresource(
    State(state): State<Arc<ApiState>>,
    Extension(identity): Extension<AuthenticatedIdentity>,
    method: Method,
    Path((group, version, _namespace, plural, name, subresource)): Path<(
        String,
        String,
        String,
        String,
        String,
        String,
    )>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, AppError> {
    let response = match lookup_crd_or_proxy(
        &state,
        &identity,
        CrdLookupRequest::new(&group, &version, &plural, method.clone(), &uri, &headers),
        || Ok(body.clone()),
    )
    .await?
    {
        CrdLookup::Proxied(resp) => resp,
        CrdLookup::Found(_) => {
            return Err(AppError::NotFound(format!(
                "custom resource subresource not supported: {}/{}/{}/{}{}{}",
                group, version, plural, name, "/", subresource
            )));
        }
    };
    Ok(response)
}

pub async fn proxy_cluster_custom_resource_subresource(
    State(state): State<Arc<ApiState>>,
    Extension(identity): Extension<AuthenticatedIdentity>,
    method: Method,
    Path((group, version, plural, name, subresource)): Path<(
        String,
        String,
        String,
        String,
        String,
    )>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, AppError> {
    let response = match lookup_crd_or_proxy(
        &state,
        &identity,
        CrdLookupRequest::new(&group, &version, &plural, method.clone(), &uri, &headers),
        || Ok(body.clone()),
    )
    .await?
    {
        CrdLookup::Proxied(resp) => resp,
        CrdLookup::Found(_) => {
            return Err(AppError::NotFound(format!(
                "custom resource subresource not supported: {}/{}/{}/{}{}{}",
                group, version, plural, name, "/", subresource
            )));
        }
    };
    Ok(response)
}

/// Validates a candidate-page boundary before it becomes the next root LIST
/// request.  The boundary is opaque to native service, but it is still a
/// protocol violation for the root to return an empty or already-consumed
/// boundary: accepting either would let a filtered CRD LIST loop forever.
fn advance_crd_candidate_boundary(
    boundary: Option<String>,
    last_boundary: &mut Option<String>,
    consumed_boundaries: &mut std::collections::BTreeSet<String>,
) -> Result<Option<String>, klights_leader_api::ResourceQueryError> {
    let Some(boundary) = boundary else {
        return Ok(None);
    };
    if boundary.is_empty() {
        return Err(klights_leader_api::ResourceQueryError::corrupt_response(
            "CRD candidate continuation omitted its opaque boundary",
        ));
    }
    if last_boundary.as_deref() == Some(boundary.as_str())
        || !consumed_boundaries.insert(boundary.clone())
    {
        return Err(klights_leader_api::ResourceQueryError::Conflict {
            message: "CRD candidate continuation repeated an opaque boundary".to_string(),
        });
    }
    *last_boundary = Some(boundary.clone());
    Ok(Some(boundary))
}

async fn list_cr_inner(
    state: &Arc<ApiState>,
    request: CustomResourceListRequest<'_>,
) -> Result<Response, AppError> {
    let CustomResourceListRequest {
        scope,
        query,
        headers,
    } = request;
    let CustomResourceScope {
        resource_type,
        namespace: ns,
        is_cluster_scope,
    } = scope;
    let CustomResourceType {
        info,
        group,
        version,
        plural,
    } = resource_type;
    let api_version = resource_type.api_version();
    // Watches retain their established live discovery path. LIST receives the
    // immutable canonical definition from the same root snapshot as its page.
    let conversion = if query.watch == Some("true".to_string()) {
        validate_crd_field_selector(
            &api_version,
            plural,
            query.label_selector.as_deref(),
            query.field_selector.as_deref(),
            info.namespaced,
            &info.selectable_fields,
        )?;
        load_crd_conversion_config(
            state.resource_mutation().resource_query.as_ref(),
            group,
            plural,
        )
        .await?
    } else {
        None
    };

    if query.watch == Some("true".to_string()) {
        query.validate_send_initial_events_watch()?;
        let kind = info.kind.clone();
        let av = api_version.clone();
        let requested_rv: i64 = query
            .resource_version
            .as_ref()
            .and_then(|rv| rv.parse::<i64>().ok())
            .unwrap_or(0);
        let emit_initial_state_for_resource_version_zero =
            query.resource_version.as_deref() == Some("0");

        let send_initial_events = query.send_initial_events.as_deref() == Some("true");
        let has_selector = query
            .label_selector
            .as_deref()
            .is_some_and(|s| !s.trim().is_empty())
            || query
                .field_selector
                .as_deref()
                .is_some_and(|s| !s.trim().is_empty());

        let send_bookmarks = query.allow_watch_bookmarks == Some("true".to_string());
        let stream_format =
            crate::current::watch_stream::negotiate_watch_stream_format(headers, false)?;
        let task_supervisor = state.operational().task_supervisor.clone();
        let label_selector = query.label_selector.clone();
        let field_selector = query.field_selector.clone();
        let timeout_seconds = query.timeout_seconds;
        let parsed_label_selector = label_selector
            .as_deref()
            .filter(|selector| !selector.trim().is_empty())
            .map(LabelSelector::parse)
            .transpose()
            .map_err(|err| AppError::BadRequest(format!("Invalid label selector: {err}")))?;
        let watch_ns = ns.map(str::to_string);
        let watch_scope = if is_cluster_scope {
            klights_leader_api::ResourceListScope::Cluster
        } else {
            watch_ns
                .clone()
                .map(klights_leader_api::ResourceListScope::Namespace)
                .unwrap_or(klights_leader_api::ResourceListScope::AllNamespaces)
        };
        let conversion_for_watch = conversion.clone();
        let group_for_watch = group.to_string();
        let plural_for_watch = plural.to_string();
        let requested_version_for_watch = version.to_string();
        let task_prefix = if is_cluster_scope {
            "cluster_custom_resource"
        } else {
            "custom_resource"
        };
        // A scoped watch (namespace and/or label/field selector) must anchor its
        // periodic BOOKMARK to the highest RV it has actually emitted for that
        // scope, not the global cursor/collection RV. See
        // `resolve_periodic_bookmark_rv` for the invariant.
        let has_scope_filter = watch_ns.is_some() || has_selector;
        let watch_versions =
            crd_watch_versions(conversion_for_watch.as_ref(), &requested_version_for_watch);
        let custom_watch_targets = if is_cluster_scope {
            watch_versions
                .iter()
                .map(|version| {
                    crate::current::custom_resource_ports::CustomResourceWatchTarget::cluster(
                        format!("{group_for_watch}/{version}"),
                        kind.clone(),
                    )
                })
                .collect::<Vec<_>>()
        } else {
            watch_versions
                .iter()
                .map(|version| {
                    let api_version = format!("{group_for_watch}/{version}");
                    if let Some(ns) = watch_ns.as_ref() {
                        crate::current::custom_resource_ports::CustomResourceWatchTarget::namespaced_in_namespace(
                            api_version,
                            kind.clone(),
                            ns.clone(),
                        )
                    } else {
                        crate::current::custom_resource_ports::CustomResourceWatchTarget::namespaced(
                            api_version,
                            kind.clone(),
                        )
                    }
                })
                .collect::<Vec<_>>()
        };
        {
            let projection = Arc::new(CrdWatchProjection {
                identity: state.resource_mutation().identity.clone(),
                resource_query: state.resource_mutation().resource_query.clone(),
                conversion: conversion_for_watch.clone(),
                group: group_for_watch.clone(),
                plural: plural_for_watch.clone(),
                requested_api_version: av.clone(),
            });
            state
                .resource_mutation()
                .custom_resource_reads
                .wait_until_fresh(requested_rv, av.clone(), kind.clone())
                .await;
            let emit_baseline = send_initial_events
                || requested_rv <= 0
                    && (has_selector || emit_initial_state_for_resource_version_zero);
            let mut start_position = None;
            let mut last_rv = requested_rv;
            let mut initial_frames = Vec::new();
            if emit_baseline {
                let list = match state
                    .resource_mutation()
                    .custom_resource_reads
                    .list_resources_for_watch_targets(
                        custom_watch_targets.clone(),
                        label_selector.clone(),
                    )
                    .await
                {
                    Ok(list) => list,
                    Err(error) => {
                        tracing::warn!(?error, "canonical CRD watch baseline LIST failed");
                        return Ok(crd_watch_frame_response(
                            stream_format,
                            crate::current::watch_stream::serialize_watch_status_line(
                                500,
                                "InternalError",
                                "failed to establish custom-resource watch baseline",
                            ),
                        ));
                    }
                };
                let Some(position) = list.watch_replay_position() else {
                    return Ok(crd_watch_frame_response(
                        stream_format,
                        crate::current::watch_stream::serialize_watch_status_line(
                            500,
                            "InternalError",
                            "custom-resource baseline did not provide an atomic position",
                        ),
                    ));
                };
                start_position = Some(position);
                last_rv = last_rv.max(list.resource_version());
                let projected = match crate::current::custom_resource_ports::CustomResourceProjection::project_resources(
                    projection.as_ref(),
                    list.into_items(),
                )
                .await
                {
                    Ok(resources) => resources,
                    Err(error) => {
                        return Ok(crd_watch_frame_response(
                            stream_format,
                            crate::current::watch_stream::serialize_watch_status_line(
                                500,
                                "InternalError",
                                &error.to_string(),
                            ),
                        ));
                    }
                };
                let parsed_field_selector = field_selector
                    .as_deref()
                    .filter(|selector| !selector.is_empty())
                    .map(klights_types::FieldSelector::parse)
                    .transpose()
                    .expect("CRD field selector was validated before watch establishment");
                for resource in projected.into_iter().filter(|resource| {
                    parsed_label_selector
                        .as_ref()
                        .is_none_or(|selector| selector.matches_resource(&resource.data))
                        && parsed_field_selector
                            .as_ref()
                            .is_none_or(|selector| selector.matches_resource(&resource.data))
                }) {
                    let event = crate::current::custom_resource_ports::added_watch_event(resource);
                    initial_frames.push(
                        crate::current::watch_stream::serialize_watch_event_line_without_table(
                            event,
                        ),
                    );
                }
                if send_initial_events {
                    initial_frames.push(
                        crate::current::watch_stream::serialize_watch_event_line_without_table(
                            WatchEvent::bookmark_initial_events_end(last_rv, &av, &kind),
                        ),
                    );
                }
            }
            let start_resource_version = if requested_rv == 0
                && !emit_initial_state_for_resource_version_zero
                && !send_initial_events
            {
                None
            } else {
                Some(requested_rv)
            };
            let request = match klights_leader_api::WatchRequest::try_new(
                av.clone(),
                kind.clone(),
                watch_ns.clone(),
                label_selector.clone(),
                field_selector.clone(),
                start_resource_version,
                start_position,
            ) {
                Ok(request) => request,
                Err(error) => {
                    return Ok(crd_watch_frame_response(
                        stream_format,
                        crate::current::watch_stream::serialize_watch_status_line(
                            400,
                            "BadRequest",
                            &error.to_string(),
                        ),
                    ));
                }
            };
            let mut positioned_stream = match state
                .resource_mutation()
                .custom_resource_reads
                .watch_projected_resources(request, custom_watch_targets, projection)
                .await
            {
                Ok(stream) => stream,
                Err(error) => {
                    return Ok(crd_watch_frame_response(
                        stream_format,
                        crate::current::watch_stream::serialize_watch_status_line(
                            if matches!(
                                error,
                                klights_leader_api::LeaderWatchError::ReplayExpired { .. }
                            ) {
                                410
                            } else {
                                500
                            },
                            if matches!(
                                error,
                                klights_leader_api::LeaderWatchError::ReplayExpired { .. }
                            ) {
                                "Expired"
                            } else {
                                "InternalError"
                            },
                            &error.to_string(),
                        ),
                    ));
                }
            };
            let bookmark_reads = state.resource_mutation().custom_resource_reads.clone();
            let stream = async_stream::stream! {
                for frame in initial_frames {
                    yield Ok::<_, std::convert::Infallible>(frame);
                }
                let mut bookmark_ticks = maybe_spawn_bookmark_tick_stream(
                    send_bookmarks,
                    task_supervisor.clone(),
                    format!("{task_prefix}_watch_bookmarks_{group_for_watch}_{plural_for_watch}"),
                ).await;
                let mut timeout_tick = maybe_spawn_watch_timeout_stream(
                    timeout_seconds,
                    task_supervisor.clone(),
                    format!("{task_prefix}_watch_timeout_{group_for_watch}_{plural_for_watch}"),
                ).await;
                loop {
                    tokio::select! {
                        Some(()) = recv_watch_timeout(&mut timeout_tick) => break,
                        next = futures::StreamExt::next(&mut positioned_stream) => {
                            let Some(next) = next else { break; };
                            let event = match next {
                                Ok(event) => event,
                                Err(error) => {
                                    yield Ok::<_, std::convert::Infallible>(
                                        crate::current::watch_stream::serialize_watch_status_line(
                                            if matches!(error, klights_leader_api::LeaderWatchError::ReplayExpired { .. }) { 410 } else { 500 },
                                            if matches!(error, klights_leader_api::LeaderWatchError::ReplayExpired { .. }) { "Expired" } else { "InternalError" },
                                            &error.to_string(),
                                        ),
                                    );
                                    break;
                                }
                            };
                            last_rv = last_rv.max(event.resource().resource_version);
                            yield Ok::<_, std::convert::Infallible>(
                                crate::current::watch_stream::serialize_watch_event_line_without_table(
                                    crate::current::custom_resource_ports::resource_event_to_watch_event(&event),
                                ),
                            );
                        }
                        Some(()) = recv_bookmark_tick(&mut bookmark_ticks), if send_bookmarks => {
                            let mut rv = crate::current::watch_stream::bookmark_rv_for_watch_scope(
                                has_scope_filter,
                                last_rv,
                                last_rv,
                            );
                            if rv <= 0 && !has_scope_filter {
                                rv = bookmark_reads
                                    .current_collection_resource_version(
                                        av.clone(),
                                        kind.clone(),
                                        watch_scope.clone(),
                                    )
                                    .await
                                    .unwrap_or(0);
                            }
                            yield Ok::<_, std::convert::Infallible>(
                                crate::current::watch_stream::serialize_watch_event_line_without_table(
                                    WatchEvent::bookmark_typed(rv, &av, &kind),
                                ),
                            );
                        }
                    }
                }
            };
            return Ok(Response::builder()
                .header("Content-Type", stream_format.content_type())
                .header("Transfer-Encoding", "chunked")
                .body(Body::from_stream(stream))
                .unwrap());
        }
    }

    let operation_now = state.operational().clock.now();
    let normalized_limit = query.normalized_limit()?;
    // Every CRD collection is read from its canonical storage version. The
    // continuation stays entirely on the typed leader/root/store path; native
    // service owns only the outer envelope, never a name/RV cursor. Conversion
    // happens after the pinned page is selected.
    let collection_scope = if info.namespaced {
        ns.map(str::to_owned)
            .map(klights_leader_api::ResourceListScope::Namespace)
            .unwrap_or(klights_leader_api::ResourceListScope::AllNamespaces)
    } else {
        klights_leader_api::ResourceListScope::Cluster
    };
    let (continue_token, continuation_mode) =
        crate::generic_read::process_generic_list_continue_token(query.continue_token.clone())?;
    let resource_version_match = match query.resolve_resource_version_match(
        continuation_mode != klights_leader_api::ResourceListContinuationMode::Initial,
    )? {
        ListResourceVersionMatch::Any => klights_leader_api::ResourceListResourceVersionMatch::Any,
        ListResourceVersionMatch::NotOlderThan(rv) => {
            klights_leader_api::ResourceListResourceVersionMatch::NotOlderThan(rv)
        }
        ListResourceVersionMatch::Exact(rv) => {
            klights_leader_api::ResourceListResourceVersionMatch::Exact(rv)
        }
    };
    // Field-selector matching occurs after CRD conversion/defaulting, while
    // the root can only page canonical storage candidates.  Keep consuming
    // those bounded root pages in this one request until the requested public
    // page is full or the pinned collection is exhausted.  This is neither a
    // polling loop nor recursion: every iteration awaits one new opaque root
    // continuation boundary and is bounded by the finite pinned collection.
    let candidate_mode = query.field_selector.is_some();
    let mut current_continue_token = continue_token;
    let mut current_continuation_mode = continuation_mode;
    let mut last_opaque_boundary = candidate_mode
        .then(|| current_continue_token.clone())
        .flatten();
    let mut consumed_opaque_boundaries = std::collections::BTreeSet::new();
    if let Some(boundary) = last_opaque_boundary.as_ref() {
        consumed_opaque_boundaries.insert(boundary.clone());
    }
    let mut frozen_definition: Option<klights_cluster_core::Resource> = None;
    let mut conversion: Option<Option<crate::current::crd_conversion::CrdConversionConfig>> = None;
    let mut response_rv: Option<i64> = None;
    let mut next_inner = None;
    let mut remaining_item_count: Option<i64>;
    let mut items = Vec::new();

    loop {
        // A continuation already owns its replay position. Reapplying an
        // initial Exact/NotOlderThan contract would be invalid and can make a
        // converted field-selector scan fail only after its first 64-candidate
        // root page.
        let batch_resource_version_match = if current_continuation_mode
            == klights_leader_api::ResourceListContinuationMode::Initial
        {
            resource_version_match
        } else {
            klights_leader_api::ResourceListResourceVersionMatch::Any
        };
        let list = state
            .resource_mutation()
            .resource_query
            .list_resources(
                klights_leader_api::ResourceListRequest::try_new_with_continuation_mode(
                    &api_version,
                    &info.kind,
                    collection_scope.clone(),
                    query.label_selector.clone(),
                    query.field_selector.clone(),
                    normalized_limit,
                    current_continue_token.take(),
                    current_continuation_mode,
                    klights_leader_api::ResourceQueryConsistency::LeaderFresh,
                )?
                .with_resource_version_match(batch_resource_version_match)?
                .with_custom_resource_identity(
                    klights_leader_api::CustomResourceListIdentity::try_new(
                        group, plural, version,
                    )?,
                )?,
            )
            .await
            .map_err(|error| {
                crate::generic_read::generic_list_query_error(error, operation_now.unix_timestamp())
            })?;
        let page_definition = list
            .frozen_custom_resource_definition()
            .cloned()
            .ok_or_else(|| {
                AppError::Internal("CRD LIST response omitted frozen definition".into())
            })?;
        if let Some(expected_definition) = frozen_definition.as_ref() {
            if expected_definition.data != page_definition.data {
                return Err(AppError::Conflict(
                    "CRD candidate pages did not retain the frozen definition".into(),
                ));
            }
        } else {
            let frozen_info =
                klights_leader_api::resource_infos_from_value(page_definition.data.as_ref())
                    .map_err(AppError::Internal)?
                    .into_iter()
                    .find(|candidate| {
                        candidate.group == group
                            && candidate.plural == plural
                            && candidate.version == version
                            && candidate.kind == info.kind
                    })
                    .ok_or_else(|| {
                        AppError::BadRequest(
                            "frozen CRD does not serve requested collection".into(),
                        )
                    })?;
            validate_crd_field_selector(
                &api_version,
                plural,
                query.label_selector.as_deref(),
                query.field_selector.as_deref(),
                frozen_info.namespaced,
                &frozen_info.selectable_fields,
            )?;
            conversion = Some(
                crate::current::crd_conversion::crd_conversion_config_from_definition(
                    &page_definition,
                )?,
            );
            frozen_definition = Some(page_definition);
        }
        let candidate_continue_tokens = list.candidate_continue_tokens().to_vec();
        let (listed_resources, page_rv, _, page_next_inner, page_remaining_item_count) =
            list.into_parts();
        if candidate_mode && candidate_continue_tokens.len() != listed_resources.len() {
            return Err(AppError::Internal(format!(
                "CRD candidate response has {} resources but {} continuation boundaries",
                listed_resources.len(),
                candidate_continue_tokens.len(),
            )));
        }
        if let Some(expected_rv) = response_rv {
            if expected_rv != page_rv {
                return Err(AppError::Conflict(
                    "CRD candidate pages did not retain the pinned resourceVersion".into(),
                ));
            }
        } else {
            response_rv = Some(page_rv);
        }
        remaining_item_count = page_remaining_item_count;
        let frozen_definition = frozen_definition
            .as_ref()
            .expect("first CRD page installs its frozen definition");
        for (candidate_index, resource) in listed_resources.into_iter().enumerate() {
            let mut data = normalize_custom_resource_response_data(
                state.resource_mutation().identity.as_ref(),
                state.resource_mutation().resource_query.as_ref(),
                conversion
                    .as_ref()
                    .expect("first CRD page installs conversion configuration")
                    .as_ref(),
                group,
                plural,
                &api_version,
                std::sync::Arc::unwrap_or_clone(resource.data),
            )
            .await?;
            apply_crd_defaults_from_definition(frozen_definition, version, &mut data);
            if !object_matches_field_selector(&data, query.field_selector.as_deref()) {
                continue;
            }
            items.push(data);
            if normalized_limit.is_some_and(|limit| items.len() >= limit as usize) {
                // A per-candidate cursor after the final candidate of an
                // exhausted root page would manufacture a needless empty
                // public page.  It is needed only when candidates remain in
                // this page or the root explicitly has another page.
                let candidates_remain_in_page =
                    candidate_index + 1 < candidate_continue_tokens.len();
                let boundary = if candidates_remain_in_page {
                    Some(
                        candidate_continue_tokens
                            .get(candidate_index)
                            .cloned()
                            .flatten()
                            .ok_or_else(|| {
                                AppError::Internal(
                                    "CRD candidate response omitted a continuation boundary".into(),
                                )
                            })?,
                    )
                } else {
                    page_next_inner.clone()
                };
                next_inner = advance_crd_candidate_boundary(
                    boundary,
                    &mut last_opaque_boundary,
                    &mut consumed_opaque_boundaries,
                )
                .map_err(|error| {
                    crate::generic_read::generic_list_query_error(
                        error,
                        operation_now.unix_timestamp(),
                    )
                })?;
                break;
            }
        }
        if normalized_limit.is_some_and(|limit| items.len() >= limit as usize) || !candidate_mode {
            break;
        }
        let Some(boundary) = advance_crd_candidate_boundary(
            page_next_inner,
            &mut last_opaque_boundary,
            &mut consumed_opaque_boundaries,
        )
        .map_err(|error| {
            crate::generic_read::generic_list_query_error(error, operation_now.unix_timestamp())
        })?
        else {
            break;
        };
        current_continue_token = Some(boundary);
        current_continuation_mode = klights_leader_api::ResourceListContinuationMode::Pinned;
    }
    let response_rv = response_rv.expect("CRD LIST always receives an initial root page");
    if candidate_mode {
        remaining_item_count = None;
    }
    let mut metadata = serde_json::json!({
        "resourceVersion": response_rv.to_string(),
    });
    if let Some(inner) = next_inner {
        metadata["continue"] =
            serde_json::Value::String(crate::generic_read::encode_generic_list_continue_token(
                &inner,
                response_rv,
                operation_now.unix_timestamp(),
                crate::generic_read::PublicListContinuationMode::Pinned,
            ));
    }
    if let Some(remaining) = remaining_item_count {
        metadata["remainingItemCount"] = serde_json::Value::Number(remaining.into());
    }
    Ok(Json(serde_json::json!({
            "apiVersion": api_version,
            "kind": format!("{}List", info.kind),
            "metadata": metadata,
            "items": items,
    }))
    .into_response())
}

pub async fn list_custom_resources(
    State(state): State<Arc<ApiState>>,
    Extension(identity): Extension<AuthenticatedIdentity>,
    headers: HeaderMap,
    Path((group, version, namespace, plural)): Path<(String, String, String, String)>,
    Query(query): Query<ListQuery>,
    OriginalUri(uri): OriginalUri,
) -> Result<Response, AppError> {
    let info = match lookup_crd_or_proxy(
        &state,
        &identity,
        CrdLookupRequest::new(&group, &version, &plural, Method::GET, &uri, &headers),
        || Ok(Bytes::new()),
    )
    .await?
    {
        CrdLookup::Found(info) => info,
        CrdLookup::Proxied(resp) => return Ok(resp),
    };

    let resource_type = CustomResourceType::new(&info, &group, &version, &plural);
    let ns = if info.namespaced {
        Some(namespace.as_str())
    } else {
        None
    };
    list_cr_inner(
        &state,
        CustomResourceListRequest {
            scope: resource_type.scoped(ns, false),
            query: &query,
            headers: &headers,
        },
    )
    .await
}

async fn delete_collection_cr_inner(
    state: &Arc<ApiState>,
    request: CustomResourceCollectionDeleteRequest<'_>,
) -> Result<Response, AppError> {
    let CustomResourceCollectionDeleteRequest {
        scope,
        query,
        body,
        log_context,
    } = request;
    let CustomResourceScope {
        resource_type,
        namespace: ns,
        ..
    } = scope;
    let CustomResourceType {
        info,
        group,
        version,
        plural,
    } = resource_type;
    let api_version = resource_type.api_version();
    validate_crd_field_selector(
        &api_version,
        plural,
        query.label_selector.as_deref(),
        query.field_selector.as_deref(),
        info.namespaced,
        &info.selectable_fields,
    )?;

    let conversion = load_crd_conversion_config(
        state.resource_mutation().resource_query.as_ref(),
        group,
        plural,
    )
    .await?;

    let mut names = Vec::new();
    if let Some(conversion) = conversion.as_ref() {
        let (resources, _) = gather_custom_resources_across_served_versions(
            state.resource_mutation().resource_query.as_ref(),
            conversion,
            group,
            &info.kind,
            ns.map(str::to_string),
            query.label_selector.clone(),
        )
        .await?;
        let mut objects: Vec<Value> = resources
            .into_iter()
            .map(|r| std::sync::Arc::unwrap_or_clone(r.data))
            .collect();
        objects = convert_crd_objects_to_requested_version(
            state.resource_mutation().identity.as_ref(),
            state.resource_mutation().resource_query.as_ref(),
            conversion,
            group,
            plural,
            &api_version,
            objects,
        )
        .await?;
        for object in objects {
            if !object_matches_field_selector(&object, query.field_selector.as_deref()) {
                continue;
            }
            if let Some(name) = object
                .pointer("/metadata/name")
                .and_then(|v| v.as_str())
                .map(ToString::to_string)
            {
                names.push(name);
            }
        }
    } else {
        let list = crate::current::resource_query_ports::list_resources(
            state.resource_mutation().resource_query.as_ref(),
            &api_version,
            &info.kind,
            if info.namespaced {
                ns.map(str::to_owned)
                    .map(klights_leader_api::ResourceListScope::Namespace)
                    .unwrap_or(klights_leader_api::ResourceListScope::AllNamespaces)
            } else {
                klights_leader_api::ResourceListScope::Cluster
            },
            query.label_selector.as_deref(),
            query.field_selector.as_deref(),
            None,
            None,
        )
        .await?;
        names.extend(list.into_items().into_iter().map(|resource| resource.name));
    }

    let delete_intent =
        crate::generic_command::DeleteIntent::from_delete_collection_query_and_body(query, &body)?;
    let dry_run = delete_intent.dry_run;
    if dry_run.is_all() {
        return Ok(
            Json(crate::generic_command::delete_collection_success_status()).into_response(),
        );
    }

    let mut unique_names = std::collections::HashSet::new();
    let mut items = Vec::new();
    for name in names {
        if !unique_names.insert(name.clone()) {
            continue;
        }
        let (current, stored_api_version) = match get_existing_custom_resource_for_write(
            state,
            group,
            version,
            plural,
            &info.kind,
            ns.map(str::to_string),
            &name,
        )
        .await
        {
            Ok(value) => value,
            Err(AppError::NotFound(_)) => continue,
            Err(err) => return Err(err),
        };
        items.push((
            klights_types::ResourceKey::new(
                stored_api_version,
                info.kind.clone(),
                ns.map(str::to_string),
                name,
            ),
            current,
        ));
    }

    let strategy = crate::generic_command::FinalizerAwareDeleteStrategy {
        resource_query: state.resource_mutation().resource_query.as_ref(),
        lifecycle: state.resource_mutation().finalizer_lifecycle.as_ref(),
        operation_now: klights_auth::clock::chrono_utc(state.operational().clock.now()),
    };
    let results =
        crate::generic_command::delete_collection_items(&strategy, items, &delete_intent).await?;
    for result in results {
        match result {
            crate::generic_command::DeleteResult::HardDeleted(deleted) => {
                dispatch_custom_resource_mutation_event(
                    state,
                    klights_reconcile_api::MutationOperation::HardDelete,
                    &deleted.data,
                    "custom_delete_collection_hard_delete",
                )
                .await;
                if !delete_intent.orphan_children
                    && let Err(e) = crate::current::gc_ports::cascade_delete(
                        state.resource_mutation().gc_owner_lifecycle.as_ref(),
                        klights_reconcile_api::GcOwnerIdentity::new(
                            &deleted.api_version,
                            &deleted.kind,
                            deleted.namespace.clone(),
                            &deleted.name,
                            &deleted.uid,
                        ),
                    )
                    .await
                {
                    state
                        .controller_reconcile()
                        .metrics
                        .record_cascade_delete_failure();
                    tracing::error!(name = %deleted.name, kind = %deleted.kind, error = %e, "{log_context}: cascade delete failed");
                }
            }
            crate::generic_command::DeleteResult::MarkedTerminating(marked) => {
                dispatch_custom_resource_mutation_event(
                    state,
                    klights_reconcile_api::MutationOperation::DeleteMark,
                    &marked.data,
                    "custom_delete_collection_mark",
                )
                .await;
            }
            crate::generic_command::DeleteResult::GoneOrUidChanged => {}
        }
    }

    Ok(Json(crate::generic_command::delete_collection_success_status()).into_response())
}

pub async fn delete_collection_custom_resources(
    State(state): State<Arc<ApiState>>,
    Extension(identity): Extension<AuthenticatedIdentity>,
    headers: HeaderMap,
    Path((group, version, namespace, plural)): Path<(String, String, String, String)>,
    Query(query): Query<DeleteCollectionQuery>,
    OriginalUri(uri): OriginalUri,
    body: Bytes,
) -> Result<Response, AppError> {
    let info = match lookup_crd_or_proxy(
        &state,
        &identity,
        CrdLookupRequest::new(&group, &version, &plural, Method::DELETE, &uri, &headers),
        || Ok(body.clone()),
    )
    .await?
    {
        CrdLookup::Found(info) => info,
        CrdLookup::Proxied(resp) => return Ok(resp),
    };

    let resource_type = CustomResourceType::new(&info, &group, &version, &plural);
    let ns = if info.namespaced {
        Some(namespace.as_str())
    } else {
        None
    };
    delete_collection_cr_inner(
        &state,
        CustomResourceCollectionDeleteRequest {
            scope: resource_type.scoped(ns, false),
            query: &query,
            body,
            log_context: "delete collection (CRD)",
        },
    )
    .await
}

async fn dispatch_custom_resource_mutation_event(
    state: &Arc<ApiState>,
    operation: klights_reconcile_api::MutationOperation,
    resource: &serde_json::Value,
    context: &'static str,
) {
    crate::generic_command::dispatch_mutation_event(
        state.resource_mutation().mutation_effects.as_ref(),
        crate::generic_command::MutationEvent {
            operation,
            resource,
            old_resource: None,
            persisted: true,
            dry_run: crate::generic_command::DryRunMode::Live,
            context,
        },
    )
    .await;
}

async fn after_persisted_custom_resource_write(
    state: &Arc<ApiState>,
    resource: &Resource,
    operation: klights_reconcile_api::MutationOperation,
    owner_ref_context: &'static str,
    event_context: &'static str,
) {
    reconcile_custom_resource_owner_refs(state, resource, owner_ref_context).await;
    dispatch_custom_resource_mutation_event(state, operation, &resource.data, event_context).await;
}

struct CustomResourceCreateStrategy<'a> {
    state: &'a Arc<ApiState>,
    scope: CustomResourceScope<'a>,
    query: &'a CreateUpdateQuery,
    log_context: &'static str,
}

#[async_trait]
impl<'a> CreateStrategy for CustomResourceCreateStrategy<'a> {
    async fn before_admission(&self, body: Value) -> Result<Value, AppError> {
        let CustomResourceType {
            info,
            group,
            version,
            ..
        } = self.scope.resource_type;
        let mut body = body;
        apply_crd_defaults(
            self.state.resource_mutation().resource_query.as_ref(),
            group,
            version,
            &info.kind,
            &mut body,
        )
        .await;
        if self.query.field_validation.as_deref() == Some("Strict") {
            check_cr_field_validation_strict(
                self.state.resource_mutation().resource_query.as_ref(),
                group,
                version,
                &info.kind,
                &body,
            )
            .await?;
        }
        Ok(body)
    }

    async fn admit(
        &self,
        body: Value,
        dry_run: crate::generic_command::DryRunMode,
    ) -> Result<Value, AppError> {
        let CustomResourceType { info, .. } = self.scope.resource_type;
        let api_version = self.scope.resource_type.api_version();
        let namespace = self.scope.namespace.map(str::to_string);
        let name = body
            .get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|n| n.as_str())
            .map(ToString::to_string);
        let mut body = self
            .state
            .resource_mutation()
            .admission
            .admit(crate::generic_command::ResourceAdmissionRequest {
                api_version,
                kind: info.kind.clone(),
                resource: None,
                operation: "CREATE".to_string(),
                namespace,
                name,
                object: body,
                old_object: None,
                dry_run: dry_run.is_all(),
                subresource: None,
                options: None,
            })
            .await?;
        let CustomResourceType { group, version, .. } = self.scope.resource_type;
        apply_crd_pruning(
            self.state.resource_mutation().resource_query.as_ref(),
            group,
            version,
            &info.kind,
            &mut body,
        )
        .await;
        Ok(body)
    }

    async fn persist_create(
        &self,
        body: Value,
        dry_run: crate::generic_command::DryRunMode,
    ) -> Result<WriteResult, AppError> {
        if dry_run.is_all() {
            return Ok(WriteResult::DryRun(body));
        }
        let CustomResourceType {
            info,
            group,
            version,
            plural,
        } = self.scope.resource_type;
        let ns = self.scope.namespace;
        let api_version = self.scope.resource_type.api_version();
        let name = body["metadata"]["name"]
            .as_str()
            .ok_or_else(|| AppError::BadRequest("metadata.name required".to_string()))?
            .to_string();
        let conversion = load_crd_conversion_config(
            self.state.resource_mutation().resource_query.as_ref(),
            group,
            plural,
        )
        .await?;
        let storage_api_version =
            storage_api_version_for_request(group, version, conversion.as_ref());
        if get_existing_custom_resource_for_write(
            self.state,
            group,
            version,
            plural,
            &info.kind,
            ns.map(str::to_string),
            &name,
        )
        .await
        .is_ok()
        {
            return Err(AppError::Conflict(format!("{} already exists", name)));
        }
        let storage_body = normalize_custom_resource_storage_data(
            self.state.resource_mutation().identity.as_ref(),
            self.state.resource_mutation().resource_query.as_ref(),
            conversion.as_ref(),
            group,
            plural,
            &storage_api_version,
            body,
        )
        .await?;
        let resource = crate::generic_command::create_non_pod_resource(
            self.state.resource_mutation().resource_command.as_ref(),
            &storage_api_version,
            &info.kind,
            ns,
            &name,
            storage_body,
        )
        .await?;
        after_persisted_custom_resource_write(
            self.state,
            &resource,
            klights_reconcile_api::MutationOperation::Create,
            self.log_context,
            "custom_create",
        )
        .await;
        let data = normalize_custom_resource_response_data(
            self.state.resource_mutation().identity.as_ref(),
            self.state.resource_mutation().resource_query.as_ref(),
            conversion.as_ref(),
            group,
            plural,
            &api_version,
            inject_resource_version_with_identity(
                self.state.resource_mutation().identity.as_ref(),
                resource.data,
                resource.resource_version,
            ),
        )
        .await?;
        Ok(WriteResult::Response {
            status: StatusCode::CREATED,
            body: data,
        })
    }
}

struct CustomResourceUpdateStrategy<'a> {
    state: &'a Arc<ApiState>,
    target: CustomResourceName<'a>,
    log_context: &'static str,
}

#[async_trait]
impl<'a> UpdateStrategy for CustomResourceUpdateStrategy<'a> {
    async fn load_current(&self) -> Result<Resource, AppError> {
        let CustomResourceType {
            info,
            group,
            version,
            plural,
        } = self.target.scope.resource_type;
        let ns = self.target.scope.namespace;
        let (current, _stored_api_version) = get_existing_custom_resource_for_write(
            self.state,
            group,
            version,
            plural,
            &info.kind,
            ns.map(str::to_string),
            self.target.name,
        )
        .await?;
        Ok(current)
    }

    async fn prepare_update(
        &self,
        current: &Resource,
        mut body: Value,
        _dry_run: crate::generic_command::DryRunMode,
    ) -> Result<Value, AppError> {
        let CustomResourceType {
            info,
            group,
            version,
            ..
        } = self.target.scope.resource_type;
        let ns = self.target.scope.namespace;
        let api_version = self.target.scope.resource_type.api_version();
        body = self
            .state
            .resource_mutation()
            .admission
            .admit(crate::generic_command::ResourceAdmissionRequest {
                api_version,
                kind: info.kind.clone(),
                resource: None,
                operation: "UPDATE".to_string(),
                namespace: ns.map(str::to_string),
                name: Some(self.target.name.to_string()),
                object: body,
                old_object: Some((*current.data).clone()),
                dry_run: false,
                subresource: None,
                options: None,
            })
            .await?;
        apply_crd_pruning(
            self.state.resource_mutation().resource_query.as_ref(),
            group,
            version,
            &info.kind,
            &mut body,
        )
        .await;
        crate::generic_command::prepare_custom_generation_for_update(&current.data, &mut body);
        crate::generic_command::preserve_deletion_timestamp_on_update(&current.data, &mut body);
        Ok(body)
    }

    async fn persist_update(
        &self,
        current: Resource,
        body: Value,
        _dry_run: crate::generic_command::DryRunMode,
    ) -> Result<WriteResult, AppError> {
        let CustomResourceType {
            info,
            group,
            version: _,
            plural,
        } = self.target.scope.resource_type;
        let ns = self.target.scope.namespace;
        let api_version = self.target.scope.resource_type.api_version();
        let stored_api_version = current.api_version.clone();
        let conversion = load_crd_conversion_config(
            self.state.resource_mutation().resource_query.as_ref(),
            group,
            plural,
        )
        .await?;
        let storage_body = normalize_custom_resource_storage_data(
            self.state.resource_mutation().identity.as_ref(),
            self.state.resource_mutation().resource_query.as_ref(),
            conversion.as_ref(),
            group,
            plural,
            &stored_api_version,
            body,
        )
        .await?;
        let resource = crate::generic_command::update_non_pod_resource(
            self.state.resource_mutation().resource_command.as_ref(),
            &stored_api_version,
            &info.kind,
            ns,
            self.target.name,
            storage_body,
            current.resource_version,
        )
        .await?;
        after_persisted_custom_resource_write(
            self.state,
            &resource,
            klights_reconcile_api::MutationOperation::Update,
            self.log_context,
            "custom_update",
        )
        .await;
        crate::generic_command::finalize_after_update_if_ready(
            self.state,
            &info.kind,
            ns,
            self.target.name,
            &resource,
        )
        .await;
        let data = normalize_custom_resource_response_data(
            self.state.resource_mutation().identity.as_ref(),
            self.state.resource_mutation().resource_query.as_ref(),
            conversion.as_ref(),
            group,
            plural,
            &api_version,
            inject_resource_version_with_identity(
                self.state.resource_mutation().identity.as_ref(),
                resource.data,
                resource.resource_version,
            ),
        )
        .await?;
        Ok(WriteResult::Response {
            status: StatusCode::OK,
            body: data,
        })
    }
}

struct CustomResourcePatchStrategy<'a> {
    state: &'a Arc<ApiState>,
    target: CustomResourceName<'a>,
    query: &'a CreateUpdateQuery,
    headers: &'a HeaderMap,
    apply_create_context: &'static str,
    patch_context: &'static str,
}

#[async_trait]
impl<'a> PatchStrategy for CustomResourcePatchStrategy<'a> {
    async fn apply_patch(
        &self,
        patch: Value,
        dry_run: crate::generic_command::DryRunMode,
    ) -> Result<WriteResult, AppError> {
        let CustomResourceType {
            info,
            group,
            version,
            plural,
        } = self.target.scope.resource_type;
        let ns = self.target.scope.namespace;
        let api_version = self.target.scope.resource_type.api_version();
        let content_type = self
            .headers
            .get("content-type")
            .and_then(|h| h.to_str().ok());
        let is_apply_yaml = content_type == Some("application/apply-patch+yaml");
        let is_dry_run = dry_run.is_all();
        if is_apply_yaml && self.query.field_validation.as_deref() == Some("Strict") {
            check_cr_field_validation_strict(
                self.state.resource_mutation().resource_query.as_ref(),
                group,
                version,
                &info.kind,
                &patch,
            )
            .await?;
        }
        let conversion = load_crd_conversion_config(
            self.state.resource_mutation().resource_query.as_ref(),
            group,
            plural,
        )
        .await?;
        let storage_api_version =
            storage_api_version_for_request(group, version, conversion.as_ref());

        let existing = get_existing_custom_resource_for_write(
            self.state,
            group,
            version,
            plural,
            &info.kind,
            ns.map(str::to_string),
            self.target.name,
        )
        .await;
        let (current, stored_api_version) = match existing {
            Ok(existing) => existing,
            Err(AppError::NotFound(_)) if is_apply_yaml => {
                let mut created_resource = patch.clone();
                apply_crd_defaults(
                    self.state.resource_mutation().resource_query.as_ref(),
                    group,
                    version,
                    &info.kind,
                    &mut created_resource,
                )
                .await;
                created_resource = self
                    .state
                    .resource_mutation()
                    .admission
                    .admit(crate::generic_command::ResourceAdmissionRequest {
                        api_version: api_version.clone(),
                        kind: info.kind.clone(),
                        resource: None,
                        operation: "CREATE".to_string(),
                        namespace: ns.map(str::to_string),
                        name: Some(self.target.name.to_string()),
                        object: created_resource,
                        old_object: None,
                        dry_run: is_dry_run,
                        subresource: None,
                        options: None,
                    })
                    .await?;
                apply_crd_pruning(
                    self.state.resource_mutation().resource_query.as_ref(),
                    group,
                    version,
                    &info.kind,
                    &mut created_resource,
                )
                .await;

                if is_dry_run {
                    return Ok(WriteResult::Response {
                        status: StatusCode::CREATED,
                        body: created_resource,
                    });
                }

                let storage_created_resource = normalize_custom_resource_storage_data(
                    self.state.resource_mutation().identity.as_ref(),
                    self.state.resource_mutation().resource_query.as_ref(),
                    conversion.as_ref(),
                    group,
                    plural,
                    &api_version,
                    created_resource,
                )
                .await?;
                let resource = crate::generic_command::create_non_pod_resource(
                    self.state.resource_mutation().resource_command.as_ref(),
                    &storage_api_version,
                    &info.kind,
                    ns,
                    self.target.name,
                    storage_created_resource,
                )
                .await?;
                after_persisted_custom_resource_write(
                    self.state,
                    &resource,
                    klights_reconcile_api::MutationOperation::Create,
                    self.apply_create_context,
                    "custom_apply_create",
                )
                .await;
                let data = normalize_custom_resource_response_data(
                    self.state.resource_mutation().identity.as_ref(),
                    self.state.resource_mutation().resource_query.as_ref(),
                    conversion.as_ref(),
                    group,
                    plural,
                    &api_version,
                    inject_resource_version_with_identity(
                        self.state.resource_mutation().identity.as_ref(),
                        resource.data,
                        resource.resource_version,
                    ),
                )
                .await?;
                return Ok(WriteResult::Response {
                    status: StatusCode::CREATED,
                    body: data,
                });
            }
            Err(err) => return Err(err),
        };

        let mut patched_resource =
            crate::current::apply_patch(&current.data, &patch, content_type)?;
        patched_resource = self
            .state
            .resource_mutation()
            .admission
            .admit(crate::generic_command::ResourceAdmissionRequest {
                api_version: api_version.clone(),
                kind: info.kind.clone(),
                resource: None,
                operation: "UPDATE".to_string(),
                namespace: ns.map(str::to_string),
                name: Some(self.target.name.to_string()),
                object: patched_resource,
                old_object: Some((*current.data).clone()),
                dry_run: is_dry_run,
                subresource: None,
                options: None,
            })
            .await?;
        apply_crd_pruning(
            self.state.resource_mutation().resource_query.as_ref(),
            group,
            version,
            &info.kind,
            &mut patched_resource,
        )
        .await;
        crate::generic_command::prepare_custom_generation_for_update(
            &current.data,
            &mut patched_resource,
        );
        crate::generic_command::preserve_deletion_timestamp_on_update(
            &current.data,
            &mut patched_resource,
        );

        if is_dry_run {
            return Ok(WriteResult::DryRun(patched_resource));
        }

        let storage_patched_resource = normalize_custom_resource_storage_data(
            self.state.resource_mutation().identity.as_ref(),
            self.state.resource_mutation().resource_query.as_ref(),
            conversion.as_ref(),
            group,
            plural,
            &stored_api_version,
            patched_resource,
        )
        .await?;
        let resource = crate::generic_command::update_non_pod_resource(
            self.state.resource_mutation().resource_command.as_ref(),
            &stored_api_version,
            &info.kind,
            ns,
            self.target.name,
            storage_patched_resource,
            current.resource_version,
        )
        .await?;
        after_persisted_custom_resource_write(
            self.state,
            &resource,
            klights_reconcile_api::MutationOperation::Patch,
            self.patch_context,
            "custom_patch",
        )
        .await;
        crate::generic_command::finalize_after_update_if_ready(
            self.state,
            &info.kind,
            ns,
            self.target.name,
            &resource,
        )
        .await;
        let data = normalize_custom_resource_response_data(
            self.state.resource_mutation().identity.as_ref(),
            self.state.resource_mutation().resource_query.as_ref(),
            conversion.as_ref(),
            group,
            plural,
            &api_version,
            inject_resource_version_with_identity(
                self.state.resource_mutation().identity.as_ref(),
                resource.data,
                resource.resource_version,
            ),
        )
        .await?;
        Ok(WriteResult::Response {
            status: StatusCode::OK,
            body: data,
        })
    }
}

async fn create_cr_inner(
    state: &Arc<ApiState>,
    request: CustomResourceCreateRequest<'_>,
) -> Result<Response, AppError> {
    let CustomResourceCreateRequest {
        scope,
        query,
        body,
        log_context,
    } = request;
    let strategy = CustomResourceCreateStrategy {
        state,
        scope,
        query,
        log_context,
    };
    let dry_run = crate::generic_command::DryRunMode::from_create_update_query(query)?;
    let (status, data) = create_with_strategy(&strategy, body, dry_run)
        .await?
        .into_response_parts(
            state.resource_mutation().identity.as_ref(),
            StatusCode::CREATED,
        );
    Ok((status, Json(data)).into_response())
}

pub async fn create_custom_resource(
    State(state): State<Arc<ApiState>>,
    Extension(identity): Extension<AuthenticatedIdentity>,
    Path((group, version, namespace, plural)): Path<(String, String, String, String)>,
    Query(query): Query<CreateUpdateQuery>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    LenientJson(body): LenientJson<Value>,
) -> Result<Response, AppError> {
    let info = match lookup_crd_or_proxy(
        &state,
        &identity,
        CrdLookupRequest::new(&group, &version, &plural, Method::POST, &uri, &headers),
        || {
            serde_json::to_vec(&body)
                .map(Bytes::from)
                .map_err(|e| AppError::BadRequest(format!("Invalid APIService proxy body: {e}")))
        },
    )
    .await?
    {
        CrdLookup::Found(info) => info,
        CrdLookup::Proxied(resp) => return Ok(resp),
    };

    let resource_type = CustomResourceType::new(&info, &group, &version, &plural);
    let ns = if info.namespaced {
        Some(namespace.as_str())
    } else {
        None
    };
    create_cr_inner(
        &state,
        CustomResourceCreateRequest {
            scope: resource_type.scoped(ns, false),
            query: &query,
            body,
            log_context: "custom_create",
        },
    )
    .await
}

async fn delete_cr_inner(
    state: &Arc<ApiState>,
    request: CustomResourceDeleteRequest<'_>,
) -> Result<Response, AppError> {
    let CustomResourceDeleteRequest {
        target,
        query,
        body,
    } = request;
    let CustomResourceName { scope, name } = target;
    let CustomResourceScope {
        resource_type,
        namespace: ns,
        ..
    } = scope;
    let CustomResourceType {
        info,
        group,
        version,
        plural,
    } = resource_type;
    if name.trim().is_empty() {
        return Err(AppError::BadRequest("resource name required".to_string()));
    }

    let requested_api_version = resource_type.api_version();
    let delete_intent = crate::generic_command::DeleteIntent::from_query_and_body(query, &body)?;
    let is_dry_run = delete_intent.dry_run.is_all();
    let mut options_value =
        serde_json::to_value(&delete_intent.options).unwrap_or_else(|_| serde_json::json!({}));
    if let Some(obj) = options_value.as_object_mut() {
        obj.entry("apiVersion".to_string())
            .or_insert_with(|| serde_json::json!("v1"));
        obj.entry("kind".to_string())
            .or_insert_with(|| serde_json::json!("DeleteOptions"));
    }

    let conversion = load_crd_conversion_config(
        state.resource_mutation().resource_query.as_ref(),
        group,
        plural,
    )
    .await?;
    let (resource, stored_api_version) = get_existing_custom_resource_for_write(
        state,
        group,
        version,
        plural,
        &info.kind,
        ns.map(str::to_string),
        name,
    )
    .await?;

    crate::generic_command::ensure_delete_preconditions_match(
        &resource,
        &delete_intent.preconditions,
    )?;
    let _ = state
        .resource_mutation()
        .admission
        .admit(crate::generic_command::ResourceAdmissionRequest {
            api_version: requested_api_version.clone(),
            kind: info.kind.clone(),
            resource: None,
            operation: "DELETE".to_string(),
            namespace: ns.map(str::to_string),
            name: Some(name.to_string()),
            object: Value::Null,
            old_object: Some((*resource.data).clone()),
            dry_run: is_dry_run,
            subresource: None,
            options: Some(options_value),
        })
        .await?;

    if is_dry_run {
        return Ok(Json(crate::generic_command::delete_success_status(
            &info.kind, name,
        ))
        .into_response());
    }

    let target_identity = klights_types::ResourceKey::new(
        stored_api_version.clone(),
        info.kind.clone(),
        ns.map(str::to_string),
        name.to_string(),
    );
    let delete_strategy = crate::generic_command::FinalizerAwareDeleteStrategy {
        resource_query: state.resource_mutation().resource_query.as_ref(),
        lifecycle: state.resource_mutation().finalizer_lifecycle.as_ref(),
        operation_now: klights_auth::clock::chrono_utc(state.operational().clock.now()),
    };
    match crate::generic_command::delete_loaded_with_strategy(
        &delete_strategy,
        target_identity,
        resource,
        &delete_intent,
    )
    .await?
    {
        crate::generic_command::DeleteResult::MarkedTerminating(updated) => {
            dispatch_custom_resource_mutation_event(
                state,
                klights_reconcile_api::MutationOperation::DeleteMark,
                &updated.data,
                "custom_delete_mark",
            )
            .await;
            if let Err(e) = crate::current::gc_ports::finalize_foreground_owner(
                state.resource_mutation().gc_owner_lifecycle.as_ref(),
                updated.clone(),
            )
            .await
            {
                state
                    .controller_reconcile()
                    .metrics
                    .record_cascade_delete_failure();
                tracing::error!(name = %name, kind = %info.kind, error = %e, "CRD foreground finalize failed");
            }

            if let Some(latest) = crate::current::resource_query_ports::get_resource(
                state.resource_mutation().resource_query.as_ref(),
                &stored_api_version,
                &info.kind,
                ns,
                name,
            )
            .await?
            {
                let normalized = normalize_custom_resource_response_data(
                    state.resource_mutation().identity.as_ref(),
                    state.resource_mutation().resource_query.as_ref(),
                    conversion.as_ref(),
                    group,
                    plural,
                    &requested_api_version,
                    crate::generic_command::accepted_object(
                        state.resource_mutation().identity.as_ref(),
                        latest.data,
                        latest.resource_version,
                    ),
                )
                .await?;
                return Ok((StatusCode::ACCEPTED, Json(normalized)).into_response());
            }
        }
        crate::generic_command::DeleteResult::GoneOrUidChanged => {}
        crate::generic_command::DeleteResult::HardDeleted(deleted) => {
            dispatch_custom_resource_mutation_event(
                state,
                klights_reconcile_api::MutationOperation::HardDelete,
                &deleted.data,
                "custom_delete_hard_delete",
            )
            .await;
            if !delete_intent.orphan_children
                && let Err(e) = crate::current::gc_ports::cascade_delete(
                    state.resource_mutation().gc_owner_lifecycle.as_ref(),
                    klights_reconcile_api::GcOwnerIdentity::new(
                        &stored_api_version,
                        &info.kind,
                        ns.map(str::to_string),
                        &deleted.name,
                        &deleted.uid,
                    ),
                )
                .await
            {
                state
                    .controller_reconcile()
                    .metrics
                    .record_cascade_delete_failure();
                tracing::error!(name = %name, kind = %info.kind, error = %e, "CRD cascade delete failed");
            }
        }
    }

    Ok(Json(crate::generic_command::delete_success_status(
        &info.kind, name,
    ))
    .into_response())
}

pub async fn delete_custom_resource(
    State(state): State<Arc<ApiState>>,
    Extension(identity): Extension<AuthenticatedIdentity>,
    Path((group, version, namespace, plural, name)): Path<(String, String, String, String, String)>,
    Query(query): Query<CreateUpdateQuery>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, AppError> {
    let info = match lookup_crd_or_proxy(
        &state,
        &identity,
        CrdLookupRequest::new(&group, &version, &plural, Method::DELETE, &uri, &headers),
        || Ok(body.clone()),
    )
    .await?
    {
        CrdLookup::Found(info) => info,
        CrdLookup::Proxied(resp) => return Ok(resp),
    };
    let resource_type = CustomResourceType::new(&info, &group, &version, &plural);
    let ns = if info.namespaced {
        Some(namespace.as_str())
    } else {
        None
    };
    delete_cr_inner(
        &state,
        CustomResourceDeleteRequest {
            target: resource_type.named(ns, &name, false),
            query: &query,
            body,
        },
    )
    .await
}

async fn update_cr_inner(
    state: &Arc<ApiState>,
    request: CustomResourceUpdateRequest<'_>,
) -> Result<Response, AppError> {
    let CustomResourceUpdateRequest {
        target,
        body,
        log_context,
    } = request;
    if target.name.trim().is_empty() {
        return Err(AppError::BadRequest("resource name required".to_string()));
    }
    let strategy = CustomResourceUpdateStrategy {
        state,
        target,
        log_context,
    };
    let (status, data) =
        update_with_strategy(&strategy, body, crate::generic_command::DryRunMode::Live)
            .await?
            .into_response_parts(state.resource_mutation().identity.as_ref(), StatusCode::OK);
    Ok((status, Json(data)).into_response())
}

pub async fn update_custom_resource(
    State(state): State<Arc<ApiState>>,
    Extension(identity): Extension<AuthenticatedIdentity>,
    Path((group, version, namespace, plural, name)): Path<(String, String, String, String, String)>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    LenientJson(body): LenientJson<Value>,
) -> Result<Response, AppError> {
    let info = match lookup_crd_or_proxy(
        &state,
        &identity,
        CrdLookupRequest::new(&group, &version, &plural, Method::PUT, &uri, &headers),
        || {
            serde_json::to_vec(&body)
                .map(Bytes::from)
                .map_err(|e| AppError::BadRequest(format!("Invalid APIService proxy body: {e}")))
        },
    )
    .await?
    {
        CrdLookup::Found(info) => info,
        CrdLookup::Proxied(resp) => return Ok(resp),
    };

    let resource_type = CustomResourceType::new(&info, &group, &version, &plural);
    let ns = if info.namespaced {
        Some(namespace.as_str())
    } else {
        None
    };
    update_cr_inner(
        &state,
        CustomResourceUpdateRequest {
            target: resource_type.named(ns, &name, false),
            body,
            log_context: "custom_update",
        },
    )
    .await
}

async fn patch_cr_inner(
    state: &Arc<ApiState>,
    request: CustomResourcePatchRequest<'_>,
) -> Result<Response, AppError> {
    let CustomResourcePatchRequest {
        target,
        query,
        headers,
        body,
    } = request;
    if target.name.trim().is_empty() {
        return Err(AppError::BadRequest("resource name required".to_string()));
    }

    let (apply_create_context, patch_context) = if target.scope.is_cluster_scope {
        ("cluster_custom_apply_create", "cluster_custom_patch")
    } else {
        ("custom_apply_create", "custom_patch")
    };

    let content_type = headers.get("content-type").and_then(|h| h.to_str().ok());
    let is_apply_yaml = content_type == Some("application/apply-patch+yaml");

    let patch: Value = if body.len() >= 4 && &body[..4] == b"k8s\x00" {
        klights_kube_protobuf::decode_protobuf(&body[4..])
            .map_err(|e| AppError::BadRequest(format!("Failed to decode protobuf: {e}")))?
    } else if is_apply_yaml {
        parse_apply_yaml(&body)?
    } else {
        serde_json::from_slice(&body)
            .map_err(|e| AppError::BadRequest(format!("Invalid JSON: {e}")))?
    };

    let strategy = CustomResourcePatchStrategy {
        state,
        target,
        query,
        headers,
        apply_create_context,
        patch_context,
    };
    let dry_run = crate::generic_command::DryRunMode::from_create_update_query(query)?;
    let (status, data) = patch_with_strategy(&strategy, patch, dry_run)
        .await?
        .into_response_parts(state.resource_mutation().identity.as_ref(), StatusCode::OK);
    Ok((status, Json(data)).into_response())
}

pub async fn patch_custom_resource(
    State(state): State<Arc<ApiState>>,
    Extension(identity): Extension<AuthenticatedIdentity>,
    Path((group, version, namespace, plural, name)): Path<(String, String, String, String, String)>,
    Query(query): Query<CreateUpdateQuery>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, AppError> {
    let info = match lookup_crd_or_proxy(
        &state,
        &identity,
        CrdLookupRequest::new(&group, &version, &plural, Method::PATCH, &uri, &headers),
        || Ok(body.clone()),
    )
    .await?
    {
        CrdLookup::Found(info) => info,
        CrdLookup::Proxied(resp) => return Ok(resp),
    };

    let resource_type = CustomResourceType::new(&info, &group, &version, &plural);
    let ns = if info.namespaced {
        Some(namespace.as_str())
    } else {
        None
    };
    patch_cr_inner(
        &state,
        CustomResourcePatchRequest {
            target: resource_type.named(ns, &name, false),
            query: &query,
            headers: &headers,
            body,
        },
    )
    .await
}

pub async fn list_cluster_custom_resources(
    State(state): State<Arc<ApiState>>,
    Extension(identity): Extension<AuthenticatedIdentity>,
    headers: HeaderMap,
    Path((group, version, plural)): Path<(String, String, String)>,
    Query(query): Query<ListQuery>,
    OriginalUri(uri): OriginalUri,
) -> Result<Response, AppError> {
    let info = match lookup_crd_or_proxy(
        &state,
        &identity,
        CrdLookupRequest::new(&group, &version, &plural, Method::GET, &uri, &headers),
        || Ok(Bytes::new()),
    )
    .await?
    {
        CrdLookup::Found(info) => info,
        CrdLookup::Proxied(resp) => return Ok(resp),
    };
    let resource_type = CustomResourceType::new(&info, &group, &version, &plural);
    list_cr_inner(
        &state,
        CustomResourceListRequest {
            scope: resource_type.scoped(None, true),
            query: &query,
            headers: &headers,
        },
    )
    .await
}

pub async fn delete_collection_cluster_custom_resources(
    State(state): State<Arc<ApiState>>,
    Extension(identity): Extension<AuthenticatedIdentity>,
    headers: HeaderMap,
    Path((group, version, plural)): Path<(String, String, String)>,
    Query(query): Query<DeleteCollectionQuery>,
    OriginalUri(uri): OriginalUri,
    body: Bytes,
) -> Result<Response, AppError> {
    let info = match lookup_crd_or_proxy(
        &state,
        &identity,
        CrdLookupRequest::new(&group, &version, &plural, Method::DELETE, &uri, &headers),
        || Ok(body.clone()),
    )
    .await?
    {
        CrdLookup::Found(info) => info,
        CrdLookup::Proxied(resp) => return Ok(resp),
    };
    let resource_type = CustomResourceType::new(&info, &group, &version, &plural);
    delete_collection_cr_inner(
        &state,
        CustomResourceCollectionDeleteRequest {
            scope: resource_type.scoped(None, true),
            query: &query,
            body,
            log_context: "delete collection (cluster CRD)",
        },
    )
    .await
}

pub async fn create_cluster_custom_resource(
    State(state): State<Arc<ApiState>>,
    Extension(identity): Extension<AuthenticatedIdentity>,
    Path((group, version, plural)): Path<(String, String, String)>,
    Query(query): Query<CreateUpdateQuery>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    LenientJson(body): LenientJson<Value>,
) -> Result<Response, AppError> {
    // TokenReview is a special case handled by authentication_v1 handler,
    // not a custom resource — skip custom resource authz here.
    if group == "authentication.k8s.io"
        && (version == "v1" || version == "v1beta1")
        && plural == "tokenreviews"
    {
        let payload = serde_json::to_vec(&body)
            .map_err(|e| AppError::BadRequest(format!("Invalid TokenReview body: {e}")))?;
        let resp = create_token_review(State(state.clone()), headers, Bytes::from(payload)).await?;
        return Ok(resp.into_response());
    }

    let info = match lookup_crd_or_proxy(
        &state,
        &identity,
        CrdLookupRequest::new(&group, &version, &plural, Method::POST, &uri, &headers),
        || {
            serde_json::to_vec(&body)
                .map(Bytes::from)
                .map_err(|e| AppError::BadRequest(format!("Invalid APIService proxy body: {e}")))
        },
    )
    .await?
    {
        CrdLookup::Found(info) => info,
        CrdLookup::Proxied(resp) => return Ok(resp),
    };

    let resource_type = CustomResourceType::new(&info, &group, &version, &plural);
    create_cr_inner(
        &state,
        CustomResourceCreateRequest {
            scope: resource_type.scoped(None, true),
            query: &query,
            body,
            log_context: "cluster_custom_create",
        },
    )
    .await
}

pub async fn get_cluster_custom_resource(
    State(state): State<Arc<ApiState>>,
    Extension(identity): Extension<AuthenticatedIdentity>,
    headers: HeaderMap,
    Path((group, version, plural, name)): Path<(String, String, String, String)>,
    OriginalUri(uri): OriginalUri,
) -> Result<Response, AppError> {
    let info = match lookup_crd_or_proxy(
        &state,
        &identity,
        CrdLookupRequest::new(&group, &version, &plural, Method::GET, &uri, &headers),
        || Ok(Bytes::new()),
    )
    .await?
    {
        CrdLookup::Found(info) => info,
        CrdLookup::Proxied(resp) => return Ok(resp),
    };
    get_cr_inner(&state, &info, &group, &version, &plural, &name, None).await
}

pub async fn update_cluster_custom_resource(
    State(state): State<Arc<ApiState>>,
    Extension(identity): Extension<AuthenticatedIdentity>,
    Path((group, version, plural, name)): Path<(String, String, String, String)>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    LenientJson(body): LenientJson<Value>,
) -> Result<Response, AppError> {
    let info = match lookup_crd_or_proxy(
        &state,
        &identity,
        CrdLookupRequest::new(&group, &version, &plural, Method::PUT, &uri, &headers),
        || {
            serde_json::to_vec(&body)
                .map(Bytes::from)
                .map_err(|e| AppError::BadRequest(format!("Invalid APIService proxy body: {e}")))
        },
    )
    .await?
    {
        CrdLookup::Found(info) => info,
        CrdLookup::Proxied(resp) => return Ok(resp),
    };
    let resource_type = CustomResourceType::new(&info, &group, &version, &plural);
    update_cr_inner(
        &state,
        CustomResourceUpdateRequest {
            target: resource_type.named(None, &name, true),
            body,
            log_context: "cluster_custom_update",
        },
    )
    .await
}

pub async fn patch_cluster_custom_resource(
    State(state): State<Arc<ApiState>>,
    Extension(identity): Extension<AuthenticatedIdentity>,
    Path((group, version, plural, name)): Path<(String, String, String, String)>,
    Query(query): Query<CreateUpdateQuery>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, AppError> {
    let info = match lookup_crd_or_proxy(
        &state,
        &identity,
        CrdLookupRequest::new(&group, &version, &plural, Method::PATCH, &uri, &headers),
        || Ok(body.clone()),
    )
    .await?
    {
        CrdLookup::Found(info) => info,
        CrdLookup::Proxied(resp) => return Ok(resp),
    };
    let resource_type = CustomResourceType::new(&info, &group, &version, &plural);
    patch_cr_inner(
        &state,
        CustomResourcePatchRequest {
            target: resource_type.named(None, &name, true),
            query: &query,
            headers: &headers,
            body,
        },
    )
    .await
}

pub async fn delete_cluster_custom_resource(
    State(state): State<Arc<ApiState>>,
    Extension(identity): Extension<AuthenticatedIdentity>,
    Path((group, version, plural, name)): Path<(String, String, String, String)>,
    Query(query): Query<CreateUpdateQuery>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, AppError> {
    let info = match lookup_crd_or_proxy(
        &state,
        &identity,
        CrdLookupRequest::new(&group, &version, &plural, Method::DELETE, &uri, &headers),
        || Ok(body.clone()),
    )
    .await?
    {
        CrdLookup::Found(info) => info,
        CrdLookup::Proxied(resp) => return Ok(resp),
    };
    let resource_type = CustomResourceType::new(&info, &group, &version, &plural);
    delete_cr_inner(
        &state,
        CustomResourceDeleteRequest {
            target: resource_type.named(None, &name, true),
            query: &query,
            body,
        },
    )
    .await
}
