use super::*;

use crate::auth::identity::AuthenticatedIdentity;
use axum::Extension;

// Custom-resource authorization is enforced by the global `authorize_request`
// middleware chokepoint (see src/auth/middleware.rs). The CRD handlers still
// receive the authenticated identity because it is forwarded to APIService
// backends during aggregation proxying (see lookup_crd_or_proxy).

pub enum CrdLookup {
    Found(crate::controllers::crd::CrdResourceInfo),
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
    info: &'a crate::controllers::crd::CrdResourceInfo,
    group: &'a str,
    version: &'a str,
    plural: &'a str,
}

impl<'a> CustomResourceType<'a> {
    fn new(
        info: &'a crate::controllers::crd::CrdResourceInfo,
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
}

struct CustomResourceCollectionDeleteRequest<'a> {
    scope: CustomResourceScope<'a>,
    query: &'a DeleteCollectionQuery,
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
    state: &Arc<AppState>,
    identity: &AuthenticatedIdentity,
    request: CrdLookupRequest<'_>,
    body_for_proxy: impl FnOnce() -> Result<Bytes, AppError>,
) -> Result<CrdLookup, AppError> {
    if let Some(info) = state
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
    state: &Arc<AppState>,
    group: &str,
    version: &str,
    plural: &str,
    kind: &str,
    namespace: Option<String>,
    name: &str,
) -> Result<(Resource, String), AppError> {
    let requested_api_version = format!("{group}/{version}");
    if let Some(resource) = state
        .db
        .get_resource(
            &requested_api_version,
            kind,
            namespace.clone().as_deref(),
            name,
        )
        .await?
    {
        return Ok((resource, requested_api_version));
    }

    if let Some(conversion) = load_crd_conversion_config(state.db.as_ref(), group, plural).await? {
        for served_version in &conversion.served_versions {
            if served_version == version {
                continue;
            }
            let candidate_api_version = format!("{group}/{served_version}");
            if let Some(resource) = state
                .db
                .get_resource(
                    &candidate_api_version,
                    kind,
                    namespace.clone().as_deref(),
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
    conversion: Option<&crate::api::crd_conversion::CrdConversionConfig>,
) -> String {
    conversion
        .map(|conversion| format!("{}/{}", group, conversion.storage_version))
        .unwrap_or_else(|| format!("{}/{}", group, requested_version))
}

fn crd_watch_versions(
    conversion: Option<&crate::api::crd_conversion::CrdConversionConfig>,
    requested_version: &str,
) -> Vec<String> {
    let mut versions = std::collections::BTreeSet::new();
    versions.insert(requested_version.to_string());
    if let Some(conversion) = conversion {
        versions.insert(conversion.storage_version.clone());
        for version in &conversion.served_versions {
            versions.insert(version.clone());
        }
    }
    versions.into_iter().collect()
}

fn crd_watch_topics(
    group: &str,
    kind: &str,
    conversion: Option<&crate::api::crd_conversion::CrdConversionConfig>,
    requested_version: &str,
) -> Vec<crate::watch::WatchTopic> {
    crd_watch_versions(conversion, requested_version)
        .into_iter()
        .map(|version| crate::watch::WatchTopic::new(format!("{group}/{version}"), kind))
        .collect()
}

#[cfg(test)]
mod crd_watch_topic_tests {
    use super::*;

    #[test]
    fn conversion_crd_live_watch_topics_cover_all_served_versions() {
        let conversion = crate::api::crd_conversion::CrdConversionConfig {
            storage_version: "v1".to_string(),
            served_versions: vec!["v1".to_string(), "v2".to_string()],
            strategy: Some("Webhook".to_string()),
            webhook_client_config: None,
            webhook_review_versions: vec!["v1".to_string()],
        };

        let topics = crd_watch_topics(
            "stable.example.com",
            "SelectableFieldCrd",
            Some(&conversion),
            "v2",
        );

        assert!(
            topics.contains(&crate::watch::WatchTopic::new(
                "stable.example.com/v1",
                "SelectableFieldCrd",
            )),
            "a v2 CRD watch must subscribe to storage-version live events"
        );
        assert!(
            topics.contains(&crate::watch::WatchTopic::new(
                "stable.example.com/v2",
                "SelectableFieldCrd",
            )),
            "a v2 CRD watch must subscribe to requested-version live events"
        );
    }
}

async fn normalize_custom_resource_response_data(
    db: &dyn crate::datastore::DatastoreBackend,
    conversion: Option<&crate::api::crd_conversion::CrdConversionConfig>,
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
        db,
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
    db: &dyn crate::datastore::DatastoreBackend,
    conversion: Option<&crate::api::crd_conversion::CrdConversionConfig>,
    group: &str,
    plural: &str,
    storage_api_version: &str,
    data: Value,
) -> Result<Value, AppError> {
    let Some(conversion) = conversion else {
        return Ok(data);
    };
    convert_crd_objects_to_requested_version(
        db,
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
    state: &Arc<AppState>,
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

    if let Err(e) = controllers::gc::reconcile_owner_references(
        state.db.as_ref(),
        resource.clone(),
        state.pod_repository.as_ref() as &dyn crate::controllers::gc::GcPodDeleteSink,
    )
    .await
    {
        state
            .metrics
            .cascade_delete_failures_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
    state: &Arc<AppState>,
    info: &crate::controllers::crd::CrdResourceInfo,
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
    let conversion = load_crd_conversion_config(state.db.as_ref(), group, plural).await?;
    let mut resource_opt = state
        .db
        .get_resource(&api_version, &info.kind, ns, name)
        .await?;
    if resource_opt.is_none()
        && let Some(conversion) = conversion.as_ref()
    {
        for served_version in &conversion.served_versions {
            if served_version == version {
                continue;
            }
            let candidate = state
                .db
                .get_resource(
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
        state.db.as_ref(),
        conversion.as_ref(),
        group,
        plural,
        &api_version,
        data,
    )
    .await?;
    apply_crd_defaults(state.db.as_ref(), group, version, &info.kind, &mut data).await;
    Ok(Json(data).into_response())
}

pub async fn get_custom_resource(
    State(state): State<Arc<AppState>>,
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
    State(state): State<Arc<AppState>>,
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
    State(state): State<Arc<AppState>>,
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

/// Wrap a converted custom-resource object back into a [`Resource`] so the
/// conversion-backed list path can flow through the shared
/// [`crate::api::query::resolve_list_page`] helper. Only `data` is consumed
/// downstream (the unified item-render loop), but identity fields are populated
/// from the object for completeness.
fn synthetic_cr_resource(
    api_version: &str,
    kind: &str,
    data: Value,
    resource_version: i64,
) -> crate::datastore::Resource {
    let name = data
        .pointer("/metadata/name")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let namespace = data
        .pointer("/metadata/namespace")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let data = std::sync::Arc::new(data);
    crate::datastore::Resource {
        id: 0,
        api_version: api_version.to_string(),
        kind: kind.to_string(),
        namespace,
        name,
        uid: crate::datastore::Resource::uid_from_data(&data),
        resource_version,
        data,
    }
}

async fn list_cr_inner(
    state: &Arc<AppState>,
    request: CustomResourceListRequest<'_>,
) -> Result<Response, AppError> {
    let CustomResourceListRequest { scope, query } = request;
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
    validate_crd_field_selector(
        &api_version,
        plural,
        query.label_selector.as_deref(),
        query.field_selector.as_deref(),
        info.namespaced,
        &info.selectable_fields,
    )?;
    let conversion = load_crd_conversion_config(state.db.as_ref(), group, plural).await?;

    if query.watch == Some("true".to_string()) {
        let kind = info.kind.clone();
        let av = api_version.clone();
        let mut requested_rv: i64 = query
            .resource_version
            .as_ref()
            .and_then(|rv| rv.parse::<i64>().ok())
            .unwrap_or(0);

        let send_initial_events = query.send_initial_events.as_deref() == Some("true");
        let has_selector = query
            .label_selector
            .as_deref()
            .is_some_and(|s| !s.trim().is_empty())
            || query
                .field_selector
                .as_deref()
                .is_some_and(|s| !s.trim().is_empty());

        // Gap-free establishment floor captured BEFORE subscribing (see
        // `build_label_selector_watch_stream` for the full rationale). A
        // selector-less rv-less custom-resource watch starts "from now" — pin
        // `requested_rv` to the floor. A selector rv-less watch must instead
        // keep `requested_rv <= 0` so the stream emits existing matches as a
        // baseline ADDED list AND keeps the live-delivery floor at 0; the
        // baseline rvs are deduped via the cursor seed, and a numeric floor (a
        // pre-subscribe rv or the post-subscribe collection rv) would drop the
        // ADDED of a matching object whose rv is at/below the floor — the object
        // committed before the floor read but whose post-commit live broadcast
        // arrives below it — which is the flaky `[sig-api-machinery]
        // CustomResourceFieldSelectors MUST list and watch` conformance failure
        // under parallel load + replication latency.
        if requested_rv <= 0
            && !send_initial_events
            && !has_selector
            && let Ok(floor) = state.db.get_current_resource_version().await
            && floor > 0
        {
            requested_rv = floor;
        }

        let rx = state.db.subscribe_watch_many(crd_watch_topics(
            group,
            &kind,
            conversion.as_ref(),
            version,
        ));
        let db = state.db.clone();
        let send_bookmarks = query.allow_watch_bookmarks == Some("true".to_string());
        let task_supervisor = state.task_supervisor.clone();
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
        let conversion_for_watch = conversion.clone();
        let group_for_watch = group.to_string();
        let plural_for_watch = plural.to_string();
        let requested_version_for_watch = version.to_string();
        let log_prefix = if is_cluster_scope {
            "Cluster custom resource"
        } else {
            "Custom resource"
        };
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

        let stream = async_stream::stream! {
            let mut initial_list_rv = requested_rv;
            // Highest RV this watch has actually emitted for its scope. A scoped
            // watch BOOKMARK must never advertise an RV beyond this, or client-go
            // resuming from it skips still-undelivered in-scope events.
            let mut last_delivered_scoped_rv = requested_rv;
            let mut matched_selector_keys: std::collections::HashSet<(Option<String>, String)> =
                std::collections::HashSet::new();

            // rvs already emitted to the client as ADDED from the rv-less
            // selector baseline list below; used to seed the cursor so the
            // (intentionally low) live floor does not re-deliver them.
            let mut baseline_delivered_rvs: Vec<i64> = Vec::new();
            // Per-key low-rv exceptions for the current selector members of a
            // resourceVersion>0 watch; seeds the cursor so a below-floor live
            // transition (e.g. a replicated DELETED tombstone) still reaches the
            // client. Mirrors `build_label_selector_watch_stream`.
            let mut baseline_low_rv_allowlist: Vec<((Option<String>, String), i64)> = Vec::new();

            // Read-freshness: a follower can receive a WATCH whose resume RV was
            // minted on the leader; serving the catch-up below against
            // not-yet-applied follower state would miss events. Event-driven and
            // bounded; a no-op on a fresh node. Mirrors
            // `build_label_selector_watch_stream` (the built-in path) — the CR
            // builder previously omitted it, so a multinode CR watch resuming
            // from a leader-minted RV could serve catch-up against stale state.
            crate::api::watch_stream::wait_until_datastore_fresh(
                &db,
                requested_rv,
                crate::watch::WatchTopic::new(&av, &kind),
                &task_supervisor,
            )
            .await;

            // If the resume point predates the retained watch-event window, the
            // catch-up below (current state of modified resources) cannot replay
            // deletions that have aged out — the client would keep phantom
            // entries. Per Kubernetes "too old resource version" semantics, answer
            // 410 Gone up front so the reflector performs a fresh list+watch.
            // Mirrors the built-in path; the CR builder previously only surfaced
            // 410 reactively if the live cursor later hit Expired.
            if !send_initial_events
                && requested_rv > 0
                && let Ok(Some(earliest)) = db.earliest_watch_event_rv().await
                && requested_rv + 1 < earliest
            {
                yield Ok::<_, std::convert::Infallible>(
                    crate::api::watch_stream::serialize_watch_status_line(
                        410,
                        "Expired",
                        "too old resource version: requested resourceVersion is older than the watch history window",
                    ),
                );
                return;
            }

            if send_initial_events {
                // RV at which the initial collection snapshot was taken. Anchors
                // both the live-event floor and the `initial-events-end` bookmark
                // so a WatchList client can resume even when the initial list is
                // empty or fully filtered. Mirrors the built-in path's snapshot-RV
                // anchoring (`last_rv.max(list.resource_version)`).
                let mut send_initial_snapshot_rv = requested_rv;
                if let Some(conversion) = conversion_for_watch.as_ref() {
                    let initial_list = gather_custom_resources_across_served_versions(
                        db.as_ref(),
                        conversion,
                        &group_for_watch,
                        &kind,
                        watch_ns.clone(),
                        label_selector.clone(),
                    )
                    .await;
                    if let Ok((resources, snapshot_rv)) = initial_list {
                        let mut last_rv = 0i64;
                        let objects: Vec<Value> = resources
                            .into_iter()
                            .map(|resource| {
                                last_rv = last_rv.max(resource.resource_version);
                                std::sync::Arc::unwrap_or_clone(resource.data)
                            })
                            .collect();
                        match convert_crd_objects_to_requested_version(
                            db.as_ref(),
                            conversion,
                            &group_for_watch,
                            &plural_for_watch,
                            &av,
                            objects,
                        )
                        .await
                        {
                            Ok(converted) => {
                                for object in converted {
                                    let event = WatchEvent::added(object);
                                    let matches_selector = event.matches_filter_parsed(
                                        &kind,
                                        watch_ns.as_deref(),
                                        parsed_label_selector.as_ref(),
                                    ) && event.matches_field_selector(field_selector.as_deref());
                                    let Some(event) = apply_selector_transition_event(
                                        event,
                                        matches_selector,
                                        &mut matched_selector_keys,
                                    ) else {
                                        continue;
                                    };
                                    if let Some(delivered_rv) = event.resource_version() {
                                        last_delivered_scoped_rv =
                                            last_delivered_scoped_rv.max(delivered_rv);
                                    }
                                    let mut json = serde_json::to_vec(&event).unwrap_or_default();
                                    json.push(b'\n');
                                    yield Ok::<_, std::convert::Infallible>(json);
                                }
                            }
                            Err(err) => {
                                tracing::warn!(
                                    "{} watch initial conversion failed for {}.{}: {:?}",
                                    log_prefix,
                                    plural_for_watch,
                                    group_for_watch,
                                    err
                                );
                            }
                        }
                        let snap = snapshot_rv.max(last_rv);
                        initial_list_rv = initial_list_rv.max(snap);
                        send_initial_snapshot_rv = send_initial_snapshot_rv.max(snap);
                    }
                } else {
                    let initial_list = db.list_resources(&av, &kind, watch_ns.as_deref(), crate::datastore::ResourceListQuery::new(label_selector.as_deref(), field_selector.as_deref(), None, None)).await;
                    if let Ok(list) = initial_list {
                        let mut last_rv = 0i64;
                        for resource in list.items {
                            last_rv = last_rv.max(resource.resource_version);
                            let event = CatchUpResource {
                                resource,
                                event_type: std::borrow::Cow::Borrowed("ADDED"),
                            }
                            .into_watch_event();
                            let matches_selector = event.matches_filter_parsed(
                                &kind,
                                watch_ns.as_deref(),
                                parsed_label_selector.as_ref(),
                            ) && event.matches_field_selector(field_selector.as_deref());
                            let Some(event) = apply_selector_transition_event(
                                event,
                                matches_selector,
                                &mut matched_selector_keys,
                            ) else {
                                continue;
                            };
                            if let Some(delivered_rv) = event.resource_version() {
                                last_delivered_scoped_rv =
                                    last_delivered_scoped_rv.max(delivered_rv);
                            }
                            let mut json = serde_json::to_vec(&event).unwrap_or_default();
                            json.push(b'\n');
                            yield Ok::<_, std::convert::Infallible>(json);
                        }
                        let snap = last_rv.max(list.resource_version);
                        initial_list_rv = initial_list_rv.max(snap);
                        send_initial_snapshot_rv = send_initial_snapshot_rv.max(snap);
                    }
                }
                // Anchor the scoped resume floor to the snapshot RV and emit the
                // terminating `initial-events-end` bookmark so a WatchList client
                // learns the RV to resume from — required even when the initial
                // list was empty/filtered. The built-in path emits this; the CR
                // builder previously omitted it entirely.
                last_delivered_scoped_rv = last_delivered_scoped_rv.max(send_initial_snapshot_rv);
                initial_list_rv = initial_list_rv.max(send_initial_snapshot_rv);
                let bookmark =
                    WatchEvent::bookmark_initial_events_end(send_initial_snapshot_rv, &av, &kind);
                yield Ok::<_, std::convert::Infallible>(
                    crate::api::watch_stream::serialize_watch_event_line(bookmark, &kind, false),
                );
            } else if has_selector && requested_rv <= 0 {
                // rv-less selector custom-resource watch: emit existing matches
                // as a baseline ADDED list, mirroring
                // `build_label_selector_watch_stream`. This delivers a matching
                // object whose rv is at/below the establishment floor — it
                // committed before the floor read, so its post-commit live
                // broadcast arrives below the floor and would otherwise be
                // dropped (`rv <= floor`). That is the flaky
                // `[sig-api-machinery] CustomResourceFieldSelectors MUST list
                // and watch` conformance failure under parallel load +
                // replication latency. Reuse the catch-up path's conversion +
                // resourceVersion handling (since=0 lists every match), then
                // anchor the live floor to the pre-subscribe rv and dedup the
                // baseline rvs so the lowered floor cannot re-deliver them.
                let baseline = if let Some(conversion) = conversion_for_watch.as_ref() {
                    gather_custom_resource_events_across_served_versions(
                        db.as_ref(),
                        conversion,
                        &group_for_watch,
                        &kind,
                        watch_ns.clone(),
                        0,
                    )
                    .await
                } else {
                    db.list_resources_modified_since(&av, &kind, watch_ns.as_deref(), 0)
                        .await
                        .map_err(AppError::from)
                };
                if let Ok(baseline) = baseline {
                    for catchup in baseline {
                        let rv = catchup.resource.resource_version;
                        let event = CatchUpResource {
                            resource: catchup.resource,
                            event_type: std::borrow::Cow::Borrowed("ADDED"),
                        }
                        .into_watch_event();
                        let event = match convert_custom_resource_watch_event_to_requested_version(
                            db.as_ref(),
                            conversion_for_watch.as_ref(),
                            &group_for_watch,
                            &plural_for_watch,
                            &av,
                            event,
                        )
                        .await
                        {
                            Ok(event) => event,
                            Err(err) => {
                                tracing::warn!(
                                    "{} watch baseline conversion failed for {}.{}: {:?}",
                                    log_prefix,
                                    plural_for_watch,
                                    group_for_watch,
                                    err
                                );
                                continue;
                            }
                        };
                        let matches_selector = event.matches_filter_parsed(
                            &kind,
                            watch_ns.as_deref(),
                            parsed_label_selector.as_ref(),
                        ) && event.matches_field_selector(field_selector.as_deref());
                        let Some(event) = apply_selector_transition_event(
                            event,
                            matches_selector,
                            &mut matched_selector_keys,
                        ) else {
                            continue;
                        };
                        if let Some(delivered_rv) = event.resource_version() {
                            last_delivered_scoped_rv = last_delivered_scoped_rv.max(delivered_rv);
                        }
                        baseline_delivered_rvs.push(rv);
                        let mut json = serde_json::to_vec(&event).unwrap_or_default();
                        json.push(b'\n');
                        yield Ok::<_, std::convert::Infallible>(json);
                    }
                }
                // Keep the live-delivery floor at 0 (requested_rv <= 0 here): the
                // baseline items just emitted are deduped by exact rv via the
                // cursor seed below, so no numeric floor is needed. Anchoring the
                // floor to a pre-subscribe rv or a post-subscribe collection rv
                // can drop a genuinely live ADDED whose replicated commit
                // broadcasts after establishment with a lower rv — the same
                // regression fixed on the built-in path.
            } else if requested_rv > 0 {
                let missed = if let Some(conversion) = conversion_for_watch.as_ref() {
                    gather_custom_resource_events_across_served_versions(
                        db.as_ref(),
                        conversion,
                        &group_for_watch,
                        &kind,
                        watch_ns.clone(),
                        requested_rv,
                    )
                    .await
                } else {
                    db.list_resources_modified_since(&av, &kind, watch_ns.as_deref(), requested_rv)
                        .await
                        .map_err(AppError::from)
                };
                if let Ok(missed) = missed {
                    for catchup in missed {
                        let resource = catchup.resource.clone();
                        if resource.resource_version <= initial_list_rv { continue; }
                        initial_list_rv = initial_list_rv.max(resource.resource_version);
                        let event = CatchUpResource {
                            resource: catchup.resource,
                            event_type: catchup.event_type,
                        }
                        .into_watch_event();
                        let event = match convert_custom_resource_watch_event_to_requested_version(
                            db.as_ref(),
                            conversion_for_watch.as_ref(),
                            &group_for_watch,
                            &plural_for_watch,
                            &av,
                            event,
                        )
                        .await
                        {
                            Ok(event) => event,
                            Err(err) => {
                                tracing::warn!(
                                    "{} watch catch-up conversion failed for {}.{}: {:?}",
                                    log_prefix,
                                    plural_for_watch,
                                    group_for_watch,
                                    err
                                );
                                continue;
                            }
                        };
                        let matches_selector = event.matches_filter_parsed(
                            &kind,
                            watch_ns.as_deref(),
                            parsed_label_selector.as_ref(),
                        ) && event.matches_field_selector(field_selector.as_deref());
                        let Some(event) = apply_selector_transition_event(
                            event,
                            matches_selector,
                            &mut matched_selector_keys,
                        ) else {
                            continue;
                        };
                        if let Some(delivered_rv) = event.resource_version() {
                            last_delivered_scoped_rv = last_delivered_scoped_rv.max(delivered_rv);
                        }
                        let mut json = serde_json::to_vec(&event).unwrap_or_default();
                        json.push(b'\n');
                        yield Ok::<_, std::convert::Infallible>(json);
                    }
                }

                // Register the current selector members and grant each a per-key
                // low-rv exception, mirroring `build_label_selector_watch_stream`.
                // Without this, a resourceVersion>0 selector CR watch never
                // tracks baseline membership, so a later below-floor transition
                // (e.g. a replicated DELETED tombstone broadcast with rv <
                // requested_rv) is swallowed — the client keeps a phantom member.
                if has_selector {
                    let members = if let Some(conversion) = conversion_for_watch.as_ref() {
                        gather_custom_resources_across_served_versions(
                            db.as_ref(),
                            conversion,
                            &group_for_watch,
                            &kind,
                            watch_ns.clone(),
                            label_selector.clone(),
                        )
                        .await
                        .map(|(items, _)| items)
                    } else {
                        db.list_resources(
                            &av,
                            &kind,
                            watch_ns.as_deref(),
                            crate::datastore::ResourceListQuery::new(
                                label_selector.as_deref(),
                                field_selector.as_deref(),
                                None,
                                None,
                            ),
                        )
                        .await
                        .map(|list| list.items)
                        .map_err(AppError::from)
                    };
                    if let Ok(members) = members {
                        for resource in members {
                            // `gather` applies only the label selector; the field
                            // selector still has to be matched for conversion CRDs
                            // (the non-conversion list already applied both).
                            if conversion_for_watch.is_some()
                                && !crate::api::watch_stream::object_matches_field_selector(
                                    &resource.data,
                                    field_selector.as_deref(),
                                )
                            {
                                continue;
                            }
                            let key = crate::api::watch_stream::resource_to_seen_key(&resource);
                            matched_selector_keys.insert(key.clone());
                            baseline_low_rv_allowlist.push((key, resource.resource_version));
                        }
                    }
                }
            }

            let watch_versions =
                crd_watch_versions(conversion_for_watch.as_ref(), &requested_version_for_watch);
            let replay_targets = if is_cluster_scope {
                watch_versions
                    .iter()
                    .map(|version| {
                        let watched_api_version = format!("{}/{}", group_for_watch, version);
                        WatchTarget::cluster(watched_api_version, kind.clone())
                    })
                    .collect::<Vec<_>>()
            } else {
                watch_versions
                    .iter()
                    .map(|version| {
                        let watched_api_version = format!("{}/{}", group_for_watch, version);
                        if let Some(ns) = watch_ns.as_ref() {
                            WatchTarget::namespaced_in_namespace(
                                watched_api_version,
                                kind.clone(),
                                ns.clone(),
                            )
                        } else {
                            WatchTarget::namespaced(watched_api_version, kind.clone())
                        }
                    })
                    .collect::<Vec<_>>()
            };
            let replay_source = DatastoreWatchReplaySource::new(
                db.clone(),
                replay_targets,
            );
            let mut cursor = WatchCursor::new(rx, replay_source, initial_list_rv.max(requested_rv));
            // Dedup baseline ADDEDs and grant per-key low-rv exceptions; shared
            // with the built-in watch builder via `seed_watch_cursor_baseline`.
            crate::api::watch_stream::seed_watch_cursor_baseline(
                &mut cursor,
                baseline_delivered_rvs,
                baseline_low_rv_allowlist,
            );
            let bookmark_task_name = format!(
                "{}_watch_bookmarks_{}_{}",
                task_prefix, group_for_watch, plural_for_watch
            );
            let mut bookmark_ticks = maybe_spawn_bookmark_tick_stream(
                send_bookmarks,
                task_supervisor.clone(),
                bookmark_task_name,
            )
            .await;
            let timeout_task_name = format!(
                "{}_watch_timeout_{}_{}",
                task_prefix, group_for_watch, plural_for_watch
            );
            let mut timeout_tick = maybe_spawn_watch_timeout_stream(
                timeout_seconds,
                task_supervisor.clone(),
                timeout_task_name,
            )
            .await;

            loop {
                tokio::select! {
                    Some(()) = recv_watch_timeout(&mut timeout_tick) => {
                        break;
                    }
                    result = cursor.next_event(&task_supervisor) => {
                        let event = match result {
                            Ok(event) => event,
                            Err(WatchCursorError::Replay(err)) => {
                                tracing::warn!("{} watch replay failed for {}: {:#}", log_prefix, kind, err);
                                continue;
                            }
                            Err(WatchCursorError::Expired) => {
                                // Watch fell behind the retained history window;
                                // emit 410 Gone so the client reflector relists.
                                yield Ok::<_, std::convert::Infallible>(
                                    crate::api::watch_stream::serialize_watch_status_line(
                                        410,
                                        "Expired",
                                        "too old resource version: watch fell behind the history window",
                                    ),
                                );
                                break;
                            }
                            Err(WatchCursorError::Closed) => break,
                        };
                        if !event.matches_filter(&kind, watch_ns.as_deref(), None) {
                            continue;
                        }
                        let event = match convert_custom_resource_watch_event_to_requested_version(
                            db.as_ref(),
                            conversion_for_watch.as_ref(),
                            &group_for_watch,
                            &plural_for_watch,
                            &av,
                            event,
                        )
                        .await
                        {
                            Ok(event) => event,
                            Err(err) => {
                                tracing::warn!(
                                    "{} watch event conversion failed for {}.{}: {:?}",
                                    log_prefix,
                                    plural_for_watch,
                                    group_for_watch,
                                    err
                                );
                                continue;
                            }
                        };
                        let matches_selector = event.matches_filter_parsed(
                            &kind,
                            watch_ns.as_deref(),
                            parsed_label_selector.as_ref(),
                        ) && event.matches_field_selector(field_selector.as_deref());
                        let Some(event) = apply_selector_transition_event(
                            event,
                            matches_selector,
                            &mut matched_selector_keys,
                        ) else {
                            continue;
                        };
                        if let Some(delivered_rv) = event.resource_version() {
                            last_delivered_scoped_rv = last_delivered_scoped_rv.max(delivered_rv);
                        }
                        let mut json = serde_json::to_vec(&event).unwrap_or_default();
                        json.push(b'\n');
                        yield Ok::<_, std::convert::Infallible>(json);
                    }
                    Some(()) = recv_bookmark_tick(&mut bookmark_ticks), if send_bookmarks => {
                        let rv = crate::api::watch_stream::resolve_periodic_bookmark_rv(
                            crate::api::watch_stream::PeriodicBookmarkContext {
                                db: &db,
                                api_version: &av,
                                kind: &kind,
                                watch_namespace: watch_ns.as_deref(),
                                label_selector: label_selector.as_deref(),
                                field_selector: field_selector.as_deref(),
                                requested_rv,
                                has_scope_filter,
                                cursor_high_water_rv: cursor.high_water_rv(),
                                last_delivered_scoped_rv,
                            },
                        )
                        .await;
                        let event = WatchEvent::bookmark_typed(rv, &av, &kind);
                        yield Ok::<_, std::convert::Infallible>(
                            crate::api::watch_stream::serialize_watch_event_line(event, &kind, false),
                        );
                    }
                }
            }
        };

        let body = Body::from_stream(stream);
        return Ok(Response::builder()
            .header("Content-Type", "application/json")
            .header("Transfer-Encoding", "chunked")
            .body(body)
            .unwrap());
    }

    let normalized_limit = query.normalized_limit()?;
    let has_continue = query
        .continue_token
        .as_deref()
        .is_some_and(|t| !t.is_empty());
    let rv_match = query.resolve_resource_version_match(has_continue)?;
    let (db_continue_name, continue_resource_version) =
        process_continue_token(query.continue_token.clone())?;

    let needs_conversion = conversion
        .as_ref()
        .is_some_and(|c| c.served_versions.len() > 1 || c.strategy.as_deref() == Some("Webhook"));

    let list_query = crate::datastore::ResourceListQuery::new(
        query.label_selector.as_deref(),
        query.field_selector.as_deref(),
        normalized_limit,
        db_continue_name.as_deref(),
    );

    // Shared consistent-snapshot selection. Non-conversion CRDs live in the
    // generic resource table and pin a real historical snapshot just like the
    // core kinds. Conversion-backed CRDs build a merged cross-version view
    // client-side and cannot pin a historical snapshot, so they report `Expired`
    // to opt into the inconsistent-continuation fallback (Exact => 410). See
    // `query::resolve_list_page`.
    let crate::api::query::ResolvedListPage {
        list,
        response_rv,
        continue_resource_version,
    } = if needs_conversion {
        let conv = conversion
            .clone()
            .expect("needs_conversion implies conversion is Some");
        let state_conv = state.clone();
        let group_owned = group.to_string();
        let plural_owned = plural.to_string();
        let api_version_owned = api_version.clone();
        let kind_owned = info.kind.clone();
        crate::api::query::resolve_list_page(
            state.db.as_ref(),
            rv_match,
            continue_resource_version,
            |_srv| async { Ok(crate::datastore::SnapshotAtRv::Expired) },
            || async move {
                let (resources, rv) = gather_custom_resources_across_served_versions(
                    state_conv.db.as_ref(),
                    &conv,
                    &group_owned,
                    &kind_owned,
                    ns.map(str::to_string),
                    list_query.label_selector.map(str::to_string),
                )
                .await?;

                let mut objects: Vec<Value> = resources
                    .into_iter()
                    .map(|r| std::sync::Arc::unwrap_or_clone(r.data))
                    .collect();
                objects = convert_crd_objects_to_requested_version(
                    state_conv.db.as_ref(),
                    &conv,
                    &group_owned,
                    &plural_owned,
                    &api_version_owned,
                    objects,
                )
                .await?;
                objects.retain(|object| {
                    object_matches_field_selector(object, list_query.field_selector)
                });

                // Conversion-backed CRDs: stable sort by name, then apply
                // client-side pagination after the merged view is built.
                objects.sort_by(|a, b| {
                    let na = a
                        .pointer("/metadata/name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    let nb = b
                        .pointer("/metadata/name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    na.cmp(nb)
                });

                // Apply continue token offset by name.
                let start_offset = match list_query.continue_token {
                    Some(name) => objects.partition_point(|o| {
                        o.pointer("/metadata/name")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            <= name
                    }),
                    None => 0,
                };
                let sliced = if start_offset < objects.len() {
                    &objects[start_offset..]
                } else {
                    &[]
                };

                let (page, cont, remaining) = if let Some(lim) = list_query.limit {
                    if sliced.len() > lim as usize {
                        let last_name = sliced[lim as usize - 1]
                            .pointer("/metadata/name")
                            .and_then(|v| v.as_str())
                            .unwrap_or("");
                        (
                            sliced[..lim as usize].to_vec(),
                            Some(last_name.to_string()),
                            None, // Exact remaining count would require converting all remaining objects
                        )
                    } else {
                        (sliced.to_vec(), None, None)
                    }
                } else {
                    (sliced.to_vec(), None, None)
                };

                let items = page
                    .into_iter()
                    .map(|data| synthetic_cr_resource(&api_version_owned, &kind_owned, data, rv))
                    .collect();
                Ok(crate::datastore::ResourceList {
                    items,
                    resource_version: rv,
                    continue_token: cont,
                    remaining_item_count: remaining,
                })
            },
        )
        .await?
    } else {
        let db_for_snapshot = state.db.clone();
        let db_for_live = state.db.clone();
        let av_snap = api_version.clone();
        let av_live = api_version.clone();
        let kind_snap = info.kind.clone();
        let kind_live = info.kind.clone();
        crate::api::query::resolve_list_page(
            state.db.as_ref(),
            rv_match,
            continue_resource_version,
            |srv| async move {
                db_for_snapshot
                    .snapshot_resources_at_rv(&av_snap, &kind_snap, ns, list_query, srv)
                    .await
                    .map_err(AppError::from)
            },
            || async move {
                db_for_live
                    .list_resources(&av_live, &kind_live, ns, list_query)
                    .await
                    .map_err(AppError::from)
            },
        )
        .await?
    };

    // Unified item rendering: CRD defaults are applied to every served object,
    // whether it came from a live list, a pinned snapshot, or a converted view.
    let mut items: Vec<Value> = Vec::with_capacity(list.items.len());
    for r in list.items {
        let mut data = std::sync::Arc::unwrap_or_clone(r.data);
        apply_crd_defaults(state.db.as_ref(), group, version, &info.kind, &mut data).await;
        items.push(data);
    }
    let continue_token = list.continue_token;
    let remaining_item_count = list.remaining_item_count;
    let mut metadata = serde_json::json!({
        "resourceVersion": response_rv.to_string()
    });
    if let Some(ct) = continue_token {
        metadata["continue"] =
            serde_json::Value::String(crate::api::query::encode_response_continue_token(
                &ct,
                response_rv,
                continue_resource_version,
            ));
    }
    if let Some(ric) = remaining_item_count {
        metadata["remainingItemCount"] = serde_json::Value::Number(ric.into());
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
    State(state): State<Arc<AppState>>,
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
        },
    )
    .await
}

async fn delete_collection_cr_inner(
    state: &Arc<AppState>,
    request: CustomResourceCollectionDeleteRequest<'_>,
) -> Result<Response, AppError> {
    let CustomResourceCollectionDeleteRequest {
        scope,
        query,
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

    let conversion = load_crd_conversion_config(state.db.as_ref(), group, plural).await?;

    let mut names = Vec::new();
    if let Some(conversion) = conversion.as_ref() {
        let (resources, _) = gather_custom_resources_across_served_versions(
            state.db.as_ref(),
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
            state.db.as_ref(),
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
        let list = state
            .db
            .list_resources(
                &api_version,
                &info.kind,
                ns,
                crate::datastore::ResourceListQuery::new(
                    query.label_selector.as_deref(),
                    query.field_selector.as_deref(),
                    None,
                    None,
                ),
            )
            .await?;
        names.extend(list.items.into_iter().map(|resource| resource.name));
    }

    let mut unique_names = std::collections::HashSet::new();
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

        let owner_uid = current
            .data
            .pointer("/metadata/uid")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        state
            .db
            .delete_resource(&stored_api_version, &info.kind, ns, &name)
            .await?;

        if let Err(e) = controllers::gc::cascade_delete_with_uid(
            state.db.as_ref(),
            &owner_uid,
            &stored_api_version,
            &name,
            &info.kind,
            ns.map(str::to_string),
            state.pod_repository.as_ref() as &dyn crate::controllers::gc::GcPodDeleteSink,
        )
        .await
        {
            state
                .metrics
                .cascade_delete_failures_total
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            tracing::error!(name = %name, kind = %info.kind, error = %e, "{log_context}: cascade delete failed");
        }
    }

    Ok(Json(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Status",
        "status": "Success",
        "code": 200
    }))
    .into_response())
}

pub async fn delete_collection_custom_resources(
    State(state): State<Arc<AppState>>,
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
            log_context: "delete collection (CRD)",
        },
    )
    .await
}

async fn create_cr_inner(
    state: &Arc<AppState>,
    request: CustomResourceCreateRequest<'_>,
) -> Result<Response, AppError> {
    let CustomResourceCreateRequest {
        scope,
        query,
        mut body,
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
    let is_dry_run = query.dry_run == Some("All".to_string());
    let conversion = load_crd_conversion_config(state.db.as_ref(), group, plural).await?;
    let storage_api_version = storage_api_version_for_request(group, version, conversion.as_ref());

    apply_crd_defaults(state.db.as_ref(), group, version, &info.kind, &mut body).await;

    if query.field_validation.as_deref() == Some("Strict") {
        check_cr_field_validation_strict(state.db.as_ref(), group, version, &info.kind, &body)
            .await?;
    }

    body = run_admission_for_request(
        state.db.as_ref(),
        build_admission_context(AdmissionContextRequest {
            api_version: &api_version,
            kind: &info.kind,
            operation: "CREATE",
            namespace: ns.map(str::to_string),
            name: body
                .get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(|n| n.as_str())
                .map(ToString::to_string),
            object: body,
            old_object: None,
            dry_run: is_dry_run,
            subresource: None,
            options: None,
        }),
    )
    .await?;
    apply_crd_pruning(state.db.as_ref(), group, version, &info.kind, &mut body).await;

    if is_dry_run {
        return Ok((StatusCode::CREATED, Json(body)).into_response());
    }

    let name = body["metadata"]["name"]
        .as_str()
        .ok_or_else(|| AppError::BadRequest("metadata.name required".to_string()))?
        .to_string();

    if get_existing_custom_resource_for_write(
        state,
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
        state.db.as_ref(),
        conversion.as_ref(),
        group,
        plural,
        &storage_api_version,
        body,
    )
    .await?;

    let resource = state
        .db
        .create_resource(&storage_api_version, &info.kind, ns, &name, storage_body)
        .await?;

    reconcile_custom_resource_owner_refs(state, &resource, log_context).await;

    let data = normalize_custom_resource_response_data(
        state.db.as_ref(),
        conversion.as_ref(),
        group,
        plural,
        &api_version,
        inject_resource_version(resource.data, resource.resource_version),
    )
    .await?;
    Ok((StatusCode::CREATED, Json(data)).into_response())
}

pub async fn create_custom_resource(
    State(state): State<Arc<AppState>>,
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
    state: &Arc<AppState>,
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
    let delete_options = parse_delete_options_body(&body);
    let is_dry_run = query.dry_run == Some("All".to_string());
    let mut options_value =
        serde_json::to_value(&delete_options).unwrap_or_else(|_| serde_json::json!({}));
    if let Some(obj) = options_value.as_object_mut() {
        obj.entry("apiVersion".to_string())
            .or_insert_with(|| serde_json::json!("v1"));
        obj.entry("kind".to_string())
            .or_insert_with(|| serde_json::json!("DeleteOptions"));
    }

    let propagation_policy = delete_options
        .propagation_policy
        .as_deref()
        .or(query.propagation_policy.as_deref())
        .unwrap_or("Background");
    let orphan = propagation_policy == "Orphan"
        || delete_options.orphan_dependents == Some(true)
        || query.orphan_dependents == Some(true);

    let conversion = load_crd_conversion_config(state.db.as_ref(), group, plural).await?;
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

    let data_with_uid = inject_resource_version(resource.data.clone(), resource.resource_version);
    let _ = run_admission_for_request(
        state.db.as_ref(),
        build_admission_context(AdmissionContextRequest {
            api_version: &requested_api_version,
            kind: &info.kind,
            operation: "DELETE",
            namespace: ns.map(str::to_string),
            name: Some(name.to_string()),
            object: Value::Null,
            old_object: Some((*resource.data).clone()),
            dry_run: is_dry_run,
            subresource: None,
            options: Some(options_value),
        }),
    )
    .await?;

    if is_dry_run {
        return Ok(Json(serde_json::json!({
            "kind": "Status",
            "apiVersion": "v1",
            "metadata": {},
            "status": "Success",
            "details": {
                "name": name,
                "kind": info.kind,
            }
        }))
        .into_response());
    }

    let owner_uid = data_with_uid
        .get("metadata")
        .and_then(|m| m.get("uid"))
        .and_then(|u| u.as_str())
        .unwrap_or("");

    match (orphan, propagation_policy) {
        (false, "Foreground") => {
            let updated = crate::api::finalizer_delete::mark_foreground_deletion_with_retry(
                state.db.as_ref(),
                &stored_api_version,
                &info.kind,
                ns,
                name,
                resource,
                crate::datastore::ResourcePreconditions::uid(owner_uid.to_string()),
            )
            .await?;

            if let Err(e) = controllers::gc::finalize_foreground_owner_if_ready(
                state.db.as_ref(),
                &updated,
                state.pod_repository.as_ref() as &dyn crate::controllers::gc::GcPodDeleteSink,
            )
            .await
            {
                state
                    .metrics
                    .cascade_delete_failures_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::error!(name = %name, kind = %info.kind, error = %e, "CRD foreground finalize failed");
            }

            if let Some(latest) = state
                .db
                .get_resource(&stored_api_version, &info.kind, ns, name)
                .await?
            {
                let normalized = normalize_custom_resource_response_data(
                    state.db.as_ref(),
                    conversion.as_ref(),
                    group,
                    plural,
                    &requested_api_version,
                    inject_resource_version(latest.data, latest.resource_version),
                )
                .await?;
                return Ok(Json(normalized).into_response());
            }
        }
        (_, _) => {
            let grace_seconds = delete_options._grace_period_seconds.unwrap_or(0);
            let outcome =
                crate::api::finalizer_delete::complete_non_foreground_delete_with_live_recheck(
                    state.db.as_ref(),
                    crate::api::finalizer_delete::NonForegroundDeleteRequest {
                        target: crate::api::finalizer_delete::ResourceDeleteTarget {
                            api_version: &stored_api_version,
                            kind: &info.kind,
                            namespace: ns,
                            name,
                        },
                        initial_resource: resource,
                        delete_preconditions: crate::datastore::ResourcePreconditions::uid(
                            owner_uid.to_string(),
                        ),
                        orphan_children_before_completion: orphan,
                        uid_mismatch_is_conflict: false,
                        grace_seconds,
                    },
                )
                .await?;

            match outcome {
                crate::api::finalizer_delete::DeleteCompletion::MarkedTerminating(updated) => {
                    let normalized = normalize_custom_resource_response_data(
                        state.db.as_ref(),
                        conversion.as_ref(),
                        group,
                        plural,
                        &requested_api_version,
                        inject_resource_version(updated.data, updated.resource_version),
                    )
                    .await?;
                    return Ok(Json(normalized).into_response());
                }
                crate::api::finalizer_delete::DeleteCompletion::GoneOrUidChanged => {}
                crate::api::finalizer_delete::DeleteCompletion::HardDeleted(deleted) => {
                    if !orphan
                        && let Err(e) = controllers::gc::cascade_delete_with_uid(
                            state.db.as_ref(),
                            &deleted.uid,
                            &stored_api_version,
                            &deleted.name,
                            &info.kind,
                            ns.map(str::to_string),
                            state.pod_repository.as_ref()
                                as &dyn crate::controllers::gc::GcPodDeleteSink,
                        )
                        .await
                    {
                        state
                            .metrics
                            .cascade_delete_failures_total
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        tracing::error!(name = %name, kind = %info.kind, error = %e, "CRD cascade delete failed");
                    }
                }
            }
        }
    }

    Ok(Json(serde_json::json!({
        "kind": "Status",
        "apiVersion": "v1",
        "metadata": {},
        "status": "Success",
        "details": {
            "name": name,
            "kind": info.kind,
        }
    }))
    .into_response())
}

pub async fn delete_custom_resource(
    State(state): State<Arc<AppState>>,
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
    state: &Arc<AppState>,
    request: CustomResourceUpdateRequest<'_>,
) -> Result<Response, AppError> {
    let CustomResourceUpdateRequest {
        target,
        mut body,
        log_context,
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

    let api_version = resource_type.api_version();
    let conversion = load_crd_conversion_config(state.db.as_ref(), group, plural).await?;
    let (current, stored_api_version) = get_existing_custom_resource_for_write(
        state,
        group,
        version,
        plural,
        &info.kind,
        ns.map(str::to_string),
        name,
    )
    .await?;

    body = run_admission_for_request(
        state.db.as_ref(),
        build_admission_context(AdmissionContextRequest {
            api_version: &api_version,
            kind: &info.kind,
            operation: "UPDATE",
            namespace: ns.map(str::to_string),
            name: Some(name.to_string()),
            object: body,
            old_object: Some((*current.data).clone()),
            dry_run: false,
            subresource: None,
            options: None,
        }),
    )
    .await?;
    apply_crd_pruning(state.db.as_ref(), group, version, &info.kind, &mut body).await;
    crate::api::finalizer_delete::preserve_deletion_timestamp_on_update(&current.data, &mut body);

    let storage_body = normalize_custom_resource_storage_data(
        state.db.as_ref(),
        conversion.as_ref(),
        group,
        plural,
        &stored_api_version,
        body,
    )
    .await?;

    let resource = state
        .db
        .update_resource(
            &stored_api_version,
            &info.kind,
            ns,
            name,
            storage_body,
            current.resource_version,
        )
        .await?;

    reconcile_custom_resource_owner_refs(state, &resource, log_context).await;
    crate::api::finalizer_delete::finalize_after_update_if_ready(
        state,
        &stored_api_version,
        &info.kind,
        ns,
        name,
        &resource,
    )
    .await;

    let data = normalize_custom_resource_response_data(
        state.db.as_ref(),
        conversion.as_ref(),
        group,
        plural,
        &api_version,
        inject_resource_version(resource.data, resource.resource_version),
    )
    .await?;
    Ok(Json(data).into_response())
}

pub async fn update_custom_resource(
    State(state): State<Arc<AppState>>,
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
    state: &Arc<AppState>,
    request: CustomResourcePatchRequest<'_>,
) -> Result<Response, AppError> {
    let CustomResourcePatchRequest {
        target,
        query,
        headers,
        body,
    } = request;
    let CustomResourceName { scope, name } = target;
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
    if name.trim().is_empty() {
        return Err(AppError::BadRequest("resource name required".to_string()));
    }

    let (apply_create_ctx, patch_ctx) = if is_cluster_scope {
        ("cluster_custom_apply_create", "cluster_custom_patch")
    } else {
        ("custom_apply_create", "custom_patch")
    };

    let api_version = resource_type.api_version();
    let content_type = headers.get("content-type").and_then(|h| h.to_str().ok());
    let is_apply_yaml = content_type == Some("application/apply-patch+yaml");
    let is_dry_run = query.dry_run == Some("All".to_string());
    let conversion = load_crd_conversion_config(state.db.as_ref(), group, plural).await?;
    let storage_api_version = storage_api_version_for_request(group, version, conversion.as_ref());

    let patch: Value = if body.len() >= 4 && &body[..4] == b"k8s\x00" {
        crate::protobuf::decode_protobuf(&body[4..])
            .map_err(|e| AppError::BadRequest(format!("Failed to decode protobuf: {e}")))?
    } else if is_apply_yaml {
        parse_apply_yaml(&body)?
    } else {
        serde_json::from_slice(&body)
            .map_err(|e| AppError::BadRequest(format!("Invalid JSON: {e}")))?
    };

    if is_apply_yaml && query.field_validation.as_deref() == Some("Strict") {
        check_cr_field_validation_strict(state.db.as_ref(), group, version, &info.kind, &patch)
            .await?;
    }

    let existing = get_existing_custom_resource_for_write(
        state,
        group,
        version,
        plural,
        &info.kind,
        ns.map(str::to_string),
        name,
    )
    .await;
    let (current, stored_api_version) = match existing {
        Ok(existing) => existing,
        Err(AppError::NotFound(_)) if is_apply_yaml => {
            let mut created_resource = patch.clone();
            apply_crd_defaults(
                state.db.as_ref(),
                group,
                version,
                &info.kind,
                &mut created_resource,
            )
            .await;
            created_resource = run_admission_for_request(
                state.db.as_ref(),
                build_admission_context(AdmissionContextRequest {
                    api_version: &api_version,
                    kind: &info.kind,
                    operation: "CREATE",
                    namespace: ns.map(str::to_string),
                    name: Some(name.to_string()),
                    object: created_resource,
                    old_object: None,
                    dry_run: is_dry_run,
                    subresource: None,
                    options: None,
                }),
            )
            .await?;
            apply_crd_pruning(
                state.db.as_ref(),
                group,
                version,
                &info.kind,
                &mut created_resource,
            )
            .await;

            if is_dry_run {
                return Ok((StatusCode::CREATED, Json(created_resource)).into_response());
            }

            let storage_created_resource = normalize_custom_resource_storage_data(
                state.db.as_ref(),
                conversion.as_ref(),
                group,
                plural,
                &storage_api_version,
                created_resource,
            )
            .await?;

            let resource = state
                .db
                .create_resource(
                    &storage_api_version,
                    &info.kind,
                    ns,
                    name,
                    storage_created_resource,
                )
                .await?;
            reconcile_custom_resource_owner_refs(state, &resource, apply_create_ctx).await;
            let data = normalize_custom_resource_response_data(
                state.db.as_ref(),
                conversion.as_ref(),
                group,
                plural,
                &api_version,
                inject_resource_version(resource.data, resource.resource_version),
            )
            .await?;
            return Ok((StatusCode::CREATED, Json(data)).into_response());
        }
        Err(err) => return Err(err),
    };

    let mut patched_resource = apply_patch(&current.data, &patch, content_type)?;
    patched_resource = run_admission_for_request(
        state.db.as_ref(),
        build_admission_context(AdmissionContextRequest {
            api_version: &api_version,
            kind: &info.kind,
            operation: "UPDATE",
            namespace: ns.map(str::to_string),
            name: Some(name.to_string()),
            object: patched_resource,
            old_object: Some((*current.data).clone()),
            dry_run: is_dry_run,
            subresource: None,
            options: None,
        }),
    )
    .await?;
    apply_crd_pruning(
        state.db.as_ref(),
        group,
        version,
        &info.kind,
        &mut patched_resource,
    )
    .await;
    crate::api::finalizer_delete::preserve_deletion_timestamp_on_update(
        &current.data,
        &mut patched_resource,
    );

    if is_dry_run {
        return Ok(Json(patched_resource).into_response());
    }

    let storage_patched_resource = normalize_custom_resource_storage_data(
        state.db.as_ref(),
        conversion.as_ref(),
        group,
        plural,
        &stored_api_version,
        patched_resource,
    )
    .await?;

    let resource = state
        .db
        .update_resource(
            &stored_api_version,
            &info.kind,
            ns,
            name,
            storage_patched_resource,
            current.resource_version,
        )
        .await?;

    reconcile_custom_resource_owner_refs(state, &resource, patch_ctx).await;
    crate::api::finalizer_delete::finalize_after_update_if_ready(
        state,
        &stored_api_version,
        &info.kind,
        ns,
        name,
        &resource,
    )
    .await;

    let data = normalize_custom_resource_response_data(
        state.db.as_ref(),
        conversion.as_ref(),
        group,
        plural,
        &api_version,
        inject_resource_version(resource.data, resource.resource_version),
    )
    .await?;
    Ok(Json(data).into_response())
}

pub async fn patch_custom_resource(
    State(state): State<Arc<AppState>>,
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
    State(state): State<Arc<AppState>>,
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
        },
    )
    .await
}

pub async fn delete_collection_cluster_custom_resources(
    State(state): State<Arc<AppState>>,
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
            log_context: "delete collection (cluster CRD)",
        },
    )
    .await
}

pub async fn create_cluster_custom_resource(
    State(state): State<Arc<AppState>>,
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
    State(state): State<Arc<AppState>>,
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
    State(state): State<Arc<AppState>>,
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
    State(state): State<Arc<AppState>>,
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
    State(state): State<Arc<AppState>>,
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
