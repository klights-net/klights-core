//! Kubernetes-style request-info resolver.
//!
//! Translates an HTTP request (method + path + query) into the authorization
//! attributes the authorizer chain evaluates, exactly like the upstream
//! kube-apiserver `RequestInfoFactory`. This is the single place that knows how
//! to map URLs to verbs/resources/subresources; the global authorization
//! middleware feeds its output to `state.authorizer`. Keeping this logic in one
//! pure, exhaustively-tested function is what makes authorization DRY and
//! "secure by construction": no handler can be reached without it.

use crate::auth::request_attributes::AuthorizationRequest;
use axum::http::Method;

/// Outcome of resolving a request path into authorization attributes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResolvedAuthz {
    /// Authorize this request against the authorizer chain.
    ///
    /// Kubernetes public informational endpoints are still represented as
    /// non-resource authorization requests. Anonymous/public access is granted
    /// by bootstrapped RBAC roles, not by bypassing this resolver.
    Authorize(Box<AuthorizationRequest>),
}

/// Subresources whose authorization verb is always `create` (connect-style and
/// create-style subresources), regardless of HTTP method.
const CREATE_VERB_SUBRESOURCES: &[&str] = &["exec", "attach", "portforward", "eviction", "token"];

/// Lowercased HTTP method used as the verb for non-resource URL requests.
fn non_resource_verb(method: &Method) -> &'static str {
    match *method {
        Method::GET | Method::HEAD => "get",
        Method::POST => "post",
        Method::PUT => "put",
        Method::PATCH => "patch",
        Method::DELETE => "delete",
        Method::OPTIONS => "options",
        _ => "get",
    }
}

/// Verb for a resource request with no subresource.
fn resource_verb(method: &Method, has_name: bool, watch: bool) -> &'static str {
    match *method {
        Method::GET | Method::HEAD => {
            if has_name {
                "get"
            } else if watch {
                "watch"
            } else {
                "list"
            }
        }
        Method::POST => "create",
        Method::PUT => "update",
        Method::PATCH => "patch",
        Method::DELETE => {
            if has_name {
                "delete"
            } else {
                "deletecollection"
            }
        }
        _ => "get",
    }
}

/// Verb for a subresource request.
fn subresource_verb(method: &Method, subresource: &str) -> &'static str {
    if CREATE_VERB_SUBRESOURCES.contains(&subresource) {
        return "create";
    }
    if subresource == "log" {
        return "get";
    }
    // proxy / status / scale / ephemeralcontainers / unknown: map HTTP method.
    match *method {
        Method::GET | Method::HEAD => "get",
        Method::POST => "create",
        Method::PUT => "update",
        Method::PATCH => "patch",
        Method::DELETE => "delete",
        _ => "get",
    }
}

/// Extract and percent-decode a query parameter value.
fn query_param(raw_query: Option<&str>, key: &str) -> Option<String> {
    let raw = raw_query?;
    raw.split('&').find_map(|kv| {
        let (k, v) = kv.split_once('=')?;
        if k == key {
            Some(
                urlencoding::decode(v)
                    .map(|c| c.into_owned())
                    .unwrap_or_else(|_| v.to_string()),
            )
        } else {
            None
        }
    })
}

fn has_watch(raw_query: Option<&str>) -> bool {
    query_param(raw_query, "watch")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false)
}

/// Finalize a resource `AuthorizationRequest`, attaching selectors only for the
/// collection verbs that support them.
fn finish(req: AuthorizationRequest, raw_query: Option<&str>) -> ResolvedAuthz {
    let req = if matches!(req.verb.as_str(), "list" | "watch" | "deletecollection") {
        req.with_field_selector(query_param(raw_query, "fieldSelector"))
            .with_label_selector(query_param(raw_query, "labelSelector"))
    } else {
        req
    };
    ResolvedAuthz::Authorize(Box::new(req))
}

/// Resolve an HTTP request into authorization attributes.
///
/// `path` must be the URL path (no query string); `raw_query` is the raw query
/// string (without the leading `?`), if any.
pub fn resolve_request_info(method: &Method, path: &str, raw_query: Option<&str>) -> ResolvedAuthz {
    let segs: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

    // Determine api prefix, group and version, and the remaining path parts.
    // Anything that is not a well-formed resource path is a non-resource URL.
    let (api_group, api_version, rest): (&str, &str, &[&str]) = match segs.first().copied() {
        Some("api") => match segs.len() {
            // /api or /api/v1 (discovery) → non-resource
            0 | 1 => return non_resource(method, path),
            _ => ("", segs[1], &segs[2..]),
        },
        Some("apis") => {
            // /apis, /apis/{group}, /apis/{group}/{version} (discovery) → non-resource
            if segs.len() < 4 {
                return non_resource(method, path);
            }
            (segs[1], segs[2], &segs[3..])
        }
        _ => return non_resource(method, path),
    };

    if rest.is_empty() {
        return non_resource(method, path);
    }

    // Namespace handling, mirroring kube-apiserver RequestInfoFactory.
    if rest[0] == "namespaces" {
        match rest.len() {
            1 => {
                // GET/POST /.../namespaces → list/create namespaces (cluster-scoped)
                let verb = resource_verb(method, false, has_watch(raw_query));
                return finish(
                    AuthorizationRequest::resource(
                        verb,
                        api_group,
                        api_version,
                        "namespaces",
                        None,
                        None,
                        None,
                    ),
                    raw_query,
                );
            }
            2 => {
                // /.../namespaces/{name} → the namespace object
                let verb = resource_verb(method, true, false);
                return finish(
                    AuthorizationRequest::resource(
                        verb,
                        api_group,
                        api_version,
                        "namespaces",
                        None,
                        None,
                        Some(rest[1]),
                    ),
                    raw_query,
                );
            }
            3 if matches!(rest[2], "status" | "finalize") => {
                // /.../namespaces/{name}/status|finalize → namespace subresource
                let verb = subresource_verb(method, rest[2]);
                return finish(
                    AuthorizationRequest::resource(
                        verb,
                        api_group,
                        api_version,
                        "namespaces",
                        Some(rest[2]),
                        None,
                        Some(rest[1]),
                    ),
                    raw_query,
                );
            }
            _ => {
                // /.../namespaces/{ns}/{resource}/... → namespaced resource
                let namespace = rest[1];
                return resolve_resource_parts(
                    method,
                    api_group,
                    api_version,
                    Some(namespace),
                    &rest[2..],
                    raw_query,
                );
            }
        }
    }

    // Cluster-scoped resource.
    resolve_resource_parts(method, api_group, api_version, None, rest, raw_query)
}

/// Parse `[resource, name?, subresource?, ...]` into an authorization request.
fn resolve_resource_parts(
    method: &Method,
    api_group: &str,
    api_version: &str,
    namespace: Option<&str>,
    parts: &[&str],
    raw_query: Option<&str>,
) -> ResolvedAuthz {
    let resource = parts[0];
    let name = parts.get(1).copied();
    let subresource = parts.get(2).copied();

    let verb = match subresource {
        Some(sub) => subresource_verb(method, sub),
        None => resource_verb(method, name.is_some(), has_watch(raw_query)),
    };

    finish(
        AuthorizationRequest::resource(
            verb,
            api_group,
            api_version,
            resource,
            subresource,
            namespace,
            name,
        ),
        raw_query,
    )
}

fn non_resource(method: &Method, path: &str) -> ResolvedAuthz {
    ResolvedAuthz::Authorize(Box::new(AuthorizationRequest::non_resource(
        non_resource_verb(method),
        path,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::request_attributes::RequestKind;

    /// Helper: resolve and unwrap to an authorization request.
    fn req(method: Method, path: &str, query: Option<&str>) -> AuthorizationRequest {
        let ResolvedAuthz::Authorize(r) = resolve_request_info(&method, path, query);
        *r
    }

    #[test]
    fn k8s_non_resource_info_endpoints_still_flow_through_authorization() {
        for path in [
            "/healthz",
            "/healthz/etcd",
            "/livez",
            "/readyz",
            "/version",
            "/openid/v1/jwks",
            "/.well-known/openid-configuration",
        ] {
            let r = req(Method::GET, path, None);
            assert_eq!(r.kind, RequestKind::NonResource, "{path}");
            assert_eq!(r.verb, "get", "{path}");
            assert_eq!(r.non_resource_url.as_deref(), Some(path), "{path}");
        }
    }

    #[test]
    fn metrics_requires_non_resource_authorization() {
        let r = req(Method::GET, "/metrics", None);
        assert_eq!(r.verb, "get");
        assert_eq!(r.non_resource_url.as_deref(), Some("/metrics"));
        assert!(matches!(r.kind, RequestKind::NonResource));
    }

    #[test]
    fn klights_status_requires_non_resource_authorization() {
        let r = req(Method::GET, "/klights/v1/status", None);
        assert_eq!(r.verb, "get");
        assert_eq!(r.non_resource_url.as_deref(), Some("/klights/v1/status"));
        assert!(matches!(r.kind, RequestKind::NonResource));
    }

    #[test]
    fn core_pod_crud_verbs_and_attributes() {
        // namespaced list
        let r = req(Method::GET, "/api/v1/namespaces/default/pods", None);
        assert_eq!(r.verb, "list");
        assert_eq!(r.resource.as_deref(), Some("pods"));
        assert_eq!(r.namespace.as_deref(), Some("default"));
        assert_eq!(r.name, None);
        assert_eq!(r.api_group, None);
        assert_eq!(r.api_version.as_deref(), Some("v1"));
        assert!(r.subresource.is_none());

        // namespaced watch
        let r = req(
            Method::GET,
            "/api/v1/namespaces/default/pods",
            Some("watch=true"),
        );
        assert_eq!(r.verb, "watch");

        // get one
        let r = req(Method::GET, "/api/v1/namespaces/default/pods/p1", None);
        assert_eq!(r.verb, "get");
        assert_eq!(r.name.as_deref(), Some("p1"));

        // create
        let r = req(Method::POST, "/api/v1/namespaces/default/pods", None);
        assert_eq!(r.verb, "create");

        // update / patch / delete
        assert_eq!(
            req(Method::PUT, "/api/v1/namespaces/default/pods/p1", None).verb,
            "update"
        );
        assert_eq!(
            req(Method::PATCH, "/api/v1/namespaces/default/pods/p1", None).verb,
            "patch"
        );
        assert_eq!(
            req(Method::DELETE, "/api/v1/namespaces/default/pods/p1", None).verb,
            "delete"
        );

        // deletecollection
        assert_eq!(
            req(Method::DELETE, "/api/v1/namespaces/default/pods", None).verb,
            "deletecollection"
        );

        // cluster-wide list (list_all_pods)
        let r = req(Method::GET, "/api/v1/pods", None);
        assert_eq!(r.verb, "list");
        assert_eq!(r.resource.as_deref(), Some("pods"));
        assert_eq!(r.namespace, None);
    }

    #[test]
    fn pod_subresource_verbs() {
        // exec/attach/portforward → create regardless of method
        for (sub, m) in [
            ("exec", Method::GET),
            ("exec", Method::POST),
            ("attach", Method::GET),
            ("portforward", Method::POST),
        ] {
            let r = req(
                m,
                &format!("/api/v1/namespaces/default/pods/p1/{sub}"),
                None,
            );
            assert_eq!(r.verb, "create", "{sub}");
            assert_eq!(r.resource.as_deref(), Some("pods"));
            assert_eq!(r.subresource.as_deref(), Some(sub));
            assert_eq!(r.name.as_deref(), Some("p1"));
        }

        // log → get
        let r = req(Method::GET, "/api/v1/namespaces/default/pods/p1/log", None);
        assert_eq!(r.verb, "get");
        assert_eq!(r.subresource.as_deref(), Some("log"));

        // eviction → create
        let r = req(
            Method::POST,
            "/api/v1/namespaces/default/pods/p1/eviction",
            None,
        );
        assert_eq!(r.verb, "create");
        assert_eq!(r.subresource.as_deref(), Some("eviction"));

        // status → method-mapped
        assert_eq!(
            req(
                Method::GET,
                "/api/v1/namespaces/default/pods/p1/status",
                None
            )
            .verb,
            "get"
        );
        assert_eq!(
            req(
                Method::PUT,
                "/api/v1/namespaces/default/pods/p1/status",
                None
            )
            .verb,
            "update"
        );
        assert_eq!(
            req(
                Method::PATCH,
                "/api/v1/namespaces/default/pods/p1/status",
                None
            )
            .verb,
            "patch"
        );

        // ephemeralcontainers → method-mapped
        assert_eq!(
            req(
                Method::PUT,
                "/api/v1/namespaces/default/pods/p1/ephemeralcontainers",
                None
            )
            .verb,
            "update"
        );

        // proxy → method-mapped (incl. DELETE→delete)
        let r = req(
            Method::DELETE,
            "/api/v1/namespaces/default/pods/p1/proxy/x/y",
            None,
        );
        assert_eq!(r.verb, "delete");
        assert_eq!(r.subresource.as_deref(), Some("proxy"));
        assert_eq!(
            req(
                Method::POST,
                "/api/v1/namespaces/default/pods/p1/proxy",
                None
            )
            .verb,
            "create"
        );
        assert_eq!(
            req(
                Method::GET,
                "/api/v1/namespaces/default/pods/p1/proxy",
                None
            )
            .verb,
            "get"
        );
    }

    #[test]
    fn service_and_serviceaccount_subresources() {
        // service delete
        let r = req(
            Method::DELETE,
            "/api/v1/namespaces/default/services/s1",
            None,
        );
        assert_eq!(r.verb, "delete");
        assert_eq!(r.resource.as_deref(), Some("services"));

        // service proxy → method-mapped
        let r = req(
            Method::GET,
            "/api/v1/namespaces/default/services/s1/proxy/path",
            None,
        );
        assert_eq!(r.verb, "get");
        assert_eq!(r.subresource.as_deref(), Some("proxy"));
        assert_eq!(r.resource.as_deref(), Some("services"));

        // serviceaccount token → create
        let r = req(
            Method::POST,
            "/api/v1/namespaces/default/serviceaccounts/sa1/token",
            None,
        );
        assert_eq!(r.verb, "create");
        assert_eq!(r.resource.as_deref(), Some("serviceaccounts"));
        assert_eq!(r.subresource.as_deref(), Some("token"));
    }

    #[test]
    fn namespace_object_and_subresources() {
        // list/create namespaces
        assert_eq!(req(Method::GET, "/api/v1/namespaces", None).verb, "list");
        assert_eq!(req(Method::POST, "/api/v1/namespaces", None).verb, "create");

        // namespace object
        let r = req(Method::GET, "/api/v1/namespaces/ns1", None);
        assert_eq!(r.verb, "get");
        assert_eq!(r.resource.as_deref(), Some("namespaces"));
        assert_eq!(r.name.as_deref(), Some("ns1"));
        assert_eq!(r.namespace, None, "namespaces are cluster-scoped");
        assert!(r.subresource.is_none());

        assert_eq!(
            req(Method::DELETE, "/api/v1/namespaces/ns1", None).verb,
            "delete"
        );

        // finalize subresource (the bug-fixed path)
        let r = req(Method::PUT, "/api/v1/namespaces/ns1/finalize", None);
        assert_eq!(r.verb, "update");
        assert_eq!(r.resource.as_deref(), Some("namespaces"));
        assert_eq!(r.subresource.as_deref(), Some("finalize"));
        assert_eq!(r.name.as_deref(), Some("ns1"));
        assert_eq!(r.namespace, None);

        // status subresource
        let r = req(Method::PUT, "/api/v1/namespaces/ns1/status", None);
        assert_eq!(r.verb, "update");
        assert_eq!(r.subresource.as_deref(), Some("status"));
        assert_eq!(r.name.as_deref(), Some("ns1"));
    }

    #[test]
    fn node_proxy_and_status() {
        let r = req(Method::GET, "/api/v1/nodes/n1/proxy/pods", None);
        assert_eq!(r.verb, "get");
        assert_eq!(r.resource.as_deref(), Some("nodes"));
        assert_eq!(r.subresource.as_deref(), Some("proxy"));
        assert_eq!(r.name.as_deref(), Some("n1"));
        assert_eq!(r.namespace, None);

        assert_eq!(
            req(Method::POST, "/api/v1/nodes/n1/proxy", None).verb,
            "create"
        );
        assert_eq!(
            req(Method::DELETE, "/api/v1/nodes/n1/proxy", None).verb,
            "delete"
        );
    }

    #[test]
    fn apps_group_and_scale_subresource() {
        let r = req(
            Method::GET,
            "/apis/apps/v1/namespaces/default/deployments",
            None,
        );
        assert_eq!(r.verb, "list");
        assert_eq!(r.api_group.as_deref(), Some("apps"));
        assert_eq!(r.api_version.as_deref(), Some("v1"));
        assert_eq!(r.resource.as_deref(), Some("deployments"));

        let r = req(
            Method::PUT,
            "/apis/apps/v1/namespaces/default/deployments/d1/scale",
            None,
        );
        assert_eq!(r.verb, "update");
        assert_eq!(r.subresource.as_deref(), Some("scale"));
        assert_eq!(r.name.as_deref(), Some("d1"));
        assert_eq!(r.namespace.as_deref(), Some("default"));
    }

    #[test]
    fn custom_resources_namespaced_cluster_and_subresource() {
        // namespaced CR list with selectors
        let r = req(
            Method::GET,
            "/apis/example.com/v1/namespaces/default/widgets",
            Some("fieldSelector=metadata.name%3Dallowed&labelSelector=app%3Dx"),
        );
        assert_eq!(r.verb, "list");
        assert_eq!(r.api_group.as_deref(), Some("example.com"));
        assert_eq!(r.resource.as_deref(), Some("widgets"));
        assert_eq!(r.field_selector.as_deref(), Some("metadata.name=allowed"));
        assert_eq!(r.label_selector.as_deref(), Some("app=x"));

        // watch decode
        let r = req(
            Method::GET,
            "/apis/example.com/v1/namespaces/default/widgets",
            Some("watch=true&fieldSelector=metadata.name%3Dallowed"),
        );
        assert_eq!(r.verb, "watch");
        assert_eq!(r.field_selector.as_deref(), Some("metadata.name=allowed"));

        // cluster-scoped CR get
        let r = req(Method::GET, "/apis/example.com/v1/clusterwidgets/cw1", None);
        assert_eq!(r.verb, "get");
        assert_eq!(r.resource.as_deref(), Some("clusterwidgets"));
        assert_eq!(r.name.as_deref(), Some("cw1"));
        assert_eq!(r.namespace, None);

        // CR subresource (status) method-mapped
        let r = req(
            Method::PUT,
            "/apis/example.com/v1/namespaces/default/widgets/w1/status",
            None,
        );
        assert_eq!(r.verb, "update");
        assert_eq!(r.subresource.as_deref(), Some("status"));

        // deletecollection carries selectors
        let r = req(
            Method::DELETE,
            "/apis/example.com/v1/namespaces/default/widgets",
            Some("labelSelector=app%3Dx"),
        );
        assert_eq!(r.verb, "deletecollection");
        assert_eq!(r.label_selector.as_deref(), Some("app=x"));
    }

    #[test]
    fn selectors_only_on_collection_verbs() {
        // get must not carry selectors even if present in query
        let r = req(
            Method::GET,
            "/api/v1/namespaces/default/pods/p1",
            Some("fieldSelector=metadata.name%3Dx"),
        );
        assert_eq!(r.verb, "get");
        assert!(r.field_selector.is_none());
    }

    #[test]
    fn subject_access_review_endpoints_are_resource_requests() {
        let r = req(
            Method::POST,
            "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews",
            None,
        );
        assert_eq!(r.verb, "create");
        assert_eq!(r.api_group.as_deref(), Some("authorization.k8s.io"));
        assert_eq!(r.resource.as_deref(), Some("selfsubjectaccessreviews"));
        assert_eq!(r.namespace, None);

        let r = req(
            Method::POST,
            "/apis/authorization.k8s.io/v1/namespaces/default/localsubjectaccessreviews",
            None,
        );
        assert_eq!(r.verb, "create");
        assert_eq!(r.resource.as_deref(), Some("localsubjectaccessreviews"));
        assert_eq!(r.namespace.as_deref(), Some("default"));
    }

    #[test]
    fn discovery_and_openapi_are_non_resource() {
        for path in [
            "/api",
            "/api/v1",
            "/apis",
            "/apis/apps",
            "/apis/apps/v1",
            "/openapi/v2",
            "/openapi/v3",
            "/openapi/v3/apis/apps/v1",
            "/version",
        ] {
            let r = req(Method::GET, path, None);
            assert_eq!(r.kind, RequestKind::NonResource, "{path}");
            assert_eq!(r.verb, "get");
            assert_eq!(r.non_resource_url.as_deref(), Some(path));
            assert!(!r.resource_request);
        }
    }

    #[test]
    fn debug_and_internal_paths_are_non_resource() {
        let r = req(Method::GET, "/debug/klights/pod-lifecycle", None);
        assert_eq!(r.kind, RequestKind::NonResource);
        assert_eq!(
            r.non_resource_url.as_deref(),
            Some("/debug/klights/pod-lifecycle")
        );

        // task-supervisor admin endpoints require authz (non-resource)
        let r = req(Method::GET, "/klights/v1/task-supervisor/tasks", None);
        assert_eq!(r.kind, RequestKind::NonResource);
    }
}
