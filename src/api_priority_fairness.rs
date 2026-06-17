//! Kubernetes API Priority and Fairness request admission.
//!
//! This module is intentionally event-driven: request classification reads the
//! current FlowSchema/PriorityLevelConfiguration resources on demand, and
//! concurrency control uses Tokio semaphores. There are no background sweeps or
//! polling loops.

use crate::api::AppError;
use crate::auth::AuthenticatedIdentity;
use crate::auth::request_attributes::AuthorizationRequest;
use crate::auth::request_info::{ResolvedAuthz, resolve_request_info};
use crate::datastore::{DatastoreBackend, ResourceListQuery};
use axum::extract::Request;
use axum::http::{Method, StatusCode};
use axum::middleware::Next;
use axum::response::IntoResponse;
use axum::response::Response;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const FLOWCONTROL_API_VERSION: &str = "flowcontrol.apiserver.k8s.io/v1";

#[derive(Default)]
pub struct ApiPriorityFairness {
    limiters: Mutex<HashMap<String, PriorityLevelLimiter>>,
}

struct PriorityLevelLimiter {
    seats: usize,
    semaphore: Arc<Semaphore>,
}

pub enum ApfAdmission {
    Exempt,
    Limited(OwnedSemaphorePermit),
}

pub async fn admit_request(
    state: Arc<crate::api::AppState>,
    request: Request,
    next: Next,
) -> Response {
    let identity = request
        .extensions()
        .get::<AuthenticatedIdentity>()
        .cloned()
        .unwrap_or_else(AuthenticatedIdentity::anonymous);
    match state
        .api_priority_fairness
        .admit(
            state.db.as_ref(),
            &identity,
            request.method(),
            request.uri().path(),
            request.uri().query(),
        )
        .await
    {
        Ok(_admission) => next.run(request).await,
        Err(response) => response,
    }
}

impl ApiPriorityFairness {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn admit(
        &self,
        db: &dyn DatastoreBackend,
        identity: &AuthenticatedIdentity,
        method: &Method,
        path: &str,
        query: Option<&str>,
    ) -> Result<ApfAdmission, axum::response::Response> {
        let ResolvedAuthz::Authorize(authz) = resolve_request_info(method, path, query);
        let Some(flow_schema) = select_matching_flow_schema(db, identity, &authz).await else {
            return Ok(ApfAdmission::Exempt);
        };
        let Some(priority_level_name) = flow_schema
            .pointer("/spec/priorityLevelConfiguration/name")
            .and_then(Value::as_str)
            .filter(|name| !name.is_empty())
        else {
            return Ok(ApfAdmission::Exempt);
        };
        let priority_level = match db
            .get_resource(
                FLOWCONTROL_API_VERSION,
                "PriorityLevelConfiguration",
                None,
                priority_level_name,
            )
            .await
        {
            Ok(Some(resource)) => resource.data,
            _ => return Ok(ApfAdmission::Exempt),
        };
        admit_priority_level(self, priority_level_name, priority_level.as_ref()).await
    }

    fn limiter(&self, name: &str, seats: usize) -> Arc<Semaphore> {
        let seats = seats.max(1);
        let mut limiters = self.limiters.lock().expect("APF limiter lock");
        let entry = limiters
            .entry(name.to_string())
            .or_insert_with(|| PriorityLevelLimiter {
                seats,
                semaphore: Arc::new(Semaphore::new(seats)),
            });
        if entry.seats != seats {
            *entry = PriorityLevelLimiter {
                seats,
                semaphore: Arc::new(Semaphore::new(seats)),
            };
        }
        entry.semaphore.clone()
    }

    #[cfg(test)]
    pub fn occupy_limited_priority_level_for_test(
        &self,
        name: &str,
        seats: usize,
    ) -> Option<OwnedSemaphorePermit> {
        self.limiter(name, seats).try_acquire_owned().ok()
    }
}

async fn admit_priority_level(
    apf: &ApiPriorityFairness,
    priority_level_name: &str,
    priority_level: &Value,
) -> Result<ApfAdmission, axum::response::Response> {
    match priority_level.pointer("/spec/type").and_then(Value::as_str) {
        Some("Exempt") => Ok(ApfAdmission::Exempt),
        Some("Limited") => {
            let seats = priority_level
                .pointer("/spec/limited/nominalConcurrencyShares")
                .and_then(Value::as_u64)
                .unwrap_or(1) as usize;
            let limiter = apf.limiter(priority_level_name, seats);
            let limit_response = priority_level
                .pointer("/spec/limited/limitResponse/type")
                .and_then(Value::as_str)
                .unwrap_or("Queue");
            if limit_response == "Reject" {
                match limiter.try_acquire_owned() {
                    Ok(permit) => Ok(ApfAdmission::Limited(permit)),
                    Err(_) => Err(too_many_requests(priority_level_name)),
                }
            } else {
                match limiter.acquire_owned().await {
                    Ok(permit) => Ok(ApfAdmission::Limited(permit)),
                    Err(_) => Ok(ApfAdmission::Exempt),
                }
            }
        }
        _ => Ok(ApfAdmission::Exempt),
    }
}

fn too_many_requests(priority_level_name: &str) -> axum::response::Response {
    AppError::Status {
        code: StatusCode::TOO_MANY_REQUESTS,
        reason: "TooManyRequests",
        message: format!("PriorityLevelConfiguration \"{priority_level_name}\" is saturated"),
        details: serde_json::Value::Null,
    }
    .into_response()
}

async fn select_matching_flow_schema(
    db: &dyn DatastoreBackend,
    identity: &AuthenticatedIdentity,
    request: &AuthorizationRequest,
) -> Option<Arc<Value>> {
    let list = db
        .list_resources(
            FLOWCONTROL_API_VERSION,
            "FlowSchema",
            None,
            ResourceListQuery::all(),
        )
        .await
        .ok()?;
    let mut matches = list
        .items
        .into_iter()
        .filter(|resource| flow_schema_matches(&resource.data, identity, request))
        .collect::<Vec<_>>();
    matches.sort_by_key(|resource| {
        resource
            .data
            .pointer("/spec/matchingPrecedence")
            .and_then(Value::as_i64)
            .unwrap_or(i64::MAX)
    });
    matches.into_iter().next().map(|resource| resource.data)
}

fn flow_schema_matches(
    flow_schema: &Value,
    identity: &AuthenticatedIdentity,
    request: &AuthorizationRequest,
) -> bool {
    flow_schema
        .pointer("/spec/rules")
        .and_then(Value::as_array)
        .is_some_and(|rules| {
            rules.iter().any(|rule| {
                rule_subjects_match(rule, identity) && rule_request_matches(rule, request)
            })
        })
}

fn rule_subjects_match(rule: &Value, identity: &AuthenticatedIdentity) -> bool {
    rule.get("subjects")
        .and_then(Value::as_array)
        .is_some_and(|subjects| {
            subjects
                .iter()
                .any(|subject| subject_matches(subject, identity))
        })
}

fn subject_matches(subject: &Value, identity: &AuthenticatedIdentity) -> bool {
    match subject.get("kind").and_then(Value::as_str) {
        Some("User") => subject
            .pointer("/user/name")
            .and_then(Value::as_str)
            .is_some_and(|name| name == "*" || name == identity.username),
        Some("Group") => subject
            .pointer("/group/name")
            .and_then(Value::as_str)
            .is_some_and(|name| name == "*" || identity.groups.iter().any(|g| g == name)),
        Some("ServiceAccount") => {
            let Some((namespace, name)) = service_account_identity(&identity.username) else {
                return false;
            };
            let subject_namespace = subject
                .pointer("/serviceAccount/namespace")
                .and_then(Value::as_str);
            let subject_name = subject
                .pointer("/serviceAccount/name")
                .and_then(Value::as_str);
            subject_namespace.is_some_and(|value| value == "*" || value == namespace)
                && subject_name.is_some_and(|value| value == "*" || value == name)
        }
        _ => false,
    }
}

fn service_account_identity(username: &str) -> Option<(&str, &str)> {
    username
        .strip_prefix("system:serviceaccount:")
        .and_then(|rest| rest.split_once(':'))
}

fn rule_request_matches(rule: &Value, request: &AuthorizationRequest) -> bool {
    if request.resource_request {
        rule.get("resourceRules")
            .and_then(Value::as_array)
            .is_some_and(|rules| {
                rules
                    .iter()
                    .any(|rule| resource_rule_matches(rule, request))
            })
    } else {
        rule.get("nonResourceRules")
            .and_then(Value::as_array)
            .is_some_and(|rules| {
                rules
                    .iter()
                    .any(|rule| non_resource_rule_matches(rule, request))
            })
    }
}

fn resource_rule_matches(rule: &Value, request: &AuthorizationRequest) -> bool {
    let verb_match = string_list_matches(rule.get("verbs"), &request.verb);
    let api_group = request.api_group.as_deref().unwrap_or("");
    let group_match = string_list_matches(rule.get("apiGroups"), api_group);
    let resource = resource_with_subresource(request);
    let resource_match = string_list_matches(rule.get("resources"), &resource);
    let scope_match = if let Some(namespace) = request.namespace.as_deref() {
        string_list_matches(rule.get("namespaces"), namespace)
    } else {
        rule.get("clusterScope")
            .and_then(Value::as_bool)
            .unwrap_or(false)
    };
    verb_match && group_match && resource_match && scope_match
}

fn resource_with_subresource(request: &AuthorizationRequest) -> String {
    match (request.resource.as_deref(), request.subresource.as_deref()) {
        (Some(resource), Some(subresource)) => format!("{resource}/{subresource}"),
        (Some(resource), None) => resource.to_string(),
        _ => String::new(),
    }
}

fn non_resource_rule_matches(rule: &Value, request: &AuthorizationRequest) -> bool {
    let Some(url) = request.non_resource_url.as_deref() else {
        return false;
    };
    string_list_matches(rule.get("verbs"), &request.verb)
        && rule
            .get("nonResourceURLs")
            .and_then(Value::as_array)
            .is_some_and(|urls| {
                urls.iter()
                    .any(|value| non_resource_url_matches(value, url))
            })
}

fn non_resource_url_matches(value: &Value, url: &str) -> bool {
    let Some(pattern) = value.as_str() else {
        return false;
    };
    pattern == "*"
        || pattern == url
        || pattern
            .strip_suffix('*')
            .is_some_and(|prefix| url.starts_with(prefix))
}

fn string_list_matches(values: Option<&Value>, needle: &str) -> bool {
    values.and_then(Value::as_array).is_some_and(|values| {
        values
            .iter()
            .filter_map(Value::as_str)
            .any(|value| value == "*" || value == needle)
    })
}
