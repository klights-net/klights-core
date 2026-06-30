//! Kubernetes API Priority and Fairness request admission.
//!
//! This module is event-driven: request classification reads the
//! current FlowSchema/PriorityLevelConfiguration resources on demand and
//! maintains per-priority-level admission state.

use crate::api::AppError;
use crate::auth::AuthenticatedIdentity;
use crate::auth::request_attributes::AuthorizationRequest;
use crate::auth::request_info::{ResolvedAuthz, resolve_request_info};
use crate::datastore::{DatastoreBackend, ResourceListQuery};
use axum::extract::Request;
use axum::http::{Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

const FLOWCONTROL_API_VERSION: &str = "flowcontrol.apiserver.k8s.io/v1";

#[derive(Default)]
pub struct ApiPriorityFairness {
    states: Mutex<HashMap<String, Arc<PriorityLevelState>>>,
}

pub struct PriorityLevelPermit {
    state: Arc<PriorityLevelState>,
}

impl Drop for PriorityLevelPermit {
    fn drop(&mut self) {
        self.state.release();
    }
}

pub enum ApfAdmission {
    Exempt,
    Limited(PriorityLevelPermit),
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

        let Some(priority_level_config) = parse_priority_level_config(&priority_level) else {
            return Ok(ApfAdmission::Exempt);
        };

        let priority_level_state =
            self.state_for_config(priority_level_name, priority_level_config);
        let priority_level_key = flow_schema_name(&flow_schema);
        let flow_key = flow_distinguisher(&flow_schema, identity, &authz);
        let permit = priority_level_state
            .acquire(
                limit_level_for_message(priority_level_name),
                &priority_level_key,
                &flow_key,
            )
            .await?;

        Ok(ApfAdmission::Limited(permit))
    }

    fn state_for_config(
        &self,
        priority_level_name: &str,
        config: PriorityLevelConfig,
    ) -> Arc<PriorityLevelState> {
        let mut states = self
            .states
            .lock()
            .expect("APF priority-level state lock must not be poisoned");
        let state = states
            .entry(priority_level_name.to_string())
            .or_insert_with(|| Arc::new(PriorityLevelState::new(config.clone())));
        state.reconfigure(config);
        Arc::clone(state)
    }

    #[cfg(test)]
    pub(crate) fn try_acquire_limited_for_test(
        &self,
        priority_level: &str,
        seats: usize,
    ) -> Option<PriorityLevelPermit> {
        let state = {
            let mut states = self
                .states
                .lock()
                .expect("APF priority-level state lock must not be poisoned");
            states
                .entry(priority_level.to_string())
                .or_insert_with(|| {
                    Arc::new(PriorityLevelState::new(PriorityLevelConfig {
                        seats,
                        limit_response: LimitResponseConfig::Queue {
                            queues: 1,
                            hand_size: 1,
                            queue_length_limit: usize::MAX,
                        },
                    }))
                })
                .clone()
        };
        state.reconfigure(PriorityLevelConfig {
            seats,
            limit_response: LimitResponseConfig::Queue {
                queues: 1,
                hand_size: 1,
                queue_length_limit: usize::MAX,
            },
        });
        state
            .try_acquire_immediate()
            .then(|| PriorityLevelPermit { state })
    }

    #[cfg(test)]
    pub(crate) fn occupy_limited_priority_level_for_test(
        &self,
        priority_level: &str,
        seats: usize,
    ) -> Option<PriorityLevelPermit> {
        self.try_acquire_limited_for_test(priority_level, seats)
    }

    #[cfg(test)]
    pub(crate) fn reconfigure_priority_level_for_test(&self, priority_level: &str, seats: usize) {
        let states = self
            .states
            .lock()
            .expect("APF priority-level state lock must not be poisoned");
        let Some(state) = states.get(priority_level) else {
            return;
        };
        state.reconfigure(PriorityLevelConfig {
            seats,
            limit_response: LimitResponseConfig::Queue {
                queues: 1,
                hand_size: 1,
                queue_length_limit: usize::MAX,
            },
        });
    }

    #[cfg(test)]
    pub(crate) fn queued_count_for_test(&self, priority_level: &str) -> usize {
        self.states
            .lock()
            .expect("APF priority-level state lock must not be poisoned")
            .get(priority_level)
            .map_or(0, |state| state.queued_count())
    }

    #[cfg(test)]
    pub(crate) fn executing_count_for_test(&self, priority_level: &str) -> usize {
        self.states
            .lock()
            .expect("APF priority-level state lock must not be poisoned")
            .get(priority_level)
            .map_or(0, |state| state.executing_count())
    }
}

struct PriorityLevelState {
    inner: Mutex<PriorityLevelStateInner>,
}

#[derive(Debug)]
struct PriorityLevelStateInner {
    seats: usize,
    limit_response: LimitResponseConfig,
    executing: usize,
    queues: Vec<VecDeque<Arc<QueuedRequest>>>,
    queue_cursor: usize,
    queued_count: usize,
}

#[derive(Debug)]
struct QueuedRequest {
    priority_level_name: String,
    flow_schema_name: String,
    flow_key: String,
    queue_index: AtomicUsize,
    wake: Arc<Notify>,
    admitted: AtomicBool,
    cancelled: AtomicBool,
}

struct QueuedRequestWait {
    state: Arc<PriorityLevelState>,
    request: Arc<QueuedRequest>,
}

impl Drop for QueuedRequestWait {
    fn drop(&mut self) {
        if self.request.admitted.load(Ordering::Acquire) {
            return;
        }
        if !self.request.cancelled.swap(true, Ordering::AcqRel) {
            self.state.remove_queued_request(&self.request);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PriorityLevelConfig {
    seats: usize,
    limit_response: LimitResponseConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum LimitResponseConfig {
    Reject,
    Queue {
        queues: usize,
        hand_size: usize,
        queue_length_limit: usize,
    },
}

impl PriorityLevelState {
    fn new(config: PriorityLevelConfig) -> Self {
        Self {
            inner: Mutex::new(PriorityLevelStateInner::new(config)),
        }
    }

    fn reconfigure(&self, config: PriorityLevelConfig) {
        let mut inner = self
            .inner
            .lock()
            .expect("APF priority-level state lock must not be poisoned");
        inner.reconfigure(config);
    }

    fn try_acquire_immediate(&self) -> bool {
        let mut inner = self
            .inner
            .lock()
            .expect("APF priority-level state lock must not be poisoned");
        if inner.executing < inner.seats {
            inner.executing += 1;
            return true;
        }
        false
    }

    async fn acquire(
        self: &Arc<Self>,
        priority_level_name: &str,
        flow_schema_name: &str,
        flow_key: &str,
    ) -> Result<PriorityLevelPermit, Response> {
        loop {
            if self.try_acquire_immediate() {
                return Ok(PriorityLevelPermit {
                    state: Arc::clone(self),
                });
            }

            let queued_request = {
                let mut inner = self
                    .inner
                    .lock()
                    .expect("APF priority-level state lock must not be poisoned");
                match inner.try_enqueue(priority_level_name, flow_schema_name, flow_key) {
                    QueueAdmission::Queued(request) => request,
                    QueueAdmission::Reject => return Err(too_many_requests(priority_level_name)),
                    QueueAdmission::QueueFull => {
                        return Err(queue_full_response(priority_level_name));
                    }
                }
            };

            let wait_guard = QueuedRequestWait {
                state: Arc::clone(self),
                request: queued_request,
            };
            wait_guard.request.wake.notified().await;
            if wait_guard.request.admitted.load(Ordering::Acquire) {
                drop(wait_guard);
                return Ok(PriorityLevelPermit {
                    state: Arc::clone(self),
                });
            }
            drop(wait_guard);
        }
    }

    fn release(&self) {
        let to_notify: Vec<Arc<QueuedRequest>> = {
            let mut inner = self
                .inner
                .lock()
                .expect("APF priority-level state lock must not be poisoned");
            if inner.executing > 0 {
                inner.executing -= 1;
            }
            let mut to_notify = Vec::new();
            while inner.executing < inner.seats {
                let Some(request) = inner.pop_next_queued_for_wakeup() else {
                    break;
                };
                request.admitted.store(true, Ordering::Release);
                inner.executing += 1;
                to_notify.push(request);
            }
            to_notify
        };

        for request in to_notify {
            request.wake.notify_one();
        }
    }

    fn remove_queued_request(&self, request: &Arc<QueuedRequest>) {
        let mut inner = self
            .inner
            .lock()
            .expect("APF priority-level state lock must not be poisoned");
        let queue_index = request.queue_index.load(Ordering::Acquire);
        if let Some(queue) = inner.queues.get_mut(queue_index)
            && let Some(pos) = queue.iter().position(|queued| Arc::ptr_eq(queued, request))
        {
            queue.remove(pos);
            inner.queued_count = inner.queued_count.saturating_sub(1);
            request.cancelled.store(true, Ordering::Release);
        }
    }

    #[cfg(test)]
    fn queued_count(&self) -> usize {
        self.inner
            .lock()
            .expect("APF priority-level state lock must not be poisoned")
            .queued_count
    }

    #[cfg(test)]
    fn executing_count(&self) -> usize {
        self.inner
            .lock()
            .expect("APF priority-level state lock must not be poisoned")
            .executing
    }

    #[cfg(test)]
    fn try_enqueue_for_test(&self, flow_key: &str) -> Result<(), &'static str> {
        let mut inner = self
            .inner
            .lock()
            .expect("APF priority-level state lock must not be poisoned");
        match inner.try_enqueue("test-pl", "test-fs", flow_key) {
            QueueAdmission::Queued(_) => Ok(()),
            QueueAdmission::Reject => Err("limit reached"),
            QueueAdmission::QueueFull => Err("queue full"),
        }
    }

    #[cfg(test)]
    fn set_executing_for_test(&self, executing: usize) {
        self.inner
            .lock()
            .expect("APF priority-level state lock must not be poisoned")
            .executing = executing;
    }
}

enum QueueAdmission {
    Queued(Arc<QueuedRequest>),
    Reject,
    QueueFull,
}

impl PriorityLevelStateInner {
    fn new(config: PriorityLevelConfig) -> Self {
        let queue_count = match &config.limit_response {
            LimitResponseConfig::Queue { queues, .. } => *queues,
            LimitResponseConfig::Reject => 0,
        };
        Self {
            seats: config.seats,
            limit_response: config.limit_response,
            executing: 0,
            queues: vec![VecDeque::new(); queue_count.max(1)],
            queue_cursor: 0,
            queued_count: 0,
        }
    }

    fn reconfigure(&mut self, config: PriorityLevelConfig) {
        self.seats = config.seats;
        let new_limit = config.limit_response;
        let old_limit = std::mem::replace(&mut self.limit_response, new_limit.clone());

        match (&old_limit, &new_limit) {
            (
                LimitResponseConfig::Queue {
                    queues: old_queues, ..
                },
                LimitResponseConfig::Queue {
                    queues: new_queues, ..
                },
            ) if old_queues == new_queues => {}
            (
                LimitResponseConfig::Queue { .. },
                LimitResponseConfig::Queue {
                    queues: new_queues, ..
                },
            ) => {
                let new_queues = (*new_queues).max(1);
                self.rebuild_queues(new_queues);
            }
            (LimitResponseConfig::Queue { .. }, LimitResponseConfig::Reject) => {
                self.queues.clear();
                self.queued_count = 0;
                self.queue_cursor = 0;
            }
            (LimitResponseConfig::Reject, LimitResponseConfig::Queue { queues, .. }) => {
                let queues = (*queues).max(1);
                self.queues = vec![VecDeque::new(); queues];
                self.queued_count = 0;
                self.queue_cursor = 0;
            }
            _ => {}
        }
    }

    fn try_enqueue(
        &mut self,
        priority_level_name: &str,
        flow_schema_name: &str,
        flow_key: &str,
    ) -> QueueAdmission {
        match self.limit_response {
            LimitResponseConfig::Reject => QueueAdmission::Reject,
            LimitResponseConfig::Queue {
                queues,
                hand_size,
                queue_length_limit,
            } => {
                if queue_length_limit == 0 || self.queues.is_empty() {
                    return QueueAdmission::QueueFull;
                }
                let hand_size = hand_size.max(1).min(queues.max(1));
                let queue_index = self.choose_queue_index(
                    priority_level_name,
                    flow_schema_name,
                    flow_key,
                    hand_size,
                );
                let queue = self
                    .queues
                    .get_mut(queue_index)
                    .expect("APF queue index must be within configured queue list");
                if queue.len() >= queue_length_limit {
                    return QueueAdmission::QueueFull;
                }
                let request = Arc::new(QueuedRequest {
                    priority_level_name: priority_level_name.to_string(),
                    flow_schema_name: flow_schema_name.to_string(),
                    flow_key: flow_key.to_string(),
                    queue_index: AtomicUsize::new(queue_index),
                    wake: Arc::new(Notify::new()),
                    admitted: AtomicBool::new(false),
                    cancelled: AtomicBool::new(false),
                });
                queue.push_back(Arc::clone(&request));
                self.queued_count += 1;
                QueueAdmission::Queued(request)
            }
        }
    }

    fn rebuild_queues(&mut self, queue_count: usize) {
        let queue_count = queue_count.max(1);
        if self.queues.len() == queue_count {
            return;
        }

        let mut all_requests = Vec::new();
        for queue in &mut self.queues {
            while let Some(request) = queue.pop_front() {
                all_requests.push(request);
            }
        }

        self.queues = vec![VecDeque::new(); queue_count];
        self.queued_count = 0;
        self.queue_cursor = 0;

        for request in all_requests {
            let queue_index = self.choose_queue_index(
                &request.priority_level_name,
                &request.flow_schema_name,
                &request.flow_key,
                self.max_hand_size(),
            );
            if let Some(queue) = self.queues.get_mut(queue_index) {
                request.queue_index.store(queue_index, Ordering::Release);
                queue.push_back(request);
                self.queued_count += 1;
            }
        }
    }

    fn max_hand_size(&self) -> usize {
        match &self.limit_response {
            LimitResponseConfig::Queue { hand_size, .. } => (*hand_size).max(1),
            LimitResponseConfig::Reject => 1,
        }
    }

    fn choose_queue_index(
        &self,
        priority_level_name: &str,
        flow_schema_name: &str,
        flow_key: &str,
        hand_size: usize,
    ) -> usize {
        let queue_count = self.queues.len();
        if queue_count == 0 {
            return 0;
        }
        let hand_size = hand_size.max(1).min(queue_count);
        let seed = stable_hash(priority_level_name, flow_schema_name, flow_key);
        let start_index = (seed % queue_count as u64) as usize;
        let mut best_index = start_index;
        let mut best_len = usize::MAX;
        for offset in 0..hand_size {
            let queue_index = (start_index + offset) % queue_count;
            let queue_len = self.queues[queue_index].len();
            if queue_len < best_len || (queue_len == best_len && queue_index < best_index) {
                best_len = queue_len;
                best_index = queue_index;
            }
        }
        best_index
    }

    fn pop_next_queued_for_wakeup(&mut self) -> Option<Arc<QueuedRequest>> {
        if self.queued_count == 0 || self.queues.is_empty() {
            return None;
        }
        let queue_count = self.queues.len();
        for _ in 0..queue_count {
            let queue_index = self.queue_cursor % queue_count;
            self.queue_cursor = (self.queue_cursor + 1) % queue_count;

            if let Some(request) = self.queues[queue_index].pop_front() {
                self.queued_count = self.queued_count.saturating_sub(1);
                if request.cancelled.load(Ordering::Acquire) {
                    continue;
                }
                return Some(request);
            }
        }
        None
    }
}

fn parse_priority_level_config(priority_level: &Value) -> Option<PriorityLevelConfig> {
    let ptype = priority_level
        .pointer("/spec/type")
        .and_then(Value::as_str)?;

    match ptype {
        "Exempt" => None,
        "Limited" => Some(PriorityLevelConfig {
            seats: parse_usize(
                priority_level.pointer("/spec/limited/nominalConcurrencyShares"),
                1,
            ),
            limit_response: parse_limit_response(priority_level),
        }),
        _ => None,
    }
}

fn parse_limit_response(value: &Value) -> LimitResponseConfig {
    let response_type = value
        .pointer("/spec/limited/limitResponse/type")
        .and_then(Value::as_str)
        .unwrap_or("Queue");

    match response_type {
        "Reject" => LimitResponseConfig::Reject,
        _ => {
            let queue_config = value
                .pointer("/spec/limited/limitResponse/queuing")
                .unwrap_or(&Value::Null);
            let queues = parse_positive_non_zero_or_default(queue_config.pointer("/queues"), 64);
            let hand_size =
                parse_positive_non_zero_or_default(queue_config.pointer("/handSize"), 8);
            let queue_length_limit =
                parse_non_negative_usize(queue_config.pointer("/queueLengthLimit"), 50);
            LimitResponseConfig::Queue {
                queues,
                hand_size,
                queue_length_limit,
            }
        }
    }
}

fn parse_usize(value: Option<&Value>, default: usize) -> usize {
    value
        .and_then(Value::as_u64)
        .and_then(|v| usize::try_from(v).ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

fn parse_positive_non_zero_or_default(value: Option<&Value>, default: usize) -> usize {
    value
        .and_then(Value::as_u64)
        .and_then(|v| usize::try_from(v).ok())
        .map(|value| if value == 0 { 1 } else { value })
        .unwrap_or(default)
}

fn parse_non_negative_usize(value: Option<&Value>, default: usize) -> usize {
    value
        .and_then(Value::as_u64)
        .and_then(|v| usize::try_from(v).ok())
        .unwrap_or(default)
}

fn limit_level_for_message(priority_level_name: &str) -> &str {
    priority_level_name
}

fn too_many_requests(priority_level_name: &str) -> Response {
    AppError::Status {
        code: StatusCode::TOO_MANY_REQUESTS,
        reason: "TooManyRequests",
        message: format!("PriorityLevelConfiguration \"{priority_level_name}\" is saturated"),
        details: serde_json::Value::Null,
    }
    .into_response()
}

fn queue_full_response(priority_level_name: &str) -> Response {
    AppError::Status {
        code: StatusCode::TOO_MANY_REQUESTS,
        reason: "TooManyRequests",
        message: format!("PriorityLevelConfiguration \"{priority_level_name}\" queue-full"),
        details: serde_json::Value::Null,
    }
    .into_response()
}

fn flow_schema_name(flow_schema: &Value) -> String {
    flow_schema
        .pointer("/metadata/name")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
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
    matches.sort_by(|a, b| {
        let a_precedence = a
            .data
            .pointer("/spec/matchingPrecedence")
            .and_then(Value::as_i64)
            .unwrap_or(i64::MAX);
        let b_precedence = b
            .data
            .pointer("/spec/matchingPrecedence")
            .and_then(Value::as_i64)
            .unwrap_or(i64::MAX);
        a_precedence.cmp(&b_precedence).then_with(|| {
            a.data
                .pointer("/metadata/name")
                .and_then(Value::as_str)
                .unwrap_or("")
                .cmp(
                    b.data
                        .pointer("/metadata/name")
                        .and_then(Value::as_str)
                        .unwrap_or(""),
                )
        })
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

fn flow_distinguisher(
    flow_schema: &Value,
    identity: &AuthenticatedIdentity,
    request: &AuthorizationRequest,
) -> String {
    match flow_schema
        .pointer("/spec/distinguisherMethod/type")
        .and_then(Value::as_str)
    {
        Some("ByUser") => identity.username.clone(),
        Some("ByNamespace") => request.namespace.clone().unwrap_or_default(),
        _ => String::new(),
    }
}

fn resource_with_subresource(request: &AuthorizationRequest) -> String {
    match (request.resource.as_deref(), request.subresource.as_deref()) {
        (Some(resource), Some(subresource)) => format!("{resource}/{subresource}"),
        (Some(resource), None) => resource.to_string(),
        _ => String::new(),
    }
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

fn stable_hash(priority_level_name: &str, flow_schema_name: &str, flow_key: &str) -> u64 {
    let mut hash = 1469598103934665603u64;
    for part in [priority_level_name, flow_schema_name, flow_key] {
        for byte in part.as_bytes() {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(1099511628211);
        }
        hash ^= 255;
        hash = hash.wrapping_mul(1099511628211);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::request_attributes::AuthorizationRequest;
    use crate::datastore::test_support;

    async fn create_test_priority_level(
        db: &dyn DatastoreBackend,
        name: &str,
        nominal_concurrency_shares: usize,
        limit_type: &str,
    ) {
        db.create_resource(
            "flowcontrol.apiserver.k8s.io/v1",
            "PriorityLevelConfiguration",
            None,
            name,
            serde_json::json!({
                "apiVersion": "flowcontrol.apiserver.k8s.io/v1",
                "kind": "PriorityLevelConfiguration",
                "metadata": {"name": name},
                "spec": {
                    "type": "Limited",
                    "limited": {
                        "nominalConcurrencyShares": nominal_concurrency_shares,
                        "limitResponse": {"type": limit_type}
                    }
                }
            }),
        )
        .await
        .unwrap();
    }

    async fn create_test_flowschema(
        db: &dyn DatastoreBackend,
        name: &str,
        matching_precedence: i64,
        priority_level_name: &str,
        distinguisher_type: &str,
    ) {
        db.create_resource(
            "flowcontrol.apiserver.k8s.io/v1",
            "FlowSchema",
            None,
            name,
            serde_json::json!({
                "apiVersion": "flowcontrol.apiserver.k8s.io/v1",
                "kind": "FlowSchema",
                "metadata": {"name": name},
                "spec": {
                    "matchingPrecedence": matching_precedence,
                    "priorityLevelConfiguration": {"name": priority_level_name},
                    "distinguisherMethod": {"type": distinguisher_type},
                    "rules": [{
                        "subjects": [{"kind": "User", "user": {"name": "*"}}],
                        "resourceRules": [{
                            "verbs": ["list"],
                            "apiGroups": [""],
                            "resources": ["namespaces"],
                            "clusterScope": true
                        }]
                    }]
                }
            }),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn limited_priority_level_reconfiguration_keeps_inflight_accounting() {
        let apf = ApiPriorityFairness::new();
        let first = apf
            .try_acquire_limited_for_test("pl", 1)
            .expect("first seat");
        assert!(apf.try_acquire_limited_for_test("pl", 1).is_none());

        apf.reconfigure_priority_level_for_test("pl", 2);
        let second = apf
            .try_acquire_limited_for_test("pl", 2)
            .expect("second seat after resize");
        assert!(
            apf.try_acquire_limited_for_test("pl", 2).is_none(),
            "old in-flight permit plus new permit must consume the resized limit"
        );

        drop(first);
        assert!(apf.try_acquire_limited_for_test("pl", 2).is_some());
        drop(second);
    }

    #[tokio::test]
    async fn flow_schema_matching_breaks_equal_precedence_by_name() {
        let db = test_support::in_memory().await;

        create_test_priority_level(&db, "pl-a", 1, "Reject").await;
        create_test_priority_level(&db, "pl-b", 1, "Reject").await;
        create_test_flowschema(&db, "b-schema", 10, "pl-b", "*").await;
        create_test_flowschema(&db, "a-schema", 10, "pl-a", "*").await;

        let identity = AuthenticatedIdentity::client_cert("alice".into(), vec![]);
        let request =
            AuthorizationRequest::resource("list", "", "v1", "namespaces", None, None, None);
        let selected = select_matching_flow_schema(&db, &identity, &request)
            .await
            .expect("a matching FlowSchema must exist");

        assert_eq!(
            selected.pointer("/metadata/name").and_then(Value::as_str),
            Some("a-schema")
        );
    }

    #[test]
    fn flow_distinguisher_by_user_separates_users() {
        let schema = serde_json::json!({
            "metadata": {"name": "user-schema"},
            "spec": {"distinguisherMethod": {"type": "ByUser"}}
        });
        let request =
            AuthorizationRequest::resource("list", "", "v1", "pods", None, Some("default"), None);

        let alice = AuthenticatedIdentity::client_cert("alice".into(), vec![]);
        let bob = AuthenticatedIdentity::client_cert("bob".into(), vec![]);

        assert_ne!(
            flow_distinguisher(&schema, &alice, &request),
            flow_distinguisher(&schema, &bob, &request)
        );
    }

    #[test]
    fn flow_distinguisher_by_namespace_separates_namespaces() {
        let schema = serde_json::json!({
            "metadata": {"name": "namespace-schema"},
            "spec": {"distinguisherMethod": {"type": "ByNamespace"}}
        });
        let request =
            AuthorizationRequest::resource("list", "", "v1", "pods", None, Some("default"), None);
        let other_request = AuthorizationRequest::resource(
            "list",
            "",
            "v1",
            "pods",
            None,
            Some("kube-system"),
            None,
        );
        let identity = AuthenticatedIdentity::client_cert("alice".into(), vec![]);

        assert_ne!(
            flow_distinguisher(&schema, &identity, &request),
            flow_distinguisher(&schema, &identity, &other_request)
        );
    }

    #[test]
    fn flow_distinguisher_absent_is_empty() {
        let schema = serde_json::json!({
            "metadata": {"name": "default-schema"},
            "spec": {}
        });
        let request =
            AuthorizationRequest::resource("list", "", "v1", "pods", None, Some("default"), None);
        let alice = AuthenticatedIdentity::client_cert("alice".into(), vec![]);
        let bob = AuthenticatedIdentity::client_cert("bob".into(), vec![]);

        assert_eq!(
            flow_distinguisher(&schema, &alice, &request),
            String::new(),
            "missing distinguisherMethod must be empty"
        );
        assert_eq!(
            flow_distinguisher(&schema, &bob, &request),
            String::new(),
            "missing distinguisherMethod must be empty regardless of identity"
        );
    }

    #[test]
    fn queue_selection_keeps_unrelated_flow_key_out_of_full_queue_when_another_queue_is_open() {
        let state = PriorityLevelState::new(PriorityLevelConfig {
            seats: 1,
            limit_response: LimitResponseConfig::Queue {
                queues: 2,
                hand_size: 1,
                queue_length_limit: 1,
            },
        });
        state.set_executing_for_test(1);

        assert!(state.try_enqueue_for_test("alice").is_ok());
        assert!(state.try_enqueue_for_test("bob").is_ok());
        assert!(state.try_enqueue_for_test("alice").is_err());
    }

    #[test]
    fn release_reserves_seat_for_one_queued_request_at_a_time() {
        let state = PriorityLevelState::new(PriorityLevelConfig {
            seats: 1,
            limit_response: LimitResponseConfig::Queue {
                queues: 2,
                hand_size: 1,
                queue_length_limit: 2,
            },
        });
        state.set_executing_for_test(1);
        state.try_enqueue_for_test("alice").unwrap();
        state.try_enqueue_for_test("bob").unwrap();

        state.release();

        assert_eq!(
            state.executing_count(),
            1,
            "released seat must be reserved for exactly one queued request"
        );
        assert_eq!(
            state.queued_count(),
            1,
            "only one queued request should be admitted per released seat"
        );
    }
}
