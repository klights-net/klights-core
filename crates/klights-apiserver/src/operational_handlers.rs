use std::sync::Arc;

use axum::Json;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use klights_supervisor::{
    ActiveTaskStatus, DbQueryLoggingStatus, TaskCategory, TaskCategoryStatus, TaskSupervisor,
};
use serde_json::Value;

use crate::VersionInfo;

pub trait OperationalMetrics: Send + Sync {
    fn render_prometheus(&self) -> String;
}

impl<F> OperationalMetrics for F
where
    F: Fn() -> String + Send + Sync,
{
    fn render_prometheus(&self) -> String {
        self()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OperationalNodeRole {
    Leader,
    Controlplane {
        leader_endpoints: Vec<String>,
        as_learner: bool,
    },
    Worker {
        leader_endpoints: Vec<String>,
    },
}

#[derive(Clone)]
pub struct OperationalEndpointInputs {
    pub(super) role: OperationalNodeRole,
    pub(super) metrics: Arc<dyn OperationalMetrics>,
    pub(super) version: VersionInfo,
    pub(super) cluster_status: Arc<dyn klights_leader_api::LeaderClusterStatusMetadata>,
    pub(super) follower_diagnostics: Option<Arc<dyn klights_leader_api::LeaderFollowerDiagnostics>>,
    pub(super) task_supervisor: Arc<TaskSupervisor>,
}

impl OperationalEndpointInputs {
    pub fn new(
        role: OperationalNodeRole,
        metrics: Arc<dyn OperationalMetrics>,
        version: VersionInfo,
        cluster_status: Arc<dyn klights_leader_api::LeaderClusterStatusMetadata>,
        follower_diagnostics: Option<Arc<dyn klights_leader_api::LeaderFollowerDiagnostics>>,
        task_supervisor: Arc<TaskSupervisor>,
    ) -> Self {
        Self {
            role,
            metrics,
            version,
            cluster_status,
            follower_diagnostics,
            task_supervisor,
        }
    }
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct OperationalStatus {
    role: &'static str,
    leader_endpoint: Option<String>,
    cluster_id: String,
    leader_epoch: i64,
    current_resource_version: i64,
    replica_last_applied_resource_version: Value,
    stream_state: &'static str,
    stream_lag: Value,
    followers: Vec<FollowerStatus>,
    follower_count: usize,
    max_follower_lag: i64,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct FollowerStatus {
    node_name: String,
    applied_resource_version: i64,
    lag: i64,
    mode: String,
    encryption: String,
    public_key: Option<String>,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct StatusBody {
    kind: &'static str,
    api_version: &'static str,
    status: &'static str,
    message: String,
    reason: &'static str,
    code: u16,
}

pub(super) enum OperationalHttpError {
    BadRequest(String),
    Forbidden(String),
    Internal(String),
}

impl IntoResponse for OperationalHttpError {
    fn into_response(self) -> Response {
        let (status, reason, message) = match self {
            Self::BadRequest(message) => (StatusCode::BAD_REQUEST, "BadRequest", message),
            Self::Forbidden(message) => (StatusCode::FORBIDDEN, "Forbidden", message),
            Self::Internal(message) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "InternalError", message)
            }
        };
        (
            status,
            Json(StatusBody {
                kind: "Status",
                api_version: "v1",
                status: "Failure",
                message,
                reason,
                code: status.as_u16(),
            }),
        )
            .into_response()
    }
}

pub(super) async fn health_check() -> &'static str {
    "ok"
}

pub(super) async fn readiness_check() -> &'static str {
    "ok"
}

pub(super) async fn metrics_handler(inputs: Arc<OperationalEndpointInputs>) -> String {
    inputs.metrics.render_prometheus()
}

pub(super) async fn version_handler(inputs: Arc<OperationalEndpointInputs>) -> Json<VersionInfo> {
    Json(inputs.version.clone())
}

pub(super) async fn klights_status_handler(
    inputs: Arc<OperationalEndpointInputs>,
) -> Result<Json<OperationalStatus>, OperationalHttpError> {
    let metadata = inputs
        .cluster_status
        .cluster_status_metadata()
        .await
        .map_err(|error| {
            OperationalHttpError::Internal(format!("failed to read cluster metadata: {error}"))
        })?;
    let (role, leader_endpoint, stream_state) = match &inputs.role {
        OperationalNodeRole::Leader => ("Leader", None, "local"),
        OperationalNodeRole::Controlplane {
            leader_endpoints,
            as_learner: true,
        } if !leader_endpoints.is_empty() => {
            ("Replica", leader_endpoints.first().cloned(), "local")
        }
        OperationalNodeRole::Controlplane {
            leader_endpoints, ..
        } if leader_endpoints.is_empty() => ("ControlplaneSeed", None, "local"),
        OperationalNodeRole::Controlplane {
            leader_endpoints, ..
        } => (
            "ControlplaneJoin",
            leader_endpoints.first().cloned(),
            "local",
        ),
        OperationalNodeRole::Worker { leader_endpoints } => {
            ("Worker", leader_endpoints.first().cloned(), "streaming")
        }
    };
    let diagnostics = match &inputs.follower_diagnostics {
        Some(diagnostics) => diagnostics.follower_diagnostics().await,
        None => klights_leader_api::FollowerDiagnostics::default(),
    };
    let followers = diagnostics
        .followers
        .into_iter()
        .map(|follower| FollowerStatus {
            node_name: follower.node_name,
            applied_resource_version: follower.applied_resource_version,
            lag: follower.lag,
            mode: follower.mode,
            encryption: follower.encryption,
            public_key: follower.public_key,
        })
        .collect();

    Ok(Json(OperationalStatus {
        role,
        leader_endpoint,
        cluster_id: metadata.cluster_id,
        leader_epoch: metadata.leader_epoch,
        current_resource_version: metadata.current_resource_version,
        replica_last_applied_resource_version: Value::Null,
        stream_state,
        stream_lag: Value::Null,
        followers,
        follower_count: diagnostics.follower_count,
        max_follower_lag: diagnostics.max_lag,
    }))
}

pub(super) async fn get_task_categories(
    inputs: Arc<OperationalEndpointInputs>,
    headers: HeaderMap,
) -> Result<Json<Vec<TaskCategoryStatus>>, OperationalHttpError> {
    ensure_admin(&headers)?;
    Ok(Json(inputs.task_supervisor.category_statuses()))
}

pub(super) async fn get_active_tasks(
    inputs: Arc<OperationalEndpointInputs>,
    headers: HeaderMap,
) -> Result<Json<Vec<ActiveTaskStatus>>, OperationalHttpError> {
    ensure_admin(&headers)?;
    Ok(Json(inputs.task_supervisor.active_tasks(None)))
}

pub(super) async fn get_active_tasks_by_category(
    inputs: Arc<OperationalEndpointInputs>,
    category: String,
    headers: HeaderMap,
) -> Result<Json<Vec<ActiveTaskStatus>>, OperationalHttpError> {
    ensure_admin(&headers)?;
    let category = parse_category(&category)?;
    Ok(Json(inputs.task_supervisor.active_tasks(Some(category))))
}

pub(super) async fn get_db_query_logging(
    inputs: Arc<OperationalEndpointInputs>,
    headers: HeaderMap,
) -> Result<Json<DbQueryLoggingStatus>, OperationalHttpError> {
    ensure_admin(&headers)?;
    Ok(Json(inputs.task_supervisor.db_query_logging_status()))
}

pub(super) async fn put_db_query_logging(
    inputs: Arc<OperationalEndpointInputs>,
    headers: HeaderMap,
    enabled: bool,
) -> Result<Json<DbQueryLoggingStatus>, OperationalHttpError> {
    ensure_admin(&headers)?;
    Ok(Json(inputs.task_supervisor.set_db_query_logging(enabled)))
}

fn ensure_admin(headers: &HeaderMap) -> Result<(), OperationalHttpError> {
    let is_admin = headers
        .get_all("x-remote-group")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .any(|group| group == "system:masters");
    if is_admin {
        Ok(())
    } else {
        Err(OperationalHttpError::Forbidden(
            "task-supervisor endpoints require admin privileges".to_string(),
        ))
    }
}

fn parse_category(raw: &str) -> Result<TaskCategory, OperationalHttpError> {
    match raw {
        "background" => Ok(TaskCategory::Background),
        "file" => Ok(TaskCategory::File),
        "db" => Ok(TaskCategory::Db),
        "db-read" => Ok(TaskCategory::DbRead),
        "timer" => Ok(TaskCategory::Timer),
        "network" => Ok(TaskCategory::Network),
        "pod-delete-workqueue" => Ok(TaskCategory::PodDeleteWorkqueue),
        "pod-lifecycle-actor" => Ok(TaskCategory::PodLifecycleActor),
        "pod-lifecycle-work" => Ok(TaskCategory::PodLifecycleWork),
        "pod-probe" => Ok(TaskCategory::PodProbe),
        "others" => Ok(TaskCategory::Others),
        _ => Err(OperationalHttpError::BadRequest(format!(
            "unknown task supervisor category: {raw}"
        ))),
    }
}
