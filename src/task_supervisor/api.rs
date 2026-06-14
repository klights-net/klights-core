use super::category::TaskCategory;
use crate::api::{AppError, AppState};
use axum::{
    Json, Router,
    extract::{Path, State},
    http::HeaderMap,
    routing::get,
};
use std::sync::Arc;

#[derive(serde::Deserialize)]
struct DbQueryLoggingUpdate {
    enabled: bool,
}

pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/categories", get(get_categories))
        .route("/tasks", get(get_tasks))
        .route("/categories/{category}/tasks", get(get_tasks_by_category))
        .route(
            "/db-query-logging",
            get(get_db_query_logging).put(put_db_query_logging),
        )
}

async fn get_categories(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<super::task::TaskCategoryStatus>>, AppError> {
    ensure_admin(&headers)?;
    Ok(Json(state.task_supervisor.category_statuses()))
}

async fn get_tasks(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<super::task::ActiveTaskStatus>>, AppError> {
    ensure_admin(&headers)?;
    Ok(Json(state.task_supervisor.active_tasks(None)))
}

async fn get_tasks_by_category(
    State(state): State<Arc<AppState>>,
    Path(category): Path<String>,
    headers: HeaderMap,
) -> Result<Json<Vec<super::task::ActiveTaskStatus>>, AppError> {
    ensure_admin(&headers)?;
    let category = parse_category(&category)?;
    Ok(Json(state.task_supervisor.active_tasks(Some(category))))
}

async fn get_db_query_logging(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<super::task::DbQueryLoggingStatus>, AppError> {
    ensure_admin(&headers)?;
    Ok(Json(state.task_supervisor.db_query_logging_status()))
}

async fn put_db_query_logging(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(payload): Json<DbQueryLoggingUpdate>,
) -> Result<Json<super::task::DbQueryLoggingStatus>, AppError> {
    ensure_admin(&headers)?;
    Ok(Json(
        state.task_supervisor.set_db_query_logging(payload.enabled),
    ))
}

fn ensure_admin(headers: &HeaderMap) -> Result<(), AppError> {
    let is_admin = headers
        .get_all("x-remote-group")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .any(|group| group == "system:masters");
    if is_admin {
        return Ok(());
    }
    Err(AppError::Forbidden(
        "task-supervisor endpoints require admin privileges".to_string(),
    ))
}

fn parse_category(raw: &str) -> Result<TaskCategory, AppError> {
    match raw {
        "background" => Ok(TaskCategory::Background),
        "file" => Ok(TaskCategory::File),
        "db" => Ok(TaskCategory::Db),
        "timer" => Ok(TaskCategory::Timer),
        "network" => Ok(TaskCategory::Network),
        "pod-delete-workqueue" => Ok(TaskCategory::PodDeleteWorkqueue),
        "pod-lifecycle-actor" => Ok(TaskCategory::PodLifecycleActor),
        "pod-lifecycle-work" => Ok(TaskCategory::PodLifecycleWork),
        "pod-probe" => Ok(TaskCategory::PodProbe),
        "others" => Ok(TaskCategory::Others),
        _ => Err(AppError::BadRequest(format!(
            "unknown task supervisor category: {raw}"
        ))),
    }
}
