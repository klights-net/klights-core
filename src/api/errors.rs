use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};

#[derive(Debug)]
pub enum AppError {
    Unauthorized(String),
    NotFound(String),
    BadRequest(String),
    AlreadyExists(String),
    Conflict(String),
    NotImplemented(String),
    NotAcceptable(String),
    /// 405: the matched path does not support the request method.
    MethodNotAllowed(String),
    Forbidden(String),
    UnsupportedMediaType(String),
    UnprocessableEntity(String),
    BadGateway(String),
    InternalError(String),
    Internal(String),
    /// 503 Service Unavailable: allocator not ready (F6-02)
    ServiceUnavailable(String),
    /// 410 Gone: continue token expired; carries the inconsistent continuation token.
    ResourceExpired(String),
    /// 413 Payload Too Large / Request Entity Too Large.
    PayloadTooLarge(String),
    /// Fully-structured error carrying a `metav1.Status.details` object
    /// (`{group,kind,name,causes}`). Produced by the builder constructors below
    /// so 404/409/422 responses are spec-shaped.
    Status {
        code: StatusCode,
        reason: &'static str,
        message: String,
        details: serde_json::Value,
    },
}

/// One `metav1.StatusCause` (`{reason, message, field}`) for `Invalid` errors.
#[derive(Debug, Clone)]
pub struct StatusCause {
    pub reason: String,
    pub message: String,
    pub field: String,
}

/// Group component of an `apiVersion` (`""` for the core group).
fn group_of(api_version: &str) -> &str {
    api_version.split_once('/').map(|(g, _)| g).unwrap_or("")
}

fn resource_details(api_version: &str, kind: &str, name: &str) -> serde_json::Value {
    let mut details = serde_json::Map::new();
    let group = group_of(api_version);
    if !group.is_empty() {
        details.insert("group".to_string(), serde_json::json!(group));
    }
    details.insert("kind".to_string(), serde_json::json!(kind));
    details.insert("name".to_string(), serde_json::json!(name));
    serde_json::Value::Object(details)
}

impl AppError {
    /// 404 NotFound carrying `details.{group,kind,name}`.
    pub fn not_found(api_version: &str, kind: &str, name: &str) -> Self {
        AppError::Status {
            code: StatusCode::NOT_FOUND,
            reason: "NotFound",
            message: format!("{kind} \"{name}\" not found"),
            details: resource_details(api_version, kind, name),
        }
    }

    /// 409 AlreadyExists carrying `details.{group,kind,name}`.
    pub fn already_exists(api_version: &str, kind: &str, name: &str) -> Self {
        AppError::Status {
            code: StatusCode::CONFLICT,
            reason: "AlreadyExists",
            message: format!("{kind} \"{name}\" already exists"),
            details: resource_details(api_version, kind, name),
        }
    }

    /// 422 Invalid carrying `details.{group,kind,name,causes[]}`.
    pub fn invalid(api_version: &str, kind: &str, name: &str, causes: Vec<StatusCause>) -> Self {
        let mut details = resource_details(api_version, kind, name);
        let cause_values: Vec<serde_json::Value> = causes
            .iter()
            .map(|c| {
                serde_json::json!({
                    "reason": c.reason,
                    "message": c.message,
                    "field": c.field,
                })
            })
            .collect();
        if let Some(obj) = details.as_object_mut() {
            obj.insert("causes".to_string(), serde_json::json!(cause_values));
        }
        let summary = causes
            .iter()
            .map(|c| format!("{}: {}", c.field, c.message))
            .collect::<Vec<_>>()
            .join(", ");
        AppError::Status {
            code: StatusCode::UNPROCESSABLE_ENTITY,
            reason: "Invalid",
            message: format!("{kind} \"{name}\" is invalid: {summary}"),
            details,
        }
    }

    /// 410 Gone with reason `Expired` for a LIST whose requested
    /// `resourceVersion` is too old to reconstruct (resourceVersionMatch=Exact).
    pub fn expired(message: impl Into<String>) -> Self {
        AppError::Status {
            code: StatusCode::GONE,
            reason: "Expired",
            message: message.into(),
            details: serde_json::Value::Null,
        }
    }

    /// Upgrade a string-based NotFound/AlreadyExists/Conflict (e.g. one mapped
    /// from a datastore error) into a structured `Status` carrying
    /// `details.{group,kind,name}`, preserving the original message. Other
    /// variants pass through unchanged.
    pub fn with_resource_context(self, api_version: &str, kind: &str, name: &str) -> Self {
        let (code, reason, message) = match self {
            AppError::NotFound(msg) => (StatusCode::NOT_FOUND, "NotFound", msg),
            AppError::AlreadyExists(msg) => (StatusCode::CONFLICT, "AlreadyExists", msg),
            AppError::Conflict(msg) => (StatusCode::CONFLICT, "Conflict", msg),
            other => return other,
        };
        AppError::Status {
            code,
            reason,
            message,
            details: resource_details(api_version, kind, name),
        }
    }
}

impl From<anyhow::Error> for AppError {
    fn from(err: anyhow::Error) -> Self {
        if err
            .downcast_ref::<crate::datastore::errors::DatastoreError>()
            .is_some_and(crate::datastore::errors::DatastoreError::is_conflict)
        {
            return AppError::Conflict(err.to_string());
        }
        let msg = err.to_string();
        if msg.contains("already exists") && msg.contains("409 Conflict") {
            AppError::AlreadyExists(msg)
        } else if msg.contains("409 Conflict") {
            AppError::Conflict(msg)
        } else if msg.contains("not found") {
            AppError::NotFound(msg)
        } else {
            AppError::Internal(msg)
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        if let AppError::Status {
            code,
            reason,
            message,
            details,
        } = self
        {
            let mut body = serde_json::json!({
                "kind": "Status",
                "apiVersion": "v1",
                "metadata": {},
                "status": "Failure",
                "message": message,
                "code": code.as_u16(),
            });
            if !reason.is_empty() {
                body["reason"] = serde_json::Value::String(reason.to_string());
            }
            if details.as_object().is_some_and(|o| !o.is_empty()) {
                body["details"] = details;
            }
            return (code, Json(body)).into_response();
        }

        if let AppError::ResourceExpired(inconsistent_token) = self {
            let body = serde_json::json!({
                "kind": "Status",
                "apiVersion": "v1",
                "metadata": {"continue": inconsistent_token},
                "status": "Failure",
                "message": "The provided from parameter is too old to display a consistent list result. You must start a new list without the from parameter.",
                "reason": "Expired",
                "code": 410u16,
            });
            return (StatusCode::GONE, Json(body)).into_response();
        }

        let (status, reason, message) = match self {
            AppError::Unauthorized(msg) => (StatusCode::UNAUTHORIZED, "Unauthorized", msg),
            AppError::NotFound(msg) => (StatusCode::NOT_FOUND, "NotFound", msg),
            AppError::BadRequest(msg) => (StatusCode::BAD_REQUEST, "BadRequest", msg),
            AppError::AlreadyExists(msg) => (StatusCode::CONFLICT, "AlreadyExists", msg),
            AppError::Conflict(msg) => (StatusCode::CONFLICT, "Conflict", msg),
            AppError::NotImplemented(msg) => (StatusCode::NOT_IMPLEMENTED, "", msg),
            AppError::NotAcceptable(msg) => (StatusCode::NOT_ACCEPTABLE, "NotAcceptable", msg),
            AppError::MethodNotAllowed(msg) => {
                (StatusCode::METHOD_NOT_ALLOWED, "MethodNotAllowed", msg)
            }
            AppError::Forbidden(msg) => (StatusCode::FORBIDDEN, "Forbidden", msg),
            AppError::UnsupportedMediaType(msg) => (
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "UnsupportedMediaType",
                msg,
            ),
            AppError::UnprocessableEntity(msg) => {
                (StatusCode::UNPROCESSABLE_ENTITY, "Invalid", msg)
            }
            // 502/501 have no upstream metav1.StatusReason; reason stays empty
            // (omitted from the Status body) rather than a klights-invented value.
            AppError::BadGateway(msg) => (StatusCode::BAD_GATEWAY, "", msg),
            AppError::InternalError(msg) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "InternalError", msg)
            }
            AppError::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, "InternalError", msg),
            AppError::ServiceUnavailable(msg) => {
                (StatusCode::SERVICE_UNAVAILABLE, "ServiceUnavailable", msg)
            }
            AppError::PayloadTooLarge(msg) => {
                (StatusCode::PAYLOAD_TOO_LARGE, "RequestEntityTooLarge", msg)
            }
            AppError::ResourceExpired(_) => unreachable!("handled above"),
            AppError::Status { .. } => unreachable!("handled above"),
        };

        let mut body = serde_json::json!({
            "kind": "Status",
            "apiVersion": "v1",
            "metadata": {},
            "status": "Failure",
            "message": message,
            "code": status.as_u16(),
        });
        // `reason` is omitempty in metav1.Status: only include it when it maps to
        // a real upstream StatusReason. 502/501 carry no reason.
        if !reason.is_empty() {
            body["reason"] = serde_json::Value::String(reason.to_string());
        }

        (status, Json(body)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn body_of(err: AppError) -> (StatusCode, serde_json::Value) {
        let resp = err.into_response();
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    #[tokio::test]
    async fn bad_gateway_and_not_implemented_omit_reason() {
        let (status, body) = body_of(AppError::BadGateway("upstream down".into())).await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert!(
            body.get("reason").is_none(),
            "502 must not carry a non-standard reason: {body}"
        );
        assert_eq!(body["status"], "Failure");

        let (status, body) = body_of(AppError::NotImplemented("nope".into())).await;
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
        assert!(body.get("reason").is_none(), "501 must omit reason: {body}");
    }

    #[tokio::test]
    async fn standard_reasons_are_present() {
        let (status, body) = body_of(AppError::NotFound("pods \"x\" not found".into())).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["reason"], "NotFound");
    }

    #[tokio::test]
    async fn not_found_carries_group_kind_name_details() {
        let (status, body) = body_of(AppError::not_found("apps/v1", "Deployment", "web")).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["reason"], "NotFound");
        assert_eq!(body["details"]["group"], "apps");
        assert_eq!(body["details"]["kind"], "Deployment");
        assert_eq!(body["details"]["name"], "web");

        // Core group is omitted (empty), kind/name present.
        let (_, body) = body_of(AppError::not_found("v1", "ConfigMap", "cm")).await;
        assert!(body["details"].get("group").is_none());
        assert_eq!(body["details"]["kind"], "ConfigMap");
        assert_eq!(body["details"]["name"], "cm");
    }

    #[tokio::test]
    async fn already_exists_carries_details() {
        let (status, body) = body_of(AppError::already_exists("v1", "ConfigMap", "cm")).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body["reason"], "AlreadyExists");
        assert_eq!(body["details"]["kind"], "ConfigMap");
        assert_eq!(body["details"]["name"], "cm");
    }

    #[tokio::test]
    async fn with_resource_context_upgrades_string_variants() {
        let err = AppError::AlreadyExists("configmaps \"cm\" already exists".into())
            .with_resource_context("v1", "ConfigMap", "cm");
        let (status, body) = body_of(err).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body["reason"], "AlreadyExists");
        assert_eq!(body["details"]["name"], "cm");
        // Original message is preserved.
        assert_eq!(body["message"], "configmaps \"cm\" already exists");
    }

    #[tokio::test]
    async fn invalid_carries_causes() {
        let err = AppError::invalid(
            "v1",
            "Pod",
            "p",
            vec![StatusCause {
                reason: "FieldValueRequired".into(),
                message: "Required value".into(),
                field: "spec.containers".into(),
            }],
        );
        let (status, body) = body_of(err).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(body["reason"], "Invalid");
        let causes = body["details"]["causes"].as_array().unwrap();
        assert_eq!(causes.len(), 1);
        assert_eq!(causes[0]["reason"], "FieldValueRequired");
        assert_eq!(causes[0]["field"], "spec.containers");
        assert_eq!(causes[0]["message"], "Required value");
    }
}

pub fn map_mutating_admission_error(err: anyhow::Error) -> AppError {
    AppError::InternalError(format!("Mutating webhook failed: {}", err))
}

pub fn map_validating_admission_error(err: anyhow::Error) -> AppError {
    let msg = err.to_string();
    if msg.contains("Admission denied by webhook:") {
        AppError::Forbidden(format!("Validating webhook denied: {}", msg))
    } else if msg.contains("Admission denied by policy:") {
        AppError::UnprocessableEntity(msg)
    } else if msg.contains("sideEffects does not allow dryRun") {
        AppError::BadRequest(msg)
    } else {
        AppError::InternalError(format!("Validating webhook failed: {}", msg))
    }
}
