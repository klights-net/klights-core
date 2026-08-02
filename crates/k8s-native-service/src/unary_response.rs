use axum::{
    Json,
    body::Body,
    http::HeaderMap,
    response::{IntoResponse, Response},
};
use klights_kube_protobuf::{AcceptValue, ResponseFormat};
use serde_json::Value;

use crate::AppError;

pub struct K8sResponse {
    value: Value,
    format: Result<ResponseFormat, String>,
}

impl K8sResponse {
    pub fn new(value: Value, headers: &HeaderMap) -> Self {
        let api_version = value
            .get("apiVersion")
            .and_then(Value::as_str)
            .unwrap_or("");
        let kind = value.get("kind").and_then(Value::as_str).unwrap_or("");
        let protobuf_supported = kind == "Status"
            || klights_kube_protobuf::supports_protobuf_resource(api_version, kind);
        let format = klights_kube_protobuf::negotiate_unary_response(
            headers.get_all("accept").iter().map(|value| {
                value
                    .to_str()
                    .map_or(AcceptValue::Invalid, AcceptValue::Text)
            }),
            protobuf_supported,
        )
        .map_err(|error| error.to_string());
        Self { value, format }
    }
}

impl IntoResponse for K8sResponse {
    fn into_response(self) -> Response {
        match self.format {
            Err(message) => AppError::NotAcceptable(message).into_response(),
            Ok(ResponseFormat::Json) => Json(self.value).into_response(),
            Ok(ResponseFormat::Protobuf) => {
                match klights_kube_protobuf::encode_protobuf(&self.value) {
                    Ok(bytes) => {
                        let mut response = Response::new(Body::from(bytes));
                        response.headers_mut().insert(
                            "content-type",
                            "application/vnd.kubernetes.protobuf".parse().unwrap(),
                        );
                        response
                    }
                    Err(error) => AppError::InternalError(format!(
                        "failed to encode protobuf response for {}: {error}",
                        self.value
                            .get("kind")
                            .and_then(Value::as_str)
                            .unwrap_or("unknown")
                    ))
                    .into_response(),
                }
            }
        }
    }
}
