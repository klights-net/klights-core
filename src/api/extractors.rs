use crate::api::AppError;
use axum::body::{Body, Bytes};
use axum::http::StatusCode;

/// Custom JSON extractor that handles both JSON and K8s protobuf request bodies.
/// K8s clients may send protobuf (starts with `k8s\x00`). This extractor decodes
/// protobuf to JSON, then deserializes to the target type T.
pub struct LenientJson<T>(pub T);

pub fn parse_lenient_value_from_bytes(bytes: &[u8]) -> Result<serde_json::Value, AppError> {
    // Check for K8s protobuf magic bytes.
    if bytes.len() >= 4 && &bytes[..4] == b"k8s\x00" {
        crate::protobuf::decode_protobuf(&bytes[4..])
            .map_err(|e| AppError::BadRequest(format!("Failed to decode protobuf: {}", e)))
    } else {
        serde_json::from_slice(bytes)
            .map_err(|e| AppError::BadRequest(format!("Invalid JSON: {}", e)))
    }
}

pub fn decode_json_or_proto(body: &[u8]) -> Result<serde_json::Value, AppError> {
    if body.len() >= 4 && &body[..4] == b"k8s\x00" {
        crate::protobuf::decode_protobuf(&body[4..])
            .map_err(|e| AppError::BadRequest(format!("Failed to decode protobuf: {}", e)))
    } else {
        serde_json::from_slice(body)
            .map_err(|e| AppError::BadRequest(format!("Invalid JSON: {}", e)))
    }
}

impl<S, T> axum::extract::FromRequest<S> for LenientJson<T>
where
    T: serde::de::DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = AppError;

    async fn from_request(
        req: axum::http::Request<Body>,
        state: &S,
    ) -> Result<Self, Self::Rejection> {
        let bytes = Bytes::from_request(req, state).await.map_err(|e| {
            let status = e.status();
            let message = format!("Failed to read request body: {}", e);
            if status == StatusCode::PAYLOAD_TOO_LARGE {
                AppError::PayloadTooLarge(message)
            } else {
                AppError::BadRequest(message)
            }
        })?;

        // Check for K8s protobuf magic bytes
        let json_value = parse_lenient_value_from_bytes(&bytes)?;

        serde_json::from_value(json_value)
            .map(LenientJson)
            .map_err(|e| {
                AppError::BadRequest(format!("Failed to deserialize request payload: {}", e))
            })
    }
}
