use axum::body::{Body, Bytes};
use axum::extract::FromRequest;
use axum::http::StatusCode;

use crate::AppError;

pub struct LenientJson<T>(pub T);

impl<S, T> FromRequest<S> for LenientJson<T>
where
    T: serde::de::DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = AppError;

    async fn from_request(
        request: axum::http::Request<Body>,
        state: &S,
    ) -> Result<Self, Self::Rejection> {
        let bytes = Bytes::from_request(request, state).await.map_err(|error| {
            let message = format!("Failed to read request body: {error}");
            if error.status() == StatusCode::PAYLOAD_TOO_LARGE {
                AppError::PayloadTooLarge(message)
            } else {
                AppError::BadRequest(message)
            }
        })?;
        let value = if bytes.starts_with(b"k8s\0") {
            klights_kube_protobuf::decode_protobuf(&bytes[4..]).map_err(|error| {
                AppError::BadRequest(format!("Failed to decode protobuf: {error}"))
            })?
        } else {
            serde_json::from_slice(&bytes)
                .map_err(|error| AppError::BadRequest(format!("Invalid JSON: {error}")))?
        };
        serde_json::from_value(value).map(Self).map_err(|error| {
            AppError::BadRequest(format!("Failed to deserialize request payload: {error}"))
        })
    }
}
