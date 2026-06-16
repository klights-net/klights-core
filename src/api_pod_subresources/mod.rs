use axum::{
    Json,
    extract::ws::{Message, WebSocket},
    extract::{Path, Query, RawQuery, Request, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use futures::{sink::SinkExt, stream::StreamExt};
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;

use crate::api::{AppError, AppState, build_admission_context, run_admission_for_request};

// Authorization for all pod subresources is enforced by the global
// `authorize_request` middleware chokepoint (see src/auth/middleware.rs);
// handlers no longer authorize individually.

mod binding;
mod ephemeral;
mod eviction;
mod exec;
pub mod exec_spdy;
mod exec_ws;
pub mod logs;
mod node_proxy;
mod portforward;
mod proxy;
mod status;
#[cfg(test)]
mod tests;

pub use self::binding::*;
pub use self::ephemeral::*;
pub use self::eviction::*;
pub use self::exec::*;
pub use self::exec_ws::*;
pub use self::logs::*;
pub use self::node_proxy::*;
pub use self::portforward::*;
pub use self::proxy::MAX_APISERVICE_RESPONSE_BODY_BYTES;
pub use self::proxy::MAX_PROXY_REQUEST_BODY_BYTES;
pub use self::proxy::MAX_PROXY_RESPONSE_BODY_BYTES;
pub use self::proxy::*;
pub use self::status::*;
