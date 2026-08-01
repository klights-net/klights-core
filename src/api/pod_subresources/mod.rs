use axum::{
    Json,
    extract::{Path, Query, RawQuery, Request, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use futures::{sink::SinkExt, stream::StreamExt};
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;

use crate::api::{ApiState, AppError, build_admission_context, run_admission_for_request};

// Authorization for all pod subresources is enforced by the global
// `authorize_request` middleware chokepoint (see src/api/auth_middleware.rs);
// handlers no longer authorize individually.

mod binding;
mod ephemeral;
mod eviction;
mod exec;
mod exec_spdy;
mod exec_ws;
pub mod logs;
mod node_proxy;
mod portforward;
mod proxy;
mod spdy_framing;
mod status;
#[cfg(test)]
mod tests;

pub(in crate::api) use self::binding::*;
pub(in crate::api) use self::ephemeral::*;
pub(in crate::api) use self::eviction::*;
pub(in crate::api) use self::exec::*;
pub use self::exec_ws::*;
pub(in crate::api) use self::logs::*;
pub(in crate::api) use self::node_proxy::*;
pub(in crate::api) use self::portforward::*;
pub use self::proxy::MAX_APISERVICE_RESPONSE_BODY_BYTES;
pub use self::proxy::MAX_PROXY_REQUEST_BODY_BYTES;
pub use self::proxy::MAX_PROXY_RESPONSE_BODY_BYTES;
pub(in crate::api) use self::proxy::*;
pub(in crate::api) use self::status::*;
