//! Neutral pod lifecycle domain module shared by actor and router.
//!
//! Lives between `pod_lifecycle_actor` and `pod_lifecycle_router` to prevent
//! dependency cycles. Both modules import lifecycle types from here instead
//! of importing each other.

pub mod action;
pub mod concurrency;
pub mod message;
pub mod state;
pub mod trace;
