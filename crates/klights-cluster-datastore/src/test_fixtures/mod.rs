#![cfg(any(test, feature = "test-support"))]
#![allow(dead_code, unused_imports)]

//! Capability-scoped fixtures for passive persistence tests.

pub(crate) mod commit_observation;
pub mod live_apply;
pub(crate) mod outbox;
pub(crate) mod replicated_create;
