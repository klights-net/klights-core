//! Explicit opt-in test support for native-service consumer tests.

pub mod admission;
pub mod resource;
#[path = "streaming/test_support.rs"]
pub mod streaming;
