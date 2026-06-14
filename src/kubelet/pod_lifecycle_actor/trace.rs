//! Re-exports from `pod_lifecycle_core` so existing actor call sites
//! keep working without import churn. All new code should import from
//! `pod_lifecycle_core` directly.

pub use crate::kubelet::pod_lifecycle_core::trace::*;
