//! Root-owned ambient Kubernetes timestamps.
//!
//! Deterministic wire-format representation belongs to
//! `klights_cluster_core::k8s_time`. These remaining helpers retain only the current
//! ambient clock boundary until their feature owners receive injected clocks.

/// Current `metav1.Time` for non-policy boundary code.
pub fn now_time() -> String {
    klights_cluster_core::k8s_time::format_time(chrono::Utc::now())
}

/// Current `metav1.MicroTime` for non-policy boundary code.
pub fn now_microtime() -> String {
    klights_cluster_core::k8s_time::format_microtime(chrono::Utc::now())
}

/// Current historical timestamp shape for non-policy boundary code.
pub fn now_legacy_timestamp() -> String {
    klights_cluster_core::k8s_time::format_legacy_timestamp(chrono::Utc::now())
}
