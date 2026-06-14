//! Multi-node scheduler library (2A-8).
//!
//! Extracts scheduling predicates and scoring into focused modules.
//! This library does NOT change current Pod assignment behavior.
//! Production enablement happens in 2A-9/2A-10.
//!
//! ## Inputs
//! - Pods, Nodes, existing scheduled Pods
//! - Resource requests, taints/tolerations, nodeSelector
//! - Required node affinity, readiness, unschedulable flag
//! - Allocatable resources
//!
//! ## Outputs
//! - Typed scheduling decision: selected node, failed reasons, optional preemption victims

pub mod adapter;
pub mod engine;
pub mod predicates;
pub mod preemption;
pub mod scoring;
pub mod types;
