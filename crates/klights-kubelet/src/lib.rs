//! Embedded kubelet implementation for klights.

pub mod context;
pub mod cri_events;
pub mod lifecycle;
pub mod pod_deletion_finalizer;
pub mod pod_lifecycle_actor;
pub mod pod_lifecycle_core;
pub mod pod_lifecycle_router;
pub mod pod_lifecycle_service;
pub mod pod_repository;
pub mod runtime;
pub mod runtime_observations;
pub mod runtime_reconcile_hint;
pub mod runtime_types;
pub mod unscheduled_deletion;

#[cfg(test)]
mod phase15b2_red_tests;
#[cfg(test)]
mod test_support;
