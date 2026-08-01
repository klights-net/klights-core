//! Event-driven controller coordination runtime and side-effect registry.

pub mod annotations;
pub mod apiservice;
pub mod common;
mod coordination;
pub mod coredns;
pub mod crd;
pub mod cronjob;
pub mod cronjob_scheduler;
pub mod csr_signer;
pub mod daemonset;
pub mod default_rbac_policy;
pub mod deployment;
mod dispatcher;
pub mod endpoints;
pub mod gc;
pub mod hpa;
mod identity;
pub mod job;
pub mod kube_service;
mod lease_loop;
pub mod namespace;
pub mod node_lease;
pub mod node_lifecycle;
pub mod node_subnet;
pub mod pdb;
mod pod_readiness;
pub mod pvc;
pub mod rbac_reconcile;
pub mod replicaset;
pub mod replicationcontroller;
pub mod resource_projection;
pub mod resource_quota;
pub mod scheduler;
pub mod service;
pub mod side_effects;
pub mod statefulset;
pub mod workqueue;

pub use coordination::{
    ControllerCoordination, ControllerReconcileContext, CoordinatedControllerKind,
};
pub use dispatcher::DispatcherRuntime;
pub use identity::ControllerIdentityGenerator;
pub use lease_loop::run_under_lease;
