//! Event-driven controller coordination runtime and side-effect registry.

pub mod annotations;
pub mod apiservice;
mod apiservice_controller;
pub mod common;
mod coordination;
pub mod coredns;
pub mod crd;
pub mod cronjob;
pub mod cronjob_scheduler;
pub mod csr_signer;
mod csr_signer_controller;
pub mod daemonset;
mod daemonset_controller;
pub mod default_rbac_policy;
pub mod deployment;
mod deployment_controller;
mod dispatcher;
mod dispatcher_runtime;
pub mod endpoints;
pub mod gc;
pub mod hpa;
mod identity;
#[cfg(test)]
#[path = "tests/support.rs"]
mod internal_test_support;
pub mod job;
mod job_controller;
pub mod kube_service;
mod lease_loop;
pub mod namespace;
pub mod node_lease;
pub mod node_lifecycle;
pub mod node_subnet;
pub mod pdb;
mod pdb_controller;
mod pod_readiness;
mod ports;
pub mod pvc;
mod pvc_controller;
pub mod rbac_reconcile;
pub mod replicaset;
mod replicaset_controller;
mod replication_controller_runner;
pub mod replicationcontroller;
pub mod resource_projection;
pub mod resource_quota;
mod runtime;
pub mod scheduler;
pub mod service;
mod service_controller;
pub mod side_effects;
pub mod statefulset;
mod statefulset_controller;
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
pub mod workqueue;

pub use coordination::{
    ControllerCoordination, ControllerReconcileContext, CoordinatedControllerKind,
};
pub use dispatcher::ControllerDispatcher;
pub use dispatcher_runtime::DispatcherRuntime;
pub use identity::ControllerIdentityGenerator;
pub use lease_loop::run_under_lease;
pub use ports::{
    ControllerEffectPort, ControllerNetworkPort, ControllerPodMutationAdapter,
    ControllerReconcilePort, ControllerResourceQuery, ControllerRuntimeDependencies,
    DeploymentControllerPodMutation,
};
pub(crate) use runtime::controller_wrapper;
pub(crate) use runtime::{Context, Controller};
