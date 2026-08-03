//! Transitional registration for root-owned real-adapter controller tests.
//!
//! Phase 18 P2d removes the production `controllers` module. These suites
//! still exercise root datastore and runtime adapters, so P2g owns their
//! migration to the base integration surface. Keeping the registrations
//! explicit and test-only preserves coverage without resurrecting a root
//! production controller owner.

#[path = "../controller_policy_tests/apiservice.rs"]
mod apiservice;

#[cfg(test)]
async fn find_owned_pods(
    datastore: &dyn crate::datastore::DatastoreBackend,
    namespace: &str,
    owner_uid: &str,
) -> anyhow::Result<Vec<klights_cluster_core::Resource>> {
    datastore
        .list_resources_by_owner_uid("v1", "Pod", Some(namespace), owner_uid)
        .await
}

mod common;
mod daemonset;
mod deployment;
mod job;
mod pvc;
mod replicaset;
mod replicationcontroller;
mod statefulset;
