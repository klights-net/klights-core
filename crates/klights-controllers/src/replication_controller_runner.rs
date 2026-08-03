//! `Controller` wrapper for `ReplicationController`.

use crate::controller_wrapper;

controller_wrapper!(
    ReplicationControllerController,
    "replicationcontroller",
    crate::replicationcontroller::reconcile_replicationcontroller,
    with_node,
    with_pod_repository,
    store = replicationcontroller_store,
    reader = pod_query,
    mutation = replicationcontroller_mutation
);
