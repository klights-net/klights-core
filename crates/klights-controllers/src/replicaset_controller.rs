//! `Controller` wrapper for `ReplicaSet`.

use crate::controller_wrapper;

controller_wrapper!(
    ReplicaSetController,
    "replicaset",
    crate::replicaset::reconcile_replicaset,
    with_node,
    with_pod_repository,
    store = replicaset_store,
    reader = pod_query,
    mutation = replicaset_mutation
);
