//! `Controller` wrapper for `StatefulSet`.

use crate::controller_wrapper;

controller_wrapper!(
    StatefulSetController,
    "statefulset",
    crate::statefulset::reconcile_statefulset,
    with_node,
    with_pod_repository,
    store = statefulset_store,
    reader = pod_query,
    mutation = statefulset_mutation
);
