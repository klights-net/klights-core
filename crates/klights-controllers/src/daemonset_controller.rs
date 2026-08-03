//! `Controller` wrapper for `DaemonSet`.

use crate::controller_wrapper;

controller_wrapper!(
    DaemonSetController,
    "daemonset",
    crate::daemonset::reconcile_daemonset,
    no_node,
    with_pod_repository,
    store = daemonset_store,
    mutation = daemonset_mutation
);
