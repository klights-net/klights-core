//! `Controller` wrapper for `PersistentVolumeClaim`.

use crate::controller_wrapper;

controller_wrapper!(
    PVCController,
    "pvc",
    crate::pvc::reconcile_pvc,
    no_node,
    discard,
    with_file_process,
    store = pvc_store
);
