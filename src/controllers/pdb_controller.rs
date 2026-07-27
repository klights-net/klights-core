//! `Controller` impl for `PodDisruptionBudget`. Registered in `ControllerDispatcher`.

use crate::controllers::controller_wrapper;
use crate::controllers::pdb as pdb_core;

controller_wrapper!(
    PDBController,
    "poddisruptionbudget",
    pdb_core::reconcile_pdb,
    no_node,
    with_pod_reader,
    store = pdb_store,
    reader = pdb_reader
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controllers::Controller;

    #[test]
    fn test_pdb_controller_name() {
        assert_eq!(PDBController.name(), "poddisruptionbudget");
    }
}
