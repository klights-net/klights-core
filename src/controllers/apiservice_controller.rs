//! `Controller` impl for `APIService`. Registered in `ControllerDispatcher`.

use crate::controllers::apiservice as apiservice_core;
use crate::controllers::controller_wrapper;

controller_wrapper!(
    APIServiceController,
    "apiservice",
    apiservice_core::reconcile_apiservice,
    no_node,
    store = apiservice_store
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controllers::Controller;

    #[test]
    fn test_apiservice_controller_name() {
        assert_eq!(APIServiceController.name(), "apiservice");
    }
}
