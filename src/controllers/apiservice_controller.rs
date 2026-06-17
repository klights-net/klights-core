//! `Controller` impl for `APIService`. Registered in `ControllerDispatcher`.

use crate::controller::controller_wrapper;
use crate::controllers::apiservice as apiservice_core;

controller_wrapper!(
    APIServiceController,
    "apiservice",
    apiservice_core::reconcile_apiservice,
    no_node
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::Controller;

    #[test]
    fn test_apiservice_controller_name() {
        assert_eq!(APIServiceController.name(), "apiservice");
    }
}
