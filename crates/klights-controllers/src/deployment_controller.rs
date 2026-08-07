//! `Controller` wrapper for `Deployment`.

use crate::controller_wrapper;

controller_wrapper!(
    DeploymentController,
    "deployment",
    crate::deployment::reconcile_deployment,
    with_node,
    with_pod_repository,
    store = deployment_store,
    reader = pod_query,
    mutation = deployment_mutation
);
