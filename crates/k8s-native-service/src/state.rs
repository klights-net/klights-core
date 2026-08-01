//! Private state shell for the transitional Kubernetes-native service.
//!
//! The six generic family values are assembled by the root composition
//! adapter. Keeping them generic lets the shell own state lifetime and access
//! without reaching any concrete engine, store, replication, or transport
//! implementation while later Phase 17 packets move each family in order.

/// Complete state for the current Kubernetes-native HTTP implementation.
///
/// Fields remain private. The temporary family accessors expose only the exact
/// root-injected bundles already separated by Phase 9F.
#[derive(Clone)]
pub struct ApiState<
    AuthPolicy,
    ResourceMutation,
    Discovery,
    ControllerReconcile,
    PodNodeSubresources,
    Operational,
> {
    auth_policy: AuthPolicy,
    resource_mutation: ResourceMutation,
    discovery: Discovery,
    controller_reconcile: ControllerReconcile,
    pod_node_subresources: PodNodeSubresources,
    operational: Operational,
}

impl<AuthPolicy, ResourceMutation, Discovery, ControllerReconcile, PodNodeSubresources, Operational>
    ApiState<
        AuthPolicy,
        ResourceMutation,
        Discovery,
        ControllerReconcile,
        PodNodeSubresources,
        Operational,
    >
{
    pub fn new(
        auth_policy: AuthPolicy,
        resource_mutation: ResourceMutation,
        discovery: Discovery,
        controller_reconcile: ControllerReconcile,
        pod_node_subresources: PodNodeSubresources,
        operational: Operational,
    ) -> Self {
        Self {
            auth_policy,
            resource_mutation,
            discovery,
            controller_reconcile,
            pod_node_subresources,
            operational,
        }
    }

    pub fn auth_policy(&self) -> &AuthPolicy {
        &self.auth_policy
    }

    pub fn resource_mutation(&self) -> &ResourceMutation {
        &self.resource_mutation
    }

    pub fn discovery(&self) -> &Discovery {
        &self.discovery
    }

    pub fn controller_reconcile(&self) -> &ControllerReconcile {
        &self.controller_reconcile
    }

    pub fn pod_node_subresources(&self) -> &PodNodeSubresources {
        &self.pod_node_subresources
    }

    pub fn operational(&self) -> &Operational {
        &self.operational
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn resource_mutation_mut(&mut self) -> &mut ResourceMutation {
        &mut self.resource_mutation
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn discovery_mut(&mut self) -> &mut Discovery {
        &mut self.discovery
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn controller_reconcile_mut(&mut self) -> &mut ControllerReconcile {
        &mut self.controller_reconcile
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn pod_node_subresources_mut(&mut self) -> &mut PodNodeSubresources {
        &mut self.pod_node_subresources
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn operational_mut(&mut self) -> &mut Operational {
        &mut self.operational
    }
}

#[cfg(any(test, feature = "test-support"))]
impl<AuthPolicy, ResourceMutation, Discovery, ControllerReconcile, PodNodeSubresources, Operational>
    std::ops::Deref
    for ApiState<
        AuthPolicy,
        ResourceMutation,
        Discovery,
        ControllerReconcile,
        PodNodeSubresources,
        Operational,
    >
{
    type Target = AuthPolicy;

    fn deref(&self) -> &Self::Target {
        &self.auth_policy
    }
}

#[cfg(any(test, feature = "test-support"))]
impl<AuthPolicy, ResourceMutation, Discovery, ControllerReconcile, PodNodeSubresources, Operational>
    std::ops::DerefMut
    for ApiState<
        AuthPolicy,
        ResourceMutation,
        Discovery,
        ControllerReconcile,
        PodNodeSubresources,
        Operational,
    >
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.auth_policy
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_preserves_exact_injected_family_identity() {
        let mut state = ApiState::new(1_u8, 2_u16, 3_u32, 4_u64, 5_u128, "operational");
        assert_eq!(*state.auth_policy(), 1);
        assert_eq!(*state.resource_mutation(), 2);
        assert_eq!(*state.discovery(), 3);
        assert_eq!(*state.controller_reconcile(), 4);
        assert_eq!(*state.pod_node_subresources(), 5);
        assert_eq!(*state.operational(), "operational");
        *state.resource_mutation_mut() = 7;
        assert_eq!(*state.resource_mutation(), 7);
    }
}
