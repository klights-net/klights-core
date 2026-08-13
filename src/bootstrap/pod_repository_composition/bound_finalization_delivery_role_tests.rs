// P12.1f: relocated from base:tests/pod_bound_finalization_delivery_role.rs.
// That external binary called
// `klights::pod_repository_composition_test_support::run_local_bound_finalization_with_incidental_delivery_handles`,
// which is now private root composition support (`assembly_support.rs`) and
// unreachable from outside the crate; the test body moves with it.
#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn local_actor_finalization_with_incidental_outbox_stays_synchronous() {
        super::super::assembly_support::support::run_local_bound_finalization_with_incidental_delivery_handles()
            .await
            .expect("local bound-Pod finalization scenario");
    }
}
