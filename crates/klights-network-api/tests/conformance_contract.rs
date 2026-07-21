#[test]
fn dependency_free_reference_adapters_pass_the_shared_network_contract_suite() {
    klights_network_api::conformance::run_reference_suite();
}
