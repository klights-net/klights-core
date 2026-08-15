#![cfg(feature = "test-support")]

#[test]
fn test_support_exposes_only_focused_store_ports() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/legacy_delivery_facade.rs");
    cases.compile_fail("tests/ui/node_delivery_deref.rs");
}
