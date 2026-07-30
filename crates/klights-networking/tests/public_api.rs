#[test]
fn downstream_public_facade_is_explicit_and_internals_are_unreachable() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/ui/public_facade.rs");
    cases.compile_fail("tests/ui/private_module_escape.rs");
    cases.compile_fail("tests/ui/test_support_feature_escape.rs");
}
