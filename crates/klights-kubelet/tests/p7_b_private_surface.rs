#[test]
fn p7_b_runtime_implementation_leaves_are_external_private() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/p7_b_private_leaves.rs");
}
