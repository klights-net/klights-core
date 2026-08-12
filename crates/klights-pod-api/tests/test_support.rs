#![cfg(feature = "test-support")]

use klights_pod_api::test_support::owner_references_from_values;
use serde_json::json;

#[test]
fn owner_reference_values_preserve_validated_identity_and_flags() {
    let references = owner_references_from_values(vec![json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "name": "owner",
        "uid": "uid-1",
        "controller": true,
        "blockOwnerDeletion": false
    })])
    .expect("valid owner reference");

    assert_eq!(references.len(), 1);
    assert_eq!(references[0].api_version(), "apps/v1");
    assert_eq!(references[0].kind(), "ReplicaSet");
    assert_eq!(references[0].name(), "owner");
    assert_eq!(references[0].uid(), "uid-1");
    assert_eq!(references[0].controller(), Some(true));
    assert_eq!(references[0].block_owner_deletion(), Some(false));
}

#[test]
fn owner_reference_values_reject_missing_required_identity() {
    let error = owner_references_from_values(vec![json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "name": "owner"
    })])
    .expect_err("missing uid must fail closed");

    assert!(error.to_string().contains("missing uid"));
}
