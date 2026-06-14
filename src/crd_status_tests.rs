use crate::api::add_crd_established_condition;
use crate::api::{merge_stored_versions, validate_api_approval};
use serde_json::json;

#[test]
fn test_add_crd_established_condition() {
    // Create a CRD without status
    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {
            "name": "crontabs.stable.example.com"
        },
        "spec": {
            "group": "stable.example.com",
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {
                    "openAPIV3Schema": {
                        "type": "object",
                        "properties": {
                            "spec": {
                                "type": "object",
                                "properties": {
                                    "cronSpec": {
                                        "type": "string"
                                    }
                                }
                            }
                        }
                    }
                }
            }],
            "scope": "Namespaced",
            "names": {
                "plural": "crontabs",
                "singular": "crontab",
                "kind": "CronTab",
                "shortNames": ["ct"]
            }
        }
    });

    // Add established condition
    let crd_with_status = add_crd_established_condition(crd);

    // Verify status.conditions includes Established: True
    let status = crd_with_status.get("status");
    assert!(status.is_some(), "CRD should have status field");

    let conditions = status
        .and_then(|s| s.get("conditions"))
        .and_then(|c| c.as_array());
    assert!(
        conditions.is_some(),
        "CRD status should have conditions array"
    );

    let established = conditions
        .unwrap()
        .iter()
        .find(|c| c.get("type").and_then(|t| t.as_str()) == Some("Established"));
    assert!(
        established.is_some(),
        "CRD should have Established condition"
    );

    let condition = established.unwrap();
    assert_eq!(
        condition.get("status").and_then(|s| s.as_str()),
        Some("True"),
        "Established condition should be True"
    );
    assert_eq!(
        condition.get("reason").and_then(|r| r.as_str()),
        Some("InitialNamesAccepted"),
        "Established reason should be InitialNamesAccepted"
    );

    // Verify lastTransitionTime is present
    assert!(
        condition.get("lastTransitionTime").is_some(),
        "Condition should have lastTransitionTime"
    );
}

#[test]
fn test_add_crd_established_condition_preserves_existing_fields() {
    // Create a CRD with existing data
    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {
            "name": "widgets.example.com"
        },
        "spec": {
            "group": "example.com",
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {
                    "openAPIV3Schema": {
                        "type": "object"
                    }
                }
            }],
            "scope": "Cluster",
            "names": {
                "plural": "widgets",
                "singular": "widget",
                "kind": "Widget"
            }
        },
        "spec": {
            "versions": [{
                "name": "v1alpha1",
                "served": true
            }]
        }
    });

    // Add established condition
    let crd_with_status = add_crd_established_condition(crd);

    // Verify spec is preserved
    assert_eq!(
        crd_with_status["spec"]["versions"][0]["name"], "v1alpha1",
        "Spec should be preserved"
    );

    // Verify metadata is preserved
    assert_eq!(
        crd_with_status["metadata"]["name"], "widgets.example.com",
        "Metadata should be preserved"
    );

    // Verify status.conditions exists
    let status = crd_with_status.get("status");
    assert!(status.is_some(), "CRD should have status");

    let conditions = status
        .and_then(|s| s.get("conditions"))
        .and_then(|c| c.as_array());
    assert!(conditions.is_some(), "CRD should have conditions");
    assert!(
        !conditions.unwrap().is_empty(),
        "Conditions should not be empty"
    );
}

#[test]
fn test_add_crd_established_condition_includes_names_accepted_and_status_fields() {
    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "foos.example.com"},
        "spec": {
            "group": "example.com",
            "versions": [
                {"name": "v1", "served": true, "storage": true, "schema": {"openAPIV3Schema": {"type": "object"}}},
                {"name": "v2", "served": true, "storage": false}
            ],
            "scope": "Namespaced",
            "names": {"plural": "foos", "singular": "foo", "kind": "Foo"}
        }
    });

    let result = add_crd_established_condition(crd);
    let status = result.get("status").unwrap();

    // NamesAccepted condition
    let conditions = status["conditions"].as_array().unwrap();
    let names_accepted = conditions.iter().find(|c| c["type"] == "NamesAccepted");
    assert!(
        names_accepted.is_some(),
        "Should have NamesAccepted condition"
    );
    assert_eq!(names_accepted.unwrap()["status"], "True");
    assert_eq!(names_accepted.unwrap()["reason"], "NoConflicts");

    // acceptedNames
    let accepted = status.get("acceptedNames").unwrap();
    assert_eq!(accepted["kind"], "Foo");
    assert_eq!(accepted["plural"], "foos");

    // storedVersions (only v1 has storage: true)
    let stored = status["storedVersions"].as_array().unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0], "v1");
}

// ========================
// merge_stored_versions tests
// ========================

#[test]
fn merge_stored_versions_preserves_old_versions() {
    // When updating a CRD, existing storedVersions must be preserved
    // even if the old storage version is no longer the storage version.
    let existing = vec!["v1".to_string()];
    let new_spec_versions = json!([
        {"name": "v1", "served": true, "storage": false},
        {"name": "v2", "served": true, "storage": true}
    ]);
    let result = merge_stored_versions(&existing, &new_spec_versions);
    // Must include both the old v1 and the new storage version v2
    let arr = result.as_array().unwrap();
    assert!(
        arr.iter().any(|v| v == "v1"),
        "old storage version v1 must be preserved"
    );
    assert!(
        arr.iter().any(|v| v == "v2"),
        "new storage version v2 must be present"
    );
}

#[test]
fn merge_stored_versions_no_duplicates() {
    let existing = vec!["v1".to_string()];
    let new_spec_versions = json!([
        {"name": "v1", "served": true, "storage": true}
    ]);
    let result = merge_stored_versions(&existing, &new_spec_versions);
    let arr = result.as_array().unwrap();
    assert_eq!(arr.len(), 1, "should not duplicate v1");
    assert_eq!(arr[0], "v1");
}

#[test]
fn merge_stored_versions_empty_existing() {
    // First create: no existing storedVersions
    let existing: Vec<String> = vec![];
    let new_spec_versions = json!([
        {"name": "v1", "served": true, "storage": true}
    ]);
    let result = merge_stored_versions(&existing, &new_spec_versions);
    let arr = result.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0], "v1");
}

#[test]
fn merge_stored_versions_multiple_existing() {
    let existing = vec!["v1alpha1".to_string(), "v1beta1".to_string()];
    let new_spec_versions = json!([
        {"name": "v1alpha1", "served": true, "storage": false},
        {"name": "v1beta1", "served": true, "storage": false},
        {"name": "v1", "served": true, "storage": true}
    ]);
    let result = merge_stored_versions(&existing, &new_spec_versions);
    let arr = result.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    assert!(arr.iter().any(|v| v == "v1alpha1"));
    assert!(arr.iter().any(|v| v == "v1beta1"));
    assert!(arr.iter().any(|v| v == "v1"));
}

// ========================
// validate_api_approval tests
// ========================

#[test]
fn validate_api_approval_non_protected_group_ok() {
    // Groups not ending in .k8s.io should pass without annotation
    let result = validate_api_approval("example.com", None, None);
    assert!(result.is_ok());
}

#[test]
fn validate_api_approval_protected_group_missing_annotation() {
    let result = validate_api_approval("myapp.k8s.io", None, None);
    assert!(result.is_err());
    let msg = format!("{:?}", result.unwrap_err());
    assert!(msg.contains("api-approved.kubernetes.io"));
}

#[test]
fn validate_api_approval_protected_group_with_url() {
    let annotations = json!({"api-approved.kubernetes.io": "https://github.com/kubernetes/kubernetes/pull/78458"});
    let result = validate_api_approval("myapp.k8s.io", Some(&annotations), None);
    assert!(result.is_ok());
}

#[test]
fn validate_api_approval_protected_group_with_unapproved() {
    let annotations = json!({"api-approved.kubernetes.io": "unapproved"});
    let result = validate_api_approval("myapp.k8s.io", Some(&annotations), None);
    assert!(result.is_ok());
}

#[test]
fn validate_api_approval_protected_group_invalid_value() {
    let annotations = json!({"api-approved.kubernetes.io": "not-a-valid-value"});
    let result = validate_api_approval("myapp.k8s.io", Some(&annotations), None);
    assert!(result.is_err());
}

#[test]
fn validate_api_approval_protected_group_empty_value() {
    let annotations = json!({"api-approved.kubernetes.io": ""});
    let result = validate_api_approval("myapp.k8s.io", Some(&annotations), None);
    assert!(result.is_err());
}

#[test]
fn validate_api_approval_unchanged_on_update() {
    // If annotation hasn't changed, skip validation (even if invalid)
    let old_annotations = json!({"api-approved.kubernetes.io": ""});
    let new_annotations = json!({"api-approved.kubernetes.io": ""});
    let result = validate_api_approval(
        "myapp.k8s.io",
        Some(&new_annotations),
        Some(&old_annotations),
    );
    assert!(
        result.is_ok(),
        "unchanged annotation should skip validation"
    );
}

#[test]
fn validate_api_approval_kubernetes_io_is_protected() {
    // Groups ending in .kubernetes.io are also protected
    let result = validate_api_approval("mypackage.kubernetes.io", None, None);
    assert!(result.is_err());
}

#[test]
fn validate_api_approval_exact_k8s_io_not_protected() {
    // "k8s.io" itself does not end in ".k8s.io"
    let result = validate_api_approval("k8s.io", None, None);
    assert!(result.is_ok());
}
