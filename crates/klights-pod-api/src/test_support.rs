//! Canonical pure Pod-domain builders shared by integration tests.

use serde_json::Value;

use crate::{PodOwnerReference, PodRepositoryError};

pub fn owner_references_from_values(
    values: Vec<Value>,
) -> Result<Vec<PodOwnerReference>, PodRepositoryError> {
    values
        .into_iter()
        .map(|value| {
            let required = |field: &'static str| {
                value.get(field).and_then(Value::as_str).ok_or_else(|| {
                    PodRepositoryError::invalid_request(
                        "owner_reference",
                        format!("missing {field}"),
                    )
                })
            };
            PodOwnerReference::try_new(
                required("apiVersion")?,
                required("kind")?,
                required("name")?,
                required("uid")?,
                value.get("controller").and_then(Value::as_bool),
                value.get("blockOwnerDeletion").and_then(Value::as_bool),
            )
        })
        .collect()
}
