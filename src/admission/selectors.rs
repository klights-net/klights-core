use crate::datastore::DatastoreBackend;
use serde_json::Value;
#[cfg(test)]
use std::collections::BTreeMap;

/// Fetch namespace labels from DB for namespaceSelector matching.
#[cfg(test)]
pub(super) async fn get_namespace_labels(
    db: &dyn DatastoreBackend,
    namespace: &str,
) -> BTreeMap<String, String> {
    db.get_namespace(namespace)
        .await
        .ok()
        .flatten()
        .and_then(|ns| {
            ns.data
                .get("metadata")
                .and_then(|m| m.get("labels"))
                .and_then(|l| l.as_object())
                .map(|m| {
                    m.iter()
                        .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                        .collect()
                })
        })
        .unwrap_or_default()
}

/// Fetch namespace labels in their native `serde_json::Map<String, Value>`
/// shape so the cached `LabelSelector` can match against them without an
/// intermediate BTreeMap rebuild on every webhook evaluation.
pub(super) async fn get_namespace_labels_value(
    db: &dyn DatastoreBackend,
    namespace: &str,
) -> Option<serde_json::Map<String, Value>> {
    db.get_namespace(namespace)
        .await
        .ok()
        .flatten()
        .and_then(|ns| {
            ns.data
                .get("metadata")
                .and_then(|m| m.get("labels"))
                .and_then(|l| l.as_object())
                .cloned()
        })
}

/// Check if a label map matches a K8s LabelSelector (matchLabels + matchExpressions).
/// Returns true if selector is None/empty (matches everything).
///
/// Test-only — production code uses `LabelSelector::from_k8s_selector`
/// via `CachedWebhook` for the per-call admission path.
#[cfg(test)]
pub(super) fn matches_label_selector(selector: &Value, labels: &BTreeMap<String, String>) -> bool {
    if let Some(match_labels) = selector.get("matchLabels").and_then(|m| m.as_object()) {
        for (key, val) in match_labels {
            if labels.get(key).map(|s| s.as_str()) != val.as_str() {
                return false;
            }
        }
    }

    if let Some(exprs) = selector.get("matchExpressions").and_then(|e| e.as_array()) {
        for expr in exprs {
            let key = match expr.get("key").and_then(|k| k.as_str()) {
                Some(k) => k,
                None => continue,
            };
            let operator = expr.get("operator").and_then(|o| o.as_str()).unwrap_or("");
            let values: Vec<&str> = expr
                .get("values")
                .and_then(|v| v.as_array())
                .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect())
                .unwrap_or_default();

            let label_present = labels.contains_key(key);
            let label_val = labels.get(key).map(|s| s.as_str());

            let matches = match operator {
                "In" => label_present && values.iter().any(|v| label_val == Some(v)),
                "NotIn" => !label_present || values.iter().all(|v| label_val != Some(v)),
                "Exists" => label_present,
                "DoesNotExist" => !label_present,
                _ => true,
            };

            if !matches {
                return false;
            }
        }
    }

    true
}
