//! Kubernetes label-selector parsing and matching.

use std::fmt;

use serde_json::Value;

/// Error returned when a label selector cannot be parsed without changing its
/// Kubernetes matching meaning.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LabelSelectorParseError {
    message: String,
}

impl LabelSelectorParseError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for LabelSelectorParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for LabelSelectorParseError {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LabelSelector {
    requirements: Vec<LabelRequirement>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LabelRequirement {
    Equality { key: String, value: String },
    Inequality { key: String, value: String },
    Exists { key: String },
    NotExists { key: String },
    In { key: String, values: Vec<String> },
    NotIn { key: String, values: Vec<String> },
}

impl LabelRequirement {
    pub fn matches(&self, labels: Option<&serde_json::Map<String, Value>>) -> bool {
        match self {
            LabelRequirement::Equality { key, value } => labels
                .and_then(|l| l.get(key))
                .and_then(|v| v.as_str())
                .map(|v| v == value)
                .unwrap_or(false),
            LabelRequirement::Inequality { key, value } => labels
                .and_then(|l| l.get(key))
                .and_then(|v| v.as_str())
                .map(|v| v != value)
                .unwrap_or(true),
            LabelRequirement::Exists { key } => {
                labels.map(|l| l.contains_key(key)).unwrap_or(false)
            }
            LabelRequirement::NotExists { key } => {
                labels.map(|l| !l.contains_key(key)).unwrap_or(true)
            }
            LabelRequirement::In { key, values } => labels
                .and_then(|l| l.get(key))
                .and_then(|v| v.as_str())
                .map(|v| values.iter().any(|value| value == v))
                .unwrap_or(false),
            LabelRequirement::NotIn { key, values } => labels
                .and_then(|l| l.get(key))
                .and_then(|v| v.as_str())
                .map(|v| values.iter().all(|value| value != v))
                .unwrap_or(true),
        }
    }
}

impl LabelSelector {
    pub fn parse(selector: &str) -> Result<Self, LabelSelectorParseError> {
        Ok(Self {
            requirements: parse_label_selector(selector)?,
        })
    }

    /// Build a selector from the K8s structured shape:
    /// `{ "matchLabels": {...}, "matchExpressions": [{"key", "operator", "values"}] }`.
    /// Used by every workload controller that does selector-based child-pod
    /// matching (ReplicaSet, ReplicationController, StatefulSet, DaemonSet,
    /// Job, Service, NetworkPolicy in Phase 2). A `Value::Null` or `{}`
    /// selector parses to an empty requirements list which `matches_labels`
    /// treats as "everything matches" — matching K8s semantics where an
    /// empty selector denies-all on Service/NetworkPolicy and allows-all on
    /// ReplicaSet (caller decides via separate validation).
    pub fn from_k8s_selector(selector: &Value) -> Result<Self, LabelSelectorParseError> {
        let mut requirements = Vec::new();
        if let Some(labels) = selector.get("matchLabels").and_then(|v| v.as_object()) {
            for (key, val) in labels {
                let value = val
                    .as_str()
                    .ok_or_else(|| {
                        LabelSelectorParseError::new(format!(
                            "matchLabels[{}] must be a string, got {:?}",
                            key, val
                        ))
                    })?
                    .to_string();
                requirements.push(LabelRequirement::Equality {
                    key: key.clone(),
                    value,
                });
            }
        }
        if let Some(exprs) = selector.get("matchExpressions").and_then(|v| v.as_array()) {
            for expr in exprs {
                let key = expr
                    .get("key")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        LabelSelectorParseError::new(format!(
                            "matchExpressions entry missing key: {}",
                            expr
                        ))
                    })?
                    .to_string();
                let operator = expr
                    .get("operator")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        LabelSelectorParseError::new(format!(
                            "matchExpressions[{}] missing operator",
                            key
                        ))
                    })?;
                let collected_values: Vec<String> = expr
                    .get("values")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.as_str())
                            .map(|s| s.to_string())
                            .collect()
                    })
                    .unwrap_or_default();
                let req = match operator {
                    "In" => LabelRequirement::In {
                        key,
                        values: collected_values,
                    },
                    "NotIn" => LabelRequirement::NotIn {
                        key,
                        values: collected_values,
                    },
                    "Exists" => LabelRequirement::Exists { key },
                    "DoesNotExist" => LabelRequirement::NotExists { key },
                    other => {
                        return Err(LabelSelectorParseError::new(format!(
                            "unknown matchExpressions operator: {}",
                            other
                        )));
                    }
                };
                requirements.push(req);
            }
        }
        Ok(Self { requirements })
    }

    pub fn matches_labels(&self, labels: Option<&serde_json::Map<String, Value>>) -> bool {
        self.requirements.iter().all(|req| req.matches(labels))
    }

    /// Build a selector from a flat `{"key": "value"}` label map as used by
    /// ReplicationController `spec.selector`. Unlike `from_k8s_selector`,
    /// which expects the K8s selector shape `{"matchLabels":{...},"matchExpressions":[...]}`,
    /// this directly treats each key-value pair as an equality requirement.
    ///
    /// Returns an error if any value is not a string. An empty object produces
    /// a selector with zero requirements, which `matches_labels` treats as
    /// match-nothing for RC adoption safety.
    pub fn from_flat_match_labels(value: &Value) -> Result<Self, LabelSelectorParseError> {
        let obj = value.as_object().ok_or_else(|| {
            LabelSelectorParseError::new(format!(
                "flat selector must be a JSON object, got {value:?}"
            ))
        })?;
        let mut requirements = Vec::with_capacity(obj.len());
        for (key, val) in obj {
            let v = val.as_str().ok_or_else(|| {
                LabelSelectorParseError::new(format!(
                    "selector[{key}] must be a string, got {val:?}"
                ))
            })?;
            requirements.push(LabelRequirement::Equality {
                key: key.clone(),
                value: v.to_string(),
            });
        }
        Ok(Self { requirements })
    }

    pub fn requirements(&self) -> &[LabelRequirement] {
        &self.requirements
    }

    pub fn matches_resource(&self, resource: &Value) -> bool {
        let labels = resource
            .get("metadata")
            .and_then(|m| m.get("labels"))
            .and_then(|l| l.as_object());
        self.matches_labels(labels)
    }
}

pub fn split_selector(selector: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut paren_depth = 0usize;
    for (i, ch) in selector.char_indices() {
        match ch {
            '(' => paren_depth += 1,
            ')' => paren_depth = paren_depth.saturating_sub(1),
            ',' if paren_depth == 0 => {
                let part = selector[start..i].trim();
                if !part.is_empty() {
                    parts.push(part);
                }
                start = i + 1;
            }
            _ => {}
        }
    }
    let part = selector[start..].trim();
    if !part.is_empty() {
        parts.push(part);
    }
    parts
}

pub fn parse_label_selector(
    selector: &str,
) -> Result<Vec<LabelRequirement>, LabelSelectorParseError> {
    let mut requirements = Vec::new();
    for part in split_selector(selector) {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        if let Some(stripped) = part.strip_prefix('!') {
            requirements.push(LabelRequirement::NotExists {
                key: stripped.trim().to_string(),
            });
            continue;
        }
        if part.contains(" notin (") {
            let parts: Vec<&str> = part.split(" notin (").collect();
            if parts.len() != 2 || !parts[1].ends_with(')') {
                return Err(LabelSelectorParseError::new(format!(
                    "Invalid selector: {}",
                    part
                )));
            }
            let key = parts[0].trim().to_string();
            let values_str = &parts[1][..parts[1].len() - 1];
            let values: Vec<String> = values_str
                .split(',')
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
                .collect();
            requirements.push(LabelRequirement::NotIn { key, values });
            continue;
        }
        if part.contains(" in (") {
            let parts: Vec<&str> = part.split(" in (").collect();
            if parts.len() != 2 || !parts[1].ends_with(')') {
                return Err(LabelSelectorParseError::new(format!(
                    "Invalid selector: {}",
                    part
                )));
            }
            let key = parts[0].trim().to_string();
            let values_str = &parts[1][..parts[1].len() - 1];
            let values: Vec<String> = values_str
                .split(',')
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
                .collect();
            requirements.push(LabelRequirement::In { key, values });
            continue;
        }
        if let Some((key, value)) = part.split_once("!=") {
            requirements.push(LabelRequirement::Inequality {
                key: key.trim().to_string(),
                value: value.trim().to_string(),
            });
            continue;
        }
        if let Some((key, value)) = part.split_once("==") {
            requirements.push(LabelRequirement::Equality {
                key: key.trim().to_string(),
                value: value.trim().to_string(),
            });
            continue;
        }
        if let Some((key, value)) = part.split_once('=') {
            requirements.push(LabelRequirement::Equality {
                key: key.trim().to_string(),
                value: value.trim().to_string(),
            });
            continue;
        }
        requirements.push(LabelRequirement::Exists {
            key: part.to_string(),
        });
    }
    Ok(requirements)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_supports_double_equality() {
        let reqs = parse_label_selector("app==nginx").unwrap();
        assert_eq!(reqs.len(), 1);
        assert!(
            matches!(&reqs[0], LabelRequirement::Equality { key, value } if key == "app" && value == "nginx")
        );
    }

    #[test]
    fn from_k8s_selector_table_driven() {
        // Each row: (selector_value, pod_labels, expected_match, description).
        let cases: &[(Value, Value, bool, &str)] = &[
            (
                json!({}),
                json!({"a": "b"}),
                true,
                "empty selector matches all",
            ),
            (
                json!({"matchLabels": {"app": "nginx"}}),
                json!({"app": "nginx", "tier": "fe"}),
                true,
                "matchLabels equality match",
            ),
            (
                json!({"matchLabels": {"app": "nginx"}}),
                json!({"app": "redis"}),
                false,
                "matchLabels equality miss",
            ),
            (
                json!({"matchExpressions": [{"key": "tier", "operator": "In", "values": ["fe", "be"]}]}),
                json!({"tier": "fe"}),
                true,
                "matchExpressions In match",
            ),
            (
                json!({"matchExpressions": [{"key": "tier", "operator": "In", "values": ["fe", "be"]}]}),
                json!({"tier": "data"}),
                false,
                "matchExpressions In miss",
            ),
            (
                json!({"matchExpressions": [{"key": "tier", "operator": "NotIn", "values": ["fe"]}]}),
                json!({"tier": "be"}),
                true,
                "matchExpressions NotIn match",
            ),
            (
                json!({"matchExpressions": [{"key": "tier", "operator": "NotIn", "values": ["fe"]}]}),
                json!({"tier": "fe"}),
                false,
                "matchExpressions NotIn miss",
            ),
            (
                json!({"matchExpressions": [{"key": "has-gpu", "operator": "Exists"}]}),
                json!({"has-gpu": "yes"}),
                true,
                "matchExpressions Exists match",
            ),
            (
                json!({"matchExpressions": [{"key": "has-gpu", "operator": "Exists"}]}),
                json!({"role": "worker"}),
                false,
                "matchExpressions Exists miss",
            ),
            (
                json!({"matchExpressions": [{"key": "deprecated", "operator": "DoesNotExist"}]}),
                json!({"role": "worker"}),
                true,
                "matchExpressions DoesNotExist match",
            ),
            (
                json!({"matchExpressions": [{"key": "deprecated", "operator": "DoesNotExist"}]}),
                json!({"deprecated": "true"}),
                false,
                "matchExpressions DoesNotExist miss",
            ),
            (
                json!({
                    "matchLabels": {"app": "nginx"},
                    "matchExpressions": [{"key": "tier", "operator": "In", "values": ["fe"]}]
                }),
                json!({"app": "nginx", "tier": "fe"}),
                true,
                "combined matchLabels + matchExpressions match",
            ),
            (
                json!({
                    "matchLabels": {"app": "nginx"},
                    "matchExpressions": [{"key": "tier", "operator": "In", "values": ["fe"]}]
                }),
                json!({"app": "redis", "tier": "fe"}),
                false,
                "combined miss when matchLabels fails",
            ),
        ];
        for (selector, labels, expected, desc) in cases {
            let parsed = LabelSelector::from_k8s_selector(selector)
                .unwrap_or_else(|e| panic!("{desc}: parse failed: {e}"));
            let labels_map = labels.as_object();
            assert_eq!(
                parsed.matches_labels(labels_map),
                *expected,
                "{desc}: selector={selector} labels={labels}"
            );
        }
    }

    #[test]
    fn from_k8s_selector_rejects_unknown_operator() {
        let bad =
            json!({"matchExpressions": [{"key": "x", "operator": "GreaterThan", "values": ["1"]}]});
        assert_eq!(
            LabelSelector::from_k8s_selector(&bad)
                .expect_err("unknown operator must fail")
                .to_string(),
            "unknown matchExpressions operator: GreaterThan"
        );
    }

    #[test]
    fn structured_selector_compatibility_errors_are_stable() {
        fn assert_narrow_error<T: std::error::Error + Send + Sync + 'static>() {}
        assert_narrow_error::<LabelSelectorParseError>();

        let cases = [
            (
                json!({"matchLabels": {"app": 1}}),
                "matchLabels[app] must be a string, got Number(1)",
            ),
            (
                json!({"matchExpressions": [{"operator": "Exists"}]}),
                "matchExpressions entry missing key: {\"operator\":\"Exists\"}",
            ),
            (
                json!({"matchExpressions": [{"key": "tier"}]}),
                "matchExpressions[tier] missing operator",
            ),
        ];

        for (selector, expected) in cases {
            assert_eq!(
                LabelSelector::from_k8s_selector(&selector)
                    .expect_err("invalid structured selector must fail")
                    .to_string(),
                expected
            );
        }

        for selector in [Value::Null, json!("not-an-object"), json!([])] {
            let parsed = LabelSelector::from_k8s_selector(&selector)
                .expect("legacy non-object selector is empty");
            assert!(parsed.requirements().is_empty());
        }
    }

    #[test]
    fn absent_and_non_string_label_matching_truth_table_is_stable() {
        let labels = json!({"present": 7});
        let labels = labels.as_object();
        let cases = [
            (
                LabelRequirement::Equality {
                    key: "missing".into(),
                    value: "x".into(),
                },
                false,
            ),
            (
                LabelRequirement::Inequality {
                    key: "missing".into(),
                    value: "x".into(),
                },
                true,
            ),
            (
                LabelRequirement::Exists {
                    key: "present".into(),
                },
                true,
            ),
            (
                LabelRequirement::NotExists {
                    key: "present".into(),
                },
                false,
            ),
            (
                LabelRequirement::In {
                    key: "missing".into(),
                    values: vec!["x".into()],
                },
                false,
            ),
            (
                LabelRequirement::NotIn {
                    key: "missing".into(),
                    values: vec!["x".into()],
                },
                true,
            ),
        ];

        for (requirement, expected) in cases {
            assert_eq!(requirement.matches(labels), expected, "{requirement:?}");
        }
    }

    #[test]
    fn test_label_requirement_equality_matches() {
        let labels: serde_json::Map<String, Value> =
            serde_json::from_value(json!({"app": "nginx", "env": "prod"})).unwrap();
        let req = LabelRequirement::Equality {
            key: "app".to_string(),
            value: "nginx".to_string(),
        };
        assert!(req.matches(Some(&labels)));
    }

    #[test]
    fn test_label_requirement_equality_no_match() {
        let labels: serde_json::Map<String, Value> =
            serde_json::from_value(json!({"app": "redis"})).unwrap();
        let req = LabelRequirement::Equality {
            key: "app".to_string(),
            value: "nginx".to_string(),
        };
        assert!(!req.matches(Some(&labels)));
    }

    #[test]
    fn test_label_requirement_equality_missing_key() {
        let labels: serde_json::Map<String, Value> =
            serde_json::from_value(json!({"env": "prod"})).unwrap();
        let req = LabelRequirement::Equality {
            key: "app".to_string(),
            value: "nginx".to_string(),
        };
        assert!(!req.matches(Some(&labels)));
    }

    #[test]
    fn test_label_requirement_equality_no_labels() {
        let req = LabelRequirement::Equality {
            key: "app".to_string(),
            value: "nginx".to_string(),
        };
        assert!(!req.matches(None));
    }

    #[test]
    fn test_label_requirement_inequality_matches() {
        let labels: serde_json::Map<String, Value> =
            serde_json::from_value(json!({"env": "staging"})).unwrap();
        let req = LabelRequirement::Inequality {
            key: "env".to_string(),
            value: "prod".to_string(),
        };
        assert!(req.matches(Some(&labels)));
    }

    #[test]
    fn test_label_requirement_inequality_no_match() {
        let labels: serde_json::Map<String, Value> =
            serde_json::from_value(json!({"env": "prod"})).unwrap();
        let req = LabelRequirement::Inequality {
            key: "env".to_string(),
            value: "prod".to_string(),
        };
        assert!(!req.matches(Some(&labels)));
    }

    #[test]
    fn test_label_requirement_inequality_missing_key_returns_true() {
        let labels: serde_json::Map<String, Value> =
            serde_json::from_value(json!({"app": "nginx"})).unwrap();
        let req = LabelRequirement::Inequality {
            key: "env".to_string(),
            value: "prod".to_string(),
        };
        // K8s spec: inequality with missing key returns true (label doesn't equal value)
        assert!(req.matches(Some(&labels)));
    }

    #[test]
    fn test_label_requirement_exists_matches() {
        let labels: serde_json::Map<String, Value> =
            serde_json::from_value(json!({"has-gpu": "true"})).unwrap();
        let req = LabelRequirement::Exists {
            key: "has-gpu".to_string(),
        };
        assert!(req.matches(Some(&labels)));
    }

    #[test]
    fn test_label_requirement_exists_missing_key() {
        let labels: serde_json::Map<String, Value> =
            serde_json::from_value(json!({"app": "nginx"})).unwrap();
        let req = LabelRequirement::Exists {
            key: "has-gpu".to_string(),
        };
        assert!(!req.matches(Some(&labels)));
    }

    #[test]
    fn test_label_requirement_exists_no_labels() {
        let req = LabelRequirement::Exists {
            key: "app".to_string(),
        };
        assert!(!req.matches(None));
    }

    #[test]
    fn test_label_requirement_not_exists_matches() {
        let labels: serde_json::Map<String, Value> =
            serde_json::from_value(json!({"app": "nginx"})).unwrap();
        let req = LabelRequirement::NotExists {
            key: "deprecated".to_string(),
        };
        assert!(req.matches(Some(&labels)));
    }

    #[test]
    fn test_label_requirement_not_exists_key_present() {
        let labels: serde_json::Map<String, Value> =
            serde_json::from_value(json!({"deprecated": "true"})).unwrap();
        let req = LabelRequirement::NotExists {
            key: "deprecated".to_string(),
        };
        assert!(!req.matches(Some(&labels)));
    }

    #[test]
    fn test_label_requirement_not_exists_no_labels() {
        let req = LabelRequirement::NotExists {
            key: "deprecated".to_string(),
        };
        // K8s spec: NotExists with no labels returns true
        assert!(req.matches(None));
    }

    #[test]
    fn test_label_requirement_in_matches() {
        let labels: serde_json::Map<String, Value> =
            serde_json::from_value(json!({"env": "prod"})).unwrap();
        let req = LabelRequirement::In {
            key: "env".to_string(),
            values: vec!["prod".to_string(), "staging".to_string()],
        };
        assert!(req.matches(Some(&labels)));
    }

    #[test]
    fn test_label_requirement_in_no_match() {
        let labels: serde_json::Map<String, Value> =
            serde_json::from_value(json!({"env": "dev"})).unwrap();
        let req = LabelRequirement::In {
            key: "env".to_string(),
            values: vec!["prod".to_string(), "staging".to_string()],
        };
        assert!(!req.matches(Some(&labels)));
    }

    #[test]
    fn test_label_requirement_in_missing_key() {
        let labels: serde_json::Map<String, Value> =
            serde_json::from_value(json!({"app": "nginx"})).unwrap();
        let req = LabelRequirement::In {
            key: "env".to_string(),
            values: vec!["prod".to_string()],
        };
        assert!(!req.matches(Some(&labels)));
    }

    #[test]
    fn test_label_requirement_notin_matches() {
        let labels: serde_json::Map<String, Value> =
            serde_json::from_value(json!({"env": "prod"})).unwrap();
        let req = LabelRequirement::NotIn {
            key: "env".to_string(),
            values: vec!["dev".to_string(), "test".to_string()],
        };
        assert!(req.matches(Some(&labels)));
    }

    #[test]
    fn test_label_requirement_notin_no_match() {
        let labels: serde_json::Map<String, Value> =
            serde_json::from_value(json!({"env": "dev"})).unwrap();
        let req = LabelRequirement::NotIn {
            key: "env".to_string(),
            values: vec!["dev".to_string(), "test".to_string()],
        };
        assert!(!req.matches(Some(&labels)));
    }

    #[test]
    fn test_label_requirement_notin_missing_key_returns_true() {
        let labels: serde_json::Map<String, Value> =
            serde_json::from_value(json!({"app": "nginx"})).unwrap();
        let req = LabelRequirement::NotIn {
            key: "env".to_string(),
            values: vec!["dev".to_string()],
        };
        // K8s spec: NotIn with missing key returns true (label value is not in set)
        assert!(req.matches(Some(&labels)));
    }

    #[test]
    fn exists_and_not_exists_evaluate_against_labels() {
        let resource = json!({
            "metadata": {
                "labels": {
                    "has-gpu": "true",
                    "tier": "prod"
                }
            }
        });

        let exists = LabelSelector::parse("has-gpu").unwrap();
        assert!(exists.matches_resource(&resource));

        let not_exists = LabelSelector::parse("!deprecated").unwrap();
        assert!(not_exists.matches_resource(&resource));

        let must_fail = LabelSelector::parse("deprecated").unwrap();
        assert!(!must_fail.matches_resource(&resource));
    }
}
