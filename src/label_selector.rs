use anyhow::{Result, anyhow};
use serde_json::Value;

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
    pub fn parse(selector: &str) -> Result<Self> {
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
    pub fn from_k8s_selector(selector: &Value) -> Result<Self> {
        let mut requirements = Vec::new();
        if let Some(labels) = selector.get("matchLabels").and_then(|v| v.as_object()) {
            for (key, val) in labels {
                let value = val
                    .as_str()
                    .ok_or_else(|| anyhow!("matchLabels[{}] must be a string, got {:?}", key, val))?
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
                    .ok_or_else(|| anyhow!("matchExpressions entry missing key: {}", expr))?
                    .to_string();
                let operator = expr
                    .get("operator")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| anyhow!("matchExpressions[{}] missing operator", key))?;
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
                    other => return Err(anyhow!("unknown matchExpressions operator: {}", other)),
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
    pub fn from_flat_match_labels(value: &Value) -> Result<Self> {
        let obj = value
            .as_object()
            .ok_or_else(|| anyhow!("flat selector must be a JSON object, got {value:?}"))?;
        let mut requirements = Vec::with_capacity(obj.len());
        for (key, val) in obj {
            let v = val
                .as_str()
                .ok_or_else(|| anyhow!("selector[{key}] must be a string, got {val:?}"))?;
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

pub fn parse_label_selector(selector: &str) -> Result<Vec<LabelRequirement>> {
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
                return Err(anyhow!("Invalid selector: {}", part));
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
                return Err(anyhow!("Invalid selector: {}", part));
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
        assert!(LabelSelector::from_k8s_selector(&bad).is_err());
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
