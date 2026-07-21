use std::borrow::Cow;
use std::fmt;

use serde_json::Value;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FieldSelectorOperator {
    Equals,
    NotEquals,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FieldRequirement {
    field: String,
    operator: FieldSelectorOperator,
    value: String,
}

impl FieldRequirement {
    pub fn field(&self) -> &str {
        &self.field
    }

    pub const fn operator(&self) -> FieldSelectorOperator {
        self.operator
    }

    pub fn value(&self) -> &str {
        &self.value
    }

    pub fn matches_resource(&self, object: &Value) -> bool {
        let equal = resolve_field_value(object, &self.field)
            .as_deref()
            .unwrap_or("")
            == self.value;
        match self.operator {
            FieldSelectorOperator::Equals => equal,
            FieldSelectorOperator::NotEquals => !equal,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FieldSelector {
    requirements: Vec<FieldRequirement>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FieldSelectorParseError {
    message: String,
}

impl fmt::Display for FieldSelectorParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for FieldSelectorParseError {}

impl FieldSelector {
    pub fn parse(selector: &str) -> Result<Self, FieldSelectorParseError> {
        let mut requirements = Vec::new();
        let mut terms = split_terms(selector);
        terms.sort_unstable();
        for raw in terms {
            if raw.is_empty() {
                continue;
            }
            let (field, operator, value) = parse_requirement(raw)?;
            requirements.push(FieldRequirement {
                field: field.to_string(),
                operator,
                value: unescape_value(value)?,
            });
        }
        Ok(Self { requirements })
    }

    pub fn requirements(&self) -> &[FieldRequirement] {
        &self.requirements
    }

    pub fn matches_resource(&self, object: &Value) -> bool {
        self.requirements
            .iter()
            .all(|requirement| requirement.matches_resource(object))
    }
}

fn parse_requirement(
    raw: &str,
) -> Result<(&str, FieldSelectorOperator, &str), FieldSelectorParseError> {
    for (index, _) in raw.char_indices() {
        let remaining = &raw[index..];
        for (token, operator) in [
            ("!=", FieldSelectorOperator::NotEquals),
            ("==", FieldSelectorOperator::Equals),
            ("=", FieldSelectorOperator::Equals),
        ] {
            if remaining.starts_with(token) {
                return Ok((&raw[..index], operator, &remaining[token.len()..]));
            }
        }
    }
    Err(parse_error(format!(
        "invalid selector: can't understand requirement {raw:?}"
    )))
}

fn split_terms(selector: &str) -> Vec<&str> {
    if selector.is_empty() {
        return Vec::new();
    }
    let mut terms = Vec::new();
    let mut start = 0;
    let mut escaped = false;
    for (index, character) in selector.char_indices() {
        if escaped {
            escaped = false;
        } else if character == '\\' {
            escaped = true;
        } else if character == ',' {
            terms.push(&selector[start..index]);
            start = index + character.len_utf8();
        }
    }
    terms.push(&selector[start..]);
    terms
}

fn unescape_value(value: &str) -> Result<String, FieldSelectorParseError> {
    if !value
        .chars()
        .any(|character| matches!(character, '\\' | ',' | '='))
    {
        return Ok(value.to_string());
    }
    let mut unescaped = String::with_capacity(value.len());
    let mut characters = value.chars();
    while let Some(character) = characters.next() {
        match character {
            '\\' => match characters.next() {
                Some(escaped @ ('\\' | ',' | '=')) => unescaped.push(escaped),
                Some(other) => {
                    return Err(parse_error(format!(
                        "invalid field selector escape sequence: \\{other}"
                    )));
                }
                None => {
                    return Err(parse_error(
                        "invalid field selector escape sequence: trailing backslash",
                    ));
                }
            },
            ',' | '=' => {
                return Err(parse_error(format!(
                    "invalid field selector: unescaped character in value: {character}"
                )));
            }
            other => unescaped.push(other),
        }
    }
    Ok(unescaped)
}

fn parse_error(message: impl Into<String>) -> FieldSelectorParseError {
    FieldSelectorParseError {
        message: message.into(),
    }
}

pub fn resolve_field_value<'a>(object: &'a Value, path: &str) -> Option<Cow<'a, str>> {
    fn nonempty_string(value: Option<&Value>) -> Option<&str> {
        value
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
    }

    if path == "source" {
        for value in [
            object
                .get("source")
                .and_then(|source| source.get("component")),
            object
                .get("deprecatedSource")
                .and_then(|source| source.get("component")),
            object.get("reportingController"),
            object.get("reportingComponent"),
        ] {
            if let Some(component) = nonempty_string(value) {
                return Some(Cow::Borrowed(component));
            }
        }
    }

    if let Some(suffix) = path.strip_prefix("involvedObject.") {
        let mut current = object
            .get("involvedObject")
            .or_else(|| object.get("regarding"))?;
        for segment in suffix.split('.') {
            current = current.get(segment)?;
        }
        return match current {
            Value::String(value) => Some(Cow::Borrowed(value)),
            Value::Bool(value) => Some(Cow::Owned(value.to_string())),
            Value::Number(value) => Some(Cow::Owned(value.to_string())),
            _ => None,
        };
    }

    let mut current = object;
    for segment in path.split('.') {
        current = current.get(segment)?;
    }
    match current {
        Value::String(value) => Some(Cow::Borrowed(value)),
        Value::Bool(value) => Some(Cow::Owned(value.to_string())),
        Value::Number(value) => Some(Cow::Owned(value.to_string())),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::FieldSelector;

    #[test]
    fn strict_parser_accepts_canonical_operators_and_matches() {
        let object = json!({"metadata": {"name": "selected"}, "spec": {"ready": true}});
        for selector in [
            "metadata.name=selected",
            "metadata.name==selected",
            "metadata.name!=other",
            "metadata.name=selected,spec.ready=true",
        ] {
            assert!(
                FieldSelector::parse(selector)
                    .unwrap()
                    .matches_resource(&object)
            );
        }
    }

    #[test]
    fn strict_parser_rejects_bare_and_invalid_operator_requirements() {
        for selector in ["metadata.name", "metadata.name>foo", "metadata.name===foo"] {
            assert!(FieldSelector::parse(selector).is_err(), "{selector}");
        }
    }

    #[test]
    fn parser_matches_upstream_escape_grammar_and_missing_fields_as_empty() {
        let object = json!({
            "metadata": {"name": "selected", "note": "a,b=c\\d"},
            "empty": ""
        });
        for selector in [
            r"metadata.note=a\,b\=c\\d",
            "missing=",
            "missing!=present",
            "empty=",
            "metadata.name=selected,",
            ",metadata.name=selected",
        ] {
            assert!(
                FieldSelector::parse(selector)
                    .unwrap_or_else(|error| panic!("{selector}: {error}"))
                    .matches_resource(&object),
                "{selector}",
            );
        }

        for selector in [
            r"metadata.name=bad\q",
            r"metadata.name=bad\",
            "metadata.name=a=b",
        ] {
            assert!(FieldSelector::parse(selector).is_err(), "{selector}");
        }
    }

    #[test]
    fn event_involved_object_alias_resolves_events_v1_regarding() {
        let event = json!({
            "apiVersion": "events.k8s.io/v1",
            "kind": "Event",
            "regarding": {"kind": "Pod", "name": "pod-a", "uid": "uid-a"}
        });
        for selector in [
            "involvedObject.kind=Pod",
            "involvedObject.name=pod-a",
            "involvedObject.uid=uid-a",
        ] {
            assert!(
                FieldSelector::parse(selector)
                    .unwrap()
                    .matches_resource(&event),
                "{selector}",
            );
        }
    }
}
