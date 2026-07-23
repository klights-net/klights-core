use chrono::{DateTime, Utc};
use serde_json::Value;

pub(crate) fn k8s_microtime_format(dt: DateTime<Utc>) -> String {
    dt.format("%Y-%m-%dT%H:%M:%S.%6fZ").to_string()
}

pub(crate) fn normalize_event_microtime_fields(value: &mut Value) {
    fn normalize_path(value: &mut Value, path: &[&str]) {
        if path.is_empty() {
            return;
        }
        if path.len() == 1 {
            if let Some(raw) = value.get(path[0]).and_then(Value::as_str)
                && let Ok(parsed) = DateTime::parse_from_rfc3339(raw)
                && let Some(object) = value.as_object_mut()
            {
                object.insert(
                    path[0].to_string(),
                    Value::String(k8s_microtime_format(parsed.with_timezone(&Utc))),
                );
            }
            return;
        }
        if let Some(next) = value.get_mut(path[0]) {
            normalize_path(next, &path[1..]);
        }
    }

    normalize_path(value, &["eventTime"]);
    normalize_path(value, &["series", "lastObservedTime"]);
}
