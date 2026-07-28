//! Deterministic Kubernetes timestamp representation.

/// Format a `metav1.Time` value with second precision and a `Z` suffix.
pub fn format_time(dt: chrono::DateTime<chrono::Utc>) -> String {
    dt.format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

/// Format a `metav1.MicroTime` value with exactly six fractional digits.
pub fn format_microtime(dt: chrono::DateTime<chrono::Utc>) -> String {
    dt.format("%Y-%m-%dT%H:%M:%S.%6fZ").to_string()
}

/// Format the historical nanosecond-precision timestamp shape.
pub fn format_legacy_timestamp(dt: chrono::DateTime<chrono::Utc>) -> String {
    dt.format("%Y-%m-%dT%H:%M:%S.%fZ").to_string()
}

/// Canonicalize `events.k8s.io/v1` `metav1.MicroTime` fields in-place.
pub fn normalize_event_microtime_fields(value: &mut serde_json::Value) {
    fn normalize_path(value: &mut serde_json::Value, path: &[&str]) {
        if path.is_empty() {
            return;
        }
        if path.len() == 1 {
            if let Some(raw) = value.get(path[0]).and_then(|value| value.as_str())
                && let Ok(parsed) = chrono::DateTime::parse_from_rfc3339(raw)
            {
                let canonical = format_microtime(parsed.with_timezone(&chrono::Utc));
                if let Some(object) = value.as_object_mut() {
                    object.insert(path[0].to_string(), serde_json::Value::String(canonical));
                }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn time(raw: &str) -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::parse_from_rfc3339(raw)
            .unwrap()
            .with_timezone(&chrono::Utc)
    }

    #[test]
    fn time_precision_is_canonical() {
        assert_eq!(
            format_time(time("2026-04-23T12:34:56.789012345+00:00")),
            "2026-04-23T12:34:56Z"
        );
        assert_eq!(
            format_microtime(time("2026-04-23T12:34:56.123456789+00:00")),
            "2026-04-23T12:34:56.123456Z"
        );
        assert_eq!(
            format_legacy_timestamp(time("2026-04-23T12:34:56.123456789+00:00")),
            "2026-04-23T12:34:56.123456789Z"
        );
    }

    #[test]
    fn event_microtimes_accept_rfc3339_variants() {
        let mut event = serde_json::json!({
            "eventTime": "2017-09-19T13:49:16+00:00",
            "series": {"lastObservedTime": "2017-09-19T13:49:16.123456789+00:00"}
        });
        normalize_event_microtime_fields(&mut event);
        assert_eq!(event["eventTime"], "2017-09-19T13:49:16.000000Z");
        assert_eq!(
            event["series"]["lastObservedTime"],
            "2017-09-19T13:49:16.123456Z"
        );
    }
}
