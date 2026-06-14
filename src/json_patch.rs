use anyhow::Result;
use serde_json::Value;

/// Apply an RFC 7396 JSON Merge Patch (``merge_patch`` semantics).
///
/// This is a shared implementation used by both API patch handling and database
/// merge-patch writes so the semantics stay aligned.
pub fn apply_merge_patch(target: &mut Value, patch: &Value) -> Result<()> {
    // Delegate to the json-patch crate's merge implementation for RFC 7396
    // behavior compatibility (null deletes keys, objects merge recursively, etc.).
    ::json_patch::merge(target, patch);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::apply_merge_patch;
    use serde_json::json;

    #[test]
    fn test_apply_merge_patch_adds_field() {
        let mut doc = json!({"metadata": {"name": "node-a"}});
        let patch = json!({"metadata": {"annotations": {"a": "b"}}});

        apply_merge_patch(&mut doc, &patch).expect("merge patch should succeed");

        assert_eq!(
            doc.pointer("/metadata/annotations/a")
                .and_then(|v| v.as_str()),
            Some("b")
        );
    }

    #[test]
    fn test_apply_merge_patch_null_removes_key() {
        let mut doc = json!({"metadata": {"annotations": {"a": "b", "c": "d"}}});
        let patch = json!({"metadata": {"annotations": {"a": null}}});

        apply_merge_patch(&mut doc, &patch).expect("merge patch should succeed");

        assert!(
            !doc.pointer("/metadata/annotations")
                .and_then(|a| a.as_object())
                .is_none_or(|o| o.contains_key("a"))
        );
        assert_eq!(
            doc.pointer("/metadata/annotations/c")
                .and_then(|v| v.as_str()),
            Some("d")
        );
    }

    #[test]
    fn test_apply_merge_patch_replaces_non_objects() {
        let mut doc = json!({"status": {"phase": "Running"}});
        let patch = json!("terminated");

        apply_merge_patch(&mut doc, &patch).expect("merge patch should succeed");

        assert_eq!(doc, patch);
    }
}
