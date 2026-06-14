use crate::protobuf::*;

/// F3-03: pre-size the encode buffer using `prost::Message::encoded_len()` so
/// every protobuf-encoded resource avoids 4-6 reallocations as `Vec` doubles
/// from its default capacity. `encoded_len()` walks the message once; the
/// allocator then receives an exact-fit hint and `encode()` writes in place.
pub fn encode_message_to_vec<M>(msg: &M) -> anyhow::Result<Vec<u8>>
where
    M: prost::Message,
{
    let mut buf = Vec::with_capacity(msg.encoded_len());
    msg.encode(&mut buf)?;
    Ok(buf)
}

/// Encode JSON resource to protobuf bytes for a specific kind.
/// Converts JSON → k8s-openapi type → k8s-pb type → protobuf bytes.
/// Returns protobuf-encoded bytes (NOT wrapped in Unknown envelope).
pub fn encode_protobuf_resource(kind: &str, value: &Value) -> anyhow::Result<Vec<u8>> {
    let api_version = value
        .get("apiVersion")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let registry = global_oo_registry();
    if !registry.handles(api_version, kind) {
        anyhow::bail!("Unknown kind for protobuf encoding: {api_version}/{kind}");
    }
    registry.encode(api_version, kind, value)
}

pub fn normalize_event_microtime_fields(value: &mut Value) {
    crate::utils::normalize_event_microtime_fields(value);
}

/// Encode JSON Value to K8s protobuf format.
/// Encodes only when a concrete protobuf codec exists for the resource kind.
///
/// All resource types (including lists) are wrapped in an Unknown envelope with the "k8s\0" magic
/// prefix. This is the K8s protobuf wire format expected by the Go client for all response types.
pub fn encode_protobuf(value: &Value) -> anyhow::Result<Vec<u8>> {
    use prost::Message;

    // Extract apiVersion and kind from JSON
    let api_version = value
        .get("apiVersion")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("Missing apiVersion in JSON"))?
        .to_string();

    let kind = value
        .get("kind")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("Missing kind in JSON"))?
        .to_string();

    // Encode to a concrete protobuf type. If unsupported, return an error so the
    // HTTP layer can negotiate a JSON fallback response instead of emitting
    // JSON bytes in a protobuf envelope (which breaks typed Go decoding).
    // `Status` (error responses) is encoded from meta/v1 directly since it has
    // no entry in the resource codec registry.
    let protobuf_bytes = if kind == "Status" {
        encode_status_protobuf(value)
    } else {
        encode_protobuf_resource(&kind, value)
    }?;

    // All resources (single and list) use Unknown envelope with k8s\0 magic prefix.
    // The K8s Go client requires this format for ALL protobuf responses — it checks for
    // the [107 56 115 0] ("k8s\0") prefix and rejects bare protobuf with
    // "provided data does not appear to be a protobuf message".
    let unknown = Unknown {
        type_meta: Some(TypeMeta {
            api_version: api_version.clone(),
            kind: kind.clone(),
        }),
        raw: protobuf_bytes,
        content_encoding: String::new(),
        content_type: String::new(),
    };

    // K8s protobuf wire format: 4-byte magic prefix + Unknown envelope.
    // F3-03: pre-size the buffer to (magic + encoded_len) so prost
    // appends in place without growing the Vec. Helper isn't usable
    // here because the prefix bytes must lead the buffer.
    let body_len = unknown.encoded_len();
    let mut buf = Vec::with_capacity(4 + body_len);
    buf.extend_from_slice(&[0x6b, 0x38, 0x73, 0x00]); // "k8s\0"
    unknown.encode(&mut buf)?;
    Ok(buf)
}

/// Encode a `metav1.Status` JSON Value into its protobuf wire bytes (the inner
/// `raw` of the Unknown envelope). Used for error responses negotiated to
/// `application/vnd.kubernetes.protobuf`.
fn encode_status_protobuf(value: &Value) -> anyhow::Result<Vec<u8>> {
    use k8s_pb::apimachinery::pkg::apis::meta::v1 as metav1;
    use prost::Message;

    let str_field = |key: &str| {
        value
            .get(key)
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(str::to_string)
    };

    let metadata = value.get("metadata").and_then(|m| m.as_object());
    let list_meta = metadata.map(|m| metav1::ListMeta {
        self_link: None,
        resource_version: m
            .get("resourceVersion")
            .and_then(|v| v.as_str())
            .map(str::to_string),
        r#continue: m
            .get("continue")
            .and_then(|v| v.as_str())
            .map(str::to_string),
        remaining_item_count: None,
    });

    let details = value.get("details").and_then(|d| d.as_object()).map(|d| {
        let causes = d
            .get("causes")
            .and_then(|c| c.as_array())
            .map(|arr| {
                arr.iter()
                    .map(|c| metav1::StatusCause {
                        reason: c.get("reason").and_then(|v| v.as_str()).map(str::to_string),
                        message: c
                            .get("message")
                            .and_then(|v| v.as_str())
                            .map(str::to_string),
                        field: c.get("field").and_then(|v| v.as_str()).map(str::to_string),
                    })
                    .collect()
            })
            .unwrap_or_default();
        metav1::StatusDetails {
            name: d.get("name").and_then(|v| v.as_str()).map(str::to_string),
            group: d.get("group").and_then(|v| v.as_str()).map(str::to_string),
            kind: d.get("kind").and_then(|v| v.as_str()).map(str::to_string),
            uid: d.get("uid").and_then(|v| v.as_str()).map(str::to_string),
            causes,
            retry_after_seconds: d
                .get("retryAfterSeconds")
                .and_then(|v| v.as_i64())
                .map(|v| v as i32),
        }
    });

    let status = metav1::Status {
        metadata: list_meta,
        status: str_field("status"),
        message: str_field("message"),
        reason: str_field("reason"),
        details,
        code: value.get("code").and_then(|v| v.as_i64()).map(|c| c as i32),
    };
    Ok(status.encode_to_vec())
}
