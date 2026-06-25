//! T2 (latency-todo): zstd compression at the raft snapshot codec boundary.
//! Snapshot payloads are JSON `RaftSnapshotData` (resource-dense) and are
//! chunked over the wire via InstallSnapshot; fewer bytes ⇒ fewer HTTP/2
//! frames ⇒ proportionally less loss amplification on a lossy link.
//!
//! Framing is a 1-byte version tag so a future change stays decodable and an
//! uncompressed payload (tag `RAW`) round-trips too — this also lets the
//! codec be toggled or bypassed without a storage-format migration.
//!
//! Compression runs on this synchronous codec boundary (called from
//! `build_snapshot`/`apply_snapshot`), never on the tokio event loop. Snapshots
//! are rare relative to commits, so the CPU cost is bounded and idle-silent
//! when no snapshot flows.

use anyhow::Result;

/// Framing tag: the byte preceding the payload identifies its encoding.
/// `0x00` = raw (uncompressed), `0x5a` = zstd-compressed.
const TAG_RAW: u8 = 0x00;
const TAG_ZSTD: u8 = 0x5a;

/// Compress `raw` bytes with zstd (level 3), returning `TAG_ZSTD || data`.
/// Returns `TAG_RAW || raw` when compression does not shrink the input (small
/// or already-incompressible payloads), so we never pay framing overhead for a
/// net loss.
pub fn encode(raw: &[u8]) -> Result<Vec<u8>> {
    let compressed = zstd::encode_all(raw, 3)?;
    if compressed.len() + 1 < raw.len() + 1 {
        let mut out = Vec::with_capacity(1 + compressed.len());
        out.push(TAG_ZSTD);
        out.extend_from_slice(&compressed);
        Ok(out)
    } else {
        let mut out = Vec::with_capacity(1 + raw.len());
        out.push(TAG_RAW);
        out.extend_from_slice(raw);
        Ok(out)
    }
}

/// Decode a framed payload produced by [`encode`]. The leading tag selects
/// zstd-decompression or a raw passthrough. An unrecognized tag is an error
/// rather than a silent misinterpretation.
pub fn decode(framed: &[u8]) -> Result<Vec<u8>> {
    let Some((&tag, rest)) = framed.split_first() else {
        anyhow::bail!("compressed payload missing framing tag");
    };
    match tag {
        TAG_RAW => Ok(rest.to_vec()),
        TAG_ZSTD => Ok(zstd::decode_all(rest)?),
        other => anyhow::bail!("unknown compression framing tag: {other:#x}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_a_resource_dense_payload() {
        // A JSON-ish snapshot body: lots of repetition ⇒ highly compressible.
        let raw = b"{\"commits\":[{\"key\":\"v1/Pod/default/web/".repeat(200);
        let encoded = encode(&raw).expect("encode");
        assert_eq!(encoded[0], TAG_ZSTD, "repetitive JSON must compress");
        assert!(
            encoded.len() < raw.len() / 2,
            "zstd must shrink repetitive JSON substantially ({} -> {})",
            raw.len(),
            encoded.len()
        );
        let decoded = decode(&encoded).expect("decode");
        assert_eq!(decoded, raw, "round-trip must be lossless");
    }

    #[test]
    fn small_payload_falls_back_to_raw_framing() {
        // A tiny incompressible payload must not pay zstd framing overhead.
        let raw = b"abc";
        let encoded = encode(raw).expect("encode");
        assert_eq!(encoded[0], TAG_RAW, "tiny payload must stay raw-framed");
        assert_eq!(decode(&encoded).expect("decode"), raw);
    }

    #[test]
    fn incompressible_bytes_round_trip_via_raw_fallback() {
        // Random-ish bytes that zstd cannot shrink must still round-trip via
        // the raw-fallback path (encode never makes the payload bigger +1).
        let raw: Vec<u8> = (0..256u32)
            .map(|i| (i ^ (i.rotate_left(7))) as u8)
            .collect();
        let encoded = encode(&raw).expect("encode");
        assert_eq!(decode(&encoded).expect("decode"), raw);
    }

    #[test]
    fn rejects_unknown_framing_tag() {
        let bad = [0x01, 0x02, 0x03];
        assert!(decode(&bad).is_err());
    }
}
