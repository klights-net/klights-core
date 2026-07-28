use super::frame::{FLAG_FIN, SPDY3_DICT};
use super::*;

#[test]
fn test_parse_nv_pairs_empty() {
    let spdy = SpdyExec::new();
    let data = 0u32.to_be_bytes().to_vec();
    let result = spdy.parse_nv_pairs(&data).unwrap();
    assert!(result.is_empty());
}

#[test]
fn test_parse_nv_pairs_single() {
    let spdy = SpdyExec::new();
    let mut data = Vec::new();
    data.extend_from_slice(&1u32.to_be_bytes()); // 1 pair
    data.extend_from_slice(&10u32.to_be_bytes()); // name len
    data.extend_from_slice(b"streamtype"); // name
    data.extend_from_slice(&6u32.to_be_bytes()); // value len
    data.extend_from_slice(b"stdout"); // value

    let result = spdy.parse_nv_pairs(&data).unwrap();
    assert_eq!(result.get("streamtype"), Some(&"stdout".to_string()));
}

#[test]
fn test_parse_nv_pairs_multiple() {
    let spdy = SpdyExec::new();
    let mut data = Vec::new();
    data.extend_from_slice(&2u32.to_be_bytes()); // 2 pairs
    // Pair 1
    data.extend_from_slice(&10u32.to_be_bytes());
    data.extend_from_slice(b"streamtype");
    data.extend_from_slice(&5u32.to_be_bytes());
    data.extend_from_slice(b"error");
    // Pair 2
    data.extend_from_slice(&4u32.to_be_bytes());
    data.extend_from_slice(b"port");
    data.extend_from_slice(&4u32.to_be_bytes());
    data.extend_from_slice(b"8080");

    let result = spdy.parse_nv_pairs(&data).unwrap();
    assert_eq!(result.get("streamtype"), Some(&"error".to_string()));
    assert_eq!(result.get("port"), Some(&"8080".to_string()));
}

#[test]
fn test_parse_nv_pairs_truncated_data() {
    let spdy = SpdyExec::new();
    // Claims 1 pair but data is too short
    let data = vec![0, 0, 0, 1, 0, 0, 0, 5];
    let result = spdy.parse_nv_pairs(&data).unwrap();
    assert!(result.is_empty()); // Should not panic
}

#[test]
fn test_stream_type_eq() {
    assert_eq!(StreamType::Stdout, StreamType::Stdout);
    assert_ne!(StreamType::Stdout, StreamType::Stderr);
}

#[test]
fn test_stream_id_for() {
    let mut spdy = SpdyExec::new();
    spdy.streams.insert(1, StreamType::Stdin);
    spdy.streams.insert(2, StreamType::Stdout);
    spdy.streams.insert(3, StreamType::Stderr);
    spdy.streams.insert(4, StreamType::Error);

    assert_eq!(spdy.stream_id_for(StreamType::Stdout), Some(2));
    assert_eq!(spdy.stream_id_for(StreamType::Error), Some(4));
    assert_eq!(spdy.stream_id_for(StreamType::Resize), None);
}

#[test]
fn test_data_frame_format() {
    // Verify data frame is correctly formatted
    let stream_id: u32 = 5;
    let data = b"hello";
    let fin = true;

    let mut frame = Vec::new();
    frame.extend_from_slice(&stream_id.to_be_bytes());
    frame[0] &= 0x7F; // Clear control bit
    let flags: u8 = if fin { FLAG_FIN } else { 0 };
    frame.push(flags);
    let len_bytes = (data.len() as u32).to_be_bytes();
    frame.extend_from_slice(&len_bytes[1..4]);
    frame.extend_from_slice(data);

    // Verify header
    assert_eq!(frame[0] & 0x80, 0, "Control bit must be 0 for data frame");
    assert_eq!(
        u32::from_be_bytes([frame[0] & 0x7F, frame[1], frame[2], frame[3]]),
        5
    );
    assert_eq!(frame[4] & FLAG_FIN, FLAG_FIN, "FIN flag must be set");
    let len = ((frame[5] as u32) << 16) | ((frame[6] as u32) << 8) | (frame[7] as u32);
    assert_eq!(len, 5);
    assert_eq!(&frame[8..], b"hello");
}

#[test]
fn test_compress_headers_roundtrip() {
    let mut spdy = SpdyExec::new();
    let headers = vec![("streamtype", "stdout"), ("port", "8080")];
    let compressed = spdy.compress_headers(&headers).unwrap();
    assert!(!compressed.is_empty());
    // Decompress and verify
    let decompressed = spdy.decompress_headers(&compressed).unwrap();
    assert_eq!(decompressed.get("streamtype"), Some(&"stdout".to_string()));
    assert_eq!(decompressed.get("port"), Some(&"8080".to_string()));
}

#[test]
fn test_decompress_headers_with_spdy_dictionary() {
    // Simulate Go's compress/zlib with SPDY dictionary (what K8s clients send)
    // Compress NV headers using zlib WITH the SPDY dictionary
    let mut nv = Vec::new();
    let num_pairs = 1u32;
    nv.extend_from_slice(&num_pairs.to_be_bytes());
    nv.extend_from_slice(&10u32.to_be_bytes()); // "streamtype" len
    nv.extend_from_slice(b"streamtype");
    nv.extend_from_slice(&6u32.to_be_bytes()); // "stdout" len
    nv.extend_from_slice(b"stdout");

    // Compress with SPDY dictionary (like Go client does)
    let mut compressor = flate2::Compress::new(flate2::Compression::default(), true);
    compressor.set_dictionary(SPDY3_DICT).unwrap();

    let mut compressed = vec![0u8; 1024];
    compressor
        .compress(&nv, &mut compressed, flate2::FlushCompress::Sync)
        .unwrap();
    let compressed_len = compressor.total_out() as usize;
    let compressed = &compressed[..compressed_len];

    // Now decompress — this should trigger the "needs dictionary" flow
    let mut spdy = SpdyExec::new();
    let result = spdy.decompress_headers(compressed).unwrap();
    assert_eq!(
        result.get("streamtype"),
        Some(&"stdout".to_string()),
        "Must decompress SPDY dictionary-compressed headers"
    );
}

#[test]
fn test_compress_headers_multiple_calls_succeed() {
    // Bug: compress_headers called set_dictionary on every invocation
    // This caused "deflate compression error" on the second call
    let mut spdy = SpdyExec::new();

    // First call should work
    let result1 = spdy.compress_headers(&[(":status", "200")]);
    assert!(result1.is_ok(), "First compress_headers should succeed");

    // Second call should also work (this was failing before the fix)
    let result2 = spdy.compress_headers(&[(":version", "HTTP/1.1")]);
    assert!(
        result2.is_ok(),
        "Second compress_headers should succeed: {:?}",
        result2.err()
    );

    // Third call for good measure
    let result3 = spdy.compress_headers(&[("content-type", "text/plain")]);
    assert!(result3.is_ok(), "Third compress_headers should succeed");
}

#[tokio::test]
async fn test_spdy_negotiate_multiple_syn_replies() {
    // Simulate multiple SYN_REPLY frames (stream negotiation)
    let mut spdy = SpdyExec::new();
    let mut buffer = Vec::new();

    // First SYN_REPLY (stdout stream)
    let result1 = spdy.write_syn_reply(&mut buffer, 1).await;
    assert!(result1.is_ok(), "First SYN_REPLY should succeed");

    // Second SYN_REPLY (stderr stream) — this was failing before the fix
    let result2 = spdy.write_syn_reply(&mut buffer, 3).await;
    assert!(
        result2.is_ok(),
        "Second SYN_REPLY should succeed: {:?}",
        result2.err()
    );

    // Third SYN_REPLY (error stream)
    let result3 = spdy.write_syn_reply(&mut buffer, 5).await;
    assert!(result3.is_ok(), "Third SYN_REPLY should succeed");

    // Verify buffer contains data from all three SYN_REPLY frames
    assert!(!buffer.is_empty(), "Buffer should contain SYN_REPLY frames");
}

#[tokio::test]
async fn test_spdy_client_syn_stream() {
    // Test SPDY client mode: creating SYN_STREAM frames
    let mut spdy = SpdyExec::new();
    let mut buffer = Vec::new();

    // Client creates stdin stream
    let result1 = spdy
        .write_syn_stream(&mut buffer, 1, StreamType::Stdin)
        .await;
    assert!(result1.is_ok(), "stdin SYN_STREAM should succeed");

    // Client creates stdout stream
    let result2 = spdy
        .write_syn_stream(&mut buffer, 3, StreamType::Stdout)
        .await;
    assert!(result2.is_ok(), "stdout SYN_STREAM should succeed");

    // Client creates stderr stream
    let result3 = spdy
        .write_syn_stream(&mut buffer, 5, StreamType::Stderr)
        .await;
    assert!(result3.is_ok(), "stderr SYN_STREAM should succeed");

    // Client creates error stream
    let result4 = spdy
        .write_syn_stream(&mut buffer, 7, StreamType::Error)
        .await;
    assert!(result4.is_ok(), "error SYN_STREAM should succeed");

    // Verify buffer contains data from all four SYN_STREAM frames
    assert!(
        !buffer.is_empty(),
        "Buffer should contain SYN_STREAM frames"
    );

    // Each SYN_STREAM frame should have:
    // - Control frame header (8 bytes)
    // - Stream ID (4 bytes)
    // - Associated stream ID (4 bytes)
    // - Priority + slot (2 bytes)
    // - Compressed headers (variable)
    // Minimum size per frame: 18 bytes
    assert!(
        buffer.len() >= 18 * 4,
        "Buffer should contain at least 4 frames of minimum 18 bytes each"
    );
}
