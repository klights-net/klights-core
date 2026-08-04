use super::frame::{FLAG_FIN, SPDY3_DICT};
use super::*;

// Copied independently from moby/spdystream v0.2.0 spdy/dictionary.go.
// Do not derive this interoperability oracle from the production constant.
const MOBY_V0_2_0_SPDY3_DICT: &[u8] = b"\x00\x00\x00\x07options\x00\x00\x00\x04head\x00\x00\x00\x04post\x00\x00\x00\x03put\x00\x00\x00\x06delete\x00\x00\x00\x05trace\x00\x00\x00\x06accept\x00\x00\x00\x0eaccept-charset\x00\x00\x00\x0faccept-encoding\x00\x00\x00\x0faccept-language\x00\x00\x00\x0daccept-ranges\x00\x00\x00\x03age\x00\x00\x00\x05allow\x00\x00\x00\x0dauthorization\x00\x00\x00\rcache-control\x00\x00\x00\nconnection\x00\x00\x00\x0ccontent-base\x00\x00\x00\x10content-encoding\x00\x00\x00\x10content-language\x00\x00\x00\x0econtent-length\x00\x00\x00\x10content-location\x00\x00\x00\x0bcontent-md5\x00\x00\x00\rcontent-range\x00\x00\x00\x0ccontent-type\x00\x00\x00\x04date\x00\x00\x00\x04etag\x00\x00\x00\x06expect\x00\x00\x00\x07expires\x00\x00\x00\x04from\x00\x00\x00\x04host\x00\x00\x00\x08if-match\x00\x00\x00\x11if-modified-since\x00\x00\x00\rif-none-match\x00\x00\x00\x08if-range\x00\x00\x00\x13if-unmodified-since\x00\x00\x00\rlast-modified\x00\x00\x00\x08location\x00\x00\x00\x0cmax-forwards\x00\x00\x00\x06pragma\x00\x00\x00\x12proxy-authenticate\x00\x00\x00\x13proxy-authorization\x00\x00\x00\x05range\x00\x00\x00\x07referer\x00\x00\x00\x0bretry-after\x00\x00\x00\x06server\x00\x00\x00\x02te\x00\x00\x00\x07trailer\x00\x00\x00\x11transfer-encoding\x00\x00\x00\x07upgrade\x00\x00\x00\nuser-agent\x00\x00\x00\x04vary\x00\x00\x00\x03via\x00\x00\x00\x07warning\x00\x00\x00\x10www-authenticate\x00\x00\x00\x06method\x00\x00\x00\x03get\x00\x00\x00\x06status\x00\x00\x00\x06200 OK\x00\x00\x00\x07version\x00\x00\x00\x08HTTP/1.1\x00\x00\x00\x03url\x00\x00\x00\x06public\x00\x00\x00\nset-cookie\x00\x00\x00\nkeep-alive\x00\x00\x00\x06origin100101201202205206300302303304305306307402405406407408409410411412413414415416417502504505203 Non-Authoritative Information204 No Content301 Moved Permanently400 Bad Request401 Unauthorized403 Forbidden404 Not Found500 Internal Server Error501 Not Implemented503 Service UnavailableJan Feb Mar Apr May Jun Jul Aug Sept Oct Nov Dec 00:00:00 Mon, Tue, Wed, Thu, Fri, Sat, Sun, GMTchunked,text/html,image/png,image/jpg,image/gif,application/xml,application/xhtml+xml,text/plain,text/javascript,publicprivatemax-age=gzip,deflate,sdchcharset=utf-8charset=iso-8859-1,utf-,*,enq=0.";
const MOBY_V0_2_0_DICTIONARY_ADLER32: u32 = 0xe3c6_a7c2;
const MOBY_SONOBUOY_STDOUT_SYN_STREAM: &[u8] = b"\x80\x03\x00\x01\x00\x00\x00\x2b\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x78\xf9\xe3\xc6\xa7\xc2\x62\x60\x60\x60\x04\xa7\xb3\x92\xa2\xd4\xc4\x5c\x68\x51\xc2\x56\x5c\x92\x92\x5f\x5a\x02\x00\x00\x00\xff\xff";
const SONOBUOY_STDOUT_NV_BLOCK: &[u8] =
    b"\x00\x00\x00\x01\x00\x00\x00\x0astreamtype\x00\x00\x00\x06stdout";

fn adler32(bytes: &[u8]) -> u32 {
    const MOD_ADLER: u32 = 65_521;
    let mut a = 1_u32;
    let mut b = 0_u32;
    for byte in bytes {
        a = (a + u32::from(*byte)) % MOD_ADLER;
        b = (b + a) % MOD_ADLER;
    }
    (b << 16) | a
}

fn decode_with_moby_v0_2_0_dictionary(compressed: &[u8]) -> Vec<u8> {
    let mut decoder = flate2::Decompress::new(true);
    let mut output = [0_u8; 4096];
    let error = decoder
        .decompress(compressed, &mut output, flate2::FlushDecompress::Sync)
        .expect_err("moby SPDY header block must request its preset dictionary");
    assert_eq!(
        error.needs_dictionary(),
        Some(MOBY_V0_2_0_DICTIONARY_ADLER32)
    );
    assert_eq!(
        decoder
            .set_dictionary(MOBY_V0_2_0_SPDY3_DICT)
            .expect("canonical moby SPDY dictionary"),
        MOBY_V0_2_0_DICTIONARY_ADLER32
    );
    let input_position = decoder.total_in() as usize;
    let output_position = decoder.total_out() as usize;
    decoder
        .decompress(
            &compressed[input_position..],
            &mut output[output_position..],
            flate2::FlushDecompress::Sync,
        )
        .expect("canonical moby SPDY header block must decompress");
    output[..decoder.total_out() as usize].to_vec()
}

#[test]
fn containerd_spdy_dictionary_matches_moby_v0_2_0_contract() {
    assert_eq!(SPDY3_DICT.len(), 1423);
    assert_eq!(adler32(SPDY3_DICT), MOBY_V0_2_0_DICTIONARY_ADLER32);
    assert!(SPDY3_DICT.ends_with(b"charset=iso-8859-1,utf-,*,enq=0."));
    assert_eq!(SPDY3_DICT, MOBY_V0_2_0_SPDY3_DICT);
}

#[tokio::test]
async fn containerd_boundary_decodes_canonical_moby_syn_stream_for_sonobuoy_stdout_only() {
    // Sonobuoy retrieve uses stdout=true with stdin/stderr/tty disabled.
    let mut spdy = SpdyExec::new();
    let mut wire = MOBY_SONOBUOY_STDOUT_SYN_STREAM;
    let frame = spdy.read_frame(&mut wire).await.unwrap();
    match frame {
        SpdyFrame::SynStream { stream_id, headers } => {
            assert_eq!(stream_id, 1);
            assert_eq!(
                headers.get("streamtype").map(String::as_str),
                Some("stdout")
            );
        }
        other => panic!("expected moby SYN_STREAM, got {other:?}"),
    }
}

#[tokio::test]
async fn containerd_emitted_sonobuoy_stdout_headers_decode_with_canonical_moby_dictionary() {
    let mut spdy = SpdyExec::new();
    let mut wire = Vec::new();
    spdy.write_syn_stream(&mut wire, 1, StreamType::Stdout)
        .await
        .unwrap();

    assert_eq!(&wire[..4], &[0x80, 0x03, 0x00, 0x01]);
    let payload_len = u32::from_be_bytes([0, wire[5], wire[6], wire[7]]) as usize;
    assert_eq!(wire.len(), 8 + payload_len);
    assert_eq!(&wire[8..12], &1_u32.to_be_bytes());
    assert_eq!(
        decode_with_moby_v0_2_0_dictionary(&wire[18..]),
        SONOBUOY_STDOUT_NV_BLOCK
    );
}

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
