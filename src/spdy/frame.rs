//! Internal SPDY/3.1 client for communicating with containerd's CRI streaming server.
//!
//! External SPDY (kubectl-facing) has been removed. This module provides SPDY client
//! functionality used internally to bridge WebSocket requests to containerd.

use std::collections::{HashMap, VecDeque};

/// SPDY/3.1 header compression dictionary (full 1423-byte version)
/// From the SPDY/3 spec: https://www.chromium.org/spdy/spdy-protocol/spdy-protocol-draft3-1/
pub const SPDY3_DICT: &[u8] = b"\x00\x00\x00\x07options\x00\x00\x00\x04head\x00\x00\x00\x04post\x00\x00\x00\x03put\x00\x00\x00\x06delete\x00\x00\x00\x05trace\x00\x00\x00\x06accept\x00\x00\x00\x0eaccept-charset\x00\x00\x00\x0faccept-encoding\x00\x00\x00\x0faccept-language\x00\x00\x00\x0daccept-ranges\x00\x00\x00\x03age\x00\x00\x00\x05allow\x00\x00\x00\x0dauthorization\x00\x00\x00\rcache-control\x00\x00\x00\nconnection\x00\x00\x00\x0ccontent-base\x00\x00\x00\x10content-encoding\x00\x00\x00\x10content-language\x00\x00\x00\x0econtent-length\x00\x00\x00\x10content-location\x00\x00\x00\x0bcontent-md5\x00\x00\x00\rcontent-range\x00\x00\x00\x0ccontent-type\x00\x00\x00\x04date\x00\x00\x00\x04etag\x00\x00\x00\x06expect\x00\x00\x00\x07expires\x00\x00\x00\x04from\x00\x00\x00\x04host\x00\x00\x00\x08if-match\x00\x00\x00\x11if-modified-since\x00\x00\x00\rif-none-match\x00\x00\x00\x08if-range\x00\x00\x00\x13if-unmodified-since\x00\x00\x00\rlast-modified\x00\x00\x00\x08location\x00\x00\x00\x0cmax-forwards\x00\x00\x00\x06pragma\x00\x00\x00\x12proxy-authenticate\x00\x00\x00\x13proxy-authorization\x00\x00\x00\x05range\x00\x00\x00\x07referer\x00\x00\x00\x0bretry-after\x00\x00\x00\x06server\x00\x00\x00\x02te\x00\x00\x00\x07trailer\x00\x00\x00\x11transfer-encoding\x00\x00\x00\x07upgrade\x00\x00\x00\nuser-agent\x00\x00\x00\x04vary\x00\x00\x00\x03via\x00\x00\x00\x07warning\x00\x00\x00\x10www-authenticate\x00\x00\x00\x06method\x00\x00\x00\x03get\x00\x00\x00\x06status\x00\x00\x00\x06200 OK\x00\x00\x00\x07version\x00\x00\x00\x08HTTP/1.1\x00\x00\x00\x03url\x00\x00\x00\x06public\x00\x00\x00\nset-cookie\x00\x00\x00\nkeep-alive\x00\x00\x00\x06origin100101201202205206300302303304305306307402405406407408409410411412413414415416417502504505203 Non-Authoritative Information204 No Content301 Moved Permanently400 Bad Request401 Unauthorized403 Forbidden404 Not Found500 Internal Server Error501 Not Implemented503 Service UnavailableJan Feb Mar Apr May Jun Jul Aug Sept Oct Nov Dec 00:00:00 Mon, Tue, Wed, Thu, Fri, Sat, Sun, GMTchunked,text/html,image/png,image/jpg,image/gif,application/xml,application/xhtml+xml,text/plain,text/javascript,publicprivatemax-age=gzip,deflate,sdchcharset=utf-8charset=iso-8859-1,utf-,*,enq=0.";

// SPDY frame types
pub const SYN_STREAM: u16 = 1;
pub const SYN_REPLY: u16 = 2;
pub const RST_STREAM: u16 = 3;
pub const SETTINGS: u16 = 4;
pub const PING: u16 = 6;
pub const GOAWAY: u16 = 7;
pub const WINDOW_UPDATE: u16 = 9;

// SPDY flags
pub const FLAG_FIN: u8 = 0x01;

// SPDY version
pub const SPDY_VERSION: u16 = 3;

/// Stream type as identified by K8s remotecommand headers
#[derive(Debug, Clone, PartialEq)]
pub enum StreamType {
    Stdin,
    Stdout,
    Stderr,
    Error,
    Resize,
    /// Port-forward data stream (bidirectional)
    Data,
}

/// Parsed SPDY frame
#[derive(Debug)]
pub enum SpdyFrame {
    SynStream {
        stream_id: u32,
        headers: HashMap<String, String>,
    },
    SynReply {
        stream_id: u32,
    },
    Data {
        stream_id: u32,
        data: Vec<u8>,
        fin: bool,
    },
    Ping {
        id: u32,
    },
    RstStream {
        stream_id: u32,
    },
    Settings,
    GoAway,
    WindowUpdate {
        stream_id: u32,
    },
    Unknown,
}

/// SPDY connection handler for K8s exec
pub struct SpdyExec {
    pub streams: HashMap<u32, StreamType>,
    /// Frames read while negotiating streams that must be processed by the caller.
    pub pending_frames: VecDeque<SpdyFrame>,
    /// Zlib decompressor for headers (must persist across frames)
    pub decompressor: flate2::Decompress,
    /// Zlib compressor for headers (must persist across frames)
    pub compressor: flate2::Compress,
}

impl SpdyExec {
    pub fn new() -> Self {
        let decompressor = flate2::Decompress::new(true);
        let mut compressor = flate2::Compress::new(flate2::Compression::default(), true);
        compressor
            .set_dictionary(SPDY3_DICT)
            .expect("Failed to set SPDY dictionary");

        Self {
            streams: HashMap::new(),
            pending_frames: VecDeque::new(),
            decompressor,
            compressor,
        }
    }
}

impl Default for SpdyExec {
    fn default() -> Self {
        Self::new()
    }
}
