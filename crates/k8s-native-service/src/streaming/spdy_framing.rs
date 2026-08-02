//! Native-service-private Kubernetes SPDY/3.1 framing.
//!
//! This is the kubectl-facing server codec. Containerd's client-side streaming
//! codec is separately private to `kubelet::containerd_streaming`; keeping the
//! two adapters independent avoids either feature owning the other. Only the
//! wire primitives needed by this server adapter are implemented here.

use std::collections::{HashMap, VecDeque};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const SPDY3_DICT: &[u8] = b"\x00\x00\x00\x07options\x00\x00\x00\x04head\x00\x00\x00\x04post\x00\x00\x00\x03put\x00\x00\x00\x06delete\x00\x00\x00\x05trace\x00\x00\x00\x06accept\x00\x00\x00\x0eaccept-charset\x00\x00\x00\x0faccept-encoding\x00\x00\x00\x0faccept-language\x00\x00\x00\raccept-ranges\x00\x00\x00\x03age\x00\x00\x00\x05allow\x00\x00\x00\rauthorization\x00\x00\x00\rcache-control\x00\x00\x00\nconnection\x00\x00\x00\x0ccontent-base\x00\x00\x00\x10content-encoding\x00\x00\x00\x10content-language\x00\x00\x00\x0econtent-length\x00\x00\x00\x10content-location\x00\x00\x00\x0bcontent-md5\x00\x00\x00\rcontent-range\x00\x00\x00\x0ccontent-type\x00\x00\x00\x04date\x00\x00\x00\x04etag\x00\x00\x00\x06expect\x00\x00\x00\x07expires\x00\x00\x00\x04from\x00\x00\x00\x04host\x00\x00\x00\x08if-match\x00\x00\x00\x11if-modified-since\x00\x00\x00\rif-none-match\x00\x00\x00\x08if-range\x00\x00\x00\x13if-unmodified-since\x00\x00\x00\rlast-modified\x00\x00\x00\x08location\x00\x00\x00\x0cmax-forwards\x00\x00\x00\x06pragma\x00\x00\x00\x12proxy-authenticate\x00\x00\x00\x13proxy-authorization\x00\x00\x00\x05range\x00\x00\x00\x07referer\x00\x00\x00\x0bretry-after\x00\x00\x00\x06server\x00\x00\x00\x02te\x00\x00\x00\x07trailer\x00\x00\x00\x11transfer-encoding\x00\x00\x00\x07upgrade\x00\x00\x00\nuser-agent\x00\x00\x00\x04vary\x00\x00\x00\x03via\x00\x00\x00\x07warning\x00\x00\x00\x10www-authenticate\x00\x00\x00\x06method\x00\x00\x00\x03get\x00\x00\x00\x06status\x00\x00\x00\x06200 OK\x00\x00\x00\x07version\x00\x00\x00\x08HTTP/1.1\x00\x00\x00\x03url\x00\x00\x00\x06public\x00\x00\x00\nset-cookie\x00\x00\x00\nkeep-alive\x00\x00\x00\x06origin100101201202205206300302303304305306307402405406407408409410411412413414415416417502504505203 Non-Authoritative Information204 No Content301 Moved Permanently400 Bad Request401 Unauthorized403 Forbidden404 Not Found500 Internal Server Error501 Not Implemented503 Service UnavailableJan Feb Mar Apr May Jun Jul Aug Sept Oct Nov Dec 00:00:00 Mon, Tue, Wed, Thu, Fri, Sat, Sun, GMTchunked,text/html,image/png,image/jpg,image/gif,application/xml,application/xhtml+xml,text/plain,text/javascript,publicprivatemax-age=gzip,deflate,sdchcharset=utf-8charset=iso-8859-1utf-,*,enq=0.";

const SYN_STREAM: u16 = 1;
const SYN_REPLY: u16 = 2;
const RST_STREAM: u16 = 3;
const SETTINGS: u16 = 4;
const PING: u16 = 6;
const GOAWAY: u16 = 7;
const WINDOW_UPDATE: u16 = 9;
const FLAG_FIN: u8 = 0x01;
const SPDY_VERSION: u16 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamType {
    Stdin,
    Stdout,
    Stderr,
    Error,
    Resize,
    Data,
}

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

impl SpdyFrame {
    pub(super) fn trace_control_metadata(&self) {
        match self {
            Self::SynReply { stream_id } => {
                tracing::trace!(stream_id, "received SPDY SYN_REPLY");
            }
            Self::RstStream { stream_id } => {
                tracing::trace!(stream_id, "received SPDY RST_STREAM");
            }
            Self::WindowUpdate { stream_id } => {
                tracing::trace!(stream_id, "received SPDY WINDOW_UPDATE");
            }
            _ => {}
        }
    }
}

pub struct SpdyConnection {
    pending_frames: VecDeque<SpdyFrame>,
    decompressor: flate2::Decompress,
    compressor: flate2::Compress,
}

impl SpdyConnection {
    pub fn new() -> Self {
        let decompressor = flate2::Decompress::new(true);
        let mut compressor = flate2::Compress::new(flate2::Compression::default(), true);
        compressor
            .set_dictionary(SPDY3_DICT)
            .expect("valid SPDY dictionary");
        Self {
            pending_frames: VecDeque::new(),
            decompressor,
            compressor,
        }
    }

    pub async fn read_frame<S>(&mut self, stream: &mut S) -> anyhow::Result<SpdyFrame>
    where
        S: AsyncRead + Unpin,
    {
        if let Some(frame) = self.pending_frames.pop_front() {
            return Ok(frame);
        }

        let mut header = [0_u8; 8];
        stream.read_exact(&mut header).await?;
        if header[0] & 0x80 == 0 {
            let stream_id = u32::from_be_bytes([header[0] & 0x7f, header[1], header[2], header[3]]);
            let length = ((header[5] as u32) << 16) | ((header[6] as u32) << 8) | header[7] as u32;
            let mut data = vec![0_u8; length as usize];
            if length > 0 {
                stream.read_exact(&mut data).await?;
            }
            return Ok(SpdyFrame::Data {
                stream_id,
                data,
                fin: header[4] & FLAG_FIN != 0,
            });
        }

        let version = u16::from_be_bytes([header[0] & 0x7f, header[1]]);
        let frame_type = u16::from_be_bytes([header[2], header[3]]);
        let length = ((header[5] as u32) << 16) | ((header[6] as u32) << 8) | header[7] as u32;
        if version != SPDY_VERSION {
            tracing::warn!(version, "unexpected Kubernetes SPDY version");
        }
        let mut payload = vec![0_u8; length as usize];
        if length > 0 {
            stream.read_exact(&mut payload).await?;
        }

        match frame_type {
            SYN_STREAM if payload.len() >= 10 => {
                let stream_id =
                    u32::from_be_bytes([payload[0] & 0x7f, payload[1], payload[2], payload[3]]);
                let headers = self.decompress_headers(&payload[10..])?;
                Ok(SpdyFrame::SynStream { stream_id, headers })
            }
            SYN_STREAM => Ok(SpdyFrame::Unknown),
            SYN_REPLY if payload.len() >= 4 => Ok(SpdyFrame::SynReply {
                stream_id: u32::from_be_bytes([
                    payload[0] & 0x7f,
                    payload[1],
                    payload[2],
                    payload[3],
                ]),
            }),
            SYN_REPLY => Ok(SpdyFrame::Unknown),
            RST_STREAM => Ok(SpdyFrame::RstStream {
                stream_id: read_stream_id(&payload),
            }),
            SETTINGS => Ok(SpdyFrame::Settings),
            PING => Ok(SpdyFrame::Ping {
                id: read_u32(&payload),
            }),
            GOAWAY => Ok(SpdyFrame::GoAway),
            WINDOW_UPDATE => Ok(SpdyFrame::WindowUpdate {
                stream_id: read_stream_id(&payload),
            }),
            _ => Ok(SpdyFrame::Unknown),
        }
    }

    fn decompress_headers(&mut self, compressed: &[u8]) -> anyhow::Result<HashMap<String, String>> {
        if compressed.is_empty() {
            return Ok(HashMap::new());
        }
        let mut output = Vec::with_capacity(compressed.len() * 4);
        let mut buffer = [0_u8; 4096];
        let mut input_pos = 0;
        let mut dictionary_set = false;

        loop {
            let before_in = self.decompressor.total_in();
            let before_out = self.decompressor.total_out();
            let input = &compressed[input_pos..];
            if input.is_empty() {
                break;
            }
            match self
                .decompressor
                .decompress(input, &mut buffer, flate2::FlushDecompress::Sync)
            {
                Ok(status) => {
                    let consumed = (self.decompressor.total_in() - before_in) as usize;
                    let produced = (self.decompressor.total_out() - before_out) as usize;
                    input_pos += consumed;
                    output.extend_from_slice(&buffer[..produced]);
                    if status == flate2::Status::StreamEnd || (consumed == 0 && produced == 0) {
                        break;
                    }
                }
                Err(error) if error.to_string().contains("dictionary") && !dictionary_set => {
                    dictionary_set = true;
                    self.decompressor.set_dictionary(SPDY3_DICT)?;
                    input_pos = self.decompressor.total_in() as usize;
                }
                Err(error) => {
                    return Err(anyhow::anyhow!(
                        "Kubernetes SPDY header decompression failed: {error}"
                    ));
                }
            }
        }
        Ok(parse_nv_pairs(&output))
    }

    fn compress_headers(&mut self, headers: &[(&str, &str)]) -> anyhow::Result<Vec<u8>> {
        let mut nv = Vec::new();
        nv.extend_from_slice(&(headers.len() as u32).to_be_bytes());
        for (name, value) in headers {
            nv.extend_from_slice(&(name.len() as u32).to_be_bytes());
            nv.extend_from_slice(name.as_bytes());
            nv.extend_from_slice(&(value.len() as u32).to_be_bytes());
            nv.extend_from_slice(value.as_bytes());
        }
        let mut compressed = vec![0_u8; nv.len() * 2 + 128];
        let before_out = self.compressor.total_out();
        self.compressor
            .compress(&nv, &mut compressed, flate2::FlushCompress::Sync)?;
        compressed.truncate((self.compressor.total_out() - before_out) as usize);
        Ok(compressed)
    }

    pub async fn write_syn_reply<S>(&mut self, stream: &mut S, stream_id: u32) -> anyhow::Result<()>
    where
        S: AsyncWrite + Unpin,
    {
        let headers = self.compress_headers(&[])?;
        let mut payload = Vec::with_capacity(4 + headers.len());
        payload.extend_from_slice(&(stream_id & 0x7fff_ffff).to_be_bytes());
        payload.extend_from_slice(&headers);
        write_control_frame(stream, SYN_REPLY, &payload).await
    }

    pub async fn write_data_frame<S>(
        &self,
        stream: &mut S,
        stream_id: u32,
        data: &[u8],
        fin: bool,
    ) -> anyhow::Result<()>
    where
        S: AsyncWrite + Unpin,
    {
        let mut frame = Vec::with_capacity(8 + data.len());
        frame.extend_from_slice(&(stream_id & 0x7fff_ffff).to_be_bytes());
        frame.push(if fin { FLAG_FIN } else { 0 });
        frame.extend_from_slice(&(data.len() as u32).to_be_bytes()[1..]);
        frame.extend_from_slice(data);
        stream.write_all(&frame).await?;
        stream.flush().await?;
        Ok(())
    }

    pub(super) async fn write_ping<S>(&self, stream: &mut S, id: u32) -> anyhow::Result<()>
    where
        S: AsyncWrite + Unpin,
    {
        write_control_frame(stream, PING, &id.to_be_bytes()).await
    }

    #[cfg(any(test, feature = "test-support"))]
    pub async fn write_syn_stream<S>(
        &mut self,
        stream: &mut S,
        stream_id: u32,
        stream_type: StreamType,
    ) -> anyhow::Result<()>
    where
        S: AsyncWrite + Unpin,
    {
        let stream_type = match stream_type {
            StreamType::Stdin => "stdin",
            StreamType::Stdout => "stdout",
            StreamType::Stderr => "stderr",
            StreamType::Error => "error",
            StreamType::Resize => "resize",
            StreamType::Data => "data",
        };
        let headers = self.compress_headers(&[("streamtype", stream_type)])?;
        let mut payload = Vec::with_capacity(10 + headers.len());
        payload.extend_from_slice(&(stream_id & 0x7fff_ffff).to_be_bytes());
        payload.extend_from_slice(&[0_u8; 6]);
        payload.extend_from_slice(&headers);
        write_control_frame(stream, SYN_STREAM, &payload).await
    }
}

fn read_u32(payload: &[u8]) -> u32 {
    payload
        .get(..4)
        .map(|bytes| u32::from_be_bytes(bytes.try_into().expect("four bytes")))
        .unwrap_or(0)
}

fn read_stream_id(payload: &[u8]) -> u32 {
    read_u32(payload) & 0x7fff_ffff
}

fn parse_nv_pairs(data: &[u8]) -> HashMap<String, String> {
    let mut headers = HashMap::new();
    let Some(count) = data
        .get(..4)
        .map(|bytes| u32::from_be_bytes(bytes.try_into().expect("four bytes")) as usize)
    else {
        return headers;
    };
    let mut position = 4;
    for _ in 0..count {
        let Some(name_len) = read_length(data, &mut position) else {
            break;
        };
        let Some(name) = data.get(position..position + name_len) else {
            break;
        };
        position += name_len;
        let Some(value_len) = read_length(data, &mut position) else {
            break;
        };
        let Some(value) = data.get(position..position + value_len) else {
            break;
        };
        position += value_len;
        headers.insert(
            String::from_utf8_lossy(name).into_owned(),
            String::from_utf8_lossy(value).into_owned(),
        );
    }
    headers
}

fn read_length(data: &[u8], position: &mut usize) -> Option<usize> {
    let bytes = data.get(*position..*position + 4)?;
    *position += 4;
    Some(u32::from_be_bytes(bytes.try_into().ok()?) as usize)
}

async fn write_control_frame<S>(
    stream: &mut S,
    frame_type: u16,
    payload: &[u8],
) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let mut frame = Vec::with_capacity(8 + payload.len());
    frame.extend_from_slice(&(0x8000_u16 | SPDY_VERSION).to_be_bytes());
    frame.extend_from_slice(&frame_type.to_be_bytes());
    frame.push(0);
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes()[1..]);
    frame.extend_from_slice(payload);
    stream.write_all(&frame).await?;
    stream.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn server_framing_accepts_multiple_streams_without_node_codec() {
        let mut writer = SpdyConnection::new();
        let mut bytes = Vec::new();
        writer.write_syn_reply(&mut bytes, 1).await.unwrap();
        writer.write_syn_reply(&mut bytes, 3).await.unwrap();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn parses_remotecommand_stream_header() {
        let mut data = Vec::new();
        data.extend_from_slice(&1_u32.to_be_bytes());
        data.extend_from_slice(&10_u32.to_be_bytes());
        data.extend_from_slice(b"streamtype");
        data.extend_from_slice(&6_u32.to_be_bytes());
        data.extend_from_slice(b"stdout");
        assert_eq!(parse_nv_pairs(&data).get("streamtype").unwrap(), "stdout");
    }
}
