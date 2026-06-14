use std::collections::HashMap;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::frame::{
    FLAG_FIN, GOAWAY, PING, RST_STREAM, SETTINGS, SPDY_VERSION, SPDY3_DICT, SYN_REPLY, SYN_STREAM,
    SpdyExec, SpdyFrame, StreamType, WINDOW_UPDATE,
};

impl SpdyExec {
    /// Read a single SPDY frame from the stream
    pub async fn read_frame<S>(&mut self, stream: &mut S) -> anyhow::Result<SpdyFrame>
    where
        S: AsyncRead + Unpin,
    {
        if let Some(frame) = self.pending_frames.pop_front() {
            return Ok(frame);
        }

        // Read 8-byte frame header
        let mut header = [0u8; 8];
        stream.read_exact(&mut header).await?;

        let is_control = (header[0] & 0x80) != 0;

        if is_control {
            let version = u16::from_be_bytes([header[0] & 0x7F, header[1]]);
            let frame_type = u16::from_be_bytes([header[2], header[3]]);
            let _flags = header[4];
            let length =
                ((header[5] as u32) << 16) | ((header[6] as u32) << 8) | (header[7] as u32);

            if version != SPDY_VERSION {
                tracing::warn!("Unexpected SPDY version: {}", version);
            }

            // Read frame payload
            let mut payload = vec![0u8; length as usize];
            if length > 0 {
                stream.read_exact(&mut payload).await?;
            }

            match frame_type {
                SYN_STREAM => {
                    // Parse SYN_STREAM: stream_id (4), assoc_stream_id (4), priority (2), then headers
                    if payload.len() < 10 {
                        return Ok(SpdyFrame::Unknown);
                    }
                    let stream_id =
                        u32::from_be_bytes([payload[0] & 0x7F, payload[1], payload[2], payload[3]]);
                    // Skip associated_stream_id (4 bytes) and priority (2 bytes)
                    let header_data = &payload[10..];
                    let headers = self.decompress_headers(header_data)?;
                    Ok(SpdyFrame::SynStream { stream_id, headers })
                }
                SYN_REPLY => {
                    // Parse SYN_REPLY: stream_id (4), then headers
                    if payload.len() < 4 {
                        return Ok(SpdyFrame::Unknown);
                    }
                    let stream_id =
                        u32::from_be_bytes([payload[0] & 0x7F, payload[1], payload[2], payload[3]]);
                    Ok(SpdyFrame::SynReply { stream_id })
                }
                RST_STREAM => {
                    let stream_id = if payload.len() >= 4 {
                        u32::from_be_bytes([payload[0] & 0x7F, payload[1], payload[2], payload[3]])
                    } else {
                        0
                    };
                    Ok(SpdyFrame::RstStream { stream_id })
                }
                SETTINGS => Ok(SpdyFrame::Settings),
                PING => {
                    let id = if payload.len() >= 4 {
                        u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]])
                    } else {
                        0
                    };
                    Ok(SpdyFrame::Ping { id })
                }
                GOAWAY => Ok(SpdyFrame::GoAway),
                WINDOW_UPDATE => {
                    let stream_id = if payload.len() >= 4 {
                        u32::from_be_bytes([payload[0] & 0x7F, payload[1], payload[2], payload[3]])
                    } else {
                        0
                    };
                    Ok(SpdyFrame::WindowUpdate { stream_id })
                }
                _ => {
                    tracing::debug!("Unknown SPDY control frame type: {}", frame_type);
                    Ok(SpdyFrame::Unknown)
                }
            }
        } else {
            // Data frame
            let stream_id = u32::from_be_bytes([header[0] & 0x7F, header[1], header[2], header[3]]);
            let flags = header[4];
            let length =
                ((header[5] as u32) << 16) | ((header[6] as u32) << 8) | (header[7] as u32);
            let fin = (flags & FLAG_FIN) != 0;

            let mut data = vec![0u8; length as usize];
            if length > 0 {
                stream.read_exact(&mut data).await?;
            }

            Ok(SpdyFrame::Data {
                stream_id,
                data,
                fin,
            })
        }
    }

    /// Decompress SPDY NV (name/value) headers using zlib with SPDY dictionary.
    /// SPDY/3 uses a persistent zlib context across all frames in the connection.
    /// The first decompression attempt triggers a "need dictionary" error from zlib,
    /// at which point we provide the SPDY/3 dictionary.
    pub fn decompress_headers(
        &mut self,
        compressed: &[u8],
    ) -> anyhow::Result<HashMap<String, String>> {
        if compressed.is_empty() {
            return Ok(HashMap::new());
        }

        let decompressor = &mut self.decompressor;
        let mut decompressed = Vec::with_capacity(compressed.len() * 4);
        let mut buf = [0u8; 4096];
        let mut input_pos = 0;
        let mut dict_set = false;

        loop {
            let before_in = decompressor.total_in();
            let before_out = decompressor.total_out();
            let input = &compressed[input_pos..];

            if input.is_empty() {
                break;
            }

            match decompressor.decompress(input, &mut buf, flate2::FlushDecompress::Sync) {
                Ok(flate2::Status::Ok) => {
                    let consumed = (decompressor.total_in() - before_in) as usize;
                    let produced = (decompressor.total_out() - before_out) as usize;
                    input_pos += consumed;
                    decompressed.extend_from_slice(&buf[..produced]);
                    if consumed == 0 && produced == 0 {
                        break;
                    }
                }
                Ok(flate2::Status::StreamEnd) => {
                    let produced = (decompressor.total_out() - before_out) as usize;
                    decompressed.extend_from_slice(&buf[..produced]);
                    break;
                }
                Ok(flate2::Status::BufError) => break,
                Err(ref e) => {
                    // zlib "need dictionary" — provide the SPDY/3 dictionary and retry
                    let err_msg = format!("{}", e);
                    if err_msg.contains("dictionary") && !dict_set {
                        dict_set = true;
                        // zlib consumed the header bytes before requesting dict
                        let consumed = decompressor.total_in() as usize;
                        tracing::info!(
                            "SPDY: needs dictionary, total_in={}, input_pos={}, compressed_len={}, remaining={}",
                            consumed,
                            input_pos,
                            compressed.len(),
                            compressed.len() - consumed
                        );
                        match decompressor.set_dictionary(SPDY3_DICT) {
                            Ok(adler) => {
                                tracing::info!("SPDY: dictionary set OK, adler32={}", adler)
                            }
                            Err(e) => {
                                return Err(anyhow::anyhow!("SPDY: set_dictionary failed: {}", e));
                            }
                        }
                        input_pos = consumed;
                        continue;
                    }
                    return Err(anyhow::anyhow!("SPDY header decompression failed: {}", e));
                }
            }
        }

        self.parse_nv_pairs(&decompressed)
    }

    /// Parse SPDY NV (name/value) pairs from decompressed data
    pub fn parse_nv_pairs(&self, data: &[u8]) -> anyhow::Result<HashMap<String, String>> {
        let mut headers = HashMap::new();

        if data.len() < 4 {
            return Ok(headers);
        }

        let num_pairs = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
        let mut pos = 4;

        for _ in 0..num_pairs {
            if pos + 4 > data.len() {
                break;
            }
            let name_len =
                u32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
                    as usize;
            pos += 4;

            if pos + name_len > data.len() {
                break;
            }
            let name = String::from_utf8_lossy(&data[pos..pos + name_len]).to_string();
            pos += name_len;

            if pos + 4 > data.len() {
                break;
            }
            let value_len =
                u32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
                    as usize;
            pos += 4;

            if pos + value_len > data.len() {
                break;
            }
            let value = String::from_utf8_lossy(&data[pos..pos + value_len]).to_string();
            pos += value_len;

            headers.insert(name, value);
        }

        Ok(headers)
    }

    /// Compress headers into SPDY NV format with zlib
    pub fn compress_headers(&mut self, headers: &[(&str, &str)]) -> anyhow::Result<Vec<u8>> {
        // Build NV block
        let mut nv = Vec::new();
        let num_pairs = headers.len() as u32;
        nv.extend_from_slice(&num_pairs.to_be_bytes());

        for (name, value) in headers {
            let name_bytes = name.as_bytes();
            let value_bytes = value.as_bytes();
            nv.extend_from_slice(&(name_bytes.len() as u32).to_be_bytes());
            nv.extend_from_slice(name_bytes);
            nv.extend_from_slice(&(value_bytes.len() as u32).to_be_bytes());
            nv.extend_from_slice(value_bytes);
        }

        // Compress with persistent compressor + SPDY dictionary
        // Dictionary was set once in SpdyExec::new()

        // Allocate buffer and compress
        let mut compressed = vec![0u8; nv.len() * 2 + 128];
        let before_out = self.compressor.total_out();
        let _status =
            self.compressor
                .compress(&nv, &mut compressed, flate2::FlushCompress::Sync)?;
        let produced = (self.compressor.total_out() - before_out) as usize;
        compressed.truncate(produced);

        Ok(compressed)
    }

    /// Write a SYN_REPLY frame
    pub async fn write_syn_reply<S>(&mut self, stream: &mut S, stream_id: u32) -> anyhow::Result<()>
    where
        S: AsyncWrite + Unpin,
    {
        // SYN_REPLY: stream_id (4 bytes) + compressed headers
        let headers = self.compress_headers(&[])?;

        let payload_len = 4 + headers.len();
        let mut frame = Vec::with_capacity(8 + payload_len);

        // Control frame header
        frame.push(0x80); // Control bit + version high byte
        frame.push(SPDY_VERSION as u8); // Version low byte
        frame.extend_from_slice(&SYN_REPLY.to_be_bytes()); // Type
        frame.push(0); // Flags
        let len_bytes = (payload_len as u32).to_be_bytes();
        frame.extend_from_slice(&len_bytes[1..4]); // 24-bit length

        // Stream ID
        frame.extend_from_slice(&stream_id.to_be_bytes());

        // Compressed headers (empty for SYN_REPLY to exec streams)
        frame.extend_from_slice(&headers);

        stream.write_all(&frame).await?;
        stream.flush().await?;

        Ok(())
    }

    /// Write a DATA frame
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

        // Data frame header (control bit = 0)
        frame.extend_from_slice(&stream_id.to_be_bytes());
        frame[0] &= 0x7F; // Clear control bit

        let flags: u8 = if fin { FLAG_FIN } else { 0 };
        frame.push(flags);

        let len_bytes = (data.len() as u32).to_be_bytes();
        frame.extend_from_slice(&len_bytes[1..4]); // 24-bit length

        frame.extend_from_slice(data);

        stream.write_all(&frame).await?;
        stream.flush().await?;

        Ok(())
    }

    /// Write a SYN_STREAM frame (client initiates stream)
    pub async fn write_syn_stream<S>(
        &mut self,
        stream: &mut S,
        stream_id: u32,
        stream_type: StreamType,
    ) -> anyhow::Result<()>
    where
        S: AsyncWrite + Unpin,
    {
        // SYN_STREAM: stream_id + priority + slot + compressed headers
        // Headers include streamType: stdin|stdout|stderr|error

        let stream_type_str = match stream_type {
            StreamType::Stdin => "stdin",
            StreamType::Stdout => "stdout",
            StreamType::Stderr => "stderr",
            StreamType::Error => "error",
            StreamType::Resize => "resize",
            StreamType::Data => "data",
        };

        let headers = self.compress_headers(&[("streamtype", stream_type_str)])?;

        let payload_len = 10 + headers.len(); // stream_id(4) + associated_stream(4) + priority(1) + slot(1) + headers
        let mut frame = Vec::with_capacity(8 + payload_len);

        // Control frame header
        frame.push(0x80); // Control bit + version high byte
        frame.push(SPDY_VERSION as u8); // Version low byte
        frame.extend_from_slice(&SYN_STREAM.to_be_bytes()); // Type
        frame.push(0); // Flags
        let len_bytes = (payload_len as u32).to_be_bytes();
        frame.extend_from_slice(&len_bytes[1..4]); // 24-bit length

        // Stream ID
        frame.extend_from_slice(&stream_id.to_be_bytes());

        // Associated stream ID (0 for unassociated)
        frame.extend_from_slice(&[0, 0, 0, 0]);

        // Priority (3 bits) + unused (5 bits) = 1 byte
        frame.push(0);

        // Slot (1 byte)
        frame.push(0);

        // Compressed headers
        frame.extend_from_slice(&headers);

        stream.write_all(&frame).await?;
        stream.flush().await?;

        Ok(())
    }
}
