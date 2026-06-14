use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::frame::{PING, SPDY_VERSION, SpdyExec, StreamType};

impl SpdyExec {
    /// Connect to containerd streaming URL and perform SPDY/3.1 upgrade
    /// Returns the TCP stream after successful upgrade
    pub async fn connect_to_streaming_url(url: &str) -> anyhow::Result<tokio::net::TcpStream> {
        let url_parts: Vec<&str> = url.trim_start_matches("http://").split('/').collect();
        if url_parts.len() < 3 {
            anyhow::bail!("Invalid streaming URL format: {}", url);
        }

        let host_port = url_parts[0];
        let path = format!("/{}", url_parts[1..].join("/"));

        let mut stream = tokio::net::TcpStream::connect(host_port).await?;

        let upgrade_request = format!(
            "GET {} HTTP/1.1\r\n\
             Host: {}\r\n\
             Connection: Upgrade\r\n\
             Upgrade: SPDY/3.1\r\n\
             X-Stream-Protocol-Version: v4.channel.k8s.io\r\n\
             X-Stream-Protocol-Version: v3.channel.k8s.io\r\n\
             X-Stream-Protocol-Version: v2.channel.k8s.io\r\n\
             \r\n",
            path, host_port
        );

        stream.write_all(upgrade_request.as_bytes()).await?;
        stream.flush().await?;

        let mut response_buf = vec![0u8; 1024];
        let n = stream.read(&mut response_buf).await?;
        let response = String::from_utf8_lossy(&response_buf[..n]);

        if !response.starts_with("HTTP/1.1 101") && !response.starts_with("HTTP/1.0 101") {
            anyhow::bail!("SPDY upgrade failed. Response: {}", response);
        }

        tracing::debug!("SPDY/3.1 upgrade successful to {}", url);

        Ok(stream)
    }

    pub async fn write_ping<S>(&self, stream: &mut S, id: u32) -> anyhow::Result<()>
    where
        S: tokio::io::AsyncWrite + Unpin,
    {
        use tokio::io::AsyncWriteExt;
        let mut frame = Vec::with_capacity(12);

        frame.push(0x80);
        frame.push(SPDY_VERSION as u8);
        frame.extend_from_slice(&PING.to_be_bytes());
        frame.push(0);
        frame.extend_from_slice(&[0, 0, 4]);
        frame.extend_from_slice(&id.to_be_bytes());

        stream.write_all(&frame).await?;
        stream.flush().await?;

        Ok(())
    }

    pub fn stream_id_for(&self, stream_type: StreamType) -> Option<u32> {
        self.streams
            .iter()
            .find(|(_, t)| **t == stream_type)
            .map(|(id, _)| *id)
    }
}
