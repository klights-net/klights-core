use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::io::Read;
use std::io::{IoSlice, IoSliceMut};
use std::net::Shutdown;
use std::os::fd::AsRawFd;
use std::os::fd::FromRawFd;
use std::os::fd::RawFd;
use std::os::unix::net::UnixStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

use crate::networking::PodSubnet;

const DEFAULT_MTU: u32 = crate::networking::wireguard::WIREGUARD_MTU;
const CLEANUP_RPC_MAX_REQUEST_BYTES: usize = 64 * 1024;

pub struct CniRpcState {
    pub containerd_namespace: String,
    pub network: std::sync::Arc<crate::networking::Network>,
    pub task_supervisor: std::sync::Arc<crate::task_supervisor::TaskSupervisor>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct CniConfig {
    #[serde(rename = "cniVersion")]
    cni_version: Option<String>,
    name: String,
    bridge: Option<String>,
    subnet: String,
    mtu: Option<u32>,
    #[serde(rename = "rpcSocket")]
    rpc_socket: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct RpcRequest {
    command: String,
    container_id: String,
    netns: Option<String>,
    ifname: Option<String>,
    pod_namespace: Option<String>,
    pod_name: Option<String>,
    pod_uid: Option<String>,
    config: CniConfig,
}

#[derive(Debug, Deserialize, Serialize)]
struct RpcResponse {
    ok: bool,
    result: Option<serde_json::Value>,
    error: Option<String>,
}

pub fn rpc_socket_path(namespace: &str) -> String {
    crate::paths::cni_rpc_socket_path(namespace)
        .to_string_lossy()
        .into_owned()
}

pub struct CleanupRpcServer {
    socket_path: String,
    listener: tokio::net::UnixListener,
    task_supervisor: crate::task_supervisor::TaskSupervisor,
}

impl CleanupRpcServer {
    pub fn socket_path(&self) -> &str {
        &self.socket_path
    }

    pub async fn serve(self, cancel: CancellationToken) -> Result<()> {
        let Self {
            socket_path,
            listener,
            task_supervisor,
        } = self;

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    break;
                }
                accept = listener.accept() => {
                    let (stream, _) = match accept {
                        Ok(v) => v,
                        Err(e) => {
                            tracing::warn!("cleanup-cni-rpc: accept failed: {}", e);
                            continue;
                        }
                    };
                    let spawn_result = task_supervisor
                        .spawn_async(
                            crate::task_supervisor::TaskCategory::Others,
                            "cleanup_cni_rpc_connection",
                            async move {
                                if let Err(e) = handle_cleanup_rpc_stream(stream).await {
                                    tracing::warn!("cleanup-cni-rpc: request failed: {:#}", e);
                                }
                            },
                        )
                        .await;
                    if let Err(e) = spawn_result {
                        tracing::warn!("cleanup-cni-rpc: failed to spawn request task: {}", e);
                    }
                }
            }
        }

        let _ = crate::utils::remove_file_if_exists_async(&socket_path).await;
        Ok(())
    }
}

pub async fn bind_cleanup_rpc_server(
    namespace: &str,
    task_supervisor: crate::task_supervisor::TaskSupervisor,
) -> Result<CleanupRpcServer> {
    let socket_path = rpc_socket_path(namespace);
    let listener = bind_rpc_listener(&socket_path).await?;
    tracing::info!("cleanup-cni-rpc: listening on {}", socket_path);
    Ok(CleanupRpcServer {
        socket_path,
        listener,
        task_supervisor,
    })
}

pub fn run_from_env() -> i32 {
    match run() {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("klights-cni: {e:#}");
            1
        }
    }
}

fn run() -> Result<()> {
    let command = std::env::var("CNI_COMMAND").context("CNI_COMMAND is required")?;
    let ifname = std::env::var("CNI_IFNAME").unwrap_or_else(|_| "eth0".to_string());
    let netns = std::env::var("CNI_NETNS").unwrap_or_default();

    let mut stdin_buf = Vec::new();
    std::io::stdin()
        .read_to_end(&mut stdin_buf)
        .context("failed to read CNI config from stdin")?;
    let input =
        String::from_utf8(stdin_buf).context("CNI config on stdin is not valid UTF-8 bytes")?;

    if command == "VERSION" && input.trim().is_empty() {
        println!(
            "{}",
            json!({
                "cniVersion": "1.0.0",
                "supportedVersions": ["1.0.0"]
            })
        );
        return Ok(());
    }

    let config: CniConfig = serde_json::from_str(&input).context("invalid CNI config JSON")?;

    match command.as_str() {
        "ADD" => {
            let container_id =
                std::env::var("CNI_CONTAINERID").context("CNI_CONTAINERID is required")?;
            if netns.is_empty() {
                return Err(anyhow!("CNI_NETNS is required for ADD"));
            }
            let args = parse_cni_args(std::env::var("CNI_ARGS").unwrap_or_default().as_str());
            let req = RpcRequest {
                command: command.clone(),
                container_id,
                netns: Some(netns),
                ifname: Some(ifname),
                pod_namespace: args.get("K8S_POD_NAMESPACE").cloned(),
                pod_name: args.get("K8S_POD_NAME").cloned(),
                pod_uid: args.get("K8S_POD_UID").cloned(),
                config: config.clone(),
            };
            let netns_file = std::fs::File::open(
                req.netns
                    .as_deref()
                    .ok_or_else(|| anyhow!("missing netns path"))?,
            )
            .context("failed to open CNI_NETNS for fd passing")?;
            let resp = send_rpc(&config, &req, Some(netns_file.as_raw_fd()))?;
            if !resp.ok {
                return Err(anyhow!(
                    "{}",
                    resp.error.unwrap_or_else(|| "CNI ADD failed".to_string())
                ));
            }
            println!(
                "{}",
                serde_json::to_string(
                    &resp
                        .result
                        .ok_or_else(|| anyhow!("missing result in CNI ADD response"))?
                )?
            );
            Ok(())
        }
        "DEL" => {
            let container_id =
                std::env::var("CNI_CONTAINERID").context("CNI_CONTAINERID is required")?;
            let req = RpcRequest {
                command,
                container_id,
                netns: None,
                ifname: None,
                pod_namespace: None,
                pod_name: None,
                pod_uid: None,
                config: config.clone(),
            };
            let resp = send_rpc(&config, &req, None)?;
            if !resp.ok {
                return Err(anyhow!(
                    "{}",
                    resp.error.unwrap_or_else(|| "CNI DEL failed".to_string())
                ));
            }
            Ok(())
        }
        "CHECK" => Ok(()),
        "VERSION" => {
            println!(
                "{}",
                json!({
                    "cniVersion": cni_version(&config),
                    "supportedVersions": ["1.0.0"]
                })
            );
            Ok(())
        }
        other => Err(anyhow!("unsupported CNI_COMMAND {other}")),
    }
}

fn send_rpc(config: &CniConfig, req: &RpcRequest, netns_fd: Option<RawFd>) -> Result<RpcResponse> {
    let socket = config
        .rpc_socket
        .clone()
        .unwrap_or_else(|| rpc_socket_path("klights"));
    let mut stream =
        UnixStream::connect(&socket).with_context(|| format!("failed to connect {}", socket))?;
    let payload = serde_json::to_vec(req).context("failed to serialize CNI RPC request")?;
    let iov = [IoSlice::new(&payload)];
    if let Some(fd) = netns_fd {
        nix::sys::socket::sendmsg::<()>(
            stream.as_raw_fd(),
            &iov,
            &[nix::sys::socket::ControlMessage::ScmRights(&[fd])],
            nix::sys::socket::MsgFlags::empty(),
            None,
        )
        .context("failed to send CNI RPC request with netns fd")?;
    } else {
        nix::sys::socket::sendmsg::<()>(
            stream.as_raw_fd(),
            &iov,
            &[],
            nix::sys::socket::MsgFlags::empty(),
            None,
        )
        .context("failed to send CNI RPC request")?;
    }
    stream
        .shutdown(Shutdown::Write)
        .context("failed to shutdown CNI RPC write half")?;
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .context("failed to read CNI RPC response")?;
    serde_json::from_slice(&response).context("invalid CNI RPC response JSON")
}

pub async fn run_rpc_server(
    state: std::sync::Arc<CniRpcState>,
    cancel: CancellationToken,
) -> Result<()> {
    let socket_path = rpc_socket_path(&state.containerd_namespace);
    let listener = bind_rpc_listener(&socket_path).await?;
    tracing::info!("cni-rpc: listening on {}", socket_path);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                break;
            }
            accept = listener.accept() => {
                let (mut stream, _) = match accept {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::warn!("cni-rpc: accept failed: {}", e);
                        continue;
                    }
                };
                let state = state.clone();
                let task_supervisor = state.task_supervisor.clone();
                let task_supervisor_for_conn = task_supervisor.clone();
                if let Err(err) = task_supervisor
                    .spawn_async(
                        crate::task_supervisor::TaskCategory::Others,
                        "cni_rpc_connection",
                        async move {
                            let stream_fd = stream.as_raw_fd();
                            let recv = task_supervisor_for_conn
                                .run_blocking(
                                    crate::task_supervisor::TaskCategory::Others,
                                    "cni_rpc_recv_request",
                                    move || recv_request_blocking(stream_fd),
                                )
                                .await;
                    let (req_buf, netns_file) = match recv {
                        Ok(Ok(v)) => v,
                        Ok(Err(e)) => {
                            let _ = write_resp(
                                &mut stream,
                                RpcResponse {
                                    ok: false,
                                    result: None,
                                    error: Some(format!("read request failed: {:#}", e)),
                                },
                            )
                            .await;
                            return;
                        }
                        Err(e) => {
                            let _ = write_resp(
                                &mut stream,
                                RpcResponse {
                                    ok: false,
                                    result: None,
                                    error: Some(format!("read task failed: {}", e)),
                                },
                            )
                            .await;
                            return;
                        }
                    };
                    let req: RpcRequest = match serde_json::from_slice(&req_buf) {
                        Ok(v) => v,
                        Err(e) => {
                            let _ = write_resp(
                                &mut stream,
                                RpcResponse {
                                    ok: false,
                                    result: None,
                                    error: Some(format!("invalid request: {}", e)),
                                },
                            )
                            .await;
                            return;
                        }
                    };
                    let resp = match handle_request(state, req, netns_file).await {
                        Ok(result) => RpcResponse {
                            ok: true,
                            result,
                            error: None,
                        },
                        Err(e) => {
                            tracing::warn!("cni-rpc: request failed: {:#}", e);
                            RpcResponse {
                                ok: false,
                                result: None,
                                error: Some(format!("{:#}", e)),
                            }
                        }
                    };
                    let _ = write_resp(&mut stream, resp).await;
                        },
                    )
                    .await
                {
                    tracing::warn!("Failed to spawn cni-rpc connection task: {}", err);
                }
            }
        }
    }

    let _ = crate::utils::remove_file_if_exists_async(&socket_path).await;
    Ok(())
}

async fn bind_rpc_listener(socket_path: &str) -> Result<tokio::net::UnixListener> {
    if let Some(parent) = std::path::Path::new(&socket_path).parent() {
        crate::utils::create_dir_all_async(parent)
            .await
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let _ = crate::utils::remove_file_if_exists_async(socket_path).await;
    let listener = tokio::net::UnixListener::bind(socket_path)
        .with_context(|| format!("failed to bind {}", socket_path))?;
    Ok(listener)
}

async fn handle_cleanup_rpc_stream(mut stream: tokio::net::UnixStream) -> Result<()> {
    let req_buf = match read_cleanup_rpc_request(&mut stream).await {
        Ok(req_buf) => req_buf,
        Err(e) => {
            return write_resp(
                &mut stream,
                RpcResponse {
                    ok: false,
                    result: None,
                    error: Some(format!("{:#}", e)),
                },
            )
            .await;
        }
    };
    let resp = match serde_json::from_slice::<RpcRequest>(&req_buf) {
        Ok(req) => cleanup_rpc_response_for_request(&req),
        Err(e) => RpcResponse {
            ok: false,
            result: None,
            error: Some(format!("invalid request: {}", e)),
        },
    };
    write_resp(&mut stream, resp).await
}

async fn read_cleanup_rpc_request(stream: &mut tokio::net::UnixStream) -> Result<Vec<u8>> {
    let mut req_buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let n = stream
            .read(&mut chunk)
            .await
            .context("failed to read cleanup CNI RPC request")?;
        if n == 0 {
            break;
        }
        if req_buf.len() + n > CLEANUP_RPC_MAX_REQUEST_BYTES {
            return Err(anyhow!(
                "cleanup CNI RPC request too large: max {} bytes",
                CLEANUP_RPC_MAX_REQUEST_BYTES
            ));
        }
        req_buf.extend_from_slice(&chunk[..n]);
    }
    Ok(req_buf)
}

fn cleanup_rpc_response_for_request(req: &RpcRequest) -> RpcResponse {
    match req.command.as_str() {
        "DEL" | "CHECK" => RpcResponse {
            ok: true,
            result: None,
            error: None,
        },
        "ADD" => RpcResponse {
            ok: false,
            result: None,
            error: Some("cleanup CNI RPC server does not accept ADD".to_string()),
        },
        other => RpcResponse {
            ok: false,
            result: None,
            error: Some(format!("unsupported cleanup CNI RPC command {other}")),
        },
    }
}

async fn write_resp(stream: &mut tokio::net::UnixStream, resp: RpcResponse) -> Result<()> {
    let payload = serde_json::to_vec(&resp)?;
    stream.write_all(&payload).await?;
    stream.shutdown().await?;
    Ok(())
}

async fn handle_request(
    state: std::sync::Arc<CniRpcState>,
    req: RpcRequest,
    netns_file: Option<std::fs::File>,
) -> Result<Option<serde_json::Value>> {
    match req.command.as_str() {
        "ADD" => {
            let (netns_setns_path, netns_record_path) =
                resolve_add_netns_paths(req.netns.as_deref(), netns_file.as_ref())?;
            let pod_namespace = req
                .pod_namespace
                .clone()
                .unwrap_or_else(|| "default".to_string());
            let pod_name = req
                .pod_name
                .clone()
                .unwrap_or_else(|| req.container_id.clone());
            let pod_uid = req
                .pod_uid
                .clone()
                .unwrap_or_else(|| req.container_id.clone());
            let pod_namespace_for_log = pod_namespace.clone();
            let pod_name_for_log = pod_name.clone();
            let network = state
                .network
                .datapath
                .cni_add(crate::networking::provider::CniAddRequest {
                    sandbox_id: req.container_id.clone(),
                    namespace: pod_namespace,
                    pod_name,
                    pod_uid,
                    netns_setns_path,
                    netns_record_path: netns_record_path.clone(),
                    host_network: false,
                })
                .await
                .with_context(|| {
                    format!(
                        "failed in-process CNI ADD for {}/{} ({})",
                        pod_namespace_for_log, pod_name_for_log, req.container_id
                    )
                })?;
            let result = build_cni_result(&req.config, &netns_record_path, network.ip_addr)?;
            Ok(Some(result))
        }
        "DEL" => {
            state
                .network
                .datapath
                .cni_del(&req.container_id)
                .await
                .with_context(|| format!("failed in-process CNI DEL for {}", req.container_id))?;
            Ok(None)
        }
        "CHECK" => Ok(None),
        other => Err(anyhow!("unsupported RPC command {}", other)),
    }
}

fn resolve_add_netns_paths(
    req_netns: Option<&str>,
    netns_file: Option<&std::fs::File>,
) -> Result<(String, String)> {
    let netns_record_path = req_netns
        .map(str::to_string)
        .ok_or_else(|| anyhow!("missing netns for ADD"))?;
    let netns_setns_path = if let Some(fd_file) = netns_file {
        format!("/proc/self/fd/{}", fd_file.as_raw_fd())
    } else {
        netns_record_path.clone()
    };
    Ok((netns_setns_path, netns_record_path))
}

/// Take exclusive ownership of a file descriptor freshly received via
/// `SCM_RIGHTS` and wrap it in a `File` whose `Drop` will close it.
///
/// The kernel transfers ownership of fds delivered through ancillary
/// messages to the receiving process (see `unix(7)`); they are valid,
/// not shared with the sender, and must be closed exactly once. That
/// invariant lets us turn the raw integer into a typed owned handle
/// without a runtime check.
///
/// Reused for cross-namespace fd grafting; keeping the unsafe surface in one
/// helper means future call sites stay safe-by-construction.
///
/// # Safety
/// `fd` must be a valid, currently-open file descriptor that the caller
/// owns exclusively. Passing a stolen, closed, or shared fd is undefined
/// behavior. Within this module the only producer is the SCM_RIGHTS
/// receive path in `recv_request`, which satisfies the contract.
unsafe fn take_owned_fd_as_file(fd: RawFd) -> std::fs::File {
    // SAFETY: caller contract above guarantees `fd` is valid and uniquely
    // owned, so File::from_raw_fd moves ownership without aliasing.
    unsafe { std::fs::File::from_raw_fd(fd) }
}

fn recv_request(fd: RawFd) -> Result<(Vec<u8>, Option<std::fs::File>)> {
    let mut buf = vec![0u8; 64 * 1024];
    let mut iov = [IoSliceMut::new(&mut buf)];
    let mut cmsgspace = nix::cmsg_space!([RawFd; 1]);
    let msg = nix::sys::socket::recvmsg::<()>(
        fd,
        &mut iov,
        Some(&mut cmsgspace),
        nix::sys::socket::MsgFlags::empty(),
    )
    .context("recvmsg failed")?;
    let bytes = msg.bytes;
    if bytes == 0 {
        return Err(anyhow!("empty request"));
    }

    let mut netns_file = None;
    if let Ok(cmsgs) = msg.cmsgs() {
        for cmsg in cmsgs {
            if let nix::sys::socket::ControlMessageOwned::ScmRights(fds) = cmsg
                && let Some(netns_fd) = fds.first()
            {
                // SAFETY: ScmRights fds are kernel-allocated and ownership
                // is transferred to this process by the cmsg delivery; we
                // are the sole owner and `take_owned_fd_as_file` will close
                // the fd via File::drop.
                let f = unsafe { take_owned_fd_as_file(*netns_fd) };
                netns_file = Some(f);
                break;
            }
        }
    }

    Ok((buf[..bytes].to_vec(), netns_file))
}

fn recv_request_blocking(fd: RawFd) -> Result<(Vec<u8>, Option<std::fs::File>)> {
    // tokio UnixStream sockets are nonblocking; recvmsg on that fd from a blocking
    // thread can spuriously fail with EAGAIN under load. Duplicate the fd and force
    // blocking mode for ancillary-data (SCM_RIGHTS) reads.
    //
    // SAFETY: `dup(2)` accepts any valid fd and returns either a fresh
    // owned fd or -1; the negative-return branch below preserves the
    // invariant that `dup_fd` is only consumed when valid.
    let dup_fd = unsafe { nix::libc::dup(fd) };
    if dup_fd < 0 {
        return Err(anyhow!(
            "failed to dup cni rpc fd: {}",
            std::io::Error::last_os_error()
        ));
    }
    // SAFETY: `dup_fd` was just produced by a successful `dup(2)` (we
    // returned above on failure), so it is valid and uniquely owned by
    // this process. UnixStream takes ownership; its Drop closes the fd.
    let std_stream = unsafe { std::os::unix::net::UnixStream::from_raw_fd(dup_fd) };
    std_stream
        .set_nonblocking(false)
        .context("failed to switch cni rpc fd to blocking mode")?;
    recv_request(std_stream.as_raw_fd())
}

fn build_cni_result(
    config: &CniConfig,
    netns: &str,
    pod_ip: std::net::IpAddr,
) -> Result<serde_json::Value> {
    let bridge = config.bridge.as_deref().unwrap_or(&config.name);
    let subnet = PodSubnet::parse(&config.subnet)
        .map_err(|e| anyhow!("invalid CNI subnet {}: {}", config.subnet, e))?;
    let prefix_len = subnet.prefix();
    let gateway = subnet.bridge_ip();
    let _ = config.mtu.unwrap_or(DEFAULT_MTU);
    Ok(json!({
        "cniVersion": cni_version(config),
        "interfaces": [
            {"name": bridge},
            {"name": "eth0", "sandbox": netns}
        ],
        "ips": [
            {
                "version": "4",
                "interface": 1,
                "address": format!("{}/{}", pod_ip, prefix_len),
                "gateway": gateway.to_string()
            }
        ],
        "routes": [{"dst": "0.0.0.0/0", "gw": gateway.to_string()}],
        "dns": {}
    }))
}

fn parse_cni_args(raw: &str) -> HashMap<String, String> {
    raw.split(';')
        .filter_map(|kv| {
            let (k, v) = kv.split_once('=')?;
            Some((k.to_string(), v.to_string()))
        })
        .collect()
}

fn cni_version(config: &CniConfig) -> String {
    config
        .cni_version
        .clone()
        .unwrap_or_else(|| "1.0.0".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cni_version_defaults_to_1_0_0() {
        let config = CniConfig {
            cni_version: None,
            name: "klights".to_string(),
            bridge: Some("klights".to_string()),
            subnet: "10.43.0.0/24".to_string(),
            mtu: None,
            rpc_socket: None,
        };

        assert_eq!(cni_version(&config), "1.0.0");
    }

    #[test]
    fn parse_cni_args_extracts_k8s_keys() {
        let args = parse_cni_args(
            "IgnoreUnknown=1;K8S_POD_NAMESPACE=default;K8S_POD_NAME=ng;K8S_POD_UID=u1",
        );
        assert_eq!(
            args.get("K8S_POD_NAMESPACE").map(String::as_str),
            Some("default")
        );
        assert_eq!(args.get("K8S_POD_NAME").map(String::as_str), Some("ng"));
        assert_eq!(args.get("K8S_POD_UID").map(String::as_str), Some("u1"));
    }

    #[test]
    fn rpc_socket_path_uses_namespace() {
        assert_eq!(
            rpc_socket_path("klights"),
            crate::paths::cni_rpc_socket_path("klights")
                .to_string_lossy()
                .into_owned()
        );
    }

    #[test]
    fn cleanup_rpc_accepts_del_but_rejects_add() {
        let config = CniConfig {
            cni_version: Some("1.0.0".to_string()),
            name: "klights".to_string(),
            bridge: Some("klights".to_string()),
            subnet: "10.43.0.0/17".to_string(),
            mtu: None,
            rpc_socket: None,
        };

        let del = RpcRequest {
            command: "DEL".to_string(),
            container_id: "sandbox-a".to_string(),
            netns: None,
            ifname: None,
            pod_namespace: None,
            pod_name: None,
            pod_uid: None,
            config: config.clone(),
        };
        let add = RpcRequest {
            command: "ADD".to_string(),
            container_id: "sandbox-a".to_string(),
            netns: Some("/run/netns/cni-sandbox-a".to_string()),
            ifname: Some("eth0".to_string()),
            pod_namespace: Some("default".to_string()),
            pod_name: Some("pod-a".to_string()),
            pod_uid: Some("uid-a".to_string()),
            config,
        };

        let del_resp = cleanup_rpc_response_for_request(&del);
        assert!(
            del_resp.ok,
            "cleanup server must let containerd CNI DEL finish"
        );
        assert!(del_resp.error.is_none());

        let add_resp = cleanup_rpc_response_for_request(&add);
        assert!(
            !add_resp.ok,
            "cleanup server must not create new pod networking"
        );
        assert!(
            add_resp
                .error
                .as_deref()
                .unwrap_or_default()
                .contains("does not accept ADD")
        );
    }

    fn test_cni_config() -> CniConfig {
        CniConfig {
            cni_version: Some("1.0.0".to_string()),
            name: "klights".to_string(),
            bridge: Some("klights".to_string()),
            subnet: "10.43.0.0/17".to_string(),
            mtu: None,
            rpc_socket: None,
        }
    }

    fn test_del_request_bytes() -> Vec<u8> {
        serde_json::to_vec(&RpcRequest {
            command: "DEL".to_string(),
            container_id: "sandbox-a".to_string(),
            netns: None,
            ifname: None,
            pod_namespace: None,
            pod_name: None,
            pod_uid: None,
            config: test_cni_config(),
        })
        .unwrap()
    }

    async fn read_rpc_response(stream: &mut tokio::net::UnixStream) -> RpcResponse {
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        serde_json::from_slice(&response).unwrap()
    }

    #[tokio::test]
    async fn cleanup_rpc_rejects_oversized_request_without_unbounded_read() {
        let (mut client, server) = tokio::net::UnixStream::pair().unwrap();
        let server_task = tokio::spawn(async move { handle_cleanup_rpc_stream(server).await });

        client
            .write_all(&vec![b' '; CLEANUP_RPC_MAX_REQUEST_BYTES + 1])
            .await
            .unwrap();
        client.shutdown().await.unwrap();

        let response = read_rpc_response(&mut client).await;
        assert!(!response.ok);
        assert!(
            response
                .error
                .as_deref()
                .unwrap_or_default()
                .contains("too large"),
            "oversized cleanup RPC request must produce a size error, got {response:?}"
        );
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn cleanup_rpc_invalid_json_returns_error_response() {
        let (mut client, server) = tokio::net::UnixStream::pair().unwrap();
        let server_task = tokio::spawn(async move { handle_cleanup_rpc_stream(server).await });

        client.write_all(b"{not-json").await.unwrap();
        client.shutdown().await.unwrap();

        let response = read_rpc_response(&mut client).await;
        assert!(!response.ok);
        assert!(
            response
                .error
                .as_deref()
                .unwrap_or_default()
                .contains("invalid request"),
            "invalid JSON should produce an invalid request response, got {response:?}"
        );
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn cleanup_rpc_slow_client_does_not_block_second_client() {
        let tmp = tempfile::tempdir().unwrap();
        let socket_path = tmp.path().join("cleanup.sock");
        let socket_path = socket_path.to_string_lossy().into_owned();
        let listener = bind_rpc_listener(&socket_path).await.unwrap();
        let server = CleanupRpcServer {
            socket_path: socket_path.clone(),
            listener,
            task_supervisor: crate::task_supervisor::TaskSupervisor::new(
                crate::task_supervisor::TaskCategoryConfig::default(),
            ),
        };
        let cancel = CancellationToken::new();
        let server_cancel = cancel.clone();
        let server_task = tokio::spawn(async move { server.serve(server_cancel).await });

        let mut slow = tokio::net::UnixStream::connect(&socket_path).await.unwrap();
        slow.write_all(b"{").await.unwrap();

        let mut fast = tokio::net::UnixStream::connect(&socket_path).await.unwrap();
        fast.write_all(&test_del_request_bytes()).await.unwrap();
        fast.shutdown().await.unwrap();

        let response = tokio::time::timeout(
            std::time::Duration::from_millis(250),
            read_rpc_response(&mut fast),
        )
        .await
        .expect("second cleanup RPC client should not wait for the first client to close");
        assert!(response.ok, "DEL response should succeed, got {response:?}");

        cancel.cancel();
        drop(slow);
        server_task.await.unwrap().unwrap();
    }

    // The "no crate::api::AppState in cni_plugin.rs" invariant is enforced by
    // the base-repo source guard run by `./build.sh`.

    #[test]
    fn build_cni_result_includes_default_route_gateway() {
        let config = CniConfig {
            cni_version: Some("1.0.0".to_string()),
            name: "klights".to_string(),
            bridge: Some("klights".to_string()),
            subnet: "10.43.0.0/17".to_string(),
            mtu: None,
            rpc_socket: None,
        };

        let result = build_cni_result(
            &config,
            "/var/run/netns/test",
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(10, 43, 0, 42)),
        )
        .expect("cni result");
        let gw = result
            .pointer("/routes/0/gw")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        assert_eq!(
            gw, "10.43.0.1",
            "default route gateway must point at bridge gateway to make service CIDR reachable"
        );
    }

    #[test]
    fn recv_request_takes_ownership_of_scm_rights_fd() {
        // End-to-end exercise of the SCM_RIGHTS path: send a payload + a
        // /dev/null fd through a socketpair, then assert recv_request hands
        // back both the bytes and a working File whose ownership it just
        // took. Catches future regressions in the unsafe fd handover.
        use nix::sys::socket::{
            AddressFamily, ControlMessage, MsgFlags, SockFlag, SockType, sendmsg, socketpair,
        };
        use std::io::IoSlice;
        use std::os::fd::IntoRawFd;
        use std::os::unix::fs::FileTypeExt;

        let (a, b) = socketpair(
            AddressFamily::Unix,
            SockType::Stream,
            None,
            SockFlag::empty(),
        )
        .expect("socketpair");

        let dev_null = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/null")
            .expect("open /dev/null");
        let payload = b"hello-cni";
        let fd_to_send = [dev_null.as_raw_fd()];
        let cmsg = [ControlMessage::ScmRights(&fd_to_send)];
        let iov = [IoSlice::new(payload)];

        sendmsg::<()>(a.as_raw_fd(), &iov, &cmsg, MsgFlags::empty(), None)
            .expect("sendmsg with SCM_RIGHTS");
        // dev_null fd was duplicated by the kernel into the cmsg; closing
        // the original here proves recv_request gets a fresh, owned dup.
        drop(dev_null);

        let (buf, file_opt) = recv_request(b.as_raw_fd()).expect("recv_request");
        assert_eq!(&buf[..], payload, "payload bytes round-trip intact");
        let file = file_opt.expect("recv_request must take ownership of the SCM_RIGHTS fd");
        let metadata = file.metadata().expect("File handle is alive and valid");
        assert!(
            metadata.file_type().is_char_device(),
            "received fd should still point at /dev/null (a char device)"
        );

        // a and b are OwnedFd-like; let scope drop handle close.
        let _ = a.into_raw_fd();
        let _ = b.into_raw_fd();
    }

    #[test]
    fn resolve_add_netns_paths_without_fd_uses_request_path() {
        let (setns_path, record_path) =
            resolve_add_netns_paths(Some("/var/run/netns/cni-123"), None).expect("paths");
        assert_eq!(setns_path, "/var/run/netns/cni-123");
        assert_eq!(record_path, "/var/run/netns/cni-123");
    }

    #[test]
    fn resolve_add_netns_paths_with_fd_keeps_record_path_stable() {
        let f = std::fs::File::open("/proc/self/ns/net").expect("open current netns");
        let (setns_path, record_path) =
            resolve_add_netns_paths(Some("/var/run/netns/cni-123"), Some(&f)).expect("paths");
        assert_eq!(record_path, "/var/run/netns/cni-123");
        assert!(
            setns_path.starts_with("/proc/self/fd/"),
            "setns path should use SCM_RIGHTS fd when available"
        );
    }
}
