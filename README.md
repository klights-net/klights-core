# klights

klights is a resource-efficient, event-driven Kubernetes-compatible cluster
runtime. `klights` is pronounced **K-light-s**.

The goal is to build a resource-efficient Kubernetes API-compatible
implementation that can run real development workloads with near-zero idle CPU,
a small memory footprint, and the same operational shape users expect from a
Kubernetes cluster.

This repository contains the core source crate.

This beta currently offers:

- Near-zero CPU use during idle through async, event-driven runtime paths.
- A target minimum RAM requirement of 200 MB in the beta release.
- A single-node leader mode with embedded API server, scheduler, controllers,
  datastore, kubelet-facing runtime integration, and local networking.
- Kubernetes-compatible API access through the generated kubeconfig, so `kubectl`
  and Kubernetes clients can talk to klights as a cluster endpoint.
- Raft control-plane mode with exactly three control-plane voters.
- Single-leader mode, optionally paired with replica control-plane learners for
  manual recovery workflows.
- Worker-node and control-plane node joins with separate bootstrap tokens.
- Rootful container runtime integration through containerd and klights-managed
  CNI configuration.

This beta is not ready for production use yet. If you are interested in running
klights in production, sponsorship and production-use feedback would help fund
the work needed to get it there.

Upcoming work includes but is not limited to:

- Rootless operation.
- Hybrid cluster with rootless and root nodes, using built-in CNI.
- CNI plugin support for standard Kubernetes CNI providers such as Calico and
  Flannel.
- Removing the containerd dependency.
- A pluggable datastore backend, with redb as the first target.
- Continued performance and stability improvements.
- An event-driven gRPC API as an alternative to stock Kubernetes polling API
  access patterns.
- GitOps deployment tooling built on the event-driven API, with near-zero CPU
  use during idle.

Current release support is limited to rootful local development mode. Rootless
operation, hybrid rootless/root clusters, expanded CNI plugin support, and
containerd-free runtime support are not available in this release.

## Quickstart

Install klights from the public package repository.

Ubuntu 24.04 (`noble`):

```bash
sudo install -d /etc/apt/keyrings
echo "deb [trusted=yes] https://raw.githubusercontent.com/klights-net/klights-core/package-repo/apt noble main" | \
  sudo tee /etc/apt/sources.list.d/klights.list
sudo apt-get update
sudo apt-get install -y klights
```

Ubuntu 26.04 (`resolute`):

```bash
sudo install -d /etc/apt/keyrings
echo "deb [trusted=yes] https://raw.githubusercontent.com/klights-net/klights-core/package-repo/apt resolute main" | \
  sudo tee /etc/apt/sources.list.d/klights.list
sudo apt-get update
sudo apt-get install -y klights
```

RHEL 9:

```bash
sudo tee /etc/yum.repos.d/klights.repo >/dev/null <<'EOF'
[klights]
name=klights
baseurl=https://raw.githubusercontent.com/klights-net/klights-core/package-repo/rpm/el9/x86_64
enabled=1
gpgcheck=0
EOF
sudo dnf install -y klights
```

RHEL 10:

```bash
sudo tee /etc/yum.repos.d/klights.repo >/dev/null <<'EOF'
[klights]
name=klights
baseurl=https://raw.githubusercontent.com/klights-net/klights-core/package-repo/rpm/el10/x86_64
enabled=1
gpgcheck=0
EOF
sudo dnf install -y klights
```

klights has two control-plane startup modes:

- **Single-leader mode:** start one leader with `klights start`. This is the
  simplest mode for local development. You can optionally add one or more
  `replica` nodes. A replica is a backup copy of the control plane; it follows
  the leader as a Raft learner but does not vote or take over automatically. If
  the leader is lost, you can manually restart a replica as the leader for
  recovery.
- **Raft control-plane mode:** run exactly three `controlplane` voters. Use
  this mode when you want the control plane itself to run as a Raft cluster.

Start the first node with the built-in defaults:

```bash
sudo klights start
```

The defaults cover the API port, node data directory, pod CIDR, service CIDR,
and local runtime paths. With `sudo`, the default kubeconfig is usually written
under `/root/klights`:

`KLIGHTS_DATA_ROOT` defaults to `~/klights`; under `sudo`, that usually resolves
to `/root/klights`.

```bash
sudo kubectl --kubeconfig /root/klights/etc/kubeconfig.yaml get nodes
```

To join more nodes, read the bootstrap token Secrets from the first node and
write them to local token files. The `controlplane-bootstrap-token` Secret is
used by `controlplane` and `replica` joins. The `worker-bootstrap-token` Secret
is used by `worker` joins.

```bash
sudo kubectl --kubeconfig /root/klights/etc/kubeconfig.yaml \
  -n kube-system get secret controlplane-bootstrap-token -o json \
  | jq -r '
      [.data["token-id"], .data["token-secret"]]
      | map(@base64d)
      | "\(.[0]).\(.[1])"
    ' > /tmp/klights-controlplane.token

sudo kubectl --kubeconfig /root/klights/etc/kubeconfig.yaml \
  -n kube-system get secret worker-bootstrap-token -o json \
  | jq -r '
      [.data["token-id"], .data["token-secret"]]
      | map(@base64d)
      | "\(.[0]).\(.[1])"
    ' > /tmp/klights-worker.token

chmod 600 /tmp/klights-controlplane.token /tmp/klights-worker.token
```

For Raft control-plane mode, join the second and third control-plane voters:

```bash
sudo klights controlplane \
  --leader https://<leader-ip>:7679 \
  --skip-ca \
  --token-file /tmp/klights-controlplane.token
```

For single-leader mode, add an optional replica backup:

```bash
sudo klights replica \
  --leader https://<leader-ip>:7679 \
  --skip-ca \
  --token-file /tmp/klights-controlplane.token
```

Join a worker:

```bash
sudo klights worker \
  --leader https://<leader-ip>:7679 \
  --skip-ca \
  --token-file /tmp/klights-worker.token
```

`--skip-ca` is only for first bootstrap when the joiner does not already have
the leader CA certificate. Use the detailed examples below when you need fixed
node names, fixed data roots, custom pod ranges, or explicit external
endpoints.

## Prerequisites

- Linux host with root access for runtime work
- Rust toolchain with `cargo` and `rustc`
- `containerd`
- `kubectl` for interacting with the generated kubeconfig
- Native networking build dependencies such as `pkg-config`, `libnftnl`, and
  `libmnl`

## Build

From the `klights-core` repository root:

```bash
cargo build
cargo build --release
```

Plain Cargo release builds are dynamically linked and are written to:

```text
target/release/klights
```

To build a statically linked GNU/Linux release binary with Cargo, use an
explicit target and static native dependency discovery:

```bash
PKG_CONFIG_ALL_STATIC=1 \
OPENSSL_STATIC=1 \
LIBSQLITE3_SYS_STATIC=1 \
LIBZ_SYS_STATIC=1 \
CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS="-C target-feature=+crt-static" \
cargo build --release --target x86_64-unknown-linux-gnu
```

The static binary is written to:

```text
target/x86_64-unknown-linux-gnu/release/klights
```

Static builds require static archives for the native dependencies used by
`libnftnl`, `libmnl`, zlib, OpenSSL, and SQLite.

Useful local checks:

```bash
cargo fmt --all -- --check
cargo clippy -- -D warnings
cargo test
```

## Single-Node Run

Start a rootful single-node cluster:

```bash
sudo ./target/release/klights start
```

`leader` is equivalent for a single-node seed:

```bash
sudo ./target/release/klights leader
```

After startup, use the generated kubeconfig:

```bash
export KUBECONFIG="${KLIGHTS_DATA_ROOT:-$HOME/klights}/etc/kubeconfig.yaml"
kubectl get nodes
```

## Multinode Run

klights supports four runtime roles:

| Command | Role |
|---|---|
| `klights leader` / `klights start` | Seed leader for a single-node cluster |
| `klights controlplane` | Seed or join as a Raft control-plane voter |
| `klights replica` | Join as a Raft learner replica |
| `klights worker` | Join as a worker-only node |

Use `--token-file` instead of `--token` for joins so bootstrap tokens do not
appear in process arguments. `KLIGHTS_JOIN_TOKEN` can also provide the token.

### Start The First Leader

For the current release, use a control-plane seed when you intend to add more
control-plane members later:

```bash
sudo env \
  KLIGHTS_CONTAINERD_NAMESPACE=klights-cp1 \
  KLIGHTS_DATA_ROOT=/var/lib/klights/cp1 \
  KLIGHTS_NODE_NAME=cp1 \
  KLIGHTS_NODE_IP=10.0.0.10 \
  KLIGHTS_EXTERNAL_ENDPOINT=10.0.0.10 \
  KLIGHTS_POD_SUBNET=10.43.0.0/24 \
  KLIGHTS_CLUSTER_CIDR=10.43.0.0/16 \
  KLIGHTS_SERVICE_CIDR=10.44.0.0/16 \
  ./target/release/klights controlplane
```

The same seed can be started with `leader` for a single-node-only deployment:

```bash
sudo env \
  KLIGHTS_CONTAINERD_NAMESPACE=klights \
  KLIGHTS_DATA_ROOT=/var/lib/klights/leader \
  KLIGHTS_NODE_NAME=leader \
  KLIGHTS_NODE_IP=10.0.0.10 \
  KLIGHTS_EXTERNAL_ENDPOINT=10.0.0.10 \
  ./target/release/klights leader
```

### Get Join Tokens

On the seed node, use the generated admin kubeconfig to read the bootstrap
token Secrets:

```bash
export KUBECONFIG=/var/lib/klights/cp1/etc/kubeconfig.yaml

kubectl -n kube-system get secret controlplane-bootstrap-token -o json \
  | jq -r '
      [.data["token-id"], .data["token-secret"]]
      | map(@base64d)
      | "\(.[0]).\(.[1])"
    ' > /tmp/klights-controlplane.token

kubectl -n kube-system get secret worker-bootstrap-token -o json \
  | jq -r '
      [.data["token-id"], .data["token-secret"]]
      | map(@base64d)
      | "\(.[0]).\(.[1])"
    ' > /tmp/klights-worker.token

chmod 600 /tmp/klights-controlplane.token /tmp/klights-worker.token
```

The control-plane token is used by `controlplane` and `replica` joins. The
worker token is used by `worker` joins.

### Join A Control-Plane Voter

```bash
sudo env \
  KLIGHTS_CONTAINERD_NAMESPACE=klights-cp2 \
  KLIGHTS_DATA_ROOT=/var/lib/klights/cp2 \
  KLIGHTS_NODE_NAME=cp2 \
  KLIGHTS_NODE_IP=10.0.0.11 \
  KLIGHTS_EXTERNAL_ENDPOINT=10.0.0.11 \
  KLIGHTS_POD_SUBNET=10.43.1.0/24 \
  KLIGHTS_CLUSTER_CIDR=10.43.0.0/16 \
  KLIGHTS_SERVICE_CIDR=10.44.0.0/16 \
  ./target/release/klights controlplane \
    --leader https://10.0.0.10:7679 \
    --token-file /tmp/klights-controlplane.token
```

Repeat with a distinct namespace, data root, node name, node IP, external
endpoint, and pod subnet for additional voters.

### Join A Replica

`replica` joins as a Raft learner. It receives cluster state but does not vote.

```bash
sudo env \
  KLIGHTS_CONTAINERD_NAMESPACE=klights-replica1 \
  KLIGHTS_DATA_ROOT=/var/lib/klights/replica1 \
  KLIGHTS_NODE_NAME=replica1 \
  KLIGHTS_NODE_IP=10.0.0.20 \
  KLIGHTS_EXTERNAL_ENDPOINT=10.0.0.20 \
  KLIGHTS_POD_SUBNET=10.43.20.0/24 \
  KLIGHTS_CLUSTER_CIDR=10.43.0.0/16 \
  KLIGHTS_SERVICE_CIDR=10.44.0.0/16 \
  ./target/release/klights replica \
    --leader https://10.0.0.10:7679 \
    --token-file /tmp/klights-controlplane.token
```

Equivalent explicit learner form:

```bash
sudo ./target/release/klights controlplane \
  --leader https://10.0.0.10:7679 \
  --token-file /tmp/klights-controlplane.token \
  --as-learner
```

### Join A Worker

```bash
sudo env \
  KLIGHTS_CONTAINERD_NAMESPACE=klights-worker1 \
  KLIGHTS_DATA_ROOT=/var/lib/klights/worker1 \
  KLIGHTS_NODE_NAME=worker1 \
  KLIGHTS_NODE_IP=10.0.0.30 \
  KLIGHTS_EXTERNAL_ENDPOINT=10.0.0.30 \
  KLIGHTS_POD_SUBNET=10.43.30.0/24 \
  KLIGHTS_CLUSTER_CIDR=10.43.0.0/16 \
  KLIGHTS_SERVICE_CIDR=10.44.0.0/16 \
  ./target/release/klights worker \
    --leader https://10.0.0.10:7679 \
    --token-file /tmp/klights-worker.token
```

Workers may receive multiple leader endpoints, either repeated or
comma-separated:

```bash
sudo ./target/release/klights worker \
  --leader https://10.0.0.10:7679,https://10.0.0.11:7679 \
  --token-file /tmp/klights-worker.token
```

### Rejoin Existing Nodes

After the first successful join, the node has persisted client credentials
under its `KLIGHTS_DATA_ROOT`. A control-plane or worker node can usually
restart with the same command and omit `--token-file`; first-time joins need a
valid token source.

Use `--skip-ca` only for initial bootstrap when the joiner does not yet have
the leader CA certificate and the token is delivered through a trusted channel.

## Runtime Layout

All runtime files live under `KLIGHTS_DATA_ROOT`.

```text
<KLIGHTS_DATA_ROOT>/
  etc/
    ca.crt
    ca.key
    server.crt
    server.key
    admin.crt
    admin.key
    api-proxy.crt
    api-proxy.key
    apiservice-proxy.crt
    apiservice-proxy.key
    kubeconfig.yaml
  db/
    sqlite/
      cluster.db
      cluster.db-wal
      cluster.db-shm
      node.db
      node.db-wal
      node.db-shm
  logs/
    <bridge-name>.log
    pods/
  containerd/
    data/
    state/
  cni/
    net.d/
      <namespace>/
```

`cluster.db` stores Kubernetes resources and watch history. `node.db` stores
node-local runtime durability such as pod runtime rows, pod network rows, and
kubelet workqueues.

The datastore is persistent by default. Set `KLIGHTS_IN_MEMORY=true` for
ephemeral local runs where all state is discarded on exit.

During active development, schema changes can require deleting the affected
database files before startup. There is no stable migration framework before the
first public stable release.

## Environment Variables

### Cluster And Datastore

| Variable | Default | Description |
|---|---|---|
| `KLIGHTS_POD_SUBNET` | `10.43.0.0/17` | This node's pod CIDR |
| `KLIGHTS_CLUSTER_CIDR` | value of `KLIGHTS_POD_SUBNET` | Cluster-wide pod CIDR |
| `KLIGHTS_SERVICE_CIDR` | `10.43.128.0/17` | ClusterIP service CIDR |
| `KLIGHTS_BACKEND` | `sqlite` | Legacy alias for `KLIGHTS_DATASTORE_BACKEND` |
| `KLIGHTS_DATASTORE_BACKEND` | `sqlite` | Cluster datastore backend: `sqlite` or experimental `redb` |
| `KLIGHTS_NODE_LOCAL_BACKEND` | `sqlite` | Node-local datastore backend |
| `KLIGHTS_DB_DIR` | `{KLIGHTS_DATA_ROOT}/db` | Datastore parent directory |
| `KLIGHTS_IN_MEMORY` | `false` | Use in-memory datastore instead of disk |
| `KLIGHTS_DB_ENCRYPTION` | `disabled` | `sqlcipher`, requires `--features sqlcipher` |
| `KLIGHTS_DB_KEY_FILE` | `{KLIGHTS_DB_DIR}/...` | SQLCipher key file path |
| `KLIGHTS_WAL_CHECKPOINT_INTERVAL` | `0` | SQLite WAL checkpoint interval in seconds; `0` uses SQLite defaults |

### Node Runtime

| Variable | Default | Description |
|---|---|---|
| `KLIGHTS_CONTAINERD_NAMESPACE` | `klights` | Containerd namespace and default runtime namespace |
| `KLIGHTS_DATA_ROOT` | `$HOME/<namespace>` | Runtime root directory |
| `KLIGHTS_BRIDGE_NAME` | namespace value | Bridge interface name; long names are truncated to Linux IFNAMSIZ |
| `KLIGHTS_NODE_NAME` | hostname | Kubernetes Node name |
| `KLIGHTS_NODE_IP` | discovered host IP | Override the local Node `InternalIP` |
| `KLIGHTS_EXTERNAL_ENDPOINT` | unset | Reachable endpoint peers use for API joins and node transport |
| `KLIGHTS_TLS_PORT` | `7679` | HTTPS API and internal gRPC port |
| `KLIGHTS_API_FQDN` | unset | Extra DNS SAN for the API server certificate |
| `KLIGHTS_CONTAINERD_SOCKET` | unset | Use an external containerd socket |
| `KLIGHTS_LOG_FILE` | `{data_root}/logs/<bridge-name>.log` | Log output file; `true` resolves to the default file path |
| `KLIGHTS_LOG_MAX_SIZE` | `10485760` | Per-container log rotation size in bytes |
| `KLIGHTS_LOG_MAX_FILES` | `5` | Per-container rotated log file count |
| `KLIGHTS_IMAGE_PULL_RESPONSE_TIMEOUT_SECS` | `30` | CRI image pull response timeout |
| `KLIGHTS_RUNC_BINARY` | `runc` | Override runtime binary used by the runtime wrapper |
| `RUST_LOG` | `klights=debug,tower_http=debug` | Rust log filter |

### Dataplane

| Variable | Default | Description |
|---|---|---|
| `KLIGHTS_DATAPLANE_ENCRYPTION` | `enabled` | Pod dataplane encryption mode: `enabled` or `disabled` |
| `KLIGHTS_WIREGUARD_DEVICE` | `klights.wg` | Encrypted pod dataplane device name |
| `KLIGHTS_WIREGUARD_PORT` | `7679` | UDP port used by the encrypted pod dataplane |
| `KLIGHTS_WORKER_DATAPLANE_NO_INGRESS` | `false` | Set `true` only for worker nodes that cannot accept inbound dataplane traffic |

### Role And Join

| Variable | Default | Description |
|---|---|---|
| `KLIGHTS_JOIN_TOKEN` | unset | Bootstrap token used by `worker`, `replica`, and joining `controlplane` when `--token` is not passed |
| `KLIGHTS_LEADER_CA_CERT` | auto-detected from data root | CA certificate path used to verify leader bootstrap/gRPC TLS |
| `KLIGHTS_CONTROLPLANE_LIMIT` | `3` | Maximum accepted `--leader` endpoints for control-plane membership |

### Authentication

| Variable | Default | Description |
|---|---|---|
| `KLIGHTS_OIDC_ISSUER_URL` | unset | Enables OIDC bearer-token authentication when paired with client ID |
| `KLIGHTS_OIDC_CLIENT_ID` | unset | Required client ID for OIDC tokens |
| `KLIGHTS_OIDC_USERNAME_CLAIM` | `sub` | OIDC username claim |
| `KLIGHTS_OIDC_GROUPS_CLAIM` | `groups` | OIDC groups claim |
| `KLIGHTS_OIDC_GROUPS_PREFIX` | empty | Prefix prepended to OIDC groups |
| `KLIGHTS_OIDC_CA_BUNDLE` | unset | PEM CA bundle for OIDC issuer TLS |
| `KLIGHTS_WEBHOOK_AUTH_URL` | unset | Enables webhook TokenReview authentication |
| `KLIGHTS_WEBHOOK_AUTH_CA_BUNDLE` | unset | PEM CA bundle for webhook TLS |
| `KLIGHTS_WEBHOOK_AUTH_CLIENT_CERT` | unset | Client certificate for webhook mTLS |
| `KLIGHTS_WEBHOOK_AUTH_CLIENT_KEY` | unset | Client key for webhook mTLS |
| `KLIGHTS_WEBHOOK_AUTH_AUDIENCES` | Kubernetes default audience | Comma-separated webhook token audiences |
| `KLIGHTS_WEBHOOK_AUTH_CACHE_AUTHORIZED_TTL_SECS` | `300` | Authorized TokenReview cache TTL |
| `KLIGHTS_WEBHOOK_AUTH_CACHE_UNAUTHORIZED_TTL_SECS` | `30` | Unauthorized/error TokenReview cache TTL |

### Operations

| Variable | Default | Description |
|---|---|---|
| `KLIGHTS_API_SLOW_LOG_MS` | `250` | API request latency threshold for slow logs |
| `KLIGHTS_NODE_ADMIN_PORT` | `7781` | Local node-admin HTTP port on `127.0.0.1` |
| `KLIGHTS_GC_INTERVAL_SECS` | `30` | Leader-scoped GC scheduler interval |
| `KLIGHTS_MAX_WATCH_EVENTS` | `100000` | Retained watch event rows before GC |
| `KLIGHTS_NODE_NOT_READY_POD_EVICTION_GRACE_SECONDS` | `0` | Extra wait after confirmed node silence before pod cleanup |
| `KLIGHTS_POD_LIFECYCLE_MODE` | `actor` | Pod lifecycle router mode: `actor` or `multiplex` |
| `KLIGHTS_POD_ACTOR_IDLE_GRACE_SECS` | `30` | Idle grace before per-pod lifecycle actor shutdown |

### TaskSupervisor Limits

Each task category has a startup-loaded concurrency cap. Unset or empty values
use compiled defaults. `0` means no category-specific limit.

| Variable | Default | Category |
|---|---|---|
| `KLIGHTS_TASK_BACKGROUND` | `0` | Long-lived supervised tasks |
| `KLIGHTS_TASK_FILE` | `3` | Blocking filesystem offload |
| `KLIGHTS_TASK_DB` | `1` | SQLite FFI executor |
| `KLIGHTS_TASK_TIMER` | `0` | Supervised delay tasks |
| `KLIGHTS_TASK_NETWORK` | `256` | Network subsystem tasks |
| `KLIGHTS_TASK_POD_DELETE_WORKQUEUE` | `10` | Pod delete/cascade retry work |
| `KLIGHTS_TASK_POD_LIFECYCLE_ACTOR` | `0` | Per-pod lifecycle actor loops |
| `KLIGHTS_TASK_POD_LIFECYCLE_WORK` | `16` | Short-lived pod lifecycle mutation work |
| `KLIGHTS_TASK_POD_PROBE` | `64` | Probe execution work |
| `KLIGHTS_TASK_OTHERS` | `0` | Catch-all task category |

## Common Commands

```bash
# Build release binary.
cargo build --release

# Run with debug logging.
RUST_LOG=klights=debug sudo ./target/release/klights start

# Stop a systemd-managed local instance, if installed.
sudo systemctl stop klights

# Follow systemd logs, if installed.
sudo journalctl -u klights -f
```

## License

klights-core is dual-licensed under AGPL-3.0-or-later and separate commercial
license terms. See [LICENSE](LICENSE), [NOTICE](NOTICE), and
[LICENSE-AGPL-3.0](LICENSE-AGPL-3.0).

For commercial licensing inquiries, contact: lapcchan@gmail.com.

Contributions must preserve the dual-license model. See
[CONTRIBUTING.md](CONTRIBUTING.md).
