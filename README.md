# klights

klights is a resource-efficient, event-driven Kubernetes-compatible cluster
runtime. pronounced **K-light-s**.

Packaged with both ARM and x86 build on Ubuntu 24.04/26.04 and RHEL 9/10. 

Start with [QUICKSTART.md](QUICKSTART.md). 

Build, run, configuration, and operations documentation [doc/README.md](doc/README.md).

## Install from APT

Ubuntu 24.04 (`noble`) and Ubuntu 26.04 (`resolute`) packages are published in
the signed public APT repository.

```bash
sudo install -d -m 0755 /etc/apt/keyrings
sudo curl -fsSL \
  https://raw.githubusercontent.com/klights-net/klights-core/package-repo/apt/klights-archive-keyring.asc \
  -o /etc/apt/keyrings/klights-archive-keyring.asc
sudo chmod 0644 /etc/apt/keyrings/klights-archive-keyring.asc
sudo tee /etc/apt/sources.list.d/klights.sources >/dev/null <<'EOF'
Types: deb
URIs: https://raw.githubusercontent.com/klights-net/klights-core/package-repo/apt/
Suites: noble
Components: main
Signed-By: /etc/apt/keyrings/klights-archive-keyring.asc
EOF
sudo apt-get update
sudo apt-get install -y klights
```

For Ubuntu 26.04, use `Suites: resolute` instead of `Suites: noble`.

### Sonobuoy Conformance tests
Klights completed Sonobuoy Conformance tests with 3 raft controlplanes, one replicas and 2 workers nodes.

- Sonobuoy Version: v0.57.3
- Ran 424 of 7144 Specs in 2260.475 seconds
- SUCCESS! -- 424 Passed | 0 Failed | 0 Pending | 6720 Skipped


### Baseline Memory usage
startup, with klights running and only CoreDNS deployed:

- klights process RSS:      approximately 76.1 MiB
- klights process peak RSS: approximately 76.5 MiB
- klights.service cgroup:   approximately 150.6 MiB
- klights.service peak:     approximately 167.9 MiB

Processes inside klights.service at that point included:

- klights:          approximately 76.1 MiB RSS
- containerd:       approximately 54.4 MiB RSS
- containerd-shim:  approximately 10.7 MiB RSS

The idle baseline was therefore roughly:

- klights binary only:      approximately 76 MiB RSS
- klights + runtime cgroup: approximately 151 MiB

The cgroup number includes embedded runtime overhead, not just the klights process.


### This beta currently offers:

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


### Upcoming work includes but is not limited to:

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
