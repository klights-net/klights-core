#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<EOF_USAGE
Usage: $(basename "$0") --binary PATH --version VERSION --suite SUITE --distro DISTRO --arch ARCH --out DIR

Create a Debian package for klights.

Options:
  --binary PATH     path to klights binary to install
  --version VERSION version string for package metadata
  --suite SUITE     target Debian suite (e.g. stable)
  --distro DISTRO   Debian-like distro label used in release suffix
  --arch ARCH       Debian architecture (amd64 or arm64)
  --out DIR         output directory for generated .deb
EOF_USAGE
  exit 1
}

BINARY=""
VERSION=""
SUITE=""
DISTRO=""
ARCH=""
OUT_DIR=""

while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --binary)
      if [[ "$#" -lt 2 ]]; then
        echo "--binary requires PATH" >&2
        usage
      fi
      BINARY=$2
      shift 2
      ;;
    --version)
      if [[ "$#" -lt 2 ]]; then
        echo "--version requires VERSION" >&2
        usage
      fi
      VERSION=$2
      shift 2
      ;;
    --suite)
      if [[ "$#" -lt 2 ]]; then
        echo "--suite requires SUITE" >&2
        usage
      fi
      SUITE=$2
      shift 2
      ;;
    --distro)
      if [[ "$#" -lt 2 ]]; then
        echo "--distro requires DISTRO" >&2
        usage
      fi
      DISTRO=$2
      shift 2
      ;;
    --arch)
      if [[ "$#" -lt 2 ]]; then
        echo "--arch requires ARCH" >&2
        usage
      fi
      ARCH=$2
      shift 2
      ;;
    --out)
      if [[ "$#" -lt 2 ]]; then
        echo "--out requires DIR" >&2
        usage
      fi
      OUT_DIR=$2
      shift 2
      ;;
    --help|-h)
      usage
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      ;;
  esac
done

if [[ -z "$BINARY" || -z "$VERSION" || -z "$SUITE" || -z "$DISTRO" || -z "$ARCH" || -z "$OUT_DIR" ]]; then
  echo "Missing required argument." >&2
  usage
fi

if [[ "$ARCH" != "amd64" && "$ARCH" != "arm64" ]]; then
  echo "--arch must be amd64 or arm64" >&2
  exit 1
fi

if ! command -v dpkg-deb >/dev/null 2>&1; then
  echo "dpkg-deb not found. Install dpkg on this host." >&2
  exit 1
fi

if [[ ! -f "$BINARY" ]]; then
  echo "Binary not found: $BINARY" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SERVICE_SRC="$REPO_ROOT/packaging/systemd/klights.service"
DEFAULT_SRC="$REPO_ROOT/packaging/default/klights"

mkdir -p "$OUT_DIR"
TMPDIR="$(mktemp -d -t klights-deb-XXXXXXXX)"
cleanup() {
  rm -rf "$TMPDIR"
}
trap cleanup EXIT

STAGING="$TMPDIR/pkg"
mkdir -p "$STAGING/DEBIAN" "$STAGING/usr/bin"

install -m 0755 "$BINARY" "$STAGING/usr/bin/klights"

if [[ -f "$SERVICE_SRC" ]]; then
  mkdir -p "$STAGING/lib/systemd/system"
  install -m 0644 "$SERVICE_SRC" "$STAGING/lib/systemd/system/klights.service"
fi

if [[ -f "$DEFAULT_SRC" ]]; then
  mkdir -p "$STAGING/etc/default"
  install -m 0644 "$DEFAULT_SRC" "$STAGING/etc/default/klights"
  printf '/etc/default/klights\n' > "$STAGING/DEBIAN/conffiles"
fi

cat > "$STAGING/DEBIAN/control" <<EOF_CTRL
Package: klights
Version: ${VERSION}-1~${SUITE}
Section: admin
Priority: optional
Architecture: ${ARCH}
Maintainer: klights maintainers <maintainers@klights.net>
Depends: containerd, iproute2, kmod, libmnl0, libnftnl11, nftables
Description: Lightweight Kubernetes runtime
 Klights packages an async, minimal Kubernetes-compatible control plane and
 node runtime stack for local clusters.
EOF_CTRL

cat > "$STAGING/DEBIAN/postinst" <<'EOF_POST'
#!/bin/sh
set -e
if command -v systemctl >/dev/null 2>&1; then
  systemctl daemon-reload || true
  systemctl enable klights.service || true
fi
exit 0
EOF_POST

cat > "$STAGING/DEBIAN/prerm" <<'EOF_PRERM'
#!/bin/sh
set -e
if [ "$1" = "remove" ] && command -v systemctl >/dev/null 2>&1; then
  systemctl stop klights.service || true
  systemctl disable klights.service || true
  systemctl daemon-reload || true
fi
exit 0
EOF_PRERM

chmod 0755 "$STAGING/DEBIAN/postinst" "$STAGING/DEBIAN/prerm"

OUTPUT="$OUT_DIR/klights_${VERSION}-1~${SUITE}_${ARCH}.deb"
dpkg-deb --build "$STAGING" "$OUTPUT"

echo "$OUTPUT"
