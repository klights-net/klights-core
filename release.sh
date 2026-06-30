#!/bin/bash
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$REPO_ROOT"
export CARGO_INCREMENTAL=1
unset RUSTC_WRAPPER
export CARGO_HOME="${HOME}/.cargo"

usage() {
  cat <<'EOF_USAGE'
Usage: ./release.sh [--dynamic|--static]

Build modes:
  --static    Build a statically linked release binary for
              x86_64-unknown-linux-gnu and verify it has no ELF NEEDED entries.
              This is the default.
  --dynamic   Build a dynamically linked release binary. This matches plain
              `cargo build --release`.

Static builds require static archives for native dependencies, including
libnftnl, libmnl, zlib, OpenSSL, and SQLite.
EOF_USAGE
}

openssl_static_link_args() {
  if ! command -v pkg-config >/dev/null 2>&1; then
    return 0
  fi

  pkg-config --libs --static openssl 2>/dev/null | tr ' ' '\n' | while IFS= read -r arg; do
    case "$arg" in
      ""|-lssl|-lcrypto)
        ;;
      -l:libjitterentropy.a)
        printf ' -C link-arg=-l:libjitterentropy.a'
        ;;
      -l*|-L*|-Wl,*|-pthread)
        printf ' -C link-arg=%s' "$arg"
        ;;
    esac
  done
}

LINK_MODE="${KLIGHTS_RELEASE_LINK_MODE:-static}"
while [ "$#" -gt 0 ]; do
  case "$1" in
    --dynamic)
      LINK_MODE="dynamic"
      ;;
    --static)
      LINK_MODE="static"
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "ERROR: unknown release option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

if [ "$(id -u)" -eq 0 ]; then
  echo "ERROR: Do not run ./release.sh as root. Run as regular user." >&2
  echo "       Use sudo only for deploy and test scripts." >&2
  exit 1
fi

case "$LINK_MODE" in
  dynamic)
    echo "[release] building dynamically linked release binary"
    cargo build --release
    BINARY_PATH="$REPO_ROOT/target/release/klights"
    ;;
  static)
    STATIC_TARGET="${KLIGHTS_STATIC_TARGET:-x86_64-unknown-linux-gnu}"
    RUSTFLAGS_ENV="CARGO_TARGET_${STATIC_TARGET^^}_RUSTFLAGS"
    RUSTFLAGS_ENV="${RUSTFLAGS_ENV//-/_}"
    EXISTING_RUSTFLAGS="${!RUSTFLAGS_ENV:-}"

    export PKG_CONFIG_ALL_STATIC=1
    export OPENSSL_STATIC=1
    export LIBSQLITE3_SYS_STATIC=1
    export LIBZ_SYS_STATIC=1
    OPENSSL_EXTRA_LINK_ARGS="$(openssl_static_link_args)"
    export "$RUSTFLAGS_ENV=${EXISTING_RUSTFLAGS:+$EXISTING_RUSTFLAGS }-C target-feature=+crt-static${OPENSSL_EXTRA_LINK_ARGS}"

    echo "[release] building statically linked release binary for $STATIC_TARGET"
    cargo build --release --target "$STATIC_TARGET"
    BINARY_PATH="$REPO_ROOT/target/$STATIC_TARGET/release/klights"

    if ! command -v readelf >/dev/null 2>&1; then
      echo "ERROR: readelf is required to verify static release binaries" >&2
      exit 1
    fi
    if readelf -d "$BINARY_PATH" | grep -q 'NEEDED'; then
      echo "ERROR: static release binary still has shared-library dependencies:" >&2
      readelf -d "$BINARY_PATH" | grep 'NEEDED' >&2
      exit 1
    fi
    ;;
  *)
    echo "ERROR: invalid release link mode: $LINK_MODE" >&2
    exit 2
    ;;
esac

COMPAT_PATH="$REPO_ROOT/target/release/klights"
if [ "$BINARY_PATH" != "$COMPAT_PATH" ]; then
  mkdir -p "$REPO_ROOT/target/release"
  ln -sfn "$BINARY_PATH" "$COMPAT_PATH"
fi

echo "[release] done: binary at ${BINARY_PATH#$REPO_ROOT/}"
echo "[release] compatibility path at target/release/klights"
