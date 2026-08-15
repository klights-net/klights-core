#!/bin/bash

KLIGHTS_DEFAULT_TMP_ROOT="/tmp/klights"

klights_prepare_tmp_root() {
  local root="${KLIGHTS_TMP_ROOT:-$KLIGHTS_DEFAULT_TMP_ROOT}"
  case "$root" in
    /*) ;;
    *) echo "KLIGHTS_TMP_ROOT must be absolute: $root" >&2; return 2 ;;
  esac
  if [[ "$root" == "/" || "$root" == "/tmp" || -L "$root" ]]; then
    echo "unsafe KLIGHTS_TMP_ROOT: $root" >&2
    return 2
  fi
  if [[ ! -e "$root" ]]; then
    mkdir -m 1777 "$root" || return 2
  fi
  [[ -d "$root" && ! -L "$root" ]] || {
    echo "KLIGHTS_TMP_ROOT is not a real directory: $root" >&2
    return 2
  }
  if [[ "$root" == "$KLIGHTS_DEFAULT_TMP_ROOT" ]]; then
    local mode
    mode="$(stat -c '%a' "$root")" || return 2
    if [[ "$mode" != "1777" ]]; then
      chmod 1777 "$root" 2>/dev/null || true
      mode="$(stat -c '%a' "$root")" || return 2
      [[ "$mode" == "1777" ]] || {
        echo "$root must have shared sticky mode 1777 (found $mode)" >&2
        return 2
      }
    fi
  fi
  [[ -w "$root" && -x "$root" ]] || {
    echo "KLIGHTS_TMP_ROOT is not writable: $root" >&2
    return 2
  }
  export KLIGHTS_TMP_ROOT="$root"
  export TMPDIR="$root"
}

klights_mktemp_dir() {
  klights_prepare_tmp_root || return
  mktemp -d "$KLIGHTS_TMP_ROOT/${1:-klights.XXXXXX}"
}
