#!/bin/bash

KLIGHTS_DEFAULT_TMP_ROOT="/tmp/klights"
KLIGHTS_TMP_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

klights_prepare_tmp_root() {
  local root="${KLIGHTS_TMP_ROOT:-$KLIGHTS_DEFAULT_TMP_ROOT}"
  local uid root_before root_after root_uid root_gid root_mode user_root run_root component
  uid="$(id -u)"
  case "$root" in /*) ;; *) echo "KLIGHTS_TMP_ROOT must be absolute: $root" >&2; return 2;; esac
  if [[ "$root" == / || "$root" == /tmp || -L "$root" || ! -d "$root" ]]; then
    echo "unsafe or unprovisioned KLIGHTS_TMP_ROOT: $root" >&2
    return 2
  fi
  component="$root"
  while [[ "$component" != / ]]; do
    [[ ! -L "$component" ]] || { echo "temporary path contains symlink component: $component" >&2; return 2; }
    component="${component%/*}"
    [[ -n "$component" ]] || component=/
  done
  root_before="$(stat -c '%d:%i' "$root")" || return 2
  root_uid="$(stat -c '%u' "$root")" || return 2
  root_gid="$(stat -c '%g' "$root")" || return 2
  root_mode="$(stat -c '%a' "$root")" || return 2
  if [[ "$root" == "$KLIGHTS_DEFAULT_TMP_ROOT" ]]; then
    if [[ -L /tmp || "$(stat -c '%u' /tmp)" != 0 || "$(stat -c '%a' /tmp)" != 1777 ]]; then
      echo "/tmp must be root-owned mode 1777" >&2
      return 2
    fi
    if [[ "$root_uid" != 0 || "$root_gid" != 0 || "$root_mode" != 1777 ]]; then
      echo "$root must be provisioned root:root mode 1777; run the outer scripts/prepare_tmp_root.sh" >&2
      return 2
    fi
    user_root="$root/user-$uid"
    mkdir -m 700 -- "$user_root" 2>/dev/null || [[ -d "$user_root" ]] || return 2
  else
    user_root="$root"
  fi
  if [[ -L "$user_root" || ! -d "$user_root" || "$(stat -c '%u' "$user_root")" != "$uid" || "$(stat -c '%a' "$user_root")" != 700 ]]; then
    echo "untrusted private temp directory: $user_root" >&2
    return 2
  fi
  if [[ -n "${KLIGHTS_TMP_RUN_DIR:-}" ]]; then
    run_root="$KLIGHTS_TMP_RUN_DIR"
    case "$run_root" in "$user_root"/*) ;; *) echo "KLIGHTS_TMP_RUN_DIR escapes $user_root" >&2; return 2;; esac
  else
    run_root="$(mktemp -d "$user_root/run-XXXXXXXX")" || return 2
  fi
  if [[ -L "$run_root" || ! -d "$run_root" || "$(stat -c '%u' "$run_root")" != "$uid" || "$(stat -c '%a' "$run_root")" != 700 ]]; then
    echo "untrusted invocation temp directory: $run_root" >&2
    return 2
  fi
  root_after="$(stat -c '%d:%i' "$root")" || return 2
  [[ "$root_before" == "$root_after" ]] || { echo "temporary root changed during preparation" >&2; return 2; }
  export KLIGHTS_TMP_ROOT="$root"
  export KLIGHTS_TMP_USER_ROOT="$user_root"
  export KLIGHTS_TMP_RUN_DIR="$run_root"
  export TMPDIR="$run_root"
}

klights_mktemp_dir() {
  klights_prepare_tmp_root || return
  mktemp -d "$KLIGHTS_TMP_RUN_DIR/${1:-klights.XXXXXX}"
}
