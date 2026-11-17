#!/usr/bin/env bash
# Build and optionally install raft-computer for one exact source commit.
# Usage: mac-computer-replace.sh <full-commit-sha> [--apply]
#        mac-computer-replace.sh --rollback <backup-dir> --apply
set -Eeuo pipefail

usage() {
  printf 'Usage: %s <full-commit-sha> [--apply]\n' "${0##*/}"
  printf '       %s --rollback <backup-dir> --apply\n' "${0##*/}"
}

if [ "$#" -eq 1 ] && { [ "$1" = "-h" ] || [ "$1" = "--help" ]; }; then
  usage
  exit 0
fi

MODE="replace"
APPLY=0
ROLLBACK_DIR=""
TARGET_SHA=""
if [ "${1:-}" = "--rollback" ]; then
  [ "$#" -eq 3 ] && [ "$3" = "--apply" ] || { usage >&2; exit 2; }
  MODE="rollback"
  ROLLBACK_DIR="$2"
  APPLY=1
else
  if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
    usage >&2
    exit 2
  fi
  TARGET_SHA="$1"
  if [ "$#" -eq 2 ]; then
    [ "$2" = "--apply" ] || { usage >&2; exit 2; }
    APPLY=1
  fi
  if [[ ! "$TARGET_SHA" =~ ^[0-9a-f]{40}$ ]]; then
    printf 'Abort: target must be a full 40-character lowercase commit SHA.\n' >&2
    exit 2
  fi
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
BUILD_REPO="$(cd "$SCRIPT_DIR/../../.." && pwd -P)"
NODE_DIR="$HOME/raft-selfhost/node-v24.21.0-darwin-arm64"
NODE_VERSION="24.21.0"
NODE="$NODE_DIR/bin/node"
INSTALL_ROOT="$HOME/.local"
BIN="$INSTALL_ROOT/bin/raft-computer"
WASM="$INSTALL_ROOT/bin/photon_rs_bg.wasm"
BACKUP_ROOT="$HOME/raft-backups"
LABEL="com.leo.raft.computer"
JOB="gui/$(id -u)/$LABEL"
WORKTREE="$HOME/my-projects/raft-build-$TARGET_SHA"
CAND_BIN="$WORKTREE/packages/computer/dist-native/raft-computer-darwin-arm64"
CAND_WASM="$WORKTREE/packages/computer/dist-native/photon_rs_bg.wasm"
RAFT_HOME="$HOME/raft-home-leo-lab"
BOOT_LAYERS="$HOME/raft-selfhost/boot-layers.sh"
export RAFT_HOME SLOCK_HOME="$RAFT_HOME"

say() { printf '\n== %s ==\n' "$*"; }
die() { printf '\nAbort: %s\n' "$*" >&2; exit 1; }
sha256() { shasum -a 256 "$1" | awk '{print $1}'; }
service_running() {
  "$BIN" status 2>/dev/null | grep -E '^Service:[[:space:]]+running' >/dev/null
}
require_apply_context() {
  [ -t 0 ] && [ -t 1 ] || die "--apply requires a local interactive terminal"
  [ -z "${SLOCK_CURRENT_AGENT_ID:-}${SLOCK_AGENT_ID:-}${PI_CODING_AGENT:-}" ] \
    || die "agent session environment detected; run from your own terminal"
  [ -z "${SSH_CONNECTION:-}" ] || die "SSH_CONNECTION is set; run on the Mac console"
  [ "$(id -un)" = "leo" ] || die "run --apply as leo"
  launchctl print "$JOB" >/dev/null 2>&1 || die "cannot access launchd job $JOB"
}

if [ "$MODE" = "rollback" ]; then
  say "Manual rollback from a previously created backup"
  [ "$(uname -s)" = "Darwin" ] || die "rollback must run on macOS"
  [ "$(uname -m)" = "arm64" ] || die "the approved Mac target is Apple Silicon arm64"
  require_apply_context
  [ -x "$BOOT_LAYERS" ] || die "identity checker not found: $BOOT_LAYERS"
  [ -d "$INSTALL_ROOT/bin" ] || die "install directory not found: $INSTALL_ROOT/bin"
  [ -d "$ROLLBACK_DIR" ] || die "backup directory not found: $ROLLBACK_DIR"
  shopt -s nullglob
  BACKUP_BINARIES=("$ROLLBACK_DIR"/raft-computer-*)
  [ "${#BACKUP_BINARIES[@]}" -eq 1 ] || die "backup directory must contain exactly one raft-computer-* binary"
  BACKUP_BIN="${BACKUP_BINARIES[0]}"
  [ -f "$BACKUP_BIN" ] || die "backup binary not found: $BACKUP_BIN"
  BACKUP_SHA="$(sha256 "$BACKUP_BIN")"
  printf 'Restore: %s\nSHA-256: %s\n' "$BACKUP_BIN" "$BACKUP_SHA"
  printf 'Type YES to restore this binary and restart the local Computer daemon: '
  IFS= read -r CONFIRM
  [ "$CONFIRM" = "YES" ] || die "confirmation was not YES; nothing was installed"

  ROLLBACK_TMP="$(mktemp "$INSTALL_ROOT/bin/.raft-computer-rollback.XXXXXX")" \
    || die "could not create rollback temporary file"
  if ! cp -p "$BACKUP_BIN" "$ROLLBACK_TMP" || ! chmod 755 "$ROLLBACK_TMP" || ! mv -f "$ROLLBACK_TMP" "$BIN"; then
    rm -f "$ROLLBACK_TMP"
    die "could not atomically restore the backed-up Computer binary"
  fi
  ROLLBACK_TMP=""
  if [ -f "$ROLLBACK_DIR/photon_rs_bg.wasm" ]; then
    WASM_TMP="$(mktemp "$INSTALL_ROOT/bin/.photon-rollback.XXXXXX")" \
      || die "could not create wasm rollback temporary file"
    if ! cp -p "$ROLLBACK_DIR/photon_rs_bg.wasm" "$WASM_TMP" || ! mv -f "$WASM_TMP" "$WASM"; then
      rm -f "$WASM_TMP"
      die "could not atomically restore the backed-up wasm sidecar"
    fi
  fi
  [ "$(sha256 "$BIN")" = "$BACKUP_SHA" ] || die "restored binary SHA-256 does not match its backup"
  launchctl kickstart -k "$JOB" || die "launchd rollback restart failed"
  sleep 35
  if ! service_running; then
    launchctl kickstart -k "$JOB" || die "launchd rollback retry failed"
    sleep 20
  fi
  service_running || die "Computer daemon did not recover after rollback"
  "$BOOT_LAYERS" --identity-for-home "$RAFT_HOME" || die "Raft home identity check failed after rollback"
  say "Manual rollback complete"
  printf 'Restored SHA-256: %s\n' "$BACKUP_SHA"
  exit 0
fi
say "Read-only host and Node preflight"
[ "$(uname -s)" = "Darwin" ] || die "this replacement script must run on macOS"
[ "$(uname -m)" = "arm64" ] || die "the approved Mac build target is Apple Silicon arm64"
[ -x "$NODE" ] || die "pinned Node executable not found: $NODE"
NODE_ACTUAL="$("$NODE" --version)"
[ "$NODE_ACTUAL" = "v$NODE_VERSION" ] || die "expected Node v$NODE_VERSION at $NODE; found $NODE_ACTUAL"
[ -d "$BUILD_REPO/packages/computer" ] || die "script is not inside a raft-source checkout: $BUILD_REPO"
[ -f "$BUILD_REPO/pnpm-lock.yaml" ] || die "pnpm lockfile not found in $BUILD_REPO"
printf 'Host: %s %s (%s)\nNode: %s\nBuild repository: %s\n' \
  "$(uname -s)" "$(sw_vers -productVersion)" "$(uname -m)" "$NODE_ACTUAL" "$BUILD_REPO"

say "Check target commit is merged into origin/main"
git -C "$BUILD_REPO" fetch --no-tags origin main || die "could not fetch origin/main"
MAIN_SHA="$(git -C "$BUILD_REPO" rev-parse --verify 'origin/main^{commit}')" || die "origin/main is unavailable"
git -C "$BUILD_REPO" cat-file -e "$TARGET_SHA^{commit}" 2>/dev/null || die "target commit is not present in this checkout"
RESOLVED_SHA="$(git -C "$BUILD_REPO" rev-parse --verify "$TARGET_SHA^{commit}")" || die "target is not a commit"
[ "$RESOLVED_SHA" = "$TARGET_SHA" ] || die "target SHA did not resolve to the exact requested commit"
if ! git -C "$BUILD_REPO" merge-base --is-ancestor "$TARGET_SHA" "$MAIN_SHA"; then
  die "target $TARGET_SHA is not merged into origin/main ($MAIN_SHA)"
fi
printf 'Target: %s\norigin/main: %s\n' "$TARGET_SHA" "$MAIN_SHA"

say "Prepare a clean detached build worktree"
mkdir -p "$HOME/my-projects"
if [ -e "$WORKTREE" ]; then
  [ "$(git -C "$WORKTREE" rev-parse --show-toplevel 2>/dev/null || true)" = "$WORKTREE" ] \
    || die "build path exists but is not the expected worktree: $WORKTREE"
  [ "$(git -C "$WORKTREE" rev-parse HEAD)" = "$TARGET_SHA" ] \
    || die "existing build worktree is not at target $TARGET_SHA"
  WORKTREE_STATUS="$(git -C "$WORKTREE" status --porcelain --untracked-files=all)" \
    || die "could not read existing build worktree status"
  [ -z "$WORKTREE_STATUS" ] || die "existing build worktree is dirty: $WORKTREE"
else
  git -C "$BUILD_REPO" worktree add --detach "$WORKTREE" "$TARGET_SHA" \
    || die "could not create detached build worktree"
fi
[ "$(git -C "$WORKTREE" rev-parse HEAD)" = "$TARGET_SHA" ] \
  || die "build worktree HEAD does not match the requested commit"
WORKTREE_STATUS="$(git -C "$WORKTREE" status --porcelain --untracked-files=all)" \
  || die "could not read build worktree status"
[ -z "$WORKTREE_STATUS" ] || die "build worktree is dirty before build"

say "Install locked dependencies and build Computer SEA"
(
  cd "$WORKTREE"
  PATH="$NODE_DIR/bin:$PATH" pnpm install --frozen-lockfile
  cd "$WORKTREE/packages/computer"
  PATH="$NODE_DIR/bin:$PATH" pnpm build:native --node-version "$NODE_VERSION" --commit-sha "$TARGET_SHA"
) || die "dependency installation or native build failed"
[ -x "$CAND_BIN" ] || die "native build did not produce $CAND_BIN"

say "Validate candidate build provenance"
if ! BUILD_VERSIONS="$("$CAND_BIN" __build-versions 2>&1)"; then
  die "candidate __build-versions failed: $BUILD_VERSIONS"
fi
printf '%s\n' "$BUILD_VERSIONS"
if ! "$NODE" - "$BUILD_VERSIONS" "$TARGET_SHA" <<'NODE'
const [raw, expectedSha] = process.argv.slice(2);
let metadata;
try {
  metadata = JSON.parse(raw);
} catch (error) {
  console.error(`could not parse __build-versions JSON: ${error.message}`);
  process.exit(1);
}
if (metadata === null || typeof metadata !== "object" || Array.isArray(metadata)) {
  console.error("__build-versions did not return a JSON object");
  process.exit(1);
}
if (typeof metadata.commitSha !== "string" || !/^[0-9a-f]{40}$/.test(metadata.commitSha)) {
  console.error(`metadata commitSha is not a full commit SHA: ${JSON.stringify(metadata.commitSha)}`);
  process.exit(1);
}
if (metadata.commitSha !== expectedSha) {
  console.error(`metadata commitSha ${metadata.commitSha} does not match requested ${expectedSha}`);
  process.exit(1);
}
if (metadata.sourceDirty !== false) {
  console.error(`metadata sourceDirty must be false; got ${JSON.stringify(metadata.sourceDirty)}`);
  process.exit(1);
}
console.log(`Verified commitSha=${metadata.commitSha}, sourceDirty=false`);
NODE
then
  die "candidate provenance check failed; no install path was touched"
fi

CAND_SHA="$(sha256 "$CAND_BIN")"
printf 'Candidate binary SHA-256: %s\n' "$CAND_SHA"
if [ "$APPLY" -eq 0 ]; then
  say "Dry run complete"
  printf 'Validated candidate: %s\n' "$CAND_BIN"
  printf 'To install after reviewing the candidate, rerun with: %s %s --apply\n' "$0" "$TARGET_SHA"
  exit 0
fi

say "Check Mac daemon and interactive-session safety"
require_apply_context
[ -x "$BOOT_LAYERS" ] || die "identity checker not found: $BOOT_LAYERS"
[ -x "$BIN" ] || die "installed Computer binary not found: $BIN"
service_running || die "Computer daemon is not running; investigate before replacement"
CUR_SHA="$(sha256 "$BIN")"
printf 'Installed binary: %s\nInstalled SHA-256: %s\n' "$BIN" "$CUR_SHA"
printf '\nCurrent agent runners (the daemon restart will interrupt these sessions):\n'
"$BIN" runners list || die "could not list current runners"
printf '\nThe next step will replace raft-computer and restart the local Computer daemon.\n'
printf 'Type YES to continue: '
IFS= read -r CONFIRM
[ "$CONFIRM" = "YES" ] || die "confirmation was not YES; nothing was installed"

say "Back up current install"
TS="$(date -u +%Y%m%dT%H%M%SZ)"
(umask 077; mkdir -p "$BACKUP_ROOT") || die "could not create backup root $BACKUP_ROOT"
BACKUP_DIR="$(umask 077; mktemp -d "$BACKUP_ROOT/mac-$TARGET_SHA-$TS.XXXXXX")" \
  || die "could not create a unique backup directory"
cp -p "$BIN" "$BACKUP_DIR/raft-computer-$CUR_SHA" || die "could not back up installed Computer binary"
[ "$(sha256 "$BACKUP_DIR/raft-computer-$CUR_SHA")" = "$CUR_SHA" ] \
  || die "installed binary backup checksum failed"
WASM_REPLACE=0
if [ -f "$CAND_WASM" ] && [ -f "$WASM" ] && [ "$(sha256 "$CAND_WASM")" != "$(sha256 "$WASM")" ]; then
  cp -p "$WASM" "$BACKUP_DIR/photon_rs_bg.wasm" || die "could not back up installed wasm sidecar"
  WASM_REPLACE=1
fi
printf '%s  %s\n' "$CUR_SHA" "raft-computer-$CUR_SHA" > "$BACKUP_DIR/SHA256SUMS"
[ "$WASM_REPLACE" -eq 0 ] || shasum -a 256 "$BACKUP_DIR/photon_rs_bg.wasm" >> "$BACKUP_DIR/SHA256SUMS"
printf 'Backup directory: %s\n' "$BACKUP_DIR"

TMP_BIN=""
TMP_WASM=""
ROLLBACK_TMP=""
REPLACEMENT_STARTED=0
WASM_REPLACEMENT_STARTED=0
rollback_install() {
  local failed=0
  printf '\nReplacement failed; restoring the backed-up installation.\n' >&2
  ROLLBACK_TMP="$(mktemp "$INSTALL_ROOT/bin/.raft-computer-rollback.XXXXXX")" || return 1
  cp -p "$BACKUP_DIR/raft-computer-$CUR_SHA" "$ROLLBACK_TMP" \
    && chmod 755 "$ROLLBACK_TMP" \
    && mv -f "$ROLLBACK_TMP" "$BIN" || failed=1
  [ -z "$ROLLBACK_TMP" ] || rm -f "$ROLLBACK_TMP"
  ROLLBACK_TMP=""
  if [ "$WASM_REPLACEMENT_STARTED" -eq 1 ]; then
    ROLLBACK_TMP="$(mktemp "$INSTALL_ROOT/bin/.photon-rollback.XXXXXX")" || failed=1
    if [ "$failed" -eq 0 ]; then
      cp -p "$BACKUP_DIR/photon_rs_bg.wasm" "$ROLLBACK_TMP" \
        && mv -f "$ROLLBACK_TMP" "$WASM" || failed=1
    fi
    [ -z "$ROLLBACK_TMP" ] || rm -f "$ROLLBACK_TMP"
    ROLLBACK_TMP=""
  fi
  [ "$failed" -eq 0 ] && [ "$(sha256 "$BIN")" = "$CUR_SHA" ] || failed=1
  if [ "$failed" -eq 0 ]; then
    launchctl kickstart -k "$JOB" || failed=1
    sleep 35
    if ! service_running; then
      launchctl kickstart -k "$JOB" || failed=1
      sleep 20
    fi
    service_running || failed=1
    "$BOOT_LAYERS" --identity-for-home "$RAFT_HOME" >/dev/null || failed=1
  fi
  if [ "$failed" -eq 0 ]; then
    printf 'Rollback restored the prior binary and daemon. Backup: %s\n' "$BACKUP_DIR" >&2
  else
    printf 'Rollback needs attention. Prior binary backup: %s/raft-computer-%s\n' "$BACKUP_DIR" "$CUR_SHA" >&2
  fi
  return "$failed"
}
on_exit() {
  local status=$?
  trap - EXIT
  if [ "$status" -ne 0 ] && [ "$REPLACEMENT_STARTED" -eq 1 ]; then
    set +e
    rollback_install
    local rollback_status=$?
    if [ "$rollback_status" -ne 0 ]; then
      printf 'Automatic rollback did not fully verify. Restore from %s and inspect launchd job %s.\n' "$BACKUP_DIR" "$JOB" >&2
    fi
  fi
  [ -z "$TMP_BIN" ] || rm -f "$TMP_BIN"
  [ -z "$TMP_WASM" ] || rm -f "$TMP_WASM"
  [ -z "$ROLLBACK_TMP" ] || rm -f "$ROLLBACK_TMP"
  exit "$status"
}
trap on_exit EXIT

say "Atomically replace the Computer binary"
TMP_BIN="$(mktemp "$INSTALL_ROOT/bin/.raft-computer.XXXXXX")" || die "could not create an install-side temporary file"
cp "$CAND_BIN" "$TMP_BIN" && chmod 755 "$TMP_BIN" || die "could not stage candidate binary"
[ "$(sha256 "$TMP_BIN")" = "$CAND_SHA" ] || die "staged binary checksum differs from candidate"
[ "$(sha256 "$BIN")" = "$CUR_SHA" ] || die "installed binary changed after backup"
REPLACEMENT_STARTED=1
mv -f "$TMP_BIN" "$BIN" || die "atomic binary replacement failed"
TMP_BIN=""
[ "$(sha256 "$BIN")" = "$CAND_SHA" ] || die "installed binary checksum differs from candidate"
if [ "$WASM_REPLACE" -eq 1 ]; then
  TMP_WASM="$(mktemp "$INSTALL_ROOT/bin/.photon.XXXXXX")" || die "could not create wasm temporary file"
  cp "$CAND_WASM" "$TMP_WASM" || die "could not stage candidate wasm"
  WASM_REPLACEMENT_STARTED=1
  mv -f "$TMP_WASM" "$WASM" || die "atomic wasm replacement failed"
  TMP_WASM=""
  [ "$(sha256 "$WASM")" = "$(sha256 "$CAND_WASM")" ] || die "installed wasm checksum differs from candidate"
fi

say "Restart and verify the local Computer daemon"
launchctl kickstart -k "$JOB" || die "launchd restart failed"
sleep 35
if ! service_running; then
  printf 'Service did not report running; retrying launchd once for known pidfile contention.\n' >&2
  launchctl kickstart -k "$JOB" || die "launchd retry failed"
  sleep 20
fi
service_running || die "Computer daemon did not recover; automatic rollback will run"
"$BOOT_LAYERS" --identity-for-home "$RAFT_HOME" \
  || die "Raft home identity check failed after replacement; automatic rollback will run"
[ "$(sha256 "$BIN")" = "$CAND_SHA" ] || die "installed binary changed during daemon restart"
REPLACEMENT_STARTED=0
say "Replacement complete"
printf 'Installed commit: %s\nInstalled SHA-256: %s\nBackup: %s\n' "$TARGET_SHA" "$CAND_SHA" "$BACKUP_DIR"
printf 'Interactive rollback: %s --rollback %q --apply\n' "$0" "$BACKUP_DIR"
printf 'If that helper is unavailable, the backup binary is: %q\n' "$BACKUP_DIR/raft-computer-$CUR_SHA"
printf 'If a later issue requires manual recovery, restore the backup and restart launchd:\n'
printf 'tmp=$(mktemp %q)\n' "$INSTALL_ROOT/bin/.raft-computer-rollback.XXXXXX"
printf 'cp -p %q "$tmp" && chmod 755 "$tmp" && mv -f "$tmp" %q && launchctl kickstart -k %q\n' \
  "$BACKUP_DIR/raft-computer-$CUR_SHA" "$BIN" "$JOB"
if [ "$WASM_REPLACE" -eq 1 ]; then
  printf 'For the wasm sidecar, copy %q to %q when restoring.\n' "$BACKUP_DIR/photon_rs_bg.wasm" "$WASM"
fi
