#!/usr/bin/env bash
#
# ci.sh — every check this repo gates on, runnable on a laptop.
#
# Mirrors `.woodpecker.yml` (Codeberg CI), which has never been enabled: Codeberg
# gates CI behind a manual approval request that was never made. This script is
# the answer to that — the checks run here, on demand, with no external service
# in the loop. If Woodpecker is ever switched on, its steps should call this
# script rather than restate the commands, so the two cannot drift.
#
#   scripts/ci.sh          # run everything
#   scripts/ci.sh --rust   # Rust checks only
#   scripts/ci.sh --dart   # Dart package only
#
# Exit status is 0 only if every BLOCKING check passed. Unlike the sibling
# datascope repo, clippy here is BLOCKING (`-D warnings`) — that is this repo's
# standing policy, per CONTRIBUTING.md and AGENTS.md.
#
# Deliberately loud about SKIPS. A check that could not run is reported as a
# skip, never folded into a pass.
set -uo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"
cd "${repo_root}"

# These two need Docker (testcontainers), so CI excludes them. This script runs
# them when a usable Docker is present, which makes it strictly stronger than
# the pipeline it mirrors.
DOCKER_CRATES=(-p allex-postgres -p allex-identity-postgres)
EXCLUDES=(--exclude allex-postgres --exclude allex-identity-postgres)

run_rust=1
run_dart=1
case "${1:-}" in
  --rust) run_dart=0 ;;
  --dart) run_rust=0 ;;
  "")     ;;
  *) echo "usage: $0 [--rust|--dart]" >&2; exit 2 ;;
esac

results=()
failed=0

record() {
  results+=("$2|$1")
  [[ "$2" == FAIL ]] && failed=1
  return 0
}

banner() { printf '\n\033[1m=== %s ===\033[0m\n' "$1"; }

# --- toolchain ----------------------------------------------------------------
# The pinned CI toolchain equals the declared MSRV, so the pipeline doubles as
# the MSRV gate. Building on a newer rustc can therefore accept code that CI
# would reject — worth knowing before it is someone else's merge failure.
banner "toolchain"
msrv="$(grep -m1 'rust-version' Cargo.toml | sed 's/.*"\(.*\)".*/\1/')"
rustc_version="$(rustc --version | awk '{print $2}')"
echo "  rustc ${rustc_version}, declared MSRV ${msrv}"
if [[ "${rustc_version}" == "${msrv}"* ]]; then
  echo "  matches the MSRV — this run doubles as the MSRV gate"
else
  echo "  NOTE: not the MSRV toolchain. This run does NOT prove ${msrv} still"
  echo "        builds. For that: rustup toolchain install ${msrv} && \\"
  echo "          cargo +${msrv} check --workspace --all-targets"
fi

# --- Rust ---------------------------------------------------------------------
if (( run_rust )); then
  banner "fmt"
  if cargo fmt --all --check; then
    echo "  formatting clean"; record "fmt" PASS
  else
    echo "  run: cargo fmt --all"; record "fmt" FAIL
  fi

  banner "clippy (blocking: -D warnings)"
  if cargo clippy --workspace --all-targets -- -D warnings; then
    echo "  no warnings"; record "clippy" PASS
  else
    record "clippy" FAIL
  fi

  banner "test (excluding the Docker-bound crates)"
  if cargo test --workspace "${EXCLUDES[@]}"; then
    record "test" PASS
  else
    record "test" FAIL
  fi

  banner "test: postgres integration (Docker)"
  if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
    echo "  Docker is usable — running what CI cannot."
    if cargo test "${DOCKER_CRATES[@]}"; then
      record "test:postgres" PASS
    else
      record "test:postgres" FAIL
    fi
  else
    echo "  SKIPPED — no usable Docker daemon; testcontainers cannot start."
    echo "  These crates are excluded from CI too, so nothing verifies them"
    echo "  unless you run this with Docker up."
    record "test:postgres" SKIP
  fi
fi

# --- Dart ---------------------------------------------------------------------
# The Dart package is NOT covered by .woodpecker.yml, yet the datascope Flutter
# GUI path-depends on it (flutter/pubspec.yaml -> ../../allex/dart/allex). A
# break here surfaces as a mysterious failure in the other repo, so it is
# checked in this script even though the pipeline never did.
if (( run_dart )); then
  dart_dir="${repo_root}/dart/allex"
  if [[ ! -d "${dart_dir}" ]]; then
    banner "dart"
    echo "  SKIPPED — ${dart_dir} is missing."
    record "dart:analyze" SKIP
    record "dart:test" SKIP
  elif ! command -v dart >/dev/null 2>&1; then
    banner "dart"
    echo "  SKIPPED — no dart on PATH (the Flutter SDK bundles one)."
    record "dart:analyze" SKIP
    record "dart:test" SKIP
  else
    echo
    echo "  $(dart --version 2>&1)"

    banner "dart: pub get"
    if ! (cd "${dart_dir}" && dart pub get >/dev/null); then
      echo "  could not resolve dependencies"
      record "dart:analyze" FAIL
      record "dart:test" FAIL
    else
      banner "dart:analyze"
      if (cd "${dart_dir}" && dart analyze); then
        record "dart:analyze" PASS
      else
        record "dart:analyze" FAIL
      fi

      banner "dart:test"
      if (cd "${dart_dir}" && dart test); then
        record "dart:test" PASS
      else
        record "dart:test" FAIL
      fi
    fi
  fi
fi

# --- summary ------------------------------------------------------------------
banner "summary"
for entry in "${results[@]}"; do
  status="${entry%%|*}"
  name="${entry#*|}"
  case "${status}" in
    PASS) printf '  \033[32m%-5s\033[0m %s\n' "PASS" "${name}" ;;
    FAIL) printf '  \033[31m%-5s\033[0m %s\n' "FAIL" "${name}" ;;
    SKIP) printf '  \033[33m%-5s\033[0m %s — NOT verified\n' "SKIP" "${name}" ;;
  esac
done

echo
if (( failed )); then
  echo "BLOCKING CHECKS FAILED"
  exit 1
fi
if printf '%s\n' "${results[@]}" | grep -q '^SKIP|'; then
  echo "All blocking checks passed, but something was SKIPPED — see above."
  echo "A skip is not a pass."
  exit 0
fi
echo "All checks passed."
