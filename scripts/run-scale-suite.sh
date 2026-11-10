#!/usr/bin/env bash
# Runs the Relay scalability suite (loadtest/SUITE.md) on Linux/macOS/WSL.
#   scripts/run-scale-suite.sh [--tests t1,t3] [--scale full|smoke] [--workers N] [--out DIR] [--data DIR]
# All arguments are passed to the runner. Needs Node >= 22; Docker only for t4.
set -euo pipefail
cd "$(dirname "$0")/.."

major=$(node --version 2>/dev/null | sed 's/^v//; s/\..*//' || true)
if [ -z "${major:-}" ] || [ "$major" -lt 22 ]; then
  echo "Node >= 22 is required (found: ${major:-none})" >&2
  exit 1
fi
if ! docker info >/dev/null 2>&1; then
  echo "WARNING: docker is not running; t4 (real databases) will be skipped" >&2
fi
[ -d node_modules ] || npm ci --no-audit --no-fund

out="${PWD}/loadtest/results/suite-$(date +%Y%m%d-%H%M)"
args=("$@")
for ((i = 0; i < ${#args[@]}; i++)); do
  if [ "${args[i]}" = "--out" ] && [ $((i + 1)) -lt ${#args[@]} ]; then out="${args[i + 1]}"; fi
done
case " $* " in *" --out "*) ;; *) args+=(--out "$out") ;; esac
mkdir -p "$out"
start=$(date +%s)
set +e
npm run suite --workspace loadtest -- "${args[@]}" 2>&1 | tee -a "${out}/suite-log.txt"
code=${PIPESTATUS[0]}
echo "Total time: $(( $(date +%s) - start )) s; results in ${out}"
exit "$code"
