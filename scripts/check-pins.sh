#!/usr/bin/env bash
# Fails when any test output holds a pinned-virtual-thread trace: the JVM
# prints one under -Djdk.tracePinnedThreads=short when a virtual thread parks while it holds a
# monitor or sits in a native frame. Arguments: directories to search (default: the test reports
# and the integration tests' process logs).
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
dirs=("$@")
if [[ ${#dirs[@]} -eq 0 ]]; then
  dirs=("$ROOT/target/surefire-reports" "$ROOT/target/failsafe-reports" "$ROOT/target/it-logs")
fi
# JDK 21's trace: the carrier thread, then frames; the pinning frame ends "<== monitors:N".
pattern='VThreadContinuation\.onPinned|<== monitors:|^VirtualThread\[#[0-9]+\].* reason:'
found=0
for d in "${dirs[@]}"; do
  [[ -d "$d" ]] || continue
  files=$(grep -rlE --include='*.txt' --include='*.log' "$pattern" "$d" || true)
  for f in $files; do
    echo "pinned virtual thread in: $f"
    grep -nE -B2 -A8 "$pattern" "$f" | head -40
    found=1
  done
done
if [[ $found -ne 0 ]]; then
  echo "check-pins: virtual threads were pinned: use ReentrantLock, never synchronized" >&2
  exit 1
fi
echo "check-pins: no pinned virtual thread in ${dirs[*]}"
