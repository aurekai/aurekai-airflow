#!/usr/bin/env bash
set -euo pipefail
DAGS_DIR="$(cd "$(dirname "$0")/../dags" && pwd)"
PASS=0; FAIL=0
for f in "$DAGS_DIR"/*.py; do
  if python3 -m py_compile "$f" 2>/dev/null; then
    echo "  v $(basename "$f")"; PASS=$((PASS+1))
  else
    echo "  x $(basename "$f")"; FAIL=$((FAIL+1))
  fi
done
echo; echo "DAGs: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
