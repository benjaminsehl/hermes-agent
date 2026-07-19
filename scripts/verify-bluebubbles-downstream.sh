#!/usr/bin/env bash
# Fail-closed contract gate for Ben's maintained BlueBubbles downstream.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

if [[ -n "${PYTHON:-}" ]]; then
  PY="$PYTHON"
elif [[ -x .venv/bin/python ]]; then
  PY=.venv/bin/python
elif [[ -x venv/bin/python ]]; then
  PY=venv/bin/python
else
  PY=python3
fi

TEST_FILES=(
  tests/gateway/test_bluebubbles.py
  tests/gateway/test_prompt_tail_freeze.py
  tests/gateway/test_run_progress_topics.py
  tests/gateway/test_aiohttp_body_caps.py
)
MIN_CONTRACT_TESTS=192

collect_output=$(
  "$PY" -m pytest "${TEST_FILES[@]}" --collect-only -q -o addopts=
)
printf '%s\n' "$collect_output"
collected=$(printf '%s\n' "$collect_output" | "$PY" -c '
import re, sys
text = sys.stdin.read()
matches = re.findall(r"(?:collected\s+(\d+)\s+items?|(?:^|\n)(\d+)\s+tests?\s+collected)", text)
if not matches:
    raise SystemExit("could not determine collected contract-test count")
print(next(value for pair in matches[-1:] for value in pair if value))
')
if (( collected < MIN_CONTRACT_TESTS )); then
  echo "BlueBubbles contract shrank: collected $collected, require >= $MIN_CONTRACT_TESTS" >&2
  exit 1
fi

"$PY" -m pytest "${TEST_FILES[@]}" -o addopts=
"$PY" -m ruff check \
  gateway/platforms/bluebubbles.py \
  gateway/run.py \
  tests/gateway/test_bluebubbles.py
PYTHONPYCACHEPREFIX="${PYTHONPYCACHEPREFIX:-/tmp/hermes-bluebubbles-downstream-pycache}" \
  "$PY" -m compileall -q \
  gateway/platforms/bluebubbles.py \
  gateway/run.py \
  tests/gateway/test_bluebubbles.py

base_ref=${BASE_REF:-origin/main}
if git rev-parse --verify "$base_ref" >/dev/null 2>&1; then
  git diff --check "$base_ref...HEAD"
else
  echo "Base ref $base_ref unavailable; refusing to skip diff validation" >&2
  exit 1
fi

echo "BlueBubbles downstream gate passed ($collected contract tests)."
