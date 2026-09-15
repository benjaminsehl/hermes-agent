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
  tests/gateway/test_bluebubbles_quick_ack_integration.py
  tests/gateway/test_prompt_tail_freeze.py
  tests/gateway/test_run_progress_topics.py
  tests/gateway/test_aiohttp_body_caps.py
)
MIN_CONTRACT_TESTS=92

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
  gateway/run_turn.py \
  tests/gateway/test_bluebubbles.py \
  tests/gateway/test_bluebubbles_quick_ack_integration.py
PYTHONPYCACHEPREFIX="${PYTHONPYCACHEPREFIX:-/tmp/hermes-bluebubbles-downstream-pycache}" \
  "$PY" -m compileall -q \
  gateway/platforms/bluebubbles.py \
  gateway/run.py \
  gateway/run_turn.py \
  tests/gateway/test_bluebubbles.py \
  tests/gateway/test_bluebubbles_quick_ack_integration.py

CONTRACT_PATHS=(
  DOWNSTREAM_BLUEBUBBLES.md
  gateway/platforms/bluebubbles.py
  gateway/run.py
  gateway/run_turn.py
  scripts/verify-bluebubbles-downstream.sh
  tests/gateway/test_bluebubbles.py
  tests/gateway/test_bluebubbles_quick_ack_integration.py
  website/docs/user-guide/messaging/bluebubbles.md
)

base_ref=${BASE_REF:-origin/main}
if merge_head=$(git rev-parse --verify MERGE_HEAD 2>/dev/null); then
  # In an uncommitted upstream merge, compare the resolved contract files to
  # the incoming tree. Unrelated upstream whitespace is outside this gate.
  git diff --check "$merge_head" -- "${CONTRACT_PATHS[@]}"
elif git rev-parse --verify "$base_ref" >/dev/null 2>&1; then
  git diff --check "$base_ref...HEAD" -- "${CONTRACT_PATHS[@]}"
  git diff --cached --check -- "${CONTRACT_PATHS[@]}"
  git diff --check -- "${CONTRACT_PATHS[@]}"
else
  echo "Base ref $base_ref unavailable; refusing to skip diff validation" >&2
  exit 1
fi

echo "BlueBubbles downstream behavior gate passed."
