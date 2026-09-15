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

GATEWAY_REQUIRED_TESTS=(
  tests/gateway/test_bluebubbles_downstream_contract.py::test_http_errors_never_expose_query_or_userinfo_in_send_result
  tests/gateway/test_bluebubbles_downstream_contract.py::test_http_errors_never_expose_query_or_userinfo_in_logs
  tests/gateway/test_bluebubbles_downstream_contract.py::test_rejected_admission_releases_reservation_and_retry_redispatches
  tests/gateway/test_bluebubbles_downstream_contract.py::test_admission_scheduling_failure_releases_reservation_for_retry
  tests/gateway/test_bluebubbles_downstream_contract.py::test_lookup_failure_is_not_treated_as_an_empty_webhook_list
  tests/gateway/test_bluebubbles_downstream_contract.py::test_webhook_cleanup_fails_when_any_matching_entry_lacks_an_id
  tests/gateway/test_bluebubbles_downstream_contract.py::test_connect_fails_closed_and_releases_registration_lock
  tests/gateway/test_bluebubbles_downstream_contract.py::test_registration_lock_rejection_prevents_server_io
  tests/gateway/test_bluebubbles_downstream_contract.py::test_listener_setup_failure_releases_registration_lock
  tests/gateway/test_bluebubbles_downstream_contract.py::test_same_process_adapters_contend_and_non_owner_cannot_release_registration
  tests/gateway/test_bluebubbles_downstream_contract.py::test_completed_cache_obeys_size_and_ttl
  tests/gateway/test_bluebubbles_downstream_contract.py::test_waiter_and_request_join_limits_are_bounded
  tests/gateway/test_bluebubbles_downstream_contract.py::test_late_enrichment_rollback_allows_retry
  tests/gateway/test_bluebubbles_downstream_contract.py::test_expired_owner_is_taken_over_and_waiters_are_released
  tests/gateway/test_bluebubbles_downstream_contract.py::test_takeover_cancels_the_expired_owner_task
  tests/gateway/test_bluebubbles_downstream_contract.py::test_stale_owner_completion_cannot_complete_replacement
  tests/gateway/test_bluebubbles_downstream_contract.py::test_expired_inflight_entry_cannot_hold_cache_capacity
  tests/gateway/test_bluebubbles_downstream_contract.py::test_attachment_order_survives_reservation
  tests/gateway/test_bluebubbles_downstream_contract.py::test_cancellation_releases_reservation_for_retry
  tests/gateway/test_bluebubbles_downstream_contract.py::test_busy_status_webhook_retry_is_deduplicated_after_positive_admission
  tests/gateway/test_bluebubbles_downstream_contract.py::test_busy_clarification_webhook_retry_is_deduplicated_after_positive_admission
  tests/gateway/test_bluebubbles_downstream_contract.py::test_busy_handler_webhook_retry_is_deduplicated_after_positive_admission
  tests/gateway/test_bluebubbles_downstream_contract.py::test_busy_queue_cap_drop_remains_unaccepted_for_transport_retry
  tests/gateway/test_bluebubbles_downstream_contract.py::test_failed_busy_interrupt_with_full_queue_remains_unaccepted
  tests/gateway/test_bluebubbles_downstream_contract.py::test_quick_ack_generation_and_send_share_one_hard_deadline
  tests/gateway/test_bluebubbles_downstream_contract.py::test_quick_ack_hung_send_cannot_exceed_hard_deadline
  tests/gateway/test_bluebubbles_downstream_contract.py::test_quick_ack_rechecks_ownership_after_chat_lookup_before_post
  tests/gateway/test_bluebubbles_downstream_contract.py::test_late_generation_cannot_emit_a_second_ack
  tests/gateway/test_bluebubbles_downstream_contract.py::test_cancellation_resistant_quick_ack_reports_ambiguity_and_stages_sidecar
  tests/gateway/test_bluebubbles_downstream_contract.py::test_cancellation_resistant_chat_lookup_reports_ambiguous_late_post
  tests/gateway/test_bluebubbles_downstream_contract.py::test_quick_ack_http_timeout_after_post_start_is_ambiguous
  tests/gateway/test_bluebubbles_downstream_contract.py::test_paragraphs_split_in_order_and_partial_timeout_is_not_retried
  tests/gateway/test_bluebubbles_downstream_contract.py::test_superseded_turn_cannot_emit_quick_ack_or_start_agent
  tests/gateway/test_bluebubbles_downstream_contract.py::test_quick_ack_runs_after_final_admission_immediately_before_agent
)
WORKFLOW_REQUIRED_TESTS=(
  tests/ci/test_bluebubbles_sync_workflow.py::test_candidate_verification_has_read_only_contents_and_no_checkout_credentials
  tests/ci/test_bluebubbles_sync_workflow.py::test_only_minimal_publish_job_can_write_contents_and_it_uses_verified_artifact
)
SUPPORTING_TEST_FILES=(
  tests/gateway/test_bluebubbles.py
  tests/gateway/test_bluebubbles_quick_ack_integration.py
  tests/gateway/test_prompt_tail_freeze.py
  tests/gateway/test_run_progress_topics.py
  tests/gateway/test_aiohttp_body_caps.py
)

CONTRACT_HOME=$(mktemp -d "${TMPDIR:-/tmp}/hermes-bluebubbles-contract.XXXXXX")
trap 'rm -rf "$CONTRACT_HOME"' EXIT

run_required_file() {
  env -i \
    PATH="$PATH" \
    HOME="$HOME" \
    TZ=UTC \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    PYTHONHASHSEED=0 \
    PYTHONUTF8=1 \
    HERMES_HOME="$CONTRACT_HOME" \
    "$PY" -m pytest "$@" -o addopts=
}

# Exact node IDs make removal or rename of any required invariant a collection error.
# Each file gets a fresh interpreter, matching the canonical runner's isolation boundary.
run_required_file "${GATEWAY_REQUIRED_TESTS[@]}"
run_required_file "${WORKFLOW_REQUIRED_TESTS[@]}"

# Broad upstream suites provide supporting coverage without masquerading as named invariants.
scripts/run_tests.sh "${SUPPORTING_TEST_FILES[@]}"
"$PY" -m ruff check \
  gateway/platforms/base.py \
  gateway/platforms/bluebubbles.py \
  gateway/run.py \
  gateway/run_busy.py \
  gateway/run_inbound.py \
  gateway/run_turn.py \
  tests/gateway/test_bluebubbles.py \
  tests/gateway/test_bluebubbles_quick_ack_integration.py \
  tests/gateway/test_bluebubbles_downstream_contract.py \
  tests/ci/test_bluebubbles_sync_workflow.py
PYTHONPYCACHEPREFIX="${PYTHONPYCACHEPREFIX:-/tmp/hermes-bluebubbles-downstream-pycache}" \
  "$PY" -m compileall -q \
  gateway/platforms/base.py \
  gateway/platforms/bluebubbles.py \
  gateway/run.py \
  gateway/run_busy.py \
  gateway/run_inbound.py \
  gateway/run_turn.py \
  tests/gateway/test_bluebubbles.py \
  tests/gateway/test_bluebubbles_quick_ack_integration.py \
  tests/gateway/test_bluebubbles_downstream_contract.py \
  tests/ci/test_bluebubbles_sync_workflow.py

CONTRACT_PATHS=(
  .github/workflows/sync-upstream-bluebubbles.yml
  DOWNSTREAM_BLUEBUBBLES.md
  gateway/platforms/base.py
  gateway/platforms/bluebubbles.py
  gateway/run.py
  gateway/run_busy.py
  gateway/run_inbound.py
  gateway/run_turn.py
  scripts/verify-bluebubbles-downstream.sh
  tests/ci/test_bluebubbles_sync_workflow.py
  tests/gateway/test_bluebubbles.py
  tests/gateway/test_bluebubbles_downstream_contract.py
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
