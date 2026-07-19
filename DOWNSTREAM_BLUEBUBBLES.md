# BlueBubbles downstream release channel

This fork's `main` branch is Ben's tested Hermes release channel. It contains the current Nous Research `main` branch plus the BlueBubbles/iMessage hardening that is not yet upstream.

## Safety invariant

The live gateway must never be updated to a revision that silently drops the BlueBubbles behavioral contract.

Upstream changes reach fork `main` only through `.github/workflows/sync-upstream-bluebubbles.yml`. The workflow builds a candidate merge, runs `scripts/verify-bluebubbles-downstream.sh`, and advances `main` only if every check passes. Merge conflicts, dependency failures, contract regressions, lint failures, compilation failures, or diff errors leave `main` unchanged and create or update a GitHub issue.

## Remotes on the live installation

```text
origin   https://github.com/benjaminsehl/hermes-agent.git
upstream https://github.com/NousResearch/hermes-agent.git
```

The live checkout tracks `origin/main`, so ordinary `hermes update` consumes only tested downstream releases.

## Contract gate

Run locally from a Hermes checkout with development dependencies installed:

```bash
BASE_REF=upstream/main scripts/verify-bluebubbles-downstream.sh
```

The gate requires at least 192 tests across the BlueBubbles and related gateway suites, then runs those tests plus Ruff, compileall, and `git diff --check`.

The minimum test-count guard is intentional. An upstream merge must not make the gate green merely by deleting or no longer collecting downstream regressions.

## Scheduled evolution

The sync workflow runs every six hours and can be dispatched manually. A manual dispatch revalidates current `main` even when it already contains the latest upstream commit.

When the workflow is blocked:

1. Leave fork `main` and the live gateway on the last known-good revision.
2. Create a worktree from fork `main`.
3. Merge `upstream/main` into a candidate branch.
4. Port the implementation to the new upstream architecture; do not resolve conflicts by blindly choosing either side.
5. Run the contract gate, broader relevant tests, and independent review.
6. Push the repaired candidate and manually dispatch the sync workflow.
7. Run `hermes update` only after fork `main` advances successfully.

When upstream absorbs part of the implementation, remove the downstream code only after the same behavioral tests pass against upstream's replacement.

## Runtime verification after update

After a local update or rollback, verify:

- the checkout is clean and tracks `origin/main`;
- the focused contract gate passes;
- the gateway uses `HERMES_HOME=~/.hermes-imessage`;
- `http://127.0.0.1:8645/health` returns `ok`;
- BlueBubbles has exactly one equivalent Hermes webhook subscribed to `new-message` and `updated-message`;
- a duplicate-webhook canary starts one agent run and emits one acknowledgment plus one final response.

## Recovery

The pre-migration fork branch is retained as:

```text
legacy/main-pre-bluebubbles-20260719
```

The portable patch, history, and additional operating notes are maintained separately in:

```text
~/sai/apps/hermes-imessage
```

To recover the current tested downstream release, reset the live checkout to `origin/main`, reinstall dependencies if needed, run the contract gate, and restart the isolated launchd gateway only after verification succeeds.
