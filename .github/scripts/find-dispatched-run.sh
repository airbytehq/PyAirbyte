#!/usr/bin/env bash
# Locate the workflow run started by a `repository_dispatch` we just sent.
# The dispatch API returns nothing, so poll the target workflow for the
# newest `repository_dispatch` run created since this workflow run began.
#
# Inputs (env):
#   GH_TOKEN          token with actions:read on TARGET_REPO
#   TARGET_REPO       e.g. airbytehq/airbyte-ops-mcp
#   TARGET_WORKFLOW   workflow file name in TARGET_REPO
#   RUN_STARTED_AT    ISO-8601 lower bound (github.run_started_at)
#   EXCLUDE_RUN_URL   optional: a run already claimed by an earlier dispatch
#
# Output: `run_url` (falls back to the target workflow's run list).
set -euo pipefail

fallback_url="https://github.com/${TARGET_REPO}/actions/workflows/${TARGET_WORKFLOW}?query=event%3Arepository_dispatch"

find_run() {
  gh api --method GET "repos/${TARGET_REPO}/actions/workflows/${TARGET_WORKFLOW}/runs" \
      -f event=repository_dispatch \
      -f "created=>=${RUN_STARTED_AT}" \
      -F per_page=20 \
    | jq -r --arg exclude "${EXCLUDE_RUN_URL:-}" \
      '[.workflow_runs[] | select(.html_url != $exclude)] | sort_by(.created_at) | last | .html_url // empty'
}

run_url=""
for attempt in $(seq 1 12); do
  run_url="$(find_run)"
  [ -z "$run_url" ] || break
  echo "No new repository_dispatch run in ${TARGET_REPO} yet (attempt $attempt/12)."
  sleep 5
done

[ -n "$run_url" ] \
  || echo "::warning::Could not find the dispatched run in ${TARGET_REPO}; linking the workflow run list instead."

printf 'run_url=%s\n' "${run_url:-$fallback_url}" | tee -a "$GITHUB_OUTPUT"
