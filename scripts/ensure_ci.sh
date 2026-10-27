#!/usr/bin/env bash
# Usage: ensure_ci.sh <branch>. Waits up to 90 s for a ci.yml run on the head of
# the branch's pull request and dispatches one (full matrix) when none appears.
set -euo pipefail
sha=$(gh pr view "$1" --json headRefOid -q .headRefOid)
for _ in $(seq 18); do
  if [ -n "$(gh run list -w ci.yml -c "$sha" --json event -q '.[].event')" ]; then
    exit 0
  fi
  sleep 5
done
echo "No CI run for $sha: dispatching ci.yml on $1."
gh workflow run ci.yml --ref "$1" -f full=true
