#!/usr/bin/bash
set -eo

TAG=$1
REL_BRANCH="rel_${TAG//./_}_eloqsql"

set +e
git fetch origin '+refs/heads/*:refs/remotes/origin/*'
set -e

# Ensure we're on main branch first (checkout remote main if local doesn't exist)
if git show-ref --verify --quiet refs/heads/main; then
  git checkout main
else
  git checkout -b main origin/main
fi

# Validate release branch does not already exist (local or remote), then create from main
if git show-ref --verify --quiet "refs/heads/$REL_BRANCH" || \
   git ls-remote --heads origin "$REL_BRANCH" | grep -q "$REL_BRANCH"; then
  echo "Error: release branch $REL_BRANCH already exists (local or remote)" >&2
  exit 1
fi
git checkout -b "$REL_BRANCH" main
git push origin "$REL_BRANCH"
