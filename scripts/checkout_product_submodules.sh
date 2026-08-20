#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)
DATA_SUBSTRATE_DIR=data_substrate

cd "${REPO_ROOT}"

git submodule sync

# Initialize top-level product source dependencies without recursively pulling
# Data Substrate's third_party/src workspace. If a developer is testing a
# different Data Substrate checkout, preserve it.
if [ "${ELOQ_FORCE_DATA_SUBSTRATE_UPDATE:-0}" = "1" ] || \
   git diff --quiet -- "${DATA_SUBSTRATE_DIR}"; then
  git submodule update --init
else
  mapfile -t PRODUCT_SUBMODULES < <(
    git config -f .gitmodules --get-regexp '\.path$' |
      awk -v data_substrate="${DATA_SUBSTRATE_DIR}" '$2 != data_substrate { print $2 }'
  )
  if [ "${#PRODUCT_SUBMODULES[@]}" -gt 0 ]; then
    git submodule update --init -- "${PRODUCT_SUBMODULES[@]}"
  fi
  echo "Keeping locally checked-out ${DATA_SUBSTRATE_DIR}; parent gitlink has uncommitted changes."
fi

# tx-log-protos, eloq_log_service, and raft_host_manager are bundled in-tree by
# Data Substrate. EloqStore and its source dependencies remain product
# submodules; third_party/src is fetched by the third-party installer.
git -C "${DATA_SUBSTRATE_DIR}" submodule sync
git -C "${DATA_SUBSTRATE_DIR}" submodule update --init \
  store_handler/eloq_data_store_service/eloqstore

ELOQSTORE_DIR=${DATA_SUBSTRATE_DIR}/store_handler/eloq_data_store_service/eloqstore
if [ -f "${ELOQSTORE_DIR}/.gitmodules" ]; then
  git -C "${ELOQSTORE_DIR}" submodule sync
  git -C "${ELOQSTORE_DIR}" submodule update --init \
    external/concurrentqueue \
    external/inih \
    external/abseil
fi
