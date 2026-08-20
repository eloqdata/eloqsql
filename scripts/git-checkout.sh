#!/usr/bin/bash
set -eo

if [ -n "$1" ]; then
  TAG=$1
else
  TAG="main"
fi

git checkout "${TAG}"
if [ "${TAG}" = "main" ]; then
  git pull origin main
fi

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ELOQ_FORCE_DATA_SUBSTRATE_UPDATE=1 \
  bash "${SCRIPT_DIR}/checkout_product_submodules.sh"
