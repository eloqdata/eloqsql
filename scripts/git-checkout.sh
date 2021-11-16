#!/usr/bin/bash
set -eo
TAG=$1

if [ -n "${TAG}" ]; then
  git checkout "${TAG}"
else
  git checkout eloq-10.6.10
  git pull origin eloq-10.6.10
fi
git submodule update --recursive

if [ -n "${TAG}" ]; then
  RAFT_HOST_MGR_HASH=$(awk -F'=' '{ if ($1 == "raft_host_manager") {print $2} }' .private_modules)
  LOG_SERVICE_HASH=$(awk -F'=' '{ if ($1 == "log_service") {print $2} }' .private_modules)
  pushd storage/eloq/tx_service/raft_host_manager
  git checkout ${RAFT_HOST_MGR_HASH}
  popd
  pushd storage/eloq/log_service
  git checkout ${LOG_SERVICE_HASH}
  git submodule update --recursive
  popd
else
  pushd storage/eloq/tx_service/raft_host_manager
  git checkout main
  git pull origin main
  popd
  pushd storage/eloq/log_service
  git checkout main
  git pull origin main
  git submodule update --recursive
  popd
fi
