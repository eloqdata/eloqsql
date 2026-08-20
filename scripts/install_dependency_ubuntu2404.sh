#!/usr/bin/env bash
set -euo pipefail
set -x

# Install the build dependencies for a from-scratch Ubuntu 24.04 build. Source-
# built dependencies are fetched, pinned, built, and installed by Data
# Substrate's shared third-party workspace instead of being installed globally.

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)
THIRD_PARTY_INSTALLER="${REPO_ROOT}/data_substrate/scripts/third_party/install-ubuntu2404.sh"
THIRD_PARTY_PREFIX="${ELOQ_THIRD_PARTY_PREFIX:-${REPO_ROOT}/data_substrate/third_party/install}"

if [ ! -x "${THIRD_PARTY_INSTALLER}" ]; then
  echo "Missing ${THIRD_PARTY_INSTALLER}. Initialize product submodules first:" >&2
  echo "  bash scripts/checkout_product_submodules.sh" >&2
  exit 1
fi

export DEBIAN_FRONTEND=noninteractive
export TZ=${TZ:-UTC}

# Some development images expose different versions through g++ and c++. Make
# every Data Substrate build path use the same default compiler that CMake will
# select later for EloqSQL, while still honoring an explicitly chosen toolchain.
export CC=${CC:-cc}
export CXX=${CXX:-c++}

run_privileged() {
  if [ "$(id -u)" -eq 0 ]; then
    "$@"
  elif command -v sudo >/dev/null 2>&1; then
    sudo "$@"
  else
    echo "This script must run as root or with sudo available: $*" >&2
    exit 1
  fi
}

run_with_retry() {
  local attempt
  for attempt in 1 2 3; do
    if "$@"; then
      return 0
    fi
    if [ "${attempt}" -lt 3 ]; then
      sleep $((attempt * 5))
    fi
  done
  return 1
}

needs_tz_config=false
if [ ! -f /etc/timezone ] || ! grep -qE '^(Etc/UTC|UTC)$' /etc/timezone; then
  needs_tz_config=true
fi
if [ ! -L /etc/localtime ] || [ "$(readlink -f /etc/localtime)" != "/usr/share/zoneinfo/Etc/UTC" ]; then
  needs_tz_config=true
fi

if ${needs_tz_config}; then
  echo 'tzdata tzdata/Areas select Etc' | run_privileged debconf-set-selections || true
  echo 'tzdata tzdata/Zones/Etc select UTC' | run_privileged debconf-set-selections || true
  echo 'Etc/UTC' | run_privileged tee /etc/timezone >/dev/null
  run_privileged ln -sf /usr/share/zoneinfo/Etc/UTC /etc/localtime
fi

if ! command -v curl >/dev/null; then
  run_privileged apt-get update || true
  run_privileged apt-get install -y curl
fi

run_privileged apt-get update
run_privileged apt-get install -y --no-install-recommends \
  jq sudo vim wget curl apt-utils python3 python3-dev python3-pip python3-venv \
  gdb libcurl4-openssl-dev build-essential libncurses5-dev \
  gnutls-dev bison zlib1g-dev ccache rsync cmake ninja-build libuv1-dev git \
  g++ make openjdk-11-jdk openssh-client openssh-server libssl-dev libgflags-dev \
  libleveldb-dev libsnappy-dev openssl libbz2-dev liblz4-dev libzstd-dev \
  libboost-context-dev ca-certificates libc-ares-dev libc-ares2 lcov m4 pkg-config \
  tar gcc redis tcl libreadline-dev ncurses-dev patchelf libprotobuf-dev util-linux \
  protobuf-compiler libjsoncpp-dev

# Preserve the Google Cloud command-line tooling used by the GCS integration
# tests. It is not part of the native dependency prefix.
if ! command -v gcloud >/dev/null 2>&1; then
  run_privileged apt-get install -y apt-transport-https ca-certificates gnupg
  echo "deb https://packages.cloud.google.com/apt cloud-sdk main" | \
    run_privileged tee /etc/apt/sources.list.d/google-cloud-sdk.list >/dev/null
  curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | \
    run_privileged apt-key add -
  run_privileged apt-get update
  run_privileged apt-get install -y google-cloud-cli
fi

# Keep EloqSQL's Python test tooling separate from the C/C++ dependency
# workspace. This mirrors EloqKV: native source dependencies belong to Data
# Substrate, while Python packages live in a virtual environment.
if [ "${ELOQ_SKIP_PYTHON_DEPS:-0}" != "1" ]; then
  if ! command -v uv >/dev/null 2>&1; then
    curl -LsSf https://astral.sh/uv/install.sh | sh
  fi
  export PATH="${HOME}/.local/bin:${PATH}"
  uv venv --python /usr/bin/python3 "${HOME}/venv"
  run_with_retry uv pip install --python "${HOME}/venv/bin/python" --no-cache-dir \
    setuptools==45.2.0 \
    cassandra-driver==3.29.2 \
    awscli==1.29.44 \
    boto3==1.28.36 \
    botocore==1.31.44 \
    mysql-connector-python==8.1.0 \
    psutil==5.9.5 \
    grpcio==1.60.0 \
    grpcio-tools==1.60.0
fi

# CI may restore an already-built prefix and set ELOQ_SKIP_THIRD_PARTY=1.
if [ "${ELOQ_SKIP_THIRD_PARTY:-0}" != "1" ]; then
  # Data Substrate currently sizes its builds with nproc. Restrict the child
  # process to the first eight CPUs in our existing affinity set so nproc -- and
  # every compiler it launches -- can never consume more than eight cores.
  BUILD_CPU_LIST=$(python3 -c \
    'import os; print(",".join(map(str, sorted(os.sched_getaffinity(0))[:8])))')
  if [ -z "${BUILD_CPU_LIST}" ]; then
    echo "Unable to determine CPUs available for the dependency build" >&2
    exit 1
  fi

  # host_manager adds yaml-cpp from source at product configure time. Retain
  # manifest-fetched sources so that step remains offline and inside the shared
  # workspace. Callers may explicitly set this to 0 to recover disk space.
  ELOQ_THIRD_PARTY_KEEP_SOURCES="${ELOQ_THIRD_PARTY_KEEP_SOURCES:-1}" \
    taskset --cpu-list "${BUILD_CPU_LIST}" "${THIRD_PARTY_INSTALLER}"
elif [ ! -f "${THIRD_PARTY_PREFIX}/share/eloq/third-party-manifest.yml" ]; then
  echo "ELOQ_SKIP_THIRD_PARTY=1, but no completed workspace was found at ${THIRD_PARTY_PREFIX}" >&2
  exit 1
fi
