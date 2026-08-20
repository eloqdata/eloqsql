#!/bin/bash
set -exo pipefail

# This script builds EloqSQL tarball with open_log_service enabled, using Debug build on Ubuntu 24.04
# It follows the build configuration outlined in README.md, with packaging similar to build_tarball.bash

export WORKSPACE=$PWD
export AWS_PAGER=""
export DEBIAN_FRONTEND=noninteractive
export TZ=UTC

apt-get update && apt-get install -y sudo openssh-client


current_user=$(whoami)
sudo chown -R "$current_user" "$PWD"

# Setup SSH for private submodules if provided
if [ -n "${GIT_SSH_KEY}" ]; then
  mkdir -p ~/.ssh
  echo "${GIT_SSH_KEY}" > ~/.ssh/id_rsa
  chmod 600 ~/.ssh/id_rsa
  ssh-keyscan github.com >> ~/.ssh/known_hosts 2>/dev/null || true
fi

# Defaults aligned with README.md
: "${BUILD_TYPE:=Debug}"
: "${ASAN:=OFF}"
: "${DATA_STORE_TYPE:=ELOQDSS_ROCKSDB_CLOUD_S3}"
: "${NCORE:=8}"
if ! [[ "${NCORE}" =~ ^[1-9][0-9]*$ ]]; then
  echo "NCORE must be a positive integer" >&2
  exit 1
fi
if [ "${NCORE}" -gt 8 ]; then
  NCORE=8
fi

DEST_DIR="${HOME}/EloqSQL"
OUT_NAME="${OUT_NAME:-debug-openlog}"

# Prepare workspace layout expected by scripts
cd "$HOME"
ln -sfn "${WORKSPACE}/eloqsql_src" eloqsql

ELOQSQL_SRC="${HOME}/eloqsql"

# Product source submodules must be initialized before the Data Substrate
# workspace installer can be invoked.
cd "$ELOQSQL_SRC"
bash scripts/checkout_product_submodules.sh

# Install all dependencies using Ubuntu 24.04 script
source /etc/os-release
if [[ "$ID" == ubuntu* ]]; then
  echo "Installing dependencies for Ubuntu ${VERSION_ID}..."
  cd "$ELOQSQL_SRC"
  bash scripts/install_dependency_ubuntu2404.sh
  
  # Activate the Python virtual environment created by the dependency script
  source $HOME/venv/bin/activate
fi

export ELOQ_THIRD_PARTY_PREFIX="${ELOQSQL_SRC}/data_substrate/third_party/install"
export CMAKE_PREFIX_PATH="${ELOQ_THIRD_PARTY_PREFIX}${CMAKE_PREFIX_PATH:+:${CMAKE_PREFIX_PATH}}"
export LD_LIBRARY_PATH="${ELOQ_THIRD_PARTY_PREFIX}/lib:${ELOQ_THIRD_PARTY_PREFIX}/lib64${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"

# Build EloqSQL per README with OPEN_LOG_SERVICE enabled
mkdir build
cd build

cmake -DCMAKE_INSTALL_PREFIX="${DEST_DIR}" \
      -DCMAKE_BUILD_TYPE="${BUILD_TYPE}" \
      -DWITH_READLINE=1 \
      -DPLUGIN_HANDLERSOCKET=NO \
      -DPLUGIN_ROCKSDB=NO \
      -DPLUGIN_ARIA=NO \
      -DPLUGIN_ARCHIVE=NO \
      -DPLUGIN_CVS=NO \
      -DPLUGIN_FEDERATEDX=NO \
      -DPLUGIN_TOKUDB=NO \
      -DPLUGIN_MROONGA=NO \
      -DPLUGIN_OQGRAPH=NO \
      -DPLUGIN_CONNECT=NO \
      -DPLUGIN_SPIDER=NO \
      -DPLUGIN_SPHINX=NO \
      -DPLUGIN_HEAP=NO \
      -DPLUGIN_MYISAMMRG=NO \
      -DPLUGIN_SEQUENCE=NO \
      -DINSTALL_MYSQLTESTDIR= \
      -DMYSQL_MAINTAINER_MODE=OFF \
      -DWITH_SSL=system \
      -DCOROUTINE_ENABLED=ON \
      -DBRPC_WITH_GLOG=ON \
      -DMARIA_WITH_GLOG=ON \
      -DWITH_ASAN="${ASAN}" \
      -DELOQ_THIRD_PARTY_PREFIX="${ELOQSQL_SRC}/data_substrate/third_party/install" \
      -DELOQ_THIRD_PARTY_REQUIRED=ON \
      -DCMAKE_C_FLAGS_DEBUG="-O0 -g -DDBUG_ON -fno-omit-frame-pointer -fno-strict-aliasing" \
      -DCMAKE_CXX_FLAGS_DEBUG="-O0 -g -DDBUG_ON -fno-omit-frame-pointer -fno-strict-aliasing -felide-constructors -Wno-error" \
      -DWITH_DATA_STORE="${DATA_STORE_TYPE}" \
      -DOPEN_LOG_SERVICE=ON \
      ../

cmake --build . --config "${BUILD_TYPE}" -j"${NCORE}"
cmake --install . --config "${BUILD_TYPE}"

# Helper to copy dependent libraries for portability
copy_libraries() {
  local executable="$1"
  local path="$2"
  libraries=$(ldd "$executable" | awk 'NF==4{print $(NF-1)}{}')
  mkdir -p "$path"
  for lib in $libraries; do
    rsync -avL --ignore-existing "$lib" "$path/"
  done
}

# Ensure runtime libs packaged
mkdir -p "${DEST_DIR}/lib" "${DEST_DIR}/bin" "${DEST_DIR}/conf"
copy_libraries "${DEST_DIR}/bin/mariadbd" "${DEST_DIR}/lib"
copy_libraries "${DEST_DIR}/bin/mariadb" "${DEST_DIR}/lib"

# Build and include dss_server component
pushd "${ELOQSQL_SRC}/data_substrate/store_handler/eloq_data_store_service"
rm -rf bld
mkdir bld && cd bld
cmake -DCMAKE_BUILD_TYPE="${BUILD_TYPE}" -DWITH_DATA_STORE="${DATA_STORE_TYPE}" ../
cmake --build . --config "${BUILD_TYPE}" -j"${NCORE}"
copy_libraries dss_server "${DEST_DIR}/lib"
mv dss_server "${DEST_DIR}/bin/"
popd

# Build and include log_service (launch_sv)
pushd "${ELOQSQL_SRC}/data_substrate/eloq_log_service"
rm -rf bld
mkdir bld && cd bld
cmake -DCMAKE_BUILD_TYPE="${BUILD_TYPE}" ../
cmake --build . --config "${BUILD_TYPE}" -j"${NCORE}"
copy_libraries launch_sv "${DEST_DIR}/lib"
mv launch_sv "${DEST_DIR}/bin/"
popd

# Create tarball
cd "${HOME}"
tar -czvf eloqsql.tar.gz -C "${HOME}" EloqSQL

# Cleanup build directories
cd "${ELOQSQL_SRC}"
rm -rf build
rm -rf data_substrate/store_handler/eloq_data_store_service/bld
rm -rf data_substrate/eloq_log_service/bld

echo "Build completed. Tarball created at: ${HOME}/eloqsql.tar.gz"
