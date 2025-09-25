#!/bin/bash
set -exo pipefail

export WORKSPACE=$PWD
export AWS_PAGER=""

# Get current user and ensure proper ownership
current_user=$(whoami)
sudo chown -R $current_user $PWD

# Setup SSH for accessing private repos/submodules if provided
if [ -n "${GIT_SSH_KEY}" ]; then
  mkdir -p ~/.ssh
  echo "${GIT_SSH_KEY}" > ~/.ssh/id_rsa
  chmod 600 ~/.ssh/id_rsa
  ssh-keyscan github.com >> ~/.ssh/known_hosts 2>/dev/null || true
fi

# Ensure workspace ownership
sudo chown -R $current_user $HOME/workspace 2>/dev/null || true

cd $HOME
ln -s ${WORKSPACE}/eloqsql_src eloqsql
cd eloqsql
ln -s $WORKSPACE/logservice_src storage/eloq/eloq_log_service
pushd storage/eloq/tx_service
ln -s $WORKSPACE/raft_host_manager_src raft_host_manager
popd
ELOQSQL_SRC=${PWD}

export LD_LIBRARY_PATH=/usr/local/lib:/usr/local/lib64:$LD_LIBRARY_PATH

# Get OS information from /etc/os-release
source /etc/os-release
if [[ "$ID" == "centos" ]] || [[ "$ID" == "rocky" ]]; then
    OS_ID="rhel${VERSION_ID%.*}"
else
    OS_ID="${ID}${VERSION_ID%.*}"
fi
if [[ "$OS_ID" == rhel* ]]; then
    case "$VERSION_ID" in
    7*)
        sudo yum update -y
        sudo yum install rsync -y
        source /opt/rh/devtoolset-11/enable
        g++ --version
        INSTALL_PSQL="sudo yum install -y https://download.postgresql.org/pub/repos/yum/reporpms/EL-7-x86_64/pgdg-redhat-repo-latest.noarch.rpm && sudo yum install -y postgresql14"
        ;;
    8*)
        sudo dnf update -y
        sudo dnf install rsync -y
        source scl_source enable gcc-toolset-11
        g++ --version
        INSTALL_PSQL="sudo dnf install -y postgresql"
        ;;
    9*)
        sudo dnf update -y
        sudo dnf install rsync -y
        INSTALL_PSQL="sudo dnf install -y postgresql"
        # detected dubious ownership
        git config --global --add safe.directory ${WORKSPACE}/eloqsql_src
        git config --global --add safe.directory ${WORKSPACE}/logservice_src
        git config --global --add safe.directory ${WORKSPACE}/raft_host_manager_src
        ;;
    esac
elif [[ "$OS_ID" == ubuntu* ]]; then
    sudo apt update -y
    sudo apt install rsync -y
    INSTALL_PSQL="DEBIAN_FRONTEND=noninteractive sudo apt install -y postgresql-client"
fi

case $(uname -m) in
amd64 | x86_64) ARCH=amd64 ;;
arm64 | aarch64) ARCH=arm64 ;;
*) ARCH=$(uname -m) ;;
esac

if [ -n "${TAGGED}" ]; then
    TAGGED=$(git tag --sort=-v:refname | head -n 1)
    if [ -z "${TAGGED}" ]; then
        exit 1
    fi
    scripts/git-checkout.sh "${TAGGED}"
fi

copy_libraries() {
    local executable="$1"
    local path="$2"
    libraries=$(ldd "$executable" | awk 'NF==4{print $(NF-1)}{}')
    mkdir -p "$path"
    for lib in $libraries; do
        rsync -avL --ignore-existing "$lib" "$path/"
    done
}

S3_BUCKET="eloq-release"
S3_PREFIX="s3://${S3_BUCKET}/eloqsql"
DATA_STORE_ID=$(echo ${DATA_STORE_TYPE} | tr '[:upper:]' '[:lower:]')

# Normalize behavior for supported DATA_STORE_TYPE values
if [ "${DATA_STORE_TYPE}" = "ELOQDSS_ROCKSDB_CLOUD_S3" ]; then
    CMAKE_ARGS="${CMAKE_ARGS} -DWITH_LOG_STATE=ROCKSDB_CLOUD_S3 -DWITH_CLOUD_AZ_INFO=ON"
    DATA_STORE_ID="rocks_s3"
elif [ "${DATA_STORE_TYPE}" = "ELOQDSS_ROCKSDB_CLOUD_GCS" ]; then
    CMAKE_ARGS="${CMAKE_ARGS} -DWITH_LOG_STATE=ROCKSDB_CLOUD_GCS"
    DATA_STORE_ID="rocks_gcs"
elif [ "${DATA_STORE_TYPE}" = "ELOQDSS_ROCKSDB" ]; then
    DATA_STORE_ID="eloqdss_rocksdb"
elif [ "${DATA_STORE_TYPE}" = "ELOQDSS_ELOQSTORE" ]; then
    DATA_STORE_ID="eloqdss_eloqstore"
else
    echo "Unsupported DATA_STORE_TYPE: ${DATA_STORE_TYPE}"
    exit 1
fi

if [ "$ID" == "centos" ];then
    IOURING_ENABLED="OFF"
else
    IOURING_ENABLED="ON"
fi

if [ "$ASAN" = "ON" ]; then
    export ASAN_OPTIONS=abort_on_error=1:detect_container_overflow=0:leak_check_at_exit=0
fi

# init destination directory
DEST_DIR="${HOME}/EloqSQL"
mkdir ${DEST_DIR}
mkdir ${DEST_DIR}/bin
mkdir ${DEST_DIR}/lib
mkdir ${DEST_DIR}/conf

# Define the license content for tarball
LICENSE_CONTENT=$(
    cat <<EOF
License

Copyright (c) 2024 EloqData

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to use,
copy, modify, and distribute the Software, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE, AND NONINFRINGEMENT. IN NO EVENT SHALL ELOQDATA
OR ITS CONTRIBUTORS BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT, OR OTHERWISE, ARISING FROM, OUT OF, OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

IMPORTANT: By using this software, you acknowledge that EloqData shall not be
liable for any loss or damage, including but not limited to loss of data, arising
from the use of the software. The responsibility for backing up any data, checking
the software's appropriateness for your needs, and using it within the bounds of
the law lies entirely with you.
EOF
)

# Write the license content to LICENSE.txt in the destination directory
echo "$LICENSE_CONTENT" >"${DEST_DIR}/LICENSE.txt"

# build eloqsql
cd $ELOQSQL_SRC
git submodule sync
git submodule update --init --recursive

# Init and sync submodules in required locations
cd storage/eloq/eloq_log_service
git submodule sync
git submodule update --init --recursive
cd $ELOQSQL_SRC

if [ ! -d "bld" ]; then mkdir bld; fi
cd bld

cmake -DCMAKE_INSTALL_PREFIX="${DEST_DIR}" \
      -DPLUGIN_{HANDLERSOCKET,ROCKSDB,ARIA,ARCHIVE,CVS,FEDERATEDX,TOKUDB,MROONGA,OQGRAPH,CONNECT,SPIDER,SPHINX,HEAP,MYISAMMRG}=NO \
      -DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
      -DINSTALL_MYSQLTESTDIR="" \
      -DCMAKE_CXX_FLAGS_DEBUG="${CMAKE_CXX_FLAGS_DEBUG} -Wno-error" \
      -DMYSQL_MAINTAINER_MODE=OFF \
      -DWITH_SSL=system \
      -DWITH_ASAN=${ASAN} \
      -DSMALL_RANGE=ON \
      -DCOROUTINE_ENABLED=ON \
      -DEXT_TX_PROC_ENABLED=ON \
      -DMARIA_WITH_GLOG=ON \
      -DSTATISTICS=ON \
      -DWITH_DATA_STORE=${DATA_STORE_TYPE} \
      ${CMAKE_ARGS} \
      -DIOURING_ENABLED=${IOURING_ENABLED} \
      -DOPEN_LOG_SERVICE=OFF \
      -DFORK_HM_PROCESS=ON \
      ../

cmake --build . --config ${BUILD_TYPE} -j4
cmake --install . --config ${BUILD_TYPE}

# Copy main binaries and libraries
copy_libraries ${DEST_DIR}/bin/mariadbd ${DEST_DIR}/lib
copy_libraries ${DEST_DIR}/bin/mariadb ${DEST_DIR}/lib

# Build and install dss_server (only for ELOQDSS_* data stores)
if [[ "${DATA_STORE_TYPE}" == ELOQDSS_* ]]; then
    DSS_TYPE="${DATA_STORE_TYPE}"
else
    DSS_TYPE=""
fi

if [ -n "${DSS_TYPE}" ]; then
    cd ${ELOQSQL_SRC}/storage/eloq/store_handler/eloq_data_store_service
    mkdir -p build && cd build
    cmake .. -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DWITH_DATA_STORE=${DSS_TYPE} -DUSE_ONE_ELOQDSS_PARTITION_ENABLED=OFF
    cmake --build . --config ${BUILD_TYPE} -j4
    copy_libraries dss_server ${DEST_DIR}/lib
    mv dss_server ${DEST_DIR}/bin/
    cd ${ELOQSQL_SRC}
fi

# Build and install log_server (launch_sv)
cd ${ELOQSQL_SRC}/storage/eloq/eloq_log_service
mkdir bld && cd bld
cmake -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DCMAKE_EXPORT_COMPILE_COMMANDS=ON -DWITH_LOG_STATE=ROCKSDB_CLOUD_S3 ${CMAKE_ARGS} ../
cmake --build . --config ${BUILD_TYPE} -j4
copy_libraries launch_sv ${DEST_DIR}/lib
mv launch_sv ${DEST_DIR}/bin/

cd ${HOME}
tar -czvf eloqsql.tar.gz -C ${HOME} EloqSQL

if [ -n "${TAGGED}" ]; then
    SQL_TARBALL="eloqsql-${TAGGED}-${OS_ID}-${ARCH}.tar.gz"
    eval ${INSTALL_PSQL}
    SQL="INSERT INTO tx_release VALUES ('eloqsql', '${ARCH}', '${OS_ID}', '${DATA_STORE_ID}', $(echo ${TAGGED} | tr '.' ',')) ON CONFLICT DO NOTHING"
    psql postgresql://${PG_CONN}/eloq_release?sslmode=require -c "${SQL}"
else
    SQL_TARBALL="eloqsql-${OUT_NAME}-${OS_ID}-${ARCH}.tar.gz"
fi
aws s3 cp eloqsql.tar.gz ${S3_PREFIX}/${DATA_STORE_ID}/${SQL_TARBALL}
if [ -n "${CLOUDFRONT_DIST}" ]; then
    aws cloudfront create-invalidation --distribution-id ${CLOUDFRONT_DIST} --paths "/eloqsql/${DATA_STORE_ID}/${SQL_TARBALL}"
fi

# clean up eloqsql build artifacts
rm -rf eloqsql.tar.gz
cd $ELOQSQL_SRC
rm -rf bld
rm -rf storage/eloq/store_handler/eloq_data_store_service/bld
rm -rf storage/eloq/store_handler/eloq_data_store_service/build
rm -rf storage/eloq/eloq_log_service/bld
rm -rf ${DEST_DIR}

build_upload_log_srv() {
    if [ "$#" -lt 2 ]; then
      echo "Error: Function build_upload_log_srv requires at least 2 parameters."
      exit 1
    fi
    local log_tarball=$1
    local ds_type=$2
    log_sv_src=${ELOQSQL_SRC}/storage/eloq/eloq_log_service
    cd ${log_sv_src}
    mkdir -p LogService/bin
    mkdir build && cd build
    local cmake_args="-DCMAKE_BUILD_TYPE=$BUILD_TYPE -DWITH_ASAN=$ASAN -DDISABLE_CODE_LINE_IN_LOG=ON"
    if [ "$ds_type" = "ELOQDSS_ROCKSDB_CLOUD_S3" ]; then
        cmake_args="$cmake_args -DWITH_LOG_STATE=ROCKSDB_CLOUD_S3 -DWITH_CLOUD_AZ_INFO=ON"
    elif [ "$kv_type" = "ELOQDSS_ROCKSDB_CLOUD_GCS" ]; then
        cmake_args="$cmake_args -DWITH_LOG_STATE=ROCKSDB_CLOUD_GCS"
    fi
    cmake .. $cmake_args
    # build and copy log_server
    cmake --build . --config $BUILD_TYPE -j4
    mv ${log_sv_src}/build/launch_sv ${log_sv_src}/LogService/bin
    copy_libraries ${log_sv_src}/LogService/bin/launch_sv ${log_sv_src}/LogService/lib
    cd ${HOME}
    tar -czvf log_service.tar.gz -C ${log_sv_src} LogService
    aws s3 cp log_service.tar.gz ${S3_PREFIX}/logservice/${DATA_STORE_ID}/${log_tarball}
    #clean up
    rm -rf log_service.tar.gz
    cd "${log_sv_src}"
    rm -rf build
    rm -rf LogService
}

if [ "${BUILD_LOG_SRV}" = true ]; then
    # make and build log_service
    if [ -n "${TAGGED}" ]; then
        LOG_TARBALL="log-service-${TAGGED}-${OS_ID}-${ARCH}.tar.gz"
    else
        LOG_TARBALL="log-service-${OUT_NAME}-${OS_ID}-${ARCH}.tar.gz"
    fi
    build_upload_log_srv "${LOG_TARBALL}" "${DATA_STORE_TYPE}"

    if [ -n "${CLOUDFRONT_DIST}" ]; then
        aws cloudfront create-invalidation --distribution-id ${CLOUDFRONT_DIST} --paths "/eloqsql/logservice/${DATA_STORE_ID}/${LOG_TARBALL}"
    fi
fi
