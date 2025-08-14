#!/bin/bash

set -exo pipefail

export DEBIAN_FRONTEND=noninteractive

echo "installing dependencies"
sudo apt update
sudo apt-get install -y --no-install-recommends \
  jq sudo vim wget curl python3 python3-pip gdb libcurl4-openssl-dev build-essential libncurses5-dev gnutls-dev bison zlib1g-dev ccache \
  cmake ninja-build libuv1-dev git g++ make openjdk-11-jdk openssh-client \
  libssl-dev libgflags-dev libleveldb-dev libsnappy-dev openssl \
  lcov libbz2-dev liblz4-dev libzstd-dev libboost-context-dev \
  ca-certificates libc-ares-dev libc-ares2 m4 pkg-config tar gzip gcc
sudo rm -rf /var/lib/apt/lists/*

echo '%sudo ALL=(ALL) NOPASSWD:ALL' | sudo tee -a /etc/sudoers
sudo ln -s /usr/bin/python3.8 /usr/bin/python
if [ ! -d "/var/crash" ]; then
  sudo mkdir /var/crash
fi

current_user=$(whoami)
sudo chown -R ${current_user} /var/crash
export PATH=/home/${current_user}/.local/bin:$PATH
cd /home/${current_user}

# Configure environment
touch asan_suppr.conf
echo "leak:brpc" >>  asan_suppr.conf
echo "export LSAN_OPTIONS=suppressions=/home/${current_user}/asan_suppr.conf" >> ~/.bashrc

# Install Lua
curl -L -R -O https://www.lua.org/ftp/lua-5.4.6.tar.gz
tar zxf lua-5.4.6.tar.gz
cd lua-5.4.6 && make all && sudo make install
cd .. && rm -rf lua-5.4.6 && rm -rf lua-5.4.6.tar.gz

# install pip dependency for eloq_test
pip install --no-cache-dir --upgrade pip && \
pip install --no-cache-dir setuptools==45.2.0 \
  cassandra-driver==3.28.0 \
  awscli==1.29.44 \
  boto3==1.28.36 \
  botocore==1.31.44 \
  mysql-connector-python==8.1.0 \
  psutil==5.9.5 \
  grpcio==1.57.0 \
  grpcio-tools==1.57.0

if [ ! -d "workspace" ]; then
  mkdir workspace
fi
cd workspace
export WORKSPACE=$PWD

# Protobuf for bigtable
# Compile protobuf from source code.  Protobuf version need be compatibility
# with both brpc and grpc. It cannot be too high or too low.
cd $WORKSPACE
if [ ! -d "protobuf" ]; then
  mkdir -p protobuf
fi
cd protobuf
curl -fsSL https://github.com/protocolbuffers/protobuf/archive/refs/tags/v21.12.tar.gz | \
tar -xzf - --strip-components=1 && \
cmake \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_SHARED_LIBS=yes \
  -Dprotobuf_BUILD_TESTS=OFF \
  -Dprotobuf_ABSL_PROVIDER=package \
  -S . -B cmake-out && \
cmake --build cmake-out -- -j ${NCPU:-4} && \
sudo cmake --build cmake-out --target install -- -j ${NCPU:-4} && \
sudo ldconfig && \
cd ../ && rm -rf protobuf


echo "installing glog"
cd $WORKSPACE
if [ ! -d "glog" ]; then
    git clone https://github.com/monographdb/glog.git glog
fi
cd glog && \
cmake -S . -B build -G "Unix Makefiles" && \
cmake --build build -j6 && \
sudo cmake --build build --target install && \
cd ../ && \
rm -rf glog

echo "installing mimalloc"
cd $WORKSPACE
if [ ! -d "mimalloc" ]; then
  git clone https://github.com/monographdb/mimalloc.git mimalloc
fi
cd mimalloc &&  \
git checkout monograph-v2.1.2 && \
mkdir build && cd build && \
cmake .. && \
cmake --build . -j6 && \
sudo make install && \
cd ../../ && \
rm -rf mimalloc

echo "installing cuckoofilter"
cd $WORKSPACE
if [ ! -d "cuckoofilter" ]; then
  git clone https://github.com/monographdb/cuckoofilter.git cuckoofilter
fi
cd cuckoofilter &&  \
sudo make install && \
cd .. && \
rm -rf cuckoofilter

echo "installing brpc"
cd $WORKSPACE
if [ ! -d "brpc" ]; then
  git clone https://github.com/monographdb/brpc.git brpc
fi
cd brpc &&  \
mkdir build && cd build &&\
cmake .. \
-DWITH_GLOG=ON \
-DBUILD_SHARED_LIBS=ON && \
cmake --build . -j6 && \
sudo cp -r ./output/include/* /usr/include/ && \
sudo cp ./output/lib/* /usr/lib/ && \
cd ../../ && \
rm -rf brpc

echo "installing braft"
cd $WORKSPACE
if [ ! -d "braft" ]; then
   git clone https://github.com/monographdb/braft.git braft
fi
cd braft && \
sed -i 's/libbrpc.a//g' CMakeLists.txt && \
mkdir bld && cd bld && \
cmake .. -DBRPC_WITH_GLOG=ON &&\
cmake --build . -j6 && \
sudo cp -r ./output/include/* /usr/include/ && \
sudo cp ./output/lib/* /usr/lib/ && \
cd ../../ && \
rm -rf braft

# install liburing
git clone https://github.com/axboe/liburing.git liburing && \
cd liburing &&  \
git checkout tags/liburing-2.6 && \
./configure --cc=gcc --cxx=g++ && \
make -j4 && sudo make install && \
cd .. && \
rm -rf liburing

echo "installing AWSSDK"
cd $WORKSPACE
if [ ! -d "aws" ]; then
  git clone --recurse-submodules https://github.com/aws/aws-sdk-cpp.git aws
fi
cd aws && \
git checkout tags/1.11.446 && \
mkdir bld && cd bld && \
cmake .. \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DCMAKE_INSTALL_PREFIX=./output/ \
  -DENABLE_TESTING=OFF \
  -DBUILD_SHARED_LIBS=ON \
  -DFORCE_SHARED_CRT=OFF \
  -DBUILD_ONLY="dynamodb;sqs;s3;kinesis;kafka;transfer" && \
cmake --build . --config RelWithDebInfo -j6 && \
cmake --install . --config RelWithDebInfo && \
sudo cp -r ./output/include/* /usr/include/ && \
sudo cp -r ./output/lib/* /usr/lib/ && \
cd ../../ && \
rm -rf aws

echo "installing RocksDB"
cd $WORKSPACE
if [ ! -d "rocksdb" ]; then
  git clone https://github.com/facebook/rocksdb.git rocksdb
fi
cd rocksdb && \
git checkout tags/v9.1.0 && \
USE_RTTI=1 PORTABLE=1 make -j8 shared_lib && \
sudo make install-shared && \
cd ../ && \
sudo ldconfig && \
rm -rf rocksdb

echo "install prometheus-cpp"
cd $WORKSPACE
if [ ! -d "prometheus-cpp" ]; then
  git clone https://github.com/jupp0r/prometheus-cpp.git
fi
cd prometheus-cpp && \
git checkout tags/v1.1.0 && \
git submodule init && git submodule update && \
mkdir _build && cd _build && \
cmake .. -DBUILD_SHARED_LIBS=ON && \
cmake --build . -j6 && \
sudo cmake --install . &&  \
cd ../../ && \
rm -rf prometheus-cpp

# install Catch2 testing framework
echo "install Catch2"
cd $WORKSPACE
if [ ! -d "Catch2" ]; then
  git clone -b v3.3.2 https://github.com/catchorg/Catch2.git
fi
cd Catch2 && mkdir bld && cd bld && \
cmake .. \
  -DCMAKE_INSTALL_PREFIX=/usr/ \
  -DCATCH_BUILD_EXAMPLES=OFF \
  -DBUILD_TESTING=OFF && \
cmake --build . -j4 && \
sudo cmake --install . && \
cd ../../ && \
rm -rf Catch2

# Abseil for bigtable
echo "install Abseil for bigtable"
cd $WORKSPACE
if [ ! -d "abseil-cpp" ]; then
  mkdir -p abseil-cpp
fi
cd abseil-cpp
curl -fsSL https://github.com/abseil/abseil-cpp/archive/20230802.0.tar.gz | \
tar -xzf - --strip-components=1 && \
sed -i 's/^#define ABSL_OPTION_USE_\(.*\) 2/#define ABSL_OPTION_USE_\1 0/' "absl/base/options.h" && \
cmake \
  -DCMAKE_BUILD_TYPE=Release \
  -DABSL_BUILD_TESTING=OFF \
  -DBUILD_SHARED_LIBS=yes \
  -S . -B cmake-out && \
cmake --build cmake-out -- -j ${NCPU:-4} && \
sudo cmake --build cmake-out --target install -- -j ${NCPU:-4} && \
sudo ldconfig && \
cd ../ && \
rm -rf abseil-cpp

# RE2 for bigtable
echo "install RE2 for bigtable"
cd $WORKSPACE
if [ ! -d "re2" ]; then
  mkdir -p re2
fi
cd re2
curl -fsSL https://github.com/google/re2/archive/2023-08-01.tar.gz | \
tar -xzf - --strip-components=1 && \
cmake -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_SHARED_LIBS=ON \
  -DRE2_BUILD_TESTING=OFF \
  -S . -B cmake-out && \
cmake --build cmake-out -- -j ${NCPU:-4} && \
sudo cmake --build cmake-out --target install -- -j ${NCPU:-4} && \
sudo ldconfig && \
cd ../ && \
rm -rf re2

# gRPC for bigtable
# gRPC version should match protobuf.
echo "install grpc for bigtable"
cd $WORKSPACE
if [ ! -d "grpc" ]; then
  mkdir -p grpc
fi
cd grpc
curl -fsSL https://codeload.github.com/grpc/grpc/tar.gz/refs/tags/v1.51.1 | \
tar -xzf - --strip-components=1 && \
cmake \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_SHARED_LIBS=yes \
  -DgRPC_INSTALL=ON \
  -DgRPC_BUILD_TESTS=OFF \
  -DgRPC_ABSL_PROVIDER=package \
  -DgRPC_CARES_PROVIDER=package \
  -DgRPC_PROTOBUF_PROVIDER=package \
  -DgRPC_RE2_PROVIDER=package \
  -DgRPC_SSL_PROVIDER=package \
  -DgRPC_ZLIB_PROVIDER=package \
  -S . -B cmake-out && \
cmake --build cmake-out -- -j ${NCPU:-4} && \
sudo cmake --build cmake-out --target install -- -j ${NCPU:-4} && \
sudo ldconfig && \
cd ../ && \
rm -rf grpc

# crc32c for bigtable
echo "install crc32 for bigtable"
cd $WORKSPACE
if [ ! -d "crc32c" ]; then
  mkdir -p crc32c
fi
cd crc32c
curl -fsSL https://github.com/google/crc32c/archive/1.1.2.tar.gz | \
tar -xzf - --strip-components=1 && \
cmake \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_SHARED_LIBS=yes \
  -DCRC32C_BUILD_TESTS=OFF \
  -DCRC32C_BUILD_BENCHMARKS=OFF \
  -DCRC32C_USE_GLOG=OFF \
  -S . -B cmake-out && \
cmake --build cmake-out -- -j ${NCPU:-4} && \
sudo cmake --build cmake-out --target install -- -j ${NCPU:-4} && \
sudo ldconfig && \
cd ../ && \
rm -rf crc32c

# nlohmann_json for bigtable
echo "install json for bigtable"
cd $WORKSPACE
if [ ! -d "json" ]; then
  mkdir -p json
fi
cd json
curl -fsSL https://github.com/nlohmann/json/archive/v3.11.2.tar.gz | \
tar -xzf - --strip-components=1 && \
cmake \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_SHARED_LIBS=yes \
  -DBUILD_TESTING=OFF \
  -DJSON_BuildTests=OFF \
  -S . -B cmake-out && \
sudo cmake --build cmake-out --target install -- -j ${NCPU:-4} && \
sudo ldconfig && \
cd ../ && \
rm -rf json

# compile bigtable cpp client libraries
# Pick a location to install the artifacts, e.g., `/usr/local` or `/opt`
echo "install google-cloud-cpp for bigtable"
cd $WORKSPACE
if [ ! -d "google-cloud-cpp" ]; then
  mkdir -p google-cloud-cpp
fi
cd google-cloud-cpp
curl -fsSL https://codeload.github.com/googleapis/google-cloud-cpp/tar.gz/refs/tags/v2.14.0 | \
tar -xzf - --strip-components=1 && \
cmake -S . -B cmake-out \
  -DCMAKE_INSTALL_PREFIX="/usr/local" \
  -DBUILD_TESTING=OFF \
  -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
  -DGOOGLE_CLOUD_CPP_ENABLE_EXAMPLES=OFF \
  -DGOOGLE_CLOUD_CPP_ENABLE=bigtable,storage && \
cmake --build cmake-out -- -j "$(nproc)" && \
sudo cmake --build cmake-out --target install && \
cd ../ && \
rm -rf google-cloud-cpp

# install google-cloud-cli for bigtable
echo "install google-cli"
cd $WORKSPACE
curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-443.0.0-linux-x86_64.tar.gz && \
tar -xzf google-cloud-cli-443.0.0-linux-x86_64.tar.gz && \
./google-cloud-sdk/install.sh --quiet --additional-components beta bigtable cbt --rc-path $HOME/.bashrc --command-completion true --path-update true && \
rm -rf google-cloud-cli-443.0.0-linux-x86_64.tar.gz && rm -rf google-cloud-sdk

# install fakeit
echo "install fakeit"
cd $WORKSPACE
if [ ! -d "FakeIt" ]; then
  mkdir -p FakeIt
fi
cd FakeIt
git clone https://github.com/eranpeer/FakeIt.git && \
cd FakeIt && \
sudo cp single_header/catch/fakeit.hpp /usr/include/catch2/ && \
cd ../ && rm -rf FakeIt

# install rocksDB-Cloud
echo "install rocksdb-cloud"
cd $WORKSPACE
git clone git@github.com:monographdb/rocksdb-cloud.git && \
cd rocksdb-cloud && \
git checkout monographdb_main && \
LIBNAME=librocksdb-cloud-aws PORTABLE=1 USE_RTTI=1 USE_AWS=1 ROCKSDB_DISABLE_TCMALLOC=1 ROCKSDB_DISABLE_JEMALLOC=1 make shared_lib -j8 && \
LIBNAME=librocksdb-cloud-aws PREFIX=`pwd`/output make install-shared && \
sudo mkdir -p /usr/local/include/rocksdb_cloud_header && \
sudo cp -r ./output/include/* /usr/local/include/rocksdb_cloud_header && \
sudo cp -r ./output/lib/* /usr/local/lib && \
make clean && rm -rf `pwd`/output && \
LIBNAME=librocksdb-cloud-gcp PORTABLE=1 USE_RTTI=1 USE_GCP=1 ROCKSDB_DISABLE_TCMALLOC=1 ROCKSDB_DISABLE_JEMALLOC=1 make shared_lib -j8 && \
LIBNAME=librocksdb-cloud-gcp PREFIX=`pwd`/output make install-shared && \
sudo cp -r ./output/lib/* /usr/local/lib && \
cd ../ && \
sudo ldconfig && \
rm -rf rocksdb-cloud

cd $WORKSPACE
echo "dependencies installed"


echo "installing monograph"
echo "clone mariadb"
if [ ! -d mariadb ]; then
  git clone --branch eloq-10.6.10 git@github.com:monographdb/mariadb.git mariadb
fi

echo "clone monograph"
cd mariadb/storage
if [ ! -d monograph ]; then
  git clone --branch main git@github.com:monographdb/eloq_engine_for_mariadb.git monograph
fi

echo "clone cass driver"
cd monograph
if [ ! -d cass ]; then
  git clone https://github.com/monographdb/cpp-driver.git cass
fi

echo "clone tx_service"
if [ ! -d tx_service ]; then
  git clone --branch main git@github.com:monographdb/tx_service.git tx_service
fi

echo "clone log_service"
cd tx_service
if [ ! -d log_service ]; then
  git clone --branch main git@github.com:monographdb/log_service.git log_service
fi

echo "clone eloq_metrics"
cd log_service
if [ ! -d eloq_metrics ]; then
  git clone --branch main git@github.com:monographdb/eloq-metrics.git eloq_metrics
fi

echo "build monograph"

cd $WORKSPACE
if [ ! -d mariadb-bin ]; then
  mkdir mariadb-bin
fi

cd ${WORKSPACE}/mariadb
if [ ! -d bld ]; then
  mkdir bld
fi
cd bld

export ASAN_OPTIONS=abort_on_error=1:detect_container_overflow=0:leak_check_at_exit=0

CMAKE_BUILD_OPTS="-DCMAKE_BUILD_TYPE=Debug"
CMAKE_FEATURE_OPTS="-DWITH_READLINE=1"
CXXFLAGS="-Wno-error"
COROUTINE_ENABLED="ON"
EXT_TX_PROC_ENABLED="ON"
KV_TYPE="ELOQDSS_ROCKSDB"
ENABLE_ASAN="OFF"

if [ ! -f "Makefile" ]; then
    cmake $CMAKE_BUILD_OPTS $CMAKE_FEATURE_OPTS \
          -DCMAKE_INSTALL_PREFIX="~/workspace/mariadb-bin" \
          -DPLUGIN_{HANDLERSOCKET,ROCKSDB,ARIA,ARCHIVE,CVS,FEDERATEDX,TOKUDB,MROONGA,OQGRAPH,CONNECT,SPIDER,SPHINX,HEAP,MYISAMMRG,SEQUENCE}=NO \
          -DINSTALL_MYSQLTESTDIR="" \
          -DMYSQL_MAINTAINER_MODE=OFF \
          -DWITH_SSL=system \
          -DUSE_ONE_DATA_STORE_SHARD_ENABLED=off \
          -DCMAKE_CXX_FLAGS_DEBUG=${CXXFLAGS} \
          -DCOROUTINE_ENABLED=${COROUTINE_ENABLED} \
          -DEXT_TX_PROC_ENABLED=${EXT_TX_PROC_ENABLED} \
          -DWITH_DATA_STORE=${KV_TYPE} \
          -DWITH_ASAN=${ENABLE_ASAN} \
          ../
fi

cmake --build . --config Debug -j8
cmake --install . --config Debug

echo "monograph installed"
