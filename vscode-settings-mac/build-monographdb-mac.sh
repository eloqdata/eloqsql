#!/bin/bash

set -euxo pipefail

echo "installing dependencies"
brew install git cmake ninja libuv glog openssl@1.1 gnu-getopt coreutils gflags leveldb gperftools bison

# install protobuf v3.17.3
# download from -> https://github.com/protocolbuffers/protobuf/releases/download/v3.17.3/protobuf-cpp-3.17.3.tar.gz
# follow this instruction -> https://github.com/protocolbuffers/protobuf/blob/master/src/README.md#c-installation---unix

export BASE_DIR=/Users/$LOGNAME/workspace
cd $BASE_DIR

echo "installing brpc"
git clone git@github.com:apache/incubator-brpc.git brpc
cd brpc
make clean
# create a symbolic link from /usr/local/opt/openssl to /usr/local/opt/openssl@1.1
ln -s /usr/local/opt/openssl@1.1 /usr/local/opt/openssl
sh config_brpc.sh --headers=/usr/local/include --libs=/usr/local/lib --cc=clang --cxx=clang++
make -j8

cp -r ./output/include/* /usr/local/include/
cp ./output/lib/* /usr/local/lib/

echo "installing braft"
git clone git@github.com:baidu/braft.git
cd braft

# modify CMakeLists.txt: line 70 
# from find_library(BRPC_LIB NAMES libbrpc.a brpc) 
# to find_library(BRPC_LIB NAMES brpc)

mkdir bld && cd bld 
cmake ../ -DOPENSSL_ROOT_DIR="/usr/local/opt/openssl@1.1"
make -j8

cp -r ./output/include/* /usr/local/include/
cp ./output/lib/* /usr/local/lib/

##################################################################

git clone git@github.com:monographdb/mariadb.git mariadb
cd mariadb

# [optional] .gitmodules github.com --> hub.fastgit.org, github.com.cnpmjs.org

# open vscode 
# copy *.json from vscode-settings-mac/ to local .vscode/
# modify *.json based on your env

# configure -> CMake:[Debug]
# build & install -> shift+cmd+B to run the tasks


# vscode workspace settings:
# open mariadb folder in vscode (make sure the 3 sub-folders are cloned into mariadb repo, and correctly structured)
# add log_service, tx_service, eloq 3 sub-folders to workspace(File -> Add Folder to Workspace...)
# save as xxx.code-workspace
# open workspace setting (cmd+shift+p -> Preferences: Open Workspace Settings (JSON))
# copy the content from sample.code-workspace to workspace settings json file


