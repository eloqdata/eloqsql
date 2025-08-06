#!/bin/bash
set -exo pipefail

CWDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ls
export WORKSPACE=$PWD

mkdir -p ~/.ssh
echo "$GIT_SSH_KEY" > ~/.ssh/id_rsa
chmod 600 ~/.ssh/id_rsa
ssh-keyscan github.com >> ~/.ssh/known_hosts

cd $WORKSPACE
whoami
pwd
ls
current_user=$(whoami)
sudo chown -R $current_user $PWD

# make coredump dir writable.
if [ ! -d "/var/crash" ]; then sudo mkdir -p /var/crash; fi
sudo chmod 777 /var/crash

ulimit -c unlimited
echo '/var/crash/core.%t.%e.%p' | sudo tee /proc/sys/kernel/core_pattern

sudo chown -R $current_user /home/$current_user/workspace
cd /home/$current_user/workspace
ln -s $WORKSPACE/eloqsql_pr eloqsql
ln -s $WORKSPACE/eloq_test_src eloq_test

cd /home/$current_user/workspace/eloqsql
git submodule sync
git submodule update --init --recursive
pr_branch_name=$(cat .git/resource/metadata.json | jq -r '.[] | select(.name=="head_name") | .value')

cd /home/$current_user/workspace/eloqsql/storage/eloq
ln -s $WORKSPACE/logservice_src eloq_log_service

cd eloq_log_service
if [ -n "$pr_branch_name" ] && git ls-remote --exit-code --heads origin "$pr_branch_name" > /dev/null; then
  git fetch origin '+refs/heads/*:refs/remotes/origin/*'
  git checkout -b ${pr_branch_name} origin/${pr_branch_name}
  git submodule update --init --recursive
fi
cd ..

cd tx_service
ln -s $WORKSPACE/raft_host_manager_src raft_host_manager
cd raft_host_manager
if [ -n "$pr_branch_name" ] && git ls-remote --exit-code --heads origin "$pr_branch_name" > /dev/null; then
  git fetch origin '+refs/heads/*:refs/remotes/origin/*'
  git checkout -b ${pr_branch_name} origin/${pr_branch_name}
  git submodule update --init --recursive
fi
cd ..

# setup mc command
# minio_server_alias will be used by mtr script for clean up mimio bucket
# in middle of server restart
export minio_server_alias="minio_server"
mc alias set ${minio_server_alias} ${MINIO_ENDPOINT_URL} ${ELOQ_AWS_ACCESS_KEY_ID} ${ELOQ_AWS_SECRET_KEY}

# construct minio bucket
timestamp=$(($(date +%s%N)/1000000))
export bucket_name="eloqsql-mtr-test-${timestamp}"
echo "bucket name is ${bucket_name}"

#config eloq_kv_storage.cnf
sed -i "s/eloq_kv_storage.*=.\+/eloq_kv_storage=eloqds/g" $WORKSPACE/eloqsql_pr/mysql-test/include/eloq_kv_storage.cnf

echo "eloq_kv_storage.cnf"
cat $WORKSPACE/eloqsql_pr/mysql-test/include/eloq_kv_storage.cnf

# config eloq_kv_dss.cnf
# ak/sk
sed -i "s/eloq_aws_access_key_id.*=.\+/eloq_aws_access_key_id=${ELOQ_AWS_ACCESS_KEY_ID}/g" $WORKSPACE/eloqsql_pr/mysql-test/include/eloq_kv_dss.cnf
sed -i "s/eloq_aws_secret_key.*=.\+/eloq_aws_secret_key=${ELOQ_AWS_SECRET_KEY}/g" $WORKSPACE/eloqsql_pr/mysql-test/include/eloq_kv_dss.cnf
# OSS settings
sed -i "s|eloq_txlog_rocksdb_cloud_endpoint_url.*=.\+|eloq_txlog_rocksdb_cloud_endpoint_url=${MINIO_ENDPOINT_URL}|g" $WORKSPACE/eloqsql_pr/mysql-test/include/eloq_kv_dss.cnf
sed -i "s|eloq_dss_rocksdb_cloud_endpoint_url.*=.\+|eloq_dss_rocksdb_cloud_endpoint_url=${MINIO_ENDPOINT_URL}|g" $WORKSPACE/eloqsql_pr/mysql-test/include/eloq_kv_dss.cnf
sed -i "s/eloq_txlog_rocksdb_cloud_bucket_name.*=.\+/eloq_txlog_rocksdb_cloud_bucket_name=${bucket_name}/g" $WORKSPACE/eloqsql_pr/mysql-test/include/eloq_kv_dss.cnf
sed -i "s/eloq_dss_rocksdb_cloud_bucket_name.*=.\+/eloq_dss_rocksdb_cloud_bucket_name=${bucket_name}/g" $WORKSPACE/eloqsql_pr/mysql-test/include/eloq_kv_dss.cnf
sed -i "s/eloq_txlog_rocksdb_cloud_region.*=.\+/eloq_txlog_rocksdb_cloud_region=${ELOQ_AWS_REGION}/g" $WORKSPACE/eloqsql_pr/mysql-test/include/eloq_kv_dss.cnf
sed -i "s/eloq_dss_rocksdb_cloud_region.*=.\+/eloq_dss_rocksdb_cloud_region=${ELOQ_AWS_REGION}/g" $WORKSPACE/eloqsql_pr/mysql-test/include/eloq_kv_dss.cnf
sed -i "s/eloq_txlog_rocksdb_cloud_bucket_prefix.*=.\+/eloq_txlog_rocksdb_cloud_bucket_prefix=txlog-/g" $WORKSPACE/eloqsql_pr/mysql-test/include/eloq_kv_dss.cnf
sed -i "s/eloq_dss_rocksdb_cloud_bucket_prefix.*=.\+/eloq_dss_rocksdb_cloud_bucket_prefix=dss-/g" $WORKSPACE/eloqsql_pr/mysql-test/include/eloq_kv_dss.cnf
sed -i "s|eloq_dss_config_file_path.*=.\+|eloq_dss_config_file_path=${WORKSPACE}/eloqsql_pr/concourse/scripts/dss_config.example.ini|g" $WORKSPACE/eloqsql_pr/mysql-test/include/eloq_kv_dss.cnf
sed -i "s|eloq_dss_peer_node.*=.\+|eloq_dss_peer_node=|g" $WORKSPACE/eloqsql_pr/mysql-test/include/eloq_kv_dss.cnf

echo "eloq_kv_dss.cnf"
cat $WORKSPACE/eloqsql_pr/mysql-test/include/eloq_kv_dss.cnf

# config mtr_bootstrap.cnf
# ak/sk
sed -i "s/eloq_aws_access_key_id.*=.\+/eloq_aws_access_key_id=${ELOQ_AWS_ACCESS_KEY_ID}/g" $WORKSPACE/eloqsql_pr/concourse/scripts/mtr_bootstrap.cnf
sed -i "s/eloq_aws_secret_key.*=.\+/eloq_aws_secret_key=${ELOQ_AWS_SECRET_KEY}/g" $WORKSPACE/eloqsql_pr/concourse/scripts/mtr_bootstrap.cnf
# kv_storage
sed -i "s/eloq_kv_storage.*=.\+/eloq_kv_storage=eloqds/g" $WORKSPACE/eloqsql_pr/concourse/scripts/mtr_bootstrap.cnf
# OSS settings
sed -i "s|eloq_txlog_rocksdb_cloud_endpoint_url.*=.\+|eloq_txlog_rocksdb_cloud_endpoint_url=${MINIO_ENDPOINT_URL}|g" $WORKSPACE/eloqsql_pr/concourse/scripts/mtr_bootstrap.cnf
sed -i "s|eloq_dss_rocksdb_cloud_endpoint_url.*=.\+|eloq_dss_rocksdb_cloud_endpoint_url=${MINIO_ENDPOINT_URL}|g" $WORKSPACE/eloqsql_pr/concourse/scripts/mtr_bootstrap.cnf
sed -i "s/eloq_txlog_rocksdb_cloud_bucket_name.*=.\+/eloq_txlog_rocksdb_cloud_bucket_name=${bucket_name}/g" $WORKSPACE/eloqsql_pr/concourse/scripts/mtr_bootstrap.cnf
sed -i "s/eloq_dss_rocksdb_cloud_bucket_name.*=.\+/eloq_dss_rocksdb_cloud_bucket_name=${bucket_name}/g" $WORKSPACE/eloqsql_pr/concourse/scripts/mtr_bootstrap.cnf
sed -i "s/eloq_txlog_rocksdb_cloud_region.*=.\+/eloq_txlog_rocksdb_cloud_region=${ELOQ_AWS_REGION}/g" $WORKSPACE/eloqsql_pr/concourse/scripts/mtr_bootstrap.cnf
sed -i "s/eloq_dss_rocksdb_cloud_region.*=.\+/eloq_dss_rocksdb_cloud_region=${ELOQ_AWS_REGION}/g" $WORKSPACE/eloqsql_pr/concourse/scripts/mtr_bootstrap.cnf
sed -i "s/eloq_txlog_rocksdb_cloud_bucket_prefix.*=.\+/eloq_txlog_rocksdb_cloud_bucket_prefix=txlog-/g" $WORKSPACE/eloqsql_pr/concourse/scripts/mtr_bootstrap.cnf
sed -i "s/eloq_dss_rocksdb_cloud_bucket_prefix.*=.\+/eloq_dss_rocksdb_cloud_bucket_prefix=dss-/g" $WORKSPACE/eloqsql_pr/concourse/scripts/mtr_bootstrap.cnf
sed -i "s|eloq_dss_config_file_path.*=.\+|eloq_dss_config_file_path=${WORKSPACE}/eloqsql_pr/concourse/scripts/dss_config.example.ini|g" $WORKSPACE/eloqsql_pr/concourse/scripts/mtr_bootstrap.cnf
sed -i "s|eloq_dss_peer_node.*=.\+|eloq_dss_peer_node=|g" $WORKSPACE/eloqsql_pr/concourse/scripts/mtr_bootstrap.cnf

echo "mtr_bootstrap.cnf"
cat $WORKSPACE/eloqsql_pr/concourse/scripts/mtr_bootstrap.cnf

#config mtr_multi_bootstrap.cnf
# ak/sk
sed -i "s/eloq_aws_access_key_id.*=.\+/eloq_aws_access_key_id=${ELOQ_AWS_ACCESS_KEY_ID}/g" $WORKSPACE/eloqsql_pr/concourse/scripts/mtr_multi_bootstrap.cnf
sed -i "s/eloq_aws_secret_key.*=.\+/eloq_aws_secret_key=${ELOQ_AWS_SECRET_KEY}/g" $WORKSPACE/eloqsql_pr/concourse/scripts/mtr_multi_bootstrap.cnf
# kv_storage
sed -i "s/eloq_kv_storage.*=.\+/eloq_kv_storage=eloqds/g" $WORKSPACE/eloqsql_pr/concourse/scripts/mtr_multi_bootstrap.cnf
# OSS settings
sed -i "s|eloq_txlog_rocksdb_cloud_endpoint_url.*=.\+|eloq_txlog_rocksdb_cloud_endpoint_url=${MINIO_ENDPOINT_URL}|g" $WORKSPACE/eloqsql_pr/concourse/scripts/mtr_multi_bootstrap.cnf
sed -i "s|eloq_dss_rocksdb_cloud_endpoint_url.*=.\+|eloq_dss_rocksdb_cloud_endpoint_url=${MINIO_ENDPOINT_URL}|g" $WORKSPACE/eloqsql_pr/concourse/scripts/mtr_multi_bootstrap.cnf
sed -i "s/eloq_txlog_rocksdb_cloud_bucket_name.*=.\+/eloq_txlog_rocksdb_cloud_bucket_name=${bucket_name}/g" $WORKSPACE/eloqsql_pr/concourse/scripts/mtr_multi_bootstrap.cnf
sed -i "s/eloq_dss_rocksdb_cloud_bucket_name.*=.\+/eloq_dss_rocksdb_cloud_bucket_name=${bucket_name}/g" $WORKSPACE/eloqsql_pr/concourse/scripts/mtr_multi_bootstrap.cnf
sed -i "s/eloq_txlog_rocksdb_cloud_region.*=.\+/eloq_txlog_rocksdb_cloud_region=${ELOQ_AWS_REGION}/g" $WORKSPACE/eloqsql_pr/concourse/scripts/mtr_multi_bootstrap.cnf
sed -i "s/eloq_dss_rocksdb_cloud_region.*=.\+/eloq_dss_rocksdb_cloud_region=${ELOQ_AWS_REGION}/g" $WORKSPACE/eloqsql_pr/concourse/scripts/mtr_multi_bootstrap.cnf
sed -i "s/eloq_txlog_rocksdb_cloud_bucket_prefix.*=.\+/eloq_txlog_rocksdb_cloud_bucket_prefix=txlog-/g" $WORKSPACE/eloqsql_pr/concourse/scripts/mtr_multi_bootstrap.cnf
sed -i "s/eloq_dss_rocksdb_cloud_bucket_prefix.*=.\+/eloq_dss_rocksdb_cloud_bucket_prefix=dss-/g" $WORKSPACE/eloqsql_pr/concourse/scripts/mtr_multi_bootstrap.cnf
sed -i "s|eloq_dss_config_file_path.*=.\+|eloq_dss_config_file_path=|g" $WORKSPACE/eloqsql_pr/concourse/scripts/mtr_multi_bootstrap.cnf
sed -i "s|eloq_dss_peer_node.*=.\+|eloq_dss_peer_node=localhost:9100|g" $WORKSPACE/eloqsql_pr/concourse/scripts/mtr_multi_bootstrap.cnf

echo "mtr_multi_bootstrap.cnf"
cat $WORKSPACE/eloqsql_pr/concourse/scripts/mtr_multi_bootstrap.cnf

#config dss_server.ini
# ak/sk
sed -i "s/ip.*=.\+/ip=localhost/g" $WORKSPACE/eloqsql_pr/concourse/scripts/dss_server.ini
sed -i "s/port.*=.\+/port=9100/g" $WORKSPACE/eloqsql_pr/concourse/scripts/dss_server.ini
sed -i "s/data_path.*=.\+/data_path=dss_data/g" $WORKSPACE/eloqsql_pr/concourse/scripts/dss_server.ini
sed -i "s/aws_access_key_id.*=.\+/aws_access_key_id=${ELOQ_AWS_ACCESS_KEY_ID}/g" $WORKSPACE/eloqsql_pr/concourse/scripts/dss_server.ini
sed -i "s/aws_secret_key.*=.\+/aws_secret_key=${ELOQ_AWS_SECRET_KEY}/g" $WORKSPACE/eloqsql_pr/concourse/scripts/dss_server.ini
sed -i "s|rocksdb_cloud_s3_endpoint_url.*=.\+|rocksdb_cloud_s3_endpoint_url=${MINIO_ENDPOINT_URL}|g" $WORKSPACE/eloqsql_pr/concourse/scripts/dss_server.ini
sed -i "s/rocksdb_cloud_bucket_name.*=.\+/rocksdb_cloud_bucket_name=${bucket_name}/g" $WORKSPACE/eloqsql_pr/concourse/scripts/dss_server.ini
sed -i "s/rocksdb_cloud_bucket_prefix.*=.\+/rocksdb_cloud_bucket_prefix=dss-/g" $WORKSPACE/eloqsql_pr/concourse/scripts/dss_server.ini

echo "dss_server.ini"
cat $WORKSPACE/eloqsql_pr/concourse/scripts/dss_server.ini

cd /home/$current_user/workspace/eloqsql

echo "configuring"
if [ ! -d "bld" ]; then mkdir bld; fi
cd bld

if [ ! -f "Makefile" ]; then
    cmake -DCMAKE_INSTALL_PREFIX="/home/$current_user/workspace/eloqsql/install" \
          -DPLUGIN_{HANDLERSOCKET,ROCKSDB,ARIA,ARCHIVE,CVS,FEDERATEDX,TOKUDB,MROONGA,OQGRAPH,CONNECT,SPIDER,SPHINX,HEAP,MYISAMMRG}=NO \
          -DCMAKE_BUILD_TYPE=Debug \
          -DINSTALL_MYSQLTESTDIR="" \
          -DCMAKE_CXX_FLAGS_DEBUG="${CMAKE_CXX_FLAGS_DEBUG} -Wno-error" \
          -DMYSQL_MAINTAINER_MODE=OFF \
          -DWITH_SSL=system \
          -DWITH_ASAN=OFF \
          -DRANGE_PARTITION_ENABLED=ON \
          -DSMALL_RANGE=ON \
          -DCOROUTINE_ENABLED=ON \
          -DEXT_TX_PROC_ENABLED=ON \
          -DMARIA_WITH_GLOG=ON \
          -DSTATISTICS=ON \
          -DWITH_DATA_STORE=ELOQDSS_ROCKSDB_CLOUD_S3 \
          -DUSE_ROCKSDB_LOG_STATE=ON \
	  -DWITH_ROCKSDB_CLOUD=S3 \
          -DOPEN_LOG_SERVICE=OFF \
          -DFORK_HM_PROCESS=ON \
          ../
fi

export ASAN_OPTIONS=abort_on_error=1:leak_check_at_exit=0

echo "building"
cmake --build . --config Debug -j8

echo "installing"
cmake --install . --config Debug

echo "building dss_server"
cd /home/$current_user/workspace/eloqsql/storage/eloq/store_handler/eloq_data_store_service
mkdir bld && cd bld
cmake -DCMAKE_EXPORT_COMPILE_COMMANDS=ON -DCMAKE_BUILD_TYPE=Debug -DWITH_DATA_STORE=ELOQDSS_ROCKSDB_CLOUD_S3 ../
cmake --build . --config Debug -j8
echo "installing dss_server"
cp dss_server /home/$current_user/workspace/eloqsql/install/bin/

echo "building log_server"
cd /home/$current_user/workspace/eloqsql/storage/eloq/eloq_log_service
mkdir bld && cd bld
cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_EXPORT_COMPILE_COMMANDS=ON -DUSE_ROCKSDB_LOG_STATE=ON -DWITH_ROCKSDB_CLOUD=S3 ../
cmake --build . --config Debug -j8
echo "installing launch_sv"
cp launch_sv /home/$current_user/workspace/eloqsql/install/bin/

echo "build finished"

cd /home/$current_user/workspace/eloq_test
./setup
sed -i "s/eloq_aws_access_key_id.*=.\+/eloq_aws_access_key_id=${ELOQ_AWS_ACCESS_KEY_ID}/g" ./bootstrap_cnf/*_s3.cnf
sed -i "s/eloq_aws_secret_key.*=.\+/eloq_aws_secret_key=${ELOQ_AWS_SECRET_KEY}/g" ./bootstrap_cnf/*_s3.cnf
sed -i "s|eloq_txlog_rocksdb_cloud_endpoint_url.*=.\+|eloq_txlog_rocksdb_cloud_endpoint_url=${MINIO_ENDPOINT_URL}|g" ./bootstrap_cnf/*_s3.cnf
sed -i "s/eloq_txlog_rocksdb_cloud_bucket_name.*=.\+/eloq_txlog_rocksdb_cloud_bucket_name=${bucket_name}/g" ./bootstrap_cnf/*_s3.cnf
sed -i "s/eloq_txlog_rocksdb_cloud_bucket_prefix.*=.\+/eloq_txlog_rocksdb_cloud_bucket_prefix=txlog-/g" ./bootstrap_cnf/*_s3.cnf

sed -i "s/aws_access_key_id.*=.\+/aws_access_key_id=${ELOQ_AWS_ACCESS_KEY_ID}/g" ./bootstrap_cnf/eloqdss_server.cnf
sed -i "s/aws_secret_key.*=.\+/aws_secret_key=${ELOQ_AWS_SECRET_KEY}/g" ./bootstrap_cnf/eloqdss_server.cnf
sed -i "s|rocksdb_cloud_s3_endpoint_url.*=.\+|rocksdb_cloud_s3_endpoint_url=${MINIO_ENDPOINT_URL}|g" ./bootstrap_cnf/eloqdss_server.cnf
sed -i "s/rocksdb_cloud_bucket_prefix.*=.\+/rocksdb_cloud_bucket_prefix=dss-/g" ./bootstrap_cnf/eloqdss_server.cnf
sed -i "s/rocksdb_cloud_bucket_name.*=.\+/rocksdb_cloud_bucket_name=${bucket_name}/g" ./bootstrap_cnf/eloqdss_server.cnf

sed -i "s/aws_access_key_id.*=.\+/aws_access_key_id=${ELOQ_AWS_ACCESS_KEY_ID}/g" ./storage.cnf
sed -i "s/aws_secret_key.*=.\+/aws_secret_key=${ELOQ_AWS_SECRET_KEY}/g" ./storage.cnf
sed -i "s|rocksdb_cloud_s3_endpoint_url.*=.\+|rocksdb_cloud_s3_endpoint_url=${MINIO_ENDPOINT_URL}|g" ./storage.cnf
sed -i "s/rocksdb_cloud_bucket_prefix.*=.\+/rocksdb_cloud_bucket_prefix=dss-/g" ./storage.cnf
sed -i "s/rocksdb_cloud_bucket_name.*=.\+/rocksdb_cloud_bucket_name=${bucket_name}/g" ./storage.cnf

# Clean up minio buckets
echo "cleaning minio buckets"
set +e
pkill -9 dss_server
rm -rf dss_data
mc rb ${minio_server_alias}/dss-${bucket_name} --force; \
mc rb ${minio_server_alias}/txlog-${bucket_name} --force
set -e

echo "running eloq_test"
# python run_tests.py --dbtype eloqsql --storage eloqdss-rocksdb-cloud-s3 --install_path /home/$current_user/workspace/eloqsql/install

cd /home/$current_user/workspace/eloqsql/bld/mysql-test

# Clean up minio buckets
echo "cleaning minio buckets"
set +e
pkill -9 dss_server
rm -rf dss_data
mc rb ${minio_server_alias}/dss-${bucket_name} --force; \
mc rb ${minio_server_alias}/txlog-${bucket_name} --force
set -e

echo "running mono_main,mono_basic"
./mtr --clean-txlog-bucket-restart --suite=mono_main,mono_basic --testcase-timeout=30 --bootstrap-defaults-file=$WORKSPACE/eloqsql_pr/concourse/scripts/mtr_bootstrap.cnf

# Clean up minio buckets
echo "cleaning minio buckets"
set +e
pkill -9 dss_server
rm -rf dss_data
mc rb ${minio_server_alias}/dss-${bucket_name} --force; \
mc rb ${minio_server_alias}/txlog-${bucket_name} --force
set -e

# Config eloq_kv_dss.cnf for multi
sed -i "s|eloq_dss_config_file_path.*=.\+|eloq_dss_config_file_path=|g" $WORKSPACE/eloqsql_pr/mysql-test/include/eloq_kv_dss.cnf
sed -i "s|eloq_dss_peer_node.*=.*|eloq_dss_peer_node=localhost:9100|g" $WORKSPACE/eloqsql_pr/mysql-test/include/eloq_kv_dss.cnf

echo "eloq_kv_dss.cnf"
cat $WORKSPACE/eloqsql_pr/mysql-test/include/eloq_kv_dss.cnf

# Start dss_server
echo "starting dss_server"
nohup /home/$current_user/workspace/eloqsql/install/bin/dss_server --config=$WORKSPACE/eloqsql_pr/concourse/scripts/dss_server.ini > dss_server.log 2>&1 &
sleep 5

echo "running mono_multi"
./mtr --suite=mono_multi --clean-txlog-bucket-restart --force --bootstrap-defaults-file=$WORKSPACE/eloqsql_pr/concourse/scripts/mtr_multi_bootstrap.cnf

# Clean up minio bucket
# If mtr test failed, it would not be run.
echo "cleaning minio buckets"
set +e
pkill -9 dss_server
rm -rf dss_data
mc rb ${minio_server_alias}/dss-${bucket_name} --force; \
mc rb ${minio_server_alias}/txlog-${bucket_name} --force
set -e
