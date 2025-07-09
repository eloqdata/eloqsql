#!/bin/bash
set -exo pipefail

CWDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ls
export WORKSPACE=$PWD

cd $WORKSPACE
whoami
pwd
ls
sudo chown -R mono $PWD

# make coredump dir writable.
if [ ! -d "/var/crash" ]; then sudo mkdir -p /var/crash; fi
sudo chmod 777 /var/crash

ulimit -c unlimited
echo '/var/crash/core.%t.%e.%p' | sudo tee /proc/sys/kernel/core_pattern

sudo chown -R mono /home/mono/workspace
cd /home/mono/workspace
ln -s $WORKSPACE/mariadb_src mariadb

cd mariadb
git submodule sync
git submodule update --init --recursive

cd /home/mono/workspace
ln -s $WORKSPACE/mono_test_src eloq_test

cd /home/mono/workspace/mariadb/storage/eloq

ln -s $WORKSPACE/logservice_src log_service

# setup mc command
mc alias set minio_server ${MINIO_ENDPOINT_URL} ${ELOQ_AWS_ACCESS_KEY_ID} ${ELOQ_AWS_SECRET_KEY}

# construct minio bucket
timestamp=$(($(date +%s%N)/1000000))
bucket_name="eloqsql-mtr-test-${timestamp}"
echo "bucket name is ${bucket_name}"

#config eloq_kv_storage.cnf
sed -i "s/eloq_kv_storage.*=.\+/eloq_kv_storage=eloqds/g" $WORKSPACE/mariadb_src/mysql-test/include/eloq_kv_storage.cnf

echo "eloq_kv_storage.cnf"
cat $WORKSPACE/mariadb_src/mysql-test/include/eloq_kv_storage.cnf

# config eloq_kv_dss.cnf
# ak/sk
sed -i "s/eloq_aws_access_key_id.*=.\+/eloq_aws_access_key_id=${ELOQ_AWS_ACCESS_KEY_ID}/g" $WORKSPACE/mariadb_src/mysql-test/include/eloq_kv_dss.cnf
sed -i "s/eloq_aws_secret_key.*=.\+/eloq_aws_secret_key=${ELOQ_AWS_SECRET_KEY}/g" $WORKSPACE/mariadb_src/mysql-test/include/eloq_kv_dss.cnf
# OSS settings
sed -i "s|eloq_txlog_rocksdb_cloud_endpoint_url.*=.\+|eloq_txlog_rocksdb_cloud_endpoint_url=${MINIO_ENDPOINT_URL}|g" $WORKSPACE/mariadb_src/mysql-test/include/eloq_kv_dss.cnf
sed -i "s|eloq_dss_rocksdb_cloud_endpoint_url.*=.\+|eloq_dss_rocksdb_cloud_endpoint_url=${MINIO_ENDPOINT_URL}|g" $WORKSPACE/mariadb_src/mysql-test/include/eloq_kv_dss.cnf
sed -i "s/eloq_txlog_rocksdb_cloud_bucket_name.*=.\+/eloq_txlog_rocksdb_cloud_bucket_name=${bucket_name}/g" $WORKSPACE/mariadb_src/mysql-test/include/eloq_kv_dss.cnf
sed -i "s/eloq_dss_rocksdb_cloud_bucket_name.*=.\+/eloq_dss_rocksdb_cloud_bucket_name=${bucket_name}/g" $WORKSPACE/mariadb_src/mysql-test/include/eloq_kv_dss.cnf
sed -i "s/eloq_txlog_rocksdb_cloud_region.*=.\+/eloq_txlog_rocksdb_cloud_region=${ELOQ_AWS_REGION}/g" $WORKSPACE/mariadb_src/mysql-test/include/eloq_kv_dss.cnf
sed -i "s/eloq_dss_rocksdb_cloud_region.*=.\+/eloq_dss_rocksdb_cloud_region=${ELOQ_AWS_REGION}/g" $WORKSPACE/mariadb_src/mysql-test/include/eloq_kv_dss.cnf
sed -i "s/eloq_txlog_rocksdb_cloud_bucket_prefix.*=.\+/eloq_txlog_rocksdb_cloud_bucket_prefix=txlog-/g" $WORKSPACE/mariadb_src/mysql-test/include/eloq_kv_dss.cnf
sed -i "s/eloq_dss_rocksdb_cloud_bucket_prefix.*=.\+/eloq_dss_rocksdb_cloud_bucket_prefix=dss-/g" $WORKSPACE/mariadb_src/mysql-test/include/eloq_kv_dss.cnf
sed -i "s|eloq_dss_config_file_path.*=.\+|eloq_dss_config_file_path=${WORKSPACE}/mariadb_src/concourse/scripts/dss_config.example.ini|g" $WORKSPACE/mariadb_src/mysql-test/include/eloq_kv_dss.cnf
sed -i "s|eloq_dss_peer_node.*=.\+|eloq_dss_peer_node=|g" $WORKSPACE/mariadb_src/mysql-test/include/eloq_kv_dss.cnf

echo "eloq_kv_dss.cnf"
cat $WORKSPACE/mariadb_src/mysql-test/include/eloq_kv_dss.cnf

# config mtr_bootstrap.cnf
# ak/sk
sed -i "s/eloq_aws_access_key_id.*=.\+/eloq_aws_access_key_id=${ELOQ_AWS_ACCESS_KEY_ID}/g" $WORKSPACE/mariadb_src/concourse/scripts/mtr_bootstrap.cnf
sed -i "s/eloq_aws_secret_key.*=.\+/eloq_aws_secret_key=${ELOQ_AWS_SECRET_KEY}/g" $WORKSPACE/mariadb_src/concourse/scripts/mtr_bootstrap.cnf
# kv_storage
sed -i "s/eloq_kv_storage.*=.\+/eloq_kv_storage=eloqds/g" $WORKSPACE/mariadb_src/concourse/scripts/mtr_bootstrap.cnf
# OSS settings
sed -i "s|eloq_txlog_rocksdb_cloud_endpoint_url.*=.\+|eloq_txlog_rocksdb_cloud_endpoint_url=${MINIO_ENDPOINT_URL}|g" $WORKSPACE/mariadb_src/concourse/scripts/mtr_bootstrap.cnf
sed -i "s|eloq_dss_rocksdb_cloud_endpoint_url.*=.\+|eloq_dss_rocksdb_cloud_endpoint_url=${MINIO_ENDPOINT_URL}|g" $WORKSPACE/mariadb_src/concourse/scripts/mtr_bootstrap.cnf
sed -i "s/eloq_txlog_rocksdb_cloud_bucket_name.*=.\+/eloq_txlog_rocksdb_cloud_bucket_name=${bucket_name}/g" $WORKSPACE/mariadb_src/concourse/scripts/mtr_bootstrap.cnf
sed -i "s/eloq_dss_rocksdb_cloud_bucket_name.*=.\+/eloq_dss_rocksdb_cloud_bucket_name=${bucket_name}/g" $WORKSPACE/mariadb_src/concourse/scripts/mtr_bootstrap.cnf
sed -i "s/eloq_txlog_rocksdb_cloud_region.*=.\+/eloq_txlog_rocksdb_cloud_region=${ELOQ_AWS_REGION}/g" $WORKSPACE/mariadb_src/concourse/scripts/mtr_bootstrap.cnf
sed -i "s/eloq_dss_rocksdb_cloud_region.*=.\+/eloq_dss_rocksdb_cloud_region=${ELOQ_AWS_REGION}/g" $WORKSPACE/mariadb_src/concourse/scripts/mtr_bootstrap.cnf
sed -i "s/eloq_txlog_rocksdb_cloud_bucket_prefix.*=.\+/eloq_txlog_rocksdb_cloud_bucket_prefix=txlog-/g" $WORKSPACE/mariadb_src/concourse/scripts/mtr_bootstrap.cnf
sed -i "s/eloq_dss_rocksdb_cloud_bucket_prefix.*=.\+/eloq_dss_rocksdb_cloud_bucket_prefix=dss-/g" $WORKSPACE/mariadb_src/concourse/scripts/mtr_bootstrap.cnf
sed -i "s|eloq_dss_config_file_path.*=.\+|eloq_dss_config_file_path=${WORKSPACE}/mariadb_src/concourse/scripts/dss_config.example.ini|g" $WORKSPACE/mariadb_src/concourse/scripts/mtr_bootstrap.cnf
sed -i "s|eloq_dss_peer_node.*=.\+|eloq_dss_peer_node=|g" $WORKSPACE/mariadb_src/concourse/scripts/mtr_bootstrap.cnf

echo "mtr_bootstrap.cnf"
cat $WORKSPACE/mariadb_src/concourse/scripts/mtr_bootstrap.cnf

#config mtr_multi_bootstrap.cnf
# ak/sk
sed -i "s/eloq_aws_access_key_id.*=.\+/eloq_aws_access_key_id=${ELOQ_AWS_ACCESS_KEY_ID}/g" $WORKSPACE/mariadb_src/concourse/scripts/mtr_multi_bootstrap.cnf
sed -i "s/eloq_aws_secret_key.*=.\+/eloq_aws_secret_key=${ELOQ_AWS_SECRET_KEY}/g" $WORKSPACE/mariadb_src/concourse/scripts/mtr_multi_bootstrap.cnf
# kv_storage
sed -i "s/eloq_kv_storage.*=.\+/eloq_kv_storage=eloqds/g" $WORKSPACE/mariadb_src/concourse/scripts/mtr_multi_bootstrap.cnf
# OSS settings
sed -i "s|eloq_txlog_rocksdb_cloud_endpoint_url.*=.\+|eloq_txlog_rocksdb_cloud_endpoint_url=${MINIO_ENDPOINT_URL}|g" $WORKSPACE/mariadb_src/concourse/scripts/mtr_multi_bootstrap.cnf
sed -i "s|eloq_dss_rocksdb_cloud_endpoint_url.*=.\+|eloq_dss_rocksdb_cloud_endpoint_url=${MINIO_ENDPOINT_URL}|g" $WORKSPACE/mariadb_src/concourse/scripts/mtr_multi_bootstrap.cnf
sed -i "s/eloq_txlog_rocksdb_cloud_bucket_name.*=.\+/eloq_txlog_rocksdb_cloud_bucket_name=${bucket_name}/g" $WORKSPACE/mariadb_src/concourse/scripts/mtr_multi_bootstrap.cnf
sed -i "s/eloq_dss_rocksdb_cloud_bucket_name.*=.\+/eloq_dss_rocksdb_cloud_bucket_name=${bucket_name}/g" $WORKSPACE/mariadb_src/concourse/scripts/mtr_multi_bootstrap.cnf
sed -i "s/eloq_txlog_rocksdb_cloud_region.*=.\+/eloq_txlog_rocksdb_cloud_region=${ELOQ_AWS_REGION}/g" $WORKSPACE/mariadb_src/concourse/scripts/mtr_multi_bootstrap.cnf
sed -i "s/eloq_dss_rocksdb_cloud_region.*=.\+/eloq_dss_rocksdb_cloud_region=${ELOQ_AWS_REGION}/g" $WORKSPACE/mariadb_src/concourse/scripts/mtr_multi_bootstrap.cnf
sed -i "s/eloq_txlog_rocksdb_cloud_bucket_prefix.*=.\+/eloq_txlog_rocksdb_cloud_bucket_prefix=txlog-/g" $WORKSPACE/mariadb_src/concourse/scripts/mtr_multi_bootstrap.cnf
sed -i "s/eloq_dss_rocksdb_cloud_bucket_prefix.*=.\+/eloq_dss_rocksdb_cloud_bucket_prefix=dss-/g" $WORKSPACE/mariadb_src/concourse/scripts/mtr_multi_bootstrap.cnf
sed -i "s|eloq_dss_config_file_path.*=.\+|eloq_dss_config_file_path=|g" $WORKSPACE/mariadb_src/concourse/scripts/mtr_multi_bootstrap.cnf
sed -i "s|eloq_dss_peer_node.*=.\+|eloq_dss_peer_node=localhost:9100|g" $WORKSPACE/mariadb_src/concourse/scripts/mtr_multi_bootstrap.cnf

echo "mtr_multi_bootstrap.cnf"
cat $WORKSPACE/mariadb_src/concourse/scripts/mtr_multi_bootstrap.cnf

#config dss_server.ini
# ak/sk
sed -i "s/ip.*=.\+/ip=localhost/g" $WORKSPACE/mariadb_src/concourse/scripts/dss_server.ini
sed -i "s/port.*=.\+/port=9100/g" $WORKSPACE/mariadb_src/concourse/scripts/dss_server.ini
sed -i "s/data_path.*=.\+/data_path=dss_data/g" $WORKSPACE/mariadb_src/concourse/scripts/dss_server.ini
sed -i "s/aws_access_key_id.*=.\+/aws_access_key_id=${ELOQ_AWS_ACCESS_KEY_ID}/g" $WORKSPACE/mariadb_src/concourse/scripts/dss_server.ini
sed -i "s/aws_secret_key.*=.\+/aws_secret_key=${ELOQ_AWS_SECRET_KEY}/g" $WORKSPACE/mariadb_src/concourse/scripts/dss_server.ini
sed -i "s|rocksdb_cloud_s3_endpoint_url.*=.\+|rocksdb_cloud_s3_endpoint_url=${MINIO_ENDPOINT_URL}|g" $WORKSPACE/mariadb_src/concourse/scripts/dss_server.ini
sed -i "s/rocksdb_cloud_bucket_name.*=.\+/rocksdb_cloud_bucket_name=${bucket_name}/g" $WORKSPACE/mariadb_src/concourse/scripts/dss_server.ini
sed -i "s/rocksdb_cloud_bucket_prefix.*=.\+/rocksdb_cloud_bucket_prefix=dss-/g" $WORKSPACE/mariadb_src/concourse/scripts/dss_server.ini

echo "dss_server.ini"
cat $WORKSPACE/mariadb_src/concourse/scripts/dss_server.ini

cd /home/mono/workspace/mariadb

echo "configuring"
if [ ! -d "bld" ]; then mkdir bld; fi
cd bld

if [ ! -f "Makefile" ]; then
    cmake -DCMAKE_INSTALL_PREFIX="/home/mono/workspace/mariadb/install" \
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
	  -DWITH_ROCKSDB_CLOUD=OFF \
          ../
fi

export ASAN_OPTIONS=abort_on_error=1:leak_check_at_exit=0

echo "building"
cmake --build . --config Debug -j8

echo "installing"
cmake --install . --config Debug

echo "building dss_server"
cd /home/mono/workspace/mariadb/storage/eloq/store_handler/eloq_data_store_service
mkdir bld && cd bld
cmake -DCMAKE_EXPORT_COMPILE_COMMANDS=ON -DCMAKE_BUILD_TYPE=Debug -DWITH_DATA_STORE=ELOQDSS_ROCKSDB_CLOUD_S3 ../
cmake --build . --config Debug -j8
echo "installing dss_server"
cp dss_server /home/mono/workspace/mariadb/install/bin/

echo "build finished"

cd /home/mono/workspace/eloq_test
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
mc rb minio_server/dss-${bucket_name} --force; \
mc rb minio_server/txlog-${bucket_name} --force
set -e

echo "running eloq_test"
# python run_tests.py --dbtype mariadb --storage eloqdss-rocksdb-cloud-s3 --install_path /home/mono/workspace/mariadb/install 

cd /home/mono/workspace/mariadb/bld/mysql-test

# Clean up minio buckets
echo "cleaning minio buckets"
set +e
pkill -9 dss_server
rm -rf dss_data
mc rb minio_server/dss-${bucket_name} --force; \
mc rb minio_server/txlog-${bucket_name} --force
set -e

echo "running mono_main,mono_basic"
./mtr --suite=mono_main,mono_basic --testcase-timeout=30 --bootstrap-defaults-file=$WORKSPACE/mariadb_src/concourse/scripts/mtr_bootstrap.cnf

# Clean up minio buckets
echo "cleaning minio buckets"
set +e
pkill -9 dss_server
rm -rf dss_data
mc rb minio_server/dss-${bucket_name} --force; \
mc rb minio_server/txlog-${bucket_name} --force
set -e

# Config eloq_kv_dss.cnf for multi
sed -i "s|eloq_dss_config_file_path.*=.\+|eloq_dss_config_file_path=|g" $WORKSPACE/mariadb_src/mysql-test/include/eloq_kv_dss.cnf
sed -i "s|eloq_dss_peer_node.*=.*|eloq_dss_peer_node=localhost:9100|g" $WORKSPACE/mariadb_src/mysql-test/include/eloq_kv_dss.cnf

echo "eloq_kv_dss.cnf"
cat $WORKSPACE/mariadb_src/mysql-test/include/eloq_kv_dss.cnf

# Start dss_server
echo "starting dss_server"
nohup /home/mono/workspace/mariadb/install/bin/dss_server --config=$WORKSPACE/mariadb_src/concourse/scripts/dss_server.ini > dss_server.log 2>&1 &
sleep 5

echo "running mono_multi"
./mtr --suite=mono_multi --force --bootstrap-defaults-file=$WORKSPACE/mariadb_src/concourse/scripts/mtr_multi_bootstrap.cnf

# Clean up minio bucket
# If mtr test failed, it would not be run.
echo "cleaning minio buckets"
set +e
pkill -9 dss_server
rm -rf dss_data
mc rb minio_server/dss-${bucket_name} --force; \
mc rb minio_server/txlog-${bucket_name} --force
set -e
