#!/bin/bash
set -exo pipefail

CWDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ls
export WORKSPACE=$PWD
echo ${MINIO_ENDPOINT_URL}

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
ln -s $WORKSPACE/eloqsql_src eloqsql
ln -s $WORKSPACE/eloq_test_src eloq_test

cd /home/$current_user/workspace/eloqsql
git submodule sync
git submodule update --init --recursive

# setup mc command
# minio_server_alias will be used by mtr script for clean up mimio bucket
# in middle of server restart
export minio_server_alias="minio_server"
mc alias set ${minio_server_alias} ${MINIO_ENDPOINT_URL} ${ELOQ_AWS_ACCESS_KEY_ID} ${ELOQ_AWS_SECRET_KEY}

# construct minio bucket
timestamp=$(($(date +%s%N)/1000000))
export bucket_name="eloqsql-mtr-test-${timestamp}"
echo "bucket name is ${bucket_name}"

# Helper function to update config template file with required settings
update_config_template() {
    local config_file="$1"
    if [ ! -f "$config_file" ]; then
        echo "Warning: Config file $config_file does not exist"
        return 1
    fi
    
    # ak/sk
    sed -i "s/aws_access_key_id.*=.\+/aws_access_key_id=${ELOQ_AWS_ACCESS_KEY_ID}/g" "$config_file"
    sed -i "s/aws_secret_key.*=.\+/aws_secret_key=${ELOQ_AWS_SECRET_KEY}/g" "$config_file"
    # OSS settings
    sed -i "s|rocksdb_cloud_s3_endpoint_url.*=.\+|rocksdb_cloud_s3_endpoint_url=${MINIO_ENDPOINT_URL}|g" "$config_file"
    sed -i "s|txlog_rocksdb_cloud_s3_endpoint_url.*=.\+|txlog_rocksdb_cloud_s3_endpoint_url=${MINIO_ENDPOINT_URL}|g" "$config_file"
    sed -i "s/rocksdb_cloud_bucket_name.*=.\+/rocksdb_cloud_bucket_name=${bucket_name}/g" "$config_file"
    sed -i "s/txlog_rocksdb_cloud_bucket_name.*=.\+/txlog_rocksdb_cloud_bucket_name=${bucket_name}/g" "$config_file"
    sed -i "s/rocksdb_cloud_region.*=.\+/rocksdb_cloud_region=${ELOQ_AWS_REGION}/g" "$config_file"
    sed -i "s/txlog_rocksdb_cloud_region.*=.\+/txlog_rocksdb_cloud_region=${ELOQ_AWS_REGION}/g" "$config_file"
    sed -i "s/rocksdb_cloud_bucket_prefix.*=.\+/rocksdb_cloud_bucket_prefix=dss-/g" "$config_file"
    sed -i "s/txlog_rocksdb_cloud_bucket_prefix.*=.\+/txlog_rocksdb_cloud_bucket_prefix=txlog-/g" "$config_file"
    sed -i "s|eloq_dss_config_file_path.*=.\+|eloq_dss_config_file_path=${WORKSPACE}/eloqsql_src/concourse/scripts/dss_config.example.ini|g" "$config_file"
}

# config data_substrate.cnf
update_config_template "$WORKSPACE/eloqsql_src/storage/eloq/mysql-test/mono_basic/data_substrate.cnf"
update_config_template "$WORKSPACE/eloqsql_src/storage/eloq/mysql-test/mono_main/data_substrate.cnf"
update_config_template "$WORKSPACE/eloqsql_src/storage/eloq/mysql-test/mono_multi/data_substrate1.cnf"
update_config_template "$WORKSPACE/eloqsql_src/storage/eloq/mysql-test/mono_multi/data_substrate2.cnf"
update_config_template "$WORKSPACE/eloqsql_src/storage/eloq/mysql-test/mono_multi/data_substrate3.cnf"
echo "data_substrate.cnf"
cat $WORKSPACE/eloqsql_src/storage/eloq/mysql-test/mono_basic/data_substrate.cnf
cat $WORKSPACE/eloqsql_src/storage/eloq/mysql-test/mono_main/data_substrate.cnf
cat $WORKSPACE/eloqsql_src/storage/eloq/mysql-test/mono_multi/data_substrate1.cnf
cat $WORKSPACE/eloqsql_src/storage/eloq/mysql-test/mono_multi/data_substrate2.cnf
cat $WORKSPACE/eloqsql_src/storage/eloq/mysql-test/mono_multi/data_substrate3.cnf

# config mtr_bootstrap.cnf
update_config_template "$WORKSPACE/eloqsql_src/concourse/scripts/mtr_bootstrap_ds.cnf"
sed -i "s|eloq_config=.*|eloq_config=$WORKSPACE/eloqsql_src/concourse/scripts/mtr_bootstrap_ds.cnf|g" $WORKSPACE/eloqsql_src/concourse/scripts/mtr_bootstrap.cnf
sed -i "s|hm_bin_path=.*|hm_bin_path=$WORKSPACE/eloqsql_src/bld/data_substrate/host_manager|g" $WORKSPACE/eloqsql_src/concourse/scripts/mtr_bootstrap_ds.cnf
echo "mtr_bootstrap_ds.cnf"
cat $WORKSPACE/eloqsql_src/concourse/scripts/mtr_bootstrap_ds.cnf

#config mtr_multi_bootstrap.cnf
update_config_template "$WORKSPACE/eloqsql_src/concourse/scripts/mtr_multi_bootstrap_ds.cnf"
sed -i "s|eloq_config=.*|eloq_config=$WORKSPACE/eloqsql_src/concourse/scripts/mtr_multi_bootstrap_ds.cnf|g" $WORKSPACE/eloqsql_src/concourse/scripts/mtr_multi_bootstrap.cnf
sed -i "s|hm_bin_path=.*|hm_bin_path=$WORKSPACE/eloqsql_src/bld/data_substrate/host_manager|g" $WORKSPACE/eloqsql_src/concourse/scripts/mtr_multi_bootstrap_ds.cnf
echo "mtr_multi_bootstrap_ds.cnf"
cat $WORKSPACE/eloqsql_src/concourse/scripts/mtr_multi_bootstrap_ds.cnf

#config dss_server.ini
# ak/sk
sed -i "s/ip.*=.\+/ip=localhost/g" $WORKSPACE/eloqsql_src/concourse/scripts/dss_server.ini
sed -i "s/port.*=.\+/port=9100/g" $WORKSPACE/eloqsql_src/concourse/scripts/dss_server.ini
sed -i "s/data_path.*=.\+/data_path=dss_data/g" $WORKSPACE/eloqsql_src/concourse/scripts/dss_server.ini
sed -i "s/aws_access_key_id.*=.\+/aws_access_key_id=${ELOQ_AWS_ACCESS_KEY_ID}/g" $WORKSPACE/eloqsql_src/concourse/scripts/dss_server.ini
sed -i "s/aws_secret_key.*=.\+/aws_secret_key=${ELOQ_AWS_SECRET_KEY}/g" $WORKSPACE/eloqsql_src/concourse/scripts/dss_server.ini
sed -i "s|rocksdb_cloud_s3_endpoint_url.*=.\+|rocksdb_cloud_s3_endpoint_url=${MINIO_ENDPOINT_URL}|g" $WORKSPACE/eloqsql_src/concourse/scripts/dss_server.ini
sed -i "s/rocksdb_cloud_bucket_name.*=.\+/rocksdb_cloud_bucket_name=${bucket_name}/g" $WORKSPACE/eloqsql_src/concourse/scripts/dss_server.ini
sed -i "s/rocksdb_cloud_bucket_prefix.*=.\+/rocksdb_cloud_bucket_prefix=dss-/g" $WORKSPACE/eloqsql_src/concourse/scripts/dss_server.ini

echo "dss_server.ini"
cat $WORKSPACE/eloqsql_src/concourse/scripts/dss_server.ini

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
          -DSMALL_RANGE=ON \
          -DCOROUTINE_ENABLED=ON \
          -DEXT_TX_PROC_ENABLED=ON \
          -DMARIA_WITH_GLOG=ON \
          -DSTATISTICS=ON \
          -DWITH_DATA_STORE=ELOQDSS_ROCKSDB_CLOUD_S3 \
      -DELOQ_MODULE_ENABLED=ON \
          ../
fi

export ASAN_OPTIONS=abort_on_error=1:leak_check_at_exit=0

echo "building"
cmake --build . --config Debug -j8

echo "installing"
cmake --install . --config Debug

echo "building dss_server"
cd /home/$current_user/workspace/eloqsql/data_substrate/store_handler/eloq_data_store_service
mkdir bld && cd bld
cmake -DCMAKE_EXPORT_COMPILE_COMMANDS=ON -DCMAKE_BUILD_TYPE=Debug -DWITH_DATA_STORE=ELOQDSS_ROCKSDB_CLOUD_S3 ../
cmake --build . --config Debug -j8
echo "installing dss_server"
cp dss_server /home/$current_user/workspace/eloqsql/install/bin/

echo "building log_server"
cd /home/$current_user/workspace/eloqsql/data_substrate/log_service
mkdir bld && cd bld
cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_EXPORT_COMPILE_COMMANDS=ON ../
cmake --build . --config Debug -j8
echo "installing launch_sv"
cp launch_sv /home/$current_user/workspace/eloqsql/install/bin/

echo "build finished"

# cd /home/$current_user/workspace/eloq_test
# ./setup
# sed -i "s/eloq_aws_access_key_id.*=.\+/eloq_aws_access_key_id=${ELOQ_AWS_ACCESS_KEY_ID}/g" ./bootstrap_cnf/*_s3.cnf
# sed -i "s/eloq_aws_secret_key.*=.\+/eloq_aws_secret_key=${ELOQ_AWS_SECRET_KEY}/g" ./bootstrap_cnf/*_s3.cnf
# sed -i "s|eloq_txlog_rocksdb_cloud_s3_endpoint_url.*=.\+|eloq_txlog_rocksdb_cloud_s3_endpoint_url=${MINIO_ENDPOINT_URL}|g" ./bootstrap_cnf/*_s3.cnf
# sed -i "s/eloq_txlog_rocksdb_cloud_bucket_name.*=.\+/eloq_txlog_rocksdb_cloud_bucket_name=${bucket_name}/g" ./bootstrap_cnf/*_s3.cnf
# sed -i "s/eloq_txlog_rocksdb_cloud_bucket_prefix.*=.\+/eloq_txlog_rocksdb_cloud_bucket_prefix=txlog-/g" ./bootstrap_cnf/*_s3.cnf

# sed -i "s/aws_access_key_id.*=.\+/aws_access_key_id=${ELOQ_AWS_ACCESS_KEY_ID}/g" ./bootstrap_cnf/eloqdss_server.cnf
# sed -i "s/aws_secret_key.*=.\+/aws_secret_key=${ELOQ_AWS_SECRET_KEY}/g" ./bootstrap_cnf/eloqdss_server.cnf
# sed -i "s|rocksdb_cloud_s3_endpoint_url.*=.\+|rocksdb_cloud_s3_endpoint_url=${MINIO_ENDPOINT_URL}|g" ./bootstrap_cnf/eloqdss_server.cnf
# sed -i "s/rocksdb_cloud_bucket_prefix.*=.\+/rocksdb_cloud_bucket_prefix=dss-/g" ./bootstrap_cnf/eloqdss_server.cnf
# sed -i "s/rocksdb_cloud_bucket_name.*=.\+/rocksdb_cloud_bucket_name=${bucket_name}/g" ./bootstrap_cnf/eloqdss_server.cnf

# sed -i "s/aws_access_key_id.*=.\+/aws_access_key_id=${ELOQ_AWS_ACCESS_KEY_ID}/g" ./storage.cnf
# sed -i "s/aws_secret_key.*=.\+/aws_secret_key=${ELOQ_AWS_SECRET_KEY}/g" ./storage.cnf
# sed -i "s|rocksdb_cloud_s3_endpoint_url.*=.\+|rocksdb_cloud_s3_endpoint_url=${MINIO_ENDPOINT_URL}|g" ./storage.cnf
# sed -i "s/rocksdb_cloud_bucket_prefix.*=.\+/rocksdb_cloud_bucket_prefix=dss-/g" ./storage.cnf
# sed -i "s/rocksdb_cloud_bucket_name.*=.\+/rocksdb_cloud_bucket_name=${bucket_name}/g" ./storage.cnf

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
./mtr --suite=mono_main,mono_basic --testcase-timeout=30 --bootstrap-defaults-file=$WORKSPACE/eloqsql_src/concourse/scripts/mtr_bootstrap.cnf

# Clean up minio buckets
echo "cleaning minio buckets"
set +e
pkill -9 dss_server
rm -rf dss_data
mc rb ${minio_server_alias}/dss-${bucket_name} --force; \
mc rb ${minio_server_alias}/txlog-${bucket_name} --force
set -e

# Start dss_server
echo "starting dss_server"
nohup /home/$current_user/workspace/eloqsql/install/bin/dss_server --config=$WORKSPACE/eloqsql_src/concourse/scripts/dss_server.ini > dss_server.log 2>&1 &
sleep 5

echo "running mono_multi"
./mtr --suite=mono_multi --force --bootstrap-defaults-file=$WORKSPACE/eloqsql_src/concourse/scripts/mtr_multi_bootstrap.cnf

# Clean up minio bucket
# If mtr test failed, it would not be run.
echo "cleaning minio buckets"
set +e
pkill -9 dss_server
rm -rf dss_data
mc rb ${minio_server_alias}/dss-${bucket_name} --force; \
mc rb ${minio_server_alias}/txlog-${bucket_name} --force
set -e
