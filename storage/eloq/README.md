# Compile EloqDB.

## Overview

For Ubuntu 20.04, install-eloqdb-ubuntu.sh can install dependency, compile and install eloqdb in one script.

## Code Stucture


```text
     repo:mariadb(branch eloq-10.6.10)
        |
        |────storage
                |
                |────repo:eloq(eloq_engine_for_mariadb.git)
                         |
                         |
                         |────repo:cass(submodule)
                         |────repo:tx_service(submodule)
                                  |
                                  |────repo:raft_host_manager(raft_host_manager.git)
                                  |────repo:tx-log-protos(submodule)
                                  |────repo:abseil-cpp(submodule)
                         |────repo:log_service(submodule)
                         |────repo:eloq_metrics(submodule)
```

## Prerequisite

### SSH-Key
To access private repositories, ensure you've set up an SSH key.

## Run EloqDB

Run eloqDB with three MariaDB instances.

### 1. Create MariaDB configuration for single-node instance and multi-node instances.

single-node instances
```
#my-cnf0
#This configuration is used in for single node instance and bootstrap.
[mariadb]
plugin_maturity=experimental
datadir={absolute_data0_dir}
lc_messages_dir={absolute_install_dir}/share
max_connections=500
skip-log-bin
port=3317
socket=/tmp/mysqld3317.sock
plugin_load_add=ha_eloq
eloq
eloq_kv_storage=cass
eloq_cass_hosts=127.0.0.1
eloq_cass_user=cassandra
eloq_cass_password=cassandra
eloq_local_ip=127.0.0.1:8000
eloq_ip_list=127.0.0.1:8000
```

multi-node instances
```
#my-cnf1
[mariadb]
plugin_maturity=experimental
datadir={absolute_data1_dir}
lc_messages_dir={absolute_install_dir}/share
max_connections=500
skip-log-bin
port=3317
socket=/tmp/mysqld3317.sock
plugin_load_add=ha_eloq
eloq
eloq_kv_storage=cass
eloq_hosts=127.0.0.1
eloq_user=cassandra
eloq_password=cassandra
eloq_local_ip=127.0.0.1:8000
eloq_ip_list=127.0.0.1:8000,127.0.0.1:8010,127.0.0.1:8020

#my-cnf2
[mariadb]
plugin_maturity=experimental
datadir={absolute_data2_dir}
lc_messages_dir={absolute_install_dir}/share
max_connections=500
skip-log-bin
port=3318
socket=/tmp/mysqld3318.sock
plugin_load_add=ha_eloq
eloq
eloq_kv_storage=cass
eloq_cass_hosts=127.0.0.1
eloq_cass_user=cassandra
eloq_password=cassandra
eloq_local_ip=127.0.0.1:8010
eloq_ip_list=127.0.0.1:8000,127.0.0.1:8010,127.0.0.1:8020

#my-cnf3
[mariadb]
plugin_maturity=experimental
datadir={absolute_data3_dir}
lc_messages_dir={absolute_install_dir}/share
max_connections=500
skip-log-bin
port=3319
socket=/tmp/mysqld3319.sock
plugin_load_add=ha_eloq
eloq
eloq_kv_storage=cass
eloq_cass_hosts=127.0.0.1
eloq_user=cassandra
eloq_password=cassandra
eloql_ip=127.0.0.1:8020
eloq_ip_list=127.0.0.1:8000,127.0.0.1:8010,127.0.0.1:8020
```

Before initialize MariaDB Server, we should start Cassandra server, becasue `mysql_install_db` need install system
tables in Cassandra server.

### 2. Start your Cassandra cluster

```
pushd ~/workspace/apache-cassandra/
./bin/cassandra -f
./bin/cqlsh localhost -u cassandra -p cassandra
```

### 3. Initialize MariaDB instances.

```
absolute_install_dir="/home/mono/workspace/mariadb/install"
absolute_data0_dir="user-defined-path"

${absolute_install_dir}/scripts/mysql_install_db --defaults-file=~/my-config.cnf0 --basedir=${absolute_install_dir} --datadir=${absolute_data0_dir} --plugin-dir=${absolute_install_dir}/lib/plugin

<!-- extra steps for multi-node instances -->

cp -r ${absolute_data0_dir}/mysql ${absolte_data1_dir}
cp -r ${absolute_data0_dir}/sys ${absolte_data1_dir}
cp -r ${absolute_data0_dir}/test ${absolte_data1_dir}
cp -r ${absolute_data0_dir}/performance_schema ${absolte_data1_dir}

cp -r ${absolute_data0_dir}/mysql ${absolte_data2_dir}
cp -r ${absolute_data0_dir}/sys ${absolte_data2_dir}
cp -r ${absolute_data0_dir}/test ${absolte_data2_dir}
cp -r ${absolute_data0_dir}/performance_schema ${absolte_data2_dir}

cp -r ${absolute_data0_dir}/mysql ${absolte_data3_dir}
cp -r ${absolute_data0_dir}/sys ${absolte_data3_dir}
cp -r ${absolute_data0_dir}/test ${absolte_data3_dir}
cp -r ${absolute_data0_dir}/performance_schema ${absolte_data3_dir}
```

### 4. Start and connect to database based on configuratoin

```
# start MariaDB (eloq uses mimalloc to override system default allocator)
env LD_PRELOAD=/usr/local/lib/libmimalloc.so ./bin/mysqld --defaults-file=~/my-config.cnf0 [--debug=d:t:i:F:L:N:o,/tmp/mysqld.trace]

# connect to MariaDB
./bin/mysql -u {user} -p test -S /tmp/mysqld3317.sock
```

### 5. [optional] Create users mono for each MariaDB instances. Note that user information is stilled stored inside each DB
   instance, we should move them into Catalog Service in future and enable to create user on any one instance.

```
CREATE USER 'mono'@'localhost' IDENTIFIED BY 'mono';
```

### 6. Restart MariaDB instances with new configurations.

```
# start MariaDB
./bin/mysqld --defaults-file=~/my-config.cnf1
./bin/mysqld --defaults-file=~/my-config.cnf2
./bin/mysqld --defaults-file=~/my-config.cnf3

# connect to MariaDB1
./bin/mysql -u {user} -p test -S /tmp/mysqld3317.sock
create table t1(i int primary key, j int) engine=eloq;
insert into t1 values(1,3);
insert into t1 values(2,4);

# connect to MariaDB2, it should see tuples in t1.
./bin/mysql -u {user} -p test -S /tmp/mysqld3318.sock
select * from t1;
```

# Configuring GLog for Log File Rotation

To store log outputs into a log file and implement size constraint rotation, you need to specify settings for the log directory, maximum log file size, and optionally, a custom log file name prefix. This configuration helps manage disk space usage effectively by rotating the log files once they reach the specified size limit.

1. **Set the Log Directory**

   Define the directory where log files will be stored. Replace `$PATH_TO_LOG_DIR` with the path to your desired log directory:

   ```shell
   export GLOG_log_dir=$PATH_TO_LOG_DIR
   ```

2. **Configure Maximum Log File Size**

   Set the maximum size of the log files (in MB). The default is 1800 MB. This example sets it to 100 MB:

   ```shell
   export GLOG_max_log_size=100
   ```

3. **Set Log File Name Prefix**

   Customize the log file name prefix by using the `GLOG_log_file_name_prefix` environment variable. This setting is optional; if not set, the default prefix `mysqld.log` is used:

   ```shell
   export GLOG_log_file_name_prefix=mysqld.3317.log
   ```

4. **Run the Application**

   Execute your MySQL server with the specified configuration file and preload the memory allocation library. Adjust the path to your configuration file and the `LD_PRELOAD` library as needed:

   ```shell
   env LD_PRELOAD=/usr/local/lib/libmimalloc.so ./bin/mysqld --defaults-file=~/my-config.cnf
   ```

# Run Regression Test for Eloq.

Please follow test case
Readme: https://github.com/monographdb/eloq_engine_for_mariadb/blob/main/mysql-test/README.md

## Run Eloq on DynamoDB

Eloq also supports DynamoDB as its KV Storage besides Cassandra. The first thing is to
build with one more option `-DWITH_KV_STORAGE=DYNAMODB` when run cmake.
To run Eloq in DynamoDB mode:

1. Add following lines to your mysql config file.

```
eloq_kv_storage=dynamo
# credentials does not matter if you are not running DynamoDB on AWS
eloq_aws_access_key_id=XXXXXXXXXXX
eloqsecret_key=XXXXXXXXXXXXXX
eloq_dynamodb_region=ap-northeast-1
eloq_dynamodb_endpoint='http://127.0.0.1:8050'
```

2. Download [DynamoDB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html). Please make sure the downloaded region matches the region in your config file.

3. Start DynamoDB

```
java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar -sharedDb -port 8050
```

# Run Eloq on BigTable

Eloq also supports BigTable as its KV Storage besides Cassandra. The first thing is to
build with one more option `-DWITH_KV_STORAGE=BIGTABLE` when run cmake.
To run Eloq in BigTable mode:

1. Install google-cloud-sdk and google-cloud-cli referring to [Dockerfile](https://github.com/monographdb/monograph_dockerfile/blob/main/monograph-ci-ubuntu2004/Dockerfile)

2. Add following lines to your mysql config file.

```
eloq_kv_storage=bigtable
eloq_bigtable_project_id='XXXXXXXX'
eloq_bigtable_instance_id='XXXXXXXX'
```

3. Launch BigTable emulator.

```
gcloud beta emulators bigtable start
```

4. Set environment for BigTable before launch MariaDB.
```
$(gcloud beta emulators bigtable env-init)
```


# Configurations of Eloq.
| Item                                   | Datatype or Options                            | Default        | Description                                                                          |
| -------------------------------------- | ---------------------------------------------- | -------------- | ------------------------------------------------------------------------------------ |
| eloq_core_num                          | DataType: Integer Number in range [1,1024].    | 1              | Number of CPU cores                                                                  |
| eloq_node_memory_limit_mb              | DataType: Integer Number in range [1,1000000]. | 8000           | Memory limit per node (MB)                                                           |
| node_log_limit_mb                      | DataType: Integer Number in range [1,1000000]. | 16000          | log limit per node (MB)                                                              |
| eloq_cc_protocol                       | Options: OCC, OccRead, Locking.                | OccRead        | Concurrency control protocol to use                                                  |
| eloq_enable_mvcc                       | Boolean Options: on, off.                      | on             | Wheter enable muliti-versions to accomplish RepeatableRead isolation level           |
| eloq_checkpointer_interval_sec         | DataType: Integer Number in range [1,86400].   | 10             | Interval time of checkpointer.  (Unit: second)                                       |
| eloq_checkpointer_delay_sec            | DataType: Integer Number in range [0,86400].   | 5              | The time which ckpt_ts is less than min lock ts case mvcc is enabled. (Unit: second) |
| eloq_collect_active_tx_ts_interval_sec | DataType: Integer Number in range [0,86400].   | 2              | Interval of collect active tx start timestamp. (Unit: second)                        |
| eloq_local_ip                          | DataType: text of endpoint (ip:port).          | 127.0.0.1:8000 | Endpoint of the local node                                                           |
| eloq_ip_list                           | DataType: text of endpoints.                   | 127.0.0.1:8000 | Endpoints of the nodes in the cluster. （Separate with commas）                      |
| eloq_metrics_port                      | DataType:  Port Number.                        | 18081          | The port on which the metrics_collector is reported.                                 |
| eloq_report_debug_info                 | Boolean Options: on, off.                      | off            | Whether report debug information to client                                           |
| eloq_realtime_sampling                 | Boolean Options: on, off.                      | on             | Whether enable realtime sampling.                                                    |
| eloq_kv_storage                        | Options: cass, dynamo.                         | cass           | Supported Key-Value storage: cassandra or dynamo.                                    |
| eloq_cass_hosts                        | DataType: text of endpoints.                   | 127.0.0.1      | Endpoint of Cassandra                                                                |
| eloq_keyspace_name                     | DataType: text.                                | mono           | Keyspace of KV Storage                                                               |
| eloq_dynamodb_endpoint                 | DataType: text of endpoints.                   | (no default)   | Endpoint override of DynamoDB                                                        |
| eloq_aws_access_key_id                 | DataType: text.                                | (no default)   | AWS SDK secret key id                                                                |
| eloq_aws_secret_key                    | DataType: text.                                | (no default)   | AWS SDK secret key                                                                   |
| eloq_dynamodb_region                   | DataType: text.                                | ap-northeast-1 | Region of the used trable in DynamoDB                                                |
| eloq_bigtable_project_id               | DataType: text.                                | (no default)   | Bigtable project id                                                                  |
| eloq_bigtable_instance_id              | DataType: text.                                | (no default)   | Bigtable instance id                                                                 |
|                                        |                                                |                |                                                                                      |
