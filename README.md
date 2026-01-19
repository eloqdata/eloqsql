# EloqSQL  
A MySQL-compatible, high performance, elastic, distributed SQL database.

[![GitHub Stars](https://img.shields.io/github/stars/eloqdata/eloqsql?style=social)](https://github.com/eloqdata/eloqsql/stargazers)
---

## Overview
EloqSQL is a distributed SQL database designed to combine MySQL compatibility with the scalability and performance of modern distributed systems. Built on top of [Data Substrate](https://www.eloqdata.com/blog/2024/08/11/data-substrate), it replaces traditional storage engines like InnoDB with a flexible, distributed and high-performance eloq engine: [Transaction Service](https://github.com/eloqdata/tx_service). It has distributed buffer pool and support **RocksDB**, **RocksDB-Cloud**, and **DynamoDB** as storage.

EloqSQL delivers full ACID transactions, elastic scaling, and efficient resource utilization, making it ideal for demanding workloads.

EloqSQL is forked from MariaDB, and inherit the parser, optimizer and executor from MariaDB to provide a MySQL compatibility. For the difference between MySQL and MariaDB, please refer to [MySQL vs MariaDB](https://www.eloqdata.com/docs/mysql-vs-mariadb).

Explore [EloqSQL](https://www.eloqdata.com/product/eloqsql) website for more details.

This repo contains the code for the plugable MariaDB compatible API layer. See [Architecture](#Architecture) for more details. The API sits on top of EloqData's [Data Substrate](https://github.com/eloqdata/tx_service) modular database foundation and the dependencies are included as submodules.

👉 **Use Cases**: Scalable web applications, e-commerce platforms, real-time data processing — anywhere you need MySQL compatibility **but** demand distributed performance and elasticity.

---

## Key Features

### ⚙️ MySQL Compatibility
Seamlessly integrates with MySQL clients and tools, allowing you to leverage existing SQL workflows while benefiting from a distributed backend.


### 🌐 Distributed Architecture
Supports **multiple writers** and **distributed transactions**, enabling high concurrency and fault tolerance across a cluster.

### 🔄 Elastic Scalability
Independently scales CPU, memory, log, and storage resources. Scales out effortlessly without requiring data sharding, adapting to your workload dynamically.

### 🗃️ Flexible Storage Options
Stores data in high-performance key-value engines like **Cassandra**, **ScyllaDB**, and **DynamoDB**, offering better disk compression ratios than InnoDB. *Save up to 80% on disk storage costs compared to MySQL.*  

Supports **object storage** as tiered storage to reduce costs for cold data.

### 🔥 High-Performance Hot Data
Leverages the scalable [Transaction Service](https://github.com/eloqdata/tx_service) to keep hot data in memory, ensuring low-latency access. Scales the buffer pool dynamically as hot data grows—without moving data on disk.

### 🔒 Full ACID Transactions
Provides robust transaction support with **Read Committed** and **Repeatable Read** isolation levels, ensuring data consistency and reliability.

---

## Architecture

- **Hot Data Management**: Hot data resides in the in-memory `Transaction Service`, which scales independently to handle growing datasets efficiently.
- **Storage Tiering**: Combines key-value stores for active data with cost-effective object storage, optimizing both performance and cost.
- **No Sharding Required**: Unlike traditional distributed databases, EloqSQL scales out naturally without the complexity of sharding.

---

## Run with EloqCtl
EloqCtl is the cluster management tool for EloqSQL.

To deploy an EloqSQL cluster in production, download [EloqCtl](https://www.eloqdata.com/downloadeloqctl) and follow the [deployment guide](https://www.eloqdata.com/eloqsql/quick-start-ha).

---

## Run with Tarball
Download the EloqSQL tarball from the [EloqData website](https://www.eloqdata.com/download).

Follow the [instruction guide](https://www.eloqdata.com/eloqsql/install-from-binary) to set up and run EloqSQL on your local machine.

---

## Build from Source

Follow these steps to build and run EloqSQL from source.

### 1. Install Dependencies
We recommend using our Docker image with pre-installed dependencies for a quick build and run of EloqSQL.

```bash
docker pull eloqdata/eloq-dev-ci-ubuntu2404:latest
```

Or, you can manually run the following script to install dependencies on your local machine (Ubuntu 24.04 example).

```bash
bash scripts/install_dependency_ubuntu2404.sh
```

### 2. Initialize Submodules
Fetch the Transaction Service and its dependencies:

```
git submodule update --init --recursive
```


### 3. Build EloqSQL
Configure and compile with optimized settings:

```bash
mkdir build
cd build
cmake -DCMAKE_INSTALL_PREFIX=${HOME}/install \
      -DCMAKE_BUILD_TYPE=RelWithDebInfo \
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
      -DWITH_ASAN=OFF \
      -DCMAKE_C_FLAGS_RELWITHDEBINFO="-O2 -g -DNDEBUG -DDBUG_OFF -fno-omit-frame-pointer -fno-strict-aliasing" \
      -DCMAKE_CXX_FLAGS_RELWITHDEBINFO="-O2 -g -DNDEBUG -DDBUG_OFF -fno-omit-frame-pointer -fno-strict-aliasing -felide-constructors -Wno-error" \
      -DWITH_DATA_STORE=ELOQDSS_ROCKSDB_CLOUD_S3 \
      ../
cmake --build . --config RelWithDebInfo -j8
cmake --install . --config RelWithDebInfo
```

### 4. Set Up Storage Backend
EloqSQL relies on S3-compatible object storage for durable data. In development you can emulate this by running [MinIO](https://min.io/).

Download and start a MinIO instance:

```bash
wget https://dl.min.io/server/minio/release/linux-amd64/minio
chmod +x minio
./minio server ./data
```

### 5. Configure EloqSQL
EloqSQL now uses **two** configuration files:

1. A MySQL config (`my-config.cnf`) for server/bootstrap settings.
2. A Data Substrate config (`data_substrate.cnf`) that contains data substrate related settings. This file must be in INI format, and MySQL references it through the `eloq_config` system variable.

#### MySQL config (`my-config.cnf`)

```
[mariadb]
plugin_maturity=experimental
max_connections=500
skip-log-bin
thread_stack=16M
port=3316
socket=/tmp/mysqld3316.sock
plugin_load_add=ha_eloq
eloq
eloq_kv_storage=eloqds
eloq_config=${HOME}/data_substrate.cnf
```

> **Note:** `eloq_config` must point to an absolute path that the `mysqld` process can read. Without this file, the Data Substrate initialization fails and the server will not start.

#### Data Substrate config (`data_substrate.cnf`)

```
[local]
tx_ip=127.0.0.1
tx_port=8000
txlog_rocksdb_cloud_s3_endpoint_url=http://127.0.0.1:9000
txlog_rocksdb_cloud_bucket_name=eloqsql
txlog_rocksdb_cloud_bucket_prefix=txlog-
txlog_rocksdb_cloud_region=ap-northeast-1

[store]
aws_access_key_id=minioadmin
aws_secret_key=minioadmin
rocksdb_cloud_s3_endpoint_url=http://127.0.0.1:9000
rocksdb_cloud_bucket_name=eloqsql
rocksdb_cloud_bucket_prefix=dss-
rocksdb_cloud_region=ap-northeast-1
eloq_dss_config_file_path=
```

You can copy `storage/eloq/mysql-test/mono_main/data_substrate.cnf` as a starting point and then set the values to match your environment. Any option related to Data Substrate (TX service, storage backends, AWS credentials, etc.) must live in this INI file so that the `eloq_config` pointer can load it during server startup.

### 6. Bootstrap EloqSQL Node
Initialize the database:

```bash
export INSTALL_DIR=${HOME}/install
export DATA_DIR=${HOME}/eloqdata
${INSTALL_DIR}/scripts/mysql_install_db --defaults-file=${HOME}/my-config.cnf \
                                       --basedir=${INSTALL_DIR} \
                                       --datadir=${DATA_DIR} \
                                       --plugin-dir=${INSTALL_DIR}/lib/plugin
```

### 7. Start EloqSQL Node
Launch the server:

```bash
cd install
${INSTALL_DIR}/bin/mysqld --defaults-file=${HOME}/my-config.cnf --datadir=${DATA_DIR}
```

### 8. Connect to EloqSQL
Use a MySQL client to log in:

```bash
sudo ${INSTALL_DIR}/bin/mysql -u root -S /tmp/mysqld3316.sock
```

---

### 9. Run mtr test locally
MariaDB’s **mysql-test-runner (mtr)** suite still works with EloqSQL. Stop any running server before launching tests so the harness can manage its own instances.

#### 1. mono_basic and mono_main:
Edit `eloqsql/concourse/scripts/mtr_bootstrap.cnf` to point at the shared Data Substrate template. Example:
```ini
[mariadb]

...

eloq_config=/abs/path/to/eloqsql/concourse/scripts/mtr_bootstrap_ds.cnf
```

Then populate `eloqsql/concourse/scripts/mtr_bootstrap_ds.cnf` with your S3/Dynamo/AWS credentials:
```ini
[local]
tx_ip=127.0.0.1
tx_port=8000
txlog_rocksdb_cloud_s3_endpoint_url=http://127.0.0.1:9000
txlog_rocksdb_cloud_bucket_name=eloqsql-mtr-test
txlog_rocksdb_cloud_bucket_prefix=txlog-
txlog_rocksdb_cloud_region=ap-northeast-1

[store]
aws_access_key_id=minioadmin
aws_secret_key=minioadmin
rocksdb_cloud_s3_endpoint_url=http://127.0.0.1:9000
rocksdb_cloud_bucket_name=eloqsql-mtr-test
rocksdb_cloud_bucket_prefix=dss-
rocksdb_cloud_region=ap-northeast-1
```

> Tip: the same values should be copied into `storage/eloq/mysql-test/mono_basic/data_substrate.cnf` and `storage/eloq/mysql-test/mono_main/data_substrate.cnf` so the suite-specific configs stay consistent.

Run mono_basic and mono_main test
```bash
pkill -9 dss_server
rm -rf dss_data

export minio_server_alias="minio_server"
mc alias set ${minio_server_alias} http://127.0.0.1:9000 minioadmin minioadmin

mc rb ${minio_server_alias}/dss-eloqsql-mtr-test --force
mc rb ${minio_server_alias}/txlog-eloqsql-mtr-test --force

build/mysql-test/mtr --clean-txlog-bucket-restart --suite=mono_basic,mono_main --testcase-timeout=30 --bootstrap-defaults-file=concourse/scripts/mtr_bootstrap.cnf

```

#### 2. mono_multi:

Edit `eloqsql/concourse/scripts/mtr_multi_bootstrap.cnf` so that `eloq_config` references `concourse/scripts/mtr_multi_bootstrap_ds.cnf` (same pattern as mono_basic).

Configure `eloqsql/concourse/scripts/mtr_multi_bootstrap_ds.cnf` with the multi-node Data Substrate details:
```ini
[local]
tx_ip=127.0.0.1
tx_port=8000
txlog_rocksdb_cloud_s3_endpoint_url=http://127.0.0.1:9000
txlog_rocksdb_cloud_bucket_name=eloqsql-mtr-test
txlog_rocksdb_cloud_bucket_prefix=txlog-
txlog_rocksdb_cloud_region=ap-northeast-1

[cluster]
tx_ip_port_list=127.0.0.1:8000
eloq_dss_peer_node=localhost:9100

[store]
aws_access_key_id=minioadmin
aws_secret_key=minioadmin
rocksdb_cloud_s3_endpoint_url=http://127.0.0.1:9000
rocksdb_cloud_bucket_name=eloqsql-mtr-test
rocksdb_cloud_bucket_prefix=dss-
rocksdb_cloud_region=ap-northeast-1
```

Edit eloqsql/concourse/scripts/dss_server.ini with the following example settings:
```ini
[local]
ip=localhost
port=9100
data_path=dss_data
event_dispatcher_num=1
#auto_redirect=true

[store]
rocksdb_cloud_bucket_prefix=dss-
rocksdb_cloud_bucket_name=eloqsql-mtr-test
rocksdb_cloud_s3_endpoint_url=http://127.0.0.1:9000
aws_access_key_id=minioadmin
aws_secret_key=minioadmin

```

Each `storage/eloq/mysql-test/mono_multi/data_substrate*.cnf` file also mirrors these values for the individual mysqld instances in the suite.

Run mono_multi test
```bash
pkill -9 dss_server
rm -rf dss_data

export minio_server_alias="minio_server"
mc alias set ${minio_server_alias} http://127.0.0.1:9000 minioadmin minioadmin

mc rb ${minio_server_alias}/dss-eloqsql-mtr-test --force
mc rb ${minio_server_alias}/txlog-eloqsql-mtr-test --force

nohup ${INSTALL_DIR}/bin/dss_server --config=concourse/scripts/dss_server.ini &

build/mysql-test/mtr --clean-txlog-bucket-restart --suite=mono_multi --bootstrap-defaults-file=concourse/scripts/mtr_multi_bootstrap.cnf

```


## Deploy with EloqCtl
To deploy EloqSQL cluster, Please refer to [EloqCtl](https://www.eloqdata.com/eloqsql/cluster-deployment).


---

**Star This Repo ⭐** to Support Our Journey — Every Star Helps Us Reach More Developers!  


