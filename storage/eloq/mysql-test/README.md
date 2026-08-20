# Overview
There are three regression test suite for eloq currently:
1. mono_main is inherited from mysql_test/main.
2. mono_multi is used to test multi nodes deployment.
3. mono_basic is used to test basic functionality. New added test cases should be placed in mono_basic instead of mono_main.

# How to run testcase
Create a configuration file used at bootstrap stage for mtr, for example:
```
[mariadb]
plugin_maturity=experimental
plugin_load_add=ha_eloq
eloq
eloq_cass_hosts=127.0.0.1
eloq_core_num=1
eloq_cass_user=cassandra
eloq_cass_password=cassandra
eloq_local_ip=127.0.0.1:8000
eloq_ip_list=127.0.0.1:8000
eloq_keyspace_name=mono_test
```

```
export CASSANDRA_BIN_DIR=your-cassandra-bin-directory
pushd ~/mariadb/bld/mysql-test # step into build directory.
./mtr --suite=mono_main,mono_multi,mono_basic --bootstrap-defaults-file=${BOOTSTRAP_CNF}

To run one test case.
./mtr mono_basic.rnd --bootstrap-defaults-file=${BOOTSTRAP_CNF}

To generate an answer file
./mtr --record mono_basic.rnd --bootstrap-defaults-file=${BOOTSTRAP_CNF}
```

# Run testcase with DynamoDB

Add following lines to your bootstrap config file
```
eloq_kv_storage=dynamo
# credentials does not matter if you are not running DynamoDB on AWS
eloq_aws_access_key_id=XXXXXXXXXXX
eloq_aws_secret_key=XXXXXXXXXXXXXX
eloq_dynamodb_region=ap-northeast-1
eloq_dynamodb_endpoint='http://127.0.0.1:8050'
```

Update mariadb/mysql-test/include/eloq_kv_keyspace.cnf
```diff
-eloq_kv_storage=cass
+eloq_kv_storage=dynamo
```

# Run test case with existing database
mtr command will initialize a new database data directory and then run the test cases, which is hard to debug. Use the following command to run a test case on a debuggable database.

```
/Users/hzhang/workspace/mariadb/build/Debug/client/mysqltest --socket=/tmp/mysqld3316.sock --logdir=./var --test-file=/Users/hzhang/workspace/mariadb/storage/eloq/mysql-test/mono_basic/t/rnd.test -u hzhang --database=test --suite-dir=/Users/hzhang/workspace/mariadb/storage/eloq/mysql-test/mono_basic/ --basedir=/Users/hzhang/workspace/mariadb/mysql-test/
```

1. "mysqltest" is the executed file to run testcase;
2. --socket=/tmp/mysqld3316.sock  point out how to connect mysqld. We can replace it with –host and –port
3.  --logdir=./var To The path to save log. It can use default folder.
4. --test-file=eloq/t/single_node.test  The testcase path.
5. –u, -p user name and password
6. --database=test  Default database.
7.  --suite-dir=… The current suite path, used to find path for –include
8.  --basedir=… The base path of mysql-test, used by include file in base path.
