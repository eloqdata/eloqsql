/**
 *    Copyright (C) 2025 EloqData Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the following license:
 *    1. GNU General Public License as published by the Free Software
 *    Foundation; version 2 of the License.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License V2
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */
/* Copyright (c) 2004, 2013, Oracle and/or its affiliates.ws
   Copyright (c) 2010, 2014, SkySQL Ab.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1335  USA */

/**
  @file ha_example.cc

  @brief
  The ha_example engine is a stubbed storage engine for example purposes only;
  it does almost nothing at this point. Its purpose is to provide a source
  code illustration of how to begin writing new storage engines; see also
  storage/example/ha_example.h.

  Additionally, this file includes an example of a daemon plugin which does
  nothing at all - absolutely nothing, even less than example storage engine.
  But it shows that one dll/so can contain more than one plugin.

  @details
  ha_example will let you create/open/delete tables, but
  nothing further (for example, indexes are not supported nor can data
  be stored in the table). It also provides new status (example_func_example)
  and system (example_ulong_var and example_enum_var) variables.

  Use this example as a template for implementing the same functionality in
  your own storage engine. You can enable the example storage engine in your
  build by doing the following during your build process:<br> ./configure
  --with-example-storage-engine

  Once this is done, MySQL will let you create tables with:<br>
  CREATE TABLE <table name> (...) ENGINE=EXAMPLE;

  The example storage engine is set up to use table locks. It
  implements an example "SHARE" that is inserted into a hash by table
  name. You can use this to store information of state that any
  example handler object will be able to see when it is using that
  table.

  Please read the object definition in ha_example.h before reading the rest
  of this file.

  @note
  When you create an EXAMPLE table, the MySQL Server creates a table .frm
  (format) file in the database directory, using the table name as the file
  name as is customary with MySQL. No other files are created. To get an idea
  of what occurs, here is an example select that would do a scan of an entire
  table:

  @code
  ha_example::store_lock
  ha_example::external_lock
  ha_example::info
  ha_example::rnd_init
  ha_example::extra
  ENUM HA_EXTRA_CACHE        Cache record in HA_rrnd()
  ha_example::rnd_next
  ha_example::rnd_next
  ha_example::rnd_next
  ha_example::rnd_next
  ha_example::rnd_next
  ha_example::rnd_next
  ha_example::rnd_next
  ha_example::rnd_next
  ha_example::rnd_next
  ha_example::extra
  ENUM HA_EXTRA_NO_CACHE     End caching of records (def)
  ha_example::external_lock
  ha_example::extra
  ENUM HA_EXTRA_RESET        Reset database to after open
  @endcode

  Here you see that the example storage engine has 9 rows called before
  rnd_next signals that it has reached the end of its data. Also note that
  the table in question was already opened; had it not been open, a call to
  ha_example::open() would also have been necessary. Calls to
  ha_example::extra() are hints as to what will be occuring to the request.

  A Longer Example can be found called the "Skeleton Engine" which can be
  found on TangentOrg. It has both an engine and a full build environment
  for building a pluggable storage engine.

  Happy coding!<br>
    -Brian
*/

#include "mysql/plugin.h"
#include "mysql_metrics.h"
#include "mysql_version.h"
#include <climits>
#include <cstdint>
#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation // gcc: Class implementation
#endif

#define MYSQL_SERVER 1
#define DONT_DEFINE_VOID

#include <iostream>
#include <string>
#include <memory>
#include <unordered_map>
#include <map>
#include <algorithm>
#include <tuple>
#include <filesystem>

#include "my_global.h"
#include "thr_lock.h" /* THR_LOCK, THR_LOCK_DATA */
#include "my_base.h"  /* ha_rows */
#include "sql_class.h"
#include "mysqld_error.h"
#include "sql_string.h"
#include "sql_table.h"
#include "my_bitmap.h"
#include "log.h"
#include "key.h"

#include "log_wrapper.h"
#include "eloq_i_s.h"
#include "eloq_catalog_factory.h"
#include "eloq_catalog_name.h"
#include "eloq_system_handler.h"
#include "eloq_errors.h"
#include "tx_util.h"
// #include "eloq_tests.hpp"
#include "ha_eloq.h"
#include "tx_service/include/sequences/sequences.h"
#include "slice.h"

#include "store_handler/kv_store.h"

#if (defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3) ||                     \
     defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_GCS) ||                    \
     defined(DATA_STORE_TYPE_ELOQDSS_ELOQSTORE))
#define ELOQDS 1
#endif

#if defined(DATA_STORE_TYPE_CASSANDRA)
#include "store_handler/cass_handler.h"
#elif defined(DATA_STORE_TYPE_DYNAMODB)
#include "store_handler/dynamo_handler.h"
#elif defined(DATA_STORE_TYPE_BIGTABLE)
#include "store_handler/bigtable_handler.h"

#elif ELOQDS
#include "store_handler/eloq_data_store_service/data_store_service.h"
#include "store_handler/eloq_data_store_service/data_store_service_config.h"
#include "store_handler/data_store_service_client.h"
#if (defined(ROCKSDB_CLOUD_FS_TYPE) &&                                        \
     (ROCKSDB_CLOUD_FS_TYPE == ROCKSDB_CLOUD_FS_TYPE_S3 ||                    \
      ROCKSDB_CLOUD_FS_TYPE == ROCKSDB_CLOUD_FS_TYPE_GCS))
#include "store_handler/eloq_data_store_service/rocksdb_cloud_data_store_factory.h"
#include "store_handler/eloq_data_store_service/rocksdb_config.h"
#elif defined(DATA_STORE_TYPE_ELOQDSS_ELOQSTORE)
#include "store_handler/eloq_data_store_service/eloq_store_data_store_factory.h"
#endif
#else
#endif

#include "metrics_registry_impl.h"

#include "tx_service/include/constants.h"
#include "tx_service/include/statistics.h"
#include "tx_service/include/type.h"
#include "tx_service/include/tx_execution.h"
#include "tx_service/include/tx_service.h"
#include "tx_service/include/tx_service_metrics.h"
#include "tx_service/include/tx_request.h"
#include "tx_service/include/util.h"
#include "log_server.h"
#include "log_service_metrics.h"
#include "log_utils.h"

#if defined(USE_ROCKSDB_LOG_STATE) && defined(WITH_ROCKSDB_CLOUD)
#include "log_service/include/rocksdb_cloud_config.h"
#endif

// Don't put this include after sql_class.h include, it will cause compile
// error
#if (defined(DATA_STORE_TYPE_DYNAMODB) ||                                     \
     (defined(USE_ROCKSDB_LOG_STATE) &&                                       \
      (WITH_ROCKSDB_CLOUD == CS_TYPE_S3)) ||                                  \
     defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3))
#include <aws/core/Aws.h>
#endif

#define DEFAULT_SCAN_TUPLE_SIZE 128
// eloq_debug_set prefix "+d,eloq;"
#define ELOQ_DEBUG_SET_PREFIX_LEN 8
// eloq_debug_set prefix "+d,eloq_test;"
#define ELOQ_TEST_DEBUG_SET_PREFIX_LEN 13
// debug_set prefix "+d,"
#define DEBUG_SET_PREFIX_LEN 3

using namespace MyEloq;
using namespace txservice;

extern my_bool opt_bootstrap; // Defined in Mariadb context.
extern std::function<void(int)> terminate_hook;

// Internal MySQL APIs not exposed in any header.
/**
 * Check whether the current statement is PREPARE statement.
 */
extern bool thd_is_stmt_prepare(const THD *thd);
extern int16_t thd_get_group_id(const THD *thd);
extern std::pair<const std::function<void()> *, const std::function<void()> *>
thd_get_coro_functors(const THD *thd);
extern const std::function<void()> *thd_get_long_resume_func(const THD *thd);
extern std::function<
    std::pair<std::function<void()>, std::function<void(int16_t)>>(int16_t)>
    get_tx_service_functors;

static handler *eloq_create_handler(handlerton *hton, TABLE_SHARE *table,
                                    MEM_ROOT *mem_root);

handlerton *eloq_hton;

static ulong eloq_kv_storage= 0;
static char *eloq_local_ip= nullptr;
static char *eloq_ip_list= nullptr;
static char *eloq_standby_ip_list= nullptr;
static char *eloq_voter_ip_list= nullptr;
static char *eloq_hm_ip= nullptr;
static char *eloq_hm_bin_path= nullptr;
static char *eloq_cluster_config_file= nullptr;
static char *eloq_cass_hosts= nullptr;
static int eloq_cass_port= 9042;
static int eloq_cass_queue_size_io= 300000;
static char *eloq_cass_user= nullptr;
static char *eloq_cass_password= nullptr;
static char *eloq_keyspace_name= nullptr;
static char *eloq_cass_keyspace_class= nullptr;
static char *eloq_cass_replication_factor= nullptr;
static my_bool eloq_high_compression_ratio= false;
static char *eloq_dynamodb_endpoint= nullptr;
static char *eloq_aws_access_key_id= nullptr;
static char *eloq_aws_secret_key= nullptr;
static char *eloq_dynamodb_region= nullptr;
static char *eloq_bigtable_project_id= nullptr;
static char *eloq_bigtable_instance_id= nullptr;
static char *eloq_insert_semantic= nullptr;
static char *eloq_auto_increment= nullptr;
static char *eloq_invalidate_cache_once= nullptr;
static unsigned int eloq_core_num= 1;
static ulong srv_enum_var= 0;
static ulong srv_ulong_var= 0;
static double srv_double_var= 0;
static unsigned int eloq_checkpointer_interval_sec= 10;
static unsigned int eloq_checkpointer_delay_sec= 5;
static unsigned int eloq_collect_active_tx_ts_interval_sec= 2;
// range split worker number
static unsigned int eloq_range_split_worker_num= 0;
static unsigned int eloq_bthread_worker_num= 0;
static unsigned int eloq_logserver_rocksdb_scan_thread_num= 0;
// eloq_realtime_sampling cannot be changed at runtime until now, because
// broadcast configuration has not been supported.
static my_bool eloq_realtime_sampling= true;
static my_bool eloq_ddl_skip_kv= false;
static my_bool eloq_skip_redo_log= false;
static my_bool eloq_scan_skip_kv= false;
static my_bool eloq_random_scan_sort= false;
static my_bool eloq_use_key_cache= false;
static my_bool eloq_report_debug_info= false;
// memory limit default 8GB.
static unsigned int eloq_node_memory_limit_mb= 8000;
// log limit default 16GB since rocksdb is used as default engine of log
// service, it's not sensitive to log size. But we still keep the logic of
// triggering checkpoint when log size reach the limit.
static unsigned int eloq_node_log_limit_mb= 16000;
static my_bool eloq_enable_mvcc= true;
static unsigned int eloq_metrics_port= 18081;
static unsigned int eloq_deadlock_interval_sec= 10;
// If not 0, will create a hook function that will close all braft connections
// when mysqld received crash single.
static int eloq_signal_monitor= 0;
// tx log service list
static char *eloq_txlog_rocksdb_storage_path= nullptr;
static char *eloq_txlog_service_list= nullptr;
static unsigned int eloq_txlog_group_replica_num= 3;
static ulong eloq_partition_type= 0;

// metrics collection
static std::unique_ptr<metrics::MetricsRegistry> metrics_registry= nullptr;
static my_bool eloq_enable_metrics= false;

// mysql metrics
static my_bool eloq_enable_mysql_tx_metrics= true;
static my_bool eloq_enable_mysql_dml_metrics= true;

// tx_service metrics
static my_bool eloq_enable_tx_metrics= true;
static my_bool eloq_enable_cache_hit_rate= true;
static my_bool eloq_enable_kv_metrics= true;
static my_bool eloq_enable_busy_round_metrics= true;
static my_bool eloq_enable_memory_usage= true;
static my_bool eloq_enable_remote_request_metrics= true;
static unsigned long long eloq_collect_memory_usage_round= 10000;
static unsigned long long eloq_collect_tx_duration_round= 1;
static unsigned long long eloq_busy_round_threshold= 10;

// log_service metrics
static my_bool eloq_enable_log_service_metrics= false;

// log server/rocksdb/cloud
static char *eloq_txlog_rocksdb_cloud_bucket_name= nullptr;
static char *eloq_txlog_rocksdb_cloud_bucket_prefix= nullptr;
static char *eloq_txlog_rocksdb_cloud_region= nullptr;
static char *eloq_txlog_rocksdb_cloud_endpoint_url= nullptr;
static char *eloq_txlog_rocksdb_cloud_sst_file_cache_size= nullptr;
static int eloq_txlog_rocksdb_cloud_sst_file_cache_num_shard_bits= 5;
static char *eloq_txlog_rocksdb_target_file_size_base= nullptr;
static char *eloq_txlog_rocksdb_sst_files_size_limit= nullptr;
static unsigned int eloq_txlog_rocksdb_cloud_ready_timeout= 10;
static unsigned int eloq_txlog_rocksdb_cloud_file_deletion_delay= 3600;
static unsigned int eloq_node_group_replica_num= 3;
static unsigned int eloq_logserver_snapshot_interval= 600;
static unsigned int eloq_txlog_rocksdb_cloud_in_mem_log_size_high_watermark=
    50 * 10000;
static unsigned int eloq_txlog_rocksdb_max_write_buffer_number= 8;
static unsigned int eloq_txlog_rocksdb_max_background_jobs= 8;

static my_bool eloq_enable_txlog_request_checkpoint= true;
static unsigned int eloq_check_replay_log_size_interval_sec= 10;
static char *eloq_notify_checkpointer_threshold_size= nullptr;

// data_store_service
static char *eloq_dss_config_file_path= nullptr;
static char *eloq_dss_peer_node= nullptr;
static char *eloq_dss_rocksdb_cloud_bucket_name= nullptr;
static char *eloq_dss_rocksdb_cloud_bucket_prefix= nullptr;
static char *eloq_dss_rocksdb_cloud_region= nullptr;
static char *eloq_dss_rocksdb_cloud_endpoint_url= nullptr;
static char *eloq_dss_rocksdb_cloud_sst_file_cache_size= nullptr;
static int eloq_dss_rocksdb_cloud_sst_file_cache_num_shard_bits= 5;
static char *eloq_dss_rocksdb_target_file_size_base= nullptr;
static unsigned int eloq_dss_rocksdb_cloud_file_deletion_delay= 3600;
static unsigned int eloq_dss_rocksdb_max_write_buffer_number= 8;
static unsigned int eloq_dss_rocksdb_max_background_jobs= 8;
#if defined(DATA_STORE_TYPE_ELOQDSS_ELOQSTORE)
static unsigned int eloq_eloqstore_worker_num= 1;
static char *eloq_eloqstore_data_path= nullptr;
static unsigned int eloq_eloqstore_open_files_limit= 1024;
#endif

const char *enum_var_names[]= {"e1", "e2", NullS};
const char *kv_storage_names[]= {"cass", "dynamo", "bigtable", "eloqds",
                                 NullS};
const char *partition_names[]= {"Hash", "Range", NullS};

#define KV_CASS 0
#define KV_DYNAMO 1
#define KV_BIGTABLE 2
#define KV_ELOQDS 3

static my_bool eloq_enable_heap_defragment= false;
static my_bool eloq_kickout_data_for_test= false;

#define DIRTY_KEY_ID_BEGIN 16

static MYSQL_SYSVAR_STR(local_ip, eloq_local_ip,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
                        "IP address of the local node", nullptr, nullptr,
                        "127.0.0.1:8000");

static MYSQL_SYSVAR_STR(ip_list, eloq_ip_list,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
                        "IP addresses of the nodes in the cluster", nullptr,
                        nullptr, "127.0.0.1:8000");
static MYSQL_SYSVAR_STR(standby_ip_list, eloq_standby_ip_list,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
                        "IP addresses of the standby nodes in the cluster",
                        nullptr, nullptr, "");
static MYSQL_SYSVAR_STR(voter_ip_list, eloq_voter_ip_list,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
                        "IP addresses of the voter nodes in the cluster",
                        nullptr, nullptr, "");
static MYSQL_SYSVAR_STR(hm_ip, eloq_hm_ip,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
                        "IP addresses of the host manager", nullptr, nullptr,
                        "");

static MYSQL_SYSVAR_STR(hm_bin_path, eloq_hm_bin_path,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
                        "Path to host manager binary path.", nullptr, nullptr,
                        "");

static MYSQL_SYSVAR_STR(cluster_config_file, eloq_cluster_config_file,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
                        "Path to cluster config file.", nullptr, nullptr, "");

static MYSQL_SYSVAR_STR(cass_hosts, eloq_cass_hosts,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
                        "Contact points of Cassandra", nullptr, nullptr,
                        "127.0.0.1");

static MYSQL_SYSVAR_INT(cass_port, eloq_cass_port, PLUGIN_VAR_RQCMDARG,
                        "Port of Cassandra", nullptr, nullptr, 9042, 0,
                        INT_MAX, 0);

static MYSQL_SYSVAR_INT(cass_queue_size_io, eloq_cass_queue_size_io,
                        PLUGIN_VAR_RQCMDARG,
                        "Queue_size_io of Cassandra client", nullptr, nullptr,
                        300000, 0, INT_MAX, 0);

static MYSQL_SYSVAR_STR(keyspace_name, eloq_keyspace_name,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
                        "Keyspace of KV Storage", nullptr, nullptr, "mono");

static MYSQL_SYSVAR_STR(cass_keyspace_class, eloq_cass_keyspace_class,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
                        "Keyspace class of Cassandra", nullptr, nullptr,
                        "SimpleStrategy");

static MYSQL_SYSVAR_STR(cass_replication_factor, eloq_cass_replication_factor,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
                        "Keyspace replication factor of Cassandra", nullptr,
                        nullptr, "1");

static MYSQL_SYSVAR_STR(cass_user, eloq_cass_user,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
                        "Cassandra username", nullptr, nullptr, "cassandra");

static MYSQL_SYSVAR_STR(cass_password, eloq_cass_password,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
                        "Cassandra password", nullptr, nullptr, "cassandra");

static MYSQL_SYSVAR_BOOL(high_compression_ratio, eloq_high_compression_ratio,
                         PLUGIN_VAR_RQCMDARG,
                         "Cassandra enable high compression ratio", nullptr,
                         nullptr, FALSE);

static MYSQL_SYSVAR_UINT(core_num, eloq_core_num, PLUGIN_VAR_RQCMDARG,
                         "Number of CPU cores", NULL, NULL, 1, 1, 1024, 1);

static MYSQL_SYSVAR_UINT(range_split_worker_num, eloq_range_split_worker_num,
                         PLUGIN_VAR_RQCMDARG, "Number of range split worker",
                         NULL, NULL, 0, 0, 1024, 0);

static MYSQL_SYSVAR_UINT(bthread_worker_num, eloq_bthread_worker_num,
                         PLUGIN_VAR_RQCMDARG,
                         "Number of bthread worker threads", NULL, NULL, 0, 0,
                         1024, 0);

static MYSQL_SYSVAR_UINT(logserver_rocksdb_scan_thread_num,
                         eloq_logserver_rocksdb_scan_thread_num,
                         PLUGIN_VAR_RQCMDARG, "Number of rocksdb scan threads",
                         NULL, NULL, 1, 1, 1024, 1);

// global metrics
static MYSQL_SYSVAR_BOOL(enable_metrics, eloq_enable_metrics,
                         PLUGIN_VAR_RQCMDARG,
                         "When enabled, creates metric registry", NULL, NULL,
                         FALSE);
// mysql metrics
static MYSQL_SYSVAR_BOOL(
    enable_mysql_tx_metrics, eloq_enable_mysql_tx_metrics, PLUGIN_VAR_RQCMDARG,
    "Enables or disables the collection of transaction metrics in MySQL.",
    NULL, NULL, TRUE);

static MYSQL_SYSVAR_BOOL(
    enable_mysql_dml_metrics, eloq_enable_mysql_dml_metrics,
    PLUGIN_VAR_RQCMDARG,
    "Enables or disables the collection of DML metrics in MySQL. When set to "
    "ON, the plugin will track DML operation metrics such as SELECT, INSERT, "
    "UPDATE, DELETE, etc.",
    NULL, NULL, TRUE);

// tx_service metrics
static MYSQL_SYSVAR_BOOL(
    enable_tx_metrics, eloq_enable_tx_metrics, PLUGIN_VAR_RQCMDARG,
    "Enable or disable transaction metrics for `tx_service` side.", NULL, NULL,
    TRUE);

static MYSQL_SYSVAR_BOOL(enable_cache_hit_rate, eloq_enable_cache_hit_rate,
                         PLUGIN_VAR_RQCMDARG,
                         "Enable or disable cache hit rate metrics", NULL,
                         NULL, TRUE);

static MYSQL_SYSVAR_BOOL(enable_busy_round_metrics,
                         eloq_enable_busy_round_metrics, PLUGIN_VAR_RQCMDARG,
                         "When enabled, collects process transaction requests "
                         "latency, and process cc "
                         "requests latency, cc queue length",
                         NULL, NULL, TRUE);

static MYSQL_SYSVAR_BOOL(enable_memory_usage, eloq_enable_memory_usage,
                         PLUGIN_VAR_RQCMDARG,
                         "Enable or disable memory usage metrics", NULL, NULL,
                         TRUE);

static MYSQL_SYSVAR_BOOL(
    enable_remote_request_metrics, eloq_enable_remote_request_metrics,
    PLUGIN_VAR_RQCMDARG,
    "Enable or disable remote request metrics for `tx_service` side.", NULL,
    NULL, TRUE);

static MYSQL_SYSVAR_BOOL(
    enable_kv_metrics, eloq_enable_kv_metrics, PLUGIN_VAR_RQCMDARG,
    "Enable or disable KV store metrics for `tx_service` side.", NULL, NULL,
    TRUE);

static MYSQL_SYSVAR_ULONGLONG(
    busy_round_threshold, eloq_busy_round_threshold, PLUGIN_VAR_RQCMDARG,
    "If CC queue length >= `threshold` (default: 10), then the "
    "RunOneRound is busy",
    NULL, NULL, 10, 1, ULLONG_MAX, 1);

static MYSQL_SYSVAR_ULONGLONG(
    collect_memory_usage_round, eloq_collect_memory_usage_round,
    PLUGIN_VAR_RQCMDARG,
    "Interval of collecting memory usage. Only "
    "works when enable memory usage (default: 10000)",
    NULL, NULL, 10000, 0, ULLONG_MAX, 1);

static MYSQL_SYSVAR_ULONGLONG(
    collect_tx_duration_round, eloq_collect_tx_duration_round,
    PLUGIN_VAR_RQCMDARG,
    "Interval of collecting transaction duration. Only "
    "works when enable tx metrics  (default: 1)",
    NULL, NULL, 1, 0, ULLONG_MAX, 1);

// log_service metrics
static MYSQL_SYSVAR_BOOL(enable_log_service_metrics,
                         eloq_enable_log_service_metrics, PLUGIN_VAR_RQCMDARG,
                         "When enabled, collects log_service metrics.", NULL,
                         NULL, FALSE);

const char *cc_protocol_names[]= {"OCC", "OccRead", "Locking", NullS};

TYPELIB cc_protocol_typelib= {array_elements(cc_protocol_names) - 1,
                              "cc_protocol_typelib", cc_protocol_names, NULL};

static MYSQL_THDVAR_ENUM(
    cc_protocol,                                          // name
    PLUGIN_VAR_RQCMDARG,                                  // opt
    "Concurrency control protocol.(OCC|OccRead|Locking)", // comment
    NULL,                                                 // check
    NULL,                                                 // update
    1,                                                    // default(OccRead)
    &cc_protocol_typelib);                                // typelib

//(name, varname, opt, comment, check, update, def, min, max, blk)

TYPELIB enum_var_typelib= {array_elements(enum_var_names) - 1,
                           "enum_var_typelib", enum_var_names, NULL};

static MYSQL_SYSVAR_ENUM(enum_var,                       // name
                         srv_enum_var,                   // varname
                         PLUGIN_VAR_RQCMDARG,            // opt
                         "Sample ENUM system variable.", // comment
                         NULL,                           // check
                         NULL,                           // update
                         0,                              // def
                         &enum_var_typelib);             // typelib

TYPELIB kv_storage_typelib= {array_elements(kv_storage_names) - 1,
                             "kv_storage_typelib", kv_storage_names, NULL};

static MYSQL_SYSVAR_ENUM(kv_storage,                    // name
                         eloq_kv_storage,               // varname
                         PLUGIN_VAR_RQCMDARG,           // opt
                         "Supported Key-Value storage", // comment
                         NULL,                          // check
                         NULL,                          // update
                         0,                             // def
                         &kv_storage_typelib);          // typelib

static MYSQL_SYSVAR_STR(aws_access_key_id, eloq_aws_access_key_id,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
                        "aws sdk access key id", nullptr, nullptr, "");

static MYSQL_SYSVAR_STR(aws_secret_key, eloq_aws_secret_key,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
                        "AWS SDK secret key", nullptr, nullptr, "");

static MYSQL_SYSVAR_STR(dynamodb_endpoint, eloq_dynamodb_endpoint,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
                        "Endpoint override of DynamoDB", nullptr, nullptr, "");

static MYSQL_SYSVAR_STR(dynamodb_region, eloq_dynamodb_region,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
                        "Region of the used trable in DynamoDB", nullptr,
                        nullptr, "ap-northeast-1");

static MYSQL_SYSVAR_STR(bigtable_project_id, eloq_bigtable_project_id,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
                        "Project id of BigTable", nullptr, nullptr, "");

static MYSQL_SYSVAR_STR(bigtable_instance_id, eloq_bigtable_instance_id,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
                        "Instance id of BigTable", nullptr, nullptr, "");

static MYSQL_SYSVAR_STR(insert_semantic, eloq_insert_semantic,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
                        "Insert semantic: insert or upsert", nullptr, nullptr,
                        "insert");

static MYSQL_THDVAR_INT(int_var, PLUGIN_VAR_RQCMDARG, "-1..1", NULL, NULL, 0,
                        -1, 1, 0);

static MYSQL_SYSVAR_ULONG(ulong_var, srv_ulong_var, PLUGIN_VAR_RQCMDARG,
                          "0..1000", NULL, NULL, 8, 0, 1000, 0);

static MYSQL_SYSVAR_DOUBLE(double_var, srv_double_var, PLUGIN_VAR_RQCMDARG,
                           "0.500000..1000.500000", NULL, NULL, 8.5, 0.5,
                           1000.5,
                           0); // reserved always 0

static MYSQL_THDVAR_DOUBLE(double_thdvar, PLUGIN_VAR_RQCMDARG,
                           "0.500000..1000.500000", NULL, NULL, 8.5, 0.5,
                           1000.5, 0);

static MYSQL_THDVAR_ULONG(varopt_default, PLUGIN_VAR_RQCMDARG,
                          "default value of the VAROPT table option", NULL,
                          NULL, 5, 0, 100, 0);

static MYSQL_SYSVAR_UINT(checkpointer_interval_sec,
                         eloq_checkpointer_interval_sec,
                         PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                         "Interval of checkpointer(s)", NULL, NULL, 10, 1,
                         86400, 1);

static MYSQL_SYSVAR_UINT(
    checkpointer_delay_sec, eloq_checkpointer_delay_sec, PLUGIN_VAR_RQCMDARG,
    "The time(second) which ckpt_ts is less than min lock ts", NULL, NULL, 5,
    0, 86400, 1);

static MYSQL_SYSVAR_UINT(collect_active_tx_ts_interval_sec,
                         eloq_collect_active_tx_ts_interval_sec,
                         PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                         "Interval of collect active tx start timestamp(s)",
                         NULL, NULL, 2, 1, 86400, 1);
static MYSQL_SYSVAR_BOOL(
    realtime_sampling, eloq_realtime_sampling,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "Whether enable realtime sampling. If disable it, user may need execute "
    "analyze command at some time. Different from Innodb, Eloq never "
    "analyze table automatically.",
    NULL, NULL, TRUE);

static MYSQL_SYSVAR_BOOL(
    ddl_skip_kv, eloq_ddl_skip_kv, PLUGIN_VAR_NOCMDARG | PLUGIN_VAR_READONLY,
    "Skip create or drop table on kv store, only used to speed up test case",
    NULL, NULL, FALSE);
static MYSQL_SYSVAR_BOOL(skip_redo_log, eloq_skip_redo_log,
                         PLUGIN_VAR_NOCMDARG | PLUGIN_VAR_READONLY,
                         "Skip write redo log in tx_service", NULL, NULL,
                         FALSE);
static MYSQL_SYSVAR_BOOL(
    use_key_cache, eloq_use_key_cache,
    PLUGIN_VAR_NOCMDARG | PLUGIN_VAR_READONLY,
    "Use key cache in primary key to avoid kv read if key does not exists.",
    NULL, NULL, FALSE);
static MYSQL_SYSVAR_BOOL(report_debug_info, eloq_report_debug_info,
                         PLUGIN_VAR_NOCMDARG,
                         "When enabled, report debug information to client",
                         NULL, NULL, FALSE);
static MYSQL_SYSVAR_UINT(node_memory_limit_mb, eloq_node_memory_limit_mb,
                         PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                         "memory limit per node (MB)", NULL, NULL, 8000, 1,
                         1000000, 1);
static MYSQL_SYSVAR_UINT(node_log_limit_mb, eloq_node_log_limit_mb,
                         PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                         "log limit per node (MB)", NULL, NULL, 16000, 1,
                         1000000, 1);

static MYSQL_SYSVAR_UINT(
    metrics_port,                                 // name
    eloq_metrics_port,                            // varname
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,    // opt
    "The port on which the metrics_collector is " // comment
    "reported. Default is 18081",                 // comment
    NULL,                                         // check
    NULL,                                         // update
    18081,                                        // def
    8080, 65535, 1);

static MYSQL_SYSVAR_BOOL(
    enable_mvcc, eloq_enable_mvcc, PLUGIN_VAR_NOCMDARG,
    "When enabled, use muliti-versions. Repeatable Read "
    "isolation level will be converted to Snapshot isolation level",
    NULL, NULL, TRUE);

TYPELIB partition_typelib= {array_elements(partition_names) - 1,
                            "partition_typelib", partition_names, NULL};

static MYSQL_SYSVAR_ENUM(partition_type,                            // name
                         eloq_partition_type,                       // varname
                         PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY, // opt
                         "Partition type (Hash|Range)",             // comment
                         NULL,                                      // check
                         NULL,                                      // update
                         0,                   // default(Hash)
                         &partition_typelib); // typelib

static MYSQL_SYSVAR_BOOL(enable_heap_defragment, eloq_enable_heap_defragment,
                         PLUGIN_VAR_NOCMDARG,
                         "When enabled, report debug information to client",
                         NULL, NULL, FALSE);

int auto_increment_var_check(MYSQL_THD thd, struct st_mysql_sys_var *var,
                             void *save, struct st_mysql_value *value)
{
  char buff[STRING_BUFFER_USUAL_SIZE];
  const char *str;
  int length;

  length= sizeof(buff);
  if ((str= value->val_str(value, buff, &length)))
    str= thd->strmake(str, length);
  *(const char **) save= str;

  struct st_item_value_holder : public st_mysql_value
  {
    Item *item;
  };
  st_item_value_holder *mvalue= (st_item_value_holder *) value;
  LEX_CSTRING &pstr= mvalue->item->name;

  return txservice::Sequences::UpdateAutoIncrement(
      std::string(pstr.str, pstr.length),
      std::string(thd->db.str, thd->db.length), thd_get_coro_functors(thd),
      thd_get_group_id(thd));
}

static MYSQL_SYSVAR_STR(auto_increment, eloq_auto_increment,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
                        "auto increment parameters", auto_increment_var_check,
                        NULL, "");

static MYSQL_SYSVAR_BOOL(kickout_data_for_test, eloq_kickout_data_for_test,
                         PLUGIN_VAR_NOCMDARG, "Clean data for test", NULL,
                         NULL, FALSE);

static int
invalidate_table_cache(MYSQL_THD thd,
                       const std::vector<TableName> &invalidate_tables);
int invalidate_cache_once_var_check(MYSQL_THD thd,
                                    struct st_mysql_sys_var *var, void *save,
                                    struct st_mysql_value *value)
{
  char buff[STRING_BUFFER_USUAL_SIZE];
  const char *str;
  int length;

  length= sizeof(buff);
  if ((str= value->val_str(value, buff, &length)))
    str= thd->strmake(str, length);
  *(const char **) save= str;

  struct st_item_value_holder : public st_mysql_value
  {
    Item *item;
  };
  st_item_value_holder *mvalue= (st_item_value_holder *) value;

  // Input format: t1,t2,db3.t3,...
  LEX_CSTRING &pstr= mvalue->item->name;

  std::vector<TableName> invalidate_tables;
  char *tok, *ptr;
  for (tok= strtok_r(const_cast<char *>(pstr.str), ",", &ptr); tok;
       tok= strtok_r(nullptr, ",", &ptr))
  {
    std::string tx_table_name, db, table;
    std::string_view token(tok);
    size_t pos= token.find(".");
    if (pos != std::string_view::npos)
    {
      db.assign(token.data(), pos);
      table.assign(token.data() + pos + 1, token.size() - pos - 1);
    }
    else
    {
      db.assign(thd->db.str, thd->db.length);
      table.assign(token);
    }

    if (db.empty() || table.empty())
    {
      sql_print_information("Empty database/tablename");
      my_error(HA_ERR_ELOQ_CATALOG_NAME_ERROR, MYF(0));
      return -1;
    }
    if (lex_string_eq(&INFORMATION_SCHEMA_NAME, db.data(), db.size()))
    {
      sql_print_information("Tables under %s are not eloq table");
      my_error(HA_ERR_ELOQ_CATALOG_NAME_ERROR, MYF(0));
      return -1;
    }

    tx_table_name.append("./").append(db).append("/").append(table);
    invalidate_tables.emplace_back(tx_table_name, TableType::Primary,
                                   txservice::TableEngine::EloqSql);
  }

  return invalidate_table_cache(thd, invalidate_tables);
}

static MYSQL_SYSVAR_STR(
    invalidate_cache_once, eloq_invalidate_cache_once,
    PLUGIN_VAR_NOCMDOPT | PLUGIN_VAR_NOCMDARG | PLUGIN_VAR_MEMALLOC,
    "Evict caches for given tables. Mainly used after physical importing.",
    invalidate_cache_once_var_check, NULL, "");

static MYSQL_SYSVAR_BOOL(
    scan_skip_kv, eloq_scan_skip_kv, PLUGIN_VAR_NOCMDARG | PLUGIN_VAR_READONLY,
    "Skip access kv store for scan request, only used to speed up test case",
    NULL, NULL, FALSE);

static MYSQL_SYSVAR_BOOL(random_scan_sort, eloq_random_scan_sort,
                         PLUGIN_VAR_OPCMDARG,
                         "Sort output result when executing random scan, only "
                         "used to run test case",
                         NULL, NULL, FALSE);

static MYSQL_SYSVAR_UINT(deadlock_interval_sec, eloq_deadlock_interval_sec,
                         PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                         "Interval of dead lock check(s)", NULL, NULL, 300, 1,
                         3600, 1);
static MYSQL_SYSVAR_STR(txlog_rocksdb_storage_path,
                        eloq_txlog_rocksdb_storage_path,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
                        "The path for tx log service rocksdb storage", nullptr,
                        nullptr, "");
static MYSQL_SYSVAR_STR(txlog_service_list, eloq_txlog_service_list,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
                        "Ip address of the tx log service node", nullptr,
                        nullptr, "");

static MYSQL_SYSVAR_UINT(txlog_group_replica_num, eloq_txlog_group_replica_num,
                         PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                         "Replicate number of tx log group", NULL, NULL, 3, 1,
                         10, 1);

static MYSQL_SYSVAR_INT(signal_monitor, eloq_signal_monitor,
                        PLUGIN_VAR_RQCMDARG, "Monitor mysql crash signal",
                        nullptr, nullptr, 0, 0, INT_MAX, 0);

static MYSQL_SYSVAR_STR(txlog_rocksdb_cloud_bucket_name,
                        eloq_txlog_rocksdb_cloud_bucket_name,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
                        "TxLog RocksDB cloud bucket name", NULL, NULL, "");
static MYSQL_SYSVAR_STR(txlog_rocksdb_cloud_bucket_prefix,
                        eloq_txlog_rocksdb_cloud_bucket_prefix,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
                        "TxLog RocksDB cloud bucket prefix", NULL, NULL, "");
static MYSQL_SYSVAR_STR(txlog_rocksdb_cloud_region,
                        eloq_txlog_rocksdb_cloud_region,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
                        "TxLog RocksDB cloud region", NULL, NULL, "");
static MYSQL_SYSVAR_STR(txlog_rocksdb_cloud_endpoint_url,
                        eloq_txlog_rocksdb_cloud_endpoint_url,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
                        "TxLog RocksDB Cloud endpoint URL", NULL, NULL, "");
static MYSQL_SYSVAR_STR(txlog_rocksdb_cloud_sst_file_cache_size,
                        eloq_txlog_rocksdb_cloud_sst_file_cache_size,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
                        "TxLog RocksDB Cloud SST file cache size", NULL, NULL,
                        "10GB");
static MYSQL_SYSVAR_INT(txlog_rocksdb_cloud_sst_file_cache_num_shard_bits,
                        eloq_txlog_rocksdb_cloud_sst_file_cache_num_shard_bits,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
                        "TxLog RocksDB Cloud SST file cache num shard bits",
                        NULL, NULL, 5, 0, 30, 1);
static MYSQL_SYSVAR_STR(txlog_rocksdb_target_file_size_base,
                        eloq_txlog_rocksdb_target_file_size_base,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
                        "TxLog RocksDB target file size", NULL, NULL, "64MB");
static MYSQL_SYSVAR_STR(txlog_rocksdb_sst_files_size_limit,
                        eloq_txlog_rocksdb_sst_files_size_limit,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
                        "TxLog RocksDB sst files size limit", NULL, NULL,
                        "500MB");
static MYSQL_SYSVAR_UINT(txlog_rocksdb_cloud_ready_timeout,
                         eloq_txlog_rocksdb_cloud_ready_timeout,
                         PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
                         "TxLog RocksDB Cloud becomes ready timeout(seconds)",
                         NULL, NULL, 10, 1, 120, 1);
static MYSQL_SYSVAR_UINT(txlog_rocksdb_cloud_file_deletion_delay,
                         eloq_txlog_rocksdb_cloud_file_deletion_delay,
                         PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
                         "TxLog RocksDB Cloud becomes ready timeout", NULL,
                         NULL, 60, 1, 3600, 1);
static MYSQL_SYSVAR_UINT(node_group_replica_num, eloq_node_group_replica_num,
                         PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                         "Replicate number of node group(Max: 9)", NULL, NULL,
                         3, 1, 9, 1);
static MYSQL_SYSVAR_UINT(logserver_snapshot_interval,
                         eloq_logserver_snapshot_interval,
                         PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                         "Logserver log state snapshot interval", NULL, NULL,
                         600, 10, 7200, 1);
static MYSQL_SYSVAR_UINT(
    txlog_rocksdb_cloud_in_mem_log_high_watermark,
    eloq_txlog_rocksdb_cloud_in_mem_log_size_high_watermark,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "TxLog RocksDB Cloud in memory log queue high watermark", NULL, NULL,
    50 * 10000, 10000, 1000 * 10000, 1);
static MYSQL_SYSVAR_UINT(txlog_rocksdb_max_write_buffer_number,
                         eloq_txlog_rocksdb_max_write_buffer_number,
                         PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                         "TxLog RocksDB max write buffer number", NULL, NULL,
                         8, 4, 100, 1);
static MYSQL_SYSVAR_UINT(txlog_rocksdb_max_background_jobs,
                         eloq_txlog_rocksdb_max_background_jobs,
                         PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                         "TxLog RocksDB max background jobs", NULL, NULL, 8, 4,
                         100, 1);

static MYSQL_SYSVAR_BOOL(enable_txlog_request_checkpoint,
                         eloq_enable_txlog_request_checkpoint,
                         PLUGIN_VAR_NOCMDARG | PLUGIN_VAR_READONLY,
                         "Enable txlog server sending checkpoint requests "
                         "when the criteria are met.",
                         NULL, NULL, TRUE);

static MYSQL_SYSVAR_UINT(check_replay_log_size_interval_sec,
                         eloq_check_replay_log_size_interval_sec,
                         PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                         "The interval for checking replay log size.", NULL,
                         NULL, 10, 10, UINT_MAX, 1);

static MYSQL_SYSVAR_STR(
    notify_checkpointer_threshold_size,
    eloq_notify_checkpointer_threshold_size,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
    "When the replay log size reaches this threshold, the "
    "txlog server sends a checkpoint request to tx_service.",
    NULL, NULL, "1GB");

static MYSQL_SYSVAR_STR(dss_config_file_path, eloq_dss_config_file_path,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
                        "EloqDataStoreService config file path", NULL, NULL,
                        "");
static MYSQL_SYSVAR_STR(dss_peer_node, eloq_dss_peer_node,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
                        "EloqDataStoreService peer node endpoint", NULL, NULL,
                        "");

static MYSQL_SYSVAR_STR(dss_rocksdb_cloud_bucket_name,
                        eloq_dss_rocksdb_cloud_bucket_name,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
                        "EloqDataStoreService RocksDB cloud bucket name", NULL,
                        NULL, "");
static MYSQL_SYSVAR_STR(dss_rocksdb_cloud_bucket_prefix,
                        eloq_dss_rocksdb_cloud_bucket_prefix,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
                        "EloqDataStoreService RocksDB cloud bucket prefix",
                        NULL, NULL, "");
static MYSQL_SYSVAR_STR(dss_rocksdb_cloud_region,
                        eloq_dss_rocksdb_cloud_region,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
                        "EloqDataStoreService RocksDB cloud region", NULL,
                        NULL, "");
static MYSQL_SYSVAR_STR(dss_rocksdb_cloud_endpoint_url,
                        eloq_dss_rocksdb_cloud_endpoint_url,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
                        "EloqDataStoreService RocksDB Cloud endpoint URL",
                        NULL, NULL, "");
static MYSQL_SYSVAR_STR(
    dss_rocksdb_cloud_sst_file_cache_size,
    eloq_dss_rocksdb_cloud_sst_file_cache_size,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
    "EloqDataStoreService RocksDB Cloud SST file cache size", NULL, NULL,
    "10GB");
static MYSQL_SYSVAR_INT(
    dss_rocksdb_cloud_sst_file_cache_num_shard_bits,
    eloq_dss_rocksdb_cloud_sst_file_cache_num_shard_bits,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
    "EloqDataStoreService RocksDB Cloud SST file cache num shard bits", NULL,
    NULL, 5, 0, 30, 1);
static MYSQL_SYSVAR_STR(dss_rocksdb_target_file_size_base,
                        eloq_dss_rocksdb_target_file_size_base,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
                        "EloqDataStoreService RocksDB target file size", NULL,
                        NULL, "64MB");
static MYSQL_SYSVAR_UINT(
    dss_rocksdb_cloud_file_deletion_delay,
    eloq_dss_rocksdb_cloud_file_deletion_delay,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
    "EloqDataStoreService RocksDB Cloud becomes ready timeout", NULL, NULL, 60,
    1, 3600, 1);
static MYSQL_SYSVAR_UINT(
    dss_rocksdb_max_write_buffer_number,
    eloq_dss_rocksdb_max_write_buffer_number,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "EloqDataStoreService RocksDB max write buffer number", NULL, NULL, 8, 4,
    100, 1);
static MYSQL_SYSVAR_UINT(dss_rocksdb_max_background_jobs,
                         eloq_dss_rocksdb_max_background_jobs,
                         PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                         "EloqDataStoreService RocksDB max background jobs",
                         NULL, NULL, 8, 4, 100, 1);
#if defined(DATA_STORE_TYPE_ELOQDSS_ELOQSTORE)
static MYSQL_SYSVAR_UINT(eloqstore_worker_num, eloq_eloqstore_worker_num,
                         PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                         "EloqStore server worker num.", NULL, NULL, 1, 1,
                         UINT_MAX, 1);
static MYSQL_SYSVAR_STR(
    eloqstore_data_path, eloq_eloqstore_data_path,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY | PLUGIN_VAR_MEMALLOC,
    "The data path of the EloqStore (use memory store if empty).", NULL, NULL,
    "");
static MYSQL_SYSVAR_UINT(eloqstore_open_files_limit,
                         eloq_eloqstore_open_files_limit,
                         PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                         "EloqStore server max open files.", NULL, NULL, 1024,
                         1, UINT_MAX, 1);
#endif

static struct st_mysql_sys_var *eloq_system_variables[]= {
    MYSQL_SYSVAR(local_ip),
    MYSQL_SYSVAR(ip_list),
    MYSQL_SYSVAR(standby_ip_list),
    MYSQL_SYSVAR(voter_ip_list),
    MYSQL_SYSVAR(hm_ip),
    MYSQL_SYSVAR(cass_hosts),
    MYSQL_SYSVAR(cass_port),
    MYSQL_SYSVAR(cass_queue_size_io),
    MYSQL_SYSVAR(keyspace_name),
    MYSQL_SYSVAR(cass_keyspace_class),
    MYSQL_SYSVAR(cass_replication_factor),
    MYSQL_SYSVAR(cass_user),
    MYSQL_SYSVAR(cass_password),
    MYSQL_SYSVAR(high_compression_ratio),
    MYSQL_SYSVAR(dynamodb_endpoint),
    MYSQL_SYSVAR(core_num),
    MYSQL_SYSVAR(cc_protocol),
    MYSQL_SYSVAR(enum_var),
    MYSQL_SYSVAR(kv_storage),
    MYSQL_SYSVAR(dynamodb_region),
    MYSQL_SYSVAR(aws_access_key_id),
    MYSQL_SYSVAR(aws_secret_key),
    MYSQL_SYSVAR(bigtable_project_id),
    MYSQL_SYSVAR(bigtable_instance_id),
    MYSQL_SYSVAR(ulong_var),
    MYSQL_SYSVAR(int_var),
    MYSQL_SYSVAR(double_var),
    MYSQL_SYSVAR(double_thdvar),
    MYSQL_SYSVAR(varopt_default),
    MYSQL_SYSVAR(checkpointer_interval_sec),
    MYSQL_SYSVAR(checkpointer_delay_sec),
    MYSQL_SYSVAR(collect_active_tx_ts_interval_sec),
    MYSQL_SYSVAR(realtime_sampling),
    MYSQL_SYSVAR(insert_semantic),
    MYSQL_SYSVAR(ddl_skip_kv),
    MYSQL_SYSVAR(skip_redo_log),
    MYSQL_SYSVAR(use_key_cache),
    MYSQL_SYSVAR(auto_increment),
    MYSQL_SYSVAR(kickout_data_for_test),
    MYSQL_SYSVAR(invalidate_cache_once),
    MYSQL_SYSVAR(scan_skip_kv),
    MYSQL_SYSVAR(random_scan_sort),
    MYSQL_SYSVAR(report_debug_info),
    MYSQL_SYSVAR(node_memory_limit_mb),
    MYSQL_SYSVAR(node_log_limit_mb),
    MYSQL_SYSVAR(enable_mvcc),
    MYSQL_SYSVAR(metrics_port),
    MYSQL_SYSVAR(deadlock_interval_sec),
    MYSQL_SYSVAR(txlog_rocksdb_storage_path),
    MYSQL_SYSVAR(txlog_service_list),
    MYSQL_SYSVAR(txlog_group_replica_num),
    MYSQL_SYSVAR(signal_monitor),
    MYSQL_SYSVAR(partition_type),
    MYSQL_SYSVAR(enable_metrics),
    MYSQL_SYSVAR(enable_mysql_tx_metrics),
    MYSQL_SYSVAR(enable_mysql_dml_metrics),
    MYSQL_SYSVAR(enable_tx_metrics),
    MYSQL_SYSVAR(enable_cache_hit_rate),
    MYSQL_SYSVAR(enable_memory_usage),
    MYSQL_SYSVAR(enable_busy_round_metrics),
    MYSQL_SYSVAR(enable_remote_request_metrics),
    MYSQL_SYSVAR(enable_kv_metrics),
    MYSQL_SYSVAR(busy_round_threshold),
    MYSQL_SYSVAR(collect_memory_usage_round),
    MYSQL_SYSVAR(collect_tx_duration_round),
    MYSQL_SYSVAR(enable_log_service_metrics),
    MYSQL_SYSVAR(txlog_rocksdb_cloud_bucket_name),
    MYSQL_SYSVAR(txlog_rocksdb_cloud_bucket_prefix),
    MYSQL_SYSVAR(txlog_rocksdb_cloud_region),
    MYSQL_SYSVAR(txlog_rocksdb_cloud_sst_file_cache_size),
    MYSQL_SYSVAR(txlog_rocksdb_cloud_sst_file_cache_num_shard_bits),
    MYSQL_SYSVAR(txlog_rocksdb_cloud_endpoint_url),
    MYSQL_SYSVAR(txlog_rocksdb_target_file_size_base),
    MYSQL_SYSVAR(txlog_rocksdb_sst_files_size_limit),
    MYSQL_SYSVAR(txlog_rocksdb_cloud_ready_timeout),
    MYSQL_SYSVAR(txlog_rocksdb_cloud_file_deletion_delay),
    MYSQL_SYSVAR(node_group_replica_num),
    MYSQL_SYSVAR(logserver_snapshot_interval),
    MYSQL_SYSVAR(logserver_rocksdb_scan_thread_num),
    MYSQL_SYSVAR(txlog_rocksdb_cloud_in_mem_log_high_watermark),
    MYSQL_SYSVAR(txlog_rocksdb_max_write_buffer_number),
    MYSQL_SYSVAR(txlog_rocksdb_max_background_jobs),
    MYSQL_SYSVAR(enable_txlog_request_checkpoint),
    MYSQL_SYSVAR(check_replay_log_size_interval_sec),
    MYSQL_SYSVAR(notify_checkpointer_threshold_size),
    MYSQL_SYSVAR(range_split_worker_num),
    MYSQL_SYSVAR(bthread_worker_num),
    MYSQL_SYSVAR(hm_bin_path),
    MYSQL_SYSVAR(cluster_config_file),
    MYSQL_SYSVAR(enable_heap_defragment),
    MYSQL_SYSVAR(dss_config_file_path),
    MYSQL_SYSVAR(dss_peer_node),
    MYSQL_SYSVAR(dss_rocksdb_cloud_bucket_name),
    MYSQL_SYSVAR(dss_rocksdb_cloud_bucket_prefix),
    MYSQL_SYSVAR(dss_rocksdb_cloud_region),
    MYSQL_SYSVAR(dss_rocksdb_cloud_sst_file_cache_size),
    MYSQL_SYSVAR(dss_rocksdb_cloud_sst_file_cache_num_shard_bits),
    MYSQL_SYSVAR(dss_rocksdb_cloud_endpoint_url),
    MYSQL_SYSVAR(dss_rocksdb_target_file_size_base),
    MYSQL_SYSVAR(dss_rocksdb_cloud_file_deletion_delay),
    MYSQL_SYSVAR(dss_rocksdb_max_write_buffer_number),
    MYSQL_SYSVAR(dss_rocksdb_max_background_jobs),
#if defined(DATA_STORE_TYPE_ELOQDSS_ELOQSTORE)
    MYSQL_SYSVAR(eloqstore_worker_num),
    MYSQL_SYSVAR(eloqstore_data_path),
    MYSQL_SYSVAR(eloqstore_open_files_limit),
#endif
    NULL};

/**
  Structure for CREATE TABLE options (table options).
  It needs to be called ha_table_option_struct.

  The option values can be specified in the CREATE TABLE at the end:
  CREATE TABLE ( ... ) *here*
*/

struct ha_table_option_struct
{
  const char *strparam;
  ulonglong ullparam;
  uint enumparam;
  bool boolparam;
  ulonglong varparam;
};

/**
  Structure for CREATE TABLE options (field options).
  It needs to be called ha_field_option_struct.

  The option values can be specified in the CREATE TABLE per field:
  CREATE TABLE ( field ... *here*, ... )
*/

struct ha_field_option_struct
{
  const char *complex_param_to_parse_it_in_engine;
};

/*
  no example here, but index options can be declared similarly
  using the ha_index_option_struct structure.

  Their values can be specified in the CREATE TABLE per index:
  CREATE TABLE ( field ..., .., INDEX .... *here*, ... )
*/

ha_create_table_option eloq_table_option_list[]= {
    /*
      one numeric option, with the default of UINT_MAX32, valid
      range of values 0..UINT_MAX32, and a "block size" of 10
      (any value must be divisible by 10).
    */
    HA_TOPTION_NUMBER("ULL", ullparam, UINT_MAX32, 0, UINT_MAX32, 10),
    /*
      one option that takes an arbitrary string
    */
    HA_TOPTION_STRING("STR", strparam),
    /*
      one enum option. a valid values are strings ONE and TWO.
      A default value is 0, that is "one".
    */
    HA_TOPTION_ENUM("one_or_two", enumparam, "one,two", 0),
    /*
      one boolean option, the valid values are YES/NO, ON/OFF, 1/0.
      The default is 1, that is true, yes, on.
    */
    HA_TOPTION_BOOL("YESNO", boolparam, 1),
    /*
      one option defined by the system variable. The type, the range, or
      a list of allowed values is the same as for the system variable.
    */
    HA_TOPTION_SYSVAR("VAROPT", varparam, varopt_default),

    HA_TOPTION_END};

ha_create_table_option eloq_field_option_list[]= {
    /*
      If the engine wants something more complex than a string, number, enum,
      or boolean - for example a list - it needs to specify the option
      as a string and parse it internally.
    */
    HA_FOPTION_STRING("COMPLEX", complex_param_to_parse_it_in_engine),
    HA_FOPTION_END};

/**
  @brief
  Function we use in the creation of our hash to get key.
*/

#ifdef HAVE_PSI_INTERFACE
PSI_mutex_key ex_key_mutex_Eloq_share_mutex, mono_mem_cmp_space_mutex_key,
    mono_collation_data_mutex_key;

PSI_mutex_info all_eloq_mutexes[]= {
    {&ex_key_mutex_Eloq_share_mutex, "Eloq_share::mutex", 0},
    {&mono_mem_cmp_space_mutex_key, "collation space char data init",
     PSI_FLAG_GLOBAL},
    {&mono_collation_data_mutex_key, "collation data init", PSI_FLAG_GLOBAL}};

static void init_eloq_psi_keys()
{
  const char *category= "eloq";
  int count;

  count= array_elements(all_eloq_mutexes);
  mysql_mutex_register(category, all_eloq_mutexes, count);
}
#else
static void init_eloq_psi_keys() {}
#endif

/**
  @brief
  If frm_error() is called then we will use this to determine
  the file extensions that exist for the storage engine. This is also
  used by the default rename_table and delete_table method in
  handler.cc and by the default discover_many method.

  For engines that have two file name extensions (separate meta/index file
  and data file), the order of elements is relevant. First element of engine
  file name extensions array should be meta/index file extention. Second
  element - data file extention. This order is assumed by
  prepare_for_repair() when REPAIR TABLE ... USE_FRM is issued.

  @see
  rename_table method in handler.cc and
  delete_table method in handler.cc
*/

static const char *ha_eloq_exts[]= {NullS};

uint32_t node_id= 0; // node id of itself
std::unique_ptr<store::DataStoreHandler> storage_hd= nullptr;

#if ELOQDS
std::unique_ptr<EloqDS::DataStoreService> data_store_service_;
#endif

static std::unique_ptr<TxService> tx_service= nullptr;
static std::unique_ptr<::txlog::LogServer> txlog_server= nullptr;
static MariaCatalogFactory maria_catalog_factory{};
// insert semantic will check unique of primary key, while upsert semantic will
// update the duplicated entry.
bool is_insert_semantic_= true;
// unsupported sql command
static const std::unordered_map<enum_sql_command, std::string>
    eloq_unsupported_command= {{SQLCOM_ALTER_TABLE, "ALTER TABLE"},
                               {SQLCOM_CHECK, "CHECK TABLE"},
                               {SQLCOM_CHECKSUM, "CHECKSUM TABLE"},
                               {SQLCOM_LOCK_TABLES, "LOCK TABLE[S]"},
                               {SQLCOM_OPTIMIZE, "OPTIMIZE TABLE"},
                               {SQLCOM_RENAME_TABLE, "RENAME TABLE"},
                               {SQLCOM_REPAIR, "REPAIR TABLE"},
                               {SQLCOM_TRUNCATE, "TRUNCATE"},
                               {SQLCOM_UNLOCK_TABLES, "UNLOCK TABLES"}};

static MyEloqTx *get_myeloq_tx(THD *const thd)
{
  return reinterpret_cast<MyEloqTx *>(mysql::thd_get_ha_data(thd, eloq_hton));
}

Eloq_share::Eloq_share()
{
  thr_lock_init(&lock);
  mysql_mutex_init(ex_key_mutex_Eloq_share_mutex, &mutex, MY_MUTEX_INIT_FAST);
}

/**
Converts an TxService error code to a mysql error code and set error messages.
@return MySQL error code */
static int convert_tx_error(txservice::TxErrorCode err_code,
                            const char *err_msg= nullptr)
{
  if (err_msg == nullptr)
  {
    err_msg= TxErrorMessage(err_code).c_str();
  }

  switch (err_code)
  {
  case TxErrorCode::NO_ERROR:
    return 0;
  case TxErrorCode::DEAD_LOCK_ABORT:
    my_error(HA_ERR_LOCK_DEADLOCK, MYF(0), err_msg);
    return HA_ERR_LOCK_DEADLOCK;
  case TxErrorCode::OCC_BREAK_REPEATABLE_READ:
  case TxErrorCode::SI_R4W_ERR_KEY_WAS_UPDATED:
    my_error(HA_ERR_ELOQ_RECORD_WAS_UPDATED, MYF(0), err_msg);
    return HA_ERR_ELOQ_RECORD_WAS_UPDATED;
  case TxErrorCode::READ_WRITE_CONFLICT:
    my_error(HA_ERR_ELOQ_RW_CONFLICT, MYF(0), err_msg);
    return HA_ERR_ELOQ_RW_CONFLICT;
  case TxErrorCode::WRITE_WRITE_CONFLICT:
    my_error(HA_ERR_ELOQ_WW_CONFLICT, MYF(0), err_msg);
    return HA_ERR_ELOQ_WW_CONFLICT;
  case TxErrorCode::TX_INIT_FAIL:
    my_error(HA_ERR_ELOQ_START_TRANSACTION_FAILED, MYF(0), err_msg);
    return HA_ERR_ELOQ_START_TRANSACTION_FAILED;
  case TxErrorCode::DATA_STORE_ERROR:
    my_error(HA_ERR_ELOQ_DATA_STORE_ERROR, MYF(0), err_msg);
    return HA_ERR_ELOQ_DATA_STORE_ERROR;
  case TxErrorCode::INTERNAL_ERR_TIMEOUT:
    my_error(HA_ERR_LOCK_WAIT_TIMEOUT, MYF(0), err_msg);
    return HA_ERR_LOCK_WAIT_TIMEOUT;
  default:
    my_error(HA_ERR_ELOQ_DEFAULT_ERROR, MYF(0), err_msg);
    return HA_ERR_ELOQ_DEFAULT_ERROR;
  };
}

/**
 * @brief Fetch system variables (cc_protocol) and map to
 * txservice::CcProtocol.
 * Case (thd != null), fetch value of session level cc_protocol.
 * Case (thd == null), fetch value of global level cc_protocol.
 *
 */
static txservice::CcProtocol fetch_tx_cc_protocol(THD *const thd)
{
  switch (THDVAR(thd, cc_protocol))
  {
  case 0:
    return CcProtocol::OCC;
  case 1:
    return CcProtocol::OccRead;
  case 2:
    return CcProtocol::Locking;
  default:
    return CcProtocol::OCC;
  };
}

// fetch isolation level by thd and map to txservice::IsolationLevel
static bool fetch_tx_isolation_level(THD *const thd, CcProtocol cc_protocol,
                                     IsolationLevel &iso_level)
{
  enum_tx_isolation mysql_iso_level= (enum_tx_isolation) thd_tx_isolation(thd);
  switch (mysql_iso_level)
  {
  case ISO_READ_UNCOMMITTED:
    my_error(HA_ERR_ELOQ_ISO_LEVEL_UNSUPPORT_ERROR, MYF(0),
             "read uncommitted");
    return false; // eloqdb not support ISO_READ_UNCOMMITTED
  case ISO_READ_COMMITTED:
    iso_level= IsolationLevel::ReadCommitted;
    return true;
  case ISO_REPEATABLE_READ:
    // Isolation level RepeatableRead and SnapshotIsolation are similar, but
    // not same in all cases.
    // For example, RepeatableRead has phantom read, but SnapshotIsolation can
    // protect it. RepeatableRead forbids write on the same data, but
    // SnapshotIsolation allows. While MySQL only supply RepeatableRead
    // isolation level interface, and implement it with SI actually. But
    // eloq can distinguish there two different isolation levels. To make
    // it consistent with Mysql, we convert user set RepeatableRead isolation
    // level to Snapshot Isolation if mvcc is enabled in Eloq engine.
    if (eloq_enable_mvcc)
    {
      iso_level= IsolationLevel::Snapshot;
    }
    else
    {
      iso_level= IsolationLevel::RepeatableRead;
    }
    return true;
  case ISO_SERIALIZABLE:
    iso_level= IsolationLevel::Serializable;
    return true;
  default:
    my_error(HA_ERR_ELOQ_ISO_LEVEL_UNSUPPORT_ERROR, MYF(0), "-");
    return false;
  }

  return false;
}

static inline void eloq_register_tx(handlerton *const hton, THD *const thd,
                                    MyEloqTx *const tx)
{
  DBUG_ASSERT(tx != nullptr);

  trans_register_ha(thd, FALSE, eloq_hton, 0);
  if (mysql::thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN))
  {
    // tx->start_stmt();
    trans_register_ha(thd, TRUE, eloq_hton, 0);
  }
}

static bool get_or_create_myeloq_tx(THD *const thd, MyEloqTx **my_tx,
                                    bool *exists= nullptr,
                                    bool start_now= false)
{
  *my_tx= get_myeloq_tx(thd);

  if (*my_tx == nullptr || (*my_tx)->Txm() == nullptr)
  {
    txservice::CcProtocol cc_proto= fetch_tx_cc_protocol(thd);
    IsolationLevel iso_level= IsolationLevel::ReadCommitted;
    if (!fetch_tx_isolation_level(thd, cc_proto, iso_level))
    {
      return false;
    }
    if (exists)
      *exists= false;

    if (*my_tx == nullptr)
    {
      *my_tx= new MyEloqTx();
      mysql::thd_set_ha_data(thd, eloq_hton, *my_tx);
    }

    int16_t thd_group_id= thd_get_group_id(thd);
    auto [yield_func, resume_func]= thd_get_coro_functors(thd);
    txservice::TransactionExecution *txm=
        txservice::NewTxInit(tx_service.get(), iso_level, cc_proto, UINT32_MAX,
                             thd_group_id, start_now, yield_func, resume_func);
    (*my_tx)->Reset(txm, thd);

    if (txm == nullptr)
    {
      return false;
    }
    else
    {
      eloq_register_tx(eloq_hton, thd, *my_tx);
      return true;
    }
  }
  else
  {
    if (exists)
      *exists= true;
    return true;
  }
}

// A help function to drop table, outside of MariaDB context.
static void drop_table(const std::string &table_name_str)
{
  TransactionExecution *txm= NewTxInit(
      tx_service.get(), IsolationLevel::Serializable, CcProtocol::Locking);
  if (txm)
  {
    // All tables in mariadb_tables are of TableType::Primary
    std::string_view table_name_sv{table_name_str};
    TableName table_name{table_name_sv, txservice::TableType::Primary,
                         txservice::TableEngine::EloqSql};
    CatalogKey catalog_key(table_name);
    TxKey catalog_tx_key{&catalog_key};
    CatalogRecord catalog_rec;
    ReadTxRequest read_req(&txservice::catalog_ccm_name, 0, &catalog_tx_key,
                           &catalog_rec, true, false, true);
    bool exists= false;
    TxErrorCode err= TxReadCatalog(txm, read_req, exists);
    if (err != TxErrorCode::NO_ERROR)
    {
      AbortTxRequest abort_req;
      txm->Execute(&abort_req);
      abort_req.Wait();
      return;
    }

    if (exists)
    {
      std::string empty_image{""};
      UpsertTableTxRequest upsert_table_req(
          &table_name, &catalog_rec.Schema()->SchemaImage(),
          catalog_rec.SchemaTs(), &empty_image, OperationType::DropTable);
      txm->Execute(&upsert_table_req);
      upsert_table_req.Wait();
      UpsertResult rst= upsert_table_req.Result();
      if (rst == UpsertResult::Failed)
      {
        sql_print_error("Drop temporary table '%s' failed at launch.",
                        table_name.StringView());
      }
      else if (rst == UpsertResult::Unverified)
      {
        sql_print_warning("Breaked during droping temporary table '%s' and \
                          will force to continue in log recover. Please verify \
                          it in following time",
                          table_name.StringView());
      }
    }

    auto [success, commit_err]= txservice::CommitTx(txm);
    if (!success)
    {
      sql_print_error("Drop temporary table '%s' failed at launch.",
                      table_name.StringView());
    }
  }
  else
  {
    sql_print_error("Failed to start transaction at eloq engine."
                    "Txservice is not ready yet.");
  }
}

static void drop_orphan_tmp_tables()
{
  std::vector<std::string> table_names;
  if (storage_hd->DiscoverAllTableNames(table_names))
  {
    for (const std::string &table_name_str : table_names)
    {
      if (is_tmp_table_of(table_name_str, node_id))
      {
        drop_table(table_name_str);
      }
    }
  }
}

static handler *eloq_create_handler(handlerton *hton, TABLE_SHARE *table,
                                    MEM_ROOT *mem_root)
{
  return new (mem_root) ha_eloq(hton, table);
}

static int eloq_commit(handlerton *hton, THD *thd, bool commit_tx)
{
  // external_lock(F_UNLCK) is called after this function is called
  DBUG_ENTER_FUNC();

  MyEloqTx *my_tx= get_myeloq_tx(thd);

  if (my_tx != nullptr)
  {
    if (commit_tx ||
        (!mysql::thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)))
    {
      bool success= my_tx->Commit();
      if (!success)
      {
        DBUG_RETURN(HA_ERR_ELOQ_COMMIT_FAILED);
      }
    }
    else
    {
      // This is for savepoint of a nested tx.
    }
  }

  DBUG_RETURN(0);
}

static int eloq_rollback(handlerton *hton, THD *thd, bool rollback_tx)
{
  DBUG_ENTER_FUNC();

  MyEloqTx *my_tx= get_myeloq_tx(thd);

  if (my_tx != nullptr && my_tx->Txm() != nullptr)
  {
    if (rollback_tx)
    {
      // A rollback statement is issued.
      my_tx->Abort();
      DBUG_RETURN(0);
    }
    else
    {
      // Either a statement with AUTOCOMMIT=1 is being rolled back (because of
      // some error), or a statement inside a transaction is rolled back.
      // we also need send abort request to transaction service here.
      // TODO: this code could be refactored, since no matter rollback_tx is
      // true or not, we need send abort request as well.
      my_tx->Abort();
      DBUG_RETURN(0);
    }
  }

  DBUG_RETURN(0);
}

// Table discovery interface.
// Eloq table information is stored in tx service, so each time, we need
// to send a FetchCatalogRequest to check whether table exists and get the
// table catalog information(original the content of frm) if table exists.
static int eloq_discover_table(handlerton *hton, THD *thd, TABLE_SHARE *share)
{
  DBUG_ENTER_FUNC();

  share->error= OPEN_FRM_OPEN_ERROR;

  {
    // TODO: Whether should have flags HTON_CAN_RECREATE for truncate or not?
    enum_sql_command sqlcom= enum_sql_command(thd_sql_command(thd));
    switch (sqlcom)
    {
    case SQLCOM_TRUNCATE:
    case SQLCOM_LOCK_TABLES:
    case SQLCOM_UNLOCK_TABLES:
    case SQLCOM_RENAME_TABLE:
    case SQLCOM_OPTIMIZE:
    case SQLCOM_CHECK:
    case SQLCOM_REPAIR:
      my_error(ER_CHECK_NOT_IMPLEMENTED, MYF(0),
               eloq_unsupported_command.find(sqlcom)->second.c_str());
      share->error= OPEN_FRM_READ_ERROR;
      DBUG_RETURN(HA_ERR_WRONG_COMMAND);
    default:
      break;
    }
  }

  // 'host' is a legacy table which doesn't have frm file.
  // it has been removed in later version of mariadb.
  if (strcmp(share->table_name.str, "host") == 0)
  {
    DBUG_RETURN(HA_ERR_NO_SUCH_TABLE);
  }

  MyEloqTx *my_tx= nullptr;
  bool tx_exists= false;
  if (!get_or_create_myeloq_tx(thd, &my_tx, &tx_exists))
  {
    my_error(HA_ERR_ELOQ_START_TRANSACTION_FAILED, MYF(0));
    DBUG_RETURN(HA_ERR_ELOQ_START_TRANSACTION_FAILED);
  }

  bool exists= false;

  // Use table file path name instead of the table name,
  // keep consistent with create and delete table
  std::string_view table_name_sv{share->path.str};
  txservice::TableName table_name{table_name_sv, txservice::TableType::Primary,
                                  txservice::TableEngine::EloqSql};
  CatalogKey table_key{table_name};
  CatalogRecord catalog_rec;
  TxErrorCode err= my_tx->ReadCatalog(table_key, catalog_rec, false, exists);
  if (err == TxErrorCode::NO_ERROR)
  {
    if (exists)
    {
      const TableSchema *schema= catalog_rec.Schema();
      std::string frm, kv_info, schemas_ts;
      EloqDS::DeserializeSchemaImage(schema->SchemaImage(), frm, kv_info,
                                     schemas_ts);

      share->init_from_binary_frm_image(
          thd, false, reinterpret_cast<const unsigned char *>(frm.data()),
          frm.length());

      if (share->error == 0)
      {
        DBUG_RETURN(0);
      }
      else
      {
        // The catalog image is corrupted. TODO: returns a more informative
        // error message.
        DBUG_RETURN(HA_ERR_TABLE_CORRUPT);
      }
    }
    else
    {
      DBUG_RETURN(HA_ERR_NO_SUCH_TABLE);
    }
  }
  else
  {
    // The table's catalog is inaccessible. Pretends the table does not
    // exist. In future, should return an error message explaining the
    // situation.
    DBUG_RETURN(convert_tx_error(err));
  }
}

// Discovery table name interface.
// Used by show tables command to list all the eloq tables belong to the
// database.
extern "C" int eloq_discover_view_names(THD *thd, LEX_CSTRING db,
                                        std::vector<LEX_CSTRING> &view_names);
static int eloq_discover_table_names(handlerton *hton, LEX_CSTRING *db,
                                     MY_DIR *dir,
                                     handlerton::discovered_list *result)
{
  DBUG_ENTER_FUNC();

  std::string_view db_sv(db->str, db->length);

  if (!storage_hd)
  {
    my_printf_error(HA_ERR_INTERNAL_ERROR, "Eloq storage not initialized",
                    MYF(0));
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }
  else
  {
    // Discover table names.
    std::vector<std::string> full_name_vec;
    auto [yield_func, resume_func]= thd_get_coro_functors(current_thd);
    bool ok= storage_hd->DiscoverAllTableNames(full_name_vec, yield_func,
                                               resume_func);
    if (!ok)
    {
      my_printf_error(HA_ERR_INTERNAL_ERROR,
                      "Eloq discover tables from storage failed", MYF(0));
      DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
    }

    for (const std::string &full_name : full_name_vec)
    {
      std::string_view full_name_sv(full_name);
      if (full_name_sv.substr(0, 2) == "./")
      {
        // MySQL SHOW TABLES statement list non-temporary tables and views
        // only.
        // Tablename stored in cassandra follow format ./database/tablename.

        char buf_db[FN_REFLEN + 1]= {0};
        char buf_tb[FN_REFLEN + 1]= {0};
        LEX_STRING db_name= {buf_db, FN_REFLEN};
        LEX_STRING tb_name= {buf_tb, FN_REFLEN};
        monokey_to_marianame(full_name_sv, db_name, tb_name);
        if (std::string_view(db_name.str, db_name.length) == db_sv)
        {
          result->add_table(tb_name.str, tb_name.length);
        }
      }
    }

    // Discover view names.
    std::vector<LEX_CSTRING> base_name_vec;
    int err= eloq_discover_view_names(current_thd, *db, base_name_vec);
    if (err)
    {
      DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
    }

    for (LEX_CSTRING base_name : base_name_vec)
    {
      result->add_table(base_name.str, base_name.length);
    }

    DBUG_RETURN(0);
  }
}

extern "C" int eloq_drop_view(THD *thd, LEX_CSTRING db, LEX_CSTRING view);

/** After DROP DATABASE executed ha_innobase::delete_table() on all
tables that it was aware of, drop any leftover tables inside eloq. */
static void eloq_drop_database(handlerton *hton, char *path)
{
  DBUG_ENTER_FUNC();

  std::string_view path_str(path); // path format follow ./test/
  LEX_CSTRING db= monokey_to_mariadb(current_thd->mem_root, path);

  std::vector<LEX_CSTRING> view_names;
  int err= eloq_discover_view_names(current_thd, db, view_names);
  if (err)
  {
    my_printf_error(HA_ERR_INTERNAL_ERROR,
                    "Eloq discover views of database '%s' failed", MYF(0),
                    db.str);
    DBUG_VOID_RETURN;
  }

  for (LEX_CSTRING view_name : view_names)
  {
    int err= eloq_drop_view(current_thd, db, view_name);
    if (err)
    {
      my_printf_error(HA_ERR_INTERNAL_ERROR, "Eloq drop view './%s/%s' failed",
                      MYF(0), db.str, view_name.str);
      break;
    }
  }

  DBUG_VOID_RETURN;
}

/*
 * Parse session variable and send request of of injected fault from MariaDB
 * runtime to tx_service.
 */
static int redirect_injected_fault(const std::string &fault,
                                   TransactionExecution *txm, bool bRemove,
                                   const std::function<void()> *yield_func,
                                   const std::function<void()> *resume_func)
{
  DBUG_ENTER_FUNC();

  // skip eloq name
  size_t pos2= fault.find(';');
  size_t pos1= pos2 + 1;
  pos2= fault.find(';', pos1);
  if (pos2 == std::string::npos)
    pos2= fault.size();
  // parse check point name
  std::string fault_name= fault.substr(pos1, pos2 - pos1);

  // If remove, send the message to all nodes
  if (bRemove)
  {
    std::vector<int> vctId(1, -1);
    FaultInjectTxRequest fi_req(fault_name, "remove", vctId, yield_func,
                                resume_func, txm);
    txm->Execute(&fi_req);
    fi_req.Wait();
    DBUG_RETURN(0);
  }

  std::string fault_paras;
  std::vector<int> vctId;
  pos1= pos2 + 1;

  // Split string by ";" parse and check every key value pair.
  while (pos1 < fault.size())
  {
    pos2= fault.find(';', pos1);
    // For remote action, start with "<", end with ">".
    if (pos2 == std::string::npos)
      pos2= fault.size();
    else if (fault.find('<', pos1) < pos2)
    {
      pos2= fault.find('>', pos1);
      if (pos2 == std::string::npos)
        DBUG_RETURN(-1);
      pos2= fault.find(';', pos2);
      if (pos2 == std::string::npos)
        pos2= fault.size();
    }

    std::string sbs= fault.substr(pos1, pos2 - pos1);
    size_t pos3= sbs.find('=');
    if (pos3 == std::string::npos)
      DBUG_RETURN(-1);

    std::string key= sbs.substr(0, pos3);
    std::string val= sbs.substr(pos3 + 1);

    if (key.compare("node_id") == 0)
    {
      size_t nos1= 0;
      auto nodes_sptr= Sharder::Instance().GetAllNodesConfigs();
      // Parse node id. If more than one node, connect by "#"
      while (nos1 < val.size())
      {
        size_t nos2= val.find('#', nos1);
        if (nos2 == std::string::npos)
          nos2= val.size();

        int id= stoi(val.substr(nos1, nos2 - nos1));
        if (id == -1 || (nodes_sptr->find(id) != nodes_sptr->end()))
        {
          // -1: all nodes
          vctId.push_back(id);
          nos1= nos2 + 1;
        }
        else
        {

          DBUG_RETURN(-1);
        }
      }
    }
    else
    {
      if (fault_paras.size() > 0)
        fault_paras+= ";";
      fault_paras+= key + "=" + val;
    }

    pos1= pos2 + 1;
  }

  FaultInjectTxRequest fi_req(fault_name, fault_paras, vctId, yield_func,
                              resume_func, txm);
  txm->Execute(&fi_req);
  fi_req.Wait();

  DBUG_RETURN(0);
}

/*
 * Parse session variable and run eloq tests.
 */
static int redirect_eloq_tests(const std::string &test_str, MyEloqTx *my_tx,
                               bool bRemove)
{
  DBUG_ENTER_FUNC();

  std::string test_func;
  std::string test_args;

  // Split string by ";" to parse test_func and test_args.
  size_t pos1= test_str.find(';');
  if (pos1 == std::string::npos) // not found: no args
  {
    test_func= test_str;
    test_args= "";
  }
  else
  {
    test_func= test_str.substr(0, pos1);
    test_args= test_str.substr(pos1 + 1, test_str.length() - pos1);
  }

  int res= 0;
  // int res= run_eloq_test(test_func, test_args, my_tx, bRemove);
  if (res == 0)
  {
    DBUG_RETURN(0);
  }
  else
  {
    std::string err_msg= test_func + ":error " + std::to_string(res);
    my_error(HA_ERR_ELOQ_TEST_FAILED, MYF(0), err_msg.data());
    DBUG_RETURN(0);
  }
}

/**
 * @brief Forward fault inject message to TxService
 *
 * @param val the dbug message. Prefix +d means inject fault, while prefix -d
 * means reset fault.
 */
static void eloq_dbug_set(handlerton *hton, THD *thd, LEX_CSTRING *val)
{
  DBUG_ENTER_FUNC();
  MyEloqTx *my_tx= nullptr;
  bool tx_exists= false;

  if (val->length <= ELOQ_DEBUG_SET_PREFIX_LEN ||
      (strncmp(val->str + DEBUG_SET_PREFIX_LEN, "eloq;", 5) != 0 &&
       strncmp(val->str + DEBUG_SET_PREFIX_LEN, "eloq_test;", 10) != 0))
  {
    DBUG_VOID_RETURN;
  }

  if (!get_or_create_myeloq_tx(thd, &my_tx, &tx_exists))
  {
    my_error(HA_ERR_ELOQ_START_TRANSACTION_FAILED, MYF(0));
    DBUG_VOID_RETURN;
  }
  TransactionExecution *txm= my_tx->Txm();
  auto [yield_func, resume_func]= my_tx->CoroFunctors();

  // inject fault into TxService.
  // the session variable must start with "+d,eloq;" or "-d,eloq;"
  if (val->length > ELOQ_DEBUG_SET_PREFIX_LEN &&
      (val->str[0] == '-' || val->str[0] == '+') && val->str[1] == 'd' &&
      val->str[2] == ',' &&
      strncmp(val->str + DEBUG_SET_PREFIX_LEN, "eloq;", 5) == 0)
  {
    // If start with "-", it will remove session variable from fault inject
    // map.
    redirect_injected_fault(val->str + DEBUG_SET_PREFIX_LEN, txm,
                            (val->str[0] == '-'), yield_func, resume_func);
  }

  // run eloq tests.
  // the session variable must start with "+d,eloq_test;" or
  // "-d,eloq_test;"
  if (val->length > ELOQ_TEST_DEBUG_SET_PREFIX_LEN &&
      (val->str[0] == '-' || val->str[0] == '+') && val->str[1] == 'd' &&
      val->str[2] == ',' &&
      strncmp(val->str + DEBUG_SET_PREFIX_LEN, "eloq_test;", 10) == 0)
  {
    // If start with "-", it will remove session variable from fault inject
    // map.
    redirect_eloq_tests(val->str + ELOQ_TEST_DEBUG_SET_PREFIX_LEN, my_tx,
                        (val->str[0] == '-'));
  }

  DBUG_VOID_RETURN;
}

static int eloq_close_connection(handlerton *const hton, THD *const thd)
{
  DBUG_ENTER_FUNC();

  MyEloqTx *my_tx= get_myeloq_tx(thd);

  if (my_tx != nullptr)
  {
    my_tx->Commit();
    delete my_tx;
  }

  DBUG_RETURN(0);
}

/**
 * @brief  Starts a new eloq transaction if a transaction is not yet
 * started. And assigns a new snapshot for a consistent read if the
 * transaction does not yet have one.
 * @return 0
 *
 */
static int eloq_start_tx_and_assign_read_view(handlerton *hton, THD *thd)
{
  DBUG_ENTER("eloq_start_tx_and_assign_read_view");

  MyEloqTx *my_tx= nullptr;
  bool tx_exists= false;
  if (!get_or_create_myeloq_tx(thd, &my_tx, &tx_exists, true))
  {
    my_error(HA_ERR_ELOQ_START_TRANSACTION_FAILED, MYF(0));
    DBUG_RETURN(HA_ERR_ELOQ_START_TRANSACTION_FAILED);
  }
  DBUG_ASSERT(my_tx != nullptr);
  // if (my_tx->Txm()->GetIsolationLevel() !=
  // txservice::IsolationLevel::Snapshot)
  // {
  //   push_warning_printf(thd, Sql_condition::WARN_LEVEL_WARN,
  //                       HA_ERR_UNSUPPORTED,
  //                       "MonoDB: WITH CONSISTENT SNAPSHOT"
  //                       " was ignored because this phrase"
  //                       " can only be used with"
  //                       " Snapshot isolation level.");
  // }
  eloq_register_tx(eloq_hton, thd, my_tx);

  DBUG_RETURN(0);
}

static void eloq_pre_shutdown(void)
{
  DBUG_ENTER_FUNC();

  // Shutdown MariaSystemHandler in advance.
  //
  // MariaSystemHandler::ReloadCache() may create a temporary THD, whose
  // constructor depends on global_system_variables.table_plugin. However, it
  // will be set to NULL during plugin_shutdown().
  MariaSystemHandler::Instance().Shutdown();

  DBUG_VOID_RETURN;
}

static int8_t PRINT_ELOQ_CONFIG_COLUMN_WIDTH= 20;

static void PrintEloqConfig()
{
  std::cout << std::endl;
  std::cout << std::left << std::setfill('-')
            << std::setw(PRINT_ELOQ_CONFIG_COLUMN_WIDTH * 2) << "-"
            << std::endl;
  std::cout << std::setfill(' ');
  std::cout << std::left << "Eloq Build Configurations:" << std::endl;
  std::cout << std::left << std::setfill('-')
            << std::setw(PRINT_ELOQ_CONFIG_COLUMN_WIDTH * 2) << "-"
            << std::endl;
  std::cout << std::setfill(' ');

#if !defined(DBUG_OFF) && !defined(_lint)
  std::cout << std::left << std::setw(20) << "Fault inject" << std::left
            << std::setw(PRINT_ELOQ_CONFIG_COLUMN_WIDTH) << "ON" << std::endl;
#else
  std::cout << std::left << std::setw(20) << "Fault inject" << std::left
            << std::setw(PRINT_ELOQ_CONFIG_COLUMN_WIDTH) << "OFF" << std::endl;
#endif

#if defined(RANGE_PARTITION_ENABLED)
  std::cout << std::left << std::setw(PRINT_ELOQ_CONFIG_COLUMN_WIDTH)
            << "Range partition" << std::left
            << std::setw(PRINT_ELOQ_CONFIG_COLUMN_WIDTH) << "ON" << std::endl;
#else
  std::cout << std::left << std::setw(PRINT_ELOQ_CONFIG_COLUMN_WIDTH)
            << "Range partition" << std::left
            << std::setw(PRINT_ELOQ_CONFIG_COLUMN_WIDTH) << "OFF" << std::endl;
#endif

#if !defined(TX_TRACE_DISABLED)
  std::cout << std::left << std::setw(PRINT_ELOQ_CONFIG_COLUMN_WIDTH)
            << "Tx trace" << std::left
            << std::setw(PRINT_ELOQ_CONFIG_COLUMN_WIDTH) << "ON" << std::endl;
#else
  std::cout << std::left << std::setw(PRINT_ELOQ_CONFIG_COLUMN_WIDTH)
            << "Tx trace" << std::left
            << std::setw(PRINT_ELOQ_CONFIG_COLUMN_WIDTH) << "OFF" << std::endl;
#endif

  std::cout << std::left << std::setfill('-')
            << std::setw(PRINT_ELOQ_CONFIG_COLUMN_WIDTH * 2) << "-"
            << std::endl;
  std::cout << std::setfill(' ');
  std::cout << std::endl;
}

#if defined(DATA_STORE_TYPE_DYNAMODB) ||                                      \
    (defined(USE_ROCKSDB_LOG_STATE) && (WITH_ROCKSDB_CLOUD == CS_TYPE_S3)) || \
    defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3)

Aws::SDKOptions aws_options;

static int aws_init()
{
  DBUG_ENTER("aws_init");
  aws_options.loggingOptions.logLevel= Aws::Utils::Logging::LogLevel::Info;
  Aws::InitAPI(aws_options);
  DBUG_RETURN(0);
}

static int aws_deinit()
{
  DBUG_ENTER("aws_deinit");
  Aws::ShutdownAPI(aws_options);
  DBUG_RETURN(0);
}
#endif

/*Free any resources that were allocated and return failure.
@return always return 1 */
static int eloq_init_abort()
{
  DBUG_ENTER("eloq_init_abort");

  if (txlog_server != nullptr)
  {
    txlog_server= nullptr; // stop and release txlog service
  }

#if defined(DATA_STORE_TYPE_DYNAMODB) ||                                      \
    (defined(USE_ROCKSDB_LOG_STATE) && (WITH_ROCKSDB_CLOUD == CS_TYPE_S3)) || \
    defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3)
  aws_deinit();
#endif

  DBUG_RETURN(1);
}

static void RegisterFactory()
{
  txservice::TxKeyFactory::RegisterCreateTxKeyFunc(EloqKey::Create);
  txservice::TxKeyFactory::RegisterCreateDefaultTxKeyFunc(
      EloqKey::CreateDefault);
  txservice::TxKeyFactory::RegisterNegInfTxKey(EloqKey::NegInfTxKey());
  txservice::TxKeyFactory::RegisterPosInfTxKey(EloqKey::PosInfTxKey());
  txservice::TxKeyFactory::RegisterPackedNegativeInfinity(
      EloqKey::PackedNegativeInfinityTxKey());
  txservice::TxRecordFactory::RegisterCreateTxRecordFunc(EloqRecord::Create);
}

static int eloq_init_func(void *p)
{
  DBUG_ENTER_FUNC();

  PrintEloqConfig();

  RegisterFactory();

  sql_print_information("Eloq initializing.");

#if defined(DATA_STORE_TYPE_DYNAMODB) ||                                      \
    (defined(USE_ROCKSDB_LOG_STATE) && (WITH_ROCKSDB_CLOUD == CS_TYPE_S3)) || \
    defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3)
  if (aws_init())
  {
    sql_print_error("Eloq failed to initialize AWS SDK.");
    DBUG_RETURN(1);
  }
#endif

  // add mono collation and mono mem cmp mutex to psi definition
  init_eloq_psi_keys();
  mysql_mutex_init(mono_collation_data_mutex_key, &mono_collation_data_mutex,
                   MY_MUTEX_INIT_FAST);
  mysql_mutex_init(mono_mem_cmp_space_mutex_key, &mono_mem_cmp_space_mutex,
                   MY_MUTEX_INIT_FAST);

  eloq_hton= (handlerton *) p;
  eloq_hton->create= eloq_create_handler;
  eloq_hton->close_connection= eloq_close_connection;
  eloq_hton->flags= HTON_CAN_RECREATE | HTON_SUPPORTS_EXTENDED_KEYS;
  eloq_hton->table_options= eloq_table_option_list;
  eloq_hton->field_options= eloq_field_option_list;
  eloq_hton->tablefile_extensions= ha_eloq_exts;
  eloq_hton->commit= eloq_commit;
  eloq_hton->rollback= eloq_rollback;
  eloq_hton->discover_table= eloq_discover_table;
  eloq_hton->discover_table_names= eloq_discover_table_names;
  eloq_hton->drop_database= eloq_drop_database;
  eloq_hton->dbug_set= eloq_dbug_set;
  eloq_hton->start_consistent_snapshot= eloq_start_tx_and_assign_read_view;
  eloq_hton->pre_shutdown= eloq_pre_shutdown;

  std::string insert_semantic_str= eloq_insert_semantic;
  std::transform(insert_semantic_str.begin(), insert_semantic_str.end(),
                 insert_semantic_str.begin(), ::tolower);
  if (insert_semantic_str == "upsert")
  {
    is_insert_semantic_= false;
  }

  node_id= 0;
  uint16_t local_port= 8000;
  std::string local_ip;

  // parse the eloq config.
  std::string local_ip_str(eloq_local_ip);
  size_t idx= local_ip_str.find_first_of(':');

  if (idx == std::string::npos)
  {
    local_ip= local_ip_str;
    local_port= 8000;
    local_ip_str.append(":8000");
  }
  else
  {
    local_ip= local_ip_str.substr(0, idx);
    local_port= std::stoi(local_ip_str.substr(idx + 1));
  }

  sql_print_information("MariaDB Node address: %s", local_ip_str.c_str());
  sql_print_information("MariaDB data home: %s", mysql_real_data_home_ptr);

  // initialize monogrpah specific error message.
  int err= my_error_register(eloq_get_error_messages, HA_ERR_ELOQ_FIRST,
                             HA_ERR_ELOQ_LAST);
  if (err != 0)
  {
    // NO_LINT_DEBUG
    sql_print_error("EloqDB: Couldn't initialize error messages");
    DBUG_RETURN(eloq_init_abort());
  }

  std::string local_path("local://");
  local_path.append(mysql_real_data_home_ptr);
  if (local_path.at(local_path.size() - 1) == '/')
  {
    local_path.erase(local_path.size() - 1);
  }

  // Read host manager address
  std::string hm_ip("");
  uint16_t hm_port;
  std::string hm_bin_path(eloq_hm_bin_path);

  std::string cluster_config_file(eloq_cluster_config_file);
  if (cluster_config_file.empty())
  {
    cluster_config_file.append(mysql_real_data_home_ptr);
    cluster_config_file.append("/tx_service/cluster_config");
  }

  // bootstrap is done in standalone mode and does not need host manager.
  if (!opt_bootstrap)
  {
    std::string hm_ip_str(eloq_hm_ip);
    idx= hm_ip_str.find_first_of(':');
    if (idx != std::string::npos)
    {
      hm_ip= hm_ip_str.substr(0, idx);
      hm_port= std::stoi(hm_ip_str.substr(idx + 1));
    }
#ifdef FORK_HM_PROCESS
    else
    {
      hm_ip= local_ip;
      hm_port= local_port + 4;
    }
    if (hm_bin_path.empty())
    {
      char path_buf[PATH_MAX];
      ssize_t len= ::readlink("/proc/self/exe", path_buf, sizeof(path_buf));
      len-= strlen("/mariadbd");
      path_buf[len]= '\0';
      hm_bin_path= std::string(path_buf, len);
      hm_bin_path.append("/host_manager");
    }
#endif
  }
  else
  {
    // No need to enable log in bootstrap.
    eloq_skip_redo_log= true;
  }

  std::unordered_map<uint32_t, std::vector<NodeConfig>> ng_configs;
  uint64_t cluster_config_version= 2;

  std::string store_keyspace_name(eloq_keyspace_name);

  switch (eloq_kv_storage)
  {
#if defined(DATA_STORE_TYPE_CASSANDRA)
  case KV_CASS: {
    // initialize Cassandra handler.
    std::string store_host(eloq_cass_hosts);
    int store_port(eloq_cass_port);
    int store_queue_size_io(eloq_cass_queue_size_io);
    std::string keyspace_class(eloq_cass_keyspace_class);
    std::string replication_factor(eloq_cass_replication_factor);

    if (store_host.size() > 0)
    {
      std::string user(eloq_cass_user);
      std::string password(eloq_cass_password);

      storage_hd= std::make_unique<EloqDS::CassHandler>(
          store_host, store_port, user, password, store_keyspace_name,
          keyspace_class, replication_factor, eloq_high_compression_ratio,
          store_queue_size_io, opt_bootstrap, eloq_ddl_skip_kv);
      if (!storage_hd->Connect())
      {
        // connect Cassandra error
        sql_print_error(
            "!!!!!!!! Failed to connect to Cassandra server, EloqDB "
            "startup is terminated !!!!!!!!");
        DBUG_RETURN(eloq_init_abort());
      }
    }
    else
    {
      // connect Cassandra error due to empty address.
      DBUG_RETURN(eloq_init_abort());
    }
    break;
  }
#elif defined(DATA_STORE_TYPE_DYNAMODB)
  case KV_DYNAMO: {
    // initialize DynamoDB handler.
    std::string endpoint(eloq_dynamodb_endpoint);
    std::string region(eloq_dynamodb_region);
    std::string access_key(eloq_aws_access_key_id);
    std::string secret_key(eloq_aws_secret_key);

    storage_hd= std::make_unique<EloqDS::DynamoHandler>(
        store_keyspace_name, endpoint, region, access_key, secret_key,
        opt_bootstrap, eloq_ddl_skip_kv, eloq_core_num * 2);
    if (!storage_hd->Connect())
    {
      sql_print_error("!!!!!!!! Failed to connect to DynamoDB server, EloqDB "
                      "startup is terminated !!!!!!!!");
      DBUG_RETURN(eloq_init_abort());
    }
    break;
  }
#elif defined(DATA_STORE_TYPE_BIGTABLE)
  case KV_BIGTABLE: {
    std::string project_id(eloq_bigtable_project_id);
    std::string instance_id(eloq_bigtable_instance_id);
    storage_hd= std::make_unique<EloqDS::BigTableHandler>(
        store_keyspace_name, project_id, instance_id, opt_bootstrap,
        eloq_ddl_skip_kv);
    if (!storage_hd->Connect())
    {
      sql_print_error("!!!!!!!! Failed to connect BigTable server, EloqDB "
                      "startup is terminated !!!!!!!!");
      DBUG_RETURN(eloq_init_abort());
    }
    break;
  }

#elif ELOQDS
  case KV_ELOQDS: {
    bool is_single_node= true;
    std::string ds_peer_node= eloq_dss_peer_node;
    std::string dss_data_path= mysql_real_data_home_ptr;
    dss_data_path.append("/eloq_dss");
    try
    {
      if (!std::filesystem::exists(dss_data_path))
      {
        std::filesystem::create_directories(dss_data_path);
      }
    }
    catch (const std::filesystem::filesystem_error &e)
    {
      sql_print_error("Failed to create dir: %s", dss_data_path.c_str());
      DBUG_RETURN(eloq_init_abort());
    }
    std::string dss_config_file_path= eloq_dss_config_file_path;
    if (dss_config_file_path.empty())
    {
      dss_config_file_path= dss_data_path + "/dss_config.ini";
    }

    EloqDS::DataStoreServiceClusterManager ds_config;
    if (!ds_config.Load(dss_config_file_path))
    {
      if (!ds_peer_node.empty())
      {
        ds_config.SetThisNode(local_ip, local_port + 7);
        // Fetch ds topology from peer node
        if (!EloqDS::DataStoreService::FetchConfigFromPeer(ds_peer_node,
                                                           ds_config))
        {
          sql_print_error("Failed to fetch config from peer node: %s",
                          ds_peer_node.c_str());
          DBUG_RETURN(eloq_init_abort());
        }

        // Save the fetched config to the local file
        if (!ds_config.Save(dss_config_file_path))
        {
          sql_print_error("Failed to save config to file: %s",
                          dss_config_file_path.c_str());
          DBUG_RETURN(eloq_init_abort());
        }
      }
      else if (opt_bootstrap || is_single_node)
      {
        // Initialize the data store service config
        ds_config.Initialize(local_ip, local_port + 7);
        if (!ds_config.Save(dss_config_file_path))
        {
          sql_print_error("Failed to save config to file: %s",
                          dss_config_file_path.c_str());
          DBUG_RETURN(eloq_init_abort());
        }
      }
      else
      {
        sql_print_error("Failed to load data store service config file: %s",
                        dss_config_file_path.c_str());
        DBUG_RETURN(eloq_init_abort());
      }
    }
    else
    {
      sql_print_information("EloqDataStoreService loaded config file %s",
                            dss_config_file_path.c_str());
    }

#if defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3) ||                      \
    defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_GCS)
    // std::string ds_rocksdb_config_file_path=
    //     "/home/lzx/test-eloqsql/eloq_ds.ini";
    INIReader fake_config_reader(nullptr, 0);
    EloqDS::RocksDBConfig rocksdb_config(fake_config_reader, dss_data_path);
    EloqDS::RocksDBCloudConfig rocksdb_cloud_config(fake_config_reader);
#if defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3)
    rocksdb_cloud_config.aws_access_key_id_= eloq_aws_access_key_id;
    rocksdb_cloud_config.aws_secret_key_= eloq_aws_secret_key;
#endif
    rocksdb_cloud_config.bucket_name_= eloq_dss_rocksdb_cloud_bucket_name;
    rocksdb_cloud_config.bucket_prefix_= eloq_dss_rocksdb_cloud_bucket_prefix;
    rocksdb_cloud_config.region_= eloq_dss_rocksdb_cloud_region;
    rocksdb_cloud_config.s3_endpoint_url_= eloq_dss_rocksdb_cloud_endpoint_url;
    rocksdb_cloud_config.sst_file_cache_size_=
        txlog::parse_size(eloq_dss_rocksdb_cloud_sst_file_cache_size);
    rocksdb_cloud_config.sst_file_cache_num_shard_bits_=
        eloq_dss_rocksdb_cloud_sst_file_cache_num_shard_bits;
    rocksdb_cloud_config.db_file_deletion_delay_=
        eloq_dss_rocksdb_cloud_file_deletion_delay;

    bool enable_cache_replacement_= fake_config_reader.GetBoolean(
        "local", "enable_cache_replacement", false);
    auto ds_factory= std::make_unique<EloqDS::RocksDBCloudDataStoreFactory>(
        rocksdb_config, rocksdb_cloud_config, enable_cache_replacement_);
#elif defined(DATA_STORE_TYPE_ELOQDSS_ELOQSTORE)
    EloqDS::EloqStoreConfig eloq_store_config;
    eloq_store_config.worker_count_= eloq_eloqstore_worker_num;
    eloq_store_config.storage_path_= eloq_eloqstore_data_path;
    eloq_store_config.open_files_limit_= eloq_eloqstore_open_files_limit;
    auto ds_factory=
        std::make_unique<EloqDS::EloqStoreDataStoreFactory>(eloq_store_config);
#endif

    data_store_service_= std::make_unique<EloqDS::DataStoreService>(
        ds_config, eloq_dss_config_file_path, dss_data_path + "/DSMigrateLog",
        std::move(ds_factory));
    std::vector<uint32_t> dss_shards= ds_config.GetShardsForThisNode();
    std::unordered_map<uint32_t, std::unique_ptr<EloqDS::DataStore>>
        dss_shards_map;
    // setup rocksdb cloud data store
    for (int shard_id : dss_shards)
    {
#if defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3) ||                      \
    defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_GCS)
      // TODO(lzx):move setup datastore to data_store_service
      auto ds= std::make_unique<EloqDS::RocksDBCloudDataStore>(
          rocksdb_cloud_config, rocksdb_config,
          (opt_bootstrap || is_single_node), enable_cache_replacement_,
          shard_id, data_store_service_.get());
#elif defined(DATA_STORE_TYPE_ELOQDSS_ELOQSTORE)
      DLOG(INFO) << "worker: " << eloq_store_config.worker_count_
                 << ", path: " << eloq_store_config.storage_path_
                 << ", max open files: "
                 << eloq_store_config.open_files_limit_;
      ::kvstore::KvOptions store_config;
      store_config.num_threads= eloq_store_config.worker_count_;
      store_config.store_path.emplace_back()
          .append(eloq_store_config.storage_path_)
          .append("/ds_")
          .append(std::to_string(shard_id));
      store_config.fd_limit= eloq_store_config.open_files_limit_ /
                             eloq_store_config.worker_count_;
      auto ds= std::make_unique<EloqDS::EloqStoreDataStore>(
          shard_id, data_store_service_.get(), store_config);
#endif
      ds->Initialize();

      // Start db if the shard status is not closed
      if (ds_config.FetchDSShardStatus(shard_id) !=
          EloqDS::DSShardStatus::Closed)
      {
        bool ret= ds->StartDB();
        if (!ret)
        {
          sql_print_error("Failed to start db instance in data store service");
          DBUG_RETURN(eloq_init_abort());
        }
      }
      dss_shards_map[shard_id]= std::move(ds);
    }

    // setup local data store service
    bool ret= data_store_service_->StartService();
    if (!ret)
    {
      sql_print_error("Failed to start data store service");
      DBUG_RETURN(eloq_init_abort());
    }
    data_store_service_->ConnectDataStore(std::move(dss_shards_map));
    // setup data store service client
    storage_hd= std::make_unique<EloqDS::DataStoreServiceClient>(
        ds_config, data_store_service_.get());

    if (!storage_hd->Connect())
    {
      sql_print_error("!!!!!!!! Failed to connect ELOQ_DS server, EloqDB "
                      "startup is terminated !!!!!!!!");
      DBUG_RETURN(eloq_init_abort());
    }

    break;
  }

#endif

  default:
    DBUG_RETURN(eloq_init_abort());
  }
  if (opt_bootstrap)
  {
    // When execute mysql_install_db.sh, eloq should run in solo mode.
    std::vector<NodeConfig> solo_config;
    solo_config.emplace_back(0, local_ip, local_port);
    ng_configs.try_emplace(0, std::move(solo_config));
  }
  else if (!txservice::ReadClusterConfigFile(cluster_config_file, ng_configs,
                                             cluster_config_version))
  {

    // Read cluster topology from general config file in this case
    auto parse_res= txservice::ParseNgConfig(
        eloq_ip_list, eloq_standby_ip_list, eloq_voter_ip_list, ng_configs,
        eloq_node_group_replica_num, 0);
    if (!parse_res)
    {
      LOG(ERROR) << "Failed to extract cluster configs from ip_port_list.";
      DBUG_RETURN(eloq_init_abort());
    }
  }

  bool found= false;
  uint32_t native_ng_id= 0;
  // check whether this node is in cluster.
  for (auto &pair : ng_configs)
  {
    auto &ng_nodes= pair.second;
    for (size_t i= 0; i < ng_nodes.size(); i++)
    {
      if (ng_nodes[i].host_name_ == local_ip &&
          ng_nodes[i].port_ == local_port)
      {
        node_id= ng_nodes[i].node_id_;
        found= true;
        if (ng_nodes[i].is_candidate_)
        {
          // found native_ng_id.
          native_ng_id= pair.first;
          break;
        }
      }
    }
  }

  if (!found)
  {
    sql_print_error("!!!!!!!! Current node does not belong to any node "
                    "group, EloqDB "
                    "startup is terminated !!!!!!!!");
    DBUG_RETURN(eloq_init_abort());
  }

  // Set max rpc message size as 512mb.
  GFLAGS_NAMESPACE::SetCommandLineOption("max_body_size", "536870912");
  // Set bthread worker number
  if (eloq_bthread_worker_num != 0)
  {
    GFLAGS_NAMESPACE::SetCommandLineOption(
        "bthread_concurrency",
        std::to_string(eloq_bthread_worker_num).c_str());
  }
  GFLAGS_NAMESPACE::SetCommandLineOption(
      "rocksdb_scan_threads",
      std::to_string(eloq_logserver_rocksdb_scan_thread_num).c_str());
  std::vector<std::string> txlog_ips;
  std::vector<uint16_t> txlog_ports;
  if (std::strlen(eloq_txlog_service_list) == 0)
  {
    sql_print_information("Stand-alone txlog service is not provided, start "
                          "bounded txlog service.");
    // initialize log service.
    std::string txlog_path(local_path);
    txlog_path.append("/tx_log");
    std::string txlog_rocksdb_path;
    // default rocksdb path is <data_home>/tx_log/rocksdb
    if (strlen(eloq_txlog_rocksdb_storage_path) == 0)
    {
      // remove "local://" prefix
      txlog_rocksdb_path= txlog_path.substr(8) + "/rocksdb";
    }
    else
    {
      txlog_rocksdb_path= eloq_txlog_rocksdb_storage_path;
    }
    sql_print_information("EloqDB txlog path: %s", txlog_path.c_str());
    sql_print_information("EloqDB txlog rocksdb path: %s",
                          txlog_rocksdb_path.c_str());

    uint16_t log_server_port= local_port + 2;
    for (uint32_t ng= 0; ng < ng_configs.size(); ng++)
    {
      // Use cc node port + 2 for log server
      txlog_ports.emplace_back(ng_configs[ng][0].port_ + 2);
      txlog_ips.emplace_back(ng_configs[ng][0].host_name_);
    }

    bool enable_txlog_request_checkpoint=
        eloq_enable_txlog_request_checkpoint ? true : false;
    uint64_t notify_checkpointer_threshold_size=
        txlog::parse_size(eloq_notify_checkpointer_threshold_size);
    sql_print_information(
        "eloq_enable_txlog_request_checkpoint: %s",
        (eloq_enable_txlog_request_checkpoint ? "ON" : "OFF"));
    if (enable_txlog_request_checkpoint)
    {
      sql_print_information("eloq_check_replay_log_size_interval_sec: %d",
                            eloq_check_replay_log_size_interval_sec);
      sql_print_information("eloq_notify_checkpointer_threshold_size: %s",
                            eloq_notify_checkpointer_threshold_size);
    }

#ifdef USE_ROCKSDB_LOG_STATE
    size_t rocksdb_target_file_size_base_val=
        txlog::parse_size(eloq_txlog_rocksdb_target_file_size_base);
#ifdef WITH_ROCKSDB_CLOUD
    txlog::RocksDBCloudConfig rocksdb_cloud_config;
#if WITH_ROCKSDB_CLOUD == CS_TYPE_S3
    rocksdb_cloud_config.aws_access_key_id_= eloq_aws_access_key_id;
    rocksdb_cloud_config.aws_secret_key_= eloq_aws_secret_key;
#endif /* WITH_ROCKSDB_CLOUD == CS_TYPE_S3 */
    rocksdb_cloud_config.bucket_name_= eloq_txlog_rocksdb_cloud_bucket_name;
    rocksdb_cloud_config.bucket_prefix_=
        eloq_txlog_rocksdb_cloud_bucket_prefix;
    rocksdb_cloud_config.region_= eloq_txlog_rocksdb_cloud_region;
    rocksdb_cloud_config.endpoint_url_= eloq_txlog_rocksdb_cloud_endpoint_url;
    rocksdb_cloud_config.sst_file_cache_size_=
        txlog::parse_size(eloq_txlog_rocksdb_cloud_sst_file_cache_size);
    rocksdb_cloud_config.sst_file_cache_num_shard_bits_=
        eloq_txlog_rocksdb_cloud_sst_file_cache_num_shard_bits;
    rocksdb_cloud_config.db_ready_timeout_us_=
        eloq_txlog_rocksdb_cloud_ready_timeout * 1000 * 1000;
    rocksdb_cloud_config.db_file_deletion_delay_=
        eloq_txlog_rocksdb_cloud_file_deletion_delay;

    if (opt_bootstrap)
    {
#if defined(OPEN_LOG_SERVICE)
      txlog_server= std::make_unique<::txlog::LogServer>(
          node_id, log_server_port, txlog_path, 1,
          rocksdb_cloud_config,
          eloq_rocksdb_cloud_in_mem_log_size_high_watermark,
          eloq_rocksdb_max_write_buffer_number,
          eloq_rocksdb_max_background_jobs, rocksdb_target_file_size_base_val);
#else
      txlog_server= std::make_unique<::txlog::LogServer>(
          node_id, log_server_port, txlog_ips, txlog_ports, txlog_path, 0,
          eloq_txlog_group_replica_num, txlog_rocksdb_path,
          eloq_logserver_rocksdb_scan_thread_num, rocksdb_cloud_config,
          eloq_txlog_rocksdb_cloud_in_mem_log_size_high_watermark,
          eloq_txlog_rocksdb_max_write_buffer_number,
          eloq_txlog_rocksdb_max_background_jobs,
          rocksdb_target_file_size_base_val, eloq_logserver_snapshot_interval);
#endif
    }
    else
    {
#if defined(OPEN_LOG_SERVICE)
      txlog_server= std::make_unique<::txlog::LogServer>(
          node_id, log_server_port, txlog_path, 1,
          rocksdb_cloud_config,
          eloq_rocksdb_cloud_in_mem_log_size_high_watermark,
          eloq_rocksdb_max_write_buffer_number,
          eloq_rocksdb_max_background_jobs, rocksdb_target_file_size_base_val);
#else
      txlog_server= std::make_unique<::txlog::LogServer>(
          node_id, log_server_port, txlog_ips, txlog_ports, txlog_path, 0,
          eloq_txlog_group_replica_num, txlog_rocksdb_path,
          eloq_logserver_rocksdb_scan_thread_num, rocksdb_cloud_config,
          eloq_txlog_rocksdb_cloud_in_mem_log_size_high_watermark,
          eloq_txlog_rocksdb_max_write_buffer_number,
          eloq_txlog_rocksdb_max_background_jobs,
          rocksdb_target_file_size_base_val, eloq_logserver_snapshot_interval,
          enable_txlog_request_checkpoint,
          eloq_check_replay_log_size_interval_sec,
          notify_checkpointer_threshold_size);
#endif
    }
#else /* WITH_ROCKSDB_CLOUD */
    size_t rocksdb_sst_files_size_limit_val=
        txlog::parse_size(eloq_txlog_rocksdb_sst_files_size_limit);

#ifdef ELOQ_MODULE_ENABLED
    GFLAGS_NAMESPACE::SetCommandLineOption(
        "bthread_concurrency", std::to_string(eloq_core_num).c_str());
    GFLAGS_NAMESPACE::SetCommandLineOption("use_pthread_event_dispatcher",
                                           "true");
    int busy_time= 10000;
    GFLAGS_NAMESPACE::SetCommandLineOption("worker_polling_time_us",
                                           std::to_string(busy_time).c_str());
#endif

    if (opt_bootstrap)
    {
#if defined(OPEN_LOG_SERVICE)
      txlog_server= std::make_unique<::txlog::LogServer>(
          node_id, log_server_port, txlog_path, 1,
          rocksdb_sst_files_size_limit_val,
          eloq_rocksdb_max_write_buffer_number,
          eloq_rocksdb_max_background_jobs, rocksdb_target_file_size_base_val);
#else
      txlog_server= std::make_unique<::txlog::LogServer>(
          node_id, log_server_port, txlog_ips, txlog_ports, txlog_path, 0,
          eloq_txlog_group_replica_num, txlog_rocksdb_path,
          eloq_logserver_rocksdb_scan_thread_num,
          rocksdb_sst_files_size_limit_val,
          eloq_txlog_rocksdb_max_write_buffer_number,
          eloq_txlog_rocksdb_max_background_jobs,
          rocksdb_target_file_size_base_val, eloq_logserver_snapshot_interval);
#endif
    }
    else
    {
#if defined(OPEN_LOG_SERVICE)
      txlog_server= std::make_unique<::txlog::LogServer>(
          node_id, log_server_port, txlog_path, 1,
          rocksdb_sst_files_size_limit_val,
          eloq_rocksdb_max_write_buffer_number,
          eloq_rocksdb_max_background_jobs, rocksdb_target_file_size_base_val);
#else
      txlog_server= std::make_unique<::txlog::LogServer>(
          node_id, log_server_port, txlog_ips, txlog_ports, txlog_path, 0,
          eloq_txlog_group_replica_num, txlog_rocksdb_path,
          eloq_logserver_rocksdb_scan_thread_num,
          rocksdb_sst_files_size_limit_val,
          eloq_txlog_rocksdb_max_write_buffer_number,
          eloq_txlog_rocksdb_max_background_jobs,
          rocksdb_target_file_size_base_val, eloq_logserver_snapshot_interval,
          enable_txlog_request_checkpoint,
          eloq_check_replay_log_size_interval_sec,
          notify_checkpointer_threshold_size);
#endif
    }

#endif /* WITH_ROCKSDB_CLOUD */
#else  /* USE_ROCKSDB_LOG_STATE */
#if defined(OPEN_LOG_SERVICE)
    txlog_server= std::make_unique<::txlog::LogServer>(
        node_id, log_server_port, txlog_path, 1);
#else
    txlog_server= std::make_unique<::txlog::LogServer>(
        node_id, log_server_port, txlog_ips, txlog_ports, txlog_path, 0,
        eloq_txlog_group_replica_num, eloq_logserver_snapshot_interval);
#endif
#endif /* USE_ROCKSDB_LOG_STATE */
    err= txlog_server->Start();
    if (err != 0)
    {
      // TODO: append detailed error information.
      sql_print_error("Failed to start the tx log service in this node.");
      DBUG_RETURN(eloq_init_abort());
    }

    sql_print_information("Bounded txlog service started.");
  }
  else
  {
    sql_print_information("Stand-alone txlog service provided: %s.",
                          eloq_txlog_service_list);

    std::string token;
    std::istringstream txlog_ip_list_stream(eloq_txlog_service_list);
    while (std::getline(txlog_ip_list_stream, token, ','))
    {
      size_t c_idx= token.find_first_of(':');
      if (c_idx != std::string::npos)
      {
        txlog_ips.emplace_back(token.substr(0, c_idx));
        uint16_t pt= std::stoi(token.substr(c_idx + 1));
        txlog_ports.emplace_back(pt);
      }
      else
      {
        sql_print_error("Port is missing in eloq_txlog_service_list");
        DBUG_RETURN(eloq_init_abort());
      }
    }
  }

  // fetch and print the global level cc_protocol
  switch (fetch_tx_cc_protocol(nullptr))
  {
  case CcProtocol::OCC:
    sql_print_information("Using CcProtcol: OCC.");
    break;
  case CcProtocol::OccRead:
    sql_print_information("Using CcProtcol: OccRead.");
    break;
  case CcProtocol::Locking:
    sql_print_information("Using CcProtcol: Locking.");
    break;
  default:
    assert(false);
  };

  if (eloq_enable_mvcc)
  {
    sql_print_information("mvcc is enabled.");
  }
  else
  {
    sql_print_information("mvcc is disabled.");
  }

  // initialize the transaction service.
  std::map<std::string, uint32_t> tx_service_conf;
  tx_service_conf.insert(
      std::pair<std::string, uint32_t>("core_num", eloq_core_num));
  tx_service_conf.insert(std::pair<std::string, uint32_t>(
      "range_split_worker_num", eloq_range_split_worker_num));
  tx_service_conf.insert(std::pair<std::string, uint32_t>(
      "checkpointer_interval", eloq_checkpointer_interval_sec));
  tx_service_conf.insert(std::pair<std::string, uint32_t>(
      "node_memory_limit_mb", eloq_node_memory_limit_mb));
  tx_service_conf.insert(std::pair<std::string, uint32_t>(
      "node_log_limit_mb", eloq_node_log_limit_mb));
  tx_service_conf.insert(std::pair<std::string, uint32_t>(
      "checkpointer_delay_seconds", eloq_checkpointer_delay_sec));
  tx_service_conf.insert(std::pair<std::string, uint32_t>(
      "collect_active_tx_ts_interval_seconds",
      eloq_collect_active_tx_ts_interval_sec));
  tx_service_conf.insert(std::pair<std::string, uint32_t>(
      "realtime_sampling", eloq_realtime_sampling));
  tx_service_conf.insert(std::pair<std::string, uint32_t>(
      "rep_group_cnt", eloq_node_group_replica_num));
  tx_service_conf.insert(std::pair<std::string, uint32_t>(
      "enable_key_cache", eloq_use_key_cache ? 1 : 0));
  tx_service_conf.insert(std::pair<std::string, uint32_t>(
      "enable_shard_heap_defragment", eloq_enable_heap_defragment ? 1 : 0));
  tx_service_conf.insert(std::pair<std::string, uint32_t>(
      "bthread_worker_num", eloq_bthread_worker_num));
  tx_service_conf.insert(std::pair<std::string, uint32_t>(
      "kickout_data_for_test", eloq_kickout_data_for_test ? 1 : 0));

  auto log_agent= std::make_unique<MyEloqLogAgent>(
      static_cast<uint32_t>(eloq_txlog_group_replica_num));

  sql_print_information("enable_metrics: %s",
                        eloq_enable_metrics ? "ON" : "OFF");

  metrics::CommonLabels empty_common_labels= {};
  std::vector<std::tuple<metrics::Name, metrics::Type,
                         std::vector<metrics::LabelGroup>>>
      empty_external_metrics= {};
  if (eloq_enable_metrics && !opt_bootstrap)
  {
    setenv("ELOQ_METRICS_PORT", std::to_string(eloq_metrics_port).c_str(),
           false);
    MetricsRegistryImpl::MetricsRegistryResult metrics_registry_result=
        MetricsRegistryImpl::GetRegistry();
    if (metrics_registry_result.not_ok_ != nullptr)
    {
      // MetricsRegistry Init error will be exit(-1);
      sql_print_error("EloqDB enable metrics collector but "
                      "MetricsRegistry Init ERR %s",
                      metrics_registry_result.not_ok_);
      DBUG_RETURN(eloq_init_abort());
    }
    else
    {
      /* parse metrics options */
      metrics::enable_metrics= true;

      // mysql metrics
      metrics::enable_mysql_tx_metrics=
          eloq_enable_mysql_tx_metrics ? true : false;
      sql_print_information("enable_mysql_tx_metrics: %s",
                            metrics::enable_mysql_tx_metrics ? "ON" : "OFF");

      metrics::enable_mysql_dml_metrics=
          eloq_enable_mysql_dml_metrics ? true : false;
      sql_print_information("enable_mysql_dml_metrics: %s",
                            metrics::enable_mysql_dml_metrics ? "ON" : "OFF");

      // tx_service metrics
      metrics::enable_tx_metrics= eloq_enable_tx_metrics ? true : false;
      sql_print_information("enable_tx_metrics: %s",
                            metrics::enable_tx_metrics ? "ON" : "OFF");
      if (metrics::enable_tx_metrics)
      {
        metrics::collect_tx_duration_round= eloq_collect_tx_duration_round;
        sql_print_information(
            "collect `collect_tx_duration_round` every %d round(s).",
            metrics::collect_tx_duration_round);
      }

      metrics::enable_cache_hit_rate=
          eloq_enable_cache_hit_rate ? true : false;
      sql_print_information("enable_cache_hit_rate: %s",
                            metrics::enable_cache_hit_rate ? "ON" : "OFF");

      metrics::enable_busy_round_metrics=
          eloq_enable_busy_round_metrics ? true : false;
      sql_print_information("enable_busy_round_metrics: %s",
                            metrics::enable_busy_round_metrics ? "ON" : "OFF");
      if (metrics::enable_busy_round_metrics)
      {
        metrics::busy_round_threshold= eloq_busy_round_threshold;
        sql_print_information("busy_round_threshold %d.",
                              metrics::busy_round_threshold);
      }

      metrics::enable_memory_usage= eloq_enable_memory_usage ? true : false;
      sql_print_information("enable_memory_usage: %s",
                            metrics::enable_memory_usage ? "ON" : "OFF");
      if (metrics::enable_memory_usage)
      {
        metrics::collect_memory_usage_round= eloq_collect_memory_usage_round;
        sql_print_information("collect `memory_usage` every %d round(s).",
                              metrics::collect_memory_usage_round);
      }

      metrics::enable_remote_request_metrics=
          eloq_enable_remote_request_metrics ? true : false;
      sql_print_information("enable_remote_request_metrics: %s",
                            metrics::enable_remote_request_metrics ? "ON"
                                                                   : "OFF");

      metrics::enable_kv_metrics= eloq_enable_kv_metrics ? true : false;
      sql_print_information("enable_kv_metrics: %s",
                            metrics::enable_kv_metrics ? "ON" : "OFF");

      // log_service metrics
      metrics::enable_log_service_metrics=
          eloq_enable_log_service_metrics ? true : false;
      sql_print_information("enable_log_service_metrics: %s",
                            metrics::enable_log_service_metrics ? "ON"
                                                                : "OFF");

      metrics_registry= std::move(metrics_registry_result.metrics_registry_);

      metrics::CommonLabels mysql_common_labels{};
      mysql_common_labels["node_ip"]= local_ip;
      mysql_common_labels["node_port"]= std::to_string(mysqld_port);

      metrics::CommonLabels kv_common_labels{};
      kv_common_labels["node_ip"]= local_ip;
      kv_common_labels["node_port"]= std::to_string(mysqld_port);

      metrics::register_mysql_metrics(metrics_registry.get(),
                                      mysql_common_labels);
      storage_hd->RegisterKvMetrics(metrics_registry.get(), kv_common_labels);

      metrics::CommonLabels tx_service_common_labels{};
      tx_service_common_labels["node_ip"]= local_ip;
      tx_service_common_labels["node_port"]= std::to_string(mysqld_port);
      tx_service_common_labels["node_id"]= std::to_string(node_id);

      tx_service= std::make_unique<TxService>(
          &maria_catalog_factory, &MariaSystemHandler::Instance(),
          tx_service_conf, node_id, native_ng_id, &ng_configs,
          cluster_config_version, storage_hd.get(), log_agent.get(),
          eloq_enable_mvcc, eloq_skip_redo_log, false /*skip kv*/,
          true /*enable cache replacement*/, true /*auto redirect */,
          metrics_registry.get(), tx_service_common_labels);

      sql_print_information("Eloq metrics collector bind port: %d",
                            eloq_metrics_port);

#ifdef EXT_TX_PROC_ENABLED
      get_tx_service_functors= tx_service->GetTxProcFunctors();
#endif
    }
  }
  else
  {
    tx_service= std::make_unique<TxService>(
        &maria_catalog_factory, &MariaSystemHandler::Instance(),
        tx_service_conf, node_id, native_ng_id, &ng_configs,
        cluster_config_version,
        storage_hd != nullptr ? storage_hd.get() : nullptr, log_agent.get(),
        eloq_enable_mvcc, eloq_skip_redo_log, false);

#ifdef EXT_TX_PROC_ENABLED
    get_tx_service_functors= tx_service->GetTxProcFunctors();
#endif
  }

  if (tx_service->Start(node_id, native_ng_id, &ng_configs,
                        cluster_config_version, &txlog_ips, &txlog_ports,
                        &hm_ip, &hm_port, &hm_bin_path, tx_service_conf,
                        std::move(log_agent), local_path,
                        cluster_config_file) < 0)
  {
    sql_print_error("!!!!!!Failed to start tx service. EloqDB startup is "
                    "terminated!!!!!!");
    DBUG_RETURN(eloq_init_abort());
  }

  txservice::Sequences::InitSequence(tx_service.get(), storage_hd.get());

  DeadLockCheck::SetTimeInterval(eloq_deadlock_interval_sec);

  storage_hd->SetTxService(tx_service != nullptr ? tx_service.get() : nullptr);
  sql_print_information("Transaction service started.");
  sql_print_information(
      "Number of cores allocated for transaction service: %u", eloq_core_num);

#ifdef RANGE_PARTITION_ENABLED
  eloq_partition_type= 1;
#else
  eloq_partition_type= 0;
#endif
  // tx_service is a distributed service, should wait for all the tx_service
  // nodes to finish the log recovery process and setup the cc_stream_sender.
  tx_service->WaitClusterReady();
  // wait for the tx_service node to become the native group leader.
  tx_service->WaitNodeBecomeNativeGroupLeader();

  drop_orphan_tmp_tables();

  if (eloq_signal_monitor != 0)
  {
    terminate_hook= [](int sig) {
      if (txlog_server)
      {
        txlog_server->CloseBraft();
      }
    };
  }

  sql_print_information("Eloq initialized.");

  DBUG_RETURN(0);
}

// Storage Engine deinitialization function, invoked when plugin is unloaded.
static int eloq_done_func(void *p)
{
  DBUG_ENTER_FUNC();

  sql_print_information("Shutting down the tx service.");
  tx_service->Shutdown();
  sql_print_information("Tx service shut down.");

  sql_print_information("Shutting down the storage handler.");
  storage_hd= nullptr; // Wait for all in-fight requests complete.
#if ELOQDS
  if (data_store_service_ != nullptr)
  {
    data_store_service_->DisconnectDataStore();
    data_store_service_= nullptr;
  }
#endif
  sql_print_information("Storage handler shut down.");

  sql_print_information("Shutting down the log service.");
  txlog_server= nullptr;
  sql_print_information("Log service shut down.");

  txservice::Sequences::Destory();

  // Guarantee shutdown. In some cases, like bootstrap,
  // eloq_pre_shutdown() won't call.
  MariaSystemHandler::Instance().Shutdown();

  tx_service= nullptr;

  mysql_mutex_destroy(&mono_collation_data_mutex);
  mysql_mutex_destroy(&mono_mem_cmp_space_mutex);

  for (auto &it : mono_collation_data)
  {
    delete it;
    it= nullptr;
  }

  if (eloq_enable_metrics)
  {
    metrics_registry= nullptr;
  }

#if defined(DATA_STORE_TYPE_DYNAMODB) ||                                      \
    (defined(USE_ROCKSDB_LOG_STATE) && (WITH_ROCKSDB_CLOUD == CS_TYPE_S3)) || \
    defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3)

  aws_deinit();
#endif

  DBUG_RETURN(0);
}

bool MyEloqTx::Commit()
{
  const std::function<void()> *yield_ptr= nullptr;
  const std::function<void()> *resume_ptr= nullptr;

  if (thd_ != nullptr)
  {
    auto coro_functors= CoroFunctors();
    yield_ptr= coro_functors.first;
    resume_ptr= coro_functors.second;
  }

  auto [success, err]= CommitTx(Txm(), yield_ptr, resume_ptr);

  if (!success)
  {
    tx_err_code_= err;
    my_error(HA_ERR_ELOQ_COMMIT_FAILED, MYF(0), TxErrorMessage(err).data());
  }

  ClearSchemaReaders();
  Reset(nullptr, nullptr);
  return success;
}

void MyEloqTx::ClearSchemaReaders()
{
  for (auto &[tbl_name, schema_cntl] : cached_schemas_)
  {
    schema_cntl->FinishReader();
  }
  cached_schemas_.clear();
}

void MyEloqTx::ClearSchemaReader(const TableName &tbl_name)
{
  auto it= cached_schemas_.find(tbl_name);
  if (it != cached_schemas_.end())
  {
    it->second->FinishReader();
    cached_schemas_.erase(it);
  }
}

std::pair<const std::function<void()> *, const std::function<void()> *>
MyEloqTx::CoroFunctors() const
{
  return thd_get_coro_functors(thd_);
}

/**
  @brief
  Example of simple lock controls. The "share" it creates is a
  structure we will pass to each example handler. Do you have to have
  one of these? Well, you have pieces that are used for locking, and
  they are needed to function.
*/

Eloq_share *ha_eloq::get_share()
{
  Eloq_share *tmp_share;

  DBUG_ENTER("ha_eloq::get_share()");
  lock_shared_ha_data();

  if (!(tmp_share= static_cast<Eloq_share *>(get_ha_share_ptr())))
  {
    tmp_share= new Eloq_share;
    if (!tmp_share)
      goto err;
    set_ha_share_ptr(static_cast<Handler_share *>(tmp_share));
  } // endif tmp_share

err:
  unlock_shared_ha_data();
  DBUG_RETURN(tmp_share);
} // end of get_share

ha_eloq::ha_eloq(handlerton *hton, TABLE_SHARE *table_share)
    : handler(hton, table_share),
      base_table_name_(
          table_share
              ? txservice::TableName{table_share->normalized_path.str,
                                     table_share->normalized_path.length,
                                     txservice::TableType::Primary,
                                     txservice::TableEngine::EloqSql}
              : txservice::TableName{txservice::empty_sv.data(),
                                     txservice::empty_sv.size(),
                                     txservice::TableType::Primary,
                                     txservice::TableEngine::EloqSql}),
      pk_descr_(nullptr), pack_buffer_(nullptr), record_buffer_(nullptr),
      pk_packed_tuple_(nullptr), sk_packed_tuple_(nullptr)
{
}

ulonglong ha_eloq::table_flags() const
{
  DBUG_ENTER_FUNC();
  ulonglong flags=
      HA_BINLOG_ROW_CAPABLE | HA_REC_NOT_IN_SEQ | HA_CAN_INDEX_BLOBS |
      HA_PRIMARY_KEY_IN_READ_INDEX | HA_PARTIAL_COLUMN_READ |
      HA_PRIMARY_KEY_REQUIRED_FOR_POSITION | HA_NULL_IN_KEY |
      HA_TABLE_SCAN_ON_INDEX | HA_REQUIRES_KEY_COLUMNS_FOR_DELETE |
      HA_CAN_VIRTUAL_COLUMNS | HA_BATCH_ROWID;

#ifndef RANGE_PARTITION_ENABLED
  flags|= HA_CAN_TABLE_CONDITION_PUSHDOWN;
#endif
  DBUG_RETURN(flags);
}

ulong ha_eloq::index_flags(uint inx, uint part, bool all_parts) const
{
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(inx != MAX_INDEXES);

  ulong base_flags= HA_READ_NEXT | HA_READ_ORDER | HA_READ_RANGE |
                    HA_READ_PREV | HA_KEYREAD_ONLY |
                    HA_DO_RANGE_FILTER_PUSHDOWN;

  if (inx == table_share->primary_key)
  {
    /* [RocksDB]
      Index-only reads on primary key are the same as table scan for us.
      Still, we need to explicitly "allow" them, otherwise SQL layer will
      miss some plans.
    */
    base_flags|= HA_CLUSTERED_INDEX;
  }
  else
  {
    /* [RocksDB]
      We can Index Condition Pushdown any key except the primary. With
      primary key, we get (pk, record) pair immediately, there is no place to
      put the ICP check.
    */
    base_flags|= HA_DO_INDEX_COND_PUSHDOWN;
  }

  DBUG_RETURN(base_flags);
}

/**
  @brief
  Used for opening tables. The name will be the name of the file.

  @details
  A table is opened when it needs to be opened; e.g. when a request comes in
  for a SELECT on the table (tables are not open and closed for each request,
  they are cached).

  Called from handler.cc by handler::ha_open(). The server opens all tables
  by calling ha_open() which then calls the handler specific open().

  @see
  handler::ha_open() in handler.cc
*/

int ha_eloq::open(const char *name, int mode, uint test_if_locked)
{
  DBUG_ENTER_FUNC();

  THD *thd= ha_thd();
  MyEloqTx *my_tx= nullptr;
  bool tx_exists= false;
  if (!get_or_create_myeloq_tx(thd, &my_tx, &tx_exists))
  {
    my_error(HA_ERR_ELOQ_START_TRANSACTION_FAILED, MYF(0));
    DBUG_RETURN(HA_ERR_ELOQ_START_TRANSACTION_FAILED);
  }

  const MysqlTableSchema *session_schema= DiscoverTableSchema(my_tx);
  if (session_schema == nullptr)
  {
    DBUG_RETURN(HA_ERR_NO_SUCH_TABLE);
  }

  if (!(share= get_share()))
  {
    DBUG_RETURN(1);
  }
  thr_lock_data_init(&share->lock, &lock, NULL);

  /*
    [RocksDB] Full table scan actually uses primary key
    (UPDATE needs to know this, otherwise it will go into infinite loop on
    queries like "UPDATE tbl SET pk=pk+100")
  */
  key_used_on_scan= table->s->primary_key;

  pk_descr_= this->GetKeySchema()->KeyDefinition();

  /* Allocate key buffers used for packing and unpacking */
  int err= alloc_key_buffers(table);

  if (err)
  {
    DBUG_RETURN(err);
  }

  ref_length= pk_descr_->max_storage_fmt_length();

  info(HA_STATUS_NO_LOCK | HA_STATUS_VARIABLE | HA_STATUS_CONST);

  DBUG_RETURN(0);
}

/**
  @brief
  Closes a table.

  @details
  Called from sql_base.cc, sql_select.cc, and table.cc. In sql_select.cc it
  is only used to close up temporary tables or during the process where a
  temporary table is converted over to being a myisam table.

  For sql_base.cc look at close_data_tables().

  @see
  sql_base.cc, sql_select.cc and table.cc
*/

int ha_eloq::close(void)
{
  DBUG_ENTER_FUNC();

  table_schema_= nullptr;
  hidden_key_schema_= nullptr;
  table_dirty_schema_= nullptr;
  free_key_buffers();

  DBUG_RETURN(0);
}

/**
  @brief
  write_row() inserts a row. No extra() hint is given currently if a bulk
  load is happening. buf() is a byte array of data. You can use the field
  information to extract the data from the native byte array type.

  @details
  Example of this would be:
  @code
  for (Field **field=table->field ; *field ; field++)
  {
    ...
  }
  @endcode

  See ha_tina.cc for an example of extracting all of the data as strings.
  ha_berekly.cc has an example of how to store it intact by "packing" it
  for ha_berkeley's own native storage type.

  See the note for update_row() on auto_increments and timestamps. This
  case also applies to write_row().

  Called from item_sum.cc, item_sum.cc, sql_acl.cc, sql_insert.cc,
  sql_insert.cc, sql_select.cc, sql_table.cc, sql_udf.cc, and sql_update.cc.

  @see
  item_sum.cc, item_sum.cc, sql_acl.cc, sql_insert.cc,
  sql_insert.cc, sql_select.cc, sql_table.cc, sql_udf.cc and sql_update.cc
*/

int ha_eloq::write_row(const uchar *buf)
{
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(buf != nullptr);
  DBUG_ASSERT(buf == table->record[0]);

  /* Handling of Auto-Increment Columns. */
  if (table->next_number_field && buf == table->record[0])
  {
    int error_result= 0;
    if ((error_result= update_auto_increment()))
    {
      DBUG_RETURN(HA_ERR_ELOQ_AUTO_INCREMENT_FAILED);
    }
  }

  auto [new_mono_key, new_rec]= PackKeyRecord(buf);
  int rc= 0;
  if (is_bulk_insert_ && is_duplicate_error_)
  {
    rc= BulkInsert(buf, std::move(new_mono_key), std::move(new_rec));
  }
  else
  {
    rc= Insert(buf, std::move(new_mono_key), std::move(new_rec));
  }
  DBUG_RETURN(rc);
}

/**
  @brief
  Yes, update_row() does what you expect, it updates a row. old_data will
  have the previous row record in it, while new_data will have the newest
  data in it. Keep in mind that the server can do updates based on ordering
  if an ORDER BY clause was used. Consecutive ordering is not guaranteed.

  @details
  Currently new_data will not have an updated auto_increament record, or
  and updated timestamp field. You can do these for example by doing:
  @code
  if (table->next_number_field && record == table->record[0])
    update_auto_increment();
  @endcode

  Called from sql_select.cc, sql_acl.cc, sql_update.cc, and sql_insert.cc.

  @see
  sql_select.cc, sql_acl.cc, sql_update.cc and sql_insert.cc
*/
int ha_eloq::update_row(const uchar *old_data, const uchar *new_data)
{
  DBUG_ENTER_FUNC();
  DBUG_ASSERT(table_schema_ != nullptr);

  /* [RocksDB]
    old_data points to record we're updating. It is the same as the record
    we've just read (for multi-table UPDATE, too, because SQL layer will make
    an rnd_pos() call to re-read the record before calling update_row())
  */
  DBUG_ASSERT(new_data == table->record[0]);
  bool pk_changed= false;

  std::unique_ptr<EloqKey> new_primary_key= nullptr, old_primary_key= nullptr;
  std::unique_ptr<EloqRecord> new_rec= nullptr;

  if (!has_hidden_pk(table))
  {
    auto [nkey, nrec]= PackKeyRecord(new_data);
    new_primary_key= std::move(nkey);
    new_rec= std::move(nrec);

    old_primary_key= std::make_unique<EloqKey>(pk_descr_.get(), table,
                                               pack_buffer_, old_data);
    pk_changed= (*new_primary_key != *old_primary_key);
  }
  else
  {
    // The table does not define a primary key. The old and new row must be
    // located via the last read key.
    old_primary_key= std::make_unique<EloqKey>(last_read_key_);
    new_primary_key= std::make_unique<EloqKey>(last_read_key_);
    new_rec= PackRecord(new_data);
    // If hidden primary key, rowkey for new record will always be the same
    pk_changed= false;
  }

  int rc= 0;
  // Need to insert/update before update to prevent deleting old pk record
  // without inserting new pk(due to check fail).
  if (pk_changed)
  {
    rc= Insert(new_data, std::move(new_primary_key), std::move(new_rec));
    if (rc == 0)
    {
      rc= Delete(old_data, std::move(old_primary_key));
    }
  }
  else
  {
    rc= Update(new_data, old_data, std::move(new_primary_key),
               std::move(new_rec));
  }

  DBUG_RETURN(rc);
}

/**
  @brief
  This will delete a row. buf will contain a copy of the row to be deleted.
  The server will call this right after the current row has been called (from
  either a previous rnd_nexT() or index call).

  @details
  If you keep a pointer to the last row or can access a primary key it will
  make doing the deletion quite a bit easier. Keep in mind that the server
  does not guarantee consecutive deletions. ORDER BY clauses can be used.

  Called in sql_acl.cc and sql_udf.cc to manage internal table
  information.  Called in sql_delete.cc, sql_insert.cc, and
  sql_select.cc. In sql_select it is used for removing duplicates
  while in insert it is used for REPLACE calls.

  @see
  sql_acl.cc, sql_udf.cc, sql_delete.cc, sql_insert.cc and sql_select.cc
*/

int ha_eloq::delete_row(const uchar *buf)
{
  DBUG_ENTER_FUNC();

  std::unique_ptr<EloqKey> del_key= nullptr;
  if (!has_hidden_pk(table))
  {
    del_key=
        std::make_unique<EloqKey>(pk_descr_.get(), table, pack_buffer_, buf);
  }
  else
  {
    // The table does not define a primary key. If this is a delete, the
    // row to be deleted refers to the current scan key.
    del_key= std::make_unique<EloqKey>(last_read_key_);
  }
  int rc= 0;
  rc= Delete(buf, std::move(del_key));

  DBUG_RETURN(rc);
}

/**
  @brief
  Positions an index cursor to the index specified in the handle. Fetches the
  row if available. If the key value is null, begin at the first key of the
  index.
*/

int ha_eloq::index_read_map(uchar *buf, const uchar *key,
                            key_part_map keypart_map,
                            enum ha_rkey_function find_flag)
{
  DBUG_ENTER_FUNC();

  MyEloqTx *my_tx= get_myeloq_tx(ha_thd());
  DBUG_ASSERT(my_tx != nullptr);

  SetupDecodeFlagOnFirstRead();

  const TABLE_SHARE *share= table_schema_->TableShare();
  KEY *key_info= &share->key_info[active_index];
  uint32_t key_part_cnt= table->actual_n_key_parts(key_info);

  // use_full_key indicates whether or not all key fields are used in this
  // index lookup. The flag so far is only used in primary index lookups. If
  // all key fields in a primary key are used by the index lookup, we use
  // ReadRequest instead of ScanRequest.
  bool use_full_key= keypart_map == HA_WHOLE_KEY ||
                     keypart_map == (key_part_map(1) << key_part_cnt) - 1;

  uint32_t used_key_parts= key_part_cnt;
  if (!use_full_key)
  {
    used_key_parts= __builtin_popcountll(keypart_map);
  }

  DBUG_ASSERT(my_tx->Txm() != nullptr);

  if (active_index == table->s->primary_key &&
      find_flag == HA_READ_KEY_EXACT && use_full_key)
  {
    // Primary key lookup.
    EncodeScanKey(search_key_, key, keypart_map, pk_descr_.get());

    RecordStatus rec_stat= PkRead(my_tx, search_tx_key_, last_read_record_);

    if (rec_stat == RecordStatus::Unknown)
    {
      // It is kv store error.
      table->status= STATUS_NOT_FOUND;
      DBUG_RETURN(convert_tx_error(my_tx->tx_err_code_));
    }
    else if (rec_stat == RecordStatus::Normal)
    {
      DecodeRecord(buf, &search_key_, &last_read_record_);
      table->status= 0;
      DBUG_RETURN(0);
    }
    else
    {
      table->status= STATUS_NOT_FOUND;
      DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
    }
  }
  else if (active_index == table_share->primary_key)
  {
    // Primary key range scan.

    bool start_inclusive= false, end_specified= false, end_inclusive= false;
    ScanDirection scan_direction;
    if (IndexScanIsOpen())
    {
      // Close open index scan first before setting
      // scan parameters.
      IndexScanClose();
    }
    int rc= PrepareScan(key, keypart_map, find_flag, pk_descr_.get(),
                        key_part_cnt, use_full_key, start_inclusive,
                        end_specified, end_inclusive, scan_direction);

    if (rc != 0)
    {
      return rc;
    }

    if (end_specified)
    {
      rc= PkIndexScanOpen(&search_tx_key_, start_inclusive, &scan_end_tx_key_,
                          end_inclusive, scan_direction, used_key_parts);
    }
    else
    {
      rc= PkIndexScanOpen(&search_tx_key_, start_inclusive, nullptr, true,
                          scan_direction, used_key_parts);
    }

    if (rc != 0)
    {
      table->status= STATUS_NOT_FOUND;
      DBUG_RETURN(rc);
    }

    rc= PkIndexScanNext(buf);
    if (rc != 0)
    {
      table->status= STATUS_NOT_FOUND;
      DBUG_RETURN(rc);
    }
    else
    {
      table->status= 0;
      DBUG_RETURN(0);
    }
  }
  else if (active_index != table_share->primary_key &&
           table_share->key_info[active_index].flags & HA_NOSAME &&
           find_flag == HA_READ_KEY_EXACT && use_full_key)
  {
    // Unique secondary index lookup.

    const auto &[index_name, index_schema]=
        table_schema_->IndexNameSchema(active_index);
    const EloqKeySchema *sk_sch=
        static_cast<const EloqKeySchema *>(index_schema.sk_schema_.get());

    // Read query do not know trailing pk columns, so here sk_packed_tuple_
    // contains sk only cols.
    size_t unique_sk_pack_size= sk_sch->KeyDefinition()->pack_index_tuple(
        table, pack_buffer_, sk_packed_tuple_, record_buffer_, key,
        keypart_map, nullptr);

    if (table->key_info[active_index].user_defined_key_parts !=
        sk_sch->KeyDefinition()->get_key_parts())
    {
      // m_key_parts includes extended pk, user_defined_key_parts does not.
      use_full_key= false;
    }

    EloqKey unique_sk(sk_packed_tuple_, unique_sk_pack_size);
    RecordStatus rec_stat;
    uint64_t unique_sk_ts;
    std::tie(rec_stat, unique_sk_ts)=
        SkRead(my_tx, unique_sk, last_read_record_, false, active_index);

    if (rec_stat == RecordStatus::Unknown)
    {
      table->status= STATUS_NOT_FOUND;
      DBUG_RETURN(convert_tx_error(my_tx->tx_err_code_));
    }
    else if (rec_stat == RecordStatus::Normal)
    {
      DecodeUniqueSkRecord(buf, &unique_sk, &last_read_record_, sk_sch);

      if (!(table->covering_keys.is_set(active_index)))
      {
        rec_stat= PkRead(my_tx, last_read_tx_key_, last_read_record_, false,
                         unique_sk_ts);

        if (rec_stat == RecordStatus::Normal)
        {
          // A secondary index entry always points to a valid primary index
          // record.
          DecodeRecord(buf, &last_read_key_, &last_read_record_);
        }
        else
        {
          // It is kv store error.
          assert(rec_stat == RecordStatus::Unknown);
          DBUG_RETURN(convert_tx_error(my_tx->tx_err_code_));
        }
      }
      table->status= 0;
      DBUG_RETURN(0);
    }
    else
    {
      table->status= STATUS_NOT_FOUND;
      DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
    }
  }
  else if (active_index != table_share->primary_key)
  {
    // Secondary index scan

    const EloqKeySchema *sk_sch= reinterpret_cast<const EloqKeySchema *>(
        table_schema_->IndexNameSchema(active_index).second.sk_schema_.get());

    bool start_inclusive= false, end_specified= false, end_inclusive= false;
    ScanDirection scan_direction;
    if (IndexScanIsOpen())
    {
      // Close open index scan first before setting
      // scan parameters.
      IndexScanClose();
    }
    int rc=
        PrepareScan(key, keypart_map, find_flag, sk_sch->KeyDefinition().get(),
                    key_part_cnt, use_full_key, start_inclusive, end_specified,
                    end_inclusive, scan_direction);

    if (rc != 0)
    {
      DBUG_RETURN(rc);
    }

    if (end_specified)
    {
      rc= SkIndexScanOpen(&search_tx_key_, start_inclusive, &scan_end_tx_key_,
                          end_inclusive, scan_direction, used_key_parts);
    }
    else
    {
      rc= SkIndexScanOpen(&search_tx_key_, start_inclusive, nullptr, true,
                          scan_direction, used_key_parts);
    }

    if (rc != 0)
    {
      table->status= STATUS_NOT_FOUND;
      DBUG_RETURN(rc);
    }

    rc= SkIndexScanNext(buf);
    if (rc != 0)
    {
      table->status= STATUS_NOT_FOUND;
      DBUG_RETURN(rc);
    }
    else
    {
      table->status= 0;
      DBUG_RETURN(0);
    }
  }

  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

/**
  @brief
  Used to read forward through the index.
*/

int ha_eloq::index_next(uchar *buf)
{
  DBUG_ENTER_FUNC();

  int rc= active_index == table->s->primary_key ? PkIndexScanNext(buf)
                                                : SkIndexScanNext(buf);

  if (rc != 0)
  {
    if (rc == HA_ERR_END_OF_FILE || rc == HA_ERR_KEY_NOT_FOUND)
    {
      DBUG_RETURN(HA_ERR_END_OF_FILE);
    }
    else
    {
      DBUG_RETURN(rc);
    }
  }
  else
  {
    table->status= 0;
    DBUG_RETURN(0);
  }
}

/**
  @brief
  Used to read backwards through the index.
*/

int ha_eloq::index_prev(uchar *buf)
{
  // A scanner can scan forward or backward, depending on how the scan is
  // specified when the index scan starts. So, index_next() and index_prev()
  // share the same implementation.
  return index_next(buf);
}

/**
  @brief
  index_first() asks for the first key in the index.

  @details
  Called from opt_range.cc, opt_sum.cc, sql_handler.cc, and sql_select.cc.

  @see
  opt_range.cc, opt_sum.cc, sql_handler.cc and sql_select.cc
*/
int ha_eloq::index_first(uchar *buf)
{
  DBUG_ENTER_FUNC();
  assert(active_index != MAX_INDEXES);

  int rc= 0;

  SetupDecodeFlagOnFirstRead();

  KEY *key_info= &table->s->key_info[active_index];
  uint32_t key_part_cnt= table->actual_n_key_parts(key_info);

  if (active_index == table->s->primary_key)
  {
    bool end_inclusive= false;
    int pre_ret=
        PrepareForwardScanEndKey(pk_descr_.get(), key_part_cnt, end_inclusive);

    if (pre_ret == 0)
    {
      rc= PkIndexScanOpen(EloqKey::NegInfTxKey(), false, &scan_end_tx_key_,
                          end_inclusive, ScanDirection::Forward, UINT16_MAX);
    }
    else
    {
      rc= PkIndexScanOpen(EloqKey::NegInfTxKey(), false, nullptr, false,
                          ScanDirection::Forward, UINT16_MAX);
    }
  }
  else
  {
    const EloqKeySchema *sk_sch= reinterpret_cast<const EloqKeySchema *>(
        table_schema_->IndexNameSchema(active_index).second.sk_schema_.get());

    bool end_inclusive= false;
    int pre_ret= PrepareForwardScanEndKey(sk_sch->KeyDefinition().get(),
                                          key_part_cnt, end_inclusive);
    if (pre_ret == 0)
    {
      rc= SkIndexScanOpen(EloqKey::NegInfTxKey(), false, &scan_end_tx_key_,
                          end_inclusive, ScanDirection::Forward, UINT16_MAX);
    }
    else
    {
      rc= SkIndexScanOpen(EloqKey::NegInfTxKey(), false, nullptr, false,
                          ScanDirection::Forward, UINT16_MAX);
    }
  }

  if (rc != 0)
  {
    DBUG_RETURN(rc);
  }

  rc= active_index == table->s->primary_key ? PkIndexScanNext(buf)
                                            : SkIndexScanNext(buf);

  if (rc != 0)
  {
    table->status= STATUS_NOT_FOUND;
    if (rc == HA_ERR_LOCK_WAIT_TIMEOUT)
    {
      DBUG_RETURN(rc);
    }
    else
    {
      DBUG_RETURN(HA_ERR_END_OF_FILE);
    }
  }
  else
  {
    table->status= 0;
    DBUG_RETURN(0);
  }
}

int ha_eloq::index_init(uint idx, bool sorted)
{
  DBUG_ENTER_FUNC();

  need_setup_decode_flag_= true;

  DBUG_RETURN(0);
}

int ha_eloq::index_end()
{
  DBUG_ENTER_FUNC();

  need_setup_decode_flag_= false;

  if (IndexScanIsOpen())
  {
    IndexScanClose();
  }

  m_ds_mrr_.dsmrr_close();
  DBUG_RETURN(0);
}

int ha_eloq::prepare_index_scan()
{
  range_key_part= table->key_info[active_index].key_part;
  set_end_range(NULL);
  return 0;
}

int ha_eloq::prepare_range_scan(const key_range *start_key,
                                const key_range *end_key)
{
  range_key_part= table->key_info[active_index].key_part;

  if (start_key)
  {
    start_range_= *start_key;
    start_range_ptr_= &start_range_;
  }
  else
  {
    start_range_ptr_= nullptr;
  }

  set_end_range(end_key);
  return 0;
}

/**
  @brief
  index_last() asks for the last key in the index.

  @details
  Called from opt_range.cc, opt_sum.cc, sql_handler.cc, and sql_select.cc.

  @see
  opt_range.cc, opt_sum.cc, sql_handler.cc and sql_select.cc
*/
int ha_eloq::index_last(uchar *buf)
{
  DBUG_ENTER_FUNC();
  assert(active_index != MAX_INDEXES);

  int rc= 0;

  SetupDecodeFlagOnFirstRead();

  if (active_index == table->s->primary_key)
  {
    rc= PkIndexScanOpen(EloqKey::PosInfTxKey(), false, nullptr, false,
                        ScanDirection::Backward, UINT16_MAX);
  }
  else
  {
    rc= SkIndexScanOpen(EloqKey::PosInfTxKey(), false, nullptr, false,
                        ScanDirection::Backward, UINT16_MAX);
  }

  if (rc != 0)
  {
    DBUG_RETURN(rc);
  }

  rc= active_index == table->s->primary_key ? PkIndexScanNext(buf)
                                            : SkIndexScanNext(buf);

  if (rc != 0)
  {
    table->status= STATUS_NOT_FOUND;
    if (rc == HA_ERR_LOCK_WAIT_TIMEOUT)
    {
      DBUG_RETURN(rc);
    }
    else
    {
      DBUG_RETURN(HA_ERR_END_OF_FILE);
    }
  }
  else
  {
    table->status= 0;
    DBUG_RETURN(0);
  }
}

/**
  @brief
  rnd_init() is called when the system wants the storage engine to do a table
  scan. See the example in the introduction at the top of this file to see
  when rnd_init() is called.

  @details
  Called from filesort.cc, records.cc, sql_handler.cc, sql_select.cc,
  sql_table.cc, and sql_update.cc.

  @see
  filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc and
  sql_update.cc
*/
int ha_eloq::rnd_init(bool scan)
{
  DBUG_ENTER_FUNC();

  // Notice that not only hidden pk scan call rnd_*, but also non-hidden pk
  // scan. For example:
  //
  // CREATE TABLE `t1` (
  //    `i` int(11) NOT NULL,
  //    `j` int(11) DEFAULT NULL,
  //    PRIMARY KEY (`i`)
  // );
  //
  // select * from t1;
  DBUG_ASSERT(active_index == MAX_INDEXES);

  // Delay SetupDecodeFlagOnFirstRead() and PkIndexScanOpen(), because read_set
  // might changed after rnd_init().
  need_setup_decode_flag_= true;
  start_of_scan_= true;
  is_mrr_sort_rowid_= false;
  DBUG_RETURN(0);
}

int ha_eloq::rnd_end()
{
  DBUG_ENTER_FUNC();

  need_setup_decode_flag_= false;

  if (start_of_scan_)
  {
    // rnd_init() has been called, but no rnd_next(). Skip scan close.
    start_of_scan_= false;
  }
  else
  {
    IndexScanClose();
  }

  DBUG_RETURN(0);
}

/**
  @brief
  This is called for each row of the table scan. When you run out of records
  you should return HA_ERR_END_OF_FILE. Fill buff up with the row
  information. The Field structure for the table is the key to getting data
  into buf in a manner that will allow the server to understand it.

  @details
  Called from filesort.cc, records.cc, sql_handler.cc, sql_select.cc,
  sql_table.cc, and sql_update.cc.

  @see
  filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc and
  sql_update.cc
*/
int ha_eloq::rnd_next(uchar *buf)
{
  DBUG_ENTER_FUNC();

  int rc= 0;

  SetupDecodeFlagOnFirstRead();

  if (start_of_scan_)
  {
    rc= PkIndexScanOpen(EloqKey::NegInfTxKey(), false, nullptr, false,
                        ScanDirection::Forward, UINT16_MAX);
    if (rc != 0)
    {
      DBUG_RETURN(rc);
    }
    start_of_scan_= false;
  }

  rc= PkIndexScanNext(buf);
  if (rc != 0)
  {
    DBUG_RETURN(rc);
  }
  else
  {
    table->status= 0;
    DBUG_RETURN(0);
  }
}

/**
  @brief
  position() is called after each call to rnd_next() if the data needs
  to be ordered. You can do something like the following to store
  the position:
  @code
  my_store_ptr(ref, ref_length, current_position);
  @endcode

  @details
  The server uses ref to store data. ref_length in the above case is
  the size needed to store current_position. ref is just a byte array
  that the server will maintain. If you are using offsets to mark rows, then
  current_position should be the offset. If it is a primary key like in
  BDB, then it needs to be a primary key.

  Called from filesort.cc, sql_select.cc, sql_delete.cc, and sql_update.cc.

  @see
  filesort.cc, sql_select.cc, sql_delete.cc and sql_update.cc
*/
void ha_eloq::position(const uchar *record)
{
  DBUG_ENTER_FUNC();

  size_t packed_key_len;
  /** When is_mrr_sort_rowid_=true, this method will copy primary key into ref
   *  and will save it into the MRR buffer for following actions*/
  if (!has_hidden_pk(table) && !is_mrr_sort_rowid_)
  {
    // The table has a user-defined primary key.

    DBUG_ASSERT(hidden_key_schema_ == nullptr);

    // Pack record into packed key format
    packed_key_len= pk_descr_->pack_record(table, pack_buffer_, record, ref,
                                           nullptr, false);
  }
  else
  {
    // read hidden key from packed last read key
    packed_key_len= last_read_key_.PackedValueSlice().size();
    memcpy(ref, last_read_key_.PackedValue().data(), packed_key_len);
  }

  // It could be that mem-comparable form of PK occupies less than
  // ref_length bytes. Fill the remainder with zeros.
  if (ref_length > packed_key_len)
  {
    memset(ref + packed_key_len, 0, ref_length - packed_key_len);
  }

  DBUG_VOID_RETURN;
}

/**
  @brief
  This is like rnd_next, but you are given a position to use
  to determine the row. The position will be of the type that you stored in
  ref. You can use ha_get_ptr(pos,ref_length) to retrieve whatever key
  or position you saved when position() was called.

  @details
  Called from filesort.cc, records.cc, sql_insert.cc, sql_select.cc, and
  sql_update.cc.

  @see
  filesort.cc, records.cc, sql_insert.cc, sql_select.cc and sql_update.cc
*/
int ha_eloq::rnd_pos(uchar *buf, uchar *pos)
{
  DBUG_ENTER_FUNC();
  DBUG_DUMP("key", pos, ref_length);

  SetupDecodeFlagOnFirstRead();

  // position() stores pos(ref) as primary key. Hence rnd_pos() use PkRead to
  // get the record.
  MyEloqTx *my_tx= get_myeloq_tx(ha_thd());
  DBUG_ASSERT(my_tx != nullptr);

  // ref length is the max key length. Remove the padding 0s to get
  // the actual key length.
  Slice temp_last_key((const char *) pos, ref_length);
  size_t key_length= pk_descr_->key_length(table, temp_last_key);
  last_read_key_= EloqKey(pos, key_length);

  DBUG_ASSERT(my_tx->Txm() != nullptr);
  RecordStatus rec_stat= PkRead(my_tx, last_read_tx_key_, last_read_record_);

  if (rec_stat == RecordStatus::Unknown)
  {
    DBUG_RETURN(convert_tx_error(my_tx->tx_err_code_));
  }
  else if (rec_stat == RecordStatus::Normal)
  {
    DecodeRecord(buf, &last_read_key_, &last_read_record_);
    DBUG_RETURN(0);
  }
  else
  {
    DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
  }
}

/**
  @brief
  ::info() is used to return information to the optimizer. See my_base.h for
  the complete description.

  @details
  Currently this table handler doesn't implement most of the fields really
  needed. SHOW also makes use of this data.

  You will probably want to have the following in your code:
  @code
  if (records < 2)
    records = 2;
  @endcode
  The reason is that the server will optimize for cases of only a single
  record. If, in a table scan, you don't know the number of records, it
  will probably be better to set records to two so you can return as many
  records as you need. Along with records, a few more variables you may wish
  to set are:
    records
    deleted
    data_file_length
    index_file_length
    delete_length
    check_time
  Take a look at the public variables in handler.h for more information.

  Called in filesort.cc, ha_heap.cc, item_sum.cc, opt_sum.cc, sql_delete.cc,
  sql_delete.cc, sql_derived.cc, sql_select.cc, sql_select.cc, sql_select.cc,
  sql_select.cc, sql_select.cc, sql_show.cc, sql_show.cc, sql_show.cc,
  sql_show.cc, sql_table.cc, sql_union.cc, and sql_update.cc.

  @see
  filesort.cc, ha_heap.cc, item_sum.cc, opt_sum.cc, sql_delete.cc,
  sql_delete.cc, sql_derived.cc, sql_select.cc, sql_select.cc, sql_select.cc,
  sql_select.cc, sql_select.cc, sql_show.cc, sql_show.cc, sql_show.cc,
  sql_show.cc, sql_table.cc, sql_union.cc and sql_update.cc
*/
int ha_eloq::info(uint flag)
{
  DBUG_ENTER_FUNC();

  THD *thd= ha_thd();
  MyEloqTx *my_tx= nullptr;
  bool tx_exists= false;
  if (!get_or_create_myeloq_tx(thd, &my_tx, &tx_exists))
  {
    my_error(HA_ERR_ELOQ_START_TRANSACTION_FAILED, MYF(0));
    DBUG_RETURN(HA_ERR_ELOQ_START_TRANSACTION_FAILED);
  }
  // table_schema_ is not available outside of transaction context.
  DBUG_ASSERT(tx_exists);

  if (flag & HA_STATUS_VARIABLE)
  {

    const Distribution *distribution=
        GetDistribution(table_share->primary_key);
    stats.records= distribution->Records();

    // Do like InnoDB does. stats.records=0 confuses the optimizer
    if (stats.records == 0 && !(flag & (HA_STATUS_TIME | HA_STATUS_OPEN)))
    {
      stats.records++;
    }

    stats.deleted= 0;
    stats.data_file_length= 16384;
  }
  if (flag & HA_STATUS_CONST)
  {
    ref_length= pk_descr_->max_storage_fmt_length();

    for (uint i= 0; i < table_share->keys; i++)
    {
      if (EloqRecordSchema::IsHiddenPk(i, table_share))
      {
        continue;
      }

      KEY *const k= &table->key_info[i];

      const txservice::KeySchema *key_schema_ptr= nullptr;
      if (i == table_share->primary_key)
      {
        key_schema_ptr= table_schema_->KeySchema();
      }
      else
      {
        const auto &[indexname, key_schema]= table_schema_->IndexNameSchema(i);
        key_schema_ptr= &key_schema;
      }

      const Distribution *distribution= GetDistribution(i);
      std::vector<double> rec_per_key= distribution->RecordsPerKey();
      for (uint j= 0; j < k->ext_key_parts; j++)
      {
        uint x= 0;
        DBUG_ASSERT(key_schema_ptr->ExtendKeyParts() == rec_per_key.size());

        x= std::round(rec_per_key[j]);
        if (x > stats.records)
        {
          x= stats.records;
        }
        k->rec_per_key[j]= x;
      }

      (void) key_schema_ptr;
    }

    stats.create_time= std::chrono::duration_cast<std::chrono::seconds>(
                           std::chrono::microseconds(table_schema_->Version()))
                           .count();
  }

  if (flag & HA_STATUS_ERRKEY)
  {

    // Currently we support only primary keys.
    errkey= dup_errkey_;
  }

  DBUG_RETURN(0);
}

/**
  @brief
  extra() is called whenever the server wishes to send a hint to
  the storage engine. The myisam engine implements the most hints.
  ha_innodb.cc has the most exhaustive list of these hints.

    @see
  ha_innodb.cc
*/
int ha_eloq::extra(enum ha_extra_function operation)
{
  DBUG_ENTER_FUNC();

  switch (operation)
  {
  case HA_EXTRA_KEYREAD:
    break;
  case HA_EXTRA_NO_KEYREAD:
    break;
  case HA_EXTRA_FLUSH:
    /*
      If the table has blobs, then they are part of m_retrieved_record.
      This call invalidates them.
    */
    break;
  case HA_EXTRA_INSERT_WITH_UPDATE:
    // INSERT ON DUPLICATE KEY UPDATE
    break;
  case HA_EXTRA_NO_IGNORE_DUP_KEY:
    // PAIRED with HA_EXTRA_INSERT_WITH_UPDATE or HA_EXTRA_WRITE_CAN_REPLACE
    // that indicates the end of REPLACE / INSERT ON DUPLICATE KEY
    break;
  case HA_EXTRA_IGNORE_DUP_KEY:
    /**
     * Ignore or update the key if duplicate key was found.
     */
    is_duplicate_error_= false;
    break;

  default:
    break;
  }

  DBUG_RETURN(0);
}

/**
  @brief
  Used to delete all rows in a table, including cases of truncate and cases
  where the optimizer realizes that all rows will be removed as a result of
  an SQL statement.

  @details
  Called from item_sum.cc by Item_func_group_concat::clear(),
  Item_sum_count_distinct::clear(), and Item_func_group_concat::clear().
  Called from sql_delete.cc by mysql_delete().
  Called from sql_select.cc by JOIN::reinit().
  Called from sql_union.cc by st_select_lex_unit::exec().

  @see
  Item_func_group_concat::clear(), Item_sum_count_distinct::clear() and
  Item_func_group_concat::clear() in item_sum.cc;
  mysql_delete() in sql_delete.cc;
  JOIN::reinit() in sql_select.cc and
  st_select_lex_unit::exec() in sql_union.cc.
*/
int ha_eloq::delete_all_rows()
{
  std::cout << "delete_all_rows() unimplemented" << std::endl;
  DBUG_ENTER("ha_eloq::delete_all_rows");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

/**
  @brief
  This create a lock on the table. If you are implementing a storage engine
  that can handle transacations look at ha_berkely.cc to see how you will
  want to go about doing this. Otherwise you should consider calling flock()
  here. Hint: Read the section "locking functions for mysql" in lock.cc to
  understand this.

  @details
  Called from lock.cc by lock_external() and unlock_external(). Also called
  from sql_table.cc by copy_data_between_tables().

  @see
  lock.cc by lock_external() and unlock_external() in lock.cc;
  the section "locking functions for mysql" in lock.cc;
  copy_data_between_tables() in sql_table.cc.
*/
int ha_eloq::external_lock(THD *thd, int lock_type)
{
  DBUG_ENTER_FUNC();

  {
    enum_sql_command sqlcom= enum_sql_command(thd_sql_command(thd));
    switch (sqlcom)
    {
    case SQLCOM_LOCK_TABLES:
    case SQLCOM_UNLOCK_TABLES:
    case SQLCOM_RENAME_TABLE:
    case SQLCOM_OPTIMIZE:
    case SQLCOM_CHECK:
    case SQLCOM_REPAIR:
    case SQLCOM_CHECKSUM:
      my_error(ER_CHECK_NOT_IMPLEMENTED, MYF(0),
               eloq_unsupported_command.find(sqlcom)->second.c_str());
      DBUG_RETURN(HA_ERR_WRONG_COMMAND);
    default:
      break;
    }
  }

  if (lock_type == F_UNLCK)
  {
    // auto t1= std::chrono::high_resolution_clock::now();

    /*end_time_= std::chrono::high_resolution_clock::now();
    auto exec_time= end_time_ - begin_time_;

    commit_time_= end_time_ - t1;

    if (exec_time >= std::chrono::milliseconds(2))
    {
      std::cout
          << "long running tx. Exec time: "
          << std::chrono::duration_cast<std::chrono::microseconds>(exec_time)
                 .count()
          << ", begin time: "
          <<
    std::chrono::duration_cast<std::chrono::microseconds>(start_time_)
                 .count()
          << ", commit time: "
          << std::chrono::duration_cast<std::chrono::microseconds>(
                 commit_time_)
                 .count()
          << std::endl;
    }*/

    // Leaves the table
  }
  else
  {
    MyEloqTx *my_tx= nullptr;
    if (!get_or_create_myeloq_tx(thd, &my_tx))
    {
      my_error(HA_ERR_ELOQ_START_TRANSACTION_FAILED, MYF(0));
      DBUG_RETURN(HA_ERR_ELOQ_START_TRANSACTION_FAILED);
    }
    DBUG_ASSERT(my_tx != nullptr);
    eloq_register_tx(eloq_hton, thd, my_tx);
    my_tx->IncreTable();
  }

  DBUG_RETURN(0);
}

int ha_eloq::start_stmt(THD *const thd, thr_lock_type lock_type)
{
  DBUG_ENTER_FUNC();

  MyEloqTx *my_tx= nullptr;
  if (!get_or_create_myeloq_tx(thd, &my_tx))
  {
    my_error(HA_ERR_ELOQ_START_TRANSACTION_FAILED, MYF(0));
    DBUG_RETURN(HA_ERR_ELOQ_START_TRANSACTION_FAILED);
  }
  DBUG_ASSERT(my_tx != nullptr);
  eloq_register_tx(eloq_hton, thd, my_tx);

  DBUG_RETURN(0);
}

/**
  @brief
  The idea with handler::store_lock() is: The statement decides which locks
  should be needed for the table. For updates/deletes/inserts we get WRITE
  locks, for SELECT... we get read locks.

  @details
  Before adding the lock into the table lock handler (see thr_lock.c),
  mysqld calls store lock with the requested locks. Store lock can now
  modify a write lock to a read lock (or some other lock), ignore the
  lock (if we don't want to use MySQL table locks at all), or add locks
  for many tables (like we do when we are using a MERGE handler).

  Berkeley DB, for example, changes all WRITE locks to TL_WRITE_ALLOW_WRITE
  (which signals that we are doing WRITES, but are still allowing other
  readers and writers).

  When releasing locks, store_lock() is also called. In this case one
  usually doesn't have to do anything.

  In some exceptional cases MySQL may send a request for a TL_IGNORE;
  This means that we are requesting the same lock as last time and this
  should also be ignored. (This may happen when someone does a flush
  table when we have opened a part of the tables, in which case mysqld
  closes and reopens the tables and tries to get the same locks at last
  time). In the future we will probably try to remove this.

  Called from lock.cc by get_lock_data().

  @note
  In this method one should NEVER rely on table->in_use, it may, in fact,
  refer to a different thread! (this happens if get_lock_data() is called
  from mysql_lock_abort_for_thread() function)

  @see
  get_lock_data() in lock.cc
*/

THR_LOCK_DATA **ha_eloq::store_lock(THD *thd, THR_LOCK_DATA **to,
                                    enum thr_lock_type lock_type)
{
  DBUG_ENTER_FUNC();
  // The following logic is copied from ha_cassandra.cc
  if (lock_type != TL_IGNORE && lock.type == TL_UNLOCK)
  {
    /* Writes allow other writes */
    if ((lock_type >= TL_WRITE_CONCURRENT_INSERT && lock_type <= TL_WRITE))
    {
      lock_type= TL_WRITE_ALLOW_WRITE;
    }

    /* Reads allow everything, including INSERTs */
    if (lock_type == TL_READ_NO_INSERT)
    {
      lock_type= TL_READ;
    }

    lock.type= lock_type;
  }
  *to++= &lock;
  DBUG_RETURN(to);
}

const COND *ha_eloq::cond_push(const COND *cond)
{
  DBUG_ENTER_FUNC();
  // cond_push() may be called twice, one during query compilation and the
  // other right before the table is scanned. Skips the second call if the
  // pushed-down conditions have been constructed.
  if (!pushed_conds_.empty())
  {
    DBUG_RETURN(cond);
  }

  if (cond->type() == COND::COND_ITEM)
  {
    Item_cond *cond_item= (Item_cond *) cond;
    if (cond_item->functype() != Item_func::COND_AND_FUNC)
    {
      DBUG_RETURN(cond);
    }

    List<Item> *arglist= cond_item->argument_list();
    List_iterator<Item> li(*arglist);
    Item *sub_cond_item= nullptr;

    for (uint32_t c_idx= 0; c_idx < arglist->elements; ++c_idx)
    {
      sub_cond_item= li++;
      AddPushedDownCondition(sub_cond_item);
    }
  }
  else if (cond->type() == COND::FUNC_ITEM)
  {
    Item_func *cond_item= (Item_func *) cond;
    AddPushedDownCondition(cond_item);
  }
  // TODO(haozhang): return nullptr if all conditions pushed down
  DBUG_RETURN(cond);
}

void ha_eloq::unlock_row()
{
  if (scan_alias_ < UINT64_MAX && !scan_batch_.empty())
  {
    // scan_batch_idx_ points to the next record to be scanned.
    size_t curr_scan_idx= scan_batch_idx_ - 1;
    assert(curr_scan_idx < scan_batch_.size());
    const txservice::ScanBatchTuple &tuple= scan_batch_[curr_scan_idx];
    unlock_batch_.emplace_back(tuple.cce_addr_, tuple.version_ts_,
                               tuple.status_);
  }
}

int ha_eloq::analyze(THD *thd, HA_CHECK_OPT *check_opt)
{
  DBUG_ENTER_FUNC();

  int ret= 0;

#ifdef RANGE_PARTITION_ENABLED
  MyEloqTx *my_tx= get_myeloq_tx(thd);
  DBUG_ASSERT(my_tx != nullptr);

  auto [yield_func, resume_func]= my_tx->CoroFunctors();
  TransactionExecution *txm= my_tx->Txm();
  DBUG_ASSERT(txm != nullptr);

  std::vector<TableName> table_names= table_schema_->IndexNames();
  table_names.push_back(base_table_name_);

  for (const TableName &table_name : table_names)
  {
    TableName scan_table_name(table_name.StringView(),
                              TableType::RangePartition, table_name.Engine());
    ScanIndexType scan_type= table_name.Type() == TableType::Primary
                                 ? ScanIndexType::Primary
                                 : ScanIndexType::Secondary;
    const txservice::TxKey *start_key= EloqKey::NegInfTxKey();
    const txservice::TxKey *end_key= EloqKey::PosInfTxKey();
    bool start_inclusive= false;
    bool end_inclusive= false;
    ScanDirection scan_direction= ScanDirection::Forward;

    // uint64_t schema_version= txm->GetSchemaVersion(table_name);
    ScanOpenTxRequest scan_open_req(
        &scan_table_name, 0, scan_type, start_key, start_inclusive, end_key,
        end_inclusive, scan_direction, false, false, false, true, true, true,
        true, true, yield_func, resume_func, txm);
    txm->Execute(&scan_open_req);
    scan_open_req.Wait();
    if (scan_open_req.IsError())
    {
      ret= HA_ADMIN_FAILED;
      break;
    }

    size_t scan_alias= scan_open_req.Result();
    std::vector<txservice::ScanBatchTuple> scan_batch;
    do
    {
      scan_batch.clear();
      ScanBatchTxRequest scan_batch_req(scan_alias, scan_table_name,
                                        &scan_batch, yield_func, resume_func,
                                        txm);
      txm->Execute(&scan_batch_req);
      scan_batch_req.Wait();
      if (scan_batch_req.IsError())
      {
        ret= HA_ADMIN_FAILED;
        break;
      }
    } while (!scan_batch.empty());

    if (!ret)
    {
      // We have locked all ranges.
      AnalyzeTableTxRequest analyze_req(&table_name, yield_func, resume_func,
                                        txm);

      txm->Execute(&analyze_req);
      analyze_req.Wait();
      if (analyze_req.IsError())
      {
        ret= HA_ADMIN_FAILED;
      }
    }

    ScanCloseTxRequest scan_close_req(scan_batch, 0, scan_alias,
                                      scan_table_name, yield_func, resume_func,
                                      txm);
    txm->Execute(&scan_close_req);
    scan_close_req.Wait();
    assert(!scan_close_req.IsError());

    if (ret)
    {
      break;
    }
  }
#endif

  // A call to ::info is needed to repopulate some SQL level structs. This is
  // necessary for online analyze because we cannot rely on another ::open
  // call to call info for us.
  if (!ret && info(HA_STATUS_CONST | HA_STATUS_VARIABLE) != 0)
  {
    ret= HA_ADMIN_FAILED;
  }

  DBUG_RETURN(ret);
}

void ha_eloq::DecodeRecord(uchar *table_record, const EloqKey *key,
                           const EloqRecord *rec, bool is_ckpt,
                           bool is_deleted)
{
  if (!has_hidden_pk(table) && (decode_flag_ & DECODE_PK))
  {
    int err=
        pk_descr_->unpack_record(table, table_record, key->PackedValueSlice(),
                                 rec->UnpackInfoSlice(), false);
    if (err)
    {
      my_error(HA_ERR_TABLE_CORRUPT, MYF(0));
    }
  }

  if (decode_flag_ & DECODE_PAYLOAD)
  {
    MY_BITMAP *old_map;
    old_map= dbug_tmp_use_all_columns(table, &table->write_set);

    const EloqRecordSchema &rec_schema= *GetRecordSchema();
    rec_schema.Decode(DecodeSet(), table_record, table->field,
                      table->record[0], rec->encoded_blob_, is_ckpt,
                      is_deleted);
    dbug_tmp_restore_column_map(&table->write_set, old_map);
  }
}

void ha_eloq::DecodeUniqueSkRecord(uchar *table_record, EloqKey *key,
                                   EloqRecord *rec,
                                   const EloqKeySchema *sk_schema)
{
  /*
  last_read_record_.encoded_blob_ only contains packed trailing pk columns
  that do not also belong to unique_sk. So we need to concatenate packed
  unique_sk with packed trailing pk columns, then use get_primary_key_tuple()
  to get the packed full pk.

  e.g.,
  unique_sk(c1,c2,c3)
  pk(c3,c4,c5)

  here trailing pk columns are (c4,c5) and these two columns are packed
  together and stored in last_read_record_.encoded_blob_
  */

  std::string sk_packed_tuple;
  sk_packed_tuple.append(key->PackedValueSlice().data_,
                         key->PackedValueSlice().size_);
  sk_packed_tuple.append(rec->encoded_blob_.begin(), rec->encoded_blob_.end());
  Slice s{sk_packed_tuple.data(), sk_packed_tuple.size()};
  sk_schema->KeyDefinition()->unpack_record(table, table_record, s,
                                            rec->UnpackInfoSlice(), false);

  // Reconstruct full pk from unique_sk+trailing_pk_cols.
  size_t pk_len= sk_schema->KeyDefinition()->get_primary_key_tuple(
      table, *pk_descr_, s, pk_packed_tuple_);
  last_read_key_= EloqKey(pk_packed_tuple_, pk_len);
}

int ha_eloq::Update(const uchar *new_data, const uchar *old_data,
                    std::unique_ptr<EloqKey> mono_key,
                    std::unique_ptr<EloqRecord> new_mono_rec)
{
  THD *thd= ha_thd();
  MyEloqTx *my_tx= get_myeloq_tx(thd);
  DBUG_ASSERT(my_tx != nullptr);

  TransactionExecution *txm= my_tx->Txm();
  DBUG_ASSERT(txm != nullptr);

  const uchar *hidden_pk= has_hidden_pk(table)
                              ? (const uchar *) mono_key->PackedValue().data()
                              : nullptr;

  // Get the dirty indexes name.
  auto &dirty_index_names= my_tx->DirtyIndexNames();

  // No duplicate pk, need to also check unique_sk.
  bool is_unique_sk= false;
  bool is_dirty= false;
  auto dirty_it= dirty_index_names.begin();
  std::unique_ptr<EloqKey> old_sk= nullptr;
  std::unique_ptr<EloqKey> new_sk= nullptr;
  size_t old_unique_sk_pack_len= 0;
  size_t new_unique_sk_pack_len= 0;
  size_t old_pack_len= 0;
  size_t new_pack_len= 0;
  // Process all the indexes.
  for (size_t kid= 0;
       kid < table_share->keys || dirty_it != dirty_index_names.end();)
  {
    if (kid == table_share->primary_key)
    {
      ++kid;
      continue;
    }

    is_dirty= kid == table_share->keys ? true : false;
    // TODO(ysw): check unique for dirty index
    is_unique_sk=
        !is_dirty ? table_share->key_info[kid].flags & HA_NOSAME : false;

    const TableName &index_name=
        !is_dirty ? table_schema_->IndexNameSchema(kid).first : *dirty_it;
    const EloqKeySchema *index_schema= static_cast<const EloqKeySchema *>(
        !is_dirty ? table_schema_->IndexNameSchema(kid).second.sk_schema_.get()
                  : table_dirty_schema_->IndexKeySchema(index_name)
                        ->sk_schema_.get());

    old_sk= nullptr;
    new_sk= nullptr;
    old_unique_sk_pack_len= 0;
    new_unique_sk_pack_len= 0;
    old_pack_len= 0;
    new_pack_len= 0;

    old_pack_len= index_schema->KeyDefinition()->pack_record(
        table, pack_buffer_, old_data, sk_packed_tuple_, nullptr, false,
        &old_unique_sk_pack_len, hidden_pk);
    old_sk= std::make_unique<EloqKey>(sk_packed_tuple_,
                                      is_unique_sk ? old_unique_sk_pack_len
                                                   : old_pack_len);

    new_pack_len= index_schema->KeyDefinition()->pack_record(
        table, pack_buffer_, new_data, sk_packed_tuple_, &unpack_info_, false,
        &new_unique_sk_pack_len, hidden_pk);
    new_sk= std::make_unique<EloqKey>(sk_packed_tuple_,
                                      is_unique_sk ? new_unique_sk_pack_len
                                                   : new_pack_len);

    std::unique_ptr<EloqRecord> new_sk_rec= std::make_unique<EloqRecord>();

    // Only add modified SecondaryKey to write set.
    if (!(*new_sk == *old_sk))
    {
      if (is_unique_sk)
      {
        RecordStatus rec_status;
        uint64_t useless_ts;
        std::tie(rec_status, useless_ts)=
            SkRead(my_tx, *new_sk, *new_sk_rec, true, kid);
        if (rec_status == RecordStatus::Normal)
        {
          errkey= kid;
          dup_errkey_= errkey;
          return HA_ERR_FOUND_DUPP_KEY;
        }
        else if (rec_status == RecordStatus::Deleted)
        {
          new_sk_rec->SetEncodedBlob(sk_packed_tuple_ + new_unique_sk_pack_len,
                                     new_pack_len - new_unique_sk_pack_len);
        }
        else
        {
          // It is kv store error.
          assert(rec_status == RecordStatus::Unknown);
          return convert_tx_error(my_tx->tx_err_code_);
        }
      }

      new_sk_rec->SetUnpackInfo(unpack_info_.ptr(),
                                unpack_info_.get_current_pos());
      uint64_t sk_key_version= index_schema->SchemaTs();

      TxErrorCode err=
          txm->TxUpsert(index_name, sk_key_version, TxKey(std::move(new_sk)),
                        std::move(new_sk_rec), OperationType::Insert);
      if (err != TxErrorCode::NO_ERROR)
      {
        my_tx->tx_err_code_= err;
        // my_tx->detailed_err_message_= ups_sk_req.ErrorMsg();
        // my_error(HA_ERR_ELOQ_UPDATE_FAILED, MYF(0),
        //          my_tx->detailed_err_message_.data());
        return HA_ERR_OUT_OF_MEM;
      }

      err= txm->TxUpsert(index_name, sk_key_version, TxKey(std::move(old_sk)),
                         nullptr, OperationType::Delete);
      if (err != TxErrorCode::NO_ERROR)
      {
        my_tx->tx_err_code_= err;
        // my_tx->detailed_err_message_= ups_sk_req.ErrorMsg();
        // my_error(HA_ERR_ELOQ_UPDATE_FAILED, MYF(0),
        //          my_tx->detailed_err_message_.data());
        return HA_ERR_OUT_OF_MEM;
      }
    }

    // Move to the next index
    if (is_dirty)
    {
      ++dirty_it;
    }
    else
    {
      ++kid;
    }
  } /* End of indexes */

  uint64_t pk_key_version= table_schema_->KeySchema()->SchemaTs();

  // Key and record are allocated and moved to the tx request, which will
  // further transfer the ownership to the tx machine. Tx machine will own
  // them and use them for uploading when the tx starts committing.
  TxErrorCode err=
      txm->TxUpsert(*GetBaseTableNameFromTableSchema(), pk_key_version,
                    TxKey(std::move(mono_key)), std::move(new_mono_rec),
                    OperationType::Update);

  if (err != TxErrorCode::NO_ERROR)
  {
    my_tx->tx_err_code_= err;
    // my_tx->detailed_err_message_= ups_pk_req.ErrorMsg();
    // my_error(HA_ERR_ELOQ_UPDATE_FAILED, MYF(0),
    //          my_tx->detailed_err_message_.data());
    return HA_ERR_OUT_OF_MEM;
  }

  return 0;
}

int ha_eloq::Delete(const uchar *old_data, std::unique_ptr<EloqKey> mono_key)
{
  THD *thd= ha_thd();
  MyEloqTx *my_tx= get_myeloq_tx(thd);
  DBUG_ASSERT(my_tx != nullptr);

  TransactionExecution *txm= my_tx->Txm();
  DBUG_ASSERT(txm != nullptr);

  const uchar *hidden_pk= has_hidden_pk(table)
                              ? (const uchar *) mono_key->PackedValue().data()
                              : nullptr;

  // Get the dirty indexes name.
  auto &dirty_index_names= my_tx->DirtyIndexNames();

  // Process all the indexes data
  bool is_unique_key= false;
  bool is_dirty= false;
  auto dirty_it= dirty_index_names.begin();
  for (size_t kid= 0;
       kid < table_share->keys || dirty_it != dirty_index_names.end();)
  {
    if (kid == table_share->primary_key)
    {
      ++kid;
      continue;
    }

    is_dirty= kid == table_share->keys ? true : false;
    is_unique_key=
        !is_dirty ? table_share->key_info[kid].flags & HA_NOSAME : false;

    const txservice::TableName &index_name=
        !is_dirty ? table_schema_->IndexNameSchema(kid).first : *dirty_it;
    const EloqKeySchema *index_schema= static_cast<const EloqKeySchema *>(
        !is_dirty ? table_schema_->IndexNameSchema(kid).second.sk_schema_.get()
                  : table_dirty_schema_->IndexKeySchema(index_name)
                        ->sk_schema_.get());

    std::unique_ptr<EloqKey> old_sk= std::make_unique<EloqKey>(
        index_schema->KeyDefinition().get(), table, pack_buffer_, old_data,
        false, hidden_pk, is_unique_key);

    uint64_t sk_key_version= index_schema->SchemaTs();

    TxErrorCode err=
        txm->TxUpsert(index_name, sk_key_version, TxKey(std::move(old_sk)),
                      nullptr, OperationType::Delete);
    if (err != TxErrorCode::NO_ERROR)
    {
      my_tx->tx_err_code_= err;
      // my_tx->detailed_err_message_= ups_sk_req.ErrorMsg();
      // my_error(HA_ERR_ELOQ_UPDATE_FAILED, MYF(0),
      //          my_tx->detailed_err_message_.data());
      return HA_ERR_OUT_OF_MEM;
    }

    // Move to next index
    if (is_dirty)
    {
      ++dirty_it;
    }
    else
    {
      ++kid;
    }
  } /* End of indexes */

  uint64_t pk_key_version= table_schema_->KeySchema()->SchemaTs();

  TxErrorCode err= txm->TxUpsert(*GetBaseTableNameFromTableSchema(),
                                 pk_key_version, TxKey(std::move(mono_key)),
                                 nullptr, OperationType::Delete);

  if (err != TxErrorCode::NO_ERROR)
  {
    my_tx->tx_err_code_= err;
    // my_tx->detailed_err_message_= ups_pk_req.ErrorMsg();
    // my_error(HA_ERR_ELOQ_UPDATE_FAILED, MYF(0),
    //          my_tx->detailed_err_message_.data());
    return HA_ERR_OUT_OF_MEM;
  }

  return 0;
}

int ha_eloq::Insert(const uchar *buf, std::unique_ptr<EloqKey> eloq_key,
                    std::unique_ptr<EloqRecord> eloq_rec)
{
  THD *thd= ha_thd();
  MyEloqTx *my_tx= get_myeloq_tx(thd);
  DBUG_ASSERT(my_tx != nullptr);

  TransactionExecution *txm= my_tx->Txm();
  DBUG_ASSERT(txm != nullptr);

  // Check whether to-be-inserted primary key already exists. If upsert
  // semantic is used or the primary key is UUID or autoinc column, skip the
  // check. Note that autoinc column can be set at secondary key, and autoinc
  // column can specify an explicit value (insert_id_for_cur_row == 0). In
  // the above two cases, PkRead is still needed.
  RecordStatus rec_stat;

  if (!is_insert_semantic_ || table_share->primary_key == MAX_INDEXES ||
      (table_share->primary_key == table_share->next_number_index &&
       insert_id_for_cur_row != 0))
  {
    rec_stat= RecordStatus::Deleted;
  }
  else
  {
    TxKey pk_tx_key(eloq_key.get());
    rec_stat= PkRead(my_tx, pk_tx_key, last_read_record_, true, 0, true);
  }

  if (rec_stat == RecordStatus::Deleted)
  {
    const uchar *hidden_pk=
        has_hidden_pk(table) ? (const uchar *) eloq_key->PackedValue().data()
                             : nullptr;
    /*

    Special consideration is required when inserting operation fails on unique
    secondary index duplication check. Consider the following situation:

    CREATE TABLE t1 (a INT, b INT, c INT, UNIQUE(a), UNIQUE(b));
    INSERT t1 VALUES (3,4,20);
    INSERT t1 VALUES (5,6,30), (7,4,40) ON DUPLICATE KEY UPDATE c=c+100;

    When the duplication on column `b` is detected, the write_set_entry
    (7,uuid) of unique index `a` is already inserted into write_set, which is
    incorrect after the sql statement is converted to UPDATE semantic. So we
    need to erase these stale wset entries.

    Also, in the scenario where the stale entry needs to be reverted, the
    WriteIntent lock acquired during SkRead() will be released when
    postprocessing rset and the newly emplaced stale cc_entry can be ignored
    since its status is deleted.

    */

    // Get the dirty indexes name.
    auto &dirty_index_names= my_tx->DirtyIndexNames();

    // Process all the indexes data
    bool is_unique_sk= false;
    bool is_dirty= false;
    auto dirty_it= dirty_index_names.begin();
    for (uint kid= 0;
         kid < table_share->keys || dirty_it != dirty_index_names.end();)
    {
      if (kid == table_share->primary_key)
      {
        ++kid;
        continue;
      }

      is_dirty= kid == table_share->keys ? true : false;
      // TODO(ysw): check unique for dirty index
      is_unique_sk=
          !is_dirty ? table_share->key_info[kid].flags & HA_NOSAME : false;

      const txservice::TableName &index_name=
          !is_dirty ? table_schema_->IndexNameSchema(kid).first : *dirty_it;
      const EloqKeySchema *index_schema=
          reinterpret_cast<const EloqKeySchema *>(
              !is_dirty
                  ? table_schema_->IndexNameSchema(kid).second.sk_schema_.get()
                  : table_dirty_schema_->IndexKeySchema(index_name)
                        ->sk_schema_.get());

      if (is_unique_sk)
      {
        // Pack function needs hidden pk value because we pack hidden pk at
        // the end of secondary key cols to make sure the key is unique.
        size_t unique_sk_packed_size= 0;
        size_t size= index_schema->KeyDefinition()->pack_record(
            table, pack_buffer_, buf, sk_packed_tuple_, &unpack_info_, false,
            &unique_sk_packed_size, hidden_pk);

        EloqKey new_sk(sk_packed_tuple_,
                       is_unique_sk ? unique_sk_packed_size : size);
        EloqRecord new_sk_rec;

        RecordStatus rec_status;
        uint64_t useless_ts;
        std::tie(rec_status, useless_ts)=
            SkRead(my_tx, new_sk, new_sk_rec, true, kid);
        if (rec_status == RecordStatus::Deleted)
        {
          // Safe to procceed.
        }
        else if (rec_status == RecordStatus::Normal)
        {
          // Unique sk already exsits.

          for (uint i= 0; i < kid; ++i)
          {
            if (i == table_share->primary_key)
            {
              continue;
            }

            const txservice::TableName &index_name=
                table_schema_->IndexNameSchema(i).first;
            const EloqKeySchema *sk_schema=
                reinterpret_cast<const EloqKeySchema *>(
                    table_schema_->IndexNameSchema(i).second.sk_schema_.get());

            size_t unique_sk_packed_size= 0;
            size_t size= sk_schema->KeyDefinition()->pack_record(
                table, pack_buffer_, buf, sk_packed_tuple_, nullptr, false,
                &unique_sk_packed_size, hidden_pk);

            EloqKey sk(sk_packed_tuple_,
                       is_unique_sk ? unique_sk_packed_size : size);
            TxKey sk_tx_key(&sk);
            txm->TxRevert(index_name, sk_tx_key);
          }

          errkey= kid;
          dup_errkey_= errkey;
          return HA_ERR_FOUND_DUPP_KEY;
        }
        else
        {
          // It is kv store error.
          assert(rec_status == RecordStatus::Unknown);
          return convert_tx_error(my_tx->tx_err_code_);
        }
      }

      size_t size= 0;
      size_t unique_sk_packed_size= 0;

      // Pack function needs hidden pk value because we pack hidden pk at
      // the end of secondary key cols to make sure the key is unique.
      size= index_schema->KeyDefinition()->pack_record(
          table, pack_buffer_, buf, sk_packed_tuple_, &unpack_info_, false,
          &unique_sk_packed_size, hidden_pk);

      std::unique_ptr<EloqKey> new_sk= std::make_unique<EloqKey>(
          sk_packed_tuple_, is_unique_sk ? unique_sk_packed_size : size);
      std::unique_ptr<EloqRecord> new_sk_rec= std::make_unique<EloqRecord>();

      if (is_unique_sk)
      {
        new_sk_rec->SetEncodedBlob(sk_packed_tuple_ + unique_sk_packed_size,
                                   size - unique_sk_packed_size);
      }

      new_sk_rec->SetUnpackInfo(unpack_info_.ptr(),
                                unpack_info_.get_current_pos());

      uint64_t sk_key_version= index_schema->SchemaTs();

      TxErrorCode err=
          txm->TxUpsert(index_name, sk_key_version, TxKey(std::move(new_sk)),
                        std::move(new_sk_rec), OperationType::Insert);
      if (err != TxErrorCode::NO_ERROR)
      {
        my_tx->tx_err_code_= err;
        // my_tx->detailed_err_message_= ups_sk_req.ErrorMsg();
        // my_error(HA_ERR_ELOQ_INSERT_FAILED, MYF(0),
        //          my_tx->detailed_err_message_.data());
        return HA_ERR_OUT_OF_MEM;
      }

      // Move to next index
      if (is_dirty)
      {
        ++dirty_it;
      }
      else
      {
        ++kid;
      }
    } /* End of indexes */

    uint64_t pk_key_version= table_schema_->KeySchema()->SchemaTs();

    TxErrorCode err= txm->TxUpsert(*GetBaseTableNameFromTableSchema(),
                                   pk_key_version, TxKey(std::move(eloq_key)),
                                   std::move(eloq_rec), OperationType::Insert);

    if (err != TxErrorCode::NO_ERROR)
    {
      my_tx->tx_err_code_= err;
      // my_tx->detailed_err_message_= TxErrorMessage(err);
      // my_error(HA_ERR_ELOQ_INSERT_FAILED, MYF(0),
      //          my_tx->detailed_err_message_.data());
      return HA_ERR_OUT_OF_MEM;
    }

    return 0;
  }
  else if (rec_stat == RecordStatus::Normal)
  {
    // Pk already exsits.
    errkey= table_share->primary_key;
    dup_errkey_= errkey;
    return HA_ERR_FOUND_DUPP_KEY;
  }
  else
  {
    // Read fails, kv store error
    assert(rec_stat == RecordStatus::Unknown);
    return convert_tx_error(my_tx->tx_err_code_);
  }
}

int ha_eloq::BulkInsert(const uchar *buf, std::unique_ptr<EloqKey> eloq_key,
                        std::unique_ptr<EloqRecord> eloq_rec)
{
  DBUG_ENTER_FUNC();

  MyEloqTx *my_tx= get_myeloq_tx(ha_thd());
  DBUG_ASSERT(my_tx != nullptr);
  TransactionExecution *txm= my_tx->Txm();

  int res_code= 0;
  // The secondary key.
  const uchar *hidden_pk= has_hidden_pk(table)
                              ? (const uchar *) eloq_key->PackedValue().data()
                              : nullptr;
  size_t size= 0;
  size_t unique_sk_packed_size= 0;
  // The dirty index names.
  auto &dirty_index_names= my_tx->DirtyIndexNames();
  bool is_unique_sk= false;
  bool is_dirty= false;
  auto dirty_it= dirty_index_names.begin();
  for (uint kid= 0;
       kid < table_share->keys || dirty_it != dirty_index_names.end();)
  {
    if (kid == table_share->primary_key)
    {
      // The pk
      ++kid;
      continue;
    }

    is_dirty= kid == table_share->keys ? true : false;
    is_unique_sk=
        !is_dirty ? table_share->key_info[kid].flags & HA_NOSAME : false;
    const txservice::TableName &index_name=
        !is_dirty ? table_schema_->IndexNameSchema(kid).first : *dirty_it;

    auto *secondary_key_schema= table_schema_->IndexKeySchema(index_name);
    secondary_key_schema=
        secondary_key_schema != nullptr
            ? secondary_key_schema
            : table_dirty_schema_->IndexKeySchema(index_name);
    const EloqKeySchema *index_schema= reinterpret_cast<const EloqKeySchema *>(
        secondary_key_schema->sk_schema_.get());

    unique_sk_packed_size= 0;
    // Pack function needs hidden pk value because we pack hidden pk at
    // the end of secondary key cols to make sure the key is unique.
    size= index_schema->KeyDefinition()->pack_record(
        table, pack_buffer_, buf, sk_packed_tuple_, &unpack_info_, false,
        &unique_sk_packed_size, hidden_pk);

    std::unique_ptr<EloqKey> new_sk= std::make_unique<EloqKey>(
        sk_packed_tuple_, is_unique_sk ? unique_sk_packed_size : size);
    std::unique_ptr<EloqRecord> new_sk_rec= std::make_unique<EloqRecord>();
    if (is_unique_sk)
    {
      new_sk_rec->SetEncodedBlob(sk_packed_tuple_ + unique_sk_packed_size,
                                 size - unique_sk_packed_size);
    }
    new_sk_rec->SetUnpackInfo(unpack_info_.ptr(),
                              unpack_info_.get_current_pos());
    std::unordered_map<
        TableName, std::pair<uint64_t, BulkInsertBuffer>>::iterator index_it;
    if (is_unique_sk)
    {
      index_it= unique_sk_bulk_insert_buffer_.find(index_name);
      assert(index_it != unique_sk_bulk_insert_buffer_.end());
      index_it->second.second.emplace_back(new_sk.get(), new_sk_rec.get());
    }

    uint64_t sk_key_version= index_schema->SchemaTs();

    TxErrorCode err= txm->TxUpsert(
        index_name, sk_key_version, TxKey(std::move(new_sk)),
        std::move(new_sk_rec), OperationType::Insert, is_unique_sk);
    if (err != TxErrorCode::NO_ERROR)
    {
      if (err == TxErrorCode::DUPLICATE_KEY)
      {
        assert(is_unique_sk);
        index_it->second.second.clear();
        errkey= kid;
        dup_errkey_= errkey;
      }
      my_tx->tx_err_code_= err;
      res_code= (err == TxErrorCode::DUPLICATE_KEY) ? HA_ERR_FOUND_DUPP_KEY
                                                    : HA_ERR_OUT_OF_MEM;
      DBUG_RETURN(res_code);
    }

    // Next index
    if (is_dirty)
    {
      ++dirty_it;
    }
    else
    {
      ++kid;
    }
  }

  // The primary key
  bool check_primary_key=
      is_insert_semantic_ && table_share->primary_key != MAX_INDEXES &&
      !(table_share->primary_key == table_share->next_number_index &&
        table_share->key_info[table_share->primary_key]
                .key_part->field->unireg_check == mysql::Field::NEXT_NUMBER);
  if (check_primary_key)
  {
    pk_bulk_insert_buffer_.emplace_back(eloq_key.get(), eloq_rec.get());
  }

  uint64_t pk_key_version= table_schema_->KeySchema()->SchemaTs();
  TxErrorCode err=
      txm->TxUpsert(*GetBaseTableNameFromTableSchema(), pk_key_version,
                    TxKey(std::move(eloq_key)), std::move(eloq_rec),
                    OperationType::Insert, check_primary_key);
  if (err != TxErrorCode::NO_ERROR)
  {
    if (err == TxErrorCode::DUPLICATE_KEY)
    {
      pk_bulk_insert_buffer_.clear();
      errkey= table_share->primary_key;
      dup_errkey_= errkey;
    }
    my_tx->tx_err_code_= err;
    res_code= (err == TxErrorCode::DUPLICATE_KEY) ? HA_ERR_FOUND_DUPP_KEY
                                                  : HA_ERR_OUT_OF_MEM;
    DBUG_RETURN(res_code);
  }

  size_t bulk_insert_size= 0;
  if (check_primary_key)
  {
    bulk_insert_size= pk_bulk_insert_buffer_.size();
  }
  else
  {
    assert(unique_sk_bulk_insert_buffer_.size() > 0);
    bulk_insert_size=
        unique_sk_bulk_insert_buffer_.begin()->second.second.size();
  }

  if (bulk_insert_size >= batch_read_size_)
  {
    // Check the unique constraint.
    res_code= BulkUniqueCheck(bulk_insert_size);
  }

  DBUG_RETURN(res_code);
}

int ha_eloq::BulkUniqueCheck(size_t bulk_size)
{
  DBUG_ENTER_FUNC();
  std::vector<txservice::ScanBatchTuple> batch_tuples;
  batch_tuples.reserve(bulk_size);

  MyEloqTx *my_tx= get_myeloq_tx(ha_thd());
  DBUG_ASSERT(my_tx != nullptr);

  size_t dup_index= 0;
  auto UniqueConstraintChecking=
      [&batch_tuples, &bulk_size, my_tx, table_schema= table_schema_,
       table_share_ptr= table_share, &err_key= errkey,
       &dup_errkey= dup_errkey_,
       &dup_index](const txservice::TableName &table_name,
                   uint64_t schema_version) -> int {
    std::pair<const std::function<void()> *, const std::function<void()> *>
        coro_functors= my_tx->CoroFunctors();

    BatchReadTxRequest batch_read_req(
        &table_name, schema_version, batch_tuples, true, false, false,
        coro_functors.first, coro_functors.second, my_tx->Txm(), 0, true);

    my_tx->Txm()->Execute(&batch_read_req);
    batch_read_req.Wait();

    if (batch_read_req.IsError())
    {
      my_tx->tx_err_code_= batch_read_req.ErrorCode();
      return convert_tx_error(my_tx->tx_err_code_);
    }

    for (size_t idx= 0; idx < bulk_size; ++idx)
    {
      RecordStatus &rec_status= batch_tuples[idx].status_;
      if (rec_status == RecordStatus::Normal)
      {
        if (table_name.Type() == TableType::Primary)
        {
          // Pk already exsits.
          err_key= table_share_ptr->primary_key;
        }
        else
        {
          // key already exsits.
          err_key= table_schema->IndexId(table_name);
          // TODO(ysw): dirty key id.
        }
        dup_errkey= err_key;
        dup_index= idx;
        return HA_ERR_FOUND_DUPP_KEY;
      }
#ifndef RANGE_PARTITION_ENABLED
      else if (rec_status == RecordStatus::Unknown)
      {
        // For hash, should retrieve the key from the data store.
        if (storage_hd == nullptr)
        {
          rec_status= RecordStatus::Deleted;
          continue;
        }

        EloqRecord latest_rec;
        uint64_t latest_version_ts= 1U;
        bool store_found= false;
        bool success=
            storage_hd->Read(table_name, batch_tuples[idx].key_, latest_rec,
                             store_found, latest_version_ts, table_schema);
        if (!success)
        {
          rec_status= RecordStatus::Deleted;
          continue;
        }

        if (store_found)
        {
          if (table_name.Type() == TableType::Primary)
          {
            // Pk already exsits.
            err_key= table_share_ptr->primary_key;
          }
          else
          {
            // key already exsits.
            err_key= table_schema->IndexId(table_name);
          }
          dup_errkey= err_key;
          dup_index= idx;
          return HA_ERR_FOUND_DUPP_KEY;
        }
        rec_status= RecordStatus::Deleted;
      }
#endif
      else
      {
        DBUG_ASSERT(rec_status == RecordStatus::Deleted);
      }
    }
    return 0;
  };

  if (is_insert_semantic_ && table_share->primary_key != MAX_INDEXES &&
      !(table_share->primary_key == table_share->next_number_index &&
        table_share->key_info[table_share->primary_key]
                .key_part->field->unireg_check == mysql::Field::NEXT_NUMBER))
  {
    // Check the primary keys.
    for (size_t idx= 0; idx < bulk_size; ++idx)
    {
      batch_tuples.emplace_back(
          TxKey(std::get<0>(pk_bulk_insert_buffer_[idx])),
          const_cast<EloqRecord *>(std::get<1>(pk_bulk_insert_buffer_[idx])));
    }
    uint64_t pk_key_version= table_schema_->KeySchema()->SchemaTs();
    int res= UniqueConstraintChecking(*GetBaseTableNameFromTableSchema(),
                                      pk_key_version);
    if (res != 0)
    {
      if (res == HA_ERR_FOUND_DUPP_KEY)
      {
        const EloqKey *eloq_key=
            batch_tuples[dup_index].key_.GetKey<EloqKey>();
        auto *eloq_rec=
            static_cast<const EloqRecord *>(batch_tuples[dup_index].record_);
        DecodeRecord(table->record[0], eloq_key, eloq_rec);
      }
      pk_bulk_insert_buffer_.clear();
      DBUG_RETURN(res);
    }
    pk_bulk_insert_buffer_.clear();
  }

  for (auto index_it= unique_sk_bulk_insert_buffer_.begin();
       index_it != unique_sk_bulk_insert_buffer_.end(); ++index_it)
  {
    // unique secondary key
    for (size_t idx= 0; idx < bulk_size; ++idx)
    {
      if (batch_tuples.size() == bulk_size)
      {
        batch_tuples.at(idx).key_=
            TxKey(std::get<0>(index_it->second.second[idx]));
        batch_tuples.at(idx).record_= const_cast<EloqRecord *>(
            std::get<1>(index_it->second.second[idx]));
        batch_tuples.at(idx).status_= RecordStatus::Unknown;
        batch_tuples.at(idx).version_ts_= 0;
      }
      else
      {
        batch_tuples.emplace_back(
            TxKey(std::get<0>(index_it->second.second[idx])),
            const_cast<EloqRecord *>(
                std::get<1>(index_it->second.second[idx])));
      }
    }
    int res= UniqueConstraintChecking(index_it->first, index_it->second.first);
    index_it->second.second.clear();
    if (res != 0)
    {
      DBUG_RETURN(res);
    }
  } /* End of secondary key */

  DBUG_RETURN(0);
}

uint64_t ha_eloq::GetTableSchemaTs()
{
  assert(table_schema_ != nullptr);
  return table_schema_->Version();
}

const EloqKeySchema *ha_eloq::GetKeySchema()
{
  assert(table_schema_ != nullptr);

  if (hidden_key_schema_ != nullptr)
  {
    return hidden_key_schema_.get();
  }
  else
  {
    return static_cast<const EloqKeySchema *>(table_schema_->KeySchema());
  }
}

const EloqRecordSchema *ha_eloq::GetRecordSchema()
{
  DBUG_ENTER_FUNC();
  DBUG_ASSERT(table_schema_ != nullptr);

  const EloqRecordSchema *mysql_rec_schema=
      static_cast<const EloqRecordSchema *>(table_schema_->RecordSchema());

  DBUG_RETURN(mysql_rec_schema);
}

const txservice::KVCatalogInfo *ha_eloq::GetKVCatalogInfo()
{
  DBUG_ENTER_FUNC();
  DBUG_ASSERT(table_schema_ != nullptr);
  DBUG_RETURN(table_schema_->GetKVCatalogInfo());
}

const MysqlTableSchema *ha_eloq::DiscoverTableSchema(MyEloqTx *my_tx)
{
  const MysqlTableSchema *session_schema= nullptr,
                         *session_dirty_schema= nullptr;

  const MysqlTableSchema *tx_cached_schema=
      my_tx->GetCachedSchema(base_table_name_);
  if (tx_cached_schema != nullptr)
  {
    session_schema= tx_cached_schema;
  }
  else if (schema_cntl_ != nullptr)
  {
    bool success= schema_cntl_->AddReader();
    if (success)
    {
      my_tx->UpdateSchemaCntl(schema_cntl_);
      session_schema=
          static_cast<const MysqlTableSchema *>(schema_cntl_->GetObjectPtr());
    }
    else
    {
      schema_cntl_= nullptr;
    }
  }

  if (session_schema == nullptr)
  {
    std::tie(session_schema, session_dirty_schema)=
        my_tx->GetTableSchema(base_table_name_);
    if (session_schema == nullptr)
    {
      CatalogKey table_key(base_table_name_);
      TxKey tbl_tx_key(&table_key);
      CatalogRecord catalog_rec;

      std::pair<const std::function<void()> *, const std::function<void()> *>
          coro_functors= my_tx->CoroFunctors();
      TransactionExecution *txm= my_tx->Txm();

      // No need to check the version of __catalog table.
      ReadTxRequest read_tx_req(&txservice::catalog_ccm_name, 0, &tbl_tx_key,
                                &catalog_rec, false, false, true, 0, false,
                                false, false, coro_functors.first,
                                coro_functors.second, txm);
      txm->Execute(&read_tx_req);
      read_tx_req.Wait();
      const RecordStatus &rec_status= read_tx_req.Result().first;

      if (rec_status == RecordStatus::Normal)
      {
        if (catalog_rec.Schema() != nullptr)
        {
          session_schema=
              static_cast<const MysqlTableSchema *>(catalog_rec.Schema());
          session_dirty_schema=
              static_cast<const MysqlTableSchema *>(catalog_rec.DirtySchema());
          my_tx->UpdateTableSchema(
              std::static_pointer_cast<const MysqlTableSchema>(
                  catalog_rec.CopySchema()),
              std::static_pointer_cast<const MysqlTableSchema>(
                  catalog_rec.CopyDirtySchema()));

          schema_cntl_= catalog_rec.GetSchemaCntl();
        }
      }
    }
  }

  if (session_schema != nullptr)
  {
    if (table_schema_ != session_schema)
    {
      table_schema_= session_schema;
      const TABLE_SHARE *table_share= table_schema_->TableShare();
      uint64_t key_version= table_schema_->KeySchema()->SchemaTs();
      hidden_key_schema_=
          !has_hidden_pk(table)
              ? nullptr
              : std::make_unique<EloqHiddenKeySchema>(
                    table_share, table_share->stored_fields, key_version);
    }
    assert(table_schema_->StatisticsObject());

    if (table_dirty_schema_ != session_dirty_schema)
    {
      table_dirty_schema_= session_dirty_schema;
    }
  }
  else
  {
    table_schema_= nullptr;
    hidden_key_schema_= nullptr;
    table_dirty_schema_= nullptr;
    schema_cntl_= nullptr;
  }

  return session_schema;
}

bool ha_eloq::IndexScanIsOpen()
{
  DBUG_ENTER_FUNC();
  DBUG_RETURN(scan_alias_ < UINT64_MAX);
}

bool ha_eloq::IsValidScanKey(const EloqKey *key)
{
  return key != nullptr && key != EloqKey::PositiveInfinity() &&
         key != EloqKey::NegativeInfinity();
}

int ha_eloq::PkIndexScanOpen(const txservice::TxKey *start_key,
                             bool start_inclusive,
                             const txservice::TxKey *end_key,
                             bool end_inclusive, ScanDirection direction,
                             int used_key_parts)
{
  DBUG_ENTER_FUNC();

  assert(active_index == MAX_INDEXES || active_index == table->s->primary_key);

  if (IndexScanIsOpen())
  {
    IndexScanClose();
  }

  MyEloqTx *my_tx= get_myeloq_tx(ha_thd());
  DBUG_ASSERT(my_tx != nullptr);
  TransactionExecution *txm= my_tx->Txm();
  DBUG_ASSERT(txm != nullptr);

  scan_direction_= direction;
  const TableName *scan_table_name= GetBaseTableNameFromTableSchema();
  bool for_update= (lock.type >= TL_WRITE_ALLOW_WRITE);
  bool for_share= (lock.type == TL_READ_WITH_SHARED_LOCKS);

  // hidden-pk, which might be used as position, is not contained in the
  // decode_flag_.
  bool require_keys= has_hidden_pk(table) || decode_flag_ > 0;
  // EloqRecords contains payload and unpack_info.
  bool require_recs= decode_flag_ > 0;
  bool require_sort= eloq_random_scan_sort ||
                     (active_index != MAX_INDEXES && decode_flag_ > 0);

  auto [yield_func, resume_func]= my_tx->CoroFunctors();

  uint64_t key_version= table_schema_->KeySchema()->SchemaTs();

#ifdef RANGE_PARTITION_ENABLED
  scan_open_tx_req_.Reset(scan_table_name, key_version, ScanIndexType::Primary,
                          start_key, start_inclusive, end_key, end_inclusive,
                          direction, false, for_update, for_share, false,
                          require_keys, require_recs, require_sort, false,
                          yield_func, resume_func, txm);
  scan_alias_= txm->OpenTxScan(scan_open_tx_req_);
  assert(scan_alias_ != UINT64_MAX);
  scan_batch_cnt_= 0;
#else
  ScanOpenTxRequest scan_open(
      scan_table_name, key_version, ScanIndexType::Primary, start_key,
      start_inclusive, end_key, end_inclusive, direction, false, for_update,
      for_share, false, false, yield_func, resume_func, txm);
  txm->Execute(&scan_open);
  scan_open.Wait();

  if (scan_open.IsError())
  {
    DBUG_RETURN(convert_tx_error(scan_open.ErrorCode()));
  }

  scan_alias_= scan_open.Result();
#endif

  scan_index_= active_index;
  ccm_scan_open_= true;
  ccm_scan_key_= nullptr;
  ccm_scan_rec_= nullptr;

  scan_batch_.clear();
  scan_batch_idx_= UINT64_MAX;
  is_last_scan_batch_= false;

  sk_pk_scan_batch_.clear();
  sk_pk_scan_batch_idx_= UINT64_MAX;

#ifdef RANGE_PARTITION_ENABLED
  storage_scanner_= nullptr;
  advance_storage_scanner_= false;
#else
  if (storage_hd != nullptr)
  {
    // skip access kv store for scan request, used to speedup testcase only.
    // server startup stage need to read catalog tables which still requires
    // access kv store.
    if (eloq_scan_skip_kv && mysqld_server_started == 1)
    {
      storage_scanner_= nullptr;
      DBUG_RETURN(0);
    }

    auto pushed_cond= BindPushedCond();
    storage_scanner_= storage_hd->ScanForward(
        *scan_table_name, UINT32_MAX, *start_key, start_inclusive,
        used_key_parts, pushed_cond, GetKeySchema(), GetRecordSchema(),
        GetKVCatalogInfo(), scan_direction_ == ScanDirection::Forward);

    advance_storage_scanner_= false;

    REPORT_DEBUG_INFO("scan_table_name: %s, "
                      "scan_start_key: %s, "
                      "inclusive: %d, "
                      "used_key_parts: %d, "
                      "pushed_cond: %s, "
                      "scan_direction: %d",
                      scan_table_name->String().c_str(),
                      start_key->GetKey<EloqKey>()->ToString().c_str(),
                      start_inclusive, used_key_parts,
                      PushedConditionString(pushed_cond).c_str(),
                      scan_direction_);
  }
  else
  {
    storage_scanner_= nullptr;
  }
#endif

  DBUG_RETURN(0);
}

int ha_eloq::SkIndexScanOpen(const txservice::TxKey *start_index_key,
                             bool start_inclusive,
                             const txservice::TxKey *end_index_key,
                             bool end_inclusive, ScanDirection direction,
                             int used_key_parts)
{
  DBUG_ENTER_FUNC();

  assert(active_index != MAX_INDEXES && active_index != table->s->primary_key);

  if (IndexScanIsOpen())
  {
    IndexScanClose();
  }

  MyEloqTx *my_tx= get_myeloq_tx(ha_thd());
  DBUG_ASSERT(my_tx != nullptr);
  TransactionExecution *txm= my_tx->Txm();
  DBUG_ASSERT(txm != nullptr);

  scan_direction_= direction;
  const TableName &index_table_name=
      table_schema_->IndexNameSchema(active_index).first;
  bool is_covering_keys= table->covering_keys.is_set(active_index);
  bool is_require_keys= has_hidden_pk(table) || decode_flag_ > 0;
  bool is_require_recs= is_require_keys;
  bool is_require_sort= is_require_keys;

  auto [yield_func, resume_func]= my_tx->CoroFunctors();

  uint64_t key_version=
      table_schema_->IndexNameSchema(active_index).second.SchemaTs();

#ifdef RANGE_PARTITION_ENABLED
  scan_open_tx_req_.Reset(
      &index_table_name, key_version, ScanIndexType::Secondary,
      start_index_key, start_inclusive, end_index_key, end_inclusive,
      direction, false, false, false, is_covering_keys, is_require_keys,
      is_require_recs, is_require_sort, false, yield_func, resume_func, txm);
  scan_alias_= txm->OpenTxScan(scan_open_tx_req_);
  assert(scan_alias_ != UINT64_MAX);
  scan_batch_cnt_= 0;
#else
  ScanOpenTxRequest scan_open(&index_table_name, key_version,
                              ScanIndexType::Secondary, start_index_key,
                              start_inclusive, end_index_key, end_inclusive,
                              direction, false, false, false, is_covering_keys,
                              false, yield_func, resume_func, my_tx->Txm());

  txm->Execute(&scan_open);
  scan_open.Wait();

  if (scan_open.IsError())
  {
    DBUG_RETURN(convert_tx_error(scan_open.ErrorCode()));
  }

  scan_alias_= scan_open.Result();
  assert(scan_alias_ != UINT64_MAX);
#endif

  scan_index_= active_index;
  ccm_scan_open_= true;
  ccm_scan_key_= nullptr;
  ccm_scan_rec_= nullptr;

  scan_batch_.clear();
  scan_batch_idx_= UINT64_MAX;
  is_last_scan_batch_= false;

#ifdef RANGE_PARTITION_ENABLED
  storage_scanner_= nullptr;
  advance_storage_scanner_= false;
#else
  if (storage_hd != nullptr)
  {
    // skip access kv store for scan request, used to speedup testcase only.
    // server startup stage need to read catalog tables which still requires
    // access kv store.
    if (eloq_scan_skip_kv && mysqld_server_started == 1)
    {
      storage_scanner_= nullptr;
      DBUG_RETURN(0);
    }

    const EloqKeySchema *sk_sch= reinterpret_cast<const EloqKeySchema *>(
        table_schema_->IndexNameSchema(active_index).second.sk_schema_.get());
    auto pushed_cond= BindPushedCond();
    storage_scanner_= storage_hd->ScanForward(
        index_table_name, UINT32_MAX, *start_index_key, start_inclusive,
        used_key_parts, pushed_cond, sk_sch, GetRecordSchema(),
        GetKVCatalogInfo(), scan_direction_ == ScanDirection::Forward);

    advance_storage_scanner_= false;

    REPORT_DEBUG_INFO("scan_table_name: %s, "
                      "scan_start_key: %s, "
                      "inclusive: %d, "
                      "used_key_parts: %d, "
                      "pushed_cond: %s, "
                      "scan_direction: %d",
                      index_table_name.String().c_str(),
                      start_index_key->GetKey<EloqKey>()->ToString().c_str(),
                      start_inclusive, used_key_parts,
                      PushedConditionString(pushed_cond).c_str(),
                      scan_direction_);
  }
  else
  {
    storage_scanner_= nullptr;
  }
#endif

  DBUG_RETURN(0);
}

int ha_eloq::PkIndexScanNext(uchar *table_record)
{
  DBUG_ENTER_FUNC();

  THD *thd= ha_thd();

  const EloqKey *result_key= nullptr;
  const EloqRecord *result_rec= nullptr;
  auto result_rec_status= txservice::RecordStatus::Deleted;
  std::unique_ptr<EloqRecord> version_miss_rec= nullptr;

  txservice::TxKey store_tx_key;
  const EloqKey *store_key= nullptr;
  const txservice::TxRecord *store_rec= nullptr;
  uint64_t store_rec_version= UINT64_MAX;
  bool is_store_key_deleted= false;
  if (storage_scanner_ != nullptr)
  {
    if (advance_storage_scanner_)
    {
      storage_scanner_->MoveNext();
      advance_storage_scanner_= false;
    }
    storage_scanner_->Current(store_tx_key, store_rec, store_rec_version,
                              is_store_key_deleted);
    store_key= store_tx_key.GetKey<EloqKey>();
  }

  MyEloqTx *my_tx= get_myeloq_tx(ha_thd());
  DBUG_ASSERT(my_tx != nullptr);
  TransactionExecution *txm= my_tx->Txm();
  DBUG_ASSERT(txm != nullptr);

  uint64_t tx_start_ts= txm->GetStartTs();
  bool for_update= lock.type >= TL_WRITE_ALLOW_WRITE;
  bool for_share= lock.type == TL_READ_WITH_SHARED_LOCKS;
  bool is_snapshot_read=
      (!for_update && !for_share &&
       (txm->GetIsolationLevel() == txservice::IsolationLevel::Snapshot));

  do
  {
    if (thd && thd->killed)
    {
      DBUG_RETURN(HA_ERR_QUERY_INTERRUPTED);
    }

    result_key= nullptr;
    result_rec= nullptr;
    result_rec_status= txservice::RecordStatus::Deleted;
    if (storage_scanner_ != nullptr && advance_storage_scanner_)
    {
      storage_scanner_->MoveNext();
      storage_scanner_->Current(store_tx_key, store_rec, store_rec_version,
                                is_store_key_deleted);
      store_key= store_tx_key.GetKey<EloqKey>();
      advance_storage_scanner_= false;
    }

    if (ccm_scan_open_)
    {
      if (ccm_scan_key_ == nullptr)
      {
        if (scan_batch_idx_ < scan_batch_.size())
        {
          txservice::ScanBatchTuple &scan_tuple= scan_batch_[scan_batch_idx_];
          ccm_scan_key_= scan_tuple.key_.GetKey<EloqKey>();
          ccm_scan_rec_= static_cast<const EloqRecord *>(scan_tuple.record_);
          ccm_scan_rec_status_= scan_tuple.status_;

          ++scan_batch_idx_;
        }
        else if (scan_batch_idx_ == UINT64_MAX ||
                 (!scan_batch_.empty() && !is_last_scan_batch_))
        {
          // Fetches the next batch.
          scan_batch_idx_= 0;
          scan_batch_.clear();

          const TableName *table_name= GetBaseTableNameFromTableSchema();
          auto [yield_func, resume_func]= my_tx->CoroFunctors();
          ScanBatchTxRequest scan_batch_req(scan_alias_, *table_name,
                                            &scan_batch_, yield_func,
                                            resume_func, txm);
#ifdef RANGE_PARTITION_ENABLED
          scan_batch_req.prefetch_slice_cnt_= PrefetchSize();
          ++scan_batch_cnt_;
#endif
          txm->Execute(&scan_batch_req);
          scan_batch_req.Wait();

          if (scan_batch_req.IsError())
          {
            DBUG_RETURN(convert_tx_error(scan_batch_req.ErrorCode()));
          }

          is_last_scan_batch_= scan_batch_req.Result();
          if (!scan_batch_.empty())
          {
            txservice::ScanBatchTuple &scan_tuple=
                scan_batch_[scan_batch_idx_];
            ccm_scan_key_= scan_tuple.key_.GetKey<EloqKey>();
            ccm_scan_rec_= static_cast<const EloqRecord *>(scan_tuple.record_);
            ccm_scan_rec_status_= scan_tuple.status_;
            ++scan_batch_idx_;
          }
        }
      }

      if (ccm_scan_key_ == nullptr)
      {
        // The cc map scanner has reached the end. The current tuple of the
        // data store scanner, if there any, is the result candidate.
        ccm_scan_open_= false;

        if (store_key == nullptr)
        {
          DBUG_RETURN(HA_ERR_END_OF_FILE);
        }
        else
        {
          result_key= store_key;
          result_rec= static_cast<const EloqRecord *>(store_rec);
          advance_storage_scanner_= true;
          if (is_snapshot_read && store_rec_version > tx_start_ts)
          {
            result_rec_status= txservice::RecordStatus::ArchiveVersionMiss;
          }
          else if (is_store_key_deleted)
          {
            continue;
          }
          else
          {
            result_rec_status= txservice::RecordStatus::Normal;
          }
        }
      }
      else if (store_key == nullptr)
      {
        // The data store scanner has reached the end. Advances the cc map
        // scanner by setting the cached key and record pointers to null.

        if (ccm_scan_rec_status_ == txservice::RecordStatus::Deleted)
        {
          ccm_scan_key_= nullptr;
          ccm_scan_rec_= nullptr;
          continue;
        }

        result_key= ccm_scan_key_;
        result_rec= ccm_scan_rec_;
        result_rec_status= ccm_scan_rec_status_;
        ccm_scan_key_= nullptr;
        ccm_scan_rec_= nullptr;
      }
      else
      {
        // Neither the cc map scanner nor the data store scanner reaches the
        // end. Compares their current tuples.
        if ((scan_direction_ == ScanDirection::Forward &&
             *store_key < *ccm_scan_key_) ||
            (scan_direction_ == ScanDirection::Backward &&
             *ccm_scan_key_ < *store_key))
        {
          // The tupled pointed by the data store scanner is the "next"
          // result candidate.
          result_key= store_key;
          result_rec= static_cast<const EloqRecord *>(store_rec);
          advance_storage_scanner_= true;
          if (is_snapshot_read && store_rec_version > tx_start_ts)
          {
            result_rec_status= txservice::RecordStatus::ArchiveVersionMiss;
          }
          else if (is_store_key_deleted)
          {
            continue;
          }
          else
          {
            result_rec_status= txservice::RecordStatus::Normal;
          }
        }
        else
        {
          // The tuple pointed by the cc map scanner is the "next" result
          // candidate.

          // Advances the data store scanner, if its current key equals to
          // the current key of the cc map scanner. That is: the tuple
          // returned by the cc map always overrides the one returned by the
          // data store.
          if (*store_key == *ccm_scan_key_)
          {
            storage_scanner_->MoveNext();
            storage_scanner_->Current(store_tx_key, store_rec,
                                      store_rec_version, is_store_key_deleted);
            store_key= store_tx_key.GetKey<EloqKey>();
            advance_storage_scanner_= false;
          }

          if (ccm_scan_rec_status_ == txservice::RecordStatus::Deleted)
          {
            ccm_scan_key_= nullptr;
            ccm_scan_rec_= nullptr;
            continue;
          }

          result_key= ccm_scan_key_;
          result_rec= ccm_scan_rec_;
          result_rec_status= ccm_scan_rec_status_;
          ccm_scan_key_= nullptr;
          ccm_scan_rec_= nullptr;
        }
      }
    }
    else if (store_key != nullptr)
    {
      result_key= store_key;
      result_rec= static_cast<const EloqRecord *>(store_rec);
      advance_storage_scanner_= true;
      if (is_snapshot_read && store_rec_version > tx_start_ts)
      {
        result_rec_status= txservice::RecordStatus::ArchiveVersionMiss;
      }
      else if (is_store_key_deleted)
      {
        continue;
      }
      else
      {
        result_rec_status= txservice::RecordStatus::Normal;
      }
    }

    if (result_key != nullptr)
    {
      if (prefix_match_key_ != nullptr &&
          !(prefix_match_key_->IsPrefixOf(*result_key)))
      {
        // The current tuple is out of the scan range. Decrements
        // scan_batch_idx_.
        scan_batch_idx_--;
        //  Matches the prefix.
        DBUG_RETURN(HA_ERR_END_OF_FILE);
      }
      if (result_rec_status == txservice::RecordStatus::Deleted)
      {
        continue;
      }
      else if (result_rec_status == txservice::RecordStatus::Normal)
      {
        DecodeRecord(table_record, result_key, result_rec);
      }
      else
      {
        if (version_miss_rec == nullptr)
        {
          version_miss_rec= std::make_unique<EloqRecord>();
        }
        bool res= ReadSnapshotFromDataStore(
            *GetBaseTableNameFromTableSchema(), GetKVCatalogInfo(),
            tx_start_ts, result_rec_status != RecordStatus::ArchiveVersionMiss,
            *result_key, *version_miss_rec, result_rec_status);
        if (!res || result_rec_status == RecordStatus::Deleted)
        {
          continue;
        }
        else
        {
          DecodeRecord(table_record, result_key, version_miss_rec.get());
        }
      }

      // DBUG_ASSERT(has_hidden_pk(table) || decode_flag_ > 0);
      last_read_key_= *result_key;

      DBUG_RETURN(0);
    }
  } while (ccm_scan_open_ || store_key != nullptr);

  DBUG_RETURN(HA_ERR_END_OF_FILE);
}
/**
 * TO DO
 * Now SkIndexScanNext only support Range Partation. If select Hash Partation,
 * it can read records from storage directly. In this case, it can not get
 * order position from batch_order_.
 */
int ha_eloq::SkIndexScanNext(uchar *table_record)
{
  DBUG_ENTER_FUNC();
  assert(active_index != table->s->primary_key);
  bool is_pk_scan_needed=
      !(table->covering_keys.is_set(active_index) || is_mrr_sort_rowid_);

  // Only used when is_pk_scan_needed=true. Used to save the PKs loaded from SK
  // tuple and sort the Pks, save the relations between SK and PK.
  struct KeyOrder
  {
    KeyOrder(EloqKey &&key, uint64_t pos, uint64_t ts)
        : key_(std::move(key)), pos_(pos), ts_(ts)
    {
    }
    EloqKey key_;  // The PK read from SK tuple
    uint64_t pos_; // The position in SK tuples
    uint64_t ts_;  // version_ts_ in SK tuple
  };
  std::vector<KeyOrder> vct_key_order;
  // Only used when is_pk_scan_needed=true. In this case, ScanBatchTxRequest
  // return batch of tuples by scan_batch_, do-while will read the tuples one
  // by one. If the tuple meet the filter conditions, it will save the tuples
  // into tmp_scan_batch
  std::vector<txservice::ScanBatchTuple> tmp_scan_batch;

  /**
   * For sk-pk batch load (is_pk_scan_needed=true), it will call
   * ScanBatchTxRequest to scan a batch of tuple into scan_batch_ first, then
   * read the tuples one by one in do-while and filter them. If passed, the
   * tuple will save into tmp_scan_batch temporarily. If the tuples are less
   * than DEFAULT_SCAN_TUPLE_SIZE, it will continue to call ScanBatchTxRequest
   * to get next batch and handle all the batch until the tuples' count is
   * large than the size or no more tuples. Then it move the tuples from
   * tmp_scan_batch to replace scan_batch_. Then it sort the tuples by PK and
   * batch load the PK records, and save the relations between SK and PK into
   * batch_order_. After all these, it will return the records one by one by SK
   * order. When all records have been returned, it will call
   * ScanBatchTxRequest to load next batch until no more tuples.
   *
   * If is_pk_scan_needed=false, it means here does not need to load PK record,
   * it will follow old steps to load a batch of tuples first and return the
   * records one by one.
   */
  if (is_pk_scan_needed)
  {
    /**
     * If is_pk_scan_needed=true, it will to check if there has reserved
     * records. If YES, it will return the records one by one, if NOT, it will
     * reset scan_batch_idx_ to UINT64_MAX to scan next batch of tuples.
     */
    while (scan_batch_idx_ < scan_batch_.size())
    {
      // Get the PK position by SK order
      uint64_t pos= batch_order_[scan_batch_idx_];
      assert(pos != UINT64_MAX);

      txservice::ScanBatchTuple &scan_tuple= sk_pk_scan_batch_[pos];
      if (scan_tuple.status_ != RecordStatus::Normal)
      {
        scan_batch_idx_++;
        continue;
      }

      assert(has_hidden_pk(table) || decode_flag_ > 0);

      const EloqKey *key= scan_tuple.key_.GetKey<EloqKey>();
      const EloqRecord *rec=
          static_cast<const EloqRecord *>(scan_tuple.record_);
      DecodeRecord(table_record, key, rec);
      last_read_key_= std::move(*key);
      scan_batch_idx_++;
      DBUG_RETURN(0);
    }

    if (is_last_scan_batch_)
    {
      DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
    }
    // First time to load batch records or all previous batch records have been
    // returned. Initialize variable for next batch.
    vct_key_order.reserve(DEFAULT_SCAN_TUPLE_SIZE);
    tmp_scan_batch.reserve(DEFAULT_SCAN_TUPLE_SIZE);
    scan_batch_idx_= UINT64_MAX;
  }

  THD *thd= ha_thd();
  const TableName scan_table_name{
      table_schema_->IndexNameSchema(active_index).first.StringView(),
      table_schema_->IndexNameSchema(active_index).first.Type(),
      table_schema_->IndexNameSchema(active_index).first.Engine()};

  const EloqKey *result_key= nullptr;
  const EloqRecord *result_rec= nullptr;
  auto result_rec_status= txservice::RecordStatus::Deleted;
  std::unique_ptr<EloqRecord> version_miss_rec= nullptr;

  txservice::TxKey store_tx_key;
  const EloqKey *store_key= nullptr;
  const txservice::TxRecord *store_rec= nullptr;
  uint64_t store_rec_version= 0;
  bool is_store_key_deleted= false;

  if (storage_scanner_ != nullptr)
  {
    if (advance_storage_scanner_)
    {
      storage_scanner_->MoveNext();
      advance_storage_scanner_= false;
    }
    storage_scanner_->Current(store_tx_key, store_rec, store_rec_version,
                              is_store_key_deleted);
    store_key= store_tx_key.GetKey<EloqKey>();
  }

  MyEloqTx *my_tx= get_myeloq_tx(ha_thd());
  DBUG_ASSERT(my_tx != nullptr);
  TransactionExecution *txm= my_tx->Txm();
  DBUG_ASSERT(txm != nullptr);

  uint64_t tx_start_ts= txm->GetStartTs();
  bool for_update= lock.type >= TL_WRITE_ALLOW_WRITE;
  bool for_share= lock.type == TL_READ_WITH_SHARED_LOCKS;
  bool is_snapshot_read=
      (!for_update && !for_share &&
       (txm->GetIsolationLevel() == txservice::IsolationLevel::Snapshot));
  uint64_t sk_ts= 0;

  do
  {
    if (thd && thd->killed)
    {
      DBUG_RETURN(HA_ERR_QUERY_INTERRUPTED);
    }

    result_key= nullptr;
    result_rec= nullptr;
    result_rec_status= txservice::RecordStatus::Deleted;
    if (storage_scanner_ != nullptr && advance_storage_scanner_)
    {
      storage_scanner_->MoveNext();
      storage_scanner_->Current(store_tx_key, store_rec, store_rec_version,
                                is_store_key_deleted);
      store_key= store_tx_key.GetKey<EloqKey>();
      advance_storage_scanner_= false;
    }

    if (ccm_scan_open_)
    {
      if (ccm_scan_key_ == nullptr)
      {
        if (scan_batch_idx_ < scan_batch_.size())
        {
          txservice::ScanBatchTuple &scan_tuple= scan_batch_[scan_batch_idx_];
          ccm_scan_key_= scan_tuple.key_.GetKey<EloqKey>();
          ccm_scan_rec_= static_cast<const EloqRecord *>(scan_tuple.record_);
          ccm_scan_rec_status_= scan_tuple.status_;
          sk_ts= scan_tuple.version_ts_;

          ++scan_batch_idx_;
        }
        // If is_pk_scan_needed=true, it need to collect more than 128 tuples
        // before break do-while, if less than 128, it will get next scan
        // batch until the scan end.
        // If is_pk_scan_needed=false, it will get next scan batch until the
        // scan end.
        else if (scan_batch_idx_ == UINT64_MAX ||
                 (is_pk_scan_needed &&
                  vct_key_order.size() < DEFAULT_SCAN_TUPLE_SIZE &&
                  !is_last_scan_batch_) ||
                 (!scan_batch_.empty() && !is_last_scan_batch_ &&
                  !is_pk_scan_needed))
        {
          // Fetches the next batch.
          scan_batch_idx_= 0;
          scan_batch_.clear();

          auto [yield_func, resume_func]= my_tx->CoroFunctors();
          ScanBatchTxRequest scan_batch_req(scan_alias_, scan_table_name,
                                            &scan_batch_, yield_func,
                                            resume_func, txm);
#ifdef RANGE_PARTITION_ENABLED
          scan_batch_req.prefetch_slice_cnt_= PrefetchSize();
          ++scan_batch_cnt_;
#endif
          txm->Execute(&scan_batch_req);
          scan_batch_req.Wait();

          if (scan_batch_req.IsError())
          {
            DBUG_RETURN(convert_tx_error(scan_batch_req.ErrorCode()));
          }

          is_last_scan_batch_= scan_batch_req.Result();
          if (!scan_batch_.empty())
          {
            txservice::ScanBatchTuple &scan_tuple=
                scan_batch_[scan_batch_idx_];
            ccm_scan_key_= scan_tuple.key_.GetKey<EloqKey>();
            ccm_scan_rec_= static_cast<const EloqRecord *>(scan_tuple.record_);
            ccm_scan_rec_status_= scan_tuple.status_;
            sk_ts= scan_tuple.version_ts_;
            ++scan_batch_idx_;
          }
        }
      }

      if (ccm_scan_key_ == nullptr)
      {
        if (is_last_scan_batch_)
        {
          // The cc map scanner has reached the end. The current tuple of the
          // data store scanner, if there any, is the result candidate.
          ccm_scan_open_= false;
        }

        if (store_key == nullptr)
        {
          if (!is_pk_scan_needed)
          {
            DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
          }
          else
          {
            break;
          }
        }
        else
        {
          result_key= store_key;
          result_rec= static_cast<const EloqRecord *>(store_rec);
          advance_storage_scanner_= true;
          if (is_snapshot_read && store_rec_version > tx_start_ts)
          {
            result_rec_status= txservice::RecordStatus::ArchiveVersionMiss;
          }
          else if (is_store_key_deleted)
          {
            continue;
          }
          else
          {
            result_rec_status= txservice::RecordStatus::Normal;
          }
        }
      }
      else if (store_key == nullptr)
      {
        // The data store scanner has reached the end. Advances the cc map
        // scanner by setting the cached key and record pointers to null.

        if (ccm_scan_rec_status_ == txservice::RecordStatus::Deleted)
        {
          ccm_scan_key_= nullptr;
          ccm_scan_rec_= nullptr;
          continue;
        }

        result_key= ccm_scan_key_;
        result_rec= ccm_scan_rec_;
        result_rec_status= ccm_scan_rec_status_;
        ccm_scan_key_= nullptr;
        ccm_scan_rec_= nullptr;
      }
      else
      {
        // Neither the cc map scanner nor the data store scanner reaches
        // the end. Compares their current tuples.
        if ((scan_direction_ == ScanDirection::Forward &&
             *store_key < *ccm_scan_key_) ||
            (scan_direction_ == ScanDirection::Backward &&
             *ccm_scan_key_ < *store_key))
        {
          // The tupled pointed by the data store scanner is the "next"
          // result candidate.
          result_key= store_key;
          result_rec= static_cast<const EloqRecord *>(store_rec);
          advance_storage_scanner_= true;
          if (is_snapshot_read && store_rec_version > tx_start_ts)
          {
            result_rec_status= txservice::RecordStatus::ArchiveVersionMiss;
          }
          else if (is_store_key_deleted)
          {
            continue;
          }
          else
          {
            result_rec_status= txservice::RecordStatus::Normal;
          }
        }
        else
        {
          // The tuple pointed by the cc map scanner is the "next" result
          // candidate.

          // Advances the data store scanner, if its current key equals to
          // the current key of the cc map scanner. That is: the tuple
          // returned by the cc map always overrides the one returned by
          // the data store.
          if (*store_key == *ccm_scan_key_)
          {
            storage_scanner_->MoveNext();
            storage_scanner_->Current(store_tx_key, store_rec,
                                      store_rec_version, is_store_key_deleted);
            store_key= store_tx_key.GetKey<EloqKey>();
            advance_storage_scanner_= false;
          }

          if (ccm_scan_rec_status_ == txservice::RecordStatus::Deleted)
          {
            ccm_scan_key_= nullptr;
            ccm_scan_rec_= nullptr;
            continue;
          }

          result_key= ccm_scan_key_;
          result_rec= ccm_scan_rec_;
          result_rec_status= ccm_scan_rec_status_;
          ccm_scan_key_= nullptr;
          ccm_scan_rec_= nullptr;
        }
      }
    }
    else if (store_key != nullptr)
    {
      result_key= store_key;
      result_rec= static_cast<const EloqRecord *>(store_rec);
      advance_storage_scanner_= true;
      if (is_snapshot_read && store_rec_version > tx_start_ts)
      {
        result_rec_status= txservice::RecordStatus::ArchiveVersionMiss;
      }
      else if (is_store_key_deleted)
      {
        continue;
      }
      else
      {
        result_rec_status= txservice::RecordStatus::Normal;
      }
    }

    if (result_key != nullptr)
    {
      // If there is a prefix match, matches the prefix of the secondary
      // key.
      if (prefix_match_key_ != nullptr &&
          !(prefix_match_key_->IsPrefixOf(*result_key)))
      {
        // The current tuple is out of the scan range. Decrements
        // scan_batch_idx_.
        if (!is_pk_scan_needed)
        {
          --scan_batch_idx_;
          DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
        }
        else
        {
          scan_batch_idx_= scan_batch_.size();
          is_last_scan_batch_= true;
          break;
        }
      }

      if (result_rec_status == txservice::RecordStatus::Deleted)
      {
        continue;
      }
      else if (result_rec_status != txservice::RecordStatus::Normal)
      {
        if (version_miss_rec == nullptr)
        {
          version_miss_rec= std::make_unique<EloqRecord>();
        }
        bool res= ReadSnapshotFromDataStore(
            scan_table_name, GetKVCatalogInfo(), tx_start_ts,
            result_rec_status != RecordStatus::ArchiveVersionMiss, *result_key,
            *version_miss_rec, result_rec_status);
        if (!res || result_rec_status == RecordStatus::Deleted)
        {
          continue;
        }
        result_rec= version_miss_rec.get();
      }

      bool is_require_keys= has_hidden_pk(table) || decode_flag_ > 0;
      if (!is_require_keys)
      {
        DBUG_RETURN(0);
      }

      std::string sk_packed_tuple;
      Slice packed_key_slice;
      Slice unpack_info_slice;

      const EloqKeySchema *sk_schema= static_cast<const EloqKeySchema *>(
          table_schema_->IndexNameSchema(active_index)
              .second.sk_schema_.get());

      // Fill in table_record.
      if (scan_table_name.Type() == TableType::UniqueSecondary)
      {
        sk_packed_tuple.append(result_key->PackedValueSlice().data_,
                               result_key->PackedValueSlice().size_);
        sk_packed_tuple.append(result_rec->encoded_blob_.begin(),
                               result_rec->encoded_blob_.end());
        packed_key_slice=
            Slice(sk_packed_tuple.data(), sk_packed_tuple.size());
        unpack_info_slice= Slice(result_rec->unpack_info_.data(),
                                 result_rec->unpack_info_.size());

        sk_schema->KeyDefinition()->unpack_record(
            table, table_record, packed_key_slice, unpack_info_slice, false);

        // UniqueSecondary does not have condition pushed down from sql layer.
      }
      else
      {
        assert(scan_table_name.Type() == TableType::Secondary);
        packed_key_slice= result_key->PackedValueSlice();

        // Matches the conditions on the indexed columns, a.k.a. Index
        // Condition Pushdown (ICP) in MySQL, if there are any.
        int icp_rc=
            MatchIcp(table_record, *result_key, *result_rec, sk_schema);
        if (icp_rc == HA_ERR_END_OF_FILE)
        {
          if (!is_pk_scan_needed)
          {
            DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
          }
          else
          {
            scan_batch_idx_= scan_batch_.size();
            is_last_scan_batch_= true;
            break;
          }
        }
        else if (icp_rc == HA_ERR_ABORTED_BY_USER)
        {
          DBUG_RETURN(HA_ERR_ABORTED_BY_USER);
        }
        else if (icp_rc == HA_ERR_KEY_NOT_FOUND)
        {
          // The conditions do not satisfy. Moves on to the next scanned
          // record.
          continue;
        }
        else
        {
          // The conditions are statisfied.
          DBUG_ASSERT(icp_rc == 0);
        }
      }

      size_t pk_len= sk_schema->KeyDefinition()->get_primary_key_tuple(
          table, *pk_descr_, packed_key_slice, pk_packed_tuple_);
      last_read_key_= EloqKey(pk_packed_tuple_, pk_len);

      // Rowid Filter check.
      int rowid_filter_rc= MatchRowidFilter(last_read_key_);
      if (rowid_filter_rc == HA_ERR_END_OF_FILE)
      {
        if (!is_pk_scan_needed)
        {
          DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
        }
        else
        {
          scan_batch_idx_= scan_batch_.size();
          is_last_scan_batch_= true;
          break;
        }
      }
      else if (rowid_filter_rc == HA_ERR_ABORTED_BY_USER)
      {
        DBUG_RETURN(HA_ERR_ABORTED_BY_USER);
      }
      else if (rowid_filter_rc == HA_ERR_KEY_NOT_FOUND)
      {
        // The conditions do not satisfy. Moves on to the next scanned
        // record.
        continue;
      }
      else
      {
        // The conditions are statisfied.
        DBUG_ASSERT(rowid_filter_rc == 0);
      }

      /**
       * If is_mrr_sort_rowid_=true, here will only need primary key and it
       * will be parsed for future keys compare. It will do not need to look
       * up the primary index here.
       */
      if (!is_pk_scan_needed)
      {
        // If the secondary key covers all columns, the record is constructed
        // from the key. There is no need to look up the primary index.

        DBUG_RETURN(0);
      }
      else
      {
        vct_key_order.emplace_back(std::move(last_read_key_),
                                   tmp_scan_batch.size(), sk_ts);
        // TxKey and TxRecord are maybe freed when next ScanBatchTxRequest, so
        // here can not save them into scan_batch directly.
        // Tuples in tmp_scan_batch only be used in unlock_row to free the lock
        // in following time, so here only copy status, version_ts and cce_addr
        // into new tuple.
        const auto &tuple= scan_batch_[scan_batch_idx_ - 1];
        tmp_scan_batch.emplace_back(TxKey(), nullptr, tuple.status_,
                                    tuple.version_ts_, tuple.cce_addr_);
      }
    }
  } while (ccm_scan_open_ || store_key != nullptr);

  if (vct_key_order.size() == 0)
  {
    DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
  }

  // Sort vct_key_order by PKs
  struct
  {
    bool operator()(const KeyOrder &a, const KeyOrder &b)
    {
      return a.key_ < b.key_;
    }
  } KeyComp;
  std::sort(vct_key_order.begin(), vct_key_order.end(), KeyComp);

  scan_batch_= std::move(tmp_scan_batch);
  batch_order_.resize(scan_batch_.size());
  sk_pk_scan_batch_.clear();
  sk_pk_scan_batch_.reserve(vct_key_order.size());
  batch_key_.resize(vct_key_order.size());
  batch_rec_.resize(vct_key_order.size());

  for (size_t i= 0; i < vct_key_order.size(); i++)
  {
    KeyOrder &order= vct_key_order[i];
    batch_key_[i]= std::move(order.key_);
    sk_pk_scan_batch_.emplace_back(TxKey(&batch_key_[i]), &batch_rec_[i],
                                   RecordStatus::Unknown, order.ts_);
    batch_order_[order.pos_]= i;
  }

  std::pair<const std::function<void()> *, const std::function<void()> *>
      coro_functors= my_tx->CoroFunctors();
  uint64_t pk_key_version= table_schema_->KeySchema()->SchemaTs();
  BatchReadTxRequest batch_req(GetBaseTableNameFromTableSchema(),
                               pk_key_version, sk_pk_scan_batch_, for_update,
                               for_share, false, coro_functors.first,
                               coro_functors.second, txm);
  txm->Execute(&batch_req);
  batch_req.Wait();

  if (batch_req.IsError())
  {
    my_tx->tx_err_code_= batch_req.ErrorCode();
    DBUG_RETURN(convert_tx_error(my_tx->tx_err_code_));
  }

  if (storage_hd != nullptr)
  {
    for (size_t i= 0; i < sk_pk_scan_batch_.size(); i++)
    {
      RecordStatus rec_status= sk_pk_scan_batch_[i].status_;
      uint64_t latest_version_ts= 1U;

      if (rec_status == RecordStatus::Unknown ||
          rec_status == RecordStatus::VersionUnknown ||
          rec_status == RecordStatus::BaseVersionMiss)
      {
        bool store_found= false;
        bool success= storage_hd->Read(
            *GetBaseTableNameFromTableSchema(), sk_pk_scan_batch_[i].key_,
            *sk_pk_scan_batch_[i].record_, store_found, latest_version_ts,
            table_schema_);
        if (!success)
        {
          sk_pk_scan_batch_[i].status_= RecordStatus::Deleted;
          continue;
        }

        sk_pk_scan_batch_[i].status_=
            store_found ? RecordStatus::Normal : RecordStatus::Deleted;

        if (sk_pk_scan_batch_[i].status_ == RecordStatus::Unknown)
        {
          // A ReadOutside request brings a data store record into the cc map
          //  for caching.
          auto [yield_func, resume_func]= my_tx->CoroFunctors();
          ReadOutsideTxRequest read_outside(
              *sk_pk_scan_batch_[i].record_,
              sk_pk_scan_batch_[i].status_ == RecordStatus::Deleted,
              latest_version_ts, nullptr, yield_func, resume_func, txm);
          txm->Execute(&read_outside);
          read_outside.Wait();
        }
      }

      if (sk_pk_scan_batch_[i].status_ == RecordStatus::ArchiveVersionMiss ||
          (!for_update && !for_share &&
           txm->GetIsolationLevel() == IsolationLevel::Snapshot &&
           latest_version_ts > txm->GetStartTs()))
      {
        EloqRecord visible_record;
        uint64_t archive_version_ts= 1U;
        bool success= storage_hd->FetchVisibleArchive(
            *GetBaseTableNameFromTableSchema(), GetKVCatalogInfo(),
            sk_pk_scan_batch_[i].key_, txm->GetStartTs(),
            *sk_pk_scan_batch_[i].record_, sk_pk_scan_batch_[i].status_,
            archive_version_ts);
        if (!success)
        {
          my_tx->tx_err_code_= txservice::TxErrorCode::DATA_STORE_ERROR;
          DBUG_RETURN(convert_tx_error(my_tx->tx_err_code_));
        }
      }
    }
  }

  // Return the first record in this batch
  scan_batch_idx_= 0;
  while (scan_batch_idx_ < scan_batch_.size())
  {
    uint64_t pos= batch_order_[scan_batch_idx_];
    assert(pos != UINT64_MAX);
    txservice::ScanBatchTuple &scan_tuple= sk_pk_scan_batch_[pos];
    if (scan_tuple.status_ != RecordStatus::Normal)
    {
      scan_batch_idx_++;
      continue;
    }

    const EloqKey *key= scan_tuple.key_.GetKey<EloqKey>();
    const EloqRecord *rec= static_cast<const EloqRecord *>(scan_tuple.record_);
    DecodeRecord(table_record, key, rec);
    last_read_key_= std::move(*key);
    scan_batch_idx_++;
    DBUG_RETURN(0);
  }

  DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
}

int ha_eloq::IndexScanClose()
{
  DBUG_ENTER_FUNC();

  MyEloqTx *my_tx= get_myeloq_tx(ha_thd());
  DBUG_ASSERT(my_tx != nullptr);
  TransactionExecution *txm= my_tx->Txm();

  // If txm is null, then TxExecution is already recycled, no need to send
  // ScanCloseRequest. For example, statement `handler t read first` will
  // commit the transaction automatically and thus TxExection is freed. But
  // when connection closes, rnd_end() will still be called.
  if (txm != nullptr)
  {
    const TableName *scan_table_name= nullptr;
    if (scan_index_ == MAX_INDEXES || scan_index_ == table->s->primary_key)
    {
      scan_table_name= GetBaseTableNameFromTableSchema();
    }
    else
    {
      scan_table_name= &table_schema_->IndexNameSchema(scan_index_).first;
    }

    for (size_t idx= scan_batch_idx_; idx < scan_batch_.size(); ++idx)
    {
      const ScanBatchTuple &tuple= scan_batch_[idx];
      unlock_batch_.emplace_back(tuple.cce_addr_, tuple.version_ts_,
                                 tuple.status_);
    }

    if (batch_order_.size() > 0)
    {
      for (size_t idx= scan_batch_idx_; idx < scan_batch_.size(); ++idx)
      {
        uint64_t pos= batch_order_[idx];
        if (pos == UINT64_MAX)
        {
          continue;
        }
        const ScanBatchTuple &tuple= sk_pk_scan_batch_[pos];
        unlock_batch_.emplace_back(tuple.cce_addr_, tuple.version_ts_,
                                   tuple.status_);
      }
    }

    txm->CloseTxScan(scan_alias_, *scan_table_name, unlock_batch_);

#ifdef RANGE_PARTITION_ENABLED
    if (!scan_open_tx_req_.IsFinished())
    {
      scan_open_tx_req_.Wait();
    }
#endif
  }
  scan_alias_= UINT64_MAX;
  scan_index_= MAX_INDEXES;
  ccm_scan_open_= false;
  ccm_scan_key_= nullptr;
  ccm_scan_rec_= nullptr;
  ccm_scan_rec_status_= txservice::RecordStatus::Unknown;

  scan_batch_idx_= UINT64_MAX;
  if (scan_batch_.size() > DEFAULT_SCAN_TUPLE_SIZE)
  {
    scan_batch_.resize(DEFAULT_SCAN_TUPLE_SIZE);
    scan_batch_.shrink_to_fit();
  }
  scan_batch_.clear();

  sk_pk_scan_batch_idx_= UINT64_MAX;
  if (sk_pk_scan_batch_.size() > DEFAULT_SCAN_TUPLE_SIZE)
  {
    sk_pk_scan_batch_.resize(DEFAULT_SCAN_TUPLE_SIZE);
    sk_pk_scan_batch_.shrink_to_fit();
  }
  sk_pk_scan_batch_.clear();
  if (batch_order_.size() > DEFAULT_SCAN_TUPLE_SIZE)
  {
    batch_order_.resize(DEFAULT_SCAN_TUPLE_SIZE);
    batch_order_.shrink_to_fit();
  }
  batch_order_.clear();

  if (storage_scanner_ != nullptr)
  {
    storage_scanner_->End();
    storage_scanner_= nullptr;
  }
  prefix_match_key_= nullptr;

  pushed_conds_.clear();

  start_range_ptr_= nullptr;
  if (batch_key_.size() > 0)
  {
    batch_key_.clear();
    batch_rec_.clear();
    batch_key_.resize(DEFAULT_SCAN_TUPLE_SIZE);
    batch_rec_.resize(DEFAULT_SCAN_TUPLE_SIZE);
  }

  DBUG_RETURN(0);
}

/**
 * @brief Extract uuid value from packed hidden pk.
 *
 * @param hidden_pk_id
 * @param row_key
 * @return int
 */
int ha_eloq::ReadHiddenPkFromRowkey(uchar *hidden_pk_id,
                                    const EloqKey *row_key)
{
  DBUG_ASSERT(table != nullptr);
  DBUG_ASSERT(has_hidden_pk(table));

  Slice rowkey_slice= row_key->PackedValueSlice();

  // Get hidden primary key from old key slice
  mono_string_reader reader(&rowkey_slice);

  const int length= MY_UUID_SIZE; /* UUID length is 16 */
  const uchar *from= reinterpret_cast<const uchar *>(reader.read(length));
  if (from == nullptr)
  {
    /* Mem-comparable image doesn't have enough bytes */
    return HA_ERR_TABLE_CORRUPT;
  }

  memcpy(hidden_pk_id, from, length);
  return 0;
}

RecordStatus ha_eloq::PkRead(MyEloqTx *my_tx, const TxKey &pk_tx_key,
                             EloqRecord &eloq_record, bool for_update,
                             uint64_t ts, bool point_read_on_miss)
{
  TransactionExecution *txm= my_tx->Txm();
  for_update= for_update || (lock.type >= TL_WRITE_ALLOW_WRITE);
  bool for_share= (lock.type == TL_READ_WITH_SHARED_LOCKS);

  auto [yield_func, resume_func]= my_tx->CoroFunctors();

  uint64_t key_version= table_schema_->KeySchema()->SchemaTs();
  const TableName *base_table_name= GetBaseTableNameFromTableSchema();

  ReadTxRequest read_req(base_table_name, key_version, &pk_tx_key,
                         &eloq_record, for_update, for_share, false, ts, false,
                         false, point_read_on_miss, yield_func, resume_func,
                         txm);

  txm->Execute(&read_req);
  read_req.Wait();

  if (read_req.IsError())
  {
    my_tx->tx_err_code_= read_req.ErrorCode();
    return RecordStatus::Unknown;
  }

  RecordStatus rec_status= read_req.Result().first;
  if (rec_status == RecordStatus::Normal ||
      rec_status == RecordStatus::Deleted)
  {
    return rec_status;
  }

  if (storage_hd == nullptr)
  {
    return RecordStatus::Deleted;
  }

  EloqRecord latest_rec;
  RecordStatus latest_rec_status= RecordStatus::Unknown;
  uint64_t latest_version_ts= 1U;

  if (rec_status == RecordStatus::Unknown ||
      rec_status == RecordStatus::VersionUnknown ||
      rec_status == RecordStatus::BaseVersionMiss)
  {
    bool store_found= false;
    bool success= storage_hd->Read(*GetBaseTableNameFromTableSchema(),
                                   pk_tx_key, latest_rec, store_found,
                                   latest_version_ts, table_schema_);
    if (!success)
    {
      return RecordStatus::Deleted;
    }
    latest_rec_status=
        store_found ? RecordStatus::Normal : RecordStatus::Deleted;

    if (rec_status == RecordStatus::Unknown)
    {
      // A ReadOutside request brings a data store record into the cc map
      //  for caching.
      ReadOutsideTxRequest read_outside(
          latest_rec, latest_rec_status == RecordStatus::Deleted,
          latest_version_ts, nullptr, yield_func, resume_func, txm);
      txm->Execute(&read_outside);
      read_outside.Wait();
    }
  }

  if (rec_status == RecordStatus::ArchiveVersionMiss ||
      (!for_update && !for_share &&
       txm->GetIsolationLevel() == IsolationLevel::Snapshot &&
       latest_version_ts > txm->GetStartTs()))
  {
    EloqRecord visible_record;
    uint64_t archive_version_ts= 1U;
    bool success= storage_hd->FetchVisibleArchive(
        *GetBaseTableNameFromTableSchema(), GetKVCatalogInfo(), pk_tx_key,
        txm->GetStartTs(), visible_record, rec_status, archive_version_ts);
    if (success)
    {
      eloq_record= std::move(visible_record);
    }
    else
    {
      my_tx->tx_err_code_= txservice::TxErrorCode::DATA_STORE_ERROR;
      rec_status= RecordStatus::Unknown;
    }
  }
  else
  {
    eloq_record= latest_rec;
    rec_status= latest_rec_status;
  }

  return rec_status;
}

std::pair<RecordStatus, uint64_t> ha_eloq::SkRead(MyEloqTx *my_tx,
                                                  const EloqKey &eloq_key,
                                                  EloqRecord &eloq_record,
                                                  bool for_update, uint kid)
{
  TransactionExecution *txm= my_tx->Txm();

  for_update= for_update || (lock.type >= TL_WRITE_ALLOW_WRITE);
  bool for_share= (lock.type == TL_READ_WITH_SHARED_LOCKS);
  bool is_covering_keys=
      for_update ? false : table->covering_keys.is_set(active_index);

  const TableName index_name{
      table_schema_->IndexNameSchema(kid).first.StringView(),
      table_schema_->IndexNameSchema(kid).first.Type(),
      table_schema_->IndexNameSchema(kid).first.Engine()};
  assert(index_name.Type() == TableType::UniqueSecondary);

  auto [yield_func, resume_func]= my_tx->CoroFunctors();
  TxKey tx_key(&eloq_key);
  uint64_t key_version= table_schema_->IndexNameSchema(kid).second.SchemaTs();
  ReadTxRequest read_req(&index_name, key_version, &tx_key, &eloq_record,
                         for_update, for_share, false, 0, is_covering_keys,
                         false, false, yield_func, resume_func, txm);
  RecordStatus rec_status;
  uint64_t unique_sk_ts= 0;

  txm->Execute(&read_req);
  read_req.Wait();

  if (read_req.IsError())
  {
    my_tx->tx_err_code_= read_req.ErrorCode();
    return {RecordStatus::Unknown, 0};
  }

  std::tie(rec_status, unique_sk_ts)= read_req.Result();
  if (rec_status == RecordStatus::Deleted ||
      rec_status == RecordStatus::Normal)
  {
    return std::pair<RecordStatus, uint64_t>(rec_status, unique_sk_ts);
  }

  if (storage_hd == nullptr)
  {
    return std::pair<RecordStatus, uint64_t>(RecordStatus::Deleted, 0);
  }

  EloqRecord latest_rec;
  RecordStatus latest_rec_status= RecordStatus::Unknown;
  uint64_t latest_version_ts= 1U;

  if (rec_status == RecordStatus::Unknown ||
      rec_status == RecordStatus::VersionUnknown ||
      rec_status == RecordStatus::BaseVersionMiss)
  {
    bool store_found= false;
    bool success= storage_hd->Read(index_name, tx_key, latest_rec, store_found,
                                   latest_version_ts, table_schema_);
    if (!success)
    {
      return std::pair<RecordStatus, uint64_t>(RecordStatus::Deleted, 0);
    }
    latest_rec_status=
        store_found ? RecordStatus::Normal : RecordStatus::Deleted;

    if (rec_status == RecordStatus::Unknown)
    {
      // A ReadOutside request brings a data store record into the cc map
      //  for caching.
      ReadOutsideTxRequest read_outside(
          latest_rec, latest_rec_status == RecordStatus::Deleted,
          latest_version_ts, nullptr, yield_func, resume_func, txm);
      txm->Execute(&read_outside);
      read_outside.Wait();
    }
  }

  if (rec_status == RecordStatus::ArchiveVersionMiss ||
      (!for_update && !for_share &&
       txm->GetIsolationLevel() == IsolationLevel::Snapshot &&
       latest_version_ts > txm->GetStartTs()))
  {
    EloqRecord visible_record;
    uint64_t archive_version_ts= 1U;
    bool success= storage_hd->FetchVisibleArchive(
        index_name, GetKVCatalogInfo(), tx_key, txm->GetStartTs(),
        visible_record, rec_status, archive_version_ts);
    if (success)
    {
      eloq_record= std::move(visible_record);
      unique_sk_ts= archive_version_ts;
    }
    else
    {
      my_tx->tx_err_code_= txservice::TxErrorCode::DATA_STORE_ERROR;
      rec_status= RecordStatus::Unknown;
    }
  }
  else
  {
    eloq_record= latest_rec;
    rec_status= latest_rec_status;
    unique_sk_ts= latest_version_ts;
  }

  return std::pair<RecordStatus, uint64_t>(rec_status, unique_sk_ts);
}

bool ha_eloq::ReadSnapshotFromDataStore(
    const txservice::TableName &table_name,
    const txservice::KVCatalogInfo *kv_info, uint64_t read_ts,
    bool need_fetch_base, const EloqKey &eloq_key, EloqRecord &eloq_record,
    txservice::RecordStatus &rec_status)
{
  bool success= true;
  uint64_t commit_ts= 1U;
  TxKey tx_key(&eloq_key);
  if (need_fetch_base)
  {
    bool store_found= false;
    success= storage_hd->Read(table_name, tx_key, eloq_record, store_found,
                              commit_ts, table_schema_);
    rec_status= store_found ? txservice::RecordStatus::Normal
                            : txservice::RecordStatus::Deleted;
    if (!success)
    {
      return false;
    }
    else if (commit_ts <= read_ts)
    {
      return true;
    }
  }

  success=
      storage_hd->FetchVisibleArchive(table_name, kv_info, tx_key, read_ts,
                                      eloq_record, rec_status, commit_ts);
  return success;
}

/*
 * eloq store data in kv store, hence the supported collations
 * of ICP are limited by the kv store. Currently both Cassandra
 * and DynamoDB only support binary collations for index condition pushdown.
 */
static const std::set<uint> MONO_ICP_COLLATIONS= {
    COLLATION_BINARY, COLLATION_UTF8_BIN, COLLATION_LATIN1_BIN,
    COLLATION_UTF8MB4_BIN};

static bool mono_is_col_icp_supported(const Field *const field)
{
  const enum_field_types type= field->real_type();
  /* Handle [VAR](CHAR|BINARY) or TEXT|BLOB */
  if (type == MYSQL_TYPE_VARCHAR || type == MYSQL_TYPE_STRING ||
      type == MYSQL_TYPE_BLOB)
  {
    const CHARSET_INFO *cs= field->charset();
    return (MONO_ICP_COLLATIONS.find(cs->number) != MONO_ICP_COLLATIONS.end());
  }
  return true;
}

void ha_eloq::AddPushedDownCondition(Item *cond_item)
{
  // We only consider pushing down conditions in the form of "col_name
  // cmp_op value", where cmp_op is one of the comparison operators <, <=,
  // =, >=, >.
  if (cond_item->type() != Item::FUNC_ITEM)
  {
    return;
  }

  Item_func *cond_func= (Item_func *) cond_item;
  Item_func::Functype func_type= cond_func->functype();
  Item **args= cond_func->arguments();

  if (cond_func->argument_count() != 2 ||
      args[0]->type() != Item_func::FIELD_ITEM ||
      args[1]->type() != Item_func::CONST_ITEM ||
      !(func_type == Item_func::EQ_FUNC || func_type == Item_func::LT_FUNC ||
        func_type == Item_func::LE_FUNC || func_type == Item_func::GE_FUNC ||
        func_type == Item_func::GT_FUNC))
  {
    return;
  }

  Item_field *col_field= (Item_field *) args[0];
  Item_literal *val_field= (Item_literal *) args[1];

  enum_field_types col_type= col_field->field_type();
  enum_field_types val_type= val_field->field_type();

  if (!mono_is_col_icp_supported(col_field->field))
  {
    return;
  }

  // Below variables were copied/externed from field.cc
  const int FIELDTYPE_TEAR_FROM= (MYSQL_TYPE_BIT + 1);
  const int FIELDTYPE_TEAR_TO= (MYSQL_TYPE_NEWDECIMAL - 1);
  const int FIELDTYPE_LAST= 254;
  const int FIELDTYPE_NUM=
      FIELDTYPE_TEAR_FROM + (FIELDTYPE_LAST - FIELDTYPE_TEAR_TO);

  extern int merge_type2index(enum_field_types merge_type);
  extern enum_field_types field_types_merge_rules[FIELDTYPE_NUM]
                                                 [FIELDTYPE_NUM];
  enum_field_types res= field_types_merge_rules[merge_type2index(col_type)]
                                               [merge_type2index(val_type)];
  if (res == col_type ||
      (res == MYSQL_TYPE_VARCHAR && col_type == MYSQL_TYPE_STRING))
  {
    pushed_conds_.push_back({func_type, col_field, val_field});
  }
}

/**
 * Pushdown rules
 * Rule 1: Numeric data type, such as INT, DOUBLE, pushdown.
 *
 * Rule 2: Date and Time data typpe, such as DATE, DATETIME, and SET, ENUM,
 * JSON and other uncertain data types, do not pushdown currently.
 *
 * Rule 3: String data type
 * 1) CHAR/VARCHAR/TEXT
 * Do not pushdown when field value type can not match with filed type.
 * Otherwise, pushdown.
 * Furthermore, for VARCHAR/TEXT, remove trailing space from condition value,
 * and when operator is '=', using '>=' to pushdown, otherwise use original
 * operator.(This is used to adapt to MySQL PAD SPACE and NO PAD attribute,
 * such as utf8mb3_bin and utf8mb3_nopad_bin. When comparing strings, the
 * behavior depend on whether the collation is NOPAD or not.)
 * 2) BINARY/VARBINARY/BLOB
 * When field value type can not match with filed type, do not pushdown.
 * Otherwise, pushdwon.
 */
std::vector<txservice::store::DataStoreSearchCond> ha_eloq::BindPushedCond()
{
  // Don't take active index field as pushdown condition.
  std::set<const mysql::Field *> active_index_fields;
  if (active_index < MAX_INDEXES)
  {
    for (uint i= 0; i < table->key_info[active_index].user_defined_key_parts;
         i++)
    {
      active_index_fields.insert(
          table->key_info[active_index].key_part[i].field);
    }
  }
  std::vector<txservice::store::DataStoreSearchCond> res;

  for (const auto &pushed_cond : pushed_conds_)
  {
    if (active_index_fields.find(pushed_cond.col_field_->field) !=
        active_index_fields.end())
    {
      continue;
    }

    bool pushdown= true;
    txservice::store::DataStoreDataType data_type=
        txservice::store::DataStoreDataType::Numeric;

    Item_func::Functype func_type= pushed_cond.func_type_;
    Item_field *col_field= pushed_cond.col_field_;
    Item_literal *val_field= pushed_cond.val_field_;

    switch (col_field->field_type())
    {
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_LONGLONG:
    case MYSQL_TYPE_FLOAT:
    case MYSQL_TYPE_DOUBLE:
      data_type= txservice::store::DataStoreDataType::Numeric;
      break;
    case MYSQL_TYPE_VARCHAR:
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_STRING:
    case MYSQL_TYPE_BLOB:
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
      // Whether field types are matched or not.
      pushdown= (val_field->field_type() == MYSQL_TYPE_STRING ||
                 val_field->field_type() == MYSQL_TYPE_VARCHAR)
                    ? true
                    : false;
      // binary character set when number is 63.
      data_type= (col_field->field->charset()->number == 63)
                     ? txservice::store::DataStoreDataType::Blob
                     : txservice::store::DataStoreDataType::String;
      break;
    default:
      // Only push down conditions on certain data types. The list may be
      // expanded as more data types are thoroughly examined.
      pushdown= false;
      break;
    }

    if (!pushdown)
    {
      continue;
    }

    std::string field_name(col_field->field_name.str,
                           col_field->field_name.length);
    std::string op;

    switch (func_type)
    {
    case Item_func::EQ_FUNC:
      op.append("=");
      break;
    case Item_func::LT_FUNC:
      op.append("<");
      break;
    case Item_func::LE_FUNC:
      op.append("<=");
      break;
    case Item_func::GT_FUNC:
      op.append(">");
      break;
    case Item_func::GE_FUNC:
      op.append(">=");
      break;
    default:
      assert(true && "Unsupported cmp function.");
      break;
    }

    mysql::Field *field= col_field->field;

    // Move and restore field ptr is similar with
    // EloqRecordSchema::Decode().
    uint field_offset= field->ptr - table->record[0];
    uint null_offset= field->null_offset();
    bool maybe_null= field->real_maybe_null();
    field->move_field(table->record[2] + field_offset,
                      maybe_null ? table->record[2] + null_offset : nullptr,
                      field->null_bit);

    val_field->save_in_field(field, 1);

    // NOTE: 1) For EloqDataType::String, the EloqFieldType::len_
    // equal to field->field_length, and when EncodeString, the content
    // ::vector<char> is copied from field->ptr to field->ptr + len_, and
    // when BindCassStatement value_length is ::len_, so, the length of
    // pusheddown condition's value must be field_length.
    // 2) We should not using sql mode PAD_CHAR_TO_FULL_LENGTH in storage
    // engine layer, because the behavior of this sql mode is determined by
    // MySQL layer.
    std::string val_str;
    EloqFieldType field_type= EloqFieldType::Convert(field);
    switch (field_type.data_type_)
    {
    case EloqDataType::Binary:
    case EloqDataType::String: {
      val_str.append((char *) field->ptr, field_type.len_);
      break;
    }
    case EloqDataType::VarBinary: {
      if (field->real_type() == MYSQL_TYPE_VARCHAR)
      {
        size_t val_bytes_len= 0;
        uint16_t offset= field_type.len_;
        std::copy(field->ptr, field->ptr + offset, (char *) &val_bytes_len);
        val_str.append((char *) field->ptr + field_type.len_, val_bytes_len);
      }
      else
      {
        // [TINY|MEDIUM|LONG]BLOB type
        Field_blob *blob_field= (Field_blob *) field;
        uint32_t blob_len= blob_field->get_length();
        uchar *blob_ptr= blob_field->get_ptr();
        val_str.append((char *) blob_ptr, blob_len);
      }
      break;
    }
    default: {
      StringBuffer<MAX_FIELD_WIDTH> str;
      field->val_str(&str);
      val_str.append((char *) str.ptr(), str.length());
      // VARCHAR/TEXT type
      if (field_type.data_type_ == EloqDataType::VarString)
      {
        // Remove trailing space.
        size_t endpos= val_str.find_last_not_of(' ');
        if (endpos != std::string::npos && endpos < val_str.length() - 1)
        {
          val_str.replace((endpos + 1), (val_str.length() - endpos - 1), "");
        }
        // Fetch values from kv without regard for trailing sapces.
        if (!op.compare("="))
        {
          op.assign(">=");
        }
      }
      break;
    }
    }

    field->move_field(table->record[0] + field_offset,
                      maybe_null ? table->record[0] + null_offset : nullptr,
                      field->null_bit);

    res.push_back({field_name, op, val_str, data_type});
  }

  return res;
}

std::string ha_eloq::PushedConditionString(
    const std::vector<txservice::store::DataStoreSearchCond>
        &pushdown_condition)
{
  std::string pushed_conds_str;

  for (const auto &pushed_cond : pushdown_condition)
  {
    if (pushed_conds_str.size())
    {
      pushed_conds_str.append(" AND ");
    }
    pushed_conds_str.append("\"");
    pushed_conds_str.append(pushed_cond.field_name_);
    pushed_conds_str.append("\"");
    pushed_conds_str.append(pushed_cond.op_);

    switch (pushed_cond.data_type_)
    {
    case txservice::store::DataStoreDataType::String: {
      std::stringstream ss;
      ss << '\'';

      // Escape char ' in val_str.
      for (size_t i= 0; i < pushed_cond.val_str_.length(); i++)
      {
        char c= pushed_cond.val_str_[i];
        if (c == '\'')
        {
          ss << '\'' << '\'';
        }
        else
        {
          ss << c;
        }
      }

      ss << '\'';
      pushed_conds_str.append(ss.str());
      break;
    }
    case txservice::store::DataStoreDataType::Blob: {
      std::stringstream ss;
      ss << "0x" << std::hex << std::setfill('0');
      for (size_t pos= 0; pos < pushed_cond.val_str_.length(); ++pos)
      {
        ss << std::setw(2)
           << static_cast<unsigned>(
                  static_cast<uint8_t>(pushed_cond.val_str_[pos]));
      }
      pushed_conds_str.append(ss.str());
      break;
    }
    case txservice::store::DataStoreDataType::Numeric: {
      pushed_conds_str.append(pushed_cond.val_str_);
      break;
    }
    default: {
      // Type is certain for pushdown conditions, should not be here.
      assert(false);
    }
    }
  }

  return pushed_conds_str;
}

int ha_eloq::MatchIcp(uchar *table_record, const EloqKey &sk,
                      const EloqRecord &rec, const EloqKeySchema *sk_schema)
{
  int rc= 0;

  sk_schema->KeyDefinition()->unpack_record(table, table_record,
                                            sk.PackedValueSlice(),
                                            rec.UnpackInfoSlice(), false);

  if (pushed_idx_cond && pushed_idx_cond_keyno == active_index)
  {
    const check_result_t icp_status= handler_index_cond_check(this);
    switch (icp_status)
    {
    case CHECK_ERROR:
      assert(false);
      rc= HA_ERR_TABLE_CORRUPT;
      break;
    case CHECK_NEG:
      rc= HA_ERR_KEY_NOT_FOUND;
      break;
    case CHECK_POS:
      rc= 0;
      break;
    case CHECK_OUT_OF_RANGE:
      rc= HA_ERR_END_OF_FILE;
      break;
    case CHECK_ABORTED_BY_USER:
      rc= HA_ERR_ABORTED_BY_USER;
      break;
    default:
      assert(false);
      rc= 0;
      break;
    }
  }

  return rc;
}

// The filter is rowid filter, NOT primary key filter.
//
// Both Innodb and eloq take primary key as rowid. MyISAM doesn't take
// primary key as rowid.
//
// According to rowid_filter.cc/init_cost_info_for_usable_range_rowid_filters()
// and rowid_filter.cc/best_range_rowid_filter_for_partial_join(), Rowid filter
// won't apply to pk read if pk is clustering key. And example in
// https://mariadb.com/kb/en/rowid-filtering-optimization/ is not reproducible
// on Innodb or eloqdb if orderkey is primary key.
int ha_eloq::MatchRowidFilter(const EloqKey &pk)
{
  int rc= 0;

  if (pushed_rowid_filter && rowid_filter_is_active)
  {
    size_t packed_key_len= pk.PackedValueSlice().size();
    assert(ref_length >= packed_key_len);
    memcpy(ref, pk.PackedValue().data(), packed_key_len);
    memset(ref + packed_key_len, 0, ref_length - packed_key_len);

    const check_result_t icp_status= handler_rowid_filter_check(this);
    switch (icp_status)
    {
    case CHECK_ERROR:
      assert(false);
      rc= HA_ERR_TABLE_CORRUPT;
      break;
    case CHECK_NEG:
      rc= HA_ERR_KEY_NOT_FOUND;
      break;
    case CHECK_POS:
      rc= 0;
      break;
    case CHECK_OUT_OF_RANGE:
      rc= HA_ERR_END_OF_FILE;
      break;
    case CHECK_ABORTED_BY_USER:
      rc= HA_ERR_ABORTED_BY_USER;
      break;
    default:
      assert(false);
      rc= 0;
      break;
    }
  }

  return rc;
}

int ha_eloq::SetScanParameters(mysql::ha_rkey_function find_flag,
                               ScanDirection &direction, bool &inclusive,
                               bool &prefix_match)
{
  inclusive= false;
  direction= ScanDirection::Forward;
  prefix_match= false;

  switch (find_flag)
  {
  case HA_READ_KEY_EXACT:
    // R(k1, k2, v) WHERE k1 = c
    inclusive= true;
    prefix_match= true;
    break;
  case HA_READ_BEFORE_KEY:
    // R(k, v) WHERE k < c ORDER BY k DESC
    inclusive= false;
    direction= ScanDirection::Backward;
    break;
  case HA_READ_AFTER_KEY:
    // R(k, v) WHERE k > c
    // R(k1, k2, v) WHERE k1 > c
    inclusive= false;
    break;
  case HA_READ_KEY_OR_NEXT:
    // R(k, v) WHERE k >= c
    // R(k1, k2, v) WHERE k1 >= c
    inclusive= true;
    break;
  case HA_READ_KEY_OR_PREV:
  case HA_READ_PREFIX:
    // RocksDB: This flag is not used by the SQL layer.
    return HA_ERR_UNSUPPORTED;
  case HA_READ_PREFIX_LAST:
    // R(k1, k2, v) WHERE k1 = c ORDER BY k1 DESC
    prefix_match= true;
    inclusive= true;
    direction= ScanDirection::Backward;
    break;
  case HA_READ_PREFIX_LAST_OR_PREV:
    // R(k1, k2, v) WHERE k1 <= c ORDER BY k1 DESC
    // R(k, v) WHERE k <= c ORDER BY k DESC
    inclusive= true;
    direction= ScanDirection::Backward;
    break;
  default:
    return HA_ERR_UNSUPPORTED;
  }

  return 0;
}

/**
  @brief
  Used to delete a table. By the time delete_table() has been called all
  opened references to this table will have been closed (and your globally
  shared references released). The variable name will just be the name of
  the table. You will need to remove any files you have created at this
  point.

  @details
  If you do not implement this, the default delete_table() is called from
  handler.cc and it will delete all files with the file extensions returned
  by bas_ext().

  Called from handler.cc by delete_table and ha_create_table(). Only used
  during create if the table_flag HA_DROP_BEFORE_CREATE was specified for
  the storage engine.

  @see
  delete_table and ha_create_table() in handler.cc
*/
int ha_eloq::delete_table(const char *name)
{
  DBUG_ENTER_FUNC();

  THD *thd= ha_thd();

  // tx_exists is set to false for new TransactionExecution
  MyEloqTx *my_tx= nullptr;
  bool tx_exists= false;
  if (!get_or_create_myeloq_tx(thd, &my_tx, &tx_exists))
  {
    my_error(HA_ERR_ELOQ_START_TRANSACTION_FAILED, MYF(0));
    DBUG_RETURN(HA_ERR_ELOQ_START_TRANSACTION_FAILED);
  }
  DBUG_ASSERT(my_tx != nullptr);
  TransactionExecution *txm= my_tx->Txm();
  std::string_view name_sv{name};
  txservice::TableName table_name{name_sv, txservice::TableType::Primary,
                                  txservice::TableEngine::EloqSql};
  my_tx->ClearSchemaReader(table_name);

  CatalogKey table_key(table_name);
  CatalogRecord catalog_rec;

  bool exists= false;
  // ReadTxRequest of DDL should set "for_write=true" to avoid potential
  // deadlock.
  TxErrorCode err= my_tx->ReadCatalog(table_key, catalog_rec, true, exists);
  if (err != TxErrorCode::NO_ERROR)
  {
    // TODO: there is an error when accessing the data store. Should
    // return an informative error code.
    DBUG_RETURN(convert_tx_error(err));
  }
  else
  {
    if (!exists)
    {
      DBUG_RETURN(HA_ERR_NO_SUCH_TABLE);
    }
  }

  auto [yield_func, resume_func]= my_tx->CoroFunctors();

  std::string empty_image{""};
  UpsertTableTxRequest upsert_table_req(
      &table_name, &catalog_rec.Schema()->SchemaImage(),
      catalog_rec.SchemaTs(), &empty_image, OperationType::DropTable, nullptr,
      yield_func, resume_func, txm);

  txm->Execute(&upsert_table_req);
  upsert_table_req.Wait();
  UpsertResult rst= upsert_table_req.Result();

  if (rst == UpsertResult::Failed)
  {
    my_tx->tx_err_code_= upsert_table_req.ErrorCode();
  }
  else if (rst == UpsertResult::Unverified)
  {
    my_error(HA_ERR_ELOQ_TRANSACTION_BREAK, MYF(ME_WARNING),
             "Current transaction coordinator is no longer the leader node. "
             "The alter table statement will be processed in a failover node."
             " Please recheck the result of alter table statement later.");
  }

  // THD::cleanup() will close all temporary tables, but won`t call
  // trans_commit_stmt, which will leave orphan ha_info, so we should
  // call ha_commit_trans manually.
  if (is_tmp_table(std::string(name)) && !thd->in_sub_stmt)
  {
    ha_commit_trans(thd, false);
  }

  if (rst != UpsertResult::Failed)
  {
    DBUG_RETURN(0);
  }
  else
  {
    DBUG_RETURN(HA_ERR_ELOQ_DROP_TABLE_FAILED);
  }
}

/**
  @brief
  Given a starting key and an ending key, estimate the number of rows that
  will exist between the two keys.
  The handler can also optionally update the 'pages' parameter with the
  page number that contains the min and max keys. This will help the
  optimizer to know if two ranges are partly on the same pages and if the
  min and max key are on the same page.

  @details
  end_key may be empty, in which case determine if start_key matches any
  rows.

  Called from opt_range.cc by check_quick_keys().

  @see
  check_quick_keys() in opt_range.cc
*/
ha_rows ha_eloq::records_in_range(uint inx, const key_range *min_key,
                                  const key_range *max_key, page_range *pages)
{
  DBUG_ENTER_FUNC();

  ha_rows rows= 0;

  THD *thd= ha_thd();
  MyEloqTx *my_tx= nullptr;
  bool tx_exists= false;
  if (!get_or_create_myeloq_tx(thd, &my_tx, &tx_exists))
  {
    my_error(HA_ERR_ELOQ_START_TRANSACTION_FAILED, MYF(0));
    DBUG_RETURN(HA_ERR_ELOQ_START_TRANSACTION_FAILED);
  }
  // table_schema_ is not available outside of transaction context.
  DBUG_ASSERT(tx_exists);

  EloqKey mono_min_key= MonoMinKeyFromKeyRange(inx, min_key);
  EloqKey mono_max_key= MonoMaxKeyFromKeyRange(
      inx, max_key, mono_min_key.PackedValueSlice().size());

  const Distribution *distribution= GetDistribution(inx);
  if (distribution)
  {
    rows= distribution->Records(KeySchema(inx), TxKey(&mono_min_key),
                                TxKey(&mono_max_key));
  }
  if (rows <= 0)
  {
    rows= 1;
  }

  DBUG_RETURN(rows);
}

const txservice::KeySchema *ha_eloq::KeySchema(uint inx) const
{
  const txservice::KeySchema *key_schema;

  const TABLE_SHARE *share= table_schema_->TableShare();
  if (inx == share->primary_key)
  {
    key_schema= table_schema_->KeySchema();
  }
  else
  {
    const auto &[index_name, index_schema]=
        table_schema_->IndexNameSchema(inx);
    key_schema= &index_schema;
  }

  return key_schema;
}

const Distribution *ha_eloq::GetDistribution(uint inx) const
{
  const TableName *table_or_index_name;

  const TABLE_SHARE *share= table_schema_->TableShare();
  if (inx == share->primary_key)
  {
    table_or_index_name= &base_table_name_;
  }
  else
  {
    const auto &[index_name, index_schema]=
        table_schema_->IndexNameSchema(inx);
    table_or_index_name= &index_name;
  }

  return table_schema_->StatisticsObject()->GetDistribution(
      *table_or_index_name);
}

bool ha_eloq::SuccessorForKeyRange(const key_range *range_key) const
{
  // Reference ha_rocksdb::records_in_range in rocksdb/ha_rocksdb.cc
  return (range_key->flag == HA_READ_PREFIX_LAST_OR_PREV ||
          range_key->flag == HA_READ_PREFIX_LAST ||
          range_key->flag == HA_READ_AFTER_KEY);
}

EloqKey ha_eloq::MonoMinKeyFromKeyRange(uint inx,
                                        const key_range *min_key) const
{
  const TABLE_SHARE *share= table_schema_->TableShare();
  if (inx == share->primary_key)
  {
    return MonoMinPKeyFromKeyRange(min_key);
  }
  else
  {
    return MonoMinSKeyFromKeyRange(inx, min_key);
  }
}

EloqKey ha_eloq::MonoMaxKeyFromKeyRange(uint inx, const key_range *max_key,
                                        size_t min_key_size) const
{
  const TABLE_SHARE *share= table_schema_->TableShare();
  if (inx == share->primary_key)
  {
    return MonoMaxPKeyFromKeyRange(max_key, min_key_size);
  }
  else
  {
    return MonoMaxSKeyFromKeyRange(inx, max_key, min_key_size);
  }
}

EloqKey ha_eloq::MonoMinPKeyFromKeyRange(const key_range *min_key) const
{
  if (min_key && min_key->length > 0)
  {
    size_t min_key_pack_size= pk_descr_->pack_index_tuple(
        table, pack_buffer_, pk_packed_tuple_, record_buffer_, min_key->key,
        min_key->keypart_map);

    if (SuccessorForKeyRange(min_key))
    {
      pk_descr_->successor(pk_packed_tuple_, min_key_pack_size);
    }

    EloqKey mono_min_key(pk_packed_tuple_, min_key_pack_size);
    return mono_min_key;
  }
  else
  {
    const EloqKey *key= EloqKey::PackedNegativeInfinity();
    EloqKey mono_min_key= *key;
    return mono_min_key;
  }
}

EloqKey ha_eloq::MonoMinSKeyFromKeyRange(uint inx,
                                         const key_range *min_key) const
{
  if (min_key && min_key->length > 0)
  {
    const auto &[index_name, index_schema]=
        table_schema_->IndexNameSchema(inx);
    const EloqKeySchema *sk_sch=
        static_cast<const EloqKeySchema *>(index_schema.sk_schema_.get());

    size_t min_key_pack_size= sk_sch->KeyDefinition()->pack_index_tuple(
        table, pack_buffer_, sk_packed_tuple_, record_buffer_, min_key->key,
        min_key->keypart_map);

    if (SuccessorForKeyRange(min_key))
    {
      sk_sch->KeyDefinition()->successor(sk_packed_tuple_, min_key_pack_size);
    }

    EloqKey mono_min_key(sk_packed_tuple_, min_key_pack_size);

    return mono_min_key;
  }
  else
  {
    const EloqKey *key= EloqKey::PackedNegativeInfinity();
    EloqKey mono_min_key= *key;
    return mono_min_key;
  }
}

EloqKey ha_eloq::MonoMaxPKeyFromKeyRange(const key_range *max_key,
                                         size_t min_key_size) const
{
  if (max_key && max_key->length > 0)
  {
    size_t max_key_pack_size= pk_descr_->pack_index_tuple(
        table, pack_buffer_, pk_packed_tuple_, record_buffer_, max_key->key,
        max_key->keypart_map);

    if (SuccessorForKeyRange(max_key))
    {
      pk_descr_->successor(pk_packed_tuple_, max_key_pack_size);
    }

    if (min_key_size > max_key_pack_size)
    {
      memset(pk_packed_tuple_ + max_key_pack_size, 0xFF,
             min_key_size - max_key_pack_size);
      max_key_pack_size= min_key_size;
    }

    EloqKey mono_max_key(pk_packed_tuple_, max_key_pack_size);
    return mono_max_key;
  }
  else
  {
    EloqKey mono_max_key=
        EloqKey::PackedPositiveInfinity(table_schema_->KeySchema());
    return mono_max_key;
  }
}

EloqKey ha_eloq::MonoMaxSKeyFromKeyRange(uint inx, const key_range *max_key,
                                         size_t min_key_size) const
{
  const auto &[index_name, index_schema]= table_schema_->IndexNameSchema(inx);
  const EloqKeySchema *sk_sch=
      static_cast<const EloqKeySchema *>(index_schema.sk_schema_.get());

  if (max_key && max_key->length > 0)
  {

    size_t max_key_pack_size= sk_sch->KeyDefinition()->pack_index_tuple(
        table, pack_buffer_, sk_packed_tuple_, record_buffer_, max_key->key,
        max_key->keypart_map);

    if (SuccessorForKeyRange(max_key))
    {
      sk_sch->KeyDefinition()->successor(sk_packed_tuple_, max_key_pack_size);
    }

    if (min_key_size > max_key_pack_size)
    {
      memset(sk_packed_tuple_ + max_key_pack_size, 0xFF,
             min_key_size - max_key_pack_size);
      max_key_pack_size= min_key_size;
    }

    EloqKey mono_max_key(sk_packed_tuple_, max_key_pack_size);
    return mono_max_key;
  }
  else
  {
    EloqKey mono_max_key= EloqKey::PackedPositiveInfinity(&index_schema);
    return mono_max_key;
  }
}

void ha_eloq::EncodeScanKey(EloqKey &mono_key, const uchar *mysql_key,
                            mysql::key_part_map keypart_map,
                            const mono_key_def *key_def)
{
  // The input mono_key can only be search_key_ or scan_end_key_, whose
  // internal buffer (key_buf) has been set to the maximal key size when the
  // handler is initialized. So, it is safe to write to key_buf.data() when
  // encoding the input MySQL key into the memory-comparable format.
  std::string &key_buf= mono_key.PackedValue();
  key_buf.resize(key_buf.capacity());

  size_t pack_size=
      key_def->pack_index_tuple(table, pack_buffer_, (uchar *) key_buf.data(),
                                record_buffer_, mysql_key, keypart_map);
  key_buf.resize(pack_size);
}

int ha_eloq::PrepareScan(const uchar *mysql_start_key,
                         mysql::key_part_map keypart_map,
                         enum ha_rkey_function search_flag,
                         mono_key_def *key_def, uint32_t key_part_cnt,
                         bool start_use_full_key, bool &start_inclusive,
                         bool &end_specified, bool &end_inclusive,
                         ScanDirection &direction)
{
  bool prefix_match= false;
  int rc=
      SetScanParameters(search_flag, direction, start_inclusive, prefix_match);

  if (rc != 0)
  {
    return rc;
  }

  EncodeScanKey(search_key_, mysql_start_key, keypart_map, key_def);

  if (prefix_match)
  {
// If the index scan is a prefix-match scan, keeps the start key in
// the handler so as to match it against every scanned record.
// Unmatched records are not returned to the upper execution engine.
#ifndef RANGE_PARTITION_ENABLED
    prefix_match_key_= std::make_unique<EloqKey>(search_key_);
#endif

    end_specified= true;
    end_inclusive= true;

    if (direction == ScanDirection::Forward)
    {
      // This is a prefix-match, forward scan. The scan's start key is the
      // input MySQL key (>=) and the end key is the ceiling of the input key
      // (<=), i.e., the input key padded with 0xFF.
      assert(start_inclusive);
      scan_end_key_= search_key_;
      key_def->get_ceiling_key(scan_end_key_);
    }
    else
    {
      // This is a prefix-match, backward scan. The scan's start key is the
      // ceiling of the input MySQL key (<=), i.e., the input key padded with
      // 0xFF. The scan's end key is the input MySQL key (>=).
      assert(start_inclusive);
      scan_end_key_= search_key_;
      key_def->get_ceiling_key(search_key_);
    }
  }
  else if (direction == ScanDirection::Forward)
  {
    if ((!start_use_full_key || active_index != table_share->primary_key) &&
        !start_inclusive)
    {
      // Sets the start key to be the ceiling of the input start key, if the
      // forward scan is k > v. For example if the full key is (10, a), and
      // the start key passed in is (10), we need to pad the remaining bytes
      // with max value so that we have a new start key (10, MAX_VAL).
      key_def->get_ceiling_key(search_key_);
    }

    int end_ret=
        PrepareForwardScanEndKey(key_def, key_part_cnt, end_inclusive);
    end_specified= end_ret == 0;
  }
  else
  {
    if ((!start_use_full_key || active_index != table_share->primary_key) &&
        start_inclusive)
    {
      // Sets the start key to be the ceiling of the input start key, if the
      // backward scan is k <= v. For example if the full key is (10, a), and
      // the start key passed in is (10), we need to pad the remaining bytes
      // with max value so that we have a new start key (10, MAX_VAL).
      key_def->get_ceiling_key(search_key_);
    }

    int end_ret=
        PrepareBackwardScanEndKey(key_def, key_part_cnt, end_inclusive);
    end_specified= end_ret == 0;
  }

  return 0;
}

int ha_eloq::PrepareForwardScanEndKey(mono_key_def *key_def,
                                      uint32_t key_part_cnt,
                                      bool &end_inclusive)
{
  if (end_range == nullptr)
  {
    return 1;
  }

  EncodeScanKey(scan_end_key_, end_range->key, end_range->keypart_map,
                key_def);

  // For forward scans, end_range specifies two possible flags,
  // HA_READ_BEFORE_KEY or HA_READ_AFTER_KEY. The former represents < and
  // the latter represents <=. For the latter, when not all key parts are
  // specified in the end key, the end key needs to be converted to the
  // input key's ceiling.
  if (end_range->flag == HA_READ_BEFORE_KEY)
  {
    end_inclusive= false;
  }
  else if (end_range->flag == HA_READ_AFTER_KEY)
  {
    end_inclusive= true;

    bool end_use_full_key=
        end_range->keypart_map == HA_WHOLE_KEY ||
        end_range->keypart_map == (key_part_map(1) << key_part_cnt) - 1;
    if (!end_use_full_key || active_index != table_share->primary_key)
    {
      key_def->get_ceiling_key(scan_end_key_);
    }
  }
  else
  {
    LOG(WARNING) << "Unrecognizied search flag when specifying the end key "
                    "of a forward scan, flag: "
                 << (int) end_range->flag;
    return 1;
  }

  return 0;
}

int ha_eloq::PrepareBackwardScanEndKey(mono_key_def *key_def,
                                       uint32_t key_part_cnt,
                                       bool &end_inclusive)
{
  if (start_range_ptr_ == nullptr)
  {
    return 1;
  }

  EncodeScanKey(scan_end_key_, start_range_ptr_->key,
                start_range_ptr_->keypart_map, key_def);

  // For backward scans, start_range_ptr_ specifies the end key. The end
  // key's possible flags are HA_READ_KEY_OR_NEXT or HA_READ_AFTER_KEY. The
  // former represents >= and the latter represents >. For the latter, when
  // not all key parts are specified in the end key, the end key needs to
  // be converted to the input key's ceiling.
  if (start_range_ptr_->flag == HA_READ_KEY_OR_NEXT)
  {
    end_inclusive= true;
  }
  else if (start_range_ptr_->flag == HA_READ_AFTER_KEY)
  {
    end_inclusive= false;

    bool end_use_full_key=
        start_range_ptr_->keypart_map == HA_WHOLE_KEY ||
        start_range_ptr_->keypart_map == (key_part_map(1) << key_part_cnt) - 1;
    if (!end_use_full_key || active_index != table_share->primary_key)
    {
      key_def->get_ceiling_key(scan_end_key_);
    }
  }
  else
  {
    LOG(INFO) << "Unrecognizied search flag when specifying the end key "
                 "of a backward scan, flag: "
              << (int) start_range_ptr_->flag;
    return 1;
  }

  return 0;
}

std::pair<std::unique_ptr<EloqKey>, std::unique_ptr<EloqRecord>>
ha_eloq::PackKeyRecord(const uchar *buf)
{
  std::unique_ptr<EloqRecord> mono_rec= PackRecord(buf);
  std::unique_ptr<EloqKey> mono_key= nullptr;

  if (!has_hidden_pk(table))
  {
    size_t pack_size= pk_descr_->pack_record(
        table, pack_buffer_, buf, pk_packed_tuple_, &unpack_info_, false);
    mono_key= std::make_unique<EloqKey>(pk_packed_tuple_, pack_size);
    mono_rec->SetUnpackInfo(unpack_info_.ptr(),
                            unpack_info_.get_current_pos());
  }
  else
  {
    // The table does not define a primary key. If this is an insert,
    // generates a uuid as the primary key.
    uchar uuid_buffer[MY_UUID_SIZE];
    mysql::my_uuid(uuid_buffer);
    mono_key= std::make_unique<EloqKey>(pk_descr_.get(), table, pack_buffer_,
                                        uuid_buffer, true);
  }

  return {std::move(mono_key), std::move(mono_rec)};
}

std::unique_ptr<EloqRecord> ha_eloq::PackRecord(const uchar *buf)
{
  std::unique_ptr<EloqRecord> mono_rec= std::make_unique<EloqRecord>();

  MY_BITMAP *old_map= dbug_tmp_use_all_columns(table, &table->read_set);
  const EloqRecordSchema &rec_schema= *GetRecordSchema();
  rec_schema.Encode(buf, table->field, table->record[0],
                    mono_rec->encoded_blob_);
  dbug_tmp_restore_column_map(&table->read_set, old_map);

  return mono_rec;
}

void ha_eloq::SetupDecodeFlagOnFirstRead()
{
  DBUG_ENTER_FUNC();
  if (need_setup_decode_flag_)
  {
    decode_flag_= 0u;
    const MY_BITMAP *decode_set= DecodeSet();
    for (mysql::Field **field_head= table->s->field; *field_head != nullptr;
         ++field_head)
    {
      Field *field= *field_head;
      if (!field->stored_in_db())
      {
        continue;
      }

      if (bitmap_is_set(decode_set, field->field_index))
      {
        if (!has_hidden_pk(table))
        {
          if (field->part_of_key.is_set(table->s->primary_key))
          {
            decode_flag_|= DECODE_PK;
            if (decode_flag_ & DECODE_PAYLOAD)
            {
              break;
            }
          }
          else
          {
            decode_flag_|= DECODE_PAYLOAD;
            if (decode_flag_ & DECODE_PK)
            {
              break;
            }
          }
        }
        else
        {
          decode_flag_|= DECODE_PAYLOAD;
          break;
        }
      }
    }

    need_setup_decode_flag_= false;
  }
  DBUG_VOID_RETURN;
}

const MY_BITMAP *ha_eloq::DecodeSet() const
{
  bool for_update= lock.type >= TL_WRITE_ALLOW_WRITE;
  if (for_update)
  {
    return &table->s->all_set;
  }
  else
  {
    return table->read_set;
  }
}

/*
 * For index columns we need to convert them into memory comparable binary
 * format, and they need to be revertable. We need to take those
 * non-revertable collations out of the supported set.
 */
static const std::set<uint> MONO_INDEX_COLLATIONS= {
    COLLATION_BINARY, COLLATION_UTF8_BIN, COLLATION_LATIN1_BIN,
    COLLATION_UTF8MB4_BIN};

static bool mono_is_index_collation_supported(const Field *const field)
{
  const enum_field_types type= field->real_type();
  /* Handle [VAR](CHAR|BINARY) or TEXT|BLOB */
  if (type == MYSQL_TYPE_VARCHAR || type == MYSQL_TYPE_STRING ||
      type == MYSQL_TYPE_BLOB)
  {
    const CHARSET_INFO *cs= field->charset();
    return (MONO_INDEX_COLLATIONS.find(cs->number) !=
            MONO_INDEX_COLLATIONS.end()) ||
           mono_key_def::mono_is_collation_supported(cs);
  }
  return true;
}

/**
  @brief
  create() is called to create a database. The variable name will have the
  name of the table.

  @details
  When create() is called you do not need to worry about
  opening the table. Also, the .frm file will have already been
  created so adjusting create_info is not necessary. You can overwrite
  the .frm file at this point if you wish to change the table
  definition, but there are no methods currently provided for doing
  so.

  Called from handle.cc by ha_create_table().

  @see
  ha_create_table() in handle.cc
*/

int ha_eloq::create(const char *name, TABLE *table_arg,
                    HA_CREATE_INFO *create_info)
{
  DBUG_ENTER_FUNC();

  THD *thd= ha_thd();

  // TODO: Whether should have flags HTON_CAN_RECREATE for truncate or not?
  enum_sql_command sqlcom= enum_sql_command(thd_sql_command(thd));
  if (sqlcom == SQLCOM_TRUNCATE)
  {
    my_error(ER_CHECK_NOT_IMPLEMENTED, MYF(0),
             eloq_unsupported_command.find(sqlcom)->second.c_str());
    DBUG_RETURN(HA_ERR_WRONG_COMMAND);
  }

  // tx_exists is set to false for new TransactionExecution
  MyEloqTx *my_tx= nullptr;
  bool tx_exists= false;
  if (!get_or_create_myeloq_tx(thd, &my_tx, &tx_exists))
  {
    my_error(HA_ERR_ELOQ_START_TRANSACTION_FAILED, MYF(0));
    DBUG_RETURN(HA_ERR_ELOQ_START_TRANSACTION_FAILED);
  }

  DBUG_ASSERT(my_tx != nullptr);
  TransactionExecution *txm= my_tx->Txm();

  std::string_view name_sv{name};
  txservice::TableName table_name{name_sv, txservice::TableType::Primary,
                                  txservice::TableEngine::EloqSql};

  my_tx->ClearSchemaReader(table_name);

  // Convert dbname into path-like encoding ./dbname.
  // Then Check whether db exists.
  std::string_view db=
      mariadb_to_monokey(&table_arg->s->mem_root, table_arg->s->db);
  std::string db_opt;
  bool db_exists= false;

  auto [yield_func, resume_func]= my_tx->CoroFunctors();
  if (!storage_hd->FetchDatabase(db, db_opt, db_exists, yield_func,
                                 resume_func))
  {
    DBUG_RETURN(HA_ERR_ELOQ_CREATE_TABLE_FAILED);
  }
  if (!db_exists)
  {
    my_error(ER_BAD_DB_ERROR, MYF(0), table_arg->s->db.str);
    DBUG_RETURN(HA_ERR_ELOQ_COMMIT_FAILED);
  }

  // The sql-engine call eloq_discover_table at first within the same
  // transaction. That interface put no lock for non-exists table.
  // Lock it again with write intent.
  CatalogKey catalog_key(table_name);
  CatalogRecord catalog_rec;
  bool exists= false;
  TxErrorCode err= my_tx->ReadCatalog(catalog_key, catalog_rec, true, exists);
  if (err != TxErrorCode::NO_ERROR)
  {
    char buf[1024];
    my_snprintf(buf, sizeof(buf), "Failed to read catalog of %s", name);
    DBUG_RETURN(convert_tx_error(err, buf));
  }

  if (exists)
  {
    if (thd->lex->create_info.if_not_exists())
    {
      DBUG_RETURN(0);
    }
    else
    {
      my_error(ER_TABLE_EXISTS_ERROR, MYF(0), name);
      DBUG_RETURN(HA_ERR_ELOQ_CREATE_TABLE_FAILED);
    }
  }

  // TODO: Add this check to create index when it's implemented.
  for (uint i= 0; i < table_arg->s->keys; i++)
  {
    for (uint part= 0; part < table_arg->s->key_info[i].user_defined_key_parts;
         part++)
    {
      if (!mono_is_index_collation_supported(
              table_arg->s->key_info[i].key_part[part].field))
      {
        char buf[1024];
        my_snprintf(
            buf, sizeof(buf),
            "Index column %s.%s uses a collation that is not "
            "currently supported by EloqDB."
            "Please use binary collation.",
            table_arg->s->table_name.str,
            table_arg->s->key_info[i].key_part[part].field->field_name.str);

        my_error(ER_INTERNAL_ERROR, MYF(0), buf);
        DBUG_RETURN(HA_ERR_ELOQ_CREATE_TABLE_FAILED);
      }
    }
  }

  LEX_CUSTRING *frm_image= table_arg->s->frm_image;
  std::string frm_str((const char *) frm_image->str, frm_image->length);
  std::string schema_image=
      EloqDS::SerializeSchemaImage(frm_str, std::string(""), std::string(""));

  MysqlTableSchema temp_schema(table_name, schema_image, 0);
  std::string kv_info= storage_hd->CreateKVCatalogInfo(&temp_schema);

  std::string empty_image{""};
  std::string new_image=
      EloqDS::SerializeSchemaImage(frm_str, kv_info, std::string(""));

  UpsertTableTxRequest upsert_table_req(&table_name, &empty_image, 1,
                                        &new_image, OperationType::CreateTable,
                                        nullptr, yield_func, resume_func, txm);
  txm->Execute(&upsert_table_req);
  upsert_table_req.Wait();
  UpsertResult rst= upsert_table_req.Result();

  if (rst == UpsertResult::Failed)
  {
    // UpsertTableOp on multiple nodes at the same time may conflict and then
    // backoff.
    my_error(HA_ERR_ELOQ_CREATE_TABLE_FAILED, MYF(0),
             upsert_table_req.ErrorMsg().data());
    DBUG_RETURN(HA_ERR_ELOQ_CREATE_TABLE_FAILED);
  }
  else if (rst == UpsertResult::Unverified)
  {
    my_error(HA_ERR_ELOQ_TRANSACTION_BREAK, MYF(ME_WARNING),
             "Current transaction coordinator is no longer the leader node. "
             "The alter table statement will be processed in a failover node."
             " Please recheck the result of alter table statement later.");
  }

  DBUG_RETURN(0);
}

/* Returns index of primary key */
uint ha_eloq::pk_index(const TABLE *const table_arg)
{
  DBUG_ASSERT(table_arg->s != nullptr);

  return table_arg->s->primary_key == MAX_INDEXES ? table_arg->s->keys
                                                  : table_arg->s->primary_key;
}

bool ha_eloq::has_hidden_pk(const TABLE *const table) const
{
  return table->s->primary_key == MAX_INDEXES;
}

int ha_eloq::alloc_key_buffers(const TABLE *const table_arg)
{
  DBUG_ENTER_FUNC();

  const std::unordered_map<
      uint16_t, std::pair<txservice::TableName, txservice::SecondaryKeySchema>>
      *kd_arr= table_schema_->GetIndexes();

  const std::unordered_map<
      uint16_t, std::pair<txservice::TableName, txservice::SecondaryKeySchema>>
      *dirty_kd_arr= nullptr;
  if (table_dirty_schema_ != nullptr)
  {
    dirty_kd_arr= table_dirty_schema_->GetIndexes();
  }

  uint max_packed_sk_len= 0;
  uint pack_key_len= 0;
  uint record_len= table->s->reclength + table->s->null_bytes;

  pk_descr_= this->GetKeySchema()->KeyDefinition();

  pack_key_len= pk_descr_->max_storage_fmt_length();
  pk_packed_tuple_= reinterpret_cast<uchar *>(
      my_malloc(PSI_INSTRUMENT_ME, pack_key_len, MYF(0)));

  /* Sometimes, we may use m_sk_packed_tuple for storing packed PK */
  max_packed_sk_len= pack_key_len;
  for (auto it= kd_arr->cbegin(); it != kd_arr->cend(); it++)
  {
    const EloqKeySchema *sk_schema=
        static_cast<const EloqKeySchema *>(it->second.second.sk_schema_.get());
    const uint packed_len=
        sk_schema->KeyDefinition()->max_storage_fmt_length();
    if (packed_len > max_packed_sk_len)
    {
      max_packed_sk_len= packed_len;
    }
  }

  if (dirty_kd_arr != nullptr)
  {
    for (auto dirty_it= dirty_kd_arr->cbegin();
         dirty_it != dirty_kd_arr->cend(); ++dirty_it)
    {
      const EloqKeySchema *dirty_sk_schema= static_cast<const EloqKeySchema *>(
          dirty_it->second.second.sk_schema_.get());
      const uint packed_len=
          dirty_sk_schema->KeyDefinition()->max_storage_fmt_length();
      if (packed_len > max_packed_sk_len)
      {
        max_packed_sk_len= packed_len;
      }
    }
  }

  sk_packed_tuple_= reinterpret_cast<uchar *>(
      my_malloc(PSI_INSTRUMENT_ME, max_packed_sk_len, MYF(0)));
  pack_buffer_= reinterpret_cast<uchar *>(
      my_malloc(PSI_INSTRUMENT_ME, max_packed_sk_len, MYF(0)));
  record_buffer_= reinterpret_cast<uchar *>(
      my_malloc(PSI_INSTRUMENT_ME, record_len, MYF(0)));

  if (pk_packed_tuple_ == nullptr || sk_packed_tuple_ == nullptr ||
      pack_buffer_ == nullptr || record_buffer_ == nullptr)
  {
    // One or more of the above allocations failed.  Clean up and exit
    free_key_buffers();

    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }

  uint32_t max_key_capacity= std::max(pack_key_len, max_packed_sk_len);
  if (max_key_capacity > 0)
  {
    search_key_.PackedValue().reserve(max_key_capacity);
    scan_end_key_.PackedValue().reserve(max_key_capacity);
  }

  DBUG_RETURN(0);
}

void ha_eloq::free_key_buffers()
{
  if (pk_packed_tuple_ != nullptr)
  {
    my_free(pk_packed_tuple_);
    pk_packed_tuple_= nullptr;
  }

  if (sk_packed_tuple_ != nullptr)
  {
    my_free(sk_packed_tuple_);
    sk_packed_tuple_= nullptr;
  }

  if (pack_buffer_ != nullptr)
  {
    my_free(pack_buffer_);
    pack_buffer_= nullptr;
  }

  if (record_buffer_ != nullptr)
  {
    my_free(record_buffer_);
    record_buffer_= nullptr;
  }
}

/**
 * @brief Get AlterTableInfo object from ha Alter_inplace_info Object.
 * Note: out of this function, index kv name is empty.
 *
 * @param table_name
 * @param ha_alter_info
 * @param alter_table_info
 * @return AlterTableInfo object
 */
static void get_altered_table_info(const txservice::TableName &table_name,
                                   const Alter_inplace_info *ha_alter_info,
                                   txservice::AlterTableInfo &alter_table_info)
{
  alter_table_info.index_add_count_= ha_alter_info->index_add_count;
  alter_table_info.index_drop_count_= ha_alter_info->index_drop_count;
  // get add index
  for (uint8_t count= 0; count < alter_table_info.index_add_count_; count++)
  {
    uint8_t index_index= (uint8_t) ha_alter_info->index_add_buffer[count];
    bool is_unique_sk=
        ha_alter_info->key_info_buffer[index_index].flags & HA_NOSAME;

    // format: <base_table_name><INDEX_NAME_PREFIX><index_name>
    std::string format_index_name(table_name.String());
    format_index_name
        .append(is_unique_sk ? txservice::UNIQUE_INDEX_NAME_PREFIX
                             : txservice::INDEX_NAME_PREFIX)
        .append(ha_alter_info->key_info_buffer[index_index].name.str);
    txservice::TableName add_index_name(
        format_index_name.c_str(), format_index_name.length(),
        is_unique_sk ? txservice::TableType::UniqueSecondary
                     : txservice::TableType::Secondary,
        table_name.Engine());
    alter_table_info.index_add_names_.try_emplace(add_index_name);
  }
  assert(alter_table_info.index_add_names_.size() ==
         alter_table_info.index_add_count_);
  // get drop index
  for (uint8_t count= 0; count < alter_table_info.index_drop_count_; count++)
  {
    // format: <base_table_name><INDEX_NAME_PREFIX><index_name>
    bool is_unique_sk=
        ha_alter_info->index_drop_buffer[count]->flags & HA_NOSAME;
    std::string format_index_name(table_name.String());
    format_index_name
        .append(is_unique_sk ? txservice::UNIQUE_INDEX_NAME_PREFIX
                             : txservice::INDEX_NAME_PREFIX)
        .append(ha_alter_info->index_drop_buffer[count]->name.str);
    txservice::TableName drop_index_name(
        format_index_name.c_str(), format_index_name.length(),
        is_unique_sk ? txservice::TableType::UniqueSecondary
                     : txservice::TableType::Secondary,
        table_name.Engine());
    alter_table_info.index_drop_names_.try_emplace(drop_index_name);
  }
  assert(alter_table_info.index_drop_names_.size() ==
         alter_table_info.index_drop_count_);
}

/**
  check_if_supported_inplace_alter() is used to ask the engine whether
  it can execute this ALTER TABLE statement in place or the server needs to
  create a new table and copy th data over.

  The engine may answer that the inplace alter is not supported or,
  if supported, whether the server should protect the table from concurrent
  accesses. Return values are

    HA_ALTER_INPLACE_NOT_SUPPORTED
    HA_ALTER_INPLACE_EXCLUSIVE_LOCK
    HA_ALTER_INPLACE_SHARED_LOCK
    etc
*/
enum_alter_inplace_result
ha_eloq::check_if_supported_inplace_alter(TABLE *altered_table,
                                          Alter_inplace_info *ha_alter_info)
{
  HA_CREATE_INFO *info= ha_alter_info->create_info;
  DBUG_ENTER("ha_eloq::check_if_supported_inplace_alter");

  {
    THD *thd= ha_thd();
    enum_sql_command sqlcom= enum_sql_command(thd_sql_command(thd));
    switch (sqlcom)
    {
    case SQLCOM_ALTER_TABLE:
      if (!(ha_alter_info->handler_flags &
            (ALTER_OPTIONS | ALTER_CHANGE_COLUMN_DEFAULT)))
      {
        my_error(ER_CHECK_NOT_IMPLEMENTED, MYF(0),
                 eloq_unsupported_command.find(sqlcom)->second.c_str());
        DBUG_RETURN(HA_ALTER_ERROR);
      }
      break;
#ifndef RANGE_PARTITION_ENABLED
    case SQLCOM_CREATE_INDEX:
      my_error(ER_CHECK_NOT_IMPLEMENTED, MYF(0),
               "CREATE INDEX on HASH partition");
      DBUG_RETURN(HA_ALTER_ERROR);
      break;
#endif
    default:
      break;
    }
  }

  /* TODO:We don't support unique keys on table */
  if ((ha_alter_info->handler_flags & ALTER_ADD_UNIQUE_INDEX))
  {
    // Disable unique key check currently to use juicefs.
    // my_error(ER_CHECK_NOT_IMPLEMENTED, MYF(0), "UNIQUE KEY");
    // DBUG_RETURN(HA_ALTER_INPLACE_NOT_SUPPORTED);
  }

  if (ha_alter_info->handler_flags & ALTER_CHANGE_CREATE_OPTION)
  {
    /*
      This example shows how custom engine specific table and field
      options can be accessed from this function to be compared.
    */
    ha_table_option_struct *param_new= info->option_struct;
    ha_table_option_struct *param_old= table->s->option_struct;

    /*
      check important parameters:
      for this example engine, we'll assume that changing ullparam or
      boolparam requires a table to be rebuilt, while changing strparam
      or enumparam - does not.

      For debugging purposes we'll announce this to the user
      (don't do it in production!)

    */
    if (param_new->ullparam != param_old->ullparam)
    {
      push_warning_printf(ha_thd(), Sql_condition::WARN_LEVEL_NOTE,
                          ER_UNKNOWN_ERROR, "ELOQ DEBUG: ULL %llu -> %llu",
                          param_old->ullparam, param_new->ullparam);
      DBUG_RETURN(HA_ALTER_INPLACE_NOT_SUPPORTED);
    }

    if (param_new->boolparam != param_old->boolparam)
    {
      push_warning_printf(ha_thd(), Sql_condition::WARN_LEVEL_NOTE,
                          ER_UNKNOWN_ERROR, "ELOQ DEBUG: YESNO %u -> %u",
                          param_old->boolparam, param_new->boolparam);
      DBUG_RETURN(HA_ALTER_INPLACE_NOT_SUPPORTED);
    }
  }

  if (ha_alter_info->handler_flags & ALTER_COLUMN_OPTION)
  {
    for (uint i= 0; i < table->s->fields; i++)
    {
      ha_field_option_struct *f_old= table->s->field[i]->option_struct;
      ha_field_option_struct *f_new= info->fields_option_struct[i];
      DBUG_ASSERT(f_old);
      if (f_new)
      {
        push_warning_printf(ha_thd(), Sql_condition::WARN_LEVEL_NOTE,
                            ER_UNKNOWN_ERROR,
                            "EXAMPLE DEBUG: Field %`s COMPLEX '%s' -> '%s'",
                            table->s->field[i]->field_name.str,
                            f_old->complex_param_to_parse_it_in_engine,
                            f_new->complex_param_to_parse_it_in_engine);
      }
      else
        DBUG_PRINT("info", ("old field %i did not changed", i));
    }
  }

  DBUG_RETURN(HA_ALTER_INPLACE_EXCLUSIVE_LOCK);
}

/**
  Allows the storage engine to update with concurrent
  writes blocked. If check_if_supported_inplace_alter() returns
  HA_ALTER_INPLACE_COPY_NO_LOCK or HA_ALTER_INPLACE_COPY_LOCK,
  this function is called with exclusive lock otherwise the same level
  of locking as for inplace_alter_table() will be used.

  @note Storage engines are responsible for reporting any errors by
  calling my_error()/print_error()

  @note If this function reports error, commit_inplace_alter_table()
  will be called with commit= false.

  @note For partitioning, failing to prepare one partition, means that
  commit_inplace_alter_table() will be called to roll back changes for
  all partitions. This means that commit_inplace_alter_table() might be
  called without prepare_inplace_alter_table() having been called first
  for a given partition.

  @param    altered_table     TABLE object for new version of table.
  @param    ha_alter_info     Structure describing changes to be done
                              by ALTER TABLE and holding data used
                              during in-place alter.

  @retval   true              Error
  @retval   false             Success
*/
bool ha_eloq::prepare_inplace_alter_table(TABLE *altered_table,
                                          Alter_inplace_info *ha_alter_info)
{
  DBUG_ENTER("ha_eloq::prepare_inplace_alter_table");

  // Check if schema has unsupported collations
  for (uint i= 0; i < altered_table->s->keys; i++)
  {
    for (uint part= 0;
         part < altered_table->s->key_info[i].user_defined_key_parts; part++)
    {
      if (!mono_is_index_collation_supported(
              altered_table->s->key_info[i].key_part[part].field))
      {
        char buf[1024];
        my_snprintf(buf, sizeof(buf),
                    "Index column %s.%s uses a collation that is not "
                    "currently supported by EloqDB. "
                    "Please use binary collation.",
                    altered_table->s->table_name.str,
                    altered_table->s->key_info[i]
                        .key_part[part]
                        .field->field_name.str);

        my_error(ER_INTERNAL_ERROR, MYF(0), buf);
        DBUG_RETURN(true);
      }
    }
  }

  DBUG_RETURN(false);
}

/**
  Alter the table structure in-place with operations
  specified using HA_ALTER_FLAGS and Alter_inplace_information.
  The level of concurrency allowed during this operation depends
  on the return value from check_if_supported_inplace_alter().

  @param altered_table TABLE object for new version of table.
  @param ha_alter_info Structure describing changes to be done
  by ALTER TABLE and holding data used during in-place alter.

  @retval true Failure
  @retval false Success
*/
bool ha_eloq::inplace_alter_table(TABLE *altered_table,
                                  Alter_inplace_info *ha_alter_info)
{
  DBUG_ENTER("ha_eloq::inplace_alter_table");

  THD *thd= ha_thd();

  // tx_exists is set to false for new TransactionExecution
  MyEloqTx *my_tx= nullptr;
  bool tx_exists= false;
  if (!get_or_create_myeloq_tx(thd, &my_tx, &tx_exists))
  {
    my_error(HA_ERR_ELOQ_START_TRANSACTION_FAILED, MYF(0));
    DBUG_RETURN(HA_ERR_ELOQ_START_TRANSACTION_FAILED);
  }

  DBUG_ASSERT(my_tx != nullptr);
  TransactionExecution *txm= my_tx->Txm();

  // read current catalog image
  TABLE_SHARE *table_share= altered_table->s;
  std::string table_name_path;
  table_name_path.append("./")
      .append(table_share->db.str, table_share->db.length)
      .append("/")
      .append(table_share->table_name.str, table_share->table_name.length);
  txservice::TableName table_name(
      table_name_path.c_str(), table_name_path.length(),
      txservice::TableType::Primary, txservice::TableEngine::EloqSql);
  my_tx->ClearSchemaReader(table_name);
  CatalogKey catalog_key(table_name);
  CatalogRecord catalog_rec;

  bool exists= false;
  // set "for_write=true" to avoid potential deadlock.
  TxErrorCode err= my_tx->ReadCatalog(catalog_key, catalog_rec, true, exists);
  if (err != TxErrorCode::NO_ERROR)
  {
    char buf[1024];
    my_snprintf(buf, sizeof(buf), "Failed to read catalog of %s",
                table_name_path.c_str());
    DBUG_RETURN(convert_tx_error(err, buf));
  }

  if (!exists)
  {
    my_error(HA_ERR_NO_SUCH_TABLE, MYF(0), table_name_path.c_str());
    DBUG_RETURN(true);
  }

  /**
   * Generate new catalog image.
   * Using new table frm, current kvtablename, add new kvindexnames,
   * delete dropped kvindexnames.
   */
  // 1. Generate new frm string.
  std::string new_frm_str((const char *) table->s->frm_image->str,
                          table->s->frm_image->length);
  // 2. Get altered table info whose index kv name is empty.
  txservice::AlterTableInfo alter_table_info;
  get_altered_table_info(table_name, ha_alter_info, alter_table_info);

  const MysqlTableSchema *current_table_schema=
      static_cast<const MysqlTableSchema *>(catalog_rec.Schema());

  // Get current key schemas ts, excluding the key to be dropped.
  TableKeySchemaTs key_schemas_ts(TableEngine::EloqSql);
  // The pk schema ts.
  key_schemas_ts.pk_schema_ts_= current_table_schema->KeySchema()->SchemaTs();
  auto sk_schemas= current_table_schema->GetIndexes();
  for (auto index_it= sk_schemas->begin(); index_it != sk_schemas->end();
       ++index_it)
  {
    if (alter_table_info.index_drop_names_.find(index_it->second.first) !=
        alter_table_info.index_drop_names_.end())
    {
      // This index will be dropped in the new table schema, so there is no
      // need to get its key schema ts.
      continue;
    }
    // The old sk schema ts.
    key_schemas_ts.sk_schemas_ts_.emplace(
        std::piecewise_construct,
        std::forward_as_tuple(index_it->second.first.StringView(),
                              index_it->second.first.Type(),
                              index_it->second.first.Engine()),
        std::forward_as_tuple(index_it->second.second.SchemaTs()));
  }
  std::string schemas_ts_str= key_schemas_ts.Serialize();

  // 3. Generate new schema kv info and altered table info whose index
  // kv name is not empty.
  std::string new_kv_info= storage_hd->CreateNewKVCatalogInfo(
      table_name, current_table_schema, alter_table_info);

  // 4. Serialized altered table info.
  std::string alter_table_info_image=
      alter_table_info.SerializeAlteredTableInfo();

  // 5. Generate new schema image string.
  // NOTE: At this stage, the key schema ts of the new index are unknown, the
  // value of which is the `commit_ts_` of the UpsertTable Transaction. So,
  // there is no new key's schema ts in the `schemas_ts_str`.
  std::string new_schema_image=
      EloqDS::SerializeSchemaImage(new_frm_str, new_kv_info, schemas_ts_str);

  OperationType op_type= OperationType::AddIndex;
  if (ha_alter_info->handler_flags & ALTER_ADD_NON_UNIQUE_NON_PRIM_INDEX ||
      ha_alter_info->handler_flags & ALTER_ADD_UNIQUE_INDEX ||
      ha_alter_info->handler_flags & ALTER_ADD_PK_INDEX)
  {
    op_type= OperationType::AddIndex;
  }
  else if (ha_alter_info->handler_flags &
               ALTER_DROP_NON_UNIQUE_NON_PRIM_INDEX ||
           ha_alter_info->handler_flags & ALTER_DROP_UNIQUE_INDEX ||
           ha_alter_info->handler_flags & ALTER_DROP_PK_INDEX)
  {
    op_type= OperationType::DropIndex;
  }
  else if (ha_alter_info->handler_flags & ALTER_OPTIONS ||
           ha_alter_info->handler_flags & ALTER_CHANGE_COLUMN_DEFAULT)
  {
    op_type= OperationType::Update; // Update table catalog definition merely.
  }
  else
  {
    my_error(HA_ERR_ELOQ_CREATE_INDEX_FAILED, MYF(0),
             "The storage engine doesn't support this ALTER sql command");
    DBUG_RETURN(true);
  }

  auto [yield_func, resume_func]= my_tx->CoroFunctors();

  // Send upsert table tx request.
  UpsertTableTxRequest upsert_table_req(
      &table_name, &catalog_rec.Schema()->SchemaImage(),
      catalog_rec.SchemaTs(), &new_schema_image, op_type,
      &alter_table_info_image, yield_func, resume_func, txm);
  txm->Execute(&upsert_table_req);
  upsert_table_req.Wait();

  UpsertResult rst= upsert_table_req.Result();
  if (rst == UpsertResult::Failed)
  {
    my_error(HA_ERR_ELOQ_CREATE_INDEX_FAILED, MYF(0),
             upsert_table_req.ErrorMsg().data());
    DBUG_RETURN(true);
  }
  else if (rst == UpsertResult::Unverified)
  {
    my_error(HA_ERR_ELOQ_TRANSACTION_BREAK, MYF(ME_NOTE),
             "Current transaction coordinator is no longer the leader node. "
             "The alter table statement will be processed in a failover node."
             " Please recheck the result of alter table statement later.");
  }

  DBUG_RETURN(false);
}

/**
  Commit or rollback the changes made during
  prepare_inplace_alter_table() and inplace_alter_table() inside
  the storage engine. Note that the allowed level of concurrency
  during this operation will be the same as for
  inplace_alter_table() and thus might be higher than during
  prepare_inplace_alter_table(). (E.g concurrent writes were
  blocked during prepare, but might not be during commit).

  @param altered_table TABLE object for new version of table.
  @param ha_alter_info Structure describing changes to be done
  by ALTER TABLE and holding data used during in-place alter.
  @param commit true => Commit, false => Rollback.

  @retval true Failure
  @retval false Success
*/
bool ha_eloq::commit_inplace_alter_table(TABLE *altered_table,
                                         Alter_inplace_info *ha_alter_info,
                                         bool commit)
{
  DBUG_ENTER("ha_eloq::commit_inplace_alter_table");
  /* Nothing to commit/rollback, mark all handlers committed! */
  ha_alter_info->group_commit_ctx= NULL;
  DBUG_RETURN(false);
}

/** Push a primary key filter.
@param[in]	pk_filter	filter against which primary keys
                                are to be checked
@retval	false if pushed (always) */
bool ha_eloq::rowid_filter_push(Rowid_filter *rowid_filter)
{
  DBUG_ENTER_FUNC();
  pushed_rowid_filter= rowid_filter;
  DBUG_RETURN(false);
}

Item *ha_eloq::idx_cond_push(uint keyno, Item *idx_cond)
{
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(keyno != MAX_KEY);
  DBUG_ASSERT(idx_cond != nullptr);

  pushed_idx_cond= idx_cond;
  pushed_idx_cond_keyno= keyno;
  // in_range_check_pushed_down= TRUE;

  /* We will check the whole condition */
  DBUG_RETURN(nullptr);
}

int ha_eloq::reset()
{
  DBUG_ENTER_FUNC();
  DBUG_ASSERT(!IndexScanIsOpen());
  pushed_conds_.clear();
  m_ds_mrr_.dsmrr_close();
  DBUG_RETURN(0);
}

// discover_check_version is used to check whether the cached table_share
// is expired note that the first call to table_open will not trigger
// discover_check_version
int ha_eloq::discover_check_version()
{
  DBUG_ENTER_FUNC();

  LEX_CUSTRING *tabledef_version= &table->s->tabledef_version;
  std::string_view source_version(
      reinterpret_cast<const char *>(tabledef_version->str),
      tabledef_version->length);

  THD *thd= ha_thd();
  {
    enum_sql_command sqlcom= enum_sql_command(thd_sql_command(thd));
    switch (sqlcom)
    {
    case SQLCOM_ALTER_TABLE:
    case SQLCOM_LOCK_TABLES:
    case SQLCOM_UNLOCK_TABLES:
    case SQLCOM_TRUNCATE:
    case SQLCOM_RENAME_TABLE:
      my_error(ER_CHECK_NOT_IMPLEMENTED, MYF(0),
               eloq_unsupported_command.find(sqlcom)->second.c_str());
      DBUG_RETURN(HA_ERR_WRONG_COMMAND);
    default:
      break;
    }
  }

  MyEloqTx *my_tx= nullptr;
  bool tx_exists= false;
  if (!get_or_create_myeloq_tx(thd, &my_tx, &tx_exists))
  {
    my_error(HA_ERR_ELOQ_START_TRANSACTION_FAILED, MYF(0));
    DBUG_RETURN(HA_ERR_ELOQ_START_TRANSACTION_FAILED);
  }

  // If the session has read the table's schema, the session's tx must have
  // put a read lock on the table's schema. No one else could modify the
  // schema. Table version check is performed against the schema cached in
  // the session's tx. If the session has no cached schema, reads the table's
  // schema from the tx service via the session's tx.
  const MysqlTableSchema *session_schema= DiscoverTableSchema(my_tx);

  int error_code= 0;
  if (session_schema == nullptr ||
      session_schema->VersionStringView() != source_version)
  {
    error_code= HA_ERR_TABLE_DEF_CHANGED;
  }

  DBUG_RETURN(error_code);
}

/**
 * @brief Notify other node to reload the acl cache.
 * Note that notify_reload_acl_and_cache is used by Mariadb layer
 */
extern "C" int notify_reload_acl_and_cache(THD *thd)
{
  DBUG_ENTER_FUNC();
  MyEloqTx *my_tx= nullptr;
  bool tx_exists= false;
  if (!get_or_create_myeloq_tx(thd, &my_tx, &tx_exists))
  {
    my_error(HA_ERR_ELOQ_START_TRANSACTION_FAILED, MYF(0));
    DBUG_RETURN(1);
  }

  TransactionExecution *txm= my_tx->Txm();
  auto [yield_func, resume_func]= my_tx->CoroFunctors();

  ReloadCacheTxRequest reload_tx_req(yield_func, resume_func, txm);
  txm->Execute(&reload_tx_req);
  reload_tx_req.Wait();
  if (reload_tx_req.IsError())
  {
    sql_print_warning("eloq reload acl and cache on all node groups error, %s",
                      reload_tx_req.ErrorMsg().c_str());
    DBUG_RETURN(1);
  }
  else
  {
    DBUG_RETURN(0);
  }
}

static int
invalidate_table_cache(MYSQL_THD thd,
                       const std::vector<TableName> &invalidate_tables)
{
  DBUG_ENTER_FUNC();

  int ret= 0;

  MyEloqTx *my_tx= nullptr;
  bool tx_exists= false;
  if (!get_or_create_myeloq_tx(thd, &my_tx, &tx_exists))
  {
    my_error(HA_ERR_ELOQ_START_TRANSACTION_FAILED, MYF(0));
    DBUG_RETURN(1);
  }

  TransactionExecution *txm= my_tx->Txm();
  auto [yield_func, resume_func]= my_tx->CoroFunctors();

  for (const TableName &table_name : invalidate_tables)
  {
    CatalogKey table_key(table_name);
    CatalogRecord catalog_rec;
    bool exists= false;
    TxErrorCode err= my_tx->ReadCatalog(table_key, catalog_rec, true, exists);
    if (err != TxErrorCode::NO_ERROR)
    {
      sql_print_warning("Read catalog error, table %s",
                        table_name.StringView().data());
      my_error(HA_ERR_ELOQ_READ_ERROR, MYF(0));
      ret= 1;
      break;
    }
    if (!exists)
    {
      sql_print_information("Invalidate table cache for non-exist table %s",
                            table_name.StringView().data());
      my_error(HA_ERR_ELOQ_CATALOG_NAME_ERROR, MYF(0));
      ret= 1;
      break;
    }
  }

  if (ret != 0)
  {
    my_tx->Abort();
    DBUG_RETURN(ret);
  }

  for (const TableName &table_name : invalidate_tables)
  {
    InvalidateTableCacheTxRequest req(&table_name, yield_func, resume_func,
                                      txm);
    txm->Execute(&req);
    req.Wait();
    if (req.IsError())
    {
      sql_print_warning("Invalidate table cache failed, table %s",
                        table_name.StringView().data());
      my_error(HA_ERR_ELOQ_TRANSACTION_BREAK, MYF(0));
      ret= 1;
      break;
    }
  }

  if (ret != 0)
  {
    my_tx->Abort();
  }
  else
  {
    my_tx->Commit();
  }
  DBUG_RETURN(ret);
}

bool ha_eloq::get_error_message(const int error, String *const buf)
{
  DBUG_ENTER_FUNC();

  static_assert(HA_ERR_ELOQ_LAST > HA_ERR_FIRST,
                "HA_ERR_ELOQ_LAST > HA_ERR_FIRST");
  static_assert(HA_ERR_ELOQ_LAST > HA_ERR_LAST,
                "HA_ERR_ELOQ_LAST > HA_ERR_LAST");

  // If error message can be obtained from TxService side, use it as
  // detailed message. Else use the default error message of error code.
  if (error == HA_ERR_ELOQ_COMMIT_FAILED ||
      error == HA_ERR_ELOQ_DROP_TABLE_FAILED ||
      error == HA_ERR_ELOQ_CREATE_TABLE_FAILED)
  {
    THD *thd= ha_thd();
    MyEloqTx *my_tx= get_myeloq_tx(thd);

    if (my_tx && my_tx->tx_err_code_ != TxErrorCode::NO_ERROR)
    {
      const std::string &err_str= TxErrorMessage(my_tx->tx_err_code_);

      buf->append(err_str.c_str(), err_str.length());
      DBUG_RETURN(false);
    }
  }

  // default error message based on error code.
  if (error >= HA_ERR_ELOQ_FIRST && error <= HA_ERR_ELOQ_LAST)
  {
    const char *msg= eloq_error_messages[error - HA_ERR_ELOQ_FIRST];
    buf->append(msg, strlen(msg));
  }

  DBUG_RETURN(false);
}

void ha_eloq::decrement_statistics(ulong SSV::*offset) const
{
  handler::decrement_statistics(offset);
  DBUG_ASSERT(table->in_use->accessed_rows_and_keys > 0);
  table->in_use->accessed_rows_and_keys--;
}

// When a table has auto increment field, handler will call this method to
// get next integer value. In this case, offset will not be care, but
// increment will be used for compatibility with MySQL and it come from
// system variable instead of the value in sequences table. Due to here is
// distribute system, increment is only valid in a range. This method only
// can get a series values in a range. If need to cross range. It will only
// get values in this range and here need to get more next time.
void ha_eloq::get_auto_increment(ulonglong offset, ulonglong increment,
                                 ulonglong nb_desired_values,
                                 ulonglong *first_value,
                                 ulonglong *nb_reserved_values)
{
  THD *thd= ha_thd();
  MyEloqTx *my_tx= get_myeloq_tx(thd);
  DBUG_ASSERT(my_tx != nullptr);

  const MysqlTableSchema *session_schema= DiscoverTableSchema(my_tx);

  int64_t nb_res= 0;
  int64_t val= txservice::Sequences::ApplyIdOfAutoIncrColumn(
      *GetBaseTableNameFromTableSchema(), increment, nb_desired_values, nb_res,
      session_schema->KeySchema()->SchemaTs(), my_tx->CoroFunctors(),
      thd_get_long_resume_func(thd), thd_get_group_id(thd));
  if (nb_res == 0)
  {
    // If *first_value==ULONGLONG_MAX, it means that is failed to get auto
    // id and upper method handler::update_auto_increment will return error
    // HA_ERR_AUTOINC_READ_FAILED
    *first_value= ULONGLONG_MAX;
    *nb_reserved_values= 0;
  }
  else
  {
    *first_value= val;
    *nb_reserved_values= nb_res;
  }
}

int ha_eloq::multi_range_read_init(RANGE_SEQ_IF *seq, void *seq_init_param,
                                   uint n_ranges, uint mode,
                                   HANDLER_BUFFER *buf)
{
  need_setup_decode_flag_= true;

  /** Here will according to mode to set is_mrr_sort_rowid_, and it will used
   * in SkIndexScanNext and position to decide it actions. */
  is_mrr_sort_rowid_= (mode & DSMRR_IMPL_SORT_ROWIDS) &&
                      !(mode & (HA_MRR_USE_DEFAULT_IMPL | HA_MRR_SORTED));
  return m_ds_mrr_.dsmrr_init(this, seq, seq_init_param, n_ranges, mode, buf);
}

int ha_eloq::multi_range_read_next(range_id_t *range_info)
{
  SetupDecodeFlagOnFirstRead();
  return (m_ds_mrr_.dsmrr_next(range_info));
}

ha_rows ha_eloq::multi_range_read_info_const(uint keyno, RANGE_SEQ_IF *seq,
                                             void *seq_init_param,
                                             uint n_ranges, uint *bufsz,
                                             uint *flags, Cost_estimate *cost)
{
  /* See comments in ha_myisam::multi_range_read_info_const */
  m_ds_mrr_.init(this, table);

  ha_rows res= m_ds_mrr_.dsmrr_info_const(keyno, seq, seq_init_param, n_ranges,
                                          bufsz, flags, cost);
  is_mrr_sort_rowid_= false;
  return res;
}

ha_rows ha_eloq::multi_range_read_info(uint keyno, uint n_ranges, uint keys,
                                       uint key_parts, uint *bufsz,
                                       uint *flags, Cost_estimate *cost)
{
  m_ds_mrr_.init(this, table);
  ha_rows res= m_ds_mrr_.dsmrr_info(keyno, n_ranges, keys, key_parts, bufsz,
                                    flags, cost);
  return res;
}

int ha_eloq::multi_range_read_explain_info(uint mrr_mode, char *str,
                                           size_t size)
{
  return m_ds_mrr_.dsmrr_explain_info(mrr_mode, str, size);
}

int ha_eloq::batch_load_records(std::vector<uchar *> &vct_key)
{
  if (vct_key.size() > sk_pk_scan_batch_.size())
  {
    batch_key_.resize(vct_key.size());
    batch_rec_.resize(vct_key.size());
  }

  sk_pk_scan_batch_.clear();
  sk_pk_scan_batch_idx_= 0;

  MyEloqTx *my_tx= get_myeloq_tx(ha_thd());
  DBUG_ASSERT(my_tx != nullptr);

  for (size_t i= 0; i < vct_key.size(); i++)
  {
    Slice temp_last_key((const char *) vct_key[i], ref_length);
    size_t key_length= pk_descr_->key_length(table, temp_last_key);
    batch_key_[i]= EloqKey(vct_key[i], key_length);
    sk_pk_scan_batch_.emplace_back(TxKey(&batch_key_[i]), &batch_rec_[i]);
  }

  std::pair<const std::function<void()> *, const std::function<void()> *>
      coro_functors= my_tx->CoroFunctors();
  bool for_update= (lock.type >= TL_WRITE_ALLOW_WRITE);
  bool for_share= (lock.type == TL_READ_WITH_SHARED_LOCKS);

  uint64_t pk_key_version= table_schema_->KeySchema()->SchemaTs();
  BatchReadTxRequest batch_req(GetBaseTableNameFromTableSchema(),
                               pk_key_version, sk_pk_scan_batch_, for_update,
                               for_share, false, coro_functors.first,
                               coro_functors.second, my_tx->Txm());
  TransactionExecution *txm= my_tx->Txm();
  txm->Execute(&batch_req);
  batch_req.Wait();

  if (batch_req.IsError())
  {
    my_tx->tx_err_code_= batch_req.ErrorCode();
    return convert_tx_error(my_tx->tx_err_code_);
  }

  if (storage_hd == nullptr)
  {
    return 0;
  }

  // Here need to reconsider if to backfill records to memory or consider
  // Snapshot
  for (size_t i= 0; i < sk_pk_scan_batch_.size(); i++)
  {
    RecordStatus rec_status= sk_pk_scan_batch_[i].status_;
    uint64_t latest_version_ts= 1U;

    if (rec_status == RecordStatus::Unknown ||
        rec_status == RecordStatus::VersionUnknown ||
        rec_status == RecordStatus::BaseVersionMiss)
    {
      bool store_found= false;
      bool success= storage_hd->Read(
          *GetBaseTableNameFromTableSchema(), sk_pk_scan_batch_[i].key_,
          *sk_pk_scan_batch_[i].record_, store_found, latest_version_ts,
          table_schema_);
      if (!success)
      {
        sk_pk_scan_batch_[i].status_= RecordStatus::Deleted;
        continue;
      }

      sk_pk_scan_batch_[i].status_=
          store_found ? RecordStatus::Normal : RecordStatus::Deleted;
    }
  }

  return 0;
}

int ha_eloq::batch_get_record(uchar *buf)
{
  if (sk_pk_scan_batch_idx_ >= sk_pk_scan_batch_.size())
  {
    return HA_ERR_END_OF_FILE;
  }

  while (sk_pk_scan_batch_[sk_pk_scan_batch_idx_].status_ !=
         RecordStatus::Normal)
  {
    sk_pk_scan_batch_idx_++;
    if (sk_pk_scan_batch_idx_ >= sk_pk_scan_batch_.size())
    {
      return HA_ERR_END_OF_FILE;
    }
  }

  DecodeRecord(
      buf, sk_pk_scan_batch_[sk_pk_scan_batch_idx_].key_.GetKey<EloqKey>(),
      (EloqRecord *) sk_pk_scan_batch_[sk_pk_scan_batch_idx_].record_);
  sk_pk_scan_batch_idx_++;
  return 0;
}

/**
 * @brief Prepare for a multiple-rows insert operation.
 * e.g. - disable indexes (if they can be recreated fast) or
 * activate special bulk-insert optimizations
 * @param rows Rows to be inserted. 0 if we don't know
 * @param flags Flags to control index creation
 *
 * NOTICE
 *  Do not forget to call end_bulk_insert() later!
 */
void ha_eloq::start_bulk_insert(ha_rows rows, uint flags)
{
  DBUG_ENTER_FUNC();

  // Only support the bulk insert optimization in the case of return error
  // when violating the unique constraint. In other words, this optimization
  // will not be applied for the following statement:
  // 1. INSERT INTO t1 VALUES(),() ON DUPLICATE KEY UPDATE ...;
  // 2. REPLACE t2 SELECT * FROM t1;
  // 3. INSERT IGNORE INTO t1 VALUES(),(),();
  if (!is_duplicate_error_)
  {
    is_bulk_insert_= false;
    DBUG_VOID_RETURN;
  }

  THD *thd= ha_thd();
  MyEloqTx *my_tx= get_myeloq_tx(thd);
  DBUG_ASSERT(my_tx != nullptr);

  // Get the dirty indexes name.
  auto &dirty_index_names= my_tx->DirtyIndexNames();

  size_t reserve_size=
      (!rows || rows > batch_read_size_) ? batch_read_size_ : rows;
  // Check the uniqueness for primary key. If upsert semantic is used or the
  // primary key is UUID or autoinc column, skip the check.
  if (!is_insert_semantic_ || table_share->primary_key == MAX_INDEXES ||
      (table_share->primary_key == table_share->next_number_index &&
       table_share->key_info[table_share->primary_key]
               .key_part->field->unireg_check == mysql::Field::NEXT_NUMBER))
  {
    bool has_unique_key= false;
    for (uint kid= 0; kid < table_share->keys; ++kid)
    {
      if (kid == table_share->primary_key)
      {
        // The pk
        continue;
      }
      const txservice::TableName &index_name=
          table_schema_->IndexNameSchema(kid).first;
      if (index_name.StringView().find(txservice::UNIQUE_INDEX_NAME_PREFIX) !=
          std::string::npos)
      {
        has_unique_key= true;
        break;
      }
    }

    if (!has_unique_key && table_dirty_schema_ != nullptr)
    {
      for (auto &index_name : dirty_index_names)
      {
        if (index_name.StringView().find(
                txservice::UNIQUE_INDEX_NAME_PREFIX) != std::string::npos)
        {
          has_unique_key= true;
          break;
        }
      }
    }

    if (!has_unique_key)
    {
      is_bulk_insert_= false;
      DBUG_VOID_RETURN;
    }
  }
  else
  {
    // Primary key
    is_bulk_insert_= true;
    pk_bulk_insert_buffer_.reserve(reserve_size);
  }

  bool is_dirty= false;
  auto dirty_it= dirty_index_names.begin();
  for (uint kid= 0;
       kid < table_share->keys || dirty_it != dirty_index_names.end();)
  {
    if (kid == table_share->primary_key)
    {
      // The pk
      ++kid;
      continue;
    }

    is_dirty= kid == table_share->keys ? true : false;
    const txservice::TableName &index_name=
        !is_dirty ? table_schema_->IndexNameSchema(kid).first : *dirty_it;

    if (index_name.StringView().find(txservice::UNIQUE_INDEX_NAME_PREFIX) !=
        std::string::npos)
    {
      const EloqKeySchema *index_schema= static_cast<const EloqKeySchema *>(
          !is_dirty
              ? table_schema_->IndexNameSchema(kid).second.sk_schema_.get()
              : table_dirty_schema_->IndexKeySchema(index_name)
                    ->sk_schema_.get());
      uint64_t sk_key_version= index_schema->SchemaTs();

      // Unique secondary key
      auto insert_it= unique_sk_bulk_insert_buffer_.emplace(
          std::piecewise_construct,
          std::forward_as_tuple(index_name.StringView(), index_name.Type(),
                                index_name.Engine()),
          std::forward_as_tuple(sk_key_version, BulkInsertBuffer()));
      insert_it.first->second.second.reserve(reserve_size);
    }

    // Next index
    if (is_dirty)
    {
      ++dirty_it;
    }
    else
    {
      ++kid;
    }
  }

  DBUG_VOID_RETURN;
}

/**
 * @brief End special bulk-insert optimizations, which have been activated by
 * start_bulk_insert().
 *
 * @return 0 OK. != 0 Error
 */
int ha_eloq::end_bulk_insert()
{
  DBUG_ENTER_FUNC();

  int res= 0;
  size_t bulk_insert_size= pk_bulk_insert_buffer_.size();
  if (!bulk_insert_size && unique_sk_bulk_insert_buffer_.size() > 0)
  {
    bulk_insert_size=
        unique_sk_bulk_insert_buffer_.begin()->second.second.size();
  }

  if (is_bulk_insert_ && bulk_insert_size > 0)
  {
    DBUG_ASSERT(is_duplicate_error_);
    // Upsert the remain records.
    res= BulkUniqueCheck(bulk_insert_size);
    if (res)
    {
      table->file->print_error(res, MYF(0));
    }
  }
  is_duplicate_error_= true;
  is_bulk_insert_= false;
  pk_bulk_insert_buffer_.clear();
  unique_sk_bulk_insert_buffer_.clear();
  DBUG_RETURN(res);
}

struct st_mysql_storage_engine eloq_storage_engine= {
    MYSQL_HANDLERTON_INTERFACE_VERSION};

// this is an example of SHOW_SIMPLE_FUNC and of my_snprintf() service
// If this function would return an array, one should use SHOW_FUNC
static int show_func_eloq(MYSQL_THD thd, struct st_mysql_show_var *var,
                          char *buf)
{
  var->type= SHOW_CHAR;
  var->value= buf; // it's of SHOW_VAR_FUNC_BUFF_SIZE bytes
  my_snprintf(buf, SHOW_VAR_FUNC_BUFF_SIZE,
              "enum_var is %lu, ulong_var is %lu, int_var is %d, "
              "double_var is %f, %.6b", // %b is a MySQL extension
              srv_enum_var, srv_ulong_var, THDVAR(thd, int_var),
              srv_double_var, "really");
  return 0;
}

static struct st_mysql_show_var func_status[]= {
    {"func_eloq", (char *) show_func_eloq, SHOW_SIMPLE_FUNC},
    {0, 0, SHOW_UNDEF}};

struct st_mysql_daemon unusable_eloq= {MYSQL_DAEMON_INTERFACE_VERSION};

maria_declare_plugin(eloq){
    MYSQL_STORAGE_ENGINE_PLUGIN,
    &eloq_storage_engine,
    "ELOQ",
    "Liang Jeff Chen, ELOQDB",
    "Eloq storage engine",
    PLUGIN_LICENSE_GPL,
    eloq_init_func,                      /* Plugin Init */
    eloq_done_func,                      /* Plugin Deinit */
    0x0001,                              /* version number (0.1) */
    func_status,                         /* status variables */
    eloq_system_variables,               /* system variables */
    "0.1",                               /* string version */
    MariaDB_PLUGIN_MATURITY_EXPERIMENTAL /* maturity */
},
    eloq_i_s_temp_table_info /* ELOQ_TEMP_TABLE_INFO */

    maria_declare_plugin_end;
