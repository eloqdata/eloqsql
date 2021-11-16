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
#include "bigtable_handler.h"

#include "data_store_handler.h"
#include "kv_store.h"
#include "eloq_key.h"
#include "eloq_schema.h"
#include "partition.h"
#include "schema.h"
#include "tx_record.h"
#include "tx_service/include/error_messages.h"
#include "tx_service/include/type.h"

#include <endian.h>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <memory>

#ifdef set_bits
#undef set_bits // mariadb/include/my_global.h define set_bits conflict with
#endif          // boost

#include <boost/lexical_cast.hpp>

namespace cbt= ::google::cloud::bigtable;
namespace cbta= ::google::cloud::bigtable_admin;
using ::google::cloud::StatusOr;

static const std::string bigtable_table_catalog_name= "mariadb_tables";
static const std::string bigtable_database_catalog_name= "mariadb_databases";
static const std::string bigtable_mvcc_archive_name= "mvcc_archives";
static const std::string bigtable_table_statistics_version_name=
    "table_statistics_version";
static const std::string bigtable_table_statistics_name= "table_statistics";
static const std::string bigtable_range_table_name= "table_ranges";
static const std::string bigtable_last_range_id_name=
    "table_last_range_partition_id";
static const std::string bigtable_cluster_config_name= "cluster_config";

static const std::unordered_set<std::string> bigtable_sys_tables(
    {bigtable_table_catalog_name, bigtable_database_catalog_name,
     bigtable_mvcc_archive_name, bigtable_table_statistics_version_name,
     bigtable_table_statistics_name, bigtable_range_table_name,
     bigtable_last_range_id_name, bigtable_cluster_config_name});
static thread_local std::unique_ptr<PartitionFinder> partition_finder;

MyEloq::BigTableHandler::BigTableHandler(const std::string &keyspace,
                                         const std::string &project_id,
                                         const std::string &instance_id,
                                         bool bootstrap, bool ddl_skip_kv,
                                         uint32_t worker_pool_size)
    : keyspace_(keyspace), project_id_(project_id), instance_id_(instance_id),
      is_bootstrap_(bootstrap), ddl_skip_kv_(ddl_skip_kv),
      worker_pool_(worker_pool_size)
{
  // BigTable defaults options:
  // google-cloud-cpp/google/cloud/bigtable/internal/defaults.cc
  google::cloud::Options opts;

  opts.set<cbt::DataRetryPolicyOption>(
      cbt::DataLimitedTimeRetryPolicy(10s).clone());

  data_connection_= cbt::MakeDataConnection(opts);
  admin_client_= std::make_unique<cbta::BigtableTableAdminClient>(
      cbta::MakeBigtableTableAdminConnection(opts));
}

MyEloq::BigTableHandler::~BigTableHandler() { worker_pool_.Shutdown(); }

bool MyEloq::BigTableHandler::Connect()
{
  for (const std::string &sys_table : bigtable_sys_tables)
  {
    google::bigtable::admin::v2::Table table;

    auto &column_families= *table.mutable_column_families();
    auto &column_family= column_families[cf_];
    if (sys_table == bigtable_mvcc_archive_name)
    {
      auto oneday=
          std::chrono::duration_cast<std::chrono::seconds>(24h).count();
      column_family.mutable_gc_rule()->mutable_max_age()->set_seconds(oneday);
    }
    else
    {
      column_family.mutable_gc_rule()->set_max_num_versions(1);
    }
    std::string instance_name= cbt::InstanceName(project_id_, instance_id_);
    std::string table_id;
    table_id.append(keyspace_).append(".").append(sys_table);

    StatusOr<google::bigtable::admin::v2::Table> schema=
        admin_client_->CreateTable(instance_name, table_id, table);
    if (!schema.ok())
    {
      if (schema.status().code() != google::cloud::StatusCode::kAlreadyExists)
      {
        LOG(ERROR) << "Create system table failed, tablename: " << table_id
                   << ", error: " << schema.status().message();
        return false;
      }
      DLOG(INFO) << "System table already exists, tablename: " << table_id;
    }
  }

  return true;
}

bool MyEloq::BigTableHandler::InitializeClusterConfig(
    const std::unordered_map<uint32_t, std::vector<NodeConfig>> &ng_configs)
{
  uint32_t rep_group_cnt= ng_rep_cnt < ips.size() ? ng_rep_cnt : ips.size();

  CollectionCSV ng_ids_collection, ips_collection, ports_collection,
      members_collection;

  for (uint32_t group_id= 0; group_id < ips.size(); group_id++)
  {
    ng_ids_collection.Append(group_id);
    ips_collection.Append(ips.at(group_id));
    ports_collection.Append(ports.at(group_id));

    // determine the members in this ccnode's raft group.
    // for example, suppose 3 nodes, the raft configuration is as
    // follows: group1: 1(preferred leader),2,3 group2: 1,2(preferred
    // leader),3 group3: 1,2,3(preferred leader)
    // the first ip and port will be the preferred leader of the ng.
    std::string members;
    std::vector<NodeConfig> member_list;
    for (uint32_t idx= 0; idx < rep_group_cnt; ++idx)
    {
      uint32_t nid= group_id + idx;

      if (nid >= ips.size())
      {
        nid-= ips.size();
      }
      members.append(std::to_string(nid)).append(" ");
      member_list.emplace_back(nid, ips.at(nid), ports.at(nid));
    }
    members.pop_back();
    ng_configs.try_emplace(group_id, std::move(member_list));
    members_collection.Append(members);
  }

  cbt::Table cbt_handler= TableHandler(bigtable_cluster_config_name);

  std::string row_key(1, 0x0); // one byte of 0x0 as row key.
  cbt::SingleRowMutation mutation(row_key);
  mutation.emplace_back(
      cbt::SetCell(cf_, "ngids", std::move(ng_ids_collection).Payload()));
  mutation.emplace_back(
      cbt::SetCell(cf_, "ips", std::move(ips_collection).Payload()));
  mutation.emplace_back(
      cbt::SetCell(cf_, "ports", std::move(ports_collection).Payload()));
  mutation.emplace_back(cbt::SetCell(cf_, "ng_members",
                                     std::move(members_collection).Payload()));
  // use 2 as initial version since commit_ts = 1 means entry just
  // initialized and not valid yet.
  mutation.emplace_back(cbt::SetCell(cf_, "version", static_cast<int64_t>(2)));
  auto write_row= cbt_handler.Apply(std::move(mutation));
  if (!write_row.ok())
  {
    LOG(ERROR) << "Initialize cluster config failed, error: "
               << write_row.message();
    return false;
  }
  else
  {
    return true;
  }
}

bool MyEloq::BigTableHandler::ReadClusterConfig(
    std::unordered_map<uint32_t, std::vector<txservice::NodeConfig>>
        &ng_configs,
    uint64_t &version, bool &uninitialized)
{
  cbt::Table cbt_handler= TableHandler(bigtable_cluster_config_name);
  std::string row_key(1, 0x0); // one byte of 0x0 as row key.
  auto read_row= cbt_handler.ReadRow(row_key, cbt::Filter::Latest(1));
  if (!read_row.ok())
  {
    LOG(ERROR) << "Read cluster config failed, error: "
               << read_row.status().message();
    return false;
  }

  auto &[exists, row]= read_row.value();
  if (!exists)
  {
    uninitialized= true;
    return false;
  }

  std::vector<long> ng_list;
  std::map<uint32_t, std::pair<std::string, uint16_t>> node_ip_map;
  std::vector<std::string> ips;
  std::vector<long> ports;

  // Read node group id list
  CollectionCSV ng_collection(RowCell(row, "ngids").value());
  ng_collection.Deserialize(ng_list);

  // Read ip addr for each node
  CollectionCSV ip_collection(RowCell(row, "ips").value());
  ip_collection.Deserialize(ips);

  CollectionCSV ports_collection(RowCell(row, "ports").value());
  ports_collection.Deserialize(ports);

  for (size_t idx= 0; idx < ips.size(); idx++)
  {
    node_ip_map.try_emplace(
        ng_list.at(idx),
        std::make_pair(std::move(ips.at(idx)), ports.at(idx)));
  }

  // Read member node in each node group
  CollectionCSV ng_members_collection(RowCell(row, "ng_members").value());
  std::vector<std::string> members;
  ng_members_collection.Deserialize(members);

  for (size_t idx= 0; idx < members.size(); idx++)
  {
    std::vector<NodeConfig> ng_config;
    // parse member string
    std::istringstream iss(members[idx]);
    std::string nid_str;
    while (iss >> nid_str)
    {
      uint32_t nid= std::stoi(nid_str);
      ng_config.emplace_back(nid, node_ip_map.at(nid).first,
                             node_ip_map.at(nid).second);
    }
    ng_configs.try_emplace(ng_list.at(idx), std::move(ng_config));
  }

  version=
      RowCell(row, "version").decode_big_endian_integer<int64_t>().value();

  return true;
}

bool MyEloq::BigTableHandler::UpdateClusterConfig(
    const std::unordered_map<uint32_t, std::vector<txservice::NodeConfig>>
        &new_cnf,
    uint64_t version)
{
  CollectionCSV ng_ids_collection, ips_collection, ports_collection,
      members_collection;
  for (auto &ng_pair : new_cnf)
  {
    ng_ids_collection.Append(ng_pair.first);
    ips_collection.Append(ng_pair.second.front().host_name_);
    ports_collection.Append(ng_pair.second.front().port_);
    std::string members;
    for (auto &node : ng_pair.second)
    {
      members.append(std::to_string(node.node_id_) + " ");
    }
    members.pop_back();
    members_collection.Append(members);
  }

  cbt::Table cbt_handler= TableHandler(bigtable_cluster_config_name);

  std::string row_key(1, 0x0); // one byte of 0x0 as row key.
  cbt::SingleRowMutation mutation(row_key);
  mutation.emplace_back(
      cbt::SetCell(cf_, "ngids", std::move(ng_ids_collection).Payload()));
  mutation.emplace_back(
      cbt::SetCell(cf_, "ips", std::move(ips_collection).Payload()));
  mutation.emplace_back(
      cbt::SetCell(cf_, "ports", std::move(ports_collection).Payload()));
  mutation.emplace_back(cbt::SetCell(cf_, "ng_members",
                                     std::move(members_collection).Payload()));
  mutation.emplace_back(
      cbt::SetCell(cf_, "version", static_cast<int64_t>(version)));
  auto write_row= cbt_handler.Apply(std::move(mutation));
  if (!write_row.ok())
  {
    LOG(ERROR) << "Initialize cluster config failed, error: "
               << write_row.message();
    return false;
  }
  else
  {
    return true;
  }
}

bool MyEloq::BigTableHandler::PutAll(
    std::vector<txservice::FlushRecord> &batch,
    const txservice::TableName &table_name,
    const txservice::TableSchema *table_schema, uint32_t node_group)
{
  auto sz= batch.size();
  auto begin= std::chrono::steady_clock::now();

  const std::string &kv_table_name=
      table_schema->GetKVCatalogInfo()->GetKvTableName(table_name);

  const EloqRecordSchema *mysql_rec_schema=
      static_cast<const EloqRecordSchema *>(table_schema->RecordSchema());

  size_t flush_idx= 0;
  Partition *out_partition= nullptr;
  std::vector<std::pair<uint, Partition>> target_partitions;

  if (partition_finder == nullptr)
  {
    partition_finder= PartitionFinderFactory::Create();
  }

#ifdef RANGE_PARTITION_ENABLED
  if (!dynamic_cast<RangePartitionFinder *>(partition_finder.get())
           ->Init(tx_service_, node_group))
  {
    LOG(ERROR) << "Failed to init RangePartitionFinder!";
    return false;
  }
#endif

  PartitionResultType rt=
      partition_finder->FindPartitions(table_name, batch, target_partitions);
  if (rt != PartitionResultType::NORMAL)
  {
    partition_finder->ReleaseReadLocks();
    return false;
  }
  assert(target_partitions.size());
  auto part_it= target_partitions.begin();
  out_partition= &part_it->second;

  std::atomic<bool> bulk_apply_ok= true;
  std::vector<google::cloud::future<std::vector<cbt::FailedMutation>>>
      bulk_futures;
  while (flush_idx < batch.size() &&
         bulk_apply_ok.load(std::memory_order_acquire))
  {
    if (Sharder::Instance().LeaderTerm(node_group) < 0)
    {
      partition_finder->ReleaseReadLocks();
      return false;
    }

    // Start a new batch if the first flush record happens to be the first
    // record in next range.
    if (std::next(part_it) != target_partitions.end() &&
        std::next(part_it)->first == flush_idx)
    {
      part_it++;
      out_partition= &part_it->second;
    }
    int32_t pk1= out_partition->Pk1();
#ifdef RANGE_PARTITION_ENABLED
    txservice::TxKey key= batch.at(flush_idx).Key();
    int32_t new_pk1= out_partition->NewPk1(&key);
    assert(out_partition->RangeOwner() == node_group);
    pk1= new_pk1 > 0 ? new_pk1 : pk1;
#endif

    size_t mutate_rows= 0;
    cbt::BulkMutation bulk;
    for (; flush_idx < batch.size() && mutate_rows < max_mutate_rows_;
         ++flush_idx, ++mutate_rows)
    {
      using namespace txservice;
      FlushRecord &ckpt_rec= batch.at(flush_idx);

      // Start a new batch if done with current partition.
      if (std::next(part_it) != target_partitions.end() &&
          std::next(part_it)->first == flush_idx)
      {
        part_it++;
        out_partition= &part_it->second;
        break;
      }

      const EloqKey *mono_key= ckpt_rec.Key().GetKey<EloqKey>();
      const EloqRecord *mono_rec=
          static_cast<const EloqRecord *>(ckpt_rec.Payload());

      std::string row_key(mono_key->PackedValue());

      assert(ckpt_rec.payload_status_ == RecordStatus::Normal ||
             ckpt_rec.payload_status_ == RecordStatus::Deleted);

      bool deleted= ckpt_rec.payload_status_ == RecordStatus::Deleted;

      std::string payload;
      std::string unpack_info;
      if (!deleted)
      {
        mysql_rec_schema->BindBigTablePayload(mono_rec->encoded_blob_,
                                              payload);
        unpack_info= std::string(mono_rec->unpack_info_.data(),
                                 mono_rec->unpack_info_.size());
      }

      // In bigtable, gc policies are set at the column family level, we
      // can't set a cell-level gc policy, so we follow
      // https://cloud.google.com/bigtable/docs/gc-cell-level simulate
      // cell-level TTL.
      auto expire_tp_ms= std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::microseconds(ckpt_rec.commit_ts_));
      if (ckpt_rec.payload_status_ == RecordStatus::Normal)
      {
        expire_tp_ms+= 24h * 365 * 1000;
      }
      else
      {
        assert(ckpt_rec.payload_status_ == RecordStatus::Deleted);
        expire_tp_ms+= 24h;
      }

      // BigTable deal with data partition automatically, thus we ignore the
      // pk1 and pk2 column.
      if (ckpt_rec.payload_status_ == RecordStatus::Deleted)
      {
        // When we design different expire time by Normal or Deleted, we
        // found that the value would be read repeatedly during the test
        // because the old version of Deleted are not removed. so we delete
        // old value before insert.
        bulk.emplace_back(
            cbt::SingleRowMutation(row_key, cbt::DeleteFromRow()));
      }
      bulk.emplace_back(cbt::SingleRowMutation(
          row_key,
          cbt::SetCell(cf_, "___payload___", expire_tp_ms, std::move(payload)),
          cbt::SetCell(cf_, "___unpack_info___", expire_tp_ms,
                       std::move(unpack_info)),
          cbt::SetCell(cf_, "___version___", expire_tp_ms,
                       static_cast<int64_t>(ckpt_rec.commit_ts_)),
          cbt::SetCell(cf_, "___deleted___", expire_tp_ms,
                       std::string(1, deleted))));
    }

    SemiAsyncBulkApply(kv_table_name, std::move(bulk), bulk_futures,
                       bulk_apply_ok);
  }

  SemiAsyncBulkApplyJoin(bulk_futures);
  partition_finder->ReleaseReadLocks();

  auto end= std::chrono::steady_clock::now();
  auto cost_ms=
      std::chrono::duration_cast<std::chrono::milliseconds>(end - begin)
          .count();
  DLOG(INFO) << "PutAll batch sz: " << sz << ", cost(ms): " << cost_ms;
  return bulk_apply_ok.load(std::memory_order_acquire);
}

// Sync call, instead of async call like cassandra.
void MyEloq::BigTableHandler::UpsertTable(
    const txservice::TableSchema *old_table_schema,
    const txservice::TableSchema *table_schema,
    txservice::OperationType op_type, uint64_t write_time,
    txservice::NodeGroupId ng_id, int64_t tx_term,
    txservice::CcHandlerResult<txservice::Void> *hd_res,
    const txservice::AlterTableInfo *alter_table_info,
    txservice::CcRequestBase *cc_req, txservice::CcShard *ccs,
    txservice::CcErrorCode *err_code)
{
  int64_t leader_term= Sharder::Instance().TryPinNodeGroupData(ng_id);
  if (leader_term < 0)
  {
    hd_res->SetError(CcErrorCode::TX_NODE_NOT_LEADER);
    return;
  }

  std::shared_ptr<void> defer_unpin(nullptr, [ng_id](void *) {
    Sharder::Instance().UnpinNodeGroupData(ng_id);
  });

  if (leader_term != tx_term)
  {
    hd_res->SetError(CcErrorCode::NG_TERM_CHANGED);
    return;
  }

  // Use old schema for drop table as the new schema would be null.
  const txservice::TableSchema *schema=
      op_type == OperationType::DropTable ? old_table_schema : table_schema;

  // Instead of creating a detached thread, enqueue the task into a workpool,
  // in case UpsertTableOp is destructed before that detached thread finish.
  // Notice that BigTableHandler is destructed before TxService, thus using
  // worker_pool_ is safe.
  auto upsert_table_task= [this, schema, op_type, write_time, defer_unpin,
                           hd_res, alter_table_info]() {
    if (UpsertTable(schema, op_type, write_time, alter_table_info))
    {
      hd_res->SetFinished();
    }
    else
    {
      hd_res->SetError(CcErrorCode::DATA_STORE_ERR);
    }
  };

  worker_pool_.SubmitWork(std::move(upsert_table_task));
  return;
}

bool MyEloq::BigTableHandler::UpsertTable(
    const txservice::TableSchema *table_schema,
    txservice::OperationType op_type, uint64_t write_time,
    const txservice::AlterTableInfo *alter_table_info)
{
  bool ok= true;

  const txservice::TableName &base_table_name=
      table_schema->GetBaseTableName();
  const txservice::KVCatalogInfo *kv_info= table_schema->GetKVCatalogInfo();

  if (op_type == txservice::OperationType::CreateTable)
  {
    ok= ok && CreateKvTable(kv_info->kv_table_name_) &&
        std::all_of(
            kv_info->kv_index_names_.begin(), kv_info->kv_index_names_.end(),
            [this](const std::pair<txservice::TableName, std::string> &p) {
              return CreateKvTable(p.second);
            });

#ifdef RANGE_PARTITION_ENABLED
    ok= ok && InitTableRanges(base_table_name, table_schema->Version()) &&
        std::all_of(
            kv_info->kv_index_names_.begin(), kv_info->kv_index_names_.end(),
            [this, table_schema](
                const std::pair<txservice::TableName, std::string> &p) {
              return InitTableRanges(p.first, table_schema->Version());
            });

    ok= ok && InitTableLastRangePartitionId(base_table_name) &&
        std::all_of(
            kv_info->kv_index_names_.begin(), kv_info->kv_index_names_.end(),
            [this](const std::pair<txservice::TableName, std::string> &p) {
              return InitTableLastRangePartitionId(p.first);
            });
#endif

    ok= ok && UpsertCatalog(table_schema, write_time);

    if (ok && base_table_name == Sequences::table_name_)
    {
      Sequences::SetTableSchema(
          static_cast<const MysqlTableSchema *>(table_schema));
    }
  }
  else if (op_type == txservice::OperationType::Update)
  {
    ok= ok && UpsertCatalog(table_schema, write_time);
  }
  else if (op_type == txservice::OperationType::AddIndex)
  {
    ok= ok &&
        std::all_of(
            alter_table_info->index_add_names_.begin(),
            alter_table_info->index_add_names_.end(),
            [this](const std::pair<txservice::TableName, std::string> &p) {
              return CreateKvTable(p.second);
            });

#ifdef RANGE_PARTITION_ENABLED
    if (ok)
    {
      ok= std::all_of(
          alter_table_info->index_add_names_.begin(),
          alter_table_info->index_add_names_.end(),
          [this, table_schema](
              const std::pair<txservice::TableName, std::string> &p) {
            return InitTableRanges(p.first, table_schema->Version());
          });
    }

    ok= ok &&
        std::all_of(
            alter_table_info->index_add_names_.begin(),
            alter_table_info->index_add_names_.end(),
            [this](const std::pair<txservice::TableName, std::string> &p) {
              return InitTableLastRangePartitionId(p.first);
            });
#endif

    ok= ok && UpsertCatalog(table_schema, write_time);
  }
  else if (op_type == txservice::OperationType::DropIndex)
  {
    ok= ok &&
        std::all_of(
            alter_table_info->index_drop_names_.begin(),
            alter_table_info->index_drop_names_.end(),
            [this](const std::pair<txservice::TableName, std::string> &p) {
              return DropKvTable(p.second);
            });

#ifdef RANGE_PARTITION_ENABLED
    ok= ok &&
        std::all_of(
            alter_table_info->index_drop_names_.begin(),
            alter_table_info->index_drop_names_.end(),
            [this](const std::pair<txservice::TableName, std::string> &p) {
              return DeleteTableRanges(p.first);
            });

    ok= ok &&
        std::all_of(
            alter_table_info->index_drop_names_.begin(),
            alter_table_info->index_drop_names_.end(),
            [this](const std::pair<txservice::TableName, std::string> &p) {
              return DeleteTableLastRangePartitionId(p.first);
            });
#endif

    ok= ok && UpsertCatalog(table_schema, write_time);
  }
  else if (op_type == txservice::OperationType::DropTable)
  {
    ok= ok && DropKvTable(kv_info->kv_table_name_) &&
        std::all_of(
            kv_info->kv_index_names_.begin(), kv_info->kv_index_names_.end(),
            [this](const std::pair<txservice::TableName, std::string> &p) {
              return DropKvTable(p.second);
            });

#ifdef RANGE_PARTITION_ENABLED
    ok= DeleteTableRanges(base_table_name) &&
        std::all_of(
            kv_info->kv_index_names_.begin(), kv_info->kv_index_names_.end(),
            [this](const std::pair<txservice::TableName, std::string> &p) {
              return DeleteTableRanges(p.first);
            });

    ok= ok && DeleteTableLastRangePartitionId(base_table_name) &&
        std::all_of(
            kv_info->kv_index_names_.begin(), kv_info->kv_index_names_.end(),
            [this](const std::pair<txservice::TableName, std::string> &p) {
              return DeleteTableLastRangePartitionId(p.first);
            });
#endif

    if (ok)
    {
      const EloqRecordSchema *rsch=
          static_cast<const EloqRecordSchema *>(table_schema->RecordSchema());
      if (rsch->AutoIncrementIndex() >= 0)
      {
        // For CREATE TABLE, will write the initial sequence record into the
        // sequence ccmap, and the record will be flush into the data store
        // during normal checkpoint, so there is no need to insert the
        // initial sequence record into data store here.
        ok= DeleteSeqence(base_table_name);
      }
    }

    ok= ok && DeleteTableStatistics(base_table_name);

    ok= ok && DeleteCatalog(base_table_name, write_time);
  }
  else
  {
    assert("Unknown OperationType" && false);
  }

  return ok;
}

void MyEloq::BigTableHandler::FetchTableCatalog(
    const txservice::TableName &ccm_table_name,
    txservice::FetchCatalogCc *fetch_cc)
{
  cbt::Table cbt_handler= TableHandler(bigtable_table_catalog_name);
  std::string row_key(ccm_table_name.String());
  auto f= cbt_handler.AsyncReadRow(std::move(row_key), cbt::Filter::Latest(1));
  f.then(std::bind(&BigTableHandler::OnFetchCatalog, this,
                   std::placeholders::_1, fetch_cc));
}

void MyEloq::BigTableHandler::OnFetchCatalog(
    google::cloud::future<
        google::cloud::StatusOr<std::pair<bool, google::cloud::bigtable::Row>>>
        f,
    txservice::FetchCatalogCc *fetch_cc)
{
  auto status= f.get();
  if (!status.ok())
  {
    LOG(ERROR) << "Fetch catalog failed, tablename: "
               << fetch_cc->CatalogName().StringView()
               << ", error: " << status.status().message();
    fetch_cc->SetFinish(
        txservice::RecordStatus::Unknown,
        static_cast<int>(txservice::CcErrorCode::DATA_STORE_ERR));
    return;
  }

  const auto &[exists, row]= status.value();
  if (exists)
  {
    std::string &catalog_image= fetch_cc->CatalogImage();
    uint64_t &version= fetch_cc->CommitTs();

    const std::string &frm= RowCell(row, "content").value();
    const std::string &kv_table_name= RowCell(row, "kvtablename").value();
    const std::string &kv_index_names= RowCell(row, "kvindexname").value();
    const std::string &key_schemas_ts= RowCell(row, "keyschemasts").value();
    catalog_image.append(SerializeSchemaImage(
        frm, BigTableCatalogInfo(kv_table_name, kv_index_names).Serialize(),
        TableKeySchemaTs(key_schemas_ts).Serialize()));

    version= static_cast<uint64_t>(
        RowCell(row, "version").decode_big_endian_integer<int64_t>().value());
    fetch_cc->SetFinish(txservice::RecordStatus::Normal, 0);
  }
  else
  {
    fetch_cc->CatalogImage().clear();
    fetch_cc->CommitTs()= 1;
    fetch_cc->SetFinish(txservice::RecordStatus::Deleted, 0);
  }
}

void MyEloq::BigTableHandler::FetchTableRanges(
    txservice::FetchTableRangesCc *fetch_cc)
{
  cbt::Table cbt_handler= TableHandler(bigtable_range_table_name);

  std::string row_key_prefix;
  row_key_prefix.append(fetch_cc->table_name_.String()).append(":");

  auto on_row= [fetch_cc](const cbt::Row &row) {
    std::string mono_key=
        row.row_key().substr(fetch_cc->table_name_.StringView().size() + 1);

    std::unique_ptr<EloqKey> start_key= nullptr;
    if (mono_key.size() > 1 || *mono_key.data() != 0x00)
    {
      LocalCcShards *shards= Sharder::Instance().GetLocalCcShards();
      std::unique_lock<std::mutex> heap_lk(shards->table_ranges_heap_mux_);
      mi_override_thread(shards->GetTableRangesHeapThreadId());
      mi_heap_t *prev_heap= mi_heap_set_default(shards->GetTableRangesHeap());

      start_key= std::make_unique<EloqKey>(
          reinterpret_cast<const uchar *>(mono_key.data()), mono_key.size());

      mi_heap_set_default(prev_heap);
      mi_restore_default_thread_id();
    }

    int32_t partition_id=
        static_cast<int32_t>(RowCell(row, "___partition_id___")
                                 .decode_big_endian_integer<int64_t>()
                                 .value());
    uint64_t version_ts=
        static_cast<uint64_t>(RowCell(row, "___version___")
                                  .decode_big_endian_integer<int64_t>()
                                  .value());

    fetch_cc->AppendTableRange(InitRangeEntry(
        start_key == nullptr ? TxKey(EloqKey::NegativeInfinity())
                             : TxKey(std::move(start_key)),
        partition_id, version_ts));
    return google::cloud::make_ready_future(true);
  };

  auto on_finish= [fetch_cc](const google::cloud::Status &stream_status) {
    if (stream_status.ok())
    {
      if (fetch_cc->EmptyRanges())
      {
        fetch_cc->AppendTableRange(InitRangeEntry(
            TxKey(EloqKey::NegativeInfinity()),
            Partition::InitialPartitionId(fetch_cc->table_name_.StringView()),
            1));
      }

      fetch_cc->SetFinish(0);
    }
    else
    {
      LOG(ERROR) << "Fetch table ranges failed, tablename: "
                 << fetch_cc->table_name_.StringView()
                 << ", error: " << stream_status.message();
      fetch_cc->SetFinish(static_cast<int>(CcErrorCode::DATA_STORE_ERR));
    }
  };

  // Do not read slice info
  cbt::Filter filter= cbt::Filter::ColumnRegex("partition_id|version");
  filter.Chain(cbt::Filter::Latest(1));
  cbt_handler.AsyncReadRows(on_row, on_finish,
                            cbt::RowRange::Prefix(std::move(row_key_prefix)),
                            filter);
}

void MyEloq::BigTableHandler::FetchRangeSlices(
    txservice::FetchRangeSlicesReq *fetch_cc)
{
  if (Sharder::Instance().TryPinNodeGroupData(fetch_cc->cc_ng_id_) !=
      fetch_cc->cc_ng_term_)
  {
    fetch_cc->SetFinish(CcErrorCode::NG_TERM_CHANGED);
    return;
  }
  cbt::Table cbt_handler= TableHandler(bigtable_range_table_name);

  const TemplateRangeInfo<EloqKey> *range_info=
      static_cast<const TemplateRangeInfo<EloqKey> *>(
          fetch_cc->range_entry_->GetRangeInfo());

  auto range_start_key= range_info->StartKey();
  const EloqKey *mono_key=
      (range_start_key == nullptr ||
       range_start_key->Type() == txservice::KeyType::NegativeInf)
          ? EloqKey::PackedNegativeInfinity()
          : static_cast<const EloqKey *>(range_start_key);
  std::string row_key(fetch_cc->table_name_.String());
  row_key.push_back(':');
  row_key.append(mono_key->PackedValue());

  // Only read slice info columns
  cbt::Filter filter= cbt::Filter::ColumnRegex("___slice.*");
  filter.Chain(cbt::Filter::Latest(1));
  auto f= cbt_handler.AsyncReadRow(std::move(row_key), filter);
  f.then(std::bind(&BigTableHandler::OnFetchRangeSlices, this,
                   std::placeholders::_1, fetch_cc));
}

void MyEloq::BigTableHandler::OnFetchRangeSlices(
    google::cloud::future<
        google::cloud::StatusOr<std::pair<bool, google::cloud::bigtable::Row>>>
        f,
    txservice::FetchRangeSlicesReq *fetch_cc)
{
  NodeGroupId ng_id= fetch_cc->cc_ng_id_;
  auto status= f.get();
  if (!status.ok())
  {
    LOG(ERROR) << "Fetch range slices failed, tablename: "
               << fetch_cc->table_name_.StringView()
               << ", error: " << status.status().message();
    fetch_cc->SetFinish(txservice::CcErrorCode::DATA_STORE_ERR);
    Sharder::Instance().UnpinNodeGroupData(ng_id);
    return;
  }
  const auto &[exists, row]= status.value();
  if (exists)
  {
    std::vector<uint32_t> slice_sizes;
    {
      const std::string &slice_sizes_str=
          RowCell(row, "___slice_sizes___").value();
      const char *ptr= slice_sizes_str.data();
      const char *end= ptr + slice_sizes_str.size();
      while (ptr < end)
      {
        uint32_t slice_size= *reinterpret_cast<const uint32_t *>(ptr);
        slice_sizes.emplace_back(slice_size);
        ptr+= sizeof(slice_size);
      }
    }

    std::vector<std::unique_ptr<EloqKey>> slice_keys;
    {
      const std::string &slice_keys_str=
          RowCell(row, "___slice_keys___").value();

      LocalCcShards *shards= Sharder::Instance().GetLocalCcShards();
      std::unique_lock<std::mutex> heap_lk(shards->table_ranges_heap_mux_);
      mi_override_thread(shards->GetTableRangesHeapThreadId());
      mi_heap_t *prev_heap= mi_heap_set_default(shards->GetTableRangesHeap());

      size_t offset= 0;
      while (offset < slice_keys_str.size())
      {
        auto slice_key= std::make_unique<EloqKey>();
        const txservice::KeySchema *unused_key_schema= nullptr;
        slice_key->Deserialize(slice_keys_str.data(), offset,
                               unused_key_schema);
        slice_keys.emplace_back(std::move(slice_key));
      }

      mi_heap_set_default(prev_heap);
      mi_restore_default_thread_id();
      heap_lk.unlock();
    }

    assert(slice_keys.size() + 1 == slice_sizes.size());

    fetch_cc->slice_info_.emplace_back(TxKey(), slice_sizes.front(),
                                       SliceStatus::PartiallyCached);
    for (size_t i= 0; i < slice_keys.size(); ++i)
    {
      fetch_cc->slice_info_.emplace_back(TxKey(std::move(slice_keys.at(i))),
                                         slice_sizes.at(i + 1),
                                         SliceStatus::PartiallyCached);
    }
  }
  if (fetch_cc->slice_info_.empty())
  {
    fetch_cc->slice_info_.emplace_back(TxKey(), 0,
                                       SliceStatus::PartiallyCached);
  }

  fetch_cc->SetFinish(CcErrorCode::NO_ERROR);
  Sharder::Instance().UnpinNodeGroupData(ng_id);
}

bool MyEloq::BigTableHandler::Read(const txservice::TableName &table_name,
                                   const txservice::TxKey &key,
                                   txservice::TxRecord &rec, bool &found,
                                   uint64_t &version_ts,
                                   const txservice::TableSchema *table_schema)
{
  const std::string &kv_table_name=
      table_schema->GetKVCatalogInfo()->GetKvTableName(table_name);

  cbt::Table cbt_handler= TableHandler(kv_table_name);

  const EloqKey &eloq_key= *key.GetKey<EloqKey>();
  std::string row_key(eloq_key.PackedValueSlice().data(),
                      eloq_key.PackedValueSlice().size());

  metrics::TimePoint start;
  if (metrics::enable_kv_metrics)
  {
    start= metrics::Clock::now();
  }

  auto read_row=
      cbt_handler.ReadRow(std::move(row_key), cbt::Filter::Latest(1));

  if (metrics::enable_kv_metrics)
  {
    metrics::kv_meter->CollectDuration(metrics::NAME_KV_READ_DURATION, start);
    metrics::kv_meter->Collect(metrics::NAME_KV_READ_TOTAL, 1);
  }

  if (!read_row.ok())
  {
    LOG(ERROR) << "Read row failed, table_name: " << table_name.StringView()
               << ", kv_table_name: " << kv_table_name
               << ", error: " << read_row.status().message();
    return false;
  }

  const auto &[exists, row]= read_row.value();
  found= exists;
  if (found)
  {
    version_ts= static_cast<uint64_t>(RowCell(row, "___version___")
                                          .decode_big_endian_integer<int64_t>()
                                          .value());
    const std::string &deleted_str= RowCell(row, "___deleted___").value();
    assert(deleted_str.size() == 1);
    bool deleted= deleted_str.front();
    found= !deleted;
    if (!deleted)
    {
      const std::string &payload= RowCell(row, "___payload___").value();

      const EloqRecordSchema *rec_sch=
          static_cast<const EloqRecordSchema *>(table_schema->RecordSchema());
      EloqRecord &eloq_rec= static_cast<EloqRecord &>(rec);
      rec_sch->Encode(payload, eloq_rec.encoded_blob_);

      const std::string &unpack_info=
          RowCell(row, "___unpack_info___").value();
      eloq_rec.SetUnpackInfo(
          reinterpret_cast<const uchar *>(unpack_info.data()),
          unpack_info.size());
    }
  }

  return true;
}

txservice::store::DataStoreHandler::DataStoreOpStatus
MyEloq::BigTableHandler::FetchRecord(txservice::FetchRecordCc *fetch_cc)
{
  const EloqKey &mono_key= *fetch_cc->tx_key_.GetKey<EloqKey>();

  const std::string &kv_table_name=
      fetch_cc->table_schema_->GetKVCatalogInfo()->GetKvTableName(
          *fetch_cc->table_name_);
  cbt::Table cbt_handler= TableHandler(kv_table_name);
  std::string row_key(mono_key.PackedValueSlice().data(),
                      mono_key.PackedValueSlice().size());

  // set read start time
  if (metrics::enable_kv_metrics)
  {
    fetch_cc->start_= metrics::Clock::now();
  }
  auto f= cbt_handler.AsyncReadRow(std::move(row_key), cbt::Filter::Latest(1));
  f.then(std::bind(&BigTableHandler::OnFetchRecord, this,
                   std::placeholders::_1, fetch_cc));

  return store::DataStoreHandler::DataStoreOpStatus::Success;
}

void MyEloq::BigTableHandler::OnFetchRecord(
    google::cloud::future<
        google::cloud::StatusOr<std::pair<bool, google::cloud::bigtable::Row>>>
        f,
    txservice::FetchRecordCc *fetch_cc)
{
  auto status= f.get();
  if (metrics::enable_kv_metrics)
  {
    metrics::kv_meter->CollectDuration(metrics::NAME_KV_READ_DURATION,
                                       fetch_cc->start_);
    metrics::kv_meter->Collect(metrics::NAME_KV_READ_TOTAL, 1);
  }
  if (!status.ok())
  {
    LOG(ERROR) << "Fetch record failed, tablename: "
               << fetch_cc->table_name_->StringView()
               << ", error: " << status.status().message();
    fetch_cc->SetFinish(
        static_cast<int>(txservice::CcErrorCode::DATA_STORE_ERR));
    return;
  }

  const auto &[exists, row]= status.value();
  if (exists)
  {
    fetch_cc->rec_ts_=
        static_cast<uint64_t>(RowCell(row, "___version___")
                                  .decode_big_endian_integer<int64_t>()
                                  .value());
    const std::string &deleted_str= RowCell(row, "___deleted___").value();
    assert(deleted_str.size() == 1);
    bool deleted= deleted_str.front();
    if (!deleted)
    {
      fetch_cc->rec_status_= RecordStatus::Normal;
      const std::string &payload= RowCell(row, "___payload___").value();

      const EloqRecordSchema *rec_sch= static_cast<const EloqRecordSchema *>(
          fetch_cc->table_schema_->RecordSchema());
      fetch_cc->rec_= std::make_unique<EloqRecord>();
      EloqRecord &eloq_rec= static_cast<EloqRecord &>(*fetch_cc->rec_);
      // Encode NonPkColumn for pk record.
      rec_sch->Encode(payload, eloq_rec.encoded_blob_);

      const std::string &unpack_info=
          RowCell(row, "___unpack_info___").value();
      eloq_rec.SetUnpackInfo(
          reinterpret_cast<const uchar *>(unpack_info.data()),
          unpack_info.size());
    }
    else
    {
      fetch_cc->rec_status_= RecordStatus::Deleted;
    }
  }
  else
  {
    fetch_cc->rec_status_= RecordStatus::Deleted;
    fetch_cc->rec_ts_= 1;
  }

  fetch_cc->SetFinish(0);
}

bool MyEloq::BigTableHandler::FetchTable(
    const txservice::TableName &table_name, std::string &schema_image,
    bool &found, uint64_t &version_ts) const
{
  cbt::Table cbt_handler= TableHandler(bigtable_table_catalog_name);
  std::string row_key(table_name.String());
  auto status= cbt_handler.ReadRow(std::move(row_key), cbt::Filter::Latest(1));
  if (!status.ok())
  {
    LOG(ERROR) << "Fetch table failed, tablename: " << table_name.StringView()
               << ", error: " << status.status().message();
    return false;
  }

  const auto &[exists, row]= status.value();
  found= exists;

  if (!exists)
  {
    return true;
  }

  const std::string &frm= RowCell(row, "content").value();
  version_ts= static_cast<uint64_t>(
      RowCell(row, "version").decode_big_endian_integer<int64_t>().value());
  const std::string &kv_table_name= RowCell(row, "kvtablename").value();
  const std::string &kv_index_names= RowCell(row, "kvindexname").value();
  const std::string &key_schemas_ts= RowCell(row, "keyschemasts").value();
  schema_image.append(SerializeSchemaImage(
      frm, BigTableCatalogInfo(kv_table_name, kv_index_names).Serialize(),
      TableKeySchemaTs(key_schemas_ts).Serialize()));
  return true;
}

void MyEloq::BigTableHandler::FetchCurrentTableStatistics(
    const txservice::TableName &ccm_table_name,
    FetchTableStatisticsCc *fetch_cc)
{
  cbt::Table cbt_handler= TableHandler(bigtable_table_statistics_version_name);

  std::string row_key(ccm_table_name.String());
  auto f= cbt_handler.AsyncReadRow(std::move(row_key), cbt::Filter::Latest(1));
  f.then(std::bind(&BigTableHandler::OnFetchCurrentTableStatistics, this,
                   std::placeholders::_1, fetch_cc));
}

void MyEloq::BigTableHandler::OnFetchCurrentTableStatistics(
    google::cloud::future<
        google::cloud::StatusOr<std::pair<bool, google::cloud::bigtable::Row>>>
        f,
    txservice::FetchTableStatisticsCc *fetch_cc)
{
  auto status= f.get();
  if (!status.ok())
  {
    LOG(ERROR) << "Fetch table statistics version failed, tablename: "
               << fetch_cc->CatalogName().StringView()
               << ", error: " << status.status().message();
    fetch_cc->SetFinish(static_cast<int>(CcErrorCode::DATA_STORE_ERR));
    return;
  }

  const auto &[exists, row]= status.value();
  if (exists)
  {
    uint64_t version= static_cast<uint64_t>(
        RowCell(row, "version").decode_big_endian_integer<int64_t>().value());
    fetch_cc->SetCurrentVersion(version);
    FetchTableStatistics(fetch_cc->CatalogName(), fetch_cc);
  }
  else
  {
    fetch_cc->SetFinish(0);
  }
}

void MyEloq::BigTableHandler::FetchTableStatistics(
    const txservice::TableName &ccm_table_name,
    txservice::FetchTableStatisticsCc *fetch_cc)
{
  cbt::Table cbt_handler= TableHandler(bigtable_table_statistics_name);

  // row_key: tablename:version:indextype:indexname:segment_id
  std::string row_key_prefix;
  row_key_prefix.append(ccm_table_name.String());
  row_key_prefix.push_back(':');
  row_key_prefix.append(std::to_string(fetch_cc->CurrentVersion()));
  row_key_prefix.push_back(':');

  auto on_row= [fetch_cc](const cbt::Row &row) {
    const std::string &row_key= row.row_key();

    TableName table_or_index_name=
        [](const std::string &row_key) -> txservice::TableName {
      int ret= 0;
      butil::StringSplitter sp(row_key, ':');

      std::string base_table_name(sp.field_sp().data(), sp.field_sp().size());
      sp++;

      uint64_t version;
      ret= sp.to_ulong(&version);
      assert(ret == 0);
      sp++;

      uint8_t index_type= 0;
      ret= sp.to_uint8(&index_type);
      assert(ret == 0);
      sp++;

      std::string index_name(sp.field_sp().data(), sp.field_sp().size());
      sp++;

      uint32_t segment_id= 0;
      ret= sp.to_uint(&segment_id);
      assert(ret == 0);
      (void) ret;
      return txservice::TableName(
          std::move(index_name),
          static_cast<txservice::TableType>(index_type));
    }(row_key);

    int64_t records_i64=
        RowCell(row, "records").decode_big_endian_integer<int64_t>().value();
    if (records_i64 >= 0)
    {
      uint64_t records= static_cast<uint64_t>(records_i64);
      fetch_cc->SetRecords(table_or_index_name, records);
    }

    std::vector<TxKey> samplekeys;
    const std::string &samplekeys_str= RowCell(row, "samplekeys").value();
    size_t offset= 0;
    while (offset < samplekeys_str.size())
    {
      auto mono_key= std::make_unique<EloqKey>();
      const txservice::KeySchema *unused_key_schema= nullptr;
      mono_key->Deserialize(samplekeys_str.data(), offset, unused_key_schema);
      samplekeys.emplace_back(std::move(mono_key));
    }

    fetch_cc->SamplePoolMergeFrom(table_or_index_name, std::move(samplekeys));

    return google::cloud::make_ready_future(true);
  };

  auto on_finish= [fetch_cc](const google::cloud::Status &stream_status) {
    if (stream_status.ok())
    {
      fetch_cc->SetFinish(0);
    }
    else
    {
      LOG(ERROR) << "Fetch table statistics failed, tablename: "
                 << fetch_cc->CatalogName().StringView()
                 << ", error: " << stream_status.message();
      fetch_cc->SetFinish(static_cast<int>(CcErrorCode::DATA_STORE_ERR));
    }
  };

  cbt_handler.AsyncReadRows(on_row, on_finish,
                            cbt::RowRange::Prefix(row_key_prefix),
                            cbt::Filter::Latest(1));
}

bool MyEloq::BigTableHandler::UpsertTableStatistics(
    const txservice::TableName &ccm_table_name,
    const std::unordered_map<
        txservice::TableName,
        std::pair<uint64_t, std::vector<txservice::TxKey>>> &sample_pool_map,
    uint64_t version)
{
  {
    cbt::Table cbt_handler_stat= TableHandler(bigtable_table_statistics_name);
    auto to_row_key= [](const txservice::TableName &ccm_table_name,
                        const txservice::TableName &indexname,
                        uint64_t version, uint32_t segment_id) {
      std::string row_key(ccm_table_name.StringView());
      row_key.push_back(':');
      row_key.append(std::to_string(version));
      row_key.push_back(':');
      row_key.append(std::to_string(static_cast<uint8_t>(indexname.Type())));
      row_key.push_back(':');
      row_key.append(indexname.StringView());
      row_key.push_back(':');
      row_key.append(std::to_string(segment_id));
      return row_key;
    };

    for (const auto &[indexname, sample_pool] : sample_pool_map)
    {
      std::string samplekeys;

      uint32_t segment_id= 0;
      uint32_t segment_size= 0;
      size_t sz= sample_pool.second.size();
      for (size_t i= 0; i < sz; ++i)
      {
        const EloqKey *samplekey= sample_pool.second[i].GetKey<EloqKey>();
        if (segment_size + samplekey->Size() >= max_cell_size_)
        {
          std::string row_key(
              to_row_key(ccm_table_name, indexname, version, segment_id));
          auto write_row= cbt_handler_stat.Apply(cbt::SingleRowMutation(
              std::move(row_key),
              // set records to -1 means uncomplete write.
              cbt::SetCell(cf_, "records", static_cast<int64_t>(-1)),
              cbt::SetCell(cf_, "samplekeys", std::move(samplekeys))));
          if (!write_row.ok())
          {
            LOG(ERROR) << "Insert table statistics failed, tablename"
                       << ccm_table_name.StringView()
                       << ", error: " << write_row.message();
            return false;
          }

          samplekeys.resize(0);
          segment_id+= 1;
          segment_size= 0;
        }

        segment_size+= samplekey->Size();
        std::string s;
        samplekey->Serialize(s);
        samplekeys.append(s);

        if (i == sz - 1)
        {
          std::string row_key(
              to_row_key(ccm_table_name, indexname, version, segment_id));
          auto write_row= cbt_handler_stat.Apply(cbt::SingleRowMutation(
              std::move(row_key),
              cbt::SetCell(cf_, "records",
                           static_cast<int64_t>(sample_pool.first)),
              cbt::SetCell(cf_, "samplekeys", std::move(samplekeys))));
          if (!write_row.ok())
          {
            LOG(ERROR) << "Insert table statistics failed, tablename"
                       << ccm_table_name.StringView()
                       << ", error: " << write_row.message();
            return false;
          }
        }
      }
    }
  }

  {
    cbt::Table cbt_handler_stat_version=
        TableHandler(bigtable_table_statistics_version_name);
    std::string row_key(ccm_table_name.String());
    auto write_row= cbt_handler_stat_version.Apply(cbt::SingleRowMutation(
        std::move(row_key),
        cbt::SetCell(cf_, "version", static_cast<int64_t>(version))));
    if (!write_row.ok())
    {
      LOG(ERROR) << "Upsert table statistics version failed, tablename: "
                 << ccm_table_name.StringView()
                 << ", error: " << write_row.message();
      return false;
    }
  }

  {
    cbt::Table cbt_handler_stat= TableHandler(bigtable_table_statistics_name);

    cbt::BulkMutation bulk;

    std::string row_key_prefix;
    row_key_prefix.append(ccm_table_name.String()).append(":");

    auto row_reader= cbt_handler_stat.ReadRows(
        cbt::RowRange::Prefix(std::move(row_key_prefix)),
        cbt::Filter::Latest(1));
    for (const auto &row : row_reader)
    {
      if (!row.ok())
      {
        LOG(ERROR) << "Read statistics failed, tablename: "
                   << ccm_table_name.StringView()
                   << ", error: " << row.status().message();
        return false;
      }

      const std::string &row_key= row->row_key();
      size_t read_version= 0;
      butil::StringSplitter sp(row_key, ':');
      sp++;
      int rc= sp.to_ulong(&read_version);
      assert(rc == 0);
      (void) rc;
      if (read_version < version)
      {
        bulk.emplace_back(
            cbt::SingleRowMutation(row_key, cbt::DeleteFromRow()));
      }
    }

    auto delete_rows= cbt_handler_stat.BulkApply(std::move(bulk));
    if (!delete_rows.empty())
    {
      LOG(ERROR) << "Delete expired table statistics failed, tablename: "
                 << ccm_table_name.StringView()
                 << ", error: " << delete_rows.front().status().message();
      return false;
    }
  }
  return true;
}

txservice::store::DataStoreHandler::DataStoreOpStatus
MyEloq::BigTableHandler::LoadRangeSlice(
    const txservice::TableName &table_name,
    const txservice::KVCatalogInfo *kv_info, uint32_t partition_id,
    txservice::LoadRangeSliceRequest *load_slice_req)
{
  int64_t leader_term= Sharder::Instance().TryPinNodeGroupData(
      load_slice_req->GetNodeGroupId());
  if (leader_term < 0)
  {
    load_slice_req->SetError();
    return txservice::store::DataStoreHandler::DataStoreOpStatus::Error;
  }

  std::shared_ptr<void> defer_unpin(
      nullptr, [ng_id= load_slice_req->GetNodeGroupId()](void *) {
        Sharder::Instance().UnpinNodeGroupData(ng_id);
      });

  if (leader_term != load_slice_req->LeaderTerm())
  {
    load_slice_req->SetError();
    return txservice::store::DataStoreHandler::DataStoreOpStatus::Error;
  }

  const std::string &kv_table_name= kv_info->GetKvTableName(table_name);
  cbt::Table cbt_handler= TableHandler(kv_table_name);

  cbt::RowRange load_range=
      ToRowRange(load_slice_req->StartKey().GetKey<EloqKey>(),
                 load_slice_req->EndKey().GetKey<EloqKey>());

  auto on_row= [load_slice_req](const cbt::Row &row) {
    uint64_t version_ts=
        static_cast<uint64_t>(RowCell(row, "___version___")
                                  .decode_big_endian_integer<int64_t>()
                                  .value());

    const std::string &deleted_str= RowCell(row, "___deleted___").value();
    assert(deleted_str.size() == 1);
    bool is_deleted= deleted_str.front();

    if (!is_deleted ||
        (is_deleted && load_slice_req->SnapshotTs() < version_ts))
    {
      const std::string &packed_key= row.row_key();
      std::unique_ptr<EloqKey> key= std::make_unique<EloqKey>(
          reinterpret_cast<const unsigned char *>(packed_key.data()),
          packed_key.size());

      std::unique_ptr<EloqRecord> record= std::make_unique<EloqRecord>();

      if (!is_deleted)
      {
        const EloqRecordSchema *rec_schema=
            static_cast<const EloqRecordSchema *>(
                load_slice_req->RecordSchema());
        rec_schema->Encode(RowCell(row, "___payload___").value(),
                           record->encoded_blob_);
        const std::string &unpack_info=
            RowCell(row, "___unpack_info___").value();
        record->SetUnpackInfo(
            reinterpret_cast<const unsigned char *>(unpack_info.data()),
            unpack_info.size());
      }

      load_slice_req->AddDataItem(TxKey(std::move(key)), std::move(record),
                                  version_ts, is_deleted);
    }

    return google::cloud::make_ready_future(true);
  };

  auto on_finish= [this, load_slice_req,
                   defer_unpin](const google::cloud::Status &stream_status) {
    if (stream_status.ok())
    {
      load_slice_req->SetFinish();
    }
    else
    {
      LOG(ERROR) << "Load range slice failed, tablename: "
                 << load_slice_req->TblName().StringView()
                 << ", error: " << stream_status.message();
      load_slice_req->SetError();
    }
  };

  cbt_handler.AsyncReadRows(on_row, on_finish, load_range,
                            cbt::Filter::Latest(1));

  return txservice::store::DataStoreHandler::DataStoreOpStatus::Success;
}

bool MyEloq::BigTableHandler::UpdateRangeSlices(
    const txservice::TableName &table_name, uint64_t slice_version,
    txservice::TxKey range_start_key,
    std::vector<const txservice::StoreSlice *> slices, int32_t partition_id,
    uint64_t range_version)
{
  cbt::Table cbt_handler= TableHandler(bigtable_range_table_name);

  const EloqKey *mono_key= range_start_key.GetKey<EloqKey>();
  if (mono_key == nullptr ||
      mono_key->Type() == txservice::KeyType::NegativeInf)
  {
    mono_key= EloqKey::PackedNegativeInfinity();
  }

  std::string row_key(table_name.String());
  row_key.push_back(':');
  row_key.append(mono_key->PackedValue());

  std::string slice_sizes, slice_keys;

  for (const txservice::StoreSlice *slice : slices)
  {
    uint32_t slice_size= slice->Size();
    slice_sizes.append(reinterpret_cast<const char *>(&slice_size),
                       sizeof(slice_size));
  }

  // The start key of the first slice is the same as the range's start key.
  // When storing slices' start keys, skips the first slice.
  for (size_t i= 1; i < slices.size(); ++i)
  {
    const txservice::StoreSlice *slice= slices[i];
    std::string slice_key;
    slice->StartTxKey().Serialize(slice_key);
    slice_keys.append(slice_key);
  }

  cbt::SingleRowMutation mutation(std::move(row_key));
  mutation.emplace_back(cbt::SetCell(cf_, "___partition_id___",
                                     static_cast<int64_t>(partition_id)));
  mutation.emplace_back(
      cbt::SetCell(cf_, "___version___", static_cast<int64_t>(range_version)));
  mutation.emplace_back(
      cbt::SetCell(cf_, "___slice_sizes___", std::move(slice_sizes)));
  mutation.emplace_back(
      cbt::SetCell(cf_, "___slice_keys___", std::move(slice_keys)));

  auto status= cbt_handler.Apply(mutation);
  if (!status.ok())
  {
    LOG(ERROR) << "Update range slices failed, tablename: "
               << table_name.StringView() << ", error: " << status.message();
    return false;
  }

  return true;
}

bool MyEloq::BigTableHandler::UpsertRanges(
    const txservice::TableName &table_name,
    std::vector<txservice::SplitRangeInfo> range_info, uint64_t version)
{
  cbt::Table cbt_handler= TableHandler(bigtable_range_table_name);

  // for (const auto &[start_key, partition_id, slices] : range_info)
  for (auto &info : range_info)
  {
    const EloqKey *mono_key= info.start_key_.GetKey<EloqKey>();
    if (mono_key == nullptr ||
        mono_key->Type() == txservice::KeyType::NegativeInf)
    {
      mono_key= EloqKey::PackedNegativeInfinity();
    }

    std::string row_key(table_name.String());
    row_key.push_back(':');
    row_key.append(mono_key->PackedValue());

    // We only want to overwrite the range and slice specs if version > data
    // store version. BigTable only support timestamp with milliseconds, but
    // version is microseconds. So check the version manually.
    auto read_row= cbt_handler.ReadRow(row_key, cbt::Filter::Latest(1));
    if (!read_row.ok())
    {
      LOG(ERROR) << "Read table range failed, tablename: "
                 << table_name.StringView()
                 << ", error: " << read_row.status().message();
      return false;
    }

    if (const auto &[exists, row]= read_row.value(); exists)
    {
      uint64_t old_version=
          static_cast<uint64_t>(RowCell(row, "___version___")
                                    .decode_big_endian_integer<int64_t>()
                                    .value());
      if (old_version >= version)
      {
        continue;
      }
    }

    std::string slice_sizes, slice_keys;
    if (info.slices_.empty())
    {
      // empty range should have a default empty slice.
      uint32_t slice_size= 0;
      slice_sizes.append(reinterpret_cast<const char *>(&slice_size),
                         sizeof(slice_size));
    }
    else
    {
      for (const txservice::StoreSlice *slice : info.slices_)
      {
        uint32_t slice_size= slice->Size();
        slice_sizes.append(reinterpret_cast<const char *>(&slice_size),
                           sizeof(slice_size));
      }
      // The start key of the first slice is the same as the range's start key.
      // When storing slices' start keys, skips the first slice.
      for (size_t i= 1; i < info.slices_.size(); ++i)
      {
        const txservice::StoreSlice *slice= info.slices_[i];
        std::string slice_key;
        slice->StartTxKey().Serialize(slice_key);
        slice_keys.append(slice_key);
      }
    }

    auto upsert_row= cbt_handler.Apply(cbt::SingleRowMutation(
        std::move(row_key),
        cbt::SetCell(cf_, "___partition_id___",
                     static_cast<int64_t>(info.partition_id_)),
        cbt::SetCell(cf_, "___version___", static_cast<int64_t>(version)),
        cbt::SetCell(cf_, "___slice_sizes___", std::move(slice_sizes)),
        cbt::SetCell(cf_, "___slice_keys___", std::move(slice_keys))));
    if (!upsert_row.ok())
    {
      LOG(ERROR) << "Upsert table range failed, tablename: "
                 << table_name.StringView()
                 << ", error: " << upsert_row.message();
      return false;
    }
  }

  return true;
}

bool MyEloq::BigTableHandler::DiscoverAllTableNames(
    std::vector<std::string> &norm_name_vec,
    const std::function<void()> *yield_fptr,
    const std::function<void()> *resume_fptr) const
{
  cbt::Table cbt_handler= TableHandler(bigtable_table_catalog_name);
  auto row_reader= cbt_handler.ReadRows(cbt::RowRange::InfiniteRange(),
                                        cbt::Filter::Latest(1));
  for (const auto &row : row_reader)
  {
    if (!row.ok())
    {
      LOG(ERROR) << "Discover all tablenames failed, error: "
                 << row.status().message();
      return false;
    }

    const std::string &tablename= row->row_key();
    norm_name_vec.emplace_back(tablename);
  }
  return true;
}

bool MyEloq::BigTableHandler::UpsertDatabase(std::string_view db,
                                             std::string_view definition) const
{
  cbt::Table cbt_handler= TableHandler(bigtable_database_catalog_name);
  std::string row_key(db);
  std::string col_def(definition);
  auto status= cbt_handler.Apply(cbt::SingleRowMutation(
      std::move(row_key),
      cbt::SetCell(cf_, "definition", std::string(col_def))));
  if (!status.ok())
  {
    LOG(ERROR) << "Upsert database failed, db: " << db
               << ", error: " << status.message();
    return false;
  }

  return true;
}

bool MyEloq::BigTableHandler::DropDatabase(std::string_view db) const
{
  cbt::Table cbt_handler= TableHandler(bigtable_database_catalog_name);
  std::string row_key(db);
  auto status= cbt_handler.Apply(
      cbt::SingleRowMutation(std::move(row_key), cbt::DeleteFromRow()));
  if (!status.ok())
  {
    LOG(ERROR) << "Drop database failed, db: " << db
               << ", error: " << status.message();
    return false;
  }

  return true;
}

bool MyEloq::BigTableHandler::FetchDatabase(
    std::string_view db, std::string &definition, bool &found,
    const std::function<void()> *yield_fptr,
    const std::function<void()> *resume_fptr) const
{
  cbt::Table cbt_handler= TableHandler(bigtable_database_catalog_name);
  std::string row_key(db);
  auto status= cbt_handler.ReadRow(std::move(row_key), cbt::Filter::Latest(1));
  if (!status.ok())
  {
    LOG(ERROR) << "Fetch database failed, db: " << db
               << ", error: " << status.status().message();
    return false;
  }

  const auto &[exists, row]= status.value();
  found= exists;

  if (!found)
  {
    return true;
  }

  definition= RowCell(row, "definition").value();
  return true;
}

bool MyEloq::BigTableHandler::FetchAllDatabase(
    std::vector<std::string> &dbnames, const std::function<void()> *yield_fptr,
    const std::function<void()> *resume_fptr) const
{
  cbt::Table cbt_handler= TableHandler(bigtable_database_catalog_name);
  auto row_reader= cbt_handler.ReadRows(cbt::RowRange::InfiniteRange(),
                                        cbt::Filter::Latest(1));
  for (const auto &row : row_reader)
  {
    if (!row.ok())
    {
      LOG(ERROR) << "Fetch all database failed, error: "
                 << row.status().message();
      return false;
    }

    const std::string &db= row->row_key();
    dbnames.emplace_back(std::move(db));
  }

  return true;
}

bool MyEloq::BigTableHandler::DropKvTable(
    const std::string &kv_table_name) const
{
  std::string total_table_name;
  total_table_name.append(cbt::InstanceName(project_id_, instance_id_));
  total_table_name.append("/tables/");
  total_table_name.append(keyspace_).append(".").append(kv_table_name);
  auto delete_table= admin_client_->DeleteTable(total_table_name);
  if (!delete_table.ok())
  {
    if (delete_table.code() != google::cloud::StatusCode::kNotFound)
    {
      LOG(ERROR) << "Drop kvtable failed, kv_table_name: " << kv_table_name
                 << ", error: " << delete_table.message();
      return false;
    }
  }
  return true;
}

void MyEloq::BigTableHandler::DropKvTableAsync(
    const std::string &kv_table_name) const
{
  assert(false && "BigTable doesn't support DropKvTableAsync");
}

std::unique_ptr<txservice::store::DataStoreScanner>
MyEloq::BigTableHandler::ScanForward(
    const txservice::TableName &table_name, uint32_t ng_id,
    const txservice::TxKey &start_key, bool inclusive, uint8_t key_parts,
    const std::vector<txservice::store::DataStoreSearchCond> &search_cond,
    const txservice::KeySchema *key_schema,
    const txservice::RecordSchema *rec_schema,
    const txservice::KVCatalogInfo *kv_info, bool scan_forward)
{
  return nullptr;
}

bool MyEloq::BigTableHandler::PutArchivesAll(
    uint32_t node_group, const txservice::TableName &table_name,
    const txservice::KVCatalogInfo *kv_info,
    std::vector<txservice::FlushRecord> &batch)
{
  if (batch.size() == 0)
  {
    return true;
  }
  const std::string &kv_table_name= kv_info->GetKvTableName(table_name);

  std::atomic<bool> bulk_apply_ok= true;
  std::vector<google::cloud::future<std::vector<cbt::FailedMutation>>>
      bulk_futures;

  size_t flush_idx= 0;
  while (flush_idx < batch.size() &&
         bulk_apply_ok.load(std::memory_order_acquire))
  {
    if (Sharder::Instance().LeaderTerm(node_group) < 0)
    {
      return false;
    }

    cbt::BulkMutation bulk;
    for (size_t mutate_row= 0;
         mutate_row < max_mutate_rows_ && flush_idx < batch.size();
         ++mutate_row, ++flush_idx)
    {
      FlushRecord &ref= batch[flush_idx];
      const EloqKey &eloq_key= *ref.Key().GetKey<EloqKey>();
      std::string payload, unpack_info;
      // @(htobe64): convert a little-endian val to big-endian, we do the
      // search based on the row_key, this is a string, we need to make sure
      // that we compare the value of number by their literal value rather
      // than string(e.g. string(1234) < string(321))
      uint64_t commit_ts_be= htobe64(UINT64_MAX - ref.commit_ts_);
      std::string row_key;
      row_key.reserve(kv_table_name.length() +
                      eloq_key.PackedValue().length() + sizeof(uint64_t) + 2);
      row_key.append(kv_table_name)
          .append(":")
          .append(eloq_key.PackedValue())
          .append(":")
          .append(reinterpret_cast<const char *>(&commit_ts_be),
                  sizeof(commit_ts_be));

      bool deleted= ref.payload_status_ == RecordStatus::Deleted;
      if (ref.Payload() != nullptr)
      {
        const EloqRecord *typed_payload=
            static_cast<const EloqRecord *>(ref.Payload());
        payload= std::string(typed_payload->EncodedBlobSlice().data(),
                             typed_payload->EncodedBlobSlice().size());
        unpack_info= std::string(typed_payload->UnpackInfoSlice().data(),
                                 typed_payload->UnpackInfoSlice().size());
      }
      bulk.emplace_back(cbt::SingleRowMutation(
          std::move(row_key),
          cbt::SetCell(cf_, "___payload___", std::move(payload)),
          cbt::SetCell(cf_, "___unpack_info___", std::move(unpack_info)),
          cbt::SetCell(cf_, "___version___",
                       static_cast<int64_t>(ref.commit_ts_)),
          cbt::SetCell(cf_, "___deleted___", std::string(1, deleted))));
    }

    SemiAsyncBulkApply(bigtable_mvcc_archive_name, std::move(bulk),
                       bulk_futures, bulk_apply_ok);
  }

  SemiAsyncBulkApplyJoin(bulk_futures);
  return bulk_apply_ok.load(std::memory_order_acquire);
}

bool MyEloq::BigTableHandler::CopyBaseToArchive(
    std::vector<txservice::TxKey> &batch, uint32_t node_group,
    const txservice::TableName &table_name,
    const txservice::TableSchema *table_schema)
{
  const std::string &kv_table_name=
      table_schema->GetKVCatalogInfo()->GetKvTableName(table_name);
  cbt::Table cbt_handler= TableHandler(kv_table_name);

  if (partition_finder == nullptr)
  {
    partition_finder= PartitionFinderFactory::Create();
  }

#ifdef RANGE_PARTITION_ENABLED
  if (!dynamic_cast<RangePartitionFinder *>(partition_finder.get())
           ->Init(tx_service_, node_group))
  {
    LOG(ERROR) << "Failed to init RangePartitionFinder!";
    return false;
  }
#endif

  Partition pk;

  std::vector<FlushRecord> archive_vec;
  archive_vec.reserve(batch.size() * max_mutate_rows_);
  size_t flush_idx= 0;
  while (flush_idx < batch.size() &&
         Sharder::Instance().LeaderTerm(node_group) > 0)
  {
    archive_vec.clear();

    for (size_t mutate_row= 0;
         mutate_row < max_mutate_rows_ && flush_idx < batch.size();
         ++flush_idx)
    {
      const TxKey &key= batch[flush_idx];
      const EloqKey &eloq_key= *batch[flush_idx].GetKey<EloqKey>();

      PartitionResultType rt=
          partition_finder->FindPartition(table_name, key, pk);
      if (rt != PartitionResultType::NORMAL)
      {
        partition_finder->ReleaseReadLocks();
        return false;
      }
      std::string row_key(eloq_key.PackedValue());
      auto row_reader=
          cbt_handler.ReadRows(std::move(row_key), cbt::Filter::Latest(1));

      // copy to archive_vec

      for (const auto &row : row_reader)
      {
        if (!row.ok())
        {
          LOG(ERROR) << "Fetch from archive table failed, err: "
                     << row.status().message();
          return false;
        }
        auto &ref= archive_vec.emplace_back();
        ref.SetKey(TxKey(batch[flush_idx].GetKey<EloqKey>()));
        const EloqRecordSchema *rec_sch= static_cast<const EloqRecordSchema *>(
            table_schema->RecordSchema());
        ref.commit_ts_=
            static_cast<uint64_t>(RowCell(row.value(), "___version___")
                                      .decode_big_endian_integer<int64_t>()
                                      .value());
        const std::string &payload=
            RowCell(row.value(), "___payload___").value();
        const std::string &unpack_info=
            RowCell(row.value(), "___unpack_info___").value();
        const std::string &deleted_str=
            RowCell(row.value(), "___deleted___").value();
        assert(deleted_str.size() == 1);
        bool deleted= deleted_str.front();
        if (!deleted)
        {
          std::unique_ptr<EloqRecord> eloq_rec= std::make_unique<EloqRecord>();
          rec_sch->Encode(payload, eloq_rec->encoded_blob_);
          eloq_rec->SetUnpackInfo(
              reinterpret_cast<const uchar *>(unpack_info.data()),
              unpack_info.size());
          ref.SetPayload(std::move(eloq_rec));
          ref.payload_status_= txservice::RecordStatus::Normal;
        }
        else
        {
          ref.payload_status_= txservice::RecordStatus::Deleted;
        }
      }
    }
    bool ret= PutArchivesAll(node_group, table_name,
                             table_schema->GetKVCatalogInfo(), archive_vec);
    if (!ret)
    {
      partition_finder->ReleaseReadLocks();
      LOG(ERROR) << "Put Archives All failed";
      return false;
    }
  }
  partition_finder->ReleaseReadLocks();
  return true;
}

void MyEloq::BigTableHandler::SemiAsyncBulkApply(
    const std::string &kv_table_name,
    google::cloud::bigtable::BulkMutation bulk,
    std::vector<google::cloud::future<
        std::vector<google::cloud::bigtable::FailedMutation>>> &futures,
    std::atomic<bool> &bulk_apply_ok)
{
  assert(!bulk.empty());

  std::unique_lock<std::mutex> lk(flight_bulks_mux_);
  flight_bulks_cv_.wait(
      lk, [this]() { return flight_bulks_ < max_flight_bulks_; });

  cbt::Table cbt_handler= TableHandler(kv_table_name);
  flight_bulks_++;

  auto on_bulk_apply=
      [this, &bulk_apply_ok, &futures, idx= futures.size(),
       bulk_size= bulk.size(),
       &kv_table_name](google::cloud::future<
                       std::vector<google::cloud::bigtable::FailedMutation>>
                           f) {
        std::unique_lock<std::mutex> lk(flight_bulks_mux_);

        std::vector<google::cloud::bigtable::FailedMutation> failures= f.get();
        if (!failures.empty())
        {
          LOG(ERROR) << "PutAll bulk apply error: "
                     << failures.front().status().message();
          bulk_apply_ok.store(false, std::memory_order_release);
        }
        else
        {
          if (metrics::enable_kv_metrics)
          {
            if (kv_table_name == bigtable_mvcc_archive_name)
            {
              metrics::kv_meter->Collect(metrics::NAME_KV_FLUSH_ROWS_TOTAL,
                                         bulk_size, "archive");
            }
            else
            {
              metrics::kv_meter->Collect(metrics::NAME_KV_FLUSH_ROWS_TOTAL,
                                         bulk_size, "base");
            }
          }
        }

        std::swap(futures[idx], futures.back());
        futures.pop_back();
        flight_bulks_--;
        flight_bulks_cv_.notify_all();
      };

  futures.emplace_back(cbt_handler.AsyncBulkApply(std::move(bulk)))
      .then(on_bulk_apply);
}

void MyEloq::BigTableHandler::SemiAsyncBulkApplyJoin(
    std::vector<google::cloud::future<
        std::vector<google::cloud::bigtable::FailedMutation>>> &futures)
{
  std::unique_lock<std::mutex> lk(flight_bulks_mux_);
  flight_bulks_cv_.wait(lk, [&futures]() { return futures.empty(); });
}

bool MyEloq::BigTableHandler::FetchVisibleArchive(
    const txservice::TableName &table_name,
    const txservice::KVCatalogInfo *kv_info, const txservice::TxKey &key,
    const uint64_t upper_bound_ts, txservice::TxRecord &rec,
    txservice::RecordStatus &rec_status, uint64_t &commit_ts)
{
  cbt::Table cbt_handler= TableHandler(bigtable_mvcc_archive_name);
  const std::string &kv_table_name= kv_info->GetKvTableName(table_name);

  const EloqKey &eloq_key= *key.GetKey<EloqKey>();

  // @(UINT64_MAX - upper_bound_ts) : we need the most recent archive,
  // obviouly its value is largest, using reverse scan is easiest way but
  // bigtable is not yet supported on c++. that's why we using MAXVAL - BOUND
  // convert max value to min value so that we can find it in returned iter
  // at idx-0.
  //
  // @(htobe64): convert a little-endian val to big-endian, we do the search
  // based on the row_key, this is a string, we need to make sure that we
  // compare the value of number by their literal value rather than
  // string(e.g. string(1234) < string(321)).
  uint64_t lower_bound_be= htobe64(UINT64_MAX - upper_bound_ts);
  uint64_t upper= UINT64_MAX;

  std::string row_key_prefix;
  row_key_prefix.reserve(kv_table_name.length() +
                         eloq_key.PackedValue().length() + 2); // 2 = : * 2
  row_key_prefix.append(kv_table_name)
      .append(":")
      .append(eloq_key.PackedValue())
      .append(":");

  auto row_reader= cbt_handler.ReadRows(
      cbt::RowRange::Range(
          row_key_prefix +
              std::string(reinterpret_cast<const char *>(&lower_bound_be),
                          sizeof(lower_bound_be)),
          row_key_prefix + std::string(reinterpret_cast<const char *>(&upper),
                                       sizeof(upper))),
      cbt::Filter::Latest(1));

  EloqRecord *temp_rec= static_cast<EloqRecord *>(&rec);

  bool exists= false;
  auto iter= row_reader.begin(); // RowReader return a single-pass iterator.
  while (iter != row_reader.end())
  {
    auto &read_row= *iter;
    if (!read_row.ok())
    {
      LOG(ERROR) << "Fetch from archive table failed, err: "
                 << read_row.status().message();
      return false;
    }

    const std::string &row_key= read_row->row_key();
    std::string fetch_key= row_key.substr(
        kv_table_name.length() + 1,
        row_key.length() - kv_table_name.length() - 2 - sizeof(uint64_t));
    if (fetch_key == eloq_key.PackedValue())
    {
      exists= true;
      break;
    }

    iter++;
  }

  if (exists)
  {
    auto &read_row= *iter;
    commit_ts= static_cast<uint64_t>(RowCell(read_row.value(), "___version___")
                                         .decode_big_endian_integer<int64_t>()
                                         .value());
    const std::string &deleted_str=
        RowCell(read_row.value(), "___deleted___").value();
    assert(deleted_str.size() == 1);
    bool deleted= deleted_str.front();
    if (!deleted)
    {
      const std::string &payload=
          RowCell(read_row.value(), "___payload___").value();
      const std::string &unpack_info=
          RowCell(read_row.value(), "___unpack_info___").value();
      temp_rec->SetEncodedBlob(
          reinterpret_cast<const unsigned char *>(payload.data()),
          payload.length());
      temp_rec->SetUnpackInfo(
          reinterpret_cast<const uchar *>(unpack_info.data()),
          unpack_info.size());
      rec_status= txservice::RecordStatus::Normal;
    }
    else
    {
      rec_status= txservice::RecordStatus::Deleted;
    }
  }
  else
  {
    rec_status= txservice::RecordStatus::Deleted;
  }

  return true;
}

bool MyEloq::BigTableHandler::FetchArchives(
    const txservice::TableName &table_name,
    const txservice::KVCatalogInfo *kv_info, const txservice::TxKey &key,
    std::vector<txservice::VersionTxRecord> &archives, uint64_t from_ts)
{
  // MVCC
  return true;
}

bool MyEloq::BigTableHandler::DeleteOutOfRangeData(
    const txservice::TableName &table_name, int32_t partition_id,
    const txservice::TxKey *start_key,
    const txservice::TableSchema *table_schema)
{
  // BigTable manages data partition automatically.
  return true;
}

bool MyEloq::BigTableHandler::GetNextRangePartitionId(
    const txservice::TableName &tablename, uint32_t range_cnt,
    int32_t &out_next_partition_id, int retry_count)
{
  // Unlike Cassandra and DynamoDB, BigTable support an atomical command like
  // Redis INCR counter increment, thus retry is unnecessary.
  (void) retry_count;

  cbt::Table cbt_handler= TableHandler(bigtable_last_range_id_name);
  std::string row_key(tablename.String());

  auto incr_id= cbt_handler.ReadModifyWriteRow(
      std::move(row_key), cbt::ReadModifyWriteRule::IncrementAmount(
                              cf_, "last_partition_id", range_cnt));
  if (!incr_id.ok())
  {
    LOG(ERROR) << "Get next range partition id failed, tablename: "
               << tablename.StringView()
               << ", error: " << incr_id.status().message();
    return false;
  }

  out_next_partition_id=
      static_cast<int32_t>(RowCell(incr_id.value(), "last_partition_id")
                               .decode_big_endian_integer<int64_t>()
                               .value()) -
      range_cnt + 1;
  return true;
}

std::string MyEloq::BigTableHandler::CreateKVCatalogInfo(
    const txservice::TableSchema *table_schema) const
{
  boost::uuids::random_generator generator;

  BigTableCatalogInfo bigtable_info;
  bigtable_info.kv_table_name_=
      std::string("t").append(boost::lexical_cast<std::string>(generator()));

  std::vector<txservice::TableName> index_names= table_schema->IndexNames();
  for (auto idx_it= index_names.begin(); idx_it < index_names.end(); ++idx_it)
  {
    if (idx_it->Type() == txservice::TableType::Secondary)
    {
      bigtable_info.kv_index_names_.emplace(
          *idx_it, std::string("i").append(
                       boost::lexical_cast<std::string>(generator())));
    }
    else
    {
      assert((idx_it->Type() == txservice::TableType::UniqueSecondary));
      bigtable_info.kv_index_names_.emplace(
          *idx_it, std::string("u").append(
                       boost::lexical_cast<std::string>(generator())));
    }
  }
  return bigtable_info.Serialize();
}

txservice::KVCatalogInfo::uptr
MyEloq::BigTableHandler::DeserializeKVCatalogInfo(
    const std::string &kv_info_str, size_t &offset) const
{
  BigTableCatalogInfo::uptr bigtable_info=
      std::make_unique<BigTableCatalogInfo>();
  bigtable_info->Deserialize(kv_info_str.data(), offset);
  return bigtable_info;
}

std::string MyEloq::BigTableHandler::CreateNewKVCatalogInfo(
    const txservice::TableName &table_name,
    const txservice::TableSchema *current_table_schema,
    txservice::AlterTableInfo &alter_table_info)
{
  // Get current kv catalog info.
  const BigTableCatalogInfo *current_bigtable_catalog_info=
      static_cast<const BigTableCatalogInfo *>(
          current_table_schema->GetKVCatalogInfo());

  std::string new_kv_info, kv_table_name, new_kv_index_names;

  /* kv table name using current table name */
  kv_table_name= current_bigtable_catalog_info->kv_table_name_;
  uint32_t kv_val_len= kv_table_name.length();
  new_kv_info.append(reinterpret_cast<char *>(&kv_val_len), sizeof(kv_val_len))
      .append(kv_table_name.data(), kv_val_len);

  /* kv index names using new schema index names */
  // 1. remove dropped index kv name
  bool dropped= false;
  for (auto kv_index_it=
           current_bigtable_catalog_info->kv_index_names_.cbegin();
       kv_index_it != current_bigtable_catalog_info->kv_index_names_.cend();
       ++kv_index_it)
  {
    // Check if the index will be dropped.
    dropped= false;
    for (auto drop_index_it= alter_table_info.index_drop_names_.cbegin();
         alter_table_info.index_drop_count_ > 0 &&
         drop_index_it != alter_table_info.index_drop_names_.cend();
         drop_index_it++)
    {
      if (kv_index_it->first == drop_index_it->first)
      {
        dropped= true;
        // Remove dropped index
        alter_table_info.index_drop_names_[kv_index_it->first]=
            kv_index_it->second;
        break;
      }
    }
    if (!dropped)
    {
      new_kv_index_names.append(kv_index_it->first.String())
          .append(" ")
          .append(kv_index_it->second)
          .append(" ");
    }
  }
  assert(alter_table_info.index_drop_names_.size() ==
         alter_table_info.index_drop_count_);

  // 2. add new index
  boost::uuids::random_generator generator;
  for (auto add_index_it= alter_table_info.index_add_names_.cbegin();
       alter_table_info.index_add_count_ > 0 &&
       add_index_it != alter_table_info.index_add_names_.cend();
       add_index_it++)
  {
    // get index kv table name
    std::string add_index_kv_name;
    if (add_index_it->first.Type() == txservice::TableType::Secondary)
    {
      add_index_kv_name= std::string("i").append(
          boost::lexical_cast<std::string>(generator()));
    }
    else
    {
      assert(add_index_it->first.Type() ==
             txservice::TableType::UniqueSecondary);
      add_index_kv_name= std::string("u").append(
          boost::lexical_cast<std::string>(generator()));
    }

    new_kv_index_names.append(add_index_it->first.String())
        .append(" ")
        .append(add_index_kv_name.data())
        .append(" ");

    // set index kv table name
    alter_table_info.index_add_names_[add_index_it->first]= add_index_kv_name;
  }
  assert(alter_table_info.index_add_names_.size() ==
         alter_table_info.index_add_count_);

  /* create final new kv info */
  kv_val_len= new_kv_index_names.size();
  new_kv_info.append(reinterpret_cast<char *>(&kv_val_len), sizeof(kv_val_len))
      .append(new_kv_index_names.data(), kv_val_len);

  return new_kv_info;
}

bool MyEloq::BigTableHandler::UpsertCatalog(
    const txservice::TableSchema *table_schema, uint64_t write_time)
{
  cbt::Table cbt_handler= TableHandler(bigtable_table_catalog_name);

  std::string_view base_table_name=
      table_schema->GetBaseTableName().StringView();

  const std::string &catalog_image= table_schema->SchemaImage();
  std::string frm, kv_info, key_schemas_ts_str;
  DeserializeSchemaImage(catalog_image, frm, kv_info, key_schemas_ts_str);

  const BigTableCatalogInfo *bigtable_info=
      static_cast<const BigTableCatalogInfo *>(
          table_schema->GetKVCatalogInfo());
  std::string kv_table_name= bigtable_info->kv_table_name_;
  std::string key_schemas_ts;
  key_schemas_ts.append(std::to_string(table_schema->KeySchema()->SchemaTs()))
      .append(" ");

  std::string kv_index_names;
  if (bigtable_info->kv_index_names_.size() != 0)
  {
    for (auto it= bigtable_info->kv_index_names_.cbegin();
         it != bigtable_info->kv_index_names_.cend(); ++it)
    {
      kv_index_names.append(it->first.StringView())
          .append(" ")
          .append(it->second)
          .append(" ");

      auto *sk_schema= table_schema->IndexKeySchema(it->first);
      key_schemas_ts.append(it->first.StringView())
          .append(" ")
          .append(std::to_string(sk_schema->SchemaTs()))
          .append(" ");
    }
    kv_index_names.pop_back();
  }
  else
  {
    kv_index_names.clear();
  }
  key_schemas_ts.pop_back();

  std::string row_key(base_table_name);
  auto status= cbt_handler.Apply(cbt::SingleRowMutation(
      std::move(row_key), cbt::SetCell(cf_, "content", std::move(frm)),
      cbt::SetCell(cf_, "version",
                   static_cast<int64_t>(table_schema->Version())),
      cbt::SetCell(cf_, "kvtablename", std::move(kv_table_name)),
      cbt::SetCell(cf_, "kvindexname", std::move(kv_index_names)),
      cbt::SetCell(cf_, "keyschemasts", std::move(key_schemas_ts))));
  if (!status.ok())
  {
    LOG(ERROR) << "Upsert catalog failed, tablename: " << base_table_name
               << ", error: " << status.message();
    return false;
  }

  return true;
}

bool MyEloq::BigTableHandler::DeleteCatalog(
    const txservice::TableName &base_table_name, uint64_t write_time)
{
  assert(base_table_name.Type() == txservice::TableType::Primary);

  cbt::Table cbt_handler= TableHandler(bigtable_table_catalog_name);

  std::string row_key(base_table_name.String());

  auto status= cbt_handler.Apply(
      cbt::SingleRowMutation(std::move(row_key), cbt::DeleteFromRow()));
  if (!status.ok())
  {
    if (status.code() != google::cloud::StatusCode::kNotFound)
    {
      LOG(ERROR) << "Delete catalog failed, tablename: "
                 << base_table_name.StringView()
                 << ", error: " << status.message();
      return false;
    }
  }

  return true;
}

bool MyEloq::BigTableHandler::CreateKvTable(const std::string &kv_table_name)
{
  google::bigtable::admin::v2::Table table;

  auto &column_families= *table.mutable_column_families();
  auto &column_family= column_families[cf_];

  // In bigtable, gc policies are set at the column family level, we can't
  // set a cell-level gc policy, so we follow
  // https://cloud.google.com/bigtable/docs/gc-cell-level simulate
  // cell-level TTL.
  auto gc_rules=
      column_family.mutable_gc_rule()->mutable_union_()->add_rules();
  gc_rules->set_max_num_versions(1);
  gc_rules->mutable_max_age()->set_seconds(1);

  std::string instance_name= cbt::InstanceName(project_id_, instance_id_);
  std::string table_id;
  table_id.append(keyspace_).append(".").append(kv_table_name);

  StatusOr<google::bigtable::admin::v2::Table> create_table=
      admin_client_->CreateTable(instance_name, table_id, table);
  switch (create_table.status().code())
  {
  case google::cloud::StatusCode::kOk:
  case google::cloud::StatusCode::kAlreadyExists:
    return true;
  default:
    LOG(ERROR) << "Create KvTable failed, tablename: " << table_id
               << ", error: " << create_table.status().message();
    return false;
  }
}

bool MyEloq::BigTableHandler::InitTableRanges(
    const txservice::TableName &table_name, uint64_t version)
{
  cbt::Table cbt_handler= TableHandler(bigtable_range_table_name);

  const std::string &start_key=
      EloqKey::PackedNegativeInfinity()->PackedValue();

  std::string row_key;
  row_key.append(table_name.StringView()).append(":").append(start_key);

  uint32_t zero_slice_size= 0;

  auto status= cbt_handler.Apply(cbt::SingleRowMutation(
      std::move(row_key),
      cbt::SetCell(
          cf_, "___partition_id___",
          static_cast<int64_t>(LastRangePartitionIdInitValue(table_name))),
      cbt::SetCell(cf_, "___version___", static_cast<int64_t>(version)),
      cbt::SetCell(cf_, "___slice_keys___", ""),
      cbt::SetCell(
          cf_, "___slice_sizes___",
          std::string(reinterpret_cast<const char *>(&zero_slice_size),
                      sizeof(zero_slice_size)))));
  if (!status.ok())
  {
    LOG(ERROR) << "Init Table Ranges failed, tablename: "
               << table_name.StringView() << ", error: " << status.message();
    return false;
  }

  return true;
}

bool MyEloq::BigTableHandler::DeleteTableRanges(
    const txservice::TableName &table_name)
{
  cbt::Table cbt_handler= TableHandler(bigtable_range_table_name);

  std::string row_key_prefix;
  row_key_prefix.append(table_name.String()).append(":");
  auto row_reader=
      cbt_handler.ReadRows(cbt::RowRange::Prefix(std::move(row_key_prefix)),
                           cbt::Filter::Latest(1));

  cbt::BulkMutation bulk;

  for (const auto &row : row_reader)
  {
    if (!row.ok())
    {
      LOG(ERROR) << "Read table ranges failed, tablename: "
                 << table_name.StringView()
                 << ", error: " << row.status().message();
      return false;
    }

    const std::string &row_key= row.value().row_key();
    bulk.emplace_back(cbt::SingleRowMutation(row_key, cbt::DeleteFromRow()));
  }

  const auto &failures= cbt_handler.BulkApply(bulk);
  if (!failures.empty())
  {
    LOG(ERROR) << "Delete table ranges failed, tablename: "
               << table_name.StringView()
               << ", error: " << failures.front().status().message();
    return false;
  }
  else
  {
    return true;
  }
}

bool MyEloq::BigTableHandler::InitTableLastRangePartitionId(
    const txservice::TableName &table_name)
{
  cbt::Table cbt_handler= TableHandler(bigtable_last_range_id_name);

  std::string row_key(table_name.String());

  auto status= cbt_handler.Apply(cbt::SingleRowMutation(
      std::move(row_key),
      cbt::SetCell(
          cf_, "last_partition_id",
          static_cast<int64_t>(LastRangePartitionIdInitValue(table_name)))));
  if (!status.ok())
  {
    LOG(ERROR) << "InitTableLastRangePartitionId failed, tablename: "
               << table_name.StringView() << ", error: " << status.message();
    return false;
  }

  return true;
}

bool MyEloq::BigTableHandler::DeleteTableLastRangePartitionId(
    const txservice::TableName &table_name)
{
  cbt::Table cbt_handler= TableHandler(bigtable_last_range_id_name);

  std::string row_key(table_name.String());
  google::cloud::bigtable::SingleRowMutation mutation(std::move(row_key),
                                                      cbt::DeleteFromRow());
  auto status= cbt_handler.Apply(std::move(mutation));

  switch (status.code())
  {
  case google::cloud::StatusCode::kOk:
  case google::cloud::StatusCode::kNotFound:
    return true;
  default:
    LOG(ERROR) << "DeleteTableLastRangePartitionId failed, tablename: "
               << table_name.StringView() << ", error: " << status.message();
    return false;
  }
}

bool MyEloq::BigTableHandler::DeleteSeqence(
    const txservice::TableName &base_table_name)
{
  const BigTableCatalogInfo *sequence_table=
      static_cast<const BigTableCatalogInfo *>(
          Sequences::GetTableSchema()->GetKVCatalogInfo());

  cbt::Table cbt_handler= TableHandler(sequence_table->kv_table_name_);

  std::string row_key(base_table_name.String());
  google::cloud::bigtable::SingleRowMutation mutation(row_key,
                                                      cbt::DeleteFromRow());

  auto status= cbt_handler.Apply(std::move(mutation));
  switch (status.code())
  {
  case google::cloud::StatusCode::kOk:
  case google::cloud::StatusCode::kNotFound:
    return true;
  default:
    LOG(ERROR) << "Delete Sequence failed, tablename: "
               << base_table_name.StringView()
               << ", error: " << status.message();
    return false;
  }
}

bool MyEloq::BigTableHandler::DeleteTableStatistics(
    const txservice::TableName &base_table_name)
{
  {
    google::bigtable::admin::v2::DropRowRangeRequest drop_rows_req;

    std::string table_id;
    table_id.append(keyspace_).append(".").append(
        bigtable_table_statistics_name);
    drop_rows_req.set_name(
        cbt::TableName(project_id_, instance_id_, table_id));

    std::string row_key_prefix;
    row_key_prefix.append(base_table_name.StringView()).append(":");
    drop_rows_req.set_row_key_prefix(std::move(row_key_prefix));

    auto status= admin_client_->DropRowRange(drop_rows_req);
    if (!status.ok())
    {
      LOG(ERROR) << "Delete rows from " << bigtable_table_statistics_name
                 << ", with prefix " << base_table_name.StringView()
                 << " failed, error: " << status.message();
      return false;
    }
  }

  {
    cbt::Table cbt_handler=
        TableHandler(bigtable_table_statistics_version_name);
    std::string row_key(base_table_name.StringView());
    auto status= cbt_handler.Apply(
        cbt::SingleRowMutation(std::move(row_key), cbt::DeleteFromRow()));
    switch (status.code())
    {
    case google::cloud::StatusCode::kOk:
    case google::cloud::StatusCode::kNotFound:
      return true;
    default:
      LOG(ERROR) << "Delete Table Statistics failed, tablename: "
                 << base_table_name.StringView()
                 << ", error: " << status.message();
      return false;
    }
  }
}

google::cloud::bigtable::Table
MyEloq::BigTableHandler::TableHandler(const std::string &table_name) const
{
  std::string table_id;
  table_id.append(keyspace_).append(".").append(table_name);
  cbt::Table cbt_handler(data_connection_,
                         google::cloud::bigtable::TableResource(
                             project_id_, instance_id_, std::move(table_id)));
  return cbt_handler;
}

const google::cloud::bigtable::Cell &
MyEloq::BigTableHandler::RowCell(const google::cloud::bigtable::Row &row,
                                 const std::string &col_name)
{
  auto iter=
      std::find_if(row.cells().begin(), row.cells().end(),
                   [col_name](const google::cloud::bigtable::Cell &cell) {
                     return cell.column_qualifier() == col_name;
                   });
  assert(iter != row.cells().end());
  return *iter;
}

google::cloud::bigtable::RowRange
MyEloq::BigTableHandler::ToRowRange(const EloqKey *start_key,
                                    const EloqKey *end_key)
{
  bool start_neg_inf=
      start_key == nullptr || start_key->Type() == KeyType::NegativeInf;
  bool end_pos_inf=
      end_key == nullptr || end_key->Type() == KeyType::PositiveInf;

  if (start_neg_inf && end_pos_inf)
  {
    return cbt::RowRange::InfiniteRange();
  }
  else if (!start_neg_inf && end_pos_inf)
  {
    return cbt::RowRange::StartingAt(start_key->PackedValue());
  }
  else if (start_neg_inf && !end_pos_inf)
  {
    return cbt::RowRange::EndingAt(end_key->PackedValue());
  }
  else
  {
    return cbt::RowRange::RightOpen(start_key->PackedValue(),
                                    end_key->PackedValue());
  }
}

int MyEloq::BigTableHandler::LastRangePartitionIdInitValue(
    const txservice::TableName &table_name)
{
  if (table_name.StringView() == Sequences::mysql_seq_string)
  {
    return 0;
  }
  else
  {
    return Partition::InitialPartitionId(table_name.StringView());
  }
}

MyEloq::BigTableCatalogInfo::BigTableCatalogInfo(
    const std::string &table_name, const std::string &index_names)
{
  std::stringstream ss(index_names);
  std::istream_iterator<std::string> begin(ss);
  std::istream_iterator<std::string> end;
  std::vector<std::string> tokens(begin, end);
  for (auto it= tokens.begin(); it != tokens.end(); ++it)
  {
    bool is_unique_sk= std::next(it, 1)->front() == 'u';
    std::string kv_index_name;
    txservice::TableName index_name(
        *it, is_unique_sk ? txservice::TableType::UniqueSecondary
                          : txservice::TableType::Secondary);
    kv_index_name.append(*(++it));
    kv_index_names_.try_emplace(index_name, kv_index_name);
  }
  kv_table_name_= table_name;
}

void MyEloq::BigTableCatalogInfo::Deserialize(const char *buf, size_t &offset)
{
  if (buf[0] == '\0')
  {
    return;
  }
  uint32_t *len_ptr= (uint32_t *) (buf + offset);
  uint32_t len_val= *len_ptr;
  offset+= sizeof(uint32_t);

  kv_table_name_.clear();
  kv_table_name_.reserve(len_val);

  kv_table_name_.append(buf + offset, len_val);
  offset+= len_val;

  len_ptr= (uint32_t *) (buf + offset);
  len_val= *len_ptr;
  offset+= sizeof(uint32_t);
  if (len_val != 0)
  {
    std::string index_names(buf + offset, len_val);
    offset+= len_val;
    std::stringstream ss(index_names);
    std::istream_iterator<std::string> begin(ss);
    std::istream_iterator<std::string> end;
    std::vector<std::string> tokens(begin, end);
    for (auto it= tokens.begin(); it != tokens.end(); ++it)
    {
      bool is_unique_sk= std::next(it, 1)->front() == 'u';
      std::string kv_index_name;
      txservice::TableName index_name(
          *it, is_unique_sk ? txservice::TableType::UniqueSecondary
                            : txservice::TableType::Secondary);
      kv_index_name.append(*(++it));
      kv_index_names_.try_emplace(index_name, kv_index_name);
    }
  }
  else
  {
    kv_index_names_.clear();
  }
}

std::string MyEloq::BigTableCatalogInfo::Serialize() const
{
  std::string str;
  size_t len_sizeof= sizeof(uint32_t);
  uint32_t len_val= (uint32_t) kv_table_name_.size();
  char *len_ptr= reinterpret_cast<char *>(&len_val);
  str.append(len_ptr, len_sizeof);
  str.append(kv_table_name_.data(), len_val);

  std::string index_names;
  if (kv_index_names_.size() != 0)
  {
    for (auto it= kv_index_names_.cbegin(); it != kv_index_names_.cend(); ++it)
    {
      index_names.append(it->first.String())
          .append(" ")
          .append(it->second)
          .append(" ");
    }
    index_names.substr(0, index_names.size() - 1);
  }
  else
  {
    index_names.clear();
  }
  len_val= (uint32_t) index_names.size();
  str.append(len_ptr, len_sizeof);
  str.append(index_names.data(), len_val);
  return str;
}
