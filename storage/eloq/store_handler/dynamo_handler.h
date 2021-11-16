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
#pragma once

#include <aws/core/Aws.h>
#include <aws/core/client/AsyncCallerContext.h>
#include <aws/dynamodb/DynamoDBClient.h>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector> // std::vector

#include "tx_service/include/schema.h"
#include "tx_service/include/store/data_store_handler.h"
#include "tx_service/include/tx_execution.h"
#include "tx_service/include/catalog_key_record.h"
#include "tx_service/include/util.h"
#include "tx_service/include/tx_worker_pool.h"
#include "eloq_catalog_factory.h"
#include "partition.h"

#include "bthread/timer_thread.h"

namespace MyEloq
{
struct DynamoCatalogInfo;
extern const std::string dynamo_partition_key_attribute_name;
extern const std::string dynamo_sort_key_attribute_name;

extern std::unique_ptr<metrics::Meter> dynamo_metrics_meter;

inline uint64_t SwapEndiannessU64(uint64_t val)
{
  return ((val & 0x00000000000000FFULL) << 56) |
         ((val & 0x000000000000FF00ULL) << 40) |
         ((val & 0x0000000000FF0000ULL) << 24) |
         ((val & 0x00000000FF000000ULL) << 8) |
         ((val & 0x000000FF00000000ULL) >> 8) |
         ((val & 0x0000FF0000000000ULL) >> 24) |
         ((val & 0x00FF000000000000ULL) >> 40) |
         ((val & 0xFF00000000000000ULL) >> 56);
}

inline uint64_t ToBigEndian(uint64_t value)
{
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
  return SwapEndiannessU64(value);
#else
  return value;
#endif
}

inline uint64_t ToNativeEndian(uint64_t big_endian_value)
{
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
  return SwapEndiannessU64(big_endian_value);
#else
  return big_endian_value;
#endif
}

struct RangeTableSortKey
{
  static void FromByteBuffer(const Aws::Utils::ByteBuffer &byte_buffer,
                             std::unique_ptr<EloqKey> &mono_key,
                             uint64_t &segment_id)
  {
    const unsigned char *raw_data= byte_buffer.GetUnderlyingData();
    size_t raw_data_len= byte_buffer.GetLength();

    assert(raw_data_len > sizeof(uint64_t));

    segment_id= ToNativeEndian(*reinterpret_cast<const uint64_t *>(
        raw_data + (raw_data_len - sizeof(uint64_t))));

    size_t key_data_len= raw_data_len - sizeof(uint64_t);
    std::string key_buf;
    key_buf.reserve(key_data_len);

    assert(key_data_len % 9 == 0);
    size_t cell_cnt= key_data_len / 9;

    for (size_t cell_idx= 0; cell_idx < cell_cnt; ++cell_idx)
    {
      size_t offset= cell_idx * 9;
      if (cell_idx + 1 != cell_cnt)
      {
        size_t padding_cnt= static_cast<size_t>(
            0xFF - *reinterpret_cast<const uint8_t *>(raw_data + offset + 8));
        assert(padding_cnt == 0);
        (void) padding_cnt;

        key_buf.append(reinterpret_cast<const char *>(raw_data + offset), 8);
      }
      else
      {
        // last cell
        size_t padding_cnt= static_cast<size_t>(
            0xFF - *reinterpret_cast<const uint8_t *>(raw_data + offset + 8));
        key_buf.append(reinterpret_cast<const char *>(raw_data + offset),
                       8 - padding_cnt);
      }
    }

    if (key_buf.size() == 1 && key_buf.at(0) == 0x00)
    {
      mono_key= nullptr;
    }
    else
    {
      mono_key= std::make_unique<EloqKey>(
          reinterpret_cast<const uchar *>(key_buf.data()), key_buf.size());
    }
  }

  /*
   memcomparable format: guarantees the encoded value is in ascending order for
   comparison.

   For example:
   []+[segment_id(8 bytes)] -> [0, 0, 0, 0, 0, 0, 0, 0, 247]+[segment_id(8
   bytes)]

   [1, 2, 3]+[segment_id(8 bytes)] -> [1, 2, 3, 0, 0, 0, 0, 0,
   250]+[segment_id(8 bytes)]

   [1, 2, 3, 0]+[segment_id(8 bytes)] -> [1, 2, 3, 0, 0, 0, 0, 0,
   251]+[segment_id(8 bytes)]

   [1, 2, 3, 4, 5, 6, 7, 8]+[segment_id(8 bytes)] -> [1, 2, 3, 4, 5, 6, 7, 8,
   255, 0, 0, 0, 0, 0, 0, 0, 0, 247]+[segment_id(8 bytes)]

   [1, 2, 3, 4, 5, 6, 7, 8, 9]+[segment_id(8 bytes)] -> [1, 2, 3, 4, 5, 6, 7,
   8, 255, 9, 0, 0, 0, 0, 0, 0, 0, 248]+[segment_id(8 bytes)]
  */

  Aws::Utils::ByteBuffer ToByteBuffer()
  {
    size_t bytes_size= mono_key_->Size();

    std::string key_buf;
    key_buf.reserve((bytes_size / 8 + 1) * 9 + sizeof(uint64_t));

    const char *mono_key_raw_data= mono_key_->PackedValue().data();

    size_t idx= 0;
    while (idx <= mono_key_->Size())
    {
      size_t remain= mono_key_->Size() - idx;
      if (remain >= 8)
      {
        key_buf.append(mono_key_raw_data, 8);
        // padding count
        key_buf.append(1, 0xFF);
        mono_key_raw_data+= 8;
      }
      else
      {
        size_t append_size= mono_key_->Size() - idx;
        assert(append_size < 8);

        key_buf.append(mono_key_raw_data, append_size);
        // padding count
        uint8_t pading_cnt= 8 - (uint8_t) append_size;
        key_buf.append(pading_cnt, 0x00);
        key_buf.append(1, (0xFF - (uint8_t) pading_cnt));

        mono_key_raw_data+= append_size;
      }

      idx+= 8;
    }

    uint64_t big_endian_value= ToBigEndian(segment_id_);

    key_buf.append(reinterpret_cast<const char *>(&big_endian_value),
                   sizeof(uint64_t));
    return Aws::Utils::ByteBuffer(
        reinterpret_cast<const uchar *>(key_buf.data()), key_buf.size());
  }

  const EloqKey *mono_key_{nullptr};
  uint64_t segment_id_{0};
};

class DynamoHandler : public txservice::store::DataStoreHandler
{
public:
  DynamoHandler(const std::string &key_space, const std::string &endpoint,
                const std::string &region,
                const std::string &aws_access_key_id,
                const std::string &aws_secret_key, bool bootstrap,
                bool ddl_skip_kv, int worker_pool_size= 10);

  ~DynamoHandler();

  /**
   * Connect to DynamoDB service
   */
  bool Connect() override;

  void ScheduleTimerTasks() override;

  /**
   * Initialize cluster config based on the based in ips and ports. This
   * should only be called during bootstrap.
   */
  bool InitializeClusterConfig(
      const std::unordered_map<uint32_t, std::vector<NodeConfig>> &ng_configs)
      override;

  /**
   * Read cluster config from kv store cluster config table.
   */
  bool ReadClusterConfig(
      std::unordered_map<uint32_t, std::vector<txservice::NodeConfig>>
          &ng_configs,
      uint64_t &version, bool &uninitialized) override;

  bool UpdateClusterConfig(
      const std::unordered_map<uint32_t, std::vector<txservice::NodeConfig>>
          &new_cnf,
      uint64_t version) override;

  /**
   * @brief flush entries in \@param batch to base table or skindex table in
   * data store, stop and return false if node_group is not longer leader.
   * @param batch
   * @param table_name base table name or sk index name
   * @param table_schema
   * @param schema_ts
   * @param node_group
   * @return whether all entries are written to data store successfully
   */
  bool PutAll(std::vector<txservice::FlushRecord> &batch,
              const txservice::TableName &table_name,
              const txservice::TableSchema *table_schema,
              uint32_t node_group) override;

  void UpsertTable(const txservice::TableSchema *old_table_schema,
                   const txservice::TableSchema *table_schema,
                   txservice::OperationType op_type, uint64_t write_time,
                   txservice::NodeGroupId ng_id, int64_t tx_term,
                   txservice::CcHandlerResult<txservice::Void> *hd_res,
                   const txservice::AlterTableInfo *alter_table_info= nullptr,
                   txservice::CcRequestBase *cc_req= nullptr,
                   txservice::CcShard *ccs= nullptr,
                   txservice::CcErrorCode *err_code= nullptr) override;

  void FetchTableCatalog(const txservice::TableName &ccm_table_name,
                         FetchCatalogCc *fetch_cc) override;

  bool Read(const txservice::TableName &table_name,
            const txservice::TxKey &key, txservice::TxRecord &rec, bool &found,
            uint64_t &version_ts,
            const txservice::TableSchema *table_schema) override;

  std::unique_ptr<txservice::store::DataStoreScanner> ScanForward(
      const txservice::TableName &table_name, uint32_t ng_id,
      const txservice::TxKey &start_key, bool inclusive, uint8_t key_parts,
      const std::vector<txservice::store::DataStoreSearchCond> &search_cond,
      const txservice::KeySchema *key_schema,
      const txservice::RecordSchema *rec_schema,
      const txservice::KVCatalogInfo *kv_info, bool scan_foward) override;

  bool FetchTable(const txservice::TableName &table_name,
                  std::string &schema_image, bool &found,
                  uint64_t &version_ts) const override;

  bool DiscoverAllTableNames(
      std::vector<std::string> &norm_name_vec,
      const std::function<void()> *yield_fptr= nullptr,
      const std::function<void()> *resume_fptr= nullptr) const override;

  //-- statistics
  void FetchCurrentTableStatistics(const txservice::TableName &ccm_table_name,
                                   FetchTableStatisticsCc *fetch_cc) override;

  void FetchTableStatistics(const txservice::TableName &ccm_table_name,
                            FetchTableStatisticsCc *fetch_cc) override;

  bool UpsertTableStatistics(
      const txservice::TableName &ccm_table_name,
      const std::unordered_map<
          txservice::TableName,
          std::pair<uint64_t, std::vector<txservice::TxKey>>> &sample_pool_map,
      uint64_t version) override;

  txservice::store::DataStoreHandler::DataStoreOpStatus
  FetchRecord(txservice::FetchRecordCc *fetch_cc) override;

  //-- range partition
  void FetchTableRanges(FetchTableRangesCc *fetch_cc) override;
  void FetchRangeSlices(FetchRangeSlicesReq *fetch_cc) override;

  bool
  DeleteOutOfRangeData(const txservice::TableName &table_name,
                       int32_t partition_id, const txservice::TxKey *start_key,
                       const txservice::TableSchema *table_schema) override;

  bool GetNextRangePartitionId(const txservice::TableName &tablename,
                               uint32_t range_cnt,
                               int32_t &out_next_partition_id,
                               int retry_count) override;

  bool UpsertRanges(const txservice::TableName &table_name,
                    std::vector<txservice::SplitRangeInfo> range_info,
                    uint64_t version) override;

  txservice::store::DataStoreHandler::DataStoreOpStatus
  LoadRangeSlice(const txservice::TableName &table_name,
                 const txservice::KVCatalogInfo *kv_info,
                 uint32_t range_partition_id,
                 txservice::LoadRangeSliceRequest *load_slice_req) override;

  bool UpdateRangeSlices(const txservice::TableName &table_name,
                         uint64_t version, txservice::TxKey range_start_key,
                         std::vector<const txservice::StoreSlice *> slices,
                         int32_t partition_id,
                         uint64_t range_version) override;

  bool NeedCopyRange() const override { return true; }

  //-- database
  bool UpsertDatabase(std::string_view db,
                      std::string_view definition) const override;
  bool DropDatabase(std::string_view db) const override;
  bool FetchDatabase(
      std::string_view db, std::string &definition, bool &found,
      const std::function<void()> *yield_fptr= nullptr,
      const std::function<void()> *resume_fptr= nullptr) const override;
  bool FetchAllDatabase(
      std::vector<std::string> &dbnames,
      const std::function<void()> *yield_fptr= nullptr,
      const std::function<void()> *resume_fptr= nullptr) const override;

  bool DropKvTable(const std::string &kv_table_name) const override;

  void DropKvTableAsync(const std::string &kv_table_name) const override;

  std::string CreateKVCatalogInfo(
      const txservice::TableSchema *table_schema) const override;
  txservice::KVCatalogInfo::uptr
  DeserializeKVCatalogInfo(const std::string &kv_info_str,
                           size_t &offset) const override;

  std::string
  CreateNewKVCatalogInfo(const txservice::TableName &table_name,
                         const txservice::TableSchema *current_table_schema,
                         txservice::AlterTableInfo &alter_table_info) override;

  /**
   * @brief Write batch historical versions into DataStore.
   */
  bool PutArchivesAll(uint32_t node_group,
                      const txservice::TableName &table_name,
                      const txservice::KVCatalogInfo *kv_info,
                      std::vector<txservice::FlushRecord> &batch) override;

  /**
   * @brief Copy record from base/sk table to mvcc_archives.
   */
  bool CopyBaseToArchive(std::vector<txservice::TxKey> &batch,
                         uint32_t node_group,
                         const txservice::TableName &table_name,
                         const txservice::TableSchema *table_schema) override;

  /**
   * @brief  Get the latest visible(commit_ts <= upper_bound_ts) historical
   * version.
   */
  bool FetchVisibleArchive(const txservice::TableName &table_name,
                           const txservice::KVCatalogInfo *kv_info,
                           const txservice::TxKey &key,
                           const uint64_t upper_bound_ts,
                           txservice::TxRecord &rec,
                           txservice::RecordStatus &rec_status,
                           uint64_t &commit_ts) override;

  /**
   * @brief  Fetch all archives whose commit_ts >= from_ts.
   */
  bool FetchArchives(const txservice::TableName &table_name,
                     const txservice::KVCatalogInfo *kv_info,
                     const txservice::TxKey &key,
                     std::vector<txservice::VersionTxRecord> &archives,
                     uint64_t from_ts) override;

  bool ByPassDataStore() const override
  {
    return ddl_skip_kv_ && !is_bootstrap_;
  }

private:
  struct GeneralAsyncContext : public Aws::Client::AsyncCallerContext
  {
    GeneralAsyncContext()= delete;
    GeneralAsyncContext(void *data, DynamoHandler *handler= nullptr)
        : data_(data), handler_(handler)
    {
    }

    void *data_;
    DynamoHandler *handler_;
  };

  struct LoadRangeSliceData : public Aws::Client::AsyncCallerContext
  {
    LoadRangeSliceData()= delete;
    LoadRangeSliceData(txservice::LoadRangeSliceRequest *load_slice_req,
                       int32_t partition_id,
                       const txservice::TableName *table_name,
                       bool ddl_skip_kv, std::shared_ptr<void> defer_unpin)
        : load_slice_req_(load_slice_req), range_partition_id_(partition_id),
          table_name_(table_name), ddl_skip_kv_(ddl_skip_kv),
          defer_unpin_(defer_unpin)
    {
    }
    txservice::LoadRangeSliceRequest *load_slice_req_;
    int32_t range_partition_id_;
    const txservice::TableName *table_name_;
    bool ddl_skip_kv_;

    std::shared_ptr<void> defer_unpin_{nullptr};
  };

  struct UpsertTableData : public Aws::Client::AsyncCallerContext
  {
    UpsertTableData()= delete;
    UpsertTableData(DynamoHandler *dynamo_hd,
                    const txservice::TableName *table_name,
                    const txservice::TableSchema *schema,
                    txservice::OperationType op_type, uint64_t write_time,
                    std::shared_ptr<void> defer_unpin,
                    txservice::CcHandlerResult<txservice::Void> *hd_res,
                    const txservice::AlterTableInfo *alter_table_info= nullptr)
        : hd_res_(hd_res), dynamo_hd_(dynamo_hd), table_name_(table_name),
          table_schema_(schema), op_type_(op_type), write_time_(write_time),
          alter_table_info_(alter_table_info),
          defer_unpin_(std::move(defer_unpin))
    {
    }

    UpsertTableData(
        const UpsertTableData &other,
        std::unordered_map<uint, std::pair<txservice::TableName,
                                           txservice::SecondaryKeySchema>>::
            const_iterator indexes_it)
        : hd_res_(other.hd_res_), dynamo_hd_(other.dynamo_hd_),
          table_name_(other.table_name_), table_schema_(other.table_schema_),
          indexes_it_(indexes_it), op_type_(other.op_type_),
          write_time_(other.write_time_),
          alter_table_info_(other.alter_table_info_),
          defer_unpin_(other.defer_unpin_)
    {
    }

    UpsertTableData(const UpsertTableData &other,
                    std::unordered_map<txservice::TableName,
                                       std::string>::const_iterator indexes_it,
                    bool is_add_index= true)
        : hd_res_(other.hd_res_), dynamo_hd_(other.dynamo_hd_),
          table_name_(other.table_name_), table_schema_(other.table_schema_),
          op_type_(other.op_type_), write_time_(other.write_time_),
          alter_table_info_(other.alter_table_info_),
          defer_unpin_(other.defer_unpin_)
    {
      if (is_add_index)
      {
        add_indexes_it_= indexes_it;
      }
      else
      {
        drop_indexes_it_= indexes_it;
      }
    }

    txservice::CcHandlerResult<txservice::Void> *hd_res_;
    DynamoHandler *dynamo_hd_;
    const txservice::TableName *table_name_;
    const txservice::TableSchema *table_schema_;
    std::unordered_map<
        uint, std::pair<txservice::TableName, txservice::SecondaryKeySchema>>::
        const_iterator indexes_it_;
    txservice::OperationType op_type_;
    std::unordered_map<txservice::TableName, std::string>::const_iterator
        add_indexes_it_;
    std::unordered_map<txservice::TableName, std::string>::const_iterator
        drop_indexes_it_;
    uint64_t write_time_;
    const txservice::AlterTableInfo *alter_table_info_{nullptr};

    std::shared_ptr<void> defer_unpin_{nullptr};
  };

  bool RetryUnprocessedItems(
      const Aws::DynamoDB::Model::BatchWriteItemOutcome &outcome);

  static void OnCreateDynamoTable(
      const Aws::DynamoDB::DynamoDBClient *client,
      const Aws::DynamoDB::Model::CreateTableRequest &request,
      const Aws::DynamoDB::Model::CreateTableOutcome &result,
      const std::shared_ptr<const Aws::Client::AsyncCallerContext> &context);
  static void OnDeleteDynamoTable(
      const Aws::DynamoDB::DynamoDBClient *client,
      const Aws::DynamoDB::Model::DeleteTableRequest &request,
      const Aws::DynamoDB::Model::DeleteTableOutcome &result,
      const std::shared_ptr<const Aws::Client::AsyncCallerContext> &context);

  void UpdateDynamoTTL(DynamoCatalogInfo *table_schema);

  static void
  UpsertCatalog(const std::shared_ptr<const UpsertTableData> table_data);
  static void OnPutCatalog(
      const Aws::DynamoDB::DynamoDBClient *client,
      const Aws::DynamoDB::Model::PutItemRequest &request,
      const Aws::DynamoDB::Model::PutItemOutcome &result,
      const std::shared_ptr<const Aws::Client::AsyncCallerContext> &context);
  static void OnDeleteCatalog(
      const Aws::DynamoDB::DynamoDBClient *client,
      const Aws::DynamoDB::Model::DeleteItemRequest &request,
      const Aws::DynamoDB::Model::DeleteItemOutcome &result,
      const std::shared_ptr<const Aws::Client::AsyncCallerContext> &context);
  static void OnUpdateCatalog(
      const Aws::DynamoDB::DynamoDBClient *client,
      const Aws::DynamoDB::Model::UpdateItemRequest &request,
      const Aws::DynamoDB::Model::UpdateItemOutcome &result,
      const std::shared_ptr<const Aws::Client::AsyncCallerContext> &context);

  static void
  UpsertSkTable(const std::shared_ptr<const UpsertTableData> table_data);
  static void OnCreateDynamoSkTable(
      const Aws::DynamoDB::DynamoDBClient *client,
      const Aws::DynamoDB::Model::CreateTableRequest &request,
      const Aws::DynamoDB::Model::CreateTableOutcome &result,
      const std::shared_ptr<const Aws::Client::AsyncCallerContext> &context);
  static void OnDeleteDynamoSkTable(
      const Aws::DynamoDB::DynamoDBClient *client,
      const Aws::DynamoDB::Model::DeleteTableRequest &request,
      const Aws::DynamoDB::Model::DeleteTableOutcome &result,
      const std::shared_ptr<const Aws::Client::AsyncCallerContext> &context);

  static void UpsertTableStatistics(
      const std::shared_ptr<const UpsertTableData> table_data);
  static void OnUpsertTableStatistics(
      const Aws::DynamoDB::DynamoDBClient *client,
      const Aws::DynamoDB::Model::TransactWriteItemsRequest &request,
      const Aws::DynamoDB::Model::TransactWriteItemsOutcome &result,
      const std::shared_ptr<const Aws::Client::AsyncCallerContext> &context);

  static void
  UpsertSequence(const std::shared_ptr<const UpsertTableData> table_data);
  static void OnPutSequence(
      const Aws::DynamoDB::DynamoDBClient *client,
      const Aws::DynamoDB::Model::PutItemRequest &request,
      const Aws::DynamoDB::Model::PutItemOutcome &result,
      const std::shared_ptr<const Aws::Client::AsyncCallerContext> &context);
  static void OnDeleteSequence(
      const Aws::DynamoDB::DynamoDBClient *client,
      const Aws::DynamoDB::Model::DeleteItemRequest &request,
      const Aws::DynamoDB::Model::DeleteItemOutcome &result,
      const std::shared_ptr<const Aws::Client::AsyncCallerContext> &context);

  static void
  SetupRange(const std::shared_ptr<const UpsertTableData> table_data);
  bool
  UpsertLastRangeId(const std::shared_ptr<const UpsertTableData> table_data,
                    const TableName &table_name);
  bool UpsertRangeInfo(const std::shared_ptr<const UpsertTableData> table_data,
                       const TableName &table_name);

  static void OnFetchCatalog(
      const Aws::DynamoDB::DynamoDBClient *client,
      const Aws::DynamoDB::Model::GetItemRequest &request,
      const Aws::DynamoDB::Model::GetItemOutcome &result,
      const std::shared_ptr<const Aws::Client::AsyncCallerContext> &context);

  static void OnFetchCurrentTableStatistics(
      const Aws::DynamoDB::DynamoDBClient *client,
      const Aws::DynamoDB::Model::GetItemRequest &request,
      const Aws::DynamoDB::Model::GetItemOutcome &result,
      const std::shared_ptr<const Aws::Client::AsyncCallerContext> &context);

  static void OnFetchTableStatistics(
      const Aws::DynamoDB::DynamoDBClient *client,
      const Aws::DynamoDB::Model::QueryRequest &request,
      const Aws::DynamoDB::Model::QueryOutcome &result,
      const std::shared_ptr<const Aws::Client::AsyncCallerContext> &context);

  static void OnFetchTableRanges(
      const Aws::DynamoDB::DynamoDBClient *client,
      const Aws::DynamoDB::Model::QueryRequest &request,
      const Aws::DynamoDB::Model::QueryOutcome &result,
      const std::shared_ptr<const Aws::Client::AsyncCallerContext> &context);

  static void OnFetchRecord(
      const Aws::DynamoDB::DynamoDBClient *client,
      const Aws::DynamoDB::Model::GetItemRequest &request,
      const Aws::DynamoDB::Model::GetItemOutcome &result,
      const std::shared_ptr<const Aws::Client::AsyncCallerContext> &context);

  static void OnFetchRangeSlices(
      const Aws::DynamoDB::DynamoDBClient *client,
      const Aws::DynamoDB::Model::QueryRequest &request,
      const Aws::DynamoDB::Model::QueryOutcome &result,
      const std::shared_ptr<const Aws::Client::AsyncCallerContext> &context);

  static void OnLoadRangeSlice(
      const Aws::DynamoDB::DynamoDBClient *client,
      const Aws::DynamoDB::Model::QueryRequest &request,
      const Aws::DynamoDB::Model::QueryOutcome &result,
      const std::shared_ptr<const Aws::Client::AsyncCallerContext> &context);

  static void
  PutAllThread(DynamoHandler *handler, const txservice::TableName *table_name,
               std::vector<txservice::FlushRecord> *batch, uint32_t end,
               std::vector<std::pair<uint, Partition>> target_partitions,
               const txservice::TableSchema *table_schema, uint32_t node_group,
               std::atomic_bool *res);

  static void PutArchivesThread(DynamoHandler *handler,
                                std::vector<txservice::FlushRecord> *batch,
                                uint32_t start, uint32_t end,
                                const std::string &kv_table_name,
                                uint32_t node_group, std::atomic_bool *res);

  static void BatchReadThread(
      DynamoHandler *handler, std::vector<txservice::TxKey> *batch,
      uint32_t start, uint32_t end, const txservice::TableName *table_name,
      const txservice::TableSchema *table_schema, uint32_t node_group,
      std::atomic<bool> *read_success, std::mutex *result_mutex,
      std::vector<txservice::FlushRecord> *result);

  bool ListKvTableCTimeMore1d(std::set<std::string> &kv_table_names) const;

  bool ListVisibleKvTable(std::set<std::string> &kv_table_names) const;

  static void CleanDefunctKvTables(void *store_hd);

  const std::string keyspace_;
  bool is_bootstrap_{false};
  bool ddl_skip_kv_{false};
  bool archive_ttl_set_{false};
  std::unique_ptr<Aws::DynamoDB::DynamoDBClient> client_;
  txservice::TxWorkerPool worker_pool_;

  bthread::TimerThread timer_thd_;

  // dynamodb row limit 400KB (including attribute names)
  constexpr static uint32_t row_max_size_{200 * 1024};
};

struct DynamoCatalogInfo : public txservice::KVCatalogInfo
{
public:
  using uptr= std::unique_ptr<DynamoCatalogInfo>;
  DynamoCatalogInfo() {}
  DynamoCatalogInfo(const std::string &kv_table_name,
                    const std::string &kv_index_name);
  DynamoCatalogInfo(const DynamoCatalogInfo &kv_info)
  {
    kv_table_name_= kv_info.kv_table_name_;
    kv_index_names_= kv_info.kv_index_names_;
  }
  ~DynamoCatalogInfo() {}
  std::string Serialize() const override;
  void Deserialize(const char *buf, size_t &offset) override;

  bool ttl_set{false};
};
} // namespace MyEloq
