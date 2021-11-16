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

#include "eloq_key.h"
#include "eloq_schema.h"
#include "eloq_catalog_factory.h"

#include "tx_service/include/store/data_store_handler.h"
#include "tx_service/include/store/data_store_scanner.h"
#include "tx_service/include/tx_worker_pool.h"

#include "google/cloud/bigtable/table.h"
#include "google/cloud/bigtable/admin/bigtable_table_admin_client.h"

namespace MyEloq
{
class BigTableHandler : public txservice::store::DataStoreHandler
{
public:
  BigTableHandler(const std::string &keyspace, const std::string &project_id,
                  const std::string &instance_id, bool bootstrap,
                  bool ddl_skip_kv, uint32_t worker_pool_size= 3);

  ~BigTableHandler();

  bool Connect() override;

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
   * @param node_group
   * @return whether all entries are written to data store successfully
   */
  bool PutAll(std::vector<txservice::FlushRecord> &batch,
              const txservice::TableName &table_name,
              const txservice::TableSchema *table_schema,
              uint32_t node_group) override;

  void UpsertTable(const txservice::TableSchema *old_table_schema,
                   const txservice::TableSchema *table_schema,
                   txservice::OperationType op_type, uint64_t commit_ts,
                   txservice::NodeGroupId ng_id, int64_t tx_term,
                   txservice::CcHandlerResult<txservice::Void> *hd_res,
                   const txservice::AlterTableInfo *alter_table_info= nullptr,
                   txservice::CcRequestBase *cc_req= nullptr,
                   txservice::CcShard *ccs= nullptr,
                   txservice::CcErrorCode *err_code= nullptr) override;

  void FetchTableCatalog(const txservice::TableName &ccm_table_name,
                         txservice::FetchCatalogCc *fetch_cc) override;

  void FetchTableRanges(txservice::FetchTableRangesCc *fetch_cc) override;

  void FetchRangeSlices(txservice::FetchRangeSlicesReq *fetch_cc) override;

  /**
   * @brief Read a row from base table or skindex table in datastore with
   * specified key. Caller should pass in complete primary key or skindex key.
   */
  bool Read(const txservice::TableName &table_name,
            const txservice::TxKey &key, txservice::TxRecord &rec, bool &found,
            uint64_t &version_ts,
            const txservice::TableSchema *table_schema) override;

  bool FetchTable(const txservice::TableName &table_name,
                  std::string &schema_image, bool &found,
                  uint64_t &version_ts) const override;

  void FetchCurrentTableStatistics(
      const txservice::TableName &ccm_table_name,
      txservice::FetchTableStatisticsCc *fetch_cc) override;

  void
  FetchTableStatistics(const txservice::TableName &ccm_table_name,
                       txservice::FetchTableStatisticsCc *fetch_cc) override;

  txservice::store::DataStoreHandler::DataStoreOpStatus
  FetchRecord(txservice::FetchRecordCc *fetch_cc) override;

  bool UpsertTableStatistics(
      const txservice::TableName &ccm_table_name,
      const std::unordered_map<
          txservice::TableName,
          std::pair<uint64_t, std::vector<txservice::TxKey>>> &sample_pool_map,
      uint64_t version) override;

  txservice::store::DataStoreHandler::DataStoreOpStatus
  LoadRangeSlice(const txservice::TableName &table_name,
                 const txservice::KVCatalogInfo *kv_info,
                 uint32_t partition_id,
                 txservice::LoadRangeSliceRequest *load_slice_req) override;

  bool UpdateRangeSlices(const txservice::TableName &table_name,
                         uint64_t version, txservice::TxKey range_start_key,
                         std::vector<const txservice::StoreSlice *> slices,
                         int32_t partition_id,
                         uint64_t range_version) override;

  /**
   * @brief Upsert list of ranges into range table. This will also update
   * range slice sizes.
   */
  bool UpsertRanges(const txservice::TableName &table_name,
                    std::vector<txservice::SplitRangeInfo> range_info,
                    uint64_t version) override;

  bool DiscoverAllTableNames(
      std::vector<std::string> &norm_name_vec,
      const std::function<void()> *yield_fptr= nullptr,
      const std::function<void()> *resume_fptr= nullptr) const override;

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

  std::unique_ptr<txservice::store::DataStoreScanner> ScanForward(
      const txservice::TableName &table_name, uint32_t ng_id,
      const txservice::TxKey &start_key, bool inclusive, uint8_t key_parts,
      const std::vector<txservice::store::DataStoreSearchCond> &search_cond,
      const txservice::KeySchema *key_schema,
      const txservice::RecordSchema *rec_schema,
      const txservice::KVCatalogInfo *kv_info, bool scan_foward) override;

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

  void SetTxService(txservice::TxService *tx_service)
  {
    tx_service_= tx_service;
  }

  bool
  DeleteOutOfRangeData(const txservice::TableName &table_name,
                       int32_t partition_id, const txservice::TxKey *start_key,
                       const txservice::TableSchema *table_schema) override;

  bool GetNextRangePartitionId(const txservice::TableName &tablename,
                               uint32_t range_cnt,
                               int32_t &out_next_partition_id,
                               int retry_count= 5) override;

  std::string CreateKVCatalogInfo(
      const txservice::TableSchema *table_schema) const override;

  txservice::KVCatalogInfo::uptr
  DeserializeKVCatalogInfo(const std::string &kv_info_str,
                           size_t &offset) const override;

  std::string
  CreateNewKVCatalogInfo(const txservice::TableName &table_name,
                         const txservice::TableSchema *current_table_schema,
                         txservice::AlterTableInfo &alter_table_info) override;

  bool NeedCopyRange() const override { return false; }

  bool ByPassDataStore() const override
  {
    return ddl_skip_kv_ && !is_bootstrap_;
  }

private:
  void OnFetchCatalog(google::cloud::future<google::cloud::StatusOr<
                          std::pair<bool, google::cloud::bigtable::Row>>>
                          f,
                      txservice::FetchCatalogCc *fetch_cc);
  void OnFetchRecord(google::cloud::future<google::cloud::StatusOr<
                         std::pair<bool, google::cloud::bigtable::Row>>>
                         f,
                     txservice::FetchRecordCc *fetch_cc);

  void OnFetchCurrentTableStatistics(
      google::cloud::future<google::cloud::StatusOr<
          std::pair<bool, google::cloud::bigtable::Row>>>
          f,
      txservice::FetchTableStatisticsCc *fetch_cc);

  void OnFetchRangeSlices(google::cloud::future<google::cloud::StatusOr<
                              std::pair<bool, google::cloud::bigtable::Row>>>
                              f,
                          txservice::FetchRangeSlicesReq *fetch_cc);

  bool UpsertTable(const txservice::TableSchema *table_schema,
                   txservice::OperationType op_type, uint64_t write_time,
                   const txservice::AlterTableInfo *alter_table_info);

  bool UpsertCatalog(const txservice::TableSchema *table_schema,
                     uint64_t write_time);

  bool DeleteCatalog(const txservice::TableName &base_table_name,
                     uint64_t write_time);

  bool CreateKvTable(const std::string &kv_table_name);

  bool InitTableRanges(const txservice::TableName &table_name,
                       uint64_t version);

  bool DeleteTableRanges(const txservice::TableName &table_name);

  bool InitTableLastRangePartitionId(const txservice::TableName &table_name);

  bool DeleteTableLastRangePartitionId(const txservice::TableName &table_name);

  bool InitSequence(const txservice::TableName &base_table_name,
                    uint64_t commit_ts);

  bool DeleteSeqence(const txservice::TableName &base_table_name);

  bool DeleteTableStatistics(const txservice::TableName &base_table_name);

  /**
   * @brief Async bulk apply if on-flight bulks is less then max_flight_bulks_,
   * or wait.
   * @param bulk Bulk to submit.
   * @param futures Onflight bulks.
   * @param bulk_apply_ok Store error status.
   * @return Whether bigtable response any error.
   */
  void SemiAsyncBulkApply(
      const std::string &kv_table_name,
      google::cloud::bigtable::BulkMutation bulk,
      std::vector<google::cloud::future<
          std::vector<google::cloud::bigtable::FailedMutation>>> &futures,
      std::atomic<bool> &bulk_apply_ok);

  void SemiAsyncBulkApplyJoin(
      std::vector<google::cloud::future<
          std::vector<google::cloud::bigtable::FailedMutation>>> &futures);

  google::cloud::bigtable::Table
  TableHandler(const std::string &table_name) const;

private:
  // Unlike Cassandra and DynamoDB, BigTable doesn't support collection data
  // type.
  class CollectionCSV
  {
  public:
    CollectionCSV()= default;

    CollectionCSV(std::string payload) : payload_(std::move(payload)) {}

    template <typename T> void Append(T num)
    {
      if (!payload_.empty())
      {
        payload_.push_back(',');
      }
      payload_.append(std::to_string(num));
    }

    // To serialize to csv-string, str shouldn't contain ','.
    void Append(const std::string &str)
    {
      if (!payload_.empty())
      {
        payload_.push_back(',');
      }
      payload_.append(str);
    }

    void Deserialize(std::vector<long> &nums) const
    {
      char *payload= const_cast<char *>(payload_.c_str());
      char *token= strtok(payload, ",");
      while (token)
      {
        long num= strtol(token, nullptr, 10);
        assert(errno != ERANGE);
        nums.push_back(num);
        token= strtok(nullptr, ",");
      }
    }

    void Deserialize(std::vector<std::string> &strs) const
    {
      char *payload= const_cast<char *>(payload_.c_str());
      char *token= strtok(payload, ",");
      while (token)
      {
        strs.emplace_back(token);
        token= strtok(nullptr, ",");
      }
    }

    const std::string &Payload() const & { return payload_; }

    std::string Payload() && { return std::move(payload_); }

  private:
    std::string payload_;
  };

private:
  static const google::cloud::bigtable::Cell &
  RowCell(const google::cloud::bigtable::Row &row,
          const std::string &col_name);

  static google::cloud::bigtable::RowRange ToRowRange(const EloqKey *start_key,
                                                      const EloqKey *end_key);

  static int
  LastRangePartitionIdInitValue(const txservice::TableName &table_name);

private:
  constexpr static char cf_[]{"mono"};

  constexpr static uint16_t max_mutate_rows_{1000};

  // BigTable limit row size 256MB, but we limit it to 64KB like Cassandra.
  constexpr static uint32_t max_cell_size_{64 * 1024};

  constexpr static uint8_t max_flight_bulks_{16};

private:
  const std::string keyspace_;
  const std::string project_id_;
  const std::string instance_id_;
  bool is_bootstrap_{false};
  bool ddl_skip_kv_{false};

  uint8_t flight_bulks_{0};
  std::mutex flight_bulks_mux_;
  std::condition_variable flight_bulks_cv_;

  txservice::TxWorkerPool worker_pool_;

  std::unique_ptr<google::cloud::bigtable_admin::BigtableTableAdminClient>
      admin_client_;
  std::shared_ptr<google::cloud::bigtable::DataConnection> data_connection_;
};

struct BigTableCatalogInfo : public txservice::KVCatalogInfo
{
public:
  using uptr= std::unique_ptr<BigTableCatalogInfo>;
  BigTableCatalogInfo() {}
  BigTableCatalogInfo(const std::string &kv_table_name,
                      const std::string &kv_index_name);
  BigTableCatalogInfo(const BigTableCatalogInfo &kv_info)
  {
    kv_table_name_= kv_info.kv_table_name_;
    kv_index_names_= kv_info.kv_index_names_;
  }
  ~BigTableCatalogInfo() {}
  std::string Serialize() const override;
  void Deserialize(const char *buf, size_t &offset) override;
};
} // namespace MyEloq
