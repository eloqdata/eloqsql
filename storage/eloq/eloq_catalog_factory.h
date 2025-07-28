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

#include <optional>
#include <unordered_map>
#include <string>
#include <memory>

#include "tx_service/include/catalog_factory.h"
#include "tx_service/include/store/data_store_handler.h"
#include "eloq_key.h"
#include "eloq_schema.h"
#include "ha_eloq_macro.h"
#include "tx_service/include/table_statistics.h"
#include "tx_service/include/type.h" // TableName

extern std::unique_ptr<txservice::store::DataStoreHandler> storage_hd;

namespace txservice
{
class StoreRange;
}

namespace MyEloq
{
bool ReadHiddenPkFromRowkey(uchar *hidden_pk_id, const EloqKey *row_key);

struct MysqlTableSchema;

struct MysqlSkEncoder final : public txservice::SkEncoder
{
public:
  MysqlSkEncoder(const txservice::TableName &index_name,
                 const MysqlTableSchema *table_schema);
  ~MysqlSkEncoder();

  int32_t
  AppendPackedSk(const txservice::TxKey *pk, const txservice::TxRecord *record,
                 uint64_t version_ts,
                 std::vector<txservice::WriteEntry> &dest_vec) override;

  void Reset() override;

private:
  bool DecodeRecord(uchar *table_record, const EloqKey *key,
                    const EloqRecord *record) const;

  void MarkIndexColumnsForRead();

private:
  const MysqlTableSchema *mysql_table_schema_{nullptr};
  const txservice::SecondaryKeySchema *secondary_key_schema_{nullptr};

  THD *gen_packed_sk_thd_{nullptr};
  mysql::TABLE mysql_table_;
  // Temporary space for packing VarString.
  std::vector<uchar> pack_buffer_vec_;
  // Temporary buffers for storing the key part of the Key/Value pair
  std::vector<uchar> sk_packed_tuple_vec_;
};

struct MysqlTableSchema : public txservice::TableSchema
{
public:
  MysqlTableSchema(const txservice::TableName &table_name,
                   const std::string &catalog_image, uint64_t version);
  ~MysqlTableSchema();

  txservice::TableSchema::uptr Clone() const override;
  const txservice::KeySchema *KeySchema() const override;
  const txservice::RecordSchema *RecordSchema() const override;
  const std::string &SchemaImage() const override;
  uint64_t Version() const override;
  std::string_view VersionStringView() const override;
  std::vector<txservice::TableName> IndexNames() const override;
  size_t IndexesSize() const override;
  const txservice::SecondaryKeySchema *
  IndexKeySchema(const txservice::TableName &index_name) const override;
  uint16_t IndexOffset(const txservice::TableName &index_name) const override;
  const mysql::TABLE_SHARE *TableShare() const;
  const std::pair<txservice::TableName, txservice::SecondaryKeySchema> &
  IndexNameSchema(uint index_id) const;
  uint IndexId(const txservice::TableName &index_name) const;

  const txservice::TableName &GetBaseTableName() const override
  {
    return base_table_name_;
  }

  const EloqRecordSchema *MysqlRecordSchema() const { return &record_schema_; }

  const std::unordered_map<
      uint16_t,
      std::pair<txservice::TableName, txservice::SecondaryKeySchema>> *
  GetIndexes() const
  {
    return &indexes_;
  }

  void SetKVCatalogInfo(const std::string &kv_info) override
  {
    size_t offset= 0;
    kv_info_= storage_hd->DeserializeKVCatalogInfo(kv_info, offset);
  }

  txservice::KVCatalogInfo *GetKVCatalogInfo() const override
  {
    return kv_info_.get();
  }

  void
  BindStatistics(std::shared_ptr<txservice::Statistics> statistics) override
  {
    std::shared_ptr<txservice::TableStatistics<EloqKey>> table_statistics=
        std::dynamic_pointer_cast<txservice::TableStatistics<EloqKey>>(
            statistics);
    assert(table_statistics != nullptr);

    table_statistics_= table_statistics;
  }

  std::shared_ptr<txservice::Statistics> StatisticsObject() const override
  {
    return table_statistics_;
  }

  txservice::SkEncoder::uptr
  CreateSkEncoder(const txservice::TableName &index_name) const override;
  bool HasAutoIncrement() const override;
  const txservice::TableName *GetSequenceTableName() const override;
  std::pair<txservice::TxKey, txservice::TxRecord::Uptr>
  GetSequenceKeyAndInitRecord(
      const txservice::TableName &table_name) const override;

private:
  mysql::TABLE_SHARE mysql_table_share_;
  std::unique_ptr<EloqKeySchema> key_schema_; // pk schema
  EloqRecordSchema record_schema_;
  std::unordered_map<
      uint16_t, std::pair<txservice::TableName, txservice::SecondaryKeySchema>>
      indexes_; // sk schemas, string owner
  std::string schema_image_;

  uint64_t version_;
  txservice::TableName base_table_name_; // string owner
  txservice::KVCatalogInfo::uptr kv_info_;

  std::shared_ptr<txservice::TableStatistics<EloqKey>> table_statistics_{
      nullptr};
};

class MariaCatalogFactory : public txservice::CatalogFactory
{
public:
  MariaCatalogFactory()= default;
  ~MariaCatalogFactory() {}

  txservice::TableSchema::uptr
  CreateTableSchema(const txservice::TableName &table_name,
                    const std::string &catalog_image,
                    uint64_t version) override;

  txservice::CcMap::uptr
  CreatePkCcMap(const txservice::TableName &table_name,
                const txservice::TableSchema *table_schema,
                bool ccm_has_full_entries, txservice::CcShard *shard,
                txservice::NodeGroupId cc_ng_id) override;

  txservice::CcMap::uptr
  CreateSkCcMap(const txservice::TableName &table_name,
                const txservice::TableSchema *table_schema,
                txservice::CcShard *shard,
                txservice::NodeGroupId cc_ng_id) override;

  txservice::CcMap::uptr
  CreateRangeMap(const txservice::TableName &range_table_name,
                 const txservice::TableSchema *table_schema,
                 uint64_t schema_ts, txservice::CcShard *shard,
                 const txservice::NodeGroupId ng_id) override;

  std::unique_ptr<txservice::TableRangeEntry> CreateTableRange(
      txservice::TxKey start_key, uint64_t version_ts, int64_t partition_id,
      std::unique_ptr<txservice::StoreRange> slices= nullptr) override;

  std::unique_ptr<txservice::CcScanner>
  CreatePkCcmScanner(txservice::ScanDirection direction,
                     const txservice::KeySchema *key_schema) override;

  std::unique_ptr<txservice::CcScanner>
  CreateSkCcmScanner(txservice::ScanDirection direction,
                     const txservice::KeySchema *compound_key_schema) override;

  std::unique_ptr<txservice::CcScanner>
  CreateRangeCcmScanner(txservice::ScanDirection direction,
                        const txservice::KeySchema *key_schema,
                        const txservice::TableName &range_table_name) override;

  std::unique_ptr<txservice::Statistics>
  CreateTableStatistics(const txservice::TableSchema *table_schema,
                        txservice::NodeGroupId cc_ng_id) override;

  std::unique_ptr<txservice::Statistics> CreateTableStatistics(
      const txservice::TableSchema *table_schema,
      std::unordered_map<txservice::TableName,
                         std::pair<uint64_t, std::vector<txservice::TxKey>>>
          sample_pool_map,
      txservice::CcShard *ccs, txservice::NodeGroupId cc_ng_id) override;

  txservice::TxKey NegativeInfKey() override;

  txservice::TxKey PositiveInfKey() override;
};
} // namespace MyEloq
