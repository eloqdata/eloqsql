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

#include <algorithm>
#include <memory>
#include <queue>
#include <tuple>
#include <string>
#include <utility>
#include <vector>
#include <type_traits>

#include "cass/include/cassandra.h"
#include "date.h"
#include "eloq_key.h"
#include "tx_service.h"
#include "partition.h"
#include "kv_store.h"
#include "tx_service/include/store/data_store_scanner.h"

namespace MyEloq
{
class CassScanner : public txservice::store::DataStoreScanner
{
public:
  CassScanner(
      CassSession *cass_session, const std::string &keyspace_name,
      const txservice::KeySchema *key_sch, const EloqRecordSchema *rec_sch,
      const txservice::TableName &table_name,
      const txservice::KVCatalogInfo *kv_info, const EloqKey *start_key,
      bool inclusive,
      const std::vector<txservice::store::DataStoreSearchCond> &pushdown_cond,
      bool scan_forward)
      : cass_session_(cass_session), keyspace_name_v_(keyspace_name),
        key_sch_(key_sch), rec_sch_(rec_sch),
        table_name_(table_name.StringView(), table_name.Type()),
        kv_info_(kv_info), start_key_(start_key), inclusive_(inclusive),
        scan_forward_(scan_forward), pushdown_condition_(pushdown_cond),
        scan_prepared_(nullptr), initialized_(false)
  {
    assert(table_name_.Type() == txservice::TableType::Primary ||
           table_name_.Type() == txservice::TableType::Secondary ||
           table_name_.Type() == txservice::TableType::UniqueSecondary);
  }

  std::string_view ErrorMessage(CassFuture *future);

  virtual ~CassScanner();

protected:
  CassError BuildScanPartitionPrepared();
  std::string BuildPushedCondStr();
  std::pair<CassStatement *, CassFuture *>
  BuildScanPartitionStatement(const std::string &kv_table_name, int32_t pk1,
                              int16_t pk2, size_t page_size);
  void EncodeCassRow(const CassRow *row, const EloqRecordSchema *rec_sch,
                     EloqKey *key, EloqRecord *rec, uint64_t &version_ts_,
                     bool &deleted_);
  bool IsScanWithPushdownCondition();
  bool IsScanWithStartKey();

protected:
  CassSession *cass_session_;
  const std::string_view keyspace_name_v_;
  // primary key or secondary key schema
  const txservice::KeySchema *key_sch_;
  const EloqRecordSchema *rec_sch_;
  const txservice::TableName
      table_name_; // not string owner, sv -> MysqlTableSchema
  const txservice::KVCatalogInfo *kv_info_;
  const EloqKey *start_key_; // pk or (sk,pk)
  const bool inclusive_;
  bool scan_forward_;
  const std::vector<txservice::store::DataStoreSearchCond> pushdown_condition_;
  const CassPrepared *scan_prepared_{nullptr};
  bool initialized_{false};
};

template <bool Direction> class HashPartitionCassScanner : public CassScanner
{
public:
  HashPartitionCassScanner(
      CassSession *cass_session, const std::string &keyspace_name,
      const txservice::KeySchema *key_sch, const EloqRecordSchema *rec_sch,
      const txservice::TableName &table_name,
      const txservice::KVCatalogInfo *kv_info, const EloqKey &start_key,
      bool inclusive,
      const std::vector<txservice::store::DataStoreSearchCond> &pushdown_cond)
      : CassScanner(cass_session, keyspace_name, key_sch, rec_sch, table_name,
                    kv_info, &start_key, inclusive, pushdown_cond, Direction)
  {
  }

  ~HashPartitionCassScanner()
  {
    for (size_t sid= 0; sid < shard_scan_res_.size(); ++sid)
    {
      if (shard_scan_res_.at(sid) != nullptr)
      {
        cass_result_free(shard_scan_res_.at(sid));
      }

      if (shard_scan_it_.at(sid) != nullptr)
      {
        cass_iterator_free(shard_scan_it_.at(sid));
      }

      cass_statement_free(shard_scan_st_.at(sid));
    }
  }

  bool AddShardScan(CassStatement *scan_st, CassFuture *scan_future);
  void Current(txservice::TxKey &key, const txservice::TxRecord *&rec,
               uint64_t &version_ts, bool &deleted_) override;
  bool MoveNext() override;
  void End() override;

private:
  bool Init();

private:
  using CompareFunc=
      std::conditional_t<Direction, CacheCompare<EloqKey, EloqRecord>,
                         CacheReverseCompare<EloqKey, EloqRecord>>;
  std::priority_queue<ScanHeapTuple<EloqKey, EloqRecord>,
                      std::vector<ScanHeapTuple<EloqKey, EloqRecord>>,
                      CompareFunc>
      heap_cache_;
  std::vector<const CassResult *> shard_scan_res_;
  std::vector<CassIterator *> shard_scan_it_;
  std::vector<CassStatement *> shard_scan_st_;
};

#ifdef RANGE_PARTITION_ENABLED
class RangePartitionCassScanner : public CassScanner
{
public:
  RangePartitionCassScanner(
      CassSession *cass_session, const std::string &keyspace_name,
      const txservice::KeySchema *key_sch, const EloqRecordSchema *rec_sch,
      const txservice::TableName &table_name, uint32_t ng_id,
      const txservice::KVCatalogInfo *kv_info, const EloqKey &start_key,
      bool inclusive,
      const std::vector<txservice::store::DataStoreSearchCond> &pushdown_cond,
      bool scan_forward, txservice::TxService *tx_service)
      : CassScanner(cass_session, keyspace_name, key_sch, rec_sch, table_name,
                    kv_info, nullptr, inclusive, pushdown_cond, scan_forward),
        tx_service_(tx_service), ng_id_(ng_id), partition_iterator_(nullptr),
        scan_st_(nullptr),
        scan_res_(std::unique_ptr<
                  const CassResult,
                  decltype(&RangePartitionCassScanner::CassResultFree)>(
            nullptr, &cass_result_free)),
        scan_it_(nullptr), scan_finished_(false)
  {
    if (&start_key == EloqKey::NegativeInfinity())
    {
      start_key_= EloqKey::NegativeInfinity();
    }
    else if (&start_key == EloqKey::PositiveInfinity())
    {
      start_key_= EloqKey::PositiveInfinity();
    }
    else
    {
      start_key_holder_= start_key;
      start_key_= &start_key_holder_;
    }
  }

  ~RangePartitionCassScanner()
  {
    if (scan_it_ != nullptr)
    {
      cass_iterator_free(scan_it_);
    }
    if (scan_st_ != nullptr)
    {
      cass_statement_free(scan_st_);
    }
    if (!scan_finished_)
    {
      // TODO(Xiao Ji): remove the nullptr check. This is a unnecessary check,
      // since partition iterator must be there if range partition is enabled,
      // but some other bugs may cause the cass_scanner is not been initialized
      // correctly
      if (partition_iterator_ != nullptr)
      {
        partition_iterator_->ReleaseReadLocks();
      }
    }
  }

  void Current(txservice::TxKey &key, const txservice::TxRecord *&rec,
               uint64_t &version_ts, bool &deleted) override;
  bool MoveNext() override;
  void End() override;

private:
  bool ScanNextPartition();
  bool CassIteratorNext();
  bool Init();
  // For purpose of working around the compiling error on default deleter
  static void CassResultFree(const CassResult *result)
  {
    // This won't be called unless the scan_res_.get() is not nullptr when
    // destructor
    cass_result_free(result);
  };

private:
  txservice::TxService *tx_service_{nullptr};
  uint32_t ng_id_;
  std::unique_ptr<PartitionIterator> partition_iterator_;
  CassStatement *scan_st_{nullptr};
  std::unique_ptr<const CassResult,
                  decltype(&RangePartitionCassScanner::CassResultFree)>
      scan_res_;
  CassIterator *scan_it_{nullptr};
  bool scan_finished_{false};
  EloqKey start_key_holder_;
  EloqKey current_key_;
  EloqRecord current_rec_;
  uint64_t current_version_ts_;
  bool current_deleted_;
};
#endif
} // namespace MyEloq
