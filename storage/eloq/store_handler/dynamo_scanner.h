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

#include <aws/dynamodb/DynamoDBClient.h>
#include <aws/dynamodb/model/QueryRequest.h>
#include <aws/dynamodb/model/QueryResult.h>

#include <algorithm>
#include <memory>
#include <queue>
#include <tuple>
#include <string>
#include <utility>
#include <vector>
#include <type_traits>

#include "date.h"
#include "eloq_key.h"
#include "tx_service.h"
#include "partition.h"
#include "kv_store.h"
#include "tx_service/include/store/data_store_scanner.h"

namespace MyEloq
{
extern const std::string dynamo_partition_key_attribute_name;
extern const std::string dynamo_sort_key_attribute_name;
using namespace Aws::DynamoDB::Model;
class DynamoScanner : public txservice::store::DataStoreScanner
{
public:
  DynamoScanner(
      const std::string &keyspace, Aws::DynamoDB::DynamoDBClient *client,
      const txservice::KeySchema *key_sch, const EloqRecordSchema *rec_sch,
      const txservice::TableName &table_name,
      const txservice::KVCatalogInfo *kv_info, const EloqKey *start_key,
      bool inclusive,
      const std::vector<txservice::store::DataStoreSearchCond> &pushdown_cond,
      bool scan_forward)
      : keyspace_(keyspace), dynamo_client_(client), key_sch_(key_sch),
        rec_sch_(rec_sch),
        table_name_(table_name.StringView(), table_name.Type()),
        kv_info_(kv_info), start_key_(start_key), inclusive_(inclusive),
        scan_forward_(scan_forward), pushdown_condition_(pushdown_cond),
        initialized_(false)
  {
  }

  virtual ~DynamoScanner() {}

protected:
  void SetPushedCond();
  bool IsScanWithPushdownCondition();
  bool IsScanWithStartKey();
  void BuildScanPartitionRequest();
  void EncodeDynamoRow(const Aws::Map<Aws::String, AttributeValue> &row,
                       ScanHeapTuple<EloqKey, EloqRecord> &heap_tuple,
                       const EloqRecordSchema *rec_sch);

protected:
  const std::string keyspace_;
  const Aws::DynamoDB::DynamoDBClient *dynamo_client_;
  // primary key or secondary key schema
  const txservice::KeySchema *key_sch_;
  const EloqRecordSchema *rec_sch_;
  const txservice::TableName
      table_name_; // not string owner, sv -> MysqlTableSchema
  const txservice::KVCatalogInfo *kv_info_;
  // Scan key (sort key in DynamoDB)
  const EloqKey *start_key_;
  const bool inclusive_;
  bool scan_forward_;
  const std::vector<txservice::store::DataStoreSearchCond> pushdown_condition_;
  QueryRequest scan_req_;
  bool initialized_{false};
};
template <bool Direction>
class HashPartitionDynamoScanner : public DynamoScanner
{
public:
  HashPartitionDynamoScanner(
      const std::string &keyspace, Aws::DynamoDB::DynamoDBClient *client,
      const txservice::KeySchema *key_sch, const EloqRecordSchema *rec_sch,
      const txservice::TableName &table_name,
      const txservice::KVCatalogInfo *kv_info, const EloqKey &start_key,
      bool inclusive,
      const std::vector<txservice::store::DataStoreSearchCond> &pushdown_cond)
      : DynamoScanner(keyspace, client, key_sch, rec_sch, table_name, kv_info,
                      &start_key, inclusive, pushdown_cond, Direction)
  {
  }

  ~HashPartitionDynamoScanner() {}

  void Current(txservice::TxKey &key, const txservice::TxRecord *&rec,
               uint64_t &version_ts, bool &deleted) override;
  bool MoveNext() override;
  void End() override;

private:
  bool Init();
  QueryOutcomeCallable
  ScanPartition(int32_t partition, int32_t page_limit,
                Aws::Map<Aws::String, AttributeValue> *start_key= nullptr);
  bool AddPartitionResult(QueryOutcomeCallable &future, int32_t partition);

private:
  using CompareFunc=
      std::conditional_t<Direction, CacheCompare<EloqKey, EloqRecord>,
                         CacheReverseCompare<EloqKey, EloqRecord>>;
  std::priority_queue<ScanHeapTuple<EloqKey, EloqRecord>,
                      std::vector<ScanHeapTuple<EloqKey, EloqRecord>>,
                      CompareFunc>
      heap_cache_;

  // map from partition id to remaining tuples from this partition in
  // heap_cache_ and last_key from previous query request.
  std::unordered_map<int32_t,
                     std::pair<int32_t, Aws::Map<Aws::String, AttributeValue>>>
      scan_res_;
};

} // namespace MyEloq
