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
#include <utility>

#include "dynamo_scanner.h"
#include "partition.h"
#include "constants.h"
#include "dynamo_handler.h"

namespace MyEloq
{
using namespace Aws::DynamoDB::Model;
const std::string dynamo_partition_key_attribute_name= "pk1_";
const std::string dynamo_sort_key_attribute_name= "___mono_key___";
// Max number of items returned on a dynamo query request.
const int dynamo_scan_page_limit= 1000;

bool DynamoScanner::IsScanWithPushdownCondition()
{
  return !pushdown_condition_.empty();
}

bool DynamoScanner::IsScanWithStartKey()
{
  return start_key_ != nullptr && start_key_ != EloqKey::PositiveInfinity() &&
         start_key_ != EloqKey::NegativeInfinity();
}

void DynamoScanner::EncodeDynamoRow(
    const Aws::Map<Aws::String, AttributeValue> &row,
    ScanHeapTuple<EloqKey, EloqRecord> &heap_tuple,
    const EloqRecordSchema *rec_sch)
{
  heap_tuple.version_ts_= std::stoull(row.at("___version___").GetN());
  heap_tuple.deleted_= row.at("___deleted___").GetBool();
  Aws::Utils::ByteBuffer packed_key=
      row.at(dynamo_sort_key_attribute_name).GetB();
  EloqRecord *rec= heap_tuple.rec_.get();
  heap_tuple.key_= std::make_unique<EloqKey>(packed_key.GetUnderlyingData(),
                                             packed_key.GetLength());
  if (!heap_tuple.deleted_)
  {
    if (table_name_.Type() == txservice::TableType::Primary)
    {
      rec_sch->Encode(row, rec->encoded_blob_);
    }
    Aws::Utils::ByteBuffer unpack_info= row.at("___unpack_info___").GetB();
    rec->SetUnpackInfo(unpack_info.GetUnderlyingData(),
                       unpack_info.GetLength());
  }
}

/**
 * @brief Build up scan_req_
 *
 */
void DynamoScanner::BuildScanPartitionRequest()
{
  initialized_= true;
  // Setup the scan req;
  scan_req_.SetConsistentRead(true);
  const DynamoCatalogInfo *dynamo_info=
      static_cast<const DynamoCatalogInfo *>(kv_info_);
  if (table_name_.Type() == txservice::TableType::Secondary)
  {
    scan_req_.SetTableName(keyspace_ + '.' +
                           dynamo_info->kv_index_names_.at(table_name_));
  }
  else
  {
    scan_req_.SetTableName(keyspace_ + '.' + dynamo_info->kv_table_name_);
  }

  scan_req_.AddExpressionAttributeNames("#pk",
                                        dynamo_partition_key_attribute_name);
  std::string key_cond("#pk = :pk");

  if (IsScanWithStartKey())
  {
    scan_req_.AddExpressionAttributeNames("#sk",
                                          dynamo_sort_key_attribute_name);
    key_cond.append(" AND #sk");
    if (inclusive_)
    {
      if (scan_forward_)
      {
        key_cond.append(" >= :sk");
      }
      else
      {
        key_cond.append(" <= :sk");
      }
    }
    else
    {
      if (scan_forward_)
      {
        key_cond.append(" > :sk");
      }
      else
      {
        key_cond.append(" < :sk");
      }
    }
  }

  scan_req_.SetKeyConditionExpression(std::move(key_cond));

  if (table_name_.Type() != txservice::TableType::Secondary &&
      IsScanWithPushdownCondition())
  {
    SetPushedCond();
  }

  if (!scan_forward_)
  {
    scan_req_.SetScanIndexForward(false);
  }
}

void DynamoScanner::SetPushedCond()
{
  std::string pushed_str;
  for (size_t i= 0; i < pushdown_condition_.size(); ++i)
  {
    auto &pushdown_cond= pushdown_condition_[i];
    if (pushed_str.size())
    {
      pushed_str.append(" AND ");
    }
    std::string att_name("#c");
    att_name.append(std::to_string(i));
    std::string att_val(":c");
    att_val.append(std::to_string(i));
    pushed_str.append(att_name);
    pushed_str.append(" ");
    pushed_str.append(pushdown_cond.op_);
    pushed_str.append(" ");
    pushed_str.append(att_val);
    scan_req_.AddExpressionAttributeNames(std::move(att_name),
                                          pushdown_cond.field_name_);
    AttributeValue dynamo_att_val;
    switch (pushdown_cond.data_type_)
    {
    case txservice::store::DataStoreDataType::Numeric:
      dynamo_att_val.SetN(pushdown_cond.val_str_);
      break;
    case txservice::store::DataStoreDataType::String:
      dynamo_att_val.SetS(pushdown_cond.val_str_);
      break;
    case txservice::store::DataStoreDataType::Blob: {
      Aws::Utils::ByteBuffer val_buffer(
          reinterpret_cast<const uchar *>(pushdown_cond.val_str_.data()),
          pushdown_cond.val_str_.length());
      dynamo_att_val.SetB(std::move(val_buffer));
      break;
    }
    default:
      // Type is certain for pushdown conditions, should not be here.
      assert(false);
    }
    scan_req_.AddExpressionAttributeValues(std::move(att_val),
                                           std::move(dynamo_att_val));
  }
  scan_req_.SetFilterExpression(pushed_str);
}

/**
 * @brief Scan a partition and return a query request future. Will use start
 * key as exclusive start key, otherwise use start_key_ passed in from mysql.
 *
 * @tparam Direction
 * @param partition
 * @param page_limit max number of items returned per scan, or 1MB of data if
 * <= 0
 * @param start_key
 * @return QueryOutcomeCallable
 */
template <bool Direction>
QueryOutcomeCallable HashPartitionDynamoScanner<Direction>::ScanPartition(
    int32_t partition, int32_t page_limit,
    Aws::Map<Aws::String, AttributeValue> *start_key)
{
  // AddExpressionAttributeValues will not override existing values, so
  // we cannot reuse the same QueryRequest object across different partitions.
  // Need to make a new copied object with each partition.
  QueryRequest scan_req(scan_req_);
  if (start_key != nullptr)
  {
    scan_req.SetExclusiveStartKey(std::move(*start_key));
  }
  AttributeValue partition_key;
  partition_key.SetN(partition);
  scan_req.AddExpressionAttributeValues(":pk", std::move(partition_key));

  if (IsScanWithStartKey())
  {
    Aws::Utils::ByteBuffer packed_key(
        reinterpret_cast<const uchar *>(start_key_->PackedValueSlice().data()),
        start_key_->PackedValueSlice().size());
    AttributeValue sort_key;
    sort_key.SetB(std::move(packed_key));
    scan_req.AddExpressionAttributeValues(":sk", std::move(sort_key));
  }

  if (page_limit > 0)
  {
    scan_req.SetLimit(page_limit);
  }
  return dynamo_client_->QueryCallable(scan_req);
}

/**
 * @brief Retrieve result from query request future. Add results to heap_cache_
 * add emplace to scan_res_ number of items scanned from this partition.
 *
 * @tparam Direction
 * @param future
 * @param partition
 * @return true
 * @return false
 */
template <bool Direction>
bool HashPartitionDynamoScanner<Direction>::AddPartitionResult(
    QueryOutcomeCallable &future, int32_t partition)
{
  assert(future.valid());
  const QueryOutcome &result= future.get();
  if (!result.IsSuccess())
  {
    LOG(INFO) << "DynamoDB query request failed, "
              << result.GetError().GetExceptionName() << ": "
              << result.GetError().GetMessage();
    return false;
  }

  const Aws::Vector<Aws::Map<Aws::String, AttributeValue>> &rows=
      result.GetResult().GetItems();
  if (rows.size() == 0 && result.GetResult().GetLastEvaluatedKey().size() != 0)
  {
    // The page_limit we set is the number of items scanned before filtering.
    // So if we have passed in pushdown condition to the scan request, we might
    // get less items than page_limit, or even 0. But that does not
    // necessarilly mean the scan has completed. Continue the scan if last
    // evaluated key is not null.
    auto end_key= result.GetResult().GetLastEvaluatedKey();
    QueryOutcomeCallable future=
        ScanPartition(partition, dynamo_scan_page_limit, &end_key);
    return AddPartitionResult(future, partition);
  }
  if (rows.size())
  {
    for (auto row : rows)
    {
      ScanHeapTuple<EloqKey, EloqRecord> tuple(partition);
      EncodeDynamoRow(row, tuple, rec_sch_);
      heap_cache_.push(std::move(tuple));
    }
    scan_res_.insert_or_assign(
        partition,
        std::make_pair(rows.size(), result.GetResult().GetLastEvaluatedKey()));
  }

  return true;
}

template <bool Direction> bool HashPartitionDynamoScanner<Direction>::Init()
{
  BuildScanPartitionRequest();
  scan_res_.reserve(1024);

#ifdef USE_ONE_CASS_SHARD
  QueryOutcomeCallable scan_future= ScanPartition(0, dynamo_scan_page_limit);
  if (!AddPartitionResult(scan_future, 0))
  {
    return false;
  }

#else
  std::vector<QueryOutcomeCallable> future_vec;
  future_vec.reserve(1024);

  for (size_t partition= 0; partition < 1024; ++partition)
  {
    future_vec.emplace_back(ScanPartition(partition, dynamo_scan_page_limit));
  }

  for (size_t partition= 0; partition < 1024; ++partition)
  {
    if (!AddPartitionResult(future_vec.at(partition), partition))
    {
      return false;
    }
  }
#endif
  initialized_= true;
  return true;
}

template <bool Direction>
void HashPartitionDynamoScanner<Direction>::Current(
    txservice::TxKey &key, const txservice::TxRecord *&rec,
    uint64_t &version_ts, bool &deleted)
{
  if (heap_cache_.size() == 0)
  {
    key= txservice::TxKey();
    rec= nullptr;
    return;
  }

  const ScanHeapTuple<EloqKey, EloqRecord> &top= heap_cache_.top();
  key= TxKey(top.key_.get());
  rec= top.rec_.get();
  version_ts= top.version_ts_;
  deleted= top.deleted_;
}

template <bool Direction>
bool HashPartitionDynamoScanner<Direction>::MoveNext()
{
  if (!initialized_)
  {
    if (!Init())
    {
      return false;
    }
    return true;
  }

  if (heap_cache_.size() == 0)
  {
    return true;
  }

  const ScanHeapTuple<EloqKey, EloqRecord> &top= heap_cache_.top();
  // sid is the partition id used in this query.
  uint32_t partition= top.sid_;
  heap_cache_.pop();

  if (--scan_res_.at(partition).first == 0)
  {
    if (scan_res_.at(partition).second.size())
    {
      QueryOutcomeCallable scan_future= ScanPartition(
          partition, dynamo_scan_page_limit, &scan_res_.at(partition).second);
      if (!AddPartitionResult(scan_future, partition))
      {
        return false;
      }
    }
  }
  return true;
}

template <bool Direction> void HashPartitionDynamoScanner<Direction>::End() {}
template class HashPartitionDynamoScanner<true>;
template class HashPartitionDynamoScanner<false>;
} // namespace MyEloq