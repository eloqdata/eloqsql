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

#include "cass_scanner.h"
#include "partition.h"
#include "cass_handler.h"

namespace MyEloq
{

static const std::string cass_eloq_kv_table_name= "eloq_kv_table";

CassScanner::~CassScanner()
{
  if (scan_prepared_ != nullptr)
  {
    cass_prepared_free(scan_prepared_);
  }
}

std::string CassScanner::BuildPushedCondStr()
{
  std::string pushed_conds_str;

  for (const auto &pushed_cond : pushdown_condition_)
  {
    pushed_conds_str.append(" AND ");
    pushed_conds_str.append("\"");
    pushed_conds_str.append(pushed_cond.field_name_);
    pushed_conds_str.append("\"");
    pushed_conds_str.append(pushed_cond.op_);

    switch (pushed_cond.data_type_)
    {
    case txservice::store::DataStoreDataType::String: {
      std::stringstream ss;
      ss << '\'';

      // To use a single quotation mark itself in a string literal, Cassandra
      // escapes it using a single quotation mark
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

void CassScanner::EncodeCassRow(const CassRow *row,
                                const EloqRecordSchema *rec_sch, EloqKey *key,
                                EloqRecord *rec, uint64_t &version_ts_,
                                bool &deleted_)
{
  cass_value_get_int64(cass_row_get_column_by_name(row, "___version___"),
                       (int64_t *) &version_ts_);

  cass_bool_t is_deleted= cass_bool_t::cass_false;
  cass_value_get_bool(cass_row_get_column_by_name(row, "___deleted___"),
                      (cass_bool_t *) &is_deleted);
  deleted_= (is_deleted == cass_bool_t::cass_true);
  size_t packed_len= 0, unpack_len= 0;
  const cass_byte_t *packed_key= NULL, *unpack_info= NULL;
  cass_value_get_bytes(cass_row_get_column_by_name(row, "___mono_key___"),
                       &packed_key, &packed_len);

  if (table_name_.Type() == TableType::Primary ||
      table_name_.Type() == TableType::UniqueSecondary)
  {
    if (!deleted_)
    {
      const cass_byte_t *encoded_blob= NULL;
      size_t encoded_blob_len= 0;
      cass_value_get_bytes(
          cass_row_get_column_by_name(row, "___encoded_blob___"),
          &encoded_blob, &encoded_blob_len);
      rec->SetEncodedBlob(encoded_blob, encoded_blob_len);
    }
  }

  key->PackedValue().clear();
  key->PackedValue().resize(packed_len);
  memcpy(key->PackedValue().data(), packed_key, packed_len);
  if (!deleted_)
  {
    cass_value_get_bytes(cass_row_get_column_by_name(row, "___unpack_info___"),
                         &unpack_info, &unpack_len);
    rec->SetUnpackInfo(unpack_info, unpack_len);
  }
}

bool CassScanner::IsScanWithPushdownCondition()
{
  return !pushdown_condition_.empty();
}

bool CassScanner::IsScanWithStartKey()
{
  return start_key_ != nullptr &&
         start_key_->Type() == txservice::KeyType::Normal;
}

CassError CassScanner::BuildScanPartitionPrepared()
{
  std::string scan_str("SELECT ");
  scan_str.append("\"___encoded_blob___\",");
  scan_str.append(" \"___mono_key___\",");
  scan_str.append(
      " \"___unpack_info___\", \"___version___\", \"___deleted___\" FROM ");
  scan_str.append(keyspace_name_v_).append(".");

  if (table_name_.Type() == txservice::TableType::Primary)
  {
    scan_str.append(cass_eloq_kv_table_name);
  }
  else
  {
    scan_str.append(cass_eloq_kv_table_name);
  }
  scan_str.append(" WHERE kvtablename=? AND pk1_=? AND pk2_=?");

  // Start key scan is prefered over push condition
  if (IsScanWithStartKey())
  {
    scan_str.append(" AND");
    scan_str.append(" \"___mono_key___\"");

    if (inclusive_)
    {
      if (scan_forward_)
      {
        scan_str.append(">=?");
      }
      else
      {
        scan_str.append("<=?");
      }
    }
    else
    {
      if (scan_forward_)
      {
        scan_str.append(">?");
      }
      else
      {
        scan_str.append("<?");
      }
    }
  }

  if (table_name_.Type() == txservice::TableType::Primary)
  {
    if (!pushdown_condition_.empty())
    {
      scan_str.append(BuildPushedCondStr());
    }
  }

  if (start_key_ == EloqKey::PositiveInfinity())
  {
    assert(!scan_forward_);
  }

  if (!scan_forward_)
  {
    scan_str.append(" ORDER BY \"___mono_key___\" DESC");
  }

  scan_str.append(" ALLOW FILTERING");

  CassFuture *prepare_future=
      cass_session_prepare(cass_session_, scan_str.c_str());
  cass_future_wait(prepare_future);

  CassError rc= cass_future_error_code(prepare_future);
  if (rc == CASS_OK)
  {
    scan_prepared_= cass_future_get_prepared(prepare_future);
  }
  else
  {
    LOG(ERROR) << "Prepare statement system error, "
               << "error code: " << rc << ", "
               << "error message: " << ErrorMessage(prepare_future) << ", "
               << "scan_str: " << scan_str;
  }

  cass_future_free(prepare_future);
  return rc;
};

std::pair<CassStatement *, CassFuture *>
CassScanner::BuildScanPartitionStatement(const std::string &kv_table_name,
                                         int32_t pk1, int16_t pk2,
                                         size_t page_size)
{
  CassStatement *cass_statement= cass_prepared_bind(scan_prepared_);
  cass_statement_set_is_idempotent(cass_statement, cass_true);
  // Bind table uuid
  cass_statement_bind_string(cass_statement, 0, kv_table_name.data());
  cass_statement_bind_int32(cass_statement, 1, pk1);
  cass_statement_bind_int16(cass_statement, 2, pk2);

  if (IsScanWithStartKey())
  {
    // Binds packed start key
    cass_statement_bind_bytes(cass_statement, 3,
                              reinterpret_cast<const uint8_t *>(
                                  start_key_->PackedValueSlice().data()),
                              start_key_->PackedValueSlice().size());
  }

  cass_statement_set_paging_size(cass_statement, page_size);
  CassFuture *scan_future= cass_session_execute(cass_session_, cass_statement);

  return std::pair(cass_statement, scan_future);
}

std::string_view CassScanner::ErrorMessage(CassFuture *future)
{
  const char *message;
  size_t length;
  cass_future_error_message(future, &message, &length);
  return {message, length};
}

template <bool Direction> bool HashPartitionCassScanner<Direction>::Init()
{
  initialized_= true;
  CassError r= BuildScanPartitionPrepared();
  if (r != CASS_OK)
  {
    return false;
  }
  shard_scan_res_.reserve(1024);
  shard_scan_it_.reserve(1024);
  shard_scan_st_.reserve(1024);

  std::vector<CassStatement *> st_vec;
  st_vec.reserve(1024);
  std::vector<CassFuture *> ft_vec;
  ft_vec.reserve(1024);

  const CassCatalogInfo *cass_info=
      static_cast<const CassCatalogInfo *>(kv_info_);
  const std::string &physical_table_name=
      cass_info->GetKvTableName(table_name_);

#ifdef USE_ONE_CASS_SHARD
  for (size_t sid= 0; sid < 1; ++sid)
  {
    int32_t pk1= sid;
    int16_t pk2= -1;

    std::pair<CassStatement *, CassFuture *> rs=
        BuildScanPartitionStatement(physical_table_name, pk1, pk2, 10);
    CassStatement *statement= rs.first;
    CassFuture *scan_future= rs.second;

    st_vec.emplace_back(statement);
    ft_vec.emplace_back(scan_future);
  }

  for (size_t sid= 0; sid < 1; ++sid)
  {
    bool success= AddShardScan(st_vec.at(sid), ft_vec.at(sid));
    if (!success)
    {
      return false;
    }
  }
#else
  for (size_t sid= 0; sid < 1024; ++sid)
  {
    int32_t pk1= sid;
    int16_t pk2= -1;

    std::pair<CassStatement *, CassFuture *> rs=
        BuildScanPartitionStatement(physical_table_name, pk1, pk2, 10);
    CassStatement *statement= rs.first;
    CassFuture *scan_future= rs.second;

    st_vec.emplace_back(statement);
    ft_vec.emplace_back(scan_future);
  }

  for (size_t sid= 0; sid < 1024; ++sid)
  {
    bool success= AddShardScan(st_vec.at(sid), ft_vec.at(sid));
    if (!success)
    {
      return false;
    }
  }
#endif

  return true;
}

template <bool Direction>
bool HashPartitionCassScanner<Direction>::AddShardScan(CassStatement *scan_st,
                                                       CassFuture *scan_future)
{
  const CassResult *scan_res= cass_future_get_result(scan_future);
  cass_future_free(scan_future);

  if (scan_res == NULL)
  {
    // Error
    cass_statement_free(scan_st);
    return false;
  }

  CassIterator *scan_it= cass_iterator_from_result(scan_res);

  if (cass_iterator_next(scan_it))
  {
    // vectors, such as shard_scan_st_, shard_scan_res_ and shard_scan_it_,
    // only store shards which are not empty, hence the size of their vector
    // could be less than the Cassandra shard size(1024).
    shard_scan_st_.emplace_back(scan_st);
    shard_scan_res_.emplace_back(scan_res);
    shard_scan_it_.emplace_back(scan_it);

    const CassRow *row= cass_iterator_get_row(scan_it);

    ScanHeapTuple<EloqKey, EloqRecord> heap_tuple(shard_scan_res_.size() - 1);
    EncodeCassRow(row, rec_sch_, heap_tuple.key_.get(), heap_tuple.rec_.get(),
                  heap_tuple.version_ts_, heap_tuple.deleted_);

    heap_cache_.push(std::move(heap_tuple));
  }
  else
  {
    cass_iterator_free(scan_it);
    cass_result_free(scan_res);
    cass_statement_free(scan_st);
  }

  return true;
}

template <bool Direction>
void HashPartitionCassScanner<Direction>::Current(
    txservice::TxKey &key, const txservice::TxRecord *&rec,
    uint64_t &version_ts, bool &deleted_)
{
  if (heap_cache_.size() == 0)
  {
    key= TxKey();
    rec= nullptr;
    return;
  }

  const ScanHeapTuple<EloqKey, EloqRecord> &top= heap_cache_.top();
  key= TxKey(top.key_.get());
  rec= top.rec_.get();
  version_ts= top.version_ts_;
  deleted_= top.deleted_;
}

template <bool Direction> bool HashPartitionCassScanner<Direction>::MoveNext()
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
  // sid is the offset in shard_scan_XX vectors.
  uint32_t sid= top.sid_;
  heap_cache_.pop();

  CassIterator *shard_it= shard_scan_it_.at(sid);
  if (cass_iterator_next(shard_it))
  {
    const CassRow *row= cass_iterator_get_row(shard_it);
    ScanHeapTuple<EloqKey, EloqRecord> heap_tuple(sid);

    EncodeCassRow(row, rec_sch_, heap_tuple.key_.get(), heap_tuple.rec_.get(),
                  heap_tuple.version_ts_, heap_tuple.deleted_);

    heap_cache_.push(std::move(heap_tuple));
  }
  else
  {
    cass_iterator_free(shard_it);
    shard_scan_it_[sid]= nullptr;

    if (cass_result_has_more_pages(shard_scan_res_[sid]))
    {
      cass_statement_set_paging_state(shard_scan_st_.at(sid),
                                      shard_scan_res_.at(sid));

      cass_result_free(shard_scan_res_.at(sid));
      shard_scan_res_[sid]= nullptr;

      CassFuture *scan_future=
          cass_session_execute(cass_session_, shard_scan_st_.at(sid));

      const CassResult *scan_res= cass_future_get_result(scan_future);
      cass_future_free(scan_future);

      if (scan_res == NULL)
      {
        return false;
      }

      CassIterator *scan_it= cass_iterator_from_result(scan_res);
      if (cass_iterator_next(scan_it))
      {
        shard_scan_res_[sid]= scan_res;
        shard_scan_it_[sid]= scan_it;

        const CassRow *row= cass_iterator_get_row(scan_it);
        ScanHeapTuple<EloqKey, EloqRecord> heap_tuple(sid);

        EncodeCassRow(row, rec_sch_, heap_tuple.key_.get(),
                      heap_tuple.rec_.get(), heap_tuple.version_ts_,
                      heap_tuple.deleted_);

        heap_cache_.push(std::move(heap_tuple));
      }
      else
      {
        cass_iterator_free(scan_it);
        cass_result_free(scan_res);
      }
    }
    else
    {
      cass_result_free(shard_scan_res_.at(sid));
      shard_scan_res_[sid]= nullptr;
    }
  }

  return true;
}

template <bool Direction> void HashPartitionCassScanner<Direction>::End() {}

template class HashPartitionCassScanner<true>;
template class HashPartitionCassScanner<false>;

#ifdef RANGE_PARTITION_ENABLED
bool RangePartitionCassScanner::Init()
{
  initialized_= false;
  CassError r= BuildScanPartitionPrepared();
  if (r != CASS_OK)
  {
    return false;
  }
  RangeScanPartitionFinder partition_finder;
  partition_iterator_= std::unique_ptr<PartitionIterator>();

  const EloqKey *start_key= nullptr;
  if (start_key_ == nullptr)
  {
    if (scan_forward_)
    {
      start_key= EloqKey::NegativeInfinity();
    }
    else
    {
      start_key= EloqKey::PositiveInfinity();
    }
  }
  else
  {
    start_key= start_key_;
  }

  PartitionResultType rt= partition_finder.FindScanPartitions(
      table_name_, *start_key, ng_id_, tx_service_, partition_iterator_);
  if (rt != PartitionResultType::NORMAL)
  {
    return false;
  }

  if (!ScanNextPartition())
  {
    return false;
  }

  initialized_= true;
  return true;
}

bool RangePartitionCassScanner::ScanNextPartition()
{
  if (partition_iterator_->MoveNext() != PartitionResultType::NORMAL)
  {
    return false;
  }

  Partition pt= partition_iterator_->Current();
  int32_t pk1= pt.Pk1();
  int16_t pk2= pt.Pk2();

  const CassCatalogInfo *cass_info=
      static_cast<const CassCatalogInfo *>(kv_info_);
  const std::string &physical_table_name=
      cass_info->GetKvTableName(table_name_);

  std::pair<CassStatement *, CassFuture *> rs=
      BuildScanPartitionStatement(physical_table_name, pk1, pk2, 10240);
  scan_st_= rs.first;
  CassFuture *scan_future= rs.second;
  const CassResult *scan_res= cass_future_get_result(scan_future);
  if (scan_res == NULL)
  {
    return false;
  }
  scan_res_.reset(scan_res);
  cass_future_free(scan_future);
  scan_it_= cass_iterator_from_result(scan_res_.get());
  return true;
}

void RangePartitionCassScanner::Current(txservice::TxKey &key,
                                        const txservice::TxRecord *&rec,
                                        uint64_t &version_ts, bool &deleted)
{
  if (!initialized_ || scan_finished_)
  {
    key= TxKey();
    rec= nullptr;
    return;
  }

  key= TxKey(&current_key_);
  rec= &current_rec_;
  version_ts= current_version_ts_;
  deleted= current_deleted_;
}

bool RangePartitionCassScanner::CassIteratorNext()
{
  if (cass_iterator_next(scan_it_))
  {
    const CassRow *row= cass_iterator_get_row(scan_it_);

    EncodeCassRow(row, rec_sch_, &current_key_, &current_rec_,
                  current_version_ts_, current_deleted_);
    return true;
  }
  else if (cass_result_has_more_pages(scan_res_.get()))
  {
    cass_statement_set_paging_state(scan_st_, scan_res_.get());
    const CassResult *old_scan_res= scan_res_.release();
    cass_result_free(old_scan_res);

    CassFuture *scan_future= cass_session_execute(cass_session_, scan_st_);
    const CassResult *scan_res= cass_future_get_result(scan_future);
    cass_future_free(scan_future);
    if (scan_res == NULL)
    {
      return false;
    }
    scan_res_.reset(scan_res);
    cass_iterator_free(scan_it_);
    scan_it_= nullptr;
    scan_it_= cass_iterator_from_result(scan_res_.get());
    return this->CassIteratorNext();
  }
  else
  {
    cass_iterator_free(scan_it_);
    scan_it_= nullptr;
    const CassResult *old_scan_res= scan_res_.release();
    cass_result_free(old_scan_res);
    cass_statement_free(scan_st_);

    scan_st_= nullptr;
    return false;
  }
}

bool RangePartitionCassScanner::MoveNext()
{
  if (!initialized_)
  {
    if (!Init())
    {
      return false;
    }
  }
  if (scan_finished_)
  {
    return false;
  }
  if (CassIteratorNext())
  {
    return true;
  }
  else
  {
    while (ScanNextPartition())
    { // Move to next partition
      if (CassIteratorNext())
      {
        return true; // break only if next partition has data, otherwise move
                     // to next partition
      }
    }
    scan_finished_= true;
    partition_iterator_->ReleaseReadLocks();
    return false; // run out of partitions
  }
}

void RangePartitionCassScanner::End()
{
  if (!scan_finished_)
  {
    scan_finished_= true;
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
#endif

} // namespace MyEloq
