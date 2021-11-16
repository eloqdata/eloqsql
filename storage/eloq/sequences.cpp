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
#include <future>

#include "sequences.h"

#include "eloq_key.h"
#include "store_handler/partition.h"

#include "tx_service/include/tx_request.h"
#include "tx_util.h"

using namespace txservice;

uint64_t GetCurrTime()
{
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

std::unique_ptr<Sequences> Sequences::instance_;

Sequences::Sequences(TxService *tx_service,
                     store::DataStoreHandler *storage_hd)
{
  tx_service_= tx_service;
  storage_hd_= storage_hd;
  tableSchema_= nullptr;
}

// When drop a table with auto increment id, call this function to remove its
// sequence from here.
void Sequences::DeleteSeqence(const TableName &seq_name)
{
  if (instance_ != nullptr)
  {
    std::unique_lock<std::mutex> lock(instance_->mutex_);
    instance_->seq_id_map_.erase(seq_name);
  }
}

// Apply a series of ids from a sequence with name='seq_name', If increment>0,
// the step between two neighbor ids will be increment, else the step will be
// rec_step_ in sequence. If it has not enough ids in this range, it will
// return surplus ids, else return 'desired_vals' ids.
int64_t Sequences::ApplyID(
    const TableName &seq_name, int64_t increment, int64_t desired_vals,
    int64_t &reserved_vals, uint64_t table_version,
    std::pair<const std::function<void()> *, const std::function<void()> *>
        coro_functors,
    const std::function<void()> *long_resume_func, int16_t thd_group_id)
{
  RangeID *rid= nullptr;
  instance_->mutex_.lock();
  auto iter= instance_->seq_id_map_.find(seq_name);
  if (iter == instance_->seq_id_map_.end())
  {
    auto it= instance_->seq_id_map_.try_emplace(
        seq_name, std::make_unique<RangeID>(seq_name.String(), table_version));
    rid= it.first->second.get();
  }
  else if (iter->second->table_version_ != table_version)
  {
    iter->second= std::make_unique<RangeID>(seq_name.String(), table_version);
    rid= iter->second.get();
  }
  else
  {
    rid= iter->second.get();
  }
  instance_->mutex_.unlock();

  std::unique_lock<std::mutex> seq_lk(rid->mutex_id_);
  while (rid->curr_id_ >= rid->range_end_)
  {
    if (rid->seq_being_advanced_)
    {
      if (thd_group_id < 0)
      {
        // The thread is in the thread-per-connection mode. Sleeps the thread
        // until the sequence has been advanced.
        rid->seq_cv_.wait(seq_lk);
      }
      else
      {
        // This thread is from a thread group and cannot be blocked. Rather
        // than sleeping, it releases the lock and yields to the next command
        // in the group.
        const std::function<void()> *yield_fp= coro_functors.first;
        // The resume functor first enqueues the current coroutine for
        // re-execution.
        (*long_resume_func)();
        seq_lk.unlock();
        // The yield functor yields the current thread to the next coroutine in
        // the group.
        (*yield_fp)();
        seq_lk.lock();
      }
    }
    else
    {
      rid->seq_being_advanced_= true;
      seq_lk.unlock();

      int i= 0;
      for (; i < 3; i++)
      {
        int err=
            Sequences::ApplyRangeId(rid, false, coro_functors, thd_group_id);
        if (err >= 0)
        {
          break;
        }
      }

      seq_lk.lock();
      rid->seq_being_advanced_= false;
      rid->seq_cv_.notify_all();

      if (i == 3)
      {
        reserved_vals= 0;
        return -1;
      }
    }
  }

  if (increment == -1)
  {
    increment= rid->rec_step_;
  }

  if (rid->curr_id_ + increment * (desired_vals - 1) < rid->range_end_)
  {
    reserved_vals= desired_vals;
  }
  else
  {
    reserved_vals= (rid->range_end_ - rid->curr_id_ - 1) / increment + 1;
  }

  int64_t val= rid->curr_id_;
  rid->curr_id_+= increment * reserved_vals;
  return val;
}

void Sequences::SetTableSchema(const MysqlTableSchema *table_schema)
{
  instance_->seq_image_= table_schema->SchemaImage();
  instance_->table_schema_ts_= table_schema->Version();
  instance_->tableSchema_= std::make_unique<MysqlTableSchema>(
      instance_->table_name_, instance_->seq_image_,
      instance_->table_schema_ts_);
}

// This method used to generate EloqKey. Below code is only
// tempory schema and do not support charset. It maybe change in following
// time.
std::unique_ptr<EloqKey> Sequences::GenKey(const std::string &seq_name)
{
  // For my_sequence, instead of storing packed key value, we store the
  // original sequence name string into EloqKey. The reasons we do it this
  // way are
  // 1. We do not have access to TABLE of my_sequence table needed to pack the
  // key.
  // 2. We only read/write to this table here in sequences.h/cpp.
  // 3. We only do == with my_sequence EloqKey, so even if we don't pack
  // the key it won't affect the comparison result of EloqKey.
  std::unique_ptr<EloqKey> mkey= std::make_unique<EloqKey>(
      reinterpret_cast<const uchar *>(seq_name.data()), seq_name.size());
  return mkey;
}

int16_t Sequences::GenHashPk1(const std::string &seq_name)
{
#ifdef USE_ONE_CASS_SHARD
  return 0;
#else
  std::unique_ptr<EloqKey> mkey= GenKey(seq_name);
  size_t hash= mkey->Hash();
  return (hash >> 10) & 0x3FF;
#endif
}

int Sequences::ApplyRangeId(
    RangeID *rid, bool prefetch,
    std::pair<const std::function<void()> *, const std::function<void()> *>
        coro_functors,
    int16_t thd_group_id)
{
  TransactionExecution *txm= NewTxInit(
      instance_->tx_service_, txservice::IsolationLevel::RepeatableRead,
      txservice::CcProtocol::Locking, UINT32_MAX, thd_group_id);

  if (txm == nullptr)
  {
    return -1;
  }

  std::string_view seq_tbl_name_view(mysql_seq_string);
  CatalogKey table_key(
      txservice::TableName(seq_tbl_name_view, txservice::TableType::Primary));
  TxKey tbl_tx_key(&table_key);
  CatalogRecord catalog_rec;
  ReadTxRequest read_catalog_req(&txservice::catalog_ccm_name, 0, &tbl_tx_key,
                                 &catalog_rec, false, false, true, 0, false,
                                 false, false, coro_functors.first,
                                 coro_functors.second, txm);

  bool exists= false;
  TxErrorCode err= TxReadCatalog(txm, read_catalog_req, exists);

  if (err != TxErrorCode::NO_ERROR || !exists)
  {
    txservice::AbortTx(txm, coro_functors.first, coro_functors.second);
    return -1;
  }

  std::unique_ptr<EloqKey> mkey= GenKey(rid->seq_name_);
  TxKey m_tx_key(mkey.get());
  std::unique_ptr<EloqRecord> mrec= std::make_unique<EloqRecord>();
  uint64_t schema_version= Sequences::GetTableSchema()->Version();
  ReadTxRequest read_req(&table_name_, schema_version, &m_tx_key, mrec.get(),
                         true, false, false, 0, false, false, true,
                         coro_functors.first, coro_functors.second, txm);

  txm->Execute(&read_req);
  read_req.Wait();

  if (read_req.IsError())
  {
    txservice::AbortTx(txm, coro_functors.first, coro_functors.second);
    return -1;
  }

  std::string buf;
  if (read_req.Result().first == RecordStatus::Normal)
  {
    mrec->Serialize(buf);
  }
  else
  {
    assert(read_req.Result().first == RecordStatus::Unknown ||
           read_req.Result().first == RecordStatus::Deleted);
    bool store_found= false;
    uint64_t sequence_ts= 0;
    bool success=
        instance_->storage_hd_->Read(table_name_, m_tx_key, *mrec, store_found,
                                     sequence_ts, Sequences::GetTableSchema());

    if (!success || !store_found)
    {
      txservice::AbortTx(txm, coro_functors.first, coro_functors.second);
      return -1;
    }

    buf.clear();
    mrec->Serialize(buf);

    if (read_req.Result().first == RecordStatus::Unknown)
    {
      ReadOutsideTxRequest read_outside(*mrec, !store_found, sequence_ts,
                                        nullptr, coro_functors.first,
                                        coro_functors.second, txm);
      txm->Execute(&read_outside);
      read_outside.Wait();
    }
  }

  int64_t start= 0;
  if (rid->range_step_ < 0)
  {
    size_t offset= buf.size() - sizeof(int64_t) * 2 - sizeof(int32_t) * 2;
    start= *(int64_t *) (buf.data() + offset);
    offset+= sizeof(int64_t);
    rid->range_step_= *(int32_t *) (buf.data() + offset);
    offset+= sizeof(int32_t);
    rid->rec_step_= *(int32_t *) (buf.data() + offset);
  }

  size_t offset= buf.size() - sizeof(int64_t);
  int64_t *pval= (int64_t *) (buf.data() + offset);
  if (*pval < start)
  {
    *pval= start;
  }

  int64_t curr_val= *pval;
  *pval+= rid->range_step_;

  offset= 0;
  mrec->Deserialize(buf.data(), offset);

  err= txm->TxUpsert(table_name_, schema_version, TxKey(std::move(mkey)),
                     std::move(mrec), OperationType::Update);

  if (err != TxErrorCode::NO_ERROR)
  {
    txservice::AbortTx(txm, coro_functors.first, coro_functors.second);
    return -1;
  }

  auto [success, commit_err]=
      txservice::CommitTx(txm, coro_functors.first, coro_functors.second);
  if (success)
  {
    std::unique_lock<std::mutex> lock(rid->mutex_id_);
    assert(rid->curr_id_ >= rid->range_end_);
    rid->curr_id_= curr_val;
    rid->range_end_= curr_val + rid->range_step_;
  }

  return 0;
}

int Sequences::UpdateAutoIncrement(
    std::string content, std::string dbName,
    std::pair<const std::function<void()> *, const std::function<void()> *>
        coro_functors,
    int16_t group_id)
{
  if (content.size() == 0)
  {
    return 0;
  }
  int64_t start= -1;
  int incr= -1;
  int range= -1;
  std::string seq_name;

  size_t pos2= 0;
  size_t pos1= 0;
  while (pos1 < content.size())
  {
    pos2= content.find(';', pos1);
    if (pos2 == std::string::npos)
      pos2= content.size();

    std::string sbs= content.substr(pos1, pos2 - pos1);
    size_t pos3= sbs.find('=');
    if (pos3 == std::string::npos)
      return -1;

    std::string key= sbs.substr(0, pos3);
    std::string val= sbs.substr(pos3 + 1);
    std::transform(key.begin(), key.end(), key.begin(), ::tolower);

    if (key.compare("table_name") == 0)
    {
      if (val[0] != '.')
      {
        val= "./" + dbName + "/" + val;
      }
      seq_name= val;
    }
    else if (key.compare("offset") == 0)
    {
      start= stoll(val);
    }
    else if (key.compare("increment") == 0)
    {
      incr= stoi(val);
    }
    else if (key.compare("range") == 0)
    {
      range= stoi(val);
    }
    else
    {
      return -1;
    }

    pos1= pos2 + 1;
  }

  if (seq_name.size() == 0 || (start < 0 && incr < 0 && range < 0))
  {
    return -1;
  }

  TransactionExecution *txm= txservice::NewTxInit(
      instance_->tx_service_, IsolationLevel::RepeatableRead,
      CcProtocol::Locking, UINT32_MAX, group_id);
  if (txm == nullptr)
  {
    return -1;
  }

  std::unique_ptr<EloqKey> mkey= GenKey(seq_name);
  TxKey m_tx_key(mkey.get());
  std::unique_ptr<EloqRecord> mrec= std::make_unique<EloqRecord>();
  uint64_t schema_version= Sequences::GetTableSchema()->Version();
  ReadTxRequest read_req(&table_name_, schema_version, &m_tx_key, mrec.get(),
                         true, false, false, 0, false, false, true,
                         coro_functors.first, coro_functors.second, txm);
  txm->Execute(&read_req);
  read_req.Wait();

  if (read_req.IsError())
  {
    txservice::AbortTx(txm, coro_functors.first, coro_functors.second);
    return -1;
  }

  // Here will get the record from ccmap and cassandra at the same time.
  // If the record in ccmap is Normal, it will use ccmap record and other case
  // it will use record from cassandra.
  std::string buf;
  int64_t *pval= nullptr;
  if (read_req.Result().first != RecordStatus::Unknown &&
      read_req.Result().first != RecordStatus::Deleted)
  {
    mrec->Serialize(buf);
    size_t offset= buf.size() - sizeof(int64_t);
    pval= (int64_t *) (buf.data() + offset);
  }
  else
  {
    assert(read_req.Result().first == RecordStatus::Unknown ||
           read_req.Result().first == RecordStatus::Deleted);

    bool store_found= false;
    uint64_t sequence_ts= 0;

    bool success=
        instance_->storage_hd_->Read(table_name_, m_tx_key, *mrec, store_found,
                                     sequence_ts, Sequences::GetTableSchema());

    if (success && store_found)
    {
      mrec->Serialize(buf);
    }
    else
    {
      txservice::AbortTx(txm, coro_functors.first, coro_functors.second);
      LOG(ERROR) << "Sequences::UpdateAutoIncrement: Failed to get sequence"
                    " record from data store.";
      return -1;
    }
  }

  // In now case only support to update auto increment parameters when the
  // table is empty. If the table has records, it need to check and remove the
  // ids in memory cache to avoid the conflicts. This need a conplex
  // transaction to ensure data consistency.
  size_t offset= buf.size() - sizeof(int64_t);
  pval= (int64_t *) (buf.data() + offset);
  if (*pval > 1)
  {
    txservice::AbortTx(txm, coro_functors.first, coro_functors.second);
    LOG(ERROR) << "Sequences::UpdateAutoIncrement: Only support updating auto"
                  " increment parameters in the case of the empty table.";
    return -1;
  }

  // sequneces table non-pk columns:
  // start bigint,node_step int,rec_step int,curr_val bigint
  if (start >= 0)
  {
    offset= buf.size() - sizeof(int64_t) * 2 - sizeof(int32_t) * 2;
    *(int64_t *) (buf.data() + offset)= start;
  }

  if (range > 0)
  {
    offset= buf.size() - sizeof(int64_t) - sizeof(int32_t) * 2;
    *(int32_t *) (buf.data() + offset)= range;
  }

  if (incr > 0)
  {
    offset= buf.size() - sizeof(int64_t) - sizeof(int32_t);
    *(int32_t *) (buf.data() + offset)= incr;
  }

  offset= 0;
  mrec->Deserialize(buf.data(), offset);

  TxErrorCode err=
      txm->TxUpsert(table_name_, schema_version, TxKey(std::move(mkey)),
                    std::move(mrec), OperationType::Update);
  if (err != TxErrorCode::NO_ERROR)
  {
    txservice::AbortTx(txm, coro_functors.first, coro_functors.second);
    LOG(ERROR)
        << "Sequences::UpdateAutoIncrement: Failed to Upsert sequence table.";
    return -1;
  }

  auto [success, commit_err]=
      txservice::CommitTx(txm, coro_functors.first, coro_functors.second);

  return success && commit_err == TxErrorCode::NO_ERROR ? 0 : -1;
}
