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

#include <sys/stat.h>

#include <algorithm>
#include <cassert>
#include <memory>
#include <stdlib.h>

#include "date.h"
#include "my_global.h"
#include "range_record.h"
#include "scan.h"
#include "schema.h"
#include "sql_type.h"
#include "eloq_key.h"
#include "table.h"
#include "tx_execution.h"
#include "tx_util.h"
#include "type.h"
#include "tx_key.h"
#include "sequences.h"

namespace MyEloq
{

enum PartitionResultType
{
  NORMAL,
  RUNOUT,
  COMMITFAILED,
  ERR
};

enum PartitionType
{
  RANGE,
  HASH
};

class Partition
{
public:
  Partition()= default;

  Partition(int32_t pk1) : pk1_(pk1) {}

  void Reset(int32_t pk1) { pk1_= pk1; }

#ifdef RANGE_PARTITION_ENABLED
  Partition(int32_t pk1, NodeGroupId range_owner,
            const std::vector<int32_t> &new_pk1,
            const std::vector<txservice::TxKey> &new_key,
            txservice::TxKey end_key)
      : pk1_(pk1), range_owner_(range_owner), new_pk1_(new_pk1),
        end_key_(std::move(end_key))
  {
    new_key_.reserve(new_key.size());
    for (const txservice::TxKey &nk : new_key)
    {
      new_key_.emplace_back(nk.GetShallowCopy());
    }
  }

  Partition(const Partition &rhs)
      : pk1_(rhs.pk1_), range_owner_(rhs.range_owner_), new_pk1_(rhs.new_pk1_),
        end_key_(rhs.end_key_.GetShallowCopy())
  {
    new_key_.reserve(rhs.new_key_.size());
    for (const TxKey &tx_key : rhs.new_key_)
    {
      new_key_.emplace_back(tx_key.GetShallowCopy());
    }
  }

  Partition(Partition &&rhs)
      : pk1_(rhs.pk1_), range_owner_(rhs.range_owner_),
        new_pk1_(std::move(rhs.new_pk1_)), new_key_(std::move(rhs.new_key_)),
        end_key_(std::move(rhs.end_key_))
  {
  }

  Partition &operator=(Partition &&other) noexcept
  {
    if (this != &other)
    {
      pk1_= other.pk1_;
      range_owner_= other.range_owner_;
      new_pk1_= std::move(other.new_pk1_);
      new_key_= std::move(other.new_key_);
      end_key_= std::move(other.end_key_);
    }
    return *this;
  }

  int32_t NewPk1(const txservice::TxKey *key)
  {
    if (!(*key < end_key_))
    {
      // key has to be in range
      assert(false);
    }
    if (new_pk1_.size())
    {
      // Does not belong to any of the new ranges
      if (*key < new_key_.front())
      {
        return -1;
      }

      uint idx= 1;
      for (; idx < new_key_.size(); idx++)
      {
        if (*key < new_key_.at(idx))
        {
          break;
        }
      }

      return new_pk1_.at(idx - 1);
    }
    else
    {
      return -1;
    }
  }

  NodeGroupId RangeOwner() { return range_owner_; }

  const txservice::TxKey *EndTxKey() { return &end_key_; }

  void Reset(int32_t pk1, NodeGroupId range_owner, txservice::TxKey end_key)
  {
    pk1_= pk1;
    range_owner_= range_owner;
    new_key_.clear();
    new_pk1_.clear();
    end_key_= std::move(end_key);
  }

  void Reset(int32_t pk1, NodeGroupId range_owner,
             const std::vector<int32_t> &new_pk1,
             const std::vector<txservice::TxKey> &new_key,
             txservice::TxKey end_key)
  {
    pk1_= pk1;
    range_owner_= range_owner;
    new_pk1_= new_pk1;
    new_key_.clear();
    for (auto &k : new_key)
    {
      new_key_.emplace_back(k.GetShallowCopy());
    }
    end_key_= std::move(end_key);
  }
#endif

  int32_t Pk1() { return pk1_; }
  int16_t Pk2() { return -1; }

  static int InitialPartitionId(std::string_view table_name_view)
  {
    size_t table_name_hash= std::hash<std::string_view>()(table_name_view);
    return table_name_hash & 0xFFF;
  }

private:
  int32_t pk1_{-1};
#ifdef RANGE_PARTITION_ENABLED
  NodeGroupId range_owner_{UINT32_MAX};
  std::vector<int32_t> new_pk1_;
  std::vector<txservice::TxKey> new_key_;
  txservice::TxKey end_key_;
#endif
};

// keep track of which partition is being searched
class PartitionIterator
{
public:
  PartitionIterator(PartitionType pt, const EloqKey &start_key)
      : pt_(pt), start_key_(start_key){};
  virtual ~PartitionIterator()= default;
  virtual Partition Current()= 0;
  virtual PartitionResultType MoveNext()= 0;
  virtual PartitionResultType ReleaseReadLocks()= 0;
  PartitionType GetPartitionType() { return pt_; }

protected:
  PartitionType pt_;
  const EloqKey &start_key_;
};

// for index scan
class ScanPartitionFinder
{
public:
  virtual ~ScanPartitionFinder()= default;
  virtual PartitionResultType FindScanPartitions(
      const txservice::TableName &table_name, const EloqKey &start_key,
      uint32_t ng_id, txservice::TxService *tx_service,
      std::unique_ptr<PartitionIterator> &out_partition_iterator)= 0;
};

// for index lookup
class PartitionFinder
{
public:
  virtual ~PartitionFinder()= default;
  virtual PartitionResultType
  FindPartition(const txservice::TableName &table_name,
                const txservice::TxKey &tx_key, Partition &out_partition)= 0;
  virtual PartitionResultType
  FindPartitions(const txservice::TableName &table_name,
                 std::vector<txservice::FlushRecord> &ckpt_rec,
                 std::vector<std::pair<uint, Partition>> &out_partition)= 0;
  virtual PartitionResultType ReleaseReadLocks()= 0;
};

#ifdef RANGE_PARTITION_ENABLED
class RangePartitionIterator : public PartitionIterator
{
public:
  RangePartitionIterator(txservice::TxService *tx_service,
                         const txservice::TableName &table_name,
                         const EloqKey &start_key, uint32_t ng_id)
      : PartitionIterator(PartitionType::RANGE, start_key),
        tx_service_(tx_service), txm_(nullptr),
        range_table_name_(table_name.StringView(),
                          txservice::TableType::RangePartition),
        scan_alias_(0), current_pk1_(-1), finished_(false), scan_batch_idx_(0),
        initialized_(false), ng_id_(ng_id)
  {
  }

  bool InitTxm()
  {
    assert(txm_ == nullptr);
    txm_= txservice::NewTxInit(tx_service_,
                               txservice::IsolationLevel::RepeatableRead,
                               txservice::CcProtocol::Locking, ng_id_);
    return !(txm_ == nullptr);
  }

  PartitionResultType Init()
  {
    initialized_= false;

    if (!InitTxm())
    {
      txservice::AbortTx(txm_);
      return PartitionResultType::ERR;
    }

    scan_batch_idx_= 0;
    scan_batch_.clear();

    // issue scan request
    TxKey start_tx_key(&start_key_);
    txservice::ScanOpenTxRequest scan_open_req(
        &range_table_name_, 0, txservice::ScanIndexType::Primary,
        &start_tx_key, true, nullptr, false, txservice::ScanDirection::Forward,
        false, false, false, true);
    txm_->Execute(&scan_open_req);
    scan_open_req.Wait();

    if (scan_open_req.IsError())
    {
      txservice::AbortTx(txm_);
      // txm has been recycled, so reset the pointer.
      txm_= nullptr;
      return PartitionResultType::ERR;
    }

    scan_alias_= scan_open_req.Result();

    txservice::ScanBatchTxRequest scan_batch_req(
        scan_alias_, range_table_name_, &scan_batch_);
    txm_->Execute(&scan_batch_req);
    scan_batch_req.Wait();

    if (scan_batch_req.IsError())
    {
      txservice::AbortTx(txm_);
      txm_= nullptr;
      return PartitionResultType::ERR;
    }

    initialized_= true;
    return PartitionResultType::NORMAL;
  }

  Partition Current() override
  {
    assert(current_pk1_ >= 0);
    return Partition(current_pk1_, range_owner_, current_new_pk1_, new_key_,
                     TxKey(end_key_));
  }

  PartitionResultType MoveNext() override
  {
    if (finished_)
      return PartitionResultType::RUNOUT;

    if (!initialized_)
    {
      PartitionResultType r= Init();
      if (r != PartitionResultType::NORMAL)
      {
        return r;
      }
    }

    const txservice::TxKey *scan_key= nullptr;
    const txservice::RangeRecord *scan_rec= nullptr;

    if (scan_batch_idx_ < scan_batch_.size())
    {
      scan_key= &scan_batch_[scan_batch_idx_].key_;
      scan_rec= static_cast<const txservice::RangeRecord *>(
          scan_batch_[scan_batch_idx_].record_);
      ++scan_batch_idx_;
    }
    else if (!scan_batch_.empty())
    {
      scan_batch_idx_= 0;
      scan_batch_.clear();

      txservice::ScanBatchTxRequest scan_batch_req(
          scan_alias_, range_table_name_, &scan_batch_);
      txm_->Execute(&scan_batch_req);
      scan_batch_req.Wait();

      if (scan_batch_req.IsError())
      {
        txservice::AbortTx(txm_);
        // The txm has been recycled.
        txm_= nullptr;
        return PartitionResultType::ERR;
      }

      if (!scan_batch_.empty())
      {
        scan_key= &scan_batch_[scan_batch_idx_].key_;
        scan_rec= static_cast<const txservice::RangeRecord *>(
            scan_batch_[scan_batch_idx_].record_);
        ++scan_batch_idx_;
      }
    }

    if (scan_key == nullptr)
    {
      txservice::ScanCloseTxRequest close_req(scan_batch_, scan_batch_idx_,
                                              scan_alias_, range_table_name_);
      txm_->Execute(&close_req);
      close_req.Wait();
      finished_= true;
      return PartitionResultType::RUNOUT;
    }

    const txservice::RangeInfo *range_info= scan_rec->GetRangeInfo();
    const TemplateRangeInfo<EloqKey> *typed_range_info=
        static_cast<const TemplateRangeInfo<EloqKey> *>(range_info);

    current_pk1_= range_info->PartitionId();
    key_= typed_range_info->StartKey();
    end_key_= typed_range_info->EndKey();
    range_owner_= scan_rec->GetRangeOwnerNg()->BucketOwner();
    if (range_info->IsDirty())
    {
      current_new_pk1_= *range_info->NewPartitionId();
      for (auto &key : *range_info->NewKey())
      {
        new_key_.push_back(key.GetShallowCopy());
      }
    }
    else
    {
      current_new_pk1_.clear();
      new_key_.clear();
    }

    return PartitionResultType::NORMAL;
  }

  PartitionResultType ReleaseReadLocks() override
  {
    if (txm_ == nullptr)
    {
      return PartitionResultType::NORMAL;
    }

    if (!finished_)
    {
      txservice::ScanCloseTxRequest close_req(scan_batch_, scan_batch_idx_,
                                              scan_alias_, range_table_name_);
      txm_->Execute(&close_req);
      close_req.Wait();
      finished_= true;
    }
    // TODO(Xiao Ji): commit the txm_ and start an new txm_ for continue
    // iterating
    auto [success, err]= txservice::CommitTx(txm_);

    // After commit/abort, the txm has been recycled, should reset this
    // pointer.
    txm_= nullptr;

    if (err != TxErrorCode::NO_ERROR)
    {
      // Abort() will be called internally if error occurred during commit.
      return PartitionResultType::ERR;
    }

    if (success)
    {
      return PartitionResultType::NORMAL;
    }
    else
    {
      return PartitionResultType::COMMITFAILED;
    }
  }

private:
  txservice::TxService *tx_service_;
  txservice::TransactionExecution *txm_;
  txservice::TableName
      range_table_name_; // not string owner, sv -> MysqlTableSchema

  // txservice::TxKey *_temp_start_key_; //TODO: for continue iteration after
  // commit txm_
  size_t scan_alias_;
  int32_t current_pk1_;
  const EloqKey *key_;
  const EloqKey *end_key_;
  NodeGroupId range_owner_;
  std::vector<int32_t> current_new_pk1_;
  std::vector<txservice::TxKey> new_key_;
  bool finished_;

  std::vector<txservice::ScanBatchTuple> scan_batch_;
  size_t scan_batch_idx_{0};
  bool initialized_{false};
  uint32_t ng_id_;
};
#endif

class HashPartitionIterator : public PartitionIterator
{
public:
  HashPartitionIterator(const EloqKey &start_key)
      : PartitionIterator(PartitionType::HASH, start_key), current_idx_(-1)
  {
  }

  Partition Current() override
  {
    assert(current_idx_ >= 0);
    return Partition(current_idx_);
  }

  PartitionResultType MoveNext() override
  {
    if (current_idx_ < 1024)
    {
      current_idx_++;
      return PartitionResultType::NORMAL;
    }
    return PartitionResultType::RUNOUT;
  }

  PartitionResultType ReleaseReadLocks() override
  {
    return PartitionResultType::NORMAL;
  }

private:
  int32_t current_idx_;
};

class HashScanPartitionFinder : public ScanPartitionFinder
{
public:
  PartitionResultType FindScanPartitions(
      const txservice::TableName &table_name, const EloqKey &start_key,
      uint32_t ng_id, txservice::TxService *tx_service,
      std::unique_ptr<PartitionIterator> &out_partition_iterator) override
  {
    out_partition_iterator= std::make_unique<HashPartitionIterator>(start_key);
    return PartitionResultType::NORMAL;
  };

private:
};

class HashPartitionFinder : public PartitionFinder
{
public:
  HashPartitionFinder(){};
  ~HashPartitionFinder()= default;

  PartitionResultType FindPartition(const txservice::TableName &table_name,
                                    const txservice::TxKey &key,
                                    Partition &out_partition) override
  {
#ifdef USE_ONE_CASS_SHARD
    int32_t pk1_hash= 0;
#else
    int32_t pk1_hash;
    if (table_name.StringView() == Sequences::mysql_seq_string)
    {
      pk1_hash= 0;
    }
    else
    {
      size_t hash= key.Hash();
      // In the tx service, we use the lower 10 bits to distribute keys amongs
      // cores in a single node and the remaining higher bits to distribute
      // among nodes. We assume for now that the first level partitions are
      // 1024. This number needs to increase if there are more than 1024 nodes
      // for the tx service.
      pk1_hash= (hash >> 10) & 0x3FF;
    }
#endif
    out_partition.Reset(pk1_hash);
    return PartitionResultType::NORMAL;
  };

  /**
   * @brief  Set out_partition
   * with the index of the first record in this partition and its partition
   * information. For example if the partitions of the ckpt_rec passed in are
   * [1,3,2,5,2], we will first sort the ckpt_rec so that they are in order
   * [1,2,2,3,5]. out_partition will be
   * [(0, partition 1), (1, partition 2), (3, partition 3), (4, partition 5)].
   *
   * @param table_name
   * @param ckpt_rec
   * @param out_partition
   * @return PartitionResultType
   */
  PartitionResultType FindPartitions(
      const txservice::TableName &table_name,
      std::vector<txservice::FlushRecord> &ckpt_rec,
      std::vector<std::pair<uint, Partition>> &out_partition) override
  {
#ifdef USE_ONE_CASS_SHARD
    out_partition.emplace_back(0, Partition(0));
#else
    Partition part;
    int32_t last_pk= -1;
    for (uint idx= 0; idx < ckpt_rec.size(); idx++)
    {
      txservice::FlushRecord &cur_rec= ckpt_rec[idx];
      TxKey tx_key= cur_rec.Key();
      FindPartition(table_name, tx_key, part);
      if (part.Pk1() != last_pk)
      {
        out_partition.emplace_back(idx, part);
        last_pk= part.Pk1();
      }
    }
#endif
    return PartitionResultType::NORMAL;
  };

  PartitionResultType ReleaseReadLocks() override
  {
    return PartitionResultType::NORMAL;
  };
};

#ifdef RANGE_PARTITION_ENABLED
class RangePartitionFinder : public PartitionFinder
{
public:
  RangePartitionFinder()= default;
  ~RangePartitionFinder()= default;

  bool Init(txservice::TxService *tx_service, uint32_t ng_id)
  {
    txm_= txservice::NewTxInit(tx_service,
                               txservice::IsolationLevel::RepeatableRead,
                               txservice::CcProtocol::Locking, ng_id);
    return txm_ != nullptr;
  }

  PartitionResultType FindPartition(const txservice::TableName &table_name,
                                    const txservice::TxKey &key,
                                    Partition &out_partition) override
  {
    assert(txm_ != nullptr);

    // Convert primary/secondary table type to range table type.
    txservice::TableName range_table_name= txservice::TableName{
        table_name.StringView(), txservice::TableType::RangePartition};
    txservice::RangeRecord range_record;
    txservice::ReadTxRequest read_req(&range_table_name, 0, &key,
                                      &range_record, false, false, true);

    txm_->Execute(&read_req);
    read_req.Wait();

    if (read_req.IsError())
    {
      txservice::AbortTx(txm_);
      return PartitionResultType::ERR;
    }

    // It must have a range found if range partition is enabled
    assert(read_req.Result().first == txservice::RecordStatus::Normal);

    const txservice::RangeInfo *range_entry= range_record.GetRangeInfo();
    int32_t pk1= range_entry->PartitionId();
    NodeGroupId range_owner= range_record.GetRangeOwnerNg()->BucketOwner();
    if (range_entry->IsDirty())
    {
      out_partition.Reset(pk1, range_owner, *range_entry->NewPartitionId(),
                          *range_entry->NewKey(), range_entry->EndTxKey());
    }
    else
    {
      out_partition.Reset(pk1, range_owner, range_entry->EndTxKey());
    }

    return PartitionResultType::NORMAL;
  }

  /**
   * @brief  Set out_partition with the index of the first record in this
   * partition and its partition information. For example if the partitions of
   * the ckpt_rec passed in are [1,3,2,5,2], we will first sort the ckpt_rec so
   * that they are in order [1,2,2,3,5]. out_partition will be
   * [(0, partition 1), (1, partition 2), (3, partition 3), (4, partition 5)].
   *
   * @param table_name
   * @param ckpt_rec
   * @param out_partition
   * @return PartitionResultType
   */
  PartitionResultType FindPartitions(
      const txservice::TableName &table_name,
      std::vector<txservice::FlushRecord> &ckpt_rec,
      std::vector<std::pair<uint, Partition>> &out_partition) override
  {
    if (!ckpt_rec.size())
    {
      return PartitionResultType::ERR;
    }

    // ckpt vec is already sorted in CkptScanCc
    Partition part;
    int32_t last_pk= -1;
    const EloqKey *partition_end_key= nullptr;
    for (uint idx= 0; idx < ckpt_rec.size(); idx++)
    {
      txservice::FlushRecord &cur_rec= ckpt_rec[idx];
      TxKey tx_key= cur_rec.Key();
      const EloqKey *key= tx_key.GetKey<EloqKey>();
      if (last_pk == -1 ||
          (partition_end_key != nullptr && !(*key < *partition_end_key)))
      {
        if (FindPartition(table_name, tx_key, part) !=
            PartitionResultType::NORMAL)
        {
          return PartitionResultType::ERR;
        }
        partition_end_key= part.EndTxKey()->GetKey<EloqKey>();
      }
      int32_t pk= part.NewPk1(&tx_key);
      if (pk == -1)
      {
        pk= part.Pk1();
      }
      if (pk != last_pk)
      {
        out_partition.emplace_back(idx, part);
        last_pk= pk;
      }
    }
    return PartitionResultType::NORMAL;
  }

  PartitionResultType ReleaseReadLocks() override
  {
    if (txm_ == nullptr)
    {
      return PartitionResultType::ERR;
    }

    auto [success, err]= txservice::CommitTx(txm_);

    // If has error during commit, will abort it internally, so do not need to
    // call Abort() manually.
    // After commit/abort, the txm will be recycled.
    txm_= nullptr;

    return success ? PartitionResultType::NORMAL
                   : PartitionResultType::COMMITFAILED;
  }

private:
  txservice::TransactionExecution *txm_;
};

class RangeScanPartitionFinder : public ScanPartitionFinder
{
public:
  RangeScanPartitionFinder()= default;

  PartitionResultType FindScanPartitions(
      const txservice::TableName &table_name, const EloqKey &start_key,
      uint32_t ng_id, txservice::TxService *tx_service,
      std::unique_ptr<PartitionIterator> &out_partition_iterator) override
  {
    // Force to load range cc map with a read
    RangePartitionFinder range_partition_finder;
    range_partition_finder.Init(tx_service, ng_id);
    Partition out_partition;
    range_partition_finder.FindPartition(table_name, TxKey(&start_key),
                                         out_partition);
    range_partition_finder.ReleaseReadLocks();

    out_partition_iterator= std::make_unique<RangePartitionIterator>(
        tx_service, table_name, start_key, ng_id);
    return PartitionResultType::NORMAL;
  }
};
#endif

class PartitionFinderFactory
{
public:
  static std::unique_ptr<PartitionFinder> Create()
  {
#ifdef RANGE_PARTITION_ENABLED
    return std::make_unique<RangePartitionFinder>();
#else
    return std::make_unique<HashPartitionFinder>();
#endif
  }

  static std::unique_ptr<ScanPartitionFinder>
  CreateForScan(txservice::TxService *tx_service)
  {
#ifdef RANGE_PARTITION_ENABLED
    return std::make_unique<RangeScanPartitionFinder>();
#else
    return std::make_unique<HashScanPartitionFinder>();
#endif
  }
};
} // namespace MyEloq
