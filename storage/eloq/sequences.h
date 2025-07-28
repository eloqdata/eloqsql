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
#include <string>
#include <unordered_map>
#include <mutex>
#include <chrono>
#include <condition_variable>

#include "my_global.h"
#include "field.h"
#include "table.h"
#include "eloq_catalog_factory.h"
#include "eloq_key.h"
#include "tx_service/include/store/data_store_handler.h"
#include "tx_service.h"

using namespace MyEloq;
using namespace txservice;

struct RangeID
{
  RangeID(std::string name) : seq_name_(name) {};
  RangeID(std::string name, uint64_t table_version)
      : seq_name_(name), table_version_(table_version) {};

  // lock when insert a record and need to apply an id
  std::mutex mutex_id_;
  // Current sequence name
  std::string seq_name_;
  // Current id, for every apply, it will return current id, then add rec_step
  // and save into curr_id
  int64_t curr_id_= 0;
  // The end for current range, if curr_id = range1_end, it
  // means all ids in this range have been used and it need use ids in
  // range2_start or apply new id rang.
  int64_t range_end_= -1;
  // The size of id range that acquired from server every time.
  int range_step_= -1;
  // The step between two neighbor record.
  int rec_step_= -1;
  // The version for the related table
  uint64_t table_version_;
  /**
   * @brief True, if the sequence is being advanced by one of the clients.
   *
   */
  bool seq_being_advanced_{false};
  /**
   * @brief In the thread-per-collection mode, a MySQL thread sleeps on this
   * condition variable, if it intends to advance the sequence but discovers
   * that another SQL thread is doing the job.
   *
   */
  std::condition_variable seq_cv_;
};

class Sequences
{
public:
  inline static const std::string mysql_seq_string{"./mysql/sequences"};
  inline static const TableName table_name_{
      mysql_seq_string.data(), mysql_seq_string.size(), TableType::Primary};
  // The max length for seq_name field
  inline static int seq_name_max_len{255};
  static const std::string GetSeqImage() { return instance_->seq_image_; }
  static void InitSequence(TxService *tx_service,
                           store::DataStoreHandler *storage_hd)
  {
    instance_= std::make_unique<Sequences>(tx_service, storage_hd);
    bool found= false;
    bool ok= instance_->storage_hd_->FetchTable(Sequences::table_name_,
                                                instance_->seq_image_, found,
                                                instance_->table_schema_ts_);
    if (!ok || !found)
    {
      return;
    }

    instance_->tableSchema_= std::make_unique<MysqlTableSchema>(
        instance_->table_name_, instance_->seq_image_,
        instance_->table_schema_ts_);
  }
  static void DeleteSeqence(const TableName &seq_name);
  static int64_t ApplyID(
      const TableName &seq_name, int64_t increment, int64_t desired_vals,
      int64_t &reserved_vals, uint64_t table_version,
      std::pair<const std::function<void()> *, const std::function<void()> *>
          coro_functors,
      const std::function<void()> *long_resume_func, int16_t thd_group_id);
  static int16_t GenHashPk1(const std::string &seq_name);
  static std::unique_ptr<EloqKey> GenKey(const std::string &seq_name);
  static const MysqlTableSchema *GetTableSchema()
  {
    return instance_->tableSchema_.get();
  };
  static void SetTableSchema(const MysqlTableSchema *table_schema);
  static void Destory() { instance_= nullptr; }
  static int UpdateAutoIncrement(
      std::string content, std::string dbName,
      std::pair<const std::function<void()> *, const std::function<void()> *>
          coro_functors,
      int16_t group_id);

  Sequences(TxService *tx_service, store::DataStoreHandler *storage_hd);
  ~Sequences()= default; // { delete tableSchema_.release(); }

protected:
  static int ApplyRangeId(
      RangeID *rid, bool prefetch,
      std::pair<const std::function<void()> *, const std::function<void()> *>
          coro_functors,
      int16_t thd_group_id);

protected:
  static std::unique_ptr<Sequences> instance_;

  std::unique_ptr<const MysqlTableSchema> tableSchema_{nullptr};
  std::string seq_image_{""};
  std::unordered_map<TableName, std::unique_ptr<RangeID>> seq_id_map_;
  std::mutex mutex_;
  TxService *tx_service_{nullptr};
  store::DataStoreHandler *storage_hd_{nullptr};
  uint64_t table_schema_ts_{0};
};
