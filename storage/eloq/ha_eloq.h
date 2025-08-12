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
/*
  Copyright (c) 2004, 2010, Oracle and/or its affiliates

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1335  USA */

/** @file ha_example.h

    @brief
  The ha_example engine is a stubbed storage engine for example purposes only;
  it does nothing at this point. Its purpose is to provide a source
  code illustration of how to begin writing new storage engines; see also
  /storage/example/ha_example.cc.

    @note
  Please read ha_example.cc before reading this file.
  Reminder: The example storage engine implements all methods that are
  *required* to be implemented. For a full list of all methods that you can
  implement, see handler.h.

   @see
  /sql/handler.h and /storage/example/ha_example.cc
*/
#pragma once

#ifdef USE_PRAGMA_INTERFACE
#pragma interface /* gcc class implementation */
#endif

#include "my_global.h" /* ulonglong */
#include "handler.h"   /* handler */
// #include "thr_lock.h"  /* THR_LOCK, THR_LOCK_DATA */
// #include "my_base.h"   /* ha_rows */
// #include "sql_class.h"

#include <unordered_map>
#include "ha_eloq_macro.h"
#include "eloq_key.h"
#include "eloq_schema.h"
#include "tx_service/include/statistics.h"
#include "tx_service/include/tx_request.h"
#include "tx_service/include/tx_execution.h"
#include "tx_service/include/tx_util.h"
#include "tx_service/include/store/data_store_scanner.h"
#include "eloq_catalog_factory.h"
#include "eloq_buff.h"
#include "eloq_key_def.h"
#include "tx_service/include/cc/reader_writer_cntl.h"

using namespace MyEloq;
using namespace txservice;

extern uint32_t node_id; // node id of itself
extern std::unique_ptr<store::DataStoreHandler> storage_hd;

/** @brief
  Eloq_share is a class that will be shared among all open handlers.
  This example implements the minimum of what you will probably need.
*/
class Eloq_share : public Handler_share
{
public:
  mysql_mutex_t mutex;
  THR_LOCK lock;
  Eloq_share();
  ~Eloq_share()
  {
    thr_lock_delete(&lock);
    mysql_mutex_destroy(&mutex);
  }
};

class MyEloqTx
{
public:
  constexpr static size_t too_many_open_tables= 100;

  MyEloqTx() : tables_in_use_(0), txm_(nullptr) {}

  MyEloqTx(const MyEloqTx &rhs)= delete;

  ~MyEloqTx()= default;

  txservice::TransactionExecution *Txm() { return txm_; }

  void Reset(txservice::TransactionExecution *tx, THD *thd)
  {
    tables_in_use_= 0;
    txm_= tx;
    thd_= thd;
    discovered_table_schemas_.clear();
    dirty_index_names_.clear();
    tx_err_code_= TxErrorCode::NO_ERROR;
  }

  void IncreTable() { ++tables_in_use_; }

  void DecreTable()
  {
    DBUG_ASSERT(tables_in_use_ > 0);
    --tables_in_use_;
  }

  size_t TablesInUse() const { return tables_in_use_; }

  bool Commit();

  void Abort()
  {
    if (thd_ != nullptr)
    {
      auto [yield_func, resume_func]= CoroFunctors();
      AbortTx(Txm(), yield_func, resume_func);
    }
    else
    {
      AbortTx(Txm());
    }

    ClearSchemaReaders();
    Reset(nullptr, nullptr);
  }

  std::pair<const MysqlTableSchema *, const MysqlTableSchema *>
  GetTableSchema(const TableName &table_name) const
  {
    auto schema_it= discovered_table_schemas_.find(table_name);
    if (schema_it == discovered_table_schemas_.end())
    {
      return {nullptr, nullptr};
    }
    else
    {
      return std::pair<const MysqlTableSchema *, const MysqlTableSchema *>(
          schema_it->second.first.get(), schema_it->second.second.get());
    }
  }

  void UpdateTableSchema(std::shared_ptr<const MysqlTableSchema> schema,
                         std::shared_ptr<const MysqlTableSchema> dirty_schema)
  {
    discovered_table_schemas_.emplace(
        std::piecewise_construct,
        std::forward_as_tuple(schema->GetBaseTableName().StringView(),
                              schema->GetBaseTableName().Type(),
                              schema->GetBaseTableName().Engine()),
        std::forward_as_tuple(schema, dirty_schema));

    if (schema != nullptr && dirty_schema != nullptr)
    {
      auto old_index_names= schema->IndexNames();
      auto new_index_names= dirty_schema->IndexNames();
      for (const TableName &new_index_name : new_index_names)
      {
        if (std::find(old_index_names.begin(), old_index_names.end(),
                      new_index_name) == old_index_names.end())
        {
          dirty_index_names_.emplace_back(new_index_name.StringView(),
                                          new_index_name.Type(),
                                          new_index_name.Engine());
        }
      }
    }
  }

  TxErrorCode ReadCatalog(const CatalogKey &catalog_key,
                          CatalogRecord &catalog_rec, bool is_for_write,
                          bool &exists)
  {
    std::pair<const std::function<void()> *, const std::function<void()> *>
        coro_functors= CoroFunctors();
    TxKey catalog_tx_key(&catalog_key);
    ReadTxRequest read_tx_req(&txservice::catalog_ccm_name, 0, &catalog_tx_key,
                              &catalog_rec, is_for_write, false, true, 0,
                              false, false, false, coro_functors.first,
                              coro_functors.second, txm_);
    return TxReadCatalog(txm_, read_tx_req, exists);
  }

  const std::vector<TableName> &DirtyIndexNames() const
  {
    return dirty_index_names_;
  }

  const MysqlTableSchema *GetCachedSchema(const TableName &tbl_name) const
  {
    auto it= cached_schemas_.find(tbl_name);
    if (it == cached_schemas_.end())
    {
      return nullptr;
    }
    else
    {
      const TableSchema *sch= it->second->GetObjectPtr();
      assert(sch != nullptr);
      return static_cast<const MysqlTableSchema *>(sch);
    }
  }

  void
  UpdateSchemaCntl(std::shared_ptr<ReaderWriterObject<TableSchema>> sch_cntl)
  {
    const TableSchema *schema= sch_cntl->GetObjectPtr();
    assert(schema != nullptr);

    cached_schemas_.emplace(
        std::piecewise_construct,
        std::forward_as_tuple(schema->GetBaseTableName().StringView(),
                              schema->GetBaseTableName().Type(),
                              schema->GetBaseTableName().Engine()),
        std::forward_as_tuple(sch_cntl));
  }

  std::pair<const std::function<void()> *, const std::function<void()> *>
  CoroFunctors() const;

  void ClearSchemaReader(const TableName &tbl_name);

  TxErrorCode tx_err_code_;

private:
  void ClearSchemaReaders();

  size_t tables_in_use_;
  txservice::TransactionExecution *txm_;
  /**
   * @brief A collection of discovered table schemas. Each time a new table is
   * discovered in the current tx, the tx puts a read lock on the table's
   * schema in the tx service and caches the schema in the collection.
   * Subsequent access of the same table's schema, such as checking the table
   * version or opening a new handler of this table, in the same tx scope uses
   * the cached schema, without reading the schema from the tx service.
   *
   */
  absl::flat_hash_map<TableName,
                      std::pair<std::shared_ptr<const MysqlTableSchema>,
                                std::shared_ptr<const MysqlTableSchema>>>
      discovered_table_schemas_; // not string owner, sv->MysqlTableSchema

  absl::flat_hash_map<TableName,
                      std::shared_ptr<ReaderWriterObject<TableSchema>>>
      cached_schemas_;

  // A collection of dirty index names.
  std::vector<TableName> dirty_index_names_;

  THD *thd_;
};

/** @brief
  Class definition for the storage engine
*/
class ha_eloq : public handler
{
  static constexpr size_t batch_read_size_= 2500;

private:
  THR_LOCK_DATA lock;      ///< MySQL lock
  Eloq_share *share;       ///< Shared lock info
  Eloq_share *get_share(); ///< Get the share

  size_t read_time_{0};
  size_t read_cnt_{0};

public:
  ha_eloq(handlerton *hton, TABLE_SHARE *table_arg);
  ~ha_eloq() {}

  /** @brief
    The name of the index type that will be used for display.
    Don't implement this method unless you really have indexes.
   */
  const char *index_type(uint inx) override { return "BTREE"; }

  /** @brief
    This is a list of flags that indicate what functionality the storage engine
    implements. The current table flags are documented in handler.h
  */
  ulonglong table_flags() const override;

  /** @brief
    This is a bitmap of flags that indicates how the storage engine
    implements indexes. The current index flags are documented in
    handler.h. If you do not implement indexes, just return zero here.

      @details
    part is the key part to check. First key part is 0.
    If all_parts is set, MySQL wants to know the flags for the combined
    index, up to and including 'part'.
  */
  ulong index_flags(uint inx, uint part, bool all_parts) const override;

  const key_map *keys_to_use_for_scanning() override
  {
    DBUG_ENTER_FUNC();

    DBUG_RETURN(&key_map_full);
  }

  /** @brief
    unireg.cc will call max_supported_record_length(), max_supported_keys(),
    max_supported_key_parts(), uint max_supported_key_length()
    to make sure that the storage engine can handle the data it is about to
    send. Return *real* limits of your storage engine here; MySQL will do
    min(your_limits, MySQL_limits) automatically.
   */
  uint max_supported_record_length() const override
  {
    return HA_MAX_REC_LENGTH;
  }

  /** @brief
    unireg.cc will call this to make sure that the storage engine can handle
    the data it is about to send. Return *real* limits of your storage engine
    here; MySQL will do min(your_limits, MySQL_limits) automatically.

      @details
    There is no need to implement ..._key_... methods if your engine doesn't
    support indexes.
   */
  uint max_supported_keys() const override { return MAX_KEY; }

  /** @brief
    unireg.cc will call this to make sure that the storage engine can handle
    the data it is about to send. Return *real* limits of your storage engine
    here; MySQL will do min(your_limits, MySQL_limits) automatically.

      @details
    There is no need to implement ..._key_... methods if your engine doesn't
    support indexes.
   */
  uint max_supported_key_length() const override { return 1024; }

  uint max_supported_key_part_length() const override
  {
    return MAX_INDEX_FIELD_LEN;
  }

  /** @brief
    Called in test_quick_select to determine if indexes should be used.
  */
  double scan_time() override
  {
    return (double) (stats.records + stats.deleted) / 20.0 + 10;
  }

  /** @brief
    This method will never be called if you do not implement indexes.
  */
  double read_time(uint index, uint ranges, ha_rows rows) override
  {
    DBUG_ENTER_FUNC();

    if (index != table->s->primary_key)
    {
      /* Non covering index range scan */

      /* +1 to make cost of pkey read smaller than skey read for statement
       * "UPDATE tbl SET col=val WHERE col1=part1_of_pk and col2=part2_of_pk "
       */
      DBUG_RETURN(handler::read_time(index, ranges, rows) + 1);
    }

    DBUG_RETURN((rows / 20.0) + 1);
  }

  /*
    Everything below are methods that we implement in ha_eloq.cc.

    Most of these methods are not obligatory, skip them and
    MySQL will treat them as not implemented
  */
  /** @brief
    We implement this in ha_example.cc; it's a required method.
  */
  int open(const char *name, int mode,
           uint test_if_locked) override; // required

  /** @brief
    We implement this in ha_example.cc; it's a required method.
  */
  int close(void) override; // required

  /** @brief
    We implement this in ha_example.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int write_row(const uchar *buf) override;

  /** @brief
    We implement this in ha_example.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int update_row(const uchar *old_data, const uchar *new_data) override;

  /** @brief
    We implement this in ha_example.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int delete_row(const uchar *buf) override;

  /** @brief
    We implement this in ha_example.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_read_map(uchar *buf, const uchar *key, key_part_map keypart_map,
                     enum ha_rkey_function find_flag) override;

  /** @brief
    We implement this in ha_example.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_next(uchar *buf) override;

  /** @brief
    We implement this in ha_example.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_prev(uchar *buf) override;

  /** @brief
    We implement this in ha_example.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_first(uchar *buf) override;

  /** @brief
    We implement this in ha_example.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_last(uchar *buf) override;

  int index_init(uint idx, bool sorted) override;

  int index_end() override;

  /* [RocksDB]
  The following two are currently only used for getting the range bounds
  from QUICK_SELECT_DESC.
  We don't need to implement prepare_index_key_scan[_map] because it is
  only used with HA_READ_KEY_EXACT and HA_READ_PREFIX_LAST where one
  can infer the bounds of the range being scanned, anyway.
*/
  int prepare_index_scan() override;
  int prepare_range_scan(const key_range *start_key,
                         const key_range *end_key) override;

  /** @brief
    Unlike index_init(), rnd_init() can be called two consecutive times
    without rnd_end() in between (it only makes sense if scan=1). In this
    case, the second call should prepare for the new table scan (e.g if
    rnd_init() allocates the cursor, the second call should position the
    cursor to the start of the table; no need to deallocate and allocate
    it again. This is a required method.
  */
  int rnd_init(bool scan) override; // required
  int rnd_end() override;
  int rnd_next(uchar *buf) override;            ///< required
  int rnd_pos(uchar *buf, uchar *pos) override; ///< required
  void position(const uchar *record) override;  ///< required
  int info(uint) override;                      ///< required
  int extra(enum ha_extra_function operation) override;
  int external_lock(THD *thd, int lock_type) override; ///< required
  int start_stmt(THD *const thd, thr_lock_type lock_type) override;
  int delete_all_rows(void) override;
  ha_rows records_in_range(uint inx, const key_range *min_key,
                           const key_range *max_key,
                           page_range *pages) override;
  int delete_table(const char *from) override;
  int create(const char *name, TABLE *form,
             HA_CREATE_INFO *create_info) override; ///< required
  bool get_error_message(int error, String *buf) override;
  enum_alter_inplace_result
  check_if_supported_inplace_alter(TABLE *altered_table,
                                   Alter_inplace_info *ha_alter_info) override;
  bool prepare_inplace_alter_table(TABLE *altered_table,
                                   Alter_inplace_info *ha_alter_info) override;

  bool inplace_alter_table(TABLE *altered_table,
                           Alter_inplace_info *ha_alter_info) override;

  bool commit_inplace_alter_table(TABLE *altered_table,
                                  Alter_inplace_info *ha_alter_info,
                                  bool commit) override;
  bool rowid_filter_push(Rowid_filter *rowid_filter) override;
  Item *idx_cond_push(uint keyno, Item *idx_cond) override;
  int reset() override;

  THR_LOCK_DATA **
  store_lock(THD *thd, THR_LOCK_DATA **to,
             enum thr_lock_type lock_type) override; ///< required

  void unlock_row() override;

  const COND *cond_push(const COND *cond) override;
  void cond_pop() override { return; }

  int analyze(THD *thd, HA_CHECK_OPT *check_opt) override;

  void increment_statistics(ulong SSV::*offset) const
  {
    handler::increment_statistics(offset);
  }
  void decrement_statistics(ulong SSV::*offset) const;

  void DecodeRecord(uchar *table_record, const EloqKey *key,
                    const EloqRecord *rec, bool is_ckpt= false,
                    bool is_deleted= false);

  // EloqRecord::Decode(const char *buf, size_t &offset, Schema
  // *pk_schema);
  void DecodeUniqueSkRecord(uchar *table_record, EloqKey *key, EloqRecord *rec,
                            const EloqKeySchema *sk_schema);

  int Update(const uchar *new_data, const uchar *old_data,
             std::unique_ptr<EloqKey> mono_key,
             std::unique_ptr<EloqRecord> new_mono_rec);

  int Delete(const uchar *old_data, std::unique_ptr<EloqKey> mono_key);

  int Insert(const uchar *new_data, std::unique_ptr<EloqKey> eloq_key,
             std::unique_ptr<EloqRecord> eloq_rec);

  int discover_check_version() override;

  void get_auto_increment(ulonglong offset, ulonglong increment,
                          ulonglong nb_desired_values, ulonglong *first_value,
                          ulonglong *nb_reserved_values) override;
  bool has_hidden_pk(const TABLE *const table) const;

  int alloc_key_buffers(const TABLE *const table_arg)
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));
  void free_key_buffers();

  static uint pk_index(const TABLE *const table_arg)
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));

  /** Initialize multi range read @see DsMrr_impl::dsmrr_init*/
  int multi_range_read_init(RANGE_SEQ_IF *seq, void *seq_init_param,
                            uint n_ranges, uint mode,
                            HANDLER_BUFFER *buf) override;

  /** Process next multi range read @see DsMrr_impl::dsmrr_next */
  int multi_range_read_next(range_id_t *range_info) override;

  /** Initialize multi range read and get information. */
  ha_rows multi_range_read_info_const(uint keyno, RANGE_SEQ_IF *seq,
                                      void *seq_init_param, uint n_ranges,
                                      uint *bufsz, uint *flags,
                                      Cost_estimate *cost) override;

  /** Initialize multi range read and get information. */
  ha_rows multi_range_read_info(uint keyno, uint n_ranges, uint keys,
                                uint key_parts, uint *bufsz, uint *flags,
                                Cost_estimate *cost) override;

  int multi_range_read_explain_info(uint mrr_mode, char *str,
                                    size_t size) override;
  /**
   * In this function, it will inputs a batch of keys and load all the related
   * records and saved then in storage engine's cache. This records will be get
   * from batch_get_record.
   */
  int batch_load_records(std::vector<uchar *> &vct_key) override;
  /**
   * After call batch_load_records to load a batch of records, this function
   * will get the records one by one from storage engine's cache.
   */
  int batch_get_record(uchar *buf) override;

  handler *clone(const char *name, MEM_ROOT *mem_root) override
  {
    ha_eloq *new_h= static_cast<ha_eloq *>(handler::clone(name, mem_root));
    if (new_h != nullptr)
    {
      new_h->is_mrr_sort_rowid_= is_mrr_sort_rowid_;
    }

    return new_h;
  }

  /**
   * @brief Start bulk-insert optimizations.
   */
  void start_bulk_insert(ha_rows rows, uint flags) override;
  /**
   * @brief End bulk-insert optimizations
   */
  int end_bulk_insert() override;

private:
  const txservice::TableName *GetBaseTableNameFromTableSchema() const
  {
    return &table_schema_->GetBaseTableName();
  }
  const EloqKeySchema *GetKeySchema();
  const EloqRecordSchema *GetRecordSchema();
  const txservice::KVCatalogInfo *GetKVCatalogInfo();
  uint64_t GetTableSchemaTs();

  /**
   * @brief Gets the table's schema given the session's tx. If the session has
   * read the table's schema, the session's tx must have put a read lock on the
   * table's schema. No one else could modify the schema. Returns the cached
   * schema. If the session has no cached schema, reads the table's schema from
   * the tx service via the session's tx. The read will put a read lock on the
   * schema.
   *
   * @param my_tx The session's tx.
   * @return Pointer to the table's schema
   */
  const MysqlTableSchema *DiscoverTableSchema(MyEloqTx *my_tx);

  bool IndexScanIsOpen();

  int PkIndexScanOpen(const txservice::TxKey *start_pk, bool start_inclusive,
                      const txservice::TxKey *end_key, bool end_inclusive,
                      ScanDirection direction, int used_key_parts);

  int SkIndexScanOpen(const txservice::TxKey *start_index_key,
                      bool start_inclusive,
                      const txservice::TxKey *end_index_key,
                      bool end_inclusive, ScanDirection direction,
                      int used_key_parts);

  int PkIndexScanNext(uchar *table_record);
  int SkIndexScanNext(uchar *table_record);
  int IndexScanClose();

  RecordStatus PkRead(MyEloqTx *my_tx, const TxKey &eloq_key,
                      EloqRecord &eloq_record, bool for_update= false,
                      uint64_t ts= 0, bool point_read_on_miss= false);

  std::pair<RecordStatus, uint64_t> SkRead(MyEloqTx *my_tx,
                                           const EloqKey &eloq_key,
                                           EloqRecord &eloq_record,
                                           bool for_update, uint kid);

  void AddPushedDownCondition(Item *cond_item);

  std::string PushedConditionString(
      const std::vector<txservice::store::DataStoreSearchCond>
          &pushdown_condition);

  int MatchIcp(uchar *table_record, const EloqKey &sk, const EloqRecord &rec,
               const EloqKeySchema *sk_schema);

  int MatchRowidFilter(const EloqKey &pk);

  int SetScanParameters(mysql::ha_rkey_function find_flag,
                        ScanDirection &direction, bool &inclusive,
                        bool &prefix_match);

  int ReadHiddenPkFromRowkey(uchar *hidden_key, const EloqKey *row_key);

  bool IsValidScanKey(const EloqKey *key);

  const txservice::KeySchema *KeySchema(uint inx) const;
  const Distribution *GetDistribution(uint inx) const;
  bool SuccessorForKeyRange(const key_range *range_key) const;
  EloqKey MonoMinKeyFromKeyRange(uint inx, const key_range *min_key) const;
  EloqKey MonoMaxKeyFromKeyRange(uint inx, const key_range *max_key,
                                 size_t min_key_size) const;
  EloqKey MonoMinPKeyFromKeyRange(const key_range *min_key) const;
  EloqKey MonoMinSKeyFromKeyRange(uint inx, const key_range *min_key) const;
  EloqKey MonoMaxPKeyFromKeyRange(const key_range *max_key,
                                  size_t min_key_size) const;
  EloqKey MonoMaxSKeyFromKeyRange(uint inx, const key_range *max_key,
                                  size_t min_key_size) const;

  void EncodeScanKey(EloqKey &mono_key, const uchar *mysql_key,
                     mysql::key_part_map keypart_map,
                     const mono_key_def *key_def);

  int PrepareScan(const uchar *mysql_start_key,
                  mysql::key_part_map keypart_map,
                  enum ha_rkey_function search_flag, mono_key_def *key_def,
                  uint32_t key_part_cnt, bool start_use_full_key,
                  bool &start_inclusive, bool &end_specified,
                  bool &end_inclusive, ScanDirection &direction);

  int PrepareForwardScanEndKey(mono_key_def *key_def, uint32_t key_part_cnt,
                               bool &end_inclusive);
  int PrepareBackwardScanEndKey(mono_key_def *key_def, uint32_t key_part_cnt,
                                bool &end_inclusive);

  std::pair<std::unique_ptr<EloqKey>, std::unique_ptr<EloqRecord>>
  PackKeyRecord(const uchar *buf);

  std::unique_ptr<EloqRecord> PackRecord(const uchar *buf);

  uint32_t PrefetchSize()
  {
    std::array<uint32_t, 5> boundaries= {1, 4, 16, 64, 256};

    size_t idx= 0;
    for (; idx < boundaries.size(); ++idx)
    {
      if (scan_batch_cnt_ < boundaries[idx])
      {
        break;
      }
    }

    return idx < boundaries.size() ? boundaries[idx] - 1
                                   : boundaries.back() - 1;
  }

  void SetupDecodeFlagOnFirstRead();

  const MY_BITMAP *DecodeSet() const;

  struct PushedCond
  {
    Item_func::Functype func_type_;
    Item_field *col_field_;
    Item_literal *val_field_;
  };

  std::vector<txservice::store::DataStoreSearchCond> BindPushedCond();

  int BulkInsert(const uchar *buf, std::unique_ptr<EloqKey> eloq_key,
                 std::unique_ptr<EloqRecord> eloq_rec);

  int BulkUniqueCheck(size_t bulk_size);
  /**
   * @brief The variable bookkeeps the end key of a backward scan. It is same
   * as handler::end_key, but is extraly maintained (same as MyRocks). This is
   * because a backward scan's end key is smaller than the start key, but MySQL
   * runtime insists on "start key" <= "end key" for any scans when passing the
   * scan boundaries into the prepare_range_scan() API. We need to bookkeep the
   * MySQL's "start key", which is the end key of a backward scan.
   *
   */
  key_range start_range_;
  const key_range *start_range_ptr_{nullptr};

  /**
   * @brief The original start key of a prefix index scan. The start key is
   * copied in the handler after the scan is open, when we need to use it to do
   * a prefix match for each returned key of the scan. Examples include (1) a
   * prefix match of an unique index. (2)an exact or prefix match of a
   * non-unique index.
   */
  std::unique_ptr<EloqKey> prefix_match_key_{nullptr};

  EloqKey search_key_;
  txservice::TxKey search_tx_key_{&search_key_};
  EloqKey scan_end_key_;
  txservice::TxKey scan_end_tx_key_{&scan_end_key_};

  uint64_t scan_alias_{UINT64_MAX};
  // index currently used if scan is active
  uint8_t scan_index_{MAX_INDEXES};
  ScanIndexType scan_index_type_;
  txservice::ScanOpenTxRequest scan_open_tx_req_;

  /**
   * @brief A flag indicating whether the scan is a checkpoint scan. A
   * checkpoint scan is a special scan that scans the in-memory cc map of
   * the specified table and returns the records that have not been flushed
   * to the data store.
   *
   */
  bool is_ckpt_scan_;
  bool ccm_scan_open_;
  txservice::ScanDirection scan_direction_;
  uint64_t scan_batch_cnt_;
  const EloqKey *ccm_scan_key_;
  const EloqRecord *ccm_scan_rec_;
  txservice::RecordStatus ccm_scan_rec_status_;
  std::unique_ptr<store::DataStoreScanner> storage_scanner_;
  bool advance_storage_scanner_;

  std::vector<txservice::ScanBatchTuple> scan_batch_;
  size_t scan_batch_idx_{UINT64_MAX};
  bool is_last_scan_batch_{false};
  std::vector<txservice::UnlockTuple> unlock_batch_;
  // The batch to save pk records when batch load PK from SK index in
  // SKScanNext
  std::vector<txservice::ScanBatchTuple> sk_pk_scan_batch_;
  // For mrr read, to save the current position for sk_pk_scan_batch_ in
  // batch_load_records, batch_get_record
  size_t sk_pk_scan_batch_idx_{UINT64_MAX};
  std::vector<EloqKey> batch_key_;
  std::vector<EloqRecord> batch_rec_;
  // To save the relations between sk in scan_batch_ and pk in
  // sk_pk_scan_batch_.
  // batch_order_[SK order in scan_batch_]==[PK order in sk_pk_scan_batch_]
  std::vector<uint64_t> batch_order_;

  EloqKey last_read_key_;
  txservice::TxKey last_read_tx_key_{&last_read_key_};
  EloqRecord last_read_record_;

  /**
   * @brief A string representation of the conditions pushed down to the
   * data store.
   *
   */
  std::vector<PushedCond> pushed_conds_;
  const txservice::TableName
      base_table_name_; // string owner, used in DiscoverTableSchema only

  /**
   * @brief The pointer to the MySQL schema cached in the tx service. Before a
   * table handler is used, MySQL either discovers the table or checks the
   * table schema's version. Checking the verison sets the handler's schema
   * pointer. Discovering table does not set the variable, because the
   * discovery function is not bound to any handler instance. Rather, the
   * schema pointer is set in the session-specific container. In this case, the
   * handler's schema pointer can only be set later by retrieving the session's
   * container.
   *
   */
  const MysqlTableSchema *table_schema_{nullptr};
  std::unique_ptr<EloqHiddenKeySchema> hidden_key_schema_{nullptr};

  std::shared_ptr<ReaderWriterObject<TableSchema>> schema_cntl_{nullptr};

  /**
   * @brief For DML that executes concurrently during a DDL operation, this
   *  pointer equal to non-nullptr, and be used to deal with double-write.
   */
  const MysqlTableSchema *table_dirty_schema_{nullptr};

  /**
   * @brief MySQL index number for duplicate key error.
   *
   */
  uint dup_errkey_;

  /* Primary Key encoder from KeyTupleFormat to StorageFormat */
  std::shared_ptr<mono_key_def> pk_descr_;

  /*
    Temporary space for packing VARCHARs (we provide it to
    pack_record()/pack_index_tuple() calls).
  */
  uchar *pack_buffer_;

  /*
  A buffer long enough to store table record
 */
  uchar *record_buffer_;

  /* Buffer for storing PK in StorageFormat */
  uchar *pk_packed_tuple_;

  /*
    Temporary buffers for storing the key part of the Key/Value pair
    for secondary indexes.
  */

  uchar *sk_packed_tuple_;

  mono_string_writer unpack_info_;

  /**
   * Both start_of_scan_ and need_setup_decode_flag_ is used for delay
   * scan open. Maybe we could remove them if we override
   * handler::column_bitmaps_signal() and implement scan reset. Currently we
   * implement scan reset by scan close and scan open.
   */
  bool start_of_scan_{false};

  /**
   * Delay SetupDecodeFlagOnFirstRead() to first read.
   *
   * According to: https://github.com/facebook/mysql-5.6/pull/1155
   *
   * There are two ways to SetupDecodeFlagOnFirstRead().
   *
   * 1) Following Innodb/MyRocks's way, which delay setting of decoder_context_
   * to first `*_read`, since MySQL may set read_set after `*_init` sometimes.
   *
   * 2) Overriding `handler::column_bitmaps_signal`, and set decoder_context_
   * whenever bitmap changes, besides `*_init`.
   *
   * Just like above link says, `handler::column_bitmaps_signal` is not
   * well-tested, and both Innodb/MyRocks choose 1), so does EloqDB.
   */
  bool need_setup_decode_flag_{false};

  static constexpr uint8_t DECODE_PK= 0b01;
  static constexpr uint8_t DECODE_PAYLOAD= 0b10;
  uint8_t decode_flag_{0u};

  /** The multi range read session object */
  DsMrr_impl m_ds_mrr_;
  /**When mysql create a statement and initialize it, it will a uint variable
   * mode to save the flags when call ha_eloq::multi_range_read_init. This
   * variable will save if it will save primary keys into a buffer and load a
   * batch of records according to the pri keys. */
  bool is_mrr_sort_rowid_{false};

  bool is_duplicate_error_{true};
  bool is_bulk_insert_{false};
  using BulkInsertBuffer=
      std::vector<std::pair<const EloqKey *, const EloqRecord *>>;
  BulkInsertBuffer pk_bulk_insert_buffer_;
  std::unordered_map<TableName, std::pair<uint64_t, BulkInsertBuffer>>
      unique_sk_bulk_insert_buffer_;
};
