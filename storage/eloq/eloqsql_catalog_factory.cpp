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
#include "type.h"
#define MYSQL_SERVER 1

#include "eloqsql_catalog_factory.h"

#include <string>
#include <memory>
#include <utility>

#include "my_global.h"
#include "my_base.h"
#include "sql_class.h"

#include "store_handler/kv_store.h"
#include "eloqsql_key.h"

#include "cc/template_cc_map.h"
#include "constants.h"
#include "cc/range_cc_map.h"
// #include "sequences.h"
#include "range_record.h"
#include "range_slice.h"
#include "tx_service/include/sequences/sequences.h"

MYSQL_THD create_background_thd();
void destroy_background_thd(MYSQL_THD thd);
namespace MyEloq
{
/**
 * @brief Extract uuid value from packed hidden pk.
 *
 * @param hidden_pk_id
 * @param row_key
 * @return bool
 */
bool ReadHiddenPkFromRowkey(uchar *hidden_pk_id, const EloqKey *row_key)
{
  Slice rowkey_slice= row_key->PackedValueSlice();

  // Get hidden primary key from old key slice
  mono_string_reader reader(&rowkey_slice);

  // UUID length is 16
  const int length= 16;
  const uchar *from= reinterpret_cast<const uchar *>(reader.read(length));
  if (from == nullptr)
  {
    /* Mem-comparable image doesn't have enough bytes */
    return false;
  }

  memcpy(hidden_pk_id, from, length);
  return true;
}

MysqlSkEncoder::MysqlSkEncoder(const txservice::TableName &index_name,
                               const MysqlTableSchema *table_schema)
    : mysql_table_schema_(table_schema)
{
  uint32_t packed_key_len= 0;

  secondary_key_schema_= mysql_table_schema_->IndexKeySchema(index_name);
  assert(secondary_key_schema_ != nullptr);

  const EloqKeySchema *sk_schema= static_cast<const EloqKeySchema *>(
      secondary_key_schema_->sk_schema_.get());

  packed_key_len= sk_schema->KeyDefinition()->max_storage_fmt_length();

  pack_buffer_vec_.reserve(packed_key_len);
  sk_packed_tuple_vec_.reserve(packed_key_len);

  // Create a MYSQL_THD for a background thread.
  gen_packed_sk_thd_= create_background_thd();
  thd_proc_info(gen_packed_sk_thd_, "EloqDB generate packed sk");
  gen_packed_sk_thd_->lex->sql_command= SQLCOM_ALTER_TABLE;
  set_current_thd(gen_packed_sk_thd_);

  mysql::TABLE_SHARE *mysql_table_share=
      const_cast<mysql::TABLE_SHARE *>(mysql_table_schema_->TableShare());
  assert(mysql_table_share->db.str != nullptr &&
         mysql_table_share->table_name.str != nullptr);
  // Open table
  open_table_from_share(gen_packed_sk_thd_, mysql_table_share, &empty_clex_str,
                        0, READ_ALL | OPEN_FRM_FILE_ONLY, 0, &mysql_table_,
                        true);

  // Decode the index columns. Notice that SkEncoder is constructed in
  // tx_service, and knowledge about which columns to read is not passed down
  // from SQL level. Construct the knowledge manually.
  MarkIndexColumnsForRead();
}

MysqlSkEncoder::~MysqlSkEncoder()
{
  // 1. Free the THD object
  if (gen_packed_sk_thd_ != nullptr)
  {
    auto save_thd= current_thd;
    if (save_thd == gen_packed_sk_thd_)
    {
      set_current_thd(nullptr);
    }

    destroy_background_thd(gen_packed_sk_thd_);
    gen_packed_sk_thd_= nullptr;
  }

  // 2. Free the TABLE object.
  mysql::closefrm(&mysql_table_);

  // 3. Free the packed buffer
  pack_buffer_vec_.clear();
  sk_packed_tuple_vec_.clear();
}

int32_t MysqlSkEncoder::AppendPackedSk(
    const txservice::TxKey *pk, const txservice::TxRecord *record,
    uint64_t version_ts, std::vector<txservice::WriteEntry> &dest_vec)
{
  assert(secondary_key_schema_ != nullptr);
  const EloqKeySchema *sk_schema= static_cast<const EloqKeySchema *>(
      secondary_key_schema_->sk_schema_.get());

  mysql::TABLE *table= const_cast<mysql::TABLE *>(&mysql_table_);
  assert(table != nullptr && table->record[0] != nullptr);

  uchar *buf;
  buf= table->record[0];

  const EloqKey *mono_key= pk->GetKey<EloqKey>();
  const EloqRecord *mono_rec= static_cast<const EloqRecord *>(record);

  // Decode from TxRecord to buf.
  if (!DecodeRecord(buf, mono_key, mono_rec))
  {
    LOG(ERROR) << "Failed to decode record during AppendPackedSk.";
    return -1;
  }

  // For virtual columns, compute those values.
  // Copied from TABLE::update_virtual_fields.
  if (table->vfield)
  {
    Field **vfield_ptr, *vf;
    bool update= 0;

    // Mark virtual columns for update/insert commands
    // Copied from TABLE::mark_virtual_columns_for_write
    for (vfield_ptr= table->vfield; *vfield_ptr; vfield_ptr++)
    {
      vf= *vfield_ptr;
      if (bitmap_is_set(table->write_set, vf->field_index))
      {
        table->mark_virtual_column_with_deps(vf);
      }
      else if (vf->vcol_info->stored_in_db ||
               (vf->flags & (PART_KEY_FLAG | FIELD_IN_PART_FUNC_FLAG |
                             PART_INDIRECT_KEY_FLAG)))
      {
        bitmap_set_bit(table->write_set, vf->field_index);
        table->mark_virtual_column_with_deps(vf);
      }
    }

    // Iterate over virtual fields in the table
    for (vfield_ptr= table->vfield; *vfield_ptr; vfield_ptr++)
    {
      vf= (*vfield_ptr);
      Virtual_column_info *vcol_info= vf->vcol_info;
      assert(vcol_info && vcol_info->expr);

      update= bitmap_is_set(table->read_set, vf->field_index);
      // For virtual column whose type is STORED, its value has already been
      // fetched from data store, so there is no need to generate the value of
      // this column.
      if (update && !vf->vcol_info->stored_in_db)
      {
        vcol_info->expr->save_in_field(vf, 0);
      }
    }
  }

  // Temporary space for packing VARCHARs (we provide it to
  // pack_record()/pack_index_tuple() calls).
  uchar *pack_buffer= const_cast<uchar *>(pack_buffer_vec_.data());
  // Temporary buffers for storing the key part of the Key/Value pair
  // for secondary indexes.
  uchar *sk_packed_tuple= const_cast<uchar *>(sk_packed_tuple_vec_.data());
  mono_string_writer unpack_info;

  size_t size;
  size_t unique_sk_packed_size= 0;
  const mysql::TABLE_SHARE *mysql_table_share=
      mysql_table_schema_->TableShare();
  if (mysql_table_share->primary_key == MAX_INDEXES)
  {
    // Hidden pk
    // Pack function needs hidden pk value because we pack hidden pk at
    // the end of secondary key cols to make sure the key is unique.
    uchar uuid_buffer[MY_UUID_SIZE];
    if (!ReadHiddenPkFromRowkey(uuid_buffer, mono_key))
    {
      LOG(ERROR) << "Failed to read hidden pk during AppendPackedSk.";
      return -1;
    }

    size= sk_schema->KeyDefinition()->pack_record(
        table, pack_buffer, buf, sk_packed_tuple, &unpack_info, false,
        &unique_sk_packed_size, uuid_buffer);
  }
  else
  {
    // Non-hidden pk
    size= sk_schema->KeyDefinition()->pack_record(
        table, pack_buffer, buf, sk_packed_tuple, &unpack_info, false,
        &unique_sk_packed_size, nullptr);
  }

  std::unique_ptr<EloqKey> packed_sk=
      std::make_unique<EloqKey>(sk_packed_tuple, size);

  std::unique_ptr<EloqRecord> packed_sk_rec= std::make_unique<EloqRecord>();
  packed_sk_rec->SetUnpackInfo(unpack_info.ptr(),
                               unpack_info.get_current_pos());

  dest_vec.emplace_back(txservice::TxKey(std::move(packed_sk)),
                        std::move(packed_sk_rec), version_ts);
  return 1;
}

void MysqlSkEncoder::Reset()
{
  if (mysql_table_.in_use != nullptr)
  {
    set_current_thd(mysql_table_.in_use);
  }
}

bool MysqlSkEncoder::DecodeRecord(uchar *table_record, const EloqKey *key,
                                  const EloqRecord *record) const
{
  mysql::TABLE *table= const_cast<mysql::TABLE *>(&mysql_table_);
  assert(table != nullptr && table->record[0] != nullptr);

  const mysql::TABLE_SHARE *mysql_table_share=
      mysql_table_schema_->TableShare();
  if (mysql_table_share->primary_key != MAX_INDEXES)
  {
    const EloqKeySchema *key_schema=
        static_cast<const EloqKeySchema *>(mysql_table_schema_->KeySchema());
    int err= key_schema->KeyDefinition()->unpack_record(
        table, table_record, key->PackedValueSlice(),
        record->UnpackInfoSlice(), false);
    if (err)
    {
      return false;
    }
  }

  MY_BITMAP *old_map;
  old_map= dbug_tmp_use_all_columns(table, &table->write_set);
  const EloqRecordSchema *record_schema= static_cast<const EloqRecordSchema *>(
      mysql_table_schema_->RecordSchema());
  record_schema->Decode(mysql_table_.read_set, table_record, table->field,
                        table->record[0], record->encoded_blob_, false, false);
  dbug_tmp_restore_column_map(&table->write_set, old_map);
  return true;
}

/**
 * Unlike TABLE::mark_index_columns_for_read(uint index), which is
 * available after handler::open() called. This method can be called directly.
 */
void MysqlSkEncoder::MarkIndexColumnsForRead()
{
  const EloqKeySchema *sk_schema= static_cast<const EloqKeySchema *>(
      secondary_key_schema_->sk_schema_.get());

  uint keyno= sk_schema->KeyDefinition()->get_keyno();

  KEY_PART_INFO *key_part= mysql_table_.key_info[keyno].key_part;
  uint key_parts= mysql_table_.key_info[keyno].user_defined_key_parts;
  for (uint k= 0; k < key_parts; k++)
  {
    key_part[k].field->register_field_in_read_map();
  }
}

const txservice::KeySchema *MysqlTableSchema::KeySchema() const
{
  return key_schema_.get();
}

const txservice::RecordSchema *MysqlTableSchema::RecordSchema() const
{
  return &record_schema_;
}

MysqlTableSchema::MysqlTableSchema(const txservice::TableName &table_name,
                                   const std::string &catalog_image,
                                   uint64_t version)
    : schema_image_(catalog_image), version_(version),
      base_table_name_(table_name.StringView().data(),
                       table_name.StringView().size(), table_name.Type(),
                       table_name.Engine())
{
  assert(table_name.Engine() == txservice::TableEngine::EloqSql);
  // Deserialize catalog_image into frm_str and kv_info_str
  std::string frm, kv_info, schemas_ts_str;
  EloqDS::DeserializeSchemaImage(catalog_image, frm, kv_info, schemas_ts_str);

  txservice::TableKeySchemaTs key_schemas_ts(txservice::TableEngine::EloqSql);
  size_t ts_offset= 0;
  key_schemas_ts.Deserialize(schemas_ts_str.data(), ts_offset);

  mysql::bzero((char *) &mysql_table_share_, sizeof(mysql_table_share_));

  mysql::init_sql_alloc(key_memory_table_share, &mysql_table_share_.mem_root,
                        TABLE_ALLOC_BLOCK_SIZE, 0, MYF(0));

  mysql_mutex_init(key_TABLE_SHARE_LOCK_share, &mysql_table_share_.LOCK_share,
                   MY_MUTEX_INIT_SLOW);
  mysql_mutex_init(key_TABLE_SHARE_LOCK_ha_data,
                   &mysql_table_share_.LOCK_ha_data, MY_MUTEX_INIT_FAST);

  const uchar *frm_ptr= (const uchar *) frm.data();
  mysql_table_share_.init_from_binary_frm_no_thd(frm_ptr, frm.length());

  // Get db name and table name.
  std::string_view table_name_v= base_table_name_.StringView();
  size_t start_pos= 0;
  auto first_pos= table_name_v.find('/', start_pos);
  assert(first_pos != std::string_view::npos);
  start_pos= first_pos + 1;
  auto second_pos= table_name_v.find('/', start_pos);
  assert(second_pos != std::string_view::npos);
  mysql_table_share_.db.str= table_name_v.data() + first_pos + 1;
  mysql_table_share_.db.length= second_pos - first_pos - 1;
  mysql_table_share_.table_name.str= table_name_v.data() + second_pos + 1;
  mysql_table_share_.table_name.length= table_name_v.length() - second_pos - 1;

  uint64_t key_schema_ts= key_schemas_ts.GetKeySchemaTs(table_name);
  key_schema_ts= key_schema_ts == 1 ? version_ : key_schema_ts;
  if (mysql_table_share_.primary_key != MAX_INDEXES)
  {
    key_schema_= std::make_unique<EloqKeySchema>(
        &mysql_table_share_, mysql_table_share_.primary_key, key_schema_ts);
  }
  else
  {
    key_schema_= std::make_unique<EloqHiddenKeySchema>(
        &mysql_table_share_, mysql_table_share_.stored_fields, key_schema_ts);
  }

  for (size_t kid= 0; kid < mysql_table_share_.keys; ++kid)
  {
    const KEY *key_info= &mysql_table_share_.key_info[kid];

    if (kid == mysql_table_share_.primary_key)
    {
      continue;
    }
    else
    {
      for (size_t key_part_idx= 0;
           key_part_idx < key_info->user_defined_key_parts; ++key_part_idx)
      {
        KEY_PART_INFO *key_part_info= key_info->key_part + key_part_idx;
        Field *key_field= key_part_info->field;
        // The following code is copied from
        // table.cc:copy_keys_from_share(TABLE *outparam, MEM_ROOT *root) to
        // allocate new fields for the index info, when the index fields only
        // cover prefixes of the original fields. New, allocated index fields
        // are necessary because we rely on the index's fields to compare two
        // index keys. And the table share's index fields reference the
        // original fields, whose field lengths do not reflect the index
        // fields' lengths. The allocated fields will be de-allocated when the
        // table share is freed.
        if (key_part_info->length != key_field->key_length() &&
            !(key_field->flags & BLOB_FLAG))
        {
          /*
          We are using only a prefix of the column as a key:
          Create a new field for the key part that matches the index.
        */
          TABLE tmp_t;
          tmp_t.maybe_null= 0;
          key_field->table= &tmp_t;

          key_part_info->field= key_field->make_new_field(
              &mysql_table_share_.mem_root, nullptr, 0);
          key_part_info->field->field_length= key_part_info->length;
        }
      }

      bool is_unique_sk= key_info->flags & HA_NOSAME;
      std::string index_name(table_name.String());
      index_name.append(is_unique_sk ? txservice::UNIQUE_INDEX_NAME_PREFIX
                                     : txservice::INDEX_NAME_PREFIX);
      index_name.append(key_info->name.str);

      txservice::TableName index_table_name(
          index_name.data(), index_name.size(),
          is_unique_sk ? txservice::TableType::UniqueSecondary
                       : txservice::TableType::Secondary,
          table_name.Engine());
      key_schema_ts= key_schemas_ts.GetKeySchemaTs(index_table_name);
      key_schema_ts= key_schema_ts == 1 ? version_ : key_schema_ts;
      std::unique_ptr<EloqKeySchema> sk_schema_=
          std::make_unique<EloqKeySchema>(&mysql_table_share_, kid,
                                          key_schema_ts);

      indexes_.try_emplace(kid, std::move(index_table_name),
                           txservice::SecondaryKeySchema{std::move(sk_schema_),
                                                         key_schema_.get()});
    }
  }

  size_t offset= 0;
  kv_info_=
      DataSubstrate::GetGlobal()->GetStoreHandler()->DeserializeKVCatalogInfo(
          kv_info, offset);
  record_schema_= EloqRecordSchema(&mysql_table_share_);
}

MysqlTableSchema::~MysqlTableSchema()
{
  mysql::free_table_share(&mysql_table_share_);
  // Sequences::DeleteSequence(base_table_name_);
  txservice::Sequences::DeleteSequence(
      base_table_name_, txservice::SequenceType::AutoIncrementColumn, true);
}

// For multikey index. Document engine only.
txservice::TableSchema::uptr MysqlTableSchema::Clone() const
{
  auto table_schema= std::make_unique<MysqlTableSchema>(
      base_table_name_, schema_image_, version_);
  table_schema->BindStatistics(table_statistics_);
  return table_schema;
}

const std::string &MysqlTableSchema::SchemaImage() const
{
  return schema_image_;
}

uint64_t MysqlTableSchema::Version() const { return version_; }

std::string_view MysqlTableSchema::VersionStringView() const
{
  return std::string_view(
      reinterpret_cast<const char *>(mysql_table_share_.tabledef_version.str),
      mysql_table_share_.tabledef_version.length);
}

std::vector<txservice::TableName> MysqlTableSchema::IndexNames() const
{
  std::vector<txservice::TableName> index_names;
  index_names.reserve(indexes_.size());
  for (const auto &index_entry : indexes_)
  {
    index_names.emplace_back(index_entry.second.first.StringView(),
                             index_entry.second.first.Type(),
                             index_entry.second.first.Engine());
  }

  return index_names;
}

size_t MysqlTableSchema::IndexesSize() const { return indexes_.size(); }

const txservice::SecondaryKeySchema *
MysqlTableSchema::IndexKeySchema(const txservice::TableName &index_name) const
{
  for (const auto &index_entry : indexes_)
  {
    if (index_entry.second.first == index_name)
    {
      return &index_entry.second.second;
    }
  }

  return nullptr;
}

uint16_t
MysqlTableSchema::IndexOffset(const txservice::TableName &index_name) const
{
  for (const auto &[offset, index] : indexes_)
  {
    if (index.first == index_name)
    {
      return offset;
    }
  }
  return UINT16_MAX;
}

txservice::SkEncoder::uptr
MysqlTableSchema::CreateSkEncoder(const txservice::TableName &index_name) const
{
  return std::make_unique<MysqlSkEncoder>(index_name, this);
}

bool MysqlTableSchema::HasAutoIncrement() const
{
  return !(record_schema_.AutoIncrementIndex() == -1);
}

const txservice::TableName *MysqlTableSchema::GetSequenceTableName() const
{
  return &txservice::Sequences::table_name_;
}

std::pair<txservice::TxKey, txservice::TxRecord::Uptr>
MysqlTableSchema::GetSequenceKeyAndInitRecord(
    const txservice::TableName &table_name) const
{
  return txservice::Sequences::GetSequenceKeyAndInitRecord(
      table_name, txservice::SequenceType::AutoIncrementColumn);
}

const mysql::TABLE_SHARE *MysqlTableSchema::TableShare() const
{
  return &mysql_table_share_;
}

const std::pair<txservice::TableName, txservice::SecondaryKeySchema> &
MysqlTableSchema::IndexNameSchema(uint index_id) const
{
  auto index_it= indexes_.find(index_id);
  assert(index_it != indexes_.end());

  return index_it->second;
}

uint MysqlTableSchema::IndexId(const txservice::TableName &index_name) const
{
  for (const auto &index_entry : indexes_)
  {
    if (index_entry.second.first == index_name)
    {
      return index_entry.first;
    }
  }

  return UINT_MAX;
}

txservice::TableSchema::uptr
MariaCatalogFactory::CreateTableSchema(const txservice::TableName &table_name,
                                       const std::string &catalog_image,
                                       uint64_t version)
{
  assert(table_name.Engine() == txservice::TableEngine::EloqSql);

  return std::make_unique<MysqlTableSchema>(table_name, catalog_image,
                                            version);
}

txservice::CcMap::uptr MariaCatalogFactory::CreatePkCcMap(
    const txservice::TableName &table_name,
    const txservice::TableSchema *table_schema, bool ccm_has_full_entries,
    txservice::CcShard *shard, txservice::NodeGroupId cc_ng_id)
{
  assert(table_name.Engine() == txservice::TableEngine::EloqSql);

  uint64_t key_version= table_schema->KeySchema()->SchemaTs();
  return std::make_unique<
      txservice::TemplateCcMap<EloqKey, EloqRecord, true, true>>(
      shard, cc_ng_id, table_name, key_version, table_schema,
      ccm_has_full_entries);
}

txservice::CcMap::uptr
MariaCatalogFactory::CreateSkCcMap(const txservice::TableName &index_name,
                                   const txservice::TableSchema *table_schema,
                                   txservice::CcShard *shard,
                                   txservice::NodeGroupId cc_ng_id)
{
  const MysqlTableSchema *mysql_table_schema=
      static_cast<const MysqlTableSchema *>(table_schema);

  if (mysql_table_schema->IndexKeySchema(index_name) != nullptr)
  {
    uint64_t key_version= table_schema->IndexKeySchema(index_name)->SchemaTs();
    return std::make_unique<
        txservice::TemplateCcMap<EloqKey, EloqRecord, true, true>>(
        shard, cc_ng_id, index_name, key_version, table_schema, false);
  }

  return nullptr;
}

txservice::CcMap::uptr MariaCatalogFactory::CreateRangeMap(
    const txservice::TableName &range_table_name,
    const txservice::TableSchema *table_schema, uint64_t schema_ts,
    txservice::CcShard *shard, const txservice::NodeGroupId ng_id)
{
  assert(range_table_name.Type() == txservice::TableType::RangePartition);
  return std::make_unique<txservice::RangeCcMap<EloqKey>>(
      range_table_name, table_schema, schema_ts, shard, ng_id);
}

std::unique_ptr<txservice::TableRangeEntry>
MariaCatalogFactory::CreateTableRange(
    txservice::TxKey start_key, uint64_t version_ts, int64_t partition_id,
    std::unique_ptr<txservice::StoreRange> slices)
{
  assert(start_key.Type() == txservice::KeyType::NegativeInf ||
         start_key.IsOwner());
  // The range's start key must not be null. If the start points to negative
  // infinity, it points to EloqKey::NegativeInfinity().
  const EloqKey *start= start_key.GetKey<EloqKey>();
  assert(start != nullptr);

  txservice::TemplateStoreRange<EloqKey> *range_ptr=
      static_cast<txservice::TemplateStoreRange<EloqKey> *>(slices.release());

  std::unique_ptr<txservice::TemplateStoreRange<EloqKey>> typed_range{
      range_ptr};

  return std::make_unique<txservice::TemplateTableRangeEntry<EloqKey>>(
      start, version_ts, partition_id, std::move(typed_range));
}

std::unique_ptr<txservice::CcScanner>
MariaCatalogFactory::CreatePkCcmScanner(txservice::ScanDirection direction,
                                        const txservice::KeySchema *key_schema)
{
  if (direction == txservice::ScanDirection::Forward)
  {
    return std::make_unique<
        txservice::RangePartitionedCcmScanner<EloqKey, EloqRecord, true>>(
        direction, txservice::ScanIndexType::Primary, key_schema);
  }
  else
  {
    return std::make_unique<
        txservice::RangePartitionedCcmScanner<EloqKey, EloqRecord, false>>(
        direction, txservice::ScanIndexType::Primary, key_schema);
  }
}

std::unique_ptr<txservice::CcScanner> MariaCatalogFactory::CreateSkCcmScanner(
    txservice::ScanDirection direction,
    const txservice::KeySchema *compound_key_schema)
{
  if (direction == txservice::ScanDirection::Forward)
  {
    return std::make_unique<
        txservice::RangePartitionedCcmScanner<EloqKey, EloqRecord, true>>(
        direction, txservice::ScanIndexType::Secondary, compound_key_schema);
  }
  else
  {
    return std::make_unique<
        txservice::RangePartitionedCcmScanner<EloqKey, EloqRecord, false>>(
        direction, txservice::ScanIndexType::Secondary, compound_key_schema);
  }
}

std::unique_ptr<txservice::CcScanner>
MariaCatalogFactory::CreateRangeCcmScanner(
    txservice::ScanDirection direction, const txservice::KeySchema *key_schema,
    const txservice::TableName &range_table_name)
{
  assert(range_table_name.Type() == txservice::TableType::RangePartition);
  return std::make_unique<
      txservice::HashParitionCcScanner<EloqKey, txservice::RangeRecord>>(
      direction,
      range_table_name.IsBase() ? txservice::ScanIndexType::Primary
                                : txservice::ScanIndexType::Secondary,
      key_schema);
}

std::unique_ptr<txservice::Statistics>
MariaCatalogFactory::CreateTableStatistics(
    const txservice::TableSchema *table_schema,
    txservice::NodeGroupId cc_ng_id)
{
  return std::make_unique<txservice::TableStatistics<EloqKey>>(table_schema,
                                                               cc_ng_id);
}

std::unique_ptr<txservice::Statistics>
MariaCatalogFactory::CreateTableStatistics(
    const txservice::TableSchema *table_schema,
    std::unordered_map<txservice::TableName,
                       std::pair<uint64_t, std::vector<txservice::TxKey>>>
        sample_pool_map,
    txservice::CcShard *ccs, txservice::NodeGroupId cc_ng_id)
{
  return std::make_unique<txservice::TableStatistics<EloqKey>>(
      table_schema, std::move(sample_pool_map), ccs, cc_ng_id);
}

txservice::TxKey MariaCatalogFactory::NegativeInfKey() const
{
  return txservice::TxKey(EloqKey::NegativeInfinity());
}

txservice::TxKey MariaCatalogFactory::PositiveInfKey() const
{
  return txservice::TxKey(EloqKey::PositiveInfinity());
}

txservice::TxKey MariaCatalogFactory::CreateTxKey() const
{
  return txservice::TxKey(std::make_unique<EloqKey>());
}
txservice::TxKey MariaCatalogFactory::CreateTxKey(const char *data,
                                                  size_t size) const
{
  return txservice::TxKey(
      std::make_unique<EloqKey>(reinterpret_cast<const uchar *>(data), size));
}
const txservice::TxKey *MariaCatalogFactory::PackedNegativeInfinity() const
{
  return EloqKey::PackedNegativeInfinityTxKey();
}
std::unique_ptr<txservice::TxRecord>
MariaCatalogFactory::CreateTxRecord() const
{
  return std::make_unique<EloqRecord>();
}

} // namespace MyEloq
