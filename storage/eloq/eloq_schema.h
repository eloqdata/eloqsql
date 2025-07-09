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

#include <cstdint>
#include <memory>
#include <vector>
#include <string>
#include <algorithm>

#if defined(DATA_STORE_TYPE_DYNAMODB)
#include <aws/dynamodb/model/PutRequest.h>
#include <aws/dynamodb/model/AttributeValue.h>
#elif defined(DATA_STORE_TYPE_CASSANDRA)
#include "cass/include/cassandra.h"
#elif defined(DATA_STORE_TYPE_BIGTABLE)
#include <google/cloud/bigtable/table.h>
#endif

#include "my_global.h"
#include "table.h"
#include "field.h"
#include "tx_service/include/schema.h"
#include "tx_service/include/catalog_factory.h"
#include "ha_eloq_macro.h"
#include "eloq_key_def.h"
#include "tx_record.h"

namespace MyEloq
{
class mono_key_def;
enum class EloqDataType
{
  Integer,
  UnsignedInteger,
  Decimal,
  Float,
  Datetime,
  // Timestamp represents the number of microseconds since the epoch.
  Timestamp,
  // Date represents the year/month/day triple.
  Date,
  // Time represents the hour::minute::sec triple.
  Time,
  // Fixed-length string
  String,
  Binary,
  // Both VarString and VarBinary are encoded to and decoded from a sequence of
  // char. The sequence starts with a variable-length integer (1-4 bytes)
  // indicating the length of the content, and then follows the blob of the
  // field's content. VarString and VarBinary differs in that whether the blob
  // is intrepreted as std::string or as it is.
  VarString,
  VarBinary
};

enum SortOrder
{
  ASC,
  DESC
};

struct EloqFieldType
{
public:
  static const uint32_t CassDateEpochCenter= 2147483648U;

  EloqFieldType()= delete;

  EloqFieldType(const char *name, size_t name_len, EloqDataType type,
                uint16_t len, bool auto_inc= false)
      : data_type_(type), len_(len), auto_increment_(auto_inc)
  {
    std::string_view name_view(name, name_len);
    for (size_t start_pos= 0; start_pos < name_len;)
    {
      auto double_quote_pos= name_view.find('"', start_pos);
      if (double_quote_pos == std::string_view::npos)
      {
        field_name_.append(name_view.begin() + start_pos, name_view.end());
        break;
      }
      else
      {
        field_name_.append(name_view.begin() + start_pos,
                           name_view.begin() + double_quote_pos);
        field_name_.append("\"\"");
        start_pos= double_quote_pos + 1;
      }
    }
  }

  EloqFieldType(EloqFieldType &&rhs)
      : field_name_(std::move(rhs.field_name_)), data_type_(rhs.data_type_),
        len_(rhs.len_), auto_increment_(rhs.auto_increment_)
  {
  }

  EloqFieldType(const EloqFieldType &rhs)
      : field_name_(rhs.field_name_), data_type_(rhs.data_type_),
        len_(rhs.len_), auto_increment_(rhs.auto_increment_)
  {
  }

  bool operator==(const EloqFieldType &rhs) const
  {
    return field_name_ == rhs.field_name_;
  }

  static EloqFieldType Convert(const mysql::Field *field,
                               uint16_t key_length= 0);
#if defined(DATA_STORE_TYPE_CASSANDRA)
  static std::string GetCassTypeName(const EloqFieldType &field_type);
#endif
  std::string field_name_;
  EloqDataType data_type_;
  // For fixed-length data types, len_ represents the number of bytes of the
  // data. For variable-length types, len_ represents the number of bytes that
  // encode the length of the data.
  uint16_t len_;
  // If this field is auto incrment type. Only valid for integer type.
  bool auto_increment_;
};

enum collations_used
{
  COLLATION_UTF8MB4_BIN= 46,
  COLLATION_LATIN1_BIN= 47,
  COLLATION_BINARY= 63,
  COLLATION_UTF8_BIN= 83
};

enum index_type
{
  INDEX_TYPE_PRIMARY= 1,
  INDEX_TYPE_SECONDARY= 2,
  INDEX_TYPE_HIDDEN_PRIMARY= 3
};

struct EloqKeySchema : public txservice::KeySchema
{
public:
  using Uptr= std::unique_ptr<EloqKeySchema>;

  EloqKeySchema()= delete;

  EloqKeySchema(const TABLE_SHARE *table_share, uint key_no,
                uint64_t key_version);

  EloqKeySchema(EloqKeySchema &&rhs)
      : key_info_(rhs.key_info_),
        key_part_types_(std::move(rhs.key_part_types_)),
        key_def_(rhs.key_def_), schema_ts_(rhs.schema_ts_)
  {
  }

  EloqKeySchema(const EloqKeySchema &rhs)
      : key_info_(rhs.key_info_), key_part_types_(rhs.key_part_types_),
        key_def_(rhs.key_def_), schema_ts_(rhs.schema_ts_)
  {
  }

  uchar GetIndexType() const;

  bool IsUniqueIndex() const;

  int CreateKeyDefinition(const TABLE_SHARE *const table, const uint i);

  const std::shared_ptr<mono_key_def> KeyDefinition() const
  {
    return key_def_;
  }
#if defined(DATA_STORE_TYPE_CASSANDRA)
  virtual void EncodeFromBaseTable(const CassRow *row,
                                   std::vector<char> &buf) const;
  virtual void EncodeFromIndexTable(const CassRow *row, std::vector<char> &buf,
                                    size_t offset) const;
#endif
  /**
   * @brief Encode key. Only use for tests.
   *
   * @param key_cols Only the columns that make up the key.
   * @param buf
   */
  virtual void EncodeKey(const std::vector<std::string> &key_cols,
                         std::vector<char> &buf) const;

  void SearchCondition(std::string &cql) const;
  void ScanForwardCondition(std::string &cql, bool inclusive, bool scan_foward,
                            uint8_t key_parts) const;

  void ColumnList(std::string &cql, bool trim,
                  const std::vector<size_t> *pk_only_col= nullptr) const;
#if defined(DATA_STORE_TYPE_CASSANDRA)
  void
  ColumnListWithType(std::string &cql, bool trim,
                     const std::vector<size_t> *pk_only_col= nullptr) const;
#endif

  std::vector<std::pair<std::string, int8_t>>
  ScanStartFromConditions(std::string &orig_cql, bool inclusive= true,
                          bool more_or_less_than= true,
                          uint8_t key_parts= UINT8_MAX) const;

  void OrderByList(std::string &cql, SortOrder order= SortOrder::ASC) const;

  void ScanSliceCondition(std::string &cql, uint8_t key_parts) const;

  size_t ColumnCount() const { return key_part_types_.size(); }

  /**
   * @brief Compare two keys
   * @return
   *   true - Ok. column_index is the index of the first column which is
   *            different. -1 if two keys are equal.
   *   false - Data format error.
   */
  bool CompareKeys(const txservice::TxKey &key1, const txservice::TxKey &key2,
                   size_t *const column_index) const override;

  uint16_t ExtendKeyParts() const override;

  uint64_t SchemaTs() const override { return schema_ts_; }

  const EloqFieldType &FieldType(size_t field_idx) const
  {
    return key_part_types_.at(field_idx);
  }

protected:
  /**
   * @brief The pointer to the MySQL key definition. When the MySQL key info is
   * null and the key refers to the primary key, the key represents the hidden
   * primary key, which is of type uuid.
   *
   */
  const mysql::KEY *key_info_;
  std::vector<EloqFieldType> key_part_types_;

  /**
   * @brief  We store a key_def struct copied from rocksdb. It is used to
   * pack/unpack key value into a memory-comparable form which is the format we
   * store in kv storage.
   */
  std::shared_ptr<mono_key_def> key_def_;
  // The timestamp when this key was created.
  uint64_t schema_ts_{1};
};

struct EloqHiddenKeySchema : public EloqKeySchema
{
public:
  EloqHiddenKeySchema(const TABLE_SHARE *table, int16_t non_pk_fields,
                      uint64_t key_version)
      : EloqKeySchema(table, table->keys, key_version),
        non_pk_field_cnt_(non_pk_fields)
  {
    static const std::string hidden_key("uuid_key");
    key_part_types_.emplace_back(hidden_key.data(), hidden_key.length(),
                                 EloqDataType::Binary, 16);
  }

  EloqHiddenKeySchema(EloqHiddenKeySchema &&rhs)
      : EloqKeySchema(rhs), non_pk_field_cnt_(rhs.non_pk_field_cnt_)
  {
  }

  EloqHiddenKeySchema(const EloqHiddenKeySchema &rhs)
      : EloqKeySchema(rhs), non_pk_field_cnt_(rhs.non_pk_field_cnt_)
  {
  }

#if defined(DATA_STORE_TYPE_CASSANDRA)
  void EncodeFromBaseTable(const CassRow *row,
                           std::vector<char> &key_buf) const override;
  void EncodeFromIndexTable(const CassRow *row, std::vector<char> &key_buf,
                            size_t offset) const override;
#endif

private:
  /**
   * @brief Number of columns of a table without the primary key.
   *
   */
  uint16_t non_pk_field_cnt_;
};

class EloqRecordSchema final : public txservice::RecordSchema
{
public:
  using Uptr= std::unique_ptr<EloqRecordSchema>;

  EloqRecordSchema() {}
  EloqRecordSchema(const mysql::TABLE_SHARE *table_share);

  EloqRecordSchema(EloqRecordSchema &&rhs)
      : field_types_(std::move(rhs.field_types_)),
        is_part_of_pk_(std::move(rhs.is_part_of_pk_)),
        pk_field_idx_(std::move(rhs.pk_field_idx_)),
        non_pk_column_count_(rhs.non_pk_column_count_),
        auto_increment_idx_(rhs.auto_increment_idx_)
  {
  }

  EloqRecordSchema(const EloqRecordSchema &rhs)
      : field_types_(rhs.field_types_), is_part_of_pk_(rhs.is_part_of_pk_),
        pk_field_idx_(rhs.pk_field_idx_),
        non_pk_column_count_(rhs.non_pk_column_count_),
        auto_increment_idx_(rhs.auto_increment_idx_)
  {
  }

  EloqRecordSchema &operator=(EloqRecordSchema &&rhs)
  {
    if (this != &rhs)
    {
      field_types_= std::move(rhs.field_types_);
      is_part_of_pk_= std::move(rhs.is_part_of_pk_);
      pk_field_idx_= std::move(rhs.pk_field_idx_);
      non_pk_column_count_= rhs.non_pk_column_count_;
      auto_increment_idx_= rhs.auto_increment_idx_;
    }

    return *this;
  }

  int AutoIncrementIndex() const override { return auto_increment_idx_; }
  void Set(const mysql::TABLE_SHARE *table_share);

  const EloqFieldType &FieldType(uint16_t field_idx)
  {
    return field_types_[field_idx];
  }

  void Encode(const uchar *table_record, mysql::Field **field_head,
              const uchar *table_record_0, std::vector<char> &buf) const;
  void Decode(const MY_BITMAP *decode_set, uchar *table_record,
              mysql::Field **field_head, uchar *table_record_0,
              const std::vector<char> &buf, bool is_ckpt_delta= false,
              bool is_deleted= false) const;
#if defined(DATA_STORE_TYPE_CASSANDRA)
  void EncodeToSerializeFormat(txservice::TableType table_type,
                               const void *row,
                               std::string &buf) const override;
  void EncodeToTxRecord(const txservice::TableName &table_name,
                        const void *row,
                        txservice::TxRecord &tx_record) const override;
  static void BindCassStatement(const std::vector<char> &rec_buf,
                                const EloqRecordSchema *rec_schema,
                                CassStatement *statem);
#endif

  void ColumnList(std::string &col_list) const;

  void NonPkColumnList(std::string &col_list) const;
#if defined(DATA_STORE_TYPE_CASSANDRA)
  void ColumnListWithType(std::string &col_list) const;

  void NonPkColumnListWithType(std::string &col_list) const;
#endif

  uint16_t ColumnCount() const;

  uint16_t NonPkColumnCount() const;

  static bool IsHiddenPk(const uint index, const TABLE_SHARE *const table);

  static void EncodeInteger(int8_t val, std::vector<char> &buf);
  static void DecodeInteger(int8_t &val, const std::vector<char> &buf,
                            size_t &offset);

  static void EncodeInteger(int16_t val, std::vector<char> &buf);
  static void DecodeInteger(int16_t &val, const std::vector<char> &buf,
                            size_t &offset);

  static void EncodeInteger(int32_t val, std::vector<char> &buf);
  static void DecodeInteger(int32_t &val, const std::vector<char> &buf,
                            size_t &offset);

  static void EncodeInteger(int64_t val, std::vector<char> &buf);
  static void DecodeInteger(int64_t &val, const std::vector<char> &buf,
                            size_t &offset);

  static void EncodeInteger(int64_t val, std::vector<char> &buf, uint8_t len);
  static void DecodeInteger(int64_t &val, const std::vector<char> &buf,
                            size_t &offset, uint8_t len);

  static void EncodeUnsignedInteger(uint64_t val, std::vector<char> &buf,
                                    uint8_t len);
  static void DecodeUnsignedInteger(uint64_t &val,
                                    const std::vector<char> &buf,
                                    size_t &offset, uint8_t len);

  static void EncodeString(mysql::Field *field, std::vector<char> &buf,
                           uint16_t len, bool fixed_len);

  static void EncodeDatetime(int64_t elapse_micro_sec, uint8_t precision,
                             std::vector<char> &buf);
  static void DecodeDatetime(int64_t &elapse_micro_sec, uint8_t precision,
                             const std::vector<char> &buf, size_t &offset);

  static void EncodeTime(int64_t elapse_micro_sec, uint8_t precision,
                         std::vector<char> &buf);

  static void DecodeTime(int64_t &elapse_micro_sec, uint8_t precision,
                         const std::vector<char> &buf, size_t &offset);

  static void EncodeDouble(double val, std::vector<char> &buf);
  static void DecodeDouble(double &val, const std::vector<char> &buf,
                           size_t &offset);

  static void EncodeFloat(float val, std::vector<char> &buf);
  static void DecodeFloat(float &val, const std::vector<char> &buf,
                          size_t &offset);

#if defined(DATA_STORE_TYPE_CASSANDRA)
  static void EncodeCassValue(const CassValue *cass_val,
                              std::vector<char> &buf,
                              const EloqFieldType &field_type,
                              bool is_key_field);
#endif

  static void EncodePlainValue(const std::string &plain_val,
                               std::vector<char> &buf,
                               const EloqFieldType &field_type,
                               bool is_key_field);

#if defined(DATA_STORE_TYPE_CASSANDRA)
  static void BindCassStatement(const std::vector<char> &buf, size_t &offset,
                                const EloqFieldType &field_type,
                                bool is_key_field, CassStatement *statem,
                                size_t parameter_index);

#elif defined(DATA_STORE_TYPE_DYNAMODB)
  void BindDynamoRequest(const std::vector<char> &rec_buf,
                         Aws::DynamoDB::Model::PutRequest *req) const;

  void Encode(
      const Aws::Map<Aws::String, Aws::DynamoDB::Model::AttributeValue> &row,
      std::vector<char> &buf) const;

  static void BindDynamoAttribute(const std::vector<char> &rec_buf,
                                  size_t &offset,
                                  const EloqFieldType &field_type,
                                  Aws::DynamoDB::Model::AttributeValue &att);
  static void
  EncodeDynamoValue(const Aws::DynamoDB::Model::AttributeValue &att,
                    std::vector<char> &buf, const EloqFieldType &field_type,
                    bool is_key_field);
#elif defined(DATA_STORE_TYPE_BIGTABLE)
  void Encode(const std::string &payload, std::vector<char> &buf) const;

  static void BindBigTablePayload(const std::vector<char> &rec_buf,
                                  std::string &payload);
#endif
  static void UpdateOffset(const std::vector<char> &rec_buf, size_t &offset,
                           const EloqFieldType &field_type, bool is_key_field);

  const EloqFieldType &FieldType(size_t field_idx) const
  {
    return field_types_.at(field_idx);
  }

  bool IsPartOfPk(size_t fidx) const { return is_part_of_pk_[fidx]; }

private:
  // Number of bytes of the bitmap that indicates whether a field is null
  // or not. Null fields are not materialized.
  uint16_t BitmapBytes(bool is_ckpt_delta= false) const
  {
    uint16_t field_cnt_= NonPkColumnCount();

    if (is_ckpt_delta)
    {
      --field_cnt_;
    }

    return (field_cnt_ >> 3) + ((field_cnt_ & 7) == 0 ? 0 : 1);
  }

  std::vector<EloqFieldType> field_types_;
  std::vector<bool> is_part_of_pk_;

  /**
   * @brief Indexes of the primary-key fields. MySQL's primary key contains at
   * most 64 fields. The vector is empty if the table has no primary key and
   * uses the hidden primary key (a hidden column of uuid).
   *
   */
  std::vector<uint16_t> pk_field_idx_;

  /**
   * Accelerate calculation of NonPkColumnCount().
   */
  uint16_t non_pk_column_count_{0};

  // The index for auto increment field. -1 if not exist,
  int auto_increment_idx_{-1};
};

/* Provided in tx_service/include/schema.h
struct TableKeySchemaTs
{
  TableKeySchemaTs()= default;
  TableKeySchemaTs(const std::string &key_schemas_ts_str);

  std::string Serialize() const;
  void Deserialize(const char *buf, size_t &offset);
  uint64_t GetKeySchemaTs(const txservice::TableName &table_name) const;

  uint64_t pk_schema_ts_{1};
  std::unordered_map<txservice::TableName, uint64_t> sk_schemas_ts_;
};
*/
} // namespace MyEloq
