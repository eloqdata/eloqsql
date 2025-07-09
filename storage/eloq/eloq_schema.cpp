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
#include <iostream>
#include <utility>
#include <algorithm>
#include <vector>
#include <regex>

#include "eloq_key_def.h"
#include "eloq_schema.h"
#include "date.h"
#include "eloq_key.h"
#include "tx_record.h"
#if defined(DATA_STORE_TYPE_CASSANDRA)
#include "store_handler/cass_big_number.h"
#include "cass/include/cassandra.h"
#endif
#include "butil/logging.h"

MyEloq::EloqFieldType MyEloq::EloqFieldType::Convert(const mysql::Field *field,
                                                     uint16_t key_length)
{
  uint16_t len= 0;
  EloqDataType eloq_type;

  switch (field->real_type())
  {
  // Integer types
  case MYSQL_TYPE_TINY: {
    len= 1;
    const Field_num *num_field= static_cast<const Field_num *>(field);
    eloq_type= num_field->unsigned_flag ? EloqDataType::UnsignedInteger
                                        : EloqDataType::Integer;
    break;
  }
  case MYSQL_TYPE_SHORT: {
    len= 2;
    const Field_num *num_field= static_cast<const Field_num *>(field);
    eloq_type= num_field->unsigned_flag ? EloqDataType::UnsignedInteger
                                        : EloqDataType::Integer;
    break;
  }
  case MYSQL_TYPE_INT24: {
    len= 3;
    const Field_num *num_field= static_cast<const Field_num *>(field);
    eloq_type= num_field->unsigned_flag ? EloqDataType::UnsignedInteger
                                        : EloqDataType::Integer;
    break;
  }
  case MYSQL_TYPE_LONG: {
    len= 4;
    const Field_num *num_field= static_cast<const Field_num *>(field);
    eloq_type= num_field->unsigned_flag ? EloqDataType::UnsignedInteger
                                        : EloqDataType::Integer;
    break;
  }
  case MYSQL_TYPE_LONGLONG: {
    len= 8;
    const Field_num *num_field= static_cast<const Field_num *>(field);
    eloq_type= num_field->unsigned_flag ? EloqDataType::UnsignedInteger
                                        : EloqDataType::Integer;
    break;
  }
  case MYSQL_TYPE_FLOAT: {
    len= 4;
    eloq_type= EloqDataType::Float;
    break;
  }
  case MYSQL_TYPE_DOUBLE: {
    len= 8;
    eloq_type= EloqDataType::Float;
    break;
  }
  case MYSQL_TYPE_DATE:
  case MYSQL_TYPE_NEWDATE: {
    len= 3;
    eloq_type= EloqDataType::Date;
    break;
  }
  case MYSQL_TYPE_TIME:
  case MYSQL_TYPE_TIME2: {
    len= field->decimals();
    eloq_type= EloqDataType::Time;
    break;
  }
  case MYSQL_TYPE_DATETIME:
  case MYSQL_TYPE_DATETIME2: {
    len= field->decimals();
    eloq_type= EloqDataType::Datetime;
    break;
  }
  case MYSQL_TYPE_TIMESTAMP:
  case MYSQL_TYPE_TIMESTAMP2: {
    len= field->decimals();
    eloq_type= EloqDataType::Timestamp;
    break;
  }
  case MYSQL_TYPE_YEAR: {
    len= 1;
    eloq_type= EloqDataType::Integer;
    break;
  }
  case MYSQL_TYPE_VARCHAR:
  case MYSQL_TYPE_VAR_STRING: {
    Field_str *str_field= (Field_str *) field;
    if (key_length > 0)
    {
      len= key_length;
    }
    else
    {
      len= str_field->length_size();
    }
    CHARSET_INFO *charset= str_field->charset();
    // This is varbinary, refer sql/share/charsets/Index.xml
    if (charset->number == 63)
    {
      eloq_type= EloqDataType::VarBinary;
    }
    else
    {
      eloq_type= EloqDataType::VarString;
    }
    break;
  }
  case MYSQL_TYPE_STRING: {
    CHARSET_INFO *charset= field->charset();
    eloq_type=
        charset->number == 63 ? EloqDataType::Binary : EloqDataType::String;
    len= field->field_length;
    break;
  }
  // Decimal types
  case MYSQL_TYPE_DECIMAL:
  case MYSQL_TYPE_NEWDECIMAL: {
    const Field_new_decimal *decimal_field=
        static_cast<const Field_new_decimal *>(field);
    // For the decimal type, the higher 8 bits of len represents the precision
    // (total number of digits). The lower 8 bits represents the scale (number
    // of digits to the right of the dot).
    len= decimal_field->precision << 8 | decimal_field->decimals();
    eloq_type= EloqDataType::Decimal;
    break;
  }
  case MYSQL_TYPE_BIT: {
    // A bit field has at most 64 bits and therefore is represented as a
    // variable-length integer.
    const Field_bit *bit_field= static_cast<const Field_bit *>(field);
    len= (uint8_t) bit_field->bytes_in_rec;
    eloq_type= EloqDataType::Integer;
    break;
  }
  /* Compressed types are only used internally for RBR. */
  case MYSQL_TYPE_BLOB_COMPRESSED:
  case MYSQL_TYPE_VARCHAR_COMPRESSED:
  case MYSQL_TYPE_TINY_BLOB: {
    len= 1;
    CHARSET_INFO *charset= field->charset();
    eloq_type= charset->number == 63 ? EloqDataType::VarBinary
                                     : EloqDataType::VarString;
    break;
  }
  case MYSQL_TYPE_MEDIUM_BLOB: {
    len= 3;
    CHARSET_INFO *charset= field->charset();
    eloq_type= charset->number == 63 ? EloqDataType::VarBinary
                                     : EloqDataType::VarString;
    break;
  }
  case MYSQL_TYPE_LONG_BLOB: {
    len= 4;
    CHARSET_INFO *charset= field->charset();
    eloq_type= charset->number == 63 ? EloqDataType::VarBinary
                                     : EloqDataType::VarString;
    break;
  }
  case MYSQL_TYPE_BLOB: {
    const Field_blob *blob_field= static_cast<const Field_blob *>(field);
    len= blob_field->pack_length_no_ptr();
    CHARSET_INFO *charset= field->charset();
    eloq_type= charset->number == 63 ? EloqDataType::VarBinary
                                     : EloqDataType::VarString;
    break;
  }
  case MYSQL_TYPE_SET: {
    const Field_set *set_field= static_cast<const Field_set *>(field);
    uint8_t num_of_bytes= set_field->pack_length();
    if (num_of_bytes <= 2)
    {
      len= num_of_bytes;
    }
    else if (num_of_bytes <= 4)
    {
      len= 4;
    }
    else
    {
      len= 8;
    }
    eloq_type= EloqDataType::Integer;
    break;
  }
  case MYSQL_TYPE_ENUM: {
    const Field_enum *enum_field= static_cast<const Field_enum *>(field);
    uint8_t num_of_bytes= enum_field->pack_length();
    len= num_of_bytes;
    eloq_type= EloqDataType::UnsignedInteger;
    break;
  }
  case MYSQL_TYPE_NULL:
  case MYSQL_TYPE_GEOMETRY:
  default: {
    len= 4;
    eloq_type= EloqDataType::VarBinary;
    break;
  }
  }
  return EloqFieldType(field->field_name.str, field->field_name.length,
                       eloq_type, len,
                       field->unireg_check == Field::NEXT_NUMBER);
}

#if defined(DATA_STORE_TYPE_CASSANDRA)
/*
 * convert eloq data type to Cassandra type
 * Cassandra type reference:
 * https://docs.datastax.com/en/cql-oss/3.x/cql/cql_reference/cql_data_types_c.html
 */
std::string
MyEloq::EloqFieldType::GetCassTypeName(const EloqFieldType &field_type)
{
  std::string cass_type_name= "";

  switch (field_type.data_type_)
  {
  case EloqDataType::Integer:
    if (field_type.len_ == 1)
    {
      cass_type_name= "tinyint";
    }
    else if (field_type.len_ == 2)
    {
      cass_type_name= "smallint";
    }
    else if (field_type.len_ == 3 || field_type.len_ == 4)
    {
      cass_type_name= "int";
    }
    else if (field_type.len_ > 4 && field_type.len_ <= 8)
    {
      cass_type_name= "bigint";
    }
    else
    {
      DLOG(ERROR) << "Invalid length of type integer, length: "
                  << field_type.len_;
      cass_type_name= "int";
    }
    break;
  case EloqDataType::UnsignedInteger:
    if (field_type.len_ == 1)
    {
      cass_type_name= "smallint";
    }
    else if (field_type.len_ == 2 || field_type.len_ == 3)
    {
      cass_type_name= "int";
    }
    else if (field_type.len_ == 4)
    {
      cass_type_name= "bigint";
    }
    else if (field_type.len_ == 8)
    {
      cass_type_name= "decimal";
    }
    else
    {
      DLOG(ERROR) << "Invalid length of type integer, length: "
                  << field_type.len_;
      cass_type_name= "bigint";
    }
    break;
  case EloqDataType::Decimal:
    cass_type_name= "decimal";
    break;
  case EloqDataType::Float:
    if (field_type.len_ == 4)
    {
      cass_type_name= "float";
    }
    else
    {
      cass_type_name= "double";
    }
    break;
  case EloqDataType::Time:
    cass_type_name= "time";
    break;
  case EloqDataType::Date:
    cass_type_name= "date";
    break;
  case EloqDataType::VarString:
  case EloqDataType::String:
    cass_type_name= "text";
    break;
  case EloqDataType::VarBinary:
  case EloqDataType::Binary:
    cass_type_name= "blob";
    break;
  case EloqDataType::Datetime:
  case EloqDataType::Timestamp:
    cass_type_name= "bigint";
    break;
  // TODO: more specific types
  default:
    break;
  }
  return cass_type_name;
}
#endif

MyEloq::EloqRecordSchema::EloqRecordSchema(
    const mysql::TABLE_SHARE *table_share)
{
  uint8_t pk_index= table_share->primary_key;

  Field **field_head= table_share->field;
  for (uint16_t field_idx= 0; *field_head != nullptr; ++field_head)
  {
    Field *field= *field_head;
    if (!field->stored_in_db())
    {
      // For computed(virtual) field, if VIRTUAL, do not put them in
      // field_types_.
      continue;
    }

    field_types_.emplace_back(EloqFieldType::Convert(field));
    if (field->unireg_check == Field::NEXT_NUMBER)
    {
      auto_increment_idx_= field_idx;
    }

    // The primary key's index equals to MAX_INDEXES, if the table has no
    // primary index.
    if (pk_index != MAX_INDEXES && field->part_of_key.is_set(pk_index))
    {
      is_part_of_pk_.emplace_back(true);
    }
    else
    {
      is_part_of_pk_.emplace_back(false);
      ++non_pk_column_count_;
    }
    ++field_idx;
  }

  // The primary key's index equals to MAX_INDEXES, if the table has no primary
  // index.
  if (pk_index != MAX_INDEXES)
  {
    KEY *pk_info= table_share->key_info + pk_index;
    for (size_t key_part_idx= 0;
         key_part_idx < pk_info->user_defined_key_parts; ++key_part_idx)
    {
      const mysql::KEY_PART_INFO &key_part= pk_info->key_part[key_part_idx];
      pk_field_idx_.emplace_back(key_part.fieldnr - 1);
    }
  }
}

void MyEloq::EloqRecordSchema::Set(const mysql::TABLE_SHARE *table_share)
{
  field_types_.clear();
  is_part_of_pk_.clear();
  pk_field_idx_.clear();

  uint8_t pk_index= table_share->primary_key;

  Field **field_head= table_share->field;
  for (size_t field_idx= 0; *field_head != nullptr; ++field_head)
  {
    Field *field= *field_head;
    // For computed(virtual) field, if VIRTUAL, do not put them in
    // field_types_.
    if (!field->stored_in_db())
    {
      continue;
    }

    field_types_.emplace_back(EloqFieldType::Convert(field));
    if (field->unireg_check == Field::NEXT_NUMBER)
    {
      auto_increment_idx_= field_idx;
    }

    // The primary key's index equals to MAX_INDEXES, if the table has no
    // primary index.
    if (pk_index != MAX_INDEXES && field->part_of_key.is_set(pk_index))
    {
      is_part_of_pk_.emplace_back(true);
    }
    else
    {
      is_part_of_pk_.emplace_back(false);
    }
    ++field_idx;
  }

  // The primary key's index equals to MAX_INDEXES, if the table has no primary
  // index.
  if (pk_index != MAX_INDEXES)
  {
    KEY *pk_info= table_share->key_info + pk_index;
    for (size_t key_part_idx= 0;
         key_part_idx < pk_info->user_defined_key_parts; ++key_part_idx)
    {
      const mysql::KEY_PART_INFO &key_part= pk_info->key_part[key_part_idx];
      pk_field_idx_.emplace_back(key_part.fieldnr - 1);
    }
  }
}

void MyEloq::EloqRecordSchema::Encode(const uchar *table_record,
                                      Field **field_head,
                                      const uchar *table_record_0,
                                      std::vector<char> &buf) const
{
  buf.clear();
  uint16_t bitmap_bytes= BitmapBytes();
  buf.insert(buf.end(), bitmap_bytes, 0);

  uchar *rec_buf= (uchar *) table_record;
  uchar *rec_0_buf= (uchar *) table_record_0;

  std::string str_container;
  str_container.reserve(32);
  size_t fidx= 0, non_pk_offset= 0;
  for (; *field_head != nullptr; ++field_head)
  {
    // purely virtual column. and fidx need not increment.
    if (!(*field_head)->stored_in_db())
    {
      continue;
    }
    // Skips primary-key fields in the encoded record.
    if (is_part_of_pk_[fidx])
    {
      ++fidx;
      continue;
    }

    Field *field= *field_head;
    // The following code is copied from
    // Rdb_convert_to_record_value_decoder::decode of RocksDB, to redirect the
    // field pointer to the input table record, so that we can use the field's
    // functions to retrieve the field's value. Though in most cases the input
    // record points to table->record[0], the input table record may point to
    // table->record[1]. One example is SET pk = pk + 1, where table->record[0]
    // points to the new row to be inserted and table->record[1] points to the
    // old row to be deleted.
    uint field_offset= field->ptr - table_record_0;
    uint null_offset= field->null_offset();
    bool maybe_null= field->real_maybe_null();
    field->move_field(rec_buf + field_offset,
                      maybe_null ? rec_buf + null_offset : nullptr,
                      field->null_bit);

    bool is_null= maybe_null && field->is_real_null();
    if (is_null)
    {
      char &bitmap_byte= buf[non_pk_offset >> 3];
      uint8_t offset= non_pk_offset & 7;
      bitmap_byte|= is_null << offset;
    }
    else
    {
      const EloqFieldType &eloq_type= field_types_.at(fidx);
      switch (eloq_type.data_type_)
      {
      case EloqDataType::Integer: {
        buf.insert(buf.end(), field->ptr, field->ptr + eloq_type.len_);
        break;
      }
      case EloqDataType::UnsignedInteger: {
        buf.insert(buf.end(), field->ptr, field->ptr + eloq_type.len_);
        break;
      }
      case EloqDataType::Float: {
        double val= field->val_real();
        if (eloq_type.len_ == 8)
        {
          EncodeDouble(val, buf);
        }
        else
        {
          float f_val= val;
          EncodeFloat(f_val, buf);
        }
        break;
      }
      case EloqDataType::VarString: {
        EncodeString(field, buf, eloq_type.len_, false);
        break;
      }
      case EloqDataType::VarBinary: {
        if (field->real_type() == MYSQL_TYPE_VARCHAR)
        {
          uint8_t len_bytes= eloq_type.len_;
          Field_str *str_field= (Field_str *) field;
          uint32_t str_len= str_field->data_length();
          EncodeUnsignedInteger(str_len, buf, len_bytes);
          buf.insert(buf.end(), str_field->ptr + len_bytes,
                     str_field->ptr + len_bytes + str_len);
        }
        else
        {
          Field_blob *blob_field= (Field_blob *) field;
          uint32_t blob_len= blob_field->get_length();
          uchar *blob_ptr= blob_field->get_ptr();
          // If the blob uses 1 or 2 bytes for the blob length, uses a 1-byte
          // or 2-byte integer to encode the length. Or, uses a 4-byte integer.
          uint8_t len_bytes= eloq_type.len_ <= 2 ? eloq_type.len_ : 4;
          EncodeUnsignedInteger(blob_len, buf, len_bytes);
          buf.insert(buf.end(), blob_ptr, blob_ptr + blob_len);
        }
        break;
      }
      case EloqDataType::String: {
        EncodeString(field, buf, eloq_type.len_, true);
        break;
      }
      case EloqDataType::Binary: {
        buf.insert(buf.end(), field->ptr, field->ptr + eloq_type.len_);
        break;
      }
      case EloqDataType::Time: {
        // For the Time type, "len_" represents the precision.
        uint8_t storage_len= mysql::my_time_binary_length(eloq_type.len_);
        buf.insert(buf.end(), field->ptr, field->ptr + storage_len);
        break;
      }
      case EloqDataType::Date: {
        buf.insert(buf.end(), field->ptr, field->ptr + eloq_type.len_);
        break;
      }
      case EloqDataType::Timestamp: {
        uint8_t storage_len= mysql::my_timestamp_binary_length(eloq_type.len_);
        buf.insert(buf.end(), field->ptr, field->ptr + storage_len);
        break;
      }
      case EloqDataType::Datetime: {
        // For the DateTime type, "len_" represents the precision, not the
        // storage length.
        uint8_t storage_len= mysql::my_datetime_binary_length(eloq_type.len_);
        buf.insert(buf.end(), field->ptr, field->ptr + storage_len);
        break;
      }
      case EloqDataType::Decimal: {
        uint8_t precision= eloq_type.len_ >> 8;
        uint8_t scale= eloq_type.len_ & 0xFF;
        uint8_t byte_len= decimal_bin_size(precision, scale);
        buf.insert(buf.end(), field->ptr, field->ptr + byte_len);
        break;
      }
      default:
        break;
      }
    }

    ++non_pk_offset;

    // The following code is copied from
    // Rdb_convert_to_record_value_decoder::decode of RocksDB, to restore the
    // field pointer to table_record[0].
    field->move_field(rec_0_buf + field_offset,
                      maybe_null ? rec_0_buf + null_offset : nullptr,
                      field->null_bit);
    ++fidx;
  }
}

#if defined(DATA_STORE_TYPE_CASSANDRA)
/**
 * @brief Encode cass row into mysql record format. Only encode non-pk cols,
 * assume col idx start from 0 in cass row.
 *
 * @param table_type
 * @param row
 * @param buf
 */
void MyEloq::EloqRecordSchema::EncodeToSerializeFormat(
    txservice::TableType table_type, const void *row, std::string &buf) const
{
  buf.clear();

  const CassRow *cass_row= reinterpret_cast<const CassRow *>(row);
  const CassValue *unpack_info_value=
      cass_row_get_column_by_name(cass_row, "___unpack_info___");

  const cass_byte_t *unpack_info= NULL;
  size_t unpack_len= 0;
  if (cass_value_is_null(unpack_info_value) == cass_false)
  {
    cass_value_get_bytes(unpack_info_value, &unpack_info, &unpack_len);
  }

  const char *unpack_len_ptr=
      static_cast<const char *>(static_cast<const void *>(&unpack_len));
  buf.append(unpack_len_ptr, sizeof(size_t));
  buf.append(reinterpret_cast<const char *>(unpack_info), unpack_len);

  if (table_type == txservice::TableType::Primary ||
      table_type == txservice::TableType::UniqueSecondary)
  {
    const cass_byte_t *encoded_blob= NULL;
    size_t encoded_blob_len= 0;
    cass_value_get_bytes(cass_row_get_column(cass_row, 0), &encoded_blob,
                         &encoded_blob_len);
    const char *len_ptr= reinterpret_cast<const char *>(&encoded_blob_len);
    buf.append(len_ptr, sizeof(size_t));
    buf.append(reinterpret_cast<const char *>(encoded_blob), encoded_blob_len);
  }
  else
  {
    size_t len_val= 0;
    const char *len_ptr= reinterpret_cast<const char *>(&len_val);
    buf.append(len_ptr, sizeof(size_t));
  }
}

void MyEloq::EloqRecordSchema::EncodeToTxRecord(
    const txservice::TableName &table_name, const void *row,
    txservice::TxRecord &tx_record) const
{
  EloqRecord &eloq_record= static_cast<EloqRecord &>(tx_record);

  const CassRow *cass_row= reinterpret_cast<const CassRow *>(row);
  const CassValue *unpack_info_value=
      cass_row_get_column_by_name(cass_row, "___unpack_info___");

  const cass_byte_t *unpack_info= NULL;
  size_t unpack_len= 0;
  cass_value_get_bytes(unpack_info_value, &unpack_info, &unpack_len);
  eloq_record.SetUnpackInfo(unpack_info, unpack_len);

  if (table_name.Type() == txservice::TableType::Primary ||
      table_name.Type() == txservice::TableType::UniqueSecondary)
  {
    const cass_byte_t *encoded_blob= NULL;
    size_t encoded_blob_len= 0;
    cass_value_get_bytes(cass_row_get_column(cass_row, 0), &encoded_blob,
                         &encoded_blob_len);
    eloq_record.SetEncodedBlob(encoded_blob, encoded_blob_len);
  }
}
#endif

void MyEloq::EloqRecordSchema::Decode(
    const MY_BITMAP *decode_set, uchar *table_record,
    mysql::Field **field_head, uchar *table_record_0,
    const std::vector<char> &buf, bool is_ckpt_delta, bool is_deleted) const
{
  // Field data starts after the bitmap header, which indicates whether or not
  // a field is null.
  size_t buf_offset= BitmapBytes(is_ckpt_delta);
  size_t fidx= 0, fid_off= 0, non_pk_offset= 0;
  for (; *field_head != nullptr; ++field_head, ++fid_off)
  {
    // purely virtual column. and fidx need not increment.
    if (!(*field_head)->stored_in_db())
    {
      continue;
    }
    if (is_part_of_pk_[fidx])
    {
      ++fidx;
      continue;
    }

    Field *field= *field_head;

    if (is_ckpt_delta && fidx == field_types_.size() - 1)
    {
      field->set_notnull();
      field->store(is_deleted ? 1 : 0);
      break;
    }

    // The following code is copied from
    // Rdb_convert_to_record_value_decoder::decode of RocksDB, to redirect the
    // field pointer to the input table record, so that we can use the field's
    // functions to set the field's value. Though in most cases the input
    // record points to table->record[0], in sine circumstances, the input
    // table record may point to table->record[1].
    uint field_offset= field->ptr - table_record_0;
    uint null_offset= field->null_offset();
    bool maybe_null= field->real_maybe_null();
    field->move_field(table_record + field_offset,
                      maybe_null ? table_record + null_offset : nullptr,
                      field->null_bit);

    const char &bitmap_byte= buf[non_pk_offset >> 3];
    uint8_t bit_offset= non_pk_offset & 7;
    bool is_null= bitmap_byte & (1 << bit_offset);

    if (is_null)
    {
      field->set_null();
    }
    else
    {
      field->set_notnull();

      bool field_requested= bitmap_is_set(decode_set, fid_off);

      const EloqFieldType &eloq_type= field_types_.at(fidx);
      switch (eloq_type.data_type_)
      {
      case EloqDataType::Integer:
      case EloqDataType::UnsignedInteger: {
        if (field_requested)
        {
          memcpy(field->ptr, buf.data() + buf_offset, eloq_type.len_);
        }
        buf_offset+= eloq_type.len_;
        break;
      }
      case EloqDataType::Float: {
        if (field_requested)
        {
          if (eloq_type.len_ == 8)
          {
            double d_val;
            DecodeDouble(d_val, buf, buf_offset);
            field->store(d_val);
          }
          else
          {
            float f_val;
            DecodeFloat(f_val, buf, buf_offset);
            field->store(f_val);
          }
        }
        else
        {
          buf_offset+= eloq_type.len_;
        }
        break;
      }
      case EloqDataType::VarString: {
        uint8_t len_bytes= eloq_type.len_;
        uint32_t str_len=
            read_lowendian((uchar *) buf.data() + buf_offset, len_bytes);
        buf_offset+= len_bytes;
        if (field_requested)
        {
          if (field->real_type() == MYSQL_TYPE_VARCHAR)
          {
            field->store(buf.data() + buf_offset, str_len, field->charset());
          }
          else
          {
            // MySQL treat TEXT type as MYSQL_TYPE_BLOB.
            Field_blob *blob_field= static_cast<Field_blob *>(field);
            blob_field->set_ptr((uint32_t) str_len,
                                (uchar *) (buf.data() + buf_offset));
          }
        }
        buf_offset+= str_len;
        break;
      }
      case EloqDataType::VarBinary: {
        if (field->real_type() == MYSQL_TYPE_VARCHAR)
        {
          uint64_t str_len;
          uint8_t len_bytes= eloq_type.len_;
          DecodeUnsignedInteger(str_len, buf, buf_offset, len_bytes);
          if (field_requested)
          {
            field->store(buf.data() + buf_offset, str_len, field->charset());
          }
          buf_offset+= str_len;
        }
        else
        {
          uint64_t str_len;
          uint8_t len_bytes= eloq_type.len_ <= 2 ? eloq_type.len_ : 4;
          DecodeUnsignedInteger(str_len, buf, buf_offset, len_bytes);

          if (field_requested)
          {
            uchar *blob_ptr= (uchar *) buf.data() + buf_offset;

            Field_blob *blob_field= static_cast<Field_blob *>(field);
            blob_field->set_ptr((uint32_t) str_len, blob_ptr);
          }

          buf_offset+= str_len;
        }
        break;
      }
      case EloqDataType::String:
      case EloqDataType::Binary: {
        if (field_requested)
        {
          field->store(buf.data() + buf_offset, eloq_type.len_,
                       field->charset());
        }
        buf_offset+= eloq_type.len_;
        break;
      }
      case EloqDataType::Time: {
        uint8_t storage_len= mysql::my_time_binary_length(eloq_type.len_);
        if (field_requested)
        {
          memcpy(field->ptr, buf.data() + buf_offset, storage_len);
        }
        buf_offset+= storage_len;
        break;
      }
      case EloqDataType::Date: {
        if (field_requested)
        {
          memcpy(field->ptr, buf.data() + buf_offset, eloq_type.len_);
        }
        buf_offset+= eloq_type.len_;
        break;
      }
      case EloqDataType::Timestamp: {
        uint8_t storage_len= mysql::my_timestamp_binary_length(eloq_type.len_);
        if (field_requested)
        {
          memcpy(field->ptr, buf.data() + buf_offset, storage_len);
        }
        buf_offset+= storage_len;
        break;
      }
      case EloqDataType::Datetime: {
        uint8_t storage_len= mysql::my_datetime_binary_length(eloq_type.len_);
        if (field_requested)
        {
          memcpy(field->ptr, buf.data() + buf_offset, storage_len);
        }
        buf_offset+= storage_len;
        break;
      }
      case EloqDataType::Decimal: {
        uint8_t precision= eloq_type.len_ >> 8;
        uint8_t scale= eloq_type.len_ & 0xFF;
        uint8_t byte_len= decimal_bin_size(precision, scale);
        if (field_requested)
        {
          memcpy(field->ptr, buf.data() + buf_offset, byte_len);
        }
        buf_offset+= byte_len;
        break;
      }
      default:
        break;
      }
    }

    ++non_pk_offset;

    // The following code is copied from
    // Rdb_convert_to_record_value_decoder::decode of RocksDB, to restore the
    // field pointer to table_record[0].
    field->move_field(table_record_0 + field_offset,
                      maybe_null ? table_record_0 + null_offset : nullptr,
                      field->null_bit);

    ++fidx;
  }
}

#if defined(DATA_STORE_TYPE_DYNAMODB)
void MyEloq::EloqRecordSchema::Encode(
    const Aws::Map<Aws::String, Aws::DynamoDB::Model::AttributeValue> &row,
    std::vector<char> &buf) const
{
  buf.clear();
  // Reserves the bitmap bytes at the beginning.
  uint16_t bitmap_bytes= BitmapBytes();
  buf.insert(buf.end(), bitmap_bytes, 0);

  for (size_t fidx= 0, non_pk_offset= 0; fidx < field_types_.size(); ++fidx)
  {
    if (is_part_of_pk_[fidx])
    {
      continue;
    }

    const EloqFieldType &eloq_type= field_types_.at(fidx);
    const Aws::DynamoDB::Model::AttributeValue col=
        row.at(eloq_type.field_name_);

    if (col.GetNull())
    {
      char &bitmap_byte= buf[non_pk_offset >> 3];
      uint8_t offset= non_pk_offset & 7;
      bitmap_byte|= 1 << offset;
      ++non_pk_offset;
      continue;
    }
    else
    {
      ++non_pk_offset;
    }

    EncodeDynamoValue(col, buf, eloq_type, false);
  }
}

void MyEloq::EloqRecordSchema::EncodeDynamoValue(
    const Aws::DynamoDB::Model::AttributeValue &att, std::vector<char> &buf,
    const EloqFieldType &field_type, bool is_key_field)
{
  switch (field_type.data_type_)
  {
  case EloqDataType::Integer: {
    switch (field_type.len_)
    {
    case 1:
    case 2:
    case 3:
    case 4: {
      int val= std::stoi(att.GetN());

      size_t before_head= buf.size();
      buf.resize(buf.size() + field_type.len_, 0);
      store_lowendian(val, (uchar *) buf.data() + before_head,
                      field_type.len_);
      break;
    }

    case 8: {
      int64_t val= std::stoll(att.GetN());

      size_t before_head= buf.size();
      buf.resize(buf.size() + field_type.len_, 0);
      store_lowendian(val, (uchar *) buf.data() + before_head,
                      field_type.len_);
      break;
    }
    default:
      break;
    }

    break;
  }
  case EloqDataType::UnsignedInteger: {
    switch (field_type.len_)
    {
    case 1:
    case 2:
    case 3: {
      int val= std::stoi(att.GetN());

      size_t before_head= buf.size();
      buf.resize(buf.size() + field_type.len_, 0);
      store_lowendian(val, (uchar *) buf.data() + before_head,
                      field_type.len_);
      break;
    }
    case 4: {
      int64_t val= std::stoll(att.GetN());

      size_t before_head= buf.size();
      buf.resize(buf.size() + field_type.len_, 0);
      store_lowendian(val, (uchar *) buf.data() + before_head,
                      field_type.len_);
      break;
    }
    default:
      uint64_t val= std::stoull(att.GetN());

      size_t before_head= buf.size();
      buf.resize(buf.size() + field_type.len_, 0);
      store_lowendian(val, (uchar *) buf.data() + before_head,
                      field_type.len_);
      break;
    }

    break;
  }
  case EloqDataType::Float: {
    if (field_type.len_ == 8)
    {
      double d_val= std::stod(att.GetN());
      EncodeDouble(d_val, buf);
    }
    else
    {
      float f_val= std::stof(att.GetN());
      EncodeFloat(f_val, buf);
    }
    break;
  }
  case EloqDataType::VarString: {
    std::string str= att.GetS();

    if (is_key_field)
    {
      size_t before_tail= buf.size();
      // When the string field is a key field, "len" represents the string
      // length. And "len" is always encoded in a 2-byte integer.
      buf.resize(buf.size() + 2 + field_type.len_, 0);
      store_lowendian(str.size(), (uchar *) buf.data() + before_tail, 2);
      memcpy(buf.data() + before_tail + 2, str.data(), str.size());
    }
    else
    {
      size_t before_tail= buf.size();
      // When the string field is a non-key field, "len" represents the number
      // of bytes to encode the string length.
      buf.resize(buf.size() + field_type.len_ + str.size());
      store_lowendian(str.size(), (uchar *) buf.data() + before_tail,
                      field_type.len_);
      memcpy(buf.data() + before_tail + field_type.len_, str.data(),
             str.size());
    }
    break;
  }
  case EloqDataType::VarBinary: {
    Aws::Utils::ByteBuffer bytes= att.GetB();
    uint8_t len_bytes= field_type.len_ <= 2 ? field_type.len_ : 4;
    EncodeUnsignedInteger(bytes.GetLength(), buf, len_bytes);
    buf.insert(buf.end(), bytes.GetUnderlyingData(),
               bytes.GetUnderlyingData() + bytes.GetLength());

    break;
  }
  case EloqDataType::String: {
    std::string str= att.GetS();

    assert(str.size() == field_type.len_);

    buf.insert(buf.end(), str.data(), str.data() + str.size());

    break;
  }
  case EloqDataType::Binary: {
    Aws::Utils::ByteBuffer bytes= att.GetB();
    assert(bytes.GetLength() == field_type.len_);
    buf.insert(buf.end(), bytes.GetUnderlyingData(),
               bytes.GetUnderlyingData() + bytes.GetLength());
    break;
  }
  case EloqDataType::Date: {
    // We encode a DATE field as an unsigned 32-bit integer to represent
    // the number of days since Epoch centered at 2^31.
    uint32_t days= std::stoul(att.GetN());

    int32_t num_of_days= days >= EloqFieldType::CassDateEpochCenter
                             ? (days - EloqFieldType::CassDateEpochCenter)
                             : -(EloqFieldType::CassDateEpochCenter - days);

    using namespace std::chrono;

    date::sys_days sys_d{date::days{num_of_days}};
    date::year_month_day dp(sys_d);

    int year= int{dp.year()};
    int month= unsigned{dp.month()};
    int day= unsigned{dp.day()};

    size_t begin= buf.size();
    buf.resize(buf.size() + field_type.len_);

    // The following code is copied from Field_newdate::store_TIME().
    uint tmp= year * 16 * 32 + month * 32 + day;
    int3store((uchar *) (buf.data() + begin), tmp);

    break;
  }
  case EloqDataType::Time: {
    int64_t elapse_nano_sec= std::stoll(att.GetN());
    // Dynamo's time type represents the nano seconds since the mid night.
    int64_t elapse_micro_sec= elapse_nano_sec / 1000;
    EncodeTime(elapse_micro_sec, field_type.len_, buf);
    break;
  }
  case EloqDataType::Timestamp: {
    int64_t elapse_micro_sec= std::stoll(att.GetN());
    mysql::timeval tm;
    tm.tv_sec= elapse_micro_sec / 1000000;
    tm.tv_usec= elapse_micro_sec % 1000000;
    uint8_t storage_len= mysql::my_timestamp_binary_length(field_type.len_);
    size_t begin= buf.size();
    buf.resize(buf.size() + storage_len);
    mysql::my_timestamp_to_binary(&tm, (uchar *) (buf.data() + begin),
                                  field_type.len_);
    break;
  }
  case EloqDataType::Datetime: {
    int64_t elapse= std::stoll(att.GetN());
    EncodeDatetime(elapse, field_type.len_, buf);
    break;
  }
  case EloqDataType::Decimal: {
    int32_t scale= field_type.len_ & 0xFF;

    std::string decimal_str= att.GetN();

    uint8_t precision= field_type.len_ >> 8;
    my_decimal mysql_decimal;
    mysql_decimal.intg= precision - scale;
    mysql_decimal.frac= scale;
    char *end= decimal_str.data() + decimal_str.size() - 1;
    while (*end == '\0')
    {
      --end;
    }
    ++end;
    mysql::internal_str2dec(decimal_str.data(), &mysql_decimal, &end, true);

    assert(scale == (field_type.len_ & 0xFF));
    uint8_t buf_len= mysql::decimal_bin_size(precision, scale);
    size_t begin= buf.size();
    buf.resize(buf.size() + buf_len);

    mysql::decimal2bin(&mysql_decimal, (uchar *) (buf.data() + begin),
                       precision, scale);

    break;
  }
  default:
    break;
  }
}
#endif

void MyEloq::EloqRecordSchema::ColumnList(std::string &col_list) const
{
  for (const EloqFieldType &field_type : field_types_)
  {
    col_list.append("\"");
    col_list.append(field_type.field_name_);
    col_list.append("\",");
  }

  if (pk_field_idx_.empty())
  {
    col_list.append("uuid_key,");
  }
}

void MyEloq::EloqRecordSchema::NonPkColumnList(std::string &col_list) const
{
  for (size_t fidx= 0; fidx < field_types_.size(); ++fidx)
  {
    if (is_part_of_pk_[fidx])
    {
      continue;
    }
    col_list.append("\"");
    col_list.append(field_types_[fidx].field_name_);
    col_list.append("\",");
  }
}

#if defined(DATA_STORE_TYPE_CASSANDRA)
// generate Cassandra pair of non-pk column name and type name.
void MyEloq::EloqRecordSchema::NonPkColumnListWithType(
    std::string &col_list) const
{
  for (size_t fidx= 0; fidx < field_types_.size(); ++fidx)
  {
    if (is_part_of_pk_[fidx])
    {
      continue;
    }
    col_list.append("\"");
    col_list.append(field_types_[fidx].field_name_);
    col_list.append("\" ");
    col_list.append(EloqFieldType::GetCassTypeName(field_types_[fidx]));
    col_list.append(",");
  }
}

// generate Cassandra pair of column name and type name.
void MyEloq::EloqRecordSchema::ColumnListWithType(std::string &col_list) const
{
  for (const EloqFieldType &field_type : field_types_)
  {
    col_list.append("\"");
    col_list.append(field_type.field_name_);
    col_list.append("\" ");
    col_list.append(EloqFieldType::GetCassTypeName(field_type));
    col_list.append(",");
  }

  if (pk_field_idx_.empty())
  {
    col_list.append("uuid_key blob,");
  }
}
#endif

#if defined(DATA_STORE_TYPE_BIGTABLE)
void MyEloq::EloqRecordSchema::Encode(const std::string &payload,
                                      std::vector<char> &buf) const
{
  buf.clear();
  buf.assign(payload.begin(), payload.end());
}

void MyEloq::EloqRecordSchema::BindBigTablePayload(
    const std::vector<char> &rec_buf, std::string &payload)
{
  payload.assign(rec_buf.data(), rec_buf.size());
}
#endif

uint16_t MyEloq::EloqRecordSchema::ColumnCount() const
{
  uint16_t explicit_fields= (uint16_t) field_types_.size();
  return pk_field_idx_.empty() ? explicit_fields + 1 : explicit_fields;
}

/**
 * @brief Number of columns that cannot be covered by primary key, which also
 * includes columns that has its prefix as primary key part.
 *
 * @return uint16_t
 */
uint16_t MyEloq::EloqRecordSchema::NonPkColumnCount() const
{
  return non_pk_column_count_;
}

void MyEloq::EloqRecordSchema::EncodeInteger(int8_t val,
                                             std::vector<char> &buf)
{
  // Assume little-endian
  char *ptr= (char *) &val;
  buf.insert(buf.end(), ptr, ptr + 1);
}

void MyEloq::EloqRecordSchema::DecodeInteger(int8_t &val,
                                             const std::vector<char> &buf,
                                             size_t &offset)
{
  // Assume little-endian
  val= 0;
  char *ptr= (char *) &val;
  std::copy(buf.data() + offset, buf.data() + offset + 1, ptr);
  offset+= 1;
}

void MyEloq::EloqRecordSchema::EncodeInteger(int16_t val,
                                             std::vector<char> &buf)
{
  // Assume little-endian
  char *ptr= (char *) &val;
  buf.insert(buf.end(), ptr, ptr + 2);
}

void MyEloq::EloqRecordSchema::DecodeInteger(int16_t &val,
                                             const std::vector<char> &buf,
                                             size_t &offset)
{
  // Assume little-endian
  val= 0;
  char *ptr= (char *) &val;
  std::copy(buf.data() + offset, buf.data() + offset + 2, ptr);
  offset+= 2;
}

void MyEloq::EloqRecordSchema::EncodeInteger(int32_t val,
                                             std::vector<char> &buf)
{
  // Assume little-endian
  char *ptr= (char *) &val;
  buf.insert(buf.end(), ptr, ptr + 4);
}

void MyEloq::EloqRecordSchema::DecodeInteger(int32_t &val,
                                             const std::vector<char> &buf,
                                             size_t &offset)
{
  // Assume little-endian
  val= 0;
  char *ptr= (char *) &val;
  std::copy(buf.data() + offset, buf.data() + offset + 4, ptr);
  offset+= 4;
}

void MyEloq::EloqRecordSchema::EncodeInteger(int64_t val,
                                             std::vector<char> &buf)
{
  // Assume little-endian
  char *ptr= (char *) &val;
  buf.insert(buf.end(), ptr, ptr + 8);
}

void MyEloq::EloqRecordSchema::DecodeInteger(int64_t &val,
                                             const std::vector<char> &buf,
                                             size_t &offset)
{
  // Assume little-endian
  val= 0;
  char *ptr= (char *) &val;
  std::copy(buf.data() + offset, buf.data() + offset + 8, ptr);
  offset+= 8;
}

void MyEloq::EloqRecordSchema::EncodeInteger(int64_t val,
                                             std::vector<char> &buf,
                                             uint8_t len)
{
  // Assume little-endian

  uint64_t *ptr= (uint64_t *) &val;
  store_lowendian(*ptr, (uchar *) buf.data() + buf.size(), len);
}

void MyEloq::EloqRecordSchema::DecodeInteger(int64_t &val,
                                             const std::vector<char> &buf,
                                             size_t &offset, uint8_t len)
{
  // Assume little-endian

  switch (len)
  {
  case 1: {
    int8_t val_8= 0;
    char *ptr= (char *) &val_8;
    std::copy(buf.data() + offset, buf.data() + offset + 1, ptr);
    offset+= 1;
    val= val_8;
    break;
  }
  case 2: {
    int16_t val_16= 0;
    char *ptr= (char *) &val_16;
    std::copy(buf.data() + offset, buf.data() + offset + 2, ptr);
    offset+= 2;
    val= val_16;
    break;
  }
  case 4: {
    int32_t val_32= 0;
    char *ptr= (char *) &val_32;
    std::copy(buf.data() + offset, buf.data() + offset + 4, ptr);
    offset+= 4;
    val= val_32;
    break;
  }
  case 8: {
    val= 0;
    char *ptr= (char *) &val;
    std::copy(buf.data() + offset, buf.data() + offset + 8, ptr);
    offset+= 8;
    break;
  }
  default:
    break;
  }
}

void MyEloq::EloqRecordSchema::EncodeUnsignedInteger(uint64_t val,
                                                     std::vector<char> &buf,
                                                     uint8_t len)
{
  // Assume little-endian
  char *ptr= (char *) &val;
  buf.insert(buf.end(), ptr, ptr + len);
}

void MyEloq::EloqRecordSchema::DecodeUnsignedInteger(
    uint64_t &val, const std::vector<char> &buf, size_t &offset, uint8_t len)
{
  // Assume little-endian
  val= 0;
  char *ptr= (char *) &val;
  std::copy(buf.data() + offset, buf.data() + offset + len, ptr);
  offset+= len;
}

void MyEloq::EloqRecordSchema::EncodeString(Field *field,
                                            std::vector<char> &buf,
                                            uint16_t len, bool fixed_len)
{
  if (!fixed_len)
  {
    // For variable-length strings, "len" represents the number of bytes to
    // encode the string length.
    uint8_t len_bytes= len;
    uint32_t str_len= 0;
    uchar *str_ptr= nullptr;

    if (field->real_type() == MYSQL_TYPE_VARCHAR)
    {
      str_len= field->data_length();
      str_ptr= field->ptr + len_bytes;
    }
    else
    {
      // [TINY|MEDIUM|LONG]TEXT type
      Field_blob *blob_field= (Field_blob *) field;
      str_len= blob_field->get_length();
      str_ptr= blob_field->get_ptr();
    }
    size_t before_head= buf.size();
    buf.resize(buf.size() + len_bytes + str_len);
    store_lowendian(str_len, (uchar *) buf.data() + before_head, len_bytes);
    memcpy(buf.data() + before_head + len_bytes, str_ptr, str_len);
  }
  else
  {
    // For fixed-length strings, "len" represents the string length.
    buf.insert(buf.end(), field->ptr, field->ptr + len);
  }
}

void MyEloq::EloqRecordSchema::EncodeDatetime(int64_t elapse_micro_sec,
                                              uint8_t precision,
                                              std::vector<char> &buf)
{
  using namespace std::chrono;
  assert(TIME_SECOND_PART_FACTOR == std::micro::den);

  auto dur= microseconds(elapse_micro_sec);
  time_point<system_clock, microseconds> tp(dur);
  date::sys_days day= date::floor<date::days>(tp);
  auto ltime= date::make_time(tp - day);
  date::year_month_day dp(day);
  int year= int{dp.year()};

  MYSQL_TIME mysql_time;
  mysql_time.neg= false;
  mysql_time.year= year;
  mysql_time.month= unsigned{dp.month()};
  mysql_time.day= unsigned{dp.day()};
  mysql_time.hour= ltime.hours().count();
  mysql_time.minute= ltime.minutes().count();
  mysql_time.second= ltime.seconds().count();
  mysql_time.second_part= ltime.subseconds().count();

  mysql_time.time_type= enum_mysql_timestamp_type::MYSQL_TIMESTAMP_DATETIME;

  // We use the MySQL's built-in function to store the datatime field in
  // the MySQL's encoding.
  uint8_t storage_length= mysql::my_datetime_binary_length(precision);
  size_t start= buf.size();
  buf.resize(buf.size() + storage_length, 0);
  longlong tmp= mysql::TIME_to_longlong_datetime_packed(&mysql_time);
  mysql::my_datetime_packed_to_binary(tmp, (uchar *) (buf.data() + start),
                                      precision);
}

void MyEloq::EloqRecordSchema::DecodeDatetime(int64_t &elapse_micro_sec,
                                              uint8_t precision,
                                              const std::vector<char> &buf,
                                              size_t &offset)
{
  longlong tmp= mysql::my_datetime_packed_from_binary(
      (uchar *) (buf.data() + offset), precision);
  MYSQL_TIME mysql_time;
  mysql::TIME_from_longlong_datetime_packed(&mysql_time, tmp);
  offset+= mysql::my_datetime_binary_length(precision);

  using namespace std::chrono;
  assert(TIME_SECOND_PART_FACTOR == std::micro::den);

  int year= mysql_time.year;
  uint32_t month= mysql_time.month == 0 ? 1 : mysql_time.month;
  uint32_t day= mysql_time.day == 0 ? 1 : mysql_time.day;

  date::year_month_day day_point= date::year{year} / month / day;
  auto tp= date::sys_days{day_point} + hours(mysql_time.hour) +
           minutes(mysql_time.minute) + seconds(mysql_time.second) +
           microseconds(mysql_time.second_part);

  elapse_micro_sec= tp.time_since_epoch().count();
}

void MyEloq::EloqRecordSchema::EncodeTime(int64_t elapse_micro_sec,
                                          uint8_t precision,
                                          std::vector<char> &buf)
{
  using namespace std::chrono;
  assert(TIME_SECOND_PART_FACTOR == std::micro::den);

  auto dur= microseconds(elapse_micro_sec);
  auto ltime= date::make_time(dur);

  MYSQL_TIME mysql_time;
  mysql_time.neg= elapse_micro_sec < 0;
  mysql_time.year= 0;
  mysql_time.month= 0;
  mysql_time.day= 0;
  mysql_time.hour= ltime.hours().count();
  mysql_time.minute= ltime.minutes().count();
  mysql_time.second= ltime.seconds().count();
  mysql_time.second_part= ltime.subseconds().count();
  mysql_time.time_type= enum_mysql_timestamp_type::MYSQL_TIMESTAMP_TIME;

  // We use the MySQL's built-in function to store the datatime field in
  // the MySQL's encoding.
  uint8_t storage_length= mysql::my_time_binary_length(precision);
  size_t start= buf.size();
  buf.resize(buf.size() + storage_length, 0);
  longlong tmp= mysql::TIME_to_longlong_time_packed(&mysql_time);
  mysql::my_time_packed_to_binary(tmp, (uchar *) (buf.data() + start),
                                  precision);
}

void MyEloq::EloqRecordSchema::DecodeTime(int64_t &elapse_micro_sec,
                                          uint8_t precision,
                                          const std::vector<char> &buf,
                                          size_t &offset)
{
  longlong tmp= mysql::my_time_packed_from_binary(
      (uchar *) (buf.data() + offset), precision);
  MYSQL_TIME mysql_time;
  mysql::TIME_from_longlong_time_packed(&mysql_time, tmp);
  offset+= mysql::my_time_binary_length(precision);

  using namespace std::chrono;
  assert(TIME_SECOND_PART_FACTOR == std::micro::den);

  auto tp= hours(mysql_time.hour) + minutes(mysql_time.minute) +
           seconds(mysql_time.second) + microseconds(mysql_time.second_part);

  elapse_micro_sec= mysql_time.neg ? -tp.count() : tp.count();
}

void MyEloq::EloqRecordSchema::EncodeDouble(double val, std::vector<char> &buf)
{
  char *ptr= (char *) &val;
  buf.insert(buf.end(), ptr, ptr + sizeof(double));
}

void MyEloq::EloqRecordSchema::DecodeDouble(double &val,
                                            const std::vector<char> &buf,
                                            size_t &offset)
{
  char *ptr= (char *) &val;
  memset(ptr, 0, sizeof(double));
  std::copy(buf.data() + offset, buf.data() + offset + sizeof(double), ptr);
  offset+= 8;
}

void MyEloq::EloqRecordSchema::EncodeFloat(float val, std::vector<char> &buf)
{
  char *ptr= (char *) &val;
  buf.insert(buf.end(), ptr, ptr + sizeof(float));
}

void MyEloq::EloqRecordSchema::DecodeFloat(float &val,
                                           const std::vector<char> &buf,
                                           size_t &offset)
{
  char *ptr= (char *) &val;
  memset(ptr, 0, sizeof(float));
  std::copy(buf.data() + offset, buf.data() + offset + sizeof(float), ptr);
  offset+= 4;
}

#if defined(DATA_STORE_TYPE_CASSANDRA)
void MyEloq::EloqRecordSchema::EncodeCassValue(const CassValue *cass_val,
                                               std::vector<char> &buf,
                                               const EloqFieldType &field_type,
                                               bool is_key_field)
{
  switch (field_type.data_type_)
  {
  case EloqDataType::Integer: {
    switch (field_type.len_)
    {
    case 1: {
      int8_t val;
      cass_value_get_int8(cass_val, &val);

      size_t before_head= buf.size();
      buf.resize(buf.size() + field_type.len_, 0);
      store_lowendian(val, (uchar *) buf.data() + before_head,
                      field_type.len_);
      break;
    }
    case 2: {
      int16_t val;
      cass_value_get_int16(cass_val, &val);

      size_t before_head= buf.size();
      buf.resize(buf.size() + field_type.len_, 0);
      store_lowendian(val, (uchar *) buf.data() + before_head,
                      field_type.len_);
      break;
    }
    case 3:
    case 4: {
      int32_t val;
      cass_value_get_int32(cass_val, &val);

      size_t before_head= buf.size();
      buf.resize(buf.size() + field_type.len_, 0);
      store_lowendian(val, (uchar *) buf.data() + before_head,
                      field_type.len_);
      break;
    }
    case 8: {
      int64_t val;
      cass_value_get_int64(cass_val, &val);

      size_t before_head= buf.size();
      buf.resize(buf.size() + field_type.len_, 0);
      store_lowendian(val, (uchar *) buf.data() + before_head,
                      field_type.len_);
      break;
    }
    default:
      break;
    }

    break;
  }
  case EloqDataType::UnsignedInteger: {
    switch (field_type.len_)
    {
    case 1: {
      int16_t val;
      cass_value_get_int16(cass_val, &val);
      uint8_t u_val= val;

      size_t before_head= buf.size();
      buf.resize(buf.size() + field_type.len_, 0);
      store_lowendian(u_val, (uchar *) buf.data() + before_head,
                      field_type.len_);

      break;
    }
    case 2: {
      int32_t val;
      cass_value_get_int32(cass_val, &val);
      uint16_t u_val= val;

      size_t before_head= buf.size();
      buf.resize(buf.size() + field_type.len_, 0);
      store_lowendian(u_val, (uchar *) buf.data() + before_head,
                      field_type.len_);
      break;
    }
    case 3: {
      int32_t val;
      cass_value_get_int32(cass_val, &val);
      uint32_t u_val= val;

      size_t before_head= buf.size();
      buf.resize(buf.size() + field_type.len_, 0);
      store_lowendian(u_val, (uchar *) buf.data() + before_head,
                      field_type.len_);
      break;
    }
    case 4: {
      int64_t val;
      cass_value_get_int64(cass_val, &val);
      uint32_t u_val= val;

      size_t before_head= buf.size();
      buf.resize(buf.size() + field_type.len_, 0);
      store_lowendian(u_val, (uchar *) buf.data() + before_head,
                      field_type.len_);
      break;
    }
    default:

      const unsigned char *cass_decimal_buf;
      size_t cass_buf_len;
      int32_t scale;
      cass_value_get_decimal(cass_val, &cass_decimal_buf, &cass_buf_len,
                             &scale);
      EloqDS::BigNumber bn(cass_decimal_buf, cass_buf_len, scale);
      std::string decimal_str= bn.str();
      uint64_t int_val= std::stoull(decimal_str);

      size_t before_head= buf.size();
      buf.resize(buf.size() + field_type.len_, 0);
      store_lowendian(int_val, (uchar *) buf.data() + before_head,
                      field_type.len_);
      break;
    }

    break;
  }
  case EloqDataType::Float: {
    if (field_type.len_ == 8)
    {
      double d_val;
      cass_value_get_double(cass_val, &d_val);
      EncodeDouble(d_val, buf);
    }
    else
    {
      float f_val;
      cass_value_get_float(cass_val, &f_val);
      EncodeFloat(f_val, buf);
    }
    break;
  }
  case EloqDataType::VarString: {
    const char *str_ptr= nullptr;
    size_t str_len= 0;
    cass_value_get_string(cass_val, &str_ptr, &str_len);

    if (is_key_field)
    {
      size_t before_tail= buf.size();
      // When the string field is a key field, "len" represents the string
      // length. And "len" is always encoded in a 2-byte integer.
      buf.resize(buf.size() + 2 + field_type.len_, 0);
      store_lowendian(str_len, (uchar *) buf.data() + before_tail, 2);
      memcpy(buf.data() + before_tail + 2, str_ptr, str_len);
    }
    else
    {
      size_t before_tail= buf.size();
      // When the string field is a non-key field, "len" represents the number
      // of bytes to encode the string length.
      buf.resize(buf.size() + field_type.len_ + str_len);
      store_lowendian(str_len, (uchar *) buf.data() + before_tail,
                      field_type.len_);
      memcpy(buf.data() + before_tail + field_type.len_, str_ptr, str_len);
    }
    break;
  }
  case EloqDataType::VarBinary: {
    const uint8_t *blob_ptr= nullptr;
    size_t blob_len= 0;
    cass_value_get_bytes(cass_val, &blob_ptr, &blob_len);

    uint8_t len_bytes= field_type.len_ <= 2 ? field_type.len_ : 4;
    EncodeUnsignedInteger(blob_len, buf, len_bytes);
    buf.insert(buf.end(), blob_ptr, blob_ptr + blob_len);

    break;
  }
  case EloqDataType::String: {
    const char *str_ptr= nullptr;
    size_t str_len= 0;
    cass_value_get_string(cass_val, &str_ptr, &str_len);

    assert(str_len == field_type.len_);

    buf.insert(buf.end(), str_ptr, str_ptr + str_len);

    break;
  }
  case EloqDataType::Binary: {
    const uint8_t *blob_ptr= nullptr;
    size_t blob_len= 0;
    cass_value_get_bytes(cass_val, &blob_ptr, &blob_len);

    assert(blob_len == field_type.len_);

    buf.insert(buf.end(), blob_ptr, blob_ptr + blob_len);
    break;
  }
  case EloqDataType::Date: {
    // Cass returns a DATE field as an unsigned 32-bit integer to represent
    // the number of days since Epoch centered at 2^31.
    uint32_t cass_days;
    cass_value_get_uint32(cass_val, &cass_days);

    int32_t num_of_days=
        cass_days >= EloqFieldType::CassDateEpochCenter
            ? (cass_days - EloqFieldType::CassDateEpochCenter)
            : -(EloqFieldType::CassDateEpochCenter - cass_days);

    using namespace std::chrono;

    date::sys_days sys_d{date::days{num_of_days}};
    date::year_month_day dp(sys_d);

    int year= int{dp.year()};
    int month= unsigned{dp.month()};
    int day= unsigned{dp.day()};

    size_t begin= buf.size();
    buf.resize(buf.size() + field_type.len_);

    // The following code is copied from Field_newdate::store_TIME().
    uint tmp= year * 16 * 32 + month * 32 + day;
    int3store((uchar *) (buf.data() + begin), tmp);

    break;
  }
  case EloqDataType::Time: {
    int64_t elapse_nano_sec= 0;
    // Cassandra's time type represents the nano seconds since the mid night.
    cass_value_get_int64(cass_val, &elapse_nano_sec);
    int64_t elapse_micro_sec= elapse_nano_sec / 1000;
    EncodeTime(elapse_micro_sec, field_type.len_, buf);
    break;
  }
  case EloqDataType::Timestamp: {
    int64_t elapse_micro_sec= 0;
    cass_value_get_int64(cass_val, &elapse_micro_sec);
    mysql::timeval tm;
    tm.tv_sec= elapse_micro_sec / 1000000;
    tm.tv_usec= elapse_micro_sec % 1000000;
    uint8_t storage_len= mysql::my_timestamp_binary_length(field_type.len_);
    size_t begin= buf.size();
    buf.resize(buf.size() + storage_len);
    mysql::my_timestamp_to_binary(&tm, (uchar *) (buf.data() + begin),
                                  field_type.len_);
    break;
  }
  case EloqDataType::Datetime: {
    int64_t elapse= 0;
    cass_value_get_int64(cass_val, &elapse);
    EncodeDatetime(elapse, field_type.len_, buf);
    break;
  }
  case EloqDataType::Decimal: {
    const unsigned char *cass_decimal_buf;
    size_t cass_buf_len;
    int32_t scale;
    cass_value_get_decimal(cass_val, &cass_decimal_buf, &cass_buf_len, &scale);
    EloqDS::BigNumber bn(cass_decimal_buf, cass_buf_len, scale);
    std::string decimal_str= bn.str();

    uint8_t precision= field_type.len_ >> 8;
    my_decimal mysql_decimal;
    mysql_decimal.intg= precision - scale;
    mysql_decimal.frac= scale;
    char *end= decimal_str.data() + decimal_str.size() - 1;
    while (*end == '\0')
    {
      --end;
    }
    ++end;
    mysql::internal_str2dec(decimal_str.data(), &mysql_decimal, &end, true);

    assert(scale == (field_type.len_ & 0xFF));
    uint8_t buf_len= mysql::decimal_bin_size(precision, scale);
    size_t begin= buf.size();
    buf.resize(buf.size() + buf_len);

    mysql::decimal2bin(&mysql_decimal, (uchar *) (buf.data() + begin),
                       precision, scale);

    break;
  }
  default:
    break;
  }
}
#endif

void MyEloq::EloqRecordSchema::EncodePlainValue(
    const std::string &plain_val, std::vector<char> &buf,
    const EloqFieldType &field_type, bool is_key_field)
{
  switch (field_type.data_type_)
  {
  case EloqDataType::Integer: {
    switch (field_type.len_)
    {
    case 1: {
      int8_t val= static_cast<int8_t>(std::stoi(plain_val));
      EncodeInteger(val, buf);
      break;
    }
    case 2: {
      int16_t val= static_cast<int16_t>(std::stoi(plain_val));
      EncodeInteger(val, buf);
      break;
    }
    case 4: {
      int32_t val= static_cast<int32_t>(std::stoi(plain_val));
      EncodeInteger(val, buf);
      break;
    }
    case 8: {
      int64_t val= static_cast<int64_t>(std::stol(plain_val));
      EncodeInteger(val, buf);
      break;
    }
    default:
      break;
    }

    break;
  }
  case EloqDataType::UnsignedInteger: {
    switch (field_type.len_)
    {
    case 1: {
      uint8_t u_val= static_cast<uint8_t>(std::stoul(plain_val));
      EncodeUnsignedInteger(u_val, buf, 1);
      break;
    }
    case 2: {
      uint16_t u_val= static_cast<uint16_t>(std::stoul(plain_val));
      EncodeUnsignedInteger(u_val, buf, 2);
      break;
    }
    case 4: {
      uint32_t u_val= static_cast<uint32_t>(std::stoul(plain_val));
      EncodeUnsignedInteger(u_val, buf, 4);
      break;
    }
    default:
      uint64_t int_val= static_cast<uint32_t>(std::stoul(plain_val));
      EncodeUnsignedInteger(int_val, buf, 8);
      break;
    }

    break;
  }
  case EloqDataType::Float: {
    if (field_type.len_ == 8)
    {
      double d_val= static_cast<double>(std::stod(plain_val));
      EncodeDouble(d_val, buf);
    }
    else
    {
      float f_val= static_cast<float>(std::stof(plain_val));
      EncodeFloat(f_val, buf);
    }
    break;
  }
  case EloqDataType::VarString: {
    // const char *str_ptr= plain_val.data();
    size_t str_len= plain_val.length();
    // cass_value_get_string(cass_val, &str_ptr, &str_len);

    if (is_key_field)
    {
      EncodeUnsignedInteger(str_len, buf, 2);
      buf.insert(buf.end(), plain_val.cbegin(), plain_val.cend());
      if (field_type.len_ > str_len)
      {
        size_t before_tail= buf.size();
        buf.resize(buf.size() + field_type.len_ - str_len);
        std::fill(buf.begin() + before_tail, buf.end(), 0);
      }
    }
    else
    {
      EncodeUnsignedInteger(str_len, buf, field_type.len_);
      buf.insert(buf.end(), plain_val.cbegin(), plain_val.cend());
    }
    break;
  }
  case EloqDataType::VarBinary: {
    size_t blob_len= plain_val.length();
    uint8_t len_bytes= field_type.len_ <= 2 ? field_type.len_ : 4;
    EncodeUnsignedInteger(blob_len, buf, len_bytes);
    buf.insert(buf.end(), plain_val.cbegin(), plain_val.cend());

    break;
  }
  case EloqDataType::String:
  case EloqDataType::Binary: {
    size_t str_len= plain_val.length();
    assert(str_len == field_type.len_);
    buf.insert(buf.end(), plain_val.cbegin(), plain_val.cend());

    break;
  }
  case EloqDataType::Date: {
    // Cass returns a DATE field as an unsigned 32-bit integer to
    // represent
    // the number of days since Epoch centered at 2^31.
    uint32_t cass_days= static_cast<uint32_t>(std::stoul(plain_val));

    int32_t num_of_days=
        cass_days >= EloqFieldType::CassDateEpochCenter
            ? (cass_days - EloqFieldType::CassDateEpochCenter)
            : -(EloqFieldType::CassDateEpochCenter - cass_days);

    using namespace std::chrono;

    date::sys_days sys_d{date::days{num_of_days}};
    date::year_month_day dp(sys_d);

    int year= int{dp.year()};
    int month= unsigned{dp.month()};
    int day= unsigned{dp.day()};

    size_t begin= buf.size();
    buf.resize(buf.size() + field_type.len_);

    // The following code is copied from Field_newdate::store_TIME().
    uint tmp= year * 16 * 32 + month * 32 + day;
    int3store((uchar *) (buf.data() + begin), tmp);

    break;
  }
  case EloqDataType::Time: {
    int64_t elapse_nano_sec= 0;
    elapse_nano_sec= static_cast<int64_t>(std::stol(plain_val));
    int64_t elapse_micro_sec= elapse_nano_sec / 1000;
    EncodeTime(elapse_micro_sec, field_type.len_, buf);
    break;
  }
  case EloqDataType::Timestamp: {
    int64_t elapse_micro_sec= 0;
    elapse_micro_sec= static_cast<int64_t>(std::stol(plain_val));
    mysql::timeval tm;
    tm.tv_sec= elapse_micro_sec / 1000000;
    tm.tv_usec= elapse_micro_sec % 1000000;
    uint8_t storage_len= mysql::my_timestamp_binary_length(field_type.len_);
    size_t begin= buf.size();
    buf.resize(buf.size() + storage_len);
    mysql::my_timestamp_to_binary(&tm, (uchar *) (buf.data() + begin),
                                  field_type.len_);
    break;
  }
  case EloqDataType::Datetime: {
    int64_t elapse= 0;
    elapse= static_cast<int64_t>(std::stol(plain_val));
    EncodeDatetime(elapse, field_type.len_, buf);
    break;
  }
  case EloqDataType::Decimal: {
    // plain value not support 'Decimal'

    break;
  }
  default:
    break;
  }
}

void MyEloq::EloqRecordSchema::UpdateOffset(const std::vector<char> &rec_buf,
                                            size_t &offset,
                                            const EloqFieldType &field_type,
                                            bool is_key_field)
{
  switch (field_type.data_type_)
  {
  case EloqDataType::Integer: {
    offset+= field_type.len_;
    break;
  }
  case EloqDataType::UnsignedInteger: {
    offset+= field_type.len_;
    break;
  }
  case EloqDataType::Float: {
    offset+= field_type.len_;
    break;
  }
  case EloqDataType::VarString: {
    if (is_key_field)
    {
      offset+= 2;
      offset+= field_type.len_;
    }
    else
    {
      uint32_t str_len=
          read_lowendian((uchar *) rec_buf.data() + offset, field_type.len_);
      offset+= field_type.len_;
      offset+= str_len;
    }
    break;
  }
  case EloqDataType::VarBinary: {
    uint64_t blob_len;
    uint8_t len_bytes= field_type.len_ <= 2 ? field_type.len_ : 4;
    DecodeUnsignedInteger(blob_len, rec_buf, offset, len_bytes);
    offset+= blob_len;
    break;
  }
  case EloqDataType::String: {
    offset+= field_type.len_;
    break;
  }
  case EloqDataType::Date: {
    offset+= field_type.len_;
    break;
  }
  case EloqDataType::Time: {
    offset+= mysql::my_time_binary_length(field_type.len_);
    break;
  }
  case EloqDataType::Timestamp: {
    offset+= mysql::my_timestamp_binary_length(field_type.len_);
    break;
  }
  case EloqDataType::Datetime: {
    offset+= mysql::my_datetime_binary_length(field_type.len_);
    break;
  }
  case EloqDataType::Decimal: {
    my_decimal mysql_decimal;
    uint8_t precision= field_type.len_ >> 8;
    uint8_t scale= field_type.len_ & 0xFF;

    uint8_t buf_len= decimal_bin_size(precision, scale);
    offset+= buf_len;
    break;
  }
  case EloqDataType::Binary: {
    offset+= field_type.len_;
    break;
  }
  default:
    break;
  }
}

#if defined(DATA_STORE_TYPE_CASSANDRA)
void MyEloq::EloqRecordSchema::BindCassStatement(
    const std::vector<char> &rec_buf, size_t &offset,
    const EloqFieldType &field_type, bool is_key_field, CassStatement *statem,
    size_t para_idx)
{
  // TODO(lzx): check whether this function is used. If not, delete it.
  assert(false);

  switch (field_type.data_type_)
  {
  case EloqDataType::Integer: {
    switch (field_type.len_)
    {
    case 1: {
      int8_t val=
          read_lowendian((uchar *) rec_buf.data() + offset, field_type.len_);
      cass_statement_bind_int8(statem, para_idx, val);
      break;
    }
    case 2: {
      int16_t val=
          read_lowendian((uchar *) rec_buf.data() + offset, field_type.len_);
      cass_statement_bind_int16(statem, para_idx, val);
      break;
    }
    case 3:
    case 4: {
      int32_t val=
          read_lowendian((uchar *) rec_buf.data() + offset, field_type.len_);
      cass_statement_bind_int32(statem, para_idx, val);
      break;
    }
    case 8: {
      int64_t val=
          read_lowendian((uchar *) rec_buf.data() + offset, field_type.len_);
      cass_statement_bind_int64(statem, para_idx, val);
      break;
    }
    default:
      break;
    }

    offset+= field_type.len_;
    break;
  }
  case EloqDataType::UnsignedInteger: {
    switch (field_type.len_)
    {
    case 1: {
      int16_t val=
          read_lowendian((uchar *) rec_buf.data() + offset, field_type.len_);
      cass_statement_bind_int16(statem, para_idx, val);
      break;
    }
    case 2:
    case 3: {
      int32_t val=
          read_lowendian((uchar *) rec_buf.data() + offset, field_type.len_);
      cass_statement_bind_int32(statem, para_idx, val);
      break;
    }
    case 4: {
      int64_t val=
          read_lowendian((uchar *) rec_buf.data() + offset, field_type.len_);
      cass_statement_bind_int64(statem, para_idx, val);
      break;
    }
    default:
      uint64_t val=
          read_lowendian((uchar *) rec_buf.data() + offset, field_type.len_);

      std::string int_str= std::to_string(val);
      EloqDS::BigNumber bn(int_str);
      std::vector<unsigned char> encoded_vec= bn.encode_varint();

      cass_statement_bind_decimal(statem, para_idx, encoded_vec.data(),
                                  encoded_vec.size(), 0);
      break;
    }

    offset+= field_type.len_;
    break;
  }
  case EloqDataType::Float: {
    if (field_type.len_ == 8)
    {
      double d_val;
      DecodeDouble(d_val, rec_buf, offset);
      cass_statement_bind_double(statem, para_idx, d_val);
    }
    else
    {
      float f_val;
      DecodeFloat(f_val, rec_buf, offset);
      cass_statement_bind_float(statem, para_idx, f_val);
    }
    break;
  }
  case EloqDataType::VarString: {
    if (is_key_field)
    {
      uint16_t str_len= read_lowendian((uchar *) rec_buf.data() + offset, 2);
      offset+= 2;
      cass_statement_bind_string_n(statem, para_idx, rec_buf.data() + offset,
                                   str_len);
      offset+= field_type.len_;
    }
    else
    {
      uint32_t str_len=
          read_lowendian((uchar *) rec_buf.data() + offset, field_type.len_);
      offset+= field_type.len_;
      cass_statement_bind_string_n(statem, para_idx, rec_buf.data() + offset,
                                   str_len);
      offset+= str_len;
    }
    break;
  }
  case EloqDataType::VarBinary: {
    if (is_key_field)
    {
      uint64_t blob_len;
      DecodeUnsignedInteger(blob_len, rec_buf, offset, 2);
      const uint8_t *content=
          reinterpret_cast<const uint8_t *>(rec_buf.data());
      cass_statement_bind_bytes(statem, para_idx, content + offset, blob_len);
      offset+= blob_len;
    }
    else
    {
      uint64_t blob_len;
      uint8_t len_bytes= field_type.len_ <= 2 ? field_type.len_ : 4;
      DecodeUnsignedInteger(blob_len, rec_buf, offset, len_bytes);
      const uint8_t *content=
          reinterpret_cast<const uint8_t *>(rec_buf.data());
      cass_statement_bind_bytes(statem, para_idx, content + offset, blob_len);
      offset+= blob_len;
    }
    break;
  }
  case EloqDataType::String: {
    cass_statement_bind_string_n(statem, para_idx, rec_buf.data() + offset,
                                 field_type.len_);
    offset+= field_type.len_;
    break;
  }
  case EloqDataType::Date: {
    uchar *ptr= (uchar *) (rec_buf.data() + offset);
    offset+= field_type.len_;

    // The following code is copied from Field_newdate::get_TIME.
    uint32 tmp= (uint32) uint3korr(ptr);
    uint16_t day= tmp & 31;
    uint16_t month= (tmp >> 5) & 15;
    uint16_t year= (tmp >> 9);

    date::year_month_day day_point= date::year{year} / month / day;
    // Cassandra's date type uses uint32_t to represent the number of days with
    // Epoch centered at 2^31.
    uint32_t days_encoding=
        EloqFieldType::CassDateEpochCenter +
        date::sys_days{day_point}.time_since_epoch().count();

    cass_statement_bind_uint32(statem, para_idx, days_encoding);
    break;
  }
  case EloqDataType::Time: {
    int64_t micro_sec;
    DecodeTime(micro_sec, field_type.len_, rec_buf, offset);
    int64_t nano_sec= micro_sec * 1000;
    cass_statement_bind_int64(statem, para_idx, nano_sec);
    break;
  }
  case EloqDataType::Timestamp: {
    mysql::timeval tm;
    my_timestamp_from_binary(&tm, (const uchar *) (rec_buf.data() + offset),
                             field_type.len_);
    int64_t micro_sec= tm.tv_sec * 1000000 + tm.tv_usec;
    cass_statement_bind_int64(statem, para_idx, micro_sec);
    uint8_t storage_len= mysql::my_timestamp_binary_length(field_type.len_);
    offset+= storage_len;
    break;
  }
  case EloqDataType::Datetime: {
    int64_t micro_sec;
    DecodeDatetime(micro_sec, field_type.len_, rec_buf, offset);
    cass_statement_bind_int64(statem, para_idx, micro_sec);
    break;
  }
  case EloqDataType::Decimal: {
    my_decimal mysql_decimal;
    uint8_t precision= field_type.len_ >> 8;
    uint8_t scale= field_type.len_ & 0xFF;

    bin2decimal((uchar *) (rec_buf.data() + offset), &mysql_decimal, precision,
                scale);

    size_t decimal_len= my_decimal_string_length(&mysql_decimal);
    std::string decimal_str;
    decimal_str.resize(decimal_len, '\0');
    int out_len= decimal_str.size();
    decimal2string(&mysql_decimal, decimal_str.data(), &out_len, 0, scale,
                   '\0');

    if ((size_t) out_len < decimal_str.size())
    {
      decimal_str= decimal_str.substr(0, out_len);
    }

    EloqDS::BigNumber bn(decimal_str);
    std::vector<unsigned char> encoded_vec= bn.encode_varint();

    cass_statement_bind_decimal(statem, para_idx, encoded_vec.data(),
                                encoded_vec.size(), scale);

    uint8_t buf_len= decimal_bin_size(precision, scale);
    offset+= buf_len;
    break;
  }
  case EloqDataType::Binary: {
    cass_statement_bind_bytes(statem, para_idx,
                              (unsigned char *) (rec_buf.data() + offset),
                              field_type.len_);
    offset+= field_type.len_;
    break;
  }
  default:
    break;
  }
}

void MyEloq::EloqRecordSchema::BindCassStatement(
    const std::vector<char> &rec_buf, const EloqRecordSchema *rec_schema,
    CassStatement *statem)
{
  // Field data starts after the bitmap header, which indicates whether or not
  // a field is null.
  size_t rec_buf_offset= rec_schema->BitmapBytes();
  for (size_t fidx= 0, non_pk_offset= 0;
       fidx < rec_schema->field_types_.size(); ++fidx)
  {
    if (rec_schema->is_part_of_pk_[fidx])
    {
      continue;
    }

    const char &bitmap_byte= rec_buf[non_pk_offset >> 3];
    uint8_t bit_offset= non_pk_offset & 7;
    bool is_null= bitmap_byte & (1 << bit_offset);

    if (is_null)
    {
      cass_statement_bind_null(statem, fidx);
    }
    else
    {
      EloqRecordSchema::BindCassStatement(rec_buf, rec_buf_offset,
                                          rec_schema->field_types_[fidx],
                                          false, statem, non_pk_offset);
    }
    ++non_pk_offset;
  }
}
#endif

/*
  Returns true if given index number is a hidden_pk.
  - This is used when a table is created with no primary key.
*/
bool MyEloq::EloqRecordSchema::IsHiddenPk(const uint index,
                                          const TABLE_SHARE *const table)
{
  DBUG_ASSERT(table != nullptr);

  return (table->primary_key == MAX_INDEXES && index == table->keys);
}

/*
  Create key definition needed for storing data in kv storage.
  This can be called either during CREATE table or doing ADD index operations.

  @param in
    table         Table with definition
    i             Position of index being created inside table_arg->key_info

  @return
    0      - Ok
    other  - error, either given table ddl is not supported by eloq or
  OOM.
*/
int MyEloq::EloqKeySchema::CreateKeyDefinition(const TABLE_SHARE *const table,
                                               const uint i)
{
  DBUG_ENTER_FUNC();

  uchar index_type;

  if (EloqRecordSchema::IsHiddenPk(i, table))
  {
    index_type= mono_key_def::INDEX_TYPE_HIDDEN_PRIMARY;
  }
  else if (i == table->primary_key)
  {
    index_type= mono_key_def::INDEX_TYPE_PRIMARY;
  }
  else
  {
    index_type= mono_key_def::INDEX_TYPE_SECONDARY;
  }

  key_def_= std::make_unique<mono_key_def>(i, index_type);

  // initialize key_def
  key_def_->setup(table);
  DBUG_RETURN(0);
}

MyEloq::EloqKeySchema::EloqKeySchema(const TABLE_SHARE *table_share,
                                     uint key_no, uint64_t key_version)
{
  this->CreateKeyDefinition(table_share, key_no);

  schema_ts_= key_version;

  if (this->GetIndexType() == mono_key_def::INDEX_TYPE_HIDDEN_PRIMARY)
  {
    key_info_= nullptr;
    return;
  }
  key_info_= &table_share->key_info[key_no];

  for (size_t key_part_idx= 0;
       key_part_idx < key_info_->user_defined_key_parts; ++key_part_idx)
  {
    const mysql::KEY_PART_INFO &key_part= key_info_->key_part[key_part_idx];
    key_part_types_.emplace_back(
        EloqFieldType::Convert(key_part.field, key_part.length));
  }
}

#if defined(DATA_STORE_TYPE_DYNAMODB)
void MyEloq::EloqRecordSchema::BindDynamoAttribute(
    const std::vector<char> &rec_buf, size_t &offset,
    const EloqFieldType &field_type, Aws::DynamoDB::Model::AttributeValue &att)
{
  switch (field_type.data_type_)
  {
  case EloqDataType::Integer: {

    switch (field_type.len_)
    {
    case 1:
    case 2:
    case 3:
    case 4: {
      int32_t val= (int32_t) read_lowendian((uchar *) rec_buf.data() + offset,
                                            field_type.len_);
      att.SetN(val);
      break;
    }
    case 8: {
      int64_t val= (int64_t) read_lowendian((uchar *) rec_buf.data() + offset,
                                            field_type.len_);
      att.SetN(std::to_string(val));
      break;
    }
    default:
      break;
    }

    offset+= field_type.len_;
    break;
  }
  case EloqDataType::UnsignedInteger: {

    uint64_t val=
        read_lowendian((uchar *) rec_buf.data() + offset, field_type.len_);

    att.SetN(std::to_string(val));
    offset+= field_type.len_;
    break;
  }
  case EloqDataType::Float: {
    if (field_type.len_ == 8)
    {
      double d_val;
      DecodeDouble(d_val, rec_buf, offset);
      att.SetN(d_val);
    }
    else
    {
      float f_val;
      DecodeFloat(f_val, rec_buf, offset);
      att.SetN(f_val);
    }
    break;
  }
  case EloqDataType::VarString: {
    uint32_t str_len=
        read_lowendian((uchar *) rec_buf.data() + offset, field_type.len_);
    offset+= field_type.len_;
    att.SetS(std::string(rec_buf.data() + offset, str_len));
    offset+= str_len;
    break;
  }
  case EloqDataType::VarBinary: {
    uint64_t blob_len;
    uint8_t len_bytes= field_type.len_ <= 2 ? field_type.len_ : 4;
    DecodeUnsignedInteger(blob_len, rec_buf, offset, len_bytes);
    const unsigned char *content=
        reinterpret_cast<const unsigned char *>(rec_buf.data());
    att.SetB(Aws::Utils::ByteBuffer(content + offset, blob_len));
    offset+= blob_len;
    break;
  }
  case EloqDataType::String: {
    att.SetS(std::string(rec_buf.data() + offset, field_type.len_));
    offset+= field_type.len_;
    break;
  }
  case EloqDataType::Date: {
    uchar *ptr= (uchar *) (rec_buf.data() + offset);
    offset+= field_type.len_;

    // The following code is copied from Field_newdate::get_TIME.
    uint32 tmp= (uint32) uint3korr(ptr);
    uint16_t day= tmp & 31;
    uint16_t month= (tmp >> 5) & 15;
    uint16_t year= (tmp >> 9);

    date::year_month_day day_point= date::year{year} / month / day;

    // Corresponding location: EncodeDynamoValue()
    uint32_t days_encoding=
        EloqFieldType::CassDateEpochCenter +
        date::sys_days{day_point}.time_since_epoch().count();

    att.SetN(std::to_string(days_encoding));
    break;
  }
  case EloqDataType::Time: {
    int64_t micro_sec;
    DecodeTime(micro_sec, field_type.len_, rec_buf, offset);
    int64_t nano_sec= micro_sec * 1000;
    att.SetN(std::to_string(nano_sec));
    break;
  }
  case EloqDataType::Timestamp: {
    mysql::timeval tm;
    my_timestamp_from_binary(&tm, (const uchar *) (rec_buf.data() + offset),
                             field_type.len_);
    int64_t micro_sec= tm.tv_sec * 1000000 + tm.tv_usec;
    att.SetN(std::to_string(micro_sec));
    uint8_t storage_len= mysql::my_timestamp_binary_length(field_type.len_);
    offset+= storage_len;
    break;
  }
  case EloqDataType::Datetime: {
    int64_t micro_sec;
    DecodeDatetime(micro_sec, field_type.len_, rec_buf, offset);
    att.SetN(std::to_string(micro_sec));
    break;
  }
  case EloqDataType::Decimal: {
    my_decimal mysql_decimal;
    uint8_t precision= field_type.len_ >> 8;
    uint8_t scale= field_type.len_ & 0xFF;

    bin2decimal((uchar *) (rec_buf.data() + offset), &mysql_decimal, precision,
                scale);

    size_t decimal_len= my_decimal_string_length(&mysql_decimal);
    std::string decimal_str;
    decimal_str.resize(decimal_len, '\0');
    int out_len= decimal_str.size();
    decimal2string(&mysql_decimal, decimal_str.data(), &out_len, 0, scale,
                   '\0');

    if ((size_t) out_len < decimal_str.size())
    {
      decimal_str= decimal_str.substr(0, out_len);
    }

    att.SetN(decimal_str);

    uint8_t buf_len= decimal_bin_size(precision, scale);
    offset+= buf_len;
    break;
  }
  case EloqDataType::Binary: {
    att.SetB(Aws::Utils::ByteBuffer(
        reinterpret_cast<const unsigned char *>(rec_buf.data() + offset),
        field_type.len_));
    offset+= field_type.len_;
    break;
  }
  default:
    break;
  }
}

void MyEloq::EloqRecordSchema::BindDynamoRequest(
    const std::vector<char> &rec_buf,
    Aws::DynamoDB::Model::PutRequest *req) const
{

  // Field data starts after the bitmap header, which indicates whether or not
  // a field is null.
  size_t rec_buf_offset= this->BitmapBytes();
  for (size_t fidx= 0, non_pk_offset= 0; fidx < field_types_.size(); ++fidx)
  {
    if (is_part_of_pk_[fidx])
    {
      continue;
    }

    const char &bitmap_byte= rec_buf[non_pk_offset >> 3];
    uint8_t bit_offset= non_pk_offset & 7;
    bool is_null= bitmap_byte & (1 << bit_offset);
    const EloqFieldType field= field_types_[fidx];
    Aws::DynamoDB::Model::AttributeValue att;

    if (is_null)
    {
      att.SetNull(true);
    }
    else
    {
      EloqRecordSchema::BindDynamoAttribute(rec_buf, rec_buf_offset, field,
                                            att);
    }
    req->AddItem(field.field_name_, att);
    ++non_pk_offset;
  }
}
#endif

#if defined(DATA_STORE_TYPE_CASSANDRA)
void MyEloq::EloqKeySchema::EncodeFromBaseTable(const CassRow *row,
                                                std::vector<char> &buf) const
{
  buf.clear();
  buf.reserve(key_info_->key_length);

  // key_part_idx is the field position of a given primary key (loop through
  // all columns).
  for (size_t key_part_idx= 0; key_part_idx < key_part_types_.size();
       ++key_part_idx)
  {
    const mysql::KEY_PART_INFO &key_part= key_info_->key_part[key_part_idx];
    // field_idx is the field position of mysql create table stmt
    size_t field_idx= key_part.fieldnr - 1;
    const EloqFieldType &field_type= key_part_types_.at(key_part_idx);

    const CassValue *cass_val= cass_row_get_column(row, field_idx);

    if (key_part.null_bit)
    {
      if (cass_value_is_null(cass_val))
      {
        buf.emplace_back(1);
        continue;
      }
      else
      {
        buf.emplace_back(0);
      }
    }

    EloqRecordSchema::EncodeCassValue(cass_val, buf, field_type, true);
  }
}

void MyEloq::EloqKeySchema::EncodeFromIndexTable(const CassRow *row,
                                                 std::vector<char> &buf,
                                                 size_t offset) const
{
  buf.clear();
  buf.reserve(key_info_->key_length);

  // key_part_idx is the field position of a given secondary index (loop
  // through all columns).
  for (size_t key_part_idx= 0; key_part_idx < key_part_types_.size();
       ++key_part_idx)
  {
    const mysql::KEY_PART_INFO &key_part= key_info_->key_part[key_part_idx];
    const EloqFieldType &field_type= key_part_types_.at(key_part_idx);

    const CassValue *cass_val= cass_row_get_column(row, key_part_idx + offset);

    if (key_part.null_bit)
    {
      if (cass_value_is_null(cass_val))
      {
        buf.emplace_back(1);
        continue;
      }
      else
      {
        buf.emplace_back(0);
      }
    }

    EloqRecordSchema::EncodeCassValue(cass_val, buf, field_type, true);
  }
}
#endif

void MyEloq::EloqKeySchema::EncodeKey(const std::vector<std::string> &key_cols,
                                      std::vector<char> &buf) const
{
  buf.clear();
  buf.reserve(key_info_->key_length);

  for (size_t key_part_idx= 0; key_part_idx < key_part_types_.size();
       ++key_part_idx)
  {
    const mysql::KEY_PART_INFO &key_part= key_info_->key_part[key_part_idx];
    const EloqFieldType &field_type= key_part_types_.at(key_part_idx);

    if (key_part.null_bit)
    {
      if (key_cols.size() == 0)
      {
        buf.emplace_back(1);
        continue;
      }
      else
      {
        buf.emplace_back(0);
      }
    }

    EloqRecordSchema::EncodePlainValue(key_cols[key_part_idx], buf, field_type,
                                       true);
  }
}

void MyEloq::EloqKeySchema::SearchCondition(std::string &cql) const
{
  for (const EloqFieldType &field_type : key_part_types_)
  {
    cql.append(" AND \"");
    cql.append(field_type.field_name_);
    cql.append("\"=?");
  }
}

void MyEloq::EloqKeySchema::ScanForwardCondition(std::string &cql,
                                                 bool inclusive,
                                                 bool scan_foward,
                                                 uint8_t key_parts) const
{
  uint8_t key_cnt= std::min((uint8_t) key_part_types_.size(), key_parts);

  for (uint8_t fidx= 0; fidx < key_cnt; ++fidx)
  {
    cql.append(" AND \"");
    cql.append(key_part_types_.at(fidx).field_name_);
    cql.append("\"");

    if (inclusive || (key_cnt > 1 && fidx != key_cnt - 1))
    {
      if (scan_foward)
      {
        cql.append(">=?");
      }
      else
      {
        cql.append("<=?");
      }
    }
    else
    {
      if (scan_foward)
      {
        cql.append(">?");
      }
      else
      {
        cql.append("<?");
      }
    }
  }
}

uchar MyEloq::EloqKeySchema::GetIndexType() const
{
  return key_def_->m_index_type;
}

bool MyEloq::EloqKeySchema::IsUniqueIndex() const
{
  return key_info_->flags & HA_NOSAME;
}

void MyEloq::EloqKeySchema::ScanSliceCondition(std::string &cql,
                                               uint8_t key_parts) const
{
  for (uint8_t fidx= 0; fidx < key_parts; ++fidx)
  {
    cql.append(" AND \"");
    cql.append(key_part_types_.at(fidx).field_name_);
    cql.append("\"");

    if (fidx == key_parts - 1)
    {
      if (key_parts == ColumnCount())
      {
        cql.append(">=?");
      }
      else
      {
        cql.append(">?");
      }
    }
    else
    {
      cql.append("=?");
    }
  }
}

std::vector<std::pair<std::string, int8_t>>
MyEloq::EloqKeySchema::ScanStartFromConditions(std::string &orig_cql,
                                               bool inclusive,
                                               bool more_or_less_than,
                                               uint8_t key_parts) const
{
  const std::regex match_where_at_end("[ \t]*[where|WHERE][ \t]*$");
  bool has_preced_condition= !std::regex_match(orig_cql, match_where_at_end);

  uint8_t key_cnt= std::min((uint8_t) key_part_types_.size(), key_parts);
  std::vector<std::pair<std::string, int8_t>> out;

  for (uint8_t cql_idx= 0; cql_idx < key_cnt; cql_idx++)
  {
    uint8_t col_cnt= key_cnt - cql_idx;
    std::string cql= orig_cql;
    for (uint8_t col_idx= 0; col_idx < col_cnt; col_idx++)
    {
      if (has_preced_condition)
      {
        cql.append(" AND");
      }
      cql.append(" \"");
      cql.append(key_part_types_.at(col_idx).field_name_);
      cql.append("\"");

      if (col_idx < col_cnt - 1)
      {
        cql.append(" = ?");
      }
      else
      {
        if (inclusive && cql_idx == 0)
        {
          more_or_less_than ? cql.append(" >= ?") : cql.append(" <= ?");
        }
        else
        {
          more_or_less_than ? cql.append(" > ?") : cql.append(" < ?");
        }
      }
    }

    out.emplace_back(cql, col_cnt);
  }

  // In case no condition generated
  if (out.size() == 0)
  {
    out.emplace_back(orig_cql, 0);
  }

  return out;
}

void MyEloq::EloqKeySchema::ColumnList(
    std::string &cql, bool trim, const std::vector<size_t> *pk_only_col) const
{
  if (pk_only_col == nullptr)
  {
    for (const EloqFieldType &field_type : key_part_types_)
    {
      cql.append("\"");
      cql.append(field_type.field_name_);
      cql.append("\",");
    }
  }
  else
  {
    for (size_t key_part_idx : *pk_only_col)
    {
      const EloqFieldType &field_type= key_part_types_[key_part_idx];

      cql.append("\"");
      cql.append(field_type.field_name_);
      cql.append("\",");
    }
  }

  if (trim)
  {
    cql.erase(cql.size() - 1);
  }
}

#if defined(DATA_STORE_TYPE_CASSANDRA)
void MyEloq::EloqKeySchema::ColumnListWithType(
    std::string &cql, bool trim, const std::vector<size_t> *pk_only_col) const
{
  if (pk_only_col == nullptr)
  {
    for (const EloqFieldType &field_type : key_part_types_)
    {
      cql.append("\"");
      cql.append(field_type.field_name_);
      cql.append("\" ");
      cql.append(EloqFieldType::GetCassTypeName(field_type));
      cql.append(",");
    }
  }
  else
  {
    for (size_t key_part_idx : *pk_only_col)
    {
      const EloqFieldType &field_type= key_part_types_[key_part_idx];

      cql.append("\"");
      cql.append(field_type.field_name_);
      cql.append("\" ");
      cql.append(EloqFieldType::GetCassTypeName(field_type));
      cql.append(",");
    }
  }

  if (trim)
  {
    cql.erase(cql.size() - 1);
  }
}
#endif

// currently useless
void MyEloq::EloqKeySchema::OrderByList(std::string &cql,
                                        SortOrder order) const
{
  for (size_t idx= 0; idx < ColumnCount(); ++idx)
  {
    if (idx != 0)
    {
      cql.append("\", ");
    }
    cql.append("\"");
    cql.append(FieldType(idx).field_name_);
  }

  if (order == SortOrder::ASC)
  {
    cql.append("\" ASC");
  }
  else
  {
    cql.append("\" DESC");
  }
}

bool MyEloq::EloqKeySchema::CompareKeys(const txservice::TxKey &key1,
                                        const txservice::TxKey &key2,
                                        size_t *const column_index) const
{
  auto to_packed_mono_key= [this](const txservice::TxKey &key,
                                  EloqKey *buf) -> const EloqKey & {
    txservice::KeyType key_type= key.Type();
    if (key_type == txservice::KeyType::NegativeInf)
    {
      return *EloqKey::PackedNegativeInfinity();
    }
    else if (key_type == txservice::KeyType::PositiveInf)
    {
      *buf= EloqKey::PackedPositiveInfinity(this);
      return *buf;
    }
    else
    {
      return *key.GetKey<EloqKey>();
    }
  };

  EloqKey buf1, buf2;
  const EloqKey &mono_key1= to_packed_mono_key(key1, &buf1);
  const EloqKey &mono_key2= to_packed_mono_key(key2, &buf2);

  Slice mono_slice_key1= mono_key1.PackedValueSlice();
  Slice mono_slice_key2= mono_key2.PackedValueSlice();
  int rc=
      key_def_->compare_keys(&mono_slice_key1, &mono_slice_key2, column_index);
  // Actually, rc can't be -1 like comment in compare_keys says.
  // Thus, rc only indicates data formats error or not.
  assert(rc == 0 || rc == 1);
  return rc == 0;
}

uint16_t MyEloq::EloqKeySchema::ExtendKeyParts() const
{
  return key_def_->get_key_parts();
}

#if defined(DATA_STORE_TYPE_CASSANDRA)
void MyEloq::EloqHiddenKeySchema::EncodeFromBaseTable(
    const CassRow *row, std::vector<char> &key_buf) const
{
  const CassValue *cass_val= cass_row_get_column(row, non_pk_field_cnt_);
  const unsigned char *blob_ptr= nullptr;
  size_t blob_len= 0;
  cass_value_get_bytes(cass_val, &blob_ptr, &blob_len);
  assert(blob_len == 16);

  key_buf.reserve(16);
  key_buf.clear();
  key_buf.insert(key_buf.end(), blob_ptr, blob_ptr + blob_len);
}

void MyEloq::EloqHiddenKeySchema::EncodeFromIndexTable(
    const CassRow *row, std::vector<char> &key_buf, size_t offset) const
{
  const CassValue *cass_val= cass_row_get_column(row, offset);
  const unsigned char *blob_ptr= nullptr;
  size_t blob_len= 0;
  cass_value_get_bytes(cass_val, &blob_ptr, &blob_len);
  assert(blob_len == 16);

  key_buf.reserve(16);
  key_buf.clear();
  key_buf.insert(key_buf.end(), blob_ptr, blob_ptr + blob_len);
}
#endif

/*
MyEloq::TableKeySchemaTs::TableKeySchemaTs(
    const std::string &key_schemas_ts_str)
{
  std::stringstream ts_ss(key_schemas_ts_str);
  std::istream_iterator<std::string> ts_b(ts_ss);
  std::istream_iterator<std::string> ts_e;
  std::vector<std::string> schemas_ts(ts_b, ts_e);
  pk_schema_ts_= std::stoull(schemas_ts.at(0));
  for (size_t idx= 1; idx < schemas_ts.size(); ++idx)
  {
    txservice::TableType table_type;
    if (schemas_ts[idx].find(txservice::UNIQUE_INDEX_NAME_PREFIX) !=
        std::string::npos)
    {
      table_type= txservice::TableType::UniqueSecondary;
    }
    else if (schemas_ts[idx].find(txservice::INDEX_NAME_PREFIX) !=
             std::string::npos)
    {
      table_type= txservice::TableType::Secondary;
    }
    else
    {
      assert(false && "Unknown secondary key type.");
    }
    txservice::TableName table_name(schemas_ts[idx], table_type);
    ++idx;
    sk_schemas_ts_.try_emplace(std::move(table_name),
                               std::stoull(schemas_ts[idx]));
  }
}

std::string MyEloq::TableKeySchemaTs::Serialize() const
{
  std::string output_str;
  size_t len_sizeof= sizeof(size_t);

  std::string table_ts(std::to_string(pk_schema_ts_));
  size_t len_val= table_ts.size();
  char *len_ptr= reinterpret_cast<char *>(&len_val);
  output_str.append(len_ptr, len_sizeof);
  output_str.append(table_ts.data(), len_val);

  std::string index_tables_ts;
  if (sk_schemas_ts_.size() != 0)
  {
    for (auto it= sk_schemas_ts_.cbegin(); it != sk_schemas_ts_.cend(); ++it)
    {
      index_tables_ts.append(it->first.StringView())
          .append(" ")
          .append(std::to_string(it->second))
          .append(" ");
    }
    index_tables_ts.erase(index_tables_ts.size() - 1);
  }
  else
  {
    index_tables_ts.clear();
  }

  len_val= index_tables_ts.size();
  output_str.append(len_ptr, len_sizeof);
  output_str.append(index_tables_ts.data(), len_val);

  return output_str;
}

void MyEloq::TableKeySchemaTs::Deserialize(const char *buf,
                                                size_t &offset)
{
  if (buf == nullptr || buf[0] == '\0')
  {
    return;
  }
  size_t len_sizeof= sizeof(size_t);
  size_t *len_ptr= (size_t *) (buf + offset);
  size_t len_val= *len_ptr;
  offset+= len_sizeof;

  pk_schema_ts_= std::stoull(std::string(buf + offset, len_val));
  offset+= len_val;

  len_ptr= (size_t *) (buf + offset);
  len_val= *len_ptr;
  offset+= len_sizeof;
  if (len_val != 0)
  {
    std::string index_tables_ts(buf + offset, len_val);
    std::stringstream sk_ss(index_tables_ts);
    std::istream_iterator<std::string> sk_b(sk_ss);
    std::istream_iterator<std::string> sk_e;
    std::vector<std::string> sk_iter(sk_b, sk_e);
    for (auto it= sk_iter.begin(); it != sk_iter.end(); ++it)
    {
      txservice::TableType table_type;
      if (it->find(txservice::UNIQUE_INDEX_NAME_PREFIX) != std::string::npos)
      {
        table_type= txservice::TableType::UniqueSecondary;
      }
      else if (it->find(txservice::INDEX_NAME_PREFIX) != std::string::npos)
      {
        table_type= txservice::TableType::Secondary;
      }
      else
      {
        assert(false && "Unknown secondary key type.");
      }
      txservice::TableName table_name(*it, table_type);
      sk_schemas_ts_.try_emplace(std::move(table_name), std::stoull(*(++it)));
    }
  }
  else
  {
    sk_schemas_ts_.clear();
  }
  offset+= len_val;
}
*/

/**
 * @brief Get the key schema ts of the specified [primary/secondary] key.
 *
 * @param table_name key schema name.
 *
 * @return If the key is newly added, return 1. Otherwise, return normal ts.
 */
/*
uint64_t MyEloq::TableKeySchemaTs::GetKeySchemaTs(
   const txservice::TableName &table_name) const
{
 if (table_name.Type() == txservice::TableType::Primary)
 {
   return pk_schema_ts_;
 }
 else
 {
   auto v_it= sk_schemas_ts_.find(table_name);
   return v_it == sk_schemas_ts_.end() ? 1 : v_it->second;
 }
}
*/
