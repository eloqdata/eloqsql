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
   Copyright (c) 2012,2013 Monty Program Ab
   Copyright (c) 2020, MariaDB Corporation.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02111-1301 USA
 */

#include "eloq_schema.h"
#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation // gcc: Class implementation
#endif

/* For use of 'PRIu64': */
#define __STDC_FORMAT_MACROS

/* This C++ file's header file */
#include "eloq_key_def.h"

#include <inttypes.h>
/* C++ standard header files */
#include <algorithm>
#include <array>
#include <limits>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

/* MySQL header files */
#include "field.h"
#include "key.h"
#include "m_ctype.h"
#include "my_bit.h"
#include "my_bitmap.h"
#include "sql_table.h"
#include "log.h"
#include "mysqld_error.h"
#include "eloqsql_key.h"

#define HA_EXIT_FAILURE 1
#define HA_EXIT_SUCCESS 0

namespace MyEloq
{

void get_mem_comparable_space(const CHARSET_INFO *cs,
                              const std::vector<uchar> **xfrm,
                              size_t *xfrm_len, size_t *mb_len);

/*
  MariaDB's replacement for FB/MySQL Field::check_field_name_match :
*/
inline bool field_check_field_name_match(Field *field, const char *name)
{
  return (0 ==
          my_strcasecmp(system_charset_info, field->field_name.str, name));
}

/*
  Decode  current key field
  @param  fpi               IN      data structure contains field metadata
  @param  field             IN      current field
  @param  reader            IN      key slice reader
  @param  unp_reader        IN      unpack information reader
  @return
    HA_EXIT_SUCCESS    OK
    other              HA_ERR error code
*/
int mono_convert_to_record_key_decoder::decode_field(
    mono_field_packing *fpi, Field *field, mono_string_reader *reader,
    const uchar *const default_value, mono_string_reader *unpack_reader)
{
  if (fpi->m_maybe_null)
  {
    const char *nullp;
    if (!(nullp= reader->read(1)))
    {
      return HA_EXIT_FAILURE;
    }

    if (*nullp == 0)
    {
      /* Set the NULL-bit of this field */
      field->set_null();
      /* Also set the field to its default value */
      memcpy(field->ptr, default_value, field->pack_length());
      return HA_EXIT_SUCCESS;
    }
    else if (*nullp == 1)
    {
      field->set_notnull();
    }
    else
    {
      return HA_EXIT_FAILURE;
    }
  }

  return (fpi->m_unpack_func)(fpi, field, field->ptr, reader, unpack_reader);
}

/*
  Decode  current key field

  @param  buf               OUT     the buf starting address
  @param  offset            OUT     the bytes offset when data is written
  @param  fpi               IN      data structure contains field metadata
  @param  table             IN      current table
  @param  field             IN      current field
  @param  has_unpack_inf    IN      whether contains unpack inf
  @param  reader            IN      key slice reader
  @param  unp_reader        IN      unpack information reader
  @return
    HA_EXIT_SUCCESS    OK
    other              HA_ERR error code
*/
int mono_convert_to_record_key_decoder::decode(
    uchar *const buf, uint *offset, mono_field_packing *fpi, TABLE *table,
    Field *field, bool has_unpack_info, mono_string_reader *reader,
    mono_string_reader *unpack_reader)
{
  DBUG_ASSERT(buf != nullptr);
  DBUG_ASSERT(offset != nullptr);

  uint field_offset= field->ptr - table->record[0];
  *offset= field_offset;
  uint null_offset= field->null_offset();
  bool maybe_null= field->real_maybe_null();

  field->move_field(buf + field_offset,
                    maybe_null ? buf + null_offset : nullptr, field->null_bit);

  // If we need unpack info, but there is none, tell the unpack function
  // this by passing unp_reader as nullptr. If we never read unpack_info
  // during unpacking anyway, then there won't an error.
  bool maybe_missing_unpack= !has_unpack_info && fpi->uses_unpack_info();
  int res=
      decode_field(fpi, field, reader, table->s->default_values + field_offset,
                   maybe_missing_unpack ? nullptr : unpack_reader);

  // Restore field->ptr and field->null_ptr
  field->move_field(table->record[0] + field_offset,
                    maybe_null ? table->record[0] + null_offset : nullptr,
                    field->null_bit);
  if (res != UNPACK_SUCCESS)
  {
    return HA_ERR_TABLE_CORRUPT;
  }
  return HA_EXIT_SUCCESS;
}

/*
  Skip current key field

  @param  fpi          IN    data structure contains field metadata
  @param  field        IN    current field
  @param  reader       IN    key slice reader
  @param  unp_reader   IN    unpack information reader
  @return
    HA_EXIT_SUCCESS    OK
    other              HA_ERR error code
*/
int mono_convert_to_record_key_decoder::skip(const mono_field_packing *fpi,
                                             const Field *field,
                                             mono_string_reader *reader,
                                             mono_string_reader *unp_reader)
{
  /* It is impossible to unpack the column. Skip it. */
  if (fpi->m_maybe_null)
  {
    const char *nullp;
    if (!(nullp= reader->read(1)))
    {
      return HA_ERR_TABLE_CORRUPT;
    }
    if (*nullp == 0)
    {
      /* This is a NULL value */
      return HA_EXIT_SUCCESS;
    }
    /* If NULL marker is not '0', it can be only '1'  */
    if (*nullp != 1)
    {
      return HA_ERR_TABLE_CORRUPT;
    }
  }
  if ((fpi->m_skip_func)(fpi, field, reader))
  {
    return HA_ERR_TABLE_CORRUPT;
  }
  // If this is a space padded varchar, we need to skip the indicator
  // bytes for trailing bytes. They're useless since we can't restore the
  // field anyway.
  //
  // There is a special case for prefixed varchars where we do not
  // generate unpack info, because we know prefixed varchars cannot be
  // unpacked. In this case, it is not necessary to skip.
  if (fpi->m_skip_func == &mono_key_def::skip_variable_space_pad &&
      !fpi->m_unpack_info_stores_value)
  {
    unp_reader->read(fpi->m_unpack_info_uses_two_bytes ? 2 : 1);
  }
  return HA_EXIT_SUCCESS;
}

mono_key_field_iterator::mono_key_field_iterator(
    const mono_key_def *key_def, mono_field_packing *pack_info,
    mono_string_reader *reader, mono_string_reader *unp_reader, TABLE *table,
    bool has_unpack_info, const MY_BITMAP *covered_bitmap, uchar *const buf)
{
  m_key_def= key_def;
  m_pack_info= pack_info;
  m_iter_index= 0;
  m_iter_end= key_def->get_key_parts();
  m_reader= reader;
  m_unp_reader= unp_reader;
  m_table= table;
  m_has_unpack_info= has_unpack_info;
  m_buf= buf;
  m_secondary_key=
      (key_def->m_index_type == mono_key_def::INDEX_TYPE_SECONDARY);
  m_hidden_pk_exists= mono_key_def::table_has_hidden_pk(table->s);
  m_is_hidden_pk=
      (key_def->m_index_type == mono_key_def::INDEX_TYPE_HIDDEN_PRIMARY);
  m_curr_bitmap_pos= 0;
  m_offset= 0;
}

void *mono_key_field_iterator::get_dst() const { return m_buf + m_offset; }

int mono_key_field_iterator::get_field_index() const
{
  DBUG_ASSERT(m_field != nullptr);
  return m_field->field_index;
}

bool mono_key_field_iterator::get_is_null() const { return m_is_null; }
Field *mono_key_field_iterator::get_field() const
{
  DBUG_ASSERT(m_field != nullptr);
  return m_field;
}

bool mono_key_field_iterator::has_next() { return m_iter_index < m_iter_end; }

/**
 Iterate each field in the key and decode/skip one by one
*/
int mono_key_field_iterator::next()
{
  int status= HA_EXIT_SUCCESS;
  while (m_iter_index < m_iter_end)
  {
    int curr_index= m_iter_index++;

    m_fpi= &m_pack_info[curr_index];
    /*
      Hidden pk field is packed at the end of the secondary keys, but the SQL
      layer does not know about it. Skip retrieving field if hidden pk.
    */
    if ((m_secondary_key && m_hidden_pk_exists &&
         curr_index + 1 == m_iter_end) ||
        m_is_hidden_pk)
    {
      DBUG_ASSERT(!m_fpi->m_unpack_func);
      if ((m_fpi->m_skip_func)(m_fpi, nullptr, m_reader))
      {
        return HA_ERR_TABLE_CORRUPT;
      }
      return HA_EXIT_SUCCESS;
    }

    m_field= m_fpi->get_field_in_table(m_table);

    if (m_fpi->m_unpack_func)
    {
      /* It is possible to unpack this column. Do it. */
      status= mono_convert_to_record_key_decoder::decode(
          m_buf, &m_offset, m_fpi, m_table, m_field, m_has_unpack_info,
          m_reader, m_unp_reader);
      if (status)
      {
        return status;
      }
      break;
    }
    else
    {
      status= mono_convert_to_record_key_decoder::skip(m_fpi, m_field,
                                                       m_reader, m_unp_reader);
      if (status)
      {
        return status;
      }
    }
  }
  return HA_EXIT_SUCCESS;
}

/*
  mono_key_def class implementation
*/
mono_key_def::mono_key_def(uint keyno_arg, uchar index_type_arg)
    : m_index_type(index_type_arg), m_pk_part_no(nullptr),
      m_pack_info(nullptr), m_keyno(keyno_arg), m_key_parts(0),
      m_maxlength(0) // means 'not intialized'
{
  mysql_mutex_init(0, &m_mutex, MY_MUTEX_INIT_FAST);
}

mono_key_def::mono_key_def(const mono_key_def &k)
    : m_pk_part_no(k.m_pk_part_no), m_pack_info(k.m_pack_info),
      m_keyno(k.m_keyno), m_key_parts(k.m_key_parts),
      m_maxlength(k.m_maxlength)
{
  mysql_mutex_init(0, &m_mutex, MY_MUTEX_INIT_FAST);
  if (k.m_pack_info)
  {
    const size_t size= sizeof(mono_field_packing) * k.m_key_parts;
    void *pack_info= my_malloc(PSI_INSTRUMENT_ME, size, MYF(0));
    memcpy(pack_info, k.m_pack_info, size);
    m_pack_info= reinterpret_cast<mono_field_packing *>(pack_info);
  }

  if (k.m_pk_part_no)
  {
    const size_t size= sizeof(uint) * m_key_parts;
    m_pk_part_no=
        reinterpret_cast<uint *>(my_malloc(PSI_INSTRUMENT_ME, size, MYF(0)));
    memcpy(m_pk_part_no, k.m_pk_part_no, size);
  }
}

mono_key_def::~mono_key_def()
{
  mysql_mutex_destroy(&m_mutex);

  my_free(m_pk_part_no);
  m_pk_part_no= nullptr;

  my_free(m_pack_info);
  m_pack_info= nullptr;
}

void mono_key_def::setup(const TABLE_SHARE *tbl)
{

  /*
    Set max_length based on the table.  This can be called concurrently from
    multiple threads, so there is a mutex to protect this code.
  */
  const bool is_hidden_pk= (m_index_type == INDEX_TYPE_HIDDEN_PRIMARY);
  const bool secondary_key= (m_index_type == INDEX_TYPE_SECONDARY);
  const bool hidden_pk_exists= is_hidden_pk ? true : table_has_hidden_pk(tbl);

  if (!m_maxlength)
  {
    mysql_mutex_lock(&m_mutex);
    if (m_maxlength != 0)
    {
      mysql_mutex_unlock(&m_mutex);
      return;
    }

    KEY *key_info= nullptr;
    KEY *pk_info= nullptr;
    if (!is_hidden_pk)
    {
      key_info= &tbl->key_info[m_keyno];
      if (!hidden_pk_exists)
        pk_info= &tbl->key_info[tbl->primary_key];
    }

    if (secondary_key)
    {
      m_pk_key_parts= hidden_pk_exists ? 1 : pk_info->ext_key_parts;
    }
    else
    {
      pk_info= nullptr;
      m_pk_key_parts= 0;
    }

    // "unique" secondary keys support:
    m_key_parts= is_hidden_pk ? 1 : key_info->ext_key_parts;
    if (secondary_key && tbl->key_info[m_keyno].flags & HA_NOSAME)
    {
      m_unique_sk_only_key_parts= m_key_parts;
    }

    if (secondary_key)
    {
      /*
        In most cases, SQL layer puts PK columns as invisible suffix at the
        end of secondary key. There are cases where this doesn't happen:
        - unique secondary indexes.
        - partitioned tables.

        Internally, we always need PK columns as suffix (and InnoDB does,
        too, if you were wondering).

        The loop below will attempt to put all PK columns at the end of key
        definition.  Columns that are already included in the index (either
        by the user or by "extended keys" feature) are not included for the
        second time.
      */
      m_key_parts+= m_pk_key_parts;
    }

    if (secondary_key)
    {
      m_pk_part_no= reinterpret_cast<uint *>(
          my_malloc(PSI_INSTRUMENT_ME, sizeof(uint) * m_key_parts, MYF(0)));
    }
    else
    {
      m_pk_part_no= nullptr;
    }

    const size_t size= sizeof(mono_field_packing) * m_key_parts;
    m_pack_info= reinterpret_cast<mono_field_packing *>(
        my_malloc(PSI_INSTRUMENT_ME, size, MYF(0)));

    size_t max_len= 0;
    int unpack_len= 0;
    int max_part_len= 0;
    bool simulating_extkey= false;
    uint dst_i= 0;

    uint keyno_to_set= m_keyno;
    uint keypart_to_set= 0;

    if (is_hidden_pk)
    {
      Field *field= nullptr;
      m_pack_info[dst_i].setup(this, field, keyno_to_set, 0, 0);
      m_pack_info[dst_i].m_unpack_data_offset= unpack_len;
      max_len+= m_pack_info[dst_i].m_max_image_len;
      max_part_len= std::max(max_part_len, m_pack_info[dst_i].m_max_image_len);
      dst_i++;
    }
    else
    {
      KEY_PART_INFO *key_part= key_info->key_part;

      /* this loop also loops over the 'extended key' tail */
      for (uint src_i= 0; src_i < m_key_parts; src_i++, keypart_to_set++)
      {
        Field *const field= key_part ? key_part->field : nullptr;

        if (simulating_extkey && !hidden_pk_exists)
        {
          DBUG_ASSERT(secondary_key);
          /* Check if this field is already present in the key definition */
          bool found= false;
          for (uint j= 0; j < key_info->ext_key_parts; j++)
          {
            if (field->field_index ==
                    key_info->key_part[j].field->field_index &&
                key_part->length == key_info->key_part[j].length)
            {
              found= true;
              break;
            }
          }

          if (found)
          {
            key_part++;
            continue;
          }
        }

        if (field && field->real_maybe_null())
          max_len+= 1; // NULL-byte

        m_pack_info[dst_i].setup(this, field, keyno_to_set, keypart_to_set,
                                 key_part ? key_part->length : 0);
        m_pack_info[dst_i].m_unpack_data_offset= unpack_len;

        if (pk_info)
        {
          m_pk_part_no[dst_i]= -1;
          for (uint j= 0; j < m_pk_key_parts; j++)
          {
            if (field->field_index == pk_info->key_part[j].field->field_index)
            {
              m_pk_part_no[dst_i]= j;
              break;
            }
          }
        }
        else if (secondary_key && hidden_pk_exists)
        {
          /*
            The hidden pk can never be part of the sk.  So it is always
            appended to the end of the sk.
          */
          m_pk_part_no[dst_i]= -1;
          if (simulating_extkey)
            m_pk_part_no[dst_i]= 0;
        }

        max_len+= m_pack_info[dst_i].m_max_image_len;

        max_part_len=
            std::max(max_part_len, m_pack_info[dst_i].m_max_image_len);

        key_part++;
        /*
          For "unique" secondary indexes, pretend they have
          "index extensions".

          MariaDB also has this property: if an index has a partially-covered
          column like KEY(varchar_col(N)), then the SQL layer will think it is
          not "extended" with PK columns. The code below handles this case,
          also.
         */
        if (secondary_key && src_i + 1 == key_info->ext_key_parts)
        {
          simulating_extkey= true;
          if (!hidden_pk_exists)
          {
            keyno_to_set= tbl->primary_key;
            key_part= pk_info->key_part;
            keypart_to_set= (uint) -1;
          }
          else
          {
            keyno_to_set= tbl->keys;
            key_part= nullptr;
            keypart_to_set= 0;
          }
        }

        dst_i++;
      }
    }

    m_key_parts= dst_i;
    /*
      This should be the last member variable set before releasing the mutex
      so that other threads can't see the object partially set up.
     */
    m_maxlength= max_len;

    mysql_mutex_unlock(&m_mutex);
  }
}

/**
  Read a memcmp key part from a slice using the passed in reader.

  Returns -1 if field was null, 1 if error, 0 otherwise.
*/
int mono_key_def::read_memcmp_key_part(const TABLE *table_arg,
                                       mono_string_reader *reader,
                                       const uint part_num) const
{
  /* It is impossible to unpack the column. Skip it. */
  if (m_pack_info[part_num].m_maybe_null)
  {
    const char *nullp;
    if (!(nullp= reader->read(1)))
      return 1;
    if (*nullp == 0)
    {
      /* This is a NULL value */
      return -1;
    }
    else
    {
      /* If NULL marker is not '0', it can be only '1'  */
      if (*nullp != 1)
        return 1;
    }
  }

  mono_field_packing *fpi= &m_pack_info[part_num];
  DBUG_ASSERT(table_arg->s != nullptr);

  bool is_hidden_pk_part= (part_num + 1 == m_key_parts) &&
                          (table_arg->s->primary_key == MAX_INDEXES);
  Field *field= nullptr;
  if (!is_hidden_pk_part)
  {
    field= fpi->get_field_in_table(table_arg);
  }
  if ((fpi->m_skip_func)(fpi, field, reader))
  {
    return 1;
  }
  return 0;
}

/**
  Get a mem-comparable form of Primary Key from mem-comparable form of this key

  @param
    pk_descr        Primary Key descriptor
    key             Index tuple from this key in mem-comparable form
    pk_buffer  OUT  Put here mem-comparable form of the Primary Key.

  @note
    It may or may not be possible to restore primary key columns to their
    mem-comparable form.  To handle all cases, this function copies mem-
    comparable forms directly.

    RocksDB SE supports "Extended keys". This means that PK columns are present
    at the end of every key.  If the key already includes PK columns, then
    these columns are not present at the end of the key.

    Because of the above, we copy each primary key column.

  @todo
    If we checked crc32 checksums in this function, we would catch some CRC
    violations that we currently don't. On the other hand, there is a broader
    set of queries for which we would check the checksum twice.
*/

uint mono_key_def::get_primary_key_tuple(const TABLE *const table,
                                         const mono_key_def &pk_descr,
                                         const MyEloq::Slice &key,
                                         uchar *const pk_buffer) const
{
  DBUG_ASSERT(table != nullptr);
  DBUG_ASSERT(key != nullptr);
  DBUG_ASSERT(m_index_type == mono_key_def::INDEX_TYPE_SECONDARY);
  DBUG_ASSERT(pk_buffer);

  uint size= 0;
  uchar *buf= pk_buffer;
  DBUG_ASSERT(m_pk_key_parts);

  const char *start_offs[MAX_REF_PARTS];
  const char *end_offs[MAX_REF_PARTS];
  int pk_key_part;
  uint i;
  mono_string_reader reader(&key);

  for (i= 0; i < m_key_parts; i++)
  {
    if ((pk_key_part= m_pk_part_no[i]) != -1)
    {
      start_offs[pk_key_part]= reader.get_current_ptr();
    }

    if (read_memcmp_key_part(table, &reader, i) > 0)
    {
      return MONO_INVALID_KEY_LEN;
    }

    if (pk_key_part != -1)
    {
      end_offs[pk_key_part]= reader.get_current_ptr();
    }
  }

  for (i= 0; i < m_pk_key_parts; i++)
  {
    const uint part_size= end_offs[i] - start_offs[i];
    memcpy(buf, start_offs[i], end_offs[i] - start_offs[i]);
    buf+= part_size;
    size+= part_size;
  }

  return size;
}

/**
  Get a mem-comparable form of Secondary Key from mem-comparable form of this
  key, without the extended primary key tail.

  @param
    key                Index tuple from this key in mem-comparable form
    sk_buffer     OUT  Put here mem-comparable form of the Secondary Key.
    n_null_fields OUT  Put number of null fields contained within sk entry
*/
uint mono_key_def::get_memcmp_sk_parts(const TABLE *table,
                                       const MyEloq::Slice &key,
                                       uchar *sk_buffer,
                                       uint *n_null_fields) const
{
  DBUG_ASSERT(table != nullptr);
  DBUG_ASSERT(sk_buffer != nullptr);
  DBUG_ASSERT(n_null_fields != nullptr);
  DBUG_ASSERT(m_keyno != table->s->primary_key &&
              !table_has_hidden_pk(table->s));

  uchar *buf= sk_buffer;

  int res;
  mono_string_reader reader(&key);
  const char *start= reader.get_current_ptr();

  for (uint i= 0; i < table->key_info[m_keyno].user_defined_key_parts; i++)
  {
    if ((res= read_memcmp_key_part(table, &reader, i)) > 0)
    {
      return MONO_INVALID_KEY_LEN;
    }
    else if (res == -1)
    {
      (*n_null_fields)++;
    }
  }

  uint sk_memcmp_len= reader.get_current_ptr() - start;
  memcpy(buf, start, sk_memcmp_len);
  return sk_memcmp_len;
}

/**
  Convert index tuple into storage (i.e. mem-comparable) format

  @detail
    Currently this is done by unpacking into record_buffer and then
    packing index columns into storage format.

  @param pack_buffer Temporary area for packing varchar columns. Its
                     size is at least max_storage_fmt_length() bytes.
*/

uint mono_key_def::pack_index_tuple(
    TABLE *const tbl, uchar *const pack_buffer, uchar *const packed_tuple,
    uchar *const record_buffer, const uchar *const key_tuple,
    const key_part_map &keypart_map,
    mono_string_writer *const unpack_info) const
{
  DBUG_ASSERT(tbl != nullptr);
  DBUG_ASSERT(pack_buffer != nullptr);
  DBUG_ASSERT(packed_tuple != nullptr);
  DBUG_ASSERT(key_tuple != nullptr);

  /* We were given a record in KeyTupleFormat. First, save it to record */
  const uint key_len= calculate_key_len(tbl, m_keyno, key_tuple, keypart_map);
  key_restore(record_buffer, key_tuple, &tbl->key_info[m_keyno], key_len);

  uint n_used_parts= my_count_bits(keypart_map);
  if (keypart_map == HA_WHOLE_KEY)
    n_used_parts= 0; // Full key is used

  /* Then, convert the record into a mem-comparable form */
  return pack_record(tbl, pack_buffer, record_buffer, packed_tuple,
                     unpack_info, false, nullptr, nullptr, n_used_parts);
}

/**
  @brief
    Check if "unpack info" data includes checksum.

  @detail
    This is used only by CHECK TABLE to count the number of rows that have
    checksums.
*/

bool mono_key_def::unpack_info_has_checksum(const MyEloq::Slice &unpack_info)
{
  size_t size= unpack_info.size();
  if (size == 0)
  {
    return false;
  }
  const uchar *ptr= (const uchar *) unpack_info.data();

  // Skip unpack info if present.
  if (is_unpack_data_tag(ptr[0]) && size >= get_unpack_header_size(ptr[0]))
  {
    const uint16 skip_len= mono_netbuf_to_uint16(ptr + 1);

    size-= skip_len;
    ptr+= skip_len;
  }

  return (size == MONO_CHECKSUM_CHUNK_SIZE &&
          ptr[0] == MONO_CHECKSUM_DATA_TAG);
}

void mono_key_def::get_ceiling_key(EloqKey &key)
{
  std::string &packed= key.PackedValue();
  size_t pack_size= packed.size();
  assert(pack_size <= m_maxlength);
  packed.resize(m_maxlength);
  for (; pack_size < m_maxlength; pack_size++)
  {
    packed[pack_size]= uchar(0xFF);
  }
}

/*
  @return Number of bytes that were changed
*/
int mono_key_def::successor(uchar *const packed_tuple, const uint len)
{
  DBUG_ASSERT(packed_tuple != nullptr);

  int changed= 0;
  uchar *p= packed_tuple + len - 1;
  for (; p >= packed_tuple; p--)
  {
    changed++;
    if (*p != uchar(0xFF))
    {
      *p= *p + 1;
      break;
    }
    *p= '\0';
  }
  return changed;
}

/*
  @return Number of bytes that were changed
*/
int mono_key_def::predecessor(uchar *const packed_tuple, const uint len)
{
  DBUG_ASSERT(packed_tuple != nullptr);

  int changed= 0;
  uchar *p= packed_tuple + len - 1;
  for (; p >= packed_tuple; p--)
  {
    changed++;
    if (*p != uchar(0x00))
    {
      *p= *p - 1;
      break;
    }
    *p= 0xFF;
  }
  return changed;
}

static const std::map<char, size_t> UNPACK_HEADER_SIZES= {
    {MONO_UNPACK_DATA_TAG, MONO_UNPACK_HEADER_SIZE},
    {MONO_UNPACK_COVERED_DATA_TAG, MONO_UNPACK_COVERED_HEADER_SIZE}};

/*
  @return The length in bytes of the header specified by the given tag
*/
size_t mono_key_def::get_unpack_header_size(char tag)
{
  DBUG_ASSERT(is_unpack_data_tag(tag));
  return UNPACK_HEADER_SIZES.at(tag);
}

uchar *mono_key_def::pack_field(Field *const field,
                                mono_field_packing *pack_info, uchar *tuple,
                                uchar *const packed_tuple,
                                uchar *const pack_buffer,
                                mono_string_writer *const unpack_info,
                                uint *const n_null_fields) const
{
  if (field->real_maybe_null())
  {
    DBUG_ASSERT(is_storage_available(tuple - packed_tuple, 1));
    if (field->is_real_null())
    {
      /* NULL value. store '\0' so that it sorts before non-NULL values */
      *tuple++= 0;
      /* That's it, don't store anything else */
      if (n_null_fields)
        (*n_null_fields)++;
      return tuple;
    }
    else
    {
      /* Not a NULL value. Store '1' */
      *tuple++= 1;
    }
  }

  const bool create_unpack_info=
      (unpack_info && // we were requested to generate unpack_info
       pack_info->uses_unpack_info()); // and this keypart uses it
  mono_pack_field_context pack_ctx(unpack_info);

  // Set the offset for methods which do not take an offset as an argument
  DBUG_ASSERT(
      is_storage_available(tuple - packed_tuple, pack_info->m_max_image_len));

  (pack_info->m_pack_func)(pack_info, field, pack_buffer, &tuple, &pack_ctx);

  /* Make "unpack info" to be stored in the value */
  if (create_unpack_info)
  {
    (pack_info->m_make_unpack_info_func)(pack_info->m_charset_codec, field,
                                         &pack_ctx);
  }

  return tuple;
}

/**
  Get index columns from the record and pack them into mem-comparable form.

  @param
    tbl                   Table we're working on
    record           IN   Record buffer with fields in table->record format
    pack_buffer      IN   Temporary area for packing varchars. The size is
                          at least max_storage_fmt_length() bytes.
    packed_tuple     OUT  Key in the mem-comparable form
    unpack_info      OUT  Unpack data
    unpack_info_len  OUT  Unpack data length
    n_key_parts           Number of keyparts to process. 0 means all of them.
    n_null_fields    OUT  Number of key fields with NULL value.
  @detail
    Some callers do not need the unpack information, they can pass
    unpack_info=nullptr, unpack_info_len=nullptr.

  @return
    Length of the packed tuple
*/

uint mono_key_def::pack_record(const TABLE *const tbl,
                               uchar *const pack_buffer,
                               const uchar *const record,
                               uchar *const packed_tuple,
                               mono_string_writer *const unpack_info,
                               const bool should_store_row_debug_checksums,
                               size_t *unique_sk_pack_size,
                               const uchar *hidden_pk_id, uint n_key_parts,
                               uint *const n_null_fields) const
{
  DBUG_ASSERT(tbl != nullptr);
  DBUG_ASSERT(pack_buffer != nullptr);
  DBUG_ASSERT(record != nullptr);
  DBUG_ASSERT(packed_tuple != nullptr);

  uchar *tuple= packed_tuple;
  size_t unpack_start_pos= size_t(-1);
  const bool hidden_pk_exists= table_has_hidden_pk(tbl->s);

  // If n_key_parts is 0, it means all columns.
  // The following includes the 'extended key' tail.
  // The 'extended key' includes primary key. This is done to 'uniqify'
  // non-unique indexes
  const bool use_all_columns= n_key_parts == 0 || n_key_parts == MAX_REF_PARTS;
  // unique_sk_pack_size is set only when the unique sk is being modified.
  // In this scenario, we need to use m_key_parts(include trailing pk key
  // parts) instead of n_key_parts provided by mysql(include only unique_sk)
  const bool is_modifing_unique_sk= unique_sk_pack_size != nullptr;

  // If hidden pk exists, but hidden pk wasnt passed in, we can't pack the
  // hidden key part.  So we skip it (its always 1 part).
  if (hidden_pk_exists && !hidden_pk_id && use_all_columns)
  {
    n_key_parts= m_key_parts - 1;
  }
  else if (use_all_columns || is_modifing_unique_sk)
  {
    n_key_parts= m_key_parts;
  }

  if (n_null_fields)
    *n_null_fields= 0;

  if (unpack_info)
  {
    unpack_info->clear();
    unpack_start_pos= unpack_info->get_current_pos();
    // we don't know the total length yet, so write a zero
    unpack_info->write_uint16(0);
  }

  for (uint i= 0; i < n_key_parts; i++)
  {
    if (is_modifing_unique_sk && i == m_unique_sk_only_key_parts)
    {
      // During Insert/Update/Delete, write down the unique_sk_pack_size to
      // generate sk only EloqKey.
      *unique_sk_pack_size= tuple - packed_tuple;
    }

    // Fill hidden pk id into the last key part for secondary keys for tables
    // with no pk
    if (hidden_pk_exists && hidden_pk_id && i + 1 == n_key_parts)
    {
      m_pack_info[i].fill_hidden_pk_val(&tuple, hidden_pk_id);
      break;
    }

    Field *const field= m_pack_info[i].get_original_field_in_table(tbl);

    DBUG_ASSERT(field != nullptr);

    // Fix the field->field_length for prefix key
    uint original_field_length= field->field_length;
    field->field_length= m_pack_info[i].get_key_part_length();

    uint field_offset= field->ptr - tbl->record[0];
    uint null_offset= field->null_offset(tbl->record[0]);
    bool maybe_null= field->real_maybe_null();

    field->move_field(const_cast<uchar *>(record) + field_offset,
                      maybe_null ? const_cast<uchar *>(record) + null_offset
                                 : nullptr,
                      field->null_bit);
    // WARNING! Don't return without restoring field->ptr and field->null_ptr

    tuple= pack_field(field, &m_pack_info[i], tuple, packed_tuple, pack_buffer,
                      unpack_info, n_null_fields);
    // Restore field->ptr and field->null_ptr
    field->move_field(tbl->record[0] + field_offset,
                      maybe_null ? tbl->record[0] + null_offset : nullptr,
                      field->null_bit);

    // Restore the field->field_length
    field->field_length= original_field_length;
  }

  if (unpack_info)
  {
    const size_t len= unpack_info->get_current_pos() - unpack_start_pos;
    DBUG_ASSERT(len <= std::numeric_limits<uint16_t>::max());

    // If no unpack info is written by key columns, clear the writer. Otherwise
    // store the total len of unpack info.
    if (len > MONO_UNPACK_DATA_LEN_SIZE)
    {
      unpack_info->write_uint16_at(unpack_start_pos, len);
    }
    else
    {
      unpack_info->clear();
    }
  }

  DBUG_ASSERT(is_storage_available(tuple - packed_tuple, 0));

  return tuple - packed_tuple;
}

void mono_key_def::pack_key(const mysql::KEY *key_info,
                            const std::vector<std::string> &key_cols,
                            std::vector<char> &buf) const
{
  buf.clear();

  // Allocate max key length for buf. We will resize it at the end.
  buf.resize(m_maxlength);
  uchar *tuple= reinterpret_cast<uchar *>(buf.data());
  uchar pack_buffer[m_maxlength];

  for (size_t key_part_idx= 0; key_part_idx < key_info->user_defined_key_parts;
       ++key_part_idx)
  {
    const mysql::KEY_PART_INFO &key_part= key_info->key_part[key_part_idx];

    Field *const field= key_part.field;
    DBUG_ASSERT(field != nullptr);

    if (field->real_maybe_null())
    {
      if (key_cols[key_part_idx].size() == 0)
      {
        /* NULL value. store '\0' so that it sorts before non-NULL values */
        *tuple++= 0;
        /* That's it, don't store anything else */
        continue;
      }
      else
      {
        /* Not a NULL value. Store '1' */
        *tuple++= 1;
      }
    }
    // Set the offset for methods which do not take an offset as an argument
    uchar *orig_ptr= field->ptr;
    uchar *orig_null_ptr= field->null_ptr;
    field->move_field((uchar *) key_cols[key_part_idx].data(), nullptr,
                      field->null_bit);
    (m_pack_info[key_part_idx].m_pack_func)(&m_pack_info[key_part_idx], field,
                                            pack_buffer, &tuple, nullptr);
    field->move_field(orig_ptr, orig_null_ptr, field->null_bit);
  }

  // Resize the buf to correct key length
  buf.resize(tuple - (uchar *) buf.data());
}

/**
  Pack the hidden primary key into mem-comparable form.

  @param
    tbl                   Table we're working on
    hidden_pk_id     IN   New value to be packed into key
    packed_tuple     OUT  Key in the mem-comparable form

  @return
    Length of the packed tuple
*/

uint mono_key_def::pack_hidden_pk(const uchar *hidden_pk_id,
                                  uchar *const packed_tuple) const
{
  DBUG_ASSERT(packed_tuple != nullptr);

  uchar *tuple= packed_tuple;
  DBUG_ASSERT(m_key_parts == 1);
  DBUG_ASSERT(is_storage_available(tuple - packed_tuple,
                                   m_pack_info[0].m_max_image_len));

  m_pack_info[0].fill_hidden_pk_val(&tuple, hidden_pk_id);

  DBUG_ASSERT(is_storage_available(tuple - packed_tuple, 0));
  return tuple - packed_tuple;
}

/*
  Function of type mono_index_field_pack_t
*/

void mono_key_def::pack_with_make_sort_key(
    mono_field_packing *const fpi, Field *const field,
    uchar *const buf MY_ATTRIBUTE((__unused__)), uchar **dst,
    mono_pack_field_context *const pack_ctx MY_ATTRIBUTE((__unused__)))
{
  DBUG_ASSERT(fpi != nullptr);
  DBUG_ASSERT(field != nullptr);
  DBUG_ASSERT(dst != nullptr);
  DBUG_ASSERT(*dst != nullptr);

  const int max_len= fpi->m_max_image_len;
  MY_BITMAP *old_map;

  old_map= dbug_tmp_use_all_columns(field->table, &field->table->read_set);
  field->sort_string(*dst, max_len);
  dbug_tmp_restore_column_map(&field->table->read_set, old_map);
  *dst+= max_len;
}

/*
  Compares two keys without unpacking

  @detail
  @return
    0 - Ok. column_index is the index of the first column which is different.
          -1 if two kes are equal
    1 - Data format error.
*/
int mono_key_def::compare_keys(const MyEloq::Slice *key1,
                               const MyEloq::Slice *key2,
                               std::size_t *const column_index) const
{
  DBUG_ASSERT(key1 != nullptr);
  DBUG_ASSERT(key2 != nullptr);
  DBUG_ASSERT(column_index != nullptr);

  // the caller should check the return value and
  // not rely on column_index being valid
  *column_index= 0xbadf00d;

  mono_string_reader reader1(key1);
  mono_string_reader reader2(key2);

  for (uint i= 0; i < m_key_parts; i++)
  {
    const mono_field_packing *const fpi= &m_pack_info[i];
    if (fpi->m_maybe_null)
    {
      const auto nullp1= reader1.read(1);
      const auto nullp2= reader2.read(1);

      if (nullp1 == nullptr || nullp2 == nullptr)
      {
        return HA_EXIT_FAILURE;
      }

      if (*nullp1 != *nullp2)
      {
        *column_index= i;
        return HA_EXIT_SUCCESS;
      }

      if (*nullp1 == 0)
      {
        /* This is a NULL value */
        continue;
      }
    }

    const auto before_skip1= reader1.get_current_ptr();
    const auto before_skip2= reader2.get_current_ptr();
    DBUG_ASSERT(fpi->m_skip_func);
    if ((fpi->m_skip_func)(fpi, nullptr, &reader1))
    {
      return HA_EXIT_FAILURE;
    }
    if ((fpi->m_skip_func)(fpi, nullptr, &reader2))
    {
      return HA_EXIT_FAILURE;
    }
    const auto size1= reader1.get_current_ptr() - before_skip1;
    const auto size2= reader2.get_current_ptr() - before_skip2;
    if (size1 != size2)
    {
      *column_index= i;
      return HA_EXIT_SUCCESS;
    }

    if (memcmp(before_skip1, before_skip2, size1) != 0)
    {
      *column_index= i;
      return HA_EXIT_SUCCESS;
    }
  }

  *column_index= m_key_parts;
  return HA_EXIT_SUCCESS;
}

/*
  @brief
    Given a zero-padded key, determine its real key length

  @detail
    Fixed-size skip functions just read.
*/

size_t mono_key_def::key_length(const TABLE *const table,
                                const MyEloq::Slice &key) const
{
  DBUG_ASSERT(table != nullptr);

  mono_string_reader reader(&key);

  for (uint i= 0; i < m_key_parts; i++)
  {
    const mono_field_packing *fpi= &m_pack_info[i];
    const Field *field= nullptr;
    if (m_index_type != INDEX_TYPE_HIDDEN_PRIMARY)
    {
      field= fpi->get_field_in_table(table);
    }
    if ((fpi->m_skip_func)(fpi, field, &reader))
    {
      return size_t(-1);
    }
  }
  return key.size() - reader.remaining_bytes();
}

/*
  Take mem-comparable form and unpack_info and unpack it to Table->record

  @detail
    not all indexes support this

  @return
    HA_EXIT_SUCCESS    OK
    other              HA_ERR error code
*/

int mono_key_def::unpack_record(TABLE *const table, uchar *const buf,
                                const MyEloq::Slice packed_key,
                                const MyEloq::Slice unpack_info,
                                const bool verify_row_debug_checksums) const
{
  mono_string_reader reader(&packed_key);
  mono_string_reader unp_reader=
      mono_string_reader::read_or_empty(&unpack_info);
  uint unp_len= unp_reader.remaining_bytes();
  uint record_unp_len;
  if (unp_len &&
      (unp_reader.read_uint16(&record_unp_len) || record_unp_len != unp_len))
  {
    return HA_ERR_TABLE_CORRUPT;
  }

  int err= HA_EXIT_SUCCESS;

  mono_key_field_iterator iter(this, m_pack_info, &reader, &unp_reader, table,
                               unp_len != 0, nullptr, buf);
  while (iter.has_next())
  {
    err= iter.next();
    if (err)
    {
      return err;
    }
  }

  if (reader.remaining_bytes())
    return HA_ERR_TABLE_CORRUPT;

  return HA_EXIT_SUCCESS;
}

bool mono_key_def::table_has_hidden_pk(const TABLE_SHARE *const table)
{
  return table->primary_key == MAX_INDEXES;
}

void mono_key_def::report_checksum_mismatch(const bool is_key,
                                            const char *const data,
                                            const size_t data_size) const
{
  // NO_LINT_DEBUG
  sql_print_error("Checksum mismatch in %s of key-value pair.",
                  is_key ? "key" : "value");

  my_error(ER_INTERNAL_ERROR, MYF(0), "Record checksum mismatch");
}

bool mono_key_def::index_format_min_check(const int pk_min,
                                          const int sk_min) const
{
  switch (m_index_type)
  {
  case INDEX_TYPE_PRIMARY:
  case INDEX_TYPE_HIDDEN_PRIMARY:
  case INDEX_TYPE_SECONDARY:
    return true;
  default:
    DBUG_ASSERT(0);
    return false;
  }
}

///////////////////////////////////////////////////////////////////////////////////////////
// mono_field_packing
///////////////////////////////////////////////////////////////////////////////////////////

/*
  Function of type mono_index_field_skip_t
*/

int mono_key_def::skip_max_length(const mono_field_packing *const fpi,
                                  const Field *const field
                                      MY_ATTRIBUTE((__unused__)),
                                  mono_string_reader *const reader)
{
  if (!reader->read(fpi->m_max_image_len))
    return HA_EXIT_FAILURE;
  return HA_EXIT_SUCCESS;
}

/*
  (MONO_ESCAPE_LENGTH-1) must be an even number so that pieces of lines are not
  split in the middle of an UTF-8 character. See the implementation of
  unpack_binary_or_utf8_varchar.
*/
#define MONO_ESCAPE_LENGTH 9
#define MONO_LEGACY_ESCAPE_LENGTH MONO_ESCAPE_LENGTH
static_assert((MONO_ESCAPE_LENGTH - 1) % 2 == 0,
              "MONO_ESCAPE_LENGTH-1 must be even.");

#define MONO_ENCODED_SIZE(len)                                                \
  ((len + (MONO_ESCAPE_LENGTH - 2)) / (MONO_ESCAPE_LENGTH - 1)) *             \
      MONO_ESCAPE_LENGTH

#define MONO_LEGACY_ENCODED_SIZE(len)                                         \
  ((len + (MONO_LEGACY_ESCAPE_LENGTH - 1)) /                                  \
   (MONO_LEGACY_ESCAPE_LENGTH - 1)) *                                         \
      MONO_LEGACY_ESCAPE_LENGTH

/*
  Function of type mono_index_field_skip_t
*/

int mono_key_def::skip_variable_length(const mono_field_packing *const fpi,
                                       const Field *const field,
                                       mono_string_reader *const reader)
{
  const uchar *ptr;
  bool finished= false;

  size_t dst_len; /* How much data can be there */
  if (field)
  {
    const Field_varstring *const field_var=
        static_cast<const Field_varstring *>(field);
    dst_len= field_var->pack_length() - field_var->length_bytes;
  }
  else
  {
    dst_len= UINT_MAX;
  }

  /* Decode the length-emitted encoding here */
  while ((ptr= (const uchar *) reader->read(MONO_ESCAPE_LENGTH)))
  {
    uint used_bytes;

    /* See pack_with_varchar_encoding. */

    used_bytes=
        calc_unpack_variable_format(ptr[MONO_ESCAPE_LENGTH - 1], &finished);

    if (used_bytes == (uint) -1 || dst_len < used_bytes)
    {
      return HA_EXIT_FAILURE; // Corruption in the data
    }

    if (finished)
    {
      break;
    }

    dst_len-= used_bytes;
  }

  if (!finished)
  {
    return HA_EXIT_FAILURE;
  }

  return HA_EXIT_SUCCESS;
}

const int VARCHAR_CMP_LESS_THAN_SPACES= 1;
const int VARCHAR_CMP_EQUAL_TO_SPACES= 2;
const int VARCHAR_CMP_GREATER_THAN_SPACES= 3;

/*
  Skip a keypart that uses Variable-Length Space-Padded encoding
*/

int mono_key_def::skip_variable_space_pad(const mono_field_packing *const fpi,
                                          const Field *const field,
                                          mono_string_reader *const reader)
{
  const uchar *ptr;
  bool finished= false;

  size_t dst_len= UINT_MAX; /* How much data can be there */

  if (field)
  {
    const Field_varstring *const field_var=
        static_cast<const Field_varstring *>(field);
    dst_len= field_var->pack_length() - field_var->length_bytes;
  }

  /* Decode the length-emitted encoding here */
  while ((ptr= (const uchar *) reader->read(fpi->m_segment_size)))
  {
    // See pack_with_varchar_space_pad
    const uchar c= ptr[fpi->m_segment_size - 1];
    if (c == VARCHAR_CMP_EQUAL_TO_SPACES)
    {
      // This is the last segment
      finished= true;
      break;
    }
    else if (c == VARCHAR_CMP_LESS_THAN_SPACES ||
             c == VARCHAR_CMP_GREATER_THAN_SPACES)
    {
      // This is not the last segment
      if ((fpi->m_segment_size - 1) > dst_len)
      {
        // The segment is full of data but the table field can't hold that
        // much! This must be data corruption.
        return HA_EXIT_FAILURE;
      }
      dst_len-= (fpi->m_segment_size - 1);
    }
    else
    {
      // Encountered a value that's none of the VARCHAR_CMP* constants
      // It's data corruption.
      return HA_EXIT_FAILURE;
    }
  }
  return finished ? HA_EXIT_SUCCESS : HA_EXIT_FAILURE;
}

/*
  Function of type mono_index_field_unpack_t
*/

int mono_key_def::unpack_integer(
    mono_field_packing *const fpi, Field *const field, uchar *const to,
    mono_string_reader *const reader,
    mono_string_reader *const unp_reader MY_ATTRIBUTE((__unused__)))
{
  const int length= fpi->m_max_image_len;

  const uchar *from;
  if (!(from= (const uchar *) reader->read(length)))
  {
    return UNPACK_FAILURE; /* Mem-comparable image doesn't have enough bytes */
  }

#ifdef WORDS_BIGENDIAN
  {
    if (static_cast<Field_num *>(field)->unsigned_flag)
    {
      to[0]= from[0];
    }
    else
    {
      to[0]= static_cast<char>(from[0] ^ 128); // Reverse the sign bit.
    }
    memcpy(to + 1, from + 1, length - 1);
  }
#else
  {
    const int sign_byte= from[0];
    if (static_cast<Field_num *>(field)->unsigned_flag)
    {
      to[length - 1]= sign_byte;
    }
    else
    {
      to[length - 1]=
          static_cast<char>(sign_byte ^ 128); // Reverse the sign bit.
    }
    for (int i= 0, j= length - 1; i < length - 1; ++i, --j)
      to[i]= from[j];
  }
#endif
  return UNPACK_SUCCESS;
}

int mono_key_def::unpack_unsigned(
    mono_field_packing *const fpi MY_ATTRIBUTE((__unused__)),
    Field *const field MY_ATTRIBUTE((__unused__)), uchar *const to,
    mono_string_reader *const reader,
    mono_string_reader *const unp_reader MY_ATTRIBUTE((__unused__)))
{
  const int length= fpi->m_max_image_len;

  const uchar *from;
  if (!(from= (const uchar *) reader->read(length)))
  {
    return UNPACK_FAILURE; /* Mem-comparable image doesn't have enough bytes */
  }

#ifdef WORDS_BIGENDIAN
  /* Parameterized length should enable loop unrolling */
  for (int i= 0; i < length; i++)
    to[i]= from[i];
#else
  /* Parameterized length should enable loop unrolling */
  for (int i= 0, j= length - 1; i < length; ++i, --j)
    to[i]= from[j];
#endif

  return UNPACK_SUCCESS;
}

#if !defined(WORDS_BIGENDIAN)
static void mono_swap_double_bytes(uchar *const dst, const uchar *const src)
{
#if defined(__FLOAT_WORD_ORDER) && (__FLOAT_WORD_ORDER == __BIG_ENDIAN)
  // A few systems store the most-significant _word_ first on little-endian
  dst[0]= src[3];
  dst[1]= src[2];
  dst[2]= src[1];
  dst[3]= src[0];
  dst[4]= src[7];
  dst[5]= src[6];
  dst[6]= src[5];
  dst[7]= src[4];
#else
  dst[0]= src[7];
  dst[1]= src[6];
  dst[2]= src[5];
  dst[3]= src[4];
  dst[4]= src[3];
  dst[5]= src[2];
  dst[6]= src[1];
  dst[7]= src[0];
#endif
}

static void mono_swap_float_bytes(uchar *const dst, const uchar *const src)
{
  dst[0]= src[3];
  dst[1]= src[2];
  dst[2]= src[1];
  dst[3]= src[0];
}
#else
#define mono_swap_double_bytes nullptr
#define mono_swap_float_bytes nullptr
#endif

int mono_key_def::unpack_floating_point(
    uchar *const dst, mono_string_reader *const reader, const size_t size,
    const int exp_digit, const uchar *const zero_pattern,
    const uchar *const zero_val, void (*swap_func)(uchar *, const uchar *))
{
  const uchar *const from= (const uchar *) reader->read(size);
  if (from == nullptr)
  {
    /* Mem-comparable image doesn't have enough bytes */
    return UNPACK_FAILURE;
  }

  /* Check to see if the value is zero */
  if (memcmp(from, zero_pattern, size) == 0)
  {
    memcpy(dst, zero_val, size);
    return UNPACK_SUCCESS;
  }

#if defined(WORDS_BIGENDIAN)
  // On big-endian, output can go directly into result
  uchar *const tmp= dst;
#else
  // Otherwise use a temporary buffer to make byte-swapping easier later
  uchar tmp[8];
#endif

  memcpy(tmp, from, size);

  if (tmp[0] & 0x80)
  {
    // If the high bit is set the original value was positive so
    // remove the high bit and subtract one from the exponent.
    ushort exp_part= ((ushort) tmp[0] << 8) | (ushort) tmp[1];
    exp_part&= 0x7FFF;                             // clear high bit;
    exp_part-= (ushort) 1 << (16 - 1 - exp_digit); // subtract from exponent
    tmp[0]= (uchar) (exp_part >> 8);
    tmp[1]= (uchar) exp_part;
  }
  else
  {
    // Otherwise the original value was negative and all bytes have been
    // negated.
    for (size_t ii= 0; ii < size; ii++)
      tmp[ii]^= 0xFF;
  }

#if !defined(WORDS_BIGENDIAN)
  // On little-endian, swap the bytes around
  swap_func(dst, tmp);
#else
  DBUG_ASSERT(swap_func == nullptr);
#endif

  return UNPACK_SUCCESS;
}

#if !defined(DBL_EXP_DIG)
#define DBL_EXP_DIG (sizeof(double) * 8 - DBL_MANT_DIG)
#endif

/*
  Function of type mono_index_field_unpack_t

  Unpack a double by doing the reverse action of change_double_for_sort
  (sql/filesort.cc).  Note that this only works on IEEE values.
  Note also that this code assumes that NaN and +/-Infinity are never
  allowed in the database.
*/
int mono_key_def::unpack_double(
    mono_field_packing *const fpi MY_ATTRIBUTE((__unused__)),
    Field *const field MY_ATTRIBUTE((__unused__)), uchar *const field_ptr,
    mono_string_reader *const reader,
    mono_string_reader *const unp_reader MY_ATTRIBUTE((__unused__)))
{
  static double zero_val= 0.0;
  static const uchar zero_pattern[8]= {128, 0, 0, 0, 0, 0, 0, 0};

  return unpack_floating_point(field_ptr, reader, sizeof(double), DBL_EXP_DIG,
                               zero_pattern, (const uchar *) &zero_val,
                               mono_swap_double_bytes);
}

/*
  Function of type mono_index_field_unpack_t, used to
  Unpack the bit by copying it over.
  See Field_bit::unpack_bit for more details.
*/
int mono_key_def::unpack_bit(mono_field_packing *const fpi, Field *const field,
                             uchar *const to, mono_string_reader *const reader,
                             mono_string_reader *const unp_reader
                                 MY_ATTRIBUTE((__unused__)))
{
  DBUG_ASSERT(fpi != nullptr);
  DBUG_ASSERT(field != nullptr);
  const char *from;
  if (!(from= reader->read(fpi->m_max_image_len)))
  {
    /* Mem-comparable image doesn't have enough bytes */
    return UNPACK_FAILURE;
  }
  auto *field_bit= static_cast<Field_bit *>(field);
  if (field_bit->bit_len > 0)
  {
    /* uneven high bits */
    set_rec_bits(*from, field_bit->bit_ptr, field_bit->bit_ofs,
                 field_bit->bit_len);
    from++;
  }
  /* copy the rest */
  uint data_length=
      std::min((uint) fpi->m_max_image_len, field_bit->bytes_in_rec);
  memcpy(to, from, data_length);
  return UNPACK_SUCCESS;
}

#if !defined(FLT_EXP_DIG)
#define FLT_EXP_DIG (sizeof(float) * 8 - FLT_MANT_DIG)
#endif

/*
  Function of type mono_index_field_unpack_t

  Unpack a float by doing the reverse action of Field_float::make_sort_key
  (sql/field.cc).  Note that this only works on IEEE values.
  Note also that this code assumes that NaN and +/-Infinity are never
  allowed in the database.
*/
int mono_key_def::unpack_float(
    mono_field_packing *const fpi,
    Field *const field MY_ATTRIBUTE((__unused__)), uchar *const field_ptr,
    mono_string_reader *const reader,
    mono_string_reader *const unp_reader MY_ATTRIBUTE((__unused__)))
{
  static float zero_val= 0.0;
  static const uchar zero_pattern[4]= {128, 0, 0, 0};

  return unpack_floating_point(field_ptr, reader, sizeof(float), FLT_EXP_DIG,
                               zero_pattern, (const uchar *) &zero_val,
                               mono_swap_float_bytes);
}

/*
  Function of type mono_index_field_unpack_t used to
  Unpack by doing the reverse action to Field_newdate::make_sort_key.
*/

int mono_key_def::unpack_newdate(
    mono_field_packing *const fpi,
    Field *const field MY_ATTRIBUTE((__unused__)), uchar *const field_ptr,
    mono_string_reader *const reader,
    mono_string_reader *const unp_reader MY_ATTRIBUTE((__unused__)))
{
  const char *from;
  DBUG_ASSERT(fpi->m_max_image_len == 3);

  if (!(from= reader->read(3)))
  {
    /* Mem-comparable image doesn't have enough bytes */
    return UNPACK_FAILURE;
  }

  field_ptr[0]= from[2];
  field_ptr[1]= from[1];
  field_ptr[2]= from[0];
  return UNPACK_SUCCESS;
}

/*
  Function of type mono_index_field_unpack_t, used to
  Unpack the string by copying it over.
  This is for BINARY(n) where the value occupies the whole length.
*/

int mono_key_def::unpack_binary_str(
    mono_field_packing *const fpi, Field *const field, uchar *const to,
    mono_string_reader *const reader,
    mono_string_reader *const unp_reader MY_ATTRIBUTE((__unused__)))
{
  const char *from;
  if (!(from= reader->read(fpi->m_max_image_len)))
  {
    /* Mem-comparable image doesn't have enough bytes */
    return UNPACK_FAILURE;
  }

  memcpy(to, from, fpi->m_max_image_len);
  return UNPACK_SUCCESS;
}

void mono_key_def::pack_blob(
    mono_field_packing *const fpi, Field *const field,
    uchar *const buf MY_ATTRIBUTE((__unused__)), uchar **dst,
    mono_pack_field_context *const pack_ctx MY_ATTRIBUTE((__unused__)))
{
  DBUG_ASSERT(fpi != nullptr);
  DBUG_ASSERT(field != nullptr);
  DBUG_ASSERT(dst != nullptr);
  DBUG_ASSERT(*dst != nullptr);

  uchar *to= *dst;
  uint length= fpi->m_max_image_len;

  auto field_blob= static_cast<Field_blob *>(field);
  auto field_charset= field->charset();
  auto packlength= field_blob->pack_length_no_ptr();
  auto ptr= field->ptr;

  uchar *blob;

  uint blob_length= field_blob->get_length();

  if (!blob_length && field_charset->pad_char == 0)
  {
    memset(to, 0, length);
  }
  else
  {
    if (field_charset == &my_charset_bin)
    {
      uchar *pos;

      /*
        Store length of blob last in blob to shorter blobs before longer blobs
      */
      length-= packlength;
      pos= to + length;
      uint key_length= MY_MIN(blob_length, length);

      switch (packlength)
      {
      case 1:
        *pos= static_cast<char>(key_length);
        break;
      case 2:
        mi_int2store(pos, key_length);
        break;
      case 3:
        mi_int3store(pos, key_length);
        break;
      case 4:
        mi_int4store(pos, key_length);
        break;
      }
    }
    memcpy(&blob, ptr + packlength, sizeof(char *));

    size_t char_num= length / field_charset->mbmaxlen;
    blob_length= field_charset->coll->strnxfrm(field_charset, to, length,
                                               char_num, blob, blob_length,
                                               MY_STRXFRM_PAD_TO_MAXLEN);
    DBUG_ASSERT(blob_length == length);
  }

  *dst+= fpi->m_max_image_len;
}

/*
  Function of type mono_index_field_unpack_t.
  For UTF-8, we need to convert 2- or 3-byte wide-character entities back into
  UTF8 sequences.
*/
template <const int bytes>
int unpack_utf8_str_templ(mono_field_packing *const fpi, Field *const field,
                          uchar *dst, mono_string_reader *const reader,
                          mono_string_reader *const unp_reader
                              MY_ATTRIBUTE((__unused__)))
{
  CHARSET_INFO *const cset= (CHARSET_INFO *) field->charset();
  const uchar *src;
  if (!(src= (const uchar *) reader->read(fpi->m_max_image_len)))
  {
    /* Mem-comparable image doesn't have enough bytes */
    return UNPACK_FAILURE;
  }

  const uchar *const src_end= src + fpi->m_max_image_len;
  uchar *const dst_end= dst + field->pack_length();

  while (src < src_end)
  {
    my_wc_t wc= (bytes == 3) ? (src[0] << 16) | (src[1] << 8) | src[2]
                             : (src[0] << 8) | src[1];
    src+= bytes;
    int res= cset->cset->wc_mb(cset, wc, dst, dst_end);
    assert(res > 0 && res <= bytes + 1);
    if (res < 0)
      return UNPACK_FAILURE;
    dst+= res;
  }

  cset->cset->fill(cset, reinterpret_cast<char *>(dst), dst_end - dst,
                   cset->pad_char);
  return UNPACK_SUCCESS;
}

mono_index_field_unpack_t mono_key_def::unpack_utf8mb4_str=
    unpack_utf8_str_templ<3>;
mono_index_field_unpack_t mono_key_def::unpack_utf8_str=
    unpack_utf8_str_templ<2>;

/*
  This is the new algorithm.  Similarly to the legacy format the input
  is split up into N-1 bytes and a flag byte is used as the Nth byte
  in the output.

  - If the previous segment needed any padding the flag is set to the
    number of bytes used (0..N-2).  0 is possible in the first segment
    if the input is 0 bytes long.
  - If no padding was used and there is no more data left in the input
    the flag is set to N-1
  - If no padding was used and there is still data left in the input the
    flag is set to N.

  For N=9, the following input values encode to the specified
  outout (where 'X' indicates a byte of the original input):
  - 0 bytes  is encoded as 0 0 0 0 0 0 0 0 0
  - 1 byte   is encoded as X 0 0 0 0 0 0 0 1
  - 2 bytes  is encoded as X X 0 0 0 0 0 0 2
  - 7 bytes  is encoded as X X X X X X X 0 7
  - 8 bytes  is encoded as X X X X X X X X 8
  - 9 bytes  is encoded as X X X X X X X X 9 X 0 0 0 0 0 0 0 1
  - 10 bytes is encoded as X X X X X X X X 9 X X 0 0 0 0 0 0 2
*/
void mono_key_def::pack_variable_format(
    const uchar *src, // The data to encode
    size_t src_len,   // The length of the data to encode
    uchar **dst)      // The location to encode the data
{
  uchar *ptr= *dst;

  for (;;)
  {
    // Figure out how many bytes to copy, copy them and adjust pointers
    const size_t copy_len= std::min((size_t) MONO_ESCAPE_LENGTH - 1, src_len);
    memcpy(ptr, src, copy_len);
    ptr+= copy_len;
    src+= copy_len;
    src_len-= copy_len;

    // Are we at the end of the input?
    if (src_len == 0)
    {
      // pad with zeros if necessary;
      const size_t padding_bytes= MONO_ESCAPE_LENGTH - 1 - copy_len;
      if (padding_bytes > 0)
      {
        memset(ptr, 0, padding_bytes);
        ptr+= padding_bytes;
      }

      // Put the flag byte (0 - N-1) in the output
      *(ptr++)= (uchar) copy_len;
      break;
    }

    // We have more data - put the flag byte (N) in and continue
    *(ptr++)= MONO_ESCAPE_LENGTH;
  }

  *dst= ptr;
}

/*
  Function of type mono_index_field_pack_t
*/

void mono_key_def::pack_with_varchar_encoding(
    mono_field_packing *const fpi, Field *const field, uchar *buf, uchar **dst,
    mono_pack_field_context *const pack_ctx MY_ATTRIBUTE((__unused__)))
{
  const CHARSET_INFO *const charset= field->charset();
  Field_varstring *const field_var= (Field_varstring *) field;

  const size_t value_length= (field_var->length_bytes == 1)
                                 ? (uint) *field->ptr
                                 : uint2korr(field->ptr);
  size_t xfrm_len= charset->strnxfrm(
      buf, fpi->m_max_image_len, field_var->char_length(),
      field_var->ptr + field_var->length_bytes, value_length, 0);

  /* Got a mem-comparable image in 'buf'. Now, produce varlength encoding */

  pack_variable_format(buf, xfrm_len, dst);
}

/*
  Compare the string in [buf..buf_end) with a string that is an infinite
  sequence of strings in space_xfrm
*/

static int
mono_compare_string_with_spaces(const uchar *buf, const uchar *const buf_end,
                                const std::vector<uchar> *const space_xfrm)
{
  int cmp= 0;
  while (buf < buf_end)
  {
    size_t bytes= std::min((size_t) (buf_end - buf), space_xfrm->size());
    if ((cmp= memcmp(buf, space_xfrm->data(), bytes)) != 0)
      break;
    buf+= bytes;
  }
  return cmp;
}

static const int mono_TRIMMED_CHARS_OFFSET= 8;
/*
  Pack the data with Variable-Length Space-Padded Encoding.

  The encoding is there to meet two goals:

  Goal#1. Comparison. The SQL standard says

    " If the collation for the comparison has the PAD SPACE characteristic,
    for the purposes of the comparison, the shorter value is effectively
    extended to the length of the longer by concatenation of <space>s on the
    right.

  At the moment, all MySQL collations except one have the PAD SPACE
  characteristic.  The exception is the "binary" collation that is used by
  [VAR]BINARY columns. (Note that binary collations for specific charsets,
  like utf8_bin or latin1_bin are not the same as "binary" collation, they have
  the PAD SPACE characteristic).

  Goal#2 is to preserve the number of trailing spaces in the original value.

  This is achieved by using the following encoding:
  The key part:
  - Stores mem-comparable image of the column
  - It is stored in chunks of fpi->m_segment_size bytes (*)
    = If the remainder of the chunk is not occupied, it is padded with mem-
      comparable image of the space character (cs->pad_char to be precise).
  - The last byte of the chunk shows how the rest of column's mem-comparable
    image would compare to mem-comparable image of the column extended with
    spaces. There are three possible values.
     - VARCHAR_CMP_LESS_THAN_SPACES,
     - VARCHAR_CMP_EQUAL_TO_SPACES
     - VARCHAR_CMP_GREATER_THAN_SPACES

  VARCHAR_CMP_EQUAL_TO_SPACES means that this chunk is the last one (the rest
  is spaces, or something that sorts as spaces, so there is no reason to store
  it).

  Example: if fpi->m_segment_size=5, and the collation is latin1_bin:

   'abcd\0'   => [ 'abcd' <VARCHAR_CMP_LESS> ]['\0    ' <VARCHAR_CMP_EQUAL> ]
   'abcd'     => [ 'abcd' <VARCHAR_CMP_EQUAL>]
   'abcd   '  => [ 'abcd' <VARCHAR_CMP_EQUAL>]
   'abcdZZZZ' => [ 'abcd' <VARCHAR_CMP_GREATER>][ 'ZZZZ' <VARCHAR_CMP_EQUAL>]

  As mentioned above, the last chunk is padded with mem-comparable images of
  cs->pad_char. It can be 1-byte long (latin1), 2 (utf8_bin), 3 (utf8mb4), etc.

  fpi->m_segment_size depends on the used collation. It is chosen to be such
  that no mem-comparable image of space will ever stretch across the segments
  (see get_segment_size_from_collation).

  == The value part (aka unpack_info) ==
  The value part stores the number of space characters that one needs to add
  when unpacking the string.
  - If the number is positive, it means add this many spaces at the end
  - If the number is negative, it means padding has added extra spaces which
    must be removed.

  Storage considerations
  - depending on column's max size, the number may occupy 1 or 2 bytes
  - the number of spaces that need to be removed is not more than
    mono_TRIMMED_CHARS_OFFSET=8, so we offset the number by that value and
    then store it as unsigned.

  @seealso
    unpack_binary_or_utf8_varchar_space_pad
    unpack_simple_varchar_space_pad
    dummy_make_unpack_info
    skip_variable_space_pad
*/

void mono_key_def::pack_with_varchar_space_pad(
    mono_field_packing *const fpi, Field *const field, uchar *buf, uchar **dst,
    mono_pack_field_context *const pack_ctx)
{
  mono_string_writer *const unpack_info= pack_ctx->writer;
  const CHARSET_INFO *const charset= field->charset();
  const auto field_var= static_cast<Field_varstring *>(field);

  const size_t value_length= (field_var->length_bytes == 1)
                                 ? (uint) *field->ptr
                                 : uint2korr(field->ptr);

  const size_t trimmed_len= charset->lengthsp(
      (const char *) field_var->ptr + field_var->length_bytes, value_length);
  const size_t xfrm_len= charset->strnxfrm(
      buf, fpi->m_max_image_len, field_var->char_length(),
      field_var->ptr + field_var->length_bytes, trimmed_len, 0);

  /* Got a mem-comparable image in 'buf'. Now, produce varlength encoding */
  uchar *const buf_end= buf + xfrm_len;

  size_t encoded_size= 0;
  uchar *ptr= *dst;
  size_t padding_bytes;
  while (true)
  {
    const size_t copy_len=
        std::min<size_t>(fpi->m_segment_size - 1, buf_end - buf);
    padding_bytes= fpi->m_segment_size - 1 - copy_len;
    memcpy(ptr, buf, copy_len);
    ptr+= copy_len;
    buf+= copy_len;

    if (padding_bytes)
    {
      memcpy(ptr, fpi->space_xfrm->data(), padding_bytes);
      ptr+= padding_bytes;
      *ptr= VARCHAR_CMP_EQUAL_TO_SPACES; // last segment
    }
    else
    {
      // Compare the string suffix with a hypothetical infinite string of
      // spaces. It could be that the first difference is beyond the end of
      // current chunk.
      const int cmp=
          mono_compare_string_with_spaces(buf, buf_end, fpi->space_xfrm);

      if (cmp < 0)
      {
        *ptr= VARCHAR_CMP_LESS_THAN_SPACES;
      }
      else if (cmp > 0)
      {
        *ptr= VARCHAR_CMP_GREATER_THAN_SPACES;
      }
      else
      {
        // It turns out all the rest are spaces.
        *ptr= VARCHAR_CMP_EQUAL_TO_SPACES;
      }
    }
    encoded_size+= fpi->m_segment_size;

    if (*(ptr++) == VARCHAR_CMP_EQUAL_TO_SPACES)
      break;
  }

  // m_unpack_info_stores_value means unpack_info stores the whole original
  // value. There is no need to store the number of trimmed/padded endspaces
  // in that case.
  if (unpack_info && !fpi->m_unpack_info_stores_value)
  {
    // (value_length - trimmed_len) is the number of trimmed space *characters*
    // then, padding_bytes is the number of *bytes* added as padding
    // then, we add 8, because we don't store negative values.
    DBUG_ASSERT(padding_bytes % fpi->space_xfrm_len == 0);
    DBUG_ASSERT((value_length - trimmed_len) % fpi->space_mb_len == 0);
    const size_t removed_chars=
        mono_TRIMMED_CHARS_OFFSET +
        (value_length - trimmed_len) / fpi->space_mb_len -
        padding_bytes / fpi->space_xfrm_len;

    if (fpi->m_unpack_info_uses_two_bytes)
    {
      unpack_info->write_uint16(removed_chars);
    }
    else
    {
      DBUG_ASSERT(removed_chars < 0x100);
      unpack_info->write_uint8(removed_chars);
    }
  }

  *dst+= encoded_size;
}

/*
  Calculate the number of used bytes in the chunk and whether this is the
  last chunk in the input.  This is based on the new format - see
  pack_variable_format.
 */
uint mono_key_def::calc_unpack_variable_format(uchar flag, bool *done)
{
  // Check for invalid flag values
  if (flag > MONO_ESCAPE_LENGTH)
  {
    return (uint) -1;
  }

  // Values from 1 to N-1 indicate this is the last chunk and that is how
  // many bytes were used
  if (flag < MONO_ESCAPE_LENGTH)
  {
    *done= true;
    return flag;
  }

  // A value of N means we used N-1 bytes and had more to go
  *done= false;
  return MONO_ESCAPE_LENGTH - 1;
}

/*
  Unpack data that has charset information.  Each two bytes of the input is
  treated as a wide-character and converted to its multibyte equivalent in
  the output.
 */
static int
unpack_charset(const CHARSET_INFO *cset, // character set information
               const uchar *src,         // source data to unpack
               uint src_len,             // length of source data
               uchar *dst,               // destination of unpacked data
               uint dst_len,             // length of destination data
               uint *used_bytes)         // output number of bytes used
{
  if (src_len & 1)
  {
    /*
      UTF-8 characters are encoded into two-byte entities. There is no way
      we can have an odd number of bytes after encoding.
    */
    return UNPACK_FAILURE;
  }

  uchar *dst_end= dst + dst_len;
  uint used= 0;

  for (uint ii= 0; ii < src_len; ii+= 2)
  {
    my_wc_t wc= (src[ii] << 8) | src[ii + 1];
    int res= cset->wc_mb(wc, dst + used, dst_end);
    DBUG_ASSERT(res > 0 && res <= 3);
    if (res < 0)
    {
      return UNPACK_FAILURE;
    }

    used+= res;
  }

  *used_bytes= used;
  return UNPACK_SUCCESS;
}

/*
  Function of type mono_index_field_unpack_t
*/

int mono_key_def::unpack_binary_or_utf8_varchar(
    mono_field_packing *const fpi, Field *const field, uchar *dst,
    mono_string_reader *const reader,
    mono_string_reader *const unp_reader MY_ATTRIBUTE((__unused__)))
{
  const uchar *ptr;
  size_t len= 0;
  bool finished= false;
  uchar *d0= dst;
  Field_varstring *const field_var= (Field_varstring *) field;
  dst+= field_var->length_bytes;
  // How much we can unpack
  size_t dst_len= field_var->pack_length() - field_var->length_bytes;

  /* Decode the length-emitted encoding here */
  while ((ptr= (const uchar *) reader->read(MONO_ESCAPE_LENGTH)))
  {
    uint used_bytes;

    /* See pack_with_varchar_encoding. */
    used_bytes=
        calc_unpack_variable_format(ptr[MONO_ESCAPE_LENGTH - 1], &finished);

    if (used_bytes == (uint) -1 || dst_len < used_bytes)
    {
      return UNPACK_FAILURE; // Corruption in the data
    }

    /*
      Now, we need to decode used_bytes of data and append them to the value.
    */
    if (fpi->m_varchar_charset->number == COLLATION_UTF8_BIN)
    {
      int err= unpack_charset(fpi->m_varchar_charset, ptr, used_bytes, dst,
                              dst_len, &used_bytes);
      if (err != UNPACK_SUCCESS)
      {
        return err;
      }
    }
    else
    {
      memcpy(dst, ptr, used_bytes);
    }

    dst+= used_bytes;
    dst_len-= used_bytes;
    len+= used_bytes;

    if (finished)
    {
      break;
    }
  }

  if (!finished)
  {
    return UNPACK_FAILURE;
  }

  /* Save the length */
  if (field_var->length_bytes == 1)
  {
    d0[0]= (uchar) len;
  }
  else
  {
    DBUG_ASSERT(field_var->length_bytes == 2);
    int2store(d0, len);
  }
  return UNPACK_SUCCESS;
}

template <const int bytes> bool check_src_len(uint src_len);

template <> bool check_src_len<2>(uint src_len)
{
  if (src_len & 1)
  {
    /*
      utf8mb3 characters are encoded into two-byte entities. There is no way
      we can have an odd number of bytes after encoding.
    */
    return false;
  }
  return true;
}

template <> bool check_src_len<3>(uint src_len)
{
  if (src_len % 3)
  {
    /*
      utf8mb4 characters are encoded into three-byte entities. There is no way
      we can have 1 or 2 bytes after encoding.
    */
    return false;
  }
  return true;
}

/*
  @seealso
    pack_with_varchar_space_pad - packing function
    unpack_simple_varchar_space_pad - unpacking function for 'simple'
    charsets.
    skip_variable_space_pad - skip function
*/

template <const int bytes>
int mono_key_def::unpack_binary_or_utf8_varchar_space_pad(
    mono_field_packing *const fpi, Field *const field, uchar *dst,
    mono_string_reader *const reader, mono_string_reader *const unp_reader)
{
  const uchar *ptr;
  size_t len= 0;
  bool finished= false;
  Field_varstring *const field_var= static_cast<Field_varstring *>(field);
  uchar *d0= dst;
  uchar *dst_end= dst + field_var->pack_length();
  dst+= field_var->length_bytes;

  uint space_padding_bytes= 0;
  uint extra_spaces;
  if ((fpi->m_unpack_info_uses_two_bytes
           ? unp_reader->read_uint16(&extra_spaces)
           : unp_reader->read_uint8(&extra_spaces)))
  {
    return UNPACK_FAILURE;
  }

  if (extra_spaces <= mono_TRIMMED_CHARS_OFFSET)
  {
    space_padding_bytes=
        -(static_cast<int>(extra_spaces) - mono_TRIMMED_CHARS_OFFSET);
    extra_spaces= 0;
  }
  else
  {
    extra_spaces-= mono_TRIMMED_CHARS_OFFSET;
  }

  space_padding_bytes*= fpi->space_xfrm_len;

  // Check if lead segment byte is VARCHAR_CMP_EQUAL_TO_SPACES.
  // This indicates empty content or just spaces. We can bypass
  // the main loop and check for spaces to be appended.
  // TODO: encoded byte does not exist in old pack func?
  // uchar encoded_byte = *(const uchar *)reader->read(1);
  // if (encoded_byte == VARCHAR_CMP_EQUAL_TO_SPACES)
  //  goto finished;

  /* Decode the length-emitted encoding here */
  while ((ptr= (const uchar *) reader->read(fpi->m_segment_size)))
  {
    const char last_byte= ptr[fpi->m_segment_size - 1];
    size_t used_bytes;
    if (last_byte == VARCHAR_CMP_EQUAL_TO_SPACES) // this is the last segment
    {
      if (space_padding_bytes > (fpi->m_segment_size - 1))
      {
        return UNPACK_FAILURE; // Cannot happen, corrupted data
      }
      used_bytes= (fpi->m_segment_size - 1) - space_padding_bytes;
      finished= true;
    }
    else
    {
      if (last_byte != VARCHAR_CMP_LESS_THAN_SPACES &&
          last_byte != VARCHAR_CMP_GREATER_THAN_SPACES)
      {
        return UNPACK_FAILURE; // Invalid value
      }
      used_bytes= fpi->m_segment_size - 1;
    }

    // Now, need to decode used_bytes of data and append them to the value.
    if (bytes > 1)
    {
      if (!check_src_len<bytes>(used_bytes))
        return UNPACK_FAILURE;

      const uchar *src= ptr;
      const uchar *const src_end= ptr + used_bytes;
      while (src < src_end)
      {
        my_wc_t wc= (bytes == 3) ? (src[0] << 16) | (src[1] << 8) | src[2]
                                 : (src[0] << 8) | src[1];
        src+= bytes;
        const CHARSET_INFO *cset= fpi->m_varchar_charset;
        int res= cset->cset->wc_mb(cset, wc, dst, dst_end);
        assert(res <= bytes + 1);
        if (res <= 0)
          return UNPACK_FAILURE;
        dst+= res;
        len+= res;
      }
    }
    else
    {
      if (dst + used_bytes > dst_end)
        return UNPACK_FAILURE;
      memcpy(dst, ptr, used_bytes);
      dst+= used_bytes;
      len+= used_bytes;
    }

    if (finished)
      break;
  }

  if (!finished)
    return UNPACK_FAILURE;

  // finished:

  if (extra_spaces)
  {
    // Both binary and UTF-8 charset store space as ' ',
    // so the following is ok:
    if (dst + extra_spaces > dst_end)
      return UNPACK_FAILURE;
    memset(dst, fpi->m_varchar_charset->pad_char, extra_spaces);
    len+= extra_spaces;
  }
  /* Save the length */
  if (field_var->length_bytes == 1)
  {
    d0[0]= (uchar) len;
  }
  else
  {
    DBUG_ASSERT(field_var->length_bytes == 2);
    int2store(d0, len);
  }
  return UNPACK_SUCCESS;
}

mono_index_field_unpack_t mono_key_def::unpack_binary_varchar_space_pad=
    unpack_binary_or_utf8_varchar_space_pad<1>;
mono_index_field_unpack_t mono_key_def::unpack_utf8_varchar_space_pad=
    unpack_binary_or_utf8_varchar_space_pad<2>;
mono_index_field_unpack_t mono_key_def::unpack_utf8mb4_varchar_space_pad=
    unpack_binary_or_utf8_varchar_space_pad<3>;

/////////////////////////////////////////////////////////////////////////

/*
  Function of type mono_make_unpack_info_t
*/

void mono_key_def::make_unpack_unknown(
    const mono_collation_codec *codec MY_ATTRIBUTE((__unused__)),
    const Field *const field, mono_pack_field_context *const pack_ctx)
{
  pack_ctx->writer->write(field->ptr, field->pack_length());
}

/*
  This point of this function is only to indicate that unpack_info is
  available.

  The actual unpack_info data is produced by the function that packs the key,
  that is, pack_with_varchar_space_pad.
*/

void mono_key_def::dummy_make_unpack_info(
    const mono_collation_codec *codec MY_ATTRIBUTE((__unused__)),
    const Field *field MY_ATTRIBUTE((__unused__)),
    mono_pack_field_context *pack_ctx MY_ATTRIBUTE((__unused__)))
{
  // Do nothing
}

/*
  Function of type mono_index_field_unpack_t
*/

int mono_key_def::unpack_unknown(mono_field_packing *const fpi,
                                 Field *const field, uchar *const dst,
                                 mono_string_reader *const reader,
                                 mono_string_reader *const unp_reader)
{
  const uchar *ptr;
  const uint len= fpi->m_unpack_data_len;
  // We don't use anything from the key, so skip over it.
  if (skip_max_length(fpi, field, reader))
  {
    return UNPACK_FAILURE;
  }

  if ((ptr= (const uchar *) unp_reader->read(len)))
  {
    memcpy(dst, ptr, len);
    return UNPACK_SUCCESS;
  }
  return UNPACK_FAILURE;
}

/*
  Function of type mono_make_unpack_info_t
*/

void mono_key_def::make_unpack_unknown_varchar(
    const mono_collation_codec *const codec MY_ATTRIBUTE((__unused__)),
    const Field *const field, mono_pack_field_context *const pack_ctx)
{
  const auto f= static_cast<const Field_varstring *>(field);
  uint len= f->length_bytes == 1 ? (uint) *f->ptr : uint2korr(f->ptr);
  len+= f->length_bytes;
  pack_ctx->writer->write(field->ptr, len);
}

/*
  Function of type mono_index_field_unpack_t

  @detail
  Unpack a key part in an "unknown" collation from its
  (mem_comparable_form, unpack_info) form.

  "Unknown" means we have no clue about how mem_comparable_form is made from
  the original string, so we keep the whole original string in the unpack_info.

  @seealso
    make_unpack_unknown, unpack_unknown
*/

int mono_key_def::unpack_unknown_varchar(mono_field_packing *const fpi,
                                         Field *const field, uchar *dst,
                                         mono_string_reader *const reader,
                                         mono_string_reader *const unp_reader)
{
  const uchar *ptr;
  uchar *const d0= dst;
  const auto f= static_cast<Field_varstring *>(field);
  dst+= f->length_bytes;
  const uint len_bytes= f->length_bytes;
  // We don't use anything from the key, so skip over it.
  if ((fpi->m_skip_func)(fpi, field, reader))
  {
    return UNPACK_FAILURE;
  }

  DBUG_ASSERT(len_bytes > 0);
  DBUG_ASSERT(unp_reader != nullptr);

  if ((ptr= (const uchar *) unp_reader->read(len_bytes)))
  {
    memcpy(d0, ptr, len_bytes);
    const uint len= len_bytes == 1 ? (uint) *ptr : uint2korr(ptr);
    if ((ptr= (const uchar *) unp_reader->read(len)))
    {
      memcpy(dst, ptr, len);
      return UNPACK_SUCCESS;
    }
  }
  return UNPACK_FAILURE;
}

/*
  Write unpack_data for a "simple" collation
*/
static void mono_write_unpack_simple(mono_bit_writer *const writer,
                                     const mono_collation_codec *const codec,
                                     const uchar *const src,
                                     const size_t src_len)
{
  for (uint i= 0; i < src_len; i++)
  {
    writer->write(codec->m_enc_size[src[i]], codec->m_enc_idx[src[i]]);
  }
}

static uint mono_read_unpack_simple(mono_bit_reader *const reader,
                                    const mono_collation_codec *const codec,
                                    const uchar *const src,
                                    const size_t src_len, uchar *const dst)
{
  for (uint i= 0; i < src_len; i++)
  {
    if (codec->m_dec_size[src[i]] > 0)
    {
      uint *ret;
      DBUG_ASSERT(reader != nullptr);

      if ((ret= reader->read(codec->m_dec_size[src[i]])) == nullptr)
      {
        return UNPACK_FAILURE;
      }
      dst[i]= codec->m_dec_idx[*ret][src[i]];
    }
    else
    {
      dst[i]= codec->m_dec_idx[0][src[i]];
    }
  }

  return UNPACK_SUCCESS;
}

/*
  Function of type mono_make_unpack_info_t

  @detail
    Make unpack_data for VARCHAR(n) in a "simple" charset.
*/

void mono_key_def::make_unpack_simple_varchar(
    const mono_collation_codec *const codec, const Field *const field,
    mono_pack_field_context *const pack_ctx)
{
  const auto f= static_cast<const Field_varstring *>(field);
  uchar *const src= f->ptr + f->length_bytes;
  const size_t src_len=
      f->length_bytes == 1 ? (uint) *f->ptr : uint2korr(f->ptr);
  mono_bit_writer bit_writer(pack_ctx->writer);
  // The std::min compares characters with bytes, but for simple collations,
  // mbmaxlen = 1.
  mono_write_unpack_simple(&bit_writer, codec, src,
                           std::min((size_t) f->char_length(), src_len));
}

/*
  Function of type mono_index_field_unpack_t

  @seealso
    pack_with_varchar_space_pad - packing function
    unpack_binary_or_utf8_varchar_space_pad - a similar unpacking function
*/

int mono_key_def::unpack_simple_varchar_space_pad(
    mono_field_packing *const fpi, Field *const field, uchar *dst,
    mono_string_reader *const reader, mono_string_reader *const unp_reader)
{
  const uchar *ptr;
  size_t len= 0;
  bool finished= false;
  uchar *d0= dst;
  const Field_varstring *const field_var=
      static_cast<Field_varstring *>(field);
  // For simple collations, char_length is also number of bytes.
  DBUG_ASSERT((size_t) fpi->m_max_image_len >= field_var->char_length());
  uchar *dst_end= dst + field_var->pack_length();
  dst+= field_var->length_bytes;
  mono_bit_reader bit_reader(unp_reader);

  uint space_padding_bytes= 0;
  uint extra_spaces;
  DBUG_ASSERT(unp_reader != nullptr);

  if ((fpi->m_unpack_info_uses_two_bytes
           ? unp_reader->read_uint16(&extra_spaces)
           : unp_reader->read_uint8(&extra_spaces)))
  {
    return UNPACK_FAILURE;
  }

  if (extra_spaces <= 8)
  {
    space_padding_bytes= -(static_cast<int>(extra_spaces) - 8);
    extra_spaces= 0;
  }
  else
  {
    extra_spaces-= 8;
  }

  space_padding_bytes*= fpi->space_xfrm_len;

  /* Decode the length-emitted encoding here */
  while ((ptr= (const uchar *) reader->read(fpi->m_segment_size)))
  {
    const char last_byte=
        ptr[fpi->m_segment_size - 1]; // number of padding bytes
    size_t used_bytes;
    if (last_byte == VARCHAR_CMP_EQUAL_TO_SPACES)
    {
      // this is the last one
      if (space_padding_bytes > (fpi->m_segment_size - 1))
      {
        return UNPACK_FAILURE; // Cannot happen, corrupted data
      }
      used_bytes= (fpi->m_segment_size - 1) - space_padding_bytes;
      finished= true;
    }
    else
    {
      if (last_byte != VARCHAR_CMP_LESS_THAN_SPACES &&
          last_byte != VARCHAR_CMP_GREATER_THAN_SPACES)
      {
        return UNPACK_FAILURE;
      }
      used_bytes= fpi->m_segment_size - 1;
    }

    if (dst + used_bytes > dst_end)
    {
      // The value on disk is longer than the field definition allows?
      return UNPACK_FAILURE;
    }

    uint ret;
    if ((ret= mono_read_unpack_simple(&bit_reader, fpi->m_charset_codec, ptr,
                                      used_bytes, dst)) != UNPACK_SUCCESS)
    {
      return ret;
    }

    dst+= used_bytes;
    len+= used_bytes;

    if (finished)
    {
      if (extra_spaces)
      {
        if (dst + extra_spaces > dst_end)
          return UNPACK_FAILURE;
        // pad_char has a 1-byte form in all charsets that
        // are handled by mono_init_collation_mapping.
        memset(dst, field_var->charset()->pad_char, extra_spaces);
        len+= extra_spaces;
      }
      break;
    }
  }

  if (!finished)
    return UNPACK_FAILURE;

  /* Save the length */
  if (field_var->length_bytes == 1)
  {
    d0[0]= (uchar) len;
  }
  else
  {
    DBUG_ASSERT(field_var->length_bytes == 2);
    int2store(d0, len);
  }
  return UNPACK_SUCCESS;
}

/*
  Function of type mono_make_unpack_info_t

  @detail
    Make unpack_data for CHAR(n) value in a "simple" charset.
    It is CHAR(N), so SQL layer has padded the value with spaces up to N chars.

  @seealso
    The VARCHAR variant is in make_unpack_simple_varchar
*/

void mono_key_def::make_unpack_simple(const mono_collation_codec *const codec,
                                      const Field *const field,
                                      mono_pack_field_context *const pack_ctx)
{
  const uchar *const src= field->ptr;
  mono_bit_writer bit_writer(pack_ctx->writer);
  mono_write_unpack_simple(&bit_writer, codec, src, field->pack_length());
}

/*
  Function of type mono_index_field_unpack_t
*/

int mono_key_def::unpack_simple(mono_field_packing *const fpi,
                                Field *const field MY_ATTRIBUTE((__unused__)),
                                uchar *const dst,
                                mono_string_reader *const reader,
                                mono_string_reader *const unp_reader)
{
  const uchar *ptr;
  const uint len= fpi->m_max_image_len;
  mono_bit_reader bit_reader(unp_reader);

  if (!(ptr= (const uchar *) reader->read(len)))
  {
    return UNPACK_FAILURE;
  }

  return mono_read_unpack_simple(unp_reader ? &bit_reader : nullptr,
                                 fpi->m_charset_codec, ptr, len, dst);
}

// See mono_charset_space_info::spaces_xfrm
const int mono_SPACE_XFRM_SIZE= 32;

// A class holding information about how space character is represented in a
// charset.
class mono_charset_space_info
{
public:
  mono_charset_space_info(const mono_charset_space_info &)= delete;
  mono_charset_space_info &operator=(const mono_charset_space_info &)= delete;
  mono_charset_space_info()= default;

  // A few strxfrm'ed space characters, at least mono_SPACE_XFRM_SIZE bytes
  std::vector<uchar> spaces_xfrm;

  // length(strxfrm(' '))
  size_t space_xfrm_len;

  // length of the space character itself
  // Typically space is just 0x20 (length=1) but in ucs2 it is 0x00 0x20
  // (length=2)
  size_t space_mb_len;
};

static std::array<std::unique_ptr<mono_charset_space_info>,
                  MY_ALL_CHARSETS_SIZE>
    mono_mem_comparable_space;

/*
  @brief
  For a given charset, get
   - strxfrm('    '), a sample that is at least mono_SPACE_XFRM_SIZE bytes
  long.
   - length of strxfrm(charset, ' ')
   - length of the space character in the charset

  @param cs  IN    Charset to get the space for
  @param ptr OUT   A few space characters
  @param len OUT   Return length of the space (in bytes)

  @detail
    It is tempting to pre-generate mem-comparable form of space character for
    every charset on server startup.
    One can't do that: some charsets are not initialized until somebody
    attempts to use them (e.g. create or open a table that has a field that
    uses the charset).
*/

static void mono_get_mem_comparable_space(const CHARSET_INFO *const cs,
                                          const std::vector<uchar> **xfrm,
                                          size_t *const xfrm_len,
                                          size_t *const mb_len)
{
  DBUG_ASSERT(cs->number < MY_ALL_CHARSETS_SIZE);
  if (!mono_mem_comparable_space[cs->number].get())
  {
    mysql_mutex_lock(&mono_mem_cmp_space_mutex);
    if (!mono_mem_comparable_space[cs->number].get())
    {
      // Upper bound of how many bytes can be occupied by multi-byte form of a
      // character in any charset.
      const int MAX_MULTI_BYTE_CHAR_SIZE= 4;
      DBUG_ASSERT(cs->mbmaxlen <= MAX_MULTI_BYTE_CHAR_SIZE);

      // multi-byte form of the ' ' (space) character
      uchar space_mb[MAX_MULTI_BYTE_CHAR_SIZE];

      const size_t space_mb_len= cs->wc_mb((my_wc_t) cs->pad_char, space_mb,
                                           space_mb + sizeof(space_mb));

      // mem-comparable image of the space character
      std::array<uchar, 20> space;

      const size_t space_len= cs->strnxfrm(space.data(), sizeof(space), 1,
                                           space_mb, space_mb_len, 0);
      mono_charset_space_info *const info= new mono_charset_space_info;
      info->space_xfrm_len= space_len;
      info->space_mb_len= space_mb_len;
      while (info->spaces_xfrm.size() < mono_SPACE_XFRM_SIZE)
      {
        info->spaces_xfrm.insert(info->spaces_xfrm.end(), space.data(),
                                 space.data() + space_len);
      }
      mono_mem_comparable_space[cs->number].reset(info);
    }
    mysql_mutex_unlock(&mono_mem_cmp_space_mutex);
  }

  *xfrm= &mono_mem_comparable_space[cs->number]->spaces_xfrm;
  *xfrm_len= mono_mem_comparable_space[cs->number]->space_xfrm_len;
  *mb_len= mono_mem_comparable_space[cs->number]->space_mb_len;
}

mysql_mutex_t mono_mem_cmp_space_mutex;

std::array<const mono_collation_codec *, MY_ALL_CHARSETS_SIZE>
    mono_collation_data;
mysql_mutex_t mono_collation_data_mutex;

bool mono_key_def::mono_is_collation_supported(const CHARSET_INFO *const cs)
{
  return cs->strxfrm_multiply == 1 && cs->mbmaxlen == 1 &&
         !(cs->state & (MY_CS_BINSORT | MY_CS_NOPAD));
}

static const mono_collation_codec *
mono_init_collation_mapping(const CHARSET_INFO *const cs)
{
  DBUG_ASSERT(cs && cs->state & MY_CS_AVAILABLE);
  const mono_collation_codec *codec= mono_collation_data[cs->number];

  if (codec == nullptr && mono_key_def::mono_is_collation_supported(cs))
  {
    mysql_mutex_lock(&mono_collation_data_mutex);

    codec= mono_collation_data[cs->number];
    if (codec == nullptr)
    {
      mono_collation_codec *cur= nullptr;

      // Compute reverse mapping for simple collations.
      if (mono_key_def::mono_is_collation_supported(cs))
      {
        cur= new mono_collation_codec;
        std::map<uchar, std::vector<uchar>> rev_map;
        size_t max_conflict_size= 0;
        for (int src= 0; src < 256; src++)
        {
          uchar dst= cs->sort_order[src];
          rev_map[dst].push_back(src);
          max_conflict_size= std::max(max_conflict_size, rev_map[dst].size());
        }
        cur->m_dec_idx.resize(max_conflict_size);

        for (auto const &p : rev_map)
        {
          uchar dst= p.first;
          for (uint idx= 0; idx < p.second.size(); idx++)
          {
            uchar src= p.second[idx];
            uchar bits=
                my_bit_log2_uint32(my_round_up_to_next_power(p.second.size()));
            cur->m_enc_idx[src]= idx;
            cur->m_enc_size[src]= bits;
            cur->m_dec_size[dst]= bits;
            cur->m_dec_idx[idx][dst]= src;
          }
        }

        cur->m_make_unpack_info_func= {
            mono_key_def::make_unpack_simple_varchar,
            mono_key_def::make_unpack_simple};
        cur->m_unpack_func= {mono_key_def::unpack_simple_varchar_space_pad,
                             mono_key_def::unpack_simple};
      }
      else
      {
        // Out of luck for now.
      }

      if (cur != nullptr)
      {
        codec= cur;
        cur->m_cs= cs;
        mono_collation_data[cs->number]= cur;
      }
    }

    mysql_mutex_unlock(&mono_collation_data_mutex);
  }

  return codec;
}

static int get_segment_size_from_collation(const CHARSET_INFO *const cs)
{
  int ret;
  if (cs->number == COLLATION_UTF8MB4_BIN)
  {
    /*
      In these collations, a character produces one weight, which is 3 bytes.
      Segment has 3 characters, add one byte for VARCHAR_CMP_* marker, and we
      get 3*3+1=10
    */
    ret= 10;
  }
  else
  {
    /*
      All other collations. There are two classes:
      - Unicode-based, except for collations mentioned in the if-condition.
        For these all weights are 2 bytes long, a character may produce 0..8
        weights.
        in any case, 8 bytes of payload in the segment guarantee that the last
        space character won't span across segments.

      - Collations not based on unicode. These have length(strxfrm(' '))=1,
        there nothing to worry about.

      In both cases, take 8 bytes payload + 1 byte for VARCHAR_CMP* marker.
    */
    ret= 9;
  }
  DBUG_ASSERT(ret < mono_SPACE_XFRM_SIZE);
  return ret;
}

/*
  @brief
    Setup packing of index field into its mem-comparable form

  @detail
    - It is possible produce mem-comparable form for any datatype.
    - Some datatypes also allow to unpack the original value from its
      mem-comparable form.
      = Some of these require extra information to be stored in "unpack_info".
        unpack_info is not a part of mem-comparable form, it is only used to
        restore the original value

  @param
    field  IN  field to be packed/un-packed

  @return
    TRUE  -  Field can be read with index-only reads
    FALSE -  Otherwise
*/

bool mono_field_packing::setup(const mono_key_def *const key_descr,
                               const Field *const field, const uint keynr_arg,
                               const uint key_part_arg,
                               const uint16 key_length)
{
  int res= false;
  enum_field_types type= field ? field->real_type() : MYSQL_TYPE_VARCHAR;

  m_keynr= keynr_arg;
  m_key_part= key_part_arg;

  m_maybe_null= field ? field->real_maybe_null() : false;
  m_unpack_func= nullptr;
  m_make_unpack_info_func= nullptr;
  m_unpack_data_len= 0;
  space_xfrm= nullptr; // safety

  m_field_idx= field ? (field->field_index + 1) : 0;
  m_Key_part_length= key_length;

  /* Calculate image length. By default, is is pack_length() */
  m_max_image_len= field ? field->pack_length() : MY_UUID_SIZE;
  m_skip_func= mono_key_def::skip_max_length;
  m_pack_func= mono_key_def::pack_with_make_sort_key;

  // Handle hidden pk col here
  if (field == nullptr)
  {
    // We should never need pack function. Hidden pk is packed with
    // pack_hidden_pk instead of going through the regular pack_record call.
    // Hidden pk is always skipped when unpacking.
    m_pack_func= nullptr;
    m_unpack_func= nullptr;
    return true;
  }

  switch (type)
  {
  case MYSQL_TYPE_LONGLONG:
  case MYSQL_TYPE_LONG:
  case MYSQL_TYPE_INT24:
  case MYSQL_TYPE_SHORT:
  case MYSQL_TYPE_TINY:
    m_unpack_func= mono_key_def::unpack_integer;
    return true;

  case MYSQL_TYPE_DOUBLE:
    m_unpack_func= mono_key_def::unpack_double;
    return true;

  case MYSQL_TYPE_FLOAT:
    m_unpack_func= mono_key_def::unpack_float;
    return true;

  case MYSQL_TYPE_NEWDECIMAL:
  /*
    Decimal is packed with Field_new_decimal::make_sort_key, which just
    does memcpy.
    Unpacking decimal values was supported only after fix for issue#253,
    because of that ha_rocksdb::get_storage_type() handles decimal values
    in a special way.
  */
  case MYSQL_TYPE_DATETIME2:
  case MYSQL_TYPE_TIMESTAMP2:
  /* These are packed with Field_temporal_with_date_and_timef::make_sort_key
   */
  case MYSQL_TYPE_TIME2: /* TIME is packed with Field_timef::make_sort_key */
  case MYSQL_TYPE_YEAR:  /* YEAR is packed with  Field_tiny::make_sort_key */
    /* Everything that comes here is packed with just a memcpy(). */
    m_unpack_func= mono_key_def::unpack_binary_str;
    return true;

  case MYSQL_TYPE_NEWDATE:
    /*
      This is packed by Field_newdate::make_sort_key. It assumes the data is
      3 bytes, and packing is done by swapping the byte order (for both big-
      and little-endian)
    */
    m_unpack_func= mono_key_def::unpack_newdate;
    return true;
  case MYSQL_TYPE_TINY_BLOB:
  case MYSQL_TYPE_MEDIUM_BLOB:
  case MYSQL_TYPE_LONG_BLOB:
  case MYSQL_TYPE_BLOB: {
    if (key_descr)
    {
      m_pack_func= mono_key_def::pack_blob;
      // The my_charset_bin collation is special in that it will consider
      // shorter strings sorting as less than longer strings.
      //
      // See Field_blob::make_sort_key for details.
      m_max_image_len=
          key_length + (field->charset()->number == COLLATION_BINARY
                            ? reinterpret_cast<const Field_blob *>(field)
                                  ->pack_length_no_ptr()
                            : 0);
      // Return false because indexes on text/blob will always require
      // a prefix. With a prefix, the optimizer will not be able to do an
      // index-only scan since there may be content occuring after the prefix
      // length.
      return false;
    }
    break;
  }
  case MYSQL_TYPE_SET:
  case MYSQL_TYPE_ENUM: {
    m_unpack_func= mono_key_def::unpack_unsigned;
    break;
  }

  case MYSQL_TYPE_BIT:
    m_unpack_func= mono_key_def::unpack_bit;
    return true;

  case MYSQL_TYPE_VARCHAR:
    m_pack_func= mono_key_def::pack_with_varchar_encoding;
    break; // More handling below
  case MYSQL_TYPE_STRING:
    break;
  default:
    // MYSQL_TYPE_DECIMAL, MYSQL_TYPE_TIMESTAMP,
    // MYSQL_TYPE_TIME, MYSQL_TYPE_DATETIME,
    // MYSQL_TYPE_VAR_STRING are obsolete
    // MYSQL_TYPE_JSON is not supported as index
    // MYSQL_TYPE_GEOMETRY is not supported yet
    return false;
  }

  m_unpack_info_stores_value= false;
  /* Handle [VAR](CHAR|BINARY) */

  if (type == MYSQL_TYPE_VARCHAR || type == MYSQL_TYPE_STRING)
  {
    /*
      For CHAR-based columns, check how strxfrm image will take.
      field->field_length = field->char_length() * cs->mbmaxlen.
    */
    const CHARSET_INFO *cs= field->charset();
    m_max_image_len=
        cs->strnxfrmlen(type == MYSQL_TYPE_STRING ? field->pack_length()
                                                  : field->field_length);
  }
  const bool is_varchar= (type == MYSQL_TYPE_VARCHAR);
  const CHARSET_INFO *cs= field->charset();
  // max_image_len before chunking is taken into account
  const int max_image_len_before_chunks= m_max_image_len;

  if (is_varchar)
  {
    // The default for varchar is variable-length, without space-padding for
    // comparisons
    m_varchar_charset= cs;
    m_skip_func= mono_key_def::skip_variable_length;

    // Calculate the maximum size of the short section plus the
    // maximum size of the long section
    m_max_image_len= MONO_ENCODED_SIZE(m_max_image_len);

    const auto field_var= static_cast<const Field_varstring *>(field);
    m_unpack_info_uses_two_bytes= (field_var->field_length + 8 >= 0x100);
  }

  if (type == MYSQL_TYPE_VARCHAR || type == MYSQL_TYPE_STRING)
  {
    // See http://dev.mysql.com/doc/refman/5.7/en/string-types.html for
    // information about character-based datatypes are compared.
    if (cs->number == COLLATION_BINARY)
    {
      // - SQL layer pads BINARY(N) so that it always is N bytes long.
      // - For VARBINARY(N), values may have different lengths, so we're using
      //   variable-length encoding. This is also the only charset where the
      //   values are not space-padded for comparison.
      m_unpack_func= is_varchar ? mono_key_def::unpack_binary_or_utf8_varchar
                                : mono_key_def::unpack_binary_str;
      res= true;
    }
    else if (cs->number == COLLATION_LATIN1_BIN ||
             cs->number == COLLATION_UTF8_BIN ||
             cs->number == COLLATION_UTF8MB4_BIN)
    {
      // For _bin collations, mem-comparable form of the string is the string
      // itself.

      if (is_varchar)
      {
        // VARCHARs - are compared as if they were space-padded - but are
        // not actually space-padded (reading the value back produces the
        // original value, without the padding)
        m_unpack_func= (cs == &my_charset_utf8mb4_bin)
                           ? mono_key_def::unpack_utf8mb4_varchar_space_pad
                       : (cs == &my_charset_utf8mb3_bin)
                           ? mono_key_def::unpack_utf8_varchar_space_pad
                           : mono_key_def::unpack_binary_varchar_space_pad;
        m_skip_func= mono_key_def::skip_variable_space_pad;
        m_pack_func= mono_key_def::pack_with_varchar_space_pad;
        m_make_unpack_info_func= mono_key_def::dummy_make_unpack_info;
        m_segment_size= get_segment_size_from_collation(cs);
        m_max_image_len=
            (max_image_len_before_chunks / (m_segment_size - 1) + 1) *
            m_segment_size;
        mono_get_mem_comparable_space(cs, &space_xfrm, &space_xfrm_len,
                                      &space_mb_len);
      }
      else
      {
        // SQL layer pads CHAR(N) values to their maximum length.
        // We just store that and restore it back.
        assert(m_make_unpack_info_func == nullptr);
        m_unpack_func= (cs == &my_charset_utf8mb4_bin)
                           ? mono_key_def::unpack_utf8mb4_str
                       : (cs == &my_charset_utf8mb3_bin)
                           ? mono_key_def::unpack_utf8_str
                           : mono_key_def::unpack_binary_str;
      }
      res= true;
    }
    else
    {
      // This is [VAR]CHAR(n) and the collation is not $(charset_name)_bin

      res= true; // index-only scans are possible
      m_unpack_data_len= is_varchar ? 0 : field->field_length;
      const uint idx= is_varchar ? 0 : 1;
      const mono_collation_codec *codec= nullptr;

      if (is_varchar)
      {
        // VARCHAR requires space-padding for doing comparisons
        //
        // The check for cs->levels_for_order is to catch
        // latin2_czech_cs and cp1250_czech_cs - multi-level collations
        // that Variable-Length Space Padded Encoding can't handle.
        // It is not expected to work for any other multi-level collations,
        // either.
        // Currently we handle these collations as NO_PAD, even if they have
        // PAD_SPACE attribute.
        if (cs->levels_for_order == 1)
        {
          m_pack_func= mono_key_def::pack_with_varchar_space_pad;
          m_skip_func= mono_key_def::skip_variable_space_pad;
          m_segment_size= get_segment_size_from_collation(cs);
          m_max_image_len=
              (max_image_len_before_chunks / (m_segment_size - 1) + 1) *
              m_segment_size;
          mono_get_mem_comparable_space(cs, &space_xfrm, &space_xfrm_len,
                                        &space_mb_len);
        }
        else
        {
          //  NO_LINT_DEBUG
          sql_print_warning("EloqDB: you're trying to create an index "
                            "with a multi-level collation %s",
                            cs->cs_name.str);
          //  NO_LINT_DEBUG
          sql_print_warning("EloqDB will handle this collation internally "
                            " as if it had a NO_PAD attribute.");
          m_pack_func= mono_key_def::pack_with_varchar_encoding;
          m_skip_func= mono_key_def::skip_variable_length;
        }
      }

      if ((codec= mono_init_collation_mapping(cs)) != nullptr)
      {
        // The collation allows to store extra information in the unpack_info
        // which can be used to restore the original value from the
        // mem-comparable form.
        m_make_unpack_info_func= codec->m_make_unpack_info_func[idx];
        m_unpack_func= codec->m_unpack_func[idx];
        m_charset_codec= codec;
      }
      else
      {
        // We have no clue about how this collation produces mem-comparable
        // form. Our way of restoring the original value is to keep a copy of
        // the original value in unpack_info.
        m_unpack_info_stores_value= true;
        m_make_unpack_info_func=
            is_varchar ? mono_key_def::make_unpack_unknown_varchar
                       : mono_key_def::make_unpack_unknown;
        m_unpack_func= is_varchar ? mono_key_def::unpack_unknown_varchar
                                  : mono_key_def::unpack_unknown;
      }
    }

    // If key field is a prefix of the original varchar field, we assume index
    // only scan is not possible on this field.
    if (field->field_length != key_length)
    {
      res= false;
      m_unpack_func= nullptr;
      m_make_unpack_info_func= nullptr;
      m_unpack_info_stores_value= true;
    }
  }

  return res;
}

Field *mono_field_packing::get_field_in_table(const TABLE *const tbl) const
{
  return tbl->key_info[m_keynr].key_part[m_key_part].field;
}

void mono_field_packing::fill_hidden_pk_val(uchar **dst,
                                            const uchar *hidden_pk_id) const
{
  DBUG_ASSERT(m_max_image_len == 16);

  memcpy(*dst, hidden_pk_id, m_max_image_len);

  *dst+= m_max_image_len;
}

Field *
mono_field_packing::get_original_field_in_table(const TABLE *const tbl) const
{
  return tbl->field[m_field_idx - 1];
}

uint mono_field_packing::get_key_part_length() const
{
  return m_Key_part_length;
}

} // namespace MyEloq
