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
#pragma once

/* C++ standard header files */
#include <cstdlib>
#include <algorithm>
#include <atomic>
#include <map>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <array>

/* C standard header files */
#ifndef _WIN32
#include <arpa/inet.h>
#endif

#include "my_global.h"
#include "table.h"
#include "field.h"

#include "eloq_buff.h"
#include "eloqsql_schema.h"
#include "slice.h"

/* The following is copied from storage/innobase/univ.i: */
#ifndef MY_ATTRIBUTE
#if defined(__GNUC__)
#define MY_ATTRIBUTE(A) __attribute__(A)
#else
#define MY_ATTRIBUTE(A)
#endif
#endif

namespace MyEloq
{

class mono_key_def;
class mono_field_packing;
struct EloqKeySchema;
class EloqRecordSchema;
class EloqKey;

const uint32_t GTID_BUF_LEN= 60;

class mono_convert_to_record_key_decoder
{
public:
  mono_convert_to_record_key_decoder()= default;
  mono_convert_to_record_key_decoder(
      const mono_convert_to_record_key_decoder &decoder)= delete;
  mono_convert_to_record_key_decoder &
  operator=(const mono_convert_to_record_key_decoder &decoder)= delete;
  static int decode(uchar *const buf, uint *offset, mono_field_packing *fpi,
                    TABLE *table, Field *field, bool has_unpack_info,
                    mono_string_reader *reader,
                    mono_string_reader *unpack_reader);
  static int skip(const mono_field_packing *fpi, const Field *field,
                  mono_string_reader *reader,
                  mono_string_reader *unpack_reader);

private:
  static int decode_field(mono_field_packing *fpi, Field *field,
                          mono_string_reader *reader,
                          const uchar *const default_value,
                          mono_string_reader *unpack_reader);
};

/*
  @brief
  Field packing context.
  The idea is to ensure that a call to mono_index_field_pack_t function
  is followed by a call to mono_make_unpack_info_t.

  @detail
  For some datatypes, unpack_info is produced as a side effect of
  mono_index_field_pack_t function call.
  For other datatypes, packing is just calling make_sort_key(), while
  mono_make_unpack_info_t is a custom function.
  In order to accommodate both cases, we require both calls to be made and
  unpack_info is passed as context data between the two.
*/
class mono_pack_field_context
{
public:
  mono_pack_field_context(const mono_pack_field_context &)= delete;
  mono_pack_field_context &operator=(const mono_pack_field_context &)= delete;

  explicit mono_pack_field_context(mono_string_writer *const writer_arg)
      : writer(writer_arg)
  {
  }

  // NULL means we're not producing unpack_info.
  mono_string_writer *writer;
};

class mono_key_field_iterator
{
private:
  mono_field_packing *m_pack_info;
  int m_iter_index;
  int m_iter_end;
  TABLE *m_table;
  mono_string_reader *m_reader;
  mono_string_reader *m_unp_reader;
  uint m_curr_bitmap_pos;
  const MY_BITMAP *m_covered_bitmap;
  uchar *m_buf;
  bool m_has_unpack_info;
  const mono_key_def *m_key_def;
  bool m_secondary_key;
  bool m_hidden_pk_exists;
  bool m_is_hidden_pk;
  bool m_is_null;
  Field *m_field;
  uint m_offset;
  mono_field_packing *m_fpi;

public:
  mono_key_field_iterator(const mono_key_field_iterator &)= delete;
  mono_key_field_iterator &operator=(const mono_key_field_iterator &)= delete;
  mono_key_field_iterator(const mono_key_def *key_def,
                          mono_field_packing *pack_info,
                          mono_string_reader *reader,
                          mono_string_reader *unp_reader, TABLE *table,
                          bool has_unpack_info,
                          const MY_BITMAP *covered_bitmap, uchar *buf);

  int next();
  bool has_next();
  bool get_is_null() const;
  Field *get_field() const;
  int get_field_index() const;
  void *get_dst() const;
};
struct mono_collation_codec;
struct mono_index_info;

/*
  C-style "virtual table" allowing different handling of packing logic based
  on the field type. See mono_field_packing::setup() implementation.
  */
using mono_make_unpack_info_t= void (*)(const mono_collation_codec *codec,
                                        const Field *field,
                                        mono_pack_field_context *pack_ctx);
using mono_index_field_unpack_t= int (*)(mono_field_packing *fpi, Field *field,
                                         uchar *field_ptr,
                                         mono_string_reader *reader,
                                         mono_string_reader *unpack_reader);
using mono_index_field_skip_t= int (*)(const mono_field_packing *fpi,
                                       const Field *field,
                                       mono_string_reader *reader);
using mono_index_field_pack_t= void (*)(mono_field_packing *fpi, Field *field,
                                        uchar *buf, uchar **dst,
                                        mono_pack_field_context *pack_ctx);

const uint MONO_INVALID_KEY_LEN= uint(-1);

/* How much one checksum occupies when stored in the record */
const size_t MONO_CHECKSUM_SIZE= sizeof(uint32_t);

/*
  How much the checksum data occupies in record, in total.
  It is storing two checksums plus 1 tag-byte.
*/
const size_t MONO_CHECKSUM_CHUNK_SIZE= 2 * MONO_CHECKSUM_SIZE + 1;

/*
  Checksum data starts from CHECKSUM_DATA_TAG which is followed by two CRC32
  checksums.
*/
const char MONO_CHECKSUM_DATA_TAG= 0x01;

/*
  Unpack data is variable length. The header is 1 tag-byte plus a two byte
  length field. The length field includes the header as well.
*/
const char MONO_UNPACK_DATA_TAG= 0x02;
const size_t MONO_UNPACK_DATA_LEN_SIZE= sizeof(uint16_t);
const size_t MONO_UNPACK_HEADER_SIZE=
    sizeof(MONO_UNPACK_DATA_TAG) + MONO_UNPACK_DATA_LEN_SIZE;

/*
  This header format is 1 tag-byte plus a two byte length field plus a two byte
  covered bitmap. The length field includes the header size.
*/
const char MONO_UNPACK_COVERED_DATA_TAG= 0x03;
const size_t MONO_UNPACK_COVERED_DATA_LEN_SIZE= sizeof(uint16_t);
const size_t MONO_COVERED_BITMAP_SIZE= sizeof(uint16_t);
const size_t MONO_UNPACK_COVERED_HEADER_SIZE=
    sizeof(MONO_UNPACK_COVERED_DATA_TAG) + MONO_UNPACK_COVERED_DATA_LEN_SIZE +
    MONO_COVERED_BITMAP_SIZE;

/*
  Data dictionary index info field sizes.
*/
const size_t MONO_SIZEOF_INDEX_TYPE= sizeof(uchar);

// Possible return values for mono_index_field_unpack_t functions.
enum
{
  UNPACK_SUCCESS= 0,
  UNPACK_FAILURE= 1,
};

/*
  An object of this class represents information about an index in an SQL
  table. It provides services to encode and decode index tuples.

  Note: a table (as in, on-disk table) has a single mono_key_def object which
  is shared across multiple TABLE* objects and may be used simultaneously from
  different threads.

  There are several data encodings:

  === SQL LAYER ===
  SQL layer uses two encodings:

  - "Table->record format". This is the format that is used for the data in
     the record buffers, table->record[i]

  - KeyTupleFormat (see opt_range.cc) - this is used in parameters to index
    lookup functions, like handler::index_read_map().

  === Inside RocksDB ===
  Primary Key is stored as a mapping:

    index_tuple -> StoredRecord

  StoredRecord is in Table->record format, except for blobs, which are stored
  in-place. See ha_rocksdb::convert_record_to_storage_format for details.

  Secondary indexes are stored as one of two variants:

    index_tuple -> unpack_info
    index_tuple -> empty_string

  index_tuple here is the form of key that can be compared with memcmp(), aka
  "mem-comparable form".

  unpack_info is extra data that allows to restore the original value from its
  mem-comparable form. It is present only if the index supports index-only
  reads.
*/

class mono_key_def
{
public:
  /* Convert a key from KeyTupleFormat to mem-comparable form */
  uint pack_index_tuple(TABLE *const tbl, uchar *const pack_buffer,
                        uchar *const packed_tuple, uchar *const record_buffer,
                        const uchar *const key_tuple,
                        const key_part_map &keypart_map,
                        mono_string_writer *const unpack_info= nullptr) const;

  uchar *pack_field(Field *const field, mono_field_packing *pack_info,
                    uchar *tuple, uchar *const packed_tuple,
                    uchar *const pack_buffer,
                    mono_string_writer *const unpack_info,
                    uint *const n_null_fields) const;
  /* Convert a key from Table->record format to mem-comparable form */
  uint pack_record(const TABLE *const tbl, uchar *const pack_buffer,
                   const uchar *const record, uchar *const packed_tuple,
                   mono_string_writer *const unpack_info,
                   const bool should_store_row_debug_checksums,
                   size_t *unique_sk_pack_size= nullptr,
                   const uchar *hidden_pk_id= nullptr, uint n_key_parts= 0,
                   uint *const n_null_fields= nullptr) const;
  /* Pack the hidden primary key into mem-comparable form. */
  uint pack_hidden_pk(const uchar *hidden_pk_id,
                      uchar *const packed_tuple) const;
  void pack_key(const mysql::KEY *key_info,
                const std::vector<std::string> &key_cols,
                std::vector<char> &buf) const;
  int unpack_record(TABLE *const table, uchar *const buf,
                    const MyEloq::Slice packed_key,
                    const MyEloq::Slice unpack_info,
                    const bool verify_row_debug_checksums) const;

  static bool unpack_info_has_checksum(const MyEloq::Slice &unpack_info);
  int compare_keys(const MyEloq::Slice *key1, const MyEloq::Slice *key2,
                   std::size_t *const column_index) const;

  size_t key_length(const TABLE *const table, const MyEloq::Slice &key) const;

  /**
   * @brief Pad the key to max length with 0xFF so that the original key is
   * a prefix of the new key and old_key < new_key when we compare them.
   *
   * @param key
   */
  void get_ceiling_key(EloqKey &key);

  /* Make a key that is right after the given key. */
  static int successor(uchar *const packed_tuple, const uint len);

  /* Make a key that is right before the given key. */
  static int predecessor(uchar *const packed_tuple, const uint len);

  /*
    This can be used to compare prefixes.
    if  X is a prefix of Y, then we consider that X = Y.
  */
  // b describes the lookup key, which can be a prefix of a.
  // b might be outside of the index_number range, if successor() is called.
  int cmp_full_keys(const MyEloq::Slice &a, const MyEloq::Slice &b) const
  {
    return memcmp(a.data(), b.data(), std::min(a.size(), b.size()));
  }

  /*
    Return true if the passed mem-comparable key
    - is from this index, and
    - it matches the passed key prefix (the prefix is also in mem-comparable
      form)
  */
  bool value_matches_prefix(const MyEloq::Slice &value,
                            const MyEloq::Slice &prefix) const
  {
    return !cmp_full_keys(value, prefix);
  }

  uint32 get_keyno() const { return m_keyno; }

  int read_memcmp_key_part(const TABLE *table_arg, mono_string_reader *reader,
                           const uint part_num) const;

  /* Must only be called for secondary keys: */
  uint get_primary_key_tuple(const TABLE *const tbl,
                             const mono_key_def &pk_descr,
                             const MyEloq::Slice &key,
                             uchar *const pk_buffer) const;

  uint get_memcmp_sk_parts(const TABLE *table, const MyEloq::Slice &key,
                           uchar *sk_buffer, uint *n_null_fields) const;

  /* Return max length of mem-comparable form */
  uint max_storage_fmt_length() const { return m_maxlength; }

  uint get_key_parts() const { return m_key_parts; }

  /*
    Get a field object for key part #part_no

    @detail
      SQL layer thinks unique secondary indexes and indexes in partitioned
      tables are not "Extended" with Primary Key columns.

      Internally, we always extend all indexes with PK columns. This function
      uses our definition of how the index is Extended.
  */
  inline Field *get_table_field_for_part_no(TABLE *table, uint part_no) const;

  static size_t get_unpack_header_size(char tag);

  mono_key_def &operator=(const mono_key_def &)= delete;
  mono_key_def(const mono_key_def &k);
  mono_key_def(uint keyno_arg, uchar index_type_arg);
  ~mono_key_def();

  enum
  {
    CF_NUMBER_SIZE= 4,
    CF_FLAG_SIZE= 4,
    PACKED_SIZE= 4, // one int
  };

  // bit flags for combining bools when writing to disk
  enum
  {
    REVERSE_CF_FLAG= 1,
    AUTO_CF_FLAG= 2, // Deprecated
    PER_PARTITION_CF_FLAG= 4,
  };

  // Set of flags to ignore when comparing two CF-s and determining if
  // they're same.
  static const uint CF_FLAGS_TO_IGNORE= PER_PARTITION_CF_FLAG;

  // MyRocks index types
  enum
  {
    INDEX_TYPE_PRIMARY= 1,
    INDEX_TYPE_SECONDARY= 2,
    INDEX_TYPE_HIDDEN_PRIMARY= 3,
  };

  void setup(const TABLE_SHARE *table);

  /* Check if keypart #kp can be unpacked from index tuple */
  inline bool can_unpack(const uint kp) const;
  /* Check if keypart #kp needs unpack info */
  inline bool has_unpack_info(const uint kp) const;

  /* Check if given table has a primary key */
  static bool table_has_hidden_pk(const TABLE_SHARE *const table);

  void report_checksum_mismatch(const bool is_key, const char *const data,
                                const size_t data_size) const;

  /* Check if index is at least pk_min if it is a PK,
    or at least sk_min if SK.*/
  bool index_format_min_check(const int pk_min, const int sk_min) const;

  static bool mono_is_collation_supported(const CHARSET_INFO *const cs);

  static void pack_with_make_sort_key(
      mono_field_packing *const fpi, Field *const field,
      uchar *buf MY_ATTRIBUTE((__unused__)), uchar **dst,
      mono_pack_field_context *const pack_ctx MY_ATTRIBUTE((__unused__)));

  static void pack_with_varchar_encoding(
      mono_field_packing *const fpi, Field *const field, uchar *buf,
      uchar **dst,
      mono_pack_field_context *const pack_ctx MY_ATTRIBUTE((__unused__)));

  static void
  pack_with_varchar_space_pad(mono_field_packing *const fpi,
                              Field *const field, uchar *buf, uchar **dst,
                              mono_pack_field_context *const pack_ctx);

  static int unpack_hidden_pk(
      mono_field_packing *const fpi,
      Field *const field MY_ATTRIBUTE((__unused__)), uchar *const to,
      mono_string_reader *const reader,
      mono_string_reader *const unp_reader MY_ATTRIBUTE((__unused__)));

  static int unpack_integer(mono_field_packing *const fpi, Field *const field,
                            uchar *const to, mono_string_reader *const reader,
                            mono_string_reader *const unp_reader
                                MY_ATTRIBUTE((__unused__)));

  static int unpack_unsigned(
      mono_field_packing *const fpi MY_ATTRIBUTE((__unused__)),
      Field *const field MY_ATTRIBUTE((__unused__)), uchar *const to,
      mono_string_reader *const reader,
      mono_string_reader *const unp_reader MY_ATTRIBUTE((__unused__)));

  static int
  unpack_bit(mono_field_packing *const fpi, Field *const field,
             uchar *const to, mono_string_reader *const reader,
             mono_string_reader *const unp_reader MY_ATTRIBUTE((__unused__)));

  static int unpack_double(
      mono_field_packing *const fpi MY_ATTRIBUTE((__unused__)),
      Field *const field MY_ATTRIBUTE((__unused__)), uchar *const field_ptr,
      mono_string_reader *const reader,
      mono_string_reader *const unp_reader MY_ATTRIBUTE((__unused__)));

  static int unpack_float(
      mono_field_packing *const fpi,
      Field *const field MY_ATTRIBUTE((__unused__)), uchar *const field_ptr,
      mono_string_reader *const reader,
      mono_string_reader *const unp_reader MY_ATTRIBUTE((__unused__)));

  static int unpack_binary_str(
      mono_field_packing *const fpi, Field *const field, uchar *const to,
      mono_string_reader *const reader,
      mono_string_reader *const unp_reader MY_ATTRIBUTE((__unused__)));

  static void pack_blob(
      mono_field_packing *const fpi, Field *const field,
      uchar *const buf MY_ATTRIBUTE((__unused__)), uchar **dst,
      mono_pack_field_context *const pack_ctx MY_ATTRIBUTE((__unused__)));

  template <const int bytes>
  static int unpack_binary_or_utf8_varchar_space_pad(
      mono_field_packing *const fpi, Field *const field,
      uchar *dst MY_ATTRIBUTE((__unused__)), mono_string_reader *const reader,
      mono_string_reader *const unp_reader);

  static mono_index_field_unpack_t unpack_binary_varchar_space_pad;
  static mono_index_field_unpack_t unpack_utf8_varchar_space_pad;
  static mono_index_field_unpack_t unpack_utf8mb4_varchar_space_pad;

  static int unpack_binary_or_utf8_varchar(
      mono_field_packing *const fpi, Field *const field, uchar *dst,
      mono_string_reader *const reader,
      mono_string_reader *const unp_reader MY_ATTRIBUTE((__unused__)));

  static int unpack_newdate(
      mono_field_packing *const fpi,
      Field *const field MY_ATTRIBUTE((__unused__)), uchar *const field_ptr,
      mono_string_reader *const reader,
      mono_string_reader *const unp_reader MY_ATTRIBUTE((__unused__)));

  static mono_index_field_unpack_t unpack_utf8_str;
  static mono_index_field_unpack_t unpack_utf8mb4_str;

  static int unpack_unknown_varchar(mono_field_packing *const fpi,
                                    Field *const field, uchar *dst,
                                    mono_string_reader *const reader,
                                    mono_string_reader *const unp_reader);

  static int unpack_simple_varchar_space_pad(
      mono_field_packing *const fpi, Field *const field, uchar *dst,
      mono_string_reader *const reader, mono_string_reader *const unp_reader);

  static int unpack_simple(mono_field_packing *const fpi,
                           Field *const field MY_ATTRIBUTE((__unused__)),
                           uchar *const dst, mono_string_reader *const reader,
                           mono_string_reader *const unp_reader);

  static int unpack_unknown(mono_field_packing *const fpi, Field *const field,
                            uchar *const dst, mono_string_reader *const reader,
                            mono_string_reader *const unp_reader);

  static int unpack_floating_point(uchar *const dst,
                                   mono_string_reader *const reader,
                                   const size_t size, const int exp_digit,
                                   const uchar *const zero_pattern,
                                   const uchar *const zero_val,
                                   void (*swap_func)(uchar *, const uchar *));

  static void
  make_unpack_simple_varchar(const mono_collation_codec *const codec,
                             const Field *const field,
                             mono_pack_field_context *const pack_ctx);

  static void make_unpack_simple(const mono_collation_codec *const codec,
                                 const Field *const field,
                                 mono_pack_field_context *const pack_ctx);

  static void make_unpack_unknown(
      const mono_collation_codec *codec MY_ATTRIBUTE((__unused__)),
      const Field *const field, mono_pack_field_context *const pack_ctx);

  static void make_unpack_unknown_varchar(
      const mono_collation_codec *const codec MY_ATTRIBUTE((__unused__)),
      const Field *const field, mono_pack_field_context *const pack_ctx);

  static void dummy_make_unpack_info(
      const mono_collation_codec *codec MY_ATTRIBUTE((__unused__)),
      const Field *field MY_ATTRIBUTE((__unused__)),
      mono_pack_field_context *pack_ctx MY_ATTRIBUTE((__unused__)));

  static int
  skip_max_length(const mono_field_packing *const fpi,
                  const Field *const field MY_ATTRIBUTE((__unused__)),
                  mono_string_reader *const reader);

  static int skip_variable_length(const mono_field_packing *const fpi,
                                  const Field *const field,
                                  mono_string_reader *const reader);

  static int skip_variable_space_pad(const mono_field_packing *const fpi,
                                     const Field *const field,
                                     mono_string_reader *const reader);

  static inline bool is_unpack_data_tag(char c)
  {
    return c == MONO_UNPACK_DATA_TAG || c == MONO_UNPACK_COVERED_DATA_TAG;
  }

private:
#ifndef DBUG_OFF
  inline bool is_storage_available(const int offset, const int needed) const
  {
    const int storage_length= static_cast<int>(max_storage_fmt_length());
    return (storage_length - offset) >= needed;
  }
#else
  inline bool is_storage_available(const int &offset, const int &needed) const
  {
    return 1;
  }
#endif // DBUG_OFF

  static void pack_variable_format(const uchar *src, size_t src_len,
                                   uchar **dst);

  static uint calc_unpack_variable_format(uchar flag, bool *done);

public:
  uchar m_index_type;
  /* If true, the column family stores data in the reverse order */
  bool m_is_reverse_cf;

  /*
    Number of key parts in the unique secondary key, without trailing pk key
    parts
  */
  uint m_unique_sk_only_key_parts;

private:
  /* Number of key parts in the primary key*/
  uint m_pk_key_parts;

  /*
     pk_part_no[X]=Y means that keypart #X of this key is key part #Y of the
     primary key.  Y==-1 means this column is not present in the primary key.
  */
  uint *m_pk_part_no;

  /* Array of index-part descriptors. */
  mono_field_packing *m_pack_info;

  uint m_keyno; /* number of this index in the table */

  /*
    Number of key parts in the index (including "index extension"). This is how
    many elements are in the m_pack_info array.
  */
  uint m_key_parts;

  /* Maximum length of the mem-comparable form. */
  uint m_maxlength;

  /* mutex to protect setup */
  mysql_mutex_t m_mutex;
};

// "Simple" collations (those specified in strings/ctype-simple.c) are simple
// because their strnxfrm function maps one byte to one byte. However, the
// mapping is not injective, so the inverse function will take in an extra
// index parameter containing information to disambiguate what the original
// character was.
//
// The enc_* members are for encoding. Generally, we want encoding to be:
//      src -> (dst, idx)
//
// Since strnxfrm already gives us dst, we just need enc_idx_[src] to give us
// idx.
//
// For the inverse, we have:
//      (dst, idx) -> src
//
// We have m_dec_idx[idx][dst] = src to get our original character back.
//
struct mono_collation_codec
{
  const CHARSET_INFO *m_cs;
  // The first element unpacks VARCHAR(n), the second one - CHAR(n).
  std::array<mono_make_unpack_info_t, 2> m_make_unpack_info_func;
  std::array<mono_index_field_unpack_t, 2> m_unpack_func;

  std::array<uchar, 256> m_enc_idx;
  std::array<uchar, 256> m_enc_size;

  std::array<uchar, 256> m_dec_size;
  std::vector<std::array<uchar, 256>> m_dec_idx;
};

extern mysql_mutex_t mono_collation_data_mutex;
extern mysql_mutex_t mono_mem_cmp_space_mutex;
extern std::array<const mono_collation_codec *, MY_ALL_CHARSETS_SIZE>
    mono_collation_data;

class mono_field_packing
{
public:
  mono_field_packing(const mono_field_packing &)= delete;
  mono_field_packing &operator=(const mono_field_packing &)= delete;
  mono_field_packing()= default;

  /* Length of mem-comparable image of the field, in bytes */
  int m_max_image_len;

  /* Length of image in the unpack data */
  int m_unpack_data_len;
  int m_unpack_data_offset;

  bool m_maybe_null; /* TRUE <=> NULL-byte is stored */

  /*
    Valid only for VARCHAR fields.
  */
  const CHARSET_INFO *m_varchar_charset;

  // (Valid when Variable Length Space Padded Encoding is used):
  uint m_segment_size; // size of segment used

  // number of bytes used to store number of trimmed (or added)
  // spaces in the upack_info
  bool m_unpack_info_uses_two_bytes;

  const std::vector<uchar> *space_xfrm;
  size_t space_xfrm_len;
  size_t space_mb_len;

  const mono_collation_codec *m_charset_codec;

  /*
    @return TRUE: this field makes use of unpack_info.
  */
  bool uses_unpack_info() const
  {
    return (m_make_unpack_info_func != nullptr);
  }

  /* TRUE means unpack_info stores the original field value */
  bool m_unpack_info_stores_value;

  mono_index_field_pack_t m_pack_func;
  mono_make_unpack_info_t m_make_unpack_info_func;

  /*
    This function takes
    - mem-comparable form
    - unpack_info data
    and restores the original value.
  */
  mono_index_field_unpack_t m_unpack_func;

  /*
    This function skips over mem-comparable form.
  */
  mono_index_field_skip_t m_skip_func;

private:
  /*
    Location of the field in the table (key number and key part number).

    Note that this describes not the field, but rather a position of field in
    the index. Consider an example:

      col1 VARCHAR (100),
      INDEX idx1 (col1)),
      INDEX idx2 (col1(10)),

    Here, idx2 has a special Field object that is set to describe a 10-char
    prefix of col1.

    We must also store the keynr. It is needed for implicit "extended keys".
    Every key in MyRocks needs to include PK columns.  Generally, SQL layer
    includes PK columns as part of its "Extended Keys" feature, but sometimes
    it does not (known examples are unique secondary indexes and partitioned
    tables).
    In that case, MyRocks's index descriptor has invisible suffix of PK
    columns (and the point is that these columns are parts of PK, not parts
    of the current index).
  */
  uint m_keynr;
  uint m_key_part;
  // Begins count from 1. 0 - Stand for hidden pk
  field_index_t m_field_idx;
  // Length of key part in bytes, excluding NULL flag and length bytes.
  // Enqual to the KEY_PART_INFO::length
  uint m_Key_part_length;

public:
  bool setup(const mono_key_def *const key_descr, const Field *const field,
             const uint keynr_arg, const uint key_part_arg,
             const uint16 key_length);
  Field *get_field_in_table(const TABLE *const tbl) const;
  void fill_hidden_pk_val(uchar **dst, const uchar *hidden_pk_id) const;
  Field *get_original_field_in_table(const TABLE *const tbl) const;
  uint get_key_part_length() const;
};

/*
  Descriptor telling how to decode/encode a field to on-disk record storage
  format. Not all information is in the structure yet, but eventually we
  want to have as much as possible there to avoid virtual calls.

  For encoding/decoding of index tuples, see mono_key_def.
  */
class mono_field_encoder
{
public:
  mono_field_encoder(const mono_field_encoder &)= delete;
  mono_field_encoder &operator=(const mono_field_encoder &)= delete;
  /*
    STORE_NONE is set when a column can be decoded solely from their
    mem-comparable form.
    STORE_SOME is set when a column can be decoded from their mem-comparable
    form plus unpack_info.
    STORE_ALL is set when a column cannot be decoded, so its original value
    must be stored in the PK records.
    */
  enum STORAGE_TYPE
  {
    STORE_NONE,
    STORE_SOME,
    STORE_ALL,
  };
  STORAGE_TYPE m_storage_type;

  uint m_null_offset;
  uint16 m_field_index;

  uchar m_null_mask; // 0 means the field cannot be null

  enum_field_types m_field_type;

  uint m_pack_length_in_rec;

  bool maybe_null() const { return m_null_mask != 0; }

  bool uses_variable_len_encoding() const
  {
    return (m_field_type == MYSQL_TYPE_BLOB ||
            m_field_type == MYSQL_TYPE_VARCHAR);
  }
};

inline Field *mono_key_def::get_table_field_for_part_no(TABLE *table,
                                                        uint part_no) const
{
  DBUG_ASSERT(part_no < get_key_parts());
  return m_pack_info[part_no].get_field_in_table(table);
}

inline bool mono_key_def::can_unpack(const uint kp) const
{
  DBUG_ASSERT(kp < m_key_parts);
  return (m_pack_info[kp].m_unpack_func != nullptr);
}

inline bool mono_key_def::has_unpack_info(const uint kp) const
{
  DBUG_ASSERT(kp < m_key_parts);
  return m_pack_info[kp].uses_unpack_info();
}

} // namespace MyEloq
