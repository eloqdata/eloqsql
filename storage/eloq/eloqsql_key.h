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

#include <mimalloc.h>

#include <memory>
#include <vector>
#include <iomanip>
#include <iostream>
#include <string>
#include <sstream>
#include <utility>

#include "my_global.h"
#include "key.h"
#include "eloq_key_def.h"
#include "tx_service/include/tx_key.h"
#include "tx_service/include/tx_record.h"
#include "slice.h"

namespace MyEloq
{
class EloqKey
{
public:
  /**
   * EloqKey stores the mem-comparable key value of the original MySQL
   * key. The packed value can be unpacked into MySQL table record format with
   * unpack info stored in EloqRecord paired with this Eloq key.
   * Both primary key and secondary key use EloqKey as its key in ccmap.
   */

  EloqKey()= default;
  EloqKey(const mono_key_def *const key_desc, const TABLE *table,
          uchar *const pack_buffer, const uchar *const record,
          bool is_hidden= false, const uchar *const hidden_pk= nullptr,
          bool is_unique_sk= false)
  {
    size_t pack_key_len= key_desc->max_storage_fmt_length();
    packed_key_.resize(pack_key_len);
    size_t unique_sk_pack_size;
    size_t pack_size;
    if (!is_hidden)
    {
      pack_size= key_desc->pack_record(
          table, pack_buffer, record,
          reinterpret_cast<uchar *>(packed_key_.data()), nullptr, false,
          is_unique_sk ? &unique_sk_pack_size : nullptr, hidden_pk);
    }
    else
    {
      pack_size= key_desc->pack_hidden_pk(
          record, reinterpret_cast<uchar *>(packed_key_.data()));
    }
    if (pack_size != pack_key_len)
    {
      assert(pack_size < pack_key_len);
      packed_key_.resize(pack_size);
    }

    if (is_unique_sk)
    {
      // Erase tariling pk if it is a unique sk.
      assert(unique_sk_pack_size < pack_size);
      packed_key_.resize(unique_sk_pack_size);
    }
  }

  EloqKey(const uchar *packed_key, size_t packed_size)
  {
    if (packed_size)
    {
      assert(packed_key != nullptr);
      const char *char_key= reinterpret_cast<const char *>(packed_key);
      packed_key_= std::string(char_key, packed_size);
    }
  }

  static txservice::TxKey Create(const char *data, size_t size)
  {
    return txservice::TxKey(std::make_unique<EloqKey>(
        reinterpret_cast<const uchar *>(data), size));
  }

  static txservice::TxKey CreateDefault()
  {
    return txservice::TxKey(std::make_unique<EloqKey>());
  }

  // Copy constructor. The new EloqKey object allocates and owns the blob.
  EloqKey(const EloqKey &other) : packed_key_(other.packed_key_.size(), 0)
  {
    std::copy(other.packed_key_.begin(), other.packed_key_.end(),
              packed_key_.begin());
  }

  EloqKey(EloqKey &&key) noexcept : packed_key_(std::move(key.packed_key_)) {}

  ~EloqKey()= default;

  static const EloqKey *PackedNegativeInfinity()
  {
    static uchar neg_inf_packed_key= 0x00;
    static EloqKey neg_inf_key(&neg_inf_packed_key, 1);
    return &neg_inf_key;
  }

  static const txservice::TxKey *PackedNegativeInfinityTxKey()
  {
    static const txservice::TxKey packed_negative_infinity_tx_key{
        PackedNegativeInfinity()};
    return &packed_negative_infinity_tx_key;
  }

  static EloqKey PackedPositiveInfinity(const txservice::KeySchema *key_schema)
  {
    size_t max_length= 0;

    const EloqKeySchema *pk_sch=
        dynamic_cast<const EloqKeySchema *>(key_schema);
    if (pk_sch)
    {
      max_length= pk_sch->KeyDefinition()->max_storage_fmt_length();
    }
    else
    {
      const txservice::SecondaryKeySchema *sk_pk_sch=
          dynamic_cast<const txservice::SecondaryKeySchema *>(key_schema);
      assert(sk_pk_sch != nullptr);
      const EloqKeySchema *sk_sch=
          static_cast<const EloqKeySchema *>(sk_pk_sch->sk_schema_.get());
      pk_sch= static_cast<const EloqKeySchema *>(sk_pk_sch->pk_schema_);
      max_length= sk_sch->KeyDefinition()->max_storage_fmt_length() +
                  pk_sch->KeyDefinition()->max_storage_fmt_length();
    }

    EloqKey pos_inf_key;
    pos_inf_key.packed_key_.resize(max_length, 0xFF);
    return pos_inf_key;
  }

  EloqKey &operator=(EloqKey &&other) noexcept
  {
    if (this != &other)
    {
      packed_key_= std::move(other.packed_key_);
    }
    return *this;
  }

  EloqKey &operator=(const EloqKey &rhs)
  {
    if (this == &rhs)
    {
      return *this;
    }
    packed_key_.resize(rhs.packed_key_.size());
    std::memcpy(packed_key_.data(), rhs.packed_key_.data(),
                rhs.packed_key_.size());

    return *this;
  }

  friend bool operator==(const EloqKey &lhs, const EloqKey &rhs)
  {
    const EloqKey *neg_ptr= NegativeInfinity();
    const EloqKey *pos_ptr= PositiveInfinity();

    if (&lhs == neg_ptr || &lhs == pos_ptr || &rhs == neg_ptr ||
        &rhs == pos_ptr)
    {
      return &lhs == &rhs;
    }

    Slice left= lhs.PackedValueSlice();
    Slice right= rhs.PackedValueSlice();
    return left.compare(right) == 0;
  }

  friend bool operator!=(const EloqKey &lhs, const EloqKey &rhs)
  {
    return !(lhs == rhs);
  }

  /**
   * Whether a search key is prefix of a full key, to distinguish between
   * prefix equality and full equality. Returns true if *this is a prefix of
   * rhs or exactly the same as rhs, false if otherwise.
   * @param rhs
   * @return
   */
  bool IsPrefixOf(const EloqKey &rhs) const
  {
    // Compare the prefix part
    const EloqKey *neg_ptr= NegativeInfinity();
    const EloqKey *pos_ptr= PositiveInfinity();

    if (this == neg_ptr || this == pos_ptr || &rhs == neg_ptr ||
        &rhs == pos_ptr)
    {
      return this == &rhs;
    }
    size_t key_len= std::min(packed_key_.size(), rhs.packed_key_.size());
    if (memcmp(packed_key_.data(), rhs.packed_key_.data(), key_len) != 0)
    {
      return false;
    }

    return packed_key_.size() <= rhs.packed_key_.size();
  }

  friend bool operator<(const EloqKey &lhs, const EloqKey &rhs)
  {
    const EloqKey *neg_ptr= NegativeInfinity();
    const EloqKey *pos_ptr= PositiveInfinity();

    if (&lhs == neg_ptr)
    {
      // Negative infinity is less than any key, except itself.
      return &rhs != neg_ptr;
    }
    else if (&lhs == pos_ptr || &rhs == neg_ptr)
    {
      return false;
    }
    else if (&rhs == pos_ptr)
    {
      // Positive infinity is greater than any key, except itself.
      return &lhs != pos_ptr;
    }

    return lhs.packed_key_ < rhs.packed_key_;
  }

  friend bool operator<=(const EloqKey &lhs, const EloqKey &rhs)
  {
    return !(rhs < lhs);
  }

  size_t Hash() const
  {
    return std::hash<std::string_view>()(
        std::string_view(packed_key_.data(), packed_key_.size()));
  }

  void Serialize(std::vector<char> &buf, size_t &offset) const
  {
    buf.resize(offset + sizeof(uint16_t) + packed_key_.size());
    uint16_t len_val= (uint16_t) packed_key_.size();
    const char *val_ptr=
        static_cast<const char *>(static_cast<const void *>(&len_val));
    std::copy(val_ptr, val_ptr + sizeof(uint16_t), buf.begin() + offset);
    offset+= sizeof(uint16_t);
    std::copy(packed_key_.begin(), packed_key_.end(), buf.begin() + offset);
    offset+= len_val;
  }

  void Serialize(std::string &str) const
  {
    size_t len_sizeof= sizeof(uint16_t);
    // A 2-byte integer represents key lengths up to 65535, which is far more
    // enough for MySQL index keys.
    uint16_t len_val= (uint16_t) packed_key_.size();
    const char *len_ptr= reinterpret_cast<const char *>(&len_val);
    str.append(len_ptr, len_sizeof);
    str.append(packed_key_.data(), len_val);
  }

  size_t SerializedLength() const
  {
    // packed key's length + packed key
    return sizeof(uint16_t) + packed_key_.size();
  }

  void Deserialize(const char *buf, size_t &offset,
                   const txservice::KeySchema *schema)
  {
    uint16_t *len_ptr= (uint16_t *) (buf + offset);
    uint16_t len_val= *len_ptr;
    offset+= sizeof(uint16_t);
    packed_key_.clear();
    packed_key_.reserve(len_val);
    const unsigned char *u_buf= (const unsigned char *) buf;
    std::copy(u_buf + offset, u_buf + offset + len_val,
              std::back_inserter(packed_key_));
    offset+= len_val;
  }

  std::string_view KVSerialize() const
  {
    // should not be called
    assert(false);
    return "";
  }

  void KVDeserialize(const char *buf, size_t len)
  {
    // should not be called
    assert(false);
  }

  txservice::TxKey CloneTxKey() const
  {
    if (this == NegativeInfinity())
    {
      return txservice::TxKey(NegativeInfinity());
    }
    else if (this == PositiveInfinity())
    {
      return txservice::TxKey(PositiveInfinity());
    }
    else
    {
      return txservice::TxKey(std::make_unique<EloqKey>(*this));
    }
  }

  void Copy(const EloqKey &rhs) { *this= rhs; }

  txservice::KeyType Type() const
  {
    if (this == EloqKey::NegativeInfinity())
    {
      return txservice::KeyType::NegativeInf;
    }
    else if (this == EloqKey::PositiveInfinity())
    {
      return txservice::KeyType::PositiveInf;
    }
    else
    {
      return txservice::KeyType::Normal;
    }
  }

  bool NeedsDefrag(mi_heap_t *heap)
  {
    if (packed_key_.data() != nullptr)
    {
      float page_utilization=
          mi_heap_page_utilization(heap, packed_key_.data());
      if (page_utilization < 0.8)
      {
        return true;
      }
    }
    return false;
  }

  static const EloqKey *NegativeInfinity()
  {
    static const EloqKey neg_inf;
    return &neg_inf;
  }

  static const EloqKey *PositiveInfinity()
  {
    static const EloqKey pos_inf;
    return &pos_inf;
  }

  std::string ToString() const
  {
    if (Type() == txservice::KeyType::NegativeInf)
    {
      return std::string("NegativeInf");
    }
    if (Type() == txservice::KeyType::PositiveInf)
    {
      return std::string("PositiveInf");
    }
    if (packed_key_.size() == 0)
    {
      return std::string("NULL");
    }
    std::stringstream ss;
    ss << std::hex << std::setfill('0');
    for (size_t pos= 0; pos < packed_key_.size(); ++pos)
    {
      ss << std::setw(2)
         << static_cast<unsigned>(static_cast<uint8_t>(packed_key_[pos]));
    }
    return std::string("0x").append(ss.str());
  }

  size_t MemUsage() const
  {
    // Most C++ implementations provide SSO, Small String Optimization. Based
    // on libstdc++, basic_string<char> has a local capacity of 15, plus one
    // more byte to store '\0', i.e. strings shorter than 16 are stored
    // directly in the String object, only strings longer than 16 have an
    // allocated buffer to store the contents.
    size_t mem_usage= sizeof(EloqKey);
    if (packed_key_.capacity() >= 16)
    {
      mem_usage+= packed_key_.capacity();
    }
    return mem_usage;
  }

  double PosInInterval(const EloqKey &min_key, const EloqKey &max_key) const
  {
    assert(min_key <= max_key);

    if (*this <= min_key)
    {
      return 0;
    }
    else if (max_key <= *this)
    {
      return 1;
    }
    else
    {
      return 0.5;
    }
  }

  /**
   * Assume that values between (min_key, max_key) are equally-distributed.
   * Return percentage of (this_key - min_key) / (max_key - min_key).
   * To calculate the value, these keys should be converted into numbers.
   *
   * First, get string form for every key. Which is memory comparable.
   * Second, find their longest common prefix.
   * Third, take 8bytes after the longest common prefix for every string, and
   * convert them into three uint64_t number.
   * At last, calculate the percentage with the three uint64_t number.
   *
   * Reference Field::pos_in_interval_val_str() in sql/field.cc
   */
  double PosInInterval(const txservice::KeySchema *key_schema,
                       const EloqKey &min_key, const EloqKey &max_key) const
  {
    double pos= PosInInterval(min_key, max_key);
    assert(pos >= 0 && pos <= 1);

    if (pos == 0 || pos == 1)
      return pos;

    assert(this->Type() == txservice::KeyType::Normal);

    const EloqKey &mono_min_key= static_cast<const EloqKey &>(min_key);
    const EloqKey &mono_max_key= static_cast<const EloqKey &>(max_key);

    assert(mono_min_key < *this && *this < mono_max_key);

    // Eloq hidden pk is uuid. It changes at every run time.
    unsigned trim_suffix_size= 0;
    {
      const txservice::SecondaryKeySchema *sk_sch=
          dynamic_cast<const txservice::SecondaryKeySchema *>(key_schema);
      if (sk_sch)
      {
        const EloqKeySchema *mono_sk_schema=
            static_cast<const EloqKeySchema *>(sk_sch->sk_schema_.get());
        const EloqKeySchema *mono_pk_schema=
            static_cast<const EloqKeySchema *>(sk_sch->pk_schema_);

        if (mono_pk_schema->GetIndexType() ==
                mono_key_def::INDEX_TYPE_HIDDEN_PRIMARY &&
            !mono_sk_schema->IsUniqueIndex())
        {
          // non-unique secondary index need trim hidden pk.
          trim_suffix_size= MY_UUID_SIZE;
        }
      }
    }

    Slice min_key_slice= mono_min_key.PackedValueSlice();
    Slice max_key_slice= mono_max_key.PackedValueSlice();
    Slice this_key_slice= this->PackedValueSlice();

    size_t offset= min_key_slice.difference_offset(max_key_slice);
    assert(min_key_slice.size() >= offset);
    assert(max_key_slice.size() >= offset);
    assert(this_key_slice.size() >= offset);

    size_t trim_size= offset + trim_suffix_size;

    Slice min_key_slice_trimmed= Slice(min_key_slice.data() + offset,
                                       min_key_slice.size() > trim_size
                                           ? min_key_slice.size() - trim_size
                                           : 0);
    Slice max_key_slice_trimmed= Slice(max_key_slice.data() + offset,
                                       max_key_slice.size() > trim_size
                                           ? max_key_slice.size() - trim_size
                                           : 0);
    Slice this_key_slice_trimmed=
        Slice(this_key_slice.data() + offset, this_key_slice.size() - offset);

    assert(min_key_slice_trimmed.compare(this_key_slice_trimmed) <= 0 &&
           this_key_slice_trimmed.compare(max_key_slice_trimmed) <= 0);

    constexpr size_t buf_size= sizeof(unsigned long long);

    std::string min_key_buf(buf_size, '\0');
    std::string max_key_buf(buf_size, '\0');
    std::string this_key_buf(buf_size, '\0');

    memcpy(min_key_buf.data(), min_key_slice_trimmed.data(),
           std::min(buf_size, min_key_slice_trimmed.size()));
    memcpy(max_key_buf.data(), max_key_slice_trimmed.data(),
           std::min(buf_size, max_key_slice_trimmed.size()));
    memcpy(this_key_buf.data(), this_key_slice_trimmed.data(),
           std::min(buf_size, this_key_slice_trimmed.size()));

    std::reverse(min_key_buf.begin(), min_key_buf.end());
    std::reverse(max_key_buf.begin(), max_key_buf.end());
    std::reverse(this_key_buf.begin(), this_key_buf.end());

    unsigned long long min_key_value= uint8korr(min_key_buf.data());
    unsigned long long max_key_value= uint8korr(max_key_buf.data());
    unsigned long long this_key_value= uint8korr(this_key_buf.data());

    assert(min_key_value <= this_key_value && this_key_value <= max_key_value);

    double d= (max_key_value > min_key_value)
                  ? double(max_key_value - min_key_value)
                  : -double(min_key_value - max_key_value);
    if (d == 0)
      return 0.5;
    if (d < 0)
      return 1.0;
    double n= (this_key_value > min_key_value)
                  ? double(this_key_value - min_key_value)
                  : -double(min_key_value - this_key_value);
    if (n <= 0)
      return 0.0;
    return std::min(n / d, 1.0);
  }

  void SetPackedKey(const char *data, size_t size)
  {
    packed_key_.resize(size);
    std::copy(data, data + size, packed_key_.begin());
  }

  // Slice works as a string_view but has overloaded comparision
  // function and operators to compare the packed value.
  const Slice PackedValueSlice() const
  {
    return Slice(packed_key_.data(), packed_key_.size());
  }

  std::string &PackedValue() { return packed_key_; }

  const std::string &PackedValue() const { return packed_key_; }
  const char *Data() const { return packed_key_.data(); }
  size_t Size() const { return packed_key_.size(); }

  static const txservice::TxKey *NegInfTxKey()
  {
    static const txservice::TxKey neg_inf_tx_key{NegativeInfinity()};
    return &neg_inf_tx_key;
  }

  static txservice::TxKey GetNegInfTxKey()
  {
    return txservice::TxKey(NegativeInfinity());
  }

  static const txservice::TxKey *PosInfTxKey()
  {
    static const txservice::TxKey pos_inf_tx_key{PositiveInfinity()};
    return &pos_inf_tx_key;
  }

  static txservice::TxKey GetPosInfTxKey()
  {
    return txservice::TxKey(PositiveInfinity());
  }

  static const ::txservice::TxKeyInterface *TxKeyImpl()
  {
    static const txservice::TxKeyInterface tx_key_impl{
        *EloqKey::NegativeInfinity()};
    return &tx_key_impl;
  }

private:
  std::string packed_key_;
};

class EloqRecord : public txservice::TxRecord
{
public:
  EloqRecord()
  {
    encoded_blob_.reserve(32);
    unpack_info_.reserve(8);
  }

  EloqRecord(const EloqRecord &rhs)
      : encoded_blob_(rhs.encoded_blob_), unpack_info_(rhs.unpack_info_)
  {
  }

  EloqRecord(EloqRecord &&rhs)
      : encoded_blob_(std::move(rhs.encoded_blob_)),
        unpack_info_(std::move(rhs.unpack_info_))
  {
  }

  ~EloqRecord()= default;

  static TxRecord::Uptr Create() { return std::make_unique<EloqRecord>(); }

  // const unsigned char *Buf() const { return sqlrec_blob_.data(); }
  size_t Length() const override
  {
    return encoded_blob_.size() + unpack_info_.size();
  }

  size_t MemUsage() const override
  {
    return sizeof(EloqRecord) + encoded_blob_.capacity() +
           unpack_info_.capacity();
  }

  void Serialize(std::vector<char> &buf, size_t &offset) const override
  {
    size_t len= sizeof(size_t);
    buf.resize(offset + 2 * sizeof(size_t) + encoded_blob_.size() +
               unpack_info_.size());

    size_t blob_len= (size_t) unpack_info_.size();
    const unsigned char *val_ptr= static_cast<const unsigned char *>(
        static_cast<const void *>(&blob_len));
    std::copy(val_ptr, val_ptr + len, buf.begin() + offset);
    offset+= len;
    std::copy(unpack_info_.begin(), unpack_info_.end(), buf.begin() + offset);
    offset+= blob_len;

    blob_len= encoded_blob_.size();
    val_ptr= static_cast<const unsigned char *>(
        static_cast<const void *>(&blob_len));
    std::copy(val_ptr, val_ptr + len, buf.begin() + offset);
    offset+= len;

    std::copy(encoded_blob_.begin(), encoded_blob_.end(),
              buf.begin() + offset);
    offset+= encoded_blob_.size();
  }

  void Serialize(std::string &str) const override
  {
    size_t len_sizeof= sizeof(size_t);
    size_t unpack_len= (size_t) unpack_info_.size();
    const char *unpack_len_ptr=
        static_cast<const char *>(static_cast<const void *>(&unpack_len));
    str.append(unpack_len_ptr, len_sizeof);
    str.append(unpack_info_.data(), unpack_len);

    size_t blob_len= encoded_blob_.size();
    const char *blob_len_ptr=
        static_cast<const char *>(static_cast<const void *>(&blob_len));

    str.append(blob_len_ptr, len_sizeof);
    str.append(encoded_blob_.data(), blob_len);
  }

  size_t SerializedLength() const override
  {
    // unpack_info_ and encoded_blob_ and their length
    return sizeof(size_t) * 2 + unpack_info_.size() + encoded_blob_.size();
  }

  void Deserialize(const char *buf, size_t &offset) override
  {
    size_t *len_ptr= (size_t *) (buf + offset);
    size_t len= *len_ptr;
    offset+= sizeof(size_t);
    unpack_info_.clear();
    unpack_info_.reserve(len);
    std::copy(buf + offset, buf + offset + len,
              std::back_inserter(unpack_info_));
    offset+= len;

    len_ptr= (size_t *) (buf + offset);
    len= *len_ptr;
    offset+= sizeof(size_t);
    encoded_blob_.clear();
    encoded_blob_.reserve(len);
    std::copy(buf + offset, buf + offset + len,
              std::back_inserter(encoded_blob_));
    offset+= len;
  };

  TxRecord::Uptr Clone() const override
  {
    return std::make_unique<EloqRecord>(*this);
  }

  void Copy(const TxRecord &rhs) override
  {
    const EloqRecord &typed_rhs= static_cast<const EloqRecord &>(rhs);

    encoded_blob_= typed_rhs.encoded_blob_;
    unpack_info_= typed_rhs.unpack_info_;
  }

  std::string ToString() const override
  {
    if (encoded_blob_.empty())
    {
      return std::string("NULL");
    }
    std::stringstream ss;
    ss << std::hex << std::setfill('0');
    for (size_t pos= 0; pos < encoded_blob_.size(); ++pos)
    {
      ss << std::setw(2)
         << static_cast<unsigned>(static_cast<uint8_t>(encoded_blob_[pos]));
    }
    return std::string("0x").append(ss.str());
  }

  EloqRecord &operator=(const EloqRecord &rhs)
  {
    if (this != &rhs)
    {
      encoded_blob_.resize(rhs.encoded_blob_.size());
      std::memcpy(encoded_blob_.data(), rhs.encoded_blob_.data(),
                  rhs.encoded_blob_.size());
      unpack_info_.resize(rhs.unpack_info_.size());
      std::memcpy(unpack_info_.data(), rhs.unpack_info_.data(),
                  rhs.unpack_info_.size());
    }

    return *this;
  }

  EloqRecord &operator=(EloqRecord &&other) noexcept
  {
    if (this != &other)
    {
      encoded_blob_= std::move(other.encoded_blob_);
      unpack_info_= std::move(other.unpack_info_);
    }
    return *this;
  }

  const Slice UnpackInfoSlice() const
  {
    return Slice(unpack_info_.data(), unpack_info_.size());
  }

  void SetUnpackInfo(const uchar *unpack_ptr,
                     const size_t unpack_size) override
  {
    unpack_info_.clear();
    unpack_info_.resize(unpack_size);
    memcpy(unpack_info_.data(), unpack_ptr, unpack_size);
  }

  const Slice EncodedBlobSlice() const
  {
    return Slice(encoded_blob_.data(), encoded_blob_.size());
  }

  const char *EncodedBlobData() const override { return encoded_blob_.data(); }

  size_t EncodedBlobSize() const override { return encoded_blob_.size(); }

  const char *UnpackInfoData() const override { return unpack_info_.data(); }

  size_t UnpackInfoSize() const override { return unpack_info_.size(); }

  void SetEncodedBlob(const uchar *blob_ptr, const size_t blob_size) override
  {
    encoded_blob_.clear();
    encoded_blob_.resize(blob_size);
    memcpy(encoded_blob_.data(), blob_ptr, blob_size);
  }

  bool NeedsDefrag(mi_heap_t *heap) override
  {
    bool defraged= false;

    if (encoded_blob_.data() != nullptr)
    {
      float encoded_blob_utilization=
          mi_heap_page_utilization(heap, encoded_blob_.data());
      if (encoded_blob_utilization < 0.8)
      {
        defraged= true;
      }
    }

    if (unpack_info_.data() != nullptr)
    {
      float unpack_info_utilization=
          mi_heap_page_utilization(heap, unpack_info_.data());
      if (unpack_info_utilization < 0.8)
      {
        defraged= true;
      }
    }

    return defraged;
  }
  // Encoded blob for non-pk cols
  size_t Size() const override { return encoded_blob_.size(); }

  void Prefetch() const override
  {
    if (encoded_blob_.data())
    {
      __builtin_prefetch(encoded_blob_.data(), 1, 1);
    }
    if (unpack_info_.data())
    {

      __builtin_prefetch(unpack_info_.data(), 1, 1);
    }
  }

  std::vector<char> encoded_blob_;
  // Unpack info used to unpack EloqKey into MySQL table record format
  std::vector<char> unpack_info_;
};

} // namespace MyEloq
