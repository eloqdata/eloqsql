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

#include <string>
#include <cassert>
#include <cstdint>
#include <memory>

namespace MyEloq
{
template <typename KeyT, typename ValueT> struct ScanHeapTuple
{
  ScanHeapTuple()= delete;

  ScanHeapTuple(uint32_t shard_id)
      : key_(std::make_unique<KeyT>()), rec_(std::make_unique<ValueT>()),
        sid_(shard_id)
  {
  }

  std::unique_ptr<KeyT> key_;
  std::unique_ptr<ValueT> rec_;
  uint64_t version_ts_;
  bool deleted_;
  // sid_ is the offset in shard_scan_XX vectors.
  uint32_t sid_;
};

template <typename KeyT, typename ValueT> struct CacheCompare
{
  bool operator()(const ScanHeapTuple<KeyT, ValueT> &lhs,
                  const ScanHeapTuple<KeyT, ValueT> &rhs)
  {
    return !(lhs.key_ < rhs.key_);
  }
};

template <typename KeyT, typename ValueT> struct CacheReverseCompare
{
  bool operator()(const ScanHeapTuple<KeyT, ValueT> &lhs,
                  const ScanHeapTuple<KeyT, ValueT> &rhs)
  {
    return lhs.key_ < rhs.key_;
  }
};

static inline std::string
SerializeSchemaImage(const std::string &frm, const std::string &kv_info,
                     const std::string &key_schemas_ts)
{
  size_t len= frm.length();
  std::string res;

  res.append(reinterpret_cast<const char *>(&len), sizeof(len));
  res.append(frm.data(), frm.length());
  len= kv_info.length();
  res.append(reinterpret_cast<const char *>(&len), sizeof(len));
  res.append(kv_info.data(), kv_info.length());
  len= key_schemas_ts.length();
  res.append(reinterpret_cast<const char *>(&len), sizeof(len));
  res.append(key_schemas_ts.data(), key_schemas_ts.length());

  return res;
}

static inline void DeserializeSchemaImage(const std::string &image,
                                          std::string &frm,
                                          std::string &kv_info,
                                          std::string &key_schemas_ts)
{
  size_t offset= 0;
  const char *buf= image.data();
  size_t len_val;
  len_val= *(size_t *) (buf + offset);
  offset+= sizeof(len_val);
  frm.append(buf + offset, len_val);
  offset+= len_val;

  len_val= *(size_t *) (buf + offset);
  offset+= sizeof(len_val);
  kv_info.append(buf + offset, len_val);
  offset+= len_val;

  len_val= *(size_t *) (buf + offset);
  offset+= sizeof(len_val);
  key_schemas_ts.append(buf + offset, len_val);
  offset+= len_val;

  assert(offset == image.length());
}
} // namespace MyEloq