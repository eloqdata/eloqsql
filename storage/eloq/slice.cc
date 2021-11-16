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
#include "slice.h"

namespace MyEloq
{
// 2 small internal utility functions, for efficient hex conversions
// and no need for snprintf, toupper etc...
// Originally from wdt/util/EncryptionUtils.cpp - for ToString(true)/DecodeHex:
char toHex(unsigned char v)
{
  if (v <= 9)
  {
    return '0' + v;
  }
  return 'A' + v - 10;
}
// most of the code is for validation/error check
int fromHex(char c)
{
  // toupper:
  if (c >= 'a' && c <= 'f')
  {
    c-= ('a' - 'A'); // aka 0x20
  }
  // validation
  if (c < '0' || (c > '9' && (c < 'A' || c > 'F')))
  {
    return -1; // invalid not 0-9A-F hex char
  }
  if (c <= '9')
  {
    return c - '0';
  }
  return c - 'A' + 10;
}

// Return a string that contains the copy of the referenced data.
std::string Slice::ToString(bool hex) const
{
  std::string result; // RVO/NRVO/move
  if (hex)
  {
    result.reserve(2 * size_);
    for (size_t i= 0; i < size_; ++i)
    {
      unsigned char c= data_[i];
      result.push_back(toHex(c >> 4));
      result.push_back(toHex(c & 0xf));
    }
    return result;
  }
  else
  {
    result.assign(data_, size_);
    return result;
  }
}

// Originally from rocksdb/utilities/ldb_cmd.h
bool Slice::DecodeHex(std::string *result) const
{
  std::string::size_type len= size_;
  if (len % 2)
  {
    // Hex string must be even number of hex digits to get complete bytes back
    return false;
  }
  if (!result)
  {
    return false;
  }
  result->clear();
  result->reserve(len / 2);

  for (size_t i= 0; i < len;)
  {
    int h1= fromHex(data_[i++]);
    if (h1 < 0)
    {
      return false;
    }
    int h2= fromHex(data_[i++]);
    if (h2 < 0)
    {
      return false;
    }
    result->push_back(static_cast<char>((h1 << 4) | h2));
  }
  return true;
}
} // namespace MyEloq
