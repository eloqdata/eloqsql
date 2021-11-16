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
#include "cass_big_number.h"

#include <cstring>

namespace MyEloq
{
BigNumber::BigNumber(std::string &bn_str) : big_number_(BN_new()), scale_(0)
{
  BIGNUM *bignum= big_number_;

  // Ensure the number is valid
  if (is_valid(bn_str))
  {
    // Check if number is a decimal
    size_t decimal_location= bn_str.find(".");
    if (decimal_location != std::string::npos)
    {
      // Remove decimal and calculate scale
      bn_str.erase(decimal_location, 1);
      scale_= bn_str.size() - decimal_location;
    }

    BN_dec2bn(&bignum, bn_str.c_str());
  }
  else
  {
    //   EXPECT_TRUE(false) << "Invalid BigNumber " << copy
    //                      << ": Value will be 0";
    BIGNUM *bignum= big_number_;
    BN_zero(bignum);
  }
}

BigNumber::BigNumber(const unsigned char *bytes, size_t bytes_length,
                     int32_t scale)
    : scale_(scale)
{
  if (bytes && bytes_length > 0)
  {
    // Determine if value is negative and handle two's complement
    bool is_negative= ((bytes[0] & 0x80) != 0);
    if (is_negative)
    {
      // Create a copy and convert to two's complement
      std::vector<unsigned char> twos_complement(bytes_length);
      memcpy(&twos_complement[0], bytes, bytes_length);
      bool is_carry= true;
      for (ssize_t i= bytes_length - 1; i >= 0; --i)
      {
        twos_complement[i]^= 0xFF;
        if (is_carry)
        {
          is_carry= ((++twos_complement[i]) == 0);
        }
      }
      big_number_= BN_bin2bn(&twos_complement[0], bytes_length, NULL);
      BN_set_negative(big_number_, 1);
    }
    else
    {
      big_number_= BN_bin2bn(bytes, bytes_length, NULL);
      BN_set_negative(big_number_, 0);
    }
  }
  else
  {
    big_number_= BN_new();
    BIGNUM *bignum= big_number_;
    BN_zero(bignum);
    scale_= 0;
  }
}

BigNumber::~BigNumber() { BN_free(big_number_); }

std::vector<unsigned char> BigNumber::encode_varint() const
{
  // Handle NULL and zero varint
  if (!big_number_ || BN_num_bytes(big_number_) == 0)
  {
    std::vector<unsigned char> bytes(1);
    bytes[0]= 0x00;
    return bytes;
  }

  size_t number_of_bytes= BN_num_bytes(big_number_) + 1;
  std::vector<unsigned char> bytes(number_of_bytes);
  if (BN_bn2bin(big_number_, &bytes[1]) > 0)
  {
    // Set the sign and convert to two's complement (if necessary)
    if (BN_is_negative(big_number_))
    {
      bool is_carry= true;
      for (ssize_t i= number_of_bytes - 1; i >= 0; --i)
      {
        bytes[i]^= 0xFF;
        if (is_carry)
        {
          is_carry= ((++bytes[i]) == 0);
        }
      }
      bytes[0]|= 0x80;
    }
    else
    {
      bytes[0]= 0x00;
    }
  }

  return bytes;
}

int BigNumber::compare(const BigNumber &rhs) const
{
  if (scale_ < rhs.scale_)
    return -1;
  if (scale_ > rhs.scale_)
    return 1;
  return BN_cmp(big_number_, rhs.big_number_);
}

std::string BigNumber::str() const
{
  std::string result;
  std::string normalized_result;

  if (!big_number_)
    return result;

  char *decimal= BN_bn2dec(big_number_);
  result.assign(decimal);
  OPENSSL_free(decimal);

  // Normalize - strip leading zeros
  result.erase(0, result.find_first_not_of('0'));
  if (result.size() == 0)
  {
    result= "0";
  }

  // return the result if result only has integer part.
  if (scale_ == 0)
  {
    return result;
  }
  else
  {
    // add prefix for 0.x, since heading zeros are truncated by big_number_
    if (result.size() <= (uint32_t) scale_)
    {
      normalized_result= "0.";
      int delta= scale_ - result.size();
      for (int i= 0; i < delta; i++)
      {
        normalized_result.append("0");
      }
      normalized_result.append(result);
    }
    else
    {
      size_t decimal_location= result.size() - scale_;
      normalized_result= result.substr(0, decimal_location) + "." +
                         result.substr(decimal_location);
    }
    return normalized_result;
  }
}
} // namespace MyEloq
