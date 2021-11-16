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
  Copyright (c) DataStax, Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

#ifndef __TEST_BIG_NUMBER_HPP__
#define __TEST_BIG_NUMBER_HPP__
// #include "objects/object_base.hpp"
// #include "test_utils.hpp"

#include <algorithm>
#include <string>
#include <vector>

#include <openssl/bn.h>

#define TRIM_DELIMETERS " \f\n\r\t\v"

struct bignum_st;

namespace MyEloq
{

/**
 * Helper class for working with Java byte arrays (e.g. BigInteger and
 * BigDecimal); converting binary values.
 */
class BigNumber
{
public:
  BigNumber() : scale_(0) {}
  BigNumber(std::string &big_number);
  BigNumber(const unsigned char *bytes, size_t bytes_length, int32_t scale);
  ~BigNumber();

  /**
   * Comparison operation for BigNumber
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const BigNumber &rhs) const;
  /**
   * Encode the varint using two's complement
   *
   * @return Vector of bytes in two's complement
   */
  std::vector<unsigned char> encode_varint() const;

  /**
   * Get the scale for the big number
   *
   * @return Scale for number
   */
  int32_t scale() const { return scale_; }

  /**
   * Get the string representation of the big number
   *
   * @return String representation of numerical value
   */
  std::string str() const;

private:
  /**
   * OpenSSL big number
   */
  //   Object<BIGNUM, BN_free> big_number_;
  bignum_st *big_number_;

  /**
   * Scale for decimal values
   */
  int32_t scale_;

  /**
   * Ensure the big number is valid (digits and period)
   *
   * @param big_number String value to check
   * @return True if string is valid; false otherwise
   */
  bool is_valid(const std::string &big_number)
  {
    // Ensure the big number is a number
    if (big_number.empty())
      return false;
    if (big_number.find_first_not_of("0123456789-.") != std::string::npos)
    {
      return false;
    }

    // Ensure the big number has at most 1 decimal place
    if (std::count(big_number.begin(), big_number.end(), '.') > 1)
    {
      return false;
    }

    // Ensure the big number has at most 1 minus sign (and is at the beginning)
    size_t count= std::count(big_number.begin(), big_number.end(), '-');
    if (count > 1)
    {
      return false;
    }
    else if (count == 1)
    {
      if (big_number[0] != '-')
      {
        return false;
      }
    }

    // Big number is valid
    return true;
  }

  std::string trim(const std::string &input)
  {
    std::string result;
    if (!input.empty())
    {
      // Trim right
      result= input.substr(0, input.find_last_not_of(TRIM_DELIMETERS) + 1);
      if (!result.empty())
      {
        // Trim left
        result= result.substr(result.find_first_not_of(TRIM_DELIMETERS));
      }
    }
    return result;
  }
};
} // namespace MyEloq

#endif // __TEST_BIG_NUMBER_HPP__
