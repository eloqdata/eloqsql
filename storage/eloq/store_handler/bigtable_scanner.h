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

#include "tx_service/include/store/data_store_scanner.h"

namespace MyEloq
{
class BigTableScanner : public txservice::store::DataStoreScanner
{
public:
  BigTableScanner()= default;
  virtual ~BigTableScanner()= default;

private:
};

template <bool Direction>
class HashPartitionBigTableScanner : public BigTableScanner
{
public:
  void Current(txservice::TxKey &key, const txservice::TxRecord *&rec,
               uint64_t &version_ts, bool &deleted_) override;
  bool MoveNext() override;
  void End() override;

private:
};

class RangePartitionBigTableScanner : public BigTableScanner
{
public:
  void Current(txservice::TxKey &key, const txservice::TxRecord *&rec,
               uint64_t &version_ts, bool &deleted_) override;
  bool MoveNext() override;
  void End() override;

private:
};
} // namespace MyEloq
