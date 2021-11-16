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
#include "bigtable_scanner.h"

namespace MyEloq
{
template <bool Direction>
void HashPartitionBigTableScanner<Direction>::Current(
    txservice::TxKey &key, const txservice::TxRecord *&rec,
    uint64_t &version_ts, bool &deleted_)
{
}

template <bool Direction>
bool HashPartitionBigTableScanner<Direction>::MoveNext()
{
  return false;
}

template <bool Direction> void HashPartitionBigTableScanner<Direction>::End()
{
}

template class HashPartitionBigTableScanner<true>;
template class HashPartitionBigTableScanner<false>;

void RangePartitionBigTableScanner::Current(txservice::TxKey &key,
                                            const txservice::TxRecord *&rec,
                                            uint64_t &version_ts,
                                            bool &deleted)
{
}

bool RangePartitionBigTableScanner::MoveNext() { return false; }

void RangePartitionBigTableScanner::End() {}
} // namespace MyEloq
