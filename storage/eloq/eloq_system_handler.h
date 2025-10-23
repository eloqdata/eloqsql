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
#include "tx_service/include/system_handler.h"

#include <atomic>
#include <deque>
#include <functional>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <future>

class THD;

namespace MyEloq
{
class MariaSystemHandler : public txservice::SystemHandler
{
public:
  static MariaSystemHandler &Instance()
  {
    static MariaSystemHandler instance;
    return instance;
  }

  void ReloadCache(std::function<void(bool)> done) override;

  void Shutdown() override;

private:
  MariaSystemHandler();
  ~MariaSystemHandler()= default;

  void SubmitWork(std::packaged_task<bool()> work);

private:
  std::thread thd_;
  std::deque<std::packaged_task<bool()>> work_queue_;
  std::condition_variable cv_;
  std::mutex mux_;
  std::atomic<bool> shutdown_{false};
};
} // namespace MyEloq
