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
#include "eloq_system_handler.h"

#define MYSQL_SERVER 1

#include "my_global.h"
#include "sql_reload.h"
#include "sql_class.h"
#include "sql_servers.h"

#include <glog/logging.h>

extern my_bool opt_bootstrap;
extern my_bool plugins_are_initialized;

namespace MyEloq
{
MariaSystemHandler::MariaSystemHandler()
{
  thd_= std::thread([this]() {
    // Set up thread local variables. Constructor of THD depends them.
    my_thread_init();

    while (shutdown_.load(std::memory_order_acquire) == false)
    {
      std::unique_lock lk(mux_);
      cv_.wait(lk, [this]() {
        return !work_queue_.empty() ||
               shutdown_.load(std::memory_order_acquire);
      });
      DLOG(INFO) << "System handler thread woke up";

      if (!work_queue_.empty())
      {
        DLOG(INFO) << "Processing work item in system handler thread";
        std::packaged_task<bool()> work= std::move(work_queue_.front());
        work_queue_.pop_front();
        lk.unlock();
        work();
        DLOG(INFO) << "Finished work item in system handler thread";
      }
    }

    DLOG(INFO) << "System handler thread exiting";
    my_thread_end();
    DLOG(INFO) << "System handler thread exited";
  });
}

void MariaSystemHandler::ReloadCache(std::function<void(bool)> done)
{
  std::packaged_task<bool()> work([done= std::move(done)]() {
    bool ok= true;

    if (!opt_bootstrap && plugins_are_initialized)
    {
      for (int i= 0; i < 5; i++)
      {
        int write_to_binlog= 0;
        ok= (reload_acl_and_cache(nullptr, REFRESH_GRANT, nullptr,
                                  &write_to_binlog) == 0);
        if (ok)
        {
          break;
        }
      }

      if (!ok)
      {
        sql_print_error("reload_acl_and_cache failed");
      }
    }
    done(ok);
    return ok;
  });

  DLOG(INFO) << "Submitting reload cache work to system handler thread";
  SubmitWork(std::move(work));
}

void MariaSystemHandler::Shutdown()
{
  std::unique_lock<std::mutex> lk(mux_);
  if (!shutdown_.load(std::memory_order_acquire))
  {
    DLOG(INFO) << "Shutting down system handler thread";
    shutdown_.store(true, std::memory_order_release);
    cv_.notify_one();
    lk.unlock();
    thd_.join();
  }
}

void MariaSystemHandler::SubmitWork(std::packaged_task<bool()> work)
{
  std::unique_lock<std::mutex> lk(mux_);
  if (shutdown_.load(std::memory_order_acquire) == false)
  {
    work_queue_.push_back(std::move(work));
    cv_.notify_one();
  }
}
} // namespace MyEloq
