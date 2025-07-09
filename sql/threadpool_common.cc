/* Copyright (C) 2012, 2020, MariaDB

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1335 USA */

#include "mariadb.h"
#include <violite.h>
#include <sql_priv.h>
#include <sql_class.h>
#include <my_pthread.h>
#include <scheduler.h>
#include <sql_connect.h>
#include <sql_audit.h>
#include <debug_sync.h>
#include <threadpool.h>
#include <sql_class.h>
#include <sql_parse.h>

#ifdef WITH_WSREP
#include "wsrep_trans_observer.h"
#endif /* WITH_WSREP */

#ifdef _WIN32
#include "threadpool_winsockets.h"
#endif

#ifdef COROUTINE_ENABLED
#include <boost/context/continuation.hpp>
#include <boost/context/stack_context.hpp>
#include <boost/context/preallocated.hpp>
#include "threadpool_generic.h"
#include <chrono>
#include <condition_variable>
#include <mutex>
#endif

/* Threadpool parameters */

uint threadpool_min_threads;
uint threadpool_idle_timeout;
uint threadpool_size;
uint threadpool_max_size;
uint threadpool_stall_limit;
uint threadpool_max_threads;
uint threadpool_oversubscribe;
uint threadpool_mode;
uint threadpool_prio_kickup_timer;
my_bool threadpool_exact_stats;
my_bool threadpool_dedicated_listener;

/* Stats */
TP_STATISTICS tp_stats;

#ifndef COROUTINE_ENABLED
static void  threadpool_remove_connection(THD *thd);
static THD*  threadpool_add_connection(CONNECT *connect, TP_connection *c);
#endif
static dispatch_command_return threadpool_process_request(THD *thd);
static void tp_wait_begin(THD *thd, int type);
static void tp_wait_end(THD *thd);

extern bool do_command(THD*);

static inline TP_connection *get_TP_connection(THD *thd)
{
  return (TP_connection *)thd->event_scheduler.data;
}

/*
  Worker threads contexts, and THD contexts.
  =========================================

  Both worker threads and connections have their sets of thread local variables
  At the moment it is mysys_var (this has specific data for dbug, my_error and
  similar goodies), and PSI per-client structure.

  Whenever query is executed following needs to be done:

  1. Save worker thread context.
  2. Change TLS variables to connection specific ones using thread_attach(THD*).
     This function does some additional work , e.g setting up
     thread_stack/thread_ends_here pointers.
  3. Process query
  4. Restore worker thread context.

  Connection login and termination follows similar schema w.r.t saving and
  restoring contexts.

  For both worker thread, and for the connection, mysys variables are created
  using my_thread_init() and freed with my_thread_end().

*/
struct Worker_thread_context
{
  PSI_thread *psi_thread;
  st_my_thread_var* mysys_var;

  void save()
  {
    psi_thread= PSI_CALL_get_thread();
    mysys_var= my_thread_var;
  }

  void restore()
  {
    PSI_CALL_set_thread(psi_thread);
    set_mysys_var(mysys_var);
    set_current_thd(nullptr);
  }
};


#ifdef HAVE_PSI_INTERFACE

/*
  The following fixes PSI "idle" psi instrumentation.
  The server assumes that connection  becomes idle
  just before net_read_packet() and switches to active after it.
  In out setup, server becomes idle when async socket io is made.
*/

extern void net_before_header_psi(struct st_net *net, void *user_data, size_t);

static void dummy_before_header(struct st_net *, void *, size_t)
{
}

static void re_init_net_server_extension(THD *thd)
{
  thd->m_net_server_extension.m_before_header = dummy_before_header;
}

#else

#define re_init_net_server_extension(thd)

#endif /* HAVE_PSI_INTERFACE */


static inline void set_thd_idle(THD *thd)
{
  thd->net.reading_or_writing= 1;
#ifdef HAVE_PSI_INTERFACE
  net_before_header_psi(&thd->net, thd, 0);
#endif
}

/*
  Attach/associate the connection with the OS thread,
*/
static void thread_attach(THD* thd)
{
#ifdef WITH_WSREP
  /* Wait until possible background rollback has finished before
     attaching the thd. */
  wsrep_wait_rollback_complete_and_acquire_ownership(thd);
#endif /* WITH_WSREP */
  set_mysys_var(thd->mysys_var);
  thd->thread_stack=(char*)&thd;
  thd->store_globals();
  PSI_CALL_set_thread(thd->get_psi());
  mysql_socket_set_thread_owner(thd->net.vio->mysql_socket);
}

/*
  Determine connection priority , using current
  transaction state and 'threadpool_priority' variable value.
*/
static TP_PRIORITY get_priority(TP_connection *c)
{
  DBUG_ASSERT(c->thd == current_thd);
  TP_PRIORITY prio= (TP_PRIORITY)c->thd->variables.threadpool_priority;
  if (prio == TP_PRIORITY_AUTO)
    prio= c->thd->transaction->is_active() ? TP_PRIORITY_HIGH : TP_PRIORITY_LOW;

  return prio;
}

#ifdef COROUTINE_ENABLED
static void CoroutineYield(boost::context::continuation &&main,
                           TP_connection_generic *conn)
{
  LIST *node= conn->acquired_mutexes;
  while (node)
  {
    // Release all mutexes acquired by this coroutine.
    // These mutexes will be reacquired when this coroutine resumes,
    // and until then, all mutex lock on this mutex will be blocked as well.
    // This makes sure that the same mutex is always locked and unlocked by the
    // same pthread. Otherwise if a coroutine yields when holding a mutex, then
    // resumed on a different pthread in the thread group, the mutex lock will
    // fail.
#ifdef SAFE_MUTEX
    inline_mysql_mutex_unlock((mysql_mutex_t *) node->data, __FILE__,
                              __LINE__);
#else
    inline_mysql_mutex_unlock((mysql_mutex_t *) node->data);
#endif
    node= node->next;
  }
  main= main.resume();
}

static void CoroutineResume(thread_group_t *thd_group,
                            TP_connection_generic *conn)
{
  CoroutineInfo &coro_info= *thd_group->coroutine_info_;
  coro_info.resume_queue_.Enqueue(conn);

  // By the time the first load() returns 0 and the mutex is locked,
  // the resumed request must have been put into the resume queue.
  // If the running thread count is non-zero, no need to wake up the
  // thread group, because the last active thread will pick it up
  // before trying to sleep.
  if (!thd_group->HasActiveWorker())
  {
#ifdef ELOQ_MODULE_ENABLED
    eloq::EloqModule::NotifyWorker(coro_info.group_id_);
#else
    mysql_mutex_lock(&thd_group->mutex);

    if (!thd_group->HasActiveWorker())
    {
      int err= thd_group->WakeOrCreateThread();
      if (err != 0)
      {
        sql_print_warning(
            "Resumed command fails to wake up the thread group %d, err: %d",
            coro_info.group_id_, err);
      }
    }

    mysql_mutex_unlock(&thd_group->mutex);
#endif
  }
}

boost::context::continuation
CloseConnCoroutine(boost::context::continuation &&caller, THD *thd)
{
  // The address of stack_thd marks the beginning of the coroutine's stack.
  THD *stack_thd= thd;
  thread_attach(stack_thd);
  TP_connection *c= get_TP_connection(thd);
  TP_connection_generic *connection=(TP_connection_generic *)c;
  connection->end_io();

  thread_group_t *thread_group= connection->thread_group;

  thd->yield_func_= [&main= caller, conn= connection]() {
    CoroutineYield(std::move(main), conn);
  };

  thd->resume_func_= [thd_group= thread_group, conn= connection]() {
    CoroutineResume(thd_group, conn);
  };

  thd->long_resume_func_= [thd_group= thread_group, conn= connection]() {
    thd_group->coroutine_info_->req_queue_.Enqueue(conn);
  };

  // The following code is copied from threadpool_remove_connection().
  // thread_attach(thd);
  thd->net.reading_or_writing = 0;
  end_connection(thd);
  close_connection(thd, 0);
  unlink_thd(thd);
  // PSI_CALL_delete_current_thread(); // before THD is destroyed
  // delete thd;
  /*
    Free resources associated with this connection:
    mysys thread_var and PSI thread.
  */
  // my_thread_end();

  thd->coro_status_= THD::CoroStatus::Finished;
  return std::move(caller);
}

void ResumeCoroutine(THD *thd, bool bind_socket = true)
{
  // The following code is copied from thread_attach() to restore session
  // context in the physical thread before resuming the command coroutine.
  // Different from thread_attach(), the 2nd line is intentionally commented.
  // thread_atack marks the begin of the coroutine stack and is set when the
  // coroutine starts. It is used by MySQL to track a query's execution stack
  // overflow.
  set_mysys_var(thd->mysys_var);
  // thd->thread_stack= (char *) &thd;
  thd->store_globals();
  PSI_CALL_set_thread(thd->get_psi());
  if (bind_socket)
  {
    mysql_socket_set_thread_owner(thd->net.vio->mysql_socket);
  }
  /* restore all mutexes of this thd that was released on yield.*/
  TP_connection_generic *connection= (TP_connection_generic *) get_TP_connection(thd);
  LIST *node= connection->acquired_mutexes;
  // Reacquire all mutex locks that we released on yield.
  while(node)
  {
#if defined(SAFE_MUTEX) || defined (HAVE_PSI_MUTEX_INTERFACE)
  inline_mysql_mutex_lock((mysql_mutex_t *)node->data, __FILE__, __LINE__);
#else
  inline_mysql_mutex_lock((mysql_mutex_t *)node->data);
#endif
    node= node->next;
  }

  thd->cmd_coroutine_= thd->cmd_coroutine_.resume();
}

void PrepareCoroutine(THD *thd, TP_connection_generic *conn)
{
  thread_group_t *thd_group= conn->thread_group;

  thd->SetThdGroupId(thd_group->coroutine_info_->group_id_);
  thd->coro_status_= THD::CoroStatus::Ongoing;
  thd_group->coroutine_info_->coro_cnt_.fetch_add(1,
                                                  std::memory_order_relaxed);
}

void FinishCoroutine(THD *thd, TP_connection_generic *conn)
{
  thd->coro_status_= THD::CoroStatus::Empty;
  thread_group_t *thd_group= conn->thread_group;
  thd_group->coroutine_info_->coro_cnt_.fetch_sub(1,
                                                  std::memory_order_relaxed);
}

THD *PrepareConnection(TP_connection *c)
{
  THD *thd= NULL;
  CONNECT *connect= c->connect;

  /*
    Create a new connection context: mysys_thread_var and PSI thread
    Store them in THD.
  */

  set_mysys_var(NULL);
  my_thread_init();
  st_my_thread_var *mysys_var= my_thread_var;
  PSI_CALL_set_thread(
      PSI_CALL_new_thread(key_thread_one_connection, connect, 0));
  if (!mysys_var || !(thd= connect->create_thd(NULL)))
  {
    /* Out of memory? */
    connect->close_and_delete();
    if (mysys_var)
      my_thread_end();
    return nullptr;
  }

  thd->event_scheduler.data= c;
  server_threads.insert(thd); // Make THD visible in show processlist
  delete connect;             // must be after server_threads.insert, see
                              // close_connections()
  thd->set_mysys_var(mysys_var);

  return thd;
}

boost::context::continuation
AddConnCoroutine(boost::context::continuation &&caller, TP_connection *c)
{
  THD *thd= c->thd;

  TP_connection_generic *conn= (TP_connection_generic *) c;
  thread_group_t *thread_group= conn->thread_group;

  /* Login. */
  thread_attach(thd);
  re_init_net_server_extension(thd);
  ulonglong now= microsecond_interval_timer();
  thd->prior_thr_create_utime= now;
  thd->start_utime= now;
  thd->thr_create_utime= now;

  setup_connection_thread_globals(thd);

  thd->yield_func_= [&main= caller, conn= conn]() {
    CoroutineYield(std::move(main), conn);
  };

  thd->resume_func_= [thd_group= thread_group, conn= conn]() {
    CoroutineResume(thd_group, conn);
  };

  thd->long_resume_func_= [thd_group= thread_group, conn= conn]() {
    thd_group->coroutine_info_->req_queue_.Enqueue(conn);
  };

  if (thd_prepare_connection(thd))
    goto end;

  c->init_vio(thd->net.vio);

  /*
    Check if THD is ok, as prepare_new_connection_state()
    can fail, for example if init command failed.
  */
  if (!thd_is_connection_alive(thd))
    goto end;

  thd->skip_wait_timeout= true;
  set_thd_idle(thd);
  c->state= TP_STATE_CONNECTED;
  return std::move(caller);

end:
  c->state= TP_STATE_CONNECTED_ERR;
  return std::move(caller);
}
#endif

void tp_callback(TP_connection *c)
{
  DBUG_ASSERT(c);

  c->being_processed_.store(true, std::memory_order_relaxed);

  Worker_thread_context worker_context;
  worker_context.save();

  THD *thd= c->thd;

  if (unlikely(!thd) || c->state == TP_STATE_CONNECTING)
  {
    /* No THD, need to login first. */
    DBUG_ASSERT(c->connect);
#ifdef COROUTINE_ENABLED
    if (thd == nullptr)
    {
      thd= PrepareConnection(c);
      if (thd == nullptr)
      {
        goto error;
      }

      c->thd= thd;
      c->state= TP_STATE_CONNECTING;
      PrepareCoroutine(thd, (TP_connection_generic *) c);

      boost::context::stack_context scx= thd->CoroStackContext();
      boost::context::preallocated prealloc(scx.sp, THD::sql_coro_stack_size,
                                            scx);
      thd->cmd_coroutine_= boost::context::callcc(
          std::allocator_arg, prealloc, THD::NoopAllocator(),
          std::bind(AddConnCoroutine, std::placeholders::_1, c));
    }
    else if (c->state == TP_STATE_CONNECTING)
    {
      ResumeCoroutine(thd, false);
    }

    if (c->state == TP_STATE_CONNECTING)
    {
      // The add connection coroutine has not finished.
      worker_context.restore();
      c->being_processed_.store(false, std::memory_order_relaxed);
      return;
    }
    else
    {
      // The add connection coroutine has finished.
      c->connect= 0;
      FinishCoroutine(thd, (TP_connection_generic *) c);

      if (c->state == TP_STATE_CONNECTED_ERR)
      {
        goto error;
      }
    }
#else
    thd= c->thd= threadpool_add_connection(c->connect, c);
    if (!thd)
    {
      /* Bail out on connect error.*/
      goto error;
    }
    c->connect= 0;
#endif
  }
#ifdef COROUTINE_ENABLED
  else if (c->state == TP_STATE_CLOSING)
  {
    goto error;
  }
#endif
  else
  {
    c->state = TP_STATE_RUNNING;
retry:
    switch(threadpool_process_request(thd))
    {
      case DISPATCH_COMMAND_WOULDBLOCK:
        if (!thd->async_state.try_suspend())
        {
          /*
            All async operations finished meanwhile, thus nobody is will wake up
            this THD. Therefore, we'll resume "manually" here.
          */
          thd->async_state.m_state = thd_async_state::enum_async_state::RESUMED;
          goto retry;
        }
        worker_context.restore();
        c->being_processed_.store(false, std::memory_order_relaxed);
        return;
      case DISPATCH_COMMAND_CLOSE_CONNECTION:
        /* QUIT or an error occurred. */
        goto error;
      case DISPATCH_COMMAND_SUCCESS:
        break;
      case DISPATCH_COMMAND_YIELD:
        worker_context.restore();
        c->being_processed_.store(false, std::memory_order_relaxed);
        return;
    }
    thd->async_state.m_state= thd_async_state::enum_async_state::NONE;
  }

  /* Set priority */
  c->priority= get_priority(c);

  /* Read next command from client. */
  c->set_io_timeout(thd->get_net_wait_timeout());
  c->state= TP_STATE_IDLE;
  if (c->start_io())
    goto error;

  worker_context.restore();
  c->being_processed_.store(false, std::memory_order_relaxed);
  return;

error:
#ifdef COROUTINE_ENABLED
  if (thd)
  {
    c->state = TP_STATE_CLOSING;
    if (thd->coro_status_ == THD::CoroStatus::Empty)
    {
      PrepareCoroutine(thd, (TP_connection_generic *)c);

      boost::context::stack_context scx= thd->CoroStackContext();
      boost::context::preallocated prealloc(scx.sp, THD::sql_coro_stack_size,
                                            scx);
      thd->cmd_coroutine_= boost::context::callcc(
        std::allocator_arg, prealloc, THD::NoopAllocator(),
        std::bind(CloseConnCoroutine, std::placeholders::_1, thd));
    }
    else if (thd->coro_status_ == THD::CoroStatus::Ongoing)
    {
      ResumeCoroutine(thd, false);
    }

    if (thd->coro_status_ == THD::CoroStatus::Finished)
    {
      FinishCoroutine(thd, (TP_connection_generic *)c);

      PSI_CALL_delete_current_thread(); // before THD is destroyed
      delete thd;
      /*
        Free resources associated with this connection:
        mysys thread_var and PSI thread.
      */
      my_thread_end();

      delete c;
      worker_context.restore();
    }
    else
    {
      c->being_processed_.store(false, std::memory_order_relaxed);
      worker_context.restore();
    }
  }
  else
  {
    delete c;
    worker_context.restore();
  }
#else
  c->thd= 0;
  if (thd)
  {
    threadpool_remove_connection(thd);
  }
  delete c;
  worker_context.restore();
#endif
}

#ifdef COROUTINE_ENABLED
struct SyncTuple
{
  std::mutex mux_;
  std::condition_variable cv_;
  bool finished_{false};
};
#endif

#ifndef COROUTINE_ENABLED
static THD *threadpool_add_connection(CONNECT *connect, TP_connection *c)
{
  THD *thd= NULL;

  /*
    Create a new connection context: mysys_thread_var and PSI thread
    Store them in THD.
  */

  set_mysys_var(NULL);
  my_thread_init();
  st_my_thread_var* mysys_var= my_thread_var;
  PSI_CALL_set_thread(PSI_CALL_new_thread(key_thread_one_connection, connect, 0));
  if (!mysys_var ||!(thd= connect->create_thd(NULL)))
  {
    /* Out of memory? */
    connect->close_and_delete();
    if (mysys_var)
      my_thread_end();
    return NULL;
  }

  thd->event_scheduler.data= c;
  server_threads.insert(thd); // Make THD visible in show processlist
  delete connect; // must be after server_threads.insert, see close_connections()
  thd->set_mysys_var(mysys_var);

  /* Login. */
  thread_attach(thd);
  re_init_net_server_extension(thd);
  ulonglong now= microsecond_interval_timer();
  thd->prior_thr_create_utime= now;
  thd->start_utime= now;
  thd->thr_create_utime= now;

  setup_connection_thread_globals(thd);

  if (thd_prepare_connection(thd))
    goto end;

  c->init_vio(thd->net.vio);

  /*
    Check if THD is ok, as prepare_new_connection_state()
    can fail, for example if init command failed.
  */
  if (!thd_is_connection_alive(thd))
    goto end;

  thd->skip_wait_timeout= true;
  set_thd_idle(thd);
  return thd;

end:
  threadpool_remove_connection(thd);
  return NULL;
}


static void threadpool_remove_connection(THD *thd)
{
  thread_attach(thd);
  thd->net.reading_or_writing = 0;
  end_connection(thd);
  close_connection(thd, 0);
  unlink_thd(thd);
  PSI_CALL_delete_current_thread(); // before THD is destroyed
  delete thd;

  /*
    Free resources associated with this connection:
    mysys thread_var and PSI thread.
  */
  my_thread_end();
}
#endif


/*
  Ensure that proper error message is sent to client,
  and "aborted" message appears in the log in case of
  wait timeout.

  See also timeout handling in net_serv.cc
*/
static void handle_wait_timeout(THD *thd)
{
#ifdef COROUTINE_ENABLED
  set_current_thd(thd);
#endif
  thd->get_stmt_da()->reset_diagnostics_area();
  thd->reset_killed();
  my_error(ER_NET_READ_INTERRUPTED, MYF(0));
  thd->net.last_errno= ER_NET_READ_INTERRUPTED;
  thd->net.error= 2;
}

/** Check if some client data is cached in thd->net or thd->net.vio */
static bool has_unread_data(THD* thd)
{
  NET *net= &thd->net;
  if (net->compress && net->remain_in_buf)
    return true;
  Vio *vio= net->vio;
  return vio->has_data(vio);
}

#ifdef COROUTINE_ENABLED
boost::context::continuation
SqlCmdCoroutine(boost::context::continuation &&caller, THD *thd)
{
  // The address of stack_thd marks the beginning of the coroutine's stack.
  THD *stack_thd= thd;
  thread_attach(stack_thd);

  TP_connection *c= get_TP_connection(thd);
  TP_connection_generic *connection=(TP_connection_generic *)c;
  thread_group_t *thread_group= connection->thread_group;
  thd->yield_func_= [&main= caller, conn= connection]() {
    CoroutineYield(std::move(main), conn);
  };

  thd->resume_func_= [thd_group= thread_group, conn= connection]() {
    CoroutineResume(thd_group, conn);
  };

  thd->long_resume_func_= [thd_group= thread_group, conn= connection]() {
    thd_group->coroutine_info_->req_queue_.Enqueue(conn);
  };

  dispatch_command_return cmd_ret= DISPATCH_COMMAND_SUCCESS;

  for (;;)
  {
    thd->net.reading_or_writing= 0;
    if (mysql_audit_release_required(thd))
      mysql_audit_release(thd);

    cmd_ret= do_command(thd, false);
    switch (cmd_ret)
    {
    case DISPATCH_COMMAND_WOULDBLOCK: {
      thd->coro_ret_= 2; // DISPATCH_COMMAND_WOULDBLOCK
      thd->coro_status_= THD::CoroStatus::Finished;
      return std::move(caller);
    }
    case DISPATCH_COMMAND_CLOSE_CONNECTION: {
      thd->coro_ret_= 1; // DISPATCH_COMMAND_CLOSE_CONNECTION;
      thd->coro_status_= THD::CoroStatus::Finished;
      return std::move(caller);
    }
    case DISPATCH_COMMAND_SUCCESS:
      break;
    case DISPATCH_COMMAND_YIELD:
      assert("do_command() cannot return DISPATCH_COMMAND_YIELD");
      break;
    }

    if (!thd_is_connection_alive(thd))
    {
      thd->coro_status_= THD::CoroStatus::Finished;
      thd->coro_ret_= 1; // DISPATCH_COMMAND_CLOSE_CONNECTION;
      return std::move(caller);
    }

    set_thd_idle(thd);

    if (!has_unread_data(thd))
    {
      /* More info on this debug sync is in sql_parse.cc*/
      DEBUG_SYNC(thd, "before_do_command_net_read");
      thd->coro_status_= THD::CoroStatus::Finished;
      thd->coro_ret_= (uint8_t) cmd_ret;
      return std::move(caller);
    }
  }

  return std::move(caller);
}
#endif

static TP_pool *pool;

#if defined(COROUTINE_ENABLED) && defined(IOURING_ENABLED)
extern thread_local IoUringWrapper iouring_wrap;
#endif

/**
 Process a single client request or a single batch.
*/
static dispatch_command_return threadpool_process_request(THD *thd)
{
  dispatch_command_return retval= DISPATCH_COMMAND_SUCCESS;

#ifndef COROUTINE_ENABLED
  thread_attach(thd);
#else
  TP_connection *c= get_TP_connection(thd);
#endif

  if(thd->async_state.m_state == thd_async_state::enum_async_state::RESUMED)
    goto resume;

  if (thd->killed >= KILL_CONNECTION && 
      thd->coro_status_ == THD::CoroStatus::Empty)
  {
    /*
      killed flag was set by timeout handler
      or KILL command. Return error.
    */
    retval= DISPATCH_COMMAND_CLOSE_CONNECTION;
    if(thd->killed == KILL_WAIT_TIMEOUT)
      handle_wait_timeout(thd);
    goto end;
  }

#ifdef COROUTINE_ENABLED
  if (thd->coro_status_ == THD::CoroStatus::Empty)
  {
    PrepareCoroutine(thd, (TP_connection_generic *) c);

    boost::context::stack_context scx= thd->CoroStackContext();
    boost::context::preallocated prealloc(scx.sp, THD::sql_coro_stack_size,
                                          scx);
#ifdef IOURING_ENABLED
    if (iouring_wrap.init_success_)
    {
      thd->iouring_= &iouring_wrap;
    }
    else
    {
      thd->iouring_= nullptr;
    }
#endif
    thd->cmd_coroutine_= boost::context::callcc(
        std::allocator_arg, prealloc, THD::NoopAllocator(),
        std::bind(SqlCmdCoroutine, std::placeholders::_1, thd));
  }
  else if (thd->coro_status_ == THD::CoroStatus::Ongoing)
  {
#ifdef IOURING_ENABLED
    if (iouring_wrap.init_success_)
    {
      thd->iouring_= &iouring_wrap;
    }
    else
    {
      thd->iouring_= nullptr;
    }
#endif
    ResumeCoroutine(thd);
  }

  if (thd->coro_status_ == THD::CoroStatus::Finished)
  {
    FinishCoroutine(thd, (TP_connection_generic *) c);

    switch (thd->coro_ret_)
    {
    case 0:
      return DISPATCH_COMMAND_SUCCESS;
    case 1:
      return DISPATCH_COMMAND_CLOSE_CONNECTION;
    default:
      return DISPATCH_COMMAND_WOULDBLOCK;
    }
  }
  else
  {
    return DISPATCH_COMMAND_YIELD;
  }
#endif

  /*
    In the loop below, the flow is essentially the copy of
    thead-per-connections
    logic, see do_handle_one_connection() in sql_connect.c

    The goal is to execute a single query, thus the loop is normally executed 
    only once. However for SSL connections, it can be executed multiple times 
    (SSL can preread and cache incoming data, and vio->has_data() checks if it 
    was the case).
  */
  for(;;)
  {
    thd->net.reading_or_writing= 0;
    if (mysql_audit_release_required(thd))
      mysql_audit_release(thd);

resume:
    retval= do_command(thd, false);
    switch(retval)
    {
      case DISPATCH_COMMAND_WOULDBLOCK:
      case DISPATCH_COMMAND_CLOSE_CONNECTION:
      case DISPATCH_COMMAND_YIELD:
        goto end;
      case DISPATCH_COMMAND_SUCCESS:
        break;
    }

    if (!thd_is_connection_alive(thd))
    {
      retval=DISPATCH_COMMAND_CLOSE_CONNECTION;
      goto end;
    }

    set_thd_idle(thd);

    if (!has_unread_data(thd))
    {
      /* More info on this debug sync is in sql_parse.cc*/
      DEBUG_SYNC(thd, "before_do_command_net_read");
      goto end;
    }
  }

end:
  return retval;
}


static bool tp_init()
{

#ifdef _WIN32
  if (threadpool_mode == TP_MODE_WINDOWS)
    pool= new (std::nothrow) TP_pool_win;
  else
    pool= new (std::nothrow) TP_pool_generic;
#else
  pool= new (std::nothrow) TP_pool_generic;
#endif
  if (!pool)
    return true;
  if (pool->init())
  {
    delete pool;
    pool= 0;
    return true;
  }
#ifdef _WIN32
  init_win_aio_buffers(max_connections);
#endif
  return false;
}

static void tp_add_connection(CONNECT *connect)
{
  TP_connection *c= pool->new_connection(connect);
  DBUG_EXECUTE_IF("simulate_failed_connection_1", delete c ; c= 0;);
  if (c)
    pool->add(c);
  else
    connect->close_and_delete();
}

int tp_get_idle_thread_count()
{
  return pool? pool->get_idle_thread_count(): 0;
}

int tp_get_thread_count()
{
  return pool ? pool->get_thread_count() : 0;
}

void tp_set_min_threads(uint val)
{
  if (pool)
    pool->set_min_threads(val);
}


void tp_set_max_threads(uint val)
{
  if (pool)
    pool->set_max_threads(val);
}

void tp_set_threadpool_size(uint val)
{
  if (pool)
    pool->set_pool_size(val);
}


void tp_set_threadpool_stall_limit(uint val)
{
  if (pool)
    pool->set_stall_limit(val);
}


void tp_timeout_handler(TP_connection *c)
{
  if (c->state != TP_STATE_IDLE)
    return;
  THD *thd= c->thd;
  mysql_mutex_lock(&thd->LOCK_thd_kill);
  Vio *vio= thd->net.vio;
  if (vio && (vio_pending(vio) > 0 || vio->has_data(vio)) &&
      c->state == TP_STATE_IDLE)
  {
    /*
     There is some data on that connection, i.e
     i.e there was no inactivity timeout.
     Don't kill.
    */
    c->state= TP_STATE_PENDING;
  }
  else if (c->state == TP_STATE_IDLE)
  {
    thd->set_killed_no_mutex(KILL_WAIT_TIMEOUT);
    c->priority= TP_PRIORITY_HIGH;
    post_kill_notification(thd);
  }
  mysql_mutex_unlock(&thd->LOCK_thd_kill);
}

MY_ALIGNED(CPU_LEVEL1_DCACHE_LINESIZE) Atomic_counter<unsigned long long> tp_waits[THD_WAIT_LAST];

static void tp_wait_begin(THD *thd, int type)
{
  TP_connection *c = get_TP_connection(thd);
  if (c)
  {
    DBUG_ASSERT(type > 0 && type < THD_WAIT_LAST);
    tp_waits[type]++;
    c->wait_begin(type);
  }
}


static void tp_wait_end(THD *thd)
{
  TP_connection *c = get_TP_connection(thd);
  if (c)
    c->wait_end();
}


static void tp_end()
{
  delete pool;
#ifdef _WIN32
  destroy_win_aio_buffers();
#endif
}

static void tp_post_kill_notification(THD *thd)
{
  TP_connection *c= get_TP_connection(thd);
  if (c)
    c->priority= TP_PRIORITY_HIGH;
  post_kill_notification(thd);
}

/* Resume previously suspended THD */
static void tp_resume(THD* thd)
{
  DBUG_ASSERT(thd->async_state.m_state == thd_async_state::enum_async_state::SUSPENDED);
  thd->async_state.m_state = thd_async_state::enum_async_state::RESUMED;
  TP_connection* c = get_TP_connection(thd);
  pool->resume(c);
}

static scheduler_functions tp_scheduler_functions=
{
  0,                                  // max_threads
  NULL,
  NULL,
  tp_init,                            // init
  tp_add_connection,                  // add_connection
  tp_wait_begin,                      // thd_wait_begin
  tp_wait_end,                        // thd_wait_end
  tp_post_kill_notification,          // post kill notification
  tp_end,                              // end
  tp_resume
};

void pool_of_threads_scheduler(struct scheduler_functions *func,
    ulong *arg_max_connections,
    Atomic_counter<uint> *arg_connection_count)
{
  *func = tp_scheduler_functions;
  func->max_threads= threadpool_max_threads;
  func->max_connections= arg_max_connections;
  func->connection_count= arg_connection_count;
  scheduler_init();
}
