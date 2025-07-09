/* Copyright (C) 2012, 2020, MariaDB Corporation.

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

#if (defined HAVE_POOL_OF_THREADS) && !defined(EMBEDDED_LIBRARY)

#include "threadpool_generic.h"
#include "mariadb.h"
#include <violite.h>
#include <sql_priv.h>
#include <sql_class.h>
#include <my_pthread.h>
#include <scheduler.h>
#include <sql_connect.h>
#include <mysqld.h>
#include <debug_sync.h>
#include <time.h>
#include <sql_plist.h>
#include <threadpool.h>
#include <algorithm>
#include <thread>
#ifdef _WIN32
#include "threadpool_winsockets.h"
#define OPTIONAL_IO_POLL_READ_PARAM this
#else 
#define OPTIONAL_IO_POLL_READ_PARAM 0
#endif

#if defined(COROUTINE_ENABLED) && defined(IOURING_ENABLED)
#include <liburing.h>
#endif
#include "is_sql_thd.h"
#ifdef ELOQ_MODULE_ENABLED
#include <bthread/bthread.h>
#endif

static void io_poll_close(TP_file_handle fd)
{
#ifdef _WIN32
  CloseHandle(fd);
#else
  close(fd);
#endif
}

/** Maximum number of native events a listener can read in one go */
#define MAX_EVENTS 1024

/** Indicates that threadpool was initialized*/
static bool threadpool_started= false; 

/* 
  Define PSI Keys for performance schema. 
  We have a mutex per group, worker threads, condition per worker thread, 
  and timer thread  with its own mutex and condition.
*/
 
 
#ifdef HAVE_PSI_INTERFACE
static PSI_mutex_key key_group_mutex;
static PSI_mutex_key key_timer_mutex;
static PSI_mutex_info mutex_list[]=
{
  { &key_group_mutex, "group_mutex", 0},
  { &key_timer_mutex, "timer_mutex", PSI_FLAG_GLOBAL}
};

static PSI_cond_key key_worker_cond;
static PSI_cond_key key_timer_cond;
static PSI_cond_info cond_list[]=
{
  { &key_worker_cond, "worker_cond", 0},
  { &key_timer_cond, "timer_cond", PSI_FLAG_GLOBAL}
};

static PSI_thread_key key_worker_thread;
static PSI_thread_key key_timer_thread;
static PSI_thread_info	thread_list[] =
{
 {&key_worker_thread, "worker_thread", 0},
 {&key_timer_thread, "timer_thread", PSI_FLAG_GLOBAL}
};

/* Macro to simplify performance schema registration */
#define PSI_register(X) \
 if(PSI_server) PSI_server->register_ ## X("threadpool", X ## _list, array_elements(X ## _list))
#else
#define PSI_register(X) /* no-op */
#endif

thread_group_t *all_groups;
static uint group_count;
static Atomic_counter<uint32_t> shutdown_group_count;
#ifdef ELOQ_MODULE_ENABLED
static MariaModule maria_module;
#endif

/**
 Used for printing "pool blocked" message, see
 print_pool_blocked_message();
*/
static ulonglong pool_block_start;

/* Global timer for all groups  */
struct pool_timer_t
{
  mysql_mutex_t mutex;
  mysql_cond_t cond;
  volatile uint64 current_microtime;
  std::atomic<uint64_t> next_timeout_check;
  int  tick_interval;
  bool shutdown;
  pthread_t timer_thread_id;
};

static pool_timer_t pool_timer;

#ifndef COROUTINE_ENABLED
static void queue_put(thread_group_t *thread_group, TP_connection_generic *connection);
#endif
static void queue_put(thread_group_t *thread_group, native_event *ev, int cnt);
static int  wake_thread(thread_group_t *thread_group,bool due_to_stall);
static int wake_or_create_thread(thread_group_t *thread_group,
                                 bool due_to_stall= false,
                                 bool force_create= false);
static int create_worker(thread_group_t *thread_group, bool due_to_stall,
                         bool force_create= false);
static void *worker_main(void *param);
#ifdef COROUTINE_ENABLED
static void lockfree_check_stall(thread_group_t *thread_group);
#else
static void check_stall(thread_group_t *thread_group);
#endif
static void set_next_timeout_check(ulonglong abstime);
static void print_pool_blocked_message(bool);

/**
 Asynchronous network IO.

 We use native edge-triggered network IO multiplexing facility.
 This maps to different APIs on different Unixes.

 Supported are currently Linux with epoll, Solaris with event ports,
 OSX and BSD with kevent, Windows with IOCP. All those API's are used with one-shot flags
 (the event is signalled once client has written something into the socket,
 then socket is removed from the "poll-set" until the  command is finished,
 and we need to re-arm/re-register socket)

 No implementation for poll/select is currently provided.

 The API closely resembles all of the above mentioned platform APIs
 and consists of following functions.

 - io_poll_create()
 Creates an io_poll descriptor
 On Linux: epoll_create()

 - io_poll_associate_fd(int poll_fd, TP_file_handle fd, void *data, void *opt)
 Associate file descriptor with io poll descriptor
 On Linux : epoll_ctl(..EPOLL_CTL_ADD))

 - io_poll_disassociate_fd(TP_file_handle pollfd, TP_file_handle fd)
  Associate file descriptor with io poll descriptor
  On Linux: epoll_ctl(..EPOLL_CTL_DEL)


 - io_poll_start_read(int poll_fd,int fd, void *data, void *opt)
 The same as io_poll_associate_fd(), but cannot be used before
 io_poll_associate_fd() was called.
 On Linux : epoll_ctl(..EPOLL_CTL_MOD)

 - io_poll_wait (TP_file_handle pollfd, native_event *native_events, int maxevents,
   int timeout_ms)

 wait until one or more descriptors added with io_poll_associate_fd()
 or io_poll_start_read() becomes readable. Data associated with
 descriptors can be retrieved from native_events array, using
 native_event_get_userdata() function.

 On Linux: epoll_wait()
*/

#if defined (__linux__)
#ifndef EPOLLRDHUP
/* Early 2.6 kernel did not have EPOLLRDHUP */
#define EPOLLRDHUP 0
#endif
static TP_file_handle io_poll_create()
{
  return epoll_create(1);
}


int io_poll_associate_fd(TP_file_handle pollfd, TP_file_handle fd, void *data, void*)
{
  struct epoll_event ev;
  ev.data.u64= 0; /* Keep valgrind happy */
  ev.data.ptr= data;
#ifdef COROUTINE_ENABLED
  ev.events=  EPOLLIN|EPOLLET|EPOLLERR|EPOLLRDHUP;
#else
  ev.events=  EPOLLIN|EPOLLET|EPOLLERR|EPOLLRDHUP|EPOLLONESHOT;
#endif
  return epoll_ctl(pollfd, EPOLL_CTL_ADD,  fd, &ev);
}



int io_poll_start_read(TP_file_handle pollfd, TP_file_handle fd, void *data, void *)
{
  struct epoll_event ev;
  ev.data.u64= 0; /* Keep valgrind happy */
  ev.data.ptr= data;
  ev.events=  EPOLLIN|EPOLLET|EPOLLERR|EPOLLRDHUP|EPOLLONESHOT;
  return epoll_ctl(pollfd, EPOLL_CTL_MOD, fd, &ev);
}

int io_poll_disassociate_fd(TP_file_handle pollfd, TP_file_handle fd)
{
  struct epoll_event ev;
  return epoll_ctl(pollfd, EPOLL_CTL_DEL,  fd, &ev);
}


/*
 Wrapper around epoll_wait.
 NOTE - in case of EINTR, it restarts with original timeout. Since we use
 either infinite or 0 timeouts, this is not critical
*/
int io_poll_wait(TP_file_handle pollfd, native_event *native_events, int maxevents,
              int timeout_ms)
{
  int ret;
  do
  {
    ret = epoll_wait(pollfd, native_events, maxevents, timeout_ms);
  }
  while(ret == -1 && errno == EINTR);
  return ret;
}


static void *native_event_get_userdata(native_event *event)
{
  return event->data.ptr;
}

#elif defined(HAVE_KQUEUE)

/*
  NetBSD prior to 9.99.17 is incompatible with other BSDs, last parameter
  in EV_SET macro (udata, user data) needs to be intptr_t, whereas it needs
  to be void* everywhere else.
*/

#ifdef __NetBSD__
#include <sys/param.h>
#  if !__NetBSD_Prereq__(9,99,17)
#define MY_EV_SET(a, b, c, d, e, f, g) EV_SET(a, b, c, d, e, f, (intptr_t)g)
#  endif
#endif

#ifndef MY_EV_SET
#define MY_EV_SET(a, b, c, d, e, f, g) EV_SET(a, b, c, d, e, f, g)
#endif


TP_file_handle io_poll_create()
{
  return kqueue();
}

int io_poll_start_read(TP_file_handle pollfd, TP_file_handle fd, void *data,void *)
{
  struct kevent ke;
  MY_EV_SET(&ke, fd, EVFILT_READ, EV_ADD|EV_ONESHOT,
         0, 0, data);
  return kevent(pollfd, &ke, 1, 0, 0, 0);
}


int io_poll_associate_fd(TP_file_handle pollfd, TP_file_handle fd, void *data,void *)
{
  struct kevent ke;
  MY_EV_SET(&ke, fd, EVFILT_READ, EV_ADD|EV_ONESHOT,
         0, 0, data);
  return io_poll_start_read(pollfd,fd, data, 0);
}


int io_poll_disassociate_fd(TP_file_handle pollfd, TP_file_handle fd)
{
  struct kevent ke;
  MY_EV_SET(&ke,fd, EVFILT_READ, EV_DELETE, 0, 0, 0);
  return kevent(pollfd, &ke, 1, 0, 0, 0);
}


int io_poll_wait(TP_file_handle pollfd, struct kevent *events, int maxevents, int timeout_ms)
{
  struct timespec ts;
  int ret;
  if (timeout_ms >= 0)
  {
    ts.tv_sec= timeout_ms/1000;
    ts.tv_nsec= (timeout_ms%1000)*1000000;
  }
  do
  {
    ret= kevent(pollfd, 0, 0, events, maxevents,
               (timeout_ms >= 0)?&ts:NULL);
  }
  while (ret == -1 && errno == EINTR);
  return ret;
}

static void* native_event_get_userdata(native_event *event)
{
  return (void *)event->udata;
}

#elif defined (__sun)

static TP_file_handle io_poll_create()
{
  return port_create();
}

int io_poll_start_read(TP_file_handle pollfd, TP_file_handle fd, void *data, void *)
{
  return port_associate(pollfd, PORT_SOURCE_FD, fd, POLLIN, data);
}

static int io_poll_associate_fd(TP_file_handle pollfd, TP_file_handle fd, void *data, void *)
{
  return io_poll_start_read(pollfd, fd, data, 0);
}

int io_poll_disassociate_fd(TP_file_handle pollfd, TP_file_handle fd)
{
  return port_dissociate(pollfd, PORT_SOURCE_FD, fd);
}

int io_poll_wait(TP_file_handle pollfd, native_event *events, int maxevents, int timeout_ms)
{
  struct timespec ts;
  int ret;
  uint_t nget= 1;
  if (timeout_ms >= 0)
  {
    ts.tv_sec= timeout_ms/1000;
    ts.tv_nsec= (timeout_ms%1000)*1000000;
  }
  do
  {
    ret= port_getn(pollfd, events, maxevents, &nget,
            (timeout_ms >= 0)?&ts:NULL);
  }
  while (ret == -1 && errno == EINTR);
  DBUG_ASSERT(nget < INT_MAX);
  return (int)nget;
}

static void* native_event_get_userdata(native_event *event)
{
  return event->portev_user;
}

#elif defined(_WIN32)


static TP_file_handle io_poll_create()
{
  return CreateIoCompletionPort(INVALID_HANDLE_VALUE, 0, 0, 0);
}


int io_poll_start_read(TP_file_handle pollfd, TP_file_handle fd, void *, void *opt)
{
  auto c= (TP_connection_generic *) opt;
  return (int) c->win_sock.begin_read();
}


static int io_poll_associate_fd(TP_file_handle pollfd, TP_file_handle fd, void *data, void *opt)
{
  HANDLE h= CreateIoCompletionPort(fd, pollfd, (ULONG_PTR)data, 0);
  if (!h)
    return -1;
  return io_poll_start_read(pollfd,fd, 0, opt);
}


typedef LONG NTSTATUS;

typedef struct _IO_STATUS_BLOCK {
  union {
    NTSTATUS Status;
    PVOID Pointer;
  };
  ULONG_PTR Information;
} IO_STATUS_BLOCK, * PIO_STATUS_BLOCK;

struct FILE_COMPLETION_INFORMATION {
  HANDLE Port;
  PVOID Key;
};

enum FILE_INFORMATION_CLASS {
  FileReplaceCompletionInformation = 0x3D
};


typedef NTSTATUS(WINAPI* pNtSetInformationFile)(HANDLE, PIO_STATUS_BLOCK, PVOID, ULONG, FILE_INFORMATION_CLASS);

int io_poll_disassociate_fd(TP_file_handle pollfd, TP_file_handle fd)
{
  static pNtSetInformationFile my_NtSetInformationFile = (pNtSetInformationFile)
    GetProcAddress(GetModuleHandle("ntdll"), "NtSetInformationFile");
  if (!my_NtSetInformationFile)
    return -1; /* unexpected, we only support Windows 8.1+*/
  IO_STATUS_BLOCK iosb{};
  FILE_COMPLETION_INFORMATION fci{};
  if (my_NtSetInformationFile(fd,&iosb,&fci,sizeof(fci),FileReplaceCompletionInformation))
    return -1;
  return 0;
}


static void *native_event_get_userdata(native_event *event)
{
  return (void *) event->lpCompletionKey;
}

int io_poll_wait(TP_file_handle pollfd, native_event *events, int maxevents,
                 int timeout_ms)
{
  ULONG n;
  if (!GetQueuedCompletionStatusEx(pollfd, events, maxevents, &n, timeout_ms, FALSE))
    return -1;

  /* Update win_sock with number of bytes read.*/
  for (ULONG i= 0; i < n; i++)
  {
    auto ev= &events[i];
    auto c= (TP_connection_generic *) native_event_get_userdata(ev);
    /* null userdata zero means shutdown (see PostQueuedCompletionStatus() usage*/
    if (c)
    {
      c->win_sock.end_read(ev->dwNumberOfBytesTransferred, 0);
    }
  }

  return (int) n;
}

#endif


/* Dequeue element from a workqueue */

static TP_connection_generic *queue_get(thread_group_t *thread_group)
{
  DBUG_ENTER("queue_get");
  thread_group->queue_event_count++;
  TP_connection_generic *c;
  for (int i=0; i < NQUEUES;i++)
  {
    c= thread_group->queues[i].pop_front();
    if (c)
      DBUG_RETURN(c);
  }
  DBUG_RETURN(0);
}

static TP_connection_generic* queue_get(thread_group_t* group, operation_origin origin)
{
  auto ret = queue_get(group);
  if (ret)
  {
    TP_INCREMENT_GROUP_COUNTER(group, dequeues[(int)origin]);
  }
  return ret;
}

static bool is_queue_empty(thread_group_t *thread_group)
{
  for (int i=0; i < NQUEUES; i++)
  {
    if (!thread_group->queues[i].is_empty())
      return false;
  }
  return true;
}

static void queue_init(thread_group_t *thread_group)
{
  for (int i=0; i < NQUEUES; i++)
  {
    thread_group->queues[i].empty();
  }
}

static void queue_put(thread_group_t *thread_group, native_event *ev, int cnt)
{
  ulonglong now= threadpool_exact_stats?microsecond_interval_timer():pool_timer.current_microtime;
  for(int i=0; i < cnt; i++)
  {
    TP_connection_generic *c = (TP_connection_generic *)native_event_get_userdata(&ev[i]);
    c->enqueue_time= now;
    thread_group->queues[c->priority].push_back(c);
  }
}

#ifdef COROUTINE_ENABLED
static void lockfree_queue_put_bulk(thread_group_t *thread_group,
                                    native_event *ev, int cnt)
{
  thread_group->io_event_count.fetch_add(cnt, std::memory_order_relaxed);

  ulonglong now= threadpool_exact_stats ?
    microsecond_interval_timer():pool_timer.current_microtime;
  for(int i=0; i < cnt; i++)
  {
    TP_connection_generic *c = (TP_connection_generic *)native_event_get_userdata(&ev[i]);
    if (c == nullptr)
    {
      continue;
    }
    int prev= c->epoll_events_.fetch_add(1, std::memory_order_acq_rel);
    // If the prior epoll events is non-zero, it means there is an active
    // coroutine processing the connection.
    if (prev > 0)
    {
      continue;
    }
    c->enqueue_time= now;
    thread_group->coroutine_info_->req_queue_.Enqueue(c);
  }
}
#endif

/*
  Handle wait timeout :
  Find connections that have been idle for too long and kill them.
  Also, recalculate time when next timeout check should run.
*/

static my_bool timeout_check(THD *thd, pool_timer_t *timer)
{
  DBUG_ENTER("timeout_check");
  if (thd->net.reading_or_writing == 1)
  {
    TP_connection_generic *connection= (TP_connection_generic *)thd->event_scheduler.data;
    if (!connection || connection->state != TP_STATE_IDLE)
    {
      /*
        Connection does not have scheduler data. This happens for example
        if THD belongs to a different scheduler, that is listening to extra_port.
      */
      DBUG_RETURN(0);
    }

    if(connection->abs_wait_timeout < timer->current_microtime)
    {
      tp_timeout_handler(connection);
    }
    else
    {
      if (connection->abs_wait_timeout < timer->current_microtime)
        tp_timeout_handler(connection);
      else
        set_next_timeout_check(connection->abs_wait_timeout);
    }
  }
  DBUG_RETURN(0);
}


/*
 Timer thread.

  Periodically, check if one of the thread groups is stalled. Stalls happen if
  events are not being dequeued from the queue, or from the network, Primary
  reason for stall can be a lengthy executing non-blocking request. It could
  also happen that thread is waiting but wait_begin/wait_end is forgotten by
  storage engine. Timer thread will create a new thread in group in case of
  a stall.

  Besides checking for stalls, timer thread is also responsible for terminating
  clients that have been idle for longer than wait_timeout seconds.

  TODO: Let the timer sleep for long time if there is no work to be done.
  Currently it wakes up rather often on and idle server.
*/

static void* timer_thread(void *param)
{
  uint i;
  pool_timer_t* timer=(pool_timer_t *)param;

  my_thread_init();
  DBUG_ENTER("timer_thread");
  timer->next_timeout_check.store(std::numeric_limits<uint64_t>::max(),
                                  std::memory_order_relaxed);
  timer->current_microtime= microsecond_interval_timer();

  for(;;)
  {
    struct timespec ts;
    int err;

    set_timespec_nsec(ts,timer->tick_interval*1000000);
    mysql_mutex_lock(&timer->mutex);
    err= mysql_cond_timedwait(&timer->cond, &timer->mutex, &ts);
    if (timer->shutdown)
    {
      mysql_mutex_unlock(&timer->mutex);
      break;
    }
    if (err == ETIMEDOUT)
    {
      timer->current_microtime= microsecond_interval_timer();

      /* Check stalls in thread groups */
      for (i= 0; i < threadpool_max_size; i++)
      {
        if(all_groups[i].connection_count)
#ifdef COROUTINE_ENABLED
           lockfree_check_stall(&all_groups[i]);
#else
           check_stall(&all_groups[i]);
#endif
      }

      /* Check if any client exceeded wait_timeout */
      if (timer->next_timeout_check.load(std::memory_order_relaxed) <=
          timer->current_microtime)
      {
        /* Reset next timeout check, it will be recalculated below */
        timer->next_timeout_check.store(std::numeric_limits<uint64_t>::max(),
                                        std::memory_order_relaxed);
        server_threads.iterate(timeout_check, timer);
      }
    }
    mysql_mutex_unlock(&timer->mutex);
  }

  mysql_mutex_destroy(&timer->mutex);
  my_thread_end();
  return NULL;
}

#ifndef COROUTINE_ENABLED
void check_stall(thread_group_t *thread_group)
{
  mysql_mutex_lock(&thread_group->mutex);

  /*
   Bump priority for the low priority connections that spent too much
   time in low prio queue.
  */
  TP_connection_generic *c;
  for (;;)
  {
    c= thread_group->queues[TP_PRIORITY_LOW].front();
    if (c && pool_timer.current_microtime - c->enqueue_time > 1000ULL * threadpool_prio_kickup_timer)
    {
      thread_group->queues[TP_PRIORITY_LOW].remove(c);
      thread_group->queues[TP_PRIORITY_HIGH].push_back(c);
    }
    else
      break;
  }

  /*
    Check if listener is present. If not,  check whether any IO
    events were dequeued since last time. If not, this means
    listener is either in tight loop or thd_wait_begin()
    was forgotten. Create a new worker(it will make itself listener).
  */
  if (!thread_group->listener && !thread_group->io_event_count)
  {
    wake_or_create_thread(thread_group, true);
    mysql_mutex_unlock(&thread_group->mutex);
    return;
  }

  /*  Reset io event count */
  thread_group->io_event_count= 0;

  /*
    Check whether requests from the workqueue are being dequeued.

    The stall detection and resolution works as follows:

    1. There is a counter thread_group->queue_event_count for the number of
       events removed from the queue. Timer resets the counter to 0 on each run.
    2. Timer determines stall if this counter remains 0 since last check
       and the queue is not empty.
    3. Once timer determined a stall it sets thread_group->stalled flag and
       wakes and idle worker (or creates a new one, subject to throttling).
    4. The stalled flag is reset, when an event is dequeued.

    Q : Will this handling lead to an unbound growth of threads, if queue
    stalls permanently?
    A : No. If queue stalls permanently, it is an indication for many very long
    simultaneous queries. The maximum number of simultanoues queries is
    max_connections, further we have threadpool_max_threads limit, upon which no
    worker threads are created. So in case there is a flood of very long
    queries, threadpool would slowly approach thread-per-connection behavior.
    NOTE:
    If long queries never wait, creation of the new threads is done by timer,
    so it is slower than in real thread-per-connection. However if long queries
    do wait and indicate that via thd_wait_begin/end callbacks, thread creation
    will be faster.
  */
  if (!is_queue_empty(thread_group) && !thread_group->queue_event_count)
  {
    thread_group->stalled= true;
    TP_INCREMENT_GROUP_COUNTER(thread_group,stalls);
    wake_or_create_thread(thread_group,true);
  }

  /* Reset queue event count */
  thread_group->queue_event_count= 0;

  mysql_mutex_unlock(&thread_group->mutex);
}
#else
void lockfree_check_stall(thread_group_t *thread_group)
{
  /*  Reset io event count */
  thread_group->io_event_count.store(0, std::memory_order_relaxed);

  /*
    Check whether requests from the workqueue are being dequeued.

    The stall detection and resolution works as follows:

    1. There is a counter thread_group->queue_event_count for the number of
       events removed from the queue. Timer resets the counter to 0 on each run.
    2. Timer determines stall if this counter remains 0 since last check
       and the queue is not empty.
    3. Once timer determined a stall it sets thread_group->stalled flag and
       wakes and idle worker (or creates a new one, subject to throttling).
    4. The stalled flag is reset, when an event is dequeued.

    Q : Will this handling lead to an unbound growth of threads, if queue
    stalls permanently?
    A : No. If queue stalls permanently, it is an indication for many very long
    simultaneous queries. The maximum number of simultanoues queries is
    max_connections, further we have threadpool_max_threads limit, upon which no
    worker threads are created. So in case there is a flood of very long
    queries, threadpool would slowly approach thread-per-connection behavior.
    NOTE:
    If long queries never wait, creation of the new threads is done by timer,
    so it is slower than in real thread-per-connection. However if long queries
    do wait and indicate that via thd_wait_begin/end callbacks, thread creation
    will be faster.
  */
  if (!thread_group->coroutine_info_->req_queue_.IsEmpty() &&
      thread_group->queue_event_count.load(std::memory_order_relaxed) == 0)
  {
    thread_group->stalled.store(true, std::memory_order_relaxed);

    mysql_mutex_lock(&thread_group->mutex);
    TP_INCREMENT_GROUP_COUNTER(thread_group,stalls);
    wake_or_create_thread(thread_group,true);
    mysql_mutex_unlock(&thread_group->mutex);
  }

  /* Reset queue event count */
  thread_group->queue_event_count.store(0, std::memory_order_relaxed);
}
#endif

static void start_timer(pool_timer_t* timer)
{
  DBUG_ENTER("start_timer");
  mysql_mutex_init(key_timer_mutex,&timer->mutex, NULL);
  mysql_cond_init(key_timer_cond, &timer->cond, NULL);
  timer->shutdown = false;
  mysql_thread_create(key_timer_thread, &timer->timer_thread_id, NULL,
                      timer_thread, timer);
  DBUG_VOID_RETURN;
}


static void stop_timer(pool_timer_t *timer)
{
  DBUG_ENTER("stop_timer");
  mysql_mutex_lock(&timer->mutex);
  timer->shutdown = true;
  mysql_cond_signal(&timer->cond);
  mysql_mutex_unlock(&timer->mutex);
  pthread_join(timer->timer_thread_id, NULL);
  DBUG_VOID_RETURN;
}

/**
  Poll for socket events and distribute them to worker threads
  In many case current thread will handle single event itself.

  @return a ready connection, or NULL on shutdown
*/
static TP_connection_generic * listener(worker_thread_t *current_thread,
                               thread_group_t *thread_group)
{
  DBUG_ENTER("listener");
  TP_connection_generic *retval= NULL;

  for(;;)
  {
    native_event ev[MAX_EVENTS];
    int cnt;

    if (thread_group->shutdown)
      break;

    cnt = io_poll_wait(thread_group->pollfd, ev, MAX_EVENTS, -1);
    TP_INCREMENT_GROUP_COUNTER(thread_group, polls[(int)operation_origin::LISTENER]);
    if (cnt <=0)
    {
      DBUG_ASSERT(thread_group->shutdown);
      break;
    }

    mysql_mutex_lock(&thread_group->mutex);

    if (thread_group->shutdown)
    {
      mysql_mutex_unlock(&thread_group->mutex);
      break;
    }

    thread_group->io_event_count += cnt;

    /*
     We got some network events and need to make decisions : whether
     listener  hould handle events and whether or not any wake worker
     threads so they can handle events.

     Q1 : Should listener handle an event itself, or put all events into
     queue  and let workers handle the events?

     Solution :
     Generally, listener that handles events itself is preferable. We do not
     want listener thread to change its state from waiting  to running too
     often, Since listener has just woken from poll, it better uses its time
     slice and does some work. Besides, not handling events means they go to
     the  queue, and often to wake another worker must wake up to handle the
     event. This is not good, as we want to avoid wakeups.

     The downside of listener that also handles queries is that we can
     potentially leave thread group  for long time not picking the new
     network events. It is not  a major problem, because this stall will be
     detected  sooner or later by  the timer thread. Still, relying on timer
     is not always good, because it may "tick" too slow (large timer_interval)

     We use following strategy to solve this problem - if queue was not empty
     we suspect flood of network events and listener stays, Otherwise, it
     handles a query.

     Q2: If queue is not empty, how many workers to wake?

     Solution:
     We generally try to keep one thread per group active (threads handling
     queries are considered active, unless they stuck in inside some "wait")
     Thus, we will wake only one worker, and only if there is not active
     threads currently,and listener is not going to handle a query. When we
     don't wake, we hope that  currently active  threads will finish fast and
     handle the queue. If this does  not happen, timer thread will detect stall
     and wake a worker.

     NOTE: Currently nothing is done to detect or prevent long queuing times.
     A solution for the future would be to give up "one active thread per
     group" principle, if events stay  in the queue for too long, and just wake
     more workers.
    */

    bool listener_picks_event=is_queue_empty(thread_group) && !threadpool_dedicated_listener;
    queue_put(thread_group, ev, cnt);
    if (listener_picks_event)
    {
      /* Handle the first event. */
      retval= queue_get(thread_group, operation_origin::LISTENER);
      mysql_mutex_unlock(&thread_group->mutex);
      break;
    }

    if(thread_group->active_thread_count==0)
    {
      /* We added some work items to queue, now wake a worker. */
      if(wake_thread(thread_group, false))
      {
        /*
          Wake failed, hence groups has no idle threads. Now check if there are
          any threads in the group except listener.
        */
        if(thread_group->thread_count == 1)
        {
           /*
             Currently there is no worker thread in the group, as indicated by
             thread_count == 1 (this means listener is the only one thread in
             the group).
             The queue is not empty, and listener is not going to handle
             events. In order to drain the queue,  we create a worker here.
             Alternatively, we could just rely on timer to detect stall, and
             create thread, but waiting for timer would be an inefficient and
             pointless delay.
           */
           create_worker(thread_group, false);
        }
      }
    }
    mysql_mutex_unlock(&thread_group->mutex);
  }

  DBUG_RETURN(retval);
}

#ifdef COROUTINE_ENABLED
/**
  Poll for socket events and distribute them to worker threads
  In many case current thread will handle single event itself.

  @return a ready connection, or NULL on shutdown
*/
static void lockfree_listener(worker_thread_t *current_thread,
                              thread_group_t *thread_group)
{
  DBUG_ENTER("listener");

  for(;;)
  {
    native_event ev[MAX_EVENTS];
    int cnt;

    while (!thread_group->shutdown && thread_group->HasActiveWorker())
    {
      std::unique_lock<std::mutex> lk(thread_group->listener_mux_);
      thread_group->listener_cv_.wait_for(lk, std::chrono::seconds(5));
    }

    if (thread_group->shutdown)
      break;

    cnt = io_poll_wait(thread_group->pollfd, ev, MAX_EVENTS, -1);
    TP_INCREMENT_GROUP_COUNTER(thread_group, polls[(int)operation_origin::LISTENER]);
    if (cnt <=0)
    {
      DBUG_ASSERT(thread_group->shutdown);
      break;
    }

    if (thread_group->shutdown.load(std::memory_order_relaxed))
    {
      break;
    }

    lockfree_queue_put_bulk(thread_group, ev, cnt);

    if (!thread_group->HasActiveWorker())
    {
#ifdef ELOQ_MODULE_ENABLED
      // When SQL runtime is registered as a module, wakes up the external worker.
      eloq::EloqModule::NotifyWorker(thread_group->coroutine_info_->group_id_);
#else
      mysql_mutex_lock(&thread_group->mutex);

      // Re-checks active threads after acquiring the mutex.
      int active_thd_cnt =
        thread_group->active_thread_count.load(std::memory_order_relaxed);

      /* We added some work items to queue, now wake a worker. */
      if(active_thd_cnt == 0 && wake_thread(thread_group, false))
      {
        /*
          Wake failed, hence groups has no idle threads. Now check if there are
          any threads in the group except listener.
        */
        if(thread_group->thread_count == 1)
        {
           /*
             Currently there is no worker thread in the group, as indicated by
             thread_count == 1 (this means listener is the only one thread in
             the group).
             The queue is not empty, and listener is not going to handle
             events. In order to drain the queue,  we create a worker here.
             Alternatively, we could just rely on timer to detect stall, and
             create thread, but waiting for timer would be an inefficient and
             pointless delay.
           */
           create_worker(thread_group, false);
        }
      }
      mysql_mutex_unlock(&thread_group->mutex);
#endif
    }
  }

  DBUG_VOID_RETURN;
}
#endif

/**
  Adjust thread counters in group or global
  whenever thread is created or is about to exit

  @param thread_group
  @param count -  1, when new thread is created
                 -1, when thread is about to exit
*/

static void add_thread_count(thread_group_t *thread_group, int32 count)
{
  thread_group->thread_count += count;
#ifndef COROUTINE_ENABLED
  /* worker starts out and end in "active" state */
  thread_group->active_thread_count += count;
#endif
  tp_stats.num_worker_threads+= count;
}


/**
  Creates a new worker thread.
  thread_mutex must be held when calling this function

  NOTE: in rare cases, the number of threads can exceed
  threadpool_max_threads, because we need at least 2 threads
  per group to prevent deadlocks (one listener + one worker)
*/

static int create_worker(thread_group_t *thread_group, bool due_to_stall,
                         bool force_create)
{
  pthread_t thread_id;
  bool max_threads_reached= false;
  int err;

  DBUG_ENTER("create_worker");
  if (!force_create && tp_stats.num_worker_threads >= threadpool_max_threads &&
      thread_group->thread_count >= 2)
  {
    err= 1;
    max_threads_reached= true;
    goto end;
  }

  err= mysql_thread_create(key_worker_thread, &thread_id,
         thread_group->pthread_attr, worker_main, thread_group);
  if (!err)
  {
    thread_group->last_thread_creation_time=microsecond_interval_timer();
    statistic_increment(thread_created,&LOCK_status);
    add_thread_count(thread_group, 1);
    TP_INCREMENT_GROUP_COUNTER(thread_group,thread_creations);
    if(due_to_stall)
    {
      TP_INCREMENT_GROUP_COUNTER(thread_group, thread_creations_due_to_stall);
    }
  }
  else
  {
    my_errno= errno;
  }

end:
  if (err)
    print_pool_blocked_message(max_threads_reached);
  else
    pool_block_start= 0; /* Reset pool blocked timer, if it was set */

  DBUG_RETURN(err);
}


/**
 Calculate microseconds throttling delay for thread creation.

 The value depends on how many threads are already in the group:
 small number of threads means no delay, the more threads the larger
 the delay.

 The actual values were not calculated using any scientific methods.
 They just look right, and behave well in practice.
*/

#define THROTTLING_FACTOR (threadpool_stall_limit/std::max(DEFAULT_THREADPOOL_STALL_LIMIT,threadpool_stall_limit))

static ulonglong microsecond_throttling_interval(thread_group_t *thread_group)
{
  int count= thread_group->thread_count;

  if (count < 1+ (int)threadpool_oversubscribe)
    return 0;

  if (count < 8)
    return 50*1000*THROTTLING_FACTOR;

  if(count < 16)
    return 100*1000*THROTTLING_FACTOR;

  return 200*100*THROTTLING_FACTOR;
}


/**
  Wakes a worker thread, or creates a new one.

  Worker creation is throttled, so we avoid too many threads
  to be created during the short time.
*/
static int wake_or_create_thread(thread_group_t *thread_group,
                                 bool due_to_stall, bool force_create)
{
  DBUG_ENTER("wake_or_create_thread");

  if (thread_group->shutdown)
   DBUG_RETURN(0);

  if (wake_thread(thread_group, due_to_stall) == 0)
  {
    DBUG_RETURN(0);
  }

  if (!force_create &&
      thread_group->thread_count > thread_group->connection_count)
    DBUG_RETURN(-1);


  if (thread_group->active_thread_count == 0)
  {
    /*
     We're better off creating a new thread here  with no delay, either there
     are no workers at all, or they all are all blocking and there was no
     idle  thread to wakeup. Smells like a potential deadlock or very slowly
     executing requests, e.g sleeps or user locks.
    */
    DBUG_RETURN(create_worker(thread_group, due_to_stall, force_create));
  }

  ulonglong now = microsecond_interval_timer();
  ulonglong time_since_last_thread_created =
    (now - thread_group->last_thread_creation_time);

  /* Throttle thread creation. */
  if (force_create || time_since_last_thread_created >
                          microsecond_throttling_interval(thread_group))
  {
    DBUG_RETURN(create_worker(thread_group, due_to_stall, force_create));
  }

  TP_INCREMENT_GROUP_COUNTER(thread_group,throttles);
  DBUG_RETURN(-1);
}



int thread_group_init(thread_group_t *thread_group, pthread_attr_t* thread_attr)
{
  DBUG_ENTER("thread_group_init");
  thread_group->pthread_attr = thread_attr;
  mysql_mutex_init(key_group_mutex, &thread_group->mutex, NULL);
  thread_group->pollfd= INVALID_HANDLE_VALUE;
  thread_group->shutdown_pipe[0]= -1;
  thread_group->shutdown_pipe[1]= -1;
  queue_init(thread_group);
#ifdef COROUTINE_ENABLED
  thread_group->coroutine_info_ = std::make_unique<CoroutineInfo>();
#endif
  DBUG_RETURN(0);
}


void thread_group_destroy(thread_group_t *thread_group)
{
  mysql_mutex_destroy(&thread_group->mutex);
  if (thread_group->pollfd != INVALID_HANDLE_VALUE)
  {
    io_poll_close(thread_group->pollfd);
    thread_group->pollfd= INVALID_HANDLE_VALUE;
  }
#ifdef COROUTINE_ENABLED
  thread_group->coroutine_info_ = nullptr;
#endif
#ifndef _WIN32
  for(int i=0; i < 2; i++)
  {
    if(thread_group->shutdown_pipe[i] != -1)
    {
      close(thread_group->shutdown_pipe[i]);
      thread_group->shutdown_pipe[i]= -1;
    }
  }
#endif

  if (!--shutdown_group_count)
  {
    my_free(all_groups);
    all_groups= 0;
  }
}

/**
  Wake sleeping thread from waiting list
*/

static int wake_thread(thread_group_t *thread_group,bool due_to_stall)
{
  DBUG_ENTER("wake_thread");
  worker_thread_t *thread = thread_group->waiting_threads.front();
  if(thread)
  {
    thread->woken= true;
    thread_group->waiting_threads.remove(thread);
    mysql_cond_signal(&thread->cond);
    TP_INCREMENT_GROUP_COUNTER(thread_group, wakes);
    if (due_to_stall)
    {
      TP_INCREMENT_GROUP_COUNTER(thread_group, wakes_due_to_stall);
    }
    DBUG_RETURN(0);
  }
  DBUG_RETURN(1); /* no thread in waiter list => missed wakeup */
}

/*
   Wake listener thread (during shutdown)
   Self-pipe trick is used in most cases,except IOCP.
*/
static int wake_listener(thread_group_t *thread_group)
{
#ifndef _WIN32
  if (pipe(thread_group->shutdown_pipe))
  {
    return -1;
  }

  /* Wake listener */
#ifdef COROUTINE_ENABLED
  {
    std::unique_lock<std::mutex> lk(thread_group->listener_mux_);
    thread_group->listener_cv_.notify_one();
  }
#endif

  if (io_poll_associate_fd(thread_group->pollfd,
    thread_group->shutdown_pipe[0], NULL, NULL))
  {
    return -1;
  }
  char c= 0;
  if (write(thread_group->shutdown_pipe[1], &c, 1) < 0)
    return -1;
#else
  PostQueuedCompletionStatus(thread_group->pollfd, 0, 0, 0);
#endif
  return 0;
}
/**
  Initiate shutdown for thread group.

  The shutdown is asynchronous, we only care to  wake all threads in here, so
  they can finish. We do not wait here until threads terminate. Final cleanup
  of the group (thread_group_destroy) will be done by the last exiting threads.
*/

static void thread_group_close(thread_group_t *thread_group)
{
  DBUG_ENTER("thread_group_close");

  mysql_mutex_lock(&thread_group->mutex);
  if (thread_group->thread_count == 0)
  {
    mysql_mutex_unlock(&thread_group->mutex);
    thread_group_destroy(thread_group);
    DBUG_VOID_RETURN;
  }

  thread_group->shutdown= true;
  thread_group->listener= NULL;

  wake_listener(thread_group);

  /* Wake all workers. */
  while(wake_thread(thread_group, false) == 0)
  {
  }

  mysql_mutex_unlock(&thread_group->mutex);

  DBUG_VOID_RETURN;
}

#ifndef COROUTINE_ENABLED
/*
  Add work to the queue. Maybe wake a worker if they all sleep.

  Currently, this function is only used when new connections need to
  perform login (this is done in worker threads).

*/
static void queue_put(thread_group_t *thread_group, TP_connection_generic *connection)
{
  DBUG_ENTER("queue_put");

  connection->enqueue_time= threadpool_exact_stats?microsecond_interval_timer():pool_timer.current_microtime;
  thread_group->queues[connection->priority].push_back(connection);

  if (thread_group->active_thread_count == 0)
    wake_or_create_thread(thread_group);

  DBUG_VOID_RETURN;
}
#endif

#ifdef COROUTINE_ENABLED
static void lockfree_queue_put(thread_group_t *thread_group, TP_connection_generic *connection)
{
  DBUG_ENTER("queue_put");

  connection->epoll_events_.store(1, std::memory_order_relaxed);
  connection->events_snapshot_ = 1;
  connection->enqueue_time= threadpool_exact_stats?microsecond_interval_timer():pool_timer.current_microtime;
  thread_group->coroutine_info_->req_queue_.Enqueue(connection);

  if (!thread_group->HasActiveWorker())
  {
#ifdef ELOQ_MODULE_ENABLED
    eloq::EloqModule::NotifyWorker(thread_group->coroutine_info_->group_id_);
#else
    mysql_mutex_lock(&thread_group->mutex);
    if (thread_group->active_thread_count.load(std::memory_order_relaxed) == 0)
    {
      wake_or_create_thread(thread_group);
    }
    mysql_mutex_unlock(&thread_group->mutex);
#endif
  }

  DBUG_VOID_RETURN;
}
#endif

/*
  Prevent too many threads executing at the same time,if the workload is
  not CPU bound.
*/

static bool too_many_threads(thread_group_t *thread_group)
{
#ifdef COROUTINE_ENABLED
  return thread_group->active_thread_count.load(std::memory_order_relaxed)
         >= 1+(int)threadpool_oversubscribe &&
         !thread_group->stalled.load(std::memory_order_relaxed);
#else
  return (thread_group->active_thread_count >= 1+(int)threadpool_oversubscribe
   && !thread_group->stalled);
#endif
}


/**
  Retrieve a connection with pending event.

  Pending event in our case means that there is either a pending login request
  (if connection is not yet logged in), or there are unread bytes on the socket.

  If there are no pending events currently, thread will wait.
  If timeout specified in abstime parameter passes, the function returns NULL.

  @param current_thread - current worker thread
  @param thread_group - current thread group
  @param abstime - absolute wait timeout

  @return
  connection with pending event.
  NULL is returned if timeout has expired,or on shutdown.
*/

TP_connection_generic *get_event(worker_thread_t *current_thread,
  thread_group_t *thread_group,  struct timespec *abstime)
{
  DBUG_ENTER("get_event");
  TP_connection_generic *connection = NULL;


  mysql_mutex_lock(&thread_group->mutex);
  DBUG_ASSERT(thread_group->active_thread_count >= 0);

  for(;;)
  {
    int err=0;
    bool oversubscribed = too_many_threads(thread_group);
    if (thread_group->shutdown)
     break;

    /* Check if queue is not empty */
    if (!oversubscribed)
    {
      connection = queue_get(thread_group, operation_origin::WORKER);
      if(connection)
      {
        break;
      }
    }

    /* If there is  currently no listener in the group, become one. */
    if(!thread_group->listener)
    {
      thread_group->listener= current_thread;
      thread_group->active_thread_count--;
      mysql_mutex_unlock(&thread_group->mutex);

      connection = listener(current_thread, thread_group);

      mysql_mutex_lock(&thread_group->mutex);
      thread_group->active_thread_count++;
      /* There is no listener anymore, it just returned. */
      thread_group->listener= NULL;
      break;
    }


    /*
      Last thing we try before going to sleep is to
      non-blocking event poll, i.e with timeout = 0.
      If this returns events, pick one
    */
    if (!oversubscribed && !threadpool_dedicated_listener)
    {
      native_event ev[MAX_EVENTS];
      int cnt = io_poll_wait(thread_group->pollfd, ev, MAX_EVENTS, 0);
      TP_INCREMENT_GROUP_COUNTER(thread_group, polls[(int)operation_origin::WORKER]);
      if (cnt > 0)
      {
        queue_put(thread_group, ev, cnt);
        connection= queue_get(thread_group,operation_origin::WORKER);
        break;
      }
    }


    /* And now, finally sleep */
    current_thread->woken = false; /* wake() sets this to true */

    /*
      Add current thread to the head of the waiting list  and wait.
      It is important to add thread to the head rather than tail
      as it ensures LIFO wakeup order (hot caches, working inactivity timeout)
    */
    thread_group->waiting_threads.push_front(current_thread);

    thread_group->active_thread_count--;
    if (abstime)
    {
      err = mysql_cond_timedwait(&current_thread->cond, &thread_group->mutex,
                                 abstime);
    }
    else
    {
      err = mysql_cond_wait(&current_thread->cond, &thread_group->mutex);
    }
    thread_group->active_thread_count++;

    if (!current_thread->woken)
    {
      /*
        Thread was not signalled by wake(), it might be a spurious wakeup or
        a timeout. Anyhow, we need to remove ourselves from the list now.
        If thread was explicitly woken, than caller removed us from the list.
      */
      thread_group->waiting_threads.remove(current_thread);
    }

    if (err)
      break;
  }

  thread_group->stalled= false;

  mysql_mutex_unlock(&thread_group->mutex);

  DBUG_RETURN(connection);
}

#ifdef COROUTINE_ENABLED
thread_local std::array<TP_connection_generic*, 128> local_conns;
thread_local uint8_t conns_cnt{0};
thread_local uint8_t conns_offset{0};
#if defined(IOURING_ENABLED)
thread_local IoUringWrapper iouring_wrap{};
#endif

void get_event_bulk(worker_thread_t *current_thread,
                    thread_group_t *thread_group,
                    struct timespec *abstime)
{
  DBUG_ENTER("get_event");

  mysql_mutex_lock(&thread_group->mutex);
  DBUG_ASSERT(thread_group->active_thread_count >= 0);

  CoroutineInfo *coro_info = thread_group->coroutine_info_.get();

  for(;;)
  {
    int err=0;
    bool oversubscribed = too_many_threads(thread_group);
    if (thread_group->shutdown)
     break;

    /* Check if queue is not empty */
    if (!oversubscribed)
    {
      thread_group->queue_event_count.fetch_add(1, std::memory_order_relaxed);

#ifdef ELOQ_MODULE_ENABLED
      if (!coro_info->sql_native_queue_.IsEmpty())
      {
        conns_cnt= coro_info->sql_native_queue_.TryDequeueBulk(
            local_conns.begin(), local_conns.size());
        coro_info->empty_run_cnt_= 0;
        break;
      }
#endif

      conns_cnt = coro_info->resume_queue_.TryDequeueBulk(
          local_conns.begin(), local_conns.size());
      if (conns_cnt == 0)
      {
        conns_cnt = coro_info->req_queue_.TryDequeueBulk(
          local_conns.begin(), local_conns.size());

        // TP_connection_generic *tp_c = nullptr;
        // bool success = coro_info->req_queue_.try_dequeue(tp_c);
        // if (success)
        // {
        //   conn_array[conn_cnt] = tp_c;
        //   ++conn_cnt;
        // }
      }

      if(conns_cnt > 0)
      {
        coro_info->empty_run_cnt_ = 0;
        break;
      }
#ifndef ELOQ_MODULE_ENABLED
      else if (thread_group->active_thread_count.load(
                   std::memory_order_relaxed) == 1 &&
               thread_group->listener != nullptr)
      {
        // If this is the last active thread besides the listener and there are
        // ongoing coroutines, busy waits for the coroutines to resume.
        if (coro_info->coro_cnt_.load(std::memory_order_relaxed) > 0)
        {
          coro_info->empty_run_cnt_= 0;
          // Inserts a pseudo connection.
          local_conns[0]= nullptr;
          conns_cnt= 1;
          break;
        }

        // If there is no coroutine or incoming request, busy loops for a while
        // before entering the sleep mode.
        if (coro_info->empty_run_cnt_ == 0)
        {
          coro_info->empty_begin_tp_= std::chrono::system_clock::now();
        }

        coro_info->empty_run_cnt_++;
        using namespace std::chrono_literals;
        auto dur_in_milli_sec= 0ms;
        if (coro_info->empty_run_cnt_ % 100000 == 0)
        {
          auto now= std::chrono::system_clock::now();
          auto dur= now.time_since_epoch() -
                    coro_info->empty_begin_tp_.time_since_epoch();
          dur_in_milli_sec=
              std::chrono::duration_cast<std::chrono::milliseconds>(dur);
        }

        if (dur_in_milli_sec < 100ms)
        {
          // Inserts a pseudo connection.
          local_conns[0]= nullptr;
          conns_cnt= 1;
          break;
        }
      }
#endif
    }

    /* If there is currently no listener in the group, become one. */
    if(!thread_group->listener)
    {
      thread_group->listener= current_thread;
      thread_group->active_thread_count.fetch_sub(1,
                                                  std::memory_order_relaxed);
#if defined (EXT_TX_PROC_ENABLED) && !defined (ELOQ_MODULE_ENABLED)
      (coro_info->update_ext_proc_)(-1);
#endif
      mysql_mutex_unlock(&thread_group->mutex);
      lockfree_listener(current_thread, thread_group);
      mysql_mutex_lock(&thread_group->mutex);
      // The listener thread is about to exit. Does not increase the active
      // thread count.

#if defined (EXT_TX_PROC_ENABLED) && !defined (ELOQ_MODULE_ENABLED)
      (coro_info->update_ext_proc_)(1);
#endif
      /* There is no listener anymore, it just returned. */
      thread_group->listener= NULL;
      break;
    }

    int32_t prev_active_cnt= thread_group->active_thread_count.fetch_sub(
        1, std::memory_order_acq_rel);

    // This is the last running thread who is going to sleep.
    if (prev_active_cnt == 1)
    {
#if ELOQ_MODULE_ENABLED
      // This is the last SQL thread to sleep. Ensures the external worker
      // thread is active.
      eloq::EloqModule::NotifyWorker(coro_info->group_id_);
#else
      // Re-checks the resume and request queues before sleeping. Do not sleep
      // if there are requests to process.
      if (!coro_info->IsEmpty())
      {
        thread_group->active_thread_count.fetch_add(1,
                                                    std::memory_order_relaxed);
        continue;
      }

      // This is the last thread going to sleep. Wakes up the listener thread.
      std::unique_lock<std::mutex> lk(thread_group->listener_mux_);
      thread_group->listener_cv_.notify_one();
#endif
    }

#if defined (EXT_TX_PROC_ENABLED) && !defined (ELOQ_MODULE_ENABLED)
    (coro_info->update_ext_proc_)(-1);
#endif

    /* And now, finally sleep */
    current_thread->woken = false; /* wake() sets this to true */

    /*
      Add current thread to the head of the waiting list  and wait.
      It is important to add thread to the head rather than tail
      as it ensures LIFO wakeup order (hot caches, working inactivity timeout)
    */
    thread_group->waiting_threads.push_front(current_thread);

    if (abstime)
    {
      err = mysql_cond_timedwait(&current_thread->cond, &thread_group->mutex,
                                 abstime);
    }
    else
    {
      err = mysql_cond_wait(&current_thread->cond, &thread_group->mutex);
    }
    prev_active_cnt= thread_group->active_thread_count.fetch_add(
        1, std::memory_order_relaxed);

#if defined (EXT_TX_PROC_ENABLED) && !defined (ELOQ_MODULE_ENABLED)
    (coro_info->update_ext_proc_)(1);
#endif

    if (!current_thread->woken)
    {
      /*
        Thread was not signalled by wake(), it might be a spurious wakeup or
        a timeout. Anyhow, we need to remove ourselves from the list now.
        If thread was explicitly woken, than caller removed us from the list.
      */
      thread_group->waiting_threads.remove(current_thread);
    }

    if (err)
    {
      // Prepares to exit, if (1) this is not the last thread timing out
      // (because we expect there is only SQL thread unless there are stalls),
      // or (2) this is the last thread and there is no request to process, or
      // (3) there is an active external worker thread.
      if (thread_group->ext_worker_active_.load(std::memory_order_relaxed))
      {
        break;
      }
      else if (prev_active_cnt > 0 || coro_info->IsEmpty())
      {
        // Ready to exit. Decreases the active thread count.
        prev_active_cnt= thread_group->active_thread_count.fetch_sub(
            1, std::memory_order_relaxed);

        // Re-checks if there are requests to process.
        if (prev_active_cnt == 1 && !coro_info->IsEmpty())
        {
          thread_group->active_thread_count.fetch_add(
              1, std::memory_order_relaxed);
        }
        else
        {
          if (prev_active_cnt == 1)
          {
            // This is the last thread going to exit. Wakes up the listener
            // thread.
            std::unique_lock<std::mutex> lk(thread_group->listener_mux_);
            thread_group->listener_cv_.notify_one();
          }
          break;
        }
      }
      // else: this is the last thread timing out, but there are requests. Does
      // not exit and continues the loop.
    }
  }

  thread_group->stalled.store(false, std::memory_order_relaxed);

  // When the get_event_bulk() returns no requests to process, 
  // the thread is about to exit. Excludes the thread from the working set.
  if (conns_cnt == 0)
  {
#if defined (EXT_TX_PROC_ENABLED) && !defined (ELOQ_MODULE_ENABLED)
    (coro_info->update_ext_proc_)(-1);
#endif
  }

  mysql_mutex_unlock(&thread_group->mutex);

  DBUG_VOID_RETURN;
}
#endif

/**
  Tells the pool that worker starts waiting  on IO, lock, condition,
  sleep() or similar.
*/

void wait_begin(thread_group_t *thread_group, TP_connection_generic *conn)
{
  DBUG_ENTER("wait_begin");
  mysql_mutex_lock(&thread_group->mutex);
  if (is_sql_thd)
  {
    conn->wait_from_sql_thd_= true;
  #ifdef COROUTINE_ENABLED
    thread_group->active_thread_count.fetch_sub(1, std::memory_order_relaxed);
  #else
    thread_group->active_thread_count--;
  #endif
  }
  else
  {
    conn->wait_from_sql_thd_= false;
  }
#ifdef COROUTINE_ENABLED
  CoroutineInfo *coro_info = thread_group->coroutine_info_.get();

  // Before entering the wait mode, re-enqueues to-be-processed commands
  // into the group to allow other threads in the group to process.
  if (conns_offset + 1 < conns_cnt)
  {
    THD *first_thd = local_conns[conns_offset + 1]->thd;
    // The local cached commands are either resumed coroutines or new commands.
    // Get the status from the first cached command.
    bool is_ongoing_coro = first_thd == nullptr ||
                           first_thd->coro_status_ == THD::CoroStatus::Ongoing;
    uint8_t remaining = conns_cnt - conns_offset - 1;

    if (is_ongoing_coro)
    {
      coro_info->resume_queue_.EnqueueBulk(
        local_conns.begin() + conns_offset + 1, remaining);
    }
    else
    {
      coro_info->req_queue_.EnqueueBulk(
        local_conns.begin() + conns_offset + 1, remaining);
    }

    conns_cnt = conns_offset + 1;
  }
#endif

  DBUG_ASSERT(thread_group->active_thread_count >=0);
  DBUG_ASSERT(thread_group->connection_count > 0);

#ifdef COROUTINE_ENABLED
  if ((thread_group->active_thread_count.load(std::memory_order_relaxed) == 0) &&
      (!is_sql_thd || !coro_info->IsEmpty() || !thread_group->listener || 
      coro_info->coro_cnt_.load(std::memory_order_relaxed) > 0))
#else
  if ((thread_group->active_thread_count == 0) &&
      (!is_queue_empty(thread_group) || !thread_group->listener))
#endif
  {
    /*
      Group might stall while this thread waits, thus wake
      or create a worker to prevent stall.
    */
    wake_or_create_thread(thread_group, false, true);
  }

#if defined (EXT_TX_PROC_ENABLED) && !defined (ELOQ_MODULE_ENABLED)
  (coro_info->update_ext_proc_)(-1);
#endif

#if defined(COROUTINE_ENABLED) & defined (ELOQ_MODULE_ENABLED)
  if (!is_sql_thd)
  {
    // A SQL command entering the waiting section can only be processed by SQL
    // threads. If this is the external worker thread, enqueus the coroutine
    // into the SQL queue which can only be processed by SQL threads.
    coro_info->sql_native_queue_.Enqueue(conn);
    mysql_mutex_unlock(&thread_group->mutex);
    conn->thd->yield_func_();
    // When the coroutine resumes, it can only be SQL threads.
    assert(is_sql_thd);
    DBUG_VOID_RETURN;
  }
#endif
  mysql_mutex_unlock(&thread_group->mutex);
  DBUG_VOID_RETURN;
}

/**
  Tells the pool has finished waiting.
*/

void wait_end(thread_group_t *thread_group, TP_connection_generic *conn)
{
  DBUG_ENTER("wait_end");
  mysql_mutex_lock(&thread_group->mutex);
  assert(is_sql_thd);
  if (conn->wait_from_sql_thd_)
  {
#ifdef COROUTINE_ENABLED
    thread_group->active_thread_count.fetch_add(1, std::memory_order_relaxed);
#else
    thread_group->active_thread_count++;
#endif
  }
  conn->wait_from_sql_thd_ = true;
#ifdef COROUTINE_ENABLED
#if defined (EXT_TX_PROC_ENABLED) && !defined (ELOQ_MODULE_ENABLED)
  CoroutineInfo *coro_info = thread_group->coroutine_info_.get();
  (coro_info->update_ext_proc_)(1);
#endif
#endif
  mysql_mutex_unlock(&thread_group->mutex);
  DBUG_VOID_RETURN;
}


TP_connection * TP_pool_generic::new_connection(CONNECT *c)
{
  return new (std::nothrow) TP_connection_generic(c);
}

/**
  Add a new connection to thread pool
*/

void TP_pool_generic::add(TP_connection *c)
{
  DBUG_ENTER("tp_add_connection");

  TP_connection_generic *connection=(TP_connection_generic *)c;
  thread_group_t *thread_group= connection->thread_group;
  /*
    Add connection to the work queue.Actual logon
    will be done by a worker thread.
  */
 #ifdef COROUTINE_ENABLED
  lockfree_queue_put(thread_group, connection);
 #else
  mysql_mutex_lock(&thread_group->mutex);
  queue_put(thread_group, connection);
  mysql_mutex_unlock(&thread_group->mutex);
 #endif

  DBUG_VOID_RETURN;
}

void TP_pool_generic::resume(TP_connection* c)
{
  add(c);
}

/**
  MySQL scheduler callback: wait begin
*/

void TP_connection_generic::wait_begin(int type)
{
  DBUG_ENTER("wait_begin");

  DBUG_ASSERT(!waiting);
  waiting++;
  if (waiting == 1)
    ::wait_begin(thread_group, this);
  DBUG_VOID_RETURN;
}

/**
  MySQL scheduler callback: wait end
*/

void TP_connection_generic::wait_end()
{
  DBUG_ENTER("wait_end");
  DBUG_ASSERT(waiting);
  waiting--;
  if (waiting == 0)
    ::wait_end(thread_group, this);
  DBUG_VOID_RETURN;
}


static void set_next_timeout_check(ulonglong abstime)
{
  auto old= pool_timer.next_timeout_check.load(std::memory_order_relaxed);
  DBUG_ENTER("set_next_timeout_check");
  while (abstime < old)
  {
    if (pool_timer.next_timeout_check.
                   compare_exchange_weak(old, abstime,
                                         std::memory_order_relaxed,
                                         std::memory_order_relaxed))
     break;
  }
  DBUG_VOID_RETURN;
}

static size_t get_group_id(my_thread_id tid)
{
  return size_t(tid % group_count);
}


TP_connection_generic::TP_connection_generic(CONNECT *c):
  TP_connection(c),
  thread_group(0),
  next_in_queue(0),
  prev_in_queue(0),
  abs_wait_timeout(ULONGLONG_MAX),
  bound_to_poll_descriptor(false),
  waiting(false),
  fix_group(false)
{
  DBUG_ASSERT(c->vio_type != VIO_CLOSED);

#ifdef _WIN32
  fd= (c->vio_type == VIO_TYPE_NAMEDPIPE) ?
    c->pipe: (TP_file_handle) mysql_socket_getfd(c->sock);
#else
  fd= mysql_socket_getfd(c->sock);
#endif

  /* Assign connection to a group. */
  thread_group_t *group=
    &all_groups[get_group_id(c->thread_id)];
  thread_group=group;

  mysql_mutex_lock(&group->mutex);
  group->connection_count++;
  mysql_mutex_unlock(&group->mutex);
}

TP_connection_generic::~TP_connection_generic()
{
  mysql_mutex_lock(&thread_group->mutex);
  thread_group->connection_count--;
  mysql_mutex_unlock(&thread_group->mutex);
}

/**
  Set wait timeout for connection.
*/

void TP_connection_generic::set_io_timeout(int timeout_sec)
{
  DBUG_ENTER("set_wait_timeout");
  /*
    Calculate wait deadline for this connection.
    Instead of using microsecond_interval_timer() which has a syscall
    overhead, use pool_timer.current_microtime and take
    into account that its value could be off by at most
    one tick interval.
  */

  abs_wait_timeout= pool_timer.current_microtime +
    1000LL*pool_timer.tick_interval +
    1000000LL*timeout_sec;

  set_next_timeout_check(abs_wait_timeout);
  DBUG_VOID_RETURN;
}


/**
  Handle a (rare) special case,where connection needs to
  migrate to a different group because group_count has changed
  after thread_pool_size setting.
*/

static int change_group(TP_connection_generic *c,
 thread_group_t *old_group,
 thread_group_t *new_group)
{
  int ret= 0;

  DBUG_ASSERT(c->thread_group == old_group);

  /* Remove connection from the old group. */
  mysql_mutex_lock(&old_group->mutex);
  if (c->bound_to_poll_descriptor)
  {
    io_poll_disassociate_fd(old_group->pollfd,c->fd);
    c->bound_to_poll_descriptor= false;
  }
  c->thread_group->connection_count--;
  mysql_mutex_unlock(&old_group->mutex);

  /* Add connection to the new group. */
  mysql_mutex_lock(&new_group->mutex);
  c->thread_group= new_group;
  new_group->connection_count++;
  /* Ensure that there is a listener in the new group. */
  if (!new_group->thread_count)
    ret= create_worker(new_group, false);
  mysql_mutex_unlock(&new_group->mutex);
  return ret;
}


int TP_connection_generic::start_io()
{
  /*
    Usually, connection will stay in the same group for the entire
    connection's life. However, we do allow group_count to
    change at runtime, which means in rare cases when it changes is 
    connection should need to migrate  to another group, this ensures
    to ensure equal load between groups.

    So we recalculate in which group the connection should be, based
    on thread_id and current group count, and migrate if necessary.
  */
  if (fix_group)
  {
    fix_group = false;
    thread_group_t *new_group= &all_groups[get_group_id(thd->thread_id)];

    if (new_group != thread_group)
    {
      if (change_group(this, thread_group, new_group))
        return -1;
    }
  }

  /*
    Bind to poll descriptor if not yet done.
  */
#ifdef COROUTINE_ENABLED
  if (!bound_to_poll_descriptor)
  {
    bound_to_poll_descriptor= true;
    epoll_events_.store(0, std::memory_order_relaxed);
    events_snapshot_= 0;
    return io_poll_associate_fd(thread_group->pollfd, fd, this,
                                OPTIONAL_IO_POLL_READ_PARAM);
  }
  else
  {
    // If CAS fails, there is a new epoll event since last read, re-enqueues
    // the connection for execution.
    assert(events_snapshot_ > 0);
    if (!epoll_events_.compare_exchange_strong(events_snapshot_, 0,
                                               std::memory_order_acq_rel))
    {
      events_snapshot_= 1;
      epoll_events_.store(1, std::memory_order_relaxed);
      thread_group->coroutine_info_->req_queue_.Enqueue(this);
    }
    return 0;
  }
#else
  if (!bound_to_poll_descriptor)
  {
    bound_to_poll_descriptor= true;
    return io_poll_associate_fd(thread_group->pollfd, fd, this, OPTIONAL_IO_POLL_READ_PARAM);
  }

  return io_poll_start_read(thread_group->pollfd, fd, this, OPTIONAL_IO_POLL_READ_PARAM);
#endif
}

int TP_connection_generic::end_io()
{
  return io_poll_disassociate_fd(thread_group->pollfd, fd);
}

/**
  Worker thread's main
*/

std::function<
  std::pair<std::function<void()>,
  std::function<void(int16_t)>>(int16_t)> get_tx_service_functors;

static void *worker_main(void *param)
{
  worker_thread_t this_thread;
  pthread_detach_this_thread();
  my_thread_init();

  DBUG_ENTER("worker_main");
  is_sql_thd = true;

  thread_group_t *thread_group = (thread_group_t *)param;

  /* Init per-thread structure */
  mysql_cond_init(key_worker_cond, &this_thread.cond, NULL);
  this_thread.thread_group= thread_group;
  this_thread.event_count=0;
#ifdef COROUTINE_ENABLED
  CoroutineInfo *coro_info = thread_group->coroutine_info_.get();
  thread_group->active_thread_count.fetch_add(1, std::memory_order_relaxed);
#if defined (EXT_TX_PROC_ENABLED) && !defined (ELOQ_MODULE_ENABLED)
  if (coro_info->tx_processor_exec_ == nullptr)
  {
    int16_t group_id = coro_info->group_id_;
    assert(group_id >= 0);
    auto functors = get_tx_service_functors(group_id);
    coro_info->tx_processor_exec_ = functors.first;
    coro_info->update_ext_proc_ = functors.second;
  }
  (coro_info->update_ext_proc_)(1);
#endif
#endif

  struct timespec ts;

  /* Run event loop */
  for(;;)
  {
#ifdef COROUTINE_ENABLED
    set_timespec(ts, threadpool_idle_timeout);

    conns_cnt = 0;
    get_event_bulk(&this_thread, thread_group, &ts);
    
    if (conns_cnt == 0)
    {
      break;
    }

    for (conns_offset = 0; conns_offset < conns_cnt; ++conns_offset)
    {
      TP_connection_generic *conn = local_conns[conns_offset];

      if (conn == nullptr)
      {
        continue;
      }

      if (conn->being_processed_.load(std::memory_order_relaxed))
      {
        // The connection's corresponding coroutine has not returned, but the
        // connection has been scheduled to resume and is picked up by a separate
        // physical thread. Re-enqueus the connection for re-execution, until the
        // prior execution of coroutine has fully returned.
        // mysql_mutex_lock(&thread_group->mutex);
        // thread_group->coroutine_info_->coroutine_queue_.emplace_back(conn);
        // mysql_mutex_unlock(&thread_group->mutex);
        thread_group->coroutine_info_->resume_queue_.Enqueue(conn);
        continue;
      }

      if (conn->thd == nullptr ||
          conn->thd->coro_status_ == THD::CoroStatus::Empty)
      {
        this_thread.event_count++;
      }

      tp_callback(conn);
    }

#ifdef IOURING_ENABLED
    if (iouring_wrap.to_submit_reqs_ > 0)
    {
      int ret= io_uring_submit(&iouring_wrap.ring_);
      if (ret > 0)
      {
        iouring_wrap.to_peek_reqs_+= ret;
        iouring_wrap.to_submit_reqs_-= ret;
      }
      else
      {
        sql_print_error(
            "ThreadGroup(%d): failed to do io_uring_submit, error %d.",
            coro_info->group_id_, ret);
      }
    }
#endif

#if defined (EXT_TX_PROC_ENABLED) && !defined (ELOQ_MODULE_ENABLED)
    (thread_group->coroutine_info_->tx_processor_exec_)();
#endif

#ifdef IOURING_ENABLED
    if (iouring_wrap.to_peek_reqs_ > 0)
    {
      unsigned head;
      struct io_uring_cqe *cqe;
      int ret= io_uring_peek_cqe(&iouring_wrap.ring_, &cqe);
      if (ret == 0)
      {
        unsigned num_completed= 0;
        io_uring_for_each_cqe(&iouring_wrap.ring_, head, cqe)
        {
          ++num_completed;
          // resume the sql task
          THD *thd= (THD *) cqe->user_data;
          assert(thd != nullptr);
          thd->iouring_cqe_res_= cqe->res;
          auto resume_fp= thd->CoroFunctors().second;
          assert(resume_fp!=nullptr);
          (*resume_fp)();
        }
        io_uring_cq_advance(&iouring_wrap.ring_, num_completed);
        iouring_wrap.to_peek_reqs_-= num_completed;
      }
    }
#endif

    if (!thread_group->shutdown.load(std::memory_order_relaxed))
    {
      native_event ev[MAX_EVENTS];
      int cnt= io_poll_wait(thread_group->pollfd, ev, MAX_EVENTS, 0);
      if (cnt > 0)
      {
        lockfree_queue_put_bulk(thread_group, ev, cnt);
      }
    }
#else
    TP_connection_generic *connection;
    set_timespec(ts,threadpool_idle_timeout);
    connection = get_event(&this_thread, thread_group, &ts);
    if (!connection)
      break;
    this_thread.event_count++;
    tp_callback(connection);
#endif
  }

  /* Thread shutdown: cleanup per-worker-thread structure. */
  mysql_cond_destroy(&this_thread.cond);

  bool last_thread;                    /* last thread in group exits */
  mysql_mutex_lock(&thread_group->mutex);
  add_thread_count(thread_group, -1);
  last_thread= ((thread_group->thread_count == 0) && thread_group->shutdown);
  mysql_mutex_unlock(&thread_group->mutex);

  /* Last thread in group exits and pool is terminating, destroy group.*/
  if (last_thread)
    thread_group_destroy(thread_group);

  my_thread_end();
  return NULL;
}


TP_pool_generic::TP_pool_generic()
{}

int TP_pool_generic::init()
{
  DBUG_ENTER("TP_pool_generic::TP_pool_generic");
  threadpool_max_size= MY_MAX(threadpool_size, 128);
  all_groups= (thread_group_t *)
    my_malloc(PSI_INSTRUMENT_ME,
              sizeof(thread_group_t) * threadpool_max_size, MYF(MY_WME|MY_ZEROFILL));
  if (!all_groups)
  {
    threadpool_max_size= 0;
    sql_print_error("Allocation failed");
    DBUG_RETURN(-1);
  }
  PSI_register(mutex);
  PSI_register(cond);
  PSI_register(thread);
  scheduler_init();
  threadpool_started= true;
  for (uint i= 0; i < threadpool_max_size; i++)
  {
    thread_group_init(&all_groups[i], get_connection_attrib());
#ifdef COROUTINE_ENABLED
    all_groups[i].coroutine_info_->group_id_ = i;
#endif
  }
  set_pool_size(threadpool_size);
  if(group_count == 0)
  {
    /* Something went wrong */
    sql_print_error("Can't set threadpool size to %d",threadpool_size);
    DBUG_RETURN(-1);
  }
  pool_timer.tick_interval= threadpool_stall_limit;
  start_timer(&pool_timer);
#ifdef ELOQ_MODULE_ENABLED
  register_module(&maria_module);
#endif
  DBUG_RETURN(0);
}

TP_pool_generic::~TP_pool_generic()
{
  DBUG_ENTER("tp_end");

  if (!threadpool_started)
    DBUG_VOID_RETURN;

#ifdef ELOQ_MODULE_ENABLED
  unregister_module(&maria_module);
#endif

  stop_timer(&pool_timer);
  shutdown_group_count= threadpool_max_size;
  for (uint i= 0; i < threadpool_max_size; i++)
  {
    thread_group_close(&all_groups[i]);
  }

  /*
    Wait until memory occupied by all_groups is freed.
  */
  int timeout_ms=5000;
  while(all_groups && timeout_ms--)
    my_sleep(1000);

  threadpool_started= false;
  DBUG_VOID_RETURN;
}


static my_bool thd_reset_group(THD* thd, void*)
{
  auto c= (TP_connection_generic*)thd->event_scheduler.data;
  if(c)
    c->fix_group= true;
  return FALSE;
}

/** Ensure that poll descriptors are created when threadpool_size changes */
int TP_pool_generic::set_pool_size(uint size)
{
  bool success= true;

#ifdef ELOQ_MODULE_ENABLED
  maria_module.ResizeGroups(size);
#endif

  for(uint i=0; i< size; i++)
  {
    thread_group_t *group= &all_groups[i];
    mysql_mutex_lock(&group->mutex);
    if (group->pollfd == INVALID_HANDLE_VALUE)
    {
      group->pollfd= io_poll_create();
      success= (group->pollfd != INVALID_HANDLE_VALUE);
      if(!success)
      {
        sql_print_error("io_poll_create() failed, errno=%d", errno);
      }
    }
    mysql_mutex_unlock(&group->mutex);
    if (!success)
    {
      group_count= i;
#ifdef ELOQ_MODULE_ENABLED
      maria_module.ResizeGroups(i);
#endif
      return -1;
    }
#ifdef ELOQ_MODULE_ENABLED
    maria_module.SetGroup(i, group);
    create_worker(group, false);
#endif
  }
  group_count= size;
  server_threads.iterate(thd_reset_group);
  return 0;
}

int TP_pool_generic::set_stall_limit(uint limit)
{
  mysql_mutex_lock(&(pool_timer.mutex));
  pool_timer.tick_interval= limit;
  mysql_mutex_unlock(&(pool_timer.mutex));
  mysql_cond_signal(&(pool_timer.cond));
  return 0;
}


/**
 Calculate number of idle/waiting threads in the pool.

 Sum idle threads over all groups.
 Don't do any locking, it is not required for stats.
*/

int TP_pool_generic::get_idle_thread_count()
{
  int sum=0;
  for (uint i= 0; i < threadpool_max_size && all_groups[i].pollfd != INVALID_HANDLE_VALUE; i++)
  {
    sum+= (all_groups[i].thread_count - all_groups[i].active_thread_count);
  }
  return sum;
}


/* Report threadpool problems */

/**
   Delay in microseconds, after which "pool blocked" message is printed.
   (30 sec == 30 Mio usec)
*/
#define BLOCK_MSG_DELAY (30*1000000)

#define MAX_THREADS_REACHED_MSG \
"Threadpool could not create additional thread to handle queries, because the \
number of allowed threads was reached. Increasing 'thread_pool_max_threads' \
parameter can help in this situation.\n \
If 'extra_port' parameter is set, you can still connect to the database with \
superuser account (it must be TCP connection using extra_port as TCP port) \
and troubleshoot the situation. \
A likely cause of pool blocks are clients that lock resources for long time. \
'show processlist' or 'show engine innodb status' can give additional hints."

#define CREATE_THREAD_ERROR_MSG "Can't create threads in threadpool (errno=%d)."

/**
 Write a message when blocking situation in threadpool occurs.
 The message is written only when pool blocks for BLOCK_MSG_DELAY (30) seconds.
 It will be just a single message for each blocking situation (to prevent
 log flood).
*/

static void print_pool_blocked_message(bool max_threads_reached)
{
  ulonglong now;
  static bool msg_written;

  now= microsecond_interval_timer();
  if (pool_block_start == 0)
  {
    pool_block_start= now;
    msg_written = false;
    return;
  }

  if (now > pool_block_start + BLOCK_MSG_DELAY && !msg_written)
  {
    if (max_threads_reached)
      sql_print_warning(MAX_THREADS_REACHED_MSG);
    else
      sql_print_warning(CREATE_THREAD_ERROR_MSG, my_errno);

    sql_print_information("Threadpool has been blocked for %u seconds\n",
      (uint)((now- pool_block_start)/1000000));
    /* avoid reperated messages for the same blocking situation */
    msg_written= true;
  }
}

#ifdef COROUTINE_ENABLED
int thread_group_t::WakeOrCreateThread()
{
  return wake_or_create_thread(this, false, true);
}

bool thread_group_t::HasActiveWorker() const
{
  return ext_worker_active_.load(std::memory_order_relaxed) ||
         active_thread_count.load(std::memory_order_relaxed) > 0;
}

#ifdef ELOQ_MODULE_ENABLED
void MariaModule::ExtThdStart(int thd_id)
{
  if (thd_id < 0 || (size_t) thd_id >= groups_.size())
  {
    return;
  }

  is_sql_thd = false;
  groups_[thd_id]->ext_worker_active_.store(true, std::memory_order_relaxed);
}

void MariaModule::ExtThdEnd(int thd_id)
{
  if (thd_id < 0 || (size_t) thd_id >= groups_.size())
  {
    return;
  }
  thread_group_t *group = groups_[thd_id];
  group->ext_worker_active_.store(false, std::memory_order_relaxed);

  // Wakes up the listener thread.
  std::unique_lock<std::mutex> lk(group->listener_mux_);
  group->listener_cv_.notify_one();
}

void MariaModule::Process(int thd_id)
{
  if (thd_id < 0 || (size_t) thd_id >= groups_.size())
  {
    return;
  }

  thread_group_t *group = groups_[thd_id];
  if (group->shutdown.load(std::memory_order_relaxed))
  {
    return;
  }

  // Increments the queue event count so that the stall checker does not miss
  // the external worker thread.
  group->queue_event_count.fetch_add(1, std::memory_order_relaxed);

  CoroutineInfo *coro_info= group->coroutine_info_.get();

  conns_cnt= coro_info->resume_queue_.TryDequeueBulk(local_conns.begin(),
                                                     local_conns.size());
  if (conns_cnt == 0)
  {
    conns_cnt= coro_info->req_queue_.TryDequeueBulk(local_conns.begin(),
                                                    local_conns.size());
  }

  for (conns_offset= 0; conns_offset < conns_cnt; ++conns_offset)
  {
    TP_connection_generic *conn= local_conns[conns_offset];

    if (conn->being_processed_.load(std::memory_order_relaxed))
    {
      // The connection's corresponding coroutine has not returned, but the
      // connection has been scheduled to resume and is picked up by a separate
      // physical thread. Re-enqueus the connection for re-execution, until the
      // prior execution of coroutine has fully returned.
      group->coroutine_info_->resume_queue_.Enqueue(conn);
      continue;
    }

    tp_callback(conn);
  }

#ifdef IOURING_ENABLED
  if (iouring_wrap.to_submit_reqs_ > 0)
  {
    int ret= io_uring_submit(&iouring_wrap.ring_);
    if (ret > 0)
    {
      iouring_wrap.to_peek_reqs_+= ret;
      iouring_wrap.to_submit_reqs_-= ret;
    }
    else
    {
      sql_print_error(
          "ThreadGroup(%d): failed to do io_uring_submit, error %d.",
          coro_info->group_id_, ret);
    }
  }
#endif

#ifdef IOURING_ENABLED
  if (iouring_wrap.to_peek_reqs_ > 0)
  {
    unsigned head;
    struct io_uring_cqe *cqe;
    int ret= io_uring_peek_cqe(&iouring_wrap.ring_, &cqe);
    if (ret == 0)
    {
      unsigned num_completed= 0;
      io_uring_for_each_cqe(&iouring_wrap.ring_, head, cqe)
      {
        ++num_completed;
        // resume the sql task
        THD *thd= (THD *) cqe->user_data;
        assert(thd != nullptr);
        thd->iouring_cqe_res_= cqe->res;
        auto resume_fp= thd->CoroFunctors().second;
        assert(resume_fp != nullptr);
        (*resume_fp)();
      }
      io_uring_cq_advance(&iouring_wrap.ring_, num_completed);
      iouring_wrap.to_peek_reqs_-= num_completed;
    }
  }
#endif

  native_event ev[MAX_EVENTS];
  int cnt= io_poll_wait(group->pollfd, ev, MAX_EVENTS, 0);
  if (cnt > 0)
  {
    lockfree_queue_put_bulk(group, ev, cnt);
  }
}

bool MariaModule::HasTask(int thd_id) const
{
  if (thd_id < 0 || (size_t) thd_id >= groups_.size())
  {
    return false;
  }

  thread_group_t *group= groups_[thd_id];
  CoroutineInfo *coro_info= group->coroutine_info_.get();
  return !coro_info->req_queue_.IsEmpty() ||
         coro_info->coro_cnt_.load(std::memory_order_relaxed) > 0;
}
#endif
#endif

#if defined(COROUTINE_ENABLED) && defined(IOURING_ENABLED)
IoUringWrapper::IoUringWrapper()
    : ring_(), to_submit_reqs_(0), to_peek_reqs_(0)
{
  uint32_t entry_size= (max_connections / threadpool_size) * 1.5;
  entry_size= MY_MAX(entry_size, 128U);
  struct io_uring_params params;
  memset(&params, 0, sizeof(struct io_uring_params));
  // Try set iouring flags to improve performance:
  // IORING_SETUP_COOP_TASKRUN(linux-kernel>=5.19);
  // IORING_SETUP_SINGLE_ISSUER(linux-kernel>=6.0);
#if LINUX_KERNEL_MAJOR_VERSION >= 6
  params.flags= IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_COOP_TASKRUN;
#elif LINUX_KERNEL_MAJOR_VERSION >= 5 && LINUX_KERNEL_MINOR_VERSION >= 19
  params.flags= IORING_SETUP_COOP_TASKRUN;
#endif
  int ret= io_uring_queue_init_params(entry_size, &ring_, &params);
  if (ret != 0)
  {
    sql_print_error("IoUringWrapper(%p): failed to init io uring, error:%d",
                    this, ret);
    init_success_= false;
    return;
  }
  else
  {
    init_success_= true;
  }
}

IoUringWrapper::~IoUringWrapper() { io_uring_queue_exit(&ring_); }

#endif

#ifdef COROUTINE_ENABLED
static inline TP_connection *get_TP_connection(THD *thd)
{
  return (TP_connection *)thd->event_scheduler.data;
}

#ifdef IOURING_ENABLED
ssize_t iouring_socket_send(
#ifdef HAVE_PSI_SOCKET_INTERFACE
    const char *src_file, uint src_line,
#endif
    MYSQL_SOCKET mysql_socket, const SOCKBUF_T *buf, size_t n, int flags)
{
  ssize_t result;
  DBUG_ASSERT(mysql_socket.fd != INVALID_SOCKET);
#ifdef HAVE_PSI_SOCKET_INTERFACE
  if (psi_likely(mysql_socket.m_psi != NULL))
  {
    /* Instrumentation start */
    PSI_socket_locker *locker;
    PSI_socket_locker_state state;
    locker= PSI_SOCKET_CALL(start_socket_wait)(
        &state, mysql_socket.m_psi, PSI_SOCKET_SEND, n, src_file, src_line);

    /* Instrumented code */
    result= send(mysql_socket.fd, buf, IF_WIN((int), ) n, flags);

    /* Instrumentation end */
    if (locker != NULL)
    {
      size_t bytes_written= (result > 0) ? (size_t) result : 0;
      PSI_SOCKET_CALL(end_socket_wait)(locker, bytes_written);
    }

    return result;
  }
#endif

  THD *thd= _current_thd();

  const std::function<void()> *yield_fp= thd->CoroFunctors().first;
  if (*thd->CoroLongResumeFunctor() == nullptr || yield_fp == nullptr ||
      thd->iouring_ == nullptr)
  {
    return mysql_socket_send(mysql_socket, buf, IF_WIN((int), ) n, flags);
  }
  assert(&iouring_wrap == thd->iouring_);
  struct io_uring_sqe *sqe= io_uring_get_sqe(&(thd->iouring_->ring_));
  if (sqe == nullptr)
  {
    sql_print_warning("ThreadGroup(%d): THD(%d) failed to get io_uring_sqe, "
                      "use mysql_socket_send.",
                      thd->ThdGroupId(), thd->thread_id);
    return mysql_socket_send(mysql_socket, buf, IF_WIN((int), ) n, flags);
  }
  io_uring_sqe_set_data(sqe, (void *) thd);
  io_uring_prep_send(sqe, mysql_socket.fd, buf, n, flags);
  thd->iouring_->to_submit_reqs_++;

  (*yield_fp)();

  if (thd->iouring_cqe_res_ >= 0)
  {
    return thd->iouring_cqe_res_;
  }
  else
  {
    // Operation may be not supportted by io_uring or some error occurs,
    // retry with mysql_socket_send.
    sql_print_warning(
        "ThreadGroup(%d): THD(%d) failed to send socket through io uring, "
        "error %d. Retry through mysql socket send.",
        thd->ThdGroupId(), thd->thread_id, (int) thd->iouring_cqe_res_);

    return mysql_socket_send(mysql_socket, buf, IF_WIN((int), ) n, flags);
  }
}
#endif

int coro_mutex_trylock(
  mysql_mutex_t *that
#if defined(SAFE_MUTEX) || defined (HAVE_PSI_MUTEX_INTERFACE)
  , const char *src_file, uint src_line
#endif
  )
{
  // Check pthread mutex condition first.
#if defined(SAFE_MUTEX) || defined (HAVE_PSI_MUTEX_INTERFACE)
  int res= inline_mysql_mutex_trylock(that, src_file, src_line);
#else
  int res= inline_mysql_mutex_trylock(that);
#endif

  if (res == 0)
  {
    // Check if this mutex is held by a coroutine process.
    if (that->l.prev != 0 || that->l.next != 0)
    {
      // Already held by another coroutine process
      res= EBUSY;
      inline_mysql_mutex_unlock(that 
#if defined (SAFE_MUTEX)
    , src_file, src_line
#endif
      );
    }
    else if (current_thd && current_thd->ThdGroupId() >= 0){
      // Put this lock into thread local acquired mutex list
      TP_connection *c= get_TP_connection(current_thd);
      TP_connection_generic *connection=(TP_connection_generic *)c;
      connection->acquired_mutexes= list_add(connection->acquired_mutexes, &that->l);
    }
  }
  
  return res;
}

int coro_mutex_lock(
  mysql_mutex_t *that
#if defined(SAFE_MUTEX) || defined (HAVE_PSI_MUTEX_INTERFACE)
  , const char *src_file, uint src_line
#endif
  )
{
  int res= -1;
  THD *thd= current_thd;
  bool is_coro_thd = thd && thd->ThdGroupId() >= 0;

  const std::function<void()> *long_resume_fp= nullptr;
  const std::function<void()> *yield_fp= nullptr;
  // Since we release the acquired mutexes when a coroutine yields away,
  // we need to verify if the mutex is held by a yielded coroutine. If the
  // mutex is on a threadpool connection acquired mutex list, we need to keep
  // retrying until the mutex is released by the coroutine.
  while (true)
  {
    res= inline_mysql_mutex_lock(that
#if defined(SAFE_MUTEX) || defined (HAVE_PSI_MUTEX_INTERFACE)
    ,src_file, src_line
#endif
    );
    // If lock is acquired by other coroutine, busy loop until the lock is released.
    if (res == 0 && (that->l.prev || that->l.next))
    {
      inline_mysql_mutex_unlock(that 
#if defined (SAFE_MUTEX) 
    , src_file, src_line
#endif
      );
      if (is_coro_thd)
      {
        if (!long_resume_fp)
        {
          long_resume_fp= thd->CoroLongResumeFunctor();
          if (!*long_resume_fp)
          {
            // If long resume fp is not set then this is not a coroutine. We can safely
            // busy loop here without worrying about deadlock.
            is_coro_thd= false;
            continue;
          }
          yield_fp= thd->CoroFunctors().first;
        }

        // Yield this coroutine process so that it won't block others.
        (*long_resume_fp)();
        (*yield_fp)();
      }
    }
    else
    {
      break;
    }
  }

  if (res == 0 && thd && thd->ThdGroupId() >= 0)
  {
    // Put this lock into thread local acquired mutex list
    TP_connection *c= get_TP_connection(current_thd);
    TP_connection_generic *connection=(TP_connection_generic *)c;
    connection->acquired_mutexes= list_add(connection->acquired_mutexes, &that->l);
  }

  return res;
}

int coro_mutex_unlock(
  mysql_mutex_t *that
#ifdef SAFE_MUTEX
  , const char *src_file, uint src_line
#endif
  )
{
#ifdef SAFE_MUTEX
    int result= inline_mysql_mutex_unlock(that, src_file, src_line);
#else
    int result= inline_mysql_mutex_unlock(that);
#endif

  if (result == 0 && current_thd && current_thd->ThdGroupId() >= 0)
  {
    // remove mutex from thd acquired mutex list.
    TP_connection *c= get_TP_connection(current_thd);
    TP_connection_generic *connection=(TP_connection_generic *)c;
    connection->acquired_mutexes= list_delete(connection->acquired_mutexes, &that->l);
    that->l.prev = NULL;
    that->l.next = NULL;
    DBUG_ASSERT(that->l.prev == 0 && that->l.next == 0);
  }

  return result;
}

int coro_cond_timedwait(
  mysql_cond_t *that,
  mysql_mutex_t *mutex,
  const struct timespec *abstime
#if defined(SAFE_MUTEX) || defined(HAVE_PSI_COND_INTERFACE)
  , const char *src_file, uint src_line
#endif
  )
{
  // mutex will be released in cond wait, remove it from thd mutex list
  if (current_thd && current_thd->ThdGroupId() >= 0)
  {
    TP_connection *c= get_TP_connection(current_thd);
    TP_connection_generic *connection=(TP_connection_generic *)c;
    connection->acquired_mutexes= list_delete(connection->acquired_mutexes, &mutex->l);
    mutex->l.prev = NULL;
    mutex->l.next = NULL;
    DBUG_ASSERT(mutex->l.prev == 0 && mutex->l.next == 0);
  }
  int res= inline_mysql_cond_timedwait(that, mutex, abstime
#if defined(SAFE_MUTEX) || defined(HAVE_PSI_COND_INTERFACE)
  , src_file, src_line
#endif
  );

  
    // If mutex is held by any other coroutine, busy loop until
    // it is released
    while (mutex->l.prev || mutex->l.next)
    {
#ifdef SAFE_MUTEX
      inline_mysql_mutex_unlock(mutex, __FILE__, __LINE__);
#else
      inline_mysql_mutex_unlock(mutex);
#endif
#if defined(SAFE_MUTEX) || defined(HAVE_PSI_MUTEX_INTERFACE)
      inline_mysql_mutex_lock(mutex, __FILE__, __LINE__);
#else
      inline_mysql_mutex_lock(mutex);
#endif
    }
    if (current_thd && current_thd->ThdGroupId() >= 0)
    {
      // Put this lock into thread local acquired mutex list
      TP_connection *c= get_TP_connection(current_thd);
      TP_connection_generic *connection= (TP_connection_generic *) c;
      DBUG_ASSERT(mutex->l.prev == 0 && mutex->l.next == 0);
      connection->acquired_mutexes=
          list_add(connection->acquired_mutexes, &mutex->l);
    }
  
  return res;
}

int coro_cond_wait(
  mysql_cond_t *that,
  mysql_mutex_t *mutex
#if defined(SAFE_MUTEX) || defined(HAVE_PSI_COND_INTERFACE)
  , const char *src_file, uint src_line
#endif
  )
{
  // mutex will be released in cond wait, remove it from thd mutex list
  if (current_thd && current_thd->ThdGroupId() >= 0)
  {
    TP_connection *c= get_TP_connection(current_thd);
    TP_connection_generic *connection=(TP_connection_generic *)c;
    connection->acquired_mutexes= list_delete(connection->acquired_mutexes, &mutex->l);
    mutex->l.prev = NULL;
    mutex->l.next = NULL;
    DBUG_ASSERT(mutex->l.prev == 0 && mutex->l.next == 0);
  }
  int res= inline_mysql_cond_wait(that, mutex
#if defined(SAFE_MUTEX) || defined(HAVE_PSI_COND_INTERFACE)
  , src_file, src_line
#endif
  );

  
    // If mutex is held by any other coroutine, busy loop until
    // it is released
    while (mutex->l.prev || mutex->l.next)
    {
#ifdef SAFE_MUTEX
      inline_mysql_mutex_unlock(mutex, __FILE__, __LINE__);
#else
      inline_mysql_mutex_unlock(mutex);
#endif
#if defined(SAFE_MUTEX) || defined(HAVE_PSI_MUTEX_INTERFACE)
      inline_mysql_mutex_lock(mutex, __FILE__, __LINE__);
#else
      inline_mysql_mutex_lock(mutex);
#endif
    }
    if (current_thd && current_thd->ThdGroupId() >= 0)
    {
      // Put this lock into thread local acquired mutex list
      TP_connection *c= get_TP_connection(current_thd);
      TP_connection_generic *connection= (TP_connection_generic *) c;
      DBUG_ASSERT(mutex->l.prev == 0 && mutex->l.next == 0);
      connection->acquired_mutexes=
          list_add(connection->acquired_mutexes, &mutex->l);
    }
  
  return res;
}
#endif
#endif /* HAVE_POOL_OF_THREADS */
