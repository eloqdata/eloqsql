#pragma once

inline thread_local bool is_sql_thd{true};

static __attribute__((noinline, unused)) bool GetIsSqlThd()
{
  asm volatile("");
  return is_sql_thd;
}

static __attribute__((noinline, unused)) void SetIsSqlThd(bool val)
{
  asm volatile("");
  is_sql_thd= val;
}