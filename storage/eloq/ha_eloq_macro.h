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

/*
  Portability: use __PRETTY_FUNCTION__ when available, otherwise use __func__
  which is in the standard.
*/

#ifdef __GNUC__
#define __PORTABLE_PRETTY_FUNCTION__ __PRETTY_FUNCTION__
#else
#define __PORTABLE_PRETTY_FUNCTION__ __func__
#endif

/* [RocksDB]
  Intent behind this macro is to avoid manually typing the function name every
  time we want to add the debugging statement and use the compiler for this
  work. This avoids typical refactoring problems when one renames a function,
  but the tracing message doesn't get updated.

  We could use __func__ or __FUNCTION__ macros, but __PRETTY_FUNCTION__
  contains the signature of the function as well as its bare name and provides
  therefore more context when interpreting the logs.
*/
#define DBUG_ENTER_FUNC() DBUG_ENTER(__PORTABLE_PRETTY_FUNCTION__)

/*
  From MyRocks:
  Introduce C-style pseudo-namespaces, a handy way to make code more readble
  when calling into a legacy API, which does not have any namespace defined.
  Since we cannot or don't want to change the API in any way, we can use this
  mechanism to define readability tokens that look like C++ namespaces, but are
  not enforced in any way by the compiler, since the pre-compiler strips them
  out. However, on the calling side, code looks like my_core::thd_get_ha_data()
  rather than plain a thd_get_ha_data() call. This technique adds an immediate
  visible cue on what type of API we are calling into.
*/

#ifndef mysql
// C-style pseudo-namespace for MySQL Core API, to be used in decorating calls
// to non-obvious MySQL functions, like the ones that do not start with well
// known prefixes: "my_", "sql_", and "mysql_".
#define mysql
#endif // mysql

/**
 * @brief Max length of an index field. The number is consistent with InnoDB.
 *
 */
#define MAX_INDEX_FIELD_LEN 3072

#if !defined(DBUG_OFF) && !defined(_lint)
#define REPORT_DEBUG_INFO(format, ...)                                        \
  do                                                                          \
  {                                                                           \
    extern my_bool eloq_report_debug_info;                                    \
    if (eloq_report_debug_info)                                               \
    {                                                                         \
      push_warning_printf(ha_thd(), Sql_condition::WARN_LEVEL_NOTE,           \
                          ER_UNKNOWN_ERROR, format, __VA_ARGS__);             \
    }                                                                         \
  } while (0);
#else
#define REPORT_DEBUG_INFO(thd, level, code, format, ...)
#endif