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

#include <string>

class THD;
struct TABLE_LIST;
struct Schema_specification_st;
typedef struct st_mysql_const_lex_string LEX_CSTRING;
template <class Elem> class Dynamic_array;

#define plugin_handle(pi) ((plugin_dlib(pi))->handle)

//-- Below macros are borrowed from linux kernel SYSCALL_DEFINEx

#define __SC_DECL1(t1, a1) t1 a1
#define __SC_DECL2(t2, a2, ...) t2 a2, __SC_DECL1(__VA_ARGS__)
#define __SC_DECL3(t3, a3, ...) t3 a3, __SC_DECL2(__VA_ARGS__)
#define __SC_DECL4(t4, a4, ...) t4 a4, __SC_DECL3(__VA_ARGS__)
#define __SC_DECL5(t5, a5, ...) t5 a5, __SC_DECL4(__VA_ARGS__)
#define __SC_DECL6(t6, a6, ...) t6 a6, __SC_DECL5(__VA_ARGS__)

#define __SC_CAST1(t1, a1) (t1) a1
#define __SC_CAST2(t2, a2, ...) (t2) a2, __SC_CAST1(__VA_ARGS__)
#define __SC_CAST3(t3, a3, ...) (t3) a3, __SC_CAST2(__VA_ARGS__)
#define __SC_CAST4(t4, a4, ...) (t4) a4, __SC_CAST3(__VA_ARGS__)
#define __SC_CAST5(t5, a5, ...) (t5) a5, __SC_CAST4(__VA_ARGS__)
#define __SC_CAST6(t6, a6, ...) (t6) a6, __SC_CAST5(__VA_ARGS__)

#define __ARG1(t1, a1, ...) a1

/**
 * Export global functions defined in eloq.
 *
 * These functions must be wrapped with extern "C", and function signature
 * must match with that defined by ELOQ_DEFINEx.
 */
// Statically linked: declare the symbol and let the linker resolve it to the
// implementation provided in the static library (see eloq_db.cpp extern "C"
// block).
#define MONOCALL_DEFINEx(x, name, ...)                                        \
  extern "C" int name(__SC_DECL##x(__VA_ARGS__));
#define MONOCALL_DEFINE1(name, ...) MONOCALL_DEFINEx(1, name, __VA_ARGS__)
#define MONOCALL_DEFINE2(name, ...) MONOCALL_DEFINEx(2, name, __VA_ARGS__)
#define MONOCALL_DEFINE3(name, ...) MONOCALL_DEFINEx(3, name, __VA_ARGS__)
#define MONOCALL_DEFINE4(name, ...) MONOCALL_DEFINEx(4, name, __VA_ARGS__)
#define MONOCALL_DEFINE5(name, ...) MONOCALL_DEFINEx(5, name, __VA_ARGS__)
#define MONOCALL_DEFINE6(name, ...) MONOCALL_DEFINEx(6, name, __VA_ARGS__)

//-- Export dynamic loaded interfaces in eloq module.

/**
 * @brief Create database.
 *        Default charset, default collation for tables in db, and db comment
 * will be stored into kv storage.
 * @return 0 success,
 *         ER_TOO_LONG_DATABASE_COMMENT,
 *         HA_ERR_INTERNAL_ERROR.
 */
MONOCALL_DEFINE3(eloq_create_database, THD *, thd, LEX_CSTRING, db,
                 const Schema_specification_st *, create_info);

/**
 * @brief Alter database.
 * @return 0 success,
 *         ER_TOO_LONG_DATABASE_COMMENT,
 *         HA_ERR_INTERNAL_ERROR.
 *         HA_ERR_KEY_NOT_FOUND
 */
MONOCALL_DEFINE3(eloq_update_database, THD *, thd, LEX_CSTRING, db,
                 const Schema_specification_st *, create_info);

/**
 * @brief Drop database
 * @return 0 success,
 *         HA_ERR_INTERNAL_ERROR.
 */
MONOCALL_DEFINE2(eloq_drop_database, THD *, thd, LEX_CSTRING, db);

/**
 * @brief Check whether database exist.
 * @return 0 success,
 *         HA_ERR_INTERNAL_ERROR.
 *
 */
MONOCALL_DEFINE3(eloq_exist_database, THD *, thd, LEX_CSTRING, db, bool &,
                 exist);

/**
 * @brief Fetch database definition.
 *        Like load_db_opt, set table default charset when load db failed.
 * @return 0 success,
 *         HA_ERR_KEY_NOT_FOUND,
 *         HA_ERR_INTERNAL_ERROR.
 */
MONOCALL_DEFINE3(eloq_discover_database, THD *, thd, LEX_CSTRING, db,
                 Schema_specification_st *, create_info);

/**
 * @brief List all databases.
 * @return 0 success,
 *         HA_ERR_INTERNAL_ERROR.
 */
MONOCALL_DEFINE2(eloq_discover_database_names, THD *, thd,
                 Dynamic_array<LEX_CSTRING *> *, dbnames);

/**
 * @brief List all databases.
 * @return 0 success,
 *         HA_ERR_INTERNAL_ERROR.
 */
MONOCALL_DEFINE2(eloq_discover_database_names_wild, THD *, thd,
                 Discovered_table_list &, tl);

/**
 * @brief Create view.
 * @return 0 success,
 *         ER_DATA_TOO_LONG
 *         HA_ERR_INTERNAL_ERROR.
 */
MONOCALL_DEFINE6(eloq_upsert_view, THD *, thd, LEX_CSTRING, db, LEX_CSTRING,
                 view, const LEX_CSTRING *, type, uchar *, base, File_option *,
                 parameter);
MONOCALL_DEFINE5(eloq_upsert_view_p, THD *, thd, LEX_CSTRING, key,
                 const LEX_CSTRING *, type, uchar *, base, File_option *,
                 parameter);

/**
 * @brief Drop view.
 * @return 0 success.
 *         HA_ERR_INTERNAL_ERROR.
 */
MONOCALL_DEFINE3(eloq_drop_view, THD *, thd, LEX_CSTRING, db, LEX_CSTRING,
                 view);
MONOCALL_DEFINE2(eloq_drop_view_p, THD *, thd, LEX_CSTRING, key);

/**
 * @brief Check whether view exist.
 * @return 0 success,
 *         HA_ERR_INTERNAL_ERROR
 */
MONOCALL_DEFINE4(eloq_exist_view, THD *, thd, LEX_CSTRING, db, LEX_CSTRING,
                 view, bool &, exist);
MONOCALL_DEFINE3(eloq_exist_view_p, THD *, thd, LEX_CSTRING, key, bool &,
                 exist);

/**
 * @brief Check view version by comparing it's binary frm.
 * @return 0 success,
 *         HA_ERR_INTERNAL_ERROR.
 */
MONOCALL_DEFINE5(eloq_check_view, THD *, thd, LEX_CSTRING, db, LEX_CSTRING,
                 view, LEX_CSTRING, old_frm_binary, bool &, eq);
MONOCALL_DEFINE4(eloq_check_view_p, THD *, thd, LEX_CSTRING, key, LEX_CSTRING,
                 old_frm_binary, bool &, eq);

/**
 * @brief Fetch view definition.
 * @return 0 success,
 *         HA_ERR_KEY_NOT_FOUND,
 *         HA_ERR_INTERNAL_ERROR.
 */
MONOCALL_DEFINE4(eloq_fetch_view, THD *, thd, LEX_CSTRING, db, LEX_CSTRING,
                 view, LEX_CSTRING &, frm_binary);
MONOCALL_DEFINE3(eloq_fetch_view_p, THD *, thd, LEX_CSTRING, key,
                 LEX_CSTRING &, frm_binary);

/**
 * @brief Check whether view exist.
 * @param frm_name Table name or view name.
 * @return 0 success,
 *         HA_ERR_INTERNAL_ERROR
 */
MONOCALL_DEFINE4(eloq_exist_frm, THD *, thd, LEX_CSTRING, db, LEX_CSTRING,
                 frm_name, bool &, exist);
MONOCALL_DEFINE3(eloq_exist_frm_p, THD *, thd, LEX_CSTRING, key, bool &,
                 exist);

/**
 * @brief Fetch frm from mariadb_tables and mariadb_views
 * @param frm_name Table name or view name.
 * @return 0 success,
 *         HA_ERR_KEY_NOT_FOUND,
 *         HA_ERR_INTERNAL_ERROR.
 */
MONOCALL_DEFINE4(eloq_fetch_frm, THD *, thd, LEX_CSTRING, db, LEX_CSTRING,
                 frm_name, LEX_CSTRING &, frm_binary);
MONOCALL_DEFINE3(eloq_fetch_frm_p, THD *, thd, LEX_CSTRING, key, LEX_CSTRING &,
                 frm_binary);

/**
 * @brief Get node id.
 * @return node id
 */
MONOCALL_DEFINE1(eloq_node_id, THD *, thd);

MONOCALL_DEFINE1(notify_reload_acl_and_cache, THD *, thd);
