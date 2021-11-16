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

#include "my_global.h"
#include "sql_table.h"
#include <regex>

/**
 * 1. Deal with temporary table name.
 * eg:
 * cassandra tablename:
 * var___folders___ng___h1b022kj3dlc4mssc1k5r18r0000gn___t____sql_temptable_dc88_3_3_0
 * mariadb_tables.tablename:
 * /var/folders/ng/h1b022kj3dlc4mssc1k5r18r0000gn/T/#sql-temptable-dc88-3-3-0
 * The last number(hex) represents node id who creates it.
 *
 * 2. Format db/view/frm name using `charset_filename` format.
 *
 * charset_filename format is used for shell-path originally, which is also
 * used in ha_eloq::create, ha_eloq::delete_table. It escape special
 * character with @dddd.
 */

namespace MyEloq
{
inline const std::regex re_tmp_tablename("^/[[:print:]]+/"
                                         "#sql-temptable-"
                                         "([[:xdigit:]]+)-([[:xdigit:]]+)-"
                                         "([[:xdigit:]]+)-([[:xdigit:]]+)$");
inline bool is_tmp_table(const std::string &tablename)
{
  return std::regex_match(tablename, re_tmp_tablename);
}

inline bool is_tmp_table_of(const std::string &tablename, uint32_t node_id)
{
  std::smatch m;
  if (std::regex_match(tablename, m, re_tmp_tablename))
  {
    std::string node= m[re_tmp_tablename.mark_count()].str();
    uint32_t nid= std::strtoul(node.c_str(), nullptr, 10);
    return nid == node_id;
  }
  else
  {
    return false;
  }
}

/**
 * @brief Get base part of tablename
 * tablename:
 * | ./dbname/basename
 * | /p1/p2/p3/basename
 */
inline std::string basename(const std::string &tablename)
{
  DBUG_ASSERT(std::isalnum(tablename.back()));
  std::string::size_type pos= tablename.find_last_of('/');
  pos++;
  DBUG_ASSERT(pos > 1 && pos < tablename.size());
  return {tablename.begin() + pos, tablename.end()};
}

// Escape (db, table) -> ./dbname/tablename
inline std::string_view marianame_to_monokey(MEM_ROOT *mem_root,
                                             LEX_CSTRING db, LEX_CSTRING name)
{
  DBUG_ASSERT(db.length + name.length < FN_REFLEN);

  char *buffer= (char *) alloc_root(mem_root, FN_REFLEN + 1);
  size_t length=
      build_table_filename(buffer, FN_REFLEN, db.str, name.str, "", 0);
  return {buffer, length};
}

// Escape db -> ./dbname
inline std::string_view mariadb_to_monokey(MEM_ROOT *mem_root, LEX_CSTRING db)
{
  DBUG_ASSERT(db.length < FN_REFLEN);

  char *buffer= (char *) alloc_root(mem_root, FN_REFLEN + 1);
  size_t length= build_table_filename(buffer, FN_REFLEN, db.str, "", "", 0);
  buffer[--length]= 0;
  return {buffer, length};
}

// Escape db -> ./dbname. Caller allocates memory.
inline void mariadb_to_monokey(LEX_CSTRING db, LEX_STRING &key)
{
  DBUG_ASSERT(key.length >= FN_REFLEN);

  key.length= build_table_filename(key.str, FN_REFLEN, db.str, "", "", 0);
  key.str[--key.length]= 0; // Trim tailing '/'
}

// Unescape ./dbname -> db
inline LEX_CSTRING monokey_to_mariadb(MEM_ROOT *mem_root, std::string_view key)
{
  DBUG_ASSERT(key.length() < FN_REFLEN);

  constexpr char prefix[]= {'.', '/'}; // Trim heading "./"
  char *buffer= (char *) alloc_root(mem_root, FN_REFLEN + 1);
  size_t length= filename_to_tablename(key.data() + sizeof(prefix), buffer,
                                       FN_REFLEN, true);
  return {buffer, length};
}

// Unescape ./dbname/tablename -> (db, table). Caller allocates memory.
inline void monokey_to_marianame(std::string_view key, LEX_STRING &db,
                                 LEX_STRING &name)
{
  DBUG_ASSERT(db.length >= FN_REFLEN && name.length >= FN_REFLEN);
  bzero(db.str, db.length);
  bzero(name.str, name.length);

  constexpr char prefix[]= {'.', '/'}; // Trim heading "./"

  char dirp[FN_REFLEN + 1]= {0};
  size_t len_dirp= 0;
  dirname_part(dirp, key.data(), &len_dirp);
  dirp[len_dirp - 1]= 0; // Trim tailing '/'

  db.length=
      filename_to_tablename(dirp + sizeof(prefix), db.str, db.length, true);
  name.length= filename_to_tablename(key.data() + len_dirp, name.str,
                                     name.length, true);
}

// Unescape ./dbname/tablename -> (db, table).
inline void monokey_to_marianame(MEM_ROOT *mem_root, LEX_CSTRING key,
                                 LEX_CSTRING &db, LEX_CSTRING &name)
{
  char *buf_db= (char *) alloc_root(mem_root, FN_REFLEN + 1);
  char *buf_tb= (char *) alloc_root(mem_root, FN_REFLEN + 1);
  LEX_STRING db_name{buf_db, FN_REFLEN};
  LEX_STRING tb_name{buf_tb, FN_REFLEN};
  monokey_to_marianame(std::string_view(key.str, key.length), db_name,
                       tb_name);
  db.str= db_name.str;
  db.length= db_name.length;
  name.str= tb_name.str;
  name.length= tb_name.length;
}
} // namespace MyEloq
