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
#define MYSQL_SERVER 1

#include "my_global.h"
#include "key.h"
#include "sql_base.h"

#include "tx_service/include/store/data_store_handler.h"

#include "ha_eloq.h"
#include "ha_eloq_macro.h"
#include "eloq_catalog_name.h"
#include "eloq_errors.h"
#include "store_handler/kv_store.h"

using namespace MyEloq;
using namespace txservice;

extern my_bool opt_bootstrap; // Defined in Mariadb context.

static std::string mysql_mono_view{"./mysql/mono_view"};
static TableName VIEW_TABLE_NAME{mysql_mono_view, TableType::Primary,
                                 txservice::TableEngine::EloqSql};
static LEX_CSTRING VIEW_TABLE_NAME_CS{STRING_WITH_LEN("mono_view")};

extern std::pair<const std::function<void()> *, const std::function<void()> *>
thd_get_coro_functors(const THD *thd);
enum
{
  MYSQL_MONO_VIEW_FIELD_DB= 0,
  MYSQL_MONO_VIEW_FIELD_NAME,
  MYSQL_MONO_VIEW_FIELD_DATA,
};

static int eloq_fetch_view_bootstrap(THD *thd, LEX_CSTRING db,
                                     LEX_CSTRING view,
                                     LEX_CSTRING &frm_binary);
static int eloq_fetch_view_normal(THD *thd, LEX_CSTRING db, LEX_CSTRING view,
                                  LEX_CSTRING &frm_binary);
static int eloq_fetch_view_error(THD *thd, LEX_CSTRING db, LEX_CSTRING view,
                                 LEX_CSTRING &frm_binary);
static auto eloq_fetch_view_impl= eloq_fetch_view_bootstrap;

extern "C"
{
  int eloq_create_database(THD *thd, LEX_CSTRING db,
                           Schema_specification_st *create);
  int eloq_update_database(THD *thd, LEX_CSTRING db,
                           Schema_specification_st *create);
  int eloq_drop_database(THD *thd, LEX_CSTRING db);
  int eloq_exist_database(THD *thd, LEX_CSTRING db, bool &exist);
  int eloq_discover_database(THD *thd, LEX_CSTRING db,
                             Schema_specification_st *create);
  int eloq_discover_database_names(THD *thd,
                                   Dynamic_array<LEX_CSTRING *> *dbnames);
  int eloq_discover_database_names_wild(THD *thd, Discovered_table_list &tl);
  int eloq_discover_view_names(THD *thd, LEX_CSTRING db,
                               std::vector<LEX_CSTRING> &view_names);
  int eloq_upsert_view_p(THD *thd, LEX_CSTRING key, const LEX_CSTRING *type,
                         uchar *base, File_option *parameters);
  int eloq_upsert_view(THD *thd, LEX_CSTRING db, LEX_CSTRING view,
                       const LEX_CSTRING *type, uchar *base,
                       File_option *parameters);
  int eloq_drop_view_p(THD *thd, LEX_CSTRING key);
  int eloq_drop_view(THD *thd, LEX_CSTRING db, LEX_CSTRING view);
  int eloq_drop_views(THD *thd, LEX_CSTRING db);
  int eloq_exist_view_p(THD *thd, LEX_CSTRING key, bool &exist);
  int eloq_exist_view(THD *thd, LEX_CSTRING db, LEX_CSTRING view, bool &exist);
  int eloq_fetch_view_p(THD *thd, LEX_CSTRING key, LEX_CSTRING &frm_binary);
  int eloq_fetch_view(THD *thd, LEX_CSTRING db, LEX_CSTRING view,
                      LEX_CSTRING &frm_binary);
  int eloq_check_view_p(THD *thd, LEX_CSTRING key, LEX_CSTRING old_frm_binary,
                        bool &eq);
  int eloq_check_view(THD *thd, LEX_CSTRING db, LEX_CSTRING view,
                      LEX_CSTRING old_frm_binary, bool &eq);
  int eloq_exist_frm_p(THD *thd, LEX_CSTRING key, bool &exist);
  int eloq_exist_frm(THD *thd, LEX_CSTRING db, LEX_CSTRING frm_name,
                     bool &exist);
  int eloq_fetch_frm_p(THD *thd, LEX_CSTRING key, LEX_CSTRING &frm_binary);
  int eloq_fetch_frm(THD *thd, LEX_CSTRING db, LEX_CSTRING frm_name,
                     LEX_CSTRING &frm_binary);
  int eloq_node_id(THD *thd);
} // extern "C"

/**
 * Override my_b_write, write_escaped_string, write_parameter
 * with taking std::stringstream as output.
 */

static inline int my_b_write(std::stringstream &ss, const uchar *Buffer,
                             size_t Count)
{
  MEM_CHECK_DEFINED(Buffer, Count);
  ss << std::string_view(reinterpret_cast<const char *>(Buffer), Count);
  return 0;
}

static my_bool write_escaped_string(std::stringstream &file, LEX_STRING *val_s)
{
  char *eos= val_s->str + val_s->length;
  char *ptr= val_s->str;

  for (; ptr < eos; ptr++)
  {
    /*
      Should be in sync with read_escaped_string() and
      parse_quoted_escaped_string()
    */
    switch (*ptr)
    {
    case '\\': // escape character
      if (my_b_write(file, (const uchar *) STRING_WITH_LEN("\\\\")))
        return TRUE;
      break;
    case '\n': // parameter value delimiter
      if (my_b_write(file, (const uchar *) STRING_WITH_LEN("\\n")))
        return TRUE;
      break;
    case '\0': // problem for some string processing utilities
      if (my_b_write(file, (const uchar *) STRING_WITH_LEN("\\0")))
        return TRUE;
      break;
    case 26: // problem for windows utilities (Ctrl-Z)
      if (my_b_write(file, (const uchar *) STRING_WITH_LEN("\\z")))
        return TRUE;
      break;
    case '\'': // list of string delimiter
      if (my_b_write(file, (const uchar *) STRING_WITH_LEN("\\\'")))
        return TRUE;
      break;
    default:
      if (my_b_write(file, (const uchar *) ptr, 1))
        return TRUE;
    }
  }
  return FALSE;
}

// Copy from parse_file.cc, instead of include that file.
// It is a sql level private function.
static ulonglong view_algo_to_frm(ulonglong val)
{
  switch (val)
  {
  case VIEW_ALGORITHM_UNDEFINED:
    return VIEW_ALGORITHM_UNDEFINED_FRM;
  case VIEW_ALGORITHM_MERGE:
    return VIEW_ALGORITHM_MERGE_FRM;
  case VIEW_ALGORITHM_TMPTABLE:
    return VIEW_ALGORITHM_TMPTABLE_FRM;
  }
  DBUG_ASSERT(0); /* Should never happen */
  return VIEW_ALGORITHM_UNDEFINED;
}

static my_bool write_parameter(std::stringstream &file, const uchar *base,
                               File_option *parameter)
{
  char num_buf[20]; // buffer for numeric operations
  // string for numeric operations
  String num(num_buf, sizeof(num_buf), &my_charset_bin);
  DBUG_ENTER("write_parameter");

  switch (parameter->type)
  {
  case FILE_OPTIONS_STRING: {
    LEX_STRING *val_s= (LEX_STRING *) (base + parameter->offset);
    if (my_b_write(file, (const uchar *) val_s->str, val_s->length))
      DBUG_RETURN(TRUE);
    break;
  }
  case FILE_OPTIONS_ESTRING: {
    if (write_escaped_string(file, (LEX_STRING *) (base + parameter->offset)))
      DBUG_RETURN(TRUE);
    break;
  }
  case FILE_OPTIONS_ULONGLONG:
  case FILE_OPTIONS_VIEW_ALGO: {
    ulonglong val= *(ulonglong *) (base + parameter->offset);

    if (parameter->type == FILE_OPTIONS_VIEW_ALGO)
      val= view_algo_to_frm(val);

    num.set(val, &my_charset_bin);
    if (my_b_write(file, (const uchar *) num.ptr(), num.length()))
      DBUG_RETURN(TRUE);
    break;
  }
  case FILE_OPTIONS_TIMESTAMP: {
    /* string have to be allocated already */
    LEX_STRING *val_s= (LEX_STRING *) (base + parameter->offset);
    time_t tm= my_time(0);

    get_date(val_s->str, GETDATE_DATE_TIME | GETDATE_GMT | GETDATE_FIXEDLENGTH,
             tm);
    val_s->length= PARSE_FILE_TIMESTAMPLENGTH;
    if (my_b_write(file, (const uchar *) val_s->str,
                   PARSE_FILE_TIMESTAMPLENGTH))
      DBUG_RETURN(TRUE);
    break;
  }
  case FILE_OPTIONS_STRLIST: {
    List_iterator_fast<LEX_STRING> it(
        *((List<LEX_STRING> *) (base + parameter->offset)));
    bool first= 1;
    LEX_STRING *str;
    while ((str= it++))
    {
      // We need ' ' after string to detect list continuation
      if ((!first && my_b_write(file, (const uchar *) STRING_WITH_LEN(" "))) ||
          my_b_write(file, (const uchar *) STRING_WITH_LEN("\'")) ||
          write_escaped_string(file, str) ||
          my_b_write(file, (const uchar *) STRING_WITH_LEN("\'")))
      {
        DBUG_RETURN(TRUE);
      }
      first= 0;
    }
    break;
  }
  case FILE_OPTIONS_ULLLIST: {
    List_iterator_fast<ulonglong> it(
        *((List<ulonglong> *) (base + parameter->offset)));
    bool first= 1;
    ulonglong *val;
    while ((val= it++))
    {
      num.set(*val, &my_charset_bin);
      // We need ' ' after string to detect list continuation
      if ((!first && my_b_write(file, (const uchar *) STRING_WITH_LEN(" "))) ||
          my_b_write(file, (const uchar *) num.ptr(), num.length()))
      {
        DBUG_RETURN(TRUE);
      }
      first= 0;
    }
    break;
  }
  default:
    DBUG_ASSERT(0); // never should happened
  }
  DBUG_RETURN(FALSE);
}

static int build_db_opt_binary(THD *thd, Schema_specification_st *create,
                               LEX_CSTRING &opt_binary)
{
  // Below code is based on write_db_opt in sql_db.cc

  size_t size_buf= 256 + DATABASE_COMMENT_MAXLEN;
  char *buf= (char *) thd->alloc(size_buf);

  if (create->schema_comment)
  {
    if (mysql::validate_comment_length(
            thd, create->schema_comment, DATABASE_COMMENT_MAXLEN,
            ER_TOO_LONG_DATABASE_COMMENT, thd->lex->name.str))
    {
      return ER_TOO_LONG_DATABASE_COMMENT;
    }
  }

  if (!create->default_table_charset)
    create->default_table_charset= thd->variables.collation_server;

  ulong length;
  length= (ulong) (strxnmov(buf, size_buf - 1, "default-character-set=",
                            create->default_table_charset->cs_name.str,
                            "\ndefault-collation=",
                            create->default_table_charset->coll_name.str, "\n",
                            NullS) -
                   buf);

  if (create->schema_comment)
    length= (ulong) (strxnmov(buf + length, sizeof(buf) - 1 - length,
                              "comment=", create->schema_comment->str, "\n",
                              NullS) -
                     buf);

  opt_binary.str= buf;
  opt_binary.length= length;
  return 0;
}

void parse_opt_binary(THD *thd, LEX_CSTRING db, const std::string &opt_binary,
                      Schema_specification_st *create)
{
  // Below code is based on load_db_opt in sql_db.cc

  myf utf8_flag= thd->get_utf8_flag();

  std::string str;
  std::istringstream ss(opt_binary);
  while (getline(ss, str))
  {
    char *buf= str.data();
    size_t nbytes= str.size();

    char *pos= buf + nbytes;
    /* Remove end space and control characters */
    while (pos > buf && !my_isgraph(&my_charset_latin1, pos[-1]))
      pos--;
    *pos= 0;
    if ((pos= strchr(buf, '=')))
    {
      if (!strncmp(buf, "default-character-set", (pos - buf)))
      {
        /*
           Try character set name, and if it fails
           try collation name, probably it's an old
           4.1.0 db.opt file, which didn't have
           separate default-character-set and
           default-collation commands.
        */
        if (!(create->default_table_charset= get_charset_by_csname(
                  pos + 1, MY_CS_PRIMARY, MYF(utf8_flag))) &&
            !(create->default_table_charset=
                  get_charset_by_name(pos + 1, MYF(utf8_flag))))
        {
          sql_print_error("Error while loading database options: '%s':",
                          db.str);
          sql_print_error(ER_THD(thd, ER_UNKNOWN_CHARACTER_SET), pos + 1);
          create->default_table_charset= default_charset_info;
        }
      }
      else if (!strncmp(buf, "default-collation", (pos - buf)))
      {
        if (!(create->default_table_charset=
                  get_charset_by_name(pos + 1, MYF(utf8_flag))))
        {
          sql_print_error("Error while loading database options: '%s':",
                          db.str);
          sql_print_error(ER_THD(thd, ER_UNKNOWN_COLLATION), pos + 1);
          create->default_table_charset= default_charset_info;
        }
      }
      else if (!strncmp(buf, "comment", (pos - buf)))
        create->schema_comment=
            thd->make_clex_string(pos + 1, strlen(pos + 1));
    }

    str.clear();
  }
}

static std::string build_view_frm_binary(const LEX_CSTRING *type, uchar *base,
                                         File_option *parameters)
{
  std::stringstream ss;

  // write header (file signature)
  my_b_write(ss, (const uchar *) STRING_WITH_LEN("TYPE="));
  my_b_write(ss, (const uchar *) type->str, type->length);
  my_b_write(ss, (const uchar *) STRING_WITH_LEN("\n"));

  // write parameters to temporary file
  for (auto &param= parameters; param->name.str; param++)
  {
    my_b_write(ss, (const uchar *) param->name.str, param->name.length);
    my_b_write(ss, (const uchar *) STRING_WITH_LEN("="));
    write_parameter(ss, base, param);
    my_b_write(ss, (const uchar *) STRING_WITH_LEN("\n"));
  }

  return ss.str();
}

int eloq_create_database(THD *thd, LEX_CSTRING db,
                         Schema_specification_st *create)
{
  DBUG_ENTER_FUNC();

  std::string_view key= mariadb_to_monokey(thd->mem_root, db);

  LEX_CSTRING opt_binary;
  int ret= build_db_opt_binary(thd, create, opt_binary);
  if (ret)
  {
    DBUG_RETURN(ret);
  }

  auto [yield_func, resume_func]= thd_get_coro_functors(thd);
  bool ok= storage_hd->UpsertDatabase(key, {opt_binary.str, opt_binary.length},
                                      yield_func, resume_func);
  if (!ok)
  {
    my_printf_error(HA_ERR_INTERNAL_ERROR, "Eloq upsert database '%s' failed",
                    MYF(0), key.data());
    ret= HA_ERR_INTERNAL_ERROR;
  }

  DBUG_RETURN(ret);
}

int eloq_update_database(THD *thd, LEX_CSTRING db,
                         Schema_specification_st *create)
{
  DBUG_ENTER_FUNC();

  std::string_view key= mariadb_to_monokey(thd->mem_root, db);

  /* Use existing values of schema_comment and charset for
      ALTER DATABASE queries */
  Schema_specification_st tmp;
  tmp.init();
  int ret= eloq_discover_database(thd, db, &tmp);
  if (ret)
  {
    DBUG_RETURN(ret);
  }

  if (!create->schema_comment)
    create->schema_comment= tmp.schema_comment;

  if (!create->default_table_charset)
    create->default_table_charset= tmp.default_table_charset;

  LEX_CSTRING opt_binary;
  ret= build_db_opt_binary(thd, create, opt_binary);
  if (ret)
  {
    DBUG_RETURN(ret);
  }

  auto [yield_func, resume_func]= thd_get_coro_functors(thd);
  bool ok= storage_hd->UpsertDatabase(key, {opt_binary.str, opt_binary.length},
                                      yield_func, resume_func);
  if (!ok)
  {
    my_printf_error(HA_ERR_INTERNAL_ERROR, "Eloq upsert database '%s' failed",
                    MYF(0), key.data());
    ret= HA_ERR_INTERNAL_ERROR;
  }

  DBUG_RETURN(ret);
}

int eloq_drop_database(THD *thd, LEX_CSTRING db)
{
  DBUG_ENTER_FUNC();

  std::string_view key= mariadb_to_monokey(thd->mem_root, db);

  int ret= eloq_drop_views(thd, db);
  if (ret)
  {
    DBUG_RETURN(ret);
  }

  auto [yield_func, resume_func]= thd_get_coro_functors(thd);
  bool ok= storage_hd->DropDatabase(key, yield_func, resume_func);
  if (!ok)
  {
    my_printf_error(HA_ERR_INTERNAL_ERROR, "Eloq drop database '%s' failed",
                    MYF(0), key.data());
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }
  else
  {
    DBUG_RETURN(0);
  }
}

int eloq_exist_database(THD *thd, LEX_CSTRING db, bool &exist)
{

  DBUG_ENTER_FUNC();

  std::string_view key= mariadb_to_monokey(thd->mem_root, db);

  auto [yield_func, resume_func]= thd_get_coro_functors(thd);
  std::string data;
  bool ok=
      storage_hd->FetchDatabase(key, data, exist, yield_func, resume_func);
  if (!ok)
  {
    my_printf_error(HA_ERR_INTERNAL_ERROR, "Eloq fetch database '%s' failed",
                    MYF(0), key.data());
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }
  else
  {
    DBUG_RETURN(0);
  }
}

int eloq_discover_database(THD *thd, LEX_CSTRING db,
                           Schema_specification_st *create)
{
  DBUG_ENTER_FUNC();

  bzero((char *) create, sizeof(*create));
  create->default_table_charset= thd->variables.collation_server;

  std::string_view key= mariadb_to_monokey(thd->mem_root, db);

  auto [yield_func, resume_func]= thd_get_coro_functors(thd);
  bool exists= false;
  std::string data;
  bool ok=
      storage_hd->FetchDatabase(key, data, exists, yield_func, resume_func);
  if (!ok)
  {
    my_printf_error(HA_ERR_INTERNAL_ERROR, "Eloq fetch database '%s' failed",
                    MYF(0), key.data());
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  if (!exists)
  {
    DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
  }

  parse_opt_binary(thd, db, data, create);

  DBUG_RETURN(0);
}

int eloq_discover_database_names(THD *thd,
                                 Dynamic_array<LEX_CSTRING *> *dbnames)
{
  DBUG_ENTER_FUNC();

  auto [yield_func, resume_func]= thd_get_coro_functors(thd);
  std::vector<std::string> names;
  bool ok= storage_hd->FetchAllDatabase(names, yield_func, resume_func);
  if (!ok)
  {
    my_printf_error(HA_ERR_INTERNAL_ERROR, "Eloq fetch all databases failed",
                    MYF(0));
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  for (const std::string &name : names)
  {
    LEX_CSTRING db= monokey_to_mariadb(thd->mem_root, name);
    LEX_CSTRING *db_ptr= (LEX_CSTRING *) thd->alloc(sizeof(LEX_CSTRING));
    lex_string_set3(db_ptr, db.str, db.length);
    dbnames->append_val(db_ptr);
  }

  DBUG_RETURN(0);
}

int eloq_discover_database_names_wild(THD *thd, Discovered_table_list &tl)
{
  DBUG_ENTER_FUNC();

  auto [yield_func, resume_func]= thd_get_coro_functors(thd);
  std::vector<std::string> names;
  bool ok= storage_hd->FetchAllDatabase(names, yield_func, resume_func);
  if (!ok)
  {
    my_printf_error(HA_ERR_INTERNAL_ERROR, "Eloq fetch all databases failed",
                    MYF(0));
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  for (const std::string &name : names)
  {
    LEX_CSTRING db= monokey_to_mariadb(thd->mem_root, name);
    tl.add_table(db.str, db.length);
  }

  DBUG_RETURN(0);
}

static int load_view_fetch_func()
{
  int ret= 0;

  bool exists= false;
  uint64_t version_ts= 0;
  std::string schema_image;
  bool ok= storage_hd->FetchTable(VIEW_TABLE_NAME, schema_image, exists,
                                  version_ts);
  if (ok)
  {
    if (exists)
    {
      eloq_fetch_view_impl= eloq_fetch_view_normal;
      ret= 0;
    }
    else
    {
      eloq_fetch_view_impl= eloq_fetch_view_bootstrap;
      ret= HA_ERR_KEY_NOT_FOUND;
    }
  }
  else
  {
    eloq_fetch_view_impl= eloq_fetch_view_error;
    ret= HA_ERR_INTERNAL_ERROR;
  }
  return ret;
}

static int eloq_fetch_view_bootstrap(THD *thd, LEX_CSTRING db,
                                     LEX_CSTRING view, LEX_CSTRING &frm_binary)
{
  DBUG_ENTER_FUNC();
  int err= load_view_fetch_func();
  if (err == 0)
  {
    DBUG_RETURN(eloq_fetch_view_normal(thd, db, view, frm_binary));
  }
  else if (err == HA_ERR_KEY_NOT_FOUND)
  {
    DBUG_RETURN(opt_bootstrap ? HA_ERR_KEY_NOT_FOUND : HA_ERR_INTERNAL_ERROR);
  }
  else
  {
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }
}

static int eloq_fetch_view_normal(THD *thd, LEX_CSTRING db, LEX_CSTRING view,
                                  LEX_CSTRING &frm_binary)
{
  DBUG_ENTER_FUNC();

  int ret= 0;

  if (lex_string_eq(&db, &MYSQL_SCHEMA_NAME) &&
      lex_string_eq(&view, &VIEW_TABLE_NAME_CS))
  {
    // Fast path for system table 'view' itself.
    //
    // Dead recursive call may happen if we remove this branch. For example:
    // To access table t1, we need open system table 'view' first.
    // To access table 'view', we need to open system table 'view' again.
    // And again...
    ret= HA_ERR_KEY_NOT_FOUND;
    DBUG_RETURN(ret);
  }

  start_new_trans new_trans(thd);

  TABLE_LIST table_list;
  TABLE *table;
  table_list.init_one_table(&MYSQL_SCHEMA_NAME, &VIEW_TABLE_NAME_CS, NULL,
                            TL_READ);
  open_system_tables_for_read(thd, &table_list);
  table= table_list.table;
  do
  {
    if (!table)
    {
      my_printf_error(HA_ERR_INTERNAL_ERROR,
                      "Eloq open system table '%s' failed", MYF(0),
                      VIEW_TABLE_NAME_CS.str);
      ret= HA_ERR_INTERNAL_ERROR;
      break;
    }

    table->field[MYSQL_MONO_VIEW_FIELD_DB]->store(db,
                                                  mysql::system_charset_info);
    table->field[MYSQL_MONO_VIEW_FIELD_NAME]->store(
        view, mysql::system_charset_info);
    uchar key[MAX_KEY_LENGTH]; // db, name, optional key length type
    key_copy(key, table->record[0], table->key_info,
             table->key_info->key_length);

    ret= table->file->ha_index_read_idx_map(table->record[0], 0, key,
                                            HA_WHOLE_KEY, HA_READ_KEY_EXACT);
    // Decrease key read count stats to match mysql behavior. Otherwise using
    // LIMIT clause when selecting from a view will return incorrect number of
    // rows.
    static_cast<ha_eloq *>(table->file)
        ->decrement_statistics(&SSV::ha_read_key_count);
    if (ret)
    {
      if (ret != HA_ERR_KEY_NOT_FOUND)
      {
        my_printf_error(HA_ERR_INTERNAL_ERROR,
                        "Eloq read view './%s/%s' failed", MYF(0), db.str,
                        view.str);
        ret= HA_ERR_INTERNAL_ERROR;
      }
      break;
    }

    table->field[MYSQL_MONO_VIEW_FIELD_DATA]->val_str_nopad(thd->mem_root,
                                                            &frm_binary);
  } while (0);

  if (table)
  {
    if (thd->commit_whole_transaction_and_close_tables())
    {
      my_printf_error(
          HA_ERR_INTERNAL_ERROR,
          "Eloq commit transaction for fetch view './%s/%s' failed", MYF(0),
          db.str, view.str);
      ret= HA_ERR_INTERNAL_ERROR;
    }
  }
  new_trans.restore_old_transaction();

  DBUG_RETURN(ret);
}

static int eloq_fetch_view_error(THD *thd, LEX_CSTRING db, LEX_CSTRING view,
                                 LEX_CSTRING &frm_binary)
{
  DBUG_ENTER_FUNC();
  my_printf_error(HA_ERR_INTERNAL_ERROR,
                  "Eloq failed to load system table '%s'", MYF(0),
                  VIEW_TABLE_NAME.StringView().data());
  DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
}

int eloq_discover_view_names(THD *thd, LEX_CSTRING db,
                             std::vector<LEX_CSTRING> &view_names)
{
  DBUG_ENTER_FUNC();

  int ret= 0;

  bool do_ha_index_end= false;

  start_new_trans new_trans(thd);

  TABLE_LIST table_list;
  TABLE *table;
  table_list.init_one_table(&MYSQL_SCHEMA_NAME, &VIEW_TABLE_NAME_CS, NULL,
                            TL_READ);
  open_system_tables_for_read(thd, &table_list);
  table= table_list.table;
  do
  {
    if (!table)
    {
      my_printf_error(HA_ERR_INTERNAL_ERROR,
                      "Eloq open system table '%s' failed", MYF(0),
                      VIEW_TABLE_NAME_CS.str);
      ret= HA_ERR_INTERNAL_ERROR;
      break;
    }

    uint key_len= 0;
    uchar keybuf[MAX_KEY_LENGTH]= {0};
    table->field[MYSQL_MONO_VIEW_FIELD_DB]->store(db,
                                                  mysql::system_charset_info);
    key_len= table->key_info->key_part[0].store_length;
    table->field[MYSQL_PROC_FIELD_DB]->get_key_image(keybuf, key_len,
                                                     Field::itRAW);
    ret= table->file->ha_index_init(0, 1);
    if (ret)
    {
      my_printf_error(HA_ERR_INTERNAL_ERROR,
                      "Eloq discover views of database '%s' failed", MYF(0),
                      db.str);
      ret= HA_ERR_INTERNAL_ERROR;
      break;
    }
    do_ha_index_end= true;

    ret= table->file->ha_index_read_map(table->record[0], keybuf,
                                        (key_part_map) 1, HA_READ_KEY_EXACT);
    if (ret)
    {
      if (ret != HA_ERR_END_OF_FILE)
      {
        my_printf_error(HA_ERR_INTERNAL_ERROR,
                        "Eloq discover views of database '%s' failed", MYF(0),
                        db.str);
        ret= HA_ERR_INTERNAL_ERROR;
      }
      else
      {
        ret= 0;
      }
      break;
    }

    bool stop= false;
    do
    {
      LEX_CSTRING view_name;
      table->field[MYSQL_MONO_VIEW_FIELD_NAME]->val_str_nopad(thd->mem_root,
                                                              &view_name);
      view_names.emplace_back(view_name);

      ret= table->file->ha_index_next_same(table->record[0], keybuf, key_len);
      if (ret)
      {
        stop= true;
        if (ret != HA_ERR_END_OF_FILE)
        {
          my_printf_error(HA_ERR_INTERNAL_ERROR,
                          "Eloq discover views of database '%s' failed",
                          MYF(0), db.str);
          ret= HA_ERR_INTERNAL_ERROR;
          break;
        }
        else
        {
          ret= 0;
        }
      }
    } while (!stop);
  } while (0);

  if (do_ha_index_end)
  {
    table->file->ha_index_end();
  }

  if (table)
  {
    if (thd->commit_whole_transaction_and_close_tables())
    {
      my_printf_error(HA_ERR_INTERNAL_ERROR,
                      "Eloq commit transaction for discovering views of "
                      "database '%s' failed",
                      MYF(0), db.str);
      ret= HA_ERR_INTERNAL_ERROR;
    }
  }
  new_trans.restore_old_transaction();

  DBUG_RETURN(ret);
}

int eloq_upsert_view_p(THD *thd, LEX_CSTRING key, const LEX_CSTRING *type,
                       uchar *base, File_option *parameters)
{
  LEX_CSTRING db, view;
  monokey_to_marianame(thd->mem_root, key, db, view);
  return eloq_upsert_view(thd, db, view, type, base, parameters);
}

int eloq_upsert_view(THD *thd, LEX_CSTRING db, LEX_CSTRING view,
                     const LEX_CSTRING *type, uchar *base,
                     File_option *parameters)
{
  DBUG_ENTER_FUNC();

  int ret= 0;

  std::string frm_binary= build_view_frm_binary(type, base, parameters);
  LEX_CSTRING data{frm_binary.data(), frm_binary.size()};

  start_new_trans new_trans(thd);

  MDL_savepoint mdl_savepoint= thd->mdl_context.mdl_savepoint();

  TABLE_LIST table_list;
  TABLE *table;
  table_list.init_one_table(&MYSQL_SCHEMA_NAME, &VIEW_TABLE_NAME_CS, NULL,
                            TL_WRITE);
  table= open_system_table_for_update(thd, &table_list);
  do
  {
    if (!table)
    {
      my_printf_error(HA_ERR_INTERNAL_ERROR,
                      "Eloq open system table '%s' failed", MYF(0),
                      VIEW_TABLE_NAME_CS.str);
      ret= HA_ERR_INTERNAL_ERROR;
      break;
    }

    if (frm_binary.size() >
        table->field[MYSQL_MONO_VIEW_FIELD_DATA]->field_length)
    {
      my_error(ER_DATA_TOO_LONG, MYF(0),
               table->field[MYSQL_MONO_VIEW_FIELD_DATA]->field_name.str,
               MYSQL_MONO_VIEW_FIELD_DATA);
      ret= ER_DATA_TOO_LONG;
      break;
    }

    table->field[MYSQL_MONO_VIEW_FIELD_DB]->store(db,
                                                  mysql::system_charset_info);
    table->field[MYSQL_MONO_VIEW_FIELD_NAME]->store(
        view, mysql::system_charset_info);
    uchar key[MAX_KEY_LENGTH]; // db, name, optional key length type
    key_copy(key, table->record[0], table->key_info,
             table->key_info->key_length);

    ret= table->file->ha_index_read_idx_map(table->record[0], 0, key,
                                            HA_WHOLE_KEY, HA_READ_KEY_EXACT);
    if (ret)
    {
      if (ret != HA_ERR_KEY_NOT_FOUND)
      {
        my_printf_error(HA_ERR_INTERNAL_ERROR,
                        "Eloq read view './%s/%s' failed", MYF(0), db.str,
                        view.str);
        ret= HA_ERR_INTERNAL_ERROR;
      }
      else
      {
        table->field[MYSQL_MONO_VIEW_FIELD_DATA]->store(
            data, mysql::system_charset_info);
        ret= table->file->ha_write_row(table->record[0]);
        if (ret)
        {
          my_printf_error(HA_ERR_INTERNAL_ERROR,
                          "Eloq write view './%s/%s' failed", MYF(0), db.str,
                          view.str);
          ret= HA_ERR_INTERNAL_ERROR;
        }
      }
    }
    else
    {
      store_record(table, record[1]);
      table->field[MYSQL_MONO_VIEW_FIELD_DATA]->store(
          data, mysql::system_charset_info);

      ret= table->file->ha_update_row(table->record[1], table->record[0]);
      if (ret)
      {
        my_printf_error(HA_ERR_INTERNAL_ERROR,
                        "Eloq update view './%s/%s' failed", MYF(0), db.str,
                        view.str);
        ret= HA_ERR_INTERNAL_ERROR;
      }
    }
  } while (0);

  if (table)
  {
    if (thd->commit_whole_transaction_and_close_tables())
    {
      my_printf_error(
          HA_ERR_INTERNAL_ERROR,
          "Eloq commit transaction for upsert view './%s/%s' failed", MYF(0),
          db.str, view.str);
      ret= HA_ERR_INTERNAL_ERROR;
    }
  }
  thd->mdl_context.rollback_to_savepoint(mdl_savepoint);
  new_trans.restore_old_transaction();

  DBUG_RETURN(ret);
}

int eloq_drop_view_p(THD *thd, LEX_CSTRING key)
{
  LEX_CSTRING db, view;
  monokey_to_marianame(thd->mem_root, key, db, view);
  return eloq_drop_view(thd, db, view);
}

int eloq_drop_view(THD *thd, LEX_CSTRING db, LEX_CSTRING view)
{
  DBUG_ENTER_FUNC();

  int ret= 0;

  start_new_trans new_trans(thd);

  MDL_savepoint mdl_savepoint= thd->mdl_context.mdl_savepoint();

  TABLE_LIST table_list;
  TABLE *table;
  table_list.init_one_table(&MYSQL_SCHEMA_NAME, &VIEW_TABLE_NAME_CS, NULL,
                            TL_WRITE);
  table= open_system_table_for_update(thd, &table_list);
  do
  {
    if (!table)
    {
      my_printf_error(HA_ERR_INTERNAL_ERROR,
                      "Eloq open system table '%s' failed", MYF(0),
                      VIEW_TABLE_NAME_CS.str);
      ret= HA_ERR_INTERNAL_ERROR;
      break;
    }

    table->field[MYSQL_MONO_VIEW_FIELD_DB]->store(db,
                                                  mysql::system_charset_info);
    table->field[MYSQL_MONO_VIEW_FIELD_NAME]->store(
        view, mysql::system_charset_info);
    uchar key[MAX_KEY_LENGTH]; // db, name, optional key length type
    key_copy(key, table->record[0], table->key_info,
             table->key_info->key_length);

    ret= table->file->ha_index_read_idx_map(table->record[0], 0, key,
                                            HA_WHOLE_KEY, HA_READ_KEY_EXACT);
    if (ret)
    {
      if (table->status == STATUS_NOT_FOUND)
      {
        ret= HA_ERR_KEY_NOT_FOUND;
      }
      else
      {
        my_printf_error(HA_ERR_INTERNAL_ERROR,
                        "Eloq read view './%s/%s' failed", MYF(0), db.str,
                        view.str);
        ret= HA_ERR_INTERNAL_ERROR;
      }
      break;
    }

    ret= table->file->ha_delete_row(table->record[0]);
    if (ret)
    {
      my_printf_error(HA_ERR_INTERNAL_ERROR, "Eloq drop view './%s/%s' failed",
                      MYF(0), db.str, view.str);
      ret= HA_ERR_INTERNAL_ERROR;
    }
  } while (0);

  if (table)
  {
    if (thd->commit_whole_transaction_and_close_tables())
    {
      my_printf_error(HA_ERR_INTERNAL_ERROR,
                      "Eloq commit transaction for drop view './%s/%s' failed",
                      MYF(0), db.str, view.str);
      ret= HA_ERR_INTERNAL_ERROR;
    }
  }
  thd->mdl_context.rollback_to_savepoint(mdl_savepoint);
  new_trans.restore_old_transaction();

  DBUG_RETURN(ret);
}

int eloq_drop_views(THD *thd, LEX_CSTRING db)
{
  DBUG_ENTER_FUNC();

  int ret= 0;

  bool do_ha_index_end= false;

  start_new_trans new_trans(thd);

  MDL_savepoint mdl_savepoint= thd->mdl_context.mdl_savepoint();

  TABLE_LIST table_list;
  TABLE *table;
  table_list.init_one_table(&MYSQL_SCHEMA_NAME, &VIEW_TABLE_NAME_CS, NULL,
                            TL_WRITE);
  table= open_system_table_for_update(thd, &table_list);
  table= table_list.table;
  do
  {
    if (!table)
    {
      my_printf_error(HA_ERR_INTERNAL_ERROR,
                      "Eloq open system table '%s' failed", MYF(0),
                      VIEW_TABLE_NAME_CS.str);
      ret= HA_ERR_INTERNAL_ERROR;
      break;
    }

    uint key_len= 0;
    uchar keybuf[MAX_KEY_LENGTH]= {0};
    table->field[MYSQL_MONO_VIEW_FIELD_DB]->store(db,
                                                  mysql::system_charset_info);
    key_len= table->key_info->key_part[0].store_length;
    table->field[MYSQL_PROC_FIELD_DB]->get_key_image(keybuf, key_len,
                                                     Field::itRAW);
    ret= table->file->ha_index_init(0, 1);
    if (ret)
    {
      my_printf_error(HA_ERR_INTERNAL_ERROR,
                      "Eloq discover views of database '%s' failed", MYF(0),
                      db.str);
      ret= HA_ERR_INTERNAL_ERROR;
      break;
    }
    do_ha_index_end= true;

    ret= table->file->ha_index_read_map(table->record[0], keybuf,
                                        (key_part_map) 1, HA_READ_KEY_EXACT);
    if (ret)
    {
      if (ret != HA_ERR_END_OF_FILE)
      {
        my_printf_error(HA_ERR_INTERNAL_ERROR,
                        "Eloq discover views of database '%s' failed", MYF(0),
                        db.str);
        ret= HA_ERR_INTERNAL_ERROR;
      }
      else
      {
        ret= 0;
      }
      break;
    }

    bool stop= false;
    do
    {
      LEX_CSTRING view;
      table->field[MYSQL_MONO_VIEW_FIELD_NAME]->val_str_nopad(thd->mem_root,
                                                              &view);
      ret= table->file->ha_delete_row(table->record[0]);
      if (ret)
      {
        stop= true;
        my_printf_error(HA_ERR_INTERNAL_ERROR,
                        "Eloq drop view './%s/%s' failed", MYF(0), db.str,
                        view.str);
        ret= HA_ERR_INTERNAL_ERROR;
        break;
      }

      ret= table->file->ha_index_next_same(table->record[0], keybuf, key_len);
      if (ret)
      {
        stop= true;
        if (ret != HA_ERR_END_OF_FILE)
        {
          my_printf_error(HA_ERR_INTERNAL_ERROR,
                          "Eloq discover views of database '%s' failed",
                          MYF(0), db.str);
          ret= HA_ERR_INTERNAL_ERROR;
          break;
        }
        else
        {
          ret= 0;
        }
      }
    } while (!stop);
  } while (0);

  if (do_ha_index_end)
  {
    table->file->ha_index_end();
  }

  if (table)
  {
    if (thd->commit_whole_transaction_and_close_tables())
    {
      my_printf_error(HA_ERR_INTERNAL_ERROR,
                      "Eloq commit transaction for drop views in '%s' failed",
                      MYF(0), db.str);
      ret= HA_ERR_INTERNAL_ERROR;
    }
  }
  thd->mdl_context.rollback_to_savepoint(mdl_savepoint);
  new_trans.restore_old_transaction();

  DBUG_RETURN(ret);
}

int eloq_exist_view_p(THD *thd, LEX_CSTRING key, bool &exist)
{
  LEX_CSTRING db, view;
  monokey_to_marianame(thd->mem_root, key, db, view);
  return eloq_exist_view(thd, db, view, exist);
}

int eloq_exist_view(THD *thd, LEX_CSTRING db, LEX_CSTRING view, bool &exist)
{
  DBUG_ENTER_FUNC();

  LEX_CSTRING frm_binary;
  int ret= eloq_fetch_view(thd, db, view, frm_binary);
  if (ret)
  {
    if (ret == HA_ERR_KEY_NOT_FOUND)
    {
      exist= false;
    }
  }
  else
  {
    exist= true;
  }

  DBUG_RETURN(ret);
}

int eloq_fetch_view_p(THD *thd, LEX_CSTRING key, LEX_CSTRING &frm_binary)
{
  LEX_CSTRING db, view;
  monokey_to_marianame(thd->mem_root, key, db, view);
  return eloq_fetch_view(thd, db, view, frm_binary);
}

int eloq_fetch_view(THD *thd, LEX_CSTRING db, LEX_CSTRING view,
                    LEX_CSTRING &frm_binary)
{
  return eloq_fetch_view_impl(thd, db, view, frm_binary);
}

int eloq_check_view_p(THD *thd, LEX_CSTRING key, LEX_CSTRING old_frm_binary,
                      bool &eq)
{
  LEX_CSTRING db, view;
  monokey_to_marianame(thd->mem_root, key, db, view);
  return eloq_check_view(thd, db, view, old_frm_binary, eq);
}

int eloq_check_view(THD *thd, LEX_CSTRING db, LEX_CSTRING view,
                    LEX_CSTRING old_frm_binary, bool &eq)
{
  DBUG_ENTER_FUNC();

  LEX_CSTRING new_frm_binary;
  int ret= eloq_fetch_view(thd, db, view, new_frm_binary);
  if (ret)
  {
    DBUG_RETURN(ret);
  }

  std::string_view new_view= {new_frm_binary.str, new_frm_binary.length};
  std::string_view old_view= {old_frm_binary.str, old_frm_binary.length};

  // Check whether the above two frm are same.
  // We don't compare them by call operator= directly, because:
  // 1. The old frm binary data has trimmed the header `TYPE=VIEW`;
  // 2. String compare may be slow.
  // Instead, we compare them by comparing their md5 field.

  std::string_view::size_type npos= new_view.find("\nmd5=");
  std::string_view::size_type opos= old_view.find("\nmd5=");
  std::string_view::size_type nsize= new_view.size();
  std::string_view::size_type osize= old_view.size();
  npos+= 5;
  opos+= 5;
  DBUG_ASSERT(npos < nsize && opos < osize);
  while (new_view[npos] == old_view[opos] &&
         (new_view[npos] != '\n' && old_view[opos] != '\n'))
  {
    npos+= 1;
    opos+= 1;
  }
  DBUG_ASSERT(npos < nsize && opos < osize);
  (void) nsize, (void) osize;
  eq= (new_view[npos] == '\n') && (old_view[opos] == '\n');

  DBUG_RETURN(0);
}

int eloq_exist_frm_p(THD *thd, LEX_CSTRING key, bool &exist)
{
  LEX_CSTRING db, name;
  monokey_to_marianame(thd->mem_root, key, db, name);
  return eloq_exist_frm(thd, db, name, exist);
}

int eloq_exist_frm(THD *thd, LEX_CSTRING db, LEX_CSTRING frm_name, bool &exist)
{
  DBUG_ENTER_FUNC();

  LEX_CSTRING frm_binary;
  int ret= eloq_fetch_frm(thd, db, frm_name, frm_binary);
  if (ret)
  {
    if (ret == HA_ERR_KEY_NOT_FOUND)
    {
      exist= false;
      ret= 0;
    }
  }
  else
  {
    exist= true;
  }

  DBUG_RETURN(ret);
}

int eloq_fetch_frm_p(THD *thd, LEX_CSTRING key, LEX_CSTRING &frm_binary)
{
  LEX_CSTRING db, name;
  monokey_to_marianame(thd->mem_root, key, db, name);
  return eloq_fetch_frm(thd, db, name, frm_binary);
}

int eloq_fetch_frm(THD *thd, LEX_CSTRING db, LEX_CSTRING frm_name,
                   LEX_CSTRING &frm_binary)
{
  DBUG_ENTER_FUNC();

  std::string_view key= marianame_to_monokey(thd->mem_root, db, frm_name);

  bool exists= false;
  std::string data;
  uint64_t commit_ts;
  auto [yield_func, resume_func]= thd_get_coro_functors(thd);
  bool ok= storage_hd->FetchTable(
      TableName(key, TableType::Catalog, TableEngine::EloqSql), data, exists,
      commit_ts, yield_func, resume_func);
  if (!ok)
  {
    my_printf_error(HA_ERR_INTERNAL_ERROR, "Eloq fetch frm './%s/%s' failed",
                    MYF(0), db.str, frm_name.str);
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }
  if (exists)
  {
    std::string frm, kv_info, key_schemas_ts;
    EloqDS::DeserializeSchemaImage(data, frm, kv_info, key_schemas_ts);
    frm_binary= thd->strmake_lex_cstring(frm.data(), frm.size());
    DBUG_RETURN(0);
  }

  DBUG_RETURN(eloq_fetch_view(thd, db, frm_name, frm_binary));
}

int eloq_node_id(THD *thd) { return static_cast<int>(node_id); }
