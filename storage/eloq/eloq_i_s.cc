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
#include "my_global.h"
#include "field.h"
#include "sql_show.h"
#include "log.h"

#include "tx_service.h"

#include "tx_service/include/store/data_store_handler.h"
#include "eloq_catalog_name.h"
#include "ha_eloq_macro.h"

using namespace mysql::Show;
using namespace MyEloq;
using namespace txservice;

extern std::unique_ptr<TxService> tx_service;
extern store::DataStoreHandler *storage_hd;

extern std::pair<const std::function<void()> *, const std::function<void()> *>
thd_get_coro_functors(const THD *thd);

static struct st_mysql_information_schema eloq_i_s_info= {
    MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION,
};

enum ELOQ_TEMP_TABLE_INFO_FIELD
{
  NAME= 0,
};

static ST_FIELD_INFO eloq_i_s_temp_table_info_fields_info[]= {
    Column("NAME", Varchar(NAME_LEN + 1), NOT_NULL),
    CEnd(),
};

/**
 * @brief Function to populate INFORMATION_SCHEMA.ELOQ_TEMP_TABLE_INFO
 * table.
 * @return 0 on success, HA_ERR_INTERNAL_ERROR on error.
 */
static int eloq_i_s_temp_table_info_fill_table(THD *thd, TABLE_LIST *tables,
                                               Item *cond)
{
  DBUG_ENTER_FUNC();

  int ret= 0;

  auto [yield_func, resume_func]= thd_get_coro_functors(thd);
  std::vector<std::string> table_names;
  bool ok= storage_hd->DiscoverAllTableNames(
      txservice::TableEngine::EloqSql, table_names, yield_func, resume_func);
  if (ok)
  {
    for (const std::string &table_name : table_names)
    {
      if (is_tmp_table(table_name))
      {
        std::string name= basename(table_name);
        // Call filename_to_tablename is not necessary here.
        // Since temporary table name doesn't contain special character.
        tables->table->field[ELOQ_TEMP_TABLE_INFO_FIELD::NAME]->store(
            name.c_str(), name.length(), mysql::system_charset_info);
        if (mysql::schema_table_store_record(thd, tables->table))
        {
          ret= HA_ERR_INTERNAL_ERROR;
          sql_print_error("Table store record failed.");
          break;
        }
      }
    }
  }
  else
  {
    ret= HA_ERR_INTERNAL_ERROR;
    sql_print_error("Discover all table names failed.");
  }

  DBUG_RETURN(ret);
}

/* Initialize the information_schema.temp_table_info virtual table */
static int eloq_i_s_temp_table_info_init(void *const p)
{
  mysql::ST_SCHEMA_TABLE *schema;

  DBUG_ENTER_FUNC();
  DBUG_ASSERT(p != nullptr);

  schema= reinterpret_cast<mysql::ST_SCHEMA_TABLE *>(p);
  schema->fields_info= eloq_i_s_temp_table_info_fields_info;
  schema->fill_table= eloq_i_s_temp_table_info_fill_table;

  DBUG_RETURN(0);
}

struct st_maria_plugin eloq_i_s_temp_table_info= {
    MYSQL_INFORMATION_SCHEMA_PLUGIN,
    &eloq_i_s_info,
    "ELOQ_TEMP_TABLE_INFO",
    "Liang Jeff Chen, ELOQDB",
    "Eloq Temp Table Stats",
    PLUGIN_LICENSE_GPL,
    eloq_i_s_temp_table_info_init,       /* Plugin Init */
    nullptr,                             /* Plugin Deinit */
    0x0001,                              /* version number (0.1) */
    nullptr,                             /* status variable */
    nullptr,                             /* system variable */
    "0.1",                               /* string version */
    MariaDB_PLUGIN_MATURITY_EXPERIMENTAL /* maturity */
};