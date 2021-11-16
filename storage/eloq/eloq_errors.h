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

/*
  ELOQDB specific error codes. NB! Please make sure that you will update
  HA_ERR_ELOQ_LAST when adding new ones.  Also update the strings in
  eloq_error_messages to include any new error messages.
*/
#define HA_ERR_ELOQ_FIRST (HA_ERR_LAST + 1)
#define HA_ERR_ELOQ_COMMIT_FAILED (HA_ERR_LAST + 1)
#define HA_ERR_ELOQ_CREATE_TABLE_FAILED (HA_ERR_LAST + 2)
#define HA_ERR_ELOQ_DROP_TABLE_FAILED (HA_ERR_LAST + 3)
#define HA_ERR_ELOQ_START_TRANSACTION_FAILED (HA_ERR_LAST + 4)
#define HA_ERR_ELOQ_ISO_LEVEL_UNSUPPORT_ERROR (HA_ERR_LAST + 5)
#define HA_ERR_ELOQ_AUTO_INCREMENT_FAILED (HA_ERR_LAST + 6)
#define HA_ERR_ELOQ_READ_ERROR (HA_ERR_LAST + 7)
#define HA_ERR_ELOQ_TEST_FAILED (HA_ERR_LAST + 8)
#define HA_ERR_ELOQ_INSERT_FAILED (HA_ERR_LAST + 9)
#define HA_ERR_ELOQ_UPDATE_FAILED (HA_ERR_LAST + 10)
#define HA_ERR_ELOQ_CATALOG_NAME_ERROR (HA_ERR_LAST + 11)
#define HA_ERR_ELOQ_CREATE_INDEX_FAILED (HA_ERR_LAST + 12)
#define HA_ERR_ELOQ_DROP_INDEX_FAILED (HA_ERR_LAST + 13)
#define HA_ERR_ELOQ_RW_CONFLICT (HA_ERR_LAST + 14)
#define HA_ERR_ELOQ_WW_CONFLICT (HA_ERR_LAST + 15)
#define HA_ERR_ELOQ_RECORD_WAS_UPDATED (HA_ERR_LAST + 16)
#define HA_ERR_ELOQ_DATA_STORE_ERROR (HA_ERR_LAST + 17)
#define HA_ERR_ELOQ_DEFAULT_ERROR (HA_ERR_LAST + 18)
#define HA_ERR_ELOQ_TRANSACTION_BREAK (HA_ERR_LAST + 19)
#define HA_ERR_ELOQ_LAST HA_ERR_ELOQ_TRANSACTION_BREAK

extern const char *eloq_error_messages[];
extern const char **eloq_get_error_messages(int nr);
