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
#include "eloq_errors.h"

const char *eloq_error_messages[]= {
    "Eloq: failed to commit transaction. try restarting transaction. %s",
    "Eloq: failed to create table. %s",
    "Eloq: failed to drop table. %s",
    "Eloq: failed to start transaction at eloq engine, Txservice is "
    "not ready yet.",
    "Eloq: does not support '%s' isolation level.",
    "Eloq: failed to get an auto increment value.",
    "Eloq: failed to read. '%s'",
    "Eloq: failed to run test. '%s'",
    "Eloq: failed to insert. '%s'",
    "Eloq: failed to update. '%s'",
    "Eloq: failed to create catalog, the given name contains illegal "
    "characters",
    "Eloq: failed to create index. ",
    "Eloq: failed to drop index. ",
    "Eloq: failed due to read/write conflict. try restarting "
    "transaction.",
    "Eloq: failed due to write/write conflict. try restarting "
    "transaction.",
    "Eloq: failed due to record was updated by other trancations, try "
    "restarting transaction.",
    "Eloq: data storage is not available.",
    "Eloq: error. try restarting transaction. '%s'",
    "Eloq: warning. '%s'",
    "Eloq: internal force error due to timeout. '%s'",
};

const char **eloq_get_error_messages(int nr) { return eloq_error_messages; }
