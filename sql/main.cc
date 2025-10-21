/* Copyright (c) 2009 Sun Microsystems, Inc.
   Use is subject to license terms.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1335  USA */

/* 
  main() for mysqld.
  Calls mysqld_main() entry point exported by sql library.
  On Windows, might do some service handling.
*/
#include <gflags/gflags.h>
#include <data_substrate.h>

// Only MySQL-related CLI flag we accept
DEFINE_string(eloqsql_config, "", 
  "Path to MySQL configuration file. All MySQL options must be in this config file.");

DEFINE_string(data_substrate_config, "", 
  "Path to data substrate configuration file.");

#ifdef WITH_GLOG
#include "glog_error_logging.h"
#endif /* WITH_GLOG */

#ifdef _WIN32
/* Windows main function, service handling, calls mysqld_main */
extern int mysqld_win_main(int argc, char **argv);
#else
extern int mysqld_main(int argc, char **argv);
#endif


int main(int argc, char **argv)
{
  // Initialize gflags - will error on unknown flags
  gflags::SetUsageMessage(
    "mysqld [gflags options]\n\n"
    "MySQL system variables must be in config file specified by --eloqsql_config.\n"
    "Data substrate flags can be passed on command line.\n"
    "Use --help to see all available flags.");
  
  
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // DataSubstrate::InitializeGlobal(FLAGS_data_substrate_config);


#ifdef WITH_GLOG
  InitGoogleLogging(argv);
#endif /* WITH_GLOG */

#ifdef _WIN32
  return mysqld_win_main(argc, argv);
#else
  return mysqld_main(argc, argv);
#endif
}
