# ==== Purpose ====
#
# The same as 'connection $mono_connection_name', but it can also
# prints the connection name.  The connection is printed if $mono_debug
# is set, or if conn_connection.inc is not called between two
# invocations of begin_include_file.inc/end_include_file.inc.
# Otherwise the connection name is not printed.
#
#
# ==== Usage ====
#
# --let $mono_connection_name= master
# [--let $mono_debug= 1]
# --source include/mono_connection.inc
#
# Parameters:
#   $mono_connection_name
#     Name of the connection to connect to.
#
#   $mono_debug
#     By default, the connection name is printed only when this file
#     is sourced from a top-level test script. If $mono_debug is set,
#     the connection name is also printed whenever auxiliary files
#     like mono_init.inc change connection.


if (!$mono_connection_name)
{
  --die ERROR IN TEST: you must set $mono_connection_name before sourcing mono_connection.inc
}

# This is the same as "if (!$_mono_include_file_depth || $mono_debug)",
# but the mysqltest language doesn't have boolean operations.

if (!$_include_file_depth)
{
  --echo [connection $mono_connection_name]
}
if ($_include_file_depth)
{
  if ($mono_debug)
  {
    --echo [connection $mono_connection_name]
  }
}
disable_connect_log;
--connection $mono_connection_name
enable_connect_log;
--let $mono_connection_name=
