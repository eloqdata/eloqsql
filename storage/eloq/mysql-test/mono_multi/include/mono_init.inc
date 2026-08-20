# ==== Purpose ====
#
# This file will create connections for all nodes.
#
# By default, this script does the following:
#  - Creates the connections serv_1, serv_2, ..., serv_N, as
#    well as extra connections serv_1_1, server_2_1, ...,
#    server_N_1. server_1 and server_1_1 are connections to
#    the same server.
#  - Sets the connection to server_1 before exiting.
#
# ==== Usage ====
#
# 1. In this suite path, the file my.cnf has configured a cluster
#    with 3 nodes, If you need other config, you can change it.
#
# 2. Execute the following near the top of the test:
#
#    [--let $mono_server_count= 3]
#    [--let $mono_debug= 1]
#    --source include/mono_init.inc
#
#    Parameters:
#
#    $mono_server_count
#      The number of servers to configure.  If this is not set, 
#      the default value is 3.
#
#    $mono_debug
#      By default, this script only outputs a static text that says
#      that mono_init.inc was invoked. If $mono_debug is set, additional
#      debug info is printed. The debug info may be nondeterministic,
#      so no test case should be checked in with $mono_debug set.
#
# ==== Side effects ====
#

--let $include_filename= mono_init.inc
--source include/begin_include_file.inc


if ($mono_debug)
{
  --echo ---- Check input ----
  --echo MONO_PORT_1='$MONO_PORT_1' MONO_PORT_2='$MONO_PORT_2' MONO_PORT_3='$MONO_PORT_3'
}

# Check that $mono_server_count is set
if (!$mono_server_count)
{
  --let $mono_server_count= 3
}

if ($mono_debug)
{
  --echo ---- Setup connections and reset each server ----
}

if (!$mono_debug)
{
  --disable_query_log
}

# Create two connections to each server;
--let $_mono_server= $mono_server_count
--let $_mono_one= _1
while ($_mono_server)
{
  # Connect.
  --let $mono_server_number= $_mono_server
  --let $mono_connection_name= serv_$_mono_server
  --source include/mono_connect.inc
  --let $mono_connection_name= serv_$_mono_server$_mono_one
  --source include/mono_connect.inc
  --dec $_mono_server
}

# Signal that initialization is done and all connections created.
--let $mono_inited= 1

--let $mono_connection_name= serv_1
--source include/mono_connection.inc


--let $include_filename= mono_init.inc
--source include/end_include_file.inc
