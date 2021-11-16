# ==== Purpose ====
#
# Create a connection to a given numbered server.
#
# This script is normally used internally by mono_init.inc,
# but it can also be used in test cases that need to
# create more connections or re-create connections after disconnect.
#
#
# ==== Usage ====
#
# --let $mono_connection_name= <connection_name>
# --let $mono_server_number= <server_number>
# [--let $mono_debug= 1]
# --source include/mono_connect.inc
#
# Parameters:
#   $mono_connection_name
#     The name of the connection to create.
#
#   $mono_server_number
#     The number of the server to connect to.
#
#   $mono_debug
#     See include/mono_init.inc


--let $include_filename= mono_connect.inc [creating $mono_connection_name]
--source include/begin_include_file.inc

if (!$mono_server_number)
{
  --die ERROR IN TEST: You must set $mono_server_number before sourcing include/mono_connect.inc
}
if (!$mono_connection_name)
{
  --die ERROR IN TEST: You must set $mono_connection_name before sourcing include/mono_connect.inc
}

# Get port number
--let $_mono_port= \$MONO_PORT_$mono_server_number
if (!$_mono_port)
{
  --echo Bug in test case: '\$MONO_PORT_$mono_server_number' not initialized. Check the test's .cnf file.
  --die Not all MONO_PORT_* environment variables are setup correctly.
}

# Create connection.
if ($mono_debug)
{
  --echo connect ($mono_connection_name,127.0.0.1,root,,test,$_mono_port,)
}
disable_connect_log;
--connect ($mono_connection_name,127.0.0.1,root,,test,$_mono_port,)
enable_connect_log;


--let $include_filename= mono_connect.inc
--source include/end_include_file.inc
