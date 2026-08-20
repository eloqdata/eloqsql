echo # switching back from mysql.user to mysql.global_priv;
disable_query_log;
drop table mysql.user;
rename table mysql.user_bak to mysql.user;
#FIXME: rename no support
#rename table mysql.global_priv_bak to mysql.global_priv;
create table mysql.global_priv as select * from mysql.global_priv_bak;
drop table mysql.global_priv_bak;
flush privileges;
enable_query_log;
