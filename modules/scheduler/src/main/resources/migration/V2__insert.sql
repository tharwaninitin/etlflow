insert into userinfo (user_name,password,user_active) values ('admin','admin','true');
insert into cronjob (job_name,schedule,failed,success) values ('EtlJob1PARQUETtoORCtoBQLocalWith2Steps','0 */15 * * * ?',0,0);
insert into cronjob (job_name,schedule,failed,success) values ('EtlJob2CSVtoPARQUETtoBQLocalWith3Steps','0 */1 * * * ?',0,0);
insert into cronjob (job_name,schedule,failed,success) values ('EtlJob4BQtoBQ','0 */1 * * * ?',0,0);