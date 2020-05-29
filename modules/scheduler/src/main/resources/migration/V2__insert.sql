insert into userinfo (user_name,password,user_active) values ('admin','admin','true');
insert into cronjob (job_name,schedule) values ('EtlJob1PARQUETtoORCtoBQLocalWith2Steps','0 */15 * * * ?');
-- insert into cronjob (job_name,schedule) values ('EtlJob2CSVtoPARQUETtoBQLocalWith3Steps','0 */1 * * * ?');