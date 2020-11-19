insert into userinfo (user_name,password,user_active,user_role) values ('admin','admin','true','admin');
insert into cronjob (job_name,schedule,failed,success,is_active) values ('EtlJobDownload','',0,0,'t');
insert into cronjob (job_name,schedule,failed,success,is_active) values ('EtlJob4','',0,0,'t');
insert into jobrun (job_run_id,job_name,description,properties,state,start_time,job_type,is_master) values ('a27a7415-57b2-4b53-8f9b-5254e847a301','EtlJobDownload','sample_job','','pass','2020-08-10 10:35:01','GenericEtlJob','true');
insert into jobrun (job_run_id,job_name,description,properties,state,start_time,job_type,is_master) values ('a27a7415-57b2-4b53-8f9b-5254e847a302','EtlJobSpr','sample_job','','pass','2020-08-10 10:35:01','GenericEtlJob','true');
insert into steprun (job_run_id,step_name,properties,state,start_time,inserted_at,elapsed_time,step_type,step_run_id) values ('a27a7415-57b2-4b53-8f9b-5254e847a301','download_spr','','pass','2020-08-10 10:35:01',1234,'1.6 mins','GenericEtlStep','123');
insert into credentials (name,type,value) values ('flyway','jdbc','{
                                                                     "url" : "jdbc:postgresql://localhost:5432/postgres",
                                                                     "user" : "postgres",
                                                                     "password" : "swap123",
                                                                     "driver" : "org.postgresql.Driver"
                                                                   }');