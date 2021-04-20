INSERT INTO userinfo (user_name,password,user_active,user_role)
VALUES ('admin','$2a$10$gABYeKWB2W0nI.zGCoovD.7emHUlHq1flgxWjqAIowdLMWkzYlIOy','true','admin');

INSERT INTO job(job_name,job_description,schedule,failed,success,is_active)
VALUES ('Job1','','',0,0,'t');
INSERT INTO job(job_name,job_description,schedule,failed,success,is_active)
VALUES ('Job2','','',0,0,'f');

INSERT INTO jobrun (job_run_id,job_name,properties,state,start_time,elapsed_time,job_type,is_master)
VALUES ('a27a7415-57b2-4b53-8f9b-5254e847a301','EtlJobDownload','{}','pass','2020-08-10 10:35:01','','GenericEtlJob','true');
INSERT INTO jobrun (job_run_id,job_name,properties,state,start_time,elapsed_time,job_type,is_master)
VALUES ('a27a7415-57b2-4b53-8f9b-5254e847a302','EtlJobSpr','{}','pass','2020-08-10 10:35:01','','GenericEtlJob','true');

INSERT INTO steprun (job_run_id,step_name,properties,state,start_time,elapsed_time,step_type,step_run_id)
VALUES ('a27a7415-57b2-4b53-8f9b-5254e847a301','download_spr','{}','pass','2020-08-10 10:35:01','1.6 mins','GenericEtlStep','123');