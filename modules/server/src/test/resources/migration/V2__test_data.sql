INSERT INTO userinfo (user_name,password,user_active,user_role)
VALUES ('admin','$2a$10$gABYeKWB2W0nI.zGCoovD.7emHUlHq1flgxWjqAIowdLMWkzYlIOy','true','admin');

INSERT INTO job(job_name,job_description,schedule,failed,success,is_active)
VALUES ('Job1','','',0,0,'t');
INSERT INTO job(job_name,job_description,schedule,failed,success,is_active)
VALUES ('Job2','','',0,0,'f');
INSERT INTO job(job_name,job_description,schedule,failed,success,is_active)
VALUES ('Job3','','',0,0,'t');
INSERT INTO job(job_name,job_description,schedule,failed,success,is_active)
VALUES ('Job6','','',0,0,'t');
INSERT INTO job(job_name,job_description,schedule,failed,success,is_active)
VALUES ('Job7','','',0,0,'t');
INSERT INTO job(job_name,job_description,schedule,failed,success,is_active)
VALUES ('Job8','','',0,0,'t');
INSERT INTO job(job_name,job_description,schedule,failed,success,is_active)
VALUES ('Job9','','',0,0,'t');

INSERT INTO jobrun (job_run_id,job_name,properties,state,elapsed_time,job_type,is_master,inserted_at)
VALUES ('a27a7415-57b2-4b53-8f9b-5254e847a301','EtlJobDownload','{}','pass','','GenericEtlJob','true',1234567);
INSERT INTO jobrun (job_run_id,job_name,properties,state,elapsed_time,job_type,is_master,inserted_at)
VALUES ('a27a7415-57b2-4b53-8f9b-5254e847a302','EtlJobSpr','{}','pass','','GenericEtlJob','true',1234567);

INSERT INTO steprun (job_run_id,step_name,properties,state,elapsed_time,step_type,step_run_id,inserted_at)
VALUES ('a27a7415-57b2-4b53-8f9b-5254e847a301','download_spr','{}','pass','1.6 mins','GenericEtlStep','123',1234567);

INSERT INTO credential(name, type, value, valid_from)
VALUES('AWS','JDBC','{}','2021-07-21 12:37:19.298812');