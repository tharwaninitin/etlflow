CREATE TABLE cronjobrun(job_name varchar(100), schedule varchar(100));
CREATE TABLE jobrun(job_run_id varchar(100),job_name varchar(100), description varchar(100), properties varchar(8000), state varchar(100),inserted_at bigint);
CREATE TABLE step(job_run_id varchar(100), step_name varchar(100), properties varchar(8000), state varchar(100), elapsed_time varchar(100));
CREATE TABLE userinfo(user_name varchar(100), password varchar(100), user_active varchar(100));
CREATE TABLE userauthtokens(token varchar(100));