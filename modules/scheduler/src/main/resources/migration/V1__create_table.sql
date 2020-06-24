CREATE TABLE cronjob(job_name varchar(100) PRIMARY KEY, schedule varchar(100), failed bigint, success bigint);
CREATE TABLE jobrun(job_run_id varchar(100) PRIMARY KEY,
    job_name varchar(100),
    description varchar(100),
    properties text,
    state text,
    inserted_at bigint
    );
CREATE TABLE steprun(job_run_id varchar(100),
    step_name varchar(100),
    properties text,
    state text,
    elapsed_time varchar(100)
    );
CREATE TABLE userinfo(user_name varchar(100) PRIMARY KEY, password varchar(100), user_active varchar(100));
CREATE TABLE userauthtokens(token varchar(100));