CREATE TABLE job(job_name varchar(100) PRIMARY KEY,job_description varchar(100),schedule varchar(100), failed bigint, success bigint, is_active boolean);
CREATE TABLE jobrun(job_run_id varchar(100) PRIMARY KEY,
                    job_name text,
                    properties text,
                    state text,
                    start_time varchar(100),
                    elapsed_time varchar(100),
                    job_type varchar(100),
                    is_master varchar(100),
                    inserted_at timestamp DEFAULT current_timestamp
);
CREATE TABLE steprun(job_run_id varchar(100),
                     step_name varchar(100),
                     properties text,
                     state text,
                     start_time varchar(100),
                     elapsed_time varchar(100),
                     step_type varchar(100),
                     step_run_id varchar(100),
                     inserted_at timestamp DEFAULT current_timestamp
);
CREATE INDEX steprun_job_run_id on steprun (job_run_id);

CREATE TABLE userinfo(user_name varchar(100) PRIMARY KEY, password varchar(100), user_active varchar(100),user_role varchar(100));

CREATE TABLE credentials(name varchar(100) PRIMARY KEY,type varchar(100), value text);