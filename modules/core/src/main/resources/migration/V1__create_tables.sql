CREATE TABLE "job" (
    "job_name" varchar(100) PRIMARY KEY,
    "job_description" varchar(100) NOT NULL,
    "schedule" varchar(100) NOT NULL,
    "failed" bigint NOT NULL,
    "success" bigint NOT NULL,
    "is_active" boolean NOT NULL
);

CREATE TABLE "jobrun" (
    "job_run_id" varchar(100) PRIMARY KEY,
    "job_name" text NOT NULL,
    "properties" text NOT NULL,
    "state" text NOT NULL,
    "start_time" varchar(100) NOT NULL,
    "elapsed_time" varchar(100) NOT NULL,
    "job_type" varchar(100) NOT NULL,
    "is_master" varchar(100) NOT NULL,
    "inserted_at" timestamp NOT NULL DEFAULT (current_timestamp)
);

CREATE TABLE "steprun" (
    "job_run_id" varchar(100) NOT NULL,
    "step_name" varchar(100) NOT NULL,
    "properties" text NOT NULL,
    "state" text NOT NULL,
    "start_time" varchar(100) NOT NULL,
    "elapsed_time" varchar(100) NOT NULL,
    "step_type" varchar(100) NOT NULL,
    "step_run_id" varchar(100) NOT NULL,
    "inserted_at" timestamp NOT NULL DEFAULT (current_timestamp)
);

CREATE TABLE "userinfo" (
    "user_name" varchar(100) PRIMARY KEY,
    "password" varchar(100) NOT NULL,
    "user_active" varchar(100) NOT NULL,
    "user_role" varchar(100) NOT NULL
);

CREATE TABLE "credentials" (
    "name" varchar(100),
    "type" varchar(100) NOT NULL,
    "value" text NOT NULL,
    "valid_from" TIMESTAMP(0) NOT NULL DEFAULT NOW(),
    "valid_to" TIMESTAMP(0),
    "credential_key" SERIAL PRIMARY KEY
);

CREATE INDEX "steprun_job_run_id" ON "steprun" ("job_run_id");
