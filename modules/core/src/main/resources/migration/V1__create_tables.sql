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
   "credential_key" SERIAL PRIMARY KEY,
   "name" varchar(100) NOT NULL,
   "type" varchar(100) NOT NULL,
   "value" jsonb NOT NULL,
   "valid_from" timestamp NOT NULL DEFAULT NOW(),
   "valid_to" timestamp
);

CREATE UNIQUE INDEX "credentials_name_type" ON "credentials" ("name","type") WHERE ("valid_to" is null);
CREATE INDEX "steprun_job_run_id" ON "steprun" ("job_run_id");
