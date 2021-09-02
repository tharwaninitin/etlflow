CREATE TABLE "job" (
    "job_name" varchar(100) PRIMARY KEY,
    "job_description" varchar(100) NOT NULL,
    "schedule" varchar(100) NOT NULL,
    "failed" bigint NOT NULL,
    "success" bigint NOT NULL,
    "is_active" boolean NOT NULL,
    "last_run_time" bigint
);

CREATE TABLE "jobrun" (
    "job_run_id" varchar(100) PRIMARY KEY,
    "job_name" text NOT NULL,
    "properties" jsonb NOT NULL,
    "state" text NOT NULL,
    "elapsed_time" varchar(100) NOT NULL,
    "job_type" varchar(100) NOT NULL,
    "is_master" varchar(100) NOT NULL,
    "inserted_at" bigint NOT NULL
);

CREATE TABLE "steprun" (
    "job_run_id" varchar(100) NOT NULL,
    "step_name" varchar(100) NOT NULL,
    "properties" jsonb NOT NULL,
    "state" text NOT NULL,
    "elapsed_time" varchar(100) NOT NULL,
    "step_type" varchar(100) NOT NULL,
    "step_run_id" varchar(100) NOT NULL,
    "inserted_at" bigint NOT NULL
);

CREATE TABLE "userinfo" (
    "user_name" varchar(100) PRIMARY KEY,
    "password" varchar(100) NOT NULL,
    "user_active" varchar(100) NOT NULL,
    "user_role" varchar(100) NOT NULL
);

CREATE TABLE "credential" (
   "id" SERIAL PRIMARY KEY,
   "name" varchar(100) NOT NULL,
   "type" varchar(100) NOT NULL,
   "value" jsonb NOT NULL,
   "valid_from" timestamp NOT NULL DEFAULT NOW(),
   "valid_to" timestamp
);

CREATE UNIQUE INDEX "credential_name_type" ON "credential" ("name","type") WHERE ("valid_to" is null);
CREATE INDEX "steprun_job_run_id" ON "steprun" ("job_run_id");
