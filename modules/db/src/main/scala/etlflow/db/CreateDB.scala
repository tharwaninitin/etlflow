package etlflow.db

import zio.ZIO

object CreateDB {
  def apply(reset: Boolean = false): ZIO[DBEnv, Throwable, Unit] = {
    def createTable(name: String) =
      if (reset)
        s"""
           |DROP TABLE IF EXISTS $name;
           |CREATE TABLE $name""".stripMargin
      else s"CREATE TABLE IF NOT EXISTS $name"
    def createUniqueIndex(name: String) =
      if (reset)
        s"""
           |DROP INDEX IF EXISTS $name;
           |CREATE UNIQUE INDEX $name""".stripMargin
      else s"CREATE UNIQUE INDEX IF NOT EXISTS $name"
    def createIndex(name: String) =
      if (reset)
        s"""
           |DROP INDEX IF EXISTS $name;
           |CREATE INDEX $name""".stripMargin
      else s"CREATE INDEX IF NOT EXISTS $name"
    val sql = s"""
                 |${createTable("job")} (
                 |    "job_name" varchar(100) PRIMARY KEY,
                 |    "job_description" varchar(100) NOT NULL,
                 |    "schedule" varchar(100) NOT NULL,
                 |    "failed" bigint NOT NULL,
                 |    "success" bigint NOT NULL,
                 |    "is_active" boolean NOT NULL,
                 |    "last_run_time" bigint
                 |);
                 |
                 |${createTable("jobrun")} (
                 |    "job_run_id" varchar(100) PRIMARY KEY,
                 |    "job_name" text NOT NULL,
                 |    "properties" jsonb NOT NULL,
                 |    "status" text NOT NULL,
                 |    "elapsed_time" varchar(100) NOT NULL,
                 |    "job_type" varchar(100) NOT NULL,
                 |    "is_master" varchar(100) NOT NULL,
                 |    "inserted_at" bigint NOT NULL
                 |);
                 |
                 |${createTable("steprun")} (
                 |    "step_run_id" varchar(100) PRIMARY KEY,
                 |    "job_run_id" varchar(100),
                 |    "step_name" text NOT NULL,
                 |    "properties" jsonb NOT NULL,
                 |    "status" text NOT NULL,
                 |    "elapsed_time" varchar(100) NOT NULL,
                 |    "step_type" varchar(100) NOT NULL,
                 |    "inserted_at" bigint NOT NULL
                 |);
                 |
                 |${createTable("userinfo")} (
                 |    "user_name" varchar(100) PRIMARY KEY,
                 |    "password" varchar(100) NOT NULL,
                 |    "user_active" varchar(100) NOT NULL,
                 |    "user_role" varchar(100) NOT NULL
                 |);
                 |
                 |${createTable("credential")} (
                 |   "id" SERIAL PRIMARY KEY,
                 |   "name" varchar(100) NOT NULL,
                 |   "type" varchar(100) NOT NULL,
                 |   "value" jsonb NOT NULL,
                 |   "valid_from" timestamp NOT NULL DEFAULT NOW(),
                 |   "valid_to" timestamp
                 |);
                 |
                 |${createUniqueIndex("credential_name_type")} ON "credential" ("name","type") WHERE ("valid_to" is null);
                 |${createIndex("steprun_job_run_id")} ON "steprun" ("job_run_id");
                 |""".stripMargin
    DBApi.executeQuery(sql)
  }
}
