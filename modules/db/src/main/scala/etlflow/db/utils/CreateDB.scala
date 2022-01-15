package etlflow.db.utils

import etlflow.db.{DBApi, DBEnv}
import etlflow.utils.ApplicationLogger
import zio.ZIO

object CreateDB extends ApplicationLogger {
  def apply(reset: Boolean = false): ZIO[DBEnv, Throwable, Unit] = {
    def createTable(name: String) =
      if (reset)
        s"""
           |DROP TABLE IF EXISTS $name CASCADE;
           |CREATE TABLE $name""".stripMargin
      else s"CREATE TABLE IF NOT EXISTS $name"
    def createUniqueIndex(name: String) =
      if (reset)
        s"""
           |DROP INDEX IF EXISTS $name;
           |CREATE UNIQUE INDEX $name""".stripMargin
      else s"CREATE UNIQUE INDEX IF NOT EXISTS $name"

    val tbl1 = s"""
                 |${createTable("job")} (
                 |    job_name varchar(100) PRIMARY KEY,
                 |    job_description varchar(100) NOT NULL,
                 |    schedule varchar(100) NOT NULL,
                 |    failed bigint NOT NULL,
                 |    success bigint NOT NULL,
                 |    is_active boolean NOT NULL,
                 |    last_run_time bigint
                 |);""".stripMargin
    val tbl2 = s"""
                 |${createTable("jobrun")} (
                 |    job_run_id varchar(100) PRIMARY KEY,
                 |    job_name text NOT NULL,
                 |    properties json NOT NULL,
                 |    status text NOT NULL,
                 |    elapsed_time varchar(100) NOT NULL,
                 |    job_type varchar(100) NOT NULL,
                 |    is_master varchar(100) NOT NULL,
                 |    inserted_at bigint NOT NULL
                 |);""".stripMargin
    val tbl3 = s"""
                 |${createTable("steprun")} (
                 |    step_run_id varchar(100) PRIMARY KEY,
                 |    job_run_id varchar(100) NOT NULL,
                 |    step_name text NOT NULL,
                 |    properties json NOT NULL,
                 |    status text NOT NULL,
                 |    elapsed_time varchar(100) NOT NULL,
                 |    step_type varchar(100) NOT NULL,
                 |    inserted_at bigint NOT NULL,
                 |    FOREIGN KEY (job_run_id) REFERENCES jobrun (job_run_id)
                 |);""".stripMargin
    val tbl4 = s"""
                 |${createTable("userinfo")} (
                 |    user_name varchar(100) PRIMARY KEY,
                 |    password varchar(100) NOT NULL,
                 |    user_active varchar(100) NOT NULL,
                 |    user_role varchar(100) NOT NULL
                 |);""".stripMargin
    val tbl5 = s"""
                 |${createTable("credential")} (
                 |   id SERIAL PRIMARY KEY,
                 |   name varchar(100) NOT NULL,
                 |   type varchar(100) NOT NULL,
                 |   value json NOT NULL,
                 |   valid_from timestamp NOT NULL DEFAULT NOW(),
                 |   valid_to timestamp
                 |);""".stripMargin
    val index1 = s"""
                 |${createUniqueIndex("credential_name_type")} ON credential (name,type) WHERE (valid_to is null);
                 |""".stripMargin

    for {
      _ <- DBApi.executeQuery(tbl1)
      _ <- DBApi.executeQuery(tbl2)
      _ <- DBApi.executeQuery(tbl3)
      _ <- DBApi.executeQuery(tbl4)
      _ <- DBApi.executeQuery(tbl5)
      _ <- DBApi.executeQuery(index1)
    } yield ()
  }
}
