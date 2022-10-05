package etlflow.db.utils

import etlflow.db.{DBApi, DBEnv}
import etlflow.log.ApplicationLogger
import zio.ZIO

object CreateDB extends ApplicationLogger {
  def apply(reset: Boolean = false): ZIO[DBEnv, Throwable, Unit] = {
    def createTable(name: String): String =
      if (reset)
        s"""
           |DROP TABLE IF EXISTS $name CASCADE;
           |CREATE TABLE $name""".stripMargin
      else s"CREATE TABLE IF NOT EXISTS $name"

    val jobrun = s"""
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
    val taskrun = s"""
                     |${createTable("taskrun")} (
                     |    task_run_id varchar(100) PRIMARY KEY,
                     |    job_run_id varchar(100) NOT NULL,
                     |    task_name text NOT NULL,
                     |    properties json NOT NULL,
                     |    status text NOT NULL,
                     |    elapsed_time varchar(100) NOT NULL,
                     |    task_type varchar(100) NOT NULL,
                     |    inserted_at bigint NOT NULL,
                     |    FOREIGN KEY (job_run_id) REFERENCES jobrun (job_run_id)
                     |);""".stripMargin

    for {
      _ <- DBApi.executeQuery(jobrun).as(logger.info(jobrun))
      _ <- DBApi.executeQuery(taskrun).as(logger.info(taskrun))
    } yield ()
  }
}
