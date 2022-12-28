package etlflow.audit

import etlflow.log.ApplicationLogger
import zio.{RIO, Task}

object CreateBQ extends ApplicationLogger with zio.ZIOAppDefault {

  def execute(reset: Boolean = false): RIO[gcp4zio.bq.BQ, Unit] = {
    def createTable(name: String): String =
      if (reset)
        s"""
           |DROP TABLE IF EXISTS etlflow.$name;
           |CREATE TABLE etlflow.$name""".stripMargin
      else s"CREATE TABLE IF NOT EXISTS etlflow.$name"

    val jobrun = s"""
                    |${createTable("jobrun")} (
                    |    job_run_id STRING(100) NOT NULL,
                    |    job_name STRING NOT NULL,
                    |    props JSON NOT NULL,
                    |    status STRING NOT NULL,
                    |    created_at TIMESTAMP NOT NULL,
                    |    updated_at TIMESTAMP NOT NULL
                    |);""".stripMargin
    val taskrun = s"""
                     |${createTable("taskrun")} (
                     |    task_run_id STRING(100) NOT NULL,
                     |    job_run_id STRING(100) NOT NULL,
                     |    task_name STRING NOT NULL,
                     |    task_type STRING(100) NOT NULL,
                     |    props JSON NOT NULL,
                     |    status STRING NOT NULL,
                     |    created_at TIMESTAMP NOT NULL,
                     |    updated_at TIMESTAMP NOT NULL
                     |);""".stripMargin

    for {
      _ <- gcp4zio.bq.BQ.executeQuery(jobrun).as(logger.info(jobrun))
      _ <- gcp4zio.bq.BQ.executeQuery(taskrun).as(logger.info(taskrun))
    } yield ()
  }

  val program: Task[Unit] = execute(true).provideLayer(gcp4zio.bq.BQ.live())

  override def run: Task[Unit] = program
}
