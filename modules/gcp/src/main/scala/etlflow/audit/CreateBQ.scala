package etlflow.audit

import etlflow.log.ApplicationLogger
import zio.{RIO, Task}

object CreateBQ extends ApplicationLogger with zio.ZIOAppDefault {

  def execute(reset: Boolean = false): RIO[gcp4zio.bq.BQ, Unit] = {
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
                    |    args json NOT NULL,
                    |    properties json NOT NULL,
                    |    status text NOT NULL,
                    |    elapsed_time varchar(100) NOT NULL,
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
      _ <- gcp4zio.bq.BQ.executeQuery(jobrun).as(logger.info(jobrun))
      _ <- gcp4zio.bq.BQ.executeQuery(taskrun).as(logger.info(taskrun))
    } yield ()
  }

  val program: Task[Unit] = execute(sys.env("INIT").toBoolean).provideLayer(gcp4zio.bq.BQ.live())

  override def run: Task[Unit] = program
}
