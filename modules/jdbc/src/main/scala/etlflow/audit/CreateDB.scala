package etlflow.audit

import etlflow.log.ApplicationLogger
import etlflow.model.Credential.JDBC
import zio.{RIO, Task, ZIO}

object CreateDB extends ApplicationLogger with zio.ZIOAppDefault {

  def execute(reset: Boolean = false): RIO[etlflow.db.DB, Unit] = {
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
                    |    props json NOT NULL,
                    |    status text NOT NULL,
                    |    created_at timestamp(6) NOT NULL,
                    |    updated_at timestamp(6) NOT NULL
                    |);""".stripMargin
    val taskrun = s"""
                     |${createTable("taskrun")} (
                     |    task_run_id varchar(100) PRIMARY KEY,
                     |    job_run_id varchar(100) NOT NULL,
                     |    task_name text NOT NULL,
                     |    task_type varchar(100) NOT NULL,
                     |    props json NOT NULL,
                     |    status text NOT NULL,
                     |    created_at timestamp(6) NOT NULL,
                     |    updated_at timestamp(6) NOT NULL,
                     |    FOREIGN KEY (job_run_id) REFERENCES jobrun (job_run_id)
                     |);""".stripMargin

    for {
      _ <- etlflow.db.DB.executeQuery(jobrun).as(logger.info(jobrun))
      _ <- etlflow.db.DB.executeQuery(taskrun).as(logger.info(taskrun))
    } yield ()
  }

  val program: Task[Unit] = ZIO
    .attempt(JDBC(sys.env("DB_URL"), sys.env("DB_USER"), sys.env("DB_PWD"), sys.env("DB_DRIVER")))
    .tapError(_ => ZIO.logInfo("""Set environment variables to continue
                                 | DB_URL
                                 | DB_USER
                                 | DB_PWD
                                 | DB_DRIVER
                                 | INIT
                                 |""".stripMargin))
    .flatMap(cred => CreateDB.execute(sys.env("INIT").toBoolean).provideLayer(etlflow.db.DB.live(cred)))

  override def run: Task[Unit] = program
}
