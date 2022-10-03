package etlflow.db.utils

import etlflow.db.liveDB
import etlflow.model.Credential.JDBC
import etlflow.utils.ApplicationLogger
import zio.{Task, ZIO}

object ResetDB extends zio.ZIOAppDefault with ApplicationLogger {
  private val program = ZIO
    .attempt(JDBC(sys.env("LOG_DB_URL"), sys.env("LOG_DB_USER"), sys.env("LOG_DB_PWD"), sys.env("LOG_DB_DRIVER")))
    .tapError(_ => ZIO.succeed(logger.error("""Set environment variables to continue
                                              | LOG_DB_URL
                                              | LOG_DB_USER
                                              | LOG_DB_PWD
                                              | LOG_DB_DRIVER
                                              |""".stripMargin)))
    .flatMap(cred => CreateDB(true).provideLayer(liveDB(cred)))

  override def run: Task[Unit] = program
}
