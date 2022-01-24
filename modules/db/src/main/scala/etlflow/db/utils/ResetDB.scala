package etlflow.db.utils

import etlflow.db.liveDB
import etlflow.model.Credential.JDBC
import etlflow.utils.ApplicationLogger
import zio.{ExitCode, Task, UIO, URIO}

object ResetDB extends zio.App with ApplicationLogger {
  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    Task(JDBC(sys.env("LOG_DB_URL"), sys.env("LOG_DB_USER"), sys.env("LOG_DB_PWD"), sys.env("LOG_DB_DRIVER")))
      .tapError(_ => UIO(logger.error("""Set environment variables to continue
                                        | LOG_DB_URL
                                        | LOG_DB_USER
                                        | LOG_DB_PWD
                                        | LOG_DB_DRIVER
                                        |""".stripMargin)))
      .flatMap(cred => CreateDB(true).provideLayer(liveDB(cred)))
      .exitCode
}
