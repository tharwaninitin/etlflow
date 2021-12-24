package etlflow.db

import etlflow.schema.Credential.JDBC
import zio.{ExitCode, URIO}

object ResetDB extends zio.App {
  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {
    val cred = JDBC(sys.env("LOG_DB_URL"), sys.env("LOG_DB_USER"), sys.env("LOG_DB_PWD"), sys.env("LOG_DB_DRIVER"))
    CreateDB(true).provideLayer(liveDB(cred)).exitCode
  }
}
