package etlflow.db.utils

import etlflow.db.liveDB
import etlflow.model.Credential.JDBC
import zio.{ExitCode, URIO}

object InitDB extends zio.App {
  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {
    val cred = JDBC(sys.env("LOG_DB_URL"), sys.env("LOG_DB_USER"), sys.env("LOG_DB_PWD"), sys.env("LOG_DB_DRIVER"))
    CreateDB().provideLayer(liveDB(cred)).exitCode
  }
}
