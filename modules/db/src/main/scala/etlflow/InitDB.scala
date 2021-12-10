package etlflow

import etlflow.db.RunDbMigration
import etlflow.schema.Credential.JDBC
import zio.{ExitCode, URIO}

object InitDB extends zio.App {
  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {
    val cred = JDBC(sys.env("LOG_DB_URL"), sys.env("LOG_DB_USER"), sys.env("LOG_DB_PWD"), "org.postgresql.Driver")
    RunDbMigration(cred).exitCode
  }
}
