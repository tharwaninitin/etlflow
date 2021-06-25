package etlflow

import etlflow.db.RunDbMigration
import etlflow.schema.Config
import etlflow.schema.Credential.JDBC
import io.circe.generic.auto._

trait DbSuiteHelper {
  val config: Config = io.circe.config.parser.decode[Config]().toOption.get
  val credentials: JDBC = config.dbLog
  zio.Runtime.default.unsafeRun(RunDbMigration(credentials,clean = true))

}
