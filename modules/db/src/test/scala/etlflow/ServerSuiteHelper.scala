package etlflow

import etlflow.schema.Config
import etlflow.schema.Credential.JDBC
import etlflow.utils.DbManager
import io.circe.generic.auto._

trait ServerSuiteHelper extends DbManager  {

  val config: Config = io.circe.config.parser.decode[Config]().toOption.get
  val credentials: JDBC = config.dbLog
}
