package etlflow.etlsteps

import etlflow.jdbc.DbManager
import etlflow.utils.Configuration
import zio.Task

case class GetCredentialStep[T : Manifest](name: String, credential_name: String) extends EtlStep[Unit,T] with DbManager with Configuration {
  override def process(input_state: => Unit): Task[T] = {
    getDbCredentials[T](credential_name,config.dbLog,scala.concurrent.ExecutionContext.Implicits.global)
  }
}
