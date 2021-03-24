package etlflow.etlsteps

import etlflow.jdbc.DbManager
import etlflow.utils.{Configuration, LoggingLevel}
import zio.Task

case class GetCredentialStep[T : Manifest](name: String, credential_name: String) extends EtlStep[Unit,T] with DbManager with Configuration {

  override def process(input_state: => Unit): Task[T] = {
    getDbCredentials[T](credential_name,config.dbLog,scala.concurrent.ExecutionContext.Implicits.global)
  }

  final override def getStepProperties(level: LoggingLevel): Map[String, String] = Map("credential_name" -> credential_name)
}
