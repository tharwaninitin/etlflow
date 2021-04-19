package etlflow.etlsteps

import etlflow.JobEnv
import etlflow.jdbc.QueryApi
import etlflow.utils.{JsonJackson, LoggingLevel}
import zio.{RIO, Task}

case class GetCredentialStep[T : Manifest](name: String, credential_name: String) extends EtlStep[Unit,T] {

  override def process(input_state: => Unit): RIO[JobEnv,T] = {
    val query = s"SELECT value FROM credential WHERE name='$credential_name' and valid_to is null;"
    for {
        result <- QueryApi.executeQueryWithSingleResponse[String](query)
        op     <- Task(JsonJackson.convertToObject[T](result))
      } yield op
  }

  final override def getStepProperties(level: LoggingLevel): Map[String, String] = Map("credential_name" -> credential_name)
}
