package etlflow.etlsteps

import etlflow.JobEnv
import etlflow.db.DBApi
import etlflow.utils.{Encryption, JsonJackson, LoggingLevel}
import zio.{RIO, Task}

case class GetCredentialStep[T : Manifest](name: String, credential_name: String) extends EtlStep[Unit,T] {

  override def process(input_state: => Unit): RIO[JobEnv,T] = {
    val query = s"SELECT value FROM credential WHERE name='$credential_name' and valid_to is null;"
    for {
        result <- DBApi.executeQueryWithSingleResponse[String](query)
        dValue <- Task(Encryption.getDecreptValues[T](result))
        op     <- Task(JsonJackson.convertToObject[T](dValue))
      } yield op
  }

  final override def getStepProperties(level: LoggingLevel): Map[String, String] = Map("credential_name" -> credential_name)
}
