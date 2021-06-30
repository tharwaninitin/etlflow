package etlflow.etlsteps

import etlflow.JobEnv
import etlflow.db.DBApi
import etlflow.json.JsonApi
import etlflow.schema.LoggingLevel
import etlflow.utils.EncryptionAPI
import io.circe.Decoder
import zio.RIO

import scala.reflect.runtime.universe.TypeTag

case class GetCredentialStep[T : TypeTag : Decoder](name: String, credential_name: String) extends EtlStep[Unit,T] {

  override def process(input_state: => Unit): RIO[JobEnv,T] = {
    val query = s"SELECT value FROM credential WHERE name='$credential_name' and valid_to is null;"
    for {
      result <- DBApi.executeQueryWithSingleResponse[String](query)
      dValue <- EncryptionAPI.getDecryptValues[T](result)
      op     <- JsonApi.convertToObject[T](dValue.toString())
    } yield op
  }

  final override def getStepProperties(level: LoggingLevel): Map[String, String] = Map("credential_name" -> credential_name)
}
