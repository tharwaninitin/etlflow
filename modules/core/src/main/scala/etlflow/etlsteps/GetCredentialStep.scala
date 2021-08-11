package etlflow.etlsteps

import etlflow.CoreEnv
import etlflow.crypto.CryptoApi
import etlflow.db.DBApi
import etlflow.json.JsonApi
import etlflow.schema.LoggingLevel
import io.circe.Decoder
import zio.{RIO,Tag}

case class GetCredentialStep[T : Tag : Decoder](name: String, credential_name: String) extends EtlStep[Unit,T] {

  override def process(input_state: => Unit): RIO[CoreEnv, T] = {
    val query = s"SELECT value FROM credential WHERE name='$credential_name' and valid_to is null;"
    for {
      result <- DBApi.executeQuerySingleOutput[String](query)(rs => rs.string("value"))
      value  <- CryptoApi.decryptCredential[T](result)
      op     <- JsonApi.convertToObject[T](value)
    } yield op
  }

  final override def getStepProperties(level: LoggingLevel): Map[String, String] = Map("credential_name" -> credential_name)
}
