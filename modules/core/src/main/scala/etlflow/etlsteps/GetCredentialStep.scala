package etlflow.etlsteps

import etlflow.JobEnv
import etlflow.crypto.CryptoApi
import etlflow.db.DBApi
import etlflow.json.JsonApi
import etlflow.schema.LoggingLevel
import etlflow.utils.Configuration
import io.circe.Decoder
import zio.RIO
import scala.reflect.runtime.universe.TypeTag

case class GetCredentialStep[T : TypeTag : Decoder](name: String, credential_name: String) extends EtlStep[Unit,T] {

  override def process(input_state: => Unit): RIO[JobEnv, T] = {
    val query = s"SELECT value FROM credential WHERE name='$credential_name' and valid_to is null;"
    for {
      key    <- Configuration.config.map(_.webserver.flatMap(_.secretKey))
      result <- DBApi.executeQueryWithSingleResponse[String](query)
      value  <- CryptoApi.decryptCredential[T](result)
      op     <- JsonApi.convertToObject[T](value)
    } yield op
  }

  final override def getStepProperties(level: LoggingLevel): Map[String, String] = Map("credential_name" -> credential_name)
}
