package etlflow.etlsteps

import etlflow.core.CoreEnv
import etlflow.crypto.CryptoApi
import etlflow.db.{DBApi, liveDB}
import etlflow.json.{JsonApi, JsonEnv}
import etlflow.schema.LoggingLevel
import etlflow.utils.Configuration
import io.circe.Decoder
import zio.{RIO, Tag}

case class GetCredentialStep[T: Tag : Decoder](name: String, credential_name: String) extends EtlStep[Unit, T] {

  override def process(input_state: => Unit): RIO[CoreEnv, T] = {
    val query = s"SELECT value FROM credential WHERE name='$credential_name' and valid_to is null;"
    for {
      config <- Configuration.config
      result <- DBApi.executeQuerySingleOutput[String](query)(rs => rs.string("value")).provideLayer(liveDB(config.db.get, "Credential-Step-" + name + "-Pool", 1))
      value <- CryptoApi.decryptCredential[T](result).provideSomeLayer[JsonEnv](etlflow.crypto.Implementation.live(config.secretkey))
      op <- JsonApi.convertToObject[T](value)
    } yield op
  }

  final override def getStepProperties(level: LoggingLevel): Map[String, String] = Map("credential_name" -> credential_name)
}
