package etlflow.etlsteps

import etlflow.core.CoreEnv
import etlflow.crypto.{CryptoApi, CryptoEnv}
import etlflow.db.{DBApi, liveDB}
import etlflow.json.{JsonApi, JsonEnv}
import etlflow.schema.Credential.{AWS, JDBC}
import etlflow.utils.Configuration
import io.circe.Decoder
import zio.blocking.Blocking
import zio.{RIO, Tag}
import io.circe.generic.auto._

case class GetCredentialStep[T: Tag : Decoder](name: String, credential_name: String) extends EtlStep[Unit, T] {

  override def process(input_state: => Unit): RIO[CoreEnv, T] = {
    val query = s"SELECT value FROM credential WHERE name='$credential_name' and valid_to is null;"
    val step = for {
      config <- Configuration.config
      result <- DBApi.executeQuerySingleOutput[String](query)(rs => rs.string("value")).provideLayer(liveDB(config.db.get, "Credential-Step-" + name + "-Pool", 1))
      value <- decryptCredential[T](result).provideSomeLayer[JsonEnv](etlflow.crypto.Implementation.live(config.secretkey))
      op <- JsonApi.convertToObject[T](value)
    } yield op
    step.provideSomeLayer[Blocking](etlflow.json.Implementation.live)
  }

  def decryptCredential[T: Tag](text: String): RIO[CryptoEnv with JsonEnv,String] = {
    implicitly[Tag[T]].tag match {
      case t if t =:= Tag[JDBC].tag =>
        for {
          jdbc                <- JsonApi.convertToObject[JDBC](text)
          decrypt_user        <- CryptoApi.decrypt(jdbc.user)
          decrypt_password    <- CryptoApi.decrypt(jdbc.password)
          json                <- JsonApi.convertToString(JDBC(jdbc.url, decrypt_user, decrypt_password, jdbc.driver))
        } yield json
      case t if t =:= Tag[AWS].tag =>
        for {
          aws        <- JsonApi.convertToObject[AWS](text)
          decrypt_access_key <- CryptoApi.decrypt(aws.access_key)
          decrypt_secret_key <- CryptoApi.decrypt(aws.secret_key)
          json       <- JsonApi.convertToString(AWS(decrypt_access_key, decrypt_secret_key))
        } yield json
    }
  }

  final override def getStepProperties: Map[String, String] = Map("credential_name" -> credential_name)
}
