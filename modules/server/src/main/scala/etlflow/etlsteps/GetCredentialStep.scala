package etlflow.etlsteps

import crypto4s.Crypto
import etlflow.db.{liveDB, DBApi}
import etlflow.json.{JsonApi, JsonEnv}
import etlflow.model.Credential.{AWS, JDBC}
import etlflow.utils.Configuration
import zio.blocking.Blocking
import zio.json._
import zio.{RIO, Tag}
import etlflow.json.CredentialImplicits._

case class GetCredentialStep[T: Tag: JsonDecoder](name: String, credential_name: String) extends EtlStep[Blocking, T] {

  protected def process: RIO[Blocking, T] = {
    val query = s"SELECT value FROM credential WHERE name='$credential_name' and valid_to is null;"
    val step = for {
      config <- Configuration.config
      result <- DBApi
        .executeQuerySingleOutput[String](query)(rs => rs.string("value"))
        .provideLayer(liveDB(config.db.get, "Credential-Step-" + name + "-Pool", 1))
      crypto = Crypto(config.secretkey)
      value <- decryptCredential[T](result, crypto)
      op    <- JsonApi.convertToObject[T](value)
    } yield op
    step.provideSomeLayer[Blocking](etlflow.json.Implementation.live)
  }

  def decryptCredential[C: Tag](text: String, crypto: Crypto): RIO[JsonEnv, String] =
    implicitly[Tag[C]].tag match {
      case t if t =:= Tag[JDBC].tag =>
        for {
          jdbc <- JsonApi.convertToObject[JDBC](text)
          decrypt_user     = crypto.decrypt(jdbc.user)
          decrypt_password = crypto.decrypt(jdbc.password)
          json <- JsonApi.convertToString(JDBC(jdbc.url, decrypt_user, decrypt_password, jdbc.driver))
        } yield json
      case t if t =:= Tag[AWS].tag =>
        for {
          aws <- JsonApi.convertToObject[AWS](text)
          decrypt_access_key = crypto.decrypt(aws.access_key)
          decrypt_secret_key = crypto.decrypt(aws.secret_key)
          json <- JsonApi.convertToString(AWS(decrypt_access_key, decrypt_secret_key))
        } yield json
    }

  final override def getStepProperties: Map[String, String] = Map("credential_name" -> credential_name)
}
