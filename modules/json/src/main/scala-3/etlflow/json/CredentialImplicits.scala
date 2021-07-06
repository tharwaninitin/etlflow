package etlflow.json

import io.circe.Decoder
import io.circe.Encoder
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import scala.language.implicitConversions
import etlflow.schema.Credential.{JDBC, AWS}

object CredentialImplicits {
  given AwsDecoder: Decoder[AWS] = deriveDecoder[AWS]
  given JdbcDecoder: Decoder[JDBC] = deriveDecoder[JDBC]
  given AwsEncoder: Encoder[AWS] = deriveEncoder[AWS]
  given JdbcEncoder: Encoder[JDBC]  = deriveEncoder[JDBC]
}
