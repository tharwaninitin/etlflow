package etlflow.crypto

import etlflow.schema.Credential.{AWS, JDBC}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

object CredentialImplicits {
  implicit val AwsDecoder = deriveDecoder[AWS]
  implicit val JdbcDecoder = deriveDecoder[JDBC]
  implicit val AwsEncoder = deriveEncoder[AWS]
  implicit val JdbcEncoder = deriveEncoder[JDBC]
}
