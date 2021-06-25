package etlflow.utils

import etlflow.db.JsonString
import etlflow.json.{Implementation, JsonService}
import etlflow.schema.Credential.{AWS, JDBC}
import io.circe.Json
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import zio.Task

private [etlflow]  object EncryptCred {

  implicit val AwsDecoder = deriveDecoder[AWS]
  implicit val JdbcDecoder = deriveDecoder[JDBC]
  implicit val JdbcEncoder = deriveEncoder[JDBC]
  implicit val AwsEncoder = deriveEncoder[AWS]

  def apply(`type`: String,value:JsonString): Task[Json] = {
    `type` match {
      case "jdbc" => {
        for {
          jdbc <- JsonService.convertToObject[JDBC](value.str)
          userName = Encryption.encrypt(jdbc.user)
          password = Encryption.encrypt(jdbc.password)
          jdbc_schema = JDBC(jdbc.url, userName, password, jdbc.driver)
          jsonValue  <- JsonService.convertToJsonByRemovingKeys(jdbc_schema, List.empty)
        } yield jsonValue
      }.provideLayer(Implementation.live)
      case "aws" => {
        for {
          aws <- JsonService.convertToObject[AWS](value.str)
          accessKey = Encryption.encrypt(aws.access_key)
          secretKey = Encryption.encrypt(aws.secret_key)
          aws_schema = AWS(accessKey,secretKey)
          jsonValue  <- JsonService.convertToJsonByRemovingKeys(aws_schema, List.empty)
        } yield jsonValue
      }.provideLayer(Implementation.live)
    }
  }

}
