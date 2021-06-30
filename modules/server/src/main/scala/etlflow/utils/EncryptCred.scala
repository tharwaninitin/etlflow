package etlflow.utils

import etlflow.db.JsonString
import etlflow.json.{JsonApi, JsonEnv}
import etlflow.schema.Credential.{AWS, JDBC}
import io.circe.Json
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import zio.RIO

private [etlflow]  object EncryptCred {

  implicit val AwsDecoder = deriveDecoder[AWS]
  implicit val JdbcDecoder = deriveDecoder[JDBC]
  implicit val JdbcEncoder = deriveEncoder[JDBC]
  implicit val AwsEncoder = deriveEncoder[AWS]

  def apply(`type`: String,value:JsonString): RIO[JsonEnv,String] = {
    `type` match {
      case "jdbc" => {
        for {
          jdbc <- JsonApi.convertToObject[JDBC](value.str)
          userName = EncryptionAPI.encrypt(jdbc.user)
          password = EncryptionAPI.encrypt(jdbc.password)
          jdbc_schema = JDBC(jdbc.url, userName, password, jdbc.driver)
          jsonValue  <- JsonApi.convertToString(jdbc_schema, List.empty)
        } yield jsonValue
      }
      case "aws" => {
        for {
          aws <- JsonApi.convertToObject[AWS](value.str)
          accessKey = EncryptionAPI.encrypt(aws.access_key)
          secretKey = EncryptionAPI.encrypt(aws.secret_key)
          aws_schema = AWS(accessKey,secretKey)
          jsonValue  <- JsonApi.convertToString(aws_schema, List.empty)
        } yield jsonValue
      }
    }
  }

}
