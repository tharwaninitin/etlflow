package etlflow.utils

import etlflow.jdbc.JsonString
import etlflow.schema.Credential.{AWS, JDBC}
import io.circe.generic.semiauto.deriveDecoder

object EncryptCred {

  implicit val AwsDecoder = deriveDecoder[AWS]
  implicit val JdbcDecoder = deriveDecoder[JDBC]

  def apply(`type`: String,value:JsonString):JsonString = {
    `type` match {
      case "jdbc" => {
        val jdbc = JsonCirce.convertToObject[JDBC](value.str)
        val userName = Encryption.encrypt(jdbc.user)
        val password = Encryption.encrypt(jdbc.password)
        val jdbc_schema = JDBC(jdbc.url,userName,password,jdbc.driver)
        JsonString(JsonJackson.convertToJsonByRemovingKeys(jdbc_schema,List.empty))
      }
      case "aws" => {
        val aws = JsonCirce.convertToObject[AWS](value.str)
        val accessKey = Encryption.encrypt(aws.access_key)
        val secretKey = Encryption.encrypt(aws.secret_key)
        val aws_schema = AWS(accessKey,secretKey)
        JsonString(JsonJackson.convertToJsonByRemovingKeys(aws_schema,List.empty))
      }
    }
  }

}
