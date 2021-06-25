package etlflow.coretests.utils

import etlflow.json.CredentialImplicits._
import etlflow.json.{Implementation, JsonService}
import etlflow.schema.Credential.{AWS, JDBC}
import etlflow.utils.Encryption
import zio.test.Assertion.equalTo
import zio.test.{DefaultRunnableSpec, ZSpec, assertM, environment}

object EncryptionTestSuite  extends DefaultRunnableSpec  {

  val jdbc_value = """{"url": "localhost123","user": "AKIA4FADZ4","password": "ZiLo6CsbF6twGR","driver": "org.postgresql.Driver"}""".stripMargin
  val aws_value = {
    """{
      |"access_key": "AKIA4FADZ4",
      |"secret_key": "ZiLo6CsbF6twGR"
      |}""".stripMargin
  }

  val expected_jdbc_encrypt = """{
                                | "url" : "localhost123",
                                | "user" : "XRhxQfeHwehR/kvJGFbviw==",
                                | "password" : "B67uKTvOC2B5GQEMAjnfPQ==",
                                | "driver" : "org.postgresql.Driver"
                                |}""".stripMargin

  val expected_aws_encrypt = """{"access_key":"XRhxQfeHwehR/kvJGFbviw==","secret_key":"B67uKTvOC2B5GQEMAjnfPQ=="}"""

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("Encryption Test")(
      testM("Encryption should return correct decrypted JDBC value") {
        val actual_jdbc_encrypt = for {
          jdbc <- JsonService.convertToObject[JDBC](jdbc_value)
          userName    = Encryption.encrypt(jdbc.user)
          password    = Encryption.encrypt(jdbc.password)
          jdbc_schema = JDBC(jdbc.url,userName,password,jdbc.driver)
          jdbc_encrypted    <- JsonService.convertToJsonByRemovingKeys(jdbc_schema, List.empty)
        } yield jdbc_encrypted.toString().replaceAll("\\s", "")
        assertM(actual_jdbc_encrypt)(equalTo(expected_jdbc_encrypt.replaceAll("\\s", "") ))
      },
      testM("Encryption should return correct decrypted AWS value") {
        val actual_aws_encrypt = for {
          aws <- JsonService.convertToObject[AWS](aws_value)
          access_key    = Encryption.encrypt(aws.access_key)
          secret_key    = Encryption.encrypt(aws.secret_key)
          aws_schema = AWS(access_key,secret_key)
          aws_encrypted    <- JsonService.convertToJsonByRemovingKeys(aws_schema, List.empty)
        } yield aws_encrypted.toString().replaceAll("\\s", "")
        assertM(actual_aws_encrypt)(equalTo(expected_aws_encrypt.replaceAll("\\s", "") ))
      }
    ).provideLayer(Implementation.live)
}
