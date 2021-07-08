package etlflow.coretests.utils

import etlflow.json.JsonApi
import etlflow.schema.Credential.{AWS, JDBC}
import etlflow.utils.EncryptionAPI
import zio.test.Assertion.equalTo
import zio.test.{DefaultRunnableSpec, TestAspect, ZSpec, assertM, environment}
import etlflow.utils.CredentialImplicits._

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
          jdbc           <- JsonApi.convertToObject[JDBC](jdbc_value)
          userName       = EncryptionAPI.encrypt(jdbc.user)
          password       = EncryptionAPI.encrypt(jdbc.password)
          jdbc_schema    = JDBC(jdbc.url,userName,password,jdbc.driver)
          jdbc_encrypted <- JsonApi.convertToString(jdbc_schema, List.empty)
        } yield jdbc_encrypted.replaceAll("\\s", "")
        assertM(actual_jdbc_encrypt)(equalTo(expected_jdbc_encrypt.replaceAll("\\s", "") ))
      },
      testM("Encryption should return correct decrypted AWS value") {
        val actual_aws_encrypt = for {
          aws           <- JsonApi.convertToObject[AWS](aws_value)
          access_key    = EncryptionAPI.encrypt(aws.access_key)
          secret_key    = EncryptionAPI.encrypt(aws.secret_key)
          aws_schema    = AWS(access_key,secret_key)
          aws_encrypted <- JsonApi.convertToString(aws_schema, List.empty)
        } yield aws_encrypted.replaceAll("\\s", "")
        assertM(actual_aws_encrypt)(equalTo(expected_aws_encrypt.replaceAll("\\s", "") ))
      }
    ).provideLayer(etlflow.json.Implementation.live) @@ TestAspect.flaky
}
