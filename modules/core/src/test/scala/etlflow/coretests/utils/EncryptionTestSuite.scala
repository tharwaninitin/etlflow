package etlflow.coretests.utils

import etlflow.coretests.TestSuiteHelper
import etlflow.json.JsonApi
import etlflow.schema.Credential.{AWS, JDBC}
import etlflow.utils.CredentialImplicits._
import etlflow.utils.{EncryptionAPI, ReflectAPI => RF}
import zio.test.Assertion.equalTo
import zio.test._
import zio.{Task, ZIO}


object EncryptionTestSuite  extends DefaultRunnableSpec  with TestSuiteHelper {

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
      },
      testM("Decryption should return correct  AWS value") {
        assertM(EncryptionAPI.getDecryptValues[AWS](expected_aws_encrypt))(equalTo("""{"access_key":"AKIA4FADZ4","secret_key":"ZiLo6CsbF6twGR"}"""))
      },
      testM("GetEtlJobPropsMapping should return correct  error message") {
        val x = Task(RF.getEtlJobPropsMapping[MEJP]("Job1","")).foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("ok"))
        assertM(x)(equalTo("Job1 not present"))
      },
    ).provideLayer(etlflow.json.Implementation.live) @@ TestAspect.flaky
}
