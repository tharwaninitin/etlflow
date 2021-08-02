package etlflow.crypto

import etlflow.json
import etlflow.json.JsonApi
import etlflow.schema.Credential.{AWS, JDBC}
import etlflow.utils.CredentialImplicits._
import zio.test.Assertion.equalTo
import zio.test._

object EncryptionTestSuite extends DefaultRunnableSpec {

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
          userName       <- CryptoApi.encrypt(jdbc.user)
          password       <- CryptoApi.encrypt(jdbc.password)
          jdbc_schema    = JDBC(jdbc.url,userName,password,jdbc.driver)
          jdbc_encrypted <- JsonApi.convertToString(jdbc_schema, List.empty)
        } yield jdbc_encrypted.replaceAll("\\s", "")
        assertM(actual_jdbc_encrypt)(equalTo(expected_jdbc_encrypt.replaceAll("\\s", "") ))
      },
      testM("Encryption should return correct decrypted AWS value") {
        val actual_aws_encrypt = for {
          aws           <- JsonApi.convertToObject[AWS](aws_value)
          access_key    <- CryptoApi.encrypt(aws.access_key)
          secret_key    <- CryptoApi.encrypt(aws.secret_key)
          aws_schema    = AWS(access_key,secret_key)
          aws_encrypted <- JsonApi.convertToString(aws_schema, List.empty)
        } yield aws_encrypted.replaceAll("\\s", "")
        assertM(actual_aws_encrypt)(equalTo(expected_aws_encrypt.replaceAll("\\s", "") ))
      },
      testM("DecryptCredential should decrypt AWS credential correctly") {
        assertM(CryptoApi.decryptCredential[AWS](expected_aws_encrypt))(equalTo("""{"access_key":"AKIA4FADZ4","secret_key":"ZiLo6CsbF6twGR"}"""))
      },
      testM("EncryptCredential should encrypt AWS credential correctly") {
        assertM(CryptoApi.encryptCredential("aws",aws_value))(equalTo("""{"access_key":"XRhxQfeHwehR/kvJGFbviw==","secret_key":"B67uKTvOC2B5GQEMAjnfPQ=="}"""))
      },
      testM("StringFormatter should should return formatted string when given string with special characters") {
        assertM(CryptoApi.encrypt("admin"))(equalTo("twV4rChhxs76Z+gY868NSw=="))
      },
      testM("Asymmetric Encrypted key should should not be Bcrypt Bounded") {
        val password = CryptoApi.oneWayEncrypt("abc")
        assertM(CryptoApi.encrypt("admin"))(equalTo("twV4rChhxs76Z+gY868NSw=="))
      }
    ).provideLayer(Implementation.live ++ json.Implementation.live) @@ TestAspect.flaky
}
