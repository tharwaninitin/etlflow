package etlflow.crypto

import etlflow.json
import etlflow.schema.Credential.AWS
import org.mindrot.jbcrypt.BCrypt
import zio.test.Assertion.equalTo
import zio.test._

object CryptoTestSuite extends DefaultRunnableSpec {

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
      testM("EncryptCredential should encrypt JDBC credential correctly") {
        assertM(CryptoApi.encryptCredential("jdbc",jdbc_value))(equalTo(expected_jdbc_encrypt.replaceAll("\\s", "")))
      },
      testM("DecryptCredential should decrypt AWS credential correctly") {
        assertM(CryptoApi.decryptCredential[AWS](expected_aws_encrypt))(equalTo("""{"access_key":"AKIA4FADZ4","secret_key":"ZiLo6CsbF6twGR"}"""))
      },
      testM("EncryptCredential should encrypt AWS credential correctly") {
        assertM(CryptoApi.encryptCredential("aws",aws_value))(equalTo("""{"access_key":"XRhxQfeHwehR/kvJGFbviw==","secret_key":"B67uKTvOC2B5GQEMAjnfPQ=="}"""))
      },
      testM("Encrypt should encrypt string correctly") {
        assertM(CryptoApi.encrypt("admin"))(equalTo("twV4rChhxs76Z+gY868NSw=="))
      },
      testM("Asymmetric Encrypted should not be Bcrypt Bounded without salt") {
        assertM(CryptoApi.oneWayEncrypt("abc").map(p => BCrypt.checkpw("abc", p)))(equalTo(true))
      },
      testM("Asymmetric Encrypted should not be Bcrypt Bounded with salt") {
        assertM(CryptoApi.oneWayEncrypt("abc",Some(10)).map(p => BCrypt.checkpw("abc", p)))(equalTo(true))
      },
      testM("Asymmetric Encrypted should not be Bcrypt Bounded with negative case") {
        assertM(CryptoApi.oneWayEncrypt("abc",Some(10)).map(p => BCrypt.checkpw("abc1", p)))(equalTo(false))
      }
    ).provideLayer(Implementation.live(None) ++ json.Implementation.live) @@ TestAspect.flaky
}
