package etlflow.coretests.utils

import etlflow.Credential.{AWS, JDBC}
import etlflow.utils.{Encryption, JsonCirce, JsonJackson}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class EncryptionTestSuite extends AnyFlatSpec with should.Matchers {

  val jdbc_value = """{"url": "localhost123","user": "localhost","password": "swap123","driver": "org.postgresql.Driver"}""".stripMargin

  val aws_value = {
  """{
     |"access_key": "AKIA4FADZ4",
     |"secret_key": "ZiLo6CsbF6twGR"
     |}""".stripMargin
  }

  val input_case1 = "etlflow-library"

  val jdbc        = JsonCirce.convertToObject[JDBC](jdbc_value)
  val userName    = Encryption.encrypt(jdbc.user)
  val password    = Encryption.encrypt(jdbc.password)
  val jdbc_schema = JDBC(jdbc.url,userName,password,jdbc.driver)
  val jdbc_encrypted   = JsonJackson.convertToJsonByRemovingKeys(jdbc_schema,List.empty)
  val jdbc_decrypted   = Encryption.getDecreptValues[JDBC](jdbc_encrypted)

  val aws        = JsonCirce.convertToObject[AWS](aws_value)
  val access_key = Encryption.encrypt(aws.access_key)
  val secret_key = Encryption.encrypt(aws.secret_key)
  val aws_schema = AWS(access_key,secret_key)
  val aws_encrypted   = JsonJackson.convertToJsonByRemovingKeys(aws_schema,List.empty)
  val aws_decrypted   = Encryption.getDecreptValues[AWS](aws_encrypted)


  val encrypted_case1 = Encryption.encrypt(input_case1)
  val decrypted_case1 = Encryption.decrypt(encrypted_case1)


  "Encryption" should "return correct decrypted JDBC value" in {
    assert(jdbc_value.replaceAll("\\s", "") == jdbc_decrypted.replaceAll("\\s", ""))
  }

  "Encryption" should "return correct decrypted AWS value" in {
    assert(aws_value.replaceAll("\\s", "") == aws_decrypted.replaceAll("\\s", ""))
  }

  "Encryption" should "return correct decrypted value" in {
    assert(input_case1 == decrypted_case1)
  }

}

